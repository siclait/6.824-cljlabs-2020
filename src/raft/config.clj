(ns raft.config
  "Support for Raft tester."
  (:require [clojure.core.async :as a]
            [clojure.string :as string]
            [clojure.test :as test]
            [crypto.random :as crand]
            [go.sync :as sync]
            [go.time :as time]
            [lab-rpc.core :as lab-rpc]
            [raft.core :as raft]
            [raft.persister :as persister]
            [taoensso.timbre :as log])
  (:import java.util.Random))

(defn rand-string
  [n]
  (let [s (crand/base64 (* 2 n))]
    (subs s 0 n)))

(defn make-seed
  []
  (BigInteger. 62 (Random.)))

(defprotocol IConfig
  (crash1 [this i])
  (start1 [this i])
  (check-timeout [this])
  (cleanup [this])
  (connect [this i])
  (disconnect [this i])
  (rpc-count [this server])
  (rpc-total [this])
  (unreliable! [this unreliable])
  (long-reordering! [this long-reordering])
  (bytes-total [this])
  (check-one-leader [this])
  (check-terms [this])
  (check-no-leader [this])
  (n-committed [this index])
  (wait [this index n start-term])
  (one [this cmd expected-servers retry])
  (begin [this description])
  (end [this]))

(defrecord Config [mu net n rafts apply-err connected saved end-names logs
                   start t0 rpcs0 bytes0 cmds0 max-index max-index0]
  IConfig
  ;; shut down a Raft server but save its persistent state.
  (crash1 [this i]
    (disconnect this i)
    (lab-rpc/delete-server! net i)
    (locking mu
      ;; a fresh persister, in case old instance
      ;; continues to update the Persister.
      ;; but copy old persister's content so that we always
      ;; pass Make() the last persisted state.
      (when-let [saved-persister (get @saved i)]
        (swap! saved assoc i (persister/copy saved-persister)))
      (when-let [rf (get @rafts i)]
        (monitor-exit mu)
        (raft/kill rf)
        (monitor-enter mu)
        (swap! rafts assoc i nil))
      (when-let [saved-persister (get @saved i)]
        (let [raft-log (persister/read-raft-state saved-persister)]
          (swap! saved assoc i (persister/make-persister))
          (persister/save-raft-state! (get @saved i) raft-log)))))

  ;; start or re-start a Raft.
  ;; if one already exists, "kill" it first.
  ;; allocate new outgoing port file names, and a new
  ;; state persister, to isolate previous instance of
  ;; this server. since we cannot really kill it.
  (start1 [this i]
    (crash1 this i)
    ;;	a fresh set of outgoing ClientEnd names.
    ;;	so that old crashed instance's ClientEnds can't send.
    (swap! end-names assoc i
           (vec (take n (repeatedly #(rand-string 20)))))
    ;; a fresh set of ClientEnds.
    (let [ends (doall
                 (for [j    (range n)
                       :let [end-name (get-in @end-names [i j])
                             end (lab-rpc/make-end! net end-name)
                             _ (lab-rpc/connect! net end-name j)]]
                   end))]
      (locking mu
        ;; a fresh persister, so old instance doesn't overwrite
        ;; new instance's persisted state.
        ;; but copy old persister's content so that we always
        ;; pass Make() the last persisted state.
        (if-let [saved-persister (get @saved i)]
          (swap! saved assoc i (persister/copy saved-persister))
          (swap! saved assoc i (persister/make-persister))))
      ;; listen to messages from Raft indicating newly committed messages.
      (let [apply-ch (a/chan)]
        (a/thread
          (loop []
            (let [m       (a/<!! apply-ch)
                  err-msg (atom "")]
              (when (:command-valid m)
                (let [v (:command m)]
                  (monitor-enter mu)
                  (dotimes [j (count @logs)]
                    (when-let [old (get-in @logs [j (:command-index m)])]
                      (when (not= old v)
                        (reset! err-msg (format "commit index=%d server=%d %s != server=%d %d"
                                                (:command-index m) i (:command m) j old)))))
                  (let [prev-ok (get-in @logs [i (dec (:command-index m))])]
                    (swap! logs assoc-in [i (:command-index m)] v)
                    (when (< @max-index (:command-index m))
                      (reset! max-index (:command-index m)))
                    (monitor-exit mu)
                    (when (and (< 1 (:command-index m))
                               (nil? prev-ok))
                      (reset! err-msg (format "server %d apply out of order %d"
                                              i (:command-index m)))))))
              (if (not (string/blank? @err-msg))
                (do
                  (log/fatalf "apply error: %s" @err-msg)
                  (swap! apply-err assoc i @err-msg))
                (recur)))))
        ;; keep reading after error so that Raft doesn't block
        ;; holding locks...
        (let [rf (raft/make ends i (get @saved i) apply-ch)]
          (locking mu
            (swap! rafts assoc i rf))
          (let [svc (lab-rpc/make-service rf)
                srv (lab-rpc/make-server)
                _   (lab-rpc/add-service! srv svc)
                _   (lab-rpc/add-server! net i srv)])))))

  (check-timeout [_]
    ;; enforce a two minute real-time limit on each test
    (test/is (<= (time/since start) 120) "test took longer than 120 seconds"))

  (cleanup [this]
    (doseq [raft @rafts]
      (when (some? raft)
        (raft/kill raft)))
    (lab-rpc/cleanup net)
    (check-timeout this))

  ;; attach server i to the net.
  (connect [_ i]
    (swap! connected assoc i true)
    ;; outgoing ClientEnds
    (dotimes [j n]
      (when (get @connected j)
        (let [end-name (get-in @end-names [i j])]
          (lab-rpc/enable! net end-name true))))
    ;; incoming ClientEnds
    (dotimes [j n]
      (when (get @connected j)
        (let [end-name (get-in @end-names [j i])]
          (lab-rpc/enable! net end-name true)))))

  ;; detach server i from the net.
  (disconnect [_ i]
    (swap! connected assoc i false)
    ;; outgoing ClientEnds
    (dotimes [j n]
      (when (some? (get @end-names i))
        (let [end-name (get-in @end-names [i j])]
          (lab-rpc/enable! net end-name false))))
    ;; incoming ClientEnds
    (dotimes [j n]
      (when (some? (get @end-names j))
        (let [end-name (get-in @end-names [j i])]
          (lab-rpc/enable! net end-name false)))))

  (rpc-count [_ server]
    (lab-rpc/get-count net server))

  (rpc-total [_]
    (lab-rpc/total-count net))

  (unreliable! [_ unreliable]
    (lab-rpc/reliable! net (not unreliable)))

  (long-reordering! [_ long-reordering]
    (lab-rpc/long-reordering! net long-reordering))

  (bytes-total [_]
    (lab-rpc/get-total-bytes net))

  ;; check that there's exactly one leader.
  ;; try a few times in case re-elections are needed.
  ;; check that everyone agrees on the term.
  (check-one-leader [_]
    (let [leader (loop [iteration 10]
                   (when (pos? iteration)
                     (let [ms                    (+ 450 (rand-int 100))
                           _                     (Thread/sleep ms)
                           leaders               (reduce (fn [m i]
                                                           (if (get @connected i)
                                                             (let [[term leader] (raft/get-state (get @rafts i))]
                                                               (if leader
                                                                 (update m term (comp vec conj) i)
                                                                 m))
                                                             m))
                                                         {}
                                                         (range n))
                           last-term-with-leader (when-let [terms (keys leaders)]
                                                   (apply max terms))
                           _                     (doseq [[term leaders] leaders]
                                                   (test/is (<= (count leaders) 1)
                                                            (format "term %d has %d (>1) leaders", term, (count leaders))))]
                       (if (pos? (count leaders))
                         (get-in leaders [last-term-with-leader 0])
                         (recur (dec iteration))))))]
      (test/is (some? leader) "expected one leader, got none")
      leader))

  (check-terms [_]
    (let [terms (->> (for [i     (range n)
                           :when (get @connected i)]
                       (raft/get-state (get @rafts i)))
                     (map first))]
      (test/is (apply = terms) "servers disagree on term")
      (if (pos? (count terms))
        (first terms)
        -1)))

  ;; check that there's no leader
  (check-no-leader [_]
    (let [leader-claims (for [i     (range n)
                              :when (get @connected i)
                              :let  [[_ is-leader] (raft/get-state (get @rafts i))]]
                          is-leader)]
      (test/is (every? false? leader-claims)
               (let [idx (->> leader-claims
                              (keep-indexed #(when (false? %2) %1))
                              first)]
                 (format "expected no leader, but %d claims to be leader" idx)))))

  ;; how many servers think a log entry is committed?
  (n-committed [_ index]
    (loop [i 0 cnt 0 cmd nil]
      (if (< i (count @rafts))
        (let [err-msg (get @apply-err i)]
          (test/is (string/blank? err-msg)
                   err-msg)
          (monitor-enter mu)
          (let [cmd1 (get-in @logs [i index])]
            (monitor-exit mu)
            (if (some? cmd1)
              (do
                (test/is (or (zero? cnt)
                             (= cmd cmd1))
                         (format "committed values do not match: index %d, %s, %s"
                                 index
                                 (pr-str cmd)
                                 (pr-str cmd1)))
                (recur (inc i) (inc cnt) cmd1))
              (recur (inc i) cnt cmd))))
        [cnt cmd])))

  ;; wait for at least n servers to commit.
  ;; but don't wait forever.
  (wait [this index n start-term]
    (let [result (loop [iters 0 to 10]
                   (if (< iters 30)
                     (let [[nd _] (n-committed this index)]
                       (if (<= n nd)
                         :break
                         (let [_  (Thread/sleep to)
                               to (if (< to 1000)
                                    (* 2 to)
                                    to)]
                           (if (< -1 start-term)
                             (if (some (fn [[t _]]
                                         (< start-term t))
                                       (map raft/get-state @rafts))
                               ;; someone has moved on
                               ;; can no longer guarantee that we'll "win"
                               nil
                               (recur (inc iters) to))
                             (recur (inc iters) to)))))
                     :break))]
      (if (nil? result)
        nil
        (let [[nd cmd] (n-committed this index)]
          (test/is (<= n nd) (format "only %d decided for index %d; wanted %d" nd index n))
          cmd))))

  ;; do a complete agreement.
  ;; it might choose the wrong leader initially,
  ;; and have to re-submit after giving up.
  ;; entirely gives up after about 10 seconds.
  ;; indirectly checks that the servers agree on the
  ;; same value, since nCommitted() checks this,
  ;; as do the threads that read from applyCh.
  ;; returns index.
  ;; if retry==true, may submit the command multiple
  ;; times, in case a leader fails just after Start().
  ;; if retry==false, calls Start() only once, in order
  ;; to simplify the early Lab 2B tests.
  (one [this cmd expected-servers retry]
    (let [t0    (time/now)
          index (loop [starts 0]
                  (when (< (time/since t0) 10)
                    (let [[index starts] (loop [si 0 starts starts]
                                           (if (< si n)
                                             (let [starts (mod (inc starts) n)]
                                               (if-let [rf (when (get @connected starts)
                                                             (get @rafts starts))]
                                                 (let [[index1 _ ok] (raft/start rf cmd)]
                                                   (if ok
                                                     [index1 starts]
                                                     (recur (inc si) starts)))
                                                 (recur (inc si) starts)))
                                             [nil starts]))]
                      (if (some? index)
                        (let [t1    (time/now)
                              index (loop []
                                      (when (< (time/since t1) 2)
                                        (let [[nd cmd1] (n-committed this index)]
                                          (if (and (< 0 nd)
                                                   (<= expected-servers nd)
                                                   (= cmd1 cmd))
                                            index
                                            (do
                                              (Thread/sleep 20)
                                              (recur))))))]
                          (if (some? index)
                            index
                            (when retry
                              (recur starts))))
                        (do
                          (Thread/sleep 50)
                          (recur starts))))))]
      (test/is (some? index) (format "one(%s) failed to reach agreement" (pr-str cmd)))
      index))

  ;; start a Test.
  ;; print the Test message.
  ;; e.g. (begin "Test (2B): RPC counts aren't too high")
  (begin [this description]
    (log/infof "%s ..." description)
    (reset! t0 (time/now))
    (reset! rpcs0 (rpc-total this))
    (reset! bytes0 (bytes-total this))
    (reset! cmds0 0)
    (reset! max-index0 @max-index))

  ;; end a Test -- the fact that we got here means there
  ;; was no failure.
  ;; print the Passed message,
  ;; and some performance numbers.
  (end [this]
    (check-timeout this)
    (when (or (nil? test/*report-counters*)
              (zero? (:fail @test/*report-counters*)))
      (monitor-enter mu)
      (let [t       (time/since @t0)
            n-peers n
            n-rpc   (- (rpc-total this) @rpcs0)
            n-cmds  (- @max-index @max-index0)]
        (monitor-exit mu)
        (log/infof "  ... Passed --  %4.1f  %d %4d %4d" t n-peers n-rpc n-cmds)))))

(def ncpu-once (sync/once))

(defn make-config
  [n unreliable]
  (sync/do ncpu-once
           (fn []
             (when (< (.availableProcessors (Runtime/getRuntime)) 2)
               (log/info "warning: only one CPU, which may conceal locking bugs"))
             ;; NOTE: Unlike in config.go, we don't set the random seed here.
             ))
  (let [cfg (map->Config {:mu         (Object.)
                          :net        (lab-rpc/make-network)
                          :n          n
                          :rafts      (atom [])
                          :apply-err  (atom (vec (repeat n nil)))
                          :connected  (atom [])
                          :saved      (atom [])
                          :end-names  (atom [])
                          :logs       (atom [])
                          :start      (time/now)
                          :t0         (atom 0)
                          :rpcs0      (atom 0)
                          :bytes0     (atom 0)
                          :cmds0      (atom 0)
                          :max-index  (atom 0)
                          :max-index0 (atom 0)})]
    (unreliable! cfg unreliable)
    (lab-rpc/long-delays! (:net cfg) true)
    (dotimes [i (:n cfg)]
      (swap! (:logs cfg) conj {})
      (start1 cfg i))
    (dotimes [i (:n cfg)]
      (connect cfg i))
    cfg))
