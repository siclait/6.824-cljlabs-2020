(ns raft.core-test
  "A translation of the tests for MIT's 6.824 Lab 2 from Go."
  (:require [clojure.core.async :as async]
            [clojure.set :as set]
            [clojure.test :refer [deftest is]]
            [go.core :refer [defer]]
            [go.sync :as sync]
            [raft.config :as config]
            [raft.core :as raft]))

;; The tester generously allows solutions to complete elections in one second
;; (much more than the paper's range of timeouts).
(def ^:const raft-election-timeout 1000)

(def rand-big-int #(rand-int Integer/MAX_VALUE))

(deftest initial-election-2A-test
  (let [servers 3
        cfg     (config/make-config servers false)]
    (defer (config/cleanup cfg)
      (let [_ (config/begin cfg "Test (2A): initial election")

            ;; is a leader elected?
            _ (config/check-one-leader cfg)

            ;; sleep a bit to avoid racing with followers learning of the
            ;; election, then check that all peers agree on the term.
            _     (Thread/sleep 50)
            term1 (config/check-terms cfg)
            _     (is (<= 1 term1) (format "term is %d, but should be at least 1" term1))

            ;; does the leader+term stay the same if there is no network failure?
            _     (Thread/sleep (* 2 raft-election-timeout))
            term2 (config/check-terms cfg)]
        (is (= term1 term2) "warning: term changed even though there were no failures")
        (config/check-one-leader cfg)
        (config/end cfg)))))

(deftest re-election-2A-test
  (let [servers 3
        cfg     (config/make-config servers false)]
    (defer (config/cleanup cfg)
      (let [_       (config/begin cfg "Test (2A): election after network failure")
            leader1 (config/check-one-leader cfg)

            ;; if the leader disconnects, a new one should be elected.
            _ (config/disconnect cfg leader1)
            _ (config/check-one-leader cfg)

            ;; if the old leader rejoins, that shouldn't
            ;; disturb the new leader.
            _       (config/connect cfg leader1)
            leader2 (config/check-one-leader cfg)];; if there's no quorum, no leader should
        ;; be elected.
        (config/disconnect cfg leader2)
        (config/disconnect cfg (mod (inc leader2) servers))
        (Thread/sleep (* 2 raft-election-timeout))
        (config/check-no-leader cfg)

        ;; if a quorum arises, it should elect a leader.
        (config/connect cfg (mod (inc leader2) servers))
        (config/check-one-leader cfg)

        ;; re-join of last node shouldn't prevent leader from existing.
        (config/connect cfg leader2)
        (config/check-one-leader cfg)
        (config/end cfg)))))

(deftest basic-agree-2B-test
  (let [servers 3
        cfg     (config/make-config servers false)]

    (defer (config/cleanup cfg)
      (let [_ (config/begin cfg "Test (2B): basic agreement")

            iters 3]
        (doseq [index (range 1 (inc iters))]
          (let [[nd _]  (config/n-committed cfg index)
                _       (is (zero? nd) "some have committed before Start()")
                x-index (config/one cfg (* 100 index) servers false)
                _       (is (= index x-index) (format "got index %s but expected %d" x-index index))]))
        (config/end cfg)))))

;; check, based on counting bytes of RPCs, that
;; each command is sent to each peer just once.
(deftest rpc-bytes-2B-test
  (let [servers 3
        cfg     (config/make-config servers false)]
    (defer (config/cleanup cfg)
      (let [_ (config/begin cfg "Test (2B): RPC byte count")

            _      (config/one cfg 99 servers false)
            bytes0 (config/bytes-total cfg)

            iters 10
            sent  (reduce + (for [idx (range 2 (+ iters 2))]
                              (let [cmd     (config/rand-string 5000)
                                    x-index (config/one cfg cmd servers false)]
                                (is (= idx x-index) (format "got index %d but expected %d" x-index idx))
                                (count cmd))))

            bytes1   (config/bytes-total cfg)
            got      (- bytes1 bytes0)
            expected (* servers sent)]
        (is (<= got (+ expected 50000)) (format "too many RPC bytes; got %d, expected %d" got expected))

        (config/end cfg)))))

(deftest fail-agree-2B-test
  (let [servers 3
        cfg     (config/make-config servers false)]
    (defer (config/cleanup cfg)
      (let [_ (config/begin cfg "Test (2B): agreement despite follower disconnection")

            _ (config/one cfg 101 servers false)

            leader (config/check-one-leader cfg)]
        ;; disconnect one follower from the network.
        (config/disconnect cfg (mod (inc leader) servers))

        ;; the leader and remaining follower should be
        ;; able to agree despite the disconnected follower.
        (config/one cfg 102 (dec servers) false)
        (config/one cfg 103 (dec servers) false)
        (Thread/sleep raft-election-timeout)
        (config/one cfg 104 (dec servers) false)
        (config/one cfg 105 (dec servers) false)

        ;; re-connect
        (config/connect cfg (mod (inc leader) servers))

        ;; the full set of servers should preserve
        ;; previous agreements, and be able to agree
        ;; on new commands.
        (config/one cfg 106 servers true)
        (Thread/sleep raft-election-timeout)
        (config/one cfg 107 servers true)
        (config/end cfg)))))

(deftest fail-no-agree-2B-test
  (let [servers 5
        cfg     (config/make-config servers false)]
    (defer (config/cleanup cfg)
      (let [_ (config/begin cfg "Test (2B): no agreement if too many followers disconnect")

            _ (config/one cfg 10 servers false)

            ;; 3 of 5 followers disconnect
            leader (config/check-one-leader cfg)
            _      (config/disconnect cfg (mod (+ 1 leader) servers))
            _      (config/disconnect cfg (mod (+ 2 leader) servers))
            _      (config/disconnect cfg (mod (+ 3 leader) servers))

            [index _ ok] (raft/start (get @(:rafts cfg) leader) 20)
            _            (is ok "leader rejected Start()")
            _            (is (= 2 index) (format "expected index 2, got %d" index))

            _ (Thread/sleep (* 2 raft-election-timeout))

            [n _] (config/n-committed cfg index)
            _     (is (<= n 0) (format "%d committed but no majority" n))

            ;; repair
            _ (config/connect cfg (mod (+ 1 leader) servers))
            _ (config/connect cfg (mod (+ 2 leader) servers))
            _ (config/connect cfg (mod (+ 3 leader) servers))

            ;; the disconnected majority may have chosen a leader from
            ;; among their own ranks, forgetting index 2.
            leader2        (config/check-one-leader cfg)
            [index2 _ ok2] (raft/start (get @(:rafts cfg) leader2) 30)]
        (is ok2 "leader rejected Start()")

        (is (and (>= index2 2)
                 (<= index2 3)) (format "unexpected index %d" index))

        (config/one cfg 1000 servers true)
        (config/end cfg)))))

(deftest concurrent-starts-2b-test
  (let [servers 3
        cfg     (config/make-config servers false)

        _       (config/begin cfg "Test (2B): concurrent Start()s")
        success (loop []
                  (let [result (loop [try 0]
                                 (when (< try 5)
                                   (when (< 0 try)
                                     ;; give solution some time to settle
                                     (Thread/sleep (* 3 1000)))

                                   (let [leader      (config/check-one-leader cfg)
                                         [_ term ok] (-> (get @(:rafts cfg) leader)
                                                         (raft/start 1))]
                                     (if (not ok)
                                       ;; leader moved on really quickly
                                       (recur (inc try))
                                       (let [iters 5
                                             wg    (sync/wait-group)
                                             is-ch (async/chan iters)]
                                         (dotimes [i iters]
                                           (sync/add wg 1)
                                           (async/thread
                                             (let [[i term1 ok] (-> (get @(:rafts cfg) leader)
                                                                    (raft/start (+ 100 i)))]
                                               (when (and (= term1 term)
                                                          ok)
                                                 (async/put! is-ch i))
                                               (sync/done wg))))

                                         (sync/wait! wg)
                                         (async/close! is-ch)

                                         (let [result (reduce (fn [_ j]
                                                                (let [[t _] (-> (get @(:rafts cfg) j)
                                                                                (raft/get-state))]
                                                                  (when (not= t term)
                                                                    (reduced :continue))))
                                                              nil
                                                              (range servers))]
                                           (if (= :continue result)
                                             :continue
                                             (let [[cmds failed] (loop [cmds [] failed false]
                                                                   (let [index (async/<!! is-ch)]
                                                                     (if index
                                                                       (let [cmd (config/wait cfg index servers term)]
                                                                         (cond
                                                                           (nil? cmd) [cmds true]
                                                                           (int? cmd) (recur (conj cmds cmd) failed)
                                                                           :else      (do
                                                                                        (is (int? cmd) (format "value %s is not an int" (pr-str cmd)))
                                                                                        [cmds true])))
                                                                       [cmds failed])))]
                                               (if failed
                                                 (recur (inc try))
                                                 (let [[x ok] (reduce (fn [_ i]
                                                                        (let [x  (+ 100 i)
                                                                              ok (some #(= x %) cmds)]
                                                                          (if (not ok)
                                                                            (reduced [x ok])
                                                                            [x ok])))
                                                                      nil
                                                                      (range iters))]
                                                   (is ok (format "cmd %d missing in %s" x (pr-str cmds)))
                                                   ;; This might not be a global counter across all tests, which would be wrong.
                                                   (zero? (if-let [counters clojure.test/*report-counters*]
                                                            (:fail @counters)
                                                            0))))))))))))]
                    (if (= :continue result)
                      (recur)
                      result)))]
    (is success "term changed too often")
    (config/end cfg)
    (config/cleanup cfg)))

(deftest rejoin-2B-test
  (let [servers 3
        cfg     (config/make-config servers false)

        _ (config/begin cfg "Test (2B): rejoin of partitioned leader")

        _ (config/one cfg 101 servers true)

        ;; leader network failure
        leader1 (config/check-one-leader cfg)
        _       (config/disconnect cfg leader1)

        ;; make old leader try to agree on some entries
        _ (-> (get @(:rafts cfg) leader1)
              (raft/start 102))
        _ (-> (get @(:rafts cfg) leader1)
              (raft/start 103))
        _ (-> (get @(:rafts cfg) leader1)
              (raft/start 104))

        ;; new leader commits, also for index=2
        _ (config/one cfg 103 2 true)

        ;; new leader network failure
        leader2 (config/check-one-leader cfg)
        _       (config/disconnect cfg leader2)

        ;; old leader connected again
        _ (config/connect cfg leader1)

        _ (config/one cfg 104 2 true)

        ;; all together now
        _ (config/connect cfg leader2)

        _ (config/one cfg 105 servers true)]
    (config/end cfg)
    (config/cleanup cfg)))

(deftest backup-2b-test
  (let [servers 5
        cfg     (config/make-config servers false)

        _ (config/begin cfg "Test (2B): leader backs up quickly over incorrect follower logs")

        _ (config/one cfg (rand-big-int) servers true)

        ;; put leader and one follower in a partition
        leader1 (config/check-one-leader cfg)
        _       (config/disconnect cfg (mod (+ 2 leader1) servers))
        _       (config/disconnect cfg (mod (+ 3 leader1) servers))
        _       (config/disconnect cfg (mod (+ 4 leader1) servers))

        ;; submit lots of commands that won't commit
        _ (dotimes [_ 50]
            (-> (get @(:rafts cfg) leader1)
                (raft/start (rand-big-int))))

        _ (Thread/sleep (/ raft-election-timeout 2))

        _ (config/disconnect cfg (mod (+ 0 leader1) servers))
        _ (config/disconnect cfg (mod (+ 1 leader1) servers))

        ;; allow other partition to recover
        _ (config/connect cfg (mod (+ 2 leader1) servers))
        _ (config/connect cfg (mod (+ 3 leader1) servers))
        _ (config/connect cfg (mod (+ 4 leader1) servers))

        ;; lots of successful commands to new group.
        _ (dotimes [_ 50]
            (config/one cfg (rand-big-int) 3 true))

        ;; now another partitioned leader and one follower
        leader2 (config/check-one-leader cfg)
        other   (mod (+ 2 leader1) servers)
        other   (if (= leader2 other)
                  (mod (inc leader2) servers)
                  other)
        _       (config/disconnect cfg other)

        ;; lots more commands that won't commit
        _ (dotimes [_ 50]
            (-> (get @(:rafts cfg) leader2)
                (raft/start (rand-big-int))))

        _ (Thread/sleep (/ raft-election-timeout 2))

        ;; bring original leader back to life,
        _ (dotimes [i servers]
            (config/disconnect cfg i))
        _ (config/connect cfg (mod (+ 0 leader1) servers))
        _ (config/connect cfg (mod (+ 1 leader1) servers))
        _ (config/connect cfg other)

        ;; lots of successful commands to new group.
        _ (dotimes [_ 50]
            (config/one cfg (rand-big-int) 3 true))

        ;; now everyone
        _ (dotimes [i servers]
            (config/connect cfg i))
        _ (config/one cfg (rand-big-int) servers true)]
    (config/end cfg)
    (config/cleanup cfg)))

(deftest count-2b-test
  (let [servers 3
        cfg     (config/make-config servers false)

        _ (config/begin cfg "Test (2B): RPC counts aren't too high")

        rpcs (fn []
               (->> (range servers)
                    (map #(config/rpc-count cfg %))
                    (reduce +)))

        _ (config/check-one-leader cfg)

        total1 (rpcs)

        _ (is (and (<= total1 30)
                   (<= 1 total1))
              (format "too many or few RPCs (%d) to elect initial leader" total1))

        [total2 success] (loop []
                           (let [result (loop [try 0]
                                          (when (< try 5)
                                            (when (< 0 try)
                                              ;; give solution some time to settle
                                              (Thread/sleep (* 3 1000)))

                                            (let [leader            (config/check-one-leader cfg)
                                                  total1            (rpcs)
                                                  iters             10
                                                  [start-i term ok] (-> (get @(:rafts cfg) leader)
                                                                        (raft/start 1))]
                                              (if (not ok)
                                                ;; leader moved on really quickly
                                                (recur (inc try))
                                                (let [[result cmds] (loop [i 1 cmds []]
                                                                      (if (< i (+ 2 iters))
                                                                        (let [x                 (rand-big-int)
                                                                              cmds              (conj cmds x)
                                                                              [index1 term1 ok] (-> (get @(:rafts cfg) leader)
                                                                                                    (raft/start x))]
                                                                          (if (not= term1 term)
                                                                            ;; term changed while starting
                                                                            [:continue cmds]
                                                                            ;; No longer the leader, so term has changed
                                                                            (if (not ok)
                                                                              [:continue cmds]
                                                                              (do
                                                                                (is (= (+ start-i i) index1) "Start() failed")
                                                                                (recur (inc i) cmds)))))
                                                                        [nil cmds]))]
                                                  (if (= :continue result)
                                                    :continue
                                                    (let [result (loop [i 1]
                                                                   (when (< i (inc iters))
                                                                     (let [cmd      (config/wait cfg (+ start-i i) servers term)
                                                                           expected (get cmds (dec i))]
                                                                       (if (nil? cmd)
                                                                         ;; term changed - try again
                                                                         :continue
                                                                         (if (= cmd expected)
                                                                           (recur (inc i))
                                                                           [cmd (+ start-i i) expected])))))]
                                                      (if (= :continue result)
                                                        :continue
                                                        (do
                                                          (when (not (nil? result))
                                                            (let [[cmd index expected] result]
                                                              (is (= cmd expected)
                                                                  (format "wrong value %d committed for index %d; expected %d" cmd index expected))))
                                                          (let [[total2 failed] (loop [j 0 total2 0]
                                                                                  (if (< j servers)
                                                                                    (let [[t _] (-> (get @(:rafts cfg) j)
                                                                                                    raft/get-state)]
                                                                                      (if (not= t term)
                                                                                        [total2 true]
                                                                                        (recur (inc j) (+ total2 (config/rpc-count cfg j)))))
                                                                                    [total2 false]))]
                                                            (if failed
                                                              :continue
                                                              (do
                                                                (is (<= (- total2 total1)
                                                                        (* 3 (+ iters 1 3)))
                                                                    (format "too many RPCs (%d) for %d entries" (- total2 total1) iters))
                                                                [total2 true]))))))))))))]
                             (if (= :continue result)
                               (recur)
                               result)))]
    (is success "term changed too often")

    (Thread/sleep raft-election-timeout)

    (let [total3 (rpcs)]
      (is (<= (- total3 total2) (* 3 20)) (format "too many RPCs (%d) for 1 second of idleness" (- total3 total2))))

    (config/end cfg)
    (config/cleanup cfg)))

(deftest persist1-2C-test
  (let [servers 3
        cfg     (config/make-config servers false)]
    (defer (config/cleanup cfg)
      (config/begin cfg "Test (2C): basic persistence")

      (config/one cfg 11 servers true)

      ;; crash and restart all
      (dotimes [i servers]
        (config/start1 cfg i))
      (dotimes [i servers]
        (config/disconnect cfg i)
        (config/connect cfg i))

      (config/one cfg 12 servers true)

      (let [leader1 (config/check-one-leader cfg)
            _       (config/disconnect cfg leader1)
            _       (config/start1 cfg leader1)
            _       (config/connect cfg leader1)])

      (config/one cfg 13 servers true)

      (let [leader2 (config/check-one-leader cfg)
            _       (config/disconnect cfg leader2)
            _       (config/one cfg 14 (dec servers) true)
            _       (config/start1 cfg leader2)
            _       (config/connect cfg leader2)])

      (config/wait cfg 4 servers -1)

      (let [i3 (-> (config/check-one-leader cfg)
                   inc
                   (mod servers))
            _  (config/disconnect cfg i3)
            _  (config/one cfg 15 (dec servers) true)
            _  (config/start1 cfg i3)
            _  (config/connect cfg i3)])

      (config/one cfg 16 servers true)

      (config/end cfg))))

(deftest persist2-2C-test
  (let [servers 5
        cfg     (config/make-config servers false)]
    (defer (config/cleanup cfg)
      (config/begin cfg "Test (2C): more persistence")

      (loop [index 1 iters 0]
        (when (< iters 5)
          (config/one cfg (+ 10 index) servers true)

          (let [index   (inc index)
                leader1 (config/check-one-leader cfg)]
            (config/disconnect cfg (mod (+ 1 leader1) servers))
            (config/disconnect cfg (mod (+ 2 leader1) servers))

            (config/one cfg (+ 10 index) (- servers 2) true)

            (let [index (inc index)]
              (config/disconnect cfg (mod (+ 0 leader1) servers))
              (config/disconnect cfg (mod (+ 3 leader1) servers))
              (config/disconnect cfg (mod (+ 4 leader1) servers))

              (config/start1 cfg (mod (+ 1 leader1) servers))
              (config/start1 cfg (mod (+ 2 leader1) servers))
              (config/connect cfg (mod (+ 1 leader1) servers))
              (config/connect cfg (mod (+ 2 leader1) servers))

              (Thread/sleep raft-election-timeout)

              (config/start1 cfg (mod (+ 3 leader1) servers))
              (config/connect cfg (mod (+ 3 leader1) servers))

              (config/one cfg (+ 10 index) (- servers 2) true)

              (config/connect cfg (mod (+ 4 leader1) servers))
              (config/connect cfg (mod (+ 0 leader1) servers))

              (recur (inc index) (inc iters))))))

      (config/one cfg 1000 servers true)

      (config/end cfg))))

(deftest persist3-2C-test
  (let [servers 3
        cfg     (config/make-config servers false)]
    (defer (config/cleanup cfg)
      (config/begin cfg "Test (2C): partitioned leader and one follower crash, leader restarts")

      (config/one cfg 101 3 true)

      (let [leader (config/check-one-leader cfg)]
        (config/disconnect cfg (mod (+ 2 leader) servers))

        (config/one cfg 102 2 true)

        (config/crash1 cfg (mod (+ 0 leader) servers))
        (config/crash1 cfg (mod (+ 1 leader) servers))
        (config/connect cfg (mod (+ 2 leader) servers))
        (config/start1 cfg (mod (+ 0 leader) servers))
        (config/connect cfg (mod (+ 0 leader) servers))

        (config/one cfg 103 2 true)

        (config/start1 cfg (mod (+ 1 leader) servers))
        (config/connect cfg (mod (+ 1 leader) servers))

        (config/one cfg 104 servers true))

      (config/end cfg))))

;; Test the scenarios described in Figure 8 of the extended Raft paper. Each
;; iteration asks a leader, if there is one, to insert a command in the Raft
;; log.  If there is a leader, that leader will fail quickly with a high
;; probability (perhaps without committing the command), or crash after a while
;; with low probability (most likey committing the command).  If the number of
;; alive servers isn't enough to form a majority, perhaps start a new server.
;; The leader in a new term may try to finish replicating log entries that
;; haven't been committed yet.
(deftest figure8-2C-test
  (let [servers 5
        cfg     (config/make-config servers false)]
    (defer (config/cleanup cfg)
      (config/begin cfg "Test (2C): Figure 8")

      (config/one cfg (rand-big-int) 1 true)

      (loop [nup servers iters 0]
        (when (< iters 1000)
          (let [leader (loop [leader -1 i 0]
                         (if (< i servers)
                           (if-let [rf (get @(:rafts cfg) i)]
                             (let [[_ _ ok] (raft/start rf (rand-big-int))]
                               (if ok
                                 (recur i (inc i))
                                 (recur leader (inc i))))
                             (recur leader (inc i)))
                           leader))]
            (if (< (mod (rand-big-int) 1000)
                   100)
              (let [ms (mod (rand-big-int)
                            (/ raft-election-timeout 2))]
                (Thread/sleep ms))
              (let [ms (mod (rand-big-int) 13)]
                (Thread/sleep ms)))

            (let [nup (if (not= leader -1)
                        (do
                          (config/crash1 cfg leader)
                          (dec nup))
                        nup)]
              (if (< nup 3)
                (let [s (mod (rand-big-int) servers)]
                  (if (nil? (get @(:rafts cfg) s))
                    (do
                      (config/start1 cfg s)
                      (config/connect cfg s)
                      (recur (inc nup) (inc iters)))
                    (recur nup (inc iters))))
                (recur nup (inc iters)))))))

      (dotimes [i servers]
        (when (nil? (get @(:rafts cfg) i))
          (config/start1 cfg i)
          (config/connect cfg i)))

      (config/one cfg (rand-big-int) servers true)

      (config/end cfg))))

(deftest unreliable-agree-2C-test
  (let [servers 5
        cfg     (config/make-config servers true)]
    (defer (config/cleanup cfg)
      (config/begin cfg "Test (2C): unreliable agreement")

      (let [wg (sync/wait-group)]
        (doseq [iters (range 1 50)]
          (dotimes [j 4]
            (sync/add wg 1)
            (async/thread
              (defer (sync/done wg)
                (config/one cfg (+ (* 100 iters) j) 1 true))))
          (config/one cfg iters 1 true))

        (config/unreliable! cfg false)
        (sync/wait! wg))

      (config/one cfg 100 servers true)

      (config/end cfg))))

(deftest figure8-unreliable-2C-test
  (let [servers 5
        cfg     (config/make-config servers true)]
    (defer (config/cleanup cfg)
      (config/begin cfg "Test (2C): Figure 8 (unreliable)")

      (config/one cfg (mod (rand-big-int) 10000) 1 true)

      (loop [nup servers iters 0]
        (when (< iters 1000)
          (when (= iters 200)
            (config/long-reordering! cfg true))
          (let [leader (loop [leader -1 i 0]
                         (if (< i servers)
                           (if-let [rf (get @(:rafts cfg) i)]
                             (let [[_ _ ok] (raft/start rf (mod (rand-big-int)
                                                                10000))]
                               (if (and ok (get @(:connected cfg) i))
                                 (recur i (inc i))
                                 (recur leader (inc i))))
                             (recur leader (inc i)))
                           leader))]
            (if (< (mod (rand-big-int) 1000)
                   100)
              (let [ms (mod (rand-big-int)
                            (/ raft-election-timeout 2))]
                (Thread/sleep ms))
              (let [ms (mod (rand-big-int) 13)]
                (Thread/sleep ms)))

            (let [nup (if (and (not= leader -1)
                               (< (mod (rand-big-int) 1000)
                                  (/ raft-election-timeout 2)))
                        (do
                          (config/disconnect cfg leader)
                          (dec nup))
                        nup)]
              (if (< nup 3)
                (let [s (mod (rand-big-int) servers)]
                  (if (not (get @(:connected cfg) s))
                    (do
                      (config/connect cfg s)
                      (recur (inc nup) (inc iters)))
                    (recur nup (inc iters))))
                (recur nup (inc iters)))))))

      (dotimes [i servers]
        (when (not (get @(:connected cfg) i))
          (config/connect cfg i)))

      (config/one cfg (mod (rand-big-int) 10000) servers true)

      (config/end cfg))))

(defn- internal-churn
  [unreliable]
  (let [servers 5
        cfg     (config/make-config servers unreliable)]
    (defer (config/cleanup cfg)
      (if unreliable
        (config/begin cfg "Test (2C): unreliable churn")
        (config/begin cfg "Test (2C): churn"))

      (let [stop (atom 0)
            cfn  (fn [me ch]
                   (let [values (atom [])]
                     (defer (async/put! ch @values)
                       (while (zero? @stop)
                         (let [x          (rand-big-int)
                               [index ok] (loop [index -1 ok false i 0]
                                            ;; try them all, maybe one of them is a leader
                                            (if (< i servers)
                                              (if-let [rf (get @(:rafts cfg) i)]
                                                (let [[index1 _ ok1] (raft/start rf x)]
                                                  (if ok1
                                                    (recur index1 ok1 (inc i))
                                                    (recur index ok (inc i))))
                                                (recur index ok (inc i)))
                                              [index ok]))]
                           (if ok
                             ;; maybe leader will commit our value, maybe not.
                             ;; but don't wait forever.
                             (loop [tos [10 20 50 100 200]]
                               (when (seq tos)
                                 (let [[nd cmd] (config/n-committed cfg index)]
                                   (if (< 0 nd)
                                     (when (= x cmd)
                                       (swap! values conj x))
                                     (do
                                       (Thread/sleep (first tos))
                                       (recur (rest tos)))))))
                             (Thread/sleep (* 79 me 17))))))))
            ncli 3
            cha  (doall (for [i (range ncli)]
                          (let [ch (async/chan)]
                            (async/thread
                              (cfn i ch))
                            ch)))]
        (dotimes [_ 20]
          (when (< (mod (rand-big-int) 1000) 200)
            (let [i (mod (rand-big-int) servers)]
              (config/disconnect cfg i)))

          (when (< (mod (rand-big-int) 1000) 500)
            (let [i (mod (rand-big-int) servers)]
              (when (nil? (get @(:rafts cfg) i))
                (config/start1 cfg i))
              (config/connect cfg i)))

          (when (< (mod (rand-big-int) 1000) 200)
            (let [i (mod (rand-big-int) servers)]
              (when (nil? (get @(:rafts cfg) i))
                (config/crash1 cfg i))))

          ;; Make crash/restart infrequent enough that the peers can often
          ;; keep up, but not so infrequent that everything has settled
          ;; down from one change to the next. Pick a value smaller than
          ;; the election timeout, but not hugely smaller.
          (Thread/sleep (/ (* raft-election-timeout 7) 10)))

        (Thread/sleep raft-election-timeout)

        (config/unreliable! cfg false)

        (dotimes [i servers]
          (when (nil? (get @(:rafts cfg) i))
            (config/start1 cfg i))
          (config/connect cfg i))

        (reset! stop 1)

        (let [values     (loop [i 0 values []]
                           (if (< i ncli)
                             (let [vs (async/<!! (nth cha i))]
                               (is (some? vs) "client failed")
                               (recur (inc i) (apply conj values vs)))
                             values))
              _          (Thread/sleep raft-election-timeout)
              last-index (config/one cfg (rand-big-int) servers true)
              really     (loop [index 1 really []]
                           (if (<= index last-index)
                             (let [v (config/wait cfg index servers -1)]
                               (if (some? v)
                                 (recur (inc index) (conj really v))
                                 (is false "not an int")))
                             really))]
          (is (empty? (set/difference (into #{} values)
                                      (into #{} really))) "didn't find a value")))
      (config/end cfg))))

(deftest reliable-churn-2C-test
  (internal-churn false))

(deftest unreliable-churn-2C-test
  (internal-churn true))

