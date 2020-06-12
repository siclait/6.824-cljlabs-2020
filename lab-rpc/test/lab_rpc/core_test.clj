(ns lab-rpc.core-test
  (:require [clojure.core.async :as async]
            [clojure.string :as string]
            [clojure.test :refer [deftest is]]
            [go.time :as time]
            [lab-rpc.core :as lab-rpc]
            [taoensso.timbre :as log]))

(defn- parse-int
  [^String s]
  (Integer/parseInt s))

(defn- seconds
  [n]
  (* 1000 n))

(defprotocol IJunkServer
  (Handler1 [this args])
  (Handler2 [this args])
  (Handler3 [this args])
  (Handler4 [this args])
  (Handler5 [this args])
  (Handler6 [this args])
  (Handler7 [this args]))

(defrecord JunkServer [mu log1 log2]
  IJunkServer
  (Handler1 [_ args]
    (locking mu
      (swap! log1 conj args)
      (parse-int args)))

  (Handler2 [_ args]
    (locking mu
      (swap! log2 conj args)
      (format "handler2-%d" args)))

  (Handler3 [_ args]
    (locking mu
      (Thread/sleep (seconds 20))
      (- args)))

  (Handler4 [_ args]
    {:x "pointer"})

  (Handler5 [_ args]
    {:x "no pointer"})

  (Handler6 [_ args]
    (count args))

  (Handler7 [_ args]
    (-> (repeat args "y")
        string/join)))

(defn junk-server []
  (->JunkServer (Object.) (atom []) (atom [])))

(deftest basic-test
  (let [rn  (lab-rpc/make-network)
        e   (lab-rpc/make-end! rn "end1-99")
        js  (junk-server)
        svc (lab-rpc/make-service js)
        rs  (lab-rpc/make-server)
        _   (lab-rpc/add-service! rs svc)
        _   (lab-rpc/add-server! rn "server99" rs)
        _   (lab-rpc/connect! rn "end1-99" "server99")
        _   (lab-rpc/enable! rn "end1-99" true)]
    (is (= "handler2-111"
           (lab-rpc/call! e :JunkServer/Handler2 111)))
    (is (= 9099
           (lab-rpc/call! e :JunkServer/Handler1 "9099")))
    (lab-rpc/cleanup rn)))

(deftest types-test
  (let [rn  (lab-rpc/make-network)
        e   (lab-rpc/make-end! rn "end1-99")
        js  (junk-server)
        svc (lab-rpc/make-service js)
        rs  (lab-rpc/make-server)
        _   (lab-rpc/add-service! rs svc)
        _   (lab-rpc/add-server! rn "server99" rs)
        _   (lab-rpc/connect! rn "end1-99" "server99")
        _   (lab-rpc/enable! rn "end1-99" true)]
    (is (= "pointer"
           (-> (lab-rpc/call! e :JunkServer/Handler4 {})
               :x)))
    (is (= "no pointer"
           (-> (lab-rpc/call! e :JunkServer/Handler5 {})
               :x)))
    (lab-rpc/cleanup rn)))

; does net.Enable(endname, false) really disconnect a client?
(deftest disconnect-test
  (let [rn  (lab-rpc/make-network)
        e   (lab-rpc/make-end! rn "end1-99")
        js  (junk-server)
        svc (lab-rpc/make-service js)
        rs  (lab-rpc/make-server)
        _   (lab-rpc/add-service! rs svc)
        _   (lab-rpc/add-server! rn "server99" rs)
        _   (lab-rpc/connect! rn "end1-99" "server99")]
    (is (nil? (lab-rpc/call! e :JunkServer/Handler2 111)))
    (lab-rpc/enable! rn "end1-99" true)
    (is (= 9099
           (lab-rpc/call! e :JunkServer/Handler1 "9099")))
    (lab-rpc/cleanup rn)))

; test net.GetCount()
(deftest counts-test
  (let [rn  (lab-rpc/make-network)
        e   (lab-rpc/make-end! rn "end1-99")
        js  (junk-server)
        svc (lab-rpc/make-service js)
        rs  (lab-rpc/make-server)
        _   (lab-rpc/add-service! rs svc)
        _   (lab-rpc/add-server! rn 99 rs)
        _   (lab-rpc/connect! rn "end1-99" 99)
        _   (lab-rpc/enable! rn "end1-99" true)]
    (dotimes [i 17]
      (is (= (format "handler2-%d" i)
             (lab-rpc/call! e :JunkServer/Handler2 i))))
    (is (= 17
           (lab-rpc/get-count rn 99)))
    (lab-rpc/cleanup rn)))

; test RPCs from concurrent ClientEnds
(deftest concurrent-many-test
  (let [rn        (lab-rpc/make-network)
        js        (junk-server)
        svc       (lab-rpc/make-service js)
        rs        (lab-rpc/make-server)
        _         (lab-rpc/add-service! rs svc)
        _         (lab-rpc/add-server! rn 1000 rs)
        ch        (async/chan)
        n-clients 20
        n-rpcs    10]
    (dotimes [i n-clients]
      (async/thread
        (let [e (lab-rpc/make-end! rn i)
              _ (lab-rpc/connect! rn i 1000)
              _ (lab-rpc/enable! rn i true)
              n (count (for [j (range n-rpcs)]
                         (let [arg (+ (* i 100) j)]
                           (is (= (format "handler2-%d" arg)
                                  (lab-rpc/call! e :JunkServer/Handler2 arg))))))]
          (async/put! ch n))))
    (let [total (reduce + (for [_ (range n-clients)]
                            (async/<!! ch)))]
      (is (= total
             (* n-clients n-rpcs)))
      (is (= total
             (lab-rpc/get-count rn 1000))))
    (lab-rpc/cleanup rn)))

(deftest unreliable-test
  (let [rn        (lab-rpc/make-network)
        _         (lab-rpc/reliable! rn false)
        js        (junk-server)
        svc       (lab-rpc/make-service js)
        rs        (lab-rpc/make-server)
        _         (lab-rpc/add-service! rs svc)
        _         (lab-rpc/add-server! rn 1000 rs)
        ch        (async/chan)
        n-clients 300]
    (dotimes [i n-clients]
      (async/thread
        (let [e     (lab-rpc/make-end! rn i)
              _     (lab-rpc/connect! rn i 1000)
              _     (lab-rpc/enable! rn i true)
              arg   (* i 100)
              reply (lab-rpc/call! e :JunkServer/Handler2 arg)
              n     (if (some? reply) 1 0)]
          (when (some? reply)
            (is (= (format "handler2-%d" arg)
                   reply)))
          (async/put! ch n))))
    (let [total (reduce + (for [_ (range n-clients)]
                            (async/<!! ch)))]
      (is (and (not= total n-clients)
               (pos? total))))
    (lab-rpc/cleanup rn)))

(deftest bytes-test
  (let [rn  (lab-rpc/make-network)
        e   (lab-rpc/make-end! rn "end1-99")
        js  (junk-server)
        svc (lab-rpc/make-service js)
        rs  (lab-rpc/make-server)
        _   (lab-rpc/add-service! rs svc)
        _   (lab-rpc/add-server! rn 99 rs)
        _   (lab-rpc/connect! rn "end1-99" 99)
        _   (lab-rpc/enable! rn "end1-99" true)]

    (dotimes [_ 16]
      (let [args   "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
            args   (str args args)
            args   (str args args)
            reply  (lab-rpc/call! e :JunkServer/Handler6 args)
            wanted (count args)]
        (is (= wanted reply) (format "wrong reply %d from Handler6, expecting %d" reply wanted))))

    (let [n (lab-rpc/get-total-bytes rn)]
      (is (and (<= 4828 n)
               (<= n 6000)) (format "wrong get-total-bytes %d, expected about 5000" n))

      (dotimes [_ 16]
        (let [args   107
              reply  (lab-rpc/call! e :JunkServer/Handler7 args)
              wanted args]
          (is (= wanted (count reply)) (format "wrong reply len=%d from Handler7, expecting %d" (count reply) wanted))))

      (let [nn (- (lab-rpc/get-total-bytes rn) n)]
        (is (and (<= 1800 nn)
                 (<= nn 2500)) (format "wrong get-total-bytes %d, expected about 2000" nn))))

    (lab-rpc/cleanup rn)))

; test concurrent RPCs from a single ClientEnd
(deftest concurrent-one-test
  (let [rn     (lab-rpc/make-network)
        js     (junk-server)
        svc    (lab-rpc/make-service js)
        rs     (lab-rpc/make-server)
        _      (lab-rpc/add-service! rs svc)
        _      (lab-rpc/add-server! rn 1000 rs)
        e      (lab-rpc/make-end! rn "c")
        _      (lab-rpc/connect! rn "c" 1000)
        _      (lab-rpc/enable! rn "c" true)
        ch     (async/chan)
        n-rpcs 20]
    (dotimes [i n-rpcs]
      (async/thread
        (let [arg (+ 100 i)]
          (is (= (format "handler2-%d" arg)
                 (lab-rpc/call! e :JunkServer/Handler2 arg)))
          (async/put! ch 1))))
    (let [total (reduce + (for [_ (range n-rpcs)]
                            (async/<!! ch)))]
      (is (= total n-rpcs))
      (is (= n-rpcs
             (count @(:log2 js))))
      (is (= total
             (lab-rpc/get-count rn 1000))))
    (lab-rpc/cleanup rn)))

; regression: an RPC that's delayed during Enabled=false
; should not delay subsequent RPCs (e.g. after Enabled=true).
(deftest regression1-test
  (let [rn     (lab-rpc/make-network)
        js     (junk-server)
        svc    (lab-rpc/make-service js)
        rs     (lab-rpc/make-server)
        _      (lab-rpc/add-service! rs svc)
        _      (lab-rpc/add-server! rn 1000 rs)
        e      (lab-rpc/make-end! rn "c")
        _      (lab-rpc/connect! rn "c" 1000)
                                        ; start some RPCs while the ClientEnd is disabled.
                                        ; they'll be delayed.
        _      (lab-rpc/enable! rn "c" false)
        ch     (async/chan)
        n-rpcs 20]

    (dotimes [i n-rpcs]
      (async/thread
        (let [arg (+ 100 i)]
          (lab-rpc/call! e :JunkServer/Handler2 arg)
          (async/put! ch true))))

    (Thread/sleep 100)

    (let [t0  (System/nanoTime)
          _   (lab-rpc/enable! rn "c" true)
          arg 99]
      (is (= (format "handler2-%d" arg)
             (lab-rpc/call! e :JunkServer/Handler2 arg)))
      (let [dur (time/since t0)]
        (is (< dur 0.03))))

    (dotimes [_ n-rpcs]
      (async/<!! ch))

    (is (= 1 (count @(:log2 js))))
    (is (= 1 (lab-rpc/get-count rn 1000)))

    (lab-rpc/cleanup rn)))

; if an RPC is stuck in a server, and the server
; is killed with DeleteServer(), does the RPC
; get un-stuck?
(deftest killed-test
  (let [rn      (lab-rpc/make-network)
        e       (lab-rpc/make-end! rn "end1-99")
        js      (junk-server)
        svc     (lab-rpc/make-service js)
        rs      (lab-rpc/make-server)
        _       (lab-rpc/add-service! rs svc)
        _       (lab-rpc/add-server! rn "server-99" rs)
        _       (lab-rpc/connect! rn "end1-99" "server-99")
        _       (lab-rpc/enable! rn "end1-99" true)
        done-ch (async/chan)]

    (async/thread
      (let [ok (some? (lab-rpc/call! e :JunkServer/Handler3 99))]
        (async/put! done-ch ok)))

    (Thread/sleep 1000)

    (let [done (async/alt!!
                 done-ch true
                 (async/timeout 100) nil)]
      (is (nil? done)))

    (lab-rpc/delete-server! rn "server-99")

    (let [reply (async/alt!!
                  done-ch ([r] r)
                  (async/timeout 100) :timed-out)]
      (is (not= :timed-out reply))
      (is (false? reply)))

    (lab-rpc/cleanup rn)))

(deftest benchmark-test
  (let [rn  (lab-rpc/make-network)
        e   (lab-rpc/make-end! rn "end1-99")
        js  (junk-server)
        svc (lab-rpc/make-service js)
        rs  (lab-rpc/make-server)
        _   (lab-rpc/add-service! rs svc)
        _   (lab-rpc/add-server! rn "server-99" rs)
        _   (lab-rpc/connect! rn "end1-99" "server-99")
        _   (lab-rpc/enable! rn "end1-99" true)
        t0  (System/nanoTime)
        n   100000]
    (dotimes [_ n]
      (is (= "handler2-111"
             (lab-rpc/call! e :JunkServer/Handler2 111))))
    (log/infof "%fs for %d" (time/since t0) n)
    (lab-rpc/cleanup rn)))
