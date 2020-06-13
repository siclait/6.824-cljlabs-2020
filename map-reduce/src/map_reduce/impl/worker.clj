(ns map-reduce.impl.worker
  "Write your code here."
  (:require [go.fnv :as fnv]
            [go.rpc :as rpc]))

(def output-directory "mr-tmp")
(def port 3000)

(defn ihash
  "Use ihash(key) % `n-reduce` to choose the reduce
  task number for each key/value pair emitted by map."
  [s]
  (bit-and (fnv/fnv1a32 s) 0x7fffffff))

(defn call!
  [rpc-name args]
  (let [client   (rpc/dial port)
        response (rpc/call! client rpc-name args)]
    (rpc/close! client)
    response))

(defrecord Worker [mapf reducef])

(defn new-worker
  [mapf reducef]
  (map->Worker {:mapf    mapf
                :reducef reducef}))

(defn call-example!
  "Example function to show how to make an RPC call to the master."
  []
  ;; Send the RPC request, wait for the reply.
  (let [reply (call! :Master/Example {:x 99})]
    ;; reply.y should be 100.
    (println "reply.y:" (:y reply))))

(defn worker
  "map-reduce.worker calls this function."
  [mapf reducef]
  ;; Your worker implementation here.

  ;; Uncomment to send the Example RPC to the master.
  #_(call-example!))
