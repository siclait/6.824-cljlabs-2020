(ns map-reduce.impl.master
  "Write your code here."
  (:require
   [go.net :as net]
   [go.rpc :as rpc]))

(def port 3000)
(def output-directory "mr-tmp")

(defprotocol IMaster
  (Example [this args]
    "Example RPC handler."))

(defrecord Master []
  IMaster
  (Example [this args]
    {:y (inc (:x args))}))

(defn master
  []
  (->Master))

(defn start-server
  "Start a thread that listens for RPCs from worker.clj"
  [master]
  (let [server (rpc/new-server)
        _      (rpc/register server master)
        _      (rpc/handle-http server)
        l      (net/listen port)]
    (rpc/serve server l)
    server))

(defn done
  "map-reduce.master calls `done` periodically to find out
  if the entire job has finished."
  [master]
  false)

(defn make-master
  "Create a Master.

  map-reduce.master calls this function.
  `n-reduce` is the number of reduce tasks to use."
  [files n-reduce]
  (let [m (master)
        _ (start-server m)]
    m))
