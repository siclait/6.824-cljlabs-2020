(ns go.time
  (:require [clojure.core.async :as async]))

(defprotocol ITimer
  (stop [this]))

(defrecord Timer [done-ch]
  ITimer
  (stop [_]
    (async/close! done-ch)))

(defn timer
  [done-ch]
  (map->Timer {:done-ch done-ch}))

(defn after-func
  [ms f]
  (let [done-ch (async/chan)]
    (async/thread
      (let [result (async/alt!!
                     done-ch nil
                     (async/timeout ms) :timeout)]
        (when (some? result)
          (f))))
    (timer done-ch)))

(defn since
  [t0]
  (/ (double (- (System/nanoTime) t0)) 1000000000.0))

(defn now
  []
  (System/nanoTime))
