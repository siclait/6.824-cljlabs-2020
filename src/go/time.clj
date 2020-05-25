(ns go.time
  (:require [clojure.core.async :as a]))

(defprotocol ITimer
  (stop [this]))

(defrecord Timer [done-ch]
  ITimer
  (stop [_]
    (a/close! done-ch)))

(defn timer
  [done-ch]
  (map->Timer {:done-ch done-ch}))

(defn after-func
  [ms f]
  (let [done-ch (a/chan)]
    (a/thread
      (let [result (a/alt!!
                     done-ch nil
                     (a/timeout ms) :timeout)]
        (when (some? result)
          (f))))
    (timer done-ch)))

(defn since
  [t0]
  (/ (double (- (System/nanoTime) t0)) 1000000000.0))

(defn now
  []
  (System/nanoTime))
