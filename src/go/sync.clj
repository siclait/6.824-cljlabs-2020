(ns go.sync
  "A reimplemntation of a subset of Golang's \"sync\" package."
  (:refer-clojure :exclude [do])
  (:require
   [clojure.core.async :as a]))

(defprotocol Waitable
  (wait! [this]
    "Blocks until the wait is released."))

(defprotocol IWaitGroup
  (add [this delta]
    "Adds a delta, which may be negative, to the WaitGroup counter.")
  (done [this]
    "Decrements the WaitGroup counter by one."))

(defn- add*
  [x n done-ch]
  (let [x* (if (nil? x)
             1
             (+ x n))]
    (when (zero? x*)
      (a/close! done-ch))
    x*))

(defn- dec*
  [x done-ch]
  (let [x* (dec x)]
    (when (zero? x*)
      (a/close! done-ch))
    x*))

(defrecord WaitGroup [count done-ch]
  IWaitGroup
  (add [_ n]
    (swap! count add* n done-ch))
  (done [_]
    (swap! count dec* done-ch))

  Waitable
  (wait! [_]
    (a/<!! done-ch)))

(defn wait-group
  "Waits for a collection of async operations to finish.

  The main thread calls `add` to set the number of operations to
  wait for. Then each async operation calls `done` when finished.
  Simultaneously `wait!` can be used to block until all async operations
  have finished."
  []
  (->WaitGroup (atom nil) (a/chan)))

(defprotocol ICond
  (broadcast [this])
  #_(signal [this]))

(defrecord Cond [done-ch]
  ICond
  (broadcast [_]
    (a/close! @done-ch)
    (reset! done-ch (a/chan)))
  #_(signal [this])

  Waitable
  (wait! [_]
    (a/<!! @done-ch)))

(defn new-cond
  []
  (->Cond (atom (a/chan))))

(defprotocol IOnce
  (do [this f]))

(defrecord Once [done]
  IOnce
  (do [_ f]
      (when (compare-and-set! done false true)
        (f))))

(defn once
  []
  (->Once (atom false)))
