(ns map-reduce.plugin.wc
  "A word-count application \"plugin\" for MapReduce."
  (:require [clojure.string :as string]
            [map-reduce.plugin :as plugin]))

(defn mapf
  [_ contents]
  (for [word (string/split contents #"[^a-zA-Z]+")]
    {:key word :value "1"}))

(defn reducef
  [_ vs]
  (str (count vs)))

(defmethod plugin/load-plugin :wc [_]
  {:mapf    mapf
   :reducef reducef})
