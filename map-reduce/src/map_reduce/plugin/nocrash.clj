(ns map-reduce.plugin.nocrash
  (:require
   [clojure.string :as string]
   [map-reduce.plugin :as plugin]))

(defn mapf
  [filename contents]
  [{:key "a" :value filename}
   {:key "b" :value (count filename)}
   {:key "c" :value (count contents)}
   {:key "d" :value "xyzzy"}])

(defn reducef
  [_ vs]
  (string/join " " (sort vs)))

(defmethod plugin/load-plugin :nocrash [_]
  {:mapf    mapf
   :reducef reducef})
