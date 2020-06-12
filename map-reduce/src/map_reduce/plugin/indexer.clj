(ns map-reduce.plugin.indexer
  (:require [clojure.string :as string]
            [map-reduce.plugin :as plugin]))

(defn mapf
  [document contents]
  (for [word (-> (string/split contents #"[^a-zA-Z]+")
                 distinct)]
    {:key word :value (last (string/split document #"/"))}))

(defn reducef
  [_ vs]
  (format "%d %s" (count vs) (string/join "," (sort vs))))

(defmethod plugin/load-plugin :indexer [_]
  {:mapf    mapf
   :reducef reducef})
