(ns map-reduce.plugin.crash
  (:require
   [clojure.string :as string]
   [map-reduce.plugin :as plugin]))

(defn maybe-crash
  []
  (let [max 1000
        rr  (rand-int max)]
    (cond
      (< rr 330) (System/exit 1)
      (< rr 660) (let [max-ms (* 10 1000)
                       ms     (rand-int max-ms)]
                   (Thread/sleep ms)))))

(defn mapf
  [filename contents]
  (maybe-crash)

  [{:key "a" :value filename}
   {:key "b" :value (count filename)}
   {:key "c" :value (count contents)}
   {:key "d" :value "xyzzy"}])

(defn reducef
  [_ vs]
  (maybe-crash)

  (string/join " " (sort vs)))

(defmethod plugin/load-plugin :crash [_]
  {:mapf    mapf
   :reducef reducef})
