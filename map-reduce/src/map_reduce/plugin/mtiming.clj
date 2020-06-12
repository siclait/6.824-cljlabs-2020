(ns map-reduce.plugin.mtiming
  (:require [clojure.java.io :as io]
            [clojure.java.shell :as shell]
            [clojure.string :as string]
            [map-reduce.plugin :as plugin])
  (:import java.lang.management.ManagementFactory))

(def output-directory "mr-tmp")

(defn get-pid
  []
  (-> (ManagementFactory/getRuntimeMXBean)
      .getName
      (string/split #"@")
      first))

(defn n-parallel
  [phase]
  (let [pid          (get-pid)
        filename     (io/file output-directory (format "mr-worker-%s-%s" phase pid))
        _            (spit filename "x")
        names        (->> (io/file output-directory)
                          file-seq
                          (map str)
                          (filter #(re-find (-> "mr-worker-%s"
                                                (format phase)
                                                re-pattern)
                                            %)))
        running-list (into [] (for [name names]
                                (let [[_ x-pid] (re-find (-> "mr-worker-%s-(\\d+)"
                                                             (format phase)
                                                             re-pattern)
                                                         name)]
                                  (if (zero? (-> (shell/sh "kill" "-0" x-pid)
                                                 :exit))
                                    1
                                    0))))]
    (Thread/sleep 1000)

    (io/delete-file filename)

    (reduce + running-list)))

(defn mapf
  [_ _]
  (let [ts  (/ (System/currentTimeMillis) 1000.0)
        pid (get-pid)
        n   (n-parallel "map")]
    [{:key   (format "times-%s" pid)
      :value (format "%.1f" ts)}
     {:key   (format "parallel-%s" pid)
      :value n}]))

(defn reducef
  [_ vs]
  (string/join " " (sort vs)))

(defmethod plugin/load-plugin :mtiming [_]
  {:mapf    mapf
   :reducef reducef})
