(ns map-reduce.plugin.rtiming
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
  [{:key "a" :value "1"}
   {:key "b" :value "1"}
   {:key "c" :value "1"}
   {:key "d" :value "1"}
   {:key "e" :value "1"}
   {:key "f" :value "1"}
   {:key "g" :value "1"}
   {:key "h" :value "1"}
   {:key "i" :value "1"}
   {:key "j" :value "1"}])

(defn reducef
  [_ _]
  (let [n (n-parallel "reduce")]
    (str n)))

(defmethod plugin/load-plugin :rtiming [_]
  {:mapf    mapf
   :reducef reducef})
