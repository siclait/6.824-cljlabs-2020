(ns map-reduce.sequential
  "MapReduce code for 6.824 2020 Lab 1."
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.tools.cli :as cli]
            [map-reduce.plugin :as plugin]
            [map-reduce.plugin crash indexer mtiming nocrash rtiming wc]
            [map-reduce.util :as util]
            [taoensso.timbre :as log]))

(def output-directory "mr-tmp")

(def cli-options
  [[nil "--app APP" "Application to run."
    :validate [#{"wc" "indexer" "mtiming" "rtiming" "crash" "nocrash"} "Must be a valid application"]]
   [nil "--output-path PATH" "Location to write output files."]
   ["-h" "--help"]])

(defn usage [options-summary]
  (->> ["Sequential MapReduce runner."
        ""
        "Usage: sequential [options] input-files"
        ""
        "Options:"
        options-summary]
       (string/join \newline)))

(defn error-msg [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (string/join \newline errors)))

(defn validate-args [args]
  (let [{:keys [options arguments errors summary]} (cli/parse-opts args cli-options)]
    (cond
      (:help options)          {:exit-message (usage summary) :ok? true}
      errors                   {:exit-message (error-msg errors)}
      (>= (count arguments) 1) {:files arguments :options options}
      :else                    {:exit-message (usage summary)})))

(defn exit [status msg]
  (log/info msg)
  (System/exit status))

(defn -main [& args]
  (let [{:keys [files options exit-message ok?]} (validate-args args)]
    (if exit-message
      (exit (if ok? 0 1) exit-message)
      (let [app                    (-> (:app options)
                                       keyword)
            {:keys [mapf reducef]} (plugin/load-plugin app)
            ;;	read each input file,
            ;;	pass it to Map,
            ;;	accumulate the intermediate Map output.

            ;;	a big difference from real MapReduce is that all the
            ;;	intermediate data is in one place, intermediate[],
            ;;	rather than being partitioned into NxM buckets.
            intermediate (->> files
                              (map #(mapf % (slurp %)))
                              (apply concat)
                              (filter (comp (complement string/blank?) :key)))
            ;; call Reduce on each distinct key in intermediate[],
            ;;	and print the result to mr-out-0.
            output       (->> intermediate
                              (util/index-by :key)
                              (util/map-over-values :value)
                              (map (fn [[k vs]] [k (reducef k vs)]))
                              (sort-by first))]
        (with-open [writer (io/writer (io/file output-directory "mr-out-0"))]
          (doseq [[k v] output]
            (.write writer (format "%s %s\n" k v))))))))
