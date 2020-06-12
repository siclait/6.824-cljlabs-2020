(ns map-reduce.worker
  (:require [clojure.string :as string]
            [clojure.tools.cli :as cli]
            [map-reduce.impl.worker :as worker]
            [map-reduce.plugin :as plugin]
            [map-reduce.plugin crash indexer mtiming nocrash rtiming wc]
            [taoensso.timbre :as log]))

(def cli-options
  [[nil "--app APP" "Application to run."
    :validate [#{"wc" "indexer" "mtiming" "rtiming" "crash" "nocrash"} "Must be a valid application"]]
   ["-h" "--help"]])

(defn usage
  [options-summary]
  (->> ["MapReduce worker."
        ""
        "Usage: worker [options]"
        ""
        "Options:"
        options-summary]
       (string/join \newline)))

(defn error-msg
  [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (string/join \newline errors)))

(defn validate-args
  [args]
  (let [{:keys [options errors summary]} (cli/parse-opts args cli-options)]
    (cond
      (:help options) {:exit-message (usage summary) :ok? true}
      errors          {:exit-message (error-msg errors)}
      :else           {:options options})))

(defn exit
  [status msg]
  (log/info msg)
  (System/exit status))

(defn -main
  [& args]
  (let [{:keys [options exit-message ok?]} (validate-args args)]
    (if exit-message
      (exit (if ok? 0 1) exit-message)
      (let [app                    (-> (:app options)
                                       keyword)
            {:keys [mapf reducef]} (plugin/load-plugin app)]
        (worker/worker mapf reducef)))))
