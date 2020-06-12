(ns map-reduce.master
  (:require [clojure.string :as string]
            [clojure.tools.cli :as cli]
            [map-reduce.impl.master :as master]
            [taoensso.timbre :as log]))

(def cli-options
  [["-h" "--help"]])

(defn usage [options-summary]
  (->> ["MapReduce master."
        ""
        "Usage: master input-files"
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
      (:help options) {:exit-message (usage summary) :ok? true}
      errors {:exit-message (error-msg errors)}
      (>= (count arguments) 1) {:files arguments :options options}
      :else {:exit-message (usage summary)})))

(defn exit [status msg]
  (log/info msg)
  (System/exit status))

(defn -main [& args]
  (let [{:keys [files exit-message ok?]} (validate-args args)]
    (if exit-message
      (exit (if ok? 0 1) exit-message)
      (let [m (master/make-master files 10)]
        (while (not (master/done m))
          (Thread/sleep 1000))
        (Thread/sleep 1000)))))
