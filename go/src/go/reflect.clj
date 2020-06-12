(ns go.reflect
  (:refer-clojure :exclude [methods name type])
  (:require
   [clojure.set]
   [clojure.string :as string]))

(defrecord Empty [])

(defprotocol Nameable
  (name [this]))

(defprotocol IMethod
  (func [this]))

(defrecord Method [^Object o ^java.lang.reflect.Method meth]
  IMethod
  (func [_]
    (fn [arg]
      (.invoke meth o (into-array Object [arg]))))

  Nameable
  (name [_]
    (.getName meth)))

(defprotocol IType
  (num-method [this])
  (methods [this])
  (method [this idx]))

(defn declared-methods
  [c]
  (.getDeclaredMethods c))

(defn method-name
  [m]
  (.getName m))

(defrecord Type [o]
  IType
  (num-method [this]
    (count (methods this)))

  (methods [_]
    (let [method-set (fn [c]
                       (into #{} (map method-name) (declared-methods c)))]
      (vec
        (clojure.set/difference
          (method-set (class o))
          (method-set Empty)))))

  (method [this idx]
    (let [method-name       (nth (methods this) idx)
          methods-with-name (->> (declared-methods (class o))
                                 (filter #(= (.getName %) method-name)))
          matching-method   (first methods-with-name)]
      (->Method o matching-method)))

  Nameable
  (name [_]
    (-> (class o)
        .getName
        (string/split #"\.")
        last)))

(defn type-of
  [o]
  (->Type o))
