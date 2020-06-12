(ns map-reduce.util)

(defn index-by
  [k ms]
  (letfn [(index-by-k [acc m]
            (update acc (get m k) #(conj % (dissoc m k))))]
    (reduce index-by-k {} ms)))

(defn map-over-values
  [f m]
  (reduce-kv (fn [m k v]
               (assoc m k (map f v)))
             {}
             m))
