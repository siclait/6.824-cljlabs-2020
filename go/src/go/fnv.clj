(ns go.fnv
  "An incomplete, direct port of Go's hash/fnv library to Clojure.")

(def fnv1-32-init 0x811c9dc5)
(def fnv-prime (+ (bit-shift-left 1 24) (bit-shift-left 1 8) 0x93))

(defn fnv1a32
  [s]
  (loop [byte-values (map byte s)
         seed        fnv1-32-init]
    (if (seq byte-values)
      (let [xor-seed (bit-xor seed (first byte-values))
            mul-seed (unchecked-multiply xor-seed fnv-prime)]
        (recur (rest byte-values) mul-seed))
      (bit-and seed 0x00000000ffffffff))))
