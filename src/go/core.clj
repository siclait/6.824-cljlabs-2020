(ns go.core)

(defmacro defer
  [cb & body]
  `(try
     ~@body
     (finally
       ~cb)))
