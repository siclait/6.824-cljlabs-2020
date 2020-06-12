(ns go.net
  (:import (java.net ServerSocket Socket)))

(defprotocol IConnection
  (output-stream [this])
  (input-stream [this]))

(defprotocol Closeable
  (close! [this]))

(defrecord Connection [^Socket socket]
  IConnection
  (output-stream [_]
    (.getOutputStream socket))
  (input-stream [_]
    (.getInputStream socket))
  Closeable
  (close! [_]
    (.close socket)))

(defn connection
  [socket]
  (->Connection socket))

(defprotocol IListener
  (-listen [this address])
  (accept [this]))

(defrecord TCPListener [server-socket socket]
  IListener
  (-listen [_ port]
    (reset! server-socket (ServerSocket. port)))
  (accept [_]
    (connection (.accept @server-socket)))
  Closeable
  (close! [_]
    (.close @server-socket)))

(defn tcp-listener
  []
  (->TCPListener (atom nil) (atom nil)))

(defn listen
  [port]
  (let [listener (tcp-listener)]
    (-listen listener port)
    listener))

(defn dial
  [port]
  (connection (Socket. "localhost" port)))
