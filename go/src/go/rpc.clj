(ns go.rpc
  "An incomplete, direct port of Go's net/rpc library to Clojure"
  (:require
   [cognitect.transit :as transit]
   [go.net :as net]
   [go.reflect :as reflect]
   [clojure.string :as string]
   [clojure.core.async :as async]
   [taoensso.timbre :as log]))

(defrecord Service [name methods])

(defn make-service
  [rcvr]
  (let [rcvr-type    (reflect/type-of rcvr)
        svc-name     (reflect/name rcvr-type)
        capitalized? (fn [s] (let [first-char (subs s 0 1)]
                              (= (string/upper-case first-char)
                                 first-char)))
        methods      (->> (for [m     (range (reflect/num-method rcvr-type))
                                :let  [method (reflect/method rcvr-type m)
                                       method-name (reflect/name method)]
                                :when (capitalized? method-name)]
                            [(keyword method-name) (reflect/func method)])
                          (into {}))]
    (map->Service {:name    svc-name
                   :methods methods})))

(defprotocol IServer
  (handle-http [this])
  (register [this rcvr])
  (serve [this l])
  (serve-conn [this conn]))

(defprotocol Closeable
  (close! [this]))

(defrecord Server [state]
  IServer
  (handle-http [_])

  (register [_ rcvr]
    (let [svc          (make-service rcvr)
          service-name (-> (:name svc)
                           keyword)]
      (swap! state assoc-in [:service-map service-name] (make-service rcvr))))

  (serve [this l]
    (async/thread
      (loop []
        (let [conn (try (net/accept l)
                        (catch Exception e
                          (log/debugf "rpc/serve: accept error, %s" e)))]
          (when (some? conn)
            (async/thread
              (serve-conn this conn)
              (net/close! conn))
            (recur))))))

  (serve-conn [_ conn]
    (let [in                         (net/input-stream conn)
          reader                     (transit/reader in :json)
          {:keys [rpc-name args]}    (transit/read reader)
          [service-name method-name] (string/split (str (.-sym rpc-name)) #"/")
          f                          (get-in @state [:service-map
                                                     (keyword service-name)
                                                     :methods
                                                     (keyword method-name)])
          response                   (f args)
          out                        (net/output-stream conn)
          writer                     (transit/writer out :json)]
      (transit/write writer response))))

(defn new-server
  []
  (->Server (atom {})))

(defprotocol IClient
  (call! [this rpc-name args]))

(defrecord Client [conn]
  IClient
  (call! [_ rpc-name args]
    (let [out    (net/output-stream conn)
          writer (transit/writer out :json)
          _      (transit/write writer {:rpc-name rpc-name
                                        :args     args})
          in     (net/input-stream conn)
          reader (transit/reader in :json)]
      (transit/read reader)))

  Closeable
  (close! [_]
    (net/close! conn)))

(defn client
  [conn]
  (->Client conn))

(defn dial
  [port]
  (let [conn (net/dial port)]
    (client conn)))
