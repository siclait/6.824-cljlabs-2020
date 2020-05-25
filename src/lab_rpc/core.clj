(ns lab-rpc.core
  (:require [clojure.core.async :as a]
            [clojure.string :as string]
            [go.reflect :as reflect]
            [go.time :as time]
            [taoensso.timbre :as log]))

(def buffer-header-size 9)

(defn ->bytes
  [x]
  (cond
    (number? x) (.toByteArray (biginteger x))
    (string? x) (map byte x)
    (map? x)    (->bytes (pr-str x))
    :else       (throw (Exception. (format "Attempting to cast invalid type %s to bytes" (type x))))))

(defrecord ReqMsg [end-name svc-meth args reply-ch])

(defrecord ReplyMsg [ok reply])

(defprotocol IClientEnd
  (call! [this svc-meth args]))

(defrecord ClientEnd [end-name ch done]
  IClientEnd
  (call! [_ svc-meth args]
    (let [req    (->ReqMsg end-name svc-meth args (a/chan))
          result (a/alt!!
                   [[ch req]] true
                   done nil)]
      (when result
        (let [{:keys [reply]} (a/<!! (:reply-ch req))]
          reply)))))

(defprotocol INetwork
  (cleanup [this])
  (reliable! [this yes])
  (long-reordering! [this yes])
  (long-delays! [this yes])
  (read-end-name-info [this end-name])
  (server-dead? [this end-name server-name server])
  (process-request [this req])
  (make-end! [this end-name])
  (add-server! [this server-name rs])
  (delete-server! [this server-name])
  (connect! [this end-name server-name])
  (enable! [this end-name enabled])
  (total-count [this])
  (get-total-bytes [this]))

(defprotocol Countable
  (get-count [this] [this arg]))

(defprotocol Dispatchable
  (dispatch! [this arg1] [this arg1 arg2]))

(defn make-end* [{:keys [ends] :as state} end-name client-end]
  (if (contains? ends end-name)
    (log/fatalf "MakeEnd: %s already exists" end-name)
    (-> state
        (assoc-in [:ends end-name] client-end)
        (assoc-in [:enabled end-name] false)
        (assoc-in [:connections end-name] nil))))

(defrecord Network [state end-ch done]
  INetwork
  (cleanup [_]
    (a/close! done))
  (reliable! [_ yes]
    (swap! state assoc :reliable yes))
  (long-reordering! [_ yes]
    (swap! state assoc :long-reordering yes))
  (long-delays! [_ yes]
    (swap! state assoc :long-delays yes))
  (read-end-name-info [_ end-name]
    (let [{:keys [reliable long-reordering] :as state} @state
          server-name                                  (get-in state [:connections end-name])]
      {:enabled         (get-in state [:enabled end-name])
       :server-name     server-name
       :server          (when server-name (get-in state [:servers server-name]))
       :reliable        reliable
       :long-reordering long-reordering}))
  (server-dead? [_ end-name server-name server]
    (let [state @state]
      (or (not (get-in state [:enabled end-name]))
          (not= (get-in state [:servers server-name]) server))))
  (process-request [this {:keys [end-name reply-ch] :as req}]
    (let [{:keys [enabled server-name server reliable long-reordering]}
          (read-end-name-info this end-name)]
      (if (and enabled
               (some? server-name)
               (some? server))
        (do
          (when (not reliable)
                                        ; Short delay.
            (let [ms (rand-int 27)]
              (Thread/sleep ms)))

          (if (and (not reliable)
                   (< (rand-int 1000) 100))
                                        ; Drop the request, return as if timed out.
            (a/put! reply-ch (->ReplyMsg false nil))

            (let [ech (a/chan)]
              (a/thread
                (let [r (dispatch! server req)]
                  (a/put! ech r)))

              (let [[reply reply-ok]
                    (loop []
                      (if-let [reply (a/alt!!
                                       ech ([r] r)
                                       (a/timeout 100) nil)]
                        [reply true]
                        (if (server-dead? this end-name server-name server)
                          (do
                            (a/thread (a/<!! ech))
                            [nil false])
                          (recur))))]
                (let [server-dead (server-dead? this end-name server-name server)]
                  (cond
                    (or (not reply-ok)
                        server-dead)
                                        ; server was killed while we were waiting; return error.
                    (a/put! reply-ch (->ReplyMsg false nil))

                    (and (not reliable)
                         (< (rand-int 1000) 100))
                                        ; drop the reply, return as if timeout
                    (a/put! reply-ch (->ReplyMsg false nil))

                    (and long-reordering
                         (< (rand-int 900) 600))
                                        ; delay the response for a while
                    (let [ms (+ 200 (rand-int (inc (rand-int 2000))))]
                      (time/after-func ms #(do
                                             (swap! state update :bytes (partial + buffer-header-size (-> (:reply reply)
                                                                                                          ->bytes
                                                                                                          count)))
                                             (a/put! reply-ch reply))))

                    :else
                    (do
                      (swap! state update :bytes (partial + buffer-header-size (-> (:reply reply)
                                                                                   ->bytes
                                                                                   count)))
                      (a/put! reply-ch reply))))))))
                                        ; simulate no reply and eventual timeout.
        (let [ms (if (:long-delays @state)
                   (rand-int 7000)
                   (rand-int 100))]
          (time/after-func ms #(a/put! reply-ch (->ReplyMsg false nil)))))))
  (make-end! [_ end-name]
    (let [e (->ClientEnd end-name end-ch done)]
      (swap! state make-end* end-name e)
      e))
  (add-server! [_ server-name rs]
    (swap! state assoc-in [:servers server-name] rs))
  (delete-server! [_ server-name]
    (swap! state assoc-in [:servers server-name] nil))
  (connect! [_ end-name server-name]
    (swap! state assoc-in [:connections end-name] server-name))
  (enable! [_ end-name enabled]
    (swap! state assoc-in [:enabled end-name] enabled))
  (total-count [_]
    (:count @state))
  (get-total-bytes [_]
    (:bytes @state))
  Countable
  (get-count [_ server-name]
    (let [svr (get-in @state [:servers server-name])]
      (get-count svr))))

(defn make-network []
  (let [rn     (map->Network {:state  (atom {:reliable        true
                                             :long-delays     false
                                             :long-reordering false
                                             :ends            {}
                                             :enabled         {}
                                             :servers         {}
                                             :connections     {}
                                             :count           0
                                             :bytes           0})
                              :end-ch (a/chan)
                              :done   (a/chan)})
        end-ch (:end-ch rn)
        done   (:done rn)]
                                        ; Handles all ClientEnd.Call()s
    (a/thread
      (let [state (:state rn)]
        (loop []
          (when-let [req (a/alt!!
                           end-ch ([r] r)
                           done nil)]
            (swap! state update :count inc)
            (swap! state update :bytes (partial + buffer-header-size (-> (:args req)
                                                                         ->bytes
                                                                         count)))
            (a/thread (process-request rn req))
            (recur)))))
    rn))

(defprotocol IServer
  (add-service! [this svc]))

(defrecord Server [state]
  IServer
  (add-service! [_ svc]
    (swap! state assoc-in [:services (:name svc)] svc))
  Dispatchable
  (dispatch! [_ {:keys [svc-meth] :as req}]
    (swap! state update :count inc)
    (let [[service-name method-name] (string/split (str (.-sym svc-meth)) #"/")
          service                    (get (:services @state) service-name)]
      (if (some? service)
        (dispatch! service method-name req)
        (let [choices (keys (:services @state))]
          (log/fatalf "labrpc.Server.dispatch(): unknown service %s in %s.%s; expecting one of %s"
                      service-name service-name method-name choices)
          (->ReplyMsg false nil)))))
  Countable
  (get-count [_]
    (:count @state)))

(defn make-server []
  (->Server (atom {:services {}
                   :count    0})))

(defrecord Service [name methods]
  Dispatchable
  (dispatch! [_ method-name {:keys [args]}]
    (if-let [method (get methods method-name)]
      (->ReplyMsg true (method args))
      (let [choices (keys methods)]
        (log/fatalf "labrpc.Service.dispatch(): unknown method %s in %s; expecting one of %s"
                    method-name name choices)
        (->ReplyMsg false nil)))))

(defn make-service [rcvr]
  (let [rcvr-type    (reflect/type-of rcvr)
        svc-name     (reflect/name rcvr-type)
        capitalized? (fn [s] (let [first-char (subs s 0 1)]
                              (= (string/upper-case first-char)
                                 first-char)))
        methods      (->> (for [m     (range (reflect/num-method rcvr-type))
                                :let  [method (reflect/method rcvr-type m)
                                       method-name (reflect/name method)]
                                :when (capitalized? method-name)]
                            [method-name (reflect/func method)])
                          (into {}))]
    (map->Service {:name    svc-name
                   :methods methods})))
