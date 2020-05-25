(ns raft.persister
  "Support for Raft and kvraft to save persistent
   Raft state (log &c) and k/v server snapshots.")

(defprotocol IPersister
  (copy [this])
  (save-raft-state! [this raft-state])
  (read-raft-state [this])
  (raft-state-size [this])
  (save-state-and-snapshot! [this raft-state snapshot])
  (read-snapshot [this])
  (snapshot-size [this]))

(defrecord Persister [state])

;; TODO: The state and snapshot should both be arrays of bytes.
(defn make-persister
  ([]
   (->Persister (atom {:raft-state {}
                       :snapshot   {}})))
  ([raft-state snapshot]
   (->Persister (atom {:raft-state raft-state
                       :snapshot   snapshot}))))

(extend-type Persister
  IPersister
  (copy [this]
    (let [state @(:state this)]
      (make-persister (:raft-state state) (:snapshot state))))

  (save-raft-state! [this raft-state]
    (swap! (:state this) assoc :raft-state raft-state))

  (read-raft-state [this]
    (:raft-state @(:state this)))

  (raft-state-size [this]
    (count (:raft-state @(:state this))))

  (save-state-and-snapshot! [this raft-state snapshot]
    (reset! (:state this) {:raft-state raft-state
                           :snapshot   snapshot}))

  (read-snapshot [this]
    (:snapshot @(:state this)))

  (snapshot-size [this]
    (count (:snapshot @(:state this)))))
