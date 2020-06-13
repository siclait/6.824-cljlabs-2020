(ns raft.core
  "Write your code here."
  (:require
   [lab-rpc.core :as lab-rpc]
   [raft.persister :as persister]))

(defprotocol IRaft
  (RequestVote [this args]
    "Example RPC handler."))

(defrecord Raft
    [me
     peers
     persister]
  IRaft
  (RequestVote [this args]
    ;; Your code here (2A, 2B).
    ))

(defn raft
  [peers persister me]
  (map->Raft {:peers     peers
              :persister persister
              :me        me }))

(defn get-state
  "Get the current state for the Raft instance."
  [raft]
  (let [term    nil
        leader? nil]
    ;; Your code here (2A).
    [term leader?]))

(defn persist
  "Save Raft's persistent state to stable storage,
  where it can later be retrieved after a crash and restart.
  See paper's Figure 2 for a description of what should be persistent."
  [raft]
  ;; Your code here (2C).
  )

(defn read-persist
  "Restore previously persisted state."
  [raft data]
  ;; Your code here (2C).
  )

;; example code to send a RequestVote RPC to a server.
;; server is the index of the target server in rf.peers[].
;; expects RPC arguments in args.
;;
;; The lab-rpc package simulates a lossy network, in which servers
;; may be unreachable, and in which requests and replies may be lost.
;; call! sends a request and waits for a reply. If a reply arrives
;; within a timeout interval, call! returns the result; otherwise
;; call! returns nil. Thus call! may not return for a while.
;; A nil return can be caused by a dead server, a live server that
;; can't be reached, a lost request, or a lost reply.
;;
;; call! is guaranteed to return (perhaps after a delay) *except* if the
;; handler function on the server side does not return. Thus there
;; is no need to implement your own timeouts around call!.
;;
;; look at the comments in lab-rpc/src/lab_rpc/core.clj for more details.
(defn send-request-vote
  [raft server args]
  (let [peers (:peers raft)]
    (lab-rpc/call! (nth peers server) :Raft/RequestVote args)))

;; the service using Raft (e.g. a k/v server) wants to start
;; agreement on the next command to be appended to Raft's log. if this
;; server isn't the leader, returns false. otherwise start the
;; agreement and return immediately. there is no guarantee that this
;; command will ever be committed to the Raft log, since the leader
;; may fail or lose an election. even if the Raft instance has been killed,
;; this function should return gracefully.
;;
;; the first return value is the index that the command will appear at
;; if it's ever committed. the second return value is the current
;; term. the third return value is true if this server believes it is
;; the leader.
(defn start
  "Start agreement on the next command."
  [raft command]
  (let [index   nil
        term    nil
        leader? true]
    ;; Your code here (2B).
    [index term leader?]))

(defn kill
"Kill this Raft instance."
[raft]
;; Your code here.
)

;; the service or tester wants to create a Raft server. the ports
;; of all the Raft servers (including this one) are in peers[]. this
;; server's port is peers[me]. all the servers' peers[] arrays
;; have the same order. persister is a place for this server to
;; save its persistent state, and also initially holds the most
;; recent saved state, if any. applyCh is a channel on which the
;; tester or service expects Raft to send ApplyMsg messages.
;; Make() must return quickly, so it should start goroutines (or threads)
;; for any long-running work.
(defn make
  [peers me persister apply-ch]
  (let [rf (raft peers persister me)]
    ;; Your initialization code here (2A, 2B, 2C).

    ;; initialize from state persisted before a crash
    (read-persist rf (persister/read-raft-state persister))

    rf))
