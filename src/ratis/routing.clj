(ns ratis.routing
  (:require
   [lamina.core]
   [clojure.tools.logging :as log]
   [ratis.redis :as redis]))

(defn active-server
  "Returns true if the agent server is active and alive for queries"
  [server]
  (and (not (agent-error server))
       (< 0 (:last_update @server))
       (not= 0 (:loading @server))
       (or
        (= nil (:master_sync_in_progress @server))
        (= 0 (Integer/parseInt (:master_sync_in_progress @server))))
       (not (:down @server))
       true))

(defn least-loaded
  "Returns the server that has the least load"
  [servers]
  (first (sort-by :cpu_delta servers)))

(defn respond-master
  "Routes payload to the master in pool for response"
  [cmd ch pool]
  (log/info "Routing to master:" cmd)
  (let [all-servers (map deref (filter active-server (:servers pool)))
        server (first (filter #(= "master" (:role %)) all-servers))]
    (redis/send-to-redis-and-respond
     (redis/create-redis-connection (:host server) (:port server)) cmd ch)))

(defn respond-slave-eligible
  "Routes payload to a redis server for response"
  [cmd ch pool]
  (log/info "Routing anywhere:" cmd)
  (let [all-servers (map deref (filter active-server (:servers pool)))
        server (least-loaded all-servers)]
    (log/info cmd "can be sent to" (count all-servers) "for" (:name pool))
    (redis/send-to-redis-and-respond
     (redis/create-redis-connection (:host server) (:port server)) cmd ch)))

(defn master-only?
  [client cmd]
  (and (redis/master-only-command? cmd)
       (not (:transaction-connection @client))))

(defn slave-eligible?
  [client cmd]
  (and (not (redis/master-only-command? cmd))
       (not (redis/advanced-command? cmd))
       (not (:transaction-connection @client))))

(defn in-transaction?
  [client cmd]
  (and
   (:transaction-connection @client)
   (not (redis/finished-transaction? cmd))
   (not (redis/start-transaction-command? cmd))))

(defn start-transaction?
  [client cmd]
  (redis/start-transaction-command? cmd))

(defn end-transaction?
  [client cmd]
  (and (:transaction-connection @client)
       (redis/finished-transaction? cmd)))

(defn startup-transaction!
  "Starts up the transaction, sends to Redis and updates client state"
  [cmd ch pool client]
  (log/info "Starting up transaction for" @client)
  (let [all-servers (map deref (filter active-server (:servers pool)))
        server (first (filter #(= "master" (:role %)) all-servers))
        redis-connection (redis/create-redis-connection
                          (:host server) (:port server))]
    (swap! client assoc :transaction-connection redis-connection)
    (lamina.core/enqueue redis-connection cmd)
    (lamina.core/receive-all redis-connection
                         (fn [response]
                           (if (= 1 (count response))
                             (lamina.core/enqueue ch
                                                  (first response))
                             (lamina.core/enqueue ch
                                                  response))))))

(defn stop-transaction!
  "Sends the final command to Redis and stops transaction context"
  [cmd ch pool client]
  (let [redis-connection (:transaction-connection @client)]
    (log/info "Stopping transaction for" @client "with" cmd)
    (lamina.core/enqueue redis-connection cmd)
    (swap! client dissoc :transaction-connection)))

(defn respond-transaction
  "Sends the command to Redis when in transaction state"
  [cmd ch pool client]
  (let [redis-connection (:transaction-connection @client)]
    (log/info "Sending" cmd "in transaction")
    (lamina.core/enqueue redis-connection cmd)))

(defn create-redis-handler-multiple
  "Connects to all servers and returns a function to listen for commands"
  [pool]
  (fn [ch client-info]
    (let [client (atom client-info)
          start-transaction-channel (lamina.core/filter*
                                     (partial start-transaction? client) ch)
          in-transaction-channel (lamina.core/filter*
                                  (partial in-transaction? client) ch)
          stop-transaction-channel (lamina.core/filter*
                                    (partial end-transaction? client) ch)
          master-channel (lamina.core/filter* (partial master-only? client) ch)
          slave-channel (lamina.core/filter* (partial slave-eligible? client) ch)]
      (lamina.core/receive-all master-channel
                               (fn [cmd]
                                 (respond-master cmd ch pool)))
      (lamina.core/receive-all slave-channel
                               (fn [cmd]
                                 (respond-slave-eligible cmd ch pool)))
      (lamina.core/receive-all start-transaction-channel
                               (fn [cmd]
                                 (startup-transaction! cmd ch pool client)))
      (lamina.core/receive-all in-transaction-channel
                               (fn [cmd]
                                 (respond-transaction cmd ch pool client)))
      (lamina.core/receive-all stop-transaction-channel
                               (fn [cmd]
                                 (stop-transaction! cmd ch pool client)))
      )))
