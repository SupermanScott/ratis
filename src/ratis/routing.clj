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

(defn create-redis-handler-multiple
  "Connects to all servers and returns a function to listen for commands"
  [pool]
  (fn [ch client-info]
    (let [master-channel (lamina.core/filter* redis/master-only-command? ch)
          slave-channel (lamina.core/filter*
                          #(not (redis/master-only-command? %)) ch)]
      (lamina.core/receive-all master-channel
                               (fn [cmd]
                                 (respond-master cmd ch pool)))
      (lamina.core/receive-all slave-channel
                               (fn [cmd]
                                 (respond-slave-eligible cmd ch pool)))
      )))
