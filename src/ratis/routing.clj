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

(defn create-redis-handler
  "Returns a function that is setup to listen for commands"
  [pool]
  (fn [ch client-info]
    (let [master-only (lamina.core/channel)
          slave-eligible (lamina.core/channel)]
      (lamina.core/receive-all master-only respond-master)
      (lamina.core/receive-all slave-eligible respond-slave-eligible)
      (lamina.core/receive-all ch (fn [cmd]
                                    (if (redis/master-only-command? cmd)
                                        (respond-master cmd ch pool)
                                        (respond-slave-eligible cmd ch pool)))))
    ))
