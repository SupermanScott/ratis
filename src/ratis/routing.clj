(ns ratis.routing
  (:require
   [lamina.core]
   [clojure.tools.logging :as log]
   [ratis.redis :as redis]))

(defn active-server
  "Returns true if the agent server is active and alive for queries"
  [server]
  (and (not (agent-error server))
       (> (:last_update @server) 0)
       (not (:down @server))
       true))
(defn respond-master
  "Routes payload to the master in pool for response"
  [{cmd :cmd ch :ch pool :pool}]
  (log/info "Routing to master:" cmd)
  (let [all-servers (map deref (:servers pool))
        server (first (filter #(= "master" (:role %)) all-servers))]
    (redis/send-to-redis-and-respond (:host server) (:port server) cmd ch)))

(defn respond-slave-eligible
  "Routes payload to a redis server for response"
  [{cmd :cmd ch :ch pool :pool}]
  (log/info "Routing anywhere:" cmd)
  (let [all-servers (map deref (filter active-server (:servers pool)))
        server (rand-nth all-servers)]
    (redis/send-to-redis-and-respond (:host server) (:port server) cmd ch)))

(defn create-redis-handler
  "Returns a function that is setup to listen for commands"
  [pool]
  (fn [ch client-info]
    (let [master-only (lamina.core/channel)
          slave-eligible (lamina.core/channel)]
      (lamina.core/receive-all master-only respond-master)
      (lamina.core/receive-all slave-eligible respond-slave-eligible)
      (lamina.core/receive-all ch (fn [cmd]
                                    (let [payload {:cmd cmd :ch ch :pool pool}]
                                      (if (redis/master-only-command cmd)
                                        (respond-master payload)
                                        (respond-slave-eligible payload))))))))
