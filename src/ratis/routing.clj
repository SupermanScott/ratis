(ns ratis.routing
  (:require
   [lamina.core]
   [clojure.tools.logging :as log]
   [ratis.redis :as redis]))

(defn respond
  [ch cmd]
  (redis/send-to-redis-and-respond "localhost" 6379 cmd ch))

(defn respond-master
  "Routes payload to the master in pool for response"
  [payload]
  (log/info "Routing to master:" (:cmd payload))
  (respond (:ch payload) (:cmd payload)))

(defn respond-slave-eligible
  "Routes payload to a redis server for response"
  [payload]
  (log/info "Routing anywhere:" (:cmd payload))
  (respond (:ch payload) (:cmd payload)))

(defn redis-handler
  "Responsible for listening for commands and sending to the proper machine"
  [ch client-info]
  (let [master-only (lamina.core/channel)
        slave-eligible (lamina.core/channel)]
    (lamina.core/receive-all master-only respond-master)
    (lamina.core/receive-all slave-eligible respond-slave-eligible)
    (lamina.core/receive-all ch
                             #(if (redis/master-only-command %)
                                (lamina.core/enqueue master-only {:cmd % :ch ch})
                                (lamina.core/enqueue slave-eligible {:cmd % :ch ch})))))
