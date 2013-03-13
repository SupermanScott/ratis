(ns ratis.routing
  (:require
   [lamina.core]
   [ratis.redis :as redis]))

(defn redis-handler
  "Responsible for listening for commands and sending to the proper machine"
  [ch client-info]
  (lamina.core/receive-all ch
                           #(lamina.core/enqueue ch
                                                 (redis/send-to-redis
                                                  "localhost" 6379 %))))
