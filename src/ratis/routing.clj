(ns ratis.routing
  (:require
   [lamina.core]
   [ratis.redis :as redis]))

(defn respond
  [ch cmd]
  (redis/send-to-redis-and-respond "localhost" 6379 cmd ch))

(defn redis-handler
  "Responsible for listening for commands and sending to the proper machine"
  [ch client-info]
  (lamina.core/receive-all ch
                           #(respond ch %)))
