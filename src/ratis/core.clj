(ns ratis.core
  (:require [ratis.redis :as redis]
            [aleph.tcp])
  (:gen-class))

(defn -main
  "Start up the proxy server"
  [& args]
  (aleph.tcp/start-tcp-server redis/redis-handler {:port 10000 :frame redis/redis-codec}))
