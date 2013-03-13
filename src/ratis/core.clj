(ns ratis.core
  (:require [ratis.redis :as redis]
            [ratis.routing :as routing]
            [aleph.tcp])
  (:gen-class))

(defn -main
  "Start up the proxy server"
  [& args]
  (aleph.tcp/start-tcp-server routing/redis-handler {
                                                     :port 10000
                                                     :frame redis/redis-codec})
  "Running")
