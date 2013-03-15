(ns ratis.core
  (:require [ratis.redis :as redis]
            [ratis.routing :as routing]
            [ratis.config :as config])
  (:gen-class))

(defn -main
  "Start up the proxy server"
  [& args]
  (config/start-handlers "examples/config.yml"))
