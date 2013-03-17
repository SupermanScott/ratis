(ns ratis.core
  (:require
   [clojure.tools.logging :as log]
   [ratis.redis :as redis]
   [ratis.routing :as routing]
   [ratis.config :as config])
  (:gen-class))

(defn -main
  "Start up the proxy server"
  [& args]
  (let [running-pools (config/start-handlers "examples/config.yml")]
    (log/info "Running" (count running-pools))))
