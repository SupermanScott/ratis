(ns ratis.core
  (:require
   [clojure.tools.logging :as log]
   [ratis.redis :as redis]
   [ratis.routing :as routing]
   [ratis.config :as config])
  (:gen-class))

(def pools (atom nil))
(def config-file (atom nil))

(declare stop!)

(defn reload-pools!
  "Reloads the pools based on the current config file"
  []
  (when @pools
    (stop!))
  (reset! pools (config/start-pools @config-file)))

(defn start-up!
  "Startup pools based on the configuration"
  [file-path]
  (reset! config-file file-path)
  (reload-pools!))

(defn stop!
  "Stop current pools"
  []
  (config/stop-all-pools @pools))

(defn -main
  "Start up the proxy server"
  [& args]
  (start-up! "examples/config.yml")
  (log/info "Running" (count @pools)))
