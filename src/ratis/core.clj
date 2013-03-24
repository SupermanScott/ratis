(ns ratis.core
  (:require
   [clojure.tools.logging :as log]
   [ratis.redis :as redis]
   [ratis.routing :as routing]
   [ratis.config :as config])
  (:gen-class))

(def pools (atom nil))
(def config-file (atom nil))

(defn reload-pools
  "Reloads the pools based on the current config file"
  []
  (when @pools
    (config/stop-all-pools @pools))
  (reset! pools (config/start-pools @config-file)))

(defn start-up
  [file-path]
  (reset! config-file file-path)
  (reload-pools))

(defn -main
  "Start up the proxy server"
  [& args]
  (reset! config-file "examples/config.yml")
  (reload-pools)
  (log/info "Running" (count @pools)))
