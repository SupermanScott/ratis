(ns ratis.core
  (:require
   [clojure.tools.logging :as log]
   [ratis.redis :as redis]
   [ratis.routing :as routing]
   [ratis.config :as config])
  (:gen-class))

(def pools (atom nil))
(def config-file (atom nil))

(declare stop! update-server-in-pools)

(defn reload-pools!
  "Reloads the pools based on the current config file"
  []
  (when @pools
    (stop!))
  (let [pools (reset! pools (config/start-pools @config-file))]
          (doall (map #(add-watch % :alive update-server-in-pools)
                      (apply concat (map :servers pools))))
          pools))

(defn start-up!
  "Startup pools based on the configuration"
  [file-path]
  (reset! config-file file-path)
  (reload-pools!))

(defn stop!
  "Stop current pools"
  []
  (config/stop-all-pools @pools))

(defn update-server-in-pools
  "Updates the pool based on a new server state"
  [key server-agent old-state new-state]
  (when (and
         (:down new-state)
         (= "master" (:role new-state)))
    (log/error "Crap master is down for pool" (:pool-name new-state))))

(defn -main
  "Start up the proxy server"
  [& args]
  (start-up! "examples/config.yml")
  (log/info "Running" (count @pools)))
