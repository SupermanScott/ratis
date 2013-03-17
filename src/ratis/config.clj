(ns ratis.config
  (:require [clj-yaml.core :as yaml]
            [aleph.tcp]
            [ratis.routing :as routing]
            [ratis.redis :as redis]
            ))

(defrecord Pool [server_retry_timeout server_failure_limit servers])
(defrecord Server [host port priority])

(defn generate-config
  "Generate a configuration map from a file"
  [path]
  (yaml/parse-string (slurp path)))

(defn start-handler
  "Starts up a handler for a pool by creating the pool and servers"
  [pool-map]
  (let [servers (map #(agent (->Server (:host %) (:port %) (get % :priority 1)))
                     (get pool-map :servers []))
        server_retry_timeout (:server_retry_timeout pool-map)
        server_failure_limit (:server_failure_limit pool-map)
        pool (->Pool server_retry_timeout server_failure_limit servers)
        config {:port (:port pool-map) :frame redis/redis-codec}]
    (aleph.tcp/start-tcp-server routing/redis-handler config)
    pool))

(defn start-handlers
  "Starts the handlers defined in the configuration file in path"
  [path]
  (let [config (generate-config path)]
    (map #(start-handler (% config)) (keys config))))

(defn update-server-state
  "Queries the *agent* server for its current state and updates itself"
  [server]
  (. Thread (sleep 1000))
  (when true
    (send-off *agent* #'update-server-state))
  (routing/query-server-state (:host server) (:port server)))
