(ns ratis.config
  (:require [clj-yaml.core :as yaml]))

(defrecord Pool [server_retry_timeout server_failure_limit servers])
(defrecord Server [host port priority])

(defn generate-config
  "Generate a configuration map from a file"
  [path]
  (yaml/parse-string (slurp path)))

(defn start-handler
  "Starts up a handler for a pool by creating the pool and servers"
  [pool-map]
  (let [servers (map #(Server. (:host %) (:port %) (get % :priority 1))
                     (get pool-map :servers []))
        server_retry_timeout (:server_retry_timeout pool-map)
        server_failure_limit (:server_failure_limit pool-map)
        pool (Pool. server_retry_timeout server_failure_limit servers)]
    pool))

(defn start-handlers
  "Starts the handlers defined in the configuration file provided"
  [path]
  (let [config (generate-config path)]
          (loop [pools (keys config)
                 pool (get pools (first pools))
                 port (:port pool)
                 server_retry_timeout (get pool :server_retry_timeout 1000)
                 server_failure_limit (get pool :server_failure_limit 1)]
            (Pool. server_retry_timeout server_failure_limit [])
            (recur (rest pools)
                   (get pools (first pools))
                   (:port pool)
                   (get pool :server_retry_timeout 1000)
                   (get pool :server_failure_limit 1)))
    ))
