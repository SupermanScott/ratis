(ns ratis.config
  (:require [clj-yaml.core :as yaml]
            [aleph.tcp]
            [ratis.routing :as routing]
            [clojure.tools.logging :as log]
            [ratis.redis :as redis])
  (:import (java.net ConnectException)))

(defrecord Pool [server_retry_timeout server_failure_limit servers])
(defrecord Server [host port priority last_update stopping])

(declare update-server-state)
(declare server-down)

(defn generate-config
  "Generate a configuration map from a file"
  [path]
  (yaml/parse-string (slurp path)))

(defn create-server
  [host port priority]
  (let [server-value (->Server host port priority 0 false)
        agent-var (agent server-value)]
    (set-error-handler! agent-var server-down)
    (send-off agent-var update-server-state)
    agent-var))

(defn start-handler
  "Starts up a handler for a pool by creating the pool and servers"
  [pool-map]
  (let [servers (map #(create-server (:host %) (:port %) (get % :priority 1))
                     (get pool-map :servers []))
        server_retry_timeout (:server_retry_timeout pool-map)
        server_failure_limit (:server_failure_limit pool-map)
        pool (->Pool server_retry_timeout server_failure_limit servers)
        config {:port (:port pool-map) :frame redis/redis-codec}]
    (doall servers)
    (assoc pool :connection (aleph.tcp/start-tcp-server
                             (routing/create-redis-handler pool) config))))

(defn start-handlers
  "Starts the handlers defined in the configuration file in path"
  [path]
  (log/info "starting up with" path)
  (let [config (generate-config path)]
    (doall (map #(start-handler (% config)) (keys config)))))

(defn update-server-state
  "Queries the *agent* server for its current state and updates itself"
  [server]
  (when (not= 0 (:last_update server))
      (. Thread (sleep (+ 10 (rand-int 85)))))
  (when (not (:stopping server))
    (send-off *agent* #'update-server-state))
  (try
    (let [redis-response (redis/query-server-state (:host server) (:port server))
          update-state (merge server
                              redis-response
                              {:last_update (System/currentTimeMillis)
                               :down false})]
      update-state)
    (catch ConnectException ce (assoc server :down true))))

(defn server-down
  "Error handler function for the server agent"
  [server-agent ex]
  (log/error "Agent threw exception" server-agent ex)
  (restart-agent server-agent (merge @server-agent {:last_update 0}) :clear-actions true)
  (send-off server-agent update-server-state))

(defn stop-server
  "Agent function that marks the server as stopped"
  [server]
  (assoc server :stopping true))

(defn stop-all-pools
  [pools]
  (doall (map #(send-off % #'stop-server) (apply concat (map :servers pools))))
  (doall (map #((->> % :connection)) pools)))
