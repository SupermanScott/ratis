(ns ratis.config
  (:require [clj-yaml.core :as yaml]
            [aleph.tcp]
            [ratis.routing :as routing]
            [clojure.tools.logging :as log]
            [ratis.redis :as redis])
  (:import (java.net ConnectException)))

(defrecord Pool [name servers])
(defrecord Server [host port priority last_update stopping used_cpu_user
                   failure_count failure_limit healthcheck_milliseconds
                   pool-name])

(declare update-server-state)
(declare server-down)

(defn generate-config
  "Generate a configuration map from a file"
  [path]
  (yaml/parse-string (slurp path)))

(defn create-server
  [{host :host port :port priority :priority failure_limit :failure_limit
    healthcheck_milliseconds :healthcheck_milliseconds}
   pool-name]
  (let [server-value (->Server host port priority 0 false "0" 0 failure_limit
                               healthcheck_milliseconds pool-name)
        agent-var (agent server-value)]
    (set-error-handler! agent-var server-down)
    (send-off agent-var update-server-state)
    agent-var))

(defn start-handler
  "Starts up a handler for a pool by creating the pool and servers"
  [pool-map]
  (let [servers (map #(create-server % (:name pool-map))
                     (get pool-map :servers []))
        name (:name pool-map)
        pool (->Pool name servers)
        config {:port (:port pool-map) :frame redis/redis-codec}]
    (doall servers)
    (assoc pool :connection (aleph.tcp/start-tcp-server
                             (routing/create-redis-handler-multiple pool)
                             config))))

(defn start-pools
  "Starts the handlers in the pools defined in the configuration file in path"
  [path]
  (log/info "starting up with" path)
  (let [config (generate-config path)]
    (doall (map #(start-handler (assoc (% config) :name (name %)))
                                (keys config)))))

(defn calc-new-state
  [server]
  (try
    (let [redis-response (redis/query-server-state (:host server) (:port server))
          update-state (merge server
                              redis-response
                              {:last_update (System/currentTimeMillis)
                               :down false
                               :failure_count 0
                               :cpu_delta (-
                                           (Integer/parseInt
                                            (:used_cpu_user redis-response))
                                           (Integer/parseInt
                                            (:used_cpu_user server)))})]
      update-state)
    (catch ConnectException ce
      (let [down-server (assoc server :failure_count
                               (inc (:failure_count server)))]
      (log/error "Server" (:host down-server) (:port down-server) "is down"
                 (:failure_count down-server))
      (if (<= (:failure_limit down-server) (:failure_count down-server))
        (assoc down-server :down true)
        down-server)))))

(defn update-server-state
  "Queries the *agent* server for its current state and updates itself"
  [server]
  (when (not= 0 (:last_update server))
    (. Thread (sleep (min (+ (* (+ 1 (:failure_count server))
                                (:healthcheck_milliseconds server))
                             (rand-int 85))
                          30000))))
  (when (not (:stopping server))
    (send-off *agent* #'update-server-state))
  (calc-new-state server)
)
(defn server-down
  "Error handler function for the server agent"
  [server-agent ex]
  (log/error "Agent threw exception" server-agent ex))

(defn stop-server
  "Agent function that marks the server as stopped"
  [server]
  (assoc server :stopping true))

(defn stop-all-pools
  [pools]
  (doall (map #(if-not (agent-error %) (send-off % #'stop-server)) (apply concat (map :servers pools))))
  (doall (map #((->> % :connection)) pools)))
