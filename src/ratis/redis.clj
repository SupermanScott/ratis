(ns ratis.redis
  (:require
   [clojure.tools.logging :as log]
   [aleph.redis.protocol :as proto]
   [clojure.string :as str]
   [gloss.io]
   [gloss.core]
   [aleph.tcp]
   [lamina.core]))

(defn redis-codec [charset]
  (let [codecs (proto/codec-map charset)]
    (gloss.core/compile-frame (gloss.core/header proto/format-byte codecs first))))

(def redis-codec (proto/redis-codec :utf-8))

(def master-only #{
                   "APPEND"
                  "BGREWRITEAOF"
                  "BGSAVE"
                  "BLPOP"
                  "BRPOPLPUSH"
                  "DECR"
                  "DECRBY"
                  "DEL"
                  "EXISTS"
                  "FLUSHDB"
                  "GETSET"
                  "HDEL"
                  "HEXISTS"
                  "HINCRBY"
                  "HMSET"
                  "HSET"
                  "HSETNX"
                  "INCR"
                  "INCRBY"
                  "INCRBYFLOAT"
                  "LASTSAVE"
                  "LINSERT"
                  "LPOP"
                  "LPUSH"
                  "LPUSHX"
                  "LREM"
                  "LSET"
                  "MIGRATE"
                  "MOVE"
                  "MSETNX"
                  "PERSIST"
                  "PEXPIRE"
                  "PEXPIREAT"
                  "PSETNX"
                  "PUBLISH"
                  "PUNSUBSCRIBE"
                  "RENAME"
                  "RENAMENX"
                  "RPOP"
                  "RPOPLPUSH"
                  "RPUSH"
                  "RPUSHX"
                  "SADD"
                  "SDIFFSTORE"
                  "SET"
                  "SETBIT"
                  "SETEX"
                  "SETNX"
                  "SETRANGE"
                  "SINTERSTORE"
                  "SMOVE"
                  "SPOP"
                  "SUNIONSTORE"
                  "ZADD"
                  "ZINCRBY"
                  "ZINTERSTORE"
                  "ZREM"
                  "ZREMRANGEBYRANK"
                  "ZREMRANGEBYSCORE"
                  "ZUNIONSTORE"})

(defn parse-buffer
  "Returns a sequence of the command parameters"
  [command-buffer]
  (gloss.io/decode redis-codec command-buffer))

(defn master-only-command
  "Returns true / false if the command is for master only"
  [cmd]
  (contains? master-only (str/upper-case (first cmd))))

(defn send-to-redis
  "Sends the command to the specified host and returns the response"
  [host port cmd]
  (let [ch (lamina.core/wait-for-result
            (aleph.tcp/tcp-client {:host host :port port :frame redis-codec}))]
    (log/info "cmd is" cmd)
    (lamina.core/enqueue ch cmd)
    (let [response [(lamina.core/wait-for-message ch)]]
      (log/info "response is " response)
      response)))

(defn redis-handler
  "Responsible for listening for commands and sending to the proper machine"
  [ch client-info]
  (lamina.core/receive-all ch
                           #(lamina.core/enqueue ch
                                                 (send-to-redis
                                                  "localhost" 6379 %))))
