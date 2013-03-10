(ns ratis.redis
  (:require
   [aleph.redis.protocol :as proto]
   [clojure.string :as str]
   [gloss.io]
   [aleph.tcp]
   [lamina.core]))

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
    (lamina.core/enqueue ch cmd)
    (lamina.core/wait-for-message ch)))
