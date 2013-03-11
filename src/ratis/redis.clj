(ns ratis.redis
  (:require
   [clojure.tools.logging :as log]
   [clojure.string :as str]
   [gloss.io]
   [gloss.core]
   [aleph.tcp]
   [lamina.core]))

;;; a lot of this code is taking from aleph
(defn string-prefix [count-offset]
  (gloss.core/prefix (gloss.core/string-integer :ascii :delimiters ["\r\n"] :as-str true)
          #(if (neg? %) 0 (+ % count-offset))
          #(if-not % -1 (- % count-offset))))

(def format-byte
  (gloss.core/enum :byte
        {:error \-
         :single-line \+
         :integer \:
         :bulk \$
         :multi-bulk \*}))

(defn codec-map [charset]
  (let [m {:error (gloss.core/string charset :delimiters ["\r\n"])
           :single-line (gloss.core/string charset :delimiters ["\r\n"])
           :integer (gloss.core/string-integer :ascii :delimiters ["\r\n"])
           :bulk (gloss.core/finite-frame (string-prefix 2)
                                          (gloss.core/string charset
                                           :suffix "\r\n"))}
        m (into {}
                (map
                 (fn [[k v]] [k (gloss.core/compile-frame [k v])])
                 m))
        m (atom m)]
    (swap! m assoc
           :multi-bulk (gloss.core/compile-frame
                        [:multi-bulk
                         (gloss.core/repeated
                          (gloss.core/header format-byte #(@m %) first)
                          :prefix (string-prefix 0))]))
    @m))

(defn create-redis-codec [charset]
  (let [codecs (codec-map charset)]
    (gloss.core/compile-frame (gloss.core/header format-byte codecs first))))

(def redis-codec (create-redis-codec :utf-8))

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
    (println "cmd is" cmd)
    (lamina.core/enqueue ch cmd)
    (let [response [(lamina.core/wait-for-message ch)]]
      (log/info "response is " response)
      (if (= 1 (count response)) (first response)
          response))))

(defn redis-handler
  "Responsible for listening for commands and sending to the proper machine"
  [ch client-info]
  (lamina.core/receive-all ch
                           #(lamina.core/enqueue ch
                                                 (send-to-redis
                                                  "localhost" 6379 %))))
