(ns ratis.redis
  (:require
   [clojure.tools.logging :as log]
   [clojure.string :as str]
   [gloss.io]
   [gloss.core]
   [aleph.tcp]
   [lamina.core]))

(def format-byte
  (gloss.core/enum :byte
    {:error \-
     :single-line \+
     :integer \:
     :bulk \$
     :multi-bulk \*}))

(gloss.core/defcodec single-line-codec
  {:type :single-line
   :message (gloss.core/string :utf-8 :delimiters ["\r\n"])})

(gloss.core/defcodec error-codec
  {:type :error
   :message (gloss.core/string :ascii :delimiters ["\r\n"])})

(gloss.core/defcodec integer-codec
  {:type :integer
   :value (gloss.core/string-integer :ascii :delimiters ["\r\n"])})

(def codec-mapping
  {:single-line single-line-codec
   :error error-codec
   :integer integer-codec
   })

(gloss.core/defcodec redis-codec
  (gloss.core/header format-byte codec-mapping :type))

(def advanced-commands #{
                         "DISCARD"
                         "EXEC"
                         "MULTI"
                         "UNWATCH"
                         "WATCH"
                         })

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

(defn master-only-command?
  "Returns true / false if the command is for master only"
  [cmd]
  (contains? master-only (->> cmd second first second str/upper-case)))

(defn advanced-command?
  "Returns true / false if the command is an advanced command"
  [cmd]
  (contains? advanced-commands (->> cmd second first second str/upper-case)))

(defn start-transaction-command?
  "Returns true if the command is the start of a transaction"
  [cmd]
  (contains? #{"MULTI" "WATCH"} (->> cmd second first second str/upper-case)))

(defn finished-transaction?
  "Returns true when the command is the final one of a transaction"
  [cmd]
  (contains? #{"EXEC" "DISCARD"} (->> cmd second first second str/upper-case)))

(defn create-redis-connection
  [host port]
  (lamina.core/wait-for-result
   (aleph.tcp/tcp-client {:host host :port port :frame redis-codec})))

(defn send-to-redis-and-respond
  "Sends the command to the specified host and returns the response"
  [redis-connection cmd receiver]
  (log/info "Received command" cmd)
  (lamina.core/enqueue redis-connection cmd)
  (lamina.core/receive redis-connection (fn [response]
                                          (log/info "Command processed" cmd)
                                          (lamina.core/close redis-connection)
                                          (if (= 1 (count response))
                                            (lamina.core/enqueue receiver
                                                                 (first response))
                                              (lamina.core/enqueue receiver
                                                                   response)))))

(defn send-to-redis
  "Sends the command to the specified host and returns the response"
  [host port cmd]
  (let [ch (create-redis-connection host port)]
    (lamina.core/enqueue ch cmd)
    (let [response [(lamina.core/wait-for-message ch)]]
      (lamina.core/close ch)
      (if (= 1 (count response)) (first response)
          response))))

(def info-cmd [:multi-bulk [[:bulk "info"]]])

(defn query-server-state
  "Returns the map of the server's current state"
  [host port]
  (let [response (send-to-redis host port info-cmd)
        info-string (second response)
        status {}
        final-status (map #(assoc status (keyword (first %)) (second %))
                          (map #(clojure.string/split % #":") (re-seq #"\w+:\w+" info-string)
                               ))]
    (reduce merge final-status)))
