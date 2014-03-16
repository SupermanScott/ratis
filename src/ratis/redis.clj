(ns ratis.redis
  (:require
   [clojure.tools.logging :as log]
   [clojure.string :as str]
   [gloss.io]
   [gloss.core]
   [aleph.tcp]
   [lamina.core]))

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

(gloss.core/defcodec single-line-codec
  {:type :single-line
   :message (gloss.core/string :utf-8 :delimiters ["\r\n"])})

(gloss.core/defcodec error-codec
  {:type :error
   :message (gloss.core/string :ascii :delimiters ["\r\n"])})

(gloss.core/defcodec integer-codec
  {:type :integer
   :value (gloss.core/string-integer :ascii :delimiters ["\r\n"])})

(gloss.core/defcodec bulk-codec
  (gloss.core/header
   (gloss.core/string-integer :ascii :delimiters ["\r\n"])
   (fn [size]
     (if (= -1 size)
       (gloss.core/compile-frame {:type :bulk :value nil})
       (gloss.core/compile-frame {:type :bulk :value
                                  (gloss.core/string :utf-8 :length size :suffix "\r\n")})
       ))
   (fn [body]
     (if (:value body)
       (count (:value body))
       -1))))

(def codec-mapping
  {:single-line single-line-codec
   :error error-codec
   :integer integer-codec
   :bulk bulk-codec
   })

(gloss.core/defcodec multi-bulk-codec
  {:type :multi-bulk
   :payload (gloss.core/repeated
             (gloss.core/header format-byte codec-mapping :type)
             :prefix (string-prefix 0))})

(gloss.core/defcodec redis-codec
  (gloss.core/header format-byte
                     (assoc codec-mapping :multi-bulk multi-bulk-codec)
                     :type))

(def advanced-commands #{
                         "EXEC"
                         "MULTI"
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
                   "DISCARD"
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
                   "UNWATCH"
                   "ZADD"
                   "ZINCRBY"
                   "ZINTERSTORE"
                   "ZREM"
                   "ZREMRANGEBYRANK"
                   "ZREMRANGEBYSCORE"
                   "ZUNIONSTORE"})

(defn- find-command
  [cmd]
  (->> cmd :payload first :value str/upper-case))

(defn master-only-command?
  "Returns true / false if the command is for master only"
  [cmd]
  (contains? master-only (find-command cmd)))

(defn advanced-command?
  "Returns true / false if the command is an advanced command"
  [cmd]
  (contains? advanced-commands (find-command cmd)))

(defn start-transaction-command?
  "Returns true if the command is the start of a transaction"
  [cmd]
  (contains? #{"MULTI" "WATCH"} (find-command cmd)))

(defn finished-transaction?
  "Returns true when the command is the final one of a transaction"
  [cmd]
  (contains? #{"EXEC" "DISCARD"} (find-command cmd)))

(defn create-redis-connection
  [host port]
  (lamina.core/wait-for-result
   (aleph.tcp/tcp-client {:host host :port port :frame redis-codec})))

(defn send-to-redis-and-respond
  "Sends the command to the specified host and returns the response"
  [redis-connection cmd receiver recycle-channel]
  (log/info "Received command" cmd)
  (lamina.core/enqueue redis-connection cmd)
  (lamina.core/receive redis-connection (fn [response]
                                          (log/info "Command processed" cmd response)
                                          (lamina.core/enqueue
                                           recycle-channel redis-connection)
                                          (if (= 1 (count response))
                                            (lamina.core/enqueue receiver
                                                                 (first response))
                                              (lamina.core/enqueue receiver
                                                                   response)))))

(defn send-to-redis
  "Sends the command to the specified host and returns the response"
  [ch cmd]
  (lamina.core/enqueue ch cmd)
  (let [response [(lamina.core/wait-for-message ch)]]
    (if (= 1 (count response)) (first response)
        response)))

(def info-cmd {:type :multi-bulk :payload [{:type :bulk :value "INFO"}]})

(defn query-server-state
  "Returns the map of the server's current state"
  [connection recycle-ch]
  (let [response (send-to-redis connection info-cmd)
        info-string (:value response)
        status {}
        final-status (map #(assoc status (keyword (first %)) (second %))
                          (map #(clojure.string/split % #":") (re-seq #"\w+:\w+" info-string)
                               ))]
    (lamina.core/enqueue recycle-ch connection)
    (reduce merge final-status)))
