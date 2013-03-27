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

(defn finished-transaction?
  "Returns true when the command is the final one of a transaction"
  [cmd]
  (contains? #{"EXEC" "DISCARD"} (->> cmd second first second str/upper-case)))

(defn send-to-redis-and-respond
  "Sends the command to the specified host and returns the response"
  [host port cmd receiver]
  (log/info "Received command" cmd "sending to" host port)
  (let [ch (lamina.core/wait-for-result
            (aleph.tcp/tcp-client {:host host :port port :frame redis-codec}))]
    (lamina.core/enqueue ch cmd)
    (lamina.core/receive ch (fn [response]
      (log/info "Command processed" cmd)
      (lamina.core/close ch)
      (if (= 1 (count response)) (lamina.core/enqueue receiver (first response))
          (lamina.core/enqueue receiver response))))))

(defn send-to-redis
  "Sends the command to the specified host and returns the response"
  [host port cmd]
  (let [ch (lamina.core/wait-for-result
            (aleph.tcp/tcp-client {:host host :port port :frame redis-codec}))]
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
