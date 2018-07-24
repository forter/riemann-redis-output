(ns riemann-redis-output.core
  (:require [taoensso.carmine :as car]
            [cheshire.core :as json]
            [clojure.tools.logging :refer [error info debug]]
            [clojure.set :refer [rename-keys]]
            [riemann.service :refer [Service ServiceEquiv]]
            [riemann.config :refer [service!]]))

(def default-opts {:buff-size 1000 :conn-spec {}})

(defn- ^{:testable true} consume-until-empty
  "Consume messages from a queue until no more are available or `limit` messages have been consumed.
   Returns a vector of consumed messages"
  [queue limit]
  (loop [events [] ^String event (.take queue)] ; blocks until a message is available on channel
    (if (or (nil? event) (>= (count events) limit))
      events ; return events we got and end the loop
      (recur (conj events event) (.poll queue))))) ; try to get more messages from channel

; for performance reasons we prefer to send events to redis using a pipeline. We therefor consume events from the queue in bulks using consume-until-empty
(defn- redis-flush
  "Send messages to redis. Consumes messages from queue"
  [queue server-conn flush-size]
  {:pre [(> flush-size 0) (map? server-conn)]}
  (try
    (car/wcar server-conn
      (doseq [^String event (consume-until-empty queue flush-size)]
        (debug "Sending event to redis" event)
        (car/lpush "events" event)))
    (catch Exception e
      (error e "Failed to send event to redis"))))

(defprotocol RedisClient
  (send-event [service event]))

(defrecord RedisFlusherService [running core queue buff-size server-conn]
  ServiceEquiv
  (equiv? [this other] (= [buff-size server-conn] (select-keys other [:buff-size :server-conn])))
  Service
  (start! [this]
    (locking this
      (when-not @running
        (reset! running true)
        (let [new-queue (java.util.concurrent.ArrayBlockingQueue. buff-size)
              flush-size (int (/ buff-size 2))
              flusher-thread (Thread. #(while @running (redis-flush new-queue server-conn flush-size)))]
          (.start flusher-thread)
          (reset! queue new-queue)))))
  (stop! [this] (locking this (reset! running false)))
  (reload! [this new-core] (reset! core new-core))
  (conflict? [this other] (instance? RedisFlusherService other))
  RedisClient
  (send-event [{queue :queue} event] (.put @queue (json/generate-string event))))

(defn output
  "Get an event handler for redis. This function will start an asyncronous redis flusher RedisFlusherService.
  Usage:

  (redis-output) ; -> simplest form, no options
  (redis-output {:conn-spec {:db 13 :host \"redis\" :port 6379} :buff-size 1000}) ; -> with options

  Options:

  :conn-spec - carmine spec, e.g. {:host \"redisserver\" :port 6379 :db 13}
  :buff-size - Event queue buffer size
  "
  ([] (output {}))
  ([opts]
  (let [{:keys [conn-spec buff-size]} (merge default-opts opts)
        server-conn {:pool {} :spec conn-spec}
        transcode json/generate-string
        service (service!
                  (RedisFlusherService. (atom false) (atom nil) (atom nil) buff-size server-conn)) ; start the sender loop in a thread
        ]
    (partial send-event service))))
