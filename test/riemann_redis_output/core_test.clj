(ns riemann-redis-output.core-test
  (:use midje.sweet)
  (:use [riemann-redis-output.core])
  (:require [midje.util :refer [expose-testables]]))

(expose-testables riemann-redis-output.core)

(fact "`consume-until-empty` should consume all messages from queue"
  (let [messages (range 100)
        queue (java.util.concurrent.ArrayBlockingQueue. 1000)]
        (doseq [msg messages] (.put queue msg))
        (consume-until-empty queue 100) => (range 100)
        (doseq [msg (range 100)] (.put queue msg))
        (consume-until-empty queue 10) => (range 10)))
