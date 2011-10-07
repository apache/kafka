(ns #^{:doc "Producer/Consumer example."}
  kafka.example
  (:use (clojure.contrib logging)
        (kafka types kafka print)))

(defmacro thread
  "Executes body in a thread, logs exceptions."
  [ & body]
  `(future
     (try
       ~@body
       (catch Exception e#
         (error "Exception." e#)))))

(defn start-consumer
  []
  (thread
    (with-open [c (consumer "localhost" 9092)]
      (doseq [m (consume-seq c "test" 0 {:blocking true})]
        (println "Consumed <-- " m)))
    (println "Finished consuming.")))

(defn start-producer
  []
  (thread
    (with-open [p (producer "localhost" 9092)]
      (doseq [i (range 1 20)]
        (let [m (str "Message " i)]
          (produce p "test" 0 m)
          (println "Produced --> " m)
          (Thread/sleep 1000))))
    (println "Finished producing.")))

(defn run
  []
  (start-consumer)
  (start-producer))

