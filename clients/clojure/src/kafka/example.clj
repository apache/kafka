;; Licensed to the Apache Software Foundation (ASF) under one or more
;; contributor license agreements.  See the NOTICE file distributed with
;; this work for additional information regarding copyright ownership.
;; The ASF licenses this file to You under the Apache License, Version 2.0
;; (the "License"); you may not use this file except in compliance with
;; the License.  You may obtain a copy of the License at
;; 
;;    http://www.apache.org/licenses/LICENSE-2.0
;; 
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
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

