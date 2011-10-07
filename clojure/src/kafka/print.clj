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
(ns #^{:doc "Basic Clojure print-dup -> read-string message serialization."}
  kafka.print
  (:use kafka.types)
  (:import (kafka.types Message)))

(extend-type Object
  Pack
    (pack [this]
      (let [^String st (with-out-str (print-dup this *out*))]
        (kafka.types.Message. (.getBytes st "UTF-8")))))

(extend-type Message
  Unpack
    (unpack [this] 
      (let [^bytes ba  (.message this)
                   msg (String. ba "UTF-8")]
        (if (not (empty? msg))
          (try
            (read-string msg)
            (catch Exception e
              (println "Invalid expression " msg)))))))

