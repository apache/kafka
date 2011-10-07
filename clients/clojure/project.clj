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
(defproject kafka-clj "0.1-SNAPSHOT"
  :description "Kafka client for Clojure."
  :url          "http://sna-projects.com/kafka/"
  :dependencies [[org.clojure/clojure	"1.2.0"]
                 [org.clojure/clojure-contrib	"1.2.0"]
                 [log4j "1.2.15" :exclusions [javax.mail/mail
                                              javax.jms/jms
                                              com.sun.jdmk/jmxtools
                                              com.sun.jmx/jmxri]]]
  :disable-deps-clean false
  :warn-on-reflection true
  :source-path "src"
  :test-path "test")
