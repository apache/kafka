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
(ns #^{:doc "Base kafka-clj types."}
  kafka.types)

(deftype #^{:doc "Message type, a wrapper around a byte array."}
  Message [^bytes message])

(defprotocol Pack
  "Pack protocol converts an object to a Message."
  (pack [this] "Convert object to a Message."))

(defprotocol Unpack
  "Unpack protocol, reads an object from a Message."
  (unpack [^Message this] "Read an object from the message."))

(defprotocol Producer
  "Producer protocol."
  (produce [this topic partition messages] "Send message[s] for a topic to a partition.")
  (close   [this]                          "Closes the producer, socket and channel."))

(defprotocol Consumer
  "Consumer protocol."
  (consume      [this topic partition offset max-size]  "Fetch messages. Returns a pair [last-offset, message sequence]")
  (offsets      [this topic partition time max-offsets] "Query offsets. Returns offsets seq.")

  (consume-seq  [this topic partition]                  
                [this topic partition opts]             "Creates a sequence over the consumer.")
  (close        [this]                                  "Close the consumer, socket and channel."))

