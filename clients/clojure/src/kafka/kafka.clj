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
(ns #^{:doc "Core kafka-clj module,
            provides producer and consumer factories."}
  kafka.kafka
  (:use (kafka types buffer)
        (clojure.contrib logging))
  (:import (kafka.types Message)
           (java.nio.channels SocketChannel)
           (java.net Socket InetSocketAddress)
           (java.util.zip CRC32)))

; 
; Utils
;

(defn- crc32-int
  "CRC for byte array."
  [^bytes ba]
  (let [crc (doto (CRC32.) (.update ba))
        lv  (.getValue crc)]
    (.intValue (bit-and lv 0xffffffff))))

(defn- new-channel
  "Create and setup a new channel for a host name, port and options.
  Supported options:
  :receive-buffer-size - receive socket buffer size, default 65536.
  :send-buffer-size    - send socket buffer size, default 65536.
  :socket-timeout      - socket timeout."
  [^String host ^Integer port opts]
  (let [receive-buf-size (or (:receive-buffer-size opts) 65536)
        send-buf-size    (or (:send-buffer-size opts) 65536)
        so-timeout       (or (:socket-timeout opts) 60000)
        ch (SocketChannel/open)]
    (doto (.socket ch)
      (.setReceiveBufferSize receive-buf-size)
      (.setSendBufferSize send-buf-size)
      (.setSoTimeout so-timeout))
    (doto ch
      (.configureBlocking true)
      (.connect (InetSocketAddress. host port)))))

(defn- close-channel
  "Close the channel."
  [^SocketChannel channel]
  (.close channel)
  (.close (.socket channel)))

(defn- response-size
  "Read first four bytes from channel as an integer."
  [channel]
  (with-buffer (buffer 4)
    (read-completely-from channel)
    (flip)
    (get-int)))

(defmacro with-error-code
  "Convenience response error code check."
  [request & body]
  `(let [error-code# (get-short)] ; error code
     (if (not= error-code# 0)
       (error (str "Request " ~request " returned error code: " error-code# "."))
       ~@body)))

; 
; Producer
;

(defn- send-message
  "Send messages."
  [channel topic partition messages opts]
  (let [size (or (:send-buffer-size opts) 65536)]
    (with-buffer (buffer size)
        (length-encoded int                   ; request size
          (put (short 0))                     ; request type
          (length-encoded short               ; topic size
            (put topic))                      ; topic
          (put (int partition))               ; partition
          (length-encoded int                 ; messages size
            (doseq [m messages]
              (let [^Message pm (pack m)]
                (length-encoded int           ; message size
                  (put (byte 0))              ; magic
                  (with-put 4 crc32-int       ; crc
                    (put (.message pm)))))))) ; message
        (flip)
        (write-to channel))))

(defn producer
  "Producer factory. See new-channel for list of supported options."
  [host port & [opts]]
  (let [channel (new-channel host port opts)]
    (reify Producer
      (produce [this topic partition messages]
               (let [msg (if (sequential? messages) messages [messages])]
                 (send-message channel topic partition msg opts)))
      (close [this]
             (close-channel channel)))))

;
; Consumer
;

; Offset

(defn- offset-fetch-request
  "Fetch offsets request."
  [channel topic partition time max-offsets]
  (let [size     (+ 4 2 2 (count topic) 4 8 4)]
    (with-buffer (buffer size)
      (length-encoded int         ; request size
        (put (short 4))           ; request type
        (length-encoded short     ; topic size
          (put topic))            ; topic
        (put (int partition))     ; partition
        (put (long time))         ; time
        (put (int max-offsets)))  ; max-offsets
        (flip)
        (write-to channel))))

(defn- fetch-offsets
  "Fetch offsets as an integer sequence."
  [channel topic partition time max-offsets]
  (offset-fetch-request channel topic partition time max-offsets)
  (let [rsp-size (response-size channel)]
    (with-buffer (buffer rsp-size)
      (read-completely-from channel)
      (flip)
      (with-error-code "Fetch-Offsets"
        (loop [c (get-int) res []]
          (if (> c 0)
            (recur (dec c) (conj res (get-long)))
            (doall res)))))))
 
; Messages

(defn- message-fetch-request
  "Fetch messages request."
  [channel topic partition offset max-size]
  (let [size (+ 4 2 2 (count topic) 4 8 4)]
    (with-buffer (buffer size)
      (length-encoded int     ; request size
        (put (short 1))       ; request type
        (length-encoded short ; topic size
          (put topic))        ; topic
        (put (int partition)) ; partition
        (put (long offset))   ; offset
        (put (int max-size))) ; max size
        (flip)
        (write-to channel))))

(defn- read-response
  "Read response from buffer. Returns a pair [new offset, messages sequence]."
  [offset]
  (with-error-code "Fetch-Messages"
    (loop [off offset msg []]
      (if (has-remaining)
        (let [size    (get-int)  ; message size
              magic   (get-byte) ; magic
              crc     (get-int)  ; crc
              message (get-array (- size 5))]
          (recur (+ off size 4) (conj msg (unpack (Message. message)))))
          [off (doall msg)]))))

(defn- fetch-messages
  "Message fetch, returns a pair [new offset, messages sequence]."
  [channel topic partition offset max-size]
  (message-fetch-request channel topic partition offset max-size)
  (let [rsp-size (response-size channel)]
    (with-buffer (buffer rsp-size)
      (read-completely-from channel)
      (flip)
      (read-response offset))))

; Consumer sequence

(defn- seq-fetch
  "Non-blocking fetch function used by consumer sequence."
  [channel topic partition opts]
  (let [max-size (or (:max-size opts) 1000000)]
    (fn [offset]
      (fetch-messages channel topic partition offset max-size))))

(defn- blocking-seq-fetch
  "Blocking fetch function used by consumer sequence."
  [channel topic partition opts]
  (let [repeat-count   (or (:repeat-count opts) 10)
        repeat-timeout (or (:repeat-timeout opts) 1000)
        max-size       (or (:max-size opts) 1000000)]
    (fn [offset]
      (loop [c repeat-count]
        (if (> c 0)
          (let [rs (fetch-messages channel topic partition offset max-size)]
            (if (or (nil? rs) (= offset (first rs)))
              (do
                (Thread/sleep repeat-timeout)
                (recur (dec c)))
              (doall rs)))
          (debug "Stopping blocking seq fetch."))))))

(defn- fetch-queue
  [offset queue fetch-fn]
  (if (empty? @queue)
    (let [[new-offset msg] (fetch-fn @offset)]
      (when new-offset
        (debug (str "Fetched " (count msg) " messages:"))
        (debug (str "New offset " new-offset "."))
        (swap! queue #(reduce conj % (reverse msg)))
        (reset! offset new-offset)))))

(defn- consumer-seq
  "Sequence constructor."
  [offset fetch-fn]
  (let [offset (atom offset)
        queue  (atom (seq []))]
    (reify
      clojure.lang.IPersistentCollection
        (seq [this]    this)
        (cons [this _] (throw (Exception. "cons not supported for consumer sequence.")))
        (empty [this]  nil)
        (equiv [this o]
          (fatal "Implement equiv for consumer seq!")
          false)
      clojure.lang.ISeq
        (first [this] 
          (fetch-queue offset queue fetch-fn)
          (first @queue))
        (next [this]
          (swap! queue rest)
          (fetch-queue offset queue fetch-fn)
          (if (not (empty? @queue)) this))
        (more [this]
          (swap! queue rest)
          (fetch-queue offset queue fetch-fn)
          (if (empty? @queue) (empty) this))
      Object
      (toString [this]
        (str "ConsumerQueue")))))

; Consumer factory 

(defn consumer
  "Consumer factory. See new-channel for list of supported options."
  [host port & [opts]]
  (let [channel (new-channel host port opts)]
    (reify Consumer
      (consume [this topic partition offset max-size]
        (fetch-messages channel topic partition offset max-size))
      
      (offsets [this topic partition time max-offsets]
        (fetch-offsets channel topic partition time max-offsets))

      (consume-seq [this topic partition]
        (let [[offset] (fetch-offsets channel topic partition -1 1)]
          (debug (str "Initializing last offset to " offset "."))
          (consumer-seq (or offset 0) (seq-fetch channel topic partition opts))))

      (consume-seq [this topic partition opts]
        (let [[offset] (or (:offset opts)
                           (fetch-offsets channel topic partition -1 1))
              fetch-fn (if (:blocking opts)
                         (blocking-seq-fetch channel topic partition opts)
                         (seq-fetch channel topic partition opts))]
          (debug (str "Initializing last offset to " offset "."))
          (consumer-seq (or offset 0) fetch-fn)))

      (close [this]
        (close-channel channel)))))

