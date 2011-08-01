(ns #^{:doc "Wrapper around ByteBuffer,
            provides a DSL to model byte messages."}
  kafka.buffer
  (:import (java.nio ByteBuffer)
           (java.nio.channels SocketChannel)))

(def #^{:doc "Buffer stack bind in with-buffer."}
  *buf* [])

(def #^{:doc "Number of attempts to read a complete buffer from channel."}
  *channel-read-count* 5)

;
; Main buffer functions
;

(defn buffer
  "Creates a new ByteBuffer of argument size."
  [^int size]
  (ByteBuffer/allocate size))

(defn ^ByteBuffer top
  "Returns top buffer from *buf* stack."
  []
  (peek *buf*))

(defn flip
  []
  (.flip (top)))

(defn rewind
  []
  (.rewind (top)))

(defn clear
  []
  (.clear (top)))

(defn has-remaining
  []
  (.hasRemaining (top)))

;
; Write to buffer
;

(defprotocol Put
  "Put protocol defines a generic buffer put method."
  (put [this]))

(extend-type Byte
  Put
    (put [this] (.put (top) this)))

(extend-type Integer
  Put
    (put [this] (.putInt (top) this)))

(extend-type Short
  Put
    (put [this] (.putShort (top) this)))

(extend-type Long
  Put
    (put [this] (.putLong (top) this)))

(extend-type String
  Put
    (put [this] (.put (top) (.getBytes this "UTF-8"))))

(extend-type (class (byte-array 0))
  Put
    (put [this] (.put (top) ^bytes this)))

(extend-type clojure.lang.IPersistentCollection
  Put
    (put [this] (doseq [e this] (put e))))

(defmacro length-encoded
  [type & body]
  `(with-buffer (.slice (top))
     (put (~type 0))
     (let [^ByteBuffer this#      (top)
           ^ByteBuffer parent#    (peek (pop *buf*))
                       type-size# (.position this#)]
       ~@body
       (let [size# (.position this#)]
         (.rewind this#)
         (put (~type (- size# type-size#)))
         (.position parent# (+ (.position parent#) size#))))))

(defmacro with-put
  [size f & body]
  `(with-buffer (.slice (top))
     (put (byte-array ~size))
     ~@body
     (let [^ByteBuffer this#   (top)
           ^ByteBuffer parent# (peek (pop *buf*))
                       pos#    (.position this#)
                       ba#     (byte-array (- pos# ~size))]
       (doto this# (.rewind) (.get (byte-array ~size)) (.get ba#))
       (.rewind this#)
       (put (~f ba#))
       (.position parent# (+ (.position parent#) pos#)))))

;
; Read from buffer
;

(defn get-byte
  []
  (.get (top)))

(defn get-short
  []
  (.getShort (top)))

(defn get-int
  []
  (.getInt (top)))

(defn get-long
  []
  (.getLong (top)))

(defn get-array
  "Reads byte array of argument length from buffer."
  [^int length]
  (let [ba (byte-array length)]
    (.get (top) ba)
    ba))

(defn get-string
  "Reads string of argument length from buffer."
  [^int length]
  (let [ba (byte-array length)]
    (.get (top) ba)
    (String. ba "UTF-8")))

;
; Util functions and macros
;

(defmacro with-buffer
  "Evaluates body in the context of the buffer."
  [buffer & body]
  `(binding [*buf* (conj *buf* ~buffer)]
     ~@body))

(defn read-from
  "Reads from channel to the underlying top buffer.
  Throws ConnectException if channel is closed."
  [^SocketChannel channel]
  (let [size (.read channel (top))]
    (if (< size 0)
      (throw (java.net.ConnectException. "Channel closed?"))
      size)))

(defn read-completely-from
  "Read the complete top buffer from the channel."
  [^SocketChannel channel]
  (loop [t *channel-read-count* size 0]
    (let [s (read-from channel)]
      (cond
        (< t 0)
          (throw (Exception. "Unable to read complete buffer from channel."))
        (has-remaining)
          (recur (dec t) (+ size s))
        :else size))))

(defn write-to
  "Writes underlying top buffer to channel."
  [^SocketChannel channel]
  (.write channel (top)))

