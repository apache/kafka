(ns #^{:doc "Serialization for all Java Serializable objects."}
  kafka.serializable
  (:use kafka.types)
  (:import (kafka.types Message)
           (java.io Serializable 
                    ObjectOutputStream ByteArrayOutputStream
                    ObjectInputStream ByteArrayInputStream)))

(extend-type Serializable
  Pack
    (pack [this]
      (let [bas (ByteArrayOutputStream.)]
        (with-open [oos (ObjectOutputStream. bas)]
          (.writeObject oos this))
        (kafka.types.Message. (.toByteArray bas)))))

(extend-type Message
  Unpack
    (unpack [this]
      (with-open [ois (ObjectInputStream. (ByteArrayInputStream. (.message this)))]
        (.readObject ois))))

