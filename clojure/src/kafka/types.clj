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

