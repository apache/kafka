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

