# kafka-clj
kafka-clj provides a producer and consumer that supports a basic fetch API as well as a managed sequence interface. Multifetch is not supported yet.

## Quick Start

Download and start [Kafka](http://sna-projects.com/kafka/quickstart.php). 

Pull dependencies with [Leiningen](https://github.com/technomancy/leiningen):

    $ lein deps

And run the example:

    $ lein run-example

## Usage

### Sending messages

    (with-open [p (producer "localhost" 9092)]
      (produce p "test" 0 "Message 1")
      (produce p "test" 0 ["Message 2" "Message 3"]))

### Simple consumer

    (with-open [c (consumer "localhost" 9092)]
      (let [offs (offsets c "test" 0 -1 10)]
        (consume c "test" 0 (last offs) 1000000)))

### Consumer sequence

    (with-open [c (consumer "localhost" 9092)]
      (doseq [m (consume-seq c "test" 0 {:blocking true})]
        (println m)))

Following options are supported:

* :blocking _boolean_ default false, sequence returns nil the first time fetch does not return new messages. If set to true, the sequence tries to fetch new messages :repeat-count times every :repeat-timeout milliseconds. 
* :repeat-count _int_ number of attempts to fetch new messages before terminating, default 10.
* :repeat-timeout _int_ wait time in milliseconds between fetch attempts, default 1000.
* :offset   _long_ initialized to highest offset if not provided.
* :max-size _int_  max result message size, default 1000000.

### Serialization

Load namespace _kafka.print_ for basic print_dup/read-string serialization or _kafka.serializeable_ for Java object serialization. For custom serialization implement Pack and Unpack protocols.


Questions? Email adam.smyczek \_at\_ gmail.com.

