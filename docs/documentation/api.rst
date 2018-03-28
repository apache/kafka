.. _kafka-api:

Kafka APIs
==========

.. contents::
    :local:

Kafka includes five core APIs:

#. The `Producer <#producerapi>`__ API allows applications to send
   streams of data to topics in the Kafka cluster.
#. The `Consumer <#consumerapi>`__ API allows applications to read
   streams of data from topics in the Kafka cluster.
#. The `Streams <#streamsapi>`__ API allows transforming streams of data
   from input topics to output topics.
#. The `Connect <#connectapi>`__ API allows implementing connectors that
   continually pull from some source system or application into Kafka or
   push from Kafka into some sink system or application.
#. The `AdminClient <#adminapi>`__ API allows managing and inspecting
   topics, brokers, and other Kafka objects.

Kafka exposes all its functionality over a language independent protocol
which has clients available in many programming languages. However only
the Java clients are maintained as part of the main Kafka project, the
others are available as independent open source projects. A list of
non-Java clients is available
`here <https://cwiki.apache.org/confluence/display/KAFKA/Clients>`__.

.. _producerapi:

----------------
2.1 Producer API
----------------

The Producer API allows applications to send streams of data to topics
in the Kafka cluster.

Examples showing how to use the producer are given in the
:javadoc:`javadocs|org/apache/kafka/clients/producer/KafkaProducer.html`.

To use the producer, you can use the following maven dependency:

.. codewithvars:: bash

            <dependency>
                <groupId>org.apache.kafka</groupId>
                <artifactId>kafka-clients</artifactId>
                <version>|release|</version>
            </dependency>
        

.. _ consumerapi:

----------------
2.2 Consumer API
----------------

The Consumer API allows applications to read streams of data from topics
in the Kafka cluster.

Examples showing how to use the consumer are given in the
:javadoc:`javadocs|org/apache/kafka/clients/consumer/KafkaConsumer.html`.

To use the consumer, you can use the following maven dependency:

.. codewithvars:: bash

            <dependency>
                <groupId>org.apache.kafka</groupId>
                <artifactId>kafka-clients</artifactId>
                <version>|release|</version>
            </dependency>
        

.. _streamsapi:

---------------
2.3 Streams API
---------------

The `Streams <#streamsapi>`__ API allows transforming streams of data
from input topics to output topics.

Examples showing how to use this library are given in the
:javadoc:`javadocs|org/apache/kafka/streams/KafkaStreams.html`

Additional documentation on using the Streams API is available
:ref:`here <kafka-streams>`.

To use Kafka Streams you can use the following maven dependency:

.. codewithvars:: bash

            <dependency>
                <groupId>org.apache.kafka</groupId>
                <artifactId>kafka-streams</artifactId>
                <version>|release|</version>
            </dependency>
        

.. _connectapi:

---------------
2.4 Connect API
---------------

The Connect API allows implementing connectors that continually pull
from some source data system into Kafka or push from Kafka into some
sink data system.

Many users of Connect won't need to use this API directly, though, they
can use pre-built connectors without needing to write any code.
Additional information on using Connect is available
`here </documentation.html#connect>`__.

Those who want to implement custom connectors can see the
:javadoc:`javadoc|org/apache/kafka/connect>`.

.. _adminapi:

-------------------
2.5 AdminClient API
-------------------

The AdminClient API supports managing and inspecting topics, brokers,
acls, and other Kafka objects.

To use the AdminClient API, add the following Maven dependency:

.. codewithvars:: bash

            <dependency>
                <groupId>org.apache.kafka</groupId>
                <artifactId>kafka-clients</artifactId>
                <version>|release|</version>
            </dependency>
        

For more information about the AdminClient APIs, see the
:javadoc:`javadoc|org/apache/kafka/clients/admin/AdminClient.html`.

.. _legacyapis:

---------------
2.6 Legacy APIs
---------------

A more limited legacy producer and consumer API is also included in
Kafka. These old Scala APIs are deprecated and only still available for
compatibility purposes. Information on them can be found here
`here </081/documentation.html#producerapi>`__.
