.. _kafka-streams::

Kafka Streams
=============

.. toctree::
   :maxdepth: 1

   quickstart
   tutorial
   core-concepts
   architecture
   developer-guide/index
   upgrade-guide


*The easiest way to write mission-critical real-time applications and microservices*

Kafka Streams is a client library for building applications and
microservices, where the input and output data are stored in Kafka
clusters. It combines the simplicity of writing and deploying standard
Java and Scala applications on the client side with the benefits of
Kafka's server-side cluster technology.

Click here to watch a screencast of the `Intro to Streams on YouTube <https://www.youtube.com/embed/Z3JKCLG3VP4>`_.

.. raw:: html

    <div style="position: relative; padding-bottom: 56.25%; height: 0; overflow: hidden; max-width: 100%; height: auto;">
        <iframe src="https://www.youtube.com/embed/Z3JKCLG3VP4" frameborder="0" allowfullscreen style="position: absolute; top: 0; left: 0; width: 100%; height: 100%;"></iframe>
    </div>

This video is the first in a series of Streams tutorial videos:

#. `Creating a Streams Application <https://www.youtube.com/watch?v=LxxeXI1mPKo>`_
#. `Transforming Data Pt. 1 <https://www.youtube.com/watch?v=7JYEEx7SBuE>`_
#. `Transforming Data Pt. 2 <https://www.youtube.com/watch?v=3kJgYIkAeHs>`_


Why you'll love using Kafka Streams!
------------------------------------

-  Elastic, highly scalable, fault-tolerant
-  Deploy to containers, VMs, bare metal, cloud
-  Equally viable for small, medium, & large use cases
-  Fully integrated with Kafka security
-  Write standard Java applications
-  Exactly-once processing semantics
-  No separate processing cluster required
-  Develop on Mac, Linux, Windows

.. raw:: html

     <div class="first__app__cta">
         <a href="/{{version}}/documentation/streams/tutorial" class="first__app__btn">Write your first app</a>
     </div>

--------------

Kafka Streams use cases
-----------------------

.. image:: ../../images/powered-by/NYT.jpg
    :target: https://open.nytimes.com/publishing-with-apache-kafka-at-the-new-york-times-7f0e3b7d2077
    :align: center

`The New York Times uses Apache
Kafka <https://open.nytimes.com/publishing-with-apache-kafka-at-the-new-york-times-7f0e3b7d2077>`__ and
the Kafka Streams to store and distribute, in real-time, published
content to the various applications and systems that make it available
to the readers.

.. image:: ../../images/powered-by/zalando.jpg
    :target: https://www.confluent.io/blog/ranking-websites-real-time-apache-kafkas-streams-api/
    :align: center

As the leading online fashion retailer in Europe, Zalando uses Kafka as
an ESB (Enterprise Service Bus), which helps us in transitioning from a
monolithic to a micro services architecture. Using Kafka for processing
`event
streams <https://www.confluent.io/blog/ranking-websites-real-time-apache-kafkas-streams-api/>`__
enables our technical team to do near-real time business intelligence.

.. image:: ../../images/powered-by/line.png
    :target: https://engineering.linecorp.com/en/blog/detail/80
    :align: center
    :scale: 50 %

`LINE uses Apache
Kafka <https://engineering.linecorp.com/en/blog/detail/80>`__ as a
central datahub for our services to communicate to one another. Hundreds
of billions of messages are produced daily and are used to execute
various business logic, threat detection, search indexing and data
analysis. LINE leverages Kafka Streams to reliably transform and filter
topics enabling sub topics consumers can efficiently consume, meanwhile
retaining easy maintainability thanks to its sophisticated yet minimal
code base.

.. image:: ../../images/powered-by/pinterest.png
    :target: https://medium.com/@Pinterest_Engineering/using-kafka-streams-api-for-predictive-budgeting-9f58d206c996
    :align: center
    :scale: 50 %

`Pinterest uses Apache Kafka and the Kafka
Streams <https://medium.com/@Pinterest_Engineering/using-kafka-streams-api-for-predictive-budgeting-9f58d206c996>`__
at large scale to power the real-time, predictive budgeting system of
their advertising infrastructure. With Kafka Streams, spend predictions
are more accurate than ever.

.. image:: ../../images/powered-by/rabobank.jpg
    :target: https://www.confluent.io/blog/real-time-financial-alerts-rabobank-apache-kafkas-streams-api/
    :align: center
    :scale: 50 %

Rabobank is
one of the 3 largest banks in the Netherlands. Its digital
nervous system, the Business Event Bus, is powered by Apache Kafka. It
is used by an increasing amount of financial processes and services, one
of which is Rabo Alerts. This service alerts customers in real-time upon
financial events and is `built using Kafka
Streams. <https://www.confluent.io/blog/real-time-financial-alerts-rabobank-apache-kafkas-streams-api/>`__

.. image:: ../../images/powered-by/trivago.png
    :target: https://speakerdeck.com/xenji/kafka-and-debezium-at-trivago-code-dot-talks-2017-edition
    :align: center
    :scale: 25 %

Trivago is a global hotel search platform. We are focused on reshaping
the way travelers search for and compare hotels, while enabling hotel
advertisers to grow their businesses by providing access to a broad
audience of travelers via our websites and apps. As of 2017, we offer
access to approximately 1.8 million hotels and other accommodations in
over 190 countries. We use Kafka, Kafka Connect, and Kafka Streams to
`enable our
developers <https://speakerdeck.com/xenji/kafka-and-debezium-at-trivago-code-dot-talks-2017-edition>`__
to access data freely in the company. Kafka Streams powers parts of our
analytics pipeline and delivers endless options to explore and operate
on the data sources we have at hand.



Hello Kafka Streams
-------------------

The code example below implements a WordCount application that is
elastic, highly scalable, fault-tolerant, stateful, and ready to run in
production at large scale.

.. contents::
    :local:

-------
Java 8+
-------

.. code:: bash

       import org.apache.kafka.common.serialization.Serdes;
       import org.apache.kafka.streams.KafkaStreams;
       import org.apache.kafka.streams.StreamsBuilder;
       import org.apache.kafka.streams.StreamsConfig;
       import org.apache.kafka.streams.Topology;
       import org.apache.kafka.streams.kstream.Materialized;
       import org.apache.kafka.streams.kstream.Produced;
       import org.apache.kafka.streams.state.KeyValueStore;

       import java.util.Arrays;
       import java.util.Properties;

       public class WordCountApplication {

           public static void main(final String[] args) throws Exception {
               Properties config = new Properties();
               config.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
               config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-broker1:9092");
               config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
               config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

               StreamsBuilder builder = new StreamsBuilder();
               KStream<String, String> textLines = builder.stream("TextLinesTopic");
               KTable<String, Long> wordCounts = textLines
                   .flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split("\\W+")))
                   .groupBy((key, word) -> word)
                   .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"));
               wordCounts.toStream().to("WordsWithCountsTopic", Produced.with(Serdes.String(), Serdes.Long()));

               KafkaStreams streams = new KafkaStreams(builder.build(), config);
               streams.start();
           }

       }
                   

------
Java 7
------

.. code:: bash

                       import org.apache.kafka.common.serialization.Serdes;
                       import org.apache.kafka.streams.KafkaStreams;
                       import org.apache.kafka.streams.StreamsBuilder;
                       import org.apache.kafka.streams.StreamsConfig;
                       import org.apache.kafka.streams.Topology;
                       import org.apache.kafka.streams.kstream.KeyValueMapper;
                       import org.apache.kafka.streams.kstream.Materialized;
                       import org.apache.kafka.streams.kstream.Produced;
                       import org.apache.kafka.streams.kstream.ValueMapper;
                       import org.apache.kafka.streams.state.KeyValueStore;

                       import java.util.Arrays;
                       import java.util.Properties;
           
                       public class WordCountApplication {
           
                           public static void main(final String[] args) throws Exception {
                               Properties config = new Properties();
                               config.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
                               config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-broker1:9092");
                               config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
                               config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
           
                               StreamsBuilder builder = new StreamsBuilder();
                               KStream<String, String> textLines = builder.stream("TextLinesTopic");
                               KTable<String, Long> wordCounts = textLines
                                   .flatMapValues(new ValueMapper<String, Iterable<String>>() {
                                       @Override
                                       public Iterable<String> apply(String textLine) {
                                           return Arrays.asList(textLine.toLowerCase().split("\\W+"));
                                       }
                                   })
                                   .groupBy(new KeyValueMapper<String, String, String>() {
                                       @Override
                                       public String apply(String key, String word) {
                                           return word;
                                       }
                                   })
                                   .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"));


                               wordCounts.toStream().to("WordsWithCountsTopic", Produced.with(Serdes.String(), Serdes.Long()));
           
                               KafkaStreams streams = new KafkaStreams(builder.build(), config);
                               streams.start();
                           }
           
                       }
                   

-----
Scala
-----

.. code:: bash

                       import java.lang.Long
                       import java.util.Properties
                       import java.util.concurrent.TimeUnit
           
                       import org.apache.kafka.common.serialization._
                       import org.apache.kafka.streams._
                       import org.apache.kafka.streams.kstream.{KeyValueMapper, Materialized, Produced, ValueMapper}
                       import org.apache.kafka.streams.state.KeyValueStore;
           
                       import scala.collection.JavaConverters.asJavaIterableConverter
           
                       object WordCountApplication {
           
                           def main(args: Array[String]) {
                               val config: Properties = {
                                   val p = new Properties()
                                   p.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application")
                                   p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-broker1:9092")
                                   p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
                                   p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
                                   p
                               }
           
                               val builder: StreamsBuilder = new StreamsBuilder()
                               val textLines: KStream[String, String] = builder.stream("TextLinesTopic")
                               val wordCounts: KTable[String, Long] = textLines
                                   .flatMapValues(textLine => textLine.toLowerCase.split("\\W+").toIterable.asJava)
                                   .groupBy((_, word) => word)
                                   .count(Materialized.as("counts-store").asInstanceOf[Materialized[String, Long, KeyValueStore[Bytes, Array[Byte]]]])
                               wordCounts.toStream().to("WordsWithCountsTopic", Produced.with(Serdes.String(), Serdes.Long()))
           
                               val streams: KafkaStreams = new KafkaStreams(builder.build(), config)
                               streams.start()
           
                               Runtime.getRuntime.addShutdownHook(new Thread(() => {
                                   streams.close(10, TimeUnit.SECONDS)
                               }))
                           }
           
                       }
                   
