Tutorial: Write a Kafka Streams Application
===========================================

.. contents::
    :local:

In this guide we will start from scratch on setting up your own project
to write a stream processing application using Kafka Streams. It is
highly recommended to read the
`quickstart </%7B%7Bversion%7D%7D/documentation/streams/quickstart>`__
first on how to run a Streams application written in Kafka Streams if
you have not done so.

`Setting up a Maven Project <#tutorial_maven_setup>`__
------------------------------------------------------

We are going to use a Kafka Streams Maven Archetype for creating a
Streams project structure with the following commands:

.. codewithvars:: bash

            mvn archetype:generate \
                -DarchetypeGroupId=org.apache.kafka \
                -DarchetypeArtifactId=streams-quickstart-java \
                -DarchetypeVersion=|release| \
                -DgroupId=streams.examples \
                -DartifactId=streams.examples \
                -Dversion=0.1 \
                -Dpackage=myapps
        

You can use a different value for ``groupId``, ``artifactId`` and
``package`` parameters if you like. Assuming the above parameter values
are used, this command will create a project structure that looks like
this:

.. code:: bash

            > tree streams.examples
            streams-quickstart
            |-- pom.xml
            |-- src
                |-- main
                    |-- java
                    |   |-- myapps
                    |       |-- LineSplit.java
                    |       |-- Pipe.java
                    |       |-- WordCount.java
                    |-- resources
                        |-- log4j.properties
        

The ``pom.xml`` file included in the project already has the Streams
dependency defined, and there are already several example programs
written with Streams library under ``src/main/java``. Since we are going
to start writing such programs from scratch, we can now delete these
examples:

.. code:: bash

            > cd streams-quickstart
            > rm src/main/java/myapps/*.java
        

`Writing a first Streams application: Pipe <#tutorial_code_pipe>`__
-------------------------------------------------------------------

It's coding time now! Feel free to open your favorite IDE and import
this Maven project, or simply open a text editor and create a java file
under ``src/main/java``. Let's name it ``Pipe.java``:

.. code:: bash

            package myapps;

            public class Pipe {

                public static void main(String[] args) throws Exception {

                }
            }
        

We are going to fill in the ``main`` function to write this pipe
program. Note that we will not list the import statements as we go since
IDEs can usually add them automatically. However if you are using a text
editor you need to manually add the imports, and at the end of this
section we'll show the complete code snippet with import statement for
you.

The first step to write a Streams application is to create a
``java.util.Properties`` map to specify different Streams execution
configuration values as defined in ``StreamsConfig``. A couple of
important configuration values you need to set are:
``StreamsConfig.BOOTSTRAP_SERVERS_CONFIG``, which specifies a list of
host/port pairs to use for establishing the initial connection to the
Kafka cluster, and ``StreamsConfig.APPLICATION_ID_CONFIG``, which gives
the unique identifier of your Streams application to distinguish itself
with other applications talking to the same Kafka cluster:

.. code:: bash

            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");    // assuming that the Kafka broker this application is talking to runs on local machine with port 9092
        

In addition, you can customize other configurations in the same map, for
example, default serialization and deserialization libraries for the
record key-value pairs:

.. code:: bash

            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        

For a full list of configurations of Kafka Streams please refer to this
`table </%7B%7Bversion%7D%7D/documentation/#streamsconfigs>`__.

Next we will define the computational logic of our Streams application.
In Kafka Streams this computational logic is defined as a ``topology``
of connected processor nodes. We can use a topology builder to construct
such a topology,

.. code:: bash

            final StreamsBuilder builder = new StreamsBuilder();
        

And then create a source stream from a Kafka topic named
``streams-plaintext-input`` using this topology builder:

.. code:: bash

            KStream<String, String> source = builder.stream("streams-plaintext-input");
        

Now we get a ``KStream`` that is continuously generating records from
its source Kafka topic ``streams-plaintext-input``. The records are
organized as ``String`` typed key-value pairs. The simplest thing we can
do with this stream is to write it into another Kafka topic, say it's
named ``streams-pipe-output``:

.. code:: bash

            source.to("streams-pipe-output");
        

Note that we can also concatenate the above two lines into a single line
as:

.. code:: bash

            builder.stream("streams-plaintext-input").to("streams-pipe-output");
        

We can inspect what kind of ``topology`` is created from this builder by
doing the following:

.. code:: bash

            final Topology topology = builder.build();
        

And print its description to standard output as:

.. code:: bash

            System.out.println(topology.describe());
        

If we just stop here, compile and run the program, it will output the
following information:

.. code:: bash

            > mvn clean package
            > mvn exec:java -Dexec.mainClass=myapps.Pipe
            Sub-topologies:
              Sub-topology: 0
                Source: KSTREAM-SOURCE-0000000000(topics: streams-plaintext-input) --> KSTREAM-SINK-0000000001
                Sink: KSTREAM-SINK-0000000001(topic: streams-pipe-output) <-- KSTREAM-SOURCE-0000000000
            Global Stores:
              none
        

As shown above, it illustrates that the constructed topology has two
processor nodes, a source node ``KSTREAM-SOURCE-0000000000`` and a sink
node ``KSTREAM-SINK-0000000001``. ``KSTREAM-SOURCE-0000000000``
continuously read records from Kafka topic ``streams-plaintext-input``
and pipe them to its downstream node ``KSTREAM-SINK-0000000001``;
``KSTREAM-SINK-0000000001`` will write each of its received record in
order to another Kafka topic ``streams-pipe-output`` (the ``-->`` and
``<--`` arrows dictates the downstream and upstream processor nodes of
this node, i.e. "children" and "parents" within the topology graph). It
also illustrates that this simple topology has no global state stores
associated with it (we will talk about state stores more in the
following sections).

Note that we can always describe the topology as we did above at any
given point while we are building it in the code, so as a user you can
interactively "try and taste" your computational logic defined in the
topology until you are happy with it. Suppose we are already done with
this simple topology that just pipes data from one Kafka topic to
another in an endless streaming manner, we can now construct the Streams
client with the two components we have just constructed above: the
configuration map and the topology object (one can also construct a
``StreamsConfig`` object from the ``props`` map and then pass that
object to the constructor, ``KafkaStreams`` have overloaded constructor
functions to takes either type).

.. code:: bash

            final KafkaStreams streams = new KafkaStreams(topology, props);
        

By calling its ``start()`` function we can trigger the execution of this
client. The execution won't stop until ``close()`` is called on this
client. We can, for example, add a shutdown hook with a countdown latch
to capture a user interrupt and close the client upon terminating this
program:

.. code:: bash

            final CountDownLatch latch = new CountDownLatch(1);

            // attach shutdown handler to catch control-c
            Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
                @Override
                public void run() {
                    streams.close();
                    latch.countDown();
                }
            });

            try {
                streams.start();
                latch.await();
            } catch (Throwable e) {
                System.exit(1);
            }
            System.exit(0);
        

The complete code so far looks like this:

.. code:: bash

            package myapps;

            import org.apache.kafka.common.serialization.Serdes;
            import org.apache.kafka.streams.KafkaStreams;
            import org.apache.kafka.streams.StreamsBuilder;
            import org.apache.kafka.streams.StreamsConfig;
            import org.apache.kafka.streams.Topology;

            import java.util.Properties;
            import java.util.concurrent.CountDownLatch;

            public class Pipe {

                public static void main(String[] args) throws Exception {
                    Properties props = new Properties();
                    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
                    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
                    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
                    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

                    final StreamsBuilder builder = new StreamsBuilder();

                    builder.stream("streams-plaintext-input").to("streams-pipe-output");

                    final Topology topology = builder.build();

                    final KafkaStreams streams = new KafkaStreams(topology, props);
                    final CountDownLatch latch = new CountDownLatch(1);

                    // attach shutdown handler to catch control-c
                    Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
                        @Override
                        public void run() {
                            streams.close();
                            latch.countDown();
                        }
                    });

                    try {
                        streams.start();
                        latch.await();
                    } catch (Throwable e) {
                        System.exit(1);
                    }
                    System.exit(0);
                }
            }
        

If you already have the Kafka broker up and running at
``localhost:9092``, and the topics ``streams-plaintext-input`` and
``streams-pipe-output`` created on that broker, you can run this code in
your IDE or on the command line, using Maven:

.. code:: bash

            > mvn clean package
            > mvn exec:java -Dexec.mainClass=myapps.Pipe
        

For detailed instructions on how to run a Streams application and
observe its computing results, please read the `Play with a Streams
Application </%7B%7Bversion%7D%7D/documentation/streams/quickstart>`__
section. We will not talk about this in the rest of this section.

`Writing a second Streams application: Line Split <#tutorial_code_linesplit>`__
-------------------------------------------------------------------------------

We have learned how to construct a Streams client with its two key
components: the ``StreamsConfig`` and ``Topology``. Now let's move on to
add some real processing logic by augmenting the current topology. We
can first create another program by first copy the existing
``Pipe.java`` class:

.. code:: bash

            > cp src/main/java/myapps/Pipe.java src/main/java/myapps/LineSplit.java
        

And change its class name as well as the application id config to
distinguish with the original program:

.. code:: bash

            public class LineSplit {

                public static void main(String[] args) throws Exception {
                    Properties props = new Properties();
                    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-linesplit");
                    // ...
                }
            }
        

Since each of the source stream's record is a ``String`` typed key-value
pair, let's treat the value string as a text line and split it into
words with a ``FlatMapValues`` operator:

.. code:: bash

            KStream<String, String> source = builder.stream("streams-plaintext-input");
            KStream<String, String> words = source.flatMapValues(new ValueMapper<String, Iterable<String>>() {
                        @Override
                        public Iterable<String> apply(String value) {
                            return Arrays.asList(value.split("\\W+"));
                        }
                    });
        

The operator will take the ``source`` stream as its input, and generate
a new stream named ``words`` by processing each record from its source
stream in order and breaking its value string into a list of words, and
producing each word as a new record to the output ``words`` stream. This
is a stateless operator that does not need to keep track of any
previously received records or processed results. Note if you are using
JDK 8 you can use lambda expression and simplify the above code as:

.. code:: bash

            KStream<String, String> source = builder.stream("streams-plaintext-input");
            KStream<String, String> words = source.flatMapValues(value -> Arrays.asList(value.split("\\W+")));
        

And finally we can write the word stream back into another Kafka topic,
say ``streams-linesplit-output``. Again, these two steps can be
concatenated as the following (assuming lambda expression is used):

.. code:: bash

            KStream<String, String> source = builder.stream("streams-plaintext-input");
            source.flatMapValues(value -> Arrays.asList(value.split("\\W+")))
                  .to("streams-linesplit-output");
        

If we now describe this augmented topology as
``System.out.println(topology.describe())``, we will get the following:

.. code:: bash

            > mvn clean package
            > mvn exec:java -Dexec.mainClass=myapps.LineSplit
            Sub-topologies:
              Sub-topology: 0
                Source: KSTREAM-SOURCE-0000000000(topics: streams-plaintext-input) --> KSTREAM-FLATMAPVALUES-0000000001
                Processor: KSTREAM-FLATMAPVALUES-0000000001(stores: []) --> KSTREAM-SINK-0000000002 <-- KSTREAM-SOURCE-0000000000
                Sink: KSTREAM-SINK-0000000002(topic: streams-linesplit-output) <-- KSTREAM-FLATMAPVALUES-0000000001
              Global Stores:
                none
        

As we can see above, a new processor node
``KSTREAM-FLATMAPVALUES-0000000001`` is injected into the topology
between the original source and sink nodes. It takes the source node as
its parent and the sink node as its child. In other words, each record
fetched by the source node will first traverse to the newly added
``KSTREAM-FLATMAPVALUES-0000000001`` node to be processed, and one or
more new records will be generated as a result. They will continue
traverse down to the sink node to be written back to Kafka. Note this
processor node is "stateless" as it is not associated with any stores
(i.e. ``(stores: [])``).

The complete code looks like this (assuming lambda expression is used):

.. code:: bash

            package myapps;

            import org.apache.kafka.common.serialization.Serdes;
            import org.apache.kafka.streams.KafkaStreams;
            import org.apache.kafka.streams.StreamsBuilder;
            import org.apache.kafka.streams.StreamsConfig;
            import org.apache.kafka.streams.Topology;
            import org.apache.kafka.streams.kstream.KStream;

            import java.util.Arrays;
            import java.util.Properties;
            import java.util.concurrent.CountDownLatch;

            public class LineSplit {

                public static void main(String[] args) throws Exception {
                    Properties props = new Properties();
                    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-linesplit");
                    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
                    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
                    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

                    final StreamsBuilder builder = new StreamsBuilder();

                    KStream<String, String> source = builder.stream("streams-plaintext-input");
                    source.flatMapValues(value -> Arrays.asList(value.split("\\W+")))
                          .to("streams-linesplit-output");

                    final Topology topology = builder.build();
                    final KafkaStreams streams = new KafkaStreams(topology, props);
                    final CountDownLatch latch = new CountDownLatch(1);

                    // ... same as Pipe.java above
                }
            }
        

`Writing a third Streams application: Wordcount <#tutorial_code_wordcount>`__
-----------------------------------------------------------------------------

Let's now take a step further to add some "stateful" computations to the
topology by counting the occurrence of the words split from the source
text stream. Following similar steps let's create another program based
on the ``LineSplit.java`` class:

.. code:: bash

            public class WordCount {

                public static void main(String[] args) throws Exception {
                    Properties props = new Properties();
                    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
                    // ...
                }
            }
        

In order to count the words we can first modify the ``flatMapValues``
operator to treat all of them as lower case (assuming lambda expression
is used):

.. code:: bash

            source.flatMapValues(new ValueMapper<String, Iterable<String>>() {
                        @Override
                        public Iterable<String> apply(String value) {
                            return Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+"));
                        }
                    });
        

In order to do the counting aggregation we have to first specify that we
want to key the stream on the value string, i.e. the lower cased word,
with a ``groupBy`` operator. This operator generate a new grouped
stream, which can then be aggregated by a ``count`` operator, which
generates a running count on each of the grouped keys:

.. code:: bash

            KTable<String, Long> counts =
            source.flatMapValues(new ValueMapper<String, Iterable<String>>() {
                        @Override
                        public Iterable<String> apply(String value) {
                            return Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+"));
                        }
                    })
                  .groupBy(new KeyValueMapper<String, String, String>() {
                       @Override
                       public String apply(String key, String value) {
                           return value;
                       }
                    })
                  // Materialize the result into a KeyValueStore named "counts-store".
                  // The Materialized store is always of type <Bytes, byte[]> as this is the format of the inner most store.
                  .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>> as("counts-store"));
        

Note that the ``count`` operator has a ``Materialized`` parameter that
specifies that the running count should be stored in a state store named
``counts-store``. This ``Counts`` store can be queried in real-time,
with details described in the `Developer
Manual </%7B%7Bversion%7D%7D/documentation/streams/developer-guide#streams_interactive_queries>`__.

We can also write the ``counts`` KTable's changelog stream back into
another Kafka topic, say ``streams-wordcount-output``. Because the
result is a changelog stream, the output topic
``streams-wordcount-output`` should be configured with log compaction
enabled. Note that this time the value type is no longer ``String`` but
``Long``, so the default serialization classes are not viable for
writing it to Kafka anymore. We need to provide overridden serialization
methods for ``Long`` types, otherwise a runtime exception will be
thrown:

.. code:: bash

            counts.toStream().to("streams-wordcount-output", Produced.with(Serdes.String(), Serdes.Long());
        

Note that in order to read the changelog stream from topic
``streams-wordcount-output``, one needs to set the value deserialization
as ``org.apache.kafka.common.serialization.LongDeserializer``. Details
of this can be found in the `Play with a Streams
Application </%7B%7Bversion%7D%7D/documentation/streams/quickstart>`__
section. Assuming lambda expression from JDK 8 can be used, the above
code can be simplified as:

.. code:: bash

            KStream<String, String> source = builder.stream("streams-plaintext-input");
            source.flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
                  .groupBy((key, value) -> value)
                  .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"))
                  .toStream()
                  .to("streams-wordcount-output", Produced.with(Serdes.String(), Serdes.Long());
        

If we again describe this augmented topology as
``System.out.println(topology.describe())``, we will get the following:

.. code:: bash

            > mvn clean package
            > mvn exec:java -Dexec.mainClass=myapps.WordCount
            Sub-topologies:
              Sub-topology: 0
                Source: KSTREAM-SOURCE-0000000000(topics: streams-plaintext-input) --> KSTREAM-FLATMAPVALUES-0000000001
                Processor: KSTREAM-FLATMAPVALUES-0000000001(stores: []) --> KSTREAM-KEY-SELECT-0000000002 <-- KSTREAM-SOURCE-0000000000
                Processor: KSTREAM-KEY-SELECT-0000000002(stores: []) --> KSTREAM-FILTER-0000000005 <-- KSTREAM-FLATMAPVALUES-0000000001
                Processor: KSTREAM-FILTER-0000000005(stores: []) --> KSTREAM-SINK-0000000004 <-- KSTREAM-KEY-SELECT-0000000002
                Sink: KSTREAM-SINK-0000000004(topic: Counts-repartition) <-- KSTREAM-FILTER-0000000005
              Sub-topology: 1
                Source: KSTREAM-SOURCE-0000000006(topics: Counts-repartition) --> KSTREAM-AGGREGATE-0000000003
                Processor: KSTREAM-AGGREGATE-0000000003(stores: [Counts]) --> KTABLE-TOSTREAM-0000000007 <-- KSTREAM-SOURCE-0000000006
                Processor: KTABLE-TOSTREAM-0000000007(stores: []) --> KSTREAM-SINK-0000000008 <-- KSTREAM-AGGREGATE-0000000003
                Sink: KSTREAM-SINK-0000000008(topic: streams-wordcount-output) <-- KTABLE-TOSTREAM-0000000007
            Global Stores:
              none
        

As we can see above, the topology now contains two disconnected
sub-topologies. The first sub-topology's sink node
``KSTREAM-SINK-0000000004`` will write to a repartition topic
``Counts-repartition``, which will be read by the second sub-topology's
source node ``KSTREAM-SOURCE-0000000006``. The repartition topic is used
to "shuffle" the source stream by its aggregation key, which is in this
case the value string. In addition, inside the first sub-topology a
stateless ``KSTREAM-FILTER-0000000005`` node is injected between the
grouping ``KSTREAM-KEY-SELECT-0000000002`` node and the sink node to
filter out any intermediate record whose aggregate key is empty.

In the second sub-topology, the aggregation node
``KSTREAM-AGGREGATE-0000000003`` is associated with a state store named
``Counts`` (the name is specified by the user in the ``count``
operator). Upon receiving each record from its upcoming stream source
node, the aggregation processor will first query its associated
``Counts`` store to get the current count for that key, augment by one,
and then write the new count back to the store. Each updated count for
the key will also be piped downstream to the
``KTABLE-TOSTREAM-0000000007`` node, which interpret this update stream
as a record stream before further piping to the sink node
``KSTREAM-SINK-0000000008`` for writing back to Kafka.

The complete code looks like this (assuming lambda expression is used):

.. code:: bash

            package myapps;

            import org.apache.kafka.common.serialization.Serdes;
            import org.apache.kafka.streams.KafkaStreams;
            import org.apache.kafka.streams.StreamsBuilder;
            import org.apache.kafka.streams.StreamsConfig;
            import org.apache.kafka.streams.Topology;
            import org.apache.kafka.streams.kstream.KStream;

            import java.util.Arrays;
            import java.util.Locale;
            import java.util.Properties;
            import java.util.concurrent.CountDownLatch;

            public class WordCount {

                public static void main(String[] args) throws Exception {
                    Properties props = new Properties();
                    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
                    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
                    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
                    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

                    final StreamsBuilder builder = new StreamsBuilder();

                    KStream<String, String> source = builder.stream("streams-plaintext-input");
                    source.flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
                          .groupBy((key, value) -> value)
                          .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"))
                          .toStream()
                          .to("streams-wordcount-output", Produced.with(Serdes.String(), Serdes.Long());

                    final Topology topology = builder.build();
                    final KafkaStreams streams = new KafkaStreams(topology, props);
                    final CountDownLatch latch = new CountDownLatch(1);

                    // ... same as Pipe.java above
                }
            }
        


