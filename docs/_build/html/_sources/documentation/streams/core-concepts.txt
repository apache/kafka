.. _streams_concepts:

Core Concepts
=============

.. concepts::
    :local:

Kafka Streams is a client library for processing and analyzing data
stored in Kafka. It builds upon important stream processing concepts
such as properly distinguishing between event time and processing time,
windowing support, and simple yet efficient management and real-time
querying of application state.

Kafka Streams has a **low barrier to entry**: You can quickly write and
run a small-scale proof-of-concept on a single machine; and you only
need to run additional instances of your application on multiple
machines to scale up to high-volume production workloads. Kafka Streams
transparently handles the load balancing of multiple instances of the
same application by leveraging Kafka's parallelism model.

Some highlights of Kafka Streams:

-  Designed as a **simple and lightweight client library**, which can be
   easily embedded in any Java application and integrated with any
   existing packaging, deployment and operational tools that users have
   for their streaming applications.
-  Has **no external dependencies on systems other than Apache Kafka
   itself** as the internal messaging layer; notably, it uses Kafka's
   partitioning model to horizontally scale processing while maintaining
   strong ordering guarantees.
-  Supports **fault-tolerant local state**, which enables very fast and
   efficient stateful operations like windowed joins and aggregations.
-  Supports **exactly-once** processing semantics to guarantee that each
   record will be processed once and only once even when there is a
   failure on either Streams clients or Kafka brokers in the middle of
   processing.
-  Employs **one-record-at-a-time processing** to achieve millisecond
   processing latency, and supports **event-time based windowing
   operations** with late arrival of records.
-  Offers necessary stream processing primitives, along with a
   **high-level Streams DSL** and a **low-level Processor API**.

We first summarize the key concepts of Kafka Streams.

`Stream Processing Topology <#streams_topology>`__
--------------------------------------------------

-  A **stream** is the most important abstraction provided by Kafka
   Streams: it represents an unbounded, continuously updating data set.
   A stream is an ordered, replayable, and fault-tolerant sequence of
   immutable data records, where a **data record** is defined as a
   key-value pair.
-  A **stream processing application** is any program that makes use of
   the Kafka Streams library. It defines its computational logic through
   one or more **processor topologies**, where a processor topology is a
   graph of stream processors (nodes) that are connected by streams
   (edges).
-  A **stream processor** is a node in the processor topology; it
   represents a processing step to transform data in streams by
   receiving one input record at a time from its upstream processors in
   the topology, applying its operation to it, and may subsequently
   produce one or more output records to its downstream processors.

There are two special processors in the topology:

-  **Source Processor**: A source processor is a special type of stream
   processor that does not have any upstream processors. It produces an
   input stream to its topology from one or multiple Kafka topics by
   consuming records from these topics and forwarding them to its
   down-stream processors.
-  **Sink Processor**: A sink processor is a special type of stream
   processor that does not have down-stream processors. It sends any
   received records from its up-stream processors to a specified Kafka
   topic.

Note that in normal processor nodes other remote systems can also be
accessed while processing the current record. Therefore the processed
results can either be streamed back into Kafka or written to an external
system.

.. image:: ../../images/streams-architecture-topology.jpg
    :align: center
    :width: 400px

Kafka Streams offers two ways to define the stream processing topology:
the `Kafka Streams
DSL </%7B%7Bversion%7D%7D/documentation/streams/developer-guide#streams_dsl>`__
provides the most common data transformation operations such as ``map``,
``filter``, ``join`` and ``aggregations`` out of the box; the
lower-level `Processor
API </%7B%7Bversion%7D%7D/documentation/streams/developer-guide#streams_processor>`__
allows developers define and connect custom processors as well as to
interact with `state stores <#streams_state>`__.

A processor topology is merely a logical abstraction for your stream
processing code. At runtime, the logical topology is instantiated and
replicated inside the application for parallel processing (see `Stream
Partitions and Tasks <#streams_architecture_tasks>`__ for details).

`Time <#streams_time>`__
------------------------

A critical aspect in stream processing is the notion of **time**, and
how it is modeled and integrated. For example, some operations such as
**windowing** are defined based on time boundaries.

Common notions of time in streams are:

-  **Event time** - The point in time when an event or data record
   occurred, i.e. was originally created "at the source". **Example:**
   If the event is a geo-location change reported by a GPS sensor in a
   car, then the associated event-time would be the time when the GPS
   sensor captured the location change.
-  **Processing time** - The point in time when the event or data record
   happens to be processed by the stream processing application, i.e.
   when the record is being consumed. The processing time may be
   milliseconds, hours, or days etc. later than the original event time.
   **Example:** Imagine an analytics application that reads and
   processes the geo-location data reported from car sensors to present
   it to a fleet management dashboard. Here, processing-time in the
   analytics application might be milliseconds or seconds (e.g. for
   real-time pipelines based on Apache Kafka and Kafka Streams) or hours
   (e.g. for batch pipelines based on Apache Hadoop or Apache Spark)
   after event-time.
-  **Ingestion time** - The point in time when an event or data record
   is stored in a topic partition by a Kafka broker. The difference to
   event time is that this ingestion timestamp is generated when the
   record is appended to the target topic by the Kafka broker, not when
   the record is created "at the source". The difference to processing
   time is that processing time is when the stream processing
   application processes the record. **For example,** if a record is
   never processed, there is no notion of processing time for it, but it
   still has an ingestion time.

The choice between event-time and ingestion-time is actually done
through the configuration of Kafka (not Kafka Streams): From Kafka
0.10.x onwards, timestamps are automatically embedded into Kafka
messages. Depending on Kafka's configuration these timestamps represent
event-time or ingestion-time. The respective Kafka configuration setting
can be specified on the broker level or per topic. The default timestamp
extractor in Kafka Streams will retrieve these embedded timestamps
as-is. Hence, the effective time semantics of your application depend on
the effective Kafka configuration for these embedded timestamps.

Kafka Streams assigns a **timestamp** to every data record via the
``TimestampExtractor`` interface. These per-record timestamps describe
the progress of a stream with regards to time and are leveraged by
time-dependent operations such as window operations. As a result, this
time will only advance when a new record arrives at the processor. We
call this data-driven time the **stream time** of the application to
differentiate with the **wall-clock time** when this application is
actually executing. Concrete implementations of the
``TimestampExtractor`` interface will then provide different semantics
to the stream time definition. For example retrieving or computing
timestamps based on the actual contents of data records such as an
embedded timestamp field to provide event time semantics, and returning
the current wall-clock time thereby yield processing time semantics to
stream time. Developers can thus enforce different notions of time
depending on their business needs.

Finally, whenever a Kafka Streams application writes records to Kafka,
then it will also assign timestamps to these new records. The way the
timestamps are assigned depends on the context:

-  When new output records are generated via processing some input
   record, for example, ``context.forward()`` triggered in the
   ``process()`` function call, output record timestamps are inherited
   from input record timestamps directly.
-  When new output records are generated via periodic functions such as
   ``Punctuator#punctuate()``, the output record timestamp is defined as
   the current internal time (obtained through ``context.timestamp()``)
   of the stream task.
-  For aggregations, the timestamp of a resulting aggregate update
   record will be that of the latest arrived input record that triggered
   the update.

`States <#streams_state>`__
---------------------------

Some stream processing applications don't require state, which means the
processing of a message is independent from the processing of all other
messages. However, being able to maintain state opens up many
possibilities for sophisticated stream processing applications: you can
join input streams, or group and aggregate data records. Many such
stateful operators are provided by the `Kafka Streams
DSL </%7B%7Bversion%7D%7D/documentation/streams/developer-guide#streams_dsl>`__.

Kafka Streams provides so-called **state stores**, which can be used by
stream processing applications to store and query data. This is an
important capability when implementing stateful operations. Every task
in Kafka Streams embeds one or more state stores that can be accessed
via APIs to store and query data required for processing. These state
stores can either be a persistent key-value store, an in-memory hashmap,
or another convenient data structure. Kafka Streams offers
fault-tolerance and automatic recovery for local state stores.

Kafka Streams allows direct read-only queries of the state stores by
methods, threads, processes or applications external to the stream
processing application that created the state stores. This is provided
through a feature called **Interactive Queries**. All stores are named
and Interactive Queries exposes only the read operations of the
underlying implementation.

| 

`Processing Guarantees <#streams_processing_guarantee>`__
---------------------------------------------------------

In stream processing, one of the most frequently asked question is "does
my stream processing system guarantee that each record is processed once
and only once, even if some failures are encountered in the middle of
processing?" Failing to guarantee exactly-once stream processing is a
deal-breaker for many applications that cannot tolerate any data-loss or
data duplicates, and in that case a batch-oriented framework is usually
used in addition to the stream processing pipeline, known as the `Lambda
Architecture <http://lambda-architecture.net/>`__. Prior to 0.11.0.0,
Kafka only provides at-least-once delivery guarantees and hence any
stream processing systems that leverage it as the backend storage could
not guarantee end-to-end exactly-once semantics. In fact, even for those
stream processing systems that claim to support exactly-once processing,
as long as they are reading from / writing to Kafka as the source /
sink, their applications cannot actually guarantee that no duplicates
will be generated throughout the pipeline. Since the 0.11.0.0 release,
Kafka has added support to allow its producers to send messages to
different topic partitions in a `transactional and idempotent
manner <https://kafka.apache.org/documentation/#semantics>`__, and Kafka
Streams has hence added the end-to-end exactly-once processing semantics
by leveraging these features. More specifically, it guarantees that for
any record read from the source Kafka topics, its processing results
will be reflected exactly once in the output Kafka topic as well as in
the state stores for stateful operations. Note the key difference
between Kafka Streams end-to-end exactly-once guarantee with other
stream processing frameworks' claimed guarantees is that Kafka Streams
tightly integrates with the underlying Kafka storage system and ensure
that commits on the input topic offsets, updates on the state stores,
and writes to the output topics will be completed atomically instead of
treating Kafka as an external system that may have side-effects. To read
more details on how this is done inside Kafka Streams, readers are
recommended to read
`KIP-129 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-129%3A+Streams+Exactly-Once+Semantics>`__.
In order to achieve exactly-once semantics when running Kafka Streams
applications, users can simply set the ``processing.guarantee`` config
value to **exactly_once** (default value is **at_least_once**). More
details can be found in the `Kafka Streams
Configs </%7B%7Bversion%7D%7D/documentation#streamsconfigs>`__
section.



