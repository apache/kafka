.. _streams_developer-guide_processor-api:

`Processor API <#processor-api>`__
==================================

The Processor API allows developers to define and connect custom
processors and to interact with state stores. With the Processor API,
you can define arbitrary stream processors that process one received
record at a time, and connect these processors with their associated
state stores to compose the processor topology that represents a
customized processing logic.

.. contents:: Table of Contents
   :local:

`Overview <#overview>`__
------------------------

The Processor API can be used to implement both **stateless** as well as
**stateful** operations, where the latter is achieved through the use of
`state stores <#streams-developer-guide-state-store>`__.

.. tip::

   **Combining the DSL and the Processor API:** You can combine the
   convenience of the DSL with the power and flexibility of the Processor
   API as described in the section `Applying processors and transformers
   (Processor API
   integration) <dsl-api.html#streams-developer-guide-dsl-process>`__.

For a complete list of available API functionality, see the `Kafka
Streams API docs <../javadocs.html#streams-javadocs>`__.

`Defining a Stream Processor <#defining-a-stream-processor>`__
--------------------------------------------------------------

A `stream processor <../concepts.html#streams-concepts>`__ is a node in
the processor topology that represents a single processing step. With
the Processor API, you can define arbitrary stream processors that
processes one received record at a time, and connect these processors
with their associated state stores to compose the processor topology.

You can define a customized stream processor by implementing the
``Processor`` interface, which provides the ``process()`` API method.
The ``process()`` method is called on each of the received records.

The ``Processor`` interface also has an ``init()`` method, which is
called by the Kafka Streams library during task construction phase.
Processor instances should perform any required initialization in this
method. The ``init()`` method passes in a ``ProcessorContext`` instance,
which provides access to the metadata of the currently processed record,
including its source Kafka topic and partition, its corresponding
message offset, and further such information. You can also use this
context instance to schedule a punctuation function (via
``ProcessorContext#schedule()``), to forward a new record as a key-value
pair to the downstream processors (via ``ProcessorContext#forward()``),
and to commit the current processing progress (via
``ProcessorContext#commit()``).

Specifically, ``ProcessorContext#schedule()`` accepts a user
``Punctuator`` callback interface, which triggers its ``punctuate()``
API method periodically based on the ``PunctuationType``. The
``PunctuationType`` determines what notion of time is used for the
punctuation scheduling: either
`stream-time <../concepts.html#streams-concepts-time>`__ or
wall-clock-time (by default, stream-time is configured to represent
event-time via ``TimestampExtractor``). When stream-time is used,
``punctuate()`` is triggered purely by data because stream-time is
determined (and advanced forward) by the timestamps derived from the
input data. When there is no new input data arriving, stream-time is not
advanced and thus ``punctuate()`` is not called.

For example, if you schedule a ``Punctuator`` function every 10 seconds
based on ``PunctuationType.STREAM_TIME`` and if you process a stream of
60 records with consecutive timestamps from 1 (first record) to 60
seconds (last record), then ``punctuate()`` would be called 6 times.
This happens regardless of the time required to actually process those
records. ``punctuate()`` would be called 6 times regardless of whether
processing these 60 records takes a second, a minute, or an hour.

When wall-clock-time (i.e. ``PunctuationType.WALL_CLOCK_TIME``) is used,
``punctuate()`` is triggered purely by the wall-clock time. Reusing the
example above, if the ``Punctuator`` function is scheduled based on
``PunctuationType.WALL_CLOCK_TIME``, and if these 60 records were
processed within 20 seconds, ``punctuate()`` is called 2 times (one time
every 10 seconds). If these 60 records were processed within 5 seconds,
then no ``punctuate()`` is called at all. Note that you can schedule
multiple ``Punctuator`` callbacks with different ``PunctuationType``
types within the same processor by calling
``ProcessorContext#schedule()`` multiple times inside ``init()`` method.

.. attention::

   Stream-time is only advanced if all input partitions over all input
   topics have new data (with newer timestamps) available. If at least one
   partition does not have any new data available, stream-time will not be
   advanced and thus ``punctuate()`` will not be triggered if
   ``PunctuationType.STREAM_TIME`` was specified. This behavior is
   independent of the configured timestamp extractor, i.e., using
   ``WallclockTimestampExtractor`` does not enable wall-clock triggering of
   ``punctuate()``.

The following example ``Processor`` defines a simple word-count
algorithm and the following actions are performed:

-  In the ``init()`` method, schedule the punctuation every 1000 time
   units (the time unit is normally milliseconds, which in this example
   would translate to punctuation every 1 second) and retrieve the local
   state store by its name “Counts”.
-  In the ``process()`` method, upon each received record, split the
   value string into words, and update their counts into the state store
   (we will talk about this later in this section).
-  In the ``punctuate()`` method, iterate the local state store and send
   the aggregated counts to the downstream processor (we will talk about
   downstream processors later in this section), and commit the current
   stream state.

.. code:: java

    public class WordCountProcessor implements Processor<String, String> {

      private ProcessorContext context;
      private KeyValueStore<String, Long> kvStore;

      @Override
      @SuppressWarnings("unchecked")
      public void init(ProcessorContext context) {
          // keep the processor context locally because we need it in punctuate() and commit()
          this.context = context;

          // retrieve the key-value store named "Counts"
          kvStore = (KeyValueStore) context.getStateStore("Counts");

          // schedule a punctuate() method every 1000 milliseconds based on stream-time
          this.context.schedule(1000, PunctuationType.STREAM_TIME, (timestamp) -> {
              KeyValueIterator<String, Long> iter = this.kvStore.all();
              while (iter.hasNext()) {
                  KeyValue<String, Long> entry = iter.next();
                  context.forward(entry.key, entry.value.toString());
              }
              iter.close();

              // commit the current processing progress
              context.commit();
          });
      }

      @Override
      public void punctuate(long timestamp) {
          // this method is deprecated and should not be used anymore
      }

      @Override
      public void close() {
          // close the key-value store
          kvStore.close();
      }

    }

.. note::

   **Stateful processing with state stores:** The ``WordCountProcessor``
   defined above can access the currently received record in its
   ``process()`` method, and it can leverage `state
   stores <#streams-developer-guide-state-store>`__ to maintain processing
   states to, for example, remember recently arrived records for stateful
   processing needs like aggregations and joins. For more information, see
   the `state stores <#streams-developer-guide-state-store>`__
   documentation.

`State Stores <#state-stores>`__
--------------------------------

To implement a **stateful** ``Processor`` or ``Transformer``, you must
provide one or more state stores to the processor or transformer
(*stateless* processors or transformers do not need state stores). State
stores can be used to remember recently received input records, to track
rolling aggregates, to de-duplicate input records, and more. Another
feature of state stores is that they can be `interactively
queried <interactive-queries.html#streams-developer-guide-interactive-queries>`__
from other applications, such as a NodeJS-based dashboard or a
microservice implemented in Scala or Go.

The `available state store
types <#streams-developer-guide-state-store-defining>`__ in Kafka
Streams have `fault
tolerance <#streams-developer-guide-state-store-fault-tolerance>`__
enabled by default.

`Defining and creating a State Store <#defining-and-creating-a-state-store>`__
------------------------------------------------------------------------------

You can either use one of the available store types or `implement your
own custom store type <#streams-developer-guide-state-store-custom>`__.
It’s common practice to leverage an existing store type via the
``Stores`` factory.

Note that, when using Kafka Streams, you normally don’t create or
instantiate state stores directly in your code. Rather, you define state
stores indirectly by creating a so-called ``StoreBuilder``. This
buildeer is used by Kafka Streams as a factory to instantiate the actual
state stores locally in application instances when and where needed.

The following store types are available out of the box.

+----------------------------+----------------+--------------------------+--------------------------------------------------------------------------+
| Store Type                 | Storage Engine | Fault-tolerant?          | Description                                                              |
+============================+================+==========================+==========================================================================+
| Persistent                 | RocksDB        | Yes (enabled by default) |                                                                          |
| ``KeyValueStore<K, V>``    |                |                          | - **The recommended store type for most use cases.**                     |
|                            |                |                          | - Stores its data on local disk.                                         |
|                            |                |                          | - Storage capacity:                                                      |
|                            |                |                          |   managed local state can be larger than the memory (heap space) of an   |
|                            |                |                          |   application instance, but must fit into the available local disk       |
|                            |                |                          |   space.                                                                 |
|                            |                |                          | - RocksDB settings can be fine-tuned, see                                |
|                            |                |                          |   :ref:`RocksDB configuration <streams_developer-guide_rocksdb-config>`. |
|                            |                |                          | - Available :streams-apidocs-store-persistent:`store variants|`:         |
|                            |                |                          |   time window key-value store, session window key-value store.           |
|                            |                |                          |                                                                          |
|                            |                |                          | .. literalinclude:: api-papi-store-persistent.java                       |
|                            |                |                          |    :language: java                                                       |
|                            |                |                          |                                                                          |
|                            |                |                          | See                                                                      |
|                            |                |                          | :streams-apidocs-store-persistent:`PersistentKeyValueFactory|` for       |
|                            |                |                          | detailed factory options.                                                |
+----------------------------+----------------+--------------------------+--------------------------------------------------------------------------+
| In-memory                  | \-             | Yes (enabled by default) |                                                                          |
| ``KeyValueStore<K, V>``    |                |                          | - Stores its data in memory.                                             |
|                            |                |                          | - Storage capacity:                                                      |
|                            |                |                          |   managed local state must fit into memory (heap space) of an            |
|                            |                |                          |   application instance.                                                  |
|                            |                |                          | - Useful when application instances run in an environment where local    |
|                            |                |                          |   disk space is either not available or local disk space is wiped        |
|                            |                |                          |   in-between app instance restarts.                                      |
|                            |                |                          |                                                                          |
|                            |                |                          | .. literalinclude:: api-papi-store-inmemory.java                         |
|                            |                |                          |    :language: java                                                       |
|                            |                |                          |                                                                          |
|                            |                |                          | See                                                                      |
|                            |                |                          | :streams-apidocs-store-inmem:`InMemoryKeyValueFactory|` for              |
|                            |                |                          | detailed factory options.                                                |
+----------------------------+----------------+--------------------------+--------------------------------------------------------------------------+

`Fault-tolerant State Stores <#fault-tolerant-state-stores>`__
--------------------------------------------------------------

To make state stores fault-tolerant and to allow for state store
migration without data loss, a state store can be continuously backed up
to a Kafka topic behind the scenes. For example, to migrate a stateful
stream task from one machine to another when `elastically adding or
removing capacity from your
application <running-app.html#streams-developer-guide-execution-scaling>`__.
This topic is sometimes referred to as the state store’s associated
*changelog topic*, or its *changelog*. For example, if you experience
machine failure, the state store and the application’s state can be
fully restored from its changelog. You can `enable or disable this
backup
feature <#streams-developer-guide-state-store-enable-disable-fault-tolerance>`__
for a state store.

By default, persistent key-value stores are fault-tolerant. They are
backed by a
`compacted <https://kafka.apache.org/documentation.html#compaction>`__
changelog topic. The purpose of compacting this topic is to prevent the
topic from growing indefinitely, to reduce the storage consumed in the
associated Kafka cluster, and to minimize recovery time if a state store
needs to be restored from its changelog topic.

Similarly, persistent window stores are fault-tolerant. They are backed
by a topic that uses both compaction and deletion. Because of the
structure of the message keys that are being sent to the changelog
topics, this combination of deletion and compaction is required for the
changelog topics of window stores. For window stores, the message keys
are composite keys that include the “normal” key and window timestamps.
For these types of composite keys it would not be sufficient to only
enable compaction to prevent a changelog topic from growing out of
bounds. With deletion enabled, old windows that have expired will be
cleaned up by Kafka’s log cleaner as the log segments expire. The
default retention setting is ``Windows#maintainMs()`` + 1 day. You can
override this setting by specifying
``StreamsConfig.WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG``
in the ``StreamsConfig``.

When you open an ``Iterator`` from a state store you must call
``close()`` on the iterator when you are done working with it to reclaim
resources; or you can use the iterator from within a try-with-resources
statement. If you do not close an iterator, you may encounter an OOM
error.

`Enable or Disable Fault Tolerance of State Stores (Store Changelogs) <#enable-or-disable-fault-tolerance-of-state-stores-store-changelogs>`__
----------------------------------------------------------------------------------------------------------------------------------------------

You can enable or disable fault tolerance for a state store by enabling
or disabling the change logging of the store through ``enableLogging()``
and ``disableLogging()``. You can also fine-tune the associated topic’s
configuration if needed.

Example for disabling fault-tolerance:

.. code:: java

    import org.apache.kafka.streams.state.StoreBuilder;
    import org.apache.kafka.streams.state.Stores;

    StoreBuilder<KeyValueStore<String, Long>> countStoreSupplier = Stores.keyValueStoreBuilder(
      Stores.persistentKeyValueStore("Counts"),
        Serdes.String(),
        Serdes.Long())
      .withLoggingDisabled(); // disable backing up the store to a changelog topic

.. attention::

   If the changelog is disabled then the attached state store is no longer
   fault tolerant and it can’t have any `standby
   replicas <config-streams.html#streams-developer-guide-standby-replicas>`__.

Here is an example for enabling fault tolerance, with additional
changelog-topic configuration: You can add any log config from
`kafka.log.LogConfig <https://github.com/apache/kafka/blob/1.0/core/src/main/scala/kafka/log/LogConfig.scala#L61>`__.
Unrecognized configs will be ignored.

.. code:: java

    import org.apache.kafka.streams.state.StoreBuilder;
    import org.apache.kafka.streams.state.Stores;

    Map<String, String> changelogConfig = new HashMap();
    // override min.insync.replicas
    changelogConfig.put("min.insyc.replicas", "1")

    StoreBuilder<KeyValueStore<String, Long>> countStoreSupplier = Stores.keyValueStoreBuilder(
      Stores.persistentKeyValueStore("Counts"),
        Serdes.String(),
        Serdes.Long())
      .withLoggingEnabled(changlogConfig); // enable changelogging, with custom changelog settings


`Implementing Custom State Stores <#implementing-custom-state-stores>`__
------------------------------------------------------------------------

You can use the `built-in state store
types <#streams-developer-guide-state-store-defining>`__ or implement
your own. The primary interface to implement for the store is
``org.apache.kafka.streams.processor.StateStore``. Kafka Streams also
has a few extended interfaces such as ``KeyValueStore``.

You also need to provide a “factory” for the store by implementing the
``org.apache.kafka.streams.processor.StateStoreSupplier`` interface,
which Kafka Streams uses to create instances of your store.

`Connecting Processors and State Stores <#connecting-processors-and-state-stores>`__
------------------------------------------------------------------------------------

Now that a `processor <#streams-developer-guide-stream-processor>`__
(WordCountProcessor) and the state stores have been defined, you can
construct the processor topology by connecting these processors and
state stores together by using the ``Topology`` instance. In addition,
you can add source processors with the specified Kafka topics to
generate input data streams into the topology, and sink processors with
the specified Kafka topics to generate output data streams out of the
topology.

Here is an example implementation:

.. code:: java

    Topology builder = new Topology();

    // add the source processor node that takes Kafka topic "source-topic" as input
    builder.addSource("Source", "source-topic")

        // add the WordCountProcessor node which takes the source processor as its upstream processor
        .addProcessor("Process", () -> new WordCountProcessor(), "Source")

        // add the count store associated with the WordCountProcessor processor
        .addStateStore(countStoreBuilder, "Process")

        // add the sink processor node that takes Kafka topic "sink-topic" as output
        // and the WordCountProcessor node as its upstream processor
        .addSink("Sink", "sink-topic", "Process");


Here is a quick explanation of this example:

-  A source processor node named ``"Source"`` is added to the topology
   using the ``addSource`` method, with one Kafka topic
   ``"source-topic"`` fed to it.
-  A processor node named ``"Process"`` with the pre-defined
   ``WordCountProcessor`` logic is then added as the downstream
   processor of the ``"Source"`` node using the ``addProcessor`` method.
-  A predefined persistent key-value state store is created and
   associated with the ``"Process"`` node, using ``countStoreBuilder``.
-  A sink processor node is then added to complete the topology using
   the ``addSink`` method, taking the ``"Process"`` node as its upstream
   processor and writing to a separate ``"sink-topic"`` Kafka topic.

In this topology, the ``"Process"`` stream processor node is considered
a downstream processor of the ``"Source"`` node, and an upstream
processor of the ``"Sink"`` node. As a result, whenever the ``"Source"``
node forwards a newly fetched record from Kafka to its downstream
``"Process"`` node, the ``WordCountProcessor#process()`` method is
triggered to process the record and update the associated state store.
Whenever ``context#forward()`` is called in the
``WordCountProcessor#punctuate()`` method, the aggregate key-value pair
will be sent via the ``"Sink"`` processor node to the Kafka topic
``"sink-topic"``. Note that in the ``WordCountProcessor``
implementation, you must refer to the same store name ``"Counts"`` when
accessing the key-value store, otherwise an exception will be thrown at
runtime, indicating that the state store cannot be found. If the state
store is not associated with the processor in the ``Topology`` code,
accessing it in the processor’s ``init()`` method will also throw an
exception at runtime, indicating the state store is not accessible from
this processor.

Now that you have fully defined your processor topology in your
application, you can proceed to `running the Kafka Streams
application <running-app.html#streams-developer-guide-execution>`__.





