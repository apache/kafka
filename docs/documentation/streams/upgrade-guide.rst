Upgrade Guide and API Changes
=============================

.. contents::
    :local:

If you are using Java 7 and want to upgrade from 1.0.x to 1.1.0 you
don't need to make any code changes as the public API is fully backward
compatible. If you are using Java 8 method references in your Kafka
Streams code you might need to update your code to resolve method
ambiguties. Hot-swaping the jar-file only might not work for this case.
See `below <#streams_api_changes_110>`__ for a complete list of 1.1.0
API and semantic changes that allow you to advance your application
and/or simplify your code base.

If you want to upgrade from 0.11.0.x to 1.0.0 you don't need to make any
code changes as the public API is fully backward compatible. However,
some public APIs were deprecated and thus it is recommended to update
your code eventually to allow for future upgrades. See
`below <#streams_api_changes_100>`__ for a complete list of 1.0.0 API
and semantic changes that allow you to advance your application and/or
simplify your code base.

If you want to upgrade from 0.10.2.x to 0.11.0 you don't need to make
any code changes as the public API is fully backward compatible.
However, some configuration parameters were deprecated and thus it is
recommended to update your code eventually to allow for future upgrades.
See `below <#streams_api_changes_0110>`__ for a complete list of 0.11.0
API and semantic changes that allow you to advance your application
and/or simplify your code base.

If you want to upgrade from 0.10.1.x to 0.10.2, see the `**Upgrade
Section for
0.10.2** </%7B%7Bversion%7D%7D/documentation/#upgrade_1020_streams>`__.
It highlights incompatible changes you need to consider to upgrade your
code and application. See `below <#streams_api_changes_0102>`__ for a
complete list of 0.10.2 API and semantic changes that allow you to
advance your application and/or simplify your code base.

If you want to upgrade from 0.10.0.x to 0.10.1, see the `**Upgrade
Section for
0.10.1** </%7B%7Bversion%7D%7D/documentation/#upgrade_1010_streams>`__.
It highlights incompatible changes you need to consider to upgrade your
code and application. See `below <#streams_api_changes_0101>`__ a
complete list of 0.10.1 API changes that allow you to advance your
application and/or simplify your code base, including the usage of new
features.

`Streams API changes in 1.1.0 <#streams_api_changes_110>`__
-----------------------------------------------------------

We have added support for methods in ``ReadOnlyWindowStore`` which
allows for querying ``WindowStore``\ s without the neccesity of
providing keys.

There is a new artifact ``kafka-streams-test-utils`` providing a
``TopologyTestDriver``, ``ConsumerRecordFactory``, and
``OutputVerifier`` class. You can include the new artifact as a regular
dependency to your unit tests and use the test driver to test your
business logic of your Kafka Streams application. For more details, see
`KIP-247 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-247%3A+Add+public+test+utils+for+Kafka+Streams>`__.

The introduction of
`KIP-220 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-220%3A+Add+AdminClient+into+Kafka+Streams%27+ClientSupplier>`__
enables you to provide configuration parameters for the embedded admin
client created by Kafka Streams, similar to the embedded producer and
consumer clients. You can provide the configs via ``StreamsConfig`` by
adding the configs with the prefix ``admin.`` as defined by
``StreamsConfig#adminClientPrefix(String)`` to distinguish them from
configurations of other clients that share the same config names.

New method in ``GlobalKTable``

-  A method has been provided such that it will return the store name
   associated with the ``GlobalKTable`` or ``null`` if the store name is
   non-queryable.

New methods in ``KafkaStreams``:

-  added overload for the constructor that allows overriding the
   ``Time`` object used for tracking system wall-clock time; this is
   useful for unit testing your application code.

New methods in ``KafkaClientSupplier``:

-  added ``getAdminClient(config)`` that allows to override an
   ``AdminClient`` used for administrative requests such as internal
   topic creations, etc.

New error handling for exceptions during production:

-  added interface ``ProductionExceptionHandler`` that allows
   implementors to decide whether or not Streams should ``FAIL`` or
   ``CONTINUE`` when certain exception occur while trying to produce.
-  provided an implementation, ``DefaultProductionExceptionHandler``
   that always fails, preserving the existing behavior by default.
-  changing which implementation is used can be done by settings
   ``default.production.exception.handler`` to the fully qualified name
   of a class implementing this interface.

Changes in ``StreamsResetter``:

-  added options to specify input topics offsets to reset according to
   `KIP-171 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-171+-+Extend+Consumer+Group+Reset+Offset+for+Stream+Application>`__

`Streams API changes in 1.0.0 <#streams_api_changes_100>`__
-----------------------------------------------------------

With 1.0 a major API refactoring was accomplished and the new API is
cleaner and easier to use. This change includes the five main classes
``KafkaStreams``, ``KStreamBuilder``, ``KStream``, ``KTable``, and
``TopologyBuilder`` (and some more others). All changes are fully
backward compatible as old API is only deprecated but not removed. We
recommend to move to the new API as soon as you can. We will summarize
all API changes in the next paragraphs.

The two main classes to specify a topology via the DSL
(``KStreamBuilder``) or the Processor API (``TopologyBuilder``) were
deprecated and replaced by ``StreamsBuilder`` and ``Topology`` (both new
classes are located in package ``org.apache.kafka.streams``). Note, that
``StreamsBuilder`` does not extend ``Topology``, i.e., the class
hierarchy is different now. The new classes have basically the same
methods as the old ones to build a topology via DSL or Processor API.
However, some internal methods that were public in ``KStreamBuilder``
and ``TopologyBuilder`` but not part of the actual API are not present
in the new classes any longer. Furthermore, some overloads were
simplified compared to the original classes. See
`KIP-120 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-120%3A+Cleanup+Kafka+Streams+builder+API>`__
and
`KIP-182 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-182%3A+Reduce+Streams+DSL+overloads+and+allow+easier+use+of+custom+storage+engines>`__
for full details.

Changing how a topology is specified also affects ``KafkaStreams``
constructors, that now only accept a ``Topology``. Using the DSL builder
class ``StreamsBuilder`` one can get the constructed ``Topology`` via
``StreamsBuilder#build()``. Additionally, a new class
``org.apache.kafka.streams.TopologyDescription`` (and some more
dependent classes) were added. Those can be used to get a detailed
description of the specified topology and can be obtained by calling
``Topology#describe()``. An example using this new API is shown in the
`quickstart
section </%7B%7Bversion%7D%7D/documentation/streams/quickstart>`__.

New methods in ``KStream``:

-  With the introduction of
   `KIP-202 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-202+Move+merge%28%29+from+StreamsBuilder+to+KStream>`__
   a new method ``merge()`` has been created in ``KStream`` as the
   StreamsBuilder class's ``StreamsBuilder#merge()`` has been removed.
   The method signature was also changed, too: instead of providing
   multiple ``KStream``\ s into the method at the once, only a single
   ``KStream`` is accepted.

New methods in ``KafkaStreams``:

-  retrieve the current runtime information about the local threads via
   ``localThreadsMetadata()``
-  observe the restoration of all state stores via
   ``setGlobalStateRestoreListener()``, in which users can provide their
   customized implementation of the
   ``org.apache.kafka.streams.processor.StateRestoreListener`` interface

Deprecated / modified methods in ``KafkaStreams``:

-  ``toString()``, ``toString(final String indent)`` were previously
   used to return static and runtime information. They have been
   deprecated in favor of using the new classes/methods
   ``localThreadsMetadata()`` / ``ThreadMetadata`` (returning runtime
   information) and ``TopologyDescription`` / ``Topology#describe()``
   (returning static information).
-  With the introduction of
   `KIP-182 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-182%3A+Reduce+Streams+DSL+overloads+and+allow+easier+use+of+custom+storage+engines>`__
   you should no longer pass in ``Serde`` to ``KStream#print``
   operations. If you can't rely on using ``toString`` to print your
   keys an values, you should instead you provide a custom
   ``KeyValueMapper`` via the ``Printed#withKeyValueMapper`` call.
-  ``setStateListener()`` now can only be set before the application
   start running, i.e. before ``KafkaStreams.start()`` is called.

Deprecated methods in ``KGroupedStream``

-  Windowed aggregations have been deprecated from ``KGroupedStream``
   and moved to ``WindowedKStream``. You can now perform a windowed
   aggregation by, for example, using
   ``KGroupedStream#windowedBy(Windows)#reduce(Reducer)``.

Modified methods in ``Processor``:

-  The Processor API was extended to allow users to schedule
   ``punctuate`` functions either based on data-driven **stream time**
   or wall-clock time. As a result, the original
   ``ProcessorContext#schedule`` is deprecated with a new overloaded
   function that accepts a user customizable ``Punctuator`` callback
   interface, which triggers its ``punctuate`` API method periodically
   based on the ``PunctuationType``. The ``PunctuationType`` determines
   what notion of time is used for the punctuation scheduling: either
   `stream
   time </%7B%7Bversion%7D%7D/documentation/streams/core-concepts#streams_time>`__
   or wall-clock time (by default, **stream time** is configured to
   represent event time via ``TimestampExtractor``). In addition, the
   ``punctuate`` function inside ``Processor`` is also deprecated.

   Before this, users could only schedule based on stream time (i.e.
   ``PunctuationType.STREAM_TIME``) and hence the ``punctuate`` function
   was data-driven only because stream time is determined (and advanced
   forward) by the timestamps derived from the input data. If there is
   no data arriving at the processor, the stream time would not advance
   and hence punctuation will not be triggered. On the other hand, When
   wall-clock time (i.e. ``PunctuationType.WALL_CLOCK_TIME``) is used,
   ``punctuate`` will be triggered purely based on wall-clock time. So
   for example if the ``Punctuator`` function is scheduled based on
   ``PunctuationType.WALL_CLOCK_TIME``, if these 60 records were
   processed within 20 seconds, ``punctuate`` would be called 2 times
   (one time every 10 seconds); if these 60 records were processed
   within 5 seconds, then no ``punctuate`` would be called at all. Users
   can schedule multiple ``Punctuator`` callbacks with different
   ``PunctuationType``\ s within the same processor by simply calling
   ``ProcessorContext#schedule`` multiple times inside processor's
   ``init()`` method.

If you are monitoring on task level or processor-node / state store
level Streams metrics, please note that the metrics sensor name and
hierarchy was changed: The task ids, store names and processor names are
no longer in the sensor metrics names, but instead are added as tags of
the sensors to achieve consistent metrics hierarchy. As a result you may
need to make corresponding code changes on your metrics reporting and
monitoring tools when upgrading to 1.0.0. Detailed metrics sensor can be
found in the `Streams Monitoring <#kafka_streams_monitoring>`__ section.

The introduction of
`KIP-161 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-161%3A+streams+deserialization+exception+handlers>`__
enables you to provide a default exception handler for deserialization
errors when reading data from Kafka rather than throwing the exception
all the way out of your streams application. You can provide the configs
via the ``StreamsConfig`` as
``StreamsConfig#DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG``.
The specified handler must implement the
``org.apache.kafka.streams.errors.DeserializationExceptionHandler``
interface.

The introduction of
`KIP-173 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-173%3A+Add+prefix+to+StreamsConfig+to+enable+setting+default+internal+topic+configs>`__
enables you to provide topic configuration parameters for any topics
created by Kafka Streams. This includes repartition and changelog
topics. You can provide the configs via the ``StreamsConfig`` by adding
the configs with the prefix as defined by
``StreamsConfig#topicPrefix(String)``. Any properties in the
``StreamsConfig`` with the prefix will be applied when creating internal
topics. Any configs that aren't topic configs will be ignored. If you
already use ``StateStoreSupplier`` or ``Materialized`` to provide
configs for changelogs, then they will take precedence over those
supplied in the config.

`Streams API changes in 0.11.0.0 <#streams_api_changes_0110>`__
---------------------------------------------------------------

Updates in ``StreamsConfig``:

-  new configuration parameter ``processing.guarantee`` is added
-  configuration parameter ``key.serde`` was deprecated and replaced by
   ``default.key.serde``
-  configuration parameter ``value.serde`` was deprecated and replaced
   by ``default.value.serde``
-  configuration parameter ``timestamp.extractor`` was deprecated and
   replaced by ``default.timestamp.extractor``
-  method ``keySerde()`` was deprecated and replaced by
   ``defaultKeySerde()``
-  method ``valueSerde()`` was deprecated and replaced by
   ``defaultValueSerde()``
-  new method ``defaultTimestampExtractor()`` was added

New methods in ``TopologyBuilder``:

-  added overloads for ``addSource()`` that allow to define a
   ``TimestampExtractor`` per source node
-  added overloads for ``addGlobalStore()`` that allow to define a
   ``TimestampExtractor`` per source node associated with the global
   store

New methods in ``KStreamBuilder``:

-  added overloads for ``stream()`` that allow to define a
   ``TimestampExtractor`` per input stream
-  added overloads for ``table()`` that allow to define a
   ``TimestampExtractor`` per input table
-  added overloads for ``globalKTable()`` that allow to define a
   ``TimestampExtractor`` per global table

Deprecated methods in ``KTable``:

-  ``void foreach(final ForeachAction<? super K, ? super V> action)``
-  ``void print()``
-  ``void print(final String streamName)``
-  ``void print(final Serde<K> keySerde, final Serde<V> valSerde)``
-  ``void print(final Serde<K> keySerde, final Serde<V> valSerde, final String streamName)``
-  ``void writeAsText(final String filePath)``
-  ``void writeAsText(final String filePath, final String streamName)``
-  ``void writeAsText(final String filePath, final Serde<K> keySerde, final Serde<V> valSerde)``
-  ``void writeAsText(final String filePath, final String streamName, final Serde<K> keySerde, final Serde<V> valSerde)``

The above methods have been deprecated in favor of using the Interactive
Queries API. If you want to query the current content of the state store
backing the KTable, use the following approach:

-  Make a call to
   ``KafkaStreams.store(final String storeName, final QueryableStoreType<T> queryableStoreType)``
-  Then make a call to ``ReadOnlyKeyValueStore.all()`` to iterate over
   the keys of a ``KTable``.

If you want to view the changelog stream of the ``KTable`` then you
could call ``KTable.toStream().print(Printed.toSysOut)``.

Metrics using exactly-once semantics:

If exactly-once processing is enabled via the ``processing.guarantees``
parameter, internally Streams switches from a producer per thread to a
producer per task runtime model. In order to distinguish the different
producers, the producer's ``client.id`` additionally encodes the task-ID
for this case. Because the producer's ``client.id`` is used to report
JMX metrics, it might be required to update tools that receive those
metrics.

Producer's ``client.id`` naming schema:

-  at-least-once (default):
   ``[client.Id]-StreamThread-[sequence-number]``
-  exactly-once: ``[client.Id]-StreamThread-[sequence-number]-[taskId]``

``[client.Id]`` is either set via Streams configuration parameter
``client.id`` or defaults to ``[application.id]-[processId]``
(``[processId]`` is a random UUID).

`Notable changes in 0.10.2.1 <#streams_api_changes_01021>`__
------------------------------------------------------------

Parameter updates in ``StreamsConfig``:

-  The default config values of embedded producer's ``retries`` and
   consumer's ``max.poll.interval.ms`` have been changed to improve the
   resiliency of a Kafka Streams application

`Streams API changes in 0.10.2.0 <#streams_api_changes_0102>`__
---------------------------------------------------------------

New methods in ``KafkaStreams``:

-  set a listener to react on application state change via
   ``setStateListener(StateListener listener)``
-  retrieve the current application state via ``state()``
-  retrieve the global metrics registry via ``metrics()``
-  apply a timeout when closing an application via
   ``close(long timeout, TimeUnit timeUnit)``
-  specify a custom indent when retrieving Kafka Streams information via
   ``toString(String indent)``

Parameter updates in ``StreamsConfig``:

-  parameter ``zookeeper.connect`` was deprecated; a Kafka Streams
   application does no longer interact with ZooKeeper for topic
   management but uses the new broker admin protocol (cf. `KIP-4,
   Section "Topic Admin
   Schema" <https://cwiki.apache.org/confluence/display/KAFKA/KIP-4+-+Command+line+and+centralized+administrative+operations#KIP-4-Commandlineandcentralizedadministrativeoperations-TopicAdminSchema.1>`__)
-  added many new parameters for metrics, security, and client
   configurations

Changes in ``StreamsMetrics`` interface:

-  removed methods: ``addLatencySensor()``
-  added methods: ``addLatencyAndThroughputSensor()``,
   ``addThroughputSensor()``, ``recordThroughput()``, ``addSensor()``,
   ``removeSensor()``

New methods in ``TopologyBuilder``:

-  added overloads for ``addSource()`` that allow to define a
   ``auto.offset.reset`` policy per source node
-  added methods ``addGlobalStore()`` to add global ``StateStore``\ s

New methods in ``KStreamBuilder``:

-  added overloads for ``stream()`` and ``table()`` that allow to define
   a ``auto.offset.reset`` policy per input stream/table
-  added method ``globalKTable()`` to create a ``GlobalKTable``

New joins for ``KStream``:

-  added overloads for ``join()`` to join with ``KTable``
-  added overloads for ``join()`` and ``leftJoin()`` to join with
   ``GlobalKTable``
-  note, join semantics in 0.10.2 were improved and thus you might see
   different result compared to 0.10.0.x and 0.10.1.x (cf. `Kafka
   Streams Join
   Semantics <https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Join+Semantics>`__
   in the Apache Kafka wiki)

Aligned ``null``-key handling for ``KTable`` joins:

-  like all other KTable operations, ``KTable-KTable`` joins do not
   throw an exception on ``null`` key records anymore, but drop those
   records silently

New window type *Session Windows*:

-  added class ``SessionWindows`` to specify session windows
-  added overloads for ``KGroupedStream`` methods ``count()``,
   ``reduce()``, and ``aggregate()`` to allow session window
   aggregations

Changes to ``TimestampExtractor``:

-  method ``extract()`` has a second parameter now
-  new default timestamp extractor class ``FailOnInvalidTimestamp`` (it
   gives the same behavior as old (and removed) default extractor
   ``ConsumerRecordTimestampExtractor``)
-  new alternative timestamp extractor classes
   ``LogAndSkipOnInvalidTimestamp`` and
   ``UsePreviousTimeOnInvalidTimestamps``

Relaxed type constraints of many DSL interfaces, classes, and methods
(cf.
`KIP-100 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-100+-+Relax+Type+constraints+in+Kafka+Streams+API>`__).

`Streams API changes in 0.10.1.0 <#streams_api_changes_0101>`__
---------------------------------------------------------------

Stream grouping and aggregation split into two methods:

-  old: KStream #aggregateByKey(), #reduceByKey(), and #countByKey()
-  new: KStream#groupByKey() plus KGroupedStream #aggregate(),
   #reduce(), and #count()
-  Example: stream.countByKey() changes to stream.groupByKey().count()

Auto Repartitioning:

-  a call to through() after a key-changing operator and before an
   aggregation/join is no longer required
-  Example: stream.selectKey(...).through(...).countByKey() changes to
   stream.selectKey().groupByKey().count()

TopologyBuilder:

-  methods #sourceTopics(String applicationId) and #topicGroups(String
   applicationId) got simplified to #sourceTopics() and #topicGroups()

DSL: new parameter to specify state store names:

-  The new Interactive Queries feature requires to specify a store name
   for all source KTables and window aggregation result KTables
   (previous parameter "operator/window name" is now the storeName)
-  KStreamBuilder#table(String topic) changes to #topic(String topic,
   String storeName)
-  KTable#through(String topic) changes to #through(String topic, String
   storeName)
-  KGroupedStream #aggregate(), #reduce(), and #count() require
   additional parameter "String storeName"
-  Example: stream.countByKey(TimeWindows.of("windowName", 1000))
   changes to stream.groupByKey().count(TimeWindows.of(1000),
   "countStoreName")

Windowing:

-  Windows are not named anymore: TimeWindows.of("name", 1000) changes
   to TimeWindows.of(1000) (cf. DSL: new parameter to specify state
   store names)
-  JoinWindows has no default size anymore:
   JoinWindows.of("name").within(1000) changes to JoinWindows.of(1000)


