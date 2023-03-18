# Kafka Connect

## 8.1 Overview {#connect_overview}

Kafka Connect is a tool for scalably and reliably streaming data between
Apache Kafka and other systems. It makes it simple to quickly define
*connectors* that move large collections of data into and out of Kafka.
Kafka Connect can ingest entire databases or collect metrics from all
your application servers into Kafka topics, making the data available
for stream processing with low latency. An export job can deliver data
from Kafka topics into secondary storage and query systems or into batch
systems for offline analysis.

Kafka Connect features include:

-   **A common framework for Kafka connectors** - Kafka Connect
    standardizes integration of other data systems with Kafka,
    simplifying connector development, deployment, and management
-   **Distributed and standalone modes** - scale up to a large,
    centrally managed service supporting an entire organization or scale
    down to development, testing, and small production deployments
-   **REST interface** - submit and manage connectors to your Kafka
    Connect cluster via an easy to use REST API
-   **Automatic offset management** - with just a little information
    from connectors, Kafka Connect can manage the offset commit process
    automatically so connector developers do not need to worry about
    this error prone part of connector development
-   **Distributed and scalable by default** - Kafka Connect builds on
    the existing group management protocol. More workers can be added to
    scale up a Kafka Connect cluster.
-   **Streaming/batch integration** - leveraging Kafka\'s existing
    capabilities, Kafka Connect is an ideal solution for bridging
    streaming and batch data systems

## 8.2 User Guide {#connect_user}

The [quickstart](../quickstart) provides a brief example of how to run a
standalone version of Kafka Connect. This section describes how to
configure, run, and manage Kafka Connect in more detail.

### Running Kafka Connect {#connect_running}

Kafka Connect currently supports two modes of execution: standalone
(single process) and distributed.

In standalone mode all work is performed in a single process. This
configuration is simpler to setup and get started with and may be useful
in situations where only one worker makes sense (e.g. collecting log
files), but it does not benefit from some of the features of Kafka
Connect such as fault tolerance. You can start a standalone process with
the following command:

```shell {linenos=false} {.brush: .bash;}
> bin/connect-standalone.sh config/connect-standalone.properties [connector1.properties connector2.properties ...]
```

The first parameter is the configuration for the worker. This includes
settings such as the Kafka connection parameters, serialization format,
and how frequently to commit offsets. The provided example should work
well with a local cluster running with the default configuration
provided by `config/server.properties`. It will require tweaking to use
with a different configuration or production deployment. All workers
(both standalone and distributed) require a few configs:

-   `bootstrap.servers` - List of Kafka servers used to bootstrap
    connections to Kafka
-   `key.converter` - Converter class used to convert between Kafka
    Connect format and the serialized form that is written to Kafka.
    This controls the format of the keys in messages written to or read
    from Kafka, and since this is independent of connectors it allows
    any connector to work with any serialization format. Examples of
    common formats include JSON and Avro.
-   `value.converter` - Converter class used to convert between Kafka
    Connect format and the serialized form that is written to Kafka.
    This controls the format of the values in messages written to or
    read from Kafka, and since this is independent of connectors it
    allows any connector to work with any serialization format. Examples
    of common formats include JSON and Avro.
-   `plugin.path` (default `empty`) - a list of paths that contain
    Connect plugins (connectors, converters, transformations). Before
    running quick starts, users must add the absolute path that contains
    the example FileStreamSourceConnector and FileStreamSinkConnector
    packaged in `connect-file-"version".jar`, because these connectors
    are not included by default to the `CLASSPATH` or the `plugin.path`
    of the Connect worker (see
    [plugin.path](#connectconfigs_plugin.path) property for examples).

The important configuration options specific to standalone mode are:

-   `offset.storage.file.filename` - File to store source connector
    offsets

The parameters that are configured here are intended for producers and
consumers used by Kafka Connect to access the configuration, offset and
status topics. For configuration of the producers used by Kafka source
tasks and the consumers used by Kafka sink tasks, the same parameters
can be used but need to be prefixed with `producer.` and `consumer.`
respectively. The only Kafka client parameter that is inherited without
a prefix from the worker configuration is `bootstrap.servers`, which in
most cases will be sufficient, since the same cluster is often used for
all purposes. A notable exception is a secured cluster, which requires
extra parameters to allow connections. These parameters will need to be
set up to three times in the worker configuration, once for management
access, once for Kafka sources and once for Kafka sinks.

Starting with 2.3.0, client configuration overrides can be configured
individually per connector by using the prefixes `producer.override.`
and `consumer.override.` for Kafka sources or Kafka sinks respectively.
These overrides are included with the rest of the connector\'s
configuration properties.

The remaining parameters are connector configuration files. You may
include as many as you want, but all will execute within the same
process (on different threads). You can also choose not to specify any
connector configuration files on the command line, and instead use the
REST API to create connectors at runtime after your standalone worker
starts.

Distributed mode handles automatic balancing of work, allows you to
scale up (or down) dynamically, and offers fault tolerance both in the
active tasks and for configuration and offset commit data. Execution is
very similar to standalone mode:

```shell {linenos=false} {.brush: .bash;}
> bin/connect-distributed.sh config/connect-distributed.properties
```

The difference is in the class which is started and the configuration
parameters which change how the Kafka Connect process decides where to
store configurations, how to assign work, and where to store offsets and
task statues. In the distributed mode, Kafka Connect stores the offsets,
configs and task statuses in Kafka topics. It is recommended to manually
create the topics for offset, configs and statuses in order to achieve
the desired the number of partitions and replication factors. If the
topics are not yet created when starting Kafka Connect, the topics will
be auto created with default number of partitions and replication
factor, which may not be best suited for its usage.

In particular, the following configuration parameters, in addition to
the common settings mentioned above, are critical to set before starting
your cluster:

-   `group.id` (default `connect-cluster`) - unique name for the
    cluster, used in forming the Connect cluster group; note that this
    **must not conflict** with consumer group IDs
-   `config.storage.topic` (default `connect-configs`) - topic to use
    for storing connector and task configurations; note that this should
    be a single partition, highly replicated, compacted topic. You may
    need to manually create the topic to ensure the correct
    configuration as auto created topics may have multiple partitions or
    be automatically configured for deletion rather than compaction
-   `offset.storage.topic` (default `connect-offsets`) - topic to use
    for storing offsets; this topic should have many partitions, be
    replicated, and be configured for compaction
-   `status.storage.topic` (default `connect-status`) - topic to use for
    storing statuses; this topic can have multiple partitions, and
    should be replicated and configured for compaction

Note that in distributed mode the connector configurations are not
passed on the command line. Instead, use the REST API described below to
create, modify, and destroy connectors.

### Configuring Connectors {#connect_configuring}

Connector configurations are simple key-value mappings. In both
standalone and distributed mode, they are included in the JSON payload
for the REST request that creates (or modifies) the connector. In
standalone mode these can also be defined in a properties file and
passed to the Connect process on the command line.

Most configurations are connector dependent, so they can\'t be outlined
here. However, there are a few common options:

-   `name` - Unique name for the connector. Attempting to register again
    with the same name will fail.
-   `connector.class` - The Java class for the connector
-   `tasks.max` - The maximum number of tasks that should be created for
    this connector. The connector may create fewer tasks if it cannot
    achieve this level of parallelism.
-   `key.converter` - (optional) Override the default key converter set
    by the worker.
-   `value.converter` - (optional) Override the default value converter
    set by the worker.

The `connector.class` config supports several formats: the full name or
alias of the class for this connector. If the connector is
org.apache.kafka.connect.file.FileStreamSinkConnector, you can either
specify this full name or use FileStreamSink or FileStreamSinkConnector
to make the configuration a bit shorter.

Sink connectors also have a few additional options to control their
input. Each sink connector must set one of the following:

-   `topics` - A comma-separated list of topics to use as input for this
    connector
-   `topics.regex` - A Java regular expression of topics to use as input
    for this connector

For any other options, you should consult the documentation for the
connector.

### Transformations {#connect_transforms}

Connectors can be configured with transformations to make lightweight
message-at-a-time modifications. They can be convenient for data
massaging and event routing.

A transformation chain can be specified in the connector configuration.

-   `transforms` - List of aliases for the transformation, specifying
    the order in which the transformations will be applied.
-   `transforms.$alias.type` - Fully qualified class name for the
    transformation.
-   `transforms.$alias.$transformationSpecificConfig` Configuration
    properties for the transformation

For example, lets take the built-in file source connector and use a
transformation to add a static field.

Throughout the example we\'ll use schemaless JSON data format. To use
schemaless format, we changed the following two lines in
`connect-standalone.properties` from true to false:

``` {.brush: .text;}
key.converter.schemas.enable
value.converter.schemas.enable
```

The file source connector reads each line as a String. We will wrap each
line in a Map and then add a second field to identify the origin of the
event. To do this, we use two transformations:

-   **HoistField** to place the input line inside a Map
-   **InsertField** to add the static field. In this example we\'ll
    indicate that the record came from a file connector

After adding the transformations, `connect-file-source.properties` file
looks as following:

```java-properties {.brush: .text;}
name=local-file-source
connector.class=FileStreamSource
tasks.max=1
file=test.txt
topic=connect-test
transforms=MakeMap, InsertSource
transforms.MakeMap.type=org.apache.kafka.connect.transforms.HoistField$Value
transforms.MakeMap.field=line
transforms.InsertSource.type=org.apache.kafka.connect.transforms.InsertField$Value
transforms.InsertSource.static.field=data_source
transforms.InsertSource.static.value=test-file-source
```

All the lines starting with `transforms` were added for the
transformations. You can see the two transformations we created:
\"InsertSource\" and \"MakeMap\" are aliases that we chose to give the
transformations. The transformation types are based on the list of
built-in transformations you can see below. Each transformation type has
additional configuration: HoistField requires a configuration called
\"field\", which is the name of the field in the map that will include
the original String from the file. InsertField transformation lets us
specify the field name and the value that we are adding.

When we ran the file source connector on my sample file without the
transformations, and then read them using `kafka-console-consumer.sh`,
the results were:

``` {.brush: .text;}
"foo"
"bar"
"hello world"
```

We then create a new file connector, this time after adding the
transformations to the configuration file. This time, the results will
be:

``` {.brush: .json;}
{"line":"foo","data_source":"test-file-source"}
{"line":"bar","data_source":"test-file-source"}
{"line":"hello world","data_source":"test-file-source"}
```

You can see that the lines we\'ve read are now part of a JSON map, and
there is an extra field with the static value we specified. This is just
one example of what you can do with transformations.

#### Included transformations {#connect_included_transformation}

Several widely-applicable data and routing transformations are included
with Kafka Connect:

-   InsertField - Add a field using either static data or record
    metadata
-   ReplaceField - Filter or rename fields
-   MaskField - Replace field with valid null value for the type (0,
    empty string, etc) or custom replacement (non-empty string or
    numeric value only)
-   ValueToKey - Replace the record key with a new key formed from a
    subset of fields in the record value
-   HoistField - Wrap the entire event as a single field inside a Struct
    or a Map
-   ExtractField - Extract a specific field from Struct and Map and
    include only this field in results
-   SetSchemaMetadata - modify the schema name or version
-   TimestampRouter - Modify the topic of a record based on original
    topic and timestamp. Useful when using a sink that needs to write to
    different tables or indexes based on timestamps
-   RegexRouter - modify the topic of a record based on original topic,
    replacement string and a regular expression
-   Filter - Removes messages from all further processing. This is used
    with a [predicate](#connect_predicates) to selectively filter
    certain messages.
-   InsertHeader - Add a header using static data
-   HeadersFrom - Copy or move fields in the key or value to the record
    headers
-   DropHeaders - Remove headers by name

Details on how to configure each transformation are listed below:

{{< connect-component-configs connect_transforms  >}}

#### Predicates {#connect_predicates}

Transformations can be configured with predicates so that the
transformation is applied only to messages which satisfy some condition.
In particular, when combined with the **Filter** transformation
predicates can be used to selectively filter out certain messages.

Predicates are specified in the connector configuration.

-   `predicates` - Set of aliases for the predicates to be applied to
    some of the transformations.
-   `predicates.$alias.type` - Fully qualified class name for the
    predicate.
-   `predicates.$alias.$predicateSpecificConfig` - Configuration
    properties for the predicate.

All transformations have the implicit config properties `predicate` and
`negate`. A predicular predicate is associated with a transformation by
setting the transformation\'s `predicate` config to the predicate\'s
alias. The predicate\'s value can be reversed using the `negate`
configuration property.

For example, suppose you have a source connector which produces messages
to many different topics and you want to:

-   filter out the messages in the \'foo\' topic entirely
-   apply the ExtractField transformation with the field name
    \'other_field\' to records in all topics *except* the topic \'bar\'

To do this we need first to filter out the records destined for the
topic \'foo\'. The Filter transformation removes records from further
processing, and can use the TopicNameMatches predicate to apply the
transformation only to records in topics which match a certain regular
expression. TopicNameMatches\'s only configuration property is `pattern`
which is a Java regular expression for matching against the topic name.
The configuration would look like this:

```java-properties {.brush: .text;}
transforms=Filter
transforms.Filter.type=org.apache.kafka.connect.transforms.Filter
transforms.Filter.predicate=IsFoo

predicates=IsFoo
predicates.IsFoo.type=org.apache.kafka.connect.transforms.predicates.TopicNameMatches
predicates.IsFoo.pattern=foo
```

Next we need to apply ExtractField only when the topic name of the
record is not \'bar\'. We can\'t just use TopicNameMatches directly,
because that would apply the transformation to matching topic names, not
topic names which do *not* match. The transformation\'s implicit
`negate` config properties allows us to invert the set of records which
a predicate matches. Adding the configuration for this to the previous
example we arrive at:

```java-properties {.brush: .text;}
transforms=Filter,Extract
transforms.Filter.type=org.apache.kafka.connect.transforms.Filter
transforms.Filter.predicate=IsFoo

transforms.Extract.type=org.apache.kafka.connect.transforms.ExtractField$Key
transforms.Extract.field=other_field
transforms.Extract.predicate=IsBar
transforms.Extract.negate=true

predicates=IsFoo,IsBar
predicates.IsFoo.type=org.apache.kafka.connect.transforms.predicates.TopicNameMatches
predicates.IsFoo.pattern=foo

predicates.IsBar.type=org.apache.kafka.connect.transforms.predicates.TopicNameMatches
predicates.IsBar.pattern=bar
```

Kafka Connect includes the following predicates:

-   `TopicNameMatches` - matches records in a topic with a name matching a particular Java regular expression.
-   `HasHeaderKey` - matches records which have a header with the given key.
-   `RecordIsTombstone` - matches tombstone records, that is records with a null value.

Details on how to configure each predicate are listed below:

{{< connect-component-configs connect_predicates  >}}

### REST API {#connect_rest}

Since Kafka Connect is intended to be run as a service, it also provides
a REST API for managing connectors. This REST API is available in both
standalone and distributed mode. The REST API server can be configured
using the `listeners` configuration option. This field should contain a
list of listeners in the following format:
`protocol://host:port,protocol2://host2:port2`. Currently supported
protocols are `http` and `https`. For example:

```shell {linenos=false} {.brush: .text;}
listeners=http://localhost:8080,https://localhost:8443
```

By default, if no `listeners` are specified, the REST server runs on
port 8083 using the HTTP protocol. When using HTTPS, the configuration
has to include the SSL configuration. By default, it will use the
`ssl.*` settings. In case it is needed to use different configuration
for the REST API than for connecting to Kafka brokers, the fields can be
prefixed with `listeners.https`. When using the prefix, only the
prefixed options will be used and the `ssl.*` options without the prefix
will be ignored. Following fields can be used to configure HTTPS for the
REST API:

-   `ssl.keystore.location`
-   `ssl.keystore.password`
-   `ssl.keystore.type`
-   `ssl.key.password`
-   `ssl.truststore.location`
-   `ssl.truststore.password`
-   `ssl.truststore.type`
-   `ssl.enabled.protocols`
-   `ssl.provider`
-   `ssl.protocol`
-   `ssl.cipher.suites`
-   `ssl.keymanager.algorithm`
-   `ssl.secure.random.implementation`
-   `ssl.trustmanager.algorithm`
-   `ssl.endpoint.identification.algorithm`
-   `ssl.client.auth`

The REST API is used not only by users to monitor / manage Kafka
Connect. In distributed mode, it is also used for the Kafka Connect
cross-cluster communication. Some requests received on the follower
nodes REST API will be forwarded to the leader node REST API. In case
the URI under which is given host reachable is different from the URI
which it listens on, the configuration options
`rest.advertised.host.name`, `rest.advertised.port` and
`rest.advertised.listener` can be used to change the URI which will be
used by the follower nodes to connect with the leader. When using both
HTTP and HTTPS listeners, the `rest.advertised.listener` option can be
also used to define which listener will be used for the cross-cluster
communication. When using HTTPS for communication between nodes, the
same `ssl.*` or `listeners.https` options will be used to configure the
HTTPS client.

The following are the currently supported REST API endpoints:

-   `GET /connectors` - return a list of active connectors
-   `POST /connectors` - create a new connector; the request body should
    be a JSON object containing a string `name` field and an object
    `config` field with the connector configuration parameters
-   `GET /connectors/{name}` - get information about a specific
    connector
-   `GET /connectors/{name}/config` - get the configuration parameters
    for a specific connector
-   `PUT /connectors/{name}/config` - update the configuration
    parameters for a specific connector
-   `GET /connectors/{name}/status` - get current status of the
    connector, including if it is running, failed, paused, etc., which
    worker it is assigned to, error information if it has failed, and
    the state of all its tasks
-   `GET /connectors/{name}/tasks` - get a list of tasks currently
    running for a connector
-   `GET /connectors/{name}/tasks/{taskid}/status` - get current status
    of the task, including if it is running, failed, paused, etc., which
    worker it is assigned to, and error information if it has failed
-   `PUT /connectors/{name}/pause` - pause the connector and its tasks,
    which stops message processing until the connector is resumed
-   `PUT /connectors/{name}/resume` - resume a paused connector (or do
    nothing if the connector is not paused)
-   `POST /connectors/{name}/restart?includeTasks=<true|false>&onlyFailed=<true|false>` -
    restart a connector and its tasks instances.
    -   the \"includeTasks\" parameter specifies whether to restart the
        connector instance and task instances (\"includeTasks=true\") or
        just the connector instance (\"includeTasks=false\"), with the
        default (\"false\") preserving the same behavior as earlier
        versions.
    -   the \"onlyFailed\" parameter specifies whether to restart just
        the instances with a FAILED status (\"onlyFailed=true\") or all
        instances (\"onlyFailed=false\"), with the default (\"false\")
        preserving the same behavior as earlier versions.
-   `POST /connectors/{name}/tasks/{taskId}/restart` - restart an
    individual task (typically because it has failed)
-   `DELETE /connectors/{name}` - delete a connector, halting all tasks
    and deleting its configuration
-   `GET /connectors/{name}/topics` - get the set of topics that a
    specific connector is using since the connector was created or since
    a request to reset its set of active topics was issued
-   `PUT /connectors/{name}/topics/reset` - send a request to empty the
    set of active topics of a connector

Kafka Connect also provides a REST API for getting information about
connector plugins:

-   `GET /connector-plugins`- return a list of connector plugins
    installed in the Kafka Connect cluster. Note that the API only
    checks for connectors on the worker that handles the request, which
    means you may see inconsistent results, especially during a rolling
    upgrade if you add new connector jars
-   `PUT /connector-plugins/{connector-type}/config/validate` - validate
    the provided configuration values against the configuration
    definition. This API performs per config validation, returns
    suggested values and error messages during validation.

The following is a supported REST request at the top-level (root)
endpoint:

-   `GET /`- return basic information about the Kafka Connect cluster
    such as the version of the Connect worker that serves the REST
    request (including git commit ID of the source code) and the Kafka
    cluster ID that is connected to.

For the complete specification of the REST API, see the 
[OpenAPI documentation](connect_rest.yaml)

### Error Reporting in Connect {#connect_errorreporting}

Kafka Connect provides error reporting to handle errors encountered
along various stages of processing. By default, any error encountered
during conversion or within transformations will cause the connector to
fail. Each connector configuration can also enable tolerating such
errors by skipping them, optionally writing each error and the details
of the failed operation and problematic record (with various levels of
detail) to the Connect application log. These mechanisms also capture
errors when a sink connector is processing the messages consumed from
its Kafka topics, and all of the errors can be written to a configurable
\"dead letter queue\" (DLQ) Kafka topic.

To report errors within a connector\'s converter, transforms, or within
the sink connector itself to the log, set `errors.log.enable=true` in
the connector configuration to log details of each error and problem
record\'s topic, partition, and offset. For additional debugging
purposes, set `errors.log.include.messages=true` to also log the problem
record key, value, and headers to the log (note this may log sensitive
information).

To report errors within a connector\'s converter, transforms, or within
the sink connector itself to a dead letter queue topic, set
`errors.deadletterqueue.topic.name`, and optionally
`errors.deadletterqueue.context.headers.enable=true`.

By default connectors exhibit \"fail fast\" behavior immediately upon an
error or exception. This is equivalent to adding the following
configuration properties with their defaults to a connector
configuration:

```java-properties {.brush: .text;}
# disable retries on failure
errors.retry.timeout=0

# do not log the error and their contexts
errors.log.enable=false

# do not record errors in a dead letter queue topic
errors.deadletterqueue.topic.name=

# Fail on first error
errors.tolerance=none
```

These and other related connector configuration properties can be
changed to provide different behavior. For example, the following
configuration properties can be added to a connector configuration to
setup error handling with multiple retries, logging to the application
logs and the `my-connector-errors` Kafka topic, and tolerating all
errors by reporting them rather than failing the connector task:

```java-properties {.brush: .text;}
# retry for at most 10 minutes times waiting up to 30 seconds between consecutive failures
errors.retry.timeout=600000
errors.retry.delay.max.ms=30000

# log error context along with application logs, but do not include configs and messages
errors.log.enable=true
errors.log.include.messages=false

# produce error context into the Kafka topic
errors.deadletterqueue.topic.name=my-connector-errors

# Tolerate all errors.
errors.tolerance=all
```

### Exactly-once support {#connect_exactlyonce}

Kafka Connect is capable of providing exactly-once semantics for sink
connectors (as of version 0.11.0) and source connectors (as of version
3.3.0). Please note that **support for exactly-once semantics is highly
dependent on the type of connector you run.** Even if you set all the
correct worker properties in the configuration for each node in a
cluster, if a connector is not designed to, or cannot take advantage of
the capabilities of the Kafka Connect framework, exactly-once may not be
possible.

#### Sink connectors {#connect_exactlyoncesink}

If a sink connector supports exactly-once semantics, to enable
exactly-once at the Connect worker level, you must ensure its consumer
group is configured to ignore records in aborted transactions. You can
do this by setting the worker property `consumer.isolation.level` to
`read_committed` or, if running a version of Kafka Connect that supports
it, using a 
[connector client config override policy](#connectconfigs_connector.client.config.override.policy) that
allows the `consumer.override.isolation.level` property to be set to
`read_committed` in individual connector configs. There are no
additional ACL requirements.

#### Source connectors {#connect_exactlyoncesource}

If a source connector supports exactly-once semantics, you must
configure your Connect cluster to enable framework-level support for
exactly-once source connectors. Additional ACLs may be necessary if
running against a secured Kafka cluster. Note that exactly-once support
for source connectors is currently only available in distributed mode;
standalone Connect workers cannot provide exactly-once semantics.

##### Worker configuration

For new Connect clusters, set the `exactly.once.source.support` property
to `enabled` in the worker config for each node in the cluster. For
existing clusters, two rolling upgrades are necessary. During the first
upgrade, the `exactly.once.source.support` property should be set to
`preparing`, and during the second, it should be set to `enabled`.

##### ACL requirements

With exactly-once source support enabled, the principal for each Connect
worker will require the following ACLs:

{{< connect-acl-resource-table acl-worker >}}

And the principal for each individual connector will require the
following ACLs:

{{< connect-acl-resource-table acl-connector >}}

## 8.3 Connector Development Guide {#connect_development}

This guide describes how developers can write new connectors for Kafka
Connect to move data between Kafka and other systems. It briefly reviews
a few key concepts and then describes how to create a simple connector.

### Core Concepts and APIs {#connect_concepts}

#### Connectors and Tasks {#connect_connectorsandtasks}

To copy data between Kafka and another system, users create a
`Connector` for the system they want to pull data from or push data to.
Connectors come in two flavors: `SourceConnectors` import data from
another system (e.g. `JDBCSourceConnector` would import a relational
database into Kafka) and `SinkConnectors` export data (e.g.
`HDFSSinkConnector` would export the contents of a Kafka topic to an
HDFS file).

`Connectors` do not perform any data copying themselves: their
configuration describes the data to be copied, and the `Connector` is
responsible for breaking that job into a set of `Tasks` that can be
distributed to workers. These `Tasks` also come in two corresponding
flavors: `SourceTask` and `SinkTask`.

With an assignment in hand, each `Task` must copy its subset of the data
to or from Kafka. In Kafka Connect, it should always be possible to
frame these assignments as a set of input and output streams consisting
of records with consistent schemas. Sometimes this mapping is obvious:
each file in a set of log files can be considered a stream with each
parsed line forming a record using the same schema and offsets stored as
byte offsets in the file. In other cases it may require more effort to
map to this model: a JDBC connector can map each table to a stream, but
the offset is less clear. One possible mapping uses a timestamp column
to generate queries incrementally returning new data, and the last
queried timestamp can be used as the offset.

#### Streams and Records {#connect_streamsandrecords}

Each stream should be a sequence of key-value records. Both the keys and
values can have complex structure \-- many primitive types are provided,
but arrays, objects, and nested data structures can be represented as
well. The runtime data format does not assume any particular
serialization format; this conversion is handled internally by the
framework.

In addition to the key and value, records (both those generated by
sources and those delivered to sinks) have associated stream IDs and
offsets. These are used by the framework to periodically commit the
offsets of data that have been processed so that in the event of
failures, processing can resume from the last committed offsets,
avoiding unnecessary reprocessing and duplication of events.

#### Dynamic Connectors {#connect_dynamicconnectors}

Not all jobs are static, so `Connector` implementations are also
responsible for monitoring the external system for any changes that
might require reconfiguration. For example, in the `JDBCSourceConnector`
example, the `Connector` might assign a set of tables to each `Task`.
When a new table is created, it must discover this so it can assign the
new table to one of the `Tasks` by updating its configuration. When it
notices a change that requires reconfiguration (or a change in the
number of `Tasks`), it notifies the framework and the framework updates
any corresponding `Tasks`.

### Developing a Simple Connector {#connect_developing}

Developing a connector only requires implementing two interfaces, the
`Connector` and `Task`. A simple example is included with the source
code for Kafka in the `file` package. This connector is meant for use in
standalone mode and has implementations of a
`SourceConnector`/`SourceTask` to read each line of a file and emit it
as a record and a `SinkConnector`/`SinkTask` that writes each record to
a file.

The rest of this section will walk through some code to demonstrate the
key steps in creating a connector, but developers should also refer to
the full example source code as many details are omitted for brevity.

#### Connector Example {#connect_connectorexample}

We\'ll cover the `SourceConnector` as a simple example. `SinkConnector`
implementations are very similar. Start by creating the class that
inherits from `SourceConnector` and add a field that will store the
configuration information to be propagated to the task(s) (the topic to
send data to, and optionally - the filename to read from and the maximum
batch size):

```java
public class FileStreamSourceConnector extends SourceConnector {
    private Map<String, String> props;
```

The easiest method to fill in is `taskClass()`, which defines the class
that should be instantiated in worker processes to actually read the
data:

```java
@Override
public Class<? extends Task> taskClass() {
    return FileStreamSourceTask.class;
}
```

We will define the `FileStreamSourceTask` class below. Next, we add some
standard lifecycle methods, `start()` and `stop()`:

```java
@Override
public void start(Map<String, String> props) {
    // Initialization logic and setting up of resources can take place in this method.
    // This connector doesn't need to do any of that, but we do log a helpful message to the user.

    this.props = props;
    AbstractConfig config = new AbstractConfig(CONFIG_DEF, props);
    String filename = config.getString(FILE_CONFIG);
    filename = (filename == null || filename.isEmpty()) ? "standard input" : config.getString(FILE_CONFIG);
    log.info("Starting file source connector reading from {}", filename);
}

@Override
public void stop() {
    // Nothing to do since no background monitoring is required.
}
```

Finally, the real core of the implementation is in `taskConfigs()`. In
this case we are only handling a single file, so even though we may be
permitted to generate more tasks as per the `maxTasks` argument, we
return a list with only one entry:

```java
@Override
public List<Map<String, String>> taskConfigs(int maxTasks) {
    // Note that the task configs could contain configs additional to or different from the connector configs if needed. For instance,
    // if different tasks have different responsibilities, or if different tasks are meant to process different subsets of the source data stream).
    ArrayList<Map<String, String>> configs = new ArrayList<>();
    // Only one input stream makes sense.
    configs.add(props);
    return configs;
}
```

Even with multiple tasks, this method implementation is usually pretty
simple. It just has to determine the number of input tasks, which may
require contacting the remote service it is pulling data from, and then
divvy them up. Because some patterns for splitting work among tasks are
so common, some utilities are provided in `ConnectorUtils` to simplify
these cases.

Note that this simple example does not include dynamic input. See the
discussion in the next section for how to trigger updates to task
configs.

#### Task Example - Source Task {#connect_taskexample}

Next we\'ll describe the implementation of the corresponding
`SourceTask`. The implementation is short, but too long to cover
completely in this guide. We\'ll use pseudo-code to describe most of the
implementation, but you can refer to the source code for the full
example.

Just as with the connector, we need to create a class inheriting from
the appropriate base `Task` class. It also has some standard lifecycle
methods:

```java
public class FileStreamSourceTask extends SourceTask {
    private String filename;
    private InputStream stream;
    private String topic;
    private int batchSize;

    @Override
    public void start(Map<String, String> props) {
        filename = props.get(FileStreamSourceConnector.FILE_CONFIG);
        stream = openOrThrowError(filename);
        topic = props.get(FileStreamSourceConnector.TOPIC_CONFIG);
        batchSize = props.get(FileStreamSourceConnector.TASK_BATCH_SIZE_CONFIG);
    }

    @Override
    public synchronized void stop() {
        stream.close();
    }
```

These are slightly simplified versions, but show that these methods
should be relatively simple and the only work they should perform is
allocating or freeing resources. There are two points to note about this
implementation. First, the `start()` method does not yet handle resuming
from a previous offset, which will be addressed in a later section.
Second, the `stop()` method is synchronized. This will be necessary
because `SourceTasks` are given a dedicated thread which they can block
indefinitely, so they need to be stopped with a call from a different
thread in the Worker.

Next, we implement the main functionality of the task, the `poll()`
method which gets events from the input system and returns a
`List<SourceRecord>`:

```java
@Override
public List<SourceRecord> poll() throws InterruptedException {
    try {
        ArrayList<SourceRecord> records = new ArrayList<>();
        while (streamValid(stream) && records.isEmpty()) {
            LineAndOffset line = readToNextLine(stream);
            if (line != null) {
                Map<String, Object> sourcePartition = Collections.singletonMap("filename", filename);
                Map<String, Object> sourceOffset = Collections.singletonMap("position", streamOffset);
                records.add(new SourceRecord(sourcePartition, sourceOffset, topic, Schema.STRING_SCHEMA, line));
                if (records.size() >= batchSize) {
                    return records;
                }
            } else {
                Thread.sleep(1);
            }
        }
        return records;
    } catch (IOException e) {
        // Underlying stream was killed, probably as a result of calling stop. Allow to return
        // null, and driving thread will handle any shutdown if necessary.
    }
    return null;
}
```

Again, we\'ve omitted some details, but we can see the important steps:
the `poll()` method is going to be called repeatedly, and for each call
it will loop trying to read records from the file. For each line it
reads, it also tracks the file offset. It uses this information to
create an output `SourceRecord` with four pieces of information: the
source partition (there is only one, the single file being read), source
offset (byte offset in the file), output topic name, and output value
(the line, and we include a schema indicating this value will always be
a string). Other variants of the `SourceRecord` constructor can also
include a specific output partition, a key, and headers.

Note that this implementation uses the normal Java `InputStream`
interface and may sleep if data is not available. This is acceptable
because Kafka Connect provides each task with a dedicated thread. While
task implementations have to conform to the basic `poll()` interface,
they have a lot of flexibility in how they are implemented. In this
case, an NIO-based implementation would be more efficient, but this
simple approach works, is quick to implement, and is compatible with
older versions of Java.

Although not used in the example, `SourceTask` also provides two APIs to
commit offsets in the source system: `commit` and `commitRecord`. The
APIs are provided for source systems which have an acknowledgement
mechanism for messages. Overriding these methods allows the source
connector to acknowledge messages in the source system, either in bulk
or individually, once they have been written to Kafka. The `commit` API
stores the offsets in the source system, up to the offsets that have
been returned by `poll`. The implementation of this API should block
until the commit is complete. The `commitRecord` API saves the offset in
the source system for each `SourceRecord` after it is written to Kafka.
As Kafka Connect will record offsets automatically, `SourceTask`s are
not required to implement them. In cases where a connector does need to
acknowledge messages in the source system, only one of the APIs is
typically required.

#### Sink Tasks {#connect_sinktasks}

The previous section described how to implement a simple `SourceTask`.
Unlike `SourceConnector` and `SinkConnector`, `SourceTask` and
`SinkTask` have very different interfaces because `SourceTask` uses a
pull interface and `SinkTask` uses a push interface. Both share the
common lifecycle methods, but the `SinkTask` interface is quite
different:

```java
public abstract class SinkTask implements Task {
    public void initialize(SinkTaskContext context) {
        this.context = context;
    }

    public abstract void put(Collection<SinkRecord> records);

    public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    }
```

The `SinkTask` documentation contains full details, but this interface
is nearly as simple as the `SourceTask`. The `put()` method should
contain most of the implementation, accepting sets of `SinkRecords`,
performing any required translation, and storing them in the destination
system. This method does not need to ensure the data has been fully
written to the destination system before returning. In fact, in many
cases internal buffering will be useful so an entire batch of records
can be sent at once, reducing the overhead of inserting events into the
downstream data store. The `SinkRecords` contain essentially the same
information as `SourceRecords`: Kafka topic, partition, offset, the
event key and value, and optional headers.

The `flush()` method is used during the offset commit process, which
allows tasks to recover from failures and resume from a safe point such
that no events will be missed. The method should push any outstanding
data to the destination system and then block until the write has been
acknowledged. The `offsets` parameter can often be ignored, but is
useful in some cases where implementations want to store offset
information in the destination store to provide exactly-once delivery.
For example, an HDFS connector could do this and use atomic move
operations to make sure the `flush()` operation atomically commits the
data and offsets to a final location in HDFS.

#### Errant Record Reporter {#connect_errantrecordreporter}

When [error reporting](#connect_errorreporting) is enabled for a
connector, the connector can use an `ErrantRecordReporter` to report
problems with individual records sent to a sink connector. The following
example shows how a connector\'s `SinkTask` subclass might obtain and
use the `ErrantRecordReporter`, safely handling a null reporter when the
DLQ is not enabled or when the connector is installed in an older
Connect runtime that doesn\'t have this reporter feature:

```java
private ErrantRecordReporter reporter;

@Override
public void start(Map<String, String> props) {
    ...
    try {
        reporter = context.errantRecordReporter(); // may be null if DLQ not enabled
    } catch (NoSuchMethodException | NoClassDefFoundError e) {
        // Will occur in Connect runtimes earlier than 2.6
        reporter = null;
    }
}

@Override
public void put(Collection<SinkRecord> records) {
    for (SinkRecord record: records) {
        try {
            // attempt to process and send record to data sink
            process(record);
        } catch(Exception e) {
            if (reporter != null) {
                // Send errant record to error reporter
                reporter.report(record, e);
            } else {
                // There's no error reporter, so fail
                throw new ConnectException("Failed on record", e);
            }
        }
    }
}
```

#### Resuming from Previous Offsets {#connect_resuming}

The `SourceTask` implementation included a stream ID (the input
filename) and offset (position in the file) with each record. The
framework uses this to commit offsets periodically so that in the case
of a failure, the task can recover and minimize the number of events
that are reprocessed and possibly duplicated (or to resume from the most
recent offset if Kafka Connect was stopped gracefully, e.g. in
standalone mode or due to a job reconfiguration). This commit process is
completely automated by the framework, but only the connector knows how
to seek back to the right position in the input stream to resume from
that location.

To correctly resume upon startup, the task can use the `SourceContext`
passed into its `initialize()` method to access the offset data. In
`initialize()`, we would add a bit more code to read the offset (if it
exists) and seek to that position:

```java
stream = new FileInputStream(filename);
Map<String, Object> offset = context.offsetStorageReader().offset(Collections.singletonMap(FILENAME_FIELD, filename));
if (offset != null) {
    Long lastRecordedOffset = (Long) offset.get("position");
    if (lastRecordedOffset != null)
        seekToOffset(stream, lastRecordedOffset);
}
```

Of course, you might need to read many keys for each of the input
streams. The `OffsetStorageReader` interface also allows you to issue
bulk reads to efficiently load all offsets, then apply them by seeking
each input stream to the appropriate position.

#### Exactly-once source connectors {#connect_exactlyoncesourceconnectors}

##### Supporting exactly-once

With the passing of
[KIP-618](https://cwiki.apache.org/confluence/display/KAFKA/KIP-618%3A+Exactly-Once+Support+for+Source+Connectors),
Kafka Connect supports exactly-once source connectors as of version
3.3.0. In order for a source connector to take advantage of this
support, it must be able to provide meaningful source offsets for each
record that it emits, and resume consumption from the external system at
the exact position corresponding to any of those offsets without
dropping or duplicating messages.

##### Defining transaction boundaries

By default, the Kafka Connect framework will create and commit a new
Kafka transaction for each batch of records that a source task returns
from its `poll` method. However, connectors can also define their own
transaction boundaries, which can be enabled by users by setting the
`transaction.boundary` property to `connector` in the config for the
connector.

If enabled, the connector\'s tasks will have access to a
`TransactionContext` from their `SourceTaskContext`, which they can use
to control when transactions are aborted and committed.

For example, to commit a transaction at least every ten records:

```java
private int recordsSent;

@Override
public void start(Map<String, String> props) {
    this.recordsSent = 0;
}

@Override
public List<SourceRecord> poll() {
    List<SourceRecord> records = fetchRecords();
    boolean shouldCommit = false;
    for (SourceRecord record : records) {
        if (++this.recordsSent >= 10) {
            shouldCommit = true;
        }
    }
    if (shouldCommit) {
        this.recordsSent = 0;
        this.context.transactionContext().commitTransaction();
    }
    return records;
}
```

Or to commit a transaction for exactly every tenth record:

```java
private int recordsSent;

@Override
public void start(Map<String, String> props) {
    this.recordsSent = 0;
}

@Override
public List<SourceRecord> poll() {
    List<SourceRecord> records = fetchRecords();
    for (SourceRecord record : records) {
        if (++this.recordsSent % 10 == 0) {
            this.context.transactionContext().commitTransaction(record);
        }
    }
    return records;
}
```

Most connectors do not need to define their own transaction boundaries.
However, it may be useful if files or objects in the source system are
broken up into multiple source records, but should be delivered
atomically. Additionally, it may be useful if it is impossible to give
each source record a unique source offset, if every record with a given
offset is delivered within a single transaction.

Note that if the user has not enabled connector-defined transaction
boundaries in the connector configuration, the `TransactionContext`
returned by `context.transactionContext()` will be `null`.

##### Validation APIs

A few additional preflight validation APIs can be implemented by source
connector developers.

Some users may require exactly-once semantics from a connector. In this
case, they may set the `exactly.once.support` property to `required` in
the configuration for the connector. When this happens, the Kafka
Connect framework will ask the connector whether it can provide
exactly-once semantics with the specified configuration. This is done by
invoking the `exactlyOnceSupport` method on the connector.

If a connector doesn\'t support exactly-once semantics, it should still
implement this method to let users know for certain that it cannot
provide exactly-once semantics:

```java
@Override
public ExactlyOnceSupport exactlyOnceSupport(Map<String, String> props) {
    // This connector cannot provide exactly-once semantics under any conditions
    return ExactlyOnceSupport.UNSUPPORTED;
}
```

Otherwise, a connector should examine the configuration, and return
`ExactlyOnceSupport.SUPPORTED` if it can provide exactly-once semantics:

```java
@Override
public ExactlyOnceSupport exactlyOnceSupport(Map<String, String> props) {
    // This connector can always provide exactly-once semantics
    return ExactlyOnceSupport.SUPPORTED;
}
```

Additionally, if the user has configured the connector to define its own
transaction boundaries, the Kafka Connect framework will ask the
connector whether it can define its own transaction boundaries with the
specified configuration, using the `canDefineTransactionBoundaries`
method:

```java
@Override
public ConnectorTransactionBoundaries canDefineTransactionBoundaries(Map<String, String> props) {
    // This connector can always define its own transaction boundaries
    return ConnectorTransactionBoundaries.SUPPORTED;
}
```

This method should only be implemented for connectors that can define
their own transaction boundaries in some cases. If a connector is never
able to define its own transaction boundaries, it does not need to
implement this method.

### Dynamic Input/Output Streams {#connect_dynamicio}

Kafka Connect is intended to define bulk data copying jobs, such as
copying an entire database rather than creating many jobs to copy each
table individually. One consequence of this design is that the set of
input or output streams for a connector can vary over time.

Source connectors need to monitor the source system for changes, e.g.
table additions/deletions in a database. When they pick up changes, they
should notify the framework via the `ConnectorContext` object that
reconfiguration is necessary. For example, in a `SourceConnector`:

```java
if (inputsChanged())
    this.context.requestTaskReconfiguration();
```

The framework will promptly request new configuration information and
update the tasks, allowing them to gracefully commit their progress
before reconfiguring them. Note that in the `SourceConnector` this
monitoring is currently left up to the connector implementation. If an
extra thread is required to perform this monitoring, the connector must
allocate it itself.

Ideally this code for monitoring changes would be isolated to the
`Connector` and tasks would not need to worry about them. However,
changes can also affect tasks, most commonly when one of their input
streams is destroyed in the input system, e.g. if a table is dropped
from a database. If the `Task` encounters the issue before the
`Connector`, which will be common if the `Connector` needs to poll for
changes, the `Task` will need to handle the subsequent error.
Thankfully, this can usually be handled simply by catching and handling
the appropriate exception.

`SinkConnectors` usually only have to handle the addition of streams,
which may translate to new entries in their outputs (e.g., a new
database table). The framework manages any changes to the Kafka input,
such as when the set of input topics changes because of a regex
subscription. `SinkTasks` should expect new input streams, which may
require creating new resources in the downstream system, such as a new
table in a database. The trickiest situation to handle in these cases
may be conflicts between multiple `SinkTasks` seeing a new input stream
for the first time and simultaneously trying to create the new resource.
`SinkConnectors`, on the other hand, will generally require no special
code for handling a dynamic set of streams.

### Connect Configuration Validation {#connect_configs}

Kafka Connect allows you to validate connector configurations before
submitting a connector to be executed and can provide feedback about
errors and recommended values. To take advantage of this, connector
developers need to provide an implementation of `config()` to expose the
configuration definition to the framework.

The following code in `FileStreamSourceConnector` defines the
configuration and exposes it to the framework.

```java
static final ConfigDef CONFIG_DEF = new ConfigDef()
    .define(FILE_CONFIG, Type.STRING, null, Importance.HIGH, "Source filename. If not specified, the standard input will be used")
    .define(TOPIC_CONFIG, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(), Importance.HIGH, "The topic to publish data to")
    .define(TASK_BATCH_SIZE_CONFIG, Type.INT, DEFAULT_TASK_BATCH_SIZE, Importance.LOW,
        "The maximum number of records the source task can read from the file each time it is polled");

public ConfigDef config() {
    return CONFIG_DEF;
}
```

`ConfigDef` class is used for specifying the set of expected
configurations. For each configuration, you can specify the name, the
type, the default value, the documentation, the group information, the
order in the group, the width of the configuration value and the name
suitable for display in the UI. Plus, you can provide special validation
logic used for single configuration validation by overriding the
`Validator` class. Moreover, as there may be dependencies between
configurations, for example, the valid values and visibility of a
configuration may change according to the values of other
configurations. To handle this, `ConfigDef` allows you to specify the
dependents of a configuration and to provide an implementation of
`Recommender` to get valid values and set visibility of a configuration
given the current configuration values.

Also, the `validate()` method in `Connector` provides a default
validation implementation which returns a list of allowed configurations
together with configuration errors and recommended values for each
configuration. However, it does not use the recommended values for
configuration validation. You may provide an override of the default
implementation for customized configuration validation, which may use
the recommended values.

### Working with Schemas {#connect_schemas}

The FileStream connectors are good examples because they are simple, but
they also have trivially structured data \-- each line is just a string.
Almost all practical connectors will need schemas with more complex data
formats.

To create more complex data, you\'ll need to work with the Kafka Connect
`data` API. Most structured records will need to interact with two
classes in addition to primitive types: `Schema` and `Struct`.

The API documentation provides a complete reference, but here is a
simple example creating a `Schema` and `Struct`:

```java
Schema schema = SchemaBuilder.struct().name(NAME)
    .field("name", Schema.STRING_SCHEMA)
    .field("age", Schema.INT_SCHEMA)
    .field("admin", SchemaBuilder.bool().defaultValue(false).build())
    .build();

Struct struct = new Struct(schema)
    .put("name", "Barbara Liskov")
    .put("age", 75);
```

If you are implementing a source connector, you\'ll need to decide when
and how to create schemas. Where possible, you should avoid recomputing
them as much as possible. For example, if your connector is guaranteed
to have a fixed schema, create it statically and reuse a single
instance.

However, many connectors will have dynamic schemas. One simple example
of this is a database connector. Considering even just a single table,
the schema will not be predefined for the entire connector (as it varies
from table to table). But it also may not be fixed for a single table
over the lifetime of the connector since the user may execute an
`ALTER TABLE` command. The connector must be able to detect these
changes and react appropriately.

Sink connectors are usually simpler because they are consuming data and
therefore do not need to create schemas. However, they should take just
as much care to validate that the schemas they receive have the expected
format. When the schema does not match \-- usually indicating the
upstream producer is generating invalid data that cannot be correctly
translated to the destination system \-- sink connectors should throw an
exception to indicate this error to the system.

### Kafka Connect Administration {#connect_administration}

Kafka Connect\'s [REST layer](#connect_rest) provides a set of APIs to
enable administration of the cluster. This includes APIs to view the
configuration of connectors and the status of their tasks, as well as to
alter their current behavior (e.g. changing configuration and restarting
tasks).

When a connector is first submitted to the cluster, a rebalance is
triggered between the Connect workers in order to distribute the load
that consists of the tasks of the new connector. This same rebalancing
procedure is also used when connectors increase or decrease the number
of tasks they require, when a connector\'s configuration is changed, or
when a worker is added or removed from the group as part of an
intentional upgrade of the Connect cluster or due to a failure.

In versions prior to 2.3.0, the Connect workers would rebalance the full
set of connectors and their tasks in the cluster as a simple way to make
sure that each worker has approximately the same amount of work. This
behavior can be still enabled by setting `connect.protocol=eager`.

Starting with 2.3.0, Kafka Connect is using by default a protocol that performs 
[incremental cooperative rebalancing](https://cwiki.apache.org/confluence/display/KAFKA/KIP-415%3A+Incremental+Cooperative+Rebalancing+in+Kafka+Connect)
that incrementally balances the connectors and tasks across the Connect
workers, affecting only tasks that are new, to be removed, or need to
move from one worker to another. Other tasks are not stopped and
restarted during the rebalance, as they would have been with the old
protocol.

If a Connect worker leaves the group, intentionally or due to a failure,
Connect waits for `scheduled.rebalance.max.delay.ms` before triggering a
rebalance. This delay defaults to five minutes (`300000ms`) to tolerate
failures or upgrades of workers without immediately redistributing the
load of a departing worker. If this worker returns within the configured
delay, it gets its previously assigned tasks in full. However, this
means that the tasks will remain unassigned until the time specified by
`scheduled.rebalance.max.delay.ms` elapses. If a worker does not return
within that time limit, Connect will reassign those tasks among the
remaining workers in the Connect cluster.

The new Connect protocol is enabled when all the workers that form the
Connect cluster are configured with `connect.protocol=compatible`, which
is also the default value when this property is missing. Therefore,
upgrading to the new Connect protocol happens automatically when all the
workers upgrade to 2.3.0. A rolling upgrade of the Connect cluster will
activate incremental cooperative rebalancing when the last worker joins
on version 2.3.0.

You can use the REST API to view the current status of a connector and
its tasks, including the ID of the worker to which each was assigned.
For example, the `GET /connectors/file-source/status` request shows the
status of a connector named `file-source`:

```json
{
    "name": "file-source",
    "connector": {
        "state": "RUNNING",
        "worker_id": "192.168.1.208:8083"
    },
    "tasks": [
        {
        "id": 0,
        "state": "RUNNING",
        "worker_id": "192.168.1.209:8083"
        }
    ]
}
```

Connectors and their tasks publish status updates to a shared topic
(configured with `status.storage.topic`) which all workers in the
cluster monitor. Because the workers consume this topic asynchronously,
there is typically a (short) delay before a state change is visible
through the status API. The following states are possible for a
connector or one of its tasks:

-   **UNASSIGNED:** The connector/task has not yet been assigned to a
    worker.
-   **RUNNING:** The connector/task is running.
-   **PAUSED:** The connector/task has been administratively paused.
-   **FAILED:** The connector/task has failed (usually by raising an
    exception, which is reported in the status output).
-   **RESTARTING:** The connector/task is either actively restarting or
    is expected to restart soon

In most cases, connector and task states will match, though they may be
different for short periods of time when changes are occurring or if
tasks have failed. For example, when a connector is first started, there
may be a noticeable delay before the connector and its tasks have all
transitioned to the RUNNING state. States will also diverge when tasks
fail since Connect does not automatically restart failed tasks. To
restart a connector/task manually, you can use the restart APIs listed
above. Note that if you try to restart a task while a rebalance is
taking place, Connect will return a 409 (Conflict) status code. You can
retry after the rebalance completes, but it might not be necessary since
rebalances effectively restart all the connectors and tasks in the
cluster.

Starting with 2.5.0, Kafka Connect uses the `status.storage.topic` to
also store information related to the topics that each connector is
using. Connect Workers use these per-connector topic status updates to
respond to requests to the REST endpoint `GET /connectors/{name}/topics`
by returning the set of topic names that a connector is using. A request
to the REST endpoint `PUT /connectors/{name}/topics/reset` resets the
set of active topics for a connector and allows a new set to be
populated, based on the connector\'s latest pattern of topic usage. Upon
connector deletion, the set of the connector\'s active topics is also
deleted. Topic tracking is enabled by default but can be disabled by
setting `topic.tracking.enable=false`. If you want to disallow requests
to reset the active topics of connectors during runtime, set the Worker
property `topic.tracking.allow.reset=false`.

It\'s sometimes useful to temporarily stop the message processing of a
connector. For example, if the remote system is undergoing maintenance,
it would be preferable for source connectors to stop polling it for new
data instead of filling logs with exception spam. For this use case,
Connect offers a pause/resume API. While a source connector is paused,
Connect will stop polling it for additional records. While a sink
connector is paused, Connect will stop pushing new messages to it. The
pause state is persistent, so even if you restart the cluster, the
connector will not begin message processing again until the task has
been resumed. Note that there may be a delay before all of a
connector\'s tasks have transitioned to the PAUSED state since it may
take time for them to finish whatever processing they were in the middle
of when being paused. Additionally, failed tasks will not transition to
the PAUSED state until they have been restarted.
