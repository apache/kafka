---
title: User Guide
description: User Guide
weight: 2
tags: ['kafka', 'docs']
aliases: 
keywords: 
type: docs
---

The [quickstart](../quickstart) provides a brief example of how to run a standalone version of Kafka Connect. This section describes how to configure, run, and manage Kafka Connect in more detail.

## Running Kafka Connect

Kafka Connect currently supports two modes of execution: standalone (single process) and distributed.

In standalone mode all work is performed in a single process. This configuration is simpler to setup and get started with and may be useful in situations where only one worker makes sense (e.g. collecting log files), but it does not benefit from some of the features of Kafka Connect such as fault tolerance. You can start a standalone process with the following command:
    
    
    > bin/connect-standalone.sh config/connect-standalone.properties [connector1.properties connector2.properties ...]

The first parameter is the configuration for the worker. This includes settings such as the Kafka connection parameters, serialization format, and how frequently to commit offsets. The provided example should work well with a local cluster running with the default configuration provided by `config/server.properties`. It will require tweaking to use with a different configuration or production deployment. All workers (both standalone and distributed) require a few configs:

  * `bootstrap.servers` \- List of Kafka servers used to bootstrap connections to Kafka
  * `key.converter` \- Converter class used to convert between Kafka Connect format and the serialized form that is written to Kafka. This controls the format of the keys in messages written to or read from Kafka, and since this is independent of connectors it allows any connector to work with any serialization format. Examples of common formats include JSON and Avro.
  * `value.converter` \- Converter class used to convert between Kafka Connect format and the serialized form that is written to Kafka. This controls the format of the values in messages written to or read from Kafka, and since this is independent of connectors it allows any connector to work with any serialization format. Examples of common formats include JSON and Avro.
  * `plugin.path` (default `empty`) - a list of paths that contain Connect plugins (connectors, converters, transformations). Before running quick starts, users must add the absolute path that contains the example FileStreamSourceConnector and FileStreamSinkConnector packaged in `connect-file-"version".jar`, because these connectors are not included by default to the `CLASSPATH` or the `plugin.path` of the Connect worker (see plugin.path property for examples).



The important configuration options specific to standalone mode are:

  * `offset.storage.file.filename` \- File to store source connector offsets



The parameters that are configured here are intended for producers and consumers used by Kafka Connect to access the configuration, offset and status topics. For configuration of the producers used by Kafka source tasks and the consumers used by Kafka sink tasks, the same parameters can be used but need to be prefixed with `producer.` and `consumer.` respectively. The only Kafka client parameter that is inherited without a prefix from the worker configuration is `bootstrap.servers`, which in most cases will be sufficient, since the same cluster is often used for all purposes. A notable exception is a secured cluster, which requires extra parameters to allow connections. These parameters will need to be set up to three times in the worker configuration, once for management access, once for Kafka sources and once for Kafka sinks.

Starting with 2.3.0, client configuration overrides can be configured individually per connector by using the prefixes `producer.override.` and `consumer.override.` for Kafka sources or Kafka sinks respectively. These overrides are included with the rest of the connector's configuration properties.

The remaining parameters are connector configuration files. You may include as many as you want, but all will execute within the same process (on different threads). You can also choose not to specify any connector configuration files on the command line, and instead use the REST API to create connectors at runtime after your standalone worker starts.

Distributed mode handles automatic balancing of work, allows you to scale up (or down) dynamically, and offers fault tolerance both in the active tasks and for configuration and offset commit data. Execution is very similar to standalone mode:
    
    
    > bin/connect-distributed.sh config/connect-distributed.properties

The difference is in the class which is started and the configuration parameters which change how the Kafka Connect process decides where to store configurations, how to assign work, and where to store offsets and task statues. In the distributed mode, Kafka Connect stores the offsets, configs and task statuses in Kafka topics. It is recommended to manually create the topics for offset, configs and statuses in order to achieve the desired the number of partitions and replication factors. If the topics are not yet created when starting Kafka Connect, the topics will be auto created with default number of partitions and replication factor, which may not be best suited for its usage.

In particular, the following configuration parameters, in addition to the common settings mentioned above, are critical to set before starting your cluster:

  * `group.id` (default `connect-cluster`) - unique name for the cluster, used in forming the Connect cluster group; note that this **must not conflict** with consumer group IDs
  * `config.storage.topic` (default `connect-configs`) - topic to use for storing connector and task configurations; note that this should be a single partition, highly replicated, compacted topic. You may need to manually create the topic to ensure the correct configuration as auto created topics may have multiple partitions or be automatically configured for deletion rather than compaction
  * `offset.storage.topic` (default `connect-offsets`) - topic to use for storing offsets; this topic should have many partitions, be replicated, and be configured for compaction
  * `status.storage.topic` (default `connect-status`) - topic to use for storing statuses; this topic can have multiple partitions, and should be replicated and configured for compaction



Note that in distributed mode the connector configurations are not passed on the command line. Instead, use the REST API described below to create, modify, and destroy connectors.

## Configuring Connectors

Connector configurations are simple key-value mappings. In both standalone and distributed mode, they are included in the JSON payload for the REST request that creates (or modifies) the connector. In standalone mode these can also be defined in a properties file and passed to the Connect process on the command line.

Most configurations are connector dependent, so they can't be outlined here. However, there are a few common options:

  * `name` \- Unique name for the connector. Attempting to register again with the same name will fail.
  * `connector.class` \- The Java class for the connector
  * `tasks.max` \- The maximum number of tasks that should be created for this connector. The connector may create fewer tasks if it cannot achieve this level of parallelism.
  * `key.converter` \- (optional) Override the default key converter set by the worker.
  * `value.converter` \- (optional) Override the default value converter set by the worker.



The `connector.class` config supports several formats: the full name or alias of the class for this connector. If the connector is org.apache.kafka.connect.file.FileStreamSinkConnector, you can either specify this full name or use FileStreamSink or FileStreamSinkConnector to make the configuration a bit shorter.

Sink connectors also have a few additional options to control their input. Each sink connector must set one of the following:

  * `topics` \- A comma-separated list of topics to use as input for this connector
  * `topics.regex` \- A Java regular expression of topics to use as input for this connector



For any other options, you should consult the documentation for the connector.

## Transformations

Connectors can be configured with transformations to make lightweight message-at-a-time modifications. They can be convenient for data massaging and event routing.

A transformation chain can be specified in the connector configuration.

  * `transforms` \- List of aliases for the transformation, specifying the order in which the transformations will be applied.
  * `transforms.$alias.type` \- Fully qualified class name for the transformation.
  * `transforms.$alias.$transformationSpecificConfig` Configuration properties for the transformation



For example, lets take the built-in file source connector and use a transformation to add a static field.

Throughout the example we'll use schemaless JSON data format. To use schemaless format, we changed the following two lines in `connect-standalone.properties` from true to false:
    
    
    key.converter.schemas.enable
    value.converter.schemas.enable

The file source connector reads each line as a String. We will wrap each line in a Map and then add a second field to identify the origin of the event. To do this, we use two transformations:

  * **HoistField** to place the input line inside a Map
  * **InsertField** to add the static field. In this example we'll indicate that the record came from a file connector



After adding the transformations, `connect-file-source.properties` file looks as following:
    
    
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

All the lines starting with `transforms` were added for the transformations. You can see the two transformations we created: "InsertSource" and "MakeMap" are aliases that we chose to give the transformations. The transformation types are based on the list of built-in transformations you can see below. Each transformation type has additional configuration: HoistField requires a configuration called "field", which is the name of the field in the map that will include the original String from the file. InsertField transformation lets us specify the field name and the value that we are adding.

When we ran the file source connector on my sample file without the transformations, and then read them using `kafka-console-consumer.sh`, the results were:
    
    
    "foo"
    "bar"
    "hello world"

We then create a new file connector, this time after adding the transformations to the configuration file. This time, the results will be:
    
    
    {"line":"foo","data_source":"test-file-source"}
    {"line":"bar","data_source":"test-file-source"}
    {"line":"hello world","data_source":"test-file-source"}

You can see that the lines we've read are now part of a JSON map, and there is an extra field with the static value we specified. This is just one example of what you can do with transformations.

### Included transformations

Several widely-applicable data and routing transformations are included with Kafka Connect:

  * InsertField - Add a field using either static data or record metadata
  * ReplaceField - Filter or rename fields
  * MaskField - Replace field with valid null value for the type (0, empty string, etc) or custom replacement (non-empty string or numeric value only)
  * ValueToKey - Replace the record key with a new key formed from a subset of fields in the record value
  * HoistField - Wrap the entire event as a single field inside a Struct or a Map
  * ExtractField - Extract a specific field from Struct and Map and include only this field in results
  * SetSchemaMetadata - modify the schema name or version
  * TimestampRouter - Modify the topic of a record based on original topic and timestamp. Useful when using a sink that needs to write to different tables or indexes based on timestamps
  * RegexRouter - modify the topic of a record based on original topic, replacement string and a regular expression
  * Filter - Removes messages from all further processing. This is used with a predicate to selectively filter certain messages.
  * InsertHeader - Add a header using static data
  * HeadersFrom - Copy or move fields in the key or value to the record headers
  * DropHeaders - Remove headers by name



Details on how to configure each transformation are listed below:

{{< include-html file="/static/35/generated/connect_transforms.html" >}} 

### Predicates

Transformations can be configured with predicates so that the transformation is applied only to messages which satisfy some condition. In particular, when combined with the **Filter** transformation predicates can be used to selectively filter out certain messages.

Predicates are specified in the connector configuration.

  * `predicates` \- Set of aliases for the predicates to be applied to some of the transformations.
  * `predicates.$alias.type` \- Fully qualified class name for the predicate.
  * `predicates.$alias.$predicateSpecificConfig` \- Configuration properties for the predicate.



All transformations have the implicit config properties `predicate` and `negate`. A predicular predicate is associated with a transformation by setting the transformation's `predicate` config to the predicate's alias. The predicate's value can be reversed using the `negate` configuration property.

For example, suppose you have a source connector which produces messages to many different topics and you want to:

  * filter out the messages in the 'foo' topic entirely
  * apply the ExtractField transformation with the field name 'other_field' to records in all topics _except_ the topic 'bar'



To do this we need first to filter out the records destined for the topic 'foo'. The Filter transformation removes records from further processing, and can use the TopicNameMatches predicate to apply the transformation only to records in topics which match a certain regular expression. TopicNameMatches's only configuration property is `pattern` which is a Java regular expression for matching against the topic name. The configuration would look like this:
    
    
    transforms=Filter
    transforms.Filter.type=org.apache.kafka.connect.transforms.Filter
    transforms.Filter.predicate=IsFoo
    
    predicates=IsFoo
    predicates.IsFoo.type=org.apache.kafka.connect.transforms.predicates.TopicNameMatches
    predicates.IsFoo.pattern=foo

Next we need to apply ExtractField only when the topic name of the record is not 'bar'. We can't just use TopicNameMatches directly, because that would apply the transformation to matching topic names, not topic names which do _not_ match. The transformation's implicit `negate` config properties allows us to invert the set of records which a predicate matches. Adding the configuration for this to the previous example we arrive at:
    
    
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

Kafka Connect includes the following predicates:

  * `TopicNameMatches` \- matches records in a topic with a name matching a particular Java regular expression.
  * `HasHeaderKey` \- matches records which have a header with the given key.
  * `RecordIsTombstone` \- matches tombstone records, that is records with a null value.



Details on how to configure each predicate are listed below:

{{< include-html file="/static/35/generated/connect_predicates.html" >}} 

## REST API

Since Kafka Connect is intended to be run as a service, it also provides a REST API for managing connectors. This REST API is available in both standalone and distributed mode. The REST API server can be configured using the `listeners` configuration option. This field should contain a list of listeners in the following format: `protocol://host:port,protocol2://host2:port2`. Currently supported protocols are `http` and `https`. For example:
    
    
    listeners=http://localhost:8080,https://localhost:8443

By default, if no `listeners` are specified, the REST server runs on port 8083 using the HTTP protocol. When using HTTPS, the configuration has to include the SSL configuration. By default, it will use the `ssl.*` settings. In case it is needed to use different configuration for the REST API than for connecting to Kafka brokers, the fields can be prefixed with `listeners.https`. When using the prefix, only the prefixed options will be used and the `ssl.*` options without the prefix will be ignored. Following fields can be used to configure HTTPS for the REST API:

  * `ssl.keystore.location`
  * `ssl.keystore.password`
  * `ssl.keystore.type`
  * `ssl.key.password`
  * `ssl.truststore.location`
  * `ssl.truststore.password`
  * `ssl.truststore.type`
  * `ssl.enabled.protocols`
  * `ssl.provider`
  * `ssl.protocol`
  * `ssl.cipher.suites`
  * `ssl.keymanager.algorithm`
  * `ssl.secure.random.implementation`
  * `ssl.trustmanager.algorithm`
  * `ssl.endpoint.identification.algorithm`
  * `ssl.client.auth`



The REST API is used not only by users to monitor / manage Kafka Connect. In distributed mode, it is also used for the Kafka Connect cross-cluster communication. Some requests received on the follower nodes REST API will be forwarded to the leader node REST API. In case the URI under which is given host reachable is different from the URI which it listens on, the configuration options `rest.advertised.host.name`, `rest.advertised.port` and `rest.advertised.listener` can be used to change the URI which will be used by the follower nodes to connect with the leader. When using both HTTP and HTTPS listeners, the `rest.advertised.listener` option can be also used to define which listener will be used for the cross-cluster communication. When using HTTPS for communication between nodes, the same `ssl.*` or `listeners.https` options will be used to configure the HTTPS client.

The following are the currently supported REST API endpoints:

  * `GET /connectors` \- return a list of active connectors
  * `POST /connectors` \- create a new connector; the request body should be a JSON object containing a string `name` field and an object `config` field with the connector configuration parameters
  * `GET /connectors/{name}` \- get information about a specific connector
  * `GET /connectors/{name}/config` \- get the configuration parameters for a specific connector
  * `PUT /connectors/{name}/config` \- update the configuration parameters for a specific connector
  * `GET /connectors/{name}/status` \- get current status of the connector, including if it is running, failed, paused, etc., which worker it is assigned to, error information if it has failed, and the state of all its tasks
  * `GET /connectors/{name}/tasks` \- get a list of tasks currently running for a connector
  * `GET /connectors/{name}/tasks/{taskid}/status` \- get current status of the task, including if it is running, failed, paused, etc., which worker it is assigned to, and error information if it has failed
  * `PUT /connectors/{name}/pause` \- pause the connector and its tasks, which stops message processing until the connector is resumed. Any resources claimed by its tasks are left allocated, which allows the connector to begin processing data quickly once it is resumed.
  * `PUT /connectors/{name}/stop` \- stop the connector and shut down its tasks, deallocating any resources claimed by its tasks. This is more efficient from a resource usage standpoint than pausing the connector, but can cause it to take longer to begin processing data once resumed.
  * `PUT /connectors/{name}/resume` \- resume a paused or stopped connector (or do nothing if the connector is not paused or stopped)
  * `POST /connectors/{name}/restart?includeTasks=<true|false>&onlyFailed=<true|false>` \- restart a connector and its tasks instances. 
    * the "includeTasks" parameter specifies whether to restart the connector instance and task instances ("includeTasks=true") or just the connector instance ("includeTasks=false"), with the default ("false") preserving the same behavior as earlier versions.
    * the "onlyFailed" parameter specifies whether to restart just the instances with a FAILED status ("onlyFailed=true") or all instances ("onlyFailed=false"), with the default ("false") preserving the same behavior as earlier versions.
  * `POST /connectors/{name}/tasks/{taskId}/restart` \- restart an individual task (typically because it has failed)
  * `DELETE /connectors/{name}` \- delete a connector, halting all tasks and deleting its configuration
  * `GET /connectors/{name}/topics` \- get the set of topics that a specific connector is using since the connector was created or since a request to reset its set of active topics was issued
  * `PUT /connectors/{name}/topics/reset` \- send a request to empty the set of active topics of a connector
  * `GET /connectors/{name}/offsets` \- get the current offsets for a connector (see [KIP-875](https://cwiki.apache.org/confluence/display/KAFKA/KIP-875%3A+First-class+offsets+support+in+Kafka+Connect) for more details)



Kafka Connect also provides a REST API for getting information about connector plugins:

  * `GET /connector-plugins`\- return a list of connector plugins installed in the Kafka Connect cluster. Note that the API only checks for connectors on the worker that handles the request, which means you may see inconsistent results, especially during a rolling upgrade if you add new connector jars
  * `PUT /connector-plugins/{connector-type}/config/validate` \- validate the provided configuration values against the configuration definition. This API performs per config validation, returns suggested values and error messages during validation.



The following is a supported REST request at the top-level (root) endpoint:

  * `GET /`\- return basic information about the Kafka Connect cluster such as the version of the Connect worker that serves the REST request (including git commit ID of the source code) and the Kafka cluster ID that is connected to. 


For the complete specification of the REST API, see the [OpenAPI documentation](/35/generated/connect_rest.yaml)

## Error Reporting in Connect

Kafka Connect provides error reporting to handle errors encountered along various stages of processing. By default, any error encountered during conversion or within transformations will cause the connector to fail. Each connector configuration can also enable tolerating such errors by skipping them, optionally writing each error and the details of the failed operation and problematic record (with various levels of detail) to the Connect application log. These mechanisms also capture errors when a sink connector is processing the messages consumed from its Kafka topics, and all of the errors can be written to a configurable "dead letter queue" (DLQ) Kafka topic.

To report errors within a connector's converter, transforms, or within the sink connector itself to the log, set `errors.log.enable=true` in the connector configuration to log details of each error and problem record's topic, partition, and offset. For additional debugging purposes, set `errors.log.include.messages=true` to also log the problem record key, value, and headers to the log (note this may log sensitive information).

To report errors within a connector's converter, transforms, or within the sink connector itself to a dead letter queue topic, set `errors.deadletterqueue.topic.name`, and optionally `errors.deadletterqueue.context.headers.enable=true`.

By default connectors exhibit "fail fast" behavior immediately upon an error or exception. This is equivalent to adding the following configuration properties with their defaults to a connector configuration:
    
    
    # disable retries on failure
    errors.retry.timeout=0
    
    # do not log the error and their contexts
    errors.log.enable=false
    
    # do not record errors in a dead letter queue topic
    errors.deadletterqueue.topic.name=
    
    # Fail on first error
    errors.tolerance=none

These and other related connector configuration properties can be changed to provide different behavior. For example, the following configuration properties can be added to a connector configuration to setup error handling with multiple retries, logging to the application logs and the `my-connector-errors` Kafka topic, and tolerating all errors by reporting them rather than failing the connector task:
    
    
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

## Exactly-once support

Kafka Connect is capable of providing exactly-once semantics for sink connectors (as of version 0.11.0) and source connectors (as of version 3.3.0). Please note that **support for exactly-once semantics is highly dependent on the type of connector you run.** Even if you set all the correct worker properties in the configuration for each node in a cluster, if a connector is not designed to, or cannot take advantage of the capabilities of the Kafka Connect framework, exactly-once may not be possible.

### Sink connectors

If a sink connector supports exactly-once semantics, to enable exactly-once at the Connect worker level, you must ensure its consumer group is configured to ignore records in aborted transactions. You can do this by setting the worker property `consumer.isolation.level` to `read_committed` or, if running a version of Kafka Connect that supports it, using a connector client config override policy that allows the `consumer.override.isolation.level` property to be set to `read_committed` in individual connector configs. There are no additional ACL requirements.

### Source connectors

If a source connector supports exactly-once semantics, you must configure your Connect cluster to enable framework-level support for exactly-once source connectors. Additional ACLs may be necessary if running against a secured Kafka cluster. Note that exactly-once support for source connectors is currently only available in distributed mode; standalone Connect workers cannot provide exactly-once semantics.

#### Worker configuration

For new Connect clusters, set the `exactly.once.source.support` property to `enabled` in the worker config for each node in the cluster. For existing clusters, two rolling upgrades are necessary. During the first upgrade, the `exactly.once.source.support` property should be set to `preparing`, and during the second, it should be set to `enabled`.

#### ACL requirements

With exactly-once source support enabled, the principal for each Connect worker will require the following ACLs:

Operation | Resource Type | Resource Name | Note  
---|---|---|---  
Write | TransactionalId | `connect-cluster-${groupId}`, where `${groupId}` is the `group.id` of the cluster |   
Describe | TransactionalId | `connect-cluster-${groupId}`, where `${groupId}` is the `group.id` of the cluster |   
IdempotentWrite | Cluster | ID of the Kafka cluster that hosts the worker's config topic | The IdempotentWrite ACL has been deprecated as of 2.8 and will only be necessary for Connect clusters running on pre-2.8 Kafka clusters  
  
And the principal for each individual connector will require the following ACLs:

Operation | Resource Type | Resource Name | Note  
---|---|---|---  
Write | TransactionalId | `${groupId}-${connector}-${taskId}`, for each task that the connector will create, where `${groupId}` is the `group.id` of the Connect cluster, `${connector}` is the name of the connector, and `${taskId}` is the ID of the task (starting from zero) | A wildcard prefix of `${groupId}-${connector}*` can be used for convenience if there is no risk of conflict with other transactional IDs or if conflicts are acceptable to the user.  
Describe | TransactionalId | `${groupId}-${connector}-${taskId}`, for each task that the connector will create, where `${groupId}` is the `group.id` of the Connect cluster, `${connector}` is the name of the connector, and `${taskId}` is the ID of the task (starting from zero) | A wildcard prefix of `${groupId}-${connector}*` can be used for convenience if there is no risk of conflict with other transactional IDs or if conflicts are acceptable to the user.  
Write | Topic | Offsets topic used by the connector, which is either the value of the `offsets.storage.topic` property in the connector’s configuration if provided, or the value of the `offsets.storage.topic` property in the worker’s configuration if not. |   
Read | Topic | Offsets topic used by the connector, which is either the value of the `offsets.storage.topic` property in the connector’s configuration if provided, or the value of the `offsets.storage.topic` property in the worker’s configuration if not. |   
Describe | Topic | Offsets topic used by the connector, which is either the value of the `offsets.storage.topic` property in the connector’s configuration if provided, or the value of the `offsets.storage.topic` property in the worker’s configuration if not. |   
Create | Topic | Offsets topic used by the connector, which is either the value of the `offsets.storage.topic` property in the connector’s configuration if provided, or the value of the `offsets.storage.topic` property in the worker’s configuration if not. | Only necessary if the offsets topic for the connector does not exist yet  
IdempotentWrite | Cluster | ID of the Kafka cluster that the source connector writes to | The IdempotentWrite ACL has been deprecated as of 2.8 and will only be necessary for Connect clusters running on pre-2.8 Kafka clusters  
  