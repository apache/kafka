---
title: User Guide
description: User Guide
weight: 2
tags: ['kafka', 'docs']
aliases: 
keywords: 
type: docs
---

<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->


The [quickstart](../getting-started/quickstart) provides a brief example of how to run a standalone version of Kafka Connect. This section describes how to configure, run, and manage Kafka Connect in more detail.

## Running Kafka Connect

Kafka Connect currently supports two modes of execution: standalone (single process) and distributed.

In standalone mode all work is performed in a single process. This configuration is simpler to setup and get started with and may be useful in situations where only one worker makes sense (e.g. collecting log files), but it does not benefit from some of the features of Kafka Connect such as fault tolerance. You can start a standalone process with the following command:
    
    
    $ bin/connect-standalone.sh config/connect-standalone.properties [connector1.properties connector2.json …]

The first parameter is the configuration for the worker. This includes settings such as the Kafka connection parameters, serialization format, and how frequently to commit offsets. The provided example should work well with a local cluster running with the default configuration provided by `config/server.properties`. It will require tweaking to use with a different configuration or production deployment. All workers (both standalone and distributed) require a few configs:

  * `bootstrap.servers` \- List of Kafka servers used to bootstrap connections to Kafka
  * `key.converter` \- Converter class used to convert between Kafka Connect format and the serialized form that is written to Kafka. This controls the format of the keys in messages written to or read from Kafka, and since this is independent of connectors it allows any connector to work with any serialization format. Examples of common formats include JSON and Avro.
  * `value.converter` \- Converter class used to convert between Kafka Connect format and the serialized form that is written to Kafka. This controls the format of the values in messages written to or read from Kafka, and since this is independent of connectors it allows any connector to work with any serialization format. Examples of common formats include JSON and Avro.
  * `plugin.path` (default `empty`) - a list of paths that contain Connect plugins (connectors, converters, transformations). Before running quick starts, users must add the absolute path that contains the example FileStreamSourceConnector and FileStreamSinkConnector packaged in `connect-file-"version".jar`, because these connectors are not included by default to the `CLASSPATH` or the `plugin.path` of the Connect worker (see plugin.path property for examples).



The important configuration options specific to standalone mode are:

  * `offset.storage.file.filename` \- File to store source connector offsets



The parameters that are configured here are intended for producers and consumers used by Kafka Connect to access the configuration, offset and status topics. For configuration of the producers used by Kafka source tasks and the consumers used by Kafka sink tasks, the same parameters can be used but need to be prefixed with `producer.` and `consumer.` respectively. The only Kafka client parameter that is inherited without a prefix from the worker configuration is `bootstrap.servers`, which in most cases will be sufficient, since the same cluster is often used for all purposes. A notable exception is a secured cluster, which requires extra parameters to allow connections. These parameters will need to be set up to three times in the worker configuration, once for management access, once for Kafka sources and once for Kafka sinks.

Starting with 2.3.0, client configuration overrides can be configured individually per connector by using the prefixes `producer.override.` and `consumer.override.` for Kafka sources or Kafka sinks respectively. These overrides are included with the rest of the connector's configuration properties.

The remaining parameters are connector configuration files. Each file may either be a Java Properties file or a JSON file containing an object with the same structure as the request body of either the `POST /connectors` endpoint or the `PUT /connectors/{name}/config` endpoint (see the [OpenAPI documentation](/39/generated/connect_rest.yaml)). You may include as many as you want, but all will execute within the same process (on different threads). You can also choose not to specify any connector configuration files on the command line, and instead use the REST API to create connectors at runtime after your standalone worker starts.

Distributed mode handles automatic balancing of work, allows you to scale up (or down) dynamically, and offers fault tolerance both in the active tasks and for configuration and offset commit data. Execution is very similar to standalone mode:
    
    
    $ bin/connect-distributed.sh config/connect-distributed.properties

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

  * Cast \- Cast fields or the entire key or value to a specific type
  * DropHeaders \- Remove headers by name
  * ExtractField \- Extract a specific field from Struct and Map and include only this field in results
  * Filter \- Removes messages from all further processing. This is used with a predicate to selectively filter certain messages
  * Flatten \- Flatten a nested data structure
  * HeaderFrom \- Copy or move fields in the key or value to the record headers
  * HoistField \- Wrap the entire event as a single field inside a Struct or a Map
  * InsertField \- Add a field using either static data or record metadata
  * InsertHeader \- Add a header using static data
  * MaskField \- Replace field with valid null value for the type (0, empty string, etc) or custom replacement (non-empty string or numeric value only)
  * RegexRouter \- modify the topic of a record based on original topic, replacement string and a regular expression
  * ReplaceField \- Filter or rename fields
  * SetSchemaMetadata \- modify the schema name or version
  * TimestampConverter \- Convert timestamps between different formats
  * TimestampRouter \- Modify the topic of a record based on original topic and timestamp. Useful when using a sink that needs to write to different tables or indexes based on timestamps
  * ValueToKey \- Replace the record key with a new key formed from a subset of fields in the record value



Details on how to configure each transformation are listed below:

{{< include-html file="/static/39/generated/connect_transforms.html" >}} 

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

{{< include-html file="/static/39/generated/connect_predicates.html" >}} 

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
  * `POST /connectors` \- create a new connector; the request body should be a JSON object containing a string `name` field and an object `config` field with the connector configuration parameters. The JSON object may also optionally contain a string `initial_state` field which can take the following values - `STOPPED`, `PAUSED` or `RUNNING` (the default value)
  * `GET /connectors/{name}` \- get information about a specific connector
  * `GET /connectors/{name}/config` \- get the configuration parameters for a specific connector
  * `PUT /connectors/{name}/config` \- update the configuration parameters for a specific connector
  * `PATCH /connectors/{name}/config` \- patch the configuration parameters for a specific connector, where `null` values in the JSON body indicates removing of the key from the final configuration
  * `GET /connectors/{name}/status` \- get current status of the connector, including if it is running, failed, paused, etc., which worker it is assigned to, error information if it has failed, and the state of all its tasks
  * `GET /connectors/{name}/tasks` \- get a list of tasks currently running for a connector along with their configurations
  * `GET /connectors/{name}/tasks-config` \- get the configuration of all tasks for a specific connector. This endpoint is deprecated and will be removed in the next major release. Please use the `GET /connectors/{name}/tasks` endpoint instead. Note that the response structures of the two endpoints differ slightly, please refer to the [OpenAPI documentation](/39/generated/connect_rest.yaml) for more details
  * `GET /connectors/{name}/tasks/{taskid}/status` \- get current status of the task, including if it is running, failed, paused, etc., which worker it is assigned to, and error information if it has failed
  * `PUT /connectors/{name}/pause` \- pause the connector and its tasks, which stops message processing until the connector is resumed. Any resources claimed by its tasks are left allocated, which allows the connector to begin processing data quickly once it is resumed.
  * `PUT /connectors/{name}/stop` \- stop the connector and shut down its tasks, deallocating any resources claimed by its tasks. This is more efficient from a resource usage standpoint than pausing the connector, but can cause it to take longer to begin processing data once resumed. Note that the offsets for a connector can be only modified via the offsets management endpoints if it is in the stopped state
  * `PUT /connectors/{name}/resume` \- resume a paused or stopped connector (or do nothing if the connector is not paused or stopped)
  * `POST /connectors/{name}/restart?includeTasks=<true|false>&onlyFailed=<true|false>` \- restart a connector and its tasks instances. 
    * the "includeTasks" parameter specifies whether to restart the connector instance and task instances ("includeTasks=true") or just the connector instance ("includeTasks=false"), with the default ("false") preserving the same behavior as earlier versions.
    * the "onlyFailed" parameter specifies whether to restart just the instances with a FAILED status ("onlyFailed=true") or all instances ("onlyFailed=false"), with the default ("false") preserving the same behavior as earlier versions.
  * `POST /connectors/{name}/tasks/{taskId}/restart` \- restart an individual task (typically because it has failed)
  * `DELETE /connectors/{name}` \- delete a connector, halting all tasks and deleting its configuration
  * `GET /connectors/{name}/topics` \- get the set of topics that a specific connector is using since the connector was created or since a request to reset its set of active topics was issued
  * `PUT /connectors/{name}/topics/reset` \- send a request to empty the set of active topics of a connector
  * Offsets management endpoints (see [KIP-875](https://cwiki.apache.org/confluence/display/KAFKA/KIP-875%3A+First-class+offsets+support+in+Kafka+Connect) for more details): 
    * `GET /connectors/{name}/offsets` \- get the current offsets for a connector
    * `DELETE /connectors/{name}/offsets` \- reset the offsets for a connector. The connector must exist and must be in the stopped state (see `PUT /connectors/{name}/stop`)
    * `PATCH /connectors/{name}/offsets` \- alter the offsets for a connector. The connector must exist and must be in the stopped state (see `PUT /connectors/{name}/stop`). The request body should be a JSON object containing a JSON array `offsets` field, similar to the response body of the `GET /connectors/{name}/offsets` endpoint. An example request body for the `FileStreamSourceConnector`: 
          
          {
            "offsets": [
              {
                "partition": {
                  "filename": "test.txt"
                },
                "offset": {
                  "position": 30
                }
              }
            ]
          }

An example request body for the `FileStreamSinkConnector`: 
          
          {
            "offsets": [
              {
                "partition": {
                  "kafka_topic": "test",
                  "kafka_partition": 0
                },
                "offset": {
                  "kafka_offset": 5
                }
              },
              {
                "partition": {
                  "kafka_topic": "test",
                  "kafka_partition": 1
                },
                "offset": null
              }
            ]
          }

The "offset" field may be null to reset the offset for a specific partition (applicable to both source and sink connectors). Note that the request body format depends on the connector implementation in the case of source connectors, whereas there is a common format across all sink connectors.



Kafka Connect also provides a REST API for getting information about connector plugins:

  * `GET /connector-plugins`\- return a list of connector plugins installed in the Kafka Connect cluster. Note that the API only checks for connectors on the worker that handles the request, which means you may see inconsistent results, especially during a rolling upgrade if you add new connector jars
  * `GET /connector-plugins/{plugin-type}/config` \- get the configuration definition for the specified plugin.
  * `PUT /connector-plugins/{connector-type}/config/validate` \- validate the provided configuration values against the configuration definition. This API performs per config validation, returns suggested values and error messages during validation.



The following is a supported REST request at the top-level (root) endpoint:

  * `GET /`\- return basic information about the Kafka Connect cluster such as the version of the Connect worker that serves the REST request (including git commit ID of the source code) and the Kafka cluster ID that is connected to. 


The `admin.listeners` configuration can be used to configure admin REST APIs on Kafka Connect's REST API server. Similar to the `listeners` configuration, this field should contain a list of listeners in the following format: `protocol://host:port,protocol2://host2:port2`. Currently supported protocols are `http` and `https`. For example:
    
    
    admin.listeners=http://localhost:8080,https://localhost:8443

By default, if `admin.listeners` is not configured, the admin REST APIs will be available on the regular listeners.

The following are the currently supported admin REST API endpoints:

  * `GET /admin/loggers` \- list the current loggers that have their levels explicitly set and their log levels
  * `GET /admin/loggers/{name}` \- get the log level for the specified logger
  * `PUT /admin/loggers/{name}` \- set the log level for the specified logger



See [KIP-495](https://cwiki.apache.org/confluence/display/KAFKA/KIP-495%3A+Dynamically+Adjust+Log+Levels+in+Connect) for more details about the admin logger REST APIs.

For the complete specification of the Kafka Connect REST API, see the [OpenAPI documentation](/39/generated/connect_rest.yaml)

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

With exactly-once source support enabled, or with `exactly.once.source.support` set to `preparing`, the principal for each Connect worker will require the following ACLs:  
  
<table>  
<tr>  
<th>

Operation
</th>  
<th>

Resource Type
</th>  
<th>

Resource Name
</th>  
<th>

Note
</th> </tr>  
<tr>  
<td>

Write
</td>  
<td>

TransactionalId
</td>  
<td>

`connect-cluster-${groupId}`, where `${groupId}` is the `group.id` of the cluster
</td>  
<td>


</td> </tr>  
<tr>  
<td>

Describe
</td>  
<td>

TransactionalId
</td>  
<td>

`connect-cluster-${groupId}`, where `${groupId}` is the `group.id` of the cluster
</td>  
<td>


</td> </tr>  
<tr>  
<td>

IdempotentWrite
</td>  
<td>

Cluster
</td>  
<td>

ID of the Kafka cluster that hosts the worker's config topic
</td>  
<td>

The IdempotentWrite ACL has been deprecated as of 2.8 and will only be necessary for Connect clusters running on pre-2.8 Kafka clusters
</td> </tr> </table>

And with exactly-once source enabled (but not if `exactly.once.source.support` is set to `preparing`), the principal for each individual connector will require the following ACLs:  
  
<table>  
<tr>  
<th>

Operation
</th>  
<th>

Resource Type
</th>  
<th>

Resource Name
</th>  
<th>

Note
</th> </tr>  
<tr>  
<td>

Write
</td>  
<td>

TransactionalId
</td>  
<td>

`${groupId}-${connector}-${taskId}`, for each task that the connector will create, where `${groupId}` is the `group.id` of the Connect cluster, `${connector}` is the name of the connector, and `${taskId}` is the ID of the task (starting from zero)
</td>  
<td>

A wildcard prefix of `${groupId}-${connector}*` can be used for convenience if there is no risk of conflict with other transactional IDs or if conflicts are acceptable to the user.
</td> </tr>  
<tr>  
<td>

Describe
</td>  
<td>

TransactionalId
</td>  
<td>

`${groupId}-${connector}-${taskId}`, for each task that the connector will create, where `${groupId}` is the `group.id` of the Connect cluster, `${connector}` is the name of the connector, and `${taskId}` is the ID of the task (starting from zero)
</td>  
<td>

A wildcard prefix of `${groupId}-${connector}*` can be used for convenience if there is no risk of conflict with other transactional IDs or if conflicts are acceptable to the user.
</td> </tr>  
<tr>  
<td>

Write
</td>  
<td>

Topic
</td>  
<td>

Offsets topic used by the connector, which is either the value of the `offsets.storage.topic` property in the connector’s configuration if provided, or the value of the `offsets.storage.topic` property in the worker’s configuration if not.
</td>  
<td>


</td> </tr>  
<tr>  
<td>

Read
</td>  
<td>

Topic
</td>  
<td>

Offsets topic used by the connector, which is either the value of the `offsets.storage.topic` property in the connector’s configuration if provided, or the value of the `offsets.storage.topic` property in the worker’s configuration if not.
</td>  
<td>


</td> </tr>  
<tr>  
<td>

Describe
</td>  
<td>

Topic
</td>  
<td>

Offsets topic used by the connector, which is either the value of the `offsets.storage.topic` property in the connector’s configuration if provided, or the value of the `offsets.storage.topic` property in the worker’s configuration if not.
</td>  
<td>


</td> </tr>  
<tr>  
<td>

Create
</td>  
<td>

Topic
</td>  
<td>

Offsets topic used by the connector, which is either the value of the `offsets.storage.topic` property in the connector’s configuration if provided, or the value of the `offsets.storage.topic` property in the worker’s configuration if not.
</td>  
<td>

Only necessary if the offsets topic for the connector does not exist yet
</td> </tr>  
<tr>  
<td>

IdempotentWrite
</td>  
<td>

Cluster
</td>  
<td>

ID of the Kafka cluster that the source connector writes to
</td>  
<td>

The IdempotentWrite ACL has been deprecated as of 2.8 and will only be necessary for Connect clusters running on pre-2.8 Kafka clusters
</td> </tr> </table>

## Plugin Discovery

Plugin discovery is the name for the strategy which the Connect worker uses to find plugin classes and make them accessible to configure and run in connectors. This is controlled by the plugin.discovery worker configuration, and has a significant impact on worker startup time. `service_load` is the fastest strategy, but care should be taken to verify that plugins are compatible before setting this configuration to `service_load`.

Prior to version 3.6, this strategy was not configurable, and behaved like the `only_scan` mode which is compatible with all plugins. For version 3.6 and later, this mode defaults to `hybrid_warn` which is also compatible with all plugins, but logs a warning for plugins which are incompatible with `service_load`. The `hybrid_fail` strategy stops the worker with an error if a plugin incompatible with `service_load` is detected, asserting that all plugins are compatible. Finally, the `service_load` strategy disables the slow legacy scanning mechanism used in all other modes, and instead uses the faster `ServiceLoader` mechanism. Plugins which are incompatible with that mechanism may be unusable.

### Verifying Plugin Compatibility

To verify if all of your plugins are compatible with `service_load`, first ensure that you are using version 3.6 or later of Kafka Connect. You can then perform one of the following checks:

  * Start your worker with the default `hybrid_warn`strategy, and WARN logs enabled for the `org.apache.kafka.connect` package. At least one WARN log message mentioning the `plugin.discovery` configuration should be printed. This log message will explicitly say that all plugins are compatible, or list the incompatible plugins.
  * Start your worker in a test environment with `hybrid_fail`. If all plugins are compatible, startup will succeed. If at least one plugin is not compatible the worker will fail to start up, and all incompatible plugins will be listed in the exception.



If the verification step succeeds, then your current set of installed plugins is compatible, and it should be safe to change the `plugin.discovery` configuration to `service_load`. If the verification fails, you cannot use `service_load` strategy and should take note of the list of incompatible plugins. All plugins must be addressed before using the `service_load` strategy. It is recommended to perform this verification after installing or changing plugin versions, and the verification can be done automatically in a Continuous Integration environment.

### Operators: Artifact Migration

As an operator of Connect, if you discover incompatible plugins, there are multiple ways to resolve the incompatibility. They are listed below from most to least preferable.

  1. Check the latest release from your plugin provider, and if it is compatible, upgrade.
  2. Contact your plugin provider and request that they migrate the plugin to be compatible, following the source migration instructions, and then upgrade to the compatible version.
  3. Migrate the plugin artifacts yourself using the included migration script.



The migration script is located in `bin/connect-plugin-path.sh` and `bin\windows\connect-plugin-path.bat` of your Kafka installation. The script can migrate incompatible plugin artifacts already installed on your Connect worker's `plugin.path` by adding or modifying JAR or resource files. This is not suitable for environments using code-signing, as this can change artifacts such that they will fail signature verification. View the built-in help with `--help`.

To perform a migration, first use the `list` subcommand to get an overview of the plugins available to the script. You must tell the script where to find plugins, which can be done with the repeatable `--worker-config`, `--plugin-path`, and `--plugin-location` arguments. The script will ignore plugins on the classpath, so any custom plugins on your classpath should be moved to the plugin path in order to be used with this migration script, or migrated manually. Be sure to compare the output of `list` with the worker startup warning or error message to ensure that all of your affected plugins are found by the script.

Once you see that all incompatible plugins are included in the listing, you can proceed to dry-run the migration with `sync-manifests --dry-run`. This will perform all parts of the migration, except for writing the results of the migration to disk. Note that the `sync-manifests` command requires all specified paths to be writable, and may alter the contents of the directories. Make a backup of your plugins in the specified paths, or copy them to a writable directory.

Ensure that you have a backup of your plugins and the dry-run succeeds before removing the `--dry-run` flag and actually running the migration. If the migration fails without the `--dry-run` flag, then the partially migrated artifacts should be discarded. The migration is idempotent, so running it multiple times and on already-migrated plugins is safe. After the script finishes, you should verify the migration is complete. The migration script is suitable for use in a Continuous Integration environment for automatic migration.

### Developers: Source Migration

To make plugins compatible with `service_load`, it is necessary to add [ServiceLoader](https://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html) manifests to your source code, which should then be packaged in the release artifact. Manifests are resource files in `META-INF/services/` named after their superclass type, and contain a list of fully-qualified subclass names, one on each line.

In order for a plugin to be compatible, it must appear as a line in a manifest corresponding to the plugin superclass it extends. If a single plugin implements multiple plugin interfaces, then it should appear in a manifest for each interface it implements. If you have no classes for a certain type of plugin, you do not need to include a manifest file for that type. If you have classes which should not be visible as plugins, they should be marked abstract. The following types are expected to have manifests:

  * `org.apache.kafka.connect.sink.SinkConnector`
  * `org.apache.kafka.connect.source.SourceConnector`
  * `org.apache.kafka.connect.storage.Converter`
  * `org.apache.kafka.connect.storage.HeaderConverter`
  * `org.apache.kafka.connect.transforms.Transformation`
  * `org.apache.kafka.connect.transforms.predicates.Predicate`
  * `org.apache.kafka.common.config.provider.ConfigProvider`
  * `org.apache.kafka.connect.rest.ConnectRestExtension`
  * `org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy`



For example, if you only have one connector with the fully-qualified name `com.example.MySinkConnector`, then only one manifest file must be added to resources in `META-INF/services/org.apache.kafka.connect.sink.SinkConnector`, and the contents should be similar to the following:
    
    
    # license header or comment
    com.example.MySinkConnector

You should then verify that your manifests are correct by using the verification steps with a pre-release artifact. If the verification succeeds, you can then release the plugin normally, and operators can upgrade to the compatible version.
