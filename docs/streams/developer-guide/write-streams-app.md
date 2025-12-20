---
title: Writing a Streams Application
description: 
weight: 1
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





Any Java or Scala application that makes use of the Kafka Streams library is considered a Kafka Streams application. The computational logic of a Kafka Streams application is defined as a [processor topology](../core-concepts#streams_topology), which is a graph of stream processors (nodes) and streams (edges).

You can define the processor topology with the Kafka Streams APIs:

[Kafka Streams DSL](dsl-api.html#streams-developer-guide-dsl)
    A high-level API that provides the most common data transformation operations such as `map`, `filter`, `join`, and `aggregations` out of the box. The DSL is the recommended starting point for developers new to Kafka Streams, and should cover many use cases and stream processing needs. If you're writing a Scala application then you can use the [Kafka Streams DSL for Scala](dsl-api.html#scala-dsl) library which removes much of the Java/Scala interoperability boilerplate as opposed to working directly with the Java DSL.
[Processor API](processor-api.html#streams-developer-guide-processor-api)
    A low-level API that lets you add and connect processors as well as interact directly with state stores. The Processor API provides you with even more flexibility than the DSL but at the expense of requiring more manual work on the side of the application developer (e.g., more lines of code).

# Libraries and Maven artifacts

This section lists the Kafka Streams related libraries that are available for writing your Kafka Streams applications.

You can define dependencies on the following libraries for your Kafka Streams applications.  
  
<table>  
<tr>  
<th>

Group ID
</th>  
<th>

Artifact ID
</th>  
<th>

Version
</th>  
<th>

Description
</th> </tr>  
<tr>  
<td>

`org.apache.kafka`
</td>  
<td>

`kafka-streams`
</td>  
<td>

`4.0.1`
</td>  
<td>

(Required) Base library for Kafka Streams.
</td> </tr>  
<tr>  
<td>

`org.apache.kafka`
</td>  
<td>

`kafka-clients`
</td>  
<td>

`4.0.1`
</td>  
<td>

(Required) Kafka client library. Contains built-in serializers/deserializers.
</td> </tr>  
<tr>  
<td>

`org.apache.kafka`
</td>  
<td>

`kafka-streams-scala`
</td>  
<td>

`4.0.1`
</td>  
<td>

(Optional) Kafka Streams DSL for Scala library to write Scala Kafka Streams applications. When not using SBT you will need to suffix the artifact ID with the correct version of Scala your application is using (`_2.12`, `_2.13`)
</td> </tr> </table>

**Tip**

See the section [Data Types and Serialization](datatypes.html#streams-developer-guide-serdes) for more information about Serializers/Deserializers.

Example `pom.xml` snippet when using Maven:
    
    
    <dependency>
        <groupId>org.apache.kafka</groupId>
        <artifactId>kafka-streams</artifactId>
        <version>4.0.1</version>
    </dependency>
    <dependency>
        <groupId>org.apache.kafka</groupId>
        <artifactId>kafka-clients</artifactId>
        <version>4.0.1</version>
    </dependency>
        <dependency>
        <groupId>org.apache.kafka</groupId>
        <artifactId>kafka-streams-scala_2.13</artifactId>
        <version>4.0.1</version>
    </dependency>

# Using Kafka Streams within your application code

You can call Kafka Streams from anywhere in your application code, but usually these calls are made within the `main()` method of your application, or some variant thereof. The basic elements of defining a processing topology within your application are described below.

First, you must create an instance of `KafkaStreams`.

  * The first argument of the `KafkaStreams` constructor takes a topology (either `StreamsBuilder#build()` for the [DSL](dsl-api.html#streams-developer-guide-dsl) or `Topology` for the [Processor API](processor-api.html#streams-developer-guide-processor-api)) that is used to define a topology.
  * The second argument is an instance of `java.util.Properties`, which defines the configuration for this specific topology.



Code example:
    
    
    import org.apache.kafka.streams.KafkaStreams;
    import org.apache.kafka.streams.kstream.StreamsBuilder;
    import org.apache.kafka.streams.processor.Topology;
    
    // Use the builders to define the actual processing topology, e.g. to specify
    // from which input topics to read, which stream operations (filter, map, etc.)
    // should be called, and so on.  We will cover this in detail in the subsequent
    // sections of this Developer Guide.
    
    StreamsBuilder builder = ...;  // when using the DSL
    Topology topology = builder.build();
    //
    // OR
    //
    Topology topology = ...; // when using the Processor API
    
    // Use the configuration to tell your application where the Kafka cluster is,
    // which Serializers/Deserializers to use by default, to specify security settings,
    // and so on.
    Properties props = ...;
    
    KafkaStreams streams = new KafkaStreams(topology, props);

At this point, internal structures are initialized, but the processing is not started yet. You have to explicitly start the Kafka Streams thread by calling the `KafkaStreams#start()` method:
    
    
    // Start the Kafka Streams threads
    streams.start();

If there are other instances of this stream processing application running elsewhere (e.g., on another machine), Kafka Streams transparently re-assigns tasks from the existing instances to the new instance that you just started. For more information, see [Stream Partitions and Tasks](../architecture.html#streams_architecture_tasks) and [Threading Model](../architecture.html#streams_architecture_threads).

To catch any unexpected exceptions, you can set an `java.lang.Thread.UncaughtExceptionHandler` before you start the application. This handler is called whenever a stream thread is terminated by an unexpected exception:
    
    
    streams.setUncaughtExceptionHandler((Thread thread, Throwable throwable) -> {
      // here you should examine the throwable/exception and perform an appropriate action!
    });
    

To stop the application instance, call the `KafkaStreams#close()` method:
    
    
    // Stop the Kafka Streams threads
    streams.close();

To allow your application to gracefully shutdown in response to SIGTERM, it is recommended that you add a shutdown hook and call `KafkaStreams#close`.

Here is a shutdown hook example in Java:
    
    
    // Add shutdown hook to stop the Kafka Streams threads.
    // You can optionally provide a timeout to `close`.
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

After an application is stopped, Kafka Streams will migrate any tasks that had been running in this instance to available remaining instances.

# Testing a Streams application

Kafka Streams comes with a `test-utils` module to help you test your application [here](testing.html). 

  * [Documentation](/documentation)
  * [Kafka Streams](/documentation/streams)
  * [Developer Guide](/documentation/streams/developer-guide/)


