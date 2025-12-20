---
title: Data Types and Serialization
description: 
weight: 6
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


Every Kafka Streams application must provide Serdes (Serializer/Deserializer) for the data types of record keys and record values (e.g. `java.lang.String`) to materialize the data when necessary. Operations that require such Serdes information include: `stream()`, `table()`, `to()`, `repartition()`, `groupByKey()`, `groupBy()`.

You can provide Serdes by using either of these methods, but you must use at least one:

  * By setting default Serdes in the `java.util.Properties` config instance.
  * By specifying explicit Serdes when calling the appropriate API methods, thus overriding the defaults.






# Configuring Serdes

Serdes specified in the Streams configuration are used as the default in your Kafka Streams application. Because this config's default is null, you must either set a default Serde by using this configuration or pass in Serdes explicitly, as described below.
    
    
    import org.apache.kafka.common.serialization.Serdes;
    import org.apache.kafka.streams.StreamsConfig;
    
    Properties settings = new Properties();
    // Default serde for keys of data records (here: built-in serde for String type)
    settings.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    // Default serde for values of data records (here: built-in serde for Long type)
    settings.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());

# Overriding default Serdes

You can also specify Serdes explicitly by passing them to the appropriate API methods, which overrides the default serde settings:
    
    
    import org.apache.kafka.common.serialization.Serde;
    import org.apache.kafka.common.serialization.Serdes;
    
    final Serde<String> stringSerde = Serdes.String();
    final Serde<Long> longSerde = Serdes.Long();
    
    // The stream userCountByRegion has type `String` for record keys (for region)
    // and type `Long` for record values (for user counts).
    KStream<String, Long> userCountByRegion = ...;
    userCountByRegion.to("RegionCountsTopic", Produced.with(stringSerde, longSerde));

If you want to override serdes selectively, i.e., keep the defaults for some fields, then don't specify the serde whenever you want to leverage the default settings:
    
    
    import org.apache.kafka.common.serialization.Serde;
    import org.apache.kafka.common.serialization.Serdes;
    
    // Use the default serializer for record keys (here: region as String) by not specifying the key serde,
    // but override the default serializer for record values (here: userCount as Long).
    final Serde<Long> longSerde = Serdes.Long();
    KStream<String, Long> userCountByRegion = ...;
    userCountByRegion.to("RegionCountsTopic", Produced.valueSerde(Serdes.Long()));

If some of your incoming records are corrupted or ill-formatted, they will cause the deserializer class to report an error. Since 1.0.x we have introduced an `DeserializationExceptionHandler` interface which allows you to customize how to handle such records. The customized implementation of the interface can be specified via the `StreamsConfig`. For more details, please feel free to read the [Configuring a Streams Application](config-streams.html#default-deserialization-exception-handler) section. 

# Available Serdes

## Primitive and basic types

Apache Kafka includes several built-in serde implementations for Java primitives and basic types such as `byte[]` in its `kafka-clients` Maven artifact:
    
    
    <dependency>
        <groupId>org.apache.kafka</groupId>
        <artifactId>kafka-clients</artifactId>
        <version>4.2.0</version>
    </dependency>

This artifact provides the following serde implementations under the package [org.apache.kafka.common.serialization](https://github.com/apache/kafka/blob/4.2/clients/src/main/java/org/apache/kafka/common/serialization), which you can leverage when e.g., defining default serializers in your Streams configuration.  
  
<table>  
<tr>  
<th>

Data type
</th>  
<th>

Serde
</th> </tr>  
<tr>  
<td>

byte[]
</td>  
<td>

`Serdes.ByteArray()`, `Serdes.Bytes()` (see tip below)
</td> </tr>  
<tr>  
<td>

ByteBuffer
</td>  
<td>

`Serdes.ByteBuffer()`
</td> </tr>  
<tr>  
<td>

Double
</td>  
<td>

`Serdes.Double()`
</td> </tr>  
<tr>  
<td>

Integer
</td>  
<td>

`Serdes.Integer()`
</td> </tr>  
<tr>  
<td>

Long
</td>  
<td>

`Serdes.Long()`
</td> </tr>  
<tr>  
<td>

String
</td>  
<td>

`Serdes.String()`
</td> </tr>  
<tr>  
<td>

UUID
</td>  
<td>

`Serdes.UUID()`
</td> </tr>  
<tr>  
<td>

Void
</td>  
<td>

`Serdes.Void()`
</td> </tr>  
<tr>  
<td>

List
</td>  
<td>

`Serdes.ListSerde()`
</td> </tr>  
<tr>  
<td>

Boolean
</td>  
<td>

`Serdes.Boolean()`
</td> </tr> </table>

**Tip**

[Bytes](https://github.com/apache/kafka/blob/4.2/clients/src/main/java/org/apache/kafka/common/utils/Bytes.java) is a wrapper for Java's `byte[]` (byte array) that supports proper equality and ordering semantics. You may want to consider using `Bytes` instead of `byte[]` in your applications.

## JSON

The Kafka Streams code examples also include a basic serde implementation for JSON:

  * [PageViewTypedDemo](https://github.com/apache/kafka/blob/4.2/streams/examples/src/main/java/org/apache/kafka/streams/examples/pageview/PageViewTypedDemo.java#L83)



As shown in the example, you can use JSONSerdes inner classes `Serdes.serdeFrom(<serializerInstance>, <deserializerInstance>)` to construct JSON compatible serializers and deserializers. 

## Window Serdes

Apache Kafka Streams includes serde implementations for windowed types in its `kafka-streams` Maven artifact:
    
    
    <dependency>
        <groupId>org.apache.kafka</groupId>
        <artifactId>kafka-streams</artifactId>
        <version>4.2.0</version>
    </dependency>

This artifact provides the following windowed serde implementations under the package [org.apache.kafka.streams.kstream](https://github.com/apache/kafka/blob/4.2/streams/src/main/java/org/apache/kafka/streams/kstream):

**Serdes:**

  * `WindowedSerdes.TimeWindowedSerde<T>`
  * `WindowedSerdes.SessionWindowedSerde<T>`



**Serializers:**

  * `TimeWindowedSerializer<T>`
  * `SessionWindowedSerializer<T>`



**Deserializers:**

  * `TimeWindowedDeserializer<T>`
  * `SessionWindowedDeserializer<T>`



### Usage in Code

When using windowed serdes in your application code, you typically create instances via constructors or factory methods:
    
    
    // Time windowed serde - using factory method
    Serde<Windowed<String>> timeWindowedSerde = 
        WindowedSerdes.timeWindowedSerdeFrom(String.class, 500L);
    
    // Time windowed serde - using constructor
    Serde<Windowed<String>> timeWindowedSerde2 = 
        new WindowedSerdes.TimeWindowedSerde<>(Serdes.String(), 500L);
    
    // Session windowed serde - using factory method
    Serde<Windowed<String>> sessionWindowedSerde = 
        WindowedSerdes.sessionWindowedSerdeFrom(String.class);
    
    // Session windowed serde - using constructor  
    Serde<Windowed<String>> sessionWindowedSerde2 = 
        new WindowedSerdes.SessionWindowedSerde<>(Serdes.String());
    
    // Using individual serializers/deserializers
    TimeWindowedSerializer<String> serializer = new TimeWindowedSerializer<>(Serdes.String().serializer());
    TimeWindowedDeserializer<String> deserializer = new TimeWindowedDeserializer<>(Serdes.String().deserializer(), 500L);

### Usage in Command Line

When using command-line tools (like `bin/kafka-console-consumer.sh`), you can configure windowed deserializers by passing the inner class and window size via configuration properties. The property names use a prefix pattern:
    
    
    # Time windowed deserializer configuration
    --formatter-property print.key=true \
    --formatter-property key.deserializer=org.apache.kafka.streams.kstream.TimeWindowedDeserializer \
    --formatter-property key.deserializer.windowed.inner.deserializer.class=org.apache.kafka.common.serialization.StringDeserializer \
    --formatter-property key.deserializer.window.size.ms=500
    
    # Session windowed deserializer configuration  
    --formatter-property print.key=true \
    --formatter-property key.deserializer=org.apache.kafka.streams.kstream.SessionWindowedDeserializer \
    --formatter-property key.deserializer.windowed.inner.deserializer.class=org.apache.kafka.common.serialization.StringDeserializer

### Deprecated Configs

The following `StreamsConfig` parameters are deprecated in favor of passing parameters directly to serializer/deserializer constructors:

  * `StreamsConfig.WINDOWED_INNER_CLASS_SERDE` is deprecated in favor of `TimeWindowedSerializer.WINDOWED_INNER_SERIALIZER_CLASS` and `TimeWindowedDeserializer.WINDOWED_INNER_DESERIALIZER_CLASS`
  * `StreamsConfig.WINDOW_SIZE_MS_CONFIG` is deprecated in favor of `TimeWindowedDeserializer.WINDOW_SIZE_MS_CONFIG`



# Implementing custom Serdes

If you need to implement custom Serdes, your best starting point is to take a look at the source code references of existing Serdes (see previous section). Typically, your workflow will be similar to:

  1. Write a _serializer_ for your data type `T` by implementing [org.apache.kafka.common.serialization.Serializer](https://github.com/apache/kafka/blob/4.2/clients/src/main/java/org/apache/kafka/common/serialization/Serializer.java).
  2. Write a _deserializer_ for `T` by implementing [org.apache.kafka.common.serialization.Deserializer](https://github.com/apache/kafka/blob/4.2/clients/src/main/java/org/apache/kafka/common/serialization/Deserializer.java).
  3. Write a _serde_ for `T` by implementing [org.apache.kafka.common.serialization.Serde](https://github.com/apache/kafka/blob/4.2/clients/src/main/java/org/apache/kafka/common/serialization/Serde.java), which you either do manually (see existing Serdes in the previous section) or by leveraging helper functions in [Serdes](https://github.com/apache/kafka/blob/4.2/clients/src/main/java/org/apache/kafka/common/serialization/Serdes.java) such as `Serdes.serdeFrom(Serializer<T>, Deserializer<T>)`. Note that you will need to implement your own class (that has no generic types) if you want to use your custom serde in the configuration provided to `KafkaStreams`. If your serde class has generic types or you use `Serdes.serdeFrom(Serializer<T>, Deserializer<T>)`, you can pass your serde only via methods calls (for example `builder.stream("topicName", Consumed.with(...))`).



# Kafka Streams DSL for Scala Implicit Serdes[](scala-dsl-serdes "Permalink to this headline")

When using the [Kafka Streams DSL for Scala](dsl-api.html#scala-dsl) you're not required to configure a default Serdes. In fact, it's not supported. Serdes are instead provided implicitly by default implementations for common primitive datatypes. See the [Implicit Serdes](dsl-api.html#scala-dsl-implicit-serdes) and [User-Defined Serdes](dsl-api.html#scala-dsl-user-defined-serdes) sections in the DSL API documentation for details

  * [Documentation](/documentation)
  * [Kafka Streams](/documentation/streams)
  * [Developer Guide](/documentation/streams/developer-guide/)


