---
title: Data Types and Serialization
description: 
weight: 6
tags: ['kafka', 'docs']
aliases: 
keywords: 
type: docs
---

# Data Types and Serialization

Every Kafka Streams application must provide Serdes (Serializer/Deserializer) for the data types of record keys and record values (e.g. `java.lang.String`) to materialize the data when necessary. Operations that require such Serdes information include: `stream()`, `table()`, `to()`, `repartition()`, `groupByKey()`, `groupBy()`.

You can provide Serdes by using either of these methods, but you must use at least one:

  * By setting default Serdes in the `java.util.Properties` config instance.
  * By specifying explicit Serdes when calling the appropriate API methods, thus overriding the defaults.



**Table of Contents**

  * Configuring Serdes
  * Overriding default Serdes
  * Available Serdes
    * Primitive and basic types
    * JSON
    * Implementing custom serdes
  * Kafka Streams DSL for Scala Implicit Serdes



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

# Primitive and basic types

Apache Kafka includes several built-in serde implementations for Java primitives and basic types such as `byte[]` in its `kafka-clients` Maven artifact:
    
    
    <dependency>
        <groupId>org.apache.kafka</groupId>
        <artifactId>kafka-clients</artifactId>
        <version>2.8.0</version>
    </dependency>

This artifact provides the following serde implementations under the package [org.apache.kafka.common.serialization](https://github.com/apache/kafka/blob/4.0/clients/src/main/java/org/apache/kafka/common/serialization), which you can leverage when e.g., defining default serializers in your Streams configuration.

Data type | Serde  
---|---  
byte[] | `Serdes.ByteArray()`, `Serdes.Bytes()` (see tip below)  
ByteBuffer | `Serdes.ByteBuffer()`  
Double | `Serdes.Double()`  
Integer | `Serdes.Integer()`  
Long | `Serdes.Long()`  
String | `Serdes.String()`  
UUID | `Serdes.UUID()`  
Void | `Serdes.Void()`  
List | `Serdes.ListSerde()`  
Boolean | `Serdes.Boolean()`  
  
**Tip**

[Bytes](https://github.com/apache/kafka/blob/4.0/clients/src/main/java/org/apache/kafka/common/utils/Bytes.java) is a wrapper for Java's `byte[]` (byte array) that supports proper equality and ordering semantics. You may want to consider using `Bytes` instead of `byte[]` in your applications.

# JSON

You can use `JsonSerializer` and `JsonDeserializer` from Kafka Connect to construct JSON compatible serializers and deserializers using `Serdes.serdeFrom(<serializerInstance>, <deserializerInstance>)`. Note, that Kafka Connect's Json (de)serializer requires Java 17. 

# Implementing custom Serdes

If you need to implement custom Serdes, your best starting point is to take a look at the source code references of existing Serdes (see previous section). Typically, your workflow will be similar to:

  1. Write a _serializer_ for your data type `T` by implementing [org.apache.kafka.common.serialization.Serializer](https://github.com/apache/kafka/blob/4.0/clients/src/main/java/org/apache/kafka/common/serialization/Serializer.java).
  2. Write a _deserializer_ for `T` by implementing [org.apache.kafka.common.serialization.Deserializer](https://github.com/apache/kafka/blob/4.0/clients/src/main/java/org/apache/kafka/common/serialization/Deserializer.java).
  3. Write a _serde_ for `T` by implementing [org.apache.kafka.common.serialization.Serde](https://github.com/apache/kafka/blob/4.0/clients/src/main/java/org/apache/kafka/common/serialization/Serde.java), which you either do manually (see existing Serdes in the previous section) or by leveraging helper functions in [Serdes](https://github.com/apache/kafka/blob/4.0/clients/src/main/java/org/apache/kafka/common/serialization/Serdes.java) such as `Serdes.serdeFrom(Serializer<T>, Deserializer<T>)`. Note that you will need to implement your own class (that has no generic types) if you want to use your custom serde in the configuration provided to `KafkaStreams`. If your serde class has generic types or you use `Serdes.serdeFrom(Serializer<T>, Deserializer<T>)`, you can pass your serde only via methods calls (for example `builder.stream("topicName", Consumed.with(...))`).



# Kafka Streams DSL for Scala Implicit Serdes[](scala-dsl-serdes "Permalink to this headline")

When using the [Kafka Streams DSL for Scala](dsl-api.html#scala-dsl) you're not required to configure a default Serdes. In fact, it's not supported. Serdes are instead provided implicitly by default implementations for common primitive datatypes. See the [Implicit Serdes](dsl-api.html#scala-dsl-implicit-serdes) and [User-Defined Serdes](dsl-api.html#scala-dsl-user-defined-serdes) sections in the DSL API documentation for details

[Previous](/40/streams/developer-guide/processor-api) [Next](/40/streams/developer-guide/testing)

  * [Documentation](/documentation)
  * [Kafka Streams](/streams)
  * [Developer Guide](/streams/developer-guide/)


