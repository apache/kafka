---
title: Introduction
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


# Kafka Streams

## The easiest way to write mission-critical real-time applications and microservices

Kafka Streams is a client library for building applications and microservices, where the input and output data are stored in Kafka clusters. It combines the simplicity of writing and deploying standard Java and Scala applications on the client side with the benefits of Kafka's server-side cluster technology.

 

 

 

 


## Tour of the Streams API

{{< carousel >}}
{{< carousel-item title="1. Intro to Streams" active="true" >}}
{{% youtube "ni3XPsYC5cQ" %}}
{{< /carousel-item >}}
{{< carousel-item title="2. Creating a Streams Application" >}}
{{% youtube "9ZhsnXM2OVM" %}}
{{< /carousel-item >}}
{{< carousel-item title="3. Transforming Data Pt. 1" >}}
{{% youtube "SYmqwvE8umM" %}}
{{< /carousel-item >}}
{{< carousel-item title="4. Transforming Data Pt. 2" >}}
{{% youtube "Vk55Kl9x_Fw" %}}
{{< /carousel-item >}}
{{< /carousel >}}



* * *

## Why you'll love using Kafka Streams!

  * Elastic, highly scalable, fault-tolerant
  * Deploy to containers, VMs, bare metal, cloud
  * Equally viable for small, medium, & large use cases
  * Fully integrated with Kafka security
  * Write standard Java and Scala applications
  * Exactly-once processing semantics
  * No separate processing cluster required
  * Develop on Mac, Linux, Windows



[Write your first app](/37/documentation/streams/tutorial)

* * *

## Kafka Streams use cases


{{< about/kstreams-users >}}


## Hello Kafka Streams

The code example below implements a WordCount application that is elastic, highly scalable, fault-tolerant, stateful, and ready to run in production at large scale

{{< tabpane >}}
{{% tab header="Java 8+ Java 7" %}}
```java
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Properties;

public class WordCountApplication {

   public static void main(final String[] args) throws Exception {
       Properties props = new Properties();
       props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
       props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-broker1:9092");
       props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
       props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

       StreamsBuilder builder = new StreamsBuilder();
       KStream<String, String> textLines = builder.stream("TextLinesTopic");
       KTable<String, Long> wordCounts = textLines
           .flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split("\W+")))
           .groupBy((key, word) -> word)
           .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"));
       wordCounts.toStream().to("WordsWithCountsTopic", Produced.with(Serdes.String(), Serdes.Long()));

       KafkaStreams streams = new KafkaStreams(builder.build(), props);
       streams.start();
   }

}


import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Properties;

public class WordCountApplication {

   public static void main(final String[] args) throws Exception {
       Properties props = new Properties();
       props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
       props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-broker1:9092");
       props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
       props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

       StreamsBuilder builder = new StreamsBuilder();
       KStream<String, String> textLines = builder.stream("TextLinesTopic");
       KTable<String, Long> wordCounts = textLines
           .flatMapValues(new ValueMapper<String, Iterable<String>>() {
               @Override
               public Iterable<String> apply(String textLine) {
                   return Arrays.asList(textLine.toLowerCase().split("\W+"));
               }
           })
           .groupBy(new KeyValueMapper<String, String, String>() {
               @Override
               public String apply(String key, String word) {
                   return word;
               }
           })
           .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"));


       wordCounts.toStream().to("WordsWithCountsTopic", Produced.with(Serdes.String(), Serdes.Long()));

       KafkaStreams streams = new KafkaStreams(builder.build(), props);
       streams.start();
   }

}
```
{{% /tab %}}
{{% tab header="Scala" %}}
```scala
import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

object WordCountApplication extends App {
  import Serdes._

  val props: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-broker1:9092")
    p
  }

  val builder: StreamsBuilder = new StreamsBuilder
  val textLines: KStream[String, String] = builder.stream[String, String]("TextLinesTopic")
  val wordCounts: KTable[String, Long] = textLines
    .flatMapValues(textLine => textLine.toLowerCase.split("\W+"))
    .groupBy((_, word) => word)
    .count()(Materialized.as("counts-store"))
  wordCounts.toStream.to("WordsWithCountsTopic")

  val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
  streams.start()

  sys.ShutdownHookThread {
     streams.close(10, TimeUnit.SECONDS)
  }
}
```
{{% /tab %}}
{{< /tabpane >}}

