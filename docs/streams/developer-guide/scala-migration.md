---
title: Migrating from Streams Scala to Java API
description:
weight: 16
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

> **⚠️ DEPRECATION NOTICE**: The `kafka-streams-scala` library is deprecated as of Kafka 4.3
> and will be removed in Kafka 5.0. This guide will help you migrate your Scala applications
> to use the Java Streams API directly.
> For more information, see [KIP-1244](https://cwiki.apache.org/confluence/x/r4LMFw).

## Migration Overview

The Java Streams API works well from Scala with minimal adjustments. The main differences are:

1. **Use Java types directly** instead of Scala wrapper classes
2. **Configure Serdes explicitly** via `StreamsConfig` or pass them to methods

### Example: Word Count Application

#### Scala Wrapper Approach (Deprecated)

```scala
import java.util.Properties

import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.scala.serialization.Serdes._

object WordCountScala extends App {
  val props = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

  val builder = new StreamsBuilder  // Scala wrapper
  val textLines: KStream[String, String] = builder.stream[String, String]("input-topic")

  val wordCounts: KTable[String, Long] = textLines
    .flatMapValues(line => line.toLowerCase.split("\\W+"))
    .groupBy((_, word) => word)
    .count()

  wordCounts.toStream.to("output-topic")

  val streams = new KafkaStreams(builder.build(), props)
  streams.start()
}
```

#### Java API Approach

```scala
import java.util.Properties

import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig}
import org.apache.kafka.streams.kstream.{KStream, KTable, Produced}
import org.apache.kafka.common.serialization.Serdes
import scala.jdk.CollectionConverters._

object WordCountJava extends App {
  val props = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  // Configure default serdes
  props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, classOf[Serdes.StringSerde])
  props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[Serdes.StringSerde])

  val builder = new StreamsBuilder  // Java StreamsBuilder
  val textLines = builder.stream[String, String]("input-topic")

  val wordCounts = textLines
    .flatMapValues(_.toLowerCase.split("\\W+"))
    .groupBy((_, word) => word)
    .count()

  wordCounts.toStream.to("output-topic", Produced.`with`(Serdes.String(), Serdes.Long()))

  val streams = new KafkaStreams(builder.build(), props)
  streams.start()
}
```
