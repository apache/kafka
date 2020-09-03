/*
 * Copyright (C) 2018 Lightbend Inc. <https://www.lightbend.com>
 * Copyright (C) 2017-2018 Alexis Seigneurin.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.scala

import java.time.Duration
import java.util
import java.util.{Locale, Properties}
import java.util.regex.Pattern

import org.apache.kafka.common.serialization.{Serdes => SerdesJ}
import org.apache.kafka.streams.kstream.{
  Aggregator,
  Initializer,
  JoinWindows,
  KeyValueMapper,
  KGroupedStream => KGroupedStreamJ,
  KStream => KStreamJ,
  KTable => KTableJ,
  Materialized => MaterializedJ,
  Reducer,
  StreamJoined => StreamJoinedJ,
  Transformer,
  ValueJoiner,
  ValueMapper
}
import org.apache.kafka.streams.processor.{AbstractProcessor, ProcessorContext, ProcessorSupplier}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.serialization.{Serdes => NewSerdes}
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KeyValue, StreamsConfig, TopologyDescription, StreamsBuilder => StreamsBuilderJ}
import org.junit.Assert._
import org.junit._

import scala.jdk.CollectionConverters._

/**
 * Test suite that verifies that the topology built by the Java and Scala APIs match.
 */
class TopologyTest {

  private val inputTopic = "input-topic"
  private val userClicksTopic = "user-clicks-topic"
  private val userRegionsTopic = "user-regions-topic"

  private val pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS)

  @Test
  def shouldBuildIdenticalTopologyInJavaNScalaSimple(): Unit = {

    // build the Scala topology
    def getTopologyScala: TopologyDescription = {

      import org.apache.kafka.streams.scala.serialization.Serdes._

      val streamBuilder = new StreamsBuilder
      val textLines = streamBuilder.stream[String, String](inputTopic)

      val _: KStream[String, String] = textLines.flatMapValues(v => pattern.split(v.toLowerCase))

      streamBuilder.build().describe()
    }

    // build the Java topology
    def getTopologyJava: TopologyDescription = {
      val streamBuilder = new StreamsBuilderJ
      val textLines = streamBuilder.stream[String, String](inputTopic)
      val _: KStreamJ[String, String] = textLines.flatMapValues(s => pattern.split(s.toLowerCase).toIterable.asJava)
      streamBuilder.build().describe()
    }

    // should match
    assertEquals(getTopologyScala, getTopologyJava)
  }

  @Test
  def shouldBuildIdenticalTopologyInJavaNScalaAggregate(): Unit = {

    // build the Scala topology
    def getTopologyScala: TopologyDescription = {

      import org.apache.kafka.streams.scala.serialization.Serdes._

      val streamBuilder = new StreamsBuilder
      val textLines = streamBuilder.stream[String, String](inputTopic)

      textLines
        .flatMapValues(v => pattern.split(v.toLowerCase))
        .groupBy((_, v) => v)
        .count()

      streamBuilder.build().describe()
    }

    // build the Java topology
    def getTopologyJava: TopologyDescription = {

      val streamBuilder = new StreamsBuilderJ
      val textLines: KStreamJ[String, String] = streamBuilder.stream[String, String](inputTopic)

      val splits: KStreamJ[String, String] =
        textLines.flatMapValues(s => pattern.split(s.toLowerCase).toIterable.asJava)

      val grouped: KGroupedStreamJ[String, String] = splits.groupBy((_, v) => v)

      grouped.count()

      streamBuilder.build().describe()
    }

    // should match
    assertEquals(getTopologyScala, getTopologyJava)
  }

  @Test def shouldBuildIdenticalTopologyInJavaNScalaCogroupSimple(): Unit = {

    // build the Scala topology
    def getTopologyScala: TopologyDescription = {

      import org.apache.kafka.streams.scala.serialization.Serdes._

      val streamBuilder = new StreamsBuilder
      val textLines = streamBuilder.stream[String, String](inputTopic)
      textLines
        .mapValues(v => v.length)
        .groupByKey
        .cogroup((_, v1, v2: Long) => v1 + v2)
        .aggregate(0L)

      streamBuilder.build().describe()
    }

    // build the Java topology
    def getTopologyJava: TopologyDescription = {

      val streamBuilder = new StreamsBuilderJ
      val textLines: KStreamJ[String, String] = streamBuilder.stream[String, String](inputTopic)

      val splits: KStreamJ[String, Int] = textLines.mapValues(
        new ValueMapper[String, Int] {
          def apply(s: String): Int = s.length
        }
      )

      splits.groupByKey
        .cogroup((k: String, v: Int, a: Long) => a + v)
        .aggregate(() => 0L)

      streamBuilder.build().describe()
    }

    // should match
    assertEquals(getTopologyScala, getTopologyJava)
  }

  @Test def shouldBuildIdenticalTopologyInJavaNScalaCogroup(): Unit = {

    // build the Scala topology
    def getTopologyScala: TopologyDescription = {

      import org.apache.kafka.streams.scala.serialization.Serdes._

      val streamBuilder = new StreamsBuilder
      val textLines1 = streamBuilder.stream[String, String](inputTopic)
      val textLines2 = streamBuilder.stream[String, String]("inputTopic2")

      textLines1
        .mapValues(v => v.length)
        .groupByKey
        .cogroup((_, v1, v2: Long) => v1 + v2)
        .cogroup(textLines2.groupByKey, (_, v: String, a) => v.length + a)
        .aggregate(0L)

      streamBuilder.build().describe()
    }

    // build the Java topology
    def getTopologyJava: TopologyDescription = {

      val streamBuilder = new StreamsBuilderJ
      val textLines1: KStreamJ[String, String] = streamBuilder.stream[String, String](inputTopic)
      val textLines2: KStreamJ[String, String] = streamBuilder.stream[String, String]("inputTopic2")

      val splits: KStreamJ[String, Int] = textLines1.mapValues(
        new ValueMapper[String, Int] {
          def apply(s: String): Int = s.length
        }
      )

      splits.groupByKey
        .cogroup((k: String, v: Int, a: Long) => a + v)
        .cogroup(textLines2.groupByKey(), (k: String, v: String, a: Long) => v.length + a)
        .aggregate(() => 0L)

      streamBuilder.build().describe()
    }

    // should match
    assertEquals(getTopologyScala, getTopologyJava)
  }

  @Test def shouldBuildIdenticalTopologyInJavaNScalaJoin(): Unit = {

    // build the Scala topology
    def getTopologyScala: TopologyDescription = {
      import org.apache.kafka.streams.scala.serialization.Serdes._

      val builder = new StreamsBuilder()

      val userClicksStream: KStream[String, Long] = builder.stream(userClicksTopic)

      val userRegionsTable: KTable[String, String] = builder.table(userRegionsTopic)

      // clicks per region
      userClicksStream
        .leftJoin(userRegionsTable)((clicks, region) => (if (region == null) "UNKNOWN" else region, clicks))
        .map((_, regionWithClicks) => regionWithClicks)
        .groupByKey
        .reduce(_ + _)

      builder.build().describe()
    }

    // build the Java topology
    def getTopologyJava: TopologyDescription = {

      import java.lang.{Long => JLong}

      val builder: StreamsBuilderJ = new StreamsBuilderJ()

      val userClicksStream: KStreamJ[String, JLong] =
        builder.stream[String, JLong](userClicksTopic, Consumed.`with`[String, JLong])

      val userRegionsTable: KTableJ[String, String] =
        builder.table[String, String](userRegionsTopic, Consumed.`with`[String, String])

      // Join the stream against the table.
      val valueJoinerJ: ValueJoiner[JLong, String, (String, JLong)] =
        (clicks: JLong, region: String) => (if (region == null) "UNKNOWN" else region, clicks)
      val userClicksJoinRegion: KStreamJ[String, (String, JLong)] = userClicksStream.leftJoin(
        userRegionsTable,
        valueJoinerJ,
        Joined.`with`[String, JLong, String]
      )

      // Change the stream from <user> -> <region, clicks> to <region> -> <clicks>
      val clicksByRegion: KStreamJ[String, JLong] = userClicksJoinRegion.map { (_, regionWithClicks) =>
        new KeyValue(regionWithClicks._1, regionWithClicks._2)
      }

      // Compute the total per region by summing the individual click counts per region.
      clicksByRegion
        .groupByKey(Grouped.`with`[String, JLong])
        .reduce((v1, v2) => v1 + v2)

      builder.build().describe()
    }

    // should match
    assertEquals(getTopologyScala, getTopologyJava)
  }

  @Test
  def shouldBuildIdenticalTopologyInJavaNScalaTransform(): Unit = {

    // build the Scala topology
    def getTopologyScala: TopologyDescription = {

      import org.apache.kafka.streams.scala.serialization.Serdes._

      val streamBuilder = new StreamsBuilder
      val textLines = streamBuilder.stream[String, String](inputTopic)

      val _: KTable[String, Long] = textLines
        .transform(
          () =>
            new Transformer[String, String, KeyValue[String, String]] {
              override def init(context: ProcessorContext): Unit = ()
              override def transform(key: String, value: String): KeyValue[String, String] =
                new KeyValue(key, value.toLowerCase)
              override def close(): Unit = ()
          }
        )
        .groupBy((_, v) => v)
        .count()

      streamBuilder.build().describe()
    }

    // build the Java topology
    def getTopologyJava: TopologyDescription = {

      val streamBuilder = new StreamsBuilderJ
      val textLines: KStreamJ[String, String] = streamBuilder.stream[String, String](inputTopic)

      val lowered: KStreamJ[String, String] = textLines.transform(
        () =>
          new Transformer[String, String, KeyValue[String, String]] {
            override def init(context: ProcessorContext): Unit = ()
            override def transform(key: String, value: String): KeyValue[String, String] =
              new KeyValue(key, value.toLowerCase)
            override def close(): Unit = ()
        }
      )

      val grouped: KGroupedStreamJ[String, String] = lowered.groupBy((_, v) => v)

      // word counts
      grouped.count()

      streamBuilder.build().describe()
    }

    // should match
    assertEquals(getTopologyScala, getTopologyJava)
  }

  @Test
  def shouldBuildIdenticalTopologyInJavaNScalaProperties(): Unit = {

    val props = new Properties()
    props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE)

    val propsNoOptimization = new Properties()
    propsNoOptimization.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.NO_OPTIMIZATION)

    val AGGREGATION_TOPIC = "aggregationTopic"
    val REDUCE_TOPIC = "reduceTopic"
    val JOINED_TOPIC = "joinedTopic"

    // build the Scala topology
    def getTopologyScala: StreamsBuilder = {

      val aggregator = (_: String, v: String, agg: Int) => agg + v.length
      val reducer = (v1: String, v2: String) => v1 + ":" + v2
      val processorValueCollector: util.List[String] = new util.ArrayList[String]

      val builder: StreamsBuilder = new StreamsBuilder

      val sourceStream: KStream[String, String] =
        builder.stream(inputTopic)(Consumed.`with`(NewSerdes.stringSerde, NewSerdes.stringSerde))

      val mappedStream: KStream[String, String] =
        sourceStream.map((k: String, v: String) => (k.toUpperCase(Locale.getDefault), v))
      mappedStream
        .filter((k: String, _: String) => k == "B")
        .mapValues((v: String) => v.toUpperCase(Locale.getDefault))
        .process(() => new SimpleProcessor(processorValueCollector))

      val stream2 = mappedStream.groupByKey
        .aggregate(0)(aggregator)(Materialized.`with`(NewSerdes.stringSerde, NewSerdes.intSerde))
        .toStream
      stream2.to(AGGREGATION_TOPIC)(Produced.`with`(NewSerdes.stringSerde, NewSerdes.intSerde))

      // adding operators for case where the repartition node is further downstream
      val stream3 = mappedStream
        .filter((_: String, _: String) => true)
        .peek((k: String, v: String) => System.out.println(k + ":" + v))
        .groupByKey
        .reduce(reducer)(Materialized.`with`(NewSerdes.stringSerde, NewSerdes.stringSerde))
        .toStream
      stream3.to(REDUCE_TOPIC)(Produced.`with`(NewSerdes.stringSerde, NewSerdes.stringSerde))

      mappedStream
        .filter((k: String, _: String) => k == "A")
        .join(stream2)((v1: String, v2: Int) => v1 + ":" + v2.toString, JoinWindows.of(Duration.ofMillis(5000)))(
          StreamJoined.`with`(NewSerdes.stringSerde, NewSerdes.stringSerde, NewSerdes.intSerde)
        )
        .to(JOINED_TOPIC)

      mappedStream
        .filter((k: String, _: String) => k == "A")
        .join(stream3)((v1: String, v2: String) => v1 + ":" + v2.toString, JoinWindows.of(Duration.ofMillis(5000)))(
          StreamJoined.`with`(NewSerdes.stringSerde, NewSerdes.stringSerde, NewSerdes.stringSerde)
        )
        .to(JOINED_TOPIC)

      builder
    }

    // build the Java topology
    def getTopologyJava: StreamsBuilderJ = {

      val keyValueMapper: KeyValueMapper[String, String, KeyValue[String, String]] =
        (key, value) => KeyValue.pair(key.toUpperCase(Locale.getDefault), value)
      val initializer: Initializer[Integer] = () => 0
      val aggregator: Aggregator[String, String, Integer] = (_, value, aggregate) => aggregate + value.length
      val reducer: Reducer[String] = (v1, v2) => v1 + ":" + v2
      val valueMapper: ValueMapper[String, String] = v => v.toUpperCase(Locale.getDefault)
      val processorValueCollector = new util.ArrayList[String]
      val processorSupplier: ProcessorSupplier[String, String] = () => new SimpleProcessor(processorValueCollector)
      val valueJoiner2: ValueJoiner[String, Integer, String] = (value1, value2) => value1 + ":" + value2.toString
      val valueJoiner3: ValueJoiner[String, String, String] = (value1, value2) => value1 + ":" + value2

      val builder = new StreamsBuilderJ

      val sourceStream = builder.stream(inputTopic, Consumed.`with`(NewSerdes.stringSerde, NewSerdes.stringSerde))

      val mappedStream: KStreamJ[String, String] =
        sourceStream.map(keyValueMapper)
      mappedStream
        .filter((key, _) => key == "B")
        .mapValues[String](valueMapper)
        .process(processorSupplier)

      val stream2: KStreamJ[String, Integer] = mappedStream.groupByKey
        .aggregate(initializer, aggregator, MaterializedJ.`with`(NewSerdes.stringSerde, SerdesJ.Integer))
        .toStream
      stream2.to(AGGREGATION_TOPIC, Produced.`with`(NewSerdes.stringSerde, SerdesJ.Integer))

      // adding operators for case where the repartition node is further downstream
      val stream3 = mappedStream
        .filter((_, _) => true)
        .peek((k, v) => System.out.println(k + ":" + v))
        .groupByKey
        .reduce(reducer, MaterializedJ.`with`(NewSerdes.stringSerde, NewSerdes.stringSerde))
        .toStream
      stream3.to(REDUCE_TOPIC, Produced.`with`(NewSerdes.stringSerde, NewSerdes.stringSerde))

      mappedStream
        .filter((key, _) => key == "A")
        .join[Integer, String](stream2,
                               valueJoiner2,
                               JoinWindows.of(Duration.ofMillis(5000)),
                               StreamJoinedJ.`with`(NewSerdes.stringSerde, NewSerdes.stringSerde, SerdesJ.Integer))
        .to(JOINED_TOPIC)

      mappedStream
        .filter((key, _) => key == "A")
        .join(stream3,
              valueJoiner3,
              JoinWindows.of(Duration.ofMillis(5000)),
              StreamJoinedJ.`with`(NewSerdes.stringSerde, NewSerdes.stringSerde, SerdesJ.String))
        .to(JOINED_TOPIC)

      builder
    }

    assertNotEquals(getTopologyScala.build(props).describe.toString,
                    getTopologyScala.build(propsNoOptimization).describe.toString)
    assertEquals(getTopologyScala.build(propsNoOptimization).describe.toString,
                 getTopologyJava.build(propsNoOptimization).describe.toString)
    assertEquals(getTopologyScala.build(props).describe.toString, getTopologyJava.build(props).describe.toString)
  }

  private class SimpleProcessor private[TopologyTest] (val valueList: util.List[String])
      extends AbstractProcessor[String, String] {
    override def process(key: String, value: String): Unit =
      valueList.add(value)
  }
}
