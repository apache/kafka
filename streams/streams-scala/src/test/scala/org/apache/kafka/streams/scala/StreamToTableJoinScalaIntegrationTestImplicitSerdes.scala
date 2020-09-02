/*
 * Copyright (C) 2018 Lightbend Inc. <https://www.lightbend.com>
 * Copyright (C) 2017-2018 Alexis Seigneurin.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.util.Properties

import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig}
import org.apache.kafka.streams.scala.serialization.{Serdes => NewSerdes}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.scala.utils.StreamToTableJoinScalaIntegrationTestBase
import org.apache.kafka.test.IntegrationTest
import org.junit._
import org.junit.experimental.categories.Category

/**
 * Test suite that does an example to demonstrate stream-table joins in Kafka Streams
 * <p>
 * The suite contains the test case using Scala APIs `testShouldCountClicksPerRegion` and the same test case using the
 * Java APIs `testShouldCountClicksPerRegionJava`. The idea is to demonstrate that both generate the same result.
 */
@Category(Array(classOf[IntegrationTest]))
class StreamToTableJoinScalaIntegrationTestImplicitSerdes extends StreamToTableJoinScalaIntegrationTestBase {

  @Test def testShouldCountClicksPerRegion(): Unit = {

    // DefaultSerdes brings into scope implicit serdes (mostly for primitives) that will set up all Grouped, Produced,
    // Consumed and Joined instances. So all APIs below that accept Grouped, Produced, Consumed or Joined will
    // get these instances automatically
    import org.apache.kafka.streams.scala.serialization.Serdes._

    val streamsConfiguration: Properties = getStreamsConfiguration()

    val builder = new StreamsBuilder()

    val userClicksStream: KStream[String, Long] = builder.stream(userClicksTopic)

    val userRegionsTable: KTable[String, String] = builder.table(userRegionsTopic)

    // Compute the total per region by summing the individual click counts per region.
    val clicksPerRegion: KTable[String, Long] =
      userClicksStream

      // Join the stream against the table.
        .leftJoin(userRegionsTable)((clicks, region) => (if (region == null) "UNKNOWN" else region, clicks))

        // Change the stream from <user> -> <region, clicks> to <region> -> <clicks>
        .map((_, regionWithClicks) => regionWithClicks)

        // Compute the total per region by summing the individual click counts per region.
        .groupByKey
        .reduce(_ + _)

    // Write the (continuously updating) results to the output topic.
    clicksPerRegion.toStream.to(outputTopic)

    val streams: KafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration)
    streams.start()

    val actualClicksPerRegion: java.util.List[KeyValue[String, Long]] =
      produceNConsume(userClicksTopic, userRegionsTopic, outputTopic)

    Assert.assertTrue("Expected to process some data", !actualClicksPerRegion.isEmpty)

    streams.close()
  }

  @Test
  def testShouldCountClicksPerRegionWithNamedRepartitionTopic(): Unit = {

    // DefaultSerdes brings into scope implicit serdes (mostly for primitives) that will set up all Grouped, Produced,
    // Consumed and Joined instances. So all APIs below that accept Grouped, Produced, Consumed or Joined will
    // get these instances automatically
    import org.apache.kafka.streams.scala.serialization.Serdes._

    val streamsConfiguration: Properties = getStreamsConfiguration()

    val builder = new StreamsBuilder()

    val userClicksStream: KStream[String, Long] = builder.stream(userClicksTopic)

    val userRegionsTable: KTable[String, String] = builder.table(userRegionsTopic)

    // Compute the total per region by summing the individual click counts per region.
    val clicksPerRegion: KTable[String, Long] =
      userClicksStream

      // Join the stream against the table.
        .leftJoin(userRegionsTable)((clicks, region) => (if (region == null) "UNKNOWN" else region, clicks))

        // Change the stream from <user> -> <region, clicks> to <region> -> <clicks>
        .map((_, regionWithClicks) => regionWithClicks)

        // Compute the total per region by summing the individual click counts per region.
        .groupByKey
        .reduce(_ + _)

    // Write the (continuously updating) results to the output topic.
    clicksPerRegion.toStream.to(outputTopic)

    val streams: KafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration)
    streams.start()

    val actualClicksPerRegion: java.util.List[KeyValue[String, Long]] =
      produceNConsume(userClicksTopic, userRegionsTopic, outputTopic)

    Assert.assertTrue("Expected to process some data", !actualClicksPerRegion.isEmpty)

    streams.close()
  }

  @Test
  def testShouldCountClicksPerRegionJava(): Unit = {

    import java.lang.{Long => JLong}

    import org.apache.kafka.streams.kstream.{KStream => KStreamJ, KTable => KTableJ, _}
    import org.apache.kafka.streams.{KafkaStreams => KafkaStreamsJ, StreamsBuilder => StreamsBuilderJ}

    val streamsConfiguration: Properties = getStreamsConfiguration()

    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, NewSerdes.stringSerde.getClass.getName)
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, NewSerdes.stringSerde.getClass.getName)

    val builder: StreamsBuilderJ = new StreamsBuilderJ()

    val userClicksStream: KStreamJ[String, JLong] =
      builder.stream[String, JLong](userClicksTopicJ, Consumed.`with`(NewSerdes.stringSerde, NewSerdes.javaLongSerde))

    val userRegionsTable: KTableJ[String, String] =
      builder.table[String, String](userRegionsTopicJ, Consumed.`with`(NewSerdes.stringSerde, NewSerdes.stringSerde))

    // Join the stream against the table.
    val valueJoinerJ: ValueJoiner[JLong, String, (String, JLong)] =
      (clicks: JLong, region: String) => (if (region == null) "UNKNOWN" else region, clicks)
    val userClicksJoinRegion: KStreamJ[String, (String, JLong)] = userClicksStream.leftJoin(
      userRegionsTable,
      valueJoinerJ,
      Joined.`with`[String, JLong, String](NewSerdes.stringSerde, NewSerdes.javaLongSerde, NewSerdes.stringSerde)
    )

    // Change the stream from <user> -> <region, clicks> to <region> -> <clicks>
    val clicksByRegion: KStreamJ[String, JLong] = userClicksJoinRegion.map { (_, regionWithClicks) =>
      new KeyValue(regionWithClicks._1, regionWithClicks._2)
    }

    // Compute the total per region by summing the individual click counts per region.
    val clicksPerRegion: KTableJ[String, JLong] = clicksByRegion
      .groupByKey(Grouped.`with`(NewSerdes.stringSerde, NewSerdes.javaLongSerde))
      .reduce((v1, v2) => v1 + v2)

    // Write the (continuously updating) results to the output topic.
    clicksPerRegion.toStream.to(outputTopicJ, Produced.`with`(NewSerdes.stringSerde, NewSerdes.javaLongSerde))

    val streams = new KafkaStreamsJ(builder.build(), streamsConfiguration)

    streams.start()
    produceNConsume(userClicksTopicJ, userRegionsTopicJ, outputTopicJ)
    streams.close()
  }
}
