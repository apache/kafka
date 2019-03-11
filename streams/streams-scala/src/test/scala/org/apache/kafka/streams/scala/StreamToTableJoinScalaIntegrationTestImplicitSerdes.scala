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

import org.apache.kafka.streams._
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
 * <p>
 * Note: In the current project settings SAM type conversion is turned off as it's experimental in Scala 2.11.
 * Hence the native Java API based version is more verbose.
 */
@Category(Array(classOf[IntegrationTest]))
class StreamToTableJoinScalaIntegrationTestImplicitSerdes extends StreamToTableJoinScalaIntegrationTestBase {

  @Test def testShouldCountClicksPerRegion(): Unit = {

    // DefaultSerdes brings into scope implicit serdes (mostly for primitives) that will set up all Grouped, Produced,
    // Consumed and Joined instances. So all APIs below that accept Grouped, Produced, Consumed or Joined will
    // get these instances automatically
    import Serdes._

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

  @Test def testShouldCountClicksPerRegionWithNamedRepartitionTopic(): Unit = {

    // DefaultSerdes brings into scope implicit serdes (mostly for primitives) that will set up all Grouped, Produced,
    // Consumed and Joined instances. So all APIs below that accept Grouped, Produced, Consumed or Joined will
    // get these instances automatically
    import Serdes._

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

  @Test def testShouldCountClicksPerRegionJava(): Unit = {

    import java.lang.{Long => JLong}

    import org.apache.kafka.streams.kstream.{KStream => KStreamJ, KTable => KTableJ, _}
    import org.apache.kafka.streams.{KafkaStreams => KafkaStreamsJ, StreamsBuilder => StreamsBuilderJ}

    val streamsConfiguration: Properties = getStreamsConfiguration()

    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)

    val builder: StreamsBuilderJ = new StreamsBuilderJ()

    val userClicksStream: KStreamJ[String, JLong] =
      builder.stream[String, JLong](userClicksTopicJ, Consumed.`with`(Serdes.String, Serdes.JavaLong))

    val userRegionsTable: KTableJ[String, String] =
      builder.table[String, String](userRegionsTopicJ, Consumed.`with`(Serdes.String, Serdes.String))

    // Join the stream against the table.
    val userClicksJoinRegion: KStreamJ[String, (String, JLong)] = userClicksStream
      .leftJoin(
        userRegionsTable,
        new ValueJoiner[JLong, String, (String, JLong)] {
          def apply(clicks: JLong, region: String): (String, JLong) =
            (if (region == null) "UNKNOWN" else region, clicks)
        },
        Joined.`with`[String, JLong, String](Serdes.String, Serdes.JavaLong, Serdes.String)
      )

    // Change the stream from <user> -> <region, clicks> to <region> -> <clicks>
    val clicksByRegion: KStreamJ[String, JLong] = userClicksJoinRegion
      .map {
        new KeyValueMapper[String, (String, JLong), KeyValue[String, JLong]] {
          def apply(k: String, regionWithClicks: (String, JLong)) =
            new KeyValue[String, JLong](regionWithClicks._1, regionWithClicks._2)
        }
      }

    // Compute the total per region by summing the individual click counts per region.
    val clicksPerRegion: KTableJ[String, JLong] = clicksByRegion
      .groupByKey(Grouped.`with`[String, JLong](Serdes.String, Serdes.JavaLong))
      .reduce {
        new Reducer[JLong] {
          def apply(v1: JLong, v2: JLong): JLong = v1 + v2
        }
      }

    // Write the (continuously updating) results to the output topic.
    clicksPerRegion.toStream.to(outputTopicJ, Produced.`with`(Serdes.String, Serdes.JavaLong))

    val streams: KafkaStreamsJ = new KafkaStreamsJ(builder.build(), streamsConfiguration)

    streams.start()
    produceNConsume(userClicksTopicJ, userRegionsTopicJ, outputTopicJ)
    streams.close()
  }
}
