/**
 * Copyright (C) 2018 Lightbend Inc. <https://www.lightbend.com>
 * Adapted from Confluent Inc. whose copyright is reproduced below.
 */

/*
 * Copyright Confluent Inc.
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

import org.junit.Assert._
import org.scalatest.junit.JUnitSuite
import org.junit.{Before, After, Test}

import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import ImplicitConversions._
import com.typesafe.scalalogging.LazyLogging
import _root_.scala.util.Random
import org.apache.kafka.streams.scala.kstream._

import net.manub.embeddedkafka._
import ConsumerExtensions._
import streams._

/**
  * End-to-end integration test that demonstrates how to perform a join between a KStream and a
  * KTable (think: KStream.leftJoin(KTable)), i.e. an example of a stateful computation.
  *
  * See StreamToTableJoinIntegrationTest for the equivalent Java example.
  *
  * Note: We intentionally use JUnit4 (wrapped by ScalaTest) for implementing this Scala integration
  * test so it is easier to compare this Scala code with the equivalent Java code at
  * StreamToTableJoinIntegrationTest.  One difference is that, to simplify the Scala/Junit integration, we
  * switched from BeforeClass (which must be `static`) to Before as well as from @ClassRule (which
  * must be `static` and `public`) to a workaround combination of `@Rule def` and a `private val`.
  */

class StreamToTableJoinScalaIntegrationTestImplicitSerdes extends JUnitSuite
  with StreamToTableJoinTestData with LazyLogging with EmbeddedKafkaStreamsAllInOne {

  @Test def testShouldCountClicksPerRegion(): Unit = {

    // DefaultSerdes brings into scope implicit serdes (mostly for primitives) that will set up all Serialized, Produced, 
    // Consumed and Joined instances. So all APIs below that accept Serialized, Produced, Consumed or Joined will
    // get these instances automatically
    import DefaultSerdes._

    val builder = new StreamsBuilder()

    val userClicksStream: KStream[String, Long] = builder.stream(userClicksTopic)

    val userRegionsTable: KTable[String, String] = builder.table(userRegionsTopic)

    // Compute the total per region by summing the individual click counts per region.
    val clicksPerRegion: KTable[String, Long] =
      userClicksStream

        // Join the stream against the table.
        .leftJoin(userRegionsTable, (clicks: Long, region: String) => (if (region == null) "UNKNOWN" else region, clicks))

        // Change the stream from <user> -> <region, clicks> to <region> -> <clicks>
        .map((_, regionWithClicks) => regionWithClicks)

        // Compute the total per region by summing the individual click counts per region.
        .groupByKey
        .reduce(_ + _)

    // Write the (continuously updating) results to the output topic.
    clicksPerRegion.toStream.to(outputTopic)

    runStreams(
      topicsToCreate = Seq(userRegionsTopic, userClicksTopic, outputTopic),
      topology = builder.build()){ 

      implicit val ks = stringSerde.serializer()
      implicit val vs = longSerde.serializer()
      implicit val kds = stringSerde.deserializer()
      implicit val vds = longSerde.deserializer()

      userRegions.foreach { r =>
        publishToKafka(userRegionsTopic, r.key, r.value)
      }

      userClicks.foreach { r =>
        publishToKafka(userClicksTopic, r.key, r.value)
      }

      withConsumer[String, Long, Unit] { consumer =>
        implicit val cr = ConsumerRetryConfig(10, 3000)
        val consumedMessages = consumer.consumeLazily(outputTopic)
        assertEquals(consumedMessages.sortBy(_.key).map(m => new KeyValue(m.key, m.value)), expectedClicksPerRegion.sortBy(_.key))
      }
    }
  }
}
