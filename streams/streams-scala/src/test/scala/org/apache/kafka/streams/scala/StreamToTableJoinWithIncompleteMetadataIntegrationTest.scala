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

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization._
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.streams._
import org.apache.kafka.streams.integration.utils.{EmbeddedKafkaCluster, IntegrationTestUtils}
import org.apache.kafka.streams.processor.internals.StreamThread
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.test.TestUtils
import org.junit.Assert._
import org.junit._
import org.junit.rules.TemporaryFolder
import org.scalatest.junit.JUnitSuite

/**
 * Test suite that verifies the shutdown of StreamThread when metadata is incomplete during stream-table joins in Kafka Streams
 * <p>
 */
class StreamToTableJoinWithIncompleteMetadataIntegrationTest extends StreamToTableJoinScalaIntegrationTestBase {

  @Test def testShouldAutoShutdownOnIncompleteMetadata(): Unit = {

    // DefaultSerdes brings into scope implicit serdes (mostly for primitives) that will set up all Serialized, Produced,
    // Consumed and Joined instances. So all APIs below that accept Serialized, Produced, Consumed or Joined will
    // get these instances automatically
    import Serdes._

    val streamsConfiguration: Properties = getStreamsConfiguration()

    val builder = new StreamsBuilder()

    val userClicksStream: KStream[String, Long] = builder.stream(userClicksTopic)

    val userRegionsTable: KTable[String, String] = builder.table(userRegionsTopic+"1")

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

    val streams: KafkaStreamsWrapper = new KafkaStreamsWrapper(builder.build(), streamsConfiguration)
    val listener = new IntegrationTestUtils.StateListenerStub()
    streams.setStreamThreadStateListener(listener)
    streams.start()

    val actualClicksPerRegion: java.util.List[KeyValue[String, Long]] =
      produceNConsume(userClicksTopic, userRegionsTopic, outputTopic, false)
    while (!listener.revokedToPendingShutdownSeen()) {
      Thread.sleep(3)
    }
    streams.close()
    assertEquals(listener.runningToRevokedSeen(), true)
    assertEquals(listener.revokedToPendingShutdownSeen(), true)
  }
}
