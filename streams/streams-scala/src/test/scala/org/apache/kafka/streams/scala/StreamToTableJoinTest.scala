/*
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

import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.scala.utils.TestDriver
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

import scala.jdk.CollectionConverters._

/**
 * Test suite that demonstrates a stream-table join in Kafka Streams using the Scala API with implicit serdes.
 */
class StreamToTableJoinTest extends TestDriver {

  private val userClicksTopic = "user-clicks"
  private val userRegionsTopic = "user-regions"
  private val outputTopic = "output-topic"

  // Input 1: Clicks per user (multiple records allowed per user).
  private val userClicks: Seq[(String, Long)] = Seq(
    "alice" -> 13L,
    "bob" -> 4L,
    "chao" -> 25L,
    "bob" -> 19L,
    "dave" -> 56L,
    "eve" -> 78L,
    "alice" -> 40L,
    "fang" -> 99L
  )

  // Input 2: Region per user (multiple records allowed per user).
  private val userRegions: Seq[(String, String)] = Seq(
    "alice" -> "asia",
    "bob" -> "americas",
    "chao" -> "asia",
    "dave" -> "europe",
    "alice" -> "europe",
    "eve" -> "americas",
    "fang" -> "asia"
  )

  private val expectedClicksPerRegion: Map[String, Long] = Map(
    "americas" -> 101L,
    "europe" -> 109L,
    "asia" -> 124L
  )

  @Test
  def testShouldCountClicksPerRegionWithImplicitSerdes(): Unit = {
    // DefaultSerdes brings into scope implicit serdes (mostly for primitives) that will set up all Grouped, Produced,
    // Consumed and Joined instances. So all APIs below that accept Grouped, Produced, Consumed or Joined will
    // get these instances automatically.
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

    val testDriver = createTestDriver(builder)
    val userRegionsInput = testDriver.createInput[String, String](userRegionsTopic)
    val userClicksInput = testDriver.createInput[String, Long](userClicksTopic)
    val output = testDriver.createOutput[String, Long](outputTopic)

    userRegions.foreach { case (user, region) => userRegionsInput.pipeInput(user, region) }
    userClicks.foreach { case (user, clicks) => userClicksInput.pipeInput(user, clicks) }

    assertEquals(expectedClicksPerRegion.asJava, output.readKeyValuesToMap())

    testDriver.close()
  }
}
