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
package org.apache.kafka.streams.scala.kstream

import org.apache.kafka.streams.kstream.Named
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.scala.utils.TestDriver
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import scala.jdk.CollectionConverters._

class KStreamSplitTest extends TestDriver {

  @Test
  def testRouteMessagesAccordingToPredicates(): Unit = {
    val builder = new StreamsBuilder()
    val sourceTopic = "source"
    val sinkTopic = Array("default", "even", "three");

    val m = builder
      .stream[Integer, Integer](sourceTopic)
      .split(Named.as("_"))
      .branch((_, v) => v % 2 == 0)
      .branch((_, v) => v % 3 == 0)
      .defaultBranch()

    m("_0").to(sinkTopic(0))
    m("_1").to(sinkTopic(1))
    m("_2").to(sinkTopic(2))

    val testDriver = createTestDriver(builder)
    val testInput = testDriver.createInput[Integer, Integer](sourceTopic)
    val testOutput = sinkTopic.map(name => testDriver.createOutput[Integer, Integer](name))

    testInput.pipeValueList(
      List(1, 2, 3, 4, 5)
        .map(Integer.valueOf)
        .asJava
    )
    assertEquals(List(1, 5), testOutput(0).readValuesToList().asScala)
    assertEquals(List(2, 4), testOutput(1).readValuesToList().asScala)
    assertEquals(List(3), testOutput(2).readValuesToList().asScala)

    testDriver.close()
  }

  @Test
  def testRouteMessagesToConsumers(): Unit = {
    val builder = new StreamsBuilder()
    val sourceTopic = "source"

    val m = builder
      .stream[Integer, Integer](sourceTopic)
      .split(Named.as("_"))
      .branch((_, v) => v % 2 == 0, Branched.withConsumer(ks => ks.to("even"), "consumedEvens"))
      .branch((_, v) => v % 3 == 0, Branched.withFunction(ks => ks.mapValues(x => x * x), "mapped"))
      .noDefaultBranch()

    m("_mapped").to("mapped")

    val testDriver = createTestDriver(builder)
    val testInput = testDriver.createInput[Integer, Integer](sourceTopic)
    testInput.pipeValueList(
      List(1, 2, 3, 4, 5, 9)
        .map(Integer.valueOf)
        .asJava
    )

    val even = testDriver.createOutput[Integer, Integer]("even")
    val mapped = testDriver.createOutput[Integer, Integer]("mapped")

    assertEquals(List(2, 4), even.readValuesToList().asScala)
    assertEquals(List(9, 81), mapped.readValuesToList().asScala)

    testDriver.close()
  }

  @Test
  def testRouteMessagesToAnonymousConsumers(): Unit = {
    val builder = new StreamsBuilder()
    val sourceTopic = "source"

    val m = builder
      .stream[Integer, Integer](sourceTopic)
      .split(Named.as("_"))
      .branch((_, v) => v % 2 == 0, Branched.withConsumer(ks => ks.to("even")))
      .branch((_, v) => v % 3 == 0, Branched.withFunction(ks => ks.mapValues(x => x * x)))
      .noDefaultBranch()

    m("_2").to("mapped")

    val testDriver = createTestDriver(builder)
    val testInput = testDriver.createInput[Integer, Integer](sourceTopic)
    testInput.pipeValueList(
      List(1, 2, 3, 4, 5, 9)
        .map(Integer.valueOf)
        .asJava
    )

    val even = testDriver.createOutput[Integer, Integer]("even")
    val mapped = testDriver.createOutput[Integer, Integer]("mapped")

    assertEquals(List(2, 4), even.readValuesToList().asScala)
    assertEquals(List(9, 81), mapped.readValuesToList().asScala)

    testDriver.close()
  }
}
