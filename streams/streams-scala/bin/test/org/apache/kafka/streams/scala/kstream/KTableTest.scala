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

import org.apache.kafka.streams.kstream.Suppressed.BufferConfig
import org.apache.kafka.streams.kstream.{
  Named,
  SessionWindows,
  SlidingWindows,
  Suppressed => JSuppressed,
  TimeWindows,
  Windowed
}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.scala.utils.TestDriver
import org.apache.kafka.streams.scala.{ByteArrayKeyValueStore, StreamsBuilder}
import org.junit.jupiter.api.Assertions.{assertEquals, assertNull, assertTrue}
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Duration.ofMillis

import scala.jdk.CollectionConverters._

//noinspection ScalaDeprecation
class KTableTest extends TestDriver {

  @Test
  def testFilterRecordsSatisfyingPredicate(): Unit = {
    val builder = new StreamsBuilder()
    val sourceTopic = "source"
    val sinkTopic = "sink"

    val table = builder.stream[String, String](sourceTopic).groupBy((key, _) => key).count()
    table.filter((key, value) => key.equals("a") && value == 1).toStream.to(sinkTopic)

    val testDriver = createTestDriver(builder)
    val testInput = testDriver.createInput[String, String](sourceTopic)
    val testOutput = testDriver.createOutput[String, Long](sinkTopic)

    {
      testInput.pipeInput("a", "passes filter : add new row to table")
      val record = testOutput.readKeyValue
      assertEquals("a", record.key)
      assertEquals(1, record.value)
    }
    {
      testInput.pipeInput("a", "fails filter : remove existing row from table")
      val record = testOutput.readKeyValue
      assertEquals("a", record.key)
      assertNull(record.value)
    }
    {
      testInput.pipeInput("b", "fails filter : no output")
      assertTrue(testOutput.isEmpty)
    }
    assertTrue(testOutput.isEmpty)

    testDriver.close()
  }

  @Test
  def testFilterRecordsNotSatisfyingPredicate(): Unit = {
    val builder = new StreamsBuilder()
    val sourceTopic = "source"
    val sinkTopic = "sink"

    val table = builder.stream[String, String](sourceTopic).groupBy((key, _) => key).count()
    table.filterNot((_, value) => value > 1).toStream.to(sinkTopic)

    val testDriver = createTestDriver(builder)
    val testInput = testDriver.createInput[String, String](sourceTopic)
    val testOutput = testDriver.createOutput[String, Long](sinkTopic)

    {
      testInput.pipeInput("1", "value1")
      val record = testOutput.readKeyValue
      assertEquals("1", record.key)
      assertEquals(1, record.value)
    }
    {
      testInput.pipeInput("1", "value2")
      val record = testOutput.readKeyValue
      assertEquals("1", record.key)
      assertNull(record.value)
    }
    {
      testInput.pipeInput("2", "value1")
      val record = testOutput.readKeyValue
      assertEquals("2", record.key)
      assertEquals(1, record.value)
    }
    assertTrue(testOutput.isEmpty)

    testDriver.close()
  }

  @Test
  def testJoinCorrectlyRecords(): Unit = {
    val builder = new StreamsBuilder()
    val sourceTopic1 = "source1"
    val sourceTopic2 = "source2"
    val sinkTopic = "sink"

    val table1 = builder.stream[String, String](sourceTopic1).groupBy((key, _) => key).count()
    val table2 = builder.stream[String, String](sourceTopic2).groupBy((key, _) => key).count()
    table1.join(table2)((a, b) => a + b).toStream.to(sinkTopic)

    val testDriver = createTestDriver(builder)
    val testInput1 = testDriver.createInput[String, String](sourceTopic1)
    val testInput2 = testDriver.createInput[String, String](sourceTopic2)
    val testOutput = testDriver.createOutput[String, Long](sinkTopic)

    testInput1.pipeInput("1", "topic1value1")
    testInput2.pipeInput("1", "topic2value1")
    assertEquals(2, testOutput.readValue)

    assertTrue(testOutput.isEmpty)

    testDriver.close()
  }

  @Test
  def testJoinCorrectlyRecordsAndStateStore(): Unit = {
    val builder = new StreamsBuilder()
    val sourceTopic1 = "source1"
    val sourceTopic2 = "source2"
    val sinkTopic = "sink"
    val stateStore = "store"
    val materialized = Materialized.as[String, Long, ByteArrayKeyValueStore](stateStore)

    val table1 = builder.stream[String, String](sourceTopic1).groupBy((key, _) => key).count()
    val table2 = builder.stream[String, String](sourceTopic2).groupBy((key, _) => key).count()
    table1.join(table2, materialized)((a, b) => a + b).toStream.to(sinkTopic)

    val testDriver = createTestDriver(builder)
    val testInput1 = testDriver.createInput[String, String](sourceTopic1)
    val testInput2 = testDriver.createInput[String, String](sourceTopic2)
    val testOutput = testDriver.createOutput[String, Long](sinkTopic)

    testInput1.pipeInput("1", "topic1value1")
    testInput2.pipeInput("1", "topic2value1")
    assertEquals(2, testOutput.readValue)
    assertEquals(2, testDriver.getKeyValueStore[String, Long](stateStore).get("1"))

    assertTrue(testOutput.isEmpty)

    testDriver.close()
  }

  @Test
  def testCorrectlySuppressResultsUsingSuppressedUntilTimeLimit(): Unit = {
    val builder = new StreamsBuilder()
    val sourceTopic = "source"
    val sinkTopic = "sink"
    val window = TimeWindows.ofSizeAndGrace(Duration.ofSeconds(1L), Duration.ofHours(24))
    val suppression = JSuppressed.untilTimeLimit[Windowed[String]](Duration.ofSeconds(2L), BufferConfig.unbounded())

    val table: KTable[Windowed[String], Long] = builder
      .stream[String, String](sourceTopic)
      .groupByKey
      .windowedBy(window)
      .count()
      .suppress(suppression)

    table.toStream((k, _) => s"${k.window().start()}:${k.window().end()}:${k.key()}").to(sinkTopic)

    val testDriver = createTestDriver(builder)
    val testInput = testDriver.createInput[String, String](sourceTopic)
    val testOutput = testDriver.createOutput[String, Long](sinkTopic)

    {
      // publish key=1 @ time 0 => count==1
      testInput.pipeInput("1", "value1", 0L)
      assertTrue(testOutput.isEmpty)
    }
    {
      // publish key=1 @ time 1 => count==2
      testInput.pipeInput("1", "value2", 1L)
      assertTrue(testOutput.isEmpty)
    }
    {
      // move event time past the first window, but before the suppression window
      testInput.pipeInput("2", "value1", 1001L)
      assertTrue(testOutput.isEmpty)
    }
    {
      // move event time riiiight before suppression window ends
      testInput.pipeInput("2", "value2", 1999L)
      assertTrue(testOutput.isEmpty)
    }
    {
      // publish a late event before suppression window terminates => count==3
      testInput.pipeInput("1", "value3", 999L)
      assertTrue(testOutput.isEmpty)
    }
    {
      // move event time right past the suppression window of the first window.
      testInput.pipeInput("2", "value3", 2001L)
      val record = testOutput.readKeyValue
      assertEquals("0:1000:1", record.key)
      assertEquals(3L, record.value)
    }
    assertTrue(testOutput.isEmpty)

    testDriver.close()
  }

  @Test
  def testCorrectlyGroupByKeyWindowedBySlidingWindow(): Unit = {
    val builder = new StreamsBuilder()
    val sourceTopic = "source"
    val sinkTopic = "sink"
    val window = SlidingWindows.ofTimeDifferenceAndGrace(ofMillis(1000L), ofMillis(1000L))
    val suppression = JSuppressed.untilWindowCloses(BufferConfig.unbounded())

    val table: KTable[Windowed[String], Long] = builder
      .stream[String, String](sourceTopic)
      .groupByKey
      .windowedBy(window)
      .count()
      .suppress(suppression)

    table.toStream((k, _) => s"${k.window().start()}:${k.window().end()}:${k.key()}").to(sinkTopic)

    val testDriver = createTestDriver(builder)
    val testInput = testDriver.createInput[String, String](sourceTopic)
    val testOutput = testDriver.createOutput[String, Long](sinkTopic)

    {
      // publish key=1 @ time 0 => count==1
      testInput.pipeInput("1", "value1", 0L)
      assertTrue(testOutput.isEmpty)
    }
    {
      // move event time right past the grace period of the first window.
      testInput.pipeInput("2", "value3", 5001L)
      val record = testOutput.readKeyValue
      assertEquals("0:1000:1", record.key)
      assertEquals(1L, record.value)
    }
    assertTrue(testOutput.isEmpty)

    testDriver.close()
  }

  @Test
  def testCorrectlySuppressResultsUsingSuppressedUntilWindowClosesByWindowed(): Unit = {
    val builder = new StreamsBuilder()
    val sourceTopic = "source"
    val sinkTopic = "sink"
    val window = TimeWindows.ofSizeAndGrace(Duration.ofSeconds(1L), Duration.ofSeconds(1L))
    val suppression = JSuppressed.untilWindowCloses(BufferConfig.unbounded())

    val table: KTable[Windowed[String], Long] = builder
      .stream[String, String](sourceTopic)
      .groupByKey
      .windowedBy(window)
      .count()
      .suppress(suppression)

    table.toStream((k, _) => s"${k.window().start()}:${k.window().end()}:${k.key()}").to(sinkTopic)

    val testDriver = createTestDriver(builder)
    val testInput = testDriver.createInput[String, String](sourceTopic)
    val testOutput = testDriver.createOutput[String, Long](sinkTopic)

    {
      // publish key=1 @ time 0 => count==1
      testInput.pipeInput("1", "value1", 0L)
      assertTrue(testOutput.isEmpty)
    }
    {
      // publish key=1 @ time 1 => count==2
      testInput.pipeInput("1", "value2", 1L)
      assertTrue(testOutput.isEmpty)
    }
    {
      // move event time past the window, but before the grace period
      testInput.pipeInput("2", "value1", 1001L)
      assertTrue(testOutput.isEmpty)
    }
    {
      // move event time riiiight before grace period ends
      testInput.pipeInput("2", "value2", 1999L)
      assertTrue(testOutput.isEmpty)
    }
    {
      // publish a late event before grace period terminates => count==3
      testInput.pipeInput("1", "value3", 999L)
      assertTrue(testOutput.isEmpty)
    }
    {
      // move event time right past the grace period of the first window.
      testInput.pipeInput("2", "value3", 2001L)
      val record = testOutput.readKeyValue
      assertEquals("0:1000:1", record.key)
      assertEquals(3L, record.value)
    }
    assertTrue(testOutput.isEmpty)

    testDriver.close()
  }

  @Test
  def testCorrectlySuppressResultsUsingSuppressedUntilWindowClosesBySession(): Unit = {
    val builder = new StreamsBuilder()
    val sourceTopic = "source"
    val sinkTopic = "sink"
    // Very similar to SuppressScenarioTest.shouldSupportFinalResultsForSessionWindows
    val window = SessionWindows.ofInactivityGapAndGrace(Duration.ofMillis(5L), Duration.ofMillis(10L))
    val suppression = JSuppressed.untilWindowCloses(BufferConfig.unbounded())

    val table: KTable[Windowed[String], Long] = builder
      .stream[String, String](sourceTopic)
      .groupByKey
      .windowedBy(window)
      .count()
      .suppress(suppression)

    table.toStream((k, _) => s"${k.window().start()}:${k.window().end()}:${k.key()}").to(sinkTopic)

    val testDriver = createTestDriver(builder)
    val testInput = testDriver.createInput[String, String](sourceTopic)
    val testOutput = testDriver.createOutput[String, Long](sinkTopic)

    {
      // first window
      testInput.pipeInput("k1", "v1", 0L)
      assertTrue(testOutput.isEmpty)
    }
    {
      // first window
      testInput.pipeInput("k1", "v1", 1L)
      assertTrue(testOutput.isEmpty)
    }
    {
      // new window, but grace period hasn't ended for first window
      testInput.pipeInput("k1", "v1", 8L)
      assertTrue(testOutput.isEmpty)
    }
    {
      // out-of-order event for first window, included since grade period hasn't passed
      testInput.pipeInput("k1", "v1", 2L)
      assertTrue(testOutput.isEmpty)
    }
    {
      // add to second window
      testInput.pipeInput("k1", "v1", 13L)
      assertTrue(testOutput.isEmpty)
    }
    {
      // add out-of-order to second window
      testInput.pipeInput("k1", "v1", 10L)
      assertTrue(testOutput.isEmpty)
    }
    {
      // push stream time forward to flush other events through
      testInput.pipeInput("k1", "v1", 30L)
      // late event should get dropped from the stream
      testInput.pipeInput("k1", "v1", 3L)
      // should now have to results
      val r1 = testOutput.readRecord
      assertEquals("0:2:k1", r1.key)
      assertEquals(3L, r1.value)
      assertEquals(2L, r1.timestamp)
      val r2 = testOutput.readRecord
      assertEquals("8:13:k1", r2.key)
      assertEquals(3L, r2.value)
      assertEquals(13L, r2.timestamp)
    }
    assertTrue(testOutput.isEmpty)

    testDriver.close()
  }

  @Test
  def testCorrectlySuppressResultsUsingSuppressedUntilTimeLimtByNonWindowed(): Unit = {
    val builder = new StreamsBuilder()
    val sourceTopic = "source"
    val sinkTopic = "sink"
    val suppression = JSuppressed.untilTimeLimit[String](Duration.ofSeconds(2L), BufferConfig.unbounded())

    val table: KTable[String, Long] = builder
      .stream[String, String](sourceTopic)
      .groupByKey
      .count()
      .suppress(suppression)

    table.toStream.to(sinkTopic)

    val testDriver = createTestDriver(builder)
    val testInput = testDriver.createInput[String, String](sourceTopic)
    val testOutput = testDriver.createOutput[String, Long](sinkTopic)

    {
      // publish key=1 @ time 0 => count==1
      testInput.pipeInput("1", "value1", 0L)
      assertTrue(testOutput.isEmpty)
    }
    {
      // publish key=1 @ time 1 => count==2
      testInput.pipeInput("1", "value2", 1L)
      assertTrue(testOutput.isEmpty)
    }
    {
      // move event time past the window, but before the grace period
      testInput.pipeInput("2", "value1", 1001L)
      assertTrue(testOutput.isEmpty)
    }
    {
      // move event time right before grace period ends
      testInput.pipeInput("2", "value2", 1999L)
      assertTrue(testOutput.isEmpty)
    }
    {
      // publish a late event before grace period terminates => count==3
      testInput.pipeInput("1", "value3", 999L)
      assertTrue(testOutput.isEmpty)
    }
    {
      // move event time right past the grace period of the first window.
      testInput.pipeInput("2", "value3", 2001L)
      val record = testOutput.readKeyValue
      assertEquals("1", record.key)
      assertEquals(3L, record.value)
    }
    assertTrue(testOutput.isEmpty)

    testDriver.close()
  }

  @Test
  def testSettingNameOnFilterProcessor(): Unit = {
    val builder = new StreamsBuilder()
    val sourceTopic = "source"
    val sinkTopic = "sink"

    val table = builder.stream[String, String](sourceTopic).groupBy((key, _) => key).count()
    table
      .filter((key, value) => key.equals("a") && value == 1, Named.as("my-name"))
      .toStream
      .to(sinkTopic)

    import scala.jdk.CollectionConverters._

    val filterNode = builder.build().describe().subtopologies().asScala.toList(1).nodes().asScala.toList(3)
    assertEquals("my-name", filterNode.name())
  }

  @Test
  def testSettingNameOnCountProcessor(): Unit = {
    val builder = new StreamsBuilder()
    val sourceTopic = "source"
    val sinkTopic = "sink"

    val table = builder.stream[String, String](sourceTopic).groupBy((key, _) => key).count(Named.as("my-name"))
    table.toStream.to(sinkTopic)

    import scala.jdk.CollectionConverters._

    val countNode = builder.build().describe().subtopologies().asScala.toList(1).nodes().asScala.toList(1)
    assertEquals("my-name", countNode.name())
  }

  @Test
  def testSettingNameOnJoinProcessor(): Unit = {
    val builder = new StreamsBuilder()
    val sourceTopic1 = "source1"
    val sourceTopic2 = "source2"
    val sinkTopic = "sink"

    val table1 = builder.stream[String, String](sourceTopic1).groupBy((key, _) => key).count()
    val table2 = builder.stream[String, String](sourceTopic2).groupBy((key, _) => key).count()
    table1
      .join(table2, Named.as("my-name"))((a, b) => a + b)
      .toStream
      .to(sinkTopic)

    val joinNodeLeft = builder.build().describe().subtopologies().asScala.toList(1).nodes().asScala.toList(6)
    val joinNodeRight = builder.build().describe().subtopologies().asScala.toList(1).nodes().asScala.toList(7)
    assertTrue(joinNodeLeft.name().contains("my-name"))
    assertTrue(joinNodeRight.name().contains("my-name"))
  }

  @Test
  def testMapValuesWithValueMapperWithMaterialized(): Unit = {
    val builder = new StreamsBuilder()
    val sourceTopic = "source"
    val stateStore = "store"
    val materialized = Materialized.as[String, Long, ByteArrayKeyValueStore](stateStore)

    val table = builder.stream[String, String](sourceTopic).toTable
    table.mapValues(value => value.length.toLong, materialized)

    val testDriver = createTestDriver(builder)
    val testInput = testDriver.createInput[String, String](sourceTopic)

    testInput.pipeInput("1", "topic1value1")
    assertEquals(12, testDriver.getKeyValueStore[String, Long](stateStore).get("1"))

    testDriver.close()
  }

  @Test
  def testMapValuesWithValueMapperWithKeyAndWithMaterialized(): Unit = {
    val builder = new StreamsBuilder()
    val sourceTopic = "source"
    val stateStore = "store"
    val materialized = Materialized.as[String, Long, ByteArrayKeyValueStore](stateStore)

    val table = builder.stream[String, String](sourceTopic).toTable
    table.mapValues((key, value) => key.length + value.length.toLong, materialized)

    val testDriver = createTestDriver(builder)
    val testInput = testDriver.createInput[String, String](sourceTopic)

    testInput.pipeInput("1", "topic1value1")
    assertEquals(13, testDriver.getKeyValueStore[String, Long](stateStore).get("1"))

    testDriver.close()
  }
}
