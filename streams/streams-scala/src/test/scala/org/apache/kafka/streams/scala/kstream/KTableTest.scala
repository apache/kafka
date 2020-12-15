/*
 * Copyright (C) 2018 Joan Goyeau.
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
package org.apache.kafka.streams.scala.kstream

import java.time.Duration

import org.apache.kafka.streams.kstream.{SessionWindows, Suppressed => JSuppressed, TimeWindows, Windowed}
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.scala.utils.TestDriver
import org.apache.kafka.streams.scala.{ByteArrayKeyValueStore, StreamsBuilder}
import org.junit.runner.RunWith
import org.scalatest.{FlatSpec, Matchers}
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class KTableTest extends FlatSpec with Matchers with TestDriver {

  "filter a KTable" should "filter records satisfying the predicate" in {
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
      record.key shouldBe "a"
      record.value shouldBe 1
    }
    {
      testInput.pipeInput("a", "fails filter : remove existing row from table")
      val record = testOutput.readKeyValue
      record.key shouldBe "a"
      record.value shouldBe (null: java.lang.Long)
    }
    {
      testInput.pipeInput("b", "fails filter : no output")
      testOutput.isEmpty shouldBe true
    }
    testOutput.isEmpty shouldBe true

    testDriver.close()
  }

  "filterNot a KTable" should "filter records not satisfying the predicate" in {
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
      record.key shouldBe "1"
      record.value shouldBe 1
    }
    {
      testInput.pipeInput("1", "value2")
      val record = testOutput.readKeyValue
      record.key shouldBe "1"
      record.value shouldBe (null: java.lang.Long)
    }
    {
      testInput.pipeInput("2", "value1")
      val record = testOutput.readKeyValue
      record.key shouldBe "2"
      record.value shouldBe 1
    }
    testOutput.isEmpty shouldBe true

    testDriver.close()
  }

  "join 2 KTables" should "join correctly records" in {
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
    testOutput.readValue shouldBe 2

    testOutput.isEmpty shouldBe true

    testDriver.close()
  }

  "join 2 KTables with a Materialized" should "join correctly records and state store" in {
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
    testOutput.readValue shouldBe 2
    testDriver.getKeyValueStore[String, Long](stateStore).get("1") shouldBe 2

    testOutput.isEmpty shouldBe true

    testDriver.close()
  }

  "windowed KTable#suppress" should "correctly suppress results using Suppressed.untilTimeLimit" in {
    val builder = new StreamsBuilder()
    val sourceTopic = "source"
    val sinkTopic = "sink"
    val window = TimeWindows.of(Duration.ofSeconds(1L))
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
      testOutput.isEmpty shouldBe true
    }
    {
      // publish key=1 @ time 1 => count==2
      testInput.pipeInput("1", "value2", 1L)
      testOutput.isEmpty shouldBe true
    }
    {
      // move event time past the first window, but before the suppression window
      testInput.pipeInput("2", "value1", 1001L)
      testOutput.isEmpty shouldBe true
    }
    {
      // move event time riiiight before suppression window ends
      testInput.pipeInput("2", "value2", 1999L)
      testOutput.isEmpty shouldBe true
    }
    {
      // publish a late event before suppression window terminates => count==3
      testInput.pipeInput("1", "value3", 999L)
      testOutput.isEmpty shouldBe true
    }
    {
      // move event time right past the suppression window of the first window.
      testInput.pipeInput("2", "value3", 2001L)
      val record = testOutput.readKeyValue
      record.key shouldBe "0:1000:1"
      record.value shouldBe 3L
    }
    testOutput.isEmpty shouldBe true

    testDriver.close()
  }

  "windowed KTable#suppress" should "correctly suppress results using Suppressed.untilWindowCloses" in {
    val builder = new StreamsBuilder()
    val sourceTopic = "source"
    val sinkTopic = "sink"
    val window = TimeWindows.of(Duration.ofSeconds(1L)).grace(Duration.ofSeconds(1L))
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
      testOutput.isEmpty shouldBe true
    }
    {
      // publish key=1 @ time 1 => count==2
      testInput.pipeInput("1", "value2", 1L)
      testOutput.isEmpty shouldBe true
    }
    {
      // move event time past the window, but before the grace period
      testInput.pipeInput("2", "value1", 1001L)
      testOutput.isEmpty shouldBe true
    }
    {
      // move event time riiiight before grace period ends
      testInput.pipeInput("2", "value2", 1999L)
      testOutput.isEmpty shouldBe true
    }
    {
      // publish a late event before grace period terminates => count==3
      testInput.pipeInput("1", "value3", 999L)
      testOutput.isEmpty shouldBe true
    }
    {
      // move event time right past the grace period of the first window.
      testInput.pipeInput("2", "value3", 2001L)
      val record = testOutput.readKeyValue
      record.key shouldBe "0:1000:1"
      record.value shouldBe 3L
    }
    testOutput.isEmpty shouldBe true

    testDriver.close()
  }

  "session windowed KTable#suppress" should "correctly suppress results using Suppressed.untilWindowCloses" in {
    val builder = new StreamsBuilder()
    val sourceTopic = "source"
    val sinkTopic = "sink"
    // Very similar to SuppressScenarioTest.shouldSupportFinalResultsForSessionWindows
    val window = SessionWindows.`with`(Duration.ofMillis(5L)).grace(Duration.ofMillis(10L))
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
      testOutput.isEmpty shouldBe true
    }
    {
      // first window
      testInput.pipeInput("k1", "v1", 1L)
      testOutput.isEmpty shouldBe true
    }
    {
      // new window, but grace period hasn't ended for first window
      testInput.pipeInput("k1", "v1", 8L)
      testOutput.isEmpty shouldBe true
    }
    {
      // out-of-order event for first window, included since grade period hasn't passed
      testInput.pipeInput("k1", "v1", 2L)
      testOutput.isEmpty shouldBe true
    }
    {
      // add to second window
      testInput.pipeInput("k1", "v1", 13L)
      testOutput.isEmpty shouldBe true
    }
    {
      // add out-of-order to second window
      testInput.pipeInput("k1", "v1", 10L)
      testOutput.isEmpty shouldBe true
    }
    {
      // push stream time forward to flush other events through
      testInput.pipeInput("k1", "v1", 30L)
      // late event should get dropped from the stream
      testInput.pipeInput("k1", "v1", 3L)
      // should now have to results
      val r1 = testOutput.readRecord
      r1.key shouldBe "0:2:k1"
      r1.value shouldBe 3L
      r1.timestamp shouldBe 2L
      val r2 = testOutput.readRecord
      r2.key shouldBe "8:13:k1"
      r2.value shouldBe 3L
      r2.timestamp shouldBe 13L
    }
    testOutput.isEmpty shouldBe true

    testDriver.close()
  }

  "non-windowed KTable#suppress" should "correctly suppress results using Suppressed.untilTimeLimit" in {
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
      testOutput.isEmpty shouldBe true
    }
    {
      // publish key=1 @ time 1 => count==2
      testInput.pipeInput("1", "value2", 1L)
      testOutput.isEmpty shouldBe true
    }
    {
      // move event time past the window, but before the grace period
      testInput.pipeInput("2", "value1", 1001L)
      testOutput.isEmpty shouldBe true
    }
    {
      // move event time right before grace period ends
      testInput.pipeInput("2", "value2", 1999L)
      testOutput.isEmpty shouldBe true
    }
    {
      // publish a late event before grace period terminates => count==3
      testInput.pipeInput("1", "value3", 999L)
      testOutput.isEmpty shouldBe true
    }
    {
      // move event time right past the grace period of the first window.
      testInput.pipeInput("2", "value3", 2001L)
      val record = testOutput.readKeyValue
      record.key shouldBe "1"
      record.value shouldBe 3L
    }
    testOutput.isEmpty shouldBe true

    testDriver.close()
  }
}
