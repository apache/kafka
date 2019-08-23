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

import org.apache.kafka.streams.kstream.{SessionWindows, TimeWindows, Windowed}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.kstream.Suppressed.BufferConfig
import org.apache.kafka.streams.scala.utils.TestDriver
import org.apache.kafka.streams.scala.{ByteArrayKeyValueStore, StreamsBuilder}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class KTableTest extends FlatSpec with Matchers with TestDriver {

  "filter a KTable" should "filter records satisfying the predicate" in {
    val builder = new StreamsBuilder()
    val sourceTopic = "source"
    val sinkTopic = "sink"

    val table = builder.stream[String, String](sourceTopic).groupBy((key, _) => key).count()
    table.filter((_, value) => value > 1).toStream.to(sinkTopic)

    val testDriver = createTestDriver(builder)

    {
      testDriver.pipeRecord(sourceTopic, ("1", "value1"))
      val record = testDriver.readRecord[String, Long](sinkTopic)
      record.key shouldBe "1"
      record.value shouldBe (null: java.lang.Long)
    }
    {
      testDriver.pipeRecord(sourceTopic, ("1", "value2"))
      val record = testDriver.readRecord[String, Long](sinkTopic)
      record.key shouldBe "1"
      record.value shouldBe 2
    }
    {
      testDriver.pipeRecord(sourceTopic, ("2", "value1"))
      val record = testDriver.readRecord[String, Long](sinkTopic)
      record.key shouldBe "2"
      record.value shouldBe (null: java.lang.Long)
    }
    testDriver.readRecord[String, Long](sinkTopic) shouldBe null

    testDriver.close()
  }

  "filterNot a KTable" should "filter records not satisfying the predicate" in {
    val builder = new StreamsBuilder()
    val sourceTopic = "source"
    val sinkTopic = "sink"

    val table = builder.stream[String, String](sourceTopic).groupBy((key, _) => key).count()
    table.filterNot((_, value) => value > 1).toStream.to(sinkTopic)

    val testDriver = createTestDriver(builder)

    {
      testDriver.pipeRecord(sourceTopic, ("1", "value1"))
      val record = testDriver.readRecord[String, Long](sinkTopic)
      record.key shouldBe "1"
      record.value shouldBe 1
    }
    {
      testDriver.pipeRecord(sourceTopic, ("1", "value2"))
      val record = testDriver.readRecord[String, Long](sinkTopic)
      record.key shouldBe "1"
      record.value shouldBe (null: java.lang.Long)
    }
    {
      testDriver.pipeRecord(sourceTopic, ("2", "value1"))
      val record = testDriver.readRecord[String, Long](sinkTopic)
      record.key shouldBe "2"
      record.value shouldBe 1
    }
    testDriver.readRecord[String, Long](sinkTopic) shouldBe null

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

    testDriver.pipeRecord(sourceTopic1, ("1", "topic1value1"))
    testDriver.pipeRecord(sourceTopic2, ("1", "topic2value1"))
    testDriver.readRecord[String, Long](sinkTopic).value shouldBe 2

    testDriver.readRecord[String, Long](sinkTopic) shouldBe null

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

    testDriver.pipeRecord(sourceTopic1, ("1", "topic1value1"))
    testDriver.pipeRecord(sourceTopic2, ("1", "topic2value1"))
    testDriver.readRecord[String, Long](sinkTopic).value shouldBe 2
    testDriver.getKeyValueStore[String, Long](stateStore).get("1") shouldBe 2

    testDriver.readRecord[String, Long](sinkTopic) shouldBe null

    testDriver.close()
  }

  "windowed KTable#suppress" should "correctly suppress results using Suppressed.untilTimeLimit" in {
    val builder = new StreamsBuilder()
    val sourceTopic = "source"
    val sinkTopic = "sink"
    val window = TimeWindows.of(Duration.ofSeconds(1L))
    val suppression = Suppressed.untilTimeLimit[Windowed[String]](Duration.ofSeconds(2L), BufferConfig.unbounded())

    val table: KTable[Windowed[String], Long] = builder
      .stream[String, String](sourceTopic)
      .groupByKey
      .windowedBy(window)
      .count
      .suppress(suppression)

    table.toStream((k, _) => s"${k.window().start()}:${k.window().end()}:${k.key()}").to(sinkTopic)

    val testDriver = createTestDriver(builder)

    {
      // publish key=1 @ time 0 => count==1
      testDriver.pipeRecord(sourceTopic, ("1", "value1"), 0L)
      Option(testDriver.readRecord[String, Long](sinkTopic)) shouldBe None
    }
    {
      // publish key=1 @ time 1 => count==2
      testDriver.pipeRecord(sourceTopic, ("1", "value2"), 1L)
      Option(testDriver.readRecord[String, Long](sinkTopic)) shouldBe None
    }
    {
      // move event time past the first window, but before the suppression window
      testDriver.pipeRecord(sourceTopic, ("2", "value1"), 1001L)
      Option(testDriver.readRecord[String, Long](sinkTopic)) shouldBe None
    }
    {
      // move event time riiiight before suppression window ends
      testDriver.pipeRecord(sourceTopic, ("2", "value2"), 1999L)
      Option(testDriver.readRecord[String, Long](sinkTopic)) shouldBe None
    }
    {
      // publish a late event before suppression window terminates => count==3
      testDriver.pipeRecord(sourceTopic, ("1", "value3"), 999L)
      Option(testDriver.readRecord[String, Long](sinkTopic)) shouldBe None
    }
    {
      // move event time right past the suppression window of the first window.
      testDriver.pipeRecord(sourceTopic, ("2", "value3"), 2001L)
      val record = testDriver.readRecord[String, Long](sinkTopic)
      record.key shouldBe "0:1000:1"
      record.value shouldBe 3L
    }
    testDriver.readRecord[String, Long](sinkTopic) shouldBe null

    testDriver.close()
  }

  "windowed KTable#suppress" should "correctly suppress results using Suppressed.untilWindowCloses" in {
    val builder = new StreamsBuilder()
    val sourceTopic = "source"
    val sinkTopic = "sink"
    val window = TimeWindows.of(Duration.ofSeconds(1L)).grace(Duration.ofSeconds(1L))
    val suppression = Suppressed.untilWindowCloses[String](BufferConfig.unbounded())

    val table: KTable[Windowed[String], Long] = builder
      .stream[String, String](sourceTopic)
      .groupByKey
      .windowedBy(window)
      .count
      .suppress(suppression)

    table.toStream((k, _) => s"${k.window().start()}:${k.window().end()}:${k.key()}").to(sinkTopic)

    val testDriver = createTestDriver(builder)

    {
      // publish key=1 @ time 0 => count==1
      testDriver.pipeRecord(sourceTopic, ("1", "value1"), 0L)
      Option(testDriver.readRecord[String, Long](sinkTopic)) shouldBe None
    }
    {
      // publish key=1 @ time 1 => count==2
      testDriver.pipeRecord(sourceTopic, ("1", "value2"), 1L)
      Option(testDriver.readRecord[String, Long](sinkTopic)) shouldBe None
    }
    {
      // move event time past the window, but before the grace period
      testDriver.pipeRecord(sourceTopic, ("2", "value1"), 1001L)
      Option(testDriver.readRecord[String, Long](sinkTopic)) shouldBe None
    }
    {
      // move event time riiiight before grace period ends
      testDriver.pipeRecord(sourceTopic, ("2", "value2"), 1999L)
      Option(testDriver.readRecord[String, Long](sinkTopic)) shouldBe None
    }
    {
      // publish a late event before grace period terminates => count==3
      testDriver.pipeRecord(sourceTopic, ("1", "value3"), 999L)
      Option(testDriver.readRecord[String, Long](sinkTopic)) shouldBe None
    }
    {
      // move event time right past the grace period of the first window.
      testDriver.pipeRecord(sourceTopic, ("2", "value3"), 2001L)
      val record = testDriver.readRecord[String, Long](sinkTopic)
      record.key shouldBe "0:1000:1"
      record.value shouldBe 3L
    }
    testDriver.readRecord[String, Long](sinkTopic) shouldBe null

    testDriver.close()
  }

  "session windowed KTable#suppress" should "correctly suppress results using Suppressed.untilWindowCloses" in {
    val builder = new StreamsBuilder()
    val sourceTopic = "source"
    val sinkTopic = "sink"
    // Very similar to SuppressScenarioTest.shouldSupportFinalResultsForSessionWindows
    val window = SessionWindows.`with`(Duration.ofMillis(5L)).grace(Duration.ofMillis(10L))
    val suppression = Suppressed.untilWindowCloses[String](BufferConfig.unbounded())

    val table: KTable[Windowed[String], Long] = builder
      .stream[String, String](sourceTopic)
      .groupByKey
      .windowedBy(window)
      .count
      .suppress(suppression)

    table.toStream((k, _) => s"${k.window().start()}:${k.window().end()}:${k.key()}").to(sinkTopic)

    val testDriver = createTestDriver(builder)

    {
      // first window
      testDriver.pipeRecord(sourceTopic, ("k1", "v1"), 0L)
      Option(testDriver.readRecord[String, Long](sinkTopic)) shouldBe None
    }
    {
      // first window
      testDriver.pipeRecord(sourceTopic, ("k1", "v1"), 1L)
      Option(testDriver.readRecord[String, Long](sinkTopic)) shouldBe None
    }
    {
      // new window, but grace period hasn't ended for first window
      testDriver.pipeRecord(sourceTopic, ("k1", "v1"), 8L)
      Option(testDriver.readRecord[String, Long](sinkTopic)) shouldBe None
    }
    {
      // out-of-order event for first window, included since grade period hasn't passed
      testDriver.pipeRecord(sourceTopic, ("k1", "v1"), 2L)
      Option(testDriver.readRecord[String, Long](sinkTopic)) shouldBe None
    }
    {
      // add to second window
      testDriver.pipeRecord(sourceTopic, ("k1", "v1"), 13L)
      Option(testDriver.readRecord[String, Long](sinkTopic)) shouldBe None
    }
    {
      // add out-of-order to second window
      testDriver.pipeRecord(sourceTopic, ("k1", "v1"), 10L)
      Option(testDriver.readRecord[String, Long](sinkTopic)) shouldBe None
    }
    {
      // push stream time forward to flush other events through
      testDriver.pipeRecord(sourceTopic, ("k1", "v1"), 30L)
      // late event should get dropped from the stream
      testDriver.pipeRecord(sourceTopic, ("k1", "v1"), 3L)
      // should now have to results
      val r1 = testDriver.readRecord[String, Long](sinkTopic)
      r1.key shouldBe "0:2:k1"
      r1.value shouldBe 3L
      r1.timestamp shouldBe 2L
      val r2 = testDriver.readRecord[String, Long](sinkTopic)
      r2.key shouldBe "8:13:k1"
      r2.value shouldBe 3L
      r2.timestamp shouldBe 13L
    }
    testDriver.readRecord[String, Long](sinkTopic) shouldBe null

    testDriver.close()
  }

  "non-windowed KTable#suppress" should "correctly suppress results using Suppressed.untilTimeLimit" in {
    val builder = new StreamsBuilder()
    val sourceTopic = "source"
    val sinkTopic = "sink"
    val suppression = Suppressed.untilTimeLimit[String](Duration.ofSeconds(2L), BufferConfig.unbounded())

    val table: KTable[String, Long] = builder
      .stream[String, String](sourceTopic)
      .groupByKey
      .count
      .suppress(suppression)

    table.toStream.to(sinkTopic)

    val testDriver = createTestDriver(builder)

    {
      // publish key=1 @ time 0 => count==1
      testDriver.pipeRecord(sourceTopic, ("1", "value1"), 0L)
      Option(testDriver.readRecord[String, Long](sinkTopic)) shouldBe None
    }
    {
      // publish key=1 @ time 1 => count==2
      testDriver.pipeRecord(sourceTopic, ("1", "value2"), 1L)
      Option(testDriver.readRecord[String, Long](sinkTopic)) shouldBe None
    }
    {
      // move event time past the window, but before the grace period
      testDriver.pipeRecord(sourceTopic, ("2", "value1"), 1001L)
      Option(testDriver.readRecord[String, Long](sinkTopic)) shouldBe None
    }
    {
      // move event time riiiight before grace period ends
      testDriver.pipeRecord(sourceTopic, ("2", "value2"), 1999L)
      Option(testDriver.readRecord[String, Long](sinkTopic)) shouldBe None
    }
    {
      // publish a late event before grace period terminates => count==3
      testDriver.pipeRecord(sourceTopic, ("1", "value3"), 999L)
      Option(testDriver.readRecord[String, Long](sinkTopic)) shouldBe None
    }
    {
      // move event time right past the grace period of the first window.
      testDriver.pipeRecord(sourceTopic, ("2", "value3"), 2001L)
      val record = testDriver.readRecord[String, Long](sinkTopic)
      record.key shouldBe "1"
      record.value shouldBe 3L
    }
    testDriver.readRecord[String, Long](sinkTopic) shouldBe null

    testDriver.close()
  }
}
