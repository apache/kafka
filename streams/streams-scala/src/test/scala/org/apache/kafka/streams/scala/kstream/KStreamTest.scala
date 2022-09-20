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

import java.time.Duration.ofSeconds
import java.time.{Duration, Instant}
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.{
  JoinWindows,
  Named,
  Transformer,
  ValueTransformer,
  ValueTransformerSupplier,
  ValueTransformerWithKey,
  ValueTransformerWithKeySupplier
}
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.utils.TestDriver
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

import scala.annotation.nowarn
import scala.jdk.CollectionConverters._

class KStreamTest extends TestDriver {

  @Test
  def testFilterRecordsSatisfyingPredicate(): Unit = {
    val builder = new StreamsBuilder()
    val sourceTopic = "source"
    val sinkTopic = "sink"

    builder.stream[String, String](sourceTopic).filter((_, value) => value != "value2").to(sinkTopic)

    val testDriver = createTestDriver(builder)
    val testInput = testDriver.createInput[String, String](sourceTopic)
    val testOutput = testDriver.createOutput[String, String](sinkTopic)

    testInput.pipeInput("1", "value1")
    assertEquals("value1", testOutput.readValue)

    testInput.pipeInput("2", "value2")
    assertTrue(testOutput.isEmpty)

    testInput.pipeInput("3", "value3")
    assertEquals("value3", testOutput.readValue)

    assertTrue(testOutput.isEmpty)

    testDriver.close()
  }

  @Test
  def testFilterRecordsNotSatisfyingPredicate(): Unit = {
    val builder = new StreamsBuilder()
    val sourceTopic = "source"
    val sinkTopic = "sink"

    builder.stream[String, String](sourceTopic).filterNot((_, value) => value == "value2").to(sinkTopic)

    val testDriver = createTestDriver(builder)
    val testInput = testDriver.createInput[String, String](sourceTopic)
    val testOutput = testDriver.createOutput[String, String](sinkTopic)

    testInput.pipeInput("1", "value1")
    assertEquals("value1", testOutput.readValue)

    testInput.pipeInput("2", "value2")
    assertTrue(testOutput.isEmpty)

    testInput.pipeInput("3", "value3")
    assertEquals("value3", testOutput.readValue)

    assertTrue(testOutput.isEmpty)

    testDriver.close()
  }

  @Test
  def testForeachActionsOnRecords(): Unit = {
    val builder = new StreamsBuilder()
    val sourceTopic = "source"

    var acc = ""
    builder.stream[String, String](sourceTopic).foreach((_, value) => acc += value)

    val testDriver = createTestDriver(builder)
    val testInput = testDriver.createInput[String, String](sourceTopic)

    testInput.pipeInput("1", "value1")
    assertEquals("value1", acc)

    testInput.pipeInput("2", "value2")
    assertEquals("value1value2", acc)

    testDriver.close()
  }

  @Test
  def testPeekActionsOnRecords(): Unit = {
    val builder = new StreamsBuilder()
    val sourceTopic = "source"
    val sinkTopic = "sink"

    var acc = ""
    builder.stream[String, String](sourceTopic).peek((_, v) => acc += v).to(sinkTopic)

    val testDriver = createTestDriver(builder)
    val testInput = testDriver.createInput[String, String](sourceTopic)
    val testOutput = testDriver.createOutput[String, String](sinkTopic)

    testInput.pipeInput("1", "value1")
    assertEquals("value1", acc)
    assertEquals("value1", testOutput.readValue)

    testInput.pipeInput("2", "value2")
    assertEquals("value1value2", acc)
    assertEquals("value2", testOutput.readValue)

    testDriver.close()
  }

  @Test
  def testSelectNewKey(): Unit = {
    val builder = new StreamsBuilder()
    val sourceTopic = "source"
    val sinkTopic = "sink"

    builder.stream[String, String](sourceTopic).selectKey((_, value) => value).to(sinkTopic)

    val testDriver = createTestDriver(builder)
    val testInput = testDriver.createInput[String, String](sourceTopic)
    val testOutput = testDriver.createOutput[String, String](sinkTopic)

    testInput.pipeInput("1", "value1")
    assertEquals("value1", testOutput.readKeyValue.key)

    testInput.pipeInput("1", "value2")
    assertEquals("value2", testOutput.readKeyValue.key)

    assertTrue(testOutput.isEmpty)

    testDriver.close()
  }

  @Test
  def testRepartitionKStream(): Unit = {
    val builder = new StreamsBuilder()
    val sourceTopic = "source"
    val repartitionName = "repartition"
    val sinkTopic = "sink"

    builder.stream[String, String](sourceTopic).repartition(Repartitioned.`with`(repartitionName)).to(sinkTopic)

    val testDriver = createTestDriver(builder)
    val testInput = testDriver.createInput[String, String](sourceTopic)
    val testOutput = testDriver.createOutput[String, String](sinkTopic)

    testInput.pipeInput("1", "value1")
    val kv1 = testOutput.readKeyValue
    assertEquals("1", kv1.key)
    assertEquals("value1", kv1.value)

    testInput.pipeInput("2", "value2")
    val kv2 = testOutput.readKeyValue
    assertEquals("2", kv2.key)
    assertEquals("value2", kv2.value)

    assertTrue(testOutput.isEmpty)

    // appId == "test"
    testDriver.producedTopicNames() contains "test-" + repartitionName + "-repartition"

    testDriver.close()
  }

  // noinspection ScalaDeprecation
  @Test
  def testJoinCorrectlyRecords(): Unit = {
    val builder = new StreamsBuilder()
    val sourceTopic1 = "source1"
    val sourceTopic2 = "source2"
    val sinkTopic = "sink"

    val stream1 = builder.stream[String, String](sourceTopic1)
    val stream2 = builder.stream[String, String](sourceTopic2)
    stream1
      .join(stream2)((a, b) => s"$a-$b", JoinWindows.ofTimeDifferenceAndGrace(ofSeconds(1), Duration.ofHours(24)))
      .to(sinkTopic)

    val now = Instant.now()

    val testDriver = createTestDriver(builder, now)
    val testInput1 = testDriver.createInput[String, String](sourceTopic1)
    val testInput2 = testDriver.createInput[String, String](sourceTopic2)
    val testOutput = testDriver.createOutput[String, String](sinkTopic)

    testInput1.pipeInput("1", "topic1value1", now)
    testInput2.pipeInput("1", "topic2value1", now)

    assertEquals("topic1value1-topic2value1", testOutput.readValue)

    assertTrue(testOutput.isEmpty)

    testDriver.close()
  }

  @nowarn
  @Test
  def testTransformCorrectlyRecords(): Unit = {
    class TestTransformer extends Transformer[String, String, KeyValue[String, String]] {
      override def init(context: ProcessorContext): Unit = {}

      override def transform(key: String, value: String): KeyValue[String, String] =
        new KeyValue(s"$key-transformed", s"$value-transformed")

      override def close(): Unit = {}
    }
    val builder = new StreamsBuilder()
    val sourceTopic = "source"
    val sinkTopic = "sink"

    val stream = builder.stream[String, String](sourceTopic)
    stream
      .transform(() => new TestTransformer)
      .to(sinkTopic)

    val now = Instant.now()
    val testDriver = createTestDriver(builder, now)
    val testInput = testDriver.createInput[String, String](sourceTopic)
    val testOutput = testDriver.createOutput[String, String](sinkTopic)

    testInput.pipeInput("1", "value", now)

    val result = testOutput.readKeyValue()
    assertEquals("value-transformed", result.value)
    assertEquals("1-transformed", result.key)

    assertTrue(testOutput.isEmpty)

    testDriver.close()
  }

  @nowarn
  @Test
  def testFlatTransformCorrectlyRecords(): Unit = {
    class TestTransformer extends Transformer[String, String, Iterable[KeyValue[String, String]]] {
      override def init(context: ProcessorContext): Unit = {}

      override def transform(key: String, value: String): Iterable[KeyValue[String, String]] =
        Array(new KeyValue(s"$key-transformed", s"$value-transformed"))

      override def close(): Unit = {}
    }
    val builder = new StreamsBuilder()
    val sourceTopic = "source"
    val sinkTopic = "sink"

    val stream = builder.stream[String, String](sourceTopic)
    stream
      .flatTransform(() => new TestTransformer)
      .to(sinkTopic)

    val now = Instant.now()
    val testDriver = createTestDriver(builder, now)
    val testInput = testDriver.createInput[String, String](sourceTopic)
    val testOutput = testDriver.createOutput[String, String](sinkTopic)

    testInput.pipeInput("1", "value", now)

    val result = testOutput.readKeyValue()
    assertEquals("value-transformed", result.value)
    assertEquals("1-transformed", result.key)

    assertTrue(testOutput.isEmpty)

    testDriver.close()
  }

  @nowarn
  @Test
  def testCorrectlyFlatTransformValuesInRecords(): Unit = {
    class TestTransformer extends ValueTransformer[String, Iterable[String]] {
      override def init(context: ProcessorContext): Unit = {}

      override def transform(value: String): Iterable[String] =
        Array(s"$value-transformed")

      override def close(): Unit = {}
    }
    val builder = new StreamsBuilder()
    val sourceTopic = "source"
    val sinkTopic = "sink"

    val stream = builder.stream[String, String](sourceTopic)
    stream
      .flatTransformValues(new ValueTransformerSupplier[String, Iterable[String]] {
        def get(): ValueTransformer[String, Iterable[String]] =
          new TestTransformer
      })
      .to(sinkTopic)

    val now = Instant.now()
    val testDriver = createTestDriver(builder, now)
    val testInput = testDriver.createInput[String, String](sourceTopic)
    val testOutput = testDriver.createOutput[String, String](sinkTopic)

    testInput.pipeInput("1", "value", now)

    assertEquals("value-transformed", testOutput.readValue)

    assertTrue(testOutput.isEmpty)

    testDriver.close()
  }

  @nowarn
  @Test
  def testCorrectlyFlatTransformValuesInRecordsWithKey(): Unit = {
    class TestTransformer extends ValueTransformerWithKey[String, String, Iterable[String]] {
      override def init(context: ProcessorContext): Unit = {}

      override def transform(key: String, value: String): Iterable[String] =
        Array(s"$value-transformed-$key")

      override def close(): Unit = {}
    }
    val builder = new StreamsBuilder()
    val sourceTopic = "source"
    val sinkTopic = "sink"

    val stream = builder.stream[String, String](sourceTopic)
    stream
      .flatTransformValues(new ValueTransformerWithKeySupplier[String, String, Iterable[String]] {
        def get(): ValueTransformerWithKey[String, String, Iterable[String]] =
          new TestTransformer
      })
      .to(sinkTopic)

    val now = Instant.now()
    val testDriver = createTestDriver(builder, now)
    val testInput = testDriver.createInput[String, String](sourceTopic)
    val testOutput = testDriver.createOutput[String, String](sinkTopic)

    testInput.pipeInput("1", "value", now)

    assertEquals("value-transformed-1", testOutput.readValue)

    assertTrue(testOutput.isEmpty)

    testDriver.close()
  }

  @Test
  def testJoinTwoKStreamToTables(): Unit = {
    val builder = new StreamsBuilder()
    val sourceTopic1 = "source1"
    val sourceTopic2 = "source2"
    val sinkTopic = "sink"

    val table1 = builder.stream[String, String](sourceTopic1).toTable
    val table2 = builder.stream[String, String](sourceTopic2).toTable
    table1.join(table2)((a, b) => a + b).toStream.to(sinkTopic)

    val testDriver = createTestDriver(builder)
    val testInput1 = testDriver.createInput[String, String](sourceTopic1)
    val testInput2 = testDriver.createInput[String, String](sourceTopic2)
    val testOutput = testDriver.createOutput[String, String](sinkTopic)

    testInput1.pipeInput("1", "topic1value1")
    testInput2.pipeInput("1", "topic2value1")

    assertEquals("topic1value1topic2value1", testOutput.readValue)

    assertTrue(testOutput.isEmpty)

    testDriver.close()
  }

  @Test
  def testSettingNameOnFilter(): Unit = {
    val builder = new StreamsBuilder()
    val sourceTopic = "source"
    val sinkTopic = "sink"

    builder
      .stream[String, String](sourceTopic)
      .filter((_, value) => value != "value2", Named.as("my-name"))
      .to(sinkTopic)

    import scala.jdk.CollectionConverters._

    val filterNode = builder.build().describe().subtopologies().asScala.head.nodes().asScala.toList(1)
    assertEquals("my-name", filterNode.name())
  }

  @Test
  def testSettingNameOnOutputTable(): Unit = {
    val builder = new StreamsBuilder()
    val sourceTopic1 = "source1"
    val sinkTopic = "sink"

    builder
      .stream[String, String](sourceTopic1)
      .toTable(Named.as("my-name"))
      .toStream
      .to(sinkTopic)

    import scala.jdk.CollectionConverters._

    val tableNode = builder.build().describe().subtopologies().asScala.head.nodes().asScala.toList(1)
    assertEquals("my-name", tableNode.name())
  }

  @Test
  def testSettingNameOnJoin(): Unit = {
    val builder = new StreamsBuilder()
    val sourceTopic1 = "source"
    val sourceGTable = "table"
    val sinkTopic = "sink"

    val stream = builder.stream[String, String](sourceTopic1)
    val table = builder.globalTable[String, String](sourceGTable)
    stream
      .join(table, Named.as("my-name"))((a, b) => s"$a-$b", (a, b) => a + b)
      .to(sinkTopic)

    import scala.jdk.CollectionConverters._

    val joinNode = builder.build().describe().subtopologies().asScala.head.nodes().asScala.toList(1)
    assertEquals("my-name", joinNode.name())
  }

  @nowarn
  @Test
  def testSettingNameOnTransform(): Unit = {
    class TestTransformer extends Transformer[String, String, KeyValue[String, String]] {
      override def init(context: ProcessorContext): Unit = {}

      override def transform(key: String, value: String): KeyValue[String, String] =
        new KeyValue(s"$key-transformed", s"$value-transformed")

      override def close(): Unit = {}
    }
    val builder = new StreamsBuilder()
    val sourceTopic = "source"
    val sinkTopic = "sink"

    val stream = builder.stream[String, String](sourceTopic)
    stream
      .transform(() => new TestTransformer, Named.as("my-name"))
      .to(sinkTopic)

    val transformNode = builder.build().describe().subtopologies().asScala.head.nodes().asScala.toList(1)
    assertEquals("my-name", transformNode.name())
  }
}
