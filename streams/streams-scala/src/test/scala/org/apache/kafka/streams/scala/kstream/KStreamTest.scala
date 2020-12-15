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

import java.time.Duration.ofSeconds
import java.time.Instant

import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.{
  JoinWindows,
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
import org.junit.runner.RunWith
import org.scalatest.{FlatSpec, Matchers}
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class KStreamTest extends FlatSpec with Matchers with TestDriver {

  "filter a KStream" should "filter records satisfying the predicate" in {
    val builder = new StreamsBuilder()
    val sourceTopic = "source"
    val sinkTopic = "sink"

    builder.stream[String, String](sourceTopic).filter((_, value) => value != "value2").to(sinkTopic)

    val testDriver = createTestDriver(builder)
    val testInput = testDriver.createInput[String, String](sourceTopic)
    val testOutput = testDriver.createOutput[String, String](sinkTopic)

    testInput.pipeInput("1", "value1")
    testOutput.readValue shouldBe "value1"

    testInput.pipeInput("2", "value2")
    testOutput.isEmpty shouldBe true

    testInput.pipeInput("3", "value3")
    testOutput.readValue shouldBe "value3"

    testOutput.isEmpty shouldBe true

    testDriver.close()
  }

  "filterNot a KStream" should "filter records not satisfying the predicate" in {
    val builder = new StreamsBuilder()
    val sourceTopic = "source"
    val sinkTopic = "sink"

    builder.stream[String, String](sourceTopic).filterNot((_, value) => value == "value2").to(sinkTopic)

    val testDriver = createTestDriver(builder)
    val testInput = testDriver.createInput[String, String](sourceTopic)
    val testOutput = testDriver.createOutput[String, String](sinkTopic)

    testInput.pipeInput("1", "value1")
    testOutput.readValue shouldBe "value1"

    testInput.pipeInput("2", "value2")
    testOutput.isEmpty shouldBe true

    testInput.pipeInput("3", "value3")
    testOutput.readValue shouldBe "value3"

    testOutput.isEmpty shouldBe true

    testDriver.close()
  }

  "foreach a KStream" should "run foreach actions on records" in {
    val builder = new StreamsBuilder()
    val sourceTopic = "source"

    var acc = ""
    builder.stream[String, String](sourceTopic).foreach((_, value) => acc += value)

    val testDriver = createTestDriver(builder)
    val testInput = testDriver.createInput[String, String](sourceTopic)

    testInput.pipeInput("1", "value1")
    acc shouldBe "value1"

    testInput.pipeInput("2", "value2")
    acc shouldBe "value1value2"

    testDriver.close()
  }

  "peek a KStream" should "run peek actions on records" in {
    val builder = new StreamsBuilder()
    val sourceTopic = "source"
    val sinkTopic = "sink"

    var acc = ""
    builder.stream[String, String](sourceTopic).peek((_, v) => acc += v).to(sinkTopic)

    val testDriver = createTestDriver(builder)
    val testInput = testDriver.createInput[String, String](sourceTopic)
    val testOutput = testDriver.createOutput[String, String](sinkTopic)

    testInput.pipeInput("1", "value1")
    acc shouldBe "value1"
    testOutput.readValue shouldBe "value1"

    testInput.pipeInput("2", "value2")
    acc shouldBe "value1value2"
    testOutput.readValue shouldBe "value2"

    testDriver.close()
  }

  "selectKey a KStream" should "select a new key" in {
    val builder = new StreamsBuilder()
    val sourceTopic = "source"
    val sinkTopic = "sink"

    builder.stream[String, String](sourceTopic).selectKey((_, value) => value).to(sinkTopic)

    val testDriver = createTestDriver(builder)
    val testInput = testDriver.createInput[String, String](sourceTopic)
    val testOutput = testDriver.createOutput[String, String](sinkTopic)

    testInput.pipeInput("1", "value1")
    testOutput.readKeyValue.key shouldBe "value1"

    testInput.pipeInput("1", "value2")
    testOutput.readKeyValue.key shouldBe "value2"

    testOutput.isEmpty shouldBe true

    testDriver.close()
  }

  "repartition" should "repartition a KStream" in {
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
    kv1.key shouldBe "1"
    kv1.value shouldBe "value1"

    testInput.pipeInput("2", "value2")
    val kv2 = testOutput.readKeyValue
    kv2.key shouldBe "2"
    kv2.value shouldBe "value2"

    testOutput.isEmpty shouldBe true

    // appId == "test"
    testDriver.producedTopicNames() contains "test-" + repartitionName + "-repartition"

    testDriver.close()
  }

  "join 2 KStreams" should "join correctly records" in {
    val builder = new StreamsBuilder()
    val sourceTopic1 = "source1"
    val sourceTopic2 = "source2"
    val sinkTopic = "sink"

    val stream1 = builder.stream[String, String](sourceTopic1)
    val stream2 = builder.stream[String, String](sourceTopic2)
    stream1.join(stream2)((a, b) => s"$a-$b", JoinWindows.of(ofSeconds(1))).to(sinkTopic)

    val now = Instant.now()

    val testDriver = createTestDriver(builder, now)
    val testInput1 = testDriver.createInput[String, String](sourceTopic1)
    val testInput2 = testDriver.createInput[String, String](sourceTopic2)
    val testOutput = testDriver.createOutput[String, String](sinkTopic)

    testInput1.pipeInput("1", "topic1value1", now)
    testInput2.pipeInput("1", "topic2value1", now)

    testOutput.readValue shouldBe "topic1value1-topic2value1"

    testOutput.isEmpty shouldBe true

    testDriver.close()
  }

  "transform a KStream" should "transform correctly records" in {
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
    result.value shouldBe "value-transformed"
    result.key shouldBe "1-transformed"

    testOutput.isEmpty shouldBe true

    testDriver.close()
  }

  "flatTransform a KStream" should "flatTransform correctly records" in {
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
    result.value shouldBe "value-transformed"
    result.key shouldBe "1-transformed"

    testOutput.isEmpty shouldBe true

    testDriver.close()
  }

  "flatTransformValues a KStream" should "correctly flatTransform values in records" in {
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

    testOutput.readValue shouldBe "value-transformed"

    testOutput.isEmpty shouldBe true

    testDriver.close()
  }

  "flatTransformValues with key in a KStream" should "correctly flatTransformValues in records" in {
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

    testOutput.readValue shouldBe "value-transformed-1"

    testOutput.isEmpty shouldBe true

    testDriver.close()
  }

  "join 2 KStreamToTables" should "join correctly records" in {
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

    testOutput.readValue shouldBe "topic1value1topic2value1"

    testOutput.isEmpty shouldBe true

    testDriver.close()
  }
}
