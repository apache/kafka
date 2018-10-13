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

import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes._
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
}
