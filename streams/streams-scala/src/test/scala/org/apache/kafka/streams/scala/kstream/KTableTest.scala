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

    val table = builder.table[String, String](sourceTopic).mapValues(_.length).filter((_, value) => value > 5)
    table.toStream.to(sinkTopic)

    val testDriver = createTestDriver(builder)

    {
      testDriver.pipeRecord(sourceTopic, ("1", "firstvalue"))
      val record = testDriver.readRecord[String, Int](sinkTopic)
      record.key shouldBe "1"
      record.value shouldBe 10
    }
    {
      testDriver.pipeRecord(sourceTopic, ("1", "secondvalue"))
      val record = testDriver.readRecord[String, Int](sinkTopic)
      record.key shouldBe "1"
      record.value shouldBe 11
    }
    {
      testDriver.pipeRecord(sourceTopic, ("1", "short"))
      val record = testDriver.readRecord[String, Int](sinkTopic)
      record.key shouldBe "1"
      record.value shouldBe (null: java.lang.Long)
    }
    {
      testDriver.pipeRecord(sourceTopic, ("2", "val3"))
      val record = testDriver.readRecord[String, Int](sinkTopic)
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

    val table = builder.table[String, String](sourceTopic).filterNot((_, value) => value.exists(_.isUpper))
    table.toStream.to(sinkTopic)

    val testDriver = createTestDriver(builder)

    {
      testDriver.pipeRecord(sourceTopic, ("1", "FirstValue"))
      val record = testDriver.readRecord[String, String](sinkTopic)
      record.key shouldBe "1"
      record.value shouldBe (null: java.lang.String)
    }
    {
      testDriver.pipeRecord(sourceTopic, ("1", "secondvalue"))
      val record = testDriver.readRecord[String, String](sinkTopic)
      record.key shouldBe "1"
      record.value shouldBe "secondvalue"
    }
    {
      testDriver.pipeRecord(sourceTopic, ("1", "Short"))
      val record = testDriver.readRecord[String, String](sinkTopic)
      record.key shouldBe "1"
      record.value shouldBe (null: java.lang.String)
    }
    {
      testDriver.pipeRecord(sourceTopic, ("2", "val"))
      val record = testDriver.readRecord[String, String](sinkTopic)
      record.key shouldBe "2"
      record.value shouldBe "val"
    }
    testDriver.readRecord[String, String](sinkTopic) shouldBe null

    testDriver.close()
  }

  "join 2 KTables" should "join correctly records" in {
    val builder = new StreamsBuilder()
    val sourceTopic1 = "source1"
    val sourceTopic2 = "source2"
    val sinkTopic = "sink"

    val table1 = builder.table[String, Int](sourceTopic1).mapValues((_, value) => value*2)
    val table2 = builder.table[String, Int](sourceTopic2).mapValues((_, value) => value+2)
    table1.join(table2)((a, b) => a + b).toStream.to(sinkTopic)

    val testDriver = createTestDriver(builder)

    testDriver.pipeRecord(sourceTopic1, ("1", 3))
    testDriver.pipeRecord(sourceTopic2, ("1", 2))
    testDriver.readRecord[String, Int](sinkTopic).value shouldBe 10

    testDriver.readRecord[String, Int](sinkTopic) shouldBe null

    testDriver.close()
  }

  "join 2 KTables with a Materialized" should "join correctly records and state store" in {
    val builder = new StreamsBuilder()
    val sourceTopic1 = "source1"
    val sourceTopic2 = "source2"
    val sinkTopic = "sink"
    val stateStore = "store"
    val materialized = Materialized.as[String, Int, ByteArrayKeyValueStore](stateStore)

    val table1 = builder.table[String, Int](sourceTopic1).mapValues((_, value) => value*2)
    val table2 = builder.table[String, Int](sourceTopic2).mapValues((_, value) => value+2)
    table1.join(table2, materialized)((a, b) => a + b).toStream.to(sinkTopic)

    val testDriver = createTestDriver(builder)

    testDriver.pipeRecord(sourceTopic1, ("1", 1))
    testDriver.pipeRecord(sourceTopic2, ("1", 3))
    testDriver.readRecord[String, Int](sinkTopic).value shouldBe 7

    testDriver.getKeyValueStore[String, Int](stateStore).get("1") shouldBe 7

    testDriver.readRecord[String, Int](sinkTopic) shouldBe null

    testDriver.close()
  }
}
