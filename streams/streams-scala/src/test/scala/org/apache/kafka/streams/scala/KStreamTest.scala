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
package org.apache.kafka.streams.scala

import org.apache.kafka.streams.kstream.JoinWindows
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.utils.TestDriver
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class KStreamTest extends FlatSpec with Matchers with TestDriver {

  "selectKey a KStream" should "select a new key" in {
    val builder = new StreamsBuilder()
    val sourceTopic = "source"
    val sinkTopic = "sink"

    builder.stream[String, String](sourceTopic).selectKey((_, value) => value).to(sinkTopic)

    val testDriver = createTestDriver(builder)

    testDriver.pipeRecord(sourceTopic, ("1", "value1"))
    testDriver.readRecord[String, String](sinkTopic).key shouldBe "value1"

    testDriver.pipeRecord(sourceTopic, ("1", "value2"))
    testDriver.readRecord[String, String](sinkTopic).key shouldBe "value2"

    testDriver.close()
  }

  "join 2 KStreams" should "join correctly records" in {
    val builder = new StreamsBuilder()
    val sourceTopic1 = "source1"
    val sourceTopic2 = "source2"
    val sinkTopic = "sink"

    val stream1 = builder.stream[String, String](sourceTopic1)
    val stream2 = builder.stream[String, String](sourceTopic2)
    stream1.join(stream2)((a, b) => s"$a-$b", JoinWindows.of(1000)).to(sinkTopic)

    val testDriver = createTestDriver(builder)

    testDriver.pipeRecord(sourceTopic1, ("1", "topic1value1"))
    testDriver.pipeRecord(sourceTopic2, ("1", "topic2value1"))
    testDriver.readRecord[String, String](sinkTopic).value shouldBe "topic1value1-topic2value1"

    testDriver.readRecord[String, String](sinkTopic) shouldBe null

    testDriver.close()
  }
}
