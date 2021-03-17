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

import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.internals.ConsumedInternal
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.scala.serialization.Serdes
import org.junit.runner.RunWith
import org.scalatest.{FlatSpec, Matchers}
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ConsumedTest extends FlatSpec with Matchers {

  "Create a Consumed" should "create a Consumed with Serdes" in {
    val consumed: Consumed[String, Long] = Consumed.`with`[String, Long]

    val internalConsumed = new ConsumedInternal(consumed)
    internalConsumed.keySerde.getClass shouldBe Serdes.stringSerde.getClass
    internalConsumed.valueSerde.getClass shouldBe Serdes.longSerde.getClass
  }

  "Create a Consumed with timestampExtractor and resetPolicy" should "create a Consumed with Serdes, timestampExtractor and resetPolicy" in {
    val timestampExtractor = new FailOnInvalidTimestamp()
    val resetPolicy = Topology.AutoOffsetReset.LATEST
    val consumed: Consumed[String, Long] =
      Consumed.`with`[String, Long](timestampExtractor, resetPolicy)

    val internalConsumed = new ConsumedInternal(consumed)
    internalConsumed.keySerde.getClass shouldBe Serdes.stringSerde.getClass
    internalConsumed.valueSerde.getClass shouldBe Serdes.longSerde.getClass
    internalConsumed.timestampExtractor shouldBe timestampExtractor
    internalConsumed.offsetResetPolicy shouldBe resetPolicy
  }

  "Create a Consumed with timestampExtractor" should "create a Consumed with Serdes and timestampExtractor" in {
    val timestampExtractor = new FailOnInvalidTimestamp()
    val consumed: Consumed[String, Long] = Consumed.`with`[String, Long](timestampExtractor)

    val internalConsumed = new ConsumedInternal(consumed)
    internalConsumed.keySerde.getClass shouldBe Serdes.stringSerde.getClass
    internalConsumed.valueSerde.getClass shouldBe Serdes.longSerde.getClass
    internalConsumed.timestampExtractor shouldBe timestampExtractor
  }

  "Create a Consumed with resetPolicy" should "create a Consumed with Serdes and resetPolicy" in {
    val resetPolicy = Topology.AutoOffsetReset.LATEST
    val consumed: Consumed[String, Long] = Consumed.`with`[String, Long](resetPolicy)

    val internalConsumed = new ConsumedInternal(consumed)
    internalConsumed.keySerde.getClass shouldBe Serdes.stringSerde.getClass
    internalConsumed.valueSerde.getClass shouldBe Serdes.longSerde.getClass
    internalConsumed.offsetResetPolicy shouldBe resetPolicy
  }
}
