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

import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy
import org.apache.kafka.streams.AutoOffsetReset
import org.apache.kafka.streams.kstream.internals.ConsumedInternal
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class ConsumedTest {

  @Test
  def testCreateConsumed(): Unit = {
    val consumed: Consumed[String, Long] = Consumed.`with`[String, Long]

    val internalConsumed = new ConsumedInternal(consumed)
    assertEquals(Serdes.stringSerde.getClass, internalConsumed.keySerde.getClass)
    assertEquals(Serdes.longSerde.getClass, internalConsumed.valueSerde.getClass)
  }

  @Test
  def testCreateConsumedWithTimestampExtractorAndResetPolicy(): Unit = {
    val timestampExtractor = new FailOnInvalidTimestamp()
    val resetPolicy = AutoOffsetReset.latest()
    val consumed: Consumed[String, Long] =
      Consumed.`with`(timestampExtractor, resetPolicy)

    val internalConsumed = new ConsumedInternal(consumed)
    assertEquals(Serdes.stringSerde.getClass, internalConsumed.keySerde.getClass)
    assertEquals(Serdes.longSerde.getClass, internalConsumed.valueSerde.getClass)
    assertEquals(timestampExtractor, internalConsumed.timestampExtractor)
    assertEquals(AutoOffsetResetStrategy.StrategyType.LATEST, internalConsumed.offsetResetPolicy.offsetResetStrategy())
  }

  @Test
  def testCreateConsumedWithTimestampExtractor(): Unit = {
    val timestampExtractor = new FailOnInvalidTimestamp()
    val consumed: Consumed[String, Long] = Consumed.`with`[String, Long](timestampExtractor)

    val internalConsumed = new ConsumedInternal(consumed)
    assertEquals(Serdes.stringSerde.getClass, internalConsumed.keySerde.getClass)
    assertEquals(Serdes.longSerde.getClass, internalConsumed.valueSerde.getClass)
    assertEquals(timestampExtractor, internalConsumed.timestampExtractor)
  }

  @Test
  def testCreateConsumedWithResetPolicy(): Unit = {
    val resetPolicy = AutoOffsetReset.latest()
    val consumed: Consumed[String, Long] = Consumed.`with`[String, Long](resetPolicy)

    val internalConsumed = new ConsumedInternal(consumed)
    assertEquals(Serdes.stringSerde.getClass, internalConsumed.keySerde.getClass)
    assertEquals(Serdes.longSerde.getClass, internalConsumed.valueSerde.getClass)
    assertEquals(AutoOffsetResetStrategy.StrategyType.LATEST, internalConsumed.offsetResetPolicy.offsetResetStrategy())
  }
}
