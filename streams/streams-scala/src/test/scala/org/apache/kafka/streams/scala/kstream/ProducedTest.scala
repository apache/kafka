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

import org.apache.kafka.streams.kstream.internals.ProducedInternal
import org.apache.kafka.streams.processor.StreamPartitioner
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

import java.util
import java.util.Optional

class ProducedTest {

  @Test
  def testCreateProducedWithSerdes(): Unit = {
    val produced: Produced[String, Long] = Produced.`with`[String, Long]

    val internalProduced = new ProducedInternal(produced)
    assertEquals(Serdes.stringSerde.getClass, internalProduced.keySerde.getClass)
    assertEquals(Serdes.longSerde.getClass, internalProduced.valueSerde.getClass)
  }

  @Test
  def testCreateProducedWithSerdesAndStreamPartitioner(): Unit = {
    val partitioner = new StreamPartitioner[String, Long] {
      override def partitions(
        topic: String,
        key: String,
        value: Long,
        numPartitions: Int
      ): Optional[util.Set[Integer]] = {
        val partitions = new util.HashSet[Integer]()
        partitions.add(Int.box(0))
        Optional.of(partitions)
      }
    }
    val produced: Produced[String, Long] = Produced.`with`(partitioner)

    val internalProduced = new ProducedInternal(produced)
    assertEquals(Serdes.stringSerde.getClass, internalProduced.keySerde.getClass)
    assertEquals(Serdes.longSerde.getClass, internalProduced.valueSerde.getClass)
    assertEquals(partitioner, internalProduced.streamPartitioner)
  }
}
