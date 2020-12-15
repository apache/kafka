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

import org.apache.kafka.streams.kstream.internals.ProducedInternal
import org.apache.kafka.streams.processor.StreamPartitioner
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.scala.serialization.Serdes
import org.junit.runner.RunWith
import org.scalatest.{FlatSpec, Matchers}
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ProducedTest extends FlatSpec with Matchers {

  "Create a Produced" should "create a Produced with Serdes" in {
    val produced: Produced[String, Long] = Produced.`with`[String, Long]

    val internalProduced = new ProducedInternal(produced)
    internalProduced.keySerde.getClass shouldBe Serdes.stringSerde.getClass
    internalProduced.valueSerde.getClass shouldBe Serdes.longSerde.getClass
  }

  "Create a Produced with streamPartitioner" should "create a Produced with Serdes and streamPartitioner" in {
    val partitioner = new StreamPartitioner[String, Long] {
      override def partition(topic: String, key: String, value: Long, numPartitions: Int): Integer = 0
    }
    val produced: Produced[String, Long] = Produced.`with`(partitioner)

    val internalProduced = new ProducedInternal(produced)
    internalProduced.keySerde.getClass shouldBe Serdes.stringSerde.getClass
    internalProduced.valueSerde.getClass shouldBe Serdes.longSerde.getClass
    internalProduced.streamPartitioner shouldBe partitioner
  }
}
