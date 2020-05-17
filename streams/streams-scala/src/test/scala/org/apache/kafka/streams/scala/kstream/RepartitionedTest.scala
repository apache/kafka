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

import org.apache.kafka.streams.kstream.Repartitioned
import org.apache.kafka.streams.kstream.internals.{ProducedInternal, RepartitionedInternal}
import org.apache.kafka.streams.processor.StreamPartitioner
import org.apache.kafka.streams.scala.Serdes
import org.apache.kafka.streams.scala.Serdes._
import org.junit.runner.RunWith
import org.scalatest.{FlatSpec, Matchers}
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RepartitionedTest extends FlatSpec with Matchers {

  "Create a Repartitioned" should "create a Repartitioned with Serdes" in {
    val repartitioned: Repartitioned[String, Long] = Repartitioned.`with`[String, Long]

    val internalRepartitioned = new RepartitionedInternal(repartitioned)
    internalRepartitioned.keySerde.getClass shouldBe Serdes.String.getClass
    internalRepartitioned.valueSerde.getClass shouldBe Serdes.Long.getClass
  }

  "Create a Repartitioned with numPartitions, topicName, and streamPartitioner" should "create a Repartitioned with Serdes, numPartitions, topicName and streamPartitioner" in {
    val partitioner = new StreamPartitioner[String, Long] {
      override def partition(topic: String, key: String, value: Long, numPartitions: Int): Integer = 0
    }
    val repartitioned: Repartitioned[String, Long] =
      Repartitioned.`numberOfPartitions`(5)
                   .withName("repartitionTopic")
                   .withStreamPartitioner(partitioner)

    val internalRepartitioned = new RepartitionedInternal(repartitioned)
    internalRepartitioned.keySerde.getClass shouldBe Serdes.String.getClass
    internalRepartitioned.valueSerde.getClass shouldBe Serdes.Long.getClass
    internalRepartitioned.numberOfPartitions shouldBe partitioner
    internalRepartitioned.name shouldBe "repartitionTopic"
    internalRepartitioned.streamPartitioner shouldBe partitioner
  }
}
