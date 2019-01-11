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
package org.apache.kafka.streams.scala.utils

import java.util.{Properties, UUID}

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.test.ConsumerRecordFactory
import org.apache.kafka.streams.{StreamsConfig, TopologyTestDriver}
import org.scalatest.Suite

trait TestDriver { this: Suite =>

  def createTestDriver(builder: StreamsBuilder,
                       initialWallClockTimeMs: Long = System.currentTimeMillis()): TopologyTestDriver = {
    val config = new Properties()
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test")
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")
    config.put(StreamsConfig.STATE_DIR_CONFIG, s"out/state-store-${UUID.randomUUID()}")
    new TopologyTestDriver(builder.build(), config, initialWallClockTimeMs)
  }

  implicit class TopologyTestDriverOps(inner: TopologyTestDriver) {
    def pipeRecord[K, V](topic: String, record: (K, V), timestampMs: Long = System.currentTimeMillis())(
      implicit serdeKey: Serde[K],
      serdeValue: Serde[V]
    ): Unit = {
      val recordFactory = new ConsumerRecordFactory[K, V](serdeKey.serializer, serdeValue.serializer)
      inner.pipeInput(recordFactory.create(topic, record._1, record._2, timestampMs))
    }

    def readRecord[K, V](topic: String)(implicit serdeKey: Serde[K], serdeValue: Serde[V]): ProducerRecord[K, V] =
      inner.readOutput(topic, serdeKey.deserializer, serdeValue.deserializer)
  }
}
