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

import java.time.Instant
import java.util.Properties

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.{StreamsConfig, TestInputTopic, TestOutputTopic, TopologyTestDriver}
import org.apache.kafka.test.TestUtils
import org.scalatest.Suite

trait TestDriver { this: Suite =>

  def createTestDriver(builder: StreamsBuilder, initialWallClockTime: Instant = Instant.now()): TopologyTestDriver = {
    val config = new Properties()
    config.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath)
    new TopologyTestDriver(builder.build(), config, initialWallClockTime)
  }

  implicit class TopologyTestDriverOps(inner: TopologyTestDriver) {
    def createInput[K, V](topic: String)(implicit serdeKey: Serde[K], serdeValue: Serde[V]): TestInputTopic[K, V] =
      inner.createInputTopic(topic, serdeKey.serializer, serdeValue.serializer)

    def createOutput[K, V](topic: String)(implicit serdeKey: Serde[K], serdeValue: Serde[V]): TestOutputTopic[K, V] =
      inner.createOutputTopic(topic, serdeKey.deserializer, serdeValue.deserializer)
  }
}
