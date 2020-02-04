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

import java.time.Duration

import org.apache.kafka.streams.kstream.internals.suppress.{
  BufferFullStrategy,
  EagerBufferConfigImpl,
  FinalResultsSuppressionBuilder,
  StrictBufferConfigImpl,
  SuppressedInternal
}
import org.apache.kafka.streams.scala.kstream.Suppressed.BufferConfig
import org.junit.runner.RunWith
import org.scalatest.{FlatSpec, Matchers}
import org.scalatestplus.junit.JUnitRunner

@deprecated(message = "org.apache.kafka.streams.scala.kstream.Suppressed has been deprecated", since = "2.5")
@RunWith(classOf[JUnitRunner])
class SuppressedTest extends FlatSpec with Matchers {

  "Suppressed.untilWindowCloses" should "produce the correct suppression" in {
    val bufferConfig = BufferConfig.unbounded()
    val suppression = Suppressed.untilWindowCloses[String](bufferConfig)
    suppression shouldEqual new FinalResultsSuppressionBuilder(null, bufferConfig)
    suppression.withName("soup") shouldEqual new FinalResultsSuppressionBuilder("soup", bufferConfig)
  }

  "Suppressed.untilTimeLimit" should "produce the correct suppression" in {
    val bufferConfig = BufferConfig.unbounded()
    val duration = Duration.ofMillis(1)
    Suppressed.untilTimeLimit[String](duration, bufferConfig) shouldEqual
      new SuppressedInternal[String](null, duration, bufferConfig, null, false)
  }

  "BufferConfig.maxRecords" should "produce the correct buffer config" in {
    BufferConfig.maxRecords(4) shouldEqual new EagerBufferConfigImpl(4, Long.MaxValue)
    BufferConfig.maxRecords(4).withMaxBytes(5) shouldEqual new EagerBufferConfigImpl(4, 5)
  }

  "BufferConfig.maxBytes" should "produce the correct buffer config" in {
    BufferConfig.maxBytes(4) shouldEqual new EagerBufferConfigImpl(Long.MaxValue, 4)
    BufferConfig.maxBytes(4).withMaxRecords(5) shouldEqual new EagerBufferConfigImpl(5, 4)
  }

  "BufferConfig.unbounded" should "produce the correct buffer config" in {
    BufferConfig.unbounded() shouldEqual
      new StrictBufferConfigImpl(Long.MaxValue, Long.MaxValue, BufferFullStrategy.SHUT_DOWN)
  }

  "BufferConfig" should "support very long chains of factory methods" in {
    val bc1 = BufferConfig
      .unbounded()
      .emitEarlyWhenFull()
      .withMaxRecords(3L)
      .withMaxBytes(4L)
      .withMaxRecords(5L)
      .withMaxBytes(6L)
    bc1 shouldEqual new EagerBufferConfigImpl(5L, 6L)
    bc1.shutDownWhenFull() shouldEqual new StrictBufferConfigImpl(5L, 6L, BufferFullStrategy.SHUT_DOWN)

    val bc2 = BufferConfig
      .maxBytes(4)
      .withMaxRecords(5)
      .withMaxBytes(6)
      .withNoBound()
      .withMaxBytes(7)
      .withMaxRecords(8)

    bc2 shouldEqual new StrictBufferConfigImpl(8L, 7L, BufferFullStrategy.SHUT_DOWN)
    bc2.withNoBound() shouldEqual BufferConfig.unbounded()

    val bc3 = BufferConfig
      .maxRecords(5L)
      .withMaxBytes(10L)
      .emitEarlyWhenFull()
      .withMaxRecords(11L)

    bc3 shouldEqual new EagerBufferConfigImpl(11L, 10L)
  }
}
