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

import org.apache.kafka.streams.kstream.internals.suppress._
import org.apache.kafka.streams.scala.kstream.Suppressed.BufferConfig
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

import java.time.Duration

@deprecated(message = "org.apache.kafka.streams.scala.kstream.Suppressed has been deprecated", since = "2.5")
class SuppressedTest {

  @Test
  def testProduceCorrectSuppressionUntilWindowCloses(): Unit = {
    val bufferConfig = BufferConfig.unbounded()
    val suppression = Suppressed.untilWindowCloses[String](bufferConfig)
    assertEquals(suppression, new FinalResultsSuppressionBuilder(null, bufferConfig))
    assertEquals(suppression.withName("soup"), new FinalResultsSuppressionBuilder("soup", bufferConfig))
  }

  @Test
  def testProduceCorrectSuppressionUntilTimeLimit(): Unit = {
    val bufferConfig = BufferConfig.unbounded()
    val duration = Duration.ofMillis(1)
    assertEquals(Suppressed.untilTimeLimit[String](duration, bufferConfig),
                 new SuppressedInternal[String](null, duration, bufferConfig, null, false))
  }

  @Test
  def testProduceCorrectBufferConfigWithMaxRecords(): Unit = {
    assertEquals(BufferConfig.maxRecords(4), new EagerBufferConfigImpl(4, Long.MaxValue))
    assertEquals(BufferConfig.maxRecords(4).withMaxBytes(5), new EagerBufferConfigImpl(4, 5))
  }

  @Test
  def testProduceCorrectBufferConfigWithMaxBytes(): Unit = {
    assertEquals(BufferConfig.maxBytes(4), new EagerBufferConfigImpl(Long.MaxValue, 4))
    assertEquals(BufferConfig.maxBytes(4).withMaxRecords(5), new EagerBufferConfigImpl(5, 4))
  }

  @Test
  def testProduceCorrectBufferConfigWithUnbounded(): Unit =
    assertEquals(BufferConfig.unbounded(),
                 new StrictBufferConfigImpl(Long.MaxValue, Long.MaxValue, BufferFullStrategy.SHUT_DOWN))

  @Test
  def testSupportLongChainsOfFactoryMethods(): Unit = {
    val bc1 = BufferConfig
      .unbounded()
      .emitEarlyWhenFull()
      .withMaxRecords(3L)
      .withMaxBytes(4L)
      .withMaxRecords(5L)
      .withMaxBytes(6L)
    assertEquals(new EagerBufferConfigImpl(5L, 6L), bc1)
    assertEquals(new StrictBufferConfigImpl(5L, 6L, BufferFullStrategy.SHUT_DOWN), bc1.shutDownWhenFull())

    val bc2 = BufferConfig
      .maxBytes(4)
      .withMaxRecords(5)
      .withMaxBytes(6)
      .withNoBound()
      .withMaxBytes(7)
      .withMaxRecords(8)

    assertEquals(new StrictBufferConfigImpl(8L, 7L, BufferFullStrategy.SHUT_DOWN), bc2)
    assertEquals(BufferConfig.unbounded(), bc2.withNoBound())

    val bc3 = BufferConfig
      .maxRecords(5L)
      .withMaxBytes(10L)
      .emitEarlyWhenFull()
      .withMaxRecords(11L)

    assertEquals(new EagerBufferConfigImpl(11L, 10L), bc3)
  }
}
