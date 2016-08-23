/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.tools

import kafka.consumer.BaseConsumerRecord
import org.apache.kafka.common.record.{Record, TimestampType}
import org.junit.Assert._
import org.junit.Test

class MirrorMakerTest {

  @Test
  def testDefaultMirrorMakerMessageHandler() {
    val now = 12345L
    val consumerRecord = BaseConsumerRecord("topic", 0, 1L, now, TimestampType.CREATE_TIME, "key".getBytes, "value".getBytes)

    val result = MirrorMaker.defaultMirrorMakerMessageHandler.handle(consumerRecord)
    assertEquals(1, result.size)

    val producerRecord = result.get(0)
    assertEquals(now, producerRecord.timestamp)
    assertEquals("topic", producerRecord.topic)
    assertNull(producerRecord.partition)
    assertEquals("key", new String(producerRecord.key))
    assertEquals("value", new String(producerRecord.value))
  }

  @Test
  def testDefaultMirrorMakerMessageHandlerWithNoTimestampInSourceMessage() {
    val consumerRecord = BaseConsumerRecord("topic", 0, 1L, Record.NO_TIMESTAMP, TimestampType.CREATE_TIME, "key".getBytes, "value".getBytes)

    val result = MirrorMaker.defaultMirrorMakerMessageHandler.handle(consumerRecord)
    assertEquals(1, result.size)

    val producerRecord = result.get(0)
    assertNull(producerRecord.timestamp)
    assertEquals("topic", producerRecord.topic)
    assertNull(producerRecord.partition)
    assertEquals("key", new String(producerRecord.key))
    assertEquals("value", new String(producerRecord.value))
  }

}
