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
package kafka.api

import kafka.utils.TestUtils.waitUntilTrue
import org.junit.jupiter.api.Assertions.{assertNotNull, assertNull, assertTrue}
import org.junit.jupiter.api.Test

import java.time.Duration
import scala.jdk.CollectionConverters._

class BaseAsyncConsumerTest extends AbstractConsumerTest {
  val defaultBlockingAPITimeoutMs = 1000

  @Test
  def testCommitAsync(): Unit = {
    val consumer = createAsyncConsumer()
    val producer = createProducer()
    val numRecords = 10000
    val startingTimestamp = System.currentTimeMillis()
    val cb = new CountConsumerCommitCallback
    sendRecords(producer, numRecords, tp, startingTimestamp = startingTimestamp)
    consumer.assign(List(tp).asJava)
    consumer.commitAsync(cb)
    waitUntilTrue(() => {
      cb.successCount == 1
    }, "wait until commit is completed successfully", defaultBlockingAPITimeoutMs)
    val committedOffset = consumer.committed(Set(tp).asJava, Duration.ofMillis(defaultBlockingAPITimeoutMs))
    assertNotNull(committedOffset)
    // No valid fetch position due to the absence of consumer.poll; and therefore no offset was committed to
    // tp. The committed offset should be null. This is intentional.
    assertNull(committedOffset.get(tp))
    assertTrue(consumer.assignment.contains(tp))
  }

  @Test
  def testCommitSync(): Unit = {
    val consumer = createAsyncConsumer()
    val producer = createProducer()
    val numRecords = 10000
    val startingTimestamp = System.currentTimeMillis()
    sendRecords(producer, numRecords, tp, startingTimestamp = startingTimestamp)
    consumer.assign(List(tp).asJava)
    consumer.commitSync()
    val committedOffset = consumer.committed(Set(tp).asJava, Duration.ofMillis(defaultBlockingAPITimeoutMs))
    assertNotNull(committedOffset)
    // No valid fetch position due to the absence of consumer.poll; and therefore no offset was committed to
    // tp. The committed offset should be null. This is intentional.
    assertNull(committedOffset.get(tp))
    assertTrue(consumer.assignment.contains(tp))
  }
}
