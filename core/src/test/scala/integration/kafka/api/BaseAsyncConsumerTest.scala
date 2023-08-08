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
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.PartitionInfo
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.{assertNotNull, assertNull, assertTrue}
import org.junit.jupiter.api.Test

import java.time.Duration
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.jdk.CollectionConverters.SeqHasAsJava


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

    assertTrue(consumer.assignment.contains(tp))
    assertNotNull(committedOffset)
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
    assertNull(committedOffset.get(tp))
    assertTrue(consumer.assignment.contains(tp))
  }

  @Test
  def testCommitSyncAllConsumed(): Unit = {
    val numRecords = 10000

    val producer = createProducer()
    val startingTimestamp = System.currentTimeMillis()
    sendRecords(producer, numRecords, tp, startingTimestamp = startingTimestamp)

    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    consumer.seek(tp, 0)
    consumeAndVerifyRecords(consumer = consumer, numRecords, startingOffset = 0, startingTimestamp = startingTimestamp)

    consumer.commitSync()
    val committedOffset = consumer.committed(Set(tp).asJava)
    assertNotNull(committedOffset)
    assertNotNull(committedOffset.get(tp))
    assertEquals(numRecords, committedOffset.get(tp).offset())
  }

  @Test
  def testSimpleConsume(): Unit = {
    val numRecords = 10

    val producer = createProducer()
    val startingTimestamp = System.currentTimeMillis()
    sendRecords(producer, numRecords, tp, startingTimestamp = startingTimestamp)

    val consumer = createConsumer(configsToRemove = List(ConsumerConfig.GROUP_ID_CONFIG))
    consumer.assign(List(tp).asJava)
    consumeAndVerifyRecords(consumer = consumer, numRecords, startingOffset = 0, startingTimestamp = startingTimestamp)

    assertEquals(numRecords, consumer.position(tp))
  }

  @Test
  def testSimpleConsumeSkippingPosition(): Unit = {
    val numRecords = 10

    val producer = createProducer()
    val startingTimestamp = System.currentTimeMillis()
    sendRecords(producer, numRecords, tp, startingTimestamp = startingTimestamp)

    val consumer = createConsumer(configsToRemove = List(ConsumerConfig.GROUP_ID_CONFIG))
    consumer.assign(List(tp).asJava)
    val offset = 1
    consumer.seek(tp, offset)
    consumeAndVerifyRecords(consumer = consumer, numRecords - offset, startingOffset = offset,
      startingKeyAndValueIndex = offset, startingTimestamp = startingTimestamp + offset)

    assertEquals(numRecords, consumer.position(tp))
  }

  @Test
  def testSimpleConsumeWithLeaderChangeValidatingPositions(): Unit = {
    val numRecords = 10
    val producer = createProducer()
    val startingTimestamp = System.currentTimeMillis()
    sendRecords(producer, numRecords, tp, startingTimestamp = startingTimestamp)
    val consumer = createConsumer(configsToRemove = List(ConsumerConfig.GROUP_ID_CONFIG))
    consumer.assign(List(tp).asJava)
    consumeAndVerifyRecords(consumer = consumer, numRecords, startingOffset = 0, startingTimestamp = startingTimestamp)

    // Force leader epoch change to trigger position validation
    var parts: mutable.Buffer[PartitionInfo] = null
    while (parts == null)
      parts = consumer.partitionsFor(tp.topic()).asScala
    val leader = parts.head.leader().id()
    this.servers(leader).shutdown()
    this.servers(leader).startup()

    // Consume after leader change
    sendRecords(producer, numRecords, tp, startingTimestamp = startingTimestamp)
    consumeAndVerifyRecords(consumer = consumer, numRecords, startingOffset = 10,
      startingTimestamp = startingTimestamp)
  }
}
