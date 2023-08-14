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
import org.apache.kafka.clients.consumer.{ConsumerConfig, OffsetAndMetadata}
import org.apache.kafka.common.errors.InvalidGroupIdException
import org.apache.kafka.common.{PartitionInfo, TopicPartition}
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertNull, assertThrows, assertTrue}
import org.junit.jupiter.api.{Disabled, Test}

import java.time.Duration
import scala.collection.immutable.{Map, Set}
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

  @Test
  def testFetchCommittedOffsets(): Unit = {
    val numRecords = 100
    val startingTimestamp = System.currentTimeMillis()
    val producer = createProducer()
    sendRecords(producer, numRecords, tp, startingTimestamp = startingTimestamp)
    val consumer = createAsyncConsumer()
    consumer.assign(List(tp).asJava)
    // First consumer consumes and commits offsets
    consumer.seek(tp, 0)
    consumeAndVerifyRecords(consumer = consumer, numRecords, startingOffset = 0,
      startingTimestamp = startingTimestamp)
    consumer.commitSync()
    assertEquals(numRecords, consumer.committed(Set(tp).asJava).get(tp).offset)
    // We should see the committed offsets from another consumer
    val anotherConsumer = createAsyncConsumer()
    anotherConsumer.assign(List(tp).asJava)
    assertEquals(numRecords, anotherConsumer.committed(Set(tp).asJava).get(tp).offset)
  }


  @Test
  def testConsumeFromCommittedOffsets(): Unit = {
    val producer = createProducer()
    val numRecords = 100
    val startingTimestamp = System.currentTimeMillis()
    sendRecords(producer, numRecords = numRecords, tp, startingTimestamp = startingTimestamp)

    // Commit offset with first consumer
    val consumer = createConsumerWithGroupId("group1")
    consumer.assign(List(tp).asJava)
    val offset = 10
    consumer.commitSync(Map[TopicPartition, OffsetAndMetadata]((tp, new OffsetAndMetadata(offset)))
      .asJava)
    assertEquals(offset, consumer.committed(Set(tp).asJava).get(tp).offset)
    consumer.close()

    // Consume from committed offsets with another consumer in same group
    val anotherConsumer = createConsumerWithGroupId("group1")
    assertEquals(offset, anotherConsumer.committed(Set(tp).asJava).get(tp).offset)
    anotherConsumer.assign(List(tp).asJava)
    consumeAndVerifyRecords(consumer = anotherConsumer, numRecords - offset,
      startingOffset = offset, startingKeyAndValueIndex = offset,
      startingTimestamp = startingTimestamp + offset)
  }

  @Test
  def testRetrievingCommittedOffsetsMultipleTimes(): Unit = {
    val numRecords = 100
    val startingTimestamp = System.currentTimeMillis()
    val producer = createProducer()
    sendRecords(producer, numRecords, tp, startingTimestamp = startingTimestamp)

    val consumer = createAsyncConsumer()
    consumer.assign(List(tp).asJava)

    // Consume and commit offsets
    consumer.seek(tp, 0)
    consumeAndVerifyRecords(consumer = consumer, numRecords, startingOffset = 0,
      startingTimestamp = startingTimestamp)
    consumer.commitSync()

    // Check committed offsets twice with same consumer
    assertEquals(numRecords, consumer.committed(Set(tp).asJava).get(tp).offset)
    assertEquals(numRecords, consumer.committed(Set(tp).asJava).get(tp).offset)
  }

  @Disabled("requires fix in KAFKA-15327")
  @Test
  def testAutoCommitOnCloseWithAssign(): Unit = {
    this.consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    val consumer = createAsyncConsumer()
    consumer.assign(List(tp, tp2).asJava)

    // should auto-commit seeked positions before closing
    consumer.seek(tp, 300)
    consumer.seek(tp2, 500)
    consumer.close()

    // we should see the committed offsets from another consumer
    val anotherConsumer = createAsyncConsumer()
    anotherConsumer.assign(List(tp, tp2).asJava)
    assertEquals(300, anotherConsumer.committed(Set(tp).asJava).get(tp).offset)
    assertEquals(500, anotherConsumer.committed(Set(tp2).asJava).get(tp2).offset)
  }

  @Test
  def testEmptyGroupNotSupported(): Unit = {
    // This replaces the existing testConsumingWithEmptyGroupId, given that empty group ID is not
    // supported in the new consumer implementation
    val consumer = createConsumerWithGroupId("")
    consumer.assign(List(tp).asJava)
    assertThrows(classOf[InvalidGroupIdException], () => consumer.commitSync())
  }
}