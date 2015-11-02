/**
  * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
  * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
  * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
  * License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
  * specific language governing permissions and limitations under the License.
  */
package kafka.api

import java.util.regex.Pattern

import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer.{NoOffsetForPartitionException, OffsetAndMetadata, KafkaConsumer, ConsumerConfig}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.errors.{OffsetOutOfRangeException, RecordTooLargeException}
import org.junit.Assert._
import org.junit.Test
import scala.collection.JavaConverters
import JavaConverters._

/* We have some tests in this class instead of `BaseConsumerTest` in order to keep the build time under control. */
class PlaintextConsumerTest extends BaseConsumerTest {

  @Test
  def testAutoCommitOnClose() {
    this.consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    val consumer0 = new KafkaConsumer(this.consumerConfig, new ByteArrayDeserializer(), new ByteArrayDeserializer())

    val numRecords = 10000
    sendRecords(numRecords)

    consumer0.subscribe(List(topic).asJava)

    val assignment = Set(tp, tp2)
    TestUtils.waitUntilTrue(() => {
      consumer0.poll(50)
      consumer0.assignment() == assignment.asJava
    }, s"Expected partitions ${assignment.asJava} but actually got ${consumer0.assignment()}")

    // should auto-commit seeked positions before closing
    consumer0.seek(tp, 300)
    consumer0.seek(tp2, 500)
    consumer0.close()

    // now we should see the committed positions from another consumer
    assertEquals(300, this.consumers(0).committed(tp).offset)
    assertEquals(500, this.consumers(0).committed(tp2).offset)
  }

  @Test
  def testAutoCommitOnCloseAfterWakeup() {
    this.consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    val consumer0 = new KafkaConsumer(this.consumerConfig, new ByteArrayDeserializer(), new ByteArrayDeserializer())

    val numRecords = 10000
    sendRecords(numRecords)

    consumer0.subscribe(List(topic).asJava)

    val assignment = Set(tp, tp2)
    TestUtils.waitUntilTrue(() => {
      consumer0.poll(50)
      consumer0.assignment() == assignment.asJava
    }, s"Expected partitions ${assignment.asJava} but actually got ${consumer0.assignment()}")

    // should auto-commit seeked positions before closing
    consumer0.seek(tp, 300)
    consumer0.seek(tp2, 500)

    // wakeup the consumer before closing to simulate trying to break a poll
    // loop from another thread
    consumer0.wakeup()
    consumer0.close()

    // now we should see the committed positions from another consumer
    assertEquals(300, this.consumers(0).committed(tp).offset)
    assertEquals(500, this.consumers(0).committed(tp2).offset)
  }

  @Test
  def testAutoOffsetReset() {
    sendRecords(1)
    this.consumers(0).assign(List(tp).asJava)
    consumeAndVerifyRecords(this.consumers(0), numRecords = 1, startingOffset = 0)
  }

  @Test
  def testGroupConsumption() {
    sendRecords(10)
    this.consumers(0).subscribe(List(topic).asJava)
    consumeAndVerifyRecords(this.consumers(0), numRecords = 1, startingOffset = 0)
  }

  @Test
  def testPatternSubscription() {
    val numRecords = 10000
    sendRecords(numRecords)

    val topic1 = "tblablac" // matches subscribed pattern
    TestUtils.createTopic(this.zkUtils, topic1, 2, serverCount, this.servers)
    sendRecords(1000, new TopicPartition(topic1, 0))
    sendRecords(1000, new TopicPartition(topic1, 1))

    val topic2 = "tblablak" // does not match subscribed pattern
    TestUtils.createTopic(this.zkUtils, topic2, 2, serverCount, this.servers)
    sendRecords(1000, new TopicPartition(topic2, 0))
    sendRecords(1000, new TopicPartition(topic2, 1))

    val topic3 = "tblab1" // does not match subscribed pattern
    TestUtils.createTopic(this.zkUtils, topic3, 2, serverCount, this.servers)
    sendRecords(1000, new TopicPartition(topic3, 0))
    sendRecords(1000, new TopicPartition(topic3, 1))

    assertEquals(0, this.consumers(0).assignment().size)

    val pattern = Pattern.compile("t.*c")
    this.consumers(0).subscribe(pattern, new TestConsumerReassignmentListener)
    this.consumers(0).poll(50)

    var subscriptions = Set(
      new TopicPartition(topic, 0),
      new TopicPartition(topic, 1),
      new TopicPartition(topic1, 0),
      new TopicPartition(topic1, 1))

    TestUtils.waitUntilTrue(() => {
      this.consumers(0).poll(50)
      this.consumers(0).assignment() == subscriptions.asJava
    }, s"Expected partitions ${subscriptions.asJava} but actually got ${this.consumers(0).assignment()}")

    val topic4 = "tsomec" // matches subscribed pattern
    TestUtils.createTopic(this.zkUtils, topic4, 2, serverCount, this.servers)
    sendRecords(1000, new TopicPartition(topic4, 0))
    sendRecords(1000, new TopicPartition(topic4, 1))

    subscriptions ++= Set(
      new TopicPartition(topic4, 0),
      new TopicPartition(topic4, 1))


    TestUtils.waitUntilTrue(() => {
      this.consumers(0).poll(50)
      this.consumers(0).assignment() == subscriptions.asJava
    }, s"Expected partitions ${subscriptions.asJava} but actually got ${this.consumers(0).assignment()}")

    this.consumers(0).unsubscribe()
    assertEquals(0, this.consumers(0).assignment().size)
  }

  @Test
  def testPatternUnsubscription() {
    val numRecords = 10000
    sendRecords(numRecords)

    val topic1 = "tblablac" // matches subscribed pattern
    TestUtils.createTopic(this.zkUtils, topic1, 2, serverCount, this.servers)
    sendRecords(1000, new TopicPartition(topic1, 0))
    sendRecords(1000, new TopicPartition(topic1, 1))

    assertEquals(0, this.consumers(0).assignment().size)

    this.consumers(0).subscribe(Pattern.compile("t.*c"), new TestConsumerReassignmentListener)
    this.consumers(0).poll(50)

    val subscriptions = Set(
      new TopicPartition(topic, 0),
      new TopicPartition(topic, 1),
      new TopicPartition(topic1, 0),
      new TopicPartition(topic1, 1))

    TestUtils.waitUntilTrue(() => {
      this.consumers(0).poll(50)
      this.consumers(0).assignment() == subscriptions.asJava
    }, s"Expected partitions ${subscriptions.asJava} but actually got ${this.consumers(0).assignment()}")

    this.consumers(0).unsubscribe()
    assertEquals(0, this.consumers(0).assignment().size)
  }

  @Test
  def testCommitMetadata() {
    this.consumers(0).assign(List(tp).asJava)

    // sync commit
    val syncMetadata = new OffsetAndMetadata(5, "foo")
    this.consumers(0).commitSync(Map((tp, syncMetadata)).asJava)
    assertEquals(syncMetadata, this.consumers(0).committed(tp))

    // async commit
    val asyncMetadata = new OffsetAndMetadata(10, "bar")
    val callback = new CountConsumerCommitCallback
    this.consumers(0).commitAsync(Map((tp, asyncMetadata)).asJava, callback)
    awaitCommitCallback(this.consumers(0), callback)

    assertEquals(asyncMetadata, this.consumers(0).committed(tp))
  }

  @Test
  def testExpandingTopicSubscriptions() {
    val otherTopic = "other"
    val subscriptions = Set(new TopicPartition(topic, 0), new TopicPartition(topic, 1))
    val expandedSubscriptions = subscriptions ++ Set(new TopicPartition(otherTopic, 0), new TopicPartition(otherTopic, 1))
    this.consumers(0).subscribe(List(topic).asJava)
    TestUtils.waitUntilTrue(() => {
      this.consumers(0).poll(50)
      this.consumers(0).assignment == subscriptions.asJava
    }, s"Expected partitions ${subscriptions.asJava} but actually got ${this.consumers(0).assignment}")

    TestUtils.createTopic(this.zkUtils, otherTopic, 2, serverCount, this.servers)
    this.consumers(0).subscribe(List(topic, otherTopic).asJava)
    TestUtils.waitUntilTrue(() => {
      this.consumers(0).poll(50)
      this.consumers(0).assignment == expandedSubscriptions.asJava
    }, s"Expected partitions ${expandedSubscriptions.asJava} but actually got ${this.consumers(0).assignment}")
  }

  @Test
  def testShrinkingTopicSubscriptions() {
    val otherTopic = "other"
    TestUtils.createTopic(this.zkUtils, otherTopic, 2, serverCount, this.servers)
    val subscriptions = Set(new TopicPartition(topic, 0), new TopicPartition(topic, 1), new TopicPartition(otherTopic, 0), new TopicPartition(otherTopic, 1))
    val shrunkenSubscriptions = Set(new TopicPartition(topic, 0), new TopicPartition(topic, 1))
    this.consumers(0).subscribe(List(topic, otherTopic).asJava)
    TestUtils.waitUntilTrue(() => {
      this.consumers(0).poll(50)
      this.consumers(0).assignment == subscriptions.asJava
    }, s"Expected partitions ${subscriptions.asJava} but actually got ${this.consumers(0).assignment}")

    this.consumers(0).subscribe(List(topic).asJava)
    TestUtils.waitUntilTrue(() => {
      this.consumers(0).poll(50)
      this.consumers(0).assignment == shrunkenSubscriptions.asJava
    }, s"Expected partitions ${shrunkenSubscriptions.asJava} but actually got ${this.consumers(0).assignment}")
  }

  @Test
  def testPartitionsFor() {
    val numParts = 2
    TestUtils.createTopic(this.zkUtils, "part-test", numParts, 1, this.servers)
    val parts = this.consumers(0).partitionsFor("part-test")
    assertNotNull(parts)
    assertEquals(2, parts.size)
    assertNull(this.consumers(0).partitionsFor("non-exist-topic"))
  }

  @Test
  def testSeek() {
    val consumer = this.consumers(0)
    val totalRecords = 50L
    sendRecords(totalRecords.toInt)
    consumer.assign(List(tp).asJava)

    consumer.seekToEnd(tp)
    assertEquals(totalRecords, consumer.position(tp))
    assertFalse(consumer.poll(totalRecords).iterator().hasNext)

    consumer.seekToBeginning(tp)
    assertEquals(0, consumer.position(tp), 0)
    consumeAndVerifyRecords(consumer, numRecords = 1, startingOffset = 0)

    val mid = totalRecords / 2
    consumer.seek(tp, mid)
    assertEquals(mid, consumer.position(tp))
    consumeAndVerifyRecords(consumer, numRecords = 1, startingOffset = mid.toInt, startingKeyAndValueIndex = mid.toInt)
  }

  def testPositionAndCommit() {
    sendRecords(5)

    // committed() on a partition with no committed offset throws an exception
    intercept[NoOffsetForPartitionException] {
      this.consumers(0).committed(new TopicPartition(topic, 15))
    }

    // position() on a partition that we aren't subscribed to throws an exception
    intercept[IllegalArgumentException] {
      this.consumers(0).position(new TopicPartition(topic, 15))
    }

    this.consumers(0).assign(List(tp).asJava)

    assertEquals("position() on a partition that we are subscribed to should reset the offset", 0L, this.consumers(0).position(tp))
    this.consumers(0).commitSync()
    assertEquals(0L, this.consumers(0).committed(tp).offset)

    consumeAndVerifyRecords(this.consumers(0), 5, 0)
    assertEquals("After consuming 5 records, position should be 5", 5L, this.consumers(0).position(tp))
    this.consumers(0).commitSync()
    assertEquals("Committed offset should be returned", 5L, this.consumers(0).committed(tp).offset)

    sendRecords(1)

    // another consumer in the same group should get the same position
    this.consumers(1).assign(List(tp).asJava)
    consumeAndVerifyRecords(this.consumers(1), 1, 5)
  }

  @Test
  def testPartitionPauseAndResume() {
    sendRecords(5)
    this.consumers(0).assign(List(tp).asJava)
    consumeAndVerifyRecords(this.consumers(0), 5, 0)
    this.consumers(0).pause(tp)
    sendRecords(5)
    assertTrue(this.consumers(0).poll(0).isEmpty)
    this.consumers(0).resume(tp)
    consumeAndVerifyRecords(this.consumers(0), 5, 5)
  }

  @Test
  def testFetchInvalidOffset() {
    this.consumerConfig.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
    val consumer0 = new KafkaConsumer(this.consumerConfig, new ByteArrayDeserializer(), new ByteArrayDeserializer())

    // produce one record
    val totalRecords = 2
    sendRecords(totalRecords, tp)
    consumer0.assign(List(tp).asJava)

    // poll should fail because there is no offset reset strategy set
    intercept[NoOffsetForPartitionException] {
      consumer0.poll(50)
    }

    // seek to out of range position
    val outOfRangePos = totalRecords + 1
    consumer0.seek(tp, outOfRangePos)
    val e = intercept[OffsetOutOfRangeException] {
      consumer0.poll(20000)
    }
    val outOfRangePartitions = e.offsetOutOfRangePartitions()
    assertNotNull(outOfRangePartitions)
    assertEquals(1, outOfRangePartitions.size)
    assertEquals(outOfRangePos.toLong, outOfRangePartitions.get(tp))

    consumer0.close()
  }

  @Test
  def testFetchRecordTooLarge() {
    val maxFetchBytes = 10 * 1024
    this.consumerConfig.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, maxFetchBytes.toString)
    val consumer0 = new KafkaConsumer(this.consumerConfig, new ByteArrayDeserializer(), new ByteArrayDeserializer())

    // produce a record that is larger than the configured fetch size
    val record = new ProducerRecord[Array[Byte],Array[Byte]](tp.topic(), tp.partition(), "key".getBytes, new Array[Byte](maxFetchBytes + 1))
    this.producers(0).send(record)

    // consuming a too-large record should fail
    consumer0.assign(List(tp).asJava)
    val e = intercept[RecordTooLargeException] {
      consumer0.poll(20000)
    }
    val oversizedPartitions = e.recordTooLargePartitions()
    assertNotNull(oversizedPartitions)
    assertEquals(1, oversizedPartitions.size)
    // the oversized message is at offset 0
    assertEquals(0L, oversizedPartitions.get(tp))

    consumer0.close()
  }
}
