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

import java.util
import java.util.regex.Pattern
import java.util.{Collections, Locale, Properties}

import kafka.log.LogConfig
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.{MetricName, TopicPartition}
import org.apache.kafka.common.errors.InvalidTopicException
import org.apache.kafka.common.record.{CompressionType, TimestampType}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, StringDeserializer, StringSerializer}
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.test.{MockConsumerInterceptor, MockProducerInterceptor}
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._
import scala.collection.mutable.Buffer

/* We have some tests in this class instead of `BaseConsumerTest` in order to keep the build time under control. */
class PlaintextConsumerTest extends BaseConsumerTest {

  @Test
  def testMaxPollRecords() {
    val maxPollRecords = 2
    val numRecords = 10000

    sendRecords(numRecords)

    this.consumerConfig.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords.toString)
    val consumer0 = new KafkaConsumer(this.consumerConfig, new ByteArrayDeserializer(), new ByteArrayDeserializer())
    consumers += consumer0

    consumer0.assign(List(tp).asJava)

    consumeAndVerifyRecords(consumer0, numRecords = numRecords, startingOffset = 0,
      maxPollRecords = maxPollRecords)
  }

  @Test
  def testMaxPollIntervalMs() {
    this.consumerConfig.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 3000.toString)
    this.consumerConfig.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 500.toString)
    this.consumerConfig.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 2000.toString)

    val consumer0 = new KafkaConsumer(this.consumerConfig, new ByteArrayDeserializer(), new ByteArrayDeserializer())
    consumers += consumer0

    val listener = new TestConsumerReassignmentListener()
    consumer0.subscribe(List(topic).asJava, listener)

    // poll once to get the initial assignment
    consumer0.poll(0)
    assertEquals(1, listener.callsToAssigned)
    assertEquals(1, listener.callsToRevoked)

    Thread.sleep(3500)

    // we should fall out of the group and need to rebalance
    consumer0.poll(0)
    assertEquals(2, listener.callsToAssigned)
    assertEquals(2, listener.callsToRevoked)
  }

  @Test
  def testMaxPollIntervalMsDelayInRevocation() {
    this.consumerConfig.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 5000.toString)
    this.consumerConfig.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 500.toString)
    this.consumerConfig.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 1000.toString)
    this.consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false.toString)

    val consumer0 = new KafkaConsumer(this.consumerConfig, new ByteArrayDeserializer(), new ByteArrayDeserializer())
    consumers += consumer0

    var commitCompleted = false
    var committedPosition: Long = -1

    val listener = new TestConsumerReassignmentListener {
      override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {
        if (callsToRevoked > 0) {
          // on the second rebalance (after we have joined the group initially), sleep longer
          // than session timeout and then try a commit. We should still be in the group,
          // so the commit should succeed
          Utils.sleep(1500)
          committedPosition = consumer0.position(tp)
          consumer0.commitSync(Map(tp -> new OffsetAndMetadata(committedPosition)).asJava)
          commitCompleted = true
        }
        super.onPartitionsRevoked(partitions)
      }
    }

    consumer0.subscribe(List(topic).asJava, listener)

    // poll once to join the group and get the initial assignment
    consumer0.poll(0)

    // force a rebalance to trigger an invocation of the revocation callback while in the group
    consumer0.subscribe(List("otherTopic").asJava, listener)
    consumer0.poll(0)

    assertEquals(0, committedPosition)
    assertTrue(commitCompleted)
  }

  @Test
  def testMaxPollIntervalMsDelayInAssignment() {
    this.consumerConfig.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 5000.toString)
    this.consumerConfig.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 500.toString)
    this.consumerConfig.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 1000.toString)
    this.consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false.toString)

    val consumer0 = new KafkaConsumer(this.consumerConfig, new ByteArrayDeserializer(), new ByteArrayDeserializer())
    consumers += consumer0

    val listener = new TestConsumerReassignmentListener {
      override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
        // sleep longer than the session timeout, we should still be in the group after invocation
        Utils.sleep(1500)
        super.onPartitionsAssigned(partitions)
      }
    }
    consumer0.subscribe(List(topic).asJava, listener)

    // poll once to join the group and get the initial assignment
    consumer0.poll(0)

    // we should still be in the group after this invocation
    consumer0.poll(0)

    assertEquals(1, listener.callsToAssigned)
    assertEquals(1, listener.callsToRevoked)
  }

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
    assertEquals(300, this.consumers.head.committed(tp).offset)
    assertEquals(500, this.consumers.head.committed(tp2).offset)
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
    assertEquals(300, this.consumers.head.committed(tp).offset)
    assertEquals(500, this.consumers.head.committed(tp2).offset)
  }

  @Test
  def testAutoOffsetReset() {
    sendRecords(1)
    this.consumers.head.assign(List(tp).asJava)
    consumeAndVerifyRecords(consumer = this.consumers.head, numRecords = 1, startingOffset = 0)
  }

  @Test
  def testGroupConsumption() {
    sendRecords(10)
    this.consumers.head.subscribe(List(topic).asJava)
    consumeAndVerifyRecords(consumer = this.consumers.head, numRecords = 1, startingOffset = 0)
  }

  /**
   * Verifies that pattern subscription performs as expected.
   * The pattern matches the topics 'topic' and 'tblablac', but not 'tblablak' or 'tblab1'.
   * It is expected that the consumer is subscribed to all partitions of 'topic' and
   * 'tblablac' after the subscription when metadata is refreshed.
   * When a new topic 'tsomec' is added afterwards, it is expected that upon the next
   * metadata refresh the consumer becomes subscribed to this new topic and all partitions
   * of that topic are assigned to it.
   */
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

    assertEquals(0, this.consumers.head.assignment().size)

    val pattern = Pattern.compile("t.*c")
    this.consumers.head.subscribe(pattern, new TestConsumerReassignmentListener)
    this.consumers.head.poll(50)

    var subscriptions = Set(
      new TopicPartition(topic, 0),
      new TopicPartition(topic, 1),
      new TopicPartition(topic1, 0),
      new TopicPartition(topic1, 1))

    TestUtils.waitUntilTrue(() => {
      this.consumers.head.poll(50)
      this.consumers.head.assignment() == subscriptions.asJava
    }, s"Expected partitions ${subscriptions.asJava} but actually got ${this.consumers.head.assignment()}")

    val topic4 = "tsomec" // matches subscribed pattern
    TestUtils.createTopic(this.zkUtils, topic4, 2, serverCount, this.servers)
    sendRecords(1000, new TopicPartition(topic4, 0))
    sendRecords(1000, new TopicPartition(topic4, 1))

    subscriptions ++= Set(
      new TopicPartition(topic4, 0),
      new TopicPartition(topic4, 1))


    TestUtils.waitUntilTrue(() => {
      this.consumers.head.poll(50)
      this.consumers.head.assignment() == subscriptions.asJava
    }, s"Expected partitions ${subscriptions.asJava} but actually got ${this.consumers.head.assignment()}")

    this.consumers.head.unsubscribe()
    assertEquals(0, this.consumers.head.assignment().size)
  }

  /**
   * Verifies that a second call to pattern subscription succeeds and performs as expected.
   * The initial subscription is to a pattern that matches two topics 'topic' and 'foo'.
   * The second subscription is to a pattern that matches 'foo' and a new topic 'bar'.
   * It is expected that the consumer is subscribed to all partitions of 'topic' and 'foo' after
   * the first subscription, and to all partitions of 'foo' and 'bar' after the second.
   * The metadata refresh interval is intentionally increased to a large enough value to guarantee
   * that it is the subscription call that triggers a metadata refresh, and not the timeout.
   */
  @Test
  def testSubsequentPatternSubscription() {
    this.consumerConfig.setProperty(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "30000")
    val consumer0 = new KafkaConsumer(this.consumerConfig, new ByteArrayDeserializer(), new ByteArrayDeserializer())
    consumers += consumer0

    val numRecords = 10000
    sendRecords(numRecords)

    // the first topic ('topic')  matches first subscription pattern only

    val fooTopic = "foo" // matches both subscription patterns
    TestUtils.createTopic(this.zkUtils, fooTopic, 1, serverCount, this.servers)
    sendRecords(1000, new TopicPartition(fooTopic, 0))

    assertEquals(0, consumer0.assignment().size)

    val pattern1 = Pattern.compile(".*o.*") // only 'topic' and 'foo' match this 
    consumer0.subscribe(pattern1, new TestConsumerReassignmentListener)
    consumer0.poll(50)

    var subscriptions = Set(
      new TopicPartition(topic, 0),
      new TopicPartition(topic, 1),
      new TopicPartition(fooTopic, 0))

    TestUtils.waitUntilTrue(() => {
      consumer0.poll(50)
      consumer0.assignment() == subscriptions.asJava
    }, s"Expected partitions ${subscriptions.asJava} but actually got ${consumer0.assignment()}")

    val barTopic = "bar" // matches the next subscription pattern
    TestUtils.createTopic(this.zkUtils, barTopic, 1, serverCount, this.servers)
    sendRecords(1000, new TopicPartition(barTopic, 0))

    val pattern2 = Pattern.compile("...") // only 'foo' and 'bar' match this
    consumer0.subscribe(pattern2, new TestConsumerReassignmentListener)
    consumer0.poll(50)

    subscriptions --= Set(
      new TopicPartition(topic, 0),
      new TopicPartition(topic, 1))

    subscriptions ++= Set(
      new TopicPartition(barTopic, 0))

    TestUtils.waitUntilTrue(() => {
      consumer0.poll(50)
      consumer0.assignment() == subscriptions.asJava
    }, s"Expected partitions ${subscriptions.asJava} but actually got ${consumer0.assignment()}")

    consumer0.unsubscribe()
    assertEquals(0, consumer0.assignment().size)
  }

  /**
   * Verifies that pattern unsubscription performs as expected.
   * The pattern matches the topics 'topic' and 'tblablac'.
   * It is expected that the consumer is subscribed to all partitions of 'topic' and
   * 'tblablac' after the subscription when metadata is refreshed.
   * When consumer unsubscribes from all its subscriptions, it is expected that its
   * assignments are cleared right away.
   */
  @Test
  def testPatternUnsubscription() {
    val numRecords = 10000
    sendRecords(numRecords)

    val topic1 = "tblablac" // matches the subscription pattern
    TestUtils.createTopic(this.zkUtils, topic1, 2, serverCount, this.servers)
    sendRecords(1000, new TopicPartition(topic1, 0))
    sendRecords(1000, new TopicPartition(topic1, 1))

    assertEquals(0, this.consumers.head.assignment().size)

    this.consumers.head.subscribe(Pattern.compile("t.*c"), new TestConsumerReassignmentListener)
    this.consumers.head.poll(50)

    val subscriptions = Set(
      new TopicPartition(topic, 0),
      new TopicPartition(topic, 1),
      new TopicPartition(topic1, 0),
      new TopicPartition(topic1, 1))

    TestUtils.waitUntilTrue(() => {
      this.consumers.head.poll(50)
      this.consumers.head.assignment() == subscriptions.asJava
    }, s"Expected partitions ${subscriptions.asJava} but actually got ${this.consumers.head.assignment()}")

    this.consumers.head.unsubscribe()
    assertEquals(0, this.consumers.head.assignment().size)
  }

  @Test
  def testCommitMetadata() {
    this.consumers.head.assign(List(tp).asJava)

    // sync commit
    val syncMetadata = new OffsetAndMetadata(5, "foo")
    this.consumers.head.commitSync(Map((tp, syncMetadata)).asJava)
    assertEquals(syncMetadata, this.consumers.head.committed(tp))

    // async commit
    val asyncMetadata = new OffsetAndMetadata(10, "bar")
    val callback = new CountConsumerCommitCallback
    this.consumers.head.commitAsync(Map((tp, asyncMetadata)).asJava, callback)
    awaitCommitCallback(this.consumers.head, callback)
    assertEquals(asyncMetadata, this.consumers.head.committed(tp))

    // handle null metadata
    val nullMetadata = new OffsetAndMetadata(5, null)
    this.consumers.head.commitSync(Map((tp, nullMetadata)).asJava)
    assertEquals(nullMetadata, this.consumers.head.committed(tp))
  }

  @Test
  def testAsyncCommit() {
    val consumer = this.consumers.head
    consumer.assign(List(tp).asJava)
    consumer.poll(0)

    val callback = new CountConsumerCommitCallback
    val count = 5
    for (i <- 1 to count)
      consumer.commitAsync(Map(tp -> new OffsetAndMetadata(i)).asJava, callback)

    awaitCommitCallback(consumer, callback, count=count)
    assertEquals(new OffsetAndMetadata(count), consumer.committed(tp))
  }

  @Test
  def testExpandingTopicSubscriptions() {
    val otherTopic = "other"
    val subscriptions = Set(new TopicPartition(topic, 0), new TopicPartition(topic, 1))
    val expandedSubscriptions = subscriptions ++ Set(new TopicPartition(otherTopic, 0), new TopicPartition(otherTopic, 1))
    this.consumers.head.subscribe(List(topic).asJava)
    TestUtils.waitUntilTrue(() => {
      this.consumers.head.poll(50)
      this.consumers.head.assignment == subscriptions.asJava
    }, s"Expected partitions ${subscriptions.asJava} but actually got ${this.consumers.head.assignment}")

    TestUtils.createTopic(this.zkUtils, otherTopic, 2, serverCount, this.servers)
    this.consumers.head.subscribe(List(topic, otherTopic).asJava)
    TestUtils.waitUntilTrue(() => {
      this.consumers.head.poll(50)
      this.consumers.head.assignment == expandedSubscriptions.asJava
    }, s"Expected partitions ${expandedSubscriptions.asJava} but actually got ${this.consumers.head.assignment}")
  }

  @Test
  def testShrinkingTopicSubscriptions() {
    val otherTopic = "other"
    TestUtils.createTopic(this.zkUtils, otherTopic, 2, serverCount, this.servers)
    val subscriptions = Set(new TopicPartition(topic, 0), new TopicPartition(topic, 1), new TopicPartition(otherTopic, 0), new TopicPartition(otherTopic, 1))
    val shrunkenSubscriptions = Set(new TopicPartition(topic, 0), new TopicPartition(topic, 1))
    this.consumers.head.subscribe(List(topic, otherTopic).asJava)
    TestUtils.waitUntilTrue(() => {
      this.consumers.head.poll(50)
      this.consumers.head.assignment == subscriptions.asJava
    }, s"Expected partitions ${subscriptions.asJava} but actually got ${this.consumers.head.assignment}")

    this.consumers.head.subscribe(List(topic).asJava)
    TestUtils.waitUntilTrue(() => {
      this.consumers.head.poll(50)
      this.consumers.head.assignment == shrunkenSubscriptions.asJava
    }, s"Expected partitions ${shrunkenSubscriptions.asJava} but actually got ${this.consumers.head.assignment}")
  }

  @Test
  def testPartitionsFor() {
    val numParts = 2
    TestUtils.createTopic(this.zkUtils, "part-test", numParts, 1, this.servers)
    val parts = this.consumers.head.partitionsFor("part-test")
    assertNotNull(parts)
    assertEquals(2, parts.size)
  }

  @Test
  def testPartitionsForAutoCreate() {
    val partitions = this.consumers.head.partitionsFor("non-exist-topic")
    assertFalse(partitions.isEmpty)
  }

  @Test(expected = classOf[InvalidTopicException])
  def testPartitionsForInvalidTopic() {
    this.consumers.head.partitionsFor(";3# ads,{234")
  }

  @Test
  def testSeek() {
    val consumer = this.consumers.head
    val totalRecords = 50L
    val mid = totalRecords / 2

    // Test seek non-compressed message
    sendRecords(totalRecords.toInt, tp)
    consumer.assign(List(tp).asJava)

    consumer.seekToEnd(List(tp).asJava)
    assertEquals(totalRecords, consumer.position(tp))
    assertFalse(consumer.poll(totalRecords).iterator().hasNext)

    consumer.seekToBeginning(List(tp).asJava)
    assertEquals(0, consumer.position(tp), 0)
    consumeAndVerifyRecords(consumer, numRecords = 1, startingOffset = 0)

    consumer.seek(tp, mid)
    assertEquals(mid, consumer.position(tp))

    consumeAndVerifyRecords(consumer, numRecords = 1, startingOffset = mid.toInt, startingKeyAndValueIndex = mid.toInt,
      startingTimestamp = mid.toLong)

    // Test seek compressed message
    sendCompressedMessages(totalRecords.toInt, tp2)
    consumer.assign(List(tp2).asJava)

    consumer.seekToEnd(List(tp2).asJava)
    assertEquals(totalRecords, consumer.position(tp2))
    assertFalse(consumer.poll(totalRecords).iterator().hasNext)

    consumer.seekToBeginning(List(tp2).asJava)
    assertEquals(0, consumer.position(tp2), 0)
    consumeAndVerifyRecords(consumer, numRecords = 1, startingOffset = 0, tp = tp2)

    consumer.seek(tp2, mid)
    assertEquals(mid, consumer.position(tp2))
    consumeAndVerifyRecords(consumer, numRecords = 1, startingOffset = mid.toInt, startingKeyAndValueIndex = mid.toInt,
      startingTimestamp = mid.toLong, tp = tp2)
  }

  private def sendCompressedMessages(numRecords: Int, tp: TopicPartition) {
    val producerProps = new Properties()
    producerProps.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, CompressionType.GZIP.name)
    producerProps.setProperty(ProducerConfig.LINGER_MS_CONFIG, Long.MaxValue.toString)
    val producer = TestUtils.createNewProducer(brokerList, securityProtocol = securityProtocol, trustStoreFile = trustStoreFile,
        saslProperties = clientSaslProperties, retries = 0, lingerMs = Long.MaxValue, props = Some(producerProps))
    (0 until numRecords).foreach { i =>
      producer.send(new ProducerRecord(tp.topic, tp.partition, i.toLong, s"key $i".getBytes, s"value $i".getBytes))
    }
    producer.close()
  }

  @Test
  def testPositionAndCommit() {
    sendRecords(5)

    assertNull(this.consumers.head.committed(new TopicPartition(topic, 15)))

    // position() on a partition that we aren't subscribed to throws an exception
    intercept[IllegalArgumentException] {
      this.consumers.head.position(new TopicPartition(topic, 15))
    }

    this.consumers.head.assign(List(tp).asJava)

    assertEquals("position() on a partition that we are subscribed to should reset the offset", 0L, this.consumers.head.position(tp))
    this.consumers.head.commitSync()
    assertEquals(0L, this.consumers.head.committed(tp).offset)

    consumeAndVerifyRecords(consumer = this.consumers.head, numRecords = 5, startingOffset = 0)
    assertEquals("After consuming 5 records, position should be 5", 5L, this.consumers.head.position(tp))
    this.consumers.head.commitSync()
    assertEquals("Committed offset should be returned", 5L, this.consumers.head.committed(tp).offset)

    sendRecords(1)

    // another consumer in the same group should get the same position
    this.consumers(1).assign(List(tp).asJava)
    consumeAndVerifyRecords(consumer = this.consumers(1), numRecords = 1, startingOffset = 5)
  }

  @Test
  def testPartitionPauseAndResume() {
    val partitions = List(tp).asJava
    sendRecords(5)
    this.consumers.head.assign(partitions)
    consumeAndVerifyRecords(consumer = this.consumers.head, numRecords = 5, startingOffset = 0)
    this.consumers.head.pause(partitions)
    sendRecords(5)
    assertTrue(this.consumers.head.poll(0).isEmpty)
    this.consumers.head.resume(partitions)
    consumeAndVerifyRecords(consumer = this.consumers.head, numRecords = 5, startingOffset = 5)
  }

  @Test
  def testFetchInvalidOffset() {
    this.consumerConfig.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none")
    val consumer0 = new KafkaConsumer(this.consumerConfig, new ByteArrayDeserializer(), new ByteArrayDeserializer())
    consumers += consumer0

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
  }

  @Test
  def testFetchRecordLargerThanFetchMaxBytes() {
    val maxFetchBytes = 10 * 1024
    this.consumerConfig.setProperty(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, maxFetchBytes.toString)
    checkLargeRecord(maxFetchBytes + 1)
  }

  private def checkLargeRecord(producerRecordSize: Int): Unit = {
    val consumer0 = new KafkaConsumer(this.consumerConfig, new ByteArrayDeserializer(), new ByteArrayDeserializer())
    consumers += consumer0

    // produce a record that is larger than the configured fetch size
    val record = new ProducerRecord(tp.topic(), tp.partition(), "key".getBytes,
      new Array[Byte](producerRecordSize))
    this.producers.head.send(record)

    // consuming a record that is too large should succeed since KIP-74
    consumer0.assign(List(tp).asJava)
    val records = consumer0.poll(20000)
    assertEquals(1, records.count)
    val consumerRecord = records.iterator().next()
    assertEquals(0L, consumerRecord.offset)
    assertEquals(tp.topic(), consumerRecord.topic())
    assertEquals(tp.partition(), consumerRecord.partition())
    assertArrayEquals(record.key(), consumerRecord.key())
    assertArrayEquals(record.value(), consumerRecord.value())
  }

  /** We should only return a large record if it's the first record in the first non-empty partition of the fetch request */
  @Test
  def testFetchHonoursFetchSizeIfLargeRecordNotFirst(): Unit = {
    val maxFetchBytes = 10 * 1024
    this.consumerConfig.setProperty(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, maxFetchBytes.toString)
    checkFetchHonoursSizeIfLargeRecordNotFirst(maxFetchBytes)
  }

  private def checkFetchHonoursSizeIfLargeRecordNotFirst(largeProducerRecordSize: Int): Unit = {
    val consumer0 = new KafkaConsumer(this.consumerConfig, new ByteArrayDeserializer(), new ByteArrayDeserializer())
    consumers += consumer0

    val smallRecord = new ProducerRecord(tp.topic(), tp.partition(), "small".getBytes,
      "value".getBytes)
    val largeRecord = new ProducerRecord(tp.topic(), tp.partition(), "large".getBytes,
      new Array[Byte](largeProducerRecordSize))
    this.producers.head.send(smallRecord)
    this.producers.head.send(largeRecord)

    // we should only get the small record in the first `poll`
    consumer0.assign(List(tp).asJava)
    val records = consumer0.poll(20000)
    assertEquals(1, records.count)
    val consumerRecord = records.iterator().next()
    assertEquals(0L, consumerRecord.offset)
    assertEquals(tp.topic(), consumerRecord.topic())
    assertEquals(tp.partition(), consumerRecord.partition())
    assertArrayEquals(smallRecord.key(), consumerRecord.key())
    assertArrayEquals(smallRecord.value(), consumerRecord.value())
  }

  /** We should only return a large record if it's the first record in the first partition of the fetch request */
  @Test
  def testFetchHonoursMaxPartitionFetchBytesIfLargeRecordNotFirst(): Unit = {
    val maxPartitionFetchBytes = 10 * 1024
    this.consumerConfig.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, maxPartitionFetchBytes.toString)
    checkFetchHonoursSizeIfLargeRecordNotFirst(maxPartitionFetchBytes)
  }

  @Test
  def testFetchRecordLargerThanMaxPartitionFetchBytes() {
    val maxPartitionFetchBytes = 10 * 1024
    this.consumerConfig.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, maxPartitionFetchBytes.toString)
    checkLargeRecord(maxPartitionFetchBytes + 1)
  }

  /** Test that we consume all partitions if fetch max bytes and max.partition.fetch.bytes are low */
  @Test
  def testLowMaxFetchSizeForRequestAndPartition(): Unit = {
    // one of the effects of this is that there will be some log reads where `0 > remaining limit bytes < message size`
    // and we don't return the message because it's not the first message in the first non-empty partition of the fetch
    // this behaves a little different than when remaining limit bytes is 0 and it's important to test it
    this.consumerConfig.setProperty(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "500")
    this.consumerConfig.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "100")
    val consumer0 = new KafkaConsumer(this.consumerConfig, new ByteArrayDeserializer(), new ByteArrayDeserializer())
    consumers += consumer0

    val topic1 = "topic1"
    val topic2 = "topic2"
    val topic3 = "topic3"
    val partitionCount = 30
    val topics = Seq(topic1, topic2, topic3)
    topics.foreach { topicName =>
      TestUtils.createTopic(zkUtils, topicName, partitionCount, serverCount, servers)
    }

    val partitions = topics.flatMap { topic =>
      (0 until partitionCount).map(new TopicPartition(topic, _))
    }

    assertEquals(0, consumer0.assignment().size)

    consumer0.subscribe(List(topic1, topic2, topic3).asJava)

    TestUtils.waitUntilTrue(() => {
      consumer0.poll(50)
      consumer0.assignment() == partitions.toSet.asJava
    }, s"Expected partitions ${partitions.asJava} but actually got ${consumer0.assignment}")

    val producerRecords = partitions.flatMap(sendRecords(partitionCount, _))
    val consumerRecords = consumeRecords(consumer0, producerRecords.size)

    val expected = producerRecords.map { record =>
      (record.topic, record.partition, new String(record.key), new String(record.value), record.timestamp)
    }.toSet

    val actual = consumerRecords.map { record =>
      (record.topic, record.partition, new String(record.key), new String(record.value), record.timestamp)
    }.toSet

    assertEquals(expected, actual)
  }

  @Test
  def testRoundRobinAssignment() {
    // 1 consumer using round-robin assignment
    this.consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "roundrobin-group")
    this.consumerConfig.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, classOf[RoundRobinAssignor].getName)
    val consumer0 = new KafkaConsumer(this.consumerConfig, new ByteArrayDeserializer(), new ByteArrayDeserializer())
    consumers += consumer0

    // create two new topics, each having 2 partitions
    val topic1 = "topic1"
    val topic2 = "topic2"
    val expectedAssignment = createTopicAndSendRecords(topic1, 2, 100) ++ createTopicAndSendRecords(topic2, 2, 100)

    assertEquals(0, consumer0.assignment().size)

    // subscribe to two topics
    consumer0.subscribe(List(topic1, topic2).asJava)
    TestUtils.waitUntilTrue(() => {
      consumer0.poll(50)
      consumer0.assignment() == expectedAssignment.asJava
    }, s"Expected partitions ${expectedAssignment.asJava} but actually got ${consumer0.assignment()}")

    // add one more topic with 2 partitions
    val topic3 = "topic3"
    createTopicAndSendRecords(topic3, 2, 100)

    val newExpectedAssignment = expectedAssignment ++ Set(new TopicPartition(topic3, 0), new TopicPartition(topic3, 1))
    consumer0.subscribe(List(topic1, topic2, topic3).asJava)
    TestUtils.waitUntilTrue(() => {
      consumer0.poll(50)
      consumer0.assignment() == newExpectedAssignment.asJava
    }, s"Expected partitions ${newExpectedAssignment.asJava} but actually got ${consumer0.assignment()}")

    // remove the topic we just added
    consumer0.subscribe(List(topic1, topic2).asJava)
    TestUtils.waitUntilTrue(() => {
      consumer0.poll(50)
      consumer0.assignment() == expectedAssignment.asJava
    }, s"Expected partitions ${expectedAssignment.asJava} but actually got ${consumer0.assignment()}")

    consumer0.unsubscribe()
    assertEquals(0, consumer0.assignment().size)
  }

  @Test
  def testMultiConsumerRoundRobinAssignment() {
    this.consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "roundrobin-group")
    this.consumerConfig.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, classOf[RoundRobinAssignor].getName)

    // create two new topics, total number of partitions must be greater than number of consumers
    val topic1 = "topic1"
    val topic2 = "topic2"
    val subscriptions = createTopicAndSendRecords(topic1, 5, 100) ++ createTopicAndSendRecords(topic2, 8, 100)

    // create a group of consumers, subscribe the consumers to all the topics and start polling
    // for the topic partition assignment
    val (_, consumerPollers) = createConsumerGroupAndWaitForAssignment(10, List(topic1, topic2), subscriptions)
    try {
      validateGroupAssignment(consumerPollers, subscriptions, s"Did not get valid initial assignment for partitions ${subscriptions.asJava}")

      // add one more consumer and validate re-assignment
      addConsumersToGroupAndWaitForGroupAssignment(1, consumers, consumerPollers, List(topic1, topic2), subscriptions)
    } finally {
      consumerPollers.foreach(_.shutdown())
    }
  }

  /**
   * This test re-uses BaseConsumerTest's consumers.
   * As a result, it is testing the default assignment strategy set by BaseConsumerTest
   */
  @Test
  def testMultiConsumerDefaultAssignment() {
    // use consumers and topics defined in this class + one more topic
    sendRecords(100, tp)
    sendRecords(100, tp2)
    val topic1 = "topic1"
    val subscriptions = Set(tp, tp2) ++ createTopicAndSendRecords(topic1, 5, 100)

    // subscribe all consumers to all topics and validate the assignment
    val consumerPollers = subscribeConsumers(consumers, List(topic, topic1))

    try {
      validateGroupAssignment(consumerPollers, subscriptions, s"Did not get valid initial assignment for partitions ${subscriptions.asJava}")

      // add 2 more consumers and validate re-assignment
      addConsumersToGroupAndWaitForGroupAssignment(2, consumers, consumerPollers, List(topic, topic1), subscriptions)

      // add one more topic and validate partition re-assignment
      val topic2 = "topic2"
      val expandedSubscriptions = subscriptions ++ createTopicAndSendRecords(topic2, 3, 100)
      changeConsumerGroupSubscriptionAndValidateAssignment(consumerPollers, List(topic, topic1, topic2), expandedSubscriptions)

      // remove the topic we just added and validate re-assignment
      changeConsumerGroupSubscriptionAndValidateAssignment(consumerPollers, List(topic, topic1), subscriptions)

    } finally {
      consumerPollers.foreach(_.shutdown())
    }
  }

  @Test
  def testMultiConsumerSessionTimeoutOnStopPolling(): Unit = {
    runMultiConsumerSessionTimeoutTest(false)
  }

  @Test
  def testMultiConsumerSessionTimeoutOnClose(): Unit = {
    runMultiConsumerSessionTimeoutTest(true)
  }

  @Test
  def testInterceptors() {
    val appendStr = "mock"
    MockConsumerInterceptor.resetCounters()
    MockProducerInterceptor.resetCounters()

    // create producer with interceptor
    val producerProps = new Properties()
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    producerProps.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "org.apache.kafka.test.MockProducerInterceptor")
    producerProps.put("mock.interceptor.append", appendStr)
    val testProducer = new KafkaProducer(producerProps, new StringSerializer, new StringSerializer)

    // produce records
    val numRecords = 10
    (0 until numRecords).map { i =>
      testProducer.send(new ProducerRecord(tp.topic, tp.partition, s"key $i", s"value $i"))
    }.foreach(_.get)
    assertEquals(numRecords, MockProducerInterceptor.ONSEND_COUNT.intValue)
    assertEquals(numRecords, MockProducerInterceptor.ON_SUCCESS_COUNT.intValue)
    // send invalid record
    try {
      testProducer.send(null)
      fail("Should not allow sending a null record")
    } catch {
      case _: Throwable =>
        assertEquals("Interceptor should be notified about exception", 1, MockProducerInterceptor.ON_ERROR_COUNT.intValue)
        assertEquals("Interceptor should not receive metadata with an exception when record is null", 0, MockProducerInterceptor.ON_ERROR_WITH_METADATA_COUNT.intValue())
    }

    // create consumer with interceptor
    this.consumerConfig.setProperty(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, "org.apache.kafka.test.MockConsumerInterceptor")
    val testConsumer = new KafkaConsumer(this.consumerConfig, new StringDeserializer, new StringDeserializer)
    testConsumer.assign(List(tp).asJava)
    testConsumer.seek(tp, 0)

    // consume and verify that values are modified by interceptors
    val records = consumeRecords(testConsumer, numRecords)
    for (i <- 0 until numRecords) {
      val record = records(i)
      assertEquals(s"key $i", new String(record.key))
      assertEquals(s"value $i$appendStr".toUpperCase(Locale.ROOT), new String(record.value))
    }

    // commit sync and verify onCommit is called
    val commitCountBefore = MockConsumerInterceptor.ON_COMMIT_COUNT.intValue
    testConsumer.commitSync(Map[TopicPartition, OffsetAndMetadata]((tp, new OffsetAndMetadata(2L))).asJava)
    assertEquals(2, testConsumer.committed(tp).offset)
    assertEquals(commitCountBefore + 1, MockConsumerInterceptor.ON_COMMIT_COUNT.intValue)

    // commit async and verify onCommit is called
    val commitCallback = new CountConsumerCommitCallback()
    testConsumer.commitAsync(Map[TopicPartition, OffsetAndMetadata]((tp, new OffsetAndMetadata(5L))).asJava, commitCallback)
    awaitCommitCallback(testConsumer, commitCallback)
    assertEquals(5, testConsumer.committed(tp).offset)
    assertEquals(commitCountBefore + 2, MockConsumerInterceptor.ON_COMMIT_COUNT.intValue)

    testConsumer.close()
    testProducer.close()

    // cleanup
    MockConsumerInterceptor.resetCounters()
    MockProducerInterceptor.resetCounters()
  }

  @Test
  def testAutoCommitIntercept() {
    val topic2 = "topic2"
    TestUtils.createTopic(this.zkUtils, topic2, 2, serverCount, this.servers)

    // produce records
    val numRecords = 100
    val testProducer = new KafkaProducer[String, String](this.producerConfig, new StringSerializer, new StringSerializer)
    (0 until numRecords).map { i =>
      testProducer.send(new ProducerRecord(tp.topic(), tp.partition(), s"key $i", s"value $i"))
    }.foreach(_.get)

    // create consumer with interceptor
    this.consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    this.consumerConfig.setProperty(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, "org.apache.kafka.test.MockConsumerInterceptor")
    val testConsumer = new KafkaConsumer[String, String](this.consumerConfig, new StringDeserializer(), new StringDeserializer())
    val rebalanceListener = new ConsumerRebalanceListener {
      override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]) = {
        // keep partitions paused in this test so that we can verify the commits based on specific seeks
        testConsumer.pause(partitions)
      }

      override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]) = {}
    }
    changeConsumerSubscriptionAndValidateAssignment(testConsumer, List(topic), Set(tp, tp2), rebalanceListener)
    testConsumer.seek(tp, 10)
    testConsumer.seek(tp2, 20)

    // change subscription to trigger rebalance
    val commitCountBeforeRebalance = MockConsumerInterceptor.ON_COMMIT_COUNT.intValue()
    changeConsumerSubscriptionAndValidateAssignment(testConsumer,
                                                    List(topic, topic2),
                                                    Set(tp, tp2, new TopicPartition(topic2, 0), new TopicPartition(topic2, 1)),
                                                    rebalanceListener)

    // after rebalancing, we should have reset to the committed positions
    assertEquals(10, testConsumer.committed(tp).offset)
    assertEquals(20, testConsumer.committed(tp2).offset)
    assertTrue(MockConsumerInterceptor.ON_COMMIT_COUNT.intValue() > commitCountBeforeRebalance)

    // verify commits are intercepted on close
    val commitCountBeforeClose = MockConsumerInterceptor.ON_COMMIT_COUNT.intValue()
    testConsumer.close()
    assertTrue(MockConsumerInterceptor.ON_COMMIT_COUNT.intValue() > commitCountBeforeClose)
    testProducer.close()

    // cleanup
    MockConsumerInterceptor.resetCounters()
  }

  @Test
  def testInterceptorsWithWrongKeyValue() {
    val appendStr = "mock"
    // create producer with interceptor that has different key and value types from the producer
    val producerProps = new Properties()
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    producerProps.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "org.apache.kafka.test.MockProducerInterceptor")
    producerProps.put("mock.interceptor.append", appendStr)
    val testProducer = new KafkaProducer(producerProps, new ByteArraySerializer(), new ByteArraySerializer())
    producers += testProducer

    // producing records should succeed
    testProducer.send(new ProducerRecord(tp.topic(), tp.partition(), s"key".getBytes, s"value will not be modified".getBytes))

    // create consumer with interceptor that has different key and value types from the consumer
    this.consumerConfig.setProperty(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, "org.apache.kafka.test.MockConsumerInterceptor")
    val testConsumer = new KafkaConsumer[Array[Byte], Array[Byte]](this.consumerConfig, new ByteArrayDeserializer(), new ByteArrayDeserializer())
    consumers += testConsumer

    testConsumer.assign(List(tp).asJava)
    testConsumer.seek(tp, 0)

    // consume and verify that values are not modified by interceptors -- their exceptions are caught and logged, but not propagated
    val records = consumeRecords(testConsumer, 1)
    val record = records.head
    assertEquals(s"value will not be modified", new String(record.value()))
  }

  def testConsumeMessagesWithCreateTime() {
    val numRecords = 50
    // Test non-compressed messages
    sendRecords(numRecords, tp)
    this.consumers.head.assign(List(tp).asJava)
    consumeAndVerifyRecords(consumer = this.consumers.head, numRecords = numRecords, startingOffset = 0, startingKeyAndValueIndex = 0,
      startingTimestamp = 0)

    // Test compressed messages
    sendCompressedMessages(numRecords, tp2)
    this.consumers.head.assign(List(tp2).asJava)
    consumeAndVerifyRecords(consumer = this.consumers.head, numRecords = numRecords, tp = tp2, startingOffset = 0, startingKeyAndValueIndex = 0,
      startingTimestamp = 0)
  }

  @Test
  def testConsumeMessagesWithLogAppendTime() {
    val topicName = "testConsumeMessagesWithLogAppendTime"
    val topicProps = new Properties()
    topicProps.setProperty(LogConfig.MessageTimestampTypeProp, "LogAppendTime")
    TestUtils.createTopic(zkUtils, topicName, 2, 2, servers, topicProps)

    val startTime = System.currentTimeMillis()
    val numRecords = 50

    // Test non-compressed messages
    val tp1 = new TopicPartition(topicName, 0)
    sendRecords(numRecords, tp1)
    this.consumers.head.assign(List(tp1).asJava)
    consumeAndVerifyRecords(consumer = this.consumers.head, numRecords = numRecords, tp = tp1, startingOffset = 0, startingKeyAndValueIndex = 0,
      startingTimestamp = startTime, timestampType = TimestampType.LOG_APPEND_TIME)

    // Test compressed messages
    val tp2 = new TopicPartition(topicName, 1)
    sendCompressedMessages(numRecords, tp2)
    this.consumers.head.assign(List(tp2).asJava)
    consumeAndVerifyRecords(consumer = this.consumers.head, numRecords = numRecords, tp = tp2, startingOffset = 0, startingKeyAndValueIndex = 0,
      startingTimestamp = startTime, timestampType = TimestampType.LOG_APPEND_TIME)
  }

  @Test
  def testListTopics() {
    val numParts = 2
    val topic1 = "part-test-topic-1"
    val topic2 = "part-test-topic-2"
    val topic3 = "part-test-topic-3"
    TestUtils.createTopic(this.zkUtils, topic1, numParts, 1, this.servers)
    TestUtils.createTopic(this.zkUtils, topic2, numParts, 1, this.servers)
    TestUtils.createTopic(this.zkUtils, topic3, numParts, 1, this.servers)

    val topics = this.consumers.head.listTopics()
    assertNotNull(topics)
    assertEquals(5, topics.size())
    assertEquals(5, topics.keySet().size())
    assertEquals(2, topics.get(topic1).size)
    assertEquals(2, topics.get(topic2).size)
    assertEquals(2, topics.get(topic3).size)
  }

  @Test
  def testOffsetsForTimes() {
    val numParts = 2
    val topic1 = "part-test-topic-1"
    val topic2 = "part-test-topic-2"
    val topic3 = "part-test-topic-3"
    val props = new Properties()
    props.setProperty(LogConfig.MessageFormatVersionProp, "0.9.0")
    TestUtils.createTopic(this.zkUtils, topic1, numParts, 1, this.servers)
    // Topic2 is in old message format.
    TestUtils.createTopic(this.zkUtils, topic2, numParts, 1, this.servers, props)
    TestUtils.createTopic(this.zkUtils, topic3, numParts, 1, this.servers)

    val consumer = this.consumers.head

    // Test negative target time
    intercept[IllegalArgumentException](
      consumer.offsetsForTimes(Collections.singletonMap(new TopicPartition(topic1, 0), -1)))

    val timestampsToSearch = new util.HashMap[TopicPartition, java.lang.Long]()
    var i = 0
    for (topic <- List(topic1, topic2, topic3)) {
      for (part <- 0 until numParts) {
        val tp = new TopicPartition(topic, part)
        // In sendRecords(), each message will have key, value and timestamp equal to the sequence number.
        sendRecords(100, tp)
        timestampsToSearch.put(tp, i * 20)
        i += 1
      }
    }
    // The timestampToSearch map should contain:
    // (topic1Partition0 -> 0,
    //  topic1Partitoin1 -> 20,
    //  topic2Partition0 -> 40,
    //  topic2Partition1 -> 60,
    //  topic3Partition0 -> 80,
    //  topic3Partition1 -> 100)
    val timestampOffsets = consumer.offsetsForTimes(timestampsToSearch)
    assertEquals(0, timestampOffsets.get(new TopicPartition(topic1, 0)).offset())
    assertEquals(0, timestampOffsets.get(new TopicPartition(topic1, 0)).timestamp())
    assertEquals(20, timestampOffsets.get(new TopicPartition(topic1, 1)).offset())
    assertEquals(20, timestampOffsets.get(new TopicPartition(topic1, 1)).timestamp())
    assertEquals("null should be returned when message format is 0.9.0",
      null, timestampOffsets.get(new TopicPartition(topic2, 0)))
    assertEquals("null should be returned when message format is 0.9.0",
      null, timestampOffsets.get(new TopicPartition(topic2, 1)))
    assertEquals(80, timestampOffsets.get(new TopicPartition(topic3, 0)).offset())
    assertEquals(80, timestampOffsets.get(new TopicPartition(topic3, 0)).timestamp())
    assertEquals(null, timestampOffsets.get(new TopicPartition(topic3, 1)))
  }

  @Test
  def testEarliestOrLatestOffsets() {
    val topic0 = "topicWithNewMessageFormat"
    val topic1 = "topicWithOldMessageFormat"
    createTopicAndSendRecords(topicName = topic0, numPartitions = 2, recordsPerPartition = 100)
    val props = new Properties()
    props.setProperty(LogConfig.MessageFormatVersionProp, "0.9.0")
    TestUtils.createTopic(this.zkUtils, topic1, numPartitions = 1, replicationFactor = 1, this.servers, props)
    sendRecords(100, new TopicPartition(topic1, 0))

    val t0p0 = new TopicPartition(topic0, 0)
    val t0p1 = new TopicPartition(topic0, 1)
    val t1p0 = new TopicPartition(topic1, 0)
    val partitions = Set(t0p0, t0p1, t1p0).asJava
    val consumer = this.consumers.head

    val earliests = consumer.beginningOffsets(partitions)
    assertEquals(0L, earliests.get(t0p0))
    assertEquals(0L, earliests.get(t0p1))
    assertEquals(0L, earliests.get(t1p0))

    val latests = consumer.endOffsets(partitions)
    assertEquals(100L, latests.get(t0p0))
    assertEquals(100L, latests.get(t0p1))
    assertEquals(100L, latests.get(t1p0))
  }

  @Test
  def testUnsubscribeTopic() {
    this.consumerConfig.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "100") // timeout quickly to avoid slow test
    this.consumerConfig.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "30")
    val consumer0 = new KafkaConsumer(this.consumerConfig, new ByteArrayDeserializer(), new ByteArrayDeserializer())
    consumers += consumer0

    val listener = new TestConsumerReassignmentListener()
    consumer0.subscribe(List(topic).asJava, listener)

    // the initial subscription should cause a callback execution
    while (listener.callsToAssigned == 0)
      consumer0.poll(50)

    consumer0.subscribe(List[String]().asJava)
    assertEquals(0, consumer0.assignment.size())
  }

  @Test
  def testPauseStateNotPreservedByRebalance() {
    this.consumerConfig.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "100") // timeout quickly to avoid slow test
    this.consumerConfig.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "30")
    val consumer0 = new KafkaConsumer(this.consumerConfig, new ByteArrayDeserializer(), new ByteArrayDeserializer())
    consumers += consumer0

    sendRecords(5)
    consumer0.subscribe(List(topic).asJava)
    consumeAndVerifyRecords(consumer = consumer0, numRecords = 5, startingOffset = 0)
    consumer0.pause(List(tp).asJava)

    // subscribe to a new topic to trigger a rebalance
    consumer0.subscribe(List("topic2").asJava)

    // after rebalance, our position should be reset and our pause state lost,
    // so we should be able to consume from the beginning
    consumeAndVerifyRecords(consumer = consumer0, numRecords = 0, startingOffset = 5)
  }

  @Test
  def testCommitSpecifiedOffsets() {
    sendRecords(5, tp)
    sendRecords(7, tp2)

    this.consumers.head.assign(List(tp, tp2).asJava)

    // Need to poll to join the group
    this.consumers.head.poll(50)
    val pos1 = this.consumers.head.position(tp)
    val pos2 = this.consumers.head.position(tp2)
    this.consumers.head.commitSync(Map[TopicPartition, OffsetAndMetadata]((tp, new OffsetAndMetadata(3L))).asJava)
    assertEquals(3, this.consumers.head.committed(tp).offset)
    assertNull(this.consumers.head.committed(tp2))

    // Positions should not change
    assertEquals(pos1, this.consumers.head.position(tp))
    assertEquals(pos2, this.consumers.head.position(tp2))
    this.consumers.head.commitSync(Map[TopicPartition, OffsetAndMetadata]((tp2, new OffsetAndMetadata(5L))).asJava)
    assertEquals(3, this.consumers.head.committed(tp).offset)
    assertEquals(5, this.consumers.head.committed(tp2).offset)

    // Using async should pick up the committed changes after commit completes
    val commitCallback = new CountConsumerCommitCallback()
    this.consumers.head.commitAsync(Map[TopicPartition, OffsetAndMetadata]((tp2, new OffsetAndMetadata(7L))).asJava, commitCallback)
    awaitCommitCallback(this.consumers.head, commitCallback)
    assertEquals(7, this.consumers.head.committed(tp2).offset)
  }

  @Test
  def testAutoCommitOnRebalance() {
    val topic2 = "topic2"
    TestUtils.createTopic(this.zkUtils, topic2, 2, serverCount, this.servers)

    this.consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    val consumer0 = new KafkaConsumer(this.consumerConfig, new ByteArrayDeserializer(), new ByteArrayDeserializer())
    consumers += consumer0

    val numRecords = 10000
    sendRecords(numRecords)

    val rebalanceListener = new ConsumerRebalanceListener {
      override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]) = {
        // keep partitions paused in this test so that we can verify the commits based on specific seeks
        consumer0.pause(partitions)
      }

      override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]) = {}
    }

    consumer0.subscribe(List(topic).asJava, rebalanceListener)

    val assignment = Set(tp, tp2)
    TestUtils.waitUntilTrue(() => {
      consumer0.poll(50)
      consumer0.assignment() == assignment.asJava
    }, s"Expected partitions ${assignment.asJava} but actually got ${consumer0.assignment()}")

    consumer0.seek(tp, 300)
    consumer0.seek(tp2, 500)

    // change subscription to trigger rebalance
    consumer0.subscribe(List(topic, topic2).asJava, rebalanceListener)

    val newAssignment = Set(tp, tp2, new TopicPartition(topic2, 0), new TopicPartition(topic2, 1))
    TestUtils.waitUntilTrue(() => {
      consumer0.poll(50)
      consumer0.assignment() == newAssignment.asJava
    }, s"Expected partitions ${newAssignment.asJava} but actually got ${consumer0.assignment()}")

    // after rebalancing, we should have reset to the committed positions
    assertEquals(300, consumer0.committed(tp).offset)
    assertEquals(500, consumer0.committed(tp2).offset)
  }


  @Test
  def testPerPartitionLagMetricsCleanUpWithSubscribe() {
    val numMessages = 1000
    val topic2 = "topic2"
    TestUtils.createTopic(this.zkUtils, topic2, 2, serverCount, this.servers)
    // send some messages.
    sendRecords(numMessages, tp)
    // Test subscribe
    // Create a consumer and consumer some messages.
    consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testPerPartitionLagMetricsCleanUpWithSubscribe")
    consumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "testPerPartitionLagMetricsCleanUpWithSubscribe")
    val consumer = new KafkaConsumer(this.consumerConfig, new ByteArrayDeserializer(), new ByteArrayDeserializer())
    try {
      val listener0 = new TestConsumerReassignmentListener
      consumer.subscribe(List(topic, topic2).asJava, listener0)
      var records: ConsumerRecords[Array[Byte], Array[Byte]] = ConsumerRecords.empty()
      TestUtils.waitUntilTrue(() => {
        records = consumer.poll(100)
        !records.records(tp).isEmpty
      }, "Consumer did not consume any message before timeout.")
      assertEquals("should be assigned once", 1, listener0.callsToAssigned)
      // Verify the metric exist.
      val tags = Collections.singletonMap("client-id", "testPerPartitionLagMetricsCleanUpWithSubscribe")
      val fetchLag0 = consumer.metrics.get(new MetricName(tp + ".records-lag", "consumer-fetch-manager-metrics", "", tags))
      assertNotNull(fetchLag0)
      val expectedLag = numMessages - records.count
      assertEquals(s"The lag should be $expectedLag", expectedLag, fetchLag0.value, epsilon)

      // Remove topic from subscription
      consumer.subscribe(List(topic2).asJava, listener0)
      TestUtils.waitUntilTrue(() => {
        consumer.poll(100)
        listener0.callsToAssigned >= 2
      }, "Expected rebalance did not occur.")
      // Verify the metric has gone
      assertNull(consumer.metrics.get(new MetricName(tp + ".records-lag", "consumer-fetch-manager-metrics", "", tags)))
      assertNull(consumer.metrics.get(new MetricName(tp2 + ".records-lag", "consumer-fetch-manager-metrics", "", tags)))
    } finally {
      consumer.close()
    }
  }

  @Test
  def testPerPartitionLagMetricsCleanUpWithAssign() {
    val numMessages = 1000
    // Test assign
    // send some messages.
    sendRecords(numMessages, tp)
    sendRecords(numMessages, tp2)
    consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testPerPartitionLagMetricsCleanUpWithAssign")
    consumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "testPerPartitionLagMetricsCleanUpWithAssign")
    val consumer = new KafkaConsumer(this.consumerConfig, new ByteArrayDeserializer(), new ByteArrayDeserializer())
    try {
      consumer.assign(List(tp).asJava)
      var records: ConsumerRecords[Array[Byte], Array[Byte]] = ConsumerRecords.empty()
      TestUtils.waitUntilTrue(() => {
        records = consumer.poll(100)
        !records.records(tp).isEmpty
      }, "Consumer did not consume any message before timeout.")
      // Verify the metric exist.
      val tags = Collections.singletonMap("client-id", "testPerPartitionLagMetricsCleanUpWithAssign")
      val fetchLag = consumer.metrics.get(new MetricName(tp + ".records-lag", "consumer-fetch-manager-metrics", "", tags))
      assertNotNull(fetchLag)
      val expectedLag = numMessages - records.count
      assertEquals(s"The lag should be $expectedLag", expectedLag, fetchLag.value, epsilon)

      consumer.assign(List(tp2).asJava)
      TestUtils.waitUntilTrue(() => !consumer.poll(100).isEmpty, "Consumer did not consume any message before timeout.")
      assertNull(consumer.metrics.get(new MetricName(tp + ".records-lag", "consumer-fetch-manager-metrics", "", tags)))
    } finally {
      consumer.close()
    }
  }

  @Test
  def testPerPartitionLagWithMaxPollRecords() {
    val numMessages = 1000
    val maxPollRecords = 10
    sendRecords(numMessages, tp)
    consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testPerPartitionLagWithMaxPollRecords")
    consumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "testPerPartitionLagWithMaxPollRecords")
    consumerConfig.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords.toString)
    val consumer = new KafkaConsumer(this.consumerConfig, new ByteArrayDeserializer(), new ByteArrayDeserializer())
    consumer.assign(List(tp).asJava)
    try {
      var records: ConsumerRecords[Array[Byte], Array[Byte]] = ConsumerRecords.empty()
      TestUtils.waitUntilTrue(() => {
        records = consumer.poll(100)
        !records.isEmpty
      }, "Consumer did not consume any message before timeout.")
      val tags = Collections.singletonMap("client-id", "testPerPartitionLagWithMaxPollRecords")
      val lag = consumer.metrics.get(new MetricName(tp + ".records-lag", "consumer-fetch-manager-metrics", "", tags))
      assertEquals(s"The lag should be ${numMessages - records.count}", numMessages - records.count, lag.value, epsilon)
    } finally {
      consumer.close()
    }
  }

  def runMultiConsumerSessionTimeoutTest(closeConsumer: Boolean): Unit = {
    // use consumers defined in this class plus one additional consumer
    // Use topic defined in this class + one additional topic
    sendRecords(100, tp)
    sendRecords(100, tp2)
    val topic1 = "topic1"
    val subscriptions = Set(tp, tp2) ++ createTopicAndSendRecords(topic1, 6, 100)

    // first subscribe consumers that are defined in this class
    val consumerPollers = Buffer[ConsumerAssignmentPoller]()
    for (consumer <- consumers)
      consumerPollers += subscribeConsumerAndStartPolling(consumer, List(topic, topic1))

    // create one more consumer and add it to the group; we will timeout this consumer
    val timeoutConsumer = new KafkaConsumer[Array[Byte], Array[Byte]](this.consumerConfig)
    // Close the consumer on test teardown, unless this test will manually
    if(!closeConsumer)
      consumers += timeoutConsumer
    val timeoutPoller = subscribeConsumerAndStartPolling(timeoutConsumer, List(topic, topic1))
    consumerPollers += timeoutPoller

    // validate the initial assignment
    validateGroupAssignment(consumerPollers, subscriptions, s"Did not get valid initial assignment for partitions ${subscriptions.asJava}")

    // stop polling and close one of the consumers, should trigger partition re-assignment among alive consumers
    timeoutPoller.shutdown()
    if (closeConsumer)
      timeoutConsumer.close()

    val maxSessionTimeout = this.serverConfig.getProperty(KafkaConfig.GroupMaxSessionTimeoutMsProp).toLong
    validateGroupAssignment(consumerPollers, subscriptions,
      s"Did not get valid assignment for partitions ${subscriptions.asJava} after one consumer left", 3 * maxSessionTimeout)

    // done with pollers and consumers
    for (poller <- consumerPollers)
      poller.shutdown()
  }

  /**
   * Creates topic 'topicName' with 'numPartitions' partitions and produces 'recordsPerPartition'
   * records to each partition
   */
  def createTopicAndSendRecords(topicName: String, numPartitions: Int, recordsPerPartition: Int): Set[TopicPartition] = {
    TestUtils.createTopic(this.zkUtils, topicName, numPartitions, serverCount, this.servers)
    var parts = Set[TopicPartition]()
    for (partition <- 0 until numPartitions) {
      val tp = new TopicPartition(topicName, partition)
      sendRecords(recordsPerPartition, tp)
      parts = parts + tp
    }
    parts
  }

  /**
   * Subscribes consumer 'consumer' to a given list of topics 'topicsToSubscribe', creates
   * consumer poller and starts polling.
   * Assumes that the consumer is not subscribed to any topics yet
    *
    * @param consumer consumer
   * @param topicsToSubscribe topics that this consumer will subscribe to
   * @return consumer poller for the given consumer
   */
  def subscribeConsumerAndStartPolling(consumer: Consumer[Array[Byte], Array[Byte]],
                                       topicsToSubscribe: List[String]): ConsumerAssignmentPoller = {
    assertEquals(0, consumer.assignment().size)
    val consumerPoller = new ConsumerAssignmentPoller(consumer, topicsToSubscribe)
    consumerPoller.start()
    consumerPoller
  }

  /**
   * Creates consumer pollers corresponding to a given consumer group, one per consumer; subscribes consumers to
   * 'topicsToSubscribe' topics, waits until consumers get topics assignment.
   *
   * When the function returns, consumer pollers will continue to poll until shutdown is called on every poller.
   *
   * @param consumerGroup consumer group
   * @param topicsToSubscribe topics to which consumers will subscribe to
   * @return collection of consumer pollers
   */
  def subscribeConsumers(consumerGroup: Buffer[KafkaConsumer[Array[Byte], Array[Byte]]],
                         topicsToSubscribe: List[String]): Buffer[ConsumerAssignmentPoller] = {
    val consumerPollers = Buffer[ConsumerAssignmentPoller]()
    for (consumer <- consumerGroup)
      consumerPollers += subscribeConsumerAndStartPolling(consumer, topicsToSubscribe)
    consumerPollers
  }

  /**
   * Creates 'consumerCount' consumers and consumer pollers, one per consumer; subscribes consumers to
   * 'topicsToSubscribe' topics, waits until consumers get topics assignment.
   *
   * When the function returns, consumer pollers will continue to poll until shutdown is called on every poller.
   *
   * @param consumerCount number of consumers to create
   * @param topicsToSubscribe topics to which consumers will subscribe to
   * @param subscriptions set of all topic partitions
   * @return collection of created consumers and collection of corresponding consumer pollers
   */
  def createConsumerGroupAndWaitForAssignment(consumerCount: Int,
                                              topicsToSubscribe: List[String],
                                              subscriptions: Set[TopicPartition]): (Buffer[KafkaConsumer[Array[Byte], Array[Byte]]], Buffer[ConsumerAssignmentPoller]) = {
    assertTrue(consumerCount <= subscriptions.size)
    val consumerGroup = Buffer[KafkaConsumer[Array[Byte], Array[Byte]]]()
    for (_ <- 0 until consumerCount)
      consumerGroup += new KafkaConsumer[Array[Byte], Array[Byte]](this.consumerConfig)
    consumers ++= consumerGroup

    // create consumer pollers, wait for assignment and validate it
    val consumerPollers = subscribeConsumers(consumerGroup, topicsToSubscribe)

    (consumerGroup, consumerPollers)
  }

  /**
   * Create 'numOfConsumersToAdd' consumers add then to the consumer group 'consumerGroup', and create corresponding
   * pollers for these consumers. Wait for partition re-assignment and validate.
   *
   * Currently, assignment validation requires that total number of partitions is greater or equal to
   * number of consumers, so subscriptions.size must be greater or equal the resulting number of consumers in the group
   *
   * @param numOfConsumersToAdd number of consumers to create and add to the consumer group
   * @param consumerGroup current consumer group
   * @param consumerPollers current consumer pollers
   * @param topicsToSubscribe topics to which new consumers will subscribe to
   * @param subscriptions set of all topic partitions
   */
  def addConsumersToGroupAndWaitForGroupAssignment(numOfConsumersToAdd: Int,
                                                   consumerGroup: Buffer[KafkaConsumer[Array[Byte], Array[Byte]]],
                                                   consumerPollers: Buffer[ConsumerAssignmentPoller],
                                                   topicsToSubscribe: List[String],
                                                   subscriptions: Set[TopicPartition]): Unit = {
    assertTrue(consumerGroup.size + numOfConsumersToAdd <= subscriptions.size)
    for (_ <- 0 until numOfConsumersToAdd) {
      val newConsumer = new KafkaConsumer[Array[Byte], Array[Byte]](this.consumerConfig)
      consumerGroup += newConsumer
      consumerPollers += subscribeConsumerAndStartPolling(newConsumer, topicsToSubscribe)
    }

    // wait until topics get re-assigned and validate assignment
    validateGroupAssignment(consumerPollers, subscriptions,
      s"Did not get valid assignment for partitions ${subscriptions.asJava} after we added ${numOfConsumersToAdd} consumer(s)")
  }

  /**
   * Wait for consumers to get partition assignment and validate it.
   *
   * @param consumerPollers consumer pollers corresponding to the consumer group we are testing
   * @param subscriptions set of all topic partitions
   * @param msg message to print when waiting for/validating assignment fails
   */
  def validateGroupAssignment(consumerPollers: Buffer[ConsumerAssignmentPoller],
                              subscriptions: Set[TopicPartition],
                              msg: String,
                              waitTime: Long = 10000L): Unit = {
    TestUtils.waitUntilTrue(() => {
      val assignments = Buffer[Set[TopicPartition]]()
      consumerPollers.foreach(assignments += _.consumerAssignment())
      isPartitionAssignmentValid(assignments, subscriptions)
    }, msg, waitTime)
  }

  def changeConsumerGroupSubscriptionAndValidateAssignment(consumerPollers: Buffer[ConsumerAssignmentPoller],
                                                           topicsToSubscribe: List[String],
                                                           subscriptions: Set[TopicPartition]): Unit = {
    for (poller <- consumerPollers)
      poller.subscribe(topicsToSubscribe)

    // since subscribe call to poller does not actually call consumer subscribe right away, wait
    // until subscribe is called on all consumers
    TestUtils.waitUntilTrue(() => {
      consumerPollers forall (poller => poller.isSubscribeRequestProcessed())
    }, s"Failed to call subscribe on all consumers in the group for subscription ${subscriptions}", 1000L)

    validateGroupAssignment(consumerPollers, subscriptions,
      s"Did not get valid assignment for partitions ${subscriptions.asJava} after we changed subscription")
  }

  def changeConsumerSubscriptionAndValidateAssignment[K, V](consumer: Consumer[K, V],
                                                            topicsToSubscribe: List[String],
                                                            subscriptions: Set[TopicPartition],
                                                            rebalanceListener: ConsumerRebalanceListener): Unit = {
    consumer.subscribe(topicsToSubscribe.asJava, rebalanceListener)
    TestUtils.waitUntilTrue(() => {
      consumer.poll(50)
      consumer.assignment() == subscriptions.asJava
    }, s"Expected partitions ${subscriptions.asJava} but actually got ${consumer.assignment()}")
  }

}
