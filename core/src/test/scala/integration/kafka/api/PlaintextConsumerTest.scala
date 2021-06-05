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

import java.time.Duration
import java.util
import java.util.Arrays.asList
import java.util.regex.Pattern
import java.util.{Collections, Locale, Optional, Properties}

import kafka.log.LogConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.{MetricName, TopicPartition}
import org.apache.kafka.common.errors.{InvalidGroupIdException, InvalidTopicException}
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.record.{CompressionType, TimestampType}
import org.apache.kafka.common.serialization._
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.test.{MockConsumerInterceptor, MockProducerInterceptor}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import scala.jdk.CollectionConverters._
import scala.collection.mutable.Buffer
import kafka.server.QuotaType
import kafka.server.KafkaServer

import scala.collection.mutable

/* We have some tests in this class instead of `BaseConsumerTest` in order to keep the build time under control. */
class PlaintextConsumerTest extends BaseConsumerTest {

  @Test
  def testHeaders(): Unit = {
    val numRecords = 1
    val record = new ProducerRecord(tp.topic, tp.partition, null, "key".getBytes, "value".getBytes)

    record.headers().add("headerKey", "headerValue".getBytes)

    val producer = createProducer()
    producer.send(record)

    val consumer = createConsumer()
    assertEquals(0, consumer.assignment.size)
    consumer.assign(List(tp).asJava)
    assertEquals(1, consumer.assignment.size)

    consumer.seek(tp, 0)
    val records = consumeRecords(consumer = consumer, numRecords = numRecords)

    assertEquals(numRecords, records.size)

    for (i <- 0 until numRecords) {
      val record = records(i)
      val header = record.headers().lastHeader("headerKey")
      assertEquals("headerValue", if (header == null) null else new String(header.value()))
    }
  }

  trait SerializerImpl extends Serializer[Array[Byte]]{
    var serializer = new ByteArraySerializer()

    override def serialize(topic: String, headers: Headers, data: Array[Byte]): Array[Byte] = {
      headers.add("content-type", "application/octet-stream".getBytes)
      serializer.serialize(topic, data)
    }

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = serializer.configure(configs, isKey)

    override def close(): Unit = serializer.close()

    override def serialize(topic: String, data: Array[Byte]): Array[Byte] = {
      fail("method should not be invoked")
      null
    }
  }

  trait DeserializerImpl extends Deserializer[Array[Byte]]{
    var deserializer = new ByteArrayDeserializer()

    override def deserialize(topic: String, headers: Headers, data: Array[Byte]): Array[Byte] = {
      val header = headers.lastHeader("content-type")
      assertEquals("application/octet-stream", if (header == null) null else new String(header.value()))
      deserializer.deserialize(topic, data)
    }

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = deserializer.configure(configs, isKey)

    override def close(): Unit = deserializer.close()

    override def deserialize(topic: String, data: Array[Byte]): Array[Byte] = {
      fail("method should not be invoked")
      null
    }
  }

  private def testHeadersSerializeDeserialize(serializer: Serializer[Array[Byte]], deserializer: Deserializer[Array[Byte]]): Unit = {
    val numRecords = 1
    val record = new ProducerRecord(tp.topic, tp.partition, null, "key".getBytes, "value".getBytes)

    val producer = createProducer(
      keySerializer = new ByteArraySerializer,
      valueSerializer = serializer)
    producer.send(record)

    val consumer = createConsumer(
      keyDeserializer = new ByteArrayDeserializer,
      valueDeserializer = deserializer)
    assertEquals(0, consumer.assignment.size)
    consumer.assign(List(tp).asJava)
    assertEquals(1, consumer.assignment.size)

    consumer.seek(tp, 0)
    val records = consumeRecords(consumer = consumer, numRecords = numRecords)

    assertEquals(numRecords, records.size)
  }

  @deprecated("poll(Duration) is the replacement", since = "2.0")
  @Test
  def testDeprecatedPollBlocksForAssignment(): Unit = {
    val consumer = createConsumer()
    consumer.subscribe(Set(topic).asJava)
    consumer.poll(0)
    assertEquals(Set(tp, tp2), consumer.assignment().asScala)
  }

  @Test
  def testHeadersSerializerDeserializer(): Unit = {
    val extendedSerializer = new Serializer[Array[Byte]] with SerializerImpl

    val extendedDeserializer = new Deserializer[Array[Byte]] with DeserializerImpl

    testHeadersSerializeDeserialize(extendedSerializer, extendedDeserializer)
  }

  @Test
  def testMaxPollRecords(): Unit = {
    val maxPollRecords = 2
    val numRecords = 10000

    val producer = createProducer()
    val startingTimestamp = System.currentTimeMillis()
    sendRecords(producer, numRecords, tp, startingTimestamp = startingTimestamp)

    this.consumerConfig.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords.toString)
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    consumeAndVerifyRecords(consumer, numRecords = numRecords, startingOffset = 0, maxPollRecords = maxPollRecords,
      startingTimestamp = startingTimestamp)
  }

  @Test
  def testMaxPollIntervalMs(): Unit = {
    this.consumerConfig.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 1000.toString)
    this.consumerConfig.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 500.toString)
    this.consumerConfig.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 2000.toString)

    val consumer = createConsumer()

    val listener = new TestConsumerReassignmentListener()
    consumer.subscribe(List(topic).asJava, listener)

    // rebalance to get the initial assignment
    awaitRebalance(consumer, listener)
    assertEquals(1, listener.callsToAssigned)
    assertEquals(0, listener.callsToRevoked)

    // after we extend longer than max.poll a rebalance should be triggered
    // NOTE we need to have a relatively much larger value than max.poll to let heartbeat expired for sure
    Thread.sleep(3000)

    awaitRebalance(consumer, listener)
    assertEquals(2, listener.callsToAssigned)
    assertEquals(1, listener.callsToRevoked)
  }

  @Test
  def testMaxPollIntervalMsDelayInRevocation(): Unit = {
    this.consumerConfig.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 5000.toString)
    this.consumerConfig.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 500.toString)
    this.consumerConfig.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 1000.toString)
    this.consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false.toString)

    val consumer = createConsumer()
    var commitCompleted = false
    var committedPosition: Long = -1

    val listener = new TestConsumerReassignmentListener {
      override def onPartitionsLost(partitions: util.Collection[TopicPartition]): Unit = {}
      override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {
        if (!partitions.isEmpty && partitions.contains(tp)) {
          // on the second rebalance (after we have joined the group initially), sleep longer
          // than session timeout and then try a commit. We should still be in the group,
          // so the commit should succeed
          Utils.sleep(1500)
          committedPosition = consumer.position(tp)
          consumer.commitSync(Map(tp -> new OffsetAndMetadata(committedPosition)).asJava)
          commitCompleted = true
        }
        super.onPartitionsRevoked(partitions)
      }
    }

    consumer.subscribe(List(topic).asJava, listener)

    // rebalance to get the initial assignment
    awaitRebalance(consumer, listener)

    // force a rebalance to trigger an invocation of the revocation callback while in the group
    consumer.subscribe(List("otherTopic").asJava, listener)
    awaitRebalance(consumer, listener)

    assertEquals(0, committedPosition)
    assertTrue(commitCompleted)
  }

  @Test
  def testMaxPollIntervalMsDelayInAssignment(): Unit = {
    this.consumerConfig.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 5000.toString)
    this.consumerConfig.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 500.toString)
    this.consumerConfig.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 1000.toString)
    this.consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false.toString)

    val consumer = createConsumer()
    val listener = new TestConsumerReassignmentListener {
      override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
        // sleep longer than the session timeout, we should still be in the group after invocation
        Utils.sleep(1500)
        super.onPartitionsAssigned(partitions)
      }
    }
    consumer.subscribe(List(topic).asJava, listener)

    // rebalance to get the initial assignment
    awaitRebalance(consumer, listener)

    // We should still be in the group after this invocation
    ensureNoRebalance(consumer, listener)
  }

  @Test
  def testAutoCommitOnClose(): Unit = {
    this.consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    val consumer = createConsumer()

    val numRecords = 10000
    val producer = createProducer()
    sendRecords(producer, numRecords, tp)

    consumer.subscribe(List(topic).asJava)
    awaitAssignment(consumer, Set(tp, tp2))

    // should auto-commit seeked positions before closing
    consumer.seek(tp, 300)
    consumer.seek(tp2, 500)
    consumer.close()

    // now we should see the committed positions from another consumer
    val anotherConsumer = createConsumer()
    assertEquals(300, anotherConsumer.committed(Set(tp).asJava).get(tp).offset)
    assertEquals(500, anotherConsumer.committed(Set(tp2).asJava).get(tp2).offset)
  }

  @Test
  def testAutoCommitOnCloseAfterWakeup(): Unit = {
    this.consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    val consumer = createConsumer()

    val numRecords = 10000
    val producer = createProducer()
    sendRecords(producer, numRecords, tp)

    consumer.subscribe(List(topic).asJava)
    awaitAssignment(consumer, Set(tp, tp2))

    // should auto-commit seeked positions before closing
    consumer.seek(tp, 300)
    consumer.seek(tp2, 500)

    // wakeup the consumer before closing to simulate trying to break a poll
    // loop from another thread
    consumer.wakeup()
    consumer.close()

    // now we should see the committed positions from another consumer
    val anotherConsumer = createConsumer()
    assertEquals(300, anotherConsumer.committed(Set(tp).asJava).get(tp).offset)
    assertEquals(500, anotherConsumer.committed(Set(tp2).asJava).get(tp2).offset)
  }

  @Test
  def testAutoOffsetReset(): Unit = {
    val producer = createProducer()
    val startingTimestamp = System.currentTimeMillis()
    sendRecords(producer, numRecords = 1, tp, startingTimestamp = startingTimestamp)

    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    consumeAndVerifyRecords(consumer = consumer, numRecords = 1, startingOffset = 0, startingTimestamp = startingTimestamp)
  }

  @Test
  def testGroupConsumption(): Unit = {
    val producer = createProducer()
    val startingTimestamp = System.currentTimeMillis()
    sendRecords(producer, numRecords = 10, tp, startingTimestamp = startingTimestamp)

    val consumer = createConsumer()
    consumer.subscribe(List(topic).asJava)
    consumeAndVerifyRecords(consumer = consumer, numRecords = 1, startingOffset = 0, startingTimestamp = startingTimestamp)
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
  def testPatternSubscription(): Unit = {
    val numRecords = 10000
    val producer = createProducer()
    sendRecords(producer, numRecords, tp)

    val topic1 = "tblablac" // matches subscribed pattern
    createTopic(topic1, 2, brokerCount)
    sendRecords(producer, numRecords = 1000, new TopicPartition(topic1, 0))
    sendRecords(producer, numRecords = 1000, new TopicPartition(topic1, 1))

    val topic2 = "tblablak" // does not match subscribed pattern
    createTopic(topic2, 2, brokerCount)
    sendRecords(producer,numRecords = 1000, new TopicPartition(topic2, 0))
    sendRecords(producer, numRecords = 1000, new TopicPartition(topic2, 1))

    val topic3 = "tblab1" // does not match subscribed pattern
    createTopic(topic3, 2, brokerCount)
    sendRecords(producer, numRecords = 1000, new TopicPartition(topic3, 0))
    sendRecords(producer, numRecords = 1000, new TopicPartition(topic3, 1))

    val consumer = createConsumer()
    assertEquals(0, consumer.assignment().size)

    val pattern = Pattern.compile("t.*c")
    consumer.subscribe(pattern, new TestConsumerReassignmentListener)

    var assignment = Set(
      new TopicPartition(topic, 0),
      new TopicPartition(topic, 1),
      new TopicPartition(topic1, 0),
      new TopicPartition(topic1, 1))
    awaitAssignment(consumer, assignment)

    val topic4 = "tsomec" // matches subscribed pattern
    createTopic(topic4, 2, brokerCount)
    sendRecords(producer, numRecords = 1000, new TopicPartition(topic4, 0))
    sendRecords(producer, numRecords = 1000, new TopicPartition(topic4, 1))

    assignment ++= Set(
      new TopicPartition(topic4, 0),
      new TopicPartition(topic4, 1))
    awaitAssignment(consumer, assignment)

    consumer.unsubscribe()
    assertEquals(0, consumer.assignment().size)
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
  def testSubsequentPatternSubscription(): Unit = {
    this.consumerConfig.setProperty(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "30000")
    val consumer = createConsumer()

    val numRecords = 10000
    val producer = createProducer()
    sendRecords(producer, numRecords = numRecords, tp)

    // the first topic ('topic')  matches first subscription pattern only

    val fooTopic = "foo" // matches both subscription patterns
    createTopic(fooTopic, 1, brokerCount)
    sendRecords(producer, numRecords = 1000, new TopicPartition(fooTopic, 0))

    assertEquals(0, consumer.assignment().size)

    val pattern1 = Pattern.compile(".*o.*") // only 'topic' and 'foo' match this
    consumer.subscribe(pattern1, new TestConsumerReassignmentListener)

    var assignment = Set(
      new TopicPartition(topic, 0),
      new TopicPartition(topic, 1),
      new TopicPartition(fooTopic, 0))
    awaitAssignment(consumer, assignment)

    val barTopic = "bar" // matches the next subscription pattern
    createTopic(barTopic, 1, brokerCount)
    sendRecords(producer, numRecords = 1000, new TopicPartition(barTopic, 0))

    val pattern2 = Pattern.compile("...") // only 'foo' and 'bar' match this
    consumer.subscribe(pattern2, new TestConsumerReassignmentListener)
    assignment --= Set(
      new TopicPartition(topic, 0),
      new TopicPartition(topic, 1))
    assignment ++= Set(
      new TopicPartition(barTopic, 0))
    awaitAssignment(consumer, assignment)

    consumer.unsubscribe()
    assertEquals(0, consumer.assignment().size)
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
  def testPatternUnsubscription(): Unit = {
    val numRecords = 10000
    val producer = createProducer()
    sendRecords(producer, numRecords, tp)

    val topic1 = "tblablac" // matches the subscription pattern
    createTopic(topic1, 2, brokerCount)
    sendRecords(producer, numRecords = 1000, new TopicPartition(topic1, 0))
    sendRecords(producer, numRecords = 1000, new TopicPartition(topic1, 1))

    val consumer = createConsumer()
    assertEquals(0, consumer.assignment().size)

    consumer.subscribe(Pattern.compile("t.*c"), new TestConsumerReassignmentListener)
    val assignment = Set(
      new TopicPartition(topic, 0),
      new TopicPartition(topic, 1),
      new TopicPartition(topic1, 0),
      new TopicPartition(topic1, 1))
    awaitAssignment(consumer, assignment)

    consumer.unsubscribe()
    assertEquals(0, consumer.assignment().size)
  }

  @Test
  def testCommitMetadata(): Unit = {
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)

    // sync commit
    val syncMetadata = new OffsetAndMetadata(5, Optional.of(15), "foo")
    consumer.commitSync(Map((tp, syncMetadata)).asJava)
    assertEquals(syncMetadata, consumer.committed(Set(tp).asJava).get(tp))

    // async commit
    val asyncMetadata = new OffsetAndMetadata(10, "bar")
    sendAndAwaitAsyncCommit(consumer, Some(Map(tp -> asyncMetadata)))
    assertEquals(asyncMetadata, consumer.committed(Set(tp).asJava).get(tp))

    // handle null metadata
    val nullMetadata = new OffsetAndMetadata(5, null)
    consumer.commitSync(Map(tp -> nullMetadata).asJava)
    assertEquals(nullMetadata, consumer.committed(Set(tp).asJava).get(tp))
  }

  @Test
  def testAsyncCommit(): Unit = {
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)

    val callback = new CountConsumerCommitCallback
    val count = 5

    for (i <- 1 to count)
      consumer.commitAsync(Map(tp -> new OffsetAndMetadata(i)).asJava, callback)

    TestUtils.pollUntilTrue(consumer, () => callback.successCount >= count || callback.lastError.isDefined,
      "Failed to observe commit callback before timeout", waitTimeMs = 10000)

    assertEquals(None, callback.lastError)
    assertEquals(count, callback.successCount)
    assertEquals(new OffsetAndMetadata(count), consumer.committed(Set(tp).asJava).get(tp))
  }

  @Test
  def testExpandingTopicSubscriptions(): Unit = {
    val otherTopic = "other"
    val initialAssignment = Set(new TopicPartition(topic, 0), new TopicPartition(topic, 1))
    val consumer = createConsumer()
    consumer.subscribe(List(topic).asJava)
    awaitAssignment(consumer, initialAssignment)

    createTopic(otherTopic, 2, brokerCount)
    val expandedAssignment = initialAssignment ++ Set(new TopicPartition(otherTopic, 0), new TopicPartition(otherTopic, 1))
    consumer.subscribe(List(topic, otherTopic).asJava)
    awaitAssignment(consumer, expandedAssignment)
  }

  @Test
  def testShrinkingTopicSubscriptions(): Unit = {
    val otherTopic = "other"
    createTopic(otherTopic, 2, brokerCount)
    val initialAssignment = Set(new TopicPartition(topic, 0), new TopicPartition(topic, 1), new TopicPartition(otherTopic, 0), new TopicPartition(otherTopic, 1))
    val consumer = createConsumer()
    consumer.subscribe(List(topic, otherTopic).asJava)
    awaitAssignment(consumer, initialAssignment)

    val shrunkenAssignment = Set(new TopicPartition(topic, 0), new TopicPartition(topic, 1))
    consumer.subscribe(List(topic).asJava)
    awaitAssignment(consumer, shrunkenAssignment)
  }

  @Test
  def testPartitionsFor(): Unit = {
    val numParts = 2
    createTopic("part-test", numParts, 1)
    val consumer = createConsumer()
    val parts = consumer.partitionsFor("part-test")
    assertNotNull(parts)
    assertEquals(2, parts.size)
  }

  @Test
  def testPartitionsForAutoCreate(): Unit = {
    val consumer = createConsumer()
    // First call would create the topic
    consumer.partitionsFor("non-exist-topic")
    val partitions = consumer.partitionsFor("non-exist-topic")
    assertFalse(partitions.isEmpty)
  }

  @Test
  def testPartitionsForInvalidTopic(): Unit = {
    val consumer = createConsumer()
    assertThrows(classOf[InvalidTopicException], () => consumer.partitionsFor(";3# ads,{234"))
  }

  @Test
  def testSeek(): Unit = {
    val consumer = createConsumer()
    val totalRecords = 50L
    val mid = totalRecords / 2

    // Test seek non-compressed message
    val producer = createProducer()
    val startingTimestamp = 0
    sendRecords(producer, totalRecords.toInt, tp, startingTimestamp = startingTimestamp)
    consumer.assign(List(tp).asJava)

    consumer.seekToEnd(List(tp).asJava)
    assertEquals(totalRecords, consumer.position(tp))
    assertTrue(consumer.poll(Duration.ofMillis(50)).isEmpty)

    consumer.seekToBeginning(List(tp).asJava)
    assertEquals(0L, consumer.position(tp))
    consumeAndVerifyRecords(consumer, numRecords = 1, startingOffset = 0, startingTimestamp = startingTimestamp)

    consumer.seek(tp, mid)
    assertEquals(mid, consumer.position(tp))

    consumeAndVerifyRecords(consumer, numRecords = 1, startingOffset = mid.toInt, startingKeyAndValueIndex = mid.toInt,
      startingTimestamp = mid.toLong)

    // Test seek compressed message
    sendCompressedMessages(totalRecords.toInt, tp2)
    consumer.assign(List(tp2).asJava)

    consumer.seekToEnd(List(tp2).asJava)
    assertEquals(totalRecords, consumer.position(tp2))
    assertTrue(consumer.poll(Duration.ofMillis(50)).isEmpty)

    consumer.seekToBeginning(List(tp2).asJava)
    assertEquals(0L, consumer.position(tp2))
    consumeAndVerifyRecords(consumer, numRecords = 1, startingOffset = 0, tp = tp2)

    consumer.seek(tp2, mid)
    assertEquals(mid, consumer.position(tp2))
    consumeAndVerifyRecords(consumer, numRecords = 1, startingOffset = mid.toInt, startingKeyAndValueIndex = mid.toInt,
      startingTimestamp = mid.toLong, tp = tp2)
  }

  private def sendCompressedMessages(numRecords: Int, tp: TopicPartition): Unit = {
    val producerProps = new Properties()
    producerProps.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, CompressionType.GZIP.name)
    producerProps.setProperty(ProducerConfig.LINGER_MS_CONFIG, Int.MaxValue.toString)
    val producer = createProducer(configOverrides = producerProps)
    (0 until numRecords).foreach { i =>
      producer.send(new ProducerRecord(tp.topic, tp.partition, i.toLong, s"key $i".getBytes, s"value $i".getBytes))
    }
    producer.close()
  }

  @Test
  def testPositionAndCommit(): Unit = {
    val producer = createProducer()
    var startingTimestamp = System.currentTimeMillis()
    sendRecords(producer, numRecords = 5, tp, startingTimestamp = startingTimestamp)

    val topicPartition = new TopicPartition(topic, 15)
    val consumer = createConsumer()
    assertNull(consumer.committed(Set(topicPartition).asJava).get(topicPartition))

    // position() on a partition that we aren't subscribed to throws an exception
    assertThrows(classOf[IllegalStateException], () => consumer.position(topicPartition))

    consumer.assign(List(tp).asJava)

    assertEquals(0L, consumer.position(tp), "position() on a partition that we are subscribed to should reset the offset")
    consumer.commitSync()
    assertEquals(0L, consumer.committed(Set(tp).asJava).get(tp).offset)
    
    consumeAndVerifyRecords(consumer = consumer, numRecords = 5, startingOffset = 0, startingTimestamp = startingTimestamp)
    assertEquals(5L, consumer.position(tp), "After consuming 5 records, position should be 5")
    consumer.commitSync()
    assertEquals(5L, consumer.committed(Set(tp).asJava).get(tp).offset, "Committed offset should be returned")

    startingTimestamp = System.currentTimeMillis()
    sendRecords(producer, numRecords = 1, tp, startingTimestamp = startingTimestamp)

    // another consumer in the same group should get the same position
    val otherConsumer = createConsumer()
    otherConsumer.assign(List(tp).asJava)
    consumeAndVerifyRecords(consumer = otherConsumer, numRecords = 1, startingOffset = 5, startingTimestamp = startingTimestamp)
  }

  @Test
  def testPartitionPauseAndResume(): Unit = {
    val partitions = List(tp).asJava
    val producer = createProducer()
    var startingTimestamp = System.currentTimeMillis()
    sendRecords(producer, numRecords = 5, tp, startingTimestamp = startingTimestamp)

    val consumer = createConsumer()
    consumer.assign(partitions)
    consumeAndVerifyRecords(consumer = consumer, numRecords = 5, startingOffset = 0, startingTimestamp = startingTimestamp)
    consumer.pause(partitions)
    startingTimestamp = System.currentTimeMillis()
    sendRecords(producer, numRecords = 5, tp, startingTimestamp = startingTimestamp)
    assertTrue(consumer.poll(Duration.ofMillis(100)).isEmpty)
    consumer.resume(partitions)
    consumeAndVerifyRecords(consumer = consumer, numRecords = 5, startingOffset = 5, startingTimestamp = startingTimestamp)
  }

  @Test
  def testFetchInvalidOffset(): Unit = {
    this.consumerConfig.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none")
    val consumer = createConsumer()

    // produce one record
    val totalRecords = 2
    val producer = createProducer()
    sendRecords(producer, totalRecords, tp)
    consumer.assign(List(tp).asJava)

    // poll should fail because there is no offset reset strategy set.
    // we fail only when resetting positions after coordinator is known, so using a long timeout.
    assertThrows(classOf[NoOffsetForPartitionException], () => consumer.poll(Duration.ofMillis(15000)))

    // seek to out of range position
    val outOfRangePos = totalRecords + 1
    consumer.seek(tp, outOfRangePos)
    val e = assertThrows(classOf[OffsetOutOfRangeException], () => consumer.poll(Duration.ofMillis(20000)))
    val outOfRangePartitions = e.offsetOutOfRangePartitions()
    assertNotNull(outOfRangePartitions)
    assertEquals(1, outOfRangePartitions.size)
    assertEquals(outOfRangePos.toLong, outOfRangePartitions.get(tp))
  }

  @Test
  def testFetchRecordLargerThanFetchMaxBytes(): Unit = {
    val maxFetchBytes = 10 * 1024
    this.consumerConfig.setProperty(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, maxFetchBytes.toString)
    checkLargeRecord(maxFetchBytes + 1)
  }

  private def checkLargeRecord(producerRecordSize: Int): Unit = {
    val consumer = createConsumer()

    // produce a record that is larger than the configured fetch size
    val record = new ProducerRecord(tp.topic(), tp.partition(), "key".getBytes,
      new Array[Byte](producerRecordSize))
    val producer = createProducer()
    producer.send(record)

    // consuming a record that is too large should succeed since KIP-74
    consumer.assign(List(tp).asJava)
    val records = consumer.poll(Duration.ofMillis(20000))
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
    val consumer = createConsumer()

    val smallRecord = new ProducerRecord(tp.topic(), tp.partition(), "small".getBytes,
      "value".getBytes)
    val largeRecord = new ProducerRecord(tp.topic(), tp.partition(), "large".getBytes,
      new Array[Byte](largeProducerRecordSize))

    val producer = createProducer()
    producer.send(smallRecord).get
    producer.send(largeRecord).get

    // we should only get the small record in the first `poll`
    consumer.assign(List(tp).asJava)
    val records = consumer.poll(Duration.ofMillis(20000))
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
  def testFetchRecordLargerThanMaxPartitionFetchBytes(): Unit = {
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

    // Avoid a rebalance while the records are being sent (the default is 6 seconds)
    this.consumerConfig.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 20000.toString)
    val consumer = createConsumer()

    val topic1 = "topic1"
    val topic2 = "topic2"
    val topic3 = "topic3"
    val partitionCount = 30
    val topics = Seq(topic1, topic2, topic3)
    topics.foreach { topicName =>
      createTopic(topicName, partitionCount, brokerCount)
    }

    val partitions = topics.flatMap { topic =>
      (0 until partitionCount).map(new TopicPartition(topic, _))
    }

    assertEquals(0, consumer.assignment().size)

    consumer.subscribe(List(topic1, topic2, topic3).asJava)

    awaitAssignment(consumer, partitions.toSet)

    val producer = createProducer()

    val producerRecords = partitions.flatMap(sendRecords(producer, numRecords = partitionCount, _))

    val consumerRecords = consumeRecords(consumer, producerRecords.size)

    val expected = producerRecords.map { record =>
      (record.topic, record.partition, new String(record.key), new String(record.value), record.timestamp)
    }.toSet

    val actual = consumerRecords.map { record =>
      (record.topic, record.partition, new String(record.key), new String(record.value), record.timestamp)
    }.toSet

    assertEquals(expected, actual)
  }

  @Test
  def testRoundRobinAssignment(): Unit = {
    // 1 consumer using round-robin assignment
    this.consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "roundrobin-group")
    this.consumerConfig.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, classOf[RoundRobinAssignor].getName)
    val consumer = createConsumer()

    // create two new topics, each having 2 partitions
    val topic1 = "topic1"
    val topic2 = "topic2"
    val producer = createProducer()
    val expectedAssignment = createTopicAndSendRecords(producer, topic1, 2, 100) ++
      createTopicAndSendRecords(producer, topic2, 2, 100)

    assertEquals(0, consumer.assignment().size)

    // subscribe to two topics
    consumer.subscribe(List(topic1, topic2).asJava)
    awaitAssignment(consumer, expectedAssignment)

    // add one more topic with 2 partitions
    val topic3 = "topic3"
    createTopicAndSendRecords(producer, topic3, 2, 100)

    val newExpectedAssignment = expectedAssignment ++ Set(new TopicPartition(topic3, 0), new TopicPartition(topic3, 1))
    consumer.subscribe(List(topic1, topic2, topic3).asJava)
    awaitAssignment(consumer, newExpectedAssignment)

    // remove the topic we just added
    consumer.subscribe(List(topic1, topic2).asJava)
    awaitAssignment(consumer, expectedAssignment)

    consumer.unsubscribe()
    assertEquals(0, consumer.assignment().size)
  }

  @Test
  def testMultiConsumerRoundRobinAssignment(): Unit = {
    this.consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "roundrobin-group")
    this.consumerConfig.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, classOf[RoundRobinAssignor].getName)

    // create two new topics, total number of partitions must be greater than number of consumers
    val topic1 = "topic1"
    val topic2 = "topic2"
    val producer = createProducer()
    val subscriptions = createTopicAndSendRecords(producer, topic1, 5, 100) ++
      createTopicAndSendRecords(producer, topic2, 8, 100)

    // create a group of consumers, subscribe the consumers to all the topics and start polling
    // for the topic partition assignment
    val (consumerGroup, consumerPollers) = createConsumerGroupAndWaitForAssignment(10, List(topic1, topic2), subscriptions)
    try {
      validateGroupAssignment(consumerPollers, subscriptions)

      // add one more consumer and validate re-assignment
      addConsumersToGroupAndWaitForGroupAssignment(1, consumerGroup, consumerPollers,
        List(topic1, topic2), subscriptions, "roundrobin-group")
    } finally {
      consumerPollers.foreach(_.shutdown())
    }
  }

  /**
   * This test runs the following scenario to verify sticky assignor behavior.
   * Topics: single-topic, with random number of partitions, where #par is 10, 20, 30, 40, 50, 60, 70, 80, 90, or 100
   * Consumers: 9 consumers subscribed to the single topic
   * Expected initial assignment: partitions are assigned to consumers in a round robin fashion.
   *  - (#par mod 9) consumers will get (#par / 9 + 1) partitions, and the rest get (#par / 9) partitions
   * Then consumer #10 is added to the list (subscribing to the same single topic)
   * Expected new assignment:
   *  - (#par / 10) partition per consumer, where one partition from each of the early (#par mod 9) consumers
   *    will move to consumer #10, leading to a total of (#par mod 9) partition movement
   */
  @Test
  def testMultiConsumerStickyAssignment(): Unit = {

    def reverse(m: Map[Long, Set[TopicPartition]]) =
      m.values.toSet.flatten.map(v => (v, m.keys.filter(m(_).contains(v)).head)).toMap

    this.consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "sticky-group")
    this.consumerConfig.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, classOf[StickyAssignor].getName)

    // create one new topic
    val topic = "single-topic"
    val rand = 1 + scala.util.Random.nextInt(10)
    val producer = createProducer()
    val partitions = createTopicAndSendRecords(producer, topic, rand * 10, 100)

    // create a group of consumers, subscribe the consumers to the single topic and start polling
    // for the topic partition assignment
    val (consumerGroup, consumerPollers) = createConsumerGroupAndWaitForAssignment(9, List(topic), partitions)
    validateGroupAssignment(consumerPollers, partitions)
    val prePartition2PollerId = reverse(consumerPollers.map(poller => (poller.getId, poller.consumerAssignment())).toMap)

    // add one more consumer and validate re-assignment
    addConsumersToGroupAndWaitForGroupAssignment(1, consumerGroup, consumerPollers, List(topic), partitions, "sticky-group")

    val postPartition2PollerId = reverse(consumerPollers.map(poller => (poller.getId, poller.consumerAssignment())).toMap)
    val keys = prePartition2PollerId.keySet.union(postPartition2PollerId.keySet)
    var changes = 0
    keys.foreach { key =>
      val preVal = prePartition2PollerId.get(key)
      val postVal = postPartition2PollerId.get(key)
      if (preVal.nonEmpty && postVal.nonEmpty) {
        if (preVal.get != postVal.get)
          changes += 1
      } else
        changes += 1
    }

    consumerPollers.foreach(_.shutdown())

    assertEquals(rand, changes, "Expected only two topic partitions that have switched to other consumers.")
  }

  /**
   * This test re-uses BaseConsumerTest's consumers.
   * As a result, it is testing the default assignment strategy set by BaseConsumerTest
   */
  @Test
  def testMultiConsumerDefaultAssignment(): Unit = {
    // use consumers and topics defined in this class + one more topic
    val producer = createProducer()
    sendRecords(producer, numRecords = 100, tp)
    sendRecords(producer, numRecords = 100, tp2)
    val topic1 = "topic1"
    val subscriptions = Set(tp, tp2) ++ createTopicAndSendRecords(producer, topic1, 5, 100)

    // subscribe all consumers to all topics and validate the assignment

    val consumersInGroup = Buffer[KafkaConsumer[Array[Byte], Array[Byte]]]()
    consumersInGroup += createConsumer()
    consumersInGroup += createConsumer()

    val consumerPollers = subscribeConsumers(consumersInGroup, List(topic, topic1))
    try {
      validateGroupAssignment(consumerPollers, subscriptions)

      // add 2 more consumers and validate re-assignment
      addConsumersToGroupAndWaitForGroupAssignment(2, consumersInGroup, consumerPollers, List(topic, topic1), subscriptions)

      // add one more topic and validate partition re-assignment
      val topic2 = "topic2"
      val expandedSubscriptions = subscriptions ++ createTopicAndSendRecords(producer, topic2, 3, 100)
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
  def testInterceptors(): Unit = {
    val appendStr = "mock"
    MockConsumerInterceptor.resetCounters()
    MockProducerInterceptor.resetCounters()

    // create producer with interceptor
    val producerProps = new Properties()
    producerProps.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, classOf[MockProducerInterceptor].getName)
    producerProps.put("mock.interceptor.append", appendStr)
    val testProducer = createProducer(keySerializer = new StringSerializer,
      valueSerializer = new StringSerializer,
      configOverrides = producerProps)

    // produce records
    val numRecords = 10
    (0 until numRecords).map { i =>
      testProducer.send(new ProducerRecord(tp.topic, tp.partition, s"key $i", s"value $i"))
    }.foreach(_.get)
    assertEquals(numRecords, MockProducerInterceptor.ONSEND_COUNT.intValue)
    assertEquals(numRecords, MockProducerInterceptor.ON_SUCCESS_COUNT.intValue)
    // send invalid record
    assertThrows(classOf[Throwable], () => testProducer.send(null), () => "Should not allow sending a null record")
    assertEquals(1, MockProducerInterceptor.ON_ERROR_COUNT.intValue, "Interceptor should be notified about exception")
    assertEquals(0, MockProducerInterceptor.ON_ERROR_WITH_METADATA_COUNT.intValue(), "Interceptor should not receive metadata with an exception when record is null")

    // create consumer with interceptor
    this.consumerConfig.setProperty(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, "org.apache.kafka.test.MockConsumerInterceptor")
    val testConsumer = createConsumer(keyDeserializer = new StringDeserializer, valueDeserializer = new StringDeserializer)
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
    assertEquals(2, testConsumer.committed(Set(tp).asJava).get(tp).offset)
    assertEquals(commitCountBefore + 1, MockConsumerInterceptor.ON_COMMIT_COUNT.intValue)

    // commit async and verify onCommit is called
    sendAndAwaitAsyncCommit(testConsumer, Some(Map(tp -> new OffsetAndMetadata(5L))))
    assertEquals(5, testConsumer.committed(Set(tp).asJava).get(tp).offset)
    assertEquals(commitCountBefore + 2, MockConsumerInterceptor.ON_COMMIT_COUNT.intValue)

    testConsumer.close()
    testProducer.close()

    // cleanup
    MockConsumerInterceptor.resetCounters()
    MockProducerInterceptor.resetCounters()
  }

  @Test
  def testAutoCommitIntercept(): Unit = {
    val topic2 = "topic2"
    createTopic(topic2, 2, brokerCount)

    // produce records
    val numRecords = 100
    val testProducer = createProducer(keySerializer = new StringSerializer, valueSerializer = new StringSerializer)
    (0 until numRecords).map { i =>
      testProducer.send(new ProducerRecord(tp.topic(), tp.partition(), s"key $i", s"value $i"))
    }.foreach(_.get)

    // create consumer with interceptor
    this.consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    this.consumerConfig.setProperty(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, "org.apache.kafka.test.MockConsumerInterceptor")
    val testConsumer = createConsumer(keyDeserializer = new StringDeserializer, valueDeserializer = new StringDeserializer)
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
    assertEquals(10, testConsumer.committed(Set(tp).asJava).get(tp).offset)
    assertEquals(20, testConsumer.committed(Set(tp2).asJava).get(tp2).offset)
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
  def testInterceptorsWithWrongKeyValue(): Unit = {
    val appendStr = "mock"
    // create producer with interceptor that has different key and value types from the producer
    val producerProps = new Properties()
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    producerProps.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "org.apache.kafka.test.MockProducerInterceptor")
    producerProps.put("mock.interceptor.append", appendStr)
    val testProducer = createProducer()

    // producing records should succeed
    testProducer.send(new ProducerRecord(tp.topic(), tp.partition(), s"key".getBytes, s"value will not be modified".getBytes))

    // create consumer with interceptor that has different key and value types from the consumer
    this.consumerConfig.setProperty(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, "org.apache.kafka.test.MockConsumerInterceptor")
    val testConsumer = createConsumer()

    testConsumer.assign(List(tp).asJava)
    testConsumer.seek(tp, 0)

    // consume and verify that values are not modified by interceptors -- their exceptions are caught and logged, but not propagated
    val records = consumeRecords(testConsumer, 1)
    val record = records.head
    assertEquals(s"value will not be modified", new String(record.value()))
  }

  @Test
  def testConsumeMessagesWithCreateTime(): Unit = {
    val numRecords = 50
    // Test non-compressed messages
    val producer = createProducer()
    val startingTimestamp = System.currentTimeMillis()
    sendRecords(producer, numRecords, tp, startingTimestamp = startingTimestamp)
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    consumeAndVerifyRecords(consumer = consumer, numRecords = numRecords, startingOffset = 0, startingTimestamp = startingTimestamp)

    // Test compressed messages
    sendCompressedMessages(numRecords, tp2)
    consumer.assign(List(tp2).asJava)
    consumeAndVerifyRecords(consumer = consumer, numRecords = numRecords, tp = tp2, startingOffset = 0)
  }

  @Test
  def testConsumeMessagesWithLogAppendTime(): Unit = {
    val topicName = "testConsumeMessagesWithLogAppendTime"
    val topicProps = new Properties()
    topicProps.setProperty(LogConfig.MessageTimestampTypeProp, "LogAppendTime")
    createTopic(topicName, 2, 2, topicProps)

    val startTime = System.currentTimeMillis()
    val numRecords = 50

    // Test non-compressed messages
    val tp1 = new TopicPartition(topicName, 0)
    val producer = createProducer()
    sendRecords(producer, numRecords, tp1)

    val consumer = createConsumer()
    consumer.assign(List(tp1).asJava)
    consumeAndVerifyRecords(consumer = consumer, numRecords = numRecords, tp = tp1, startingOffset = 0, startingKeyAndValueIndex = 0,
      startingTimestamp = startTime, timestampType = TimestampType.LOG_APPEND_TIME)

    // Test compressed messages
    val tp2 = new TopicPartition(topicName, 1)
    sendCompressedMessages(numRecords, tp2)
    consumer.assign(List(tp2).asJava)
    consumeAndVerifyRecords(consumer = consumer, numRecords = numRecords, tp = tp2, startingOffset = 0, startingKeyAndValueIndex = 0,
      startingTimestamp = startTime, timestampType = TimestampType.LOG_APPEND_TIME)
  }

  @Test
  def testListTopics(): Unit = {
    val numParts = 2
    val topic1 = "part-test-topic-1"
    val topic2 = "part-test-topic-2"
    val topic3 = "part-test-topic-3"
    createTopic(topic1, numParts, 1)
    createTopic(topic2, numParts, 1)
    createTopic(topic3, numParts, 1)

    val consumer = createConsumer()
    val topics = consumer.listTopics()
    assertNotNull(topics)
    assertEquals(5, topics.size())
    assertEquals(5, topics.keySet().size())
    assertEquals(2, topics.get(topic1).size)
    assertEquals(2, topics.get(topic2).size)
    assertEquals(2, topics.get(topic3).size)
  }

  @Test
  def testOffsetsForTimes(): Unit = {
    val numParts = 2
    val topic1 = "part-test-topic-1"
    val topic2 = "part-test-topic-2"
    val topic3 = "part-test-topic-3"
    val props = new Properties()
    props.setProperty(LogConfig.MessageFormatVersionProp, "0.9.0")
    createTopic(topic1, numParts, 1)
    // Topic2 is in old message format.
    createTopic(topic2, numParts, 1, props)
    createTopic(topic3, numParts, 1)

    val consumer = createConsumer()

    // Test negative target time
    assertThrows(classOf[IllegalArgumentException],
      () => consumer.offsetsForTimes(Collections.singletonMap(new TopicPartition(topic1, 0), -1)))

    val producer = createProducer()
    val timestampsToSearch = new util.HashMap[TopicPartition, java.lang.Long]()
    var i = 0
    for (topic <- List(topic1, topic2, topic3)) {
      for (part <- 0 until numParts) {
        val tp = new TopicPartition(topic, part)
        // In sendRecords(), each message will have key, value and timestamp equal to the sequence number.
        sendRecords(producer, numRecords = 100, tp, startingTimestamp = 0)
        timestampsToSearch.put(tp, (i * 20).toLong)
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

    val timestampTopic1P0 = timestampOffsets.get(new TopicPartition(topic1, 0))
    assertEquals(0, timestampTopic1P0.offset)
    assertEquals(0, timestampTopic1P0.timestamp)
    assertEquals(Optional.of(0), timestampTopic1P0.leaderEpoch)

    val timestampTopic1P1 = timestampOffsets.get(new TopicPartition(topic1, 1))
    assertEquals(20, timestampTopic1P1.offset)
    assertEquals(20, timestampTopic1P1.timestamp)
    assertEquals(Optional.of(0), timestampTopic1P1.leaderEpoch)

    assertNull(timestampOffsets.get(new TopicPartition(topic2, 0)), "null should be returned when message format is 0.9.0")
    assertNull(timestampOffsets.get(new TopicPartition(topic2, 1)), "null should be returned when message format is 0.9.0")

    val timestampTopic3P0 = timestampOffsets.get(new TopicPartition(topic3, 0))
    assertEquals(80, timestampTopic3P0.offset)
    assertEquals(80, timestampTopic3P0.timestamp)
    assertEquals(Optional.of(0), timestampTopic3P0.leaderEpoch)

    assertNull(timestampOffsets.get(new TopicPartition(topic3, 1)))
  }

  @Test
  def testEarliestOrLatestOffsets(): Unit = {
    val topic0 = "topicWithNewMessageFormat"
    val topic1 = "topicWithOldMessageFormat"
    val producer = createProducer()
    createTopicAndSendRecords(producer, topicName = topic0, numPartitions = 2, recordsPerPartition = 100)
    val props = new Properties()
    props.setProperty(LogConfig.MessageFormatVersionProp, "0.9.0")
    createTopic(topic1, numPartitions = 1, replicationFactor = 1, props)
    sendRecords(producer, numRecords = 100, new TopicPartition(topic1, 0))

    val t0p0 = new TopicPartition(topic0, 0)
    val t0p1 = new TopicPartition(topic0, 1)
    val t1p0 = new TopicPartition(topic1, 0)
    val partitions = Set(t0p0, t0p1, t1p0).asJava
    val consumer = createConsumer()

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
  def testUnsubscribeTopic(): Unit = {
    this.consumerConfig.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "100") // timeout quickly to avoid slow test
    this.consumerConfig.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "30")
    val consumer = createConsumer()

    val listener = new TestConsumerReassignmentListener()
    consumer.subscribe(List(topic).asJava, listener)

    // the initial subscription should cause a callback execution
    awaitRebalance(consumer, listener)

    consumer.subscribe(List[String]().asJava)
    assertEquals(0, consumer.assignment.size())
  }

  @Test
  def testPauseStateNotPreservedByRebalance(): Unit = {
    this.consumerConfig.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "100") // timeout quickly to avoid slow test
    this.consumerConfig.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "30")
    val consumer = createConsumer()

    val producer = createProducer()
    val startingTimestamp = System.currentTimeMillis()
    sendRecords(producer, numRecords = 5, tp, startingTimestamp = startingTimestamp)
    consumer.subscribe(List(topic).asJava)
    consumeAndVerifyRecords(consumer = consumer, numRecords = 5, startingOffset = 0, startingTimestamp = startingTimestamp)
    consumer.pause(List(tp).asJava)

    // subscribe to a new topic to trigger a rebalance
    consumer.subscribe(List("topic2").asJava)

    // after rebalance, our position should be reset and our pause state lost,
    // so we should be able to consume from the beginning
    consumeAndVerifyRecords(consumer = consumer, numRecords = 0, startingOffset = 5, startingTimestamp = startingTimestamp)
  }

  @Test
  def testCommitSpecifiedOffsets(): Unit = {
    val producer = createProducer()
    sendRecords(producer, numRecords = 5, tp)
    sendRecords(producer, numRecords = 7, tp2)

    val consumer = createConsumer()
    consumer.assign(List(tp, tp2).asJava)

    val pos1 = consumer.position(tp)
    val pos2 = consumer.position(tp2)
    consumer.commitSync(Map[TopicPartition, OffsetAndMetadata]((tp, new OffsetAndMetadata(3L))).asJava)
    assertEquals(3, consumer.committed(Set(tp).asJava).get(tp).offset)
    assertNull(consumer.committed(Set(tp2).asJava).get(tp2))

    // Positions should not change
    assertEquals(pos1, consumer.position(tp))
    assertEquals(pos2, consumer.position(tp2))
    consumer.commitSync(Map[TopicPartition, OffsetAndMetadata]((tp2, new OffsetAndMetadata(5L))).asJava)
    assertEquals(3, consumer.committed(Set(tp).asJava).get(tp).offset)
    assertEquals(5, consumer.committed(Set(tp2).asJava).get(tp2).offset)

    // Using async should pick up the committed changes after commit completes
    sendAndAwaitAsyncCommit(consumer, Some(Map(tp2 -> new OffsetAndMetadata(7L))))
    assertEquals(7, consumer.committed(Set(tp2).asJava).get(tp2).offset)
  }

  @Test
  def testAutoCommitOnRebalance(): Unit = {
    val topic2 = "topic2"
    createTopic(topic2, 2, brokerCount)

    this.consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    val consumer = createConsumer()

    val numRecords = 10000
    val producer = createProducer()
    sendRecords(producer, numRecords, tp)

    val rebalanceListener = new ConsumerRebalanceListener {
      override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]) = {
        // keep partitions paused in this test so that we can verify the commits based on specific seeks
        consumer.pause(partitions)
      }

      override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]) = {}
    }

    consumer.subscribe(List(topic).asJava, rebalanceListener)

    awaitAssignment(consumer, Set(tp, tp2))

    consumer.seek(tp, 300)
    consumer.seek(tp2, 500)

    // change subscription to trigger rebalance
    consumer.subscribe(List(topic, topic2).asJava, rebalanceListener)

    val newAssignment = Set(tp, tp2, new TopicPartition(topic2, 0), new TopicPartition(topic2, 1))
    awaitAssignment(consumer, newAssignment)

    // after rebalancing, we should have reset to the committed positions
    assertEquals(300, consumer.committed(Set(tp).asJava).get(tp).offset)
    assertEquals(500, consumer.committed(Set(tp2).asJava).get(tp2).offset)
  }

  @Test
  def testPerPartitionLeadMetricsCleanUpWithSubscribe(): Unit = {
    val numMessages = 1000
    val topic2 = "topic2"
    createTopic(topic2, 2, brokerCount)
    // send some messages.
    val producer = createProducer()
    sendRecords(producer, numMessages, tp)
    // Test subscribe
    // Create a consumer and consumer some messages.
    consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testPerPartitionLeadMetricsCleanUpWithSubscribe")
    consumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "testPerPartitionLeadMetricsCleanUpWithSubscribe")
    val consumer = createConsumer()
    val listener = new TestConsumerReassignmentListener
    consumer.subscribe(List(topic, topic2).asJava, listener)
    val records = awaitNonEmptyRecords(consumer, tp)
    assertEquals(1, listener.callsToAssigned, "should be assigned once")
    // Verify the metric exist.
    val tags1 = new util.HashMap[String, String]()
    tags1.put("client-id", "testPerPartitionLeadMetricsCleanUpWithSubscribe")
    tags1.put("topic", tp.topic())
    tags1.put("partition", String.valueOf(tp.partition()))

    val tags2 = new util.HashMap[String, String]()
    tags2.put("client-id", "testPerPartitionLeadMetricsCleanUpWithSubscribe")
    tags2.put("topic", tp2.topic())
    tags2.put("partition", String.valueOf(tp2.partition()))
    val fetchLead0 = consumer.metrics.get(new MetricName("records-lead", "consumer-fetch-manager-metrics", "", tags1))
    assertNotNull(fetchLead0)
    assertEquals(records.count.toDouble, fetchLead0.metricValue(), s"The lead should be ${records.count}")

    // Remove topic from subscription
    consumer.subscribe(List(topic2).asJava, listener)
    awaitRebalance(consumer, listener)
    // Verify the metric has gone
    assertNull(consumer.metrics.get(new MetricName("records-lead", "consumer-fetch-manager-metrics", "", tags1)))
    assertNull(consumer.metrics.get(new MetricName("records-lead", "consumer-fetch-manager-metrics", "", tags2)))
  }

  @Test
  def testPerPartitionLagMetricsCleanUpWithSubscribe(): Unit = {
    val numMessages = 1000
    val topic2 = "topic2"
    createTopic(topic2, 2, brokerCount)
    // send some messages.
    val producer = createProducer()
    sendRecords(producer, numMessages, tp)
    // Test subscribe
    // Create a consumer and consumer some messages.
    consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testPerPartitionLagMetricsCleanUpWithSubscribe")
    consumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "testPerPartitionLagMetricsCleanUpWithSubscribe")
    val consumer = createConsumer()
    val listener = new TestConsumerReassignmentListener
    consumer.subscribe(List(topic, topic2).asJava, listener)
    val records = awaitNonEmptyRecords(consumer, tp)
    assertEquals(1, listener.callsToAssigned, "should be assigned once")
    // Verify the metric exist.
    val tags1 = new util.HashMap[String, String]()
    tags1.put("client-id", "testPerPartitionLagMetricsCleanUpWithSubscribe")
    tags1.put("topic", tp.topic())
    tags1.put("partition", String.valueOf(tp.partition()))

    val tags2 = new util.HashMap[String, String]()
    tags2.put("client-id", "testPerPartitionLagMetricsCleanUpWithSubscribe")
    tags2.put("topic", tp2.topic())
    tags2.put("partition", String.valueOf(tp2.partition()))
    val fetchLag0 = consumer.metrics.get(new MetricName("records-lag", "consumer-fetch-manager-metrics", "", tags1))
    assertNotNull(fetchLag0)
    val expectedLag = numMessages - records.count
    assertEquals(expectedLag, fetchLag0.metricValue.asInstanceOf[Double], epsilon, s"The lag should be $expectedLag")

    // Remove topic from subscription
    consumer.subscribe(List(topic2).asJava, listener)
    awaitRebalance(consumer, listener)
    // Verify the metric has gone
    assertNull(consumer.metrics.get(new MetricName("records-lag", "consumer-fetch-manager-metrics", "", tags1)))
    assertNull(consumer.metrics.get(new MetricName("records-lag", "consumer-fetch-manager-metrics", "", tags2)))
  }

  @Test
  def testPerPartitionLeadMetricsCleanUpWithAssign(): Unit = {
    val numMessages = 1000
    // Test assign
    // send some messages.
    val producer = createProducer()
    sendRecords(producer, numMessages, tp)
    sendRecords(producer, numMessages, tp2)

    consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testPerPartitionLeadMetricsCleanUpWithAssign")
    consumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "testPerPartitionLeadMetricsCleanUpWithAssign")
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    val records = awaitNonEmptyRecords(consumer, tp)
    // Verify the metric exist.
    val tags = new util.HashMap[String, String]()
    tags.put("client-id", "testPerPartitionLeadMetricsCleanUpWithAssign")
    tags.put("topic", tp.topic())
    tags.put("partition", String.valueOf(tp.partition()))
    val fetchLead = consumer.metrics.get(new MetricName("records-lead", "consumer-fetch-manager-metrics", "", tags))
    assertNotNull(fetchLead)

    assertEquals(records.count.toDouble, fetchLead.metricValue(), s"The lead should be ${records.count}")

    consumer.assign(List(tp2).asJava)
    awaitNonEmptyRecords(consumer ,tp2)
    assertNull(consumer.metrics.get(new MetricName("records-lead", "consumer-fetch-manager-metrics", "", tags)))
  }

  @Test
  def testPerPartitionLagMetricsCleanUpWithAssign(): Unit = {
    val numMessages = 1000
    // Test assign
    // send some messages.
    val producer = createProducer()
    sendRecords(producer, numMessages, tp)
    sendRecords(producer, numMessages, tp2)

    consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testPerPartitionLagMetricsCleanUpWithAssign")
    consumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "testPerPartitionLagMetricsCleanUpWithAssign")
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    val records = awaitNonEmptyRecords(consumer, tp)
    // Verify the metric exist.
    val tags = new util.HashMap[String, String]()
    tags.put("client-id", "testPerPartitionLagMetricsCleanUpWithAssign")
    tags.put("topic", tp.topic())
    tags.put("partition", String.valueOf(tp.partition()))
    val fetchLag = consumer.metrics.get(new MetricName("records-lag", "consumer-fetch-manager-metrics", "", tags))
    assertNotNull(fetchLag)

    val expectedLag = numMessages - records.count
    assertEquals(expectedLag, fetchLag.metricValue.asInstanceOf[Double], epsilon, s"The lag should be $expectedLag")

    consumer.assign(List(tp2).asJava)
    awaitNonEmptyRecords(consumer, tp2)
    assertNull(consumer.metrics.get(new MetricName(tp.toString + ".records-lag", "consumer-fetch-manager-metrics", "", tags)))
    assertNull(consumer.metrics.get(new MetricName("records-lag", "consumer-fetch-manager-metrics", "", tags)))
  }

  @Test
  def testPerPartitionLagMetricsWhenReadCommitted(): Unit = {
    val numMessages = 1000
    // send some messages.
    val producer = createProducer()
    sendRecords(producer, numMessages, tp)
    sendRecords(producer, numMessages, tp2)

    consumerConfig.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
    consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testPerPartitionLagMetricsCleanUpWithAssign")
    consumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "testPerPartitionLagMetricsCleanUpWithAssign")
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    awaitNonEmptyRecords(consumer, tp)
    // Verify the metric exist.
    val tags = new util.HashMap[String, String]()
    tags.put("client-id", "testPerPartitionLagMetricsCleanUpWithAssign")
    tags.put("topic", tp.topic())
    tags.put("partition", String.valueOf(tp.partition()))
    val fetchLag = consumer.metrics.get(new MetricName("records-lag", "consumer-fetch-manager-metrics", "", tags))
    assertNotNull(fetchLag)
  }

  @Test
  def testPerPartitionLeadWithMaxPollRecords(): Unit = {
    val numMessages = 1000
    val maxPollRecords = 10
    val producer = createProducer()
    sendRecords(producer, numMessages, tp)

    consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testPerPartitionLeadWithMaxPollRecords")
    consumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "testPerPartitionLeadWithMaxPollRecords")
    consumerConfig.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords.toString)
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    awaitNonEmptyRecords(consumer, tp)

    val tags = new util.HashMap[String, String]()
    tags.put("client-id", "testPerPartitionLeadWithMaxPollRecords")
    tags.put("topic", tp.topic())
    tags.put("partition", String.valueOf(tp.partition()))
    val lead = consumer.metrics.get(new MetricName("records-lead", "consumer-fetch-manager-metrics", "", tags))
    assertEquals(maxPollRecords, lead.metricValue().asInstanceOf[Double], s"The lead should be $maxPollRecords")
  }

  @Test
  def testPerPartitionLagWithMaxPollRecords(): Unit = {
    val numMessages = 1000
    val maxPollRecords = 10
    val producer = createProducer()
    sendRecords(producer, numMessages, tp)

    consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testPerPartitionLagWithMaxPollRecords")
    consumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "testPerPartitionLagWithMaxPollRecords")
    consumerConfig.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords.toString)
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    val records = awaitNonEmptyRecords(consumer, tp)

    val tags = new util.HashMap[String, String]()
    tags.put("client-id", "testPerPartitionLagWithMaxPollRecords")
    tags.put("topic", tp.topic())
    tags.put("partition", String.valueOf(tp.partition()))
    val lag = consumer.metrics.get(new MetricName("records-lag", "consumer-fetch-manager-metrics", "", tags))

    assertEquals(numMessages - records.count, lag.metricValue.asInstanceOf[Double], epsilon, s"The lag should be ${numMessages - records.count}")
  }

  @Test
  def testQuotaMetricsNotCreatedIfNoQuotasConfigured(): Unit = {
    val numRecords = 1000
    val producer = createProducer()
    val startingTimestamp = System.currentTimeMillis()
    sendRecords(producer, numRecords, tp, startingTimestamp = startingTimestamp)

    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    consumer.seek(tp, 0)
    consumeAndVerifyRecords(consumer = consumer, numRecords = numRecords, startingOffset = 0, startingTimestamp = startingTimestamp)

    def assertNoMetric(broker: KafkaServer, name: String, quotaType: QuotaType, clientId: String): Unit = {
        val metricName = broker.metrics.metricName("throttle-time",
                                  quotaType.toString,
                                  "",
                                  "user", "",
                                  "client-id", clientId)
        assertNull(broker.metrics.metric(metricName), "Metric should not have been created " + metricName)
    }
    servers.foreach(assertNoMetric(_, "byte-rate", QuotaType.Produce, producerClientId))
    servers.foreach(assertNoMetric(_, "throttle-time", QuotaType.Produce, producerClientId))
    servers.foreach(assertNoMetric(_, "byte-rate", QuotaType.Fetch, consumerClientId))
    servers.foreach(assertNoMetric(_, "throttle-time", QuotaType.Fetch, consumerClientId))

    servers.foreach(assertNoMetric(_, "request-time", QuotaType.Request, producerClientId))
    servers.foreach(assertNoMetric(_, "throttle-time", QuotaType.Request, producerClientId))
    servers.foreach(assertNoMetric(_, "request-time", QuotaType.Request, consumerClientId))
    servers.foreach(assertNoMetric(_, "throttle-time", QuotaType.Request, consumerClientId))

    def assertNoExemptRequestMetric(broker: KafkaServer): Unit = {
        val metricName = broker.metrics.metricName("exempt-request-time", QuotaType.Request.toString, "")
        assertNull(broker.metrics.metric(metricName), "Metric should not have been created " + metricName)
    }
    servers.foreach(assertNoExemptRequestMetric)
  }

  def runMultiConsumerSessionTimeoutTest(closeConsumer: Boolean): Unit = {
    // use consumers defined in this class plus one additional consumer
    // Use topic defined in this class + one additional topic
    val producer = createProducer()
    sendRecords(producer, numRecords = 100, tp)
    sendRecords(producer, numRecords = 100, tp2)
    val topic1 = "topic1"
    val subscriptions = Set(tp, tp2) ++ createTopicAndSendRecords(producer, topic1, 6, 100)

    // first subscribe consumers that are defined in this class
    val consumerPollers = Buffer[ConsumerAssignmentPoller]()
    consumerPollers += subscribeConsumerAndStartPolling(createConsumer(), List(topic, topic1))
    consumerPollers += subscribeConsumerAndStartPolling(createConsumer(), List(topic, topic1))

    // create one more consumer and add it to the group; we will timeout this consumer
    val timeoutConsumer = createConsumer()
    val timeoutPoller = subscribeConsumerAndStartPolling(timeoutConsumer, List(topic, topic1))
    consumerPollers += timeoutPoller

    // validate the initial assignment
    validateGroupAssignment(consumerPollers, subscriptions)

    // stop polling and close one of the consumers, should trigger partition re-assignment among alive consumers
    timeoutPoller.shutdown()
    consumerPollers -= timeoutPoller
    if (closeConsumer)
      timeoutConsumer.close()

    validateGroupAssignment(consumerPollers, subscriptions,
      Some(s"Did not get valid assignment for partitions ${subscriptions.asJava} after one consumer left"), 3 * groupMaxSessionTimeoutMs)

    // done with pollers and consumers
    for (poller <- consumerPollers)
      poller.shutdown()
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
  def subscribeConsumers(consumerGroup: mutable.Buffer[KafkaConsumer[Array[Byte], Array[Byte]]],
                         topicsToSubscribe: List[String]): mutable.Buffer[ConsumerAssignmentPoller] = {
    val consumerPollers = mutable.Buffer[ConsumerAssignmentPoller]()
    for (consumer <- consumerGroup)
      consumerPollers += subscribeConsumerAndStartPolling(consumer, topicsToSubscribe)
    consumerPollers
  }

  /**
   * Creates topic 'topicName' with 'numPartitions' partitions and produces 'recordsPerPartition'
   * records to each partition
   */
  def createTopicAndSendRecords(producer: KafkaProducer[Array[Byte], Array[Byte]],
                                topicName: String,
                                numPartitions: Int,
                                recordsPerPartition: Int): Set[TopicPartition] = {
    createTopic(topicName, numPartitions, brokerCount)
    var parts = Set[TopicPartition]()
    for (partition <- 0 until numPartitions) {
      val tp = new TopicPartition(topicName, partition)
      sendRecords(producer, recordsPerPartition, tp)
      parts = parts + tp
    }
    parts
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
      consumerGroup += createConsumer()

    // create consumer pollers, wait for assignment and validate it
    val consumerPollers = subscribeConsumers(consumerGroup, topicsToSubscribe)
    (consumerGroup, consumerPollers)
  }

  def changeConsumerGroupSubscriptionAndValidateAssignment(consumerPollers: Buffer[ConsumerAssignmentPoller],
                                                           topicsToSubscribe: List[String],
                                                           subscriptions: Set[TopicPartition]): Unit = {
    for (poller <- consumerPollers)
      poller.subscribe(topicsToSubscribe)

    // since subscribe call to poller does not actually call consumer subscribe right away, wait
    // until subscribe is called on all consumers
    TestUtils.waitUntilTrue(() => {
      consumerPollers.forall { poller => poller.isSubscribeRequestProcessed }
    }, s"Failed to call subscribe on all consumers in the group for subscription $subscriptions", 1000L)

    validateGroupAssignment(consumerPollers, subscriptions,
      Some(s"Did not get valid assignment for partitions ${subscriptions.asJava} after we changed subscription"))
  }

  def changeConsumerSubscriptionAndValidateAssignment[K, V](consumer: Consumer[K, V],
                                                            topicsToSubscribe: List[String],
                                                            expectedAssignment: Set[TopicPartition],
                                                            rebalanceListener: ConsumerRebalanceListener): Unit = {
    consumer.subscribe(topicsToSubscribe.asJava, rebalanceListener)
    awaitAssignment(consumer, expectedAssignment)
  }

  private def awaitNonEmptyRecords[K, V](consumer: Consumer[K, V], partition: TopicPartition): ConsumerRecords[K, V] = {
    TestUtils.pollRecordsUntilTrue(consumer, (polledRecords: ConsumerRecords[K, V]) => {
      if (polledRecords.records(partition).asScala.nonEmpty)
        return polledRecords
      false
    }, s"Consumer did not consume any messages for partition $partition before timeout.")
    throw new IllegalStateException("Should have timed out before reaching here")
  }

  private def awaitAssignment(consumer: Consumer[_, _], expectedAssignment: Set[TopicPartition]): Unit = {
    TestUtils.pollUntilTrue(consumer, () => consumer.assignment() == expectedAssignment.asJava,
      s"Timed out while awaiting expected assignment $expectedAssignment. " +
        s"The current assignment is ${consumer.assignment()}")
  }

  @Test
  def testConsumingWithNullGroupId(): Unit = {
    val topic = "test_topic"
    val partition = 0;
    val tp = new TopicPartition(topic, partition)
    createTopic(topic, 1, 1)

    TestUtils.waitUntilTrue(() => {
      this.zkClient.topicExists(topic)
    }, "Failed to create topic")

    val producer = createProducer()
    producer.send(new ProducerRecord(topic, partition, "k1".getBytes, "v1".getBytes)).get()
    producer.send(new ProducerRecord(topic, partition, "k2".getBytes, "v2".getBytes)).get()
    producer.send(new ProducerRecord(topic, partition, "k3".getBytes, "v3".getBytes)).get()
    producer.close()

    // consumer 1 uses the default group id and consumes from earliest offset
    val consumer1Config = new Properties(consumerConfig)
    consumer1Config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    consumer1Config.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer1")
    val consumer1 = createConsumer(
      configOverrides = consumer1Config,
      configsToRemove = List(ConsumerConfig.GROUP_ID_CONFIG))

    // consumer 2 uses the default group id and consumes from latest offset
    val consumer2Config = new Properties(consumerConfig)
    consumer2Config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    consumer2Config.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer2")
    val consumer2 = createConsumer(
      configOverrides = consumer2Config,
      configsToRemove = List(ConsumerConfig.GROUP_ID_CONFIG))

    // consumer 3 uses the default group id and starts from an explicit offset
    val consumer3Config = new Properties(consumerConfig)
    consumer3Config.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer3")
    val consumer3 = createConsumer(
      configOverrides = consumer3Config,
      configsToRemove = List(ConsumerConfig.GROUP_ID_CONFIG))

    consumer1.assign(asList(tp))
    consumer2.assign(asList(tp))
    consumer3.assign(asList(tp))
    consumer3.seek(tp, 1)

    val numRecords1 = consumer1.poll(Duration.ofMillis(5000)).count()
    assertThrows(classOf[InvalidGroupIdException], () => consumer1.commitSync())
    assertThrows(classOf[InvalidGroupIdException], () => consumer2.committed(Set(tp).asJava))

    val numRecords2 = consumer2.poll(Duration.ofMillis(5000)).count()
    val numRecords3 = consumer3.poll(Duration.ofMillis(5000)).count()

    consumer1.unsubscribe()
    consumer2.unsubscribe()
    consumer3.unsubscribe()

    consumer1.close()
    consumer2.close()
    consumer3.close()

    assertEquals(3, numRecords1, "Expected consumer1 to consume from earliest offset")
    assertEquals(0, numRecords2, "Expected consumer2 to consume from latest offset")
    assertEquals(2, numRecords3, "Expected consumer3 to consume from offset 1")
  }

  @Test
  def testConsumingWithEmptyGroupId(): Unit = {
    val topic = "test_topic"
    val partition = 0;
    val tp = new TopicPartition(topic, partition)
    createTopic(topic, 1, 1)

    TestUtils.waitUntilTrue(() => {
      this.zkClient.topicExists(topic)
    }, "Failed to create topic")

    val producer = createProducer()
    producer.send(new ProducerRecord(topic, partition, "k1".getBytes, "v1".getBytes)).get()
    producer.send(new ProducerRecord(topic, partition, "k2".getBytes, "v2".getBytes)).get()
    producer.close()

    // consumer 1 uses the empty group id
    val consumer1Config = new Properties(consumerConfig)
    consumer1Config.put(ConsumerConfig.GROUP_ID_CONFIG, "")
    consumer1Config.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer1")
    consumer1Config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1")
    val consumer1 = createConsumer(configOverrides = consumer1Config)

    // consumer 2 uses the empty group id and consumes from latest offset if there is no committed offset
    val consumer2Config = new Properties(consumerConfig)
    consumer2Config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    consumer2Config.put(ConsumerConfig.GROUP_ID_CONFIG, "")
    consumer2Config.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer2")
    consumer2Config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1")
    val consumer2 = createConsumer(configOverrides = consumer2Config)

    consumer1.assign(asList(tp))
    consumer2.assign(asList(tp))

    val records1 = consumer1.poll(Duration.ofMillis(5000))
    consumer1.commitSync()

    val records2 = consumer2.poll(Duration.ofMillis(5000))
    consumer2.commitSync()

    consumer1.close()
    consumer2.close()

    assertTrue(records1.count() == 1 && records1.records(tp).asScala.head.offset == 0,
      "Expected consumer1 to consume one message from offset 0")
    assertTrue(records2.count() == 1 && records2.records(tp).asScala.head.offset == 1,
      "Expected consumer2 to consume one message from offset 1, which is the committed offset of consumer1")
  }
}
