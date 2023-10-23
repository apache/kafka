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

import kafka.server.{KafkaServer, QuotaType}
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.{NewPartitions, NewTopic}
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.errors.{InvalidGroupIdException, InvalidTopicException}
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.record.{CompressionType, TimestampType}
import org.apache.kafka.common.serialization._
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.{MetricName, TopicPartition}
import org.apache.kafka.test.{MockConsumerInterceptor, MockProducerInterceptor}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{Disabled, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.time.Duration
import java.util
import java.util.Arrays.asList
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import java.util.regex.Pattern
import java.util.{Locale, Optional, Properties}
import scala.collection.mutable
import scala.collection.mutable.Buffer
import scala.jdk.CollectionConverters._

/* This is a copy of the PlaintextAsyncConsumer class but it disables the tests that are not
yet supported by the async consumer. Disabled tests will be enabled as the async consumer
evolves. The moment all tests are supported this file will be deleted (KAFKA-15515). */
class PlaintextAsyncConsumer extends BaseConsumerTest {

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
  def testAutoOffsetReset(): Unit = {
    val producer = createProducer()
    val startingTimestamp = System.currentTimeMillis()
    sendRecords(producer, numRecords = 1, tp, startingTimestamp = startingTimestamp)

    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    consumeAndVerifyRecords(consumer = consumer, numRecords = 1, startingOffset = 0, startingTimestamp = startingTimestamp)
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
    val consumer = createConsumer(configOverrides = this.consumerConfig)

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
  def testFetchOutOfRangeOffsetResetConfigEarliest(): Unit = {
    this.consumerConfig.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    // ensure no in-flight fetch request so that the offset can be reset immediately
    this.consumerConfig.setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "0")
    val consumer = createConsumer(configOverrides = this.consumerConfig)
    val totalRecords = 10L

    val producer = createProducer()
    val startingTimestamp = 0
    sendRecords(producer, totalRecords.toInt, tp, startingTimestamp = startingTimestamp)
    consumer.assign(List(tp).asJava)
    consumeAndVerifyRecords(consumer = consumer, numRecords = totalRecords.toInt, startingOffset =0)
    // seek to out of range position
    val outOfRangePos = totalRecords + 1
    consumer.seek(tp, outOfRangePos)
    // assert that poll resets to the beginning position
    consumeAndVerifyRecords(consumer = consumer, numRecords = 1, startingOffset = 0)
  }


  @Test
  def testFetchOutOfRangeOffsetResetConfigLatest(): Unit = {
    this.consumerConfig.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    // ensure no in-flight fetch request so that the offset can be reset immediately
    this.consumerConfig.setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "0")
    val consumer = createConsumer(configOverrides = this.consumerConfig)
    val totalRecords = 10L

    val producer = createProducer()
    val startingTimestamp = 0
    sendRecords(producer, totalRecords.toInt, tp, startingTimestamp = startingTimestamp)
    consumer.assign(List(tp).asJava)
    consumer.seek(tp, 0)
    // consume some, but not all of the records
    consumeAndVerifyRecords(consumer = consumer, numRecords = totalRecords.toInt/2, startingOffset = 0)
    // seek to out of range position
    val outOfRangePos = totalRecords + 17 // arbitrary, much higher offset
    consumer.seek(tp, outOfRangePos)
    // assert that poll resets to the ending position
    assertTrue(consumer.poll(Duration.ofMillis(50)).isEmpty)
    sendRecords(producer, totalRecords.toInt, tp, startingTimestamp = totalRecords)
    val nextRecord = consumer.poll(Duration.ofMillis(50)).iterator().next()
    // ensure the seek went to the last known record at the time of the previous poll
    assertEquals(totalRecords, nextRecord.offset())
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
    topicProps.setProperty(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, "LogAppendTime")
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
  def subscribeConsumers(consumerGroup: mutable.Buffer[Consumer[Array[Byte], Array[Byte]]],
                         topicsToSubscribe: List[String]): mutable.Buffer[ConsumerAssignmentPoller] = {
    val consumerPollers = mutable.Buffer[ConsumerAssignmentPoller]()
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
                                              subscriptions: Set[TopicPartition]): (Buffer[Consumer[Array[Byte], Array[Byte]]], Buffer[ConsumerAssignmentPoller]) = {
    assertTrue(consumerCount <= subscriptions.size)
    val consumerGroup = Buffer[Consumer[Array[Byte], Array[Byte]]]()
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

  @Disabled("Not supported by async consumer yet")
  @Test
  override def testCoordinatorFailover(): Unit = {
    super.testCoordinatorFailover()
  }

  @Disabled("Not supported by async consumer yet")
  @Test
  override def testClusterResourceListener(): Unit = {
    super.testClusterResourceListener()
  }
}