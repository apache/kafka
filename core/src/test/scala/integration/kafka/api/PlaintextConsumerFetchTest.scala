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

import kafka.utils.TestInfoUtils
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.apache.kafka.common.TopicPartition

import java.time.{Duration, Instant}
import scala.jdk.CollectionConverters._

/**
 * Integration tests for the consumer that covers fetching logic
 */
@Timeout(600)
class PlaintextConsumerFetchTest extends AbstractConsumerTest {

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testFetchInvalidOffset(quorum: String, groupProtocol: String): Unit = {
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

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testFetchOutOfRangeOffsetResetConfigEarliest(quorum: String, groupProtocol: String): Unit = {
    this.consumerConfig.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    // ensure no in-flight fetch request so that the offset can be reset immediately
    this.consumerConfig.setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "0")
    val consumer = createConsumer(configOverrides = this.consumerConfig)
    val totalRecords = 10L

    val producer = createProducer()
    val startingTimestamp = 0
    sendRecords(producer, totalRecords.toInt, tp, startingTimestamp = startingTimestamp)
    consumer.assign(List(tp).asJava)
    consumeAndVerifyRecords(consumer = consumer, numRecords = totalRecords.toInt, startingOffset = 0)
    // seek to out of range position
    val outOfRangePos = totalRecords + 1
    consumer.seek(tp, outOfRangePos)
    // assert that poll resets to the beginning position
    consumeAndVerifyRecords(consumer = consumer, numRecords = 1, startingOffset = 0)
  }


  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testFetchOutOfRangeOffsetResetConfigLatest(quorum: String, groupProtocol: String): Unit = {
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
    consumeAndVerifyRecords(consumer = consumer, numRecords = totalRecords.toInt / 2, startingOffset = 0)
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

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testFetchOutOfRangeOffsetResetConfigByDuration(quorum: String, groupProtocol: String): Unit = {
    this.consumerConfig.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "by_duration:PT1H")
    // ensure no in-flight fetch request so that the offset can be reset immediately
    this.consumerConfig.setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "0")

    // Test the scenario where the requested duration much earlier than the starting offset
    val consumer1 = createConsumer(configOverrides = this.consumerConfig)
    val producer1 = createProducer()
    val totalRecords = 10L
    var startingTimestamp = System.currentTimeMillis()
    sendRecords(producer1, totalRecords.toInt, tp, startingTimestamp = startingTimestamp)
    consumer1.assign(List(tp).asJava)
    consumeAndVerifyRecords(consumer = consumer1, numRecords = totalRecords.toInt, startingOffset = 0, startingTimestamp = startingTimestamp)

    // seek to out of range position
    var outOfRangePos = totalRecords + 1
    consumer1.seek(tp, outOfRangePos)
    // assert that poll resets to the beginning position
    consumeAndVerifyRecords(consumer = consumer1, numRecords = 1, startingOffset = 0, startingTimestamp = startingTimestamp)

    // Test the scenario where starting offset is earlier than the requested duration
    val consumer2 = createConsumer(configOverrides = this.consumerConfig)
    val producer2 = createProducer()
    val totalRecords2 = 25L
    startingTimestamp = Instant.now().minus(Duration.ofHours(24)).toEpochMilli
    //generate records with 1 hour interval for 1 day
    sendRecords(producer2, totalRecords2.toInt, tp2, startingTimestamp = startingTimestamp, Duration.ofHours(1).toMillis)
    consumer2.assign(List(tp2).asJava)
    //consumer should read one record from last one hour
    consumeAndVerifyRecords(consumer = consumer2, numRecords = 1, startingOffset = 24, startingKeyAndValueIndex = 24,
      startingTimestamp = startingTimestamp + 24 * Duration.ofHours(1).toMillis,
      tp  = tp2,
      timestampIncrement = Duration.ofHours(1).toMillis)

    // seek to out of range position
    outOfRangePos = totalRecords2 + 1
    consumer2.seek(tp2, outOfRangePos)
    // assert that poll resets to the duration offset. consumer should read one record from last one hour
    consumeAndVerifyRecords(consumer = consumer2, numRecords = 1, startingOffset = 24, startingKeyAndValueIndex = 24,
      startingTimestamp = startingTimestamp + 24 * Duration.ofHours(1).toMillis,
      tp  = tp2,
      timestampIncrement = Duration.ofHours(1).toMillis)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testFetchRecordLargerThanFetchMaxBytes(quorum: String, groupProtocol: String): Unit = {
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
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testFetchHonoursFetchSizeIfLargeRecordNotFirst(quorum: String, groupProtocol: String): Unit = {
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
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testFetchHonoursMaxPartitionFetchBytesIfLargeRecordNotFirst(quorum: String, groupProtocol: String): Unit = {
    val maxPartitionFetchBytes = 10 * 1024
    this.consumerConfig.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, maxPartitionFetchBytes.toString)
    checkFetchHonoursSizeIfLargeRecordNotFirst(maxPartitionFetchBytes)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testFetchRecordLargerThanMaxPartitionFetchBytes(quorum: String, groupProtocol: String): Unit = {
    val maxPartitionFetchBytes = 10 * 1024
    this.consumerConfig.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, maxPartitionFetchBytes.toString)
    checkLargeRecord(maxPartitionFetchBytes + 1)
  }

  /** Test that we consume all partitions if fetch max bytes and max.partition.fetch.bytes are low */
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testLowMaxFetchSizeForRequestAndPartition(quorum: String, groupProtocol: String): Unit = {
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

}
