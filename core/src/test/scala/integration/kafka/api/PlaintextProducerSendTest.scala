/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
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

import java.util.Properties
import java.util.concurrent.{ExecutionException, Future, TimeUnit}
import kafka.log.LogConfig
import kafka.server.Defaults
import kafka.utils.{TestInfoUtils, TestUtils}
import org.apache.kafka.clients.producer.{BufferExhaustedException, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.errors.{InvalidTimestampException, RecordTooLargeException, SerializationException, TimeoutException}
import org.apache.kafka.common.record.{DefaultRecord, DefaultRecordBatch, Records, TimestampType}
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.nio.charset.StandardCharsets


class PlaintextProducerSendTest extends BaseProducerSendTest {

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testWrongSerializer(quorum: String): Unit = {
    val producerProps = new Properties()
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers())
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    val producer = registerProducer(new KafkaProducer(producerProps))
    val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, 0, "key".getBytes, "value".getBytes)
    assertThrows(classOf[SerializationException], () => producer.send(record))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testBatchSizeZero(quorum: String): Unit = {
    val producer = createProducer(
      lingerMs = Int.MaxValue,
      deliveryTimeoutMs = Int.MaxValue,
      batchSize = 0)
    sendAndVerify(producer)
  }

  @Timeout(value = 15, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testBatchSizeZeroNoPartitionNoRecordKey(quorum: String): Unit = {
    val producer = createProducer(batchSize = 0)
    val numRecords = 10;
    try {
      TestUtils.createTopicWithAdmin(admin, topic, brokers, 2)
      val futures = for (i <- 1 to numRecords) yield {
        val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, null, s"value$i".getBytes(StandardCharsets.UTF_8))
        producer.send(record)
      }
      producer.flush()
      val lastOffset = futures.foldLeft(0) { (offset, future) =>
        val recordMetadata = future.get
        assertEquals(topic, recordMetadata.topic)
        offset + 1
      }
      assertEquals(numRecords, lastOffset)
    } finally {
      producer.close()
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testSendCompressedMessageWithLogAppendTime(quorum: String): Unit = {
    val producer = createProducer(
      compressionType = "gzip",
      lingerMs = Int.MaxValue,
      deliveryTimeoutMs = Int.MaxValue)
    sendAndVerifyTimestamp(producer, TimestampType.LOG_APPEND_TIME)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testSendNonCompressedMessageWithLogAppendTime(quorum: String): Unit = {
    val producer = createProducer(lingerMs = Int.MaxValue, deliveryTimeoutMs = Int.MaxValue)
    sendAndVerifyTimestamp(producer, TimestampType.LOG_APPEND_TIME)
  }

  /**
   * testAutoCreateTopic
   *
   * The topic should be created upon sending the first message
   */
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testAutoCreateTopic(quorum: String): Unit = {
    val producer = createProducer()
    try {
      // Send a message to auto-create the topic
      val record = new ProducerRecord(topic, null, "key".getBytes, "value".getBytes)
      assertEquals(0L, producer.send(record).get.offset, "Should have offset 0")

      // double check that the topic is created with leader elected
      TestUtils.waitUntilLeaderIsElectedOrChangedWithAdmin(admin, topic, 0)
    } finally {
      producer.close()
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testSendWithInvalidCreateTime(quorum: String): Unit = {
    val topicProps = new Properties()
    topicProps.setProperty(LogConfig.MessageTimestampDifferenceMaxMsProp, "1000")
    TestUtils.createTopicWithAdmin(admin, topic, brokers, 1, 2, topicConfig = topicProps)

    val producer = createProducer()
    try {
      val e = assertThrows(classOf[ExecutionException],
        () => producer.send(new ProducerRecord(topic, 0, System.currentTimeMillis() - 1001, "key".getBytes, "value".getBytes)).get()).getCause
      assertTrue(e.isInstanceOf[InvalidTimestampException])
    } finally {
      producer.close()
    }

    // Test compressed messages.
    val compressedProducer = createProducer(compressionType = "gzip")
    try {
      val e = assertThrows(classOf[ExecutionException],
        () => compressedProducer.send(new ProducerRecord(topic, 0, System.currentTimeMillis() - 1001, "key".getBytes, "value".getBytes)).get()).getCause
      assertTrue(e.isInstanceOf[InvalidTimestampException])
    } finally {
      compressedProducer.close()
    }
  }

  // Test that producer with max.block.ms=0 can be used to send in non-blocking mode
  // where requests are failed immediately without blocking if metadata is not available
  // or buffer is full.
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testNonBlockingProducer(quorum: String): Unit = {

    def send(producer: KafkaProducer[Array[Byte],Array[Byte]]): Future[RecordMetadata] = {
      producer.send(new ProducerRecord(topic, 0, "key".getBytes, new Array[Byte](1000)))
    }

    def sendUntilQueued(producer: KafkaProducer[Array[Byte],Array[Byte]]): Future[RecordMetadata] = {
      val (future, _) = TestUtils.computeUntilTrue(send(producer))(future => {
        if (future.isDone) {
          try {
            future.get
            true  // Send was queued and completed successfully
          } catch {
            case _: ExecutionException => false
          }
        } else
          true    // Send future not yet complete, so it has been queued to be sent
      })
      future
    }

    def verifySendSuccess(future: Future[RecordMetadata]): Unit = {
      val recordMetadata = future.get(30, TimeUnit.SECONDS)
      assertEquals(topic, recordMetadata.topic)
      assertEquals(0, recordMetadata.partition)
      assertTrue(recordMetadata.offset >= 0, s"Invalid offset $recordMetadata")
    }

    def verifyMetadataNotAvailable(future: Future[RecordMetadata]): Unit = {
      assertTrue(future.isDone)  // verify future was completed immediately
      assertEquals(classOf[TimeoutException], assertThrows(classOf[ExecutionException], () => future.get).getCause.getClass)
    }

    def verifyBufferExhausted(future: Future[RecordMetadata]): Unit = {
      assertTrue(future.isDone)  // verify future was completed immediately
      assertEquals(classOf[BufferExhaustedException], assertThrows(classOf[ExecutionException], () => future.get).getCause.getClass)
    }

    // Topic metadata not available, send should fail without blocking
    val producer = createProducer(maxBlockMs = 0)
    verifyMetadataNotAvailable(send(producer))

    // Test that send starts succeeding once metadata is available
    val future = sendUntilQueued(producer)
    verifySendSuccess(future)

    // Verify that send fails immediately without blocking when there is no space left in the buffer
    val producer2 = createProducer(maxBlockMs = 0,
                                   lingerMs = 15000, batchSize = 1100, bufferSize = 1500)
    val future2 = sendUntilQueued(producer2) // wait until metadata is available and one record is queued
    verifyBufferExhausted(send(producer2))       // should fail send since buffer is full
    verifySendSuccess(future2)               // previous batch should be completed and sent now
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testSendRecordBatchWithMaxRequestSizeAndHigher(quorum: String): Unit = {
    val producerProps = new Properties()
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers())
    val producer = registerProducer(new KafkaProducer(producerProps, new ByteArraySerializer, new ByteArraySerializer))

    val keyLengthSize = 1
    val headerLengthSize = 1
    val valueLengthSize = 3
    val overhead = Records.LOG_OVERHEAD + DefaultRecordBatch.RECORD_BATCH_OVERHEAD + DefaultRecord.MAX_RECORD_OVERHEAD +
      keyLengthSize + headerLengthSize + valueLengthSize
    val valueSize = Defaults.MessageMaxBytes - overhead

    val record0 = new ProducerRecord(topic, new Array[Byte](0), new Array[Byte](valueSize))
    assertEquals(record0.value.length, producer.send(record0).get.serializedValueSize)

    val record1 = new ProducerRecord(topic, new Array[Byte](0), new Array[Byte](valueSize + 1))
    assertEquals(classOf[RecordTooLargeException], assertThrows(classOf[ExecutionException], () => producer.send(record1).get).getCause.getClass)
  }

}
