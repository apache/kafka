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

import java.nio.charset.StandardCharsets
import java.util.Properties
import java.util.concurrent.TimeUnit

import collection.JavaConverters._
import kafka.integration.KafkaServerTestHarness
import kafka.log.LogConfig
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.junit.Assert._
import org.junit.{After, Before, Test}

import scala.collection.mutable.{ArrayBuffer, Buffer}

abstract class BaseProducerSendTest extends KafkaServerTestHarness {

  def generateConfigs = {
    val overridingProps = new Properties()
    val numServers = 2
    overridingProps.put(KafkaConfig.NumPartitionsProp, 4.toString)
    TestUtils.createBrokerConfigs(numServers, zkConnect, false, interBrokerSecurityProtocol = Some(securityProtocol),
      trustStoreFile = trustStoreFile, saslProperties = serverSaslProperties).map(KafkaConfig.fromProps(_, overridingProps))
  }

  private var consumer: KafkaConsumer[Array[Byte], Array[Byte]] = _
  private val producers = Buffer[KafkaProducer[Array[Byte], Array[Byte]]]()

  protected val topic = "topic"
  private val numRecords = 100

  @Before
  override def setUp() {
    super.setUp()
    consumer = TestUtils.createConsumer(TestUtils.getBrokerListStrFromServers(servers), securityProtocol = SecurityProtocol.PLAINTEXT)
  }

  @After
  override def tearDown() {
    consumer.close()
    // Ensure that all producers are closed since unclosed producers impact other tests when Kafka server ports are reused
    producers.foreach(_.close())

    super.tearDown()
  }

  protected def createProducer(brokerList: String, retries: Int = 0, lingerMs: Long = 0, props: Option[Properties] = None): KafkaProducer[Array[Byte],Array[Byte]] = {
    val producer = TestUtils.createProducer(brokerList, securityProtocol = securityProtocol, trustStoreFile = trustStoreFile,
      saslProperties = clientSaslProperties, retries = retries, lingerMs = lingerMs, props = props)
    registerProducer(producer)
  }

  protected def registerProducer(producer: KafkaProducer[Array[Byte], Array[Byte]]): KafkaProducer[Array[Byte], Array[Byte]] = {
    producers += producer
    producer
  }

  private def pollUntilNumRecords(numRecords: Int) : Seq[ConsumerRecord[Array[Byte], Array[Byte]]] = {
    val records = new ArrayBuffer[ConsumerRecord[Array[Byte], Array[Byte]]]()
    TestUtils.waitUntilTrue(() => {
      records ++= consumer.poll(50).asScala
      records.size == numRecords
    }, s"Consumed ${records.size} records until timeout, but expected $numRecords records.")
    records
  }

  /**
   * testSendOffset checks the basic send API behavior
   *
   * 1. Send with null key/value/partition-id should be accepted; send with null topic should be rejected.
   * 2. Last message of the non-blocking send should return the correct offset metadata
   */
  @Test
  def testSendOffset() {
    val producer = createProducer(brokerList)
    val partition = 0

    object callback extends Callback {
      var offset = 0L

      def onCompletion(metadata: RecordMetadata, exception: Exception) {
        if (exception == null) {
          assertEquals(offset, metadata.offset())
          assertEquals(topic, metadata.topic())
          assertEquals(partition, metadata.partition())
          offset match {
            case 0 => assertEquals(metadata.serializedKeySize + metadata.serializedValueSize,
              "key".getBytes(StandardCharsets.UTF_8).length + "value".getBytes(StandardCharsets.UTF_8).length)
            case 1 => assertEquals(metadata.serializedKeySize(), "key".getBytes(StandardCharsets.UTF_8).length)
            case 2 => assertEquals(metadata.serializedValueSize, "value".getBytes(StandardCharsets.UTF_8).length)
            case _ => assertTrue(metadata.serializedValueSize > 0)
          }
          assertNotEquals(metadata.checksum(), 0)
          offset += 1
        } else {
          fail("Send callback returns the following exception", exception)
        }
      }
    }

    try {
      // create topic
      createTopic(topic, 1, 2)

      // send a normal record
      val record0 = new ProducerRecord[Array[Byte], Array[Byte]](topic, partition, "key".getBytes(StandardCharsets.UTF_8),
        "value".getBytes(StandardCharsets.UTF_8))
      assertEquals("Should have offset 0", 0L, producer.send(record0, callback).get.offset)

      // send a record with null value should be ok
      val record1 = new ProducerRecord[Array[Byte], Array[Byte]](topic, partition, "key".getBytes(StandardCharsets.UTF_8), null)
      assertEquals("Should have offset 1", 1L, producer.send(record1, callback).get.offset)

      // send a record with null key should be ok
      val record2 = new ProducerRecord[Array[Byte], Array[Byte]](topic, partition, null, "value".getBytes(StandardCharsets.UTF_8))
      assertEquals("Should have offset 2", 2L, producer.send(record2, callback).get.offset)

      // send a record with null part id should be ok
      val record3 = new ProducerRecord[Array[Byte], Array[Byte]](topic, null, "key".getBytes(StandardCharsets.UTF_8),
        "value".getBytes(StandardCharsets.UTF_8))
      assertEquals("Should have offset 3", 3L, producer.send(record3, callback).get.offset)

      // send a record with null topic should fail
      try {
        val record4 = new ProducerRecord[Array[Byte], Array[Byte]](null, partition, "key".getBytes(StandardCharsets.UTF_8),
          "value".getBytes(StandardCharsets.UTF_8))
        producer.send(record4, callback)
        fail("Should not allow sending a record without topic")
      } catch {
        case _: IllegalArgumentException => // this is ok
      }

      // non-blocking send a list of records
      for (_ <- 1 to numRecords)
        producer.send(record0, callback)

      // check that all messages have been acked via offset
      assertEquals("Should have offset " + (numRecords + 4), numRecords + 4L, producer.send(record0, callback).get.offset)

    } finally {
      producer.close()
    }
  }

  @Test
  def testSendCompressedMessageWithCreateTime() {
    val producerProps = new Properties()
    producerProps.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip")
    val producer = createProducer(brokerList = brokerList, lingerMs = Long.MaxValue, props = Some(producerProps))
    sendAndVerifyTimestamp(producer, TimestampType.CREATE_TIME)
  }

  @Test
  def testSendNonCompressedMessageWithCreateTime() {
    val producer = createProducer(brokerList = brokerList, lingerMs = Long.MaxValue)
    sendAndVerifyTimestamp(producer, TimestampType.CREATE_TIME)
  }

  protected def sendAndVerify(producer: KafkaProducer[Array[Byte], Array[Byte]],
                              numRecords: Int = numRecords,
                              timeoutMs: Long = 20000L) {
    val partition = 0
    try {
      createTopic(topic, 1, 2)

      val futures = for (i <- 1 to numRecords) yield {
        val record = new ProducerRecord(topic, partition, s"key$i".getBytes(StandardCharsets.UTF_8),
          s"value$i".getBytes(StandardCharsets.UTF_8))
        producer.send(record)
      }
      producer.close(timeoutMs, TimeUnit.MILLISECONDS)
      val lastOffset = futures.foldLeft(0) { (offset, future) =>
        val recordMetadata = future.get
        assertEquals(topic, recordMetadata.topic)
        assertEquals(partition, recordMetadata.partition)
        assertEquals(offset, recordMetadata.offset)
        offset + 1
      }
      assertEquals(numRecords, lastOffset)
    } finally {
      producer.close()
    }
  }

  protected def sendAndVerifyTimestamp(producer: KafkaProducer[Array[Byte], Array[Byte]], timestampType: TimestampType) {
    val partition = 0

    val baseTimestamp = 123456L
    val startTime = System.currentTimeMillis()

    object callback extends Callback {
      var offset = 0L
      var timestampDiff = 1L

      def onCompletion(metadata: RecordMetadata, exception: Exception) {
        if (exception == null) {
          assertEquals(offset, metadata.offset)
          assertEquals(topic, metadata.topic)
          if (timestampType == TimestampType.CREATE_TIME)
            assertEquals(baseTimestamp + timestampDiff, metadata.timestamp)
          else
            assertTrue(metadata.timestamp >= startTime && metadata.timestamp <= System.currentTimeMillis())
          assertEquals(partition, metadata.partition)
          offset += 1
          timestampDiff += 1
        } else {
          fail("Send callback returns the following exception", exception)
        }
      }
    }

    try {
      // create topic
      val topicProps = new Properties()
      if (timestampType == TimestampType.LOG_APPEND_TIME)
        topicProps.setProperty(LogConfig.MessageTimestampTypeProp, "LogAppendTime")
      else
        topicProps.setProperty(LogConfig.MessageTimestampTypeProp, "CreateTime")
      createTopic(topic, 1, 2, topicProps)

      val recordAndFutures = for (i <- 1 to numRecords) yield {
        val record = new ProducerRecord(topic, partition, baseTimestamp + i, s"key$i".getBytes(StandardCharsets.UTF_8),
          s"value$i".getBytes(StandardCharsets.UTF_8))
        (record, producer.send(record, callback))
      }
      producer.close(20000L, TimeUnit.MILLISECONDS)
      recordAndFutures.foreach { case (record, future) =>
        val recordMetadata = future.get
        if (timestampType == TimestampType.LOG_APPEND_TIME)
          assertTrue(recordMetadata.timestamp >= startTime && recordMetadata.timestamp <= System.currentTimeMillis())
        else
          assertEquals(record.timestamp, recordMetadata.timestamp)
      }
      assertEquals(s"Should have offset $numRecords but only successfully sent ${callback.offset}", numRecords, callback.offset)
    } finally {
      producer.close()
    }
  }

  /**
   * testClose checks the closing behavior
   *
   * After close() returns, all messages should be sent with correct returned offset metadata
   */
  @Test
  def testClose() {
    val producer = createProducer(brokerList)

    try {
      // create topic
      createTopic(topic, 1, 2)

      // non-blocking send a list of records
      val record0 = new ProducerRecord[Array[Byte], Array[Byte]](topic, null, "key".getBytes(StandardCharsets.UTF_8),
        "value".getBytes(StandardCharsets.UTF_8))
      for (_ <- 1 to numRecords)
        producer.send(record0)
      val response0 = producer.send(record0)

      // close the producer
      producer.close()

      // check that all messages have been acked via offset,
      // this also checks that messages with same key go to the same partition
      assertTrue("The last message should be acked before producer is shutdown", response0.isDone)
      assertEquals("Should have offset " + numRecords, numRecords.toLong, response0.get.offset)

    } finally {
      producer.close()
    }
  }

  /**
   * testSendToPartition checks the partitioning behavior
   *
   * The specified partition-id should be respected
   */
  @Test
  def testSendToPartition() {
    val producer = createProducer(brokerList)

    try {
      createTopic(topic, 2, 2)
      val partition = 1

      val now = System.currentTimeMillis()
      val futures = (1 to numRecords).map { i =>
        producer.send(new ProducerRecord(topic, partition, now, null, ("value" + i).getBytes(StandardCharsets.UTF_8)))
      }.map(_.get(30, TimeUnit.SECONDS))

      // make sure all of them end up in the same partition with increasing offset values
      for ((recordMetadata, offset) <- futures zip (0 until numRecords)) {
        assertEquals(offset.toLong, recordMetadata.offset)
        assertEquals(topic, recordMetadata.topic)
        assertEquals(partition, recordMetadata.partition)
      }

      consumer.assign(List(new TopicPartition(topic, partition)).asJava)

      // make sure the fetched messages also respect the partitioning and ordering
      val records = pollUntilNumRecords(numRecords)

      records.zipWithIndex.foreach { case (record, i) =>
        assertEquals(topic, record.topic)
        assertEquals(partition, record.partition)
        assertEquals(i.toLong, record.offset)
        assertNull(record.key)
        assertEquals(s"value${i + 1}", new String(record.value))
        assertEquals(now, record.timestamp)
      }

    } finally {
      producer.close()
    }
  }

  /**
    * Checks partitioning behavior before and after partitions are added
    *
    * Producer will attempt to send messages to the partition specified in each record, and should
    * succeed as long as the partition is included in the metadata.
    */
  @Test
  def testSendBeforeAndAfterPartitionExpansion() {
    val producer = createProducer(brokerList)

    // create topic
    createTopic(topic, 1, 2)
    val partition0 = 0

    var futures0 = (1 to numRecords).map { i =>
      producer.send(new ProducerRecord(topic, partition0, null, ("value" + i).getBytes(StandardCharsets.UTF_8)))
    }.map(_.get(30, TimeUnit.SECONDS))

    // make sure all of them end up in the same partition with increasing offset values
    for ((recordMetadata, offset) <- futures0 zip (0 until numRecords)) {
      assertEquals(offset.toLong, recordMetadata.offset)
      assertEquals(topic, recordMetadata.topic)
      assertEquals(partition0, recordMetadata.partition)
    }

    // Trying to send a record to a partition beyond topic's partition range before adding the partition should fail.
    val partition1 = 1
    try {
      producer.send(new ProducerRecord(topic, partition1, null, "value".getBytes(StandardCharsets.UTF_8)))
      fail("Should not allow sending a record to a partition not present in the metadata")
    } catch {
      case _: KafkaException => // this is ok
    }

    val existingAssignment = zkClient.getReplicaAssignmentForTopics(Set(topic)).map {
      case (topicPartition, replicas) => topicPartition.partition -> replicas
    }
    adminZkClient.addPartitions(topic, existingAssignment, adminZkClient.getBrokerMetadatas(), 2)
    // read metadata from a broker and verify the new topic partitions exist
    TestUtils.waitUntilMetadataIsPropagated(servers, topic, 0)
    TestUtils.waitUntilMetadataIsPropagated(servers, topic, 1)

    // send records to the newly added partition after confirming that metadata have been updated.
    val futures1 = (1 to numRecords).map { i =>
      producer.send(new ProducerRecord(topic, partition1, null, ("value" + i).getBytes(StandardCharsets.UTF_8)))
    }.map(_.get(30, TimeUnit.SECONDS))

    // make sure all of them end up in the same partition with increasing offset values
    for ((recordMetadata, offset) <- futures1 zip (0 until numRecords)) {
      assertEquals(offset.toLong, recordMetadata.offset)
      assertEquals(topic, recordMetadata.topic)
      assertEquals(partition1, recordMetadata.partition)
    }

    futures0 = (1 to numRecords).map { i =>
      producer.send(new ProducerRecord(topic, partition0, null, ("value" + i).getBytes(StandardCharsets.UTF_8)))
    }.map(_.get(30, TimeUnit.SECONDS))

    // make sure all of them end up in the same partition with increasing offset values starting where previous
    for ((recordMetadata, offset) <- futures0 zip (numRecords until 2 * numRecords)) {
      assertEquals(offset.toLong, recordMetadata.offset)
      assertEquals(topic, recordMetadata.topic)
      assertEquals(partition0, recordMetadata.partition)
    }
  }

  /**
   * Test that flush immediately sends all accumulated requests.
   */
  @Test
  def testFlush() {
    val producer = createProducer(brokerList, lingerMs = Long.MaxValue)
    try {
      createTopic(topic, 2, 2)
      val record = new ProducerRecord[Array[Byte], Array[Byte]](topic,
        "value".getBytes(StandardCharsets.UTF_8))
      for (_ <- 0 until 50) {
        val responses = (0 until numRecords) map (_ => producer.send(record))
        assertTrue("No request is complete.", responses.forall(!_.isDone()))
        producer.flush()
        assertTrue("All requests are complete.", responses.forall(_.isDone()))
      }
    } finally {
      producer.close()
    }
  }

  /**
   * Test close with zero timeout from caller thread
   */
  @Test
  def testCloseWithZeroTimeoutFromCallerThread() {
    createTopic(topic, 2, 2)
    val partition = 0
    consumer.assign(List(new TopicPartition(topic, partition)).asJava)
    val record0 = new ProducerRecord[Array[Byte], Array[Byte]](topic, partition, null,
      "value".getBytes(StandardCharsets.UTF_8))

    // Test closing from caller thread.
    for (_ <- 0 until 50) {
      val producer = createProducer(brokerList, lingerMs = Long.MaxValue)
      val responses = (0 until numRecords) map (_ => producer.send(record0))
      assertTrue("No request is complete.", responses.forall(!_.isDone()))
      producer.close(0, TimeUnit.MILLISECONDS)
      responses.foreach { future =>
        try {
          future.get()
          fail("No message should be sent successfully.")
        } catch {
          case e: Exception =>
            assertEquals("java.lang.IllegalStateException: Producer is closed forcefully.", e.getMessage)
        }
      }
      assertEquals("Fetch response should have no message returned.", 0, consumer.poll(50).count)
    }
  }

  /**
   * Test close with zero and non-zero timeout from sender thread
   */
  @Test
  def testCloseWithZeroTimeoutFromSenderThread() {
    createTopic(topic, 1, 2)
    val partition = 0
    consumer.assign(List(new TopicPartition(topic, partition)).asJava)
    val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, partition, null, "value".getBytes(StandardCharsets.UTF_8))

    // Test closing from sender thread.
    class CloseCallback(producer: KafkaProducer[Array[Byte], Array[Byte]], sendRecords: Boolean) extends Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception) {
        // Trigger another batch in accumulator before close the producer. These messages should
        // not be sent.
        if (sendRecords)
          (0 until numRecords) foreach (_ => producer.send(record))
        // The close call will be called by all the message callbacks. This tests idempotence of the close call.
        producer.close(0, TimeUnit.MILLISECONDS)
        // Test close with non zero timeout. Should not block at all.
        producer.close(Long.MaxValue, TimeUnit.MICROSECONDS)
      }
    }
    for (i <- 0 until 50) {
      val producer = createProducer(brokerList, lingerMs = Long.MaxValue)
      try {
        // send message to partition 0
        // Only send the records in the first callback since we close the producer in the callback and no records
        // can be sent afterwards.
        val responses = (0 until numRecords) map (i => producer.send(record, new CloseCallback(producer, i == 0)))
        assertTrue("No request is complete.", responses.forall(!_.isDone()))
        // flush the messages.
        producer.flush()
        assertTrue("All requests are complete.", responses.forall(_.isDone()))
        // Check the messages received by broker.
        pollUntilNumRecords(numRecords)
      } finally {
        producer.close()
      }
    }
  }

}
