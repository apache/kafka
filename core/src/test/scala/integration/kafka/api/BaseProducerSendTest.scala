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
import java.util.concurrent.TimeUnit

import kafka.admin.AdminUtils
import kafka.consumer.SimpleConsumer
import kafka.integration.KafkaServerTestHarness
import kafka.log.LogConfig
import kafka.message.Message
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import kafka.utils.TestUtils._
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.record.TimestampType
import org.junit.Assert._
import org.junit.{After, Before, Test}

import scala.collection.mutable.Buffer

abstract class BaseProducerSendTest extends KafkaServerTestHarness {

  def generateConfigs = {
    val overridingProps = new Properties()
    val numServers = 2
    overridingProps.put(KafkaConfig.NumPartitionsProp, 4.toString)
    TestUtils.createBrokerConfigs(numServers, zkConnect, false, interBrokerSecurityProtocol = Some(securityProtocol),
      trustStoreFile = trustStoreFile, saslProperties = saslProperties).map(KafkaConfig.fromProps(_, overridingProps))
  }

  private var consumer1: SimpleConsumer = null
  private var consumer2: SimpleConsumer = null
  private val producers = Buffer[KafkaProducer[Array[Byte], Array[Byte]]]()

  protected val topic = "topic"
  private val numRecords = 100

  @Before
  override def setUp() {
    super.setUp()

    // TODO: we need to migrate to new consumers when 0.9 is final
    consumer1 = new SimpleConsumer("localhost", servers.head.boundPort(), 100, 1024 * 1024, "")
    consumer2 = new SimpleConsumer("localhost", servers(1).boundPort(), 100, 1024 * 1024, "")
  }

  @After
  override def tearDown() {
    consumer1.close()
    consumer2.close()
    // Ensure that all producers are closed since unclosed producers impact other tests when Kafka server ports are reused
    producers.foreach(_.close())

    super.tearDown()
  }

  protected def createProducer(brokerList: String, retries: Int = 0, lingerMs: Long = 0, props: Option[Properties] = None): KafkaProducer[Array[Byte],Array[Byte]] = {
    val producer = TestUtils.createNewProducer(brokerList, securityProtocol = securityProtocol, trustStoreFile = trustStoreFile,
      saslProperties = saslProperties, retries = retries, lingerMs = lingerMs, props = props)
    registerProducer(producer)
  }

  protected def registerProducer(producer: KafkaProducer[Array[Byte], Array[Byte]]): KafkaProducer[Array[Byte], Array[Byte]] = {
    producers += producer
    producer
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
    val partition = new Integer(0)

    object callback extends Callback {
      var offset = 0L

      def onCompletion(metadata: RecordMetadata, exception: Exception) {
        if (exception == null) {
          assertEquals(offset, metadata.offset())
          assertEquals(topic, metadata.topic())
          assertEquals(partition, metadata.partition())
          offset match {
            case 0 => assertEquals(metadata.serializedKeySize + metadata.serializedValueSize, "key".getBytes.length + "value".getBytes.length)
            case 1 => assertEquals(metadata.serializedKeySize(), "key".getBytes.length)
            case 2 => assertEquals(metadata.serializedValueSize, "value".getBytes.length)
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
      TestUtils.createTopic(zkUtils, topic, 1, 2, servers)

      // send a normal record
      val record0 = new ProducerRecord[Array[Byte], Array[Byte]](topic, partition, "key".getBytes, "value".getBytes)
      assertEquals("Should have offset 0", 0L, producer.send(record0, callback).get.offset)

      // send a record with null value should be ok
      val record1 = new ProducerRecord[Array[Byte], Array[Byte]](topic, partition, "key".getBytes, null)
      assertEquals("Should have offset 1", 1L, producer.send(record1, callback).get.offset)

      // send a record with null key should be ok
      val record2 = new ProducerRecord[Array[Byte], Array[Byte]](topic, partition, null, "value".getBytes)
      assertEquals("Should have offset 2", 2L, producer.send(record2, callback).get.offset)

      // send a record with null part id should be ok
      val record3 = new ProducerRecord[Array[Byte], Array[Byte]](topic, null, "key".getBytes, "value".getBytes)
      assertEquals("Should have offset 3", 3L, producer.send(record3, callback).get.offset)

      // send a record with null topic should fail
      try {
        val record4 = new ProducerRecord[Array[Byte], Array[Byte]](null, partition, "key".getBytes, "value".getBytes)
        producer.send(record4, callback)
        fail("Should not allow sending a record without topic")
      } catch {
        case iae: IllegalArgumentException => // this is ok
        case e: Throwable => fail("Only expecting IllegalArgumentException", e)
      }

      // non-blocking send a list of records
      for (i <- 1 to numRecords)
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

  protected def sendAndVerifyTimestamp(producer: KafkaProducer[Array[Byte], Array[Byte]], timestampType: TimestampType) {
    val partition = new Integer(0)

    val baseTimestamp = 123456L
    val startTime = System.currentTimeMillis()

    object callback extends Callback {
      var offset = 0L
      var timestampDiff = 1L

      def onCompletion(metadata: RecordMetadata, exception: Exception) {
        if (exception == null) {
          assertEquals(offset, metadata.offset())
          assertEquals(topic, metadata.topic())
          if (timestampType == TimestampType.CREATE_TIME)
            assertEquals(baseTimestamp + timestampDiff, metadata.timestamp())
          else
            assertTrue(metadata.timestamp() >= startTime && metadata.timestamp() <= System.currentTimeMillis())
          assertEquals(partition, metadata.partition())
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
      TestUtils.createTopic(zkUtils, topic, 1, 2, servers, topicProps)

      for (i <- 1 to numRecords) {
        val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, partition, baseTimestamp + i, "key".getBytes, "value".getBytes)
        producer.send(record, callback)
      }
      producer.close(20000L, TimeUnit.MILLISECONDS)
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
      TestUtils.createTopic(zkUtils, topic, 1, 2, servers)

      // non-blocking send a list of records
      val record0 = new ProducerRecord[Array[Byte], Array[Byte]](topic, null, "key".getBytes, "value".getBytes)
      for (i <- 1 to numRecords)
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
      // create topic
      val leaders = TestUtils.createTopic(zkUtils, topic, 2, 2, servers)
      val partition = 1

      val now = System.currentTimeMillis()
      val futures = (1 to numRecords).map { i =>
        producer.send(new ProducerRecord(topic, partition, now, null, ("value" + i).getBytes))
      }.map(_.get(30, TimeUnit.SECONDS))

      // make sure all of them end up in the same partition with increasing offset values
      for ((recordMetadata, offset) <- futures zip (0 until numRecords)) {
        assertEquals(offset.toLong, recordMetadata.offset)
        assertEquals(topic, recordMetadata.topic)
        assertEquals(partition, recordMetadata.partition)
      }

      val leader1 = leaders(partition)
      // make sure the fetched messages also respect the partitioning and ordering
      val fetchResponse1 = if (leader1.get == configs.head.brokerId) {
        consumer1.fetch(new FetchRequestBuilder().addFetch(topic, partition, 0, Int.MaxValue).build())
      } else {
        consumer2.fetch(new FetchRequestBuilder().addFetch(topic, partition, 0, Int.MaxValue).build())
      }
      val messageSet1 = fetchResponse1.messageSet(topic, partition).iterator.toBuffer
      assertEquals("Should have fetched " + numRecords + " messages", numRecords, messageSet1.size)

      // TODO: also check topic and partition after they are added in the return messageSet
      for (i <- 0 until numRecords) {
        assertEquals(new Message(bytes = ("value" + (i + 1)).getBytes, now, Message.MagicValue_V1), messageSet1(i).message)
        assertEquals(i.toLong, messageSet1(i).offset)
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
    TestUtils.createTopic(zkUtils, topic, 1, 2, servers)
    val partition0 = 0

    var futures0 = (1 to numRecords).map { i =>
      producer.send(new ProducerRecord(topic, partition0, null, ("value" + i).getBytes))
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
      producer.send(new ProducerRecord(topic, partition1, null, "value".getBytes))
      fail("Should not allow sending a record to a partition not present in the metadata")
    } catch {
      case ke: KafkaException => // this is ok
      case e: Throwable => fail("Only expecting KafkaException", e)
    }

    AdminUtils.addPartitions(zkUtils, topic, 2)
    // read metadata from a broker and verify the new topic partitions exist
    TestUtils.waitUntilMetadataIsPropagated(servers, topic, 0)
    TestUtils.waitUntilMetadataIsPropagated(servers, topic, 1)

    // send records to the newly added partition after confirming that metadata have been updated.
    val futures1 = (1 to numRecords).map { i =>
      producer.send(new ProducerRecord(topic, partition1, null, ("value" + i).getBytes))
    }.map(_.get(30, TimeUnit.SECONDS))

    // make sure all of them end up in the same partition with increasing offset values
    for ((recordMetadata, offset) <- futures1 zip (0 until numRecords)) {
      assertEquals(offset.toLong, recordMetadata.offset)
      assertEquals(topic, recordMetadata.topic)
      assertEquals(partition1, recordMetadata.partition)
    }

    futures0 = (1 to numRecords).map { i =>
      producer.send(new ProducerRecord(topic, partition0, null, ("value" + i).getBytes))
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
      TestUtils.createTopic(zkUtils, topic, 2, 2, servers)
      val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, "value".getBytes)
      for (i <- 0 until 50) {
        val responses = (0 until numRecords) map (i => producer.send(record))
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
    // create topic
    val leaders = TestUtils.createTopic(zkUtils, topic, 2, 2, servers)
    val leader0 = leaders(0)
    val leader1 = leaders(1)

    // create record
    val record0 = new ProducerRecord[Array[Byte], Array[Byte]](topic, 0, null, "value".getBytes)

    // Test closing from caller thread.
    for (i <- 0 until 50) {
      val producer = createProducer(brokerList, lingerMs = Long.MaxValue)
      val responses = (0 until numRecords) map (i => producer.send(record0))
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
      val fetchResponse = if (leader0.get == configs.head.brokerId) {
        consumer1.fetch(new FetchRequestBuilder().addFetch(topic, 0, 0, Int.MaxValue).build())
      } else {
        consumer2.fetch(new FetchRequestBuilder().addFetch(topic, 0, 0, Int.MaxValue).build())
      }
      assertEquals("Fetch response should have no message returned.", 0, fetchResponse.messageSet(topic, 0).size)
    }
  }

  /**
   * Test close with zero and non-zero timeout from sender thread
   */
  @Test
  def testCloseWithZeroTimeoutFromSenderThread() {
    // create topic
    val leaders = TestUtils.createTopic(zkUtils, topic, 1, 2, servers)
    val leader = leaders(0)

    // create record
    val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, 0, null, "value".getBytes)

    // Test closing from sender thread.
    class CloseCallback(producer: KafkaProducer[Array[Byte], Array[Byte]], sendRecords: Boolean) extends Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception) {
        // Trigger another batch in accumulator before close the producer. These messages should
        // not be sent.
        if (sendRecords)
          (0 until numRecords) foreach (i => producer.send(record))
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
        assertTrue("All request are complete.", responses.forall(_.isDone()))
        // Check the messages received by broker.
        val fetchResponse = if (leader.get == configs.head.brokerId) {
          consumer1.fetch(new FetchRequestBuilder().addFetch(topic, 0, 0, Int.MaxValue).build())
        } else {
          consumer2.fetch(new FetchRequestBuilder().addFetch(topic, 0, 0, Int.MaxValue).build())
        }
        val expectedNumRecords = (i + 1) * numRecords
        assertEquals("Fetch response to partition 0 should have %d messages.".format(expectedNumRecords),
          expectedNumRecords, fetchResponse.messageSet(topic, 0).size)
      } finally {
        producer.close()
      }
    }
  }

}
