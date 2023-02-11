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

import integration.kafka.api.ProducerSendTestHelpers
import kafka.server.KafkaConfig
import kafka.utils.{TestInfoUtils, TestUtils}
import org.apache.kafka.clients.admin.NewPartitions
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.Collections
import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionException
import scala.jdk.CollectionConverters._

abstract class BaseProducerSendTest extends ProducerSendTestHelpers {

  def generateConfigs: scala.collection.Seq[KafkaConfig] = {
    baseProps.map(KafkaConfig.fromProps)
  }

  /**
   * testSendOffset checks the basic send API behavior
   *
   * 1. Send with null key/value/partition-id should be accepted; send with null topic should be rejected.
   * 2. Last message of the non-blocking send should return the correct offset metadata
   */
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testSendOffset(quorum: String): Unit = {
    val producer = createProducer()
    val partition = 0

    object callback extends Callback {
      var offset = 0L

      def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
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
          offset += 1
        } else {
          fail(s"Send callback returns the following exception: $exception")
        }
      }
    }

    try {
      // create topic
      TestUtils.createTopicWithAdmin(admin, topic, brokers, 1, 2)

      // send a normal record
      val record0 = new ProducerRecord[Array[Byte], Array[Byte]](topic, partition, "key".getBytes(StandardCharsets.UTF_8),
        "value".getBytes(StandardCharsets.UTF_8))
      assertEquals(0L, producer.send(record0, callback).get.offset, "Should have offset 0")

      // send a record with null value should be ok
      val record1 = new ProducerRecord[Array[Byte], Array[Byte]](topic, partition, "key".getBytes(StandardCharsets.UTF_8), null)
      assertEquals(1L, producer.send(record1, callback).get.offset, "Should have offset 1")

      // send a record with null key should be ok
      val record2 = new ProducerRecord[Array[Byte], Array[Byte]](topic, partition, null, "value".getBytes(StandardCharsets.UTF_8))
      assertEquals(2L, producer.send(record2, callback).get.offset, "Should have offset 2")

      // send a record with null part id should be ok
      val record3 = new ProducerRecord[Array[Byte], Array[Byte]](topic, null, "key".getBytes(StandardCharsets.UTF_8),
        "value".getBytes(StandardCharsets.UTF_8))
      assertEquals(3L, producer.send(record3, callback).get.offset, "Should have offset 3")

      // non-blocking send a list of records
      for (_ <- 1 to numRecords)
        producer.send(record0, callback)

      // check that all messages have been acked via offset
      assertEquals(numRecords + 4L, producer.send(record0, callback).get.offset, "Should have offset " + (numRecords + 4))

    } finally {
      producer.close()
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testSendCompressedMessageWithCreateTime(quorum: String): Unit = {
    val producer = createProducer(
      compressionType = "gzip",
      lingerMs = Int.MaxValue,
      deliveryTimeoutMs = Int.MaxValue)
    sendAndVerifyTimestamp(producer, TimestampType.CREATE_TIME)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testSendNonCompressedMessageWithCreateTime(quorum: String): Unit = {
    val producer = createProducer(lingerMs = Int.MaxValue, deliveryTimeoutMs = Int.MaxValue)
    sendAndVerifyTimestamp(producer, TimestampType.CREATE_TIME)
  }

  /**
   * testClose checks the closing behavior
   *
   * After close() returns, all messages should be sent with correct returned offset metadata
   */
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testClose(quorum: String): Unit = {
    val producer = createProducer()

    try {
      // create topic
      TestUtils.createTopicWithAdmin(admin, topic, brokers, 1, 2)

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
      assertTrue(response0.isDone, "The last message should be acked before producer is shutdown")
      assertEquals(numRecords.toLong, response0.get.offset, "Should have offset " + numRecords)

    } finally {
      producer.close()
    }
  }

  /**
   * testSendToPartition checks the partitioning behavior
   *
   * The specified partition-id should be respected
   */
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testSendToPartition(quorum: String): Unit = {
    sendToPartition(quorum)
  }

  /**
    * Checks partitioning behavior before and after partitions are added
    *
    * Producer will attempt to send messages to the partition specified in each record, and should
    * succeed as long as the partition is included in the metadata.
    */
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testSendBeforeAndAfterPartitionExpansion(quorum: String): Unit = {
    val producer = createProducer(maxBlockMs = 5 * 1000L)

    // create topic
    TestUtils.createTopicWithAdmin(admin, topic, brokers, 1, 2)

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
    val e = assertThrows(classOf[ExecutionException], () => producer.send(new ProducerRecord(topic, partition1, null, "value".getBytes(StandardCharsets.UTF_8))).get())
    assertEquals(classOf[TimeoutException], e.getCause.getClass)

    admin.createPartitions(Collections.singletonMap(topic, NewPartitions.increaseTo(2))).all().get()

    // read metadata from a broker and verify the new topic partitions exist
    TestUtils.waitForPartitionMetadata(brokers, topic, 0)
    TestUtils.waitForPartitionMetadata(brokers, topic, 1)

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
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testFlush(quorum: String): Unit = {
    val producer = createProducer(lingerMs = Int.MaxValue, deliveryTimeoutMs = Int.MaxValue)
    try {
      TestUtils.createTopicWithAdmin(admin, topic, brokers, 2, 2)
      val record = new ProducerRecord[Array[Byte], Array[Byte]](topic,
        "value".getBytes(StandardCharsets.UTF_8))
      for (_ <- 0 until 50) {
        val responses = (0 until numRecords) map (_ => producer.send(record))
        assertTrue(responses.forall(!_.isDone), "No request is complete.")
        producer.flush()
        assertTrue(responses.forall(_.isDone), "All requests are complete.")
      }
    } finally {
      producer.close()
    }
  }

  /**
   * Test close with zero timeout from caller thread
   */
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testCloseWithZeroTimeoutFromCallerThread(quorum: String): Unit = {
    TestUtils.createTopicWithAdmin(admin, topic, brokers, 2, 2)
    val partition = 0
    consumer.assign(List(new TopicPartition(topic, partition)).asJava)
    val record0 = new ProducerRecord[Array[Byte], Array[Byte]](topic, partition, null,
      "value".getBytes(StandardCharsets.UTF_8))

    // Test closing from caller thread.
    for (_ <- 0 until 50) {
      val producer = createProducer(lingerMs = Int.MaxValue, deliveryTimeoutMs = Int.MaxValue)
      val responses = (0 until numRecords) map (_ => producer.send(record0))
      assertTrue(responses.forall(!_.isDone), "No request is complete.")
      producer.close(Duration.ZERO)
      responses.foreach { future =>
        val e = assertThrows(classOf[ExecutionException], () => future.get())
        assertEquals(classOf[KafkaException], e.getCause.getClass)
      }
      assertEquals(0, consumer.poll(Duration.ofMillis(50L)).count, "Fetch response should have no message returned.")
    }
  }

  /**
   * Test close with zero and non-zero timeout from sender thread
   */
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testCloseWithZeroTimeoutFromSenderThread(quorum: String): Unit = {
    TestUtils.createTopicWithAdmin(admin, topic, brokers, 1, 2)
    val partition = 0
    consumer.assign(List(new TopicPartition(topic, partition)).asJava)
    val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, partition, null, "value".getBytes(StandardCharsets.UTF_8))

    // Test closing from sender thread.
    class CloseCallback(producer: KafkaProducer[Array[Byte], Array[Byte]], sendRecords: Boolean) extends Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        // Trigger another batch in accumulator before close the producer. These messages should
        // not be sent.
        if (sendRecords)
          (0 until numRecords) foreach (_ => producer.send(record))
        // The close call will be called by all the message callbacks. This tests idempotence of the close call.
        producer.close(Duration.ZERO)
        // Test close with non zero timeout. Should not block at all.
        producer.close()
      }
    }
    for (i <- 0 until 50) {
      val producer = createProducer(lingerMs = Int.MaxValue, deliveryTimeoutMs = Int.MaxValue)
      try {
        // send message to partition 0
        // Only send the records in the first callback since we close the producer in the callback and no records
        // can be sent afterwards.
        val responses = (0 until numRecords) map (i => producer.send(record, new CloseCallback(producer, i == 0)))
        assertTrue(responses.forall(!_.isDone), "No request is complete.")
        // flush the messages.
        producer.flush()
        assertTrue(responses.forall(_.isDone), "All requests are complete.")
        // Check the messages received by broker.
        TestUtils.pollUntilAtLeastNumRecords(consumer, numRecords)
      } finally {
        producer.close()
      }
    }
  }

}
