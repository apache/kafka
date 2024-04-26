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

import java.util.Properties

import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.TopicPartition
import kafka.utils.TestUtils
import kafka.server.{BaseRequestTest, KafkaConfig}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, TestInfo}

import scala.jdk.CollectionConverters._
import scala.collection.mutable.ArrayBuffer
import org.apache.kafka.clients.producer.KafkaProducer

/**
 * Extension point for share consumer integration tests.
 */
abstract class AbstractShareConsumerTest extends BaseRequestTest {

  val epsilon = 0.1
  override def brokerCount: Int = 3

  val topic = "topic"
  val part = 0
  val tp = new TopicPartition(topic, part)
  val group = "share"
  val producerClientId = "ConsumerTestProducer"
  val consumerClientId = "ConsumerTestShareConsumer"
  val groupMaxSessionTimeoutMs = 60000L

  this.producerConfig.setProperty(ProducerConfig.CLIENT_ID_CONFIG, producerClientId)
  this.consumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, consumerClientId)
  this.consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group)

  override protected def brokerPropertyOverrides(properties: Properties): Unit = {
    properties.setProperty(KafkaConfig.ControlledShutdownEnableProp, "false") // speed up shutdown
    properties.setProperty(KafkaConfig.OffsetsTopicReplicationFactorProp, "3") // don't want to lose offset
    properties.setProperty(KafkaConfig.OffsetsTopicPartitionsProp, "1")
    properties.setProperty(KafkaConfig.GroupMinSessionTimeoutMsProp, "100") // set small enough session timeout
    properties.setProperty(KafkaConfig.GroupMaxSessionTimeoutMsProp, groupMaxSessionTimeoutMs.toString)
    properties.setProperty(KafkaConfig.GroupInitialRebalanceDelayMsProp, "10")
    properties.setProperty(KafkaConfig.ShareGroupRecordLockDurationMsProp, "10000")
    properties.setProperty(KafkaConfig.ShareGroupPartitionMaxRecordLocksProp, "10000")
  }

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)

    // create the test topic with all the brokers as replicas
    createTopic(topic, 1, brokerCount, adminClientConfig = this.adminClientConfig)
  }

  protected def sendRecords(producer: KafkaProducer[Array[Byte], Array[Byte]], numRecords: Int,
                            tp: TopicPartition,
                            startingTimestamp: Long = System.currentTimeMillis()): Seq[ProducerRecord[Array[Byte], Array[Byte]]] = {
    val records = (0 until numRecords).map { i =>
      val timestamp = startingTimestamp + i.toLong
      val record = new ProducerRecord(tp.topic(), tp.partition(), timestamp, s"key $i".getBytes, s"value $i".getBytes)
      producer.send(record)
      record
    }
    producer.flush()

    records
  }

  protected def consumeAndVerifyRecords(consumer: ShareConsumer[Array[Byte], Array[Byte]],
                                        numRecords: Int,
                                        startingOffset: Int,
                                        startingKeyAndValueIndex: Int = 0,
                                        startingTimestamp: Long = 0L,
                                        timestampType: TimestampType = TimestampType.CREATE_TIME,
                                        tp: TopicPartition = tp,
                                        maxPollRecords: Int = Int.MaxValue): Unit = {
    val records = consumeRecords(consumer, numRecords, maxPollRecords = maxPollRecords)
    val now = System.currentTimeMillis()
    for (i <- 0 until numRecords) {
      val record = records(i)
      val offset = startingOffset + i
      assertEquals(tp.topic, record.topic)
      assertEquals(tp.partition, record.partition)
      if (timestampType == TimestampType.CREATE_TIME) {
        assertEquals(timestampType, record.timestampType)
        val timestamp = startingTimestamp + i
        assertEquals(timestamp, record.timestamp)
      } else
        assertTrue(record.timestamp >= startingTimestamp && record.timestamp <= now,
          s"Got unexpected timestamp ${record.timestamp}. Timestamp should be between [$startingTimestamp, $now}]")
      assertEquals(offset.toLong, record.offset)
      val keyAndValueIndex = startingKeyAndValueIndex + i
      assertEquals(s"key $keyAndValueIndex", new String(record.key))
      assertEquals(s"value $keyAndValueIndex", new String(record.value))
      // this is true only because K and V are byte arrays
      assertEquals(s"key $keyAndValueIndex".length, record.serializedKeySize)
      assertEquals(s"value $keyAndValueIndex".length, record.serializedValueSize)
    }
  }

  protected def consumeRecords[K, V](consumer: ShareConsumer[K, V],
                                     numRecords: Int,
                                     maxPollRecords: Int = Int.MaxValue): ArrayBuffer[ConsumerRecord[K, V]] = {
    val records = new ArrayBuffer[ConsumerRecord[K, V]]
    def pollAction(polledRecords: ConsumerRecords[K, V]): Boolean = {
      assertTrue(polledRecords.asScala.size <= maxPollRecords)
      records ++= polledRecords.asScala
      records.size >= numRecords
    }
    TestUtils.pollShareRecordsUntilTrue(consumer, pollAction, waitTimeMs = 60000,
      msg = s"Timed out before consuming expected $numRecords records. " +
        s"The number consumed was ${records.size}.")
    records
  }

  /**
   * Creates topic 'topicName' with 'numPartitions' partitions and produces 'recordsPerPartition'
   * records to each partition
   */
  protected def createTopicAndSendRecords(producer: KafkaProducer[Array[Byte], Array[Byte]],
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

}
