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

package kafka.server

import java.nio.ByteBuffer
import java.util.{Collections, Properties}
import kafka.log.LogConfig
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.ProduceRequestData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.{ProduceRequest, ProduceResponse}
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.apache.kafka.server.record.BrokerCompressionType
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import scala.jdk.CollectionConverters._

/**
  * Subclasses of `BaseProduceSendRequestTest` exercise the producer and produce request/response. This class
  * complements those classes with tests that require lower-level access to the protocol.
  */
class ProduceRequestTest extends BaseRequestTest {

  val metricsKeySet = KafkaYammerMetrics.defaultRegistry.allMetrics.keySet.asScala

  @Test
  def testSimpleProduceRequest(): Unit = {
    val (partition, leader) = createTopicAndFindPartitionWithLeader("topic")

    def sendAndCheck(memoryRecords: MemoryRecords, expectedOffset: Long): Unit = {
      val topicPartition = new TopicPartition("topic", partition)
      val produceResponse = sendProduceRequest(leader,
          ProduceRequest.forCurrentMagic(new ProduceRequestData()
            .setTopicData(new ProduceRequestData.TopicProduceDataCollection(Collections.singletonList(
              new ProduceRequestData.TopicProduceData()
                .setName(topicPartition.topic())
                .setPartitionData(Collections.singletonList(new ProduceRequestData.PartitionProduceData()
                  .setIndex(topicPartition.partition())
                  .setRecords(memoryRecords)))).iterator))
            .setAcks((-1).toShort)
            .setTimeoutMs(3000)
            .setTransactionalId(null)).build())
      assertEquals(1, produceResponse.data.responses.size)
      val topicProduceResponse = produceResponse.data.responses.asScala.head
      assertEquals(1, topicProduceResponse.partitionResponses.size)   
      val partitionProduceResponse = topicProduceResponse.partitionResponses.asScala.head
      val tp = new TopicPartition(topicProduceResponse.name, partitionProduceResponse.index)
      assertEquals(topicPartition, tp)
      assertEquals(Errors.NONE, Errors.forCode(partitionProduceResponse.errorCode))
      assertEquals(expectedOffset, partitionProduceResponse.baseOffset)
      assertEquals(-1, partitionProduceResponse.logAppendTimeMs)
      assertTrue(partitionProduceResponse.recordErrors.isEmpty)
    }

    sendAndCheck(MemoryRecords.withRecords(CompressionType.NONE,
      new SimpleRecord(System.currentTimeMillis(), "key".getBytes, "value".getBytes)), 0)

    sendAndCheck(MemoryRecords.withRecords(CompressionType.GZIP,
      new SimpleRecord(System.currentTimeMillis(), "key1".getBytes, "value1".getBytes),
      new SimpleRecord(System.currentTimeMillis(), "key2".getBytes, "value2".getBytes)), 1)
  }

  @Test
  def testProduceWithInvalidTimestamp(): Unit = {
    val topic = "topic"
    val partition = 0
    val topicConfig = new Properties
    topicConfig.setProperty(LogConfig.MessageTimestampDifferenceMaxMsProp, "1000")
    val partitionToLeader = TestUtils.createTopic(zkClient, topic, 1, 1, servers, topicConfig)
    val leader = partitionToLeader(partition)

    def createRecords(magicValue: Byte, timestamp: Long, codec: CompressionType): MemoryRecords = {
      val buf = ByteBuffer.allocate(512)
      val builder = MemoryRecords.builder(buf, magicValue, codec, TimestampType.CREATE_TIME, 0L)
      builder.appendWithOffset(0, timestamp, null, "hello".getBytes)
      builder.appendWithOffset(1, timestamp, null, "there".getBytes)
      builder.appendWithOffset(2, timestamp, null, "beautiful".getBytes)
      builder.build()
    }

    val records = createRecords(RecordBatch.MAGIC_VALUE_V2, System.currentTimeMillis() - 1001L, CompressionType.GZIP)
    val topicPartition = new TopicPartition("topic", partition)
    val produceResponse = sendProduceRequest(leader, ProduceRequest.forCurrentMagic(new ProduceRequestData()
      .setTopicData(new ProduceRequestData.TopicProduceDataCollection(Collections.singletonList(
        new ProduceRequestData.TopicProduceData()
          .setName(topicPartition.topic())
          .setPartitionData(Collections.singletonList(new ProduceRequestData.PartitionProduceData()
            .setIndex(topicPartition.partition())
            .setRecords(records)))).iterator))
      .setAcks((-1).toShort)
      .setTimeoutMs(3000)
      .setTransactionalId(null)).build())

    assertEquals(1, produceResponse.data.responses.size)
    val topicProduceResponse = produceResponse.data.responses.asScala.head
    assertEquals(1, topicProduceResponse.partitionResponses.size)   
    val partitionProduceResponse = topicProduceResponse.partitionResponses.asScala.head
    val tp = new TopicPartition(topicProduceResponse.name, partitionProduceResponse.index)
    assertEquals(topicPartition, tp)
    assertEquals(Errors.INVALID_TIMESTAMP, Errors.forCode(partitionProduceResponse.errorCode))
    // there are 3 records with InvalidTimestampException created from inner function createRecords
    assertEquals(3, partitionProduceResponse.recordErrors.size)
    val recordErrors = partitionProduceResponse.recordErrors.asScala
    recordErrors.indices.foreach(i => assertEquals(i, recordErrors(i).batchIndex))
    recordErrors.foreach(recordError => assertNotNull(recordError.batchIndexErrorMessage))
    assertEquals("One or more records have been rejected due to invalid timestamp", partitionProduceResponse.errorMessage)
  }

  @Test
  def testProduceToNonReplica(): Unit = {
    val topic = "topic"
    val partition = 0

    // Create a single-partition topic and find a broker which is not the leader
    val partitionToLeader = TestUtils.createTopic(zkClient, topic, numPartitions = 1, 1, servers)
    val leader = partitionToLeader(partition)
    val nonReplicaOpt = servers.find(_.config.brokerId != leader)
    assertTrue(nonReplicaOpt.isDefined)
    val nonReplicaId =  nonReplicaOpt.get.config.brokerId

    // Send the produce request to the non-replica
    val records = MemoryRecords.withRecords(CompressionType.NONE, new SimpleRecord("key".getBytes, "value".getBytes))
    val topicPartition = new TopicPartition("topic", partition)
    val produceRequest = ProduceRequest.forCurrentMagic(new ProduceRequestData()
      .setTopicData(new ProduceRequestData.TopicProduceDataCollection(Collections.singletonList(
        new ProduceRequestData.TopicProduceData()
          .setName(topicPartition.topic())
          .setPartitionData(Collections.singletonList(new ProduceRequestData.PartitionProduceData()
            .setIndex(topicPartition.partition())
            .setRecords(records)))).iterator))
      .setAcks((-1).toShort)
      .setTimeoutMs(3000)
      .setTransactionalId(null)).build()

    val produceResponse = sendProduceRequest(nonReplicaId, produceRequest)
    assertEquals(1, produceResponse.data.responses.size)
    val topicProduceResponse = produceResponse.data.responses.asScala.head
    assertEquals(1, topicProduceResponse.partitionResponses.size)   
    val partitionProduceResponse = topicProduceResponse.partitionResponses.asScala.head
    assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, Errors.forCode(partitionProduceResponse.errorCode))
  }

  /* returns a pair of partition id and leader id */
  private def createTopicAndFindPartitionWithLeader(topic: String): (Int, Int) = {
    val partitionToLeader = TestUtils.createTopic(zkClient, topic, 3, 2, servers)
    partitionToLeader.collectFirst {
      case (partition, leader) if leader != -1 => (partition, leader)
    }.getOrElse(throw new AssertionError(s"No leader elected for topic $topic"))
  }

  @Test
  def testCorruptLz4ProduceRequest(): Unit = {
    val (partition, leader) = createTopicAndFindPartitionWithLeader("topic")
    val timestamp = 1000000
    val memoryRecords = MemoryRecords.withRecords(CompressionType.LZ4,
      new SimpleRecord(timestamp, "key".getBytes, "value".getBytes))
    // Change the lz4 checksum value (not the kafka record crc) so that it doesn't match the contents
    val lz4ChecksumOffset = 6
    memoryRecords.buffer.array.update(DefaultRecordBatch.RECORD_BATCH_OVERHEAD + lz4ChecksumOffset, 0)
    val topicPartition = new TopicPartition("topic", partition)
    val produceResponse = sendProduceRequest(leader, ProduceRequest.forCurrentMagic(new ProduceRequestData()
      .setTopicData(new ProduceRequestData.TopicProduceDataCollection(Collections.singletonList(
        new ProduceRequestData.TopicProduceData()
          .setName(topicPartition.topic())
          .setPartitionData(Collections.singletonList(new ProduceRequestData.PartitionProduceData()
            .setIndex(topicPartition.partition())
            .setRecords(memoryRecords)))).iterator))
      .setAcks((-1).toShort)
      .setTimeoutMs(3000)
      .setTransactionalId(null)).build())

    assertEquals(1, produceResponse.data.responses.size)
    val topicProduceResponse = produceResponse.data.responses.asScala.head
    assertEquals(1, topicProduceResponse.partitionResponses.size)   
    val partitionProduceResponse = topicProduceResponse.partitionResponses.asScala.head
    val tp = new TopicPartition(topicProduceResponse.name, partitionProduceResponse.index)
    assertEquals(topicPartition, tp)
    assertEquals(Errors.CORRUPT_MESSAGE, Errors.forCode(partitionProduceResponse.errorCode))
    assertEquals(-1, partitionProduceResponse.baseOffset)
    assertEquals(-1, partitionProduceResponse.logAppendTimeMs)
    assertEquals(metricsKeySet.count(_.getMBeanName.endsWith(s"${BrokerTopicStats.InvalidMessageCrcRecordsPerSec}")), 1)
    assertTrue(TestUtils.meterCount(s"${BrokerTopicStats.InvalidMessageCrcRecordsPerSec}") > 0)
  }

  @Test
  def testZSTDProduceRequest(): Unit = {
    val topic = "topic"
    val partition = 0

    // Create a single-partition topic compressed with ZSTD
    val topicConfig = new Properties
    topicConfig.setProperty(LogConfig.CompressionTypeProp, BrokerCompressionType.ZSTD.name)
    val partitionToLeader = TestUtils.createTopic(zkClient, topic, 1, 1, servers, topicConfig)
    val leader = partitionToLeader(partition)
    val memoryRecords = MemoryRecords.withRecords(CompressionType.ZSTD,
      new SimpleRecord(System.currentTimeMillis(), "key".getBytes, "value".getBytes))
    val topicPartition = new TopicPartition("topic", partition)
    val partitionRecords = new ProduceRequestData()
      .setTopicData(new ProduceRequestData.TopicProduceDataCollection(Collections.singletonList(
        new ProduceRequestData.TopicProduceData()
          .setName("topic").setPartitionData(Collections.singletonList(
            new ProduceRequestData.PartitionProduceData()
              .setIndex(partition)
              .setRecords(memoryRecords))))
        .iterator))
      .setAcks((-1).toShort)
      .setTimeoutMs(3000)
      .setTransactionalId(null)

    // produce request with v7: works fine!
    val produceResponse1 = sendProduceRequest(leader, new ProduceRequest.Builder(7, 7, partitionRecords).build())

    val topicProduceResponse1 = produceResponse1.data.responses.asScala.head
    val partitionProduceResponse1 = topicProduceResponse1.partitionResponses.asScala.head
    val tp1 = new TopicPartition(topicProduceResponse1.name, partitionProduceResponse1.index)
    assertEquals(topicPartition, tp1)
    assertEquals(Errors.NONE, Errors.forCode(partitionProduceResponse1.errorCode))
    assertEquals(0, partitionProduceResponse1.baseOffset)
    assertEquals(-1, partitionProduceResponse1.logAppendTimeMs)

    // produce request with v3: returns Errors.UNSUPPORTED_COMPRESSION_TYPE.
    val produceResponse2 = sendProduceRequest(leader, new ProduceRequest.Builder(3, 3, partitionRecords).buildUnsafe(3))
    val topicProduceResponse2 = produceResponse2.data.responses.asScala.head
    val partitionProduceResponse2 = topicProduceResponse2.partitionResponses.asScala.head
    val tp2 = new TopicPartition(topicProduceResponse2.name, partitionProduceResponse2.index)
    assertEquals(topicPartition, tp2)
    assertEquals(Errors.UNSUPPORTED_COMPRESSION_TYPE, Errors.forCode(partitionProduceResponse2.errorCode))
  }

  private def sendProduceRequest(leaderId: Int, request: ProduceRequest): ProduceResponse = {
    connectAndReceive[ProduceResponse](request, destination = brokerSocketServer(leaderId))
  }

}
