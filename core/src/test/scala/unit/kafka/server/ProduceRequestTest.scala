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

import java.util.Properties

import kafka.log.LogConfig
import kafka.message.ZStdCompressionCodec
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.{InitProducerIdRequest, InitProducerIdResponse, ProduceRequest, ProduceResponse}
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._

/**
  * Subclasses of `BaseProduceSendRequestTest` exercise the producer and produce request/response. This class
  * complements those classes with tests that require lower-level access to the protocol.
  */
class ProduceRequestTest extends BaseRequestTest {

  val sr = new SimpleRecord(System.currentTimeMillis(), "key".getBytes, "value".getBytes)

  @Test
  def testSimpleProduceRequest() {
    val (partition, leader) = createTopicAndFindPartitionWithLeader("topic")

    sendAndCheck(partition, leader, MemoryRecords.withRecords(CompressionType.NONE, sr),
      expectedBaseOffset = 0,
      expectedLEO = 1)

    sendAndCheck(partition, leader, MemoryRecords.withRecords(CompressionType.GZIP, sr, sr),
      expectedBaseOffset = 1,
      expectedLEO = 3)

    // sending a batch with non-0 base offset
    sendAndCheck(partition, leader, MemoryRecords.withRecords(2000, CompressionType.NONE, sr),
      expectedBaseOffset = -1, expectedLSO = -1, expectedLEO = -1,
      expectedError = Errors.CORRUPT_MESSAGE)
  }

  @Test
  def testProduceRequestWithOffsetGoldenPath() {
    val (partition, leader) = createTopicAndFindPartitionWithLeader("topic")

    sendAndCheck(partition, leader, MemoryRecords.withRecords(2000, CompressionType.NONE, sr), useOffsets = true,
      expectedBaseOffset = 2000,
      expectedLEO = 2001)

    val simpleRecords = (1 until 50).toArray.map(_ => sr)
    val offsets = (2101L until 2150L).toArray
    val memoryRecordsWithOffsets = TestUtils.recordsWithOffset(simpleRecords, offsets, baseOffset = 2101L)
    sendAndCheck(partition, leader, memoryRecordsWithOffsets, useOffsets = true,
      expectedBaseOffset = 2101, expectedLEO = 2150)

    sendAndCheck(partition, leader, MemoryRecords.withRecords(CompressionType.NONE, sr),
      expectedBaseOffset = 2150, expectedLEO = 2151)
  }

  @Test
  def testProduceRequestWithOffsetErrorPath() {
    val (partition, leader) = createTopicAndFindPartitionWithLeader("topic")

    sendAndCheck(partition, leader, MemoryRecords.withRecords(2000, CompressionType.NONE, sr, sr, sr), useOffsets = true,
      expectedBaseOffset = 2000, expectedLEO = 2003)

    sendAndCheck(partition, leader, MemoryRecords.withRecords(2000, CompressionType.NONE, sr), useOffsets = true,
      expectedBaseOffset = -1, expectedLSO = -1, expectedLEO = 2003, expectedError = Errors.INVALID_PRODUCE_OFFSET)

    val simpleRecords = (3 until 10 by 2).toArray.map(_ => sr)
    val offsets = (2003L until 2010L by 2).toArray
    val memoryRecordsWithOffsets = TestUtils.recordsWithOffset(simpleRecords, offsets, baseOffset = 2003L)
    sendAndCheck(partition, leader,
      memoryRecordsWithOffsets,
      useOffsets = true,
      expectedBaseOffset = -1L,
      expectedLSO = -1L,
      expectedLEO = -1L,
      expectedError = Errors.CORRUPT_MESSAGE)
  }

  @Test
  def testProduceToNonReplica() {
    val topic = "topic"
    val partition = 0

    // Create a single-partition topic and find a broker which is not the leader
    val partitionToLeader = TestUtils.createTopic(zkClient, topic, numPartitions = 1, 1, servers)
    val leader = partitionToLeader(partition)
    val nonReplicaOpt = servers.find(_.config.brokerId != leader)
    assertTrue(nonReplicaOpt.isDefined)
    val nonReplicaId =  nonReplicaOpt.get.config.brokerId

    // Send the produce request to the non-replica
    val records = MemoryRecords.withRecords(CompressionType.NONE, sr)
    val topicPartition = new TopicPartition("topic", partition)
    val partitionRecords = Map(topicPartition -> records)
    val produceRequest = ProduceRequest.Builder.forCurrentMagic(-1, 3000, partitionRecords.asJava).build()

    val produceResponse = sendProduceRequest(nonReplicaId, produceRequest)
    assertEquals(1, produceResponse.responses.size)
    assertEquals(Errors.NOT_LEADER_FOR_PARTITION, produceResponse.responses.asScala.head._2.error)
  }

  /* returns a pair of partition id and leader id */
  private def createTopicAndFindPartitionWithLeader(topic: String): (Int, Int) = {
    val partitionToLeader = TestUtils.createTopic(zkClient, topic, 3, 2, servers)
    partitionToLeader.collectFirst {
      case (partition, leader) if leader != -1 => (partition, leader)
    }.getOrElse(fail(s"No leader elected for topic $topic"))
  }

  @Test
  def testCorruptLz4ProduceRequest() {
    val (partition, leader) = createTopicAndFindPartitionWithLeader("topic")
    val timestamp = 1000000
    val memoryRecords = MemoryRecords.withRecords(CompressionType.LZ4, sr)
    // Change the lz4 checksum value (not the kafka record crc) so that it doesn't match the contents
    val lz4ChecksumOffset = 6
    memoryRecords.buffer.array.update(DefaultRecordBatch.RECORD_BATCH_OVERHEAD + lz4ChecksumOffset, 0)
    val topicPartition = new TopicPartition("topic", partition)
    val partitionRecords = Map(topicPartition -> memoryRecords)
    val produceResponse = sendProduceRequest(leader, 
      ProduceRequest.Builder.forCurrentMagic(-1, 3000, partitionRecords.asJava).build())
    assertEquals(1, produceResponse.responses.size)
    val (tp, partitionResponse) = produceResponse.responses.asScala.head
    assertEquals(topicPartition, tp)
    assertEquals(Errors.CORRUPT_MESSAGE, partitionResponse.error)
    assertEquals(-1, partitionResponse.baseOffset)
    assertEquals(-1, partitionResponse.logAppendTime)
    assertEquals(-1, partitionResponse.logStartOffset)
  }

  @Test
  def testZSTDProduceRequest(): Unit = {
    val topic = "topic"
    val partition = 0

    // Create a single-partition topic compressed with ZSTD
    val topicConfig = new Properties
    topicConfig.setProperty(LogConfig.CompressionTypeProp, ZStdCompressionCodec.name)
    val partitionToLeader = TestUtils.createTopic(zkClient, topic, 1, 1, servers, topicConfig)
    val leader = partitionToLeader(partition)
    val memoryRecords = MemoryRecords.withRecords(CompressionType.ZSTD, sr)
    val topicPartition = new TopicPartition("topic", partition)
    val partitionRecords = Map(topicPartition -> memoryRecords)

    // produce request with v7: works fine!
    val res1 = sendProduceRequest(leader,
      new ProduceRequest.Builder(7, 7, -1, 3000, partitionRecords.asJava, null, false).build())
    val (tp, partitionResponse) = res1.responses.asScala.head
    assertEquals(topicPartition, tp)
    assertEquals(Errors.NONE, partitionResponse.error)
    assertEquals(0, partitionResponse.baseOffset)
    assertEquals(-1, partitionResponse.logAppendTime)
    assertEquals(0, partitionResponse.logStartOffset)
}

  private def sendProduceRequest(leaderId: Int, request: ProduceRequest): ProduceResponse = {
    val response = connectAndSend(request, ApiKeys.PRODUCE, destination = brokerSocketServer(leaderId))
    ProduceResponse.parse(response, request.version)
  }

  private def sendAndCheck(partition: Int, 
                          leader: Int, 
                          memoryRecords: MemoryRecords,
                          useOffsets: Boolean = false,
                          expectedBaseOffset: Long,
                          expectedLSO : Long = 0, //logs have not rolled in this test
                          expectedLEO: Long,
                          expectedError: Errors = Errors.NONE) {
    val topicPartition = new TopicPartition("topic", partition)
    val partitionRecords = Map(topicPartition -> memoryRecords)

    val produceResponse = sendProduceRequest(leader, ProduceRequest.Builder.forMagic(
                                                      RecordBatch.CURRENT_MAGIC_VALUE,
                                                      -1, 3000,
                                                      partitionRecords.asJava,
                                                      null, useOffsets).build())

    assertEquals(1, produceResponse.responses.size)
    val (tp, partitionResponse) = produceResponse.responses.asScala.head
    assertEquals(topicPartition, tp)
    assertEquals(expectedError, partitionResponse.error)

    assertEquals(expectedBaseOffset, partitionResponse.baseOffset)
    assertEquals(expectedLSO, partitionResponse.logStartOffset)
    assertEquals(expectedLEO, partitionResponse.logEndOffset)
    assertEquals(-1, partitionResponse.logAppendTime) // we're using create time
  }
}
