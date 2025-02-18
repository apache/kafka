/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.api

import kafka.utils.TestInfoUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.record.{AbstractRecords, CompressionType, MemoryRecords, RecordBatch, RecordVersion, SimpleRecord, TimestampType}
import org.junit.jupiter.api.Assertions.{assertEquals, assertNull, assertThrows}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

import java.nio.ByteBuffer
import java.util
import java.util.{Collections, Optional}
import scala.jdk.CollectionConverters._

class ConsumerWithLegacyMessageFormatIntegrationTest extends AbstractConsumerTest {

  val topic1 = "part-test-topic-1"
  val topic2 = "part-test-topic-2"
  val topic3 = "part-test-topic-3"

  val t1p0 = new TopicPartition(topic1, 0)
  val t1p1 = new TopicPartition(topic1, 1)
  val t2p0 = new TopicPartition(topic2, 0)
  val t2p1 = new TopicPartition(topic2, 1)
  val t3p0 = new TopicPartition(topic3, 0)
  val t3p1 = new TopicPartition(topic3, 1)

  private def appendLegacyRecords(numRecords: Int, tp: TopicPartition, brokerId: Int, magicValue: Byte): Unit = {
    val records = (0 until numRecords).map { i =>
      new SimpleRecord(i, s"key $i".getBytes, s"value $i".getBytes)
    }
    val buffer = ByteBuffer.allocate(AbstractRecords.estimateSizeInBytes(magicValue, CompressionType.NONE, records.asJava))
    val builder = MemoryRecords.builder(buffer, magicValue, Compression.of(CompressionType.NONE).build,
      TimestampType.CREATE_TIME, 0L, RecordBatch.NO_TIMESTAMP, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH,
      0, false, RecordBatch.NO_PARTITION_LEADER_EPOCH)

    records.foreach(builder.append)

    brokers.filter(_.config.brokerId == brokerId).foreach(b => {
      val unifiedLog = b.replicaManager.logManager.getLog(tp).get
      unifiedLog.appendAsLeaderWithRecordVersion(
        records = builder.build(),
        leaderEpoch = 0,
        recordVersion = RecordVersion.lookup(magicValue)
      )
      // Default isolation.level is read_uncommitted. It makes Partition#fetchOffsetForTimestamp to return UnifiedLog#highWatermark,
      // so increasing high watermark to make it return the correct offset.
      unifiedLog.maybeIncrementHighWatermark(unifiedLog.logEndOffsetMetadata)
    })
  }

  private def setupTopics(): Unit = {
    val producer = createProducer()
    createTopic(topic1, numPartitions = 2)
    createTopicWithAssignment(topic2, Map(0 -> List(0), 1 -> List(1)))
    createTopicWithAssignment(topic3, Map(0 -> List(0), 1 -> List(1)))

    // v2 message format for topic1
    sendRecords(producer, numRecords = 100, t1p0, startingTimestamp = 0)
    sendRecords(producer, numRecords = 100, t1p1, startingTimestamp = 0)
    // v0 message format for topic2
    appendLegacyRecords(100, t2p0, 0, RecordBatch.MAGIC_VALUE_V0)
    appendLegacyRecords(100, t2p1, 1, RecordBatch.MAGIC_VALUE_V0)
    // v1 message format for topic3
    appendLegacyRecords(100, t3p0, 0, RecordBatch.MAGIC_VALUE_V1)
    appendLegacyRecords(100, t3p1, 1, RecordBatch.MAGIC_VALUE_V1)

    producer.close()
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testOffsetsForTimes(quorum: String, groupProtocol: String): Unit = {
    setupTopics()
    val consumer = createConsumer()

    // Test negative target time
    assertThrows(classOf[IllegalArgumentException],
      () => consumer.offsetsForTimes(Collections.singletonMap(t1p0, -1)))

    val timestampsToSearch = util.Map.of[TopicPartition, java.lang.Long](
      t1p0, 0L,
      t1p1, 20L,
      t2p0, 40L,
      t2p1, 60L,
      t3p0, 80L,
      t3p1, 100L
    )

    val timestampOffsets = consumer.offsetsForTimes(timestampsToSearch)

    val timestampTopic1P0 = timestampOffsets.get(t1p0)
    assertEquals(0, timestampTopic1P0.offset)
    assertEquals(0, timestampTopic1P0.timestamp)
    assertEquals(Optional.of(0), timestampTopic1P0.leaderEpoch)

    val timestampTopic1P1 = timestampOffsets.get(t1p1)
    assertEquals(20, timestampTopic1P1.offset)
    assertEquals(20, timestampTopic1P1.timestamp)
    assertEquals(Optional.of(0), timestampTopic1P1.leaderEpoch)

    // v0 message format doesn't have timestamp
    val timestampTopic2P0 = timestampOffsets.get(t2p0)
    assertNull(timestampTopic2P0)

    val timestampTopic2P1 = timestampOffsets.get(t2p1)
    assertNull(timestampTopic2P1)

    // v1 message format doesn't have leader epoch
    val timestampTopic3P0 = timestampOffsets.get(t3p0)
    assertEquals(80, timestampTopic3P0.offset)
    assertEquals(80, timestampTopic3P0.timestamp)
    assertEquals(Optional.empty, timestampTopic3P0.leaderEpoch)

    assertNull(timestampOffsets.get(t3p1))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testEarliestOrLatestOffsets(quorum: String, groupProtocol: String): Unit = {
    setupTopics()

    val partitions = Set(t1p0, t1p1, t2p0, t2p1, t3p0, t3p1).asJava
    val consumer = createConsumer()

    val earliests = consumer.beginningOffsets(partitions)
    assertEquals(0L, earliests.get(t1p0))
    assertEquals(0L, earliests.get(t1p1))
    assertEquals(0L, earliests.get(t2p0))
    assertEquals(0L, earliests.get(t2p1))
    assertEquals(0L, earliests.get(t3p0))
    assertEquals(0L, earliests.get(t3p1))

    val latests = consumer.endOffsets(partitions)
    assertEquals(100L, latests.get(t1p0))
    assertEquals(100L, latests.get(t1p1))
    assertEquals(100L, latests.get(t2p0))
    assertEquals(100L, latests.get(t2p1))
    assertEquals(100L, latests.get(t3p0))
    assertEquals(100L, latests.get(t3p1))
  }
}
