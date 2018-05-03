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

package kafka.coordinator.group

import kafka.api.ApiVersion
import kafka.cluster.Partition
import kafka.common.OffsetAndMetadata
import kafka.log.{Log, LogAppendInfo}
import kafka.server.{FetchDataInfo, KafkaConfig, LogOffsetMetadata, ReplicaManager}
import kafka.utils.TestUtils.fail
import kafka.utils.{KafkaScheduler, MockTime, TestUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.{IsolationLevel, OffsetFetchResponse}
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.easymock.{Capture, EasyMock, IAnswer}
import org.junit.Assert.{assertEquals, assertFalse, assertTrue, assertNull}
import org.junit.{Before, Test}
import java.nio.ByteBuffer

import com.yammer.metrics.Metrics
import com.yammer.metrics.core.Gauge
import org.apache.kafka.common.internals.Topic

import scala.collection.JavaConverters._
import scala.collection._
import java.util.concurrent.locks.ReentrantLock

import kafka.zk.KafkaZkClient

class GroupMetadataManagerTest {

  var time: MockTime = null
  var replicaManager: ReplicaManager = null
  var groupMetadataManager: GroupMetadataManager = null
  var scheduler: KafkaScheduler = null
  var zkClient: KafkaZkClient = null
  var partition: Partition = null

  val groupId = "foo"
  val groupPartitionId = 0
  val groupTopicPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupPartitionId)
  val protocolType = "protocolType"
  val rebalanceTimeout = 60000
  val sessionTimeout = 10000

  @Before
  def setUp(): Unit = {
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(nodeId = 0, zkConnect = ""))

    val offsetConfig = OffsetConfig(maxMetadataSize = config.offsetMetadataMaxSize,
      loadBufferSize = config.offsetsLoadBufferSize,
      offsetsRetentionMs = config.offsetsRetentionMinutes * 60 * 1000L,
      offsetsRetentionCheckIntervalMs = config.offsetsRetentionCheckIntervalMs,
      offsetsTopicNumPartitions = config.offsetsTopicPartitions,
      offsetsTopicSegmentBytes = config.offsetsTopicSegmentBytes,
      offsetsTopicReplicationFactor = config.offsetsTopicReplicationFactor,
      offsetsTopicCompressionCodec = config.offsetsTopicCompressionCodec,
      offsetCommitTimeoutMs = config.offsetCommitTimeoutMs,
      offsetCommitRequiredAcks = config.offsetCommitRequiredAcks)

    // make two partitions of the group topic to make sure some partitions are not owned by the coordinator
    zkClient = EasyMock.createNiceMock(classOf[KafkaZkClient])
    EasyMock.expect(zkClient.getTopicPartitionCount(Topic.GROUP_METADATA_TOPIC_NAME)).andReturn(Some(2))
    EasyMock.replay(zkClient)

    time = new MockTime
    replicaManager = EasyMock.createNiceMock(classOf[ReplicaManager])
    groupMetadataManager = new GroupMetadataManager(0, ApiVersion.latestVersion, offsetConfig, replicaManager, zkClient, time)
    partition = EasyMock.niceMock(classOf[Partition])
  }

  @Test
  def testLoadOffsetsWithoutGroup(): Unit = {
    val groupMetadataTopicPartition = groupTopicPartition
    val startOffset = 15L

    val committedOffsets = Map(
      new TopicPartition("foo", 0) -> 23L,
      new TopicPartition("foo", 1) -> 455L,
      new TopicPartition("bar", 0) -> 8992L
    )

    val offsetCommitRecords = createCommittedOffsetRecords(committedOffsets)
    val records = MemoryRecords.withRecords(startOffset, CompressionType.NONE, offsetCommitRecords: _*)
    expectGroupMetadataLoad(groupMetadataTopicPartition, startOffset, records)

    EasyMock.replay(replicaManager)

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, _ => ())

    val group = groupMetadataManager.getGroup(groupId).getOrElse(fail("Group was not loaded into the cache"))
    assertEquals(groupId, group.groupId)
    assertEquals(Empty, group.currentState)
    assertEquals(committedOffsets.size, group.allOffsets.size)
    committedOffsets.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.offset))
    }
  }

  @Test
  def testLoadEmptyGroupWithOffsets(): Unit = {
    val groupMetadataTopicPartition = groupTopicPartition
    val generation = 15
    val protocolType = "consumer"
    val startOffset = 15L
    val committedOffsets = Map(
      new TopicPartition("foo", 0) -> 23L,
      new TopicPartition("foo", 1) -> 455L,
      new TopicPartition("bar", 0) -> 8992L
    )

    val offsetCommitRecords = createCommittedOffsetRecords(committedOffsets)
    val groupMetadataRecord = buildEmptyGroupRecord(generation, protocolType)
    val records = MemoryRecords.withRecords(startOffset, CompressionType.NONE,
      offsetCommitRecords ++ Seq(groupMetadataRecord): _*)

    expectGroupMetadataLoad(groupMetadataTopicPartition, startOffset, records)

    EasyMock.replay(replicaManager)

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, _ => ())

    val group = groupMetadataManager.getGroup(groupId).getOrElse(fail("Group was not loaded into the cache"))
    assertEquals(groupId, group.groupId)
    assertEquals(Empty, group.currentState)
    assertEquals(generation, group.generationId)
    assertEquals(Some(protocolType), group.protocolType)
    assertNull(group.leaderOrNull)
    assertNull(group.protocolOrNull)
    committedOffsets.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.offset))
    }
  }

  @Test
  def testLoadTransactionalOffsetsWithoutGroup(): Unit = {
    val groupMetadataTopicPartition = groupTopicPartition
    val producerId = 1000L
    val producerEpoch: Short = 2

    val committedOffsets = Map(
      new TopicPartition("foo", 0) -> 23L,
      new TopicPartition("foo", 1) -> 455L,
      new TopicPartition("bar", 0) -> 8992L
    )

    val buffer = ByteBuffer.allocate(1024)
    var nextOffset = 0
    nextOffset += appendTransactionalOffsetCommits(buffer, producerId, producerEpoch, nextOffset, committedOffsets)
    nextOffset += completeTransactionalOffsetCommit(buffer, producerId, producerEpoch, nextOffset, isCommit = true)
    buffer.flip()

    val records = MemoryRecords.readableRecords(buffer)
    expectGroupMetadataLoad(groupMetadataTopicPartition, 0, records)

    EasyMock.replay(replicaManager)

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, _ => ())

    val group = groupMetadataManager.getGroup(groupId).getOrElse(fail("Group was not loaded into the cache"))
    assertEquals(groupId, group.groupId)
    assertEquals(Empty, group.currentState)
    assertEquals(committedOffsets.size, group.allOffsets.size)
    committedOffsets.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.offset))
    }
  }

  @Test
  def testDoNotLoadAbortedTransactionalOffsetCommits(): Unit = {
    val groupMetadataTopicPartition = groupTopicPartition
    val producerId = 1000L
    val producerEpoch: Short = 2

    val abortedOffsets = Map(
      new TopicPartition("foo", 0) -> 23L,
      new TopicPartition("foo", 1) -> 455L,
      new TopicPartition("bar", 0) -> 8992L
    )

    val buffer = ByteBuffer.allocate(1024)
    var nextOffset = 0
    nextOffset += appendTransactionalOffsetCommits(buffer, producerId, producerEpoch, nextOffset, abortedOffsets)
    nextOffset += completeTransactionalOffsetCommit(buffer, producerId, producerEpoch, nextOffset, isCommit = false)
    buffer.flip()

    val records = MemoryRecords.readableRecords(buffer)
    expectGroupMetadataLoad(groupMetadataTopicPartition, 0, records)

    EasyMock.replay(replicaManager)

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, _ => ())

    // Since there are no committed offsets for the group, and there is no other group metadata, we don't expect the
    // group to be loaded.
    assertEquals(None, groupMetadataManager.getGroup(groupId))
  }

  @Test
  def testGroupLoadedWithPendingCommits(): Unit = {
    val groupMetadataTopicPartition = groupTopicPartition
    val producerId = 1000L
    val producerEpoch: Short = 2

    val pendingOffsets = Map(
      new TopicPartition("foo", 0) -> 23L,
      new TopicPartition("foo", 1) -> 455L,
      new TopicPartition("bar", 0) -> 8992L
    )

    val buffer = ByteBuffer.allocate(1024)
    var nextOffset = 0
    nextOffset += appendTransactionalOffsetCommits(buffer, producerId, producerEpoch, nextOffset, pendingOffsets)
    buffer.flip()

    val records = MemoryRecords.readableRecords(buffer)
    expectGroupMetadataLoad(groupMetadataTopicPartition, 0, records)

    EasyMock.replay(replicaManager)

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, _ => ())

    // The group should be loaded with pending offsets.
    val group = groupMetadataManager.getGroup(groupId).getOrElse(fail("Group was not loaded into the cache"))
    assertEquals(groupId, group.groupId)
    assertEquals(Empty, group.currentState)
    // Ensure that no offsets are materialized, but that we have offsets pending.
    assertEquals(0, group.allOffsets.size)
    assertTrue(group.hasOffsets)
    assertTrue(group.hasPendingOffsetCommitsFromProducer(producerId))
  }

  @Test
  def testLoadWithCommittedAndAbortedTransactionalOffsetCommits(): Unit = {
    // A test which loads a log with a mix of committed and aborted transactional offset committed messages.
    val groupMetadataTopicPartition = groupTopicPartition
    val producerId = 1000L
    val producerEpoch: Short = 2

    val committedOffsets = Map(
      new TopicPartition("foo", 0) -> 23L,
      new TopicPartition("foo", 1) -> 455L,
      new TopicPartition("bar", 0) -> 8992L
    )

    val abortedOffsets = Map(
      new TopicPartition("foo", 2) -> 231L,
      new TopicPartition("foo", 3) -> 4551L,
      new TopicPartition("bar", 1) -> 89921L
    )

    val buffer = ByteBuffer.allocate(1024)
    var nextOffset = 0
    nextOffset += appendTransactionalOffsetCommits(buffer, producerId, producerEpoch, nextOffset, abortedOffsets)
    nextOffset += completeTransactionalOffsetCommit(buffer, producerId, producerEpoch, nextOffset, isCommit = false)
    nextOffset += appendTransactionalOffsetCommits(buffer, producerId, producerEpoch, nextOffset, committedOffsets)
    nextOffset += completeTransactionalOffsetCommit(buffer, producerId, producerEpoch, nextOffset, isCommit = true)
    buffer.flip()

    val records = MemoryRecords.readableRecords(buffer)
    expectGroupMetadataLoad(groupMetadataTopicPartition, 0, records)

    EasyMock.replay(replicaManager)

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, _ => ())

    val group = groupMetadataManager.getGroup(groupId).getOrElse(fail("Group was not loaded into the cache"))
    assertEquals(groupId, group.groupId)
    assertEquals(Empty, group.currentState)
    // Ensure that only the committed offsets are materialized, and that there are no pending commits for the producer.
    // This allows us to be certain that the aborted offset commits are truly discarded.
    assertEquals(committedOffsets.size, group.allOffsets.size)
    committedOffsets.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.offset))
    }
    assertFalse(group.hasPendingOffsetCommitsFromProducer(producerId))
  }

  @Test
  def testLoadWithCommittedAndAbortedAndPendingTransactionalOffsetCommits(): Unit = {
    val groupMetadataTopicPartition = groupTopicPartition
    val producerId = 1000L
    val producerEpoch: Short = 2

    val committedOffsets = Map(
      new TopicPartition("foo", 0) -> 23L,
      new TopicPartition("foo", 1) -> 455L,
      new TopicPartition("bar", 0) -> 8992L
    )

    val abortedOffsets = Map(
      new TopicPartition("foo", 2) -> 231L,
      new TopicPartition("foo", 3) -> 4551L,
      new TopicPartition("bar", 1) -> 89921L
    )

    val pendingOffsets = Map(
      new TopicPartition("foo", 3) -> 2312L,
      new TopicPartition("foo", 4) -> 45512L,
      new TopicPartition("bar", 2) -> 899212L
    )

    val buffer = ByteBuffer.allocate(1024)
    var nextOffset = 0
    val commitOffsetsLogPosition = nextOffset
    nextOffset += appendTransactionalOffsetCommits(buffer, producerId, producerEpoch, nextOffset, committedOffsets)
    nextOffset += completeTransactionalOffsetCommit(buffer, producerId, producerEpoch, nextOffset, isCommit = true)
    nextOffset += appendTransactionalOffsetCommits(buffer, producerId, producerEpoch, nextOffset, abortedOffsets)
    nextOffset += completeTransactionalOffsetCommit(buffer, producerId, producerEpoch, nextOffset, isCommit = false)
    nextOffset += appendTransactionalOffsetCommits(buffer, producerId, producerEpoch, nextOffset, pendingOffsets)
    buffer.flip()

    val records = MemoryRecords.readableRecords(buffer)
    expectGroupMetadataLoad(groupMetadataTopicPartition, 0, records)

    EasyMock.replay(replicaManager)

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, _ => ())

    val group = groupMetadataManager.getGroup(groupId).getOrElse(fail("Group was not loaded into the cache"))
    assertEquals(groupId, group.groupId)
    assertEquals(Empty, group.currentState)

    // Ensure that only the committed offsets are materialized, and that there are no pending commits for the producer.
    // This allows us to be certain that the aborted offset commits are truly discarded.
    assertEquals(committedOffsets.size, group.allOffsets.size)
    committedOffsets.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.offset))
      assertEquals(Some(commitOffsetsLogPosition), group.offsetWithRecordMetadata(topicPartition).head.appendedBatchOffset)
    }

    // We should have pending commits.
    assertTrue(group.hasPendingOffsetCommitsFromProducer(producerId))

    // The loaded pending commits should materialize after a commit marker comes in.
    groupMetadataManager.handleTxnCompletion(producerId, List(groupMetadataTopicPartition.partition).toSet, isCommit = true)
    assertFalse(group.hasPendingOffsetCommitsFromProducer(producerId))
    pendingOffsets.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.offset))
    }
  }

  @Test
  def testLoadTransactionalOffsetCommitsFromMultipleProducers(): Unit = {
    val groupMetadataTopicPartition = groupTopicPartition
    val firstProducerId = 1000L
    val firstProducerEpoch: Short = 2
    val secondProducerId = 1001L
    val secondProducerEpoch: Short = 3

    val committedOffsetsFirstProducer = Map(
      new TopicPartition("foo", 0) -> 23L,
      new TopicPartition("foo", 1) -> 455L,
      new TopicPartition("bar", 0) -> 8992L
    )

    val committedOffsetsSecondProducer = Map(
      new TopicPartition("foo", 2) -> 231L,
      new TopicPartition("foo", 3) -> 4551L,
      new TopicPartition("bar", 1) -> 89921L
    )

    val buffer = ByteBuffer.allocate(1024)
    var nextOffset = 0L

    val firstProduceRecordOffset = nextOffset
    nextOffset += appendTransactionalOffsetCommits(buffer, firstProducerId, firstProducerEpoch, nextOffset, committedOffsetsFirstProducer)
    nextOffset += completeTransactionalOffsetCommit(buffer, firstProducerId, firstProducerEpoch, nextOffset, isCommit = true)

    val secondProducerRecordOffset = nextOffset
    nextOffset += appendTransactionalOffsetCommits(buffer, secondProducerId, secondProducerEpoch, nextOffset, committedOffsetsSecondProducer)
    nextOffset += completeTransactionalOffsetCommit(buffer, secondProducerId, secondProducerEpoch, nextOffset, isCommit = true)
    buffer.flip()

    val records = MemoryRecords.readableRecords(buffer)
    expectGroupMetadataLoad(groupMetadataTopicPartition, 0, records)

    EasyMock.replay(replicaManager)

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, _ => ())

    val group = groupMetadataManager.getGroup(groupId).getOrElse(fail("Group was not loaded into the cache"))
    assertEquals(groupId, group.groupId)
    assertEquals(Empty, group.currentState)

    // Ensure that only the committed offsets are materialized, and that there are no pending commits for the producer.
    // This allows us to be certain that the aborted offset commits are truly discarded.
    assertEquals(committedOffsetsFirstProducer.size + committedOffsetsSecondProducer.size, group.allOffsets.size)
    committedOffsetsFirstProducer.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.offset))
      assertEquals(Some(firstProduceRecordOffset), group.offsetWithRecordMetadata(topicPartition).head.appendedBatchOffset)
    }
    committedOffsetsSecondProducer.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.offset))
      assertEquals(Some(secondProducerRecordOffset), group.offsetWithRecordMetadata(topicPartition).head.appendedBatchOffset)
    }
  }

  @Test
  def testGroupLoadWithConsumerAndTransactionalOffsetCommitsConsumerWins(): Unit = {
    val groupMetadataTopicPartition = groupTopicPartition
    val producerId = 1000L
    val producerEpoch: Short = 2

    val transactionalOffsetCommits = Map(
      new TopicPartition("foo", 0) -> 23L
    )

    val consumerOffsetCommits = Map(
      new TopicPartition("foo", 0) -> 24L
    )

    val buffer = ByteBuffer.allocate(1024)
    var nextOffset = 0
    nextOffset += appendTransactionalOffsetCommits(buffer, producerId, producerEpoch, nextOffset, transactionalOffsetCommits)
    val consumerRecordOffset = nextOffset
    nextOffset += appendConsumerOffsetCommit(buffer, nextOffset, consumerOffsetCommits)
    nextOffset += completeTransactionalOffsetCommit(buffer, producerId, producerEpoch, nextOffset, isCommit = true)
    buffer.flip()

    val records = MemoryRecords.readableRecords(buffer)
    expectGroupMetadataLoad(groupMetadataTopicPartition, 0, records)

    EasyMock.replay(replicaManager)

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, _ => ())

    // The group should be loaded with pending offsets.
    val group = groupMetadataManager.getGroup(groupId).getOrElse(fail("Group was not loaded into the cache"))
    assertEquals(groupId, group.groupId)
    assertEquals(Empty, group.currentState)
    assertEquals(1, group.allOffsets.size)
    assertTrue(group.hasOffsets)
    assertFalse(group.hasPendingOffsetCommitsFromProducer(producerId))
    assertEquals(consumerOffsetCommits.size, group.allOffsets.size)
    consumerOffsetCommits.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.offset))
      assertEquals(Some(consumerRecordOffset), group.offsetWithRecordMetadata(topicPartition).head.appendedBatchOffset)
    }
  }

  @Test
  def testGroupLoadWithConsumerAndTransactionalOffsetCommitsTransactionWins(): Unit = {
    val groupMetadataTopicPartition = groupTopicPartition
    val producerId = 1000L
    val producerEpoch: Short = 2

    val transactionalOffsetCommits = Map(
      new TopicPartition("foo", 0) -> 23L
    )

    val consumerOffsetCommits = Map(
      new TopicPartition("foo", 0) -> 24L
    )

    val buffer = ByteBuffer.allocate(1024)
    var nextOffset = 0
    nextOffset += appendConsumerOffsetCommit(buffer, nextOffset, consumerOffsetCommits)
    nextOffset += appendTransactionalOffsetCommits(buffer, producerId, producerEpoch, nextOffset, transactionalOffsetCommits)
    nextOffset += completeTransactionalOffsetCommit(buffer, producerId, producerEpoch, nextOffset, isCommit = true)
    buffer.flip()

    val records = MemoryRecords.readableRecords(buffer)
    expectGroupMetadataLoad(groupMetadataTopicPartition, 0, records)

    EasyMock.replay(replicaManager)

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, _ => ())

    // The group should be loaded with pending offsets.
    val group = groupMetadataManager.getGroup(groupId).getOrElse(fail("Group was not loaded into the cache"))
    assertEquals(groupId, group.groupId)
    assertEquals(Empty, group.currentState)
    assertEquals(1, group.allOffsets.size)
    assertTrue(group.hasOffsets)
    assertFalse(group.hasPendingOffsetCommitsFromProducer(producerId))
    assertEquals(consumerOffsetCommits.size, group.allOffsets.size)
    transactionalOffsetCommits.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.offset))
    }
  }

  @Test
  def testGroupNotExists(): Unit = {
    // group is not owned
    assertFalse(groupMetadataManager.groupNotExists(groupId))

    groupMetadataManager.addPartitionOwnership(groupPartitionId)
    // group is owned but does not exist yet
    assertTrue(groupMetadataManager.groupNotExists(groupId))

    val group = new GroupMetadata(groupId, initialState = Empty)
    groupMetadataManager.addGroup(group)

    // group is owned but not Dead
    assertFalse(groupMetadataManager.groupNotExists(groupId))

    group.transitionTo(Dead)
    // group is owned and Dead
    assertTrue(groupMetadataManager.groupNotExists(groupId))
  }

  private def appendConsumerOffsetCommit(buffer: ByteBuffer, baseOffset: Long, offsets: Map[TopicPartition, Long]) = {
    val builder = MemoryRecords.builder(buffer, CompressionType.NONE, TimestampType.LOG_APPEND_TIME, baseOffset)
    val commitRecords = createCommittedOffsetRecords(offsets)
    commitRecords.foreach(builder.append)
    builder.build()
    offsets.size
  }

  private def appendTransactionalOffsetCommits(buffer: ByteBuffer, producerId: Long, producerEpoch: Short,
                                               baseOffset: Long, offsets: Map[TopicPartition, Long]): Int = {
    val builder = MemoryRecords.builder(buffer, CompressionType.NONE, baseOffset, producerId, producerEpoch, 0, true)
    val commitRecords = createCommittedOffsetRecords(offsets)
    commitRecords.foreach(builder.append)
    builder.build()
    offsets.size
  }

  private def completeTransactionalOffsetCommit(buffer: ByteBuffer, producerId: Long, producerEpoch: Short, baseOffset: Long,
                                                isCommit: Boolean): Int = {
    val builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V2, CompressionType.NONE,
      TimestampType.LOG_APPEND_TIME, baseOffset, time.milliseconds(), producerId, producerEpoch, 0, true, true,
      RecordBatch.NO_PARTITION_LEADER_EPOCH)
    val controlRecordType = if (isCommit) ControlRecordType.COMMIT else ControlRecordType.ABORT
    builder.appendEndTxnMarker(time.milliseconds(), new EndTransactionMarker(controlRecordType, 0))
    builder.build()
    1
  }

  @Test
  def testLoadOffsetsWithTombstones(): Unit = {
    val groupMetadataTopicPartition = groupTopicPartition
    val startOffset = 15L

    val tombstonePartition = new TopicPartition("foo", 1)
    val committedOffsets = Map(
      new TopicPartition("foo", 0) -> 23L,
      tombstonePartition -> 455L,
      new TopicPartition("bar", 0) -> 8992L
    )

    val offsetCommitRecords = createCommittedOffsetRecords(committedOffsets)
    val tombstone = new SimpleRecord(GroupMetadataManager.offsetCommitKey(groupId, tombstonePartition), null)
    val records = MemoryRecords.withRecords(startOffset, CompressionType.NONE,
      offsetCommitRecords ++ Seq(tombstone): _*)

    expectGroupMetadataLoad(groupMetadataTopicPartition, startOffset, records)

    EasyMock.replay(replicaManager)

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, _ => ())

    val group = groupMetadataManager.getGroup(groupId).getOrElse(fail("Group was not loaded into the cache"))
    assertEquals(groupId, group.groupId)
    assertEquals(Empty, group.currentState)
    assertEquals(committedOffsets.size - 1, group.allOffsets.size)
    committedOffsets.foreach { case (topicPartition, offset) =>
      if (topicPartition == tombstonePartition)
        assertEquals(None, group.offset(topicPartition))
      else
        assertEquals(Some(offset), group.offset(topicPartition).map(_.offset))
    }
  }

  @Test
  def testLoadOffsetsAndGroup(): Unit = {
    val groupMetadataTopicPartition = groupTopicPartition
    val generation = 935
    val protocolType = "consumer"
    val protocol = "range"
    val startOffset = 15L
    val committedOffsets = Map(
      new TopicPartition("foo", 0) -> 23L,
      new TopicPartition("foo", 1) -> 455L,
      new TopicPartition("bar", 0) -> 8992L
    )

    val offsetCommitRecords = createCommittedOffsetRecords(committedOffsets)
    val memberId = "98098230493"
    val groupMetadataRecord = buildStableGroupRecordWithMember(generation, protocolType, protocol, memberId)
    val records = MemoryRecords.withRecords(startOffset, CompressionType.NONE,
      offsetCommitRecords ++ Seq(groupMetadataRecord): _*)

    expectGroupMetadataLoad(groupMetadataTopicPartition, startOffset, records)

    EasyMock.replay(replicaManager)

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, _ => ())

    val group = groupMetadataManager.getGroup(groupId).getOrElse(fail("Group was not loaded into the cache"))
    assertEquals(groupId, group.groupId)
    assertEquals(Stable, group.currentState)
    assertEquals(memberId, group.leaderOrNull)
    assertEquals(generation, group.generationId)
    assertEquals(Some(protocolType), group.protocolType)
    assertEquals(protocol, group.protocolOrNull)
    assertEquals(Set(memberId), group.allMembers)
    assertEquals(committedOffsets.size, group.allOffsets.size)
    committedOffsets.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.offset))
    }
  }

  @Test
  def testLoadGroupWithTombstone(): Unit = {
    val groupMetadataTopicPartition = groupTopicPartition
    val startOffset = 15L
    val memberId = "98098230493"
    val groupMetadataRecord = buildStableGroupRecordWithMember(generation = 15,
      protocolType = "consumer", protocol = "range", memberId)
    val groupMetadataTombstone = new SimpleRecord(GroupMetadataManager.groupMetadataKey(groupId), null)
    val records = MemoryRecords.withRecords(startOffset, CompressionType.NONE,
      Seq(groupMetadataRecord, groupMetadataTombstone): _*)

    expectGroupMetadataLoad(groupMetadataTopicPartition, startOffset, records)

    EasyMock.replay(replicaManager)

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, _ => ())

    assertEquals(None, groupMetadataManager.getGroup(groupId))
  }

  @Test
  def testOffsetWriteAfterGroupRemoved(): Unit = {
    // this test case checks the following scenario:
    // 1. the group exists at some point in time, but is later removed (because all members left)
    // 2. a "simple" consumer (i.e. not a consumer group) then uses the same groupId to commit some offsets

    val groupMetadataTopicPartition = groupTopicPartition
    val generation = 293
    val protocolType = "consumer"
    val protocol = "range"
    val startOffset = 15L

    val committedOffsets = Map(
      new TopicPartition("foo", 0) -> 23L,
      new TopicPartition("foo", 1) -> 455L,
      new TopicPartition("bar", 0) -> 8992L
    )
    val offsetCommitRecords = createCommittedOffsetRecords(committedOffsets)
    val memberId = "98098230493"
    val groupMetadataRecord = buildStableGroupRecordWithMember(generation, protocolType, protocol, memberId)
    val groupMetadataTombstone = new SimpleRecord(GroupMetadataManager.groupMetadataKey(groupId), null)
    val records = MemoryRecords.withRecords(startOffset, CompressionType.NONE,
      Seq(groupMetadataRecord, groupMetadataTombstone) ++ offsetCommitRecords: _*)

    expectGroupMetadataLoad(groupMetadataTopicPartition, startOffset, records)

    EasyMock.replay(replicaManager)

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, _ => ())

    val group = groupMetadataManager.getGroup(groupId).getOrElse(TestUtils.fail("Group was not loaded into the cache"))
    assertEquals(groupId, group.groupId)
    assertEquals(Empty, group.currentState)
    assertEquals(committedOffsets.size, group.allOffsets.size)
    committedOffsets.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.offset))
    }
  }

  @Test
  def testLoadGroupAndOffsetsFromDifferentSegments(): Unit = {
    val generation = 293
    val protocolType = "consumer"
    val protocol = "range"
    val startOffset = 15L
    val tp0 = new TopicPartition("foo", 0)
    val tp1 = new TopicPartition("foo", 1)
    val tp2 = new TopicPartition("bar", 0)
    val tp3 = new TopicPartition("xxx", 0)

    val logMock =  EasyMock.mock(classOf[Log])
    EasyMock.expect(replicaManager.getLog(groupTopicPartition)).andStubReturn(Some(logMock))

    val segment1MemberId = "a"
    val segment1Offsets = Map(tp0 -> 23L, tp1 -> 455L, tp3 -> 42L)
    val segment1Records = MemoryRecords.withRecords(startOffset, CompressionType.NONE,
      createCommittedOffsetRecords(segment1Offsets) ++ Seq(buildStableGroupRecordWithMember(
        generation, protocolType, protocol, segment1MemberId)): _*)
    val segment1End = expectGroupMetadataLoad(logMock, startOffset, segment1Records)

    val segment2MemberId = "b"
    val segment2Offsets = Map(tp0 -> 33L, tp2 -> 8992L, tp3 -> 10L)
    val segment2Records = MemoryRecords.withRecords(segment1End, CompressionType.NONE,
      createCommittedOffsetRecords(segment2Offsets) ++ Seq(buildStableGroupRecordWithMember(
        generation, protocolType, protocol, segment2MemberId)): _*)
    val segment2End = expectGroupMetadataLoad(logMock, segment1End, segment2Records)

    EasyMock.expect(replicaManager.getLogEndOffset(groupTopicPartition)).andStubReturn(Some(segment2End))

    EasyMock.replay(logMock, replicaManager)

    groupMetadataManager.loadGroupsAndOffsets(groupTopicPartition, _ => ())

    val group = groupMetadataManager.getGroup(groupId).getOrElse(fail("Group was not loaded into the cache"))
    assertEquals(groupId, group.groupId)
    assertEquals(Stable, group.currentState)

    assertEquals("segment2 group record member should be elected", segment2MemberId, group.leaderOrNull)
    assertEquals("segment2 group record member should be only member", Set(segment2MemberId), group.allMembers)

    // offsets of segment1 should be overridden by segment2 offsets of the same topic partitions
    val committedOffsets = segment1Offsets ++ segment2Offsets
    assertEquals(committedOffsets.size, group.allOffsets.size)
    committedOffsets.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.offset))
    }
  }

  @Test
  def testAddGroup(): Unit = {
    val group = new GroupMetadata("foo", initialState = Empty)
    assertEquals(group, groupMetadataManager.addGroup(group))
    assertEquals(group, groupMetadataManager.addGroup(new GroupMetadata("foo", initialState = Empty)))
  }

  @Test
  def testStoreEmptyGroup(): Unit = {
    val generation = 27
    val protocolType = "consumer"

    val group = GroupMetadata.loadGroup(groupId, Empty, generation, protocolType, null, null, Seq.empty)
    groupMetadataManager.addGroup(group)

    val capturedRecords = expectAppendMessage(Errors.NONE)
    EasyMock.replay(replicaManager)

    var maybeError: Option[Errors] = None
    def callback(error: Errors): Unit = {
      maybeError = Some(error)
    }

    groupMetadataManager.storeGroup(group, Map.empty, callback)
    assertEquals(Some(Errors.NONE), maybeError)
    assertTrue(capturedRecords.hasCaptured)
    val records = capturedRecords.getValue()(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupPartitionId))
        .records.asScala.toList
    assertEquals(1, records.size)

    val record = records.head
    val groupMetadata = GroupMetadataManager.readGroupMessageValue(groupId, record.value)
    assertTrue(groupMetadata.is(Empty))
    assertEquals(generation, groupMetadata.generationId)
    assertEquals(Some(protocolType), groupMetadata.protocolType)
  }

  @Test
  def testStoreEmptySimpleGroup(): Unit = {
    val group = new GroupMetadata(groupId, initialState = Empty)
    groupMetadataManager.addGroup(group)

    val capturedRecords = expectAppendMessage(Errors.NONE)
    EasyMock.replay(replicaManager)

    var maybeError: Option[Errors] = None
    def callback(error: Errors): Unit = {
      maybeError = Some(error)
    }

    groupMetadataManager.storeGroup(group, Map.empty, callback)
    assertEquals(Some(Errors.NONE), maybeError)
    assertTrue(capturedRecords.hasCaptured)

    assertTrue(capturedRecords.hasCaptured)
    val records = capturedRecords.getValue()(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupPartitionId))
      .records.asScala.toList
    assertEquals(1, records.size)

    val record = records.head
    val groupMetadata = GroupMetadataManager.readGroupMessageValue(groupId, record.value)
    assertTrue(groupMetadata.is(Empty))
    assertEquals(0, groupMetadata.generationId)
    assertEquals(None, groupMetadata.protocolType)
  }

  @Test
  def testStoreGroupErrorMapping(): Unit = {
    assertStoreGroupErrorMapping(Errors.NONE, Errors.NONE)
    assertStoreGroupErrorMapping(Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.COORDINATOR_NOT_AVAILABLE)
    assertStoreGroupErrorMapping(Errors.NOT_ENOUGH_REPLICAS, Errors.COORDINATOR_NOT_AVAILABLE)
    assertStoreGroupErrorMapping(Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND, Errors.COORDINATOR_NOT_AVAILABLE)
    assertStoreGroupErrorMapping(Errors.NOT_LEADER_FOR_PARTITION, Errors.NOT_COORDINATOR)
    assertStoreGroupErrorMapping(Errors.MESSAGE_TOO_LARGE, Errors.UNKNOWN_SERVER_ERROR)
    assertStoreGroupErrorMapping(Errors.RECORD_LIST_TOO_LARGE, Errors.UNKNOWN_SERVER_ERROR)
    assertStoreGroupErrorMapping(Errors.INVALID_FETCH_SIZE, Errors.UNKNOWN_SERVER_ERROR)
    assertStoreGroupErrorMapping(Errors.CORRUPT_MESSAGE, Errors.CORRUPT_MESSAGE)
  }

  private def assertStoreGroupErrorMapping(appendError: Errors, expectedError: Errors): Unit = {
    EasyMock.reset(replicaManager)

    val group = new GroupMetadata(groupId, initialState = Empty)
    groupMetadataManager.addGroup(group)

    expectAppendMessage(appendError)
    EasyMock.replay(replicaManager)

    var maybeError: Option[Errors] = None
    def callback(error: Errors): Unit = {
      maybeError = Some(error)
    }

    groupMetadataManager.storeGroup(group, Map.empty, callback)
    assertEquals(Some(expectedError), maybeError)

    EasyMock.verify(replicaManager)
  }

  @Test
  def testStoreNonEmptyGroup(): Unit = {
    val memberId = "memberId"
    val clientId = "clientId"
    val clientHost = "localhost"

    val group = new GroupMetadata(groupId, initialState = Empty)
    groupMetadataManager.addGroup(group)

    val member = new MemberMetadata(memberId, groupId, clientId, clientHost, rebalanceTimeout, sessionTimeout,
      protocolType, List(("protocol", Array[Byte]())))
    member.awaitingJoinCallback = _ => ()
    group.add(member)
    group.transitionTo(PreparingRebalance)
    group.initNextGeneration()

    expectAppendMessage(Errors.NONE)
    EasyMock.replay(replicaManager)

    var maybeError: Option[Errors] = None
    def callback(error: Errors): Unit = {
      maybeError = Some(error)
    }

    groupMetadataManager.storeGroup(group, Map(memberId -> Array[Byte]()), callback)
    assertEquals(Some(Errors.NONE), maybeError)

    EasyMock.verify(replicaManager)
  }

  @Test
  def testStoreNonEmptyGroupWhenCoordinatorHasMoved(): Unit = {
    EasyMock.expect(replicaManager.getMagic(EasyMock.anyObject())).andReturn(None)
    val memberId = "memberId"
    val clientId = "clientId"
    val clientHost = "localhost"

    val group = new GroupMetadata(groupId, initialState = Empty)

    val member = new MemberMetadata(memberId, groupId, clientId, clientHost, rebalanceTimeout, sessionTimeout,
      protocolType, List(("protocol", Array[Byte]())))
    member.awaitingJoinCallback = _ => ()
    group.add(member)
    group.transitionTo(PreparingRebalance)
    group.initNextGeneration()

    EasyMock.replay(replicaManager)

    var maybeError: Option[Errors] = None
    def callback(error: Errors): Unit = {
      maybeError = Some(error)
    }

    groupMetadataManager.storeGroup(group, Map(memberId -> Array[Byte]()), callback)
    assertEquals(Some(Errors.NOT_COORDINATOR), maybeError)

    EasyMock.verify(replicaManager)
  }

  @Test
  def testCommitOffset(): Unit = {
    val memberId = ""
    val topicPartition = new TopicPartition("foo", 0)
    val offset = 37

    groupMetadataManager.addPartitionOwnership(groupPartitionId)

    val group = new GroupMetadata(groupId, initialState = Empty)
    groupMetadataManager.addGroup(group)

    val offsets = immutable.Map(topicPartition -> OffsetAndMetadata(offset))

    expectAppendMessage(Errors.NONE)
    EasyMock.replay(replicaManager)

    var commitErrors: Option[immutable.Map[TopicPartition, Errors]] = None
    def callback(errors: immutable.Map[TopicPartition, Errors]): Unit = {
      commitErrors = Some(errors)
    }

    groupMetadataManager.storeOffsets(group, memberId, offsets, callback)
    assertTrue(group.hasOffsets)

    assertFalse(commitErrors.isEmpty)
    val maybeError = commitErrors.get.get(topicPartition)
    assertEquals(Some(Errors.NONE), maybeError)
    assertTrue(group.hasOffsets)

    val cachedOffsets = groupMetadataManager.getOffsets(groupId, Some(Seq(topicPartition)))
    val maybePartitionResponse = cachedOffsets.get(topicPartition)
    assertFalse(maybePartitionResponse.isEmpty)

    val partitionResponse = maybePartitionResponse.get
    assertEquals(Errors.NONE, partitionResponse.error)
    assertEquals(offset, partitionResponse.offset)

    EasyMock.verify(replicaManager)
  }

  @Test
  def testTransactionalCommitOffsetCommitted(): Unit = {
    val memberId = ""
    val topicPartition = new TopicPartition("foo", 0)
    val offset = 37
    val producerId = 232L
    val producerEpoch = 0.toShort

    groupMetadataManager.addPartitionOwnership(groupPartitionId)

    val group = new GroupMetadata(groupId, initialState = Empty)
    groupMetadataManager.addGroup(group)

    val offsets = immutable.Map(topicPartition -> OffsetAndMetadata(offset))

    val capturedResponseCallback = appendAndCaptureCallback()
    EasyMock.replay(replicaManager)

    var commitErrors: Option[immutable.Map[TopicPartition, Errors]] = None
    def callback(errors: immutable.Map[TopicPartition, Errors]): Unit = {
      commitErrors = Some(errors)
    }

    groupMetadataManager.storeOffsets(group, memberId, offsets, callback, producerId, producerEpoch)
    assertTrue(group.hasOffsets)
    assertTrue(group.allOffsets.isEmpty)
    capturedResponseCallback.getValue.apply(Map(groupTopicPartition ->
      new PartitionResponse(Errors.NONE, 0L, RecordBatch.NO_TIMESTAMP, 0L)))

    assertTrue(group.hasOffsets)
    assertTrue(group.allOffsets.isEmpty)

    group.completePendingTxnOffsetCommit(producerId, isCommit = true)
    assertTrue(group.hasOffsets)
    assertFalse(group.allOffsets.isEmpty)
    assertEquals(Some(OffsetAndMetadata(offset)), group.offset(topicPartition))

    EasyMock.verify(replicaManager)
  }

  @Test
  def testTransactionalCommitOffsetAppendFailure(): Unit = {
    val memberId = ""
    val topicPartition = new TopicPartition("foo", 0)
    val offset = 37
    val producerId = 232L
    val producerEpoch = 0.toShort

    groupMetadataManager.addPartitionOwnership(groupPartitionId)

    val group = new GroupMetadata(groupId, initialState = Empty)
    groupMetadataManager.addGroup(group)

    val offsets = immutable.Map(topicPartition -> OffsetAndMetadata(offset))

    val capturedResponseCallback = appendAndCaptureCallback()
    EasyMock.replay(replicaManager)

    var commitErrors: Option[immutable.Map[TopicPartition, Errors]] = None
    def callback(errors: immutable.Map[TopicPartition, Errors]): Unit = {
      commitErrors = Some(errors)
    }

    groupMetadataManager.storeOffsets(group, memberId, offsets, callback, producerId, producerEpoch)
    assertTrue(group.hasOffsets)
    assertTrue(group.allOffsets.isEmpty)
    capturedResponseCallback.getValue.apply(Map(groupTopicPartition ->
      new PartitionResponse(Errors.NOT_ENOUGH_REPLICAS, 0L, RecordBatch.NO_TIMESTAMP, 0L)))

    assertFalse(group.hasOffsets)
    assertTrue(group.allOffsets.isEmpty)

    group.completePendingTxnOffsetCommit(producerId, isCommit = false)
    assertFalse(group.hasOffsets)
    assertTrue(group.allOffsets.isEmpty)

    EasyMock.verify(replicaManager)
  }

  @Test
  def testTransactionalCommitOffsetAborted(): Unit = {
    val memberId = ""
    val topicPartition = new TopicPartition("foo", 0)
    val offset = 37
    val producerId = 232L
    val producerEpoch = 0.toShort

    groupMetadataManager.addPartitionOwnership(groupPartitionId)

    val group = new GroupMetadata(groupId, initialState = Empty)
    groupMetadataManager.addGroup(group)

    val offsets = immutable.Map(topicPartition -> OffsetAndMetadata(offset))

    val capturedResponseCallback = appendAndCaptureCallback()
    EasyMock.replay(replicaManager)

    var commitErrors: Option[immutable.Map[TopicPartition, Errors]] = None
    def callback(errors: immutable.Map[TopicPartition, Errors]): Unit = {
      commitErrors = Some(errors)
    }

    groupMetadataManager.storeOffsets(group, memberId, offsets, callback, producerId, producerEpoch)
    assertTrue(group.hasOffsets)
    assertTrue(group.allOffsets.isEmpty)
    capturedResponseCallback.getValue.apply(Map(groupTopicPartition ->
      new PartitionResponse(Errors.NONE, 0L, RecordBatch.NO_TIMESTAMP, 0L)))

    assertTrue(group.hasOffsets)
    assertTrue(group.allOffsets.isEmpty)

    group.completePendingTxnOffsetCommit(producerId, isCommit = false)
    assertFalse(group.hasOffsets)
    assertTrue(group.allOffsets.isEmpty)

    EasyMock.verify(replicaManager)
  }

  @Test
  def testCommitOffsetWhenCoordinatorHasMoved(): Unit = {
    EasyMock.expect(replicaManager.getMagic(EasyMock.anyObject())).andReturn(None)
    val memberId = ""
    val topicPartition = new TopicPartition("foo", 0)
    val offset = 37

    groupMetadataManager.addPartitionOwnership(groupPartitionId)

    val group = new GroupMetadata(groupId, initialState = Empty)
    groupMetadataManager.addGroup(group)

    val offsets = immutable.Map(topicPartition -> OffsetAndMetadata(offset))

    EasyMock.replay(replicaManager)

    var commitErrors: Option[immutable.Map[TopicPartition, Errors]] = None
    def callback(errors: immutable.Map[TopicPartition, Errors]): Unit = {
      commitErrors = Some(errors)
    }

    groupMetadataManager.storeOffsets(group, memberId, offsets, callback)

    assertFalse(commitErrors.isEmpty)
    val maybeError = commitErrors.get.get(topicPartition)
    assertEquals(Some(Errors.NOT_COORDINATOR), maybeError)

    EasyMock.verify(replicaManager)
  }

  @Test
  def testCommitOffsetFailure(): Unit = {
    assertCommitOffsetErrorMapping(Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.COORDINATOR_NOT_AVAILABLE)
    assertCommitOffsetErrorMapping(Errors.NOT_ENOUGH_REPLICAS, Errors.COORDINATOR_NOT_AVAILABLE)
    assertCommitOffsetErrorMapping(Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND, Errors.COORDINATOR_NOT_AVAILABLE)
    assertCommitOffsetErrorMapping(Errors.NOT_LEADER_FOR_PARTITION, Errors.NOT_COORDINATOR)
    assertCommitOffsetErrorMapping(Errors.MESSAGE_TOO_LARGE, Errors.INVALID_COMMIT_OFFSET_SIZE)
    assertCommitOffsetErrorMapping(Errors.RECORD_LIST_TOO_LARGE, Errors.INVALID_COMMIT_OFFSET_SIZE)
    assertCommitOffsetErrorMapping(Errors.INVALID_FETCH_SIZE, Errors.INVALID_COMMIT_OFFSET_SIZE)
    assertCommitOffsetErrorMapping(Errors.CORRUPT_MESSAGE, Errors.CORRUPT_MESSAGE)
  }

  private def assertCommitOffsetErrorMapping(appendError: Errors, expectedError: Errors): Unit = {
    EasyMock.reset(replicaManager)

    val memberId = ""
    val topicPartition = new TopicPartition("foo", 0)
    val offset = 37

    groupMetadataManager.addPartitionOwnership(groupPartitionId)

    val group = new GroupMetadata(groupId, initialState = Empty)
    groupMetadataManager.addGroup(group)

    val offsets = immutable.Map(topicPartition -> OffsetAndMetadata(offset))

    val capturedResponseCallback = appendAndCaptureCallback()
    EasyMock.replay(replicaManager)

    var commitErrors: Option[immutable.Map[TopicPartition, Errors]] = None
    def callback(errors: immutable.Map[TopicPartition, Errors]): Unit = {
      commitErrors = Some(errors)
    }

    groupMetadataManager.storeOffsets(group, memberId, offsets, callback)
    assertTrue(group.hasOffsets)
    capturedResponseCallback.getValue.apply(Map(groupTopicPartition ->
      new PartitionResponse(appendError, 0L, RecordBatch.NO_TIMESTAMP, 0L)))

    assertFalse(commitErrors.isEmpty)
    val maybeError = commitErrors.get.get(topicPartition)
    assertEquals(Some(expectedError), maybeError)
    assertFalse(group.hasOffsets)

    val cachedOffsets = groupMetadataManager.getOffsets(groupId, Some(Seq(topicPartition)))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(topicPartition).map(_.offset))

    EasyMock.verify(replicaManager)
  }

  @Test
  def testExpireOffset(): Unit = {
    val memberId = ""
    val topicPartition1 = new TopicPartition("foo", 0)
    val topicPartition2 = new TopicPartition("foo", 1)
    val offset = 37

    groupMetadataManager.addPartitionOwnership(groupPartitionId)

    val group = new GroupMetadata(groupId, initialState = Empty)
    groupMetadataManager.addGroup(group)

    // expire the offset after 1 millisecond
    val startMs = time.milliseconds
    val offsets = immutable.Map(
      topicPartition1 -> OffsetAndMetadata(offset, "", startMs, startMs + 1),
      topicPartition2 -> OffsetAndMetadata(offset, "", startMs, startMs + 3))

    mockGetPartition()
    expectAppendMessage(Errors.NONE)
    EasyMock.replay(replicaManager)

    var commitErrors: Option[immutable.Map[TopicPartition, Errors]] = None
    def callback(errors: immutable.Map[TopicPartition, Errors]): Unit = {
      commitErrors = Some(errors)
    }

    groupMetadataManager.storeOffsets(group, memberId, offsets, callback)
    assertTrue(group.hasOffsets)

    assertFalse(commitErrors.isEmpty)
    assertEquals(Some(Errors.NONE), commitErrors.get.get(topicPartition1))

    // expire only one of the offsets
    time.sleep(2)

    EasyMock.reset(partition)
    EasyMock.expect(partition.appendRecordsToLeader(EasyMock.anyObject(classOf[MemoryRecords]),
      isFromClient = EasyMock.eq(false), requiredAcks = EasyMock.anyInt()))
      .andReturn(LogAppendInfo.UnknownLogAppendInfo)
    EasyMock.replay(partition)

    groupMetadataManager.cleanupGroupMetadata()

    assertEquals(Some(group), groupMetadataManager.getGroup(groupId))
    assertEquals(None, group.offset(topicPartition1))
    assertEquals(Some(offset), group.offset(topicPartition2).map(_.offset))

    val cachedOffsets = groupMetadataManager.getOffsets(groupId, Some(Seq(topicPartition1, topicPartition2)))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(topicPartition1).map(_.offset))
    assertEquals(Some(offset), cachedOffsets.get(topicPartition2).map(_.offset))

    EasyMock.verify(replicaManager)
  }

  @Test
  def testGroupMetadataRemoval(): Unit = {
    val topicPartition1 = new TopicPartition("foo", 0)
    val topicPartition2 = new TopicPartition("foo", 1)

    groupMetadataManager.addPartitionOwnership(groupPartitionId)

    val group = new GroupMetadata(groupId, initialState = Empty)
    groupMetadataManager.addGroup(group)
    group.generationId = 5

    // expect the group metadata tombstone
    EasyMock.reset(partition)
    val recordsCapture: Capture[MemoryRecords] = EasyMock.newCapture()

    EasyMock.expect(replicaManager.getMagic(EasyMock.anyObject())).andStubReturn(Some(RecordBatch.CURRENT_MAGIC_VALUE))
    mockGetPartition()
    EasyMock.expect(partition.appendRecordsToLeader(EasyMock.capture(recordsCapture),
      isFromClient = EasyMock.eq(false), requiredAcks = EasyMock.anyInt()))
      .andReturn(LogAppendInfo.UnknownLogAppendInfo)
    EasyMock.replay(replicaManager, partition)

    groupMetadataManager.cleanupGroupMetadata()

    assertTrue(recordsCapture.hasCaptured)

    val records = recordsCapture.getValue.records.asScala.toList
    recordsCapture.getValue.batches.asScala.foreach { batch =>
      assertEquals(RecordBatch.CURRENT_MAGIC_VALUE, batch.magic)
      assertEquals(TimestampType.CREATE_TIME, batch.timestampType)
    }
    assertEquals(1, records.size)

    val metadataTombstone = records.head
    assertTrue(metadataTombstone.hasKey)
    assertFalse(metadataTombstone.hasValue)
    assertTrue(metadataTombstone.timestamp > 0)

    val groupKey = GroupMetadataManager.readMessageKey(metadataTombstone.key).asInstanceOf[GroupMetadataKey]
    assertEquals(groupId, groupKey.key)

    // the full group should be gone since all offsets were removed
    assertEquals(None, groupMetadataManager.getGroup(groupId))
    val cachedOffsets = groupMetadataManager.getOffsets(groupId, Some(Seq(topicPartition1, topicPartition2)))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(topicPartition1).map(_.offset))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(topicPartition2).map(_.offset))
  }

  @Test
  def testGroupMetadataRemovalWithLogAppendTime(): Unit = {
    val topicPartition1 = new TopicPartition("foo", 0)
    val topicPartition2 = new TopicPartition("foo", 1)

    groupMetadataManager.addPartitionOwnership(groupPartitionId)

    val group = new GroupMetadata(groupId, initialState = Empty)
    groupMetadataManager.addGroup(group)
    group.generationId = 5

    // expect the group metadata tombstone
    EasyMock.reset(partition)
    val recordsCapture: Capture[MemoryRecords] = EasyMock.newCapture()

    EasyMock.expect(replicaManager.getMagic(EasyMock.anyObject())).andStubReturn(Some(RecordBatch.CURRENT_MAGIC_VALUE))
    mockGetPartition()
    EasyMock.expect(partition.appendRecordsToLeader(EasyMock.capture(recordsCapture),
      isFromClient = EasyMock.eq(false), requiredAcks = EasyMock.anyInt()))
      .andReturn(LogAppendInfo.UnknownLogAppendInfo)
    EasyMock.replay(replicaManager, partition)

    groupMetadataManager.cleanupGroupMetadata()

    assertTrue(recordsCapture.hasCaptured)

    val records = recordsCapture.getValue.records.asScala.toList
    recordsCapture.getValue.batches.asScala.foreach { batch =>
      assertEquals(RecordBatch.CURRENT_MAGIC_VALUE, batch.magic)
      // Use CREATE_TIME, like the producer. The conversion to LOG_APPEND_TIME (if necessary) happens automatically.
      assertEquals(TimestampType.CREATE_TIME, batch.timestampType)
    }
    assertEquals(1, records.size)

    val metadataTombstone = records.head
    assertTrue(metadataTombstone.hasKey)
    assertFalse(metadataTombstone.hasValue)
    assertTrue(metadataTombstone.timestamp > 0)

    val groupKey = GroupMetadataManager.readMessageKey(metadataTombstone.key).asInstanceOf[GroupMetadataKey]
    assertEquals(groupId, groupKey.key)

    // the full group should be gone since all offsets were removed
    assertEquals(None, groupMetadataManager.getGroup(groupId))
    val cachedOffsets = groupMetadataManager.getOffsets(groupId, Some(Seq(topicPartition1, topicPartition2)))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(topicPartition1).map(_.offset))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(topicPartition2).map(_.offset))
  }

  @Test
  def testExpireGroupWithOffsetsOnly(): Unit = {
    // verify that the group is removed properly, but no tombstone is written if
    // this is a group which is only using kafka for offset storage

    val memberId = ""
    val topicPartition1 = new TopicPartition("foo", 0)
    val topicPartition2 = new TopicPartition("foo", 1)
    val offset = 37

    groupMetadataManager.addPartitionOwnership(groupPartitionId)

    val group = new GroupMetadata(groupId, initialState = Empty)
    groupMetadataManager.addGroup(group)

    // expire the offset after 1 millisecond
    val startMs = time.milliseconds
    val offsets = immutable.Map(
      topicPartition1 -> OffsetAndMetadata(offset, "", startMs, startMs + 1),
      topicPartition2 -> OffsetAndMetadata(offset, "", startMs, startMs + 3))

    mockGetPartition()
    expectAppendMessage(Errors.NONE)
    EasyMock.replay(replicaManager)

    var commitErrors: Option[immutable.Map[TopicPartition, Errors]] = None
    def callback(errors: immutable.Map[TopicPartition, Errors]): Unit = {
      commitErrors = Some(errors)
    }

    groupMetadataManager.storeOffsets(group, memberId, offsets, callback)
    assertTrue(group.hasOffsets)

    assertFalse(commitErrors.isEmpty)
    assertEquals(Some(Errors.NONE), commitErrors.get.get(topicPartition1))

    // expire all of the offsets
    time.sleep(4)

    // expect the offset tombstone
    EasyMock.reset(partition)
    val recordsCapture: Capture[MemoryRecords] = EasyMock.newCapture()

    EasyMock.expect(partition.appendRecordsToLeader(EasyMock.capture(recordsCapture),
      isFromClient = EasyMock.eq(false), requiredAcks = EasyMock.anyInt()))
      .andReturn(LogAppendInfo.UnknownLogAppendInfo)
    EasyMock.replay(partition)

    groupMetadataManager.cleanupGroupMetadata()

    assertTrue(recordsCapture.hasCaptured)

    // verify the tombstones are correct and only for the expired offsets
    val records = recordsCapture.getValue.records.asScala.toList
    assertEquals(2, records.size)
    records.foreach { message =>
      assertTrue(message.hasKey)
      assertFalse(message.hasValue)
      val offsetKey = GroupMetadataManager.readMessageKey(message.key).asInstanceOf[OffsetKey]
      assertEquals(groupId, offsetKey.key.group)
      assertEquals("foo", offsetKey.key.topicPartition.topic)
    }

    // the full group should be gone since all offsets were removed
    assertEquals(None, groupMetadataManager.getGroup(groupId))
    val cachedOffsets = groupMetadataManager.getOffsets(groupId, Some(Seq(topicPartition1, topicPartition2)))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(topicPartition1).map(_.offset))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(topicPartition2).map(_.offset))

    EasyMock.verify(replicaManager)
  }

  @Test
  def testExpireOffsetsWithActiveGroup(): Unit = {
    val memberId = "memberId"
    val clientId = "clientId"
    val clientHost = "localhost"
    val topicPartition1 = new TopicPartition("foo", 0)
    val topicPartition2 = new TopicPartition("foo", 1)
    val offset = 37

    groupMetadataManager.addPartitionOwnership(groupPartitionId)

    val group = new GroupMetadata(groupId, initialState = Empty)
    groupMetadataManager.addGroup(group)

    val member = new MemberMetadata(memberId, groupId, clientId, clientHost, rebalanceTimeout, sessionTimeout,
      protocolType, List(("protocol", Array[Byte]())))
    member.awaitingJoinCallback = _ => ()
    group.add(member)
    group.transitionTo(PreparingRebalance)
    group.initNextGeneration()

    // expire the offset after 1 millisecond
    val startMs = time.milliseconds
    val offsets = immutable.Map(
      topicPartition1 -> OffsetAndMetadata(offset, "", startMs, startMs + 1),
      topicPartition2 -> OffsetAndMetadata(offset, "", startMs, startMs + 3))

    mockGetPartition()
    expectAppendMessage(Errors.NONE)
    EasyMock.replay(replicaManager)

    var commitErrors: Option[immutable.Map[TopicPartition, Errors]] = None
    def callback(errors: immutable.Map[TopicPartition, Errors]): Unit = {
      commitErrors = Some(errors)
    }

    groupMetadataManager.storeOffsets(group, memberId, offsets, callback)
    assertTrue(group.hasOffsets)

    assertFalse(commitErrors.isEmpty)
    assertEquals(Some(Errors.NONE), commitErrors.get.get(topicPartition1))

    // expire all of the offsets
    time.sleep(4)

    // expect the offset tombstone
    EasyMock.reset(partition)
    EasyMock.expect(partition.appendRecordsToLeader(EasyMock.anyObject(classOf[MemoryRecords]),
      isFromClient = EasyMock.eq(false), requiredAcks = EasyMock.anyInt()))
      .andReturn(LogAppendInfo.UnknownLogAppendInfo)
    EasyMock.replay(partition)

    groupMetadataManager.cleanupGroupMetadata()

    // group should still be there, but the offsets should be gone
    assertEquals(Some(group), groupMetadataManager.getGroup(groupId))
    assertEquals(None, group.offset(topicPartition1))
    assertEquals(None, group.offset(topicPartition2))

    val cachedOffsets = groupMetadataManager.getOffsets(groupId, Some(Seq(topicPartition1, topicPartition2)))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(topicPartition1).map(_.offset))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(topicPartition2).map(_.offset))

    EasyMock.verify(replicaManager)
  }

  private def appendAndCaptureCallback(): Capture[Map[TopicPartition, PartitionResponse] => Unit] = {
    val capturedArgument: Capture[Map[TopicPartition, PartitionResponse] => Unit] = EasyMock.newCapture()
    EasyMock.expect(replicaManager.appendRecords(EasyMock.anyLong(),
      EasyMock.anyShort(),
      internalTopicsAllowed = EasyMock.eq(true),
      isFromClient = EasyMock.eq(false),
      EasyMock.anyObject().asInstanceOf[Map[TopicPartition, MemoryRecords]],
      EasyMock.capture(capturedArgument),
      EasyMock.anyObject().asInstanceOf[Option[ReentrantLock]],
      EasyMock.anyObject())
    )
    EasyMock.expect(replicaManager.getMagic(EasyMock.anyObject())).andStubReturn(Some(RecordBatch.CURRENT_MAGIC_VALUE))
    capturedArgument
  }

  private def expectAppendMessage(error: Errors): Capture[Map[TopicPartition, MemoryRecords]] = {
    val capturedCallback: Capture[Map[TopicPartition, PartitionResponse] => Unit] = EasyMock.newCapture()
    val capturedRecords: Capture[Map[TopicPartition, MemoryRecords]] = EasyMock.newCapture()
    EasyMock.expect(replicaManager.appendRecords(EasyMock.anyLong(),
      EasyMock.anyShort(),
      internalTopicsAllowed = EasyMock.eq(true),
      isFromClient = EasyMock.eq(false),
      EasyMock.capture(capturedRecords),
      EasyMock.capture(capturedCallback),
      EasyMock.anyObject().asInstanceOf[Option[ReentrantLock]],
      EasyMock.anyObject())
    ).andAnswer(new IAnswer[Unit] {
      override def answer = capturedCallback.getValue.apply(
        Map(groupTopicPartition ->
          new PartitionResponse(error, 0L, RecordBatch.NO_TIMESTAMP, 0L)
        )
      )})
    EasyMock.expect(replicaManager.getMagic(EasyMock.anyObject())).andStubReturn(Some(RecordBatch.CURRENT_MAGIC_VALUE))
    capturedRecords
  }

  private def buildStableGroupRecordWithMember(generation: Int,
                                               protocolType: String,
                                               protocol: String,
                                               memberId: String): SimpleRecord = {
    val memberProtocols = List((protocol, Array.emptyByteArray))
    val member = new MemberMetadata(memberId, groupId, "clientId", "clientHost", 30000, 10000, protocolType, memberProtocols)
    val group = GroupMetadata.loadGroup(groupId, Stable, generation, protocolType, protocol,
      leaderId = memberId, Seq(member))
    val groupMetadataKey = GroupMetadataManager.groupMetadataKey(groupId)
    val groupMetadataValue = GroupMetadataManager.groupMetadataValue(group, Map(memberId -> Array.empty[Byte]))
    new SimpleRecord(groupMetadataKey, groupMetadataValue)
  }

  private def buildEmptyGroupRecord(generation: Int, protocolType: String): SimpleRecord = {
    val group = GroupMetadata.loadGroup(groupId, Empty, generation, protocolType, null, null, Seq.empty)
    val groupMetadataKey = GroupMetadataManager.groupMetadataKey(groupId)
    val groupMetadataValue = GroupMetadataManager.groupMetadataValue(group, Map.empty)
    new SimpleRecord(groupMetadataKey, groupMetadataValue)
  }

  private def expectGroupMetadataLoad(groupMetadataTopicPartition: TopicPartition,
                                      startOffset: Long,
                                      records: MemoryRecords): Unit = {
    val logMock =  EasyMock.mock(classOf[Log])
    EasyMock.expect(replicaManager.getLog(groupMetadataTopicPartition)).andStubReturn(Some(logMock))
    val endOffset = expectGroupMetadataLoad(logMock, startOffset, records)
    EasyMock.expect(replicaManager.getLogEndOffset(groupMetadataTopicPartition)).andStubReturn(Some(endOffset))
    EasyMock.replay(logMock)
  }

  /**
   * mock records into a mocked log
   *
   * @return the calculated end offset to be mocked into [[ReplicaManager.getLogEndOffset]]
   */
  private def expectGroupMetadataLoad(logMock: Log,
                                      startOffset: Long,
                                      records: MemoryRecords): Long = {
    val endOffset = startOffset + records.records.asScala.size
    val fileRecordsMock = EasyMock.mock(classOf[FileRecords])

    EasyMock.expect(logMock.logStartOffset).andStubReturn(startOffset)
    EasyMock.expect(logMock.read(EasyMock.eq(startOffset), EasyMock.anyInt(), EasyMock.eq(None),
      EasyMock.eq(true), EasyMock.eq(IsolationLevel.READ_UNCOMMITTED)))
      .andReturn(FetchDataInfo(LogOffsetMetadata(startOffset), fileRecordsMock))
    EasyMock.expect(fileRecordsMock.readInto(EasyMock.anyObject(classOf[ByteBuffer]), EasyMock.anyInt()))
      .andReturn(records.buffer)
    EasyMock.replay(fileRecordsMock)

    endOffset
  }

  private def createCommittedOffsetRecords(committedOffsets: Map[TopicPartition, Long],
                                           groupId: String = groupId): Seq[SimpleRecord] = {
    committedOffsets.map { case (topicPartition, offset) =>
      val offsetAndMetadata = OffsetAndMetadata(offset)
      val offsetCommitKey = GroupMetadataManager.offsetCommitKey(groupId, topicPartition)
      val offsetCommitValue = GroupMetadataManager.offsetCommitValue(offsetAndMetadata)
      new SimpleRecord(offsetCommitKey, offsetCommitValue)
    }.toSeq
  }

  private def mockGetPartition(): Unit = {
    EasyMock.expect(replicaManager.getPartition(groupTopicPartition)).andStubReturn(Some(partition))
    EasyMock.expect(replicaManager.nonOfflinePartition(groupTopicPartition)).andStubReturn(Some(partition))
  }

  private def getGauge(manager: GroupMetadataManager, name: String): Gauge[Int]  = {
    Metrics.defaultRegistry().allMetrics().get(manager.metricName(name, Map.empty)).asInstanceOf[Gauge[Int]]
  }

  private def expectMetrics(manager: GroupMetadataManager,
                            expectedNumGroups: Int,
                            expectedNumGroupsPreparingRebalance: Int,
                            expectedNumGroupsCompletingRebalance: Int): Unit = {
    assertEquals(expectedNumGroups, getGauge(manager, "NumGroups").value)
    assertEquals(expectedNumGroupsPreparingRebalance, getGauge(manager, "NumGroupsPreparingRebalance").value)
    assertEquals(expectedNumGroupsCompletingRebalance, getGauge(manager, "NumGroupsCompletingRebalance").value)
  }

  @Test
  def testMetrics(): Unit = {
    groupMetadataManager.cleanupGroupMetadata()
    expectMetrics(groupMetadataManager, 0, 0, 0)
    val group = new GroupMetadata("foo2", Stable)
    groupMetadataManager.addGroup(group)
    expectMetrics(groupMetadataManager, 1, 0, 0)
    group.transitionTo(PreparingRebalance)
    expectMetrics(groupMetadataManager, 1, 1, 0)
    group.transitionTo(CompletingRebalance)
    expectMetrics(groupMetadataManager, 1, 0, 1)
  }
}
