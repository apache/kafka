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

import java.lang.management.ManagementFactory
import java.nio.ByteBuffer
import java.util.concurrent.locks.ReentrantLock
import java.util.{Collections, OptionalInt, OptionalLong}
import com.yammer.metrics.core.Gauge

import javax.management.ObjectName
import kafka.cluster.Partition
import kafka.log.UnifiedLog
import kafka.server.{HostedPartition, KafkaConfig, ReplicaManager}
import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.metrics.{JmxReporter, KafkaMetricsContext, Metrics => kMetrics}
import org.apache.kafka.common.protocol.types.Field.TaggedFieldsSection
import org.apache.kafka.common.protocol.types.{CompactArrayOf, Field, Schema, Struct, Type}
import org.apache.kafka.common.protocol.{ByteBufferAccessor, Errors, MessageUtil}
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.OffsetFetchResponse
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.coordinator.group.{GroupCoordinatorConfig, OffsetAndMetadata, OffsetConfig}
import org.apache.kafka.coordinator.group.generated.{CoordinatorRecordType, GroupMetadataValue, OffsetCommitKey, OffsetCommitValue, GroupMetadataKey => GroupMetadataKeyData}
import org.apache.kafka.server.common.{MetadataVersion, RequestLocal}
import org.apache.kafka.server.common.MetadataVersion._
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.apache.kafka.server.storage.log.FetchIsolation
import org.apache.kafka.server.util.{KafkaScheduler, MockTime}
import org.apache.kafka.storage.internals.log.{AppendOrigin, FetchDataInfo, LogAppendInfo, LogOffsetMetadata, VerificationGuard}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.mockito.ArgumentMatchers.{any, anyInt, anyLong, anyShort}
import org.mockito.Mockito.{mock, reset, times, verify, when}

import scala.jdk.CollectionConverters._
import scala.collection.{immutable, _}

class GroupMetadataManagerTest {

  var time: MockTime = _
  var replicaManager: ReplicaManager = _
  var groupMetadataManager: GroupMetadataManager = _
  var scheduler: KafkaScheduler = _
  var partition: Partition = _
  var defaultOffsetRetentionMs = Long.MaxValue
  var metrics: kMetrics = _

  val groupId = "foo"
  val groupInstanceId = "bar"
  val groupPartitionId = 0
  val groupTopicPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupPartitionId)
  val protocolType = "protocolType"
  val rebalanceTimeout = 60000
  val sessionTimeout = 10000
  val defaultRequireStable = false
  val numOffsetsPartitions = 2
  val noLeader = OptionalInt.empty()
  val noExpiration = OptionalLong.empty()

  private val offsetConfig = {
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(nodeId = 0, zkConnect = ""))
    new OffsetConfig(config.groupCoordinatorConfig.offsetMetadataMaxSize,
      config.groupCoordinatorConfig.offsetsLoadBufferSize,
      config.groupCoordinatorConfig.offsetsRetentionMs,
      config.groupCoordinatorConfig.offsetsRetentionCheckIntervalMs,
      config.groupCoordinatorConfig.offsetsTopicPartitions,
      config.groupCoordinatorConfig.offsetsTopicSegmentBytes,
      config.groupCoordinatorConfig.offsetsTopicReplicationFactor,
      config.groupCoordinatorConfig.offsetTopicCompressionType,
      config.groupCoordinatorConfig.offsetCommitTimeoutMs)
  }

  @BeforeEach
  def setUp(): Unit = {
    defaultOffsetRetentionMs = offsetConfig.offsetsRetentionMs
    metrics = new kMetrics()
    time = new MockTime
    replicaManager = mock(classOf[ReplicaManager])
    groupMetadataManager = new GroupMetadataManager(0, MetadataVersion.latestTesting, offsetConfig, replicaManager,
      time, metrics)
    groupMetadataManager.startup(() => numOffsetsPartitions, enableMetadataExpiration = false)
    partition = mock(classOf[Partition])
  }

  @AfterEach
  def tearDown(): Unit = {
    groupMetadataManager.shutdown()
  }

  @Test
  def testLogInfoFromCleanupGroupMetadata(): Unit = {
    var expiredOffsets: Int = 0
    var infoCount = 0
    val gmm = new GroupMetadataManager(0, MetadataVersion.latestTesting, offsetConfig, replicaManager, time, metrics) {
      override def cleanupGroupMetadata(groups: Iterable[GroupMetadata], requestLocal: RequestLocal,
                                        selector: GroupMetadata => Map[TopicPartition, OffsetAndMetadata]): Int = expiredOffsets

      override def info(msg: => String): Unit = infoCount += 1
    }
    gmm.startup(() => numOffsetsPartitions, enableMetadataExpiration = false)
    try {
      // if there are no offsets to expire, we skip to log
      gmm.cleanupGroupMetadata()
      assertEquals(0, infoCount)
      // if there are offsets to expire, we should log info
      expiredOffsets = 100
      gmm.cleanupGroupMetadata()
      assertEquals(1, infoCount)
    } finally {
      gmm.shutdown()
    }
  }

  @Test
  def testLoadOffsetsWithoutGroup(): Unit = {
    val groupMetadataTopicPartition = groupTopicPartition
    val startOffset = 15L
    val groupEpoch = 2

    val committedOffsets = Map(
      new TopicPartition("foo", 0) -> 23L,
      new TopicPartition("foo", 1) -> 455L,
      new TopicPartition("bar", 0) -> 8992L
    )

    val offsetCommitRecords = createCommittedOffsetRecords(committedOffsets)
    val records = MemoryRecords.withRecords(startOffset, Compression.NONE, offsetCommitRecords.toArray: _*)
    expectGroupMetadataLoad(groupMetadataTopicPartition, startOffset, records)

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, groupEpoch, _ => (), 0L)

    val group = groupMetadataManager.getGroup(groupId).getOrElse(throw new AssertionError("Group was not loaded into the cache"))
    assertEquals(groupId, group.groupId)
    assertEquals(Empty, group.currentState)
    assertEquals(committedOffsets.size, group.allOffsets.size)
    committedOffsets.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.committedOffset))
    }
  }

  @Test
  def testLoadEmptyGroupWithOffsets(): Unit = {
    val groupMetadataTopicPartition = groupTopicPartition
    val generation = 15
    val protocolType = "consumer"
    val startOffset = 15L
    val groupEpoch = 2
    val committedOffsets = Map(
      new TopicPartition("foo", 0) -> 23L,
      new TopicPartition("foo", 1) -> 455L,
      new TopicPartition("bar", 0) -> 8992L
    )

    val offsetCommitRecords = createCommittedOffsetRecords(committedOffsets)
    val groupMetadataRecord = buildEmptyGroupRecord(generation, protocolType)
    val records = MemoryRecords.withRecords(startOffset, Compression.NONE,
      (offsetCommitRecords ++ Seq(groupMetadataRecord)).toArray: _*)

    expectGroupMetadataLoad(groupMetadataTopicPartition, startOffset, records)

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, groupEpoch, _ => (), 0L)

    val group = groupMetadataManager.getGroup(groupId).getOrElse(throw new AssertionError("Group was not loaded into the cache"))
    assertEquals(groupId, group.groupId)
    assertEquals(Empty, group.currentState)
    assertEquals(generation, group.generationId)
    assertEquals(Some(protocolType), group.protocolType)
    assertNull(group.leaderOrNull)
    assertNull(group.protocolName.orNull)
    committedOffsets.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.committedOffset))
    }
  }

  @Test
  def testLoadTransactionalOffsetsWithoutGroup(): Unit = {
    val groupMetadataTopicPartition = groupTopicPartition
    val producerId = 1000L
    val producerEpoch: Short = 2
    val groupEpoch = 2

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

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, groupEpoch, _ => (), 0L)

    val group = groupMetadataManager.getGroup(groupId).getOrElse(throw new AssertionError("Group was not loaded into the cache"))
    assertEquals(groupId, group.groupId)
    assertEquals(Empty, group.currentState)
    assertEquals(committedOffsets.size, group.allOffsets.size)
    committedOffsets.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.committedOffset))
    }
  }

  @Test
  def testDoNotLoadAbortedTransactionalOffsetCommits(): Unit = {
    val groupMetadataTopicPartition = groupTopicPartition
    val producerId = 1000L
    val producerEpoch: Short = 2
    val groupEpoch = 2

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

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, groupEpoch, _ => (), 0L)

    // Since there are no committed offsets for the group, and there is no other group metadata, we don't expect the
    // group to be loaded.
    assertEquals(None, groupMetadataManager.getGroup(groupId))
  }

  @Test
  def testGroupLoadedWithPendingCommits(): Unit = {
    val groupMetadataTopicPartition = groupTopicPartition
    val producerId = 1000L
    val producerEpoch: Short = 2
    val groupEpoch = 2

    val foo0 = new TopicPartition("foo", 0)
    val foo1 = new TopicPartition("foo", 1)
    val bar0 = new TopicPartition("bar", 0)
    val pendingOffsets = Map(
      foo0 -> 23L,
      foo1 -> 455L,
      bar0 -> 8992L
    )

    val buffer = ByteBuffer.allocate(1024)
    var nextOffset = 0
    nextOffset += appendTransactionalOffsetCommits(buffer, producerId, producerEpoch, nextOffset, pendingOffsets)
    buffer.flip()

    val records = MemoryRecords.readableRecords(buffer)
    expectGroupMetadataLoad(groupMetadataTopicPartition, 0, records)

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, groupEpoch, _ => (), 0L)

    // The group should be loaded with pending offsets.
    val group = groupMetadataManager.getGroup(groupId).getOrElse(throw new AssertionError("Group was not loaded into the cache"))
    assertEquals(groupId, group.groupId)
    assertEquals(Empty, group.currentState)
    // Ensure that no offsets are materialized, but that we have offsets pending.
    assertEquals(0, group.allOffsets.size)
    assertTrue(group.hasOffsets)
    assertTrue(group.hasPendingOffsetCommitsFromProducer(producerId))
    assertTrue(group.hasPendingOffsetCommitsForTopicPartition(foo0))
    assertTrue(group.hasPendingOffsetCommitsForTopicPartition(foo1))
    assertTrue(group.hasPendingOffsetCommitsForTopicPartition(bar0))
  }

  @Test
  def testLoadWithCommittedAndAbortedTransactionalOffsetCommits(): Unit = {
    // A test which loads a log with a mix of committed and aborted transactional offset committed messages.
    val groupMetadataTopicPartition = groupTopicPartition
    val producerId = 1000L
    val producerEpoch: Short = 2
    val groupEpoch = 2

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

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, groupEpoch, _ => (), 0L)

    val group = groupMetadataManager.getGroup(groupId).getOrElse(throw new AssertionError("Group was not loaded into the cache"))
    assertEquals(groupId, group.groupId)
    assertEquals(Empty, group.currentState)
    // Ensure that only the committed offsets are materialized, and that there are no pending commits for the producer.
    // This allows us to be certain that the aborted offset commits are truly discarded.
    assertEquals(committedOffsets.size, group.allOffsets.size)
    committedOffsets.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.committedOffset))
    }
    assertFalse(group.hasPendingOffsetCommitsFromProducer(producerId))
  }

  @Test
  def testLoadWithCommittedAndAbortedAndPendingTransactionalOffsetCommits(): Unit = {
    val groupMetadataTopicPartition = groupTopicPartition
    val producerId = 1000L
    val producerEpoch: Short = 2
    val groupEpoch = 2

    val committedOffsets = Map(
      new TopicPartition("foo", 0) -> 23L,
      new TopicPartition("foo", 1) -> 455L,
      new TopicPartition("bar", 0) -> 8992L
    )

    val foo3 = new TopicPartition("foo", 3)

    val abortedOffsets = Map(
      new TopicPartition("foo", 2) -> 231L,
      foo3 -> 4551L,
      new TopicPartition("bar", 1) -> 89921L
    )

    val pendingOffsets = Map(
      foo3 -> 2312L,
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

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, groupEpoch, _ => (), 0L)

    val group = groupMetadataManager.getGroup(groupId).getOrElse(throw new AssertionError("Group was not loaded into the cache"))
    assertEquals(groupId, group.groupId)
    assertEquals(Empty, group.currentState)

    // Ensure that only the committed offsets are materialized, and that there are no pending commits for the producer.
    // This allows us to be certain that the aborted offset commits are truly discarded.
    assertEquals(committedOffsets.size, group.allOffsets.size)
    committedOffsets.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.committedOffset))
      assertEquals(Some(commitOffsetsLogPosition), group.offsetWithRecordMetadata(topicPartition).head.appendedBatchOffset)
    }

    // We should have pending commits.
    assertTrue(group.hasPendingOffsetCommitsFromProducer(producerId))
    assertTrue(group.hasPendingOffsetCommitsForTopicPartition(foo3))

    // The loaded pending commits should materialize after a commit marker comes in.
    groupMetadataManager.handleTxnCompletion(producerId, List(groupMetadataTopicPartition.partition).toSet, isCommit = true)
    assertFalse(group.hasPendingOffsetCommitsFromProducer(producerId))
    pendingOffsets.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.committedOffset))
    }
  }

  @Test
  def testLoadTransactionalOffsetCommitsFromMultipleProducers(): Unit = {
    val groupMetadataTopicPartition = groupTopicPartition
    val firstProducerId = 1000L
    val firstProducerEpoch: Short = 2
    val secondProducerId = 1001L
    val secondProducerEpoch: Short = 3
    val groupEpoch = 2

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

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, groupEpoch, _ => (), 0L)

    val group = groupMetadataManager.getGroup(groupId).getOrElse(throw new AssertionError("Group was not loaded into the cache"))
    assertEquals(groupId, group.groupId)
    assertEquals(Empty, group.currentState)

    // Ensure that only the committed offsets are materialized, and that there are no pending commits for the producer.
    // This allows us to be certain that the aborted offset commits are truly discarded.
    assertEquals(committedOffsetsFirstProducer.size + committedOffsetsSecondProducer.size, group.allOffsets.size)
    committedOffsetsFirstProducer.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.committedOffset))
      assertEquals(Some(firstProduceRecordOffset), group.offsetWithRecordMetadata(topicPartition).head.appendedBatchOffset)
    }
    committedOffsetsSecondProducer.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.committedOffset))
      assertEquals(Some(secondProducerRecordOffset), group.offsetWithRecordMetadata(topicPartition).head.appendedBatchOffset)
    }
  }

  @Test
  def testGroupLoadWithConsumerAndTransactionalOffsetCommitsConsumerWins(): Unit = {
    val groupMetadataTopicPartition = groupTopicPartition
    val producerId = 1000L
    val producerEpoch: Short = 2
    val groupEpoch = 2

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

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, groupEpoch, _ => (), 0L)

    // The group should be loaded with pending offsets.
    val group = groupMetadataManager.getGroup(groupId).getOrElse(throw new AssertionError("Group was not loaded into the cache"))
    assertEquals(groupId, group.groupId)
    assertEquals(Empty, group.currentState)
    assertEquals(1, group.allOffsets.size)
    assertTrue(group.hasOffsets)
    assertFalse(group.hasPendingOffsetCommitsFromProducer(producerId))
    assertEquals(consumerOffsetCommits.size, group.allOffsets.size)
    consumerOffsetCommits.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.committedOffset))
      assertEquals(Some(consumerRecordOffset), group.offsetWithRecordMetadata(topicPartition).head.appendedBatchOffset)
    }
  }

  @Test
  def testGroupLoadWithConsumerAndTransactionalOffsetCommitsTransactionWins(): Unit = {
    val groupMetadataTopicPartition = groupTopicPartition
    val producerId = 1000L
    val producerEpoch: Short = 2
    val groupEpoch = 2

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

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, groupEpoch, _ => (), 0L)

    // The group should be loaded with pending offsets.
    val group = groupMetadataManager.getGroup(groupId).getOrElse(throw new AssertionError("Group was not loaded into the cache"))
    assertEquals(groupId, group.groupId)
    assertEquals(Empty, group.currentState)
    assertEquals(1, group.allOffsets.size)
    assertTrue(group.hasOffsets)
    assertFalse(group.hasPendingOffsetCommitsFromProducer(producerId))
    assertEquals(consumerOffsetCommits.size, group.allOffsets.size)
    transactionalOffsetCommits.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.committedOffset))
    }
  }

  @Test
  def testGroupNotExists(): Unit = {
    // group is not owned
    assertFalse(groupMetadataManager.groupNotExists(groupId))

    groupMetadataManager.addOwnedPartition(groupPartitionId)
    // group is owned but does not exist yet
    assertTrue(groupMetadataManager.groupNotExists(groupId))

    val group = new GroupMetadata(groupId, Empty, time)
    groupMetadataManager.addGroup(group)

    // group is owned but not Dead
    assertFalse(groupMetadataManager.groupNotExists(groupId))

    group.transitionTo(Dead)
    // group is owned and Dead
    assertTrue(groupMetadataManager.groupNotExists(groupId))
  }

  private def appendConsumerOffsetCommit(buffer: ByteBuffer, baseOffset: Long, offsets: Map[TopicPartition, Long]) = {
    val builder = MemoryRecords.builder(buffer, Compression.NONE, TimestampType.LOG_APPEND_TIME, baseOffset)
    val commitRecords = createCommittedOffsetRecords(offsets)
    commitRecords.foreach(builder.append)
    builder.build()
    offsets.size
  }

  private def appendTransactionalOffsetCommits(buffer: ByteBuffer, producerId: Long, producerEpoch: Short,
                                               baseOffset: Long, offsets: Map[TopicPartition, Long]): Int = {
    val builder = MemoryRecords.builder(buffer, Compression.NONE, baseOffset, producerId, producerEpoch, 0, true)
    val commitRecords = createCommittedOffsetRecords(offsets)
    commitRecords.foreach(builder.append)
    builder.build()
    offsets.size
  }

  private def completeTransactionalOffsetCommit(buffer: ByteBuffer, producerId: Long, producerEpoch: Short, baseOffset: Long,
                                                isCommit: Boolean): Int = {
    val builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V2, Compression.NONE,
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
    val groupEpoch = 2

    val tombstonePartition = new TopicPartition("foo", 1)
    val committedOffsets = Map(
      new TopicPartition("foo", 0) -> 23L,
      tombstonePartition -> 455L,
      new TopicPartition("bar", 0) -> 8992L
    )

    val offsetCommitRecords = createCommittedOffsetRecords(committedOffsets)
    val tombstone = new SimpleRecord(GroupMetadataManager.offsetCommitKey(groupId, tombstonePartition), null)
    val records = MemoryRecords.withRecords(startOffset, Compression.NONE,
      (offsetCommitRecords ++ Seq(tombstone)).toArray: _*)

    expectGroupMetadataLoad(groupMetadataTopicPartition, startOffset, records)

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, groupEpoch, _ => (), 0L)

    val group = groupMetadataManager.getGroup(groupId).getOrElse(throw new AssertionError("Group was not loaded into the cache"))
    assertEquals(groupId, group.groupId)
    assertEquals(Empty, group.currentState)
    assertEquals(committedOffsets.size - 1, group.allOffsets.size)
    committedOffsets.foreach { case (topicPartition, offset) =>
      if (topicPartition == tombstonePartition)
        assertEquals(None, group.offset(topicPartition))
      else
        assertEquals(Some(offset), group.offset(topicPartition).map(_.committedOffset))
    }
  }

  @Test
  def testLoadOffsetsAndGroup(): Unit = {
    loadOffsetsAndGroup(groupTopicPartition, 2)
  }

  def loadOffsetsAndGroup(groupMetadataTopicPartition: TopicPartition, groupEpoch: Int): GroupMetadata = {
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

    val records = MemoryRecords.withRecords(startOffset, Compression.NONE,
      (offsetCommitRecords ++ Seq(groupMetadataRecord)).toArray: _*)

    expectGroupMetadataLoad(groupMetadataTopicPartition, startOffset, records)

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, groupEpoch, _ => (), 0L)

    val group = groupMetadataManager.getGroup(groupId).getOrElse(throw new AssertionError("Group was not loaded into the cache"))
    assertEquals(groupId, group.groupId)
    assertEquals(Stable, group.currentState)
    assertEquals(memberId, group.leaderOrNull)
    assertEquals(generation, group.generationId)
    assertEquals(Some(protocolType), group.protocolType)
    assertEquals(protocol, group.protocolName.orNull)
    assertEquals(Set(memberId), group.allMembers)
    assertEquals(committedOffsets.size, group.allOffsets.size)
    committedOffsets.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.committedOffset))
      assertTrue(group.offset(topicPartition).map(_.expireTimestampMs).get.isEmpty)
    }
    group
  }

  @Test
  def testLoadOffsetsAndGroupIgnored(): Unit = {
    val groupEpoch = 2
    loadOffsetsAndGroup(groupTopicPartition, groupEpoch)
    assertEquals(groupEpoch, groupMetadataManager.epochForPartitionId.get(groupTopicPartition.partition()))

    groupMetadataManager.removeGroupsAndOffsets(groupTopicPartition, OptionalInt.of(groupEpoch), _ => ())
    assertTrue(groupMetadataManager.getGroup(groupId).isEmpty,
      "Removed group remained in cache")
    assertEquals(groupEpoch, groupMetadataManager.epochForPartitionId.get(groupTopicPartition.partition()))

    groupMetadataManager.loadGroupsAndOffsets(groupTopicPartition, groupEpoch - 1, _ => (), 0L)
    assertTrue(groupMetadataManager.getGroup(groupId).isEmpty,
      "Removed group remained in cache")
    assertEquals(groupEpoch, groupMetadataManager.epochForPartitionId.get(groupTopicPartition.partition()))
  }

  @Test
  def testUnloadOffsetsAndGroup(): Unit = {
    val groupEpoch = 2
    loadOffsetsAndGroup(groupTopicPartition, groupEpoch)

    groupMetadataManager.removeGroupsAndOffsets(groupTopicPartition, OptionalInt.of(groupEpoch), _ => ())
    assertEquals(groupEpoch, groupMetadataManager.epochForPartitionId.get(groupTopicPartition.partition()))
    assertTrue(groupMetadataManager.getGroup(groupId).isEmpty,
    "Removed group remained in cache")
  }

  @Test
  def testUnloadOffsetsAndGroupIgnored(): Unit = {
    val groupEpoch = 2
    val initiallyLoaded = loadOffsetsAndGroup(groupTopicPartition, groupEpoch)

    groupMetadataManager.removeGroupsAndOffsets(groupTopicPartition, OptionalInt.of(groupEpoch - 1), _ => ())
    assertEquals(groupEpoch, groupMetadataManager.epochForPartitionId.get(groupTopicPartition.partition()))
    val group = groupMetadataManager.getGroup(groupId).getOrElse(throw new AssertionError("Group was not loaded into the cache"))
    assertEquals(initiallyLoaded.groupId, group.groupId)
    assertEquals(initiallyLoaded.currentState, group.currentState)
    assertEquals(initiallyLoaded.leaderOrNull, group.leaderOrNull)
    assertEquals(initiallyLoaded.generationId, group.generationId)
    assertEquals(initiallyLoaded.protocolType, group.protocolType)
    assertEquals(initiallyLoaded.protocolName.orNull, group.protocolName.orNull)
    assertEquals(initiallyLoaded.allMembers, group.allMembers)
    assertEquals(initiallyLoaded.allOffsets.size, group.allOffsets.size)
    initiallyLoaded.allOffsets.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition))
      assertTrue(group.offset(topicPartition).map(_.expireTimestampMs).get.isEmpty)
    }
  }

  @Test
  def testUnloadOffsetsAndGroupIgnoredAfterStopReplica(): Unit = {
    val groupEpoch = 2
    val initiallyLoaded = loadOffsetsAndGroup(groupTopicPartition, groupEpoch)

    groupMetadataManager.removeGroupsAndOffsets(groupTopicPartition, OptionalInt.empty, _ => ())
    assertTrue(groupMetadataManager.getGroup(groupId).isEmpty,
      "Removed group remained in cache")
    assertEquals(groupEpoch, groupMetadataManager.epochForPartitionId.get(groupTopicPartition.partition()),
    "Replica which was stopped still in epochForPartitionId")

    loadOffsetsAndGroup(groupTopicPartition, groupEpoch + 1)
    val group = groupMetadataManager.getGroup(groupId).getOrElse(throw new AssertionError("Group was not loaded into the cache"))
    assertEquals(initiallyLoaded.groupId, group.groupId)
    assertEquals(initiallyLoaded.currentState, group.currentState)
    assertEquals(initiallyLoaded.leaderOrNull, group.leaderOrNull)
    assertEquals(initiallyLoaded.generationId, group.generationId)
    assertEquals(initiallyLoaded.protocolType, group.protocolType)
    assertEquals(initiallyLoaded.protocolName.orNull, group.protocolName.orNull)
    assertEquals(initiallyLoaded.allMembers, group.allMembers)
    assertEquals(initiallyLoaded.allOffsets.size, group.allOffsets.size)
    initiallyLoaded.allOffsets.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition))
      assertTrue(group.offset(topicPartition).map(_.expireTimestampMs).get.isEmpty)
    }
  }

  @Test
  def testLoadGroupWithTombstone(): Unit = {
    val groupMetadataTopicPartition = groupTopicPartition
    val startOffset = 15L
    val groupEpoch = 2
    val memberId = "98098230493"
    val groupMetadataRecord = buildStableGroupRecordWithMember(generation = 15,
      protocolType = "consumer", protocol = "range", memberId)
    val groupMetadataTombstone = new SimpleRecord(GroupMetadataManager.groupMetadataKey(groupId), null)
    val records = MemoryRecords.withRecords(startOffset, Compression.NONE,
      Seq(groupMetadataRecord, groupMetadataTombstone).toArray: _*)

    expectGroupMetadataLoad(groupMetadataTopicPartition, startOffset, records)

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, groupEpoch, _ => (), 0L)

    assertEquals(None, groupMetadataManager.getGroup(groupId))
  }

  @Test
  def testLoadGroupWithLargeGroupMetadataRecord(): Unit = {
    val groupMetadataTopicPartition = groupTopicPartition
    val startOffset = 15L
    val groupEpoch = 2
    val committedOffsets = Map(
      new TopicPartition("foo", 0) -> 23L,
      new TopicPartition("foo", 1) -> 455L,
      new TopicPartition("bar", 0) -> 8992L
    )

    // create a GroupMetadata record larger then offsets.load.buffer.size (here at least 16 bytes larger)
    val assignmentSize = GroupCoordinatorConfig.OFFSETS_LOAD_BUFFER_SIZE_DEFAULT + 16
    val memberId = "98098230493"

    val offsetCommitRecords = createCommittedOffsetRecords(committedOffsets)
    val groupMetadataRecord = buildStableGroupRecordWithMember(generation = 15,
      protocolType = "consumer", protocol = "range", memberId, new Array[Byte](assignmentSize))
    val records = MemoryRecords.withRecords(startOffset, Compression.NONE,
      (offsetCommitRecords ++ Seq(groupMetadataRecord)).toArray: _*)

    expectGroupMetadataLoad(groupMetadataTopicPartition, startOffset, records)

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, groupEpoch, _ => (), 0L)

    val group = groupMetadataManager.getGroup(groupId).getOrElse(throw new AssertionError("Group was not loaded into the cache"))
    committedOffsets.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.committedOffset))
    }
  }

  @Test
  def testLoadGroupAndOffsetsWithCorruptedLog(): Unit = {
    // Simulate a case where startOffset < endOffset but log is empty. This could theoretically happen
    // when all the records are expired and the active segment is truncated or when the partition
    // is accidentally corrupted.
    val startOffset = 0L
    val endOffset = 10L
    val groupEpoch = 2

    val logMock: UnifiedLog = mock(classOf[UnifiedLog])
    when(replicaManager.getLog(groupTopicPartition)).thenReturn(Some(logMock))
    expectGroupMetadataLoad(logMock, startOffset, MemoryRecords.EMPTY)
    when(replicaManager.getLogEndOffset(groupTopicPartition)).thenReturn(Some(endOffset))
    groupMetadataManager.loadGroupsAndOffsets(groupTopicPartition, groupEpoch, _ => (), 0L)

    verify(logMock).logStartOffset
    verify(logMock).read(ArgumentMatchers.eq(startOffset),
      maxLength = anyInt(),
      isolation = ArgumentMatchers.eq(FetchIsolation.LOG_END),
      minOneMessage = ArgumentMatchers.eq(true))
    verify(replicaManager).getLog(groupTopicPartition)
    verify(replicaManager, times(2)).getLogEndOffset(groupTopicPartition)

    assertFalse(groupMetadataManager.isPartitionLoading(groupTopicPartition.partition()))
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
    val groupEpoch = 2

    val committedOffsets = Map(
      new TopicPartition("foo", 0) -> 23L,
      new TopicPartition("foo", 1) -> 455L,
      new TopicPartition("bar", 0) -> 8992L
    )
    val offsetCommitRecords = createCommittedOffsetRecords(committedOffsets)
    val memberId = "98098230493"
    val groupMetadataRecord = buildStableGroupRecordWithMember(generation, protocolType, protocol, memberId)
    val groupMetadataTombstone = new SimpleRecord(GroupMetadataManager.groupMetadataKey(groupId), null)
    val records = MemoryRecords.withRecords(startOffset, Compression.NONE,
      (Seq(groupMetadataRecord, groupMetadataTombstone) ++ offsetCommitRecords).toArray: _*)

    expectGroupMetadataLoad(groupMetadataTopicPartition, startOffset, records)

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, groupEpoch, _ => (), 0L)

    val group = groupMetadataManager.getGroup(groupId).getOrElse(throw new AssertionError("Group was not loaded into the cache"))
    assertEquals(groupId, group.groupId)
    assertEquals(Empty, group.currentState)
    assertEquals(committedOffsets.size, group.allOffsets.size)
    committedOffsets.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.committedOffset))
    }
  }

  @Test
  def testLoadGroupAndOffsetsFromDifferentSegments(): Unit = {
    val generation = 293
    val protocolType = "consumer"
    val protocol = "range"
    val startOffset = 15L
    val groupEpoch = 2
    val tp0 = new TopicPartition("foo", 0)
    val tp1 = new TopicPartition("foo", 1)
    val tp2 = new TopicPartition("bar", 0)
    val tp3 = new TopicPartition("xxx", 0)

    val fileRecordsMock: FileRecords = mock(classOf[FileRecords])
    val logMock: UnifiedLog = mock(classOf[UnifiedLog])
    when(replicaManager.getLog(groupTopicPartition)).thenReturn(Some(logMock))

    val segment1MemberId = "a"
    val segment1Offsets = Map(tp0 -> 23L, tp1 -> 455L, tp3 -> 42L)
    val segment1Records = MemoryRecords.withRecords(startOffset, Compression.NONE,
      (createCommittedOffsetRecords(segment1Offsets) ++ Seq(buildStableGroupRecordWithMember(
        generation, protocolType, protocol, segment1MemberId))).toArray: _*)
    val segment1End = startOffset + segment1Records.records.asScala.size

    val segment2MemberId = "b"
    val segment2Offsets = Map(tp0 -> 33L, tp2 -> 8992L, tp3 -> 10L)
    val segment2Records = MemoryRecords.withRecords(segment1End, Compression.NONE,
      (createCommittedOffsetRecords(segment2Offsets) ++ Seq(buildStableGroupRecordWithMember(
        generation, protocolType, protocol, segment2MemberId))).toArray: _*)
    val segment2End = segment1End + segment2Records.records.asScala.size

    when(logMock.logStartOffset)
      .thenReturn(segment1End)
      .thenReturn(segment2End)
    when(logMock.read(ArgumentMatchers.eq(segment1End),
      maxLength = anyInt(),
      isolation = ArgumentMatchers.eq(FetchIsolation.LOG_END),
      minOneMessage = ArgumentMatchers.eq(true)))
      .thenReturn(new FetchDataInfo(new LogOffsetMetadata(segment1End), fileRecordsMock))
    when(logMock.read(ArgumentMatchers.eq(segment2End),
      maxLength = anyInt(),
      isolation = ArgumentMatchers.eq(FetchIsolation.LOG_END),
      minOneMessage = ArgumentMatchers.eq(true)))
      .thenReturn(new FetchDataInfo(new LogOffsetMetadata(segment2End), fileRecordsMock))
    when(fileRecordsMock.sizeInBytes())
      .thenReturn(segment1Records.sizeInBytes)
      .thenReturn(segment2Records.sizeInBytes)

    val bufferCapture: ArgumentCaptor[ByteBuffer] = ArgumentCaptor.forClass(classOf[ByteBuffer])
    when(fileRecordsMock.readInto(bufferCapture.capture(), anyInt()))
      .thenAnswer(_ => {
      val buffer = bufferCapture.getValue
      buffer.put(segment1Records.buffer.duplicate)
      buffer.flip()
    }).thenAnswer(_ => {
      val buffer = bufferCapture.getValue
      buffer.put(segment2Records.buffer.duplicate)
      buffer.flip()
    })

    when(replicaManager.getLogEndOffset(groupTopicPartition)).thenReturn(Some(segment2End))

    groupMetadataManager.loadGroupsAndOffsets(groupTopicPartition, groupEpoch, _ => (), 0L)

    val group = groupMetadataManager.getGroup(groupId).getOrElse(throw new AssertionError("Group was not loaded into the cache"))
    assertEquals(groupId, group.groupId)
    assertEquals(Stable, group.currentState)

    assertEquals(segment2MemberId, group.leaderOrNull, "segment2 group record member should be elected")
    assertEquals(Set(segment2MemberId), group.allMembers, "segment2 group record member should be only member")

    // offsets of segment1 should be overridden by segment2 offsets of the same topic partitions
    val committedOffsets = segment1Offsets ++ segment2Offsets
    assertEquals(committedOffsets.size, group.allOffsets.size)
    committedOffsets.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.committedOffset))
    }
  }

  @Test
  def testAddGroup(): Unit = {
    val group = new GroupMetadata("foo", Empty, time)
    assertEquals(group, groupMetadataManager.addGroup(group))
    assertEquals(group, groupMetadataManager.addGroup(new GroupMetadata("foo", Empty, time)))
  }

  @Test
  def testloadGroupWithStaticMember(): Unit = {
    val generation = 27
    val protocolType = "consumer"
    val staticMemberId = "staticMemberId"
    val dynamicMemberId = "dynamicMemberId"

    val staticMember = new MemberMetadata(staticMemberId, Some(groupInstanceId), "", "", rebalanceTimeout, sessionTimeout,
      protocolType, List(("protocol", Array[Byte]())))

    val dynamicMember = new MemberMetadata(dynamicMemberId, None, "", "", rebalanceTimeout, sessionTimeout,
      protocolType, List(("protocol", Array[Byte]())))

    val members = Seq(staticMember, dynamicMember)

    val group = GroupMetadata.loadGroup(groupId, Empty, generation, protocolType, null, null, None, members, time)

    assertTrue(group.is(Empty))
    assertEquals(generation, group.generationId)
    assertEquals(Some(protocolType), group.protocolType)
    assertTrue(group.has(staticMemberId))
    assertTrue(group.has(dynamicMemberId))
    assertTrue(group.hasStaticMember(groupInstanceId))
    assertEquals(Some(staticMemberId), group.currentStaticMemberId(groupInstanceId))
  }

  @Test
  def testLoadConsumerGroup(): Unit = {
    val generation = 27
    val protocolType = "consumer"
    val protocol = "protocol"
    val memberId = "member1"
    val topic = "foo"

    val subscriptions = List(
      ("protocol", ConsumerProtocol.serializeSubscription(new Subscription(List(topic).asJava)).array())
    )

    val member = new MemberMetadata(memberId, Some(groupInstanceId), "", "", rebalanceTimeout,
      sessionTimeout, protocolType, subscriptions)

    val members = Seq(member)

    val group = GroupMetadata.loadGroup(groupId, Stable, generation, protocolType, protocol, null, None,
      members, time)

    assertTrue(group.is(Stable))
    assertEquals(generation, group.generationId)
    assertEquals(Some(protocolType), group.protocolType)
    assertEquals(protocol, group.protocolName.orNull)
    assertEquals(Some(Set(topic)), group.getSubscribedTopics)
    assertTrue(group.has(memberId))
  }

  @Test
  def testLoadEmptyConsumerGroup(): Unit = {
    val generation = 27
    val protocolType = "consumer"

    val group = GroupMetadata.loadGroup(groupId, Empty, generation, protocolType, null, null, None,
      Seq(), time)

    assertTrue(group.is(Empty))
    assertEquals(generation, group.generationId)
    assertEquals(Some(protocolType), group.protocolType)
    assertNull(group.protocolName.orNull)
    assertEquals(Some(Set.empty), group.getSubscribedTopics)
  }

  @Test
  def testLoadConsumerGroupWithFaultyConsumerProtocol(): Unit = {
    val generation = 27
    val protocolType = "consumer"
    val protocol = "protocol"
    val memberId = "member1"

    val subscriptions = List(("protocol", Array[Byte]()))

    val member = new MemberMetadata(memberId, Some(groupInstanceId), "", "", rebalanceTimeout,
      sessionTimeout, protocolType, subscriptions)

    val members = Seq(member)

    val group = GroupMetadata.loadGroup(groupId, Stable, generation, protocolType, protocol, null, None,
      members, time)

    assertTrue(group.is(Stable))
    assertEquals(generation, group.generationId)
    assertEquals(Some(protocolType), group.protocolType)
    assertEquals(protocol, group.protocolName.orNull)
    assertEquals(None, group.getSubscribedTopics)
    assertTrue(group.has(memberId))
  }

  @Test
  def testShouldThrowExceptionForUnsupportedGroupMetadataVersion(): Unit = {
    val generation = 1
    val protocol = "range"
    val memberId = "memberId"
    val unsupportedVersion = Short.MinValue

    // put the unsupported version as the version value
    val groupMetadataRecordValue = buildStableGroupRecordWithMember(generation, protocolType, protocol, memberId)
      .value().putShort(unsupportedVersion)
    // reset the position to the starting position 0 so that it can read the data in correct order
    groupMetadataRecordValue.position(0)

    val e = assertThrows(classOf[IllegalStateException],
      () => GroupMetadataManager.readGroupMessageValue(groupId, groupMetadataRecordValue, time))
    assertEquals(s"Unknown group metadata message version: $unsupportedVersion", e.getMessage)
  }

  @Test
  def testCurrentStateTimestampForAllGroupMetadataVersions(): Unit = {
    val generation = 1
    val protocol = "range"
    val memberId = "memberId"

    for (metadataVersion <- MetadataVersion.VERSIONS) {
      val groupMetadataRecord = buildStableGroupRecordWithMember(generation, protocolType, protocol, memberId, metadataVersion = metadataVersion)

      val deserializedGroupMetadata = GroupMetadataManager.readGroupMessageValue(groupId, groupMetadataRecord.value(), time)
      // GROUP_METADATA_VALUE_SCHEMA_V2 or higher should correctly set the currentStateTimestamp
      if (metadataVersion.isAtLeast(IBP_2_1_IV0))
        assertEquals(Some(time.milliseconds()), deserializedGroupMetadata.currentStateTimestamp,
          s"the metadataVersion $metadataVersion doesn't set the currentStateTimestamp correctly.")
      else
        assertTrue(deserializedGroupMetadata.currentStateTimestamp.isEmpty,
          s"the metadataVersion $metadataVersion should not set the currentStateTimestamp.")
    }
  }

  @Test
  def testReadFromOldGroupMetadata(): Unit = {
    val generation = 1
    val protocol = "range"
    val memberId = "memberId"
    val oldMetadataVersions = Array(IBP_0_9_0, IBP_0_10_1_IV0, IBP_2_1_IV0)

    for (metadataVersion <- oldMetadataVersions) {
      val groupMetadataRecord = buildStableGroupRecordWithMember(generation, protocolType, protocol, memberId, metadataVersion = metadataVersion)

      val deserializedGroupMetadata = GroupMetadataManager.readGroupMessageValue(groupId, groupMetadataRecord.value(), time)
      assertEquals(groupId, deserializedGroupMetadata.groupId)
      assertEquals(generation, deserializedGroupMetadata.generationId)
      assertEquals(protocolType, deserializedGroupMetadata.protocolType.get)
      assertEquals(protocol, deserializedGroupMetadata.protocolName.orNull)
      assertEquals(1, deserializedGroupMetadata.allMembers.size)
      assertEquals(deserializedGroupMetadata.allMembers, deserializedGroupMetadata.allDynamicMembers)
      assertTrue(deserializedGroupMetadata.allMembers.contains(memberId))
      assertTrue(deserializedGroupMetadata.allStaticMembers.isEmpty)
    }
  }

  @Test
  def testStoreEmptyGroup(): Unit = {
    val generation = 27
    val protocolType = "consumer"

    val group = GroupMetadata.loadGroup(groupId, Empty, generation, protocolType, null, null, None, Seq.empty, time)
    groupMetadataManager.addGroup(group)

    val capturedRecords = expectAppendMessage(Errors.NONE)
    var maybeError: Option[Errors] = None
    def callback(error: Errors): Unit = {
      maybeError = Some(error)
    }

    groupMetadataManager.storeGroup(group, Map.empty, callback)
    assertEquals(Some(Errors.NONE), maybeError)
    val records = capturedRecords.getValue()(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupPartitionId))
        .records.asScala.toList
    assertEquals(1, records.size)

    val record = records.head
    val groupMetadata = GroupMetadataManager.readGroupMessageValue(groupId, record.value, time)
    assertTrue(groupMetadata.is(Empty))
    assertEquals(generation, groupMetadata.generationId)
    assertEquals(Some(protocolType), groupMetadata.protocolType)
  }

  @Test
  def testStoreEmptySimpleGroup(): Unit = {
    val group = new GroupMetadata(groupId, Empty, time)
    groupMetadataManager.addGroup(group)

    val capturedRecords = expectAppendMessage(Errors.NONE)
    var maybeError: Option[Errors] = None
    def callback(error: Errors): Unit = {
      maybeError = Some(error)
    }

    groupMetadataManager.storeGroup(group, Map.empty, callback)
    assertEquals(Some(Errors.NONE), maybeError)
    val records = capturedRecords.getValue()(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupPartitionId))
      .records.asScala.toList
    assertEquals(1, records.size)

    val record = records.head
    val groupMetadata = GroupMetadataManager.readGroupMessageValue(groupId, record.value, time)
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
    assertStoreGroupErrorMapping(Errors.NOT_LEADER_OR_FOLLOWER, Errors.NOT_COORDINATOR)
    assertStoreGroupErrorMapping(Errors.MESSAGE_TOO_LARGE, Errors.UNKNOWN_SERVER_ERROR)
    assertStoreGroupErrorMapping(Errors.RECORD_LIST_TOO_LARGE, Errors.UNKNOWN_SERVER_ERROR)
    assertStoreGroupErrorMapping(Errors.INVALID_FETCH_SIZE, Errors.UNKNOWN_SERVER_ERROR)
    assertStoreGroupErrorMapping(Errors.CORRUPT_MESSAGE, Errors.CORRUPT_MESSAGE)
  }

  private def assertStoreGroupErrorMapping(appendError: Errors, expectedError: Errors): Unit = {
    reset(replicaManager)
    val group = new GroupMetadata(groupId, Empty, time)
    groupMetadataManager.addGroup(group)

    expectAppendMessage(appendError)
    var maybeError: Option[Errors] = None
    def callback(error: Errors): Unit = {
      maybeError = Some(error)
    }

    groupMetadataManager.storeGroup(group, Map.empty, callback)
    assertEquals(Some(expectedError), maybeError)

    verify(replicaManager).appendRecords(anyLong(),
      anyShort(),
      any(),
      any(),
      any(),
      any(),
      any[Option[ReentrantLock]],
      any(),
      any(),
      any(),
      any())
    verify(replicaManager).onlinePartition(any())
  }

  @Test
  def testStoreNonEmptyGroup(): Unit = {
    val memberId = "memberId"
    val clientId = "clientId"
    val clientHost = "localhost"

    val group = new GroupMetadata(groupId, Empty, time)
    groupMetadataManager.addGroup(group)

    val member = new MemberMetadata(memberId, Some(groupInstanceId), clientId, clientHost, rebalanceTimeout, sessionTimeout,
      protocolType, List(("protocol", Array[Byte]())))
    group.add(member, _ => ())
    group.transitionTo(PreparingRebalance)
    group.initNextGeneration()

    expectAppendMessage(Errors.NONE)
    var maybeError: Option[Errors] = None
    def callback(error: Errors): Unit = {
      maybeError = Some(error)
    }

    groupMetadataManager.storeGroup(group, Map(memberId -> Array[Byte]()), callback)
    assertEquals(Some(Errors.NONE), maybeError)

    verify(replicaManager).appendRecords(anyLong(),
      anyShort(),
      any(),
      any(),
      any(),
      any(),
      any[Option[ReentrantLock]],
      any(),
      any(),
      any(),
      any())
    verify(replicaManager).onlinePartition(any())
  }

  @Test
  def testStoreNonEmptyGroupWhenCoordinatorHasMoved(): Unit = {
    when(replicaManager.onlinePartition(any())).thenReturn(None)
    val memberId = "memberId"
    val clientId = "clientId"
    val clientHost = "localhost"

    val group = new GroupMetadata(groupId, Empty, time)

    val member = new MemberMetadata(memberId, Some(groupInstanceId), clientId, clientHost, rebalanceTimeout, sessionTimeout,
      protocolType, List(("protocol", Array[Byte]())))
    group.add(member, _ => ())
    group.transitionTo(PreparingRebalance)
    group.initNextGeneration()

    var maybeError: Option[Errors] = None
    def callback(error: Errors): Unit = {
      maybeError = Some(error)
    }

    groupMetadataManager.storeGroup(group, Map(memberId -> Array[Byte]()), callback)
    assertEquals(Some(Errors.NOT_COORDINATOR), maybeError)

    verify(replicaManager).onlinePartition(any())
  }

  @Test
  def testCommitOffset(): Unit = {
    val memberId = ""
    val topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), 0, "foo")
    val offset = 37

    groupMetadataManager.addOwnedPartition(groupPartitionId)

    val group = new GroupMetadata(groupId, Empty, time)
    groupMetadataManager.addGroup(group)

    val offsetTopicPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupMetadataManager.partitionFor(group.groupId))
    val offsets = immutable.Map(topicIdPartition -> new OffsetAndMetadata(offset, noLeader, "", time.milliseconds(), noExpiration))

    expectAppendMessage(Errors.NONE)
    var commitErrors: Option[immutable.Map[TopicIdPartition, Errors]] = None
    def callback(errors: immutable.Map[TopicIdPartition, Errors]): Unit = {
      commitErrors = Some(errors)
    }

    assertEquals(0, TestUtils.totalMetricValue(metrics, "offset-commit-count"))
    groupMetadataManager.storeOffsets(group, memberId, offsetTopicPartition, offsets, callback, verificationGuard = None)
    assertTrue(group.hasOffsets)

    assertFalse(commitErrors.isEmpty)
    val maybeError = commitErrors.get.get(topicIdPartition)
    assertEquals(Some(Errors.NONE), maybeError)
    assertTrue(group.hasOffsets)

    val cachedOffsets = groupMetadataManager.getOffsets(groupId, defaultRequireStable, Some(Seq(topicIdPartition.topicPartition)))
    val maybePartitionResponse = cachedOffsets.get(topicIdPartition.topicPartition)
    assertFalse(maybePartitionResponse.isEmpty)

    val partitionResponse = maybePartitionResponse.get
    assertEquals(Errors.NONE, partitionResponse.error)
    assertEquals(offset, partitionResponse.offset)

    verify(replicaManager).appendRecords(anyLong(),
      anyShort(),
      any(),
      any(),
      any(),
      any(),
      any[Option[ReentrantLock]],
      any(),
      any(),
      any(),
      any())
    // Will update sensor after commit
    assertEquals(1, TestUtils.totalMetricValue(metrics, "offset-commit-count"))
  }

  @Test
  def testTransactionalCommitOffsetCommitted(): Unit = {
    val memberId = ""
    val topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), 0, "foo")
    val offset = 37
    val producerId = 232L
    val producerEpoch = 0.toShort

    groupMetadataManager.addOwnedPartition(groupPartitionId)

    val group = new GroupMetadata(groupId, Empty, time)
    groupMetadataManager.addGroup(group)

    val offsetTopicPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupMetadataManager.partitionFor(group.groupId))
    val offsetAndMetadata = new OffsetAndMetadata(offset, noLeader, "", time.milliseconds(), noExpiration)
    val offsets = immutable.Map(topicIdPartition -> offsetAndMetadata)

    val capturedResponseCallback: ArgumentCaptor[Map[TopicPartition, PartitionResponse] => Unit] = ArgumentCaptor.forClass(classOf[Map[TopicPartition, PartitionResponse] => Unit])
    when(replicaManager.onlinePartition(any())).thenReturn(Some(mock(classOf[Partition])))
    var commitErrors: Option[immutable.Map[TopicIdPartition, Errors]] = None
    def callback(errors: immutable.Map[TopicIdPartition, Errors]): Unit = {
      commitErrors = Some(errors)
    }
    val verificationGuard = new VerificationGuard()

    groupMetadataManager.storeOffsets(group, memberId, offsetTopicPartition, offsets, callback, producerId, producerEpoch, verificationGuard = Some(verificationGuard))
    assertTrue(group.hasOffsets)
    assertTrue(group.allOffsets.isEmpty)

    verify(replicaManager).appendRecords(anyLong(),
      anyShort(),
      any(),
      any(),
      any[Map[TopicPartition, MemoryRecords]],
      capturedResponseCallback.capture(),
      any[Option[ReentrantLock]],
      any(),
      any(),
      any(),
      ArgumentMatchers.eq(Map(offsetTopicPartition -> verificationGuard)))
    verify(replicaManager).onlinePartition(any())
    capturedResponseCallback.getValue.apply(Map(groupTopicPartition ->
      new PartitionResponse(Errors.NONE, 0L, RecordBatch.NO_TIMESTAMP, 0L)))

    assertTrue(group.hasOffsets)
    assertTrue(group.allOffsets.isEmpty)

    group.completePendingTxnOffsetCommit(producerId, isCommit = true)
    assertTrue(group.hasOffsets)
    assertFalse(group.allOffsets.isEmpty)
    assertEquals(Some(offsetAndMetadata), group.offset(topicIdPartition.topicPartition))
  }

  @Test
  def testTransactionalCommitOffsetAppendFailure(): Unit = {
    val memberId = ""
    val topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), 0, "foo")
    val offset = 37
    val producerId = 232L
    val producerEpoch = 0.toShort

    groupMetadataManager.addOwnedPartition(groupPartitionId)

    val group = new GroupMetadata(groupId, Empty, time)
    groupMetadataManager.addGroup(group)

    val offsetTopicPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupMetadataManager.partitionFor(group.groupId))
    val offsets = immutable.Map(topicIdPartition -> new OffsetAndMetadata(offset, noLeader, "", time.milliseconds(), noExpiration))

    when(replicaManager.onlinePartition(any())).thenReturn(Some(mock(classOf[Partition])))

    var commitErrors: Option[immutable.Map[TopicIdPartition, Errors]] = None
    def callback(errors: immutable.Map[TopicIdPartition, Errors]): Unit = {
      commitErrors = Some(errors)
    }
    val verificationGuard = new VerificationGuard()

    groupMetadataManager.storeOffsets(group, memberId, offsetTopicPartition, offsets, callback, producerId, producerEpoch, verificationGuard = Some(verificationGuard))
    assertTrue(group.hasOffsets)
    assertTrue(group.allOffsets.isEmpty)
    val capturedResponseCallback = verifyAppendAndCaptureCallback()
    capturedResponseCallback.getValue.apply(Map(groupTopicPartition ->
      new PartitionResponse(Errors.NOT_ENOUGH_REPLICAS, 0L, RecordBatch.NO_TIMESTAMP, 0L)))

    assertFalse(group.hasOffsets)
    assertTrue(group.allOffsets.isEmpty)

    group.completePendingTxnOffsetCommit(producerId, isCommit = false)
    assertFalse(group.hasOffsets)
    assertTrue(group.allOffsets.isEmpty)

    verify(replicaManager).appendRecords(anyLong(),
      anyShort(),
      any(),
      any(),
      any[Map[TopicPartition, MemoryRecords]],
      any(),
      any[Option[ReentrantLock]],
      any(),
      any(),
      any(),
      ArgumentMatchers.eq(Map(offsetTopicPartition -> verificationGuard)))
    verify(replicaManager).onlinePartition(any())
  }

  @Test
  def testTransactionalCommitOffsetAborted(): Unit = {
    val memberId = ""
    val topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), 0, "foo")
    val offset = 37
    val producerId = 232L
    val producerEpoch = 0.toShort

    groupMetadataManager.addOwnedPartition(groupPartitionId)

    val group = new GroupMetadata(groupId, Empty, time)
    groupMetadataManager.addGroup(group)

    val offsetTopicPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupMetadataManager.partitionFor(group.groupId))
    val offsets = immutable.Map(topicIdPartition -> new OffsetAndMetadata(offset, noLeader, "", time.milliseconds(), noExpiration))

    when(replicaManager.onlinePartition(any())).thenReturn(Some(mock(classOf[Partition])))

    var commitErrors: Option[immutable.Map[TopicIdPartition, Errors]] = None
    def callback(errors: immutable.Map[TopicIdPartition, Errors]): Unit = {
      commitErrors = Some(errors)
    }
    val verificationGuard = new VerificationGuard()

    groupMetadataManager.storeOffsets(group, memberId, offsetTopicPartition, offsets, callback, producerId, producerEpoch, verificationGuard = Some(verificationGuard))
    assertTrue(group.hasOffsets)
    assertTrue(group.allOffsets.isEmpty)
    val capturedResponseCallback = verifyAppendAndCaptureCallback()
    capturedResponseCallback.getValue.apply(Map(groupTopicPartition ->
      new PartitionResponse(Errors.NONE, 0L, RecordBatch.NO_TIMESTAMP, 0L)))

    assertTrue(group.hasOffsets)
    assertTrue(group.allOffsets.isEmpty)

    group.completePendingTxnOffsetCommit(producerId, isCommit = false)
    assertFalse(group.hasOffsets)
    assertTrue(group.allOffsets.isEmpty)

    verify(replicaManager).appendRecords(anyLong(),
      anyShort(),
      any(),
      any(),
      any[Map[TopicPartition, MemoryRecords]],
      any(),
      any[Option[ReentrantLock]],
      any(),
      any(),
      any(),
      ArgumentMatchers.eq(Map(offsetTopicPartition -> verificationGuard)))
    verify(replicaManager).onlinePartition(any())
  }

  @Test
  def testCommitOffsetWhenCoordinatorHasMoved(): Unit = {
    when(replicaManager.onlinePartition(any())).thenReturn(None)
    val memberId = ""
    val topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), 0, "foo")
    val offset = 37

    groupMetadataManager.addOwnedPartition(groupPartitionId)

    val group = new GroupMetadata(groupId, Empty, time)
    groupMetadataManager.addGroup(group)

    val offsetTopicPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupMetadataManager.partitionFor(group.groupId))
    val offsets = immutable.Map(topicIdPartition -> new OffsetAndMetadata(offset, noLeader, "", time.milliseconds(), noExpiration))

    var commitErrors: Option[immutable.Map[TopicIdPartition, Errors]] = None
    def callback(errors: immutable.Map[TopicIdPartition, Errors]): Unit = {
      commitErrors = Some(errors)
    }

    groupMetadataManager.storeOffsets(group, memberId, offsetTopicPartition, offsets, callback, verificationGuard = None)

    assertFalse(commitErrors.isEmpty)
    val maybeError = commitErrors.get.get(topicIdPartition)
    assertEquals(Some(Errors.NOT_COORDINATOR), maybeError)

    verify(replicaManager).onlinePartition(any())
  }

  @Test
  def testCommitOffsetFailure(): Unit = {
    assertCommitOffsetErrorMapping(Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.COORDINATOR_NOT_AVAILABLE)
    assertCommitOffsetErrorMapping(Errors.NOT_ENOUGH_REPLICAS, Errors.COORDINATOR_NOT_AVAILABLE)
    assertCommitOffsetErrorMapping(Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND, Errors.COORDINATOR_NOT_AVAILABLE)
    assertCommitOffsetErrorMapping(Errors.NOT_LEADER_OR_FOLLOWER, Errors.NOT_COORDINATOR)
    assertCommitOffsetErrorMapping(Errors.MESSAGE_TOO_LARGE, Errors.INVALID_COMMIT_OFFSET_SIZE)
    assertCommitOffsetErrorMapping(Errors.RECORD_LIST_TOO_LARGE, Errors.INVALID_COMMIT_OFFSET_SIZE)
    assertCommitOffsetErrorMapping(Errors.INVALID_FETCH_SIZE, Errors.INVALID_COMMIT_OFFSET_SIZE)
    assertCommitOffsetErrorMapping(Errors.CORRUPT_MESSAGE, Errors.CORRUPT_MESSAGE)
  }

  private def assertCommitOffsetErrorMapping(appendError: Errors, expectedError: Errors): Unit = {
    reset(replicaManager)
    val memberId = ""
    val topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), 0, "foo")
    val offset = 37

    groupMetadataManager.addOwnedPartition(groupPartitionId)

    val group = new GroupMetadata(groupId, Empty, time)
    groupMetadataManager.addGroup(group)

    val offsetTopicPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupMetadataManager.partitionFor(group.groupId))
    val offsets = immutable.Map(topicIdPartition -> new OffsetAndMetadata(offset, noLeader, "", time.milliseconds(), noExpiration))

    when(replicaManager.onlinePartition(any())).thenReturn(Some(mock(classOf[Partition])))

    var commitErrors: Option[immutable.Map[TopicIdPartition, Errors]] = None
    def callback(errors: immutable.Map[TopicIdPartition, Errors]): Unit = {
      commitErrors = Some(errors)
    }

    assertEquals(0, TestUtils.totalMetricValue(metrics, "offset-commit-count"))
    groupMetadataManager.storeOffsets(group, memberId, offsetTopicPartition, offsets, callback, verificationGuard = None)
    assertTrue(group.hasOffsets)
    val capturedResponseCallback = verifyAppendAndCaptureCallback()
    capturedResponseCallback.getValue.apply(Map(groupTopicPartition ->
      new PartitionResponse(appendError, 0L, RecordBatch.NO_TIMESTAMP, 0L)))

    assertFalse(commitErrors.isEmpty)
    val maybeError = commitErrors.get.get(topicIdPartition)
    assertEquals(Some(expectedError), maybeError)
    assertFalse(group.hasOffsets)

    val cachedOffsets = groupMetadataManager.getOffsets(
      groupId,
      defaultRequireStable,
      Some(Seq(topicIdPartition.topicPartition))
    )
    assertEquals(
      Some(OffsetFetchResponse.INVALID_OFFSET),
      cachedOffsets.get(topicIdPartition.topicPartition).map(_.offset)
    )

    verify(replicaManager).onlinePartition(any())
    // Will not update sensor if failed
    assertEquals(0, TestUtils.totalMetricValue(metrics, "offset-commit-count"))
  }

  @Test
  def testCommitOffsetPartialFailure(): Unit = {
    val memberId = ""
    val topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), 0, "foo")
    val topicIdPartitionFailed = new TopicIdPartition(Uuid.randomUuid(), 1, "foo")
    val offset = 37

    groupMetadataManager.addOwnedPartition(groupPartitionId)

    val group = new GroupMetadata(groupId, Empty, time)
    groupMetadataManager.addGroup(group)

    val offsetTopicPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupMetadataManager.partitionFor(group.groupId))
    val offsets = immutable.Map(
      topicIdPartition -> new OffsetAndMetadata(offset, noLeader, "", time.milliseconds(), noExpiration),
      // This will failed
      topicIdPartitionFailed -> new OffsetAndMetadata(offset, noLeader, "s" * (offsetConfig.maxMetadataSize + 1) , time.milliseconds(), noExpiration)
    )

    when(replicaManager.onlinePartition(any())).thenReturn(Some(mock(classOf[Partition])))

    var commitErrors: Option[immutable.Map[TopicIdPartition, Errors]] = None
    def callback(errors: immutable.Map[TopicIdPartition, Errors]): Unit = {
      commitErrors = Some(errors)
    }

    assertEquals(0, TestUtils.totalMetricValue(metrics, "offset-commit-count"))
    groupMetadataManager.storeOffsets(group, memberId, offsetTopicPartition, offsets, callback, verificationGuard = None)
    assertTrue(group.hasOffsets)
    val capturedResponseCallback = verifyAppendAndCaptureCallback()
    capturedResponseCallback.getValue.apply(Map(groupTopicPartition ->
      new PartitionResponse(Errors.NONE, 0L, RecordBatch.NO_TIMESTAMP, 0L)))

    assertFalse(commitErrors.isEmpty)
    assertEquals(Some(Errors.NONE), commitErrors.get.get(topicIdPartition))
    assertEquals(Some(Errors.OFFSET_METADATA_TOO_LARGE), commitErrors.get.get(topicIdPartitionFailed))
    assertTrue(group.hasOffsets)

    val cachedOffsets = groupMetadataManager.getOffsets(
      groupId,
      defaultRequireStable,
      Some(Seq(topicIdPartition.topicPartition, topicIdPartitionFailed.topicPartition))
    )
    assertEquals(
      Some(offset),
      cachedOffsets.get(topicIdPartition.topicPartition).map(_.offset)
    )
    assertEquals(
      Some(OffsetFetchResponse.INVALID_OFFSET),
      cachedOffsets.get(topicIdPartitionFailed.topicPartition).map(_.offset)
    )

    verify(replicaManager).appendRecords(anyLong(),
      anyShort(),
      any(),
      any(),
      any[Map[TopicPartition, MemoryRecords]],
      any(),
      any[Option[ReentrantLock]],
      any(),
      any(),
      any(),
      any())
    verify(replicaManager).onlinePartition(any())
    assertEquals(1, TestUtils.totalMetricValue(metrics, "offset-commit-count"))
  }

  @Test
  def testOffsetMetadataTooLarge(): Unit = {
    val memberId = ""
    val topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), 0, "foo")
    val offset = 37

    groupMetadataManager.addOwnedPartition(groupPartitionId)
    val group = new GroupMetadata(groupId, Empty, time)
    groupMetadataManager.addGroup(group)

    val offsetTopicPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupMetadataManager.partitionFor(group.groupId))
    val offsets = immutable.Map(
      topicIdPartition -> new OffsetAndMetadata(offset, noLeader, "s" * (offsetConfig.maxMetadataSize + 1) , time.milliseconds(), noExpiration)
    )

    var commitErrors: Option[immutable.Map[TopicIdPartition, Errors]] = None
    def callback(errors: immutable.Map[TopicIdPartition, Errors]): Unit = {
      commitErrors = Some(errors)
    }

    assertEquals(0, TestUtils.totalMetricValue(metrics, "offset-commit-count"))
    groupMetadataManager.storeOffsets(group, memberId, offsetTopicPartition, offsets, callback, verificationGuard = None)
    assertFalse(group.hasOffsets)

    assertFalse(commitErrors.isEmpty)
    val maybeError = commitErrors.get.get(topicIdPartition)
    assertEquals(Some(Errors.OFFSET_METADATA_TOO_LARGE), maybeError)
    assertFalse(group.hasOffsets)

    val cachedOffsets = groupMetadataManager.getOffsets(
      groupId,
      defaultRequireStable,
      Some(Seq(topicIdPartition.topicPartition))
    )
    assertEquals(
      Some(OffsetFetchResponse.INVALID_OFFSET),
      cachedOffsets.get(topicIdPartition.topicPartition).map(_.offset)
    )

    assertEquals(0, TestUtils.totalMetricValue(metrics, "offset-commit-count"))
  }

  @Test
  def testOffsetMetadataTooLargePartialFailure(): Unit = {
    val memberId = ""
    val topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), 0, "foo")
    val validTopicIdPartition = new TopicIdPartition(topicIdPartition.topicId, 1, "foo")
    val offset = 37
    val requireStable = true

    groupMetadataManager.addOwnedPartition(groupPartitionId)
    val group = new GroupMetadata(groupId, Empty, time)
    groupMetadataManager.addGroup(group)

    val offsetTopicPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupMetadataManager.partitionFor(group.groupId))
    val offsets = immutable.Map(
      topicIdPartition -> new OffsetAndMetadata(offset, noLeader, "s" * (offsetConfig.maxMetadataSize + 1) , time.milliseconds(), noExpiration),
      validTopicIdPartition -> new OffsetAndMetadata(offset, noLeader, "", time.milliseconds(), noExpiration)
    )

    expectAppendMessage(Errors.NONE)

    var commitErrors: Option[immutable.Map[TopicIdPartition, Errors]] = None
    def callback(errors: immutable.Map[TopicIdPartition, Errors]): Unit = {
      commitErrors = Some(errors)
    }

    assertEquals(0, TestUtils.totalMetricValue(metrics, "offset-commit-count"))
    groupMetadataManager.storeOffsets(group, memberId, offsetTopicPartition, offsets, callback, verificationGuard = None)
    assertTrue(group.hasOffsets)

    assertEquals(Some(Map(
      topicIdPartition -> Errors.OFFSET_METADATA_TOO_LARGE,
      validTopicIdPartition -> Errors.NONE)
    ), commitErrors)

    val cachedOffsets = groupMetadataManager.getOffsets(
      groupId,
      requireStable,
      Some(Seq(topicIdPartition.topicPartition, validTopicIdPartition.topicPartition))
    )

    assertEquals(
      Some(OffsetFetchResponse.INVALID_OFFSET),
      cachedOffsets.get(topicIdPartition.topicPartition).map(_.offset)
    )
    assertEquals(
      Some(Errors.NONE),
      cachedOffsets.get(topicIdPartition.topicPartition).map(_.error)
    )
    assertEquals(
      Some(offset),
      cachedOffsets.get(validTopicIdPartition.topicPartition).map(_.offset)
    )

    assertEquals(1, TestUtils.totalMetricValue(metrics, "offset-commit-count"))
  }

  @Test
  def testTransactionalCommitOffsetWithOffsetMetadataTooLargePartialFailure(): Unit = {
    val memberId = ""
    val foo0 = new TopicIdPartition(Uuid.randomUuid(), 0, "foo")
    val foo1 = new TopicIdPartition(Uuid.randomUuid(), 1, "foo")
    val producerId = 232L
    val producerEpoch = 0.toShort

    groupMetadataManager.addOwnedPartition(groupPartitionId)

    val group = new GroupMetadata(groupId, Empty, time)
    groupMetadataManager.addGroup(group)

    val offsetTopicPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupMetadataManager.partitionFor(group.groupId))
    val offsets = immutable.Map(
      foo0 -> new OffsetAndMetadata(37, noLeader, "", time.milliseconds(), noExpiration),
      foo1 -> new OffsetAndMetadata(38, noLeader, "s" * (offsetConfig.maxMetadataSize + 1), time.milliseconds(), noExpiration)
    )

    val capturedResponseCallback: ArgumentCaptor[Map[TopicPartition, PartitionResponse] => Unit] =
      ArgumentCaptor.forClass(classOf[Map[TopicPartition, PartitionResponse] => Unit])
    when(replicaManager.onlinePartition(any())).thenReturn(Some(mock(classOf[Partition])))
    var commitErrors: Option[immutable.Map[TopicIdPartition, Errors]] = None

    def callback(errors: immutable.Map[TopicIdPartition, Errors]): Unit = {
      commitErrors = Some(errors)
    }

    val verificationGuard = new VerificationGuard()

    groupMetadataManager.storeOffsets(
      group,
      memberId,
      offsetTopicPartition,
      offsets,
      callback,
      producerId,
      producerEpoch,
      verificationGuard = Some(verificationGuard)
    )
    assertTrue(group.hasOffsets)
    assertTrue(group.allOffsets.isEmpty)

    verify(replicaManager).appendRecords(anyLong(),
      anyShort(),
      any(),
      any(),
      any[Map[TopicPartition, MemoryRecords]],
      capturedResponseCallback.capture(),
      any[Option[ReentrantLock]],
      any(),
      any(),
      any(),
      ArgumentMatchers.eq(Map(offsetTopicPartition -> verificationGuard)))
    verify(replicaManager).onlinePartition(any())
    capturedResponseCallback.getValue.apply(Map(groupTopicPartition ->
      new PartitionResponse(Errors.NONE, 0L, RecordBatch.NO_TIMESTAMP, 0L)))

    assertEquals(Some(Map(
      foo0 -> Errors.NONE,
      foo1 -> Errors.OFFSET_METADATA_TOO_LARGE
    )), commitErrors)

    assertTrue(group.hasOffsets)
    assertTrue(group.allOffsets.isEmpty)

    group.completePendingTxnOffsetCommit(producerId, isCommit = true)
    assertTrue(group.hasOffsets)
    assertFalse(group.allOffsets.isEmpty)
    assertEquals(offsets.get(foo0), group.offset(foo0.topicPartition))
  }

  @Test
  def testExpireOffset(): Unit = {
    val memberId = ""
    val topicIdPartition1 = new TopicIdPartition(Uuid.randomUuid(), 0, "foo")
    val topicIdPartition2 = new TopicIdPartition(topicIdPartition1.topicId, 1, "foo")
    val offset = 37

    groupMetadataManager.addOwnedPartition(groupPartitionId)

    val group = new GroupMetadata(groupId, Empty, time)
    groupMetadataManager.addGroup(group)

    val offsetTopicPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupMetadataManager.partitionFor(group.groupId))
    // expire the offset after 1 millisecond
    val startMs = time.milliseconds
    val offsets = immutable.Map(
      topicIdPartition1 -> new OffsetAndMetadata(offset, noLeader, "", startMs, OptionalLong.of(startMs + 1)),
      topicIdPartition2 -> new OffsetAndMetadata(offset, noLeader, "", startMs, OptionalLong.of(startMs + 3)))

    mockGetPartition()
    expectAppendMessage(Errors.NONE)
    var commitErrors: Option[immutable.Map[TopicIdPartition, Errors]] = None
    def callback(errors: immutable.Map[TopicIdPartition, Errors]): Unit = {
      commitErrors = Some(errors)
    }

    groupMetadataManager.storeOffsets(group, memberId, offsetTopicPartition, offsets, callback, verificationGuard = None)
    assertTrue(group.hasOffsets)

    assertFalse(commitErrors.isEmpty)
    assertEquals(Some(Errors.NONE), commitErrors.get.get(topicIdPartition1))

    // expire only one of the offsets
    time.sleep(2)

    when(partition.appendRecordsToLeader(any[MemoryRecords],
      origin = ArgumentMatchers.eq(AppendOrigin.COORDINATOR), requiredAcks = anyInt(),
      any(), any())).thenReturn(LogAppendInfo.UNKNOWN_LOG_APPEND_INFO)
    groupMetadataManager.cleanupGroupMetadata()

    assertEquals(Some(group), groupMetadataManager.getGroup(groupId))
    assertEquals(None, group.offset(topicIdPartition1.topicPartition))
    assertEquals(Some(offset), group.offset(topicIdPartition2.topicPartition).map(_.committedOffset))

    val cachedOffsets = groupMetadataManager.getOffsets(
      groupId,
      defaultRequireStable,
      Some(Seq(topicIdPartition1.topicPartition, topicIdPartition2.topicPartition))
    )
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(topicIdPartition1.topicPartition).map(_.offset))
    assertEquals(Some(offset), cachedOffsets.get(topicIdPartition2.topicPartition).map(_.offset))

    verify(replicaManager).appendRecords(anyLong(),
      anyShort(),
      any(),
      any(),
      any(),
      any(),
      any[Option[ReentrantLock]],
      any(),
      any(),
      any(),
      any())
    verify(replicaManager, times(2)).onlinePartition(any())
  }

  @Test
  def testGroupMetadataRemoval(): Unit = {
    val topicPartition1 = new TopicPartition("foo", 0)
    val topicPartition2 = new TopicPartition("foo", 1)

    groupMetadataManager.addOwnedPartition(groupPartitionId)

    val group = new GroupMetadata(groupId, Empty, time)
    groupMetadataManager.addGroup(group)
    group.generationId = 5

    // expect the group metadata tombstone
    val recordsCapture: ArgumentCaptor[MemoryRecords] = ArgumentCaptor.forClass(classOf[MemoryRecords])

    when(replicaManager.onlinePartition(any())).thenReturn(Some(mock(classOf[Partition])))
    mockGetPartition()
    when(partition.appendRecordsToLeader(recordsCapture.capture(),
      origin = ArgumentMatchers.eq(AppendOrigin.COORDINATOR), requiredAcks = anyInt(),
      any(), any())).thenReturn(LogAppendInfo.UNKNOWN_LOG_APPEND_INFO)
    groupMetadataManager.cleanupGroupMetadata()

    val records = recordsCapture.getValue.records.asScala.toList
    recordsCapture.getValue.batches.forEach { batch =>
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
    val cachedOffsets = groupMetadataManager.getOffsets(groupId, defaultRequireStable, Some(Seq(topicPartition1, topicPartition2)))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(topicPartition1).map(_.offset))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(topicPartition2).map(_.offset))
  }

  @Test
  def testGroupMetadataRemovalWithLogAppendTime(): Unit = {
    val topicPartition1 = new TopicPartition("foo", 0)
    val topicPartition2 = new TopicPartition("foo", 1)

    groupMetadataManager.addOwnedPartition(groupPartitionId)

    val group = new GroupMetadata(groupId, Empty, time)
    groupMetadataManager.addGroup(group)
    group.generationId = 5

    // expect the group metadata tombstone
    val recordsCapture: ArgumentCaptor[MemoryRecords] = ArgumentCaptor.forClass(classOf[MemoryRecords])

    when(replicaManager.onlinePartition(any())).thenReturn(Some(mock(classOf[Partition])))
    mockGetPartition()
    when(partition.appendRecordsToLeader(recordsCapture.capture(),
      origin = ArgumentMatchers.eq(AppendOrigin.COORDINATOR), requiredAcks = anyInt(),
      any(), any())).thenReturn(LogAppendInfo.UNKNOWN_LOG_APPEND_INFO)
    groupMetadataManager.cleanupGroupMetadata()

    val records = recordsCapture.getValue.records.asScala.toList
    recordsCapture.getValue.batches.forEach { batch =>
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
    val cachedOffsets = groupMetadataManager.getOffsets(groupId, defaultRequireStable, Some(Seq(topicPartition1, topicPartition2)))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(topicPartition1).map(_.offset))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(topicPartition2).map(_.offset))
  }

  @Test
  def testExpireGroupWithOffsetsOnly(): Unit = {
    // verify that the group is removed properly, but no tombstone is written if
    // this is a group which is only using kafka for offset storage

    val memberId = ""
    val topicIdPartition1 = new TopicIdPartition(Uuid.randomUuid(), 0, "foo")
    val topicIdPartition2 = new TopicIdPartition(topicIdPartition1.topicId, 1, "foo")
    val offset = 37

    groupMetadataManager.addOwnedPartition(groupPartitionId)

    val group = new GroupMetadata(groupId, Empty, time)
    groupMetadataManager.addGroup(group)

    val offsetTopicPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupMetadataManager.partitionFor(group.groupId))
    // expire the offset after 1 millisecond
    val startMs = time.milliseconds
    val offsets = immutable.Map(
      topicIdPartition1 -> new OffsetAndMetadata(offset, OptionalInt.empty(), "", startMs, OptionalLong.of(startMs + 1)),
      topicIdPartition2 -> new OffsetAndMetadata(offset, OptionalInt.empty(), "", startMs, OptionalLong.of(startMs + 3)))

    mockGetPartition()
    expectAppendMessage(Errors.NONE)
    var commitErrors: Option[immutable.Map[TopicIdPartition, Errors]] = None
    def callback(errors: immutable.Map[TopicIdPartition, Errors]): Unit = {
      commitErrors = Some(errors)
    }

    groupMetadataManager.storeOffsets(group, memberId, offsetTopicPartition, offsets, callback, verificationGuard = None)
    assertTrue(group.hasOffsets)

    assertFalse(commitErrors.isEmpty)
    assertEquals(Some(Errors.NONE), commitErrors.get.get(topicIdPartition1))

    // expire all of the offsets
    time.sleep(4)

    // expect the offset tombstone
    val recordsCapture: ArgumentCaptor[MemoryRecords] = ArgumentCaptor.forClass(classOf[MemoryRecords])
    when(replicaManager.onlinePartition(any())).thenReturn(Some(partition))
    when(partition.appendRecordsToLeader(recordsCapture.capture(),
      origin = ArgumentMatchers.eq(AppendOrigin.COORDINATOR), requiredAcks = anyInt(),
      any(), any())).thenReturn(LogAppendInfo.UNKNOWN_LOG_APPEND_INFO)
    groupMetadataManager.cleanupGroupMetadata()

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
    val cachedOffsets = groupMetadataManager.getOffsets(
      groupId,
      defaultRequireStable,
      Some(Seq(topicIdPartition1.topicPartition, topicIdPartition2.topicPartition))
    )
    assertEquals(
      Some(OffsetFetchResponse.INVALID_OFFSET),
      cachedOffsets.get(topicIdPartition1.topicPartition).map(_.offset)
    )
    assertEquals(
      Some(OffsetFetchResponse.INVALID_OFFSET),
      cachedOffsets.get(topicIdPartition2.topicPartition).map(_.offset)
    )

    verify(replicaManager, times(2)).onlinePartition(groupTopicPartition)
  }

  @Test
  def testOffsetExpirationSemantics(): Unit = {
    val memberId = "memberId"
    val clientId = "clientId"
    val clientHost = "localhost"
    val topic = "foo"
    val topicIdPartition1 = new TopicIdPartition(Uuid.randomUuid(), 0, topic)
    val topicIdPartition2 = new TopicIdPartition(topicIdPartition1.topicId, 1, topic)
    val topicIdPartition3 = new TopicIdPartition(topicIdPartition1.topicId, 2, topic)
    val offset = 37

    groupMetadataManager.addOwnedPartition(groupPartitionId)

    val group = new GroupMetadata(groupId, Empty, time)
    groupMetadataManager.addGroup(group)

    val subscription = new Subscription(List(topic).asJava)
    val member = new MemberMetadata(memberId, Some(groupInstanceId), clientId, clientHost, rebalanceTimeout, sessionTimeout,
      protocolType, List(("protocol", ConsumerProtocol.serializeSubscription(subscription).array())))
    group.add(member, _ => ())
    group.transitionTo(PreparingRebalance)
    group.initNextGeneration()

    val offsetTopicPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupMetadataManager.partitionFor(group.groupId))
    val startMs = time.milliseconds
    // old clients, expiry timestamp is explicitly set
    val tp1OffsetAndMetadata = new OffsetAndMetadata(offset, noLeader, "", startMs, OptionalLong.of(startMs + 1))
    val tp2OffsetAndMetadata = new OffsetAndMetadata(offset, noLeader, "", startMs, OptionalLong.of(startMs + 3))
    // new clients, no per-partition expiry timestamp, offsets of group expire together
    val tp3OffsetAndMetadata = new OffsetAndMetadata(offset, noLeader, "", startMs, noExpiration)
    val offsets = immutable.Map(
      topicIdPartition1 -> tp1OffsetAndMetadata,
      topicIdPartition2 -> tp2OffsetAndMetadata,
      topicIdPartition3 -> tp3OffsetAndMetadata)

    mockGetPartition()
    expectAppendMessage(Errors.NONE)
    var commitErrors: Option[immutable.Map[TopicIdPartition, Errors]] = None
    def callback(errors: immutable.Map[TopicIdPartition, Errors]): Unit = {
      commitErrors = Some(errors)
    }

    groupMetadataManager.storeOffsets(group, memberId, offsetTopicPartition, offsets, callback, verificationGuard = None)
    assertTrue(group.hasOffsets)

    assertFalse(commitErrors.isEmpty)
    assertEquals(Some(Errors.NONE), commitErrors.get.get(topicIdPartition1))

    // do not expire any offset even though expiration timestamp is reached for one (due to group still being active)
    time.sleep(2)

    groupMetadataManager.cleanupGroupMetadata()

    // group and offsets should still be there
    assertEquals(Some(group), groupMetadataManager.getGroup(groupId))
    assertEquals(Some(tp1OffsetAndMetadata), group.offset(topicIdPartition1.topicPartition))
    assertEquals(Some(tp2OffsetAndMetadata), group.offset(topicIdPartition2.topicPartition))
    assertEquals(Some(tp3OffsetAndMetadata), group.offset(topicIdPartition3.topicPartition))

    var cachedOffsets = groupMetadataManager.getOffsets(
      groupId,
      defaultRequireStable,
      Some(Seq(topicIdPartition1.topicPartition, topicIdPartition2.topicPartition, topicIdPartition3.topicPartition))
    )
    assertEquals(Some(offset), cachedOffsets.get(topicIdPartition1.topicPartition).map(_.offset))
    assertEquals(Some(offset), cachedOffsets.get(topicIdPartition2.topicPartition).map(_.offset))
    assertEquals(Some(offset), cachedOffsets.get(topicIdPartition3.topicPartition).map(_.offset))

    verify(replicaManager, times(2)).onlinePartition(groupTopicPartition)

    group.transitionTo(PreparingRebalance)
    group.transitionTo(Empty)

    // expect the offset tombstone
    when(partition.appendRecordsToLeader(any[MemoryRecords],
      origin = ArgumentMatchers.eq(AppendOrigin.COORDINATOR), requiredAcks = anyInt(),
      any(), any())).thenReturn(LogAppendInfo.UNKNOWN_LOG_APPEND_INFO)
    groupMetadataManager.cleanupGroupMetadata()

    // group is empty now, only one offset should expire
    assertEquals(Some(group), groupMetadataManager.getGroup(groupId))
    assertEquals(None, group.offset(topicIdPartition1.topicPartition))
    assertEquals(Some(tp2OffsetAndMetadata), group.offset(topicIdPartition2.topicPartition))
    assertEquals(Some(tp3OffsetAndMetadata), group.offset(topicIdPartition3.topicPartition))

    cachedOffsets = groupMetadataManager.getOffsets(
      groupId,
      defaultRequireStable,
      Some(Seq(topicIdPartition1.topicPartition, topicIdPartition2.topicPartition, topicIdPartition3.topicPartition))
    )
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(topicIdPartition1.topicPartition).map(_.offset))
    assertEquals(Some(offset), cachedOffsets.get(topicIdPartition2.topicPartition).map(_.offset))
    assertEquals(Some(offset), cachedOffsets.get(topicIdPartition3.topicPartition).map(_.offset))

    verify(replicaManager, times(3)).onlinePartition(groupTopicPartition)

    time.sleep(2)

    // expect the offset tombstone
    when(partition.appendRecordsToLeader(any[MemoryRecords],
      origin = ArgumentMatchers.eq(AppendOrigin.COORDINATOR), requiredAcks = anyInt(),
      any(), any())).thenReturn(LogAppendInfo.UNKNOWN_LOG_APPEND_INFO)
    groupMetadataManager.cleanupGroupMetadata()

    // one more offset should expire
    assertEquals(Some(group), groupMetadataManager.getGroup(groupId))
    assertEquals(None, group.offset(topicIdPartition1.topicPartition))
    assertEquals(None, group.offset(topicIdPartition2.topicPartition))
    assertEquals(Some(tp3OffsetAndMetadata), group.offset(topicIdPartition3.topicPartition))

    cachedOffsets = groupMetadataManager.getOffsets(
      groupId,
      defaultRequireStable,
      Some(Seq(topicIdPartition1.topicPartition, topicIdPartition2.topicPartition, topicIdPartition3.topicPartition))
    )
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(topicIdPartition1.topicPartition).map(_.offset))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(topicIdPartition2.topicPartition).map(_.offset))
    assertEquals(Some(offset), cachedOffsets.get(topicIdPartition3.topicPartition).map(_.offset))

    verify(replicaManager, times(4)).onlinePartition(groupTopicPartition)

    // advance time to just before the offset of last partition is to be expired, no offset should expire
    time.sleep(group.currentStateTimestamp.get + defaultOffsetRetentionMs - time.milliseconds() - 1)

    groupMetadataManager.cleanupGroupMetadata()

    // one more offset should expire
    assertEquals(Some(group), groupMetadataManager.getGroup(groupId))
    assertEquals(None, group.offset(topicIdPartition1.topicPartition))
    assertEquals(None, group.offset(topicIdPartition2.topicPartition))
    assertEquals(Some(tp3OffsetAndMetadata), group.offset(topicIdPartition3.topicPartition))

    cachedOffsets = groupMetadataManager.getOffsets(
      groupId,
      defaultRequireStable,
      Some(Seq(topicIdPartition1.topicPartition, topicIdPartition2.topicPartition, topicIdPartition3.topicPartition))
    )
    assertEquals(
      Some(OffsetFetchResponse.INVALID_OFFSET),
      cachedOffsets.get(topicIdPartition1.topicPartition).map(_.offset)
    )
    assertEquals(
      Some(OffsetFetchResponse.INVALID_OFFSET),
      cachedOffsets.get(topicIdPartition2.topicPartition).map(_.offset)
    )
    assertEquals(
      Some(offset),
      cachedOffsets.get(topicIdPartition3.topicPartition).map(_.offset)
    )

    verify(replicaManager, times(5)).onlinePartition(groupTopicPartition)

    // advance time enough for that last offset to expire
    time.sleep(2)

    // expect the offset tombstone
    when(partition.appendRecordsToLeader(any[MemoryRecords],
      origin = ArgumentMatchers.eq(AppendOrigin.COORDINATOR), requiredAcks = anyInt(),
      any(), any())).thenReturn(LogAppendInfo.UNKNOWN_LOG_APPEND_INFO)
    groupMetadataManager.cleanupGroupMetadata()

    // group and all its offsets should be gone now
    assertEquals(None, groupMetadataManager.getGroup(groupId))
    assertEquals(None, group.offset(topicIdPartition1.topicPartition))
    assertEquals(None, group.offset(topicIdPartition2.topicPartition))
    assertEquals(None, group.offset(topicIdPartition3.topicPartition))

    cachedOffsets = groupMetadataManager.getOffsets(
      groupId,
      defaultRequireStable,
      Some(Seq(topicIdPartition1.topicPartition, topicIdPartition2.topicPartition, topicIdPartition3.topicPartition))
    )
    assertEquals(
      Some(OffsetFetchResponse.INVALID_OFFSET),
      cachedOffsets.get(topicIdPartition1.topicPartition).map(_.offset)
    )
    assertEquals(
      Some(OffsetFetchResponse.INVALID_OFFSET),
      cachedOffsets.get(topicIdPartition2.topicPartition).map(_.offset)
    )
    assertEquals(
      Some(OffsetFetchResponse.INVALID_OFFSET),
      cachedOffsets.get(topicIdPartition3.topicPartition).map(_.offset)
    )

    verify(replicaManager, times(6)).onlinePartition(groupTopicPartition)

    assert(group.is(Dead))
  }

  @Test
  def testOffsetExpirationOfSimpleConsumer(): Unit = {
    val memberId = "memberId"
    val topic = "foo"
    val topicIdPartition1 = new TopicIdPartition(Uuid.randomUuid(), 0, topic)
    val offset = 37

    groupMetadataManager.addOwnedPartition(groupPartitionId)

    val group = new GroupMetadata(groupId, Empty, time)
    groupMetadataManager.addGroup(group)

    val offsetTopicPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupMetadataManager.partitionFor(group.groupId))
    // expire the offset after 1 and 3 milliseconds (old clients) and after default retention (new clients)
    val startMs = time.milliseconds
    // old clients, expiry timestamp is explicitly set
    val tp1OffsetAndMetadata = new OffsetAndMetadata(offset, noLeader, "", startMs, noExpiration)
    // new clients, no per-partition expiry timestamp, offsets of group expire together
    val offsets = immutable.Map(
      topicIdPartition1 -> tp1OffsetAndMetadata)

    mockGetPartition()
    expectAppendMessage(Errors.NONE)
    var commitErrors: Option[immutable.Map[TopicIdPartition, Errors]] = None
    def callback(errors: immutable.Map[TopicIdPartition, Errors]): Unit = {
      commitErrors = Some(errors)
    }

    groupMetadataManager.storeOffsets(group, memberId, offsetTopicPartition, offsets, callback, verificationGuard = None)
    assertTrue(group.hasOffsets)

    assertFalse(commitErrors.isEmpty)
    assertEquals(Some(Errors.NONE), commitErrors.get.get(topicIdPartition1))

    // do not expire offsets while within retention period since commit timestamp
    val expiryTimestamp = offsets(topicIdPartition1).commitTimestampMs + defaultOffsetRetentionMs
    time.sleep(expiryTimestamp - time.milliseconds() - 1)

    groupMetadataManager.cleanupGroupMetadata()

    // group and offsets should still be there
    assertEquals(Some(group), groupMetadataManager.getGroup(groupId))
    assertEquals(Some(tp1OffsetAndMetadata), group.offset(topicIdPartition1.topicPartition))

    var cachedOffsets = groupMetadataManager.getOffsets(
      groupId,
      defaultRequireStable,
      Some(Seq(topicIdPartition1.topicPartition))
    )
    assertEquals(Some(offset), cachedOffsets.get(topicIdPartition1.topicPartition).map(_.offset))

    verify(replicaManager, times(2)).onlinePartition(groupTopicPartition)

    // advance time to enough for offsets to expire
    time.sleep(2)

    // expect the offset tombstone
    when(partition.appendRecordsToLeader(any[MemoryRecords],
      origin = ArgumentMatchers.eq(AppendOrigin.COORDINATOR), requiredAcks = anyInt(),
      any(), any())).thenReturn(LogAppendInfo.UNKNOWN_LOG_APPEND_INFO)
    groupMetadataManager.cleanupGroupMetadata()

    // group and all its offsets should be gone now
    assertEquals(None, groupMetadataManager.getGroup(groupId))
    assertEquals(None, group.offset(topicIdPartition1.topicPartition))

    cachedOffsets = groupMetadataManager.getOffsets(
      groupId,
      defaultRequireStable,
      Some(Seq(topicIdPartition1.topicPartition))
    )
    assertEquals(
      Some(OffsetFetchResponse.INVALID_OFFSET),
      cachedOffsets.get(topicIdPartition1.topicPartition).map(_.offset)
    )

    verify(replicaManager, times(3)).onlinePartition(groupTopicPartition)

    assert(group.is(Dead))
  }

  @Test
  def testOffsetExpirationOfActiveGroupSemantics(): Unit = {
    val memberId = "memberId"
    val clientId = "clientId"
    val clientHost = "localhost"

    val topic1 = "foo"
    val topic1IdPartition0 = new TopicIdPartition(Uuid.randomUuid(), 0, topic1)
    val topic1IdPartition1 = new TopicIdPartition(topic1IdPartition0.topicId, 1, topic1)

    val topic2 = "bar"
    val topic2IdPartition0 = new TopicIdPartition(Uuid.randomUuid(), 0, topic2)
    val topic2IdPartition1 = new TopicIdPartition(topic2IdPartition0.topicId, 1, topic2)

    val offset = 37

    groupMetadataManager.addOwnedPartition(groupPartitionId)

    val group = new GroupMetadata(groupId, Empty, time)
    groupMetadataManager.addGroup(group)

    // Subscribe to topic1 and topic2
    val subscriptionTopic1AndTopic2 = new Subscription(List(topic1, topic2).asJava)

    val member = new MemberMetadata(
      memberId,
      Some(groupInstanceId),
      clientId,
      clientHost,
      rebalanceTimeout,
      sessionTimeout,
      ConsumerProtocol.PROTOCOL_TYPE,
      List(("protocol", ConsumerProtocol.serializeSubscription(subscriptionTopic1AndTopic2).array()))
    )

    group.add(member, _ => ())
    group.transitionTo(PreparingRebalance)
    group.initNextGeneration()
    group.transitionTo(Stable)

    val offsetTopicPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupMetadataManager.partitionFor(group.groupId))
    val startMs = time.milliseconds

    val t1p0OffsetAndMetadata = new OffsetAndMetadata(offset, noLeader, "", startMs, noExpiration)
    val t1p1OffsetAndMetadata = new OffsetAndMetadata(offset, noLeader, "", startMs, noExpiration)

    val t2p0OffsetAndMetadata = new OffsetAndMetadata(offset, noLeader, "", startMs, noExpiration)
    val t2p1OffsetAndMetadata = new OffsetAndMetadata(offset, noLeader, "", startMs, noExpiration)

    val offsets = immutable.Map(
      topic1IdPartition0 -> t1p0OffsetAndMetadata,
      topic1IdPartition1 -> t1p1OffsetAndMetadata,
      topic2IdPartition0 -> t2p0OffsetAndMetadata,
      topic2IdPartition1 -> t2p1OffsetAndMetadata)

    mockGetPartition()
    expectAppendMessage(Errors.NONE)
    var commitErrors: Option[immutable.Map[TopicIdPartition, Errors]] = None
    def callback(errors: immutable.Map[TopicIdPartition, Errors]): Unit = {
      commitErrors = Some(errors)
    }

    groupMetadataManager.storeOffsets(group, memberId, offsetTopicPartition, offsets, callback, verificationGuard = None)
    assertTrue(group.hasOffsets)

    assertFalse(commitErrors.isEmpty)
    assertEquals(Some(Errors.NONE), commitErrors.get.get(topic1IdPartition0))

    // advance time to just after the offset of last partition is to be expired
    time.sleep(defaultOffsetRetentionMs + 2)

    // no offset should expire because all topics are actively consumed
    groupMetadataManager.cleanupGroupMetadata()

    assertEquals(Some(group), groupMetadataManager.getGroup(groupId))
    assert(group.is(Stable))

    assertEquals(Some(t1p0OffsetAndMetadata), group.offset(topic1IdPartition0.topicPartition))
    assertEquals(Some(t1p1OffsetAndMetadata), group.offset(topic1IdPartition1.topicPartition))
    assertEquals(Some(t2p0OffsetAndMetadata), group.offset(topic2IdPartition0.topicPartition))
    assertEquals(Some(t2p1OffsetAndMetadata), group.offset(topic2IdPartition1.topicPartition))

    var cachedOffsets = groupMetadataManager.getOffsets(
      groupId,
      defaultRequireStable,
      Some(Seq(
        topic1IdPartition0.topicPartition,
        topic1IdPartition1.topicPartition,
        topic2IdPartition0.topicPartition,
        topic2IdPartition1.topicPartition)
      )
    )

    assertEquals(
      Some(offset),
      cachedOffsets.get(topic1IdPartition0.topicPartition).map(_.offset)
    )
    assertEquals(
      Some(offset),
      cachedOffsets.get(topic1IdPartition1.topicPartition).map(_.offset)
    )
    assertEquals(
      Some(offset),
      cachedOffsets.get(topic2IdPartition0.topicPartition).map(_.offset)
    )
    assertEquals(
      Some(offset),
      cachedOffsets.get(topic2IdPartition1.topicPartition).map(_.offset)
    )

    verify(replicaManager, times(2)).onlinePartition(groupTopicPartition)

    group.transitionTo(PreparingRebalance)

    // Subscribe to topic1, offsets of topic2 should be removed
    val subscriptionTopic1 = new Subscription(List(topic1).asJava)

    group.updateMember(
      member,
      List(("protocol", ConsumerProtocol.serializeSubscription(subscriptionTopic1).array())),
      member.rebalanceTimeoutMs,
      member.sessionTimeoutMs,
      null
    )

    group.initNextGeneration()
    group.transitionTo(Stable)

    when(replicaManager.onlinePartition(any)).thenReturn(Some(partition))
    // expect the offset tombstone
    when(partition.appendRecordsToLeader(any[MemoryRecords],
      origin = ArgumentMatchers.eq(AppendOrigin.COORDINATOR), requiredAcks = anyInt(),
      any(), any())).thenReturn(LogAppendInfo.UNKNOWN_LOG_APPEND_INFO)

    groupMetadataManager.cleanupGroupMetadata()

    verify(partition).appendRecordsToLeader(any[MemoryRecords],
      origin = ArgumentMatchers.eq(AppendOrigin.COORDINATOR), requiredAcks = anyInt(),
      any(), any())
    verify(replicaManager, times(3)).onlinePartition(groupTopicPartition)

    assertEquals(Some(group), groupMetadataManager.getGroup(groupId))
    assert(group.is(Stable))

    assertEquals(Some(t1p0OffsetAndMetadata), group.offset(topic1IdPartition0.topicPartition))
    assertEquals(Some(t1p1OffsetAndMetadata), group.offset(topic1IdPartition1.topicPartition))
    assertEquals(None, group.offset(topic2IdPartition0.topicPartition))
    assertEquals(None, group.offset(topic2IdPartition1.topicPartition))

    cachedOffsets = groupMetadataManager.getOffsets(
      groupId,
      defaultRequireStable,
      Some(Seq(
        topic1IdPartition0.topicPartition,
        topic1IdPartition1.topicPartition,
        topic2IdPartition0.topicPartition,
        topic2IdPartition1.topicPartition)
      )
    )

    assertEquals(Some(offset), cachedOffsets.get(topic1IdPartition0.topicPartition).map(_.offset))
    assertEquals(Some(offset), cachedOffsets.get(topic1IdPartition1.topicPartition).map(_.offset))
    assertEquals(
      Some(OffsetFetchResponse.INVALID_OFFSET),
      cachedOffsets.get(topic2IdPartition0.topicPartition).map(_.offset)
    )
    assertEquals(
      Some(OffsetFetchResponse.INVALID_OFFSET),
      cachedOffsets.get(topic2IdPartition1.topicPartition).map(_.offset)
    )
  }

  @Test
  def testLoadOffsetFromOldCommit(): Unit = {
    val groupMetadataTopicPartition = groupTopicPartition
    val generation = 935
    val protocolType = "consumer"
    val protocol = "range"
    val startOffset = 15L
    val groupEpoch = 2
    val committedOffsets = Map(
      new TopicPartition("foo", 0) -> 23L,
      new TopicPartition("foo", 1) -> 455L,
      new TopicPartition("bar", 0) -> 8992L
    )

    val metadataVersion = IBP_1_1_IV0
    val offsetCommitRecords = createCommittedOffsetRecords(committedOffsets, metadataVersion = metadataVersion, retentionTimeOpt = Some(100))
    val memberId = "98098230493"
    val groupMetadataRecord = buildStableGroupRecordWithMember(generation, protocolType, protocol, memberId, metadataVersion = metadataVersion)
    val records = MemoryRecords.withRecords(startOffset, Compression.NONE,
      (offsetCommitRecords ++ Seq(groupMetadataRecord)).toArray: _*)

    expectGroupMetadataLoad(groupMetadataTopicPartition, startOffset, records)

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, groupEpoch, _ => (), 0L)

    val group = groupMetadataManager.getGroup(groupId).getOrElse(throw new AssertionError("Group was not loaded into the cache"))
    assertEquals(groupId, group.groupId)
    assertEquals(Stable, group.currentState)
    assertEquals(memberId, group.leaderOrNull)
    assertEquals(generation, group.generationId)
    assertEquals(Some(protocolType), group.protocolType)
    assertEquals(protocol, group.protocolName.orNull)
    assertEquals(Set(memberId), group.allMembers)
    assertEquals(committedOffsets.size, group.allOffsets.size)
    committedOffsets.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.committedOffset))
      assertTrue(group.offset(topicPartition).map(_.expireTimestampMs).get.isPresent)
    }
  }

  @Test
  def testLoadOffsetWithExplicitRetention(): Unit = {
    val groupMetadataTopicPartition = groupTopicPartition
    val generation = 935
    val protocolType = "consumer"
    val protocol = "range"
    val startOffset = 15L
    val groupEpoch = 2
    val committedOffsets = Map(
      new TopicPartition("foo", 0) -> 23L,
      new TopicPartition("foo", 1) -> 455L,
      new TopicPartition("bar", 0) -> 8992L
    )

    val offsetCommitRecords = createCommittedOffsetRecords(committedOffsets, retentionTimeOpt = Some(100))
    val memberId = "98098230493"
    val groupMetadataRecord = buildStableGroupRecordWithMember(generation, protocolType, protocol, memberId)
    val records = MemoryRecords.withRecords(startOffset, Compression.NONE,
      (offsetCommitRecords ++ Seq(groupMetadataRecord)).toArray: _*)

    expectGroupMetadataLoad(groupMetadataTopicPartition, startOffset, records)

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, groupEpoch, _ => (), 0L)

    val group = groupMetadataManager.getGroup(groupId).getOrElse(throw new AssertionError("Group was not loaded into the cache"))
    assertEquals(groupId, group.groupId)
    assertEquals(Stable, group.currentState)
    assertEquals(memberId, group.leaderOrNull)
    assertEquals(generation, group.generationId)
    assertEquals(Some(protocolType), group.protocolType)
    assertEquals(protocol, group.protocolName.orNull)
    assertEquals(Set(memberId), group.allMembers)
    assertEquals(committedOffsets.size, group.allOffsets.size)
    committedOffsets.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.committedOffset))
      assertTrue(group.offset(topicPartition).map(_.expireTimestampMs).get.isPresent)
    }
  }

  @Test
  def testSerdeOffsetCommitValue(): Unit = {
    val offsetAndMetadata = new OffsetAndMetadata(
      537L,
      OptionalInt.of(15),
      "metadata",
      time.milliseconds(),
      noExpiration)

    def verifySerde(metadataVersion: MetadataVersion, expectedOffsetCommitValueVersion: Int): Unit = {
      val bytes = GroupMetadataManager.offsetCommitValue(offsetAndMetadata, metadataVersion)
      val buffer = ByteBuffer.wrap(bytes)

      assertEquals(expectedOffsetCommitValueVersion, buffer.getShort(0).toInt)

      val deserializedOffsetAndMetadata = GroupMetadataManager.readOffsetMessageValue(buffer)
      assertEquals(offsetAndMetadata.committedOffset, deserializedOffsetAndMetadata.committedOffset)
      assertEquals(offsetAndMetadata.metadata, deserializedOffsetAndMetadata.metadata)
      assertEquals(offsetAndMetadata.commitTimestampMs, deserializedOffsetAndMetadata.commitTimestampMs)

      // Serialization drops the leader epoch silently if an older inter-broker protocol is in use
      val expectedLeaderEpoch = if (expectedOffsetCommitValueVersion >= 3)
        offsetAndMetadata.leaderEpoch
      else
        noLeader

      assertEquals(expectedLeaderEpoch, deserializedOffsetAndMetadata.leaderEpoch)
    }

    for (version <- MetadataVersion.VERSIONS) {
      val expectedSchemaVersion = version match {
        case v if v.isLessThan(IBP_2_1_IV0) => 1
        case v if v.isLessThan(IBP_2_1_IV1) => 2
        case _ => 3
      }
      verifySerde(version, expectedSchemaVersion)
    }
  }

  @Test
  def testSerdeOffsetCommitValueWithExpireTimestamp(): Unit = {
    // If expire timestamp is set, we should always use version 1 of the offset commit
    // value schema since later versions do not support it

    val offsetAndMetadata = new OffsetAndMetadata(
      537L,
      noLeader,
      "metadata",
      time.milliseconds(),
      OptionalLong.of(time.milliseconds() + 1000))

    def verifySerde(metadataVersion: MetadataVersion): Unit = {
      val bytes = GroupMetadataManager.offsetCommitValue(offsetAndMetadata, metadataVersion)
      val buffer = ByteBuffer.wrap(bytes)
      assertEquals(1, buffer.getShort(0).toInt)

      val deserializedOffsetAndMetadata = GroupMetadataManager.readOffsetMessageValue(buffer)
      assertEquals(offsetAndMetadata, deserializedOffsetAndMetadata)
    }

    for (version <- MetadataVersion.VERSIONS)
      verifySerde(version)
  }

  @Test
  def testSerdeOffsetCommitValueWithNoneExpireTimestamp(): Unit = {
    val offsetAndMetadata = new OffsetAndMetadata(
      537L,
      noLeader,
      "metadata",
      time.milliseconds(),
      noExpiration)

    def verifySerde(metadataVersion: MetadataVersion): Unit = {
      val bytes = GroupMetadataManager.offsetCommitValue(offsetAndMetadata, metadataVersion)
      val buffer = ByteBuffer.wrap(bytes)
      val version = buffer.getShort(0).toInt
      if (metadataVersion.isLessThan(IBP_2_1_IV0))
        assertEquals(1, version)
      else if (metadataVersion.isLessThan(IBP_2_1_IV1))
        assertEquals(2, version)
      else
        assertEquals(3, version)

      val deserializedOffsetAndMetadata = GroupMetadataManager.readOffsetMessageValue(buffer)
      assertEquals(offsetAndMetadata, deserializedOffsetAndMetadata)
    }

    for (version <- MetadataVersion.VERSIONS)
      verifySerde(version)
  }

  @Test
  def testSerializeGroupMetadataValueToHighestNonFlexibleVersion(): Unit = {
    val generation = 935
    val protocolType = "consumer"
    val protocol = "range"
    val memberId = "98098230493"
    val assignmentBytes = Utils.toArray(ConsumerProtocol.serializeAssignment(
      new ConsumerPartitionAssignor.Assignment(List(new TopicPartition("topic", 0)).asJava, null)
    ))
    val record = TestUtils.records(Seq(
      buildStableGroupRecordWithMember(generation, protocolType, protocol, memberId, assignmentBytes)
    )).records.asScala.head
    assertEquals(3, record.value.getShort)
  }

  @Test
  def testSerializeOffsetCommitValueToHighestNonFlexibleVersion(): Unit = {
    val committedOffsets = Map(
      new TopicPartition("foo", 0) -> 23L,
      new TopicPartition("foo", 1) -> 455L,
    )

    val offsetCommitRecords = createCommittedOffsetRecords(committedOffsets)
    offsetCommitRecords.foreach { record =>
      assertEquals(3, record.value.getShort)
    }
  }

  @Test
  def testDeserializeHighestSupportedGroupMetadataValueVersion(): Unit = {
    val member = new GroupMetadataValue.MemberMetadata()
      .setMemberId("member")
      .setClientId("client")
      .setClientHost("host")

    val generation = 935
    val protocolType = "consumer"
    val protocol = "range"
    val leader = "leader"
    val groupMetadataValue = new GroupMetadataValue()
      .setProtocolType(protocolType)
      .setGeneration(generation)
      .setProtocol(protocol)
      .setLeader(leader)
      .setMembers(java.util.Collections.singletonList(member))

    val deserialized = GroupMetadataManager.readGroupMessageValue("groupId",
      MessageUtil.toVersionPrefixedByteBuffer(4, groupMetadataValue), time)

    assertEquals(generation, deserialized.generationId)
    assertEquals(protocolType, deserialized.protocolType.get)
    assertEquals(protocol, deserialized.protocolName.get)
    assertEquals(leader, deserialized.leaderOrNull)

    val actualMember = deserialized.allMemberMetadata.head
    assertEquals(member.memberId, actualMember.memberId)
    assertEquals(member.clientId, actualMember.clientId)
    assertEquals(member.clientHost, actualMember.clientHost)
  }

  @Test
  def testDeserializeHighestSupportedOffsetCommitValueVersion(): Unit = {
    val offsetCommitValue = new OffsetCommitValue()
      .setOffset(1000L)
      .setMetadata("metadata")
      .setCommitTimestamp(1500L)
      .setLeaderEpoch(1)

    val serialized = MessageUtil.toVersionPrefixedByteBuffer(4, offsetCommitValue)
    val deserialized = GroupMetadataManager.readOffsetMessageValue(serialized)

    assertEquals(1000L, deserialized.committedOffset)
    assertEquals("metadata", deserialized.metadata)
    assertEquals(1500L, deserialized.commitTimestampMs)
    assertEquals(1, deserialized.leaderEpoch.getAsInt)
  }

  @Test
  def testDeserializeFutureOffsetCommitValue(): Unit = {
    // Copy of OffsetCommitValue.SCHEMA_4 with a few
    // additional tagged fields.
    val futureOffsetCommitSchema = new Schema(
      new Field("offset", Type.INT64, ""),
      new Field("leader_epoch", Type.INT32, ""),
      new Field("metadata", Type.COMPACT_STRING, ""),
      new Field("commit_timestamp", Type.INT64, ""),
      TaggedFieldsSection.of(
        Int.box(0), new Field("offset_foo", Type.STRING, ""),
        Int.box(1), new Field("offset_bar", Type.INT32, "")
      )
    )

    // Create OffsetCommitValue with tagged fields
    val offsetCommit = new Struct(futureOffsetCommitSchema)
    offsetCommit.set("offset", 1000L)
    offsetCommit.set("leader_epoch", 100)
    offsetCommit.set("metadata", "metadata")
    offsetCommit.set("commit_timestamp", 2000L)
    val offsetCommitTaggedFields = new java.util.TreeMap[Integer, Any]()
    offsetCommitTaggedFields.put(0, "foo")
    offsetCommitTaggedFields.put(1, 4000)
    offsetCommit.set("_tagged_fields", offsetCommitTaggedFields)

    // Prepare the buffer.
    val buffer = ByteBuffer.allocate(offsetCommit.sizeOf() + 2)
    buffer.put(0.toByte)
    buffer.put(4.toByte) // Add 4 as version.
    offsetCommit.writeTo(buffer)
    buffer.flip()

    // Read the buffer with the real schema and verify that tagged
    // fields were read but ignored.
    buffer.getShort() // Skip version.
    val value = new OffsetCommitValue(new ByteBufferAccessor(buffer), 4.toShort)
    assertEquals(Seq(0, 1), value.unknownTaggedFields().asScala.map(_.tag))

    // Read the buffer with readOffsetMessageValue.
    buffer.rewind()
    val offsetAndMetadata = GroupMetadataManager.readOffsetMessageValue(buffer)
    assertEquals(1000L, offsetAndMetadata.committedOffset)
    assertEquals(100, offsetAndMetadata.leaderEpoch.getAsInt)
    assertEquals("metadata", offsetAndMetadata.metadata)
    assertEquals(2000L, offsetAndMetadata.commitTimestampMs)
  }

  @Test
  def testDeserializeFutureGroupMetadataValue(): Unit = {
    // Copy of GroupMetadataValue.MemberMetadata.SCHEMA_4 with a few
    // additional tagged fields.
    val futureMemberSchema = new Schema(
      new Field("member_id", Type.COMPACT_STRING, ""),
      new Field("group_instance_id", Type.COMPACT_NULLABLE_STRING, ""),
      new Field("client_id", Type.COMPACT_STRING, ""),
      new Field("client_host", Type.COMPACT_STRING, ""),
      new Field("rebalance_timeout", Type.INT32, ""),
      new Field("session_timeout", Type.INT32, ""),
      new Field("subscription", Type.COMPACT_BYTES, ""),
      new Field("assignment", Type.COMPACT_BYTES, ""),
      TaggedFieldsSection.of(
        Int.box(0), new Field("member_foo", Type.STRING, ""),
        Int.box(1), new Field("member_foo", Type.INT32, "")
      )
    )

    // Copy of GroupMetadataValue.SCHEMA_4 with a few
    // additional tagged fields.
    val futureGroupSchema = new Schema(
      new Field("protocol_type", Type.COMPACT_STRING, ""),
      new Field("generation", Type.INT32, ""),
      new Field("protocol", Type.COMPACT_NULLABLE_STRING, ""),
      new Field("leader", Type.COMPACT_NULLABLE_STRING, ""),
      new Field("current_state_timestamp", Type.INT64, ""),
      new Field("members", new CompactArrayOf(futureMemberSchema), ""),
      TaggedFieldsSection.of(
        Int.box(0), new Field("group_foo", Type.STRING, ""),
        Int.box(1), new Field("group_bar", Type.INT32, "")
      )
    )

    // Create a member with tagged fields.
    val member = new Struct(futureMemberSchema)
    member.set("member_id", "member_id")
    member.set("group_instance_id", "group_instance_id")
    member.set("client_id", "client_id")
    member.set("client_host", "client_host")
    member.set("rebalance_timeout", 1)
    member.set("session_timeout", 2)
    member.set("subscription", ByteBuffer.allocate(0))
    member.set("assignment", ByteBuffer.allocate(0))

    val memberTaggedFields = new java.util.TreeMap[Integer, Any]()
    memberTaggedFields.put(0, "foo")
    memberTaggedFields.put(1, 4000)
    member.set("_tagged_fields", memberTaggedFields)

    // Create a group with tagged fields.
    val group = new Struct(futureGroupSchema)
    group.set("protocol_type", "consumer")
    group.set("generation", 10)
    group.set("protocol", "range")
    group.set("leader", "leader")
    group.set("current_state_timestamp", 1000L)
    group.set("members", Array(member))

    val groupTaggedFields = new java.util.TreeMap[Integer, Any]()
    groupTaggedFields.put(0, "foo")
    groupTaggedFields.put(1, 4000)
    group.set("_tagged_fields", groupTaggedFields)

    // Prepare the buffer.
    val buffer = ByteBuffer.allocate(group.sizeOf() + 2)
    buffer.put(0.toByte)
    buffer.put(4.toByte) // Add 4 as version.
    group.writeTo(buffer)
    buffer.flip()

    // Read the buffer with the real schema and verify that tagged
    // fields were read but ignored.
    buffer.getShort() // Skip version.
    val value = new GroupMetadataValue(new ByteBufferAccessor(buffer), 4.toShort)
    assertEquals(Seq(0, 1), value.unknownTaggedFields().asScala.map(_.tag))
    assertEquals(Seq(0, 1), value.members().get(0).unknownTaggedFields().asScala.map(_.tag))

    // Read the buffer with readGroupMessageValue.
    buffer.rewind()
    val groupMetadata = GroupMetadataManager.readGroupMessageValue("group", buffer, time)
    assertEquals("consumer", groupMetadata.protocolType.get)
    assertEquals("leader", groupMetadata.leaderOrNull)
    assertTrue(groupMetadata.allMembers.contains("member_id"))
  }

  @Test
  def testLoadOffsetsWithEmptyControlBatch(): Unit = {
    val groupMetadataTopicPartition = groupTopicPartition
    val startOffset = 15L
    val generation = 15
    val groupEpoch = 2

    val committedOffsets = Map(
      new TopicPartition("foo", 0) -> 23L,
      new TopicPartition("foo", 1) -> 455L,
      new TopicPartition("bar", 0) -> 8992L
    )

    val offsetCommitRecords = createCommittedOffsetRecords(committedOffsets)
    val groupMetadataRecord = buildEmptyGroupRecord(generation, protocolType)
    val records = MemoryRecords.withRecords(startOffset, Compression.NONE,
      (offsetCommitRecords ++ Seq(groupMetadataRecord)).toArray: _*)

    // Prepend empty control batch to valid records
    val mockBatch: MutableRecordBatch = mock(classOf[MutableRecordBatch])
    when(mockBatch.iterator).thenReturn(Collections.emptyIterator[Record])
    when(mockBatch.isControlBatch).thenReturn(true)
    when(mockBatch.isTransactional).thenReturn(true)
    when(mockBatch.nextOffset).thenReturn(16L)
    val mockRecords: MemoryRecords = mock(classOf[MemoryRecords])
    when(mockRecords.batches).thenReturn((Iterable[MutableRecordBatch](mockBatch) ++ records.batches.asScala).asJava)
    when(mockRecords.records).thenReturn(records.records())
    when(mockRecords.sizeInBytes()).thenReturn(DefaultRecordBatch.RECORD_BATCH_OVERHEAD + records.sizeInBytes())
    val logMock: UnifiedLog = mock(classOf[UnifiedLog])
    when(logMock.logStartOffset).thenReturn(startOffset)
    when(logMock.read(ArgumentMatchers.eq(startOffset),
      maxLength = anyInt(),
      isolation = ArgumentMatchers.eq(FetchIsolation.LOG_END),
      minOneMessage = ArgumentMatchers.eq(true)))
      .thenReturn(new FetchDataInfo(new LogOffsetMetadata(startOffset), mockRecords))
    when(replicaManager.getLog(groupMetadataTopicPartition)).thenReturn(Some(logMock))
    when(replicaManager.getLogEndOffset(groupMetadataTopicPartition)).thenReturn(Some[Long](18))
    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, groupEpoch, _ => (), 0L)

    // Empty control batch should not have caused the load to fail
    val group = groupMetadataManager.getGroup(groupId).getOrElse(throw new AssertionError("Group was not loaded into the cache"))
    assertEquals(groupId, group.groupId)
    assertEquals(Empty, group.currentState)
    assertEquals(generation, group.generationId)
    assertEquals(Some(protocolType), group.protocolType)
    assertNull(group.leaderOrNull)
    assertNull(group.protocolName.orNull)
    committedOffsets.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.committedOffset))
    }
  }

  private def verifyAppendAndCaptureCallback(): ArgumentCaptor[Map[TopicPartition, PartitionResponse] => Unit] = {
    val capturedArgument: ArgumentCaptor[Map[TopicPartition, PartitionResponse] => Unit] = ArgumentCaptor.forClass(classOf[Map[TopicPartition, PartitionResponse] => Unit])
    verify(replicaManager).appendRecords(anyLong(),
      anyShort(),
      any(),
      any(),
      any[Map[TopicPartition, MemoryRecords]],
      capturedArgument.capture(),
      any[Option[ReentrantLock]],
      any(),
      any(),
      any(),
      any())
    capturedArgument
  }

  private def expectAppendMessage(error: Errors): ArgumentCaptor[Map[TopicPartition, MemoryRecords]] = {
    val capturedCallback: ArgumentCaptor[Map[TopicPartition, PartitionResponse] => Unit] = ArgumentCaptor.forClass(classOf[Map[TopicPartition, PartitionResponse] => Unit])
    val capturedRecords: ArgumentCaptor[Map[TopicPartition, MemoryRecords]] = ArgumentCaptor.forClass(classOf[Map[TopicPartition, MemoryRecords]])
    when(replicaManager.appendRecords(anyLong(),
      anyShort(),
      any(),
      any(),
      capturedRecords.capture(),
      capturedCallback.capture(),
      any[Option[ReentrantLock]],
      any(),
      any(),
      any(),
      any()
    )).thenAnswer(_ => {
      capturedCallback.getValue.apply(
        Map(groupTopicPartition ->
          new PartitionResponse(error, 0L, RecordBatch.NO_TIMESTAMP, 0L)
        )
      )})
    when(replicaManager.onlinePartition(any())).thenReturn(Some(mock(classOf[Partition])))
    capturedRecords
  }

  private def buildStableGroupRecordWithMember(generation: Int,
                                               protocolType: String,
                                               protocol: String,
                                               memberId: String,
                                               assignmentBytes: Array[Byte] = Array.emptyByteArray,
                                               metadataVersion: MetadataVersion = MetadataVersion.latestTesting): SimpleRecord = {
    val memberProtocols = List((protocol, Array.emptyByteArray))
    val member = new MemberMetadata(memberId, Some(groupInstanceId), "clientId", "clientHost", 30000, 10000, protocolType, memberProtocols)
    val group = GroupMetadata.loadGroup(groupId, Stable, generation, protocolType, protocol, memberId,
      if (metadataVersion.isAtLeast(IBP_2_1_IV0)) Some(time.milliseconds()) else None, Seq(member), time)
    val groupMetadataKey = GroupMetadataManager.groupMetadataKey(groupId)
    val groupMetadataValue = GroupMetadataManager.groupMetadataValue(group, Map(memberId -> assignmentBytes), metadataVersion)
    new SimpleRecord(groupMetadataKey, groupMetadataValue)
  }

  private def buildEmptyGroupRecord(generation: Int, protocolType: String): SimpleRecord = {
    val group = GroupMetadata.loadGroup(groupId, Empty, generation, protocolType, null, null, None, Seq.empty, time)
    val groupMetadataKey = GroupMetadataManager.groupMetadataKey(groupId)
    val groupMetadataValue = GroupMetadataManager.groupMetadataValue(group, Map.empty, MetadataVersion.latestTesting)
    new SimpleRecord(groupMetadataKey, groupMetadataValue)
  }

  private def expectGroupMetadataLoad(groupMetadataTopicPartition: TopicPartition,
                                      startOffset: Long,
                                      records: MemoryRecords): Unit = {
    val logMock: UnifiedLog =  mock(classOf[UnifiedLog])
    when(replicaManager.getLog(groupMetadataTopicPartition)).thenReturn(Some(logMock))
    val endOffset = expectGroupMetadataLoad(logMock, startOffset, records)
    when(replicaManager.getLogEndOffset(groupMetadataTopicPartition)).thenReturn(Some(endOffset))
  }

  /**
   * mock records into a mocked log
   *
   * @return the calculated end offset to be mocked into [[ReplicaManager.getLogEndOffset]]
   */
  private def expectGroupMetadataLoad(logMock: UnifiedLog,
                                      startOffset: Long,
                                      records: MemoryRecords): Long = {
    val endOffset = startOffset + records.records.asScala.size
    val fileRecordsMock: FileRecords = mock(classOf[FileRecords])

    when(logMock.logStartOffset).thenReturn(startOffset)
    when(logMock.read(ArgumentMatchers.eq(startOffset),
      maxLength = anyInt(),
      isolation = ArgumentMatchers.eq(FetchIsolation.LOG_END),
      minOneMessage = ArgumentMatchers.eq(true)))
      .thenReturn(new FetchDataInfo(new LogOffsetMetadata(startOffset), fileRecordsMock))

    when(fileRecordsMock.sizeInBytes()).thenReturn(records.sizeInBytes)

    val bufferCapture: ArgumentCaptor[ByteBuffer] = ArgumentCaptor.forClass(classOf[ByteBuffer])
    when(fileRecordsMock.readInto(bufferCapture.capture(), anyInt())).thenAnswer(_ => {
      val buffer = bufferCapture.getValue
      buffer.put(records.buffer.duplicate)
      buffer.flip()
    })
    endOffset
  }

  private def createCommittedOffsetRecords(committedOffsets: Map[TopicPartition, Long],
                                           groupId: String = groupId,
                                           metadataVersion: MetadataVersion = MetadataVersion.latestTesting,
                                           retentionTimeOpt: Option[Long] = None): Seq[SimpleRecord] = {
    committedOffsets.map { case (topicPartition, offset) =>
      val commitTimestamp = time.milliseconds()
      val offsetAndMetadata = retentionTimeOpt match {
        case Some(retentionTimeMs) =>
          val expirationTime = OptionalLong.of(commitTimestamp + retentionTimeMs)
          new OffsetAndMetadata(offset, noLeader, "", commitTimestamp, expirationTime)
        case None =>
          new OffsetAndMetadata(offset, noLeader, "", commitTimestamp, noExpiration)
      }
      val offsetCommitKey = GroupMetadataManager.offsetCommitKey(groupId, topicPartition)
      val offsetCommitValue = GroupMetadataManager.offsetCommitValue(offsetAndMetadata, metadataVersion)
      new SimpleRecord(offsetCommitKey, offsetCommitValue)
    }.toSeq
  }

  private def mockGetPartition(): Unit = {
    when(replicaManager.getPartition(groupTopicPartition)).thenReturn(HostedPartition.Online(partition))
    when(replicaManager.onlinePartition(groupTopicPartition)).thenReturn(Some(partition))
  }

  private def getGauge(manager: GroupMetadataManager, name: String): Gauge[Int]  = {
    KafkaYammerMetrics.defaultRegistry().allMetrics().get(manager.metricsGroup.metricName(name, Collections.emptyMap())).asInstanceOf[Gauge[Int]]
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
    val group = new GroupMetadata("foo2", Stable, time)
    groupMetadataManager.addGroup(group)
    expectMetrics(groupMetadataManager, 1, 0, 0)
    group.transitionTo(PreparingRebalance)
    expectMetrics(groupMetadataManager, 1, 1, 0)
    group.transitionTo(CompletingRebalance)
    expectMetrics(groupMetadataManager, 1, 0, 1)
  }

  @Test
  def testPartitionLoadMetric(): Unit = {
    val server = ManagementFactory.getPlatformMBeanServer
    val mBeanName = "kafka.server:type=group-coordinator-metrics"
    val reporter = new JmxReporter
    val metricsContext = new KafkaMetricsContext("kafka.server")
    reporter.contextChange(metricsContext)
    metrics.addReporter(reporter)

    def partitionLoadTime(attribute: String): Double = {
      server.getAttribute(new ObjectName(mBeanName), attribute).asInstanceOf[Double]
    }

    assertTrue(server.isRegistered(new ObjectName(mBeanName)))
    assertEquals(Double.NaN, partitionLoadTime( "partition-load-time-max"), 0)
    assertEquals(Double.NaN, partitionLoadTime("partition-load-time-avg"), 0)
    assertTrue(reporter.containsMbean(mBeanName))

    val groupMetadataTopicPartition = groupTopicPartition
    val startOffset = 15L
    val memberId = "98098230493"
    val committedOffsets = Map(
      new TopicPartition("foo", 0) -> 23L,
      new TopicPartition("foo", 1) -> 455L,
      new TopicPartition("bar", 0) -> 8992L
    )

    val offsetCommitRecords = createCommittedOffsetRecords(committedOffsets)
    val groupMetadataRecord = buildStableGroupRecordWithMember(generation = 15,
      protocolType = "consumer", protocol = "range", memberId)
    val records = MemoryRecords.withRecords(startOffset, Compression.NONE,
      (offsetCommitRecords ++ Seq(groupMetadataRecord)).toArray: _*)

    expectGroupMetadataLoad(groupMetadataTopicPartition, startOffset, records)
    // When passed a specific start offset, assert that the measured values are in excess of that.
    val now = time.milliseconds()
    val diff = 1000
    val groupEpoch = 2
    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, groupEpoch, _ => (), now - diff)
    assertTrue(partitionLoadTime("partition-load-time-max") >= diff)
    assertTrue(partitionLoadTime("partition-load-time-avg") >= diff)
  }

  @Test
  def testReadMessageKeyCanReadUnknownMessage(): Unit = {
    val record = new org.apache.kafka.coordinator.group.generated.GroupMetadataKey()
    val unknownRecord = MessageUtil.toVersionPrefixedBytes(Short.MaxValue, record)
    val key = GroupMetadataManager.readMessageKey(ByteBuffer.wrap(unknownRecord))
    assertEquals(UnknownKey(Short.MaxValue), key)
  }

  @Test
  def testLoadGroupsAndOffsetsWillIgnoreUnknownMessage(): Unit = {
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

    // Should ignore unknown record
    val unknownKey = new org.apache.kafka.coordinator.group.generated.GroupMetadataKey()
    val lowestUnsupportedVersion = (CoordinatorRecordType.GROUP_METADATA.id + 1).toShort

    val unknownMessage1 = MessageUtil.toVersionPrefixedBytes(Short.MaxValue, unknownKey)
    val unknownMessage2 = MessageUtil.toVersionPrefixedBytes(lowestUnsupportedVersion, unknownKey)
    val unknownRecord1 = new SimpleRecord(unknownMessage1, unknownMessage1)
    val unknownRecord2 = new SimpleRecord(unknownMessage2, unknownMessage2)

    val records = MemoryRecords.withRecords(startOffset, Compression.NONE,
      (offsetCommitRecords ++ Seq(unknownRecord1, unknownRecord2) ++ Seq(groupMetadataRecord)).toArray: _*)

    expectGroupMetadataLoad(groupTopicPartition, startOffset, records)

    groupMetadataManager.loadGroupsAndOffsets(groupTopicPartition, 1, _ => (), 0L)

    val group = groupMetadataManager.getGroup(groupId).getOrElse(throw new AssertionError("Group was not loaded into the cache"))
    assertEquals(groupId, group.groupId)
    assertEquals(Stable, group.currentState)
    assertEquals(memberId, group.leaderOrNull)
    assertEquals(generation, group.generationId)
    assertEquals(Some(protocolType), group.protocolType)
    assertEquals(protocol, group.protocolName.orNull)
    assertEquals(Set(memberId), group.allMembers)
    assertEquals(committedOffsets.size, group.allOffsets.size)
    committedOffsets.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.committedOffset))
      assertTrue(group.offset(topicPartition).map(_.expireTimestampMs).get.isEmpty)
    }
  }

  @Test
  def testOffsetCommitKey(): Unit = {
    val bytes = GroupMetadataManager.offsetCommitKey(
      "foo",
      new TopicPartition("__consumer_offsets", 0)
    )
    val buffer = ByteBuffer.wrap(bytes)
    assertEquals(1.toShort, buffer.getShort)
    assertEquals(
      new OffsetCommitKey(new ByteBufferAccessor(buffer), 0.toShort),
      new OffsetCommitKey()
        .setGroup("foo")
        .setTopic("__consumer_offsets")
        .setPartition(0)
    )
  }

  @Test
  def testGroupMetadataKey(): Unit = {
    val bytes = GroupMetadataManager.groupMetadataKey("foo")
    val buffer = ByteBuffer.wrap(bytes)
    assertEquals(2.toShort, buffer.getShort())
    assertEquals(
      new GroupMetadataKeyData(new ByteBufferAccessor(buffer), 0.toShort),
      new GroupMetadataKeyData()
        .setGroup("foo")
    )
  }
}
