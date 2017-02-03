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

package kafka.coordinator

import java.nio.ByteBuffer

import kafka.api.ApiVersion
import kafka.cluster.Partition
import kafka.common.{OffsetAndMetadata, Topic}
import kafka.log.{Log, LogAppendInfo}
import kafka.server.{FetchDataInfo, KafkaConfig, LogOffsetMetadata, ReplicaManager}
import kafka.utils.{KafkaScheduler, MockTime, TestUtils, ZkUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.{FileRecords, MemoryRecords, Record, TimestampType}
import org.apache.kafka.common.requests.OffsetFetchResponse
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.easymock.{Capture, EasyMock, IAnswer}
import org.junit.{Before, Test}
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import kafka.utils.TestUtils.fail

import scala.collection._
import JavaConverters._

class GroupMetadataManagerTest {

  var time: MockTime = null
  var replicaManager: ReplicaManager = null
  var groupMetadataManager: GroupMetadataManager = null
  var scheduler: KafkaScheduler = null
  var zkUtils: ZkUtils = null
  var partition: Partition = null

  val groupId = "foo"
  val groupPartitionId = 0
  val protocolType = "protocolType"
  val rebalanceTimeout = 60000
  val sessionTimeout = 10000

  @Before
  def setUp() {
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
    val ret = mutable.Map[String, Map[Int, Seq[Int]]]()
    ret += (Topic.GroupMetadataTopicName -> Map(0 -> Seq(1), 1 -> Seq(1)))

    zkUtils = EasyMock.createNiceMock(classOf[ZkUtils])
    EasyMock.expect(zkUtils.getPartitionAssignmentForTopics(Seq(Topic.GroupMetadataTopicName))).andReturn(ret)
    EasyMock.replay(zkUtils)

    time = new MockTime
    replicaManager = EasyMock.createNiceMock(classOf[ReplicaManager])
    groupMetadataManager = new GroupMetadataManager(0, ApiVersion.latestVersion, offsetConfig, replicaManager, zkUtils, time)
    partition = EasyMock.niceMock(classOf[Partition])
  }

  @Test
  def testLoadOffsetsWithoutGroup() {
    val groupMetadataTopicPartition = new TopicPartition(Topic.GroupMetadataTopicName, groupPartitionId)
    val startOffset = 15L

    val committedOffsets = Map(
      new TopicPartition("foo", 0) -> 23L,
      new TopicPartition("foo", 1) -> 455L,
      new TopicPartition("bar", 0) -> 8992L
    )

    val offsetCommitRecords = createCommittedOffsetRecords(committedOffsets)
    val records = MemoryRecords.withRecords(startOffset, offsetCommitRecords: _*)
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
  def testLoadOffsetsWithTombstones() {
    val groupMetadataTopicPartition = new TopicPartition(Topic.GroupMetadataTopicName, groupPartitionId)
    val startOffset = 15L

    val tombstonePartition = new TopicPartition("foo", 1)
    val committedOffsets = Map(
      new TopicPartition("foo", 0) -> 23L,
      tombstonePartition -> 455L,
      new TopicPartition("bar", 0) -> 8992L
    )

    val offsetCommitRecords = createCommittedOffsetRecords(committedOffsets)
    val tombstone = Record.create(GroupMetadataManager.offsetCommitKey(groupId, tombstonePartition), null)
    val records = MemoryRecords.withRecords(startOffset, offsetCommitRecords ++ Seq(tombstone): _*)

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
  def testLoadOffsetsAndGroup() {
    val groupMetadataTopicPartition = new TopicPartition(Topic.GroupMetadataTopicName, groupPartitionId)
    val startOffset = 15L
    val committedOffsets = Map(
      new TopicPartition("foo", 0) -> 23L,
      new TopicPartition("foo", 1) -> 455L,
      new TopicPartition("bar", 0) -> 8992L
    )

    val offsetCommitRecords = createCommittedOffsetRecords(committedOffsets)
    val memberId = "98098230493"
    val groupMetadataRecord = buildStableGroupRecordWithMember(memberId)
    val records = MemoryRecords.withRecords(startOffset, offsetCommitRecords ++ Seq(groupMetadataRecord): _*)

    expectGroupMetadataLoad(groupMetadataTopicPartition, startOffset, records)

    EasyMock.replay(replicaManager)

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, _ => ())

    val group = groupMetadataManager.getGroup(groupId).getOrElse(fail("Group was not loaded into the cache"))
    assertEquals(groupId, group.groupId)
    assertEquals(Stable, group.currentState)
    assertEquals(memberId, group.leaderId)
    assertEquals(Set(memberId), group.allMembers)
    assertEquals(committedOffsets.size, group.allOffsets.size)
    committedOffsets.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.offset))
    }
  }

  @Test
  def testLoadGroupWithTombstone() {
    val groupMetadataTopicPartition = new TopicPartition(Topic.GroupMetadataTopicName, groupPartitionId)
    val startOffset = 15L

    val memberId = "98098230493"
    val groupMetadataRecord = buildStableGroupRecordWithMember(memberId)
    val groupMetadataTombstone = Record.create(GroupMetadataManager.groupMetadataKey(groupId), null)
    val records = MemoryRecords.withRecords(startOffset, Seq(groupMetadataRecord, groupMetadataTombstone): _*)

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

    val groupMetadataTopicPartition = new TopicPartition(Topic.GroupMetadataTopicName, groupPartitionId)
    val startOffset = 15L

    val committedOffsets = Map(
      new TopicPartition("foo", 0) -> 23L,
      new TopicPartition("foo", 1) -> 455L,
      new TopicPartition("bar", 0) -> 8992L
    )
    val offsetCommitRecords = createCommittedOffsetRecords(committedOffsets)
    val memberId = "98098230493"
    val groupMetadataRecord = buildStableGroupRecordWithMember(memberId)
    val groupMetadataTombstone = Record.create(GroupMetadataManager.groupMetadataKey(groupId), null)
    val records = MemoryRecords.withRecords(startOffset,
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
  def testAddGroup() {
    val group = new GroupMetadata("foo")
    assertEquals(group, groupMetadataManager.addGroup(group))
    assertEquals(group, groupMetadataManager.addGroup(new GroupMetadata("foo")))
  }

  @Test
  def testStoreEmptyGroup() {
    val group = new GroupMetadata(groupId)
    groupMetadataManager.addGroup(group)

    expectAppendMessage(Errors.NONE)
    EasyMock.replay(replicaManager)

    var maybeError: Option[Errors] = None
    def callback(error: Errors) {
      maybeError = Some(error)
    }

    val delayedStore = groupMetadataManager.prepareStoreGroup(group, Map.empty, callback).get
    groupMetadataManager.store(delayedStore)
    assertEquals(Some(Errors.NONE), maybeError)
  }

  @Test
  def testStoreGroupErrorMapping() {
    assertStoreGroupErrorMapping(Errors.NONE, Errors.NONE)
    assertStoreGroupErrorMapping(Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.GROUP_COORDINATOR_NOT_AVAILABLE)
    assertStoreGroupErrorMapping(Errors.NOT_ENOUGH_REPLICAS, Errors.GROUP_COORDINATOR_NOT_AVAILABLE)
    assertStoreGroupErrorMapping(Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND, Errors.GROUP_COORDINATOR_NOT_AVAILABLE)
    assertStoreGroupErrorMapping(Errors.NOT_LEADER_FOR_PARTITION, Errors.NOT_COORDINATOR_FOR_GROUP)
    assertStoreGroupErrorMapping(Errors.MESSAGE_TOO_LARGE, Errors.UNKNOWN)
    assertStoreGroupErrorMapping(Errors.RECORD_LIST_TOO_LARGE, Errors.UNKNOWN)
    assertStoreGroupErrorMapping(Errors.INVALID_FETCH_SIZE, Errors.UNKNOWN)
    assertStoreGroupErrorMapping(Errors.CORRUPT_MESSAGE, Errors.CORRUPT_MESSAGE)
  }

  private def assertStoreGroupErrorMapping(appendError: Errors, expectedError: Errors) {
    EasyMock.reset(replicaManager)

    val group = new GroupMetadata(groupId)
    groupMetadataManager.addGroup(group)

    expectAppendMessage(appendError)
    EasyMock.replay(replicaManager)

    var maybeError: Option[Errors] = None
    def callback(error: Errors) {
      maybeError = Some(error)
    }

    val delayedStore = groupMetadataManager.prepareStoreGroup(group, Map.empty, callback).get
    groupMetadataManager.store(delayedStore)
    assertEquals(Some(expectedError), maybeError)

    EasyMock.verify(replicaManager)
  }

  @Test
  def testStoreNonEmptyGroup() {
    val memberId = "memberId"
    val clientId = "clientId"
    val clientHost = "localhost"

    val group = new GroupMetadata(groupId)
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
    def callback(error: Errors) {
      maybeError = Some(error)
    }

    val delayedStore = groupMetadataManager.prepareStoreGroup(group, Map(memberId -> Array[Byte]()), callback).get
    groupMetadataManager.store(delayedStore)
    assertEquals(Some(Errors.NONE), maybeError)
  }

  @Test
  def testStoreNonEmptyGroupWhenCoordinatorHasMoved() {
    EasyMock.expect(replicaManager.getMagicAndTimestampType(EasyMock.anyObject())).andReturn(None)
    val memberId = "memberId"
    val clientId = "clientId"
    val clientHost = "localhost"

    val group = new GroupMetadata(groupId)

    val member = new MemberMetadata(memberId, groupId, clientId, clientHost, rebalanceTimeout, sessionTimeout,
      protocolType, List(("protocol", Array[Byte]())))
    member.awaitingJoinCallback = _ => ()
    group.add(member)
    group.transitionTo(PreparingRebalance)
    group.initNextGeneration()

    EasyMock.replay(replicaManager)

    var maybeError: Option[Errors] = None
    def callback(error: Errors) {
      maybeError = Some(error)
    }

    groupMetadataManager.prepareStoreGroup(group, Map(memberId -> Array[Byte]()), callback)
    assertEquals(Some(Errors.NOT_COORDINATOR_FOR_GROUP), maybeError)
    EasyMock.verify(replicaManager)
  }

  @Test
  def testCommitOffset() {
    val memberId = ""
    val generationId = -1
    val topicPartition = new TopicPartition("foo", 0)
    val offset = 37

    groupMetadataManager.addPartitionOwnership(groupPartitionId)

    val group = new GroupMetadata(groupId)
    groupMetadataManager.addGroup(group)

    val offsets = immutable.Map(topicPartition -> OffsetAndMetadata(offset))

    expectAppendMessage(Errors.NONE)
    EasyMock.replay(replicaManager)

    var commitErrors: Option[immutable.Map[TopicPartition, Short]] = None
    def callback(errors: immutable.Map[TopicPartition, Short]) {
      commitErrors = Some(errors)
    }

    val delayedStore = groupMetadataManager.prepareStoreOffsets(group, memberId, generationId, offsets, callback).get
    assertTrue(group.hasOffsets)

    groupMetadataManager.store(delayedStore)

    assertFalse(commitErrors.isEmpty)
    val maybeError = commitErrors.get.get(topicPartition)
    assertEquals(Some(Errors.NONE.code), maybeError)
    assertTrue(group.hasOffsets)

    val cachedOffsets = groupMetadataManager.getOffsets(groupId, Some(Seq(topicPartition)))
    val maybePartitionResponse = cachedOffsets.get(topicPartition)
    assertFalse(maybePartitionResponse.isEmpty)

    val partitionResponse = maybePartitionResponse.get
    assertEquals(Errors.NONE, partitionResponse.error)
    assertEquals(offset, partitionResponse.offset)
  }

  @Test
  def testCommitOffsetWhenCoordinatorHasMoved() {
    EasyMock.expect(replicaManager.getMagicAndTimestampType(EasyMock.anyObject())).andReturn(None)
    val memberId = ""
    val generationId = -1
    val topicPartition = new TopicPartition("foo", 0)
    val offset = 37

    groupMetadataManager.addPartitionOwnership(groupPartitionId)

    val group = new GroupMetadata(groupId)
    groupMetadataManager.addGroup(group)

    val offsets = immutable.Map(topicPartition -> OffsetAndMetadata(offset))

    EasyMock.replay(replicaManager)

    var commitErrors: Option[immutable.Map[TopicPartition, Short]] = None
    def callback(errors: immutable.Map[TopicPartition, Short]) {
      commitErrors = Some(errors)
    }

    groupMetadataManager.prepareStoreOffsets(group, memberId, generationId, offsets, callback)

    assertFalse(commitErrors.isEmpty)
    val maybeError = commitErrors.get.get(topicPartition)
    assertEquals(Some(Errors.NOT_COORDINATOR_FOR_GROUP.code), maybeError)
    EasyMock.verify(replicaManager)
  }

  @Test
  def testCommitOffsetFailure() {
    assertCommitOffsetErrorMapping(Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.GROUP_COORDINATOR_NOT_AVAILABLE)
    assertCommitOffsetErrorMapping(Errors.NOT_ENOUGH_REPLICAS, Errors.GROUP_COORDINATOR_NOT_AVAILABLE)
    assertCommitOffsetErrorMapping(Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND, Errors.GROUP_COORDINATOR_NOT_AVAILABLE)
    assertCommitOffsetErrorMapping(Errors.NOT_LEADER_FOR_PARTITION, Errors.NOT_COORDINATOR_FOR_GROUP)
    assertCommitOffsetErrorMapping(Errors.MESSAGE_TOO_LARGE, Errors.INVALID_COMMIT_OFFSET_SIZE)
    assertCommitOffsetErrorMapping(Errors.RECORD_LIST_TOO_LARGE, Errors.INVALID_COMMIT_OFFSET_SIZE)
    assertCommitOffsetErrorMapping(Errors.INVALID_FETCH_SIZE, Errors.INVALID_COMMIT_OFFSET_SIZE)
    assertCommitOffsetErrorMapping(Errors.CORRUPT_MESSAGE, Errors.CORRUPT_MESSAGE)
  }

  private def assertCommitOffsetErrorMapping(appendError: Errors, expectedError: Errors): Unit = {
    EasyMock.reset(replicaManager)

    val memberId = ""
    val generationId = -1
    val topicPartition = new TopicPartition("foo", 0)
    val offset = 37

    groupMetadataManager.addPartitionOwnership(groupPartitionId)

    val group = new GroupMetadata(groupId)
    groupMetadataManager.addGroup(group)

    val offsets = immutable.Map(topicPartition -> OffsetAndMetadata(offset))

    expectAppendMessage(appendError)
    EasyMock.replay(replicaManager)

    var commitErrors: Option[immutable.Map[TopicPartition, Short]] = None
    def callback(errors: immutable.Map[TopicPartition, Short]) {
      commitErrors = Some(errors)
    }

    val delayedStore = groupMetadataManager.prepareStoreOffsets(group, memberId, generationId, offsets, callback).get
    assertTrue(group.hasOffsets)

    groupMetadataManager.store(delayedStore)

    assertFalse(commitErrors.isEmpty)
    val maybeError = commitErrors.get.get(topicPartition)
    assertEquals(Some(expectedError.code), maybeError)
    assertFalse(group.hasOffsets)

    val cachedOffsets = groupMetadataManager.getOffsets(groupId, Some(Seq(topicPartition)))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(topicPartition).map(_.offset))

    EasyMock.verify(replicaManager)
  }

  @Test
  def testExpireOffset() {
    val memberId = ""
    val generationId = -1
    val topicPartition1 = new TopicPartition("foo", 0)
    val topicPartition2 = new TopicPartition("foo", 1)
    val offset = 37

    groupMetadataManager.addPartitionOwnership(groupPartitionId)

    val group = new GroupMetadata(groupId)
    groupMetadataManager.addGroup(group)

    // expire the offset after 1 millisecond
    val startMs = time.milliseconds
    val offsets = immutable.Map(
      topicPartition1 -> OffsetAndMetadata(offset, "", startMs, startMs + 1),
      topicPartition2 -> OffsetAndMetadata(offset, "", startMs, startMs + 3))

    EasyMock.expect(replicaManager.getPartition(new TopicPartition(Topic.GroupMetadataTopicName, groupPartitionId))).andStubReturn(Some(partition))
    expectAppendMessage(Errors.NONE)
    EasyMock.replay(replicaManager)

    var commitErrors: Option[immutable.Map[TopicPartition, Short]] = None
    def callback(errors: immutable.Map[TopicPartition, Short]) {
      commitErrors = Some(errors)
    }

    val delayedStore = groupMetadataManager.prepareStoreOffsets(group, memberId, generationId, offsets, callback).get
    assertTrue(group.hasOffsets)

    groupMetadataManager.store(delayedStore)
    assertFalse(commitErrors.isEmpty)
    assertEquals(Some(Errors.NONE.code), commitErrors.get.get(topicPartition1))

    // expire only one of the offsets
    time.sleep(2)

    EasyMock.reset(partition)
    EasyMock.expect(partition.appendRecordsToLeader(EasyMock.anyObject(classOf[MemoryRecords]), EasyMock.anyInt()))
      .andReturn(LogAppendInfo.UnknownLogAppendInfo)
    EasyMock.replay(partition)

    groupMetadataManager.cleanupGroupMetadata()

    assertEquals(Some(group), groupMetadataManager.getGroup(groupId))
    assertEquals(None, group.offset(topicPartition1))
    assertEquals(Some(offset), group.offset(topicPartition2).map(_.offset))

    val cachedOffsets = groupMetadataManager.getOffsets(groupId, Some(Seq(topicPartition1, topicPartition2)))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(topicPartition1).map(_.offset))
    assertEquals(Some(offset), cachedOffsets.get(topicPartition2).map(_.offset))
  }

  @Test
  def testGroupMetadataRemoval() {
    val topicPartition1 = new TopicPartition("foo", 0)
    val topicPartition2 = new TopicPartition("foo", 1)

    groupMetadataManager.addPartitionOwnership(groupPartitionId)

    val group = new GroupMetadata(groupId)
    groupMetadataManager.addGroup(group)
    group.generationId = 5

    // expect the group metadata tombstone
    EasyMock.reset(partition)
    val recordsCapture: Capture[MemoryRecords] = EasyMock.newCapture()

    EasyMock.expect(replicaManager.getMagicAndTimestampType(EasyMock.anyObject()))
      .andStubReturn(Some(Record.MAGIC_VALUE_V1, TimestampType.CREATE_TIME))
    EasyMock.expect(replicaManager.getPartition(new TopicPartition(Topic.GroupMetadataTopicName, groupPartitionId))).andStubReturn(Some(partition))
    EasyMock.expect(partition.appendRecordsToLeader(EasyMock.capture(recordsCapture), EasyMock.anyInt()))
      .andReturn(LogAppendInfo.UnknownLogAppendInfo)
    EasyMock.replay(replicaManager, partition)

    groupMetadataManager.cleanupGroupMetadata()

    assertTrue(recordsCapture.hasCaptured)

    val records = recordsCapture.getValue.records.asScala.toList
    assertEquals(1, records.size)

    val metadataTombstone = records.head
    assertTrue(metadataTombstone.hasKey)
    assertTrue(metadataTombstone.hasNullValue)
    assertEquals(Record.MAGIC_VALUE_V1, metadataTombstone.magic)
    assertEquals(TimestampType.CREATE_TIME, metadataTombstone.timestampType)
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
  def testGroupMetadataRemovalWithLogAppendTime() {
    val topicPartition1 = new TopicPartition("foo", 0)
    val topicPartition2 = new TopicPartition("foo", 1)

    groupMetadataManager.addPartitionOwnership(groupPartitionId)

    val group = new GroupMetadata(groupId)
    groupMetadataManager.addGroup(group)
    group.generationId = 5

    // expect the group metadata tombstone
    EasyMock.reset(partition)
    val recordsCapture: Capture[MemoryRecords] = EasyMock.newCapture()

    EasyMock.expect(replicaManager.getMagicAndTimestampType(EasyMock.anyObject()))
      .andStubReturn(Some(Record.MAGIC_VALUE_V1, TimestampType.LOG_APPEND_TIME))
    EasyMock.expect(replicaManager.getPartition(new TopicPartition(Topic.GroupMetadataTopicName, groupPartitionId))).andStubReturn(Some(partition))
    EasyMock.expect(partition.appendRecordsToLeader(EasyMock.capture(recordsCapture), EasyMock.anyInt()))
      .andReturn(LogAppendInfo.UnknownLogAppendInfo)
    EasyMock.replay(replicaManager, partition)

    groupMetadataManager.cleanupGroupMetadata()

    assertTrue(recordsCapture.hasCaptured)

    val records = recordsCapture.getValue.records.asScala.toList
    assertEquals(1, records.size)

    val metadataTombstone = records.head
    assertTrue(metadataTombstone.hasKey)
    assertTrue(metadataTombstone.hasNullValue)
    assertEquals(Record.MAGIC_VALUE_V1, metadataTombstone.magic)
    assertEquals(TimestampType.LOG_APPEND_TIME, metadataTombstone.timestampType)
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
  def testExpireGroupWithOffsetsOnly() {
    // verify that the group is removed properly, but no tombstone is written if
    // this is a group which is only using kafka for offset storage

    val memberId = ""
    val generationId = -1
    val topicPartition1 = new TopicPartition("foo", 0)
    val topicPartition2 = new TopicPartition("foo", 1)
    val offset = 37

    groupMetadataManager.addPartitionOwnership(groupPartitionId)

    val group = new GroupMetadata(groupId)
    groupMetadataManager.addGroup(group)

    // expire the offset after 1 millisecond
    val startMs = time.milliseconds
    val offsets = immutable.Map(
      topicPartition1 -> OffsetAndMetadata(offset, "", startMs, startMs + 1),
      topicPartition2 -> OffsetAndMetadata(offset, "", startMs, startMs + 3))

    EasyMock.expect(replicaManager.getPartition(new TopicPartition(Topic.GroupMetadataTopicName, groupPartitionId))).andStubReturn(Some(partition))
    expectAppendMessage(Errors.NONE)
    EasyMock.replay(replicaManager)

    var commitErrors: Option[immutable.Map[TopicPartition, Short]] = None
    def callback(errors: immutable.Map[TopicPartition, Short]) {
      commitErrors = Some(errors)
    }

    val delayedStore = groupMetadataManager.prepareStoreOffsets(group, memberId, generationId, offsets, callback).get
    assertTrue(group.hasOffsets)

    groupMetadataManager.store(delayedStore)
    assertFalse(commitErrors.isEmpty)
    assertEquals(Some(Errors.NONE.code), commitErrors.get.get(topicPartition1))

    // expire all of the offsets
    time.sleep(4)

    // expect the offset tombstone
    EasyMock.reset(partition)
    val recordsCapture: Capture[MemoryRecords] = EasyMock.newCapture()

    EasyMock.expect(partition.appendRecordsToLeader(EasyMock.capture(recordsCapture), EasyMock.anyInt()))
      .andReturn(LogAppendInfo.UnknownLogAppendInfo)
    EasyMock.replay(partition)

    groupMetadataManager.cleanupGroupMetadata()

    assertTrue(recordsCapture.hasCaptured)

    // verify the tombstones are correct and only for the expired offsets
    val records = recordsCapture.getValue.records.asScala.toList
    assertEquals(2, records.size)
    records.foreach { message =>
      assertTrue(message.hasKey)
      assertTrue(message.hasNullValue)
      val offsetKey = GroupMetadataManager.readMessageKey(message.key).asInstanceOf[OffsetKey]
      assertEquals(groupId, offsetKey.key.group)
      assertEquals("foo", offsetKey.key.topicPartition.topic)
    }

    // the full group should be gone since all offsets were removed
    assertEquals(None, groupMetadataManager.getGroup(groupId))
    val cachedOffsets = groupMetadataManager.getOffsets(groupId, Some(Seq(topicPartition1, topicPartition2)))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(topicPartition1).map(_.offset))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(topicPartition2).map(_.offset))
  }

  @Test
  def testExpireOffsetsWithActiveGroup() {
    val memberId = "memberId"
    val clientId = "clientId"
    val clientHost = "localhost"
    val topicPartition1 = new TopicPartition("foo", 0)
    val topicPartition2 = new TopicPartition("foo", 1)
    val offset = 37

    groupMetadataManager.addPartitionOwnership(groupPartitionId)

    val group = new GroupMetadata(groupId)
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

    EasyMock.expect(replicaManager.getPartition(new TopicPartition(Topic.GroupMetadataTopicName, groupPartitionId))).andStubReturn(Some(partition))
    expectAppendMessage(Errors.NONE)
    EasyMock.replay(replicaManager)

    var commitErrors: Option[immutable.Map[TopicPartition, Short]] = None
    def callback(errors: immutable.Map[TopicPartition, Short]) {
      commitErrors = Some(errors)
    }

    val delayedStore = groupMetadataManager.prepareStoreOffsets(group, memberId, group.generationId, offsets, callback).get
    assertTrue(group.hasOffsets)

    groupMetadataManager.store(delayedStore)
    assertFalse(commitErrors.isEmpty)
    assertEquals(Some(Errors.NONE.code), commitErrors.get.get(topicPartition1))

    // expire all of the offsets
    time.sleep(4)

    // expect the offset tombstone
    EasyMock.reset(partition)
    EasyMock.expect(partition.appendRecordsToLeader(EasyMock.anyObject(classOf[MemoryRecords]), EasyMock.anyInt()))
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
  }

  private def expectAppendMessage(error: Errors) {
    val capturedArgument: Capture[Map[TopicPartition, PartitionResponse] => Unit] = EasyMock.newCapture()
    EasyMock.expect(replicaManager.appendRecords(EasyMock.anyLong(),
      EasyMock.anyShort(),
      EasyMock.anyBoolean(),
      EasyMock.anyObject().asInstanceOf[Map[TopicPartition, MemoryRecords]],
      EasyMock.capture(capturedArgument))).andAnswer(new IAnswer[Unit] {
      override def answer = capturedArgument.getValue.apply(
        Map(new TopicPartition(Topic.GroupMetadataTopicName, groupPartitionId) ->
          new PartitionResponse(error, 0L, Record.NO_TIMESTAMP)
        )
      )})
    EasyMock.expect(replicaManager.getMagicAndTimestampType(EasyMock.anyObject()))
      .andStubReturn(Some(Record.MAGIC_VALUE_V1, TimestampType.CREATE_TIME))
  }

  private def buildStableGroupRecordWithMember(memberId: String): Record = {
    val group = new GroupMetadata(groupId)
    group.transitionTo(PreparingRebalance)
    val memberProtocols = List(("roundrobin", Array.emptyByteArray))
    val member = new MemberMetadata(memberId, groupId, "clientId", "clientHost", 30000, 10000, "consumer", memberProtocols)
    group.add(member)
    member.awaitingJoinCallback = _ => {}
    group.initNextGeneration()
    group.transitionTo(Stable)

    val groupMetadataKey = GroupMetadataManager.groupMetadataKey(groupId)
    val groupMetadataValue = GroupMetadataManager.groupMetadataValue(group, Map(memberId -> Array.empty[Byte]))
    Record.create(groupMetadataKey, groupMetadataValue)
  }

  private def expectGroupMetadataLoad(groupMetadataTopicPartition: TopicPartition,
                                      startOffset: Long,
                                      records: MemoryRecords): Unit = {
    val endOffset = startOffset + records.deepEntries.asScala.size
    val logMock =  EasyMock.mock(classOf[Log])
    val fileRecordsMock = EasyMock.mock(classOf[FileRecords])

    EasyMock.expect(replicaManager.getLog(groupMetadataTopicPartition)).andStubReturn(Some(logMock))
    EasyMock.expect(logMock.logStartOffset).andStubReturn(startOffset)
    EasyMock.expect(replicaManager.getHighWatermark(groupMetadataTopicPartition)).andStubReturn(Some(endOffset))
    EasyMock.expect(logMock.read(EasyMock.eq(startOffset), EasyMock.anyInt(), EasyMock.eq(None), EasyMock.eq(true)))
      .andReturn(FetchDataInfo(LogOffsetMetadata(startOffset), fileRecordsMock))
    EasyMock.expect(fileRecordsMock.readInto(EasyMock.anyObject(classOf[ByteBuffer]), EasyMock.anyInt()))
      .andReturn(records.buffer)

    EasyMock.replay(logMock, fileRecordsMock)
  }

  private def createCommittedOffsetRecords(committedOffsets: Map[TopicPartition, Long],
                                           groupId: String = groupId): Seq[Record] = {
    committedOffsets.map { case (topicPartition, offset) =>
      val offsetAndMetadata = OffsetAndMetadata(offset)
      val offsetCommitKey = GroupMetadataManager.offsetCommitKey(groupId, topicPartition)
      val offsetCommitValue = GroupMetadataManager.offsetCommitValue(offsetAndMetadata)
      Record.create(offsetCommitKey, offsetCommitValue)
    }.toSeq
  }

}
