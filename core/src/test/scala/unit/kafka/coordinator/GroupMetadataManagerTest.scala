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

import kafka.api.ApiVersion
import kafka.cluster.Partition
import kafka.common.{OffsetAndMetadata, Topic}
import kafka.log.LogAppendInfo
import kafka.message.{ByteBufferMessageSet, Message, MessageSet}
import kafka.server.{KafkaConfig, ReplicaManager}
import kafka.utils.{KafkaScheduler, MockTime, TestUtils, ZkUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.Record
import org.apache.kafka.common.requests.OffsetFetchResponse
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.easymock.{Capture, EasyMock, IAnswer}
import org.junit.{After, Before, Test}
import org.junit.Assert._

import scala.collection._

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

  @After
  def tearDown() {
    EasyMock.reset(replicaManager)
    EasyMock.reset(partition)
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

    val delayedStore = groupMetadataManager.prepareStoreGroup(group, Map.empty, callback)
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

    val delayedStore = groupMetadataManager.prepareStoreGroup(group, Map.empty, callback)
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
    member.awaitingJoinCallback = (joinGroupResult: JoinGroupResult) => {}
    group.add(memberId, member)
    group.transitionTo(PreparingRebalance)
    group.initNextGeneration()

    expectAppendMessage(Errors.NONE)
    EasyMock.replay(replicaManager)

    var maybeError: Option[Errors] = None
    def callback(error: Errors) {
      maybeError = Some(error)
    }

    val delayedStore = groupMetadataManager.prepareStoreGroup(group, Map(memberId -> Array[Byte]()), callback)
    groupMetadataManager.store(delayedStore)
    assertEquals(Some(Errors.NONE), maybeError)
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

    val delayedStore = groupMetadataManager.prepareStoreOffsets(group, memberId, generationId, offsets, callback)
    assertTrue(group.hasOffsets)

    groupMetadataManager.store(delayedStore)

    assertFalse(commitErrors.isEmpty)
    val maybeError = commitErrors.get.get(topicPartition)
    assertEquals(Some(Errors.NONE.code), maybeError)
    assertTrue(group.hasOffsets)

    val cachedOffsets = groupMetadataManager.getOffsets(groupId, Seq(topicPartition))
    val maybePartitionResponse = cachedOffsets.get(topicPartition)
    assertFalse(maybePartitionResponse.isEmpty)

    val partitionResponse = maybePartitionResponse.get
    assertEquals(Errors.NONE.code, partitionResponse.errorCode)
    assertEquals(offset, partitionResponse.offset)
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

    val delayedStore = groupMetadataManager.prepareStoreOffsets(group, memberId, generationId, offsets, callback)
    assertTrue(group.hasOffsets)

    groupMetadataManager.store(delayedStore)

    assertFalse(commitErrors.isEmpty)
    val maybeError = commitErrors.get.get(topicPartition)
    assertEquals(Some(expectedError.code), maybeError)
    assertFalse(group.hasOffsets)

    val cachedOffsets = groupMetadataManager.getOffsets(groupId, Seq(topicPartition))
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

    EasyMock.expect(replicaManager.getPartition(Topic.GroupMetadataTopicName, groupPartitionId)).andStubReturn(Some(partition))
    expectAppendMessage(Errors.NONE)
    EasyMock.replay(replicaManager)

    var commitErrors: Option[immutable.Map[TopicPartition, Short]] = None
    def callback(errors: immutable.Map[TopicPartition, Short]) {
      commitErrors = Some(errors)
    }

    val delayedStore = groupMetadataManager.prepareStoreOffsets(group, memberId, generationId, offsets, callback)
    assertTrue(group.hasOffsets)

    groupMetadataManager.store(delayedStore)
    assertFalse(commitErrors.isEmpty)
    assertEquals(Some(Errors.NONE.code), commitErrors.get.get(topicPartition1))

    // expire only one of the offsets
    time.sleep(2)

    EasyMock.reset(partition)
    EasyMock.expect(partition.appendMessagesToLeader(EasyMock.anyObject(classOf[ByteBufferMessageSet]), EasyMock.anyInt()))
      .andReturn(LogAppendInfo.UnknownLogAppendInfo)
    EasyMock.replay(partition)

    groupMetadataManager.cleanupGroupMetadata()

    assertEquals(Some(group), groupMetadataManager.getGroup(groupId))
    assertEquals(None, group.offset(topicPartition1))
    assertEquals(Some(offset), group.offset(topicPartition2).map(_.offset))

    val cachedOffsets = groupMetadataManager.getOffsets(groupId, Seq(topicPartition1, topicPartition2))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(topicPartition1).map(_.offset))
    assertEquals(Some(offset), cachedOffsets.get(topicPartition2).map(_.offset))
  }

  @Test
  def testExpireGroup() {
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

    EasyMock.expect(replicaManager.getPartition(Topic.GroupMetadataTopicName, groupPartitionId)).andStubReturn(Some(partition))
    expectAppendMessage(Errors.NONE)
    EasyMock.replay(replicaManager)

    var commitErrors: Option[immutable.Map[TopicPartition, Short]] = None
    def callback(errors: immutable.Map[TopicPartition, Short]) {
      commitErrors = Some(errors)
    }

    val delayedStore = groupMetadataManager.prepareStoreOffsets(group, memberId, generationId, offsets, callback)
    assertTrue(group.hasOffsets)

    groupMetadataManager.store(delayedStore)
    assertFalse(commitErrors.isEmpty)
    assertEquals(Some(Errors.NONE.code), commitErrors.get.get(topicPartition1))

    // expire all of the offsets
    time.sleep(4)

    // expect the offset tombstone
    EasyMock.reset(partition)
    EasyMock.expect(partition.appendMessagesToLeader(EasyMock.anyObject(classOf[ByteBufferMessageSet]), EasyMock.anyInt()))
      .andReturn(LogAppendInfo.UnknownLogAppendInfo)
    EasyMock.replay(partition)

    groupMetadataManager.cleanupGroupMetadata()

    // the full group should be gone since all offsets were removed
    assertEquals(None, groupMetadataManager.getGroup(groupId))
    val cachedOffsets = groupMetadataManager.getOffsets(groupId, Seq(topicPartition1, topicPartition2))
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
    member.awaitingJoinCallback = (joinGroupResult: JoinGroupResult) => {}
    group.add(memberId, member)
    group.transitionTo(PreparingRebalance)
    group.initNextGeneration()

    // expire the offset after 1 millisecond
    val startMs = time.milliseconds
    val offsets = immutable.Map(
      topicPartition1 -> OffsetAndMetadata(offset, "", startMs, startMs + 1),
      topicPartition2 -> OffsetAndMetadata(offset, "", startMs, startMs + 3))

    EasyMock.expect(replicaManager.getPartition(Topic.GroupMetadataTopicName, groupPartitionId)).andStubReturn(Some(partition))
    expectAppendMessage(Errors.NONE)
    EasyMock.replay(replicaManager)

    var commitErrors: Option[immutable.Map[TopicPartition, Short]] = None
    def callback(errors: immutable.Map[TopicPartition, Short]) {
      commitErrors = Some(errors)
    }

    val delayedStore = groupMetadataManager.prepareStoreOffsets(group, memberId, group.generationId, offsets, callback)
    assertTrue(group.hasOffsets)

    groupMetadataManager.store(delayedStore)
    assertFalse(commitErrors.isEmpty)
    assertEquals(Some(Errors.NONE.code), commitErrors.get.get(topicPartition1))

    // expire all of the offsets
    time.sleep(4)

    // expect the offset tombstone
    EasyMock.reset(partition)
    EasyMock.expect(partition.appendMessagesToLeader(EasyMock.anyObject(classOf[ByteBufferMessageSet]), EasyMock.anyInt()))
      .andReturn(LogAppendInfo.UnknownLogAppendInfo)
    EasyMock.replay(partition)

    groupMetadataManager.cleanupGroupMetadata()

    // group should still be there, but the offsets should be gone
    assertEquals(Some(group), groupMetadataManager.getGroup(groupId))
    assertEquals(None, group.offset(topicPartition1))
    assertEquals(None, group.offset(topicPartition2))

    val cachedOffsets = groupMetadataManager.getOffsets(groupId, Seq(topicPartition1, topicPartition2))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(topicPartition1).map(_.offset))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(topicPartition2).map(_.offset))
  }

  private def expectAppendMessage(error: Errors) {
    val capturedArgument: Capture[Map[TopicPartition, PartitionResponse] => Unit] = EasyMock.newCapture()
    EasyMock.expect(replicaManager.appendMessages(EasyMock.anyLong(),
      EasyMock.anyShort(),
      EasyMock.anyBoolean(),
      EasyMock.anyObject().asInstanceOf[Map[TopicPartition, MessageSet]],
      EasyMock.capture(capturedArgument))).andAnswer(new IAnswer[Unit] {
      override def answer = capturedArgument.getValue.apply(
        Map(new TopicPartition(Topic.GroupMetadataTopicName, groupPartitionId) ->
          new PartitionResponse(error.code, 0L, Record.NO_TIMESTAMP)
        )
      )})
    EasyMock.expect(replicaManager.getMessageFormatVersion(EasyMock.anyObject())).andStubReturn(Some(Message.MagicValue_V1))
  }


}
