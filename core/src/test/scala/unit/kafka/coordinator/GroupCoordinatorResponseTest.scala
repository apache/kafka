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

import kafka.utils.timer.MockTimer
import org.apache.kafka.common.record.Record
import org.junit.Assert._
import kafka.common.{OffsetAndMetadata, Topic}
import kafka.message.{Message, MessageSet}
import kafka.server.{DelayedOperationPurgatory, ReplicaManager, KafkaConfig}
import kafka.utils._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{OffsetCommitRequest, JoinGroupRequest}
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.easymock.{Capture, IAnswer, EasyMock}
import org.junit.{After, Before, Test}
import org.scalatest.junit.JUnitSuite
import java.util.concurrent.TimeUnit
import scala.collection._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}

/**
 * Test GroupCoordinator responses
 */
class GroupCoordinatorResponseTest extends JUnitSuite {
  type JoinGroupCallback = JoinGroupResult => Unit
  type SyncGroupCallbackParams = (Array[Byte], Short)
  type SyncGroupCallback = (Array[Byte], Short) => Unit
  type HeartbeatCallbackParams = Short
  type HeartbeatCallback = Short => Unit
  type CommitOffsetCallbackParams = Map[TopicPartition, Short]
  type CommitOffsetCallback = Map[TopicPartition, Short] => Unit
  type LeaveGroupCallbackParams = Short
  type LeaveGroupCallback = Short => Unit

  val ClientId = "consumer-test"
  val ClientHost = "localhost"
  val ConsumerMinSessionTimeout = 10
  val ConsumerMaxSessionTimeout = 1000
  val DefaultRebalanceTimeout = 500
  val DefaultSessionTimeout = 500
  var timer: MockTimer = null
  var groupCoordinator: GroupCoordinator = null
  var replicaManager: ReplicaManager = null
  var scheduler: KafkaScheduler = null
  var zkUtils: ZkUtils = null

  private val groupId = "groupId"
  private val protocolType = "consumer"
  private val memberId = "memberId"
  private val metadata = Array[Byte]()
  private val protocols = List(("range", metadata))
  private var groupPartitionId: Int = -1

  // we use this string value since its hashcode % #.partitions is different
  private val otherGroupId = "otherGroup"

  @Before
  def setUp() {
    val props = TestUtils.createBrokerConfig(nodeId = 0, zkConnect = "")
    props.setProperty(KafkaConfig.GroupMinSessionTimeoutMsProp, ConsumerMinSessionTimeout.toString)
    props.setProperty(KafkaConfig.GroupMaxSessionTimeoutMsProp, ConsumerMaxSessionTimeout.toString)

    // make two partitions of the group topic to make sure some partitions are not owned by the coordinator
    val ret = mutable.Map[String, Map[Int, Seq[Int]]]()
    ret += (Topic.GroupMetadataTopicName -> Map(0 -> Seq(1), 1 -> Seq(1)))

    replicaManager = EasyMock.createNiceMock(classOf[ReplicaManager])

    zkUtils = EasyMock.createNiceMock(classOf[ZkUtils])
    EasyMock.expect(zkUtils.getPartitionAssignmentForTopics(Seq(Topic.GroupMetadataTopicName))).andReturn(ret)
    EasyMock.replay(zkUtils)

    timer = new MockTimer

    val config = KafkaConfig.fromProps(props)

    val heartbeatPurgatory = new DelayedOperationPurgatory[DelayedHeartbeat]("Heartbeat", timer, config.brokerId, reaperEnabled = false)
    val joinPurgatory = new DelayedOperationPurgatory[DelayedJoin]("Rebalance", timer, config.brokerId, reaperEnabled = false)

    groupCoordinator = GroupCoordinator(config, zkUtils, replicaManager, heartbeatPurgatory, joinPurgatory, timer.time)
    groupCoordinator.startup(false)

    // add the partition into the owned partition list
    groupPartitionId = groupCoordinator.partitionFor(groupId)
    groupCoordinator.groupManager.addPartitionOwnership(groupPartitionId)
  }

  @After
  def tearDown() {
    EasyMock.reset(replicaManager)
    if (groupCoordinator != null)
      groupCoordinator.shutdown()
  }

  @Test
  def testJoinGroupWrongCoordinator() {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val joinGroupResult = joinGroup(otherGroupId, memberId, protocolType, protocols)
    val joinGroupErrorCode = joinGroupResult.errorCode
    assertEquals(Errors.NOT_COORDINATOR_FOR_GROUP.code, joinGroupErrorCode)
  }

  @Test
  def testJoinGroupSessionTimeoutTooSmall() {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val joinGroupResult = joinGroup(groupId, memberId, protocolType, protocols, sessionTimeout = ConsumerMinSessionTimeout - 1)
    val joinGroupErrorCode = joinGroupResult.errorCode
    assertEquals(Errors.INVALID_SESSION_TIMEOUT.code, joinGroupErrorCode)
  }

  @Test
  def testJoinGroupSessionTimeoutTooLarge() {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val joinGroupResult = joinGroup(groupId, memberId, protocolType, protocols, sessionTimeout = ConsumerMaxSessionTimeout + 1)
    val joinGroupErrorCode = joinGroupResult.errorCode
    assertEquals(Errors.INVALID_SESSION_TIMEOUT.code, joinGroupErrorCode)
  }

  @Test
  def testJoinGroupUnknownConsumerNewGroup() {
    val joinGroupResult = joinGroup(groupId, memberId, protocolType, protocols)
    val joinGroupErrorCode = joinGroupResult.errorCode
    assertEquals(Errors.UNKNOWN_MEMBER_ID.code, joinGroupErrorCode)
  }

  @Test
  def testInvalidGroupId() {
    val groupId = ""
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val joinGroupResult = joinGroup(groupId, memberId, protocolType, protocols)
    assertEquals(Errors.INVALID_GROUP_ID.code, joinGroupResult.errorCode)
  }

  @Test
  def testValidJoinGroup() {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val joinGroupResult = joinGroup(groupId, memberId, protocolType, protocols)
    val joinGroupErrorCode = joinGroupResult.errorCode
    assertEquals(Errors.NONE.code, joinGroupErrorCode)
  }

  @Test
  def testJoinGroupInconsistentProtocolType() {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val otherMemberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val joinGroupResult = joinGroup(groupId, memberId, protocolType, protocols)
    assertEquals(Errors.NONE.code, joinGroupResult.errorCode)

    EasyMock.reset(replicaManager)
    val otherJoinGroupResult = joinGroup(groupId, otherMemberId, "connect", protocols)
    assertEquals(Errors.INCONSISTENT_GROUP_PROTOCOL.code, otherJoinGroupResult.errorCode)
  }

  @Test
  def testJoinGroupInconsistentGroupProtocol() {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val otherMemberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val joinGroupResult = joinGroup(groupId, memberId, protocolType, List(("range", metadata)))
    assertEquals(Errors.NONE.code, joinGroupResult.errorCode)

    EasyMock.reset(replicaManager)
    val otherJoinGroupResult = joinGroup(groupId, otherMemberId, protocolType, List(("roundrobin", metadata)))
    assertEquals(Errors.INCONSISTENT_GROUP_PROTOCOL.code, otherJoinGroupResult.errorCode)
  }

  @Test
  def testJoinGroupUnknownConsumerExistingGroup() {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val otherMemberId = "memberId"

    val joinGroupResult = joinGroup(groupId, memberId, protocolType, protocols)
    assertEquals(Errors.NONE.code, joinGroupResult.errorCode)

    EasyMock.reset(replicaManager)
    val otherJoinGroupResult = joinGroup(groupId, otherMemberId, protocolType, protocols)
    assertEquals(Errors.UNKNOWN_MEMBER_ID.code, otherJoinGroupResult.errorCode)
  }

  @Test
  def testHeartbeatWrongCoordinator() {

    val heartbeatResult = heartbeat(otherGroupId, memberId, -1)
    assertEquals(Errors.NOT_COORDINATOR_FOR_GROUP.code, heartbeatResult)
  }

  @Test
  def testHeartbeatUnknownGroup() {

    val heartbeatResult = heartbeat(groupId, memberId, -1)
    assertEquals(Errors.UNKNOWN_MEMBER_ID.code, heartbeatResult)
  }

  @Test
  def testHeartbeatUnknownConsumerExistingGroup() {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val otherMemberId = "memberId"

    val joinGroupResult = joinGroup(groupId, memberId, protocolType, protocols)
    val assignedMemberId = joinGroupResult.memberId
    val joinGroupErrorCode = joinGroupResult.errorCode
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    EasyMock.reset(replicaManager)
    val syncGroupResult = syncGroupLeader(groupId, joinGroupResult.generationId, assignedMemberId, Map(assignedMemberId -> Array[Byte]()))
    val syncGroupErrorCode = syncGroupResult._2
    assertEquals(Errors.NONE.code, syncGroupErrorCode)

    EasyMock.reset(replicaManager)
    val heartbeatResult = heartbeat(groupId, otherMemberId, 1)
    assertEquals(Errors.UNKNOWN_MEMBER_ID.code, heartbeatResult)
  }

  @Test
  def testHeartbeatRebalanceInProgress() {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val joinGroupResult = joinGroup(groupId, memberId, protocolType, protocols)
    val assignedMemberId = joinGroupResult.memberId
    val joinGroupErrorCode = joinGroupResult.errorCode
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    EasyMock.reset(replicaManager)
    val heartbeatResult = heartbeat(groupId, assignedMemberId, 2)
    assertEquals(Errors.REBALANCE_IN_PROGRESS.code, heartbeatResult)
  }

  @Test
  def testHeartbeatIllegalGeneration() {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val joinGroupResult = joinGroup(groupId, memberId, protocolType, protocols)
    val assignedMemberId = joinGroupResult.memberId
    val joinGroupErrorCode = joinGroupResult.errorCode
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    EasyMock.reset(replicaManager)
    val syncGroupResult = syncGroupLeader(groupId, joinGroupResult.generationId, assignedMemberId, Map(assignedMemberId -> Array[Byte]()))
    val syncGroupErrorCode = syncGroupResult._2
    assertEquals(Errors.NONE.code, syncGroupErrorCode)

    EasyMock.reset(replicaManager)
    val heartbeatResult = heartbeat(groupId, assignedMemberId, 2)
    assertEquals(Errors.ILLEGAL_GENERATION.code, heartbeatResult)
  }

  @Test
  def testValidHeartbeat() {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val joinGroupResult = joinGroup(groupId, memberId, protocolType, protocols)
    val assignedConsumerId = joinGroupResult.memberId
    val generationId = joinGroupResult.generationId
    val joinGroupErrorCode = joinGroupResult.errorCode
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    EasyMock.reset(replicaManager)
    val syncGroupResult = syncGroupLeader(groupId, generationId, assignedConsumerId, Map(assignedConsumerId -> Array[Byte]()))
    val syncGroupErrorCode = syncGroupResult._2
    assertEquals(Errors.NONE.code, syncGroupErrorCode)

    EasyMock.reset(replicaManager)
    val heartbeatResult = heartbeat(groupId, assignedConsumerId, 1)
    assertEquals(Errors.NONE.code, heartbeatResult)
  }

  @Test
  def testSessionTimeout() {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val joinGroupResult = joinGroup(groupId, memberId, protocolType, protocols)
    val assignedConsumerId = joinGroupResult.memberId
    val generationId = joinGroupResult.generationId
    val joinGroupErrorCode = joinGroupResult.errorCode
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    EasyMock.reset(replicaManager)
    val (_, syncGroupErrorCode) = syncGroupLeader(groupId, generationId, assignedConsumerId, Map(assignedConsumerId -> Array[Byte]()))
    assertEquals(Errors.NONE.code, syncGroupErrorCode)

    EasyMock.reset(replicaManager)
    EasyMock.expect(replicaManager.getPartition(Topic.GroupMetadataTopicName, groupPartitionId)).andReturn(None)
    EasyMock.expect(replicaManager.getMessageFormatVersion(EasyMock.anyObject())).andReturn(Some(Message.MagicValue_V1)).anyTimes()
    EasyMock.replay(replicaManager)

    timer.advanceClock(DefaultSessionTimeout + 100)

    EasyMock.reset(replicaManager)
    val heartbeatResult = heartbeat(groupId, assignedConsumerId, 1)
    assertEquals(Errors.UNKNOWN_MEMBER_ID.code, heartbeatResult)
  }

  @Test
  def testHeartbeatMaintainsSession() {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val sessionTimeout = 1000

    val joinGroupResult = joinGroup(groupId, memberId, protocolType, protocols,
      rebalanceTimeout = sessionTimeout, sessionTimeout = sessionTimeout)
    val assignedConsumerId = joinGroupResult.memberId
    val generationId = joinGroupResult.generationId
    val joinGroupErrorCode = joinGroupResult.errorCode
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    EasyMock.reset(replicaManager)
    val (_, syncGroupErrorCode) = syncGroupLeader(groupId, generationId, assignedConsumerId, Map(assignedConsumerId -> Array[Byte]()))
    assertEquals(Errors.NONE.code, syncGroupErrorCode)

    timer.advanceClock(sessionTimeout / 2)

    EasyMock.reset(replicaManager)
    var heartbeatResult = heartbeat(groupId, assignedConsumerId, 1)
    assertEquals(Errors.NONE.code, heartbeatResult)

    timer.advanceClock(sessionTimeout / 2 + 100)

    EasyMock.reset(replicaManager)
    heartbeatResult = heartbeat(groupId, assignedConsumerId, 1)
    assertEquals(Errors.NONE.code, heartbeatResult)
  }

  @Test
  def testCommitMaintainsSession() {
    val sessionTimeout = 1000
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val tp = new TopicPartition("topic", 0)
    val offset = OffsetAndMetadata(0)

    val joinGroupResult = joinGroup(groupId, memberId, protocolType, protocols,
      rebalanceTimeout = sessionTimeout, sessionTimeout = sessionTimeout)
    val assignedConsumerId = joinGroupResult.memberId
    val generationId = joinGroupResult.generationId
    val joinGroupErrorCode = joinGroupResult.errorCode
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    EasyMock.reset(replicaManager)
    val (_, syncGroupErrorCode) = syncGroupLeader(groupId, generationId, assignedConsumerId, Map(assignedConsumerId -> Array[Byte]()))
    assertEquals(Errors.NONE.code, syncGroupErrorCode)

    timer.advanceClock(sessionTimeout / 2)

    EasyMock.reset(replicaManager)
    val commitOffsetResult = commitOffsets(groupId, assignedConsumerId, generationId, immutable.Map(tp -> offset))
    assertEquals(Errors.NONE.code, commitOffsetResult(tp))

    timer.advanceClock(sessionTimeout / 2 + 100)

    EasyMock.reset(replicaManager)
    val heartbeatResult = heartbeat(groupId, assignedConsumerId, 1)
    assertEquals(Errors.NONE.code, heartbeatResult)
  }

  @Test
  def testSessionTimeoutDuringRebalance() {
    // create a group with a single member
    val firstJoinResult = joinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols,
      rebalanceTimeout = 2000, sessionTimeout = 1000)
    val firstMemberId = firstJoinResult.memberId
    val firstGenerationId = firstJoinResult.generationId
    assertEquals(firstMemberId, firstJoinResult.leaderId)
    assertEquals(Errors.NONE.code, firstJoinResult.errorCode)

    EasyMock.reset(replicaManager)
    val firstSyncResult = syncGroupLeader(groupId, firstGenerationId, firstMemberId, Map(firstMemberId -> Array[Byte]()))
    assertEquals(Errors.NONE.code, firstSyncResult._2)

    // now have a new member join to trigger a rebalance
    EasyMock.reset(replicaManager)
    val otherJoinFuture = sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols)

    timer.advanceClock(500)

    EasyMock.reset(replicaManager)
    var heartbeatResult = heartbeat(groupId, firstMemberId, firstGenerationId)
    assertEquals(Errors.REBALANCE_IN_PROGRESS.code, heartbeatResult)

    // letting the session expire should make the member fall out of the group
    timer.advanceClock(1100)

    EasyMock.reset(replicaManager)
    heartbeatResult = heartbeat(groupId, firstMemberId, firstGenerationId)
    assertEquals(Errors.UNKNOWN_MEMBER_ID.code, heartbeatResult)

    // and the rebalance should complete with only the new member
    val otherJoinResult = await(otherJoinFuture, DefaultSessionTimeout+100)
    assertEquals(Errors.NONE.code, otherJoinResult.errorCode)
  }

  @Test
  def testRebalanceCompletesBeforeMemberJoins() {
    // create a group with a single member
    val firstJoinResult = joinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols,
      rebalanceTimeout = 1200, sessionTimeout = 1000)
    val firstMemberId = firstJoinResult.memberId
    val firstGenerationId = firstJoinResult.generationId
    assertEquals(firstMemberId, firstJoinResult.leaderId)
    assertEquals(Errors.NONE.code, firstJoinResult.errorCode)

    EasyMock.reset(replicaManager)
    val firstSyncResult = syncGroupLeader(groupId, firstGenerationId, firstMemberId, Map(firstMemberId -> Array[Byte]()))
    assertEquals(Errors.NONE.code, firstSyncResult._2)

    // now have a new member join to trigger a rebalance
    EasyMock.reset(replicaManager)
    val otherJoinFuture = sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols)

    // send a couple heartbeats to keep the member alive while the rebalance finishes
    timer.advanceClock(500)
    EasyMock.reset(replicaManager)
    var heartbeatResult = heartbeat(groupId, firstMemberId, firstGenerationId)
    assertEquals(Errors.REBALANCE_IN_PROGRESS.code, heartbeatResult)

    timer.advanceClock(500)
    EasyMock.reset(replicaManager)
    heartbeatResult = heartbeat(groupId, firstMemberId, firstGenerationId)
    assertEquals(Errors.REBALANCE_IN_PROGRESS.code, heartbeatResult)

    // now timeout the rebalance, which should kick the unjoined member out of the group
    // and let the rebalance finish with only the new member
    timer.advanceClock(500)
    val otherJoinResult = await(otherJoinFuture, DefaultSessionTimeout+100)
    assertEquals(Errors.NONE.code, otherJoinResult.errorCode)
  }

  @Test
  def testSyncGroupEmptyAssignment() {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val joinGroupResult = joinGroup(groupId, memberId, protocolType, protocols)
    val assignedConsumerId = joinGroupResult.memberId
    val generationId = joinGroupResult.generationId
    val joinGroupErrorCode = joinGroupResult.errorCode
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    EasyMock.reset(replicaManager)
    val syncGroupResult = syncGroupLeader(groupId, generationId, assignedConsumerId, Map())
    val syncGroupErrorCode = syncGroupResult._2
    assertEquals(Errors.NONE.code, syncGroupErrorCode)
    assertTrue(syncGroupResult._1.isEmpty)

    EasyMock.reset(replicaManager)
    val heartbeatResult = heartbeat(groupId, assignedConsumerId, 1)
    assertEquals(Errors.NONE.code, heartbeatResult)
  }

  @Test
  def testSyncGroupNotCoordinator() {
    val generation = 1

    val syncGroupResult = syncGroupFollower(otherGroupId, generation, memberId)
    assertEquals(Errors.NOT_COORDINATOR_FOR_GROUP.code, syncGroupResult._2)
  }

  @Test
  def testSyncGroupFromUnknownGroup() {
    val generation = 1

    val syncGroupResult = syncGroupFollower(groupId, generation, memberId)
    assertEquals(Errors.UNKNOWN_MEMBER_ID.code, syncGroupResult._2)
  }

  @Test
  def testSyncGroupFromUnknownMember() {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val joinGroupResult = joinGroup(groupId, memberId, protocolType, protocols)
    val assignedConsumerId = joinGroupResult.memberId
    val generationId = joinGroupResult.generationId
    assertEquals(Errors.NONE.code, joinGroupResult.errorCode)

    EasyMock.reset(replicaManager)
    val syncGroupResult = syncGroupLeader(groupId, generationId, assignedConsumerId, Map(assignedConsumerId -> Array[Byte]()))
    val syncGroupErrorCode = syncGroupResult._2
    assertEquals(Errors.NONE.code, syncGroupErrorCode)

    EasyMock.reset(replicaManager)
    val unknownMemberId = "blah"
    val unknownMemberSyncResult = syncGroupFollower(groupId, generationId, unknownMemberId)
    assertEquals(Errors.UNKNOWN_MEMBER_ID.code, unknownMemberSyncResult._2)
  }

  @Test
  def testSyncGroupFromIllegalGeneration() {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val joinGroupResult = joinGroup(groupId, memberId, protocolType, protocols)
    val assignedConsumerId = joinGroupResult.memberId
    val generationId = joinGroupResult.generationId
    assertEquals(Errors.NONE.code, joinGroupResult.errorCode)

    EasyMock.reset(replicaManager)
    // send the sync group with an invalid generation
    val syncGroupResult = syncGroupLeader(groupId, generationId+1, assignedConsumerId, Map(assignedConsumerId -> Array[Byte]()))
    assertEquals(Errors.ILLEGAL_GENERATION.code, syncGroupResult._2)
  }

  @Test
  def testJoinGroupFromUnchangedFollowerDoesNotRebalance() {
    // to get a group of two members:
    // 1. join and sync with a single member (because we can't immediately join with two members)
    // 2. join and sync with the first member and a new member

    val firstJoinResult = joinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols)
    val firstMemberId = firstJoinResult.memberId
    val firstGenerationId = firstJoinResult.generationId
    assertEquals(firstMemberId, firstJoinResult.leaderId)
    assertEquals(Errors.NONE.code, firstJoinResult.errorCode)

    EasyMock.reset(replicaManager)
    val firstSyncResult = syncGroupLeader(groupId, firstGenerationId, firstMemberId, Map(firstMemberId -> Array[Byte]()))
    assertEquals(Errors.NONE.code, firstSyncResult._2)

    EasyMock.reset(replicaManager)
    val otherJoinFuture = sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols)

    EasyMock.reset(replicaManager)
    val joinFuture = sendJoinGroup(groupId, firstMemberId, protocolType, protocols)

    val joinResult = await(joinFuture, DefaultSessionTimeout+100)
    val otherJoinResult = await(otherJoinFuture, DefaultSessionTimeout+100)
    assertEquals(Errors.NONE.code, joinResult.errorCode)
    assertEquals(Errors.NONE.code, otherJoinResult.errorCode)
    assertTrue(joinResult.generationId == otherJoinResult.generationId)

    assertEquals(firstMemberId, joinResult.leaderId)
    assertEquals(firstMemberId, otherJoinResult.leaderId)

    val nextGenerationId = joinResult.generationId

    // this shouldn't cause a rebalance since protocol information hasn't changed
    EasyMock.reset(replicaManager)
    val followerJoinResult = joinGroup(groupId, otherJoinResult.memberId, protocolType, protocols)

    assertEquals(Errors.NONE.code, followerJoinResult.errorCode)
    assertEquals(nextGenerationId, followerJoinResult.generationId)
  }

  @Test
  def testJoinGroupFromUnchangedLeaderShouldRebalance() {
    val firstJoinResult = joinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols)
    val firstMemberId = firstJoinResult.memberId
    val firstGenerationId = firstJoinResult.generationId
    assertEquals(firstMemberId, firstJoinResult.leaderId)
    assertEquals(Errors.NONE.code, firstJoinResult.errorCode)

    EasyMock.reset(replicaManager)
    val firstSyncResult = syncGroupLeader(groupId, firstGenerationId, firstMemberId, Map(firstMemberId -> Array[Byte]()))
    assertEquals(Errors.NONE.code, firstSyncResult._2)

    // join groups from the leader should force the group to rebalance, which allows the
    // leader to push new assignments when local metadata changes

    EasyMock.reset(replicaManager)
    val secondJoinResult = joinGroup(groupId, firstMemberId, protocolType, protocols)

    assertEquals(Errors.NONE.code, secondJoinResult.errorCode)
    assertNotEquals(firstGenerationId, secondJoinResult.generationId)
  }

  @Test
  def testLeaderFailureInSyncGroup() {
    // to get a group of two members:
    // 1. join and sync with a single member (because we can't immediately join with two members)
    // 2. join and sync with the first member and a new member

    val firstJoinResult = joinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols)
    val firstMemberId = firstJoinResult.memberId
    val firstGenerationId = firstJoinResult.generationId
    assertEquals(firstMemberId, firstJoinResult.leaderId)
    assertEquals(Errors.NONE.code, firstJoinResult.errorCode)

    EasyMock.reset(replicaManager)
    val firstSyncResult = syncGroupLeader(groupId, firstGenerationId, firstMemberId, Map(firstMemberId -> Array[Byte]()))
    assertEquals(Errors.NONE.code, firstSyncResult._2)

    EasyMock.reset(replicaManager)
    val otherJoinFuture = sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols)

    EasyMock.reset(replicaManager)
    val joinFuture = sendJoinGroup(groupId, firstMemberId, protocolType, protocols)

    val joinResult = await(joinFuture, DefaultSessionTimeout+100)
    val otherJoinResult = await(otherJoinFuture, DefaultSessionTimeout+100)
    assertEquals(Errors.NONE.code, joinResult.errorCode)
    assertEquals(Errors.NONE.code, otherJoinResult.errorCode)
    assertTrue(joinResult.generationId == otherJoinResult.generationId)

    assertEquals(firstMemberId, joinResult.leaderId)
    assertEquals(firstMemberId, otherJoinResult.leaderId)

    val nextGenerationId = joinResult.generationId

    // with no leader SyncGroup, the follower's request should failure with an error indicating
    // that it should rejoin
    EasyMock.reset(replicaManager)
    val followerSyncFuture = sendSyncGroupFollower(groupId, nextGenerationId, otherJoinResult.memberId)

    timer.advanceClock(DefaultSessionTimeout + 100)

    val followerSyncResult = await(followerSyncFuture, DefaultSessionTimeout+100)
    assertEquals(Errors.REBALANCE_IN_PROGRESS.code, followerSyncResult._2)
  }

  @Test
  def testSyncGroupFollowerAfterLeader() {
    // to get a group of two members:
    // 1. join and sync with a single member (because we can't immediately join with two members)
    // 2. join and sync with the first member and a new member

    val firstJoinResult = joinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols)
    val firstMemberId = firstJoinResult.memberId
    val firstGenerationId = firstJoinResult.generationId
    assertEquals(firstMemberId, firstJoinResult.leaderId)
    assertEquals(Errors.NONE.code, firstJoinResult.errorCode)

    EasyMock.reset(replicaManager)
    val firstSyncResult = syncGroupLeader(groupId, firstGenerationId, firstMemberId, Map(firstMemberId -> Array[Byte]()))
    assertEquals(Errors.NONE.code, firstSyncResult._2)

    EasyMock.reset(replicaManager)
    val otherJoinFuture = sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols)

    EasyMock.reset(replicaManager)
    val joinFuture = sendJoinGroup(groupId, firstMemberId, protocolType, protocols)

    val joinResult = await(joinFuture, DefaultSessionTimeout+100)
    val otherJoinResult = await(otherJoinFuture, DefaultSessionTimeout+100)
    assertEquals(Errors.NONE.code, joinResult.errorCode)
    assertEquals(Errors.NONE.code, otherJoinResult.errorCode)
    assertTrue(joinResult.generationId == otherJoinResult.generationId)

    assertEquals(firstMemberId, joinResult.leaderId)
    assertEquals(firstMemberId, otherJoinResult.leaderId)

    val nextGenerationId = joinResult.generationId
    val leaderId = firstMemberId
    val leaderAssignment = Array[Byte](0)
    val followerId = otherJoinResult.memberId
    val followerAssignment = Array[Byte](1)

    EasyMock.reset(replicaManager)
    val leaderSyncResult = syncGroupLeader(groupId, nextGenerationId, leaderId,
      Map(leaderId -> leaderAssignment, followerId -> followerAssignment))
    assertEquals(Errors.NONE.code, leaderSyncResult._2)
    assertEquals(leaderAssignment, leaderSyncResult._1)

    EasyMock.reset(replicaManager)
    val followerSyncResult = syncGroupFollower(groupId, nextGenerationId, otherJoinResult.memberId)
    assertEquals(Errors.NONE.code, followerSyncResult._2)
    assertEquals(followerAssignment, followerSyncResult._1)
  }

  @Test
  def testSyncGroupLeaderAfterFollower() {
    // to get a group of two members:
    // 1. join and sync with a single member (because we can't immediately join with two members)
    // 2. join and sync with the first member and a new member

    val joinGroupResult = joinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols)
    val firstMemberId = joinGroupResult.memberId
    val firstGenerationId = joinGroupResult.generationId
    assertEquals(firstMemberId, joinGroupResult.leaderId)
    assertEquals(Errors.NONE.code, joinGroupResult.errorCode)

    EasyMock.reset(replicaManager)
    val syncGroupResult = syncGroupLeader(groupId, firstGenerationId, firstMemberId, Map(firstMemberId -> Array[Byte]()))
    val syncGroupErrorCode = syncGroupResult._2
    assertEquals(Errors.NONE.code, syncGroupErrorCode)

    EasyMock.reset(replicaManager)
    val otherJoinFuture = sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols)

    EasyMock.reset(replicaManager)
    val joinFuture = sendJoinGroup(groupId, firstMemberId, protocolType, protocols)

    val joinResult = await(joinFuture, DefaultSessionTimeout+100)
    val otherJoinResult = await(otherJoinFuture, DefaultSessionTimeout+100)
    assertEquals(Errors.NONE.code, joinResult.errorCode)
    assertEquals(Errors.NONE.code, otherJoinResult.errorCode)
    assertTrue(joinResult.generationId == otherJoinResult.generationId)

    val nextGenerationId = joinResult.generationId
    val leaderId = joinResult.leaderId
    val leaderAssignment = Array[Byte](0)
    val followerId = otherJoinResult.memberId
    val followerAssignment = Array[Byte](1)

    assertEquals(firstMemberId, joinResult.leaderId)
    assertEquals(firstMemberId, otherJoinResult.leaderId)

    EasyMock.reset(replicaManager)
    val followerSyncFuture = sendSyncGroupFollower(groupId, nextGenerationId, followerId)

    EasyMock.reset(replicaManager)
    val leaderSyncResult = syncGroupLeader(groupId, nextGenerationId, leaderId,
      Map(leaderId -> leaderAssignment, followerId -> followerAssignment))
    assertEquals(Errors.NONE.code, leaderSyncResult._2)
    assertEquals(leaderAssignment, leaderSyncResult._1)

    val followerSyncResult = await(followerSyncFuture, DefaultSessionTimeout+100)
    assertEquals(Errors.NONE.code, followerSyncResult._2)
    assertEquals(followerAssignment, followerSyncResult._1)
  }

  @Test
  def testCommitOffsetFromUnknownGroup() {
    val generationId = 1
    val tp = new TopicPartition("topic", 0)
    val offset = OffsetAndMetadata(0)

    val commitOffsetResult = commitOffsets(groupId, memberId, generationId, immutable.Map(tp -> offset))
    assertEquals(Errors.ILLEGAL_GENERATION.code, commitOffsetResult(tp))
  }

  @Test
  def testCommitOffsetWithDefaultGeneration() {
    val tp = new TopicPartition("topic", 0)
    val offset = OffsetAndMetadata(0)

    val commitOffsetResult = commitOffsets(groupId, OffsetCommitRequest.DEFAULT_MEMBER_ID,
      OffsetCommitRequest.DEFAULT_GENERATION_ID, immutable.Map(tp -> offset))
    assertEquals(Errors.NONE.code, commitOffsetResult(tp))
  }

  @Test
  def testCommitOffsetInAwaitingSync() {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val tp = new TopicPartition("topic", 0)
    val offset = OffsetAndMetadata(0)

    val joinGroupResult = joinGroup(groupId, memberId, protocolType, protocols)
    val assignedMemberId = joinGroupResult.memberId
    val generationId = joinGroupResult.generationId
    val joinGroupErrorCode = joinGroupResult.errorCode
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    EasyMock.reset(replicaManager)
    val commitOffsetResult = commitOffsets(groupId, assignedMemberId, generationId, immutable.Map(tp -> offset))
    assertEquals(Errors.REBALANCE_IN_PROGRESS.code, commitOffsetResult(tp))
  }

  @Test
  def testHeartbeatDuringRebalanceCausesRebalanceInProgress() {
    // First start up a group (with a slightly larger timeout to give us time to heartbeat when the rebalance starts)
    val joinGroupResult = joinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols)
    val assignedConsumerId = joinGroupResult.memberId
    val initialGenerationId = joinGroupResult.generationId
    val joinGroupErrorCode = joinGroupResult.errorCode
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    // Then join with a new consumer to trigger a rebalance
    EasyMock.reset(replicaManager)
    sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols)

    // We should be in the middle of a rebalance, so the heartbeat should return rebalance in progress
    EasyMock.reset(replicaManager)
    val heartbeatResult = heartbeat(groupId, assignedConsumerId, initialGenerationId)
    assertEquals(Errors.REBALANCE_IN_PROGRESS.code, heartbeatResult)
  }

  @Test
  def testGenerationIdIncrementsOnRebalance() {
    val joinGroupResult = joinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols)
    val initialGenerationId = joinGroupResult.generationId
    val joinGroupErrorCode = joinGroupResult.errorCode
    val memberId = joinGroupResult.memberId
    assertEquals(1, initialGenerationId)
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    EasyMock.reset(replicaManager)
    val syncGroupResult = syncGroupLeader(groupId, initialGenerationId, memberId, Map(memberId -> Array[Byte]()))
    val syncGroupErrorCode = syncGroupResult._2
    assertEquals(Errors.NONE.code, syncGroupErrorCode)

    EasyMock.reset(replicaManager)
    val otherJoinGroupResult = joinGroup(groupId, memberId, protocolType, protocols)
    val nextGenerationId = otherJoinGroupResult.generationId
    val otherJoinGroupErrorCode = otherJoinGroupResult.errorCode
    assertEquals(2, nextGenerationId)
    assertEquals(Errors.NONE.code, otherJoinGroupErrorCode)
  }

  @Test
  def testLeaveGroupWrongCoordinator() {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val leaveGroupResult = leaveGroup(otherGroupId, memberId)
    assertEquals(Errors.NOT_COORDINATOR_FOR_GROUP.code, leaveGroupResult)
  }

  @Test
  def testLeaveGroupUnknownGroup() {

    val leaveGroupResult = leaveGroup(groupId, memberId)
    assertEquals(Errors.UNKNOWN_MEMBER_ID.code, leaveGroupResult)
  }

  @Test
  def testLeaveGroupUnknownConsumerExistingGroup() {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val otherMemberId = "consumerId"

    val joinGroupResult = joinGroup(groupId, memberId, protocolType, protocols)
    val joinGroupErrorCode = joinGroupResult.errorCode
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    EasyMock.reset(replicaManager)
    val leaveGroupResult = leaveGroup(groupId, otherMemberId)
    assertEquals(Errors.UNKNOWN_MEMBER_ID.code, leaveGroupResult)
  }

  @Test
  def testValidLeaveGroup() {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val joinGroupResult = joinGroup(groupId, memberId, protocolType, protocols)
    val assignedMemberId = joinGroupResult.memberId
    val joinGroupErrorCode = joinGroupResult.errorCode
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    EasyMock.reset(replicaManager)
    val leaveGroupResult = leaveGroup(groupId, assignedMemberId)
    assertEquals(Errors.NONE.code, leaveGroupResult)
  }

  @Test
  def testListGroupsIncludesStableGroups() {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val joinGroupResult = joinGroup(groupId, memberId, protocolType, protocols)
    val assignedMemberId = joinGroupResult.memberId
    val generationId = joinGroupResult.generationId
    assertEquals(Errors.NONE.code, joinGroupResult.errorCode)

    EasyMock.reset(replicaManager)
    val syncGroupResult = syncGroupLeader(groupId, generationId, assignedMemberId, Map(assignedMemberId -> Array[Byte]()))
    val syncGroupErrorCode = syncGroupResult._2
    assertEquals(Errors.NONE.code, syncGroupErrorCode)

    val (error, groups) = groupCoordinator.handleListGroups()
    assertEquals(Errors.NONE, error)
    assertEquals(1, groups.size)
    assertEquals(GroupOverview("groupId", "consumer"), groups.head)
  }

  @Test
  def testListGroupsIncludesRebalancingGroups() {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val joinGroupResult = joinGroup(groupId, memberId, protocolType, protocols)
    assertEquals(Errors.NONE.code, joinGroupResult.errorCode)

    val (error, groups) = groupCoordinator.handleListGroups()
    assertEquals(Errors.NONE, error)
    assertEquals(1, groups.size)
    assertEquals(GroupOverview("groupId", "consumer"), groups.head)
  }

  @Test
  def testDescribeGroupWrongCoordinator() {
    EasyMock.reset(replicaManager)
    val (error, _) = groupCoordinator.handleDescribeGroup(otherGroupId)
    assertEquals(Errors.NOT_COORDINATOR_FOR_GROUP, error)
  }

  @Test
  def testDescribeGroupInactiveGroup() {
    EasyMock.reset(replicaManager)
    val (error, summary) = groupCoordinator.handleDescribeGroup(groupId)
    assertEquals(Errors.NONE, error)
    assertEquals(GroupCoordinator.DeadGroup, summary)
  }

  @Test
  def testDescribeGroupStable() {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val joinGroupResult = joinGroup(groupId, memberId, protocolType, protocols)
    val assignedMemberId = joinGroupResult.memberId
    val generationId = joinGroupResult.generationId
    val joinGroupErrorCode = joinGroupResult.errorCode
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    EasyMock.reset(replicaManager)
    val syncGroupResult = syncGroupLeader(groupId, generationId, assignedMemberId, Map(assignedMemberId -> Array[Byte]()))

    val syncGroupErrorCode = syncGroupResult._2
    assertEquals(Errors.NONE.code, syncGroupErrorCode)

    EasyMock.reset(replicaManager)
    val (error, summary) = groupCoordinator.handleDescribeGroup(groupId)
    assertEquals(Errors.NONE, error)
    assertEquals(protocolType, summary.protocolType)
    assertEquals("range", summary.protocol)
    assertEquals(List(assignedMemberId), summary.members.map(_.memberId))
  }

  @Test
  def testDescribeGroupRebalancing() {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val joinGroupResult = joinGroup(groupId, memberId, protocolType, protocols)
    val joinGroupErrorCode = joinGroupResult.errorCode
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    EasyMock.reset(replicaManager)
    val (error, summary) = groupCoordinator.handleDescribeGroup(groupId)
    assertEquals(Errors.NONE, error)
    assertEquals(protocolType, summary.protocolType)
    assertEquals(GroupCoordinator.NoProtocol, summary.protocol)
    assertEquals(AwaitingSync.toString, summary.state)
    assertTrue(summary.members.map(_.memberId).contains(joinGroupResult.memberId))
    assertTrue(summary.members.forall(_.metadata.isEmpty))
    assertTrue(summary.members.forall(_.assignment.isEmpty))
  }

  private def setupJoinGroupCallback: (Future[JoinGroupResult], JoinGroupCallback) = {
    val responsePromise = Promise[JoinGroupResult]
    val responseFuture = responsePromise.future
    val responseCallback: JoinGroupCallback = responsePromise.success(_)
    (responseFuture, responseCallback)
  }

  private def setupSyncGroupCallback: (Future[SyncGroupCallbackParams], SyncGroupCallback) = {
    val responsePromise = Promise[SyncGroupCallbackParams]
    val responseFuture = responsePromise.future
    val responseCallback: SyncGroupCallback = (assignment, errorCode) =>
      responsePromise.success((assignment, errorCode))
    (responseFuture, responseCallback)
  }

  private def setupHeartbeatCallback: (Future[HeartbeatCallbackParams], HeartbeatCallback) = {
    val responsePromise = Promise[HeartbeatCallbackParams]
    val responseFuture = responsePromise.future
    val responseCallback: HeartbeatCallback = errorCode => responsePromise.success(errorCode)
    (responseFuture, responseCallback)
  }

  private def setupCommitOffsetsCallback: (Future[CommitOffsetCallbackParams], CommitOffsetCallback) = {
    val responsePromise = Promise[CommitOffsetCallbackParams]
    val responseFuture = responsePromise.future
    val responseCallback: CommitOffsetCallback = offsets => responsePromise.success(offsets)
    (responseFuture, responseCallback)
  }

  private def sendJoinGroup(groupId: String,
                            memberId: String,
                            protocolType: String,
                            protocols: List[(String, Array[Byte])],
                            rebalanceTimeout: Int = DefaultRebalanceTimeout,
                            sessionTimeout: Int = DefaultSessionTimeout): Future[JoinGroupResult] = {
    val (responseFuture, responseCallback) = setupJoinGroupCallback

    EasyMock.replay(replicaManager)

    groupCoordinator.handleJoinGroup(groupId, memberId, "clientId", "clientHost", rebalanceTimeout, sessionTimeout,
      protocolType, protocols, responseCallback)
    responseFuture
  }


  private def sendSyncGroupLeader(groupId: String,
                                  generation: Int,
                                  leaderId: String,
                                  assignment: Map[String, Array[Byte]]): Future[SyncGroupCallbackParams] = {
    val (responseFuture, responseCallback) = setupSyncGroupCallback

    val capturedArgument: Capture[Map[TopicPartition, PartitionResponse] => Unit] = EasyMock.newCapture()

    EasyMock.expect(replicaManager.appendMessages(EasyMock.anyLong(),
      EasyMock.anyShort(),
      EasyMock.anyBoolean(),
      EasyMock.anyObject().asInstanceOf[Map[TopicPartition, MessageSet]],
      EasyMock.capture(capturedArgument))).andAnswer(new IAnswer[Unit] {
      override def answer = capturedArgument.getValue.apply(
        Map(new TopicPartition(Topic.GroupMetadataTopicName, groupPartitionId) ->
          new PartitionResponse(Errors.NONE.code, 0L, Record.NO_TIMESTAMP)
        )
      )})
    EasyMock.expect(replicaManager.getMessageFormatVersion(EasyMock.anyObject())).andReturn(Some(Message.MagicValue_V1)).anyTimes()
    EasyMock.replay(replicaManager)

    groupCoordinator.handleSyncGroup(groupId, generation, leaderId, assignment, responseCallback)
    responseFuture
  }

  private def sendSyncGroupFollower(groupId: String,
                                    generation: Int,
                                    memberId: String): Future[SyncGroupCallbackParams] = {
    val (responseFuture, responseCallback) = setupSyncGroupCallback

    EasyMock.replay(replicaManager)

    groupCoordinator.handleSyncGroup(groupId, generation, memberId, Map.empty[String, Array[Byte]], responseCallback)
    responseFuture
  }

  private def joinGroup(groupId: String,
                        memberId: String,
                        protocolType: String,
                        protocols: List[(String, Array[Byte])],
                        sessionTimeout: Int = DefaultSessionTimeout,
                        rebalanceTimeout: Int = DefaultRebalanceTimeout): JoinGroupResult = {
    val responseFuture = sendJoinGroup(groupId, memberId, protocolType, protocols, rebalanceTimeout, sessionTimeout)
    timer.advanceClock(10)
    // should only have to wait as long as session timeout, but allow some extra time in case of an unexpected delay
    Await.result(responseFuture, Duration(rebalanceTimeout + 100, TimeUnit.MILLISECONDS))
  }


  private def syncGroupFollower(groupId: String,
                                generationId: Int,
                                memberId: String,
                                sessionTimeout: Int = DefaultSessionTimeout): SyncGroupCallbackParams = {
    val responseFuture = sendSyncGroupFollower(groupId, generationId, memberId)
    Await.result(responseFuture, Duration(sessionTimeout + 100, TimeUnit.MILLISECONDS))
  }

  private def syncGroupLeader(groupId: String,
                              generationId: Int,
                              memberId: String,
                              assignment: Map[String, Array[Byte]],
                              sessionTimeout: Int = DefaultSessionTimeout): SyncGroupCallbackParams = {
    val responseFuture = sendSyncGroupLeader(groupId, generationId, memberId, assignment)
    Await.result(responseFuture, Duration(sessionTimeout + 100, TimeUnit.MILLISECONDS))
  }

  private def heartbeat(groupId: String,
                        consumerId: String,
                        generationId: Int): HeartbeatCallbackParams = {
    val (responseFuture, responseCallback) = setupHeartbeatCallback

    EasyMock.replay(replicaManager)

    groupCoordinator.handleHeartbeat(groupId, consumerId, generationId, responseCallback)
    Await.result(responseFuture, Duration(40, TimeUnit.MILLISECONDS))
  }

  private def await[T](future: Future[T], millis: Long): T = {
    Await.result(future, Duration(millis, TimeUnit.MILLISECONDS))
  }

  private def commitOffsets(groupId: String,
                            consumerId: String,
                            generationId: Int,
                            offsets: immutable.Map[TopicPartition, OffsetAndMetadata]): CommitOffsetCallbackParams = {
    val (responseFuture, responseCallback) = setupCommitOffsetsCallback

    val capturedArgument: Capture[Map[TopicPartition, PartitionResponse] => Unit] = EasyMock.newCapture()

    EasyMock.expect(replicaManager.appendMessages(EasyMock.anyLong(),
      EasyMock.anyShort(),
      EasyMock.anyBoolean(),
      EasyMock.anyObject().asInstanceOf[Map[TopicPartition, MessageSet]],
      EasyMock.capture(capturedArgument))).andAnswer(new IAnswer[Unit] {
      override def answer = capturedArgument.getValue.apply(
        Map(new TopicPartition(Topic.GroupMetadataTopicName, groupPartitionId) ->
          new PartitionResponse(Errors.NONE.code, 0L, Record.NO_TIMESTAMP)
        )
      )})
    EasyMock.expect(replicaManager.getMessageFormatVersion(EasyMock.anyObject())).andReturn(Some(Message.MagicValue_V1)).anyTimes()
    EasyMock.replay(replicaManager)

    groupCoordinator.handleCommitOffsets(groupId, consumerId, generationId, offsets, responseCallback)
    Await.result(responseFuture, Duration(40, TimeUnit.MILLISECONDS))
  }

  private def leaveGroup(groupId: String, consumerId: String): LeaveGroupCallbackParams = {
    val (responseFuture, responseCallback) = setupHeartbeatCallback

    EasyMock.expect(replicaManager.getPartition(Topic.GroupMetadataTopicName, groupPartitionId)).andReturn(None)
    EasyMock.expect(replicaManager.getMessageFormatVersion(EasyMock.anyObject())).andReturn(Some(Message.MagicValue_V1)).anyTimes()
    EasyMock.replay(replicaManager)

    groupCoordinator.handleLeaveGroup(groupId, consumerId, responseCallback)
    Await.result(responseFuture, Duration(40, TimeUnit.MILLISECONDS))
  }

}
