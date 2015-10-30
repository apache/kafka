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


import java.util.concurrent.TimeUnit

import org.junit.Assert._
import kafka.common.{OffsetAndMetadata, TopicAndPartition}
import kafka.server.KafkaConfig
import kafka.utils.{Pool, TestUtils}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{OffsetCommitRequest, JoinGroupRequest}
import org.easymock.{IAnswer, EasyMock}
import org.junit.{After, Before, Test}
import org.scalatest.junit.JUnitSuite

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
  type CommitOffsetCallbackParams = Map[TopicAndPartition, Short]
  type CommitOffsetCallback = Map[TopicAndPartition, Short] => Unit
  type LeaveGroupCallbackParams = Short
  type LeaveGroupCallback = Short => Unit

  val ConsumerMinSessionTimeout = 10
  val ConsumerMaxSessionTimeout = 1000
  val DefaultSessionTimeout = 500
  var consumerCoordinator: GroupCoordinator = null
  var groupManager : GroupMetadataManager = null

  private val groupId = "groupId"
  private val protocolType = "consumer"
  private val memberId = "memberId"
  private val metadata = Array[Byte]()
  private val protocols = List(("range", metadata))

  private val groupsCache = new Pool[String, GroupMetadata]

  private val propagateRebalanceAnswer = new IAnswer[Unit] {
    override def answer = {
      for (member <- groupsCache.get(groupId).allMembers) {
        if (member.awaitingSyncCallback != null) {
          member.awaitingSyncCallback.apply(Array.empty[Byte], Errors.REBALANCE_IN_PROGRESS.code)
          member.awaitingSyncCallback = null
        }
      }
    }
  }

  private var memberIndex : Int = 1

  @Before
  def setUp() {
    val props = TestUtils.createBrokerConfig(nodeId = 0, zkConnect = "")
    props.setProperty(KafkaConfig.GroupMinSessionTimeoutMsProp, ConsumerMinSessionTimeout.toString)
    props.setProperty(KafkaConfig.GroupMaxSessionTimeoutMsProp, ConsumerMaxSessionTimeout.toString)
    
    groupManager = EasyMock.createStrictMock(classOf[GroupMetadataManager])
    
    consumerCoordinator = GroupCoordinator.create(KafkaConfig.fromProps(props), groupManager)
    consumerCoordinator.startup()
  }

  @After
  def tearDown() {
    groupsCache.clear()
    EasyMock.reset(groupManager)
    consumerCoordinator.shutdown()
  }

  @Test
  def testJoinGroupWrongCoordinator() {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val joinGroupResult = joinGroup(groupId, memberId, DefaultSessionTimeout, protocolType,
      protocols, isCoordinatorForGroup = false)
    val joinGroupErrorCode = joinGroupResult.errorCode
    assertEquals(Errors.NOT_COORDINATOR_FOR_GROUP.code, joinGroupErrorCode)
  }

  @Test
  def testJoinGroupSessionTimeoutTooSmall() {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val joinGroupResult = joinGroup(groupId, memberId, ConsumerMinSessionTimeout - 1, protocolType, protocols,
      isCoordinatorForGroup = true)
    val joinGroupErrorCode = joinGroupResult.errorCode
    assertEquals(Errors.INVALID_SESSION_TIMEOUT.code, joinGroupErrorCode)
  }

  @Test
  def testJoinGroupSessionTimeoutTooLarge() {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val joinGroupResult = joinGroup(groupId, memberId, ConsumerMaxSessionTimeout + 1, protocolType, protocols,
      isCoordinatorForGroup = true)
    val joinGroupErrorCode = joinGroupResult.errorCode
    assertEquals(Errors.INVALID_SESSION_TIMEOUT.code, joinGroupErrorCode)
  }

  @Test
  def testJoinGroupUnknownConsumerNewGroup() {
    val joinGroupResult = joinGroup(groupId, memberId, DefaultSessionTimeout, protocolType, protocols,
      isCoordinatorForGroup = true)
    val joinGroupErrorCode = joinGroupResult.errorCode
    assertEquals(Errors.UNKNOWN_MEMBER_ID.code, joinGroupErrorCode)
  }

  @Test
  def testInvalidGroupId() {
    val groupId = ""
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val joinGroupResult = joinGroup(groupId, memberId, DefaultSessionTimeout, protocolType,
      protocols, isCoordinatorForGroup = true)
    assertEquals(Errors.INVALID_GROUP_ID.code, joinGroupResult.errorCode)
  }

  @Test
  def testValidJoinGroup() {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val joinGroupResult = joinGroup(groupId, memberId, DefaultSessionTimeout, protocolType,
      protocols, isCoordinatorForGroup = true)
    val joinGroupErrorCode = joinGroupResult.errorCode
    assertEquals(Errors.NONE.code, joinGroupErrorCode)
  }

  @Test
  def testJoinGroupInconsistentProtocolType() {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val otherMemberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val joinGroupResult = joinGroup(groupId, memberId, DefaultSessionTimeout, protocolType,
      protocols, isCoordinatorForGroup = true)
    assertEquals(Errors.NONE.code, joinGroupResult.errorCode)

    EasyMock.reset(groupManager)
    val otherJoinGroupResult = joinGroup(groupId, otherMemberId, DefaultSessionTimeout, "copycat",
      protocols, isCoordinatorForGroup = true)
    assertEquals(Errors.INCONSISTENT_GROUP_PROTOCOL.code, otherJoinGroupResult.errorCode)
  }

  @Test
  def testJoinGroupInconsistentGroupProtocol() {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val otherMemberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val joinGroupResult = joinGroup(groupId, memberId, DefaultSessionTimeout, protocolType, List(("range", metadata)),
      isCoordinatorForGroup = true)
    assertEquals(Errors.NONE.code, joinGroupResult.errorCode)

    EasyMock.reset(groupManager)
    val otherJoinGroupResult = joinGroup(groupId, otherMemberId, DefaultSessionTimeout, protocolType,
      List(("roundrobin", metadata)), isCoordinatorForGroup = true)
    assertEquals(Errors.INCONSISTENT_GROUP_PROTOCOL.code, otherJoinGroupResult.errorCode)
  }

  @Test
  def testJoinGroupUnknownConsumerExistingGroup() {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val otherMemberId = "memberId"

    val joinGroupResult = joinGroup(groupId, memberId, DefaultSessionTimeout, protocolType, protocols,
      isCoordinatorForGroup = true)
    assertEquals(Errors.NONE.code, joinGroupResult.errorCode)

    EasyMock.reset(groupManager)
    val otherJoinGroupResult = joinGroup(groupId, otherMemberId, DefaultSessionTimeout, protocolType, protocols,
      isCoordinatorForGroup = true)
    assertEquals(Errors.UNKNOWN_MEMBER_ID.code, otherJoinGroupResult.errorCode)
  }

  @Test
  def testHeartbeatWrongCoordinator() {

    val heartbeatResult = heartbeat(groupId, memberId, -1, isCoordinatorForGroup = false)
    assertEquals(Errors.NOT_COORDINATOR_FOR_GROUP.code, heartbeatResult)
  }

  @Test
  def testHeartbeatUnknownGroup() {

    val heartbeatResult = heartbeat(groupId, memberId, -1, isCoordinatorForGroup = true)
    assertEquals(Errors.UNKNOWN_MEMBER_ID.code, heartbeatResult)
  }

  @Test
  def testHeartbeatUnknownConsumerExistingGroup() {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val otherMemberId = "memberId"

    val joinGroupResult = joinGroup(groupId, memberId, DefaultSessionTimeout, protocolType, protocols,
      isCoordinatorForGroup = true)
    val assignedMemberId = joinGroupResult.memberId
    val joinGroupErrorCode = joinGroupResult.errorCode
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    EasyMock.reset(groupManager)
    val syncGroupResult = syncGroupLeader(groupId, joinGroupResult.generationId, assignedMemberId, Map(assignedMemberId -> Array[Byte]()), true)
    val syncGroupErrorCode = syncGroupResult._2
    assertEquals(Errors.NONE.code, syncGroupErrorCode)

    EasyMock.reset(groupManager)
    val heartbeatResult = heartbeat(groupId, otherMemberId, 1, isCoordinatorForGroup = true)
    assertEquals(Errors.UNKNOWN_MEMBER_ID.code, heartbeatResult)
  }

  @Test
  def testHeartbeatRebalanceInProgress() {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val joinGroupResult = joinGroup(groupId, memberId, DefaultSessionTimeout, protocolType, protocols,
      isCoordinatorForGroup = true)
    val assignedMemberId = joinGroupResult.memberId
    val joinGroupErrorCode = joinGroupResult.errorCode
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    EasyMock.reset(groupManager)
    val heartbeatResult = heartbeat(groupId, assignedMemberId, 2, isCoordinatorForGroup = true)
    assertEquals(Errors.REBALANCE_IN_PROGRESS.code, heartbeatResult)
  }

  @Test
  def testHeartbeatIllegalGeneration() {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val joinGroupResult = joinGroup(groupId, memberId, DefaultSessionTimeout, protocolType, protocols,
      isCoordinatorForGroup = true)
    val assignedMemberId = joinGroupResult.memberId
    val joinGroupErrorCode = joinGroupResult.errorCode
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    EasyMock.reset(groupManager)
    val syncGroupResult = syncGroupLeader(groupId, joinGroupResult.generationId, assignedMemberId, Map(assignedMemberId -> Array[Byte]()), true)
    val syncGroupErrorCode = syncGroupResult._2
    assertEquals(Errors.NONE.code, syncGroupErrorCode)

    EasyMock.reset(groupManager)
    val heartbeatResult = heartbeat(groupId, assignedMemberId, 2, isCoordinatorForGroup = true)
    assertEquals(Errors.ILLEGAL_GENERATION.code, heartbeatResult)
  }

  @Test
  def testValidHeartbeat() {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val joinGroupResult = joinGroup(groupId, memberId, DefaultSessionTimeout, protocolType, protocols,
      isCoordinatorForGroup = true)
    val assignedConsumerId = joinGroupResult.memberId
    val generationId = joinGroupResult.generationId
    val joinGroupErrorCode = joinGroupResult.errorCode
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    EasyMock.reset(groupManager)
    val syncGroupResult = syncGroupLeader(groupId, generationId, assignedConsumerId, Map(assignedConsumerId -> Array[Byte]()), true)
    val syncGroupErrorCode = syncGroupResult._2
    assertEquals(Errors.NONE.code, syncGroupErrorCode)

    EasyMock.reset(groupManager)
    val heartbeatResult = heartbeat(groupId, assignedConsumerId, 1, isCoordinatorForGroup = true)
    assertEquals(Errors.NONE.code, heartbeatResult)
  }

  @Test
  def testSyncGroupNotCoordinator() {
    val generation = 1

    val syncGroupResult = syncGroupFollower(groupId, generation, memberId, false)
    assertEquals(Errors.NOT_COORDINATOR_FOR_GROUP.code, syncGroupResult._2)
  }

  @Test
  def testSyncGroupFromUnknownGroup() {
    val generation = 1

    val syncGroupResult = syncGroupFollower(groupId, generation, memberId, true)
    assertEquals(Errors.UNKNOWN_MEMBER_ID.code, syncGroupResult._2)
  }

  @Test
  def testSyncGroupFromUnknownMember() {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val joinGroupResult = joinGroup(groupId, memberId, DefaultSessionTimeout, protocolType, protocols,
      isCoordinatorForGroup = true)
    val assignedConsumerId = joinGroupResult.memberId
    val generationId = joinGroupResult.generationId
    assertEquals(Errors.NONE.code, joinGroupResult.errorCode)

    EasyMock.reset(groupManager)
    val syncGroupResult = syncGroupLeader(groupId, generationId, assignedConsumerId, Map(assignedConsumerId -> Array[Byte]()), true)
    val syncGroupErrorCode = syncGroupResult._2
    assertEquals(Errors.NONE.code, syncGroupErrorCode)

    EasyMock.reset(groupManager)
    val unknownMemberId = "blah"
    val unknownMemberSyncResult = syncGroupFollower(groupId, generationId, unknownMemberId, true)
    assertEquals(Errors.UNKNOWN_MEMBER_ID.code, unknownMemberSyncResult._2)
  }

  @Test
  def testSyncGroupFromIllegalGeneration() {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val joinGroupResult = joinGroup(groupId, memberId, DefaultSessionTimeout, protocolType, protocols,
      isCoordinatorForGroup = true)
    val assignedConsumerId = joinGroupResult.memberId
    val generationId = joinGroupResult.generationId
    assertEquals(Errors.NONE.code, joinGroupResult.errorCode)

    EasyMock.reset(groupManager)
    // send the sync group with an invalid generation
    val syncGroupResult = syncGroupLeader(groupId, generationId+1, assignedConsumerId, Map(assignedConsumerId -> Array[Byte]()), true)
    assertEquals(Errors.ILLEGAL_GENERATION.code, syncGroupResult._2)
  }

  @Test
  def testJoinGroupFromUnchangedFollowerDoesNotRebalance() {
    // to get a group of two members:
    // 1. join and sync with a single member (because we can't immediately join with two members)
    // 2. join and sync with the first member and a new member

    val firstJoinResult = joinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, DefaultSessionTimeout,
      protocolType, protocols, isCoordinatorForGroup = true)
    val firstMemberId = firstJoinResult.memberId
    val firstGenerationId = firstJoinResult.generationId
    assertEquals(firstMemberId, firstJoinResult.leaderId)
    assertEquals(Errors.NONE.code, firstJoinResult.errorCode)

    EasyMock.reset(groupManager)
    val firstSyncResult = syncGroupLeader(groupId, firstGenerationId, firstMemberId, Map(firstMemberId -> Array[Byte]()), true)
    assertEquals(Errors.NONE.code, firstSyncResult._2)

    EasyMock.reset(groupManager)
    val otherJoinFuture = sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, DefaultSessionTimeout,
      protocolType, protocols, isCoordinatorForGroup = true)

    EasyMock.reset(groupManager)
    val joinFuture = sendJoinGroup(groupId, firstMemberId, DefaultSessionTimeout, protocolType, protocols,
      isCoordinatorForGroup = true)

    val joinResult = await(joinFuture, DefaultSessionTimeout+100)
    val otherJoinResult = await(otherJoinFuture, DefaultSessionTimeout+100)
    assertEquals(Errors.NONE.code, joinResult.errorCode)
    assertEquals(Errors.NONE.code, otherJoinResult.errorCode)
    assertTrue(joinResult.generationId == otherJoinResult.generationId)

    assertEquals(firstMemberId, joinResult.leaderId)
    assertEquals(firstMemberId, otherJoinResult.leaderId)

    val nextGenerationId = joinResult.generationId

    // this shouldn't cause a rebalance since protocol information hasn't changed
    EasyMock.reset(groupManager)
    val followerJoinResult = joinGroup(groupId, otherJoinResult.memberId, DefaultSessionTimeout, protocolType, protocols,
      isCoordinatorForGroup = true)

    assertEquals(Errors.NONE.code, followerJoinResult.errorCode)
    assertEquals(nextGenerationId, followerJoinResult.generationId)
  }

  @Test
  def testJoinGroupFromUnchangedLeaderShouldRebalance() {
    val firstJoinResult = joinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, DefaultSessionTimeout,
      protocolType, protocols, isCoordinatorForGroup = true)
    val firstMemberId = firstJoinResult.memberId
    val firstGenerationId = firstJoinResult.generationId
    assertEquals(firstMemberId, firstJoinResult.leaderId)
    assertEquals(Errors.NONE.code, firstJoinResult.errorCode)

    EasyMock.reset(groupManager)
    val firstSyncResult = syncGroupLeader(groupId, firstGenerationId, firstMemberId, Map(firstMemberId -> Array[Byte]()), true)
    assertEquals(Errors.NONE.code, firstSyncResult._2)

    // join groups from the leader should force the group to rebalance, which allows the
    // leader to push new assignments when local metadata changes

    EasyMock.reset(groupManager)
    val secondJoinResult = joinGroup(groupId, firstMemberId, DefaultSessionTimeout, protocolType, protocols,
      isCoordinatorForGroup = true)

    assertEquals(Errors.NONE.code, secondJoinResult.errorCode)
    assertNotEquals(firstGenerationId, secondJoinResult.generationId)
  }

  @Test
  def testLeaderFailureInSyncGroup() {
    // to get a group of two members:
    // 1. join and sync with a single member (because we can't immediately join with two members)
    // 2. join and sync with the first member and a new member

    val firstJoinResult = joinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, DefaultSessionTimeout,
      protocolType, protocols, isCoordinatorForGroup = true)
    val firstMemberId = firstJoinResult.memberId
    val firstGenerationId = firstJoinResult.generationId
    assertEquals(firstMemberId, firstJoinResult.leaderId)
    assertEquals(Errors.NONE.code, firstJoinResult.errorCode)

    EasyMock.reset(groupManager)
    val firstSyncResult = syncGroupLeader(groupId, firstGenerationId, firstMemberId, Map(firstMemberId -> Array[Byte]()), true)
    assertEquals(Errors.NONE.code, firstSyncResult._2)

    EasyMock.reset(groupManager)
    val otherJoinFuture = sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, DefaultSessionTimeout,
      protocolType, protocols, isCoordinatorForGroup = true)

    EasyMock.reset(groupManager)
    val joinFuture = sendJoinGroup(groupId, firstMemberId, DefaultSessionTimeout, protocolType, protocols,
      isCoordinatorForGroup = true)

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
    EasyMock.reset(groupManager)
    val followerSyncFuture= sendSyncGroupFollower(groupId, nextGenerationId, otherJoinResult.memberId,
      isCoordinatorForGroup = true)
    val followerSyncResult = await(followerSyncFuture, DefaultSessionTimeout+100)
    assertEquals(Errors.REBALANCE_IN_PROGRESS.code, followerSyncResult._2)
  }

  @Test
  def testSyncGroupFollowerAfterLeader() {
    // to get a group of two members:
    // 1. join and sync with a single member (because we can't immediately join with two members)
    // 2. join and sync with the first member and a new member

    val firstJoinResult = joinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, DefaultSessionTimeout,
      protocolType, protocols, isCoordinatorForGroup = true)
    val firstMemberId = firstJoinResult.memberId
    val firstGenerationId = firstJoinResult.generationId
    assertEquals(firstMemberId, firstJoinResult.leaderId)
    assertEquals(Errors.NONE.code, firstJoinResult.errorCode)

    EasyMock.reset(groupManager)
    val firstSyncResult = syncGroupLeader(groupId, firstGenerationId, firstMemberId, Map(firstMemberId -> Array[Byte]()), true)
    assertEquals(Errors.NONE.code, firstSyncResult._2)

    EasyMock.reset(groupManager)
    val otherJoinFuture = sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, DefaultSessionTimeout,
      protocolType, protocols, isCoordinatorForGroup = true)

    EasyMock.reset(groupManager)
    val joinFuture = sendJoinGroup(groupId, firstMemberId, DefaultSessionTimeout, protocolType, protocols,
      isCoordinatorForGroup = true)

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

    EasyMock.reset(groupManager)
    val leaderSyncResult = syncGroupLeader(groupId, nextGenerationId, leaderId,
      Map(leaderId -> leaderAssignment, followerId -> followerAssignment), isCoordinatorForGroup = true)
    assertEquals(Errors.NONE.code, leaderSyncResult._2)
    assertEquals(leaderAssignment, leaderSyncResult._1)

    EasyMock.reset(groupManager)
    val followerSyncResult = syncGroupFollower(groupId, nextGenerationId, otherJoinResult.memberId,
      isCoordinatorForGroup = true)
    assertEquals(Errors.NONE.code, followerSyncResult._2)
    assertEquals(followerAssignment, followerSyncResult._1)
  }

  @Test
  def testSyncGroupLeaderAfterFollower() {
    // to get a group of two members:
    // 1. join and sync with a single member (because we can't immediately join with two members)
    // 2. join and sync with the first member and a new member

    val joinGroupResult = joinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, DefaultSessionTimeout,
      protocolType, protocols, isCoordinatorForGroup = true)
    val firstMemberId = joinGroupResult.memberId
    val firstGenerationId = joinGroupResult.generationId
    assertEquals(firstMemberId, joinGroupResult.leaderId)
    assertEquals(Errors.NONE.code, joinGroupResult.errorCode)

    EasyMock.reset(groupManager)
    val syncGroupResult = syncGroupLeader(groupId, firstGenerationId, firstMemberId, Map(firstMemberId -> Array[Byte]()), true)
    val syncGroupErrorCode = syncGroupResult._2
    assertEquals(Errors.NONE.code, syncGroupErrorCode)

    EasyMock.reset(groupManager)
    val otherJoinFuture = sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, DefaultSessionTimeout,
      protocolType, protocols, isCoordinatorForGroup = true)

    EasyMock.reset(groupManager)
    val joinFuture = sendJoinGroup(groupId, firstMemberId, DefaultSessionTimeout, protocolType, protocols,
      isCoordinatorForGroup = true)

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

    EasyMock.reset(groupManager)
    val followerSyncFuture = sendSyncGroupFollower(groupId, nextGenerationId, followerId, isCoordinatorForGroup = true)

    EasyMock.reset(groupManager)
    val leaderSyncResult = syncGroupLeader(groupId, nextGenerationId, leaderId,
      Map(leaderId -> leaderAssignment, followerId -> followerAssignment), isCoordinatorForGroup = true)
    assertEquals(Errors.NONE.code, leaderSyncResult._2)
    assertEquals(leaderAssignment, leaderSyncResult._1)

    val followerSyncResult = await(followerSyncFuture, DefaultSessionTimeout+100)
    assertEquals(Errors.NONE.code, followerSyncResult._2)
    assertEquals(followerAssignment, followerSyncResult._1)
  }

  @Test
  def testCommitOffsetFromUnknownGroup() {
    val generationId = 1
    val tp = new TopicAndPartition("topic", 0)
    val offset = OffsetAndMetadata(0)

    val commitOffsetResult = commitOffsets(groupId, memberId, generationId, Map(tp -> offset), true)
    assertEquals(Errors.ILLEGAL_GENERATION.code, commitOffsetResult(tp))
  }

  @Test
  def testCommitOffsetWithDefaultGeneration() {
    val tp = new TopicAndPartition("topic", 0)
    val offset = OffsetAndMetadata(0)

    val commitOffsetResult = commitOffsets(groupId, OffsetCommitRequest.DEFAULT_MEMBER_ID,
      OffsetCommitRequest.DEFAULT_GENERATION_ID, Map(tp -> offset), true)
    assertEquals(Errors.NONE.code, commitOffsetResult(tp))
  }

  @Test
  def testCommitOffsetInAwaitingSync() {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val tp = new TopicAndPartition("topic", 0)
    val offset = OffsetAndMetadata(0)

    val joinGroupResult = joinGroup(groupId, memberId, DefaultSessionTimeout, protocolType, protocols,
      isCoordinatorForGroup = true)
    val assignedMemberId = joinGroupResult.memberId
    val generationId = joinGroupResult.generationId
    val joinGroupErrorCode = joinGroupResult.errorCode
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    EasyMock.reset(groupManager)
    val commitOffsetResult = commitOffsets(groupId, assignedMemberId, generationId, Map(tp -> offset), true)
    assertEquals(Errors.REBALANCE_IN_PROGRESS.code, commitOffsetResult(tp))
  }

  @Test
  def testHeartbeatDuringRebalanceCausesRebalanceInProgress() {
    // First start up a group (with a slightly larger timeout to give us time to heartbeat when the rebalance starts)
    val joinGroupResult = joinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, DefaultSessionTimeout,
      protocolType, protocols, isCoordinatorForGroup = true)
    val assignedConsumerId = joinGroupResult.memberId
    val initialGenerationId = joinGroupResult.generationId
    val joinGroupErrorCode = joinGroupResult.errorCode
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    // Then join with a new consumer to trigger a rebalance
    EasyMock.reset(groupManager)
    sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, DefaultSessionTimeout, protocolType, protocols,
      isCoordinatorForGroup = true)

    // We should be in the middle of a rebalance, so the heartbeat should return rebalance in progress
    EasyMock.reset(groupManager)
    val heartbeatResult = heartbeat(groupId, assignedConsumerId, initialGenerationId, isCoordinatorForGroup = true)
    assertEquals(Errors.REBALANCE_IN_PROGRESS.code, heartbeatResult)
  }

  @Test
  def testGenerationIdIncrementsOnRebalance() {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val otherMemberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val joinGroupResult = joinGroup(groupId, memberId, DefaultSessionTimeout, protocolType, protocols,
      isCoordinatorForGroup = true)
    val initialGenerationId = joinGroupResult.generationId
    val joinGroupErrorCode = joinGroupResult.errorCode
    assertEquals(1, initialGenerationId)
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    EasyMock.reset(groupManager)
    val otherJoinGroupResult = joinGroup(groupId, otherMemberId, DefaultSessionTimeout, protocolType, protocols,
      isCoordinatorForGroup = true)
    val nextGenerationId = otherJoinGroupResult.generationId
    val otherJoinGroupErrorCode = otherJoinGroupResult.errorCode
    assertEquals(2, nextGenerationId)
    assertEquals(Errors.NONE.code, otherJoinGroupErrorCode)
  }

  @Test
  def testLeaveGroupWrongCoordinator() {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val leaveGroupResult = leaveGroup(groupId, memberId, isCoordinatorForGroup = false)
    assertEquals(Errors.NOT_COORDINATOR_FOR_GROUP.code, leaveGroupResult)
  }

  @Test
  def testLeaveGroupUnknownGroup() {

    val leaveGroupResult = leaveGroup(groupId, memberId, isCoordinatorForGroup = true)
    assertEquals(Errors.UNKNOWN_MEMBER_ID.code, leaveGroupResult)
  }

  @Test
  def testLeaveGroupUnknownConsumerExistingGroup() {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val otherMemberId = "consumerId"

    val joinGroupResult = joinGroup(groupId, memberId, DefaultSessionTimeout, protocolType, protocols,
      isCoordinatorForGroup = true)
    val joinGroupErrorCode = joinGroupResult.errorCode
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    EasyMock.reset(groupManager)
    val leaveGroupResult = leaveGroup(groupId, otherMemberId, isCoordinatorForGroup = true)
    assertEquals(Errors.UNKNOWN_MEMBER_ID.code, leaveGroupResult)
  }

  @Test
  def testValidLeaveGroup() {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val joinGroupResult = joinGroup(groupId, memberId, DefaultSessionTimeout, protocolType, protocols,
      isCoordinatorForGroup = true)
    val assignedMemberId = joinGroupResult.memberId
    val joinGroupErrorCode = joinGroupResult.errorCode
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    EasyMock.reset(groupManager)
    val leaveGroupResult = leaveGroup(groupId, assignedMemberId, isCoordinatorForGroup = true)
    assertEquals(Errors.NONE.code, leaveGroupResult)
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
                            sessionTimeout: Int,
                            protocolType: String,
                            protocols: List[(String, Array[Byte])],
                            isCoordinatorForGroup: Boolean): Future[JoinGroupResult] = {
    val (responseFuture, responseCallback) = setupJoinGroupCallback

    EasyMock.expect(groupManager.partitionFor(groupId)).andReturn(1)
    EasyMock.expect(groupManager.partitionLeaderIsLocal(1)).andReturn(isCoordinatorForGroup)
    EasyMock.expect(groupManager.partitionFor(groupId)).andReturn(1)
    EasyMock.expect(groupManager.partitionLoadingInProgress(1)).andReturn(false)
    EasyMock.expect(groupManager.getGroup(groupId)).andReturn(groupsCache.get(groupId))
    if (!groupsCache.contains(groupId)) {
      val groupMetadata = new GroupMetadata(groupId, protocolType)



      groupMetadata.add(memberId, new MemberMetadata(memberId, groupId, sessionTimeout, protocols))
      groupsCache.putIfNotExists(groupId, groupMetadata)

      EasyMock.expect(groupManager.addGroup(groupId, protocolType)).andReturn(groupMetadata)
    }
    EasyMock.expect(groupManager.propagateAssignment(groupsCache.get(groupId), Errors.REBALANCE_IN_PROGRESS.code))
      .andAnswer(propagateRebalanceAnswer)

    EasyMock.replay(groupManager)

    consumerCoordinator.handleJoinGroup(groupId, memberId, sessionTimeout, protocolType, protocols, responseCallback)
    responseFuture
  }


  private def sendSyncGroupLeader(groupId: String,
                                  generation: Int,
                                  leaderId: String,
                                  assignment: Map[String, Array[Byte]],
                                  isCoordinatorForGroup: Boolean): Future[SyncGroupCallbackParams] = {
    val (responseFuture, responseCallback) = setupSyncGroupCallback

    val storeGroupAnswer = new IAnswer[Unit] {
      override def answer = {
        for (member <- groupsCache.get(groupId).allMembers) {
          member.assignment = assignment.getOrElse(member.memberId, Array.empty[Byte])
          if (member.awaitingSyncCallback != null) {
            member.awaitingSyncCallback.apply(member.assignment, Errors.NONE.code)
            member.awaitingSyncCallback = null
          }
        }
      }
    }

    EasyMock.expect(groupManager.partitionFor(groupId)).andReturn(1)
    EasyMock.expect(groupManager.partitionLeaderIsLocal(1)).andReturn(isCoordinatorForGroup)
    EasyMock.expect(groupManager.getGroup(groupId)).andReturn(groupsCache.get(groupId))
    if (groupsCache.contains(groupId)) {
      val members = groupsCache.get(groupId).allMembers.filter(_.memberId.equals(leaderId))
      if (members.length != 1) {
        responseCallback.apply(null, Errors.UNKNOWN_MEMBER_ID.code)
      } else {
        members.head.awaitingSyncCallback = responseCallback
      }
    }

    EasyMock.expect(groupManager.storeGroup(groupsCache.get(groupId), assignment)).andAnswer(storeGroupAnswer)
    EasyMock.replay(groupManager)

    consumerCoordinator.handleSyncGroup(groupId, generation, leaderId, assignment, responseCallback)
    responseFuture
  }

  private def sendSyncGroupFollower(groupId: String,
                                    generation: Int,
                                    memberId: String,
                                    isCoordinatorForGroup: Boolean): Future[SyncGroupCallbackParams] = {
    val (responseFuture, responseCallback) = setupSyncGroupCallback

    EasyMock.expect(groupManager.partitionFor(groupId)).andReturn(1)
    EasyMock.expect(groupManager.partitionLeaderIsLocal(1)).andReturn(isCoordinatorForGroup)
    EasyMock.expect(groupManager.getGroup(groupId)).andReturn(groupsCache.get(groupId))
    if (groupsCache.contains(groupId)) {
      val members = groupsCache.get(groupId).allMembers.filter(_.memberId.equals(memberId))
      if (members.length != 1) {
        responseCallback.apply(null, Errors.UNKNOWN_MEMBER_ID.code)
      } else {
        val member = members.head
        if (member.assignment != null) {
          responseCallback.apply(member.assignment, Errors.NONE.code)
        } else {
          member.awaitingSyncCallback = responseCallback
        }
      }
    } else {
      responseCallback.apply(null, Errors.UNKNOWN_MEMBER_ID.code)
    }
    EasyMock.expect(groupManager.propagateAssignment(groupsCache.get(groupId), Errors.REBALANCE_IN_PROGRESS.code))
      .andAnswer(propagateRebalanceAnswer)
    EasyMock.replay(groupManager)

    consumerCoordinator.handleSyncGroup(groupId, generation, memberId, Map.empty[String, Array[Byte]], responseCallback)
    responseFuture
  }

  private def joinGroup(groupId: String,
                        memberId: String,
                        sessionTimeout: Int,
                        protocolType: String,
                        protocols: List[(String, Array[Byte])],
                        isCoordinatorForGroup: Boolean): JoinGroupResult = {
    val responseFuture = sendJoinGroup(groupId, memberId, sessionTimeout, protocolType, protocols, isCoordinatorForGroup)
    // should only have to wait as long as session timeout, but allow some extra time in case of an unexpected delay
    Await.result(responseFuture, Duration(sessionTimeout+100, TimeUnit.MILLISECONDS))
  }


  private def syncGroupFollower(groupId: String,
                                generationId: Int,
                                memberId: String,
                                isCoordinatorForGroup: Boolean): SyncGroupCallbackParams = {
    val responseFuture = sendSyncGroupFollower(groupId, generationId, memberId, isCoordinatorForGroup)
    Await.result(responseFuture, Duration(DefaultSessionTimeout+100, TimeUnit.MILLISECONDS))
  }

  private def syncGroupLeader(groupId: String,
                              generationId: Int,
                              memberId: String,
                              assignment: Map[String, Array[Byte]],
                              isCoordinatorForGroup: Boolean): SyncGroupCallbackParams = {
    val responseFuture = sendSyncGroupLeader(groupId, generationId, memberId, assignment, isCoordinatorForGroup)
    Await.result(responseFuture, Duration(DefaultSessionTimeout+100, TimeUnit.MILLISECONDS))
  }

  private def heartbeat(groupId: String,
                        consumerId: String,
                        generationId: Int,
                        isCoordinatorForGroup: Boolean): HeartbeatCallbackParams = {
    val (responseFuture, responseCallback) = setupHeartbeatCallback

    EasyMock.expect(groupManager.partitionFor(groupId)).andReturn(1)
    EasyMock.expect(groupManager.partitionLeaderIsLocal(1)).andReturn(isCoordinatorForGroup)
    EasyMock.expect(groupManager.partitionFor(groupId)).andReturn(1)
    EasyMock.expect(groupManager.partitionLoadingInProgress(1)).andReturn(false)
    EasyMock.expect(groupManager.getGroup(groupId)).andReturn(groupsCache.get(groupId))
    EasyMock.replay(groupManager)

    consumerCoordinator.handleHeartbeat(groupId, consumerId, generationId, responseCallback)
    Await.result(responseFuture, Duration(40, TimeUnit.MILLISECONDS))
  }

  private def await[T](future: Future[T], millis: Long): T = {
    Await.result(future, Duration(millis, TimeUnit.MILLISECONDS))
  }

  private def commitOffsets(groupId: String,
                            consumerId: String,
                            generationId: Int,
                            offsets: Map[TopicAndPartition, OffsetAndMetadata],
                            isCoordinatorForGroup: Boolean): CommitOffsetCallbackParams = {
    val (responseFuture, responseCallback) = setupCommitOffsetsCallback

    val storeOffsetAnswer = new IAnswer[Unit] {
      override def answer = responseCallback.apply(offsets.mapValues(_ => Errors.NONE.code))
    }

    EasyMock.expect(groupManager.partitionFor(groupId)).andReturn(1)
    EasyMock.expect(groupManager.partitionLeaderIsLocal(1)).andReturn(isCoordinatorForGroup)
    EasyMock.expect(groupManager.partitionFor(groupId)).andReturn(1)
    EasyMock.expect(groupManager.partitionLoadingInProgress(1)).andReturn(false)
    EasyMock.expect(groupManager.getGroup(groupId)).andReturn(groupsCache.get(groupId))
    EasyMock.expect(groupManager.storeOffsets(groupId, consumerId, generationId, offsets, responseCallback))
      .andAnswer(storeOffsetAnswer)
    EasyMock.replay(groupManager)

    consumerCoordinator.handleCommitOffsets(groupId, consumerId, generationId, offsets, responseCallback)
    Await.result(responseFuture, Duration(40, TimeUnit.MILLISECONDS))
  }

  private def leaveGroup(groupId: String, consumerId: String, isCoordinatorForGroup: Boolean): LeaveGroupCallbackParams = {
    val (responseFuture, responseCallback) = setupHeartbeatCallback

    EasyMock.expect(groupManager.partitionFor(groupId)).andReturn(1)
    EasyMock.expect(groupManager.partitionLeaderIsLocal(1)).andReturn(isCoordinatorForGroup)
    EasyMock.expect(groupManager.partitionFor(groupId)).andReturn(1)
    EasyMock.expect(groupManager.partitionLoadingInProgress(1)).andReturn(false)
    EasyMock.expect(groupManager.getGroup(groupId)).andReturn(groupsCache.get(groupId))
    EasyMock.expect(groupManager.propagateAssignment(groupsCache.get(groupId), Errors.REBALANCE_IN_PROGRESS.code))
      .andAnswer(propagateRebalanceAnswer)
    EasyMock.expect(groupManager.removeGroup(groupId))
    EasyMock.replay(groupManager)

    consumerCoordinator.handleLeaveGroup(groupId, consumerId, responseCallback)
    Await.result(responseFuture, Duration(40, TimeUnit.MILLISECONDS))
  }
}
