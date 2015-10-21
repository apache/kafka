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
import kafka.server.{OffsetManager, KafkaConfig}
import kafka.utils.TestUtils
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
  var offsetManager : OffsetManager = null

  @Before
  def setUp() {
    val props = TestUtils.createBrokerConfig(nodeId = 0, zkConnect = "")
    props.setProperty(KafkaConfig.GroupMinSessionTimeoutMsProp, ConsumerMinSessionTimeout.toString)
    props.setProperty(KafkaConfig.GroupMaxSessionTimeoutMsProp, ConsumerMaxSessionTimeout.toString)
    offsetManager = EasyMock.createStrictMock(classOf[OffsetManager])
    consumerCoordinator = GroupCoordinator.create(KafkaConfig.fromProps(props), null, offsetManager)
    consumerCoordinator.startup()
  }

  @After
  def tearDown() {
    EasyMock.reset(offsetManager)
    consumerCoordinator.shutdown()
  }

  @Test
  def testJoinGroupWrongCoordinator() {
    val groupId = "groupId"
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val metadata = Array[Byte]()
    val protocolType = "consumer"
    val protocols = List(("range", metadata))

    val joinGroupResult = joinGroup(groupId, memberId, DefaultSessionTimeout, protocolType,
      protocols, isCoordinatorForGroup = false)
    val joinGroupErrorCode = joinGroupResult.errorCode
    assertEquals(Errors.NOT_COORDINATOR_FOR_GROUP.code, joinGroupErrorCode)
  }

  @Test
  def testJoinGroupSessionTimeoutTooSmall() {
    val groupId = "groupId"
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val metadata = Array[Byte]()
    val protocolType = "consumer"
    val protocols = List(("range", metadata))

    val joinGroupResult = joinGroup(groupId, memberId, ConsumerMinSessionTimeout - 1, protocolType, protocols,
      isCoordinatorForGroup = true)
    val joinGroupErrorCode = joinGroupResult.errorCode
    assertEquals(Errors.INVALID_SESSION_TIMEOUT.code, joinGroupErrorCode)
  }

  @Test
  def testJoinGroupSessionTimeoutTooLarge() {
    val groupId = "groupId"
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val metadata = Array[Byte]()
    val protocolType = "consumer"
    val protocols = List(("range", metadata))

    val joinGroupResult = joinGroup(groupId, memberId, ConsumerMaxSessionTimeout + 1, protocolType, protocols,
      isCoordinatorForGroup = true)
    val joinGroupErrorCode = joinGroupResult.errorCode
    assertEquals(Errors.INVALID_SESSION_TIMEOUT.code, joinGroupErrorCode)
  }

  @Test
  def testJoinGroupUnknownConsumerNewGroup() {
    val groupId = "groupId"
    val memberId = "memberId"
    val metadata = Array[Byte]()
    val protocolType = "consumer"
    val protocols = List(("range", metadata))

    val joinGroupResult = joinGroup(groupId, memberId, DefaultSessionTimeout, protocolType, protocols,
      isCoordinatorForGroup = true)
    val joinGroupErrorCode = joinGroupResult.errorCode
    assertEquals(Errors.UNKNOWN_MEMBER_ID.code, joinGroupErrorCode)
  }

  @Test
  def testValidJoinGroup() {
    val groupId = "groupId"
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val metadata = Array[Byte]()
    val protocolType = "consumer"
    val protocols = List(("range", metadata))

    val joinGroupResult = joinGroup(groupId, memberId, DefaultSessionTimeout, protocolType,
      protocols, isCoordinatorForGroup = true)
    val joinGroupErrorCode = joinGroupResult.errorCode
    assertEquals(Errors.NONE.code, joinGroupErrorCode)
  }

  @Test
  def testJoinGroupInconsistentProtocolType() {
    val groupId = "groupId"
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val metadata = Array[Byte]()

    val otherMemberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val joinGroupResult = joinGroup(groupId, memberId, DefaultSessionTimeout, "consumer", List(("range", metadata)),
      isCoordinatorForGroup = true)
    assertEquals(Errors.NONE.code, joinGroupResult.errorCode)

    EasyMock.reset(offsetManager)
    val otherJoinGroupResult = joinGroup(groupId, otherMemberId, DefaultSessionTimeout, "copycat",
      List(("range", metadata)), isCoordinatorForGroup = true)
    assertEquals(Errors.INCONSISTENT_GROUP_PROTOCOL.code, otherJoinGroupResult.errorCode)
  }

  @Test
  def testJoinGroupInconsistentGroupProtocol() {
    val groupId = "groupId"
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val protocolType = "consumer"
    val metadata = Array[Byte]()

    val otherMemberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val joinGroupResult = joinGroup(groupId, memberId, DefaultSessionTimeout, protocolType, List(("range", metadata)),
      isCoordinatorForGroup = true)
    assertEquals(Errors.NONE.code, joinGroupResult.errorCode)

    EasyMock.reset(offsetManager)
    val otherJoinGroupResult = joinGroup(groupId, otherMemberId, DefaultSessionTimeout, protocolType,
      List(("roundrobin", metadata)), isCoordinatorForGroup = true)
    assertEquals(Errors.INCONSISTENT_GROUP_PROTOCOL.code, otherJoinGroupResult.errorCode)
  }

  @Test
  def testJoinGroupUnknownConsumerExistingGroup() {
    val groupId = "groupId"
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val otherMemberId = "memberId"
    val metadata = Array[Byte]()
    val protocolType = "consumer"
    val protocols = List(("range", metadata))

    val joinGroupResult = joinGroup(groupId, memberId, DefaultSessionTimeout, protocolType, protocols,
      isCoordinatorForGroup = true)
    assertEquals(Errors.NONE.code, joinGroupResult.errorCode)

    EasyMock.reset(offsetManager)
    val otherJoinGroupResult = joinGroup(groupId, otherMemberId, DefaultSessionTimeout, protocolType, protocols,
      isCoordinatorForGroup = true)
    assertEquals(Errors.UNKNOWN_MEMBER_ID.code, otherJoinGroupResult.errorCode)
  }

  @Test
  def testHeartbeatWrongCoordinator() {
    val groupId = "groupId"
    val consumerId = "memberId"

    val heartbeatResult = heartbeat(groupId, consumerId, -1, isCoordinatorForGroup = false)
    assertEquals(Errors.NOT_COORDINATOR_FOR_GROUP.code, heartbeatResult)
  }

  @Test
  def testHeartbeatUnknownGroup() {
    val groupId = "groupId"
    val consumerId = "memberId"

    val heartbeatResult = heartbeat(groupId, consumerId, -1, isCoordinatorForGroup = true)
    assertEquals(Errors.UNKNOWN_MEMBER_ID.code, heartbeatResult)
  }

  @Test
  def testHeartbeatUnknownConsumerExistingGroup() {
    val groupId = "groupId"
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val otherMemberId = "memberId"
    val metadata = Array[Byte]()
    val protocolType = "consumer"
    val protocols = List(("range", metadata))

    val joinGroupResult = joinGroup(groupId, memberId, DefaultSessionTimeout, protocolType, protocols,
      isCoordinatorForGroup = true)
    val joinGroupErrorCode = joinGroupResult.errorCode
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    EasyMock.reset(offsetManager)
    val syncGroupResult = syncGroupLeader(groupId, joinGroupResult.generationId, joinGroupResult.memberId, Map.empty, true)
    val syncGroupErrorCode = syncGroupResult._2
    assertEquals(Errors.NONE.code, syncGroupErrorCode)

    EasyMock.reset(offsetManager)
    val heartbeatResult = heartbeat(groupId, otherMemberId, 1, isCoordinatorForGroup = true)
    assertEquals(Errors.UNKNOWN_MEMBER_ID.code, heartbeatResult)
  }

  @Test
  def testHeartbeatRebalanceInProgress() {
    val groupId = "groupId"
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val metadata = Array[Byte]()
    val protocolType = "consumer"
    val protocols = List(("range", metadata))

    val joinGroupResult = joinGroup(groupId, memberId, DefaultSessionTimeout, protocolType, protocols,
      isCoordinatorForGroup = true)
    val assignedMemberId = joinGroupResult.memberId
    val joinGroupErrorCode = joinGroupResult.errorCode
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    EasyMock.reset(offsetManager)
    val heartbeatResult = heartbeat(groupId, assignedMemberId, 2, isCoordinatorForGroup = true)
    assertEquals(Errors.REBALANCE_IN_PROGRESS.code, heartbeatResult)
  }

  @Test
  def testHeartbeatIllegalGeneration() {
    val groupId = "groupId"
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val metadata = Array[Byte]()
    val protocolType = "consumer"
    val protocols = List(("range", metadata))

    val joinGroupResult = joinGroup(groupId, memberId, DefaultSessionTimeout, protocolType, protocols,
      isCoordinatorForGroup = true)
    val assignedMemberId = joinGroupResult.memberId
    val joinGroupErrorCode = joinGroupResult.errorCode
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    EasyMock.reset(offsetManager)
    val syncGroupResult = syncGroupLeader(groupId, joinGroupResult.generationId, assignedMemberId, Map.empty, true)
    val syncGroupErrorCode = syncGroupResult._2
    assertEquals(Errors.NONE.code, syncGroupErrorCode)

    EasyMock.reset(offsetManager)
    val heartbeatResult = heartbeat(groupId, assignedMemberId, 2, isCoordinatorForGroup = true)
    assertEquals(Errors.ILLEGAL_GENERATION.code, heartbeatResult)
  }

  @Test
  def testValidHeartbeat() {
    val groupId = "groupId"
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val metadata = Array[Byte]()
    val protocolType = "consumer"
    val protocols = List(("range", metadata))

    val joinGroupResult = joinGroup(groupId, memberId, DefaultSessionTimeout, protocolType, protocols,
      isCoordinatorForGroup = true)
    val assignedConsumerId = joinGroupResult.memberId
    val generationId = joinGroupResult.generationId
    val joinGroupErrorCode = joinGroupResult.errorCode
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    EasyMock.reset(offsetManager)
    val syncGroupResult = syncGroupLeader(groupId, generationId, assignedConsumerId, Map.empty, true)
    val syncGroupErrorCode = syncGroupResult._2
    assertEquals(Errors.NONE.code, syncGroupErrorCode)

    EasyMock.reset(offsetManager)
    val heartbeatResult = heartbeat(groupId, assignedConsumerId, 1, isCoordinatorForGroup = true)
    assertEquals(Errors.NONE.code, heartbeatResult)
  }

  @Test
  def testSyncGroupNotCoordinator() {
    val groupId = "groupId"
    val memberId = "member"
    val generation = 1

    val syncGroupResult = syncGroupFollower(groupId, generation, memberId, false)
    assertEquals(Errors.NOT_COORDINATOR_FOR_GROUP.code, syncGroupResult._2)
  }

  @Test
  def testSyncGroupFromUnknownGroup() {
    val groupId = "groupId"
    val memberId = "member"
    val generation = 1

    val syncGroupResult = syncGroupFollower(groupId, generation, memberId, true)
    assertEquals(Errors.UNKNOWN_MEMBER_ID.code, syncGroupResult._2)
  }

  @Test
  def testSyncGroupFromUnknownMember() {
    val groupId = "groupId"
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val metadata = Array[Byte]()
    val protocolType = "consumer"
    val protocols = List(("range", metadata))

    val joinGroupResult = joinGroup(groupId, memberId, DefaultSessionTimeout, protocolType, protocols,
      isCoordinatorForGroup = true)
    val assignedConsumerId = joinGroupResult.memberId
    val generationId = joinGroupResult.generationId
    assertEquals(Errors.NONE.code, joinGroupResult.errorCode)

    EasyMock.reset(offsetManager)
    val syncGroupResult = syncGroupLeader(groupId, generationId, assignedConsumerId, Map.empty, true)
    val syncGroupErrorCode = syncGroupResult._2
    assertEquals(Errors.NONE.code, syncGroupErrorCode)

    EasyMock.reset(offsetManager)
    val unknownMemberId = "blah"
    val unknownMemberSyncResult = syncGroupFollower(groupId, generationId, unknownMemberId, true)
    assertEquals(Errors.UNKNOWN_MEMBER_ID.code, unknownMemberSyncResult._2)
  }

  @Test
  def testSyncGroupFromIllegalGeneration() {
    val groupId = "groupId"
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val metadata = Array[Byte]()
    val protocolType = "consumer"
    val protocols = List(("range", metadata))

    val joinGroupResult = joinGroup(groupId, memberId, DefaultSessionTimeout, protocolType, protocols,
      isCoordinatorForGroup = true)
    val assignedConsumerId = joinGroupResult.memberId
    val generationId = joinGroupResult.generationId
    assertEquals(Errors.NONE.code, joinGroupResult.errorCode)

    EasyMock.reset(offsetManager)
    // send the sync group with an invalid generation
    val syncGroupResult = syncGroupLeader(groupId, generationId+1, assignedConsumerId, Map.empty, true)
    assertEquals(Errors.ILLEGAL_GENERATION.code, syncGroupResult._2)
  }

  @Test
  def testJoinGroupFromUnchangedFollowerDoesNotRebalance() {
    val groupId = "groupId"
    val protocolType = "consumer"
    val protocols = List(("range", Array[Byte]()))

    // to get a group of two members:
    // 1. join and sync with a single member (because we can't immediately join with two members)
    // 2. join and sync with the first member and a new member

    val firstJoinResult = joinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, DefaultSessionTimeout,
      protocolType, protocols, isCoordinatorForGroup = true)
    val firstMemberId = firstJoinResult.memberId
    val firstGenerationId = firstJoinResult.generationId
    assertEquals(firstMemberId, firstJoinResult.leaderId)
    assertEquals(Errors.NONE.code, firstJoinResult.errorCode)

    EasyMock.reset(offsetManager)
    val firstSyncResult = syncGroupLeader(groupId, firstGenerationId, firstMemberId, Map.empty, true)
    assertEquals(Errors.NONE.code, firstSyncResult._2)

    EasyMock.reset(offsetManager)
    val otherJoinFuture = sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, DefaultSessionTimeout,
      protocolType, protocols, isCoordinatorForGroup = true)

    EasyMock.reset(offsetManager)
    val joinFuture = sendJoinGroup(groupId, firstMemberId, DefaultSessionTimeout, protocolType, protocols,
      isCoordinatorForGroup = true);

    val joinResult = await(joinFuture, DefaultSessionTimeout+100)
    val otherJoinResult = await(otherJoinFuture, DefaultSessionTimeout+100)
    assertEquals(Errors.NONE.code, joinResult.errorCode)
    assertEquals(Errors.NONE.code, otherJoinResult.errorCode)
    assertTrue(joinResult.generationId == otherJoinResult.generationId)

    assertEquals(firstMemberId, joinResult.leaderId)
    assertEquals(firstMemberId, otherJoinResult.leaderId)

    val nextGenerationId = joinResult.generationId

    // this shouldn't cause a rebalance since protocol information hasn't changed
    EasyMock.reset(offsetManager)
    val followerJoinResult = joinGroup(groupId, otherJoinResult.memberId, DefaultSessionTimeout, protocolType, protocols,
      isCoordinatorForGroup = true)

    assertEquals(Errors.NONE.code, followerJoinResult.errorCode)
    assertEquals(nextGenerationId, followerJoinResult.generationId)
  }

  @Test
  def testJoinGroupFromUnchangedLeaderShouldRebalance() {
    val groupId = "groupId"
    val protocolType = "consumer"
    val protocols = List(("range", Array[Byte]()))

    val firstJoinResult = joinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, DefaultSessionTimeout,
      protocolType, protocols, isCoordinatorForGroup = true)
    val firstMemberId = firstJoinResult.memberId
    val firstGenerationId = firstJoinResult.generationId
    assertEquals(firstMemberId, firstJoinResult.leaderId)
    assertEquals(Errors.NONE.code, firstJoinResult.errorCode)

    EasyMock.reset(offsetManager)
    val firstSyncResult = syncGroupLeader(groupId, firstGenerationId, firstMemberId, Map.empty, true)
    assertEquals(Errors.NONE.code, firstSyncResult._2)

    // join groups from the leader should force the group to rebalance, which allows the
    // leader to push new assignments when local metadata changes

    EasyMock.reset(offsetManager)
    val secondJoinResult = joinGroup(groupId, firstMemberId, DefaultSessionTimeout, protocolType, protocols,
      isCoordinatorForGroup = true)

    assertEquals(Errors.NONE.code, secondJoinResult.errorCode)
    assertNotEquals(firstGenerationId, secondJoinResult.generationId)
  }

  @Test
  def testLeaderFailureInSyncGroup() {
    val groupId = "groupId"
    val protocolType = "consumer"
    val protocols = List(("range", Array[Byte]()))

    // to get a group of two members:
    // 1. join and sync with a single member (because we can't immediately join with two members)
    // 2. join and sync with the first member and a new member

    val firstJoinResult = joinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, DefaultSessionTimeout,
      protocolType, protocols, isCoordinatorForGroup = true)
    val firstMemberId = firstJoinResult.memberId
    val firstGenerationId = firstJoinResult.generationId
    assertEquals(firstMemberId, firstJoinResult.leaderId)
    assertEquals(Errors.NONE.code, firstJoinResult.errorCode)

    EasyMock.reset(offsetManager)
    val firstSyncResult = syncGroupLeader(groupId, firstGenerationId, firstMemberId, Map.empty, true)
    assertEquals(Errors.NONE.code, firstSyncResult._2)

    EasyMock.reset(offsetManager)
    val otherJoinFuture = sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, DefaultSessionTimeout,
      protocolType, protocols, isCoordinatorForGroup = true)

    EasyMock.reset(offsetManager)
    val joinFuture = sendJoinGroup(groupId, firstMemberId, DefaultSessionTimeout, protocolType, protocols,
      isCoordinatorForGroup = true);

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
    EasyMock.reset(offsetManager)
    val followerSyncFuture= sendSyncGroupFollower(groupId, nextGenerationId, otherJoinResult.memberId,
      isCoordinatorForGroup = true)
    val followerSyncResult = await(followerSyncFuture, DefaultSessionTimeout+100)
    assertEquals(Errors.REBALANCE_IN_PROGRESS.code, followerSyncResult._2)
  }

  @Test
  def testSyncGroupFollowerAfterLeader() {
    val groupId = "groupId"
    val protocolType = "consumer"
    val protocols = List(("range", Array[Byte]()))

    // to get a group of two members:
    // 1. join and sync with a single member (because we can't immediately join with two members)
    // 2. join and sync with the first member and a new member

    val firstJoinResult = joinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, DefaultSessionTimeout,
      protocolType, protocols, isCoordinatorForGroup = true)
    val firstMemberId = firstJoinResult.memberId
    val firstGenerationId = firstJoinResult.generationId
    assertEquals(firstMemberId, firstJoinResult.leaderId)
    assertEquals(Errors.NONE.code, firstJoinResult.errorCode)

    EasyMock.reset(offsetManager)
    val firstSyncResult = syncGroupLeader(groupId, firstGenerationId, firstMemberId, Map.empty, true)
    assertEquals(Errors.NONE.code, firstSyncResult._2)

    EasyMock.reset(offsetManager)
    val otherJoinFuture = sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, DefaultSessionTimeout,
      protocolType, protocols, isCoordinatorForGroup = true)

    EasyMock.reset(offsetManager)
    val joinFuture = sendJoinGroup(groupId, firstMemberId, DefaultSessionTimeout, protocolType, protocols,
      isCoordinatorForGroup = true);

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

    EasyMock.reset(offsetManager)
    val leaderSyncResult = syncGroupLeader(groupId, nextGenerationId, leaderId,
      Map(leaderId -> leaderAssignment, followerId -> followerAssignment), isCoordinatorForGroup = true)
    assertEquals(Errors.NONE.code, leaderSyncResult._2)
    assertEquals(leaderAssignment, leaderSyncResult._1)

    EasyMock.reset(offsetManager)
    val followerSyncResult = syncGroupFollower(groupId, nextGenerationId, otherJoinResult.memberId,
      isCoordinatorForGroup = true)
    assertEquals(Errors.NONE.code, followerSyncResult._2)
    assertEquals(followerAssignment, followerSyncResult._1)
  }

  @Test
  def testSyncGroupLeaderAfterFollower() {
    val groupId = "groupId"
    val protocolType = "consumer"
    val protocols = List(("range", Array[Byte]()))

    // to get a group of two members:
    // 1. join and sync with a single member (because we can't immediately join with two members)
    // 2. join and sync with the first member and a new member

    val joinGroupResult = joinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, DefaultSessionTimeout,
      protocolType, protocols, isCoordinatorForGroup = true)
    val firstMemberId = joinGroupResult.memberId
    val firstGenerationId = joinGroupResult.generationId
    assertEquals(firstMemberId, joinGroupResult.leaderId)
    assertEquals(Errors.NONE.code, joinGroupResult.errorCode)

    EasyMock.reset(offsetManager)
    val syncGroupResult = syncGroupLeader(groupId, firstGenerationId, firstMemberId, Map.empty, true)
    val syncGroupErrorCode = syncGroupResult._2
    assertEquals(Errors.NONE.code, syncGroupErrorCode)

    EasyMock.reset(offsetManager)
    val otherJoinFuture = sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, DefaultSessionTimeout,
      protocolType, protocols, isCoordinatorForGroup = true)

    EasyMock.reset(offsetManager)
    val joinFuture = sendJoinGroup(groupId, firstMemberId, DefaultSessionTimeout, protocolType, protocols,
      isCoordinatorForGroup = true);

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

    EasyMock.reset(offsetManager)
    val followerSyncFuture = sendSyncGroupFollower(groupId, nextGenerationId, followerId, isCoordinatorForGroup = true)

    EasyMock.reset(offsetManager)
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
    val groupId = "groupId"
    val consumerId = "consumer"
    val generationId = 1
    val tp = new TopicAndPartition("topic", 0)
    val offset = OffsetAndMetadata(0)

    val commitOffsetResult = commitOffsets(groupId, consumerId, generationId, Map(tp -> offset), true)
    assertEquals(Errors.ILLEGAL_GENERATION.code, commitOffsetResult(tp))
  }

  @Test
  def testCommitOffsetWithDefaultGeneration() {
    val groupId = "groupId"
    val tp = new TopicAndPartition("topic", 0)
    val offset = OffsetAndMetadata(0)

    val commitOffsetResult = commitOffsets(groupId, OffsetCommitRequest.DEFAULT_MEMBER_ID,
      OffsetCommitRequest.DEFAULT_GENERATION_ID, Map(tp -> offset), true)
    assertEquals(Errors.NONE.code, commitOffsetResult(tp))
  }

  @Test
  def testCommitOffsetInAwaitingSync() {
    val groupId = "groupId"
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val metadata = Array[Byte]()
    val protocolType = "consumer"
    val protocols = List(("range", metadata))
    val tp = new TopicAndPartition("topic", 0)
    val offset = OffsetAndMetadata(0)

    val joinGroupResult = joinGroup(groupId, memberId, DefaultSessionTimeout, protocolType, protocols,
      isCoordinatorForGroup = true)
    val assignedMemberId = joinGroupResult.memberId
    val generationId = joinGroupResult.generationId
    val joinGroupErrorCode = joinGroupResult.errorCode
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    EasyMock.reset(offsetManager)
    val commitOffsetResult = commitOffsets(groupId, assignedMemberId, generationId, Map(tp -> offset), true)
    assertEquals(Errors.REBALANCE_IN_PROGRESS.code, commitOffsetResult(tp))
  }

  @Test
  def testHeartbeatDuringRebalanceCausesRebalanceInProgress() {
    val groupId = "groupId"
    val metadata = Array[Byte]()
    val protocolType = "consumer"
    val protocols = List(("range", metadata))

    // First start up a group (with a slightly larger timeout to give us time to heartbeat when the rebalance starts)
    val joinGroupResult = joinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, DefaultSessionTimeout,
      protocolType, protocols, isCoordinatorForGroup = true)
    val assignedConsumerId = joinGroupResult.memberId
    val initialGenerationId = joinGroupResult.generationId
    val joinGroupErrorCode = joinGroupResult.errorCode
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    // Then join with a new consumer to trigger a rebalance
    EasyMock.reset(offsetManager)
    sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, DefaultSessionTimeout, protocolType, protocols,
      isCoordinatorForGroup = true)

    // We should be in the middle of a rebalance, so the heartbeat should return rebalance in progress
    EasyMock.reset(offsetManager)
    val heartbeatResult = heartbeat(groupId, assignedConsumerId, initialGenerationId, isCoordinatorForGroup = true)
    assertEquals(Errors.REBALANCE_IN_PROGRESS.code, heartbeatResult)
  }

  @Test
  def testGenerationIdIncrementsOnRebalance() {
    val groupId = "groupId"
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val otherMemberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val metadata = Array[Byte]()
    val protocolType = "consumer"
    val protocols = List(("range", metadata))

    val joinGroupResult = joinGroup(groupId, memberId, DefaultSessionTimeout, protocolType, protocols,
      isCoordinatorForGroup = true)
    val initialGenerationId = joinGroupResult.generationId
    val joinGroupErrorCode = joinGroupResult.errorCode
    assertEquals(1, initialGenerationId)
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    EasyMock.reset(offsetManager)
    val otherJoinGroupResult = joinGroup(groupId, otherMemberId, DefaultSessionTimeout, protocolType, protocols,
      isCoordinatorForGroup = true)
    val nextGenerationId = otherJoinGroupResult.generationId
    val otherJoinGroupErrorCode = otherJoinGroupResult.errorCode
    assertEquals(2, nextGenerationId)
    assertEquals(Errors.NONE.code, otherJoinGroupErrorCode)
  }

  @Test
  def testLeaveGroupWrongCoordinator() {
    val groupId = "groupId"
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val leaveGroupResult = leaveGroup(groupId, memberId, isCoordinatorForGroup = false)
    assertEquals(Errors.NOT_COORDINATOR_FOR_GROUP.code, leaveGroupResult)
  }

  @Test
  def testLeaveGroupUnknownGroup() {
    val groupId = "groupId"
    val memberId = "consumerId"

    val leaveGroupResult = leaveGroup(groupId, memberId, isCoordinatorForGroup = true)
    assertEquals(Errors.UNKNOWN_MEMBER_ID.code, leaveGroupResult)
  }

  @Test
  def testLeaveGroupUnknownConsumerExistingGroup() {
    val groupId = "groupId"
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val otherMemberId = "consumerId"
    val metadata = Array[Byte]()
    val protocolType = "consumer"
    val protocols = List(("range", metadata))

    val joinGroupResult = joinGroup(groupId, memberId, DefaultSessionTimeout, protocolType, protocols,
      isCoordinatorForGroup = true)
    val joinGroupErrorCode = joinGroupResult.errorCode
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    EasyMock.reset(offsetManager)
    val leaveGroupResult = leaveGroup(groupId, otherMemberId, isCoordinatorForGroup = true)
    assertEquals(Errors.UNKNOWN_MEMBER_ID.code, leaveGroupResult)
  }

  @Test
  def testValidLeaveGroup() {
    val groupId = "groupId"
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val metadata = Array[Byte]()
    val protocolType = "consumer"
    val protocols = List(("range", metadata))

    val joinGroupResult = joinGroup(groupId, memberId, DefaultSessionTimeout, protocolType, protocols,
      isCoordinatorForGroup = true)
    val assignedMemberId = joinGroupResult.memberId
    val joinGroupErrorCode = joinGroupResult.errorCode
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    EasyMock.reset(offsetManager)
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
    EasyMock.expect(offsetManager.partitionFor(groupId)).andReturn(1)
    EasyMock.expect(offsetManager.leaderIsLocal(1)).andReturn(isCoordinatorForGroup)
    EasyMock.replay(offsetManager)
    consumerCoordinator.handleJoinGroup(groupId, memberId, sessionTimeout, protocolType, protocols, responseCallback)
    responseFuture
  }


  private def sendSyncGroupLeader(groupId: String,
                                  generation: Int,
                                  leaderId: String,
                                  assignment: Map[String, Array[Byte]],
                                  isCoordinatorForGroup: Boolean): Future[SyncGroupCallbackParams] = {
    val (responseFuture, responseCallback) = setupSyncGroupCallback
    EasyMock.expect(offsetManager.partitionFor(groupId)).andReturn(1)
    EasyMock.expect(offsetManager.leaderIsLocal(1)).andReturn(isCoordinatorForGroup)
    EasyMock.replay(offsetManager)
    consumerCoordinator.handleSyncGroup(groupId, generation, leaderId, assignment, responseCallback)
    responseFuture
  }

  private def sendSyncGroupFollower(groupId: String,
                                    generation: Int,
                                    memberId: String,
                                    isCoordinatorForGroup: Boolean): Future[SyncGroupCallbackParams] = {
    val (responseFuture, responseCallback) = setupSyncGroupCallback
    EasyMock.expect(offsetManager.partitionFor(groupId)).andReturn(1)
    EasyMock.expect(offsetManager.leaderIsLocal(1)).andReturn(isCoordinatorForGroup)
    EasyMock.replay(offsetManager)
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
    EasyMock.expect(offsetManager.partitionFor(groupId)).andReturn(1)
    EasyMock.expect(offsetManager.leaderIsLocal(1)).andReturn(isCoordinatorForGroup)
    EasyMock.replay(offsetManager)
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
    EasyMock.expect(offsetManager.partitionFor(groupId)).andReturn(1)
    EasyMock.expect(offsetManager.leaderIsLocal(1)).andReturn(isCoordinatorForGroup)
    val storeOffsetAnswer = new IAnswer[Unit] {
      override def answer = responseCallback.apply(offsets.mapValues(_ => Errors.NONE.code))
    }
    EasyMock.expect(offsetManager.storeOffsets(groupId, consumerId, generationId, offsets, responseCallback))
      .andAnswer(storeOffsetAnswer)
    EasyMock.replay(offsetManager)
    consumerCoordinator.handleCommitOffsets(groupId, consumerId, generationId, offsets, responseCallback)
    Await.result(responseFuture, Duration(40, TimeUnit.MILLISECONDS))
  }

  private def leaveGroup(groupId: String, consumerId: String, isCoordinatorForGroup: Boolean): LeaveGroupCallbackParams = {
    val (responseFuture, responseCallback) = setupHeartbeatCallback
    EasyMock.expect(offsetManager.partitionFor(groupId)).andReturn(1)
    EasyMock.expect(offsetManager.leaderIsLocal(1)).andReturn(isCoordinatorForGroup)
    EasyMock.replay(offsetManager)
    consumerCoordinator.handleLeaveGroup(groupId, consumerId, responseCallback)
    Await.result(responseFuture, Duration(40, TimeUnit.MILLISECONDS))
  }
}
