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
 * Test ConsumerCoordinator responses
 */
class ConsumerCoordinatorResponseTest extends JUnitSuite {
  type JoinGroupCallbackParams = (Set[TopicAndPartition], String, Int, Short)
  type JoinGroupCallback = (Set[TopicAndPartition], String, Int, Short) => Unit
  type HeartbeatCallbackParams = Short
  type HeartbeatCallback = Short => Unit
  type CommitOffsetCallbackParams = Map[TopicAndPartition, Short]
  type CommitOffsetCallback = Map[TopicAndPartition, Short] => Unit
  type LeaveGroupCallbackParams = Short
  type LeaveGroupCallback = Short => Unit

  val ConsumerMinSessionTimeout = 10
  val ConsumerMaxSessionTimeout = 200
  val DefaultSessionTimeout = 100
  var consumerCoordinator: ConsumerCoordinator = null
  var offsetManager : OffsetManager = null

  @Before
  def setUp() {
    val props = TestUtils.createBrokerConfig(nodeId = 0, zkConnect = "")
    props.setProperty(KafkaConfig.ConsumerMinSessionTimeoutMsProp, ConsumerMinSessionTimeout.toString)
    props.setProperty(KafkaConfig.ConsumerMaxSessionTimeoutMsProp, ConsumerMaxSessionTimeout.toString)
    offsetManager = EasyMock.createStrictMock(classOf[OffsetManager])
    consumerCoordinator = ConsumerCoordinator.create(KafkaConfig.fromProps(props), null, offsetManager)
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
    val consumerId = JoinGroupRequest.UNKNOWN_CONSUMER_ID
    val partitionAssignmentStrategy = "range"

    val joinGroupResult = joinGroup(groupId, consumerId, partitionAssignmentStrategy, DefaultSessionTimeout, isCoordinatorForGroup = false)
    val joinGroupErrorCode = joinGroupResult._4
    assertEquals(Errors.NOT_COORDINATOR_FOR_CONSUMER.code, joinGroupErrorCode)
  }

  @Test
  def testJoinGroupUnknownPartitionAssignmentStrategy() {
    val groupId = "groupId"
    val consumerId = JoinGroupRequest.UNKNOWN_CONSUMER_ID
    val partitionAssignmentStrategy = "foo"

    val joinGroupResult = joinGroup(groupId, consumerId, partitionAssignmentStrategy, DefaultSessionTimeout, isCoordinatorForGroup = true)
    val joinGroupErrorCode = joinGroupResult._4
    assertEquals(Errors.UNKNOWN_PARTITION_ASSIGNMENT_STRATEGY.code, joinGroupErrorCode)
  }

  @Test
  def testJoinGroupSessionTimeoutTooSmall() {
    val groupId = "groupId"
    val consumerId = JoinGroupRequest.UNKNOWN_CONSUMER_ID
    val partitionAssignmentStrategy = "range"

    val joinGroupResult = joinGroup(groupId, consumerId, partitionAssignmentStrategy, ConsumerMinSessionTimeout - 1, isCoordinatorForGroup = true)
    val joinGroupErrorCode = joinGroupResult._4
    assertEquals(Errors.INVALID_SESSION_TIMEOUT.code, joinGroupErrorCode)
  }

  @Test
  def testJoinGroupSessionTimeoutTooLarge() {
    val groupId = "groupId"
    val consumerId = JoinGroupRequest.UNKNOWN_CONSUMER_ID
    val partitionAssignmentStrategy = "range"

    val joinGroupResult = joinGroup(groupId, consumerId, partitionAssignmentStrategy, ConsumerMaxSessionTimeout + 1, isCoordinatorForGroup = true)
    val joinGroupErrorCode = joinGroupResult._4
    assertEquals(Errors.INVALID_SESSION_TIMEOUT.code, joinGroupErrorCode)
  }

  @Test
  def testJoinGroupUnknownConsumerNewGroup() {
    val groupId = "groupId"
    val consumerId = "consumerId"
    val partitionAssignmentStrategy = "range"

    val joinGroupResult = joinGroup(groupId, consumerId, partitionAssignmentStrategy, DefaultSessionTimeout, isCoordinatorForGroup = true)
    val joinGroupErrorCode = joinGroupResult._4
    assertEquals(Errors.UNKNOWN_CONSUMER_ID.code, joinGroupErrorCode)
  }

  @Test
  def testValidJoinGroup() {
    val groupId = "groupId"
    val consumerId = JoinGroupRequest.UNKNOWN_CONSUMER_ID
    val partitionAssignmentStrategy = "range"

    val joinGroupResult = joinGroup(groupId, consumerId, partitionAssignmentStrategy, DefaultSessionTimeout, isCoordinatorForGroup = true)
    val joinGroupErrorCode = joinGroupResult._4
    assertEquals(Errors.NONE.code, joinGroupErrorCode)
  }

  @Test
  def testJoinGroupInconsistentPartitionAssignmentStrategy() {
    val groupId = "groupId"
    val consumerId = JoinGroupRequest.UNKNOWN_CONSUMER_ID
    val otherConsumerId = JoinGroupRequest.UNKNOWN_CONSUMER_ID
    val partitionAssignmentStrategy = "range"
    val otherPartitionAssignmentStrategy = "roundrobin"

    val joinGroupResult = joinGroup(groupId, consumerId, partitionAssignmentStrategy, DefaultSessionTimeout, isCoordinatorForGroup = true)
    val joinGroupErrorCode = joinGroupResult._4
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    EasyMock.reset(offsetManager)
    val otherJoinGroupResult = joinGroup(groupId, otherConsumerId, otherPartitionAssignmentStrategy, DefaultSessionTimeout, isCoordinatorForGroup = true)
    val otherJoinGroupErrorCode = otherJoinGroupResult._4
    assertEquals(Errors.INCONSISTENT_PARTITION_ASSIGNMENT_STRATEGY.code, otherJoinGroupErrorCode)
  }

  @Test
  def testJoinGroupUnknownConsumerExistingGroup() {
    val groupId = "groupId"
    val consumerId = JoinGroupRequest.UNKNOWN_CONSUMER_ID
    val otherConsumerId = "consumerId"
    val partitionAssignmentStrategy = "range"

    val joinGroupResult = joinGroup(groupId, consumerId, partitionAssignmentStrategy, DefaultSessionTimeout, isCoordinatorForGroup = true)
    val joinGroupErrorCode = joinGroupResult._4
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    EasyMock.reset(offsetManager)
    val otherJoinGroupResult = joinGroup(groupId, otherConsumerId, partitionAssignmentStrategy, DefaultSessionTimeout, isCoordinatorForGroup = true)
    val otherJoinGroupErrorCode = otherJoinGroupResult._4
    assertEquals(Errors.UNKNOWN_CONSUMER_ID.code, otherJoinGroupErrorCode)
  }

  @Test
  def testHeartbeatWrongCoordinator() {
    val groupId = "groupId"
    val consumerId = "consumerId"

    val heartbeatResult = heartbeat(groupId, consumerId, -1, isCoordinatorForGroup = false)
    assertEquals(Errors.NOT_COORDINATOR_FOR_CONSUMER.code, heartbeatResult)
  }

  @Test
  def testHeartbeatUnknownGroup() {
    val groupId = "groupId"
    val consumerId = "consumerId"

    val heartbeatResult = heartbeat(groupId, consumerId, -1, isCoordinatorForGroup = true)
    assertEquals(Errors.UNKNOWN_CONSUMER_ID.code, heartbeatResult)
  }

  @Test
  def testHeartbeatUnknownConsumerExistingGroup() {
    val groupId = "groupId"
    val consumerId = JoinGroupRequest.UNKNOWN_CONSUMER_ID
    val otherConsumerId = "consumerId"
    val partitionAssignmentStrategy = "range"

    val joinGroupResult = joinGroup(groupId, consumerId, partitionAssignmentStrategy, DefaultSessionTimeout, isCoordinatorForGroup = true)
    val joinGroupErrorCode = joinGroupResult._4
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    EasyMock.reset(offsetManager)
    val heartbeatResult = heartbeat(groupId, otherConsumerId, 1, isCoordinatorForGroup = true)
    assertEquals(Errors.UNKNOWN_CONSUMER_ID.code, heartbeatResult)
  }

  @Test
  def testHeartbeatIllegalGeneration() {
    val groupId = "groupId"
    val consumerId = JoinGroupRequest.UNKNOWN_CONSUMER_ID
    val partitionAssignmentStrategy = "range"

    val joinGroupResult = joinGroup(groupId, consumerId, partitionAssignmentStrategy, DefaultSessionTimeout, isCoordinatorForGroup = true)
    val assignedConsumerId = joinGroupResult._2
    val joinGroupErrorCode = joinGroupResult._4
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    EasyMock.reset(offsetManager)
    val heartbeatResult = heartbeat(groupId, assignedConsumerId, 2, isCoordinatorForGroup = true)
    assertEquals(Errors.ILLEGAL_GENERATION.code, heartbeatResult)
  }

  @Test
  def testValidHeartbeat() {
    val groupId = "groupId"
    val consumerId = JoinGroupRequest.UNKNOWN_CONSUMER_ID
    val partitionAssignmentStrategy = "range"

    val joinGroupResult = joinGroup(groupId, consumerId, partitionAssignmentStrategy, DefaultSessionTimeout, isCoordinatorForGroup = true)
    val assignedConsumerId = joinGroupResult._2
    val joinGroupErrorCode = joinGroupResult._4
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    EasyMock.reset(offsetManager)
    val heartbeatResult = heartbeat(groupId, assignedConsumerId, 1, isCoordinatorForGroup = true)
    assertEquals(Errors.NONE.code, heartbeatResult)
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

    val commitOffsetResult = commitOffsets(groupId, OffsetCommitRequest.DEFAULT_CONSUMER_ID,
      OffsetCommitRequest.DEFAULT_GENERATION_ID, Map(tp -> offset), true)
    assertEquals(Errors.NONE.code, commitOffsetResult(tp))
  }

  @Test
  def testHeartbeatDuringRebalanceCausesRebalanceInProgress() {
    val groupId = "groupId"
    val partitionAssignmentStrategy = "range"

    // First start up a group (with a slightly larger timeout to give us time to heartbeat when the rebalance starts)
    val joinGroupResult = joinGroup(groupId, JoinGroupRequest.UNKNOWN_CONSUMER_ID, partitionAssignmentStrategy,
      DefaultSessionTimeout, isCoordinatorForGroup = true)
    val assignedConsumerId = joinGroupResult._2
    val initialGenerationId = joinGroupResult._3
    val joinGroupErrorCode = joinGroupResult._4
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    // Then join with a new consumer to trigger a rebalance
    EasyMock.reset(offsetManager)
    sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_CONSUMER_ID, partitionAssignmentStrategy,
      DefaultSessionTimeout, isCoordinatorForGroup = true)

    // We should be in the middle of a rebalance, so the heartbeat should return rebalance in progress
    EasyMock.reset(offsetManager)
    val heartbeatResult = heartbeat(groupId, assignedConsumerId, initialGenerationId, isCoordinatorForGroup = true)
    assertEquals(Errors.REBALANCE_IN_PROGRESS.code, heartbeatResult)
  }

  @Test
  def testGenerationIdIncrementsOnRebalance() {
    val groupId = "groupId"
    val consumerId = JoinGroupRequest.UNKNOWN_CONSUMER_ID
    val otherConsumerId = JoinGroupRequest.UNKNOWN_CONSUMER_ID
    val partitionAssignmentStrategy = "range"

    val joinGroupResult = joinGroup(groupId, consumerId, partitionAssignmentStrategy, DefaultSessionTimeout, isCoordinatorForGroup = true)
    val initialGenerationId = joinGroupResult._3
    val joinGroupErrorCode = joinGroupResult._4
    assertEquals(1, initialGenerationId)
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    EasyMock.reset(offsetManager)
    val otherJoinGroupResult = joinGroup(groupId, otherConsumerId, partitionAssignmentStrategy, DefaultSessionTimeout, isCoordinatorForGroup = true)
    val nextGenerationId = otherJoinGroupResult._3
    val otherJoinGroupErrorCode = otherJoinGroupResult._4
    assertEquals(2, nextGenerationId)
    assertEquals(Errors.NONE.code, otherJoinGroupErrorCode)
  }

  @Test
  def testLeaveGroupWrongCoordinator() {
    val groupId = "groupId"
    val consumerId = JoinGroupRequest.UNKNOWN_CONSUMER_ID

    val leaveGroupResult = leaveGroup(groupId, consumerId, isCoordinatorForGroup = false)
    assertEquals(Errors.NOT_COORDINATOR_FOR_CONSUMER.code, leaveGroupResult)
  }

  @Test
  def testLeaveGroupUnknownGroup() {
    val groupId = "groupId"
    val consumerId = "consumerId"

    val leaveGroupResult = leaveGroup(groupId, consumerId, isCoordinatorForGroup = true)
    assertEquals(Errors.UNKNOWN_CONSUMER_ID.code, leaveGroupResult)
  }

  @Test
  def testLeaveGroupUnknownConsumerExistingGroup() {
    val groupId = "groupId"
    val consumerId = JoinGroupRequest.UNKNOWN_CONSUMER_ID
    val otherConsumerId = "consumerId"
    val partitionAssignmentStrategy = "range"

    val joinGroupResult = joinGroup(groupId, consumerId, partitionAssignmentStrategy, DefaultSessionTimeout, isCoordinatorForGroup = true)
    val joinGroupErrorCode = joinGroupResult._4
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    EasyMock.reset(offsetManager)
    val leaveGroupResult = leaveGroup(groupId, otherConsumerId, isCoordinatorForGroup = true)
    assertEquals(Errors.UNKNOWN_CONSUMER_ID.code, leaveGroupResult)
  }

  @Test
  def testValidLeaveGroup() {
    val groupId = "groupId"
    val consumerId = JoinGroupRequest.UNKNOWN_CONSUMER_ID
    val partitionAssignmentStrategy = "range"

    val joinGroupResult = joinGroup(groupId, consumerId, partitionAssignmentStrategy, DefaultSessionTimeout, isCoordinatorForGroup = true)
    val assignedConsumerId = joinGroupResult._2
    val joinGroupErrorCode = joinGroupResult._4
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    EasyMock.reset(offsetManager)
    val leaveGroupResult = leaveGroup(groupId, assignedConsumerId, isCoordinatorForGroup = true)
    assertEquals(Errors.NONE.code, leaveGroupResult)
  }

  private def setupJoinGroupCallback: (Future[JoinGroupCallbackParams], JoinGroupCallback) = {
    val responsePromise = Promise[JoinGroupCallbackParams]
    val responseFuture = responsePromise.future
    val responseCallback: JoinGroupCallback = (partitions, consumerId, generationId, errorCode) =>
      responsePromise.success((partitions, consumerId, generationId, errorCode))
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

  private def setupLeaveGroupCallback: (Future[LeaveGroupCallbackParams], LeaveGroupCallback) = {
    val responsePromise = Promise[LeaveGroupCallbackParams]
    val responseFuture = responsePromise.future
    val responseCallback: LeaveGroupCallback = errorCode => responsePromise.success(errorCode)
    (responseFuture, responseCallback)
  }

  private def sendJoinGroup(groupId: String,
                            consumerId: String,
                            partitionAssignmentStrategy: String,
                            sessionTimeout: Int,
                            isCoordinatorForGroup: Boolean): Future[JoinGroupCallbackParams] = {
    val (responseFuture, responseCallback) = setupJoinGroupCallback
    EasyMock.expect(offsetManager.partitionFor(groupId)).andReturn(1)
    EasyMock.expect(offsetManager.leaderIsLocal(1)).andReturn(isCoordinatorForGroup)
    EasyMock.replay(offsetManager)
    consumerCoordinator.handleJoinGroup(groupId, consumerId, Set.empty, sessionTimeout, partitionAssignmentStrategy, responseCallback)
    responseFuture
  }

  private def joinGroup(groupId: String,
                        consumerId: String,
                        partitionAssignmentStrategy: String,
                        sessionTimeout: Int,
                        isCoordinatorForGroup: Boolean): JoinGroupCallbackParams = {
    val responseFuture = sendJoinGroup(groupId, consumerId, partitionAssignmentStrategy, sessionTimeout, isCoordinatorForGroup)
    // should only have to wait as long as session timeout, but allow some extra time in case of an unexpected delay
    Await.result(responseFuture, Duration(sessionTimeout+100, TimeUnit.MILLISECONDS))
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
