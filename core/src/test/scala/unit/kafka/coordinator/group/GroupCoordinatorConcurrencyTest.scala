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

import java.util.concurrent.{ ConcurrentHashMap, TimeUnit }

import kafka.common.OffsetAndMetadata
import kafka.coordinator.AbstractCoordinatorConcurrencyTest
import kafka.coordinator.AbstractCoordinatorConcurrencyTest._
import kafka.coordinator.group.GroupCoordinatorConcurrencyTest._
import kafka.server.{ DelayedOperationPurgatory, KafkaConfig }
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.JoinGroupRequest
import org.easymock.EasyMock
import org.junit.Assert._
import org.junit.{ After, Before, Test }

import scala.collection._
import scala.concurrent.duration.Duration
import scala.concurrent.{ Await, Future, Promise, TimeoutException }

class GroupCoordinatorConcurrencyTest extends AbstractCoordinatorConcurrencyTest[GroupMember] {

  private val protocolType = "consumer"
  private val metadata = Array[Byte]()
  private val protocols = List(("range", metadata))

  private val nGroups = nThreads * 10
  private val nMembersPerGroup = nThreads * 5
  private val numPartitions = 2

  private val allOperations = Seq(
      new JoinGroupOperation,
      new SyncGroupOperation,
      new CommitOffsetsOperation,
      new HeartbeatOperation,
      new LeaveGroupOperation
    )
  private val allOperationsWithTxn = Seq(
    new JoinGroupOperation,
    new SyncGroupOperation,
    new CommitTxnOffsetsOperation,
    new CompleteTxnOperation,
    new HeartbeatOperation,
    new LeaveGroupOperation
  )

  var groupCoordinator: GroupCoordinator = _

  @Before
  override def setUp() {
    super.setUp()

    EasyMock.expect(zkClient.getTopicPartitionCount(Topic.GROUP_METADATA_TOPIC_NAME))
      .andReturn(Some(numPartitions))
      .anyTimes()
    EasyMock.replay(zkClient)

    serverProps.setProperty(KafkaConfig.GroupMinSessionTimeoutMsProp, ConsumerMinSessionTimeout.toString)
    serverProps.setProperty(KafkaConfig.GroupMaxSessionTimeoutMsProp, ConsumerMaxSessionTimeout.toString)
    serverProps.setProperty(KafkaConfig.GroupInitialRebalanceDelayMsProp, GroupInitialRebalanceDelay.toString)

    val config = KafkaConfig.fromProps(serverProps)

    val heartbeatPurgatory = new DelayedOperationPurgatory[DelayedHeartbeat]("Heartbeat", timer, config.brokerId, reaperEnabled = false)
    val joinPurgatory = new DelayedOperationPurgatory[DelayedJoin]("Rebalance", timer, config.brokerId, reaperEnabled = false)

    groupCoordinator = GroupCoordinator(config, zkClient, replicaManager, heartbeatPurgatory, joinPurgatory, timer.time)
    groupCoordinator.startup(false)
  }

  @After
  override def tearDown() {
    try {
      if (groupCoordinator != null)
        groupCoordinator.shutdown()
    } finally {
      super.tearDown()
    }
  }

  def createGroupMembers(groupPrefix: String): Set[GroupMember] = {
    (0 until nGroups).flatMap { i =>
      new Group(s"$groupPrefix$i", nMembersPerGroup, groupCoordinator, replicaManager).members
    }.toSet
  }

  @Test
  def testConcurrentGoodPathSequence() {
    verifyConcurrentOperations(createGroupMembers, allOperations)
  }

  @Test
  def testConcurrentTxnGoodPathSequence() {
    verifyConcurrentOperations(createGroupMembers, allOperationsWithTxn)
  }

  @Test
  def testConcurrentRandomSequence() {
    verifyConcurrentRandomSequences(createGroupMembers, allOperationsWithTxn)
  }

  abstract class GroupOperation[R, C] extends Operation {
    val responseFutures = new ConcurrentHashMap[GroupMember, Future[R]]()

    def setUpCallback(member: GroupMember): C = {
      val responsePromise = Promise[R]
      val responseFuture = responsePromise.future
      responseFutures.put(member, responseFuture)
      responseCallback(responsePromise)
    }
    def responseCallback(responsePromise: Promise[R]): C

    override def run(member: GroupMember): Unit = {
      val responseCallback = setUpCallback(member)
      runWithCallback(member, responseCallback)
    }

    def runWithCallback(member: GroupMember, responseCallback: C): Unit

    def await(member: GroupMember, timeoutMs: Long): R = {
      var retries = (timeoutMs + 10) / 10
      val responseFuture = responseFutures.get(member)
      while (retries > 0) {
        timer.advanceClock(10)
        try {
          return Await.result(responseFuture, Duration(10, TimeUnit.MILLISECONDS))
        } catch {
          case _: TimeoutException =>
        }
        retries -= 1
      }
      throw new TimeoutException(s"Operation did not complete within $timeoutMs millis")
    }
  }


  class JoinGroupOperation extends GroupOperation[JoinGroupResult, JoinGroupCallback] {
    override def responseCallback(responsePromise: Promise[JoinGroupResult]): JoinGroupCallback = {
      val callback: JoinGroupCallback = responsePromise.success(_)
      callback
    }
    override def runWithCallback(member: GroupMember, responseCallback: JoinGroupCallback): Unit = {
      groupCoordinator.handleJoinGroup(member.groupId, member.memberId, "clientId", "clientHost",
       DefaultRebalanceTimeout, DefaultSessionTimeout,
       protocolType, protocols, responseCallback)
    }
    override def awaitAndVerify(member: GroupMember): Unit = {
       val joinGroupResult = await(member, DefaultRebalanceTimeout)
       assertEquals(Errors.NONE, joinGroupResult.error)
       member.memberId = joinGroupResult.memberId
       member.generationId = joinGroupResult.generationId
    }
  }

  class SyncGroupOperation extends GroupOperation[SyncGroupCallbackParams, SyncGroupCallback] {
    override def responseCallback(responsePromise: Promise[SyncGroupCallbackParams]): SyncGroupCallback = {
      val callback: SyncGroupCallback = (assignment, error) =>
        responsePromise.success((assignment, error))
      callback
    }
    override def runWithCallback(member: GroupMember, responseCallback: SyncGroupCallback): Unit = {
      if (member.leader) {
        groupCoordinator.handleSyncGroup(member.groupId, member.generationId, member.memberId,
            member.group.assignment, responseCallback)
      } else {
         groupCoordinator.handleSyncGroup(member.groupId, member.generationId, member.memberId,
             Map.empty[String, Array[Byte]], responseCallback)
      }
    }
    override def awaitAndVerify(member: GroupMember): Unit = {
       val result = await(member, DefaultSessionTimeout)
       assertEquals(Errors.NONE, result._2)
    }
  }

  class HeartbeatOperation extends GroupOperation[HeartbeatCallbackParams, HeartbeatCallback] {
    override def responseCallback(responsePromise: Promise[HeartbeatCallbackParams]): HeartbeatCallback = {
      val callback: HeartbeatCallback = error => responsePromise.success(error)
      callback
    }
    override def runWithCallback(member: GroupMember, responseCallback: HeartbeatCallback): Unit = {
      groupCoordinator.handleHeartbeat( member.groupId, member.memberId,  member.generationId, responseCallback)
    }
    override def awaitAndVerify(member: GroupMember): Unit = {
       val error = await(member, DefaultSessionTimeout)
       assertEquals(Errors.NONE, error)
    }
  }
  class CommitOffsetsOperation extends GroupOperation[CommitOffsetCallbackParams, CommitOffsetCallback] {
    override def responseCallback(responsePromise: Promise[CommitOffsetCallbackParams]): CommitOffsetCallback = {
      val callback: CommitOffsetCallback = offsets => responsePromise.success(offsets)
      callback
    }
    override def runWithCallback(member: GroupMember, responseCallback: CommitOffsetCallback): Unit = {
      val tp = new TopicPartition("topic", 0)
      val offsets = immutable.Map(tp -> OffsetAndMetadata(1))
      groupCoordinator.handleCommitOffsets(member.groupId, member.memberId, member.generationId,
          offsets, responseCallback)
    }
    override def awaitAndVerify(member: GroupMember): Unit = {
       val offsets = await(member, 500)
       offsets.foreach { case (_, error) => assertEquals(Errors.NONE, error) }
    }
  }

  class CommitTxnOffsetsOperation extends CommitOffsetsOperation {
    override def runWithCallback(member: GroupMember, responseCallback: CommitOffsetCallback): Unit = {
      val tp = new TopicPartition("topic", 0)
      val offsets = immutable.Map(tp -> OffsetAndMetadata(1))
      val producerId = 1000L
      val producerEpoch : Short = 2
      // When transaction offsets are appended to the log, transactions may be scheduled for
      // completion. Since group metadata locks are acquired for transaction completion, include
      // this in the callback to test that there are no deadlocks.
      def callbackWithTxnCompletion(errors: Map[TopicPartition, Errors]): Unit = {
        val offsetsPartitions = (0 to numPartitions).map(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, _))
        groupCoordinator.groupManager.scheduleHandleTxnCompletion(producerId,
          offsetsPartitions.map(_.partition).toSet, isCommit = random.nextBoolean)
        responseCallback(errors)
      }
      groupCoordinator.handleTxnCommitOffsets(member.group.groupId,
          producerId, producerEpoch, offsets, callbackWithTxnCompletion)
    }
  }

  class CompleteTxnOperation extends GroupOperation[CompleteTxnCallbackParams, CompleteTxnCallback] {
    override def responseCallback(responsePromise: Promise[CompleteTxnCallbackParams]): CompleteTxnCallback = {
      val callback: CompleteTxnCallback = error => responsePromise.success(error)
      callback
    }
    override def runWithCallback(member: GroupMember, responseCallback: CompleteTxnCallback): Unit = {
      val producerId = 1000L
      val offsetsPartitions = (0 to numPartitions).map(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, _))
      groupCoordinator.groupManager.handleTxnCompletion(producerId,
        offsetsPartitions.map(_.partition).toSet, isCommit = random.nextBoolean)
      responseCallback(Errors.NONE)
    }
    override def awaitAndVerify(member: GroupMember): Unit = {
      val error = await(member, 500)
      assertEquals(Errors.NONE, error)
    }
  }

  class LeaveGroupOperation extends GroupOperation[LeaveGroupCallbackParams, LeaveGroupCallback] {
    override def responseCallback(responsePromise: Promise[LeaveGroupCallbackParams]): LeaveGroupCallback = {
      val callback: LeaveGroupCallback = error => responsePromise.success(error)
      callback
    }
    override def runWithCallback(member: GroupMember, responseCallback: LeaveGroupCallback): Unit = {
      groupCoordinator.handleLeaveGroup(member.group.groupId, member.memberId, responseCallback)
    }
    override def awaitAndVerify(member: GroupMember): Unit = {
       val error = await(member, DefaultSessionTimeout)
       assertEquals(Errors.NONE, error)
    }
  }
}

object GroupCoordinatorConcurrencyTest {

  type JoinGroupCallback = JoinGroupResult => Unit
  type SyncGroupCallbackParams = (Array[Byte], Errors)
  type SyncGroupCallback = (Array[Byte], Errors) => Unit
  type HeartbeatCallbackParams = Errors
  type HeartbeatCallback = Errors => Unit
  type CommitOffsetCallbackParams = Map[TopicPartition, Errors]
  type CommitOffsetCallback = Map[TopicPartition, Errors] => Unit
  type LeaveGroupCallbackParams = Errors
  type LeaveGroupCallback = Errors => Unit
  type CompleteTxnCallbackParams = Errors
  type CompleteTxnCallback = Errors => Unit

  private val ConsumerMinSessionTimeout = 10
  private val ConsumerMaxSessionTimeout = 120 * 1000
  private val DefaultRebalanceTimeout = 60 * 1000
  private val DefaultSessionTimeout = 60 * 1000
  private val GroupInitialRebalanceDelay = 50

  class Group(val groupId: String, nMembers: Int,
      groupCoordinator: GroupCoordinator, replicaManager: TestReplicaManager) {
    val groupPartitionId = groupCoordinator.partitionFor(groupId)
    groupCoordinator.groupManager.addPartitionOwnership(groupPartitionId)
    val members = (0 until nMembers).map { i =>
      new GroupMember(this, groupPartitionId, i == 0)
    }
    def assignment = members.map { m => (m.memberId, Array[Byte]()) }.toMap
  }

  class GroupMember(val group: Group, val groupPartitionId: Int, val leader: Boolean) extends CoordinatorMember {
    @volatile var memberId: String = JoinGroupRequest.UNKNOWN_MEMBER_ID
    @volatile var generationId: Int = -1
    def groupId: String = group.groupId
  }
}
