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

import java.util.Properties
import java.util.concurrent.locks.{Lock, ReentrantLock}
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import kafka.common.OffsetAndMetadata
import kafka.coordinator.AbstractCoordinatorConcurrencyTest
import kafka.coordinator.AbstractCoordinatorConcurrencyTest._
import kafka.coordinator.group.GroupCoordinatorConcurrencyTest._
import kafka.server.{DelayedOperationPurgatory, KafkaConfig, KafkaRequestHandler, RequestLocal}
import kafka.utils.CoreUtils
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{JoinGroupRequest, OffsetFetchResponse}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.mockito.Mockito.when

import scala.collection._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise, TimeoutException}

class GroupCoordinatorConcurrencyTest extends AbstractCoordinatorConcurrencyTest[GroupMember] {

  private val protocolType = "consumer"
  private val protocolName = "range"
  private val metadata = Array[Byte]()
  private val protocols = List((protocolName, metadata))
  private val nGroups = nThreads * 10
  private val nMembersPerGroup = nThreads * 5
  private val numPartitions = 2
  private var metrics: Metrics = _

  private val allOperations = Seq(
      new JoinGroupOperation,
      new SyncGroupOperation,
      new OffsetFetchOperation,
      new CommitOffsetsOperation,
      new HeartbeatOperation,
      new LeaveGroupOperation
  )

  var heartbeatPurgatory: DelayedOperationPurgatory[DelayedHeartbeat] = _
  var rebalancePurgatory: DelayedOperationPurgatory[DelayedRebalance] = _
  var groupCoordinator: GroupCoordinator = _

  @BeforeEach
  override def setUp(): Unit = {
    super.setUp()

    when(zkClient.getTopicPartitionCount(Topic.GROUP_METADATA_TOPIC_NAME))
      .thenReturn(Some(numPartitions))

    serverProps.setProperty(GroupCoordinatorConfig.GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, ConsumerMinSessionTimeout.toString)
    serverProps.setProperty(GroupCoordinatorConfig.GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG, ConsumerMaxSessionTimeout.toString)
    serverProps.setProperty(GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, GroupInitialRebalanceDelay.toString)

    val config = KafkaConfig.fromProps(serverProps)

    heartbeatPurgatory = new DelayedOperationPurgatory[DelayedHeartbeat]("Heartbeat", timer, config.brokerId, reaperEnabled = false)
    rebalancePurgatory = new DelayedOperationPurgatory[DelayedRebalance]("Rebalance", timer, config.brokerId, reaperEnabled = false)

    metrics = new Metrics
    groupCoordinator = GroupCoordinator(config, replicaManager, heartbeatPurgatory, rebalancePurgatory, timer.time, metrics)
    groupCoordinator.startup(() => zkClient.getTopicPartitionCount(Topic.GROUP_METADATA_TOPIC_NAME).getOrElse(config.offsetsTopicPartitions),
      enableMetadataExpiration = false)

    // Transactional appends attempt to schedule to the request handler thread using
    // a non request handler thread. Set this to avoid error.
    KafkaRequestHandler.setBypassThreadCheck(true)
  }

  @AfterEach
  override def tearDown(): Unit = {
    try {
      CoreUtils.swallow(groupCoordinator.shutdown(), this)
      CoreUtils.swallow(metrics.close(), this)
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
  def testConcurrentGoodPathSequence(): Unit = {
    verifyConcurrentOperations(createGroupMembers, allOperations)
  }

  @Test
  def testConcurrentTxnGoodPathSequence(): Unit = {
    verifyConcurrentOperations(createGroupMembers, Seq(
      new JoinGroupOperation,
      new SyncGroupOperation,
      new OffsetFetchOperation,
      new CommitTxnOffsetsOperation,
      new CompleteTxnOperation,
      new HeartbeatOperation,
      new LeaveGroupOperation
    ))
  }

  @Test
  def testConcurrentRandomSequence(): Unit = {
    /**
     * handleTxnCommitOffsets does not complete delayed requests now so it causes error if handleTxnCompletion is executed
     * before completing delayed request. In random mode, we use this global lock to prevent such an error.
     */
    val lock = new ReentrantLock()
    verifyConcurrentRandomSequences(createGroupMembers, Seq(
      new JoinGroupOperation,
      new SyncGroupOperation,
      new OffsetFetchOperation,
      new CommitTxnOffsetsOperation(lock = Some(lock)),
      new CompleteTxnOperation(lock = Some(lock)),
      new HeartbeatOperation,
      new LeaveGroupOperation
    ))
  }

  @Test
  def testConcurrentJoinGroupEnforceGroupMaxSize(): Unit = {
    val groupMaxSize = 1
    val newProperties = new Properties
    newProperties.put(GroupCoordinatorConfig.GROUP_MAX_SIZE_CONFIG, groupMaxSize.toString)
    val config = KafkaConfig.fromProps(serverProps, newProperties)

    if (groupCoordinator != null)
      groupCoordinator.shutdown()
    groupCoordinator = GroupCoordinator(config, replicaManager, heartbeatPurgatory,
      rebalancePurgatory, timer.time, new Metrics())
    groupCoordinator.startup(() => zkClient.getTopicPartitionCount(Topic.GROUP_METADATA_TOPIC_NAME).getOrElse(config.offsetsTopicPartitions),
      enableMetadataExpiration = false)

    val members = new Group(s"group", nMembersPerGroup, groupCoordinator, replicaManager)
      .members
    val joinOp = new JoinGroupOperation()

    verifyConcurrentActions(members.toSet.map(joinOp.actionNoVerify))

    val errors = members.map { member =>
      val joinGroupResult = joinOp.await(member, DefaultRebalanceTimeout)
      joinGroupResult.error
    }

    assertEquals(groupMaxSize, errors.count(_ == Errors.NONE))
    assertEquals(members.size-groupMaxSize, errors.count(_ == Errors.GROUP_MAX_SIZE_REACHED))
  }

  abstract class GroupOperation[R, C] extends Operation {
    val responseFutures = new ConcurrentHashMap[GroupMember, Future[R]]()

    def setUpCallback(member: GroupMember): C = {
      val responsePromise = Promise[R]()
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

  class JoinGroupOperation extends GroupOperation[JoinGroupCallbackParams, JoinGroupCallback] {
    override def responseCallback(responsePromise: Promise[JoinGroupCallbackParams]): JoinGroupCallback = {
      val callback: JoinGroupCallback = responsePromise.success(_)
      callback
    }
    override def runWithCallback(member: GroupMember, responseCallback: JoinGroupCallback): Unit = {
      groupCoordinator.handleJoinGroup(member.groupId, member.memberId, None, requireKnownMemberId = false,
        supportSkippingAssignment = false, "clientId", "clientHost", DefaultRebalanceTimeout,
        DefaultSessionTimeout, protocolType, protocols, responseCallback)
      replicaManager.tryCompleteActions()
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
      val callback: SyncGroupCallback = syncGroupResult =>
        responsePromise.success(syncGroupResult.error, syncGroupResult.memberAssignment)
      callback
    }
    override def runWithCallback(member: GroupMember, responseCallback: SyncGroupCallback): Unit = {
      if (member.leader) {
        groupCoordinator.handleSyncGroup(member.groupId, member.generationId, member.memberId,
          Some(protocolType), Some(protocolName), member.groupInstanceId, member.group.assignment, responseCallback)
      } else {
        groupCoordinator.handleSyncGroup(member.groupId, member.generationId, member.memberId,
          Some(protocolType), Some(protocolName), member.groupInstanceId, Map.empty[String, Array[Byte]], responseCallback)
      }
      replicaManager.tryCompleteActions()
    }
    override def awaitAndVerify(member: GroupMember): Unit = {
      val result = await(member, DefaultSessionTimeout)
      assertEquals(Errors.NONE, result._1)
      assertNotNull(result._2)
      assertEquals(0, result._2.length)
    }
  }

  class HeartbeatOperation extends GroupOperation[HeartbeatCallbackParams, HeartbeatCallback] {
    override def responseCallback(responsePromise: Promise[HeartbeatCallbackParams]): HeartbeatCallback = {
      val callback: HeartbeatCallback = error => responsePromise.success(error)
      callback
    }
    override def runWithCallback(member: GroupMember, responseCallback: HeartbeatCallback): Unit = {
      groupCoordinator.handleHeartbeat(member.groupId, member.memberId,
        member.groupInstanceId, member.generationId, responseCallback)
      replicaManager.tryCompleteActions()
    }
    override def awaitAndVerify(member: GroupMember): Unit = {
       val error = await(member, DefaultSessionTimeout)
       assertEquals(Errors.NONE, error)
    }
  }

  class OffsetFetchOperation extends GroupOperation[OffsetFetchCallbackParams, OffsetFetchCallback] {
    override def responseCallback(responsePromise: Promise[OffsetFetchCallbackParams]): OffsetFetchCallback = {
      val callback: OffsetFetchCallback = (error, offsets) => responsePromise.success(error, offsets)
      callback
    }
    override def runWithCallback(member: GroupMember, responseCallback: OffsetFetchCallback): Unit = {
      val (error, partitionData) = groupCoordinator.handleFetchOffsets(member.groupId, requireStable = true, None)
      replicaManager.tryCompleteActions()
      responseCallback(error, partitionData)
    }
    override def awaitAndVerify(member: GroupMember): Unit = {
      val result = await(member, 500)
      assertEquals(Errors.NONE, result._1)
      assertEquals(Map.empty, result._2)
    }
  }

  class CommitOffsetsOperation extends GroupOperation[CommitOffsetCallbackParams, CommitOffsetCallback] {
    override def responseCallback(responsePromise: Promise[CommitOffsetCallbackParams]): CommitOffsetCallback = {
      val callback: CommitOffsetCallback = offsets => responsePromise.success(offsets)
      callback
    }
    override def runWithCallback(member: GroupMember, responseCallback: CommitOffsetCallback): Unit = {
      val tip = new TopicIdPartition(Uuid.randomUuid(), 0, "topic")
      val offsets = immutable.Map(tip -> OffsetAndMetadata(1, "", Time.SYSTEM.milliseconds()))
      groupCoordinator.handleCommitOffsets(member.groupId, member.memberId,
        member.groupInstanceId, member.generationId, offsets, responseCallback)
      replicaManager.tryCompleteActions()
    }
    override def awaitAndVerify(member: GroupMember): Unit = {
       val offsets = await(member, 500)
       offsets.foreach { case (_, error) => assertEquals(Errors.NONE, error) }
    }
  }

  class CommitTxnOffsetsOperation(lock: Option[Lock] = None) extends CommitOffsetsOperation {
    override def runWithCallback(member: GroupMember, responseCallback: CommitOffsetCallback): Unit = {
      val offsets = immutable.Map(new TopicIdPartition(Uuid.randomUuid(), 0, "topic") -> OffsetAndMetadata(1, "", Time.SYSTEM.milliseconds()))
      val producerId = 1000L
      val producerEpoch : Short = 2
      // When transaction offsets are appended to the log, transactions may be scheduled for
      // completion. Since group metadata locks are acquired for transaction completion, include
      // this in the callback to test that there are no deadlocks.
      def callbackWithTxnCompletion(errors: Map[TopicIdPartition, Errors]): Unit = {
        val offsetsPartitions = (0 to numPartitions).map(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, _))
        groupCoordinator.groupManager.scheduleHandleTxnCompletion(producerId,
          offsetsPartitions.map(_.partition).toSet, isCommit = random.nextBoolean)
        responseCallback(errors)
      }
      lock.foreach(_.lock())
      try {
        // Since the replica manager is mocked we can use a dummy value for transactionalId.
        groupCoordinator.handleTxnCommitOffsets(member.group.groupId, "dummy-txn-id", producerId, producerEpoch,
          JoinGroupRequest.UNKNOWN_MEMBER_ID, Option.empty, JoinGroupRequest.UNKNOWN_GENERATION_ID,
          offsets, callbackWithTxnCompletion, RequestLocal.NoCaching, ApiKeys.TXN_OFFSET_COMMIT.latestVersion())
        replicaManager.tryCompleteActions()
      } finally lock.foreach(_.unlock())
    }
  }

  class CompleteTxnOperation(lock: Option[Lock] = None) extends GroupOperation[CompleteTxnCallbackParams, CompleteTxnCallback] {
    override def responseCallback(responsePromise: Promise[CompleteTxnCallbackParams]): CompleteTxnCallback = {
      val callback: CompleteTxnCallback = error => responsePromise.success(error)
      callback
    }
    override def runWithCallback(member: GroupMember, responseCallback: CompleteTxnCallback): Unit = {
      val producerId = 1000L
      val offsetsPartitions = (0 to numPartitions).map(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, _))
      lock.foreach(_.lock())
      try {
        groupCoordinator.groupManager.handleTxnCompletion(producerId,
          offsetsPartitions.map(_.partition).toSet, isCommit = random.nextBoolean)
        responseCallback(Errors.NONE)
      } finally lock.foreach(_.unlock())

    }
    override def awaitAndVerify(member: GroupMember): Unit = {
      val error = await(member, 500)
      assertEquals(Errors.NONE, error)
    }
  }

  class LeaveGroupOperation extends GroupOperation[LeaveGroupCallbackParams, LeaveGroupCallback] {
    override def responseCallback(responsePromise: Promise[LeaveGroupCallbackParams]): LeaveGroupCallback = {
      val callback: LeaveGroupCallback = result => responsePromise.success(result)
      callback
    }
    override def runWithCallback(member: GroupMember, responseCallback: LeaveGroupCallback): Unit = {
      val memberIdentity = new MemberIdentity()
          .setMemberId(member.memberId)
      groupCoordinator.handleLeaveGroup(member.group.groupId, List(memberIdentity), responseCallback)
    }
    override def awaitAndVerify(member: GroupMember): Unit = {
      val leaveGroupResult = await(member, DefaultSessionTimeout)

      val memberResponses = leaveGroupResult.memberResponses
      GroupCoordinatorTest.verifyLeaveGroupResult(leaveGroupResult, Errors.NONE, List(Errors.NONE))
      assertEquals(member.memberId, memberResponses.head.memberId)
      assertEquals(None, memberResponses.head.groupInstanceId)
    }
  }
}

object GroupCoordinatorConcurrencyTest {

  type JoinGroupCallbackParams = JoinGroupResult
  type JoinGroupCallback = JoinGroupResult => Unit
  type SyncGroupCallbackParams = (Errors, Array[Byte])
  type SyncGroupCallback = SyncGroupResult => Unit
  type HeartbeatCallbackParams = Errors
  type HeartbeatCallback = Errors => Unit
  type OffsetFetchCallbackParams = (Errors, Map[TopicPartition, OffsetFetchResponse.PartitionData])
  type OffsetFetchCallback = (Errors, Map[TopicPartition, OffsetFetchResponse.PartitionData]) => Unit
  type CommitOffsetCallbackParams = Map[TopicIdPartition, Errors]
  type CommitOffsetCallback = Map[TopicIdPartition, Errors] => Unit
  type LeaveGroupCallbackParams = LeaveGroupResult
  type LeaveGroupCallback = LeaveGroupResult => Unit
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
    groupCoordinator.groupManager.addOwnedPartition(groupPartitionId)
    val members = (0 until nMembers).map { i =>
      new GroupMember(this, groupPartitionId, i == 0)
    }
    def assignment: Map[String, Array[Byte]] = members.map { m => (m.memberId, Array[Byte]()) }.toMap
  }

  class GroupMember(val group: Group, val groupPartitionId: Int, val leader: Boolean) extends CoordinatorMember {
    @volatile var memberId: String = JoinGroupRequest.UNKNOWN_MEMBER_ID
    @volatile var groupInstanceId: Option[String] = None
    @volatile var generationId: Int = -1
    def groupId: String = group.groupId
  }

}
