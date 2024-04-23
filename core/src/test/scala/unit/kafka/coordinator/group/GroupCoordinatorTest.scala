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

import java.util.{Optional, OptionalInt}
import kafka.common.OffsetAndMetadata
import kafka.server.{ActionQueue, DelayedOperationPurgatory, HostedPartition, KafkaConfig, KafkaRequestHandler, ReplicaManager, RequestLocal}
import kafka.utils._
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.{MemoryRecords, RecordBatch}
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests.{JoinGroupRequest, OffsetCommitRequest, OffsetFetchResponse, TransactionResult}

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kafka.cluster.Partition
import kafka.zk.KafkaZkClient
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.server.util.timer.MockTimer
import org.apache.kafka.server.util.{KafkaScheduler, MockTime}
import org.apache.kafka.storage.internals.log.{AppendOrigin, VerificationGuard}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.mockito.ArgumentMatchers.{any, anyLong, anyShort}
import org.mockito.Mockito.{mock, when}

import scala.jdk.CollectionConverters._
import scala.collection.{Seq, mutable}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise, TimeoutException}

class GroupCoordinatorTest {
  import GroupCoordinatorTest._

  type JoinGroupCallback = JoinGroupResult => Unit
  type SyncGroupCallback = SyncGroupResult => Unit
  type HeartbeatCallbackParams = Errors
  type HeartbeatCallback = Errors => Unit
  type CommitOffsetCallbackParams = Map[TopicIdPartition, Errors]
  type CommitOffsetCallback = Map[TopicIdPartition, Errors] => Unit
  type LeaveGroupCallback = LeaveGroupResult => Unit

  val ClientId = "consumer-test"
  val ClientHost = "localhost"
  val GroupMinSessionTimeout = 10
  val GroupMaxSessionTimeout = 10 * 60 * 1000
  val GroupMaxSize = 4
  val DefaultRebalanceTimeout = 500
  val DefaultSessionTimeout = 500
  val GroupInitialRebalanceDelay = 50
  var timer: MockTimer = _
  var groupCoordinator: GroupCoordinator = _
  var replicaManager: ReplicaManager = _
  var scheduler: KafkaScheduler = _
  var zkClient: KafkaZkClient = _

  private val groupId = "groupId"
  private val protocolType = "consumer"
  private val protocolName = "range"
  private val memberId = "memberId"
  private val groupInstanceId = "groupInstanceId"
  private val leaderInstanceId = "leader"
  private val followerInstanceId = "follower"
  private val invalidMemberId = "invalidMember"
  private val metadata = Array[Byte]()
  private val protocols = List((protocolName, metadata))
  private val protocolSuperset = List((protocolName, metadata), ("roundrobin", metadata))
  private val requireStable = true
  private var groupPartitionId: Int = -1

  // we use this string value since its hashcode % #.partitions is different
  private val otherGroupId = "otherGroup"

  @BeforeEach
  def setUp(): Unit = {
    val props = TestUtils.createBrokerConfig(nodeId = 0, zkConnect = "")
    props.setProperty(GroupCoordinatorConfig.GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, GroupMinSessionTimeout.toString)
    props.setProperty(GroupCoordinatorConfig.GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG, GroupMaxSessionTimeout.toString)
    props.setProperty(GroupCoordinatorConfig.GROUP_MAX_SIZE_CONFIG, GroupMaxSize.toString)
    props.setProperty(GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, GroupInitialRebalanceDelay.toString)
    // make two partitions of the group topic to make sure some partitions are not owned by the coordinator
    val ret = mutable.Map[String, Map[Int, Seq[Int]]]()
    ret += (Topic.GROUP_METADATA_TOPIC_NAME -> Map(0 -> Seq(1), 1 -> Seq(1)))

    replicaManager = mock(classOf[ReplicaManager])

    zkClient = mock(classOf[KafkaZkClient])
    // make two partitions of the group topic to make sure some partitions are not owned by the coordinator
    when(zkClient.getTopicPartitionCount(Topic.GROUP_METADATA_TOPIC_NAME)).thenReturn(Some(2))

    timer = new MockTimer

    val config = KafkaConfig.fromProps(props)

    val heartbeatPurgatory = new DelayedOperationPurgatory[DelayedHeartbeat]("Heartbeat", timer, config.brokerId, reaperEnabled = false)
    val rebalancePurgatory = new DelayedOperationPurgatory[DelayedRebalance]("Rebalance", timer, config.brokerId, reaperEnabled = false)

    groupCoordinator = GroupCoordinator(config, replicaManager, heartbeatPurgatory, rebalancePurgatory, timer.time, new Metrics())
    groupCoordinator.startup(() => zkClient.getTopicPartitionCount(Topic.GROUP_METADATA_TOPIC_NAME).getOrElse(config.offsetsTopicPartitions),
      enableMetadataExpiration = false)

    // add the partition into the owned partition list
    groupPartitionId = groupCoordinator.partitionFor(groupId)
    groupCoordinator.groupManager.addOwnedPartition(groupPartitionId)
  }

  @AfterEach
  def tearDown(): Unit = {
    if (groupCoordinator != null)
      groupCoordinator.shutdown()
  }

  @Test
  def testRequestHandlingWhileLoadingInProgress(): Unit = {
    val otherGroupPartitionId = groupCoordinator.groupManager.partitionFor(otherGroupId)
    assertTrue(otherGroupPartitionId != groupPartitionId)

    groupCoordinator.groupManager.addLoadingPartition(otherGroupPartitionId)
    assertTrue(groupCoordinator.groupManager.isGroupLoading(otherGroupId))

    // Dynamic Member JoinGroup
    var joinGroupResponse: Option[JoinGroupResult] = None
    groupCoordinator.handleJoinGroup(otherGroupId, memberId, None, true, true, "clientId", "clientHost", 60000, 10000, "consumer",
      List("range" -> new Array[Byte](0)), result => { joinGroupResponse = Some(result)})
    assertEquals(Some(Errors.COORDINATOR_LOAD_IN_PROGRESS), joinGroupResponse.map(_.error))

    // Static Member JoinGroup
    groupCoordinator.handleJoinGroup(otherGroupId, memberId, Some("groupInstanceId"), false, true, "clientId", "clientHost", 60000, 10000, "consumer",
      List("range" -> new Array[Byte](0)), result => { joinGroupResponse = Some(result)})
    assertEquals(Some(Errors.COORDINATOR_LOAD_IN_PROGRESS), joinGroupResponse.map(_.error))

    // SyncGroup
    var syncGroupResponse: Option[Errors] = None
    groupCoordinator.handleSyncGroup(otherGroupId, 1, memberId, Some("consumer"), Some("range"), None, Map.empty[String, Array[Byte]],
      syncGroupResult => syncGroupResponse = Some(syncGroupResult.error))
    assertEquals(Some(Errors.REBALANCE_IN_PROGRESS), syncGroupResponse)

    // OffsetCommit
    val topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), 0 , "foo")
    var offsetCommitErrors = Map.empty[TopicIdPartition, Errors]
    groupCoordinator.handleCommitOffsets(otherGroupId, memberId, None, 1,
      Map(topicIdPartition -> offsetAndMetadata(15L)), result => { offsetCommitErrors = result })
    assertEquals(Map(topicIdPartition -> Errors.COORDINATOR_LOAD_IN_PROGRESS), offsetCommitErrors)

    // Heartbeat
    var heartbeatError: Option[Errors] = None
    groupCoordinator.handleHeartbeat(otherGroupId, memberId, None, 1, error => { heartbeatError = Some(error) })
    assertEquals(Some(Errors.NONE), heartbeatError)

    // DescribeGroups
    val (describeGroupError, _) = groupCoordinator.handleDescribeGroup(otherGroupId)
    assertEquals(Errors.COORDINATOR_LOAD_IN_PROGRESS, describeGroupError)

    // ListGroups
    val (listGroupsError, _) = groupCoordinator.handleListGroups(Set(), Set())
    assertEquals(Errors.COORDINATOR_LOAD_IN_PROGRESS, listGroupsError)

    // DeleteGroups
    val deleteGroupsErrors = groupCoordinator.handleDeleteGroups(Set(otherGroupId))
    assertEquals(Some(Errors.COORDINATOR_LOAD_IN_PROGRESS), deleteGroupsErrors.get(otherGroupId))

    // Check that non-loading groups are still accessible
    assertEquals(Errors.NONE, groupCoordinator.handleDescribeGroup(groupId)._1)

    // After loading, we should be able to access the group
    val otherGroupMetadataTopicPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, otherGroupPartitionId)
    when(replicaManager.getLog(otherGroupMetadataTopicPartition)).thenReturn(None)
    // Call removeGroupsAndOffsets so that partition removed from loadingPartitions
    groupCoordinator.groupManager.removeGroupsAndOffsets(otherGroupMetadataTopicPartition, OptionalInt.of(1), group => {})
    groupCoordinator.groupManager.loadGroupsAndOffsets(otherGroupMetadataTopicPartition, 1, group => {}, 0L)
    assertEquals(Errors.NONE, groupCoordinator.handleDescribeGroup(otherGroupId)._1)
  }

  @Test
  def testOffsetsRetentionMsIntegerOverflow(): Unit = {
    val props = TestUtils.createBrokerConfig(nodeId = 0, zkConnect = "")
    props.setProperty(GroupCoordinatorConfig.OFFSETS_RETENTION_MINUTES_CONFIG, Integer.MAX_VALUE.toString)
    val config = KafkaConfig.fromProps(props)
    val offsetConfig = GroupCoordinator.offsetConfig(config)
    assertEquals(offsetConfig.offsetsRetentionMs, Integer.MAX_VALUE * 60L * 1000L)
  }

  @Test
  def testJoinGroupWrongCoordinator(): Unit = {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    var joinGroupResult = dynamicJoinGroup(otherGroupId, memberId, protocolType, protocols)
    assertEquals(Errors.NOT_COORDINATOR, joinGroupResult.error)

    joinGroupResult = staticJoinGroup(otherGroupId, memberId, groupInstanceId, protocolType, protocols)
    assertEquals(Errors.NOT_COORDINATOR, joinGroupResult.error)
  }

  @Test
  def testJoinGroupShouldReceiveErrorIfGroupOverMaxSize(): Unit = {
    val futures = ArrayBuffer[Future[JoinGroupResult]]()
    val rebalanceTimeout = GroupInitialRebalanceDelay * 2

    for (i <- 1.to(GroupMaxSize)) {
      futures += sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols, rebalanceTimeout = rebalanceTimeout)
      if (i != 1)
        timer.advanceClock(GroupInitialRebalanceDelay)
    }
    // advance clock beyond rebalanceTimeout
    timer.advanceClock(GroupInitialRebalanceDelay + 1)
    for (future <- futures) {
      assertEquals(Errors.NONE, await(future, 1).error)
    }

    // Should receive an error since the group is full
    val errorFuture = sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols, rebalanceTimeout = rebalanceTimeout)
    assertEquals(Errors.GROUP_MAX_SIZE_REACHED, await(errorFuture, 1).error)
  }

  @Test
  def testDynamicMembersJoinGroupWithMaxSizeAndRequiredKnownMember(): Unit = {
    val requiredKnownMemberId = true
    val nbMembers = GroupMaxSize + 1

    // First JoinRequests
    var futures = 1.to(nbMembers).map { _ =>
      sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols,
        None, DefaultSessionTimeout, DefaultRebalanceTimeout, requiredKnownMemberId)
    }

    // Get back the assigned member ids
    val memberIds = futures.map(await(_, 1).memberId)

    // Second JoinRequests
    futures = memberIds.map { memberId =>
      sendJoinGroup(groupId, memberId, protocolType, protocols,
        None, DefaultSessionTimeout, DefaultRebalanceTimeout, requiredKnownMemberId)
    }

    // advance clock by GroupInitialRebalanceDelay to complete first InitialDelayedJoin
    timer.advanceClock(GroupInitialRebalanceDelay + 1)
    // advance clock by GroupInitialRebalanceDelay to complete second InitialDelayedJoin
    timer.advanceClock(GroupInitialRebalanceDelay + 1)

    // Awaiting results
    val errors = futures.map(await(_, DefaultRebalanceTimeout + 1).error)

    assertEquals(GroupMaxSize, errors.count(_ == Errors.NONE))
    assertEquals(nbMembers-GroupMaxSize, errors.count(_ == Errors.GROUP_MAX_SIZE_REACHED))

    // Members which were accepted can rejoin, others are rejected, while
    // completing rebalance
    futures = memberIds.map { memberId =>
      sendJoinGroup(groupId, memberId, protocolType, protocols,
        None, DefaultSessionTimeout, DefaultRebalanceTimeout, requiredKnownMemberId)
    }

    // Awaiting results
    val rejoinErrors = futures.map(await(_, 1).error)

    assertEquals(errors, rejoinErrors)
  }

  @Test
  def testDynamicMembersJoinGroupWithMaxSize(): Unit = {
    val requiredKnownMemberId = false
    val nbMembers = GroupMaxSize + 1

    // JoinRequests
    var futures = 1.to(nbMembers).map { _ =>
      sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols,
        None, DefaultSessionTimeout, DefaultRebalanceTimeout, requiredKnownMemberId)
    }

    // advance clock by GroupInitialRebalanceDelay to complete first InitialDelayedJoin
    timer.advanceClock(GroupInitialRebalanceDelay + 1)
    // advance clock by GroupInitialRebalanceDelay to complete second InitialDelayedJoin
    timer.advanceClock(GroupInitialRebalanceDelay + 1)

    // Awaiting results
    val joinGroupResults = futures.map(await(_, DefaultRebalanceTimeout + 1))
    val errors = joinGroupResults.map(_.error)

    assertEquals(GroupMaxSize, errors.count(_ == Errors.NONE))
    assertEquals(nbMembers-GroupMaxSize, errors.count(_ == Errors.GROUP_MAX_SIZE_REACHED))

    // Members which were accepted can rejoin, others are rejected, while
    // completing rebalance
    val memberIds = joinGroupResults.map(_.memberId)
    futures = memberIds.map { memberId =>
      sendJoinGroup(groupId, memberId, protocolType, protocols,
        None, DefaultSessionTimeout, DefaultRebalanceTimeout, requiredKnownMemberId)
    }

    // Awaiting results
    val rejoinErrors = futures.map(await(_, 1).error)

    assertEquals(errors, rejoinErrors)
  }

  @Test
  def testStaticMembersJoinGroupWithMaxSize(): Unit = {
    val nbMembers = GroupMaxSize + 1
    val instanceIds = 1.to(nbMembers).map(i => Some(s"instance-id-$i"))

    // JoinRequests
    var futures = instanceIds.map { instanceId =>
      sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols,
        instanceId, DefaultSessionTimeout, DefaultRebalanceTimeout)
    }

    // advance clock by GroupInitialRebalanceDelay to complete first InitialDelayedJoin
    timer.advanceClock(GroupInitialRebalanceDelay + 1)
    // advance clock by GroupInitialRebalanceDelay to complete second InitialDelayedJoin
    timer.advanceClock(GroupInitialRebalanceDelay + 1)

    // Awaiting results
    val joinGroupResults = futures.map(await(_, DefaultRebalanceTimeout + 1))
    val errors = joinGroupResults.map(_.error)

    assertEquals(GroupMaxSize, errors.count(_ == Errors.NONE))
    assertEquals(nbMembers-GroupMaxSize, errors.count(_ == Errors.GROUP_MAX_SIZE_REACHED))

    // Members which were accepted can rejoin, others are rejected, while
    // completing rebalance
    val memberIds = joinGroupResults.map(_.memberId)
    futures = instanceIds.zip(memberIds).map { case (instanceId, memberId) =>
      sendJoinGroup(groupId, memberId, protocolType, protocols,
        instanceId, DefaultSessionTimeout, DefaultRebalanceTimeout)
    }

    // Awaiting results
    val rejoinErrors = futures.map(await(_, 1).error)

    assertEquals(errors, rejoinErrors)
  }

  @Test
  def testDynamicMembersCanReJoinGroupWithMaxSizeWhileRebalancing(): Unit = {
    val requiredKnownMemberId = true
    val nbMembers = GroupMaxSize + 1

    // First JoinRequests
    var futures = 1.to(nbMembers).map { _ =>
      sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols,
        None, DefaultSessionTimeout, DefaultRebalanceTimeout, requiredKnownMemberId)
    }

    // Get back the assigned member ids
    val memberIds = futures.map(await(_, 1).memberId)

    // Second JoinRequests
    memberIds.map { memberId =>
      sendJoinGroup(groupId, memberId, protocolType, protocols,
        None, DefaultSessionTimeout, DefaultRebalanceTimeout, requiredKnownMemberId)
    }

    // Members can rejoin while rebalancing
    futures = memberIds.map { memberId =>
      sendJoinGroup(groupId, memberId, protocolType, protocols,
        None, DefaultSessionTimeout, DefaultRebalanceTimeout, requiredKnownMemberId)
    }

    // advance clock by GroupInitialRebalanceDelay to complete first InitialDelayedJoin
    timer.advanceClock(GroupInitialRebalanceDelay + 1)
    // advance clock by GroupInitialRebalanceDelay to complete second InitialDelayedJoin
    timer.advanceClock(GroupInitialRebalanceDelay + 1)

    // Awaiting results
    val errors = futures.map(await(_, DefaultRebalanceTimeout + 1).error)

    assertEquals(GroupMaxSize, errors.count(_ == Errors.NONE))
    assertEquals(nbMembers-GroupMaxSize, errors.count(_ == Errors.GROUP_MAX_SIZE_REACHED))
  }

  @Test
  def testLastJoiningMembersAreKickedOutWhenReJoiningGroupWithMaxSize(): Unit = {
    val nbMembers = GroupMaxSize + 2
    val group = new GroupMetadata(groupId, Stable, new MockTime())
    val memberIds = 1.to(nbMembers).map(_ => group.generateMemberId(ClientId, None))

    memberIds.foreach { memberId =>
      group.add(new MemberMetadata(memberId, None, ClientId, ClientHost,
        DefaultRebalanceTimeout, GroupMaxSessionTimeout, protocolType, protocols))
    }
    groupCoordinator.groupManager.addGroup(group)

    groupCoordinator.prepareRebalance(group, "")

    val futures = memberIds.map { memberId =>
      sendJoinGroup(groupId, memberId, protocolType, protocols,
        None, GroupMaxSessionTimeout, DefaultRebalanceTimeout)
    }

    // advance clock by GroupInitialRebalanceDelay to complete first InitialDelayedJoin
    timer.advanceClock(DefaultRebalanceTimeout + 1)

    // Awaiting results
    val errors = futures.map(await(_, DefaultRebalanceTimeout + 1).error)

    assertEquals(Set(Errors.NONE), errors.take(GroupMaxSize).toSet)
    assertEquals(Set(Errors.GROUP_MAX_SIZE_REACHED), errors.drop(GroupMaxSize).toSet)

    memberIds.drop(GroupMaxSize).foreach { memberId =>
      assertFalse(group.has(memberId))
    }
  }

  @Test
  def testJoinGroupSessionTimeoutTooSmall(): Unit = {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val joinGroupResult = dynamicJoinGroup(groupId, memberId, protocolType, protocols, sessionTimeout = GroupMinSessionTimeout - 1)
    assertEquals(Errors.INVALID_SESSION_TIMEOUT, joinGroupResult.error)
  }

  @Test
  def testJoinGroupSessionTimeoutTooLarge(): Unit = {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val joinGroupResult = dynamicJoinGroup(groupId, memberId, protocolType, protocols, sessionTimeout = GroupMaxSessionTimeout + 1)
    assertEquals(Errors.INVALID_SESSION_TIMEOUT, joinGroupResult.error)
  }

  @Test
  def testJoinGroupUnknownConsumerNewGroup(): Unit = {
    var joinGroupResult = dynamicJoinGroup(groupId, memberId, protocolType, protocols)
    assertEquals(Errors.UNKNOWN_MEMBER_ID, joinGroupResult.error)

    joinGroupResult = staticJoinGroup(groupId, memberId, groupInstanceId, protocolType, protocols)
    assertEquals(Errors.UNKNOWN_MEMBER_ID, joinGroupResult.error)
  }

  @Test
  def testInvalidGroupId(): Unit = {
    val groupId = ""
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val joinGroupResult = dynamicJoinGroup(groupId, memberId, protocolType, protocols)
    assertEquals(Errors.INVALID_GROUP_ID, joinGroupResult.error)
  }

  @Test
  def testValidJoinGroup(): Unit = {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val joinGroupResult = dynamicJoinGroup(groupId, memberId, protocolType, protocols)
    assertEquals(Errors.NONE, joinGroupResult.error)
  }

  @Test
  def testJoinGroupInconsistentProtocolType(): Unit = {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val otherMemberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val joinGroupResult = dynamicJoinGroup(groupId, memberId, protocolType, protocols)
    assertEquals(Errors.NONE, joinGroupResult.error)

    val otherJoinGroupResult = await(sendJoinGroup(groupId, otherMemberId, "connect", protocols), 1)
    assertEquals(Errors.INCONSISTENT_GROUP_PROTOCOL, otherJoinGroupResult.error)
  }

  @Test
  def testJoinGroupWithEmptyProtocolType(): Unit = {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    var joinGroupResult = dynamicJoinGroup(groupId, memberId, "", protocols)
    assertEquals(Errors.INCONSISTENT_GROUP_PROTOCOL, joinGroupResult.error)

    joinGroupResult = staticJoinGroup(groupId, memberId, groupInstanceId, "", protocols)
    assertEquals(Errors.INCONSISTENT_GROUP_PROTOCOL, joinGroupResult.error)
  }

  @Test
  def testJoinGroupWithEmptyGroupProtocol(): Unit = {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val joinGroupResult = dynamicJoinGroup(groupId, memberId, protocolType, List())
    assertEquals(Errors.INCONSISTENT_GROUP_PROTOCOL, joinGroupResult.error)
  }

  @Test
  def testNewMemberTimeoutCompletion(): Unit = {
    val sessionTimeout = GroupCoordinator.NewMemberJoinTimeoutMs + 5000
    val responseFuture = sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols, None, sessionTimeout, DefaultRebalanceTimeout, false)

    timer.advanceClock(GroupInitialRebalanceDelay + 1)

    val joinResult = Await.result(responseFuture, Duration(DefaultRebalanceTimeout + 100, TimeUnit.MILLISECONDS))
    val group = groupCoordinator.groupManager.getGroup(groupId).get
    val memberId = joinResult.memberId

    assertEquals(Errors.NONE, joinResult.error)
    assertEquals(0, group.allMemberMetadata.count(_.isNew))

    val syncGroupResult = syncGroupLeader(groupId, joinResult.generationId, memberId, Map(memberId -> Array[Byte]()))
    assertEquals(Errors.NONE, syncGroupResult.error)
    assertEquals(1, group.size)

    timer.advanceClock(GroupCoordinator.NewMemberJoinTimeoutMs + 100)

    // Make sure the NewMemberTimeout is not still in effect, and the member is not kicked
    assertEquals(1, group.size)

    timer.advanceClock(sessionTimeout + 100)
    assertEquals(0, group.size)
  }

  @Test
  def testNewMemberJoinExpiration(): Unit = {
    // This tests new member expiration during a protracted rebalance. We first create a
    // group with one member which uses a large value for session timeout and rebalance timeout.
    // We then join with one new member and let the rebalance hang while we await the first member.
    // The new member join timeout expires and its JoinGroup request is failed.

    val sessionTimeout = GroupCoordinator.NewMemberJoinTimeoutMs + 5000
    val rebalanceTimeout = GroupCoordinator.NewMemberJoinTimeoutMs * 2

    val firstJoinResult = dynamicJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols,
      sessionTimeout, rebalanceTimeout)
    val firstMemberId = firstJoinResult.memberId
    assertEquals(firstMemberId, firstJoinResult.leaderId)
    assertEquals(Errors.NONE, firstJoinResult.error)

    val groupOpt = groupCoordinator.groupManager.getGroup(groupId)
    assertTrue(groupOpt.isDefined)
    val group = groupOpt.get
    assertEquals(0, group.allMemberMetadata.count(_.isNew))

    val responseFuture = sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols, None, sessionTimeout, rebalanceTimeout)
    assertFalse(responseFuture.isCompleted)

    assertEquals(2, group.allMembers.size)
    assertEquals(1, group.allMemberMetadata.count(_.isNew))

    val newMember = group.allMemberMetadata.find(_.isNew).get
    assertNotEquals(firstMemberId, newMember.memberId)

    timer.advanceClock(GroupCoordinator.NewMemberJoinTimeoutMs + 1)
    assertTrue(responseFuture.isCompleted)

    val response = Await.result(responseFuture, Duration(0, TimeUnit.MILLISECONDS))
    assertEquals(Errors.UNKNOWN_MEMBER_ID, response.error)
    assertEquals(1, group.allMembers.size)
    assertEquals(0, group.allMemberMetadata.count(_.isNew))
    assertEquals(firstMemberId, group.allMembers.head)
  }

  @Test
  def testNewMemberFailureAfterJoinGroupCompletion(): Unit = {
    // For old versions of the JoinGroup protocol, new members were subject
    // to expiration if the rebalance took long enough. This test case ensures
    // that following completion of the JoinGroup phase, new members follow
    // normal heartbeat expiration logic.

    val firstJoinResult = dynamicJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols)
    val firstMemberId = firstJoinResult.memberId
    val firstGenerationId = firstJoinResult.generationId
    assertEquals(firstMemberId, firstJoinResult.leaderId)
    assertEquals(Errors.NONE, firstJoinResult.error)

    val firstSyncResult = syncGroupLeader(groupId, firstGenerationId, firstMemberId,
      Map(firstMemberId -> Array[Byte]()))
    assertEquals(Errors.NONE, firstSyncResult.error)

    val otherJoinFuture = sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols)

    val joinFuture = sendJoinGroup(groupId, firstMemberId, protocolType, protocols,
      requireKnownMemberId = false)

    val joinResult = await(joinFuture, DefaultSessionTimeout+100)
    val otherJoinResult = await(otherJoinFuture, DefaultSessionTimeout+100)
    assertEquals(Errors.NONE, joinResult.error)
    assertEquals(Errors.NONE, otherJoinResult.error)

    verifySessionExpiration(groupId)
  }

  @Test
  def testNewMemberFailureAfterSyncGroupCompletion(): Unit = {
    // For old versions of the JoinGroup protocol, new members were subject
    // to expiration if the rebalance took long enough. This test case ensures
    // that following completion of the SyncGroup phase, new members follow
    // normal heartbeat expiration logic.

    val firstJoinResult = dynamicJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols)
    val firstMemberId = firstJoinResult.memberId
    val firstGenerationId = firstJoinResult.generationId
    assertEquals(firstMemberId, firstJoinResult.leaderId)
    assertEquals(Errors.NONE, firstJoinResult.error)

    val firstSyncResult = syncGroupLeader(groupId, firstGenerationId, firstMemberId,
      Map(firstMemberId -> Array[Byte]()))
    assertEquals(Errors.NONE, firstSyncResult.error)

    val otherJoinFuture = sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols)

    val joinFuture = sendJoinGroup(groupId, firstMemberId, protocolType, protocols,
      requireKnownMemberId = false)

    val joinResult = await(joinFuture, DefaultSessionTimeout+100)
    val otherJoinResult = await(otherJoinFuture, DefaultSessionTimeout+100)
    assertEquals(Errors.NONE, joinResult.error)
    assertEquals(Errors.NONE, otherJoinResult.error)
    val secondGenerationId = joinResult.generationId
    val secondMemberId = otherJoinResult.memberId

    sendSyncGroupFollower(groupId, secondGenerationId, secondMemberId)

    val syncGroupResult = syncGroupLeader(groupId, secondGenerationId, firstMemberId,
      Map(firstMemberId -> Array.emptyByteArray, secondMemberId -> Array.emptyByteArray))
    assertEquals(Errors.NONE, syncGroupResult.error)

    verifySessionExpiration(groupId)
  }

  private def verifySessionExpiration(groupId: String): Unit = {
    when(replicaManager.getMagic(any[TopicPartition]))
      .thenReturn(Some(RecordBatch.CURRENT_MAGIC_VALUE))

    timer.advanceClock(DefaultSessionTimeout + 1)

    val groupMetadata = group(groupId)
    assertEquals(Empty, groupMetadata.currentState)
    assertTrue(groupMetadata.allMembers.isEmpty)
  }

  @Test
  def testJoinGroupInconsistentGroupProtocol(): Unit = {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val otherMemberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val joinGroupFuture = sendJoinGroup(groupId, memberId, protocolType, List(("range", metadata)))

    val otherJoinGroupResult = dynamicJoinGroup(groupId, otherMemberId, protocolType, List(("roundrobin", metadata)))
    timer.advanceClock(GroupInitialRebalanceDelay + 1)

    val joinGroupResult = await(joinGroupFuture, 1)
    assertEquals(Errors.NONE, joinGroupResult.error)
    assertEquals(Errors.INCONSISTENT_GROUP_PROTOCOL, otherJoinGroupResult.error)
  }

  @Test
  def testJoinGroupUnknownConsumerExistingGroup(): Unit = {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val otherMemberId = "memberId"

    val joinGroupResult = dynamicJoinGroup(groupId, memberId, protocolType, protocols)
    assertEquals(Errors.NONE, joinGroupResult.error)

    val otherJoinGroupResult = await(sendJoinGroup(groupId, otherMemberId, protocolType, protocols), 1)
    assertEquals(Errors.UNKNOWN_MEMBER_ID, otherJoinGroupResult.error)
  }

  @Test
  def testJoinGroupUnknownConsumerNewDeadGroup(): Unit = {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val deadGroupId = "deadGroupId"

    groupCoordinator.groupManager.addGroup(new GroupMetadata(deadGroupId, Dead, new MockTime()))
    val joinGroupResult = dynamicJoinGroup(deadGroupId, memberId, protocolType, protocols)
    assertEquals(Errors.COORDINATOR_NOT_AVAILABLE, joinGroupResult.error)
  }

  @Test
  def testSyncDeadGroup(): Unit = {
    val memberId = "memberId"
    val deadGroupId = "deadGroupId"

    groupCoordinator.groupManager.addGroup(new GroupMetadata(deadGroupId, Dead, new MockTime()))
    val syncGroupResult = syncGroupFollower(deadGroupId, 1, memberId)
    assertEquals(Errors.COORDINATOR_NOT_AVAILABLE, syncGroupResult.error)
  }

  @Test
  def testJoinGroupSecondJoinInconsistentProtocol(): Unit = {
    var responseFuture = sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols, requireKnownMemberId = true)
    var joinGroupResult = Await.result(responseFuture, Duration(DefaultRebalanceTimeout + 1, TimeUnit.MILLISECONDS))
    assertEquals(Errors.MEMBER_ID_REQUIRED, joinGroupResult.error)
    val memberId = joinGroupResult.memberId

    // Sending an inconsistent protocol shall be refused
    responseFuture = sendJoinGroup(groupId, memberId, protocolType, List(), requireKnownMemberId = true)
    joinGroupResult = Await.result(responseFuture, Duration(DefaultRebalanceTimeout + 1, TimeUnit.MILLISECONDS))
    assertEquals(Errors.INCONSISTENT_GROUP_PROTOCOL, joinGroupResult.error)

    // Sending consistent protocol shall be accepted
    responseFuture = sendJoinGroup(groupId, memberId, protocolType, protocols, requireKnownMemberId = true)
    timer.advanceClock(GroupInitialRebalanceDelay + 1)
    joinGroupResult = Await.result(responseFuture, Duration(DefaultRebalanceTimeout + 1, TimeUnit.MILLISECONDS))
    assertEquals(Errors.NONE, joinGroupResult.error)
  }

  @Test
  def staticMemberJoinAsFirstMember(): Unit = {
    val joinGroupResult = staticJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, groupInstanceId, protocolType, protocols)
    assertEquals(Errors.NONE, joinGroupResult.error)
  }

  @Test
  def staticMemberReJoinWithExplicitUnknownMemberId(): Unit = {
    var joinGroupResult = staticJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, groupInstanceId, protocolType, protocols)
    assertEquals(Errors.NONE, joinGroupResult.error)

    val unknownMemberId = "unknown_member"
    joinGroupResult = staticJoinGroup(groupId, unknownMemberId, groupInstanceId, protocolType, protocols)
    assertEquals(Errors.FENCED_INSTANCE_ID, joinGroupResult.error)
  }

  @Test
  def staticMemberFenceDuplicateRejoinedFollower(): Unit = {
    val rebalanceResult = staticMembersJoinAndRebalance(leaderInstanceId, followerInstanceId)

    // A third member joins will trigger rebalance.
    sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols)
    timer.advanceClock(1)
    assertTrue(getGroup(groupId).is(PreparingRebalance))

    timer.advanceClock(1)
    // Old follower rejoins group will be matching current member.id.
    val oldFollowerJoinGroupFuture =
      sendJoinGroup(groupId, rebalanceResult.followerId, protocolType, protocols, groupInstanceId = Some(followerInstanceId))

    timer.advanceClock(1)
    // Duplicate follower joins group with unknown member id will trigger member.id replacement.
    val duplicateFollowerJoinFuture =
      sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols, groupInstanceId = Some(followerInstanceId))

    timer.advanceClock(1)
    // Old member shall be fenced immediately upon duplicate follower joins.
    val oldFollowerJoinGroupResult = Await.result(oldFollowerJoinGroupFuture, Duration(1, TimeUnit.MILLISECONDS))
    checkJoinGroupResult(oldFollowerJoinGroupResult,
      Errors.FENCED_INSTANCE_ID,
      -1,
      Set.empty,
      PreparingRebalance,
      None)
    verifyDelayedTaskNotCompleted(duplicateFollowerJoinFuture)
  }

  @Test
  def staticMemberFenceDuplicateSyncingFollowerAfterMemberIdChanged(): Unit = {
    val rebalanceResult = staticMembersJoinAndRebalance(leaderInstanceId, followerInstanceId)

    // Known leader rejoins will trigger rebalance.
    val leaderJoinGroupFuture =
      sendJoinGroup(groupId, rebalanceResult.leaderId, protocolType, protocols, groupInstanceId = Some(leaderInstanceId))
    timer.advanceClock(1)
    assertTrue(getGroup(groupId).is(PreparingRebalance))

    timer.advanceClock(1)
    // Old follower rejoins group will match current member.id.
    val oldFollowerJoinGroupFuture =
      sendJoinGroup(groupId, rebalanceResult.followerId, protocolType, protocols, groupInstanceId = Some(followerInstanceId))

    timer.advanceClock(1)
    val leaderJoinGroupResult = Await.result(leaderJoinGroupFuture, Duration(1, TimeUnit.MILLISECONDS))
    checkJoinGroupResult(leaderJoinGroupResult,
      Errors.NONE,
      rebalanceResult.generation + 1,
      Set(leaderInstanceId, followerInstanceId),
      CompletingRebalance,
      Some(protocolType))
    assertEquals(rebalanceResult.leaderId, leaderJoinGroupResult.memberId)
    assertEquals(rebalanceResult.leaderId, leaderJoinGroupResult.leaderId)

    // Old follower shall be getting a successful join group response.
    val oldFollowerJoinGroupResult = Await.result(oldFollowerJoinGroupFuture, Duration(1, TimeUnit.MILLISECONDS))
    checkJoinGroupResult(oldFollowerJoinGroupResult,
      Errors.NONE,
      rebalanceResult.generation + 1,
      Set.empty,
      CompletingRebalance,
      Some(protocolType),
      expectedLeaderId = leaderJoinGroupResult.memberId)
    assertEquals(rebalanceResult.followerId, oldFollowerJoinGroupResult.memberId)
    assertEquals(rebalanceResult.leaderId, oldFollowerJoinGroupResult.leaderId)
    assertTrue(getGroup(groupId).is(CompletingRebalance))

    // Duplicate follower joins group with unknown member id will trigger member.id replacement,
    // and will also trigger a rebalance under CompletingRebalance state; the old follower sync callback
    // will return fenced exception while broker replaces the member identity with the duplicate follower joins.
    val oldFollowerSyncGroupFuture = sendSyncGroupFollower(groupId, oldFollowerJoinGroupResult.generationId,
      oldFollowerJoinGroupResult.memberId, Some(protocolType), Some(protocolName), Some(followerInstanceId))

    val duplicateFollowerJoinFuture =
      sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols, groupInstanceId = Some(followerInstanceId))
    timer.advanceClock(1)

    val oldFollowerSyncGroupResult = Await.result(oldFollowerSyncGroupFuture, Duration(1, TimeUnit.MILLISECONDS))
    assertEquals(Errors.FENCED_INSTANCE_ID, oldFollowerSyncGroupResult.error)
    assertTrue(getGroup(groupId).is(PreparingRebalance))

    timer.advanceClock(GroupInitialRebalanceDelay + 1)
    timer.advanceClock(DefaultRebalanceTimeout + 1)

    val duplicateFollowerJoinGroupResult = Await.result(duplicateFollowerJoinFuture, Duration(1, TimeUnit.MILLISECONDS))
    checkJoinGroupResult(duplicateFollowerJoinGroupResult,
      Errors.NONE,
      rebalanceResult.generation + 2,
      Set(followerInstanceId),   // this follower will become the new leader, and hence it would have the member list
      CompletingRebalance,
      Some(protocolType),
      expectedLeaderId = duplicateFollowerJoinGroupResult.memberId)
    assertTrue(getGroup(groupId).is(CompletingRebalance))
  }

  @Test
  def staticMemberFenceDuplicateRejoiningFollowerAfterMemberIdChanged(): Unit = {
    val rebalanceResult = staticMembersJoinAndRebalance(leaderInstanceId, followerInstanceId)

    // Known leader rejoins will trigger rebalance.
    val leaderJoinGroupFuture =
      sendJoinGroup(groupId, rebalanceResult.leaderId, protocolType, protocols, groupInstanceId = Some(leaderInstanceId))
    timer.advanceClock(1)
    assertTrue(getGroup(groupId).is(PreparingRebalance))

    // Duplicate follower joins group will trigger member.id replacement.
    val duplicateFollowerJoinGroupFuture =
      sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols, groupInstanceId = Some(followerInstanceId))

    timer.advanceClock(1)
    // Old follower rejoins group will fail because member.id already updated.
    val oldFollowerJoinGroupFuture =
      sendJoinGroup(groupId, rebalanceResult.followerId, protocolType, protocols, groupInstanceId = Some(followerInstanceId))

    val leaderRejoinGroupResult = Await.result(leaderJoinGroupFuture, Duration(1, TimeUnit.MILLISECONDS))
    checkJoinGroupResult(leaderRejoinGroupResult,
      Errors.NONE,
      rebalanceResult.generation + 1,
      Set(leaderInstanceId, followerInstanceId),
      CompletingRebalance,
      Some(protocolType))

    val duplicateFollowerJoinGroupResult = Await.result(duplicateFollowerJoinGroupFuture, Duration(1, TimeUnit.MILLISECONDS))
    checkJoinGroupResult(duplicateFollowerJoinGroupResult,
      Errors.NONE,
      rebalanceResult.generation + 1,
      Set.empty,
      CompletingRebalance,
      Some(protocolType))
    assertNotEquals(rebalanceResult.followerId, duplicateFollowerJoinGroupResult.memberId)

    val oldFollowerJoinGroupResult = Await.result(oldFollowerJoinGroupFuture, Duration(1, TimeUnit.MILLISECONDS))
    checkJoinGroupResult(oldFollowerJoinGroupResult,
      Errors.FENCED_INSTANCE_ID,
      -1,
      Set.empty,
      CompletingRebalance,
      None)
  }

  @Test
  def staticMemberRejoinWithKnownMemberId(): Unit = {
    var joinGroupResult = staticJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, groupInstanceId, protocolType, protocols)
    assertEquals(Errors.NONE, joinGroupResult.error)

    val assignedMemberId = joinGroupResult.memberId
    // The second join group should return immediately since we are using the same metadata during CompletingRebalance.
    val rejoinResponseFuture = sendJoinGroup(groupId, assignedMemberId, protocolType, protocols, Some(groupInstanceId))
    timer.advanceClock(1)
    joinGroupResult = Await.result(rejoinResponseFuture, Duration(1, TimeUnit.MILLISECONDS))
    assertEquals(Errors.NONE, joinGroupResult.error)
    assertTrue(getGroup(groupId).is(CompletingRebalance))

    val syncGroupFuture = sendSyncGroupLeader(groupId, joinGroupResult.generationId, assignedMemberId,
      Some(protocolType), Some(protocolName), Some(groupInstanceId), Map(assignedMemberId -> Array[Byte]()))
    timer.advanceClock(1)
    val syncGroupResult = Await.result(syncGroupFuture, Duration(1, TimeUnit.MILLISECONDS))
    assertEquals(Errors.NONE, syncGroupResult.error)
    assertTrue(getGroup(groupId).is(Stable))
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def staticMemberRejoinWithLeaderIdAndUnknownMemberId(supportSkippingAssignment: Boolean): Unit = {
    val rebalanceResult = staticMembersJoinAndRebalance(leaderInstanceId, followerInstanceId)

    // A static leader rejoin with unknown id will not trigger rebalance, and no assignment will be returned.
    val joinGroupResult = staticJoinGroupWithPersistence(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID,
      leaderInstanceId, protocolType, protocolSuperset, clockAdvance = 1, supportSkippingAssignment = supportSkippingAssignment)

    checkJoinGroupResult(joinGroupResult,
      Errors.NONE,
      rebalanceResult.generation, // The group should be at the same generation
      if (supportSkippingAssignment) Set(leaderInstanceId, followerInstanceId) else Set.empty,
      Stable,
      Some(protocolType),
      if (supportSkippingAssignment) joinGroupResult.memberId else rebalanceResult.leaderId,
      expectedSkipAssignment = supportSkippingAssignment
    )

    val oldLeaderJoinGroupResult = staticJoinGroup(groupId, rebalanceResult.leaderId, leaderInstanceId, protocolType, protocolSuperset, clockAdvance = 1)
    assertEquals(Errors.FENCED_INSTANCE_ID, oldLeaderJoinGroupResult.error)

    // Old leader will get fenced.
    val oldLeaderSyncGroupResult = syncGroupLeader(groupId, rebalanceResult.generation, rebalanceResult.leaderId,
      Map.empty, None, None, Some(leaderInstanceId))
    assertEquals(Errors.FENCED_INSTANCE_ID, oldLeaderSyncGroupResult.error)

    // Calling sync on old leader.id will fail because that leader.id is no longer valid and replaced.
    val newLeaderSyncGroupResult = syncGroupLeader(groupId, rebalanceResult.generation, rebalanceResult.leaderId, Map.empty)
    assertEquals(Errors.UNKNOWN_MEMBER_ID, newLeaderSyncGroupResult.error)
  }

  @Test
  def staticMemberRejoinWithLeaderIdAndKnownMemberId(): Unit = {
    val rebalanceResult = staticMembersJoinAndRebalance(leaderInstanceId, followerInstanceId,
      sessionTimeout = DefaultRebalanceTimeout / 2)

    // A static leader with known id rejoin will trigger rebalance.
    val joinGroupResult = staticJoinGroup(groupId, rebalanceResult.leaderId, leaderInstanceId,
      protocolType, protocolSuperset, clockAdvance = DefaultRebalanceTimeout + 1)
    // Timeout follower in the meantime.
    assertFalse(getGroup(groupId).hasStaticMember(followerInstanceId))
    checkJoinGroupResult(joinGroupResult,
      Errors.NONE,
      rebalanceResult.generation + 1, // The group has promoted to the new generation.
      Set(leaderInstanceId),
      CompletingRebalance,
      Some(protocolType),
      rebalanceResult.leaderId,
      rebalanceResult.leaderId)
  }

  @Test
  def staticMemberRejoinWithLeaderIdAndUnexpectedDeadGroup(): Unit = {
    val rebalanceResult = staticMembersJoinAndRebalance(leaderInstanceId, followerInstanceId)

    getGroup(groupId).transitionTo(Dead)

    val joinGroupResult = staticJoinGroup(groupId, rebalanceResult.leaderId, leaderInstanceId, protocolType, protocols, clockAdvance = 1)
    assertEquals(Errors.COORDINATOR_NOT_AVAILABLE, joinGroupResult.error)
  }

  @Test
  def staticMemberRejoinWithLeaderIdAndUnexpectedEmptyGroup(): Unit = {
    val rebalanceResult = staticMembersJoinAndRebalance(leaderInstanceId, followerInstanceId)

    getGroup(groupId).transitionTo(PreparingRebalance)
    getGroup(groupId).transitionTo(Empty)

    val joinGroupResult = staticJoinGroup(groupId, rebalanceResult.leaderId, leaderInstanceId, protocolType, protocols, clockAdvance = 1)
    assertEquals(Errors.UNKNOWN_MEMBER_ID, joinGroupResult.error)
  }

  @Test
  def staticMemberRejoinWithFollowerIdAndChangeOfProtocol(): Unit = {
    val rebalanceResult = staticMembersJoinAndRebalance(leaderInstanceId, followerInstanceId, sessionTimeout = DefaultSessionTimeout * 2)

    // A static follower rejoin with changed protocol will trigger rebalance.
    val newProtocols = List(("roundrobin", metadata))
    // Old leader hasn't joined in the meantime, triggering a re-election.
    val joinGroupResult = staticJoinGroup(groupId, rebalanceResult.followerId, followerInstanceId, protocolType, newProtocols, clockAdvance = DefaultSessionTimeout + 1)

    assertEquals(rebalanceResult.followerId, joinGroupResult.memberId)
    assertTrue(getGroup(groupId).hasStaticMember(leaderInstanceId))
    assertTrue(getGroup(groupId).isLeader(rebalanceResult.followerId))
    checkJoinGroupResult(joinGroupResult,
      Errors.NONE,
      rebalanceResult.generation + 1, // The group has promoted to the new generation, and leader has changed because old one times out.
      Set(leaderInstanceId, followerInstanceId),
      CompletingRebalance,
      Some(protocolType),
      rebalanceResult.followerId,
      rebalanceResult.followerId)
  }

  @Test
  def staticMemberRejoinWithUnknownMemberIdAndChangeOfProtocolWithSelectedProtocolChanged(): Unit = {
    val rebalanceResult = staticMembersJoinAndRebalance(leaderInstanceId, followerInstanceId)

    // A static follower rejoin with protocol changed and also cause updated group's selectedProtocol changed
    // should trigger rebalance.
    val selectedProtocols = getGroup(groupId).selectProtocol
    val newProtocols = List(("roundrobin", metadata))
    assert(!newProtocols.map(_._1).contains(selectedProtocols))
    // Old leader hasn't joined in the meantime, triggering a re-election.
    val joinGroupResult = staticJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, followerInstanceId, protocolType, newProtocols, clockAdvance = DefaultSessionTimeout + 1)

    checkJoinGroupResult(joinGroupResult,
      Errors.NONE,
      rebalanceResult.generation + 1,
      Set(leaderInstanceId, followerInstanceId),
      CompletingRebalance,
      Some(protocolType))

    assertTrue(getGroup(groupId).isLeader(joinGroupResult.memberId))
    assertNotEquals(rebalanceResult.followerId, joinGroupResult.memberId)
    assertEquals(joinGroupResult.protocolName, Some("roundrobin"))
  }

  @Test
  def staticMemberRejoinWithUnknownMemberIdAndChangeOfProtocolWhileSelectProtocolUnchangedPersistenceFailure(): Unit = {
    val rebalanceResult = staticMembersJoinAndRebalance(leaderInstanceId, followerInstanceId)

    val selectedProtocol = getGroup(groupId).selectProtocol
    val newProtocols = List((selectedProtocol, metadata))
    // Timeout old leader in the meantime.
    val joinGroupResult = staticJoinGroupWithPersistence(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID,
      followerInstanceId, protocolType, newProtocols, clockAdvance = 1, appendRecordError = Errors.MESSAGE_TOO_LARGE)

    checkJoinGroupResult(joinGroupResult,
      Errors.UNKNOWN_SERVER_ERROR,
      rebalanceResult.generation,
      Set.empty,
      Stable,
      Some(protocolType))

    // Join with old member id will not fail because the member id is not updated because of persistence failure
    assertNotEquals(rebalanceResult.followerId, joinGroupResult.memberId)
    val oldFollowerJoinGroupResult = staticJoinGroup(groupId, rebalanceResult.followerId, followerInstanceId, protocolType, newProtocols, clockAdvance = 1)
    assertEquals(Errors.NONE, oldFollowerJoinGroupResult.error)

    // Sync with old member id will also not fail because the member id is not updated because of persistence failure
    val syncGroupWithOldMemberIdResult = syncGroupFollower(groupId, rebalanceResult.generation,
      rebalanceResult.followerId, None, None, Some(followerInstanceId))
    assertEquals(Errors.NONE, syncGroupWithOldMemberIdResult.error)
  }

 @Test
 def staticMemberRejoinWithUpdatedSessionAndRebalanceTimeoutsButCannotPersistChange(): Unit = {
   val rebalanceResult = staticMembersJoinAndRebalance(leaderInstanceId, followerInstanceId)
   val joinGroupResult = staticJoinGroupWithPersistence(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, followerInstanceId, protocolType, protocolSuperset, clockAdvance = 1, 2 * DefaultSessionTimeout, 2 * DefaultRebalanceTimeout, appendRecordError = Errors.MESSAGE_TOO_LARGE)
   checkJoinGroupResult(joinGroupResult,
     Errors.UNKNOWN_SERVER_ERROR,
     rebalanceResult.generation,
     Set.empty,
     Stable,
     Some(protocolType))
   assertTrue(groupCoordinator.groupManager.getGroup(groupId).isDefined)
   val group = groupCoordinator.groupManager.getGroup(groupId).get
   group.allMemberMetadata.foreach { member =>
     assertEquals(member.sessionTimeoutMs, DefaultSessionTimeout)
     assertEquals(member.rebalanceTimeoutMs, DefaultRebalanceTimeout)
   }
 }


  @Test
  def staticMemberRejoinWithUpdatedSessionAndRebalanceTimeoutsAndPersistChange(): Unit = {
    val rebalanceResult = staticMembersJoinAndRebalance(leaderInstanceId, followerInstanceId)
    val followerJoinGroupResult = staticJoinGroupWithPersistence(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, followerInstanceId, protocolType, protocolSuperset, clockAdvance = 1, 2 * DefaultSessionTimeout, 2 * DefaultRebalanceTimeout)
    checkJoinGroupResult(followerJoinGroupResult,
      Errors.NONE,
      rebalanceResult.generation,
      Set.empty,
      Stable,
      Some(protocolType))
    val leaderJoinGroupResult = staticJoinGroupWithPersistence(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, leaderInstanceId, protocolType, protocolSuperset, clockAdvance = 1, 2 * DefaultSessionTimeout, 2 * DefaultRebalanceTimeout)
    checkJoinGroupResult(leaderJoinGroupResult,
      Errors.NONE,
      rebalanceResult.generation,
      Set(leaderInstanceId, followerInstanceId),
      Stable,
      Some(protocolType),
      leaderJoinGroupResult.leaderId,
      leaderJoinGroupResult.memberId,
      true)
    assertTrue(groupCoordinator.groupManager.getGroup(groupId).isDefined)
    val group = groupCoordinator.groupManager.getGroup(groupId).get
    group.allMemberMetadata.foreach { member =>
      assertEquals(member.sessionTimeoutMs, 2 * DefaultSessionTimeout)
      assertEquals(member.rebalanceTimeoutMs, 2 * DefaultRebalanceTimeout)
    }
  }
  @Test
  def staticMemberRejoinWithUnknownMemberIdAndChangeOfProtocolWhileSelectProtocolUnchanged(): Unit = {
    val rebalanceResult = staticMembersJoinAndRebalance(leaderInstanceId, followerInstanceId)

    // A static follower rejoin with protocol changing to leader protocol subset won't trigger rebalance if updated
    // group's selectProtocol remain unchanged.
    val selectedProtocol = getGroup(groupId).selectProtocol
    val newProtocols = List((selectedProtocol, metadata))
    // Timeout old leader in the meantime.
    val joinGroupResult = staticJoinGroupWithPersistence(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID,
      followerInstanceId, protocolType, newProtocols, clockAdvance = 1)

    checkJoinGroupResult(joinGroupResult,
      Errors.NONE,
      rebalanceResult.generation,
      Set.empty,
      Stable,
      Some(protocolType))

    // Join with old member id will fail because the member id is updated
    assertNotEquals(rebalanceResult.followerId, joinGroupResult.memberId)
    val oldFollowerJoinGroupResult = staticJoinGroup(groupId, rebalanceResult.followerId, followerInstanceId, protocolType, newProtocols, clockAdvance = 1)
    assertEquals(Errors.FENCED_INSTANCE_ID, oldFollowerJoinGroupResult.error)

    // Sync with old member id will fail because the member id is updated
    val syncGroupWithOldMemberIdResult = syncGroupFollower(groupId, rebalanceResult.generation,
      rebalanceResult.followerId, None, None, Some(followerInstanceId))
    assertEquals(Errors.FENCED_INSTANCE_ID, syncGroupWithOldMemberIdResult.error)

    val syncGroupWithNewMemberIdResult = syncGroupFollower(groupId, rebalanceResult.generation,
      joinGroupResult.memberId, None, None, Some(followerInstanceId))
    assertEquals(Errors.NONE, syncGroupWithNewMemberIdResult.error)
    assertEquals(rebalanceResult.followerAssignment, syncGroupWithNewMemberIdResult.memberAssignment)
  }

  @Test
  def staticMemberRejoinWithKnownLeaderIdToTriggerRebalanceAndFollowerWithChangeofProtocol(): Unit = {
    val rebalanceResult = staticMembersJoinAndRebalance(leaderInstanceId, followerInstanceId)

    // A static leader rejoin with known member id will trigger rebalance.
    val leaderRejoinGroupFuture = sendJoinGroup(groupId, rebalanceResult.leaderId, protocolType,
      protocolSuperset, Some(leaderInstanceId))
    // Rebalance complete immediately after follower rejoin.
    val followerRejoinWithFuture = sendJoinGroup(groupId, rebalanceResult.followerId, protocolType,
      protocolSuperset, Some(followerInstanceId))

    timer.advanceClock(1)

    // Leader should get the same assignment as last round.
    checkJoinGroupResult(await(leaderRejoinGroupFuture, 1),
      Errors.NONE,
      rebalanceResult.generation + 1, // The group has promoted to the new generation.
      Set(leaderInstanceId, followerInstanceId),
      CompletingRebalance,
      Some(protocolType),
      rebalanceResult.leaderId,
      rebalanceResult.leaderId)

    checkJoinGroupResult(await(followerRejoinWithFuture, 1),
      Errors.NONE,
      rebalanceResult.generation + 1, // The group has promoted to the new generation.
      Set.empty,
      CompletingRebalance,
      Some(protocolType),
      rebalanceResult.leaderId,
      rebalanceResult.followerId)

    // The follower protocol changed from protocolSuperset to general protocols.
    val followerRejoinWithProtocolChangeFuture = sendJoinGroup(groupId, rebalanceResult.followerId,
      protocolType, protocols, Some(followerInstanceId))
    // The group will transit to PreparingRebalance due to protocol change from follower.
    assertTrue(getGroup(groupId).is(PreparingRebalance))

    timer.advanceClock(DefaultRebalanceTimeout + 1)
    checkJoinGroupResult(await(followerRejoinWithProtocolChangeFuture, 1),
      Errors.NONE,
      rebalanceResult.generation + 2, // The group has promoted to the new generation.
      Set(followerInstanceId),
      CompletingRebalance,
      Some(protocolType),
      rebalanceResult.followerId,
      rebalanceResult.followerId)
  }

  @Test
  def staticMemberRejoinAsFollowerWithUnknownMemberId(): Unit = {
    val rebalanceResult = staticMembersJoinAndRebalance(leaderInstanceId, followerInstanceId)

    // A static follower rejoin with no protocol change will not trigger rebalance.
    val joinGroupResult = staticJoinGroupWithPersistence(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, followerInstanceId, protocolType, protocolSuperset, clockAdvance = 1)

    // Old leader shouldn't be timed out.
    assertTrue(getGroup(groupId).hasStaticMember(leaderInstanceId))
    checkJoinGroupResult(joinGroupResult,
      Errors.NONE,
      rebalanceResult.generation, // The group has no change.
      Set.empty,
      Stable,
      Some(protocolType))

    assertNotEquals(rebalanceResult.followerId, joinGroupResult.memberId)

    val syncGroupResult = syncGroupFollower(groupId, rebalanceResult.generation, joinGroupResult.memberId)
    assertEquals(Errors.NONE, syncGroupResult.error)
    assertEquals(rebalanceResult.followerAssignment, syncGroupResult.memberAssignment)
  }

  @Test
  def staticMemberRejoinAsFollowerWithKnownMemberIdAndNoProtocolChange(): Unit = {
    val rebalanceResult  = staticMembersJoinAndRebalance(leaderInstanceId, followerInstanceId)

    // A static follower rejoin with no protocol change will not trigger rebalance.
    val joinGroupResult = staticJoinGroup(groupId, rebalanceResult.followerId, followerInstanceId, protocolType, protocolSuperset, clockAdvance = 1)

    // Old leader shouldn't be timed out.
    assertTrue(getGroup(groupId).hasStaticMember(leaderInstanceId))
    checkJoinGroupResult(joinGroupResult,
      Errors.NONE,
      rebalanceResult.generation, // The group has no change.
      Set.empty,
      Stable,
      Some(protocolType),
      rebalanceResult.leaderId,
      rebalanceResult.followerId)
  }

  @Test
  def staticMemberRejoinAsFollowerWithMismatchedMemberId(): Unit = {
    val rebalanceResult = staticMembersJoinAndRebalance(leaderInstanceId, followerInstanceId)

    val joinGroupResult = staticJoinGroup(groupId, rebalanceResult.followerId, leaderInstanceId, protocolType, protocolSuperset, clockAdvance = 1)
    assertEquals(Errors.FENCED_INSTANCE_ID, joinGroupResult.error)
  }

  @Test
  def staticMemberRejoinAsLeaderWithMismatchedMemberId(): Unit = {
    val rebalanceResult = staticMembersJoinAndRebalance(leaderInstanceId, followerInstanceId)

    val joinGroupResult = staticJoinGroup(groupId, rebalanceResult.leaderId, followerInstanceId, protocolType, protocolSuperset, clockAdvance = 1)
    assertEquals(Errors.FENCED_INSTANCE_ID, joinGroupResult.error)
  }

  @Test
  def staticMemberSyncAsLeaderWithInvalidMemberId(): Unit = {
    val rebalanceResult = staticMembersJoinAndRebalance(leaderInstanceId, followerInstanceId)

    val syncGroupResult = syncGroupLeader(groupId, rebalanceResult.generation, "invalid",
      Map.empty, None, None, Some(leaderInstanceId))
    assertEquals(Errors.FENCED_INSTANCE_ID, syncGroupResult.error)
  }

  @Test
  def staticMemberHeartbeatLeaderWithInvalidMemberId(): Unit = {
    val rebalanceResult = staticMembersJoinAndRebalance(leaderInstanceId, followerInstanceId)

    val syncGroupResult = syncGroupLeader(groupId, rebalanceResult.generation, rebalanceResult.leaderId, Map.empty)
    assertEquals(Errors.NONE, syncGroupResult.error)

    val validHeartbeatResult = heartbeat(groupId, rebalanceResult.leaderId, rebalanceResult.generation)
    assertEquals(Errors.NONE, validHeartbeatResult)

    val invalidHeartbeatResult = heartbeat(groupId, invalidMemberId, rebalanceResult.generation, Some(leaderInstanceId))
    assertEquals(Errors.FENCED_INSTANCE_ID, invalidHeartbeatResult)
  }

  @Test
  def shouldGetDifferentStaticMemberIdAfterEachRejoin(): Unit = {
    val initialResult = staticMembersJoinAndRebalance(leaderInstanceId, followerInstanceId)

    val timeAdvance = 1
    var lastMemberId = initialResult.leaderId
    for (_ <- 1 to 5) {
      val joinGroupResult = staticJoinGroupWithPersistence(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID,
        leaderInstanceId, protocolType, protocols, clockAdvance = timeAdvance)
      assertTrue(joinGroupResult.memberId.startsWith(leaderInstanceId))
      assertNotEquals(lastMemberId, joinGroupResult.memberId)
      lastMemberId = joinGroupResult.memberId
    }
  }

  @Test
  def testOffsetCommitDeadGroup(): Unit = {
    val memberId = "memberId"

    val deadGroupId = "deadGroupId"
    val tip = new TopicIdPartition(Uuid.randomUuid(), 0, "topic")
    val offset = offsetAndMetadata(0)

    groupCoordinator.groupManager.addGroup(new GroupMetadata(deadGroupId, Dead, new MockTime()))
    val offsetCommitResult = commitOffsets(deadGroupId, memberId, 1, Map(tip -> offset))
    assertEquals(Map(tip -> Errors.COORDINATOR_NOT_AVAILABLE), offsetCommitResult)
  }

  @Test
  def staticMemberCommitOffsetWithInvalidMemberId(): Unit = {
    val rebalanceResult = staticMembersJoinAndRebalance(leaderInstanceId, followerInstanceId)

    val syncGroupResult = syncGroupLeader(groupId, rebalanceResult.generation, rebalanceResult.leaderId, Map.empty)
    assertEquals(Errors.NONE, syncGroupResult.error)

    val tip = new TopicIdPartition(Uuid.randomUuid(), 0, "topic")
    val offset = offsetAndMetadata(0)
    val validOffsetCommitResult = commitOffsets(groupId, rebalanceResult.leaderId, rebalanceResult.generation, Map(tip -> offset))
    assertEquals(Map(tip -> Errors.NONE), validOffsetCommitResult)

    val invalidOffsetCommitResult = commitOffsets(groupId, invalidMemberId, rebalanceResult.generation,
      Map(tip -> offset), Some(leaderInstanceId))
    assertEquals(Map(tip -> Errors.FENCED_INSTANCE_ID), invalidOffsetCommitResult)
  }

  @Test
  def staticMemberJoinWithUnknownInstanceIdAndKnownMemberId(): Unit = {
    val rebalanceResult = staticMembersJoinAndRebalance(leaderInstanceId, followerInstanceId)

    val joinGroupResult = staticJoinGroup(groupId, rebalanceResult.leaderId, "unknown_instance",
      protocolType, protocolSuperset, clockAdvance = 1)

    assertEquals(Errors.UNKNOWN_MEMBER_ID, joinGroupResult.error)
  }

  @Test
  def staticMemberReJoinWithIllegalStateAsUnknownMember(): Unit = {
    staticMembersJoinAndRebalance(leaderInstanceId, followerInstanceId)
    val group = groupCoordinator.groupManager.getGroup(groupId).get
    group.transitionTo(PreparingRebalance)
    group.transitionTo(Empty)

    // Illegal state exception shall trigger since follower id resides in pending member bucket.
    val expectedException = assertThrows(classOf[IllegalStateException],
      () => staticJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, followerInstanceId, protocolType, protocolSuperset, clockAdvance = 1))

    val message = expectedException.getMessage
    assertTrue(message.contains(group.groupId))
    assertTrue(message.contains(followerInstanceId))
  }

  @Test
  def testLeaderFailToRejoinBeforeFinalRebalanceTimeoutWithLongSessionTimeout(): Unit = {
    groupStuckInRebalanceTimeoutDueToNonjoinedStaticMember()

    timer.advanceClock(DefaultRebalanceTimeout + 1)
    // The static leader should already session timeout, moving group towards Empty
    assertEquals(Set.empty, getGroup(groupId).allMembers)
    assertNull(getGroup(groupId).leaderOrNull)
    assertEquals(3, getGroup(groupId).generationId)
    assertGroupState(groupState = Empty)
  }

  @Test
  def testLeaderRejoinBeforeFinalRebalanceTimeoutWithLongSessionTimeout(): Unit = {
    groupStuckInRebalanceTimeoutDueToNonjoinedStaticMember()

    // The static leader should be back now, moving group towards CompletingRebalance
    val leaderRejoinGroupResult = staticJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, leaderInstanceId, protocolType, protocols)
    checkJoinGroupResult(leaderRejoinGroupResult,
      Errors.NONE,
      3,
      Set(leaderInstanceId),
      CompletingRebalance,
      Some(protocolType)
    )
    assertEquals(Set(leaderRejoinGroupResult.memberId), getGroup(groupId).allMembers)
    assertNotNull(getGroup(groupId).leaderOrNull)
    assertEquals(3, getGroup(groupId).generationId)
  }

  def groupStuckInRebalanceTimeoutDueToNonjoinedStaticMember(): Unit = {
    val longSessionTimeout = DefaultSessionTimeout * 2
    val rebalanceResult = staticMembersJoinAndRebalance(leaderInstanceId, followerInstanceId, sessionTimeout = longSessionTimeout)

    val dynamicJoinFuture = sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocolSuperset, sessionTimeout = longSessionTimeout)
    timer.advanceClock(DefaultRebalanceTimeout + 1)

    val dynamicJoinResult = await(dynamicJoinFuture, 100)
    // The new dynamic member has been elected as leader
    assertEquals(dynamicJoinResult.leaderId, dynamicJoinResult.memberId)
    assertEquals(Errors.NONE, dynamicJoinResult.error)
    assertEquals(3, dynamicJoinResult.members.size)
    assertEquals(2, dynamicJoinResult.generationId)
    assertGroupState(groupState = CompletingRebalance)

    assertEquals(Set(rebalanceResult.leaderId, rebalanceResult.followerId,
      dynamicJoinResult.memberId), getGroup(groupId).allMembers)
    assertEquals(Set(leaderInstanceId, followerInstanceId),
      getGroup(groupId).allStaticMembers)
    assertEquals(Set(dynamicJoinResult.memberId), getGroup(groupId).allDynamicMembers)

    // Send a special leave group request from static follower, moving group towards PreparingRebalance
    val followerLeaveGroupResults = singleLeaveGroup(groupId, rebalanceResult.followerId)
    verifyLeaveGroupResult(followerLeaveGroupResults)
    assertGroupState(groupState = PreparingRebalance)

    timer.advanceClock(DefaultRebalanceTimeout + 1)
    // Only static leader is maintained, and group is stuck at PreparingRebalance stage
    assertTrue(getGroup(groupId).allDynamicMembers.isEmpty)
    assertEquals(Set(rebalanceResult.leaderId), getGroup(groupId).allMembers)
    assertTrue(getGroup(groupId).allDynamicMembers.isEmpty)
    assertEquals(2, getGroup(groupId).generationId)
    assertGroupState(groupState = PreparingRebalance)
  }

  @Test
  def testStaticMemberFollowerFailToRejoinBeforeRebalanceTimeout(): Unit = {
    // Increase session timeout so that the follower won't be evicted when rebalance timeout is reached.
    val initialRebalanceResult = staticMembersJoinAndRebalance(leaderInstanceId, followerInstanceId, sessionTimeout = DefaultRebalanceTimeout * 2)

    val newMemberInstanceId = "newMember"

    val leaderId = initialRebalanceResult.leaderId

    val newMemberJoinGroupFuture = sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType,
      protocolSuperset, Some(newMemberInstanceId))
    assertGroupState(groupState = PreparingRebalance)

    val leaderRejoinGroupResult = staticJoinGroup(groupId, leaderId, leaderInstanceId, protocolType, protocolSuperset, clockAdvance = DefaultRebalanceTimeout + 1)
    checkJoinGroupResult(leaderRejoinGroupResult,
      Errors.NONE,
      initialRebalanceResult.generation + 1,
      Set(leaderInstanceId, followerInstanceId, newMemberInstanceId),
      CompletingRebalance,
      Some(protocolType),
      expectedLeaderId = leaderId,
      expectedMemberId = leaderId)

    val newMemberJoinGroupResult = Await.result(newMemberJoinGroupFuture, Duration(1, TimeUnit.MILLISECONDS))
    assertEquals(Errors.NONE, newMemberJoinGroupResult.error)
    checkJoinGroupResult(newMemberJoinGroupResult,
      Errors.NONE,
      initialRebalanceResult.generation + 1,
      Set.empty,
      CompletingRebalance,
      Some(protocolType),
      expectedLeaderId = leaderId)
  }

  @Test
  def testStaticMemberLeaderFailToRejoinBeforeRebalanceTimeout(): Unit = {
    // Increase session timeout so that the leader won't be evicted when rebalance timeout is reached.
    val initialRebalanceResult = staticMembersJoinAndRebalance(leaderInstanceId, followerInstanceId, sessionTimeout = DefaultRebalanceTimeout * 2)

    val newMemberInstanceId = "newMember"

    val newMemberJoinGroupFuture = sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType,
      protocolSuperset, Some(newMemberInstanceId))
    timer.advanceClock(1)
    assertGroupState(groupState = PreparingRebalance)

    val oldFollowerRejoinGroupResult = staticJoinGroup(groupId, initialRebalanceResult.followerId, followerInstanceId, protocolType, protocolSuperset, clockAdvance = DefaultRebalanceTimeout + 1)
    val newMemberJoinGroupResult = Await.result(newMemberJoinGroupFuture, Duration(1, TimeUnit.MILLISECONDS))

    val (newLeaderResult, newFollowerResult) = if (oldFollowerRejoinGroupResult.leaderId == oldFollowerRejoinGroupResult.memberId)
      (oldFollowerRejoinGroupResult, newMemberJoinGroupResult)
    else
      (newMemberJoinGroupResult, oldFollowerRejoinGroupResult)

    checkJoinGroupResult(newLeaderResult,
      Errors.NONE,
      initialRebalanceResult.generation + 1,
      Set(leaderInstanceId, followerInstanceId, newMemberInstanceId),
      CompletingRebalance,
      Some(protocolType))

    checkJoinGroupResult(newFollowerResult,
      Errors.NONE,
      initialRebalanceResult.generation + 1,
      Set.empty,
      CompletingRebalance,
      Some(protocolType),
      expectedLeaderId = newLeaderResult.memberId)
  }

  @Test
  def testJoinGroupProtocolTypeIsNotProvidedWhenAnErrorOccurs(): Unit = {
    // JoinGroup(leader)
    val leaderResponseFuture = sendJoinGroup(groupId, "fake-id", protocolType,
      protocolSuperset, Some(leaderInstanceId), DefaultSessionTimeout)

    // The Protocol Type is None when there is an error
    val leaderJoinGroupResult = await(leaderResponseFuture, 1)
    assertEquals(Errors.UNKNOWN_MEMBER_ID, leaderJoinGroupResult.error)
    assertEquals(None, leaderJoinGroupResult.protocolType)
  }

  @Test
  def testJoinGroupReturnsTheProtocolType(): Unit = {
    // JoinGroup(leader)
    val leaderResponseFuture = sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType,
      protocolSuperset, Some(leaderInstanceId), DefaultSessionTimeout)

    // JoinGroup(follower)
    val followerResponseFuture = sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType,
      protocolSuperset, Some(followerInstanceId), DefaultSessionTimeout)

    timer.advanceClock(GroupInitialRebalanceDelay + 1)
    timer.advanceClock(DefaultRebalanceTimeout + 1)

    // The Protocol Type is Defined when there is not error
    val leaderJoinGroupResult = await(leaderResponseFuture, 1)
    assertEquals(Errors.NONE, leaderJoinGroupResult.error)
    assertEquals(protocolType, leaderJoinGroupResult.protocolType.orNull)

    // The Protocol Type is Defined when there is not error
    val followerJoinGroupResult = await(followerResponseFuture, 1)
    assertEquals(Errors.NONE, followerJoinGroupResult.error)
    assertEquals(protocolType, followerJoinGroupResult.protocolType.orNull)
  }

  @Test
  def testSyncGroupReturnsAnErrorWhenProtocolTypeIsInconsistent(): Unit = {
    testSyncGroupProtocolTypeAndNameWith(Some("whatever"), None, Errors.INCONSISTENT_GROUP_PROTOCOL,
      None, None)
  }

  @Test
  def testSyncGroupReturnsAnErrorWhenProtocolNameIsInconsistent(): Unit = {
    testSyncGroupProtocolTypeAndNameWith(None, Some("whatever"), Errors.INCONSISTENT_GROUP_PROTOCOL,
      None, None)
  }

  @Test
  def testSyncGroupSucceedWhenProtocolTypeAndNameAreNotProvided(): Unit = {
    testSyncGroupProtocolTypeAndNameWith(None, None, Errors.NONE,
      Some(protocolType), Some(protocolName))
  }

  @Test
  def testSyncGroupSucceedWhenProtocolTypeAndNameAreConsistent(): Unit = {
    testSyncGroupProtocolTypeAndNameWith(Some(protocolType), Some(protocolName),
      Errors.NONE, Some(protocolType), Some(protocolName))
  }

  private def testSyncGroupProtocolTypeAndNameWith(protocolType: Option[String],
                                                   protocolName: Option[String],
                                                   expectedError: Errors,
                                                   expectedProtocolType: Option[String],
                                                   expectedProtocolName: Option[String]): Unit = {
    // JoinGroup(leader) with the Protocol Type of the group
    val leaderResponseFuture = sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, this.protocolType,
      protocolSuperset, Some(leaderInstanceId), DefaultSessionTimeout)

    // JoinGroup(follower) with the Protocol Type of the group
    val followerResponseFuture = sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, this.protocolType,
      protocolSuperset, Some(followerInstanceId), DefaultSessionTimeout)

    timer.advanceClock(GroupInitialRebalanceDelay + 1)
    timer.advanceClock(DefaultRebalanceTimeout + 1)

    val leaderJoinGroupResult = await(leaderResponseFuture, 1)
    val leaderId = leaderJoinGroupResult.memberId
    val generationId = leaderJoinGroupResult.generationId
    val followerJoinGroupResult = await(followerResponseFuture, 1)
    val followerId = followerJoinGroupResult.memberId

    // SyncGroup with the provided Protocol Type and Name
    val leaderSyncGroupResult = syncGroupLeader(groupId, generationId, leaderId,
      Map(leaderId -> Array.empty), protocolType, protocolName)
    assertEquals(expectedError, leaderSyncGroupResult.error)
    assertEquals(expectedProtocolType, leaderSyncGroupResult.protocolType)
    assertEquals(expectedProtocolName, leaderSyncGroupResult.protocolName)

    // SyncGroup with the provided Protocol Type and Name
    val followerSyncGroupResult = syncGroupFollower(groupId, generationId, followerId,
      protocolType, protocolName)
    assertEquals(expectedError, followerSyncGroupResult.error)
    assertEquals(expectedProtocolType, followerSyncGroupResult.protocolType)
    assertEquals(expectedProtocolName, followerSyncGroupResult.protocolName)
  }

  private class RebalanceResult(val generation: Int,
                                val leaderId: String,
                                val leaderAssignment: Array[Byte],
                                val followerId: String,
                                val followerAssignment: Array[Byte])
  /**
    * Generate static member rebalance results, including:
    *   - generation
    *   - leader id
    *   - leader assignment
    *   - follower id
    *   - follower assignment
    */
  private def staticMembersJoinAndRebalance(leaderInstanceId: String,
                                            followerInstanceId: String,
                                            sessionTimeout: Int = DefaultSessionTimeout,
                                            rebalanceTimeout: Int = DefaultRebalanceTimeout): RebalanceResult = {
    val leaderResponseFuture = sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType,
      protocolSuperset, Some(leaderInstanceId), sessionTimeout, rebalanceTimeout)

    val followerResponseFuture = sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType,
      protocolSuperset, Some(followerInstanceId), sessionTimeout, rebalanceTimeout)
    // The goal for two timer advance is to let first group initial join complete and set newMemberAdded flag to false. Next advance is
    // to trigger the rebalance as needed for follower delayed join. One large time advance won't help because we could only populate one
    // delayed join from purgatory and the new delayed op is created at that time and never be triggered.
    timer.advanceClock(GroupInitialRebalanceDelay + 1)
    timer.advanceClock(DefaultRebalanceTimeout + 1)
    val newGeneration = 1

    val leaderJoinGroupResult = await(leaderResponseFuture, 1)
    assertEquals(Errors.NONE, leaderJoinGroupResult.error)
    assertEquals(newGeneration, leaderJoinGroupResult.generationId)

    val followerJoinGroupResult = await(followerResponseFuture, 1)
    assertEquals(Errors.NONE, followerJoinGroupResult.error)
    assertEquals(newGeneration, followerJoinGroupResult.generationId)

    val leaderId = leaderJoinGroupResult.memberId
    val leaderSyncGroupResult = syncGroupLeader(groupId, leaderJoinGroupResult.generationId, leaderId, Map(leaderId -> Array[Byte]()))
    assertEquals(Errors.NONE, leaderSyncGroupResult.error)
    assertTrue(getGroup(groupId).is(Stable))

    val followerId = followerJoinGroupResult.memberId
    val followerSyncGroupResult = syncGroupFollower(groupId, leaderJoinGroupResult.generationId, followerId)
    assertEquals(Errors.NONE, followerSyncGroupResult.error)
    assertTrue(getGroup(groupId).is(Stable))

    new RebalanceResult(newGeneration,
      leaderId,
      leaderSyncGroupResult.memberAssignment,
      followerId,
      followerSyncGroupResult.memberAssignment)
  }

  private def checkJoinGroupResult(joinGroupResult: JoinGroupResult,
                                   expectedError: Errors,
                                   expectedGeneration: Int,
                                   expectedGroupInstanceIds: Set[String],
                                   expectedGroupState: GroupState,
                                   expectedProtocolType: Option[String],
                                   expectedLeaderId: String = JoinGroupRequest.UNKNOWN_MEMBER_ID,
                                   expectedMemberId: String = JoinGroupRequest.UNKNOWN_MEMBER_ID,
                                   expectedSkipAssignment: Boolean = false): Unit = {
    assertEquals(expectedError, joinGroupResult.error)
    assertEquals(expectedGeneration, joinGroupResult.generationId)
    assertEquals(expectedGroupInstanceIds.size, joinGroupResult.members.size)
    val resultedGroupInstanceIds = joinGroupResult.members.map(member => member.groupInstanceId).toSet
    assertEquals(expectedGroupInstanceIds, resultedGroupInstanceIds)
    assertGroupState(groupState = expectedGroupState)
    assertEquals(expectedProtocolType, joinGroupResult.protocolType)
    assertEquals(expectedSkipAssignment, joinGroupResult.skipAssignment)

    if (!expectedLeaderId.equals(JoinGroupRequest.UNKNOWN_MEMBER_ID)) {
      assertEquals(expectedLeaderId, joinGroupResult.leaderId)
    }
    if (!expectedMemberId.equals(JoinGroupRequest.UNKNOWN_MEMBER_ID)) {
      assertEquals(expectedMemberId, joinGroupResult.memberId)
    }
  }

  @Test
  def testHeartbeatWrongCoordinator(): Unit = {
    val heartbeatResult = heartbeat(otherGroupId, memberId, -1)
    assertEquals(Errors.NOT_COORDINATOR, heartbeatResult)
  }

  @Test
  def testHeartbeatUnknownGroup(): Unit = {
    val heartbeatResult = heartbeat(groupId, memberId, -1)
    assertEquals(Errors.UNKNOWN_MEMBER_ID, heartbeatResult)
  }

  @Test
  def testHeartbeatDeadGroup(): Unit = {
    val memberId = "memberId"

    val deadGroupId = "deadGroupId"

    groupCoordinator.groupManager.addGroup(new GroupMetadata(deadGroupId, Dead, new MockTime()))
    val heartbeatResult = heartbeat(deadGroupId, memberId, 1)
    assertEquals(Errors.COORDINATOR_NOT_AVAILABLE, heartbeatResult)
  }

  @Test
  def testHeartbeatEmptyGroup(): Unit = {
    val memberId = "memberId"

    val group = new GroupMetadata(groupId, Empty, new MockTime())
    val member = new MemberMetadata(memberId, Some(groupInstanceId),
      ClientId, ClientHost, DefaultRebalanceTimeout, DefaultSessionTimeout,
      protocolType, List(("range", Array.empty[Byte]), ("roundrobin", Array.empty[Byte])))

    group.add(member)
    groupCoordinator.groupManager.addGroup(group)
    val heartbeatResult = heartbeat(groupId, memberId, 0)
    assertEquals(Errors.UNKNOWN_MEMBER_ID, heartbeatResult)
  }

  @Test
  def testHeartbeatUnknownConsumerExistingGroup(): Unit = {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val otherMemberId = "memberId"

    val joinGroupResult = dynamicJoinGroup(groupId, memberId, protocolType, protocols)
    val assignedMemberId = joinGroupResult.memberId
    val joinGroupError = joinGroupResult.error
    assertEquals(Errors.NONE, joinGroupError)

    val syncGroupResult = syncGroupLeader(groupId, joinGroupResult.generationId, assignedMemberId, Map(assignedMemberId -> Array[Byte]()))
    assertEquals(Errors.NONE, syncGroupResult.error)

    val heartbeatResult = heartbeat(groupId, otherMemberId, 1)
    assertEquals(Errors.UNKNOWN_MEMBER_ID, heartbeatResult)
  }

  @Test
  def testHeartbeatRebalanceInProgress(): Unit = {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val joinGroupResult = dynamicJoinGroup(groupId, memberId, protocolType, protocols)
    val assignedMemberId = joinGroupResult.memberId
    val joinGroupError = joinGroupResult.error
    assertEquals(Errors.NONE, joinGroupError)

    val heartbeatResult = heartbeat(groupId, assignedMemberId, 1)
    assertEquals(Errors.NONE, heartbeatResult)
  }

  @Test
  def testHeartbeatIllegalGeneration(): Unit = {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val joinGroupResult = dynamicJoinGroup(groupId, memberId, protocolType, protocols)
    val assignedMemberId = joinGroupResult.memberId
    val joinGroupError = joinGroupResult.error
    assertEquals(Errors.NONE, joinGroupError)

    val syncGroupResult = syncGroupLeader(groupId, joinGroupResult.generationId, assignedMemberId, Map(assignedMemberId -> Array[Byte]()))
    assertEquals(Errors.NONE, syncGroupResult.error)

    val heartbeatResult = heartbeat(groupId, assignedMemberId, 2)
    assertEquals(Errors.ILLEGAL_GENERATION, heartbeatResult)
  }

  @Test
  def testValidHeartbeat(): Unit = {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val joinGroupResult = dynamicJoinGroup(groupId, memberId, protocolType, protocols)
    val assignedConsumerId = joinGroupResult.memberId
    val generationId = joinGroupResult.generationId
    val joinGroupError = joinGroupResult.error
    assertEquals(Errors.NONE, joinGroupError)

    val syncGroupResult = syncGroupLeader(groupId, generationId, assignedConsumerId, Map(assignedConsumerId -> Array[Byte]()))
    assertEquals(Errors.NONE, syncGroupResult.error)

    val heartbeatResult = heartbeat(groupId, assignedConsumerId, 1)
    assertEquals(Errors.NONE, heartbeatResult)
  }

  @Test
  def testSessionTimeout(): Unit = {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val joinGroupResult = dynamicJoinGroup(groupId, memberId, protocolType, protocols)
    val assignedConsumerId = joinGroupResult.memberId
    val generationId = joinGroupResult.generationId
    val joinGroupError = joinGroupResult.error
    assertEquals(Errors.NONE, joinGroupError)

    val syncGroupResult = syncGroupLeader(groupId, generationId, assignedConsumerId, Map(assignedConsumerId -> Array[Byte]()))
    assertEquals(Errors.NONE, syncGroupResult.error)

    when(replicaManager.getPartition(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupPartitionId)))
      .thenReturn(HostedPartition.None)
    when(replicaManager.getMagic(any[TopicPartition])).thenReturn(Some(RecordBatch.MAGIC_VALUE_V1))

    timer.advanceClock(DefaultSessionTimeout + 100)

    val heartbeatResult = heartbeat(groupId, assignedConsumerId, 1)
    assertEquals(Errors.UNKNOWN_MEMBER_ID, heartbeatResult)
  }

  @Test
  def testHeartbeatMaintainsSession(): Unit = {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val sessionTimeout = 1000

    val joinGroupResult = dynamicJoinGroup(groupId, memberId, protocolType, protocols,
      rebalanceTimeout = sessionTimeout, sessionTimeout = sessionTimeout)
    val assignedConsumerId = joinGroupResult.memberId
    val generationId = joinGroupResult.generationId
    val joinGroupError = joinGroupResult.error
    assertEquals(Errors.NONE, joinGroupError)

    val syncGroupResult = syncGroupLeader(groupId, generationId, assignedConsumerId, Map(assignedConsumerId -> Array[Byte]()))
    assertEquals(Errors.NONE, syncGroupResult.error)

    timer.advanceClock(sessionTimeout / 2)

    var heartbeatResult = heartbeat(groupId, assignedConsumerId, 1)
    assertEquals(Errors.NONE, heartbeatResult)

    timer.advanceClock(sessionTimeout / 2 + 100)

    heartbeatResult = heartbeat(groupId, assignedConsumerId, 1)
    assertEquals(Errors.NONE, heartbeatResult)
  }

  @Test
  def testCommitMaintainsSession(): Unit = {
    val sessionTimeout = 1000
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val tip = new TopicIdPartition(Uuid.randomUuid(), 0, "topic")
    val offset = offsetAndMetadata(0)

    val joinGroupResult = dynamicJoinGroup(groupId, memberId, protocolType, protocols,
      rebalanceTimeout = sessionTimeout, sessionTimeout = sessionTimeout)
    val assignedMemberId = joinGroupResult.memberId
    val generationId = joinGroupResult.generationId
    val joinGroupError = joinGroupResult.error
    assertEquals(Errors.NONE, joinGroupError)

    val syncGroupResult = syncGroupLeader(groupId, generationId, assignedMemberId, Map(assignedMemberId -> Array[Byte]()))
    assertEquals(Errors.NONE, syncGroupResult.error)

    timer.advanceClock(sessionTimeout / 2)

    val commitOffsetResult = commitOffsets(groupId, assignedMemberId, generationId, Map(tip -> offset))
    assertEquals(Map(tip -> Errors.NONE), commitOffsetResult)

    timer.advanceClock(sessionTimeout / 2 + 100)

    val heartbeatResult = heartbeat(groupId, assignedMemberId, 1)
    assertEquals(Errors.NONE, heartbeatResult)
  }

  @Test
  def testSessionTimeoutDuringRebalance(): Unit = {
    // create a group with a single member
    val firstJoinResult = dynamicJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols,
      rebalanceTimeout = 2000, sessionTimeout = 1000)
    val firstMemberId = firstJoinResult.memberId
    val firstGenerationId = firstJoinResult.generationId
    assertEquals(firstMemberId, firstJoinResult.leaderId)
    assertEquals(Errors.NONE, firstJoinResult.error)

    val firstSyncResult = syncGroupLeader(groupId, firstGenerationId, firstMemberId, Map(firstMemberId -> Array[Byte]()))
    assertEquals(Errors.NONE, firstSyncResult.error)

    // now have a new member join to trigger a rebalance
    val otherJoinFuture = sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols)

    timer.advanceClock(500)

    var heartbeatResult = heartbeat(groupId, firstMemberId, firstGenerationId)
    assertEquals(Errors.REBALANCE_IN_PROGRESS, heartbeatResult)

    // letting the session expire should make the member fall out of the group
    timer.advanceClock(1100)

    heartbeatResult = heartbeat(groupId, firstMemberId, firstGenerationId)
    assertEquals(Errors.UNKNOWN_MEMBER_ID, heartbeatResult)

    // and the rebalance should complete with only the new member
    val otherJoinResult = await(otherJoinFuture, DefaultSessionTimeout+100)
    assertEquals(Errors.NONE, otherJoinResult.error)
  }

  @Test
  def testRebalanceCompletesBeforeMemberJoins(): Unit = {
    // create a group with a single member
    val firstJoinResult = staticJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, leaderInstanceId, protocolType, protocols,
      rebalanceTimeout = 1200, sessionTimeout = 1000)
    val firstMemberId = firstJoinResult.memberId
    val firstGenerationId = firstJoinResult.generationId
    assertEquals(firstMemberId, firstJoinResult.leaderId)
    assertEquals(Errors.NONE, firstJoinResult.error)

    val firstSyncResult = syncGroupLeader(groupId, firstGenerationId, firstMemberId, Map(firstMemberId -> Array[Byte]()))
    assertEquals(Errors.NONE, firstSyncResult.error)

    // now have a new member join to trigger a rebalance
    val otherMemberSessionTimeout = DefaultSessionTimeout
    val otherJoinFuture = sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols)

    // send a couple heartbeats to keep the member alive while the rebalance finishes
    var expectedResultList = List(Errors.REBALANCE_IN_PROGRESS, Errors.REBALANCE_IN_PROGRESS)
    for (expectedResult <- expectedResultList) {
      timer.advanceClock(otherMemberSessionTimeout)
      val heartbeatResult = heartbeat(groupId, firstMemberId, firstGenerationId)
      assertEquals(expectedResult, heartbeatResult)
    }

    // now timeout the rebalance
    timer.advanceClock(otherMemberSessionTimeout)
    val otherJoinResult = await(otherJoinFuture, otherMemberSessionTimeout+100)
    val otherMemberId = otherJoinResult.memberId
    val otherGenerationId = otherJoinResult.generationId
    val syncResult = syncGroupLeader(groupId, otherGenerationId, otherMemberId, Map(otherMemberId -> Array[Byte]()))
    assertEquals(Errors.NONE, syncResult.error)

    // the unjoined static member should be remained in the group before session timeout.
    assertEquals(Errors.NONE, otherJoinResult.error)
    var heartbeatResult = heartbeat(groupId, firstMemberId, firstGenerationId)
    assertEquals(Errors.ILLEGAL_GENERATION, heartbeatResult)

    expectedResultList = List(Errors.NONE, Errors.NONE, Errors.REBALANCE_IN_PROGRESS)

    // now session timeout the unjoined member. Still keeping the new member.
    for (expectedResult <- expectedResultList) {
      timer.advanceClock(otherMemberSessionTimeout)
      heartbeatResult = heartbeat(groupId, otherMemberId, otherGenerationId)
      assertEquals(expectedResult, heartbeatResult)
    }

    val otherRejoinGroupFuture = sendJoinGroup(groupId, otherMemberId, protocolType, protocols)
    val otherReJoinResult = await(otherRejoinGroupFuture, otherMemberSessionTimeout+100)
    assertEquals(Errors.NONE, otherReJoinResult.error)

    val otherRejoinGenerationId = otherReJoinResult.generationId
    val reSyncResult = syncGroupLeader(groupId, otherRejoinGenerationId, otherMemberId, Map(otherMemberId -> Array[Byte]()))
    assertEquals(Errors.NONE, reSyncResult.error)

    // the joined member should get heart beat response with no error. Let the new member keep heartbeating for a while
    // to verify that no new rebalance is triggered unexpectedly
    for ( _ <-  1 to 20) {
      timer.advanceClock(500)
      heartbeatResult = heartbeat(groupId, otherMemberId, otherRejoinGenerationId)
      assertEquals(Errors.NONE, heartbeatResult)
    }
  }

  @Test
  def testSyncGroupEmptyAssignment(): Unit = {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val joinGroupResult = dynamicJoinGroup(groupId, memberId, protocolType, protocols)
    val assignedConsumerId = joinGroupResult.memberId
    val generationId = joinGroupResult.generationId
    val joinGroupError = joinGroupResult.error
    assertEquals(Errors.NONE, joinGroupError)

    val syncGroupResult = syncGroupLeader(groupId, generationId, assignedConsumerId, Map())
    assertEquals(Errors.NONE, syncGroupResult.error)
    assertTrue(syncGroupResult.memberAssignment.isEmpty)

    val heartbeatResult = heartbeat(groupId, assignedConsumerId, 1)
    assertEquals(Errors.NONE, heartbeatResult)
  }

  @Test
  def testSyncGroupNotCoordinator(): Unit = {
    val generation = 1

    val syncGroupResult = syncGroupFollower(otherGroupId, generation, memberId)
    assertEquals(Errors.NOT_COORDINATOR, syncGroupResult.error)
  }

  @Test
  def testSyncGroupFromUnknownGroup(): Unit = {
    val syncGroupResult = syncGroupFollower(groupId, 1, memberId)
    assertEquals(Errors.UNKNOWN_MEMBER_ID, syncGroupResult.error)
  }

  @Test
  def testSyncGroupFromUnknownMember(): Unit = {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val joinGroupResult = dynamicJoinGroup(groupId, memberId, protocolType, protocols)
    val assignedConsumerId = joinGroupResult.memberId
    val generationId = joinGroupResult.generationId
    assertEquals(Errors.NONE, joinGroupResult.error)

    val syncGroupResult = syncGroupLeader(groupId, generationId, assignedConsumerId, Map(assignedConsumerId -> Array[Byte]()))
    val syncGroupError = syncGroupResult.error
    assertEquals(Errors.NONE, syncGroupError)

    val unknownMemberId = "blah"
    val unknownMemberSyncResult = syncGroupFollower(groupId, generationId, unknownMemberId)
    assertEquals(Errors.UNKNOWN_MEMBER_ID, unknownMemberSyncResult.error)
  }

  @Test
  def testSyncGroupFromIllegalGeneration(): Unit = {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val joinGroupResult = dynamicJoinGroup(groupId, memberId, protocolType, protocols)
    val assignedConsumerId = joinGroupResult.memberId
    val generationId = joinGroupResult.generationId
    assertEquals(Errors.NONE, joinGroupResult.error)

    // send the sync group with an invalid generation
    val syncGroupResult = syncGroupLeader(groupId, generationId+1, assignedConsumerId, Map(assignedConsumerId -> Array[Byte]()))
    assertEquals(Errors.ILLEGAL_GENERATION, syncGroupResult.error)
  }

  @Test
  def testJoinGroupFromUnchangedFollowerDoesNotRebalance(): Unit = {
    // to get a group of two members:
    // 1. join and sync with a single member (because we can't immediately join with two members)
    // 2. join and sync with the first member and a new member

    val firstJoinResult = dynamicJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols)
    val firstMemberId = firstJoinResult.memberId
    val firstGenerationId = firstJoinResult.generationId
    assertEquals(firstMemberId, firstJoinResult.leaderId)
    assertEquals(Errors.NONE, firstJoinResult.error)

    val firstSyncResult = syncGroupLeader(groupId, firstGenerationId, firstMemberId, Map(firstMemberId -> Array[Byte]()))
    assertEquals(Errors.NONE, firstSyncResult.error)

    val otherJoinFuture = sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols)

    val joinFuture = sendJoinGroup(groupId, firstMemberId, protocolType, protocols)

    val joinResult = await(joinFuture, DefaultSessionTimeout+100)
    val otherJoinResult = await(otherJoinFuture, DefaultSessionTimeout+100)
    assertEquals(Errors.NONE, joinResult.error)
    assertEquals(Errors.NONE, otherJoinResult.error)
    assertTrue(joinResult.generationId == otherJoinResult.generationId)

    assertEquals(firstMemberId, joinResult.leaderId)
    assertEquals(firstMemberId, otherJoinResult.leaderId)

    val nextGenerationId = joinResult.generationId

    // this shouldn't cause a rebalance since protocol information hasn't changed
    val followerJoinResult = await(sendJoinGroup(groupId, otherJoinResult.memberId, protocolType, protocols), 1)

    assertEquals(Errors.NONE, followerJoinResult.error)
    assertEquals(nextGenerationId, followerJoinResult.generationId)
  }

  @Test
  def testJoinGroupFromUnchangedLeaderShouldRebalance(): Unit = {
    val firstJoinResult = dynamicJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols)
    val firstMemberId = firstJoinResult.memberId
    val firstGenerationId = firstJoinResult.generationId
    assertEquals(firstMemberId, firstJoinResult.leaderId)
    assertEquals(Errors.NONE, firstJoinResult.error)

    val firstSyncResult = syncGroupLeader(groupId, firstGenerationId, firstMemberId, Map(firstMemberId -> Array[Byte]()))
    assertEquals(Errors.NONE, firstSyncResult.error)

    // join groups from the leader should force the group to rebalance, which allows the
    // leader to push new assignments when local metadata changes

    val secondJoinResult = await(sendJoinGroup(groupId, firstMemberId, protocolType, protocols), 1)

    assertEquals(Errors.NONE, secondJoinResult.error)
    assertNotEquals(firstGenerationId, secondJoinResult.generationId)
  }

  /**
    * Test if the following scenario completes a rebalance correctly: A new member starts a JoinGroup request with
    * an UNKNOWN_MEMBER_ID, attempting to join a stable group. But never initiates the second JoinGroup request with
    * the provided member ID and times out. The test checks if original member remains the sole member in this group,
    * which should remain stable throughout this test.
    */
  @Test
  def testSecondMemberPartiallyJoinAndTimeout(): Unit = {
    val firstJoinResult = dynamicJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols)
    val firstMemberId = firstJoinResult.memberId
    val firstGenerationId = firstJoinResult.generationId
    assertEquals(firstMemberId, firstJoinResult.leaderId)
    assertEquals(Errors.NONE, firstJoinResult.error)

    // Starting sync group leader
    val firstSyncResult = syncGroupLeader(groupId, firstGenerationId, firstMemberId, Map(firstMemberId -> Array[Byte]()))
    assertEquals(Errors.NONE, firstSyncResult.error)
    timer.advanceClock(100)
    assertEquals(Set(firstMemberId), groupCoordinator.groupManager.getGroup(groupId).get.allMembers)
    assertEquals(groupCoordinator.groupManager.getGroup(groupId).get.allMembers,
      groupCoordinator.groupManager.getGroup(groupId).get.allDynamicMembers)
    assertEquals(0, groupCoordinator.groupManager.getGroup(groupId).get.numPending)
    val group = groupCoordinator.groupManager.getGroup(groupId).get

    // ensure the group is stable before a new member initiates join request
    assertEquals(Stable, group.currentState)

    // new member initiates join group
    val secondJoinResult = joinGroupPartial(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols)
    assertEquals(Errors.MEMBER_ID_REQUIRED, secondJoinResult.error)
    assertEquals(1, group.numPending)
    assertEquals(Stable, group.currentState)

    when(replicaManager.getMagic(any[TopicPartition])).thenReturn(Some(RecordBatch.MAGIC_VALUE_V1))

    // advance clock to timeout the pending member
    assertEquals(Set(firstMemberId), group.allMembers)
    assertEquals(1, group.numPending)
    timer.advanceClock(300)

    // original (firstMember) member sends heartbeats to prevent session timeouts.
    val heartbeatResult = heartbeat(groupId, firstMemberId, 1)
    assertEquals(Errors.NONE, heartbeatResult)

    // timeout the pending member
    timer.advanceClock(300)

    // at this point the second member should have been removed from pending list (session timeout),
    // and the group should be in Stable state with only the first member in it.
    assertEquals(Set(firstMemberId), group.allMembers)
    assertEquals(0, group.numPending)
    assertEquals(Stable, group.currentState)
    assertTrue(group.has(firstMemberId))
  }

  /**
    * Create a group with two members in Stable state. Create a third pending member by completing it's first JoinGroup
    * request without a member id.
    */
  private def setupGroupWithPendingMember(): JoinGroupResult = {
    // add the first member
    val joinResult1 = dynamicJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols)
    assertGroupState(groupState = CompletingRebalance)

    // now the group is stable, with the one member that joined above
    val firstSyncResult = syncGroupLeader(groupId, joinResult1.generationId, joinResult1.memberId, Map(joinResult1.memberId -> Array[Byte]()))
    assertEquals(Errors.NONE, firstSyncResult.error)
    assertGroupState(groupState = Stable)

    // start the join for the second member
    val secondJoinFuture = sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols)

    // rejoin the first member back into the group
    val firstJoinFuture = sendJoinGroup(groupId, joinResult1.memberId, protocolType, protocols)
    val firstMemberJoinResult = await(firstJoinFuture, DefaultSessionTimeout+100)
    val secondMemberJoinResult = await(secondJoinFuture, DefaultSessionTimeout+100)
    assertGroupState(groupState = CompletingRebalance)

    // stabilize the group
    val secondSyncResult = syncGroupLeader(groupId, firstMemberJoinResult.generationId, joinResult1.memberId, Map(joinResult1.memberId -> Array[Byte]()))
    assertEquals(Errors.NONE, secondSyncResult.error)
    assertGroupState(groupState = Stable)

    // re-join an existing member, to transition the group to PreparingRebalance state.
    sendJoinGroup(groupId, firstMemberJoinResult.memberId, protocolType, protocols)
    assertGroupState(groupState = PreparingRebalance)

    // create a pending member in the group
    val pendingMember = joinGroupPartial(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols, sessionTimeout=100)
    assertEquals(1, groupCoordinator.groupManager.getGroup(groupId).get.numPending)

    // re-join the second existing member
    sendJoinGroup(groupId, secondMemberJoinResult.memberId, protocolType, protocols)
    assertGroupState(groupState = PreparingRebalance)
    assertEquals(1, groupCoordinator.groupManager.getGroup(groupId).get.numPending)

    pendingMember
  }

  /**
    * Setup a group in with a pending member. The test checks if the a pending member joining completes the rebalancing
    * operation
    */
  @Test
  def testJoinGroupCompletionWhenPendingMemberJoins(): Unit = {
    val pendingMember = setupGroupWithPendingMember()

    // compete join group for the pending member
    val pendingMemberJoinFuture = sendJoinGroup(groupId, pendingMember.memberId, protocolType, protocols)
    await(pendingMemberJoinFuture, DefaultSessionTimeout+100)

    assertGroupState(groupState = CompletingRebalance)
    assertEquals(3, group().allMembers.size)
    assertEquals(0, group().numPending)
  }

  /**
    * Setup a group in with a pending member. The test checks if the timeout of the pending member will
    * cause the group to return to a CompletingRebalance state.
    */
  @Test
  def testJoinGroupCompletionWhenPendingMemberTimesOut(): Unit = {
    setupGroupWithPendingMember()

    // Advancing Clock by > 100 (session timeout for third and fourth member)
    // and < 500 (for first and second members). This will force the coordinator to attempt join
    // completion on heartbeat expiration (since we are in PendingRebalance stage).
    when(replicaManager.getMagic(any[TopicPartition])).thenReturn(Some(RecordBatch.MAGIC_VALUE_V1))
    timer.advanceClock(120)

    assertGroupState(groupState = CompletingRebalance)
    assertEquals(2, group().allMembers.size)
    assertEquals(0, group().numPending)
  }

  @Test
  def testPendingMembersLeavesGroup(): Unit = {
    val pending = setupGroupWithPendingMember()

    val leaveGroupResults = singleLeaveGroup(groupId, pending.memberId)
    verifyLeaveGroupResult(leaveGroupResults)

    assertGroupState(groupState = CompletingRebalance)
    assertEquals(2, group().allMembers.size)
    assertEquals(2, group().allDynamicMembers.size)
    assertEquals(0, group().numPending)
  }

  private def verifyHeartbeat(
    joinGroupResult: JoinGroupResult,
    expectedError: Errors
  ): Unit = {
    val heartbeatResult = heartbeat(
      groupId,
      joinGroupResult.memberId,
      joinGroupResult.generationId
    )
    assertEquals(expectedError, heartbeatResult)
  }

  private def joinWithNMembers(nbMembers: Int): Seq[JoinGroupResult] = {
    val requiredKnownMemberId = true

    // First JoinRequests
    var futures = 1.to(nbMembers).map { _ =>
      sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols,
        None, DefaultSessionTimeout, DefaultRebalanceTimeout, requiredKnownMemberId)
    }

    // Get back the assigned member ids
    val memberIds = futures.map(await(_, 1).memberId)

    // Second JoinRequests
    futures = memberIds.map { memberId =>
      sendJoinGroup(groupId, memberId, protocolType, protocols,
        None, DefaultSessionTimeout, DefaultRebalanceTimeout, requiredKnownMemberId)
    }

    timer.advanceClock(GroupInitialRebalanceDelay + 1)
    timer.advanceClock(DefaultRebalanceTimeout + 1)

    futures.map(await(_, 1))
  }

  @Test
  def testRebalanceTimesOutWhenSyncRequestIsNotReceived(): Unit = {
    // This test case ensure that the DelayedSync does kick out all members
    // if they don't sent a sync request before the rebalance timeout. The
    // group is in the Stable state in this case.
    val results = joinWithNMembers(nbMembers = 3)
    assertEquals(Set(Errors.NONE), results.map(_.error).toSet)

    // Advance time
    timer.advanceClock(DefaultRebalanceTimeout / 2)

    // Heartbeats to ensure that heartbeating does not interfere with the
    // delayed sync operation.
    results.foreach { joinGroupResult =>
      verifyHeartbeat(joinGroupResult, Errors.NONE)
    }

    // Advance part the rebalance timeout to trigger the delayed operation.
    when(replicaManager.getMagic(any[TopicPartition]))
      .thenReturn(Some(RecordBatch.MAGIC_VALUE_V1))

    timer.advanceClock(DefaultRebalanceTimeout / 2 + 1)

    // Heartbeats fail because none of the members have sent the sync request
    results.foreach { joinGroupResult =>
      verifyHeartbeat(joinGroupResult, Errors.UNKNOWN_MEMBER_ID)
    }
  }

  @Test
  def testRebalanceTimesOutWhenSyncRequestIsNotReceivedFromFollowers(): Unit = {
    // This test case ensure that the DelayedSync does kick out the followers
    // if they don't sent a sync request before the rebalance timeout. The
    // group is in the Stable state in this case.
    val results = joinWithNMembers(nbMembers = 3)
    assertEquals(Set(Errors.NONE), results.map(_.error).toSet)

    // Advance time
    timer.advanceClock(DefaultRebalanceTimeout / 2)

    // Heartbeats to ensure that heartbeating does not interfere with the
    // delayed sync operation.
    results.foreach { joinGroupResult =>
      verifyHeartbeat(joinGroupResult, Errors.NONE)
    }

    // Leader sends Sync
    val assignments = results.map(result => result.memberId -> Array.empty[Byte]).toMap
    val leaderResult = sendSyncGroupLeader(groupId, results.head.generationId, results.head.memberId,
      Some(protocolType), Some(protocolName), None, assignments)

    assertEquals(Errors.NONE, await(leaderResult, 1).error)

    // Leader should be able to heartbeat
    verifyHeartbeat(results.head, Errors.NONE)

    // Advance part the rebalance timeout to trigger the delayed operation.
    timer.advanceClock(DefaultRebalanceTimeout / 2 + 1)

    // Leader should be able to heartbeat
    verifyHeartbeat(results.head, Errors.REBALANCE_IN_PROGRESS)

    // Followers should have been removed.
    results.tail.foreach { joinGroupResult =>
      verifyHeartbeat(joinGroupResult, Errors.UNKNOWN_MEMBER_ID)
    }
  }

  @Test
  def testRebalanceTimesOutWhenSyncRequestIsNotReceivedFromLeaders(): Unit = {
    // This test case ensure that the DelayedSync does kick out the leader
    // if it does not sent a sync request before the rebalance timeout. The
    // group is in the CompletingRebalance state in this case.
    val results = joinWithNMembers(nbMembers = 3)
    assertEquals(Set(Errors.NONE), results.map(_.error).toSet)

    // Advance time
    timer.advanceClock(DefaultRebalanceTimeout / 2)

    // Heartbeats to ensure that heartbeating does not interfere with the
    // delayed sync operation.
    results.foreach { joinGroupResult =>
      verifyHeartbeat(joinGroupResult, Errors.NONE)
    }

    // Followers send Sync
    val followerResults = results.tail.map { joinGroupResult =>
      sendSyncGroupFollower(groupId, joinGroupResult.generationId, joinGroupResult.memberId,
        Some(protocolType), Some(protocolName), None)
    }

    // Advance part the rebalance timeout to trigger the delayed operation.
    timer.advanceClock(DefaultRebalanceTimeout / 2 + 1)

    val followerErrors = followerResults.map(await(_, 1).error)
    assertEquals(Set(Errors.REBALANCE_IN_PROGRESS), followerErrors.toSet)

    // Leader should have been removed.
    verifyHeartbeat(results.head, Errors.UNKNOWN_MEMBER_ID)

    // Followers should be able to heartbeat.
    results.tail.foreach { joinGroupResult =>
      verifyHeartbeat(joinGroupResult, Errors.REBALANCE_IN_PROGRESS)
    }
  }

  @Test
  def testRebalanceDoesNotTimeOutWhenAllSyncAreReceived(): Unit = {
    // This test case ensure that the DelayedSync does not kick any
    // members out when they have all sent their sync requests.
    val results = joinWithNMembers(nbMembers = 3)
    assertEquals(Set(Errors.NONE), results.map(_.error).toSet)

    // Advance time
    timer.advanceClock(DefaultRebalanceTimeout / 2)

    // Heartbeats to ensure that heartbeating does not interfere with the
    // delayed sync operation.
    results.foreach { joinGroupResult =>
      verifyHeartbeat(joinGroupResult, Errors.NONE)
    }

    val assignments = results.map(result => result.memberId -> Array.empty[Byte]).toMap
    val leaderResult = sendSyncGroupLeader(groupId, results.head.generationId, results.head.memberId,
      Some(protocolType), Some(protocolName), None, assignments)

    assertEquals(Errors.NONE, await(leaderResult, 1).error)

    // Followers send Sync
    val followerResults = results.tail.map { joinGroupResult =>
      sendSyncGroupFollower(groupId, joinGroupResult.generationId, joinGroupResult.memberId,
        Some(protocolType), Some(protocolName), None)
    }

    val followerErrors = followerResults.map(await(_, 1).error)
    assertEquals(Set(Errors.NONE), followerErrors.toSet)

    // Advance past the rebalance timeout to expire the Sync timeout. All
    // members should remain and the group should not rebalance.
    timer.advanceClock(DefaultRebalanceTimeout / 2 + 1)

    // Followers should be able to heartbeat.
    results.foreach { joinGroupResult =>
      verifyHeartbeat(joinGroupResult, Errors.NONE)
    }

    // Advance a bit more.
    timer.advanceClock(DefaultRebalanceTimeout / 2)

    // Followers should be able to heartbeat.
    results.foreach { joinGroupResult =>
      verifyHeartbeat(joinGroupResult, Errors.NONE)
    }
  }

  private def group(groupId: String = groupId) = {
    groupCoordinator.groupManager.getGroup(groupId) match {
      case Some(g) => g
      case None => null
    }
  }

  private def assertGroupState(groupId: String = groupId,
                               groupState: GroupState): Unit = {
    groupCoordinator.groupManager.getGroup(groupId) match {
      case Some(group) => assertEquals(groupState, group.currentState)
      case None => fail(s"Group $groupId not found in coordinator")
    }
  }

  private def joinGroupPartial(groupId: String,
                               memberId: String,
                               protocolType: String,
                               protocols: List[(String, Array[Byte])],
                               sessionTimeout: Int = DefaultSessionTimeout,
                               rebalanceTimeout: Int = DefaultRebalanceTimeout): JoinGroupResult = {
    val requireKnownMemberId = true
    val responseFuture = sendJoinGroup(groupId, memberId, protocolType, protocols, None, sessionTimeout, rebalanceTimeout, requireKnownMemberId)
    Await.result(responseFuture, Duration(rebalanceTimeout + 100, TimeUnit.MILLISECONDS))
  }

  @Test
  def testLeaderFailureInSyncGroup(): Unit = {
    // to get a group of two members:
    // 1. join and sync with a single member (because we can't immediately join with two members)
    // 2. join and sync with the first member and a new member

    val firstJoinResult = dynamicJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols)
    val firstMemberId = firstJoinResult.memberId
    val firstGenerationId = firstJoinResult.generationId
    assertEquals(firstMemberId, firstJoinResult.leaderId)
    assertEquals(Errors.NONE, firstJoinResult.error)

    val firstSyncResult = syncGroupLeader(groupId, firstGenerationId, firstMemberId, Map(firstMemberId -> Array[Byte]()))
    assertEquals(Errors.NONE, firstSyncResult.error)

    val otherJoinFuture = sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols)

    val joinFuture = sendJoinGroup(groupId, firstMemberId, protocolType, protocols)

    val joinResult = await(joinFuture, DefaultSessionTimeout+100)
    val otherJoinResult = await(otherJoinFuture, DefaultSessionTimeout+100)
    assertEquals(Errors.NONE, joinResult.error)
    assertEquals(Errors.NONE, otherJoinResult.error)
    assertTrue(joinResult.generationId == otherJoinResult.generationId)

    assertEquals(firstMemberId, joinResult.leaderId)
    assertEquals(firstMemberId, otherJoinResult.leaderId)

    val nextGenerationId = joinResult.generationId

    // with no leader SyncGroup, the follower's request should fail with an error indicating
    // that it should rejoin
    val followerSyncFuture = sendSyncGroupFollower(groupId, nextGenerationId, otherJoinResult.memberId, None, None, None)

    timer.advanceClock(DefaultSessionTimeout + 100)

    val followerSyncResult = await(followerSyncFuture, DefaultSessionTimeout+100)
    assertEquals(Errors.REBALANCE_IN_PROGRESS, followerSyncResult.error)
  }

  @Test
  def testSyncGroupFollowerAfterLeader(): Unit = {
    // to get a group of two members:
    // 1. join and sync with a single member (because we can't immediately join with two members)
    // 2. join and sync with the first member and a new member

    val firstJoinResult = dynamicJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols)
    val firstMemberId = firstJoinResult.memberId
    val firstGenerationId = firstJoinResult.generationId
    assertEquals(firstMemberId, firstJoinResult.leaderId)
    assertEquals(Errors.NONE, firstJoinResult.error)

    val firstSyncResult = syncGroupLeader(groupId, firstGenerationId, firstMemberId, Map(firstMemberId -> Array[Byte]()))
    assertEquals(Errors.NONE, firstSyncResult.error)

    val otherJoinFuture = sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols)

    val joinFuture = sendJoinGroup(groupId, firstMemberId, protocolType, protocols)

    val joinResult = await(joinFuture, DefaultSessionTimeout+100)
    val otherJoinResult = await(otherJoinFuture, DefaultSessionTimeout+100)
    assertEquals(Errors.NONE, joinResult.error)
    assertEquals(Errors.NONE, otherJoinResult.error)
    assertTrue(joinResult.generationId == otherJoinResult.generationId)

    assertEquals(firstMemberId, joinResult.leaderId)
    assertEquals(firstMemberId, otherJoinResult.leaderId)

    val nextGenerationId = joinResult.generationId
    val leaderId = firstMemberId
    val leaderAssignment = Array[Byte](0)
    val followerId = otherJoinResult.memberId
    val followerAssignment = Array[Byte](1)

    val leaderSyncResult = syncGroupLeader(groupId, nextGenerationId, leaderId,
      Map(leaderId -> leaderAssignment, followerId -> followerAssignment))
    assertEquals(Errors.NONE, leaderSyncResult.error)
    assertEquals(leaderAssignment, leaderSyncResult.memberAssignment)

    val followerSyncResult = syncGroupFollower(groupId, nextGenerationId, otherJoinResult.memberId)
    assertEquals(Errors.NONE, followerSyncResult.error)
    assertEquals(followerAssignment, followerSyncResult.memberAssignment)
  }

  @Test
  def testSyncGroupLeaderAfterFollower(): Unit = {
    // to get a group of two members:
    // 1. join and sync with a single member (because we can't immediately join with two members)
    // 2. join and sync with the first member and a new member

    val joinGroupResult = dynamicJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols)
    val firstMemberId = joinGroupResult.memberId
    val firstGenerationId = joinGroupResult.generationId
    assertEquals(firstMemberId, joinGroupResult.leaderId)
    assertEquals(Errors.NONE, joinGroupResult.error)

    val syncGroupResult = syncGroupLeader(groupId, firstGenerationId, firstMemberId, Map(firstMemberId -> Array[Byte]()))
    assertEquals(Errors.NONE, syncGroupResult.error)

    val otherJoinFuture = sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols)

    val joinFuture = sendJoinGroup(groupId, firstMemberId, protocolType, protocols)

    val joinResult = await(joinFuture, DefaultSessionTimeout+100)
    val otherJoinResult = await(otherJoinFuture, DefaultSessionTimeout+100)
    assertEquals(Errors.NONE, joinResult.error)
    assertEquals(Errors.NONE, otherJoinResult.error)
    assertTrue(joinResult.generationId == otherJoinResult.generationId)

    val nextGenerationId = joinResult.generationId
    val leaderId = joinResult.leaderId
    val leaderAssignment = Array[Byte](0)
    val followerId = otherJoinResult.memberId
    val followerAssignment = Array[Byte](1)

    assertEquals(firstMemberId, joinResult.leaderId)
    assertEquals(firstMemberId, otherJoinResult.leaderId)

    val followerSyncFuture = sendSyncGroupFollower(groupId, nextGenerationId, followerId, None, None, None)

    val leaderSyncResult = syncGroupLeader(groupId, nextGenerationId, leaderId,
      Map(leaderId -> leaderAssignment, followerId -> followerAssignment))
    assertEquals(Errors.NONE, leaderSyncResult.error)
    assertEquals(leaderAssignment, leaderSyncResult.memberAssignment)

    val followerSyncResult = await(followerSyncFuture, DefaultSessionTimeout+100)
    assertEquals(Errors.NONE, followerSyncResult.error)
    assertEquals(followerAssignment, followerSyncResult.memberAssignment)
  }

  @Test
  def testCommitOffsetFromUnknownGroup(): Unit = {
    val generationId = 1
    val tip = new TopicIdPartition(Uuid.randomUuid(), 0, "topic")
    val offset = offsetAndMetadata(0)

    val commitOffsetResult = commitOffsets(groupId, memberId, generationId, Map(tip -> offset))
    assertEquals(Map(tip -> Errors.ILLEGAL_GENERATION), commitOffsetResult)
  }

  @Test
  def testCommitOffsetWithDefaultGeneration(): Unit = {
    val tip = new TopicIdPartition(Uuid.randomUuid(), 0, "topic")
    val offset = offsetAndMetadata(0)

    val commitOffsetResult = commitOffsets(groupId, OffsetCommitRequest.DEFAULT_MEMBER_ID,
      OffsetCommitRequest.DEFAULT_GENERATION_ID, Map(tip -> offset))
    assertEquals(Map(tip -> Errors.NONE), commitOffsetResult)
  }

  @Test
  def testCommitOffsetsAfterGroupIsEmpty(): Unit = {
    // Tests the scenario where the reset offset tool modifies the offsets
    // of a group after it becomes empty

    // A group member joins
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val joinGroupResult = dynamicJoinGroup(groupId, memberId, protocolType, protocols)
    val assignedMemberId = joinGroupResult.memberId
    val joinGroupError = joinGroupResult.error
    assertEquals(Errors.NONE, joinGroupError)

    // and leaves.
    val leaveGroupResults = singleLeaveGroup(groupId, assignedMemberId)
    verifyLeaveGroupResult(leaveGroupResults)

    // The simple offset commit should now fail
    val tip = new TopicIdPartition(Uuid.randomUuid(), 0, "topic")
    val offset = offsetAndMetadata(0)
    val commitOffsetResult = commitOffsets(groupId, OffsetCommitRequest.DEFAULT_MEMBER_ID,
      OffsetCommitRequest.DEFAULT_GENERATION_ID, Map(tip -> offset))
    assertEquals(Map(tip -> Errors.NONE), commitOffsetResult)

    val (error, partitionData) = groupCoordinator.handleFetchOffsets(groupId, requireStable, Some(Seq(tip.topicPartition)))
    assertEquals(Errors.NONE, error)
    assertEquals(Some(0), partitionData.get(tip.topicPartition).map(_.offset))
  }

  @Test
  def testFetchOffsets(): Unit = {
    val tip = new TopicIdPartition(Uuid.randomUuid(), 0, "topic")
    val offset = 97L
    val metadata = "some metadata"
    val leaderEpoch = Optional.of[Integer](15)
    val offsetAndMetadata = OffsetAndMetadata(offset, leaderEpoch, metadata, timer.time.milliseconds(), None)

    val commitOffsetResult = commitOffsets(groupId, OffsetCommitRequest.DEFAULT_MEMBER_ID,
      OffsetCommitRequest.DEFAULT_GENERATION_ID, Map(tip -> offsetAndMetadata))
    assertEquals(Map(tip -> Errors.NONE), commitOffsetResult)

    val (error, partitionData) = groupCoordinator.handleFetchOffsets(groupId, requireStable, Some(Seq(tip.topicPartition)))
    assertEquals(Errors.NONE, error)

    val maybePartitionData = partitionData.get(tip.topicPartition)
    assertTrue(maybePartitionData.isDefined)
    assertEquals(offset, maybePartitionData.get.offset)
    assertEquals(metadata, maybePartitionData.get.metadata)
    assertEquals(leaderEpoch, maybePartitionData.get.leaderEpoch)
  }

  @Test
  def testCommitAndFetchOffsetsWithEmptyGroup(): Unit = {
    // For backwards compatibility, the coordinator supports committing/fetching offsets with an empty groupId.
    // To allow inspection and removal of the empty group, we must also support DescribeGroups and DeleteGroups

    val tip = new TopicIdPartition(Uuid.randomUuid(), 0, "topic")
    val offset = offsetAndMetadata(0)
    val groupId = ""

    val commitOffsetResult = commitOffsets(groupId, OffsetCommitRequest.DEFAULT_MEMBER_ID,
      OffsetCommitRequest.DEFAULT_GENERATION_ID, Map(tip -> offset))
    assertEquals(Map(tip -> Errors.NONE), commitOffsetResult)

    val (fetchError, partitionData) = groupCoordinator.handleFetchOffsets(groupId, requireStable, Some(Seq(tip.topicPartition)))
    assertEquals(Errors.NONE, fetchError)
    assertEquals(Some(0), partitionData.get(tip.topicPartition).map(_.offset))

    val (describeError, summary) = groupCoordinator.handleDescribeGroup(groupId)
    assertEquals(Errors.NONE, describeError)
    assertEquals(Empty.toString, summary.state)

    val groupTopicPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupPartitionId)
    val partition: Partition = mock(classOf[Partition])

    when(replicaManager.getMagic(any[TopicPartition])).thenReturn(Some(RecordBatch.CURRENT_MAGIC_VALUE))
    when(replicaManager.getPartition(groupTopicPartition)).thenReturn(HostedPartition.Online(partition))
    when(replicaManager.onlinePartition(groupTopicPartition)).thenReturn(Some(partition))

    val deleteErrors = groupCoordinator.handleDeleteGroups(Set(groupId))
    assertEquals(Errors.NONE, deleteErrors(groupId))

    val (err, data) = groupCoordinator.handleFetchOffsets(groupId, requireStable, Some(Seq(tip.topicPartition)))
    assertEquals(Errors.NONE, err)
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), data.get(tip.topicPartition).map(_.offset))
  }

  @Test
  def testBasicFetchTxnOffsets(): Unit = {
    val tip = new TopicIdPartition(Uuid.randomUuid(), 0, "topic")
    val offset = offsetAndMetadata(0)
    val producerId = 1000L
    val producerEpoch : Short = 2

    val commitOffsetResult = commitTransactionalOffsets(groupId, producerId, producerEpoch, Map(tip -> offset))
    assertEquals(Map(tip -> Errors.NONE), commitOffsetResult)

    val (error, partitionData) = groupCoordinator.handleFetchOffsets(groupId, requireStable, Some(Seq(tip.topicPartition)))

    // Validate that the offset isn't materialjzed yet.
    assertEquals(Errors.NONE, error)
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), partitionData.get(tip.topicPartition).map(_.offset))

    val offsetsTopic = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupPartitionId)

    // Send commit marker.
    handleTxnCompletion(producerId, List(offsetsTopic), TransactionResult.COMMIT)

    // Validate that committed offset is materialized.
    val (secondReqError, secondReqPartitionData) = groupCoordinator.handleFetchOffsets(groupId, requireStable, Some(Seq(tip.topicPartition)))
    assertEquals(Errors.NONE, secondReqError)
    assertEquals(Some(0), secondReqPartitionData.get(tip.topicPartition).map(_.offset))
  }

  @Test
  def testFetchTxnOffsetsWithAbort(): Unit = {
    val tip = new TopicIdPartition(Uuid.randomUuid(), 0, "topic")
    val offset = offsetAndMetadata(0)
    val producerId = 1000L
    val producerEpoch : Short = 2

    val commitOffsetResult = commitTransactionalOffsets(groupId, producerId, producerEpoch, Map(tip -> offset))
    assertEquals(Map(tip -> Errors.NONE), commitOffsetResult)

    val (error, partitionData) = groupCoordinator.handleFetchOffsets(groupId, requireStable, Some(Seq(tip.topicPartition)))
    assertEquals(Errors.NONE, error)
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), partitionData.get(tip.topicPartition).map(_.offset))

    val offsetsTopic = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupPartitionId)

    // Validate that the pending commit is discarded.
    handleTxnCompletion(producerId, List(offsetsTopic), TransactionResult.ABORT)

    val (secondReqError, secondReqPartitionData) = groupCoordinator.handleFetchOffsets(groupId, requireStable, Some(Seq(tip.topicPartition)))
    assertEquals(Errors.NONE, secondReqError)
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), secondReqPartitionData.get(tip.topicPartition).map(_.offset))
  }

  @Test
  def testFetchPendingTxnOffsetsWithAbort(): Unit = {
    val tip = new TopicIdPartition(Uuid.randomUuid(), 0, "topic")
    val offset = offsetAndMetadata(0)
    val producerId = 1000L
    val producerEpoch : Short = 2

    val commitOffsetResult = commitTransactionalOffsets(groupId, producerId, producerEpoch, Map(tip -> offset))
    assertEquals(Map(tip -> Errors.NONE), commitOffsetResult)

    val nonExistTp = new TopicPartition("non-exist-topic", 0)
    val (error, partitionData) = groupCoordinator.handleFetchOffsets(groupId, requireStable, Some(Seq(tip.topicPartition, nonExistTp)))
    assertEquals(Errors.NONE, error)
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), partitionData.get(tip.topicPartition).map(_.offset))
    assertEquals(Some(Errors.UNSTABLE_OFFSET_COMMIT), partitionData.get(tip.topicPartition).map(_.error))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), partitionData.get(nonExistTp).map(_.offset))
    assertEquals(Some(Errors.NONE), partitionData.get(nonExistTp).map(_.error))

    val offsetsTopic = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupPartitionId)

    // Validate that the pending commit is discarded.
    handleTxnCompletion(producerId, List(offsetsTopic), TransactionResult.ABORT)

    val (secondReqError, secondReqPartitionData) = groupCoordinator.handleFetchOffsets(groupId, requireStable, Some(Seq(tip.topicPartition)))
    assertEquals(Errors.NONE, secondReqError)
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), secondReqPartitionData.get(tip.topicPartition).map(_.offset))
    assertEquals(Some(Errors.NONE), secondReqPartitionData.get(tip.topicPartition).map(_.error))
  }

  @Test
  def testFetchPendingTxnOffsetsWithCommit(): Unit = {
    val tip = new TopicIdPartition(Uuid.randomUuid(), 0, "offset")
    val offset = offsetAndMetadata(25)
    val producerId = 1000L
    val producerEpoch : Short = 2

    val commitOffsetResult = commitTransactionalOffsets(groupId, producerId, producerEpoch, Map(tip -> offset))
    assertEquals(Map(tip -> Errors.NONE), commitOffsetResult)

    val (error, partitionData) = groupCoordinator.handleFetchOffsets(groupId, requireStable, Some(Seq(tip.topicPartition)))
    assertEquals(Errors.NONE, error)
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), partitionData.get(tip.topicPartition).map(_.offset))
    assertEquals(Some(Errors.UNSTABLE_OFFSET_COMMIT), partitionData.get(tip.topicPartition).map(_.error))

    val offsetsTopic = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupPartitionId)

    // Validate that the pending commit is committed
    handleTxnCompletion(producerId, List(offsetsTopic), TransactionResult.COMMIT)

    val (secondReqError, secondReqPartitionData) = groupCoordinator.handleFetchOffsets(groupId, requireStable, Some(Seq(tip.topicPartition)))
    assertEquals(Errors.NONE, secondReqError)
    assertEquals(Some(25), secondReqPartitionData.get(tip.topicPartition).map(_.offset))
    assertEquals(Some(Errors.NONE), secondReqPartitionData.get(tip.topicPartition).map(_.error))
  }

  @Test
  def testFetchTxnOffsetsIgnoreSpuriousCommit(): Unit = {
    val tip = new TopicIdPartition(Uuid.randomUuid(), 0, "topic")
    val offset = offsetAndMetadata(0)
    val producerId = 1000L
    val producerEpoch : Short = 2

    val commitOffsetResult = commitTransactionalOffsets(groupId, producerId, producerEpoch, Map(tip -> offset))
    assertEquals(Map(tip -> Errors.NONE), commitOffsetResult)

    val (error, partitionData) = groupCoordinator.handleFetchOffsets(groupId, requireStable, Some(Seq(tip.topicPartition)))
    assertEquals(Errors.NONE, error)
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), partitionData.get(tip.topicPartition).map(_.offset))

    val offsetsTopic = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupPartitionId)
    handleTxnCompletion(producerId, List(offsetsTopic), TransactionResult.ABORT)

    val (secondReqError, secondReqPartitionData) = groupCoordinator.handleFetchOffsets(groupId, requireStable, Some(Seq(tip.topicPartition)))
    assertEquals(Errors.NONE, secondReqError)
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), secondReqPartitionData.get(tip.topicPartition).map(_.offset))

    // Ignore spurious commit.
    handleTxnCompletion(producerId, List(offsetsTopic), TransactionResult.COMMIT)

    val (thirdReqError, thirdReqPartitionData) = groupCoordinator.handleFetchOffsets(groupId, requireStable, Some(Seq(tip.topicPartition)))
    assertEquals(Errors.NONE, thirdReqError)
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), thirdReqPartitionData.get(tip.topicPartition).map(_.offset))
  }

  @Test
  def testFetchTxnOffsetsOneProducerMultipleGroups(): Unit = {
    // One producer, two groups located on separate offsets topic partitions.
    // Both group have pending offset commits.
    // Marker for only one partition is received. That commit should be materialized while the other should not.

    val topicIdPartitions = List(
      new TopicIdPartition(Uuid.randomUuid(), 0, "topic1"),
      new TopicIdPartition(Uuid.randomUuid(), 0, "topic2")
    )
    val offsets = List(offsetAndMetadata(10), offsetAndMetadata(15))
    val producerId = 1000L
    val producerEpoch: Short = 3

    val groupIds = List(groupId, otherGroupId)
    val offsetTopicPartitions = List(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupCoordinator.partitionFor(groupId)),
      new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupCoordinator.partitionFor(otherGroupId)))

    groupCoordinator.groupManager.addOwnedPartition(offsetTopicPartitions(1).partition)
    val errors = mutable.ArrayBuffer[Errors]()
    val partitionData = mutable.ArrayBuffer[scala.collection.Map[TopicPartition, OffsetFetchResponse.PartitionData]]()

    val commitOffsetResults = mutable.ArrayBuffer[CommitOffsetCallbackParams]()

    // Ensure that the two groups map to different partitions.
    assertNotEquals(offsetTopicPartitions(0), offsetTopicPartitions(1))

    commitOffsetResults.append(commitTransactionalOffsets(groupId, producerId, producerEpoch, Map(topicIdPartitions(0) -> offsets(0))))
    assertEquals(Errors.NONE, commitOffsetResults(0)(topicIdPartitions(0)))
    commitOffsetResults.append(commitTransactionalOffsets(otherGroupId, producerId, producerEpoch, Map(topicIdPartitions(1) -> offsets(1))))
    assertEquals(Errors.NONE, commitOffsetResults(1)(topicIdPartitions(1)))

    // We got a commit for only one __consumer_offsets partition. We should only materialize it's group offsets.
    val topicPartitions = topicIdPartitions.map(_.topicPartition)
    handleTxnCompletion(producerId, List(offsetTopicPartitions(0)), TransactionResult.COMMIT)
    groupCoordinator.handleFetchOffsets(groupIds(0), requireStable, Some(topicPartitions)) match {
      case (error, partData) =>
        errors.append(error)
        partitionData.append(partData)
      case _ =>
    }

    groupCoordinator.handleFetchOffsets(groupIds(1), requireStable, Some(topicPartitions)) match {
      case (error, partData) =>
        errors.append(error)
        partitionData.append(partData)
      case _ =>
    }

    assertEquals(2, errors.size)
    assertEquals(Errors.NONE, errors(0))
    assertEquals(Errors.NONE, errors(1))

    // Exactly one offset commit should have been materialized.
    assertEquals(Some(offsets(0).offset), partitionData(0).get(topicPartitions(0)).map(_.offset))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), partitionData(0).get(topicPartitions(1)).map(_.offset))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), partitionData(1).get(topicPartitions(0)).map(_.offset))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), partitionData(1).get(topicPartitions(1)).map(_.offset))

    // Now we receive the other marker.
    handleTxnCompletion(producerId, List(offsetTopicPartitions(1)), TransactionResult.COMMIT)
    errors.clear()
    partitionData.clear()
    groupCoordinator.handleFetchOffsets(groupIds(0), requireStable, Some(topicPartitions)) match {
      case (error, partData) =>
        errors.append(error)
        partitionData.append(partData)
      case _ =>
    }

     groupCoordinator.handleFetchOffsets(groupIds(1), requireStable, Some(topicPartitions)) match {
      case (error, partData) =>
        errors.append(error)
        partitionData.append(partData)
      case _ =>
    }
    // Two offsets should have been materialized
    assertEquals(Some(offsets(0).offset), partitionData(0).get(topicPartitions(0)).map(_.offset))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), partitionData(0).get(topicPartitions(1)).map(_.offset))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), partitionData(1).get(topicPartitions(0)).map(_.offset))
    assertEquals(Some(offsets(1).offset), partitionData(1).get(topicPartitions(1)).map(_.offset))
  }

  @Test
  def testFetchTxnOffsetsMultipleProducersOneGroup(): Unit = {
    // One group, two producers
    // Different producers will commit offsets for different partitions.
    // Each partition's offsets should be materialized when the corresponding producer's marker is received.

    val topicIdPartitions = List(
      new TopicIdPartition(Uuid.randomUuid(), 0, "topic1"),
      new TopicIdPartition(Uuid.randomUuid(), 0, "topic2")
    )
    val offsets = List(offsetAndMetadata(10), offsetAndMetadata(15))
    val producerIds = List(1000L, 1005L)
    val producerEpochs: Seq[Short] = List(3, 4)

    val offsetTopicPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupCoordinator.partitionFor(groupId))

    val errors = mutable.ArrayBuffer[Errors]()
    val partitionData = mutable.ArrayBuffer[scala.collection.Map[TopicPartition, OffsetFetchResponse.PartitionData]]()

    val commitOffsetResults = mutable.ArrayBuffer[CommitOffsetCallbackParams]()

    // producer0 commits the offsets for partition0
    commitOffsetResults.append(commitTransactionalOffsets(groupId, producerIds(0), producerEpochs(0), Map(topicIdPartitions(0) -> offsets(0))))
    assertEquals(Errors.NONE, commitOffsetResults(0)(topicIdPartitions(0)))

    // producer1 commits the offsets for partition1
    commitOffsetResults.append(commitTransactionalOffsets(groupId, producerIds(1), producerEpochs(1), Map(topicIdPartitions(1) -> offsets(1))))
    assertEquals(Errors.NONE, commitOffsetResults(1)(topicIdPartitions(1)))

    // producer0 commits its transaction.
    val topicPartitions = topicIdPartitions.map(_.topicPartition)
    handleTxnCompletion(producerIds(0), List(offsetTopicPartition), TransactionResult.COMMIT)
    groupCoordinator.handleFetchOffsets(groupId, requireStable, Some(topicPartitions)) match {
      case (error, partData) =>
        errors.append(error)
        partitionData.append(partData)
      case _ =>
    }

    assertEquals(Errors.NONE, errors(0))

    // We should only see the offset commit for producer0
    assertEquals(Some(offsets(0).offset), partitionData(0).get(topicPartitions(0)).map(_.offset))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), partitionData(0).get(topicPartitions(1)).map(_.offset))

    // producer1 now commits its transaction.
    handleTxnCompletion(producerIds(1), List(offsetTopicPartition), TransactionResult.COMMIT)

    groupCoordinator.handleFetchOffsets(groupId, requireStable, Some(topicPartitions)) match {
      case (error, partData) =>
        errors.append(error)
        partitionData.append(partData)
      case _ =>
    }

    assertEquals(Errors.NONE, errors(1))

    // We should now see the offset commits for both producers.
    assertEquals(Some(offsets(0).offset), partitionData(1).get(topicPartitions(0)).map(_.offset))
    assertEquals(Some(offsets(1).offset), partitionData(1).get(topicPartitions(1)).map(_.offset))
  }

  @Test
  def testFetchOffsetForUnknownPartition(): Unit = {
    val tp = new TopicPartition("topic", 0)
    val (error, partitionData) = groupCoordinator.handleFetchOffsets(groupId, requireStable, Some(Seq(tp)))
    assertEquals(Errors.NONE, error)
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), partitionData.get(tp).map(_.offset))
  }

  @Test
  def testFetchOffsetNotCoordinatorForGroup(): Unit = {
    val tp = new TopicPartition("topic", 0)
    val (error, partitionData) = groupCoordinator.handleFetchOffsets(otherGroupId, requireStable, Some(Seq(tp)))
    assertEquals(Errors.NOT_COORDINATOR, error)
    assertTrue(partitionData.isEmpty)
  }

  @Test
  def testFetchAllOffsets(): Unit = {
    val tip1 = new TopicIdPartition(Uuid.randomUuid(), 0, "topic")
    val tip2 = new TopicIdPartition(tip1.topicId, 1, "topic")
    val tip3 = new TopicIdPartition(Uuid.randomUuid(), 0, "other-topic")
    val offset1 = offsetAndMetadata(15)
    val offset2 = offsetAndMetadata(16)
    val offset3 = offsetAndMetadata(17)

    assertEquals((Errors.NONE, Map.empty), groupCoordinator.handleFetchOffsets(groupId, requireStable))

    val commitOffsetResult = commitOffsets(groupId, OffsetCommitRequest.DEFAULT_MEMBER_ID,
      OffsetCommitRequest.DEFAULT_GENERATION_ID, Map(tip1 -> offset1, tip2 -> offset2, tip3 -> offset3))
    assertEquals(Map(tip1 -> Errors.NONE, tip2 -> Errors.NONE, tip3 -> Errors.NONE), commitOffsetResult)

    val (error, partitionData) = groupCoordinator.handleFetchOffsets(groupId, requireStable)
    assertEquals(Errors.NONE, error)
    assertEquals(3, partitionData.size)
    assertTrue(partitionData.forall(_._2.error == Errors.NONE))
    assertEquals(Some(offset1.offset), partitionData.get(tip1.topicPartition).map(_.offset))
    assertEquals(Some(offset2.offset), partitionData.get(tip2.topicPartition).map(_.offset))
    assertEquals(Some(offset3.offset), partitionData.get(tip3.topicPartition).map(_.offset))
  }

  @Test
  def testCommitOffsetInCompletingRebalance(): Unit = {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val tip = new TopicIdPartition(Uuid.randomUuid(), 0, "topic")
    val offset = offsetAndMetadata(0)

    val joinGroupResult = dynamicJoinGroup(groupId, memberId, protocolType, protocols)
    val assignedMemberId = joinGroupResult.memberId
    val generationId = joinGroupResult.generationId
    val joinGroupError = joinGroupResult.error
    assertEquals(Errors.NONE, joinGroupError)

    val commitOffsetResult = commitOffsets(groupId, assignedMemberId, generationId, Map(tip -> offset))
    assertEquals(Map(tip -> Errors.REBALANCE_IN_PROGRESS), commitOffsetResult)
  }

  @Test
  def testCommitOffsetInCompletingRebalanceFromUnknownMemberId(): Unit = {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val tip = new TopicIdPartition(Uuid.randomUuid(), 0 , "topic")
    val offset = offsetAndMetadata(0)

    val joinGroupResult = dynamicJoinGroup(groupId, memberId, protocolType, protocols)
    val generationId = joinGroupResult.generationId
    val joinGroupError = joinGroupResult.error
    assertEquals(Errors.NONE, joinGroupError)

    val commitOffsetResult = commitOffsets(groupId, memberId, generationId, Map(tip -> offset))
    assertEquals(Map(tip -> Errors.UNKNOWN_MEMBER_ID), commitOffsetResult)
  }

  @Test
  def testCommitOffsetInCompletingRebalanceFromIllegalGeneration(): Unit = {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val tip = new TopicIdPartition(Uuid.randomUuid(), 0, "topic")
    val offset = offsetAndMetadata(0)

    val joinGroupResult = dynamicJoinGroup(groupId, memberId, protocolType, protocols)
    val assignedMemberId = joinGroupResult.memberId
    val generationId = joinGroupResult.generationId
    val joinGroupError = joinGroupResult.error
    assertEquals(Errors.NONE, joinGroupError)

    val commitOffsetResult = commitOffsets(groupId, assignedMemberId, generationId + 1, Map(tip -> offset))
    assertEquals(Map(tip -> Errors.ILLEGAL_GENERATION), commitOffsetResult)
  }

  @Test
  def testManualCommitOffsetShouldNotValidateMemberIdAndInstanceId(): Unit = {
    val tip = new TopicIdPartition(Uuid.randomUuid(), 0, "topic")

    var commitOffsetResult = commitOffsets(
      groupId,
      JoinGroupRequest.UNKNOWN_MEMBER_ID,
      -1,
      Map(tip -> offsetAndMetadata(0)),
      Some("instance-id")
    )
    assertEquals(Map(tip -> Errors.NONE), commitOffsetResult)

    commitOffsetResult = commitOffsets(
      groupId,
      "unknown",
      -1,
      Map(tip -> offsetAndMetadata(0)),
      None
    )
    assertEquals(Map(tip -> Errors.NONE), commitOffsetResult)
  }

  @Test
  def testTxnCommitOffsetWithFencedInstanceId(): Unit = {
    val tip = new TopicIdPartition(Uuid.randomUuid(), 0, "topic")
    val offset = offsetAndMetadata(0)
    val producerId = 1000L
    val producerEpoch : Short = 2

    val rebalanceResult = staticMembersJoinAndRebalance(leaderInstanceId, followerInstanceId)

    val leaderNoMemberIdCommitOffsetResult = commitTransactionalOffsets(groupId, producerId, producerEpoch,
      Map(tip -> offset), memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID, groupInstanceId = Some(leaderInstanceId))
    assertEquals(Map(tip -> Errors.FENCED_INSTANCE_ID), leaderNoMemberIdCommitOffsetResult)

    val leaderInvalidMemberIdCommitOffsetResult = commitTransactionalOffsets(groupId, producerId, producerEpoch,
      Map(tip -> offset), memberId = "invalid-member", groupInstanceId = Some(leaderInstanceId))
    assertEquals(Map(tip -> Errors.FENCED_INSTANCE_ID), leaderInvalidMemberIdCommitOffsetResult)

    val leaderCommitOffsetResult = commitTransactionalOffsets(groupId, producerId, producerEpoch,
      Map(tip -> offset), rebalanceResult.leaderId, Some(leaderInstanceId), rebalanceResult.generation)
    assertEquals(Map(tip -> Errors.NONE), leaderCommitOffsetResult)
  }

  @Test
  def testTxnCommitOffsetWithInvalidMemberId(): Unit = {
    val tip = new TopicIdPartition(Uuid.randomUuid(), 0, "topic")
    val offset = offsetAndMetadata(0)
    val producerId = 1000L
    val producerEpoch : Short = 2

    val joinGroupResult = dynamicJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols)
    val joinGroupError = joinGroupResult.error
    assertEquals(Errors.NONE, joinGroupError)

    val invalidIdCommitOffsetResult = commitTransactionalOffsets(groupId, producerId, producerEpoch,
      Map(tip -> offset), "invalid-member")
    assertEquals(Map(tip -> Errors.UNKNOWN_MEMBER_ID), invalidIdCommitOffsetResult)
  }

  @Test
  def testTxnCommitOffsetWithKnownMemberId(): Unit = {
    val tip = new TopicIdPartition(Uuid.randomUuid(), 0, "topic")
    val offset = offsetAndMetadata(0)
    val producerId = 1000L
    val producerEpoch : Short = 2

    val joinGroupResult = dynamicJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols)
    val joinGroupError = joinGroupResult.error
    assertEquals(Errors.NONE, joinGroupError)


    val assignedConsumerId = joinGroupResult.memberId
    val leaderCommitOffsetResult = commitTransactionalOffsets(groupId, producerId, producerEpoch,
      Map(tip -> offset), assignedConsumerId, generationId = joinGroupResult.generationId)
    assertEquals(Map(tip -> Errors.NONE), leaderCommitOffsetResult)
  }

  @Test
  def testTxnCommitOffsetWithIllegalGeneration(): Unit = {
    val tip = new TopicIdPartition(Uuid.randomUuid(), 0, "topic")
    val offset = offsetAndMetadata(0)
    val producerId = 1000L
    val producerEpoch : Short = 2

    val joinGroupResult = dynamicJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols)
    val joinGroupError = joinGroupResult.error
    assertEquals(Errors.NONE, joinGroupError)


    val assignedConsumerId = joinGroupResult.memberId
    val initialGenerationId = joinGroupResult.generationId
    val illegalGenerationCommitOffsetResult = commitTransactionalOffsets(groupId, producerId, producerEpoch,
      Map(tip -> offset), memberId = assignedConsumerId, generationId = initialGenerationId + 5)
    assertEquals(Map(tip -> Errors.ILLEGAL_GENERATION), illegalGenerationCommitOffsetResult)
  }

  @Test
  def testTxnCommitOffsetWithLegalGeneration(): Unit = {
    val tip = new TopicIdPartition(Uuid.randomUuid(), 0, "topic")
    val offset = offsetAndMetadata(0)
    val producerId = 1000L
    val producerEpoch : Short = 2

    val joinGroupResult = dynamicJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols)
    val joinGroupError = joinGroupResult.error
    assertEquals(Errors.NONE, joinGroupError)


    val assignedConsumerId = joinGroupResult.memberId
    val initialGenerationId = joinGroupResult.generationId
    val leaderCommitOffsetResult = commitTransactionalOffsets(groupId, producerId, producerEpoch,
      Map(tip -> offset), memberId = assignedConsumerId, generationId = initialGenerationId)
    assertEquals(Map(tip -> Errors.NONE), leaderCommitOffsetResult)
  }

  @Test
  def testHeartbeatDuringRebalanceCausesRebalanceInProgress(): Unit = {
    // First start up a group (with a slightly larger timeout to give us time to heartbeat when the rebalance starts)
    val joinGroupResult = dynamicJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols)
    val assignedConsumerId = joinGroupResult.memberId
    val initialGenerationId = joinGroupResult.generationId
    val joinGroupError = joinGroupResult.error
    assertEquals(Errors.NONE, joinGroupError)

    // Then join with a new consumer to trigger a rebalance
    sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols)

    // We should be in the middle of a rebalance, so the heartbeat should return rebalance in progress
    val heartbeatResult = heartbeat(groupId, assignedConsumerId, initialGenerationId)
    assertEquals(Errors.REBALANCE_IN_PROGRESS, heartbeatResult)
  }

  @Test
  def testGenerationIdIncrementsOnRebalance(): Unit = {
    val joinGroupResult = dynamicJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols)
    val initialGenerationId = joinGroupResult.generationId
    val joinGroupError = joinGroupResult.error
    val memberId = joinGroupResult.memberId
    assertEquals(1, initialGenerationId)
    assertEquals(Errors.NONE, joinGroupError)

    val syncGroupResult = syncGroupLeader(groupId, initialGenerationId, memberId, Map(memberId -> Array[Byte]()))
    assertEquals(Errors.NONE, syncGroupResult.error)

    val joinGroupFuture = sendJoinGroup(groupId, memberId, protocolType, protocols)
    val otherJoinGroupResult = await(joinGroupFuture, 1)

    val nextGenerationId = otherJoinGroupResult.generationId
    val otherJoinGroupError = otherJoinGroupResult.error
    assertEquals(2, nextGenerationId)
    assertEquals(Errors.NONE, otherJoinGroupError)
  }

  @Test
  def testLeaveGroupWrongCoordinator(): Unit = {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val leaveGroupResults = singleLeaveGroup(otherGroupId, memberId)
    verifyLeaveGroupResult(leaveGroupResults, Errors.NOT_COORDINATOR)
  }

  @Test
  def testLeaveGroupUnknownGroup(): Unit = {
    val leaveGroupResults = singleLeaveGroup(groupId, memberId)
    verifyLeaveGroupResult(leaveGroupResults, Errors.NONE, List(Errors.UNKNOWN_MEMBER_ID))
  }

  @Test
  def testLeaveGroupUnknownConsumerExistingGroup(): Unit = {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val otherMemberId = "consumerId"

    val joinGroupResult = dynamicJoinGroup(groupId, memberId, protocolType, protocols)
    val joinGroupError = joinGroupResult.error
    assertEquals(Errors.NONE, joinGroupError)

    val leaveGroupResults = singleLeaveGroup(groupId, otherMemberId)
    verifyLeaveGroupResult(leaveGroupResults, Errors.NONE, List(Errors.UNKNOWN_MEMBER_ID))
  }

  @Test
  def testSingleLeaveDeadGroup(): Unit = {
    val deadGroupId = "deadGroupId"

    groupCoordinator.groupManager.addGroup(new GroupMetadata(deadGroupId, Dead, new MockTime()))
    val leaveGroupResults = singleLeaveGroup(deadGroupId, memberId)
    verifyLeaveGroupResult(leaveGroupResults, Errors.COORDINATOR_NOT_AVAILABLE)
  }

  @Test
  def testBatchLeaveDeadGroup(): Unit = {
    val deadGroupId = "deadGroupId"

    groupCoordinator.groupManager.addGroup(new GroupMetadata(deadGroupId, Dead, new MockTime()))
    val leaveGroupResults = batchLeaveGroup(deadGroupId,
      List(new MemberIdentity().setMemberId(memberId), new MemberIdentity().setMemberId(memberId)))
    verifyLeaveGroupResult(leaveGroupResults, Errors.COORDINATOR_NOT_AVAILABLE)
  }

  @Test
  def testValidLeaveGroup(): Unit = {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val joinGroupResult = dynamicJoinGroup(groupId, memberId, protocolType, protocols)
    val assignedMemberId = joinGroupResult.memberId
    val joinGroupError = joinGroupResult.error
    assertEquals(Errors.NONE, joinGroupError)

    val leaveGroupResults = singleLeaveGroup(groupId, assignedMemberId)
    verifyLeaveGroupResult(leaveGroupResults)
  }

  @Test
  def testLeaveGroupWithFencedInstanceId(): Unit = {
    val joinGroupResult = staticJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, leaderInstanceId, protocolType, protocolSuperset)
    assertEquals(Errors.NONE, joinGroupResult.error)

    val leaveGroupResults = singleLeaveGroup(groupId, "some_member", Some(leaderInstanceId))
    verifyLeaveGroupResult(leaveGroupResults, Errors.NONE, List(Errors.FENCED_INSTANCE_ID))
  }

  @Test
  def testLeaveGroupStaticMemberWithUnknownMemberId(): Unit = {
    val joinGroupResult = staticJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, leaderInstanceId, protocolType, protocolSuperset)
    assertEquals(Errors.NONE, joinGroupResult.error)

    // Having unknown member id will not affect the request processing.
    val leaveGroupResults = singleLeaveGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, Some(leaderInstanceId))
    verifyLeaveGroupResult(leaveGroupResults, Errors.NONE, List(Errors.NONE))
  }

  @Test
  def testStaticMembersValidBatchLeaveGroup(): Unit = {
    staticMembersJoinAndRebalance(leaderInstanceId, followerInstanceId)

    val leaveGroupResults = batchLeaveGroup(groupId, List(new MemberIdentity()
      .setGroupInstanceId(leaderInstanceId), new MemberIdentity().setGroupInstanceId(followerInstanceId)))

    verifyLeaveGroupResult(leaveGroupResults, Errors.NONE, List(Errors.NONE, Errors.NONE))
  }

  @Test
  def testStaticMembersWrongCoordinatorBatchLeaveGroup(): Unit = {
    staticMembersJoinAndRebalance(leaderInstanceId, followerInstanceId)

    val leaveGroupResults = batchLeaveGroup("invalid-group", List(new MemberIdentity()
      .setGroupInstanceId(leaderInstanceId), new MemberIdentity().setGroupInstanceId(followerInstanceId)))

    verifyLeaveGroupResult(leaveGroupResults, Errors.NOT_COORDINATOR)
  }

  @Test
  def testStaticMembersUnknownGroupBatchLeaveGroup(): Unit = {
    val leaveGroupResults = batchLeaveGroup(groupId, List(new MemberIdentity()
      .setGroupInstanceId(leaderInstanceId), new MemberIdentity().setGroupInstanceId(followerInstanceId)))

    verifyLeaveGroupResult(leaveGroupResults, Errors.NONE, List(Errors.UNKNOWN_MEMBER_ID, Errors.UNKNOWN_MEMBER_ID))
  }

  @Test
  def testStaticMembersFencedInstanceBatchLeaveGroup(): Unit = {
    staticMembersJoinAndRebalance(leaderInstanceId, followerInstanceId)

    val leaveGroupResults = batchLeaveGroup(groupId, List(new MemberIdentity()
      .setGroupInstanceId(leaderInstanceId), new MemberIdentity()
      .setGroupInstanceId(followerInstanceId)
      .setMemberId("invalid-member")))

    verifyLeaveGroupResult(leaveGroupResults, Errors.NONE, List(Errors.NONE, Errors.FENCED_INSTANCE_ID))
  }

  @Test
  def testStaticMembersUnknownInstanceBatchLeaveGroup(): Unit = {
    staticMembersJoinAndRebalance(leaderInstanceId, followerInstanceId)

    val leaveGroupResults = batchLeaveGroup(groupId, List(new MemberIdentity()
      .setGroupInstanceId("unknown-instance"), new MemberIdentity()
      .setGroupInstanceId(followerInstanceId)))

    verifyLeaveGroupResult(leaveGroupResults, Errors.NONE, List(Errors.UNKNOWN_MEMBER_ID, Errors.NONE))
  }

  @Test
  def testPendingMemberBatchLeaveGroup(): Unit = {
    val pendingMember = setupGroupWithPendingMember()

    val leaveGroupResults = batchLeaveGroup(groupId, List(new MemberIdentity()
      .setGroupInstanceId("unknown-instance"), new MemberIdentity()
      .setMemberId(pendingMember.memberId)))

    verifyLeaveGroupResult(leaveGroupResults, Errors.NONE, List(Errors.UNKNOWN_MEMBER_ID, Errors.NONE))
  }

  @Test
  def testListGroupsIncludesStableGroups(): Unit = {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val joinGroupResult = dynamicJoinGroup(groupId, memberId, protocolType, protocols)
    val assignedMemberId = joinGroupResult.memberId
    val generationId = joinGroupResult.generationId
    assertEquals(Errors.NONE, joinGroupResult.error)

    val syncGroupResult = syncGroupLeader(groupId, generationId, assignedMemberId, Map(assignedMemberId -> Array[Byte]()))
    assertEquals(Errors.NONE, syncGroupResult.error)

    val (error, groups) = groupCoordinator.handleListGroups(Set(), Set())
    assertEquals(Errors.NONE, error)
    assertEquals(1, groups.size)
    assertEquals(GroupOverview("groupId", "consumer", Stable.toString, "classic"), groups.head)
  }

  @Test
  def testListGroupsIncludesRebalancingGroups(): Unit = {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val joinGroupResult = dynamicJoinGroup(groupId, memberId, protocolType, protocols)
    assertEquals(Errors.NONE, joinGroupResult.error)

    val (error, groups) = groupCoordinator.handleListGroups(Set(), Set())
    assertEquals(Errors.NONE, error)
    assertEquals(1, groups.size)
    assertEquals(GroupOverview("groupId", "consumer", CompletingRebalance.toString, "classic"), groups.head)
  }

  @Test
  def testListGroupsWithStates(): Unit = {
    val allStates = Set(PreparingRebalance, CompletingRebalance, Stable, Dead, Empty).map(s => s.toString)
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    // Member joins the group
    val joinGroupResult = dynamicJoinGroup(groupId, memberId, protocolType, protocols)
    val assignedMemberId = joinGroupResult.memberId
    val generationId = joinGroupResult.generationId
    assertEquals(Errors.NONE, joinGroupResult.error)

    // The group should be in CompletingRebalance
    val (error, groups) = groupCoordinator.handleListGroups(Set(CompletingRebalance.toString), Set())
    assertEquals(Errors.NONE, error)
    assertEquals(1, groups.size)
    val (error2, groups2) = groupCoordinator.handleListGroups(allStates.filterNot(s => s == CompletingRebalance.toString), Set())
    assertEquals(Errors.NONE, error2)
    assertEquals(0, groups2.size)

    // Member syncs
    val syncGroupResult = syncGroupLeader(groupId, generationId, assignedMemberId, Map(assignedMemberId -> Array[Byte]()))
    assertEquals(Errors.NONE, syncGroupResult.error)

    // The group is now stable
    val (error3, groups3) = groupCoordinator.handleListGroups(Set(Stable.toString), Set())
    assertEquals(Errors.NONE, error3)
    assertEquals(1, groups3.size)
    val (error4, groups4) = groupCoordinator.handleListGroups(allStates.filterNot(s => s == Stable.toString), Set())
    assertEquals(Errors.NONE, error4)
    assertEquals(0, groups4.size)

    // Member leaves
    val leaveGroupResults = singleLeaveGroup(groupId, assignedMemberId)
    verifyLeaveGroupResult(leaveGroupResults)

    // The group is now empty
    val (error5, groups5) = groupCoordinator.handleListGroups(Set(Empty.toString), Set())
    assertEquals(Errors.NONE, error5)
    assertEquals(1, groups5.size)
    val (error6, groups6) = groupCoordinator.handleListGroups(allStates.filterNot(s => s == Empty.toString), Set())
    assertEquals(Errors.NONE, error6)
    assertEquals(0, groups6.size)
  }

  @Test
  def testListGroupsWithTypes(): Unit = {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val joinGroupResult = dynamicJoinGroup(groupId, memberId, protocolType, protocols)
    val assignedMemberId = joinGroupResult.memberId
    val generationId = joinGroupResult.generationId
    assertEquals(Errors.NONE, joinGroupResult.error)

    val syncGroupResult = syncGroupLeader(groupId, generationId, assignedMemberId, Map(assignedMemberId -> Array[Byte]()))
    assertEquals(Errors.NONE, syncGroupResult.error)

    // When a group type filter is specified:
    // All groups are returned if the type is classic, else nothing is returned.
    val (error1, groups1) = groupCoordinator.handleListGroups(Set(), Set("classic"))
    assertEquals(Errors.NONE, error1)
    assertEquals(1, groups1.size)
    assertEquals(GroupOverview("groupId", "consumer", Stable.toString, "classic"), groups1.head)

    val (error2, groups2) = groupCoordinator.handleListGroups(Set(), Set("consumer"))
    assertEquals(Errors.NONE, error2)
    assertEquals(0, groups2.size)

    // No groups are returned when an incorrect group type is passed.
    val (error3, groups3) = groupCoordinator.handleListGroups(Set(), Set("Invalid"))
    assertEquals(Errors.NONE, error3)
    assertEquals(0, groups3.size)

    // When no group type filter is specified, all groups are returned with classic group type.
    val (error4, groups4) = groupCoordinator.handleListGroups(Set(), Set())
    assertEquals(Errors.NONE, error4)
    assertEquals(1, groups4.size)
    assertEquals(GroupOverview("groupId", "consumer", Stable.toString, "classic"), groups4.head)

    // Check that group type is case-insensitive.
    val (error5, groups5) = groupCoordinator.handleListGroups(Set(), Set("Classic"))
    assertEquals(Errors.NONE, error5)
    assertEquals(1, groups5.size)
    assertEquals(GroupOverview("groupId", "consumer", Stable.toString, "classic"), groups5.head)
  }

  @Test
  def testDescribeGroupWrongCoordinator(): Unit = {
    val (error, _) = groupCoordinator.handleDescribeGroup(otherGroupId)
    assertEquals(Errors.NOT_COORDINATOR, error)
  }

  @Test
  def testDescribeGroupInactiveGroup(): Unit = {
    val (error, summary) = groupCoordinator.handleDescribeGroup(groupId)
    assertEquals(Errors.NONE, error)
    assertEquals(GroupCoordinator.DeadGroup, summary)
  }

  @Test
  def testDescribeGroupStableForDynamicMember(): Unit = {
    val joinGroupResult = dynamicJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols)
    val assignedMemberId = joinGroupResult.memberId
    val generationId = joinGroupResult.generationId
    val joinGroupError = joinGroupResult.error
    assertEquals(Errors.NONE, joinGroupError)

    val syncGroupResult = syncGroupLeader(groupId, generationId, assignedMemberId, Map(assignedMemberId -> Array[Byte]()))
    assertEquals(Errors.NONE, syncGroupResult.error)

    val (error, summary) = groupCoordinator.handleDescribeGroup(groupId)
    assertEquals(Errors.NONE, error)
    assertEquals(protocolType, summary.protocolType)
    assertEquals("range", summary.protocol)
    assertEquals(List(assignedMemberId), summary.members.map(_.memberId))
  }

  @Test
  def testDescribeGroupStableForStaticMember(): Unit = {
    val joinGroupResult = staticJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, leaderInstanceId, protocolType, protocols)
    val assignedMemberId = joinGroupResult.memberId
    val generationId = joinGroupResult.generationId
    val joinGroupError = joinGroupResult.error
    assertEquals(Errors.NONE, joinGroupError)

    val syncGroupResult = syncGroupLeader(groupId, generationId, assignedMemberId, Map(assignedMemberId -> Array[Byte]()))
    assertEquals(Errors.NONE, syncGroupResult.error)

    val (error, summary) = groupCoordinator.handleDescribeGroup(groupId)
    assertEquals(Errors.NONE, error)
    assertEquals(protocolType, summary.protocolType)
    assertEquals("range", summary.protocol)
    assertEquals(List(assignedMemberId), summary.members.map(_.memberId))
    assertEquals(List(leaderInstanceId), summary.members.flatMap(_.groupInstanceId))
  }

  @Test
  def testDescribeGroupRebalancing(): Unit = {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val joinGroupResult = dynamicJoinGroup(groupId, memberId, protocolType, protocols)
    val joinGroupError = joinGroupResult.error
    assertEquals(Errors.NONE, joinGroupError)

    val (error, summary) = groupCoordinator.handleDescribeGroup(groupId)
    assertEquals(Errors.NONE, error)
    assertEquals(protocolType, summary.protocolType)
    assertEquals(GroupCoordinator.NoProtocol, summary.protocol)
    assertEquals(CompletingRebalance.toString, summary.state)
    assertTrue(summary.members.map(_.memberId).contains(joinGroupResult.memberId))
    assertTrue(summary.members.forall(_.metadata.isEmpty))
    assertTrue(summary.members.forall(_.assignment.isEmpty))
  }

  @Test
  def testDeleteNonEmptyGroup(): Unit = {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    dynamicJoinGroup(groupId, memberId, protocolType, protocols)

    val result = groupCoordinator.handleDeleteGroups(Set(groupId))
    assert(result.size == 1 && result.contains(groupId) && result.get(groupId).contains(Errors.NON_EMPTY_GROUP))
  }

  @Test
  def testDeleteGroupWithInvalidGroupId(): Unit = {
    val invalidGroupId = null
    val result = groupCoordinator.handleDeleteGroups(Set(invalidGroupId))
    assert(result.size == 1 && result.contains(invalidGroupId) && result.get(invalidGroupId).contains(Errors.INVALID_GROUP_ID))
  }

  @Test
  def testDeleteGroupWithWrongCoordinator(): Unit = {
    val result = groupCoordinator.handleDeleteGroups(Set(otherGroupId))
    assert(result.size == 1 && result.contains(otherGroupId) && result.get(otherGroupId).contains(Errors.NOT_COORDINATOR))
  }

  @Test
  def testDeleteEmptyGroup(): Unit = {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val joinGroupResult = dynamicJoinGroup(groupId, memberId, protocolType, protocols)

    val leaveGroupResults = singleLeaveGroup(groupId, joinGroupResult.memberId)
    verifyLeaveGroupResult(leaveGroupResults)

    val groupTopicPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupPartitionId)
    val partition: Partition = mock(classOf[Partition])

    when(replicaManager.getMagic(any[TopicPartition])).thenReturn(Some(RecordBatch.CURRENT_MAGIC_VALUE))
    when(replicaManager.getPartition(groupTopicPartition)).thenReturn(HostedPartition.Online(partition))
    when(replicaManager.onlinePartition(groupTopicPartition)).thenReturn(Some(partition))

    val result = groupCoordinator.handleDeleteGroups(Set(groupId))
    assert(result.size == 1 && result.contains(groupId) && result.get(groupId).contains(Errors.NONE))
  }

  @Test
  def testDeleteEmptyGroupWithStoredOffsets(): Unit = {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val joinGroupResult = dynamicJoinGroup(groupId, memberId, protocolType, protocols)
    val assignedMemberId = joinGroupResult.memberId
    val joinGroupError = joinGroupResult.error
    assertEquals(Errors.NONE, joinGroupError)

    val syncGroupResult = syncGroupLeader(groupId, joinGroupResult.generationId, assignedMemberId, Map(assignedMemberId -> Array[Byte]()))
    assertEquals(Errors.NONE, syncGroupResult.error)

    val tip = new TopicIdPartition(Uuid.randomUuid(), 0, "topic")
    val offset = offsetAndMetadata(0)
    val commitOffsetResult = commitOffsets(groupId, assignedMemberId, joinGroupResult.generationId, Map(tip -> offset))
    assertEquals(Map(tip -> Errors.NONE), commitOffsetResult)

    val describeGroupResult = groupCoordinator.handleDescribeGroup(groupId)
    assertEquals(Stable.toString, describeGroupResult._2.state)
    assertEquals(assignedMemberId, describeGroupResult._2.members.head.memberId)

    val leaveGroupResults = singleLeaveGroup(groupId, assignedMemberId)
    verifyLeaveGroupResult(leaveGroupResults)

    val groupTopicPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupPartitionId)
    val partition: Partition = mock(classOf[Partition])

    when(replicaManager.getMagic(any[TopicPartition])).thenReturn(Some(RecordBatch.CURRENT_MAGIC_VALUE))
    when(replicaManager.getPartition(groupTopicPartition)).thenReturn(HostedPartition.Online(partition))
    when(replicaManager.onlinePartition(groupTopicPartition)).thenReturn(Some(partition))

    val result = groupCoordinator.handleDeleteGroups(Set(groupId))
    assert(result.size == 1 && result.contains(groupId) && result.get(groupId).contains(Errors.NONE))

    assertEquals(Dead.toString, groupCoordinator.handleDescribeGroup(groupId)._2.state)
  }

  @Test
  def testDeleteOffsetOfNonExistingGroup(): Unit = {
    val tp = new TopicPartition("foo", 0)
    val (groupError, topics) = groupCoordinator.handleDeleteOffsets(groupId, Seq(tp),
      RequestLocal.NoCaching)

    assertEquals(Errors.GROUP_ID_NOT_FOUND, groupError)
    assertTrue(topics.isEmpty)
  }

  @Test
  def testDeleteOffsetOfNonEmptyNonConsumerGroup(): Unit = {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    dynamicJoinGroup(groupId, memberId, "My Protocol", protocols)
    val tp = new TopicPartition("foo", 0)
    val (groupError, topics) = groupCoordinator.handleDeleteOffsets(groupId, Seq(tp),
      RequestLocal.NoCaching)

    assertEquals(Errors.NON_EMPTY_GROUP, groupError)
    assertTrue(topics.isEmpty)
  }

  @Test
  def testDeleteOffsetOfEmptyNonConsumerGroup(): Unit = {
    // join the group
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID

    val joinGroupResult = dynamicJoinGroup(groupId, memberId, "My Protocol", protocols)
    assertEquals(Errors.NONE, joinGroupResult.error)

    val syncGroupResult = syncGroupLeader(groupId, joinGroupResult.generationId, joinGroupResult.leaderId, Map.empty)
    assertEquals(Errors.NONE, syncGroupResult.error)

    val ti1p0 = new TopicIdPartition(Uuid.randomUuid(), 0, "foo")
    val ti2p0 = new TopicIdPartition(Uuid.randomUuid(), 0, "bar")
    val offset = offsetAndMetadata(37)

    val validOffsetCommitResult = commitOffsets(groupId, joinGroupResult.memberId, joinGroupResult.generationId,
      Map(ti1p0 -> offset, ti2p0 -> offset))
    assertEquals(Map(ti1p0 -> Errors.NONE, ti2p0 -> Errors.NONE), validOffsetCommitResult)

    // and leaves.
    val leaveGroupResults = singleLeaveGroup(groupId, joinGroupResult.memberId)
    verifyLeaveGroupResult(leaveGroupResults)

    assertTrue(groupCoordinator.groupManager.getGroup(groupId).exists(_.is(Empty)))

    val groupTopicPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupPartitionId)
    val partition: Partition = mock(classOf[Partition])

    when(replicaManager.getMagic(any[TopicPartition])).thenReturn(Some(RecordBatch.CURRENT_MAGIC_VALUE))
    when(replicaManager.getPartition(groupTopicPartition)).thenReturn(HostedPartition.Online(partition))
    when(replicaManager.onlinePartition(groupTopicPartition)).thenReturn(Some(partition))

    val (groupError, topics) = groupCoordinator.handleDeleteOffsets(groupId, Seq(ti1p0.topicPartition),
      RequestLocal.NoCaching)

    assertEquals(Errors.NONE, groupError)
    assertEquals(1, topics.size)
    assertEquals(Some(Errors.NONE), topics.get(ti1p0.topicPartition))

    val cachedOffsets = groupCoordinator.groupManager.getOffsets(groupId, requireStable, Some(Seq(ti1p0.topicPartition, ti2p0.topicPartition)))

    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(ti1p0.topicPartition).map(_.offset))
    assertEquals(Some(offset.offset), cachedOffsets.get(ti2p0.topicPartition).map(_.offset))
  }

  @Test
  def testDeleteOffsetOfConsumerGroupWithUnparsableProtocol(): Unit = {
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val joinGroupResult = dynamicJoinGroup(groupId, memberId, protocolType, protocols)

    val syncGroupResult = syncGroupLeader(groupId, joinGroupResult.generationId, joinGroupResult.leaderId, Map.empty)
    assertEquals(Errors.NONE, syncGroupResult.error)

    val tip = new TopicIdPartition(Uuid.randomUuid(), 0, "foo")
    val offset = offsetAndMetadata(37)

    val validOffsetCommitResult = commitOffsets(groupId, joinGroupResult.memberId, joinGroupResult.generationId,
      Map(tip -> offset))
    assertEquals(Map(tip -> Errors.NONE), validOffsetCommitResult)

    val (groupError, topics) = groupCoordinator.handleDeleteOffsets(groupId, Seq(tip.topicPartition),
      RequestLocal.NoCaching)

    assertEquals(Errors.NONE, groupError)
    assertEquals(1, topics.size)
    assertEquals(Some(Errors.GROUP_SUBSCRIBED_TO_TOPIC), topics.get(tip.topicPartition))
  }

  @Test
  def testDeleteOffsetOfDeadConsumerGroup(): Unit = {
    val group = new GroupMetadata(groupId, Dead, new MockTime())
    group.protocolType = Some(protocolType)
    groupCoordinator.groupManager.addGroup(group)

    val tp = new TopicPartition("foo", 0)
    val (groupError, topics) = groupCoordinator.handleDeleteOffsets(groupId, Seq(tp),
      RequestLocal.NoCaching)

    assertEquals(Errors.GROUP_ID_NOT_FOUND, groupError)
    assertTrue(topics.isEmpty)
  }

  @Test
  def testDeleteOffsetOfEmptyConsumerGroup(): Unit = {
    // join the group
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val joinGroupResult = dynamicJoinGroup(groupId, memberId, protocolType, protocols)
    assertEquals(Errors.NONE, joinGroupResult.error)

    val syncGroupResult = syncGroupLeader(groupId, joinGroupResult.generationId, joinGroupResult.leaderId, Map.empty)
    assertEquals(Errors.NONE, syncGroupResult.error)

    val ti1p0 = new TopicIdPartition(Uuid.randomUuid(), 0, "foo")
    val ti2p0 = new TopicIdPartition(Uuid.randomUuid(), 0, "bar")
    val offset = offsetAndMetadata(37)

    val validOffsetCommitResult = commitOffsets(groupId, joinGroupResult.memberId, joinGroupResult.generationId,
      Map(ti1p0 -> offset, ti2p0 -> offset))
    assertEquals(Map(ti1p0 -> Errors.NONE, ti2p0 -> Errors.NONE), validOffsetCommitResult)

    // and leaves.
    val leaveGroupResults = singleLeaveGroup(groupId, joinGroupResult.memberId)
    verifyLeaveGroupResult(leaveGroupResults)

    assertTrue(groupCoordinator.groupManager.getGroup(groupId).exists(_.is(Empty)))

    val groupTopicPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupPartitionId)
    val partition: Partition = mock(classOf[Partition])

    when(replicaManager.getMagic(any[TopicPartition])).thenReturn(Some(RecordBatch.CURRENT_MAGIC_VALUE))
    when(replicaManager.getPartition(groupTopicPartition)).thenReturn(HostedPartition.Online(partition))
    when(replicaManager.onlinePartition(groupTopicPartition)).thenReturn(Some(partition))

    val (groupError, topics) = groupCoordinator.handleDeleteOffsets(groupId, Seq(ti1p0.topicPartition),
      RequestLocal.NoCaching)

    assertEquals(Errors.NONE, groupError)
    assertEquals(1, topics.size)
    assertEquals(Some(Errors.NONE), topics.get(ti1p0.topicPartition))

    val cachedOffsets = groupCoordinator.groupManager.getOffsets(groupId, requireStable, Some(Seq(ti1p0.topicPartition, ti2p0.topicPartition)))

    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(ti1p0.topicPartition).map(_.offset))
    assertEquals(Some(offset.offset), cachedOffsets.get(ti2p0.topicPartition).map(_.offset))
  }

  @Test
  def testDeleteOffsetOfStableConsumerGroup(): Unit = {
    // join the group
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val subscription = new Subscription(List("bar").asJava)

    val joinGroupResult = dynamicJoinGroup(groupId, memberId, protocolType,
      List(("protocol", ConsumerProtocol.serializeSubscription(subscription).array())))
    assertEquals(Errors.NONE, joinGroupResult.error)

    val syncGroupResult = syncGroupLeader(groupId, joinGroupResult.generationId, joinGroupResult.leaderId, Map.empty)
    assertEquals(Errors.NONE, syncGroupResult.error)

    val ti1p0 = new TopicIdPartition(Uuid.randomUuid(), 0, "foo")
    val ti2p0 = new TopicIdPartition(Uuid.randomUuid(), 0, "bar")
    val offset = offsetAndMetadata(37)

    val validOffsetCommitResult = commitOffsets(groupId, joinGroupResult.memberId, joinGroupResult.generationId,
      Map(ti1p0 -> offset, ti2p0 -> offset))
    assertEquals(Map(ti1p0 -> Errors.NONE, ti2p0 -> Errors.NONE), validOffsetCommitResult)

    assertTrue(groupCoordinator.groupManager.getGroup(groupId).exists(_.is(Stable)))

    val groupTopicPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupPartitionId)
    val partition: Partition = mock(classOf[Partition])

    when(replicaManager.getMagic(any[TopicPartition])).thenReturn(Some(RecordBatch.CURRENT_MAGIC_VALUE))
    when(replicaManager.getPartition(groupTopicPartition)).thenReturn(HostedPartition.Online(partition))
    when(replicaManager.onlinePartition(groupTopicPartition)).thenReturn(Some(partition))

    val (groupError, topics) = groupCoordinator.handleDeleteOffsets(groupId, Seq(ti1p0.topicPartition, ti2p0.topicPartition),
      RequestLocal.NoCaching)

    assertEquals(Errors.NONE, groupError)
    assertEquals(2, topics.size)
    assertEquals(Some(Errors.NONE), topics.get(ti1p0.topicPartition))
    assertEquals(Some(Errors.GROUP_SUBSCRIBED_TO_TOPIC), topics.get(ti2p0.topicPartition))

    val cachedOffsets = groupCoordinator.groupManager.getOffsets(groupId, requireStable, Some(Seq(ti1p0.topicPartition, ti2p0.topicPartition)))

    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(ti1p0.topicPartition).map(_.offset))
    assertEquals(Some(offset.offset), cachedOffsets.get(ti2p0.topicPartition).map(_.offset))
  }

  @Test
  def shouldDelayInitialRebalanceByGroupInitialRebalanceDelayOnEmptyGroup(): Unit = {
    val firstJoinFuture = sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols)
    timer.advanceClock(GroupInitialRebalanceDelay / 2)
    verifyDelayedTaskNotCompleted(firstJoinFuture)
    timer.advanceClock((GroupInitialRebalanceDelay / 2) + 1)
    val joinGroupResult = await(firstJoinFuture, 1)
    assertEquals(Errors.NONE, joinGroupResult.error)
  }

  private def verifyDelayedTaskNotCompleted(firstJoinFuture: Future[JoinGroupResult]) = {
    assertThrows(classOf[TimeoutException], () => await(firstJoinFuture, 1),
      () => "should have timed out as rebalance delay not expired")
  }

  @Test
  def shouldResetRebalanceDelayWhenNewMemberJoinsGroupInInitialRebalance(): Unit = {
    val rebalanceTimeout = GroupInitialRebalanceDelay * 3
    val firstMemberJoinFuture = sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols, rebalanceTimeout = rebalanceTimeout)
    timer.advanceClock(GroupInitialRebalanceDelay - 1)
    val secondMemberJoinFuture = sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols, rebalanceTimeout = rebalanceTimeout)
    timer.advanceClock(2)

    // advance past initial rebalance delay and make sure that tasks
    // haven't been completed
    timer.advanceClock(GroupInitialRebalanceDelay / 2 + 1)
    verifyDelayedTaskNotCompleted(firstMemberJoinFuture)
    verifyDelayedTaskNotCompleted(secondMemberJoinFuture)
    // advance clock beyond updated delay and make sure the
    // tasks have completed
    timer.advanceClock(GroupInitialRebalanceDelay / 2)
    val firstResult = await(firstMemberJoinFuture, 1)
    val secondResult = await(secondMemberJoinFuture, 1)
    assertEquals(Errors.NONE, firstResult.error)
    assertEquals(Errors.NONE, secondResult.error)
  }

  @Test
  def shouldDelayRebalanceUptoRebalanceTimeout(): Unit = {
    val rebalanceTimeout = GroupInitialRebalanceDelay * 2
    val firstMemberJoinFuture = sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols, rebalanceTimeout = rebalanceTimeout)
    val secondMemberJoinFuture = sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols, rebalanceTimeout = rebalanceTimeout)
    timer.advanceClock(GroupInitialRebalanceDelay + 1)
    val thirdMemberJoinFuture = sendJoinGroup(groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, protocols, rebalanceTimeout = rebalanceTimeout)
    timer.advanceClock(GroupInitialRebalanceDelay)

    verifyDelayedTaskNotCompleted(firstMemberJoinFuture)
    verifyDelayedTaskNotCompleted(secondMemberJoinFuture)
    verifyDelayedTaskNotCompleted(thirdMemberJoinFuture)

    // advance clock beyond rebalanceTimeout
    timer.advanceClock(1)

    val firstResult = await(firstMemberJoinFuture, 1)
    val secondResult = await(secondMemberJoinFuture, 1)
    val thirdResult = await(thirdMemberJoinFuture, 1)
    assertEquals(Errors.NONE, firstResult.error)
    assertEquals(Errors.NONE, secondResult.error)
    assertEquals(Errors.NONE, thirdResult.error)
  }

  @Test
  def testCompleteHeartbeatWithGroupDead(): Unit = {
    val rebalanceResult = staticMembersJoinAndRebalance(leaderInstanceId, followerInstanceId)
    heartbeat(groupId, rebalanceResult.leaderId, rebalanceResult.generation)
    val group = getGroup(groupId)
    group.transitionTo(Dead)
    val leaderMemberId = rebalanceResult.leaderId
    assertTrue(groupCoordinator.tryCompleteHeartbeat(group, leaderMemberId, false, () => true))
    groupCoordinator.onExpireHeartbeat(group, leaderMemberId, false)
    assertTrue(group.has(leaderMemberId))
  }

  @Test
  def testCompleteHeartbeatWithMemberAlreadyRemoved(): Unit = {
    val rebalanceResult = staticMembersJoinAndRebalance(leaderInstanceId, followerInstanceId)
    heartbeat(groupId, rebalanceResult.leaderId, rebalanceResult.generation)
    val group = getGroup(groupId)
    val leaderMemberId = rebalanceResult.leaderId
    group.remove(leaderMemberId)
    assertTrue(groupCoordinator.tryCompleteHeartbeat(group, leaderMemberId, false, () => true))
  }

  @Test
  def testVerificationErrorsForTxnOffsetCommits(): Unit = {
    val tip1 = new TopicIdPartition(Uuid.randomUuid(), 0, "topic-1")
    val offset1 = offsetAndMetadata(0)
    val tip2 = new TopicIdPartition(Uuid.randomUuid(), 0, "topic-2")
    val offset2 = offsetAndMetadata(0)
    val producerId = 1000L
    val producerEpoch: Short = 2

    def verifyErrors(error: Errors, expectedError: Errors): Unit = {
      val commitOffsetResult = commitTransactionalOffsets(groupId,
        producerId,
        producerEpoch,
        Map(tip1 -> offset1, tip2 -> offset2),
        verificationError = error)
      assertEquals(expectedError, commitOffsetResult(tip1))
      assertEquals(expectedError, commitOffsetResult(tip2))
    }

    verifyErrors(Errors.INVALID_PRODUCER_ID_MAPPING, Errors.INVALID_PRODUCER_ID_MAPPING)
    verifyErrors(Errors.INVALID_TXN_STATE, Errors.INVALID_TXN_STATE)
    verifyErrors(Errors.NETWORK_EXCEPTION, Errors.COORDINATOR_LOAD_IN_PROGRESS)
    verifyErrors(Errors.NOT_ENOUGH_REPLICAS, Errors.COORDINATOR_NOT_AVAILABLE)
    verifyErrors(Errors.NOT_LEADER_OR_FOLLOWER, Errors.NOT_COORDINATOR)
    verifyErrors(Errors.KAFKA_STORAGE_ERROR, Errors.NOT_COORDINATOR)
  }

  @Test
  def testTxnOffsetMetadataTooLarge(): Unit = {
    val tip = new TopicIdPartition(Uuid.randomUuid(), 0, "foo")
    val offset = 37
    val producerId = 100L
    val producerEpoch: Short = 3

    val offsets = Map(
      tip -> OffsetAndMetadata(offset, "s" * (GroupCoordinatorConfig.OFFSET_METADATA_MAX_SIZE_DEFAULT + 1), 0)
    )

    val commitOffsetResult = commitTransactionalOffsets(groupId, producerId, producerEpoch, offsets)
    assertEquals(Map(tip -> Errors.OFFSET_METADATA_TOO_LARGE), commitOffsetResult)
  }

  private def getGroup(groupId: String): GroupMetadata = {
    val groupOpt = groupCoordinator.groupManager.getGroup(groupId)
    assertTrue(groupOpt.isDefined)
    groupOpt.get
  }

  private def setupJoinGroupCallback: (Future[JoinGroupResult], JoinGroupCallback) = {
    val responsePromise = Promise[JoinGroupResult]()
    val responseFuture = responsePromise.future
    val responseCallback: JoinGroupCallback = responsePromise.success
    (responseFuture, responseCallback)
  }

  private def setupSyncGroupCallback: (Future[SyncGroupResult], SyncGroupCallback) = {
    val responsePromise = Promise[SyncGroupResult]()
    val responseFuture = responsePromise.future
    val responseCallback: SyncGroupCallback = responsePromise.success
    (responseFuture, responseCallback)
  }

  private def setupHeartbeatCallback: (Future[HeartbeatCallbackParams], HeartbeatCallback) = {
    val responsePromise = Promise[HeartbeatCallbackParams]()
    val responseFuture = responsePromise.future
    val responseCallback: HeartbeatCallback = error => responsePromise.success(error)
    (responseFuture, responseCallback)
  }

  private def setupCommitOffsetsCallback: (Future[CommitOffsetCallbackParams], CommitOffsetCallback) = {
    val responsePromise = Promise[CommitOffsetCallbackParams]()
    val responseFuture = responsePromise.future
    val responseCallback: CommitOffsetCallback = offsets => responsePromise.success(offsets)
    (responseFuture, responseCallback)
  }

  private def setupLeaveGroupCallback: (Future[LeaveGroupResult], LeaveGroupCallback) = {
    val responsePromise = Promise[LeaveGroupResult]()
    val responseFuture = responsePromise.future
    val responseCallback: LeaveGroupCallback = result => responsePromise.success(result)
    (responseFuture, responseCallback)
  }

  private def sendJoinGroup(groupId: String,
                            memberId: String,
                            protocolType: String,
                            protocols: List[(String, Array[Byte])],
                            groupInstanceId: Option[String] = None,
                            sessionTimeout: Int = DefaultSessionTimeout,
                            rebalanceTimeout: Int = DefaultRebalanceTimeout,
                            requireKnownMemberId: Boolean = false,
                            supportSkippingAssignment: Boolean = true): Future[JoinGroupResult] = {
    val (responseFuture, responseCallback) = setupJoinGroupCallback

    when(replicaManager.getMagic(any[TopicPartition])).thenReturn(Some(RecordBatch.MAGIC_VALUE_V1))

    groupCoordinator.handleJoinGroup(groupId, memberId, groupInstanceId, requireKnownMemberId, supportSkippingAssignment,
      "clientId", "clientHost", rebalanceTimeout, sessionTimeout, protocolType, protocols, responseCallback)
    responseFuture
  }

  private def sendStaticJoinGroupWithPersistence(groupId: String,
                                                 memberId: String,
                                                 protocolType: String,
                                                 protocols: List[(String, Array[Byte])],
                                                 groupInstanceId: String,
                                                 sessionTimeout: Int,
                                                 rebalanceTimeout: Int,
                                                 appendRecordError: Errors,
                                                 requireKnownMemberId: Boolean = false,
                                                 supportSkippingAssignment: Boolean): Future[JoinGroupResult] = {
    val (responseFuture, responseCallback) = setupJoinGroupCallback

    val capturedArgument: ArgumentCaptor[scala.collection.Map[TopicPartition, PartitionResponse] => Unit] = ArgumentCaptor.forClass(classOf[scala.collection.Map[TopicPartition, PartitionResponse] => Unit])

    when(replicaManager.appendRecords(anyLong,
      anyShort(),
      internalTopicsAllowed = ArgumentMatchers.eq(true),
      origin = ArgumentMatchers.eq(AppendOrigin.COORDINATOR),
      any[Map[TopicPartition, MemoryRecords]],
      capturedArgument.capture(),
      any[Option[ReentrantLock]],
      any(),
      any(classOf[RequestLocal]),
      any[ActionQueue],
      any[Map[TopicPartition, VerificationGuard]]
    )).thenAnswer(_ => {
      capturedArgument.getValue.apply(
        Map(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupPartitionId) ->
          new PartitionResponse(appendRecordError, 0L, RecordBatch.NO_TIMESTAMP, 0L)
       )
      )
    })
    when(replicaManager.getMagic(any[TopicPartition])).thenReturn(Some(RecordBatch.MAGIC_VALUE_V1))

    groupCoordinator.handleJoinGroup(groupId, memberId, Some(groupInstanceId), requireKnownMemberId, supportSkippingAssignment,
      "clientId", "clientHost", rebalanceTimeout, sessionTimeout, protocolType, protocols, responseCallback)
    responseFuture
  }

  private def sendSyncGroupLeader(groupId: String,
                                  generation: Int,
                                  leaderId: String,
                                  protocolType: Option[String],
                                  protocolName: Option[String],
                                  groupInstanceId: Option[String],
                                  assignment: Map[String, Array[Byte]]): Future[SyncGroupResult] = {
    val (responseFuture, responseCallback) = setupSyncGroupCallback

    val capturedArgument: ArgumentCaptor[scala.collection.Map[TopicPartition, PartitionResponse] => Unit] = ArgumentCaptor.forClass(classOf[scala.collection.Map[TopicPartition, PartitionResponse] => Unit])

    when(replicaManager.appendRecords(anyLong,
      anyShort(),
      internalTopicsAllowed = ArgumentMatchers.eq(true),
      origin = ArgumentMatchers.eq(AppendOrigin.COORDINATOR),
      any[Map[TopicPartition, MemoryRecords]],
      capturedArgument.capture(),
      any[Option[ReentrantLock]],
      any(),
      any(classOf[RequestLocal]),
      any[ActionQueue],
      any[Map[TopicPartition, VerificationGuard]]
    )).thenAnswer(_ => {
        capturedArgument.getValue.apply(
          Map(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupPartitionId) ->
            new PartitionResponse(Errors.NONE, 0L, RecordBatch.NO_TIMESTAMP, 0L)
          )
        )
      }
    )
    when(replicaManager.getMagic(any[TopicPartition])).thenReturn(Some(RecordBatch.MAGIC_VALUE_V1))

    groupCoordinator.handleSyncGroup(groupId, generation, leaderId, protocolType, protocolName,
      groupInstanceId, assignment, responseCallback)
    responseFuture
  }

  private def sendSyncGroupFollower(groupId: String,
                                    generation: Int,
                                    memberId: String,
                                    prototolType: Option[String] = None,
                                    prototolName: Option[String] = None,
                                    groupInstanceId: Option[String] = None): Future[SyncGroupResult] = {
    val (responseFuture, responseCallback) = setupSyncGroupCallback


    groupCoordinator.handleSyncGroup(groupId, generation, memberId,
      prototolType, prototolName, groupInstanceId, Map.empty[String, Array[Byte]], responseCallback)
    responseFuture
  }

  private def dynamicJoinGroup(groupId: String,
                               memberId: String,
                               protocolType: String,
                               protocols: List[(String, Array[Byte])],
                               sessionTimeout: Int = DefaultSessionTimeout,
                               rebalanceTimeout: Int = DefaultRebalanceTimeout): JoinGroupResult = {
    val requireKnownMemberId = true
    var responseFuture = sendJoinGroup(groupId, memberId, protocolType, protocols, None, sessionTimeout, rebalanceTimeout, requireKnownMemberId)

    // Since member id is required, we need another bounce to get the successful join group result.
    if (memberId == JoinGroupRequest.UNKNOWN_MEMBER_ID && requireKnownMemberId) {
      val joinGroupResult = Await.result(responseFuture, Duration(rebalanceTimeout + 100, TimeUnit.MILLISECONDS))
      // If some other error is triggered, return the error immediately for caller to handle.
      if (joinGroupResult.error != Errors.MEMBER_ID_REQUIRED) {
        return joinGroupResult
      }
      responseFuture = sendJoinGroup(groupId, joinGroupResult.memberId, protocolType, protocols, None, sessionTimeout, rebalanceTimeout, requireKnownMemberId)
    }
    timer.advanceClock(GroupInitialRebalanceDelay + 1)
    // should only have to wait as long as session timeout, but allow some extra time in case of an unexpected delay
    Await.result(responseFuture, Duration(rebalanceTimeout + 100, TimeUnit.MILLISECONDS))
  }

  private def staticJoinGroup(groupId: String,
                              memberId: String,
                              groupInstanceId: String,
                              protocolType: String,
                              protocols: List[(String, Array[Byte])],
                              clockAdvance: Int = GroupInitialRebalanceDelay + 1,
                              sessionTimeout: Int = DefaultSessionTimeout,
                              rebalanceTimeout: Int = DefaultRebalanceTimeout,
                              supportSkippingAssignment: Boolean = true): JoinGroupResult = {
    val responseFuture = sendJoinGroup(groupId, memberId, protocolType, protocols, Some(groupInstanceId), sessionTimeout, rebalanceTimeout,
      supportSkippingAssignment = supportSkippingAssignment)

    timer.advanceClock(clockAdvance)
    // should only have to wait as long as session timeout, but allow some extra time in case of an unexpected delay
    Await.result(responseFuture, Duration(rebalanceTimeout + 100, TimeUnit.MILLISECONDS))
  }

  private def staticJoinGroupWithPersistence(groupId: String,
                                             memberId: String,
                                             groupInstanceId: String,
                                             protocolType: String,
                                             protocols: List[(String, Array[Byte])],
                                             clockAdvance: Int,
                                             sessionTimeout: Int = DefaultSessionTimeout,
                                             rebalanceTimeout: Int = DefaultRebalanceTimeout,
                                             appendRecordError: Errors = Errors.NONE,
                                             supportSkippingAssignment: Boolean = true): JoinGroupResult = {
    val responseFuture = sendStaticJoinGroupWithPersistence(groupId, memberId, protocolType, protocols,
      groupInstanceId, sessionTimeout, rebalanceTimeout, appendRecordError, supportSkippingAssignment = supportSkippingAssignment)

    timer.advanceClock(clockAdvance)
    // should only have to wait as long as session timeout, but allow some extra time in case of an unexpected delay
    Await.result(responseFuture, Duration(rebalanceTimeout + 100, TimeUnit.MILLISECONDS))
  }

  private def syncGroupFollower(groupId: String,
                                generationId: Int,
                                memberId: String,
                                protocolType: Option[String] = None,
                                protocolName: Option[String] = None,
                                groupInstanceId: Option[String] = None,
                                sessionTimeout: Int = DefaultSessionTimeout): SyncGroupResult = {
    val responseFuture = sendSyncGroupFollower(groupId, generationId, memberId, protocolType,
      protocolName, groupInstanceId)
    Await.result(responseFuture, Duration(sessionTimeout + 100, TimeUnit.MILLISECONDS))
  }

  private def syncGroupLeader(groupId: String,
                              generationId: Int,
                              memberId: String,
                              assignment: Map[String, Array[Byte]],
                              protocolType: Option[String] = None,
                              protocolName: Option[String] = None,
                              groupInstanceId: Option[String] = None,
                              sessionTimeout: Int = DefaultSessionTimeout): SyncGroupResult = {
    val responseFuture = sendSyncGroupLeader(groupId, generationId, memberId, protocolType,
      protocolName, groupInstanceId, assignment)
    Await.result(responseFuture, Duration(sessionTimeout + 100, TimeUnit.MILLISECONDS))
  }

  private def heartbeat(groupId: String,
                        consumerId: String,
                        generationId: Int,
                        groupInstanceId: Option[String] = None): HeartbeatCallbackParams = {
    val (responseFuture, responseCallback) = setupHeartbeatCallback


    groupCoordinator.handleHeartbeat(groupId, consumerId, groupInstanceId, generationId, responseCallback)
    Await.result(responseFuture, Duration(40, TimeUnit.MILLISECONDS))
  }

  private def await[T](future: Future[T], millis: Long): T = {
    Await.result(future, Duration(millis, TimeUnit.MILLISECONDS))
  }

  private def commitOffsets(groupId: String,
                            memberId: String,
                            generationId: Int,
                            offsets: Map[TopicIdPartition, OffsetAndMetadata],
                            groupInstanceId: Option[String] = None): CommitOffsetCallbackParams = {
    val (responseFuture, responseCallback) = setupCommitOffsetsCallback

    val capturedArgument: ArgumentCaptor[scala.collection.Map[TopicPartition, PartitionResponse] => Unit] = ArgumentCaptor.forClass(classOf[scala.collection.Map[TopicPartition, PartitionResponse] => Unit])

    when(replicaManager.appendRecords(anyLong,
      anyShort(),
      internalTopicsAllowed = ArgumentMatchers.eq(true),
      origin = ArgumentMatchers.eq(AppendOrigin.COORDINATOR),
      any[Map[TopicPartition, MemoryRecords]],
      capturedArgument.capture(),
      any[Option[ReentrantLock]],
      any(),
      any(classOf[RequestLocal]),
      any[ActionQueue],
      any[Map[TopicPartition, VerificationGuard]]
    )).thenAnswer(_ => {
      capturedArgument.getValue.apply(
        Map(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupPartitionId) ->
          new PartitionResponse(Errors.NONE, 0L, RecordBatch.NO_TIMESTAMP, 0L)
        )
      )
    })
    when(replicaManager.getMagic(any[TopicPartition])).thenReturn(Some(RecordBatch.MAGIC_VALUE_V1))

    groupCoordinator.handleCommitOffsets(groupId, memberId, groupInstanceId, generationId, offsets, responseCallback)
    Await.result(responseFuture, Duration(40, TimeUnit.MILLISECONDS))
  }

  private def commitTransactionalOffsets(groupId: String,
                                         producerId: Long,
                                         producerEpoch: Short,
                                         offsets: Map[TopicIdPartition, OffsetAndMetadata],
                                         memberId: String = JoinGroupRequest.UNKNOWN_MEMBER_ID,
                                         groupInstanceId: Option[String] = Option.empty,
                                         generationId: Int = JoinGroupRequest.UNKNOWN_GENERATION_ID,
                                         verificationError: Errors = Errors.NONE): CommitOffsetCallbackParams = {
    val (responseFuture, responseCallback) = setupCommitOffsetsCallback

    val capturedArgument: ArgumentCaptor[scala.collection.Map[TopicPartition, PartitionResponse] => Unit] = ArgumentCaptor.forClass(classOf[scala.collection.Map[TopicPartition, PartitionResponse] => Unit])

    // Since transactional ID is only used for verification, we can use a dummy value. Ensure it passes through.
    val transactionalId = "dummy-txn-id"
    val offsetTopicPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupCoordinator.partitionFor(groupId))

    val postVerificationCallback: ArgumentCaptor[((Errors, VerificationGuard)) => Unit] =
      ArgumentCaptor.forClass(classOf[((Errors, VerificationGuard)) => Unit])

    // Transactional appends attempt to schedule to the request handler thread using
    // a non request handler thread. Set this to avoid error.
    KafkaRequestHandler.setBypassThreadCheck(true)

    when(replicaManager.maybeStartTransactionVerificationForPartition(
      ArgumentMatchers.eq(offsetTopicPartition),
      ArgumentMatchers.eq(transactionalId),
      ArgumentMatchers.eq(producerId),
      ArgumentMatchers.eq(producerEpoch),
      any(),
      postVerificationCallback.capture(),
      any()
    )).thenAnswer(
      _ => postVerificationCallback.getValue()((verificationError, VerificationGuard.SENTINEL))
    )
    when(replicaManager.appendRecords(anyLong,
      anyShort(),
      internalTopicsAllowed = ArgumentMatchers.eq(true),
      origin = ArgumentMatchers.eq(AppendOrigin.COORDINATOR),
      any[Map[TopicPartition, MemoryRecords]],
      capturedArgument.capture(),
      any[Option[ReentrantLock]],
      any(),
      any(classOf[RequestLocal]),
      any[ActionQueue],
      any[Map[TopicPartition, VerificationGuard]]
    )).thenAnswer(_ => {
      capturedArgument.getValue.apply(
        Map(offsetTopicPartition ->
          new PartitionResponse(Errors.NONE, 0L, RecordBatch.NO_TIMESTAMP, 0L)
        )
      )
    })
    when(replicaManager.getMagic(any[TopicPartition])).thenReturn(Some(RecordBatch.MAGIC_VALUE_V2))

    groupCoordinator.handleTxnCommitOffsets(groupId, transactionalId, producerId, producerEpoch,
      memberId, groupInstanceId, generationId, offsets, responseCallback, RequestLocal.NoCaching, ApiKeys.TXN_OFFSET_COMMIT.latestVersion())
    val result = Await.result(responseFuture, Duration(40, TimeUnit.MILLISECONDS))
    result
  }

  private def singleLeaveGroup(groupId: String,
                               consumerId: String,
                               groupInstanceId: Option[String] = None): LeaveGroupResult = {
    val singleMemberIdentity = List(
      new MemberIdentity()
        .setMemberId(consumerId)
        .setGroupInstanceId(groupInstanceId.orNull))
    batchLeaveGroup(groupId, singleMemberIdentity)
  }

  private def batchLeaveGroup(groupId: String,
                              memberIdentities: List[MemberIdentity]): LeaveGroupResult = {
    val (responseFuture, responseCallback) = setupLeaveGroupCallback

    when(replicaManager.getPartition(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupPartitionId)))
      .thenReturn(HostedPartition.None)
    when(replicaManager.getMagic(any[TopicPartition])).thenReturn(Some(RecordBatch.MAGIC_VALUE_V1))

    groupCoordinator.handleLeaveGroup(groupId, memberIdentities, responseCallback)
    Await.result(responseFuture, Duration(40, TimeUnit.MILLISECONDS))
  }

  def handleTxnCompletion(producerId: Long,
                          offsetsPartitions: Iterable[TopicPartition],
                          transactionResult: TransactionResult): Unit = {
    val isCommit = transactionResult == TransactionResult.COMMIT
    groupCoordinator.groupManager.handleTxnCompletion(producerId, offsetsPartitions.map(_.partition).toSet, isCommit)
  }

  private def offsetAndMetadata(offset: Long): OffsetAndMetadata = {
    OffsetAndMetadata(offset, "", timer.time.milliseconds())
  }
}

object GroupCoordinatorTest {
  def verifyLeaveGroupResult(leaveGroupResult: LeaveGroupResult,
                             expectedTopLevelError: Errors = Errors.NONE,
                             expectedMemberLevelErrors: List[Errors] = List.empty): Unit = {
    assertEquals(expectedTopLevelError, leaveGroupResult.topLevelError)
    if (expectedMemberLevelErrors.nonEmpty) {
      assertEquals(expectedMemberLevelErrors.size, leaveGroupResult.memberResponses.size)
      for (i <- expectedMemberLevelErrors.indices) {
            assertEquals(expectedMemberLevelErrors(i), leaveGroupResult.memberResponses(i).error)
          }
    }
  }
}
