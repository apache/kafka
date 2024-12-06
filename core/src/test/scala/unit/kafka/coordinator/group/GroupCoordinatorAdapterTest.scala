/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.coordinator.group

import kafka.coordinator.group.GroupCoordinatorConcurrencyTest.{JoinGroupCallback, SyncGroupCallback}
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.errors.{InvalidGroupIdException, UnsupportedVersionException}
import org.apache.kafka.common.message.{ConsumerGroupHeartbeatRequestData, DeleteGroupsResponseData, DescribeGroupsResponseData, HeartbeatRequestData, HeartbeatResponseData, JoinGroupRequestData, JoinGroupResponseData, LeaveGroupRequestData, LeaveGroupResponseData, ListGroupsRequestData, ListGroupsResponseData, OffsetCommitRequestData, OffsetCommitResponseData, OffsetDeleteRequestData, OffsetDeleteResponseData, OffsetFetchRequestData, OffsetFetchResponseData, ShareGroupHeartbeatRequestData, SyncGroupRequestData, SyncGroupResponseData, TxnOffsetCommitRequestData, TxnOffsetCommitResponseData}
import org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocol
import org.apache.kafka.common.message.JoinGroupResponseData.JoinGroupResponseMember
import org.apache.kafka.common.message.OffsetDeleteRequestData.{OffsetDeleteRequestPartition, OffsetDeleteRequestTopic, OffsetDeleteRequestTopicCollection}
import org.apache.kafka.common.message.OffsetDeleteResponseData.{OffsetDeleteResponsePartition, OffsetDeleteResponsePartitionCollection, OffsetDeleteResponseTopic, OffsetDeleteResponseTopicCollection}
import org.apache.kafka.common.network.{ClientInformation, ListenerName}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{OffsetFetchResponse, RequestContext, RequestHeader}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.utils.{BufferSupplier, Time}
import org.apache.kafka.common.utils.annotation.ApiKeyVersionsSource
import org.apache.kafka.coordinator.group.OffsetAndMetadata
import org.apache.kafka.server.common.RequestLocal
import org.apache.kafka.server.util.MockTime
import org.apache.kafka.test.TestUtils.assertFutureThrows
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.mockito.Mockito.{mock, verify, when}

import java.net.InetAddress
import java.util.{Optional, OptionalInt, OptionalLong}
import scala.jdk.CollectionConverters._

class GroupCoordinatorAdapterTest {

  private def makeContext(
    apiKey: ApiKeys,
    apiVersion: Short
  ): RequestContext = {
    new RequestContext(
      new RequestHeader(apiKey, apiVersion, "client", 0),
      "1",
      InetAddress.getLocalHost,
      KafkaPrincipal.ANONYMOUS,
      ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
      SecurityProtocol.PLAINTEXT,
      ClientInformation.EMPTY,
      false
    )
  }

  @Test
  def testJoinConsumerGroup(): Unit = {
    val groupCoordinator = mock(classOf[GroupCoordinator])
    val adapter = new GroupCoordinatorAdapter(groupCoordinator, Time.SYSTEM)

    val ctx = makeContext(ApiKeys.CONSUMER_GROUP_HEARTBEAT, ApiKeys.CONSUMER_GROUP_HEARTBEAT.latestVersion)
    val request = new ConsumerGroupHeartbeatRequestData()
      .setGroupId("group")

    val future = adapter.consumerGroupHeartbeat(ctx, request)

    assertTrue(future.isDone)
    assertTrue(future.isCompletedExceptionally)
    assertFutureThrows(future, classOf[UnsupportedVersionException])
  }

  @Test
  def testJoinShareGroup(): Unit = {
    val groupCoordinator = mock(classOf[GroupCoordinator])
    val adapter = new GroupCoordinatorAdapter(groupCoordinator, Time.SYSTEM)

    val ctx = makeContext(ApiKeys.SHARE_GROUP_HEARTBEAT, ApiKeys.SHARE_GROUP_HEARTBEAT.latestVersion)
    val request = new ShareGroupHeartbeatRequestData()
      .setGroupId("group")

    val future = adapter.shareGroupHeartbeat(ctx, request)

    assertTrue(future.isDone)
    assertTrue(future.isCompletedExceptionally)
    assertFutureThrows(future, classOf[UnsupportedVersionException])
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.JOIN_GROUP)
  def testJoinGroup(version: Short): Unit = {
    val groupCoordinator = mock(classOf[GroupCoordinator])
    val adapter = new GroupCoordinatorAdapter(groupCoordinator, Time.SYSTEM)

    val ctx = makeContext(ApiKeys.JOIN_GROUP, version)
    val request = new JoinGroupRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setProtocolType("consumer")
      .setRebalanceTimeoutMs(1000)
      .setSessionTimeoutMs(2000)
      .setReason("reason")
      .setProtocols(new JoinGroupRequestData.JoinGroupRequestProtocolCollection(List(
        new JoinGroupRequestProtocol()
          .setName("first")
          .setMetadata("first".getBytes()),
        new JoinGroupRequestProtocol()
          .setName("second")
          .setMetadata("second".getBytes())).iterator.asJava))
    val bufferSupplier = BufferSupplier.create()

    val future = adapter.joinGroup(ctx, request, bufferSupplier)
    assertFalse(future.isDone)

    val capturedProtocols: ArgumentCaptor[List[(String, Array[Byte])]] =
      ArgumentCaptor.forClass(classOf[List[(String, Array[Byte])]])
    val capturedCallback: ArgumentCaptor[JoinGroupCallback] =
      ArgumentCaptor.forClass(classOf[JoinGroupCallback])

    verify(groupCoordinator).handleJoinGroup(
      ArgumentMatchers.eq(request.groupId),
      ArgumentMatchers.eq(request.memberId),
      ArgumentMatchers.eq(None),
      ArgumentMatchers.eq(if (version >= 4) true else false),
      ArgumentMatchers.eq(if (version >= 9) true else false),
      ArgumentMatchers.eq(ctx.clientId),
      ArgumentMatchers.eq(InetAddress.getLocalHost.toString),
      ArgumentMatchers.eq(request.rebalanceTimeoutMs),
      ArgumentMatchers.eq(request.sessionTimeoutMs),
      ArgumentMatchers.eq(request.protocolType),
      capturedProtocols.capture(),
      capturedCallback.capture(),
      ArgumentMatchers.eq(Some("reason")),
      ArgumentMatchers.eq(new RequestLocal(bufferSupplier))
    )

    assertEquals(List(
      ("first", "first"),
      ("second", "second")
    ), capturedProtocols.getValue.map { case (name, metadata) =>
      (name, new String(metadata))
    })

    capturedCallback.getValue.apply(JoinGroupResult(
      members = List(
        new JoinGroupResponseMember()
          .setMemberId("member")
          .setMetadata("member".getBytes())
          .setGroupInstanceId("instance")
      ),
      memberId = "member",
      generationId = 10,
      protocolType = Some("consumer"),
      protocolName = Some("range"),
      leaderId = "leader",
      skipAssignment = true,
      error = Errors.UNKNOWN_MEMBER_ID
    ))

    val expectedData = new JoinGroupResponseData()
      .setMembers(List(new JoinGroupResponseMember()
        .setMemberId("member")
        .setMetadata("member".getBytes())
        .setGroupInstanceId("instance")).asJava)
      .setMemberId("member")
      .setGenerationId(10)
      .setProtocolType("consumer")
      .setProtocolName("range")
      .setLeader("leader")
      .setSkipAssignment(true)
      .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code)

    assertTrue(future.isDone)
    assertEquals(expectedData, future.get())
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.SYNC_GROUP)
  def testSyncGroup(version: Short): Unit = {
    val groupCoordinator = mock(classOf[GroupCoordinator])
    val adapter = new GroupCoordinatorAdapter(groupCoordinator, Time.SYSTEM)

    val ctx = makeContext(ApiKeys.SYNC_GROUP, version)
    val data = new SyncGroupRequestData()
      .setGroupId("group")
      .setMemberId("member1")
      .setGroupInstanceId("instance")
      .setProtocolType("consumer")
      .setProtocolName("range")
      .setGenerationId(10)
      .setAssignments(List(
        new SyncGroupRequestData.SyncGroupRequestAssignment()
          .setMemberId("member1")
          .setAssignment("member1".getBytes()),
        new SyncGroupRequestData.SyncGroupRequestAssignment()
          .setMemberId("member2")
          .setAssignment("member2".getBytes())
      ).asJava)
    val bufferSupplier = BufferSupplier.create()

    val future = adapter.syncGroup(ctx, data, bufferSupplier)
    assertFalse(future.isDone)

    val capturedAssignment: ArgumentCaptor[Map[String, Array[Byte]]] =
      ArgumentCaptor.forClass(classOf[Map[String, Array[Byte]]])
    val capturedCallback: ArgumentCaptor[SyncGroupCallback] =
      ArgumentCaptor.forClass(classOf[SyncGroupCallback])

    verify(groupCoordinator).handleSyncGroup(
      ArgumentMatchers.eq(data.groupId),
      ArgumentMatchers.eq(data.generationId),
      ArgumentMatchers.eq(data.memberId),
      ArgumentMatchers.eq(Some(data.protocolType)),
      ArgumentMatchers.eq(Some(data.protocolName)),
      ArgumentMatchers.eq(Some(data.groupInstanceId)),
      capturedAssignment.capture(),
      capturedCallback.capture(),
      ArgumentMatchers.eq(new RequestLocal(bufferSupplier))
    )

    assertEquals(Map(
      "member1" -> "member1",
      "member2" -> "member2",
    ), capturedAssignment.getValue.map { case (member, metadata) =>
      (member, new String(metadata))
    })

    capturedCallback.getValue.apply(SyncGroupResult(
      error = Errors.NONE,
      protocolType = Some("consumer"),
      protocolName = Some("range"),
      memberAssignment = "member1".getBytes()
    ))

    val expectedResponseData = new SyncGroupResponseData()
      .setErrorCode(Errors.NONE.code)
      .setProtocolType("consumer")
      .setProtocolName("range")
      .setAssignment("member1".getBytes())

    assertTrue(future.isDone)
    assertEquals(expectedResponseData, future.get())
  }

  @Test
  def testHeartbeat(): Unit = {
    val groupCoordinator = mock(classOf[GroupCoordinator])
    val adapter = new GroupCoordinatorAdapter(groupCoordinator, Time.SYSTEM)

    val ctx = makeContext(ApiKeys.HEARTBEAT, ApiKeys.HEARTBEAT.latestVersion)
    val data = new HeartbeatRequestData()
      .setGroupId("group")
      .setMemberId("member1")
      .setGenerationId(0)

    val future = adapter.heartbeat(ctx, data)

    val capturedCallback: ArgumentCaptor[Errors => Unit] =
      ArgumentCaptor.forClass(classOf[Errors => Unit])

    verify(groupCoordinator).handleHeartbeat(
      ArgumentMatchers.eq(data.groupId),
      ArgumentMatchers.eq(data.memberId),
      ArgumentMatchers.eq(None),
      ArgumentMatchers.eq(data.generationId),
      capturedCallback.capture(),
    )

    assertFalse(future.isDone)

    capturedCallback.getValue.apply(Errors.NONE)

    assertTrue(future.isDone)
    assertEquals(new HeartbeatResponseData(), future.get())
  }

  def testLeaveGroup(): Unit = {
    val groupCoordinator = mock(classOf[GroupCoordinator])
    val adapter = new GroupCoordinatorAdapter(groupCoordinator, Time.SYSTEM)

    val ctx = makeContext(ApiKeys.LEAVE_GROUP, ApiKeys.LEAVE_GROUP.latestVersion)
    val data = new LeaveGroupRequestData()
      .setGroupId("group")
      .setMembers(List(
        new LeaveGroupRequestData.MemberIdentity()
          .setMemberId("member-1")
          .setGroupInstanceId("instance-1"),
        new LeaveGroupRequestData.MemberIdentity()
          .setMemberId("member-2")
          .setGroupInstanceId("instance-2")
      ).asJava)

    val future = adapter.leaveGroup(ctx, data)

    val capturedCallback: ArgumentCaptor[LeaveGroupResult => Unit] =
      ArgumentCaptor.forClass(classOf[LeaveGroupResult => Unit])

    verify(groupCoordinator).handleLeaveGroup(
      ArgumentMatchers.eq(data.groupId),
      ArgumentMatchers.eq(data.members.asScala.toList),
      capturedCallback.capture(),
    )

    assertFalse(future.isDone)

    capturedCallback.getValue.apply(LeaveGroupResult(
      topLevelError = Errors.NONE,
      memberResponses = List(
        LeaveMemberResponse(
          memberId = "member-1",
          groupInstanceId = Some("instance-1"),
          error = Errors.NONE
        ),
        LeaveMemberResponse(
          memberId = "member-2",
          groupInstanceId = Some("instance-2"),
          error = Errors.NONE
        )
      )
    ))

    val expectedData = new LeaveGroupResponseData()
      .setMembers(List(
        new LeaveGroupResponseData.MemberResponse()
          .setMemberId("member-1")
          .setGroupInstanceId("instance-1"),
        new LeaveGroupResponseData.MemberResponse()
          .setMemberId("member-2")
          .setGroupInstanceId("instance-2")
      ).asJava)

    assertTrue(future.isDone)
    assertEquals(expectedData, future.get())
  }

  @Test
  def testListGroups(): Unit = {
    testListGroups(null, null, Set.empty, Set.empty)
    testListGroups(List(), List(), Set.empty, Set.empty)
    testListGroups(List("Stable, Empty"), List(), Set("Stable, Empty"), Set.empty)
    testListGroups(List(), List("classic"), Set.empty, Set("classic"))
  }

  def testListGroups(
    statesFilter: List[String],
    typesFilter: List[String],
    expectedStatesFilter: Set[String],
    expectedTypesFilter: Set[String]
  ): Unit = {
    val groupCoordinator = mock(classOf[GroupCoordinator])
    val adapter = new GroupCoordinatorAdapter(groupCoordinator, Time.SYSTEM)

    val ctx = makeContext(ApiKeys.LIST_GROUPS, ApiKeys.LIST_GROUPS.latestVersion)
    val data = new ListGroupsRequestData()
      .setStatesFilter(statesFilter.asJava)
      .setTypesFilter(typesFilter.asJava)

    when(groupCoordinator.handleListGroups(expectedStatesFilter, expectedTypesFilter)).thenReturn {
      (Errors.NOT_COORDINATOR, List(
        GroupOverview("group1", "protocol1", "Stable", "classic"),
        GroupOverview("group2", "qwerty", "Empty", "classic")
      ))
    }

    val future = adapter.listGroups(ctx, data)
    assertTrue(future.isDone)

    val expectedData = new ListGroupsResponseData()
      .setErrorCode(Errors.NOT_COORDINATOR.code)
      .setGroups(List(
        new ListGroupsResponseData.ListedGroup()
          .setGroupId("group1")
          .setProtocolType("protocol1")
          .setGroupState("Stable")
          .setGroupType("classic"),
        new ListGroupsResponseData.ListedGroup()
          .setGroupId("group2")
          .setProtocolType("qwerty")
          .setGroupState("Empty")
          .setGroupType("classic")
      ).asJava)

    assertTrue(future.isDone)
    assertEquals(expectedData, future.get())
  }

  @Test
  def testDescribeGroup(): Unit = {
    val groupCoordinator = mock(classOf[GroupCoordinator])
    val adapter = new GroupCoordinatorAdapter(groupCoordinator, Time.SYSTEM)

    val groupId1 = "group-1"
    val groupId2 = "group-2"

    val groupSummary1 = GroupSummary(
      "Stable",
      "consumer",
      "roundrobin",
      List(MemberSummary(
        "memberid",
        Some("instanceid"),
        "clientid",
        "clienthost",
        "metadata".getBytes(),
        "assignment".getBytes()
      ))
    )

    when(groupCoordinator.handleDescribeGroup(groupId1, ApiKeys.DESCRIBE_GROUPS.latestVersion)).thenReturn {
      (Errors.NONE, None, groupSummary1)
    }

    when(groupCoordinator.handleDescribeGroup(groupId2, ApiKeys.DESCRIBE_GROUPS.latestVersion)).thenReturn {
      (Errors.NOT_COORDINATOR, None, GroupCoordinator.EmptyGroup)
    }

    val ctx = makeContext(ApiKeys.DESCRIBE_GROUPS, ApiKeys.DESCRIBE_GROUPS.latestVersion)
    val future = adapter.describeGroups(ctx, List(groupId1, groupId2).asJava)
    assertTrue(future.isDone)

    val expectedDescribedGroups = List(
      new DescribeGroupsResponseData.DescribedGroup()
        .setGroupId(groupId1)
        .setErrorCode(Errors.NONE.code)
        .setProtocolType(groupSummary1.protocolType)
        .setProtocolData(groupSummary1.protocol)
        .setGroupState(groupSummary1.state)
        .setMembers(List(new DescribeGroupsResponseData.DescribedGroupMember()
          .setMemberId(groupSummary1.members.head.memberId)
          .setGroupInstanceId(groupSummary1.members.head.groupInstanceId.orNull)
          .setClientId(groupSummary1.members.head.clientId)
          .setClientHost(groupSummary1.members.head.clientHost)
          .setMemberMetadata(groupSummary1.members.head.metadata)
          .setMemberAssignment(groupSummary1.members.head.assignment)
        ).asJava),
      new DescribeGroupsResponseData.DescribedGroup()
        .setGroupId(groupId2)
        .setErrorCode(Errors.NOT_COORDINATOR.code)
    ).asJava

    assertEquals(expectedDescribedGroups, future.get())
  }

  @Test
  def testDeleteGroups(): Unit = {
    val groupCoordinator = mock(classOf[GroupCoordinator])
    val adapter = new GroupCoordinatorAdapter(groupCoordinator, Time.SYSTEM)

    val ctx = makeContext(ApiKeys.DELETE_GROUPS, ApiKeys.DELETE_GROUPS.latestVersion)
    val groupIds = List("group-1", "group-2", "group-3")
    val bufferSupplier = BufferSupplier.create()

    when(groupCoordinator.handleDeleteGroups(
      groupIds.toSet,
      new RequestLocal(bufferSupplier)
    )).thenReturn(Map(
      "group-1" -> Errors.NONE,
      "group-2" -> Errors.NOT_COORDINATOR,
      "group-3" -> Errors.INVALID_GROUP_ID,
    ))

    val future = adapter.deleteGroups(ctx, groupIds.asJava, bufferSupplier)
    assertTrue(future.isDone)

    val expectedResults = new DeleteGroupsResponseData.DeletableGroupResultCollection()
    expectedResults.add(new DeleteGroupsResponseData.DeletableGroupResult()
      .setGroupId("group-1")
      .setErrorCode(Errors.NONE.code))
    expectedResults.add(new DeleteGroupsResponseData.DeletableGroupResult()
      .setGroupId("group-2")
      .setErrorCode(Errors.NOT_COORDINATOR.code))
    expectedResults.add(new DeleteGroupsResponseData.DeletableGroupResult()
      .setGroupId("group-3")
      .setErrorCode(Errors.INVALID_GROUP_ID.code))

    assertEquals(expectedResults, future.get())
  }

  @Test
  def testFetchAllOffsets(): Unit = {
    val foo0 = new TopicPartition("foo", 0)
    val foo1 = new TopicPartition("foo", 1)
    val bar1 = new TopicPartition("bar", 1)

    val groupCoordinator = mock(classOf[GroupCoordinator])
    val adapter = new GroupCoordinatorAdapter(groupCoordinator, Time.SYSTEM)

    when(groupCoordinator.handleFetchOffsets(
      "group",
      requireStable = true,
      None
    )).thenReturn((
      Errors.NONE,
      Map(
        foo0 -> new OffsetFetchResponse.PartitionData(
          100,
          Optional.of(1),
          "foo",
          Errors.NONE
        ),
        bar1 -> new OffsetFetchResponse.PartitionData(
          -1,
          Optional.empty[Integer],
          "",
          Errors.UNKNOWN_TOPIC_OR_PARTITION
        ),
        foo1 -> new OffsetFetchResponse.PartitionData(
          200,
          Optional.empty[Integer],
          "",
          Errors.NONE
        ),
      )
    ))

    val ctx = makeContext(ApiKeys.OFFSET_FETCH, ApiKeys.OFFSET_FETCH.latestVersion)
    val future = adapter.fetchAllOffsets(
      ctx,
      new OffsetFetchRequestData.OffsetFetchRequestGroup().setGroupId("group"),
      requireStable = true
    )

    assertTrue(future.isDone)

    val expectedResponse = List(
      new OffsetFetchResponseData.OffsetFetchResponseTopics()
        .setName(foo0.topic)
        .setPartitions(List(
          new OffsetFetchResponseData.OffsetFetchResponsePartitions()
            .setPartitionIndex(foo0.partition)
            .setCommittedOffset(100)
            .setCommittedLeaderEpoch(1)
            .setMetadata("foo")
            .setErrorCode(Errors.NONE.code),
          new OffsetFetchResponseData.OffsetFetchResponsePartitions()
            .setPartitionIndex(foo1.partition)
            .setCommittedOffset(200)
            .setCommittedLeaderEpoch(-1)
            .setMetadata("")
            .setErrorCode(Errors.NONE.code),
        ).asJava),
      new OffsetFetchResponseData.OffsetFetchResponseTopics()
        .setName(bar1.topic)
        .setPartitions(List(
          new OffsetFetchResponseData.OffsetFetchResponsePartitions()
            .setPartitionIndex(bar1.partition)
            .setCommittedOffset(-1)
            .setCommittedLeaderEpoch(-1)
            .setMetadata("")
            .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
        ).asJava)
    )

    assertEquals("group", future.get().groupId)
    assertEquals(
      expectedResponse.sortWith(_.name > _.name),
      future.get().topics.asScala.toList.sortWith(_.name > _.name)
    )
  }

  @Test
  def testFetchOffsets(): Unit = {
    val foo0 = new TopicPartition("foo", 0)
    val foo1 = new TopicPartition("foo", 1)
    val bar1 = new TopicPartition("bar", 1)

    val groupCoordinator = mock(classOf[GroupCoordinator])
    val adapter = new GroupCoordinatorAdapter(groupCoordinator, Time.SYSTEM)

    when(groupCoordinator.handleFetchOffsets(
      "group",
      requireStable = true,
      Some(Seq(foo0, foo1, bar1))
    )).thenReturn((
      Errors.NONE,
      Map(
        foo0 -> new OffsetFetchResponse.PartitionData(
          100,
          Optional.of(1),
          "foo",
          Errors.NONE
        ),
        bar1 -> new OffsetFetchResponse.PartitionData(
          -1,
          Optional.empty[Integer],
          "",
          Errors.UNKNOWN_TOPIC_OR_PARTITION
        ),
        foo1 -> new OffsetFetchResponse.PartitionData(
          200,
          Optional.empty[Integer],
          "",
          Errors.NONE
        ),
      )
    ))

    val ctx = makeContext(ApiKeys.OFFSET_FETCH, ApiKeys.OFFSET_FETCH.latestVersion)
    val future = adapter.fetchOffsets(
      ctx,
      new OffsetFetchRequestData.OffsetFetchRequestGroup()
        .setGroupId("group")
        .setTopics(List(
          new OffsetFetchRequestData.OffsetFetchRequestTopics()
            .setName(foo0.topic)
            .setPartitionIndexes(List[Integer](foo0.partition, foo1.partition).asJava),
          new OffsetFetchRequestData.OffsetFetchRequestTopics()
            .setName(bar1.topic)
            .setPartitionIndexes(List[Integer](bar1.partition).asJava)).asJava),
      requireStable = true
    )

    assertTrue(future.isDone)

    val expectedResponse = List(
      new OffsetFetchResponseData.OffsetFetchResponseTopics()
        .setName(foo0.topic)
        .setPartitions(List(
          new OffsetFetchResponseData.OffsetFetchResponsePartitions()
            .setPartitionIndex(foo0.partition)
            .setCommittedOffset(100)
            .setCommittedLeaderEpoch(1)
            .setMetadata("foo")
            .setErrorCode(Errors.NONE.code),
          new OffsetFetchResponseData.OffsetFetchResponsePartitions()
            .setPartitionIndex(foo1.partition)
            .setCommittedOffset(200)
            .setCommittedLeaderEpoch(-1)
            .setMetadata("")
            .setErrorCode(Errors.NONE.code),
        ).asJava),
      new OffsetFetchResponseData.OffsetFetchResponseTopics()
        .setName(bar1.topic)
        .setPartitions(List(
          new OffsetFetchResponseData.OffsetFetchResponsePartitions()
            .setPartitionIndex(bar1.partition)
            .setCommittedOffset(-1)
            .setCommittedLeaderEpoch(-1)
            .setMetadata("")
            .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
        ).asJava)
    )

    assertEquals("group", future.get().groupId)
    assertEquals(
      expectedResponse.sortWith(_.name > _.name),
      future.get().topics.asScala.toList.sortWith(_.name > _.name)
    )
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_COMMIT)
  def testCommitOffsets(version: Short): Unit = {
    val groupCoordinator = mock(classOf[GroupCoordinator])
    val time = new MockTime()
    val adapter = new GroupCoordinatorAdapter(groupCoordinator, time)
    val now = time.milliseconds()

    val ctx = makeContext(ApiKeys.OFFSET_COMMIT, version)
    val data = new OffsetCommitRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setGenerationIdOrMemberEpoch(10)
      .setRetentionTimeMs(1000)
      .setTopics(List(
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName("foo")
          .setPartitions(List(
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(100)
              .setCommitTimestamp(now)
              .setCommittedLeaderEpoch(1)
          ).asJava)
      ).asJava)
    val bufferSupplier = BufferSupplier.create()

    val future = adapter.commitOffsets(ctx, data, bufferSupplier)
    assertFalse(future.isDone)

    val capturedCallback: ArgumentCaptor[Map[TopicIdPartition, Errors] => Unit] =
      ArgumentCaptor.forClass(classOf[Map[TopicIdPartition, Errors] => Unit])

    verify(groupCoordinator).handleCommitOffsets(
      ArgumentMatchers.eq(data.groupId),
      ArgumentMatchers.eq(data.memberId),
      ArgumentMatchers.eq(None),
      ArgumentMatchers.eq(data.generationIdOrMemberEpoch),
      ArgumentMatchers.eq(Map(
        new TopicIdPartition(Uuid.ZERO_UUID, 0 , "foo") -> new OffsetAndMetadata(
          100,
          OptionalInt.of(1),
          "",
          now,
          OptionalLong.of(now + 1000L)
        )
      )),
      capturedCallback.capture(),
      ArgumentMatchers.eq(new RequestLocal(bufferSupplier))
    )

    capturedCallback.getValue.apply(Map(
      new TopicIdPartition(Uuid.ZERO_UUID, 0 , "foo") -> Errors.NONE
    ))

    val expectedResponseData = new OffsetCommitResponseData()
      .setTopics(List(
        new OffsetCommitResponseData.OffsetCommitResponseTopic()
          .setName("foo")
          .setPartitions(List(
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code)
          ).asJava)
      ).asJava)

    assertTrue(future.isDone)
    assertEquals(expectedResponseData, future.get())
  }

  @Test
  def testCommitTransactionalOffsets(): Unit = {
    val groupCoordinator = mock(classOf[GroupCoordinator])
    val time = new MockTime()
    val adapter = new GroupCoordinatorAdapter(groupCoordinator, time)
    val now = time.milliseconds()

    val ctx = makeContext(ApiKeys.TXN_OFFSET_COMMIT, ApiKeys.TXN_OFFSET_COMMIT.latestVersion)
    val data = new TxnOffsetCommitRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setGenerationId(10)
      .setProducerEpoch(1)
      .setProducerId(2)
      .setTransactionalId("transaction-id")
      .setTopics(List(
        new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
          .setName("foo")
          .setPartitions(List(
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(100)
              .setCommittedLeaderEpoch(1)
          ).asJava)
      ).asJava)
    val bufferSupplier = BufferSupplier.create()

    val future = adapter.commitTransactionalOffsets(ctx, data, bufferSupplier)
    assertFalse(future.isDone)

    val capturedCallback: ArgumentCaptor[Map[TopicIdPartition, Errors] => Unit] =
      ArgumentCaptor.forClass(classOf[Map[TopicIdPartition, Errors] => Unit])

    verify(groupCoordinator).handleTxnCommitOffsets(
      ArgumentMatchers.eq(data.groupId),
      ArgumentMatchers.eq(data.transactionalId),
      ArgumentMatchers.eq(data.producerId),
      ArgumentMatchers.eq(data.producerEpoch),
      ArgumentMatchers.eq(data.memberId),
      ArgumentMatchers.eq(None),
      ArgumentMatchers.eq(data.generationId),
      ArgumentMatchers.eq(Map(
        new TopicIdPartition(Uuid.ZERO_UUID, 0 , "foo") -> new OffsetAndMetadata(
          100,
          OptionalInt.of(1),
          "",
          now,
          OptionalLong.empty()
        )
      )),
      capturedCallback.capture(),
      ArgumentMatchers.eq(new RequestLocal(bufferSupplier)),
      ArgumentMatchers.any()
    )

    capturedCallback.getValue.apply(Map(
      new TopicIdPartition(Uuid.ZERO_UUID, 0 , "foo") -> Errors.NONE
    ))

    val expectedData = new TxnOffsetCommitResponseData()
      .setTopics(List(
        new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
          .setName("foo")
          .setPartitions(List(
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code)
          ).asJava)
      ).asJava)

    assertTrue(future.isDone)
    assertEquals(expectedData, future.get())
  }

  def testDeleteOffsets(): Unit = {
    val groupCoordinator = mock(classOf[GroupCoordinator])
    val adapter = new GroupCoordinatorAdapter(groupCoordinator, Time.SYSTEM)

    val foo0 = new TopicPartition("foo", 0)
    val foo1 = new TopicPartition("foo", 1)
    val bar0 = new TopicPartition("bar", 0)
    val bar1 = new TopicPartition("bar", 1)

    val ctx = makeContext(ApiKeys.OFFSET_DELETE, ApiKeys.OFFSET_DELETE.latestVersion)
    val data = new OffsetDeleteRequestData()
      .setGroupId("group")
      .setTopics(new OffsetDeleteRequestTopicCollection(List(
        new OffsetDeleteRequestTopic()
          .setName("foo")
          .setPartitions(List(
            new OffsetDeleteRequestPartition().setPartitionIndex(0),
            new OffsetDeleteRequestPartition().setPartitionIndex(1)
          ).asJava),
        new OffsetDeleteRequestTopic()
          .setName("bar")
          .setPartitions(List(
            new OffsetDeleteRequestPartition().setPartitionIndex(0),
            new OffsetDeleteRequestPartition().setPartitionIndex(1)
          ).asJava)
      ).asJava.iterator))
    val bufferSupplier = BufferSupplier.create()

    when(groupCoordinator.handleDeleteOffsets(
      data.groupId,
      Seq(foo0, foo1, bar0, bar1),
      new RequestLocal(bufferSupplier)
    )).thenReturn((
      Errors.NONE,
      Map(
        foo0 -> Errors.NONE,
        foo1 -> Errors.NONE,
        bar0 -> Errors.GROUP_SUBSCRIBED_TO_TOPIC,
        bar1 -> Errors.GROUP_SUBSCRIBED_TO_TOPIC,
      )
    ))

    val future = adapter.deleteOffsets(ctx, data, bufferSupplier)

    val expectedData = new OffsetDeleteResponseData()
      .setTopics(new OffsetDeleteResponseTopicCollection(List(
        new OffsetDeleteResponseTopic()
          .setName("foo")
          .setPartitions(new OffsetDeleteResponsePartitionCollection(List(
            new OffsetDeleteResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new OffsetDeleteResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)
          ).asJava.iterator)),
        new OffsetDeleteResponseTopic()
          .setName("bar")
          .setPartitions(new OffsetDeleteResponsePartitionCollection(List(
            new OffsetDeleteResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.GROUP_SUBSCRIBED_TO_TOPIC.code),
            new OffsetDeleteResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.GROUP_SUBSCRIBED_TO_TOPIC.code)
          ).asJava.iterator)),
      ).asJava.iterator))

    assertTrue(future.isDone)
    assertEquals(expectedData, future.get())
  }

  @Test
  def testDeleteOffsetsWithGroupLevelError(): Unit = {
    val groupCoordinator = mock(classOf[GroupCoordinator])
    val adapter = new GroupCoordinatorAdapter(groupCoordinator, Time.SYSTEM)

    val foo0 = new TopicPartition("foo", 0)
    val foo1 = new TopicPartition("foo", 1)

    val ctx = makeContext(ApiKeys.OFFSET_DELETE, ApiKeys.OFFSET_DELETE.latestVersion)
    val data = new OffsetDeleteRequestData()
      .setGroupId("group")
      .setTopics(new OffsetDeleteRequestTopicCollection(List(
        new OffsetDeleteRequestTopic()
          .setName("foo")
          .setPartitions(List(
            new OffsetDeleteRequestPartition().setPartitionIndex(0),
            new OffsetDeleteRequestPartition().setPartitionIndex(1)
          ).asJava)
      ).asJava.iterator))
    val bufferSupplier = BufferSupplier.create()

    when(groupCoordinator.handleDeleteOffsets(
      data.groupId,
      Seq(foo0, foo1),
      new RequestLocal(bufferSupplier)
    )).thenReturn((Errors.INVALID_GROUP_ID, Map.empty[TopicPartition, Errors]))

    val future = adapter.deleteOffsets(ctx, data, bufferSupplier)
    assertTrue(future.isDone)
    assertTrue(future.isCompletedExceptionally)
    assertFutureThrows(future, classOf[InvalidGroupIdException])
  }

  @Test
  def testConsumerGroupDescribe(): Unit = {
    val groupCoordinator = mock(classOf[GroupCoordinator])
    val adapter = new GroupCoordinatorAdapter(groupCoordinator, Time.SYSTEM)
    val context = makeContext(ApiKeys.CONSUMER_GROUP_DESCRIBE, ApiKeys.CONSUMER_GROUP_DESCRIBE.latestVersion)
    val groupIds = List("group-id-1", "group-id-2").asJava

    val future = adapter.consumerGroupDescribe(context, groupIds)
    assertTrue(future.isDone)
    assertTrue(future.isCompletedExceptionally)
    assertFutureThrows(future, classOf[UnsupportedVersionException])
  }

  @Test
  def testShareGroupDescribe(): Unit = {
    val groupCoordinator = mock(classOf[GroupCoordinator])
    val adapter = new GroupCoordinatorAdapter(groupCoordinator, Time.SYSTEM)
    val context = makeContext(ApiKeys.SHARE_GROUP_DESCRIBE, ApiKeys.SHARE_GROUP_DESCRIBE.latestVersion)
    val groupIds = List("group-id-1", "group-id-2").asJava

    val future = adapter.shareGroupDescribe(context, groupIds)
    assertTrue(future.isDone)
    assertTrue(future.isCompletedExceptionally)
    assertFutureThrows(future, classOf[UnsupportedVersionException])
  }
}
