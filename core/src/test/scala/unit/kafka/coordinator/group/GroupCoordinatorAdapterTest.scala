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
import kafka.server.RequestLocal
import org.apache.kafka.common.message.{DeleteGroupsResponseData, DescribeGroupsResponseData, HeartbeatRequestData, HeartbeatResponseData, JoinGroupRequestData, JoinGroupResponseData, LeaveGroupRequestData, LeaveGroupResponseData, ListGroupsRequestData, ListGroupsResponseData, SyncGroupRequestData, SyncGroupResponseData}
import org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocol
import org.apache.kafka.common.message.JoinGroupResponseData.JoinGroupResponseMember
import org.apache.kafka.common.network.{ClientInformation, ListenerName}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{RequestContext, RequestHeader}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.utils.BufferSupplier
import org.apache.kafka.common.utils.annotation.ApiKeyVersionsSource
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.mockito.Mockito.{mock, verify, when}

import java.net.InetAddress
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

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.JOIN_GROUP)
  def testJoinGroup(version: Short): Unit = {
    val groupCoordinator = mock(classOf[GroupCoordinator])
    val adapter = new GroupCoordinatorAdapter(groupCoordinator)

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
      ArgumentMatchers.eq(RequestLocal(bufferSupplier))
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
    val adapter = new GroupCoordinatorAdapter(groupCoordinator)

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
      ArgumentMatchers.eq(RequestLocal(bufferSupplier))
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
    val adapter = new GroupCoordinatorAdapter(groupCoordinator)

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
    val adapter = new GroupCoordinatorAdapter(groupCoordinator)

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
    testListGroups(null, Set.empty)
    testListGroups(List(), Set.empty)
    testListGroups(List("Stable"), Set("Stable"))
  }

  def testListGroups(
    statesFilter: List[String],
    expectedStatesFilter: Set[String]
  ): Unit = {
    val groupCoordinator = mock(classOf[GroupCoordinator])
    val adapter = new GroupCoordinatorAdapter(groupCoordinator)

    val ctx = makeContext(ApiKeys.LIST_GROUPS, ApiKeys.LIST_GROUPS.latestVersion)
    val data = new ListGroupsRequestData()
      .setStatesFilter(statesFilter.asJava)

    when(groupCoordinator.handleListGroups(expectedStatesFilter)).thenReturn {
      (Errors.NOT_COORDINATOR, List(
        GroupOverview("group1", "protocol1", "Stable"),
        GroupOverview("group2", "qwerty", "Empty")
      ))
    }

    val future = adapter.listGroups(ctx, data)
    assertTrue(future.isDone)

    val expectedData = new ListGroupsResponseData()
      .setErrorCode(Errors.NOT_COORDINATOR.code)
      .setGroups(List(
        new ListGroupsResponseData.ListedGroup()
          .setGroupId("group1")
          .setGroupState("Stable")
          .setProtocolType("protocol1"),
        new ListGroupsResponseData.ListedGroup()
          .setGroupId("group2")
          .setGroupState("Empty")
          .setProtocolType("qwerty")
      ).asJava)

    assertEquals(expectedData, future.get())
  }

  @Test
  def testDescribeGroup(): Unit = {
    val groupCoordinator = mock(classOf[GroupCoordinator])
    val adapter = new GroupCoordinatorAdapter(groupCoordinator)

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

    when(groupCoordinator.handleDescribeGroup(groupId1)).thenReturn {
      (Errors.NONE, groupSummary1)
    }

    when(groupCoordinator.handleDescribeGroup(groupId2)).thenReturn {
      (Errors.NOT_COORDINATOR, GroupCoordinator.EmptyGroup)
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
    val adapter = new GroupCoordinatorAdapter(groupCoordinator)

    val ctx = makeContext(ApiKeys.DELETE_GROUPS, ApiKeys.DELETE_GROUPS.latestVersion)
    val groupIds = List("group-1", "group-2", "group-3")
    val bufferSupplier = BufferSupplier.create()

    when(groupCoordinator.handleDeleteGroups(
      groupIds.toSet,
      RequestLocal(bufferSupplier)
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
}
