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

import kafka.server.RequestLocal
import kafka.utils.Implicits.MapExtensionMethods
import org.apache.kafka.common.message.{DeleteGroupsResponseData, DescribeGroupsResponseData, HeartbeatRequestData, HeartbeatResponseData, JoinGroupRequestData, JoinGroupResponseData, LeaveGroupRequestData, LeaveGroupResponseData, ListGroupsRequestData, ListGroupsResponseData, SyncGroupRequestData, SyncGroupResponseData}
import org.apache.kafka.common.requests.RequestContext
import org.apache.kafka.common.utils.BufferSupplier

import java.util
import java.util.concurrent.CompletableFuture
import scala.collection.immutable
import scala.jdk.CollectionConverters._

/**
 * GroupCoordinatorAdapter is a thin wrapper around kafka.coordinator.group.GroupCoordinator
 * that exposes the new org.apache.kafka.coordinator.group.GroupCoordinator interface.
 */
class GroupCoordinatorAdapter(
  val coordinator: GroupCoordinator
) extends org.apache.kafka.coordinator.group.GroupCoordinator {

  override def joinGroup(
    context: RequestContext,
    request: JoinGroupRequestData,
    bufferSupplier: BufferSupplier
  ): CompletableFuture[JoinGroupResponseData] = {
    val future = new CompletableFuture[JoinGroupResponseData]()

    def callback(joinResult: JoinGroupResult): Unit = {
      future.complete(new JoinGroupResponseData()
        .setErrorCode(joinResult.error.code)
        .setGenerationId(joinResult.generationId)
        .setProtocolType(joinResult.protocolType.orNull)
        .setProtocolName(joinResult.protocolName.orNull)
        .setLeader(joinResult.leaderId)
        .setSkipAssignment(joinResult.skipAssignment)
        .setMemberId(joinResult.memberId)
        .setMembers(joinResult.members.asJava)
      )
    }

    val groupInstanceId = Option(request.groupInstanceId)

    // Only return MEMBER_ID_REQUIRED error if joinGroupRequest version is >= 4
    // and groupInstanceId is configured to unknown.
    val requireKnownMemberId = context.apiVersion >= 4 && groupInstanceId.isEmpty

    val protocols = request.protocols.valuesList.asScala.map { protocol =>
      (protocol.name, protocol.metadata)
    }.toList

    val supportSkippingAssignment = context.apiVersion >= 9

    coordinator.handleJoinGroup(
      request.groupId,
      request.memberId,
      groupInstanceId,
      requireKnownMemberId,
      supportSkippingAssignment,
      context.clientId,
      context.clientAddress.toString,
      request.rebalanceTimeoutMs,
      request.sessionTimeoutMs,
      request.protocolType,
      protocols,
      callback,
      Option(request.reason),
      RequestLocal(bufferSupplier)
    )

    future
  }

  override def syncGroup(
    context: RequestContext,
    request: SyncGroupRequestData,
    bufferSupplier: BufferSupplier
  ): CompletableFuture[SyncGroupResponseData] = {
    val future = new CompletableFuture[SyncGroupResponseData]()

    def callback(syncGroupResult: SyncGroupResult): Unit = {
      future.complete(new SyncGroupResponseData()
        .setErrorCode(syncGroupResult.error.code)
        .setProtocolType(syncGroupResult.protocolType.orNull)
        .setProtocolName(syncGroupResult.protocolName.orNull)
        .setAssignment(syncGroupResult.memberAssignment)
      )
    }

    val assignmentMap = immutable.Map.newBuilder[String, Array[Byte]]
    request.assignments.forEach { assignment =>
      assignmentMap += assignment.memberId -> assignment.assignment
    }

    coordinator.handleSyncGroup(
      request.groupId,
      request.generationId,
      request.memberId,
      Option(request.protocolType),
      Option(request.protocolName),
      Option(request.groupInstanceId),
      assignmentMap.result(),
      callback,
      RequestLocal(bufferSupplier)
    )

    future
  }

  override def heartbeat(
    context: RequestContext,
    request: HeartbeatRequestData
  ): CompletableFuture[HeartbeatResponseData] = {
    val future = new CompletableFuture[HeartbeatResponseData]()

    coordinator.handleHeartbeat(
      request.groupId,
      request.memberId,
      Option(request.groupInstanceId),
      request.generationId,
      error => future.complete(new HeartbeatResponseData()
        .setErrorCode(error.code))
    )

    future
  }

  override def leaveGroup(
    context: RequestContext,
    request: LeaveGroupRequestData
  ): CompletableFuture[LeaveGroupResponseData] = {
    val future = new CompletableFuture[LeaveGroupResponseData]()

    def callback(leaveGroupResult: LeaveGroupResult): Unit = {
      future.complete(new LeaveGroupResponseData()
        .setErrorCode(leaveGroupResult.topLevelError.code)
        .setMembers(leaveGroupResult.memberResponses.map { member =>
          new LeaveGroupResponseData.MemberResponse()
            .setErrorCode(member.error.code)
            .setMemberId(member.memberId)
            .setGroupInstanceId(member.groupInstanceId.orNull)
        }.asJava)
      )
    }

    coordinator.handleLeaveGroup(
      request.groupId,
      request.members.asScala.toList,
      callback
    )

    future
  }

  override def listGroups(
    context: RequestContext,
    request: ListGroupsRequestData
  ): CompletableFuture[ListGroupsResponseData] = {
    // Handle a null array the same as empty
    val (error, groups) = coordinator.handleListGroups(
      Option(request.statesFilter).map(_.asScala.toSet).getOrElse(Set.empty)
    )

    val response = new ListGroupsResponseData()
      .setErrorCode(error.code)

    groups.foreach { group =>
      response.groups.add(new ListGroupsResponseData.ListedGroup()
        .setGroupId(group.groupId)
        .setProtocolType(group.protocolType)
        .setGroupState(group.state))
    }

    CompletableFuture.completedFuture(response)
  }

  override def describeGroups(
    context: RequestContext,
    groupIds: util.List[String]
  ): CompletableFuture[util.List[DescribeGroupsResponseData.DescribedGroup]] = {

    def describeGroup(groupId: String): DescribeGroupsResponseData.DescribedGroup = {
      val (error, summary) = coordinator.handleDescribeGroup(groupId)

      new DescribeGroupsResponseData.DescribedGroup()
        .setErrorCode(error.code)
        .setGroupId(groupId)
        .setGroupState(summary.state)
        .setProtocolType(summary.protocolType)
        .setProtocolData(summary.protocol)
        .setMembers(summary.members.map { member =>
          new DescribeGroupsResponseData.DescribedGroupMember()
            .setMemberId(member.memberId)
            .setGroupInstanceId(member.groupInstanceId.orNull)
            .setClientId(member.clientId)
            .setClientHost(member.clientHost)
            .setMemberAssignment(member.assignment)
            .setMemberMetadata(member.metadata)
        }.asJava)
    }

    CompletableFuture.completedFuture(groupIds.asScala.map(describeGroup).asJava)
  }

  override def deleteGroups(
    context: RequestContext,
    groupIds: util.List[String],
    bufferSupplier: BufferSupplier
  ): CompletableFuture[DeleteGroupsResponseData.DeletableGroupResultCollection] = {
    val results = new DeleteGroupsResponseData.DeletableGroupResultCollection()
    coordinator.handleDeleteGroups(
      groupIds.asScala.toSet,
      RequestLocal(bufferSupplier)
    ).forKeyValue { (groupId, error) =>
      results.add(new DeleteGroupsResponseData.DeletableGroupResult()
        .setGroupId(groupId)
        .setErrorCode(error.code))
    }
    CompletableFuture.completedFuture(results)
  }
}
