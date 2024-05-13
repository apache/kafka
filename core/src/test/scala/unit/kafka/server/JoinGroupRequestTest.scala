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
package kafka.server

import kafka.test.ClusterInstance
import kafka.test.annotation.{ClusterConfigProperty, ClusterTest, Type}
import kafka.test.junit.ClusterTestExtensions
import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol
import org.apache.kafka.common.message.JoinGroupResponseData.JoinGroupResponseMember
import org.apache.kafka.common.message.{JoinGroupResponseData, SyncGroupRequestData}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.coordinator.group.classic.ClassicGroupState
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{Tag, Timeout}
import org.junit.jupiter.api.extension.ExtendWith

import java.util.Collections
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters._

@Timeout(120)
@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
@Tag("integration")
class JoinGroupRequestTest(cluster: ClusterInstance) extends GroupCoordinatorBaseRequestTest(cluster) {
  @ClusterTest(types = Array(Type.KRAFT), serverProperties = Array(
    new ClusterConfigProperty(key = "group.coordinator.new.enable", value = "true"),
    new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
    new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
  ))
  def testJoinGroupWithOldConsumerGroupProtocolAndNewGroupCoordinator(): Unit = {
    testJoinGroup()
  }

  @ClusterTest(serverProperties = Array(
    new ClusterConfigProperty(key = "group.coordinator.new.enable", value = "false"),
    new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
    new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
  ))
  def testJoinGroupWithOldConsumerGroupProtocolAndOldGroupCoordinator(): Unit = {
    testJoinGroup()
  }

  private def testJoinGroup(): Unit = {
    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    createOffsetsTopic()

    // Create the topic.
    createTopic(
      topic = "foo",
      numPartitions = 3
    )

    for (version <- ApiKeys.JOIN_GROUP.oldestVersion to ApiKeys.JOIN_GROUP.latestVersion(isUnstableApiEnabled)) {
      val metadata = ConsumerProtocol.serializeSubscription(
        new ConsumerPartitionAssignor.Subscription(Collections.singletonList("foo"))
      ).array

      // Join a dynamic member without member id.
      // Prior to JoinGroup version 4, a new member is immediately added if it sends a join group request with UNKNOWN_MEMBER_ID.
      val joinLeaderResponseData = sendJoinRequest(
        groupId = "grp",
        metadata = metadata,
        version = version.toShort
      )
      val leaderMemberId = joinLeaderResponseData.memberId
      if (version >= 4) {
        verifyJoinGroupResponseDataEquals(
          new JoinGroupResponseData()
            .setErrorCode(Errors.MEMBER_ID_REQUIRED.code)
            .setMemberId(leaderMemberId)
            .setProtocolName(if (version >= 7) null else ""),
          joinLeaderResponseData
        )
      } else {
        verifyJoinGroupResponseDataEquals(
          new JoinGroupResponseData()
            .setGenerationId(1)
            .setLeader(leaderMemberId)
            .setMemberId(leaderMemberId)
            .setProtocolName("consumer-range")
            .setMembers(List(new JoinGroupResponseMember()
              .setMemberId(leaderMemberId)
              .setMetadata(metadata)
            ).asJava),
          joinLeaderResponseData
        )
      }

      // Rejoin the group with the member id.
      if (version >= 4) {
        val rejoinLeaderResponseData = sendJoinRequest(
          groupId = "grp",
          memberId = leaderMemberId,
          metadata = metadata,
          version = version.toShort
        )
        verifyJoinGroupResponseDataEquals(
          new JoinGroupResponseData()
            .setGenerationId(1)
            .setMemberId(leaderMemberId)
            .setProtocolName("consumer-range")
            .setProtocolType(if (version >= 7) "consumer" else null)
            .setLeader(leaderMemberId)
            .setMembers(List(new JoinGroupResponseMember()
              .setMemberId(leaderMemberId)
              .setMetadata(metadata)
            ).asJava),
          rejoinLeaderResponseData
        )
      }

      // Send a SyncGroup request.
      syncGroupWithOldProtocol(
        groupId = "grp",
        memberId = leaderMemberId,
        generationId = 1,
        assignments = List(new SyncGroupRequestData.SyncGroupRequestAssignment()
          .setMemberId(leaderMemberId)
          .setAssignment(Array[Byte](1))
        ),
        expectedAssignment = Array[Byte](1)
      )

      // Join with an unknown member id.
      verifyJoinGroupResponseDataEquals(
        new JoinGroupResponseData()
          .setMemberId("member-id-unknown")
          .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code)
          .setProtocolName(if (version >= 7) null else ""),
        sendJoinRequest(
          groupId = "grp",
          memberId = "member-id-unknown",
          version = version.toShort
        )
      )

      // Join with an inconsistent protocolType.
      verifyJoinGroupResponseDataEquals(
        new JoinGroupResponseData()
          .setErrorCode(Errors.INCONSISTENT_GROUP_PROTOCOL.code)
          .setProtocolName(if (version >= 7) null else ""),
        sendJoinRequest(
          groupId = "grp",
          protocolType = "connect",
          version = version.toShort
        )
      )

      // Join a second member.
      // Non-null group instance id is not supported until JoinGroup version 5,
      // so only version 4 needs to join a dynamic group (version < 5) and needs an extra join request to get the member id (version > 3).
      var joinFollowerResponseData: JoinGroupResponseData = null
      if (version == 4) {
        joinFollowerResponseData = sendJoinRequest(
          groupId = "grp",
          metadata = metadata,
          version = version.toShort
        )
      }

      val joinFollowerFuture = Future {
        sendJoinRequest(
          groupId = "grp",
          memberId = if (version != 4) "" else joinFollowerResponseData.memberId,
          groupInstanceId = if (version >= 5) "group-instance-id" else null,
          metadata = metadata,
          version = version.toShort
        )
      }

      TestUtils.waitUntilTrue(() => {
        val described = describeGroups(groupIds = List("grp"))
        ClassicGroupState.PREPARING_REBALANCE.toString == described.head.groupState
      }, msg = s"The group is not in PREPARING_REBALANCE state.")

      // The leader rejoins.
      val rejoinLeaderResponseData = sendJoinRequest(
        groupId = "grp",
        memberId = leaderMemberId,
        metadata = metadata,
        version = version.toShort
      )

      val joinFollowerFutureResponseData = Await.result(joinFollowerFuture, Duration.Inf)
      var followerMemberId = joinFollowerFutureResponseData.memberId

      verifyJoinGroupResponseDataEquals(
        new JoinGroupResponseData()
          .setGenerationId(2)
          .setProtocolType(if (version >= 7) "consumer" else null)
          .setProtocolName("consumer-range")
          .setLeader(leaderMemberId)
          .setMemberId(followerMemberId),
        joinFollowerFutureResponseData
      )
      verifyJoinGroupResponseDataEquals(
        new JoinGroupResponseData()
          .setGenerationId(2)
          .setProtocolType(if (version >= 7) "consumer" else null)
          .setProtocolName("consumer-range")
          .setLeader(leaderMemberId)
          .setMemberId(leaderMemberId)
          .setMembers(List(
            new JoinGroupResponseMember()
              .setMemberId(leaderMemberId)
              .setMetadata(metadata),
            new JoinGroupResponseMember()
              .setMemberId(followerMemberId)
              .setGroupInstanceId(if (version >= 5) "group-instance-id" else null)
              .setMetadata(metadata)
          ).asJava),
        rejoinLeaderResponseData
      )

      // Sync the leader.
      syncGroupWithOldProtocol(
        groupId = "grp",
        memberId = leaderMemberId,
        generationId = rejoinLeaderResponseData.generationId,
        assignments = List(
          new SyncGroupRequestData.SyncGroupRequestAssignment()
            .setMemberId(leaderMemberId)
            .setAssignment(Array[Byte](1)),
          new SyncGroupRequestData.SyncGroupRequestAssignment()
            .setMemberId(followerMemberId)
            .setAssignment(Array[Byte](2))
        ),
        expectedAssignment = Array[Byte](1)
      )

      // Sync the follower.
      syncGroupWithOldProtocol(
        groupId = "grp",
        memberId = followerMemberId,
        generationId = joinFollowerFutureResponseData.generationId,
        expectedAssignment = Array[Byte](2)
      )

      // The follower rejoin doesn't trigger a rebalance if it's unchanged.
      verifyJoinGroupResponseDataEquals(
        new JoinGroupResponseData()
          .setGenerationId(2)
          .setProtocolType(if (version >= 7) "consumer" else null)
          .setProtocolName("consumer-range")
          .setLeader(leaderMemberId)
          .setMemberId(followerMemberId),
        sendJoinRequest(
          groupId = "grp",
          groupInstanceId = if (version >= 5) "group-instance-id" else null,
          memberId = followerMemberId,
          metadata = metadata,
          version = version.toShort
        )
      )

      if (version >= 5) {
        followerMemberId = testFencedStaticGroup(leaderMemberId, followerMemberId, metadata, version)
      }

      leaveGroup(
        groupId = "grp",
        memberId = leaderMemberId,
        useNewProtocol = false,
        version = ApiKeys.LEAVE_GROUP.latestVersion(isUnstableApiEnabled)
      )
      leaveGroup(
        groupId = "grp",
        memberId = followerMemberId,
        useNewProtocol = false,
        version = ApiKeys.LEAVE_GROUP.latestVersion(isUnstableApiEnabled)
      )

      deleteGroups(
        groupIds = List("grp"),
        expectedErrors = List(Errors.NONE),
        version = ApiKeys.DELETE_GROUPS.latestVersion(isUnstableApiEnabled)
      )
    }
  }

  private def testFencedStaticGroup(
    leaderMemberId: String,
    followerMemberId: String,
    metadata: Array[Byte],
    version: Int,
  ): String = {
    // The leader rejoins and triggers a rebalance.
    val rejoinLeaderFuture = Future {
      sendJoinRequest(
        groupId = "grp",
        memberId = leaderMemberId,
        metadata = metadata,
        version = version.toShort
      )
    }

    TestUtils.waitUntilTrue(() => {
      val described = describeGroups(groupIds = List("grp"))
      ClassicGroupState.PREPARING_REBALANCE.toString == described.head.groupState
    }, msg = s"The group is not in PREPARING_REBALANCE state.")

    // A new follower with duplicated group instance id joins.
    val joinNewFollowerResponseData = sendJoinRequest(
      groupId = "grp",
      groupInstanceId = "group-instance-id",
      metadata = metadata,
      version = version.toShort
    )

    TestUtils.waitUntilTrue(() => {
      val described = describeGroups(groupIds = List("grp"))
      ClassicGroupState.COMPLETING_REBALANCE.toString == described.head.groupState
    }, msg = s"The group is not in COMPLETING_REBALANCE state.")

    // The old follower rejoin request should be fenced.
    val rejoinFollowerResponseData = sendJoinRequest(
      groupId = "grp",
      memberId = followerMemberId,
      groupInstanceId = "group-instance-id",
      metadata = metadata,
      version = version.toShort
    )

    val rejoinLeaderFutureResponseData = Await.result(rejoinLeaderFuture, Duration.Inf)
    val newFollowerMemberId = joinNewFollowerResponseData.memberId

    verifyJoinGroupResponseDataEquals(
      new JoinGroupResponseData()
        .setGenerationId(3)
        .setProtocolType(if (version >= 7) "consumer" else null)
        .setProtocolName("consumer-range")
        .setLeader(leaderMemberId)
        .setMemberId(leaderMemberId)
        .setMembers(List(
          new JoinGroupResponseMember()
            .setMemberId(leaderMemberId)
            .setMetadata(metadata),
          new JoinGroupResponseMember()
            .setMemberId(newFollowerMemberId)
            .setGroupInstanceId("group-instance-id")
            .setMetadata(metadata)
        ).asJava),
      rejoinLeaderFutureResponseData
    )

    verifyJoinGroupResponseDataEquals(
      new JoinGroupResponseData()
        .setGenerationId(3)
        .setProtocolType(if (version >= 7) "consumer" else null)
        .setProtocolName("consumer-range")
        .setLeader(leaderMemberId)
        .setMemberId(newFollowerMemberId),
      joinNewFollowerResponseData
    )

    verifyJoinGroupResponseDataEquals(
      new JoinGroupResponseData()
        .setProtocolName(if (version >= 7) null else "")
        .setMemberId(followerMemberId)
        .setErrorCode(Errors.FENCED_INSTANCE_ID.code),
      rejoinFollowerResponseData
    )

    newFollowerMemberId
  }

  private def normalize(responseData: JoinGroupResponseData): JoinGroupResponseData = {
    val newResponseData = responseData.duplicate
    Collections.sort(newResponseData.members,
      (m1: JoinGroupResponseMember, m2: JoinGroupResponseMember) => m1.memberId.compareTo(m2.memberId)
    )
    newResponseData
  }

  private def verifyJoinGroupResponseDataEquals(
    expected: JoinGroupResponseData,
    actual: JoinGroupResponseData
  ): Unit = {
    assertEquals(normalize(expected), normalize(actual))
  }
}
