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
import kafka.test.annotation.{ClusterConfigProperty, ClusterTest, ClusterTestDefaults, Type}
import kafka.test.junit.ClusterTestExtensions
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol
import org.apache.kafka.common.message.JoinGroupResponseData.JoinGroupResponseMember
import org.apache.kafka.common.message.{JoinGroupResponseData, SyncGroupRequestData}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
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
@ClusterTestDefaults(clusterType = Type.KRAFT, brokers = 1)
@Tag("integration")
class JoinGroupRequestTest(cluster: ClusterInstance) extends GroupCoordinatorBaseRequestTest(cluster) {
  @ClusterTest(serverProperties = Array(
    new ClusterConfigProperty(key = "unstable.api.versions.enable", value = "false"),
    new ClusterConfigProperty(key = "group.coordinator.new.enable", value = "true"),
    new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
    new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
  ))
  def testJoinGroupWithOldConsumerGroupProtocolAndNewGroupCoordinator(): Unit = {
    testJoinGroup()
  }

  @ClusterTest(clusterType = Type.ALL, serverProperties = Array(
    new ClusterConfigProperty(key = "unstable.api.versions.enable", value = "false"),
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

    // Non-null group instance id is supported starting from version 5.
    for (version <- 5 to ApiKeys.JOIN_GROUP.latestVersion(isUnstableApiEnabled)) {
      val metadata = ConsumerProtocol.serializeSubscription(
        new ConsumerPartitionAssignor.Subscription(Collections.singletonList("foo"))
      ).array

      // Join a dynamic member.
      val joinLeaderResponseData = sendJoinRequest(
        groupId = "grp",
        metadata = metadata,
        version = Option(version.toShort)
      )
      val leaderMemberId = joinLeaderResponseData.memberId
      verifyJoinGroupResponseDataEquals(
        new JoinGroupResponseData()
          .setErrorCode(Errors.MEMBER_ID_REQUIRED.code)
          .setMemberId(leaderMemberId)
          .setProtocolName(if (version >= 7) null else ""),
        joinLeaderResponseData
      )

      // Rejoin the group with the member id.
      val rejoinLeaderResponseData = sendJoinRequest(
        groupId = "grp",
        memberId = joinLeaderResponseData.memberId,
        metadata = metadata,
        version = Option(version.toShort)
      )
      verifyJoinGroupResponseDataEquals(
        new JoinGroupResponseData()
          .setErrorCode(Errors.MEMBER_ID_REQUIRED.code)
          .setMemberId(leaderMemberId)
          .setProtocolName(if (version >= 7) null else ""),
        joinLeaderResponseData
      )

      // Send a SyncGroup request.
      syncGroupWithOldProtocol(
        groupId = "grp",
        memberId = leaderMemberId,
        generationId = rejoinLeaderResponseData.generationId,
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
          version = Option(version.toShort)
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
          version = Option(version.toShort)
        )
      )

      // Join a second static member.
      val joinFollowerFuture = Future {
        sendJoinRequest(
          groupId = "grp",
          groupInstanceId = "group-instance-id",
          metadata = metadata,
          version = Option(version.toShort)
        )
      }

      // The leader rejoins.
      val rejoinLeaderFuture1 = Future {
        // Sleep for a while to make sure the requests are processed according to the sequence.
        Thread.sleep(1000)
        sendJoinRequest(
          groupId = "grp",
          memberId = leaderMemberId,
          metadata = metadata,
          version = Option(version.toShort)
        )
      }

      val joinFollowerFutureResponseData = Await.result(joinFollowerFuture, Duration.Inf)
      val rejoinLeaderFutureResponseData1 = Await.result(rejoinLeaderFuture1, Duration.Inf)
      val followerMemberId = joinFollowerFutureResponseData.memberId

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
              .setGroupInstanceId("group-instance-id")
              .setMetadata(metadata)
          ).asJava),
        rejoinLeaderFutureResponseData1
      )

      // Send leader SyncGroup request.
      syncGroupWithOldProtocol(
        groupId = "grp",
        memberId = leaderMemberId,
        generationId = rejoinLeaderFutureResponseData1.generationId,
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

      // Send follower SyncGroup request.
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
          groupInstanceId = "group-instance-id",
          memberId = followerMemberId,
          metadata = metadata,
          version = Option(version.toShort)
        )
      )

      // The leader rejoins and triggers a rebalance.
      val rejoinLeaderFuture2 = Future {
        sendJoinRequest(
          groupId = "grp",
          memberId = leaderMemberId,
          metadata = metadata,
          version = Option(version.toShort)
        )
      }

      // A new follower with duplicated group instance id joins.
      val joinNewFollowerFuture = Future {
        // Sleep for a while to make sure the requests are processed according to the sequence.
        Thread.sleep(1000)
        sendJoinRequest(
          groupId = "grp",
          groupInstanceId = "group-instance-id",
          metadata = metadata,
          version = Option(version.toShort)
        )
      }

      // The old follower rejoin request should be fenced.
      val rejoinFollowerFuture = Future {
        // Sleep for a while to make sure the requests are processed according to the sequence.
        Thread.sleep(2000)
        sendJoinRequest(
          groupId = "grp",
          memberId = followerMemberId,
          groupInstanceId = "group-instance-id",
          metadata = metadata,
          version = Option(version.toShort)
        )
      }

      val rejoinLeaderFutureResponseData2 = Await.result(rejoinLeaderFuture2, Duration.Inf)
      val joinNewFollowerFutureResponseData = Await.result(joinNewFollowerFuture, Duration.Inf)
      val rejoinFollowerFutureResponseData = Await.result(rejoinFollowerFuture, Duration.Inf)
      val newFollowerMemberId = joinNewFollowerFutureResponseData.memberId

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
        rejoinLeaderFutureResponseData2
      )

      verifyJoinGroupResponseDataEquals(
        new JoinGroupResponseData()
          .setGenerationId(3)
          .setProtocolType(if (version >= 7) "consumer" else null)
          .setProtocolName("consumer-range")
          .setLeader(leaderMemberId)
          .setMemberId(newFollowerMemberId),
        joinNewFollowerFutureResponseData
      )

      verifyJoinGroupResponseDataEquals(
        new JoinGroupResponseData()
          .setProtocolName(if (version >= 7) null else "")
          .setMemberId(followerMemberId)
          .setErrorCode(Errors.FENCED_INSTANCE_ID.code),
        rejoinFollowerFutureResponseData
      )

      leaveGroup(
        groupId = "grp",
        memberId = leaderMemberId,
        useNewProtocol = false,
        version = ApiKeys.LEAVE_GROUP.latestVersion(isUnstableApiEnabled)
      )
      leaveGroup(
        groupId = "grp",
        memberId = newFollowerMemberId,
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

  private def verifyJoinGroupResponseDataEquals(
    expected: JoinGroupResponseData,
    actual: JoinGroupResponseData
  ): Unit = {
    assertEquals(expected.errorCode, actual.errorCode)
    assertEquals(expected.generationId, actual.generationId)
    assertEquals(expected.protocolType, actual.protocolType)
    assertEquals(expected.protocolName, actual.protocolName)
    assertEquals(expected.leader, actual.leader)
    assertEquals(expected.skipAssignment, actual.skipAssignment)
    assertEquals(expected.memberId, actual.memberId)
    assertEquals(expected.members.asScala.toSet, actual.members.asScala.toSet)
  }
}