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
import org.apache.kafka.common.message.{JoinGroupResponseData, SyncGroupRequestData, SyncGroupResponseData}
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

      // Send leader SyncGroup request.
      assertEquals(
        new SyncGroupResponseData()
          .setProtocolType("consumer")
          .setProtocolName("consumer-range")
          .setAssignment(Array[Byte](1)),
        syncGroupWithOldProtocol(
          groupId = "grp",
          memberId = leaderMemberId,
          generationId = rejoinLeaderResponseData.generationId,
          assignments = List(new SyncGroupRequestData.SyncGroupRequestAssignment()
            .setMemberId(leaderMemberId)
            .setAssignment(Array[Byte](1))
          )
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
      val rejoinLeaderFuture = Future {
        // Sleep for a while to make sure the new join group request is processed first.
        Thread.sleep(1000)
        sendJoinRequest(
          groupId = "grp",
          memberId = leaderMemberId,
          metadata = metadata,
          version = Option(version.toShort)
        )
      }

      val joinFollowerFutureResponseData = Await.result(joinFollowerFuture, Duration.Inf)
      val rejoinLeaderFutureResponseData = Await.result(rejoinLeaderFuture, Duration.Inf)
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
        rejoinLeaderFutureResponseData
      )

      // Send leader SyncGroup request.
      val leaderSyncfuture = Future {
        syncGroupWithOldProtocol(
          groupId = "grp",
          memberId = leaderMemberId,
          generationId = rejoinLeaderFutureResponseData.generationId,
          assignments = List(
            new SyncGroupRequestData.SyncGroupRequestAssignment()
              .setMemberId(leaderMemberId)
              .setAssignment(Array[Byte](1)),
            new SyncGroupRequestData.SyncGroupRequestAssignment()
              .setMemberId(followerMemberId)
              .setAssignment(Array[Byte](2))
          )
        )
      }

      // Send follower SyncGroup request.
      val followerSyncfuture = Future {
        syncGroupWithOldProtocol(
          groupId = "grp",
          memberId = followerMemberId,
          generationId = joinFollowerFutureResponseData.generationId
        )
      }

      assertEquals(
        new SyncGroupResponseData()
          .setProtocolType("consumer")
          .setProtocolName("consumer-range")
          .setAssignment(Array[Byte](1)),
        Await.result(leaderSyncfuture, Duration.Inf)
      )
      assertEquals(
        new SyncGroupResponseData()
          .setProtocolType("consumer")
          .setProtocolName("consumer-range")
          .setAssignment(Array[Byte](2)),
        Await.result(followerSyncfuture, Duration.Inf)
      )

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