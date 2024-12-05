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

import org.apache.kafka.common.Uuid
import org.apache.kafka.common.message.LeaveGroupResponseData
import org.apache.kafka.common.test.api.ClusterInstance
import org.apache.kafka.common.test.api.{ClusterConfigProperty, ClusterTest, ClusterTestDefaults, Type}
import org.apache.kafka.common.test.api.ClusterTestExtensions
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.JoinGroupRequest
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.coordinator.group.classic.ClassicGroupState
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroup.ConsumerGroupState
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.extension.ExtendWith

import scala.jdk.CollectionConverters._

@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
@ClusterTestDefaults(types = Array(Type.KRAFT), serverProperties = Array(
  new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
  new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1")
))
class LeaveGroupRequestTest(cluster: ClusterInstance) extends GroupCoordinatorBaseRequestTest(cluster) {
  @ClusterTest
  def testLeaveGroupWithNewConsumerGroupProtocolAndNewGroupCoordinator(): Unit = {
    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    createOffsetsTopic()

    // Create the topic.
    createTopic(
      topic = "foo",
      numPartitions = 3
    )

    for (version <- 3 to ApiKeys.LEAVE_GROUP.latestVersion(isUnstableApiEnabled)) {
      val memberId = Uuid.randomUuid().toString
      assertEquals(Errors.NONE.code, consumerGroupHeartbeat(
        groupId = "group",
        memberId = memberId,
        memberEpoch = 0,
        instanceId = "instance-id",
        rebalanceTimeoutMs = 5 * 60 * 1000,
        subscribedTopicNames = List("foo"),
        topicPartitions = List.empty,
      ).errorCode)

      assertEquals(
        new LeaveGroupResponseData()
          .setMembers(List(
            new LeaveGroupResponseData.MemberResponse()
              .setMemberId(memberId)
              .setGroupInstanceId("instance-id")
          ).asJava),
        classicLeaveGroup(
          groupId = "group",
          memberIds = List(JoinGroupRequest.UNKNOWN_MEMBER_ID),
          groupInstanceIds = List("instance-id"),
          version = version.toShort
        )
      )

      assertEquals(
        ConsumerGroupState.EMPTY.toString,
        consumerGroupDescribe(List("group")).head.groupState
      )
    }
  }

  @ClusterTest
  def testLeaveGroupWithOldConsumerGroupProtocolAndNewGroupCoordinator(): Unit = {
    testLeaveGroup()
  }

  @ClusterTest
  def testLeaveGroupWithOldConsumerGroupProtocolAndOldGroupCoordinator(): Unit = {
    testLeaveGroup()
  }

  private def testLeaveGroup(): Unit = {
    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    createOffsetsTopic()

    // Create the topic.
    createTopic(
      topic = "foo",
      numPartitions = 3
    )

    for (version <- ApiKeys.LEAVE_GROUP.oldestVersion() to ApiKeys.LEAVE_GROUP.latestVersion(isUnstableApiEnabled)) {
      // Join the consumer group. Note that we don't heartbeat here so we must use
      // a session long enough for the duration of the test.
      val (memberId1, _) = joinDynamicConsumerGroupWithOldProtocol("grp-1")
      if (version >= 3) {
        joinStaticConsumerGroupWithOldProtocol("grp-2", "group-instance-id")
      }

      // Request with empty group id.
      leaveGroupWithOldProtocol(
        groupId = "",
        memberIds = List(memberId1),
        expectedLeaveGroupError = Errors.INVALID_GROUP_ID,
        expectedMemberErrors = List(Errors.NONE),
        version = version.toShort
      )

      // Request with invalid group id and unknown member id should still get Errors.INVALID_GROUP_ID.
      leaveGroupWithOldProtocol(
        groupId = "",
        memberIds = List("member-id-unknown"),
        expectedLeaveGroupError = Errors.INVALID_GROUP_ID,
        expectedMemberErrors = List(Errors.NONE),
        version = version.toShort
      )

      // Request with unknown group id gets Errors.UNKNOWN_MEMBER_ID.
      leaveGroupWithOldProtocol(
        groupId = "grp-unknown",
        memberIds = List(memberId1),
        expectedLeaveGroupError = if (version >= 3) Errors.NONE else Errors.UNKNOWN_MEMBER_ID,
        expectedMemberErrors = if (version >= 3) List(Errors.UNKNOWN_MEMBER_ID) else List.empty,
        version = version.toShort
      )

      // Request with unknown member ids.
      leaveGroupWithOldProtocol(
        groupId = "grp-1",
        memberIds = if (version >= 3) List("unknown-member-id", JoinGroupRequest.UNKNOWN_MEMBER_ID) else List("unknown-member-id"),
        expectedLeaveGroupError = if (version >= 3) Errors.NONE else Errors.UNKNOWN_MEMBER_ID,
        expectedMemberErrors = if (version >= 3) List(Errors.UNKNOWN_MEMBER_ID, Errors.UNKNOWN_MEMBER_ID) else List.empty,
        version = version.toShort
      )

      // Success GroupLeave request.
      leaveGroupWithOldProtocol(
        groupId = "grp-1",
        memberIds = List(memberId1),
        expectedLeaveGroupError = Errors.NONE,
        expectedMemberErrors = if (version >= 3) List(Errors.NONE) else List.empty,
        version = version.toShort
      )

      // grp-1 is empty.
      assertEquals(
        ClassicGroupState.EMPTY.toString,
        describeGroups(List("grp-1")).head.groupState
      )

      if (version >= 3) {
        // Request with fenced group instance id.
        leaveGroupWithOldProtocol(
          groupId = "grp-2",
          memberIds = List("member-id-fenced"),
          groupInstanceIds = List("group-instance-id"),
          expectedLeaveGroupError = Errors.NONE,
          expectedMemberErrors = List(Errors.FENCED_INSTANCE_ID),
          version = version.toShort
        )

        // Having unknown member id will not affect the request processing.
        leaveGroupWithOldProtocol(
          groupId = "grp-2",
          memberIds = List(JoinGroupRequest.UNKNOWN_MEMBER_ID),
          groupInstanceIds = List("group-instance-id"),
          expectedLeaveGroupError = Errors.NONE,
          expectedMemberErrors = List(Errors.NONE),
          version = version.toShort
        )

        // grp-2 is empty.
        assertEquals(
          ClassicGroupState.EMPTY.toString,
          describeGroups(List("grp-2")).head.groupState
        )
      }
    }
  }
}
