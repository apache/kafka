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
import org.apache.kafka.common.message.ListGroupsResponseData
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.coordinator.group.Group
import org.apache.kafka.coordinator.group.classic.ClassicGroupState
import org.apache.kafka.coordinator.group.consumer.ConsumerGroup.ConsumerGroupState
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.extension.ExtendWith

@Timeout(120)
@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
@ClusterTestDefaults(clusterType = Type.KRAFT, brokers = 1)
@Tag("integration")
class ConsumerProtocolMigrationTest(cluster: ClusterInstance) extends GroupCoordinatorBaseRequestTest(cluster) {
  @ClusterTest(serverProperties = Array(
    new ClusterConfigProperty(key = "group.coordinator.new.enable", value = "true"),
    new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
    new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
  ))
  def testOfflineUpgrade(): Unit = {
    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    createOffsetsTopic()

    // Create the topic.
    createTopic(
      topic = "foo",
      numPartitions = 3
    )

    // Create a classic group by joining a member.
    val groupId = "grp"
    val (memberId, _) = joinDynamicConsumerGroupWithOldProtocol(groupId)

    // The joining request from a consumer group member is rejected.
    val responseData = consumerGroupHeartbeat(
      groupId = groupId,
      rebalanceTimeoutMs = 5 * 60 * 1000,
      subscribedTopicNames = List("foo"),
      topicPartitions = List.empty,
      expectedError = Errors.GROUP_ID_NOT_FOUND
    )
    assertEquals("Group grp is not a consumer group.", responseData.errorMessage)

    // The member leaves the group.
    leaveGroup(
      groupId = groupId,
      memberId = memberId,
      useNewProtocol = false,
      version = ApiKeys.LEAVE_GROUP.latestVersion(isUnstableApiEnabled)
    )

    // Verify that the group is empty.
    val listGroupsResponseData1 = listGroups(
      statesFilter = List.empty,
      typesFilter = List(Group.GroupType.CLASSIC.toString)
    )

    val expectedListResponse1 = List(new ListGroupsResponseData.ListedGroup()
      .setGroupId(groupId)
      .setProtocolType("consumer")
      .setGroupState(ClassicGroupState.EMPTY.toString)
      .setGroupType(Group.GroupType.CLASSIC.toString))

    assertEquals(expectedListResponse1, listGroupsResponseData1)

    // The joining request with a consumer group member is accepted.
    consumerGroupHeartbeat(
      groupId = groupId,
      memberId = memberId,
      rebalanceTimeoutMs = 5 * 60 * 1000,
      subscribedTopicNames = List("foo"),
      topicPartitions = List.empty,
      expectedError = Errors.NONE
    )

    // The group has become a consumer group.
    val listGroupsResponseData2 = listGroups(
      statesFilter = List.empty,
      typesFilter = List(Group.GroupType.CONSUMER.toString)
    )

    val expectedListResponse2 = List(new ListGroupsResponseData.ListedGroup()
      .setGroupId(groupId)
      .setProtocolType("consumer")
      .setGroupState(ConsumerGroupState.STABLE.toString)
      .setGroupType(Group.GroupType.CONSUMER.toString))

    assertEquals(expectedListResponse2, listGroupsResponseData2)
  }

  @ClusterTest(serverProperties = Array(
    new ClusterConfigProperty(key = "group.coordinator.new.enable", value = "true"),
    new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
    new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
  ))
  def testOfflineDowngrade(): Unit = {
    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    createOffsetsTopic()

    // Create the topic.
    createTopic(
      topic = "foo",
      numPartitions = 3
    )

    // Create a consumer group by joining a member.
    val groupId = "grp"
    val (memberId, _) = joinConsumerGroupWithNewProtocol(groupId)

    // The joining request from a classic group member is rejected.
    val joinGroupResponseData = sendJoinRequest(groupId = groupId)
    assertEquals(Errors.GROUP_ID_NOT_FOUND.code, joinGroupResponseData.errorCode)

    // The member leaves the group.
    leaveGroup(
      groupId = groupId,
      memberId = memberId,
      useNewProtocol = true,
      version = ApiKeys.CONSUMER_GROUP_HEARTBEAT.latestVersion(isUnstableApiEnabled)
    )

    // Verify that the group is empty.
    val listGroupsResponseData1 = listGroups(
      statesFilter = List.empty,
      typesFilter = List(Group.GroupType.CONSUMER.toString)
    )

    val expectedListResponse1 = List(new ListGroupsResponseData.ListedGroup()
      .setGroupId(groupId)
      .setProtocolType("consumer")
      .setGroupState(ClassicGroupState.EMPTY.toString)
      .setGroupType(Group.GroupType.CONSUMER.toString))

    assertEquals(expectedListResponse1, listGroupsResponseData1)

    // The joining request with a classic group member is accepted.
    joinDynamicConsumerGroupWithOldProtocol(groupId = groupId)

    // The group has become a classic group.
    val listGroupsResponseData2 = listGroups(
      statesFilter = List.empty,
      typesFilter = List(Group.GroupType.CLASSIC.toString)
    )

    val expectedListResponse2 = List(new ListGroupsResponseData.ListedGroup()
      .setGroupId(groupId)
      .setProtocolType("consumer")
      .setGroupState(ClassicGroupState.STABLE.toString)
      .setGroupType(Group.GroupType.CLASSIC.toString))

    assertEquals(expectedListResponse2, listGroupsResponseData2)
  }
}
