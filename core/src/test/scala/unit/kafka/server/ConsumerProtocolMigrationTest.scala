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
@ClusterTestDefaults(types = Array(Type.KRAFT))
@Tag("integration")
class ConsumerProtocolMigrationTest(cluster: ClusterInstance) extends GroupCoordinatorBaseRequestTest(cluster) {
  @ClusterTest(serverProperties = Array(
    new ClusterConfigProperty(key = "group.coordinator.rebalance.protocols", value = "classic,consumer"),
    new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
    new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
  ))
  def testUpgradeFromEmptyClassicToConsumerGroup(): Unit = {
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

    // The member leaves the group.
    leaveGroup(
      groupId = groupId,
      memberId = memberId,
      useNewProtocol = false,
      version = ApiKeys.LEAVE_GROUP.latestVersion(isUnstableApiEnabled)
    )

    // Verify that the group is empty.
    assertEquals(
      List(
        new ListGroupsResponseData.ListedGroup()
          .setGroupId(groupId)
          .setProtocolType("consumer")
          .setGroupState(ClassicGroupState.EMPTY.toString)
          .setGroupType(Group.GroupType.CLASSIC.toString)
      ),
      listGroups(
        statesFilter = List.empty,
        typesFilter = List(Group.GroupType.CLASSIC.toString)
      )
    )

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
    assertEquals(
      List(
        new ListGroupsResponseData.ListedGroup()
          .setGroupId(groupId)
          .setProtocolType("consumer")
          .setGroupState(ConsumerGroupState.STABLE.toString)
          .setGroupType(Group.GroupType.CONSUMER.toString)
      ),
      listGroups(
        statesFilter = List.empty,
        typesFilter = List(Group.GroupType.CONSUMER.toString)
      )
    )
  }

  @ClusterTest(serverProperties = Array(
    new ClusterConfigProperty(key = "group.coordinator.rebalance.protocols", value = "classic,consumer"),
    new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
    new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
  ))
  def testDowngradeFromEmptyConsumerToClassicGroup(): Unit = {
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

    // The member leaves the group.
    leaveGroup(
      groupId = groupId,
      memberId = memberId,
      useNewProtocol = true,
      version = ApiKeys.CONSUMER_GROUP_HEARTBEAT.latestVersion(isUnstableApiEnabled)
    )

    // Verify that the group is empty.
    assertEquals(
      List(
        new ListGroupsResponseData.ListedGroup()
          .setGroupId(groupId)
          .setProtocolType("consumer")
          .setGroupState(ClassicGroupState.EMPTY.toString)
          .setGroupType(Group.GroupType.CONSUMER.toString)
      ),
      listGroups(
        statesFilter = List.empty,
        typesFilter = List(Group.GroupType.CONSUMER.toString)
      )
    )

    // The joining request with a classic group member is accepted.
    joinDynamicConsumerGroupWithOldProtocol(groupId = groupId)

    // The group has become a classic group.
    assertEquals(
      List(
        new ListGroupsResponseData.ListedGroup()
          .setGroupId(groupId)
          .setProtocolType("consumer")
          .setGroupState(ClassicGroupState.STABLE.toString)
          .setGroupType(Group.GroupType.CLASSIC.toString)
      ),
      listGroups(
        statesFilter = List.empty,
        typesFilter = List(Group.GroupType.CLASSIC.toString)
      )
    )
  }

  @ClusterTest(serverProperties = Array(
    new ClusterConfigProperty(key = "group.coordinator.rebalance.protocols", value = "classic,consumer"),
    new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
    new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
  ))
  def testUpgradeFromSimpleGroupToConsumerGroup(): Unit = {
    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    createOffsetsTopic()

    val topicName = "foo"
    // Create the topic.
    createTopic(
      topic = topicName,
      numPartitions = 3
    )

    // An admin client commits offsets and creates the simple group.
    val groupId = "group-id"
    commitOffset(
      groupId = groupId,
      memberId = "member-id",
      memberEpoch = -1,
      topic = topicName,
      partition = 0,
      offset = 1000L,
      expectedError = Errors.NONE,
      version = ApiKeys.OFFSET_COMMIT.latestVersion(isUnstableApiEnabled)
    )

    // Verify that the simple group is created.
    assertEquals(
      List(
        new ListGroupsResponseData.ListedGroup()
          .setGroupId(groupId)
          .setGroupState(ClassicGroupState.EMPTY.toString)
          .setGroupType(Group.GroupType.CLASSIC.toString)
      ),
      listGroups(
        statesFilter = List.empty,
        typesFilter = List(Group.GroupType.CLASSIC.toString)
      )
    )

    // The joining request with a consumer group member is accepted.
    consumerGroupHeartbeat(
      groupId = groupId,
      rebalanceTimeoutMs = 5 * 60 * 1000,
      subscribedTopicNames = List(topicName),
      topicPartitions = List.empty,
      expectedError = Errors.NONE
    )

    // The group has become a consumer group.
    assertEquals(
      List(
        new ListGroupsResponseData.ListedGroup()
          .setGroupId(groupId)
          .setProtocolType("consumer")
          .setGroupState(ConsumerGroupState.STABLE.toString)
          .setGroupType(Group.GroupType.CONSUMER.toString)
      ),
      listGroups(
        statesFilter = List.empty,
        typesFilter = List(Group.GroupType.CONSUMER.toString)
      )
    )
  }
}
