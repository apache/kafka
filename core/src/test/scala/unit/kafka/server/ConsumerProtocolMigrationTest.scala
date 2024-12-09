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

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.message.{JoinGroupResponseData, ListGroupsResponseData, OffsetFetchResponseData, SyncGroupResponseData}
import org.apache.kafka.common.test.api.ClusterInstance
import org.apache.kafka.common.test.api.{ClusterConfigProperty, ClusterTest, ClusterTestDefaults, Type}
import org.apache.kafka.common.test.api.ClusterTestExtensions
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.coordinator.group.{Group, GroupCoordinatorConfig}
import org.apache.kafka.coordinator.group.classic.ClassicGroupState
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroup.ConsumerGroupState
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.extension.ExtendWith

import java.nio.ByteBuffer
import java.util.Collections
import scala.jdk.CollectionConverters._

@Timeout(120)
@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
@ClusterTestDefaults(types = Array(Type.KRAFT))
class ConsumerProtocolMigrationTest(cluster: ClusterInstance) extends GroupCoordinatorBaseRequestTest(cluster) {
  @ClusterTest(
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, value = "bidirectional")
    )
  )
  def testUpgradeFromEmptyClassicToConsumerGroupWithBidirectionalPolicy(): Unit = {
    testUpgradeFromEmptyClassicToConsumerGroup()
  }

  @ClusterTest(
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, value = "upgrade")
    )
  )
  def testUpgradeFromEmptyClassicToConsumerGroupWithUpgradePolicy(): Unit = {
    testUpgradeFromEmptyClassicToConsumerGroup()
  }

  @ClusterTest(
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, value = "downgrade")
    )
  )
  def testUpgradeFromEmptyClassicToConsumerGroupWithDowngradePolicy(): Unit = {
    testUpgradeFromEmptyClassicToConsumerGroup()
  }

  @ClusterTest(
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, value = "disabled")
    )
  )
  def testUpgradeFromEmptyClassicToConsumerGroupWithDisabledPolicy(): Unit = {
    testUpgradeFromEmptyClassicToConsumerGroup()
  }

  @ClusterTest(
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, value = "bidirectional")
    )
  )
  def testDowngradeFromEmptyConsumerToClassicGroupWithBidirectionalPolicy(): Unit = {
    testDowngradeFromEmptyConsumerToClassicGroup()
  }

  @ClusterTest(
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, value = "upgrade")
    )
  )
  def testDowngradeFromEmptyConsumerToClassicGroupWithUpgradePolicy(): Unit = {
    testDowngradeFromEmptyConsumerToClassicGroup()
  }

  @ClusterTest(
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, value = "downgrade")
    )
  )
  def testDowngradeFromEmptyConsumerToClassicGroupWithDowngradePolicy(): Unit = {
    testDowngradeFromEmptyConsumerToClassicGroup()
  }

  @ClusterTest(
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, value = "disabled")
    )
  )
  def testDowngradeFromEmptyConsumerToClassicGroupWithDisabledPolicy(): Unit = {
    testDowngradeFromEmptyConsumerToClassicGroup()
  }

  @ClusterTest(
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, value = "bidirectional")
    )
  )
  def testUpgradeFromSimpleGroupToConsumerGroupWithBidirectionalPolicy(): Unit = {
    testUpgradeFromSimpleGroupToConsumerGroup()
  }

  @ClusterTest(
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, value = "upgrade")
    )
  )
  def testUpgradeFromSimpleGroupToConsumerGroupWithUpgradePolicy(): Unit = {
    testUpgradeFromSimpleGroupToConsumerGroup()
  }

  @ClusterTest(
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, value = "downgrade")
    )
  )
  def testUpgradeFromSimpleGroupToConsumerGroupWithDowngradePolicy(): Unit = {
    testUpgradeFromSimpleGroupToConsumerGroup()
  }

  @ClusterTest(
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, value = "disabled")
    )
  )
  def testUpgradeFromSimpleGroupToConsumerGroupWithDisabledPolicy(): Unit = {
    testUpgradeFromSimpleGroupToConsumerGroup()
  }

  @ClusterTest(
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, value = "bidirectional")
    )
  )
  def testOnlineMigrationWithEagerAssignmentStrategyAndDynamicMembers(): Unit = {
    testOnlineMigrationWithEagerAssignmentStrategy(useStaticMembers = false)
  }

  @ClusterTest(
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, value = "bidirectional")
    )
  )
  def testOnlineMigrationWithEagerAssignmentStrategyAndStaticMembers(): Unit = {
    testOnlineMigrationWithEagerAssignmentStrategy(useStaticMembers = true)
  }

  @ClusterTest(
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, value = "bidirectional")
    )
  )
  def testOnlineMigrationWithCooperativeAssignmentStrategyAndDynamicMembers(): Unit = {
    testOnlineMigrationWithCooperativeAssignmentStrategy(useStaticMembers = false)
  }

  @ClusterTest(
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, value = "bidirectional")
    )
  )
  def testOnlineMigrationWithCooperativeAssignmentStrategyAndStaticMembers(): Unit = {
    testOnlineMigrationWithCooperativeAssignmentStrategy(useStaticMembers = true)
  }

  @ClusterTest(
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, value = "upgrade")
    )
  )
  def testUpgradeMigrationPolicy(): Unit = {
    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    createOffsetsTopic()

    // Create the topic.
    createTopic(
      topic = "foo",
      numPartitions = 3
    )

    // Classic member 1 joins the classic group.
    val groupId = "grp"

    joinDynamicConsumerGroupWithOldProtocol(
      groupId = groupId,
      metadata = metadata(List.empty),
      assignment = assignment(List(0, 1, 2))
    )

    // The joining request with a consumer group member 2 is accepted.
    val memberId2 = consumerGroupHeartbeat(
      groupId = groupId,
      memberId = Uuid.randomUuid.toString,
      rebalanceTimeoutMs = 5 * 60 * 1000,
      subscribedTopicNames = List("foo"),
      topicPartitions = List.empty,
      expectedError = Errors.NONE
    ).memberId

    // The group has become a consumer group.
    assertEquals(
      List(
        new ListGroupsResponseData.ListedGroup()
          .setGroupId(groupId)
          .setProtocolType("consumer")
          .setGroupState(ConsumerGroupState.RECONCILING.toString)
          .setGroupType(Group.GroupType.CONSUMER.toString)
      ),
      listGroups(
        statesFilter = List.empty,
        typesFilter = List(Group.GroupType.CONSUMER.toString)
      )
    )

    // Downgrade the group by leaving member 2.
    leaveGroupWithNewProtocol(
      groupId = groupId,
      memberId = memberId2
    )

    // The group is still a consumer group.
    assertEquals(
      List(
        new ListGroupsResponseData.ListedGroup()
          .setGroupId(groupId)
          .setProtocolType("consumer")
          .setGroupState(ConsumerGroupState.ASSIGNING.toString)
          .setGroupType(Group.GroupType.CONSUMER.toString)
      ),
      listGroups(
        statesFilter = List.empty,
        typesFilter = List(Group.GroupType.CONSUMER.toString)
      )
    )
  }

  @ClusterTest(
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, value = "downgrade")
    )
  )
  def testDowngradeMigrationPolicy(): Unit = {
    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    createOffsetsTopic()

    // Create the topic.
    createTopic(
      topic = "foo",
      numPartitions = 3
    )

    val groupId = "grp"

    // Consumer member 1 joins the group.
    val (memberId1, _) = joinConsumerGroupWithNewProtocol(groupId, Uuid.randomUuid.toString)

    // Classic member 2 joins the group.
    val joinGroupResponseData = sendJoinRequest(
      groupId = groupId
    )
    val memberId2 = sendJoinRequest(
      groupId = groupId,
      memberId = joinGroupResponseData.memberId,
      metadata = metadata(List.empty)
    ).memberId

    // Member 2 syncs. The assigned partition is empty.
    assertEquals(
      new SyncGroupResponseData()
        .setErrorCode(Errors.NONE.code)
        .setProtocolType("consumer")
        .setProtocolName("consumer-range")
        .setAssignment(assignment(List.empty)),
      syncGroupWithOldProtocol(
        groupId = groupId,
        memberId = memberId2,
        generationId = 2
      )
    )

    // Member 2 heartbeats.
    heartbeat(
      groupId = groupId,
      generationId = 2,
      memberId = memberId2
    )

    // Member 1 heartbeats to revoke partitions.
    consumerGroupHeartbeat(
      groupId = groupId,
      memberId = memberId1,
      rebalanceTimeoutMs = 5 * 60 * 1000,
      subscribedTopicNames = List("foo"),
      topicPartitions = List.empty,
      expectedError = Errors.NONE
    )

    // Member 2 heartbeats and gets REBALANCE_IN_PROGRESS.
    heartbeat(
      groupId = groupId,
      generationId = 2,
      memberId = memberId2,
      expectedError = Errors.REBALANCE_IN_PROGRESS
    )

    // Downgrade the group by leaving member 1.
    leaveGroupWithNewProtocol(
      groupId = groupId,
      memberId = memberId1
    )

    // The group has become a classic group.
    assertEquals(
      List(
        new ListGroupsResponseData.ListedGroup()
          .setGroupId(groupId)
          .setProtocolType("consumer")
          .setGroupState(ClassicGroupState.PREPARING_REBALANCE.toString)
          .setGroupType(Group.GroupType.CLASSIC.toString)
      ),
      listGroups(
        statesFilter = List.empty,
        typesFilter = List(Group.GroupType.CLASSIC.toString)
      )
    )

    // The consumerGroupHeartbeat request is rejected.
    consumerGroupHeartbeat(
      groupId = groupId,
      memberId = memberId1,
      rebalanceTimeoutMs = 5 * 60 * 1000,
      subscribedTopicNames = List("foo"),
      topicPartitions = List.empty,
      expectedError = Errors.GROUP_ID_NOT_FOUND
    )
  }

  @ClusterTest(
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, value = "disabled")
    )
  )
  def testUpgradeWithDisabledMigrationPolicy(): Unit = {
    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    createOffsetsTopic()

    // Create the topic.
    createTopic(
      topic = "foo",
      numPartitions = 3
    )

    // Classic member 1 joins and creates the classic group.
    val groupId = "grp"

    joinDynamicConsumerGroupWithOldProtocol(
      groupId = groupId,
      metadata = metadata(List.empty),
      assignment = assignment(List(0, 1, 2))
    )

    // The consumerGroupHeartbeat request is rejected.
    consumerGroupHeartbeat(
      groupId = groupId,
      memberId = Uuid.randomUuid.toString,
      rebalanceTimeoutMs = 5 * 60 * 1000,
      subscribedTopicNames = List("foo"),
      topicPartitions = List.empty,
      expectedError = Errors.GROUP_ID_NOT_FOUND
    )
  }

  @ClusterTest(
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, value = "disabled")
    )
  )
  def testDowngradeWithDisabledMigrationPolicy(): Unit = {
    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    createOffsetsTopic()

    // Create the topic.
    createTopic(
      topic = "foo",
      numPartitions = 3
    )

    val groupId = "grp"

    // Consumer member 1 joins the group.
    val (memberId1, _) = joinConsumerGroupWithNewProtocol(groupId, Uuid.randomUuid.toString)

    // Classic member 2 joins the group.
    val joinGroupResponseData = sendJoinRequest(
      groupId = groupId
    )
    sendJoinRequest(
      groupId = groupId,
      memberId = joinGroupResponseData.memberId,
      metadata = metadata(List.empty)
    )

    // Try to downgrade the group by leaving member 1.
    leaveGroupWithNewProtocol(
      groupId = groupId,
      memberId = memberId1
    )

    // The group is still a consumer group.
    assertEquals(
      List(
        new ListGroupsResponseData.ListedGroup()
          .setGroupId(groupId)
          .setProtocolType("consumer")
          .setGroupState(ConsumerGroupState.ASSIGNING.toString)
          .setGroupType(Group.GroupType.CONSUMER.toString)
      ),
      listGroups(
        statesFilter = List.empty,
        typesFilter = List(Group.GroupType.CONSUMER.toString)
      )
    )
  }

  /**
   * The test method checks the following scenario:
   * 1. Creating a classic group with member 1, whose assignment has non-empty user data.
   * 2. Member 2 using consumer protocol joins. The group cannot be upgraded and the join is
   *    rejected.
   * 3. Member 1 leaves.
   * 4. Member 2 using consumer protocol joins. The group is upgraded.
   */
  @ClusterTest(
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, value = "bidirectional")
    )
  )
  def testOnlineMigrationWithNonEmptyUserDataInAssignment(): Unit = {
    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    createOffsetsTopic()

    // Create the topic.
    createTopic(
      topic = "foo",
      numPartitions = 3
    )

    // Classic member 1 joins the classic group.
    val groupId = "grp"

    val memberId1 = joinDynamicConsumerGroupWithOldProtocol(
      groupId = groupId,
      metadata = metadata(List.empty),
      assignment = assignment(List(0, 1, 2), ByteBuffer.allocate(1))
    )._1

    // The joining request with a consumer group member 2 is rejected.
    val errorMessage = consumerGroupHeartbeat(
      groupId = groupId,
      memberId = Uuid.randomUuid.toString,
      rebalanceTimeoutMs = 5 * 60 * 1000,
      subscribedTopicNames = List("foo"),
      topicPartitions = List.empty,
      expectedError = Errors.GROUP_ID_NOT_FOUND
    ).errorMessage

    assertEquals(
      "Cannot upgrade classic group grp to consumer group because an unsupported custom assignor is in use. " +
      "Please refer to the documentation or switch to a default assignor before re-attempting the upgrade.",
      errorMessage
    )

    // The group is still a classic group.
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

    // Classic member 1 leaves the group.
    leaveGroup(
      groupId = groupId,
      memberId = memberId1,
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
      memberId = Uuid.randomUuid.toString,
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

  private def testUpgradeFromEmptyClassicToConsumerGroup(): Unit = {
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

  private def testDowngradeFromEmptyConsumerToClassicGroup(): Unit = {
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
    val (memberId, _) = joinConsumerGroupWithNewProtocol(groupId, Uuid.randomUuid.toString)

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

  private def testUpgradeFromSimpleGroupToConsumerGroup(): Unit = {
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
      memberId = Uuid.randomUuid.toString,
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

  /**
   * The test method checks the following scenario:
   * 1. Creating a classic group with member 1.
   * 2. Member 2 using consumer protocol joins. The group is upgraded and a rebalance is triggered.
   * 3. Member 1 performs different operations. (Heartbeat, OffsetCommit, OffsetFetch)
   * 4. Member 1 reconciles with the cooperative strategy. It revokes all its partitions and rejoin.
   * 5. Member 2 rejoins. The groups is stabilized.
   * 6. Member 2 leaves. The group is downgraded.
   *
   * @param useStaticMembers A boolean indicating whether member 1 and member 2 are static members.
   */
  private def testOnlineMigrationWithEagerAssignmentStrategy(useStaticMembers: Boolean): Unit = {
    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    createOffsetsTopic()

    // Create the topic.
    createTopic(
      topic = "foo",
      numPartitions = 3
    )

    // Classic member 1 joins the classic group.
    val groupId = "grp"
    val instanceId1 = "instance-id-1"
    val instanceId2 = "instance-id-2"

    var memberId1: String = null
    if (!useStaticMembers) {
      memberId1 = joinDynamicConsumerGroupWithOldProtocol(
        groupId = groupId,
        metadata = metadata(List.empty),
        assignment = assignment(List(0, 1, 2))
      )._1
    } else {
      memberId1 = joinStaticConsumerGroupWithOldProtocol(
        groupId = groupId,
        groupInstanceId = instanceId1,
        metadata = metadata(List.empty),
        assignment = assignment(List(0, 1, 2))
      )._1
    }

    // The joining request with a consumer group member 2 is accepted.
    val memberId2 = consumerGroupHeartbeat(
      groupId = groupId,
      memberId = Uuid.randomUuid.toString,
      instanceId = if (useStaticMembers) instanceId2 else null,
      rebalanceTimeoutMs = 5 * 60 * 1000,
      subscribedTopicNames = List("foo"),
      topicPartitions = List.empty,
      expectedError = Errors.NONE
    ).memberId

    // The group has become a consumer group.
    assertEquals(
      List(
        new ListGroupsResponseData.ListedGroup()
          .setGroupId(groupId)
          .setProtocolType("consumer")
          .setGroupState(ConsumerGroupState.RECONCILING.toString)
          .setGroupType(Group.GroupType.CONSUMER.toString)
      ),
      listGroups(
        statesFilter = List.empty,
        typesFilter = List(Group.GroupType.CONSUMER.toString)
      )
    )

    // Member 1 heartbeats with illegal generation id.
    heartbeat(
      groupId = groupId,
      generationId = 2,
      memberId = memberId1,
      expectedError = Errors.ILLEGAL_GENERATION
    )

    // Heartbeat with unknown member id.
    heartbeat(
      groupId = groupId,
      generationId = 1,
      memberId = "unknown-member-id",
      expectedError = Errors.UNKNOWN_MEMBER_ID
    )

    // Member 1 heartbeats with unknown group id.
    heartbeat(
      groupId = "unknown-group-id",
      generationId = 1,
      memberId = memberId1,
      expectedError = Errors.UNKNOWN_MEMBER_ID
    )

    // Member 2 heartbeats with classic protocol.
    heartbeat(
      groupId = groupId,
      generationId = 1,
      memberId = memberId2,
      expectedError = Errors.UNKNOWN_MEMBER_ID
    )

    // Member 1 heartbeats and gets REBALANCE_IN_PROGRESS.
    heartbeat(
      groupId = groupId,
      generationId = 1,
      memberId = memberId1,
      expectedError = Errors.REBALANCE_IN_PROGRESS
    )

    // Member 1 commits offset. Start from version 1 because version 0 goes to ZK.
    for (version <- 1 to ApiKeys.OFFSET_COMMIT.latestVersion(isUnstableApiEnabled)) {
      for (partitionId <- 0 to 2) {
        commitOffset(
          groupId = groupId,
          memberId = memberId1,
          memberEpoch = 1,
          topic = "foo",
          partition = partitionId,
          offset = 100L + 10 * version + partitionId,
          expectedError = Errors.NONE,
          version = version.toShort
        )
      }
    }
    val committedOffset = 100L + 10 * ApiKeys.OFFSET_COMMIT.latestVersion(isUnstableApiEnabled)

    // Member 1 fetches offsets. Start from version 1 because version 0 goes to ZK.
    for (version <- 1 to ApiKeys.OFFSET_FETCH.latestVersion(isUnstableApiEnabled)) {
      assertEquals(
        new OffsetFetchResponseData.OffsetFetchResponseGroup()
          .setGroupId(groupId)
          .setTopics(List(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
              .setName("foo")
              .setPartitions(List(
                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                  .setPartitionIndex(0)
                  .setCommittedOffset(committedOffset),
                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                  .setPartitionIndex(1)
                  .setCommittedOffset(committedOffset + 1),
                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                  .setPartitionIndex(2)
                  .setCommittedOffset(committedOffset + 2)
              ).asJava)
          ).asJava),
        fetchOffsets(
          groupId = groupId,
          memberId = memberId1,
          memberEpoch = 1,
          partitions = List(
            new TopicPartition("foo", 0),
            new TopicPartition("foo", 1),
            new TopicPartition("foo", 2)
          ),
          requireStable = false,
          version = version.toShort
        )
      )
    }

    // Member 1 rejoins with illegal protocol type.
    assertEquals(
      new JoinGroupResponseData()
        .setProtocolName(null)
        .setErrorCode(Errors.INCONSISTENT_GROUP_PROTOCOL.code),
      sendJoinRequest(
        groupId = groupId,
        memberId = memberId1,
        metadata = metadata(List.empty),
        protocolType = "connect"
      )
    )

    // Member 1 rejoins with empty owned partitions.
    // We still get a response without error even if the generation id is illegal.
    assertEquals(
      new JoinGroupResponseData()
        .setGenerationId(2)
        .setProtocolType("consumer")
        .setProtocolName("consumer-range")
        .setMemberId(memberId1),
      sendJoinRequest(
        groupId = groupId,
        memberId = memberId1,
        metadata = metadata(List.empty)
      )
    )

    // Try to join a new classic member with unsupported protocol name.
    assertEquals(
      new JoinGroupResponseData()
        .setProtocolName(null)
        .setErrorCode(Errors.INCONSISTENT_GROUP_PROTOCOL.code),
      sendJoinRequest(
        groupId = groupId,
        memberId = "new-member",
        metadata = metadata(List.empty),
        protocolName = "consumer-roundrobin"
      )
    )

    // Member 2 rejoins to retrieve partitions pending assignment.
    val partitionsOfMember2 = consumerGroupHeartbeat(
      groupId = groupId,
      memberId = memberId2,
      memberEpoch = 2,
      rebalanceTimeoutMs = 5 * 60 * 1000,
      subscribedTopicNames = List("foo"),
      topicPartitions = List.empty,
      expectedError = Errors.NONE
    ).assignment.topicPartitions.get(0).partitions

    // The group has been stabilized.
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

    // Member 1 syncs with illegal generation.
    verifySyncGroupWithOldProtocol(
      groupId = groupId,
      memberId = memberId1,
      generationId = 1,
      expectedProtocolType = null,
      expectedProtocolName = null,
      expectedError = Errors.ILLEGAL_GENERATION
    )

    // Member 1 syncs with unknown group id.
    verifySyncGroupWithOldProtocol(
      groupId = "unknown-group-id",
      memberId = memberId1,
      generationId = 2,
      expectedProtocolType = null,
      expectedProtocolName = null,
      expectedError = Errors.UNKNOWN_MEMBER_ID
    )

    // Sync with unknown member id.
    verifySyncGroupWithOldProtocol(
      groupId = groupId,
      memberId = "unknown-member-id",
      generationId = 2,
      expectedProtocolType = null,
      expectedProtocolName = null,
      expectedError = Errors.UNKNOWN_MEMBER_ID
    )

    // Member 1 syncs with illegal protocol type.
    verifySyncGroupWithOldProtocol(
      groupId = groupId,
      memberId = memberId1,
      generationId = 2,
      protocolType = "connect",
      expectedProtocolType = null,
      expectedProtocolName = null,
      expectedError = Errors.INCONSISTENT_GROUP_PROTOCOL
    )

    // Member 1 syncs with illegal protocol name.
    verifySyncGroupWithOldProtocol(
      groupId = groupId,
      memberId = memberId1,
      generationId = 2,
      protocolName = "consumer-roundrobin",
      expectedProtocolType = null,
      expectedProtocolName = null,
      expectedError = Errors.INCONSISTENT_GROUP_PROTOCOL
    )

    // Member 1 syncs.
    verifySyncGroupWithOldProtocol(
      groupId = groupId,
      memberId = memberId1,
      generationId = 2,
      expectedAssignment = assignment(List(0, 1, 2).filter(!partitionsOfMember2.contains(_)))
    )

    if (!useStaticMembers) {
      // Downgrade the group by leaving member 2.
      leaveGroupWithNewProtocol(
        groupId = groupId,
        memberId = memberId2
      )
    } else {
      // Static member clients don't send explicit leave request when they are
      // shutdown for downgrade, so the downgrade has to be triggered by a
      // new static member join with old protocol and the same instance id.
      joinStaticConsumerGroupWithOldProtocol(
        groupId = groupId,
        groupInstanceId = instanceId2,
        metadata = metadata(List.empty),
        completeRebalance = false
      )
    }

    // The group has become a classic group.
    // If the downgrade is triggered by the static member replacement,
    // the group should remain STABLE, otherwise, a rebalance is triggered.
    assertEquals(
      List(
        new ListGroupsResponseData.ListedGroup()
          .setGroupId(groupId)
          .setProtocolType("consumer")
          .setGroupState(
            if (useStaticMembers)
              ClassicGroupState.STABLE.toString
            else
              ClassicGroupState.PREPARING_REBALANCE.toString
          )
          .setGroupType(Group.GroupType.CLASSIC.toString)
      ),
      listGroups(
        statesFilter = List.empty,
        typesFilter = List(Group.GroupType.CLASSIC.toString)
      )
    )
  }

  /**
   * The test method checks the following scenario:
   * 1. Creating a classic group with member 1.
   * 2. Member 2 using consumer protocol joins. The group is upgraded and a rebalance is triggered.
   * 3. Member 1 performs different operations. (Heartbeat, OffsetCommit, OffsetFetch)
   * 4. Member 1 reconciles with the cooperative strategy. It rejoins with its current owned
   *    partitions and syncs to get the newly assigned partitions. Then it rejoins again with
   *    the newly assigned partitions.
   * 6. Member 2 rejoins. The groups is stabilized.
   * 7. Member 2 leaves. The group is downgraded.
   *
   * @param useStaticMembers A boolean indicating whether member 1 and member 2 are static members.
   */
  private def testOnlineMigrationWithCooperativeAssignmentStrategy(useStaticMembers: Boolean): Unit = {
    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    createOffsetsTopic()

    // Create the topic.
    createTopic(
      topic = "foo",
      numPartitions = 3
    )

    // Classic member 1 joins the classic group.
    val groupId = "grp"
    val instanceId1 = "instance-id-1"
    val instanceId2 = "instance-id-2"

    var memberId1: String = null
    if (!useStaticMembers) {
      memberId1 = joinDynamicConsumerGroupWithOldProtocol(
        groupId = groupId,
        metadata = metadata(List.empty),
        assignment = assignment(List(0, 1, 2))
      )._1
    } else {
      memberId1 = joinStaticConsumerGroupWithOldProtocol(
        groupId = groupId,
        groupInstanceId = instanceId1,
        metadata = metadata(List.empty),
        assignment = assignment(List(0, 1, 2))
      )._1
    }

    // The joining request with a consumer group member 2 is accepted.
    val memberId2 = consumerGroupHeartbeat(
      groupId = groupId,
      memberId = Uuid.randomUuid.toString,
      instanceId = if (useStaticMembers) instanceId2 else null,
      rebalanceTimeoutMs = 5 * 60 * 1000,
      subscribedTopicNames = List("foo"),
      topicPartitions = List.empty,
      expectedError = Errors.NONE
    ).memberId

    // The group has become a consumer group.
    assertEquals(
      List(
        new ListGroupsResponseData.ListedGroup()
          .setGroupId(groupId)
          .setProtocolType("consumer")
          .setGroupState(ConsumerGroupState.RECONCILING.toString)
          .setGroupType(Group.GroupType.CONSUMER.toString)
      ),
      listGroups(
        statesFilter = List.empty,
        typesFilter = List(Group.GroupType.CONSUMER.toString)
      )
    )

    // Member 1 heartbeats and gets REBALANCE_IN_PROGRESS.
    heartbeat(
      groupId = groupId,
      generationId = 1,
      memberId = memberId1,
      expectedError = Errors.REBALANCE_IN_PROGRESS
    )

    // Member 1 commits offset. Start from version 1 because version 0 goes to ZK.
    for (version <- 1 to ApiKeys.OFFSET_COMMIT.latestVersion(isUnstableApiEnabled)) {
      for (partitionId <- 0 to 2) {
        commitOffset(
          groupId = groupId,
          memberId = memberId1,
          memberEpoch = 1,
          topic = "foo",
          partition = partitionId,
          offset = 100L + 10 * version + partitionId,
          expectedError = Errors.NONE,
          version = version.toShort
        )
      }
    }
    val committedOffset = 100L + 10 * ApiKeys.OFFSET_COMMIT.latestVersion(isUnstableApiEnabled)

    // Member 1 fetches offsets. Start from version 1 because version 0 goes to ZK.
    for (version <- 1 to ApiKeys.OFFSET_FETCH.latestVersion(isUnstableApiEnabled)) {
      assertEquals(
        new OffsetFetchResponseData.OffsetFetchResponseGroup()
          .setGroupId(groupId)
          .setTopics(List(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
              .setName("foo")
              .setPartitions(List(
                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                  .setPartitionIndex(0)
                  .setCommittedOffset(committedOffset),
                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                  .setPartitionIndex(1)
                  .setCommittedOffset(committedOffset + 1),
                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                  .setPartitionIndex(2)
                  .setCommittedOffset(committedOffset + 2)
              ).asJava)
          ).asJava),
        fetchOffsets(
          groupId = groupId,
          memberId = memberId1,
          memberEpoch = 1,
          partitions = List(
            new TopicPartition("foo", 0),
            new TopicPartition("foo", 1),
            new TopicPartition("foo", 2)
          ),
          requireStable = false,
          version = version.toShort
        )
      )
    }

    // Member 1 rejoins with current owned partitions.
    assertEquals(
      new JoinGroupResponseData()
        .setGenerationId(1)
        .setProtocolType("consumer")
        .setProtocolName("consumer-range")
        .setMemberId(memberId1),
      sendJoinRequest(
        groupId = groupId,
        memberId = memberId1,
        metadata = metadata(List(0, 1, 2))
      )
    )

    // Member 1 syncs.
    val partitionsOfMember1 = ConsumerProtocol.deserializeAssignment(ByteBuffer.wrap(
      syncGroupWithOldProtocol(
        groupId = groupId,
        memberId = memberId1,
        generationId = 1
      ).assignment()
    )).partitions()

    // Member 1 heartbeats and gets REBALANCE_IN_PROGRESS.
    heartbeat(
      groupId = groupId,
      generationId = 1,
      memberId = memberId1,
      expectedError = Errors.REBALANCE_IN_PROGRESS
    )

    // Member 1 rejoins with assigned partitions.
    assertEquals(
      new JoinGroupResponseData()
        .setGenerationId(2)
        .setProtocolType("consumer")
        .setProtocolName("consumer-range")
        .setMemberId(memberId1),
      sendJoinRequest(
        groupId = groupId,
        memberId = memberId1,
        metadata = metadata(partitionsOfMember1.asScala.toList.map(_.partition))
      )
    )

    // Member 1 syncs.
    verifySyncGroupWithOldProtocol(
      groupId = groupId,
      memberId = memberId1,
      generationId = 2,
      expectedAssignment = assignment(partitionsOfMember1.asScala.toList.map(_.partition))
    )

    // Member 2 rejoins to retrieve partitions pending assignment.
    consumerGroupHeartbeat(
      groupId = groupId,
      memberId = memberId2,
      memberEpoch = 2,
      rebalanceTimeoutMs = 5 * 60 * 1000,
      subscribedTopicNames = List("foo"),
      topicPartitions = List.empty,
      expectedError = Errors.NONE
    )

    // The group has been stabilized.
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

    if (!useStaticMembers) {
      // Downgrade the group by leaving member 2.
      leaveGroupWithNewProtocol(
        groupId = groupId,
        memberId = memberId2
      )
    } else {
      // Static member clients don't send explicit leave request when they are
      // shutdown for downgrade, so the downgrade has to be triggered by a
      // new static member join with old protocol and the same instance id.
      joinStaticConsumerGroupWithOldProtocol(
        groupId = groupId,
        groupInstanceId = instanceId2,
        metadata = metadata(List.empty),
        completeRebalance = false
      )
    }

    // The group has become a classic group.
    // If the downgrade is triggered by the static member replacement,
    // the group should remain STABLE, otherwise, a rebalance is triggered.
    assertEquals(
      List(
        new ListGroupsResponseData.ListedGroup()
          .setGroupId(groupId)
          .setProtocolType("consumer")
          .setGroupState(
            if (useStaticMembers)
              ClassicGroupState.STABLE.toString
            else
              ClassicGroupState.PREPARING_REBALANCE.toString
          )
          .setGroupType(Group.GroupType.CLASSIC.toString)
      ),
      listGroups(
        statesFilter = List.empty,
        typesFilter = List(Group.GroupType.CLASSIC.toString)
      )
    )
  }

  private def metadata(ownedPartitions: List[Int]): Array[Byte] = {
    ConsumerProtocol.serializeSubscription(
      new ConsumerPartitionAssignor.Subscription(
        Collections.singletonList("foo"),
        null,
        ownedPartitions.map(new TopicPartition("foo", _)).asJava
      )
    ).array
  }

  private def assignment(assignedPartitions: List[Int], userData: ByteBuffer = null): Array[Byte] = {
    ConsumerProtocol.serializeAssignment(
      new ConsumerPartitionAssignor.Assignment(
        assignedPartitions.map(new TopicPartition("foo", _)).asJava,
        userData
      )
    ).array
  }
}
