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
import org.apache.kafka.coordinator.group.consumer.ConsumerGroup.ConsumerGroupState
import org.apache.kafka.coordinator.group.classic.ClassicGroupState
import org.apache.kafka.coordinator.group.Group
import org.junit.jupiter.api.Assertions.{assertEquals, fail}
import org.junit.jupiter.api.{Tag, Timeout}
import org.junit.jupiter.api.extension.ExtendWith

@Timeout(120)
@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
@ClusterTestDefaults(types = Array(Type.KRAFT))
@Tag("integration")
class ListGroupsRequestTest(cluster: ClusterInstance) extends GroupCoordinatorBaseRequestTest(cluster) {
  @ClusterTest(serverProperties = Array(
    new ClusterConfigProperty(key = "group.coordinator.rebalance.protocols", value = "classic,consumer"),
    new ClusterConfigProperty(key = "group.consumer.max.session.timeout.ms", value = "600000"),
    new ClusterConfigProperty(key = "group.consumer.session.timeout.ms", value = "600000"),
    new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
    new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
  ))
  def testListGroupsWithNewConsumerGroupProtocolAndNewGroupCoordinator(): Unit = {
    testListGroups(true)
  }

  @ClusterTest(serverProperties = Array(
    new ClusterConfigProperty(key = "group.coordinator.rebalance.protocols", value = "classic,consumer"),
    new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
    new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
  ))
  def testListGroupsWithOldConsumerGroupProtocolAndNewGroupCoordinator(): Unit = {
    testListGroups(false)
  }

  @ClusterTest(types = Array(Type.ZK, Type.KRAFT, Type.CO_KRAFT), serverProperties = Array(
    new ClusterConfigProperty(key = "group.coordinator.rebalance.protocols", value = "classic"),
    new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
    new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
  ))
  def testListGroupsWithOldConsumerGroupProtocolAndOldGroupCoordinator(): Unit = {
    testListGroups(false)
  }

  private def testListGroups(useNewProtocol: Boolean): Unit = {
    if (!isNewGroupCoordinatorEnabled && useNewProtocol) {
      fail("Cannot use the new protocol with the old group coordinator.")
    }

    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    createOffsetsTopic()

    // Create the topic.
    createTopic(
      topic = "foo",
      numPartitions = 3
    )

    for (version <- ApiKeys.LIST_GROUPS.oldestVersion() to ApiKeys.LIST_GROUPS.latestVersion(isUnstableApiEnabled)) {
      // Create grp-1 in old protocol and complete a rebalance. Grp-1 is in STABLE state.
      val (memberId1InGroup1, _) = joinDynamicConsumerGroupWithOldProtocol(groupId = "grp-1")
      val response1 = new ListGroupsResponseData.ListedGroup()
        .setGroupId("grp-1")
        .setProtocolType("consumer")
        .setGroupState(if (version >= 4) ClassicGroupState.STABLE.toString else "")
        .setGroupType(if (version >= 5) Group.GroupType.CLASSIC.toString else "")

      // Create grp-2 in old protocol without completing rebalance. Grp-2 is in COMPLETING_REBALANCE state.
      val (memberId1InGroup2, _) = joinDynamicConsumerGroupWithOldProtocol(groupId = "grp-2", completeRebalance = false)
      val response2 = new ListGroupsResponseData.ListedGroup()
        .setGroupId("grp-2")
        .setProtocolType("consumer")
        .setGroupState(if (version >= 4) ClassicGroupState.COMPLETING_REBALANCE.toString else "")
        .setGroupType(if (version >= 5) Group.GroupType.CLASSIC.toString else "")

      // Create grp-3 in old protocol and complete a rebalance. Then member 1 leaves grp-3. Grp-3 is in EMPTY state.
      val (memberId1InGroup3, _) = joinDynamicConsumerGroupWithOldProtocol(groupId = "grp-3")
      leaveGroup(groupId = "grp-3", memberId = memberId1InGroup3, useNewProtocol = false, ApiKeys.LEAVE_GROUP.latestVersion(isUnstableApiEnabled))
      val response3 = new ListGroupsResponseData.ListedGroup()
        .setGroupId("grp-3")
        .setProtocolType("consumer")
        .setGroupState(if (version >= 4) ClassicGroupState.EMPTY.toString else "")
        .setGroupType(if (version >= 5) Group.GroupType.CLASSIC.toString else "")

      var memberId1InGroup4: String = null
      var response4: ListGroupsResponseData.ListedGroup = null
      var memberId1InGroup5: String = null
      var memberId2InGroup5: String = null
      var response5: ListGroupsResponseData.ListedGroup = null
      var memberId1InGroup6: String = null
      var response6: ListGroupsResponseData.ListedGroup = null

      if (useNewProtocol) {
        // Create grp-4 in new protocol. Grp-4 is in STABLE state.
        memberId1InGroup4 = joinConsumerGroup("grp-4", useNewProtocol = true)._1
        response4 = new ListGroupsResponseData.ListedGroup()
          .setGroupId("grp-4")
          .setProtocolType("consumer")
          .setGroupState(if (version >= 4) ConsumerGroupState.STABLE.toString else "")
          .setGroupType(if (version >= 5) Group.GroupType.CONSUMER.toString else "")

        // Create grp-5 in new protocol. Then member 2 joins grp-5, triggering a rebalance. Grp-5 is in RECONCILING state.
        memberId1InGroup5 = joinConsumerGroup("grp-5", useNewProtocol = true)._1
        memberId2InGroup5 = joinConsumerGroup("grp-5", useNewProtocol = true)._1
        response5 = new ListGroupsResponseData.ListedGroup()
          .setGroupId("grp-5")
          .setProtocolType("consumer")
          .setGroupState(if (version >= 4) ConsumerGroupState.RECONCILING.toString else "")
          .setGroupType(if (version >= 5) Group.GroupType.CONSUMER.toString else "")

        // Create grp-6 in new protocol. Then member 1 leaves grp-6. Grp-6 is in Empty state.
        memberId1InGroup6 = joinConsumerGroup("grp-6", useNewProtocol = true)._1
        leaveGroup(groupId = "grp-6", memberId = memberId1InGroup6, useNewProtocol = true, ApiKeys.LEAVE_GROUP.latestVersion(isUnstableApiEnabled))
        response6 = new ListGroupsResponseData.ListedGroup()
          .setGroupId("grp-6")
          .setProtocolType("consumer")
          .setGroupState(if (version >= 4) ConsumerGroupState.EMPTY.toString else "")
          .setGroupType(if (version >= 5) Group.GroupType.CONSUMER.toString else "")
      }

      assertEquals(
        if (useNewProtocol)
          List(response1, response2, response3, response4, response5, response6).toSet
        else
          List(response1, response2, response3).toSet,
        listGroups(
          statesFilter = List.empty,
          typesFilter = List.empty,
          version = version.toShort
        ).toSet
      )

      // We need v4 or newer to request groups by states.
      if (version >= 4) {
        assertEquals(
          if (useNewProtocol) List(response4, response1) else List(response1),
          listGroups(
            statesFilter = List(ConsumerGroupState.STABLE.toString),
            typesFilter = List.empty,
            version = version.toShort
          )
        )

        assertEquals(
          if (useNewProtocol) List(response2, response5).toSet else List(response2).toSet,
          listGroups(
            statesFilter = List(
              ClassicGroupState.COMPLETING_REBALANCE.toString,
              ConsumerGroupState.RECONCILING.toString,
            ),
            typesFilter = List.empty,
            version = version.toShort
          ).toSet
        )

        assertEquals(
          if (useNewProtocol) List(response4, response1, response3, response6).toSet else List(response1, response3).toSet,
          listGroups(
            statesFilter = List(
              ClassicGroupState.STABLE.toString,
              ClassicGroupState.EMPTY.toString,
              ConsumerGroupState.EMPTY.toString
            ),
            typesFilter = List.empty,
            version = version.toShort
          ).toSet
        )

        assertEquals(
          List.empty,
          listGroups(
            statesFilter = List(ConsumerGroupState.ASSIGNING.toString),
            typesFilter = List.empty,
            version = version.toShort
          )
        )
      }

      // We need v5 or newer to request groups by types.
      if (version >= 5) {
        assertEquals(
          if (useNewProtocol) List(response4) else List.empty,
          listGroups(
            statesFilter = List(ConsumerGroupState.STABLE.toString),
            typesFilter = List(Group.GroupType.CONSUMER.toString),
            version = version.toShort
          )
        )

        assertEquals(
          if (useNewProtocol) List(response4, response5, response6).toSet else Set.empty,
          listGroups(
            statesFilter = List.empty,
            typesFilter = List(Group.GroupType.CONSUMER.toString),
            version = version.toShort
          ).toSet
        )

        assertEquals(
          List(response1, response2, response3).toSet,
          listGroups(
            statesFilter = List.empty,
            typesFilter = List(Group.GroupType.CLASSIC.toString),
            version = version.toShort
          ).toSet
        )
      }

      leaveGroup(groupId = "grp-1", memberId = memberId1InGroup1, useNewProtocol = false, ApiKeys.LEAVE_GROUP.latestVersion(isUnstableApiEnabled))
      leaveGroup(groupId = "grp-2", memberId = memberId1InGroup2, useNewProtocol = false, ApiKeys.LEAVE_GROUP.latestVersion(isUnstableApiEnabled))
      if (useNewProtocol) {
        leaveGroup(groupId = "grp-4", memberId = memberId1InGroup4, useNewProtocol = true, ApiKeys.LEAVE_GROUP.latestVersion(isUnstableApiEnabled))
        leaveGroup(groupId = "grp-5", memberId = memberId1InGroup5, useNewProtocol = true, ApiKeys.LEAVE_GROUP.latestVersion(isUnstableApiEnabled))
        leaveGroup(groupId = "grp-5", memberId = memberId2InGroup5, useNewProtocol = true, ApiKeys.LEAVE_GROUP.latestVersion(isUnstableApiEnabled))
      }

      deleteGroups(
        groupIds = if (useNewProtocol) List("grp-1", "grp-2", "grp-3", "grp-4", "grp-5", "grp-6") else List("grp-1", "grp-2", "grp-3"),
        expectedErrors = if (useNewProtocol) List.fill(6)(Errors.NONE) else List.fill(3)(Errors.NONE),
        version = ApiKeys.DELETE_GROUPS.latestVersion(isUnstableApiEnabled)
      )

      assertEquals(
        List.empty,
        listGroups(
          statesFilter = List.empty,
          typesFilter = List.empty,
          version = version.toShort
        )
      )
    }
  }
}
