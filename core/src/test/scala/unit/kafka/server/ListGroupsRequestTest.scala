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
import org.apache.kafka.coordinator.group.generic.GenericGroupState
import org.junit.jupiter.api.Assertions.{assertEquals, fail}
import org.junit.jupiter.api.{Tag, Timeout}
import org.junit.jupiter.api.extension.ExtendWith

@Timeout(120)
@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
@ClusterTestDefaults(clusterType = Type.KRAFT, brokers = 1)
@Tag("integration")
class ListGroupsRequestTest(cluster: ClusterInstance) extends GroupCoordinatorBaseRequestTest(cluster) {
  @ClusterTest(serverProperties = Array(
    new ClusterConfigProperty(key = "unstable.api.versions.enable", value = "true"),
    new ClusterConfigProperty(key = "group.coordinator.new.enable", value = "true"),
    new ClusterConfigProperty(key = "group.consumer.max.session.timeout.ms", value = "600000"),
    new ClusterConfigProperty(key = "group.consumer.session.timeout.ms", value = "600000"),
    new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
    new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
  ))
  def testListGroupsWithNewConsumerGroupProtocolAndNewGroupCoordinator(): Unit = {
    testListGroupsWithNewProtocol()
  }

  @ClusterTest(serverProperties = Array(
    new ClusterConfigProperty(key = "unstable.api.versions.enable", value = "true"),
    new ClusterConfigProperty(key = "group.coordinator.new.enable", value = "true"),
    new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
    new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
  ))
  def testListGroupsWithOldConsumerGroupProtocolAndNewGroupCoordinator(): Unit = {
    testListGroupsWithOldProtocol()
  }

  @ClusterTest(clusterType = Type.ALL, serverProperties = Array(
    new ClusterConfigProperty(key = "unstable.api.versions.enable", value = "false"),
    new ClusterConfigProperty(key = "group.coordinator.new.enable", value = "false"),
    new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
    new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
  ))
  def testListGroupsWithOldConsumerGroupProtocolAndOldGroupCoordinator(): Unit = {
    testListGroupsWithOldProtocol()
  }

  private def testListGroupsWithNewProtocol(): Unit = {
    if (!isNewGroupCoordinatorEnabled) {
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

      // Join the consumer group. Note that we don't heartbeat here so we must use
      // a session long enough for the duration of the test.
      val (memberId1, _) = joinConsumerGroup("grp", true)

      checkListedGroups(
        groupId = "grp",
        state = ConsumerGroupState.STABLE.toString,
        statesFilterExpectingEmptyListedGroups = List(ConsumerGroupState.ASSIGNING.toString),
        version = version
      )

      // Member 2 joins the group, triggering a rebalance.
      val (memberId2, _) = joinConsumerGroup("grp", true)

      checkListedGroups(
        groupId = "grp",
        state = ConsumerGroupState.RECONCILING.toString,
        statesFilterExpectingEmptyListedGroups = List(ConsumerGroupState.STABLE.toString),
        version = version
      )

      leaveGroup(
        groupId = "grp",
        memberId = memberId1,
        useNewProtocol = true
      )
      leaveGroup(
        groupId = "grp",
        memberId = memberId2,
        useNewProtocol = true
      )

      checkListedGroups(
        groupId = "grp",
        state = ConsumerGroupState.EMPTY.toString,
        statesFilterExpectingEmptyListedGroups = List(ConsumerGroupState.RECONCILING.toString),
        version = version
      )

      deleteGroups(
        groupIds = List("grp"),
        expectedErrors = List(Errors.NONE),
        version = ApiKeys.DELETE_GROUPS.latestVersion(isUnstableApiEnabled)
      )

      assertEquals(
        List.empty,
        listGroups(
          statesFilter = List.empty,
          version = version.toShort
        )
      )
    }
  }

  private def testListGroupsWithOldProtocol(): Unit = {

    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    createOffsetsTopic()

    // Create the topic.
    createTopic(
      topic = "foo",
      numPartitions = 3
    )

    for (version <- ApiKeys.LIST_GROUPS.oldestVersion() to ApiKeys.LIST_GROUPS.latestVersion(isUnstableApiEnabled)) {

      // Join the consumer group. Note that we don't heartbeat here so we must use
      // a session long enough for the duration of the test.
      val (memberId, memberEpoch) = joinConsumerGroupWithOldProtocol(groupId = "grp", completeRebalance = false)

      checkListedGroups(
        groupId = "grp",
        state = GenericGroupState.COMPLETING_REBALANCE.toString,
        statesFilterExpectingEmptyListedGroups = List(GenericGroupState.STABLE.toString),
        version = version
      )

      syncGroupWithOldProtocol(
        groupId = "grp",
        memberId = memberId,
        generationId = memberEpoch
      )

      checkListedGroups(
        groupId = "grp",
        state = GenericGroupState.STABLE.toString,
        statesFilterExpectingEmptyListedGroups = List(GenericGroupState.COMPLETING_REBALANCE.toString),
        version = version
      )

      leaveGroup(
        groupId = "grp",
        memberId = memberId,
        useNewProtocol = false
      )

      checkListedGroups(
        groupId = "grp",
        state = GenericGroupState.EMPTY.toString,
        statesFilterExpectingEmptyListedGroups = List(GenericGroupState.STABLE.toString),
        version = version
      )

      deleteGroups(
        groupIds = List("grp"),
        expectedErrors = List(Errors.NONE),
        version = ApiKeys.DELETE_GROUPS.latestVersion(isUnstableApiEnabled)
      )

      assertEquals(
        List.empty,
        listGroups(
          statesFilter = List.empty,
          version = version.toShort
        )
      )
    }
  }

  private def checkListedGroups(
    groupId: String,
    state: String,
    statesFilterExpectingEmptyListedGroups: List[String],
    version: Int
  ): Unit = {
    assertEquals(
      List(
        new ListGroupsResponseData.ListedGroup()
          .setGroupId(groupId)
          .setGroupState(if (version >= 4) state else "")
          .setProtocolType("consumer")
      ),
      listGroups(
        statesFilter = List.empty,
        version = version.toShort
      )
    )

    // we need v4 or newer to request groups by states
    if (version >= 4) {
      assertEquals(
        List.empty,
        listGroups(
          statesFilter = statesFilterExpectingEmptyListedGroups,
          version = version.toShort
        )
      )
    }
  }
}
