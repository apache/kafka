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

import org.apache.kafka.common.test.api.{ClusterConfigProperty, ClusterTest, ClusterTestDefaults, Type}
import org.apache.kafka.common.message.DescribeGroupsResponseData.DescribedGroup
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.test.ClusterInstance
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.coordinator.group.classic.ClassicGroupState
import org.junit.jupiter.api.Assertions.assertEquals

@ClusterTestDefaults(
  types = Array(Type.KRAFT),
  serverProperties = Array(
    new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
    new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1")
  )
)
class DeleteGroupsRequestTest(cluster: ClusterInstance) extends GroupCoordinatorBaseRequestTest(cluster) {
  @ClusterTest
  def testDeleteGroupsWithNewConsumerGroupProtocol(): Unit = {
    testDeleteGroups(true)
  }

  @ClusterTest
  def testDeleteGroupsWithOldConsumerGroupProtocol(): Unit = {
    testDeleteGroups(false)
  }

  private def testDeleteGroups(useNewProtocol: Boolean): Unit = {
    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    createOffsetsTopic()

    // Create the topic.
    val topicId = createTopic(
      topic = "foo",
      numPartitions = 3
    )

    // Join the consumer group. Note that we don't heartbeat here so we must use
    // a session long enough for the duration of the test.
    // We test DeleteGroups on empty and non-empty groups. Here we create the non-empty group.
    joinConsumerGroup(
      groupId = "grp-non-empty",
      useNewProtocol = useNewProtocol
    )

    for (version <- ApiKeys.DELETE_GROUPS.oldestVersion() to ApiKeys.DELETE_GROUPS.latestVersion(isUnstableApiEnabled)) {
      // Join the consumer group. Note that we don't heartbeat here so we must use
      // a session long enough for the duration of the test.
      val (memberId, memberEpoch) = joinConsumerGroup(
        groupId = "grp",
        useNewProtocol = useNewProtocol
      )

      // The member leaves the group so that grp is empty and ready to be deleted.
      leaveGroup(
        groupId = "grp",
        memberId = memberId,
        useNewProtocol = useNewProtocol,
        version = ApiKeys.LEAVE_GROUP.latestVersion(isUnstableApiEnabled)
      )

      deleteGroups(
        groupIds = List("grp-non-empty", "grp", ""),
        expectedErrors = List(Errors.NON_EMPTY_GROUP, Errors.NONE, Errors.GROUP_ID_NOT_FOUND),
        version = version.toShort
      )

      if (useNewProtocol) {
        commitOffset(
          groupId = "grp",
          memberId = memberId,
          memberEpoch = memberEpoch,
          topic = "foo",
          topicId = topicId,
          partition = 0,
          offset = 100L,
          expectedError = Errors.GROUP_ID_NOT_FOUND,
          version = ApiKeys.OFFSET_COMMIT.latestVersion(isUnstableApiEnabled)
        )
      } else {
        assertEquals(
          List(new DescribedGroup()
            .setGroupId("grp")
            .setGroupState(ClassicGroupState.DEAD.toString)
            .setErrorCode(Errors.GROUP_ID_NOT_FOUND.code)
            .setErrorMessage("Group grp not found.")
          ),
          describeGroups(List("grp"))
        )
      }
    }
  }
}
