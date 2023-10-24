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
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.junit.jupiter.api.{Tag, Timeout}
import org.junit.jupiter.api.extension.ExtendWith

@Timeout(120)
@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
@ClusterTestDefaults(clusterType = Type.KRAFT, brokers = 1)
@Tag("integration")
class HeartbeatRequestTest(cluster: ClusterInstance) extends GroupCoordinatorBaseRequestTest(cluster) {
  @ClusterTest(serverProperties = Array(
    new ClusterConfigProperty(key = "unstable.api.versions.enable", value = "true"),
    new ClusterConfigProperty(key = "group.coordinator.new.enable", value = "true"),
    new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
    new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
  ))
  def testHeartbeatWithOldConsumerGroupProtocolAndNewGroupCoordinator(): Unit = {
    testHeartbeat()
  }

  @ClusterTest(clusterType = Type.ALL, serverProperties = Array(
    new ClusterConfigProperty(key = "unstable.api.versions.enable", value = "false"),
    new ClusterConfigProperty(key = "group.coordinator.new.enable", value = "false"),
    new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
    new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
  ))
  def testHeartbeatWithOldConsumerGroupProtocolAndOldGroupCoordinator(): Unit = {
    testHeartbeat()
  }

  private def testHeartbeat(): Unit = {

    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    createOffsetsTopic()

    // Create the topic.
    createTopic(
      topic = "foo",
      numPartitions = 3
    )

    for (version <- ApiKeys.HEARTBEAT.oldestVersion() to ApiKeys.HEARTBEAT.latestVersion(isUnstableApiEnabled)) {

      val (memberId, memberEpoch) = joinDynamicConsumerGroupWithOldProtocol(
        groupId = "grp",
        completeRebalance = false
      )

      // Heartbeat with unknown group id and unknown member id.
      heartbeat(
        groupId = "grp-unknown",
        memberId = "member-id-unknown",
        generationId = -1,
        expectedError = Errors.UNKNOWN_MEMBER_ID,
        version = version.toShort
      )

      // Heartbeat with unknown group id.
      heartbeat(
        groupId = "grp-unknown",
        memberId = memberId,
        generationId = -1,
        expectedError = Errors.UNKNOWN_MEMBER_ID,
        version = version.toShort
      )

      // Heartbeat with unknown member id.
      heartbeat(
        groupId = "grp",
        memberId = "member-id-unknown",
        generationId = -1,
        expectedError = Errors.UNKNOWN_MEMBER_ID,
        version = version.toShort
      )

      // Heartbeat with unmatched generation id.
      heartbeat(
        groupId = "grp",
        memberId = memberId,
        generationId = -1,
        expectedError = Errors.ILLEGAL_GENERATION,
        version = version.toShort
      )

      // Heartbeat COMPLETING_REBALANCE group.
      heartbeat(
        groupId = "grp",
        memberId = memberId,
        generationId = memberEpoch,
        version = version.toShort
      )

      syncGroupWithOldProtocol(
        groupId = "grp",
        memberId = memberId,
        generationId = memberEpoch
      )

      // Heartbeat STABLE group.
      heartbeat(
        groupId = "grp",
        memberId = memberId,
        generationId = memberEpoch,
        version = version.toShort
      )

      leaveGroup(
        groupId = "grp",
        memberId = memberId,
        useNewProtocol = false,
        version = ApiKeys.LEAVE_GROUP.latestVersion(isUnstableApiEnabled)
      )

      // Heartbeat empty group.
      heartbeat(
        groupId = "grp",
        memberId = memberId,
        generationId = -1,
        expectedError = Errors.UNKNOWN_MEMBER_ID,
        version = version.toShort
      )
    }
  }
}
