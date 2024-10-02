/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server

import kafka.test.ClusterInstance
import kafka.test.annotation.{ClusterConfigProperty, ClusterFeature, ClusterTest, ClusterTestDefaults, Type}
import kafka.test.junit.ClusterTestExtensions
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.server.common.Features
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.extension.ExtendWith

@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
@ClusterTestDefaults(types = Array(Type.KRAFT))
class OffsetCommitRequestTest(cluster: ClusterInstance) extends GroupCoordinatorBaseRequestTest(cluster) {

  @ClusterTest(
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1")
    ),
    features = Array(
      new ClusterFeature(feature = Features.TRANSACTION_VERSION, version = 2),
      new ClusterFeature(feature = Features.GROUP_VERSION, version = 1)
    )
  )
  def testOffsetCommitWithNewConsumerGroupProtocolAndNewGroupCoordinator(): Unit = {
    testOffsetCommit(true)
  }

  @ClusterTest(
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1")
    )
  )
  def testOffsetCommitWithOldConsumerGroupProtocolAndNewGroupCoordinator(): Unit = {
    testOffsetCommit(false)
  }

  @ClusterTest(types = Array(Type.KRAFT, Type.CO_KRAFT), serverProperties = Array(
    new ClusterConfigProperty(key = GroupCoordinatorConfig.NEW_GROUP_COORDINATOR_ENABLE_CONFIG, value = "false"),
    new ClusterConfigProperty(key = GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG, value = "classic"),
    new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
    new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1")
  ))
  def testOffsetCommitWithOldConsumerGroupProtocolAndOldGroupCoordinator(): Unit = {
    testOffsetCommit(false)
  }

  private def testOffsetCommit(useNewProtocol: Boolean): Unit = {
    if (useNewProtocol && !isNewGroupCoordinatorEnabled) {
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

    // Join the consumer group. Note that we don't heartbeat here so we must use
    // a session long enough for the duration of the test.
    val (memberId, memberEpoch) = joinConsumerGroup("grp", useNewProtocol)

    // Start from version 1 because version 0 goes to ZK.
    for (version <- 1 to ApiKeys.OFFSET_COMMIT.latestVersion(isUnstableApiEnabled)) {
      // Commit offset.
      commitOffset(
        groupId = "grp",
        memberId = memberId,
        memberEpoch = memberEpoch,
        topic = "foo",
        partition = 0,
        offset = 100L,
        expectedError = if (useNewProtocol && version < 9) Errors.UNSUPPORTED_VERSION else Errors.NONE,
        version = version.toShort
      )

      // Commit offset with unknown group should fail.
      commitOffset(
        groupId = "unknown",
        memberId = memberId,
        memberEpoch = memberEpoch,
        topic = "foo",
        partition = 0,
        offset = 100L,
        expectedError =
          if (isNewGroupCoordinatorEnabled && version >= 9) Errors.GROUP_ID_NOT_FOUND
          else Errors.ILLEGAL_GENERATION,
        version = version.toShort
      )

      // Commit offset with empty group id should fail.
      commitOffset(
        groupId = "",
        memberId = memberId,
        memberEpoch = memberEpoch,
        topic = "foo",
        partition = 0,
        offset = 100L,
        expectedError =
          if (isNewGroupCoordinatorEnabled && version >= 9) Errors.GROUP_ID_NOT_FOUND
          else Errors.ILLEGAL_GENERATION,
        version = version.toShort
      )

      // Commit offset with unknown member id should fail.
      commitOffset(
        groupId = "grp",
        memberId = "",
        memberEpoch = memberEpoch,
        topic = "foo",
        partition = 0,
        offset = 100L,
        expectedError = Errors.UNKNOWN_MEMBER_ID,
        version = version.toShort
      )

      // Commit offset with stale member epoch should fail.
      commitOffset(
        groupId = "grp",
        memberId = memberId,
        memberEpoch = memberEpoch + 1,
        topic = "foo",
        partition = 0,
        offset = 100L,
        expectedError =
          if (useNewProtocol && version >= 9) Errors.STALE_MEMBER_EPOCH
          else if (useNewProtocol) Errors.UNSUPPORTED_VERSION
          else Errors.ILLEGAL_GENERATION,
        version = version.toShort
      )

      // Commit offset to a group without member id/epoch should succeed.
      // This simulate a call from the admin client.
      commitOffset(
        groupId = "other-grp",
        memberId = "",
        memberEpoch = -1,
        topic = "foo",
        partition = 0,
        offset = 100L,
        expectedError = Errors.NONE,
        version = version.toShort
      )
    }
  }
}
