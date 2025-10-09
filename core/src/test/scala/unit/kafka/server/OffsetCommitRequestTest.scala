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

import org.apache.kafka.common.Uuid
import org.apache.kafka.common.test.api.{ClusterConfigProperty, ClusterTest, ClusterTestDefaults, Type}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.test.ClusterInstance
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig

@ClusterTestDefaults(
  types = Array(Type.KRAFT),
  serverProperties = Array(
    new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
    new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1")
  )
)
class OffsetCommitRequestTest(cluster: ClusterInstance) extends GroupCoordinatorBaseRequestTest(cluster) {

  @ClusterTest
  def testOffsetCommitWithNewConsumerGroupProtocol(): Unit = {
    testOffsetCommit(true)
  }

  @ClusterTest
  def testOffsetCommitWithOldConsumerGroupProtocol(): Unit = {
    testOffsetCommit(false)
  }

  private def testOffsetCommit(useNewProtocol: Boolean): Unit = {
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
    val (memberId, memberEpoch) = joinConsumerGroup("grp", useNewProtocol)

    for (version <- ApiKeys.OFFSET_COMMIT.oldestVersion to ApiKeys.OFFSET_COMMIT.latestVersion(isUnstableApiEnabled)) {
      // Commit offset.
      commitOffset(
        groupId = "grp",
        memberId = memberId,
        memberEpoch = memberEpoch,
        topic = "foo",
        topicId = topicId,
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
        topicId = topicId,
        partition = 0,
        offset = 100L,
        expectedError =
          if (version >= 9) Errors.GROUP_ID_NOT_FOUND
          else Errors.ILLEGAL_GENERATION,
        version = version.toShort
      )

      // Commit offset with empty group id should fail.
      commitOffset(
        groupId = "",
        memberId = memberId,
        memberEpoch = memberEpoch,
        topic = "foo",
        topicId = topicId,
        partition = 0,
        offset = 100L,
        expectedError =
          if (version >= 9) Errors.GROUP_ID_NOT_FOUND
          else Errors.ILLEGAL_GENERATION,
        version = version.toShort
      )

      // Commit offset with unknown member id should fail.
      commitOffset(
        groupId = "grp",
        memberId = "",
        memberEpoch = memberEpoch,
        topic = "foo",
        topicId = topicId,
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
        topicId = topicId,
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
        topicId = topicId,
        partition = 0,
        offset = 100L,
        expectedError = Errors.NONE,
        version = version.toShort
      )

      // Commit offset to a group with an unknown topic id.
      if (version >= 10) {
        commitOffset(
          groupId = "grp",
          memberId = memberId,
          memberEpoch = memberEpoch,
          topic = "bar",
          topicId = Uuid.randomUuid(),
          partition = 0,
          offset = 100L,
          expectedError = Errors.UNKNOWN_TOPIC_ID,
          version = version.toShort
        )
      }
    }
  }
}
