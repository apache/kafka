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
import kafka.test.annotation.{ClusterConfigProperty, ClusterTest, ClusterTestDefaults, Type}
import kafka.test.junit.ClusterTestExtensions

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.OffsetFetchResponseData
import org.apache.kafka.common.protocol.{ApiKeys, Errors}

import org.junit.jupiter.api.Assertions.{assertEquals, fail}
import org.junit.jupiter.api.{Tag, Timeout}
import org.junit.jupiter.api.extension.ExtendWith


import scala.jdk.CollectionConverters._

@Timeout(120)
@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
@ClusterTestDefaults(types = Array(Type.KRAFT))
@Tag("integration")
class OffsetFetchRequestTest(cluster: ClusterInstance) extends GroupCoordinatorBaseRequestTest(cluster) {

  @ClusterTest(serverProperties = Array(
    new ClusterConfigProperty(key = "group.coordinator.rebalance.protocols", value = "classic,consumer"),
    new ClusterConfigProperty(key = "group.consumer.max.session.timeout.ms", value = "600000"),
    new ClusterConfigProperty(key = "group.consumer.session.timeout.ms", value = "600000"),
    new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
    new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
  ))
  def testSingleGroupOffsetFetchWithNewConsumerGroupProtocolAndNewGroupCoordinator(): Unit = {
    testSingleGroupOffsetFetch(useNewProtocol = true, requireStable = true)
  }

  @ClusterTest(serverProperties = Array(
    new ClusterConfigProperty(key = "group.coordinator.rebalance.protocols", value = "classic,consumer"),
    new ClusterConfigProperty(key = "group.consumer.max.session.timeout.ms", value = "600000"),
    new ClusterConfigProperty(key = "group.consumer.session.timeout.ms", value = "600000"),
    new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
    new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
  ))
  def testSingleGroupOffsetFetchWithOldConsumerGroupProtocolAndNewGroupCoordinator(): Unit = {
    testSingleGroupOffsetFetch(useNewProtocol = false, requireStable = false)
  }

  @ClusterTest(types = Array(Type.ZK, Type.KRAFT, Type.CO_KRAFT), serverProperties = Array(
    new ClusterConfigProperty(key = "group.coordinator.rebalance.protocols", value = "classic"),
    new ClusterConfigProperty(key = "group.consumer.max.session.timeout.ms", value = "600000"),
    new ClusterConfigProperty(key = "group.consumer.session.timeout.ms", value = "600000"),
    new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
    new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
  ))
  def testSingleGroupOffsetFetchWithOldConsumerGroupProtocolAndOldGroupCoordinator(): Unit = {
    testSingleGroupOffsetFetch(useNewProtocol = false, requireStable = true)
  }

  @ClusterTest(serverProperties = Array(
    new ClusterConfigProperty(key = "group.coordinator.rebalance.protocols", value = "classic,consumer"),
    new ClusterConfigProperty(key = "group.consumer.max.session.timeout.ms", value = "600000"),
    new ClusterConfigProperty(key = "group.consumer.session.timeout.ms", value = "600000"),
    new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
    new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
  ))
  def testSingleGroupAllOffsetFetchWithNewConsumerGroupProtocolAndNewGroupCoordinator(): Unit = {
    testSingleGroupAllOffsetFetch(useNewProtocol = true, requireStable = true)
  }

  @ClusterTest(serverProperties = Array(
    new ClusterConfigProperty(key = "group.coordinator.rebalance.protocols", value = "classic,consumer"),
    new ClusterConfigProperty(key = "group.consumer.max.session.timeout.ms", value = "600000"),
    new ClusterConfigProperty(key = "group.consumer.session.timeout.ms", value = "600000"),
    new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
    new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
  ))
  def testSingleGroupAllOffsetFetchWithOldConsumerGroupProtocolAndNewGroupCoordinator(): Unit = {
    testSingleGroupAllOffsetFetch(useNewProtocol = false, requireStable = false)
  }

  @ClusterTest(types = Array(Type.ZK, Type.KRAFT, Type.CO_KRAFT), serverProperties = Array(
    new ClusterConfigProperty(key = "group.coordinator.rebalance.protocols", value = "classic"),
    new ClusterConfigProperty(key = "group.consumer.max.session.timeout.ms", value = "600000"),
    new ClusterConfigProperty(key = "group.consumer.session.timeout.ms", value = "600000"),
    new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
    new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
  ))
  def testSingleGroupAllOffsetFetchWithOldConsumerGroupProtocolAndOldGroupCoordinator(): Unit = {
    testSingleGroupAllOffsetFetch(useNewProtocol = false, requireStable = true)
  }

  @ClusterTest(serverProperties = Array(
    new ClusterConfigProperty(key = "group.coordinator.rebalance.protocols", value = "classic,consumer"),
    new ClusterConfigProperty(key = "group.consumer.max.session.timeout.ms", value = "600000"),
    new ClusterConfigProperty(key = "group.consumer.session.timeout.ms", value = "600000"),
    new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
    new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
  ))
  def testMultiGroupsOffsetFetchWithNewConsumerGroupProtocolAndNewGroupCoordinator(): Unit = {
    testMultipleGroupsOffsetFetch(useNewProtocol = true, requireStable = true)
  }

  @ClusterTest(serverProperties = Array(
    new ClusterConfigProperty(key = "group.coordinator.rebalance.protocols", value = "classic,consumer"),
    new ClusterConfigProperty(key = "group.consumer.max.session.timeout.ms", value = "600000"),
    new ClusterConfigProperty(key = "group.consumer.session.timeout.ms", value = "600000"),
    new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
    new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
  ))
  def testMultiGroupsOffsetFetchWithOldConsumerGroupProtocolAndNewGroupCoordinator(): Unit = {
    testMultipleGroupsOffsetFetch(useNewProtocol = false, requireStable = false)
  }

  @ClusterTest(types = Array(Type.ZK, Type.KRAFT, Type.CO_KRAFT), serverProperties = Array(
    new ClusterConfigProperty(key = "group.coordinator.rebalance.protocols", value = "classic"),
    new ClusterConfigProperty(key = "group.consumer.max.session.timeout.ms", value = "600000"),
    new ClusterConfigProperty(key = "group.consumer.session.timeout.ms", value = "600000"),
    new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
    new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
  ))
  def testMultiGroupsOffsetFetchWithOldConsumerGroupProtocolAndOldGroupCoordinator(): Unit = {
    testMultipleGroupsOffsetFetch(useNewProtocol = false, requireStable = true)
  }

  private def testSingleGroupOffsetFetch(useNewProtocol: Boolean, requireStable: Boolean): Unit = {
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

    // Commit offsets.
    for (partitionId <- 0 to 2) {
      commitOffset(
        groupId = "grp",
        memberId = memberId,
        memberEpoch = memberEpoch,
        topic = "foo",
        partition = partitionId,
        offset = 100L + partitionId,
        expectedError = Errors.NONE,
        version = ApiKeys.OFFSET_COMMIT.latestVersion(isUnstableApiEnabled)
      )
    }

    // Start from version 1 because version 0 goes to ZK.
    for (version <- 1 to ApiKeys.OFFSET_FETCH.latestVersion(isUnstableApiEnabled)) {
      // Fetch with partitions.
      assertEquals(
        new OffsetFetchResponseData.OffsetFetchResponseGroup()
          .setGroupId("grp")
          .setTopics(List(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
              .setName("foo")
              .setPartitions(List(
                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                  .setPartitionIndex(0)
                  .setCommittedOffset(100L),
                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                  .setPartitionIndex(1)
                  .setCommittedOffset(101L),
                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                  .setPartitionIndex(5)
                  .setCommittedOffset(-1L)
              ).asJava)
          ).asJava),
        fetchOffsets(
          groupId = "grp",
          memberId = memberId,
          memberEpoch = memberEpoch,
          partitions = List(
            new TopicPartition("foo", 0),
            new TopicPartition("foo", 1),
            new TopicPartition("foo", 5) // This one does not exist.
          ),
          requireStable = requireStable,
          version = version.toShort
        )
      )

      // Fetch with unknown group id.
      assertEquals(
        new OffsetFetchResponseData.OffsetFetchResponseGroup()
          .setGroupId("unknown")
          .setTopics(List(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
              .setName("foo")
              .setPartitions(List(
                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                  .setPartitionIndex(0)
                  .setCommittedOffset(-1L),
                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                  .setPartitionIndex(1)
                  .setCommittedOffset(-1L),
                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                  .setPartitionIndex(5)
                  .setCommittedOffset(-1L)
              ).asJava)
          ).asJava),
        fetchOffsets(
          groupId = "unknown",
          memberId = memberId,
          memberEpoch = memberEpoch,
          partitions = List(
            new TopicPartition("foo", 0),
            new TopicPartition("foo", 1),
            new TopicPartition("foo", 5) // This one does not exist.
          ),
          requireStable = requireStable,
          version = version.toShort
        )
      )

      // Fetch with unknown group id with unknown topic or nonexistent partition.
      assertEquals(
        new OffsetFetchResponseData.OffsetFetchResponseGroup()
          .setGroupId("unknown")
          .setTopics(List(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
              .setName("foo")
              .setPartitions(List(
                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                  .setPartitionIndex(0)
                  .setCommittedOffset(-1L),
                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                  .setPartitionIndex(5)
                  .setCommittedOffset(-1L)
              ).asJava),
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
              .setName("foo-unknown")
              .setPartitions(List(
                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                  .setPartitionIndex(1)
                  .setCommittedOffset(-1L)
              ).asJava),
          ).asJava),
        fetchOffsets(
          groupId = "unknown",
          memberId = memberId,
          memberEpoch = memberEpoch,
          partitions = List(
            new TopicPartition("foo", 0),
            new TopicPartition("foo-unknown", 1),
            new TopicPartition("foo", 5) // This one does not exist.
          ),
          requireStable = requireStable,
          version = version.toShort
        )
      )

      if (useNewProtocol && version >= 9) {
        // Fetch with unknown member id.
        assertEquals(
          new OffsetFetchResponseData.OffsetFetchResponseGroup()
            .setGroupId("grp")
            .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code),
          fetchOffsets(
            groupId = "grp",
            memberId = "",
            memberEpoch = memberEpoch,
            partitions = List.empty,
            requireStable = requireStable,
            version = version.toShort
          )
        )

        // Fetch with stale member epoch.
        assertEquals(
          new OffsetFetchResponseData.OffsetFetchResponseGroup()
            .setGroupId("grp")
            .setErrorCode(Errors.STALE_MEMBER_EPOCH.code),
          fetchOffsets(
            groupId = "grp",
            memberId = memberId,
            memberEpoch = memberEpoch + 1,
            partitions = List.empty,
            requireStable = requireStable,
            version = version.toShort
          )
        )
      }
    }
  }

  private def testSingleGroupAllOffsetFetch(useNewProtocol: Boolean, requireStable: Boolean): Unit = {
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

    // Commit offsets.
    for (partitionId <- 0 to 2) {
      commitOffset(
        groupId = "grp",
        memberId = memberId,
        memberEpoch = memberEpoch,
        topic = "foo",
        partition = partitionId,
        offset = 100L + partitionId,
        expectedError = Errors.NONE,
        version = ApiKeys.OFFSET_COMMIT.latestVersion(isUnstableApiEnabled)
      )
    }

    // Start from version 2 because fetching all partitions is not
    // supported before.
    for (version <- 2 to ApiKeys.OFFSET_FETCH.latestVersion(isUnstableApiEnabled)) {
      // Fetch all partitions.
      assertEquals(
        new OffsetFetchResponseData.OffsetFetchResponseGroup()
          .setGroupId("grp")
          .setTopics(List(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
              .setName("foo")
              .setPartitions(List(
                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                  .setPartitionIndex(0)
                  .setCommittedOffset(100L),
                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                  .setPartitionIndex(1)
                  .setCommittedOffset(101L),
                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                  .setPartitionIndex(2)
                  .setCommittedOffset(102L)
              ).asJava)
          ).asJava),
        fetchOffsets(
          groupId = "grp",
          memberId = memberId,
          memberEpoch = memberEpoch,
          partitions = null,
          requireStable = requireStable,
          version = version.toShort
        )
      )

      // Fetch with a unknown group id.
      assertEquals(
        new OffsetFetchResponseData.OffsetFetchResponseGroup()
          .setGroupId("unknown"),
        fetchOffsets(
          groupId = "unknown",
          memberId = memberId,
          memberEpoch = memberEpoch,
          partitions = null,
          requireStable = requireStable,
          version = version.toShort
        )
      )

      if (useNewProtocol && version >= 9) {
        // Fetch with an unknown member id.
        assertEquals(
          new OffsetFetchResponseData.OffsetFetchResponseGroup()
            .setGroupId("grp")
            .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code),
          fetchOffsets(
            groupId = "grp",
            memberId = "",
            memberEpoch = memberEpoch,
            partitions = null,
            requireStable = requireStable,
            version = version.toShort
          )
        )

        // Fetch with a stable member epoch.
        assertEquals(
          new OffsetFetchResponseData.OffsetFetchResponseGroup()
            .setGroupId("grp")
            .setErrorCode(Errors.STALE_MEMBER_EPOCH.code),
          fetchOffsets(
            groupId = "grp",
            memberId = memberId,
            memberEpoch = memberEpoch + 1,
            partitions = null,
            requireStable = requireStable,
            version = version.toShort
          )
        )
      }
    }
  }

  private def testMultipleGroupsOffsetFetch(useNewProtocol: Boolean, requireStable: Boolean): Unit = {
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

    // Create groups and commit offsets.
    List("grp-0", "grp-1", "grp-2").foreach { groupId =>
      // Join the consumer group. Note that we don't heartbeat here so we must use
      // a session long enough for the duration of the test.
      val (memberId, memberEpoch) = joinConsumerGroup(groupId, useNewProtocol)

      for (partitionId <- 0 to 2) {
        commitOffset(
          groupId = groupId,
          memberId = memberId,
          memberEpoch = memberEpoch,
          topic = "foo",
          partition = partitionId,
          offset = 100L + partitionId,
          expectedError = Errors.NONE,
          version = ApiKeys.OFFSET_COMMIT.latestVersion(isUnstableApiEnabled)
        )
      }
    }

    // Start from version 8 because older versions do not support
    // fetch offsets for multiple groups.
    for (version <- 8 to ApiKeys.OFFSET_FETCH.latestVersion(isUnstableApiEnabled)) {
      assertEquals(
        List(
          // Fetch foo-0, foo-1 and foo-5.
          new OffsetFetchResponseData.OffsetFetchResponseGroup()
            .setGroupId("grp-0")
            .setTopics(List(
              new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setPartitions(List(
                  new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                    .setPartitionIndex(0)
                    .setCommittedOffset(100L),
                  new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                    .setPartitionIndex(1)
                    .setCommittedOffset(101L),
                  new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                    .setPartitionIndex(5)
                    .setCommittedOffset(-1L)
                ).asJava)
            ).asJava),
          // Fetch all partitions.
          new OffsetFetchResponseData.OffsetFetchResponseGroup()
            .setGroupId("grp-1")
            .setTopics(List(
              new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setPartitions(List(
                  new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                    .setPartitionIndex(0)
                    .setCommittedOffset(100L),
                  new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                    .setPartitionIndex(1)
                    .setCommittedOffset(101L),
                  new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                    .setPartitionIndex(2)
                    .setCommittedOffset(102L)
                ).asJava)
            ).asJava),
          // Fetch no partitions.
          new OffsetFetchResponseData.OffsetFetchResponseGroup()
            .setGroupId("grp-2")
            .setTopics(List.empty.asJava),
          // Fetch unknown group.
          new OffsetFetchResponseData.OffsetFetchResponseGroup()
            .setGroupId("grp-3")
            .setTopics(List(
              new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setPartitions(List(
                  new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                    .setPartitionIndex(0)
                    .setCommittedOffset(-1L)
                ).asJava)
            ).asJava),
          // Fetch unknown group with unknown topic or nonexistent partition.
          new OffsetFetchResponseData.OffsetFetchResponseGroup()
            .setGroupId("grp-4")
            .setTopics(List(
              new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setPartitions(List(
                  new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                    .setPartitionIndex(5)
                    .setCommittedOffset(-1L)
                ).asJava),
              new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo-unknown")
                .setPartitions(List(
                  new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                    .setPartitionIndex(0)
                    .setCommittedOffset(-1L)
                ).asJava)
            ).asJava),
        ).toSet,
        fetchOffsets(
          groups = Map(
            "grp-0" -> List(
              new TopicPartition("foo", 0),
              new TopicPartition("foo", 1),
              new TopicPartition("foo", 5) // This one does not exist.
            ),
            "grp-1" -> null,
            "grp-2" -> List.empty,
            "grp-3" -> List(
              new TopicPartition("foo", 0)
            ),
            "grp-4" -> List(
              new TopicPartition("foo-unknown", 0), // unknown topic id
              new TopicPartition("foo", 5)  // The partition doesn't exist.
            ),
          ),
          requireStable = requireStable,
          version = version.toShort
        ).toSet
      )
    }
  }
}
