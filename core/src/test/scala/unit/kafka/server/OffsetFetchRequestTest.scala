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

import kafka.utils.TestUtils
import org.apache.kafka.common.test.api.{ClusterConfigProperty, ClusterTest, ClusterTestDefaults, Type}
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.message.{OffsetFetchRequestData, OffsetFetchResponseData}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.test.ClusterInstance
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.junit.jupiter.api.Assertions.assertEquals

import scala.jdk.CollectionConverters._

@ClusterTestDefaults(
  types = Array(Type.KRAFT),
  serverProperties = Array(
    new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
    new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1")
  )
)
class OffsetFetchRequestTest(cluster: ClusterInstance) extends GroupCoordinatorBaseRequestTest(cluster) {

  @ClusterTest
  def testSingleGroupOffsetFetchWithNewConsumerGroupProtocol(): Unit = {
    testSingleGroupOffsetFetch(useNewProtocol = true, requireStable = true)
  }

  @ClusterTest
  def testSingleGroupOffsetFetchWithOldConsumerGroupProtocol(): Unit = {
    testSingleGroupOffsetFetch(useNewProtocol = false, requireStable = false)
  }

  @ClusterTest
  def testSingleGroupAllOffsetFetchWithNewConsumerGroupProtocol(): Unit = {
    testSingleGroupAllOffsetFetch(useNewProtocol = true, requireStable = true)
  }

  @ClusterTest
  def testSingleGroupAllOffsetFetchWithOldConsumerGroupProtocol(): Unit = {
    testSingleGroupAllOffsetFetch(useNewProtocol = false, requireStable = false)
  }

  @ClusterTest
  def testMultiGroupsOffsetFetchWithNewConsumerGroupProtocol(): Unit = {
    testMultipleGroupsOffsetFetch(useNewProtocol = true, requireStable = true)
  }

  @ClusterTest
  def testMultiGroupsOffsetFetchWithOldConsumerGroupProtocol(): Unit = {
    testMultipleGroupsOffsetFetch(useNewProtocol = false, requireStable = false)
  }

  private def testSingleGroupOffsetFetch(useNewProtocol: Boolean, requireStable: Boolean): Unit = {
    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    createOffsetsTopic()

    val unknownTopicId = Uuid.randomUuid()

    // Create the topic.
    val topicId = createTopic(
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
        topicId = topicId,
        partition = partitionId,
        offset = 100L + partitionId,
        expectedError = Errors.NONE,
        version = ApiKeys.OFFSET_COMMIT.latestVersion(isUnstableApiEnabled)
      )
    }

    for (version <- 1 to ApiKeys.OFFSET_FETCH.latestVersion(isUnstableApiEnabled)) {
      // Fetch with partitions.
      assertEquals(
        new OffsetFetchResponseData.OffsetFetchResponseGroup()
          .setGroupId("grp")
          .setTopics(List(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
              .setName(if (version < 10) "foo" else "")
              .setTopicId(if (version >= 10) topicId else Uuid.ZERO_UUID)
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
          group = new OffsetFetchRequestData.OffsetFetchRequestGroup()
            .setGroupId("grp")
            .setMemberId(memberId)
            .setMemberEpoch(memberEpoch)
            .setTopics(List(
              new OffsetFetchRequestData.OffsetFetchRequestTopics()
                .setName("foo")
                .setTopicId(topicId)
                .setPartitionIndexes(List[Integer](0, 1, 5).asJava) // 5 does not exist.
            ).asJava),
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
              .setName(if (version < 10) "foo" else "")
              .setTopicId(if (version >= 10) topicId else Uuid.ZERO_UUID)
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
          group = new OffsetFetchRequestData.OffsetFetchRequestGroup()
            .setGroupId("unknown")
            .setMemberId(memberId)
            .setMemberEpoch(memberEpoch)
            .setTopics(List(
              new OffsetFetchRequestData.OffsetFetchRequestTopics()
                .setName("foo")
                .setTopicId(topicId)
                .setPartitionIndexes(List[Integer](0, 1, 5).asJava) // 5 does not exist.
            ).asJava),
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
              .setName(if (version < 10) "foo" else "")
              .setTopicId(if (version >= 10) topicId else Uuid.ZERO_UUID)
              .setPartitions(List(
                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                  .setPartitionIndex(0)
                  .setCommittedOffset(-1L),
                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                  .setPartitionIndex(5)
                  .setCommittedOffset(-1L)
              ).asJava),
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
              .setName(if (version < 10) "foo-unknown" else "")
              .setTopicId(if (version >= 10) unknownTopicId else Uuid.ZERO_UUID)
              .setPartitions(List(
                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                  .setPartitionIndex(1)
                  .setCommittedOffset(-1L)
                  .setErrorCode(if (version >= 10) Errors.UNKNOWN_TOPIC_ID.code else Errors.NONE.code)
              ).asJava),
          ).asJava),
        fetchOffsets(
          group = new OffsetFetchRequestData.OffsetFetchRequestGroup()
            .setGroupId("unknown")
            .setMemberId(memberId)
            .setMemberEpoch(memberEpoch)
            .setTopics(List(
              new OffsetFetchRequestData.OffsetFetchRequestTopics()
                .setName("foo")
                .setTopicId(topicId)
                .setPartitionIndexes(List[Integer](0, 5).asJava), // 5 does not exist.
              new OffsetFetchRequestData.OffsetFetchRequestTopics()
                .setName("foo-unknown")
                .setTopicId(unknownTopicId)
                .setPartitionIndexes(List[Integer](1).asJava) // 5 does not exist.
            ).asJava),
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
            group = new OffsetFetchRequestData.OffsetFetchRequestGroup()
              .setGroupId("grp")
              .setMemberId("")
              .setMemberEpoch(memberEpoch)
              .setTopics(List.empty.asJava),
            requireStable = requireStable,
            version = version.toShort
          )
        )

        // Fetch with empty group id.
        assertEquals(
          new OffsetFetchResponseData.OffsetFetchResponseGroup()
            .setGroupId("")
            .setTopics(List(
              new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName(if (version < 10) "foo" else "")
                .setTopicId(if (version >= 10) topicId else Uuid.ZERO_UUID)
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
            group = new OffsetFetchRequestData.OffsetFetchRequestGroup()
              .setGroupId("")
              .setMemberId(memberId)
              .setMemberEpoch(memberEpoch)
              .setTopics(List(
                new OffsetFetchRequestData.OffsetFetchRequestTopics()
                  .setName("foo")
                  .setTopicId(topicId)
                  .setPartitionIndexes(List[Integer](0, 1, 5).asJava) // 5 does not exist.
              ).asJava),
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
            group = new OffsetFetchRequestData.OffsetFetchRequestGroup()
              .setGroupId("grp")
              .setMemberId(memberId)
              .setMemberEpoch(memberEpoch + 1)
              .setTopics(List.empty.asJava),
            requireStable = requireStable,
            version = version.toShort
          )
        )
      }
    }
  }

  private def testSingleGroupAllOffsetFetch(useNewProtocol: Boolean, requireStable: Boolean): Unit = {
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

    // Commit offsets.
    for (partitionId <- 0 to 2) {
      commitOffset(
        groupId = "grp",
        memberId = memberId,
        memberEpoch = memberEpoch,
        topic = "foo",
        topicId = topicId,
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
              .setName(if (version < 10) "foo" else "")
              .setTopicId(if (version >= 10) topicId else Uuid.ZERO_UUID)
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
          group = new OffsetFetchRequestData.OffsetFetchRequestGroup()
            .setGroupId("grp")
            .setMemberId(memberId)
            .setMemberEpoch(memberEpoch)
            .setTopics(null),
          requireStable = requireStable,
          version = version.toShort
        )
      )

      // Fetch with a unknown group id.
      assertEquals(
        new OffsetFetchResponseData.OffsetFetchResponseGroup()
          .setGroupId("unknown"),
        fetchOffsets(
          group = new OffsetFetchRequestData.OffsetFetchRequestGroup()
            .setGroupId("unknown")
            .setMemberId(memberId)
            .setMemberEpoch(memberEpoch)
            .setTopics(null),
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
            group = new OffsetFetchRequestData.OffsetFetchRequestGroup()
              .setGroupId("grp")
              .setMemberId("")
              .setMemberEpoch(memberEpoch)
              .setTopics(null),
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
            group = new OffsetFetchRequestData.OffsetFetchRequestGroup()
              .setGroupId("grp")
              .setMemberId(memberId)
              .setMemberEpoch(memberEpoch + 1)
              .setTopics(null),
            requireStable = requireStable,
            version = version.toShort
          )
        )
      }
    }
  }

  private def testMultipleGroupsOffsetFetch(useNewProtocol: Boolean, requireStable: Boolean): Unit = {
    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    createOffsetsTopic()

    val unknownTopicId = Uuid.randomUuid()

    // Create the topic.
    val topicId = createTopic(
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
          topicId = topicId,
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
                .setName(if (version < 10) "foo" else "")
                .setTopicId(if (version >= 10) topicId else Uuid.ZERO_UUID)
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
                .setName(if (version < 10) "foo" else "")
                .setTopicId(if (version >= 10) topicId else Uuid.ZERO_UUID)
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
                .setName(if (version < 10) "foo" else "")
                .setTopicId(if (version >= 10) topicId else Uuid.ZERO_UUID)
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
                .setName(if (version < 10) "foo" else "")
                .setTopicId(if (version >= 10) topicId else Uuid.ZERO_UUID)
                .setPartitions(List(
                  new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                    .setPartitionIndex(5)
                    .setCommittedOffset(-1L)
                ).asJava),
              new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName(if (version < 10) "foo-unknown" else "")
                .setTopicId(if (version >= 10) unknownTopicId else Uuid.ZERO_UUID)
                .setPartitions(List(
                  new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                    .setPartitionIndex(0)
                    .setCommittedOffset(-1L)
                    .setErrorCode(if (version >= 10) Errors.UNKNOWN_TOPIC_ID.code else Errors.NONE.code)
                ).asJava)
            ).asJava),
        ).toSet,
        fetchOffsets(
          groups = List(
            new OffsetFetchRequestData.OffsetFetchRequestGroup()
              .setGroupId("grp-0")
              .setTopics(List(
                new OffsetFetchRequestData.OffsetFetchRequestTopics()
                  .setName("foo")
                  .setTopicId(topicId)
                  .setPartitionIndexes(List[Integer](0, 1, 5).asJava) // 5 does not exist.
              ).asJava),
            new OffsetFetchRequestData.OffsetFetchRequestGroup()
              .setGroupId("grp-1")
              .setTopics(null),
            new OffsetFetchRequestData.OffsetFetchRequestGroup()
              .setGroupId("grp-2")
              .setTopics(List.empty.asJava),
            new OffsetFetchRequestData.OffsetFetchRequestGroup()
              .setGroupId("grp-3")
              .setTopics(List(
                new OffsetFetchRequestData.OffsetFetchRequestTopics()
                  .setName("foo")
                  .setTopicId(topicId)
                  .setPartitionIndexes(List[Integer](0).asJava)
              ).asJava),
            new OffsetFetchRequestData.OffsetFetchRequestGroup()
              .setGroupId("grp-4")
              .setTopics(List(
                new OffsetFetchRequestData.OffsetFetchRequestTopics()
                  .setName("foo-unknown") // Unknown topic
                  .setTopicId(unknownTopicId)
                  .setPartitionIndexes(List[Integer](0).asJava),
                new OffsetFetchRequestData.OffsetFetchRequestTopics()
                  .setName("foo")
                  .setTopicId(topicId)
                  .setPartitionIndexes(List[Integer](5).asJava) // 5 does not exist.
              ).asJava),
          ),
          requireStable = requireStable,
          version = version.toShort
        ).toSet
      )
    }
  }

  @ClusterTest
  def testFetchOffsetWithRecreatedTopic(): Unit = {
    // There are two ways to ensure that committed of recreated topics are not returned.
    // 1) When a topic is deleted, GroupCoordinatorService#onPartitionsDeleted is called to
    //    delete all its committed offsets.
    // 2) Since version 10 of the OffsetCommit API, the topic id is stored alongside the
    //    committed offset. When it is queried, it is only returned iff the topic id of
    //    committed offset matches the requested one.
    // The test tests both conditions but not in a deterministic way as they race
    // against each others.

    createOffsetsTopic()

    // Create the topic.
    var topicId = createTopic(
      topic = "foo",
      numPartitions = 3
    )

    // Join the consumer group. Note that we don't heartbeat here so we must use
    // a session long enough for the duration of the test.
    val (memberId, memberEpoch) = joinConsumerGroup("grp", true)

    // Commit offsets.
    for (partitionId <- 0 to 2) {
      commitOffset(
        groupId = "grp",
        memberId = memberId,
        memberEpoch = memberEpoch,
        topic = "foo",
        topicId = topicId,
        partition = partitionId,
        offset = 100L + partitionId,
        expectedError = Errors.NONE,
        version = ApiKeys.OFFSET_COMMIT.latestVersion(isUnstableApiEnabled)
      )
    }

    // Delete topic.
    deleteTopic("foo")

    // Recreate topic.
    topicId = createTopic(
      topic = "foo",
      numPartitions = 3
    )

    // Start from version 10 because fetching topic id is not supported before.
    for (version <- 10 to ApiKeys.OFFSET_FETCH.latestVersion(isUnstableApiEnabled)) {
      assertEquals(
        new OffsetFetchResponseData.OffsetFetchResponseGroup()
          .setGroupId("grp")
          .setTopics(List(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
              .setTopicId(topicId)
              .setPartitions(List(
                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                  .setPartitionIndex(0)
                  .setCommittedOffset(-1L),
                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                  .setPartitionIndex(1)
                  .setCommittedOffset(-1L),
                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                  .setPartitionIndex(2)
                  .setCommittedOffset(-1L)
              ).asJava)
          ).asJava),
        fetchOffsets(
          group = new OffsetFetchRequestData.OffsetFetchRequestGroup()
            .setGroupId("grp")
            .setMemberId(memberId)
            .setMemberEpoch(memberEpoch)
            .setTopics(List(
              new OffsetFetchRequestData.OffsetFetchRequestTopics()
                .setTopicId(topicId)
                .setPartitionIndexes(List[Integer](0, 1, 2).asJava)
            ).asJava),
          requireStable = true,
          version = version.toShort
        )
      )
    }
  }

  @ClusterTest
  def testCommittedOffsetsDeletedWhenTopicDeleted(): Unit = {
    // This test verifies that committed offsets are deleted when a topic is deleted.
    // When a topic is deleted, GroupCoordinatorService.onMetadataUpdate is called which
    // schedules the deletion of all committed offsets for the deleted topic partitions.

    createOffsetsTopic()

    // Create the topic.
    val topicId = createTopic(
      topic = "foo",
      numPartitions = 3
    )

    // Join the consumer group. Note that we don't heartbeat here so we must use
    // a session long enough for the duration of the test.
    val (memberId, memberEpoch) = joinConsumerGroup("grp", useNewProtocol = true)

    // Commit offsets.
    for (partitionId <- 0 to 2) {
      commitOffset(
        groupId = "grp",
        memberId = memberId,
        memberEpoch = memberEpoch,
        topic = "foo",
        topicId = topicId,
        partition = partitionId,
        offset = 100L + partitionId,
        expectedError = Errors.NONE,
        version = ApiKeys.OFFSET_COMMIT.latestVersion(isUnstableApiEnabled)
      )
    }

    // Verify that the offsets are committed.
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
        group = new OffsetFetchRequestData.OffsetFetchRequestGroup()
          .setGroupId("grp")
          .setMemberId(memberId)
          .setMemberEpoch(memberEpoch)
          .setTopics(null),
        requireStable = false,
        version = 9
      )
    )

    // Delete the topic.
    deleteTopic("foo")

    // Wait until the offsets are deleted. The offsets should be deleted
    // when the topic deletion is processed by onMetadataUpdate.
    TestUtils.waitUntilTrue(
      () => {
        fetchOffsets(
          group = new OffsetFetchRequestData.OffsetFetchRequestGroup()
            .setGroupId("grp")
            .setMemberId("")
            .setMemberEpoch(-1)
            .setTopics(null),
          requireStable = false,
          // Force using version 9 (and topic names) because unknown topic ids
          // are filtered out.
          version = 9
        ).topics.isEmpty
      },
      msg = "Offsets should be deleted after topic deletion."
    )
  }

  @ClusterTest
  def testGroupErrors(): Unit = {
    val topicId = createTopic(
      topic = "foo",
      numPartitions = 3
    )

    for (version <- ApiKeys.OFFSET_FETCH.oldestVersion() to ApiKeys.OFFSET_FETCH.latestVersion(isUnstableApiEnabled)) {
      assertEquals(
        if (version >= 2) {
          new OffsetFetchResponseData.OffsetFetchResponseGroup()
            .setGroupId("unknown")
            .setErrorCode(Errors.NOT_COORDINATOR.code)
        } else {
          // Version 1 does not support group level errors. Hence, the error is
          // returned at the partition level.
          new OffsetFetchResponseData.OffsetFetchResponseGroup()
            .setGroupId("unknown")
            .setTopics(List(
              new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setPartitions(List(
                  new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                    .setPartitionIndex(0)
                    .setErrorCode(Errors.NOT_COORDINATOR.code)
                    .setCommittedOffset(-1)
                    .setCommittedLeaderEpoch(-1)
                    .setMetadata("")
                ).asJava)
            ).asJava)
        },
        fetchOffsets(
          group = new OffsetFetchRequestData.OffsetFetchRequestGroup()
            .setGroupId("unknown")
            .setMemberId("")
            .setMemberEpoch(0)
            .setTopics(List(
              new OffsetFetchRequestData.OffsetFetchRequestTopics()
                .setName("foo")
                .setTopicId(topicId)
                .setPartitionIndexes(List[Integer](0).asJava)
            ).asJava),
          requireStable = false,
          version = version.toShort
        )
      )
    }
  }
}
