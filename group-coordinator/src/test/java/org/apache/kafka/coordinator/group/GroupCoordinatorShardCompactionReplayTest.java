/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.coordinator.group;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.group.CompactionReplayTestContext.ConsumerMemberState;
import org.apache.kafka.coordinator.group.CompactionReplayTestContext.StreamsMemberState;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics;
import org.apache.kafka.coordinator.group.streams.MockTaskAssignor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.apache.kafka.coordinator.group.CompactionReplayTestContext.BAR_TOPIC_NAME;
import static org.apache.kafka.coordinator.group.CompactionReplayTestContext.FOO_TOPIC_NAME;

/**
 * Compaction replay tests for the group coordinator. Tests capture written records,
 * compact the resulting log, and replay records through a new group coordinator
 * shard to verify loading.
 *
 * Two compaction cases are tested:
 *  - Prefix compaction (standard): A prefix of the log is compacted, so during loading,
 *    the group coordinator reads a compacted section followed by an uncompacted section.
 *  - Concurrent compaction: When compaction occurs concurrent with a load, the group
 *    coordinator can read a compacted section in between uncompacted sections.
 *    More precisely, something like the following can happen -
 *      1. Group coordinator loads segment A (uncompacted)
 *      2. Sections A and B are compacted
 *      3. Group coordinator loads sections B (compacted) then C (active, uncompacted)
 *    This scenario has been seen in production and caused KAFKA-19862.
 *
 * The compaction model in this test class aligns compaction to batch boundaries to match
 * realistic compaction performance. Consider a scenario like the following:
 *   1. ConsumerGroupMemberMetadataKey <- memberEpoch=0
 *   ...
 *   2. ConsumerGroupCurrentMemberAssignmentKey, compacted
 *   --- batch boundary ---
 *   3. ConsumerGroupCurrentMemberAssignmentKey = tombstone, compacted
 *   4. ConsumerGroupTargetAssignmentMemberKey = tombstone
 *   5. ConsumerGroupMemberMetadataKey = tombstone
 *
 * If we were to compact records 2,3 across a batch boundary, then record 5 will fail on load
 * because it will see memberEpoch=0 (from record 1) but expect LEAVE_GROUP_MEMBER_EPOCH. Put
 * another way, the batch boundaries mean that either all or no records for a given group
 * operation are compacted.
 */
public class GroupCoordinatorShardCompactionReplayTest {

    private static final GroupCoordinatorConfig REPLAY_CONFIG = GroupCoordinatorConfig.fromProps(Map.of());
    private static final GroupCoordinatorMetrics REPLAY_METRICS = new GroupCoordinatorMetrics();

    private Uuid fooTopicId;
    private Uuid barTopicId;
    private CoordinatorMetadataImage metadataImage;
    private CompactionReplayTestContext replay;

    @BeforeEach
    public void setUp() {
        fooTopicId = Uuid.randomUuid();
        barTopicId = Uuid.randomUuid();
        metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, FOO_TOPIC_NAME, 6)
            .addTopic(barTopicId, BAR_TOPIC_NAME, 3)
            .addRacks()
            .buildCoordinatorMetadataImage();
        MockPartitionAssignor consumerAssignor = new MockPartitionAssignor("range");
        MockTaskAssignor streamsAssignor = new MockTaskAssignor("sticky");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.UPGRADE.toString())
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(consumerAssignor))
            .withStreamsGroupTaskAssignors(List.of(streamsAssignor))
            .withMetadataImage(metadataImage)
            .build();
        replay = new CompactionReplayTestContext(context, consumerAssignor, streamsAssignor, metadataImage);
    }

    /**
     * Classic -> consumer group upgrade with offset commits. Related bugs: KAFKA-19862
     *
     * Scenario:
     *  Classic group created
     *  Classic offset commit
     *  Classic group rebalance
     *  Member joins with consumer protocol
     *  Upgrades to consumer group
     *  Consumer group rebalance
     */
    @Test
    public void testClassicGroupUpgradeToConsumerGroup() throws Exception {
        String groupId = "consumer-lifecycle-group";

        // A classic group is created when its first member joins and syncs
        JoinGroupResponseData joinResponseA = replay.joinFirstClassicMember(groupId);
        String classicMemberA = joinResponseA.memberId();
        replay.syncClassicMember(groupId, classicMemberA, joinResponseA.generationId(), Map.of(
            classicMemberA, List.of(
                new TopicPartition(FOO_TOPIC_NAME, 0),
                new TopicPartition(FOO_TOPIC_NAME, 1),
                new TopicPartition(FOO_TOPIC_NAME, 2),
                new TopicPartition(FOO_TOPIC_NAME, 3),
                new TopicPartition(FOO_TOPIC_NAME, 4),
                new TopicPartition(FOO_TOPIC_NAME, 5),
                new TopicPartition(BAR_TOPIC_NAME, 0),
                new TopicPartition(BAR_TOPIC_NAME, 1),
                new TopicPartition(BAR_TOPIC_NAME, 2))
        ));

        // Offset commit
        replay.commitOffset(groupId, FOO_TOPIC_NAME, 0, 10L);
        replay.commitOffset(groupId, BAR_TOPIC_NAME, 0, 20L);

        // Member B joins with classic protocol, triggering rebalance
        String classicMemberB = replay.joinClassicMember(groupId);

        // Member A rejoins
        JoinGroupResponseData rejoinResponseA = replay.rejoinClassicMember(groupId, classicMemberA);
        replay.syncClassicMember(groupId, classicMemberA, rejoinResponseA.generationId(), Map.of(
            classicMemberA, List.of(
                new TopicPartition(FOO_TOPIC_NAME, 0),
                new TopicPartition(FOO_TOPIC_NAME, 1),
                new TopicPartition(FOO_TOPIC_NAME, 2),
                new TopicPartition(BAR_TOPIC_NAME, 0)),
            classicMemberB, List.of(
                new TopicPartition(FOO_TOPIC_NAME, 3),
                new TopicPartition(FOO_TOPIC_NAME, 4),
                new TopicPartition(FOO_TOPIC_NAME, 5),
                new TopicPartition(BAR_TOPIC_NAME, 1),
                new TopicPartition(BAR_TOPIC_NAME, 2))
        ));
        replay.syncClassicMember(groupId, classicMemberB, rejoinResponseA.generationId(), Map.of());

        // Member C joins with consumer protocol, triggering an online classic -> consumer group upgrade.
        String memberC = Uuid.randomUuid().toString();
        replay.prepareConsumerAssignment(Map.of(
            classicMemberA, mkAssignment(mkTopicAssignment(fooTopicId, 0, 1), mkTopicAssignment(barTopicId, 0)),
            classicMemberB, mkAssignment(mkTopicAssignment(fooTopicId, 2, 3), mkTopicAssignment(barTopicId, 1)),
            memberC, mkAssignment(mkTopicAssignment(fooTopicId, 4, 5), mkTopicAssignment(barTopicId, 2))));
        Map<String, ConsumerMemberState> members = new LinkedHashMap<>();
        replay.joinConsumerMember(groupId, memberC, members);

        // Members A and B move onto the consumer protocol one at a time.
        String memberA = Uuid.randomUuid().toString();
        replay.prepareConsumerAssignment(Map.of(
            classicMemberB, mkAssignment(mkTopicAssignment(fooTopicId, 0, 1, 2, 3), mkTopicAssignment(barTopicId, 0, 1)),
            memberC, mkAssignment(mkTopicAssignment(fooTopicId, 4, 5), mkTopicAssignment(barTopicId, 2))));
        replay.leaveClassicMember(groupId, classicMemberA);
        replay.prepareConsumerAssignment(Map.of(
            classicMemberB, mkAssignment(mkTopicAssignment(fooTopicId, 2, 3), mkTopicAssignment(barTopicId, 1)),
            memberC, mkAssignment(mkTopicAssignment(fooTopicId, 4, 5), mkTopicAssignment(barTopicId, 2)),
            memberA, mkAssignment(mkTopicAssignment(fooTopicId, 0, 1), mkTopicAssignment(barTopicId, 0))));
        replay.waitForAssignmentInterval();
        replay.joinConsumerMember(groupId, memberA, members);
        replay.completeConsumerGroupRebalance(groupId, members);

        String memberB = Uuid.randomUuid().toString();
        replay.prepareConsumerAssignment(Map.of(
            memberA, mkAssignment(mkTopicAssignment(fooTopicId, 0, 1, 2, 3), mkTopicAssignment(barTopicId, 0, 1)),
            memberC, mkAssignment(mkTopicAssignment(fooTopicId, 4, 5), mkTopicAssignment(barTopicId, 2))));
        replay.leaveClassicMember(groupId, classicMemberB);
        replay.completeConsumerGroupRebalance(groupId, members);
        replay.prepareConsumerAssignment(Map.of(
            memberA, mkAssignment(mkTopicAssignment(fooTopicId, 0, 1), mkTopicAssignment(barTopicId, 0)),
            memberB, mkAssignment(mkTopicAssignment(fooTopicId, 2, 3), mkTopicAssignment(barTopicId, 1)),
            memberC, mkAssignment(mkTopicAssignment(fooTopicId, 4, 5), mkTopicAssignment(barTopicId, 2))));
        replay.waitForAssignmentInterval();
        replay.joinConsumerMember(groupId, memberB, members);
        replay.completeConsumerGroupRebalance(groupId, members);

        // Member D joins with consumer protocol, triggering a consumer group rebalance.
        String memberD = Uuid.randomUuid().toString();
        replay.prepareConsumerAssignment(Map.of(
            memberA, mkAssignment(mkTopicAssignment(fooTopicId, 4, 5)),
            memberB, mkAssignment(mkTopicAssignment(fooTopicId, 0), mkTopicAssignment(barTopicId, 0)),
            memberC, mkAssignment(mkTopicAssignment(fooTopicId, 1), mkTopicAssignment(barTopicId, 1)),
            memberD, mkAssignment(mkTopicAssignment(fooTopicId, 2, 3), mkTopicAssignment(barTopicId, 2))));
        replay.waitForAssignmentInterval();
        replay.joinConsumerMember(groupId, memberD, members);
        replay.completeConsumerGroupRebalance(groupId, members);

        // Group commits one more offset
        replay.commitOffset(groupId, FOO_TOPIC_NAME, 1, 30L);

        // Verify the partitions can be reloaded cleanly from log.
        assertCompactedVariantsLoadCleanly();
    }

    /**
     * Classic -> streams upgrade with offset commits. Related bugs: KAFKA-19862, KAFKA-20254
     *
     * Scenario:
     *  Classic group created
     *  Classic offset commit
     *  Classic group rebalance
     *  Group upgrades offline to streams protocol
     *  Members join/leave and group rebalances accordingly
     */
    @Test
    public void testClassicGroupMigratedToStreamsGroup() throws Exception {
        String groupId = "streams-lifecycle-group";

        // A classic group is created when its first member joins and syncs
        JoinGroupResponseData joinResponseA = replay.joinFirstClassicMember(groupId);
        String classicMemberA = joinResponseA.memberId();
        replay.syncClassicMember(groupId, classicMemberA, joinResponseA.generationId(), Map.of(
            classicMemberA, List.of(
                new TopicPartition(FOO_TOPIC_NAME, 0),
                new TopicPartition(FOO_TOPIC_NAME, 1),
                new TopicPartition(FOO_TOPIC_NAME, 2),
                new TopicPartition(FOO_TOPIC_NAME, 3),
                new TopicPartition(FOO_TOPIC_NAME, 4),
                new TopicPartition(FOO_TOPIC_NAME, 5))
        ));

        // Offset commit
        replay.commitOffset(groupId, FOO_TOPIC_NAME, 0, 10L);
        replay.commitOffset(groupId, FOO_TOPIC_NAME, 1, 20L);

        // Member B joins with classic protocol, triggering rebalance
        String classicMemberB = replay.joinClassicMember(groupId);

        // Member A rejoins
        JoinGroupResponseData rejoinResponseA = replay.rejoinClassicMember(groupId, classicMemberA);
        replay.syncClassicMember(groupId, classicMemberA, rejoinResponseA.generationId(), Map.of(
            classicMemberA, List.of(
                new TopicPartition(FOO_TOPIC_NAME, 0),
                new TopicPartition(FOO_TOPIC_NAME, 1),
                new TopicPartition(FOO_TOPIC_NAME, 2)),
            classicMemberB, List.of(
                new TopicPartition(FOO_TOPIC_NAME, 3),
                new TopicPartition(FOO_TOPIC_NAME, 4),
                new TopicPartition(FOO_TOPIC_NAME, 5))
        ));
        replay.syncClassicMember(groupId, classicMemberB, rejoinResponseA.generationId(), Map.of());

        // Group is shut down for offline upgrade to streams
        replay.leaveClassicMember(groupId, classicMemberA);
        replay.leaveClassicMember(groupId, classicMemberB);

        // Group restarts with streams protocol. The leftover classic group is tombstoned.
        String streamsMemberA = Uuid.randomUuid().toString();
        replay.prepareStreamsAssignment(Map.of(streamsMemberA, replay.tasks(0, 1, 2, 3, 4, 5)));
        Map<String, StreamsMemberState> members = new LinkedHashMap<>();
        replay.joinStreamsMember(groupId, streamsMemberA, "process-a", members);
        replay.completeStreamsGroupRebalance(groupId, members);

        // Member B joins and group rebalances.
        String streamsMemberB = Uuid.randomUuid().toString();
        replay.prepareStreamsAssignment(Map.of(
            streamsMemberA, replay.tasks(0, 1, 2),
            streamsMemberB, replay.tasks(3, 4, 5)));
        replay.waitForAssignmentInterval();
        replay.joinStreamsMember(groupId, streamsMemberB, "process-b", members);
        replay.completeStreamsGroupRebalance(groupId, members);

        // Member C joins and the group rebalances.
        String streamsMemberC = Uuid.randomUuid().toString();
        replay.prepareStreamsAssignment(Map.of(
            streamsMemberA, replay.tasks(0, 1),
            streamsMemberB, replay.tasks(2, 3),
            streamsMemberC, replay.tasks(4, 5)));
        replay.waitForAssignmentInterval();
        replay.joinStreamsMember(groupId, streamsMemberC, "process-c", members);
        replay.completeStreamsGroupRebalance(groupId, members);

        // Member A leaves and the group rebalances.
        replay.prepareStreamsAssignment(Map.of(
            streamsMemberB, replay.tasks(0, 1, 2),
            streamsMemberC, replay.tasks(3, 4, 5)));
        replay.waitForAssignmentInterval();
        replay.leaveStreamsMember(groupId, streamsMemberA, members);
        replay.completeStreamsGroupRebalance(groupId, members);

        // Member B leaves and group rebalances (all tasks now owned by member C).
        replay.prepareStreamsAssignment(Map.of(streamsMemberC, replay.tasks(0, 1, 2, 3, 4, 5)));
        replay.waitForAssignmentInterval();
        replay.leaveStreamsMember(groupId, streamsMemberB, members);
        replay.completeStreamsGroupRebalance(groupId, members);

        replay.commitOffset(groupId, FOO_TOPIC_NAME, 2, 30L);

        // Verify partitions can be reloaded cleanly from log.
        assertCompactedVariantsLoadCleanly();
    }

    /**
     * One compacted variant of a captured log: the records a load observes, the window that was cleaned
     * to produce them, and the positions in the original log the cleaner removed.
     */
    private record CompactedVariant(
        List<CoordinatorRecord> records,
        int from,
        int to,
        List<Integer> removedPositions
    ) {
        @Override
        public String toString() {
            return removedPositions.isEmpty()
                ? "the uncompacted log"
                : "the log with window [" + from + ", " + to + ") cleaned, removing positions " + removedPositions;
        }
    }

    /**
     * Replays every compacted variant (each contiguous subset of record batches) of the captured log
     * through a fresh coordinator and asserts that each one loads without throwing.
     */
    private void assertCompactedVariantsLoadCleanly() {
        List<CoordinatorRecord> log = replay.records();

        Set<ApiMessage> laterKeys = new HashSet<>();
        Set<Integer> compactable = new HashSet<>();
        for (int position = log.size() - 1; position >= 0; position--) {
            CoordinatorRecord record = log.get(position);
            if (record.value() == null || laterKeys.contains(record.key())) {
                compactable.add(position);
            }
            laterKeys.add(record.key());
        }

        List<Integer> boundaries = replay.batchBoundaries();

        assertLoadsCleanly(compact(log, compactable, 0, 0));

        for (int firstBatch = 0; firstBatch < boundaries.size() - 1; firstBatch++) {
            for (int lastBatch = firstBatch + 1; lastBatch < boundaries.size(); lastBatch++) {
                assertLoadsCleanly(compact(log, compactable, boundaries.get(firstBatch), boundaries.get(lastBatch)));
            }
        }
    }

    /**
     * Replays {@code variant} through a real {@link GroupCoordinatorShard} over a fresh coordinator.
     */
    private void assertLoadsCleanly(CompactedVariant variant) {
        GroupMetadataManagerTestContext replayContext =
            new GroupMetadataManagerTestContext.Builder()
                .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.UPGRADE.toString())
                .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(new MockPartitionAssignor("range")))
                .withStreamsGroupTaskAssignors(List.of(new MockTaskAssignor("sticky")))
                .withMetadataImage(metadataImage)
                .build();
        LogContext logContext = new LogContext();
        GroupCoordinatorShard shard = new GroupCoordinatorShard(
            logContext,
            replayContext.groupMetadataManager,
            new OffsetMetadataManager.Builder()
                .withLogContext(logContext)
                .withTime(replayContext.time)
                .withSnapshotRegistry(replayContext.snapshotRegistry)
                .withGroupMetadataManager(replayContext.groupMetadataManager)
                .withGroupCoordinatorConfig(REPLAY_CONFIG)
                .withGroupCoordinatorMetricsShard(replayContext.metrics)
                .build(),
            replayContext.time,
            replayContext.timer,
            REPLAY_CONFIG,
            REPLAY_METRICS,
            replayContext.metrics
        );

        List<CoordinatorRecord> records = variant.records();
        int offset = 0;
        try {
            for (; offset < records.size(); offset++) {
                shard.replay(offset, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, records.get(offset));
            }
        } catch (Throwable t) {
            throw new AssertionError(
                "Replaying " + variant + " failed to load.\n"
                    + "  failed on surviving record " + offset + " " + records.get(offset).key() + "\n"
                    + "  survivingRecords=" + records.stream()
                        .map(record -> record.key().getClass().getSimpleName()
                            + (record.value() == null ? "(tombstone)" : ""))
                        .collect(Collectors.joining(", ")),
                t
            );
        }
    }

    /**
     * Removes every compactable record in {@code log[from..to)}. Models a contiguous
     * stretch of compacted log records.
     */
    private static CompactedVariant compact(
        List<CoordinatorRecord> log,
        Set<Integer> compactable,
        int from,
        int to
    ) {
        Set<ApiMessage> survivingKeys = new HashSet<>();
        List<CoordinatorRecord> compacted = new ArrayList<>(log.size());
        List<Integer> removed = new ArrayList<>();
        for (int position = 0; position < log.size(); position++) {
            CoordinatorRecord record = log.get(position);
            boolean cleaned = position >= from && position < to && compactable.contains(position);
            boolean isRetainedTombstone = record.value() == null && survivingKeys.contains(record.key());
            if (!cleaned || isRetainedTombstone) {
                compacted.add(record);
                survivingKeys.add(record.key());
            } else {
                removed.add(position);
            }
        }
        return new CompactedVariant(compacted, from, to, removed);
    }
}
