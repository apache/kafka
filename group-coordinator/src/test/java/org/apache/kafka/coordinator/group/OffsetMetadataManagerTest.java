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
import org.apache.kafka.common.errors.CoordinatorNotAvailableException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.IllegalGenerationException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.StaleMemberEpochException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetDeleteRequestData;
import org.apache.kafka.common.message.OffsetDeleteResponseData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.annotation.ApiKeyVersionsSource;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorResult;
import org.apache.kafka.coordinator.common.runtime.KRaftCoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.MockCoordinatorExecutor;
import org.apache.kafka.coordinator.common.runtime.MockCoordinatorTimer;
import org.apache.kafka.coordinator.group.classic.ClassicGroup;
import org.apache.kafka.coordinator.group.classic.ClassicGroupMember;
import org.apache.kafka.coordinator.group.classic.ClassicGroupState;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.CoordinatorRecordType;
import org.apache.kafka.coordinator.group.generated.OffsetCommitKey;
import org.apache.kafka.coordinator.group.generated.OffsetCommitValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetricsShard;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroup;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroupMember;
import org.apache.kafka.coordinator.group.streams.StreamsGroup;
import org.apache.kafka.coordinator.group.streams.StreamsGroupMember;
import org.apache.kafka.coordinator.group.streams.StreamsTopology;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.timeline.SnapshotRegistry;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.InetAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Stream;

import static org.apache.kafka.common.requests.OffsetFetchResponse.INVALID_OFFSET;
import static org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics.OFFSET_COMMITS_SENSOR_NAME;
import static org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics.OFFSET_DELETIONS_SENSOR_NAME;
import static org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics.OFFSET_EXPIRED_SENSOR_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class OffsetMetadataManagerTest {
    static class OffsetMetadataManagerTestContext {
        public static class Builder {
            private final MockTime time = new MockTime();
            private final MockCoordinatorTimer<Void, CoordinatorRecord> timer = new MockCoordinatorTimer<>(time);
            private final MockCoordinatorExecutor<CoordinatorRecord> executor = new MockCoordinatorExecutor<>();
            private final LogContext logContext = new LogContext();
            private final SnapshotRegistry snapshotRegistry = new SnapshotRegistry(logContext);
            private final GroupCoordinatorMetricsShard metrics = mock(GroupCoordinatorMetricsShard.class);
            private final GroupConfigManager configManager = mock(GroupConfigManager.class);
            private GroupMetadataManager groupMetadataManager = null;
            private MetadataImage metadataImage = null;
            private GroupCoordinatorConfig config = null;

            Builder withOffsetMetadataMaxSize(int offsetMetadataMaxSize) {
                config = GroupCoordinatorConfigTest.createGroupCoordinatorConfig(offsetMetadataMaxSize, 60000L, 24 * 60);
                return this;
            }

            Builder withOffsetsRetentionMinutes(int offsetsRetentionMinutes) {
                config = GroupCoordinatorConfigTest.createGroupCoordinatorConfig(4096, 60000L, offsetsRetentionMinutes);
                return this;
            }

            Builder withGroupMetadataManager(GroupMetadataManager groupMetadataManager) {
                this.groupMetadataManager = groupMetadataManager;
                return this;
            }

            OffsetMetadataManagerTestContext build() {
                if (metadataImage == null) metadataImage = MetadataImage.EMPTY;
                if (config == null) {
                    config = GroupCoordinatorConfigTest.createGroupCoordinatorConfig(4096, 60000L, 24);
                }

                if (groupMetadataManager == null) {
                    groupMetadataManager = new GroupMetadataManager.Builder()
                        .withTime(time)
                        .withTimer(timer)
                        .withExecutor(executor)
                        .withSnapshotRegistry(snapshotRegistry)
                        .withLogContext(logContext)
                        .withMetadataImage(new KRaftCoordinatorMetadataImage(metadataImage))
                        .withGroupCoordinatorMetricsShard(metrics)
                        .withGroupConfigManager(configManager)
                        .withConfig(GroupCoordinatorConfig.fromProps(Map.of()))
                        .build();
                }

                OffsetMetadataManager offsetMetadataManager = new OffsetMetadataManager.Builder()
                    .withTime(time)
                    .withLogContext(logContext)
                    .withSnapshotRegistry(snapshotRegistry)
                    .withMetadataImage(metadataImage)
                    .withGroupMetadataManager(groupMetadataManager)
                    .withGroupCoordinatorConfig(config)
                    .withGroupCoordinatorMetricsShard(metrics)
                    .build();

                return new OffsetMetadataManagerTestContext(
                    time,
                    timer,
                    snapshotRegistry,
                    metrics,
                    groupMetadataManager,
                    offsetMetadataManager
                );
            }
        }

        final MockTime time;
        final MockCoordinatorTimer<Void, CoordinatorRecord> timer;
        final SnapshotRegistry snapshotRegistry;
        final GroupCoordinatorMetricsShard metrics;
        final GroupMetadataManager groupMetadataManager;
        final OffsetMetadataManager offsetMetadataManager;

        long lastCommittedOffset = 0L;
        long lastWrittenOffset = 0L;

        OffsetMetadataManagerTestContext(
            MockTime time,
            MockCoordinatorTimer<Void, CoordinatorRecord> timer,
            SnapshotRegistry snapshotRegistry,
            GroupCoordinatorMetricsShard metrics,
            GroupMetadataManager groupMetadataManager,
            OffsetMetadataManager offsetMetadataManager
        ) {
            this.time = time;
            this.timer = timer;
            this.snapshotRegistry = snapshotRegistry;
            this.metrics = metrics;
            this.groupMetadataManager = groupMetadataManager;
            this.offsetMetadataManager = offsetMetadataManager;
        }

        public Group getOrMaybeCreateGroup(
            Group.GroupType groupType,
            String groupId
        ) {
            return switch (groupType) {
                case CLASSIC -> groupMetadataManager.getOrMaybeCreateClassicGroup(groupId, true);
                case CONSUMER -> groupMetadataManager.getOrMaybeCreatePersistedConsumerGroup(groupId, true);
                case STREAMS -> groupMetadataManager.getOrMaybeCreatePersistedStreamsGroup(groupId, true);
                default -> throw new IllegalArgumentException("Invalid group type: " + groupType);
            };
        }

        public void commit() {
            long lastCommittedOffset = this.lastCommittedOffset;
            this.lastCommittedOffset = lastWrittenOffset;
            snapshotRegistry.deleteSnapshotsUpTo(lastCommittedOffset);
        }

        public CoordinatorResult<OffsetCommitResponseData, CoordinatorRecord> commitOffset(
            OffsetCommitRequestData request
        ) {
            return commitOffset(ApiKeys.OFFSET_COMMIT.latestVersion(), request);
        }

        public CoordinatorResult<OffsetCommitResponseData, CoordinatorRecord> commitOffset(
            short version,
            OffsetCommitRequestData request
        ) {
            RequestContext context = new RequestContext(
                new RequestHeader(
                    ApiKeys.OFFSET_COMMIT,
                    version,
                    "client",
                    0
                ),
                "1",
                InetAddress.getLoopbackAddress(),
                KafkaPrincipal.ANONYMOUS,
                ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
                SecurityProtocol.PLAINTEXT,
                ClientInformation.EMPTY,
                false
            );

            CoordinatorResult<OffsetCommitResponseData, CoordinatorRecord> result = offsetMetadataManager.commitOffset(
                context,
                request
            );

            result.records().forEach(this::replay);
            return result;
        }

        public CoordinatorResult<TxnOffsetCommitResponseData, CoordinatorRecord> commitTransactionalOffset(
            TxnOffsetCommitRequestData request
        ) {
            RequestContext context = new RequestContext(
                new RequestHeader(
                    ApiKeys.TXN_OFFSET_COMMIT,
                    ApiKeys.TXN_OFFSET_COMMIT.latestVersion(),
                    "client",
                    0
                ),
                "1",
                InetAddress.getLoopbackAddress(),
                KafkaPrincipal.ANONYMOUS,
                ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
                SecurityProtocol.PLAINTEXT,
                ClientInformation.EMPTY,
                false
            );

            CoordinatorResult<TxnOffsetCommitResponseData, CoordinatorRecord> result = offsetMetadataManager.commitTransactionalOffset(
                context,
                request
            );

            result.records().forEach(record -> replay(
                request.producerId(),
                record
            ));

            return result;
        }

        public List<CoordinatorRecord> deletePartitions(
            List<TopicPartition> topicPartitions
        ) {
            List<CoordinatorRecord> records = offsetMetadataManager.onPartitionsDeleted(topicPartitions);
            records.forEach(this::replay);
            return records;
        }

        public CoordinatorResult<OffsetDeleteResponseData, CoordinatorRecord> deleteOffsets(
            OffsetDeleteRequestData request
        ) {
            CoordinatorResult<OffsetDeleteResponseData, CoordinatorRecord> result = offsetMetadataManager.deleteOffsets(request);
            result.records().forEach(this::replay);
            return result;
        }

        public int deleteAllOffsets(
            String groupId,
            List<CoordinatorRecord> records
        ) {
            List<CoordinatorRecord> addedRecords = new ArrayList<>();
            int numDeletedOffsets = offsetMetadataManager.deleteAllOffsets(groupId, addedRecords);
            addedRecords.forEach(this::replay);

            records.addAll(addedRecords);
            return numDeletedOffsets;
        }

        public boolean cleanupExpiredOffsets(String groupId, List<CoordinatorRecord> records) {
            List<CoordinatorRecord> addedRecords = new ArrayList<>();
            boolean isOffsetsEmptyForGroup = offsetMetadataManager.cleanupExpiredOffsets(groupId, addedRecords);
            addedRecords.forEach(this::replay);

            records.addAll(addedRecords);
            return isOffsetsEmptyForGroup;
        }

        public List<OffsetFetchResponseData.OffsetFetchResponseTopics> fetchOffsets(
            String groupId,
            List<OffsetFetchRequestData.OffsetFetchRequestTopics> topics,
            long committedOffset
        ) {
            return fetchOffsets(
                groupId,
                null,
                -1,
                topics,
                committedOffset
            );
        }

        public List<OffsetFetchResponseData.OffsetFetchResponseTopics> fetchOffsets(
            String groupId,
            String memberId,
            int memberEpoch,
            List<OffsetFetchRequestData.OffsetFetchRequestTopics> topics,
            long committedOffset
        ) {
            OffsetFetchResponseData.OffsetFetchResponseGroup response = offsetMetadataManager.fetchOffsets(
                new OffsetFetchRequestData.OffsetFetchRequestGroup()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setMemberEpoch(memberEpoch)
                    .setTopics(topics),
                committedOffset
            );
            assertEquals(groupId, response.groupId());
            return response.topics();
        }

        public List<OffsetFetchResponseData.OffsetFetchResponseTopics> fetchAllOffsets(
            String groupId,
            long committedOffset
        ) {
            return fetchAllOffsets(
                groupId,
                null,
                -1,
                committedOffset
            );
        }

        public List<OffsetFetchResponseData.OffsetFetchResponseTopics> fetchAllOffsets(
            String groupId,
            String memberId,
            int memberEpoch,
            long committedOffset
        ) {
            OffsetFetchResponseData.OffsetFetchResponseGroup response = offsetMetadataManager.fetchAllOffsets(
                new OffsetFetchRequestData.OffsetFetchRequestGroup()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setMemberEpoch(memberEpoch),
                committedOffset
            );
            assertEquals(groupId, response.groupId());
            return response.topics();
        }

        public List<MockCoordinatorTimer.ExpiredTimeout<Void, CoordinatorRecord>> sleep(long ms) {
            time.sleep(ms);
            List<MockCoordinatorTimer.ExpiredTimeout<Void, CoordinatorRecord>> timeouts = timer.poll();
            timeouts.forEach(timeout -> {
                if (timeout.result().replayRecords()) {
                    timeout.result().records().forEach(this::replay);
                }
            });
            return timeouts;
        }

        public void commitOffset(
            String groupId,
            String topic,
            int partition,
            long offset,
            int leaderEpoch
        ) {
            commitOffset(
                groupId,
                topic,
                partition,
                offset,
                leaderEpoch,
                time.milliseconds()
            );
        }

        public void commitOffset(
            String groupId,
            String topic,
            int partition,
            long offset,
            int leaderEpoch,
            long commitTimestamp
        ) {
            commitOffset(
                RecordBatch.NO_PRODUCER_ID,
                groupId,
                topic,
                partition,
                offset,
                leaderEpoch,
                commitTimestamp
            );
        }

        public void commitOffset(
            long producerId,
            String groupId,
            String topic,
            int partition,
            long offset,
            int leaderEpoch,
            long commitTimestamp
        ) {
            commitOffset(
                producerId,
                groupId,
                Uuid.ZERO_UUID,
                topic,
                partition,
                offset,
                leaderEpoch,
                commitTimestamp
            );
        }

        public void commitOffset(
            long producerId,
            String groupId,
            Uuid topicId,
            String topic,
            int partition,
            long offset,
            int leaderEpoch,
            long commitTimestamp
        ) {
            replay(producerId, GroupCoordinatorRecordHelpers.newOffsetCommitRecord(
                groupId,
                topic,
                partition,
                new OffsetAndMetadata(
                    offset,
                    OptionalInt.of(leaderEpoch),
                    "metadata",
                    commitTimestamp,
                    OptionalLong.empty(),
                    topicId
                )
            ));
        }

        public void deleteOffset(
            String groupId,
            String topic,
            int partition
        ) {
            replay(GroupCoordinatorRecordHelpers.newOffsetCommitTombstoneRecord(
                groupId,
                topic,
                partition
            ));
        }

        private ApiMessage messageOrNull(ApiMessageAndVersion apiMessageAndVersion) {
            if (apiMessageAndVersion == null) {
                return null;
            } else {
                return apiMessageAndVersion.message();
            }
        }

        private void replay(
            CoordinatorRecord record
        ) {
            replay(
                RecordBatch.NO_PRODUCER_ID,
                record
            );
        }

        private void replay(
            long producerId,
            CoordinatorRecord record
        ) {
            snapshotRegistry.idempotentCreateSnapshot(lastWrittenOffset);

            ApiMessage key = record.key();
            ApiMessageAndVersion value = record.value();

            if (key == null) {
                throw new IllegalStateException("Received a null key in " + record);
            }

            switch (CoordinatorRecordType.fromId(record.key().apiKey())) {
                case OFFSET_COMMIT:
                    offsetMetadataManager.replay(
                        lastWrittenOffset,
                        producerId,
                        (OffsetCommitKey) key,
                        (OffsetCommitValue) messageOrNull(value)
                    );
                    break;

                default:
                    throw new IllegalStateException("Received an unknown record type " + record.key().apiKey()
                        + " in " + record);
            }

            lastWrittenOffset++;
        }

        private void replayEndTransactionMarker(
            long producerId,
            TransactionResult result
        ) {
            snapshotRegistry.idempotentCreateSnapshot(lastWrittenOffset);
            offsetMetadataManager.replayEndTransactionMarker(producerId, result);
            lastWrittenOffset++;
        }

        public void testOffsetDeleteWith(
            String groupId,
            String topic,
            int partition,
            Errors expectedError
        ) {
            final OffsetDeleteRequestData.OffsetDeleteRequestTopicCollection requestTopicCollection =
                new OffsetDeleteRequestData.OffsetDeleteRequestTopicCollection(List.of(
                    new OffsetDeleteRequestData.OffsetDeleteRequestTopic()
                        .setName(topic)
                        .setPartitions(List.of(
                            new OffsetDeleteRequestData.OffsetDeleteRequestPartition().setPartitionIndex(partition)
                        ))
                ).iterator());

            final OffsetDeleteResponseData.OffsetDeleteResponsePartitionCollection expectedResponsePartitionCollection =
                new OffsetDeleteResponseData.OffsetDeleteResponsePartitionCollection();
            expectedResponsePartitionCollection.add(
                new OffsetDeleteResponseData.OffsetDeleteResponsePartition()
                    .setPartitionIndex(partition)
                    .setErrorCode(expectedError.code())
            );

            final OffsetDeleteResponseData.OffsetDeleteResponseTopicCollection expectedResponseTopicCollection =
                new OffsetDeleteResponseData.OffsetDeleteResponseTopicCollection(List.of(
                    new OffsetDeleteResponseData.OffsetDeleteResponseTopic()
                        .setName(topic)
                        .setPartitions(expectedResponsePartitionCollection)
                ).iterator());

            List<CoordinatorRecord> expectedRecords = List.of();
            if (hasOffset(groupId, topic, partition) && expectedError == Errors.NONE) {
                expectedRecords = List.of(
                    GroupCoordinatorRecordHelpers.newOffsetCommitTombstoneRecord(groupId, topic, partition)
                );
            }

            final CoordinatorResult<OffsetDeleteResponseData, CoordinatorRecord> coordinatorResult = deleteOffsets(
                new OffsetDeleteRequestData()
                    .setGroupId(groupId)
                    .setTopics(requestTopicCollection)
            );

            assertEquals(new OffsetDeleteResponseData().setTopics(expectedResponseTopicCollection), coordinatorResult.response());
            assertEquals(expectedRecords, coordinatorResult.records());
        }

        public boolean hasOffset(
            String groupId,
            String topic,
            int partition
        ) {
            return offsetMetadataManager.hasCommittedOffset(groupId, topic, partition) ||
                offsetMetadataManager.hasPendingTransactionalOffsets(groupId, topic, partition);
        }
    }

    private static Stream<Uuid> uuids() {
        return Stream.of(
            Uuid.ZERO_UUID,
            Uuid.randomUuid()
        );
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_COMMIT)
    public void testOffsetCommitWithUnknownGroup(short version) {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        Class<? extends Throwable> expectedType;
        if (version >= 9) {
            expectedType = GroupIdNotFoundException.class;
        } else {
            expectedType = IllegalGenerationException.class;
        }

        // Verify that the request is rejected with the correct exception.
        assertThrows(expectedType, () -> context.commitOffset(
            version,
            new OffsetCommitRequestData()
                .setGroupId("foo")
                .setMemberId("member")
                .setGenerationIdOrMemberEpoch(10)
                .setTopics(List.of(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(List.of(
                            new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                        ))
                ))
            )
        );
    }

    @Test
    public void testGenericGroupOffsetCommitWithDeadGroup() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create a dead group.
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup(
            "foo",
            true
        );
        group.transitionTo(ClassicGroupState.DEAD);

        // Verify that the request is rejected with the correct exception.
        assertThrows(CoordinatorNotAvailableException.class, () -> context.commitOffset(
            new OffsetCommitRequestData()
                .setGroupId("foo")
                .setMemberId("member")
                .setGenerationIdOrMemberEpoch(10)
                .setTopics(List.of(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(List.of(
                            new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                        ))
                ))
            )
        );
    }

    @Test
    public void testGenericGroupOffsetCommitWithUnknownMemberId() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create an empty group.
        context.groupMetadataManager.getOrMaybeCreateClassicGroup(
            "foo",
            true
        );

        // Verify that the request is rejected with the correct exception.
        assertThrows(UnknownMemberIdException.class, () -> context.commitOffset(
            new OffsetCommitRequestData()
                .setGroupId("foo")
                .setMemberId("member")
                .setGenerationIdOrMemberEpoch(10)
                .setTopics(List.of(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(List.of(
                            new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                        ))
                ))
            )
        );
    }

    @Test
    public void testGenericGroupOffsetCommitWithIllegalGeneration() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create an empty group.
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup(
            "foo",
            true
        );

        // Add member.
        group.add(mkGenericMember("member", Optional.of("new-instance-id")));

        // Transition to next generation.
        group.transitionTo(ClassicGroupState.PREPARING_REBALANCE);
        group.initNextGeneration();
        assertEquals(1, group.generationId());

        // Verify that the request is rejected with the correct exception.
        assertThrows(IllegalGenerationException.class, () -> context.commitOffset(
            new OffsetCommitRequestData()
                .setGroupId("foo")
                .setMemberId("member")
                .setGenerationIdOrMemberEpoch(10)
                .setTopics(List.of(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(List.of(
                            new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                        ))
                ))
            )
        );
    }

    @Test
    public void testGenericGroupOffsetCommitWithUnknownInstanceId() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create an empty group.
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup(
            "foo",
            true
        );

        // Add member without static id.
        group.add(mkGenericMember("member", Optional.empty()));

        // Verify that the request is rejected with the correct exception.
        assertThrows(UnknownMemberIdException.class, () -> context.commitOffset(
            new OffsetCommitRequestData()
                .setGroupId("foo")
                .setMemberId("member")
                .setGroupInstanceId("instanceid")
                .setGenerationIdOrMemberEpoch(10)
                .setTopics(List.of(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(List.of(
                            new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                        ))
                ))
            )
        );
    }

    @Test
    public void testGenericGroupOffsetCommitWithFencedInstanceId() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create an empty group.
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup(
            "foo",
            true
        );

        // Add member with static id.
        group.add(mkGenericMember("member", Optional.of("new-instance-id")));

        // Verify that the request is rejected with the correct exception.
        assertThrows(UnknownMemberIdException.class, () -> context.commitOffset(
            new OffsetCommitRequestData()
                .setGroupId("foo")
                .setMemberId("member")
                .setGroupInstanceId("old-instance-id")
                .setGenerationIdOrMemberEpoch(10)
                .setTopics(List.of(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(List.of(
                            new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                        ))
                ))
            )
        );
    }

    @Test
    public void testGenericGroupOffsetCommitWhileInCompletingRebalanceState() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create an empty group.
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup(
            "foo",
            true
        );

        // Add member.
        group.add(mkGenericMember("member", Optional.of("new-instance-id")));

        // Transition to next generation.
        group.transitionTo(ClassicGroupState.PREPARING_REBALANCE);
        group.initNextGeneration();
        assertEquals(1, group.generationId());

        // Verify that the request is rejected with the correct exception.
        assertThrows(RebalanceInProgressException.class, () -> context.commitOffset(
            new OffsetCommitRequestData()
                .setGroupId("foo")
                .setMemberId("member")
                .setGenerationIdOrMemberEpoch(1)
                .setTopics(List.of(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(List.of(
                            new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                        ))
                ))
            )
        );
    }

    @Test
    public void testGenericGroupOffsetCommitWithoutMemberIdAndGeneration() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create an empty group.
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup(
            "foo",
            true
        );

        // Add member.
        group.add(mkGenericMember("member", Optional.of("new-instance-id")));

        // Transition to next generation.
        group.transitionTo(ClassicGroupState.PREPARING_REBALANCE);
        group.initNextGeneration();
        assertEquals(1, group.generationId());

        // Verify that the request is rejected with the correct exception.
        assertThrows(UnknownMemberIdException.class, () -> context.commitOffset(
            new OffsetCommitRequestData()
                .setGroupId("foo")
                .setTopics(List.of(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(List.of(
                            new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                        ))
                ))
            )
        );
    }

    @Test
    public void testGenericGroupOffsetCommitWithRetentionTime() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create an empty group.
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup(
            "foo",
            true
        );

        // Add member.
        group.add(mkGenericMember("member", Optional.of("new-instance-id")));

        // Transition to next generation.
        group.transitionTo(ClassicGroupState.PREPARING_REBALANCE);
        group.initNextGeneration();
        assertEquals(1, group.generationId());
        group.transitionTo(ClassicGroupState.STABLE);

        CoordinatorResult<OffsetCommitResponseData, CoordinatorRecord> result = context.commitOffset(
            new OffsetCommitRequestData()
                .setGroupId("foo")
                .setMemberId("member")
                .setGenerationIdOrMemberEpoch(1)
                .setRetentionTimeMs(1234L)
                .setTopics(List.of(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(List.of(
                            new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                        ))
                ))
        );

        assertEquals(
            new OffsetCommitResponseData()
                .setTopics(List.of(
                    new OffsetCommitResponseData.OffsetCommitResponseTopic()
                        .setName("bar")
                        .setPartitions(List.of(
                            new OffsetCommitResponseData.OffsetCommitResponsePartition()
                                .setPartitionIndex(0)
                                .setErrorCode(Errors.NONE.code())
                        ))
                )),
            result.response()
        );

        assertEquals(
            List.of(GroupCoordinatorRecordHelpers.newOffsetCommitRecord(
                "foo",
                "bar",
                0,
                new OffsetAndMetadata(
                    100L,
                    OptionalInt.empty(),
                    "",
                    context.time.milliseconds(),
                    OptionalLong.of(context.time.milliseconds() + 1234L),
                    Uuid.ZERO_UUID
                )
            )),
            result.records()
        );
    }

    @Test
    public void testGenericGroupOffsetCommitMaintainsSession() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create a group.
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup(
            "foo",
            true
        );

        // Add member.
        ClassicGroupMember member = mkGenericMember("member", Optional.empty());
        group.add(member);

        // Transition to next generation.
        group.transitionTo(ClassicGroupState.PREPARING_REBALANCE);
        group.initNextGeneration();
        assertEquals(1, group.generationId());
        group.transitionTo(ClassicGroupState.STABLE);

        // Schedule session timeout. This would be normally done when
        // the group transitions to stable.
        context.groupMetadataManager.rescheduleClassicGroupMemberHeartbeat(group, member);

        // Advance time by half of the session timeout. No timeouts are
        // expired.
        assertEquals(List.of(), context.sleep(5000 / 2));

        // Commit.
        context.commitOffset(
            new OffsetCommitRequestData()
                .setGroupId("foo")
                .setMemberId("member")
                .setGenerationIdOrMemberEpoch(1)
                .setRetentionTimeMs(1234L)
                .setTopics(List.of(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(List.of(
                            new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                        ))
                ))
        );

        // Advance time by half of the session timeout. No timeouts are
        // expired.
        assertEquals(List.of(), context.sleep(5000 / 2));

        // Advance time by half of the session timeout again. The timeout should
        // expire and the member is removed from the group.
        List<MockCoordinatorTimer.ExpiredTimeout<Void, CoordinatorRecord>> timeouts =
            context.sleep(5000 / 2);
        assertEquals(1, timeouts.size());
        assertFalse(group.hasMember(member.memberId()));
    }

    @ParameterizedTest
    @MethodSource("uuids")
    public void testSimpleGroupOffsetCommit(Uuid topicId) {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        CoordinatorResult<OffsetCommitResponseData, CoordinatorRecord> result = context.commitOffset(
            new OffsetCommitRequestData()
                .setGroupId("foo")
                .setTopics(List.of(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setTopicId(topicId)
                        .setPartitions(List.of(
                            new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                        ))
                ))
        );

        assertEquals(
            new OffsetCommitResponseData()
                .setTopics(List.of(
                    new OffsetCommitResponseData.OffsetCommitResponseTopic()
                        .setName("bar")
                        .setTopicId(topicId)
                        .setPartitions(List.of(
                            new OffsetCommitResponseData.OffsetCommitResponsePartition()
                                .setPartitionIndex(0)
                                .setErrorCode(Errors.NONE.code())
                        ))
                )),
            result.response()
        );

        assertEquals(
            List.of(GroupCoordinatorRecordHelpers.newOffsetCommitRecord(
                "foo",
                "bar",
                0,
                new OffsetAndMetadata(
                    100L,
                    OptionalInt.empty(),
                    "",
                    context.time.milliseconds(),
                    OptionalLong.empty(),
                    topicId
                )
            )),
            result.records()
        );

        // A generic should have been created.
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup(
            "foo",
            false
        );
        assertNotNull(group);
        assertEquals("foo", group.groupId());
    }

    @Test
    public void testSimpleGroupOffsetCommitWithInstanceId() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        CoordinatorResult<OffsetCommitResponseData, CoordinatorRecord> result = context.commitOffset(
            new OffsetCommitRequestData()
                .setGroupId("foo")
                // Instance id should be ignored.
                .setGroupInstanceId("instance-id")
                .setTopics(List.of(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(List.of(
                            new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                        ))
                ))
        );

        assertEquals(
            new OffsetCommitResponseData()
                .setTopics(List.of(
                    new OffsetCommitResponseData.OffsetCommitResponseTopic()
                        .setName("bar")
                        .setPartitions(List.of(
                            new OffsetCommitResponseData.OffsetCommitResponsePartition()
                                .setPartitionIndex(0)
                                .setErrorCode(Errors.NONE.code())
                        ))
                )),
            result.response()
        );

        assertEquals(
            List.of(GroupCoordinatorRecordHelpers.newOffsetCommitRecord(
                "foo",
                "bar",
                0,
                new OffsetAndMetadata(
                    100L,
                    OptionalInt.empty(),
                    "",
                    context.time.milliseconds(),
                    OptionalLong.empty(),
                    Uuid.ZERO_UUID
                )
            )),
            result.records()
        );
    }

    @Test
    public void testConsumerGroupOffsetCommitWithUnknownMemberId() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create an empty group.
        context.groupMetadataManager.getOrMaybeCreatePersistedConsumerGroup(
            "foo",
            true
        );

        verifyOffsetCommitWithUnknownMemberId(context);
    }

    @Test
    public void testStreamsGroupOffsetCommitWithUnknownMemberId() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create an empty group.
        context.groupMetadataManager.getOrMaybeCreatePersistedStreamsGroup(
            "foo",
            true
        );

        verifyOffsetCommitWithUnknownMemberId(context);
    }

    private static void verifyOffsetCommitWithUnknownMemberId(OffsetMetadataManagerTestContext context) {
        // Verify that the request is rejected with the correct exception.
        assertThrows(UnknownMemberIdException.class, () -> context.commitOffset(
            new OffsetCommitRequestData()
                .setGroupId("foo")
                .setMemberId("member")
                .setGenerationIdOrMemberEpoch(10)
                .setTopics(List.of(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(List.of(
                            new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                        ))
                ))
            )
        );
    }

    @Test
    public void testConsumerGroupOffsetCommitWithStaleMemberEpoch() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create an empty group.
        ConsumerGroup group = context.groupMetadataManager.getOrMaybeCreatePersistedConsumerGroup(
            "foo",
            true
        );

        // Add member.
        group.updateMember(new ConsumerGroupMember.Builder("member")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .build()
        );

        verifyOffsetCommitWithStaleMemberEpoch(context);
    }

    @Test
    public void testStreamsGroupOffsetCommitWithStaleMemberEpoch() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create an empty group.
        StreamsGroup group = context.groupMetadataManager.getOrMaybeCreatePersistedStreamsGroup(
            "foo",
            true
        );

        // Add member.
        group.updateMember(StreamsGroupMember.Builder.withDefaults("member")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .build()
        );

        verifyOffsetCommitWithStaleMemberEpoch(context);
    }

    private static void verifyOffsetCommitWithStaleMemberEpoch(OffsetMetadataManagerTestContext context) {
        OffsetCommitRequestData request = new OffsetCommitRequestData()
            .setGroupId("foo")
            .setMemberId("member")
            .setGenerationIdOrMemberEpoch(9)
            .setTopics(List.of(
                new OffsetCommitRequestData.OffsetCommitRequestTopic()
                    .setName("bar")
                    .setPartitions(List.of(
                        new OffsetCommitRequestData.OffsetCommitRequestPartition()
                            .setPartitionIndex(0)
                            .setCommittedOffset(100L)
                    ))
            ));

        // Verify that a smaller epoch is rejected.
        assertThrows(StaleMemberEpochException.class, () -> context.commitOffset(request));

        // Verify that a larger epoch is rejected.
        request.setGenerationIdOrMemberEpoch(11);
        assertThrows(StaleMemberEpochException.class, () -> context.commitOffset(request));
    }

    @Test
    public void testConsumerGroupOffsetCommitWithIllegalGenerationId() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create an empty group.
        ConsumerGroup group = context.groupMetadataManager.getOrMaybeCreatePersistedConsumerGroup(
            "foo",
            true
        );

        // Add member.
        group.updateMember(new ConsumerGroupMember.Builder("member")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setClassicMemberMetadata(new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata())
            .build()
        );

        OffsetCommitRequestData request = new OffsetCommitRequestData()
            .setGroupId("foo")
            .setMemberId("member")
            .setGenerationIdOrMemberEpoch(9)
            .setTopics(List.of(
                new OffsetCommitRequestData.OffsetCommitRequestTopic()
                    .setName("bar")
                    .setPartitions(List.of(
                        new OffsetCommitRequestData.OffsetCommitRequestPartition()
                            .setPartitionIndex(0)
                            .setCommittedOffset(100L)
                    ))
            ));

        // Verify that a smaller epoch is rejected.
        assertThrows(IllegalGenerationException.class, () -> context.commitOffset(request));

        // Verify that a larger epoch is rejected.
        request.setGenerationIdOrMemberEpoch(11);
        assertThrows(IllegalGenerationException.class, () -> context.commitOffset(request));
    }

    @Test
    public void testConsumerGroupOffsetCommitFromAdminClient() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create an empty group.
        context.groupMetadataManager.getOrMaybeCreatePersistedConsumerGroup(
            "foo",
            true
        );

        verifyOffsetCommitFromAdminClient(context);
    }

    @Test
    public void testStreamsGroupOffsetCommitFromAdminClient() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create an empty group.
        context.groupMetadataManager.getOrMaybeCreatePersistedStreamsGroup(
            "foo",
            true
        );

        verifyOffsetCommitFromAdminClient(context);
    }

    private static void verifyOffsetCommitFromAdminClient(OffsetMetadataManagerTestContext context) {
        CoordinatorResult<OffsetCommitResponseData, CoordinatorRecord> result = context.commitOffset(
            new OffsetCommitRequestData()
                .setGroupId("foo")
                .setTopics(List.of(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(List.of(
                            new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                        ))
                ))
        );

        assertEquals(
            new OffsetCommitResponseData()
                .setTopics(List.of(
                    new OffsetCommitResponseData.OffsetCommitResponseTopic()
                        .setName("bar")
                        .setPartitions(List.of(
                            new OffsetCommitResponseData.OffsetCommitResponsePartition()
                                .setPartitionIndex(0)
                                .setErrorCode(Errors.NONE.code())
                        ))
                )),
            result.response()
        );

        assertEquals(
            List.of(GroupCoordinatorRecordHelpers.newOffsetCommitRecord(
                "foo",
                "bar",
                0,
                new OffsetAndMetadata(
                    100L,
                    OptionalInt.empty(),
                    "",
                    context.time.milliseconds(),
                    OptionalLong.empty(),
                    Uuid.ZERO_UUID
                )
            )),
            result.records()
        );
    }

    @ParameterizedTest
    @MethodSource("uuids")
    public void testConsumerGroupOffsetCommit(Uuid topicId) {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create an empty group.
        ConsumerGroup group = context.groupMetadataManager.getOrMaybeCreatePersistedConsumerGroup(
            "foo",
            true
        );

        // Add member.
        group.updateMember(new ConsumerGroupMember.Builder("member")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .build()
        );

        verifyOffsetCommit(topicId, context);
    }

    @ParameterizedTest
    @MethodSource("uuids")
    public void testStreamsGroupOffsetCommit(Uuid topicId) {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create an empty group.
        StreamsGroup group = context.groupMetadataManager.getOrMaybeCreatePersistedStreamsGroup(
            "foo",
            true
        );

        // Add member.
        group.updateMember(StreamsGroupMember.Builder.withDefaults("member")
            .setMemberEpoch(10)
            .build()
        );

        verifyOffsetCommit(topicId, context);
    }

    private static void verifyOffsetCommit(Uuid topicId, OffsetMetadataManagerTestContext context) {
        CoordinatorResult<OffsetCommitResponseData, CoordinatorRecord> result = context.commitOffset(
            new OffsetCommitRequestData()
                .setGroupId("foo")
                .setMemberId("member")
                .setGenerationIdOrMemberEpoch(10)
                .setTopics(List.of(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setTopicId(topicId)
                        .setPartitions(List.of(
                            new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                                .setCommittedLeaderEpoch(10)
                                .setCommittedMetadata("metadata")
                        ))
                ))
        );

        assertEquals(
            new OffsetCommitResponseData()
                .setTopics(List.of(
                    new OffsetCommitResponseData.OffsetCommitResponseTopic()
                        .setName("bar")
                        .setTopicId(topicId)
                        .setPartitions(List.of(
                            new OffsetCommitResponseData.OffsetCommitResponsePartition()
                                .setPartitionIndex(0)
                                .setErrorCode(Errors.NONE.code())
                        ))
                )),
            result.response()
        );

        assertEquals(
            List.of(GroupCoordinatorRecordHelpers.newOffsetCommitRecord(
                "foo",
                "bar",
                0,
                new OffsetAndMetadata(
                    100L,
                    OptionalInt.of(10),
                    "metadata",
                    context.time.milliseconds(),
                    OptionalLong.empty(),
                    topicId
                )
            )),
            result.records()
        );
    }

    @Test
    public void testConsumerGroupOffsetCommitWithOffsetMetadataTooLarge() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder()
            .withOffsetMetadataMaxSize(5)
            .build();

        // Create an empty group.
        ConsumerGroup group = context.groupMetadataManager.getOrMaybeCreatePersistedConsumerGroup(
            "foo",
            true
        );

        // Add member.
        group.updateMember(new ConsumerGroupMember.Builder("member")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .build()
        );

        verifyOffsetCommitWithOffsetMetadataTooLarge(context);
    }

    @Test
    public void testStreamsGroupOffsetCommitWithOffsetMetadataTooLarge() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder()
            .withOffsetMetadataMaxSize(5)
            .build();

        // Create an empty group.
        StreamsGroup group = context.groupMetadataManager.getOrMaybeCreatePersistedStreamsGroup(
            "foo",
            true
        );

        // Add member.
        group.updateMember(StreamsGroupMember.Builder.withDefaults("member")
            .setMemberEpoch(10)
            .build()
        );

        verifyOffsetCommitWithOffsetMetadataTooLarge(context);
    }

    private static void verifyOffsetCommitWithOffsetMetadataTooLarge(OffsetMetadataManagerTestContext context) {
        CoordinatorResult<OffsetCommitResponseData, CoordinatorRecord> result = context.commitOffset(
            new OffsetCommitRequestData()
                .setGroupId("foo")
                .setMemberId("member")
                .setGenerationIdOrMemberEpoch(10)
                .setTopics(List.of(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(Arrays.asList(
                            new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                                .setCommittedLeaderEpoch(10)
                                .setCommittedMetadata("toolarge"),
                            new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                .setPartitionIndex(1)
                                .setCommittedOffset(100L)
                                .setCommittedLeaderEpoch(10)
                                .setCommittedMetadata("small")
                        ))
                ))
        );

        assertEquals(
            new OffsetCommitResponseData()
                .setTopics(List.of(
                    new OffsetCommitResponseData.OffsetCommitResponseTopic()
                        .setName("bar")
                        .setPartitions(Arrays.asList(
                            new OffsetCommitResponseData.OffsetCommitResponsePartition()
                                .setPartitionIndex(0)
                                .setErrorCode(Errors.OFFSET_METADATA_TOO_LARGE.code()),
                            new OffsetCommitResponseData.OffsetCommitResponsePartition()
                                .setPartitionIndex(1)
                                .setErrorCode(Errors.NONE.code())
                        ))
                )),
            result.response()
        );

        assertEquals(
            List.of(GroupCoordinatorRecordHelpers.newOffsetCommitRecord(
                "foo",
                "bar",
                1,
                new OffsetAndMetadata(
                    100L,
                    OptionalInt.of(10),
                    "small",
                    context.time.milliseconds(),
                    OptionalLong.empty(),
                    Uuid.ZERO_UUID
                )
            )),
            result.records()
        );
    }

    @Test
    public void testConsumerGroupTransactionalOffsetCommit() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create an empty group.
        ConsumerGroup group = context.groupMetadataManager.getOrMaybeCreatePersistedConsumerGroup(
            "foo",
            true
        );

        // Add member.
        group.updateMember(new ConsumerGroupMember.Builder("member")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .build()
        );

        verifyTransactionalOffsetCommit(context);
    }

    @Test
    public void testStreamsGroupTransactionalOffsetCommit() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create an empty group.
        StreamsGroup group = context.groupMetadataManager.getOrMaybeCreatePersistedStreamsGroup(
            "foo",
            true
        );

        // Add member.
        group.updateMember(StreamsGroupMember.Builder.withDefaults("member")
            .setMemberEpoch(10)
            .build()
        );

        verifyTransactionalOffsetCommit(context);
    }

    private static void verifyTransactionalOffsetCommit(OffsetMetadataManagerTestContext context) {
        CoordinatorResult<TxnOffsetCommitResponseData, CoordinatorRecord> result = context.commitTransactionalOffset(
            new TxnOffsetCommitRequestData()
                .setGroupId("foo")
                .setMemberId("member")
                .setGenerationId(10)
                .setTopics(List.of(
                    new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(List.of(
                            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                                .setCommittedLeaderEpoch(10)
                                .setCommittedMetadata("metadata")
                        ))
                ))
        );

        assertEquals(
            new TxnOffsetCommitResponseData()
                .setTopics(List.of(
                    new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
                        .setName("bar")
                        .setPartitions(List.of(
                            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
                                .setPartitionIndex(0)
                                .setErrorCode(Errors.NONE.code())
                        ))
                )),
            result.response()
        );

        assertEquals(
            List.of(GroupCoordinatorRecordHelpers.newOffsetCommitRecord(
                "foo",
                "bar",
                0,
                new OffsetAndMetadata(
                    100L,
                    OptionalInt.of(10),
                    "metadata",
                    context.time.milliseconds(),
                    OptionalLong.empty(),
                    Uuid.ZERO_UUID
                )
            )),
            result.records()
        );
    }

    @Test
    public void testTransactionalOffsetCommitWithUnknownGroupId() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        assertThrows(IllegalGenerationException.class, () -> context.commitTransactionalOffset(
            new TxnOffsetCommitRequestData()
                .setGroupId("foo")
                .setMemberId("member")
                .setGenerationId(10)
                .setTopics(List.of(
                    new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(List.of(
                            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                                .setCommittedLeaderEpoch(10)
                                .setCommittedMetadata("metadata")
                        ))
                ))
        ));
    }

    @Test
    public void testConsumerGroupTransactionalOffsetCommitWithUnknownMemberId() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create an empty group.
        context.groupMetadataManager.getOrMaybeCreatePersistedConsumerGroup(
            "foo",
            true
        );

        verifyTransactionalOffsetCommitWithUnknownMemberId(context);
    }

    @Test
    public void testStreamsGroupTransactionalOffsetCommitWithUnknownMemberId() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create an empty group.
        context.groupMetadataManager.getOrMaybeCreatePersistedStreamsGroup(
            "foo",
            true
        );

        verifyTransactionalOffsetCommitWithUnknownMemberId(context);
    }

    private static void verifyTransactionalOffsetCommitWithUnknownMemberId(OffsetMetadataManagerTestContext context) {
        assertThrows(UnknownMemberIdException.class, () -> context.commitTransactionalOffset(
            new TxnOffsetCommitRequestData()
                .setGroupId("foo")
                .setMemberId("member")
                .setGenerationId(10)
                .setTopics(List.of(
                    new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(List.of(
                            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                                .setCommittedLeaderEpoch(10)
                                .setCommittedMetadata("metadata")
                        ))
                ))
        ));
    }

    @Test
    public void testConsumerGroupTransactionalOffsetCommitWithStaleMemberEpoch() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create an empty group.
        ConsumerGroup group = context.groupMetadataManager.getOrMaybeCreatePersistedConsumerGroup(
            "foo",
            true
        );

        // Add member.
        group.updateMember(new ConsumerGroupMember.Builder("member")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .build()
        );

        verifyTransactionalOffsetCommitWithStaleMemberEpoch(context);
    }

    @Test
    public void testStreamsGroupTransactionalOffsetCommitWithStaleMemberEpoch() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create an empty group.
        StreamsGroup group = context.groupMetadataManager.getOrMaybeCreatePersistedStreamsGroup(
            "foo",
            true
        );

        // Add member.
        group.updateMember(StreamsGroupMember.Builder.withDefaults("member")
            .setMemberEpoch(10)
            .build()
        );

        verifyTransactionalOffsetCommitWithStaleMemberEpoch(context);
    }

    private static void verifyTransactionalOffsetCommitWithStaleMemberEpoch(OffsetMetadataManagerTestContext context) {
        assertThrows(IllegalGenerationException.class, () -> context.commitTransactionalOffset(
            new TxnOffsetCommitRequestData()
                .setGroupId("foo")
                .setMemberId("member")
                .setGenerationId(100)
                .setTopics(List.of(
                    new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(List.of(
                            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                                .setCommittedLeaderEpoch(10)
                                .setCommittedMetadata("metadata")
                        ))
                ))
        ));
    }

    @Test
    public void testGenericGroupTransactionalOffsetCommit() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create a group.
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup(
            "foo",
            true
        );

        // Add member.
        ClassicGroupMember member = mkGenericMember("member", Optional.empty());
        group.add(member);

        // Transition to next generation.
        group.transitionTo(ClassicGroupState.PREPARING_REBALANCE);
        group.initNextGeneration();
        assertEquals(1, group.generationId());
        group.transitionTo(ClassicGroupState.STABLE);

        CoordinatorResult<TxnOffsetCommitResponseData, CoordinatorRecord> result = context.commitTransactionalOffset(
            new TxnOffsetCommitRequestData()
                .setGroupId("foo")
                .setMemberId("member")
                .setGenerationId(1)
                .setTopics(List.of(
                    new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(List.of(
                            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                                .setCommittedLeaderEpoch(10)
                                .setCommittedMetadata("metadata")
                        ))
                ))
        );

        assertEquals(
            new TxnOffsetCommitResponseData()
                .setTopics(List.of(
                    new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
                        .setName("bar")
                        .setPartitions(List.of(
                            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
                                .setPartitionIndex(0)
                                .setErrorCode(Errors.NONE.code())
                        ))
                )),
            result.response()
        );

        assertEquals(
            List.of(GroupCoordinatorRecordHelpers.newOffsetCommitRecord(
                "foo",
                "bar",
                0,
                new OffsetAndMetadata(
                    100L,
                    OptionalInt.of(10),
                    "metadata",
                    context.time.milliseconds(),
                    OptionalLong.empty(),
                    Uuid.ZERO_UUID
                )
            )),
            result.records()
        );
    }

    @Test
    public void testGenericGroupTransactionalOffsetCommitWithUnknownGroupId() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        assertThrows(IllegalGenerationException.class, () -> context.commitTransactionalOffset(
            new TxnOffsetCommitRequestData()
                .setGroupId("foo")
                .setMemberId("member")
                .setGenerationId(10)
                .setTopics(List.of(
                    new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(List.of(
                            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                                .setCommittedLeaderEpoch(10)
                                .setCommittedMetadata("metadata")
                        ))
                ))
        ));
    }

    @Test
    public void testGenericGroupTransactionalOffsetCommitWithUnknownMemberId() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create an empty group.
        context.groupMetadataManager.getOrMaybeCreateClassicGroup(
            "foo",
            true
        );

        assertThrows(UnknownMemberIdException.class, () -> context.commitTransactionalOffset(
            new TxnOffsetCommitRequestData()
                .setGroupId("foo")
                .setMemberId("member")
                .setGenerationId(10)
                .setTopics(List.of(
                    new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(List.of(
                            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                                .setCommittedLeaderEpoch(10)
                                .setCommittedMetadata("metadata")
                        ))
                ))
        ));
    }

    @Test
    public void testGenericGroupTransactionalOffsetCommitWithIllegalGenerationId() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create a group.
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup(
            "foo",
            true
        );

        // Add member.
        ClassicGroupMember member = mkGenericMember("member", Optional.empty());
        group.add(member);

        // Transition to next generation.
        group.transitionTo(ClassicGroupState.PREPARING_REBALANCE);
        group.initNextGeneration();
        assertEquals(1, group.generationId());
        group.transitionTo(ClassicGroupState.STABLE);

        assertThrows(IllegalGenerationException.class, () -> context.commitTransactionalOffset(
            new TxnOffsetCommitRequestData()
                .setGroupId("foo")
                .setMemberId("member")
                .setGenerationId(100)
                .setTopics(List.of(
                    new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(List.of(
                            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                                .setCommittedLeaderEpoch(10)
                                .setCommittedMetadata("metadata")
                        ))
                ))
        ));
    }

    @Test
    public void testGenericGroupFetchOffsetsWithDeadGroup() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create a dead group.
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup(
            "group",
            true
        );
        group.transitionTo(ClassicGroupState.DEAD);

        List<OffsetFetchRequestData.OffsetFetchRequestTopics> request = Arrays.asList(
            new OffsetFetchRequestData.OffsetFetchRequestTopics()
                .setName("foo")
                .setPartitionIndexes(Arrays.asList(0, 1)),
            new OffsetFetchRequestData.OffsetFetchRequestTopics()
                .setName("bar")
                .setPartitionIndexes(List.of(0))
        );

        List<OffsetFetchResponseData.OffsetFetchResponseTopics> expectedResponse = Arrays.asList(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setPartitions(Arrays.asList(
                    mkInvalidOffsetPartitionResponse(0),
                    mkInvalidOffsetPartitionResponse(1)
                )),
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("bar")
                .setPartitions(List.of(
                    mkInvalidOffsetPartitionResponse(0)
                ))
        );

        assertEquals(expectedResponse, context.fetchOffsets("group", request, Long.MAX_VALUE));
    }

    @Test
    public void testFetchOffsetsWithUnknownGroup() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        List<OffsetFetchRequestData.OffsetFetchRequestTopics> request = Arrays.asList(
            new OffsetFetchRequestData.OffsetFetchRequestTopics()
                .setName("foo")
                .setPartitionIndexes(Arrays.asList(0, 1)),
            new OffsetFetchRequestData.OffsetFetchRequestTopics()
                .setName("bar")
                .setPartitionIndexes(List.of(0))
        );

        List<OffsetFetchResponseData.OffsetFetchResponseTopics> expectedResponse = Arrays.asList(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setPartitions(Arrays.asList(
                    mkInvalidOffsetPartitionResponse(0),
                    mkInvalidOffsetPartitionResponse(1)
                )),
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("bar")
                .setPartitions(List.of(
                    mkInvalidOffsetPartitionResponse(0)
                ))
        );

        assertEquals(expectedResponse, context.fetchOffsets("group", request, Long.MAX_VALUE));
    }

    @Test
    public void testFetchOffsetsWithTopicIds() {
        Uuid fooId = Uuid.randomUuid();
        Uuid barId = Uuid.randomUuid();
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        context.groupMetadataManager.getOrMaybeCreatePersistedConsumerGroup("group", true);

        context.commitOffset("group", "foo", 0, 100L, 1);
        context.commitOffset("group", "bar", 0, 200L, 1);

        List<OffsetFetchRequestData.OffsetFetchRequestTopics> request = List.of(
            new OffsetFetchRequestData.OffsetFetchRequestTopics()
                .setName("foo")
                .setTopicId(fooId)
                .setPartitionIndexes(List.of(0)),
            new OffsetFetchRequestData.OffsetFetchRequestTopics()
                .setName("bar")
                .setTopicId(barId)
                .setPartitionIndexes(List.of(0))
        );

        assertEquals(List.of(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setTopicId(fooId)
                .setPartitions(List.of(
                    mkOffsetPartitionResponse(0, 100L, 1, "metadata")
                )),
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("bar")
                .setTopicId(barId)
                .setPartitions(List.of(
                    mkOffsetPartitionResponse(0, 200L, 1, "metadata")
                ))
        ), context.fetchOffsets("group", request, Long.MAX_VALUE));
    }

    @Test
    public void testFetchOffsetsWithRecreatedTopic() {
        Uuid fooId1 = Uuid.randomUuid();
        Uuid fooId2 = Uuid.randomUuid();
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        context.groupMetadataManager.getOrMaybeCreatePersistedConsumerGroup("group", true);

        context.commitOffset(
            RecordBatch.NO_PRODUCER_ID,
            "group",
            fooId1,
            "foo",
            0,
            100L,
            1,
            context.time.milliseconds()
        );

        context.commitOffset(
            RecordBatch.NO_PRODUCER_ID,
            "group",
            fooId1,
            "foo",
            1,
            100L,
            1,
            context.time.milliseconds()
        );

        // Request with the correct topic id.
        var request = List.of(
            new OffsetFetchRequestData.OffsetFetchRequestTopics()
                .setName("foo")
                .setTopicId(fooId1)
                .setPartitionIndexes(List.of(0, 1))
        );

        assertEquals(List.of(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setTopicId(fooId1)
                .setPartitions(List.of(
                    mkOffsetPartitionResponse(0, 100L, 1, "metadata"),
                    mkOffsetPartitionResponse(1, 100L, 1, "metadata")
                ))
        ), context.fetchOffsets("group", request, Long.MAX_VALUE));

        // Request with the incorrect topic id.
        request = List.of(
            new OffsetFetchRequestData.OffsetFetchRequestTopics()
                .setName("foo")
                .setTopicId(fooId2)
                .setPartitionIndexes(List.of(0, 1))
        );

        assertEquals(List.of(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setTopicId(fooId2)
                .setPartitions(List.of(
                    mkInvalidOffsetPartitionResponse(0),
                    mkInvalidOffsetPartitionResponse(1)
                ))
        ), context.fetchOffsets("group", request, Long.MAX_VALUE));
    }

    @Test
    public void testFetchOffsetsAtDifferentCommittedOffset() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        context.groupMetadataManager.getOrMaybeCreatePersistedConsumerGroup("group", true);

        assertEquals(0, context.lastWrittenOffset);
        context.commitOffset("group", "foo", 0, 100L, 1);
        assertEquals(1, context.lastWrittenOffset);
        context.commitOffset("group", "foo", 1, 110L, 1);
        assertEquals(2, context.lastWrittenOffset);
        context.commitOffset("group", "bar", 0, 200L, 1);
        assertEquals(3, context.lastWrittenOffset);
        context.commitOffset("group", "foo", 1, 111L, 2);
        assertEquals(4, context.lastWrittenOffset);
        context.commitOffset("group", "bar", 1, 210L, 2);
        assertEquals(5, context.lastWrittenOffset);

        // Always use the same request.
        List<OffsetFetchRequestData.OffsetFetchRequestTopics> request = Arrays.asList(
            new OffsetFetchRequestData.OffsetFetchRequestTopics()
                .setName("foo")
                .setPartitionIndexes(Arrays.asList(0, 1)),
            new OffsetFetchRequestData.OffsetFetchRequestTopics()
                .setName("bar")
                .setPartitionIndexes(Arrays.asList(0, 1))
        );

        // Fetching with 0 should return all invalid offsets.
        assertEquals(Arrays.asList(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setPartitions(Arrays.asList(
                    mkInvalidOffsetPartitionResponse(0),
                    mkInvalidOffsetPartitionResponse(1)
                )),
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("bar")
                .setPartitions(Arrays.asList(
                    mkInvalidOffsetPartitionResponse(0),
                    mkInvalidOffsetPartitionResponse(1)
                ))
        ), context.fetchOffsets("group", request, 0L));

        // Fetching with 1 should return data up to offset 1.
        assertEquals(Arrays.asList(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setPartitions(Arrays.asList(
                    mkOffsetPartitionResponse(0, 100L, 1, "metadata"),
                    mkInvalidOffsetPartitionResponse(1)
                )),
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("bar")
                .setPartitions(Arrays.asList(
                    mkInvalidOffsetPartitionResponse(0),
                    mkInvalidOffsetPartitionResponse(1)
                ))
        ), context.fetchOffsets("group", request, 1L));

        // Fetching with 2 should return data up to offset 2.
        assertEquals(Arrays.asList(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setPartitions(Arrays.asList(
                    mkOffsetPartitionResponse(0, 100L, 1, "metadata"),
                    mkOffsetPartitionResponse(1, 110L, 1, "metadata")
                )),
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("bar")
                .setPartitions(Arrays.asList(
                    mkInvalidOffsetPartitionResponse(0),
                    mkInvalidOffsetPartitionResponse(1)
                ))
        ), context.fetchOffsets("group", request, 2L));

        // Fetching with 3 should return data up to offset 3.
        assertEquals(Arrays.asList(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setPartitions(Arrays.asList(
                    mkOffsetPartitionResponse(0, 100L, 1, "metadata"),
                    mkOffsetPartitionResponse(1, 110L, 1, "metadata")
                )),
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("bar")
                .setPartitions(Arrays.asList(
                    mkOffsetPartitionResponse(0, 200L, 1, "metadata"),
                    mkInvalidOffsetPartitionResponse(1)
                ))
        ), context.fetchOffsets("group", request, 3L));

        // Fetching with 4 should return data up to offset 4.
        assertEquals(Arrays.asList(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setPartitions(Arrays.asList(
                    mkOffsetPartitionResponse(0, 100L, 1, "metadata"),
                    mkOffsetPartitionResponse(1, 111L, 2, "metadata")
                )),
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("bar")
                .setPartitions(Arrays.asList(
                    mkOffsetPartitionResponse(0, 200L, 1, "metadata"),
                    mkInvalidOffsetPartitionResponse(1)
                ))
        ), context.fetchOffsets("group", request, 4L));

        // Fetching with 5 should return data up to offset 5.
        assertEquals(Arrays.asList(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setPartitions(Arrays.asList(
                    mkOffsetPartitionResponse(0, 100L, 1, "metadata"),
                    mkOffsetPartitionResponse(1, 111L, 2, "metadata")
                )),
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("bar")
                .setPartitions(Arrays.asList(
                    mkOffsetPartitionResponse(0, 200L, 1, "metadata"),
                    mkOffsetPartitionResponse(1, 210L, 2, "metadata")
                ))
        ), context.fetchOffsets("group", request, 5L));

        // Fetching with Long.MAX_VALUE should return all offsets.
        assertEquals(Arrays.asList(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setPartitions(Arrays.asList(
                    mkOffsetPartitionResponse(0, 100L, 1, "metadata"),
                    mkOffsetPartitionResponse(1, 111L, 2, "metadata")
                )),
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("bar")
                .setPartitions(Arrays.asList(
                    mkOffsetPartitionResponse(0, 200L, 1, "metadata"),
                    mkOffsetPartitionResponse(1, 210L, 2, "metadata")
                ))
        ), context.fetchOffsets("group", request, Long.MAX_VALUE));
    }

    @Test
    public void testFetchOffsetsWithPendingTransactionalOffsets() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        context.groupMetadataManager.getOrMaybeCreatePersistedConsumerGroup("group", true);

        context.commitOffset("group", "foo", 0, 100L, 1);
        context.commitOffset("group", "foo", 1, 110L, 1);
        context.commitOffset("group", "bar", 0, 200L, 1);

        context.commit();

        assertEquals(3, context.lastWrittenOffset);
        assertEquals(3, context.lastCommittedOffset);

        context.commitOffset(10L, "group", "foo", 1, 111L, 1, context.time.milliseconds());
        context.commitOffset(10L, "group", "bar", 0, 201L, 1, context.time.milliseconds());
        // Note that bar-1 does not exist in the initial commits. UNSTABLE_OFFSET_COMMIT errors
        // must be returned in this case too.
        context.commitOffset(10L, "group", "bar", 1, 211L, 1, context.time.milliseconds());

        // Always use the same request.
        List<OffsetFetchRequestData.OffsetFetchRequestTopics> request = Arrays.asList(
            new OffsetFetchRequestData.OffsetFetchRequestTopics()
                .setName("foo")
                .setPartitionIndexes(Arrays.asList(0, 1)),
            new OffsetFetchRequestData.OffsetFetchRequestTopics()
                .setName("bar")
                .setPartitionIndexes(Arrays.asList(0, 1))
        );

        // Fetching offsets with "require stable" (Long.MAX_VALUE) should return the committed offset for
        // foo-0 and the UNSTABLE_OFFSET_COMMIT error for foo-1, bar-0 and bar-1.
        assertEquals(Arrays.asList(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setPartitions(Arrays.asList(
                    mkOffsetPartitionResponse(0, 100L, 1, "metadata"),
                    mkOffsetPartitionResponse(1, Errors.UNSTABLE_OFFSET_COMMIT)
                )),
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("bar")
                .setPartitions(Arrays.asList(
                    mkOffsetPartitionResponse(0, Errors.UNSTABLE_OFFSET_COMMIT),
                    mkOffsetPartitionResponse(1, Errors.UNSTABLE_OFFSET_COMMIT)
                ))
        ), context.fetchOffsets("group", request, Long.MAX_VALUE));

        // Fetching offsets without "require stable" (lastCommittedOffset) should return the committed
        // offset for foo-0, foo-1 and bar-0 and the INVALID_OFFSET for bar-1.
        assertEquals(Arrays.asList(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setPartitions(Arrays.asList(
                    mkOffsetPartitionResponse(0, 100L, 1, "metadata"),
                    mkOffsetPartitionResponse(1, 110L, 1, "metadata")
                )),
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("bar")
                .setPartitions(Arrays.asList(
                    mkOffsetPartitionResponse(0, 200L, 1, "metadata"),
                    mkInvalidOffsetPartitionResponse(1)
                ))
        ), context.fetchOffsets("group", request, context.lastCommittedOffset));

        // Commit the ongoing transaction.
        context.replayEndTransactionMarker(10L, TransactionResult.COMMIT);

        // Fetching offsets with "require stable" (Long.MAX_VALUE) should not return any errors now.
        assertEquals(Arrays.asList(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setPartitions(Arrays.asList(
                    mkOffsetPartitionResponse(0, 100L, 1, "metadata"),
                    mkOffsetPartitionResponse(1, 111L, 1, "metadata")
                )),
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("bar")
                .setPartitions(Arrays.asList(
                    mkOffsetPartitionResponse(0, 201L, 1, "metadata"),
                    mkOffsetPartitionResponse(1, 211L, 1, "metadata")
                ))
        ), context.fetchOffsets("group", request, Long.MAX_VALUE));
    }

    @Test
    public void testGenericGroupFetchAllOffsetsWithDeadGroup() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create a dead group.
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup(
            "group",
            true
        );
        group.transitionTo(ClassicGroupState.DEAD);

        assertEquals(List.of(), context.fetchAllOffsets("group", Long.MAX_VALUE));
    }

    @Test
    public void testFetchAllOffsetsWithUnknownGroup() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();
        assertEquals(List.of(), context.fetchAllOffsets("group", Long.MAX_VALUE));
    }

    @Test
    public void testFetchAllOffsetsAtDifferentCommittedOffset() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        context.groupMetadataManager.getOrMaybeCreatePersistedConsumerGroup("group", true);

        assertEquals(0, context.lastWrittenOffset);
        context.commitOffset("group", "foo", 0, 100L, 1);
        assertEquals(1, context.lastWrittenOffset);
        context.commitOffset("group", "foo", 1, 110L, 1);
        assertEquals(2, context.lastWrittenOffset);
        context.commitOffset("group", "bar", 0, 200L, 1);
        assertEquals(3, context.lastWrittenOffset);
        context.commitOffset("group", "foo", 1, 111L, 2);
        assertEquals(4, context.lastWrittenOffset);
        context.commitOffset("group", "bar", 1, 210L, 2);
        assertEquals(5, context.lastWrittenOffset);

        // Fetching with 0 should no offsets.
        assertEquals(List.of(), context.fetchAllOffsets("group", 0L));

        // Fetching with 1 should return data up to offset 1.
        assertEquals(Arrays.asList(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setPartitions(Arrays.asList(
                    mkOffsetPartitionResponse(0, 100L, 1, "metadata")
                ))
        ), context.fetchAllOffsets("group", 1L));

        // Fetching with 2 should return data up to offset 2.
        assertEquals(Arrays.asList(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setPartitions(Arrays.asList(
                    mkOffsetPartitionResponse(0, 100L, 1, "metadata"),
                    mkOffsetPartitionResponse(1, 110L, 1, "metadata")
                ))
        ), context.fetchAllOffsets("group", 2L));

        // Fetching with 3 should return data up to offset 3.
        assertEquals(Arrays.asList(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("bar")
                .setPartitions(Arrays.asList(
                    mkOffsetPartitionResponse(0, 200L, 1, "metadata")
                )),
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setPartitions(Arrays.asList(
                    mkOffsetPartitionResponse(0, 100L, 1, "metadata"),
                    mkOffsetPartitionResponse(1, 110L, 1, "metadata")
                ))
        ), context.fetchAllOffsets("group", 3L));

        // Fetching with 4 should return data up to offset 4.
        assertEquals(Arrays.asList(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("bar")
                .setPartitions(Arrays.asList(
                    mkOffsetPartitionResponse(0, 200L, 1, "metadata")
                )),
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setPartitions(Arrays.asList(
                    mkOffsetPartitionResponse(0, 100L, 1, "metadata"),
                    mkOffsetPartitionResponse(1, 111L, 2, "metadata")
                ))
        ), context.fetchAllOffsets("group", 4L));

        // Fetching with Long.MAX_VALUE should return all offsets.
        assertEquals(Arrays.asList(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("bar")
                .setPartitions(Arrays.asList(
                    mkOffsetPartitionResponse(0, 200L, 1, "metadata"),
                    mkOffsetPartitionResponse(1, 210L, 2, "metadata")
                )),
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setPartitions(Arrays.asList(
                    mkOffsetPartitionResponse(0, 100L, 1, "metadata"),
                    mkOffsetPartitionResponse(1, 111L, 2, "metadata")
                ))
        ), context.fetchAllOffsets("group", Long.MAX_VALUE));
    }

    @Test
    public void testFetchAllOffsetsWithPendingTransactionalOffsets() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        context.groupMetadataManager.getOrMaybeCreatePersistedConsumerGroup("group", true);

        context.commitOffset("group", "foo", 0, 100L, 1);
        context.commitOffset("group", "foo", 1, 110L, 1);
        context.commitOffset("group", "bar", 0, 200L, 1);

        context.commit();

        assertEquals(3, context.lastWrittenOffset);
        assertEquals(3, context.lastCommittedOffset);

        context.commitOffset(10L, "group", "foo", 1, 111L, 1, context.time.milliseconds());
        context.commitOffset(10L, "group", "bar", 0, 201L, 1, context.time.milliseconds());
        // Note that bar-1 does not exist in the initial commits. The API does not return it at all until
        // the transaction is committed.
        context.commitOffset(10L, "group", "bar", 1, 211L, 1, context.time.milliseconds());

        // Fetching offsets with "require stable" (Long.MAX_VALUE) should return the committed offset for
        // foo-0 and the UNSTABLE_OFFSET_COMMIT error for foo-1 and bar-0.
        assertEquals(Arrays.asList(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("bar")
                .setPartitions(Arrays.asList(
                    mkOffsetPartitionResponse(0, Errors.UNSTABLE_OFFSET_COMMIT)
                )),
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setPartitions(Arrays.asList(
                    mkOffsetPartitionResponse(0, 100L, 1, "metadata"),
                    mkOffsetPartitionResponse(1, Errors.UNSTABLE_OFFSET_COMMIT)
                ))
        ), context.fetchAllOffsets("group", Long.MAX_VALUE));

        // Fetching offsets without "require stable" (lastCommittedOffset) should the committed
        // offset for the foo-0, foo-1 and bar-0.
        assertEquals(Arrays.asList(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("bar")
                .setPartitions(Arrays.asList(
                    mkOffsetPartitionResponse(0, 200L, 1, "metadata")
                )),
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setPartitions(Arrays.asList(
                    mkOffsetPartitionResponse(0, 100L, 1, "metadata"),
                    mkOffsetPartitionResponse(1, 110L, 1, "metadata")
                ))
        ), context.fetchAllOffsets("group", context.lastCommittedOffset));

        // Commit the ongoing transaction.
        context.replayEndTransactionMarker(10L, TransactionResult.COMMIT);

        // Fetching offsets with "require stable" (Long.MAX_VALUE) should not return any errors now.
        assertEquals(Arrays.asList(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("bar")
                .setPartitions(Arrays.asList(
                    mkOffsetPartitionResponse(0, 201L, 1, "metadata"),
                    mkOffsetPartitionResponse(1, 211L, 1, "metadata")
                )),
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setPartitions(Arrays.asList(
                    mkOffsetPartitionResponse(0, 100L, 1, "metadata"),
                    mkOffsetPartitionResponse(1, 111L, 1, "metadata")
                ))
        ), context.fetchAllOffsets("group", Long.MAX_VALUE));
    }

    @Test
    public void testConsumerGroupOffsetFetchWithMemberIdAndEpoch() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();
        // Create consumer group.
        ConsumerGroup group = context.groupMetadataManager.getOrMaybeCreatePersistedConsumerGroup("group", true);
        // Create member.
        group.updateMember(new ConsumerGroupMember.Builder("member").build());

        verifyOffsetFetchWithMemberIdAndEpoch(context);
    }

    @Test
    public void testStreamsGroupOffsetFetchWithMemberIdAndEpoch() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();
        // Create streams group.
        StreamsGroup group = context.groupMetadataManager.getOrMaybeCreatePersistedStreamsGroup("group", true);
        // Create member.
        group.updateMember(StreamsGroupMember.Builder.withDefaults("member").build());

        verifyOffsetFetchWithMemberIdAndEpoch(context);
    }

    private static void verifyOffsetFetchWithMemberIdAndEpoch(OffsetMetadataManagerTestContext context) {
        // Commit offset.
        context.commitOffset("group", "foo", 0, 100L, 1);

        // Fetch offsets case.
        List<OffsetFetchRequestData.OffsetFetchRequestTopics> topics = List.of(
            new OffsetFetchRequestData.OffsetFetchRequestTopics()
                .setName("foo")
                .setPartitionIndexes(List.of(0))
        );

        assertEquals(List.of(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setPartitions(List.of(
                    mkOffsetPartitionResponse(0, 100L, 1, "metadata")
                ))
        ), context.fetchOffsets("group", "member", 0, topics, Long.MAX_VALUE));

        // Fetch all offsets case.
        assertEquals(List.of(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setPartitions(List.of(
                    mkOffsetPartitionResponse(0, 100L, 1, "metadata")
                ))
        ), context.fetchAllOffsets("group", "member", 0, Long.MAX_VALUE));
    }

    @Test
    public void testConsumerGroupOffsetFetchFromAdminClient() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();
        // Create consumer group.
        ConsumerGroup group = context.groupMetadataManager.getOrMaybeCreatePersistedConsumerGroup("group", true);
        // Create member.
        group.getOrMaybeCreateMember("member", true);

        verifyOffsetFetchFromAdminClient(context);
    }

    @Test
    public void testStreamsGroupOffsetFetchFromAdminClient() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();
        // Create streams group.
        StreamsGroup group = context.groupMetadataManager.getOrMaybeCreatePersistedStreamsGroup("group", true);
        // Create member.
        group.getOrCreateDefaultMember("member");

        verifyOffsetFetchFromAdminClient(context);
    }

    private static void verifyOffsetFetchFromAdminClient(OffsetMetadataManagerTestContext context) {
        // Commit offset.
        context.commitOffset("group", "foo", 0, 100L, 1);

        // Fetch offsets case.
        List<OffsetFetchRequestData.OffsetFetchRequestTopics> topics = List.of(
            new OffsetFetchRequestData.OffsetFetchRequestTopics()
                .setName("foo")
                .setPartitionIndexes(List.of(0))
        );

        assertEquals(List.of(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setPartitions(List.of(
                    mkOffsetPartitionResponse(0, 100L, 1, "metadata")
                ))
        ), context.fetchOffsets("group", topics, Long.MAX_VALUE));

        // Fetch all offsets case.
        assertEquals(List.of(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setPartitions(List.of(
                    mkOffsetPartitionResponse(0, 100L, 1, "metadata")
                ))
        ), context.fetchAllOffsets("group", Long.MAX_VALUE));
    }

    @Test
    public void testConsumerGroupOffsetFetchWithUnknownMemberId() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();
        context.groupMetadataManager.getOrMaybeCreatePersistedConsumerGroup("group", true);

        verifyFetchWithUnknownMemberId(context);
    }
    
    @Test
    public void testStreamsGroupOffsetFetchWithUnknownMemberId() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();
        context.groupMetadataManager.getOrMaybeCreatePersistedStreamsGroup("group", true);

        verifyFetchWithUnknownMemberId(context);
    }

    private static void verifyFetchWithUnknownMemberId(OffsetMetadataManagerTestContext context) {
        // Fetch offsets case.
        List<OffsetFetchRequestData.OffsetFetchRequestTopics> topics = List.of(
            new OffsetFetchRequestData.OffsetFetchRequestTopics()
                .setName("foo")
                .setPartitionIndexes(List.of(0))
        );

        // Fetch offsets cases.
        assertThrows(UnknownMemberIdException.class,
            () -> context.fetchOffsets("group", "", 0, topics, Long.MAX_VALUE));
        assertThrows(UnknownMemberIdException.class,
            () -> context.fetchOffsets("group", "member", 0, topics, Long.MAX_VALUE));

        // Fetch all offsets cases.
        assertThrows(UnknownMemberIdException.class,
            () -> context.fetchAllOffsets("group", "", 0, Long.MAX_VALUE));
        assertThrows(UnknownMemberIdException.class,
            () -> context.fetchAllOffsets("group", "member", 0, Long.MAX_VALUE));
    }

    @Test
    public void testConsumerGroupOffsetFetchWithStaleMemberEpoch() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();
        ConsumerGroup group = context.groupMetadataManager.getOrMaybeCreatePersistedConsumerGroup("group", true);
        group.updateMember(new ConsumerGroupMember.Builder("member").build());

        verifyOffsetFetchWithStaleMemberEpoch(context);
    }
    
    @Test
    public void testStreamsGroupOffsetFetchWithStaleMemberEpoch() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();
        StreamsGroup group = context.groupMetadataManager.getOrMaybeCreatePersistedStreamsGroup("group", true);
        group.updateMember(StreamsGroupMember.Builder.withDefaults("member").build());

        verifyOffsetFetchWithStaleMemberEpoch(context);
    }

    private static void verifyOffsetFetchWithStaleMemberEpoch(OffsetMetadataManagerTestContext context) {
        // Fetch offsets case.
        List<OffsetFetchRequestData.OffsetFetchRequestTopics> topics = List.of(
            new OffsetFetchRequestData.OffsetFetchRequestTopics()
                .setName("foo")
                .setPartitionIndexes(List.of(0))
        );

        // Fetch offsets case.
        assertThrows(StaleMemberEpochException.class,
            () -> context.fetchOffsets("group", "member", 10, topics, Long.MAX_VALUE));

        // Fetch all offsets case.
        assertThrows(StaleMemberEpochException.class,
            () -> context.fetchAllOffsets("group", "member", 10, Long.MAX_VALUE));
    }

    @Test
    public void testConsumerGroupOffsetFetchWithIllegalGenerationId() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();
        ConsumerGroup group = context.groupMetadataManager.getOrMaybeCreatePersistedConsumerGroup("group", true);
        group.updateMember(new ConsumerGroupMember.Builder("member")
                .setClassicMemberMetadata(new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata())
                .build()
        );

        List<OffsetFetchRequestData.OffsetFetchRequestTopics> topics = List.of(
            new OffsetFetchRequestData.OffsetFetchRequestTopics()
                .setName("foo")
                .setPartitionIndexes(List.of(0))
        );

        // Fetch offsets case.
        assertThrows(IllegalGenerationException.class,
            () -> context.fetchOffsets("group", "member", 10, topics, Long.MAX_VALUE));

        // Fetch all offsets case.
        assertThrows(IllegalGenerationException.class,
            () -> context.fetchAllOffsets("group", "member", 10, Long.MAX_VALUE));
    }

    @Test
    public void testGenericGroupOffsetDelete() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup(
            "foo",
            true
        );
        context.commitOffset("foo", "bar", 0, 100L, 0);
        group.setSubscribedTopics(Optional.of(Set.of()));
        context.testOffsetDeleteWith("foo", "bar", 0, Errors.NONE);
        assertFalse(context.hasOffset("foo", "bar", 0));
    }

    @Test
    public void testGenericGroupOffsetDeleteWithErrors() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup(
            "foo",
            true
        );
        group.setSubscribedTopics(Optional.of(Set.of("bar")));
        context.commitOffset("foo", "bar", 0, 100L, 0);

        // Delete the offset whose topic partition doesn't exist.
        context.testOffsetDeleteWith("foo", "bar1", 0, Errors.NONE);
        // Delete the offset from the topic that the group is subscribed to.
        context.testOffsetDeleteWith("foo", "bar", 0, Errors.GROUP_SUBSCRIBED_TO_TOPIC);
    }

    @Test
    public void testGenericGroupOffsetDeleteWithPendingTransactionalOffsets() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup(
            "foo",
            true
        );
        context.commitOffset(10L, "foo", "bar", 0, 100L, 0, context.time.milliseconds());
        group.setSubscribedTopics(Optional.of(Set.of()));
        context.testOffsetDeleteWith("foo", "bar", 0, Errors.NONE);
        assertFalse(context.hasOffset("foo", "bar", 0));
    }

    @Test
    public void testConsumerGroupOffsetDelete() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();
        ConsumerGroup group = context.groupMetadataManager.getOrMaybeCreatePersistedConsumerGroup(
            "foo",
            true
        );
        verifyOffsetDelete(context, group);
    }
    
    @Test
    public void testStreamsGroupOffsetDelete() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();
        StreamsGroup group = context.groupMetadataManager.getOrMaybeCreatePersistedStreamsGroup(
            "foo",
            true
        );
        verifyOffsetDelete(context, group);
    }

    private static void verifyOffsetDelete(OffsetMetadataManagerTestContext context, Group group) {
        context.commitOffset("foo", "bar", 0, 100L, 0);
        assertFalse(group.isSubscribedToTopic("bar"));
        context.testOffsetDeleteWith("foo", "bar", 0, Errors.NONE);
    }

    @Test
    public void testConsumerGroupOffsetDeleteWithErrors() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();
        ConsumerGroup group = context.groupMetadataManager.getOrMaybeCreatePersistedConsumerGroup(
            "foo",
            true
        );
        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder("member1")
            .setSubscribedTopicNames(List.of("bar"))
            .build();
        group.updateMember(member1);
        verifyOffsetDeleteWithErrors(context, group);
    }
    
    @Test
    public void testStreamsGroupOffsetDeleteWithErrors() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();
        StreamsGroup group = context.groupMetadataManager.getOrMaybeCreatePersistedStreamsGroup(
            "foo",
            true
        );
        StreamsGroupMember member1 = StreamsGroupMember.Builder.withDefaults("member1")
            .build();
        StreamsTopology topology = new StreamsTopology(0,
            Map.of("subtopology",
                new StreamsGroupTopologyValue.Subtopology()
                    .setSubtopologyId("subtopology")
                    .setSourceTopics(List.of("bar"))
            ));
        group.setTopology(topology);
        group.updateMember(member1);
        verifyOffsetDeleteWithErrors(context, group);
    }

    private static void verifyOffsetDeleteWithErrors(OffsetMetadataManagerTestContext context, Group group) {
        context.commitOffset("foo", "bar", 0, 100L, 0);
        assertTrue(group.isSubscribedToTopic("bar"));

        // Delete the offset whose topic partition doesn't exist.
        context.testOffsetDeleteWith("foo", "bar1", 0, Errors.NONE);
        // Delete the offset from the topic that the group is subscribed to.
        context.testOffsetDeleteWith("foo", "bar", 0, Errors.GROUP_SUBSCRIBED_TO_TOPIC);
    }

    @Test
    public void testConsumerGroupOffsetDeleteWithPendingTransactionalOffsets() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();
        ConsumerGroup group = context.groupMetadataManager.getOrMaybeCreatePersistedConsumerGroup(
            "foo",
            true
        );
        verifyOffsetDeleteWithPendingTransactionalOffsets(context, group);
    }
    
    @Test
    public void testStreamsGroupOffsetDeleteWithPendingTransactionalOffsets() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();
        StreamsGroup group = context.groupMetadataManager.getOrMaybeCreatePersistedStreamsGroup(
            "foo",
            true
        );
        verifyOffsetDeleteWithPendingTransactionalOffsets(context, group);
    }

    private static void verifyOffsetDeleteWithPendingTransactionalOffsets(OffsetMetadataManagerTestContext context, Group group) {
        context.commitOffset(10L, "foo", "bar", 0, 100L, 0, context.time.milliseconds());
        assertFalse(group.isSubscribedToTopic("bar"));
        context.testOffsetDeleteWith("foo", "bar", 0, Errors.NONE);
        assertFalse(context.hasOffset("foo", "bar", 0));
    }

    @ParameterizedTest
    @EnumSource(value = Group.GroupType.class, names = {"CLASSIC", "CONSUMER", "STREAMS"})
    public void testDeleteGroupAllOffsets(Group.GroupType groupType) {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();
        context.getOrMaybeCreateGroup(groupType, "foo");

        context.commitOffset("foo", "bar-0", 0, 100L, 0);
        context.commitOffset("foo", "bar-0", 1, 100L, 0);
        context.commitOffset("foo", "bar-1", 0, 100L, 0);

        List<CoordinatorRecord> expectedRecords = Arrays.asList(
            GroupCoordinatorRecordHelpers.newOffsetCommitTombstoneRecord("foo", "bar-1", 0),
            GroupCoordinatorRecordHelpers.newOffsetCommitTombstoneRecord("foo", "bar-0", 0),
            GroupCoordinatorRecordHelpers.newOffsetCommitTombstoneRecord("foo", "bar-0", 1)
        );

        List<CoordinatorRecord> records = new ArrayList<>();
        int numDeleteOffsets = context.deleteAllOffsets("foo", records);

        assertEquals(expectedRecords, records);
        assertEquals(3, numDeleteOffsets);
    }

    @ParameterizedTest
    @EnumSource(value = Group.GroupType.class, names = {"CLASSIC", "CONSUMER", "STREAMS"})
    public void testDeleteGroupAllOffsetsWithPendingTransactionalOffsets(Group.GroupType groupType) {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();
        context.getOrMaybeCreateGroup(groupType, "foo");

        context.commitOffset("foo", "bar-0", 0, 100L, 0);
        context.commitOffset("foo", "bar-0", 1, 100L, 0);
        context.commitOffset("foo", "bar-1", 0, 100L, 0);

        context.commitOffset(10L, "foo", "bar-1", 0, 101L, 0, context.time.milliseconds());
        context.commitOffset(10L, "foo", "bar-2", 0, 100L, 0, context.time.milliseconds());

        List<CoordinatorRecord> expectedRecords = Arrays.asList(
            GroupCoordinatorRecordHelpers.newOffsetCommitTombstoneRecord("foo", "bar-1", 0),
            GroupCoordinatorRecordHelpers.newOffsetCommitTombstoneRecord("foo", "bar-0", 0),
            GroupCoordinatorRecordHelpers.newOffsetCommitTombstoneRecord("foo", "bar-0", 1),
            GroupCoordinatorRecordHelpers.newOffsetCommitTombstoneRecord("foo", "bar-2", 0)
        );

        List<CoordinatorRecord> records = new ArrayList<>();
        int numDeleteOffsets = context.deleteAllOffsets("foo", records);

        assertEquals(expectedRecords, records);
        assertEquals(4, numDeleteOffsets);

        assertFalse(context.hasOffset("foo", "bar-0", 0));
        assertFalse(context.hasOffset("foo", "bar-0", 1));
        assertFalse(context.hasOffset("foo", "bar-1", 0));
        assertFalse(context.hasOffset("foo", "bar-2", 0));
    }

    @Test
    public void testCleanupExpiredOffsetsGroupHasNoOffsets() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder()
            .build();

        List<CoordinatorRecord> records = new ArrayList<>();
        assertTrue(context.cleanupExpiredOffsets("unknown-group-id", records));
        assertEquals(List.of(), records);
    }

    @Test
    public void testCleanupExpiredOffsetsGroupDoesNotExist() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder()
            .withGroupMetadataManager(groupMetadataManager)
            .build();

        when(groupMetadataManager.group("unknown-group-id")).thenThrow(GroupIdNotFoundException.class);
        context.commitOffset("unknown-group-id", "topic", 0, 100L, 0);
        assertThrows(GroupIdNotFoundException.class, () -> context.cleanupExpiredOffsets("unknown-group-id", new ArrayList<>()));
    }

    @Test
    public void testCleanupExpiredOffsetsEmptyOffsetExpirationCondition() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        Group group = mock(Group.class);

        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder()
            .withGroupMetadataManager(groupMetadataManager)
            .build();

        context.commitOffset("group-id", "topic", 0, 100L, 0);

        when(groupMetadataManager.group("group-id")).thenReturn(group);
        when(group.offsetExpirationCondition()).thenReturn(Optional.empty());

        List<CoordinatorRecord> records = new ArrayList<>();
        assertFalse(context.cleanupExpiredOffsets("group-id", records));
        assertEquals(List.of(), records);
    }

    @Test
    public void testCleanupExpiredOffsets() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        Group group = mock(Group.class);

        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder()
            .withGroupMetadataManager(groupMetadataManager)
            .withOffsetsRetentionMinutes(1)
            .build();

        long commitTimestamp = context.time.milliseconds();

        context.commitOffset("group-id", "firstTopic", 0, 100L, 0, commitTimestamp);
        context.commitOffset("group-id", "secondTopic", 0, 100L, 0, commitTimestamp);
        context.commitOffset("group-id", "secondTopic", 1, 100L, 0, commitTimestamp + 500);

        context.time.sleep(Duration.ofMinutes(1).toMillis());

        // firstTopic-0: group is still subscribed to firstTopic. Do not expire.
        // secondTopic-0: should expire as offset retention has passed.
        // secondTopic-1: has not passed offset retention. Do not expire.
        List<CoordinatorRecord> expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newOffsetCommitTombstoneRecord("group-id", "secondTopic", 0)
        );

        when(groupMetadataManager.group("group-id")).thenReturn(group);
        when(group.offsetExpirationCondition()).thenReturn(Optional.of(
            new OffsetExpirationConditionImpl(offsetAndMetadata -> offsetAndMetadata.commitTimestampMs)));
        when(group.isSubscribedToTopic("firstTopic")).thenReturn(true);
        when(group.isSubscribedToTopic("secondTopic")).thenReturn(false);

        List<CoordinatorRecord> records = new ArrayList<>();
        assertFalse(context.cleanupExpiredOffsets("group-id", records));
        assertEquals(expectedRecords, records);

        // Expire secondTopic-1.
        context.time.sleep(500);
        expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newOffsetCommitTombstoneRecord("group-id", "secondTopic", 1)
        );

        records = new ArrayList<>();
        assertFalse(context.cleanupExpiredOffsets("group-id", records));
        assertEquals(expectedRecords, records);

        // Add 2 more commits, then expire all.
        when(group.isSubscribedToTopic("firstTopic")).thenReturn(false);
        context.commitOffset("group-id", "firstTopic", 1, 100L, 0, commitTimestamp + 500);
        context.commitOffset("group-id", "secondTopic", 0, 101L, 0, commitTimestamp + 500);

        expectedRecords = Arrays.asList(
            GroupCoordinatorRecordHelpers.newOffsetCommitTombstoneRecord("group-id", "firstTopic", 0),
            GroupCoordinatorRecordHelpers.newOffsetCommitTombstoneRecord("group-id", "firstTopic", 1),
            GroupCoordinatorRecordHelpers.newOffsetCommitTombstoneRecord("group-id", "secondTopic", 0)
        );

        records = new ArrayList<>();
        assertTrue(context.cleanupExpiredOffsets("group-id", records));
        assertEquals(expectedRecords, records);
    }

    @Test
    public void testCleanupExpiredOffsetsWithPendingTransactionalOffsets() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        Group group = mock(Group.class);

        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder()
            .withGroupMetadataManager(groupMetadataManager)
            .withOffsetsRetentionMinutes(1)
            .build();

        long commitTimestamp = context.time.milliseconds();

        context.commitOffset("group-id", "foo", 0, 100L, 0, commitTimestamp);
        context.commitOffset(10L, "group-id", "foo", 0, 101L, 0, commitTimestamp + 500);

        context.time.sleep(Duration.ofMinutes(1).toMillis());

        when(groupMetadataManager.group("group-id")).thenReturn(group);
        when(group.offsetExpirationCondition()).thenReturn(Optional.of(
            new OffsetExpirationConditionImpl(offsetAndMetadata -> offsetAndMetadata.commitTimestampMs)));
        when(group.isSubscribedToTopic("foo")).thenReturn(false);

        // foo-0 should not be expired because it has a pending transactional offset commit.
        List<CoordinatorRecord> records = new ArrayList<>();
        assertFalse(context.cleanupExpiredOffsets("group-id", records));
        assertEquals(List.of(), records);
    }

    @Test
    public void testCleanupExpiredOffsetsWithPendingTransactionalOffsetsOnly() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        Group group = mock(Group.class);

        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder()
            .withGroupMetadataManager(groupMetadataManager)
            .withOffsetsRetentionMinutes(1)
            .build();

        long commitTimestamp = context.time.milliseconds();

        context.commitOffset("group-id", "foo", 0, 100L, 0, commitTimestamp);
        context.commitOffset(10L, "group-id", "foo", 1, 101L, 0, commitTimestamp + 500);

        context.time.sleep(Duration.ofMinutes(1).toMillis());

        when(groupMetadataManager.group("group-id")).thenReturn(group);
        when(group.offsetExpirationCondition()).thenReturn(Optional.of(
            new OffsetExpirationConditionImpl(offsetAndMetadata -> offsetAndMetadata.commitTimestampMs)));
        when(group.isSubscribedToTopic("foo")).thenReturn(false);

        // foo-0 is expired, but the group is not deleted beacuse it has pending transactional offset commits.
        List<CoordinatorRecord> expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newOffsetCommitTombstoneRecord("group-id", "foo", 0)
        );
        List<CoordinatorRecord> records = new ArrayList<>();
        assertFalse(context.cleanupExpiredOffsets("group-id", records));
        assertEquals(expectedRecords, records);

        // No offsets are expired, and the group is still not deleted because it has pending transactional offset commits.
        records = new ArrayList<>();
        assertFalse(context.cleanupExpiredOffsets("group-id", records));
        assertEquals(List.of(), records);
    }

    private static OffsetFetchResponseData.OffsetFetchResponsePartitions mkOffsetPartitionResponse(
        int partition,
        long offset,
        int leaderEpoch,
        String metadata
    ) {
        return new OffsetFetchResponseData.OffsetFetchResponsePartitions()
            .setPartitionIndex(partition)
            .setCommittedOffset(offset)
            .setCommittedLeaderEpoch(leaderEpoch)
            .setMetadata(metadata);
    }

    private static OffsetFetchResponseData.OffsetFetchResponsePartitions mkInvalidOffsetPartitionResponse(int partition) {
        return new OffsetFetchResponseData.OffsetFetchResponsePartitions()
            .setPartitionIndex(partition)
            .setCommittedOffset(INVALID_OFFSET)
            .setCommittedLeaderEpoch(-1)
            .setMetadata("");
    }

    private static OffsetFetchResponseData.OffsetFetchResponsePartitions mkOffsetPartitionResponse(int partition, Errors error) {
        return new OffsetFetchResponseData.OffsetFetchResponsePartitions()
            .setPartitionIndex(partition)
            .setErrorCode(error.code())
            .setCommittedOffset(INVALID_OFFSET)
            .setCommittedLeaderEpoch(-1)
            .setMetadata("");
    }

    @ParameterizedTest
    @MethodSource("uuids")
    public void testReplay(Uuid topicId) {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        verifyReplay(context, "foo", "bar", 0, new OffsetAndMetadata(
            0L,
            100L,
            OptionalInt.empty(),
            "small",
            context.time.milliseconds(),
            OptionalLong.empty(),
            topicId
        ));

        verifyReplay(context, "foo", "bar", 0, new OffsetAndMetadata(
            1L,
            200L,
            OptionalInt.of(10),
            "small",
            context.time.milliseconds(),
            OptionalLong.empty(),
            topicId
        ));

        verifyReplay(context, "foo", "bar", 1, new OffsetAndMetadata(
            2L,
            200L,
            OptionalInt.of(10),
            "small",
            context.time.milliseconds(),
            OptionalLong.empty(),
            topicId
        ));

        verifyReplay(context, "foo", "bar", 1, new OffsetAndMetadata(
            3L,
            300L,
            OptionalInt.of(10),
            "small",
            context.time.milliseconds(),
            OptionalLong.of(12345L),
            topicId
        ));
    }

    @Test
    public void testTransactionalReplay() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        verifyTransactionalReplay(context, 5, "foo", "bar", 0, new OffsetAndMetadata(
            0L,
            100L,
            OptionalInt.empty(),
            "small",
            context.time.milliseconds(),
            OptionalLong.empty(),
            Uuid.ZERO_UUID
        ));

        verifyTransactionalReplay(context, 5, "foo", "bar", 1, new OffsetAndMetadata(
            1L,
            101L,
            OptionalInt.empty(),
            "small",
            context.time.milliseconds(),
            OptionalLong.empty(),
            Uuid.ZERO_UUID
        ));

        verifyTransactionalReplay(context, 5, "bar", "zar", 0, new OffsetAndMetadata(
            2L,
            100L,
            OptionalInt.empty(),
            "small",
            context.time.milliseconds(),
            OptionalLong.empty(),
            Uuid.ZERO_UUID
        ));

        verifyTransactionalReplay(context, 5, "bar", "zar", 1, new OffsetAndMetadata(
            3L,
            101L,
            OptionalInt.empty(),
            "small",
            context.time.milliseconds(),
            OptionalLong.empty(),
            Uuid.ZERO_UUID
        ));

        verifyTransactionalReplay(context, 6, "foo", "bar", 2, new OffsetAndMetadata(
            4L,
            102L,
            OptionalInt.empty(),
            "small",
            context.time.milliseconds(),
            OptionalLong.empty(),
            Uuid.ZERO_UUID
        ));

        verifyTransactionalReplay(context, 6, "foo", "bar", 3, new OffsetAndMetadata(
            5L,
            102L,
            OptionalInt.empty(),
            "small",
            context.time.milliseconds(),
            OptionalLong.empty(),
            Uuid.ZERO_UUID
        ));
    }

    @Test
    public void testReplayWithTombstoneAndPendingTransactionalOffsets() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Add the offsets.
        verifyReplay(context, "foo", "bar", 0, new OffsetAndMetadata(
            0L,
            100L,
            OptionalInt.empty(),
            "small",
            context.time.milliseconds(),
            OptionalLong.empty(),
            Uuid.ZERO_UUID
        ));

        verifyTransactionalReplay(context, 10L, "foo", "bar", 0, new OffsetAndMetadata(
            1L,
            100L,
            OptionalInt.empty(),
            "small",
            context.time.milliseconds(),
            OptionalLong.empty(),
            Uuid.ZERO_UUID
        ));

        verifyTransactionalReplay(context, 10L, "foo", "bar", 1, new OffsetAndMetadata(
            2L,
            100L,
            OptionalInt.empty(),
            "small",
            context.time.milliseconds(),
            OptionalLong.empty(),
            Uuid.ZERO_UUID
        ));

        // Delete the offsets.
        context.replay(GroupCoordinatorRecordHelpers.newOffsetCommitTombstoneRecord(
            "foo",
            "bar",
            0
        ));

        context.replay(GroupCoordinatorRecordHelpers.newOffsetCommitTombstoneRecord(
            "foo",
            "bar",
            1
        ));

        // Verify that the offset is gone.
        assertFalse(context.hasOffset("foo", "bar", 0));
        assertFalse(context.hasOffset("foo", "bar", 1));
    }

    @Test
    public void testReplayTransactionEndMarkerWithCommit() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Add regular offset commit.
        verifyReplay(context, "foo", "bar", 0, new OffsetAndMetadata(
            0L,
            99L,
            OptionalInt.empty(),
            "small",
            context.time.milliseconds(),
            OptionalLong.empty(),
            Uuid.ZERO_UUID
        ));

        // Add pending transactional commit for producer id 5.
        verifyTransactionalReplay(context, 5L, "foo", "bar", 0, new OffsetAndMetadata(
            1L,
            100L,
            OptionalInt.empty(),
            "small",
            context.time.milliseconds(),
            OptionalLong.empty(),
            Uuid.ZERO_UUID
        ));

        // Add pending transactional commit for producer id 6.
        verifyTransactionalReplay(context, 6L, "foo", "bar", 1, new OffsetAndMetadata(
            2L,
            200L,
            OptionalInt.empty(),
            "small",
            context.time.milliseconds(),
            OptionalLong.empty(),
            Uuid.ZERO_UUID
        ));

        // Replaying an end marker with an unknown producer id should not fail.
        context.replayEndTransactionMarker(1L, TransactionResult.COMMIT);

        // Replaying an end marker to commit transaction of producer id 5.
        context.replayEndTransactionMarker(5L, TransactionResult.COMMIT);

        // The pending offset is removed...
        assertNull(context.offsetMetadataManager.pendingTransactionalOffset(
            5L,
            "foo",
            "bar",
            0
        ));

        // ... and added to the main offset storage.
        assertEquals(new OffsetAndMetadata(
            1L,
            100L,
            OptionalInt.empty(),
            "small",
            context.time.milliseconds(),
            OptionalLong.empty(),
            Uuid.ZERO_UUID
        ), context.offsetMetadataManager.offset(
            "foo",
            "bar",
            0
        ));

        // Replaying an end marker to abort transaction of producer id 6.
        context.replayEndTransactionMarker(6L, TransactionResult.ABORT);

        // The pending offset is removed from the pending offsets and
        // it is not added to the main offset storage.
        assertNull(context.offsetMetadataManager.pendingTransactionalOffset(
            6L,
            "foo",
            "bar",
            1
        ));
        assertNull(context.offsetMetadataManager.offset(
            "foo",
            "bar",
            1
        ));
    }

    @Test
    public void testReplayTransactionEndMarkerKeepsTheMostRecentCommittedOffset() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Add pending transactional offset commit for producer id 5.
        verifyTransactionalReplay(context, 5L, "foo", "bar", 0, new OffsetAndMetadata(
            0L,
            100L,
            OptionalInt.empty(),
            "small",
            context.time.milliseconds(),
            OptionalLong.empty(),
            Uuid.ZERO_UUID
        ));

        // Add regular offset commit.
        verifyReplay(context, "foo", "bar", 0, new OffsetAndMetadata(
            1L,
            101L,
            OptionalInt.empty(),
            "small",
            context.time.milliseconds(),
            OptionalLong.empty(),
            Uuid.ZERO_UUID
        ));

        // Replaying an end marker to commit transaction of producer id 5.
        context.replayEndTransactionMarker(5L, TransactionResult.COMMIT);

        // The pending offset is removed...
        assertNull(context.offsetMetadataManager.pendingTransactionalOffset(
            5L,
            "foo",
            "bar",
            0
        ));

        // ... but it is not added to the main storage because the regular
        // committed offset is more recent.
        assertEquals(new OffsetAndMetadata(
            1L,
            101L,
            OptionalInt.empty(),
            "small",
            context.time.milliseconds(),
            OptionalLong.empty(),
            Uuid.ZERO_UUID
        ), context.offsetMetadataManager.offset(
            "foo",
            "bar",
            0
        ));
    }

    @Test
    public void testOffsetCommitsNumberMetricWithTransactionalOffsets() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Add pending transactional commit for producer id 4.
        verifyTransactionalReplay(context, 4L, "foo", "bar", 0, new OffsetAndMetadata(
            0L,
            100L,
            OptionalInt.empty(),
            "small",
            context.time.milliseconds(),
            OptionalLong.empty(),
            Uuid.ZERO_UUID
        ));

        // Add pending transactional commit for producer id 5.
        verifyTransactionalReplay(context, 5L, "foo", "bar", 0, new OffsetAndMetadata(
            1L,
            101L,
            OptionalInt.empty(),
            "small",
            context.time.milliseconds(),
            OptionalLong.empty(),
            Uuid.ZERO_UUID
        ));

        // Add pending transactional commit for producer id 6.
        verifyTransactionalReplay(context, 6L, "foo", "bar", 1, new OffsetAndMetadata(
            2L,
            200L,
            OptionalInt.empty(),
            "small",
            context.time.milliseconds(),
            OptionalLong.empty(),
            Uuid.ZERO_UUID
        ));

        // Commit all the transactions.
        context.replayEndTransactionMarker(4L, TransactionResult.COMMIT);
        context.replayEndTransactionMarker(5L, TransactionResult.COMMIT);
        context.replayEndTransactionMarker(6L, TransactionResult.COMMIT);

        // Verify the sensor is called twice as we have only
        // two partitions.
        verify(context.metrics, times(2)).incrementNumOffsets();
    }

    @Test
    public void testOffsetCommitsSensor() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create an empty group.
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup(
            "foo",
            true
        );

        // Add member.
        group.add(mkGenericMember("member", Optional.of("new-instance-id")));

        // Transition to next generation.
        group.transitionTo(ClassicGroupState.PREPARING_REBALANCE);
        group.initNextGeneration();
        assertEquals(1, group.generationId());
        group.transitionTo(ClassicGroupState.STABLE);

        context.commitOffset(
            new OffsetCommitRequestData()
                .setGroupId("foo")
                .setMemberId("member")
                .setGenerationIdOrMemberEpoch(1)
                .setRetentionTimeMs(1234L)
                .setTopics(List.of(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(Arrays.asList(
                            new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L),
                            new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                .setPartitionIndex(1)
                                .setCommittedOffset(150L)
                        ))
                ))
        );

        verify(context.metrics).record(OFFSET_COMMITS_SENSOR_NAME, 2);
    }

    @Test
    public void testOffsetsExpiredSensor() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        Group group = mock(Group.class);

        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder()
            .withGroupMetadataManager(groupMetadataManager)
            .withOffsetsRetentionMinutes(1)
            .build();

        long commitTimestamp = context.time.milliseconds();

        context.commitOffset("group-id", "firstTopic", 0, 100L, 0, commitTimestamp);
        context.commitOffset("group-id", "secondTopic", 0, 100L, 0, commitTimestamp);
        context.commitOffset("group-id", "secondTopic", 1, 100L, 0, commitTimestamp + 500);

        context.time.sleep(Duration.ofMinutes(1).toMillis());

        // firstTopic-0: group is still subscribed to firstTopic. Do not expire.
        // secondTopic-0: should expire as offset retention has passed.
        // secondTopic-1: has not passed offset retention. Do not expire.
        List<CoordinatorRecord> expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newOffsetCommitTombstoneRecord("group-id", "secondTopic", 0)
        );

        when(groupMetadataManager.group("group-id")).thenReturn(group);
        when(group.offsetExpirationCondition()).thenReturn(Optional.of(
            new OffsetExpirationConditionImpl(offsetAndMetadata -> offsetAndMetadata.commitTimestampMs)));
        when(group.isSubscribedToTopic("firstTopic")).thenReturn(true);
        when(group.isSubscribedToTopic("secondTopic")).thenReturn(false);

        List<CoordinatorRecord> records = new ArrayList<>();
        assertFalse(context.cleanupExpiredOffsets("group-id", records));
        assertEquals(expectedRecords, records);

        // Expire secondTopic-1.
        context.time.sleep(500);

        records = new ArrayList<>();
        assertFalse(context.cleanupExpiredOffsets("group-id", records));
        verify(context.metrics, times(2)).record(OFFSET_EXPIRED_SENSOR_NAME, 1);

        // Add 2 more commits, then expire all.
        when(group.isSubscribedToTopic("firstTopic")).thenReturn(false);
        context.commitOffset("group-id", "firstTopic", 1, 100L, 0, commitTimestamp + 500);
        context.commitOffset("group-id", "secondTopic", 0, 101L, 0, commitTimestamp + 500);

        records = new ArrayList<>();
        assertTrue(context.cleanupExpiredOffsets("group-id", records));
        verify(context.metrics).record(OFFSET_EXPIRED_SENSOR_NAME, 3);
    }

    @Test
    public void testOffsetDeletionsSensor() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup("foo", true);

        context.commitOffset("foo", "bar", 0, 100L, 0);
        context.commitOffset("foo", "bar", 1, 150L, 0);
        group.setSubscribedTopics(Optional.of(Set.of()));

        OffsetDeleteRequestData.OffsetDeleteRequestTopicCollection requestTopicCollection =
            new OffsetDeleteRequestData.OffsetDeleteRequestTopicCollection(List.of(
                new OffsetDeleteRequestData.OffsetDeleteRequestTopic()
                    .setName("bar")
                    .setPartitions(Arrays.asList(
                        new OffsetDeleteRequestData.OffsetDeleteRequestPartition().setPartitionIndex(0),
                        new OffsetDeleteRequestData.OffsetDeleteRequestPartition().setPartitionIndex(1)
                    ))
            ).iterator());

        context.deleteOffsets(
            new OffsetDeleteRequestData()
                .setGroupId("foo")
                .setTopics(requestTopicCollection)
        );

        verify(context.metrics).record(OFFSET_DELETIONS_SENSOR_NAME, 2);
    }

    @Test
    public void testOnPartitionsDeleted() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Commit offsets.
        context.commitOffset("grp-0", "foo", 1, 100, 1, context.time.milliseconds());
        context.commitOffset("grp-0", "foo", 2, 200, 1, context.time.milliseconds());
        context.commitOffset("grp-0", "foo", 3, 300, 1, context.time.milliseconds());

        context.commitOffset("grp-1", "bar", 1, 100, 1, context.time.milliseconds());
        context.commitOffset("grp-1", "bar", 2, 200, 1, context.time.milliseconds());
        context.commitOffset("grp-1", "bar", 3, 300, 1, context.time.milliseconds());

        context.commitOffset(100L, "grp-2", "foo", 1, 100, 1, context.time.milliseconds());
        context.commitOffset(100L, "grp-2", "foo", 2, 200, 1, context.time.milliseconds());
        context.commitOffset(100L, "grp-2", "foo", 3, 300, 1, context.time.milliseconds());

        // Delete partitions.
        List<CoordinatorRecord> records = context.deletePartitions(Arrays.asList(
            new TopicPartition("foo", 1),
            new TopicPartition("foo", 2),
            new TopicPartition("foo", 3),
            new TopicPartition("bar", 1)
        ));

        // Verify.
        List<CoordinatorRecord> expectedRecords = Arrays.asList(
            GroupCoordinatorRecordHelpers.newOffsetCommitTombstoneRecord("grp-0", "foo", 1),
            GroupCoordinatorRecordHelpers.newOffsetCommitTombstoneRecord("grp-0", "foo", 2),
            GroupCoordinatorRecordHelpers.newOffsetCommitTombstoneRecord("grp-0", "foo", 3),
            GroupCoordinatorRecordHelpers.newOffsetCommitTombstoneRecord("grp-1", "bar", 1),
            GroupCoordinatorRecordHelpers.newOffsetCommitTombstoneRecord("grp-2", "foo", 1),
            GroupCoordinatorRecordHelpers.newOffsetCommitTombstoneRecord("grp-2", "foo", 2),
            GroupCoordinatorRecordHelpers.newOffsetCommitTombstoneRecord("grp-2", "foo", 3)
        );

        assertEquals(new HashSet<>(expectedRecords), new HashSet<>(records));

        assertFalse(context.hasOffset("grp-0", "foo", 1));
        assertFalse(context.hasOffset("grp-0", "foo", 2));
        assertFalse(context.hasOffset("grp-0", "foo", 3));
        assertFalse(context.hasOffset("grp-1", "bar", 1));
        assertFalse(context.hasOffset("grp-2", "foo", 1));
        assertFalse(context.hasOffset("grp-2", "foo", 2));
        assertFalse(context.hasOffset("grp-2", "foo", 3));
    }

    private void verifyReplay(
        OffsetMetadataManagerTestContext context,
        String groupId,
        String topic,
        int partition,
        OffsetAndMetadata offsetAndMetadata
    ) {
        context.replay(GroupCoordinatorRecordHelpers.newOffsetCommitRecord(
            groupId,
            topic,
            partition,
            offsetAndMetadata
        ));

        assertEquals(offsetAndMetadata, context.offsetMetadataManager.offset(
            groupId,
            topic,
            partition
        ));
    }

    private void verifyTransactionalReplay(
        OffsetMetadataManagerTestContext context,
        long producerId,
        String groupId,
        String topic,
        int partition,
        OffsetAndMetadata offsetAndMetadata
    ) {
        context.replay(producerId, GroupCoordinatorRecordHelpers.newOffsetCommitRecord(
            groupId,
            topic,
            partition,
            offsetAndMetadata
        ));

        assertEquals(offsetAndMetadata, context.offsetMetadataManager.pendingTransactionalOffset(
            producerId,
            groupId,
            topic,
            partition
        ));
    }

    private ClassicGroupMember mkGenericMember(
        String memberId,
        Optional<String> groupInstanceId
    ) {
        return new ClassicGroupMember(
            memberId,
            groupInstanceId,
            "client-id",
            "host",
            5000,
            5000,
            "consumer",
            new JoinGroupRequestData.JoinGroupRequestProtocolCollection(
                List.of(new JoinGroupRequestData.JoinGroupRequestProtocol()
                    .setName("range")
                    .setMetadata(new byte[0])
                ).iterator()
            )
        );
    }
}
