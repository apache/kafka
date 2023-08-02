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
import org.apache.kafka.common.errors.CoordinatorNotAvailableException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.IllegalGenerationException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.StaleMemberEpochException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.annotation.ApiKeyVersionsSource;
import org.apache.kafka.coordinator.group.assignor.RangeAssignor;
import org.apache.kafka.coordinator.group.consumer.ConsumerGroup;
import org.apache.kafka.coordinator.group.consumer.ConsumerGroupMember;
import org.apache.kafka.coordinator.group.generated.OffsetCommitKey;
import org.apache.kafka.coordinator.group.generated.OffsetCommitValue;
import org.apache.kafka.coordinator.group.generic.GenericGroup;
import org.apache.kafka.coordinator.group.generic.GenericGroupMember;
import org.apache.kafka.coordinator.group.generic.GenericGroupState;
import org.apache.kafka.coordinator.group.runtime.CoordinatorResult;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class OffsetMetadataManagerTest {
    static class OffsetMetadataManagerTestContext {
        public static class Builder {
            final private MockTime time = new MockTime();
            final private MockCoordinatorTimer<Void, Record> timer = new MockCoordinatorTimer<>(time);
            final private LogContext logContext = new LogContext();
            final private SnapshotRegistry snapshotRegistry = new SnapshotRegistry(logContext);
            private MetadataImage metadataImage = null;
            private int offsetMetadataMaxSize = 4096;

            Builder withOffsetMetadataMaxSize(int offsetMetadataMaxSize) {
                this.offsetMetadataMaxSize = offsetMetadataMaxSize;
                return this;
            }

            OffsetMetadataManagerTestContext build() {
                if (metadataImage == null) metadataImage = MetadataImage.EMPTY;

                GroupMetadataManager groupMetadataManager = new GroupMetadataManager.Builder()
                    .withTime(time)
                    .withTimer(timer)
                    .withSnapshotRegistry(snapshotRegistry)
                    .withLogContext(logContext)
                    .withMetadataImage(metadataImage)
                    .withConsumerGroupAssignors(Collections.singletonList(new RangeAssignor()))
                    .build();

                OffsetMetadataManager offsetMetadataManager = new OffsetMetadataManager.Builder()
                    .withTime(time)
                    .withLogContext(logContext)
                    .withSnapshotRegistry(snapshotRegistry)
                    .withMetadataImage(metadataImage)
                    .withGroupMetadataManager(groupMetadataManager)
                    .withOffsetMetadataMaxSize(offsetMetadataMaxSize)
                    .build();

                return new OffsetMetadataManagerTestContext(
                    time,
                    timer,
                    snapshotRegistry,
                    groupMetadataManager,
                    offsetMetadataManager
                );
            }
        }

        final MockTime time;
        final MockCoordinatorTimer<Void, Record> timer;
        final SnapshotRegistry snapshotRegistry;
        final GroupMetadataManager groupMetadataManager;
        final OffsetMetadataManager offsetMetadataManager;

        long lastCommittedOffset = 0L;
        long lastWrittenOffset = 0L;

        OffsetMetadataManagerTestContext(
            MockTime time,
            MockCoordinatorTimer<Void, Record> timer,
            SnapshotRegistry snapshotRegistry,
            GroupMetadataManager groupMetadataManager,
            OffsetMetadataManager offsetMetadataManager
        ) {
            this.time = time;
            this.timer = timer;
            this.snapshotRegistry = snapshotRegistry;
            this.groupMetadataManager = groupMetadataManager;
            this.offsetMetadataManager = offsetMetadataManager;
        }

        public CoordinatorResult<OffsetCommitResponseData, Record> commitOffset(
            OffsetCommitRequestData request
        ) {
            return commitOffset(ApiKeys.OFFSET_COMMIT.latestVersion(), request);
        }

        public CoordinatorResult<OffsetCommitResponseData, Record> commitOffset(
            short version,
            OffsetCommitRequestData request
        ) {
            snapshotRegistry.getOrCreateSnapshot(lastCommittedOffset);

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

            CoordinatorResult<OffsetCommitResponseData, Record> result = offsetMetadataManager.commitOffset(
                context,
                request
            );

            result.records().forEach(this::replay);
            return result;
        }

        public List<MockCoordinatorTimer.ExpiredTimeout<Void, Record>> sleep(long ms) {
            time.sleep(ms);
            List<MockCoordinatorTimer.ExpiredTimeout<Void, Record>> timeouts = timer.poll();
            timeouts.forEach(timeout -> {
                if (timeout.result.replayRecords()) {
                    timeout.result.records().forEach(this::replay);
                }
            });
            return timeouts;
        }

        private ApiMessage messageOrNull(ApiMessageAndVersion apiMessageAndVersion) {
            if (apiMessageAndVersion == null) {
                return null;
            } else {
                return apiMessageAndVersion.message();
            }
        }

        private void replay(
            Record record
        ) {
            ApiMessageAndVersion key = record.key();
            ApiMessageAndVersion value = record.value();

            if (key == null) {
                throw new IllegalStateException("Received a null key in " + record);
            }

            switch (key.version()) {
                case OffsetCommitKey.HIGHEST_SUPPORTED_VERSION:
                    offsetMetadataManager.replay(
                        (OffsetCommitKey) key.message(),
                        (OffsetCommitValue) messageOrNull(value)
                    );
                    break;

                default:
                    throw new IllegalStateException("Received an unknown record type " + key.version()
                        + " in " + record);
            }

            lastWrittenOffset++;
        }
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
                .setTopics(Collections.singletonList(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(Collections.singletonList(
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
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup(
            "foo",
            true
        );
        group.transitionTo(GenericGroupState.DEAD);

        // Verify that the request is rejected with the correct exception.
        assertThrows(CoordinatorNotAvailableException.class, () -> context.commitOffset(
            new OffsetCommitRequestData()
                .setGroupId("foo")
                .setMemberId("member")
                .setGenerationIdOrMemberEpoch(10)
                .setTopics(Collections.singletonList(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(Collections.singletonList(
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
        context.groupMetadataManager.getOrMaybeCreateGenericGroup(
            "foo",
            true
        );

        // Verify that the request is rejected with the correct exception.
        assertThrows(UnknownMemberIdException.class, () -> context.commitOffset(
            new OffsetCommitRequestData()
                .setGroupId("foo")
                .setMemberId("member")
                .setGenerationIdOrMemberEpoch(10)
                .setTopics(Collections.singletonList(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(Collections.singletonList(
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
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup(
            "foo",
            true
        );

        // Add member.
        group.add(mkGenericMember("member", Optional.of("new-instance-id")));

        // Transition to next generation.
        group.transitionTo(GenericGroupState.PREPARING_REBALANCE);
        group.initNextGeneration();
        assertEquals(1, group.generationId());

        // Verify that the request is rejected with the correct exception.
        assertThrows(IllegalGenerationException.class, () -> context.commitOffset(
            new OffsetCommitRequestData()
                .setGroupId("foo")
                .setMemberId("member")
                .setGenerationIdOrMemberEpoch(10)
                .setTopics(Collections.singletonList(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(Collections.singletonList(
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
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup(
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
                .setTopics(Collections.singletonList(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(Collections.singletonList(
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
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup(
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
                .setTopics(Collections.singletonList(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(Collections.singletonList(
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
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup(
            "foo",
            true
        );

        // Add member.
        group.add(mkGenericMember("member", Optional.of("new-instance-id")));

        // Transition to next generation.
        group.transitionTo(GenericGroupState.PREPARING_REBALANCE);
        group.initNextGeneration();
        assertEquals(1, group.generationId());

        // Verify that the request is rejected with the correct exception.
        assertThrows(RebalanceInProgressException.class, () -> context.commitOffset(
            new OffsetCommitRequestData()
                .setGroupId("foo")
                .setMemberId("member")
                .setGenerationIdOrMemberEpoch(1)
                .setTopics(Collections.singletonList(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(Collections.singletonList(
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
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup(
            "foo",
            true
        );

        // Add member.
        group.add(mkGenericMember("member", Optional.of("new-instance-id")));

        // Transition to next generation.
        group.transitionTo(GenericGroupState.PREPARING_REBALANCE);
        group.initNextGeneration();
        assertEquals(1, group.generationId());

        // Verify that the request is rejected with the correct exception.
        assertThrows(UnknownMemberIdException.class, () -> context.commitOffset(
            new OffsetCommitRequestData()
                .setGroupId("foo")
                .setTopics(Collections.singletonList(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(Collections.singletonList(
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
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup(
            "foo",
            true
        );

        // Add member.
        group.add(mkGenericMember("member", Optional.of("new-instance-id")));

        // Transition to next generation.
        group.transitionTo(GenericGroupState.PREPARING_REBALANCE);
        group.initNextGeneration();
        assertEquals(1, group.generationId());
        group.transitionTo(GenericGroupState.STABLE);

        CoordinatorResult<OffsetCommitResponseData, Record> result = context.commitOffset(
            new OffsetCommitRequestData()
                .setGroupId("foo")
                .setMemberId("member")
                .setGenerationIdOrMemberEpoch(1)
                .setRetentionTimeMs(1234L)
                .setTopics(Collections.singletonList(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(Collections.singletonList(
                            new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                        ))
                ))
        );

        assertEquals(
            new OffsetCommitResponseData()
                .setTopics(Collections.singletonList(
                    new OffsetCommitResponseData.OffsetCommitResponseTopic()
                        .setName("bar")
                        .setPartitions(Collections.singletonList(
                            new OffsetCommitResponseData.OffsetCommitResponsePartition()
                                .setPartitionIndex(0)
                                .setErrorCode(Errors.NONE.code())
                        ))
                )),
            result.response()
        );

        assertEquals(
            Collections.singletonList(RecordHelpers.newOffsetCommitRecord(
                "foo",
                "bar",
                0,
                new OffsetAndMetadata(
                    100L,
                    OptionalInt.empty(),
                    "",
                    context.time.milliseconds(),
                    OptionalLong.of(context.time.milliseconds() + 1234L)
                ),
                MetadataImage.EMPTY.features().metadataVersion()
            )),
            result.records()
        );
    }

    @Test
    public void testGenericGroupOffsetCommitMaintainsSession() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create a group.
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup(
            "foo",
            true
        );

        // Add member.
        GenericGroupMember member = mkGenericMember("member", Optional.empty());
        group.add(member);

        // Transition to next generation.
        group.transitionTo(GenericGroupState.PREPARING_REBALANCE);
        group.initNextGeneration();
        assertEquals(1, group.generationId());
        group.transitionTo(GenericGroupState.STABLE);

        // Schedule session timeout. This would be normally done when
        // the group transitions to stable.
        context.groupMetadataManager.rescheduleGenericGroupMemberHeartbeat(group, member);

        // Advance time by half of the session timeout. No timeouts are
        // expired.
        assertEquals(Collections.emptyList(), context.sleep(5000 / 2));

        // Commit.
        context.commitOffset(
            new OffsetCommitRequestData()
                .setGroupId("foo")
                .setMemberId("member")
                .setGenerationIdOrMemberEpoch(1)
                .setRetentionTimeMs(1234L)
                .setTopics(Collections.singletonList(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(Collections.singletonList(
                            new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                        ))
                ))
        );

        // Advance time by half of the session timeout. No timeouts are
        // expired.
        assertEquals(Collections.emptyList(), context.sleep(5000 / 2));

        // Advance time by half of the session timeout again. The timeout should
        // expire and the member is removed from the group.
        List<MockCoordinatorTimer.ExpiredTimeout<Void, Record>> timeouts =
            context.sleep(5000 / 2);
        assertEquals(1, timeouts.size());
        assertFalse(group.hasMemberId(member.memberId()));
    }

    @Test
    public void testSimpleGroupOffsetCommit() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        CoordinatorResult<OffsetCommitResponseData, Record> result = context.commitOffset(
            new OffsetCommitRequestData()
                .setGroupId("foo")
                .setTopics(Collections.singletonList(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(Collections.singletonList(
                            new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                        ))
                ))
        );

        assertEquals(
            new OffsetCommitResponseData()
                .setTopics(Collections.singletonList(
                    new OffsetCommitResponseData.OffsetCommitResponseTopic()
                        .setName("bar")
                        .setPartitions(Collections.singletonList(
                            new OffsetCommitResponseData.OffsetCommitResponsePartition()
                                .setPartitionIndex(0)
                                .setErrorCode(Errors.NONE.code())
                        ))
                )),
            result.response()
        );

        assertEquals(
            Collections.singletonList(RecordHelpers.newOffsetCommitRecord(
                "foo",
                "bar",
                0,
                new OffsetAndMetadata(
                    100L,
                    OptionalInt.empty(),
                    "",
                    context.time.milliseconds(),
                    OptionalLong.empty()
                ),
                MetadataImage.EMPTY.features().metadataVersion()
            )),
            result.records()
        );

        // A generic should have been created.
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup(
            "foo",
            false
        );
        assertNotNull(group);
        assertEquals("foo", group.groupId());
    }

    @Test
    public void testSimpleGroupOffsetCommitWithInstanceId() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        CoordinatorResult<OffsetCommitResponseData, Record> result = context.commitOffset(
            new OffsetCommitRequestData()
                .setGroupId("foo")
                // Instance id should be ignored.
                .setGroupInstanceId("instance-id")
                .setTopics(Collections.singletonList(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(Collections.singletonList(
                            new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                        ))
                ))
        );

        assertEquals(
            new OffsetCommitResponseData()
                .setTopics(Collections.singletonList(
                    new OffsetCommitResponseData.OffsetCommitResponseTopic()
                        .setName("bar")
                        .setPartitions(Collections.singletonList(
                            new OffsetCommitResponseData.OffsetCommitResponsePartition()
                                .setPartitionIndex(0)
                                .setErrorCode(Errors.NONE.code())
                        ))
                )),
            result.response()
        );

        assertEquals(
            Collections.singletonList(RecordHelpers.newOffsetCommitRecord(
                "foo",
                "bar",
                0,
                new OffsetAndMetadata(
                    100L,
                    OptionalInt.empty(),
                    "",
                    context.time.milliseconds(),
                    OptionalLong.empty()
                ),
                MetadataImage.EMPTY.features().metadataVersion()
            )),
            result.records()
        );
    }

    @Test
    public void testConsumerGroupOffsetCommitWithUnknownMemberId() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create an empty group.
        context.groupMetadataManager.getOrMaybeCreateConsumerGroup(
            "foo",
            true
        );

        // Verify that the request is rejected with the correct exception.
        assertThrows(UnknownMemberIdException.class, () -> context.commitOffset(
            new OffsetCommitRequestData()
                .setGroupId("foo")
                .setMemberId("member")
                .setGenerationIdOrMemberEpoch(10)
                .setTopics(Collections.singletonList(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(Collections.singletonList(
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
        ConsumerGroup group = context.groupMetadataManager.getOrMaybeCreateConsumerGroup(
            "foo",
            true
        );

        // Add member.
        group.updateMember(new ConsumerGroupMember.Builder("member")
            .setMemberEpoch(10)
            .setTargetMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .build()
        );

        OffsetCommitRequestData request = new OffsetCommitRequestData()
            .setGroupId("foo")
            .setMemberId("member")
            .setGenerationIdOrMemberEpoch(9)
            .setTopics(Collections.singletonList(
                new OffsetCommitRequestData.OffsetCommitRequestTopic()
                    .setName("bar")
                    .setPartitions(Collections.singletonList(
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

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_COMMIT)
    public void testConsumerGroupOffsetCommitWithVersionSmallerThanVersion9(short version) {
        // All the newer versions are fine.
        if (version >= 9) return;
        // Version 0 does not support MemberId and GenerationIdOrMemberEpoch fields.
        if (version == 0) return;

        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create an empty group.
        ConsumerGroup group = context.groupMetadataManager.getOrMaybeCreateConsumerGroup(
            "foo",
            true
        );

        // Add member.
        group.updateMember(new ConsumerGroupMember.Builder("member")
            .setMemberEpoch(10)
            .setTargetMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .build()
        );

        // Verify that the request is rejected with the correct exception.
        assertThrows(UnsupportedVersionException.class, () -> context.commitOffset(
            version,
            new OffsetCommitRequestData()
                .setGroupId("foo")
                .setMemberId("member")
                .setGenerationIdOrMemberEpoch(9)
                .setTopics(Collections.singletonList(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(Collections.singletonList(
                            new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                        ))
                ))
            )
        );
    }

    @Test
    public void testConsumerGroupOffsetCommitFromAdminClient() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create an empty group.
        context.groupMetadataManager.getOrMaybeCreateConsumerGroup(
            "foo",
            true
        );

        CoordinatorResult<OffsetCommitResponseData, Record> result = context.commitOffset(
            new OffsetCommitRequestData()
                .setGroupId("foo")
                .setTopics(Collections.singletonList(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(Collections.singletonList(
                            new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                        ))
                ))
        );

        assertEquals(
            new OffsetCommitResponseData()
                .setTopics(Collections.singletonList(
                    new OffsetCommitResponseData.OffsetCommitResponseTopic()
                        .setName("bar")
                        .setPartitions(Collections.singletonList(
                            new OffsetCommitResponseData.OffsetCommitResponsePartition()
                                .setPartitionIndex(0)
                                .setErrorCode(Errors.NONE.code())
                        ))
                )),
            result.response()
        );

        assertEquals(
            Collections.singletonList(RecordHelpers.newOffsetCommitRecord(
                "foo",
                "bar",
                0,
                new OffsetAndMetadata(
                    100L,
                    OptionalInt.empty(),
                    "",
                    context.time.milliseconds(),
                    OptionalLong.empty()
                ),
                MetadataImage.EMPTY.features().metadataVersion()
            )),
            result.records()
        );
    }

    @Test
    public void testConsumerGroupOffsetCommit() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create an empty group.
        ConsumerGroup group = context.groupMetadataManager.getOrMaybeCreateConsumerGroup(
            "foo",
            true
        );

        // Add member.
        group.updateMember(new ConsumerGroupMember.Builder("member")
            .setMemberEpoch(10)
            .setTargetMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .build()
        );

        CoordinatorResult<OffsetCommitResponseData, Record> result = context.commitOffset(
            new OffsetCommitRequestData()
                .setGroupId("foo")
                .setMemberId("member")
                .setGenerationIdOrMemberEpoch(10)
                .setTopics(Collections.singletonList(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(Collections.singletonList(
                            new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                                .setCommittedLeaderEpoch(10)
                                .setCommittedMetadata("metadata")
                                .setCommitTimestamp(context.time.milliseconds())
                        ))
                ))
        );

        assertEquals(
            new OffsetCommitResponseData()
                .setTopics(Collections.singletonList(
                    new OffsetCommitResponseData.OffsetCommitResponseTopic()
                        .setName("bar")
                        .setPartitions(Collections.singletonList(
                            new OffsetCommitResponseData.OffsetCommitResponsePartition()
                                .setPartitionIndex(0)
                                .setErrorCode(Errors.NONE.code())
                        ))
                )),
            result.response()
        );

        assertEquals(
            Collections.singletonList(RecordHelpers.newOffsetCommitRecord(
                "foo",
                "bar",
                0,
                new OffsetAndMetadata(
                    100L,
                    OptionalInt.of(10),
                    "metadata",
                    context.time.milliseconds(),
                    OptionalLong.empty()
                ),
                MetadataImage.EMPTY.features().metadataVersion()
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
        ConsumerGroup group = context.groupMetadataManager.getOrMaybeCreateConsumerGroup(
            "foo",
            true
        );

        // Add member.
        group.updateMember(new ConsumerGroupMember.Builder("member")
            .setMemberEpoch(10)
            .setTargetMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .build()
        );

        CoordinatorResult<OffsetCommitResponseData, Record> result = context.commitOffset(
            new OffsetCommitRequestData()
                .setGroupId("foo")
                .setMemberId("member")
                .setGenerationIdOrMemberEpoch(10)
                .setTopics(Collections.singletonList(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(Arrays.asList(
                            new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                                .setCommittedLeaderEpoch(10)
                                .setCommittedMetadata("toolarge")
                                .setCommitTimestamp(context.time.milliseconds()),
                            new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                .setPartitionIndex(1)
                                .setCommittedOffset(100L)
                                .setCommittedLeaderEpoch(10)
                                .setCommittedMetadata("small")
                                .setCommitTimestamp(context.time.milliseconds())
                        ))
                ))
        );

        assertEquals(
            new OffsetCommitResponseData()
                .setTopics(Collections.singletonList(
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
            Collections.singletonList(RecordHelpers.newOffsetCommitRecord(
                "foo",
                "bar",
                1,
                new OffsetAndMetadata(
                    100L,
                    OptionalInt.of(10),
                    "small",
                    context.time.milliseconds(),
                    OptionalLong.empty()
                ),
                MetadataImage.EMPTY.features().metadataVersion()
            )),
            result.records()
        );
    }

    @Test
    public void testReplay() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        verifyReplay(context, "foo", "bar", 0, new OffsetAndMetadata(
            100L,
            OptionalInt.empty(),
            "small",
            context.time.milliseconds(),
            OptionalLong.empty()
        ));

        verifyReplay(context, "foo", "bar", 0, new OffsetAndMetadata(
            200L,
            OptionalInt.of(10),
            "small",
            context.time.milliseconds(),
            OptionalLong.empty()
        ));

        verifyReplay(context, "foo", "bar", 1, new OffsetAndMetadata(
            200L,
            OptionalInt.of(10),
            "small",
            context.time.milliseconds(),
            OptionalLong.empty()
        ));

        verifyReplay(context, "foo", "bar", 1, new OffsetAndMetadata(
            300L,
            OptionalInt.of(10),
            "small",
            context.time.milliseconds(),
            OptionalLong.of(12345L)
        ));
    }

    @Test
    public void testReplayWithTombstone() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Verify replay adds the offset the map.
        verifyReplay(context, "foo", "bar", 0, new OffsetAndMetadata(
            100L,
            OptionalInt.empty(),
            "small",
            context.time.milliseconds(),
            OptionalLong.empty()
        ));

        // Create a tombstone record and replay it to delete the record.
        context.replay(RecordHelpers.newOffsetCommitTombstoneRecord(
            "foo",
            "bar",
            0
        ));

        // Verify that the offset is gone.
        assertNull(context.offsetMetadataManager.offset("foo", new TopicPartition("bar", 0)));
    }

    private void verifyReplay(
        OffsetMetadataManagerTestContext context,
        String groupId,
        String topic,
        int partition,
        OffsetAndMetadata offsetAndMetadata
    ) {
        context.replay(RecordHelpers.newOffsetCommitRecord(
            groupId,
            topic,
            partition,
            offsetAndMetadata,
            MetadataImage.EMPTY.features().metadataVersion()
        ));

        assertEquals(offsetAndMetadata, context.offsetMetadataManager.offset(
            groupId,
            new TopicPartition(topic, partition)
        ));
    }

    private GenericGroupMember mkGenericMember(
        String memberId,
        Optional<String> groupInstanceId
    ) {
        return new GenericGroupMember(
            memberId,
            groupInstanceId,
            "client-id",
            "host",
            5000,
            5000,
            "consumer",
            new JoinGroupRequestData.JoinGroupRequestProtocolCollection(
                Collections.singletonList(new JoinGroupRequestData.JoinGroupRequestProtocol()
                    .setName("range")
                    .setMetadata(new byte[0])
                ).iterator()
            )
        );
    }
}
