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

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.CoordinatorNotAvailableException;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.FencedMemberEpochException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupMaxSizeReachedException;
import org.apache.kafka.common.errors.IllegalGenerationException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.UnsupportedAssignorException;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.JoinGroupResponseData.JoinGroupResponseMember;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.SyncGroupRequestData.SyncGroupRequestAssignment;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.metadata.TopicRecord;
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
import org.apache.kafka.coordinator.group.MockCoordinatorTimer.ExpiredTimeout;
import org.apache.kafka.coordinator.group.MockCoordinatorTimer.ScheduledTimeout;
import org.apache.kafka.coordinator.group.assignor.AssignmentSpec;
import org.apache.kafka.coordinator.group.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.assignor.PartitionAssignor;
import org.apache.kafka.coordinator.group.assignor.PartitionAssignorException;
import org.apache.kafka.coordinator.group.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.consumer.Assignment;
import org.apache.kafka.coordinator.group.consumer.ConsumerGroup;
import org.apache.kafka.coordinator.group.consumer.ConsumerGroupMember;
import org.apache.kafka.coordinator.group.consumer.TopicMetadata;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMetadataValue;
import org.apache.kafka.coordinator.group.generated.GroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.GroupMetadataValue;
import org.apache.kafka.coordinator.group.generic.GenericGroup;
import org.apache.kafka.coordinator.group.generic.GenericGroupMember;
import org.apache.kafka.coordinator.group.generic.GenericGroupState;
import org.apache.kafka.coordinator.group.runtime.CoordinatorResult;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.TopicsImage;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocol;
import static org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocolCollection;
import static org.apache.kafka.common.requests.JoinGroupRequest.UNKNOWN_MEMBER_ID;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.apache.kafka.coordinator.group.GroupMetadataManager.appendGroupMetadataErrorToResponseError;
import static org.apache.kafka.coordinator.group.GroupMetadataManager.consumerGroupRevocationTimeoutKey;
import static org.apache.kafka.coordinator.group.GroupMetadataManager.consumerGroupSessionTimeoutKey;
import static org.apache.kafka.coordinator.group.GroupMetadataManager.EMPTY_RESULT;
import static org.apache.kafka.coordinator.group.GroupMetadataManager.genericGroupHeartbeatKey;
import static org.apache.kafka.coordinator.group.GroupMetadataManager.genericGroupSyncKey;
import static org.apache.kafka.coordinator.group.RecordHelpersTest.assertRecordEquals;
import static org.apache.kafka.coordinator.group.RecordHelpersTest.assertRecordsEquals;
import static org.apache.kafka.coordinator.group.RecordHelpersTest.mkMapOfPartitionRacks;
import static org.apache.kafka.coordinator.group.generic.GenericGroupState.COMPLETING_REBALANCE;
import static org.apache.kafka.coordinator.group.generic.GenericGroupState.DEAD;
import static org.apache.kafka.coordinator.group.generic.GenericGroupState.EMPTY;
import static org.apache.kafka.coordinator.group.generic.GenericGroupState.PREPARING_REBALANCE;
import static org.apache.kafka.coordinator.group.generic.GenericGroupState.STABLE;
import static org.junit.jupiter.api.AssertionFailureBuilder.assertionFailure;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GroupMetadataManagerTest {
    static class MockPartitionAssignor implements PartitionAssignor {
        private final String name;
        private GroupAssignment prepareGroupAssignment = null;

        MockPartitionAssignor(String name) {
            this.name = name;
        }

        public void prepareGroupAssignment(GroupAssignment prepareGroupAssignment) {
            this.prepareGroupAssignment = prepareGroupAssignment;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public GroupAssignment assign(AssignmentSpec assignmentSpec, SubscribedTopicDescriber subscribedTopicDescriber) throws PartitionAssignorException {
            return prepareGroupAssignment;
        }
    }

    public static class MetadataImageBuilder {
        private MetadataDelta delta = new MetadataDelta(MetadataImage.EMPTY);

        public MetadataImageBuilder addTopic(
            Uuid topicId,
            String topicName,
            int numPartitions
        ) {
            // For testing purposes, the following criteria are used:
            // - Number of replicas for each partition: 2
            // - Number of brokers available in the cluster: 4
            delta.replay(new TopicRecord().setTopicId(topicId).setName(topicName));
            for (int i = 0; i < numPartitions; i++) {
                delta.replay(new PartitionRecord()
                    .setTopicId(topicId)
                    .setPartitionId(i)
                    .setReplicas(Arrays.asList(i % 4, (i + 1) % 4)));
            }
            return this;
        }

        /**
         * Add rack Ids for 4 broker Ids.
         *
         * For testing purposes, each broker is mapped
         * to a rack Id with the same broker Id as a suffix.
         */
        public MetadataImageBuilder addRacks() {
            for (int i = 0; i < 4; i++) {
                delta.replay(new RegisterBrokerRecord().setBrokerId(i).setRack("rack" + i));
            }
            return this;
        }

        public MetadataImage build() {
            return delta.apply(MetadataProvenance.EMPTY);
        }
    }

    static class ConsumerGroupBuilder {
        private final String groupId;
        private final int groupEpoch;
        private int assignmentEpoch;
        private final Map<String, ConsumerGroupMember> members = new HashMap<>();
        private final Map<String, Assignment> assignments = new HashMap<>();
        private Map<String, TopicMetadata> subscriptionMetadata;

        public ConsumerGroupBuilder(String groupId, int groupEpoch) {
            this.groupId = groupId;
            this.groupEpoch = groupEpoch;
            this.assignmentEpoch = 0;
        }

        public ConsumerGroupBuilder withMember(ConsumerGroupMember member) {
            this.members.put(member.memberId(), member);
            return this;
        }

        public ConsumerGroupBuilder withSubscriptionMetadata(Map<String, TopicMetadata> subscriptionMetadata) {
            this.subscriptionMetadata = subscriptionMetadata;
            return this;
        }

        public ConsumerGroupBuilder withAssignment(String memberId, Map<Uuid, Set<Integer>> assignment) {
            this.assignments.put(memberId, new Assignment(assignment));
            return this;
        }

        public ConsumerGroupBuilder withAssignmentEpoch(int assignmentEpoch) {
            this.assignmentEpoch = assignmentEpoch;
            return this;
        }

        public List<Record> build(TopicsImage topicsImage) {
            List<Record> records = new ArrayList<>();

            // Add subscription records for members.
            members.forEach((memberId, member) -> {
                records.add(RecordHelpers.newMemberSubscriptionRecord(groupId, member));
            });

            // Add subscription metadata.
            if (subscriptionMetadata == null) {
                subscriptionMetadata = new HashMap<>();
                members.forEach((memberId, member) -> {
                    member.subscribedTopicNames().forEach(topicName -> {
                        TopicImage topicImage = topicsImage.getTopic(topicName);
                        if (topicImage != null) {
                            subscriptionMetadata.put(topicName, new TopicMetadata(
                                topicImage.id(),
                                topicImage.name(),
                                topicImage.partitions().size(),
                                Collections.emptyMap()));
                        }
                    });
                });
            }

            if (!subscriptionMetadata.isEmpty()) {
                records.add(RecordHelpers.newGroupSubscriptionMetadataRecord(groupId, subscriptionMetadata));
            }

            // Add group epoch record.
            records.add(RecordHelpers.newGroupEpochRecord(groupId, groupEpoch));

            // Add target assignment records.
            assignments.forEach((memberId, assignment) -> {
                records.add(RecordHelpers.newTargetAssignmentRecord(groupId, memberId, assignment.partitions()));
            });

            // Add target assignment epoch.
            records.add(RecordHelpers.newTargetAssignmentEpochRecord(groupId, assignmentEpoch));

            // Add current assignment records for members.
            members.forEach((memberId, member) -> {
                records.add(RecordHelpers.newCurrentAssignmentRecord(groupId, member));
            });

            return records;
        }
    }

    static class GroupMetadataManagerTestContext {
        static class Builder {
            final private MockTime time = new MockTime();
            final private MockCoordinatorTimer<Void, Record> timer = new MockCoordinatorTimer<>(time);
            final private LogContext logContext = new LogContext();
            final private SnapshotRegistry snapshotRegistry = new SnapshotRegistry(logContext);
            final private TopicPartition groupMetadataTopicPartition = new TopicPartition("topic", 0);
            private MetadataImage metadataImage;
            private List<PartitionAssignor> consumerGroupAssignors = Collections.singletonList(new MockPartitionAssignor("range"));
            private List<ConsumerGroupBuilder> consumerGroupBuilders = new ArrayList<>();
            private int consumerGroupMaxSize = Integer.MAX_VALUE;
            private int consumerGroupMetadataRefreshIntervalMs = Integer.MAX_VALUE;
            private int genericGroupMaxSize = Integer.MAX_VALUE;
            private int genericGroupInitialRebalanceDelayMs = 3000;
            final private int genericGroupNewMemberJoinTimeoutMs = 5 * 60 * 1000;
            private int genericGroupMinSessionTimeoutMs = 10;
            private int genericGroupMaxSessionTimeoutMs = 10 * 60 * 1000;

            public Builder withMetadataImage(MetadataImage metadataImage) {
                this.metadataImage = metadataImage;
                return this;
            }

            public Builder withAssignors(List<PartitionAssignor> assignors) {
                this.consumerGroupAssignors = assignors;
                return this;
            }

            public Builder withConsumerGroup(ConsumerGroupBuilder builder) {
                this.consumerGroupBuilders.add(builder);
                return this;
            }

            public Builder withConsumerGroupMaxSize(int consumerGroupMaxSize) {
                this.consumerGroupMaxSize = consumerGroupMaxSize;
                return this;
            }

            public Builder withConsumerGroupMetadataRefreshIntervalMs(int consumerGroupMetadataRefreshIntervalMs) {
                this.consumerGroupMetadataRefreshIntervalMs = consumerGroupMetadataRefreshIntervalMs;
                return this;
            }

            public Builder withGenericGroupMaxSize(int genericGroupMaxSize) {
                this.genericGroupMaxSize = genericGroupMaxSize;
                return this;
            }

            public Builder withGenericGroupInitialRebalanceDelayMs(int genericGroupInitialRebalanceDelayMs) {
                this.genericGroupInitialRebalanceDelayMs = genericGroupInitialRebalanceDelayMs;
                return this;
            }

            public Builder withGenericGroupMinSessionTimeoutMs(int genericGroupMinSessionTimeoutMs) {
                this.genericGroupMinSessionTimeoutMs = genericGroupMinSessionTimeoutMs;
                return this;
            }

            public Builder withGenericGroupMaxSessionTimeoutMs(int genericGroupMaxSessionTimeoutMs) {
                this.genericGroupMaxSessionTimeoutMs = genericGroupMaxSessionTimeoutMs;
                return this;
            }

            public GroupMetadataManagerTestContext build() {
                if (metadataImage == null) metadataImage = MetadataImage.EMPTY;
                if (consumerGroupAssignors == null) consumerGroupAssignors = Collections.emptyList();

                GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext(
                    time,
                    timer,
                    snapshotRegistry,
                    new GroupMetadataManager.Builder()
                        .withSnapshotRegistry(snapshotRegistry)
                        .withLogContext(logContext)
                        .withTime(time)
                        .withTimer(timer)
                        .withMetadataImage(metadataImage)
                        .withConsumerGroupHeartbeatInterval(5000)
                        .withConsumerGroupSessionTimeout(45000)
                        .withConsumerGroupMaxSize(consumerGroupMaxSize)
                        .withConsumerGroupAssignors(consumerGroupAssignors)
                        .withConsumerGroupMetadataRefreshIntervalMs(consumerGroupMetadataRefreshIntervalMs)
                        .withGenericGroupMaxSize(genericGroupMaxSize)
                        .withGenericGroupMinSessionTimeoutMs(genericGroupMinSessionTimeoutMs)
                        .withGenericGroupMaxSessionTimeoutMs(genericGroupMaxSessionTimeoutMs)
                        .withGenericGroupInitialRebalanceDelayMs(genericGroupInitialRebalanceDelayMs)
                        .withGenericGroupNewMemberJoinTimeoutMs(genericGroupNewMemberJoinTimeoutMs)
                        .build(),
                    genericGroupInitialRebalanceDelayMs,
                    genericGroupNewMemberJoinTimeoutMs
                );

                consumerGroupBuilders.forEach(builder -> {
                    builder.build(metadataImage.topics()).forEach(context::replay);
                });

                context.commit();

                return context;
            }
        }

        final MockTime time;
        final MockCoordinatorTimer<Void, Record> timer;
        final SnapshotRegistry snapshotRegistry;
        final GroupMetadataManager groupMetadataManager;
        final int genericGroupInitialRebalanceDelayMs;
        final int genericGroupNewMemberJoinTimeoutMs;

        long lastCommittedOffset = 0L;
        long lastWrittenOffset = 0L;

        public GroupMetadataManagerTestContext(
            MockTime time,
            MockCoordinatorTimer<Void, Record> timer,
            SnapshotRegistry snapshotRegistry,
            GroupMetadataManager groupMetadataManager,
            int genericGroupInitialRebalanceDelayMs,
            int genericGroupNewMemberJoinTimeoutMs
        ) {
            this.time = time;
            this.timer = timer;
            this.snapshotRegistry = snapshotRegistry;
            this.groupMetadataManager = groupMetadataManager;
            this.genericGroupInitialRebalanceDelayMs = genericGroupInitialRebalanceDelayMs;
            this.genericGroupNewMemberJoinTimeoutMs = genericGroupNewMemberJoinTimeoutMs;
        }

        public void commit() {
            long lastCommittedOffset = this.lastCommittedOffset;
            this.lastCommittedOffset = lastWrittenOffset;
            snapshotRegistry.deleteSnapshotsUpTo(lastCommittedOffset);
        }

        public void rollback() {
            lastWrittenOffset = lastCommittedOffset;
            snapshotRegistry.revertToSnapshot(lastCommittedOffset);
        }

        public ConsumerGroup.ConsumerGroupState consumerGroupState(
            String groupId
        ) {
            return groupMetadataManager
                .getOrMaybeCreateConsumerGroup(groupId, false)
                .state();
        }

        public ConsumerGroupMember.MemberState consumerGroupMemberState(
            String groupId,
            String memberId
        ) {
            return groupMetadataManager
                .getOrMaybeCreateConsumerGroup(groupId, false)
                .getOrMaybeCreateMember(memberId, false)
                .state();
        }

        public CoordinatorResult<ConsumerGroupHeartbeatResponseData, Record> consumerGroupHeartbeat(
            ConsumerGroupHeartbeatRequestData request
        ) {
            snapshotRegistry.getOrCreateSnapshot(lastCommittedOffset);

            RequestContext context = new RequestContext(
                new RequestHeader(
                    ApiKeys.CONSUMER_GROUP_HEARTBEAT,
                    ApiKeys.CONSUMER_GROUP_HEARTBEAT.latestVersion(),
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

            CoordinatorResult<ConsumerGroupHeartbeatResponseData, Record> result = groupMetadataManager.consumerGroupHeartbeat(
                context,
                request
            );

            result.records().forEach(this::replay);
            return result;
        }

        public List<ExpiredTimeout<Void, Record>> sleep(long ms) {
            time.sleep(ms);
            List<ExpiredTimeout<Void, Record>> timeouts = timer.poll();
            timeouts.forEach(timeout -> {
                if (timeout.result.replayRecords()) {
                    timeout.result.records().forEach(this::replay);
                }
            });
            return timeouts;
        }

        public ScheduledTimeout<Void, Record> assertSessionTimeout(
            String groupId,
            String memberId,
            long delayMs
        ) {
            ScheduledTimeout<Void, Record> timeout =
                timer.timeout(consumerGroupSessionTimeoutKey(groupId, memberId));
            assertNotNull(timeout);
            assertEquals(time.milliseconds() + delayMs, timeout.deadlineMs);
            return timeout;
        }

        public void assertNoSessionTimeout(
            String groupId,
            String memberId
        ) {
            ScheduledTimeout<Void, Record> timeout =
                timer.timeout(consumerGroupSessionTimeoutKey(groupId, memberId));
            assertNull(timeout);
        }

        public ScheduledTimeout<Void, Record> assertRevocationTimeout(
            String groupId,
            String memberId,
            long delayMs
        ) {
            ScheduledTimeout<Void, Record> timeout =
                timer.timeout(consumerGroupRevocationTimeoutKey(groupId, memberId));
            assertNotNull(timeout);
            assertEquals(time.milliseconds() + delayMs, timeout.deadlineMs);
            return timeout;
        }

        public void assertNoRevocationTimeout(
            String groupId,
            String memberId
        ) {
            ScheduledTimeout<Void, Record> timeout =
                timer.timeout(consumerGroupRevocationTimeoutKey(groupId, memberId));
            assertNull(timeout);
        }

        GenericGroup createGenericGroup(String groupId) {
            return groupMetadataManager.getOrMaybeCreateGenericGroup(groupId, true);
        }

        public CoordinatorResult<Void, Record> sendGenericGroupJoin(
            JoinGroupRequestData request,
            CompletableFuture<JoinGroupResponseData> responseFuture
        ) {
            return sendGenericGroupJoin(request, responseFuture, false);
        }

        public CoordinatorResult<Void, Record> sendGenericGroupJoin(
            JoinGroupRequestData request,
            CompletableFuture<JoinGroupResponseData> responseFuture,
            boolean requireKnownMemberId
        ) {
            return sendGenericGroupJoin(request, responseFuture, requireKnownMemberId, false);
        }

        public CoordinatorResult<Void, Record> sendGenericGroupJoin(
            JoinGroupRequestData request,
            CompletableFuture<JoinGroupResponseData> responseFuture,
            boolean requireKnownMemberId,
            boolean supportSkippingAssignment
        ) {
            // requireKnownMemberId is true: version >= 4 (See JoinGroupRequest#requiresKnownMemberId())
            // supportSkippingAssignment is true: version >= 9 (See JoinGroupRequest#supportsSkippingAssignment())
            short joinGroupVersion = 3;

            if (requireKnownMemberId) {
                joinGroupVersion = 4;
                if (supportSkippingAssignment) {
                    joinGroupVersion = ApiKeys.JOIN_GROUP.latestVersion();
                }
            }

            RequestContext context = new RequestContext(
                new RequestHeader(
                    ApiKeys.JOIN_GROUP,
                    joinGroupVersion,
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

            return groupMetadataManager.genericGroupJoin(
                context,
                request,
                responseFuture
            );
        }

        public JoinGroupResponseData joinGenericGroupAsDynamicMemberAndCompleteRebalance(
            String groupId
        ) throws Exception {
            GenericGroup group = createGenericGroup(groupId);

            JoinGroupResponseData leaderJoinResponse =
                joinGenericGroupAsDynamicMemberAndCompleteJoin(new JoinGroupRequestBuilder()
                    .withGroupId("group-id")
                    .withMemberId(UNKNOWN_MEMBER_ID)
                    .withDefaultProtocolTypeAndProtocols()
                    .withRebalanceTimeoutMs(10000)
                    .withSessionTimeoutMs(5000)
                    .build());

            assertEquals(1, leaderJoinResponse.generationId());
            assertTrue(group.isInState(COMPLETING_REBALANCE));

            SyncResult syncResult = sendGenericGroupSync(new SyncGroupRequestBuilder()
                .withGroupId("group-id")
                .withMemberId(leaderJoinResponse.memberId())
                .withGenerationId(leaderJoinResponse.generationId())
                .build());

            assertEquals(
                Collections.singletonList(newGroupMetadataRecord(group, MetadataVersion.latest())),
                syncResult.records
            );
            // Simulate a successful write to the log.
            syncResult.appendFuture.complete(null);

            assertTrue(syncResult.syncFuture.isDone());
            assertEquals(Errors.NONE.code(), syncResult.syncFuture.get().errorCode());
            assertTrue(group.isInState(STABLE));

            return leaderJoinResponse;
        }

        public JoinGroupResponseData joinGenericGroupAsDynamicMemberAndCompleteJoin(
            JoinGroupRequestData request
        ) throws ExecutionException, InterruptedException {
            boolean requireKnownMemberId = true;
            String newMemberId = request.memberId();

            if (request.memberId().equals(UNKNOWN_MEMBER_ID)) {
                // Since member id is required, we need another round to get the successful join group result.
                CompletableFuture<JoinGroupResponseData> firstJoinFuture = new CompletableFuture<>();
                CoordinatorResult<Void, Record> result = sendGenericGroupJoin(
                    request,
                    firstJoinFuture,
                    requireKnownMemberId
                );
                assertTrue(result.records().isEmpty());
                assertTrue(firstJoinFuture.isDone());
                assertEquals(Errors.MEMBER_ID_REQUIRED.code(), firstJoinFuture.get().errorCode());
                newMemberId = firstJoinFuture.get().memberId();
            }

            // Second round
            CompletableFuture<JoinGroupResponseData> secondJoinFuture = new CompletableFuture<>();
            JoinGroupRequestData secondRequest = new JoinGroupRequestData()
                .setGroupId(request.groupId())
                .setMemberId(newMemberId)
                .setProtocolType(request.protocolType())
                .setProtocols(request.protocols())
                .setSessionTimeoutMs(request.sessionTimeoutMs())
                .setRebalanceTimeoutMs(request.rebalanceTimeoutMs())
                .setReason(request.reason());

            CoordinatorResult<Void, Record> result = sendGenericGroupJoin(
                secondRequest,
                secondJoinFuture,
                requireKnownMemberId
            );

            assertTrue(result.records().isEmpty());
            List<ExpiredTimeout<Void, Record>> timeouts = sleep(genericGroupInitialRebalanceDelayMs);
            assertEquals(1, timeouts.size());
            assertTrue(secondJoinFuture.isDone());
            assertEquals(Errors.NONE.code(), secondJoinFuture.get().errorCode());
            return secondJoinFuture.get();
        }

        public JoinGroupResponseData joinGenericGroupAndCompleteJoin(
            JoinGroupRequestData request,
            boolean requireKnownMemberId,
            boolean supportSkippingAssignment
        ) throws ExecutionException, InterruptedException {
            return joinGenericGroupAndCompleteJoin(
                request,
                requireKnownMemberId,
                supportSkippingAssignment,
                genericGroupInitialRebalanceDelayMs
            );
        }

        public JoinGroupResponseData joinGenericGroupAndCompleteJoin(
            JoinGroupRequestData request,
            boolean requireKnownMemberId,
            boolean supportSkippingAssignment,
            int advanceClockMs
        ) throws ExecutionException, InterruptedException {
            if (requireKnownMemberId && request.groupInstanceId().isEmpty()) {
                return joinGenericGroupAsDynamicMemberAndCompleteJoin(request);
            }

            try {
                CompletableFuture<JoinGroupResponseData> responseFuture = new CompletableFuture<>();
                sendGenericGroupJoin(
                    request,
                    responseFuture,
                    requireKnownMemberId,
                    supportSkippingAssignment
                );

                sleep(advanceClockMs);
                assertTrue(responseFuture.isDone());
                assertEquals(Errors.NONE.code(), responseFuture.get().errorCode());
                return responseFuture.get();
            } catch (Exception e) {
                fail("Failed to due: " + e.getMessage());
            }
            return null;
        }

        public SyncResult sendGenericGroupSync(SyncGroupRequestData request) {
            RequestContext context = new RequestContext(
                new RequestHeader(
                    ApiKeys.SYNC_GROUP,
                    ApiKeys.SYNC_GROUP.latestVersion(),
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

            CompletableFuture<SyncGroupResponseData> responseFuture = new CompletableFuture<>();

            CoordinatorResult<Void, Record> coordinatorResult = groupMetadataManager.genericGroupSync(
                context,
                request,
                responseFuture
            );
            
            return new SyncResult(responseFuture, coordinatorResult);
        }


        public RebalanceResult staticMembersJoinAndRebalance(
            String groupId,
            String leaderInstanceId,
            String followerInstanceId
        ) throws Exception {
            return staticMembersJoinAndRebalance(
                groupId,
                leaderInstanceId,
                followerInstanceId,
                10000,
                5000
            );
        }

        public RebalanceResult staticMembersJoinAndRebalance(
            String groupId,
            String leaderInstanceId,
            String followerInstanceId,
            int rebalanceTimeoutMs,
            int sessionTimeoutMs
        ) throws Exception {
            GenericGroup group = createGenericGroup("group-id");

            JoinGroupRequestData joinRequest = new JoinGroupRequestBuilder()
                .withGroupId(groupId)
                .withGroupInstanceId(leaderInstanceId)
                .withMemberId(UNKNOWN_MEMBER_ID)
                .withProtocolType("consumer")
                .withProtocolSuperset()
                .withRebalanceTimeoutMs(rebalanceTimeoutMs)
                .withSessionTimeoutMs(sessionTimeoutMs)
                .build();

            CompletableFuture<JoinGroupResponseData> leaderJoinResponseFuture = new CompletableFuture<>();
            CoordinatorResult<Void, Record> result = sendGenericGroupJoin(joinRequest, leaderJoinResponseFuture);

            assertTrue(result.records().isEmpty());

            CompletableFuture<JoinGroupResponseData> followerJoinResponseFuture = new CompletableFuture<>();
            result = sendGenericGroupJoin(
                joinRequest
                    .setGroupInstanceId(followerInstanceId),
                followerJoinResponseFuture);

            assertTrue(result.records().isEmpty());
            assertFalse(leaderJoinResponseFuture.isDone());
            assertFalse(followerJoinResponseFuture.isDone());

            // The goal for two timer advance is to let first group initial join complete and set newMemberAdded flag to false. Next advance is
            // to trigger the rebalance as needed for follower delayed join. One large time advance won't help because we could only populate one
            // delayed join from purgatory and the new delayed op is created at that time and never be triggered.
            assertNoOrEmptyResult(sleep(genericGroupInitialRebalanceDelayMs));
            assertNoOrEmptyResult(sleep(genericGroupInitialRebalanceDelayMs));

            assertTrue(leaderJoinResponseFuture.isDone());
            assertTrue(followerJoinResponseFuture.isDone());
            assertEquals(Errors.NONE.code(), leaderJoinResponseFuture.get().errorCode());
            assertEquals(Errors.NONE.code(), followerJoinResponseFuture.get().errorCode());
            assertEquals(1, leaderJoinResponseFuture.get().generationId());
            assertEquals(1, followerJoinResponseFuture.get().generationId());
            assertEquals(2, group.size());
            assertEquals(1, group.generationId());
            assertTrue(group.isInState(COMPLETING_REBALANCE));

            String leaderId = leaderJoinResponseFuture.get().memberId();
            String followerId = followerJoinResponseFuture.get().memberId();
            List<SyncGroupRequestAssignment> assignment = new ArrayList<>();
            assignment.add(new SyncGroupRequestAssignment().setMemberId(leaderId)
                .setAssignment(new byte[]{1}));
            assignment.add(new SyncGroupRequestAssignment().setMemberId(followerId)
                .setAssignment(new byte[]{2}));

            SyncGroupRequestData syncRequest = new SyncGroupRequestBuilder()
                .withGroupId(groupId)
                .withGroupInstanceId(leaderInstanceId)
                .withMemberId(leaderId)
                .withGenerationId(1)
                .withAssignment(assignment)
                .build();

            SyncResult leaderSyncResult = sendGenericGroupSync(syncRequest);

            // The generated record should contain the new assignment.
            Map<String, byte[]> groupAssignment = assignment.stream().collect(Collectors.toMap(
                SyncGroupRequestAssignment::memberId, SyncGroupRequestAssignment::assignment
            ));
            assertEquals(
                Collections.singletonList(
                    RecordHelpers.newGroupMetadataRecord(group, groupAssignment, MetadataVersion.latest())),
                leaderSyncResult.records
            );

            // Simulate a successful write to the log.
            leaderSyncResult.appendFuture.complete(null);

            assertTrue(leaderSyncResult.syncFuture.isDone());
            assertEquals(Errors.NONE.code(), leaderSyncResult.syncFuture.get().errorCode());
            assertTrue(group.isInState(STABLE));

            SyncResult followerSyncResult = sendGenericGroupSync(
                syncRequest.setGroupInstanceId(followerInstanceId)
                    .setMemberId(followerId)
                    .setAssignments(Collections.emptyList())
            );

            assertTrue(followerSyncResult.records.isEmpty());
            assertTrue(followerSyncResult.syncFuture.isDone());
            assertEquals(Errors.NONE.code(), followerSyncResult.syncFuture.get().errorCode());
            assertTrue(group.isInState(STABLE));

            assertEquals(2, group.size());
            assertEquals(1, group.generationId());

            return new RebalanceResult(
                1,
                leaderId,
                leaderSyncResult.syncFuture.get().assignment(),
                followerId,
                followerSyncResult.syncFuture.get().assignment()
            );
        }

        public JoinGroupResponseData setupGroupWithPendingMember(GenericGroup group) throws Exception {
            // Add the first member
            JoinGroupRequestData joinRequest = new JoinGroupRequestBuilder()
                .withGroupId("group-id")
                .withMemberId(UNKNOWN_MEMBER_ID)
                .withDefaultProtocolTypeAndProtocols()
                .withRebalanceTimeoutMs(10000)
                .withSessionTimeoutMs(5000)
                .build();

            JoinGroupResponseData leaderJoinResponse =
                joinGenericGroupAsDynamicMemberAndCompleteJoin(joinRequest);

            List<SyncGroupRequestAssignment> assignment = new ArrayList<>();
            assignment.add(new SyncGroupRequestAssignment().setMemberId(leaderJoinResponse.memberId()));
            SyncGroupRequestData syncRequest = new SyncGroupRequestBuilder()
                .withGroupId("group-id")
                .withMemberId(leaderJoinResponse.memberId())
                .withGenerationId(leaderJoinResponse.generationId())
                .withAssignment(assignment)
                .build();

            SyncResult syncResult = sendGenericGroupSync(syncRequest);

            // Now the group is stable, with the one member that joined above
            assertEquals(
                Collections.singletonList(newGroupMetadataRecord(group, MetadataVersion.latest())),
                syncResult.records
            );
            // Simulate a successful write to log.
            syncResult.appendFuture.complete(null);

            assertTrue(syncResult.syncFuture.isDone());
            assertEquals(Errors.NONE.code(), syncResult.syncFuture.get().errorCode());

            // Start the join for the second member
            CompletableFuture<JoinGroupResponseData> followerJoinFuture = new CompletableFuture<>();
            CoordinatorResult<Void, Record> result = sendGenericGroupJoin(
                joinRequest
                    .setMemberId(UNKNOWN_MEMBER_ID),
                followerJoinFuture
            );

            assertTrue(result.records().isEmpty());
            assertFalse(followerJoinFuture.isDone());

            CompletableFuture<JoinGroupResponseData> leaderJoinFuture = new CompletableFuture<>();
            result = sendGenericGroupJoin(
                joinRequest
                    .setMemberId(leaderJoinResponse.memberId()),
                leaderJoinFuture);

            assertTrue(result.records().isEmpty());
            assertTrue(group.isInState(COMPLETING_REBALANCE));
            assertTrue(leaderJoinFuture.isDone());
            assertTrue(followerJoinFuture.isDone());
            assertEquals(Errors.NONE.code(), leaderJoinFuture.get().errorCode());
            assertEquals(Errors.NONE.code(), followerJoinFuture.get().errorCode());
            assertEquals(leaderJoinFuture.get().generationId(), followerJoinFuture.get().generationId());
            assertEquals(leaderJoinResponse.memberId(), leaderJoinFuture.get().leader());
            assertEquals(leaderJoinResponse.memberId(), followerJoinFuture.get().leader());

            int nextGenerationId = leaderJoinFuture.get().generationId();
            String followerId = followerJoinFuture.get().memberId();

            // Stabilize the group
            syncResult = sendGenericGroupSync(syncRequest.setGenerationId(nextGenerationId));

            assertEquals(
                Collections.singletonList(newGroupMetadataRecord(group, MetadataVersion.latest())),
                syncResult.records
            );
            // Simulate a successful write to log.
            syncResult.appendFuture.complete(null);

            assertTrue(syncResult.syncFuture.isDone());
            assertEquals(Errors.NONE.code(), syncResult.syncFuture.get().errorCode());
            assertTrue(group.isInState(STABLE));

            // Re-join an existing member, to transition the group to PreparingRebalance state.
            leaderJoinFuture = new CompletableFuture<>();
            result = sendGenericGroupJoin(
                joinRequest
                    .setMemberId(leaderJoinResponse.memberId()),
                leaderJoinFuture);

            assertTrue(result.records().isEmpty());
            assertFalse(leaderJoinFuture.isDone());
            assertTrue(group.isInState(PREPARING_REBALANCE));

            // Create a pending member in the group
            CompletableFuture<JoinGroupResponseData> pendingMemberJoinFuture = new CompletableFuture<>();
            result = sendGenericGroupJoin(
                joinRequest
                    .setMemberId(UNKNOWN_MEMBER_ID)
                    .setSessionTimeoutMs(2500),
                pendingMemberJoinFuture,
                true
            );

            assertTrue(result.records().isEmpty());
            assertTrue(pendingMemberJoinFuture.isDone());
            assertEquals(Errors.MEMBER_ID_REQUIRED.code(), pendingMemberJoinFuture.get().errorCode());
            assertEquals(1, group.numPendingJoinMembers());

            // Re-join the second existing member
            followerJoinFuture = new CompletableFuture<>();
            result = sendGenericGroupJoin(
                joinRequest.setMemberId(followerId).setSessionTimeoutMs(5000),
                followerJoinFuture
            );

            assertTrue(result.records().isEmpty());
            assertFalse(followerJoinFuture.isDone());
            assertTrue(group.isInState(PREPARING_REBALANCE));
            assertEquals(1, group.numPendingJoinMembers());

            return pendingMemberJoinFuture.get();
        }

        public void verifySessionExpiration(GenericGroup group, int timeoutMs) {
            Set<String> expectedHeartbeatKeys = group.allMembers().stream()
                .map(member -> genericGroupHeartbeatKey(group.groupId(), member.memberId())).collect(Collectors.toSet());

            // Member should be removed as session expires.
            List<ExpiredTimeout<Void, Record>> timeouts = sleep(timeoutMs);
            List<Record> expectedRecords = Collections.singletonList(newGroupMetadataRecord(
                group.groupId(),
                new GroupMetadataValue()
                    .setMembers(Collections.emptyList())
                    .setGeneration(group.generationId())
                    .setLeader(null)
                    .setProtocolType("consumer")
                    .setProtocol(null)
                    .setCurrentStateTimestamp(time.milliseconds()),
                MetadataVersion.latest()));


            Set<String> heartbeatKeys = timeouts.stream().map(timeout -> timeout.key).collect(Collectors.toSet());
            assertEquals(expectedHeartbeatKeys, heartbeatKeys);

            // Only the last member leaving the group should result in the empty group metadata record.
            int timeoutsSize = timeouts.size();
            assertEquals(expectedRecords, timeouts.get(timeoutsSize - 1).result.records());
            assertNoOrEmptyResult(timeouts.subList(0, timeoutsSize - 1));
            assertTrue(group.isInState(EMPTY));
            assertEquals(0, group.size());
        }

        public HeartbeatResponseData sendGenericGroupHeartbeat(
            HeartbeatRequestData request
        ) {
            RequestContext context = new RequestContext(
                new RequestHeader(
                    ApiKeys.HEARTBEAT,
                    ApiKeys.HEARTBEAT.latestVersion(),
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

            return groupMetadataManager.genericGroupHeartbeat(
                context,
                request
            );
        }

        public void verifyHeartbeat(
            String groupId,
            JoinGroupResponseData joinResponse,
            Errors expectedError
        ) {
            HeartbeatRequestData request = new HeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(joinResponse.memberId())
                .setGenerationId(joinResponse.generationId());

            if (expectedError == Errors.UNKNOWN_MEMBER_ID) {
                assertThrows(UnknownMemberIdException.class, () -> sendGenericGroupHeartbeat(request));
            } else {
                HeartbeatResponseData response = sendGenericGroupHeartbeat(request);
                assertEquals(expectedError.code(), response.errorCode());
            }
        }

        public List<JoinGroupResponseData> joinWithNMembers(
            String groupId,
            int numMembers,
            int rebalanceTimeoutMs,
            int sessionTimeoutMs
        ) {
            GenericGroup group = createGenericGroup(groupId);
            boolean requireKnownMemberId = true;

            // First join requests
            JoinGroupRequestData request = new JoinGroupRequestBuilder()
                .withGroupId(groupId)
                .withMemberId(UNKNOWN_MEMBER_ID)
                .withDefaultProtocolTypeAndProtocols()
                .withRebalanceTimeoutMs(rebalanceTimeoutMs)
                .withSessionTimeoutMs(sessionTimeoutMs)
                .build();

            List<CompletableFuture<JoinGroupResponseData>> joinFutures = IntStream.range(0, numMembers)
                .mapToObj(__ -> new CompletableFuture<JoinGroupResponseData>())
                .collect(Collectors.toList());

            IntStream.range(0, numMembers).forEach(i -> {
                CoordinatorResult<Void, Record> result = sendGenericGroupJoin(
                    request, joinFutures.get(i), requireKnownMemberId);

                assertTrue(result.records().isEmpty());
            });

            List<String> memberIds = joinFutures.stream().map(future -> {
                assertTrue(future.isDone());
                try {
                    return future.get().memberId();
                } catch (Exception e) {
                    fail("Unexpected exception: " + e.getMessage());
                }
                return null;
            }).collect(Collectors.toList());

            // Second join requests
            List<CompletableFuture<JoinGroupResponseData>> secondJoinFutures = IntStream.range(0, numMembers)
                .mapToObj(__ -> new CompletableFuture<JoinGroupResponseData>())
                .collect(Collectors.toList());

            IntStream.range(0, numMembers).forEach(i -> {
                CoordinatorResult<Void, Record> result = sendGenericGroupJoin(
                    request.setMemberId(memberIds.get(i)), secondJoinFutures.get(i), requireKnownMemberId);

                assertTrue(result.records().isEmpty());
            });
            secondJoinFutures.forEach(future -> assertFalse(future.isDone()));

            // Advance clock by initial rebalance delay.
            assertNoOrEmptyResult(sleep(genericGroupInitialRebalanceDelayMs));
            secondJoinFutures.forEach(future -> assertFalse(future.isDone()));
            // Advance clock by rebalance timeout to complete join phase.
            assertNoOrEmptyResult(sleep(rebalanceTimeoutMs));

            List<JoinGroupResponseData> joinResponses = secondJoinFutures.stream().map(future -> {
                assertTrue(future.isDone());
                try {
                    assertEquals(Errors.NONE.code(), future.get().errorCode());
                    return future.get();
                } catch (Exception e) {
                    fail("Unexpected exception: " + e.getMessage());
                }
                return null;
            }).collect(Collectors.toList());

            assertEquals(numMembers, group.size());
            assertTrue(group.isInState(COMPLETING_REBALANCE));

            return joinResponses;
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
                case GroupMetadataKey.HIGHEST_SUPPORTED_VERSION:
                    groupMetadataManager.replay(
                        (GroupMetadataKey) key.message(),
                        (GroupMetadataValue) messageOrNull(value)
                    );
                    break;

                case ConsumerGroupMemberMetadataKey.HIGHEST_SUPPORTED_VERSION:
                    groupMetadataManager.replay(
                        (ConsumerGroupMemberMetadataKey) key.message(),
                        (ConsumerGroupMemberMetadataValue) messageOrNull(value)
                    );
                    break;

                case ConsumerGroupMetadataKey.HIGHEST_SUPPORTED_VERSION:
                    groupMetadataManager.replay(
                        (ConsumerGroupMetadataKey) key.message(),
                        (ConsumerGroupMetadataValue) messageOrNull(value)
                    );
                    break;

                case ConsumerGroupPartitionMetadataKey.HIGHEST_SUPPORTED_VERSION:
                    groupMetadataManager.replay(
                        (ConsumerGroupPartitionMetadataKey) key.message(),
                        (ConsumerGroupPartitionMetadataValue) messageOrNull(value)
                    );
                    break;

                case ConsumerGroupTargetAssignmentMemberKey.HIGHEST_SUPPORTED_VERSION:
                    groupMetadataManager.replay(
                        (ConsumerGroupTargetAssignmentMemberKey) key.message(),
                        (ConsumerGroupTargetAssignmentMemberValue) messageOrNull(value)
                    );
                    break;

                case ConsumerGroupTargetAssignmentMetadataKey.HIGHEST_SUPPORTED_VERSION:
                    groupMetadataManager.replay(
                        (ConsumerGroupTargetAssignmentMetadataKey) key.message(),
                        (ConsumerGroupTargetAssignmentMetadataValue) messageOrNull(value)
                    );
                    break;

                case ConsumerGroupCurrentMemberAssignmentKey.HIGHEST_SUPPORTED_VERSION:
                    groupMetadataManager.replay(
                        (ConsumerGroupCurrentMemberAssignmentKey) key.message(),
                        (ConsumerGroupCurrentMemberAssignmentValue) messageOrNull(value)
                    );
                    break;

                default:
                    throw new IllegalStateException("Received an unknown record type " + key.version()
                        + " in " + record);
            }

            lastWrittenOffset++;
        }
    }

    @Test
    public void testConsumerHeartbeatRequestValidation() {
        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withAssignors(Collections.singletonList(assignor))
            .build();
        Exception ex;

        // GroupId must be present in all requests.
        ex = assertThrows(InvalidRequestException.class, () -> context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()));
        assertEquals("GroupId can't be empty.", ex.getMessage());

        // RebalanceTimeoutMs must be present in the first request (epoch == 0).
        ex = assertThrows(InvalidRequestException.class, () -> context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId("foo")
                .setMemberEpoch(0)));
        assertEquals("RebalanceTimeoutMs must be provided in first request.", ex.getMessage());

        // TopicPartitions must be present and empty in the first request (epoch == 0).
        ex = assertThrows(InvalidRequestException.class, () -> context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId("foo")
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(5000)));
        assertEquals("TopicPartitions must be empty when (re-)joining.", ex.getMessage());

        // SubscribedTopicNames must be present and empty in the first request (epoch == 0).
        ex = assertThrows(InvalidRequestException.class, () -> context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId("foo")
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(5000)
                .setTopicPartitions(Collections.emptyList())));
        assertEquals("SubscribedTopicNames must be set in first request.", ex.getMessage());

        // MemberId must be non-empty in all requests except for the first one where it
        // could be empty (epoch != 0).
        ex = assertThrows(InvalidRequestException.class, () -> context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId("foo")
                .setMemberEpoch(1)));
        assertEquals("MemberId can't be empty.", ex.getMessage());

        // InstanceId must be non-empty if provided in all requests.
        ex = assertThrows(InvalidRequestException.class, () -> context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId("foo")
                .setMemberId(Uuid.randomUuid().toString())
                .setMemberEpoch(1)
                .setInstanceId("")));
        assertEquals("InstanceId can't be empty.", ex.getMessage());

        // RackId must be non-empty if provided in all requests.
        ex = assertThrows(InvalidRequestException.class, () -> context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId("foo")
                .setMemberId(Uuid.randomUuid().toString())
                .setMemberEpoch(1)
                .setRackId("")));
        assertEquals("RackId can't be empty.", ex.getMessage());

        // ServerAssignor must exist if provided in all requests.
        ex = assertThrows(UnsupportedAssignorException.class, () -> context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId("foo")
                .setMemberId(Uuid.randomUuid().toString())
                .setMemberEpoch(1)
                .setServerAssignor("bar")));
        assertEquals("ServerAssignor bar is not supported. Supported assignors: range.", ex.getMessage());
    }

    @Test
    public void testMemberIdGeneration() {
        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withAssignors(Collections.singletonList(assignor))
            .withMetadataImage(MetadataImage.EMPTY)
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(
            Collections.emptyMap()
        ));

        CoordinatorResult<ConsumerGroupHeartbeatResponseData, Record> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId("group-foo")
                .setMemberEpoch(0)
                .setServerAssignor("range")
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
                .setTopicPartitions(Collections.emptyList()));

        // Verify that a member id was generated for the new member.
        String memberId = result.response().memberId();
        assertNotNull(memberId);
        assertNotEquals("", memberId);

        // The response should get a bumped epoch and should not
        // contain any assignment because we did not provide
        // topics metadata.
        assertEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(1)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()),
            result.response()
        );
    }

    @Test
    public void testUnknownGroupId() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withAssignors(Collections.singletonList(assignor))
            .build();

        assertThrows(GroupIdNotFoundException.class, () ->
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setMemberEpoch(100) // Epoch must be > 0.
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
                    .setTopicPartitions(Collections.emptyList())));
    }

    @Test
    public void testUnknownMemberIdJoinsConsumerGroup() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withAssignors(Collections.singletonList(assignor))
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(Collections.emptyMap()));

        // A first member joins to create the group.
        context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(0)
                .setServerAssignor("range")
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
                .setTopicPartitions(Collections.emptyList()));

        // The second member is rejected because the member id is unknown and
        // the member epoch is not zero.
        assertThrows(UnknownMemberIdException.class, () ->
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(Uuid.randomUuid().toString())
                    .setMemberEpoch(1)
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
                    .setTopicPartitions(Collections.emptyList())));
    }

    @Test
    public void testConsumerGroupMemberEpochValidation() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();
        Uuid fooTopicId = Uuid.randomUuid();

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withAssignors(Collections.singletonList(assignor))
            .build();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder(memberId)
            .setMemberEpoch(100)
            .setPreviousMemberEpoch(99)
            .setTargetMemberEpoch(100)
            .setRebalanceTimeoutMs(5000)
            .setClientId("client")
            .setClientHost("localhost/127.0.0.1")
            .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
            .setServerAssignorName("range")
            .setAssignedPartitions(mkAssignment(mkTopicAssignment(fooTopicId, 1, 2, 3)))
            .build();

        context.replay(RecordHelpers.newMemberSubscriptionRecord(groupId, member));

        context.replay(RecordHelpers.newGroupEpochRecord(groupId, 100));

        context.replay(RecordHelpers.newTargetAssignmentRecord(groupId, memberId, mkAssignment(
            mkTopicAssignment(fooTopicId, 1, 2, 3)
        )));

        context.replay(RecordHelpers.newTargetAssignmentEpochRecord(groupId, 100));

        context.replay(RecordHelpers.newCurrentAssignmentRecord(groupId, member));

        // Member epoch is greater than the expected epoch.
        assertThrows(FencedMemberEpochException.class, () ->
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setMemberEpoch(200)
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(Arrays.asList("foo", "bar"))));

        // Member epoch is smaller than the expected epoch.
        assertThrows(FencedMemberEpochException.class, () ->
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setMemberEpoch(50)
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(Arrays.asList("foo", "bar"))));

        // Member joins with previous epoch but without providing partitions.
        assertThrows(FencedMemberEpochException.class, () ->
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setMemberEpoch(99)
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(Arrays.asList("foo", "bar"))));

        // Member joins with previous epoch and has a subset of the owned partitions. This
        // is accepted as the response with the bumped epoch may have been lost. In this
        // case, we provide back the correct epoch to the member.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, Record> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(99)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
                .setTopicPartitions(Collections.singletonList(new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(fooTopicId)
                    .setPartitions(Arrays.asList(1, 2)))));
        assertEquals(100, result.response().memberEpoch());
    }

    @Test
    public void testMemberJoinsEmptyConsumerGroup() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withAssignors(Collections.singletonList(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addTopic(barTopicId, barTopicName, 3)
                .addRacks()
                .build())
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(
            Collections.singletonMap(memberId, new MemberAssignment(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5),
                mkTopicAssignment(barTopicId, 0, 1, 2)
            )))
        ));

        assertThrows(GroupIdNotFoundException.class, () ->
            context.groupMetadataManager.getOrMaybeCreateConsumerGroup(groupId, false));

        CoordinatorResult<ConsumerGroupHeartbeatResponseData, Record> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(0)
                .setServerAssignor("range")
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
                .setTopicPartitions(Collections.emptyList()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(1)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setAssignedTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(Arrays.asList(0, 1, 2, 3, 4, 5)),
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(barTopicId)
                            .setPartitions(Arrays.asList(0, 1, 2))
                    ))),
            result.response()
        );

        ConsumerGroupMember expectedMember = new ConsumerGroupMember.Builder(memberId)
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(0)
            .setTargetMemberEpoch(1)
            .setClientId("client")
            .setClientHost("localhost/127.0.0.1")
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
            .setServerAssignorName("range")
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5),
                mkTopicAssignment(barTopicId, 0, 1, 2)))
            .build();

        List<Record> expectedRecords = Arrays.asList(
            RecordHelpers.newMemberSubscriptionRecord(groupId, expectedMember),
            RecordHelpers.newGroupSubscriptionMetadataRecord(groupId, new HashMap<String, TopicMetadata>() {{
                    put(fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 6, mkMapOfPartitionRacks(6)));
                    put(barTopicName, new TopicMetadata(barTopicId, barTopicName, 3, mkMapOfPartitionRacks(3)));
                }}),
            RecordHelpers.newGroupEpochRecord(groupId, 1),
            RecordHelpers.newTargetAssignmentRecord(groupId, memberId, mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5),
                mkTopicAssignment(barTopicId, 0, 1, 2)
            )),
            RecordHelpers.newTargetAssignmentEpochRecord(groupId, 1),
            RecordHelpers.newCurrentAssignmentRecord(groupId, expectedMember)
        );

        assertRecordsEquals(expectedRecords, result.records());
    }

    @Test
    public void testUpdatingSubscriptionTriggersNewTargetAssignment() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withAssignors(Collections.singletonList(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addTopic(barTopicId, barTopicName, 3)
                .addRacks()
                .build())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setTargetMemberEpoch(10)
                    .setClientId("client")
                    .setClientHost("localhost/127.0.0.1")
                    .setSubscribedTopicNames(Collections.singletonList("foo"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)))
                    .build())
                .withAssignment(memberId, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)))
                .withAssignmentEpoch(10))
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(
            Collections.singletonMap(memberId, new MemberAssignment(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5),
                mkTopicAssignment(barTopicId, 0, 1, 2)
            )))
        ));

        CoordinatorResult<ConsumerGroupHeartbeatResponseData, Record> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(10)
                .setSubscribedTopicNames(Arrays.asList("foo", "bar")));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setAssignedTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(Arrays.asList(0, 1, 2, 3, 4, 5)),
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(barTopicId)
                            .setPartitions(Arrays.asList(0, 1, 2))
                    ))),
            result.response()
        );

        ConsumerGroupMember expectedMember = new ConsumerGroupMember.Builder(memberId)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(10)
            .setTargetMemberEpoch(11)
            .setClientId("client")
            .setClientHost("localhost/127.0.0.1")
            .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
            .setServerAssignorName("range")
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5),
                mkTopicAssignment(barTopicId, 0, 1, 2)))
            .build();

        List<Record> expectedRecords = Arrays.asList(
            RecordHelpers.newMemberSubscriptionRecord(groupId, expectedMember),
            RecordHelpers.newGroupSubscriptionMetadataRecord(groupId, new HashMap<String, TopicMetadata>() {
                {
                    put(fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 6, mkMapOfPartitionRacks(6)));
                    put(barTopicName, new TopicMetadata(barTopicId, barTopicName, 3, mkMapOfPartitionRacks(3)));
                }
            }),
            RecordHelpers.newGroupEpochRecord(groupId, 11),
            RecordHelpers.newTargetAssignmentRecord(groupId, memberId, mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5),
                mkTopicAssignment(barTopicId, 0, 1, 2)
            )),
            RecordHelpers.newTargetAssignmentEpochRecord(groupId, 11),
            RecordHelpers.newCurrentAssignmentRecord(groupId, expectedMember)
        );

        assertRecordsEquals(expectedRecords, result.records());
    }

    @Test
    public void testNewJoiningMemberTriggersNewTargetAssignment() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String memberId3 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withAssignors(Collections.singletonList(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addTopic(barTopicId, barTopicName, 3)
                .addRacks()
                .build())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setTargetMemberEpoch(10)
                    .setClientId("client")
                    .setClientHost("localhost/127.0.0.1")
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2),
                        mkTopicAssignment(barTopicId, 0, 1)))
                    .build())
                .withMember(new ConsumerGroupMember.Builder(memberId2)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setTargetMemberEpoch(10)
                    .setClientId("client")
                    .setClientHost("localhost/127.0.0.1")
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 3, 4, 5),
                        mkTopicAssignment(barTopicId, 2)))
                    .build())
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2),
                    mkTopicAssignment(barTopicId, 0, 1)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5),
                    mkTopicAssignment(barTopicId, 2)))
                .withAssignmentEpoch(10))
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(
            new HashMap<String, MemberAssignment>() {
                {
                    put(memberId1, new MemberAssignment(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1),
                        mkTopicAssignment(barTopicId, 0)
                    )));
                    put(memberId2, new MemberAssignment(mkAssignment(
                        mkTopicAssignment(fooTopicId, 2, 3),
                        mkTopicAssignment(barTopicId, 1)
                    )));
                    put(memberId3, new MemberAssignment(mkAssignment(
                        mkTopicAssignment(fooTopicId, 4, 5),
                        mkTopicAssignment(barTopicId, 2)
                    )));
                }
            }
        ));

        // Member 3 joins the consumer group.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, Record> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId3)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
                .setServerAssignor("range")
                .setTopicPartitions(Collections.emptyList()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId3)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setPendingTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(Arrays.asList(4, 5)),
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(barTopicId)
                            .setPartitions(Collections.singletonList(2))
                    ))),
            result.response()
        );

        ConsumerGroupMember expectedMember3 = new ConsumerGroupMember.Builder(memberId3)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(0)
            .setTargetMemberEpoch(11)
            .setClientId("client")
            .setClientHost("localhost/127.0.0.1")
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
            .setServerAssignorName("range")
            .setPartitionsPendingAssignment(mkAssignment(
                mkTopicAssignment(fooTopicId, 4, 5),
                mkTopicAssignment(barTopicId, 2)))
            .build();

        List<Record> expectedRecords = Arrays.asList(
            RecordHelpers.newMemberSubscriptionRecord(groupId, expectedMember3),
            RecordHelpers.newGroupSubscriptionMetadataRecord(groupId, new HashMap<String, TopicMetadata>() {
                {
                    put(fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 6, mkMapOfPartitionRacks(6)));
                    put(barTopicName, new TopicMetadata(barTopicId, barTopicName, 3, mkMapOfPartitionRacks(3)));
                }
            }),
            RecordHelpers.newGroupEpochRecord(groupId, 11),
            RecordHelpers.newTargetAssignmentRecord(groupId, memberId1, mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1),
                mkTopicAssignment(barTopicId, 0)
            )),
            RecordHelpers.newTargetAssignmentRecord(groupId, memberId2, mkAssignment(
                mkTopicAssignment(fooTopicId, 2, 3),
                mkTopicAssignment(barTopicId, 1)
            )),
            RecordHelpers.newTargetAssignmentRecord(groupId, memberId3, mkAssignment(
                mkTopicAssignment(fooTopicId, 4, 5),
                mkTopicAssignment(barTopicId, 2)
            )),
            RecordHelpers.newTargetAssignmentEpochRecord(groupId, 11),
            RecordHelpers.newCurrentAssignmentRecord(groupId, expectedMember3)
        );

        assertRecordsEquals(expectedRecords.subList(0, 3), result.records().subList(0, 3));
        assertUnorderedListEquals(expectedRecords.subList(3, 6), result.records().subList(3, 6));
        assertRecordsEquals(expectedRecords.subList(6, 8), result.records().subList(6, 8));
    }

    @Test
    public void testLeavingMemberBumpsGroupEpoch() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";
        Uuid zarTopicId = Uuid.randomUuid();
        String zarTopicName = "zar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");

        // Consumer group with two members.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withAssignors(Collections.singletonList(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addTopic(barTopicId, barTopicName, 3)
                .addTopic(zarTopicId, zarTopicName, 1)
                .addRacks()
                .build())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setTargetMemberEpoch(10)
                    .setClientId("client")
                    .setClientHost("localhost/127.0.0.1")
                    .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2),
                        mkTopicAssignment(barTopicId, 0, 1)))
                    .build())
                .withMember(new ConsumerGroupMember.Builder(memberId2)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setTargetMemberEpoch(10)
                    .setClientId("client")
                    .setClientHost("localhost/127.0.0.1")
                    // Use zar only here to ensure that metadata needs to be recomputed.
                    .setSubscribedTopicNames(Arrays.asList("foo", "bar", "zar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 3, 4, 5),
                        mkTopicAssignment(barTopicId, 2)))
                    .build())
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2),
                    mkTopicAssignment(barTopicId, 0, 1)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5),
                    mkTopicAssignment(barTopicId, 2)))
                .withAssignmentEpoch(10))
            .build();

        // Member 2 leaves the consumer group.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, Record> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setMemberEpoch(-1)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
                .setTopicPartitions(Collections.emptyList()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(-1),
            result.response()
        );

        List<Record> expectedRecords = Arrays.asList(
            RecordHelpers.newCurrentAssignmentTombstoneRecord(groupId, memberId2),
            RecordHelpers.newTargetAssignmentTombstoneRecord(groupId, memberId2),
            RecordHelpers.newMemberSubscriptionTombstoneRecord(groupId, memberId2),
            // Subscription metadata is recomputed because zar is no longer there.
            RecordHelpers.newGroupSubscriptionMetadataRecord(groupId, new HashMap<String, TopicMetadata>() {
                {
                    put(fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 6, mkMapOfPartitionRacks(6)));
                    put(barTopicName, new TopicMetadata(barTopicId, barTopicName, 3, mkMapOfPartitionRacks(3)));
                }
            }),
            RecordHelpers.newGroupEpochRecord(groupId, 11)
        );

        assertRecordsEquals(expectedRecords, result.records());
    }

    @Test
    public void testReconciliationProcess() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String memberId3 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        // Create a context with one consumer group containing two members.
        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withAssignors(Collections.singletonList(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addTopic(barTopicId, barTopicName, 3)
                .addRacks()
                .build())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setTargetMemberEpoch(10)
                    .setClientId("client")
                    .setClientHost("localhost/127.0.0.1")
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2),
                        mkTopicAssignment(barTopicId, 0, 1)))
                    .build())
                .withMember(new ConsumerGroupMember.Builder(memberId2)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setTargetMemberEpoch(10)
                    .setClientId("client")
                    .setClientHost("localhost/127.0.0.1")
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 3, 4, 5),
                        mkTopicAssignment(barTopicId, 2)))
                    .build())
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2),
                    mkTopicAssignment(barTopicId, 0, 1)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5),
                    mkTopicAssignment(barTopicId, 2)))
                .withAssignmentEpoch(10))
            .build();

        // Prepare new assignment for the group.
        assignor.prepareGroupAssignment(new GroupAssignment(
            new HashMap<String, MemberAssignment>() {
                {
                    put(memberId1, new MemberAssignment(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1),
                        mkTopicAssignment(barTopicId, 0)
                    )));
                    put(memberId2, new MemberAssignment(mkAssignment(
                        mkTopicAssignment(fooTopicId, 2, 3),
                        mkTopicAssignment(barTopicId, 2)
                    )));
                    put(memberId3, new MemberAssignment(mkAssignment(
                        mkTopicAssignment(fooTopicId, 4, 5),
                        mkTopicAssignment(barTopicId, 1)
                    )));
                }
            }
        ));

        CoordinatorResult<ConsumerGroupHeartbeatResponseData, Record> result;

        // Members in the group are in Stable state.
        assertEquals(ConsumerGroupMember.MemberState.STABLE, context.consumerGroupMemberState(groupId, memberId1));
        assertEquals(ConsumerGroupMember.MemberState.STABLE, context.consumerGroupMemberState(groupId, memberId2));
        assertEquals(ConsumerGroup.ConsumerGroupState.STABLE, context.consumerGroupState(groupId));

        // Member 3 joins the group. This triggers the computation of a new target assignment
        // for the group. Member 3 does not get any assigned partitions yet because they are
        // all owned by other members. However, it transitions to epoch 11 / Assigning state.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId3)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
                .setServerAssignor("range")
                .setTopicPartitions(Collections.emptyList()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId3)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setPendingTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(Arrays.asList(4, 5)),
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(barTopicId)
                            .setPartitions(Arrays.asList(1))
                    ))),
            result.response()
        );

        // We only check the last record as the subscription/target assignment updates are
        // already covered by other tests.
        assertRecordEquals(
            RecordHelpers.newCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberId3)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(0)
                .setTargetMemberEpoch(11)
                .setPartitionsPendingAssignment(mkAssignment(
                    mkTopicAssignment(fooTopicId, 4, 5),
                    mkTopicAssignment(barTopicId, 1)))
                .build()),
            result.records().get(result.records().size() - 1)
        );

        assertEquals(ConsumerGroupMember.MemberState.ASSIGNING, context.consumerGroupMemberState(groupId, memberId3));
        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, context.consumerGroupState(groupId));

        // Member 1 heartbeats. It remains at epoch 10 but transitions to Revoking state until
        // it acknowledges the revocation of its partitions. The response contains the new
        // assignment without the partitions that must be revoked.
        result = context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId1)
            .setMemberEpoch(10));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(10)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setAssignedTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(Arrays.asList(0, 1)),
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(barTopicId)
                            .setPartitions(Arrays.asList(0))
                    ))),
            result.response()
        );

        assertRecordsEquals(Collections.singletonList(
            RecordHelpers.newCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberId1)
                .setMemberEpoch(10)
                .setPreviousMemberEpoch(9)
                .setTargetMemberEpoch(11)
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1),
                    mkTopicAssignment(barTopicId, 0)))
                .setPartitionsPendingRevocation(mkAssignment(
                    mkTopicAssignment(fooTopicId, 2),
                    mkTopicAssignment(barTopicId, 1)))
                .build())),
            result.records()
        );

        assertEquals(ConsumerGroupMember.MemberState.REVOKING, context.consumerGroupMemberState(groupId, memberId1));
        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, context.consumerGroupState(groupId));

        // Member 2 heartbeats. It remains at epoch 10 but transitions to Revoking state until
        // it acknowledges the revocation of its partitions. The response contains the new
        // assignment without the partitions that must be revoked.
        result = context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId2)
            .setMemberEpoch(10));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(10)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setAssignedTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(Arrays.asList(3)),
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(barTopicId)
                            .setPartitions(Arrays.asList(2))
                    ))),
            result.response()
        );

        assertRecordsEquals(Collections.singletonList(
            RecordHelpers.newCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberId2)
                .setMemberEpoch(10)
                .setPreviousMemberEpoch(9)
                .setTargetMemberEpoch(11)
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(fooTopicId, 3),
                    mkTopicAssignment(barTopicId, 2)))
                .setPartitionsPendingRevocation(mkAssignment(
                    mkTopicAssignment(fooTopicId, 4, 5)))
                .setPartitionsPendingAssignment(mkAssignment(
                    mkTopicAssignment(fooTopicId, 2)))
                .build())),
            result.records()
        );

        assertEquals(ConsumerGroupMember.MemberState.REVOKING, context.consumerGroupMemberState(groupId, memberId2));
        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, context.consumerGroupState(groupId));

        // Member 3 heartbeats. The response does not contain any assignment
        // because the member is still waiting on other members to revoke partitions.
        result = context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId3)
            .setMemberEpoch(11));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId3)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000),
            result.response()
        );

        assertEquals(Collections.emptyList(), result.records());
        assertEquals(ConsumerGroupMember.MemberState.ASSIGNING, context.consumerGroupMemberState(groupId, memberId3));
        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, context.consumerGroupState(groupId));

        // Member 1 acknowledges the revocation of the partitions. It does so by providing the
        // partitions that it still owns in the request. This allows him to transition to epoch 11
        // and to the Stable state.
        result = context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId1)
            .setMemberEpoch(10)
            .setTopicPartitions(Arrays.asList(
                new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(fooTopicId)
                    .setPartitions(Arrays.asList(0, 1)),
                new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(barTopicId)
                    .setPartitions(Arrays.asList(0))
            )));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setAssignedTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(Arrays.asList(0, 1)),
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(barTopicId)
                            .setPartitions(Arrays.asList(0))
                    ))),
            result.response()
        );

        assertRecordsEquals(Collections.singletonList(
            RecordHelpers.newCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberId1)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(10)
                .setTargetMemberEpoch(11)
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1),
                    mkTopicAssignment(barTopicId, 0)))
                .build())),
            result.records()
        );

        assertEquals(ConsumerGroupMember.MemberState.STABLE, context.consumerGroupMemberState(groupId, memberId1));
        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, context.consumerGroupState(groupId));

        // Member 2 heartbeats but without acknowledging the revocation yet. This is basically a no-op.
        result = context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId2)
            .setMemberEpoch(10));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(10)
                .setHeartbeatIntervalMs(5000),
            result.response()
        );

        assertEquals(Collections.emptyList(), result.records());
        assertEquals(ConsumerGroupMember.MemberState.REVOKING, context.consumerGroupMemberState(groupId, memberId2));
        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, context.consumerGroupState(groupId));

        // Member 3 heartbeats. It receives the partitions revoked by member 1 but remains
        // in Assigning state because it still waits on other partitions.
        result = context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId3)
            .setMemberEpoch(11));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId3)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setAssignedTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(barTopicId)
                            .setPartitions(Arrays.asList(1))))
                    .setPendingTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(Arrays.asList(4, 5))))),
            result.response()
        );

        assertRecordsEquals(Collections.singletonList(
            RecordHelpers.newCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberId3)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(11)
                .setTargetMemberEpoch(11)
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(barTopicId, 1)))
                .setPartitionsPendingAssignment(mkAssignment(
                    mkTopicAssignment(fooTopicId, 4, 5)))
                .build())),
            result.records()
        );

        assertEquals(ConsumerGroupMember.MemberState.ASSIGNING, context.consumerGroupMemberState(groupId, memberId3));
        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, context.consumerGroupState(groupId));

        // Member 3 heartbeats. Member 2 has not acknowledged the revocation of its partition so
        // member keeps its current assignment.
        result = context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId3)
            .setMemberEpoch(11));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId3)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000),
            result.response()
        );

        assertEquals(Collections.emptyList(), result.records());
        assertEquals(ConsumerGroupMember.MemberState.ASSIGNING, context.consumerGroupMemberState(groupId, memberId3));
        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, context.consumerGroupState(groupId));

        // Member 2 acknowledges the revocation of the partitions. It does so by providing the
        // partitions that it still owns in the request. This allows him to transition to epoch 11
        // and to the Stable state.
        result = context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId2)
            .setMemberEpoch(10)
            .setTopicPartitions(Arrays.asList(
                new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(fooTopicId)
                    .setPartitions(Arrays.asList(3)),
                new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(barTopicId)
                    .setPartitions(Arrays.asList(2))
            )));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setAssignedTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(Arrays.asList(2, 3)),
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(barTopicId)
                            .setPartitions(Arrays.asList(2))
                    ))),
            result.response()
        );

        assertRecordsEquals(Collections.singletonList(
            RecordHelpers.newCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberId2)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(10)
                .setTargetMemberEpoch(11)
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(fooTopicId, 2, 3),
                    mkTopicAssignment(barTopicId, 2)))
                .build())),
            result.records()
        );

        assertEquals(ConsumerGroupMember.MemberState.STABLE, context.consumerGroupMemberState(groupId, memberId2));
        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, context.consumerGroupState(groupId));

        // Member 3 heartbeats. It receives all its partitions and transitions to Stable.
        result = context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId3)
            .setMemberEpoch(11));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId3)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setAssignedTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(Arrays.asList(4, 5)),
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(barTopicId)
                            .setPartitions(Arrays.asList(1))))),
            result.response()
        );

        assertRecordsEquals(Collections.singletonList(
            RecordHelpers.newCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberId3)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(11)
                .setTargetMemberEpoch(11)
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(fooTopicId, 4, 5),
                    mkTopicAssignment(barTopicId, 1)))
                .build())),
            result.records()
        );

        assertEquals(ConsumerGroupMember.MemberState.STABLE, context.consumerGroupMemberState(groupId, memberId3));
        assertEquals(ConsumerGroup.ConsumerGroupState.STABLE, context.consumerGroupState(groupId));
    }

    @Test
    public void testReconciliationRestartsWhenNewTargetAssignmentIsInstalled() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String memberId3 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        // Create a context with one consumer group containing one member.
        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withAssignors(Collections.singletonList(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addRacks()
                .build())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setTargetMemberEpoch(10)
                    .setClientId("client")
                    .setClientHost("localhost/127.0.0.1")
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2)))
                    .build())
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2)))
                .withAssignmentEpoch(10))
            .build();

        CoordinatorResult<ConsumerGroupHeartbeatResponseData, Record> result;

        // Prepare new assignment for the group.
        assignor.prepareGroupAssignment(new GroupAssignment(
            new HashMap<String, MemberAssignment>() {
                {
                    put(memberId1, new MemberAssignment(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1)
                    )));
                    put(memberId2, new MemberAssignment(mkAssignment(
                        mkTopicAssignment(fooTopicId, 2)
                    )));
                }
            }
        ));

        // Member 2 joins.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
                .setServerAssignor("range")
                .setTopicPartitions(Collections.emptyList()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setPendingTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(Arrays.asList(2))
                    ))),
            result.response()
        );

        assertRecordEquals(
            RecordHelpers.newCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberId2)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(0)
                .setTargetMemberEpoch(11)
                .setPartitionsPendingAssignment(mkAssignment(
                    mkTopicAssignment(fooTopicId, 2)))
                .build()),
            result.records().get(result.records().size() - 1)
        );

        assertEquals(ConsumerGroupMember.MemberState.ASSIGNING, context.consumerGroupMemberState(groupId, memberId2));

        // Member 1 heartbeats and transitions to Revoking.
        result = context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId1)
            .setMemberEpoch(10));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(10)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setAssignedTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(Arrays.asList(0, 1))))),
            result.response()
        );

        assertRecordsEquals(Collections.singletonList(
            RecordHelpers.newCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberId1)
                .setMemberEpoch(10)
                .setPreviousMemberEpoch(9)
                .setTargetMemberEpoch(11)
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1)))
                .setPartitionsPendingRevocation(mkAssignment(
                    mkTopicAssignment(fooTopicId, 2)))
                .build())),
            result.records()
        );

        assertEquals(ConsumerGroupMember.MemberState.REVOKING, context.consumerGroupMemberState(groupId, memberId1));

        // Prepare new assignment for the group.
        assignor.prepareGroupAssignment(new GroupAssignment(
            new HashMap<String, MemberAssignment>() {
                {
                    put(memberId1, new MemberAssignment(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0)
                    )));
                    put(memberId2, new MemberAssignment(mkAssignment(
                        mkTopicAssignment(fooTopicId, 2)
                    )));
                    put(memberId3, new MemberAssignment(mkAssignment(
                        mkTopicAssignment(fooTopicId, 1)
                    )));
                }
            }
        ));

        // Member 3 joins.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId3)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
                .setServerAssignor("range")
                .setTopicPartitions(Collections.emptyList()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId3)
                .setMemberEpoch(12)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setPendingTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(Arrays.asList(1))
                    ))),
            result.response()
        );

        assertRecordEquals(
            RecordHelpers.newCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberId3)
                .setMemberEpoch(12)
                .setPreviousMemberEpoch(0)
                .setTargetMemberEpoch(12)
                .setPartitionsPendingAssignment(mkAssignment(
                    mkTopicAssignment(fooTopicId, 1)))
                .build()),
            result.records().get(result.records().size() - 1)
        );

        assertEquals(ConsumerGroupMember.MemberState.ASSIGNING, context.consumerGroupMemberState(groupId, memberId3));

        // When member 1 heartbeats, it transitions to Revoke again but an updated state.
        result = context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId1)
            .setMemberEpoch(10));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(10)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setAssignedTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(Arrays.asList(0))))),
            result.response()
        );

        assertRecordsEquals(Collections.singletonList(
                RecordHelpers.newCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberId1)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setTargetMemberEpoch(12)
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0)))
                    .setPartitionsPendingRevocation(mkAssignment(
                        mkTopicAssignment(fooTopicId, 1, 2)))
                    .build())),
            result.records()
        );

        assertEquals(ConsumerGroupMember.MemberState.REVOKING, context.consumerGroupMemberState(groupId, memberId1));

        // When member 2 heartbeats, it transitions to Assign again but with an updated state.
        result = context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId2)
            .setMemberEpoch(11));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(12)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setPendingTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(Arrays.asList(2))))),
            result.response()
        );

        assertRecordsEquals(Collections.singletonList(
            RecordHelpers.newCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberId2)
                .setMemberEpoch(12)
                .setPreviousMemberEpoch(11)
                .setTargetMemberEpoch(12)
                .setPartitionsPendingAssignment(mkAssignment(
                    mkTopicAssignment(fooTopicId, 2)))
                .build())),
            result.records()
        );

        assertEquals(ConsumerGroupMember.MemberState.ASSIGNING, context.consumerGroupMemberState(groupId, memberId2));
    }

    @Test
    public void testNewMemberIsRejectedWithMaximumMembersIsReached() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String memberId3 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        // Create a context with one consumer group containing two members.
        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withAssignors(Collections.singletonList(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addTopic(barTopicId, barTopicName, 3)
                .build())
            .withConsumerGroupMaxSize(2)
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setTargetMemberEpoch(10)
                    .setClientId("client")
                    .setClientHost("localhost/127.0.0.1")
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2),
                        mkTopicAssignment(barTopicId, 0, 1)))
                    .build())
                .withMember(new ConsumerGroupMember.Builder(memberId2)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setTargetMemberEpoch(10)
                    .setClientId("client")
                    .setClientHost("localhost/127.0.0.1")
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 3, 4, 5),
                        mkTopicAssignment(barTopicId, 2)))
                    .build())
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2),
                    mkTopicAssignment(barTopicId, 0, 1)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5),
                    mkTopicAssignment(barTopicId, 2)))
                .withAssignmentEpoch(10))
            .build();

        assertThrows(GroupMaxSizeReachedException.class, () ->
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId3)
                    .setMemberEpoch(0)
                    .setServerAssignor("range")
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
                    .setTopicPartitions(Collections.emptyList())));
    }

    @Test
    public void testConsumerGroupStates() {
        String groupId = "fooup";
        String memberId1 = Uuid.randomUuid().toString();
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withAssignors(Collections.singletonList(assignor))
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10))
            .build();

        assertEquals(ConsumerGroup.ConsumerGroupState.EMPTY, context.consumerGroupState(groupId));

        context.replay(RecordHelpers.newMemberSubscriptionRecord(groupId, new ConsumerGroupMember.Builder(memberId1)
            .setSubscribedTopicNames(Collections.singletonList(fooTopicName))
            .build()));
        context.replay(RecordHelpers.newGroupEpochRecord(groupId, 11));

        assertEquals(ConsumerGroup.ConsumerGroupState.ASSIGNING, context.consumerGroupState(groupId));

        context.replay(RecordHelpers.newTargetAssignmentRecord(groupId, memberId1, mkAssignment(
            mkTopicAssignment(fooTopicId, 1, 2, 3))));
        context.replay(RecordHelpers.newTargetAssignmentEpochRecord(groupId, 11));

        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, context.consumerGroupState(groupId));

        context.replay(RecordHelpers.newCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberId1)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(10)
            .setTargetMemberEpoch(11)
            .setAssignedPartitions(mkAssignment(mkTopicAssignment(fooTopicId, 1, 2)))
            .setPartitionsPendingAssignment(mkAssignment(mkTopicAssignment(fooTopicId, 3)))
            .build()));

        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, context.consumerGroupState(groupId));

        context.replay(RecordHelpers.newCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberId1)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(10)
            .setTargetMemberEpoch(11)
            .setAssignedPartitions(mkAssignment(mkTopicAssignment(fooTopicId, 1, 2, 3)))
            .build()));

        assertEquals(ConsumerGroup.ConsumerGroupState.STABLE, context.consumerGroupState(groupId));
    }

    @Test
    public void testPartitionAssignorExceptionOnRegularHeartbeat() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        PartitionAssignor assignor = mock(PartitionAssignor.class);
        when(assignor.name()).thenReturn("range");
        when(assignor.assign(any(), any())).thenThrow(new PartitionAssignorException("Assignment failed."));

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withAssignors(Collections.singletonList(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addTopic(barTopicId, barTopicName, 3)
                .addRacks()
                .build())
            .build();

        // Member 1 joins the consumer group. The request fails because the
        // target assignment computation failed.
        assertThrows(UnknownServerException.class, () ->
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId1)
                    .setMemberEpoch(0)
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
                    .setServerAssignor("range")
                    .setTopicPartitions(Collections.emptyList())));
    }

    @Test
    public void testSubscriptionMetadataRefreshedAfterGroupIsLoaded() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        // Create a context with one consumer group containing one member.
        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withAssignors(Collections.singletonList(assignor))
            .withConsumerGroupMetadataRefreshIntervalMs(5 * 60 * 1000)
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addRacks()
                .build())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setTargetMemberEpoch(10)
                    .setClientId("client")
                    .setClientHost("localhost/127.0.0.1")
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2)))
                    .build())
                .withAssignment(memberId, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2)))
                .withAssignmentEpoch(10)
                .withSubscriptionMetadata(new HashMap<String, TopicMetadata>() {
                    {
                        // foo only has 3 partitions stored in the metadata but foo has
                        // 6 partitions the metadata image.
                        put(fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 3, mkMapOfPartitionRacks(3)));
                    }
                }))
            .build();

        // The metadata refresh flag should be true.
        ConsumerGroup consumerGroup = context.groupMetadataManager
            .getOrMaybeCreateConsumerGroup(groupId, false);
        assertTrue(consumerGroup.hasMetadataExpired(context.time.milliseconds()));

        // Prepare the assignment result.
        assignor.prepareGroupAssignment(new GroupAssignment(
            Collections.singletonMap(memberId, new MemberAssignment(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)
            )))
        ));

        // Heartbeat.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, Record> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(10));

        // The member gets partitions 3, 4 and 5 assigned.
        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setAssignedTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(Arrays.asList(0, 1, 2, 3, 4, 5))
                    ))),
            result.response()
        );

        ConsumerGroupMember expectedMember = new ConsumerGroupMember.Builder(memberId)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(10)
            .setTargetMemberEpoch(11)
            .setClientId("client")
            .setClientHost("localhost/127.0.0.1")
            .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
            .setServerAssignorName("range")
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)))
            .build();

        List<Record> expectedRecords = Arrays.asList(
            RecordHelpers.newGroupSubscriptionMetadataRecord(groupId, new HashMap<String, TopicMetadata>() {
                {
                    put(fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 6, mkMapOfPartitionRacks(6)));
                }
            }),
            RecordHelpers.newGroupEpochRecord(groupId, 11),
            RecordHelpers.newTargetAssignmentRecord(groupId, memberId, mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)
            )),
            RecordHelpers.newTargetAssignmentEpochRecord(groupId, 11),
            RecordHelpers.newCurrentAssignmentRecord(groupId, expectedMember)
        );

        assertRecordsEquals(expectedRecords, result.records());

        // Check next refresh time.
        assertFalse(consumerGroup.hasMetadataExpired(context.time.milliseconds()));
        assertEquals(context.time.milliseconds() + 5 * 60 * 1000, consumerGroup.metadataRefreshDeadline().deadlineMs);
        assertEquals(11, consumerGroup.metadataRefreshDeadline().epoch);
    }

    @Test
    public void testSubscriptionMetadataRefreshedAgainAfterWriteFailure() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        // Create a context with one consumer group containing one member.
        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withAssignors(Collections.singletonList(assignor))
            .withConsumerGroupMetadataRefreshIntervalMs(5 * 60 * 1000)
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addRacks()
                .build())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setTargetMemberEpoch(10)
                    .setClientId("client")
                    .setClientHost("localhost/127.0.0.1")
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2)))
                    .build())
                .withAssignment(memberId, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2)))
                .withAssignmentEpoch(10)
                .withSubscriptionMetadata(new HashMap<String, TopicMetadata>() {
                    {
                        // foo only has 3 partitions stored in the metadata but foo has
                        // 6 partitions the metadata image.
                        put(fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 3, mkMapOfPartitionRacks(3)));
                    }
                }))
            .build();

        // The metadata refresh flag should be true.
        ConsumerGroup consumerGroup = context.groupMetadataManager
            .getOrMaybeCreateConsumerGroup(groupId, false);
        assertTrue(consumerGroup.hasMetadataExpired(context.time.milliseconds()));

        // Prepare the assignment result.
        assignor.prepareGroupAssignment(new GroupAssignment(
            Collections.singletonMap(memberId, new MemberAssignment(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)
            )))
        ));

        // Heartbeat.
        context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(10));

        // The metadata refresh flag is set to a future time.
        assertFalse(consumerGroup.hasMetadataExpired(context.time.milliseconds()));
        assertEquals(context.time.milliseconds() + 5 * 60 * 1000, consumerGroup.metadataRefreshDeadline().deadlineMs);
        assertEquals(11, consumerGroup.metadataRefreshDeadline().epoch);

        // Rollback the uncommitted changes. This does not rollback the metadata flag
        // because it is not using a timeline data structure.
        context.rollback();

        // However, the next heartbeat should detect the divergence based on the epoch and trigger
        // a metadata refresh.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, Record> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(10));


        // The member gets partitions 3, 4 and 5 assigned.
        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setAssignedTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(Arrays.asList(0, 1, 2, 3, 4, 5))
                    ))),
            result.response()
        );

        ConsumerGroupMember expectedMember = new ConsumerGroupMember.Builder(memberId)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(10)
            .setTargetMemberEpoch(11)
            .setClientId("client")
            .setClientHost("localhost/127.0.0.1")
            .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
            .setServerAssignorName("range")
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)))
            .build();

        List<Record> expectedRecords = Arrays.asList(
            RecordHelpers.newGroupSubscriptionMetadataRecord(groupId, new HashMap<String, TopicMetadata>() {
                {
                    put(fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 6, mkMapOfPartitionRacks(6)));
                }
            }),
            RecordHelpers.newGroupEpochRecord(groupId, 11),
            RecordHelpers.newTargetAssignmentRecord(groupId, memberId, mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)
            )),
            RecordHelpers.newTargetAssignmentEpochRecord(groupId, 11),
            RecordHelpers.newCurrentAssignmentRecord(groupId, expectedMember)
        );

        assertRecordsEquals(expectedRecords, result.records());

        // Check next refresh time.
        assertFalse(consumerGroup.hasMetadataExpired(context.time.milliseconds()));
        assertEquals(context.time.milliseconds() + 5 * 60 * 1000, consumerGroup.metadataRefreshDeadline().deadlineMs);
        assertEquals(11, consumerGroup.metadataRefreshDeadline().epoch);
    }

    @Test
    public void testGroupIdsByTopics() {
        String groupId1 = "group1";
        String groupId2 = "group2";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withAssignors(Collections.singletonList(assignor))
            .build();

        assertEquals(Collections.emptySet(), context.groupMetadataManager.groupsSubscribedToTopic("foo"));
        assertEquals(Collections.emptySet(), context.groupMetadataManager.groupsSubscribedToTopic("bar"));
        assertEquals(Collections.emptySet(), context.groupMetadataManager.groupsSubscribedToTopic("zar"));

        // M1 in group 1 subscribes to foo and bar.
        context.replay(RecordHelpers.newMemberSubscriptionRecord(groupId1,
            new ConsumerGroupMember.Builder("group1-m1")
                .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
                .build()));

        assertEquals(mkSet(groupId1), context.groupMetadataManager.groupsSubscribedToTopic("foo"));
        assertEquals(mkSet(groupId1), context.groupMetadataManager.groupsSubscribedToTopic("bar"));
        assertEquals(Collections.emptySet(), context.groupMetadataManager.groupsSubscribedToTopic("zar"));

        // M1 in group 2 subscribes to foo, bar and zar.
        context.replay(RecordHelpers.newMemberSubscriptionRecord(groupId2,
            new ConsumerGroupMember.Builder("group2-m1")
                .setSubscribedTopicNames(Arrays.asList("foo", "bar", "zar"))
                .build()));

        assertEquals(mkSet(groupId1, groupId2), context.groupMetadataManager.groupsSubscribedToTopic("foo"));
        assertEquals(mkSet(groupId1, groupId2), context.groupMetadataManager.groupsSubscribedToTopic("bar"));
        assertEquals(mkSet(groupId2), context.groupMetadataManager.groupsSubscribedToTopic("zar"));

        // M2 in group 1 subscribes to bar and zar.
        context.replay(RecordHelpers.newMemberSubscriptionRecord(groupId1,
            new ConsumerGroupMember.Builder("group1-m2")
                .setSubscribedTopicNames(Arrays.asList("bar", "zar"))
                .build()));

        assertEquals(mkSet(groupId1, groupId2), context.groupMetadataManager.groupsSubscribedToTopic("foo"));
        assertEquals(mkSet(groupId1, groupId2), context.groupMetadataManager.groupsSubscribedToTopic("bar"));
        assertEquals(mkSet(groupId1, groupId2), context.groupMetadataManager.groupsSubscribedToTopic("zar"));

        // M2 in group 2 subscribes to foo and bar.
        context.replay(RecordHelpers.newMemberSubscriptionRecord(groupId2,
            new ConsumerGroupMember.Builder("group2-m2")
                .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
                .build()));

        assertEquals(mkSet(groupId1, groupId2), context.groupMetadataManager.groupsSubscribedToTopic("foo"));
        assertEquals(mkSet(groupId1, groupId2), context.groupMetadataManager.groupsSubscribedToTopic("bar"));
        assertEquals(mkSet(groupId1, groupId2), context.groupMetadataManager.groupsSubscribedToTopic("zar"));

        // M1 in group 1 is removed.
        context.replay(RecordHelpers.newCurrentAssignmentTombstoneRecord(groupId1, "group1-m1"));
        context.replay(RecordHelpers.newMemberSubscriptionTombstoneRecord(groupId1, "group1-m1"));

        assertEquals(mkSet(groupId2), context.groupMetadataManager.groupsSubscribedToTopic("foo"));
        assertEquals(mkSet(groupId1, groupId2), context.groupMetadataManager.groupsSubscribedToTopic("bar"));
        assertEquals(mkSet(groupId1, groupId2), context.groupMetadataManager.groupsSubscribedToTopic("zar"));

        // M1 in group 2 subscribes to nothing.
        context.replay(RecordHelpers.newMemberSubscriptionRecord(groupId2,
            new ConsumerGroupMember.Builder("group2-m1")
                .setSubscribedTopicNames(Collections.emptyList())
                .build()));

        assertEquals(mkSet(groupId2), context.groupMetadataManager.groupsSubscribedToTopic("foo"));
        assertEquals(mkSet(groupId1, groupId2), context.groupMetadataManager.groupsSubscribedToTopic("bar"));
        assertEquals(mkSet(groupId1), context.groupMetadataManager.groupsSubscribedToTopic("zar"));

        // M2 in group 2 subscribes to foo.
        context.replay(RecordHelpers.newMemberSubscriptionRecord(groupId2,
            new ConsumerGroupMember.Builder("group2-m2")
                .setSubscribedTopicNames(Arrays.asList("foo"))
                .build()));

        assertEquals(mkSet(groupId2), context.groupMetadataManager.groupsSubscribedToTopic("foo"));
        assertEquals(mkSet(groupId1), context.groupMetadataManager.groupsSubscribedToTopic("bar"));
        assertEquals(mkSet(groupId1), context.groupMetadataManager.groupsSubscribedToTopic("zar"));

        // M2 in group 2 subscribes to nothing.
        context.replay(RecordHelpers.newMemberSubscriptionRecord(groupId2,
            new ConsumerGroupMember.Builder("group2-m2")
                .setSubscribedTopicNames(Collections.emptyList())
                .build()));

        assertEquals(Collections.emptySet(), context.groupMetadataManager.groupsSubscribedToTopic("foo"));
        assertEquals(mkSet(groupId1), context.groupMetadataManager.groupsSubscribedToTopic("bar"));
        assertEquals(mkSet(groupId1), context.groupMetadataManager.groupsSubscribedToTopic("zar"));

        // M2 in group 1 subscribes to nothing.
        context.replay(RecordHelpers.newMemberSubscriptionRecord(groupId1,
            new ConsumerGroupMember.Builder("group1-m2")
                .setSubscribedTopicNames(Collections.emptyList())
                .build()));

        assertEquals(Collections.emptySet(), context.groupMetadataManager.groupsSubscribedToTopic("foo"));
        assertEquals(Collections.emptySet(), context.groupMetadataManager.groupsSubscribedToTopic("bar"));
        assertEquals(Collections.emptySet(), context.groupMetadataManager.groupsSubscribedToTopic("zar"));
    }

    @Test
    public void testOnNewMetadataImageWithEmptyDelta() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withAssignors(Collections.singletonList(new MockPartitionAssignor("range")))
            .build();

        MetadataDelta delta = new MetadataDelta(MetadataImage.EMPTY);
        MetadataImage image = delta.apply(MetadataProvenance.EMPTY);

        context.groupMetadataManager.onNewMetadataImage(image, delta);
        assertEquals(image, context.groupMetadataManager.image());
    }

    @Test
    public void testOnNewMetadataImage() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withAssignors(Collections.singletonList(new MockPartitionAssignor("range")))
            .build();

        // M1 in group 1 subscribes to a and b.
        context.replay(RecordHelpers.newMemberSubscriptionRecord("group1",
            new ConsumerGroupMember.Builder("group1-m1")
                .setSubscribedTopicNames(Arrays.asList("a", "b"))
                .build()));

        // M1 in group 2 subscribes to b and c.
        context.replay(RecordHelpers.newMemberSubscriptionRecord("group2",
            new ConsumerGroupMember.Builder("group2-m1")
                .setSubscribedTopicNames(Arrays.asList("b", "c"))
                .build()));

        // M1 in group 3 subscribes to d.
        context.replay(RecordHelpers.newMemberSubscriptionRecord("group3",
            new ConsumerGroupMember.Builder("group3-m1")
                .setSubscribedTopicNames(Arrays.asList("d"))
                .build()));

        // M1 in group 4 subscribes to e.
        context.replay(RecordHelpers.newMemberSubscriptionRecord("group4",
            new ConsumerGroupMember.Builder("group4-m1")
                .setSubscribedTopicNames(Arrays.asList("e"))
                .build()));

        // M1 in group 5 subscribes to f.
        context.replay(RecordHelpers.newMemberSubscriptionRecord("group5",
            new ConsumerGroupMember.Builder("group5-m1")
                .setSubscribedTopicNames(Arrays.asList("f"))
                .build()));

        // Ensures that all refresh flags are set to the future.
        Arrays.asList("group1", "group2", "group3", "group4", "group5").forEach(groupId -> {
            ConsumerGroup group = context.groupMetadataManager.getOrMaybeCreateConsumerGroup(groupId, false);
            group.setMetadataRefreshDeadline(context.time.milliseconds() + 5000L, 0);
            assertFalse(group.hasMetadataExpired(context.time.milliseconds()));
        });

        // Update the metadata image.
        Uuid topicA = Uuid.randomUuid();
        Uuid topicB = Uuid.randomUuid();
        Uuid topicC = Uuid.randomUuid();
        Uuid topicD = Uuid.randomUuid();
        Uuid topicE = Uuid.randomUuid();

        // Create a first base image with topic a, b, c and d.
        MetadataDelta delta = new MetadataDelta(MetadataImage.EMPTY);
        delta.replay(new TopicRecord().setTopicId(topicA).setName("a"));
        delta.replay(new PartitionRecord().setTopicId(topicA).setPartitionId(0));
        delta.replay(new TopicRecord().setTopicId(topicB).setName("b"));
        delta.replay(new PartitionRecord().setTopicId(topicB).setPartitionId(0));
        delta.replay(new TopicRecord().setTopicId(topicC).setName("c"));
        delta.replay(new PartitionRecord().setTopicId(topicC).setPartitionId(0));
        delta.replay(new TopicRecord().setTopicId(topicD).setName("d"));
        delta.replay(new PartitionRecord().setTopicId(topicD).setPartitionId(0));
        MetadataImage image = delta.apply(MetadataProvenance.EMPTY);

        // Create a delta which updates topic B, deletes topic D and creates topic E.
        delta = new MetadataDelta(image);
        delta.replay(new PartitionRecord().setTopicId(topicB).setPartitionId(2));
        delta.replay(new RemoveTopicRecord().setTopicId(topicD));
        delta.replay(new TopicRecord().setTopicId(topicE).setName("e"));
        delta.replay(new PartitionRecord().setTopicId(topicE).setPartitionId(1));
        image = delta.apply(MetadataProvenance.EMPTY);

        // Update metadata image with the delta.
        context.groupMetadataManager.onNewMetadataImage(image, delta);

        // Verify the groups.
        Arrays.asList("group1", "group2", "group3", "group4").forEach(groupId -> {
            ConsumerGroup group = context.groupMetadataManager.getOrMaybeCreateConsumerGroup(groupId, false);
            assertTrue(group.hasMetadataExpired(context.time.milliseconds()));
        });

        Arrays.asList("group5").forEach(groupId -> {
            ConsumerGroup group = context.groupMetadataManager.getOrMaybeCreateConsumerGroup(groupId, false);
            assertFalse(group.hasMetadataExpired(context.time.milliseconds()));
        });

        // Verify image.
        assertEquals(image, context.groupMetadataManager.image());
    }

    @Test
    public void testSessionTimeoutLifecycle() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withAssignors(Collections.singletonList(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addRacks()
                .build())
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(
            Collections.singletonMap(memberId, new MemberAssignment(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)
            )))
        ));

        // Session timer is scheduled on first heartbeat.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, Record> result =
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setMemberEpoch(0)
                    .setRebalanceTimeoutMs(90000)
                    .setSubscribedTopicNames(Collections.singletonList("foo"))
                    .setTopicPartitions(Collections.emptyList()));
        assertEquals(1, result.response().memberEpoch());

        // Verify that there is a session time.
        context.assertSessionTimeout(groupId, memberId, 45000);

        // Advance time.
        assertEquals(
            Collections.emptyList(),
            context.sleep(result.response().heartbeatIntervalMs())
        );

        // Session timer is rescheduled on second heartbeat.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(result.response().memberEpoch()));
        assertEquals(1, result.response().memberEpoch());

        // Verify that there is a session time.
        context.assertSessionTimeout(groupId, memberId, 45000);

        // Advance time.
        assertEquals(
            Collections.emptyList(),
            context.sleep(result.response().heartbeatIntervalMs())
        );

        // Session timer is cancelled on leave.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(-1));
        assertEquals(-1, result.response().memberEpoch());

        // Verify that there are no timers.
        context.assertNoSessionTimeout(groupId, memberId);
        context.assertNoRevocationTimeout(groupId, memberId);
    }

    @Test
    public void testSessionTimeoutExpiration() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withAssignors(Collections.singletonList(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addRacks()
                .build())
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(
            Collections.singletonMap(memberId, new MemberAssignment(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)
            )))
        ));

        // Session timer is scheduled on first heartbeat.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, Record> result =
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setMemberEpoch(0)
                    .setRebalanceTimeoutMs(90000)
                    .setSubscribedTopicNames(Collections.singletonList("foo"))
                    .setTopicPartitions(Collections.emptyList()));
        assertEquals(1, result.response().memberEpoch());

        // Verify that there is a session time.
        context.assertSessionTimeout(groupId, memberId, 45000);

        // Advance time past the session timeout.
        List<ExpiredTimeout<Void, Record>> timeouts = context.sleep(45000 + 1);

        // Verify the expired timeout.
        assertEquals(
            Collections.singletonList(new ExpiredTimeout<Void, Record>(
                consumerGroupSessionTimeoutKey(groupId, memberId),
                new CoordinatorResult<>(
                    Arrays.asList(
                        RecordHelpers.newCurrentAssignmentTombstoneRecord(groupId, memberId),
                        RecordHelpers.newTargetAssignmentTombstoneRecord(groupId, memberId),
                        RecordHelpers.newMemberSubscriptionTombstoneRecord(groupId, memberId),
                        RecordHelpers.newGroupSubscriptionMetadataRecord(groupId, Collections.emptyMap()),
                        RecordHelpers.newGroupEpochRecord(groupId, 2)
                    )
                )
            )),
            timeouts
        );

        // Verify that there are no timers.
        context.assertNoSessionTimeout(groupId, memberId);
        context.assertNoRevocationTimeout(groupId, memberId);
    }

    @Test
    public void testRevocationTimeoutLifecycle() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String memberId3 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withAssignors(Collections.singletonList(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 3)
                .addRacks()
                .build())
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(
            new HashMap<String, MemberAssignment>() {
                {
                    put(memberId1, new MemberAssignment(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2)
                    )));
                }
            }
        ));

        // Member 1 joins the group.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, Record> result =
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId1)
                    .setMemberEpoch(0)
                    .setRebalanceTimeoutMs(180000)
                    .setSubscribedTopicNames(Collections.singletonList("foo"))
                    .setTopicPartitions(Collections.emptyList()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(1)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setAssignedTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(Arrays.asList(0, 1, 2))))),
            result.response()
        );

        assertEquals(
            Collections.emptyList(),
            context.sleep(result.response().heartbeatIntervalMs())
        );

        // Prepare next assignment.
        assignor.prepareGroupAssignment(new GroupAssignment(
            new HashMap<String, MemberAssignment>() {
                {
                    put(memberId1, new MemberAssignment(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1)
                    )));
                    put(memberId2, new MemberAssignment(mkAssignment(
                        mkTopicAssignment(fooTopicId, 2)
                    )));
                }
            }
        ));

        // Member 2 joins the group.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(90000)
                .setSubscribedTopicNames(Collections.singletonList("foo"))
                .setTopicPartitions(Collections.emptyList()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(2)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setPendingTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(Arrays.asList(2))))),
            result.response()
        );

        assertEquals(
            Collections.emptyList(),
            context.sleep(result.response().heartbeatIntervalMs())
        );

        // Member 1 heartbeats and transitions to revoking. The revocation timeout
        // is scheduled.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId1)
                .setMemberEpoch(1)
                .setRebalanceTimeoutMs(12000)
                .setSubscribedTopicNames(Collections.singletonList("foo")));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(1)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setAssignedTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(Arrays.asList(0, 1))))),
            result.response()
        );

        // Verify that there is a revocation timeout.
        context.assertRevocationTimeout(groupId, memberId1, 12000);

        assertEquals(
            Collections.emptyList(),
            context.sleep(result.response().heartbeatIntervalMs())
        );

        // Prepare next assignment.
        assignor.prepareGroupAssignment(new GroupAssignment(
            new HashMap<String, MemberAssignment>() {
                {
                    put(memberId1, new MemberAssignment(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0)
                    )));
                    put(memberId2, new MemberAssignment(mkAssignment(
                        mkTopicAssignment(fooTopicId, 2)
                    )));
                    put(memberId3, new MemberAssignment(mkAssignment(
                        mkTopicAssignment(fooTopicId, 1)
                    )));
                }
            }
        ));

        // Member 3 joins the group.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId3)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(90000)
                .setSubscribedTopicNames(Collections.singletonList("foo"))
                .setTopicPartitions(Collections.emptyList()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId3)
                .setMemberEpoch(3)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setPendingTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(Arrays.asList(1))))),
            result.response()
        );

        assertEquals(
            Collections.emptyList(),
            context.sleep(result.response().heartbeatIntervalMs())
        );

        // Member 1 heartbeats and re-transitions to revoking. The revocation timeout
        // is re-scheduled.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId1)
                .setMemberEpoch(1)
                .setRebalanceTimeoutMs(90000)
                .setSubscribedTopicNames(Collections.singletonList("foo")));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(1)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setAssignedTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(Arrays.asList(0))))),
            result.response()
        );

        // Verify that there is a revocation timeout. Keep a reference
        // to the timeout for later.
        ScheduledTimeout<Void, Record> scheduledTimeout =
            context.assertRevocationTimeout(groupId, memberId1, 90000);

        assertEquals(
            Collections.emptyList(),
            context.sleep(result.response().heartbeatIntervalMs())
        );

        // Member 1 acks the revocation. The revocation timeout is cancelled.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId1)
                .setMemberEpoch(1)
                .setTopicPartitions(Collections.singletonList(new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(fooTopicId)
                    .setPartitions(Collections.singletonList(0)))));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(3)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setAssignedTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(Arrays.asList(0))))),
            result.response()
        );

        // Verify that there is not revocation timeout.
        context.assertNoRevocationTimeout(groupId, memberId1);

        // Execute the scheduled revocation timeout captured earlier to simulate a
        // stale timeout. This should be a no-op.
        assertEquals(Collections.emptyList(), scheduledTimeout.operation.generateRecords().records());
    }

    @Test
    public void testRevocationTimeoutExpiration() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withAssignors(Collections.singletonList(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 3)
                .addRacks()
                .build())
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(
            new HashMap<String, MemberAssignment>() {
                {
                    put(memberId1, new MemberAssignment(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2)
                    )));
                }
            }
        ));

        // Member 1 joins the group.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, Record> result =
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId1)
                    .setMemberEpoch(0)
                    .setRebalanceTimeoutMs(10000) // Use timeout smaller than session timeout.
                    .setSubscribedTopicNames(Collections.singletonList("foo"))
                    .setTopicPartitions(Collections.emptyList()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(1)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setAssignedTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(Arrays.asList(0, 1, 2))))),
            result.response()
        );

        assertEquals(
            Collections.emptyList(),
            context.sleep(result.response().heartbeatIntervalMs())
        );

        // Prepare next assignment.
        assignor.prepareGroupAssignment(new GroupAssignment(
            new HashMap<String, MemberAssignment>() {
                {
                    put(memberId1, new MemberAssignment(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1)
                    )));
                    put(memberId2, new MemberAssignment(mkAssignment(
                        mkTopicAssignment(fooTopicId, 2)
                    )));
                }
            }
        ));

        // Member 2 joins the group.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(10000)
                .setSubscribedTopicNames(Collections.singletonList("foo"))
                .setTopicPartitions(Collections.emptyList()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(2)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setPendingTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(Arrays.asList(2))))),
            result.response()
        );

        assertEquals(
            Collections.emptyList(),
            context.sleep(result.response().heartbeatIntervalMs())
        );

        // Member 1 heartbeats and transitions to revoking. The revocation timeout
        // is scheduled.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId1)
                .setMemberEpoch(1));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(1)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setAssignedTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(Arrays.asList(0, 1))))),
            result.response()
        );

        // Advance time past the revocation timeout.
        List<ExpiredTimeout<Void, Record>> timeouts = context.sleep(10000 + 1);

        // Verify the expired timeout.
        assertEquals(
            Collections.singletonList(new ExpiredTimeout<Void, Record>(
                consumerGroupRevocationTimeoutKey(groupId, memberId1),
                new CoordinatorResult<>(
                    Arrays.asList(
                        RecordHelpers.newCurrentAssignmentTombstoneRecord(groupId, memberId1),
                        RecordHelpers.newTargetAssignmentTombstoneRecord(groupId, memberId1),
                        RecordHelpers.newMemberSubscriptionTombstoneRecord(groupId, memberId1),
                        RecordHelpers.newGroupEpochRecord(groupId, 3)
                    )
                )
            )),
            timeouts
        );

        // Verify that there are no timers.
        context.assertNoSessionTimeout(groupId, memberId1);
        context.assertNoRevocationTimeout(groupId, memberId1);
    }

    @Test
    public void testOnLoaded() {
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withAssignors(Collections.singletonList(new MockPartitionAssignor("range")))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addTopic(barTopicId, barTopicName, 3)
                .build())
            .withConsumerGroup(new ConsumerGroupBuilder("foo", 10)
                .withMember(new ConsumerGroupMember.Builder("foo-1")
                    .setMemberEpoch(9)
                    .setPreviousMemberEpoch(9)
                    .setTargetMemberEpoch(10)
                    .setClientId("client")
                    .setClientHost("localhost/127.0.0.1")
                    .setSubscribedTopicNames(Arrays.asList("foo"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2)))
                    .setPartitionsPendingRevocation(mkAssignment(
                        mkTopicAssignment(fooTopicId, 3, 4, 5)))
                    .build())
                .withMember(new ConsumerGroupMember.Builder("foo-2")
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setTargetMemberEpoch(10)
                    .setClientId("client")
                    .setClientHost("localhost/127.0.0.1")
                    .setSubscribedTopicNames(Arrays.asList("foo"))
                    .setServerAssignorName("range")
                    .setPartitionsPendingAssignment(mkAssignment(
                        mkTopicAssignment(fooTopicId, 3, 4, 5)))
                    .build())
                .withAssignment("foo-1", mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2)))
                .withAssignment("foo-2", mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5)))
                .withAssignmentEpoch(10))
            .build();

        // Let's assume that all the records have been replayed and now
        // onLoaded is called to signal it.
        context.groupMetadataManager.onLoaded();

        // All members should have a session timeout in place.
        assertNotNull(context.timer.timeout(consumerGroupSessionTimeoutKey("foo", "foo-1")));
        assertNotNull(context.timer.timeout(consumerGroupSessionTimeoutKey("foo", "foo-2")));

        // foo-1 should also have a revocation timeout in place.
        assertNotNull(context.timer.timeout(consumerGroupRevocationTimeoutKey("foo", "foo-1")));
    }

    @Test
    public void testGenerateRecordsOnNewGroup() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        CompletableFuture<JoinGroupResponseData> responseFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(request, responseFuture, true);
        assertTrue(responseFuture.isDone());
        assertEquals(Errors.MEMBER_ID_REQUIRED.code(), responseFuture.get().errorCode());

        GenericGroup group = context.createGenericGroup("group-id");

        assertEquals(
            Collections.singletonList(RecordHelpers.newEmptyGroupMetadataRecord(group, MetadataVersion.latest())),
            result.records()
        );
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testReplayGroupMetadataRecords(boolean useDefaultRebalanceTimeout) {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        byte[] subscription = ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
            Collections.singletonList("foo"))).array();
        List<GroupMetadataValue.MemberMetadata> members = new ArrayList<>();
        List<GenericGroupMember> expectedMembers = new ArrayList<>();
        JoinGroupRequestProtocolCollection expectedProtocols = new JoinGroupRequestProtocolCollection(0);
        expectedProtocols.add(new JoinGroupRequestProtocol()
            .setName("range")
            .setMetadata(subscription));

        IntStream.range(0, 2).forEach(i -> {
            members.add(new GroupMetadataValue.MemberMetadata()
                .setMemberId("member-" + i)
                .setGroupInstanceId("group-instance-id-" + i)
                .setSubscription(subscription)
                .setAssignment(new byte[]{2})
                .setClientId("client-" + i)
                .setClientHost("host-" + i)
                .setSessionTimeout(4000)
                .setRebalanceTimeout(useDefaultRebalanceTimeout ? -1 : 9000)
            );

            expectedMembers.add(new GenericGroupMember(
                "member-" + i,
                Optional.of("group-instance-id-" + i),
                "client-" + i,
                "host-" + i,
                useDefaultRebalanceTimeout ? 4000 : 9000,
                4000,
                "consumer",
                expectedProtocols,
                new byte[]{2}
            ));
        });

        Record groupMetadataRecord = newGroupMetadataRecord("group-id",
            new GroupMetadataValue()
                .setMembers(members)
                .setGeneration(1)
                .setLeader("member-0")
                .setProtocolType("consumer")
                .setProtocol("range")
                .setCurrentStateTimestamp(context.time.milliseconds()),
            MetadataVersion.latest());

        context.replay(groupMetadataRecord);
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup("group-id", false);

        GenericGroup expectedGroup = new GenericGroup(
            new LogContext(),
            "group-id",
            STABLE,
            context.time,
            1,
            Optional.of("consumer"),
            Optional.of("range"),
            Optional.of("member-0"),
            Optional.of(context.time.milliseconds())
        );
        expectedMembers.forEach(expectedGroup::add);

        assertEquals(expectedGroup.groupId(), group.groupId());
        assertEquals(expectedGroup.generationId(), group.generationId());
        assertEquals(expectedGroup.protocolType(), group.protocolType());
        assertEquals(expectedGroup.protocolName(), group.protocolName());
        assertEquals(expectedGroup.leaderOrNull(), group.leaderOrNull());
        assertEquals(expectedGroup.currentState(), group.currentState());
        assertEquals(expectedGroup.currentStateTimestampOrDefault(), group.currentStateTimestampOrDefault());
        assertEquals(expectedGroup.currentGenericGroupMembers(), group.currentGenericGroupMembers());
    }

    @Test
    public void testOnLoadedExceedGroupMaxSizeTriggersRebalance() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withGenericGroupMaxSize(1)
            .build();

        byte[] subscription = ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
            Collections.singletonList("foo"))).array();
        List<GroupMetadataValue.MemberMetadata> members = new ArrayList<>();

        IntStream.range(0, 2).forEach(i -> {
            members.add(new GroupMetadataValue.MemberMetadata()
                .setMemberId("member-" + i)
                .setGroupInstanceId("group-instance-id-" + i)
                .setSubscription(subscription)
                .setAssignment(new byte[]{2})
                .setClientId("client-" + i)
                .setClientHost("host-" + i)
                .setSessionTimeout(4000)
                .setRebalanceTimeout(9000)
            );
        });

        Record groupMetadataRecord = newGroupMetadataRecord("group-id",
            new GroupMetadataValue()
                .setMembers(members)
                .setGeneration(1)
                .setLeader("member-0")
                .setProtocolType("consumer")
                .setProtocol("range")
                .setCurrentStateTimestamp(context.time.milliseconds()),
            MetadataVersion.latest());

        context.replay(groupMetadataRecord);
        context.groupMetadataManager.onLoaded();
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup("group-id", false);

        assertTrue(group.isInState(PREPARING_REBALANCE));
        assertEquals(2, group.size());
    }

    @Test
    public void testOnLoadedSchedulesGenericGroupMemberHeartbeats() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        byte[] subscription = ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
            Collections.singletonList("foo"))).array();
        List<GroupMetadataValue.MemberMetadata> members = new ArrayList<>();

        IntStream.range(0, 2).forEach(i -> {
            members.add(new GroupMetadataValue.MemberMetadata()
                .setMemberId("member-" + i)
                .setGroupInstanceId("group-instance-id-" + i)
                .setSubscription(subscription)
                .setAssignment(new byte[]{2})
                .setClientId("client-" + i)
                .setClientHost("host-" + i)
                .setSessionTimeout(4000)
                .setRebalanceTimeout(9000)
            );
        });

        Record groupMetadataRecord = newGroupMetadataRecord("group-id",
            new GroupMetadataValue()
                .setMembers(members)
                .setGeneration(1)
                .setLeader("member-0")
                .setProtocolType("consumer")
                .setProtocol("range")
                .setCurrentStateTimestamp(context.time.milliseconds()),
            MetadataVersion.latest());

        context.replay(groupMetadataRecord);
        context.groupMetadataManager.onLoaded();

        IntStream.range(0, 2).forEach(i -> {
            ScheduledTimeout<Void, Record> timeout = context.timer.timeout(
                genericGroupHeartbeatKey("group-id", "member-1"));

            assertNotNull(timeout);
            assertEquals(context.time.milliseconds() + 4000, timeout.deadlineMs);
        });
    }

    @Test
    public void testJoinGroupShouldReceiveErrorIfGroupOverMaxSize() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withGenericGroupMaxSize(10)
            .build();
        context.createGenericGroup("group-id");

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .withReason("exceed max group size")
            .build();

        IntStream.range(0, 10).forEach(i -> {
            CompletableFuture<JoinGroupResponseData> responseFuture = new CompletableFuture<>();
            CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(request, responseFuture);
            assertFalse(responseFuture.isDone());
            assertTrue(result.records().isEmpty());
        });

        CompletableFuture<JoinGroupResponseData> responseFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(request, responseFuture);
        assertTrue(result.records().isEmpty());
        assertTrue(responseFuture.isDone());
        assertEquals(Errors.GROUP_MAX_SIZE_REACHED.code(), responseFuture.get(5, TimeUnit.SECONDS).errorCode());
    }

    @Test
    public void testDynamicMembersJoinGroupWithMaxSizeAndRequiredKnownMember() {
        boolean requiredKnownMemberId = true;
        int groupMaxSize = 10;
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withGenericGroupMaxSize(groupMaxSize)
            .withGenericGroupInitialRebalanceDelayMs(50)
            .build();

        GenericGroup group = context.createGenericGroup("group-id");

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        // First round of join requests. Generate member ids. All requests will be accepted
        // as the group is still Empty.
        List<CompletableFuture<JoinGroupResponseData>> firstRoundFutures = new ArrayList<>();
        IntStream.range(0, groupMaxSize + 1).forEach(i -> {
            CompletableFuture<JoinGroupResponseData> responseFuture = new CompletableFuture<>();
            firstRoundFutures.add(responseFuture);
            CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(request, responseFuture, requiredKnownMemberId);
            assertTrue(responseFuture.isDone());
            JoinGroupResponseData response = null;
            try {
                response = responseFuture.get();
            } catch (Exception ignored) {
            }
            assertNotNull(response);
            assertEquals(Errors.MEMBER_ID_REQUIRED.code(), response.errorCode());
            assertTrue(result.records().isEmpty());
        });

        List<String> memberIds = verifyGenericGroupJoinResponses(firstRoundFutures, 0, Errors.MEMBER_ID_REQUIRED);
        assertEquals(groupMaxSize + 1, memberIds.size());
        assertEquals(0, group.size());
        assertTrue(group.isInState(EMPTY));
        assertEquals(groupMaxSize + 1, group.numPendingJoinMembers());

        // Second round of join requests with the generated member ids.
        // One of them will fail, reaching group max size.
        List<CompletableFuture<JoinGroupResponseData>> secondRoundFutures = new ArrayList<>();
        memberIds.forEach(memberId -> {
            CompletableFuture<JoinGroupResponseData> responseFuture = new CompletableFuture<>();
            secondRoundFutures.add(responseFuture);
            CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(
                request.setMemberId(memberId),
                responseFuture,
                requiredKnownMemberId
            );
            assertTrue(result.records().isEmpty());
        });

        // Advance clock by group initial rebalance delay to complete first inital delayed join.
        // This will extend the initial rebalance as new members have joined.
        assertNoOrEmptyResult(context.sleep(50));
        // Advance clock by group initial rebalance delay to complete second inital delayed join.
        // Since there are no new members that joined since the previous delayed join,
        // the join group phase will complete.
        assertNoOrEmptyResult(context.sleep(50));

        verifyGenericGroupJoinResponses(secondRoundFutures, groupMaxSize, Errors.GROUP_MAX_SIZE_REACHED);
        assertEquals(groupMaxSize, group.size());
        assertEquals(0, group.numPendingJoinMembers());
        assertTrue(group.isInState(COMPLETING_REBALANCE));

        // Members that were accepted can rejoin while others are rejected in CompletingRebalance state.
        List<CompletableFuture<JoinGroupResponseData>> thirdRoundFutures = new ArrayList<>();
        memberIds.forEach(memberId -> {
            CompletableFuture<JoinGroupResponseData> responseFuture = new CompletableFuture<>();
            thirdRoundFutures.add(responseFuture);
            CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(
                request.setMemberId(memberId),
                responseFuture,
                requiredKnownMemberId
            );
            assertTrue(result.records().isEmpty());
        });

        verifyGenericGroupJoinResponses(thirdRoundFutures, groupMaxSize, Errors.GROUP_MAX_SIZE_REACHED);
    }

    @Test
    public void testDynamicMembersJoinGroupWithMaxSizeAndNotRequiredKnownMember() {
        boolean requiredKnownMemberId = false;
        int groupMaxSize = 10;
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withGenericGroupMaxSize(groupMaxSize)
            .withGenericGroupInitialRebalanceDelayMs(50)
            .build();

        GenericGroup group = context.createGenericGroup("group-id");

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        // First round of join requests. This will trigger a rebalance.
        List<CompletableFuture<JoinGroupResponseData>> firstRoundFutures = new ArrayList<>();
        IntStream.range(0, groupMaxSize + 1).forEach(i -> {
            CompletableFuture<JoinGroupResponseData> responseFuture = new CompletableFuture<>();
            firstRoundFutures.add(responseFuture);
            CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(request, responseFuture, requiredKnownMemberId);
            assertTrue(result.records().isEmpty());
        });

        assertEquals(groupMaxSize, group.size());
        assertEquals(groupMaxSize, group.numAwaitingJoinResponse());
        assertTrue(group.isInState(PREPARING_REBALANCE));

        // Advance clock by group initial rebalance delay to complete first inital delayed join.
        // This will extend the initial rebalance as new members have joined.
        assertNoOrEmptyResult(context.sleep(50));
        // Advance clock by group initial rebalance delay to complete second inital delayed join.
        // Since there are no new members that joined since the previous delayed join,
        // we will complete the rebalance.
        assertNoOrEmptyResult(context.sleep(50));

        List<String> memberIds = verifyGenericGroupJoinResponses(firstRoundFutures, groupMaxSize, Errors.GROUP_MAX_SIZE_REACHED);

        // Members that were accepted can rejoin while others are rejected in CompletingRebalance state.
        List<CompletableFuture<JoinGroupResponseData>> secondRoundFutures = new ArrayList<>();
        memberIds.forEach(memberId -> {
            CompletableFuture<JoinGroupResponseData> responseFuture = new CompletableFuture<>();
            secondRoundFutures.add(responseFuture);
            CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(
                request.setMemberId(memberId),
                responseFuture,
                requiredKnownMemberId
            );
            assertTrue(result.records().isEmpty());
        });

        verifyGenericGroupJoinResponses(secondRoundFutures, 10, Errors.GROUP_MAX_SIZE_REACHED);
        assertEquals(groupMaxSize, group.size());
        assertEquals(0, group.numAwaitingJoinResponse());
        assertTrue(group.isInState(COMPLETING_REBALANCE));
    }

    @Test
    public void testStaticMembersJoinGroupWithMaxSize() {
        int groupMaxSize = 10;

        List<String> groupInstanceIds = IntStream.range(0, groupMaxSize + 1)
            .mapToObj(i -> "instance-id-" + i)
            .collect(Collectors.toList());

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withGenericGroupMaxSize(groupMaxSize)
            .withGenericGroupInitialRebalanceDelayMs(50)
            .build();

        GenericGroup group = context.createGenericGroup("group-id");

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        // First round of join requests. This will trigger a rebalance.
        List<CompletableFuture<JoinGroupResponseData>> firstRoundFutures = new ArrayList<>();
        groupInstanceIds.forEach(instanceId -> {
            CompletableFuture<JoinGroupResponseData> responseFuture = new CompletableFuture<>();
            firstRoundFutures.add(responseFuture);
            CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(request.setGroupInstanceId(instanceId), responseFuture);
            assertTrue(result.records().isEmpty());
        });

        assertEquals(groupMaxSize, group.size());
        assertEquals(groupMaxSize, group.numAwaitingJoinResponse());
        assertTrue(group.isInState(PREPARING_REBALANCE));

        // Advance clock by group initial rebalance delay to complete first inital delayed join.
        // This will extend the initial rebalance as new members have joined.
        assertNoOrEmptyResult(context.sleep(50));
        // Advance clock by group initial rebalance delay to complete second inital delayed join.
        // Since there are no new members that joined since the previous delayed join,
        // we will complete the rebalance.
        assertNoOrEmptyResult(context.sleep(50));

        List<String> memberIds = verifyGenericGroupJoinResponses(firstRoundFutures, groupMaxSize, Errors.GROUP_MAX_SIZE_REACHED);

        // Members which were accepted can rejoin, others are rejected, while
        // completing rebalance
        List<CompletableFuture<JoinGroupResponseData>> secondRoundFutures = new ArrayList<>();
        IntStream.range(0, groupMaxSize + 1).forEach(i -> {
            CompletableFuture<JoinGroupResponseData> responseFuture = new CompletableFuture<>();
            secondRoundFutures.add(responseFuture);
            CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(
                request.setMemberId(memberIds.get(i))
                    .setGroupInstanceId(groupInstanceIds.get(i)),
                responseFuture
            );
            assertTrue(result.records().isEmpty());
        });

        verifyGenericGroupJoinResponses(secondRoundFutures, groupMaxSize, Errors.GROUP_MAX_SIZE_REACHED);
        assertEquals(groupMaxSize, group.size());
        assertEquals(0, group.numAwaitingJoinResponse());
        assertTrue(group.isInState(COMPLETING_REBALANCE));
    }

    @Test
    public void testDynamicMembersCanRejoinGroupWithMaxSizeWhileRebalancing() {
        boolean requiredKnownMemberId = true;
        int groupMaxSize = 10;
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withGenericGroupMaxSize(groupMaxSize)
            .withGenericGroupInitialRebalanceDelayMs(50)
            .build();

        GenericGroup group = context.createGenericGroup("group-id");

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        // First round of join requests. Generate member ids.
        List<CompletableFuture<JoinGroupResponseData>> firstRoundFutures = new ArrayList<>();
        IntStream.range(0, groupMaxSize + 1).forEach(i -> {
            CompletableFuture<JoinGroupResponseData> responseFuture = new CompletableFuture<>();
            firstRoundFutures.add(responseFuture);
            CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(
                request,
                responseFuture,
                requiredKnownMemberId
            );
            assertTrue(result.records().isEmpty());
        });

        assertEquals(0, group.size());
        assertEquals(groupMaxSize + 1, group.numPendingJoinMembers());
        assertTrue(group.isInState(EMPTY));

        List<String> memberIds = verifyGenericGroupJoinResponses(firstRoundFutures, 0, Errors.MEMBER_ID_REQUIRED);
        assertEquals(groupMaxSize + 1, memberIds.size());

        // Second round of join requests with the generated member ids.
        // One of them will fail, reaching group max size.
        memberIds.forEach(memberId -> {
            CompletableFuture<JoinGroupResponseData> responseFuture = new CompletableFuture<>();
            CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(
                request.setMemberId(memberId),
                responseFuture,
                requiredKnownMemberId
            );
            assertTrue(result.records().isEmpty());
        });

        assertEquals(groupMaxSize, group.size());
        assertEquals(groupMaxSize, group.numAwaitingJoinResponse());
        assertTrue(group.isInState(PREPARING_REBALANCE));

        // Members can rejoin while rebalancing
        List<CompletableFuture<JoinGroupResponseData>> thirdRoundFutures = new ArrayList<>();
        memberIds.forEach(memberId -> {
            CompletableFuture<JoinGroupResponseData> responseFuture = new CompletableFuture<>();
            thirdRoundFutures.add(responseFuture);
            CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(
                request.setMemberId(memberId),
                responseFuture,
                requiredKnownMemberId
            );
            assertTrue(result.records().isEmpty());
        });

        // Advance clock by group initial rebalance delay to complete first inital delayed join.
        // This will extend the initial rebalance as new members have joined.
        assertNoOrEmptyResult(context.sleep(50));
        // Advance clock by group initial rebalance delay to complete second inital delayed join.
        // Since there are no new members that joined since the previous delayed join,
        // we will complete the rebalance.
        assertNoOrEmptyResult(context.sleep(50));

        verifyGenericGroupJoinResponses(thirdRoundFutures, groupMaxSize, Errors.GROUP_MAX_SIZE_REACHED);
        assertEquals(groupMaxSize, group.size());
        assertEquals(0, group.numAwaitingJoinResponse());
        assertTrue(group.isInState(COMPLETING_REBALANCE));
    }

    @Test
    public void testLastJoiningMembersAreKickedOutWhenRejoiningGroupWithMaxSize() {
        int groupMaxSize = 10;

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withGenericGroupMaxSize(groupMaxSize)
            .withGenericGroupInitialRebalanceDelayMs(50)
            .build();

        // Create a group and add members that exceed the group max size.
        GenericGroup group = context.createGenericGroup("group-id");

        List<String> memberIds = IntStream.range(0, groupMaxSize + 2)
            .mapToObj(i -> group.generateMemberId("client-id", Optional.empty()))
            .collect(Collectors.toList());

        memberIds.forEach(memberId -> {
            JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestProtocolCollection();
            protocols.add(new JoinGroupRequestProtocol()
                .setName("range")
                .setMetadata(new byte[0]));

            group.add(
                new GenericGroupMember(
                    memberId,
                    Optional.empty(),
                    "client-id",
                    "client-host",
                    10000,
                    5000,
                    "consumer",
                    protocols
                )
            );
        });

        context.groupMetadataManager.prepareRebalance(group, "test");

        List<CompletableFuture<JoinGroupResponseData>> responseFutures = new ArrayList<>();
        memberIds.forEach(memberId -> {
            JoinGroupRequestData request = new JoinGroupRequestBuilder()
                .withGroupId("group-id")
                .withMemberId(memberId)
                .withDefaultProtocolTypeAndProtocols()
                .withRebalanceTimeoutMs(10000)
                .build();

            CompletableFuture<JoinGroupResponseData> responseFuture = new CompletableFuture<>();
            responseFutures.add(responseFuture);
            CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(request, responseFuture);
            assertTrue(result.records().isEmpty());
        });

        assertEquals(groupMaxSize, group.size());
        assertEquals(groupMaxSize, group.numAwaitingJoinResponse());
        assertTrue(group.isInState(PREPARING_REBALANCE));

        // Advance clock by rebalance timeout to complete join phase.
        assertNoOrEmptyResult(context.sleep(10000));

        verifyGenericGroupJoinResponses(responseFutures, groupMaxSize, Errors.GROUP_MAX_SIZE_REACHED);

        assertEquals(groupMaxSize, group.size());
        assertTrue(group.isInState(COMPLETING_REBALANCE));

        memberIds.subList(groupMaxSize, groupMaxSize + 2)
            .forEach(memberId -> assertFalse(group.hasMemberId(memberId)));

        memberIds.subList(0, groupMaxSize)
            .forEach(memberId -> assertTrue(group.hasMemberId(memberId)));
    }

    @Test
    public void testJoinGroupSessionTimeoutTooSmall() throws Exception {
        int minSessionTimeout = 50;
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withGenericGroupMinSessionTimeoutMs(minSessionTimeout)
            .build();

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withSessionTimeoutMs(minSessionTimeout - 1)
            .build();

        CompletableFuture<JoinGroupResponseData> responseFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(request, responseFuture);
        assertTrue(responseFuture.isDone());
        assertTrue(result.records().isEmpty());
        assertEquals(Errors.INVALID_SESSION_TIMEOUT.code(), responseFuture.get(5, TimeUnit.SECONDS).errorCode());
    }

    @Test
    public void testJoinGroupSessionTimeoutTooLarge() throws Exception {
        int maxSessionTimeout = 50;
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withGenericGroupMaxSessionTimeoutMs(maxSessionTimeout)
            .build();

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withSessionTimeoutMs(maxSessionTimeout + 1)
            .build();

        CompletableFuture<JoinGroupResponseData> responseFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(request, responseFuture);

        assertTrue(result.records().isEmpty());
        assertTrue(responseFuture.isDone());
        assertEquals(Errors.INVALID_SESSION_TIMEOUT.code(), responseFuture.get().errorCode());
    }

    @Test
    public void testJoinGroupUnknownMemberNewGroup() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId("member-id")
            .build();

        CompletableFuture<JoinGroupResponseData> responseFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(request, responseFuture);

        assertTrue(result.records().isEmpty());
        assertTrue(responseFuture.isDone());
        assertEquals(Errors.UNKNOWN_MEMBER_ID.code(), responseFuture.get().errorCode());

        // Static member
        request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId("member-id")
            .withGroupInstanceId("group-instance-id")
            .build();

        responseFuture = new CompletableFuture<>();
        result = context.sendGenericGroupJoin(request, responseFuture);

        assertTrue(result.records().isEmpty());
        assertTrue(responseFuture.isDone());
        assertEquals(Errors.UNKNOWN_MEMBER_ID.code(), responseFuture.get().errorCode());
    }

    @Test
    public void testGenericGroupJoinInconsistentProtocolType() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        context.createGenericGroup("group-id");

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        context.joinGenericGroupAsDynamicMemberAndCompleteJoin(request);

        request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withProtocolType("connect")
            .withProtocols(toProtocols("range"))
            .build();

        CompletableFuture<JoinGroupResponseData> responseFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(request, responseFuture);

        assertTrue(result.records().isEmpty());
        assertTrue(responseFuture.isDone());
        assertEquals(Errors.INCONSISTENT_GROUP_PROTOCOL.code(), responseFuture.get().errorCode());
    }

    @Test
    public void testJoinGroupWithEmptyProtocolType() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        context.createGenericGroup("group-id");

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withProtocolType("")
            .withProtocols(toProtocols("range"))
            .build();

        CompletableFuture<JoinGroupResponseData> responseFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(request, responseFuture);
        assertTrue(result.records().isEmpty());
        assertTrue(responseFuture.isDone());
        assertEquals(Errors.INCONSISTENT_GROUP_PROTOCOL.code(), responseFuture.get().errorCode());

        // Send as static member join.
        responseFuture = new CompletableFuture<>();
        result = context.sendGenericGroupJoin(request.setGroupInstanceId("group-instance-id"), responseFuture, true, true);
        assertTrue(result.records().isEmpty());
        assertTrue(responseFuture.isDone());
        assertEquals(Errors.INCONSISTENT_GROUP_PROTOCOL.code(), responseFuture.get().errorCode());
    }

    @Test
    public void testJoinGroupWithEmptyGroupProtocol() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        context.createGenericGroup("group-id");

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withProtocolType("consumer")
            .withProtocols(new JoinGroupRequestProtocolCollection(0))
            .build();

        CompletableFuture<JoinGroupResponseData> responseFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(request, responseFuture);
        assertTrue(result.records().isEmpty());
        assertTrue(responseFuture.isDone());
        assertEquals(Errors.INCONSISTENT_GROUP_PROTOCOL.code(), responseFuture.get().errorCode());
    }

    @Test
    public void testNewMemberJoinExpiration() throws Exception {
        // This tests new member expiration during a protracted rebalance. We first create a
        // group with one member which uses a large value for session timeout and rebalance timeout.
        // We then join with one new member and let the rebalance hang while we await the first member.
        // The new member join timeout expires and its JoinGroup request is failed.

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        GenericGroup group = context.createGenericGroup("group-id");

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .withSessionTimeoutMs(5000 + context.genericGroupNewMemberJoinTimeoutMs)
            .withRebalanceTimeoutMs(2 * context.genericGroupNewMemberJoinTimeoutMs)
            .build();

        JoinGroupResponseData firstResponse = context.joinGenericGroupAsDynamicMemberAndCompleteJoin(request);
        String firstMemberId = firstResponse.memberId();
        assertEquals(firstResponse.leader(), firstMemberId);
        assertEquals(Errors.NONE.code(), firstResponse.errorCode());

        assertNotNull(group);
        assertEquals(0, group.allMembers().stream().filter(GenericGroupMember::isNew).count());

        // Send second join group request for a new dynamic member.
        CompletableFuture<JoinGroupResponseData> secondResponseFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(request
            .setSessionTimeoutMs(5000)
            .setRebalanceTimeoutMs(5000),
            secondResponseFuture
        );
        assertTrue(result.records().isEmpty());
        assertFalse(secondResponseFuture.isDone());

        assertEquals(2, group.allMembers().size());
        assertEquals(1, group.allMembers().stream().filter(GenericGroupMember::isNew).count());

        GenericGroupMember newMember = group.allMembers().stream().filter(GenericGroupMember::isNew).findFirst().get();
        assertNotEquals(firstMemberId, newMember.memberId());

        // Advance clock by new member join timeout to expire the second member.
        assertNoOrEmptyResult(context.sleep(context.genericGroupNewMemberJoinTimeoutMs));

        assertTrue(secondResponseFuture.isDone());
        JoinGroupResponseData secondResponse = secondResponseFuture.get();

        assertEquals(Errors.UNKNOWN_MEMBER_ID.code(), secondResponse.errorCode());
        assertEquals(1, group.allMembers().size());
        assertEquals(0, group.allMembers().stream().filter(GenericGroupMember::isNew).count());
        assertEquals(firstMemberId, group.allMembers().iterator().next().memberId());
    }

    @Test
    public void testJoinGroupInconsistentGroupProtocol() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        context.createGenericGroup("group-id");

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        CompletableFuture<JoinGroupResponseData> responseFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(request, responseFuture);

        assertTrue(result.records().isEmpty());
        assertFalse(responseFuture.isDone());

        JoinGroupRequestProtocolCollection otherProtocols = new JoinGroupRequestProtocolCollection(0);
        otherProtocols.add(new JoinGroupRequestProtocol().setName("roundrobin"));
        CompletableFuture<JoinGroupResponseData> otherResponseFuture = new CompletableFuture<>();
        result = context.sendGenericGroupJoin(request.setProtocols(otherProtocols), otherResponseFuture);

        assertTrue(result.records().isEmpty());
        assertTrue(otherResponseFuture.isDone());

        assertNoOrEmptyResult(context.sleep(context.genericGroupInitialRebalanceDelayMs));
        assertTrue(responseFuture.isDone());
        assertEquals(Errors.NONE.code(), responseFuture.get().errorCode());
        assertEquals(Errors.INCONSISTENT_GROUP_PROTOCOL.code(), otherResponseFuture.get().errorCode());
    }

    @Test
    public void testJoinGroupSecondJoinInconsistentProtocol() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        context.createGenericGroup("group-id");

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        CompletableFuture<JoinGroupResponseData> responseFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(request, responseFuture, true);

        assertTrue(result.records().isEmpty());
        assertTrue(responseFuture.isDone());
        assertEquals(Errors.MEMBER_ID_REQUIRED.code(), responseFuture.get(5, TimeUnit.SECONDS).errorCode());

        // Sending an inconsistent protocol should be refused
        String memberId = responseFuture.get(5, TimeUnit.SECONDS).memberId();
        JoinGroupRequestProtocolCollection emptyProtocols = new JoinGroupRequestProtocolCollection(0);
        request = request.setMemberId(memberId)
            .setProtocols(emptyProtocols);

        responseFuture = new CompletableFuture<>();
        result = context.sendGenericGroupJoin(request, responseFuture, true);

        assertTrue(result.records().isEmpty());
        assertTrue(responseFuture.isDone());
        assertEquals(Errors.INCONSISTENT_GROUP_PROTOCOL.code(), responseFuture.get().errorCode());

        // Sending consistent protocol should be accepted
        responseFuture = new CompletableFuture<>();
        result = context.sendGenericGroupJoin(request.setProtocols(toProtocols("range")), responseFuture, true);

        assertTrue(result.records().isEmpty());
        assertFalse(responseFuture.isDone());

        assertNoOrEmptyResult(context.sleep(context.genericGroupInitialRebalanceDelayMs));

        assertTrue(responseFuture.isDone());
        assertEquals(Errors.NONE.code(), responseFuture.get().errorCode());
    }

    @Test
    public void testStaticMemberJoinAsFirstMember() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        context.createGenericGroup("group-id");

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withGroupInstanceId("group-instance-id")
            .withDefaultProtocolTypeAndProtocols()
            .build();

        context.joinGenericGroupAndCompleteJoin(request, false, true);
    }

    @Test
    public void testStaticMemberRejoinWithExplicitUnknownMemberId() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        context.createGenericGroup("group-id");

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withGroupInstanceId("group-instance-id")
            .withDefaultProtocolTypeAndProtocols()
            .withSessionTimeoutMs(5000)
            .withRebalanceTimeoutMs(5000)
            .build();

        JoinGroupResponseData response = context.joinGenericGroupAndCompleteJoin(request, false, true);
        assertEquals(Errors.NONE.code(), response.errorCode());

        CompletableFuture<JoinGroupResponseData> responseFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(
            request.setMemberId("unknown-member-id"),
            responseFuture
        );

        assertTrue(result.records().isEmpty());
        assertTrue(responseFuture.isDone());
        assertEquals(Errors.FENCED_INSTANCE_ID.code(), responseFuture.get(5, TimeUnit.SECONDS).errorCode());
    }

    @Test
    public void testJoinGroupUnknownConsumerExistingGroup() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        context.createGenericGroup("group-id");

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .withSessionTimeoutMs(5000)
            .withRebalanceTimeoutMs(5000)
            .build();

        JoinGroupResponseData response = context.joinGenericGroupAsDynamicMemberAndCompleteJoin(request);
        assertEquals(Errors.NONE.code(), response.errorCode());

        CompletableFuture<JoinGroupResponseData> responseFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(request
            .setMemberId("other-member-id"), responseFuture);

        assertTrue(result.records().isEmpty());
        assertTrue(responseFuture.isDone());
        assertEquals(Errors.UNKNOWN_MEMBER_ID.code(), responseFuture.get().errorCode());
    }

    @Test
    public void testJoinGroupUnknownConsumerNewDeadGroup() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        GenericGroup group = context.createGenericGroup("group-id");
        group.transitionTo(DEAD);

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        CompletableFuture<JoinGroupResponseData> responseFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(request, responseFuture);

        assertTrue(result.records().isEmpty());
        assertTrue(responseFuture.isDone());
        assertEquals(Errors.COORDINATOR_NOT_AVAILABLE.code(), responseFuture.get(5, TimeUnit.SECONDS).errorCode());
    }

    @Test
    public void testJoinGroupProtocolTypeIsNotProvidedWhenAnErrorOccurs() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId("member-id")
            .withDefaultProtocolTypeAndProtocols()
            .build();

        CompletableFuture<JoinGroupResponseData> responseFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(request, responseFuture);

        assertTrue(result.records().isEmpty());
        assertTrue(responseFuture.isDone());
        assertEquals(Errors.UNKNOWN_MEMBER_ID.code(), responseFuture.get().errorCode());
        assertNull(responseFuture.get().protocolType());
    }

    @Test
    public void testJoinGroupReturnsTheProtocolType() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        context.createGenericGroup("group-id");

        // Leader joins
        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        CompletableFuture<JoinGroupResponseData> leaderResponseFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(request, leaderResponseFuture);

        assertTrue(result.records().isEmpty());
        assertFalse(leaderResponseFuture.isDone());

        // Member joins
        CompletableFuture<JoinGroupResponseData> memberResponseFuture = new CompletableFuture<>();
        result = context.sendGenericGroupJoin(request, memberResponseFuture);

        assertTrue(result.records().isEmpty());
        assertFalse(memberResponseFuture.isDone());

        // Complete join group phase
        assertNoOrEmptyResult(context.sleep(context.genericGroupInitialRebalanceDelayMs));
        assertTrue(leaderResponseFuture.isDone());
        assertTrue(memberResponseFuture.isDone());

        assertEquals(Errors.NONE.code(), leaderResponseFuture.get(5, TimeUnit.SECONDS).errorCode());
        assertEquals("consumer", leaderResponseFuture.get(5, TimeUnit.SECONDS).protocolType());
        assertEquals(Errors.NONE.code(), memberResponseFuture.get(5, TimeUnit.SECONDS).errorCode());
        assertEquals("consumer", memberResponseFuture.get(5, TimeUnit.SECONDS).protocolType());
    }

    @Test
    public void testDelayInitialRebalanceByGroupInitialRebalanceDelayOnEmptyGroup() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        context.createGenericGroup("group-id");

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        CompletableFuture<JoinGroupResponseData> responseFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(request, responseFuture);

        assertTrue(result.records().isEmpty());
        assertFalse(responseFuture.isDone());

        assertNoOrEmptyResult(context.sleep(context.genericGroupInitialRebalanceDelayMs / 2));
        assertFalse(responseFuture.isDone());

        assertNoOrEmptyResult(context.sleep(context.genericGroupInitialRebalanceDelayMs / 2 + 1));
        assertTrue(responseFuture.isDone());
        assertEquals(Errors.NONE.code(), responseFuture.get(5, TimeUnit.SECONDS).errorCode());
    }

    @Test
    public void testResetRebalanceDelayWhenNewMemberJoinsGroupDuringInitialRebalance() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        context.createGenericGroup("group-id");

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .withRebalanceTimeoutMs(context.genericGroupInitialRebalanceDelayMs * 3)
            .build();

        CompletableFuture<JoinGroupResponseData> firstMemberResponseFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(request, firstMemberResponseFuture);

        assertTrue(result.records().isEmpty());
        assertFalse(firstMemberResponseFuture.isDone());

        assertNoOrEmptyResult(context.sleep(context.genericGroupInitialRebalanceDelayMs - 1));
        CompletableFuture<JoinGroupResponseData> secondMemberResponseFuture = new CompletableFuture<>();
        result = context.sendGenericGroupJoin(request, secondMemberResponseFuture);

        assertTrue(result.records().isEmpty());
        assertFalse(secondMemberResponseFuture.isDone());
        assertNoOrEmptyResult(context.sleep(2));

        // Advance clock past initial rebalance delay and verify futures are not completed.
        assertNoOrEmptyResult(context.sleep(context.genericGroupInitialRebalanceDelayMs / 2 + 1));
        assertFalse(firstMemberResponseFuture.isDone());
        assertFalse(secondMemberResponseFuture.isDone());

        // Advance clock beyond recomputed delay and make sure the futures have completed.
        assertNoOrEmptyResult(context.sleep(context.genericGroupInitialRebalanceDelayMs / 2));
        assertTrue(firstMemberResponseFuture.isDone());
        assertTrue(secondMemberResponseFuture.isDone());
        assertEquals(Errors.NONE.code(), firstMemberResponseFuture.get().errorCode());
        assertEquals(Errors.NONE.code(), secondMemberResponseFuture.get().errorCode());
    }

    @Test
    public void testDelayRebalanceUptoRebalanceTimeout() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        context.createGenericGroup("group-id");

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .withRebalanceTimeoutMs(context.genericGroupInitialRebalanceDelayMs * 2)
            .build();

        CompletableFuture<JoinGroupResponseData> firstMemberResponseFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(request, firstMemberResponseFuture);

        assertTrue(result.records().isEmpty());
        assertFalse(firstMemberResponseFuture.isDone());

        CompletableFuture<JoinGroupResponseData> secondMemberResponseFuture = new CompletableFuture<>();
        result = context.sendGenericGroupJoin(request, secondMemberResponseFuture);

        assertTrue(result.records().isEmpty());
        assertFalse(secondMemberResponseFuture.isDone());

        assertNoOrEmptyResult(context.sleep(context.genericGroupInitialRebalanceDelayMs + 1));

        CompletableFuture<JoinGroupResponseData> thirdMemberResponseFuture = new CompletableFuture<>();
        result = context.sendGenericGroupJoin(request, thirdMemberResponseFuture);

        assertTrue(result.records().isEmpty());
        assertFalse(thirdMemberResponseFuture.isDone());

        // Advance clock right before rebalance timeout.
        assertNoOrEmptyResult(context.sleep(context.genericGroupInitialRebalanceDelayMs - 1));
        assertFalse(firstMemberResponseFuture.isDone());
        assertFalse(secondMemberResponseFuture.isDone());
        assertFalse(thirdMemberResponseFuture.isDone());

        // Advance clock beyond rebalance timeout.
        assertNoOrEmptyResult(context.sleep(1));
        assertTrue(firstMemberResponseFuture.isDone());
        assertTrue(secondMemberResponseFuture.isDone());
        assertTrue(thirdMemberResponseFuture.isDone());

        assertEquals(Errors.NONE.code(), firstMemberResponseFuture.get(5, TimeUnit.SECONDS).errorCode());
        assertEquals(Errors.NONE.code(), secondMemberResponseFuture.get(5, TimeUnit.SECONDS).errorCode());
        assertEquals(Errors.NONE.code(), thirdMemberResponseFuture.get(5, TimeUnit.SECONDS).errorCode());
    }

    @Test
    public void testJoinGroupReplaceStaticMember() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        GenericGroup group = context.createGenericGroup("group-id");

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withGroupInstanceId("group-instance-id")
            .withDefaultProtocolTypeAndProtocols()
            .withSessionTimeoutMs(5000)
            .build();

        // Send join group as static member.
        CompletableFuture<JoinGroupResponseData> oldMemberResponseFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(request, oldMemberResponseFuture);

        assertTrue(result.records().isEmpty());
        assertFalse(oldMemberResponseFuture.isDone());
        assertEquals(1, group.numAwaitingJoinResponse());
        assertEquals(1, group.size());

        // Replace static member with new member id. Old member id should be fenced.
        CompletableFuture<JoinGroupResponseData> newMemberResponseFuture = new CompletableFuture<>();
        context.sendGenericGroupJoin(request, newMemberResponseFuture);

        assertTrue(result.records().isEmpty());
        assertFalse(newMemberResponseFuture.isDone());
        assertTrue(oldMemberResponseFuture.isDone());
        assertEquals(Errors.FENCED_INSTANCE_ID.code(), oldMemberResponseFuture.get().errorCode());
        assertEquals(1, group.numAwaitingJoinResponse());
        assertEquals(1, group.size());

        // Complete join for new member.
        assertNoOrEmptyResult(context.sleep(context.genericGroupInitialRebalanceDelayMs));
        assertTrue(newMemberResponseFuture.isDone());
        assertEquals(Errors.NONE.code(), newMemberResponseFuture.get(5, TimeUnit.SECONDS).errorCode());
        assertEquals(0, group.numAwaitingJoinResponse());
        assertEquals(1, group.size());
    }

    @Test
    public void testHeartbeatExpirationShouldRemovePendingMember() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        GenericGroup group = context.createGenericGroup("group-id");

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .withSessionTimeoutMs(1000)
            .build();

        CompletableFuture<JoinGroupResponseData> responseFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(request, responseFuture, true);

        assertTrue(result.records().isEmpty());
        assertTrue(responseFuture.isDone());
        assertEquals(Errors.MEMBER_ID_REQUIRED.code(), responseFuture.get().errorCode());
        assertEquals(0, group.size());
        assertEquals(1, group.numPendingJoinMembers());

        // Advance clock by session timeout. Pending member should be removed from group as heartbeat expires.
        assertNoOrEmptyResult(context.sleep(1000));
        assertEquals(0, group.numPendingJoinMembers());
    }

    @Test
    public void testHeartbeatExpirationShouldRemoveMember() throws Exception {
        // Set initial rebalance delay to simulate a long running rebalance.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withGenericGroupInitialRebalanceDelayMs(10 * 60 * 1000)
            .build();
        GenericGroup group = context.createGenericGroup("group-id");

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        CompletableFuture<JoinGroupResponseData> responseFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(request, responseFuture);

        assertTrue(result.records().isEmpty());
        assertFalse(responseFuture.isDone());
        assertEquals(1, group.size());

        String memberId = group.leaderOrNull();
        // Advance clock by new member join timeout. Member should be removed from group as heartbeat expires.
        // A group that transitions to Empty after completing join phase will generate records.
        List<ExpiredTimeout<Void, Record>> timeouts = context.sleep(context.genericGroupNewMemberJoinTimeoutMs);

        assertEquals(1, timeouts.size());
        timeouts.forEach(timeout -> {
            assertEquals(genericGroupHeartbeatKey("group-id", memberId), timeout.key);
            assertEquals(Collections.singletonList(
                newGroupMetadataRecord(group, MetadataVersion.latest())),
                timeout.result.records());
        });

        assertTrue(responseFuture.isDone());
        assertEquals(Errors.UNKNOWN_MEMBER_ID.code(), responseFuture.get().errorCode());
        assertEquals(0, group.size());
    }

    @Test
    public void testExistingMemberJoinDeadGroup() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        GenericGroup group = context.createGenericGroup("group-id");

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        JoinGroupResponseData response = context.joinGenericGroupAsDynamicMemberAndCompleteJoin(request);
        assertEquals(Errors.NONE.code(), response.errorCode());
        String memberId = response.memberId();

        assertTrue(group.hasMemberId(memberId));

        group.transitionTo(DEAD);

        CompletableFuture<JoinGroupResponseData> responseFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(request, responseFuture);

        assertTrue(result.records().isEmpty());
        assertTrue(responseFuture.isDone());
        assertEquals(Errors.COORDINATOR_NOT_AVAILABLE.code(), responseFuture.get().errorCode());
    }

    @Test
    public void testJoinGroupExistingPendingMemberWithGroupInstanceIdThrowsException() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        context.createGenericGroup("group-id");

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        CompletableFuture<JoinGroupResponseData> responseFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(request, responseFuture, true);

        assertTrue(result.records().isEmpty());
        assertTrue(responseFuture.isDone());
        assertEquals(Errors.MEMBER_ID_REQUIRED.code(), responseFuture.get().errorCode());

        String memberId = responseFuture.get(5, TimeUnit.SECONDS).memberId();

        assertThrows(IllegalStateException.class,
            () -> context.sendGenericGroupJoin(
                request.setMemberId(memberId).setGroupInstanceId("group-instance-id"),
                new CompletableFuture<>())
        );
    }

    @Test
    public void testJoinGroupExistingMemberUpdatedMetadataTriggersRebalance() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        GenericGroup group = context.createGenericGroup("group-id");

        JoinGroupRequestProtocolCollection protocols = toProtocols("range");

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withProtocolType("consumer")
            .withProtocols(protocols)
            .build();

        JoinGroupResponseData response = context.joinGenericGroupAsDynamicMemberAndCompleteJoin(request);

        assertEquals(Errors.NONE.code(), response.errorCode());
        String memberId = response.memberId();
        GenericGroupMember member = group.member(memberId);

        assertEquals(protocols, member.supportedProtocols());
        assertTrue(group.isInState(COMPLETING_REBALANCE));
        assertEquals(1, group.generationId());

        protocols = toProtocols("range", "roundrobin");

        // Send updated member metadata. This should trigger a rebalance and complete the join phase.
        CompletableFuture<JoinGroupResponseData> responseFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(
            request.setMemberId(memberId).setProtocols(protocols),
            responseFuture);

        assertTrue(result.records().isEmpty());
        assertTrue(responseFuture.isDone());

        assertTrue(responseFuture.isDone());
        assertEquals(Errors.NONE.code(), response.errorCode());
        assertTrue(group.isInState(COMPLETING_REBALANCE));
        assertEquals(2, group.generationId());
        assertEquals(protocols, member.supportedProtocols());
    }

    @Test
    public void testJoinGroupAsExistingLeaderTriggersRebalanceInStableState() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        GenericGroup group = context.createGenericGroup("group-id");

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        JoinGroupResponseData response = context.joinGenericGroupAsDynamicMemberAndCompleteJoin(request);

        assertEquals(Errors.NONE.code(), response.errorCode());
        String memberId = response.memberId();

        assertTrue(group.isInState(COMPLETING_REBALANCE));
        assertTrue(group.isLeader(memberId));
        assertEquals(1, group.generationId());

        group.transitionTo(STABLE);
        // Sending join group as leader should trigger a rebalance.
        CompletableFuture<JoinGroupResponseData> responseFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(
            request.setMemberId(memberId),
            responseFuture);

        assertTrue(result.records().isEmpty());
        assertTrue(responseFuture.isDone());
        assertEquals(Errors.NONE.code(), responseFuture.get(5, TimeUnit.SECONDS).errorCode());
        assertTrue(group.isInState(COMPLETING_REBALANCE));
        assertEquals(2, group.generationId());
    }

    @Test
    public void testJoinGroupAsExistingMemberWithUpdatedMetadataTriggersRebalanceInStableState() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        GenericGroup group = context.createGenericGroup("group-id");


        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withProtocolType("consumer")
            .withProtocols(toProtocols("range"))
            .build();

        JoinGroupResponseData leaderResponse = context.joinGenericGroupAsDynamicMemberAndCompleteJoin(request);
        assertEquals(Errors.NONE.code(), leaderResponse.errorCode());
        String leaderId = leaderResponse.leader();
        assertEquals(1, group.generationId());

        // Member joins.
        CompletableFuture<JoinGroupResponseData> memberResponseFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(request, memberResponseFuture);

        assertTrue(result.records().isEmpty());
        assertFalse(memberResponseFuture.isDone());

        // Leader also rejoins. Completes join group phase.
        CompletableFuture<JoinGroupResponseData> leaderResponseFuture = new CompletableFuture<>();
        result = context.sendGenericGroupJoin(request.setMemberId(leaderId), leaderResponseFuture);

        assertTrue(result.records().isEmpty());
        assertTrue(leaderResponseFuture.isDone());
        assertTrue(memberResponseFuture.isDone());
        assertEquals(Errors.NONE.code(), leaderResponseFuture.get().errorCode());
        assertEquals(Errors.NONE.code(), memberResponseFuture.get().errorCode());
        assertTrue(group.isInState(COMPLETING_REBALANCE));
        assertEquals(2, group.size());
        assertEquals(2, group.generationId());

        group.transitionTo(STABLE);

        // Member rejoins with updated metadata. This should trigger a rebalance.
        String memberId = memberResponseFuture.get().memberId();

        JoinGroupRequestProtocolCollection protocols = toProtocols("range", "roundrobin");

        memberResponseFuture = new CompletableFuture<>();
        result = context.sendGenericGroupJoin(request.setMemberId(memberId).setProtocols(protocols), memberResponseFuture);

        assertTrue(result.records().isEmpty());
        assertFalse(memberResponseFuture.isDone());

        // Leader rejoins. This completes the join group phase.
        leaderResponseFuture = new CompletableFuture<>();
        result = context.sendGenericGroupJoin(request.setMemberId(leaderId), leaderResponseFuture);

        assertTrue(result.records().isEmpty());
        assertTrue(leaderResponseFuture.isDone());
        assertTrue(memberResponseFuture.isDone());
        assertEquals(Errors.NONE.code(), memberResponseFuture.get(5, TimeUnit.SECONDS).errorCode());
        assertTrue(group.isInState(COMPLETING_REBALANCE));
        assertEquals(3, group.generationId());
        assertEquals(2, group.size());
    }

    @Test
    public void testJoinGroupExistingMemberDoesNotTriggerRebalanceInStableState() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        GenericGroup group = context.createGenericGroup("group-id");

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        JoinGroupResponseData leaderResponse = context.joinGenericGroupAsDynamicMemberAndCompleteJoin(request);
        assertEquals(Errors.NONE.code(), leaderResponse.errorCode());
        String leaderId = leaderResponse.leader();
        assertEquals(1, group.generationId());

        // Member joins.
        CompletableFuture<JoinGroupResponseData> memberResponseFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(request, memberResponseFuture);

        assertTrue(result.records().isEmpty());
        assertFalse(memberResponseFuture.isDone());

        // Leader also rejoins. Completes join group phase.
        CompletableFuture<JoinGroupResponseData> leaderResponseFuture = new CompletableFuture<>();
        result = context.sendGenericGroupJoin(request.setMemberId(leaderId), leaderResponseFuture);

        assertTrue(result.records().isEmpty());
        assertTrue(leaderResponseFuture.isDone());
        assertTrue(memberResponseFuture.isDone());
        assertTrue(group.isInState(COMPLETING_REBALANCE));
        assertEquals(2, group.size());
        assertEquals(2, group.generationId());

        String memberId = memberResponseFuture.get(5, TimeUnit.SECONDS).memberId();

        group.transitionTo(STABLE);

        // Member rejoins with no metadata changes. This does not trigger a rebalance.
        memberResponseFuture = new CompletableFuture<>();
        result = context.sendGenericGroupJoin(request.setMemberId(memberId), memberResponseFuture);

        assertTrue(result.records().isEmpty());
        assertTrue(memberResponseFuture.isDone());
        assertEquals(Errors.NONE.code(), memberResponseFuture.get().errorCode());
        assertEquals(2, memberResponseFuture.get().generationId());
        assertTrue(group.isInState(STABLE));
    }

    @Test
    public void testJoinGroupExistingMemberInEmptyState() throws Exception {
        // Existing member joins a group that is in Empty/Dead state. Ask member to rejoin with generation id reset.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        GenericGroup group = context.createGenericGroup("group-id");

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        JoinGroupResponseData response = context.joinGenericGroupAsDynamicMemberAndCompleteJoin(request);

        assertEquals(Errors.NONE.code(), response.errorCode());
        String memberId = response.memberId();

        assertTrue(group.isInState(COMPLETING_REBALANCE));

        group.transitionTo(PREPARING_REBALANCE);
        group.transitionTo(EMPTY);

        CompletableFuture<JoinGroupResponseData> responseFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(request.setMemberId(memberId), responseFuture);

        assertTrue(result.records().isEmpty());
        assertTrue(responseFuture.isDone());
        assertEquals(Errors.UNKNOWN_MEMBER_ID.code(), responseFuture.get(5, TimeUnit.SECONDS).errorCode());
        assertEquals(-1, responseFuture.get(5, TimeUnit.SECONDS).generationId());
    }

    @Test
    public void testCompleteJoinRemoveNotYetRejoinedDynamicMembers() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        GenericGroup group = context.createGenericGroup("group-id");

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .withSessionTimeoutMs(1000)
            .withRebalanceTimeoutMs(1000)
            .build();

        JoinGroupResponseData leaderResponse = context.joinGenericGroupAsDynamicMemberAndCompleteJoin(request);
        assertEquals(Errors.NONE.code(), leaderResponse.errorCode());
        assertEquals(1, group.generationId());

        // Add new member. This triggers a rebalance.
        CompletableFuture<JoinGroupResponseData> memberResponseFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(request, memberResponseFuture);

        assertTrue(result.records().isEmpty());
        assertFalse(memberResponseFuture.isDone());
        assertEquals(2, group.size());
        assertTrue(group.isInState(PREPARING_REBALANCE));

        // Advance clock by rebalance timeout. This will expire the leader as it has not rejoined.
        assertNoOrEmptyResult(context.sleep(1000));

        assertTrue(memberResponseFuture.isDone());
        assertEquals(Errors.NONE.code(), memberResponseFuture.get(5, TimeUnit.SECONDS).errorCode());
        assertEquals(1, group.size());
        assertTrue(group.hasMemberId(memberResponseFuture.get(5, TimeUnit.SECONDS).memberId()));
        assertEquals(2, group.generationId());
    }

    @Test
    public void testCompleteJoinPhaseInEmptyStateSkipsRebalance() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        GenericGroup group = context.createGenericGroup("group-id");

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .withSessionTimeoutMs(1000)
            .withRebalanceTimeoutMs(1000)
            .build();

        CompletableFuture<JoinGroupResponseData> responseFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(request, responseFuture);

        assertTrue(result.records().isEmpty());
        assertFalse(responseFuture.isDone());

        assertEquals(0, group.generationId());
        assertTrue(group.isInState(PREPARING_REBALANCE));
        group.transitionTo(DEAD);

        // Advance clock by initial rebalance delay to complete join phase.
        assertNoOrEmptyResult(context.sleep(context.genericGroupInitialRebalanceDelayMs));
        assertEquals(0, group.generationId());
    }

    @Test
    public void testCompleteJoinPhaseNoMembersRejoinedExtendsJoinPhase() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        GenericGroup group = context.createGenericGroup("group-id");

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("first-instance-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .withSessionTimeoutMs(30000)
            .withRebalanceTimeoutMs(10000)
            .build();

        // First member joins group and completes join phase.
        JoinGroupResponseData firstMemberResponse = context.joinGenericGroupAndCompleteJoin(request, true, true);
        assertEquals(Errors.NONE.code(), firstMemberResponse.errorCode());
        String firstMemberId = firstMemberResponse.memberId();

        // Second member joins and group goes into rebalancing state.
        CompletableFuture<JoinGroupResponseData> secondMemberResponseFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(
            request.setGroupInstanceId("second-instance-id"), secondMemberResponseFuture);

        assertTrue(result.records().isEmpty());
        assertFalse(secondMemberResponseFuture.isDone());

        // First static member rejoins and completes join phase.
        CompletableFuture<JoinGroupResponseData> firstMemberResponseFuture = new CompletableFuture<>();
        result = context.sendGenericGroupJoin(
            request.setMemberId(firstMemberId).setGroupInstanceId("first-instance-id"),
            firstMemberResponseFuture);

        assertTrue(result.records().isEmpty());
        assertTrue(firstMemberResponseFuture.isDone());
        assertTrue(secondMemberResponseFuture.isDone());
        assertEquals(Errors.NONE.code(), firstMemberResponseFuture.get(5, TimeUnit.SECONDS).errorCode());
        assertEquals(Errors.NONE.code(), secondMemberResponseFuture.get(5, TimeUnit.SECONDS).errorCode());
        assertEquals(2, group.size());
        assertEquals(2, group.generationId());

        String secondMemberId = secondMemberResponseFuture.get(5, TimeUnit.SECONDS).memberId();

        // Trigger a rebalance. No members rejoined.
        context.groupMetadataManager.prepareRebalance(group, "trigger rebalance");

        assertEquals(2, group.size());
        assertTrue(group.isInState(PREPARING_REBALANCE));
        assertEquals(0, group.numAwaitingJoinResponse());

        // Advance clock by rebalance timeout to complete join phase. As long as both members have not
        // rejoined, we extend the join phase.
        assertNoOrEmptyResult(context.sleep(10000));
        assertEquals(10000, context.timer.timeout("join-group-id").deadlineMs - context.time.milliseconds());
        assertNoOrEmptyResult(context.sleep(10000));
        assertEquals(10000, context.timer.timeout("join-group-id").deadlineMs - context.time.milliseconds());

        assertTrue(group.isInState(PREPARING_REBALANCE));
        assertEquals(2, group.size());
        assertEquals(2, group.generationId());

        // Let first and second member rejoin. This should complete the join phase.
        firstMemberResponseFuture = new CompletableFuture<>();
        result = context.sendGenericGroupJoin(
            request.setMemberId(firstMemberId).setGroupInstanceId("first-instance-id"),
            firstMemberResponseFuture);

        assertTrue(result.records().isEmpty());
        assertFalse(firstMemberResponseFuture.isDone());
        assertTrue(group.isInState(PREPARING_REBALANCE));
        assertEquals(2, group.size());
        assertEquals(2, group.generationId());

        secondMemberResponseFuture = new CompletableFuture<>();
        result = context.sendGenericGroupJoin(
            request.setMemberId(secondMemberId).setGroupInstanceId("second-instance-id"),
            secondMemberResponseFuture);

        assertTrue(result.records().isEmpty());
        assertTrue(firstMemberResponseFuture.isDone());
        assertTrue(secondMemberResponseFuture.isDone());
        assertEquals(Errors.NONE.code(), firstMemberResponseFuture.get(5, TimeUnit.SECONDS).errorCode());
        assertEquals(Errors.NONE.code(), secondMemberResponseFuture.get(5, TimeUnit.SECONDS).errorCode());
        assertTrue(group.isInState(COMPLETING_REBALANCE));
        assertEquals(2, group.size());
        assertEquals(3, group.generationId());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testReplaceStaticMemberInStableStateNoError(
        boolean supportSkippingAssignment
    ) throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        GenericGroup group = context.createGenericGroup("group-id");

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("group-instance-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withProtocolType("consumer")
            .withProtocols(toProtocols("range"))
            .build();

        JoinGroupResponseData response = context.joinGenericGroupAndCompleteJoin(request, true, supportSkippingAssignment);
        assertEquals(Errors.NONE.code(), response.errorCode());
        String oldMemberId = response.memberId();

        assertEquals(1, group.size());
        assertEquals(1, group.generationId());
        assertTrue(group.isInState(COMPLETING_REBALANCE));

        // Simulate successful sync group phase
        group.transitionTo(STABLE);

        // Static member rejoins with UNKNOWN_MEMBER_ID. This should update the log with the generated member id.
        JoinGroupRequestProtocolCollection protocols = toProtocols("range", "roundrobin");

        CompletableFuture<JoinGroupResponseData> responseFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(
            request
                .setProtocols(protocols)
                .setRebalanceTimeoutMs(7000)
                .setSessionTimeoutMs(4500),
            responseFuture,
            true,
            supportSkippingAssignment);

        assertEquals(
            Collections.singletonList(newGroupMetadataRecord(group, MetadataVersion.latest())),
            result.records()
        );
        assertFalse(responseFuture.isDone());

        // Write was successful.
        result.appendFuture().complete(null);
        assertTrue(responseFuture.isDone());

        String newMemberId = group.staticMemberId("group-instance-id");

        JoinGroupResponseData expectedResponse = new JoinGroupResponseData()
            .setMembers(Collections.emptyList())
            .setLeader(oldMemberId)
            .setMemberId(newMemberId)
            .setGenerationId(1)
            .setProtocolType("consumer")
            .setProtocolName("range")
            .setSkipAssignment(supportSkippingAssignment)
            .setErrorCode(Errors.NONE.code());

        if (supportSkippingAssignment) {
            expectedResponse
                .setMembers(Collections.singletonList(
                    new JoinGroupResponseData.JoinGroupResponseMember()
                        .setMemberId(newMemberId)
                        .setGroupInstanceId("group-instance-id")
                        .setMetadata(protocols.find("range").metadata())
                    ))
                .setLeader(newMemberId);
        }

        GenericGroupMember updatedMember = group.member(group.staticMemberId("group-instance-id"));

        assertEquals(expectedResponse, responseFuture.get());
        assertEquals(newMemberId, updatedMember.memberId());
        assertEquals(Optional.of("group-instance-id"), updatedMember.groupInstanceId());
        assertEquals(7000, updatedMember.rebalanceTimeoutMs());
        assertEquals(4500, updatedMember.sessionTimeoutMs());
        assertEquals(protocols, updatedMember.supportedProtocols());

        assertEquals(1, group.size());
        assertEquals(1, group.generationId());
        assertTrue(group.isInState(STABLE));
    }

    @Test
    public void testReplaceStaticMemberInStableStateWithUpdatedProtocolTriggersRebalance() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        GenericGroup group = context.createGenericGroup("group-id");

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("group-instance-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withProtocolType("consumer")
            .withProtocols(toProtocols("range", "roundrobin"))
            .build();

        JoinGroupResponseData response = context.joinGenericGroupAndCompleteJoin(request, true, true);
        assertEquals(Errors.NONE.code(), response.errorCode());
        assertEquals(1, group.size());
        assertEquals(1, group.generationId());
        assertTrue(group.isInState(COMPLETING_REBALANCE));

        // Simulate successful sync group phase
        group.transitionTo(STABLE);

        // Static member rejoins with UNKNOWN_MEMBER_ID. The selected protocol changes and triggers a rebalance.

        CompletableFuture<JoinGroupResponseData> responseFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(
            request.setProtocols(toProtocols("roundrobin")),
            responseFuture
        );

        assertTrue(result.records().isEmpty());
        assertTrue(responseFuture.isDone());
        assertEquals(Errors.NONE.code(), responseFuture.get(5, TimeUnit.SECONDS).errorCode());
        assertEquals(1, group.size());
        assertEquals(2, group.generationId());
        assertTrue(group.isInState(COMPLETING_REBALANCE));
    }

    @Test
    public void testReplaceStaticMemberInStableStateErrors() throws Exception {
        // If the append future fails, confirm that the member is not updated.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        GenericGroup group = context.createGenericGroup("group-id");

        JoinGroupRequestProtocolCollection protocols = toProtocols("range");

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("group-instance-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withProtocolType("consumer")
            .withProtocols(protocols)
            .withRebalanceTimeoutMs(4000)
            .withSessionTimeoutMs(3000)
            .build();

        JoinGroupResponseData response = context.joinGenericGroupAndCompleteJoin(request, false, false);
        assertEquals(Errors.NONE.code(), response.errorCode());
        assertEquals(1, group.size());
        assertEquals(1, group.generationId());
        assertTrue(group.isInState(COMPLETING_REBALANCE));

        String oldMemberId = response.memberId();
        // Simulate successful sync group phase
        group.transitionTo(STABLE);

        // Static member rejoins with UNKNOWN_MEMBER_ID but the append fails. The group should not be updated.
        protocols.add(new JoinGroupRequestProtocol()
                .setName("roundrobin")
                .setMetadata(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                    Collections.singletonList("bar"))).array()));

        CompletableFuture<JoinGroupResponseData> responseFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(
            request
                .setProtocols(protocols)
                .setRebalanceTimeoutMs(7000)
                .setSessionTimeoutMs(6000),
            responseFuture,
            false,
            false);

        assertEquals(
            Collections.singletonList(newGroupMetadataRecord(group, MetadataVersion.latest())),
            result.records()
        );
        assertFalse(responseFuture.isDone());

        // Simulate a failed write to the log.
        result.appendFuture().completeExceptionally(new UnknownTopicOrPartitionException());
        assertTrue(responseFuture.isDone());

        JoinGroupResponseData expectedResponse = new JoinGroupResponseData()
            .setMembers(Collections.emptyList())
            .setLeader(oldMemberId)
            .setMemberId(UNKNOWN_MEMBER_ID)
            .setGenerationId(1)
            .setProtocolType("consumer")
            .setProtocolName("range")
            .setSkipAssignment(false)
            .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code());

        assertEquals(expectedResponse, responseFuture.get());

        GenericGroupMember revertedMember = group.member(group.staticMemberId("group-instance-id"));

        assertEquals(oldMemberId, revertedMember.memberId());
        assertEquals(Optional.of("group-instance-id"), revertedMember.groupInstanceId());
        assertEquals(4000, revertedMember.rebalanceTimeoutMs());
        assertEquals(3000, revertedMember.sessionTimeoutMs());
        assertEquals(protocols, revertedMember.supportedProtocols());
        assertEquals(1, group.size());
        assertEquals(1, group.generationId());
        assertTrue(group.isInState(STABLE));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testReplaceStaticMemberInStableStateSucceeds(
        boolean supportSkippingAssignment
    ) throws Exception {
        // If the append future succeeds, the soft state is updated with the new member.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        GenericGroup group = context.createGenericGroup("group-id");

        JoinGroupRequestProtocolCollection protocols = toProtocols("range");

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("group-instance-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withProtocolType("consumer")
            .withProtocols(protocols)
            .build();

        JoinGroupResponseData response = context.joinGenericGroupAndCompleteJoin(
            request,
            true,
            supportSkippingAssignment
        );

        assertEquals(Errors.NONE.code(), response.errorCode());
        assertEquals(1, group.size());
        assertEquals(1, group.generationId());
        assertTrue(group.isInState(COMPLETING_REBALANCE));

        String oldMemberId = response.memberId();
        // Simulate successful sync group phase
        group.transitionTo(STABLE);

        // Static member rejoins with UNKNOWN_MEMBER_ID and the append succeeds.
        protocols.add(new JoinGroupRequestProtocol()
            .setName("roundrobin")
            .setMetadata(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                Collections.singletonList("bar"))).array()));

        CompletableFuture<JoinGroupResponseData> responseFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(
            request
                .setProtocols(protocols)
                .setRebalanceTimeoutMs(7000)
                .setSessionTimeoutMs(6000),
            responseFuture,
            true,
            supportSkippingAssignment);

        assertEquals(
            Collections.singletonList(newGroupMetadataRecord(group, MetadataVersion.latest())),
            result.records()
        );
        assertFalse(responseFuture.isDone());

        // Simulate a successful write to the log.
        result.appendFuture().complete(null);
        assertTrue(responseFuture.isDone());

        JoinGroupResponseData expectedResponse = new JoinGroupResponseData()
            .setMembers(supportSkippingAssignment ? toJoinResponseMembers(group) : Collections.emptyList())
            .setLeader(supportSkippingAssignment ? responseFuture.get().memberId() : oldMemberId)
            .setMemberId(responseFuture.get().memberId())
            .setGenerationId(1)
            .setProtocolType("consumer")
            .setProtocolName("range")
            .setSkipAssignment(supportSkippingAssignment)
            .setErrorCode(Errors.NONE.code());

        assertEquals(expectedResponse, responseFuture.get());

        GenericGroupMember newMember = group.member(group.staticMemberId("group-instance-id"));

        assertNotEquals(oldMemberId, newMember.memberId());
        assertEquals(Optional.of("group-instance-id"), newMember.groupInstanceId());
        assertEquals(7000, newMember.rebalanceTimeoutMs());
        assertEquals(6000, newMember.sessionTimeoutMs());
        assertEquals(protocols, newMember.supportedProtocols());
        assertEquals(1, group.size());
        assertEquals(1, group.generationId());
        assertTrue(group.isInState(STABLE));
    }

    @Test
    public void testReplaceStaticMemberInCompletingRebalanceStateTriggersRebalance() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        GenericGroup group = context.createGenericGroup("group-id");

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("group-instance-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        JoinGroupResponseData response = context.joinGenericGroupAndCompleteJoin(request, true, true);
        assertEquals(Errors.NONE.code(), response.errorCode());

        assertEquals(1, group.size());
        assertEquals(1, group.generationId());
        assertTrue(group.isInState(COMPLETING_REBALANCE));

        // Static member rejoins with UNKNOWN_MEMBER_ID and triggers a rebalance.
        CompletableFuture<JoinGroupResponseData> responseFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(request, responseFuture);

        assertTrue(result.records().isEmpty());
        assertTrue(responseFuture.isDone());
        assertEquals(Errors.NONE.code(), responseFuture.get(5, TimeUnit.SECONDS).errorCode());
        assertEquals(1, group.size());
        assertEquals(2, group.generationId());
        assertTrue(group.isInState(COMPLETING_REBALANCE));
    }

    @Test
    public void testJoinGroupAppendErrorConversion() {
        assertEquals(Errors.COORDINATOR_NOT_AVAILABLE, appendGroupMetadataErrorToResponseError(Errors.UNKNOWN_TOPIC_OR_PARTITION));
        assertEquals(Errors.COORDINATOR_NOT_AVAILABLE, appendGroupMetadataErrorToResponseError(Errors.NOT_ENOUGH_REPLICAS));

        assertEquals(Errors.NOT_COORDINATOR, appendGroupMetadataErrorToResponseError(Errors.NOT_LEADER_OR_FOLLOWER));
        assertEquals(Errors.NOT_COORDINATOR, appendGroupMetadataErrorToResponseError(Errors.KAFKA_STORAGE_ERROR));

        assertEquals(Errors.UNKNOWN_SERVER_ERROR, appendGroupMetadataErrorToResponseError(Errors.MESSAGE_TOO_LARGE));
        assertEquals(Errors.UNKNOWN_SERVER_ERROR, appendGroupMetadataErrorToResponseError(Errors.RECORD_LIST_TOO_LARGE));
        assertEquals(Errors.UNKNOWN_SERVER_ERROR, appendGroupMetadataErrorToResponseError(Errors.INVALID_FETCH_SIZE));

        assertEquals(Errors.LEADER_NOT_AVAILABLE, Errors.LEADER_NOT_AVAILABLE);
    }

    public static <T> void assertUnorderedListEquals(
        List<T> expected,
        List<T> actual
    ) {
        assertEquals(new HashSet<>(expected), new HashSet<>(actual));
    }

    private void assertResponseEquals(
        ConsumerGroupHeartbeatResponseData expected,
        ConsumerGroupHeartbeatResponseData actual
    ) {
        if (!responseEquals(expected, actual)) {
            assertionFailure()
                .expected(expected)
                .actual(actual)
                .buildAndThrow();
        }
    }

    private boolean responseEquals(
        ConsumerGroupHeartbeatResponseData expected,
        ConsumerGroupHeartbeatResponseData actual
    ) {
        if (expected.throttleTimeMs() != actual.throttleTimeMs()) return false;
        if (expected.errorCode() != actual.errorCode()) return false;
        if (!Objects.equals(expected.errorMessage(), actual.errorMessage())) return false;
        if (!Objects.equals(expected.memberId(), actual.memberId())) return false;
        if (expected.memberEpoch() != actual.memberEpoch()) return false;
        if (expected.shouldComputeAssignment() != actual.shouldComputeAssignment()) return false;
        if (expected.heartbeatIntervalMs() != actual.heartbeatIntervalMs()) return false;
        // Unordered comparison of the assignments.
        return responseAssignmentEquals(expected.assignment(), actual.assignment());
    }

    private boolean responseAssignmentEquals(
        ConsumerGroupHeartbeatResponseData.Assignment expected,
        ConsumerGroupHeartbeatResponseData.Assignment actual
    ) {
        if (expected == actual) return true;
        if (expected == null) return false;
        if (actual == null) return false;

        if (!Objects.equals(fromAssignment(expected.pendingTopicPartitions()), fromAssignment(actual.pendingTopicPartitions())))
            return false;

        return Objects.equals(fromAssignment(expected.assignedTopicPartitions()), fromAssignment(actual.assignedTopicPartitions()));
    }

    private Map<Uuid, Set<Integer>> fromAssignment(
        List<ConsumerGroupHeartbeatResponseData.TopicPartitions> assignment
    ) {
        if (assignment == null) return null;

        Map<Uuid, Set<Integer>> assignmentMap = new HashMap<>();
        assignment.forEach(topicPartitions -> {
            assignmentMap.put(topicPartitions.topicId(), new HashSet<>(topicPartitions.partitions()));
        });
        return assignmentMap;
    }

    private List<String> verifyGenericGroupJoinResponses(
        List<CompletableFuture<JoinGroupResponseData>> responseFutures,
        int expectedSuccessCount,
        Errors expectedFailure
    ) {
        int successCount = 0;
        List<String> memberIds = new ArrayList<>();
        for (CompletableFuture<JoinGroupResponseData> responseFuture : responseFutures) {
            if (!responseFuture.isDone()) {
                fail("All responseFutures should be completed.");
            }
            try {
                JoinGroupResponseData joinResponse = responseFuture.get();
                if (joinResponse.errorCode() == Errors.NONE.code()) {
                    successCount++;
                } else {
                    assertEquals(expectedFailure.code(), joinResponse.errorCode());
                }
                memberIds.add(joinResponse.memberId());
            } catch (Exception e) {
                fail("Unexpected exception: " + e.getMessage());
            }
        }

        assertEquals(expectedSuccessCount, successCount);
        return memberIds;
    }

    private static void assertNoOrEmptyResult(List<ExpiredTimeout<Void, Record>> timeouts) {
        assertTrue(timeouts.size() <= 1);
        timeouts.forEach(timeout -> assertEquals(EMPTY_RESULT, timeout.result));
    }

    private static class JoinGroupRequestBuilder {
        String groupId = null;
        String groupInstanceId = null;
        String memberId = null;
        String protocolType = "consumer";
        JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestProtocolCollection(0);
        int sessionTimeoutMs = 500;
        int rebalanceTimeoutMs = 500;
        String reason = null;

        JoinGroupRequestBuilder withGroupId(String groupId) {
            this.groupId = groupId;
            return this;
        }

        JoinGroupRequestBuilder withGroupInstanceId(String groupInstanceId) {
            this.groupInstanceId = groupInstanceId;
            return this;
        }

        JoinGroupRequestBuilder withMemberId(String memberId) {
            this.memberId = memberId;
            return this;
        }

        JoinGroupRequestBuilder withDefaultProtocolTypeAndProtocols() {
            this.protocols = toProtocols("range");
            return this;
        }

        JoinGroupRequestBuilder withProtocolSuperset() {
            this.protocols = toProtocols("range", "roundrobin");
            return this;
        }

        JoinGroupRequestBuilder withProtocolType(String protocolType) {
            this.protocolType = protocolType;
            return this;
        }

        JoinGroupRequestBuilder withProtocols(JoinGroupRequestProtocolCollection protocols) {
            this.protocols = protocols;
            return this;
        }

        JoinGroupRequestBuilder withRebalanceTimeoutMs(int rebalanceTimeoutMs) {
            this.rebalanceTimeoutMs = rebalanceTimeoutMs;
            return this;
        }

        JoinGroupRequestBuilder withSessionTimeoutMs(int sessionTimeoutMs) {
            this.sessionTimeoutMs = sessionTimeoutMs;
            return this;
        }

        JoinGroupRequestBuilder withReason(String reason) {
            this.reason = reason;
            return this;
        }

        JoinGroupRequestData build() {
            return new JoinGroupRequestData()
                .setGroupId(groupId)
                .setGroupInstanceId(groupInstanceId)
                .setMemberId(memberId)
                .setProtocolType(protocolType)
                .setProtocols(protocols)
                .setRebalanceTimeoutMs(rebalanceTimeoutMs)
                .setSessionTimeoutMs(sessionTimeoutMs)
                .setReason(reason);
        }
    }

    private static Record newGroupMetadataRecord(
        String groupId,
        GroupMetadataValue value,
        MetadataVersion metadataVersion
    ) {
        return new Record(
            new ApiMessageAndVersion(
                new GroupMetadataKey()
                    .setGroup(groupId),
                (short) 2
            ),
            new ApiMessageAndVersion(
                value,
                metadataVersion.groupMetadataValueVersion()
            )
        );
    }

    @Test
    public void testNewMemberTimeoutCompletion() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        GenericGroup group = context.createGenericGroup("group-id");

        CompletableFuture<JoinGroupResponseData> joinFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .withSessionTimeoutMs(context.genericGroupNewMemberJoinTimeoutMs + 5000)
            .build(),
            joinFuture);

        assertTrue(result.records().isEmpty());
        assertFalse(joinFuture.isDone());

        // Advance clock by initial rebalance delay to complete join phase.
        assertNoOrEmptyResult(context.sleep(context.genericGroupInitialRebalanceDelayMs));

        assertTrue(joinFuture.isDone());
        assertEquals(Errors.NONE.code(), joinFuture.get().errorCode());
        assertEquals(1, group.generationId());
        assertTrue(group.isInState(COMPLETING_REBALANCE));

        assertEquals(0, group.allMembers().stream().filter(GenericGroupMember::isNew).count());

        SyncResult syncResult = context.sendGenericGroupSync(new SyncGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(joinFuture.get().memberId())
            .withGenerationId(joinFuture.get().generationId())
            .build());

        // Simulate a successful write to the log.
        syncResult.appendFuture.complete(null);

        assertTrue(syncResult.syncFuture.isDone());
        assertEquals(Errors.NONE.code(), syncResult.syncFuture.get().errorCode());
        assertEquals(1, group.size());

        // Make sure the NewMemberTimeout is not still in effect, and the member is not kicked
        assertNoOrEmptyResult(context.sleep(context.genericGroupNewMemberJoinTimeoutMs));
        assertEquals(1, group.size());

        // Member should be removed as heartbeat expires. The group is now empty.
        List<ExpiredTimeout<Void, Record>> timeouts = context.sleep(5000);
        List<Record> expectedRecords = Collections.singletonList(newGroupMetadataRecord(
            group.groupId(),
            new GroupMetadataValue()
                .setMembers(Collections.emptyList())
                .setGeneration(2)
                .setLeader(null)
                .setProtocolType("consumer")
                .setProtocol(null)
                .setCurrentStateTimestamp(context.time.milliseconds()),
            MetadataVersion.latest()));

        assertEquals(1, timeouts.size());
        String memberId = joinFuture.get().memberId();
        timeouts.forEach(timeout -> {
            assertEquals(genericGroupHeartbeatKey("group-id", memberId), timeout.key);
            assertEquals(expectedRecords, timeout.result.records());
        });

        assertEquals(0, group.size());
        assertTrue(group.isInState(EMPTY));
    }

    @Test
    public void testNewMemberFailureAfterJoinGroupCompletion() throws Exception {
        // For old versions of the JoinGroup protocol, new members were subject
        // to expiration if the rebalance took long enough. This test case ensures
        // that following completion of the JoinGroup phase, new members follow
        // normal heartbeat expiration logic.

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        GenericGroup group = context.createGenericGroup("group-id");

        JoinGroupRequestData joinRequest = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .withSessionTimeoutMs(5000)
            .withRebalanceTimeoutMs(10000)
            .build();

        JoinGroupResponseData joinResponse = context.joinGenericGroupAndCompleteJoin(joinRequest, false, false);
        assertEquals(Errors.NONE.code(), joinResponse.errorCode());

        String memberId = joinResponse.memberId();
        assertEquals(memberId, joinResponse.leader());
        assertEquals(1, joinResponse.generationId());

        SyncResult syncResult = context.sendGenericGroupSync(new SyncGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(memberId)
            .withGenerationId(1)
            .build());

        // Simulate a successful write to the log.
        syncResult.appendFuture.complete(null);

        assertTrue(syncResult.syncFuture.isDone());
        assertEquals(Errors.NONE.code(), syncResult.syncFuture.get().errorCode());

        assertTrue(group.isInState(STABLE));
        assertEquals(1, group.generationId());

        CompletableFuture<JoinGroupResponseData> otherJoinFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> otherJoinResult = context.sendGenericGroupJoin(
            joinRequest
                .setMemberId(UNKNOWN_MEMBER_ID),
            otherJoinFuture);

        CompletableFuture<JoinGroupResponseData> joinFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> joinResult = context.sendGenericGroupJoin(
            joinRequest
                .setMemberId(memberId),
            joinFuture);

        assertTrue(otherJoinResult.records().isEmpty());
        assertTrue(joinResult.records().isEmpty());
        assertTrue(joinFuture.isDone());
        assertTrue(otherJoinFuture.isDone());
        assertEquals(Errors.NONE.code(), joinFuture.get().errorCode());
        assertEquals(Errors.NONE.code(), otherJoinFuture.get().errorCode());

        context.verifySessionExpiration(group, 5000);
    }

    @Test
    public void testStaticMemberFenceDuplicateRejoinedFollower() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        RebalanceResult rebalanceResult = context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id"
        );
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup("group-id", false);

        // A third member joins. Trigger a rebalance.
        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        context.sendGenericGroupJoin(request, new CompletableFuture<>());

        assertTrue(group.isInState(PREPARING_REBALANCE));

        // Old follower rejoins group will be matching current member.id.
        CompletableFuture<JoinGroupResponseData> oldFollowerJoinFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(
            request
                .setMemberId(rebalanceResult.followerId)
                .setGroupInstanceId("follower-instance-id"),
            oldFollowerJoinFuture);

        assertTrue(result.records().isEmpty());
        assertFalse(oldFollowerJoinFuture.isDone());

        // Duplicate follower joins group with unknown member id will trigger member id replacement.
        result = context.sendGenericGroupJoin(
            request.setMemberId(UNKNOWN_MEMBER_ID).setGroupInstanceId("follower-instance-id"),
            new CompletableFuture<>());

        // Old member shall be fenced immediately upon duplicate follower joins.
        assertTrue(result.records().isEmpty());
        assertTrue(oldFollowerJoinFuture.isDone());

        JoinGroupResponseData expectedResponse = new JoinGroupResponseData()
            .setErrorCode(Errors.FENCED_INSTANCE_ID.code())
            .setProtocolName(null)
            .setProtocolType(null)
            .setLeader(UNKNOWN_MEMBER_ID)
            .setMemberId(rebalanceResult.followerId)
            .setGenerationId(-1);

        checkJoinGroupResponse(
            expectedResponse,
            oldFollowerJoinFuture.get(),
            group,
            PREPARING_REBALANCE,
            Collections.emptySet()
        );
    }

    @Test
    public void testStaticMemberFenceDuplicateSyncingFollowerAfterMemberIdChanged() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        RebalanceResult rebalanceResult = context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id"
        );
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup("group-id", false);

        // Known leader rejoins will trigger rebalance.
        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("leader-instance-id")
            .withMemberId(rebalanceResult.leaderId)
            .withProtocolSuperset()
            .withRebalanceTimeoutMs(10000)
            .build();

        CompletableFuture<JoinGroupResponseData> leaderJoinFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(request, leaderJoinFuture);

        assertTrue(result.records().isEmpty());
        assertFalse(leaderJoinFuture.isDone());
        assertTrue(group.isInState(PREPARING_REBALANCE));

        // Old follower rejoins group will match current member.id.
        CompletableFuture<JoinGroupResponseData> oldFollowerJoinFuture = new CompletableFuture<>();
        result = context.sendGenericGroupJoin(
                request.setMemberId(rebalanceResult.followerId).setGroupInstanceId("follower-instance-id"),
                oldFollowerJoinFuture);

        assertTrue(result.records().isEmpty());
        assertTrue(oldFollowerJoinFuture.isDone());
        assertTrue(leaderJoinFuture.isDone());

        JoinGroupResponseData expectedLeaderResponse = new JoinGroupResponseData()
            .setErrorCode(Errors.NONE.code())
            .setGenerationId(rebalanceResult.generationId + 1)
            .setMemberId(rebalanceResult.leaderId)
            .setLeader(rebalanceResult.leaderId)
            .setProtocolName("range")
            .setProtocolType("consumer")
            .setMembers(toJoinResponseMembers(group));

        checkJoinGroupResponse(
            expectedLeaderResponse,
            leaderJoinFuture.get(),
            group,
            COMPLETING_REBALANCE,
            mkSet("leader-instance-id", "follower-instance-id")
        );

        assertEquals(rebalanceResult.leaderId, leaderJoinFuture.get().memberId());
        assertEquals(rebalanceResult.leaderId, leaderJoinFuture.get().leader());

        // Old follower should get a successful join group response.
        assertTrue(oldFollowerJoinFuture.isDone());

        JoinGroupResponseData expectedFollowerResponse = new JoinGroupResponseData()
            .setErrorCode(Errors.NONE.code())
            .setGenerationId(rebalanceResult.generationId + 1)
            .setMemberId(oldFollowerJoinFuture.get().memberId())
            .setLeader(rebalanceResult.leaderId)
            .setProtocolName("range")
            .setProtocolType("consumer");

        checkJoinGroupResponse(
            expectedFollowerResponse,
            oldFollowerJoinFuture.get(),
            group,
            COMPLETING_REBALANCE,
            Collections.emptySet()
        );

        assertTrue(group.isInState(COMPLETING_REBALANCE));
        assertEquals(rebalanceResult.followerId, oldFollowerJoinFuture.get().memberId());
        assertEquals(rebalanceResult.leaderId, oldFollowerJoinFuture.get().leader());

        // Duplicate follower joins group with unknown member id will trigger member.id replacement,
        // and will also trigger a rebalance under CompletingRebalance state; the old follower sync callback
        // will return fenced exception while broker replaces the member identity with the duplicate follower joins.
        SyncResult oldFollowerSyncResult = context.sendGenericGroupSync(new SyncGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("follower-instance-id")
            .withGenerationId(oldFollowerJoinFuture.get().generationId())
            .withMemberId(oldFollowerJoinFuture.get().memberId())
            .build());

        assertTrue(result.records().isEmpty());
        assertFalse(oldFollowerSyncResult.syncFuture.isDone());

        CompletableFuture<JoinGroupResponseData> duplicateFollowerJoinFuture = new CompletableFuture<>();
        result = context.sendGenericGroupJoin(
            request.setMemberId(UNKNOWN_MEMBER_ID).setGroupInstanceId("follower-instance-id"),
            duplicateFollowerJoinFuture);

        assertTrue(result.records().isEmpty());
        assertTrue(group.isInState(PREPARING_REBALANCE));
        assertFalse(duplicateFollowerJoinFuture.isDone());
        assertTrue(oldFollowerSyncResult.syncFuture.isDone());
        assertEquals(Errors.FENCED_INSTANCE_ID.code(), oldFollowerSyncResult.syncFuture.get().errorCode());

        // Advance clock by rebalance timeout so that the join phase completes with duplicate follower.
        // Both heartbeats will expire but only the leader is kicked out.
        List<ExpiredTimeout<Void, Record>> timeouts = context.sleep(10000);
        assertEquals(2, timeouts.size());
        timeouts.forEach(timeout -> assertEquals(timeout.result, EMPTY_RESULT));

        assertTrue(duplicateFollowerJoinFuture.isDone());
        assertTrue(group.isInState(COMPLETING_REBALANCE));
        assertEquals(3, group.generationId());
        assertEquals(1, group.size());
        assertTrue(group.hasMemberId(duplicateFollowerJoinFuture.get().memberId()));
        assertEquals(duplicateFollowerJoinFuture.get().memberId(), duplicateFollowerJoinFuture.get().leader());
    }

    @Test
    public void testStaticMemberFenceDuplicateRejoiningFollowerAfterMemberIdChanged() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        RebalanceResult rebalanceResult = context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id"
        );
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup("group-id", false);

        // Known leader rejoins will trigger rebalance.
        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("leader-instance-id")
            .withMemberId(rebalanceResult.leaderId)
            .withDefaultProtocolTypeAndProtocols()
            .withRebalanceTimeoutMs(10000)
            .build();

        CompletableFuture<JoinGroupResponseData> leaderJoinFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(request, leaderJoinFuture);

        assertTrue(result.records().isEmpty());
        assertFalse(leaderJoinFuture.isDone());
        assertTrue(group.isInState(PREPARING_REBALANCE));

        // Duplicate follower joins group will trigger member id replacement.
        CompletableFuture<JoinGroupResponseData> duplicateFollowerJoinFuture = new CompletableFuture<>();
        result = context.sendGenericGroupJoin(
            request.setMemberId(UNKNOWN_MEMBER_ID).setGroupInstanceId("follower-instance-id"),
            duplicateFollowerJoinFuture);

        assertTrue(result.records().isEmpty());
        assertTrue(duplicateFollowerJoinFuture.isDone());

        // Old follower rejoins group will fail because member id is already updated.
        CompletableFuture<JoinGroupResponseData> oldFollowerJoinFuture = new CompletableFuture<>();
        result = context.sendGenericGroupJoin(
            request.setMemberId(rebalanceResult.followerId),
            oldFollowerJoinFuture);

        assertTrue(result.records().isEmpty());
        assertTrue(oldFollowerJoinFuture.isDone());
        assertTrue(leaderJoinFuture.isDone());

        JoinGroupResponseData expectedLeaderResponse = new JoinGroupResponseData()
            .setErrorCode(Errors.NONE.code())
            .setGenerationId(rebalanceResult.generationId + 1)
            .setMemberId(rebalanceResult.leaderId)
            .setLeader(rebalanceResult.leaderId)
            .setProtocolName("range")
            .setProtocolType("consumer")
            .setMembers(toJoinResponseMembers(group));

        checkJoinGroupResponse(
            expectedLeaderResponse,
            leaderJoinFuture.get(),
            group,
            COMPLETING_REBALANCE,
            mkSet("leader-instance-id", "follower-instance-id")
        );

        assertTrue(duplicateFollowerJoinFuture.isDone());

        JoinGroupResponseData expectedDuplicateFollowerResponse = new JoinGroupResponseData()
            .setErrorCode(Errors.NONE.code())
            .setGenerationId(rebalanceResult.generationId + 1)
            .setMemberId(duplicateFollowerJoinFuture.get().memberId())
            .setLeader(rebalanceResult.leaderId)
            .setProtocolName("range")
            .setProtocolType("consumer")
            .setMembers(Collections.emptyList());

        checkJoinGroupResponse(
            expectedDuplicateFollowerResponse,
            duplicateFollowerJoinFuture.get(),
            group,
            COMPLETING_REBALANCE,
            Collections.emptySet()
        );

        assertTrue(duplicateFollowerJoinFuture.isDone());

        JoinGroupResponseData expectedOldFollowerResponse = new JoinGroupResponseData()
            .setErrorCode(Errors.FENCED_INSTANCE_ID.code())
            .setGenerationId(-1)
            .setMemberId(rebalanceResult.followerId)
            .setLeader(UNKNOWN_MEMBER_ID)
            .setProtocolName(null)
            .setProtocolType(null)
            .setMembers(Collections.emptyList());

        checkJoinGroupResponse(
            expectedOldFollowerResponse,
            oldFollowerJoinFuture.get(),
            group,
            COMPLETING_REBALANCE,
            Collections.emptySet()
        );
    }

    @Test
    public void testStaticMemberRejoinWithKnownMemberId() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        GenericGroup group = context.createGenericGroup("group-id");

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withGroupInstanceId("group-instance-id")
            .withDefaultProtocolTypeAndProtocols()
            .build();

        JoinGroupResponseData joinResponse = context.joinGenericGroupAndCompleteJoin(request, false, false);
        assertEquals(Errors.NONE.code(), joinResponse.errorCode());

        String memberId = joinResponse.memberId();

        CompletableFuture<JoinGroupResponseData> rejoinResponseFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(
            request.setMemberId(memberId),
            rejoinResponseFuture);

        // The second join group should return immediately since we are using the same metadata during CompletingRebalance.
        assertTrue(result.records().isEmpty());
        assertTrue(group.isInState(COMPLETING_REBALANCE));
        assertTrue(rejoinResponseFuture.isDone());
        assertEquals(Errors.NONE.code(), rejoinResponseFuture.get().errorCode());

        SyncResult syncResult = context.sendGenericGroupSync(new SyncGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(memberId)
            .withGenerationId(joinResponse.generationId())
            .withGroupInstanceId("group-instance-id")
            .build());

        // Successful write to the log.
        syncResult.appendFuture.complete(null);

        assertTrue(syncResult.syncFuture.isDone());
        assertEquals(Errors.NONE.code(), syncResult.syncFuture.get().errorCode());
        assertTrue(group.isInState(STABLE));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testStaticMemberRejoinWithLeaderIdAndUnknownMemberId(
        boolean supportSkippingAssignment
    ) throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        RebalanceResult rebalanceResult = context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id"
        );
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup("group-id", false);

        // A static leader rejoin with unknown id will not trigger rebalance, and no assignment will be returned.
        // As the group was in Stable state and the member id was updated, this will generate records.
        JoinGroupRequestData joinRequest = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("leader-instance-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withProtocolSuperset()
            .build();

        CompletableFuture<JoinGroupResponseData> joinResponseFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(
            joinRequest,
            joinResponseFuture,
            true,
            supportSkippingAssignment);

        assertEquals(
            Collections.singletonList(newGroupMetadataRecord(group, MetadataVersion.latest())),
            result.records()
        );
        // Simulate a successful write to the log.
        result.appendFuture().complete(null);
        assertTrue(joinResponseFuture.isDone());

        String leader = supportSkippingAssignment ?
            joinResponseFuture.get().memberId() : rebalanceResult.leaderId;

        List<JoinGroupResponseMember> members = supportSkippingAssignment ?
            toJoinResponseMembers(group) : Collections.emptyList();

        JoinGroupResponseData expectedJoinResponse = new JoinGroupResponseData()
            .setErrorCode(Errors.NONE.code())
            .setGenerationId(rebalanceResult.generationId)
            .setMemberId(joinResponseFuture.get().memberId())
            .setLeader(leader)
            .setProtocolName("range")
            .setProtocolType("consumer")
            .setSkipAssignment(supportSkippingAssignment)
            .setMembers(members);

        checkJoinGroupResponse(
            expectedJoinResponse,
            joinResponseFuture.get(),
            group,
            STABLE,
            supportSkippingAssignment ? mkSet("leader-instance-id", "follower-instance-id") : Collections.emptySet()
        );

        CompletableFuture<JoinGroupResponseData> oldLeaderJoinFuture = new CompletableFuture<>();
        result = context.sendGenericGroupJoin(
            joinRequest
                .setMemberId(rebalanceResult.leaderId),
            oldLeaderJoinFuture,
            true,
            supportSkippingAssignment);

        assertTrue(result.records().isEmpty());
        assertTrue(oldLeaderJoinFuture.isDone());
        assertEquals(Errors.FENCED_INSTANCE_ID.code(), oldLeaderJoinFuture.get().errorCode());

        // Old leader will get fenced.
        SyncGroupRequestData oldLeaderSyncRequest = new SyncGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("leader-instance-id")
            .withGenerationId(rebalanceResult.generationId)
            .withMemberId(rebalanceResult.leaderId)
            .build();

        SyncResult oldLeaderSyncResult = context.sendGenericGroupSync(oldLeaderSyncRequest);

        assertTrue(result.records().isEmpty());
        assertTrue(oldLeaderSyncResult.syncFuture.isDone());
        assertEquals(Errors.FENCED_INSTANCE_ID.code(), oldLeaderSyncResult.syncFuture.get().errorCode());

        // Calling sync on old leader id will fail because that leader id is no longer valid and replaced.
        SyncGroupRequestData newLeaderSyncRequest = oldLeaderSyncRequest.setGroupInstanceId(null);
        SyncResult newLeaderSyncResult = context.sendGenericGroupSync(newLeaderSyncRequest);

        assertTrue(result.records().isEmpty());
        assertTrue(newLeaderSyncResult.syncFuture.isDone());
        assertEquals(Errors.UNKNOWN_MEMBER_ID.code(), newLeaderSyncResult.syncFuture.get().errorCode());
    }

    @Test
    public void testStaticMemberRejoinWithLeaderIdAndKnownMemberId() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        RebalanceResult rebalanceResult = context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id"
        );
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup("group-id", false);

        // Known static leader rejoin will trigger rebalance.
        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("leader-instance-id")
            .withMemberId(rebalanceResult.leaderId)
            .withDefaultProtocolTypeAndProtocols()
            .withRebalanceTimeoutMs(10000)
            .build();

        JoinGroupResponseData joinResponse = context.joinGenericGroupAndCompleteJoin(request, true, true, 10000);

        // Follower's heartbeat expires as the leader rejoins.
        assertFalse(group.hasMemberId(rebalanceResult.followerId));

        JoinGroupResponseData expectedResponse = new JoinGroupResponseData()
            .setErrorCode(Errors.NONE.code())
            .setGenerationId(rebalanceResult.generationId + 1)
            .setMemberId(rebalanceResult.leaderId)
            .setLeader(rebalanceResult.leaderId)
            .setProtocolName("range")
            .setProtocolType("consumer")
            .setMembers(toJoinResponseMembers(group));

        checkJoinGroupResponse(
            expectedResponse,
            joinResponse,
            group,
            COMPLETING_REBALANCE,
            Collections.singleton("leader-instance-id")
        );
    }

    @Test
    public void testStaticMemberRejoinWithLeaderIdAndUnexpectedDeadGroup() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        RebalanceResult rebalanceResult = context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id"
        );
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup("group-id", false);
        group.transitionTo(DEAD);

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("leader-instance-id")
            .withMemberId(rebalanceResult.leaderId)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        CompletableFuture<JoinGroupResponseData> joinFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(request, joinFuture, true, true);

        assertTrue(result.records().isEmpty());
        assertTrue(joinFuture.isDone());
        assertEquals(Errors.COORDINATOR_NOT_AVAILABLE.code(), joinFuture.get().errorCode());
    }

    @Test
    public void testStaticMemberRejoinWithLeaderIdAndUnexpectedEmptyGroup() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        RebalanceResult rebalanceResult = context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id"
        );
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup("group-id", false);

        group.transitionTo(PREPARING_REBALANCE);
        group.transitionTo(EMPTY);

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("leader-instance-id")
            .withMemberId(rebalanceResult.leaderId)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        CompletableFuture<JoinGroupResponseData> joinFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(request, joinFuture, true, true);

        assertTrue(result.records().isEmpty());
        assertTrue(joinFuture.isDone());
        assertEquals(Errors.UNKNOWN_MEMBER_ID.code(), joinFuture.get().errorCode());
    }

    @Test
    public void testStaticMemberRejoinWithFollowerIdAndChangeOfProtocol() throws Exception {
        int rebalanceTimeoutMs = 10000;
        int sessionTimeoutMs = 15000;
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        RebalanceResult rebalanceResult = context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id",
            rebalanceTimeoutMs,
            sessionTimeoutMs
        );
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup("group-id", false);

        // A static follower rejoin with changed protocol will trigger rebalance.
        JoinGroupRequestProtocolCollection protocols = toProtocols("roundrobin");
        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("follower-instance-id")
            .withMemberId(rebalanceResult.followerId)
            .withProtocols(protocols)
            .withRebalanceTimeoutMs(rebalanceTimeoutMs)
            .withSessionTimeoutMs(sessionTimeoutMs)
            .build();

        CompletableFuture<JoinGroupResponseData> responseFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(request, responseFuture);

        assertTrue(result.records().isEmpty());
        assertFalse(responseFuture.isDone());

        // Old leader hasn't joined in the meantime, triggering a re-election.
        assertNoOrEmptyResult(context.sleep(rebalanceTimeoutMs));

        assertTrue(responseFuture.isDone());
        assertEquals(Errors.NONE.code(), responseFuture.get().errorCode());
        assertTrue(group.hasStaticMember("leader-instance-id"));
        assertTrue(group.isLeader(rebalanceResult.followerId));

        JoinGroupResponseData expectedResponse = new JoinGroupResponseData()
            .setErrorCode(Errors.NONE.code())
            .setGenerationId(rebalanceResult.generationId + 1)
            .setMemberId(rebalanceResult.followerId)
            .setLeader(rebalanceResult.followerId)
            .setProtocolName("roundrobin")
            .setProtocolType("consumer")
            .setMembers(toJoinResponseMembers(group));

        checkJoinGroupResponse(
            expectedResponse,
            responseFuture.get(),
            group,
            COMPLETING_REBALANCE,
            mkSet("leader-instance-id", "follower-instance-id")
        );
    }

    @Test
    public void testStaticMemberRejoinWithUnknownMemberIdAndChangeOfProtocolWithSelectedProtocolChanged() throws Exception {

        int rebalanceTimeoutMs = 10000;
        int sessionTimeoutMs = 15000;
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        RebalanceResult rebalanceResult = context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id",
            rebalanceTimeoutMs,
            sessionTimeoutMs
        );
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup("group-id", false);

        assertNotEquals("roundrobin", group.selectProtocol());

        // A static follower rejoin with changed protocol will trigger rebalance.
        JoinGroupRequestProtocolCollection protocols = toProtocols("roundrobin");

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("follower-instance-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withProtocols(protocols)
            .withRebalanceTimeoutMs(rebalanceTimeoutMs)
            .withSessionTimeoutMs(sessionTimeoutMs)
            .build();

        CompletableFuture<JoinGroupResponseData> responseFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(request, responseFuture);
        assertTrue(result.records().isEmpty());
        assertFalse(responseFuture.isDone());
        assertEquals("roundrobin", group.selectProtocol());

        // Old leader hasn't joined in the meantime, triggering a re-election.
        assertNoOrEmptyResult(context.sleep(rebalanceTimeoutMs));
        assertTrue(responseFuture.isDone());
        assertEquals(Errors.NONE.code(), responseFuture.get().errorCode());
        assertTrue(group.hasStaticMember("leader-instance-id"));
        assertTrue(group.isLeader(responseFuture.get().memberId()));
        assertNotEquals(rebalanceResult.followerId, responseFuture.get().memberId());

        JoinGroupResponseData expectedResponse = new JoinGroupResponseData()
            .setErrorCode(Errors.NONE.code())
            .setGenerationId(rebalanceResult.generationId + 1)
            .setMemberId(responseFuture.get().memberId())
            .setLeader(responseFuture.get().memberId())
            .setProtocolName("roundrobin")
            .setProtocolType("consumer")
            .setMembers(toJoinResponseMembers(group));

        checkJoinGroupResponse(
            expectedResponse,
            responseFuture.get(),
            group,
            COMPLETING_REBALANCE,
            mkSet("leader-instance-id", "follower-instance-id")
        );
    }

    @Test
    public void testStaticMemberRejoinWithUnknownMemberIdAndChangeOfProtocolWhileSelectProtocolUnchangedPersistenceFailure() throws Exception {

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        RebalanceResult rebalanceResult = context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id"
        );
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup("group-id", false);

        JoinGroupRequestProtocolCollection protocols = toProtocols(group.selectProtocol());

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("follower-instance-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withProtocols(protocols)
            .build();

        CompletableFuture<JoinGroupResponseData> followerJoinFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(
            request,
            followerJoinFuture,
            true,
            true);

        assertEquals(
            Collections.singletonList(newGroupMetadataRecord(group, MetadataVersion.latest())),
            result.records()
        );
        // Simulate a failed write to the log.
        result.appendFuture().completeExceptionally(Errors.MESSAGE_TOO_LARGE.exception());
        assertTrue(followerJoinFuture.isDone());

        JoinGroupResponseData expectedResponse = new JoinGroupResponseData()
            .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
            .setGenerationId(rebalanceResult.generationId)
            .setMemberId(followerJoinFuture.get().memberId())
            .setLeader(rebalanceResult.leaderId)
            .setProtocolName("range")
            .setProtocolType("consumer")
            .setSkipAssignment(false)
            .setMembers(Collections.emptyList());

        checkJoinGroupResponse(
            expectedResponse,
            followerJoinFuture.get(),
            group,
            STABLE,
            Collections.emptySet()
        );

        // Join with old member id will not fail because the member id is not updated because of persistence failure
        assertNotEquals(rebalanceResult.followerId, followerJoinFuture.get().memberId());
        followerJoinFuture = new CompletableFuture<>();
        result = context.sendGenericGroupJoin(request.setMemberId(rebalanceResult.followerId), followerJoinFuture);

        assertTrue(result.records().isEmpty());

        // Join with leader and complete join phase.
        CompletableFuture<JoinGroupResponseData> leaderJoinFuture = new CompletableFuture<>();
        result = context.sendGenericGroupJoin(
            request.setGroupInstanceId("leader-instance-id")
                .setMemberId(rebalanceResult.leaderId),
            leaderJoinFuture
        );

        assertTrue(result.records().isEmpty());
        assertTrue(leaderJoinFuture.isDone());
        assertTrue(followerJoinFuture.isDone());
        assertEquals(Errors.NONE.code(), leaderJoinFuture.get().errorCode());
        assertEquals(Errors.NONE.code(), followerJoinFuture.get().errorCode());

        // Sync with leader and receive assignment.
        SyncGroupRequestData syncRequest = new SyncGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("leader-instance-id")
            .withMemberId(rebalanceResult.leaderId)
            .withGenerationId(rebalanceResult.generationId + 1)
            .build();

        SyncResult leaderSyncResult = context.sendGenericGroupSync(syncRequest);

        // Simulate a successful write to the log. This will update the group with the new (empty) assignment.
        leaderSyncResult.appendFuture.complete(null);

        assertEquals(
            Collections.singletonList(newGroupMetadataRecord(group, MetadataVersion.latest())),
            leaderSyncResult.records
        );

        assertTrue(leaderSyncResult.syncFuture.isDone());
        assertTrue(group.isInState(STABLE));
        assertEquals(Errors.NONE.code(), leaderSyncResult.syncFuture.get().errorCode());

        // Sync with old member id will also not fail because the member id is not updated because of persistence failure
        SyncResult oldMemberSyncResult = context.sendGenericGroupSync(
            syncRequest
                .setGroupInstanceId("follower-instance-id")
                .setMemberId(rebalanceResult.followerId)
        );
        assertTrue(result.records().isEmpty());
        assertTrue(oldMemberSyncResult.syncFuture.isDone());
        assertTrue(group.isInState(STABLE));
        assertEquals(Errors.NONE.code(), oldMemberSyncResult.syncFuture.get().errorCode());
    }

    @Test
    public void testStaticMemberRejoinWithUnknownMemberIdAndChangeOfProtocolWhileSelectProtocolUnchanged() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        RebalanceResult rebalanceResult = context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id"
        );
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup("group-id", false);

        // A static follower rejoin with protocol changing to leader protocol subset won't trigger rebalance if updated
        // group's selectProtocol remain unchanged.
        JoinGroupRequestProtocolCollection protocols = toProtocols(group.selectProtocol());

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("follower-instance-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withProtocols(protocols)
            .build();

        CompletableFuture<JoinGroupResponseData> followerJoinFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(
            request,
            followerJoinFuture,
            true,
            true);

        assertEquals(
            Collections.singletonList(newGroupMetadataRecord(group, MetadataVersion.latest())),
            result.records()
        );

        // Simulate a successful write to the log.
        result.appendFuture().complete(null);
        assertTrue(followerJoinFuture.isDone());

        JoinGroupResponseData expectedResponse = new JoinGroupResponseData()
            .setErrorCode(Errors.NONE.code())
            .setGenerationId(rebalanceResult.generationId)
            .setMemberId(followerJoinFuture.get().memberId())
            .setLeader(rebalanceResult.leaderId)
            .setProtocolName("range")
            .setProtocolType("consumer")
            .setSkipAssignment(false)
            .setMembers(Collections.emptyList());

        checkJoinGroupResponse(
            expectedResponse,
            followerJoinFuture.get(),
            group,
            STABLE,
            Collections.emptySet()
        );

        // Join with old member id will fail because the member id is updated
        String newFollowerId = followerJoinFuture.get().memberId();
        assertNotEquals(rebalanceResult.followerId, newFollowerId);
        followerJoinFuture = new CompletableFuture<>();
        result = context.sendGenericGroupJoin(request.setMemberId(rebalanceResult.followerId), followerJoinFuture);

        assertTrue(result.records().isEmpty());
        assertEquals(Errors.FENCED_INSTANCE_ID.code(), followerJoinFuture.get().errorCode());

        // Sync with old member id will fail because the member id is updated
        SyncGroupRequestData syncRequest = new SyncGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("follower-instance-id")
            .withGenerationId(rebalanceResult.generationId)
            .withMemberId(rebalanceResult.followerId)
            .withAssignment(Collections.emptyList())
            .build();

        SyncResult syncResult = context.sendGenericGroupSync(syncRequest);

        assertTrue(syncResult.records.isEmpty());
        assertTrue(syncResult.syncFuture.isDone());
        assertEquals(Errors.FENCED_INSTANCE_ID.code(), syncResult.syncFuture.get().errorCode());

        // Sync with new member id succeeds
        syncResult = context.sendGenericGroupSync(syncRequest.setMemberId(newFollowerId));

        assertTrue(syncResult.records.isEmpty());
        assertTrue(syncResult.syncFuture.isDone());
        assertEquals(Errors.NONE.code(), syncResult.syncFuture.get().errorCode());
        assertEquals(rebalanceResult.followerAssignment, syncResult.syncFuture.get().assignment());
    }

    @Test
    public void testStaticMemberRejoinWithKnownLeaderIdToTriggerRebalanceAndFollowerWithChangeofProtocol()
        throws Exception {

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        RebalanceResult rebalanceResult = context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id"
        );
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup("group-id", false);

        // A static leader rejoin with known member id will trigger rebalance.
        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("leader-instance-id")
            .withMemberId(rebalanceResult.leaderId)
            .withProtocolSuperset()
            .withRebalanceTimeoutMs(10000)
            .withSessionTimeoutMs(5000)
            .build();

        CompletableFuture<JoinGroupResponseData> leaderJoinFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(
            request,
            leaderJoinFuture,
            true,
            true);

        assertTrue(result.records().isEmpty());
        assertFalse(leaderJoinFuture.isDone());

        // Rebalance completes immediately after follower rejoins.
        CompletableFuture<JoinGroupResponseData> followerJoinFuture = new CompletableFuture<>();
        result = context.sendGenericGroupJoin(
            request.setGroupInstanceId("follower-instance-id")
                .setMemberId(rebalanceResult.followerId),
            followerJoinFuture,
            true,
            true);

        assertTrue(result.records().isEmpty());
        assertTrue(leaderJoinFuture.isDone());
        assertTrue(followerJoinFuture.isDone());
        assertTrue(group.isInState(COMPLETING_REBALANCE));
        assertEquals(2, group.generationId());

        // Leader should get the same assignment as last round.
        JoinGroupResponseData expectedLeaderResponse = new JoinGroupResponseData()
            .setErrorCode(Errors.NONE.code())
            .setGenerationId(rebalanceResult.generationId + 1) // The group has promoted to the new generation.
            .setMemberId(leaderJoinFuture.get().memberId())
            .setLeader(rebalanceResult.leaderId)
            .setProtocolName("range")
            .setProtocolType("consumer")
            .setSkipAssignment(false)
            .setMembers(toJoinResponseMembers(group));

        checkJoinGroupResponse(
            expectedLeaderResponse,
            leaderJoinFuture.get(),
            group,
            COMPLETING_REBALANCE,
            mkSet("leader-instance-id", "follower-instance-id")
        );

        JoinGroupResponseData expectedFollowerResponse = new JoinGroupResponseData()
            .setErrorCode(Errors.NONE.code())
            .setGenerationId(rebalanceResult.generationId + 1) // The group has promoted to the new generation.
            .setMemberId(followerJoinFuture.get().memberId())
            .setLeader(rebalanceResult.leaderId)
            .setProtocolName("range")
            .setProtocolType("consumer")
            .setSkipAssignment(false)
            .setMembers(Collections.emptyList());

        checkJoinGroupResponse(
            expectedFollowerResponse,
            followerJoinFuture.get(),
            group,
            COMPLETING_REBALANCE,
            Collections.emptySet()
        );

        // The follower protocol changed from protocolSuperset to general protocols.
        JoinGroupRequestProtocolCollection protocols = toProtocols("range");

        followerJoinFuture = new CompletableFuture<>();
        result = context.sendGenericGroupJoin(
            request.setGroupInstanceId("follower-instance-id")
                .setMemberId(rebalanceResult.followerId)
                .setProtocols(protocols),
            followerJoinFuture,
            true,
            true);

        assertTrue(result.records().isEmpty());
        assertFalse(followerJoinFuture.isDone());
        // The group will transition to PreparingRebalance due to protocol change from follower.
        assertTrue(group.isInState(PREPARING_REBALANCE));

        // Advance clock by session timeout to kick leader out and complete join phase.
        List<ExpiredTimeout<Void, Record>> timeouts = context.sleep(5000);
        // Both leader and follower heartbeat timers may expire. However, the follower heartbeat expiration
        // will not kick the follower out because it is awaiting a join response.
        assertTrue(timeouts.size() <= 2);
        assertTrue(followerJoinFuture.isDone());

        String newFollowerId = followerJoinFuture.get().memberId();
        expectedFollowerResponse = new JoinGroupResponseData()
            .setErrorCode(Errors.NONE.code())
            .setGenerationId(rebalanceResult.generationId + 2) // The group has promoted to the new generation.
            .setMemberId(newFollowerId)
            .setLeader(newFollowerId)
            .setProtocolName("range")
            .setProtocolType("consumer")
            .setSkipAssignment(false)
            .setMembers(toJoinResponseMembers(group));

        checkJoinGroupResponse(
            expectedFollowerResponse,
            followerJoinFuture.get(),
            group,
            COMPLETING_REBALANCE,
            Collections.singleton("follower-instance-id")
        );
    }

    @Test
    public void testStaticMemberRejoinAsFollowerWithUnknownMemberId() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        RebalanceResult rebalanceResult = context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id"
        );
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup("group-id", false);

        // A static follower rejoin with no protocol change will not trigger rebalance.
        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("follower-instance-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withProtocolSuperset()
            .build();

        CompletableFuture<JoinGroupResponseData> followerJoinFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(
            request,
            followerJoinFuture,
            true,
            true);

        assertEquals(
            Collections.singletonList(newGroupMetadataRecord(group, MetadataVersion.latest())),
            result.records()
        );
        // Simulate a successful write to log.
        result.appendFuture().complete(null);

        assertTrue(followerJoinFuture.isDone());

        // Old leader shouldn't be timed out.
        assertTrue(group.hasStaticMember("leader-instance-id"));

        JoinGroupResponseData expectedFollowerResponse = new JoinGroupResponseData()
            .setErrorCode(Errors.NONE.code())
            .setGenerationId(rebalanceResult.generationId) // The group has not changed.
            .setMemberId(followerJoinFuture.get().memberId())
            .setLeader(rebalanceResult.leaderId)
            .setProtocolName("range")
            .setProtocolType("consumer")
            .setSkipAssignment(false)
            .setMembers(Collections.emptyList());

        checkJoinGroupResponse(
            expectedFollowerResponse,
            followerJoinFuture.get(),
            group,
            STABLE,
            Collections.emptySet()
        );
        assertNotEquals(rebalanceResult.followerId, followerJoinFuture.get().memberId());

        SyncResult syncResult = context.sendGenericGroupSync(new SyncGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("follower-instance-id")
            .withGenerationId(rebalanceResult.generationId)
            .withMemberId(followerJoinFuture.get().memberId())
            .build());

        assertTrue(syncResult.records.isEmpty());
        assertTrue(syncResult.syncFuture.isDone());
        assertEquals(Errors.NONE.code(), syncResult.syncFuture.get().errorCode());
        assertEquals(rebalanceResult.followerAssignment, syncResult.syncFuture.get().assignment());
    }

    @Test
    public void testStaticMemberRejoinAsFollowerWithKnownMemberIdAndNoProtocolChange() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        RebalanceResult rebalanceResult = context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id"
        );
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup("group-id", false);

        // A static follower rejoin with no protocol change will not trigger rebalance.
        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("follower-instance-id")
            .withMemberId(rebalanceResult.followerId)
            .withProtocolSuperset()
            .build();

        CompletableFuture<JoinGroupResponseData> followerJoinFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(
            request,
            followerJoinFuture,
            true,
            true);

        // No records to write because no metadata changed.
        assertTrue(result.records().isEmpty());
        assertTrue(followerJoinFuture.isDone());

        // Old leader shouldn't be timed out.
        assertTrue(group.hasStaticMember("leader-instance-id"));

        JoinGroupResponseData expectedFollowerResponse = new JoinGroupResponseData()
            .setErrorCode(Errors.NONE.code())
            .setGenerationId(rebalanceResult.generationId) // The group has not changed.
            .setMemberId(rebalanceResult.followerId)
            .setLeader(rebalanceResult.leaderId)
            .setProtocolName("range")
            .setProtocolType("consumer")
            .setSkipAssignment(false)
            .setMembers(Collections.emptyList());

        checkJoinGroupResponse(
            expectedFollowerResponse,
            followerJoinFuture.get(),
            group,
            STABLE,
            Collections.emptySet()
        );
    }

    @Test
    public void testStaticMemberRejoinAsFollowerWithMismatchedInstanceId() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        RebalanceResult rebalanceResult = context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id"
        );

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("leader-instance-id")
            .withMemberId(rebalanceResult.followerId)
            .withProtocolSuperset()
            .build();

        CompletableFuture<JoinGroupResponseData> followerJoinFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(
            request,
            followerJoinFuture,
            true,
            true);

        assertTrue(result.records().isEmpty());
        assertTrue(followerJoinFuture.isDone());
        assertEquals(Errors.FENCED_INSTANCE_ID.code(), followerJoinFuture.get().errorCode());
    }

    @Test
    public void testStaticMemberRejoinAsLeaderWithMismatchedInstanceId() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        RebalanceResult rebalanceResult = context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id"
        );

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("follower-instance-id")
            .withMemberId(rebalanceResult.leaderId)
            .withProtocolSuperset()
            .build();

        CompletableFuture<JoinGroupResponseData> leaderJoinFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(
            request,
            leaderJoinFuture,
            true,
            true);

        assertTrue(result.records().isEmpty());
        assertTrue(leaderJoinFuture.isDone());
        assertEquals(Errors.FENCED_INSTANCE_ID.code(), leaderJoinFuture.get().errorCode());
    }

    @Test
    public void testStaticMemberSyncAsLeaderWithInvalidMemberId() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id"
        );

        SyncGroupRequestData request = new SyncGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("follower-instance-id")
            .withMemberId("invalid-member-id")
            .build();

        SyncResult leaderSyncResult = context.sendGenericGroupSync(request);

        assertTrue(leaderSyncResult.records.isEmpty());
        assertTrue(leaderSyncResult.syncFuture.isDone());
        assertEquals(Errors.FENCED_INSTANCE_ID.code(), leaderSyncResult.syncFuture.get().errorCode());
    }

    @Test
    public void testGetDifferentStaticMemberIdAfterEachRejoin() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        RebalanceResult rebalanceResult = context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id"
        );
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup("group-id", false);

        String lastMemberId = rebalanceResult.leaderId;
        for (int i = 0; i < 5; i++) {
            JoinGroupRequestData request = new JoinGroupRequestBuilder()
                .withGroupId("group-id")
                .withGroupInstanceId("leader-instance-id")
                .withMemberId(UNKNOWN_MEMBER_ID)
                .withProtocolSuperset()
                .build();

            CompletableFuture<JoinGroupResponseData> leaderJoinFuture = new CompletableFuture<>();
            CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(
                request,
                leaderJoinFuture,
                true,
                true);

            assertEquals(
                Collections.singletonList(newGroupMetadataRecord(group, MetadataVersion.latest())),
                result.records()
            );
            // Simulate a successful write to log.
            result.appendFuture().complete(null);
            assertTrue(leaderJoinFuture.isDone());
            assertEquals(group.staticMemberId("leader-instance-id"), leaderJoinFuture.get().memberId());
            assertEquals(Errors.NONE.code(), leaderJoinFuture.get().errorCode());
            assertNotEquals(lastMemberId, leaderJoinFuture.get().memberId());

            lastMemberId = leaderJoinFuture.get().memberId();
        }
    }

    @Test
    public void testStaticMemberJoinWithUnknownInstanceIdAndKnownMemberId() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        RebalanceResult rebalanceResult = context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id"
        );

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("unknown-instance-id")
            .withMemberId(rebalanceResult.leaderId)
            .withProtocolSuperset()
            .build();

        CompletableFuture<JoinGroupResponseData> joinFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(
            request,
            joinFuture,
            true,
            true);

        assertTrue(result.records().isEmpty());
        assertTrue(joinFuture.isDone());
        assertEquals(Errors.UNKNOWN_MEMBER_ID.code(), joinFuture.get().errorCode());
    }

    @Test
    public void testStaticMemberReJoinWithIllegalStateAsUnknownMember() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id"
        );
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup("group-id", false);

        group.transitionTo(PREPARING_REBALANCE);
        group.transitionTo(EMPTY);

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("follower-instance-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withProtocolSuperset()
            .build();

        // Illegal state exception shall trigger since follower id resides in pending member bucket.
        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> context.sendGenericGroupJoin(
            request,
            new CompletableFuture<>(),
            true,
            true));

        String message = exception.getMessage();
        assertTrue(message.contains(group.groupId()));
        assertTrue(message.contains("follower-instance-id"));
    }

    @Test
    public void testStaticMemberFollowerFailToRejoinBeforeRebalanceTimeout() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // Increase session timeout so that the follower won't be evicted when rebalance timeout is reached.
        RebalanceResult rebalanceResult = context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id",
            10000,
            15000
        );
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup("group-id", false);

        String newMemberInstanceId = "new-member-instance-id";
        String leaderId = rebalanceResult.leaderId;

        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId(newMemberInstanceId)
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withProtocolSuperset()
            .build();

        CompletableFuture<JoinGroupResponseData> newMemberFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(
            request,
            newMemberFuture,
            true,
            true);

        assertTrue(result.records().isEmpty());
        assertFalse(newMemberFuture.isDone());
        assertTrue(group.isInState(PREPARING_REBALANCE));

        CompletableFuture<JoinGroupResponseData> leaderJoinFuture = new CompletableFuture<>();
        result = context.sendGenericGroupJoin(
            request.setGroupInstanceId("leader-instance-id")
                .setMemberId(leaderId),
            leaderJoinFuture,
            true,
            true);

        assertTrue(result.records().isEmpty());
        assertFalse(leaderJoinFuture.isDone());

        // Advance clock by rebalance timeout to complete join phase.
        assertNoOrEmptyResult(context.sleep(10000));

        assertTrue(leaderJoinFuture.isDone());
        assertTrue(newMemberFuture.isDone());

        JoinGroupResponseData expectedLeaderResponse = new JoinGroupResponseData()
            .setErrorCode(Errors.NONE.code())
            .setGenerationId(rebalanceResult.generationId + 1)
            .setMemberId(leaderId)
            .setLeader(leaderId)
            .setProtocolName("range")
            .setProtocolType("consumer")
            .setSkipAssignment(false)
            .setMembers(toJoinResponseMembers(group));

        checkJoinGroupResponse(
            expectedLeaderResponse,
            leaderJoinFuture.get(),
            group,
            COMPLETING_REBALANCE,
            mkSet("leader-instance-id", "follower-instance-id", newMemberInstanceId)
        );

        JoinGroupResponseData expectedNewMemberResponse = new JoinGroupResponseData()
            .setErrorCode(Errors.NONE.code())
            .setGenerationId(rebalanceResult.generationId + 1)
            .setMemberId(newMemberFuture.get().memberId())
            .setLeader(leaderId)
            .setProtocolName("range")
            .setProtocolType("consumer")
            .setSkipAssignment(false)
            .setMembers(Collections.emptyList());

        checkJoinGroupResponse(
            expectedNewMemberResponse,
            newMemberFuture.get(),
            group,
            COMPLETING_REBALANCE,
            Collections.emptySet()
        );
    }

    @Test
    public void testStaticMemberLeaderFailToRejoinBeforeRebalanceTimeout() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // Increase session timeout so that the leader won't be evicted when rebalance timeout is reached.
        RebalanceResult rebalanceResult = context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id",
            10000,
            15000
        );
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup("group-id", false);

        String newMemberInstanceId = "new-member-instance-id";
        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId(newMemberInstanceId)
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withProtocolSuperset()
            .build();

        CompletableFuture<JoinGroupResponseData> newMemberFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(
            request,
            newMemberFuture,
            true,
            true);

        assertTrue(result.records().isEmpty());
        assertFalse(newMemberFuture.isDone());
        assertTrue(group.isInState(PREPARING_REBALANCE));

        CompletableFuture<JoinGroupResponseData> oldFollowerJoinFuture = new CompletableFuture<>();
        result = context.sendGenericGroupJoin(
            request.setGroupInstanceId("follower-instance-id")
                .setMemberId(rebalanceResult.followerId),
            oldFollowerJoinFuture,
            true,
            true);

        assertTrue(result.records().isEmpty());
        assertFalse(oldFollowerJoinFuture.isDone());

        // Advance clock by rebalance timeout to complete join phase.
        assertNoOrEmptyResult(context.sleep(10000));

        assertTrue(oldFollowerJoinFuture.isDone());
        assertTrue(newMemberFuture.isDone());

        JoinGroupResponseData newLeaderResponse = oldFollowerJoinFuture.get().leader()
            .equals(oldFollowerJoinFuture.get().memberId()) ? oldFollowerJoinFuture.get() : newMemberFuture.get();

        JoinGroupResponseData newFollowerResponse = oldFollowerJoinFuture.get().leader()
            .equals(oldFollowerJoinFuture.get().memberId()) ? newMemberFuture.get() : oldFollowerJoinFuture.get();

        JoinGroupResponseData expectedLeaderResponse = new JoinGroupResponseData()
            .setErrorCode(Errors.NONE.code())
            .setGenerationId(rebalanceResult.generationId + 1)
            .setMemberId(newLeaderResponse.memberId())
            .setLeader(newLeaderResponse.memberId())
            .setProtocolName("range")
            .setProtocolType("consumer")
            .setSkipAssignment(false)
            .setMembers(toJoinResponseMembers(group));

        checkJoinGroupResponse(
            expectedLeaderResponse,
            newLeaderResponse,
            group,
            COMPLETING_REBALANCE,
            mkSet("leader-instance-id", "follower-instance-id", newMemberInstanceId)
        );

        JoinGroupResponseData expectedNewMemberResponse = new JoinGroupResponseData()
            .setErrorCode(Errors.NONE.code())
            .setGenerationId(rebalanceResult.generationId + 1)
            .setMemberId(newFollowerResponse.memberId())
            .setLeader(newLeaderResponse.memberId())
            .setProtocolName("range")
            .setProtocolType("consumer")
            .setSkipAssignment(false)
            .setMembers(Collections.emptyList());

        checkJoinGroupResponse(
            expectedNewMemberResponse,
            newFollowerResponse,
            group,
            COMPLETING_REBALANCE,
            Collections.emptySet()
        );
    }

    @Test
    public void testSyncGroupReturnsAnErrorWhenProtocolTypeIsInconsistent() throws Exception {
        testSyncGroupProtocolTypeAndNameWith(
            Optional.of("protocolType"),
            Optional.empty(),
            Errors.INCONSISTENT_GROUP_PROTOCOL,
            Optional.empty(),
            Optional.empty()
        );
    }

    @Test
    public void testSyncGroupReturnsAnErrorWhenProtocolNameIsInconsistent() throws Exception {
        testSyncGroupProtocolTypeAndNameWith(
            Optional.empty(),
            Optional.of("protocolName"),
            Errors.INCONSISTENT_GROUP_PROTOCOL,
            Optional.empty(),
            Optional.empty()
        );
    }

    @Test
    public void testSyncGroupSucceedWhenProtocolTypeAndNameAreNotProvided() throws Exception {
        testSyncGroupProtocolTypeAndNameWith(
            Optional.empty(),
            Optional.empty(),
            Errors.NONE,
            Optional.of("consumer"),
            Optional.of("range")
        );
    }

    @Test
    public void testSyncGroupSucceedWhenProtocolTypeAndNameAreConsistent() throws Exception {
        testSyncGroupProtocolTypeAndNameWith(
            Optional.of("consumer"),
            Optional.of("range"),
            Errors.NONE,
            Optional.of("consumer"),
            Optional.of("range")
        );
    }

    private void testSyncGroupProtocolTypeAndNameWith(
        Optional<String> protocolType,
        Optional<String> protocolName,
        Errors expectedError,
        Optional<String> expectedProtocolType,
        Optional<String> expectedProtocolName
    ) throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        context.createGenericGroup("group-id");

        // JoinGroup(leader) with the Protocol Type of the group
        JoinGroupRequestData joinRequest = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("leader-instance-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withProtocolSuperset()
            .build();

        CompletableFuture<JoinGroupResponseData> leaderJoinFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(joinRequest, leaderJoinFuture);

        assertTrue(result.records().isEmpty());
        assertFalse(leaderJoinFuture.isDone());

        // JoinGroup(follower) with the Protocol Type of the group
        CompletableFuture<JoinGroupResponseData> followerJoinFuture = new CompletableFuture<>();
        result = context.sendGenericGroupJoin(joinRequest
            .setGroupInstanceId("follower-instance-id"), followerJoinFuture);

        assertTrue(result.records().isEmpty());
        assertFalse(followerJoinFuture.isDone());

        // Advance clock by rebalance timeout to complete join phase.
        assertNoOrEmptyResult(context.sleep(10000));
        assertTrue(leaderJoinFuture.isDone());
        assertTrue(followerJoinFuture.isDone());
        assertEquals(Errors.NONE.code(), leaderJoinFuture.get().errorCode());
        assertEquals(Errors.NONE.code(), followerJoinFuture.get().errorCode());

        String leaderId = leaderJoinFuture.get().memberId();
        String followerId = followerJoinFuture.get().memberId();
        int generationId = leaderJoinFuture.get().generationId();

        // SyncGroup with the provided Protocol Type and Name
        List<SyncGroupRequestAssignment> assignment = new ArrayList<>();
        assignment.add(new SyncGroupRequestAssignment().setMemberId(leaderId));
        SyncGroupRequestData syncRequest = new SyncGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(leaderId)
            .withProtocolType(protocolType.orElse(null))
            .withProtocolName(protocolName.orElse(null))
            .withGenerationId(generationId)
            .withAssignment(assignment)
            .build();

        SyncResult leaderSyncResult = context.sendGenericGroupSync(syncRequest);
        // Simulate a successful write to the log.
        leaderSyncResult.appendFuture.complete(null);

        assertTrue(leaderSyncResult.syncFuture.isDone());
        assertEquals(expectedError.code(), leaderSyncResult.syncFuture.get().errorCode());
        assertEquals(expectedProtocolType.orElse(null), leaderSyncResult.syncFuture.get().protocolType());
        assertEquals(expectedProtocolName.orElse(null), leaderSyncResult.syncFuture.get().protocolName());

        SyncResult followerSyncResult = context.sendGenericGroupSync(syncRequest.setMemberId(followerId));

        assertTrue(followerSyncResult.records.isEmpty());
        assertTrue(followerSyncResult.syncFuture.isDone());
        assertEquals(expectedError.code(), followerSyncResult.syncFuture.get().errorCode());
        assertEquals(expectedProtocolType.orElse(null), followerSyncResult.syncFuture.get().protocolType());
        assertEquals(expectedProtocolName.orElse(null), followerSyncResult.syncFuture.get().protocolName());
    }

    @Test
    public void testSyncGroupFromUnknownGroup() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // SyncGroup with the provided Protocol Type and Name
        SyncResult syncResult = context.sendGenericGroupSync(new SyncGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId("member-id")
            .withGenerationId(1)
            .build());

        assertTrue(syncResult.records.isEmpty());
        assertTrue(syncResult.syncFuture.isDone());
        assertEquals(Errors.UNKNOWN_MEMBER_ID.code(), syncResult.syncFuture.get().errorCode());
    }

    @Test
    public void testSyncGroupFromUnknownMember() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        context.createGenericGroup("group-id");

        JoinGroupResponseData joinResponse = context.joinGenericGroupAndCompleteJoin(
            new JoinGroupRequestBuilder()
                .withGroupId("group-id")
                .withGroupInstanceId("leader-instance-id")
                .withMemberId(UNKNOWN_MEMBER_ID)
                .withDefaultProtocolTypeAndProtocols()
                .build(),
            true,
            true
        );

        String memberId = joinResponse.memberId();
        int generationId = joinResponse.generationId();

        List<SyncGroupRequestAssignment> assignment = new ArrayList<>();
        assignment.add(new SyncGroupRequestAssignment().setMemberId(memberId));
        SyncGroupRequestData syncRequest = new SyncGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(memberId)
            .withGenerationId(generationId)
            .withAssignment(assignment)
            .build();

        SyncResult syncResult = context.sendGenericGroupSync(syncRequest);

        // Simulate a successful write to log.
        syncResult.appendFuture.complete(null);

        assertTrue(syncResult.syncFuture.isDone());
        assertEquals(Errors.NONE.code(), syncResult.syncFuture.get().errorCode());
        assertEquals(assignment.get(0).assignment(), syncResult.syncFuture.get().assignment());

        // Sync with unknown member.
        syncResult = context.sendGenericGroupSync(syncRequest.setMemberId("unknown-member-id"));

        assertTrue(syncResult.records.isEmpty());
        assertTrue(syncResult.syncFuture.isDone());
        assertEquals(Errors.UNKNOWN_MEMBER_ID.code(), syncResult.syncFuture.get().errorCode());
        assertEquals(GenericGroupMember.EMPTY_ASSIGNMENT, syncResult.syncFuture.get().assignment());
    }

    @Test
    public void testSyncGroupFromIllegalGeneration() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        context.createGenericGroup("group-id");

        JoinGroupRequestData joinRequest = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("leader-instance-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        JoinGroupResponseData joinResponse = context.joinGenericGroupAndCompleteJoin(joinRequest, true, true);

        String memberId = joinResponse.memberId();
        int generationId = joinResponse.generationId();

        // Send the sync group with an invalid generation
        SyncResult syncResult = context.sendGenericGroupSync(new SyncGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(memberId)
            .withGenerationId(generationId + 1)
            .build());

        assertTrue(syncResult.records.isEmpty());
        assertTrue(syncResult.syncFuture.isDone());
        assertEquals(Errors.ILLEGAL_GENERATION.code(), syncResult.syncFuture.get().errorCode());
    }

    @Test
    public void testJoinGroupFromUnchangedFollowerDoesNotRebalance() throws Exception {
        // To get a group of two members:
        // 1. join and sync with a single member (because we can't immediately join with two members)
        // 2. join and sync with the first member and a new member

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        JoinGroupResponseData leaderJoinResponse = context.joinGenericGroupAsDynamicMemberAndCompleteRebalance("group-id");

        JoinGroupRequestData joinRequest = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        CompletableFuture<JoinGroupResponseData> followerJoinFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(
            joinRequest,
            followerJoinFuture
        );

        assertTrue(result.records().isEmpty());
        assertFalse(followerJoinFuture.isDone());

        CompletableFuture<JoinGroupResponseData> leaderJoinFuture = new CompletableFuture<>();
        result = context.sendGenericGroupJoin(
            joinRequest
                .setMemberId(leaderJoinResponse.memberId()),
            leaderJoinFuture
        );

        assertTrue(result.records().isEmpty());
        assertTrue(leaderJoinFuture.isDone());
        assertTrue(followerJoinFuture.isDone());
        assertEquals(Errors.NONE.code(), leaderJoinFuture.get().errorCode());
        assertEquals(Errors.NONE.code(), followerJoinFuture.get().errorCode());
        assertEquals(leaderJoinFuture.get().generationId(), followerJoinFuture.get().generationId());
        assertEquals(leaderJoinResponse.memberId(), leaderJoinFuture.get().leader());
        assertEquals(leaderJoinResponse.memberId(), followerJoinFuture.get().leader());

        int nextGenerationId = leaderJoinFuture.get().generationId();
        String followerId = followerJoinFuture.get().memberId();

        // This shouldn't cause a rebalance since protocol information hasn't changed
        followerJoinFuture = new CompletableFuture<>();
        result = context.sendGenericGroupJoin(
            joinRequest.setMemberId(followerId),
            followerJoinFuture
        );

        assertTrue(result.records().isEmpty());
        assertTrue(followerJoinFuture.isDone());
        assertEquals(Errors.NONE.code(), followerJoinFuture.get().errorCode());
        assertEquals(nextGenerationId, followerJoinFuture.get().generationId());
    }

    @Test
    public void testLeaderFailureInSyncGroup() throws Exception {
        // To get a group of two members:
        // 1. join and sync with a single member (because we can't immediately join with two members)
        // 2. join and sync with the first member and a new member

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        JoinGroupResponseData leaderJoinResponse = context.joinGenericGroupAsDynamicMemberAndCompleteRebalance("group-id");
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup("group-id", false);

        JoinGroupRequestData joinRequest = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .withRebalanceTimeoutMs(10000)
            .withSessionTimeoutMs(5000)
            .build();

        CompletableFuture<JoinGroupResponseData> followerJoinFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(
            joinRequest,
            followerJoinFuture
        );

        assertTrue(result.records().isEmpty());
        assertFalse(followerJoinFuture.isDone());

        CompletableFuture<JoinGroupResponseData> leaderJoinFuture = new CompletableFuture<>();
        result = context.sendGenericGroupJoin(
            joinRequest
                .setMemberId(leaderJoinResponse.memberId()),
            leaderJoinFuture
        );

        assertTrue(result.records().isEmpty());
        assertTrue(leaderJoinFuture.isDone());
        assertTrue(followerJoinFuture.isDone());
        assertEquals(Errors.NONE.code(), leaderJoinFuture.get().errorCode());
        assertEquals(Errors.NONE.code(), followerJoinFuture.get().errorCode());
        assertEquals(leaderJoinFuture.get().generationId(), followerJoinFuture.get().generationId());
        assertEquals(leaderJoinResponse.memberId(), leaderJoinFuture.get().leader());
        assertEquals(leaderJoinResponse.memberId(), followerJoinFuture.get().leader());
        assertTrue(group.isInState(COMPLETING_REBALANCE));

        int nextGenerationId = leaderJoinFuture.get().generationId();
        String followerId = followerJoinFuture.get().memberId();

        // With no leader SyncGroup, the follower's sync request should fail with an error indicating
        // that it should rejoin
        SyncResult followerSyncResult = context.sendGenericGroupSync(new SyncGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(followerId)
            .withGenerationId(nextGenerationId)
            .build());

        assertTrue(followerSyncResult.records.isEmpty());
        assertFalse(followerSyncResult.syncFuture.isDone());

        // Advance clock by session timeout to expire leader heartbeat and prepare rebalance.
        // This should complete follower's sync response. The follower's heartbeat expiration will not kick
        // the follower out because it is awaiting sync.
        List<ExpiredTimeout<Void, Record>> timeouts = context.sleep(10000);
        assertTrue(timeouts.size() <= 2);
        timeouts.forEach(timeout -> assertTrue(timeout.result.records().isEmpty()));

        assertTrue(followerSyncResult.syncFuture.isDone());
        assertEquals(Errors.REBALANCE_IN_PROGRESS.code(), followerSyncResult.syncFuture.get().errorCode());
        assertEquals(1, group.size());
        assertTrue(group.hasMemberId(followerId));
        assertTrue(group.isInState(PREPARING_REBALANCE));
    }

    @Test
    public void testSyncGroupFollowerAfterLeader() throws Exception {
        // To get a group of two members:
        // 1. join and sync with a single member (because we can't immediately join with two members)
        // 2. join and sync with the first member and a new member

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        JoinGroupResponseData leaderJoinResponse = context.joinGenericGroupAsDynamicMemberAndCompleteRebalance("group-id");
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup("group-id", false);

        JoinGroupRequestData joinRequest = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .withRebalanceTimeoutMs(10000)
            .withSessionTimeoutMs(5000)
            .build();

        CompletableFuture<JoinGroupResponseData> followerJoinFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(
            joinRequest
                .setMemberId(UNKNOWN_MEMBER_ID),
            followerJoinFuture
        );

        assertTrue(result.records().isEmpty());
        assertFalse(followerJoinFuture.isDone());

        CompletableFuture<JoinGroupResponseData> leaderJoinFuture = new CompletableFuture<>();
        result = context.sendGenericGroupJoin(
            joinRequest
                .setMemberId(leaderJoinResponse.memberId()),
            leaderJoinFuture
        );

        assertTrue(result.records().isEmpty());
        assertTrue(leaderJoinFuture.isDone());
        assertTrue(followerJoinFuture.isDone());
        assertEquals(Errors.NONE.code(), leaderJoinFuture.get().errorCode());
        assertEquals(Errors.NONE.code(), followerJoinFuture.get().errorCode());
        assertEquals(leaderJoinFuture.get().generationId(), followerJoinFuture.get().generationId());
        assertEquals(leaderJoinResponse.memberId(), leaderJoinFuture.get().leader());
        assertEquals(leaderJoinResponse.memberId(), followerJoinFuture.get().leader());
        assertTrue(group.isInState(COMPLETING_REBALANCE));

        int nextGenerationId = leaderJoinFuture.get().generationId();
        String followerId = followerJoinFuture.get().memberId();
        byte[] leaderAssignment = new byte[]{0};
        byte[] followerAssignment = new byte[]{1};

        // Sync group with leader to get new assignment.
        List<SyncGroupRequestAssignment> assignment = new ArrayList<>();
        assignment.add(new SyncGroupRequestAssignment()
            .setMemberId(leaderJoinResponse.memberId())
            .setAssignment(leaderAssignment)
        );
        assignment.add(new SyncGroupRequestAssignment()
            .setMemberId(followerId)
            .setAssignment(followerAssignment)
        );

        SyncGroupRequestData syncRequest = new SyncGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(leaderJoinResponse.memberId())
            .withGenerationId(leaderJoinResponse.generationId())
            .withAssignment(assignment)
            .build();

        SyncResult syncResult = context.sendGenericGroupSync(syncRequest
            .setGenerationId(nextGenerationId));

        // Simulate a successful write to log. This will update the group's assignment with the new assignment.
        syncResult.appendFuture.complete(null);

        assertTrue(syncResult.syncFuture.isDone());
        assertEquals(Errors.NONE.code(), syncResult.syncFuture.get().errorCode());
        assertEquals(leaderAssignment, syncResult.syncFuture.get().assignment());

        // Sync group with follower to get new assignment.
        SyncResult followerSyncResult = context.sendGenericGroupSync(syncRequest.setMemberId(followerId)
            .setGenerationId(nextGenerationId));

        assertTrue(followerSyncResult.records.isEmpty());
        assertTrue(followerSyncResult.syncFuture.isDone());
        assertEquals(Errors.NONE.code(), followerSyncResult.syncFuture.get().errorCode());
        assertEquals(followerAssignment, followerSyncResult.syncFuture.get().assignment());
        assertTrue(group.isInState(STABLE));
    }

    @Test
    public void testSyncGroupLeaderAfterFollower() throws Exception {
        // To get a group of two members:
        // 1. join and sync with a single member (because we can't immediately join with two members)
        // 2. join and sync with the first member and a new member

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        JoinGroupResponseData leaderJoinResponse = context.joinGenericGroupAsDynamicMemberAndCompleteRebalance("group-id");
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup("group-id", false);

        JoinGroupRequestData joinRequest = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .withRebalanceTimeoutMs(10000)
            .withSessionTimeoutMs(5000)
            .build();

        CompletableFuture<JoinGroupResponseData> followerJoinFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(
            joinRequest,
            followerJoinFuture
        );

        assertTrue(result.records().isEmpty());
        assertFalse(followerJoinFuture.isDone());

        CompletableFuture<JoinGroupResponseData> leaderJoinFuture = new CompletableFuture<>();
        result = context.sendGenericGroupJoin(
            joinRequest
                .setMemberId(leaderJoinResponse.memberId()),
            leaderJoinFuture
        );

        assertTrue(result.records().isEmpty());
        assertTrue(leaderJoinFuture.isDone());
        assertTrue(followerJoinFuture.isDone());
        assertEquals(Errors.NONE.code(), leaderJoinFuture.get().errorCode());
        assertEquals(Errors.NONE.code(), followerJoinFuture.get().errorCode());
        assertEquals(leaderJoinFuture.get().generationId(), followerJoinFuture.get().generationId());
        assertEquals(leaderJoinResponse.memberId(), leaderJoinFuture.get().leader());
        assertEquals(leaderJoinResponse.memberId(), followerJoinFuture.get().leader());
        assertTrue(group.isInState(COMPLETING_REBALANCE));

        int nextGenerationId = leaderJoinFuture.get().generationId();
        String followerId = followerJoinFuture.get().memberId();
        byte[] leaderAssignment = new byte[]{0};
        byte[] followerAssignment = new byte[]{1};

        // Sync group with follower to get new assignment.
        SyncGroupRequestData syncRequest = new SyncGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(leaderJoinResponse.memberId())
            .withGenerationId(leaderJoinResponse.generationId())
            .build();

        SyncResult followerSyncResult = context.sendGenericGroupSync(syncRequest.setMemberId(followerId)
            .setGenerationId(nextGenerationId));

        assertTrue(followerSyncResult.records.isEmpty());
        assertFalse(followerSyncResult.syncFuture.isDone());

        // Sync group with leader to get new assignment.
        List<SyncGroupRequestAssignment> assignment = new ArrayList<>();
        assignment.add(new SyncGroupRequestAssignment()
            .setMemberId(leaderJoinResponse.memberId())
            .setAssignment(leaderAssignment)
        );
        assignment.add(new SyncGroupRequestAssignment()
            .setMemberId(followerId)
            .setAssignment(followerAssignment)
        );

        SyncResult syncResult = context.sendGenericGroupSync(syncRequest.setMemberId(leaderJoinResponse.memberId())
            .setGenerationId(nextGenerationId)
            .setAssignments(assignment));

        // Simulate a successful write to log. This will update the group assignment with the new assignment.
        syncResult.appendFuture.complete(null);

        Map<String, byte[]> updatedAssignment = assignment.stream().collect(Collectors.toMap(
            SyncGroupRequestAssignment::memberId, SyncGroupRequestAssignment::assignment
        ));

        assertEquals(
            Collections.singletonList(
                RecordHelpers.newGroupMetadataRecord(group, updatedAssignment, MetadataVersion.latest())),
            syncResult.records
        );

        assertTrue(syncResult.syncFuture.isDone());
        assertEquals(Errors.NONE.code(), syncResult.syncFuture.get().errorCode());
        assertEquals(leaderAssignment, syncResult.syncFuture.get().assignment());

        // Follower sync group should also be completed.
        assertEquals(Errors.NONE.code(), followerSyncResult.syncFuture.get().errorCode());
        assertEquals(followerAssignment, followerSyncResult.syncFuture.get().assignment());
        assertTrue(group.isInState(STABLE));
    }

    @Test
    public void testJoinGroupFromUnchangedLeaderShouldRebalance() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        JoinGroupResponseData leaderJoinResponse = context.joinGenericGroupAsDynamicMemberAndCompleteRebalance("group-id");

        // Join group from the leader should force the group to rebalance, which allows the
        // leader to push new assignment when local metadata changes
        CompletableFuture<JoinGroupResponseData> leaderRejoinFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(
            new JoinGroupRequestBuilder()
                .withGroupId("group-id")
                .withMemberId(leaderJoinResponse.memberId())
                .withDefaultProtocolTypeAndProtocols()
                .build(),
            leaderRejoinFuture
        );

        assertTrue(result.records().isEmpty());
        assertTrue(leaderRejoinFuture.isDone());
        assertEquals(Errors.NONE.code(), leaderRejoinFuture.get().errorCode());
        assertEquals(leaderJoinResponse.generationId() + 1, leaderRejoinFuture.get().generationId());
    }

    @Test
    public void testJoinGroupCompletionWhenPendingMemberJoins() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        GenericGroup group = context.createGenericGroup("group-id");

        // Set up a group in with a pending member. The test checks if the pending member joining
        // completes the rebalancing operation
        JoinGroupResponseData pendingMemberResponse = context.setupGroupWithPendingMember(group);

        // Compete join group for the pending member
        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(pendingMemberResponse.memberId())
            .withDefaultProtocolTypeAndProtocols()
            .build();

        CompletableFuture<JoinGroupResponseData> responseFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(request, responseFuture);

        assertTrue(result.records().isEmpty());
        assertTrue(responseFuture.isDone());
        assertEquals(Errors.NONE.code(), responseFuture.get().errorCode());
        assertTrue(group.isInState(COMPLETING_REBALANCE));
        assertEquals(3, group.allMembers().size());
        assertEquals(0, group.numPendingJoinMembers());
    }

    @Test
    public void testJoinGroupCompletionWhenPendingMemberTimesOut() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        GenericGroup group = context.createGenericGroup("group-id");

        // Set up a group in with a pending member. The test checks if the timeout of the pending member will
        // cause the group to return to a CompletingRebalance state.
        context.setupGroupWithPendingMember(group);

        // Advancing clock by > 2500 (session timeout for the third member)
        // and < 5000 (for first and second members). This will force the coordinator to attempt join
        // completion on heartbeat expiration (since we are in PendingRebalance stage).
        assertNoOrEmptyResult(context.sleep(3000));
        assertTrue(group.isInState(COMPLETING_REBALANCE));
        assertEquals(2, group.allMembers().size());
        assertEquals(0, group.numPendingJoinMembers());
    }

    @Test
    public void testGenerationIdIncrementsOnRebalance() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        JoinGroupResponseData leaderJoinResponse = context.joinGenericGroupAsDynamicMemberAndCompleteRebalance("group-id");

        CompletableFuture<JoinGroupResponseData> joinFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(
            new JoinGroupRequestBuilder()
                .withGroupId("group-id")
                .withMemberId(leaderJoinResponse.memberId())
                .withDefaultProtocolTypeAndProtocols()
                .build(),
            joinFuture);

        assertTrue(result.records().isEmpty());
        assertTrue(joinFuture.isDone());
        assertEquals(2, joinFuture.get().generationId());
        assertEquals(Errors.NONE.code(), joinFuture.get().errorCode());
    }

    private List<JoinGroupResponseMember> toJoinResponseMembers(GenericGroup group) {
        List<JoinGroupResponseMember> members = new ArrayList<>();
        String protocolName = group.protocolName().get();
        group.allMembers().forEach(member -> {
            members.add(new JoinGroupResponseMember()
                .setMemberId(member.memberId())
                .setGroupInstanceId(member.groupInstanceId().orElse(""))
                .setMetadata(member.metadata(protocolName)));
        });

        return members;
    }

    private void checkJoinGroupResponse(
        JoinGroupResponseData expectedResponse,
        JoinGroupResponseData actualResponse,
        GenericGroup group,
        GenericGroupState expectedState,
        Set<String> expectedGroupInstanceIds
    ) {
        assertEquals(expectedResponse, actualResponse);
        assertTrue(group.isInState(expectedState));

        Set<String> groupInstanceIds = actualResponse.members()
            .stream()
            .map(JoinGroupResponseData.JoinGroupResponseMember::groupInstanceId)
            .collect(Collectors.toSet());

        assertEquals(expectedGroupInstanceIds, groupInstanceIds);
    }

    private static JoinGroupRequestProtocolCollection toProtocols(String... protocolNames) {
        JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestProtocolCollection(0);
        List<String> topicNames = Arrays.asList("foo", "bar", "baz");
        for (int i = 0; i < protocolNames.length; i++) {
            protocols.add(new JoinGroupRequestProtocol()
                .setName(protocolNames[i])
                .setMetadata(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                    Collections.singletonList(topicNames.get(i % topicNames.size())))).array())
            );
        }
        return protocols;
    }

    private static class SyncGroupRequestBuilder {
        String groupId = null;
        String groupInstanceId = null;
        String memberId = null;
        String protocolType = "consumer";
        String protocolName = "range";
        int generationId = 0;
        List<SyncGroupRequestAssignment> assignment = Collections.emptyList();

        SyncGroupRequestBuilder withGroupId(String groupId) {
            this.groupId = groupId;
            return this;
        }

        SyncGroupRequestBuilder withGroupInstanceId(String groupInstanceId) {
            this.groupInstanceId = groupInstanceId;
            return this;
        }

        SyncGroupRequestBuilder withMemberId(String memberId) {
            this.memberId = memberId;
            return this;
        }

        SyncGroupRequestBuilder withGenerationId(int generationId) {
            this.generationId = generationId;
            return this;
        }

        SyncGroupRequestBuilder withProtocolType(String protocolType) {
            this.protocolType = protocolType;
            return this;
        }

        SyncGroupRequestBuilder withProtocolName(String protocolName) {
            this.protocolName = protocolName;
            return this;
        }

        SyncGroupRequestBuilder withAssignment(List<SyncGroupRequestAssignment> assignment) {
            this.assignment = assignment;
            return this;
        }


        SyncGroupRequestData build() {
            return new SyncGroupRequestData()
                .setGroupId(groupId)
                .setGroupInstanceId(groupInstanceId)
                .setMemberId(memberId)
                .setGenerationId(generationId)
                .setProtocolType(protocolType)
                .setProtocolName(protocolName)
                .setAssignments(assignment);
        }
    }

    private static Record newGroupMetadataRecord(
        GenericGroup group,
        MetadataVersion metadataVersion
    ) {
        return RecordHelpers.newGroupMetadataRecord(group, group.groupAssignment(), metadataVersion);
    }

    private static class RebalanceResult {
        int generationId;
        String leaderId;
        byte[] leaderAssignment;
        String followerId;
        byte[] followerAssignment;

        RebalanceResult(
            int generationId,
            String leaderId,
            byte[] leaderAssignment,
            String followerId,
            byte[] followerAssignment
        ) {
            this.generationId = generationId;
            this.leaderId = leaderId;
            this.leaderAssignment = leaderAssignment;
            this.followerId = followerId;
            this.followerAssignment = followerAssignment;
        }
    }

    private static class SyncResult {
        CompletableFuture<SyncGroupResponseData> syncFuture;
        List<Record> records;
        CompletableFuture<Void> appendFuture;

        public SyncResult(
            CompletableFuture<SyncGroupResponseData> syncFuture,
            CoordinatorResult<Void, Record> coordinatorResult
        ) {
            this.syncFuture = syncFuture;
            this.records = coordinatorResult.records();
            this.appendFuture = coordinatorResult.appendFuture();
        }
    }

    @Test
    public void testStaticMemberHeartbeatLeaderWithInvalidMemberId() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        RebalanceResult rebalanceResult = context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id"
        );

        SyncResult syncResult = context.sendGenericGroupSync(new SyncGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("leader-instance-id")
            .withMemberId(rebalanceResult.leaderId)
            .withGenerationId(rebalanceResult.generationId)
            .build());

        assertTrue(syncResult.records.isEmpty());
        assertTrue(syncResult.syncFuture.isDone());
        assertEquals(Errors.NONE.code(), syncResult.syncFuture.get().errorCode());

        HeartbeatRequestData heartbeatRequest = new HeartbeatRequestData()
            .setGroupId("group-id")
            .setMemberId(rebalanceResult.leaderId)
            .setGenerationId(rebalanceResult.generationId);

        HeartbeatResponseData validHeartbeatResponse = context.sendGenericGroupHeartbeat(heartbeatRequest);
        assertEquals(Errors.NONE.code(), validHeartbeatResponse.errorCode());

        assertThrows(FencedInstanceIdException.class, () -> context.sendGenericGroupHeartbeat(
            heartbeatRequest
                .setGroupInstanceId("leader-instance-id")
                .setMemberId("invalid-member-id")));
    }

    @Test
    public void testHeartbeatUnknownGroup() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        HeartbeatRequestData heartbeatRequest = new HeartbeatRequestData()
            .setGroupId("group-id")
            .setMemberId("member-id")
            .setGenerationId(-1);

        assertThrows(UnknownMemberIdException.class, () -> context.sendGenericGroupHeartbeat(heartbeatRequest));
    }

    @Test
    public void testHeartbeatDeadGroup() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        GenericGroup group = context.createGenericGroup("group-id");

        group.transitionTo(DEAD);

        HeartbeatRequestData heartbeatRequest = new HeartbeatRequestData()
            .setGroupId("group-id")
            .setMemberId("member-id")
            .setGenerationId(-1);

        assertThrows(CoordinatorNotAvailableException.class, () -> context.sendGenericGroupHeartbeat(heartbeatRequest));
    }

    @Test
    public void testHeartbeatEmptyGroup() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        GenericGroup group = context.createGenericGroup("group-id");

        group.add(new GenericGroupMember(
            "member-id",
            Optional.empty(),
            "client-id",
            "client-host",
            10000,
            5000,
            "consumer",
            toProtocols("range")
        ));

        HeartbeatRequestData heartbeatRequest = new HeartbeatRequestData()
            .setGroupId("group-id")
            .setMemberId("member-id")
            .setGenerationId(0);

        HeartbeatResponseData heartbeatResponse = context.sendGenericGroupHeartbeat(heartbeatRequest);
        assertEquals(Errors.UNKNOWN_MEMBER_ID.code(), heartbeatResponse.errorCode());
    }

    @Test
    public void testHeartbeatUnknownMemberExistingGroup() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        JoinGroupResponseData leaderJoinResponse = context.joinGenericGroupAsDynamicMemberAndCompleteRebalance("group-id");

        assertThrows(UnknownMemberIdException.class, () -> context.sendGenericGroupHeartbeat(new HeartbeatRequestData()
            .setGroupId("group-id")
            .setMemberId("unknown-member-id")
            .setGenerationId(leaderJoinResponse.generationId())));
    }

    @Test
    public void testHeartbeatDuringPreparingRebalance() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        GenericGroup group = context.createGenericGroup("group-id");

        JoinGroupRequestData joinRequest = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        CompletableFuture<JoinGroupResponseData> joinFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(joinRequest, joinFuture, true);

        assertTrue(result.records().isEmpty());
        assertTrue(joinFuture.isDone());
        assertEquals(Errors.MEMBER_ID_REQUIRED.code(), joinFuture.get().errorCode());

        String memberId = joinFuture.get().memberId();

        joinFuture = new CompletableFuture<>();
        context.sendGenericGroupJoin(joinRequest
            .setMemberId(memberId), joinFuture);

        assertTrue(group.isInState(PREPARING_REBALANCE));

        HeartbeatResponseData heartbeatResponse = context.sendGenericGroupHeartbeat(new HeartbeatRequestData()
            .setGroupId("group-id")
            .setMemberId(memberId)
            .setGenerationId(0));

        assertEquals(Errors.REBALANCE_IN_PROGRESS.code(), heartbeatResponse.errorCode());
    }

    @Test
    public void testHeartbeatDuringCompletingRebalance() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        GenericGroup group = context.createGenericGroup("group-id");

        JoinGroupResponseData leaderJoinResponse =
            context.joinGenericGroupAsDynamicMemberAndCompleteJoin(new JoinGroupRequestBuilder()
                .withGroupId("group-id")
                .withMemberId(UNKNOWN_MEMBER_ID)
                .withDefaultProtocolTypeAndProtocols()
                .build());

        assertEquals(1, leaderJoinResponse.generationId());
        assertTrue(group.isInState(COMPLETING_REBALANCE));

        HeartbeatResponseData heartbeatResponse = context.sendGenericGroupHeartbeat(new HeartbeatRequestData()
            .setGroupId("group-id")
            .setMemberId(leaderJoinResponse.memberId())
            .setGenerationId(leaderJoinResponse.generationId()));

        assertEquals(new HeartbeatResponseData(), heartbeatResponse);
    }

    @Test
    public void testHeartbeatIllegalGeneration() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        JoinGroupResponseData leaderJoinResponse = context.joinGenericGroupAsDynamicMemberAndCompleteRebalance("group-id");

        assertThrows(IllegalGenerationException.class, () -> {
            context.sendGenericGroupHeartbeat(new HeartbeatRequestData()
                .setGroupId("group-id")
                .setMemberId(leaderJoinResponse.memberId())
                .setGenerationId(leaderJoinResponse.generationId() + 1));
        });
    }

    @Test
    public void testValidHeartbeat() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        JoinGroupResponseData leaderJoinResponse = context.joinGenericGroupAsDynamicMemberAndCompleteRebalance("group-id");

        HeartbeatResponseData heartbeatResponse = context.sendGenericGroupHeartbeat(new HeartbeatRequestData()
            .setGroupId("group-id")
            .setMemberId(leaderJoinResponse.memberId())
            .setGenerationId(leaderJoinResponse.generationId()));

        assertEquals(Errors.NONE.code(), heartbeatResponse.errorCode());
    }

    @Test
    public void testGenericGroupMemberSessionTimeout() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        JoinGroupResponseData leaderJoinResponse = context.joinGenericGroupAsDynamicMemberAndCompleteRebalance("group-id");
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup("group-id", false);

        // Advance clock by session timeout to kick member out.
        context.verifySessionExpiration(group, 5000);

        assertThrows(UnknownMemberIdException.class, () -> context.sendGenericGroupHeartbeat(new HeartbeatRequestData()
            .setGroupId("group-id")
            .setMemberId(leaderJoinResponse.memberId())
            .setGenerationId(leaderJoinResponse.generationId())));
    }

    @Test
    public void testGenericGroupMemberHeartbeatMaintainSession() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        JoinGroupResponseData leaderJoinResponse = context.joinGenericGroupAsDynamicMemberAndCompleteRebalance("group-id");

        // Advance clock by 1/2 of session timeout.
        assertNoOrEmptyResult(context.sleep(2500));

        HeartbeatRequestData heartbeatRequest = new HeartbeatRequestData()
            .setGroupId("group-id")
            .setMemberId(leaderJoinResponse.memberId())
            .setGenerationId(leaderJoinResponse.generationId());

        HeartbeatResponseData heartbeatResponse = context.sendGenericGroupHeartbeat(heartbeatRequest);
        assertEquals(Errors.NONE.code(), heartbeatResponse.errorCode());

        assertNoOrEmptyResult(context.sleep(2500));

        heartbeatResponse = context.sendGenericGroupHeartbeat(heartbeatRequest);
        assertEquals(Errors.NONE.code(), heartbeatResponse.errorCode());
    }

    @Test
    public void testGenericGroupMemberSessionTimeoutDuringRebalance() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        JoinGroupResponseData leaderJoinResponse = context.joinGenericGroupAsDynamicMemberAndCompleteRebalance("group-id");
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup("group-id", false);

        // Add a new member. This should trigger a rebalance. The new member has the
        // 'genericGroupNewMemberJoinTimeoutMs` session timeout, so it has a longer expiration than the existing member.
        CompletableFuture<JoinGroupResponseData> otherJoinFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(
            new JoinGroupRequestBuilder()
                .withGroupId("group-id")
                .withMemberId(UNKNOWN_MEMBER_ID)
                .withDefaultProtocolTypeAndProtocols()
                .withRebalanceTimeoutMs(10000)
                .withSessionTimeoutMs(5000)
                .build(),
            otherJoinFuture
        );

        assertTrue(result.records().isEmpty());
        assertFalse(otherJoinFuture.isDone());
        assertTrue(group.isInState(PREPARING_REBALANCE));

        // Advance clock by 1/2 of session timeout.
        assertNoOrEmptyResult(context.sleep(2500));

        HeartbeatRequestData heartbeatRequest = new HeartbeatRequestData()
            .setGroupId("group-id")
            .setMemberId(leaderJoinResponse.memberId())
            .setGenerationId(leaderJoinResponse.generationId());

        HeartbeatResponseData heartbeatResponse = context.sendGenericGroupHeartbeat(heartbeatRequest);
        assertEquals(Errors.REBALANCE_IN_PROGRESS.code(), heartbeatResponse.errorCode());

        // Advance clock by first member's session timeout.
        assertNoOrEmptyResult(context.sleep(5000));

        assertThrows(UnknownMemberIdException.class, () -> context.sendGenericGroupHeartbeat(heartbeatRequest));

        // Advance clock by remaining rebalance timeout to complete join phase.
        assertNoOrEmptyResult(context.sleep(2500));

        assertTrue(otherJoinFuture.isDone());
        assertEquals(Errors.NONE.code(), otherJoinFuture.get().errorCode());
        assertEquals(1, group.size());
        assertEquals(2, group.generationId());
        assertTrue(group.isInState(COMPLETING_REBALANCE));
    }

    @Test
    public void testRebalanceCompletesBeforeMemberJoins() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        GenericGroup group = context.createGenericGroup("group-id");

        // Create a group with a single member
        JoinGroupRequestData joinRequest = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("leader-instance-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .withRebalanceTimeoutMs(10000)
            .withSessionTimeoutMs(5000)
            .build();

        JoinGroupResponseData leaderJoinResponse =
            context.joinGenericGroupAndCompleteJoin(joinRequest, true, true);

        String firstMemberId = leaderJoinResponse.memberId();
        int firstGenerationId = leaderJoinResponse.generationId();

        assertEquals(1, firstGenerationId);
        assertTrue(group.isInState(COMPLETING_REBALANCE));

        SyncGroupRequestData syncRequest = new SyncGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(firstMemberId)
            .withGenerationId(firstGenerationId)
            .build();

        SyncResult syncResult = context.sendGenericGroupSync(syncRequest);

        // Simulate a successful write to the log.
        syncResult.appendFuture.complete(null);

        assertTrue(syncResult.syncFuture.isDone());
        assertEquals(Errors.NONE.code(), syncResult.syncFuture.get().errorCode());
        assertTrue(group.isInState(STABLE));

        // Add a new dynamic member. This should trigger a rebalance. The new member has the
        // 'genericGroupNewMemberJoinTimeoutMs` session timeout, so it has a longer expiration than the existing member.
        CompletableFuture<JoinGroupResponseData> secondMemberJoinFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(
            joinRequest
                .setMemberId(UNKNOWN_MEMBER_ID)
                .setGroupInstanceId(null)
                .setSessionTimeoutMs(2500),
            secondMemberJoinFuture);

        assertTrue(result.records().isEmpty());
        assertFalse(secondMemberJoinFuture.isDone());
        assertTrue(group.isInState(PREPARING_REBALANCE));

        // Send a couple heartbeats to keep the first member alive while the rebalance finishes.
        HeartbeatRequestData firstMemberHeartbeatRequest = new HeartbeatRequestData()
            .setGroupId("group-id")
            .setMemberId(firstMemberId)
            .setGenerationId(firstGenerationId);

        for (int i = 0; i < 2; i++) {
            assertNoOrEmptyResult(context.sleep(2500));
            HeartbeatResponseData heartbeatResponse = context.sendGenericGroupHeartbeat(firstMemberHeartbeatRequest);
            assertEquals(Errors.REBALANCE_IN_PROGRESS.code(), heartbeatResponse.errorCode());
        }

        // Advance clock by remaining rebalance timeout to complete join phase.
        // The second member will become the leader. However, as the first member is a static member
        // it will not be kicked out.
        assertNoOrEmptyResult(context.sleep(8000));

        assertTrue(secondMemberJoinFuture.isDone());
        assertEquals(Errors.NONE.code(), secondMemberJoinFuture.get().errorCode());
        assertEquals(2, group.size());
        assertEquals(2, group.generationId());
        assertTrue(group.isInState(COMPLETING_REBALANCE));

        String otherMemberId = secondMemberJoinFuture.get().memberId();

        syncResult = context.sendGenericGroupSync(
            syncRequest
                .setGroupInstanceId(null)
                .setMemberId(otherMemberId)
                .setGenerationId(2));

        // Simulate a successful write to the log.
        syncResult.appendFuture.complete(null);

        assertTrue(syncResult.syncFuture.isDone());
        assertEquals(Errors.NONE.code(), syncResult.syncFuture.get().errorCode());
        assertTrue(group.isInState(STABLE));

        // The unjoined (first) static member should be remained in the group before session timeout.
        assertThrows(IllegalGenerationException.class, () -> context.sendGenericGroupHeartbeat(firstMemberHeartbeatRequest));

        // Now session timeout the unjoined (first) member. Still keeping the new member.
        List<Errors> expectedErrors = Arrays.asList(Errors.NONE, Errors.NONE, Errors.REBALANCE_IN_PROGRESS);
        for (Errors expectedError : expectedErrors) {
            assertNoOrEmptyResult(context.sleep(2000));
            HeartbeatResponseData heartbeatResponse = context.sendGenericGroupHeartbeat(
                firstMemberHeartbeatRequest
                    .setMemberId(otherMemberId)
                    .setGenerationId(2));

            assertEquals(expectedError.code(), heartbeatResponse.errorCode());
        }
        assertEquals(1, group.size());
        assertTrue(group.isInState(PREPARING_REBALANCE));

        CompletableFuture<JoinGroupResponseData> otherMemberRejoinFuture = new CompletableFuture<>();
        result = context.sendGenericGroupJoin(
            joinRequest
                .setMemberId(otherMemberId)
                .setGroupInstanceId(null)
                .setSessionTimeoutMs(2500),
            otherMemberRejoinFuture);

        assertTrue(result.records().isEmpty());
        assertTrue(otherMemberRejoinFuture.isDone());
        assertEquals(Errors.NONE.code(), otherMemberRejoinFuture.get().errorCode());
        assertEquals(3, otherMemberRejoinFuture.get().generationId());
        assertTrue(group.isInState(COMPLETING_REBALANCE));

        SyncResult otherMemberResyncResult = context.sendGenericGroupSync(
            syncRequest
                .setGroupInstanceId(null)
                .setMemberId(otherMemberId)
                .setGenerationId(3));

        // Simulate a successful write to the log.
        otherMemberResyncResult.appendFuture.complete(null);

        assertTrue(otherMemberResyncResult.syncFuture.isDone());
        assertEquals(Errors.NONE.code(), otherMemberResyncResult.syncFuture.get().errorCode());
        assertTrue(group.isInState(STABLE));

        // The joined member should get heart beat response with no error. Let the new member keep
        // heartbeating for a while to verify that no new rebalance is triggered unexpectedly.
        for (int i = 0; i < 20; i++) {
            assertNoOrEmptyResult(context.sleep(2000));
            HeartbeatResponseData heartbeatResponse = context.sendGenericGroupHeartbeat(
                firstMemberHeartbeatRequest
                    .setMemberId(otherMemberId)
                    .setGenerationId(3));

            assertEquals(Errors.NONE.code(), heartbeatResponse.errorCode());
        }
    }

    @Test
    public void testSyncGroupEmptyAssignment() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        JoinGroupResponseData leaderJoinResponse = context.joinGenericGroupAsDynamicMemberAndCompleteRebalance("group-id");

        HeartbeatResponseData heartbeatResponse = context.sendGenericGroupHeartbeat(new HeartbeatRequestData()
            .setGroupId("group-id")
            .setMemberId(leaderJoinResponse.memberId())
            .setGenerationId(leaderJoinResponse.generationId()));

        assertEquals(Errors.NONE.code(), heartbeatResponse.errorCode());
    }

    @Test
    public void testSecondMemberPartiallyJoinAndTimeout() throws Exception {
        // Test if the following scenario completes a rebalance correctly: A new member starts a JoinGroup request with
        // an UNKNOWN_MEMBER_ID, attempting to join a stable group. But never initiates the second JoinGroup request with
        // the provided member ID and times out. The test checks if original member remains the sole member in this group,
        // which should remain stable throughout this test.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        GenericGroup group = context.createGenericGroup("group-id");

        // Create a group with a single member
        JoinGroupRequestData joinRequest = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("leader-instance-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .withRebalanceTimeoutMs(10000)
            .withSessionTimeoutMs(5000)
            .build();

        JoinGroupResponseData leaderJoinResponse =
            context.joinGenericGroupAndCompleteJoin(joinRequest, true, true);

        String firstMemberId = leaderJoinResponse.memberId();
        int firstGenerationId = leaderJoinResponse.generationId();

        assertEquals(1, firstGenerationId);
        assertTrue(group.isInState(COMPLETING_REBALANCE));

        SyncResult syncResult = context.sendGenericGroupSync(new SyncGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(firstMemberId)
            .withGenerationId(firstGenerationId)
            .build());

        // Simulate a successful write to the log.
        syncResult.appendFuture.complete(null);

        assertTrue(syncResult.syncFuture.isDone());
        assertEquals(Errors.NONE.code(), syncResult.syncFuture.get().errorCode());
        assertTrue(group.isInState(STABLE));

        // Add a new dynamic pending member.
        CompletableFuture<JoinGroupResponseData> joinFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(
            joinRequest
                .setMemberId(UNKNOWN_MEMBER_ID)
                .setGroupInstanceId(null)
                .setSessionTimeoutMs(5000),
            joinFuture,
            true,
            true
        );

        assertTrue(result.records().isEmpty());
        assertTrue(joinFuture.isDone());
        assertEquals(Errors.MEMBER_ID_REQUIRED.code(), joinFuture.get().errorCode());
        assertEquals(1, group.numPendingJoinMembers());
        assertTrue(group.isInState(STABLE));

        // Heartbeat from the leader to maintain session while timing out pending member.
        HeartbeatRequestData heartbeatRequest = new HeartbeatRequestData()
            .setGroupId("group-id")
            .setMemberId(firstMemberId)
            .setGenerationId(firstGenerationId);

        for (int i = 0; i < 2; i++) {
            assertNoOrEmptyResult(context.sleep(2500));
            HeartbeatResponseData heartbeatResponse = context.sendGenericGroupHeartbeat(heartbeatRequest);
            assertEquals(Errors.NONE.code(), heartbeatResponse.errorCode());
        }

        // At this point the second member should have been removed from pending list (session timeout),
        // and the group should be in Stable state with only the first member in it.
        assertEquals(1, group.size());
        assertTrue(group.hasMemberId(firstMemberId));
        assertEquals(1, group.generationId());
        assertTrue(group.isInState(STABLE));
    }

    @Test
    public void testRebalanceTimesOutWhenSyncRequestIsNotReceived() throws Exception {
        // This test case ensure that the pending sync expiration does kick out all members
        // if they don't send sync requests before the rebalance timeout. The
        // group is in the Empty state in this case.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        int rebalanceTimeoutMs = 5000;
        int sessionTimeoutMs = 5000;
        List<JoinGroupResponseData> joinResponses = context.joinWithNMembers("group-id", 3, rebalanceTimeoutMs, sessionTimeoutMs);
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup("group-id", false);

        // Advance clock by 1/2 rebalance timeout.
        assertNoOrEmptyResult(context.sleep(rebalanceTimeoutMs / 2));

        // Heartbeats to ensure that heartbeating does not interfere with the
        // delayed sync operation.
        joinResponses.forEach(response -> context.verifyHeartbeat(group.groupId(), response, Errors.NONE));

        // Advance clock by 1/2 rebalance timeout to expire the pending sync. Members should be removed.
        // The group becomes empty, generating an empty group metadata record.
        List<ExpiredTimeout<Void, Record>> timeouts = context.sleep(rebalanceTimeoutMs / 2);
        assertEquals(1, timeouts.size());
        ExpiredTimeout<Void, Record> timeout = timeouts.get(0);
        assertEquals(genericGroupSyncKey("group-id"), timeout.key);
        assertEquals(
            Collections.singletonList(newGroupMetadataRecord(group, MetadataVersion.latest())),
            timeout.result.records()
        );

        // Simulate a successful write to the log.
        timeout.result.appendFuture().complete(null);

        // Heartbeats fail because none of the members have sent the sync request
        joinResponses.forEach(response -> context.verifyHeartbeat(group.groupId(), response, Errors.UNKNOWN_MEMBER_ID));
        assertTrue(group.isInState(EMPTY));
    }

    @Test
    public void testRebalanceTimesOutWhenSyncRequestIsNotReceivedFromFollowers() throws Exception {
        // This test case ensure that the pending sync expiration does kick out the followers
        // if they don't send a sync request before the rebalance timeout. The
        // group is in the PreparingRebalance state in this case.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        int rebalanceTimeoutMs = 5000;
        int sessionTimeoutMs = 5000;
        List<JoinGroupResponseData> joinResponses = context.joinWithNMembers("group-id", 3, rebalanceTimeoutMs, sessionTimeoutMs);
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup("group-id", false);

        // Advance clock by 1/2 rebalance timeout.
        assertNoOrEmptyResult(context.sleep(rebalanceTimeoutMs / 2));

        // Heartbeats to ensure that heartbeating does not interfere with the
        // delayed sync operation.
        joinResponses.forEach(response -> context.verifyHeartbeat(group.groupId(), response, Errors.NONE));

        // Leader sends a sync group request.
        SyncResult syncResult = context.sendGenericGroupSync(new SyncGroupRequestBuilder()
            .withGroupId("group-id")
            .withGenerationId(1)
            .withMemberId(joinResponses.get(0).memberId())
            .build());

        // Simulate a successful write to the log.
        syncResult.appendFuture.complete(null);

        assertTrue(syncResult.syncFuture.isDone());
        assertEquals(Errors.NONE.code(), syncResult.syncFuture.get().errorCode());
        assertTrue(group.isInState(STABLE));

        // Leader should be able to heartbeat
        context.verifyHeartbeat(group.groupId(), joinResponses.get(0), Errors.NONE);

        // Advance clock by 1/2 rebalance timeout to expire the pending sync. Followers should be removed.
        List<ExpiredTimeout<Void, Record>> timeouts = context.sleep(rebalanceTimeoutMs / 2);
        assertEquals(1, timeouts.size());
        ExpiredTimeout<Void, Record> timeout = timeouts.get(0);
        assertEquals(genericGroupSyncKey("group-id"), timeout.key);
        assertTrue(timeout.result.records().isEmpty());

        // Leader should be able to heartbeat
        joinResponses.subList(0, 1).forEach(response ->
            context.verifyHeartbeat(group.groupId(), response, Errors.REBALANCE_IN_PROGRESS));

        // Heartbeats fail because none of the followers have sent the sync request
        joinResponses.subList(1, 3).forEach(response ->
            context.verifyHeartbeat(group.groupId(), response, Errors.UNKNOWN_MEMBER_ID));

        assertTrue(group.isInState(PREPARING_REBALANCE));
    }

    @Test
    public void testRebalanceTimesOutWhenSyncRequestIsNotReceivedFromLeaders() throws Exception {
        // This test case ensure that the pending sync expiration does kick out the leader
        // if it does not send a sync request before the rebalance timeout. The
        // group is in the PreparingRebalance state in this case.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        int rebalanceTimeoutMs = 5000;
        int sessionTimeoutMs = 5000;
        List<JoinGroupResponseData> joinResponses = context.joinWithNMembers("group-id", 3, rebalanceTimeoutMs, sessionTimeoutMs);
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup("group-id", false);

        // Advance clock by 1/2 rebalance timeout.
        assertNoOrEmptyResult(context.sleep(rebalanceTimeoutMs / 2));

        // Heartbeats to ensure that heartbeating does not interfere with the
        // delayed sync operation.
        joinResponses.forEach(response -> context.verifyHeartbeat(group.groupId(), response, Errors.NONE));

        // Followers send sync group requests.
        List<CompletableFuture<SyncGroupResponseData>> followerSyncFutures = joinResponses.subList(1, 3).stream()
            .map(response -> {
                SyncResult syncResult = context.sendGenericGroupSync(new SyncGroupRequestBuilder()
                    .withGroupId("group-id")
                    .withGenerationId(1)
                    .withMemberId(response.memberId())
                    .build());

                assertTrue(syncResult.records.isEmpty());
                assertFalse(syncResult.syncFuture.isDone());
                return syncResult.syncFuture;
            }).collect(Collectors.toList());

        // Advance clock by 1/2 rebalance timeout to expire the pending sync. Leader should be kicked out.
        List<ExpiredTimeout<Void, Record>> timeouts = context.sleep(rebalanceTimeoutMs / 2);
        assertEquals(1, timeouts.size());
        ExpiredTimeout<Void, Record> timeout = timeouts.get(0);
        assertEquals(genericGroupSyncKey("group-id"), timeout.key);
        assertTrue(timeout.result.records().isEmpty());

        // Follower sync responses should fail.
        followerSyncFutures.forEach(future -> {
            assertTrue(future.isDone());
            try {
                assertEquals(Errors.REBALANCE_IN_PROGRESS.code(), future.get().errorCode());
            } catch (Exception e) {
                fail("Unexpected exception: " + e.getMessage());
            }
        });

        // Leader heartbeat should fail.
        joinResponses.subList(0, 1).forEach(response ->
            context.verifyHeartbeat(group.groupId(), response, Errors.UNKNOWN_MEMBER_ID));

        // Follower heartbeats should succeed.
        joinResponses.subList(1, 3).forEach(response ->
            context.verifyHeartbeat(group.groupId(), response, Errors.REBALANCE_IN_PROGRESS));

        assertTrue(group.isInState(PREPARING_REBALANCE));
    }

    @Test
    public void testRebalanceDoesNotTimeOutWhenAllSyncAreReceived() throws Exception {
        // This test case ensure that the pending sync expiration does not kick any
        // members out when they have all sent their sync requests. Group should be in Stable state.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        int rebalanceTimeoutMs = 5000;
        int sessionTimeoutMs = 5000;
        List<JoinGroupResponseData> joinResponses = context.joinWithNMembers("group-id", 3, rebalanceTimeoutMs, sessionTimeoutMs);
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup("group-id", false);
        String leaderId = joinResponses.get(0).memberId();

        // Advance clock by 1/2 rebalance timeout.
        assertNoOrEmptyResult(context.sleep(rebalanceTimeoutMs / 2));

        // Heartbeats to ensure that heartbeating does not interfere with the
        // delayed sync operation.
        joinResponses.forEach(response -> context.verifyHeartbeat(group.groupId(), response, Errors.NONE));

        // All members send sync group requests.
        List<CompletableFuture<SyncGroupResponseData>> syncFutures = joinResponses.stream().map(response -> {
            SyncResult syncResult = context.sendGenericGroupSync(new SyncGroupRequestBuilder()
                .withGroupId("group-id")
                .withGenerationId(1)
                .withMemberId(response.memberId())
                .build());

            if (response.memberId().equals(leaderId)) {
                assertEquals(
                    Collections.singletonList(newGroupMetadataRecord(group, MetadataVersion.latest())),
                    syncResult.records
                );

                // Simulate a successful write to the log.
                syncResult.appendFuture.complete(null);
            } else {
                assertTrue(syncResult.records.isEmpty());
            }
            assertTrue(syncResult.syncFuture.isDone());
            return syncResult.syncFuture;
        }).collect(Collectors.toList());

        for (CompletableFuture<SyncGroupResponseData> syncFuture : syncFutures) {
            assertEquals(Errors.NONE.code(), syncFuture.get().errorCode());
        }

        // Advance clock by 1/2 rebalance timeout. Pending sync should already have been cancelled.
        assertNoOrEmptyResult(context.sleep(rebalanceTimeoutMs / 2));

        // All member heartbeats should succeed.
        joinResponses.forEach(response ->
            context.verifyHeartbeat(group.groupId(), response, Errors.NONE));

        // Advance clock a bit more
        assertNoOrEmptyResult(context.sleep(rebalanceTimeoutMs / 2));

        // All member heartbeats should succeed.
        joinResponses.forEach(response ->
            context.verifyHeartbeat(group.groupId(), response, Errors.NONE));

        assertTrue(group.isInState(STABLE));
    }

    @Test
    public void testHeartbeatDuringRebalanceCausesRebalanceInProgress() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        GenericGroup group = context.createGenericGroup("group-id");

        // First start up a group (with a slightly larger timeout to give us time to heartbeat when the rebalance starts)
        JoinGroupRequestData joinRequest = new JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .withRebalanceTimeoutMs(10000)
            .withSessionTimeoutMs(5000)
            .build();

        JoinGroupResponseData leaderJoinResponse =
            context.joinGenericGroupAsDynamicMemberAndCompleteJoin(joinRequest);

        assertEquals(1, leaderJoinResponse.generationId());
        assertTrue(group.isInState(COMPLETING_REBALANCE));

        // Then join with a new consumer to trigger a rebalance
        CompletableFuture<JoinGroupResponseData> joinFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> result = context.sendGenericGroupJoin(
            joinRequest
                .setMemberId(UNKNOWN_MEMBER_ID),
            joinFuture
        );

        assertTrue(result.records().isEmpty());
        assertFalse(joinFuture.isDone());

        // We should be in the middle of a rebalance, so the heartbeat should return rebalance in progress.
        HeartbeatRequestData heartbeatRequest = new HeartbeatRequestData()
            .setGroupId("group-id")
            .setMemberId(leaderJoinResponse.memberId())
            .setGenerationId(leaderJoinResponse.generationId());

        HeartbeatResponseData heartbeatResponse = context.sendGenericGroupHeartbeat(heartbeatRequest);
        assertEquals(Errors.REBALANCE_IN_PROGRESS.code(), heartbeatResponse.errorCode());
    }
}
