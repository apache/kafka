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
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.SyncGroupResponseData;
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
import org.apache.kafka.coordinator.group.assignor.PartitionAssignor;
import org.apache.kafka.coordinator.group.classic.ClassicGroup;
import org.apache.kafka.coordinator.group.consumer.ConsumerGroup;
import org.apache.kafka.coordinator.group.consumer.ConsumerGroupBuilder;
import org.apache.kafka.coordinator.group.consumer.MemberState;
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
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetricsShard;
import org.apache.kafka.coordinator.group.runtime.CoordinatorResult;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.timeline.SnapshotRegistry;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.common.requests.JoinGroupRequest.UNKNOWN_MEMBER_ID;
import static org.apache.kafka.coordinator.group.GroupMetadataManager.EMPTY_RESULT;
import static org.apache.kafka.coordinator.group.GroupMetadataManager.classicGroupHeartbeatKey;
import static org.apache.kafka.coordinator.group.GroupMetadataManager.consumerGroupRebalanceTimeoutKey;
import static org.apache.kafka.coordinator.group.GroupMetadataManager.consumerGroupSessionTimeoutKey;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.COMPLETING_REBALANCE;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.DEAD;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.EMPTY;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.PREPARING_REBALANCE;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.STABLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

public class GroupMetadataManagerTestContext {

    public static void assertNoOrEmptyResult(List<MockCoordinatorTimer.ExpiredTimeout<Void, Record>> timeouts) {
        assertTrue(timeouts.size() <= 1);
        timeouts.forEach(timeout -> assertEquals(EMPTY_RESULT, timeout.result));
    }

    public static JoinGroupRequestData.JoinGroupRequestProtocolCollection toProtocols(String... protocolNames) {
        JoinGroupRequestData.JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestData.JoinGroupRequestProtocolCollection(0);
        List<String> topicNames = Arrays.asList("foo", "bar", "baz");
        for (int i = 0; i < protocolNames.length; i++) {
            protocols.add(new JoinGroupRequestData.JoinGroupRequestProtocol()
                .setName(protocolNames[i])
                .setMetadata(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                    Collections.singletonList(topicNames.get(i % topicNames.size())))).array())
            );
        }
        return protocols;
    }

    public static Record newGroupMetadataRecord(
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

    public static class RebalanceResult {
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

    public static class PendingMemberGroupResult {
        String leaderId;
        String followerId;
        JoinGroupResponseData pendingMemberResponse;

        public PendingMemberGroupResult(
            String leaderId,
            String followerId,
            JoinGroupResponseData pendingMemberResponse
        ) {
            this.leaderId = leaderId;
            this.followerId = followerId;
            this.pendingMemberResponse = pendingMemberResponse;
        }
    }

    public static class JoinResult {
        CompletableFuture<JoinGroupResponseData> joinFuture;
        List<Record> records;
        CompletableFuture<Void> appendFuture;

        public JoinResult(
            CompletableFuture<JoinGroupResponseData> joinFuture,
            CoordinatorResult<Void, Record> coordinatorResult
        ) {
            this.joinFuture = joinFuture;
            this.records = coordinatorResult.records();
            this.appendFuture = coordinatorResult.appendFuture();
        }
    }

    public static class SyncResult {
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

    public static class JoinGroupRequestBuilder {
        String groupId = null;
        String groupInstanceId = null;
        String memberId = null;
        String protocolType = "consumer";
        JoinGroupRequestData.JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestData.JoinGroupRequestProtocolCollection(0);
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

        JoinGroupRequestBuilder withProtocols(JoinGroupRequestData.JoinGroupRequestProtocolCollection protocols) {
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

    public static class SyncGroupRequestBuilder {
        String groupId = null;
        String groupInstanceId = null;
        String memberId = null;
        String protocolType = "consumer";
        String protocolName = "range";
        int generationId = 0;
        List<SyncGroupRequestData.SyncGroupRequestAssignment> assignment = Collections.emptyList();

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

        SyncGroupRequestBuilder withAssignment(List<SyncGroupRequestData.SyncGroupRequestAssignment> assignment) {
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

    public static class Builder {
        final private MockTime time = new MockTime();
        final private MockCoordinatorTimer<Void, Record> timer = new MockCoordinatorTimer<>(time);
        final private LogContext logContext = new LogContext();
        final private SnapshotRegistry snapshotRegistry = new SnapshotRegistry(logContext);
        private MetadataImage metadataImage;
        private List<PartitionAssignor> consumerGroupAssignors = Collections.singletonList(new MockPartitionAssignor("range"));
        final private List<ConsumerGroupBuilder> consumerGroupBuilders = new ArrayList<>();
        private int consumerGroupMaxSize = Integer.MAX_VALUE;
        private int consumerGroupMetadataRefreshIntervalMs = Integer.MAX_VALUE;
        private int classicGroupMaxSize = Integer.MAX_VALUE;
        private int classicGroupInitialRebalanceDelayMs = 3000;
        final private int classicGroupNewMemberJoinTimeoutMs = 5 * 60 * 1000;
        private int classicGroupMinSessionTimeoutMs = 10;
        private int classicGroupMaxSessionTimeoutMs = 10 * 60 * 1000;
        final private GroupCoordinatorMetricsShard metrics = mock(GroupCoordinatorMetricsShard.class);

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

        public Builder withClassicGroupMaxSize(int classicGroupMaxSize) {
            this.classicGroupMaxSize = classicGroupMaxSize;
            return this;
        }

        public Builder withClassicGroupInitialRebalanceDelayMs(int classicGroupInitialRebalanceDelayMs) {
            this.classicGroupInitialRebalanceDelayMs = classicGroupInitialRebalanceDelayMs;
            return this;
        }

        public Builder withClassicGroupMinSessionTimeoutMs(int classicGroupMinSessionTimeoutMs) {
            this.classicGroupMinSessionTimeoutMs = classicGroupMinSessionTimeoutMs;
            return this;
        }

        public Builder withClassicGroupMaxSessionTimeoutMs(int classicGroupMaxSessionTimeoutMs) {
            this.classicGroupMaxSessionTimeoutMs = classicGroupMaxSessionTimeoutMs;
            return this;
        }

        public GroupMetadataManagerTestContext build() {
            if (metadataImage == null) metadataImage = MetadataImage.EMPTY;
            if (consumerGroupAssignors == null) consumerGroupAssignors = Collections.emptyList();

            GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext(
                time,
                timer,
                snapshotRegistry,
                metrics,
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
                    .withClassicGroupMaxSize(classicGroupMaxSize)
                    .withClassicGroupMinSessionTimeoutMs(classicGroupMinSessionTimeoutMs)
                    .withClassicGroupMaxSessionTimeoutMs(classicGroupMaxSessionTimeoutMs)
                    .withClassicGroupInitialRebalanceDelayMs(classicGroupInitialRebalanceDelayMs)
                    .withClassicGroupNewMemberJoinTimeoutMs(classicGroupNewMemberJoinTimeoutMs)
                    .withGroupCoordinatorMetricsShard(metrics)
                    .build(),
                classicGroupInitialRebalanceDelayMs,
                classicGroupNewMemberJoinTimeoutMs
            );

            consumerGroupBuilders.forEach(builder -> builder.build(metadataImage.topics()).forEach(context::replay));

            context.commit();

            return context;
        }
    }

    final MockTime time;
    final MockCoordinatorTimer<Void, Record> timer;
    final SnapshotRegistry snapshotRegistry;
    final GroupCoordinatorMetricsShard metrics;
    final GroupMetadataManager groupMetadataManager;
    final int classicGroupInitialRebalanceDelayMs;
    final int classicGroupNewMemberJoinTimeoutMs;

    long lastCommittedOffset = 0L;
    long lastWrittenOffset = 0L;

    public GroupMetadataManagerTestContext(
        MockTime time,
        MockCoordinatorTimer<Void, Record> timer,
        SnapshotRegistry snapshotRegistry,
        GroupCoordinatorMetricsShard metrics,
        GroupMetadataManager groupMetadataManager,
        int classicGroupInitialRebalanceDelayMs,
        int classicGroupNewMemberJoinTimeoutMs
    ) {
        this.time = time;
        this.timer = timer;
        this.snapshotRegistry = snapshotRegistry;
        this.metrics = metrics;
        this.groupMetadataManager = groupMetadataManager;
        this.classicGroupInitialRebalanceDelayMs = classicGroupInitialRebalanceDelayMs;
        this.classicGroupNewMemberJoinTimeoutMs = classicGroupNewMemberJoinTimeoutMs;
        snapshotRegistry.getOrCreateSnapshot(lastWrittenOffset);
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

    public MemberState consumerGroupMemberState(
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

    public void assertSessionTimeout(
        String groupId,
        String memberId,
        long delayMs
    ) {
        MockCoordinatorTimer.ScheduledTimeout<Void, Record> timeout =
            timer.timeout(consumerGroupSessionTimeoutKey(groupId, memberId));
        assertNotNull(timeout);
        assertEquals(time.milliseconds() + delayMs, timeout.deadlineMs);
    }

    public void assertNoSessionTimeout(
        String groupId,
        String memberId
    ) {
        MockCoordinatorTimer.ScheduledTimeout<Void, Record> timeout =
            timer.timeout(consumerGroupSessionTimeoutKey(groupId, memberId));
        assertNull(timeout);
    }

    public MockCoordinatorTimer.ScheduledTimeout<Void, Record> assertRebalanceTimeout(
        String groupId,
        String memberId,
        long delayMs
    ) {
        MockCoordinatorTimer.ScheduledTimeout<Void, Record> timeout =
            timer.timeout(consumerGroupRebalanceTimeoutKey(groupId, memberId));
        assertNotNull(timeout);
        assertEquals(time.milliseconds() + delayMs, timeout.deadlineMs);
        return timeout;
    }

    public void assertNoRebalanceTimeout(
        String groupId,
        String memberId
    ) {
        MockCoordinatorTimer.ScheduledTimeout<Void, Record> timeout =
            timer.timeout(consumerGroupRebalanceTimeoutKey(groupId, memberId));
        assertNull(timeout);
    }

    ClassicGroup createClassicGroup(String groupId) {
        return groupMetadataManager.getOrMaybeCreateClassicGroup(groupId, true);
    }

    public JoinResult sendClassicGroupJoin(
        JoinGroupRequestData request
    ) {
        return sendClassicGroupJoin(request, false);
    }

    public JoinResult sendClassicGroupJoin(
        JoinGroupRequestData request,
        boolean requireKnownMemberId
    ) {
        return sendClassicGroupJoin(request, requireKnownMemberId, false);
    }

    public JoinResult sendClassicGroupJoin(
        JoinGroupRequestData request,
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

        CompletableFuture<JoinGroupResponseData> responseFuture = new CompletableFuture<>();
        CoordinatorResult<Void, Record> coordinatorResult = groupMetadataManager.classicGroupJoin(
            context,
            request,
            responseFuture
        );

        return new JoinResult(responseFuture, coordinatorResult);
    }

    public JoinGroupResponseData joinClassicGroupAsDynamicMemberAndCompleteRebalance(
        String groupId
    ) throws Exception {
        ClassicGroup group = createClassicGroup(groupId);

        JoinGroupResponseData leaderJoinResponse =
            joinClassicGroupAsDynamicMemberAndCompleteJoin(new JoinGroupRequestBuilder()
                .withGroupId(groupId)
                .withMemberId(UNKNOWN_MEMBER_ID)
                .withDefaultProtocolTypeAndProtocols()
                .withRebalanceTimeoutMs(10000)
                .withSessionTimeoutMs(5000)
                .build());

        assertEquals(1, leaderJoinResponse.generationId());
        assertTrue(group.isInState(COMPLETING_REBALANCE));

        SyncResult syncResult = sendClassicGroupSync(new SyncGroupRequestBuilder()
            .withGroupId(groupId)
            .withMemberId(leaderJoinResponse.memberId())
            .withGenerationId(leaderJoinResponse.generationId())
            .build());

        assertEquals(
            Collections.singletonList(RecordHelpers.newGroupMetadataRecord(group, group.groupAssignment(), MetadataVersion.latestTesting())),
            syncResult.records
        );
        // Simulate a successful write to the log.
        syncResult.appendFuture.complete(null);

        assertTrue(syncResult.syncFuture.isDone());
        assertEquals(Errors.NONE.code(), syncResult.syncFuture.get().errorCode());
        assertTrue(group.isInState(STABLE));

        return leaderJoinResponse;
    }

    public JoinGroupResponseData joinClassicGroupAsDynamicMemberAndCompleteJoin(
        JoinGroupRequestData request
    ) throws ExecutionException, InterruptedException {
        boolean requireKnownMemberId = true;
        String newMemberId = request.memberId();

        if (request.memberId().equals(UNKNOWN_MEMBER_ID)) {
            // Since member id is required, we need another round to get the successful join group result.
            JoinResult firstJoinResult = sendClassicGroupJoin(
                request,
                requireKnownMemberId
            );
            assertTrue(firstJoinResult.records.isEmpty());
            assertTrue(firstJoinResult.joinFuture.isDone());
            assertEquals(Errors.MEMBER_ID_REQUIRED.code(), firstJoinResult.joinFuture.get().errorCode());
            newMemberId = firstJoinResult.joinFuture.get().memberId();
        }

        // Second round
        JoinGroupRequestData secondRequest = new JoinGroupRequestData()
            .setGroupId(request.groupId())
            .setMemberId(newMemberId)
            .setProtocolType(request.protocolType())
            .setProtocols(request.protocols())
            .setSessionTimeoutMs(request.sessionTimeoutMs())
            .setRebalanceTimeoutMs(request.rebalanceTimeoutMs())
            .setReason(request.reason());

        JoinResult secondJoinResult = sendClassicGroupJoin(
            secondRequest,
            requireKnownMemberId
        );

        assertTrue(secondJoinResult.records.isEmpty());
        List<MockCoordinatorTimer.ExpiredTimeout<Void, Record>> timeouts = sleep(classicGroupInitialRebalanceDelayMs);
        assertEquals(1, timeouts.size());
        assertTrue(secondJoinResult.joinFuture.isDone());
        assertEquals(Errors.NONE.code(), secondJoinResult.joinFuture.get().errorCode());

        return secondJoinResult.joinFuture.get();
    }

    public JoinGroupResponseData joinClassicGroupAndCompleteJoin(
        JoinGroupRequestData request,
        boolean requireKnownMemberId,
        boolean supportSkippingAssignment
    ) throws ExecutionException, InterruptedException {
        return joinClassicGroupAndCompleteJoin(
            request,
            requireKnownMemberId,
            supportSkippingAssignment,
            classicGroupInitialRebalanceDelayMs
        );
    }

    public JoinGroupResponseData joinClassicGroupAndCompleteJoin(
        JoinGroupRequestData request,
        boolean requireKnownMemberId,
        boolean supportSkippingAssignment,
        int advanceClockMs
    ) throws ExecutionException, InterruptedException {
        if (requireKnownMemberId && request.groupInstanceId().isEmpty()) {
            return joinClassicGroupAsDynamicMemberAndCompleteJoin(request);
        }

        try {
            JoinResult joinResult = sendClassicGroupJoin(
                request,
                requireKnownMemberId,
                supportSkippingAssignment
            );

            sleep(advanceClockMs);
            assertTrue(joinResult.joinFuture.isDone());
            assertEquals(Errors.NONE.code(), joinResult.joinFuture.get().errorCode());
            return joinResult.joinFuture.get();
        } catch (Exception e) {
            fail("Failed to due: " + e.getMessage());
        }
        return null;
    }

    public SyncResult sendClassicGroupSync(SyncGroupRequestData request) {
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

        CoordinatorResult<Void, Record> coordinatorResult = groupMetadataManager.classicGroupSync(
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
        ClassicGroup group = createClassicGroup(groupId);

        JoinGroupRequestData joinRequest = new JoinGroupRequestBuilder()
            .withGroupId(groupId)
            .withGroupInstanceId(leaderInstanceId)
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withProtocolType("consumer")
            .withProtocolSuperset()
            .withRebalanceTimeoutMs(rebalanceTimeoutMs)
            .withSessionTimeoutMs(sessionTimeoutMs)
            .build();

        JoinResult leaderJoinResult = sendClassicGroupJoin(joinRequest);
        JoinResult followerJoinResult = sendClassicGroupJoin(joinRequest.setGroupInstanceId(followerInstanceId));

        assertTrue(leaderJoinResult.records.isEmpty());
        assertTrue(followerJoinResult.records.isEmpty());
        assertFalse(leaderJoinResult.joinFuture.isDone());
        assertFalse(followerJoinResult.joinFuture.isDone());

        // The goal for two timer advance is to let first group initial join complete and set newMemberAdded flag to false. Next advance is
        // to trigger the rebalance as needed for follower delayed join. One large time advance won't help because we could only populate one
        // delayed join from purgatory and the new delayed op is created at that time and never be triggered.
        assertNoOrEmptyResult(sleep(classicGroupInitialRebalanceDelayMs));
        assertNoOrEmptyResult(sleep(classicGroupInitialRebalanceDelayMs));

        assertTrue(leaderJoinResult.joinFuture.isDone());
        assertTrue(followerJoinResult.joinFuture.isDone());
        assertEquals(Errors.NONE.code(), leaderJoinResult.joinFuture.get().errorCode());
        assertEquals(Errors.NONE.code(), followerJoinResult.joinFuture.get().errorCode());
        assertEquals(1, leaderJoinResult.joinFuture.get().generationId());
        assertEquals(1, followerJoinResult.joinFuture.get().generationId());
        assertEquals(2, group.size());
        assertEquals(1, group.generationId());
        assertTrue(group.isInState(COMPLETING_REBALANCE));

        String leaderId = leaderJoinResult.joinFuture.get().memberId();
        String followerId = followerJoinResult.joinFuture.get().memberId();
        List<SyncGroupRequestData.SyncGroupRequestAssignment> assignment = new ArrayList<>();
        assignment.add(new SyncGroupRequestData.SyncGroupRequestAssignment().setMemberId(leaderId)
                                                                            .setAssignment(new byte[]{1}));
        assignment.add(new SyncGroupRequestData.SyncGroupRequestAssignment().setMemberId(followerId)
                                                                            .setAssignment(new byte[]{2}));

        SyncGroupRequestData syncRequest = new SyncGroupRequestBuilder()
            .withGroupId(groupId)
            .withGroupInstanceId(leaderInstanceId)
            .withMemberId(leaderId)
            .withGenerationId(1)
            .withAssignment(assignment)
            .build();

        SyncResult leaderSyncResult = sendClassicGroupSync(syncRequest);

        // The generated record should contain the new assignment.
        Map<String, byte[]> groupAssignment = assignment.stream().collect(Collectors.toMap(
            SyncGroupRequestData.SyncGroupRequestAssignment::memberId, SyncGroupRequestData.SyncGroupRequestAssignment::assignment
        ));
        assertEquals(
            Collections.singletonList(
                RecordHelpers.newGroupMetadataRecord(group, groupAssignment, MetadataVersion.latestTesting())),
            leaderSyncResult.records
        );

        // Simulate a successful write to the log.
        leaderSyncResult.appendFuture.complete(null);

        assertTrue(leaderSyncResult.syncFuture.isDone());
        assertEquals(Errors.NONE.code(), leaderSyncResult.syncFuture.get().errorCode());
        assertTrue(group.isInState(STABLE));

        SyncResult followerSyncResult = sendClassicGroupSync(
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

    public PendingMemberGroupResult setupGroupWithPendingMember(ClassicGroup group) throws Exception {
        // Add the first member
        JoinGroupRequestData joinRequest = new JoinGroupRequestBuilder()
            .withGroupId(group.groupId())
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .withRebalanceTimeoutMs(10000)
            .withSessionTimeoutMs(5000)
            .build();

        JoinGroupResponseData leaderJoinResponse =
            joinClassicGroupAsDynamicMemberAndCompleteJoin(joinRequest);

        List<SyncGroupRequestData.SyncGroupRequestAssignment> assignment = new ArrayList<>();
        assignment.add(new SyncGroupRequestData.SyncGroupRequestAssignment().setMemberId(leaderJoinResponse.memberId()));
        SyncGroupRequestData syncRequest = new SyncGroupRequestBuilder()
            .withGroupId(group.groupId())
            .withMemberId(leaderJoinResponse.memberId())
            .withGenerationId(leaderJoinResponse.generationId())
            .withAssignment(assignment)
            .build();

        SyncResult syncResult = sendClassicGroupSync(syncRequest);

        // Now the group is stable, with the one member that joined above
        assertEquals(
            Collections.singletonList(RecordHelpers.newGroupMetadataRecord(group, group.groupAssignment(), MetadataVersion.latestTesting())),
            syncResult.records
        );
        // Simulate a successful write to log.
        syncResult.appendFuture.complete(null);

        assertTrue(syncResult.syncFuture.isDone());
        assertEquals(Errors.NONE.code(), syncResult.syncFuture.get().errorCode());

        // Start the join for the second member
        JoinResult followerJoinResult = sendClassicGroupJoin(
            joinRequest.setMemberId(UNKNOWN_MEMBER_ID)
        );

        assertTrue(followerJoinResult.records.isEmpty());
        assertFalse(followerJoinResult.joinFuture.isDone());

        JoinResult leaderJoinResult = sendClassicGroupJoin(
            joinRequest.setMemberId(leaderJoinResponse.memberId())
        );

        assertTrue(leaderJoinResult.records.isEmpty());
        assertTrue(group.isInState(COMPLETING_REBALANCE));
        assertTrue(leaderJoinResult.joinFuture.isDone());
        assertTrue(followerJoinResult.joinFuture.isDone());
        assertEquals(Errors.NONE.code(), leaderJoinResult.joinFuture.get().errorCode());
        assertEquals(Errors.NONE.code(), followerJoinResult.joinFuture.get().errorCode());
        assertEquals(leaderJoinResult.joinFuture.get().generationId(), followerJoinResult.joinFuture.get().generationId());
        assertEquals(leaderJoinResponse.memberId(), leaderJoinResult.joinFuture.get().leader());
        assertEquals(leaderJoinResponse.memberId(), followerJoinResult.joinFuture.get().leader());

        int nextGenerationId = leaderJoinResult.joinFuture.get().generationId();
        String followerId = followerJoinResult.joinFuture.get().memberId();

        // Stabilize the group
        syncResult = sendClassicGroupSync(syncRequest.setGenerationId(nextGenerationId));

        assertEquals(
            Collections.singletonList(RecordHelpers.newGroupMetadataRecord(group, group.groupAssignment(), MetadataVersion.latestTesting())),
            syncResult.records
        );
        // Simulate a successful write to log.
        syncResult.appendFuture.complete(null);

        assertTrue(syncResult.syncFuture.isDone());
        assertEquals(Errors.NONE.code(), syncResult.syncFuture.get().errorCode());
        assertTrue(group.isInState(STABLE));

        // Re-join an existing member, to transition the group to PreparingRebalance state.
        leaderJoinResult = sendClassicGroupJoin(
            joinRequest.setMemberId(leaderJoinResponse.memberId()));

        assertTrue(leaderJoinResult.records.isEmpty());
        assertFalse(leaderJoinResult.joinFuture.isDone());
        assertTrue(group.isInState(PREPARING_REBALANCE));

        // Create a pending member in the group
        JoinResult pendingMemberJoinResult = sendClassicGroupJoin(
            joinRequest
                .setMemberId(UNKNOWN_MEMBER_ID)
                .setSessionTimeoutMs(2500),
            true
        );

        assertTrue(pendingMemberJoinResult.records.isEmpty());
        assertTrue(pendingMemberJoinResult.joinFuture.isDone());
        assertEquals(Errors.MEMBER_ID_REQUIRED.code(), pendingMemberJoinResult.joinFuture.get().errorCode());
        assertEquals(1, group.numPendingJoinMembers());

        // Re-join the second existing member
        followerJoinResult = sendClassicGroupJoin(
            joinRequest.setMemberId(followerId).setSessionTimeoutMs(5000)
        );

        assertTrue(followerJoinResult.records.isEmpty());
        assertFalse(followerJoinResult.joinFuture.isDone());
        assertTrue(group.isInState(PREPARING_REBALANCE));
        assertEquals(2, group.size());
        assertEquals(1, group.numPendingJoinMembers());

        return new PendingMemberGroupResult(
            leaderJoinResponse.memberId(),
            followerId,
            pendingMemberJoinResult.joinFuture.get()
        );
    }

    public void verifySessionExpiration(ClassicGroup group, int timeoutMs) {
        Set<String> expectedHeartbeatKeys = group.allMembers().stream()
                                                 .map(member -> classicGroupHeartbeatKey(group.groupId(), member.memberId())).collect(Collectors.toSet());

        // Member should be removed as session expires.
        List<MockCoordinatorTimer.ExpiredTimeout<Void, Record>> timeouts = sleep(timeoutMs);
        List<Record> expectedRecords = Collections.singletonList(newGroupMetadataRecord(
            group.groupId(),
            new GroupMetadataValue()
                .setMembers(Collections.emptyList())
                .setGeneration(group.generationId())
                .setLeader(null)
                .setProtocolType("consumer")
                .setProtocol(null)
                .setCurrentStateTimestamp(time.milliseconds()),
            MetadataVersion.latestTesting()
        ));


        Set<String> heartbeatKeys = timeouts.stream().map(timeout -> timeout.key).collect(Collectors.toSet());
        assertEquals(expectedHeartbeatKeys, heartbeatKeys);

        // Only the last member leaving the group should result in the empty group metadata record.
        int timeoutsSize = timeouts.size();
        assertEquals(expectedRecords, timeouts.get(timeoutsSize - 1).result.records());
        assertNoOrEmptyResult(timeouts.subList(0, timeoutsSize - 1));
        assertTrue(group.isInState(EMPTY));
        assertEquals(0, group.size());
    }

    public HeartbeatResponseData sendClassicGroupHeartbeat(
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

        return groupMetadataManager.classicGroupHeartbeat(
            context,
            request
        );
    }

    public List<ListGroupsResponseData.ListedGroup> sendListGroups(List<String> statesFilter, List<String> typesFilter) {
        Set<String> statesFilterSet = new HashSet<>(statesFilter);
        Set<String> typesFilterSet = new HashSet<>(typesFilter);
        return groupMetadataManager.listGroups(statesFilterSet, typesFilterSet, lastCommittedOffset);
    }

    public List<ConsumerGroupDescribeResponseData.DescribedGroup> sendConsumerGroupDescribe(List<String> groupIds) {
        return groupMetadataManager.consumerGroupDescribe(groupIds, lastCommittedOffset);
    }

    public List<DescribeGroupsResponseData.DescribedGroup> describeGroups(List<String> groupIds) {
        return groupMetadataManager.describeGroups(groupIds, lastCommittedOffset);
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
            assertThrows(UnknownMemberIdException.class, () -> sendClassicGroupHeartbeat(request));
        } else {
            HeartbeatResponseData response = sendClassicGroupHeartbeat(request);
            assertEquals(expectedError.code(), response.errorCode());
        }
    }

    public List<JoinGroupResponseData> joinWithNMembers(
        String groupId,
        int numMembers,
        int rebalanceTimeoutMs,
        int sessionTimeoutMs
    ) {
        ClassicGroup group = createClassicGroup(groupId);
        boolean requireKnownMemberId = true;

        // First join requests
        JoinGroupRequestData request = new JoinGroupRequestBuilder()
            .withGroupId(groupId)
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .withRebalanceTimeoutMs(rebalanceTimeoutMs)
            .withSessionTimeoutMs(sessionTimeoutMs)
            .build();

        List<String> memberIds = IntStream.range(0, numMembers).mapToObj(i -> {
            JoinResult joinResult = sendClassicGroupJoin(request, requireKnownMemberId);

            assertTrue(joinResult.records.isEmpty());
            assertTrue(joinResult.joinFuture.isDone());

            try {
                return joinResult.joinFuture.get().memberId();
            } catch (Exception e) {
                fail("Unexpected exception: " + e.getMessage());
            }
            return null;
        }).collect(Collectors.toList());

        // Second join requests
        List<CompletableFuture<JoinGroupResponseData>> secondJoinFutures = IntStream.range(0, numMembers).mapToObj(i -> {
            JoinResult joinResult = sendClassicGroupJoin(request.setMemberId(memberIds.get(i)), requireKnownMemberId);

            assertTrue(joinResult.records.isEmpty());
            assertFalse(joinResult.joinFuture.isDone());

            return joinResult.joinFuture;
        }).collect(Collectors.toList());

        // Advance clock by initial rebalance delay.
        assertNoOrEmptyResult(sleep(classicGroupInitialRebalanceDelayMs));
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

    public CoordinatorResult<LeaveGroupResponseData, Record> sendClassicGroupLeave(
        LeaveGroupRequestData request
    ) {
        RequestContext context = new RequestContext(
            new RequestHeader(
                ApiKeys.LEAVE_GROUP,
                ApiKeys.LEAVE_GROUP.latestVersion(),
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

        return groupMetadataManager.classicGroupLeave(context, request);
    }

    public void verifyDescribeGroupsReturnsDeadGroup(String groupId) {
        List<DescribeGroupsResponseData.DescribedGroup> describedGroups =
            describeGroups(Collections.singletonList(groupId));

        assertEquals(
            Collections.singletonList(new DescribeGroupsResponseData.DescribedGroup()
                .setGroupId(groupId)
                .setGroupState(DEAD.toString())
            ),
            describedGroups
        );
    }

    private ApiMessage messageOrNull(ApiMessageAndVersion apiMessageAndVersion) {
        if (apiMessageAndVersion == null) {
            return null;
        } else {
            return apiMessageAndVersion.message();
        }
    }

    public void replay(
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
        snapshotRegistry.getOrCreateSnapshot(lastWrittenOffset);
    }
}
