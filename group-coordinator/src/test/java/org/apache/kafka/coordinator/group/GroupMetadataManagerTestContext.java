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
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.ConsumerProtocolAssignment;
import org.apache.kafka.common.message.ConsumerProtocolSubscription;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.ShareGroupDescribeResponseData;
import org.apache.kafka.common.message.ShareGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ShareGroupHeartbeatResponseData;
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
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorResult;
import org.apache.kafka.coordinator.common.runtime.MockCoordinatorExecutor;
import org.apache.kafka.coordinator.common.runtime.MockCoordinatorTimer;
import org.apache.kafka.coordinator.group.api.assignor.ConsumerGroupPartitionAssignor;
import org.apache.kafka.coordinator.group.api.assignor.ShareGroupPartitionAssignor;
import org.apache.kafka.coordinator.group.classic.ClassicGroup;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupRegularExpressionKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupRegularExpressionValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMetadataValue;
import org.apache.kafka.coordinator.group.generated.GroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.GroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupCurrentMemberAssignmentKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupCurrentMemberAssignmentValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupMemberMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupPartitionMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupPartitionMetadataValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMemberKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMemberValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMetadataValue;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetricsShard;
import org.apache.kafka.coordinator.group.modern.MemberState;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroup;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroupBuilder;
import org.apache.kafka.coordinator.group.modern.share.ShareGroup;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupBuilder;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.timeline.SnapshotRegistry;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.common.requests.JoinGroupRequest.UNKNOWN_MEMBER_ID;
import static org.apache.kafka.coordinator.group.Assertions.assertResponseEquals;
import static org.apache.kafka.coordinator.group.GroupConfigManagerTest.createConfigManager;
import static org.apache.kafka.coordinator.group.GroupMetadataManager.EMPTY_RESULT;
import static org.apache.kafka.coordinator.group.GroupMetadataManager.classicGroupHeartbeatKey;
import static org.apache.kafka.coordinator.group.GroupMetadataManager.consumerGroupJoinKey;
import static org.apache.kafka.coordinator.group.GroupMetadataManager.consumerGroupRebalanceTimeoutKey;
import static org.apache.kafka.coordinator.group.GroupMetadataManager.consumerGroupSyncKey;
import static org.apache.kafka.coordinator.group.GroupMetadataManager.groupSessionTimeoutKey;
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
    static final String DEFAULT_CLIENT_ID = "client";
    static final InetAddress DEFAULT_CLIENT_ADDRESS = InetAddress.getLoopbackAddress();

    private static class GroupCoordinatorConfigContext extends GroupCoordinatorConfig {
        GroupCoordinatorConfigContext(AbstractConfig config) {
            super(config);
        }

        public static GroupCoordinatorConfig fromProps(
            Map<?, ?> props
        ) {
            return new GroupCoordinatorConfigContext(
                new AbstractConfig(
                    Utils.mergeConfigs(List.of(
                        GroupCoordinatorConfig.GROUP_COORDINATOR_CONFIG_DEF,
                        GroupCoordinatorConfig.NEW_GROUP_CONFIG_DEF,
                        GroupCoordinatorConfig.OFFSET_MANAGEMENT_CONFIG_DEF,
                        GroupCoordinatorConfig.CONSUMER_GROUP_CONFIG_DEF,
                        GroupCoordinatorConfig.SHARE_GROUP_CONFIG_DEF
                    )),
                    props
                )
            );
        }

        @Override
        @SuppressWarnings("unchecked")
        protected List<ConsumerGroupPartitionAssignor> consumerGroupAssignors(
            AbstractConfig config
        ) {
            // In unit tests, it is pretty convenient to have the ability to pass instantiated
            // assignors. Hence, we check if the provided assignors are already instantiated.
            // Otherwise, we use the regular method.
            List<?> classes = config.getList(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG);
            if (classes.stream().allMatch(o -> o instanceof ConsumerGroupPartitionAssignor)) {
                return Collections.unmodifiableList((List<ConsumerGroupPartitionAssignor>) classes);
            }

            return super.consumerGroupAssignors(config);
        }
    }

    public static void assertNoOrEmptyResult(List<MockCoordinatorTimer.ExpiredTimeout<Void, CoordinatorRecord>> timeouts) {
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
                    List.of(topicNames.get(i % topicNames.size())))).array())
            );
        }
        return protocols;
    }

    public static JoinGroupRequestData.JoinGroupRequestProtocolCollection toConsumerProtocol(
        List<String> topicNames,
        List<TopicPartition> ownedPartitions
    ) {
        return toConsumerProtocol(topicNames, ownedPartitions, ConsumerProtocolSubscription.HIGHEST_SUPPORTED_VERSION);
    }

    public static JoinGroupRequestData.JoinGroupRequestProtocolCollection toConsumerProtocol(
        List<String> topicNames,
        List<TopicPartition> ownedPartitions,
        short version
    ) {
        JoinGroupRequestData.JoinGroupRequestProtocolCollection protocols =
            new JoinGroupRequestData.JoinGroupRequestProtocolCollection(0);
        protocols.add(new JoinGroupRequestData.JoinGroupRequestProtocol()
            .setName("range")
            .setMetadata(ConsumerProtocol.serializeSubscription(
                new ConsumerPartitionAssignor.Subscription(
                    topicNames,
                    null,
                    ownedPartitions
                ),
                version
            ).array())
        );
        return protocols;
    }

    public static CoordinatorRecord newGroupMetadataRecord(
        String groupId,
        GroupMetadataValue value,
        MetadataVersion metadataVersion
    ) {
        return new CoordinatorRecord(
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
        List<CoordinatorRecord> records;
        CompletableFuture<Void> appendFuture;

        public JoinResult(
            CompletableFuture<JoinGroupResponseData> joinFuture,
            CoordinatorResult<Void, CoordinatorRecord> coordinatorResult
        ) {
            this.joinFuture = joinFuture;
            this.records = coordinatorResult.records();
            this.appendFuture = coordinatorResult.appendFuture();
        }
    }

    public static class SyncResult {
        CompletableFuture<SyncGroupResponseData> syncFuture;
        List<CoordinatorRecord> records;
        CompletableFuture<Void> appendFuture;

        public SyncResult(
            CompletableFuture<SyncGroupResponseData> syncFuture,
            CoordinatorResult<Void, CoordinatorRecord> coordinatorResult
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
        private final MockTime time = new MockTime(0, 0, 0);
        private final MockCoordinatorTimer<Void, CoordinatorRecord> timer = new MockCoordinatorTimer<>(time);
        private final MockCoordinatorExecutor<CoordinatorRecord> executor = new MockCoordinatorExecutor<>();
        private final LogContext logContext = new LogContext();
        private final SnapshotRegistry snapshotRegistry = new SnapshotRegistry(logContext);
        private MetadataImage metadataImage;
        private GroupConfigManager groupConfigManager;
        private final List<ConsumerGroupBuilder> consumerGroupBuilders = new ArrayList<>();
        private final GroupCoordinatorMetricsShard metrics = mock(GroupCoordinatorMetricsShard.class);
        private ShareGroupPartitionAssignor shareGroupAssignor = new MockPartitionAssignor("share");
        private final List<ShareGroupBuilder> shareGroupBuilders = new ArrayList<>();
        private final Map<String, Object> config = new HashMap<>();

        public Builder withConfig(String key, Object value) {
            config.put(key, value);
            return this;
        }

        public Builder withMetadataImage(MetadataImage metadataImage) {
            this.metadataImage = metadataImage;
            return this;
        }

        public Builder withConsumerGroup(ConsumerGroupBuilder builder) {
            this.consumerGroupBuilders.add(builder);
            return this;
        }

        public Builder withShareGroup(ShareGroupBuilder builder) {
            this.shareGroupBuilders.add(builder);
            return this;
        }

        public Builder withShareGroupAssignor(ShareGroupPartitionAssignor shareGroupAssignor) {
            this.shareGroupAssignor = shareGroupAssignor;
            return this;
        }

        public GroupMetadataManagerTestContext build() {
            if (metadataImage == null) metadataImage = MetadataImage.EMPTY;
            if (groupConfigManager == null) groupConfigManager = createConfigManager();

            config.putIfAbsent(
                GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG,
                List.of(new MockPartitionAssignor("range"))
            );

            GroupCoordinatorConfig groupCoordinatorConfig = GroupCoordinatorConfigContext.fromProps(config);

            GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext(
                time,
                timer,
                executor,
                snapshotRegistry,
                metrics,
                groupCoordinatorConfig,
                new GroupMetadataManager.Builder()
                    .withSnapshotRegistry(snapshotRegistry)
                    .withLogContext(logContext)
                    .withTime(time)
                    .withTimer(timer)
                    .withExecutor(executor)
                    .withConfig(groupCoordinatorConfig)
                    .withMetadataImage(metadataImage)
                    .withGroupCoordinatorMetricsShard(metrics)
                    .withShareGroupAssignor(shareGroupAssignor)
                    .withGroupConfigManager(groupConfigManager)
                    .build(),
                groupConfigManager
            );

            consumerGroupBuilders.forEach(builder -> builder.build(metadataImage.topics()).forEach(context::replay));
            shareGroupBuilders.forEach(builder -> builder.build(metadataImage.topics()).forEach(context::replay));

            context.commit();

            return context;
        }
    }

    final MockTime time;
    final MockCoordinatorTimer<Void, CoordinatorRecord> timer;
    final MockCoordinatorExecutor<CoordinatorRecord> executor;
    final SnapshotRegistry snapshotRegistry;
    final GroupCoordinatorMetricsShard metrics;
    final GroupMetadataManager groupMetadataManager;
    final GroupConfigManager groupConfigManager;
    final int classicGroupInitialRebalanceDelayMs;
    final int classicGroupNewMemberJoinTimeoutMs;

    long lastCommittedOffset = 0L;
    long lastWrittenOffset = 0L;

    public GroupMetadataManagerTestContext(
        MockTime time,
        MockCoordinatorTimer<Void, CoordinatorRecord> timer,
        MockCoordinatorExecutor<CoordinatorRecord> executor,
        SnapshotRegistry snapshotRegistry,
        GroupCoordinatorMetricsShard metrics,
        GroupCoordinatorConfig config,
        GroupMetadataManager groupMetadataManager,
        GroupConfigManager groupConfigManager
    ) {
        this.time = time;
        this.timer = timer;
        this.executor = executor;
        this.snapshotRegistry = snapshotRegistry;
        this.metrics = metrics;
        this.groupMetadataManager = groupMetadataManager;
        this.groupConfigManager = groupConfigManager;
        this.classicGroupInitialRebalanceDelayMs = config.classicGroupInitialRebalanceDelayMs();
        this.classicGroupNewMemberJoinTimeoutMs = config.classicGroupNewMemberJoinTimeoutMs();
        snapshotRegistry.idempotentCreateSnapshot(lastWrittenOffset);
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
            .consumerGroup(groupId)
            .state();
    }

    public ShareGroup.ShareGroupState shareGroupState(
        String groupId
    ) {
        return groupMetadataManager
            .shareGroup(groupId)
            .state();
    }

    public MemberState consumerGroupMemberState(
        String groupId,
        String memberId
    ) {
        return groupMetadataManager
            .consumerGroup(groupId)
            .getOrMaybeCreateMember(memberId, false)
            .state();
    }

    public CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> consumerGroupHeartbeat(
        ConsumerGroupHeartbeatRequestData request
    ) {
        return this.consumerGroupHeartbeat(request, ApiKeys.CONSUMER_GROUP_HEARTBEAT.latestVersion());
    }

    public CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> consumerGroupHeartbeat(
        ConsumerGroupHeartbeatRequestData request,
        short apiVersion
    ) {
        RequestContext context = new RequestContext(
            new RequestHeader(
                ApiKeys.CONSUMER_GROUP_HEARTBEAT,
                apiVersion,
                DEFAULT_CLIENT_ID,
                0
            ),
            "1",
            DEFAULT_CLIENT_ADDRESS,
            KafkaPrincipal.ANONYMOUS,
            ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
            SecurityProtocol.PLAINTEXT,
            ClientInformation.EMPTY,
            false
        );

        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = groupMetadataManager.consumerGroupHeartbeat(
            context,
            request
        );

        if (result.replayRecords()) {
            result.records().forEach(this::replay);
        }
        return result;
    }

    public CoordinatorResult<ShareGroupHeartbeatResponseData, CoordinatorRecord> shareGroupHeartbeat(
        ShareGroupHeartbeatRequestData request
    ) {
        RequestContext context = new RequestContext(
            new RequestHeader(
                ApiKeys.SHARE_GROUP_HEARTBEAT,
                ApiKeys.SHARE_GROUP_HEARTBEAT.latestVersion(),
                DEFAULT_CLIENT_ID,
                0
            ),
            "1",
            DEFAULT_CLIENT_ADDRESS,
            KafkaPrincipal.ANONYMOUS,
            ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
            SecurityProtocol.PLAINTEXT,
            ClientInformation.EMPTY,
            false
        );

        CoordinatorResult<ShareGroupHeartbeatResponseData, CoordinatorRecord> result = groupMetadataManager.shareGroupHeartbeat(
            context,
            request
        );

        result.records().forEach(this::replay);
        return result;
    }

    public List<MockCoordinatorTimer.ExpiredTimeout<Void, CoordinatorRecord>> sleep(long ms) {
        time.sleep(ms);
        List<MockCoordinatorTimer.ExpiredTimeout<Void, CoordinatorRecord>> timeouts = timer.poll();
        timeouts.forEach(timeout -> {
            if (timeout.result.replayRecords()) {
                timeout.result.records().forEach(this::replay);
            }
        });
        return timeouts;
    }

    public List<MockCoordinatorExecutor.ExecutorResult<CoordinatorRecord>> processTasks() {
        List<MockCoordinatorExecutor.ExecutorResult<CoordinatorRecord>> results = executor.poll();
        results.forEach(taskResult -> {
            if (taskResult.result.replayRecords()) {
                taskResult.result.records().forEach(this::replay);
            }
        });
        return results;
    }

    public void assertSessionTimeout(
        String groupId,
        String memberId,
        long delayMs
    ) {
        MockCoordinatorTimer.ScheduledTimeout<Void, CoordinatorRecord> timeout =
            timer.timeout(groupSessionTimeoutKey(groupId, memberId));
        assertNotNull(timeout);
        assertEquals(time.milliseconds() + delayMs, timeout.deadlineMs);
    }

    public void assertNoSessionTimeout(
        String groupId,
        String memberId
    ) {
        MockCoordinatorTimer.ScheduledTimeout<Void, CoordinatorRecord> timeout =
            timer.timeout(groupSessionTimeoutKey(groupId, memberId));
        assertNull(timeout);
    }

    public MockCoordinatorTimer.ScheduledTimeout<Void, CoordinatorRecord> assertRebalanceTimeout(
        String groupId,
        String memberId,
        long delayMs
    ) {
        MockCoordinatorTimer.ScheduledTimeout<Void, CoordinatorRecord> timeout =
            timer.timeout(consumerGroupRebalanceTimeoutKey(groupId, memberId));
        assertNotNull(timeout);
        assertEquals(time.milliseconds() + delayMs, timeout.deadlineMs);
        return timeout;
    }

    public void assertNoRebalanceTimeout(
        String groupId,
        String memberId
    ) {
        MockCoordinatorTimer.ScheduledTimeout<Void, CoordinatorRecord> timeout =
            timer.timeout(consumerGroupRebalanceTimeoutKey(groupId, memberId));
        assertNull(timeout);
    }

    public MockCoordinatorTimer.ScheduledTimeout<Void, CoordinatorRecord> assertJoinTimeout(
        String groupId,
        String memberId,
        long delayMs
    ) {
        MockCoordinatorTimer.ScheduledTimeout<Void, CoordinatorRecord> timeout =
            timer.timeout(consumerGroupJoinKey(groupId, memberId));
        assertNotNull(timeout);
        assertEquals(time.milliseconds() + delayMs, timeout.deadlineMs);
        return timeout;
    }

    public void assertNoJoinTimeout(
        String groupId,
        String memberId
    ) {
        MockCoordinatorTimer.ScheduledTimeout<Void, CoordinatorRecord> timeout =
            timer.timeout(consumerGroupJoinKey(groupId, memberId));
        assertNull(timeout);
    }

    public MockCoordinatorTimer.ScheduledTimeout<Void, CoordinatorRecord> assertSyncTimeout(
        String groupId,
        String memberId,
        long delayMs
    ) {
        MockCoordinatorTimer.ScheduledTimeout<Void, CoordinatorRecord> timeout =
            timer.timeout(consumerGroupSyncKey(groupId, memberId));
        assertNotNull(timeout);
        assertEquals(time.milliseconds() + delayMs, timeout.deadlineMs);
        return timeout;
    }

    public void assertNoSyncTimeout(
        String groupId,
        String memberId
    ) {
        MockCoordinatorTimer.ScheduledTimeout<Void, CoordinatorRecord> timeout =
            timer.timeout(consumerGroupSyncKey(groupId, memberId));
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
                DEFAULT_CLIENT_ID,
                0
            ),
            "1",
            DEFAULT_CLIENT_ADDRESS,
            KafkaPrincipal.ANONYMOUS,
            ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
            SecurityProtocol.PLAINTEXT,
            ClientInformation.EMPTY,
            false
        );

        CompletableFuture<JoinGroupResponseData> responseFuture = new CompletableFuture<>();
        CoordinatorResult<Void, CoordinatorRecord> coordinatorResult = groupMetadataManager.classicGroupJoin(
            context,
            request,
            responseFuture
        );

        if (coordinatorResult.replayRecords()) {
            coordinatorResult.records().forEach(this::replay);
        }

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
            List.of(GroupCoordinatorRecordHelpers.newGroupMetadataRecord(group, group.groupAssignment(), MetadataVersion.latestTesting())),
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
        List<MockCoordinatorTimer.ExpiredTimeout<Void, CoordinatorRecord>> timeouts = sleep(classicGroupInitialRebalanceDelayMs);
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
                DEFAULT_CLIENT_ID,
                0
            ),
            "1",
            DEFAULT_CLIENT_ADDRESS,
            KafkaPrincipal.ANONYMOUS,
            ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
            SecurityProtocol.PLAINTEXT,
            ClientInformation.EMPTY,
            false
        );

        CompletableFuture<SyncGroupResponseData> responseFuture = new CompletableFuture<>();

        CoordinatorResult<Void, CoordinatorRecord> coordinatorResult = groupMetadataManager.classicGroupSync(
            context,
            request,
            responseFuture
        );

        if (coordinatorResult.replayRecords()) {
            coordinatorResult.records().forEach(this::replay);
        }

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
        assertEquals(2, group.numMembers());
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
            List.of(
                GroupCoordinatorRecordHelpers.newGroupMetadataRecord(group, groupAssignment, MetadataVersion.latestTesting())),
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

        assertEquals(2, group.numMembers());
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
            List.of(GroupCoordinatorRecordHelpers.newGroupMetadataRecord(group, group.groupAssignment(), MetadataVersion.latestTesting())),
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
            List.of(GroupCoordinatorRecordHelpers.newGroupMetadataRecord(group, group.groupAssignment(), MetadataVersion.latestTesting())),
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
        assertEquals(2, group.numMembers());
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
        List<MockCoordinatorTimer.ExpiredTimeout<Void, CoordinatorRecord>> timeouts = sleep(timeoutMs);
        List<CoordinatorRecord> expectedRecords = List.of(newGroupMetadataRecord(
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
        assertEquals(0, group.numMembers());
    }

    public CoordinatorResult<HeartbeatResponseData, CoordinatorRecord> sendClassicGroupHeartbeat(
        HeartbeatRequestData request
    ) {
        RequestContext context = new RequestContext(
            new RequestHeader(
                ApiKeys.HEARTBEAT,
                ApiKeys.HEARTBEAT.latestVersion(),
                DEFAULT_CLIENT_ID,
                0
            ),
            "1",
            DEFAULT_CLIENT_ADDRESS,
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

    public List<ShareGroupDescribeResponseData.DescribedGroup> sendShareGroupDescribe(List<String> groupIds) {
        return groupMetadataManager.shareGroupDescribe(groupIds, lastCommittedOffset);
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
            HeartbeatResponseData response = sendClassicGroupHeartbeat(request).response();
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

        assertEquals(numMembers, group.numMembers());
        assertTrue(group.isInState(COMPLETING_REBALANCE));

        return joinResponses;
    }

    public CoordinatorResult<LeaveGroupResponseData, CoordinatorRecord> sendClassicGroupLeave(
        LeaveGroupRequestData request
    ) {
        RequestContext context = new RequestContext(
            new RequestHeader(
                ApiKeys.LEAVE_GROUP,
                ApiKeys.LEAVE_GROUP.latestVersion(),
                DEFAULT_CLIENT_ID,
                0
            ),
            "1",
            DEFAULT_CLIENT_ADDRESS,
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
            describeGroups(List.of(groupId));

        assertEquals(
            List.of(new DescribeGroupsResponseData.DescribedGroup()
                .setGroupId(groupId)
                .setGroupState(DEAD.toString())
            ),
            describedGroups
        );
    }

    public void verifyClassicGroupSyncToConsumerGroup(
        String groupId,
        String memberId,
        int generationId,
        String protocolName,
        String protocolType,
        List<TopicPartition> topicPartitionList,
        short version
    ) throws Exception {
        GroupMetadataManagerTestContext.SyncResult syncResult = sendClassicGroupSync(
            new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
                .withGroupId(groupId)
                .withMemberId(memberId)
                .withGenerationId(generationId)
                .withProtocolName(protocolName)
                .withProtocolType(protocolType)
                .build()
        );
        assertEquals(Collections.emptyList(), syncResult.records);
        assertFalse(syncResult.syncFuture.isDone());

        // Simulate a successful write to log.
        syncResult.appendFuture.complete(null);
        assertResponseEquals(
            new SyncGroupResponseData()
                .setProtocolType(protocolType)
                .setProtocolName(protocolName)
                .setAssignment(ConsumerProtocol.serializeAssignment(
                    new ConsumerPartitionAssignor.Assignment(topicPartitionList),
                    version
                ).array()),
            syncResult.syncFuture.get()
        );
        assertSessionTimeout(groupId, memberId, 5000);
        assertNoSyncTimeout(groupId, memberId);
    }

    public void verifyClassicGroupSyncToConsumerGroup(
        String groupId,
        String memberId,
        int generationId,
        String protocolName,
        String protocolType,
        List<TopicPartition> topicPartitionList
    ) throws Exception {
        verifyClassicGroupSyncToConsumerGroup(
            groupId,
            memberId,
            generationId,
            protocolName,
            protocolType,
            topicPartitionList,
            ConsumerProtocolAssignment.HIGHEST_SUPPORTED_VERSION
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
        CoordinatorRecord record
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

            case ShareGroupMemberMetadataKey.HIGHEST_SUPPORTED_VERSION:
                groupMetadataManager.replay(
                    (ShareGroupMemberMetadataKey) key.message(),
                    (ShareGroupMemberMetadataValue) messageOrNull(value)
                );
                break;

            case ShareGroupMetadataKey.HIGHEST_SUPPORTED_VERSION:
                groupMetadataManager.replay(
                    (ShareGroupMetadataKey) key.message(),
                    (ShareGroupMetadataValue) messageOrNull(value)
                );
                break;

            case ShareGroupPartitionMetadataKey.HIGHEST_SUPPORTED_VERSION:
                groupMetadataManager.replay(
                    (ShareGroupPartitionMetadataKey) key.message(),
                    (ShareGroupPartitionMetadataValue) messageOrNull(value)
                );
                break;

            case ShareGroupTargetAssignmentMemberKey.HIGHEST_SUPPORTED_VERSION:
                groupMetadataManager.replay(
                    (ShareGroupTargetAssignmentMemberKey) key.message(),
                    (ShareGroupTargetAssignmentMemberValue) messageOrNull(value)
                );
                break;

            case ShareGroupTargetAssignmentMetadataKey.HIGHEST_SUPPORTED_VERSION:
                groupMetadataManager.replay(
                    (ShareGroupTargetAssignmentMetadataKey) key.message(),
                    (ShareGroupTargetAssignmentMetadataValue) messageOrNull(value)
                );
                break;

            case ShareGroupCurrentMemberAssignmentKey.HIGHEST_SUPPORTED_VERSION:
                groupMetadataManager.replay(
                    (ShareGroupCurrentMemberAssignmentKey) key.message(),
                    (ShareGroupCurrentMemberAssignmentValue) messageOrNull(value)
                );
                break;

            case ConsumerGroupRegularExpressionKey.HIGHEST_SUPPORTED_VERSION:
                groupMetadataManager.replay(
                    (ConsumerGroupRegularExpressionKey) key.message(),
                    (ConsumerGroupRegularExpressionValue) messageOrNull(value)
                );
                break;

            default:
                throw new IllegalStateException("Received an unknown record type " + key.version()
                    + " in " + record);
        }

        lastWrittenOffset++;
        snapshotRegistry.idempotentCreateSnapshot(lastWrittenOffset);
    }

    void onUnloaded() {
        groupMetadataManager.onUnloaded();
    }

    public void updateGroupConfig(String groupId, Properties newGroupConfig) {
        groupConfigManager.updateGroupConfig(groupId, newGroupConfig);
    }
}
