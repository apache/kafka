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

import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.CoordinatorNotAvailableException;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.FencedMemberEpochException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupMaxSizeReachedException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.IllegalGenerationException;
import org.apache.kafka.common.errors.InconsistentGroupProtocolException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnreleasedInstanceIdException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.internals.Plugin;
import org.apache.kafka.common.message.AlterShareGroupOffsetsRequestData;
import org.apache.kafka.common.message.AlterShareGroupOffsetsResponseData;
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.ConsumerProtocolSubscription;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.DeleteShareGroupOffsetsRequestData;
import org.apache.kafka.common.message.DeleteShareGroupOffsetsResponseData;
import org.apache.kafka.common.message.DeleteShareGroupStateRequestData;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocol;
import org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocolCollection;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupResponseData.MemberResponse;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.ShareGroupDescribeResponseData;
import org.apache.kafka.common.message.ShareGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ShareGroupHeartbeatResponseData;
import org.apache.kafka.common.message.StreamsGroupDescribeResponseData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData.Endpoint;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData.KeyValue;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData.TaskIds;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData.Topology;
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData.Status;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.ShareGroupHeartbeatRequest;
import org.apache.kafka.common.requests.ShareGroupHeartbeatResponse;
import org.apache.kafka.common.requests.StreamsGroupHeartbeatResponse;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.common.runtime.CoordinatorExecutor;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataDelta;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorResult;
import org.apache.kafka.coordinator.common.runtime.CoordinatorTimer;
import org.apache.kafka.coordinator.group.api.assignor.ConsumerGroupPartitionAssignor;
import org.apache.kafka.coordinator.group.api.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignorException;
import org.apache.kafka.coordinator.group.api.assignor.ShareGroupPartitionAssignor;
import org.apache.kafka.coordinator.group.api.assignor.SubscriptionType;
import org.apache.kafka.coordinator.group.assignor.SimpleAssignor;
import org.apache.kafka.coordinator.group.classic.ClassicGroup;
import org.apache.kafka.coordinator.group.classic.ClassicGroupMember;
import org.apache.kafka.coordinator.group.classic.ClassicGroupState;
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
import org.apache.kafka.coordinator.group.generated.ShareGroupStatePartitionMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupStatePartitionMetadataValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMemberKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMemberValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMetadataValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupCurrentMemberAssignmentKey;
import org.apache.kafka.coordinator.group.generated.StreamsGroupCurrentMemberAssignmentValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupMemberMetadataKey;
import org.apache.kafka.coordinator.group.generated.StreamsGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.StreamsGroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTargetAssignmentMemberKey;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTargetAssignmentMemberValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTargetAssignmentMetadataKey;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTargetAssignmentMetadataValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyKey;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetricsShard;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.modern.MemberState;
import org.apache.kafka.coordinator.group.modern.ModernGroup;
import org.apache.kafka.coordinator.group.modern.SubscriptionCount;
import org.apache.kafka.coordinator.group.modern.TargetAssignmentBuilder;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroup;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroupMember;
import org.apache.kafka.coordinator.group.modern.consumer.CurrentAssignmentBuilder;
import org.apache.kafka.coordinator.group.modern.consumer.ResolvedRegularExpression;
import org.apache.kafka.coordinator.group.modern.share.ShareGroup;
import org.apache.kafka.coordinator.group.modern.share.ShareGroup.InitMapValue;
import org.apache.kafka.coordinator.group.modern.share.ShareGroup.ShareGroupStatePartitionMetadataInfo;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupAssignmentBuilder;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupMember;
import org.apache.kafka.coordinator.group.streams.StreamsGroup;
import org.apache.kafka.coordinator.group.streams.StreamsGroupHeartbeatResult;
import org.apache.kafka.coordinator.group.streams.StreamsGroupMember;
import org.apache.kafka.coordinator.group.streams.StreamsTopology;
import org.apache.kafka.coordinator.group.streams.TasksTuple;
import org.apache.kafka.coordinator.group.streams.TasksTupleWithEpochs;
import org.apache.kafka.coordinator.group.streams.assignor.StickyTaskAssignor;
import org.apache.kafka.coordinator.group.streams.assignor.TaskAssignor;
import org.apache.kafka.coordinator.group.streams.assignor.TaskAssignorException;
import org.apache.kafka.coordinator.group.streams.topics.ConfiguredSubtopology;
import org.apache.kafka.coordinator.group.streams.topics.ConfiguredTopology;
import org.apache.kafka.coordinator.group.streams.topics.EndpointToPartitionsManager;
import org.apache.kafka.coordinator.group.streams.topics.InternalTopicManager;
import org.apache.kafka.coordinator.group.streams.topics.TopicConfigurationException;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.share.persister.DeleteShareGroupStateParameters;
import org.apache.kafka.server.share.persister.GroupTopicPartitionData;
import org.apache.kafka.server.share.persister.InitializeShareGroupStateParameters;
import org.apache.kafka.server.share.persister.PartitionFactory;
import org.apache.kafka.server.share.persister.PartitionIdData;
import org.apache.kafka.server.share.persister.TopicData;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineHashSet;

import com.google.re2j.Pattern;
import com.google.re2j.PatternSyntaxException;

import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.kafka.common.acl.AclOperation.DESCRIBE;
import static org.apache.kafka.common.protocol.Errors.COORDINATOR_NOT_AVAILABLE;
import static org.apache.kafka.common.protocol.Errors.ILLEGAL_GENERATION;
import static org.apache.kafka.common.protocol.Errors.NOT_COORDINATOR;
import static org.apache.kafka.common.protocol.Errors.UNKNOWN_SERVER_ERROR;
import static org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH;
import static org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest.LEAVE_GROUP_STATIC_MEMBER_EPOCH;
import static org.apache.kafka.common.requests.JoinGroupRequest.UNKNOWN_MEMBER_ID;
import static org.apache.kafka.common.resource.PatternType.LITERAL;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;
import static org.apache.kafka.coordinator.group.Group.GroupType.CLASSIC;
import static org.apache.kafka.coordinator.group.Group.GroupType.CONSUMER;
import static org.apache.kafka.coordinator.group.Group.GroupType.SHARE;
import static org.apache.kafka.coordinator.group.Group.GroupType.STREAMS;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionTombstone;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataTombstoneRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newShareGroupCurrentAssignmentRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newShareGroupCurrentAssignmentTombstoneRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newShareGroupEpochRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newShareGroupMemberSubscriptionRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newShareGroupMemberSubscriptionTombstoneRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newShareGroupStatePartitionMetadataRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newShareGroupTargetAssignmentTombstoneRecord;
import static org.apache.kafka.coordinator.group.Utils.assignmentToString;
import static org.apache.kafka.coordinator.group.Utils.ofSentinel;
import static org.apache.kafka.coordinator.group.Utils.throwIfRegularExpressionIsInvalid;
import static org.apache.kafka.coordinator.group.Utils.toConsumerProtocolAssignment;
import static org.apache.kafka.coordinator.group.Utils.toTopicPartitions;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupMember.EMPTY_ASSIGNMENT;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.COMPLETING_REBALANCE;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.DEAD;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.EMPTY;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.PREPARING_REBALANCE;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.STABLE;
import static org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics.CLASSIC_GROUP_COMPLETED_REBALANCES_SENSOR_NAME;
import static org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics.CONSUMER_GROUP_REBALANCES_SENSOR_NAME;
import static org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics.SHARE_GROUP_REBALANCES_SENSOR_NAME;
import static org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics.STREAMS_GROUP_REBALANCES_SENSOR_NAME;
import static org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroupMember.hasAssignedPartitionsChanged;
import static org.apache.kafka.coordinator.group.streams.StreamsCoordinatorRecordHelpers.convertToStreamsGroupTopologyRecord;
import static org.apache.kafka.coordinator.group.streams.StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord;
import static org.apache.kafka.coordinator.group.streams.StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentTombstoneRecord;
import static org.apache.kafka.coordinator.group.streams.StreamsCoordinatorRecordHelpers.newStreamsGroupMemberRecord;
import static org.apache.kafka.coordinator.group.streams.StreamsCoordinatorRecordHelpers.newStreamsGroupMemberTombstoneRecord;
import static org.apache.kafka.coordinator.group.streams.StreamsCoordinatorRecordHelpers.newStreamsGroupMetadataRecord;
import static org.apache.kafka.coordinator.group.streams.StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentTombstoneRecord;
import static org.apache.kafka.coordinator.group.streams.StreamsCoordinatorRecordHelpers.newStreamsGroupTopologyRecord;
import static org.apache.kafka.coordinator.group.streams.StreamsGroupMember.hasAssignedTasksChanged;


/**
 * The GroupMetadataManager manages the metadata of all classic and consumer groups. It holds
 * the hard and the soft state of the groups. This class has two kinds of methods:
 * 1) The request handlers which handle the requests and generate a response and records to
 *    mutate the hard state. Those records will be written by the runtime and applied to the
 *    hard state via the replay methods.
 * 2) The replay methods which apply records to the hard state. Those are used in the request
 *    handling as well as during the initial loading of the records from the partitions.
 */
public class GroupMetadataManager {
    private static final int METADATA_REFRESH_INTERVAL_MS = Integer.MAX_VALUE;

    private static class UpdateSubscriptionMetadataResult {
        private final int groupEpoch;
        private final SubscriptionType subscriptionType;

        UpdateSubscriptionMetadataResult(
            int groupEpoch,
            SubscriptionType subscriptionType
        ) {
            this.groupEpoch = groupEpoch;
            this.subscriptionType = Objects.requireNonNull(subscriptionType);
        }
    }

    public static class Builder {
        private LogContext logContext = null;
        private SnapshotRegistry snapshotRegistry = null;
        private Time time = null;
        private CoordinatorTimer<Void, CoordinatorRecord> timer = null;
        private CoordinatorExecutor<CoordinatorRecord> executor = null;
        private GroupCoordinatorConfig config = null;
        private GroupConfigManager groupConfigManager = null;
        private CoordinatorMetadataImage metadataImage = null;
        private ShareGroupPartitionAssignor shareGroupAssignor = null;
        private GroupCoordinatorMetricsShard metrics;
        private Optional<Plugin<Authorizer>> authorizerPlugin = null;
        private List<TaskAssignor> streamsGroupAssignors = null;

        Builder withLogContext(LogContext logContext) {
            this.logContext = logContext;
            return this;
        }

        Builder withSnapshotRegistry(SnapshotRegistry snapshotRegistry) {
            this.snapshotRegistry = snapshotRegistry;
            return this;
        }

        Builder withTime(Time time) {
            this.time = time;
            return this;
        }

        Builder withTimer(CoordinatorTimer<Void, CoordinatorRecord> timer) {
            this.timer = timer;
            return this;
        }

        Builder withExecutor(CoordinatorExecutor<CoordinatorRecord> executor) {
            this.executor = executor;
            return this;
        }

        Builder withConfig(GroupCoordinatorConfig config) {
            this.config = config;
            return this;
        }

        Builder withGroupConfigManager(GroupConfigManager groupConfigManager) {
            this.groupConfigManager = groupConfigManager;
            return this;
        }

        Builder withStreamsGroupAssignors(List<TaskAssignor> streamsGroupAssignors) {
            this.streamsGroupAssignors = streamsGroupAssignors;
            return this;
        }

        Builder withMetadataImage(CoordinatorMetadataImage metadataImage) {
            this.metadataImage = metadataImage;
            return this;
        }

        Builder withGroupCoordinatorMetricsShard(GroupCoordinatorMetricsShard metrics) {
            this.metrics = metrics;
            return this;
        }

        Builder withShareGroupAssignor(ShareGroupPartitionAssignor shareGroupAssignor) {
            this.shareGroupAssignor = shareGroupAssignor;
            return this;
        }

        Builder withAuthorizerPlugin(Optional<Plugin<Authorizer>> authorizerPlugin) {
            this.authorizerPlugin = authorizerPlugin;
            return this;
        }

        GroupMetadataManager build() {
            if (logContext == null) logContext = new LogContext();
            if (snapshotRegistry == null) snapshotRegistry = new SnapshotRegistry(logContext);
            if (metadataImage == null) metadataImage = CoordinatorMetadataImage.EMPTY;
            if (time == null) time = Time.SYSTEM;
            if (authorizerPlugin == null) authorizerPlugin = Optional.empty();

            if (timer == null)
                throw new IllegalArgumentException("Timer must be set.");
            if (executor == null)
                throw new IllegalArgumentException("Executor must be set.");
            if (config == null)
                throw new IllegalArgumentException("Config must be set.");
            if (shareGroupAssignor == null)
                shareGroupAssignor = new SimpleAssignor();
            if (metrics == null)
                throw new IllegalArgumentException("GroupCoordinatorMetricsShard must be set.");
            if (groupConfigManager == null)
                throw new IllegalArgumentException("GroupConfigManager must be set.");
            if (streamsGroupAssignors == null)
                streamsGroupAssignors = List.of(new StickyTaskAssignor());

            return new GroupMetadataManager(
                snapshotRegistry,
                logContext,
                time,
                timer,
                executor,
                metrics,
                metadataImage,
                config,
                groupConfigManager,
                shareGroupAssignor,
                authorizerPlugin,
                streamsGroupAssignors
            );
        }
    }

    /**
     * The minimum amount of time between two consecutive refreshes of
     * the regular expressions within a single group.
     *
     * Package private for setting the lower limit of the refresh interval.
     */
    static final long REGEX_BATCH_REFRESH_MIN_INTERVAL_MS = 10_000L;

    /**
     * The log context.
     */
    private final LogContext logContext;

    /**
     * The logger.
     */
    private final Logger log;

    /**
     * The snapshot registry.
     */
    private final SnapshotRegistry snapshotRegistry;

    /**
     * The system time.
     */
    private final Time time;

    /**
     * The system timer.
     */
    private final CoordinatorTimer<Void, CoordinatorRecord> timer;

    /**
     * The executor to executor asynchronous tasks.
     */
    private final CoordinatorExecutor<CoordinatorRecord> executor;

    /**
     * The coordinator metrics.
     */
    private final GroupCoordinatorMetricsShard metrics;

    /**
     * The group coordinator config.
     */
    private final GroupCoordinatorConfig config;

    /**
     * The supported consumer group partition assignors keyed by their name.
     */
    private final Map<String, ConsumerGroupPartitionAssignor> consumerGroupAssignors;

    /**
     * The default consumer group assignor used.
     */
    private final ConsumerGroupPartitionAssignor defaultConsumerGroupAssignor;

    /**
     * The classic and consumer groups keyed by their name.
     */
    private final TimelineHashMap<String, Group> groups;

    /**
     * The group ids keyed by topic names.
     */
    private final TimelineHashMap<String, TimelineHashSet<String>> groupsByTopics;

    /**
     * The share group partition metadata info keyed by group id.
     */
    private final TimelineHashMap<String, ShareGroupStatePartitionMetadataInfo> shareGroupStatePartitionMetadata;

    /**
     * The group manager.
     */
    private final GroupConfigManager groupConfigManager;

    /**
     * The supported task assignors keyed by their name.
     */
    private final Map<String, TaskAssignor> streamsGroupAssignors;

    /**
     * The metadata image.
     */
    private CoordinatorMetadataImage metadataImage;

    /**
     * The cache for topic hash value by topic name.
     * A topic hash is calculated when there is a group subscribes to it.
     * A topic hash is removed when it's updated in CoordinatorMetadataImage or there is no group subscribes to it.
     */
    private final Map<String, Long> topicHashCache;

    /**
     * This tracks the version of the last metadata image
     * with newly created topics.
     */
    private long lastMetadataImageWithNewTopics = -1L;

    /**
     * An empty result returned to the state machine. This means that
     * there are no records to append to the log.
     *
     * Package private for testing.
     */
    static final CoordinatorResult<Void, CoordinatorRecord> EMPTY_RESULT =
        new CoordinatorResult<>(List.of(), CompletableFuture.completedFuture(null), false);

    /**
     * The share group partition assignor.
     */
    private final ShareGroupPartitionAssignor shareGroupAssignor;

    /**
     * The authorizer to validate the regex subscription topics.
     */
    private final Optional<Plugin<Authorizer>> authorizerPlugin;

    private GroupMetadataManager(
        SnapshotRegistry snapshotRegistry,
        LogContext logContext,
        Time time,
        CoordinatorTimer<Void, CoordinatorRecord> timer,
        CoordinatorExecutor<CoordinatorRecord> executor,
        GroupCoordinatorMetricsShard metrics,
        CoordinatorMetadataImage metadataImage,
        GroupCoordinatorConfig config,
        GroupConfigManager groupConfigManager,
        ShareGroupPartitionAssignor shareGroupAssignor,
        Optional<Plugin<Authorizer>> authorizerPlugin,
        List<TaskAssignor> streamsGroupAssignors
    ) {
        this.logContext = logContext;
        this.log = logContext.logger(GroupMetadataManager.class);
        this.snapshotRegistry = snapshotRegistry;
        this.time = time;
        this.timer = timer;
        this.executor = executor;
        this.metrics = metrics;
        this.config = config;
        this.metadataImage = metadataImage;
        this.consumerGroupAssignors = config
            .consumerGroupAssignors()
            .stream()
            .collect(Collectors.toMap(ConsumerGroupPartitionAssignor::name, Function.identity()));
        this.defaultConsumerGroupAssignor = config.consumerGroupAssignors().get(0);
        this.groups = new TimelineHashMap<>(snapshotRegistry, 0);
        this.groupsByTopics = new TimelineHashMap<>(snapshotRegistry, 0);
        this.shareGroupStatePartitionMetadata = new TimelineHashMap<>(snapshotRegistry, 0);
        this.groupConfigManager = groupConfigManager;
        this.shareGroupAssignor = shareGroupAssignor;
        this.authorizerPlugin = authorizerPlugin;
        this.streamsGroupAssignors = streamsGroupAssignors.stream().collect(Collectors.toMap(TaskAssignor::name, Function.identity()));
        this.topicHashCache = new HashMap<>();
    }

    /**
     * @return The current metadata image used by the group metadata manager.
     */
    public CoordinatorMetadataImage image() {
        return metadataImage;
    }

    /**
     * @return The group corresponding to the group id or throw GroupIdNotFoundException.
     */
    public Group group(String groupId) throws GroupIdNotFoundException {
        Group group = groups.get(groupId, Long.MAX_VALUE);
        if (group == null) {
            throw new GroupIdNotFoundException(String.format("Group %s not found.", groupId));
        }
        return group;
    }

    /**
     * @return The group corresponding to the group id at the given committed offset
     *         or throw GroupIdNotFoundException.
     */
    public Group group(String groupId, long committedOffset) throws GroupIdNotFoundException {
        Group group = groups.get(groupId, committedOffset);
        if (group == null) {
            throw new GroupIdNotFoundException(String.format("Group %s not found.", groupId));
        }
        return group;
    }

    /**
     * Get the Group List.
     *
     * @param statesFilter      The states of the groups we want to list.
     *                          If empty, all groups are returned with their state.
     *                          If invalid, no groups are returned.
     * @param typesFilter       The types of the groups we want to list.
     *                          If empty, all groups are returned with their type.
     *                          If invalid, no groups are returned.
     * @param committedOffset   A specified committed offset corresponding to this shard.
     *
     * @return A list containing the ListGroupsResponseData.ListedGroup
     */
    public List<ListGroupsResponseData.ListedGroup> listGroups(
        Set<String> statesFilter,
        Set<String> typesFilter,
        long committedOffset
    ) {
        // Converts each state filter string to lower case for a case-insensitive comparison.
        Set<String> caseInsensitiveFilterSet = statesFilter.stream()
            .map(String::toLowerCase)
            .map(String::trim)
            .collect(Collectors.toSet());

        // Converts each type filter string to a value in the GroupType enum while being case-insensitive.
        Set<Group.GroupType> enumTypesFilter = typesFilter.stream()
            .map(Group.GroupType::parse)
            .collect(Collectors.toSet());

        Predicate<Group> combinedFilter = group -> {
            boolean stateCheck = statesFilter.isEmpty() || group.isInStates(caseInsensitiveFilterSet, committedOffset);
            boolean typeCheck = enumTypesFilter.isEmpty() || enumTypesFilter.contains(group.type());

            return stateCheck && typeCheck;
        };

        Stream<Group> groupStream = groups.values(committedOffset).stream();

        return groupStream
            .filter(combinedFilter)
            .map(group -> group.asListedGroup(committedOffset))
            .toList();
    }

    /**
     * Handles a ConsumerGroupDescribe request.
     *
     * @param groupIds          The IDs of the groups to describe.
     * @param committedOffset   A specified committed offset corresponding to this shard.
     *
     * @return A list containing the ConsumerGroupDescribeResponseData.DescribedGroup.
     */
    public List<ConsumerGroupDescribeResponseData.DescribedGroup> consumerGroupDescribe(
        List<String> groupIds,
        long committedOffset
    ) {
        final List<ConsumerGroupDescribeResponseData.DescribedGroup> describedGroups = new ArrayList<>();
        groupIds.forEach(groupId -> {
            try {
                describedGroups.add(consumerGroup(groupId, committedOffset).asDescribedGroup(
                    committedOffset,
                    defaultConsumerGroupAssignor.name(),
                    metadataImage
                ));
            } catch (GroupIdNotFoundException exception) {
                describedGroups.add(new ConsumerGroupDescribeResponseData.DescribedGroup()
                    .setGroupId(groupId)
                    .setErrorCode(Errors.GROUP_ID_NOT_FOUND.code())
                    .setErrorMessage(exception.getMessage())
                );
            }
        });

        return describedGroups;
    }

    /**
     * Handles a ShareGroupDescribe request.
     *
     * @param groupIds          The IDs of the groups to describe.
     * @param committedOffset   A specified committed offset corresponding to this shard.
     *
     * @return A list containing the ShareGroupDescribeResponseData.DescribedGroup.
     */
    public List<ShareGroupDescribeResponseData.DescribedGroup> shareGroupDescribe(
        List<String> groupIds,
        long committedOffset
    ) {
        final List<ShareGroupDescribeResponseData.DescribedGroup> describedGroups = new ArrayList<>();
        groupIds.forEach(groupId -> {
            try {
                describedGroups.add(shareGroup(groupId, committedOffset).asDescribedGroup(
                    committedOffset,
                    shareGroupAssignor.name(),
                    metadataImage
                ));
            } catch (GroupIdNotFoundException exception) {
                describedGroups.add(new ShareGroupDescribeResponseData.DescribedGroup()
                    .setGroupId(groupId)
                    .setErrorCode(Errors.GROUP_ID_NOT_FOUND.code())
                    .setErrorMessage(exception.getMessage())
                );
            }
        });

        return describedGroups;
    }

    /**
     * Handles a StreamsGroupDescribe request.
     *
     * @param groupIds          The IDs of the groups to describe.
     * @param committedOffset   A specified committed offset corresponding to this shard.
     *
     * @return A list containing the StreamsGroupDescribeResponseData.DescribedGroup.
     *         If a group is not found, the DescribedGroup will contain the error code and message.
     */
    public List<StreamsGroupDescribeResponseData.DescribedGroup> streamsGroupDescribe(
        List<String> groupIds,
        long committedOffset
    ) {
        final List<StreamsGroupDescribeResponseData.DescribedGroup> describedGroups = new ArrayList<>();
        groupIds.forEach(groupId -> {
            try {
                describedGroups.add(streamsGroup(groupId, committedOffset).asDescribedGroup(committedOffset));
            } catch (GroupIdNotFoundException exception) {
                describedGroups.add(new StreamsGroupDescribeResponseData.DescribedGroup()
                    .setGroupId(groupId)
                    .setErrorCode(Errors.GROUP_ID_NOT_FOUND.code())
                    .setErrorMessage(exception.getMessage())
                );
            }
        });

        return describedGroups;
    }

    /**
     * Handles a DescribeGroup request.
     *
     * @param context           The request context.
     * @param groupIds          The IDs of the groups to describe.
     * @param committedOffset   A specified committed offset corresponding to this shard.
     *
     * @return A list containing the DescribeGroupsResponseData.DescribedGroup.
     */
    public List<DescribeGroupsResponseData.DescribedGroup> describeGroups(
        AuthorizableRequestContext context,
        List<String> groupIds,
        long committedOffset
    ) {
        final List<DescribeGroupsResponseData.DescribedGroup> describedGroups = new ArrayList<>();
        groupIds.forEach(groupId -> {
            try {
                ClassicGroup group = classicGroup(groupId, committedOffset);

                if (group.isInState(STABLE)) {
                    if (group.protocolName().isEmpty()) {
                        throw new IllegalStateException("Invalid null group protocol for stable group");
                    }

                    describedGroups.add(new DescribeGroupsResponseData.DescribedGroup()
                        .setGroupId(groupId)
                        .setGroupState(group.stateAsString())
                        .setProtocolType(group.protocolType().orElse(""))
                        .setProtocolData(group.protocolName().get())
                        .setMembers(group.allMembers().stream()
                            .map(member -> member.describe(group.protocolName().get()))
                            .toList()
                        )
                    );
                } else {
                    describedGroups.add(new DescribeGroupsResponseData.DescribedGroup()
                        .setGroupId(groupId)
                        .setGroupState(group.stateAsString())
                        .setProtocolType(group.protocolType().orElse(""))
                        .setMembers(group.allMembers().stream()
                            .map(ClassicGroupMember::describeNoMetadata)
                            .toList()
                        )
                    );
                }
            } catch (GroupIdNotFoundException exception) {
                if (context.requestVersion() >= 6) {
                    describedGroups.add(new DescribeGroupsResponseData.DescribedGroup()
                        .setGroupId(groupId)
                        .setGroupState(DEAD.toString())
                        .setErrorCode(Errors.GROUP_ID_NOT_FOUND.code())
                        .setErrorMessage(exception.getMessage())
                    );
                } else {
                    describedGroups.add(new DescribeGroupsResponseData.DescribedGroup()
                        .setGroupId(groupId)
                        .setGroupState(DEAD.toString())
                    );
                }
            }
        });
        return describedGroups;
    }

    /**
     * Gets or maybe creates a consumer group without updating the groups map.
     * The group will be materialized during the replay.
     *
     * @param groupId           The group id.
     * @param createIfNotExists A boolean indicating whether the group should be
     *                          created if it does not exist or is an empty classic group.
     * @param records           The record list to which the group tombstones are written
     *                          if the group is empty and is a classic group.
     *
     * @return A ConsumerGroup.
     * @throws GroupIdNotFoundException if the group does not exist and createIfNotExists is false or
     *                                  if the group is not a consumer group.
     *
     * Package private for testing.
     */
    ConsumerGroup getOrMaybeCreateConsumerGroup(
        String groupId,
        boolean createIfNotExists,
        List<CoordinatorRecord> records
    ) throws GroupIdNotFoundException {
        Group group = groups.get(groupId);

        if (group == null && !createIfNotExists) {
            throw new GroupIdNotFoundException(String.format("Consumer group %s not found.", groupId));
        }

        if (group == null) {
            return new ConsumerGroup(snapshotRegistry, groupId);
        } else if (createIfNotExists && maybeDeleteEmptyClassicGroup(group, records)) {
            log.info("[GroupId {}] Converted the empty classic group to a consumer group.", groupId);
            return new ConsumerGroup(snapshotRegistry, groupId);
        } else {
            if (group.type() == CONSUMER) {
                return (ConsumerGroup) group;
            } else if (createIfNotExists && group.type() == CLASSIC) {
                validateOnlineUpgrade((ClassicGroup) group);
                return convertToConsumerGroup((ClassicGroup) group, records);
            } else {
                throw new GroupIdNotFoundException(String.format("Group %s is not a consumer group.", groupId));
            }
        }
    }

    /**
     * Gets or creates a streams group without updating the groups map.
     * The group will be materialized during the replay.
     *
     * If there is an empty classic consumer group of the same name, it will be deleted and a new streams
     * group will be created.
     *
     * @param groupId           The group ID.
     * @param records           The record list to which the group tombstones are written
     *                          if the group is empty and is a classic group.
     *
     * @return A StreamsGroup.
     *
     * Package private for testing.
     */
    StreamsGroup getOrCreateStreamsGroup(
        String groupId,
        List<CoordinatorRecord> records
    ) {
        Group group = groups.get(groupId);

        if (group == null) {
            return new StreamsGroup(logContext, snapshotRegistry, groupId);
        } else if (maybeDeleteEmptyClassicGroup(group, records)) {
            log.info("[GroupId {}] Converted the empty classic group to a streams group.", groupId);
            return new StreamsGroup(logContext, snapshotRegistry, groupId);
        } else {
            return castToStreamsGroup(group);
        }
    }

    /**
     * Gets a streams group without updating the groups map. If the group does not exist,
     * a GroupIdNotFoundException is thrown.
     *
     * @param groupId           The group ID.
     *
     * @return A StreamsGroup.
     * @throws GroupIdNotFoundException if the group does not exist or is not a streams group.
     *
     * Package private for testing.
     */
    StreamsGroup getStreamsGroupOrThrow(
        String groupId
    ) throws GroupIdNotFoundException {
        Group group = groups.get(groupId);

        if (group == null) {
            throw new GroupIdNotFoundException(String.format("Streams group %s not found.", groupId));
        } else {
            return castToStreamsGroup(group);
        }
    }

    private StreamsGroup castToStreamsGroup(final Group group) {
        if (group.type() == STREAMS) {
            return (StreamsGroup) group;
        } else {
            throw new GroupIdNotFoundException(String.format("Group %s is not a streams group.", group.groupId()));
        }
    }

    /**
     * Gets a streams group by committed offset.
     *
     * @param groupId           The group id.
     * @param committedOffset   A specified committed offset corresponding to this shard.
     *
     * @return A StreamsGroup.
     * @throws GroupIdNotFoundException if the group does not exist or is not a streams group.
     */
    private StreamsGroup streamsGroup(
        String groupId,
        long committedOffset
    ) throws GroupIdNotFoundException {
        Group group = group(groupId, committedOffset);

        if (group.type() == STREAMS) {
            return (StreamsGroup) group;
        } else {
            // We don't support upgrading/downgrading between protocols at the moment so
            // we throw an exception if a group exists with the wrong type.
            throw new GroupIdNotFoundException(String.format("Group %s is not a streams group.",
                groupId));
        }
    }

    /**
     * An overloaded method of {@link GroupMetadataManager#streamsGroup(String, long)}
     */
    StreamsGroup streamsGroup(
        String groupId
    ) throws GroupIdNotFoundException {
        return streamsGroup(groupId, Long.MAX_VALUE);
    }

    /**
     * Gets a consumer group by committed offset.
     *
     * @param groupId           The group id.
     * @param committedOffset   A specified committed offset corresponding to this shard.
     *
     * @return A ConsumerGroup.
     * @throws GroupIdNotFoundException if the group does not exist or is not a consumer group.
     */
    public ConsumerGroup consumerGroup(
        String groupId,
        long committedOffset
    ) throws GroupIdNotFoundException {
        Group group = group(groupId, committedOffset);

        if (group.type() == CONSUMER) {
            return (ConsumerGroup) group;
        } else {
            throw new GroupIdNotFoundException(String.format("Group %s is not a consumer group.", groupId));
        }
    }

    /**
     * An overloaded method of {@link GroupMetadataManager#consumerGroup(String, long)}
     */
    ConsumerGroup consumerGroup(
        String groupId
    ) throws GroupIdNotFoundException {
        return consumerGroup(groupId, Long.MAX_VALUE);
    }

    /**
     * The method should be called on the replay path.
     * Gets or maybe creates a consumer group and updates the groups map if a new group is created.
     *
     * @param groupId           The group id.
     * @param createIfNotExists A boolean indicating whether the group should be
     *                          created if it does not exist.
     *
     * @return A ConsumerGroup.
     * @throws GroupIdNotFoundException if the group does not exist and createIfNotExists is false.
     * @throws IllegalStateException    if the group does not have the expected type.
     * Package private for testing.
     */
    ConsumerGroup getOrMaybeCreatePersistedConsumerGroup(
        String groupId,
        boolean createIfNotExists
    ) throws GroupIdNotFoundException, IllegalStateException {
        Group group = groups.get(groupId);

        if (group == null && !createIfNotExists) {
            throw new GroupIdNotFoundException(String.format("Consumer group %s not found.", groupId));
        }

        if (group == null) {
            ConsumerGroup consumerGroup = new ConsumerGroup(snapshotRegistry, groupId);
            groups.put(groupId, consumerGroup);
            return consumerGroup;
        } else if (group.type() == CONSUMER) {
            return (ConsumerGroup) group;
        } else if (group.type() == CLASSIC && ((ClassicGroup) group).isSimpleGroup()) {
            // If the group is a simple classic group, it was automatically created to hold committed
            // offsets if no group existed. Simple classic groups are not backed by any records
            // in the __consumer_offsets topic hence we can safely replace it here. Without this,
            // replaying consumer group records after offset commit records would not work.
            ConsumerGroup consumerGroup = new ConsumerGroup(snapshotRegistry, groupId);
            groups.put(groupId, consumerGroup);
            return consumerGroup;
        } else {
            throw new IllegalStateException(String.format("Group %s is not a consumer group", groupId));
        }
    }

    /**
     * The method should be called on the replay path.
     * Gets or maybe creates a streams group and updates the groups map if a new group is created.
     *
     * @param groupId           The group id.
     * @param createIfNotExists A boolean indicating whether the group should be
     *                          created if it does not exist.
     *
     * @return A StreamsGroup.
     * @throws GroupIdNotFoundException if the group does not exist and createIfNotExists is false or
     *                                  if the group is not a streams group.
     * @throws IllegalStateException    if the group does not have the expected type.
     * Package private for testing.
     */
    StreamsGroup getOrMaybeCreatePersistedStreamsGroup(
        String groupId,
        boolean createIfNotExists
    ) throws GroupIdNotFoundException, IllegalStateException {
        Group group = groups.get(groupId);

        if (group == null && !createIfNotExists) {
            throw new GroupIdNotFoundException(String.format("Streams group %s not found.", groupId));
        }

        if (group == null) {
            StreamsGroup streamsGroup = new StreamsGroup(logContext, snapshotRegistry, groupId);
            groups.put(groupId, streamsGroup);
            return streamsGroup;
        } else if (group.type() == STREAMS) {
            return (StreamsGroup) group;
        } else {
            // We don't support upgrading/downgrading between protocols at the moment, so
            // we throw an exception if a group exists with the wrong type.
            throw new IllegalStateException(String.format("Group %s is not a streams group.", groupId));
        }
    }

    /**
     * Gets or maybe creates a classic group.
     *
     * @param groupId           The group id.
     * @param createIfNotExists A boolean indicating whether the group should be
     *                          created if it does not exist.
     *
     * @return A ClassicGroup.
     * @throws GroupIdNotFoundException if the group does not exist and createIfNotExists is false or
     *                                  if the group is not a classic group.
     *
     * Package private for testing.
     */
    ClassicGroup getOrMaybeCreateClassicGroup(
        String groupId,
        boolean createIfNotExists
    ) throws GroupIdNotFoundException {
        Group group = groups.get(groupId);

        if (group == null && !createIfNotExists) {
            throw new GroupIdNotFoundException(String.format("Classic group %s not found.", groupId));
        }

        if (group == null) {
            ClassicGroup classicGroup = new ClassicGroup(logContext, groupId, ClassicGroupState.EMPTY, time);
            groups.put(groupId, classicGroup);
            return classicGroup;
        } else {
            if (group.type() == CLASSIC) {
                return (ClassicGroup) group;
            } else {
                throw new GroupIdNotFoundException(String.format("Group %s is not a classic group.", groupId));
            }
        }
    }

    /**
     * Gets a classic group by committed offset.
     *
     * @param groupId           The group id.
     * @param committedOffset   A specified committed offset corresponding to this shard.
     *
     * @return A ClassicGroup.
     * @throws GroupIdNotFoundException if the group does not exist or is not a classic group.
     */
    public ClassicGroup classicGroup(
        String groupId,
        long committedOffset
    ) throws GroupIdNotFoundException {
        Group group = group(groupId, committedOffset);

        if (group.type() == CLASSIC) {
            return (ClassicGroup) group;
        } else {
            throw new GroupIdNotFoundException(String.format("Group %s is not a classic group.", groupId));
        }
    }

    /**
     * Gets or maybe creates a share group without updating the groups map.
     * The group will be materialized during the replay.
     *
     * @param groupId           The group id.
     * @param createIfNotExists A boolean indicating whether the group should be
     *                          created if it does not exist.
     *
     * @return A ShareGroup.
     * @throws GroupIdNotFoundException if the group does not exist and createIfNotExists is false or
     *                                  if the group is not a share group.
     */
    private ShareGroup getOrMaybeCreateShareGroup(
        String groupId,
        boolean createIfNotExists
    ) throws GroupIdNotFoundException {
        Group group = groups.get(groupId);

        if (group == null && !createIfNotExists) {
            throw new GroupIdNotFoundException(String.format("Share group %s not found.", groupId));
        }

        if (group == null) {
            return new ShareGroup(snapshotRegistry, groupId);
        } else {
            if (group.type() == SHARE) {
                return (ShareGroup) group;
            } else {
                // We don't support upgrading/downgrading between protocols at the moment so
                // we throw an exception if a group exists with the wrong type.
                throw new GroupIdNotFoundException(String.format("Group %s is not a share group.", groupId));
            }
        }
    }

    /**
     * The method should be called on the replay path.
     * Gets or maybe creates a share group and updates the groups map if a new group is created.
     *
     * @param groupId           The group id.
     * @param createIfNotExists A boolean indicating whether the group should be
     *                          created if it does not exist.
     *
     * @return A ShareGroup.
     * @throws GroupIdNotFoundException if the group does not exist and createIfNotExists is false.
     * @throws IllegalStateException    if the group does not have the expected type.
     *
     * Package private for testing.
     */
    ShareGroup getOrMaybeCreatePersistedShareGroup(
        String groupId,
        boolean createIfNotExists
    ) throws GroupIdNotFoundException {
        Group group = groups.get(groupId);

        if (group == null && !createIfNotExists) {
            throw new GroupIdNotFoundException(String.format("Share group %s not found.", groupId));
        }

        if (group == null) {
            ShareGroup shareGroup = new ShareGroup(snapshotRegistry, groupId);
            groups.put(groupId, shareGroup);
            return shareGroup;
        } else {
            if (group.type() == SHARE) {
                return (ShareGroup) group;
            } else {
                // We don't support upgrading/downgrading between protocols at the moment so
                // we throw an exception if a group exists with the wrong type.
                throw new IllegalStateException(String.format("Group %s is not a share group.", groupId));
            }
        }
    }

    /**
     * Gets a share group by committed offset.
     *
     * @param groupId           The group id.
     * @param committedOffset   A specified committed offset corresponding to this shard.
     *
     * @return A ShareGroup.
     * @throws GroupIdNotFoundException if the group does not exist or is not a share group.
     */
    public ShareGroup shareGroup(
        String groupId,
        long committedOffset
    ) throws GroupIdNotFoundException {
        Group group = group(groupId, committedOffset);

        if (group.type() == SHARE) {
            return (ShareGroup) group;
        } else {
            // We don't support upgrading/downgrading between protocols at the moment so
            // we throw an exception if a group exists with the wrong type.
            throw new GroupIdNotFoundException(String.format("Group %s is not a share group.",
                groupId));
        }
    }

    /**
     * An overloaded method of {@link GroupMetadataManager#shareGroup(String, long)}
     */
    ShareGroup shareGroup(
        String groupId
    ) throws GroupIdNotFoundException {
        return shareGroup(groupId, Long.MAX_VALUE);
    }

    /**
     * Validates the online downgrade if consumer members are fenced from the consumer group.
     *
     * @param consumerGroup     The ConsumerGroup.
     * @param fencedMembers     The fenced members.
     * @return A boolean indicating whether it's valid to online downgrade the consumer group.
     */
    private boolean validateOnlineDowngradeWithFencedMembers(ConsumerGroup consumerGroup, Set<ConsumerGroupMember> fencedMembers) {
        if (!consumerGroup.allMembersUseClassicProtocolExcept(fencedMembers)) {
            return false;
        } else if (consumerGroup.numMembers() - fencedMembers.size() <= 0) {
            log.debug("Skip downgrading the consumer group {} to classic group because it's empty.",
                consumerGroup.groupId());
            return false;
        } else if (!config.consumerGroupMigrationPolicy().isDowngradeEnabled()) {
            log.info("Cannot downgrade consumer group {} to classic group because the online downgrade is disabled.",
                consumerGroup.groupId());
            return false;
        } else if (consumerGroup.numMembers() - fencedMembers.size() > config.classicGroupMaxSize()) {
            log.info("Cannot downgrade consumer group {} to classic group because its group size is greater than classic group max size.",
                consumerGroup.groupId());
            return false;
        }
        return true;
    }

    /**
     * Validates whether the group id is eligible for an online downgrade if an existing
     * static member is replaced by another new one uses the classic protocol.
     *
     * @param consumerGroup     The group to downgrade.
     * @param replacedMember    The replaced member.
     *
     * @return A boolean indicating whether it's valid to online downgrade the consumer group.
     */
    private boolean validateOnlineDowngradeWithReplacedMember(
        ConsumerGroup consumerGroup,
        ConsumerGroupMember replacedMember
    ) {
        if (!consumerGroup.allMembersUseClassicProtocolExcept(replacedMember)) {
            return false;
        } else if (!config.consumerGroupMigrationPolicy().isDowngradeEnabled()) {
            log.info("Cannot downgrade consumer group {} to classic group because the online downgrade is disabled.",
                consumerGroup.groupId());
            return false;
        } else if (consumerGroup.numMembers() > config.classicGroupMaxSize()) {
            log.info("Cannot downgrade consumer group {} to classic group because its group size is greater than classic group max size.",
                consumerGroup.groupId());
            return false;
        }
        return true;
    }

    /**
     * Creates a ClassicGroup corresponding to the given ConsumerGroup.
     *
     * @param consumerGroup             The converted ConsumerGroup.
     * @param leavingMembers            The leaving member(s) that triggered the downgrade validation.
     * @param joiningMember             The newly joined member if the downgrade is triggered by static member replacement.
     *                                  When not null, must have an instanceId that matches the replaced member.
     * @param hasSubscriptionChanged    The boolean indicating whether the joining member has a different subscription
     *                                  from the replaced member. Only used when joiningMember is set.
     * @param records                   The record list to which the conversion records are added.
     */
    private void convertToClassicGroup(
        ConsumerGroup consumerGroup,
        Set<ConsumerGroupMember> leavingMembers,
        ConsumerGroupMember joiningMember,
        boolean hasSubscriptionChanged,
        List<CoordinatorRecord> records
    ) {
        if (joiningMember == null) {
            consumerGroup.createGroupTombstoneRecords(records);
        } else {
            // We've already generated the records to replace replacedMember with joiningMember,
            // so we need to tombstone joiningMember instead.
            ConsumerGroupMember replacedMember = consumerGroup.staticMember(joiningMember.instanceId());
            if (replacedMember == null) {
                throw new IllegalArgumentException("joiningMember must be a static member when not null.");
            }
            consumerGroup.createGroupTombstoneRecordsWithReplacedMember(records, replacedMember.memberId(), joiningMember.memberId());
        }

        ClassicGroup classicGroup;
        try {
            classicGroup = ClassicGroup.fromConsumerGroup(
                consumerGroup,
                leavingMembers,
                joiningMember,
                logContext,
                time,
                metadataImage
            );
        } catch (SchemaException e) {
            log.warn("Cannot downgrade the consumer group " + consumerGroup.groupId() + ": fail to parse " +
                "the Consumer Protocol " + ConsumerProtocol.PROTOCOL_TYPE + ".", e);

            throw new GroupIdNotFoundException(String.format("Cannot downgrade the classic group %s: %s.",
                consumerGroup.groupId(), e.getMessage()));
        }
        classicGroup.createClassicGroupRecords(records);

        // Directly update the states instead of replaying the records because
        // the classicGroup reference is needed for triggering the rebalance.
        removeGroup(consumerGroup.groupId());
        groups.put(consumerGroup.groupId(), classicGroup);

        classicGroup.allMembers().forEach(member -> rescheduleClassicGroupMemberHeartbeat(classicGroup, member));

        // If the downgrade is triggered by a member leaving the group or a static
        // member replacement with a different subscription, a rebalance should be triggered.
        if (joiningMember == null) {
            prepareRebalance(classicGroup, String.format("Downgrade group %s from consumer to classic for member leaving.", classicGroup.groupId()));
        } else if (hasSubscriptionChanged) {
            prepareRebalance(classicGroup, String.format("Downgrade group %s from consumer to classic for static member replacement with different subscription.", classicGroup.groupId()));
        }

        log.info("[GroupId {}] Converted the consumer group to a classic group.", consumerGroup.groupId());
    }

    /**
     * Validates the online upgrade if the Classic Group receives a ConsumerGroupHeartbeat request.
     *
     * @param classicGroup A ClassicGroup.
     * @throws GroupIdNotFoundException if the group cannot be upgraded.
     */
    private void validateOnlineUpgrade(ClassicGroup classicGroup) {
        if (!config.consumerGroupMigrationPolicy().isUpgradeEnabled()) {
            log.info("Cannot upgrade classic group {} to consumer group because online upgrade is disabled.",
                classicGroup.groupId());
            throw new GroupIdNotFoundException(
                String.format("Cannot upgrade classic group %s to consumer group because online upgrade is disabled.", classicGroup.groupId())
            );
        } else if (!classicGroup.usesConsumerGroupProtocol()) {
            log.info("Cannot upgrade classic group {} to consumer group because the group does not use the consumer embedded protocol.",
                classicGroup.groupId());
            throw new GroupIdNotFoundException(
                String.format("Cannot upgrade classic group %s to consumer group because the group does not use the consumer embedded protocol.", classicGroup.groupId())
            );
        } else if (classicGroup.numMembers() > config.consumerGroupMaxSize()) {
            log.info("Cannot upgrade classic group {} to consumer group because the group size exceeds the consumer group maximum size.",
                classicGroup.groupId());
            throw new GroupIdNotFoundException(
                String.format("Cannot upgrade classic group %s to consumer group because the group size exceeds the consumer group maximum size.", classicGroup.groupId())
            );
        }
    }

    /**
     * Creates a ConsumerGroup corresponding to the given classic group.
     *
     * @param classicGroup  The ClassicGroup to convert.
     * @param records       The list of Records.
     * @return The created ConsumerGroup.
     */
    ConsumerGroup convertToConsumerGroup(ClassicGroup classicGroup, List<CoordinatorRecord> records) {
        // The upgrade is always triggered by a new member joining the classic group, which always results in
        // updatedMember.subscribedTopicNames changing, the group epoch being bumped, and triggering a new rebalance.
        // If the ClassicGroup is rebalancing, inform the awaiting consumers of another ongoing rebalance
        // so that they will rejoin for the new rebalance.
        classicGroup.completeAllJoinFutures(Errors.REBALANCE_IN_PROGRESS);
        classicGroup.completeAllSyncFutures(Errors.REBALANCE_IN_PROGRESS);

        classicGroup.createGroupTombstoneRecords(records);

        ConsumerGroup consumerGroup;
        try {
            consumerGroup = ConsumerGroup.fromClassicGroup(
                snapshotRegistry,
                classicGroup,
                topicHashCache,
                metadataImage
            );
        } catch (SchemaException e) {
            log.warn("Cannot upgrade classic group " + classicGroup.groupId() +
                " to consumer group because the embedded consumer protocol is malformed: "
                + e.getMessage() + ".", e);

            throw new GroupIdNotFoundException(
                String.format("Cannot upgrade classic group %s to consumer group because the embedded consumer protocol is malformed.", classicGroup.groupId())
            );
        } catch (UnsupportedVersionException e) {
            log.warn("Cannot upgrade classic group " + classicGroup.groupId() +
                " to consumer group: " + e.getMessage() + ".", e);

            throw new GroupIdNotFoundException(
                String.format("Cannot upgrade classic group %s to consumer group because an unsupported custom assignor is in use. " +
                    "Please refer to the documentation or switch to a default assignor before re-attempting the upgrade.", classicGroup.groupId())
            );
        }
        consumerGroup.createConsumerGroupRecords(records);

        // Create the session timeouts for the new members. If the conversion fails, the group will remain a
        // classic group, thus these timers will fail the group type check and do nothing.
        consumerGroup.members().forEach((memberId, member) ->
            scheduleConsumerGroupSessionTimeout(consumerGroup.groupId(), memberId, member.classicProtocolSessionTimeout().get())
        );

        log.info("[GroupId {}] Converted the classic group to a consumer group.", classicGroup.groupId());

        return consumerGroup;
    }

    /**
     * Removes the group.
     *
     * @param groupId The group id.
     */
    private void removeGroup(
        String groupId
    ) {
        groups.remove(groupId);
    }

    /**
     * Verifies that the partitions currently owned by the member (the ones set in the
     * request) matches the ones that the member should own. It matches if the consumer
     * only owns partitions which are in the assigned partitions. It does not match if
     * it owns any other partitions.
     *
     * @param ownedTopicPartitions  The partitions provided by the consumer in the request.
     * @param target                The partitions that the member should have.
     *
     * @return A boolean indicating whether the owned partitions are a subset or not.
     */
    private static boolean isSubset(
        List<ConsumerGroupHeartbeatRequestData.TopicPartitions> ownedTopicPartitions,
        Map<Uuid, Set<Integer>> target
    ) {
        if (ownedTopicPartitions == null) return false;

        for (ConsumerGroupHeartbeatRequestData.TopicPartitions topicPartitions : ownedTopicPartitions) {
            Set<Integer> partitions = target.get(topicPartitions.topicId());
            if (partitions == null) return false;
            for (Integer partitionId : topicPartitions.partitions()) {
                if (!partitions.contains(partitionId)) return false;
            }
        }

        return true;
    }

    /**
     * Verifies that the tasks currently owned by the member (the ones set in the
     * request) matches the ones that the member should own. It matches if the streams
     * group member only owns tasks which are in the assigned tasks. It does not match if
     * it owns any other tasks.
     *
     * @param ownedTasks    The tasks provided by the streams group member in the request.
     *                      If this is null, it indicates that we do not know which
     *                      tasks are owned by the member, so we return false.
     * @param assignedTasks The tasks that the member should have.
     *
     * @return A boolean indicating whether the owned partitions are a subset or not.
     */
    private static boolean areOwnedTasksContainedInAssignedTasks(
        List<StreamsGroupHeartbeatRequestData.TaskIds> ownedTasks,
        Map<String, Set<Integer>> assignedTasks
    ) {
        if (ownedTasks == null) return false;

        for (StreamsGroupHeartbeatRequestData.TaskIds ownedTasksOfSubtopology : ownedTasks) {
            Set<Integer> partitions = assignedTasks.get(ownedTasksOfSubtopology.subtopologyId());
            if (partitions == null) return false;
            for (Integer partitionId : ownedTasksOfSubtopology.partitions()) {
                if (!partitions.contains(partitionId)) return false;
            }
        }

        return true;
    }

    /**
     * Checks whether all the tasks contained in the list are included in the provided assignment with epochs.
     *
     * @param ownedTasks              The tasks provided by the streams group member in the request.
     *                                If this is null, it indicates that we do not know which
     *                                tasks are owned by the member, so we return false.
     * @param assignedTasksWithEpochs The tasks that the member should have (with epochs).
     * @return A boolean indicating whether the owned partitions are a subset or not.
     */
    private static boolean areOwnedTasksContainedInAssignedTasksWithEpochs(
        List<StreamsGroupHeartbeatRequestData.TaskIds> ownedTasks,
        Map<String, Map<Integer, Integer>> assignedTasksWithEpochs
    ) {
        if (ownedTasks == null) return false;

        for (StreamsGroupHeartbeatRequestData.TaskIds ownedTasksOfSubtopology : ownedTasks) {
            Map<Integer, Integer> partitionsWithEpochs = assignedTasksWithEpochs.get(ownedTasksOfSubtopology.subtopologyId());
            if (partitionsWithEpochs == null) return false;
            for (Integer partitionId : ownedTasksOfSubtopology.partitions()) {
                if (!partitionsWithEpochs.containsKey(partitionId)) return false;
            }
        }

        return true;
    }

    /**
     * Checks whether the consumer group can accept a new member or not based on the
     * max group size defined.
     *
     * @param group     The consumer group.
     * @param memberId  The member id.
     *
     * @throws GroupMaxSizeReachedException if the maximum capacity has been reached.
     */
    private void throwIfConsumerGroupIsFull(
        ConsumerGroup group,
        String memberId
    ) throws GroupMaxSizeReachedException {
        // If the consumer group has reached its maximum capacity, the member is rejected if it is not
        // already a member of the consumer group.
        if (group.numMembers() >= config.consumerGroupMaxSize() && (memberId.isEmpty() || !group.hasMember(memberId))) {
            throw new GroupMaxSizeReachedException("The consumer group has reached its maximum capacity of "
                + config.consumerGroupMaxSize() + " members.");
        }
    }

    /**
     * Checks whether the share group can accept a new member or not based on the
     * max group size defined.
     *
     * @param group     The share group.
     * @param memberId  The member id.
     *
     * @throws GroupMaxSizeReachedException if the maximum capacity has been reached.
     */
    private void throwIfShareGroupIsFull(
        ShareGroup group,
        String memberId
    ) throws GroupMaxSizeReachedException {
        // The member is rejected, if the share group has reached its maximum capacity, or it is not
        // a member of the share group.
        if (group.numMembers() >= config.shareGroupMaxSize() && (memberId.isEmpty() || !group.hasMember(memberId))) {
            throw new GroupMaxSizeReachedException("The share group has reached its maximum capacity of "
                + config.shareGroupMaxSize() + " members.");
        }
    }

    /**
     * Checks whether the share group is empty.
     *
     * @param group     The share group.
     *
     * @throws GroupNotEmptyException if the group is not empty.
     */
    private void throwIfShareGroupIsNotEmpty(
        ShareGroup group
    ) throws GroupNotEmptyException {
        if (group.numMembers() > 0) {
            throw new GroupNotEmptyException(Errors.NON_EMPTY_GROUP.message());
        }
    }

    /**
     * Validates the member epoch provided in the heartbeat request.
     *
     * @param member                The consumer group member.
     * @param receivedMemberEpoch   The member epoch.
     * @param ownedTopicPartitions  The owned partitions.
     *
     * @throws FencedMemberEpochException if the provided epoch is ahead of or behind the epoch known
     *                                    by this coordinator.
     */
    private void throwIfConsumerGroupMemberEpochIsInvalid(
        ConsumerGroupMember member,
        int receivedMemberEpoch,
        List<ConsumerGroupHeartbeatRequestData.TopicPartitions> ownedTopicPartitions
    ) {
        if (receivedMemberEpoch > member.memberEpoch()) {
            throw new FencedMemberEpochException("The consumer group member has a greater member "
                + "epoch (" + receivedMemberEpoch + ") than the one known by the group coordinator ("
                + member.memberEpoch() + "). The member must abandon all its partitions and rejoin.");
        } else if (receivedMemberEpoch < member.memberEpoch()) {
            // If the member comes with the previous epoch and has a subset of the current assignment partitions,
            // we accept it because the response with the bumped epoch may have been lost.
            if (receivedMemberEpoch != member.previousMemberEpoch() || !isSubset(ownedTopicPartitions, member.assignedPartitions())) {
                throw new FencedMemberEpochException("The consumer group member has a smaller member "
                    + "epoch (" + receivedMemberEpoch + ") than the one known by the group coordinator ("
                    + member.memberEpoch() + "). The member must abandon all its partitions and rejoin.");
            }
        }
    }

    /**
     * Validates the member epoch provided in the heartbeat request.
     *
     * @param member                The share group member.
     * @param receivedMemberEpoch   The member epoch.
     *
     * @throws FencedMemberEpochException if the provided epoch is ahead of or behind the epoch known
     *                                    by this coordinator.
     */
    private void throwIfShareGroupMemberEpochIsInvalid(
        ShareGroupMember member,
        int receivedMemberEpoch
    ) {
        if (receivedMemberEpoch > member.memberEpoch()) {
            throw new FencedMemberEpochException("The share group member has a greater member "
                + "epoch (" + receivedMemberEpoch + ") than the one known by the group coordinator ("
                + member.memberEpoch() + "). The member must abandon all its partitions and rejoin.");
        } else if (receivedMemberEpoch < member.memberEpoch()) {
            // If the member comes with the previous epoch, we accept it because the response with the bumped epoch may have been lost.
            if (receivedMemberEpoch != member.previousMemberEpoch()) {
                throw new FencedMemberEpochException("The share group member has a smaller member "
                    + "epoch (" + receivedMemberEpoch + ") than the one known by the group coordinator ("
                    + member.memberEpoch() + "), and it does not match the previous member epoch ("
                    + member.previousMemberEpoch() + "). The member must abandon all its partitions and rejoin.");
            }
        }
    }

    /**
     * Validates if the received instanceId has been released from the group
     *
     * @param member                The consumer group member.
     * @param groupId               The consumer group id.
     * @param receivedMemberId      The member id received in the request.
     * @param receivedInstanceId    The instance id received in the request.
     *
     * @throws UnreleasedInstanceIdException if the instance id received in the request is still in use by an existing static member.
     */
    private void throwIfInstanceIdIsUnreleased(ConsumerGroupMember member, String groupId, String receivedMemberId, String receivedInstanceId) {
        if (member.memberEpoch() != LEAVE_GROUP_STATIC_MEMBER_EPOCH) {
            // The new member can't join.
            log.info("[GroupId {}] Static member {} with instance id {} cannot join the group because the instance id is" +
                    " owned by member {}.", groupId, receivedMemberId, receivedInstanceId, member.memberId());
            throw Errors.UNRELEASED_INSTANCE_ID.exception("Static member " + receivedMemberId + " with instance id "
                + receivedInstanceId + " cannot join the group because the instance id is owned by " + member.memberId() + " member.");
        }
    }

    /**
     * Validates if the received instanceId has been released from the group
     *
     * @param member                The consumer group member.
     * @param groupId               The consumer group id.
     * @param receivedMemberId      The member id received in the request.
     * @param receivedInstanceId    The instance id received in the request.
     *
     * @throws FencedInstanceIdException if the instance id provided is fenced because of another static member.
     */
    private void throwIfInstanceIdIsFenced(ConsumerGroupMember member, String groupId, String receivedMemberId, String receivedInstanceId) {
        if (!member.memberId().equals(receivedMemberId)) {
            log.info("[GroupId {}] Static member {} with instance id {} is fenced by existing member {}.",
                groupId, receivedMemberId, receivedInstanceId, member.memberId());
            throw Errors.FENCED_INSTANCE_ID.exception("Static member " + receivedMemberId + " with instance id "
                + receivedInstanceId + " was fenced by member " + member.memberId() + ".");
        }
    }

    /**
     * Validates if the received instanceId has been released from the group
     *
     * @param staticMember          The static member in the group.
     * @param receivedInstanceId    The instance id received in the request.
     *
     * @throws UnknownMemberIdException if no static member exists in the group against the provided instance id.
     */
    private void throwIfStaticMemberIsUnknown(ConsumerGroupMember staticMember, String receivedInstanceId) {
        if (staticMember == null) {
            throw Errors.UNKNOWN_MEMBER_ID.exception("Instance id " + receivedInstanceId + " is unknown.");
        }
    }

    /**
     * Checks whether the streams group can accept a new member or not based on the
     * max group size defined.
     *
     * @param group     The streams group.
     *
     * @throws GroupMaxSizeReachedException if the maximum capacity has been reached.
     */
    private void throwIfStreamsGroupIsFull(
        StreamsGroup group
    ) throws GroupMaxSizeReachedException {
        // If the streams group has reached its maximum capacity, the member is rejected if it is not
        // already a member of the streams group.
        if (group.numMembers() >= config.streamsGroupMaxSize()) {
            throw new GroupMaxSizeReachedException("The streams group has reached its maximum capacity of "
                + config.streamsGroupMaxSize() + " members.");
        }
    }

    /**
     * Validates the member epoch provided in the heartbeat request.
     *
     * @param member                The streams group member.
     * @param receivedMemberEpoch   The member epoch.
     * @param ownedActiveTasks      The owned active tasks.
     * @param ownedStandbyTasks     The owned standby tasks.
     * @param ownedWarmupTasks      The owned warmup tasks.
     *
     * @throws FencedMemberEpochException if the provided epoch is ahead or behind the epoch known
     *                                    by this coordinator.
     */
    private static void throwIfStreamsGroupMemberEpochIsInvalid(
        StreamsGroupMember member,
        int receivedMemberEpoch,
        List<StreamsGroupHeartbeatRequestData.TaskIds> ownedActiveTasks,
        List<StreamsGroupHeartbeatRequestData.TaskIds> ownedStandbyTasks,
        List<StreamsGroupHeartbeatRequestData.TaskIds> ownedWarmupTasks
    ) {
        if (receivedMemberEpoch > member.memberEpoch()) {
            throw new FencedMemberEpochException("The streams group member has a greater member "
                + "epoch (" + receivedMemberEpoch + ") than the one known by the group coordinator ("
                + member.memberEpoch() + "). The member must abandon all its partitions and rejoin.");
        } else if (receivedMemberEpoch < member.memberEpoch()) {
            // If the member comes with the previous epoch and has a subset of the current assignment partitions,
            // we accept it because the response with the bumped epoch may have been lost.
            if (receivedMemberEpoch != member.previousMemberEpoch()
                || !areOwnedTasksContainedInAssignedTasksWithEpochs(ownedActiveTasks, member.assignedTasks().activeTasksWithEpochs())
                || !areOwnedTasksContainedInAssignedTasks(ownedStandbyTasks, member.assignedTasks().standbyTasks())
                || !areOwnedTasksContainedInAssignedTasks(ownedWarmupTasks, member.assignedTasks().warmupTasks())) {
                throw new FencedMemberEpochException("The streams group member has a smaller member "
                    + "epoch (" + receivedMemberEpoch + ") than the one known by the group coordinator ("
                    + member.memberEpoch() + "). The member must abandon all its partitions and rejoin.");
            }
        }
    }

    /**
     * Validates that the requested tasks exist in the configured topology and partitions are valid.
     * If tasks is null, does nothing. If an invalid task is found, throws InvalidRequestException.
     *
     * @param subtopologySortedMap The configured topology.
     * @param tasks                The list of requested tasks.
     */
    private static void throwIfRequestContainsInvalidTasks(
        SortedMap<String, ConfiguredSubtopology> subtopologySortedMap,
        List<StreamsGroupHeartbeatRequestData.TaskIds> tasks
    ) {
        if (tasks == null || tasks.isEmpty()) return;
        for (StreamsGroupHeartbeatRequestData.TaskIds task : tasks) {
            String subtopologyId = task.subtopologyId();
            ConfiguredSubtopology subtopology = subtopologySortedMap.get(subtopologyId);
            if (subtopology == null) {
                throw new InvalidRequestException("Subtopology " + subtopologyId + " does not exist in the topology.");
            }
            int numTasks = subtopology.numberOfTasks();
            for (Integer partition : task.partitions()) {
                if (partition < 0 || partition >= numTasks) {
                    throw new InvalidRequestException("Task " + partition + " for subtopology " + subtopologyId +
                        " is invalid. Number of tasks for this subtopology: " + numTasks);
                }
            }
        }
    }

    /**
     * Validates if the received classic member protocols are supported by the group.
     *
     * @param group         The ConsumerGroup.
     * @param memberId      The joining member id.
     * @param protocolType  The joining member protocol type.
     * @param protocols     The joining member protocol collection.
     */
    private void throwIfClassicProtocolIsNotSupported(
        ConsumerGroup group,
        String memberId,
        String protocolType,
        JoinGroupRequestProtocolCollection protocols
    ) {
        if (!group.supportsClassicProtocols(protocolType, ClassicGroupMember.plainProtocolSet(protocols))) {
            throw Errors.INCONSISTENT_GROUP_PROTOCOL.exception("Member " + memberId + "'s protocols are not supported.");
        }
    }

    /**
     * Validates if the consumer group member uses the classic protocol.
     *
     * @param member The ConsumerGroupMember.
     */
    private void throwIfMemberDoesNotUseClassicProtocol(ConsumerGroupMember member) {
        if (!member.useClassicProtocol()) {
            throw new UnknownMemberIdException(
                String.format("Member %s does not use the classic protocol.", member.memberId())
            );
        }
    }

    /**
     * Validates if the generation id from the request matches the member epoch.
     *
     * @param memberId              The member id.
     * @param memberEpoch           The member epoch.
     * @param requestGenerationId   The generation id from the request.
     */
    private void throwIfGenerationIdUnmatched(
        String memberId,
        int memberEpoch,
        int requestGenerationId
    ) {
        if (memberEpoch != requestGenerationId) {
            throw Errors.ILLEGAL_GENERATION.exception(
                String.format("The request generation id %s is not equal to the member epoch %d of member %s.",
                    requestGenerationId, memberEpoch, memberId)
            );
        }
    }

    /**
     * Validates if the protocol type and the protocol name from the request matches those of the consumer group.
     *
     * @param member                The ConsumerGroupMember.
     * @param requestProtocolType   The protocol type from the request.
     * @param requestProtocolName   The protocol name from the request.
     */
    private void throwIfClassicProtocolUnmatched(
        ConsumerGroupMember member,
        String requestProtocolType,
        String requestProtocolName
    ) {
        String protocolName = member.supportedClassicProtocols().get().iterator().next().name();
        if (requestProtocolType != null && !ConsumerProtocol.PROTOCOL_TYPE.equals(requestProtocolType)) {
            throw Errors.INCONSISTENT_GROUP_PROTOCOL.exception(
                String.format("The protocol type %s from member %s request is not equal to the group protocol type %s.",
                    requestProtocolType, member.memberId(), ConsumerProtocol.PROTOCOL_TYPE)
            );
        } else if (requestProtocolName != null && !protocolName.equals(requestProtocolName)) {
            throw Errors.INCONSISTENT_GROUP_PROTOCOL.exception(
                String.format("The protocol name %s from member %s request is not equal to the protocol name %s returned in the join response.",
                    requestProtocolName, member.memberId(), protocolName)
            );
        }
    }

    /**
     * Validates if a new rebalance has been triggered and the member should rejoin to catch up.
     *
     * @param group     The ConsumerGroup.
     * @param member    The ConsumerGroupMember.
     */
    private void throwIfRebalanceInProgress(
        ConsumerGroup group,
        ConsumerGroupMember member
    ) {
        // If the group epoch is greater than the member epoch, there is a new rebalance triggered and the member
        // needs to rejoin to catch up. However, if the member is in UNREVOKED_PARTITIONS state, it means the
        // member has already rejoined, so it needs to first finish revoking the partitions and the reconciliation,
        // and then the next rejoin will be triggered automatically if needed.
        if (group.groupEpoch() > member.memberEpoch() && !member.state().equals(MemberState.UNREVOKED_PARTITIONS)) {
            scheduleConsumerGroupJoinTimeoutIfAbsent(group.groupId(), member.memberId(), member.rebalanceTimeoutMs());
            throw Errors.REBALANCE_IN_PROGRESS.exception(
                String.format("A new rebalance is triggered in group %s and member %s should rejoin to catch up.",
                    group.groupId(), member.memberId())
            );
        }
    }

    /**
     * Deserialize the subscription in JoinGroupRequestProtocolCollection.
     * All the protocols have the same subscription, so the method picks a random one.
     *
     * @param protocols The JoinGroupRequestProtocolCollection.
     * @return The ConsumerProtocolSubscription.
     */
    private static ConsumerProtocolSubscription deserializeSubscription(
        JoinGroupRequestProtocolCollection protocols
    ) {
        try {
            return ConsumerProtocol.deserializeConsumerProtocolSubscription(
                ByteBuffer.wrap(protocols.iterator().next().metadata())
            );
        } catch (SchemaException e) {
            throw new IllegalStateException("Malformed embedded consumer protocol in subscription deserialization.");
        }
    }

    /**
     * Handles a regular heartbeat from a streams group member.
     * It mainly consists of five parts:
     * 1) Create or update the member.
     *    The group epoch is bumped if the member has been created or updated.
     * 2) Initialize or update the topology.
     *    The group epoch is bumped if the topology has been created or updated.
     * 3) Determine the partition metadata and any internal topics that need to be created.
     * 4) Update the target assignment for the streams group if the group epoch
     *    is larger than the current target assignment epoch.
     * 5) Reconcile the member's assignment with the target assignment.
     *
     * @param groupId             The group ID from the request.
     * @param memberId            The member ID from the request.
     * @param memberEpoch         The member epoch from the request.
     * @param instanceId          The instance ID from the request or null.
     * @param rackId              The rack ID from the request or null.
     * @param rebalanceTimeoutMs  The rebalance timeout from the request or -1.
     * @param clientId            The client ID.
     * @param clientHost          The client host.
     * @param topology            The topology from the request or null.
     * @param ownedActiveTasks    The list of owned active tasks from the request or null.
     * @param ownedStandbyTasks   The list of owned standby tasks from the request or null.
     * @param ownedWarmupTasks    The list of owned warmup tasks from the request or null.
     * @param userEndpoint        User-defined endpoint for Interactive Queries, or null.
     * @param clientTags          Used for rack-aware assignment algorithm, or null.
     * @param shutdownApplication Whether all Streams clients in the group should shut down.
     * @param memberEndpointEpoch The last endpoint information epoch seen be the group member.
     * @return A result containing the StreamsGroupHeartbeat response and a list of records to update the state machine.
     */
    private CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> streamsGroupHeartbeat(
        String groupId,
        String memberId,
        int memberEpoch,
        String instanceId,
        String rackId,
        int rebalanceTimeoutMs,
        String clientId,
        String clientHost,
        StreamsGroupHeartbeatRequestData.Topology topology,
        List<TaskIds> ownedActiveTasks,
        List<TaskIds> ownedStandbyTasks,
        List<TaskIds> ownedWarmupTasks,
        String processId,
        Endpoint userEndpoint,
        List<KeyValue> clientTags,
        boolean shutdownApplication,
        int memberEndpointEpoch
    ) throws ApiException {
        final long currentTimeMs = time.milliseconds();
        final List<CoordinatorRecord> records = new ArrayList<>();
        final List<StreamsGroupHeartbeatResponseData.Status> returnedStatus = new ArrayList<>();

        // Get or create the streams group.
        boolean isJoining = memberEpoch == 0;
        StreamsGroup group;
        if (isJoining) {
            group = getOrCreateStreamsGroup(groupId, records);
            throwIfStreamsGroupIsFull(group);
        } else {
            group = getStreamsGroupOrThrow(groupId);
        }

        // Get or create the member.
        StreamsGroupMember member;
        if (instanceId == null) {
            member = getOrMaybeCreateDynamicStreamsGroupMember(
                group,
                memberId,
                memberEpoch,
                ownedActiveTasks,
                ownedStandbyTasks,
                ownedWarmupTasks,
                isJoining
            );
        } else {
            throw new UnsupportedOperationException("Static members are not supported yet.");
        }

        // 1. Create or update the member.
        StreamsGroupMember updatedMember = new StreamsGroupMember.Builder(member)
            .maybeUpdateInstanceId(Optional.empty())
            .maybeUpdateRackId(Optional.ofNullable(rackId))
            .maybeUpdateRebalanceTimeoutMs(ofSentinel(rebalanceTimeoutMs))
            .maybeUpdateTopologyEpoch(topology != null ? OptionalInt.of(topology.epoch()) : OptionalInt.empty())
            .setClientId(clientId)
            .setClientHost(clientHost)
            .maybeUpdateProcessId(Optional.ofNullable(processId))
            .maybeUpdateClientTags(Optional.ofNullable(clientTags).map(x -> x.stream().collect(Collectors.toMap(KeyValue::key, KeyValue::value))))
            .maybeUpdateUserEndpoint(Optional.ofNullable(userEndpoint).map(x -> new StreamsGroupMemberMetadataValue.Endpoint().setHost(x.host()).setPort(x.port())))
            .build();

        // If the member is new or has changed, a StreamsGroupMemberMetadataValue record is written to the __consumer_offsets partition
        // to persist the change, and bump the group epoch later.
        boolean bumpGroupEpoch = hasStreamsMemberMetadataChanged(groupId, member, updatedMember, records);

        // 2. Initialize/Update the group topology.
        // If the topology is new or has changed, a StreamsGroupTopologyValue record is written to the __consumer_offsets partition to persist
        // the change. The group epoch is bumped if the topology has changed.
        StreamsTopology updatedTopology = maybeUpdateTopology(groupId, memberId, topology, group, records);
        maybeSetTopologyStaleStatus(group, updatedMember, returnedStatus);

        // 3. Determine any internal topics if needed.
        ConfiguredTopology updatedConfiguredTopology;
        boolean reconfigureTopology = group.topology().isEmpty();
        long metadataHash = group.metadataHash();
        if (reconfigureTopology || group.configuredTopology().isEmpty() || group.hasMetadataExpired(currentTimeMs)) {

            metadataHash = group.computeMetadataHash(
                metadataImage,
                topicHashCache,
                updatedTopology
            );

            if (metadataHash != group.metadataHash()) {
                log.info("[GroupId {}][MemberId {}] Computed new metadata hash: {}.",
                    groupId, memberId, metadataHash);
                bumpGroupEpoch = true;
                reconfigureTopology = true;
            }

            if (reconfigureTopology || group.configuredTopology().isEmpty()) {
                log.info("[GroupId {}][MemberId {}] Configuring the topology {}", groupId, memberId, updatedTopology);
                updatedConfiguredTopology = InternalTopicManager.configureTopics(logContext, metadataHash, updatedTopology, metadataImage);
                group.setConfiguredTopology(updatedConfiguredTopology);
            } else {
                updatedConfiguredTopology = group.configuredTopology().get();
            }
        } else {
            updatedConfiguredTopology = group.configuredTopology().get();
        }

        // 3b. If the topology is validated, persist the fact that it is validated.
        int validatedTopologyEpoch = -1;
        if (updatedConfiguredTopology.isReady()) {
            validatedTopologyEpoch = updatedTopology.topologyEpoch();
            SortedMap<String, ConfiguredSubtopology> subtopologySortedMap = updatedConfiguredTopology.subtopologies().get();
            throwIfRequestContainsInvalidTasks(subtopologySortedMap, ownedActiveTasks);
            throwIfRequestContainsInvalidTasks(subtopologySortedMap, ownedStandbyTasks);
            throwIfRequestContainsInvalidTasks(subtopologySortedMap, ownedWarmupTasks);
        }
        // We validated a topology that was not validated before, so bump the group epoch as we may have to reassign tasks.
        if (validatedTopologyEpoch != group.validatedTopologyEpoch()) {
            bumpGroupEpoch = true;
        }

        // Check if assignment configurations have changed
        Map<String, String> currentAssignmentConfigs = streamsGroupAssignmentConfigs(groupId);
        Map<String, String> storedAssignmentConfigs = group.lastAssignmentConfigs();
        if (!bumpGroupEpoch && !currentAssignmentConfigs.equals(storedAssignmentConfigs)) {
            log.info("[GroupId {}][MemberId {}] Assignment configurations changed to {}. Triggering rebalance.",
                groupId, memberId, currentAssignmentConfigs);
            bumpGroupEpoch = true;
        }

        // Actually bump the group epoch
        int groupEpoch = group.groupEpoch();
        if (bumpGroupEpoch) {
            groupEpoch += 1;
            records.add(newStreamsGroupMetadataRecord(groupId, groupEpoch, metadataHash, validatedTopologyEpoch, currentAssignmentConfigs));
            log.info("[GroupId {}][MemberId {}] Bumped streams group epoch to {} with metadata hash {} and validated topic epoch {}.", groupId, memberId, groupEpoch, metadataHash, validatedTopologyEpoch);
            metrics.record(STREAMS_GROUP_REBALANCES_SENSOR_NAME);
            group.setMetadataRefreshDeadline(currentTimeMs + METADATA_REFRESH_INTERVAL_MS, groupEpoch);
        }

        // 4. Update the target assignment if the group epoch is larger than the target assignment epoch or a static member
        // replaces an existing static member.
        // The delta between the existing and the new target assignment is persisted to the partition.
        int targetAssignmentEpoch;
        TasksTuple targetAssignment;
        if (groupEpoch > group.assignmentEpoch()) {
            targetAssignment = updateStreamsTargetAssignment(
                group,
                groupEpoch,
                updatedMember,
                updatedConfiguredTopology,
                metadataImage,
                records,
                currentAssignmentConfigs
            );
            targetAssignmentEpoch = groupEpoch;
        } else {
            targetAssignmentEpoch = group.assignmentEpoch();
            targetAssignment = group.targetAssignment(updatedMember.memberId());
        }

        // 5. Reconcile the member's assignment with the target assignment if the member is not
        // fully reconciled yet.
        updatedMember = maybeReconcile(
            groupId,
            updatedMember,
            group::currentActiveTaskProcessId,
            group::currentStandbyTaskProcessIds,
            group::currentWarmupTaskProcessIds,
            targetAssignmentEpoch,
            targetAssignment,
            ownedActiveTasks,
            ownedStandbyTasks,
            ownedWarmupTasks,
            records
        );

        scheduleStreamsGroupSessionTimeout(groupId, memberId);
        if (shutdownApplication) {
            group.setShutdownRequestMemberId(memberId);
        }

        // Prepare the response.
        StreamsGroupHeartbeatResponseData response = new StreamsGroupHeartbeatResponseData()
            .setMemberId(updatedMember.memberId())
            .setMemberEpoch(updatedMember.memberEpoch())
            .setHeartbeatIntervalMs(streamsGroupHeartbeatIntervalMs(groupId));
        // The assignment is only provided in the following cases:
        // 1. The member is joining.
        // 2. The member's assignment has been updated.
        if (memberEpoch == 0 || hasAssignedTasksChanged(member, updatedMember)) {
            response.setActiveTasks(createStreamsGroupHeartbeatResponseTaskIdsFromEpochs(updatedMember.assignedTasks().activeTasksWithEpochs()));
            response.setStandbyTasks(createStreamsGroupHeartbeatResponseTaskIds(updatedMember.assignedTasks().standbyTasks()));
            response.setWarmupTasks(createStreamsGroupHeartbeatResponseTaskIds(updatedMember.assignedTasks().warmupTasks()));
            if (updatedMember.userEndpoint().isPresent()) {
                // If no user endpoint is defined, there is no change in the endpoint information.
                // Otherwise, bump the endpoint information epoch
                group.setEndpointInformationEpoch(group.endpointInformationEpoch() + 1);
            }
        }

        if (group.endpointInformationEpoch() != memberEndpointEpoch) {
            response.setPartitionsByUserEndpoint(maybeBuildEndpointToPartitions(group, updatedMember));
        }
        if (groups.containsKey(group.groupId())) {
            // If we just created the group, the endpoint information epoch will not be persisted, so return epoch 0.
            // Otherwise, return the bumped epoch.
            response.setEndpointInformationEpoch(group.endpointInformationEpoch());
        }

        Map<String, CreatableTopic> internalTopicsToBeCreated = Collections.emptyMap();
        if (updatedConfiguredTopology.topicConfigurationException().isPresent()) {
            TopicConfigurationException exception = updatedConfiguredTopology.topicConfigurationException().get();
            internalTopicsToBeCreated = updatedConfiguredTopology.internalTopicsToBeCreated();
            returnedStatus.add(
                new StreamsGroupHeartbeatResponseData.Status()
                    .setStatusCode(exception.status().code())
                    .setStatusDetail(exception.getMessage())
            );
        }

        group.getShutdownRequestMemberId().ifPresent(requestingMemberId -> returnedStatus.add(
            new Status()
                .setStatusCode(StreamsGroupHeartbeatResponse.Status.SHUTDOWN_APPLICATION.code())
                .setStatusDetail(
                    String.format("Streams group member %s encountered a fatal error and requested a shutdown for the entire application.",
                        requestingMemberId)
                )
        ));

        if (!returnedStatus.isEmpty()) {
            response.setStatus(returnedStatus);
        }
        return new CoordinatorResult<>(records, new StreamsGroupHeartbeatResult(response, internalTopicsToBeCreated));
    }

    /**
     * Checks if the member's topology epoch is behind the group's topology epoch, and sets the corresponding status.
     *
     * @param group          The streams group.
     * @param member         The streams group member.
     * @param returnedStatus A mutable collection of status to be returned in the response.
     */
    private static void maybeSetTopologyStaleStatus(final StreamsGroup group, final StreamsGroupMember member, final List<Status> returnedStatus) {
        if (group.topology().isPresent() && member.topologyEpoch() < group.topology().get().topologyEpoch()) {
            returnedStatus.add(
                new Status()
                    .setStatusCode(StreamsGroupHeartbeatResponse.Status.STALE_TOPOLOGY.code())
                    .setStatusDetail(
                        String.format(
                            "The member's topology epoch %d is behind the group's topology epoch %d.",
                            member.topologyEpoch(),
                            group.topology().get().topologyEpoch()
                        )
                    )
            );
        }
    }

    /**
     * Compares the topology from the request with the one in the group.
     *
     *  - If the topology of the group is uninitialized, it is initialized with the topology from the request. A corresponding
     *    record is added to records.
     *  - If the topology of the group is initialized, and the request defines a topology, they are compared. If they
     *    are not empty, an InvalidRequestException is thrown.
     *
     * @param groupId  The group ID.
     * @param memberId The member ID.
     * @param topology The topology provided in the request. May be null.
     * @param group    The streams group.
     * @param records  A mutable collection of records to be written to the __consumer_offsets partition.
     * @return The new topology of the group (which may be the same as the current one).
     */
    private StreamsTopology maybeUpdateTopology(final String groupId,
                                                final String memberId,
                                                final Topology topology,
                                                final StreamsGroup group,
                                                final List<CoordinatorRecord> records) {
        if (topology != null) {
            StreamsTopology streamsTopologyFromRequest = StreamsTopology.fromHeartbeatRequest(topology);
            if (group.topology().isEmpty()) {
                log.info("[GroupId {}][MemberId {}] Member initialized the topology with epoch {}", groupId, memberId, topology.epoch());
                StreamsGroupTopologyValue recordValue = convertToStreamsGroupTopologyRecord(topology);
                records.add(newStreamsGroupTopologyRecord(groupId, recordValue));
                return streamsTopologyFromRequest;
            } else if (group.topology().get().topologyEpoch() > topology.epoch()) {
                log.info("[GroupId {}][MemberId {}] Member joined with stale topology epoch {}", groupId, memberId, topology.epoch());
                return group.topology().get();
            } else if (!group.topology().get().equals(streamsTopologyFromRequest)) {
                throw new InvalidRequestException("Topology updates are not supported yet.");
            } else {
                log.debug("[GroupId {}][MemberId {}] Member joined with currently initialized topology {}", groupId, memberId, topology.epoch());
                return group.topology().get();
            }
        } else if (group.topology().isPresent()) {
            return group.topology().get();
        } else {
            throw new IllegalStateException("The topology is null and the group topology is also null.");
        }
    }

    private static List<StreamsGroupHeartbeatResponseData.TaskIds> createStreamsGroupHeartbeatResponseTaskIds(final Map<String, Set<Integer>> taskIds) {
        return taskIds.entrySet().stream()
            .map(entry -> new StreamsGroupHeartbeatResponseData.TaskIds()
                .setSubtopologyId(entry.getKey())
                .setPartitions(entry.getValue().stream().sorted().toList()))
            .collect(Collectors.toList());
    }

    private static List<StreamsGroupHeartbeatResponseData.TaskIds> createStreamsGroupHeartbeatResponseTaskIdsFromEpochs(
        final Map<String, Map<Integer, Integer>> taskIdsWithEpochs
    ) {
        return taskIdsWithEpochs.entrySet().stream()
            .map(entry -> new StreamsGroupHeartbeatResponseData.TaskIds()
                .setSubtopologyId(entry.getKey())
                .setPartitions(entry.getValue().keySet().stream().sorted().toList()))
            .collect(Collectors.toList());
    }

    private List<StreamsGroupHeartbeatResponseData.EndpointToPartitions> maybeBuildEndpointToPartitions(StreamsGroup group,
                                                                                                        StreamsGroupMember updatedMember) {
        List<StreamsGroupHeartbeatResponseData.EndpointToPartitions> endpointToPartitionsList = new ArrayList<>();
        final Map<String, StreamsGroupMember> members = group.members();
        // Build endpoint information for all members except the updated member
        for (Map.Entry<String, StreamsGroupMember> entry : members.entrySet()) {
            if (updatedMember != null && entry.getKey().equals(updatedMember.memberId())) {
                continue;
            }
            EndpointToPartitionsManager.maybeEndpointToPartitions(entry.getValue(), group, metadataImage)
                .ifPresent(endpointToPartitionsList::add);
        }
        // Always build endpoint information for the updated member (whether new or existing)
        if (updatedMember != null) {
            EndpointToPartitionsManager.maybeEndpointToPartitions(updatedMember, group, metadataImage)
                .ifPresent(endpointToPartitionsList::add);
        }
        return endpointToPartitionsList;
    }

    /**
     * Handles a regular heartbeat from a consumer group member. It mainly consists of
     * three parts:
     * 1) The member is created or updated. The group epoch is bumped if the member
     *    has been created or updated.
     * 2) The target assignment for the consumer group is updated if the group epoch
     *    is larger than the current target assignment epoch.
     * 3) The member's assignment is reconciled with the target assignment.
     *
     * @param context               The request context.
     * @param groupId               The group id from the request.
     * @param memberId              The member id from the request.
     * @param memberEpoch           The member epoch from the request.
     * @param instanceId            The instance id from the request or null.
     * @param rackId                The rack id from the request or null.
     * @param rebalanceTimeoutMs    The rebalance timeout from the request or -1.
     * @param subscribedTopicNames  The list of subscribed topic names from the request
     *                              or null.
     * @param subscribedTopicRegex  The regular expression based subscription from the request
     *                              or null.
     * @param assignorName          The assignor name from the request or null.
     * @param ownedTopicPartitions  The list of owned partitions from the request or null.
     *
     * @return A Result containing the ConsumerGroupHeartbeat response and
     *         a list of records to update the state machine.
     */
    private CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> consumerGroupHeartbeat(
        AuthorizableRequestContext context,
        String groupId,
        String memberId,
        int memberEpoch,
        String instanceId,
        String rackId,
        int rebalanceTimeoutMs,
        List<String> subscribedTopicNames,
        String subscribedTopicRegex,
        String assignorName,
        List<ConsumerGroupHeartbeatRequestData.TopicPartitions> ownedTopicPartitions
    ) throws ApiException {
        final long currentTimeMs = time.milliseconds();
        final List<CoordinatorRecord> records = new ArrayList<>();

        // Get or create the consumer group.
        boolean createIfNotExists = memberEpoch == 0;
        final ConsumerGroup group = getOrMaybeCreateConsumerGroup(groupId, createIfNotExists, records);
        throwIfConsumerGroupIsFull(group, memberId);

        // Get or create the member.
        if (memberId.isEmpty()) memberId = Uuid.randomUuid().toString();
        final ConsumerGroupMember member;
        if (instanceId == null) {
            member = getOrMaybeSubscribeDynamicConsumerGroupMember(
                group,
                memberId,
                memberEpoch,
                ownedTopicPartitions,
                createIfNotExists,
                false
            );
        } else {
            member = getOrMaybeSubscribeStaticConsumerGroupMember(
                group,
                memberId,
                memberEpoch,
                instanceId,
                ownedTopicPartitions,
                createIfNotExists,
                false,
                records
            );
        }

        // 1. Create or update the member. If the member is new or has changed, a ConsumerGroupMemberMetadataValue
        // record is written to the __consumer_offsets partition to persist the change. If the subscriptions have
        // changed, the subscription metadata is updated and persisted by writing a ConsumerGroupPartitionMetadataValue
        // record to the __consumer_offsets partition. Finally, the group epoch is bumped if the subscriptions have
        // changed, and persisted by writing a ConsumerGroupMetadataValue record to the partition.
        ConsumerGroupMember updatedMember = new ConsumerGroupMember.Builder(member)
            .maybeUpdateInstanceId(Optional.ofNullable(instanceId))
            .maybeUpdateRackId(Optional.ofNullable(rackId))
            .maybeUpdateRebalanceTimeoutMs(ofSentinel(rebalanceTimeoutMs))
            .maybeUpdateServerAssignorName(Optional.ofNullable(assignorName))
            .maybeUpdateSubscribedTopicNames(Optional.ofNullable(subscribedTopicNames))
            .maybeUpdateSubscribedTopicRegex(Optional.ofNullable(subscribedTopicRegex))
            .setClientId(context.clientId())
            .setClientHost(context.clientAddress().toString())
            .setClassicMemberMetadata(null)
            .build();

        boolean subscribedTopicNamesChanged = hasMemberSubscriptionChanged(
            groupId,
            member,
            updatedMember,
            records
        );
        UpdateRegularExpressionsResult updateRegularExpressionsResult = maybeUpdateRegularExpressions(
            context,
            group,
            member,
            updatedMember,
            records
        );

        // The subscription has changed when either the subscribed topic names or subscribed topic
        // regex has changed.
        boolean hasSubscriptionChanged = subscribedTopicNamesChanged || updateRegularExpressionsResult.regexUpdated();
        int groupEpoch = group.groupEpoch();
        SubscriptionType subscriptionType = group.subscriptionType();

        boolean bumpGroupEpoch =
            // If the group is newly created, we must ensure that it moves away from
            // epoch 0 and that it is fully initialized.
            groupEpoch == 0 ||
            // Bumping the group epoch signals that the target assignment should be updated. We bump
            // the group epoch when the member has changed its subscribed topic names or the member
            // has changed its subscribed topic regex to a regex that is already resolved. We avoid
            // bumping the group epoch when the new subscribed topic regex has not been resolved
            // yet, since we will have to update the target assignment again later.
            subscribedTopicNamesChanged ||
            updateRegularExpressionsResult == UpdateRegularExpressionsResult.REGEX_UPDATED_AND_RESOLVED;

        if (bumpGroupEpoch || group.hasMetadataExpired(currentTimeMs)) {
            // The subscription metadata is updated in two cases:
            // 1) The member has updated its subscriptions;
            // 2) The refresh deadline has been reached.
            UpdateSubscriptionMetadataResult result = updateSubscriptionMetadata(
                group,
                bumpGroupEpoch,
                member,
                updatedMember,
                records
            );

            groupEpoch = result.groupEpoch;
            subscriptionType = result.subscriptionType;
        }

        // 2. Update the target assignment if the group epoch is larger than the target assignment epoch. The delta between
        // the existing and the new target assignment is persisted to the partition.
        final int targetAssignmentEpoch;
        final Assignment targetAssignment;

        if (groupEpoch > group.assignmentEpoch()) {
            targetAssignment = updateTargetAssignment(
                group,
                groupEpoch,
                member,
                updatedMember,
                subscriptionType,
                records
            );
            targetAssignmentEpoch = groupEpoch;
        } else {
            targetAssignmentEpoch = group.assignmentEpoch();
            targetAssignment = group.targetAssignment(updatedMember.memberId(), updatedMember.instanceId());
        }

        // 3. Reconcile the member's assignment with the target assignment if the member is not
        // fully reconciled yet.
        updatedMember = maybeReconcile(
            groupId,
            updatedMember,
            group::currentPartitionEpoch,
            targetAssignmentEpoch,
            targetAssignment,
            group.resolvedRegularExpressions(),
            // Force consistency with the subscription when the subscription has changed.
            hasSubscriptionChanged,
            ownedTopicPartitions,
            records
        );

        scheduleConsumerGroupSessionTimeout(groupId, memberId);

        // Prepare the response.
        ConsumerGroupHeartbeatResponseData response = new ConsumerGroupHeartbeatResponseData()
            .setMemberId(updatedMember.memberId())
            .setMemberEpoch(updatedMember.memberEpoch())
            .setHeartbeatIntervalMs(consumerGroupHeartbeatIntervalMs(groupId));

        // The assignment is only provided in the following cases:
        // 1. The member sent a full request. It does so when joining or rejoining the group with zero
        //    as the member epoch; or on any errors (e.g. timeout). We use all the non-optional fields
        //    (rebalanceTimeoutMs, (subscribedTopicNames or subscribedTopicRegex) and ownedTopicPartitions)
        //    to detect a full request as those must be set in a full request.
        // 2. The member's assignment has been updated.
        boolean isFullRequest = rebalanceTimeoutMs != -1 && (subscribedTopicNames != null || subscribedTopicRegex != null) && ownedTopicPartitions != null;
        if (memberEpoch == 0 || isFullRequest || hasAssignedPartitionsChanged(member, updatedMember)) {
            response.setAssignment(ConsumerGroupHeartbeatResponse.createAssignment(updatedMember.assignedPartitions()));
        }

        return new CoordinatorResult<>(records, response);
    }

    /**
     * Handle a JoinGroupRequest to a ConsumerGroup.
     *
     * @param group          The group to join.
     * @param context        The request context.
     * @param request        The actual JoinGroup request.
     * @param responseFuture The join group response future.
     *
     * @return The result that contains records to append if the join group phase completes.
     */
    private CoordinatorResult<Void, CoordinatorRecord> classicGroupJoinToConsumerGroup(
        ConsumerGroup group,
        AuthorizableRequestContext context,
        JoinGroupRequestData request,
        CompletableFuture<JoinGroupResponseData> responseFuture
    ) throws ApiException {
        final long currentTimeMs = time.milliseconds();
        final List<CoordinatorRecord> records = new ArrayList<>();
        final String groupId = request.groupId();
        final String instanceId = request.groupInstanceId();
        final int sessionTimeoutMs = request.sessionTimeoutMs();
        final JoinGroupRequestProtocolCollection protocols = request.protocols();

        String memberId = request.memberId();
        final boolean isUnknownMember = memberId.equals(UNKNOWN_MEMBER_ID);
        if (isUnknownMember) memberId = Uuid.randomUuid().toString();

        throwIfConsumerGroupIsFull(group, memberId);
        throwIfClassicProtocolIsNotSupported(group, memberId, request.protocolType(), protocols);

        if (JoinGroupRequest.requiresKnownMemberId(request, context.requestVersion())) {
            // A dynamic member requiring a member id joins the group. Send back a response to call for another
            // join group request with allocated member id.
            responseFuture.complete(new JoinGroupResponseData()
                .setMemberId(memberId)
                .setErrorCode(Errors.MEMBER_ID_REQUIRED.code())
            );
            log.info("[GroupId {}] Dynamic member with unknown member id joins the consumer group. " +
                "Created a new member id {} and requesting the member to rejoin with this id.", groupId, memberId);
            return EMPTY_RESULT;
        }

        // Get or create the member.
        final ConsumerGroupMember member;
        if (instanceId == null) {
            member = getOrMaybeSubscribeDynamicConsumerGroupMember(
                group,
                memberId,
                -1,
                List.of(),
                true,
                true
            );
        } else {
            member = getOrMaybeSubscribeStaticConsumerGroupMember(
                group,
                memberId,
                -1,
                instanceId,
                List.of(),
                isUnknownMember,
                true,
                records
            );
        }

        ConsumerGroupMember existingStaticMemberOrNull = group.staticMember(request.groupInstanceId());
        boolean downgrade = existingStaticMemberOrNull != null &&
            validateOnlineDowngradeWithReplacedMember(group, existingStaticMemberOrNull);

        int groupEpoch = group.groupEpoch();
        SubscriptionType subscriptionType = group.subscriptionType();
        final ConsumerProtocolSubscription subscription = deserializeSubscription(protocols);

        // 1. Create or update the member. If the member is new or has changed, a ConsumerGroupMemberMetadataValue
        // record is written to the __consumer_offsets partition to persist the change. If the subscriptions have
        // changed, the subscription metadata is updated and persisted by writing a ConsumerGroupPartitionMetadataValue
        // record to the __consumer_offsets partition. Finally, the group epoch is bumped if the subscriptions have
        // changed, and persisted by writing a ConsumerGroupMetadataValue record to the partition.
        ConsumerGroupMember updatedMember = new ConsumerGroupMember.Builder(member)
            .maybeUpdateInstanceId(Optional.ofNullable(instanceId))
            .maybeUpdateRackId(Utils.toOptional(subscription.rackId()))
            .maybeUpdateRebalanceTimeoutMs(ofSentinel(request.rebalanceTimeoutMs()))
            .maybeUpdateServerAssignorName(Optional.empty())
            .maybeUpdateSubscribedTopicNames(Optional.ofNullable(subscription.topics()))
            .setClientId(context.clientId())
            .setClientHost(context.clientAddress().toString())
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(sessionTimeoutMs)
                    .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(protocols)))
            .build();

        boolean bumpGroupEpoch = hasMemberSubscriptionChanged(
            groupId,
            member,
            updatedMember,
            records
        );

        if (bumpGroupEpoch || group.hasMetadataExpired(currentTimeMs)) {
            // The subscription metadata is updated in two cases:
            // 1) The member has updated its subscriptions;
            // 2) The refresh deadline has been reached.
            UpdateSubscriptionMetadataResult result = updateSubscriptionMetadata(
                group,
                bumpGroupEpoch,
                member,
                updatedMember,
                records
            );

            groupEpoch = result.groupEpoch;
            subscriptionType = result.subscriptionType;
        }

        if (downgrade) {
            // 2. If the static member subscription hasn't changed, reconcile the member's assignment with the existing
            // assignment if the member is not fully reconciled yet. If the static member subscription has changed, a
            // rebalance will be triggered during downgrade anyway so we can skip the reconciliation.
            if (!bumpGroupEpoch) {
                updatedMember = maybeReconcile(
                    groupId,
                    updatedMember,
                    group::currentPartitionEpoch,
                    group.assignmentEpoch(),
                    group.targetAssignment(updatedMember.memberId(), updatedMember.instanceId()),
                    group.resolvedRegularExpressions(),
                    bumpGroupEpoch,
                    toTopicPartitions(subscription.ownedPartitions(), metadataImage),
                    records
                );
            }

            // 3. Downgrade the consumer group.
            convertToClassicGroup(
                group,
                Set.of(),
                updatedMember,
                bumpGroupEpoch,
                records
            );
        } else {
            // If no downgrade is triggered.

            // 2. Update the target assignment if the group epoch is larger than the target assignment epoch.
            // The delta between the existing and the new target assignment is persisted to the partition.
            final int targetAssignmentEpoch;
            final Assignment targetAssignment;

            if (groupEpoch > group.assignmentEpoch()) {
                targetAssignment = updateTargetAssignment(
                    group,
                    groupEpoch,
                    member,
                    updatedMember,
                    subscriptionType,
                    records
                );
                targetAssignmentEpoch = groupEpoch;
            } else {
                targetAssignmentEpoch = group.assignmentEpoch();
                targetAssignment = group.targetAssignment(updatedMember.memberId(), updatedMember.instanceId());
            }

            // 3. Reconcile the member's assignment with the target assignment if the member is not fully reconciled yet.
            updatedMember = maybeReconcile(
                groupId,
                updatedMember,
                group::currentPartitionEpoch,
                targetAssignmentEpoch,
                targetAssignment,
                group.resolvedRegularExpressions(),
                // Force consistency with the subscription when the subscription has changed.
                bumpGroupEpoch,
                toTopicPartitions(subscription.ownedPartitions(), metadataImage),
                records
            );
        }

        final JoinGroupResponseData response = new JoinGroupResponseData()
            .setMemberId(updatedMember.memberId())
            .setGenerationId(updatedMember.memberEpoch())
            .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
            .setProtocolName(updatedMember.supportedClassicProtocols().get().iterator().next().name());

        CompletableFuture<Void> appendFuture = new CompletableFuture<>();
        appendFuture.whenComplete((__, t) -> {
            if (t == null) {
                cancelConsumerGroupJoinTimeout(groupId, response.memberId());
                if (!downgrade) {
                    // If the group is still a consumer group, schedule the session
                    // timeout for the joining member and the sync timeout to ensure
                    // that the member send sync request within the rebalance timeout.
                    scheduleConsumerGroupSessionTimeout(groupId, response.memberId(), sessionTimeoutMs);
                    scheduleConsumerGroupSyncTimeout(groupId, response.memberId(), request.rebalanceTimeoutMs());
                }
                responseFuture.complete(response);
            }
        });

        // If the joining member triggers a valid downgrade, the soft states will be directly
        // updated in the conversion method, so the records don't need to be replayed.
        // If the joining member doesn't trigger a valid downgrade, the group is still a
        // consumer group. We still rely on replaying records to update the soft states.
        return new CoordinatorResult<>(records, null, appendFuture, !downgrade);
    }

    /**
     * Handles a ShareGroupHeartbeat request.
     *
     * @param groupId               The group id from the request.
     * @param memberId              The member id from the request.
     * @param memberEpoch           The member epoch from the request.
     * @param rackId                The rack id from the request or null.
     * @param clientId              The client id.
     * @param clientHost            The client host.
     * @param subscribedTopicNames  The list of subscribed topic names from the request or null.
     *
     * @return A Result containing a pair of ShareGroupHeartbeat response and maybe InitializeShareGroupStateParameters
     *         and a list of records to update the state machine.
     */
    private CoordinatorResult<Map.Entry<ShareGroupHeartbeatResponseData, Optional<InitializeShareGroupStateParameters>>, CoordinatorRecord> shareGroupHeartbeat(
        String groupId,
        String memberId,
        int memberEpoch,
        String rackId,
        String clientId,
        String clientHost,
        List<String> subscribedTopicNames
    ) throws ApiException {
        final long currentTimeMs = time.milliseconds();
        final List<CoordinatorRecord> records = new ArrayList<>();

        // Get or create the share group.
        boolean createIfNotExists = memberEpoch == 0;
        final ShareGroup group = getOrMaybeCreateShareGroup(groupId, createIfNotExists);
        throwIfShareGroupIsFull(group, memberId);

        // Get or create the member.
        ShareGroupMember member = getOrMaybeSubscribeShareGroupMember(
            group,
            memberId,
            memberEpoch,
            createIfNotExists
        );

        // 1. Create or update the member. If the member is new or has changed, a ShareGroupMemberMetadataValue
        // record is written to the __consumer_offsets partition to persist the change. If the subscriptions have
        // changed, the subscription metadata is updated and persisted by writing a ShareGroupPartitionMetadataValue
        // record to the __consumer_offsets partition. Finally, the group epoch is bumped if the subscriptions have
        // changed, and persisted by writing a ShareGroupMetadataValue record to the partition.
        ShareGroupMember updatedMember = new ShareGroupMember.Builder(member)
            .maybeUpdateRackId(Optional.ofNullable(rackId))
            .maybeUpdateSubscribedTopicNames(Optional.ofNullable(subscribedTopicNames))
            .setClientId(clientId)
            .setClientHost(clientHost)
            .build();

        boolean bumpGroupEpoch = hasMemberSubscriptionChanged(
            groupId,
            member,
            updatedMember,
            records
        ) || initializedAssignmentPending(group);

        int groupEpoch = group.groupEpoch();
        Map<String, SubscriptionCount> subscribedTopicNamesMap = group.subscribedTopicNames();
        SubscriptionType subscriptionType = group.subscriptionType();

        if (bumpGroupEpoch || group.hasMetadataExpired(currentTimeMs)) {
            // The subscription metadata is updated in two cases:
            // 1) The member has updated its subscriptions;
            // 2) The refresh deadline has been reached.
            subscribedTopicNamesMap = group.computeSubscribedTopicNames(member, updatedMember);
            long groupMetadataHash = ModernGroup.computeMetadataHash(
                subscribedTopicNamesMap,
                topicHashCache,
                metadataImage
            );

            int numMembers = group.numMembers();
            if (!group.hasMember(updatedMember.memberId())) {
                numMembers++;
            }

            subscriptionType = ModernGroup.subscriptionType(
                subscribedTopicNamesMap,
                numMembers
            );

            if (groupMetadataHash != group.metadataHash()) {
                log.info("[GroupId {}] Computed new metadata hash: {}.",
                    groupId, groupMetadataHash);
                bumpGroupEpoch = true;
            }

            if (bumpGroupEpoch) {
                groupEpoch += 1;
                records.add(newShareGroupEpochRecord(groupId, groupEpoch, groupMetadataHash));
                log.info("[GroupId {}] Bumped group epoch to {} with metadata hash {}.", groupId, groupEpoch, groupMetadataHash);
                metrics.record(SHARE_GROUP_REBALANCES_SENSOR_NAME);
            }

            group.setMetadataRefreshDeadline(currentTimeMs + METADATA_REFRESH_INTERVAL_MS, groupEpoch);
        }

        // 2. Update the target assignment if the group epoch is larger than the target assignment epoch. The delta between
        // the existing and the new target assignment is persisted to the partition.
        final int targetAssignmentEpoch;
        final Assignment targetAssignment;

        if (groupEpoch > group.assignmentEpoch()) {
            targetAssignment = updateTargetAssignment(
                group,
                groupEpoch,
                updatedMember,
                subscriptionType,
                records
            );
            targetAssignmentEpoch = groupEpoch;
        } else {
            targetAssignmentEpoch = group.assignmentEpoch();
            targetAssignment = group.targetAssignment(updatedMember.memberId());
        }

        // 3. Reconcile the member's assignment with the target assignment if the member is not
        // fully reconciled yet.
        updatedMember = maybeReconcile(
            groupId,
            updatedMember,
            targetAssignmentEpoch,
            targetAssignment,
            // Force consistency with the subscription when the subscription has changed.
            bumpGroupEpoch,
            records
        );

        scheduleShareGroupSessionTimeout(groupId, memberId);

        // Prepare the response.
        ShareGroupHeartbeatResponseData response = new ShareGroupHeartbeatResponseData()
            .setMemberId(updatedMember.memberId())
            .setMemberEpoch(updatedMember.memberEpoch())
            .setHeartbeatIntervalMs(shareGroupHeartbeatIntervalMs(groupId));

        // The assignment is only provided in the following cases:
        // 1. The member sent a full request. It does so when joining or rejoining the group with zero
        //    as the member epoch; or on any errors (e.g. timeout). We use all the non-optional fields
        //    (subscribedTopicNames) to detect a full request as those must be set in a full request.
        // 2. The member's assignment has been updated.
        boolean isFullRequest = subscribedTopicNames != null;
        if (memberEpoch == 0 || isFullRequest || hasAssignedPartitionsChanged(member, updatedMember)) {
            response.setAssignment(ShareGroupHeartbeatResponse.createAssignment(updatedMember.assignedPartitions()));
        }
        return new CoordinatorResult<>(
            records,
            Map.entry(
                response,
                maybeCreateInitializeShareGroupStateRequest(groupId, groupEpoch, subscribedTopicNamesMap.keySet(), records)
            )
        );
    }

    // Visibility for testing
    boolean initializedAssignmentPending(ShareGroup group) {
        if (group.isEmpty()) {
            // No members then no point in computing assignment.
            return false;
        }

        String groupId = group.groupId();

        if (!shareGroupStatePartitionMetadata.containsKey(groupId) ||
            shareGroupStatePartitionMetadata.get(groupId).initializedTopics().isEmpty()) {
            // No initialized share partitions for the group so nothing can be assigned.
            return false;
        }

        Set<String> subscribedTopicNames = group.subscribedTopicNames().keySet();
        // No subscription then no need to compute assignment.
        if (subscribedTopicNames.isEmpty()) {
            return false;
        }

        Map<Uuid, Set<Integer>> currentAssigned = new HashMap<>();
        for (Assignment assignment : group.targetAssignment().values()) {
            for (Map.Entry<Uuid, Set<Integer>> tps : assignment.partitions().entrySet()) {
                currentAssigned.computeIfAbsent(tps.getKey(), k -> new HashSet<>())
                    .addAll(tps.getValue());
            }
        }

        for (Map.Entry<Uuid, InitMapValue> entry : shareGroupStatePartitionMetadata.get(groupId).initializedTopics().entrySet()) {
            if (subscribedTopicNames.contains(entry.getValue().name())) {
                // This topic is currently subscribed, so investigate further.
                Set<Integer> currentAssignedPartitions = currentAssigned.get(entry.getKey());
                if (currentAssignedPartitions != null && currentAssignedPartitions.equals(entry.getValue().partitions())) {
                    // The assigned and initialized partitions match, so assignment does not need to be recomputed.
                    continue;
                }
                // The assigned and initialized partitions do not match, OR
                // this topic is not currently assigned, so recompute the assignment.
                return true;
            }
        }
        return false;
    }

    /**
     * Computes the diff between the subscribed metadata and the initialized share topic
     * partitions corresponding to a share group.
     *
     * @param groupId                 The share group id for which diff is being calculated
     * @param subscriptionTopicNames  The subscription topic names to the share group.
     * @return  A map of topic partitions which are subscribed by the share group but not initialized yet.
     */
    // Visibility for testing
    Map<Uuid, InitMapValue> subscribedTopicsChangeMap(String groupId, Set<String> subscriptionTopicNames) {
        if (subscriptionTopicNames == null || subscriptionTopicNames.isEmpty()) {
            return Map.of();
        }

        Map<Uuid, InitMapValue> topicPartitionChangeMap = new HashMap<>();
        ShareGroupStatePartitionMetadataInfo info = shareGroupStatePartitionMetadata.get(groupId);

        // We must only consider initializing topics whose timestamp is fresher than delta elapsed.
        // Any initializing topics which are older than delta and are part of the subscribed topics
        // must be returned so that they can be retried.
        long curTimestamp = time.milliseconds();
        long delta = config.shareGroupInitializeRetryIntervalMs();
        Map<Uuid, InitMapValue> alreadyInitialized = info == null ? new HashMap<>() :
            combineInitMaps(
                info.initializedTopics(),
                info.initializingTopics().entrySet().stream()
                    .filter(entry -> curTimestamp - entry.getValue().timestamp() < delta)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
            );
        // Here will add any topics which are subscribed but not initialized and initializing
        // topics whose timestamp indicates that they are older than delta elapsed.
        subscriptionTopicNames.forEach(topicName -> {
            metadataImage.topicMetadata(topicName).ifPresent(topicMetadata -> {
                Set<Integer> alreadyInitializedPartSet = alreadyInitialized.containsKey(topicMetadata.id()) ? alreadyInitialized.get(topicMetadata.id()).partitions() : Set.of();
                if (alreadyInitializedPartSet.isEmpty() || alreadyInitializedPartSet.size() < topicMetadata.partitionCount()) {
                    // alreadyInitialized contains all initialized topics and initializing topics which are less than delta old
                    // which means we are putting subscribed topics which are unseen or initializing for more than delta. But, we
                    // are also updating the timestamp here which means, old initializing will not be included repeatedly.
                    topicPartitionChangeMap.computeIfAbsent(topicMetadata.id(), k -> {
                        Set<Integer> partitionSet = IntStream.range(0, topicMetadata.partitionCount()).boxed().collect(Collectors.toCollection(HashSet::new));
                        partitionSet.removeAll(alreadyInitializedPartSet);
                        return new InitMapValue(topicMetadata.name(), partitionSet, curTimestamp);
                    });
                }
            });
        });
        return topicPartitionChangeMap;
    }

    /**
     * Based on the diff between the subscribed topic partitions and the initialized topic partitions,
     * created initialize request for the non-initialized ones.
     *
     * @param groupId                 The share group id for which partitions need to be initialized.
     * @param groupEpoch              The group epoch of the share group.
     * @param subscriptionTopicNames  The subscription topic names for the share group.
     * @return An optional representing the persister initialize request.
     */
    private Optional<InitializeShareGroupStateParameters> maybeCreateInitializeShareGroupStateRequest(
        String groupId,
        int groupEpoch,
        Set<String> subscriptionTopicNames,
        List<CoordinatorRecord> records
    ) {
        if (subscriptionTopicNames == null || subscriptionTopicNames.isEmpty() || metadataImage.isEmpty()) {
            return Optional.empty();
        }

        Map<Uuid, InitMapValue> topicPartitionChangeMap = subscribedTopicsChangeMap(groupId, subscriptionTopicNames);

        // Nothing to initialize.
        if (topicPartitionChangeMap.isEmpty()) {
            return Optional.empty();
        }

        addInitializingTopicsRecords(groupId, records, topicPartitionChangeMap);
        return Optional.of(buildInitializeShareGroupStateRequest(groupId, groupEpoch, topicPartitionChangeMap));
    }

    private InitializeShareGroupStateParameters buildInitializeShareGroupStateRequest(String groupId, int groupEpoch, Map<Uuid, InitMapValue> topicPartitions) {
        return new InitializeShareGroupStateParameters.Builder().setGroupTopicPartitionData(
            new GroupTopicPartitionData<>(groupId, topicPartitions.entrySet().stream()
                .map(entry -> new TopicData<>(
                    entry.getKey(),
                    entry.getValue().partitions().stream()
                        .map(partitionId -> PartitionFactory.newPartitionStateData(partitionId, groupEpoch, -1))
                        .toList())
                ).toList()
            )).build();
    }

    private InitializeShareGroupStateParameters buildInitializeShareGroupState(String groupId, int groupEpoch, Map<Uuid, Map<Integer, Long>> offsetByTopicPartitions) {
        return new InitializeShareGroupStateParameters.Builder().setGroupTopicPartitionData(
            new GroupTopicPartitionData<>(groupId, offsetByTopicPartitions.entrySet().stream()
                .map(entry -> new TopicData<>(
                    entry.getKey(),
                    entry.getValue().entrySet().stream()
                        .map(partitionEntry -> PartitionFactory.newPartitionStateData(partitionEntry.getKey(), groupEpoch, partitionEntry.getValue()))
                        .toList())
                ).toList()
            )).build();
    }

    // Visibility for tests
    void addInitializingTopicsRecords(String groupId, List<CoordinatorRecord> records, Map<Uuid, InitMapValue> topicPartitionMap) {
        if (topicPartitionMap == null || topicPartitionMap.isEmpty()) {
            return;
        }

        ShareGroupStatePartitionMetadataInfo currentMap = shareGroupStatePartitionMetadata.get(groupId);
        if (currentMap == null) {
            records.add(newShareGroupStatePartitionMetadataRecord(groupId, topicPartitionMap, Map.of(), Map.of()));
            return;
        }

        // We must combine the existing information in the record with the topicPartitionMap argument.
        Map<Uuid, InitMapValue> finalInitializingMap = combineInitMaps(currentMap.initializingTopics(), topicPartitionMap);

        // If any initializing topics are also present in the deleting state
        // we should remove them from deleting.
        Set<Uuid> currentDeleting = new HashSet<>(currentMap.deletingTopics());
        if (!currentDeleting.isEmpty()) {
            finalInitializingMap.keySet().forEach(key -> {
                if (currentDeleting.remove(key)) {
                    String topicName = metadataImage.topicMetadata(key).map(CoordinatorMetadataImage.TopicMetadata::name).orElse(null);
                    log.warn("Initializing topic {} for share group {} found in deleting state as well, removing from deleting.", topicName, groupId);
                }
            });
        }

        records.add(
            newShareGroupStatePartitionMetadataRecord(
                groupId,
                finalInitializingMap,
                currentMap.initializedTopics(),
                attachTopicName(currentDeleting)
            )
        );
    }

    // Visibility for testing
    static Map<Uuid, InitMapValue> combineInitMaps(
        Map<Uuid, InitMapValue> initialized,
        Map<Uuid, InitMapValue> initializing
    ) {
        Map<Uuid, InitMapValue> finalInitMap = new HashMap<>();
        Set<Uuid> combinedTopicIdSet = new HashSet<>(initialized.keySet());

        Set<Uuid> initializingSet = initializing.keySet();

        combinedTopicIdSet.addAll(initializingSet);

        for (Uuid topicId : combinedTopicIdSet) {
            Set<Integer> initializedPartitions = initialized.containsKey(topicId) ? initialized.get(topicId).partitions() : new HashSet<>();
            long timestamp = initialized.containsKey(topicId) ? initialized.get(topicId).timestamp() : -1;
            String name = initialized.containsKey(topicId) ? initialized.get(topicId).name() : "UNKNOWN";

            Set<Integer> finalPartitions = new HashSet<>(initializedPartitions);
            if (initializingSet.contains(topicId)) {
                finalPartitions.addAll(initializing.get(topicId).partitions());
                timestamp = initializing.get(topicId).timestamp();
                name = initializing.get(topicId).name();
            }
            finalInitMap.putIfAbsent(topicId, new InitMapValue(name, finalPartitions, timestamp));
        }

        return finalInitMap;
    }

    static Map<Uuid, Set<Integer>> stripInitValue(
        Map<Uuid, InitMapValue> initMap
    ) {
        return initMap.entrySet().stream()
            .map(entry -> Map.entry(entry.getKey(), entry.getValue().partitions()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * Gets or subscribes a new dynamic consumer group member.
     *
     * @param group                 The consumer group.
     * @param memberId              The member id.
     * @param memberEpoch           The member epoch.
     * @param ownedTopicPartitions  The owned partitions reported by the member.
     * @param createIfNotExists     Whether the member should be created or not.
     * @param useClassicProtocol    Whether the member uses the classic protocol.
     *
     * @return The existing consumer group member or a new one.
     */
    private ConsumerGroupMember getOrMaybeSubscribeDynamicConsumerGroupMember(
        ConsumerGroup group,
        String memberId,
        int memberEpoch,
        List<ConsumerGroupHeartbeatRequestData.TopicPartitions> ownedTopicPartitions,
        boolean createIfNotExists,
        boolean useClassicProtocol
    ) {
        ConsumerGroupMember member = group.getOrMaybeCreateMember(memberId, createIfNotExists);
        if (!useClassicProtocol) {
            throwIfConsumerGroupMemberEpochIsInvalid(member, memberEpoch, ownedTopicPartitions);
        }
        if (createIfNotExists) {
            log.info("[GroupId {}] Member {} joins the consumer group using the {} protocol.",
                group.groupId(), memberId, useClassicProtocol ? "classic" : "consumer");
        }
        return member;
    }

    /**
     * Gets or creates a new dynamic streams group member.
     *
     * @param group                 The streams group.
     * @param memberId              The member id.
     * @param memberEpoch           The member epoch.
     * @param ownedActiveTasks      The owned active tasks reported by the member.
     * @param ownedStandbyTasks     The owned standby tasks reported by the member.
     * @param ownedWarmupTasks      The owned warmup tasks reported by the member.
     * @param memberIsJoining     Whether the member should be created or not.
     *
     * @return The existing streams group member or a new one.
     */
    private StreamsGroupMember getOrMaybeCreateDynamicStreamsGroupMember(
        StreamsGroup group,
        String memberId,
        int memberEpoch,
        List<StreamsGroupHeartbeatRequestData.TaskIds> ownedActiveTasks,
        List<StreamsGroupHeartbeatRequestData.TaskIds> ownedStandbyTasks,
        List<StreamsGroupHeartbeatRequestData.TaskIds> ownedWarmupTasks,
        boolean memberIsJoining
    ) {
        StreamsGroupMember member = memberIsJoining ? group.getOrCreateDefaultMember(memberId) : group.getMemberOrThrow(memberId);
        throwIfStreamsGroupMemberEpochIsInvalid(member, memberEpoch, ownedActiveTasks, ownedStandbyTasks, ownedWarmupTasks);
        if (memberIsJoining) {
            log.info("[GroupId {}][MemberId {}] Member joins the streams group.", group.groupId(), memberId);
        }
        return member;
    }

    /**
     * Gets or subscribes a static consumer group member. This method also replaces the
     * previous static member if allowed.
     *
     * @param group                 The consumer group.
     * @param memberId              The member id.
     * @param memberEpoch           The member epoch.
     * @param instanceId            The instance id.
     * @param ownedTopicPartitions  The owned partitions reported by the member.
     * @param createIfNotExists     Whether the member should be created or not.
     * @param useClassicProtocol    Whether the member uses the classic protocol.
     * @param records               The list to accumulate records created to replace
     *                              the previous static member.
     *                              
     * @return The existing consumer group member or a new one.
     */
    private ConsumerGroupMember getOrMaybeSubscribeStaticConsumerGroupMember(
        ConsumerGroup group,
        String memberId,
        int memberEpoch,
        String instanceId,
        List<ConsumerGroupHeartbeatRequestData.TopicPartitions> ownedTopicPartitions,
        boolean createIfNotExists,
        boolean useClassicProtocol,
        List<CoordinatorRecord> records
    ) {
        ConsumerGroupMember existingStaticMemberOrNull = group.staticMember(instanceId);

        if (createIfNotExists) {
            // A new static member joins or the existing static member rejoins.
            if (existingStaticMemberOrNull == null) {
                // New static member.
                ConsumerGroupMember newMember = group.getOrMaybeCreateMember(memberId, true);
                log.info("[GroupId {}] Static member {} with instance id {} joins the consumer group using the {} protocol.",
                    group.groupId(), memberId, instanceId, useClassicProtocol ? "classic" : "consumer");
                return newMember;
            } else {
                if (!useClassicProtocol && !existingStaticMemberOrNull.useClassicProtocol()) {
                    // If both the rejoining static member and the existing static member use the consumer
                    // protocol, replace the previous instance iff the previous member had sent a leave group.
                    throwIfInstanceIdIsUnreleased(existingStaticMemberOrNull, group.groupId(), memberId, instanceId);
                }

                // Copy the member but with its new member id.
                ConsumerGroupMember newMember = new ConsumerGroupMember.Builder(existingStaticMemberOrNull, memberId)
                    .setMemberEpoch(0)
                    .setPreviousMemberEpoch(0)
                    .build();

                // Generate the records to replace the member. We don't care about the regular expression
                // here because it is taken care of later after the static membership replacement.
                replaceMember(records, group, existingStaticMemberOrNull, newMember);

                log.info("[GroupId {}] Static member with instance id {} re-joins the consumer group " +
                    "using the {} protocol. Created a new member {} to replace the existing member {}.",
                    group.groupId(), instanceId, useClassicProtocol ? "classic" : "consumer", memberId, existingStaticMemberOrNull.memberId());

                return newMember;
            }
        } else {
            throwIfStaticMemberIsUnknown(existingStaticMemberOrNull, instanceId);
            throwIfInstanceIdIsFenced(existingStaticMemberOrNull, group.groupId(), memberId, instanceId);
            if (!useClassicProtocol) {
                throwIfConsumerGroupMemberEpochIsInvalid(existingStaticMemberOrNull, memberEpoch, ownedTopicPartitions);
            }
            return existingStaticMemberOrNull;
        }
    }

    /**
     * Gets or subscribes a new dynamic share group member.
     *
     * @param group                 The share group.
     * @param memberId              The member id.
     * @param memberEpoch           The member epoch.
     * @param createIfNotExists     Whether the member should be created or not.
     *
     * @return The existing share group member or a new one.
     */
    private ShareGroupMember getOrMaybeSubscribeShareGroupMember(
        ShareGroup group,
        String memberId,
        int memberEpoch,
        boolean createIfNotExists
    ) {
        ShareGroupMember member = group.getOrMaybeCreateMember(memberId, createIfNotExists);
        throwIfShareGroupMemberEpochIsInvalid(member, memberEpoch);
        if (createIfNotExists) {
            log.info("[GroupId {}] Member {} joins the share group using the share protocol.",
                group.groupId(), memberId);
        }
        return member;
    }

    /**
     * Creates the member subscription record if the updatedMember is different from
     * the old member. Returns true if the subscribedTopicNames has changed.
     *
     * @param groupId       The group id.
     * @param member        The old member.
     * @param updatedMember The updated member.
     * @param records       The list to accumulate any new records.
     * @return A boolean indicating whether the updatedMember has a different
     *         subscribedTopicNames from the old member.
     */
    private boolean hasMemberSubscriptionChanged(
        String groupId,
        ConsumerGroupMember member,
        ConsumerGroupMember updatedMember,
        List<CoordinatorRecord> records
    ) {
        String memberId = updatedMember.memberId();
        if (!updatedMember.equals(member)) {
            records.add(newConsumerGroupMemberSubscriptionRecord(groupId, updatedMember));
            if (!updatedMember.subscribedTopicNames().equals(member.subscribedTopicNames())) {
                log.debug("[GroupId {}] Member {} updated its subscribed topics to: {}.",
                    groupId, memberId, updatedMember.subscribedTopicNames());
                return true;
            }
        }
        return false;
    }

    private static boolean isNotEmpty(String value) {
        return value != null && !value.isEmpty();
    }

    private enum UpdateRegularExpressionsResult {
        NO_CHANGE,
        REGEX_UPDATED,
        REGEX_UPDATED_AND_RESOLVED;

        public boolean regexUpdated() {
            return this == REGEX_UPDATED || this == REGEX_UPDATED_AND_RESOLVED;
        }
    }

    /**
     * Check whether the member has updated its subscribed topic regular expression and
     * may trigger the resolution/the refresh of all the regular expressions in the
     * group. We align the refreshment of the regular expression in order to have
     * them trigger only one rebalance per update.
     *
     * @param context       The request context.
     * @param group         The consumer group.
     * @param member        The old member.
     * @param updatedMember The new member.
     * @param records       The records accumulator.
     * @return The result of the update.
     */
    private UpdateRegularExpressionsResult maybeUpdateRegularExpressions(
        AuthorizableRequestContext context,
        ConsumerGroup group,
        ConsumerGroupMember member,
        ConsumerGroupMember updatedMember,
        List<CoordinatorRecord> records
    ) {
        final long currentTimeMs = time.milliseconds();
        String groupId = group.groupId();
        String memberId = updatedMember.memberId();
        String oldSubscribedTopicRegex = member.subscribedTopicRegex();
        String newSubscribedTopicRegex = updatedMember.subscribedTopicRegex();

        boolean requireRefresh = false;
        UpdateRegularExpressionsResult updateRegularExpressionsResult = UpdateRegularExpressionsResult.NO_CHANGE;

        // Check whether the member has changed its subscribed regex.
        boolean subscribedTopicRegexChanged = !Objects.equals(oldSubscribedTopicRegex, newSubscribedTopicRegex);
        if (subscribedTopicRegexChanged) {
            log.debug("[GroupId {}] Member {} updated its subscribed regex to: {}.",
                groupId, memberId, newSubscribedTopicRegex);

            updateRegularExpressionsResult = UpdateRegularExpressionsResult.REGEX_UPDATED;

            if (isNotEmpty(oldSubscribedTopicRegex) && group.numSubscribedMembers(oldSubscribedTopicRegex) == 1) {
                // If the member was the last one subscribed to the regex, we delete the
                // resolved regular expression.
                records.add(newConsumerGroupRegularExpressionTombstone(
                    groupId,
                    oldSubscribedTopicRegex
                ));
            }

            if (isNotEmpty(newSubscribedTopicRegex)) {
                if (group.numSubscribedMembers(newSubscribedTopicRegex) == 0) {
                    // If the member subscribed to a new regex, we compile it to ensure its validity.
                    // We also trigger a refresh of the regexes in order to resolve it.
                    throwIfRegularExpressionIsInvalid(updatedMember.subscribedTopicRegex());
                    requireRefresh = true;
                } else {
                    // If the new regex is already resolved, we trigger a rebalance
                    // by bumping the group epoch.
                    if (group.resolvedRegularExpression(newSubscribedTopicRegex).isPresent()) {
                        updateRegularExpressionsResult = UpdateRegularExpressionsResult.REGEX_UPDATED_AND_RESOLVED;
                    }
                }
            }
        }

        // Conditions to trigger a refresh:
        // 0.   The group is subscribed to regular expressions.
        // 1.   There is no ongoing refresh for the group.
        // 2.   The last refresh is older than 10s.
        // 3.1  The group has unresolved regular expressions.
        // 3.2  Or the metadata image has new topics.
        // 3.3  Or the last refresh is older than the batch refresh max interval.

        // 0. The group is subscribed to regular expressions. We also take the one
        //    that the current may have just introduced.
        if (!requireRefresh && group.subscribedRegularExpressions().isEmpty()) {
            return updateRegularExpressionsResult;
        }

        // 1. There is no ongoing refresh for the group.
        String key = group.groupId() + "-regex";
        if (executor.isScheduled(key)) {
            return updateRegularExpressionsResult;
        }

        // 2. The last refresh is older than 10s. If the group does not have any regular
        //    expressions but the current member just brought a new one, we should continue.
        long lastRefreshTimeMs = group.lastResolvedRegularExpressionRefreshTimeMs();
        if (currentTimeMs <= lastRefreshTimeMs + REGEX_BATCH_REFRESH_MIN_INTERVAL_MS) {
            return updateRegularExpressionsResult;
        }

        // 3.1 The group has unresolved regular expressions.
        Map<String, Integer> subscribedRegularExpressions = new HashMap<>(group.subscribedRegularExpressions());
        if (isNotEmpty(oldSubscribedTopicRegex)) {
            subscribedRegularExpressions.compute(oldSubscribedTopicRegex, Utils::decValue);
        }
        if (isNotEmpty(newSubscribedTopicRegex)) {
            subscribedRegularExpressions.compute(newSubscribedTopicRegex, Utils::incValue);
        }

        requireRefresh |= subscribedRegularExpressions.size() != group.numResolvedRegularExpressions();

        // 3.2 The metadata has new topics that we must consider.
        requireRefresh |= group.lastResolvedRegularExpressionVersion() < lastMetadataImageWithNewTopics;

        // 3.3 The last refresh is older than the batch refresh max interval.
        requireRefresh |= currentTimeMs > lastRefreshTimeMs + config.consumerGroupRegexRefreshIntervalMs();

        if (requireRefresh && !subscribedRegularExpressions.isEmpty()) {
            Set<String> regexes = Collections.unmodifiableSet(subscribedRegularExpressions.keySet());
            executor.schedule(
                key,
                () -> refreshRegularExpressions(context, groupId, log, time, metadataImage, authorizerPlugin, regexes),
                (result, exception) -> handleRegularExpressionsResult(groupId, memberId, result, exception)
            );
        }

        return updateRegularExpressionsResult;
    }

    /**
     * Resolves the provided regular expressions. Note that this static method is executed
     * as an asynchronous task in the executor. Hence, it should not access any state from
     * the manager.
     *
     * @param context       The request context.
     * @param groupId       The group id.
     * @param log           The log instance.
     * @param time          The time instance.
     * @param image         The metadata image to use for listing the topics.
     * @param authorizerPlugin    The authorizer.
     * @param regexes       The list of regular expressions that must be resolved.
     * @return The list of resolved regular expressions.
     *
     * public for benchmarks.
     */
    public static Map<String, ResolvedRegularExpression> refreshRegularExpressions(
        AuthorizableRequestContext context,
        String groupId,
        Logger log,
        Time time,
        CoordinatorMetadataImage image,
        Optional<Plugin<Authorizer>> authorizerPlugin,
        Set<String> regexes
    ) {
        long startTimeMs = time.milliseconds();
        log.debug("[GroupId {}] Refreshing regular expressions: {}", groupId, regexes);

        Map<String, Set<String>> resolvedRegexes = new HashMap<>(regexes.size());
        List<Pattern> compiledRegexes = new ArrayList<>(regexes.size());
        for (String regex : regexes) {
            resolvedRegexes.put(regex, new HashSet<>());
            try {
                compiledRegexes.add(Pattern.compile(regex));
            } catch (PatternSyntaxException ex) {
                // This should not happen because the regular expressions are validated
                // when received from the members. If for some reason, it would
                // happen, we log it and ignore it.
                log.error("[GroupId {}] Couldn't parse regular expression '{}' due to `{}`. Ignoring it.",
                    groupId, regex, ex.getDescription());
            }
        }

        for (String topicName : image.topicNames()) {
            for (Pattern regex : compiledRegexes) {
                if (regex.matcher(topicName).matches()) {
                    resolvedRegexes.get(regex.pattern()).add(topicName);
                }
            }
        }

        filterTopicDescribeAuthorizedTopics(
            context,
            authorizerPlugin,
            resolvedRegexes
        );

        long version = image.version();
        Map<String, ResolvedRegularExpression> result = new HashMap<>(resolvedRegexes.size());
        for (Map.Entry<String, Set<String>> resolvedRegex : resolvedRegexes.entrySet()) {
            result.put(
                resolvedRegex.getKey(),
                new ResolvedRegularExpression(resolvedRegex.getValue(), version, startTimeMs)
            );
        }

        log.info("[GroupId {}] Scanned {} topics to refresh regular expressions {} in {}ms.",
            groupId, image.topicNames().size(), resolvedRegexes.keySet(),
            time.milliseconds() - startTimeMs);

        return result;
    }

    /**
     * This method filters the topics in the resolved regexes
     * that the member is authorized to describe.
     *
     * @param context           The request context.
     * @param authorizerPlugin  The authorizer.
     * @param resolvedRegexes   The map of the regex pattern and its set of matched topics.
     */
    private static void filterTopicDescribeAuthorizedTopics(
        AuthorizableRequestContext context,
        Optional<Plugin<Authorizer>> authorizerPlugin,
        Map<String, Set<String>> resolvedRegexes
    ) {
        if (authorizerPlugin.isEmpty()) return;

        Map<String, Integer> topicNameCount = new HashMap<>();
        resolvedRegexes.values().forEach(topicNames ->
            topicNames.forEach(topicName ->
                topicNameCount.compute(topicName, Utils::incValue)
            )
        );

        List<Action> actions = topicNameCount.entrySet().stream().map(entry -> {
            ResourcePattern resource = new ResourcePattern(TOPIC, entry.getKey(), LITERAL);
            return new Action(DESCRIBE, resource, entry.getValue(), true, false);
        }).collect(Collectors.toList());

        List<AuthorizationResult> authorizationResults = authorizerPlugin.get().get().authorize(context, actions);
        Set<String> deniedTopics = new HashSet<>();
        IntStream.range(0, actions.size()).forEach(i -> {
            if (authorizationResults.get(i) == AuthorizationResult.DENIED) {
                String deniedTopic = actions.get(i).resourcePattern().name();
                deniedTopics.add(deniedTopic);
            }
        });

        resolvedRegexes.forEach((__, topicNames) -> topicNames.removeAll(deniedTopics));
    }

    /**
     * Handle the result of the asynchronous tasks which resolves the regular expressions.
     *
     * @param groupId                       The group id.
     * @param memberId                      The member id.
     * @param resolvedRegularExpressions    The resolved regular expressions.
     * @param exception                     The exception if the resolution failed.
     * @return A CoordinatorResult containing the records to mutate the group state.
     */
    private CoordinatorResult<Void, CoordinatorRecord> handleRegularExpressionsResult(
        String groupId,
        String memberId,
        Map<String, ResolvedRegularExpression> resolvedRegularExpressions,
        Throwable exception
    ) {
        if (exception != null) {
            log.error("[GroupId {}] Couldn't update regular expression due to: {}",
                groupId, exception.getMessage(), exception);
            return new CoordinatorResult<>(List.of());
        }

        if (log.isDebugEnabled()) {
            log.debug("[GroupId {}] Received updated regular expressions based on the context of member {}: {}.",
                groupId, memberId, resolvedRegularExpressions);
        }

        List<CoordinatorRecord> records = new ArrayList<>();
        try {
            ConsumerGroup group = consumerGroup(groupId);
            Map<String, SubscriptionCount> subscribedTopicNames = new HashMap<>(group.subscribedTopicNames());

            boolean bumpGroupEpoch = false;
            for (Map.Entry<String, ResolvedRegularExpression> entry : resolvedRegularExpressions.entrySet()) {
                String regex = entry.getKey();

                // We can skip the regex if the group is no longer
                // subscribed to it.
                if (group.numSubscribedMembers(regex) == 0) continue;

                ResolvedRegularExpression newResolvedRegularExpression = entry.getValue();
                ResolvedRegularExpression oldResolvedRegularExpression = group
                    .resolvedRegularExpression(regex)
                    .orElse(ResolvedRegularExpression.EMPTY);

                if (!oldResolvedRegularExpression.topics().equals(newResolvedRegularExpression.topics())) {
                    bumpGroupEpoch = true;

                    oldResolvedRegularExpression.topics().forEach(topicName ->
                        subscribedTopicNames.compute(topicName, SubscriptionCount::decRegexCount)
                    );

                    newResolvedRegularExpression.topics().forEach(topicName ->
                        subscribedTopicNames.compute(topicName, SubscriptionCount::incRegexCount)
                    );
                }

                // Add the record to persist the change.
                records.add(GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionRecord(
                    groupId,
                    regex,
                    newResolvedRegularExpression
                ));
            }

            long groupMetadataHash = ModernGroup.computeMetadataHash(
                subscribedTopicNames,
                topicHashCache,
                metadataImage
            );

            if (groupMetadataHash != group.metadataHash()) {
                if (log.isDebugEnabled()) {
                    log.debug("[GroupId {}] Computed new metadata hash: {}.",
                        groupId, groupMetadataHash);
                }
                bumpGroupEpoch = true;
            }

            if (bumpGroupEpoch) {
                int groupEpoch = group.groupEpoch() + 1;
                records.add(newConsumerGroupEpochRecord(groupId, groupEpoch, groupMetadataHash));
                log.info("[GroupId {}] Bumped group epoch to {} with metadata hash {}.", groupId, groupEpoch, groupMetadataHash);
                metrics.record(CONSUMER_GROUP_REBALANCES_SENSOR_NAME);
                group.setMetadataRefreshDeadline(
                    time.milliseconds() + METADATA_REFRESH_INTERVAL_MS,
                    groupEpoch
                );
            }
        } catch (GroupIdNotFoundException ex) {
            log.debug("[GroupId {}] Received result of regular expression resolution but " +
                "it no longer exists.", groupId);
        }

        return new CoordinatorResult<>(records);
    }

    /**
     * Creates the member subscription record if the updatedMember is different from
     * the old member. Returns true if the subscribedTopicNames has changed.
     *
     * @param groupId       The group id.
     * @param member        The old member.
     * @param updatedMember The updated member.
     * @param records       The list to accumulate any new records.
     * @return A boolean indicating whether the updatedMember has a different
     *         subscribedTopicNames from the old member.
     */
    private boolean hasMemberSubscriptionChanged(
        String groupId,
        ShareGroupMember member,
        ShareGroupMember updatedMember,
        List<CoordinatorRecord> records
    ) {
        String memberId = updatedMember.memberId();
        if (!updatedMember.equals(member)) {
            records.add(newShareGroupMemberSubscriptionRecord(groupId, updatedMember));

            if (!updatedMember.subscribedTopicNames().equals(member.subscribedTopicNames())) {
                log.info("[GroupId {}] Member {} updated its subscribed topics to: {}.",
                    groupId, memberId, updatedMember.subscribedTopicNames());
                return true;
            }
        }
        return false;
    }

    /**
     * Creates the member metadata record record if the updatedMember is different from
     * the old member. Returns true if the metadata has changed, which is always the case
     * when a member is first created.
     *
     * @param groupId       The group id.
     * @param member        The old member.
     * @param updatedMember The updated member.
     * @param records       The list to accumulate any new records.
     * @return A boolean indicating whether the group epoch should be bumped
     *         following this change
     */
    private boolean hasStreamsMemberMetadataChanged(
        String groupId,
        StreamsGroupMember member,
        StreamsGroupMember updatedMember,
        List<CoordinatorRecord> records
    ) {
        String memberId = updatedMember.memberId();
        if (!updatedMember.equals(member)) {
            records.add(newStreamsGroupMemberRecord(groupId, updatedMember));
            log.info("[GroupId {}] Member {} updated its member metdata to {}.",
                groupId, memberId, updatedMember);

            return true;
        }
        return false;
    }

    /**
    /**
     * Reconciles the current assignment of the member towards the target assignment if needed.
     *
     * @param groupId                    The group id.
     * @param member                     The member to reconcile.
     * @param currentPartitionEpoch      The function returning the current epoch of
     *                                   a given partition.
     * @param targetAssignmentEpoch      The target assignment epoch.
     * @param targetAssignment           The target assignment.
     * @param resolvedRegularExpressions The resolved regular expressions.
     * @param hasSubscriptionChanged     Whether the member has changed its subscription on the current heartbeat.
     * @param ownedTopicPartitions       The list of partitions owned by the member. This
     *                                   is reported in the ConsumerGroupHeartbeat API and
     *                                   it could be null if not provided.
     * @param records                    The list to accumulate any new records.
     * @return The received member if no changes have been made; or a new
     *         member containing the new assignment.
     */
    private ConsumerGroupMember maybeReconcile(
        String groupId,
        ConsumerGroupMember member,
        BiFunction<Uuid, Integer, Integer> currentPartitionEpoch,
        int targetAssignmentEpoch,
        Assignment targetAssignment,
        Map<String, ResolvedRegularExpression> resolvedRegularExpressions,
        boolean hasSubscriptionChanged,
        List<ConsumerGroupHeartbeatRequestData.TopicPartitions> ownedTopicPartitions,
        List<CoordinatorRecord> records
    ) {
        if (!hasSubscriptionChanged && member.isReconciledTo(targetAssignmentEpoch)) {
            return member;
        }

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withMetadataImage(metadataImage)
            .withTargetAssignment(targetAssignmentEpoch, targetAssignment)
            .withHasSubscriptionChanged(hasSubscriptionChanged)
            .withResolvedRegularExpressions(resolvedRegularExpressions)
            .withCurrentPartitionEpoch(currentPartitionEpoch)
            .withOwnedTopicPartitions(ownedTopicPartitions)
            .build();

        if (!updatedMember.equals(member)) {
            records.add(newConsumerGroupCurrentAssignmentRecord(groupId, updatedMember));

            if (log.isDebugEnabled()) {
                log.debug("[GroupId {}] Member {} new assignment state: epoch={}, previousEpoch={}, state={}, "
                        + "assignedPartitions={} and revokedPartitions={}.",
                    groupId, updatedMember.memberId(), updatedMember.memberEpoch(), updatedMember.previousMemberEpoch(), updatedMember.state(),
                    assignmentToString(updatedMember.assignedPartitions()), assignmentToString(updatedMember.partitionsPendingRevocation()));
            }

            // Schedule/cancel the rebalance timeout if the member uses the consumer protocol.
            // The members using classic protocol only have join timer and sync timer.
            if (!updatedMember.useClassicProtocol()) {
                if (updatedMember.state() == MemberState.UNREVOKED_PARTITIONS) {
                    scheduleConsumerGroupRebalanceTimeout(
                        groupId,
                        updatedMember.memberId(),
                        updatedMember.memberEpoch(),
                        updatedMember.rebalanceTimeoutMs()
                    );
                } else {
                    cancelGroupRebalanceTimeout(groupId, updatedMember.memberId());
                }
            }
        }

        return updatedMember;
    }

    /**
     * Reconciles the current assignment of the member towards the target assignment if needed.
     *
     * @param groupId                The group id.
     * @param member                 The member to reconcile.
     * @param targetAssignmentEpoch  The target assignment epoch.
     * @param targetAssignment       The target assignment.
     * @param hasSubscriptionChanged Whether the member has changed its subscription on the current heartbeat.
     * @param records                The list to accumulate any new records.
     * @return The received member if no changes have been made; or a new
     *         member containing the new assignment.
     */
    private ShareGroupMember maybeReconcile(
        String groupId,
        ShareGroupMember member,
        int targetAssignmentEpoch,
        Assignment targetAssignment,
        boolean hasSubscriptionChanged,
        List<CoordinatorRecord> records
    ) {
        if (!hasSubscriptionChanged && member.isReconciledTo(targetAssignmentEpoch)) {
            return member;
        }

        ShareGroupMember updatedMember = new ShareGroupAssignmentBuilder(member)
            .withMetadataImage(metadataImage)
            .withTargetAssignment(targetAssignmentEpoch, targetAssignment)
            .withHasSubscriptionChanged(hasSubscriptionChanged)
            .build();

        if (!updatedMember.equals(member)) {
            records.add(newShareGroupCurrentAssignmentRecord(groupId, updatedMember));

            log.info("[GroupId {}] Member {} new assignment state: epoch={}, previousEpoch={}, state={}, "
                    + "assignedPartitions={}.",
                groupId, updatedMember.memberId(), updatedMember.memberEpoch(), updatedMember.previousMemberEpoch(), updatedMember.state(),
                assignmentToString(updatedMember.assignedPartitions()));
        }

        return updatedMember;
    }

    /**
     * Reconciles the current assignment of the member towards the target assignment if needed.
     *
     * @param groupId               The group id.
     * @param member                The member to reconcile.
     * @param currentActiveTaskProcessId The function returning the current process ID of
     *                              a given active task.
     * @param currentStandbyTaskProcessIds The function returning the current process IDs of
     *                              a given standby task.
     * @param currentWarmupTaskProcessIds The function returning the current process IDs of
     *                              a given warmup task.
     * @param targetAssignmentEpoch The target assignment epoch.
     * @param targetAssignment      The target assignment.
     * @param ownedActiveTasks      The list of active tasks owned by the member.
     *                              This is reported in the StreamsGroupHeartbeat API, and
     *                              it could be null if not provided.
     * @param ownedStandbyTasks     The list of standby owned by the member.
     *                              This is reported in the StreamsGroupHeartbeat API, and
     *                              it could be null if not provided.
     * @param ownedWarmupTasks      The list of warmup tasks owned by the member.
     *                              This is reported in the StreamsGroupHeartbeat API, and
     *                              it could be null if not provided.
     * @param records               The list to accumulate any new records.
     * @return The received member if no changes have been made; or a new
     *         member containing the new assignment.
     */
    private StreamsGroupMember maybeReconcile(
        String groupId,
        StreamsGroupMember member,
        BiFunction<String, Integer, String> currentActiveTaskProcessId,
        BiFunction<String, Integer, Set<String>> currentStandbyTaskProcessIds,
        BiFunction<String, Integer, Set<String>> currentWarmupTaskProcessIds,
        int targetAssignmentEpoch,
        org.apache.kafka.coordinator.group.streams.TasksTuple targetAssignment,
        List<StreamsGroupHeartbeatRequestData.TaskIds> ownedActiveTasks,
        List<StreamsGroupHeartbeatRequestData.TaskIds> ownedStandbyTasks,
        List<StreamsGroupHeartbeatRequestData.TaskIds> ownedWarmupTasks,
        List<CoordinatorRecord> records
    ) {
        if (member.isReconciledTo(targetAssignmentEpoch)) {
            return member;
        }

        TasksTuple ownedTasks = null;
        if (ownedActiveTasks != null && ownedStandbyTasks != null && ownedWarmupTasks != null) {
            ownedTasks = TasksTuple.fromHeartbeatRequest(ownedActiveTasks, ownedStandbyTasks, ownedWarmupTasks);
        }

        StreamsGroupMember updatedMember = new org.apache.kafka.coordinator.group.streams.CurrentAssignmentBuilder(member)
            .withTargetAssignment(targetAssignmentEpoch, targetAssignment)
            .withCurrentActiveTaskProcessId(currentActiveTaskProcessId)
            .withCurrentStandbyTaskProcessIds(currentStandbyTaskProcessIds)
            .withCurrentWarmupTaskProcessIds(currentWarmupTaskProcessIds)
            .withOwnedAssignment(ownedTasks)
            .build();

        if (!updatedMember.equals(member)) {
            records.add(newStreamsGroupCurrentAssignmentRecord(groupId, updatedMember));

            log.info("[GroupId {}][MemberId {}] Member's new assignment state: epoch={}, previousEpoch={}, state={}, "
                    + "assignedTasks={} and tasksPendingRevocation={}.",
                groupId, updatedMember.memberId(), updatedMember.memberEpoch(), updatedMember.previousMemberEpoch(), updatedMember.state(),
                updatedMember.assignedTasks().toString(),
                updatedMember.tasksPendingRevocation().toString());

            // Schedule/cancel the rebalance timeout.
            if (updatedMember.state() == org.apache.kafka.coordinator.group.streams.MemberState.UNREVOKED_TASKS) {
                scheduleStreamsGroupRebalanceTimeout(
                    groupId,
                    updatedMember.memberId(),
                    updatedMember.memberEpoch(),
                    updatedMember.rebalanceTimeoutMs()
                );
            } else {
                cancelGroupRebalanceTimeout(groupId, updatedMember.memberId());
            }
        }

        return updatedMember;
    }

    /**
     * Updates the subscription metadata and bumps the group epoch if needed.
     *
     * @param group             The consumer group.
     * @param bumpGroupEpoch    Whether the group epoch must be bumped.
     * @param member            The old member.
     * @param updatedMember     The new member.
     * @param records           The record accumulator.
     * @return The result of the update.
     */
    private UpdateSubscriptionMetadataResult updateSubscriptionMetadata(
        ConsumerGroup group,
        boolean bumpGroupEpoch,
        ConsumerGroupMember member,
        ConsumerGroupMember updatedMember,
        List<CoordinatorRecord> records
    ) {
        final long currentTimeMs = time.milliseconds();
        final String groupId = group.groupId();
        int groupEpoch = group.groupEpoch();

        Map<String, Integer> subscribedRegularExpressions = group.computeSubscribedRegularExpressions(
            member,
            updatedMember
        );
        Map<String, SubscriptionCount> subscribedTopicNamesMap = group.computeSubscribedTopicNames(
            member,
            updatedMember
        );

        long groupMetadataHash = ModernGroup.computeMetadataHash(
            subscribedTopicNamesMap,
            topicHashCache,
            metadataImage
        );

        int numMembers = group.numMembers();
        if (!group.hasMember(updatedMember.memberId()) && !group.hasStaticMember(updatedMember.instanceId())) {
            numMembers++;
        }

        SubscriptionType subscriptionType = ConsumerGroup.subscriptionType(
            subscribedRegularExpressions,
            subscribedTopicNamesMap,
            numMembers
        );

        if (groupMetadataHash != group.metadataHash()) {
            if (log.isDebugEnabled()) {
                log.debug("[GroupId {}] Computed new metadata hash: {}.",
                    groupId, groupMetadataHash);
            }
            bumpGroupEpoch = true;
        }

        if (bumpGroupEpoch) {
            groupEpoch += 1;
            records.add(newConsumerGroupEpochRecord(groupId, groupEpoch, groupMetadataHash));
            log.info("[GroupId {}] Bumped group epoch to {} with metadata hash {}.", groupId, groupEpoch, groupMetadataHash);
            metrics.record(CONSUMER_GROUP_REBALANCES_SENSOR_NAME);
        }

        group.setMetadataRefreshDeadline(currentTimeMs + METADATA_REFRESH_INTERVAL_MS, groupEpoch);

        // Before 4.0, the coordinator used subscription metadata to keep topic metadata.
        // After 4.1, the subscription metadata is replaced by the metadata hash. If there is subscription metadata in log,
        // add a tombstone record to remove it.
        if (group.hasSubscriptionMetadataRecord()) {
            records.add(newConsumerGroupSubscriptionMetadataTombstoneRecord(groupId));
        }

        return new UpdateSubscriptionMetadataResult(
            groupEpoch,
            subscriptionType
        );
    }

    /**
     * Updates the target assignment according to the updated member and subscription metadata.
     *
     * @param group            The ConsumerGroup.
     * @param groupEpoch       The group epoch.
     * @param member           The existing member.
     * @param updatedMember    The updated member.
     * @param subscriptionType The group subscription type.
     * @param records          The list to accumulate any new records.
     * @return The new target assignment.
     */
    private Assignment updateTargetAssignment(
        ConsumerGroup group,
        int groupEpoch,
        ConsumerGroupMember member,
        ConsumerGroupMember updatedMember,
        SubscriptionType subscriptionType,
        List<CoordinatorRecord> records
    ) {
        String preferredServerAssignor = group.computePreferredServerAssignor(
            member,
            updatedMember
        ).orElse(defaultConsumerGroupAssignor.name());
        try {
            TargetAssignmentBuilder.ConsumerTargetAssignmentBuilder assignmentResultBuilder =
                new TargetAssignmentBuilder.ConsumerTargetAssignmentBuilder(group.groupId(), groupEpoch, consumerGroupAssignors.get(preferredServerAssignor))
                    .withMembers(group.members())
                    .withStaticMembers(group.staticMembers())
                    .withSubscriptionType(subscriptionType)
                    .withTargetAssignment(group.targetAssignment())
                    .withInvertedTargetAssignment(group.invertedTargetAssignment())
                    .withMetadataImage(metadataImage)
                    .withResolvedRegularExpressions(group.resolvedRegularExpressions())
                    .addOrUpdateMember(updatedMember.memberId(), updatedMember);

            // If the instance id was associated to a different member, it means that the
            // static member is replaced by the current member hence we remove the previous one.
            String previousMemberId = group.staticMemberId(updatedMember.instanceId());
            if (previousMemberId != null && !updatedMember.memberId().equals(previousMemberId)) {
                assignmentResultBuilder.removeMember(previousMemberId);
            }

            long startTimeMs = time.milliseconds();
            TargetAssignmentBuilder.TargetAssignmentResult assignmentResult =
                assignmentResultBuilder.build();
            long assignorTimeMs = time.milliseconds() - startTimeMs;

            if (log.isDebugEnabled()) {
                log.debug("[GroupId {}] Computed a new target assignment for epoch {} with '{}' assignor in {}ms: {}.",
                    group.groupId(), groupEpoch, preferredServerAssignor, assignorTimeMs, assignmentResult.targetAssignment());
            } else {
                log.info("[GroupId {}] Computed a new target assignment for epoch {} with '{}' assignor in {}ms.",
                    group.groupId(), groupEpoch, preferredServerAssignor, assignorTimeMs);
            }

            records.addAll(assignmentResult.records());

            MemberAssignment newMemberAssignment = assignmentResult.targetAssignment().get(updatedMember.memberId());
            if (newMemberAssignment != null) {
                return new Assignment(newMemberAssignment.partitions());
            } else {
                return Assignment.EMPTY;
            }
        } catch (PartitionAssignorException ex) {
            String msg = String.format("Failed to compute a new target assignment for epoch %d: %s",
                groupEpoch, ex.getMessage());
            log.error("[GroupId {}] {}.", group.groupId(), msg, ex);
            throw new UnknownServerException(msg, ex);
        }
    }

    /**
     * Updates the target assignment according to the updated member and subscription metadata.
     *
     * @param group            The ShareGroup.
     * @param groupEpoch       The group epoch.
     * @param updatedMember    The updated member.
     * @param subscriptionType The group subscription type.
     * @param records          The list to accumulate any new records.
     * @return The new target assignment.
     */
    private Assignment updateTargetAssignment(
        ShareGroup group,
        int groupEpoch,
        ShareGroupMember updatedMember,
        SubscriptionType subscriptionType,
        List<CoordinatorRecord> records
    ) {
        try {
            Map<Uuid, Set<Integer>> initializedTopicPartitions = shareGroupStatePartitionMetadata.containsKey(group.groupId()) ?
                stripInitValue(shareGroupStatePartitionMetadata.get(group.groupId()).initializedTopics()) :
                Map.of();

            TargetAssignmentBuilder.ShareTargetAssignmentBuilder assignmentResultBuilder =
                new TargetAssignmentBuilder.ShareTargetAssignmentBuilder(group.groupId(), groupEpoch, shareGroupAssignor)
                    .withMembers(group.members())
                    .withSubscriptionType(subscriptionType)
                    .withTargetAssignment(group.targetAssignment())
                    .withTopicAssignablePartitionsMap(initializedTopicPartitions)
                    .withInvertedTargetAssignment(group.invertedTargetAssignment())
                    .withMetadataImage(metadataImage)
                    .addOrUpdateMember(updatedMember.memberId(), updatedMember);

            long startTimeMs = time.milliseconds();
            TargetAssignmentBuilder.TargetAssignmentResult assignmentResult =
                assignmentResultBuilder.build();
            long assignorTimeMs = time.milliseconds() - startTimeMs;

            if (log.isDebugEnabled()) {
                log.debug("[GroupId {}] Computed a new target assignment for epoch {} with '{}' assignor in {}ms: {}.",
                    group.groupId(), groupEpoch, shareGroupAssignor, assignorTimeMs, assignmentResult.targetAssignment());
            } else {
                log.info("[GroupId {}] Computed a new target assignment for epoch {} with '{}' assignor in {}ms.",
                    group.groupId(), groupEpoch, shareGroupAssignor, assignorTimeMs);
            }

            records.addAll(assignmentResult.records());

            MemberAssignment newMemberAssignment = assignmentResult.targetAssignment().get(updatedMember.memberId());
            if (newMemberAssignment != null) {
                return new Assignment(newMemberAssignment.partitions());
            } else {
                return Assignment.EMPTY;
            }
        } catch (PartitionAssignorException ex) {
            String msg = String.format("Failed to compute a new target assignment for epoch %d: %s",
                groupEpoch, ex.getMessage());
            log.error("[GroupId {}] {}.", group.groupId(), msg, ex);
            throw new UnknownServerException(msg, ex);
        }
    }

    /**
     * Updates the target assignment according to the updated member and metadata image.
     *
     * @param group                The StreamsGroup.
     * @param groupEpoch           The group epoch.
     * @param updatedMember        The updated member.
     * @param metadataImage        The metadata image.
     * @param records              The list to accumulate any new records.
     * @return The new target assignment.
     */
    private TasksTuple updateStreamsTargetAssignment(
        StreamsGroup group,
        int groupEpoch,
        StreamsGroupMember updatedMember,
        ConfiguredTopology configuredTopology,
        CoordinatorMetadataImage metadataImage,
        List<CoordinatorRecord> records,
        Map<String, String> assignmentConfigs
    ) {
        TaskAssignor assignor = streamsGroupAssignor(group.groupId());
        try {
            org.apache.kafka.coordinator.group.streams.TargetAssignmentBuilder assignmentResultBuilder =
                new org.apache.kafka.coordinator.group.streams.TargetAssignmentBuilder(
                    group.groupId(),
                    groupEpoch,
                    assignor,
                    assignmentConfigs
                )
                .withMembers(group.members())
                .withTopology(configuredTopology)
                .withStaticMembers(group.staticMembers())
                .withMetadataImage(metadataImage)
                .withTargetAssignment(group.targetAssignment())
                .addOrUpdateMember(updatedMember.memberId(), updatedMember);

            long startTimeMs = time.milliseconds();
            org.apache.kafka.coordinator.group.streams.TargetAssignmentBuilder.TargetAssignmentResult assignmentResult =
                assignmentResultBuilder.build();
            long assignorTimeMs = time.milliseconds() - startTimeMs;

            if (log.isDebugEnabled()) {
                log.debug("[GroupId {}] Computed a new target assignment for epoch {} with '{}' assignor in {}ms: {}.",
                    group.groupId(), groupEpoch, assignor, assignorTimeMs, assignmentResult.targetAssignment());
            } else {
                log.info("[GroupId {}] Computed a new target assignment for epoch {} with '{}' assignor in {}ms.",
                    group.groupId(), groupEpoch, assignor, assignorTimeMs);
            }

            records.addAll(assignmentResult.records());

            return assignmentResult.targetAssignment().get(updatedMember.memberId());
        } catch (TaskAssignorException ex) {
            String msg = String.format("Failed to compute a new target assignment for epoch %d: %s",
                groupEpoch, ex.getMessage());
            log.error("[GroupId {}] {}.", group.groupId(), msg, ex);
            throw new UnknownServerException(msg, ex);
        }
    }

    /**
     * Handles leave request from a consumer group member.
     * @param groupId       The group id from the request.
     * @param memberId      The member id from the request.
     * @param memberEpoch   The member epoch from the request.
     *
     * @return A Result containing the ConsumerGroupHeartbeat response and
     *         a list of records to update the state machine.
     */
    private CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> consumerGroupLeave(
        String groupId,
        String instanceId,
        String memberId,
        int memberEpoch
    ) throws ApiException {
        ConsumerGroup group = consumerGroup(groupId);
        ConsumerGroupHeartbeatResponseData response = new ConsumerGroupHeartbeatResponseData()
            .setMemberId(memberId)
            .setMemberEpoch(memberEpoch);

        if (instanceId == null) {
            ConsumerGroupMember member = group.getOrMaybeCreateMember(memberId, false);
            log.info("[GroupId {}] Member {} left the consumer group.", groupId, memberId);
            return consumerGroupFenceMember(group, member, response);
        } else {
            ConsumerGroupMember member = group.staticMember(instanceId);
            throwIfStaticMemberIsUnknown(member, instanceId);
            throwIfInstanceIdIsFenced(member, groupId, memberId, instanceId);
            if (memberEpoch == LEAVE_GROUP_STATIC_MEMBER_EPOCH) {
                log.info("[GroupId {}] Static member {} with instance id {} temporarily left the consumer group.",
                    group.groupId(), memberId, instanceId);
                return consumerGroupStaticMemberGroupLeave(group, member);
            } else {
                log.info("[GroupId {}] Static member {} with instance id {} left the consumer group.",
                    group.groupId(), memberId, instanceId);
                return consumerGroupFenceMember(group, member, response);
            }
        }
    }

    /**
     * Handles leave request from a streams group member.
     * @param groupId       The group id from the request.
     * @param memberId      The member id from the request.
     * @param memberEpoch   The member epoch from the request.
     *
     * @return A result containing the StreamsGroupHeartbeat response and
     *         a list of records to update the state machine.
     */
    private CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> streamsGroupLeave(
        String groupId,
        String instanceId,
        String memberId,
        int memberEpoch,
        boolean shutdownApplication
    ) throws ApiException {
        StreamsGroup group = streamsGroup(groupId);
        if (shutdownApplication) {
            group.setShutdownRequestMemberId(memberId);
        }
        StreamsGroupHeartbeatResponseData response = new StreamsGroupHeartbeatResponseData()
            .setMemberId(memberId)
            .setMemberEpoch(memberEpoch);

        if (instanceId == null) {
            StreamsGroupMember member = group.getMemberOrThrow(memberId);
            log.info("[GroupId {}][MemberId {}] Member {} left the streams group.", groupId, memberId, memberId);
            return streamsGroupFenceMember(group, member, new StreamsGroupHeartbeatResult(response, Map.of()));
        } else {
            throw new UnsupportedOperationException("Static members are not supported in streams groups.");
        }
    }

    /**
     * Handles the case when a static member decides to leave the group.
     * The member is not actually fenced from the group, and instead it's
     * member epoch is updated to -2 to reflect that a member using the given
     * instance id decided to leave the group and would be back within session
     * timeout.
     *
     * @param group     The group.
     * @param member    The static member in the group for the instance id.
     *
     * @return A CoordinatorResult with a single record signifying that the static member is leaving.
     */
    private CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> consumerGroupStaticMemberGroupLeave(
        ConsumerGroup group,
        ConsumerGroupMember member
    ) {
        // We will write a member epoch of -2 for this departing static member.
        ConsumerGroupMember leavingStaticMember = new ConsumerGroupMember.Builder(member)
            .setMemberEpoch(LEAVE_GROUP_STATIC_MEMBER_EPOCH)
            .setPartitionsPendingRevocation(Map.of())
            .build();

        return new CoordinatorResult<>(
            List.of(newConsumerGroupCurrentAssignmentRecord(group.groupId(), leavingStaticMember)),
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(member.memberId())
                .setMemberEpoch(LEAVE_GROUP_STATIC_MEMBER_EPOCH)
        );
    }

    /**
     * Handles leave request from a share group member.
     * @param groupId       The group id from the request.
     * @param memberId      The member id from the request.
     * @param memberEpoch   The member epoch from the request.
     *
     * @return A Result containing the ShareGroupHeartbeat response and
     *         a list of records to update the state machine.
     */
    private CoordinatorResult<ShareGroupHeartbeatResponseData, CoordinatorRecord> shareGroupLeave(
        String groupId,
        String memberId,
        int memberEpoch
    ) throws ApiException {
        ShareGroup group = shareGroup(groupId);
        ShareGroupHeartbeatResponseData response = new ShareGroupHeartbeatResponseData()
            .setMemberId(memberId)
            .setMemberEpoch(memberEpoch);

        ShareGroupMember member = group.getOrMaybeCreateMember(memberId, false);
        log.info("[GroupId {}] Member {} left the share group.", groupId, memberId);

        return shareGroupFenceMember(group, member, response);
    }

    /**
      * Fences a member from a consumer group and maybe downgrade the consumer group to a classic group.
     *
     * @param group     The group.
     * @param member    The member.
     * @param response  The response of the CoordinatorResult.
     *
     * @return The CoordinatorResult to be applied.
     */
    private <T> CoordinatorResult<T, CoordinatorRecord> consumerGroupFenceMember(
        ConsumerGroup group,
        ConsumerGroupMember member,
        T response
    ) {
        return consumerGroupFenceMembers(group, Set.of(member), response);
    }

    /**
     * Fences members from a consumer group and maybe downgrade the consumer group to a classic group.
     *
     * @param group     The group.
     * @param members   The members.
     * @param response  The response of the CoordinatorResult.
     *
     * @return The CoordinatorResult to be applied.
     */
    private <T> CoordinatorResult<T, CoordinatorRecord> consumerGroupFenceMembers(
        ConsumerGroup group,
        Set<ConsumerGroupMember> members,
        T response
    ) {
        if (members.isEmpty()) {
            // No members to fence. Don't bump the group epoch.
            return new CoordinatorResult<>(List.of(), response);
        }

        List<CoordinatorRecord> records = new ArrayList<>();
        if (validateOnlineDowngradeWithFencedMembers(group, members)) {
            convertToClassicGroup(group, members, null, false, records);
            return new CoordinatorResult<>(records, response, null, false);
        } else {
            for (ConsumerGroupMember member : members) {
                removeMember(records, group.groupId(), member.memberId());
            }

            // Check whether resolved regular expressions could be deleted.
            Set<String> deletedRegexes = maybeDeleteResolvedRegularExpressions(
                records,
                group,
                members
            );

            Map<String, SubscriptionCount> subscribedTopicNamesMap = group.computeSubscribedTopicNamesWithoutDeletedMembers(
                members,
                deletedRegexes
            );
            long groupMetadataHash = ModernGroup.computeMetadataHash(
                subscribedTopicNamesMap,
                topicHashCache,
                metadataImage
            );

            if (groupMetadataHash != group.metadataHash()) {
                if (log.isDebugEnabled()) {
                    log.debug("[GroupId {}] Computed new metadata hash: {}.",
                        group.groupId(), groupMetadataHash);
                }
            }

            // We bump the group epoch.
            int groupEpoch = group.groupEpoch() + 1;
            records.add(newConsumerGroupEpochRecord(group.groupId(), groupEpoch, groupMetadataHash));
            log.info("[GroupId {}] Bumped group epoch to {} with metadata hash {}.", group.groupId(), groupEpoch, groupMetadataHash);

            for (ConsumerGroupMember member : members) {
                cancelTimers(group.groupId(), member.memberId());
            }

            return new CoordinatorResult<>(records, response);
        }
    }

    /**
     * Removes a member from a share group.
     *
     * @param group       The group.
     * @param member      The member.
     *
     * @return A list of records to be applied to the state.
     */
    private <T> CoordinatorResult<T, CoordinatorRecord> shareGroupFenceMember(
        ShareGroup group,
        ShareGroupMember member,
        T response
    ) {
        List<CoordinatorRecord> records = new ArrayList<>();
        records.add(newShareGroupCurrentAssignmentTombstoneRecord(group.groupId(), member.memberId()));
        records.add(newShareGroupTargetAssignmentTombstoneRecord(group.groupId(), member.memberId()));
        records.add(newShareGroupMemberSubscriptionTombstoneRecord(group.groupId(), member.memberId()));

        // We update the subscription metadata without the leaving member.
        long groupMetadataHash = ModernGroup.computeMetadataHash(
            group.computeSubscribedTopicNames(member, null),
            topicHashCache,
            metadataImage
        );

        if (groupMetadataHash != group.metadataHash()) {
            log.info("[GroupId {}] Computed new metadata hash: {}.",
                group.groupId(), groupMetadataHash);
        }

        // We bump the group epoch.
        int groupEpoch = group.groupEpoch() + 1;
        records.add(newShareGroupEpochRecord(group.groupId(), groupEpoch, groupMetadataHash));

        cancelGroupSessionTimeout(group.groupId(), member.memberId());

        return new CoordinatorResult<>(records, response);
    }

    /**
     * Write records to replace the old member by the new member.
     *
     * @param records   The list of records to append to.
     * @param group     The consumer group.
     * @param oldMember The old member.
     * @param newMember The new member.
     */
    private void replaceMember(
        List<CoordinatorRecord> records,
        ConsumerGroup group,
        ConsumerGroupMember oldMember,
        ConsumerGroupMember newMember
    ) {
        String groupId = group.groupId();

        // Remove the member without canceling its timers in case the change is reverted. If the
        // change is not reverted, the group validation will fail and the timer will do nothing.
        removeMember(records, groupId, oldMember.memberId());

        // Generate records.
        records.add(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(
            groupId,
            newMember
        ));
        records.add(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(
            groupId,
            newMember.memberId(),
            group.targetAssignment(oldMember.memberId()).partitions()
        ));
        records.add(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(
            groupId,
            newMember
        ));
    }

    /**
     * Fences a member from a streams group.
     *
     * @param group     The group.
     * @param member    The member.
     * @param response  The response of the CoordinatorResult.
     *
     * @return The CoordinatorResult to be applied.
     */
    private <T> CoordinatorResult<T, CoordinatorRecord> streamsGroupFenceMember(
        StreamsGroup group,
        StreamsGroupMember member,
        T response
    ) {
        List<CoordinatorRecord> records = new ArrayList<>();

        records.addAll(removeStreamsMember(group.groupId(), member.memberId()));

        // We bump the group epoch.
        int groupEpoch = group.groupEpoch() + 1;
        records.add(newStreamsGroupMetadataRecord(group.groupId(), groupEpoch, group.metadataHash(), group.validatedTopologyEpoch(), group.lastAssignmentConfigs()));

        cancelTimers(group.groupId(), member.memberId());

        return new CoordinatorResult<>(records, response);
    }

    /**
     * Write tombstones for the member.
     *
     * @param groupId       The group id.
     * @param memberId      The member id.
     */
    private List<CoordinatorRecord> removeStreamsMember(String groupId, String memberId) {
        return List.of(
            newStreamsGroupCurrentAssignmentTombstoneRecord(groupId, memberId),
            newStreamsGroupTargetAssignmentTombstoneRecord(groupId, memberId),
            newStreamsGroupMemberTombstoneRecord(groupId, memberId)
        );
    }

    /**
     * Maybe delete the resolved regular expressions associated with the provided members
     * if they were the last ones subscribed to them.
     *
     * @param records   The record accumulator.
     * @param group     The group.
     * @param members   The member removed from the group.
     * @return The set of deleted regular expressions.
     */
    private Set<String> maybeDeleteResolvedRegularExpressions(
        List<CoordinatorRecord> records,
        ConsumerGroup group,
        Set<ConsumerGroupMember> members
    ) {
        Map<String, Integer> counts = new HashMap<>();
        members.forEach(member -> {
            if (isNotEmpty(member.subscribedTopicRegex())) {
                counts.compute(member.subscribedTopicRegex(), Utils::incValue);
            }
        });

        Set<String> deletedRegexes = new HashSet<>();
        counts.forEach((regex, count) -> {
            if (group.numSubscribedMembers(regex) == count) {
                records.add(newConsumerGroupRegularExpressionTombstone(group.groupId(), regex));
                deletedRegexes.add(regex);
            }
        });

        return deletedRegexes;
    }

    /**
     * Write tombstones for the member. The order matters here.
     *
     * @param records       The list of records to append the member assignment tombstone records.
     * @param groupId       The group id.
     * @param memberId      The member id.
     */
    private void removeMember(List<CoordinatorRecord> records, String groupId, String memberId) {
        records.add(newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId));
        records.add(newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId));
        records.add(newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId));
    }

    /**
     * Cancel all the timers of the member.
     *
     * @param groupId       The group id.
     * @param memberId      The member id.
     */
    private void cancelTimers(String groupId, String memberId) {
        cancelGroupSessionTimeout(groupId, memberId);
        cancelGroupRebalanceTimeout(groupId, memberId);
        cancelConsumerGroupJoinTimeout(groupId, memberId);
        cancelConsumerGroupSyncTimeout(groupId, memberId);
    }

    /**
     * Schedules (or reschedules) the session timeout for the member.
     *
     * @param groupId       The group id.
     * @param memberId      The member id.
     */
    private void scheduleStreamsGroupSessionTimeout(
        String groupId,
        String memberId
    ) {
        timer.schedule(
            groupSessionTimeoutKey(groupId, memberId),
            streamsGroupSessionTimeoutMs(groupId),
            TimeUnit.MILLISECONDS,
            true,
            () -> streamsGroupFenceMemberOperation(groupId, memberId, "the member session expired.")
        );
    }

    /**
     * Schedules (or reschedules) the session timeout for the member.
     *
     * @param groupId       The group id.
     * @param memberId      The member id.
     */
    private void scheduleConsumerGroupSessionTimeout(
        String groupId,
        String memberId
    ) {
        scheduleConsumerGroupSessionTimeout(groupId, memberId, consumerGroupSessionTimeoutMs(groupId));
    }

    /**
     * Schedules (or reschedules) the session timeout for the member.
     *
     * @param groupId       The group id.
     * @param memberId      The member id.
     */
    private void scheduleShareGroupSessionTimeout(
        String groupId,
        String memberId
    ) {
        scheduleShareGroupSessionTimeout(groupId, memberId, shareGroupSessionTimeoutMs(groupId));
    }

    /**
     * Fences a member from a consumer group. Returns an empty CoordinatorResult
     * if the group or the member doesn't exist.
     *
     * @param groupId   The group id.
     * @param memberId  The member id.
     * @param reason    The reason for fencing the member.
     *
     * @return The CoordinatorResult to be applied.
     */
    private <T> CoordinatorResult<T, CoordinatorRecord> consumerGroupFenceMemberOperation(
        String groupId,
        String memberId,
        String reason
    ) {
        try {
            ConsumerGroup group = consumerGroup(groupId);
            ConsumerGroupMember member = group.getOrMaybeCreateMember(memberId, false);
            log.info("[GroupId {}] Member {} fenced from the group because {}.",
                groupId, memberId, reason);

            return consumerGroupFenceMember(group, member, null);
        } catch (GroupIdNotFoundException ex) {
            log.debug("[GroupId {}] Could not fence {} because the group does not exist.",
                groupId, memberId);
        } catch (UnknownMemberIdException ex) {
            log.debug("[GroupId {}] Could not fence {} because the member does not exist.",
                groupId, memberId);
        }

        return new CoordinatorResult<>(List.of());
    }

    /**
     * Fences a member from a share group. Returns an empty CoordinatorResult
     * if the group or the member doesn't exist.
     *
     * @param groupId   The group id.
     * @param memberId  The member id.
     * @param reason    The reason for fencing the member.
     *
     * @return The CoordinatorResult to be applied.
     */
    private <T> CoordinatorResult<T, CoordinatorRecord> shareGroupFenceMemberOperation(
        String groupId,
        String memberId,
        String reason
    ) {
        try {
            ShareGroup group = shareGroup(groupId);
            ShareGroupMember member = group.getOrMaybeCreateMember(memberId, false);
            log.info("[GroupId {}] Member {} fenced from the group because {}.",
                groupId, memberId, reason);

            return shareGroupFenceMember(group, member, null);
        } catch (GroupIdNotFoundException ex) {
            log.debug("[GroupId {}] Could not fence {} because the group does not exist.",
                groupId, memberId);
        } catch (UnknownMemberIdException ex) {
            log.debug("[GroupId {}] Could not fence {} because the member does not exist.",
                groupId, memberId);
        }

        return new CoordinatorResult<>(List.of());
    }

    /**
     * Fences a member from a streams group.
     * Returns an empty CoordinatorResult if the group or the member doesn't exist.
     *
     * @param groupId   The group id.
     * @param memberId  The member id.
     * @param reason    The reason for fencing the member.
     *
     * @return The CoordinatorResult to be applied.
     */
    private <T> CoordinatorResult<T, CoordinatorRecord> streamsGroupFenceMemberOperation(
        String groupId,
        String memberId,
        String reason
    ) {
        try {
            StreamsGroup group = streamsGroup(groupId);
            StreamsGroupMember member = group.getMemberOrThrow(memberId);
            log.info("[GroupId {}] Streams member {} fenced from the group because {}.",
                groupId, memberId, reason);

            return streamsGroupFenceMember(group, member, null);
        } catch (GroupIdNotFoundException ex) {
            log.debug("[GroupId {}] Could not fence streams member {} because the group does not exist.",
                groupId, memberId);
        } catch (UnknownMemberIdException ex) {
            log.debug("[GroupId {}] Could not fence streams member {} because the member does not exist.",
                groupId, memberId);
        }

        return new CoordinatorResult<>(List.of());
    }

    /**
     * Schedules (or reschedules) the session timeout for the member.
     *
     * @param groupId           The group id.
     * @param memberId          The member id.
     * @param sessionTimeoutMs  The session timeout.
     */
    private void scheduleConsumerGroupSessionTimeout(
        String groupId,
        String memberId,
        int sessionTimeoutMs
    ) {
        timer.schedule(
            groupSessionTimeoutKey(groupId, memberId),
            sessionTimeoutMs,
            TimeUnit.MILLISECONDS,
            true,
            () -> consumerGroupFenceMemberOperation(groupId, memberId, "the member session expired")
        );
    }

    /**
     * Schedules (or reschedules) the session timeout for the share group member.
     *
     * @param groupId       The group id.
     * @param memberId      The member id.
     */
    private void scheduleShareGroupSessionTimeout(
        String groupId,
        String memberId,
        int sessionTimeoutMs
    ) {
        timer.schedule(
            groupSessionTimeoutKey(groupId, memberId),
            sessionTimeoutMs,
            TimeUnit.MILLISECONDS,
            true,
            () -> shareGroupFenceMemberOperation(groupId, memberId, "the member session expired")
        );
    }

    /**
     * Cancels the session timeout of the member.
     *
     * @param groupId       The group id.
     * @param memberId      The member id.
     */
    private void cancelGroupSessionTimeout(
        String groupId,
        String memberId
    ) {
        timer.cancel(groupSessionTimeoutKey(groupId, memberId));
    }

    /**
     * Schedules a rebalance timeout for the member.
     *
     * @param groupId               The group id.
     * @param memberId              The member id.
     * @param memberEpoch           The member epoch.
     * @param rebalanceTimeoutMs    The rebalance timeout.
     */
    private void scheduleConsumerGroupRebalanceTimeout(
        String groupId,
        String memberId,
        int memberEpoch,
        int rebalanceTimeoutMs
    ) {
        String key = groupRebalanceTimeoutKey(groupId, memberId);
        timer.schedule(key, rebalanceTimeoutMs, TimeUnit.MILLISECONDS, true, () -> {
            try {
                ConsumerGroup group = consumerGroup(groupId);
                ConsumerGroupMember member = group.getOrMaybeCreateMember(memberId, false);

                if (member.memberEpoch() == memberEpoch) {
                    log.info("[GroupId {}] Member {} fenced from the group because " +
                            "it failed to transition from epoch {} within {}ms.",
                        groupId, memberId, memberEpoch, rebalanceTimeoutMs);

                    return consumerGroupFenceMember(group, member, null);
                } else {
                    log.debug("[GroupId {}] Ignoring rebalance timeout for {} because the member " +
                        "left the epoch {}.", groupId, memberId, memberEpoch);
                    return new CoordinatorResult<>(List.of());
                }
            } catch (GroupIdNotFoundException ex) {
                log.debug("[GroupId {}] Could not fence {}} because the group does not exist.",
                    groupId, memberId);
            } catch (UnknownMemberIdException ex) {
                log.debug("[GroupId {}] Could not fence {} because the member does not exist.",
                    groupId, memberId);
            }

            return new CoordinatorResult<>(List.of());
        });
    }

    /**
     * Schedules a rebalance timeout for the member.
     *
     * @param groupId               The group id.
     * @param memberId              The member id.
     * @param memberEpoch           The member epoch.
     * @param rebalanceTimeoutMs    The rebalance timeout.
     */
    private void scheduleStreamsGroupRebalanceTimeout(
        String groupId,
        String memberId,
        int memberEpoch,
        int rebalanceTimeoutMs
    ) {
        String key = groupRebalanceTimeoutKey(groupId, memberId);
        timer.schedule(key, rebalanceTimeoutMs, TimeUnit.MILLISECONDS, true, () -> {
            try {
                StreamsGroup group = streamsGroup(groupId);
                StreamsGroupMember member = group.getMemberOrThrow(memberId);

                if (member.memberEpoch() == memberEpoch) {
                    log.info("[GroupId {}] Member {} fenced from the group because " +
                            "it failed to transition from epoch {} within {}ms.",
                        groupId, memberId, memberEpoch, rebalanceTimeoutMs);

                    return streamsGroupFenceMember(group, member, null);
                } else {
                    log.debug("[GroupId {}] Ignoring rebalance timeout for {} because the member " +
                        "is not in epoch {} anymore.", groupId, memberId, memberEpoch);
                    return new CoordinatorResult<>(List.of());
                }
            } catch (GroupIdNotFoundException ex) {
                log.debug("[GroupId {}] Could not fence {}} because the group does not exist.",
                    groupId, memberId);
            } catch (UnknownMemberIdException ex) {
                log.debug("[GroupId {}] Could not fence {} because the member does not exist.",
                    groupId, memberId);
            }

            return new CoordinatorResult<>(List.of());
        });
    }

    /**
     * Cancels the rebalance timeout of the member.
     *
     * @param groupId       The group id.
     * @param memberId      The member id.
     */
    private void cancelGroupRebalanceTimeout(
        String groupId,
        String memberId
    ) {
        timer.cancel(groupRebalanceTimeoutKey(groupId, memberId));
    }

    /**
     * Schedules a join timeout for the member if there's not a join timeout.
     *
     * @param groupId               The group id.
     * @param memberId              The member id.
     * @param rebalanceTimeoutMs    The rebalance timeout.
     */
    private void scheduleConsumerGroupJoinTimeoutIfAbsent(
        String groupId,
        String memberId,
        int rebalanceTimeoutMs
    ) {
        timer.scheduleIfAbsent(
            consumerGroupJoinKey(groupId, memberId),
            rebalanceTimeoutMs,
            TimeUnit.MILLISECONDS,
            true,
            () -> consumerGroupFenceMemberOperation(groupId, memberId, "the classic member failed to join within the rebalance timeout")
        );
    }

    /**
     * Cancels the join timeout of the member.
     *
     * @param groupId       The group id.
     * @param memberId      The member id.
     */
    private void cancelConsumerGroupJoinTimeout(
        String groupId,
        String memberId
    ) {
        timer.cancel(consumerGroupJoinKey(groupId, memberId));
    }

    /**
     * Schedules a sync timeout for the member.
     *
     * @param groupId               The group id.
     * @param memberId              The member id.
     * @param rebalanceTimeoutMs    The rebalance timeout.
     */
    private void scheduleConsumerGroupSyncTimeout(
        String groupId,
        String memberId,
        int rebalanceTimeoutMs
    ) {
        timer.schedule(
            consumerGroupSyncKey(groupId, memberId),
            rebalanceTimeoutMs,
            TimeUnit.MILLISECONDS,
            true,
            () -> consumerGroupFenceMemberOperation(groupId, memberId, "the member failed to sync within timeout")
        );
    }

    /**
     * Cancels the sync timeout of the member.
     *
     * @param groupId       The group id.
     * @param memberId      The member id.
     */
    private void cancelConsumerGroupSyncTimeout(
        String groupId,
        String memberId
    ) {
        timer.cancel(consumerGroupSyncKey(groupId, memberId));
    }

    /**
     * Handles a ConsumerGroupHeartbeat request.
     *
     * @param context The request context.
     * @param request The actual ConsumerGroupHeartbeat request.
     *
     * @return A Result containing the ConsumerGroupHeartbeat response and
     *         a list of records to update the state machine.
     */
    public CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> consumerGroupHeartbeat(
        AuthorizableRequestContext context,
        ConsumerGroupHeartbeatRequestData request
    ) throws ApiException {
        if (request.memberEpoch() == LEAVE_GROUP_MEMBER_EPOCH || request.memberEpoch() == LEAVE_GROUP_STATIC_MEMBER_EPOCH) {
            // -1 means that the member wants to leave the group.
            // -2 means that a static member wants to leave the group.
            return consumerGroupLeave(
                request.groupId(),
                request.instanceId(),
                request.memberId(),
                request.memberEpoch()
            );
        } else {
            // Otherwise, it is a regular heartbeat.
            return consumerGroupHeartbeat(
                context,
                request.groupId(),
                request.memberId(),
                request.memberEpoch(),
                request.instanceId(),
                request.rackId(),
                request.rebalanceTimeoutMs(),
                request.subscribedTopicNames(),
                request.subscribedTopicRegex(),
                request.serverAssignor(),
                request.topicPartitions()
            );
        }
    }

    /**
     * Handles a StreamsGroupHeartbeat request.
     *
     * @param context The request context.
     * @param request The actual StreamsGroupHeartbeat request.
     *
     * @return A result containing the StreamsGroupHeartbeat response, a list of internal topics to create and
     *         a list of records to update the state machine.
     */
    public CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> streamsGroupHeartbeat(
        AuthorizableRequestContext context,
        StreamsGroupHeartbeatRequestData request
    ) throws ApiException {
        if (request.memberEpoch() == LEAVE_GROUP_MEMBER_EPOCH || request.memberEpoch() == LEAVE_GROUP_STATIC_MEMBER_EPOCH) {
            // -1 means that the member wants to leave the group.
            // -2 means that a static member wants to leave the group.
            return streamsGroupLeave(
                request.groupId(),
                request.instanceId(),
                request.memberId(),
                request.memberEpoch(),
                request.shutdownApplication()
            );
        } else {
            return streamsGroupHeartbeat(
                request.groupId(),
                request.memberId(),
                request.memberEpoch(),
                request.instanceId(),
                request.rackId(),
                request.rebalanceTimeoutMs(),
                context.clientId(),
                context.clientAddress().toString(),
                request.topology(),
                request.activeTasks(),
                request.standbyTasks(),
                request.warmupTasks(),
                request.processId(),
                request.userEndpoint(),
                request.clientTags(),
                request.shutdownApplication(),
                request.endpointInformationEpoch()
            );
        }
    }

    /**
     * Replays StreamsGroupTopologyKey/Value to update the hard state of
     * the streams group.
     *
     * @param key   A StreamsGroupTopologyKey key.
     * @param value A StreamsGroupTopologyValue record.
     */
    public void replay(
        StreamsGroupTopologyKey key,
        StreamsGroupTopologyValue value
    ) {
        String groupId = key.groupId();
        StreamsGroup streamsGroup;
        try {
            streamsGroup = getOrMaybeCreatePersistedStreamsGroup(groupId, value != null);
        } catch (GroupIdNotFoundException ex) {
            // If the group does not exist and a tombstone is replayed, we can ignore it.
            return;
        }

        Set<String> oldSubscribedTopicNames;
        if (streamsGroup.topology().isPresent()) {
            oldSubscribedTopicNames = streamsGroup.topology().get().requiredTopics();
        } else {
            oldSubscribedTopicNames = Set.of();
        }
        if (value != null) {
            StreamsTopology topology = StreamsTopology.fromRecord(value);
            streamsGroup.setTopology(topology);
            Set<String> newSubscribedTopicNames = topology.requiredTopics();
            updateGroupsByTopics(groupId, oldSubscribedTopicNames, newSubscribedTopicNames);
        } else {
            updateGroupsByTopics(groupId, oldSubscribedTopicNames, Set.of());
            streamsGroup.setTopology(null);
        }
    }

    /**
     * Handles a ShareGroupHeartbeat request.
     *
     * @param context The request context.
     * @param request The actual ShareGroupHeartbeat request.
     *
     * @return A Result containing a pair of ShareGroupHeartbeat response and maybe InitializeShareGroupStateParameters
     *         and a list of records to update the state machine.
     */
    public CoordinatorResult<Map.Entry<ShareGroupHeartbeatResponseData, Optional<InitializeShareGroupStateParameters>>, CoordinatorRecord> shareGroupHeartbeat(
        AuthorizableRequestContext context,
        ShareGroupHeartbeatRequestData request
    ) throws ApiException {
        if (request.memberEpoch() == ShareGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH) {
            // -1 means that the member wants to leave the group.
            CoordinatorResult<ShareGroupHeartbeatResponseData, CoordinatorRecord> result = shareGroupLeave(
                request.groupId(),
                request.memberId(),
                request.memberEpoch()
            );
            return new CoordinatorResult<>(
                result.records(),
                Map.entry(result.response(), Optional.empty())
            );
        }
        // Otherwise, it is a regular heartbeat.
        return shareGroupHeartbeat(
            request.groupId(),
            request.memberId(),
            request.memberEpoch(),
            request.rackId(),
            context.clientId(),
            context.clientAddress().toString(),
            request.subscribedTopicNames());
    }

    /**
     * Handles an initialize share group state request. This is usually part of
     * shareGroupHeartbeat code flow.
     * @param groupId The group id corresponding to the share group whose share partitions have been initialized.
     * @param topicPartitionMap Map representing topic partition data to be added to the share state partition metadata.
     *
     * @return A Result containing ShareGroupStatePartitionMetadata records and Void response.
     */
    public CoordinatorResult<Void, CoordinatorRecord> initializeShareGroupState(
        String groupId,
        Map<Uuid, Set<Integer>> topicPartitionMap
    ) {
        // Should be present
        if (topicPartitionMap == null || topicPartitionMap.isEmpty()) {
            return new CoordinatorResult<>(List.of(), null);
        }
        ShareGroup group = (ShareGroup) groups.get(groupId);

        ShareGroupStatePartitionMetadataInfo currentMap = shareGroupStatePartitionMetadata.get(groupId);
        Map<Uuid, InitMapValue> enrichedTopicPartitionMap = attachInitValue(topicPartitionMap);
        if (currentMap == null) {
            return new CoordinatorResult<>(
                List.of(newShareGroupStatePartitionMetadataRecord(group.groupId(), Map.of(), enrichedTopicPartitionMap, Map.of())),
                null
            );
        }

        // We must combine the existing information in the record with the topicPartitionMap argument so that the final
        // record has up-to-date information.
        Map<Uuid, InitMapValue> finalInitializedMap = combineInitMaps(currentMap.initializedTopics(), enrichedTopicPartitionMap);

        // Fetch initializing info from state metadata.
        Map<Uuid, InitMapValue> finalInitializingMap = new HashMap<>();
        currentMap.initializingTopics().forEach((k, v) -> finalInitializingMap.put(k, new InitMapValue(v.name(), new HashSet<>(v.partitions()), v.timestamp())));

        // Remove any entries which are already initialized.
        for (Map.Entry<Uuid, Set<Integer>> entry : topicPartitionMap.entrySet()) {
            Uuid topicId = entry.getKey();
            if (finalInitializingMap.containsKey(topicId)) {
                Set<Integer> partitions = finalInitializingMap.get(topicId).partitions();
                partitions.removeAll(entry.getValue());
                if (partitions.isEmpty()) {
                    finalInitializingMap.remove(topicId);
                }
            }
        }

        return new CoordinatorResult<>(List.of(
            newShareGroupStatePartitionMetadataRecord(
                group.groupId(),
                finalInitializingMap,
                finalInitializedMap,
                attachTopicName(currentMap.deletingTopics())
            )),
            null
        );
    }

    /**
     * Removes specific topic partitions from the initializing state for a share group. This is usually part of
     * shareGroupHeartbeat code flow, specifically, if there is a persister exception.
     * @param groupId The group id corresponding to the share group whose share partitions have been initialized.
     * @param topicPartitionMap Map representing topic partition data to be cleaned from the share state partition metadata.
     *
     * @return A Result containing ShareGroupStatePartitionMetadata records and Void response.
     */
    public CoordinatorResult<Void, CoordinatorRecord> uninitializeShareGroupState(
        String groupId,
        Map<Uuid, Set<Integer>> topicPartitionMap
    ) {
        ShareGroupStatePartitionMetadataInfo info = shareGroupStatePartitionMetadata.get(groupId);
        if (info == null || info.initializingTopics().isEmpty() || topicPartitionMap.isEmpty()) {
            return new CoordinatorResult<>(List.of(), null);
        }

        Map<Uuid, InitMapValue> initializingTopics = info.initializingTopics();
        Map<Uuid, InitMapValue> finalInitializingTopics = new HashMap<>();

        for (Map.Entry<Uuid, InitMapValue> entry : initializingTopics.entrySet()) {
            Uuid topicId = entry.getKey();
            // If topicId to clean is not present in topicPartitionMap map, retain it.
            if (!topicPartitionMap.containsKey(topicId)) {
                finalInitializingTopics.put(entry.getKey(), entry.getValue());
            } else {
                Set<Integer> partitions = new HashSet<>(entry.getValue().partitions());
                partitions.removeAll(topicPartitionMap.get(topicId));
                if (!partitions.isEmpty()) {
                    finalInitializingTopics.put(entry.getKey(), new InitMapValue(entry.getValue().name(), partitions, entry.getValue().timestamp()));
                }
            }
        }

        return new CoordinatorResult<>(
            List.of(
                newShareGroupStatePartitionMetadataRecord(
                    groupId,
                    finalInitializingTopics,
                    info.initializedTopics(),
                    attachTopicName(info.deletingTopics())
                )
            ),
            null
        );
    }

    private Map<Uuid, String> attachTopicName(Set<Uuid> topicIds) {
        Map<Uuid, String> finalMap = new HashMap<>();
        for (Uuid topicId : topicIds) {
            String topicName = metadataImage.topicMetadata(topicId).map(CoordinatorMetadataImage.TopicMetadata::name).orElse("<UNKNOWN>");
            finalMap.put(topicId, topicName);
        }
        return Collections.unmodifiableMap(finalMap);
    }

    private Map<Uuid, InitMapValue> attachInitValue(Map<Uuid, Set<Integer>> initMap) {
        Map<Uuid, InitMapValue> finalMap = new HashMap<>();
        long timestamp = time.milliseconds();
        for (Map.Entry<Uuid, Set<Integer>> entry : initMap.entrySet()) {
            Uuid topicId = entry.getKey();
            String topicName = metadataImage.topicMetadata(topicId).map(CoordinatorMetadataImage.TopicMetadata::name).orElse("<UNKNOWN>");
            finalMap.put(topicId, new InitMapValue(topicName, entry.getValue(), timestamp));
        }
        return Collections.unmodifiableMap(finalMap);
    }

    /**
     * Returns the set of share partitions whose state has been initialized.
     *
     * @param groupId The group id corresponding to the share group whose share partitions have been initialized.
     *
     * @return A map representing the initialized share-partitions for the share group.
     */
    public Map<Uuid, Set<Integer>> initializedShareGroupPartitions(
        String groupId
    ) {
        Map<Uuid, Set<Integer>> resultMap = new HashMap<>();

        ShareGroupStatePartitionMetadataInfo currentMap = shareGroupStatePartitionMetadata.get(groupId);
        if (currentMap != null) {
            currentMap.initializedTopics().forEach((topicId, initMapValue) -> {
                resultMap.put(topicId, new HashSet<>(initMapValue.partitions()));
            });
        }

        return resultMap;
    }

    /**
     * Replays ConsumerGroupMemberMetadataKey/Value to update the hard state of
     * the consumer group. It updates the subscription part of the member or
     * delete the member.
     *
     * @param key   A ConsumerGroupMemberMetadataKey key.
     * @param value A ConsumerGroupMemberMetadataValue record.
     */
    public void replay(
        ConsumerGroupMemberMetadataKey key,
        ConsumerGroupMemberMetadataValue value
    ) {
        String groupId = key.groupId();
        String memberId = key.memberId();

        ConsumerGroup consumerGroup;
        try {
            consumerGroup = getOrMaybeCreatePersistedConsumerGroup(groupId, value != null);
        } catch (GroupIdNotFoundException ex) {
            // If the group does not exist and a tombstone is replayed, we can ignore it.
            return;
        }

        Set<String> oldSubscribedTopicNames = new HashSet<>(consumerGroup.subscribedTopicNames().keySet());

        if (value != null) {
            ConsumerGroupMember oldMember = consumerGroup.getOrMaybeCreateMember(memberId, true);
            consumerGroup.updateMember(new ConsumerGroupMember.Builder(oldMember)
                .updateWith(value)
                .build());
        } else {
            ConsumerGroupMember oldMember;
            try {
                oldMember = consumerGroup.getOrMaybeCreateMember(memberId, false);
            } catch (UnknownMemberIdException ex) {
                // If the member does not exist, we can ignore it.
                return;
            }

            if (oldMember.memberEpoch() != LEAVE_GROUP_MEMBER_EPOCH) {
                throw new IllegalStateException("Received a tombstone record to delete member " + memberId
                    + " but did not receive ConsumerGroupCurrentMemberAssignmentValue tombstone.");
            }
            if (consumerGroup.targetAssignment().containsKey(memberId)) {
                throw new IllegalStateException("Received a tombstone record to delete member " + memberId
                    + " but did not receive ConsumerGroupTargetAssignmentMetadataValue tombstone.");
            }
            consumerGroup.removeMember(memberId);
        }

        updateGroupsByTopics(groupId, oldSubscribedTopicNames, consumerGroup.subscribedTopicNames().keySet());
    }

    /**
     * @return The set of groups subscribed to the topic.
     */
    public Set<String> groupsSubscribedToTopic(String topicName) {
        Set<String> groups = groupsByTopics.get(topicName);
        return groups != null ? groups : Set.of();
    }

    /**
     * Subscribes a group to a topic.
     *
     * @param groupId   The group id.
     * @param topicName The topic name.
     */
    private void subscribeGroupToTopic(
        String groupId,
        String topicName
    ) {
        groupsByTopics
            .computeIfAbsent(topicName, __ -> new TimelineHashSet<>(snapshotRegistry, 1))
            .add(groupId);
    }

    /**
     * Unsubscribes a group from a topic.
     *
     * @param groupId   The group id.
     * @param topicName The topic name.
     */
    private void unsubscribeGroupFromTopic(
        String groupId,
        String topicName
    ) {
        groupsByTopics.computeIfPresent(topicName, (__, groupIds) -> {
            groupIds.remove(groupId);
            if (groupIds.isEmpty()) {
                topicHashCache.remove(topicName);
                return null;
            }
            return groupIds;
        });
    }

    /**
     * Updates the group by topics mapping.
     *
     * @param groupId               The group id.
     * @param oldSubscribedTopics   The old group subscriptions.
     * @param newSubscribedTopics   The new group subscriptions.
     */
    private void updateGroupsByTopics(
        String groupId,
        Set<String> oldSubscribedTopics,
        Set<String> newSubscribedTopics
    ) {
        if (oldSubscribedTopics.isEmpty()) {
            newSubscribedTopics.forEach(topicName ->
                subscribeGroupToTopic(groupId, topicName)
            );
        } else if (newSubscribedTopics.isEmpty()) {
            oldSubscribedTopics.forEach(topicName ->
                unsubscribeGroupFromTopic(groupId, topicName)
            );
        } else {
            oldSubscribedTopics.forEach(topicName -> {
                if (!newSubscribedTopics.contains(topicName)) {
                    unsubscribeGroupFromTopic(groupId, topicName);
                }
            });
            newSubscribedTopics.forEach(topicName -> {
                if (!oldSubscribedTopics.contains(topicName)) {
                    subscribeGroupToTopic(groupId, topicName);
                }
            });
        }
    }

    /**
     * Replays ConsumerGroupMetadataKey/Value to update the hard state of
     * the consumer group. It updates the group epoch of the consumer
     * group or deletes the consumer group.
     *
     * @param key   A ConsumerGroupMetadataKey key.
     * @param value A ConsumerGroupMetadataValue record.
     */
    public void replay(
        ConsumerGroupMetadataKey key,
        ConsumerGroupMetadataValue value
    ) {
        String groupId = key.groupId();

        if (value != null) {
            ConsumerGroup consumerGroup = getOrMaybeCreatePersistedConsumerGroup(groupId, true);
            consumerGroup.setGroupEpoch(value.epoch());
            consumerGroup.setMetadataHash(value.metadataHash());
        } else {
            ConsumerGroup consumerGroup;
            try {
                consumerGroup = getOrMaybeCreatePersistedConsumerGroup(groupId, false);
            } catch (GroupIdNotFoundException ex) {
                // If the group does not exist, we can ignore the tombstone.
                return;
            }

            if (!consumerGroup.members().isEmpty()) {
                throw new IllegalStateException("Received a tombstone record to delete group " + groupId
                    + " but the group still has " + consumerGroup.members().size() + " members.");
            }
            if (!consumerGroup.targetAssignment().isEmpty()) {
                throw new IllegalStateException("Received a tombstone record to delete group " + groupId
                    + " but the target assignment still has " + consumerGroup.targetAssignment().size()
                    + " members.");
            }
            if (consumerGroup.assignmentEpoch() != -1) {
                throw new IllegalStateException("Received a tombstone record to delete group " + groupId
                    + " but did not receive ConsumerGroupTargetAssignmentMetadataValue tombstone.");
            }
            removeGroup(groupId);
        }

    }

    /**
     * Replays ConsumerGroupPartitionMetadataKey/Value to update the hard state of
     * the consumer group. It updates the subscription metadata of the consumer
     * group.
     *
     * @param key   A ConsumerGroupPartitionMetadataKey key.
     * @param value A ConsumerGroupPartitionMetadataValue record.
     */
    public void replay(
        ConsumerGroupPartitionMetadataKey key,
        ConsumerGroupPartitionMetadataValue value
    ) {
        String groupId = key.groupId();

        ConsumerGroup group;
        try {
            group = getOrMaybeCreatePersistedConsumerGroup(groupId, value != null);
        } catch (GroupIdNotFoundException ex) {
            // If the group does not exist, we can ignore the tombstone.
            return;
        }

        // If value is not null, add subscription metadata tombstone record in the next consumer group heartbeat,
        // because the subscription metadata is replaced by metadata hash in ConsumerGroupMetadataValue.
        group.setHasSubscriptionMetadataRecord(value != null);
    }

    /**
     * Replays ConsumerGroupTargetAssignmentMemberKey/Value to update the hard state of
     * the consumer group. It updates the target assignment of the member or deletes it.
     *
     * @param key   A ConsumerGroupTargetAssignmentMemberKey key.
     * @param value A ConsumerGroupTargetAssignmentMemberValue record.
     */
    public void replay(
        ConsumerGroupTargetAssignmentMemberKey key,
        ConsumerGroupTargetAssignmentMemberValue value
    ) {
        String groupId = key.groupId();
        String memberId = key.memberId();

        if (value != null) {
            ConsumerGroup group = getOrMaybeCreatePersistedConsumerGroup(groupId, true);
            group.updateTargetAssignment(memberId, Assignment.fromRecord(value));
        } else {
            ConsumerGroup group;
            try {
                group = getOrMaybeCreatePersistedConsumerGroup(groupId, false);
            } catch (GroupIdNotFoundException ex) {
                // If the group does not exist, we can ignore the tombstone.
                return;
            }
            group.removeTargetAssignment(memberId);
        }
    }

    /**
     * Replays ConsumerGroupTargetAssignmentMetadataKey/Value to update the hard state of
     * the consumer group. It updates the target assignment epoch or set it to -1 to signal
     * that it has been deleted.
     *
     * @param key   A ConsumerGroupTargetAssignmentMetadataKey key.
     * @param value A ConsumerGroupTargetAssignmentMetadataValue record.
     */
    public void replay(
        ConsumerGroupTargetAssignmentMetadataKey key,
        ConsumerGroupTargetAssignmentMetadataValue value
    ) {
        String groupId = key.groupId();

        if (value != null) {
            ConsumerGroup group = getOrMaybeCreatePersistedConsumerGroup(groupId, true);
            group.setTargetAssignmentEpoch(value.assignmentEpoch());
        } else {
            ConsumerGroup group;
            try {
                group = getOrMaybeCreatePersistedConsumerGroup(groupId, false);
            } catch (GroupIdNotFoundException ex) {
                // If the group does not exist, we can ignore the tombstone.
                return;
            }
            if (!group.targetAssignment().isEmpty()) {
                throw new IllegalStateException("Received a tombstone record to delete target assignment of " + groupId
                    + " but the assignment still has " + group.targetAssignment().size() + " members.");
            }
            group.setTargetAssignmentEpoch(-1);
        }
    }

    /**
     * Replays ConsumerGroupCurrentMemberAssignmentKey/Value to update the hard state of
     * the consumer group. It updates the assignment of a member or deletes it.
     *
     * @param key   A ConsumerGroupCurrentMemberAssignmentKey key.
     * @param value A ConsumerGroupCurrentMemberAssignmentValue record.
     */
    public void replay(
        ConsumerGroupCurrentMemberAssignmentKey key,
        ConsumerGroupCurrentMemberAssignmentValue value
    ) {
        String groupId = key.groupId();
        String memberId = key.memberId();

        if (value != null) {
            ConsumerGroup group = getOrMaybeCreatePersistedConsumerGroup(groupId, true);
            ConsumerGroupMember oldMember = group.getOrMaybeCreateMember(memberId, true);
            ConsumerGroupMember newMember = new ConsumerGroupMember.Builder(oldMember)
                .updateWith(value)
                .build();
            group.updateMember(newMember);
        } else {
            ConsumerGroup group;
            try {
                group = getOrMaybeCreatePersistedConsumerGroup(groupId, false);
            } catch (GroupIdNotFoundException ex) {
                // If the group does not exist, we can ignore the tombstone.
                return;
            }

            ConsumerGroupMember oldMember;
            try {
                oldMember = group.getOrMaybeCreateMember(memberId, false);
            } catch (UnknownMemberIdException ex) {
                // If the member does not exist, we can ignore the tombstone.
                return;
            }

            ConsumerGroupMember newMember = new ConsumerGroupMember.Builder(oldMember)
                .setMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH)
                .setPreviousMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH)
                .setAssignedPartitions(Map.of())
                .setPartitionsPendingRevocation(Map.of())
                .build();
            group.updateMember(newMember);
        }
    }

    /**
     * Replays ConsumerGroupRegularExpressionKey/Value to update the hard state of
     * the consumer group.
     *
     * @param key   A ConsumerGroupRegularExpressionKey key.
     * @param value A ConsumerGroupRegularExpressionValue record.
     */
    public void replay(
        ConsumerGroupRegularExpressionKey key,
        ConsumerGroupRegularExpressionValue value
    ) {
        String groupId = key.groupId();
        String regex = key.regularExpression();

        ConsumerGroup consumerGroup;
        try {
            consumerGroup = getOrMaybeCreatePersistedConsumerGroup(groupId, value != null);
        } catch (GroupIdNotFoundException ex) {
            // If the group does not exist and a tombstone is replayed, we can ignore it.
            return;
        }

        Set<String> oldSubscribedTopicNames = new HashSet<>(consumerGroup.subscribedTopicNames().keySet());

        if (value != null) {
            consumerGroup.updateResolvedRegularExpression(
                regex,
                new ResolvedRegularExpression(
                    new HashSet<>(value.topics()),
                    value.version(),
                    value.timestamp()
                )
            );
        } else {
            consumerGroup.removeResolvedRegularExpression(regex);
        }

        updateGroupsByTopics(groupId, oldSubscribedTopicNames, consumerGroup.subscribedTopicNames().keySet());
    }

    /**
     * Replays StreamsGroupMetadataKey/Value to update the hard state of
     * the streams group. It updates the group epoch of the Streams
     * group or deletes the streams group.
     *
     * @param key   A StreamsGroupMetadataKey key.
     * @param value A StreamsGroupMetadataValue record.
     */
    public void replay(
        StreamsGroupMetadataKey key,
        StreamsGroupMetadataValue value
    ) {
        String groupId = key.groupId();

        if (value != null) {
            StreamsGroup streamsGroup = getOrMaybeCreatePersistedStreamsGroup(groupId, true);
            streamsGroup.setGroupEpoch(value.epoch());
            streamsGroup.setMetadataHash(value.metadataHash());
            streamsGroup.setValidatedTopologyEpoch(value.validatedTopologyEpoch());

            if (value.lastAssignmentConfigs() != null) {
                streamsGroup.setLastAssignmentConfigs(
                    value.lastAssignmentConfigs().stream()
                        .collect(Collectors.toMap(
                            StreamsGroupMetadataValue.LastAssignmentConfig::key,
                            StreamsGroupMetadataValue.LastAssignmentConfig::value
                        ))
                );
            } else {
                streamsGroup.setLastAssignmentConfigs(Map.of());
            }

        } else {
            StreamsGroup streamsGroup;
            try {
                streamsGroup = getOrMaybeCreatePersistedStreamsGroup(groupId, false);
            } catch (GroupIdNotFoundException ex) {
                // If the group does not exist, we can ignore the tombstone.
                return;
            }

            if (!streamsGroup.members().isEmpty()) {
                throw new IllegalStateException("Received a tombstone record to delete group " + groupId
                    + " but the group still has " + streamsGroup.members().size() + " members.");
            }
            if (streamsGroup.assignmentEpoch() != -1) {
                throw new IllegalStateException("Received a tombstone record to delete group " + groupId
                    + " but did not receive StreamsGroupTargetAssignmentMetadataValue tombstone.");
            }
            removeGroup(groupId);
        }

    }

    /**
     * Replays ShareGroupMemberMetadataKey/Value to update the hard state of
     * the share group. It updates the subscription part of the member or
     * delete the member.
     *
     * @param key   A ShareGroupMemberMetadataKey key.
     * @param value A ShareGroupMemberMetadataValue record.
     */
    public void replay(
        ShareGroupMemberMetadataKey key,
        ShareGroupMemberMetadataValue value
    ) {
        String groupId = key.groupId();
        String memberId = key.memberId();

        ShareGroup shareGroup;
        ShareGroupMember oldMember;
        try {
            shareGroup = getOrMaybeCreatePersistedShareGroup(groupId, value != null);
            oldMember = shareGroup.getOrMaybeCreateMember(memberId, value != null);
        } catch (GroupIdNotFoundException ex) {
            log.debug("ShareGroupMemberMetadata tombstone without group - {}", groupId, ex);
            return;
        } catch (UnknownMemberIdException ex) {
            log.debug("ShareGroupMemberMetadata tombstone for groupId - {} without member - {}", groupId, memberId, ex);
            return;
        }

        Set<String> oldSubscribedTopicNames = new HashSet<>(shareGroup.subscribedTopicNames().keySet());

        if (value != null) {
            shareGroup.updateMember(new ShareGroupMember.Builder(oldMember)
                .updateWith(value)
                .build());
        } else {
            if (oldMember.memberEpoch() != LEAVE_GROUP_MEMBER_EPOCH) {
                throw new IllegalStateException("Received a tombstone record to delete member " + memberId
                    + " with invalid leave group epoch.");
            }
            if (shareGroup.targetAssignment().containsKey(memberId)) {
                throw new IllegalStateException("Received a tombstone record to delete member " + memberId
                    + " but member exists in target assignment.");
            }
            shareGroup.removeMember(memberId);
        }

        updateGroupsByTopics(groupId, oldSubscribedTopicNames, shareGroup.subscribedTopicNames().keySet());
    }

    /**
     * Replays ShareGroupMetadataKey/Value to update the hard state of
     * the share group. It updates the group epoch of the share
     * group or deletes the share group.
     *
     * @param key   A ShareGroupMetadataKey key.
     * @param value A ShareGroupMetadataValue record.
     */
    public void replay(
        ShareGroupMetadataKey key,
        ShareGroupMetadataValue value
    ) {
        String groupId = key.groupId();

        ShareGroup shareGroup;
        try {
            shareGroup = getOrMaybeCreatePersistedShareGroup(groupId, value != null);
        } catch (GroupIdNotFoundException ex) {
            log.debug("ShareGroupMetadata tombstone without group - {}", groupId, ex);
            return;
        }

        if (value != null) {
            shareGroup.setGroupEpoch(value.epoch());
            shareGroup.setMetadataHash(value.metadataHash());
        } else {
            if (!shareGroup.members().isEmpty()) {
                throw new IllegalStateException("Received a tombstone record to delete group " + groupId
                    + " but the group still has " + shareGroup.members().size() + " members.");
            }
            if (!shareGroup.targetAssignment().isEmpty()) {
                throw new IllegalStateException("Received a tombstone record to delete group " + groupId
                    + " but the target assignment still has " + shareGroup.targetAssignment().size()
                    + " members.");
            }
            if (shareGroup.assignmentEpoch() != -1) {
                throw new IllegalStateException("Received a tombstone record to delete group " + groupId
                    + " but target assignment epoch in invalid.");
            }
            removeGroup(groupId);
        }
    }

    /**
     * Replays StreamsGroupMemberMetadataKey/Value to update the hard state of
     * the streams group.
     * It updates the subscription part of the member or deletes the member.
     *
     * @param key   A StreamsGroupMemberMetadataKey key.
     * @param value A StreamsGroupMemberMetadataValue record.
     */
    public void replay(
        StreamsGroupMemberMetadataKey key,
        StreamsGroupMemberMetadataValue value
    ) {
        String groupId = key.groupId();
        String memberId = key.memberId();

        StreamsGroup streamsGroup;
        try {
            streamsGroup = getOrMaybeCreatePersistedStreamsGroup(groupId, value != null);
        } catch (GroupIdNotFoundException ex) {
            // If the group does not exist and a tombstone is replayed, we can ignore it.
            return;
        }

        if (value != null) {
            StreamsGroupMember oldMember = streamsGroup.getOrCreateUninitializedMember(memberId);
            streamsGroup.updateMember(new StreamsGroupMember.Builder(oldMember)
                .updateWith(value)
                .build());
        } else {
            StreamsGroupMember oldMember;
            try {
                oldMember = streamsGroup.getMemberOrThrow(memberId);
            } catch (UnknownMemberIdException ex) {
                // If the member does not exist, we can ignore it.
                return;
            }

            if (oldMember.memberEpoch() != LEAVE_GROUP_MEMBER_EPOCH) {
                throw new IllegalStateException("Received a tombstone record to delete member " + memberId
                    + " but did not receive StreamsGroupCurrentMemberAssignmentValue tombstone.");
            }
            if (streamsGroup.targetAssignment().containsKey(memberId)) {
                throw new IllegalStateException("Received a tombstone record to delete member " + memberId
                    + " but did not receive StreamsGroupTargetAssignmentMetadataValue tombstone.");
            }
            streamsGroup.removeMember(memberId);
        }
    }

    /**
     * Replays StreamsGroupTargetAssignmentMetadataKey/Value to update the hard state of
     * the streams group.
     * It updates the target assignment epoch or sets it to -1 to signal that it has been deleted.
     *
     * @param key   A StreamsGroupTargetAssignmentMetadataKey key.
     * @param value A StreamsGroupTargetAssignmentMetadataValue record.
     */
    public void replay(
        StreamsGroupTargetAssignmentMetadataKey key,
        StreamsGroupTargetAssignmentMetadataValue value
    ) {
        String groupId = key.groupId();

        if (value != null) {
            StreamsGroup streamsGroup = getOrMaybeCreatePersistedStreamsGroup(groupId, true);
            streamsGroup.setTargetAssignmentEpoch(value.assignmentEpoch());
        } else {
            StreamsGroup streamsGroup;
            try {
                streamsGroup = getOrMaybeCreatePersistedStreamsGroup(groupId, false);
            } catch (GroupIdNotFoundException ex) {
                // If the group does not exist, we can ignore the tombstone.
                return;
            }
            if (!streamsGroup.targetAssignment().isEmpty()) {
                throw new IllegalStateException("Received a tombstone record to delete target assignment of " + groupId
                    + " but the assignment still has " + streamsGroup.targetAssignment().size() + " members.");
            }
            streamsGroup.setTargetAssignmentEpoch(-1);
        }
    }

    /**
     * Replays StreamsGroupTargetAssignmentMemberKey/Value to update the hard state of
     * the consumer group.
     * It updates the target assignment of the member or deletes it.
     *
     * @param key   A StreamsGroupTargetAssignmentMemberKey key.
     * @param value A StreamsGroupTargetAssignmentMemberValue record.
     */
    public void replay(
        StreamsGroupTargetAssignmentMemberKey key,
        StreamsGroupTargetAssignmentMemberValue value
    ) {
        String groupId = key.groupId();
        String memberId = key.memberId();

        if (value != null) {
            StreamsGroup streamsGroup = getOrMaybeCreatePersistedStreamsGroup(groupId, true);
            streamsGroup.updateTargetAssignment(memberId, org.apache.kafka.coordinator.group.streams.TasksTuple.fromTargetAssignmentRecord(value));
        } else {
            StreamsGroup streamsGroup;
            try {
                streamsGroup = getOrMaybeCreatePersistedStreamsGroup(groupId, false);
            } catch (GroupIdNotFoundException ex) {
                // If the group does not exist, we can ignore the tombstone.
                return;
            }
            streamsGroup.removeTargetAssignment(memberId);
        }
    }

    /**
     * Replays StreamsGroupCurrentMemberAssignmentKey/Value to update the hard state of
     * the consumer group.
     * It updates the assignment of a member or deletes it.
     *
     * @param key   A StreamsGroupCurrentMemberAssignmentKey key.
     * @param value A StreamsGroupCurrentMemberAssignmentValue record.
     */
    public void replay(
        StreamsGroupCurrentMemberAssignmentKey key,
        StreamsGroupCurrentMemberAssignmentValue value
    ) {
        String groupId = key.groupId();
        String memberId = key.memberId();

        if (value != null) {
            StreamsGroup streamsGroup = getOrMaybeCreatePersistedStreamsGroup(groupId, true);
            StreamsGroupMember oldMember = streamsGroup.getOrCreateUninitializedMember(memberId);
            StreamsGroupMember newMember = new StreamsGroupMember.Builder(oldMember)
                .updateWith(value)
                .build();
            streamsGroup.updateMember(newMember);
        } else {
            StreamsGroup streamsGroup;
            try {
                streamsGroup = getOrMaybeCreatePersistedStreamsGroup(groupId, false);
            } catch (GroupIdNotFoundException ex) {
                // If the group does not exist, we can ignore the tombstone.
                return;
            }

            StreamsGroupMember oldMember;
            try {
                oldMember = streamsGroup.getMemberOrThrow(memberId);
            } catch (UnknownMemberIdException ex) {
                // If the member does not exist, we can ignore the tombstone.
                return;
            }

            StreamsGroupMember newMember = new StreamsGroupMember.Builder(oldMember)
                .setMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH)
                .setPreviousMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH)
                .setAssignedTasks(TasksTupleWithEpochs.EMPTY)
                .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
                .build();
            streamsGroup.updateMember(newMember);
        }
    }

    /**
     * Replays ShareGroupTargetAssignmentMemberKey/Value to update the hard state of
     * the share group. It updates the target assignment of the member or deletes it.
     *
     * @param key   A ShareGroupTargetAssignmentMemberKey key.
     * @param value A ShareGroupTargetAssignmentMemberValue record.
     */
    public void replay(
        ShareGroupTargetAssignmentMemberKey key,
        ShareGroupTargetAssignmentMemberValue value
    ) {
        String groupId = key.groupId();
        String memberId = key.memberId();

        ShareGroup group;
        try {
            group = getOrMaybeCreatePersistedShareGroup(groupId, value != null);
        } catch (GroupIdNotFoundException ex) {
            log.debug("ShareGroupTargetAssignmentMember tombstone without group - {}", groupId, ex);
            return;
        }

        if (value != null) {
            group.updateTargetAssignment(memberId, Assignment.fromRecord(value));
        } else {
            group.removeTargetAssignment(memberId);
        }
    }

    /**
     * Replays ShareGroupTargetAssignmentMetadataKey/Value to update the hard state of
     * the share group. It updates the target assignment epoch or set it to -1 to signal
     * that it has been deleted.
     *
     * @param key   A ShareGroupTargetAssignmentMetadataKey key.
     * @param value A ShareGroupTargetAssignmentMetadataValue record.
     */
    public void replay(
        ShareGroupTargetAssignmentMetadataKey key,
        ShareGroupTargetAssignmentMetadataValue value
    ) {
        String groupId = key.groupId();

        ShareGroup group;
        try {
            group = getOrMaybeCreatePersistedShareGroup(groupId, value != null);
        } catch (GroupIdNotFoundException ex) {
            log.debug("ShareGroupTargetAssignmentMetadata tombstone without group - {}", groupId, ex);
            return;
        }

        if (value != null) {
            group.setTargetAssignmentEpoch(value.assignmentEpoch());
        } else {
            if (!group.targetAssignment().isEmpty()) {
                throw new IllegalStateException("Received a tombstone record to delete target assignment of " + groupId
                        + " but the assignment still has " + group.targetAssignment().size() + " members.");
            }
            group.setTargetAssignmentEpoch(-1);
        }
    }

    /**
     * Replays ShareGroupCurrentMemberAssignmentKey/Value to update the hard state of
     * the share group. It updates the assignment of a member or deletes it.
     *
     * @param key   A ShareGroupCurrentMemberAssignmentKey key.
     * @param value A ShareGroupCurrentMemberAssignmentValue record.
     */
    public void replay(
        ShareGroupCurrentMemberAssignmentKey key,
        ShareGroupCurrentMemberAssignmentValue value
    ) {
        String groupId = key.groupId();
        String memberId = key.memberId();

        ShareGroup group;
        ShareGroupMember oldMember;

        try {
            group = getOrMaybeCreatePersistedShareGroup(groupId, value != null);
            oldMember = group.getOrMaybeCreateMember(memberId, value != null);
        } catch (GroupIdNotFoundException ex) {
            log.debug("ShareGroupCurrentMemberAssignment tombstone without group - {}", groupId, ex);
            return;
        } catch (UnknownMemberIdException ex) {
            log.debug("ShareGroupCurrentMemberAssignment tombstone for groupId - {} without member - {}", groupId, memberId, ex);
            return;
        }

        if (value != null) {
            ShareGroupMember newMember = new ShareGroupMember.Builder(oldMember)
                .updateWith(value)
                .build();
            group.updateMember(newMember);
        } else {
            ShareGroupMember newMember = new ShareGroupMember.Builder(oldMember)
                .setMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH)
                .setPreviousMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH)
                .setAssignedPartitions(Map.of())
                .build();
            group.updateMember(newMember);
        }
    }

    /**
     * Replays ShareGroupStatePartitionMetadataKey/Value to update the hard state of
     * the share group.
     *
     * @param key   A ShareGroupStatePartitionMetadataKey key.
     * @param value A ShareGroupStatePartitionMetadataValue record.
     */
    public void replay(
        ShareGroupStatePartitionMetadataKey key,
        ShareGroupStatePartitionMetadataValue value
    ) {
        String groupId = key.groupId();

        // Update timeline structures with info about initialized/deleted topics.
        try {
            getOrMaybeCreatePersistedShareGroup(groupId, value != null);
        } catch (GroupIdNotFoundException ex) {
            // Ignore tombstone if group not found.
            log.debug("ShareGroupStatePartitionMetadata tombstone for non-existent share group {}", groupId, ex);
        }

        if (value == null) {
            shareGroupStatePartitionMetadata.remove(groupId);   // Should not throw any exceptions.
        } else {
            long timestamp = time.milliseconds();
            ShareGroupStatePartitionMetadataInfo info = new ShareGroupStatePartitionMetadataInfo(
                value.initializingTopics().stream()
                    .map(topicPartitionInfo -> Map.entry(
                        topicPartitionInfo.topicId(),
                        new InitMapValue(topicPartitionInfo.topicName(), new HashSet<>(topicPartitionInfo.partitions()), timestamp)))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)),

                value.initializedTopics().stream()
                    .map(topicPartitionInfo -> Map.entry(
                        topicPartitionInfo.topicId(),
                        new InitMapValue(topicPartitionInfo.topicName(), new HashSet<>(topicPartitionInfo.partitions()), timestamp)))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)),

                value.deletingTopics().stream()
                    .map(ShareGroupStatePartitionMetadataValue.TopicInfo::topicId)
                    .collect(Collectors.toSet())
            );

            // Init java record.
            shareGroupStatePartitionMetadata.put(groupId, info);
        }
    }

    /**
     * A new metadata image is available.
     *
     * @param newImage  The new metadata image.
     * @param delta     The delta image.
     */
    public void onNewMetadataImage(CoordinatorMetadataImage newImage, CoordinatorMetadataDelta delta) {
        metadataImage = newImage;

        // Initialize the last version if it was not yet.
        if (lastMetadataImageWithNewTopics == -1L) {
            lastMetadataImageWithNewTopics = metadataImage.version();
        }

        // Updated the last version of the image with newly created topics. This is used to
        // trigger a refresh of all the regular expressions when topics are created. Note
        // that we don't trigger a refresh when topics are deleted. Those are removed from
        // the subscription metadata (and the assignment) via the above mechanism. The
        // resolved regular expressions are cleaned up on the next refresh.
        if (!delta.createdTopicIds().isEmpty()) {
            lastMetadataImageWithNewTopics = metadataImage.version();
        }

        // Notify all the groups subscribed to the created, updated or
        // deleted topics.
        Set<String> allGroupIds = new HashSet<>();
        delta.changedTopicIds().forEach(topicId ->
            metadataImage.topicMetadata(topicId).ifPresent(topicMetadata -> {
                // Remove topic hash from the cache to recalculate it.
                topicHashCache.remove(topicMetadata.name());
                allGroupIds.addAll(groupsSubscribedToTopic(topicMetadata.name()));
            }));
        delta.deletedTopicIds().forEach(topicId ->
            delta.image().topicMetadata(topicId).ifPresent(topicMetadata -> {
                topicHashCache.remove(topicMetadata.name());
                allGroupIds.addAll(groupsSubscribedToTopic(topicMetadata.name()));
            }));
        allGroupIds.forEach(groupId -> {
            Group group = groups.get(groupId);
            if (group != null) {
                group.requestMetadataRefresh();
            }
        });
    }

    /**
     * Counts and updates the number of classic and consumer groups in different states.
     */
    public void updateGroupSizeCounter() {
        Map<ClassicGroupState, Long> classicGroupSizeCounter = new HashMap<>();
        Map<ConsumerGroup.ConsumerGroupState, Long> consumerGroupSizeCounter = new HashMap<>();
        Map<StreamsGroup.StreamsGroupState, Long> streamsGroupSizeCounter = new HashMap<>();
        Map<ShareGroup.ShareGroupState, Long> shareGroupSizeCounter = new HashMap<>();
        groups.forEach((__, group) -> {
            switch (group.type()) {
                case CLASSIC:
                    classicGroupSizeCounter.compute(((ClassicGroup) group).currentState(), Utils::incValue);
                    break;
                case CONSUMER:
                    consumerGroupSizeCounter.compute(((ConsumerGroup) group).state(), Utils::incValue);
                    break;
                case STREAMS:
                    streamsGroupSizeCounter.compute(((StreamsGroup) group).state(), Utils::incValue);
                    break;
                case SHARE:
                    shareGroupSizeCounter.compute(((ShareGroup) group).state(), Utils::incValue);
                    break;
                default:
                    break;
            }
        });
        metrics.setClassicGroupGauges(classicGroupSizeCounter);
        metrics.setConsumerGroupGauges(consumerGroupSizeCounter);
        metrics.setStreamsGroupGauges(streamsGroupSizeCounter);
        metrics.setShareGroupGauges(shareGroupSizeCounter);
    }

    /**
     * The coordinator has been loaded. Session timeouts are registered
     * for all members.
     */
    public void onLoaded() {
        Map<ClassicGroupState, Long> classicGroupSizeCounter = new HashMap<>();
        groups.forEach((groupId, group) -> {
            switch (group.type()) {
                case STREAMS:
                    StreamsGroup streamsGroup = (StreamsGroup) group;
                    log.info("Loaded streams group {} with {} members.", groupId, streamsGroup.members().size());
                    streamsGroup.members().forEach((memberId, member) -> {
                        log.debug("Loaded member {} in streams group {}.", memberId, groupId);
                        scheduleStreamsGroupSessionTimeout(groupId, memberId);
                        if (member.state() == org.apache.kafka.coordinator.group.streams.MemberState.UNREVOKED_TASKS) {
                            scheduleStreamsGroupRebalanceTimeout(
                                groupId,
                                member.memberId(),
                                member.memberEpoch(),
                                member.rebalanceTimeoutMs()
                            );
                        }
                    });
                    break;

                case CONSUMER:
                    ConsumerGroup consumerGroup = (ConsumerGroup) group;
                    log.info("Loaded consumer group {} with {} members.", groupId, consumerGroup.members().size());
                    consumerGroup.members().forEach((memberId, member) -> {
                        log.debug("Loaded member {} in consumer group {}.", memberId, groupId);
                        scheduleConsumerGroupSessionTimeout(groupId, memberId);
                        if (member.state() == MemberState.UNREVOKED_PARTITIONS) {
                            scheduleConsumerGroupRebalanceTimeout(
                                groupId,
                                member.memberId(),
                                member.memberEpoch(),
                                member.rebalanceTimeoutMs()
                            );
                        }
                    });
                    break;

                case CLASSIC:
                    ClassicGroup classicGroup = (ClassicGroup) group;
                    log.info("Loaded classic group {} with {} members.", groupId, classicGroup.allMembers().size());
                    classicGroup.allMembers().forEach(member -> {
                        log.debug("Loaded member {} in classic group {}.", member.memberId(), groupId);
                        rescheduleClassicGroupMemberHeartbeat(classicGroup, member);
                    });

                    if (classicGroup.numMembers() > config.classicGroupMaxSize()) {
                        // In case the max size config has changed.
                        prepareRebalance(classicGroup, "Freshly-loaded group " + groupId +
                            " (size " + classicGroup.numMembers() + ") is over capacity " + config.classicGroupMaxSize() +
                            ". Rebalancing in order to give a chance for consumers to commit offsets");
                    }

                    classicGroupSizeCounter.compute(classicGroup.currentState(), Utils::incValue);
                    break;

                case SHARE:
                    ShareGroup shareGroup = (ShareGroup) group;
                    log.info("Loaded share group {} with {} members.", groupId, shareGroup.members().size());
                    shareGroup.members().forEach((memberId, member) -> {
                        log.debug("Loaded member {} in share group {}.", memberId, groupId);
                        scheduleShareGroupSessionTimeout(groupId, memberId);
                    });
                    break;

                default:
                    log.warn("Loaded group {} with an unknown group type {}.", groupId, group.type());
                    break;
            }
        });
        metrics.setClassicGroupGauges(classicGroupSizeCounter);
    }

    /**
     * Called when the partition is unloaded.
     * ClassicGroup: Complete all awaiting join and sync futures. Transition group to Dead.
     */
    public void onUnloaded() {
        groups.values().forEach(group -> {
            switch (group.type()) {
                case STREAMS:
                    StreamsGroup streamsGroup = (StreamsGroup) group;
                    log.info("[GroupId={}] Unloaded group metadata for group epoch {}.",
                        streamsGroup.groupId(), streamsGroup.groupEpoch());
                    break;
                case CONSUMER:
                    ConsumerGroup consumerGroup = (ConsumerGroup) group;
                    log.info("[GroupId={}] Unloaded group metadata for group epoch {}.",
                        consumerGroup.groupId(), consumerGroup.groupEpoch());
                    break;
                case CLASSIC:
                    ClassicGroup classicGroup = (ClassicGroup) group;
                    log.info("[GroupId={}] Unloading group metadata for generation {}.",
                        classicGroup.groupId(), classicGroup.generationId());

                    classicGroup.transitionTo(DEAD);
                    switch (classicGroup.previousState()) {
                        case EMPTY:
                        case DEAD:
                            break;
                        case PREPARING_REBALANCE:
                            classicGroup.allMembers().forEach(member ->
                                classicGroup.completeJoinFuture(member, new JoinGroupResponseData()
                                    .setMemberId(member.memberId())
                                    .setErrorCode(NOT_COORDINATOR.code()))
                            );

                            break;
                        case COMPLETING_REBALANCE:
                        case STABLE:
                            classicGroup.allMembers().forEach(member ->
                                classicGroup.completeSyncFuture(member, new SyncGroupResponseData()
                                    .setErrorCode(NOT_COORDINATOR.code()))
                            );
                    }
                    break;
                case SHARE:
                    ShareGroup shareGroup = (ShareGroup) group;
                    log.info("[GroupId={}] Unloaded group metadata for group epoch {}.",
                        shareGroup.groupId(), shareGroup.groupEpoch());
                    break;

                default:
                    log.warn("onUnloaded group with an unknown group type {}.", group.type());
                    break;
            }
        });
    }

    public static String groupSessionTimeoutKey(String groupId, String memberId) {
        return "session-timeout-" + groupId + "-" + memberId;
    }

    public static String groupRebalanceTimeoutKey(String groupId, String memberId) {
        return "rebalance-timeout-" + groupId + "-" + memberId;
    }

    /**
     * Replays GroupMetadataKey/Value to update the soft state of
     * the classic group.
     *
     * @param key   A GroupMetadataKey key.
     * @param value A GroupMetadataValue record.
     */
    public void replay(
        GroupMetadataKey key,
        GroupMetadataValue value
    ) {
        String groupId = key.group();

        if (value == null)  {
            // Tombstone. Group should be removed.
            removeGroup(groupId);
        } else {
            List<ClassicGroupMember> loadedMembers = new ArrayList<>();
            for (GroupMetadataValue.MemberMetadata member : value.members()) {
                int rebalanceTimeout = member.rebalanceTimeout() == -1 ?
                    member.sessionTimeout() : member.rebalanceTimeout();

                JoinGroupRequestProtocolCollection supportedProtocols = new JoinGroupRequestProtocolCollection();
                supportedProtocols.add(new JoinGroupRequestProtocol()
                    .setName(value.protocol())
                    .setMetadata(member.subscription()));

                ClassicGroupMember loadedMember = new ClassicGroupMember(
                    member.memberId(),
                    Optional.ofNullable(member.groupInstanceId()),
                    member.clientId(),
                    member.clientHost(),
                    rebalanceTimeout,
                    member.sessionTimeout(),
                    value.protocolType(),
                    supportedProtocols,
                    member.assignment()
                );

                loadedMembers.add(loadedMember);
            }

            String protocolType = value.protocolType();

            ClassicGroup classicGroup = new ClassicGroup(
                this.logContext,
                groupId,
                loadedMembers.isEmpty() ? EMPTY : STABLE,
                time,
                value.generation(),
                protocolType == null || protocolType.isEmpty() ? Optional.empty() : Optional.of(protocolType),
                Optional.ofNullable(value.protocol()),
                Optional.ofNullable(value.leader()),
                value.currentStateTimestamp() == -1 ? Optional.empty() : Optional.of(value.currentStateTimestamp())
            );

            loadedMembers.forEach(member -> classicGroup.add(member, null));
            groups.put(groupId, classicGroup);

            classicGroup.setSubscribedTopics(
                classicGroup.computeSubscribedTopics()
            );
        }
    }

    /**
     * Handle a JoinGroupRequest.
     *
     * @param context        The request context.
     * @param request        The actual JoinGroup request.
     * @param responseFuture The join group response future.
     *
     * @return The result that contains records to append if the join group phase completes.
     */
    public CoordinatorResult<Void, CoordinatorRecord> classicGroupJoin(
        AuthorizableRequestContext context,
        JoinGroupRequestData request,
        CompletableFuture<JoinGroupResponseData> responseFuture
    ) {
        Group group = groups.get(request.groupId(), Long.MAX_VALUE);
        if (group != null) {
            if (group.type() == CONSUMER && !group.isEmpty()) {
                // classicGroupJoinToConsumerGroup takes the join requests to non-empty consumer groups.
                // The empty consumer groups should be converted to classic groups in classicGroupJoinToClassicGroup.
                return classicGroupJoinToConsumerGroup((ConsumerGroup) group, context, request, responseFuture);
            } else if (group.type() == CONSUMER || group.type() == CLASSIC || group.type() == STREAMS && group.isEmpty()) {
                // classicGroupJoinToClassicGroup accepts:
                // - classic groups
                // - empty streams groups
                // - empty consumer groups
                return classicGroupJoinToClassicGroup(context, request, responseFuture);
            } else {
                // Group exists but it's not a consumer group
                responseFuture.complete(new JoinGroupResponseData()
                    .setMemberId(UNKNOWN_MEMBER_ID)
                    .setErrorCode(Errors.INCONSISTENT_GROUP_PROTOCOL.code())
                );
                return EMPTY_RESULT;
            }
        } else {
            return classicGroupJoinToClassicGroup(context, request, responseFuture);
        }
    }

    /**
     * Handle a JoinGroupRequest to a ClassicGroup or a group to be created.
     *
     * @param context        The request context.
     * @param request        The actual JoinGroup request.
     * @param responseFuture The join group response future.
     *
     * @return The result that contains records to append if the join group phase completes.
     */
    CoordinatorResult<Void, CoordinatorRecord> classicGroupJoinToClassicGroup(
        AuthorizableRequestContext context,
        JoinGroupRequestData request,
        CompletableFuture<JoinGroupResponseData> responseFuture
    ) {
        CoordinatorResult<Void, CoordinatorRecord> result = EMPTY_RESULT;
        List<CoordinatorRecord> records = new ArrayList<>();

        String groupId = request.groupId();
        String memberId = request.memberId();

        boolean isUnknownMember = memberId.equals(UNKNOWN_MEMBER_ID);
        // Group is created if it does not exist and the member id is UNKNOWN. if member
        // is specified but group does not exist, request is rejected with GROUP_ID_NOT_FOUND
        ClassicGroup group;
        if (maybeDeleteEmptyConsumerGroup(groupId, records)) {
            log.info("[GroupId {}] Converted the empty consumer group to a classic group.", groupId);
        } else if (maybeDeleteEmptyStreamsGroup(groupId, records)) {
            log.info("[GroupId {}] Converted the empty streams group to a classic group.", groupId);
        }
        boolean isNewGroup = !groups.containsKey(groupId);
        try {
            group = getOrMaybeCreateClassicGroup(groupId, isUnknownMember);
        } catch (GroupIdNotFoundException t) {
            responseFuture.complete(new JoinGroupResponseData()
                .setMemberId(memberId)
                .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code())
            );
            return EMPTY_RESULT;
        }

        if (!acceptJoiningMember(group, memberId)) {
            group.remove(memberId);
            responseFuture.complete(new JoinGroupResponseData()
                .setMemberId(UNKNOWN_MEMBER_ID)
                .setErrorCode(Errors.GROUP_MAX_SIZE_REACHED.code())
            );
        } else if (isUnknownMember) {
            result = classicGroupJoinNewMember(
                context,
                request,
                group,
                responseFuture
            );
        } else {
            result = classicGroupJoinExistingMember(
                context,
                request,
                group,
                responseFuture
            );
        }

        if (isNewGroup && result == EMPTY_RESULT) {
            // If there are no records to append and if a group was newly created, we need to append
            // records to the log to commit the group to the timeline data structure.
            CompletableFuture<Void> appendFuture = new CompletableFuture<>();
            appendFuture.whenComplete((__, t) -> {
                if (t != null) {
                    // We failed to write the empty group metadata. This will revert the snapshot, removing
                    // the newly created group.
                    log.warn("Failed to write empty metadata for group {}: {}", group.groupId(), t.getMessage());

                    responseFuture.complete(new JoinGroupResponseData()
                        .setErrorCode(appendGroupMetadataErrorToResponseError(Errors.forException(t)).code()));
                }
            });

            records.add(
                GroupCoordinatorRecordHelpers.newEmptyGroupMetadataRecord(group)
            );

            return new CoordinatorResult<>(records, appendFuture, false);
        }
        return result;
    }

    /**
     * Attempt to complete join group phase. We do not complete
     * the join group phase if this is the initial rebalance.
     *
     * @param group The group.
     *
     * @return The coordinator result that will be appended to the log.
     */
    private CoordinatorResult<Void, CoordinatorRecord> maybeCompleteJoinPhase(ClassicGroup group) {
        if (!group.isInState(PREPARING_REBALANCE)) {
            log.debug("Cannot complete join phase of group {} because the group is in {} state.",
                group.groupId(), group.currentState());
            return EMPTY_RESULT;
        }

        if (group.previousState() == EMPTY) {
            log.debug("Cannot complete join phase of group {} because this is an initial rebalance.",
                group.groupId());
            return EMPTY_RESULT;
        }

        if (!group.hasAllMembersJoined()) {
            log.debug("Cannot complete join phase of group {} because not all the members have rejoined. " +
                "Members={}, AwaitingJoinResponses={}, PendingJoinMembers={}.",
                group.groupId(), group.numMembers(), group.numAwaitingJoinResponse(), group.numPendingJoinMembers());
            return EMPTY_RESULT;
        }

        return completeClassicGroupJoin(group);
    }

    /**
     * Handle a new member classic group join.
     *
     * @param context         The request context.
     * @param request         The join group request.
     * @param group           The group to add the member.
     * @param responseFuture  The response future to complete.
     *
     * @return The coordinator result that will be appended to the log.
     */
    private CoordinatorResult<Void, CoordinatorRecord> classicGroupJoinNewMember(
        AuthorizableRequestContext context,
        JoinGroupRequestData request,
        ClassicGroup group,
        CompletableFuture<JoinGroupResponseData> responseFuture
    ) {
        if (group.isInState(DEAD)) {
            // If the group is marked as dead, it means some other thread has just removed the group
            // from the coordinator metadata; it is likely that the group has migrated to some other
            // coordinator OR the group is in a transient unstable phase. Let the member retry
            // finding the correct coordinator and rejoin.
            responseFuture.complete(new JoinGroupResponseData()
                .setMemberId(UNKNOWN_MEMBER_ID)
                .setErrorCode(COORDINATOR_NOT_AVAILABLE.code())
            );
        } else if (!group.supportsProtocols(request.protocolType(), request.protocols())) {
            responseFuture.complete(new JoinGroupResponseData()
                .setMemberId(UNKNOWN_MEMBER_ID)
                .setErrorCode(Errors.INCONSISTENT_GROUP_PROTOCOL.code())
            );
        } else {
            Optional<String> groupInstanceId = Optional.ofNullable(request.groupInstanceId());
            String newMemberId = group.generateMemberId(context.clientId(), groupInstanceId);

            if (groupInstanceId.isPresent()) {
                return classicGroupJoinNewStaticMember(
                    context,
                    request,
                    group,
                    newMemberId,
                    responseFuture
                );
            } else {
                return classicGroupJoinNewDynamicMember(
                    context,
                    request,
                    group,
                    newMemberId,
                    responseFuture
                );
            }
        }

        return EMPTY_RESULT;
    }

    /**
     * Handle new static member join. If there was an existing member id for the group instance id,
     * replace that member. Otherwise, add the member and rebalance.
     *
     * @param context         The request context.
     * @param request         The join group request.
     * @param group           The group to add the member.
     * @param newMemberId     The newly generated member id.
     * @param responseFuture  The response future to complete.
     *
     * @return The coordinator result that will be appended to the log.
     */
    private CoordinatorResult<Void, CoordinatorRecord> classicGroupJoinNewStaticMember(
        AuthorizableRequestContext context,
        JoinGroupRequestData request,
        ClassicGroup group,
        String newMemberId,
        CompletableFuture<JoinGroupResponseData> responseFuture
    ) {
        String groupInstanceId = request.groupInstanceId();
        String existingMemberId = group.staticMemberId(groupInstanceId);
        if (existingMemberId != null) {
            log.info("Static member with groupInstanceId={} and unknown member id joins " +
                    "group {} in {} state. Replacing previously mapped member {} with this groupInstanceId.",
                groupInstanceId, group.groupId(), group.currentState(), existingMemberId);

            return updateStaticMemberThenRebalanceOrCompleteJoin(
                context,
                request,
                group,
                existingMemberId,
                newMemberId,
                responseFuture
            );
        } else {
            log.info("Static member with groupInstanceId={} and unknown member id joins " +
                    "group {} in {} state. Created a new member id {} for this member and added to the group.",
                groupInstanceId, group.groupId(), group.currentState(), newMemberId);

            return addMemberThenRebalanceOrCompleteJoin(context, request, group, newMemberId, responseFuture);
        }
    }

    /**
     * Handle a new dynamic member join. If the member id field is required, the group metadata manager
     * will add the new member id to the pending members and respond with MEMBER_ID_REQUIRED along with
     * the new member id for the client to join with.
     *
     * Otherwise, add the new member to the group and rebalance.
     *
     * @param context         The request context.
     * @param request         The join group request.
     * @param group           The group to add the member.
     * @param newMemberId     The newly generated member id.
     * @param responseFuture  The response future to complete.
     *
     * @return The coordinator result that will be appended to the log.
     */
    private CoordinatorResult<Void, CoordinatorRecord> classicGroupJoinNewDynamicMember(
        AuthorizableRequestContext context,
        JoinGroupRequestData request,
        ClassicGroup group,
        String newMemberId,
        CompletableFuture<JoinGroupResponseData> responseFuture
    ) {
        if (JoinGroupRequest.requiresKnownMemberId(context.requestVersion())) {
            // If member id required, register the member in the pending member list and send
            // back a response to call for another join group request with allocated member id.
            log.info("Dynamic member with unknown member id joins group {} in {} state. " +
                    "Created a new member id {} and requesting the member to rejoin with this id.",
                group.groupId(), group.currentState(), newMemberId);

            group.addPendingMember(newMemberId);
            String classicGroupHeartbeatKey = classicGroupHeartbeatKey(group.groupId(), newMemberId);

            timer.schedule(
                classicGroupHeartbeatKey,
                request.sessionTimeoutMs(),
                TimeUnit.MILLISECONDS,
                false,
                () -> expireClassicGroupMemberHeartbeat(group.groupId(), newMemberId)
            );

            responseFuture.complete(new JoinGroupResponseData()
                .setMemberId(newMemberId)
                .setErrorCode(Errors.MEMBER_ID_REQUIRED.code())
            );
        } else {
            log.info("Dynamic member with unknown member id joins group {} in state {}. " +
                    "Created a new member id {} and added the member to the group.",
                group.groupId(), group.currentState(), newMemberId);

            return addMemberThenRebalanceOrCompleteJoin(context, request, group, newMemberId, responseFuture);
        }

        return EMPTY_RESULT;
    }

    /**
     * Handle a join group request for an existing member.
     *
     * @param context         The request context.
     * @param request         The join group request.
     * @param group           The group to add the member.
     * @param responseFuture  The response future to complete.
     *
     * @return The coordinator result that will be appended to the log.
     */
    private CoordinatorResult<Void, CoordinatorRecord> classicGroupJoinExistingMember(
        AuthorizableRequestContext context,
        JoinGroupRequestData request,
        ClassicGroup group,
        CompletableFuture<JoinGroupResponseData> responseFuture
    ) {
        String memberId = request.memberId();
        String groupInstanceId = request.groupInstanceId();

        if (group.isInState(DEAD)) {
            // If the group is marked as dead, it means the group was recently removed the group
            // from the coordinator metadata; it is likely that the group has migrated to some other
            // coordinator OR the group is in a transient unstable phase. Let the member retry
            // finding the correct coordinator and rejoin.
            responseFuture.complete(new JoinGroupResponseData()
                .setMemberId(memberId)
                .setErrorCode(COORDINATOR_NOT_AVAILABLE.code())
            );
        } else if (!group.supportsProtocols(request.protocolType(), request.protocols())) {
            responseFuture.complete(new JoinGroupResponseData()
                .setMemberId(memberId)
                .setErrorCode(Errors.INCONSISTENT_GROUP_PROTOCOL.code())
            );
        } else if (group.isPendingMember(memberId)) {
            // A rejoining pending member will be accepted. Note that pending member cannot be a static member.
            if (groupInstanceId != null) {
                throw new IllegalStateException("Received unexpected JoinGroup with groupInstanceId=" +
                    groupInstanceId + " for pending member with memberId=" + memberId);
            }

            log.info("Pending dynamic member with id {} joins group {} in {} state. Adding to the group now.",
                memberId, group.groupId(), group.currentState());

            return addMemberThenRebalanceOrCompleteJoin(
                context,
                request,
                group,
                memberId,
                responseFuture
            );
        } else {
            try {
                group.validateMember(
                    memberId,
                    groupInstanceId,
                    "join-group"
                );
            } catch (KafkaException ex) {
                responseFuture.complete(new JoinGroupResponseData()
                    .setMemberId(memberId)
                    .setErrorCode(Errors.forException(ex).code())
                    .setProtocolType(null)
                    .setProtocolName(null)
                );
                return EMPTY_RESULT;
            }

            ClassicGroupMember member = group.member(memberId);
            if (group.isInState(PREPARING_REBALANCE)) {
                return updateMemberThenRebalanceOrCompleteJoin(
                    request,
                    group,
                    member,
                    "Member " + member.memberId() + " is joining group during " + group.stateAsString() +
                        "; client reason: " + JoinGroupRequest.joinReason(request),
                    responseFuture
                );
            } else if (group.isInState(COMPLETING_REBALANCE)) {
                if (member.matches(request.protocols())) {
                    // Member is joining with the same metadata (which could be because it failed to
                    // receive the initial JoinGroup response), so just return current group information
                    // for the current generation.
                    responseFuture.complete(new JoinGroupResponseData()
                        .setMembers(group.isLeader(memberId) ?
                            group.currentClassicGroupMembers() : List.of())
                        .setMemberId(memberId)
                        .setGenerationId(group.generationId())
                        .setProtocolName(group.protocolName().orElse(null))
                        .setProtocolType(group.protocolType().orElse(null))
                        .setLeader(group.leaderOrNull())
                        .setSkipAssignment(false)
                    );
                } else {
                    // Member has changed metadata, so force a rebalance
                    return updateMemberThenRebalanceOrCompleteJoin(
                        request,
                        group,
                        member,
                        "Updating metadata for member " + memberId + " during " + group.stateAsString() +
                            "; client reason: " + JoinGroupRequest.joinReason(request),
                        responseFuture
                    );
                }
            } else if (group.isInState(STABLE)) {
                if (group.isLeader(memberId)) {
                    // Force a rebalance if the leader sends JoinGroup;
                    // This allows the leader to trigger rebalances for changes affecting assignment
                    // which do not affect the member metadata (such as topic metadata changes for the consumer)
                    return updateMemberThenRebalanceOrCompleteJoin(
                        request,
                        group,
                        member,
                        "Leader " + memberId + " re-joining group during " + group.stateAsString() +
                            "; client reason: " + JoinGroupRequest.joinReason(request),
                        responseFuture
                    );
                } else if (!member.matches(request.protocols())) {
                    return updateMemberThenRebalanceOrCompleteJoin(
                        request,
                        group,
                        member,
                        "Updating metadata for member " + memberId + " during " + group.stateAsString() +
                            "; client reason: " + JoinGroupRequest.joinReason(request),
                        responseFuture
                    );
                } else {
                    // For followers with no actual change to their metadata, just return group information
                    // for the current generation which will allow them to issue SyncGroup.
                    responseFuture.complete(new JoinGroupResponseData()
                        .setMembers(List.of())
                        .setMemberId(memberId)
                        .setGenerationId(group.generationId())
                        .setProtocolName(group.protocolName().orElse(null))
                        .setProtocolType(group.protocolType().orElse(null))
                        .setLeader(group.leaderOrNull())
                        .setSkipAssignment(false)
                    );
                }
            } else {
                // Group reached unexpected (Empty) state. Let the joining member reset their generation and rejoin.
                log.warn("Attempt to add rejoining member {} of group {} in unexpected group state {}",
                    memberId, group.groupId(), group.stateAsString());

                responseFuture.complete(new JoinGroupResponseData()
                    .setMemberId(memberId)
                    .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code())
                );
            }
        }

        return EMPTY_RESULT;
    }

    /**
     * An overload of {@link GroupMetadataManager#completeClassicGroupJoin(ClassicGroup)} used as
     * timeout operation. It additionally looks up the group by the id and checks the group type.
     * completeClassicGroupJoin will only be called if the group is CLASSIC.
     */
    private CoordinatorResult<Void, CoordinatorRecord> completeClassicGroupJoin(String groupId) {
        ClassicGroup group;
        try {
            group = getOrMaybeCreateClassicGroup(groupId, false);
        } catch (GroupIdNotFoundException exception) {
            log.debug("Cannot find the group, skipping rebalance stage.", exception);
            return EMPTY_RESULT;
        }
        return completeClassicGroupJoin(group);
    }

    /**
     * Complete the join group phase. Remove all dynamic members that have not rejoined
     * during this stage and proceed with the next generation for this group. The generation id
     * is incremented and the group transitions to CompletingRebalance state if there is at least
     * one member.
     *
     * If the group is in Empty state, append a new group metadata record to the log. Otherwise,
     * complete all members' join group response futures and wait for sync requests from members.
     *
     * @param group The group that is completing the join group phase.
     *
     * @return The coordinator result that will be appended to the log.
     */
    private CoordinatorResult<Void, CoordinatorRecord> completeClassicGroupJoin(
        ClassicGroup group
    ) {
        timer.cancel(classicGroupJoinKey(group.groupId()));
        String groupId = group.groupId();

        Map<String, ClassicGroupMember> notYetRejoinedDynamicMembers =
            group.notYetRejoinedMembers().entrySet().stream()
                .filter(entry -> !entry.getValue().isStaticMember())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        if (!notYetRejoinedDynamicMembers.isEmpty()) {
            notYetRejoinedDynamicMembers.values().forEach(failedMember -> {
                group.remove(failedMember.memberId());
                timer.cancel(classicGroupHeartbeatKey(group.groupId(), failedMember.memberId()));
            });

            log.info("Group {} removed dynamic members who haven't joined: {}",
                groupId, notYetRejoinedDynamicMembers.keySet());
        }

        if (group.isInState(DEAD)) {
            log.info("Group {} is dead, skipping rebalance stage.", groupId);
        } else if (!group.maybeElectNewJoinedLeader() && !group.allMembers().isEmpty()) {
            // If all members are not rejoining, we will postpone the completion
            // of rebalance preparing stage, and send out another delayed operation
            // until session timeout removes all the non-responsive members.
            log.error("Group {} could not complete rebalance because no members rejoined.", groupId);

            timer.schedule(
                classicGroupJoinKey(groupId),
                group.rebalanceTimeoutMs(),
                TimeUnit.MILLISECONDS,
                false,
                () -> completeClassicGroupJoin(group.groupId())
            );

            return EMPTY_RESULT;
        } else {
            group.initNextGeneration();
            if (group.isInState(EMPTY)) {
                log.info("Group {} with generation {} is now empty.", groupId, group.generationId());

                CompletableFuture<Void> appendFuture = new CompletableFuture<>();
                appendFuture.whenComplete((__, t) -> {
                    if (t != null) {
                        // We failed to write the empty group metadata. If the broker fails before another rebalance,
                        // the previous generation written to the log will become active again (and most likely timeout).
                        // This should be safe since there are no active members in an empty generation, so we just warn.
                        Errors error = appendGroupMetadataErrorToResponseError(Errors.forException(t));
                        log.warn("Failed to write empty metadata for group {}: {}", group.groupId(), error.message());
                    }
                });

                List<CoordinatorRecord> records = List.of(GroupCoordinatorRecordHelpers.newGroupMetadataRecord(
                    group, Map.of()));

                return new CoordinatorResult<>(records, appendFuture, false);

            } else {
                log.info("Stabilized group {} generation {} with {} members.",
                    groupId, group.generationId(), group.numMembers());

                // Complete the awaiting join group response future for all the members after rebalancing
                group.allMembers().forEach(member -> {
                    List<JoinGroupResponseData.JoinGroupResponseMember> members = List.of();
                    if (group.isLeader(member.memberId())) {
                        members = group.currentClassicGroupMembers();
                    }

                    JoinGroupResponseData response = new JoinGroupResponseData()
                        .setMembers(members)
                        .setMemberId(member.memberId())
                        .setGenerationId(group.generationId())
                        .setProtocolName(group.protocolName().orElse(null))
                        .setProtocolType(group.protocolType().orElse(null))
                        .setLeader(group.leaderOrNull())
                        .setSkipAssignment(false)
                        .setErrorCode(Errors.NONE.code());

                    group.completeJoinFuture(member, response);
                    rescheduleClassicGroupMemberHeartbeat(group, member);
                    member.setIsNew(false);

                    group.addPendingSyncMember(member.memberId());
                });

                schedulePendingSync(group);
            }
        }

        return EMPTY_RESULT;
    }

    /**
     * Wait for sync requests for the group.
     *
     * @param group The group.
     */
    private void schedulePendingSync(ClassicGroup group) {
        timer.schedule(
            classicGroupSyncKey(group.groupId()),
            group.rebalanceTimeoutMs(),
            TimeUnit.MILLISECONDS,
            false,
            () -> expirePendingSync(group.groupId(), group.generationId()));
    }

    /**
     * Invoked when the heartbeat operation is expired from the timer. Possibly remove the member and
     * try complete the join phase.
     *
     * @param groupId   The group id.
     * @param memberId  The member id.
     *
     * @return The coordinator result that will be appended to the log.
     */
    private CoordinatorResult<Void, CoordinatorRecord> expireClassicGroupMemberHeartbeat(
        String groupId,
        String memberId
    ) {
        ClassicGroup group;
        try {
            group = getOrMaybeCreateClassicGroup(groupId, false);
        } catch (GroupIdNotFoundException exception) {
            log.debug("Received notification of heartbeat expiration for member {} after group {} " +
                "had already been deleted or upgraded.", memberId, groupId);
            return EMPTY_RESULT;
        }

        if (group.isInState(DEAD)) {
            log.info("Received notification of heartbeat expiration for member {} after group {} " +
                    "had already been unloaded or deleted.",
                memberId, group.groupId());
        } else if (group.isPendingMember(memberId)) {
            log.info("Pending member {} in group {} has been removed after session timeout expiration.",
                memberId, group.groupId());

            return removePendingMemberAndUpdateClassicGroup(group, memberId);
        } else if (!group.hasMember(memberId)) {
            log.debug("Member {} has already been removed from the group.", memberId);
        } else {
            ClassicGroupMember member = group.member(memberId);
            if (!member.hasSatisfiedHeartbeat()) {
                log.info("Member {} in group {} has failed, removing it from the group.",
                    member.memberId(), group.groupId());

                return removeMemberAndUpdateClassicGroup(
                    group,
                    member,
                    "removing member " + member.memberId() + " on heartbeat expiration."
                );
            }
        }
        return EMPTY_RESULT;
    }

    /**
     * Invoked when the heartbeat key is expired from the timer. Possibly remove the member
     * from the group and try to complete the join phase.
     *
     * @param group     The group.
     * @param member    The member.
     * @param reason    The reason for removing the member.
     *
     * @return The coordinator result that will be appended to the log.
     */
    private CoordinatorResult<Void, CoordinatorRecord> removeMemberAndUpdateClassicGroup(
        ClassicGroup group,
        ClassicGroupMember member,
        String reason
    ) {
        // New members may timeout with a pending JoinGroup while the group is still rebalancing, so we have
        // to invoke the response future before removing the member. We return UNKNOWN_MEMBER_ID so
        // that the consumer will retry the JoinGroup request if it is still active.
        group.completeJoinFuture(member, new JoinGroupResponseData()
            .setMemberId(UNKNOWN_MEMBER_ID)
            .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code())
        );
        group.remove(member.memberId());

        if (group.isInState(STABLE) || group.isInState(COMPLETING_REBALANCE)) {
            return maybePrepareRebalanceOrCompleteJoin(group, reason);
        } else if (group.isInState(PREPARING_REBALANCE) && group.hasAllMembersJoined()) {
            return completeClassicGroupJoin(group);
        }

        return EMPTY_RESULT;
    }

    /**
     * Remove a pending member from the group and possibly complete the join phase.
     *
     * @param group     The group.
     * @param memberId  The member id.
     *
     * @return The coordinator result that will be appended to the log.
     */
    private CoordinatorResult<Void, CoordinatorRecord> removePendingMemberAndUpdateClassicGroup(
        ClassicGroup group,
        String memberId
    ) {
        group.remove(memberId);

        if (group.isInState(PREPARING_REBALANCE) && group.hasAllMembersJoined()) {
            return completeClassicGroupJoin(group);
        }

        return EMPTY_RESULT;
    }

    /**
     * Update an existing member. Then begin a rebalance or complete the join phase.
     *
     * @param request         The join group request.
     * @param group           The group to add the member.
     * @param member          The member.
     * @param joinReason      The client reason for the join request.
     * @param responseFuture  The response future to complete.
     *
     * @return The coordinator result that will be appended to the log.
     */
    private CoordinatorResult<Void, CoordinatorRecord> updateMemberThenRebalanceOrCompleteJoin(
        JoinGroupRequestData request,
        ClassicGroup group,
        ClassicGroupMember member,
        String joinReason,
        CompletableFuture<JoinGroupResponseData> responseFuture
    ) {
        group.updateMember(
            member,
            request.protocols(),
            request.rebalanceTimeoutMs(),
            request.sessionTimeoutMs(),
            responseFuture
        );

        return maybePrepareRebalanceOrCompleteJoin(group, joinReason);
    }

    /**
     * Add a member then rebalance or complete join.
     *
     * @param context         The request context.
     * @param request         The join group request.
     * @param group           The group to add the member.
     * @param memberId        The member id.
     * @param responseFuture  The response future to complete.
     *
     * @return The coordinator result that will be appended to the log.
     */
    private CoordinatorResult<Void, CoordinatorRecord> addMemberThenRebalanceOrCompleteJoin(
        AuthorizableRequestContext context,
        JoinGroupRequestData request,
        ClassicGroup group,
        String memberId,
        CompletableFuture<JoinGroupResponseData> responseFuture
    ) {
        Optional<String> groupInstanceId = Optional.ofNullable(request.groupInstanceId());
        ClassicGroupMember member = new ClassicGroupMember(
            memberId,
            groupInstanceId,
            context.clientId(),
            context.clientAddress().toString(),
            request.rebalanceTimeoutMs(),
            request.sessionTimeoutMs(),
            request.protocolType(),
            request.protocols()
        );

        member.setIsNew(true);

        // Update the newMemberAdded flag to indicate that the initial rebalance can be further delayed
        if (group.isInState(PREPARING_REBALANCE) && group.previousState() == EMPTY) {
            group.setNewMemberAdded(true);
        }

        group.add(member, responseFuture);

        // The session timeout does not affect new members since they do not have their memberId and
        // cannot send heartbeats. Furthermore, we cannot detect disconnects because sockets are muted
        // while the JoinGroup request is parked. If the client does disconnect (e.g. because of a request
        // timeout during a long rebalance), they may simply retry which will lead to a lot of defunct
        // members in the rebalance. To prevent this going on indefinitely, we time out JoinGroup requests
        // for new members. If the new member is still there, we expect it to retry.
        rescheduleClassicGroupMemberHeartbeat(group, member, config.classicGroupNewMemberJoinTimeoutMs());

        return maybePrepareRebalanceOrCompleteJoin(group, "Adding new member " + memberId + " with group instance id " +
            request.groupInstanceId() + "; client reason: " + JoinGroupRequest.joinReason(request));
    }

    /**
     * Prepare a rebalance if the group is in a valid state. Otherwise, try
     * to complete the join phase.
     *
     * @param group           The group to rebalance.
     * @param reason          The reason for the rebalance.
     *
     * @return The coordinator result that will be appended to the log.
     */
    private CoordinatorResult<Void, CoordinatorRecord> maybePrepareRebalanceOrCompleteJoin(
        ClassicGroup group,
        String reason
    ) {
        if (group.canRebalance()) {
            return prepareRebalance(group, reason);
        } else {
            return maybeCompleteJoinPhase(group);
        }
    }

    /**
     * Prepare a rebalance.
     *
     * @param group           The group to rebalance.
     * @param reason          The reason for the rebalance.
     *
     * @return The coordinator result that will be appended to the log.
     *
     * Package private for testing.
     */
    CoordinatorResult<Void, CoordinatorRecord> prepareRebalance(
        ClassicGroup group,
        String reason
    ) {
        // If any members are awaiting sync, cancel their request and have them rejoin.
        if (group.isInState(COMPLETING_REBALANCE)) {
            resetAndPropagateAssignmentWithError(group, Errors.REBALANCE_IN_PROGRESS);
        }

        // If a sync expiration is pending, cancel it.
        removeSyncExpiration(group);

        boolean isInitialRebalance = group.isInState(EMPTY);
        if (isInitialRebalance) {
            // The group is new. Provide more time for the members to join.
            int delayMs = config.classicGroupInitialRebalanceDelayMs();
            int remainingMs = Math.max(group.rebalanceTimeoutMs() - config.classicGroupInitialRebalanceDelayMs(), 0);

            timer.schedule(
                classicGroupJoinKey(group.groupId()),
                delayMs,
                TimeUnit.MILLISECONDS,
                false,
                () -> tryCompleteInitialRebalanceElseSchedule(group.groupId(), delayMs, remainingMs)
            );
        }

        group.transitionTo(PREPARING_REBALANCE);

        log.info("Preparing to rebalance group {} in state {} with old generation {} (reason: {}).",
            group.groupId(), group.currentState(), group.generationId(), reason);

        return isInitialRebalance ? EMPTY_RESULT : maybeCompleteJoinElseSchedule(group);
    }

    /**
     * Try to complete the join phase. Otherwise, schedule a new join operation.
     *
     * @param group The group.
     *
     * @return The coordinator result that will be appended to the log.
     */
    private CoordinatorResult<Void, CoordinatorRecord> maybeCompleteJoinElseSchedule(
        ClassicGroup group
    ) {
        String classicGroupJoinKey = classicGroupJoinKey(group.groupId());
        if (group.hasAllMembersJoined()) {
            // All members have joined. Proceed to sync phase.
            return completeClassicGroupJoin(group);
        } else {
            timer.schedule(
                classicGroupJoinKey,
                group.rebalanceTimeoutMs(),
                TimeUnit.MILLISECONDS,
                false,
                () -> completeClassicGroupJoin(group.groupId())
            );
            return EMPTY_RESULT;
        }
    }

    /**
     * Try to complete the join phase of the initial rebalance.
     * Otherwise, extend the rebalance.
     *
     * @param groupId The group under initial rebalance.
     *
     * @return The coordinator result that will be appended to the log.
     */
    private CoordinatorResult<Void, CoordinatorRecord> tryCompleteInitialRebalanceElseSchedule(
        String groupId,
        int delayMs,
        int remainingMs
    ) {
        ClassicGroup group;
        try {
            group = getOrMaybeCreateClassicGroup(groupId, false);
        } catch (GroupIdNotFoundException exception) {
            log.debug("Cannot find the group, skipping the initial rebalance stage.", exception);
            return EMPTY_RESULT;
        }

        if (group.newMemberAdded() && remainingMs != 0) {
            // A new member was added. Extend the delay.
            group.setNewMemberAdded(false);
            int newDelayMs = Math.min(config.classicGroupInitialRebalanceDelayMs(), remainingMs);
            int newRemainingMs = Math.max(remainingMs - delayMs, 0);

            timer.schedule(
                classicGroupJoinKey(group.groupId()),
                newDelayMs,
                TimeUnit.MILLISECONDS,
                false,
                () -> tryCompleteInitialRebalanceElseSchedule(group.groupId(), newDelayMs, newRemainingMs)
            );
        } else {
            // No more time remaining. Complete the join phase.
            return completeClassicGroupJoin(group);
        }

        return EMPTY_RESULT;
    }

    /**
     * Reset assignment for all members and propagate the error to all members in the group.
     *
     * @param group  The group.
     * @param error  The error to propagate.
     */
    private void resetAndPropagateAssignmentWithError(ClassicGroup group, Errors error) {
        if (!group.isInState(COMPLETING_REBALANCE)) {
            throw new IllegalStateException("Group " + group.groupId() + " must be in " + COMPLETING_REBALANCE.name() +
                " state but is in " + group.currentState() + ".");
        }

        group.allMembers().forEach(member -> member.setAssignment(EMPTY_ASSIGNMENT));
        propagateAssignment(group, error);
    }

    /**
     * Sets assignment for group and propagate assignment and error to all members.
     *
     * @param group      The group.
     * @param assignment The assignment for all members.
     */
    private void setAndPropagateAssignment(ClassicGroup group, Map<String, byte[]> assignment) {
        if (!group.isInState(COMPLETING_REBALANCE)) {
            throw new IllegalStateException("The group must be in CompletingRebalance state " +
                "to set and propagate assignment.");
        }

        group.allMembers().forEach(member ->
            member.setAssignment(assignment.getOrDefault(member.memberId(), EMPTY_ASSIGNMENT)));

        propagateAssignment(group, Errors.NONE);
    }

    /**
     * Propagate assignment and error to all members.
     *
     * @param group  The group.
     * @param error  The error to propagate.
     */
    private void propagateAssignment(ClassicGroup group, Errors error) {
        Optional<String> protocolName = Optional.empty();
        Optional<String> protocolType = Optional.empty();
        if (error == Errors.NONE) {
            protocolName = group.protocolName();
            protocolType = group.protocolType();
        }

        for (ClassicGroupMember member : group.allMembers()) {
            if (!member.hasAssignment() && error == Errors.NONE) {
                log.warn("Sending empty assignment to member {} of {} for " + "generation {} with no errors",
                    member.memberId(), group.groupId(), group.generationId());
            }

            if (group.completeSyncFuture(member,
                new SyncGroupResponseData()
                    .setProtocolName(protocolName.orElse(null))
                    .setProtocolType(protocolType.orElse(null))
                    .setAssignment(member.assignment())
                    .setErrorCode(error.code()))) {

                // Reset the session timeout for members after propagating the member's assignment.
                // This is because if any member's session expired while we were still awaiting either
                // the leader sync group or the append future, its expiration will be ignored and no
                // future heartbeat expectations will not be scheduled.
                rescheduleClassicGroupMemberHeartbeat(group, member);
            }
        }
    }

    /**
     * Complete and schedule next heartbeat.
     *
     * @param group    The group.
     * @param member   The member.
     */
    public void rescheduleClassicGroupMemberHeartbeat(
        ClassicGroup group,
        ClassicGroupMember member
    ) {
        rescheduleClassicGroupMemberHeartbeat(group, member, member.sessionTimeoutMs());
    }

    /**
     * Reschedule the heartbeat.
     *
     * @param group      The group.
     * @param member     The member.
     * @param timeoutMs  The timeout for the new heartbeat.
     */
    private void rescheduleClassicGroupMemberHeartbeat(
        ClassicGroup group,
        ClassicGroupMember member,
        long timeoutMs
    ) {
        String classicGroupHeartbeatKey = classicGroupHeartbeatKey(group.groupId(), member.memberId());

        // Reschedule the next heartbeat expiration deadline
        timer.schedule(classicGroupHeartbeatKey,
            timeoutMs,
            TimeUnit.MILLISECONDS,
            false,
            () -> expireClassicGroupMemberHeartbeat(group.groupId(), member.memberId()));
    }

    /**
     * Remove the sync key from the timer and clear all pending sync members from the group.
     * Invoked when a new rebalance is triggered.
     *
     * @param group  The group.
     */
    private void removeSyncExpiration(ClassicGroup group) {
        group.clearPendingSyncMembers();
        timer.cancel(classicGroupSyncKey(group.groupId()));
    }

    /**
     * Expire pending sync.
     *
     * @param groupId         The group id.
     * @param generationId    The generation when the pending sync was originally scheduled.
     *
     * @return The coordinator result that will be appended to the log.
     * */
    private CoordinatorResult<Void, CoordinatorRecord> expirePendingSync(
        String groupId,
        int generationId
    ) {
        ClassicGroup group;
        try {
            group = getOrMaybeCreateClassicGroup(groupId, false);
        } catch (GroupIdNotFoundException exception) {
            log.debug("Received notification of sync expiration for an unknown classic group {}.", groupId);
            return EMPTY_RESULT;
        }

        if (generationId != group.generationId()) {
            log.error("Received unexpected notification of sync expiration for {} with an old " +
                "generation {} while the group has {}.", group.groupId(), generationId, group.generationId());
        } else {
            if (group.isInState(DEAD) || group.isInState(EMPTY) || group.isInState(PREPARING_REBALANCE)) {
                log.error("Received unexpected notification of sync expiration after group {} already " +
                    "transitioned to {} state.", group.groupId(), group.stateAsString());
            } else if (group.isInState(COMPLETING_REBALANCE) || group.isInState(STABLE)) {
                if (!group.hasReceivedSyncFromAllMembers()) {
                    Set<String> pendingSyncMembers = new HashSet<>(group.allPendingSyncMembers());
                    pendingSyncMembers.forEach(memberId -> {
                        group.remove(memberId);
                        timer.cancel(classicGroupHeartbeatKey(group.groupId(), memberId));
                    });

                    log.debug("Group {} removed members who haven't sent their sync requests: {}",
                        group.groupId(), pendingSyncMembers);

                    return prepareRebalance(group, "Removing " + pendingSyncMembers + " on pending sync request expiration");
                }
            }
        }
        return EMPTY_RESULT;
    }

    /**
     * Checks whether the group can accept a joining member.
     *
     * @param group      The group.
     * @param memberId   The member.
     *
     * @return whether the group can accept a joining member.
     */
    private boolean acceptJoiningMember(ClassicGroup group, String memberId) {
        switch (group.currentState()) {
            case EMPTY:
            case DEAD:
                // Always accept the request when the group is empty or dead
                return true;
            case PREPARING_REBALANCE:
                // An existing member is accepted if it is already awaiting. New members are accepted
                // up to the max group size. Note that the number of awaiting members is used here
                // for two reasons:
                // 1) the group size is not reliable as it could already be above the max group size
                //    if the max group size was reduced.
                // 2) using the number of awaiting members allows to kick out the last rejoining
                //    members of the group.
                return (group.hasMember(memberId) && group.member(memberId).isAwaitingJoin()) ||
                    group.numAwaitingJoinResponse() < config.classicGroupMaxSize();
            case COMPLETING_REBALANCE:
            case STABLE:
                // An existing member is accepted. New members are accepted up to the max group size.
                // Note that the group size is used here. When the group transitions to CompletingRebalance,
                // members who haven't rejoined are removed.
                return group.hasMember(memberId) || group.numMembers() < config.classicGroupMaxSize();
            default:
                throw new IllegalStateException("Unknown group state: " + group.stateAsString());
        }
    }

    /**
     * Update a static member then rebalance or complete join.
     *
     * @param context         The request context.
     * @param request         The join group request.
     * @param group           The group of the static member.
     * @param oldMemberId     The existing static member id.
     * @param newMemberId     The new joining static member id.
     * @param responseFuture  The response future to complete.
     *
     * @return The coordinator result that will be appended to the log.
     */
    private CoordinatorResult<Void, CoordinatorRecord> updateStaticMemberThenRebalanceOrCompleteJoin(
        AuthorizableRequestContext context,
        JoinGroupRequestData request,
        ClassicGroup group,
        String oldMemberId,
        String newMemberId,
        CompletableFuture<JoinGroupResponseData> responseFuture
    ) {
        String currentLeader = group.leaderOrNull();
        ClassicGroupMember newMember = group.replaceStaticMember(request.groupInstanceId(), oldMemberId, newMemberId);

        // Heartbeat of old member id will expire without effect since the group no longer contains that member id.
        // New heartbeat shall be scheduled with new member id.
        rescheduleClassicGroupMemberHeartbeat(group, newMember);

        int oldRebalanceTimeoutMs = newMember.rebalanceTimeoutMs();
        int oldSessionTimeoutMs = newMember.sessionTimeoutMs();
        JoinGroupRequestProtocolCollection oldProtocols = newMember.supportedProtocols();

        group.updateMember(
            newMember,
            request.protocols(),
            request.rebalanceTimeoutMs(),
            request.sessionTimeoutMs(),
            responseFuture
        );

        if (group.isInState(STABLE)) {
            // Check if group's selected protocol of next generation will change, if not, simply store group to persist
            // the updated static member, if yes, rebalance should be triggered to keep the group's assignment
            // and selected protocol consistent
            String groupInstanceId = request.groupInstanceId();
            String selectedProtocolForNextGeneration = group.selectProtocol();
            if (group.protocolName().orElse("").equals(selectedProtocolForNextGeneration)) {
                log.info("Static member which joins during Stable stage and doesn't affect " +
                    "the selected protocol will not trigger a rebalance.");

                Map<String, byte[]> groupAssignment = group.groupAssignment();
                CompletableFuture<Void> appendFuture = new CompletableFuture<>();
                appendFuture.whenComplete((__, t) -> {
                    if (t != null) {
                        log.warn("Failed to persist metadata for group {} static member {} with " +
                            "group instance id {} due to {}. Reverting to old member id {}.",
                            group.groupId(), newMemberId, groupInstanceId, t.getMessage(), oldMemberId);

                        // Failed to persist the member id of the given static member, revert the update of the static member in the group.
                        group.updateMember(newMember, oldProtocols, oldRebalanceTimeoutMs, oldSessionTimeoutMs, null);
                        ClassicGroupMember oldMember = group.replaceStaticMember(groupInstanceId, newMemberId, oldMemberId);
                        rescheduleClassicGroupMemberHeartbeat(group, oldMember);

                        responseFuture.complete(
                            new JoinGroupResponseData()
                                .setMemberId(UNKNOWN_MEMBER_ID)
                                .setGenerationId(group.generationId())
                                .setProtocolName(group.protocolName().orElse(null))
                                .setProtocolType(group.protocolType().orElse(null))
                                .setLeader(currentLeader)
                                .setSkipAssignment(false)
                                .setErrorCode(appendGroupMetadataErrorToResponseError(Errors.forException(t)).code()));

                    } else if (JoinGroupRequest.supportsSkippingAssignment(context.requestVersion())) {
                        boolean isLeader = group.isLeader(newMemberId);

                        group.completeJoinFuture(newMember, new JoinGroupResponseData()
                            .setMembers(isLeader ? group.currentClassicGroupMembers() : List.of())
                            .setMemberId(newMemberId)
                            .setGenerationId(group.generationId())
                            .setProtocolName(group.protocolName().orElse(null))
                            .setProtocolType(group.protocolType().orElse(null))
                            .setLeader(group.leaderOrNull())
                            .setSkipAssignment(isLeader)
                        );
                    } else {
                        group.completeJoinFuture(newMember, new JoinGroupResponseData()
                            .setMemberId(newMemberId)
                            .setGenerationId(group.generationId())
                            .setProtocolName(group.protocolName().orElse(null))
                            .setProtocolType(group.protocolType().orElse(null))
                            .setLeader(currentLeader)
                            .setSkipAssignment(false)
                        );
                    }
                });

                List<CoordinatorRecord> records = List.of(
                    GroupCoordinatorRecordHelpers.newGroupMetadataRecord(group, groupAssignment)
                );

                return new CoordinatorResult<>(records, appendFuture, false);
            } else {
                return maybePrepareRebalanceOrCompleteJoin(
                    group,
                    "Group's selectedProtocol will change because static member " +
                        newMember.memberId() + " with instance id " + groupInstanceId +
                        " joined with change of protocol; client reason: " + JoinGroupRequest.joinReason(request)
                );
            }
        } else if (group.isInState(COMPLETING_REBALANCE)) {
            // if the group is in after-sync stage, upon getting a new join-group of a known static member
            // we should still trigger a new rebalance, since the old member may already be sent to the leader
            // for assignment, and hence when the assignment gets back there would be a mismatch of the old member id
            // with the new replaced member id. As a result the new member id would not get any assignment.
            return prepareRebalance(
                group,
                "Updating metadata for static member " + newMember.memberId() + " with instance id " +
                    request.groupInstanceId() + "; client reason: " + JoinGroupRequest.joinReason(request)
            );
        } else if (group.isInState(EMPTY) || group.isInState(DEAD)) {
            throw new IllegalStateException("Group " + group.groupId() + " was not supposed to be in the state " +
                group.stateAsString() + " when the unknown static member " + request.groupInstanceId() + " rejoins.");

        }
        return maybeCompleteJoinPhase(group);
    }

    /**
     * Handle a SyncGroupRequest.
     *
     * @param context        The request context.
     * @param request        The actual SyncGroup request.
     * @param responseFuture The sync group response future.
     *
     * @return The result that contains records to append.
     */
    public CoordinatorResult<Void, CoordinatorRecord> classicGroupSync(
        AuthorizableRequestContext context,
        SyncGroupRequestData request,
        CompletableFuture<SyncGroupResponseData> responseFuture
    ) throws UnknownMemberIdException {
        Group group;
        try {
            group = group(request.groupId());
        } catch (GroupIdNotFoundException e) {
            responseFuture.complete(new SyncGroupResponseData()
                .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code()));
            return EMPTY_RESULT;
        }

        if (group.isEmpty()) {
            responseFuture.complete(new SyncGroupResponseData()
                .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code()));
            return EMPTY_RESULT;
        }

        if (group.type() == CLASSIC) {
            return classicGroupSyncToClassicGroup((ClassicGroup) group, context, request, responseFuture);
        } else if (group.type() == CONSUMER) {
            return classicGroupSyncToConsumerGroup((ConsumerGroup) group, context, request, responseFuture);
        } else {
            responseFuture.complete(new SyncGroupResponseData()
                .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code()));
            return EMPTY_RESULT;
        }
    }

    /**
     * Handle a SyncGroupRequest to a ClassicGroup.
     *
     * @param group          The ClassicGroup.
     * @param context        The request context.
     * @param request        The actual SyncGroup request.
     * @param responseFuture The sync group response future.
     *
     * @return The result that contains records to append if the group metadata manager received assignments.
     */
    private CoordinatorResult<Void, CoordinatorRecord> classicGroupSyncToClassicGroup(
        ClassicGroup group,
        AuthorizableRequestContext context,
        SyncGroupRequestData request,
        CompletableFuture<SyncGroupResponseData> responseFuture
    ) throws IllegalStateException {
        String groupId = request.groupId();
        String memberId = request.memberId();

        Optional<Errors> errorOpt = validateSyncGroup(group, request);
        if (errorOpt.isPresent()) {
            responseFuture.complete(new SyncGroupResponseData()
                .setErrorCode(errorOpt.get().code()));
        } else if (group.isInState(PREPARING_REBALANCE)) {
            responseFuture.complete(new SyncGroupResponseData()
                .setErrorCode(Errors.REBALANCE_IN_PROGRESS.code()));
        } else if (group.isInState(COMPLETING_REBALANCE)) {
            group.member(memberId).setAwaitingSyncFuture(responseFuture);
            removePendingSyncMember(group, request.memberId());

            // If this is the leader, then we can attempt to persist state and transition to stable
            if (group.isLeader(memberId)) {
                log.info("Assignment received from leader {} for group {} for generation {}. " +
                    "The group has {} members, {} of which are static.",
                    memberId, groupId, group.generationId(), group.numMembers(), group.allStaticMemberIds().size());

                // Fill all members with corresponding member assignment. If the member assignment
                // does not exist, fill with an empty assignment.
                Map<String, byte[]> assignment = new HashMap<>();
                request.assignments().forEach(memberAssignment ->
                    assignment.put(memberAssignment.memberId(), memberAssignment.assignment())
                );

                Map<String, byte[]> membersWithMissingAssignment = new HashMap<>();
                group.allMembers().forEach(member -> {
                    if (!assignment.containsKey(member.memberId())) {
                        membersWithMissingAssignment.put(member.memberId(), EMPTY_ASSIGNMENT);
                    }
                });
                assignment.putAll(membersWithMissingAssignment);

                if (!membersWithMissingAssignment.isEmpty()) {
                    log.warn("Setting empty assignments for members {} of {} for generation {}.",
                        membersWithMissingAssignment, groupId, group.generationId());
                }

                CompletableFuture<Void> appendFuture = new CompletableFuture<>();
                appendFuture.whenComplete((__, t) -> {
                    // Another member may have joined the group while we were awaiting this callback,
                    // so we must ensure we are still in the CompletingRebalance state and the same generation
                    // when it gets invoked. if we have transitioned to another state, then do nothing
                    if (group.isInState(COMPLETING_REBALANCE) && request.generationId() == group.generationId()) {
                        if (t != null) {
                            Errors error = appendGroupMetadataErrorToResponseError(Errors.forException(t));
                            resetAndPropagateAssignmentWithError(group, error);
                            maybePrepareRebalanceOrCompleteJoin(group, "Error " + error + " when storing group assignment" +
                                "during SyncGroup (member: " + memberId + ").");
                        } else {
                            // Update group's assignment and propagate to all members.
                            setAndPropagateAssignment(group, assignment);
                            group.transitionTo(STABLE);
                            metrics.record(CLASSIC_GROUP_COMPLETED_REBALANCES_SENSOR_NAME);
                        }
                    }
                });

                List<CoordinatorRecord> records = List.of(
                    GroupCoordinatorRecordHelpers.newGroupMetadataRecord(group, assignment)
                );
                return new CoordinatorResult<>(records, appendFuture, false);
            }
        } else if (group.isInState(STABLE)) {
            removePendingSyncMember(group, memberId);

            // If the group is stable, we just return the current assignment
            ClassicGroupMember member = group.member(memberId);
            responseFuture.complete(new SyncGroupResponseData()
                .setProtocolType(group.protocolType().orElse(null))
                .setProtocolName(group.protocolName().orElse(null))
                .setAssignment(member.assignment())
                .setErrorCode(Errors.NONE.code()));
        } else if (group.isInState(DEAD)) {
            throw new IllegalStateException("Reached unexpected condition for Dead group " + groupId);
        }

        return EMPTY_RESULT;
    }

    /**
     * Handle a SyncGroupRequest to a ConsumerGroup.
     *
     * @param group          The ConsumerGroup.
     * @param context        The request context.
     * @param request        The actual SyncGroup request.
     * @param responseFuture The sync group response future.
     *
     * @return The result that contains the appendFuture to return the response.
     */
    private CoordinatorResult<Void, CoordinatorRecord> classicGroupSyncToConsumerGroup(
        ConsumerGroup group,
        AuthorizableRequestContext context,
        SyncGroupRequestData request,
        CompletableFuture<SyncGroupResponseData> responseFuture
    ) throws UnknownMemberIdException, FencedInstanceIdException, IllegalGenerationException,
        InconsistentGroupProtocolException, RebalanceInProgressException, IllegalStateException {
        String groupId = request.groupId();
        String memberId = request.memberId();
        String instanceId = request.groupInstanceId();
        ConsumerGroupMember member = validateConsumerGroupMember(group, memberId, instanceId);

        throwIfMemberDoesNotUseClassicProtocol(member);
        throwIfGenerationIdUnmatched(member.memberId(), member.memberEpoch(), request.generationId());
        throwIfClassicProtocolUnmatched(member, request.protocolType(), request.protocolName());
        throwIfRebalanceInProgress(group, member);

        CompletableFuture<Void> appendFuture = new CompletableFuture<>();
        appendFuture.whenComplete((__, t) -> {
            if (t == null) {
                cancelConsumerGroupSyncTimeout(groupId, memberId);
                scheduleConsumerGroupSessionTimeout(groupId, memberId, member.classicProtocolSessionTimeout().get());

                responseFuture.complete(new SyncGroupResponseData()
                    .setProtocolType(request.protocolType())
                    .setProtocolName(request.protocolName())
                    .setAssignment(prepareAssignment(member)));
            }
        });

        return new CoordinatorResult<>(List.of(), appendFuture, false);
    }

    /**
     * Serializes the member's assigned partitions with ConsumerProtocol.
     *
     * @param member The ConsumerGroupMember.
     * @return The serialized assigned partitions.
     */
    private byte[] prepareAssignment(ConsumerGroupMember member) {
        try {
            return ConsumerProtocol.serializeAssignment(
                toConsumerProtocolAssignment(
                    member.assignedPartitions(),
                    metadataImage
                ),
                ConsumerProtocol.deserializeVersion(
                    ByteBuffer.wrap(member.classicMemberMetadata().get().supportedProtocols().iterator().next().metadata())
                )
            ).array();
        } catch (SchemaException e) {
            throw new IllegalStateException("Malformed embedded consumer protocol in version deserialization.");
        }
    }

    // Visible for testing
    static Errors appendGroupMetadataErrorToResponseError(Errors appendError) {
        switch (appendError) {
            case UNKNOWN_TOPIC_OR_PARTITION:
            case NOT_ENOUGH_REPLICAS:
            case REQUEST_TIMED_OUT:
                return COORDINATOR_NOT_AVAILABLE;

            case NOT_LEADER_OR_FOLLOWER:
            case KAFKA_STORAGE_ERROR:
                return NOT_COORDINATOR;

            case MESSAGE_TOO_LARGE:
            case RECORD_LIST_TOO_LARGE:
            case INVALID_FETCH_SIZE:
                return UNKNOWN_SERVER_ERROR;

            default:
                return appendError;
        }
    }

    private Optional<Errors> validateSyncGroup(
        ClassicGroup group,
        SyncGroupRequestData request
    ) {
        if (group.isInState(DEAD)) {
            // If the group is marked as dead, it means some other thread has just removed the group
            // from the coordinator metadata; this is likely that the group has migrated to some other
            // coordinator OR the group is in a transient unstable phase. Let the member retry
            // finding the correct coordinator and rejoin.
            return Optional.of(COORDINATOR_NOT_AVAILABLE);
        } else {
            try {
                group.validateMember(
                    request.memberId(),
                    request.groupInstanceId(),
                    "sync-group"
                );
            } catch (KafkaException ex) {
                return Optional.of(Errors.forException(ex));
            }

            if (request.generationId() != group.generationId()) {
                return Optional.of(Errors.ILLEGAL_GENERATION);
            } else if (isProtocolInconsistent(request.protocolType(), group.protocolType().orElse(null)) ||
                       isProtocolInconsistent(request.protocolName(), group.protocolName().orElse(null))) {
                return Optional.of(Errors.INCONSISTENT_GROUP_PROTOCOL);
            } else {
                return Optional.empty();
            }
        }
    }

    private void removePendingSyncMember(
        ClassicGroup group,
        String memberId
    ) {
        group.removePendingSyncMember(memberId);
        String syncKey = classicGroupSyncKey(group.groupId());
        switch (group.currentState()) {
            case DEAD:
            case EMPTY:
            case PREPARING_REBALANCE:
                timer.cancel(syncKey);
                break;
            case COMPLETING_REBALANCE:
            case STABLE:
                if (group.hasReceivedSyncFromAllMembers()) {
                    timer.cancel(syncKey);
                }
                break;
            default:
                throw new IllegalStateException("Unknown group state: " + group.stateAsString());
        }
    }

    /**
     * Handle a classic group HeartbeatRequest.
     *
     * @param context        The request context.
     * @param request        The actual Heartbeat request.
     *
     * @return The coordinator result that contains the heartbeat response.
     */
    public CoordinatorResult<HeartbeatResponseData, CoordinatorRecord> classicGroupHeartbeat(
        AuthorizableRequestContext context,
        HeartbeatRequestData request
    ) {
        Group group;
        try {
            group = group(request.groupId());
        } catch (GroupIdNotFoundException e) {
            throw new UnknownMemberIdException(
                String.format("Group %s not found.", request.groupId())
            );
        }

        if (group.type() == CLASSIC) {
            return classicGroupHeartbeatToClassicGroup((ClassicGroup) group, context, request);
        } else if (group.type() == CONSUMER) {
            return classicGroupHeartbeatToConsumerGroup((ConsumerGroup) group, context, request);
        } else {
            throw new UnknownMemberIdException(
                String.format("Group %s not found.", request.groupId())
            );
        }
    }

    /**
     * Handle a classic group HeartbeatRequest to a classic group.
     *
     * @param group          The ClassicGroup.
     * @param context        The request context.
     * @param request        The actual Heartbeat request.
     *
     * @return The coordinator result that contains the heartbeat response.
     */
    private CoordinatorResult<HeartbeatResponseData, CoordinatorRecord> classicGroupHeartbeatToClassicGroup(
        ClassicGroup group,
        AuthorizableRequestContext context,
        HeartbeatRequestData request
    ) {
        validateClassicGroupHeartbeat(group, request.memberId(), request.groupInstanceId(), request.generationId());

        switch (group.currentState()) {
            case EMPTY:
                return new CoordinatorResult<>(
                    List.of(),
                    new HeartbeatResponseData().setErrorCode(Errors.UNKNOWN_MEMBER_ID.code())
                );

            case PREPARING_REBALANCE:
                rescheduleClassicGroupMemberHeartbeat(group, group.member(request.memberId()));
                return new CoordinatorResult<>(
                    List.of(),
                    new HeartbeatResponseData().setErrorCode(Errors.REBALANCE_IN_PROGRESS.code())
                );

            case COMPLETING_REBALANCE:
            case STABLE:
                // Consumers may start sending heartbeats after join-group response, while the group
                // is in CompletingRebalance state. In this case, we should treat them as
                // normal heartbeat requests and reset the timer
                rescheduleClassicGroupMemberHeartbeat(group, group.member(request.memberId()));
                return new CoordinatorResult<>(
                    List.of(),
                    new HeartbeatResponseData()
                );

            default:
                throw new IllegalStateException("Reached unexpected state " +
                    group.currentState() + " for group " + group.groupId());
        }
    }

    /**
     * Validates a classic group heartbeat request.
     *
     * @param group              The group.
     * @param memberId           The member id.
     * @param groupInstanceId    The group instance id.
     * @param generationId       The generation id.
     *
     * @throws CoordinatorNotAvailableException If group is Dead.
     * @throws IllegalGenerationException       If the generation id in the request and the generation id of the
     *                                          group does not match.
     */
    private void validateClassicGroupHeartbeat(
        ClassicGroup group,
        String memberId,
        String groupInstanceId,
        int generationId
    ) throws CoordinatorNotAvailableException, IllegalGenerationException {
        if (group.isInState(DEAD)) {
            throw COORDINATOR_NOT_AVAILABLE.exception();
        } else {
            group.validateMember(
                memberId,
                groupInstanceId,
                "heartbeat"
            );

            if (generationId != group.generationId()) {
                throw ILLEGAL_GENERATION.exception();
            }
        }
    }

    /**
     * Handle a classic group HeartbeatRequest to a consumer group. A response with
     * REBALANCE_IN_PROGRESS is returned if 1) the member epoch is smaller than the
     * group epoch, 2) the member is in UNREVOKED_PARTITIONS, or 3) the member is in
     * UNRELEASED_PARTITIONS and all its partitions pending assignment are free.
     *
     * @param group          The ConsumerGroup.
     * @param context        The request context.
     * @param request        The actual Heartbeat request.
     *
     * @return The coordinator result that contains the heartbeat response.
     */
    private CoordinatorResult<HeartbeatResponseData, CoordinatorRecord> classicGroupHeartbeatToConsumerGroup(
        ConsumerGroup group,
        AuthorizableRequestContext context,
        HeartbeatRequestData request
    ) throws UnknownMemberIdException, FencedInstanceIdException, IllegalGenerationException {
        String groupId = request.groupId();
        String memberId = request.memberId();
        String instanceId = request.groupInstanceId();
        ConsumerGroupMember member = validateConsumerGroupMember(group, memberId, instanceId);

        throwIfMemberDoesNotUseClassicProtocol(member);
        throwIfGenerationIdUnmatched(memberId, member.memberEpoch(), request.generationId());

        scheduleConsumerGroupSessionTimeout(groupId, memberId, member.classicProtocolSessionTimeout().get());

        Errors error = Errors.NONE;
        // The member should rejoin if any of the following conditions is met.
        // 1) The group epoch is bumped so the member need to rejoin to catch up.
        // 2) The member needs to revoke some partitions and rejoin to reconcile with the new epoch.
        // 3) The member's partitions pending assignment are free, so it can rejoin to get the complete assignment.
        if (member.memberEpoch() < group.groupEpoch() ||
            member.state() == MemberState.UNREVOKED_PARTITIONS ||
            (member.state() == MemberState.UNRELEASED_PARTITIONS && !group.waitingOnUnreleasedPartition(member))) {
            error = Errors.REBALANCE_IN_PROGRESS;
            scheduleConsumerGroupJoinTimeoutIfAbsent(groupId, memberId, member.rebalanceTimeoutMs());
        }

        return new CoordinatorResult<>(
            List.of(),
            new HeartbeatResponseData().setErrorCode(error.code())
        );
    }

    /**
     * Validates that (1) the instance id exists and is mapped to the member id
     * if the group instance id is provided; and (2) the member id exists in the group.
     *
     * @param group             The consumer group.
     * @param memberId          The member id.
     * @param instanceId        The instance id.
     *
     * @return The ConsumerGroupMember.
     */
    private ConsumerGroupMember validateConsumerGroupMember(
        ConsumerGroup group,
        String memberId,
        String instanceId
    ) throws UnknownMemberIdException, FencedInstanceIdException {
        ConsumerGroupMember member;
        if (instanceId == null) {
            member = group.getOrMaybeCreateMember(memberId, false);
        } else {
            member = group.staticMember(instanceId);
            if (member == null) {
                throw new UnknownMemberIdException(
                    String.format("Member with instance id %s is not a member of group %s.", instanceId, group.groupId())
                );
            }
            throwIfInstanceIdIsFenced(member, group.groupId(), memberId, instanceId);
        }
        return member;
    }

    /**
     * Handle a classic LeaveGroupRequest.
     *
     * @param context        The request context.
     * @param request        The actual LeaveGroup request.
     *
     * @return The LeaveGroup response and the records to append.
     */
    public CoordinatorResult<LeaveGroupResponseData, CoordinatorRecord> classicGroupLeave(
        AuthorizableRequestContext context,
        LeaveGroupRequestData request
    ) throws UnknownMemberIdException {
        Group group;
        try {
            group = group(request.groupId());
        } catch (GroupIdNotFoundException e) {
            throw new UnknownMemberIdException(String.format("Group %s not found.", request.groupId()));
        }

        if (group.type() == CLASSIC) {
            return classicGroupLeaveToClassicGroup((ClassicGroup) group, request);
        } else if (group.type() == CONSUMER) {
            return classicGroupLeaveToConsumerGroup((ConsumerGroup) group, request);
        } else {
            throw new UnknownMemberIdException(String.format("Group %s not found.", request.groupId()));
        }
    }

    /**
     * Handle a classic LeaveGroupRequest to a ConsumerGroup.
     *
     * @param group          The ConsumerGroup.
     * @param request        The actual LeaveGroup request.
     *
     * @return The LeaveGroup response and the records to append.
     */
    private CoordinatorResult<LeaveGroupResponseData, CoordinatorRecord> classicGroupLeaveToConsumerGroup(
        ConsumerGroup group,
        LeaveGroupRequestData request
    ) throws UnknownMemberIdException {
        String groupId = group.groupId();
        List<MemberResponse> memberResponses = new ArrayList<>();
        Set<ConsumerGroupMember> validLeaveGroupMembers = new HashSet<>();

        for (MemberIdentity memberIdentity : request.members()) {
            String reason = memberIdentity.reason() != null ? memberIdentity.reason() : "not provided";

            try {
                ConsumerGroupMember member;

                if (memberIdentity.groupInstanceId() == null) {
                    member = group.getOrMaybeCreateMember(memberIdentity.memberId(), false);

                    log.info("[GroupId {}] Dynamic member {} has left group " +
                            "through explicit `LeaveGroup` request; client reason: {}",
                        groupId, memberIdentity.memberId(), reason);
                } else {
                    member = group.staticMember(memberIdentity.groupInstanceId());
                    throwIfStaticMemberIsUnknown(member, memberIdentity.groupInstanceId());
                    // The LeaveGroup API allows administrative removal of members by GroupInstanceId
                    // in which case we expect the MemberId to be undefined.
                    if (!UNKNOWN_MEMBER_ID.equals(memberIdentity.memberId())) {
                        throwIfInstanceIdIsFenced(member, groupId, memberIdentity.memberId(), memberIdentity.groupInstanceId());
                    }

                    log.info("[GroupId {}] Static member {} with instance id {} has left group " +
                            "through explicit `LeaveGroup` request; client reason: {}",
                        groupId, memberIdentity.memberId(), memberIdentity.groupInstanceId(), reason);
                }

                memberResponses.add(
                    new MemberResponse()
                        .setMemberId(memberIdentity.memberId())
                        .setGroupInstanceId(memberIdentity.groupInstanceId())
                );

                validLeaveGroupMembers.add(member);
            } catch (KafkaException e) {
                memberResponses.add(
                    new MemberResponse()
                        .setMemberId(memberIdentity.memberId())
                        .setGroupInstanceId(memberIdentity.groupInstanceId())
                        .setErrorCode(Errors.forException(e).code())
                );
            }
        }

        return consumerGroupFenceMembers(group, validLeaveGroupMembers, new LeaveGroupResponseData().setMembers(memberResponses));
    }

    /**
     * Handle a classic LeaveGroupRequest to a ClassicGroup.
     *
     * @param group          The ClassicGroup.
     * @param request        The actual LeaveGroup request.
     *
     * @return The LeaveGroup response and the GroupMetadata record to append if the group
     *         no longer has any members.
     */
    private CoordinatorResult<LeaveGroupResponseData, CoordinatorRecord> classicGroupLeaveToClassicGroup(
        ClassicGroup group,
        LeaveGroupRequestData request
    ) throws UnknownMemberIdException {
        if (group.isInState(DEAD)) {
            return new CoordinatorResult<>(
                List.of(),
                new LeaveGroupResponseData()
                    .setErrorCode(COORDINATOR_NOT_AVAILABLE.code())
            );
        }

        List<MemberResponse> memberResponses = new ArrayList<>();

        for (MemberIdentity member: request.members()) {
            String reason = member.reason() != null ? member.reason() : "not provided";
            // The LeaveGroup API allows administrative removal of members by GroupInstanceId
            // in which case we expect the MemberId to be undefined.
            if (UNKNOWN_MEMBER_ID.equals(member.memberId())) {
                if (member.groupInstanceId() != null && group.hasStaticMember(member.groupInstanceId())) {
                    removeCurrentMemberFromClassicGroup(
                        group,
                        group.staticMemberId(member.groupInstanceId()),
                        reason
                    );
                    memberResponses.add(
                        new MemberResponse()
                            .setMemberId(member.memberId())
                            .setGroupInstanceId(member.groupInstanceId())
                    );
                } else {
                    memberResponses.add(
                        new MemberResponse()
                            .setMemberId(member.memberId())
                            .setGroupInstanceId(member.groupInstanceId())
                            .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code())
                    );
                }
            } else if (group.isPendingMember(member.memberId())) {
                group.remove(member.memberId());
                timer.cancel(classicGroupHeartbeatKey(group.groupId(), member.memberId()));
                log.info("[Group {}] Pending member {} has left group through explicit `LeaveGroup` request; client reason: {}",
                    group.groupId(), member.memberId(), reason);

                memberResponses.add(
                    new MemberResponse()
                        .setMemberId(member.memberId())
                        .setGroupInstanceId(member.groupInstanceId())
                );
            } else {
                try {
                    group.validateMember(member.memberId(), member.groupInstanceId(), "leave-group");
                    removeCurrentMemberFromClassicGroup(
                        group,
                        member.memberId(),
                        reason
                    );
                    memberResponses.add(
                        new MemberResponse()
                            .setMemberId(member.memberId())
                            .setGroupInstanceId(member.groupInstanceId())
                    );
                } catch (KafkaException e) {
                    memberResponses.add(
                        new MemberResponse()
                            .setMemberId(member.memberId())
                            .setGroupInstanceId(member.groupInstanceId())
                            .setErrorCode(Errors.forException(e).code())
                    );
                }
            }
        }

        List<String> validLeaveGroupMembers = memberResponses.stream()
            .filter(response -> response.errorCode() == Errors.NONE.code())
            .map(MemberResponse::memberId)
            .toList();

        String reason = "explicit `LeaveGroup` request for (" + String.join(", ", validLeaveGroupMembers) + ") members.";
        CoordinatorResult<Void, CoordinatorRecord> coordinatorResult = EMPTY_RESULT;

        if (!validLeaveGroupMembers.isEmpty()) {
            switch (group.currentState()) {
                case STABLE:
                case COMPLETING_REBALANCE:
                    coordinatorResult = maybePrepareRebalanceOrCompleteJoin(group, reason);
                    break;
                case PREPARING_REBALANCE:
                    coordinatorResult = maybeCompleteJoinPhase(group);
                    break;
                default:
            }
        }

        return new CoordinatorResult<>(
            coordinatorResult.records(),
            new LeaveGroupResponseData()
                .setMembers(memberResponses),
            coordinatorResult.appendFuture(),
            coordinatorResult.replayRecords()
        );
    }

    /**
     * Remove a member from the group. Cancel member's heartbeat, and prepare rebalance
     * or complete the join phase if necessary.
     *
     * @param group     The classic group.
     * @param memberId  The member id.
     * @param reason    The reason for the LeaveGroup request.
     *
     */
    private void removeCurrentMemberFromClassicGroup(
        ClassicGroup group,
        String memberId,
        String reason
    ) {
        ClassicGroupMember member = group.member(memberId);
        timer.cancel(classicGroupHeartbeatKey(group.groupId(), memberId));
        log.info("[Group {}] Member {} has left group through explicit `LeaveGroup` request; client reason: {}",
            group.groupId(), memberId, reason);

        // New members may timeout with a pending JoinGroup while the group is still rebalancing, so we have
        // to invoke the callback before removing the member. We return UNKNOWN_MEMBER_ID so that the consumer
        // will retry the JoinGroup request if is still active.
        group.completeJoinFuture(
            member,
            new JoinGroupResponseData()
                .setMemberId(UNKNOWN_MEMBER_ID)
                .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code())
        );
        group.remove(member.memberId());
    }

    /**
     * Handles a DeleteGroups request.
     * Populates the record list passed in with record to update the state machine.
     * Validations are done in {@link GroupCoordinatorShard#deleteGroups(AuthorizableRequestContext, List)} by
     * calling {@link GroupMetadataManager#validateDeleteGroup(String)}.
     *
     * @param groupId The id of the group to be deleted. It has been checked in {@link GroupMetadataManager#validateDeleteGroup}.
     * @param records The record list to populate.
     */
    public void createGroupTombstoneRecords(
        String groupId,
        List<CoordinatorRecord> records
    ) {
        // At this point, we have already validated the group id, so we know that the group exists and that no exception will be thrown.
        createGroupTombstoneRecords(group(groupId), records);
    }

    /**
     * Populates the record list passed in with record to update the state machine.
     *
     * @param group The group to be deleted.
     * @param records The record list to populate.
     */
    public void createGroupTombstoneRecords(
        Group group,
        List<CoordinatorRecord> records
    ) {
        group.createGroupTombstoneRecords(records);
    }

    /**
     * Returns an optional of delete share group request object to be used with the persister.
     * Empty if no subscribed topics or if the share group is empty.
     * @param shareGroupId      Share group id
     * @param records           List of coordinator records to append to
     * @return Optional of object representing the share group state delete request.
     */
    public Optional<DeleteShareGroupStateParameters> shareGroupBuildPartitionDeleteRequest(String shareGroupId, List<CoordinatorRecord> records) {
        if (!shareGroupStatePartitionMetadata.containsKey(shareGroupId)) {
            return Optional.empty();
        }

        Map<Uuid, InitMapValue> deleteCandidates = combineInitMaps(
            shareGroupStatePartitionMetadata.get(shareGroupId).initializedTopics(),
            shareGroupStatePartitionMetadata.get(shareGroupId).initializingTopics()
        );

        // Ideally the deleting should be empty - if it is not then it implies
        // that some previous share group delete or delete offsets command
        // did not complete successfully. So, set up the delete request such that
        // a retry for the same is possible. Since this is part of an admin operation
        // retrying delete should not pose issues related to
        // performance. Also, the share coordinator is idempotent on delete partitions.
        Set<Uuid> currentDeleting = shareGroupStatePartitionMetadata.get(shareGroupId).deletingTopics();
        Map<Uuid, InitMapValue> deleteRetryCandidates = new HashMap<>();
        Set<Uuid> deletingToIgnore = new HashSet<>();
        if (!currentDeleting.isEmpty()) {
            if (metadataImage == null || metadataImage.equals(CoordinatorMetadataImage.EMPTY)) {
                deletingToIgnore.addAll(currentDeleting);
            } else {
                for (Uuid deletingTopicId : currentDeleting) {
                    Optional<CoordinatorMetadataImage.TopicMetadata> topicMetadataOp = metadataImage.topicMetadata(deletingTopicId);
                    if (topicMetadataOp.isEmpty()) {
                        deletingToIgnore.add(deletingTopicId);
                    } else {
                        deleteRetryCandidates.put(deletingTopicId,
                            new InitMapValue(
                                topicMetadataOp.get().name(),
                                IntStream.range(0, topicMetadataOp.get().partitionCount()).boxed().collect(Collectors.toSet()),
                                -1));
                    }
                }
            }
        }

        if (!deletingToIgnore.isEmpty()) {
            log.warn("Some topics for share group id {} were not found in the metadata image - {}", shareGroupId, deletingToIgnore);
        }

        if (!deleteRetryCandidates.isEmpty()) {
            log.info("Existing deleting entries found in share group {} - {}", shareGroupId, deleteRetryCandidates);
            deleteCandidates = combineInitMaps(deleteCandidates, deleteRetryCandidates);
        }

        // Remove all initializing and initialized topic info from record and add deleting. There
        // could be previous deleting topics due to offsets delete, we need to account for them as well.
        // If some older deleting topics could not be found in the metadata image, they will be ignored
        // and logged.
        records.add(GroupCoordinatorRecordHelpers.newShareGroupStatePartitionMetadataRecord(
            shareGroupId,
            Map.of(),
            Map.of(),
            deleteCandidates.entrySet().stream()
                .map(entry -> Map.entry(entry.getKey(), entry.getValue().name()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
        ));

        if (deleteCandidates.isEmpty()) {
            return Optional.empty();
        }

        List<TopicData<PartitionIdData>> topicDataList = new ArrayList<>(deleteCandidates.size());
        for (Map.Entry<Uuid, InitMapValue> entry : deleteCandidates.entrySet()) {
            topicDataList.add(new TopicData<>(
                entry.getKey(),
                entry.getValue().partitions().stream()
                    .map(PartitionFactory::newPartitionIdData)
                    .toList()
            ));
        }

        return Optional.of(new DeleteShareGroupStateParameters.Builder()
            .setGroupTopicPartitionData(new GroupTopicPartitionData.Builder<PartitionIdData>()
                .setGroupId(shareGroupId)
                .setTopicsData(topicDataList)
                .build())
            .build()
        );
    }

    /**
     * Returns a list of delete share group state request topic objects to be used with the persister.
     * @param groupId                    group ID of the share group
     * @param requestData                the request data for DeleteShareGroupOffsets request
     * @param errorTopicResponseList     the list of topics not found in the metadata image
     * @param records                    List of coordinator records to append to
     *
     * @return List of objects representing the share group state delete request for topics.
     */
    public List<DeleteShareGroupStateRequestData.DeleteStateData> sharePartitionsEligibleForOffsetDeletion(
        String groupId,
        DeleteShareGroupOffsetsRequestData requestData,
        List<DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic> errorTopicResponseList,
        List<CoordinatorRecord> records
    ) {
        List<DeleteShareGroupStateRequestData.DeleteStateData> deleteShareGroupStateRequestTopicsData = new ArrayList<>();

        ShareGroupStatePartitionMetadataInfo currentMap = shareGroupStatePartitionMetadata.get(groupId);

        if (currentMap == null) {
            return deleteShareGroupStateRequestTopicsData;
        }

        Map<Uuid, InitMapValue> initializedTopics = new HashMap<>();
        currentMap.initializedTopics().forEach((topicId, initValue) -> {
            initializedTopics.put(topicId, new InitMapValue(initValue.name(), new HashSet<>(initValue.partitions()), initValue.timestamp()));
        });
        Set<Uuid> deletingTopics = new HashSet<>(currentMap.deletingTopics());

        requestData.topics().forEach(topic -> {
            Optional<CoordinatorMetadataImage.TopicMetadata> topicMetadataOpt = metadataImage.topicMetadata(topic.topicName());
            if (topicMetadataOpt.isPresent()) {
                var topicMetadata = topicMetadataOpt.get();
                Uuid topicId = topicMetadata.id();
                // A deleteState request to persister should only be sent with those topic partitions for which corresponding
                // share partitions are initialized for the group.
                if (initializedTopics.containsKey(topicId)) {
                    List<DeleteShareGroupStateRequestData.PartitionData> partitions = new ArrayList<>();
                    initializedTopics.get(topicId).partitions().forEach(partition ->
                        partitions.add(new DeleteShareGroupStateRequestData.PartitionData().setPartition(partition)));
                    deleteShareGroupStateRequestTopicsData.add(
                        new DeleteShareGroupStateRequestData.DeleteStateData()
                            .setTopicId(topicId)
                            .setPartitions(partitions)
                    );
                    // Removing the topic from initializedTopics map.
                    initializedTopics.remove(topicId);
                    // Adding the topic to deletingTopics map.
                    deletingTopics.add(topicId);
                } else if (deletingTopics.contains(topicId)) {
                    // If the topic for which delete share group offsets request is sent is already present in the deletingTopics set,
                    // we will include that topic in the delete share group state request.
                    List<DeleteShareGroupStateRequestData.PartitionData> partitions = new ArrayList<>();
                    IntStream.range(0, topicMetadata.partitionCount()).forEach(partition ->
                        partitions.add(new DeleteShareGroupStateRequestData.PartitionData().setPartition(partition)));
                    deleteShareGroupStateRequestTopicsData.add(
                        new DeleteShareGroupStateRequestData.DeleteStateData()
                            .setTopicId(topicId)
                            .setPartitions(partitions)
                    );
                } else {
                    errorTopicResponseList.add(
                        new DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic()
                            .setTopicName(topic.topicName())
                            .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                            .setErrorMessage("There is no offset information to delete."));
                }
            } else {
                errorTopicResponseList.add(new DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic()
                    .setTopicName(topic.topicName())
                    .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                    .setErrorMessage(Errors.UNKNOWN_TOPIC_OR_PARTITION.message())
                );
            }
        });

        records.add(
            GroupCoordinatorRecordHelpers.newShareGroupStatePartitionMetadataRecord(
                groupId,
                currentMap.initializingTopics(),
                initializedTopics,
                attachTopicName(deletingTopics)
            )
        );

        return deleteShareGroupStateRequestTopicsData;
    }

    /**
     * Handles an AlterShareGroupOffsets request.
     *
     * Make the following checks to make sure the AlterShareGroupOffsetsRequest request is valid:
     * 1. Checks whether the provided group is empty
     * 2. Checks the requested topics are presented in the metadataImage
     * 3. Checks the corresponding share partitions in AlterShareGroupOffsetsRequest are existing
     *
     * @param groupId   The group id from the request.
     * @param topics    The topic information for altering the share group's offsets from the request.
     *
     * @return A Result containing a pair of ShareGroupHeartbeat response and maybe InitializeShareGroupStateParameters
     *         and a list of records to update the state machine.
     */
    public CoordinatorResult<Map.Entry<AlterShareGroupOffsetsResponseData, InitializeShareGroupStateParameters>, CoordinatorRecord> alterShareGroupOffsets(
        String groupId,
        AlterShareGroupOffsetsRequestData.AlterShareGroupOffsetsRequestTopicCollection topics
    ) throws ApiException {
        final long currentTimeMs = time.milliseconds();
        final List<CoordinatorRecord> records = new ArrayList<>();

        // Get or create the share group. If the group exists, check that it's empty. If it is created, it is empty.
        final ShareGroup group = getOrMaybeCreateShareGroup(groupId, true);
        throwIfShareGroupIsNotEmpty(group);

        AlterShareGroupOffsetsResponseData.AlterShareGroupOffsetsResponseTopicCollection alterShareGroupOffsetsResponseTopics = new AlterShareGroupOffsetsResponseData.AlterShareGroupOffsetsResponseTopicCollection();

        Map<Uuid, InitMapValue> initializingTopics = new HashMap<>();
        Map<Uuid, Map<Integer, Long>> offsetByTopicPartitions = new HashMap<>();

        topics.forEach(topic -> {
            Optional<CoordinatorMetadataImage.TopicMetadata> topicMetadataOpt = metadataImage.topicMetadata(topic.topicName());
            if (topicMetadataOpt.isPresent()) {
                var topicMetadata = topicMetadataOpt.get();
                Uuid topicId = topicMetadata.id();
                List<AlterShareGroupOffsetsResponseData.AlterShareGroupOffsetsResponsePartition> partitions = new ArrayList<>();
                topic.partitions().forEach(partition -> {
                    if (partition.partitionIndex() < topicMetadata.partitionCount()) {
                        partitions.add(
                            new AlterShareGroupOffsetsResponseData.AlterShareGroupOffsetsResponsePartition()
                                .setPartitionIndex(partition.partitionIndex())
                                .setErrorCode(Errors.NONE.code()));
                        offsetByTopicPartitions.computeIfAbsent(topicId, k -> new HashMap<>()).put(partition.partitionIndex(), partition.startOffset());
                    } else {
                        partitions.add(
                            new AlterShareGroupOffsetsResponseData.AlterShareGroupOffsetsResponsePartition()
                                .setPartitionIndex(partition.partitionIndex())
                                .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                                .setErrorMessage(Errors.UNKNOWN_TOPIC_OR_PARTITION.message()));
                    }
                });

                initializingTopics.put(topicId, new InitMapValue(
                    topic.topicName(),
                    topic.partitions().stream()
                        .map(AlterShareGroupOffsetsRequestData.AlterShareGroupOffsetsRequestPartition::partitionIndex)
                        .filter(part -> part < topicMetadata.partitionCount())
                        .collect(Collectors.toSet()),
                    currentTimeMs
                ));

                alterShareGroupOffsetsResponseTopics.add(
                    new AlterShareGroupOffsetsResponseData.AlterShareGroupOffsetsResponseTopic()
                        .setTopicName(topic.topicName())
                        .setTopicId(topicId)
                        .setPartitions(partitions)
                );

            } else {
                List<AlterShareGroupOffsetsResponseData.AlterShareGroupOffsetsResponsePartition> partitions = new ArrayList<>();
                topic.partitions().forEach(partition -> partitions.add(
                    new AlterShareGroupOffsetsResponseData.AlterShareGroupOffsetsResponsePartition()
                        .setPartitionIndex(partition.partitionIndex())
                        .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                        .setErrorMessage(Errors.UNKNOWN_TOPIC_OR_PARTITION.message())));
                alterShareGroupOffsetsResponseTopics.add(
                    new AlterShareGroupOffsetsResponseData.AlterShareGroupOffsetsResponseTopic()
                        .setTopicName(topic.topicName())
                        .setPartitions(partitions)
                );
            }
        });

        addInitializingTopicsRecords(groupId, records, initializingTopics);
        return new CoordinatorResult<>(
            records,
            Map.entry(
                new AlterShareGroupOffsetsResponseData()
                    .setResponses(alterShareGroupOffsetsResponseTopics),
                buildInitializeShareGroupState(groupId, group.groupEpoch(), offsetByTopicPartitions)
            )
        );
    }

    /**
     * Iterates over the share state metadata map and removes any
     * deleted topic ids from the initialized and initializing maps.
     * Also, updates the deleted set with valid values. This method does
     * not add new topic ids to the deleted map but removed input ids from
     * initializing, initialized and deleting maps.
     * Meant to be executed on topic delete events, in parallel
     * with counterpart method share coordinator.
     *
     * @param deletedTopicIds   The set of topics which are deleted
     * @return A result containing new records or empty if no change, and void response.
     */
    public CoordinatorResult<Void, CoordinatorRecord> maybeCleanupShareGroupState(
        Set<Uuid> deletedTopicIds
    ) {
        if (deletedTopicIds.isEmpty()) {
            return new CoordinatorResult<>(List.of());
        }
        List<CoordinatorRecord> records = new ArrayList<>();
        shareGroupStatePartitionMetadata.forEach((groupId, metadata) -> {
            Set<Uuid> initializingDeletedCurrent = new HashSet<>(metadata.initializingTopics().keySet());
            Set<Uuid> initializedDeletedCurrent = new HashSet<>(metadata.initializedTopics().keySet());
            Set<Uuid> deletingDeletedCurrent = new HashSet<>(metadata.deletingTopics());

            initializingDeletedCurrent.retainAll(deletedTopicIds);
            initializedDeletedCurrent.retainAll(deletedTopicIds);
            deletingDeletedCurrent.retainAll(deletedTopicIds);

            // The deleted topic ids are neither present in initializing
            // nor in initialized nor in deleting, so we have nothing to do.
            if (initializingDeletedCurrent.isEmpty() && initializedDeletedCurrent.isEmpty() && deletingDeletedCurrent.isEmpty()) {
                return;
            }

            // At this point some initialized or initializing topics
            // are to be deleted but, we will not move them to deleted
            // because the call setup of this method is such that the
            // persister call is automatically done by the BrokerMetadataPublisher
            // increasing efficiency and removing need of chained futures.
            Map<Uuid, InitMapValue> finalInitializing = new HashMap<>(metadata.initializingTopics());
            initializingDeletedCurrent.forEach(finalInitializing::remove);

            Map<Uuid, InitMapValue> finalInitialized = new HashMap<>(metadata.initializedTopics());
            initializedDeletedCurrent.forEach(finalInitialized::remove);

            Set<Uuid> finalDeleting = new HashSet<>(metadata.deletingTopics());
            finalDeleting.removeAll(deletedTopicIds);

            records.add(GroupCoordinatorRecordHelpers.newShareGroupStatePartitionMetadataRecord(
                groupId,
                finalInitializing,
                finalInitialized,
                attachTopicName(finalDeleting)
            ));
        });

        return new CoordinatorResult<>(records);
    }

    /**
     * Returns a list of {@link DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic} corresponding to the
     * topics for which persister delete share group state request was successful
     * @param groupId                    group ID of the share group
     * @param topics                     a map of topicId to topic name
     * @param records                    List of coordinator records to append to
     *
     * @return List of objects for which request was successful
     */
    public List<DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic> completeDeleteShareGroupOffsets(
        String groupId,
        Map<Uuid, String> topics,
        List<CoordinatorRecord> records
    ) {
        ShareGroupStatePartitionMetadataInfo currentMap = shareGroupStatePartitionMetadata.get(groupId);

        if (currentMap == null) {
            return List.of();
        }

        Set<Uuid> updatedDeletingTopics = new HashSet<>(currentMap.deletingTopics());

        topics.keySet().forEach(updatedDeletingTopics::remove);

        records.add(
            GroupCoordinatorRecordHelpers.newShareGroupStatePartitionMetadataRecord(
                groupId,
                currentMap.initializingTopics(),
                currentMap.initializedTopics(),
                attachTopicName(updatedDeletingTopics)
            )
        );

        return topics.entrySet().stream().map(entry ->
            new DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic()
                .setTopicId(entry.getKey())
                .setTopicName(entry.getValue())
                .setErrorCode(Errors.NONE.code())
                .setErrorMessage(null)
        ).toList();
    }

    /**
     * Validates the DeleteGroups request.
     *
     * @param groupId The id of the group to be deleted.
     */
    void validateDeleteGroup(String groupId) throws ApiException {
        Group group = group(groupId);
        group.validateDeleteGroup();
    }

    /**
     * Delete the group if it exists and is in Empty state.
     *
     * @param groupId The group id.
     * @param records The list of records to append the group metadata tombstone records.
     */
    public void maybeDeleteGroup(String groupId, List<CoordinatorRecord> records) {
        Group group = groups.get(groupId);
        if (group != null && group.isEmpty()) {
            createGroupTombstoneRecords(groupId, records);
        }
    }

    /**
     * @return true if the group is an empty classic group.
     */
    private static boolean isEmptyClassicGroup(Group group) {
        return group != null && group.type() == CLASSIC && group.isEmpty();
    }

    /**
     * @return true if the group is an empty consumer group.
     */
    private static boolean isEmptyConsumerGroup(Group group) {
        return group != null && group.type() == CONSUMER && group.isEmpty();
    }

    /**
     * @return true if the group is an empty streams group.
     */
    private static boolean isEmptyStreamsGroup(Group group) {
        return group != null && group.type() == STREAMS && group.isEmpty();
    }

    /**
     * Write tombstones for the group if it's empty and is a classic group.
     *
     * @param group     The group to be deleted.
     * @param records   The list of records to delete the group.
     *
     * @return true if the group is an empty classic group.
     */
    private boolean maybeDeleteEmptyClassicGroup(Group group, List<CoordinatorRecord> records) {
        if (isEmptyClassicGroup(group)) {
            // Delete the classic group by adding tombstones.
            // There's no need to remove the group as the replay of tombstones removes it.
            createGroupTombstoneRecords(group, records);
            return true;
        }
        return false;
    }

    /**
     * Delete and write tombstones for the group if it's empty and is a consumer group.
     *
     * @param groupId The group id to be deleted.
     * @param records The list of records to delete the group.
     *
     * @return true if the group is an empty consumer group.
     */
    private boolean maybeDeleteEmptyConsumerGroup(String groupId, List<CoordinatorRecord> records) {
        Group group = groups.get(groupId, Long.MAX_VALUE);
        if (isEmptyConsumerGroup(group)) {
            // Add tombstones for the previous consumer group. The tombstones won't actually be
            // replayed because its coordinator result has a non-null appendFuture.
            createGroupTombstoneRecords(group, records);
            removeGroup(groupId);
            return true;
        }
        return false;
    }
    
    /**
     * Delete and write tombstones for the group if it's empty and is a streams group.
     *
     * @param groupId The group id to be deleted.
     * @param records The list of records to delete the group.
     *
     * @return true if the group is an empty streams group.
     */
    private boolean maybeDeleteEmptyStreamsGroup(String groupId, List<CoordinatorRecord> records) {
        Group group = groups.get(groupId, Long.MAX_VALUE);
        if (isEmptyStreamsGroup(group)) {
            // Add tombstones for the previous streams group. The tombstones won't actually be
            // replayed because its coordinator result has a non-null appendFuture.
            createGroupTombstoneRecords(group, records);
            removeGroup(groupId);
            return true;
        }
        return false;
    }

    /**
     * Checks whether the given protocol type or name in the request is inconsistent with the group's.
     *
     * @param protocolTypeOrName       The request's protocol type or name.
     * @param groupProtocolTypeOrName  The group's protoocl type or name.
     *
     * @return  True if protocol is inconsistent, false otherwise.
     */
    private boolean isProtocolInconsistent(
        String protocolTypeOrName,
        String groupProtocolTypeOrName
    ) {
        return protocolTypeOrName != null
            && groupProtocolTypeOrName != null
            && !groupProtocolTypeOrName.equals(protocolTypeOrName);
    }

    /**
     * @return The set of all groups' ids.
     */
    public Set<String> groupIds() {
        return Collections.unmodifiableSet(this.groups.keySet());
    }

    // Visible for testing
    Map<String, Long> topicHashCache() {
        return Collections.unmodifiableMap(this.topicHashCache);
    }

    /**
     * Get the session timeout of the provided consumer group.
     */
    private int consumerGroupSessionTimeoutMs(String groupId) {
        Optional<GroupConfig> groupConfig = groupConfigManager.groupConfig(groupId);
        return groupConfig.map(GroupConfig::consumerSessionTimeoutMs)
            .orElse(config.consumerGroupSessionTimeoutMs());
    }

    /**
     * Get the heartbeat interval of the provided consumer group.
     */
    private int consumerGroupHeartbeatIntervalMs(String groupId) {
        Optional<GroupConfig> groupConfig = groupConfigManager.groupConfig(groupId);
        return groupConfig.map(GroupConfig::consumerHeartbeatIntervalMs)
            .orElse(config.consumerGroupHeartbeatIntervalMs());
    }

    /**
     * Get the session timeout of the provided share group.
     */
    private int shareGroupSessionTimeoutMs(String groupId) {
        Optional<GroupConfig> groupConfig = groupConfigManager.groupConfig(groupId);
        return groupConfig.map(GroupConfig::shareSessionTimeoutMs)
            .orElse(config.shareGroupSessionTimeoutMs());
    }

    /**
     * Get the heartbeat interval of the provided share group.
     */
    private int shareGroupHeartbeatIntervalMs(String groupId) {
        Optional<GroupConfig> groupConfig = groupConfigManager.groupConfig(groupId);
        return groupConfig.map(GroupConfig::shareHeartbeatIntervalMs)
            .orElse(config.shareGroupHeartbeatIntervalMs());
    }

    /**
     * Get the session timeout of the provided streams group.
     */
    private int streamsGroupSessionTimeoutMs(String groupId) {
        Optional<GroupConfig> groupConfig = groupConfigManager.groupConfig(groupId);
        return groupConfig.map(GroupConfig::streamsSessionTimeoutMs)
            .orElse(config.streamsGroupSessionTimeoutMs());
    }

    /**
     * Get the heartbeat interval of the provided streams group.
     */
    private int streamsGroupHeartbeatIntervalMs(String groupId) {
        Optional<GroupConfig> groupConfig = groupConfigManager.groupConfig(groupId);
        return groupConfig.map(GroupConfig::streamsHeartbeatIntervalMs)
            .orElse(config.streamsGroupHeartbeatIntervalMs());
    }

    /**
     * Get the assignor of the provided streams group.
     */
    private TaskAssignor streamsGroupAssignor(String groupId) {
        return streamsGroupAssignors.get("sticky");
    }

    /**
     * Get the assignor of the provided streams group.
     */
    private Map<String, String> streamsGroupAssignmentConfigs(String groupId) {
        Optional<GroupConfig> groupConfig = groupConfigManager.groupConfig(groupId);
        final Integer numStandbyReplicas = groupConfig.map(GroupConfig::streamsNumStandbyReplicas)
            .orElse(config.streamsGroupNumStandbyReplicas());
        return Map.of("num.standby.replicas", numStandbyReplicas.toString());
    }

    /**
     * Generate a classic group heartbeat key for the timer.
     *
     * Package private for testing.
     *
     * @param groupId   The group id.
     * @param memberId  The member id.
     *
     * @return the heartbeat key.
     */
    static String classicGroupHeartbeatKey(String groupId, String memberId) {
        return "heartbeat-" + groupId + "-" + memberId;
    }

    /**
     * Generate a classic group join key for the timer.
     *
     * Package private for testing.
     *
     * @param groupId   The group id.
     *
     * @return the join key.
     */
    static String classicGroupJoinKey(String groupId) {
        return "join-" + groupId;
    }

    /**
     * Generate a classic group sync key for the timer.
     *
     * Package private for testing.
     *
     * @param groupId   The group id.
     *
     * @return the sync key.
     */
    static String classicGroupSyncKey(String groupId) {
        return "sync-" + groupId;
    }

    /**
     * Generate a consumer group join key for the timer.
     *
     * Package private for testing.
     *
     * @param groupId   The group id.
     * @param memberId  The member id.
     *
     * @return the sync key.
     */
    static String consumerGroupJoinKey(String groupId, String memberId) {
        return "join-" + groupId + "-" + memberId;
    }

    /**
     * Generate a consumer group sync key for the timer.
     *
     * Package private for testing.
     *
     * @param groupId   The group id.
     * @param memberId  The member id.
     *
     * @return the sync key.
     */
    static String consumerGroupSyncKey(String groupId, String memberId) {
        return "sync-" + groupId + "-" + memberId;
    }

    // Visibility for testing
    Map<String, ShareGroupStatePartitionMetadataInfo> shareGroupStatePartitionMetadata() {
        return shareGroupStatePartitionMetadata;
    }
}
