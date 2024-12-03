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
import org.apache.kafka.common.errors.IllegalGenerationException;
import org.apache.kafka.common.errors.InconsistentGroupProtocolException;
import org.apache.kafka.common.errors.InvalidRegularExpression;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnreleasedInstanceIdException;
import org.apache.kafka.common.errors.UnsupportedAssignorException;
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.ConsumerProtocolSubscription;
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
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.ShareGroupHeartbeatRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.common.runtime.CoordinatorExecutor;
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
import org.apache.kafka.coordinator.group.generated.ShareGroupPartitionMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupPartitionMetadataValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMemberKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMemberValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMetadataValue;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetricsShard;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.modern.MemberState;
import org.apache.kafka.coordinator.group.modern.ModernGroup;
import org.apache.kafka.coordinator.group.modern.TargetAssignmentBuilder;
import org.apache.kafka.coordinator.group.modern.TopicMetadata;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroup;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroupMember;
import org.apache.kafka.coordinator.group.modern.consumer.CurrentAssignmentBuilder;
import org.apache.kafka.coordinator.group.modern.consumer.ResolvedRegularExpression;
import org.apache.kafka.coordinator.group.modern.share.ShareGroup;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupAssignmentBuilder;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupMember;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.TopicsDelta;
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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.common.protocol.Errors.COORDINATOR_NOT_AVAILABLE;
import static org.apache.kafka.common.protocol.Errors.ILLEGAL_GENERATION;
import static org.apache.kafka.common.protocol.Errors.NOT_COORDINATOR;
import static org.apache.kafka.common.protocol.Errors.UNKNOWN_SERVER_ERROR;
import static org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest.CONSUMER_GENERATED_MEMBER_ID_REQUIRED_VERSION;
import static org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH;
import static org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest.LEAVE_GROUP_STATIC_MEMBER_EPOCH;
import static org.apache.kafka.common.requests.JoinGroupRequest.UNKNOWN_MEMBER_ID;
import static org.apache.kafka.coordinator.group.Group.GroupType.CLASSIC;
import static org.apache.kafka.coordinator.group.Group.GroupType.CONSUMER;
import static org.apache.kafka.coordinator.group.Group.GroupType.SHARE;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newShareGroupCurrentAssignmentRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newShareGroupCurrentAssignmentTombstoneRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newShareGroupEpochRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newShareGroupMemberSubscriptionRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newShareGroupMemberSubscriptionTombstoneRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newShareGroupSubscriptionMetadataRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newShareGroupTargetAssignmentTombstoneRecord;
import static org.apache.kafka.coordinator.group.Utils.assignmentToString;
import static org.apache.kafka.coordinator.group.Utils.ofSentinel;
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
import static org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroupMember.hasAssignedPartitionsChanged;

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

    public static class Builder {
        private LogContext logContext = null;
        private SnapshotRegistry snapshotRegistry = null;
        private Time time = null;
        private CoordinatorTimer<Void, CoordinatorRecord> timer = null;
        private CoordinatorExecutor<CoordinatorRecord> executor = null;
        private GroupCoordinatorConfig config = null;
        private GroupConfigManager groupConfigManager = null;
        private MetadataImage metadataImage = null;
        private ShareGroupPartitionAssignor shareGroupAssignor = null;
        private GroupCoordinatorMetricsShard metrics;

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

        Builder withMetadataImage(MetadataImage metadataImage) {
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

        GroupMetadataManager build() {
            if (logContext == null) logContext = new LogContext();
            if (snapshotRegistry == null) snapshotRegistry = new SnapshotRegistry(logContext);
            if (metadataImage == null) metadataImage = MetadataImage.EMPTY;
            if (time == null) time = Time.SYSTEM;

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
                shareGroupAssignor
            );
        }
    }

    /**
     * The minimum amount of time between two consecutive refreshes of
     * the regular expressions within a single group.
     */
    private static final long REGEX_BATCH_REFRESH_INTERVAL_MS = 10_000L;

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
     * The group manager.
     */
    private final GroupConfigManager groupConfigManager;

    /**
     * The metadata image.
     */
    private MetadataImage metadataImage;

    /**
     * This tracks the version (or the offset) of the last metadata image
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
        new CoordinatorResult<>(Collections.emptyList(), CompletableFuture.completedFuture(null), false);

    /**
     * The share group partition assignor.
     */
    private final ShareGroupPartitionAssignor shareGroupAssignor;

    private GroupMetadataManager(
        SnapshotRegistry snapshotRegistry,
        LogContext logContext,
        Time time,
        CoordinatorTimer<Void, CoordinatorRecord> timer,
        CoordinatorExecutor<CoordinatorRecord> executor,
        GroupCoordinatorMetricsShard metrics,
        MetadataImage metadataImage,
        GroupCoordinatorConfig config,
        GroupConfigManager groupConfigManager,
        ShareGroupPartitionAssignor shareGroupAssignor
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
        this.groupConfigManager = groupConfigManager;
        this.shareGroupAssignor = shareGroupAssignor;
    }

    /**
     * @return The current metadata image used by the group metadata manager.
     */
    public MetadataImage image() {
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
            .collect(Collectors.toList());
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
                    metadataImage.topics()
                ));
            } catch (GroupIdNotFoundException exception) {
                describedGroups.add(new ConsumerGroupDescribeResponseData.DescribedGroup()
                    .setGroupId(groupId)
                    .setErrorCode(Errors.GROUP_ID_NOT_FOUND.code())
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
                    metadataImage.topics()
                ));
            } catch (GroupIdNotFoundException exception) {
                describedGroups.add(new ShareGroupDescribeResponseData.DescribedGroup()
                    .setGroupId(groupId)
                    .setErrorCode(Errors.GROUP_ID_NOT_FOUND.code())
                );
            }
        });

        return describedGroups;
    }

    /**
     * Handles a DescribeGroup request.
     *
     * @param groupIds          The IDs of the groups to describe.
     * @param committedOffset   A specified committed offset corresponding to this shard.
     *
     * @return A list containing the DescribeGroupsResponseData.DescribedGroup.
     */
    public List<DescribeGroupsResponseData.DescribedGroup> describeGroups(
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
                            .collect(Collectors.toList())
                        )
                    );
                } else {
                    describedGroups.add(new DescribeGroupsResponseData.DescribedGroup()
                        .setGroupId(groupId)
                        .setGroupState(group.stateAsString())
                        .setProtocolType(group.protocolType().orElse(""))
                        .setMembers(group.allMembers().stream()
                            .map(ClassicGroupMember::describeNoMetadata)
                            .collect(Collectors.toList())
                        )
                    );
                }
            } catch (GroupIdNotFoundException exception) {
                describedGroups.add(new DescribeGroupsResponseData.DescribedGroup()
                    .setGroupId(groupId)
                    .setGroupState(DEAD.toString())
                );
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

        if (group == null || (createIfNotExists && maybeDeleteEmptyClassicGroup(group, records))) {
            return new ConsumerGroup(snapshotRegistry, groupId, metrics);
        } else {
            if (group.type() == CONSUMER) {
                return (ConsumerGroup) group;
            } else if (createIfNotExists && group.type() == CLASSIC && validateOnlineUpgrade((ClassicGroup) group)) {
                return convertToConsumerGroup((ClassicGroup) group, records);
            } else {
                throw new GroupIdNotFoundException(String.format("Group %s is not a consumer group", groupId));
            }
        }
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
            throw new GroupIdNotFoundException(String.format("Group %s is not a consumer group", groupId));
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
     * @throws GroupIdNotFoundException if the group does not exist and createIfNotExists is false or
     *                                  if the group is not a consumer group.
     * @throws IllegalStateException    if the group does not have the expected type.
     * Package private for testing.
     */
    ConsumerGroup getOrMaybeCreatePersistedConsumerGroup(
        String groupId,
        boolean createIfNotExists
    ) throws GroupIdNotFoundException, IllegalStateException {
        Group group = groups.get(groupId);

        if (group == null && !createIfNotExists) {
            throw new GroupIdNotFoundException(String.format("Consumer group %s not found", groupId));
        }

        if (group == null) {
            ConsumerGroup consumerGroup = new ConsumerGroup(snapshotRegistry, groupId, metrics);
            groups.put(groupId, consumerGroup);
            metrics.onConsumerGroupStateTransition(null, consumerGroup.state());
            return consumerGroup;
        } else if (group.type() == CONSUMER) {
            return (ConsumerGroup) group;
        } else if (group.type() == CLASSIC && ((ClassicGroup) group).isSimpleGroup()) {
            // If the group is a simple classic group, it was automatically created to hold committed
            // offsets if no group existed. Simple classic groups are not backed by any records
            // in the __consumer_offsets topic hence we can safely replace it here. Without this,
            // replaying consumer group records after offset commit records would not work.
            ConsumerGroup consumerGroup = new ConsumerGroup(snapshotRegistry, groupId, metrics);
            groups.put(groupId, consumerGroup);
            metrics.onConsumerGroupStateTransition(null, consumerGroup.state());
            return consumerGroup;
        } else {
            throw new IllegalStateException(String.format("Group %s is not a consumer group", groupId));
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
        }

        if (group.type() != SHARE) {
            // We don't support upgrading/downgrading between protocols at the moment so
            // we throw an exception if a group exists with the wrong type.
            throw new GroupIdNotFoundException(String.format("Group %s is not a share group.",
                groupId));
        }

        return (ShareGroup) group;
    }

    /**
     * Gets or maybe creates a share group.
     *
     * @param groupId           The group id.
     * @param createIfNotExists A boolean indicating whether the group should be
     *                          created if it does not exist.
     *
     * @return A ShareGroup.
     * @throws GroupIdNotFoundException if the group does not exist and createIfNotExists is false or
     *                                  if the group is not a consumer group.
     *
     * Package private for testing.
     */
    ShareGroup getOrMaybeCreatePersistedShareGroup(
        String groupId,
        boolean createIfNotExists
    ) throws GroupIdNotFoundException {
        Group group = groups.get(groupId);

        if (group == null && !createIfNotExists) {
            throw new IllegalStateException(String.format("Share group %s not found.", groupId));
        }

        if (group == null) {
            ShareGroup shareGroup = new ShareGroup(snapshotRegistry, groupId);
            groups.put(groupId, shareGroup);
            return shareGroup;
        }

        if (group.type() != SHARE) {
            // We don't support upgrading/downgrading between protocols at the moment so
            // we throw an exception if a group exists with the wrong type.
            throw new GroupIdNotFoundException(String.format("Group %s is not a share group.", groupId));
        }

        return (ShareGroup) group;
    }

    /**
     * Gets a share group by committed offset.
     *
     * @param groupId           The group id.
     * @param committedOffset   A specified committed offset corresponding to this shard.
     *
     * @return A ConsumerGroup.
     * @throws GroupIdNotFoundException if the group does not exist or is not a consumer group.
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
     * Validates the online downgrade if a consumer member is fenced from the consumer group.
     *
     * @param consumerGroup     The ConsumerGroup.
     * @param fencedMemberId    The fenced member id.
     * @return A boolean indicating whether it's valid to online downgrade the consumer group.
     */
    private boolean validateOnlineDowngradeWithFencedMember(ConsumerGroup consumerGroup, String fencedMemberId) {
        if (!consumerGroup.allMembersUseClassicProtocolExcept(fencedMemberId)) {
            return false;
        } else if (consumerGroup.numMembers() <= 1) {
            log.debug("Skip downgrading the consumer group {} to classic group because it's empty.",
                consumerGroup.groupId());
            return false;
        } else if (!config.consumerGroupMigrationPolicy().isDowngradeEnabled()) {
            log.info("Cannot downgrade consumer group {} to classic group because the online downgrade is disabled.",
                consumerGroup.groupId());
            return false;
        } else if (consumerGroup.numMembers() - 1 > config.classicGroupMaxSize()) {
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
     * @param replacedMemberId  The replaced member id.
     *
     * @return A boolean indicating whether it's valid to online downgrade the consumer group.
     */
    private boolean validateOnlineDowngradeWithReplacedMemberId(
        ConsumerGroup consumerGroup,
        String replacedMemberId
    ) {
        if (!consumerGroup.allMembersUseClassicProtocolExcept(replacedMemberId)) {
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
     * @param consumerGroup     The converted ConsumerGroup.
     * @param leavingMemberId   The leaving member that triggers the downgrade validation.
     * @param joiningMember     The newly joined member if the downgrade is triggered by static member replacement.
     * @param records           The record list to which the conversion records are added.
     */
    private void convertToClassicGroup(
        ConsumerGroup consumerGroup,
        String leavingMemberId,
        ConsumerGroupMember joiningMember,
        List<CoordinatorRecord> records
    ) {
        if (joiningMember == null) {
            consumerGroup.createGroupTombstoneRecords(records);
        } else {
            consumerGroup.createGroupTombstoneRecordsWithReplacedMember(records, leavingMemberId, joiningMember.memberId());
        }

        ClassicGroup classicGroup;
        try {
            classicGroup = ClassicGroup.fromConsumerGroup(
                consumerGroup,
                leavingMemberId,
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
        classicGroup.createClassicGroupRecords(metadataImage.features().metadataVersion(), records);

        // Directly update the states instead of replaying the records because
        // the classicGroup reference is needed for triggering the rebalance.
        removeGroup(consumerGroup.groupId());
        groups.put(consumerGroup.groupId(), classicGroup);

        classicGroup.allMembers().forEach(member -> rescheduleClassicGroupMemberHeartbeat(classicGroup, member));

        // If the downgrade is triggered by a member leaving the group, a rebalance should be triggered.
        if (joiningMember == null) {
            prepareRebalance(classicGroup, String.format("Downgrade group %s from consumer to classic.", classicGroup.groupId()));
        }
    }

    /**
     * Validates the online upgrade if the Classic Group receives a ConsumerGroupHeartbeat request.
     *
     * @param classicGroup A ClassicGroup.
     * @return A boolean indicating whether it's valid to online upgrade the classic group.
     */
    private boolean validateOnlineUpgrade(ClassicGroup classicGroup) {
        if (!config.consumerGroupMigrationPolicy().isUpgradeEnabled()) {
            log.info("Cannot upgrade classic group {} to consumer group because the online upgrade is disabled.",
                classicGroup.groupId());
            return false;
        } else if (!classicGroup.usesConsumerGroupProtocol()) {
            log.info("Cannot upgrade classic group {} to consumer group because the group does not use the consumer embedded protocol.",
                classicGroup.groupId());
            return false;
        } else if (classicGroup.numMembers() > config.consumerGroupMaxSize()) {
            log.info("Cannot upgrade classic group {} to consumer group because the group size exceeds the consumer group maximum size.",
                classicGroup.groupId());
            return false;
        }
        return true;
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
                metrics,
                classicGroup,
                metadataImage.topics()
            );
        } catch (SchemaException e) {
            log.warn("Cannot upgrade the classic group " + classicGroup.groupId() +
                " to consumer group because the embedded consumer protocol is malformed: "
                + e.getMessage() + ".", e);

            throw new GroupIdNotFoundException("Cannot upgrade the classic group " + classicGroup.groupId() +
                " to consumer group because the embedded consumer protocol is malformed.");
        }
        consumerGroup.createConsumerGroupRecords(records);

        // Create the session timeouts for the new members. If the conversion fails, the group will remain a
        // classic group, thus these timers will fail the group type check and do nothing.
        consumerGroup.members().forEach((memberId, member) ->
            scheduleConsumerGroupSessionTimeout(consumerGroup.groupId(), memberId, member.classicProtocolSessionTimeout().get())
        );

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
        Group group = groups.remove(groupId);
        if (group != null) {
            switch (group.type()) {
                case CONSUMER:
                    ConsumerGroup consumerGroup = (ConsumerGroup) group;
                    metrics.onConsumerGroupStateTransition(consumerGroup.state(), null);
                    break;
                case CLASSIC:
                    // The classic group size counter is implemented as scheduled task.
                    break;
                case SHARE:
                    // Nothing for now, but we may want to add metrics in the future.
                    break;
                default:
                    log.warn("Removed group {} with an unknown group type {}.", groupId, group.type());
                    break;
            }
        }
    }

    /**
     * Throws an InvalidRequestException if the value is non-null and empty.
     * A string containing only whitespaces is also considered empty.
     *
     * @param value The value.
     * @param error The error message.
     * @throws InvalidRequestException
     */
    private void throwIfEmptyString(
        String value,
        String error
    ) throws InvalidRequestException {
        if (value != null && value.trim().isEmpty()) {
            throw new InvalidRequestException(error);
        }
    }

    /**
     * Throws an InvalidRequestException if the value is non-null.
     *
     * @param value The value.
     * @param error The error message.
     * @throws InvalidRequestException
     */
    private void throwIfNotNull(
        Object value,
        String error
    ) throws InvalidRequestException {
        if (value != null) {
            throw new InvalidRequestException(error);
        }
    }

    /**
     * Throws an InvalidRequestException if the value is null.
     *
     * @param value The value.
     * @param error The error message.
     * @throws InvalidRequestException
     */
    private void throwIfNull(
        Object value,
        String error
    ) throws InvalidRequestException {
        if (value == null) {
            throw new InvalidRequestException(error);
        }
    }

    /**
     * Validates the request.
     *
     * @param request The request to validate.
     * @param apiVersion The version of ConsumerGroupHeartbeat RPC
     * @throws InvalidRequestException if the request is not valid.
     * @throws UnsupportedAssignorException if the assignor is not supported.
     */
    private void throwIfConsumerGroupHeartbeatRequestIsInvalid(
        ConsumerGroupHeartbeatRequestData request,
        short apiVersion
    ) throws InvalidRequestException, UnsupportedAssignorException {
        if (apiVersion >= CONSUMER_GENERATED_MEMBER_ID_REQUIRED_VERSION ||
            request.memberEpoch() > 0 ||
            request.memberEpoch() == LEAVE_GROUP_MEMBER_EPOCH
        ) {
            throwIfEmptyString(request.memberId(), "MemberId can't be empty.");
        }

        throwIfEmptyString(request.groupId(), "GroupId can't be empty.");
        throwIfEmptyString(request.instanceId(), "InstanceId can't be empty.");
        throwIfEmptyString(request.rackId(), "RackId can't be empty.");

        if (request.memberEpoch() == 0) {
            if (request.rebalanceTimeoutMs() == -1) {
                throw new InvalidRequestException("RebalanceTimeoutMs must be provided in first request.");
            }
            if (request.topicPartitions() == null || !request.topicPartitions().isEmpty()) {
                throw new InvalidRequestException("TopicPartitions must be empty when (re-)joining.");
            }
            // We accept members joining with an empty list of names or an empty regex. It basically
            // means that they are not subscribed to any topics, but they are part of the group.
            if (request.subscribedTopicNames() == null && request.subscribedTopicRegex() == null) {
                throw new InvalidRequestException("Either SubscribedTopicNames or SubscribedTopicRegex must" +
                    " be non-null when (re-)joining.");
            }
        } else if (request.memberEpoch() == LEAVE_GROUP_STATIC_MEMBER_EPOCH) {
            throwIfNull(request.instanceId(), "InstanceId can't be null.");
        } else if (request.memberEpoch() < LEAVE_GROUP_STATIC_MEMBER_EPOCH) {
            throw new InvalidRequestException("MemberEpoch is invalid.");
        }

        if (request.serverAssignor() != null && !consumerGroupAssignors.containsKey(request.serverAssignor())) {
            throw new UnsupportedAssignorException("ServerAssignor " + request.serverAssignor()
                + " is not supported. Supported assignors: " + String.join(", ", consumerGroupAssignors.keySet())
                + ".");
        }
    }

    /**
     * Validates the ShareGroupHeartbeat request.
     *
     * @param request The request to validate.
     * @throws InvalidRequestException if the request is not valid.
     * @throws UnsupportedAssignorException if the assignor is not supported.
     */
    private void throwIfShareGroupHeartbeatRequestIsInvalid(
        ShareGroupHeartbeatRequestData request
    ) throws InvalidRequestException, UnsupportedAssignorException {
        throwIfEmptyString(request.memberId(), "MemberId can't be empty.");
        throwIfEmptyString(request.groupId(), "GroupId can't be empty.");
        throwIfEmptyString(request.rackId(), "RackId can't be empty.");

        if (request.memberEpoch() == 0) {
            if (request.subscribedTopicNames() == null || request.subscribedTopicNames().isEmpty()) {
                throw new InvalidRequestException("SubscribedTopicNames must be set in first request.");
            }
        } else if (request.memberEpoch() < ShareGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH) {
            throw new InvalidRequestException("MemberEpoch is invalid.");
        }
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
    private boolean isSubset(
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
            // If the member comes with the previous epoch and has a subset of the current assignment partitions,
            // we accept it because the response with the bumped epoch may have been lost.
            if (receivedMemberEpoch != member.previousMemberEpoch()) {
                throw new FencedMemberEpochException("The share group member has a smaller member "
                        + "epoch (" + receivedMemberEpoch + ") than the one known by the group coordinator ("
                        + member.memberEpoch() + "). The member must abandon all its partitions and rejoin.");
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
     * Validates if the provided regular expression is valid.
     *
     * @param regex The regular expression to validate.
     * @throws InvalidRegularExpression if the regular expression is invalid.
     */
    private static void throwIfRegularExpressionIsInvalid(
        String regex
    ) throws InvalidRegularExpression {
        try {
            Pattern.compile(regex);
        } catch (PatternSyntaxException ex) {
            throw new InvalidRegularExpression(
                String.format("SubscribedTopicRegex `%s` is not a valid regular expression: %s.",
                    regex, ex.getDescription()));
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

    private ConsumerGroupHeartbeatResponseData.Assignment createConsumerGroupResponseAssignment(
        ConsumerGroupMember member
    ) {
        return new ConsumerGroupHeartbeatResponseData.Assignment()
            .setTopicPartitions(fromAssignmentMap(member.assignedPartitions()));
    }

    private ShareGroupHeartbeatResponseData.Assignment createShareGroupResponseAssignment(
        ShareGroupMember member
    ) {
        return new ShareGroupHeartbeatResponseData.Assignment()
            .setTopicPartitions(fromShareGroupAssignmentMap(member.assignedPartitions()));
    }

    private List<ConsumerGroupHeartbeatResponseData.TopicPartitions> fromAssignmentMap(
        Map<Uuid, Set<Integer>> assignment
    ) {
        return assignment.entrySet().stream()
            .map(keyValue -> new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                .setTopicId(keyValue.getKey())
                .setPartitions(new ArrayList<>(keyValue.getValue())))
            .collect(Collectors.toList());
    }

    private List<ShareGroupHeartbeatResponseData.TopicPartitions> fromShareGroupAssignmentMap(
        Map<Uuid, Set<Integer>> assignment
    ) {
        return assignment.entrySet().stream()
            .map(keyValue -> new ShareGroupHeartbeatResponseData.TopicPartitions()
                .setTopicId(keyValue.getKey())
                .setPartitions(new ArrayList<>(keyValue.getValue())))
            .collect(Collectors.toList());
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
     * @param groupId               The group id from the request.
     * @param memberId              The member id from the request.
     * @param memberEpoch           The member epoch from the request.
     * @param instanceId            The instance id from the request or null.
     * @param rackId                The rack id from the request or null.
     * @param rebalanceTimeoutMs    The rebalance timeout from the request or -1.
     * @param clientId              The client id.
     * @param clientHost            The client host.
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
        String groupId,
        String memberId,
        int memberEpoch,
        String instanceId,
        String rackId,
        int rebalanceTimeoutMs,
        String clientId,
        String clientHost,
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
            .setClientId(clientId)
            .setClientHost(clientHost)
            .setClassicMemberMetadata(null)
            .build();

        // If the group is newly created, we must ensure that it moves away from
        // epoch 0 and that it is fully initialized.
        boolean bumpGroupEpoch = group.groupEpoch() == 0;

        bumpGroupEpoch |= hasMemberSubscriptionChanged(
            groupId,
            member,
            updatedMember,
            records
        );

        bumpGroupEpoch |= maybeUpdateRegularExpressions(
            group,
            member,
            updatedMember,
            records
        );

        int groupEpoch = group.groupEpoch();
        Map<String, TopicMetadata> subscriptionMetadata = group.subscriptionMetadata();
        SubscriptionType subscriptionType = group.subscriptionType();

        if (bumpGroupEpoch || group.hasMetadataExpired(currentTimeMs)) {
            // The subscription metadata is updated in two cases:
            // 1) The member has updated its subscriptions;
            // 2) The refresh deadline has been reached.
            Map<String, Integer> subscribedTopicNamesMap = group.computeSubscribedTopicNames(
                member,
                updatedMember
            );
            subscriptionMetadata = group.computeSubscriptionMetadata(
                subscribedTopicNamesMap,
                metadataImage.topics(),
                metadataImage.cluster()
            );

            int numMembers = group.numMembers();
            if (!group.hasMember(updatedMember.memberId()) && !group.hasStaticMember(updatedMember.instanceId())) {
                numMembers++;
            }

            subscriptionType = ModernGroup.subscriptionType(
                subscribedTopicNamesMap,
                numMembers
            );

            if (!subscriptionMetadata.equals(group.subscriptionMetadata())) {
                if (log.isDebugEnabled()) {
                    log.debug("[GroupId {}] Computed new subscription metadata: {}.",
                        groupId, subscriptionMetadata);
                }
                bumpGroupEpoch = true;
                records.add(newConsumerGroupSubscriptionMetadataRecord(groupId, subscriptionMetadata));
            }

            if (bumpGroupEpoch) {
                groupEpoch += 1;
                records.add(newConsumerGroupEpochRecord(groupId, groupEpoch));
                log.info("[GroupId {}] Bumped group epoch to {}.", groupId, groupEpoch);
                metrics.record(CONSUMER_GROUP_REBALANCES_SENSOR_NAME);
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
                member,
                updatedMember,
                subscriptionMetadata,
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
        //    (rebalanceTimeoutMs, subscribedTopicNames and ownedTopicPartitions) to detect a full request
        //    as those must be set in a full request.
        // 2. The member's assignment has been updated.
        boolean isFullRequest = memberEpoch == 0 || (rebalanceTimeoutMs != -1 && subscribedTopicNames != null && ownedTopicPartitions != null);
        if (isFullRequest || hasAssignedPartitionsChanged(member, updatedMember)) {
            response.setAssignment(createConsumerGroupResponseAssignment(updatedMember));
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
        RequestContext context,
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

        if (JoinGroupRequest.requiresKnownMemberId(request, context.apiVersion())) {
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
                Collections.emptyList(),
                true,
                true
            );
        } else {
            member = getOrMaybeSubscribeStaticConsumerGroupMember(
                group,
                memberId,
                -1,
                instanceId,
                Collections.emptyList(),
                isUnknownMember,
                true,
                records
            );
        }

        int groupEpoch = group.groupEpoch();
        Map<String, TopicMetadata> subscriptionMetadata = group.subscriptionMetadata();
        Map<String, Integer> subscribedTopicNamesMap = group.subscribedTopicNames();
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
            .setClientHost(context.clientAddress.toString())
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
            subscribedTopicNamesMap = group.computeSubscribedTopicNames(member, updatedMember);
            subscriptionMetadata = group.computeSubscriptionMetadata(
                subscribedTopicNamesMap,
                metadataImage.topics(),
                metadataImage.cluster()
            );

            int numMembers = group.numMembers();
            if (!group.hasMember(updatedMember.memberId()) && !group.hasStaticMember(updatedMember.instanceId())) {
                numMembers++;
            }

            subscriptionType = ConsumerGroup.subscriptionType(
                subscribedTopicNamesMap,
                numMembers
            );

            if (!subscriptionMetadata.equals(group.subscriptionMetadata())) {
                if (log.isDebugEnabled()) {
                    log.debug("[GroupId {}] Computed new subscription metadata: {}.",
                        groupId, subscriptionMetadata);
                }
                bumpGroupEpoch = true;
                records.add(newConsumerGroupSubscriptionMetadataRecord(groupId, subscriptionMetadata));
            }

            if (bumpGroupEpoch) {
                groupEpoch += 1;
                records.add(newConsumerGroupEpochRecord(groupId, groupEpoch));
                log.info("[GroupId {}] Bumped group epoch to {}.", groupId, groupEpoch);
                metrics.record(CONSUMER_GROUP_REBALANCES_SENSOR_NAME);
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
                member,
                updatedMember,
                subscriptionMetadata,
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
            toTopicPartitions(subscription.ownedPartitions(), metadataImage.topics()),
            records
        );

        // 4. Maybe downgrade the consumer group if the last static member using the
        // consumer protocol is replaced by the joining static member.
        String existingStaticMemberIdOrNull = group.staticMemberId(request.groupInstanceId());
        boolean downgrade = existingStaticMemberIdOrNull != null &&
            validateOnlineDowngradeWithReplacedMemberId(group, existingStaticMemberIdOrNull);
        if (downgrade) {
            convertToClassicGroup(
                group,
                existingStaticMemberIdOrNull,
                updatedMember,
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
     * @return A Result containing the ShareGroupHeartbeat response and
     *         a list of records to update the state machine.
     */
    private CoordinatorResult<ShareGroupHeartbeatResponseData, CoordinatorRecord> shareGroupHeartbeat(
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
        final ShareGroup group = getOrMaybeCreatePersistedShareGroup(groupId, createIfNotExists);
        throwIfShareGroupIsFull(group, memberId);

        // Get or create the member.
        if (memberId.isEmpty()) memberId = Uuid.randomUuid().toString();
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
        );

        int groupEpoch = group.groupEpoch();
        Map<String, TopicMetadata> subscriptionMetadata = group.subscriptionMetadata();
        SubscriptionType subscriptionType = group.subscriptionType();

        if (bumpGroupEpoch || group.hasMetadataExpired(currentTimeMs)) {
            // The subscription metadata is updated in two cases:
            // 1) The member has updated its subscriptions;
            // 2) The refresh deadline has been reached.
            Map<String, Integer> subscribedTopicNamesMap = group.computeSubscribedTopicNames(member, updatedMember);
            subscriptionMetadata = group.computeSubscriptionMetadata(
                subscribedTopicNamesMap,
                metadataImage.topics(),
                metadataImage.cluster()
            );

            int numMembers = group.numMembers();
            if (!group.hasMember(updatedMember.memberId())) {
                numMembers++;
            }

            subscriptionType = ModernGroup.subscriptionType(
                subscribedTopicNamesMap,
                numMembers
            );

            if (!subscriptionMetadata.equals(group.subscriptionMetadata())) {
                log.info("[GroupId {}] Computed new subscription metadata: {}.",
                    groupId, subscriptionMetadata);
                bumpGroupEpoch = true;
                records.add(newShareGroupSubscriptionMetadataRecord(groupId, subscriptionMetadata));
            }

            if (bumpGroupEpoch) {
                groupEpoch += 1;
                records.add(newShareGroupEpochRecord(groupId, groupEpoch));
                log.info("[GroupId {}] Bumped group epoch to {}.", groupId, groupEpoch);
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
                subscriptionMetadata,
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
            records
        );

        scheduleShareGroupSessionTimeout(groupId, memberId);

        // Prepare the response.
        ShareGroupHeartbeatResponseData response = new ShareGroupHeartbeatResponseData()
            .setMemberId(updatedMember.memberId())
            .setMemberEpoch(updatedMember.memberEpoch())
            .setHeartbeatIntervalMs(shareGroupHeartbeatIntervalMs(groupId));

        // The assignment is only provided in the following cases:
        // 1. The member just joined or rejoined to group (epoch equals to zero);
        // 2. The member's assignment has been updated.
        if (memberEpoch == 0 || hasAssignedPartitionsChanged(member, updatedMember)) {
            response.setAssignment(createShareGroupResponseAssignment(updatedMember));
        }

        return new CoordinatorResult<>(records, response);
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

                // Generate the records to replace the member.
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
     * @throws InvalidRegularExpression if the regular expression is invalid.
     */
    private boolean hasMemberSubscriptionChanged(
        String groupId,
        ConsumerGroupMember member,
        ConsumerGroupMember updatedMember,
        List<CoordinatorRecord> records
    ) throws InvalidRegularExpression {
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

    /**
     * Check whether the member has updated its subscribed topic regular expression and
     * may trigger the resolution/the refresh of all the regular expressions in the
     * group. We align the refreshment of the regular expression in order to have
     * them trigger only one rebalance per update.
     *
     * @param group         The consumer group.
     * @param member        The old member.
     * @param updatedMember The new member.
     * @param records       The records accumulator.
     * @return Whether a rebalance must be triggered.
     */
    private boolean maybeUpdateRegularExpressions(
        ConsumerGroup group,
        ConsumerGroupMember member,
        ConsumerGroupMember updatedMember,
        List<CoordinatorRecord> records
    ) {
        String groupId = group.groupId();
        String memberId = updatedMember.memberId();
        String oldSubscribedTopicRegex = member.subscribedTopicRegex();
        String newSubscribedTopicRegex = updatedMember.subscribedTopicRegex();

        boolean bumpGroupEpoch = false;
        boolean requireRefresh = false;

        // Check whether the member has changed its subscribed regex.
        if (!Objects.equals(oldSubscribedTopicRegex, newSubscribedTopicRegex)) {
            log.debug("[GroupId {}] Member {} updated its subscribed regex to: {}.",
                groupId, memberId, newSubscribedTopicRegex);

            if (isNotEmpty(oldSubscribedTopicRegex) && group.numSubscribedMembers(oldSubscribedTopicRegex) == 1) {
                // If the member was the last one subscribed to the regex, we delete the
                // resolved regular expression.
                records.add(GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionTombstone(
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
                    bumpGroupEpoch = group.resolvedRegularExpression(newSubscribedTopicRegex).isPresent();
                }
            }
        }

        // Conditions to trigger a refresh:
        // 0. The group is subscribed to regular expressions.
        // 1. There is no ongoing refresh for the group.
        // 2. The last refresh is older than 10s.
        // 3. The group has unresolved regular expressions.
        // 4. The metadata image has new topics.

        // 0. The group is subscribed to regular expressions. We also take the one
        //    that the current may have just introduced.
        if (!requireRefresh && group.subscribedRegularExpressions().isEmpty()) {
            return bumpGroupEpoch;
        }

        // 1. There is no ongoing refresh for the group.
        String key = group.groupId() + "-regex";
        if (executor.isScheduled(key)) {
            return bumpGroupEpoch;
        }

        // 2. The last refresh is older than 10s. If the group does not have any regular
        //    expressions but the current member just brought a new one, we should continue.
        long lastRefreshTimeMs = group.lastResolvedRegularExpressionRefreshTimeMs();
        if (time.milliseconds() <= lastRefreshTimeMs + REGEX_BATCH_REFRESH_INTERVAL_MS) {
            return bumpGroupEpoch;
        }

        // 3. The group has unresolved regular expressions.
        Map<String, Integer> subscribedRegularExpressions = new HashMap<>(group.subscribedRegularExpressions());
        if (isNotEmpty(oldSubscribedTopicRegex)) {
            subscribedRegularExpressions.compute(oldSubscribedTopicRegex, Utils::decValue);
        }
        if (isNotEmpty(newSubscribedTopicRegex)) {
            subscribedRegularExpressions.compute(newSubscribedTopicRegex, Utils::incValue);
        }

        requireRefresh |= subscribedRegularExpressions.size() != group.numResolvedRegularExpressions();

        // 4. The metadata has new topics that we must consider.
        requireRefresh |= group.lastResolvedRegularExpressionVersion() < lastMetadataImageWithNewTopics;

        if (requireRefresh && !subscribedRegularExpressions.isEmpty()) {
            Set<String> regexes = Collections.unmodifiableSet(subscribedRegularExpressions.keySet());
            executor.schedule(
                key,
                () -> refreshRegularExpressions(groupId, log, time, metadataImage, regexes),
                (result, exception) -> handleRegularExpressionsResult(groupId, result, exception)
            );
        }

        return bumpGroupEpoch;
    }

    /**
     * Resolves the provided regular expressions. Note that this static method is executed
     * as an asynchronous task in the executor. Hence, it should not access any state from
     * the manager.
     *
     * @param groupId   The group id.
     * @param log       The log instance.
     * @param time      The time instance.
     * @param image     The metadata image to use for listing the topics.
     * @param regexes   The list of regular expressions that must be resolved.
     * @return The list of resolved regular expressions.
     *
     * public for benchmarks.
     */
    public static Map<String, ResolvedRegularExpression> refreshRegularExpressions(
        String groupId,
        Logger log,
        Time time,
        MetadataImage image,
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

        for (String topicName : image.topics().topicsByName().keySet()) {
            for (Pattern regex : compiledRegexes) {
                if (regex.matcher(topicName).matches()) {
                    resolvedRegexes.get(regex.pattern()).add(topicName);
                }
            }
        }

        long version = image.provenance().lastContainedOffset();
        Map<String, ResolvedRegularExpression> result = new HashMap<>(resolvedRegexes.size());
        for (Map.Entry<String, Set<String>> resolvedRegex : resolvedRegexes.entrySet()) {
            result.put(
                resolvedRegex.getKey(),
                new ResolvedRegularExpression(resolvedRegex.getValue(), version, startTimeMs)
            );
        }

        log.info("[GroupId {}] Scanned {} topics to refresh regular expressions {} in {}ms.",
            groupId, image.topics().topicsByName().size(), resolvedRegexes.keySet(),
            time.milliseconds() - startTimeMs);

        return result;
    }

    /**
     * Handle the result of the asynchronous tasks which resolves the regular expressions.
     *
     * @param resolvedRegularExpressions    The resolved regular expressions.
     * @param exception                     The exception if the resolution failed.
     * @return A CoordinatorResult containing the records to mutate the group state.
     */
    private CoordinatorResult<Void, CoordinatorRecord> handleRegularExpressionsResult(
        String groupId,
        Map<String, ResolvedRegularExpression> resolvedRegularExpressions,
        Throwable exception
    ) {
        if (exception != null) {
            log.error("[GroupId {}] Couldn't update regular expression due to: {}",
                groupId, exception.getMessage());
            return new CoordinatorResult<>(Collections.emptyList());
        }

        if (log.isDebugEnabled()) {
            log.debug("[GroupId {}] Received updated regular expressions: {}.",
                groupId, resolvedRegularExpressions);
        }

        List<CoordinatorRecord> records = new ArrayList<>();
        try {
            ConsumerGroup group = consumerGroup(groupId);
            Map<String, Integer> subscribedTopicNames = new HashMap<>(group.subscribedTopicNames());

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

                if (!oldResolvedRegularExpression.topics.equals(newResolvedRegularExpression.topics)) {
                    bumpGroupEpoch = true;

                    oldResolvedRegularExpression.topics.forEach(topicName ->
                        subscribedTopicNames.compute(topicName, Utils::decValue)
                    );

                    newResolvedRegularExpression.topics.forEach(topicName ->
                        subscribedTopicNames.compute(topicName, Utils::incValue)
                    );
                }

                // Add the record to persist the change.
                records.add(GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionRecord(
                    groupId,
                    regex,
                    newResolvedRegularExpression
                ));
            }

            // Compute the subscription metadata.
            Map<String, TopicMetadata> subscriptionMetadata = group.computeSubscriptionMetadata(
                subscribedTopicNames,
                metadataImage.topics(),
                metadataImage.cluster()
            );

            if (!subscriptionMetadata.equals(group.subscriptionMetadata())) {
                if (log.isDebugEnabled()) {
                    log.debug("[GroupId {}] Computed new subscription metadata: {}.",
                        groupId, subscriptionMetadata);
                }
                bumpGroupEpoch = true;
                records.add(newConsumerGroupSubscriptionMetadataRecord(groupId, subscriptionMetadata));
            }

            if (bumpGroupEpoch) {
                int groupEpoch = group.groupEpoch() + 1;
                records.add(newConsumerGroupEpochRecord(groupId, groupEpoch));
                log.info("[GroupId {}] Bumped group epoch to {}.", groupId, groupEpoch);
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
     * Reconciles the current assignment of the member towards the target assignment if needed.
     *
     * @param groupId               The group id.
     * @param member                The member to reconcile.
     * @param currentPartitionEpoch The function returning the current epoch of
     *                              a given partition.
     * @param targetAssignmentEpoch The target assignment epoch.
     * @param targetAssignment      The target assignment.
     * @param ownedTopicPartitions  The list of partitions owned by the member. This
     *                              is reported in the ConsumerGroupHeartbeat API and
     *                              it could be null if not provided.
     * @param records               The list to accumulate any new records.
     * @return The received member if no changes have been made; or a new
     *         member containing the new assignment.
     */
    private ConsumerGroupMember maybeReconcile(
        String groupId,
        ConsumerGroupMember member,
        BiFunction<Uuid, Integer, Integer> currentPartitionEpoch,
        int targetAssignmentEpoch,
        Assignment targetAssignment,
        List<ConsumerGroupHeartbeatRequestData.TopicPartitions> ownedTopicPartitions,
        List<CoordinatorRecord> records
    ) {
        if (member.isReconciledTo(targetAssignmentEpoch)) {
            return member;
        }

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(targetAssignmentEpoch, targetAssignment)
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
                    cancelConsumerGroupRebalanceTimeout(groupId, updatedMember.memberId());
                }
            }
        }

        return updatedMember;
    }

    /**
     * Reconciles the current assignment of the member towards the target assignment if needed.
     *
     * @param groupId               The group id.
     * @param member                The member to reconcile.
     * @param targetAssignmentEpoch The target assignment epoch.
     * @param targetAssignment      The target assignment.
     * @param records               The list to accumulate any new records.
     * @return The received member if no changes have been made; or a new
     *         member containing the new assignment.
     */
    private ShareGroupMember maybeReconcile(
        String groupId,
        ShareGroupMember member,
        int targetAssignmentEpoch,
        Assignment targetAssignment,
        List<CoordinatorRecord> records
    ) {
        if (member.isReconciledTo(targetAssignmentEpoch)) {
            return member;
        }

        ShareGroupMember updatedMember = new ShareGroupAssignmentBuilder(member)
            .withTargetAssignment(targetAssignmentEpoch, targetAssignment)
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
     * Updates the target assignment according to the updated member and subscription metadata.
     *
     * @param group                 The ConsumerGroup.
     * @param groupEpoch            The group epoch.
     * @param member                The existing member.
     * @param updatedMember         The updated member.
     * @param subscriptionMetadata  The subscription metadata.
     * @param subscriptionType      The group subscription type.
     * @param records               The list to accumulate any new records.
     * @return The new target assignment.
     */
    private Assignment updateTargetAssignment(
        ConsumerGroup group,
        int groupEpoch,
        ConsumerGroupMember member,
        ConsumerGroupMember updatedMember,
        Map<String, TopicMetadata> subscriptionMetadata,
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
                    .withSubscriptionMetadata(subscriptionMetadata)
                    .withSubscriptionType(subscriptionType)
                    .withTargetAssignment(group.targetAssignment())
                    .withInvertedTargetAssignment(group.invertedTargetAssignment())
                    .withTopicsImage(metadataImage.topics())
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
            log.error("[GroupId {}] {}.", group.groupId(), msg);
            throw new UnknownServerException(msg, ex);
        }
    }

    /**
     * Updates the target assignment according to the updated member and subscription metadata.
     *
     * @param group                 The ShareGroup.
     * @param groupEpoch            The group epoch.
     * @param updatedMember         The updated member.
     * @param subscriptionMetadata  The subscription metadata.
     * @param subscriptionType      The group subscription type.
     * @param records               The list to accumulate any new records.
     * @return The new target assignment.
     */
    private Assignment updateTargetAssignment(
        ShareGroup group,
        int groupEpoch,
        ShareGroupMember updatedMember,
        Map<String, TopicMetadata> subscriptionMetadata,
        SubscriptionType subscriptionType,
        List<CoordinatorRecord> records
    ) {
        try {
            TargetAssignmentBuilder.ShareTargetAssignmentBuilder assignmentResultBuilder =
                new TargetAssignmentBuilder.ShareTargetAssignmentBuilder(group.groupId(), groupEpoch, shareGroupAssignor)
                    .withMembers(group.members())
                    .withSubscriptionMetadata(subscriptionMetadata)
                    .withSubscriptionType(subscriptionType)
                    .withTargetAssignment(group.targetAssignment())
                    .withInvertedTargetAssignment(group.invertedTargetAssignment())
                    .withTopicsImage(metadataImage.topics())
                    .addOrUpdateMember(updatedMember.memberId(), updatedMember);

            long startTimeMs = time.milliseconds();
            TargetAssignmentBuilder.TargetAssignmentResult assignmentResult =
                assignmentResultBuilder.build();
            long assignorTimeMs = time.milliseconds() - startTimeMs;

            log.info("[GroupId {}] Computed a new target assignment for epoch {} with '{}' assignor in {}ms: {}.",
                group.groupId(), groupEpoch, shareGroupAssignor, assignorTimeMs, assignmentResult.targetAssignment());

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
            log.error("[GroupId {}] {}.", group.groupId(), msg);
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
            .setPartitionsPendingRevocation(Collections.emptyMap())
            .build();

        return new CoordinatorResult<>(
            Collections.singletonList(newConsumerGroupCurrentAssignmentRecord(group.groupId(), leavingStaticMember)),
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
        List<CoordinatorRecord> records = new ArrayList<>();
        if (validateOnlineDowngradeWithFencedMember(group, member.memberId())) {
            convertToClassicGroup(group, member.memberId(), null, records);
            return new CoordinatorResult<>(records, response, null, false);
        } else {
            removeMember(records, group.groupId(), member.memberId());

            // We update the subscription metadata without the leaving member.
            Map<String, TopicMetadata> subscriptionMetadata = group.computeSubscriptionMetadata(
                group.computeSubscribedTopicNames(member, null),
                metadataImage.topics(),
                metadataImage.cluster()
            );

            if (!subscriptionMetadata.equals(group.subscriptionMetadata())) {
                if (log.isDebugEnabled()) {
                    log.debug("[GroupId {}] Computed new subscription metadata: {}.",
                        group.groupId(), subscriptionMetadata);
                }
                records.add(newConsumerGroupSubscriptionMetadataRecord(group.groupId(), subscriptionMetadata));
            }

            // We bump the group epoch.
            int groupEpoch = group.groupEpoch() + 1;
            records.add(newConsumerGroupEpochRecord(group.groupId(), groupEpoch));

            cancelTimers(group.groupId(), member.memberId());

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
        Map<String, TopicMetadata> subscriptionMetadata = group.computeSubscriptionMetadata(
            group.computeSubscribedTopicNames(member, null),
            metadataImage.topics(),
            metadataImage.cluster()
        );

        if (!subscriptionMetadata.equals(group.subscriptionMetadata())) {
            log.info("[GroupId {}] Computed new subscription metadata: {}.",
                group.groupId(), subscriptionMetadata);
            records.add(newShareGroupSubscriptionMetadataRecord(group.groupId(), subscriptionMetadata));
        }

        // We bump the group epoch.
        int groupEpoch = group.groupEpoch() + 1;
        records.add(newShareGroupEpochRecord(group.groupId(), groupEpoch));

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
        cancelConsumerGroupRebalanceTimeout(groupId, memberId);
        cancelConsumerGroupJoinTimeout(groupId, memberId);
        cancelConsumerGroupSyncTimeout(groupId, memberId);
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

        return new CoordinatorResult<>(Collections.emptyList());
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

        return new CoordinatorResult<>(Collections.emptyList());
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
        String key = consumerGroupRebalanceTimeoutKey(groupId, memberId);
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
                    return new CoordinatorResult<>(Collections.emptyList());
                }
            } catch (GroupIdNotFoundException ex) {
                log.debug("[GroupId {}] Could not fence {}} because the group does not exist.",
                    groupId, memberId);
            } catch (UnknownMemberIdException ex) {
                log.debug("[GroupId {}] Could not fence {} because the member does not exist.",
                    groupId, memberId);
            }

            return new CoordinatorResult<>(Collections.emptyList());
        });
    }

    /**
     * Cancels the rebalance timeout of the member.
     *
     * @param groupId       The group id.
     * @param memberId      The member id.
     */
    private void cancelConsumerGroupRebalanceTimeout(
        String groupId,
        String memberId
    ) {
        timer.cancel(consumerGroupRebalanceTimeoutKey(groupId, memberId));
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
        RequestContext context,
        ConsumerGroupHeartbeatRequestData request
    ) throws ApiException {
        throwIfConsumerGroupHeartbeatRequestIsInvalid(request, context.apiVersion());

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
                request.groupId(),
                request.memberId(),
                request.memberEpoch(),
                request.instanceId(),
                request.rackId(),
                request.rebalanceTimeoutMs(),
                context.clientId(),
                context.clientAddress.toString(),
                request.subscribedTopicNames(),
                request.subscribedTopicRegex(),
                request.serverAssignor(),
                request.topicPartitions()
            );
        }
    }

    /**
     * Handles a ShareGroupHeartbeat request.
     *
     * @param context The request context.
     * @param request The actual ShareGroupHeartbeat request.
     *
     * @return A Result containing the ShareGroupHeartbeat response and
     *         a list of records to update the state machine.
     */
    public CoordinatorResult<ShareGroupHeartbeatResponseData, CoordinatorRecord> shareGroupHeartbeat(
        RequestContext context,
        ShareGroupHeartbeatRequestData request
    ) throws ApiException {
        throwIfShareGroupHeartbeatRequestIsInvalid(request);

        if (request.memberEpoch() == ShareGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH) {
            // -1 means that the member wants to leave the group.
            return shareGroupLeave(
                request.groupId(),
                request.memberId(),
                request.memberEpoch());
        }
        // Otherwise, it is a regular heartbeat.
        return shareGroupHeartbeat(
            request.groupId(),
            request.memberId(),
            request.memberEpoch(),
            request.rackId(),
            context.clientId(),
            context.clientAddress.toString(),
            request.subscribedTopicNames());
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
        return groups != null ? groups : Collections.emptySet();
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
            return groupIds.isEmpty() ? null : groupIds;
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

        if (value != null) {
            Map<String, TopicMetadata> subscriptionMetadata = new HashMap<>();
            value.topics().forEach(topicMetadata -> {
                subscriptionMetadata.put(topicMetadata.topicName(), TopicMetadata.fromRecord(topicMetadata));
            });
            group.setSubscriptionMetadata(subscriptionMetadata);
        } else {
            group.setSubscriptionMetadata(Collections.emptyMap());
        }
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
                .setAssignedPartitions(Collections.emptyMap())
                .setPartitionsPendingRevocation(Collections.emptyMap())
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

        if (value != null) {
            ConsumerGroup group = getOrMaybeCreatePersistedConsumerGroup(groupId, true);
            group.updateResolvedRegularExpression(
                regex,
                new ResolvedRegularExpression(
                    new HashSet<>(value.topics()),
                    value.version(),
                    value.timestamp()
                )
            );
        } else {
            try {
                ConsumerGroup group = getOrMaybeCreatePersistedConsumerGroup(groupId, false);
                group.removeResolvedRegularExpression(regex);
            } catch (GroupIdNotFoundException ex) {
                // If the group does not exist, we can ignore the tombstone.
            }
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

        ShareGroup shareGroup = getOrMaybeCreatePersistedShareGroup(groupId, value != null);
        Set<String> oldSubscribedTopicNames = new HashSet<>(shareGroup.subscribedTopicNames().keySet());

        if (value != null) {
            ShareGroupMember oldMember = shareGroup.getOrMaybeCreateMember(memberId, true);
            shareGroup.updateMember(new ShareGroupMember.Builder(oldMember)
                .updateWith(value)
                .build());
        } else {
            ShareGroupMember oldMember = shareGroup.getOrMaybeCreateMember(memberId, false);
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

        if (value != null) {
            ShareGroup shareGroup = getOrMaybeCreatePersistedShareGroup(groupId, true);
            shareGroup.setGroupEpoch(value.epoch());
        } else {
            ShareGroup shareGroup = getOrMaybeCreatePersistedShareGroup(groupId, false);
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
     * Replays ShareGroupPartitionMetadataKey/Value to update the hard state of
     * the share group. It updates the subscription metadata of the share
     * group.
     *
     * @param key   A ShareGroupPartitionMetadataKey key.
     * @param value A ShareGroupPartitionMetadataValue record.
     */
    public void replay(
        ShareGroupPartitionMetadataKey key,
        ShareGroupPartitionMetadataValue value
    ) {
        String groupId = key.groupId();
        ShareGroup group = getOrMaybeCreatePersistedShareGroup(groupId, false);

        if (value != null) {
            Map<String, TopicMetadata> subscriptionMetadata = new HashMap<>();
            value.topics().forEach(topicMetadata ->
                subscriptionMetadata.put(topicMetadata.topicName(), TopicMetadata.fromRecord(topicMetadata))
            );
            group.setSubscriptionMetadata(subscriptionMetadata);
        } else {
            group.setSubscriptionMetadata(Collections.emptyMap());
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
        ShareGroup group = getOrMaybeCreatePersistedShareGroup(groupId, false);

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
        ShareGroup group = getOrMaybeCreatePersistedShareGroup(groupId, false);

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

        ShareGroup group = getOrMaybeCreatePersistedShareGroup(groupId, false);
        ShareGroupMember oldMember = group.getOrMaybeCreateMember(memberId, false);

        if (value != null) {
            ShareGroupMember newMember = new ShareGroupMember.Builder(oldMember)
                    .updateWith(value)
                    .build();
            group.updateMember(newMember);
        } else {
            ShareGroupMember newMember = new ShareGroupMember.Builder(oldMember)
                    .setMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH)
                    .setPreviousMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH)
                    .setAssignedPartitions(Collections.emptyMap())
                    .build();
            group.updateMember(newMember);
        }
    }

    /**
     * A new metadata image is available.
     *
     * @param newImage  The new metadata image.
     * @param delta     The delta image.
     */
    public void onNewMetadataImage(MetadataImage newImage, MetadataDelta delta) {
        metadataImage = newImage;

        // Initialize the last offset if it was not yet.
        if (lastMetadataImageWithNewTopics == -1L) {
            lastMetadataImageWithNewTopics = metadataImage.provenance().lastContainedOffset();
        }

        TopicsDelta topicsDelta = delta.topicsDelta();
        if (topicsDelta == null) return;

        // Updated the last offset of the image with newly created topics. This is used to
        // trigger a refresh of all the regular expressions when topics are created. Note
        // that we don't trigger a refresh when topics are deleted. Those are removed from
        // the subscription metadata (and the assignment) via the above mechanism. The
        // resolved regular expressions are cleaned up on the next refresh.
        if (!topicsDelta.createdTopicIds().isEmpty()) {
            lastMetadataImageWithNewTopics = metadataImage.provenance().lastContainedOffset();
        }

        // Notify all the groups subscribed to the created, updated or
        // deleted topics.
        Set<String> allGroupIds = new HashSet<>();
        topicsDelta.changedTopics().forEach((topicId, topicDelta) -> {
            String topicName = topicDelta.name();
            allGroupIds.addAll(groupsSubscribedToTopic(topicName));
        });
        topicsDelta.deletedTopicIds().forEach(topicId -> {
            TopicImage topicImage = delta.image().topics().getTopic(topicId);
            allGroupIds.addAll(groupsSubscribedToTopic(topicImage.name()));
        });
        allGroupIds.forEach(groupId -> {
            Group group = groups.get(groupId);
            if (group != null && (group.type() == CONSUMER || group.type() == SHARE)) {
                ((ModernGroup<?>) group).requestMetadataRefresh();
            }
        });
    }

    /**
     * Counts and updates the number of classic groups in different states.
     */
    public void updateClassicGroupSizeCounter() {
        Map<ClassicGroupState, Long> groupSizeCounter = new HashMap<>();
        groups.forEach((__, group) -> {
            if (group.type() == CLASSIC) {
                groupSizeCounter.compute(((ClassicGroup) group).currentState(), Utils::incValue);
            }
        });
        metrics.setClassicGroupGauges(groupSizeCounter);
    }

    /**
     * The coordinator has been loaded. Session timeouts are registered
     * for all members.
     */
    public void onLoaded() {
        Map<ClassicGroupState, Long> classicGroupSizeCounter = new HashMap<>();
        groups.forEach((groupId, group) -> {
            switch (group.type()) {
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
                    // Nothing for now for the ShareGroup, as no members are persisted.
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

    public static String consumerGroupRebalanceTimeoutKey(String groupId, String memberId) {
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
        RequestContext context,
        JoinGroupRequestData request,
        CompletableFuture<JoinGroupResponseData> responseFuture
    ) {
        Group group = groups.get(request.groupId(), Long.MAX_VALUE);
        if (group != null) {
            if (group.type() == CONSUMER && !group.isEmpty()) {
                // classicGroupJoinToConsumerGroup takes the join requests to non-empty consumer groups.
                // The empty consumer groups should be converted to classic groups in classicGroupJoinToClassicGroup.
                return classicGroupJoinToConsumerGroup((ConsumerGroup) group, context, request, responseFuture);
            } else if (group.type() == CONSUMER || group.type() == CLASSIC) {
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
        RequestContext context,
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
        maybeDeleteEmptyConsumerGroup(groupId, records);
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
                GroupCoordinatorRecordHelpers.newEmptyGroupMetadataRecord(group, metadataImage.features().metadataVersion())
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
        RequestContext context,
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
        RequestContext context,
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
        RequestContext context,
        JoinGroupRequestData request,
        ClassicGroup group,
        String newMemberId,
        CompletableFuture<JoinGroupResponseData> responseFuture
    ) {
        if (JoinGroupRequest.requiresKnownMemberId(context.apiVersion())) {
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
        RequestContext context,
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
                            group.currentClassicGroupMembers() : Collections.emptyList())
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
                        .setMembers(Collections.emptyList())
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

                List<CoordinatorRecord> records = Collections.singletonList(GroupCoordinatorRecordHelpers.newGroupMetadataRecord(
                    group, Collections.emptyMap(), metadataImage.features().metadataVersion()));

                return new CoordinatorResult<>(records, appendFuture, false);

            } else {
                log.info("Stabilized group {} generation {} with {} members.",
                    groupId, group.generationId(), group.numMembers());

                // Complete the awaiting join group response future for all the members after rebalancing
                group.allMembers().forEach(member -> {
                    List<JoinGroupResponseData.JoinGroupResponseMember> members = Collections.emptyList();
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
        RequestContext context,
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
        RequestContext context,
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

                    } else if (JoinGroupRequest.supportsSkippingAssignment(context.apiVersion())) {
                        boolean isLeader = group.isLeader(newMemberId);

                        group.completeJoinFuture(newMember, new JoinGroupResponseData()
                            .setMembers(isLeader ? group.currentClassicGroupMembers() : Collections.emptyList())
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

                List<CoordinatorRecord> records = Collections.singletonList(
                    GroupCoordinatorRecordHelpers.newGroupMetadataRecord(group, groupAssignment, metadataImage.features().metadataVersion())
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
        RequestContext context,
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
        RequestContext context,
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

                List<CoordinatorRecord> records = Collections.singletonList(
                    GroupCoordinatorRecordHelpers.newGroupMetadataRecord(group, assignment, metadataImage.features().metadataVersion())
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
        RequestContext context,
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

        return new CoordinatorResult<>(Collections.emptyList(), appendFuture, false);
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
                    metadataImage.topics()
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
        RequestContext context,
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
        RequestContext context,
        HeartbeatRequestData request
    ) {
        validateClassicGroupHeartbeat(group, request.memberId(), request.groupInstanceId(), request.generationId());

        switch (group.currentState()) {
            case EMPTY:
                return new CoordinatorResult<>(
                    Collections.emptyList(),
                    new HeartbeatResponseData().setErrorCode(Errors.UNKNOWN_MEMBER_ID.code())
                );

            case PREPARING_REBALANCE:
                rescheduleClassicGroupMemberHeartbeat(group, group.member(request.memberId()));
                return new CoordinatorResult<>(
                    Collections.emptyList(),
                    new HeartbeatResponseData().setErrorCode(Errors.REBALANCE_IN_PROGRESS.code())
                );

            case COMPLETING_REBALANCE:
            case STABLE:
                // Consumers may start sending heartbeats after join-group response, while the group
                // is in CompletingRebalance state. In this case, we should treat them as
                // normal heartbeat requests and reset the timer
                rescheduleClassicGroupMemberHeartbeat(group, group.member(request.memberId()));
                return new CoordinatorResult<>(
                    Collections.emptyList(),
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
        RequestContext context,
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
            Collections.emptyList(),
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
        RequestContext context,
        LeaveGroupRequestData request
    ) throws UnknownMemberIdException {
        Group group;
        try {
            group = group(request.groupId());
        } catch (GroupIdNotFoundException e) {
            throw new UnknownMemberIdException(String.format("Group %s not found.", request.groupId()));
        }

        if (group.type() == CLASSIC) {
            return classicGroupLeaveToClassicGroup((ClassicGroup) group, context, request);
        } else if (group.type() == CONSUMER) {
            return classicGroupLeaveToConsumerGroup((ConsumerGroup) group, context, request);
        } else {
            throw new UnknownMemberIdException(String.format("Group %s not found.", request.groupId()));
        }
    }

    /**
     * Handle a classic LeaveGroupRequest to a ConsumerGroup.
     *
     * @param group          The ConsumerGroup.
     * @param context        The request context.
     * @param request        The actual LeaveGroup request.
     *
     * @return The LeaveGroup response and the records to append.
     */
    private CoordinatorResult<LeaveGroupResponseData, CoordinatorRecord> classicGroupLeaveToConsumerGroup(
        ConsumerGroup group,
        RequestContext context,
        LeaveGroupRequestData request
    ) throws UnknownMemberIdException {
        String groupId = group.groupId();
        List<MemberResponse> memberResponses = new ArrayList<>();
        Set<ConsumerGroupMember> validLeaveGroupMembers = new HashSet<>();
        List<CoordinatorRecord> records = new ArrayList<>();

        for (MemberIdentity memberIdentity : request.members()) {
            String memberId = memberIdentity.memberId();
            String instanceId = memberIdentity.groupInstanceId();
            String reason = memberIdentity.reason() != null ? memberIdentity.reason() : "not provided";

            ConsumerGroupMember member;
            try {
                if (instanceId == null) {
                    member = group.getOrMaybeCreateMember(memberId, false);
                    throwIfMemberDoesNotUseClassicProtocol(member);

                    log.info("[Group {}] Dynamic member {} has left group " +
                            "through explicit `LeaveGroup` request; client reason: {}",
                        groupId, memberId, reason);
                } else {
                    member = group.staticMember(instanceId);
                    throwIfStaticMemberIsUnknown(member, instanceId);
                    // The LeaveGroup API allows administrative removal of members by GroupInstanceId
                    // in which case we expect the MemberId to be undefined.
                    if (!UNKNOWN_MEMBER_ID.equals(memberId)) {
                        throwIfInstanceIdIsFenced(member, groupId, memberId, instanceId);
                    }
                    throwIfMemberDoesNotUseClassicProtocol(member);

                    memberId = member.memberId();
                    log.info("[Group {}] Static member {} with instance id {} has left group " +
                            "through explicit `LeaveGroup` request; client reason: {}",
                        groupId, memberId, instanceId, reason);
                }

                removeMember(records, groupId, memberId);
                cancelTimers(groupId, memberId);
                memberResponses.add(
                    new MemberResponse()
                        .setMemberId(memberId)
                        .setGroupInstanceId(instanceId)
                );
                validLeaveGroupMembers.add(member);
            } catch (KafkaException e) {
                memberResponses.add(
                    new MemberResponse()
                        .setMemberId(memberId)
                        .setGroupInstanceId(instanceId)
                        .setErrorCode(Errors.forException(e).code())
                );
            }
        }

        if (!records.isEmpty()) {
            // Maybe update the subscription metadata.
            Map<String, TopicMetadata> subscriptionMetadata = group.computeSubscriptionMetadata(
                group.computeSubscribedTopicNames(validLeaveGroupMembers),
                metadataImage.topics(),
                metadataImage.cluster()
            );

            if (!subscriptionMetadata.equals(group.subscriptionMetadata())) {
                log.info("[GroupId {}] Computed new subscription metadata: {}.",
                    group.groupId(), subscriptionMetadata);
                records.add(newConsumerGroupSubscriptionMetadataRecord(group.groupId(), subscriptionMetadata));
            }

            // Bump the group epoch.
            records.add(newConsumerGroupEpochRecord(groupId, group.groupEpoch() + 1));
        }

        return new CoordinatorResult<>(records, new LeaveGroupResponseData().setMembers(memberResponses));
    }

    /**
     * Handle a classic LeaveGroupRequest to a ClassicGroup.
     *
     * @param group          The ClassicGroup.
     * @param context        The request context.
     * @param request        The actual LeaveGroup request.
     *
     * @return The LeaveGroup response and the GroupMetadata record to append if the group
     *         no longer has any members.
     */
    private CoordinatorResult<LeaveGroupResponseData, CoordinatorRecord> classicGroupLeaveToClassicGroup(
        ClassicGroup group,
        RequestContext context,
        LeaveGroupRequestData request
    ) throws UnknownMemberIdException {
        if (group.isInState(DEAD)) {
            return new CoordinatorResult<>(
                Collections.emptyList(),
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
            .collect(Collectors.toList());

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
     * Validations are done in {@link GroupCoordinatorShard#deleteGroups(RequestContext, List)} by
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
     * Write tombstones for the group if it's empty and is a classic group.
     *
     * @param group     The group to be deleted.
     * @param records   The list of records to delete the group.
     *
     * @return true if the group is empty
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
     */
    private void maybeDeleteEmptyConsumerGroup(String groupId, List<CoordinatorRecord> records) {
        Group group = groups.get(groupId, Long.MAX_VALUE);
        if (isEmptyConsumerGroup(group)) {
            // Add tombstones for the previous consumer group. The tombstones won't actually be
            // replayed because its coordinator result has a non-null appendFuture.
            createGroupTombstoneRecords(group, records);
            removeGroup(groupId);
        }
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
}
