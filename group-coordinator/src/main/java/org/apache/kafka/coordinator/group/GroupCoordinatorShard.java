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
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.DeleteGroupsResponseData;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetDeleteRequestData;
import org.apache.kafka.common.message.OffsetDeleteResponseData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.ShareGroupDescribeResponseData;
import org.apache.kafka.common.message.ShareGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ShareGroupHeartbeatResponseData;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.common.runtime.CoordinatorExecutor;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetrics;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetricsShard;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorResult;
import org.apache.kafka.coordinator.common.runtime.CoordinatorShard;
import org.apache.kafka.coordinator.common.runtime.CoordinatorShardBuilder;
import org.apache.kafka.coordinator.common.runtime.CoordinatorTimer;
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
import org.apache.kafka.coordinator.group.generated.OffsetCommitKey;
import org.apache.kafka.coordinator.group.generated.OffsetCommitValue;
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
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetricsShard;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.timeline.SnapshotRegistry;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * The group coordinator shard is a replicated state machine that manages the metadata of all
 * classic and consumer groups. It holds the hard and the soft state of the groups. This class
 * has two kinds of methods:
 * 1) The request handlers which handle the requests and generate a response and records to
 *    mutate the hard state. Those records will be written by the runtime and applied to the
 *    hard state via the replay methods.
 * 2) The replay methods which apply records to the hard state. Those are used in the request
 *    handling as well as during the initial loading of the records from the partitions.
 */
@SuppressWarnings("ClassFanOutComplexity")
public class GroupCoordinatorShard implements CoordinatorShard<CoordinatorRecord> {

    public static class Builder implements CoordinatorShardBuilder<GroupCoordinatorShard, CoordinatorRecord> {
        private final GroupCoordinatorConfig config;
        private final GroupConfigManager groupConfigManager;
        private LogContext logContext;
        private SnapshotRegistry snapshotRegistry;
        private Time time;
        private CoordinatorTimer<Void, CoordinatorRecord> timer;
        private CoordinatorExecutor<CoordinatorRecord> executor;
        private CoordinatorMetrics coordinatorMetrics;
        private TopicPartition topicPartition;

        public Builder(
            GroupCoordinatorConfig config,
            GroupConfigManager groupConfigManager
        ) {
            this.config = config;
            this.groupConfigManager = groupConfigManager;
        }

        @Override
        public CoordinatorShardBuilder<GroupCoordinatorShard, CoordinatorRecord> withLogContext(
            LogContext logContext
        ) {
            this.logContext = logContext;
            return this;
        }

        @Override
        public CoordinatorShardBuilder<GroupCoordinatorShard, CoordinatorRecord> withTime(
            Time time
        ) {
            this.time = time;
            return this;
        }

        @Override
        public CoordinatorShardBuilder<GroupCoordinatorShard, CoordinatorRecord> withTimer(
            CoordinatorTimer<Void, CoordinatorRecord> timer
        ) {
            this.timer = timer;
            return this;
        }

        @Override
        public CoordinatorShardBuilder<GroupCoordinatorShard, CoordinatorRecord> withExecutor(
            CoordinatorExecutor<CoordinatorRecord> executor
        ) {
            this.executor = executor;
            return this;
        }

        @Override
        public CoordinatorShardBuilder<GroupCoordinatorShard, CoordinatorRecord> withCoordinatorMetrics(
            CoordinatorMetrics coordinatorMetrics
        ) {
            this.coordinatorMetrics = coordinatorMetrics;
            return this;
        }

        @Override
        public CoordinatorShardBuilder<GroupCoordinatorShard, CoordinatorRecord> withTopicPartition(TopicPartition topicPartition) {
            this.topicPartition = topicPartition;
            return this;
        }

        @Override
        public CoordinatorShardBuilder<GroupCoordinatorShard, CoordinatorRecord> withSnapshotRegistry(
            SnapshotRegistry snapshotRegistry
        ) {
            this.snapshotRegistry = snapshotRegistry;
            return this;
        }

        @SuppressWarnings("NPathComplexity")
        @Override
        public GroupCoordinatorShard build() {
            if (logContext == null) logContext = new LogContext();
            if (config == null)
                throw new IllegalArgumentException("Config must be set.");
            if (snapshotRegistry == null)
                throw new IllegalArgumentException("SnapshotRegistry must be set.");
            if (time == null)
                throw new IllegalArgumentException("Time must be set.");
            if (timer == null)
                throw new IllegalArgumentException("Timer must be set.");
            if (executor == null)
                throw new IllegalArgumentException("Executor must be set.");
            if (coordinatorMetrics == null || !(coordinatorMetrics instanceof GroupCoordinatorMetrics))
                throw new IllegalArgumentException("CoordinatorMetrics must be set and be of type GroupCoordinatorMetrics.");
            if (topicPartition == null)
                throw new IllegalArgumentException("TopicPartition must be set.");
            if (groupConfigManager == null)
                throw new IllegalArgumentException("GroupConfigManager must be set.");

            GroupCoordinatorMetricsShard metricsShard = ((GroupCoordinatorMetrics) coordinatorMetrics)
                .newMetricsShard(snapshotRegistry, topicPartition);

            GroupMetadataManager groupMetadataManager = new GroupMetadataManager.Builder()
                .withLogContext(logContext)
                .withSnapshotRegistry(snapshotRegistry)
                .withTime(time)
                .withTimer(timer)
                .withExecutor(executor)
                .withConfig(config)
                .withGroupConfigManager(groupConfigManager)
                .withGroupCoordinatorMetricsShard(metricsShard)
                .build();

            OffsetMetadataManager offsetMetadataManager = new OffsetMetadataManager.Builder()
                .withLogContext(logContext)
                .withSnapshotRegistry(snapshotRegistry)
                .withTime(time)
                .withGroupMetadataManager(groupMetadataManager)
                .withGroupCoordinatorConfig(config)
                .withGroupCoordinatorMetricsShard(metricsShard)
                .build();

            return new GroupCoordinatorShard(
                logContext,
                groupMetadataManager,
                offsetMetadataManager,
                time,
                timer,
                config,
                coordinatorMetrics,
                metricsShard
            );
        }
    }

    /**
     * The group/offsets expiration key to schedule a timer task.
     *
     * Visible for testing.
     */
    static final String GROUP_EXPIRATION_KEY = "expire-group-metadata";

    /**
     * The classic group size counter key to schedule a timer task.
     *
     * Visible for testing.
     */
    static final String CLASSIC_GROUP_SIZE_COUNTER_KEY = "classic-group-size-counter";

    /**
     * Hardcoded default value of the interval to update the classic group size counter.
     */
    static final int DEFAULT_GROUP_GAUGES_UPDATE_INTERVAL_MS = 60 * 1000;

    /**
     * The logger.
     */
    private final Logger log;

    /**
     * The group metadata manager.
     */
    private final GroupMetadataManager groupMetadataManager;

    /**
     * The offset metadata manager.
     */
    private final OffsetMetadataManager offsetMetadataManager;

    /**
     * The time.
     */
    private final Time time;

    /**
     * The coordinator timer.
     */
    private final CoordinatorTimer<Void, CoordinatorRecord> timer;

    /**
     * The group coordinator config.
     */
    private final GroupCoordinatorConfig config;

    /**
     * The coordinator metrics.
     */
    private final CoordinatorMetrics coordinatorMetrics;

    /**
     * The coordinator metrics shard.
     */
    private final CoordinatorMetricsShard metricsShard;

    /**
     * Constructor.
     *
     * @param logContext            The log context.
     * @param groupMetadataManager  The group metadata manager.
     * @param offsetMetadataManager The offset metadata manager.
     * @param coordinatorMetrics    The coordinator metrics.
     * @param metricsShard          The coordinator metrics shard.
     */
    GroupCoordinatorShard(
        LogContext logContext,
        GroupMetadataManager groupMetadataManager,
        OffsetMetadataManager offsetMetadataManager,
        Time time,
        CoordinatorTimer<Void, CoordinatorRecord> timer,
        GroupCoordinatorConfig config,
        CoordinatorMetrics coordinatorMetrics,
        CoordinatorMetricsShard metricsShard
    ) {
        this.log = logContext.logger(GroupCoordinatorShard.class);
        this.groupMetadataManager = groupMetadataManager;
        this.offsetMetadataManager = offsetMetadataManager;
        this.time = time;
        this.timer = timer;
        this.config = config;
        this.coordinatorMetrics = coordinatorMetrics;
        this.metricsShard = metricsShard;
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
    ) {
        return groupMetadataManager.consumerGroupHeartbeat(context, request);
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
    ) {
        return groupMetadataManager.shareGroupHeartbeat(context, request);
    }

    /**
     * Handles a JoinGroup request.
     *
     * @param context The request context.
     * @param request The actual JoinGroup request.
     *
     * @return A Result containing the JoinGroup response and
     *         a list of records to update the state machine.
     */
    public CoordinatorResult<Void, CoordinatorRecord> classicGroupJoin(
        RequestContext context,
        JoinGroupRequestData request,
        CompletableFuture<JoinGroupResponseData> responseFuture
    ) {
        return groupMetadataManager.classicGroupJoin(
            context,
            request,
            responseFuture
        );
    }

    /**
     * Handles a SyncGroup request.
     *
     * @param context The request context.
     * @param request The actual SyncGroup request.
     *
     * @return A Result containing the SyncGroup response and
     *         a list of records to update the state machine.
     */
    public CoordinatorResult<Void, CoordinatorRecord> classicGroupSync(
        RequestContext context,
        SyncGroupRequestData request,
        CompletableFuture<SyncGroupResponseData> responseFuture
    ) {
        return groupMetadataManager.classicGroupSync(
            context,
            request,
            responseFuture
        );
    }

    /**
     * Handles a classic group HeartbeatRequest.
     *
     * @param context The request context.
     * @param request The actual Heartbeat request.
     *
     * @return A Result containing the heartbeat response and
     *         a list of records to update the state machine.
     */
    public CoordinatorResult<HeartbeatResponseData, CoordinatorRecord> classicGroupHeartbeat(
        RequestContext context,
        HeartbeatRequestData request
    ) {
        return groupMetadataManager.classicGroupHeartbeat(
            context,
            request
        );
    }

    /**
     * Handles a DeleteGroups request.
     *
     * @param context   The request context.
     * @param groupIds  The groupIds of the groups to be deleted
     * @return A Result containing the DeleteGroupsResponseData.DeletableGroupResultCollection response and
     *         a list of records to update the state machine.
     */
    public CoordinatorResult<DeleteGroupsResponseData.DeletableGroupResultCollection, CoordinatorRecord> deleteGroups(
        RequestContext context,
        List<String> groupIds
    ) throws ApiException {
        final DeleteGroupsResponseData.DeletableGroupResultCollection resultCollection =
            new DeleteGroupsResponseData.DeletableGroupResultCollection(groupIds.size());
        final List<CoordinatorRecord> records = new ArrayList<>();
        int numDeletedOffsets = 0;
        final List<String> deletedGroups = new ArrayList<>();

        for (String groupId : groupIds) {
            try {
                groupMetadataManager.validateDeleteGroup(groupId);
                numDeletedOffsets += offsetMetadataManager.deleteAllOffsets(groupId, records);
                groupMetadataManager.createGroupTombstoneRecords(groupId, records);
                deletedGroups.add(groupId);

                resultCollection.add(
                    new DeleteGroupsResponseData.DeletableGroupResult()
                        .setGroupId(groupId)
                );
            } catch (ApiException exception) {
                resultCollection.add(
                    new DeleteGroupsResponseData.DeletableGroupResult()
                        .setGroupId(groupId)
                        .setErrorCode(Errors.forException(exception).code())
                );
            }
        }

        log.info("The following groups were deleted: {}. A total of {} offsets were removed.",
            String.join(", ", deletedGroups),
            numDeletedOffsets
        );
        return new CoordinatorResult<>(records, resultCollection);
    }

    /**
     * Fetch offsets for a given set of partitions and a given group.
     *
     * @param request   The OffsetFetchRequestGroup request.
     * @param epoch     The epoch (or offset) used to read from the
     *                  timeline data structure.
     *
     * @return A List of OffsetFetchResponseTopics response.
     */
    public OffsetFetchResponseData.OffsetFetchResponseGroup fetchOffsets(
        OffsetFetchRequestData.OffsetFetchRequestGroup request,
        long epoch
    ) throws ApiException {
        return offsetMetadataManager.fetchOffsets(request, epoch);
    }

    /**
     * Fetch all offsets for a given group.
     *
     * @param request   The OffsetFetchRequestGroup request.
     * @param epoch     The epoch (or offset) used to read from the
     *                  timeline data structure.
     *
     * @return A List of OffsetFetchResponseTopics response.
     */
    public OffsetFetchResponseData.OffsetFetchResponseGroup fetchAllOffsets(
        OffsetFetchRequestData.OffsetFetchRequestGroup request,
        long epoch
    ) throws ApiException {
        return offsetMetadataManager.fetchAllOffsets(request, epoch);
    }

    /**
     * Handles an OffsetCommit request.
     *
     * @param context The request context.
     * @param request The actual OffsetCommit request.
     *
     * @return A Result containing the OffsetCommitResponse response and
     *         a list of records to update the state machine.
     */
    public CoordinatorResult<OffsetCommitResponseData, CoordinatorRecord> commitOffset(
        RequestContext context,
        OffsetCommitRequestData request
    ) throws ApiException {
        return offsetMetadataManager.commitOffset(context, request);
    }

    /**
     * Handles an TxnOffsetCommit request.
     *
     * @param context The request context.
     * @param request The actual TxnOffsetCommit request.
     *
     * @return A Result containing the TxnOffsetCommitResponse response and
     *         a list of records to update the state machine.
     */
    public CoordinatorResult<TxnOffsetCommitResponseData, CoordinatorRecord> commitTransactionalOffset(
        RequestContext context,
        TxnOffsetCommitRequestData request
    ) throws ApiException {
        return offsetMetadataManager.commitTransactionalOffset(context, request);
    }

    /**
     * Handles a ListGroups request.
     *
     * @param statesFilter      The states of the groups we want to list.
     *                          If empty, all groups are returned with their state.
     *                          If invalid, no groups are returned.
     * @param typesFilter       The types of the groups we want to list.
     *                          If empty, all groups are returned with their type.
     *                          If invalid, no groups are returned.
     * @param committedOffset   A specified committed offset corresponding to this shard.
     * @return A list containing the ListGroupsResponseData.ListedGroup
     */
    public List<ListGroupsResponseData.ListedGroup> listGroups(
        List<String> statesFilter,
        List<String> typesFilter,
        long committedOffset
    ) throws ApiException {

        Set<String> statesFilterSet = new HashSet<>(statesFilter);
        Set<String> typesFilterSet = new HashSet<>(typesFilter);

        return groupMetadataManager.listGroups(statesFilterSet, typesFilterSet, committedOffset);
    }

    /**
     * Handles a ConsumerGroupDescribe request.
     *
     * @param groupIds      The IDs of the groups to describe.
     *
     * @return A list containing the ConsumerGroupDescribeResponseData.DescribedGroup.
     *
     */
    public List<ConsumerGroupDescribeResponseData.DescribedGroup> consumerGroupDescribe(
        List<String> groupIds,
        long committedOffset
    ) {
        return groupMetadataManager.consumerGroupDescribe(groupIds, committedOffset);
    }

    /**
     * Handles a ShareGroupDescribe request.
     *
     * @param groupIds      The IDs of the groups to describe.
     *
     * @return A list containing the ShareGroupDescribeResponseData.DescribedGroup.
     *
     */
    public List<ShareGroupDescribeResponseData.DescribedGroup> shareGroupDescribe(
        List<String> groupIds,
        long committedOffset
    ) {
        return groupMetadataManager.shareGroupDescribe(groupIds, committedOffset);
    }

    /**
     * Handles a DescribeGroups request.
     *
     * @param context           The request context.
     * @param groupIds          The IDs of the groups to describe.
     * @param committedOffset   A specified committed offset corresponding to this shard.
     *
     * @return A list containing the DescribeGroupsResponseData.DescribedGroup.
     */
    public List<DescribeGroupsResponseData.DescribedGroup> describeGroups(
        RequestContext context,
        List<String> groupIds,
        long committedOffset
    ) {
        return groupMetadataManager.describeGroups(context, groupIds, committedOffset);
    }

    /**
     * Handles a LeaveGroup request.
     *
     * @param context The request context.
     * @param request The actual LeaveGroup request.
     *
     * @return A Result containing the LeaveGroup response and
     *         a list of records to update the state machine.
     */
    public CoordinatorResult<LeaveGroupResponseData, CoordinatorRecord> classicGroupLeave(
        RequestContext context,
        LeaveGroupRequestData request
    ) throws ApiException {
        return groupMetadataManager.classicGroupLeave(context, request);
    }

    /**
     * Handles a OffsetDelete request.
     *
     * @param context The request context.
     * @param request The actual OffsetDelete request.
     *
     * @return A Result containing the OffsetDeleteResponse response and
     *         a list of records to update the state machine.
     */
    public CoordinatorResult<OffsetDeleteResponseData, CoordinatorRecord> deleteOffsets(
        RequestContext context,
        OffsetDeleteRequestData request
    ) throws ApiException {
        return offsetMetadataManager.deleteOffsets(request);
    }

    /**
     * For each group, remove all expired offsets. If all offsets for the group are removed and the group is eligible
     * for deletion, delete the group.
     *
     * @return The list of tombstones (offset commit and group metadata) to append.
     */
    public CoordinatorResult<Void, CoordinatorRecord> cleanupGroupMetadata() {
        long startMs = time.milliseconds();
        List<CoordinatorRecord> records = new ArrayList<>();
        groupMetadataManager.groupIds().forEach(groupId -> {
            boolean allOffsetsExpired = offsetMetadataManager.cleanupExpiredOffsets(groupId, records);
            if (allOffsetsExpired) {
                groupMetadataManager.maybeDeleteGroup(groupId, records);
            }
        });

        log.info("Generated {} tombstone records while cleaning up group metadata in {} milliseconds.",
            records.size(), time.milliseconds() - startMs);
        // Reschedule the next cycle.
        scheduleGroupMetadataExpiration();
        return new CoordinatorResult<>(records, false);
    }

    /**
     * Schedule the group/offsets expiration job. If any exceptions are thrown above, the timer will retry.
     */
    private void scheduleGroupMetadataExpiration() {
        timer.schedule(
            GROUP_EXPIRATION_KEY,
            config.offsetsRetentionCheckIntervalMs(),
            TimeUnit.MILLISECONDS,
            true,
            config.offsetsRetentionCheckIntervalMs(),
            this::cleanupGroupMetadata
        );
    }

    /**
     * Remove offsets of the partitions that have been deleted.
     *
     * @param topicPartitions   The partitions that have been deleted.
     * @return The list of tombstones (offset commit) to append.
     */
    public CoordinatorResult<Void, CoordinatorRecord> onPartitionsDeleted(
        List<TopicPartition> topicPartitions
    ) {
        final long startTimeMs = time.milliseconds();
        final List<CoordinatorRecord> records = offsetMetadataManager.onPartitionsDeleted(topicPartitions);

        log.info("Generated {} tombstone records in {} milliseconds while deleting offsets for partitions {}.",
            records.size(), time.milliseconds() - startTimeMs, topicPartitions);

        return new CoordinatorResult<>(records, false);
    }

    /**
     * Schedules (or reschedules) the group size counter for the classic groups.
     */
    private void scheduleClassicGroupSizeCounter() {
        timer.schedule(
            CLASSIC_GROUP_SIZE_COUNTER_KEY,
            DEFAULT_GROUP_GAUGES_UPDATE_INTERVAL_MS,
            TimeUnit.MILLISECONDS,
            true,
            () -> {
                groupMetadataManager.updateClassicGroupSizeCounter();
                scheduleClassicGroupSizeCounter();
                return GroupMetadataManager.EMPTY_RESULT;
            }
        );
    }

    /**
     * Cancels the group size counter for the classic groups.
     */
    private void cancelClassicGroupSizeCounter() {
        timer.cancel(CLASSIC_GROUP_SIZE_COUNTER_KEY);
    }

    /**
     * The coordinator has been loaded. This is used to apply any
     * post loading operations (e.g. registering timers).
     *
     * @param newImage  The metadata image.
     */
    @Override
    public void onLoaded(MetadataImage newImage) {
        MetadataDelta emptyDelta = new MetadataDelta(newImage);
        groupMetadataManager.onNewMetadataImage(newImage, emptyDelta);
        offsetMetadataManager.onNewMetadataImage(newImage, emptyDelta);
        coordinatorMetrics.activateMetricsShard(metricsShard);

        groupMetadataManager.onLoaded();
        scheduleGroupMetadataExpiration();
        scheduleClassicGroupSizeCounter();
    }

    @Override
    public void onUnloaded() {
        timer.cancel(GROUP_EXPIRATION_KEY);
        coordinatorMetrics.deactivateMetricsShard(metricsShard);
        groupMetadataManager.onUnloaded();
        cancelClassicGroupSizeCounter();
    }

    /**
     * A new metadata image is available.
     *
     * @param newImage  The new metadata image.
     * @param delta     The delta image.
     */
    @Override
    public void onNewMetadataImage(MetadataImage newImage, MetadataDelta delta) {
        groupMetadataManager.onNewMetadataImage(newImage, delta);
        offsetMetadataManager.onNewMetadataImage(newImage, delta);
    }

    /**
     * Replays the Record to update the hard state of the group coordinator.
     *
     * @param offset        The offset of the record in the log.
     * @param producerId    The producer id.
     * @param producerEpoch The producer epoch.
     * @param record        The record to apply to the state machine.
     * @throws RuntimeException
     */
    @SuppressWarnings({"CyclomaticComplexity"})
    @Override
    public void replay(
        long offset,
        long producerId,
        short producerEpoch,
        CoordinatorRecord record
    ) throws RuntimeException {
        ApiMessageAndVersion key = record.key();
        ApiMessageAndVersion value = record.value();

        switch (key.version()) {
            case 0:
            case 1:
                offsetMetadataManager.replay(
                    offset,
                    producerId,
                    (OffsetCommitKey) key.message(),
                    (OffsetCommitValue) Utils.messageOrNull(value)
                );
                break;

            case 2:
                groupMetadataManager.replay(
                    (GroupMetadataKey) key.message(),
                    (GroupMetadataValue) Utils.messageOrNull(value)
                );
                break;

            case 3:
                groupMetadataManager.replay(
                    (ConsumerGroupMetadataKey) key.message(),
                    (ConsumerGroupMetadataValue) Utils.messageOrNull(value)
                );
                break;

            case 4:
                groupMetadataManager.replay(
                    (ConsumerGroupPartitionMetadataKey) key.message(),
                    (ConsumerGroupPartitionMetadataValue) Utils.messageOrNull(value)
                );
                break;

            case 5:
                groupMetadataManager.replay(
                    (ConsumerGroupMemberMetadataKey) key.message(),
                    (ConsumerGroupMemberMetadataValue) Utils.messageOrNull(value)
                );
                break;

            case 6:
                groupMetadataManager.replay(
                    (ConsumerGroupTargetAssignmentMetadataKey) key.message(),
                    (ConsumerGroupTargetAssignmentMetadataValue) Utils.messageOrNull(value)
                );
                break;

            case 7:
                groupMetadataManager.replay(
                    (ConsumerGroupTargetAssignmentMemberKey) key.message(),
                    (ConsumerGroupTargetAssignmentMemberValue) Utils.messageOrNull(value)
                );
                break;

            case 8:
                groupMetadataManager.replay(
                    (ConsumerGroupCurrentMemberAssignmentKey) key.message(),
                    (ConsumerGroupCurrentMemberAssignmentValue) Utils.messageOrNull(value)
                );
                break;

            case 9:
                groupMetadataManager.replay(
                    (ShareGroupPartitionMetadataKey) key.message(),
                    (ShareGroupPartitionMetadataValue) Utils.messageOrNull(value)
                );
                break;

            case 10:
                groupMetadataManager.replay(
                    (ShareGroupMemberMetadataKey) key.message(),
                    (ShareGroupMemberMetadataValue) Utils.messageOrNull(value)
                );
                break;

            case 11:
                groupMetadataManager.replay(
                    (ShareGroupMetadataKey) key.message(),
                    (ShareGroupMetadataValue) Utils.messageOrNull(value)
                );
                break;

            case 12:
                groupMetadataManager.replay(
                    (ShareGroupTargetAssignmentMetadataKey) key.message(),
                    (ShareGroupTargetAssignmentMetadataValue) Utils.messageOrNull(value)
                );
                break;

            case 13:
                groupMetadataManager.replay(
                    (ShareGroupTargetAssignmentMemberKey) key.message(),
                    (ShareGroupTargetAssignmentMemberValue) Utils.messageOrNull(value)
                );
                break;

            case 14:
                groupMetadataManager.replay(
                    (ShareGroupCurrentMemberAssignmentKey) key.message(),
                    (ShareGroupCurrentMemberAssignmentValue) Utils.messageOrNull(value)
                );
                break;

            case 16:
                groupMetadataManager.replay(
                    (ConsumerGroupRegularExpressionKey) key.message(),
                    (ConsumerGroupRegularExpressionValue) Utils.messageOrNull(value)
                );
                break;

            default:
                throw new IllegalStateException("Received an unknown record type " + key.version()
                    + " in " + record);
        }
    }

    /**
     * Applies the given transaction marker.
     *
     * @param producerId    The producer id.
     * @param producerEpoch The producer epoch.
     * @param result        The result of the transaction.
     * @throws RuntimeException if the transaction can not be completed.
     */
    @Override
    public void replayEndTransactionMarker(
        long producerId,
        short producerEpoch,
        TransactionResult result
    ) throws RuntimeException {
        offsetMetadataManager.replayEndTransactionMarker(producerId, result);
    }
}
