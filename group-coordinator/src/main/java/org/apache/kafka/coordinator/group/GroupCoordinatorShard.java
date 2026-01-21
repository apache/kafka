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
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.internals.Plugin;
import org.apache.kafka.common.message.AlterShareGroupOffsetsRequestData;
import org.apache.kafka.common.message.AlterShareGroupOffsetsResponseData;
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.DeleteGroupsResponseData;
import org.apache.kafka.common.message.DeleteShareGroupOffsetsRequestData;
import org.apache.kafka.common.message.DeleteShareGroupOffsetsResponseData;
import org.apache.kafka.common.message.DeleteShareGroupStateRequestData;
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
import org.apache.kafka.common.message.StreamsGroupDescribeResponseData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.common.runtime.CoordinatorExecutor;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataDelta;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
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
import org.apache.kafka.coordinator.group.generated.CoordinatorRecordType;
import org.apache.kafka.coordinator.group.generated.GroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.GroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.LegacyOffsetCommitKey;
import org.apache.kafka.coordinator.group.generated.LegacyOffsetCommitValue;
import org.apache.kafka.coordinator.group.generated.OffsetCommitKey;
import org.apache.kafka.coordinator.group.generated.OffsetCommitValue;
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
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetricsShard;
import org.apache.kafka.coordinator.group.modern.share.ShareGroup;
import org.apache.kafka.coordinator.group.streams.StreamsGroupHeartbeatResult;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.share.persister.DeleteShareGroupStateParameters;
import org.apache.kafka.server.share.persister.InitializeShareGroupStateParameters;
import org.apache.kafka.timeline.SnapshotRegistry;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
        private Optional<Plugin<Authorizer>> authorizerPlugin;

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

        public CoordinatorShardBuilder<GroupCoordinatorShard, CoordinatorRecord> withAuthorizerPlugin(
            Optional<Plugin<Authorizer>> authorizerPlugin
        ) {
            this.authorizerPlugin = authorizerPlugin;
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
            if (authorizerPlugin == null)
                throw new IllegalArgumentException("Authorizer must be set.");

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
                .withShareGroupAssignor(config.shareGroupAssignors().get(0))
                .withAuthorizerPlugin(authorizerPlugin)
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

    public static class DeleteShareGroupOffsetsResultHolder {
        private final short topLevelErrorCode;
        private final String topLevelErrorMessage;
        private final List<DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic> errorTopicResponseList;
        private final DeleteShareGroupStateParameters deleteStateRequestParameters;

        DeleteShareGroupOffsetsResultHolder(short topLevelErrorCode, String topLevelErrorMessage) {
            this(topLevelErrorCode, topLevelErrorMessage, null,  null);
        }

        DeleteShareGroupOffsetsResultHolder(
            short topLevelErrorCode,
            String topLevelErrorMessage,
            List<DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic> errorTopicResponseList
        ) {
            this(topLevelErrorCode, topLevelErrorMessage, errorTopicResponseList, null);
        }

        DeleteShareGroupOffsetsResultHolder(
            short topLevelErrorCode,
            String topLevelErrorMessage,
            List<DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic> errorTopicResponseList,
            DeleteShareGroupStateParameters deleteStateRequestParameters
        ) {
            this.topLevelErrorCode = topLevelErrorCode;
            this.topLevelErrorMessage = topLevelErrorMessage;
            this.errorTopicResponseList = errorTopicResponseList;
            this.deleteStateRequestParameters = deleteStateRequestParameters;
        }

        public short topLevelErrorCode() {
            return this.topLevelErrorCode;
        }

        public String topLevelErrorMessage() {
            return this.topLevelErrorMessage;
        }

        public List<DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic> errorTopicResponseList() {
            return this.errorTopicResponseList;
        }

        public DeleteShareGroupStateParameters deleteStateRequestParameters() {
            return this.deleteStateRequestParameters;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DeleteShareGroupOffsetsResultHolder other = (DeleteShareGroupOffsetsResultHolder) o;
            return topLevelErrorCode == other.topLevelErrorCode &&
                Objects.equals(topLevelErrorMessage, other.topLevelErrorMessage) &&
                Objects.equals(errorTopicResponseList, other.errorTopicResponseList) &&
                Objects.equals(deleteStateRequestParameters, other.deleteStateRequestParameters);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topLevelErrorCode, topLevelErrorMessage, errorTopicResponseList, deleteStateRequestParameters);
        }
    }

    /**
     * Represents a deleted topic with its ID and name.
     * Used during offset cleanup to ensure only offsets for the correct
     * topic incarnation are deleted (preventing race conditions with topic recreation).
     *
     * @param id    The topic ID.
     * @param name  The topic name.
     */
    public record DeletedTopic(Uuid id, String name) { }

    /**
     * The group/offsets expiration key to schedule a timer task.
     *
     * Visible for testing.
     */
    static final String GROUP_EXPIRATION_KEY = "expire-group-metadata";

    /**
     * The classic, consumer and streams group size counter key to schedule a timer task.
     *
     * Visible for testing.
     */
    static final String GROUP_SIZE_COUNTER_KEY = "group-size-counter";

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
        AuthorizableRequestContext context,
        ConsumerGroupHeartbeatRequestData request
    ) {
        return groupMetadataManager.consumerGroupHeartbeat(context, request);
    }

    /**
     * Handles a StreamsGroupHeartbeat request.
     *
     * @param context The request context.
     * @param request The actual StreamsGroupHeartbeat request.
     *
     * @return A Result containing the StreamsGroupHeartbeat response, a list of internal topics to be created and
     *         a list of records to update the state machine.
     */
    public CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> streamsGroupHeartbeat(
        AuthorizableRequestContext context,
        StreamsGroupHeartbeatRequestData request
    ) {
        return groupMetadataManager.streamsGroupHeartbeat(context, request);
    }

    /**
     * Handles a ShareGroupHeartbeat request.
     *
     * @param context The request context.
     * @param request The actual ShareGroupHeartbeat request.
     *
     * @return A Result containing a pair of ShareGroupHeartbeat response maybe InitializeShareGroupStateParameters
     *         and a list of records to update the state machine.
     */
    public CoordinatorResult<Map.Entry<ShareGroupHeartbeatResponseData, Optional<InitializeShareGroupStateParameters>>, CoordinatorRecord> shareGroupHeartbeat(
        AuthorizableRequestContext context,
        ShareGroupHeartbeatRequestData request
    ) {
        return groupMetadataManager.shareGroupHeartbeat(context, request);
    }

    /**
     * Handles record creation, if needed, related to ShareGroupStatePartitionMetadata
     * corresponding to a share group heartbeat request.
     *
     * @param groupId The group id corresponding to the share group whose share partitions have been initialized.
     * @param topicPartitionMap Map representing topic partition data to be added to the share state partition metadata.
     *
     * @return A Result containing coordinator records and Void response.
     */
    public CoordinatorResult<Void, CoordinatorRecord> initializeShareGroupState(
        String groupId,
        Map<Uuid, Set<Integer>> topicPartitionMap
    ) {
        return groupMetadataManager.initializeShareGroupState(groupId, topicPartitionMap);
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
        return groupMetadataManager.uninitializeShareGroupState(groupId, topicPartitionMap);
    }

    /**
     * Returns the set of share-partitions whose share-group state has been initialized in the persister.
     *
     * @param groupId The group id corresponding to the share group whose share partitions have been initialized.
     *
     * @return A map representing the initialized share-partitions for the share group.
     */
    public Map<Uuid, Set<Integer>> initializedShareGroupPartitions(
        String groupId
    ) {
        return groupMetadataManager.initializedShareGroupPartitions(groupId);
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
        AuthorizableRequestContext context,
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
        AuthorizableRequestContext context,
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
        AuthorizableRequestContext context,
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
        AuthorizableRequestContext context,
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
                groupMetadataManager.createGroupTombstoneRecordsAndCancelTimers(groupId, records);
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
     * Method returns a Map keyed on groupId and value as pair of {@link DeleteShareGroupStateParameters}
     * and any ERRORS while building the request corresponding to the valid share groups passed as the input.
     * <p>
     * The groupIds are first filtered by type to restrict the list to share groups. If a group isn't
     * found or isn't a share group, it won't trigger an error in the response since group deletions
     * are chained. Instead, that group should be retried against other group types.
     * @param groupIds - A list of groupIds as string
     * @return A result object containing a map keyed on groupId and value pair (req, error) and related coordinator records.
     */
    public CoordinatorResult<Map<String, Map.Entry<DeleteShareGroupStateParameters, Errors>>, CoordinatorRecord> sharePartitionDeleteRequests(
        List<String> groupIds
    ) {
        Map<String, Map.Entry<DeleteShareGroupStateParameters, Errors>> responseMap = new HashMap<>();
        List<CoordinatorRecord> records = new ArrayList<>();
        for (String groupId : groupIds) {
            try {
                ShareGroup group = groupMetadataManager.shareGroup(groupId);
                group.validateDeleteGroup();
                groupMetadataManager.shareGroupBuildPartitionDeleteRequest(groupId, records)
                    .ifPresent(req -> responseMap.put(groupId, Map.entry(req, Errors.NONE)));
            } catch (GroupIdNotFoundException exception) {
                log.debug("Unable to delete share group. GroupId {} not found.", groupId);
                // Do not include the error in response map, as the deletion of groups is chained hence
                // the respective group should be re-tried for deletion against other group types.
            } catch (GroupNotEmptyException exception) {
                log.debug("Unable to delete share group. Provided group {} is not empty.", groupId);
                responseMap.put(groupId, Map.entry(DeleteShareGroupStateParameters.EMPTY_PARAMS, Errors.forException(exception)));
            }
        }
        return new CoordinatorResult<>(records, responseMap);
    }

    /**
     * Ensure the following checks are true to make sure that a DeleteShareGroupOffsets request is valid and can be processed further
     * 1. Checks whether the provided group is empty
     * 2. Checks the requested topics are presented in the metadataImage
     * 3. Checks the requested share partitions are initialized for the group
     * Once these checks are passed, an appropriate ShareGroupStatePartitionMetadataRecord is created by adding the topics to
     * deleting topics list and removing them from the initialized topics list.
     *
     * @param groupId - The group ID
     * @param requestData - The request data for DeleteShareGroupOffsetsRequest
     * @return {@link DeleteShareGroupOffsetsResultHolder} an object containing top level error code, list of topic responses
     *                                               and persister deleteState request parameters
     */
    public CoordinatorResult<DeleteShareGroupOffsetsResultHolder, CoordinatorRecord> initiateDeleteShareGroupOffsets(
        String groupId,
        DeleteShareGroupOffsetsRequestData requestData
    ) {
        List<CoordinatorRecord> records = new ArrayList<>();
        try {
            ShareGroup group = groupMetadataManager.shareGroup(groupId);
            group.validateDeleteGroup();

            List<DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic> errorTopicResponseList = new ArrayList<>();
            List<DeleteShareGroupStateRequestData.DeleteStateData> deleteShareGroupStateRequestTopicsData =
                groupMetadataManager.sharePartitionsEligibleForOffsetDeletion(
                    groupId,
                    requestData,
                    errorTopicResponseList,
                    records
                );

            if (deleteShareGroupStateRequestTopicsData.isEmpty()) {
                return new CoordinatorResult<>(
                    records,
                    new DeleteShareGroupOffsetsResultHolder(Errors.NONE.code(), null, errorTopicResponseList)
                );
            }

            DeleteShareGroupStateRequestData deleteShareGroupStateRequestData = new DeleteShareGroupStateRequestData()
                .setGroupId(requestData.groupId())
                .setTopics(deleteShareGroupStateRequestTopicsData);

            return new CoordinatorResult<>(
                records,
                new DeleteShareGroupOffsetsResultHolder(
                    Errors.NONE.code(),
                    null,
                    errorTopicResponseList,
                    DeleteShareGroupStateParameters.from(deleteShareGroupStateRequestData)
                )
            );

        } catch (GroupIdNotFoundException exception) {
            log.debug("Unable to delete share group offsets. GroupId {} not found.", groupId);
            return new CoordinatorResult<>(
                records,
                new DeleteShareGroupOffsetsResultHolder(Errors.GROUP_ID_NOT_FOUND.code(), exception.getMessage())
            );
        } catch (GroupNotEmptyException exception) {
            log.debug("Unable to delete share group offsets. Provided group {} is not empty.", groupId);
            return new CoordinatorResult<>(
                records,
                new DeleteShareGroupOffsetsResultHolder(Errors.NON_EMPTY_GROUP.code(), exception.getMessage())
            );
        }
    }

    /**
     * Completes the share group offset deletion by creating a ShareGroupStatePartitionMetadataRecord removing the
     * deleted topics from deletingTopics set. Returns the final response for DeleteShareGroupOffsetsRequest
     *
     * @param groupId - The group ID
     * @param topics - The set of topics which were deleted successfully by the persister
     * @return the final response {@link DeleteShareGroupOffsetsResponseData} for the DeleteShareGroupOffsetsRequest
     */
    public CoordinatorResult<DeleteShareGroupOffsetsResponseData, CoordinatorRecord> completeDeleteShareGroupOffsets(
        String groupId,
        Map<Uuid, String> topics,
        List<DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic> errorTopicResponseList
    ) {
        List<CoordinatorRecord> records = new ArrayList<>();
        List<DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic> topicResponseList = new ArrayList<>();

        topicResponseList.addAll(
            groupMetadataManager.completeDeleteShareGroupOffsets(
                groupId,
                topics,
                records
            )
        );

        topicResponseList.addAll(errorTopicResponseList);

        return new CoordinatorResult<>(
            records,
            new DeleteShareGroupOffsetsResponseData()
                .setErrorCode(Errors.NONE.code())
                .setErrorMessage(null)
                .setResponses(topicResponseList)
        );
    }

    /**
     * Alters the offsets for a share group.
     *
     * @param groupId - The group ID
     * @param alterShareGroupOffsetsRequestData - The request data for AlterShareGroupOffsetsRequestData
     * @return A Result containing a pair of AlterShareGroupOffsets InitializeShareGroupStateParameters
     *         and a list of records to update the state machine.
     */
    public CoordinatorResult<Map.Entry<AlterShareGroupOffsetsResponseData, InitializeShareGroupStateParameters>, CoordinatorRecord> alterShareGroupOffsets(
        String groupId,
        AlterShareGroupOffsetsRequestData alterShareGroupOffsetsRequestData
    ) {
        return groupMetadataManager.alterShareGroupOffsets(groupId, alterShareGroupOffsetsRequestData.topics());
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
        if (OffsetFetchRequest.requestAllOffsets(request)) {
            return offsetMetadataManager.fetchAllOffsets(request, epoch);
        } else {
            return offsetMetadataManager.fetchOffsets(request, epoch);
        }
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
        AuthorizableRequestContext context,
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
        AuthorizableRequestContext context,
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
     * Handles a StreamsGroupDescribe request.
     *
     * @param groupIds      The IDs of the groups to describe.
     *
     * @return A list containing the StreamsGroupDescribeResponseData.DescribedGroup.
     *
     */
    public List<StreamsGroupDescribeResponseData.DescribedGroup> streamsGroupDescribe(
        List<String> groupIds,
        long committedOffset
    ) {
        return groupMetadataManager.streamsGroupDescribe(groupIds, committedOffset);
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
        AuthorizableRequestContext context,
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
        AuthorizableRequestContext context,
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
        AuthorizableRequestContext context,
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
            Group group = groupMetadataManager.group(groupId);
            if (group.shouldExpire()) {
                boolean allOffsetsExpired = offsetMetadataManager.cleanupExpiredOffsets(groupId, records);
                if (allOffsetsExpired) {
                    groupMetadataManager.maybeDeleteGroup(groupId, records);
                }
            }
        });

        if (!records.isEmpty()) {
            log.info("Generated {} tombstone records while cleaning up group metadata in {} milliseconds.",
                records.size(), time.milliseconds() - startMs);
        }

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
     * Remove offsets of the topics that have been deleted.
     *
     * @param deletedTopics   The topics that have been deleted.
     * @return The list of tombstones (offset commit) to append.
     */
    public CoordinatorResult<Void, CoordinatorRecord> onTopicsDeleted(
        List<DeletedTopic> deletedTopics
    ) {
        final long startTimeMs = time.milliseconds();
        final List<CoordinatorRecord> records = offsetMetadataManager.onTopicsDeleted(deletedTopics);

        log.info("Generated {} tombstone records in {} milliseconds while deleting offsets for topics {}.",
            records.size(), time.milliseconds() - startTimeMs, deletedTopics);

        return new CoordinatorResult<>(records, false);
    }

    public CoordinatorResult<Void, CoordinatorRecord> maybeCleanupShareGroupState(
        Set<Uuid> deletedTopicIds
    ) {
        final long startTimeMs = time.milliseconds();
        final var result = groupMetadataManager.maybeCleanupShareGroupState(deletedTopicIds);

        log.info("Generated {} records in {} milliseconds while cleaning share group state for topics {}.",
            result.records().size(), time.milliseconds() - startTimeMs, deletedTopicIds);

        return result;
    }

    /**
     * Schedules (or reschedules) the group size counter for the classic/consumer groups.
     */
    private void scheduleGroupSizeCounter() {
        timer.schedule(
            GROUP_SIZE_COUNTER_KEY,
            DEFAULT_GROUP_GAUGES_UPDATE_INTERVAL_MS,
            TimeUnit.MILLISECONDS,
            true,
            () -> {
                groupMetadataManager.updateGroupSizeCounter();
                scheduleGroupSizeCounter();
                return GroupMetadataManager.EMPTY_RESULT;
            }
        );
    }

    /**
     * Cancels the group size counter for the classic/consumer groups.
     */
    private void cancelGroupSizeCounter() {
        timer.cancel(GROUP_SIZE_COUNTER_KEY);
    }

    /**
     * The coordinator has been loaded. This is used to apply any
     * post loading operations (e.g. registering timers).
     *
     * @param newImage  The metadata image.
     */
    @Override
    public void onLoaded(CoordinatorMetadataImage newImage) {
        CoordinatorMetadataDelta emptyDelta = newImage.emptyDelta();
        groupMetadataManager.onMetadataUpdate(emptyDelta, newImage);
        coordinatorMetrics.activateMetricsShard(metricsShard);

        groupMetadataManager.onLoaded();
        scheduleGroupMetadataExpiration();
        scheduleGroupSizeCounter();
    }

    @Override
    public void onUnloaded() {
        timer.cancel(GROUP_EXPIRATION_KEY);
        coordinatorMetrics.deactivateMetricsShard(metricsShard);
        groupMetadataManager.onUnloaded();
        cancelGroupSizeCounter();
    }

    /**
     * A new metadata image is available.
     *
     * @param delta    The delta image.
     * @param newImage The new metadata image.
     */
    @Override
    public void onMetadataUpdate(CoordinatorMetadataDelta delta, CoordinatorMetadataImage newImage) {
        groupMetadataManager.onMetadataUpdate(delta, newImage);
    }

    private static OffsetCommitKey convertLegacyOffsetCommitKey(
        LegacyOffsetCommitKey key
    ) {
        return new OffsetCommitKey()
            .setGroup(key.group())
            .setTopic(key.topic())
            .setPartition(key.partition());
    }

    private static OffsetCommitValue convertLegacyOffsetCommitValue(
        LegacyOffsetCommitValue value
    ) {
        if (value == null) return null;

        return new OffsetCommitValue()
            .setOffset(value.offset())
            .setCommitTimestamp(value.commitTimestamp())
            .setMetadata(value.metadata());
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
        ApiMessage key = record.key();
        ApiMessageAndVersion value = record.value();

        CoordinatorRecordType recordType;
        try {
            recordType = CoordinatorRecordType.fromId(key.apiKey());
        } catch (UnsupportedVersionException ex) {
            throw new IllegalStateException("Received an unknown record type " + key.apiKey()
                + " in " + record, ex);
        }

        switch (recordType) {
            case LEGACY_OFFSET_COMMIT:
                offsetMetadataManager.replay(
                    offset,
                    producerId,
                    convertLegacyOffsetCommitKey((LegacyOffsetCommitKey) key),
                    convertLegacyOffsetCommitValue((LegacyOffsetCommitValue) Utils.messageOrNull(value))
                );
                break;

            case OFFSET_COMMIT:
                offsetMetadataManager.replay(
                    offset,
                    producerId,
                    (OffsetCommitKey) key,
                    (OffsetCommitValue) Utils.messageOrNull(value)
                );
                break;

            case GROUP_METADATA:
                groupMetadataManager.replay(
                    (GroupMetadataKey) key,
                    (GroupMetadataValue) Utils.messageOrNull(value)
                );
                break;

            case CONSUMER_GROUP_METADATA:
                groupMetadataManager.replay(
                    (ConsumerGroupMetadataKey) key,
                    (ConsumerGroupMetadataValue) Utils.messageOrNull(value)
                );
                break;

            case CONSUMER_GROUP_PARTITION_METADATA:
                groupMetadataManager.replay(
                    (ConsumerGroupPartitionMetadataKey) key,
                    (ConsumerGroupPartitionMetadataValue) Utils.messageOrNull(value)
                );
                break;

            case CONSUMER_GROUP_MEMBER_METADATA:
                groupMetadataManager.replay(
                    (ConsumerGroupMemberMetadataKey) key,
                    (ConsumerGroupMemberMetadataValue) Utils.messageOrNull(value)
                );
                break;

            case CONSUMER_GROUP_TARGET_ASSIGNMENT_METADATA:
                groupMetadataManager.replay(
                    (ConsumerGroupTargetAssignmentMetadataKey) key,
                    (ConsumerGroupTargetAssignmentMetadataValue) Utils.messageOrNull(value)
                );
                break;

            case CONSUMER_GROUP_TARGET_ASSIGNMENT_MEMBER:
                groupMetadataManager.replay(
                    (ConsumerGroupTargetAssignmentMemberKey) key,
                    (ConsumerGroupTargetAssignmentMemberValue) Utils.messageOrNull(value)
                );
                break;

            case CONSUMER_GROUP_CURRENT_MEMBER_ASSIGNMENT:
                groupMetadataManager.replay(
                    (ConsumerGroupCurrentMemberAssignmentKey) key,
                    (ConsumerGroupCurrentMemberAssignmentValue) Utils.messageOrNull(value)
                );
                break;

            case SHARE_GROUP_MEMBER_METADATA:
                groupMetadataManager.replay(
                    (ShareGroupMemberMetadataKey) key,
                    (ShareGroupMemberMetadataValue) Utils.messageOrNull(value)
                );
                break;

            case SHARE_GROUP_METADATA:
                groupMetadataManager.replay(
                    (ShareGroupMetadataKey) key,
                    (ShareGroupMetadataValue) Utils.messageOrNull(value)
                );
                break;

            case SHARE_GROUP_TARGET_ASSIGNMENT_METADATA:
                groupMetadataManager.replay(
                    (ShareGroupTargetAssignmentMetadataKey) key,
                    (ShareGroupTargetAssignmentMetadataValue) Utils.messageOrNull(value)
                );
                break;

            case SHARE_GROUP_TARGET_ASSIGNMENT_MEMBER:
                groupMetadataManager.replay(
                    (ShareGroupTargetAssignmentMemberKey) key,
                    (ShareGroupTargetAssignmentMemberValue) Utils.messageOrNull(value)
                );
                break;

            case SHARE_GROUP_CURRENT_MEMBER_ASSIGNMENT:
                groupMetadataManager.replay(
                    (ShareGroupCurrentMemberAssignmentKey) key,
                    (ShareGroupCurrentMemberAssignmentValue) Utils.messageOrNull(value)
                );
                break;

            case SHARE_GROUP_STATE_PARTITION_METADATA:
                groupMetadataManager.replay(
                    (ShareGroupStatePartitionMetadataKey) key,
                    (ShareGroupStatePartitionMetadataValue) Utils.messageOrNull(value)
                );
                break;

            case CONSUMER_GROUP_REGULAR_EXPRESSION:
                groupMetadataManager.replay(
                    (ConsumerGroupRegularExpressionKey) key,
                    (ConsumerGroupRegularExpressionValue) Utils.messageOrNull(value)
                );
                break;

            case STREAMS_GROUP_METADATA:
                groupMetadataManager.replay(
                    (StreamsGroupMetadataKey) key,
                    (StreamsGroupMetadataValue) Utils.messageOrNull(value)
                );
                break;

            case STREAMS_GROUP_MEMBER_METADATA:
                groupMetadataManager.replay(
                    (StreamsGroupMemberMetadataKey) key,
                    (StreamsGroupMemberMetadataValue) Utils.messageOrNull(value)
                );
                break;

            case STREAMS_GROUP_TARGET_ASSIGNMENT_METADATA:
                groupMetadataManager.replay(
                    (StreamsGroupTargetAssignmentMetadataKey) key,
                    (StreamsGroupTargetAssignmentMetadataValue) Utils.messageOrNull(value)
                );
                break;

            case STREAMS_GROUP_TARGET_ASSIGNMENT_MEMBER:
                groupMetadataManager.replay(
                    (StreamsGroupTargetAssignmentMemberKey) key,
                    (StreamsGroupTargetAssignmentMemberValue) Utils.messageOrNull(value)
                );
                break;

            case STREAMS_GROUP_CURRENT_MEMBER_ASSIGNMENT:
                groupMetadataManager.replay(
                    (StreamsGroupCurrentMemberAssignmentKey) key,
                    (StreamsGroupCurrentMemberAssignmentValue) Utils.messageOrNull(value)
                );
                break;

            case STREAMS_GROUP_TOPOLOGY:
                groupMetadataManager.replay(
                    (StreamsGroupTopologyKey) key,
                    (StreamsGroupTopologyValue) Utils.messageOrNull(value)
                );
                break;

            default:
                throw new IllegalStateException("Received an unknown record type " + recordType
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
