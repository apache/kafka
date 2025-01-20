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
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.NotCoordinatorException;
import org.apache.kafka.common.internals.Topic;
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
import org.apache.kafka.common.message.ListGroupsRequestData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetDeleteRequestData;
import org.apache.kafka.common.message.OffsetDeleteResponseData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.ShareGroupDescribeResponseData;
import org.apache.kafka.common.message.ShareGroupDescribeResponseData.DescribedGroup;
import org.apache.kafka.common.message.ShareGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ShareGroupHeartbeatResponseData;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.ConsumerGroupDescribeRequest;
import org.apache.kafka.common.requests.DeleteGroupsRequest;
import org.apache.kafka.common.requests.DescribeGroupsRequest;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.ShareGroupDescribeRequest;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.requests.TxnOffsetCommitRequest;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.common.runtime.CoordinatorEventProcessor;
import org.apache.kafka.coordinator.common.runtime.CoordinatorLoader;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorResult;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRuntime;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRuntimeMetrics;
import org.apache.kafka.coordinator.common.runtime.CoordinatorShardBuilderSupplier;
import org.apache.kafka.coordinator.common.runtime.MultiThreadedEventProcessor;
import org.apache.kafka.coordinator.common.runtime.PartitionWriter;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.server.record.BrokerCompressionType;
import org.apache.kafka.server.util.FutureUtils;
import org.apache.kafka.server.util.timer.Timer;

import org.slf4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;

import static org.apache.kafka.coordinator.common.runtime.CoordinatorOperationExceptionHelper.handleOperationException;

/**
 * The group coordinator service.
 */
public class GroupCoordinatorService implements GroupCoordinator {

    public static class Builder {
        private final int nodeId;
        private final GroupCoordinatorConfig config;
        private PartitionWriter writer;
        private CoordinatorLoader<CoordinatorRecord> loader;
        private Time time;
        private Timer timer;
        private CoordinatorRuntimeMetrics coordinatorRuntimeMetrics;
        private GroupCoordinatorMetrics groupCoordinatorMetrics;
        private GroupConfigManager groupConfigManager;

        public Builder(
            int nodeId,
            GroupCoordinatorConfig config
        ) {
            this.nodeId = nodeId;
            this.config = config;
        }

        public Builder withWriter(PartitionWriter writer) {
            this.writer = writer;
            return this;
        }

        public Builder withLoader(CoordinatorLoader<CoordinatorRecord> loader) {
            this.loader = loader;
            return this;
        }

        public Builder withTime(Time time) {
            this.time = time;
            return this;
        }

        public Builder withTimer(Timer timer) {
            this.timer = timer;
            return this;
        }

        public Builder withCoordinatorRuntimeMetrics(CoordinatorRuntimeMetrics coordinatorRuntimeMetrics) {
            this.coordinatorRuntimeMetrics = coordinatorRuntimeMetrics;
            return this;
        }

        public Builder withGroupCoordinatorMetrics(GroupCoordinatorMetrics groupCoordinatorMetrics) {
            this.groupCoordinatorMetrics = groupCoordinatorMetrics;
            return this;
        }

        public Builder withGroupConfigManager(GroupConfigManager groupConfigManager) {
            this.groupConfigManager = groupConfigManager;
            return this;
        }

        public GroupCoordinatorService build() {
            if (config == null)
                throw new IllegalArgumentException("Config must be set.");
            if (writer == null)
                throw new IllegalArgumentException("Writer must be set.");
            if (loader == null)
                throw new IllegalArgumentException("Loader must be set.");
            if (time == null)
                throw new IllegalArgumentException("Time must be set.");
            if (timer == null)
                throw new IllegalArgumentException("Timer must be set.");
            if (coordinatorRuntimeMetrics == null)
                throw new IllegalArgumentException("CoordinatorRuntimeMetrics must be set.");
            if (groupCoordinatorMetrics == null)
                throw new IllegalArgumentException("GroupCoordinatorMetrics must be set.");
            if (groupConfigManager == null)
                throw new IllegalArgumentException("GroupConfigManager must be set.");

            String logPrefix = String.format("GroupCoordinator id=%d", nodeId);
            LogContext logContext = new LogContext(String.format("[%s] ", logPrefix));

            CoordinatorShardBuilderSupplier<GroupCoordinatorShard, CoordinatorRecord> supplier = () ->
                new GroupCoordinatorShard.Builder(config, groupConfigManager);

            CoordinatorEventProcessor processor = new MultiThreadedEventProcessor(
                logContext,
                "group-coordinator-event-processor-",
                config.numThreads(),
                time,
                coordinatorRuntimeMetrics
            );

            CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime =
                new CoordinatorRuntime.Builder<GroupCoordinatorShard, CoordinatorRecord>()
                    .withTime(time)
                    .withTimer(timer)
                    .withLogPrefix(logPrefix)
                    .withLogContext(logContext)
                    .withEventProcessor(processor)
                    .withPartitionWriter(writer)
                    .withLoader(loader)
                    .withCoordinatorShardBuilderSupplier(supplier)
                    .withDefaultWriteTimeOut(Duration.ofMillis(config.offsetCommitTimeoutMs()))
                    .withCoordinatorRuntimeMetrics(coordinatorRuntimeMetrics)
                    .withCoordinatorMetrics(groupCoordinatorMetrics)
                    .withSerializer(new GroupCoordinatorRecordSerde())
                    .withCompression(Compression.of(config.offsetTopicCompressionType()).build())
                    .withAppendLingerMs(config.appendLingerMs())
                    .withExecutorService(Executors.newSingleThreadExecutor())
                    .build();

            return new GroupCoordinatorService(
                logContext,
                config,
                runtime,
                groupCoordinatorMetrics,
                groupConfigManager
            );
        }
    }

    /**
     * The logger.
     */
    private final Logger log;

    /**
     * The group coordinator configurations.
     */
    private final GroupCoordinatorConfig config;

    /**
     * The coordinator runtime.
     */
    private final CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime;

    /**
     * The metrics registry.
     */
    private final GroupCoordinatorMetrics groupCoordinatorMetrics;

    /**
     * The group config manager.
     */
    private final GroupConfigManager groupConfigManager;

    /**
     * Boolean indicating whether the coordinator is active or not.
     */
    private final AtomicBoolean isActive = new AtomicBoolean(false);

    /**
     * The number of partitions of the __consumer_offsets topics. This is provided
     * when the component is started.
     */
    private volatile int numPartitions = -1;

    /**
     *
     * @param logContext                The log context.
     * @param config                    The group coordinator config.
     * @param runtime                   The runtime.
     * @param groupCoordinatorMetrics   The group coordinator metrics.
     * @param groupConfigManager        The group config manager.
     */
    GroupCoordinatorService(
        LogContext logContext,
        GroupCoordinatorConfig config,
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime,
        GroupCoordinatorMetrics groupCoordinatorMetrics,
        GroupConfigManager groupConfigManager
    ) {
        this.log = logContext.logger(GroupCoordinatorService.class);
        this.config = config;
        this.runtime = runtime;
        this.groupCoordinatorMetrics = groupCoordinatorMetrics;
        this.groupConfigManager = groupConfigManager;
    }

    /**
     * Throws CoordinatorNotAvailableException if the not active.
     */
    private void throwIfNotActive() {
        if (!isActive.get()) {
            throw Errors.COORDINATOR_NOT_AVAILABLE.exception();
        }
    }

    /**
     * @return The topic partition for the given group.
     */
    private TopicPartition topicPartitionFor(
        String groupId
    ) {
        return new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, partitionFor(groupId));
    }

    /**
     * See {@link GroupCoordinator#isNewGroupCoordinator()}
     */
    @Override
    public boolean isNewGroupCoordinator() {
        return true;
    }

    /**
     * See {@link GroupCoordinator#partitionFor(String)}
     */
    @Override
    public int partitionFor(
        String groupId
    ) {
        throwIfNotActive();
        return Utils.abs(groupId.hashCode()) % numPartitions;
    }

    /**
     * See {@link GroupCoordinator#consumerGroupHeartbeat(RequestContext, ConsumerGroupHeartbeatRequestData)}.
     */
    @Override
    public CompletableFuture<ConsumerGroupHeartbeatResponseData> consumerGroupHeartbeat(
        RequestContext context,
        ConsumerGroupHeartbeatRequestData request
    ) {
        if (!isActive.get()) {
            return CompletableFuture.completedFuture(new ConsumerGroupHeartbeatResponseData()
                .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
            );
        }

        return runtime.scheduleWriteOperation(
            "consumer-group-heartbeat",
            topicPartitionFor(request.groupId()),
            Duration.ofMillis(config.offsetCommitTimeoutMs()),
            coordinator -> coordinator.consumerGroupHeartbeat(context, request)
        ).exceptionally(exception -> handleOperationException(
            "consumer-group-heartbeat",
            request,
            exception,
            (error, message) -> new ConsumerGroupHeartbeatResponseData()
                .setErrorCode(error.code())
                .setErrorMessage(message),
            log
        ));
    }

    /**
     * See {@link GroupCoordinator#shareGroupHeartbeat(RequestContext, ShareGroupHeartbeatRequestData)}.
     */
    @Override
    public CompletableFuture<ShareGroupHeartbeatResponseData> shareGroupHeartbeat(
        RequestContext context,
        ShareGroupHeartbeatRequestData request
    ) {
        if (!isActive.get()) {
            return CompletableFuture.completedFuture(new ShareGroupHeartbeatResponseData()
                .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
            );
        }

        return runtime.scheduleWriteOperation(
            "share-group-heartbeat",
            topicPartitionFor(request.groupId()),
            Duration.ofMillis(config.offsetCommitTimeoutMs()),
            coordinator -> coordinator.shareGroupHeartbeat(context, request)
        ).exceptionally(exception -> handleOperationException(
            "share-group-heartbeat",
            request,
            exception,
            (error, message) -> new ShareGroupHeartbeatResponseData()
                .setErrorCode(error.code())
                .setErrorMessage(message),
            log
        ));
    }

    /**
     * See {@link GroupCoordinator#joinGroup(RequestContext, JoinGroupRequestData, BufferSupplier)}.
     */
    @Override
    public CompletableFuture<JoinGroupResponseData> joinGroup(
        RequestContext context,
        JoinGroupRequestData request,
        BufferSupplier bufferSupplier
    ) {
        if (!isActive.get()) {
            return CompletableFuture.completedFuture(new JoinGroupResponseData()
                .setMemberId(request.memberId())
                .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
            );
        }

        if (!isGroupIdNotEmpty(request.groupId())) {
            return CompletableFuture.completedFuture(new JoinGroupResponseData()
                .setMemberId(request.memberId())
                .setErrorCode(Errors.INVALID_GROUP_ID.code())
            );
        }

        if (request.sessionTimeoutMs() < config.classicGroupMinSessionTimeoutMs() ||
            request.sessionTimeoutMs() > config.classicGroupMaxSessionTimeoutMs()) {
            return CompletableFuture.completedFuture(new JoinGroupResponseData()
                .setMemberId(request.memberId())
                .setErrorCode(Errors.INVALID_SESSION_TIMEOUT.code())
            );
        }

        CompletableFuture<JoinGroupResponseData> responseFuture = new CompletableFuture<>();

        runtime.scheduleWriteOperation(
            "classic-group-join",
            topicPartitionFor(request.groupId()),
            Duration.ofMillis(config.offsetCommitTimeoutMs()),
            coordinator -> coordinator.classicGroupJoin(context, request, responseFuture)
        ).exceptionally(exception -> {
            if (!responseFuture.isDone()) {
                responseFuture.complete(handleOperationException(
                    "classic-group-join",
                    request,
                    exception,
                    (error, __) -> new JoinGroupResponseData().setErrorCode(error.code()),
                    log
                ));
            }
            return null;
        });

        return responseFuture;
    }

    /**
     * See {@link GroupCoordinator#syncGroup(RequestContext, SyncGroupRequestData, BufferSupplier)}.
     */
    @Override
    public CompletableFuture<SyncGroupResponseData> syncGroup(
        RequestContext context,
        SyncGroupRequestData request,
        BufferSupplier bufferSupplier
    ) {
        if (!isActive.get()) {
            return CompletableFuture.completedFuture(new SyncGroupResponseData()
                .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
            );
        }

        if (!isGroupIdNotEmpty(request.groupId())) {
            return CompletableFuture.completedFuture(new SyncGroupResponseData()
                .setErrorCode(Errors.INVALID_GROUP_ID.code())
            );
        }

        CompletableFuture<SyncGroupResponseData> responseFuture = new CompletableFuture<>();

        runtime.scheduleWriteOperation(
            "classic-group-sync",
            topicPartitionFor(request.groupId()),
            Duration.ofMillis(config.offsetCommitTimeoutMs()),
            coordinator -> coordinator.classicGroupSync(context, request, responseFuture)
        ).exceptionally(exception -> {
            if (!responseFuture.isDone()) {
                responseFuture.complete(handleOperationException(
                    "classic-group-sync",
                    request,
                    exception,
                    (error, __) -> new SyncGroupResponseData().setErrorCode(error.code()),
                    log
                ));
            }
            return null;
        });

        return responseFuture;
    }

    /**
     * See {@link GroupCoordinator#heartbeat(RequestContext, HeartbeatRequestData)}.
     */
    @Override
    public CompletableFuture<HeartbeatResponseData> heartbeat(
        RequestContext context,
        HeartbeatRequestData request
    ) {
        if (!isActive.get()) {
            return CompletableFuture.completedFuture(new HeartbeatResponseData()
                .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
            );
        }

        if (!isGroupIdNotEmpty(request.groupId())) {
            return CompletableFuture.completedFuture(new HeartbeatResponseData()
                .setErrorCode(Errors.INVALID_GROUP_ID.code())
            );
        }

        return runtime.scheduleWriteOperation(
            "classic-group-heartbeat",
            topicPartitionFor(request.groupId()),
            Duration.ofMillis(config.offsetCommitTimeoutMs()),
            coordinator -> coordinator.classicGroupHeartbeat(context, request)
        ).exceptionally(exception -> handleOperationException(
            "classic-group-heartbeat",
            request,
            exception,
            (error, __) -> {
                if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS) {
                    // The group is still loading, so blindly respond
                    return new HeartbeatResponseData()
                        .setErrorCode(Errors.NONE.code());
                } else {
                    return new HeartbeatResponseData()
                        .setErrorCode(error.code());
                }
            },
            log
        ));
    }

    /**
     * See {@link GroupCoordinator#leaveGroup(RequestContext, LeaveGroupRequestData)}.
     */
    @Override
    public CompletableFuture<LeaveGroupResponseData> leaveGroup(
        RequestContext context,
        LeaveGroupRequestData request
    ) {
        if (!isActive.get()) {
            return CompletableFuture.completedFuture(new LeaveGroupResponseData()
                .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
            );
        }

        if (!isGroupIdNotEmpty(request.groupId())) {
            return CompletableFuture.completedFuture(new LeaveGroupResponseData()
                .setErrorCode(Errors.INVALID_GROUP_ID.code())
            );
        }

        return runtime.scheduleWriteOperation(
            "classic-group-leave",
            topicPartitionFor(request.groupId()),
            Duration.ofMillis(config.offsetCommitTimeoutMs()),
            coordinator -> coordinator.classicGroupLeave(context, request)
        ).exceptionally(exception -> handleOperationException(
            "classic-group-leave",
            request,
            exception,
            (error, __) -> {
                if (error == Errors.UNKNOWN_MEMBER_ID) {
                    // Group was not found.
                    List<LeaveGroupResponseData.MemberResponse> memberResponses = request.members().stream()
                         .map(member -> new LeaveGroupResponseData.MemberResponse()
                             .setMemberId(member.memberId())
                             .setGroupInstanceId(member.groupInstanceId())
                             .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code()))
                         .collect(Collectors.toList());
                    return new LeaveGroupResponseData()
                        .setMembers(memberResponses);
                } else {
                    return new LeaveGroupResponseData()
                        .setErrorCode(error.code());
                }
            },
            log
        ));
    }

    /**
     * See {@link GroupCoordinator#listGroups(RequestContext, ListGroupsRequestData)}.
     */
    @Override
    public CompletableFuture<ListGroupsResponseData> listGroups(
        RequestContext context,
        ListGroupsRequestData request
    ) {
        if (!isActive.get()) {
            return CompletableFuture.completedFuture(new ListGroupsResponseData()
                .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
            );
        }

        final List<CompletableFuture<List<ListGroupsResponseData.ListedGroup>>> futures = FutureUtils.mapExceptionally(
            runtime.scheduleReadAllOperation(
                "list-groups",
                (coordinator, lastCommittedOffset) -> coordinator.listGroups(
                    request.statesFilter(),
                    request.typesFilter(),
                    lastCommittedOffset
                )
            ),
            exception -> {
                exception = Errors.maybeUnwrapException(exception);
                if (exception instanceof NotCoordinatorException) {
                    return Collections.emptyList();
                } else {
                    throw new CompletionException(exception);
                }
            }
        );

        return FutureUtils
            .combineFutures(futures, ArrayList::new, List::addAll)
            .thenApply(groups -> new ListGroupsResponseData().setGroups(groups))
            .exceptionally(exception -> handleOperationException(
                "list-groups",
                request,
                exception,
                (error, __) -> new ListGroupsResponseData().setErrorCode(error.code()),
                log
            ));
    }

    /**
     * See {@link GroupCoordinator#consumerGroupDescribe(RequestContext, List)}.
     */
    @Override
    public CompletableFuture<List<ConsumerGroupDescribeResponseData.DescribedGroup>> consumerGroupDescribe(
        RequestContext context,
        List<String> groupIds
    ) {
        if (!isActive.get()) {
            return CompletableFuture.completedFuture(ConsumerGroupDescribeRequest.getErrorDescribedGroupList(
                groupIds,
                Errors.COORDINATOR_NOT_AVAILABLE
            ));
        }

        final List<CompletableFuture<List<ConsumerGroupDescribeResponseData.DescribedGroup>>> futures =
            new ArrayList<>(groupIds.size());
        final Map<TopicPartition, List<String>> groupsByTopicPartition = new HashMap<>();
        groupIds.forEach(groupId -> {
            if (isGroupIdNotEmpty(groupId)) {
                groupsByTopicPartition
                    .computeIfAbsent(topicPartitionFor(groupId), __ -> new ArrayList<>())
                    .add(groupId);
            } else {
                futures.add(CompletableFuture.completedFuture(Collections.singletonList(
                    new ConsumerGroupDescribeResponseData.DescribedGroup()
                        .setGroupId(null)
                        .setErrorCode(Errors.INVALID_GROUP_ID.code())
                )));
            }
        });

        groupsByTopicPartition.forEach((topicPartition, groupList) -> {
            CompletableFuture<List<ConsumerGroupDescribeResponseData.DescribedGroup>> future =
                runtime.scheduleReadOperation(
                    "consumer-group-describe",
                    topicPartition,
                    (coordinator, lastCommittedOffset) -> coordinator.consumerGroupDescribe(groupIds, lastCommittedOffset)
                ).exceptionally(exception -> handleOperationException(
                    "consumer-group-describe",
                    groupList,
                    exception,
                    (error, __) -> ConsumerGroupDescribeRequest.getErrorDescribedGroupList(groupList, error),
                    log
                ));

            futures.add(future);
        });

        return FutureUtils.combineFutures(futures, ArrayList::new, List::addAll);
    }

    /**
     * See {@link GroupCoordinator#shareGroupDescribe(RequestContext, List)}.
     */
    @Override
    public CompletableFuture<List<DescribedGroup>> shareGroupDescribe(
        RequestContext context,
        List<String> groupIds) {
        if (!isActive.get()) {
            return CompletableFuture.completedFuture(ShareGroupDescribeRequest.getErrorDescribedGroupList(
                groupIds,
                Errors.COORDINATOR_NOT_AVAILABLE
            ));
        }

        final List<CompletableFuture<List<ShareGroupDescribeResponseData.DescribedGroup>>> futures =
            new ArrayList<>(groupIds.size());
        final Map<TopicPartition, List<String>> groupsByTopicPartition = new HashMap<>();
        groupIds.forEach(groupId -> {
            if (isGroupIdNotEmpty(groupId)) {
                groupsByTopicPartition
                    .computeIfAbsent(topicPartitionFor(groupId), __ -> new ArrayList<>())
                    .add(groupId);
            } else {
                futures.add(CompletableFuture.completedFuture(Collections.singletonList(
                    new ShareGroupDescribeResponseData.DescribedGroup()
                        .setGroupId(null)
                        .setErrorCode(Errors.INVALID_GROUP_ID.code())
                )));
            }
        });

        groupsByTopicPartition.forEach((topicPartition, groupList) -> {
            CompletableFuture<List<ShareGroupDescribeResponseData.DescribedGroup>> future =
                runtime.scheduleReadOperation(
                    "share-group-describe",
                    topicPartition,
                    (coordinator, lastCommittedOffset) -> coordinator.shareGroupDescribe(groupIds, lastCommittedOffset)
                ).exceptionally(exception -> handleOperationException(
                    "share-group-describe",
                    groupList,
                    exception,
                    (error, __) -> ShareGroupDescribeRequest.getErrorDescribedGroupList(groupList, error),
                    log
                ));

            futures.add(future);
        });

        return FutureUtils.combineFutures(futures, ArrayList::new, List::addAll);
    }

    /**
     * See {@link GroupCoordinator#describeGroups(RequestContext, List)}.
     */
    @Override
    public CompletableFuture<List<DescribeGroupsResponseData.DescribedGroup>> describeGroups(
        RequestContext context,
        List<String> groupIds
    ) {
        if (!isActive.get()) {
            return CompletableFuture.completedFuture(DescribeGroupsRequest.getErrorDescribedGroupList(
                groupIds,
                Errors.COORDINATOR_NOT_AVAILABLE
            ));
        }

        final List<CompletableFuture<List<DescribeGroupsResponseData.DescribedGroup>>> futures =
            new ArrayList<>(groupIds.size());
        final Map<TopicPartition, List<String>> groupsByTopicPartition = new HashMap<>();
        groupIds.forEach(groupId -> {
            // For backwards compatibility, we support DescribeGroups for the empty group id.
            if (groupId == null) {
                futures.add(CompletableFuture.completedFuture(Collections.singletonList(
                    new DescribeGroupsResponseData.DescribedGroup()
                        .setGroupId(null)
                        .setErrorCode(Errors.INVALID_GROUP_ID.code())
                )));
            } else {
                final TopicPartition topicPartition = topicPartitionFor(groupId);
                groupsByTopicPartition
                    .computeIfAbsent(topicPartition, __ -> new ArrayList<>())
                    .add(groupId);
            }
        });

        groupsByTopicPartition.forEach((topicPartition, groupList) -> {
            CompletableFuture<List<DescribeGroupsResponseData.DescribedGroup>> future =
                runtime.scheduleReadOperation(
                    "describe-groups",
                    topicPartition,
                    (coordinator, lastCommittedOffset) -> coordinator.describeGroups(context, groupList, lastCommittedOffset)
                ).exceptionally(exception -> handleOperationException(
                    "describe-groups",
                    groupList,
                    exception,
                    (error, __) -> DescribeGroupsRequest.getErrorDescribedGroupList(groupList, error),
                    log
                ));

            futures.add(future);
        });

        return FutureUtils.combineFutures(futures, ArrayList::new, List::addAll);
    }

    /**
     * See {@link GroupCoordinator#deleteGroups(RequestContext, List, BufferSupplier)}.
     */
    @Override
    public CompletableFuture<DeleteGroupsResponseData.DeletableGroupResultCollection> deleteGroups(
        RequestContext context,
        List<String> groupIds,
        BufferSupplier bufferSupplier
    ) {
        if (!isActive.get()) {
            return CompletableFuture.completedFuture(DeleteGroupsRequest.getErrorResultCollection(
                groupIds,
                Errors.COORDINATOR_NOT_AVAILABLE
            ));
        }

        final List<CompletableFuture<DeleteGroupsResponseData.DeletableGroupResultCollection>> futures =
            new ArrayList<>(groupIds.size());

        final Map<TopicPartition, List<String>> groupsByTopicPartition = new HashMap<>();
        groupIds.forEach(groupId -> {
            // For backwards compatibility, we support DeleteGroups for the empty group id.
            if (groupId == null) {
                futures.add(CompletableFuture.completedFuture(DeleteGroupsRequest.getErrorResultCollection(
                    Collections.singletonList(null),
                    Errors.INVALID_GROUP_ID
                )));
            } else {
                final TopicPartition topicPartition = topicPartitionFor(groupId);
                groupsByTopicPartition
                    .computeIfAbsent(topicPartition, __ -> new ArrayList<>())
                    .add(groupId);
            }
        });

        groupsByTopicPartition.forEach((topicPartition, groupList) -> {
            CompletableFuture<DeleteGroupsResponseData.DeletableGroupResultCollection> future =
                runtime.scheduleWriteOperation(
                    "delete-groups",
                    topicPartition,
                    Duration.ofMillis(config.offsetCommitTimeoutMs()),
                    coordinator -> coordinator.deleteGroups(context, groupList)
                ).exceptionally(exception -> handleOperationException(
                    "delete-groups",
                    groupList,
                    exception,
                    (error, __) -> DeleteGroupsRequest.getErrorResultCollection(groupList, error),
                    log
                ));

            futures.add(future);
        });

        return FutureUtils.combineFutures(futures, DeleteGroupsResponseData.DeletableGroupResultCollection::new,
            // We don't use res.addAll(future.join()) because DeletableGroupResultCollection is an ImplicitLinkedHashMultiCollection,
            // which has requirements for adding elements (see ImplicitLinkedHashCollection.java#add).
            (accumulator, newResults) -> newResults.forEach(result -> accumulator.add(result.duplicate())));
    }

    /**
     * See {@link GroupCoordinator#fetchOffsets(RequestContext, OffsetFetchRequestData.OffsetFetchRequestGroup, boolean)}.
     */
    @Override
    public CompletableFuture<OffsetFetchResponseData.OffsetFetchResponseGroup> fetchOffsets(
        RequestContext context,
        OffsetFetchRequestData.OffsetFetchRequestGroup request,
        boolean requireStable
    ) {
        if (!isActive.get()) {
            return CompletableFuture.completedFuture(new OffsetFetchResponseData.OffsetFetchResponseGroup()
                .setGroupId(request.groupId())
                .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
            );
        }

        // For backwards compatibility, we support fetch commits for the empty group id.
        if (request.groupId() == null) {
            return CompletableFuture.completedFuture(new OffsetFetchResponseData.OffsetFetchResponseGroup()
                .setGroupId(request.groupId())
                .setErrorCode(Errors.INVALID_GROUP_ID.code())
            );
        }

        // The require stable flag when set tells the broker to hold on returning unstable
        // (or uncommitted) offsets. In the previous implementation of the group coordinator,
        // the UNSTABLE_OFFSET_COMMIT error is returned when unstable offsets are present. As
        // the new implementation relies on timeline data structures, the coordinator does not
        // really know whether offsets are stable or not so it is hard to return the same error.
        // Instead, we use a write operation when the flag is set to guarantee that the fetch
        // is based on all the available offsets and to ensure that the response waits until
        // the pending offsets are committed. Otherwise, we use a read operation.
        if (requireStable) {
            return runtime.scheduleWriteOperation(
                "fetch-offsets",
                topicPartitionFor(request.groupId()),
                Duration.ofMillis(config.offsetCommitTimeoutMs()),
                coordinator -> new CoordinatorResult<>(
                    Collections.emptyList(),
                    coordinator.fetchOffsets(request, Long.MAX_VALUE)
                )
            ).exceptionally(exception -> handleOffsetFetchException(
                "fetch-offsets",
                request,
                exception
            ));
        } else {
            return runtime.scheduleReadOperation(
                "fetch-offsets",
                topicPartitionFor(request.groupId()),
                (coordinator, offset) -> coordinator.fetchOffsets(request, offset)
            );
        }
    }

    /**
     * See {@link GroupCoordinator#fetchAllOffsets(RequestContext, OffsetFetchRequestData.OffsetFetchRequestGroup, boolean)}.
     */
    @Override
    public CompletableFuture<OffsetFetchResponseData.OffsetFetchResponseGroup> fetchAllOffsets(
        RequestContext context,
        OffsetFetchRequestData.OffsetFetchRequestGroup request,
        boolean requireStable
    ) {
        if (!isActive.get()) {
            return CompletableFuture.completedFuture(new OffsetFetchResponseData.OffsetFetchResponseGroup()
                .setGroupId(request.groupId())
                .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
            );
        }

        // For backwards compatibility, we support fetch commits for the empty group id.
        if (request.groupId() == null) {
            return CompletableFuture.completedFuture(new OffsetFetchResponseData.OffsetFetchResponseGroup()
                .setGroupId(request.groupId())
                .setErrorCode(Errors.INVALID_GROUP_ID.code())
            );
        }

        // The require stable flag when set tells the broker to hold on returning unstable
        // (or uncommitted) offsets. In the previous implementation of the group coordinator,
        // the UNSTABLE_OFFSET_COMMIT error is returned when unstable offsets are present. As
        // the new implementation relies on timeline data structures, the coordinator does not
        // really know whether offsets are stable or not so it is hard to return the same error.
        // Instead, we use a write operation when the flag is set to guarantee that the fetch
        // is based on all the available offsets and to ensure that the response waits until
        // the pending offsets are committed. Otherwise, we use a read operation.
        if (requireStable) {
            return runtime.scheduleWriteOperation(
                "fetch-all-offsets",
                topicPartitionFor(request.groupId()),
                Duration.ofMillis(config.offsetCommitTimeoutMs()),
                coordinator -> new CoordinatorResult<>(
                    Collections.emptyList(),
                    coordinator.fetchAllOffsets(request, Long.MAX_VALUE)
                )
            ).exceptionally(exception -> handleOffsetFetchException(
                "fetch-all-offsets",
                request,
                exception
            ));
        } else {
            return runtime.scheduleReadOperation(
                "fetch-all-offsets",
                topicPartitionFor(request.groupId()),
                (coordinator, offset) -> coordinator.fetchAllOffsets(request, offset)
            );
        }
    }

    /**
     * See {@link GroupCoordinator#commitOffsets(RequestContext, OffsetCommitRequestData, BufferSupplier)}.
     */
    @Override
    public CompletableFuture<OffsetCommitResponseData> commitOffsets(
        RequestContext context,
        OffsetCommitRequestData request,
        BufferSupplier bufferSupplier
    ) {
        if (!isActive.get()) {
            return CompletableFuture.completedFuture(OffsetCommitRequest.getErrorResponse(
                request,
                Errors.COORDINATOR_NOT_AVAILABLE
            ));
        }

        // For backwards compatibility, we support offset commits for the empty groupId.
        if (request.groupId() == null) {
            return CompletableFuture.completedFuture(OffsetCommitRequest.getErrorResponse(
                request,
                Errors.INVALID_GROUP_ID
            ));
        }

        return runtime.scheduleWriteOperation(
            "commit-offset",
            topicPartitionFor(request.groupId()),
            Duration.ofMillis(config.offsetCommitTimeoutMs()),
            coordinator -> coordinator.commitOffset(context, request)
        ).exceptionally(exception -> handleOperationException(
            "commit-offset",
            request,
            exception,
            (error, __) -> OffsetCommitRequest.getErrorResponse(request, error),
            log
        ));
    }

    /**
     * See {@link GroupCoordinator#commitTransactionalOffsets(RequestContext, TxnOffsetCommitRequestData, BufferSupplier)}.
     */
    @Override
    public CompletableFuture<TxnOffsetCommitResponseData> commitTransactionalOffsets(
        RequestContext context,
        TxnOffsetCommitRequestData request,
        BufferSupplier bufferSupplier
    ) {
        if (!isActive.get()) {
            return CompletableFuture.completedFuture(TxnOffsetCommitRequest.getErrorResponse(
                request,
                Errors.COORDINATOR_NOT_AVAILABLE
            ));
        }

        if (!isGroupIdNotEmpty(request.groupId())) {
            return CompletableFuture.completedFuture(TxnOffsetCommitRequest.getErrorResponse(
                request,
                Errors.INVALID_GROUP_ID
            ));
        }

        return runtime.scheduleTransactionalWriteOperation(
            "txn-commit-offset",
            topicPartitionFor(request.groupId()),
            request.transactionalId(),
            request.producerId(),
            request.producerEpoch(),
            Duration.ofMillis(config.offsetCommitTimeoutMs()),
            coordinator -> coordinator.commitTransactionalOffset(context, request),
            context.apiVersion()
        ).exceptionally(exception -> handleOperationException(
            "txn-commit-offset",
            request,
            exception,
            (error, __) -> TxnOffsetCommitRequest.getErrorResponse(request, error),
            log
        ));
    }

    /**
     * See {@link GroupCoordinator#deleteOffsets(RequestContext, OffsetDeleteRequestData, BufferSupplier)}.
     */
    @Override
    public CompletableFuture<OffsetDeleteResponseData> deleteOffsets(
        RequestContext context,
        OffsetDeleteRequestData request,
        BufferSupplier bufferSupplier
    ) {
        if (!isActive.get()) {
            return CompletableFuture.completedFuture(new OffsetDeleteResponseData()
                .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
            );
        }

        if (!isGroupIdNotEmpty(request.groupId())) {
            return CompletableFuture.completedFuture(new OffsetDeleteResponseData()
                .setErrorCode(Errors.INVALID_GROUP_ID.code())
            );
        }

        return runtime.scheduleWriteOperation(
            "delete-offsets",
            topicPartitionFor(request.groupId()),
            Duration.ofMillis(config.offsetCommitTimeoutMs()),
            coordinator -> coordinator.deleteOffsets(context, request)
        ).exceptionally(exception -> handleOperationException(
            "delete-offsets",
            request,
            exception,
            (error, __) -> new OffsetDeleteResponseData().setErrorCode(error.code()),
            log
        ));
    }

    /**
     * See {@link GroupCoordinator#completeTransaction(TopicPartition, long, short, int, TransactionResult, Duration)}.
     */
    @Override
    public CompletableFuture<Void> completeTransaction(
        TopicPartition tp,
        long producerId,
        short producerEpoch,
        int coordinatorEpoch,
        TransactionResult result,
        Duration timeout
    ) {
        if (!isActive.get()) {
            return FutureUtils.failedFuture(Errors.COORDINATOR_NOT_AVAILABLE.exception());
        }

        if (!tp.topic().equals(Topic.GROUP_METADATA_TOPIC_NAME)) {
            return FutureUtils.failedFuture(new IllegalStateException(
                "Completing a transaction for " + tp + " is not expected"
            ));
        }

        return runtime.scheduleTransactionCompletion(
            "write-txn-marker",
            tp,
            producerId,
            producerEpoch,
            coordinatorEpoch,
            result,
            timeout
        );
    }

    /**
     * See {@link GroupCoordinator#onTransactionCompleted(long, Iterable, TransactionResult)}.
     */
    @Override
    public CompletableFuture<Void> onTransactionCompleted(
        long producerId,
        Iterable<TopicPartition> partitions,
        TransactionResult transactionResult
    ) {
        throwIfNotActive();
        throw new IllegalStateException("onTransactionCompleted is not supported.");
    }

    /**
     * See {@link GroupCoordinator#onPartitionsDeleted(List, BufferSupplier)}.
     */
    @Override
    public void onPartitionsDeleted(
        List<TopicPartition> topicPartitions,
        BufferSupplier bufferSupplier
    ) throws ExecutionException, InterruptedException {
        throwIfNotActive();

        CompletableFuture.allOf(
            FutureUtils.mapExceptionally(
                runtime.scheduleWriteAllOperation(
                    "on-partition-deleted",
                    Duration.ofMillis(config.offsetCommitTimeoutMs()),
                    coordinator -> coordinator.onPartitionsDeleted(topicPartitions)
                ),
                exception -> {
                    log.error("Could not delete offsets for deleted partitions {} due to: {}.",
                        topicPartitions, exception.getMessage(), exception
                    );
                    return null;
                }
            ).toArray(new CompletableFuture[0])
        ).get();
    }

    /**
     * See {@link GroupCoordinator#onElection(int, int)}.
     */
    @Override
    public void onElection(
        int groupMetadataPartitionIndex,
        int groupMetadataPartitionLeaderEpoch
    ) {
        throwIfNotActive();
        runtime.scheduleLoadOperation(
            new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupMetadataPartitionIndex),
            groupMetadataPartitionLeaderEpoch
        );
    }

    /**
     * See {@link GroupCoordinator#onResignation(int, OptionalInt)}.
     */
    @Override
    public void onResignation(
        int groupMetadataPartitionIndex,
        OptionalInt groupMetadataPartitionLeaderEpoch
    ) {
        throwIfNotActive();
        runtime.scheduleUnloadOperation(
            new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupMetadataPartitionIndex),
            groupMetadataPartitionLeaderEpoch
        );
    }

    /**
     * See {@link GroupCoordinator#onNewMetadataImage(MetadataImage, MetadataDelta)}.
     */
    @Override
    public void onNewMetadataImage(
        MetadataImage newImage,
        MetadataDelta delta
    ) {
        throwIfNotActive();
        runtime.onNewMetadataImage(newImage, delta);
    }

    /**
     * See {@link GroupCoordinator#groupMetadataTopicConfigs()}.
     */
    @Override
    public Properties groupMetadataTopicConfigs() {
        Properties properties = new Properties();
        properties.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        properties.put(TopicConfig.COMPRESSION_TYPE_CONFIG, BrokerCompressionType.PRODUCER.name);
        properties.put(TopicConfig.SEGMENT_BYTES_CONFIG, String.valueOf(config.offsetsTopicSegmentBytes()));
        return properties;
    }

    /**
     * See {@link GroupCoordinator#groupConfig(String)}.
     */
    @Override
    public Optional<GroupConfig> groupConfig(String groupId) {
        return groupConfigManager.groupConfig(groupId);
    }

    /**
     * See {@link GroupCoordinator#updateGroupConfig(String, Properties)}.
     */
    @Override
    public void updateGroupConfig(String groupId, Properties newGroupConfig) {
        groupConfigManager.updateGroupConfig(groupId, newGroupConfig);
    }

    /**
     * See {@link GroupCoordinator#startup(IntSupplier)}.
     */
    @Override
    public void startup(
        IntSupplier groupMetadataTopicPartitionCount
    ) {
        if (!isActive.compareAndSet(false, true)) {
            log.warn("Group coordinator is already running.");
            return;
        }

        log.info("Starting up.");
        numPartitions = groupMetadataTopicPartitionCount.getAsInt();
        isActive.set(true);
        log.info("Startup complete.");
    }

    /**
     * See {@link GroupCoordinator#shutdown()}.
     */
    @Override
    public void shutdown() {
        if (!isActive.compareAndSet(true, false)) {
            log.warn("Group coordinator is already shutting down.");
            return;
        }

        log.info("Shutting down.");
        isActive.set(false);
        Utils.closeQuietly(runtime, "coordinator runtime");
        Utils.closeQuietly(groupCoordinatorMetrics, "group coordinator metrics");
        Utils.closeQuietly(groupConfigManager, "group config manager");
        log.info("Shutdown complete.");
    }

    private static boolean isGroupIdNotEmpty(String groupId) {
        return groupId != null && !groupId.isEmpty();
    }

    /**
     * This is the handler used by offset fetch operations to convert errors to coordinator errors.
     * The handler also handles and logs unexpected errors.
     *
     * @param operationName     The name of the operation.
     * @param request           The OffsetFetchRequestGroup request.
     * @param exception         The exception to handle.
     * @return The OffsetFetchRequestGroup response.
     */
    private OffsetFetchResponseData.OffsetFetchResponseGroup handleOffsetFetchException(
        String operationName,
        OffsetFetchRequestData.OffsetFetchRequestGroup request,
        Throwable exception
    ) {
        ApiError apiError = ApiError.fromThrowable(exception);

        switch (apiError.error()) {
            case UNKNOWN_TOPIC_OR_PARTITION:
            case NOT_ENOUGH_REPLICAS:
            case REQUEST_TIMED_OUT:
                // Remap REQUEST_TIMED_OUT to NOT_COORDINATOR, since consumers on versions prior
                // to 3.9 do not expect the error and won't retry the request. NOT_COORDINATOR
                // additionally triggers coordinator re-lookup, which is necessary if the client is
                // talking to a zombie coordinator.
                //
                // While handleOperationException does remap UNKNOWN_TOPIC_OR_PARTITION,
                // NOT_ENOUGH_REPLICAS and REQUEST_TIMED_OUT to COORDINATOR_NOT_AVAILABLE,
                // COORDINATOR_NOT_AVAILABLE is also not handled by consumers on versions prior to
                // 3.9.
                return new OffsetFetchResponseData.OffsetFetchResponseGroup()
                    .setGroupId(request.groupId())
                    .setErrorCode(Errors.NOT_COORDINATOR.code());

            default:
                return handleOperationException(
                    operationName,
                    request,
                    exception,
                    (error, __) -> new OffsetFetchResponseData.OffsetFetchResponseGroup()
                        .setGroupId(request.groupId())
                        .setErrorCode(error.code()),
                    log
                );
        }
    }
}
