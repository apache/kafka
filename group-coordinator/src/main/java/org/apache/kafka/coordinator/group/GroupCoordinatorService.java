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
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.NotCoordinatorException;
import org.apache.kafka.common.errors.StreamsInvalidTopologyException;
import org.apache.kafka.common.errors.UnsupportedAssignorException;
import org.apache.kafka.common.internals.Plugin;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.AlterShareGroupOffsetsRequestData;
import org.apache.kafka.common.message.AlterShareGroupOffsetsResponseData;
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.DeleteGroupsResponseData;
import org.apache.kafka.common.message.DeleteShareGroupOffsetsRequestData;
import org.apache.kafka.common.message.DeleteShareGroupOffsetsResponseData;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.DescribeShareGroupOffsetsRequestData;
import org.apache.kafka.common.message.DescribeShareGroupOffsetsResponseData;
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
import org.apache.kafka.common.message.ReadShareGroupStateSummaryRequestData;
import org.apache.kafka.common.message.ShareGroupDescribeResponseData;
import org.apache.kafka.common.message.ShareGroupDescribeResponseData.DescribedGroup;
import org.apache.kafka.common.message.ShareGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ShareGroupHeartbeatResponseData;
import org.apache.kafka.common.message.StreamsGroupDescribeResponseData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AlterShareGroupOffsetsRequest;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.ConsumerGroupDescribeRequest;
import org.apache.kafka.common.requests.DeleteGroupsRequest;
import org.apache.kafka.common.requests.DeleteShareGroupOffsetsRequest;
import org.apache.kafka.common.requests.DescribeGroupsRequest;
import org.apache.kafka.common.requests.DescribeShareGroupOffsetsRequest;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.ShareGroupDescribeRequest;
import org.apache.kafka.common.requests.ShareGroupHeartbeatRequest;
import org.apache.kafka.common.requests.StreamsGroupDescribeRequest;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.requests.TxnOffsetCommitRequest;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.common.runtime.CoordinatorEventProcessor;
import org.apache.kafka.coordinator.common.runtime.CoordinatorLoader;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataDelta;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorResult;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRuntime;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRuntimeMetrics;
import org.apache.kafka.coordinator.common.runtime.CoordinatorShardBuilderSupplier;
import org.apache.kafka.coordinator.common.runtime.MultiThreadedEventProcessor;
import org.apache.kafka.coordinator.common.runtime.PartitionWriter;
import org.apache.kafka.coordinator.group.api.assignor.ConsumerGroupPartitionAssignor;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics;
import org.apache.kafka.coordinator.group.streams.StreamsGroupHeartbeatResult;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.record.BrokerCompressionType;
import org.apache.kafka.server.share.persister.DeleteShareGroupStateParameters;
import org.apache.kafka.server.share.persister.DeleteShareGroupStateResult;
import org.apache.kafka.server.share.persister.GroupTopicPartitionData;
import org.apache.kafka.server.share.persister.InitializeShareGroupStateParameters;
import org.apache.kafka.server.share.persister.InitializeShareGroupStateResult;
import org.apache.kafka.server.share.persister.PartitionErrorData;
import org.apache.kafka.server.share.persister.PartitionFactory;
import org.apache.kafka.server.share.persister.PartitionStateData;
import org.apache.kafka.server.share.persister.PartitionStateSummaryData;
import org.apache.kafka.server.share.persister.Persister;
import org.apache.kafka.server.share.persister.ReadShareGroupStateSummaryParameters;
import org.apache.kafka.server.share.persister.ReadShareGroupStateSummaryResult;
import org.apache.kafka.server.share.persister.TopicData;
import org.apache.kafka.server.util.FutureUtils;
import org.apache.kafka.server.util.timer.Timer;
import org.apache.kafka.server.util.timer.TimerTask;

import org.slf4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;

import static org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest.CONSUMER_GENERATED_MEMBER_ID_REQUIRED_VERSION;
import static org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH;
import static org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest.LEAVE_GROUP_STATIC_MEMBER_EPOCH;
import static org.apache.kafka.coordinator.common.runtime.CoordinatorOperationExceptionHelper.handleOperationException;
import static org.apache.kafka.coordinator.group.Utils.throwIfEmptyString;
import static org.apache.kafka.coordinator.group.Utils.throwIfNotEmptyCollection;
import static org.apache.kafka.coordinator.group.Utils.throwIfNotNull;
import static org.apache.kafka.coordinator.group.Utils.throwIfNotNullOrEmpty;
import static org.apache.kafka.coordinator.group.Utils.throwIfNull;

/**
 * The group coordinator service.
 */
@SuppressWarnings({"ClassDataAbstractionCoupling"})
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
        private Persister persister;
        private Optional<Plugin<Authorizer>> authorizerPlugin;
        private PartitionMetadataClient partitionMetadataClient;

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

        public Builder withPersister(Persister persister) {
            this.persister = persister;
            return this;
        }

        public Builder withAuthorizerPlugin(Optional<Plugin<Authorizer>> authorizerPlugin) {
            this.authorizerPlugin = authorizerPlugin;
            return this;
        }

        public Builder withPartitionMetadataClient(PartitionMetadataClient partitionMetadataClient) {
            this.partitionMetadataClient = partitionMetadataClient;
            return this;
        }

        public GroupCoordinatorService build() {
            requireNonNull(config, "Config must be set.");
            requireNonNull(writer, "Writer must be set.");
            requireNonNull(loader, "Loader must be set.");
            requireNonNull(time, "Time must be set.");
            requireNonNull(timer, "Timer must be set.");
            requireNonNull(coordinatorRuntimeMetrics, "CoordinatorRuntimeMetrics must be set.");
            requireNonNull(groupCoordinatorMetrics, "GroupCoordinatorMetrics must be set.");
            requireNonNull(groupConfigManager, "GroupConfigManager must be set.");
            requireNonNull(persister, "Persister must be set.");
            requireNonNull(authorizerPlugin, "Authorizer must be set.");
            requireNonNull(partitionMetadataClient, "PartitionMetadataClient must be set.");

            String logPrefix = String.format("GroupCoordinator id=%d", nodeId);
            LogContext logContext = new LogContext(String.format("[%s] ", logPrefix));

            CoordinatorShardBuilderSupplier<GroupCoordinatorShard, CoordinatorRecord> supplier = () ->
                new GroupCoordinatorShard.Builder(config, groupConfigManager)
                    .withAuthorizerPlugin(authorizerPlugin);

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
                groupConfigManager,
                persister,
                timer,
                partitionMetadataClient
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
     * The Persister to persist the state of share partition state.
     */
    private final Persister persister;

    /**
     * The timer to schedule tasks.
     */
    private final Timer timer;

    /**
     * Boolean indicating whether the coordinator is active or not.
     */
    private final AtomicBoolean isActive = new AtomicBoolean(false);

    /**
     * The set of supported consumer group assignors.
     */
    private final Set<String> consumerGroupAssignors;

    /**
     * The client used for getting partition end offsets
     */
    private final PartitionMetadataClient partitionMetadataClient;

    /**
     * The number of partitions of the __consumer_offsets topics. This is provided
     * when the component is started.
     */
    private volatile int numPartitions = -1;

    /**
     * The metadata image to extract topic id to names map.
     * This is initialised when the {@link GroupCoordinator#onNewMetadataImage(CoordinatorMetadataImage, CoordinatorMetadataDelta)} is called
     */
    private CoordinatorMetadataImage metadataImage = null;

    /**
     *
     * @param logContext                The log context.
     * @param config                    The group coordinator config.
     * @param runtime                   The runtime.
     * @param groupCoordinatorMetrics   The group coordinator metrics.
     * @param groupConfigManager        The group config manager.
     * @param persister                 The persister.
     * @param timer                     The timer.
     */
    GroupCoordinatorService(
        LogContext logContext,
        GroupCoordinatorConfig config,
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime,
        GroupCoordinatorMetrics groupCoordinatorMetrics,
        GroupConfigManager groupConfigManager,
        Persister persister,
        Timer timer,
        PartitionMetadataClient partitionMetadataClient
    ) {
        this.log = logContext.logger(GroupCoordinatorService.class);
        this.config = config;
        this.runtime = runtime;
        this.groupCoordinatorMetrics = groupCoordinatorMetrics;
        this.groupConfigManager = groupConfigManager;
        this.persister = persister;
        this.timer = timer;
        this.consumerGroupAssignors = config
            .consumerGroupAssignors()
            .stream()
            .map(ConsumerGroupPartitionAssignor::name)
            .collect(Collectors.toSet());
        this.partitionMetadataClient = partitionMetadataClient;
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
     * Validates the request.
     *
     * @param request The request to validate.
     * @param apiVersion The version of ConsumerGroupHeartbeat RPC
     * @throws InvalidRequestException if the request is not valid.
     * @throws UnsupportedAssignorException if the assignor is not supported.
     */
    private void throwIfConsumerGroupHeartbeatRequestIsInvalid(
        ConsumerGroupHeartbeatRequestData request,
        int apiVersion
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

        if (request.serverAssignor() != null && !consumerGroupAssignors.contains(request.serverAssignor())) {
            throw new UnsupportedAssignorException("ServerAssignor " + request.serverAssignor()
                + " is not supported. Supported assignors: " + String.join(", ", consumerGroupAssignors)
                + ".");
        }
    }

    /**
     * See {@link GroupCoordinator#consumerGroupHeartbeat(AuthorizableRequestContext, ConsumerGroupHeartbeatRequestData)}.
     */
    @Override
    public CompletableFuture<ConsumerGroupHeartbeatResponseData> consumerGroupHeartbeat(
        AuthorizableRequestContext context,
        ConsumerGroupHeartbeatRequestData request
    ) {
        if (!isActive.get()) {
            return CompletableFuture.completedFuture(new ConsumerGroupHeartbeatResponseData()
                .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
            );
        }

        try {
            throwIfConsumerGroupHeartbeatRequestIsInvalid(request, context.requestVersion());
        } catch (Throwable ex) {
            ApiError apiError = ApiError.fromThrowable(ex);
            return CompletableFuture.completedFuture(new ConsumerGroupHeartbeatResponseData()
                .setErrorCode(apiError.error().code())
                .setErrorMessage(apiError.message())
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

    private static void throwIfInvalidTopology(
        StreamsGroupHeartbeatRequestData.Topology topology
    ) throws StreamsInvalidTopologyException {
        for (StreamsGroupHeartbeatRequestData.Subtopology subtopology: topology.subtopologies()) {
            for (StreamsGroupHeartbeatRequestData.TopicInfo topicInfo: subtopology.stateChangelogTopics()) {
                if (topicInfo.partitions() != 0) {
                    throw new StreamsInvalidTopologyException(String.format(
                        "Changelog topic %s must have an undefined partition count, but it is set to %d.",
                        topicInfo.name(), topicInfo.partitions()
                    ));
                }
            }
        }
    }

    /**
     * Validates the request.
     *
     * @param request The request to validate.
     * @throws InvalidRequestException if the request is not valid.
     * @throws UnsupportedAssignorException if the assignor is not supported.
     */
    private static void throwIfStreamsGroupHeartbeatRequestIsInvalid(
        StreamsGroupHeartbeatRequestData request
    ) throws InvalidRequestException {
        throwIfEmptyString(request.memberId(), "MemberId can't be empty.");
        throwIfEmptyString(request.groupId(), "GroupId can't be empty.");
        throwIfEmptyString(request.instanceId(), "InstanceId can't be empty.");
        throwIfEmptyString(request.rackId(), "RackId can't be empty.");

        if (request.memberEpoch() == 0) {
            if (request.rebalanceTimeoutMs() == -1) {
                throw new InvalidRequestException("RebalanceTimeoutMs must be provided in first request.");
            }
            throwIfNotEmptyCollection(request.activeTasks(), "ActiveTasks must be empty when (re-)joining.");
            throwIfNotEmptyCollection(request.standbyTasks(), "StandbyTasks must be empty when (re-)joining.");
            throwIfNotEmptyCollection(request.warmupTasks(), "WarmupTasks must be empty when (re-)joining.");
            throwIfNull(request.topology(), "Topology must be non-null when (re-)joining.");
            if (request.topology() != null) {
                throwIfInvalidTopology(request.topology());
            }
        } else if (request.memberEpoch() == LEAVE_GROUP_STATIC_MEMBER_EPOCH) {
            throwIfNull(request.instanceId(), "InstanceId can't be null.");
        } else if (request.memberEpoch() < LEAVE_GROUP_STATIC_MEMBER_EPOCH) {
            throw new InvalidRequestException(String.format("MemberEpoch is %d, but must be greater than or equal to -2.",
                request.memberEpoch()));
        }

        if (request.activeTasks() != null || request.standbyTasks() != null || request.warmupTasks() != null) {
            throwIfNull(request.activeTasks(), "If one task-type is non-null, all must be non-null.");
            throwIfNull(request.standbyTasks(), "If one task-type is non-null, all must be non-null.");
            throwIfNull(request.warmupTasks(), "If one task-type is non-null, all must be non-null.");
        }

        if (request.memberEpoch() != 0) {
            throwIfNotNull(request.topology(), "Topology can only be provided when (re-)joining.");
        }
    }

    /**
     * Validates the request. Specifically, throws if any not-yet-supported features are used.
     *
     * @param request The request to validate.
     * @throws InvalidRequestException if the request is not valid.
     */
    private static void throwIfStreamsGroupHeartbeatRequestIsUsingUnsupportedFeatures(
        StreamsGroupHeartbeatRequestData request
    ) throws InvalidRequestException {
        throwIfNotNull(request.instanceId(), "Static membership is not yet supported.");
        throwIfNotNull(request.taskOffsets(), "TaskOffsets are not supported yet.");
        throwIfNotNull(request.taskEndOffsets(), "TaskEndOffsets are not supported yet.");
        throwIfNotNullOrEmpty(request.warmupTasks(), "WarmupTasks are not supported yet.");
        if (request.topology() != null) {
            for (StreamsGroupHeartbeatRequestData.Subtopology subtopology : request.topology().subtopologies()) {
                throwIfNotEmptyCollection(subtopology.sourceTopicRegex(), "Regular expressions for source topics are not supported yet.");
            }
        }
    }

    /**
     * See
     * {@link GroupCoordinator#streamsGroupHeartbeat(AuthorizableRequestContext, StreamsGroupHeartbeatRequestData)}.
     */
    @Override
    public CompletableFuture<StreamsGroupHeartbeatResult> streamsGroupHeartbeat(
        AuthorizableRequestContext context,
        StreamsGroupHeartbeatRequestData request
    ) {
        if (!isActive.get()) {
            return CompletableFuture.completedFuture(
                new StreamsGroupHeartbeatResult(
                    new StreamsGroupHeartbeatResponseData().setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code()),
                    Map.of()
                )
            );
        }

        try {
            throwIfStreamsGroupHeartbeatRequestIsUsingUnsupportedFeatures(request);
            throwIfStreamsGroupHeartbeatRequestIsInvalid(request);
        } catch (Throwable ex) {
            ApiError apiError = ApiError.fromThrowable(ex);
            return CompletableFuture.completedFuture(
                new StreamsGroupHeartbeatResult(
                    new StreamsGroupHeartbeatResponseData()
                        .setErrorCode(apiError.error().code())
                        .setErrorMessage(apiError.message()),
                    Map.of()
                )
            );
        }

        return runtime.scheduleWriteOperation(
            "streams-group-heartbeat",
            topicPartitionFor(request.groupId()),
            Duration.ofMillis(config.offsetCommitTimeoutMs()),
            coordinator -> coordinator.streamsGroupHeartbeat(context, request)
        ).exceptionally(exception -> handleOperationException(
            "streams-group-heartbeat",
            request,
            exception,
            (error, message) ->
                new StreamsGroupHeartbeatResult(
                    new StreamsGroupHeartbeatResponseData()
                        .setErrorCode(error.code())
                        .setErrorMessage(message),
                    Map.of()
                ),
            log
        ));
    }

    /**
     * Validates the ShareGroupHeartbeat request.
     *
     * @param request The request to validate.
     * @throws InvalidRequestException if the request is not valid.
     */
    private static void throwIfShareGroupHeartbeatRequestIsInvalid(
        ShareGroupHeartbeatRequestData request
    ) throws InvalidRequestException {
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
     * See {@link GroupCoordinator#shareGroupHeartbeat(AuthorizableRequestContext, ShareGroupHeartbeatRequestData)}.
     */
    @Override
    public CompletableFuture<ShareGroupHeartbeatResponseData> shareGroupHeartbeat(
        AuthorizableRequestContext context,
        ShareGroupHeartbeatRequestData request
    ) {
        if (!isActive.get()) {
            return CompletableFuture.completedFuture(new ShareGroupHeartbeatResponseData()
                .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
            );
        }

        try {
            throwIfShareGroupHeartbeatRequestIsInvalid(request);
        } catch (Throwable ex) {
            ApiError apiError = ApiError.fromThrowable(ex);
            return CompletableFuture.completedFuture(new ShareGroupHeartbeatResponseData()
                .setErrorCode(apiError.error().code())
                .setErrorMessage(apiError.message())
            );
        }

        return runtime.scheduleWriteOperation(
            "share-group-heartbeat",
            topicPartitionFor(request.groupId()),
            Duration.ofMillis(config.offsetCommitTimeoutMs()),
            coordinator -> coordinator.shareGroupHeartbeat(context, request)
        ).thenCompose(result -> {
            if (result.getValue().isPresent()) {
                // Adding to timer makes this call async with respect to the heartbeat.
                timer.add(new TimerTask(0L) {
                    @Override
                    public void run() {
                        persisterInitialize(result.getValue().get(), result.getKey())
                            .whenComplete((__, exp) -> {
                                if (exp != null) {
                                    log.error("Persister initialization failed", exp);
                                }
                            });
                    }
                });
            }
            return CompletableFuture.completedFuture(result.getKey());
        }).exceptionally(exception -> handleOperationException(
            "share-group-heartbeat",
            request,
            exception,
            (error, message) -> new ShareGroupHeartbeatResponseData()
                .setErrorCode(error.code())
                .setErrorMessage(message),
            log
        ));
    }

    // Visibility for testing
    CompletableFuture<AlterShareGroupOffsetsResponseData> persisterInitialize(
        InitializeShareGroupStateParameters request,
        AlterShareGroupOffsetsResponseData response
    ) {
        if (request.groupTopicPartitionData().topicsData().isEmpty()) {
            return CompletableFuture.completedFuture(response);
        }

        return persister.initializeState(request)
            .handle((result, exp) -> {
                if (exp == null) {
                    if (result.errorCounts().isEmpty()) {
                        handlePersisterInitializeResponse(request.groupTopicPartitionData().groupId(), result, new ShareGroupHeartbeatResponseData());
                        return response;
                    } else {
                        return buildErrorResponse(response, result);
                    }
                } else {
                    return buildErrorResponse(request, response, exp);
                }

            });
    }
    
    private AlterShareGroupOffsetsResponseData buildErrorResponse(AlterShareGroupOffsetsResponseData response, InitializeShareGroupStateResult result) {
        AlterShareGroupOffsetsResponseData data = new AlterShareGroupOffsetsResponseData();
        Map<Uuid, Map<Integer, PartitionErrorData>> topicPartitionErrorsMap = result.getErrors();
        data.setResponses(
            new AlterShareGroupOffsetsResponseData.AlterShareGroupOffsetsResponseTopicCollection(response.responses().stream()
                .map(topic -> {
                    AlterShareGroupOffsetsResponseData.AlterShareGroupOffsetsResponseTopic topicData = new AlterShareGroupOffsetsResponseData.AlterShareGroupOffsetsResponseTopic()
                        .setTopicName(topic.topicName())
                        .setTopicId(topic.topicId());
                    topic.partitions().forEach(partition -> {
                        if (partition.errorCode() != Errors.NONE.code()) {
                            topicData.partitions().add(partition);
                            return;
                        }
                        AlterShareGroupOffsetsResponseData.AlterShareGroupOffsetsResponsePartition partitionData;
                        Map<Integer, PartitionErrorData> partitionErrors =
                            Optional.ofNullable(topicPartitionErrorsMap)
                                .map(map -> map.get(topic.topicId()))
                                .orElse(Collections.emptyMap());
                        PartitionErrorData error = partitionErrors.get(partition.partitionIndex());
                        if (error == null) {
                            partitionData = partition.duplicate();
                        } else {
                            partitionData = new AlterShareGroupOffsetsResponseData.AlterShareGroupOffsetsResponsePartition()
                                .setPartitionIndex(partition.partitionIndex())
                                .setErrorCode(error.errorCode())
                                .setErrorMessage(error.errorMessage());
                        }
                        topicData.partitions().add(partitionData);
                    });
                    return topicData;
                })
                .iterator()));
        return data;
    }

    private AlterShareGroupOffsetsResponseData buildErrorResponse(InitializeShareGroupStateParameters request, AlterShareGroupOffsetsResponseData response, Throwable exp) {
        // build new AlterShareGroupOffsetsResponseData for error response
        AlterShareGroupOffsetsResponseData data = new AlterShareGroupOffsetsResponseData();
        GroupTopicPartitionData<PartitionStateData> gtp = request.groupTopicPartitionData();
        log.error("Unable to initialize share group state for {}, {} while altering share group offsets", gtp.groupId(), gtp.topicsData(), exp);
        Errors error = Errors.forException(exp);
        data.setErrorCode(error.code())
            .setErrorMessage(exp.getMessage())
            .setResponses(response.responses());
        data.setResponses(
            new AlterShareGroupOffsetsResponseData.AlterShareGroupOffsetsResponseTopicCollection(response.responses().stream()
                .map(topic -> {
                    AlterShareGroupOffsetsResponseData.AlterShareGroupOffsetsResponseTopic topicData = new AlterShareGroupOffsetsResponseData.AlterShareGroupOffsetsResponseTopic()
                        .setTopicName(topic.topicName())
                        .setTopicId(topic.topicId());
                    topic.partitions().forEach(partition -> {
                        AlterShareGroupOffsetsResponseData.AlterShareGroupOffsetsResponsePartition partitionData = new AlterShareGroupOffsetsResponseData.AlterShareGroupOffsetsResponsePartition()
                            .setPartitionIndex(partition.partitionIndex())
                            .setErrorCode(error.code())
                            .setErrorMessage(error.message());
                        topicData.partitions().add(partitionData);
                    });
                    return topicData;
                })
                .iterator()));
        // don't uninitialized share group state here, as we regard this alter share group offsets request failed.
        return data;
    }

    // Visibility for testing
    CompletableFuture<ShareGroupHeartbeatResponseData> persisterInitialize(
        InitializeShareGroupStateParameters request,
        ShareGroupHeartbeatResponseData defaultResponse
    ) {
        return persister.initializeState(request)
            .handle((response, exp) -> {
                if (exp == null) {
                    return handlePersisterInitializeResponse(request.groupTopicPartitionData().groupId(), response, defaultResponse);
                }
                GroupTopicPartitionData<PartitionStateData> gtp = request.groupTopicPartitionData();
                log.error("Unable to initialize share group state {}, {}", gtp.groupId(), gtp.topicsData(), exp);
                Errors error = Errors.forException(exp);
                Map<Uuid, Set<Integer>> topicPartitionMap = new HashMap<>();
                gtp.topicsData().forEach(topicData -> topicPartitionMap.computeIfAbsent(topicData.topicId(), k -> new HashSet<>())
                    .addAll(topicData.partitions().stream().map(PartitionStateData::partition).collect(Collectors.toSet())));
                return uninitializeShareGroupState(error, gtp.groupId(), topicPartitionMap);
            })
            .thenCompose(resp -> resp);
    }

    private CompletableFuture<ShareGroupHeartbeatResponseData> handlePersisterInitializeResponse(
        String groupId,
        InitializeShareGroupStateResult persisterInitializeResult,
        ShareGroupHeartbeatResponseData defaultResponse
    ) {
        Errors persisterError = Errors.NONE;
        for (TopicData<PartitionErrorData> topicData : persisterInitializeResult.topicsData()) {
            Optional<PartitionErrorData> errData = topicData.partitions().stream().filter(partition -> partition.errorCode() != Errors.NONE.code()).findAny();
            if (errData.isPresent()) {
                persisterError = Errors.forCode(errData.get().errorCode());
                break;
            }
        }

        Map<Uuid, Set<Integer>> topicPartitionMap = new HashMap<>();
        for (TopicData<PartitionErrorData> topicData : persisterInitializeResult.topicsData()) {
            topicPartitionMap.put(
                topicData.topicId(),
                topicData.partitions().stream().map(PartitionErrorData::partition).collect(Collectors.toSet())
            );
        }

        if (persisterError.code() == Errors.NONE.code()) {
            if (topicPartitionMap.isEmpty()) {
                return CompletableFuture.completedFuture(defaultResponse);
            }
            return performShareGroupStateMetadataInitialize(groupId, topicPartitionMap, defaultResponse);
        }
        log.error("Received error while calling initialize state for {} on persister, errorCode: {}.", groupId, persisterError.code());
        return uninitializeShareGroupState(persisterError, groupId, topicPartitionMap);
    }

    private CompletableFuture<ShareGroupHeartbeatResponseData> uninitializeShareGroupState(
        Errors error,
        String groupId,
        Map<Uuid, Set<Integer>> topicPartitionMap
    ) {
        return runtime.scheduleWriteOperation(
            "uninitialize-share-group-state",
            topicPartitionFor(groupId),
            Duration.ofMillis(config.offsetCommitTimeoutMs()),
            coordinator -> coordinator.uninitializeShareGroupState(groupId, topicPartitionMap)
        ).thenApply(__ -> new ShareGroupHeartbeatResponseData()
            .setErrorCode(error.code())
            .setErrorMessage(error.message())
        ).exceptionally(exception -> {
            log.error("Unable to cleanup topic partitions from share group state metadata", exception);
            Errors err = Errors.forException(new IllegalStateException("Unable to cleanup topic partitions from share group state metadata", exception));
            return new ShareGroupHeartbeatResponseData()
                .setErrorCode(err.code())
                .setErrorMessage(err.message());
        });
    }

    private CompletableFuture<ShareGroupHeartbeatResponseData> performShareGroupStateMetadataInitialize(
        String groupId,
        Map<Uuid, Set<Integer>> topicPartitionMap,
        ShareGroupHeartbeatResponseData defaultResponse
    ) {
        return runtime.scheduleWriteOperation(
            "initialize-share-group-state",
            topicPartitionFor(groupId),
            Duration.ofMillis(config.offsetCommitTimeoutMs()),
            coordinator -> coordinator.initializeShareGroupState(groupId, topicPartitionMap)
        ).handle((__, exp) -> {
            if (exp == null) {
                return CompletableFuture.completedFuture(defaultResponse);
            }
            log.error("Unable to initialize share group state partition metadata for {}.", groupId, exp);
            Errors error = Errors.forException(exp);
            return uninitializeShareGroupState(error, groupId, topicPartitionMap);
        }).thenCompose(resp -> resp);
    }

    /**
     * See {@link GroupCoordinator#joinGroup(AuthorizableRequestContext, JoinGroupRequestData, BufferSupplier)}.
     */
    @Override
    public CompletableFuture<JoinGroupResponseData> joinGroup(
        AuthorizableRequestContext context,
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
     * See {@link GroupCoordinator#syncGroup(AuthorizableRequestContext, SyncGroupRequestData, BufferSupplier)}.
     */
    @Override
    public CompletableFuture<SyncGroupResponseData> syncGroup(
        AuthorizableRequestContext context,
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
     * See {@link GroupCoordinator#heartbeat(AuthorizableRequestContext, HeartbeatRequestData)}.
     */
    @Override
    public CompletableFuture<HeartbeatResponseData> heartbeat(
        AuthorizableRequestContext context,
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
     * See {@link GroupCoordinator#leaveGroup(AuthorizableRequestContext, LeaveGroupRequestData)}.
     */
    @Override
    public CompletableFuture<LeaveGroupResponseData> leaveGroup(
        AuthorizableRequestContext context,
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
                         .toList();
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
     * See {@link GroupCoordinator#listGroups(AuthorizableRequestContext, ListGroupsRequestData)}.
     */
    @Override
    public CompletableFuture<ListGroupsResponseData> listGroups(
        AuthorizableRequestContext context,
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
                    return List.of();
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
     * See {@link GroupCoordinator#consumerGroupDescribe(AuthorizableRequestContext, List)}.
     */
    @Override
    public CompletableFuture<List<ConsumerGroupDescribeResponseData.DescribedGroup>> consumerGroupDescribe(
        AuthorizableRequestContext context,
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
                futures.add(CompletableFuture.completedFuture(List.of(
                    new ConsumerGroupDescribeResponseData.DescribedGroup()
                        .setGroupId("")
                        .setErrorCode(Errors.INVALID_GROUP_ID.code())
                )));
            }
        });

        groupsByTopicPartition.forEach((topicPartition, groupList) -> {
            CompletableFuture<List<ConsumerGroupDescribeResponseData.DescribedGroup>> future =
                runtime.scheduleReadOperation(
                    "consumer-group-describe",
                    topicPartition,
                    (coordinator, lastCommittedOffset) -> coordinator.consumerGroupDescribe(groupList, lastCommittedOffset)
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
     * See {@link GroupCoordinator#streamsGroupDescribe(AuthorizableRequestContext, List)}.
     */
    @Override
    public CompletableFuture<List<StreamsGroupDescribeResponseData.DescribedGroup>> streamsGroupDescribe(
        AuthorizableRequestContext context,
        List<String> groupIds
    ) {
        if (!isActive.get()) {
            return CompletableFuture.completedFuture(StreamsGroupDescribeRequest.getErrorDescribedGroupList(
                groupIds,
                Errors.COORDINATOR_NOT_AVAILABLE
            ));
        }

        final List<CompletableFuture<List<StreamsGroupDescribeResponseData.DescribedGroup>>> futures =
            new ArrayList<>(groupIds.size());
        final Map<TopicPartition, List<String>> groupsByTopicPartition = new HashMap<>();
        groupIds.forEach(groupId -> {
            if (isGroupIdNotEmpty(groupId)) {
                groupsByTopicPartition
                    .computeIfAbsent(topicPartitionFor(groupId), __ -> new ArrayList<>())
                    .add(groupId);
            } else {
                futures.add(CompletableFuture.completedFuture(List.of(
                    new StreamsGroupDescribeResponseData.DescribedGroup()
                        .setGroupId("")
                        .setErrorCode(Errors.INVALID_GROUP_ID.code())
                )));
            }
        });

        groupsByTopicPartition.forEach((topicPartition, groupList) -> {
            CompletableFuture<List<StreamsGroupDescribeResponseData.DescribedGroup>> future =
                runtime.scheduleReadOperation(
                    "streams-group-describe",
                    topicPartition,
                    (coordinator, lastCommittedOffset) -> coordinator.streamsGroupDescribe(groupList, lastCommittedOffset)
                ).exceptionally(exception -> handleOperationException(
                    "streams-group-describe",
                    groupList,
                    exception,
                    (error, __) -> StreamsGroupDescribeRequest.getErrorDescribedGroupList(groupList, error),
                    log
                ));

            futures.add(future);
        });

        return FutureUtils.combineFutures(futures, ArrayList::new, List::addAll);
    }
    
    /**
     * See {@link GroupCoordinator#shareGroupDescribe(AuthorizableRequestContext, List)}.
     */
    @Override
    public CompletableFuture<List<DescribedGroup>> shareGroupDescribe(
        AuthorizableRequestContext context,
        List<String> groupIds
    ) {
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
                futures.add(CompletableFuture.completedFuture(List.of(
                    new ShareGroupDescribeResponseData.DescribedGroup()
                        .setGroupId("")
                        .setErrorCode(Errors.INVALID_GROUP_ID.code())
                )));
            }
        });

        groupsByTopicPartition.forEach((topicPartition, groupList) -> {
            CompletableFuture<List<ShareGroupDescribeResponseData.DescribedGroup>> future =
                runtime.scheduleReadOperation(
                    "share-group-describe",
                    topicPartition,
                    (coordinator, lastCommittedOffset) -> coordinator.shareGroupDescribe(groupList, lastCommittedOffset)
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
     * See {@link GroupCoordinator#alterShareGroupOffsets(AuthorizableRequestContext, String, AlterShareGroupOffsetsRequestData)}.
     */
    @Override
    public CompletableFuture<AlterShareGroupOffsetsResponseData> alterShareGroupOffsets(
        AuthorizableRequestContext context,
        String groupId,
        AlterShareGroupOffsetsRequestData request
    ) {
        if (!isActive.get() || metadataImage == null) {
            return CompletableFuture.completedFuture(AlterShareGroupOffsetsRequest.getErrorResponseData(Errors.COORDINATOR_NOT_AVAILABLE));
        }
        
        if (groupId == null || groupId.isEmpty()) {
            return CompletableFuture.completedFuture(AlterShareGroupOffsetsRequest.getErrorResponseData(Errors.INVALID_GROUP_ID));
        }

        if (request.topics() == null) {
            return CompletableFuture.completedFuture(new AlterShareGroupOffsetsResponseData());
        }

        return runtime.scheduleWriteOperation(
            "share-group-offsets-alter",
            topicPartitionFor(groupId),
            Duration.ofMillis(config.offsetCommitTimeoutMs()),
            coordinator -> coordinator.alterShareGroupOffsets(groupId, request)
        ).thenCompose(result ->
            persisterInitialize(result.getValue(), result.getKey())
        ).exceptionally(exception -> handleOperationException(
            "share-group-offsets-alter",
            request,
            exception,
            AlterShareGroupOffsetsRequest::getErrorResponseData,
            log
        ));
    }

    /**
     * See {@link GroupCoordinator#describeGroups(AuthorizableRequestContext, List)}.
     */
    @Override
    public CompletableFuture<List<DescribeGroupsResponseData.DescribedGroup>> describeGroups(
        AuthorizableRequestContext context,
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
                futures.add(CompletableFuture.completedFuture(List.of(
                    new DescribeGroupsResponseData.DescribedGroup()
                        .setGroupId("")
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
     * See {@link GroupCoordinator#deleteGroups(AuthorizableRequestContext, List, BufferSupplier)}.
     */
    @Override
    public CompletableFuture<DeleteGroupsResponseData.DeletableGroupResultCollection> deleteGroups(
        AuthorizableRequestContext context,
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
            // Since the specific group types are not known, group deletion operations are chained.
            // The sequence of these deletions is important: the initial share group deletion should
            // not error if the group ID isn't found, whereas the subsequent consumer group deletion
            // (the final operation) must return an error if its corresponding group ID is not found.
            CompletableFuture<DeleteGroupsResponseData.DeletableGroupResultCollection> future = deleteShareGroups(topicPartition, groupList).thenCompose(groupErrMap -> {
                DeleteGroupsResponseData.DeletableGroupResultCollection deletableGroupResults = new DeleteGroupsResponseData.DeletableGroupResultCollection();
                List<String> retainedGroupIds = updateResponseAndGetNonErrorGroupList(groupErrMap, groupList, deletableGroupResults);
                if (retainedGroupIds.isEmpty()) {
                    return CompletableFuture.completedFuture(deletableGroupResults);
                }

                return handleDeleteGroups(context, topicPartition, retainedGroupIds)
                    .whenComplete((resp, __) -> resp.forEach(result -> deletableGroupResults.add(result.duplicate())))
                    .thenApply(__ -> deletableGroupResults);
            });
            // deleteShareGroups has its own exceptionally block, so we don't need one here.

            // This future object has the following stages:
            // - First it invokes the share group delete flow where the shard sharePartitionDeleteRequests
            // method is invoked, and it returns request objects for each valid share group passed to it.
            // All initialized and initializing share partitions are moved to deleting.
            // - Then the requests are passed to the persister.deleteState method one at a time. The results
            // are collated as a Map of groupId -> persister errors
            // - The above map can be used to decide whether to invoke the group coordinator delete groups logic
            // - Share groups with failed persister delete are NOT CONSIDERED for group coordinator delete.
            // TLDR: DeleteShareGroups -> filter erroneous persister deletes -> general delete groups logic
            futures.add(future);
        });

        return FutureUtils.combineFutures(futures, DeleteGroupsResponseData.DeletableGroupResultCollection::new,
            // We don't use res.addAll(future.join()) because DeletableGroupResultCollection is an ImplicitLinkedHashMultiCollection,
            // which has requirements for adding elements (see ImplicitLinkedHashCollection.java#add).
            (accumulator, newResults) -> newResults.forEach(result -> accumulator.add(result.duplicate())));
    }

    /**
     * Processes input shareGroupErrMap by retaining only those which do not contain an error.
     * Also updates the result collection input arg with share groups containing errors.
     *
     * @param shareGroupErrMap      Map keyed on share groupId and value as the error (NONE for no error).
     * @param groupList             Entire list of groups (all types)
     * @param deletableGroupResults Collection of responses for delete groups request.
     * @return A list of all non-error groupIds
     */
    private List<String> updateResponseAndGetNonErrorGroupList(
        Map<String, Errors> shareGroupErrMap,
        List<String> groupList,
        DeleteGroupsResponseData.DeletableGroupResultCollection deletableGroupResults
    ) {
        List<String> errGroupIds = new ArrayList<>();
        shareGroupErrMap.forEach((groupId, error) -> {
            if (error.code() != Errors.NONE.code()) {
                log.error("Error deleting share group {} due to error {}", groupId, error);
                errGroupIds.add(groupId);
                deletableGroupResults.add(
                    new DeleteGroupsResponseData.DeletableGroupResult()
                        .setGroupId(groupId)
                        .setErrorCode(error.code())
                );
            }
        });

        Set<String> groupSet = new HashSet<>(groupList);
        // Remove all share group ids which have errored out
        // when deleting with persister.
        errGroupIds.forEach(groupSet::remove);

        // Let us invoke the standard procedure of any non-share
        // groups or successfully deleted share groups remaining.
        return groupSet.stream().toList();
    }

    private CompletableFuture<DeleteGroupsResponseData.DeletableGroupResultCollection> handleDeleteGroups(
        AuthorizableRequestContext context,
        TopicPartition topicPartition,
        List<String> groupIds
    ) {
        return runtime.scheduleWriteOperation(
            "delete-groups",
            topicPartition,
            Duration.ofMillis(config.offsetCommitTimeoutMs()),
            coordinator -> coordinator.deleteGroups(context, groupIds)
        ).exceptionally(exception -> handleOperationException(
            "delete-groups",
            groupIds,
            exception,
            (error, __) -> DeleteGroupsRequest.getErrorResultCollection(groupIds, error),
            log
        ));
    }

    private CompletableFuture<Map<String, Errors>> deleteShareGroups(
        TopicPartition topicPartition,
        List<String> groupList
    ) {
        // topicPartition refers to internal topic __consumer_offsets.
        return runtime.scheduleWriteOperation(
            "delete-share-groups",
            topicPartition,
            Duration.ofMillis(config.offsetCommitTimeoutMs()),
            coordinator -> coordinator.sharePartitionDeleteRequests(groupList)
        ).thenCompose(
            this::performShareGroupsDeletion
        ).exceptionally(exception -> handleOperationException(
            "delete-share-groups",
            groupList,
            exception,
            (error, __) -> {
                Map<String, Errors> errors = new HashMap<>();
                groupList.forEach(group -> errors.put(group, error));
                return errors;
            },
            log
        ));
    }

    private CompletableFuture<Map<String, Errors>> performShareGroupsDeletion(
        Map<String, Map.Entry<DeleteShareGroupStateParameters, Errors>> deleteRequests
    ) {
        List<CompletableFuture<Map.Entry<String, DeleteShareGroupStateResult>>> futures = new ArrayList<>(deleteRequests.size());
        Map<String, Errors> errorMap = new HashMap<>();
        deleteRequests.forEach((groupId, valPair) -> {
            if (valPair.getValue() == Errors.NONE) {
                futures.add(deleteShareGroup(valPair.getKey()));
            } else {
                errorMap.put(groupId, valPair.getValue());
            }
        });

        return persisterDeleteToGroupIdErrorMap(futures)
            .thenApply(respErrMap -> {
                errorMap.putAll(respErrMap);
                return errorMap;
            });
    }

    private CompletableFuture<Map.Entry<String, DeleteShareGroupStateResult>> deleteShareGroup(
        DeleteShareGroupStateParameters deleteRequest
    ) {
        String groupId = deleteRequest.groupTopicPartitionData().groupId();
        return persister.deleteState(deleteRequest)
            .thenCompose(result -> CompletableFuture.completedFuture(Map.entry(groupId, result)))
            .exceptionally(exception -> {
                // In case the deleteState call fails,
                // we should construct the appropriate response here
                // so that the subsequent callbacks don't see runtime exceptions.
                log.error("Unable to delete share group partition(s) - {} using request {}", groupId, deleteRequest, exception);
                List<TopicData<PartitionErrorData>> respTopicData = deleteRequest.groupTopicPartitionData().topicsData().stream()
                    .map(reqTopicData -> new TopicData<>(
                        reqTopicData.topicId(),
                        reqTopicData.partitions().stream()
                            .map(reqPartData -> {
                                Errors err = Errors.forException(exception);
                                return PartitionFactory.newPartitionErrorData(reqPartData.partition(), err.code(), err.message());
                            })
                            .toList()
                    ))
                    .toList();

                return Map.entry(groupId, new DeleteShareGroupStateResult.Builder()
                    .setTopicsData(respTopicData)
                    .build()
                );
            });
    }

    private CompletableFuture<Map<String, Errors>> persisterDeleteToGroupIdErrorMap(
        List<CompletableFuture<Map.Entry<String, DeleteShareGroupStateResult>>> futures
    ) {
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[]{})).thenCompose(v -> {
            Map<String, Errors> groupIds = new HashMap<>();
            for (CompletableFuture<Map.Entry<String, DeleteShareGroupStateResult>> future : futures) {
                Map.Entry<String, DeleteShareGroupStateResult> entry = future.getNow(null);  // safe as within allOff
                groupIds.putIfAbsent(entry.getKey(), Errors.NONE);
                for (TopicData<PartitionErrorData> topicData : entry.getValue().topicsData()) {
                    Optional<PartitionErrorData> errItem = topicData.partitions().stream()
                        .filter(errData -> errData.errorCode() != Errors.NONE.code())
                        .findAny();

                    errItem.ifPresent(val -> {
                        log.error("Received error while deleting share group {} - {}", entry.getKey(), val);
                        groupIds.put(entry.getKey(), Errors.forCode(val.errorCode()));
                    });
                }
            }

            return CompletableFuture.completedFuture(groupIds);
        });
    }

    /**
     * See {@link GroupCoordinator#fetchOffsets(AuthorizableRequestContext, OffsetFetchRequestData.OffsetFetchRequestGroup, boolean)}.
     */
    @Override
    public CompletableFuture<OffsetFetchResponseData.OffsetFetchResponseGroup> fetchOffsets(
        AuthorizableRequestContext context,
        OffsetFetchRequestData.OffsetFetchRequestGroup request,
        boolean requireStable
    ) {
        if (!isActive.get()) {
            return CompletableFuture.completedFuture(OffsetFetchResponse.groupError(
                request,
                Errors.COORDINATOR_NOT_AVAILABLE,
                context.requestVersion()
            ));
        }

        // For backwards compatibility, we support fetch commits for the empty group id.
        if (request.groupId() == null) {
            return CompletableFuture.completedFuture(OffsetFetchResponse.groupError(
                request,
                Errors.INVALID_GROUP_ID,
                context.requestVersion()
            ));
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
                    List.of(),
                    coordinator.fetchOffsets(request, Long.MAX_VALUE)
                )
            ).exceptionally(exception -> handleOffsetFetchException(
                "fetch-offsets",
                context,
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
     * See {@link GroupCoordinator#fetchAllOffsets(AuthorizableRequestContext, OffsetFetchRequestData.OffsetFetchRequestGroup, boolean)}.
     */
    @Override
    public CompletableFuture<OffsetFetchResponseData.OffsetFetchResponseGroup> fetchAllOffsets(
        AuthorizableRequestContext context,
        OffsetFetchRequestData.OffsetFetchRequestGroup request,
        boolean requireStable
    ) {
        if (!isActive.get()) {
            return CompletableFuture.completedFuture(OffsetFetchResponse.groupError(
                request,
                Errors.COORDINATOR_NOT_AVAILABLE,
                context.requestVersion()
            ));
        }

        // For backwards compatibility, we support fetch commits for the empty group id.
        if (request.groupId() == null) {
            return CompletableFuture.completedFuture(OffsetFetchResponse.groupError(
                request,
                Errors.INVALID_GROUP_ID,
                context.requestVersion()
            ));
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
                    List.of(),
                    coordinator.fetchAllOffsets(request, Long.MAX_VALUE)
                )
            ).exceptionally(exception -> handleOffsetFetchException(
                "fetch-all-offsets",
                context,
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
     * See {@link GroupCoordinator#describeShareGroupOffsets(AuthorizableRequestContext, DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup)}.
     */
    @Override
    public CompletableFuture<DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup> describeShareGroupOffsets(
        AuthorizableRequestContext context,
        DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup requestData
    ) {
        if (!isActive.get()) {
            return CompletableFuture.completedFuture(
                DescribeShareGroupOffsetsRequest.getErrorDescribedGroup(requestData.groupId(), Errors.COORDINATOR_NOT_AVAILABLE));
        }

        if (metadataImage == null) {
            return CompletableFuture.completedFuture(
                DescribeShareGroupOffsetsRequest.getErrorDescribedGroup(requestData.groupId(), Errors.COORDINATOR_NOT_AVAILABLE));
        }

        Map<Uuid, String> requestTopicIdToNameMapping = new HashMap<>();
        List<ReadShareGroupStateSummaryRequestData.ReadStateSummaryData> readStateSummaryData = new ArrayList<>(requestData.topics().size());
        List<DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseTopic> describeShareGroupOffsetsResponseTopicList = new ArrayList<>(requestData.topics().size());
        requestData.topics().forEach(topic -> {
            Optional<CoordinatorMetadataImage.TopicMetadata> topicMetadataOpt = metadataImage.topicMetadata(topic.topicName());
            if (topicMetadataOpt.isPresent()) {
                var topicId = topicMetadataOpt.get().id();
                requestTopicIdToNameMapping.put(topicId, topic.topicName());
                readStateSummaryData.add(new ReadShareGroupStateSummaryRequestData.ReadStateSummaryData()
                    .setTopicId(topicId)
                    .setPartitions(
                        topic.partitions().stream().map(
                            partitionIndex -> new ReadShareGroupStateSummaryRequestData.PartitionData().setPartition(partitionIndex)
                        ).toList()
                    ));
            } else {
                // If the topic does not exist, the start offset is returned as -1 (uninitialized offset).
                // This is consistent with OffsetFetch for situations in which there is no offset information to fetch.
                // It's treated as absence of data, rather than an error, unlike TOPIC_AUTHORIZATION_ERROR for example.
                describeShareGroupOffsetsResponseTopicList.add(new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseTopic()
                    .setTopicName(topic.topicName())
                    .setTopicId(Uuid.ZERO_UUID)
                    .setPartitions(topic.partitions().stream().map(
                        partition -> new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponsePartition()
                            .setPartitionIndex(partition)
                            .setStartOffset(PartitionFactory.UNINITIALIZED_START_OFFSET)
                    ).toList()));
            }
        });

        // If the request for the persister is empty, just complete the operation right away.
        if (readStateSummaryData.isEmpty()) {
            return CompletableFuture.completedFuture(
                new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup()
                    .setGroupId(requestData.groupId())
                    .setTopics(describeShareGroupOffsetsResponseTopicList));
        }

        ReadShareGroupStateSummaryRequestData readSummaryRequestData = new ReadShareGroupStateSummaryRequestData()
            .setGroupId(requestData.groupId())
            .setTopics(readStateSummaryData);

        return readShareGroupStateSummary(readSummaryRequestData, requestTopicIdToNameMapping, describeShareGroupOffsetsResponseTopicList);
    }

    /**
     * See {@link GroupCoordinator#describeShareGroupAllOffsets(AuthorizableRequestContext, DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup)}.
     */
    @Override
    public CompletableFuture<DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup> describeShareGroupAllOffsets(
        AuthorizableRequestContext context,
        DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup requestData
    ) {
        if (!isActive.get()) {
            return CompletableFuture.completedFuture(
                DescribeShareGroupOffsetsRequest.getErrorDescribedGroup(requestData.groupId(), Errors.COORDINATOR_NOT_AVAILABLE));
        }

        if (metadataImage == null) {
            return CompletableFuture.completedFuture(
                DescribeShareGroupOffsetsRequest.getErrorDescribedGroup(requestData.groupId(), Errors.COORDINATOR_NOT_AVAILABLE));
        }

        return runtime.scheduleReadOperation(
            "share-group-initialized-partitions",
            topicPartitionFor(requestData.groupId()),
            (coordinator, offset) -> coordinator.initializedShareGroupPartitions(requestData.groupId())
        ).thenCompose(topicPartitionMap -> {
            Map<Uuid, String> requestTopicIdToNameMapping = new HashMap<>();
            List<DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseTopic> describeShareGroupOffsetsResponseTopicList = new ArrayList<>(topicPartitionMap.size());
            ReadShareGroupStateSummaryRequestData readSummaryRequestData = new ReadShareGroupStateSummaryRequestData()
                .setGroupId(requestData.groupId());
            topicPartitionMap.forEach((topicId, partitionSet) -> {
                metadataImage.topicMetadata(topicId).ifPresent(topicMetadata -> {
                    requestTopicIdToNameMapping.put(topicId, topicMetadata.name());
                    readSummaryRequestData.topics().add(new ReadShareGroupStateSummaryRequestData.ReadStateSummaryData()
                        .setTopicId(topicId)
                        .setPartitions(
                            partitionSet.stream().map(
                                partitionIndex -> new ReadShareGroupStateSummaryRequestData.PartitionData().setPartition(partitionIndex)
                            ).toList()
                        ));
                });
            });
            return readShareGroupStateSummary(readSummaryRequestData, requestTopicIdToNameMapping, describeShareGroupOffsetsResponseTopicList);
        });
    }

    private CompletableFuture<DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup> readShareGroupStateSummary(
        ReadShareGroupStateSummaryRequestData readSummaryRequestData,
        Map<Uuid, String> requestTopicIdToNameMapping,
        List<DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseTopic> describeShareGroupOffsetsResponseTopicList
    ) {
        // If the request for the persister is empty, just complete the operation right away.
        if (readSummaryRequestData.topics().isEmpty()) {
            return CompletableFuture.completedFuture(
                new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup()
                    .setGroupId(readSummaryRequestData.groupId())
                    .setTopics(describeShareGroupOffsetsResponseTopicList));
        }

        CompletableFuture<DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup> future = new CompletableFuture<>();
        persister.readSummary(ReadShareGroupStateSummaryParameters.from(readSummaryRequestData))
            .whenComplete((result, error) -> {
                if (error != null) {
                    log.error("Failed to read summary of the share partition", error);
                    future.completeExceptionally(error);
                    return;
                }
                if (result == null || result.topicsData() == null) {
                    log.error("Result is null for the read state summary");
                    future.completeExceptionally(new IllegalStateException("Result is null for the read state summary"));
                    return;
                }

                // Now compute lag for each partition and build the final response.
                computeShareGroupLagAndBuildResponse(
                    result,
                    requestTopicIdToNameMapping,
                    describeShareGroupOffsetsResponseTopicList,
                    future,
                    readSummaryRequestData.groupId()
                );
            });
        return future;
    }

    private void computeShareGroupLagAndBuildResponse(
        ReadShareGroupStateSummaryResult readSummaryResult,
        Map<Uuid, String> requestTopicIdToNameMapping,
        List<DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseTopic> describeShareGroupOffsetsResponseTopicList,
        CompletableFuture<DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup> responseFuture,
        String groupId
    ) {
        // This set keeps track of the partitions for which lag computation is needed.
        Set<TopicPartition> partitionsToComputeLag = new HashSet<>();

        readSummaryResult.topicsData().forEach(topicData -> {
            topicData.partitions().forEach(partitionData -> {
                if (shouldComputeSharePartitionLag(partitionData)) {
                    // If the readSummaryResult is successful for a partition, we need to compute lag.
                    partitionsToComputeLag.add(new TopicPartition(requestTopicIdToNameMapping.get(topicData.topicId()), partitionData.partition()));
                }
            });
        });

        // Fetch latest offsets for all partitions that need lag computation.
        Map<TopicPartition, CompletableFuture<Long>> partitionLatestOffsets = partitionsToComputeLag.isEmpty() ? Map.of() :
                partitionMetadataClient.listLatestOffsets(partitionsToComputeLag);

        // Final response object to be built. It will include lag information computed from partitionMetadataClient.
        DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup responseGroup =
            new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup()
                .setGroupId(groupId);

        // List of response topics to be set in the response group.
        List<DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseTopic> responseTopics = new ArrayList<>();

        CompletableFuture.allOf(partitionLatestOffsets.values().toArray(new CompletableFuture<?>[0]))
            .whenComplete((result, error) -> {
                // The error variable will not be null when one or more of the partitionLatestOffsets futures get completed exceptionally.
                // If that is the case, then the same exception would be caught in the try catch executed below when .join() is called.
                // Thus, we do not need to check error != null here.
                readSummaryResult.topicsData().forEach(topicData -> {
                    // Build response for each topic.
                    DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseTopic topic =
                        new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseTopic()
                            .setTopicId(topicData.topicId())
                            .setTopicName(requestTopicIdToNameMapping.get(topicData.topicId()));

                    // Build response for each partition within the topic.
                    List<DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponsePartition> partitionResponses = new ArrayList<>();

                    topicData.partitions().forEach(partitionData -> {
                        TopicPartition tp = new TopicPartition(requestTopicIdToNameMapping.get(topicData.topicId()), partitionData.partition());
                        // For the partitions where lag computation is not needed, a partitionResponse is built directly.
                        // The lag is set to -1 (uninitialized lag) in these cases. If the persister returned an error for a
                        // partition, the startOffset is set to -1 (uninitialized offset) and the leaderEpoch is set to 0
                        // (default epoch). This is consistent with OffsetFetch for situations in which there is no offset
                        // information to fetch. It's treated as absence of data, rather than an error
                        if (!shouldComputeSharePartitionLag(partitionData)) {
                            partitionResponses.add(new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponsePartition()
                                .setPartitionIndex(partitionData.partition())
                                .setStartOffset(partitionData.errorCode() == Errors.NONE.code() ? partitionData.startOffset() : PartitionFactory.UNINITIALIZED_START_OFFSET)
                                .setLeaderEpoch(partitionData.errorCode() == Errors.NONE.code() ? partitionData.leaderEpoch() : PartitionFactory.DEFAULT_LEADER_EPOCH)
                                .setLag(PartitionFactory.UNINITIALIZED_LAG));
                        } else {
                            try {
                                // This code is reached when allOf above is complete, which happens when all the
                                // individual futures are complete. Thus, the call to join() here is safe.
                                long partitionLatestOffset = partitionLatestOffsets.get(tp).join();
                                // Compute lag as (partition end offset - startOffset - deliveryCompleteCount).
                                // Note, partition end offset, which is retrieved from partitionMetadataClient, is the offset of
                                // the next message to be produced, not the last message offset. Thus, the formula for lag computation
                                // does not need a +1 adjustment.
                                long lag = partitionLatestOffset - partitionData.startOffset() - partitionData.deliveryCompleteCount();
                                partitionResponses.add(new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponsePartition()
                                    .setPartitionIndex(partitionData.partition())
                                    .setStartOffset(partitionData.startOffset())
                                    .setLeaderEpoch(partitionData.leaderEpoch())
                                    .setLag(lag));
                            } catch (CompletionException e) {
                                // If fetching latest offset for a partition failed, return the error in the response for that partition.
                                partitionResponses.add(new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponsePartition()
                                    .setPartitionIndex(partitionData.partition())
                                    .setErrorCode(Errors.forException(e.getCause()).code())
                                    .setErrorMessage(e.getCause().getMessage()));
                            }
                        }
                    });
                    topic.setPartitions(partitionResponses);
                    responseTopics.add(topic);
                });

                // Add topics which did not exist in the metadata image and were handled earlier.
                responseTopics.addAll(describeShareGroupOffsetsResponseTopicList);
                // Set topics in the response group.
                responseGroup.setTopics(responseTopics);
                // Complete the future with the built response.
                responseFuture.complete(responseGroup);
            });
    }

    private boolean shouldComputeSharePartitionLag(PartitionStateSummaryData partitionData) {
        // The share partition lag would be computed for a share partition ony if -
        // 1. The read summary result for the partition is successful.
        // 3. The start offset is initialized.
        // 4. The delivery complete count is initialized.
        return partitionData.errorCode() == Errors.NONE.code() &&
            partitionData.startOffset() != PartitionFactory.UNINITIALIZED_START_OFFSET &&
            partitionData.deliveryCompleteCount() != PartitionFactory.UNINITIALIZED_DELIVERY_COMPLETE_COUNT;
    }

    /**
     * See {@link GroupCoordinator#deleteShareGroupOffsets(AuthorizableRequestContext, DeleteShareGroupOffsetsRequestData)}.
     */
    @Override
    public CompletableFuture<DeleteShareGroupOffsetsResponseData> deleteShareGroupOffsets(
        AuthorizableRequestContext context,
        DeleteShareGroupOffsetsRequestData requestData
    ) {
        if (!isActive.get() || metadataImage == null) {
            return CompletableFuture.completedFuture(DeleteShareGroupOffsetsRequest.getErrorDeleteResponseData(Errors.COORDINATOR_NOT_AVAILABLE));
        }

        String groupId = requestData.groupId();

        if (!isGroupIdNotEmpty(groupId)) {
            return CompletableFuture.completedFuture(DeleteShareGroupOffsetsRequest.getErrorDeleteResponseData(Errors.INVALID_GROUP_ID));
        }

        if (requestData.topics() == null) {
            return CompletableFuture.completedFuture(new DeleteShareGroupOffsetsResponseData()
            );
        }

        return runtime.scheduleWriteOperation(
            "initiate-delete-share-group-offsets",
            topicPartitionFor(groupId),
            Duration.ofMillis(config.offsetCommitTimeoutMs()),
            coordinator -> coordinator.initiateDeleteShareGroupOffsets(groupId, requestData)
        ).thenCompose(resultHolder -> deleteShareGroupOffsetsState(groupId, resultHolder)
        ).exceptionally(exception -> handleOperationException(
            "initiate-delete-share-group-offsets",
            groupId,
            exception,
            DeleteShareGroupOffsetsRequest::getErrorDeleteResponseData,
            log
        ));
    }

    private CompletableFuture<DeleteShareGroupOffsetsResponseData> deleteShareGroupOffsetsState(
        String groupId,
        GroupCoordinatorShard.DeleteShareGroupOffsetsResultHolder resultHolder
    ) {
        if (resultHolder == null) {
            log.error("Failed to retrieve deleteState request parameters from group coordinator for the group {}", groupId);
            return CompletableFuture.completedFuture(
                DeleteShareGroupOffsetsRequest.getErrorDeleteResponseData(Errors.UNKNOWN_SERVER_ERROR)
            );
        }

        if (resultHolder.topLevelErrorCode() != Errors.NONE.code()) {
            return CompletableFuture.completedFuture(
                DeleteShareGroupOffsetsRequest.getErrorDeleteResponseData(
                    resultHolder.topLevelErrorCode(),
                    resultHolder.topLevelErrorMessage()
                )
            );
        }

        List<DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic> errorTopicResponseList =
            resultHolder.errorTopicResponseList() == null ? new ArrayList<>() : new ArrayList<>(resultHolder.errorTopicResponseList());

        if (resultHolder.deleteStateRequestParameters() == null) {
            return CompletableFuture.completedFuture(
                new DeleteShareGroupOffsetsResponseData()
                    .setResponses(errorTopicResponseList)
            );
        }

        return persister.deleteState(resultHolder.deleteStateRequestParameters())
            .thenCompose(result -> handleDeleteShareGroupOffsetStateResult(groupId, result, errorTopicResponseList))
            .exceptionally(throwable -> {
                log.error("Failed to delete share group state due to: {}", throwable.getMessage(), throwable);
                return DeleteShareGroupOffsetsRequest.getErrorDeleteResponseData(Errors.forException(throwable));
            });
    }

    private CompletableFuture<DeleteShareGroupOffsetsResponseData> handleDeleteShareGroupOffsetStateResult(
        String groupId,
        DeleteShareGroupStateResult result,
        List<DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic> errorTopicResponses
    ) {
        if (result == null || result.topicsData() == null) {
            log.error("Result is null for the delete share group state");
            Exception exception = new IllegalStateException("Result is null for the delete share group state");
            return CompletableFuture.completedFuture(
                DeleteShareGroupOffsetsRequest.getErrorDeleteResponseData(Errors.forException(exception))
            );
        }
        Map<Uuid, String> successTopics = new HashMap<>();
        result.topicsData().forEach(topicData -> {
            Optional<PartitionErrorData> errItem = topicData.partitions().stream()
                .filter(errData -> errData.errorCode() != Errors.NONE.code())
                .findAny();

            String topicName = metadataImage.topicMetadata(topicData.topicId()).map(CoordinatorMetadataImage.TopicMetadata::name).orElse(null);

            if (errItem.isPresent()) {
                errorTopicResponses.add(
                    new DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic()
                        .setTopicId(topicData.topicId())
                        .setTopicName(topicName)
                        .setErrorMessage(Errors.forCode(errItem.get().errorCode()).message())
                        .setErrorCode(errItem.get().errorCode())
                );
            } else {
                successTopics.put(
                    topicData.topicId(),
                    topicName
                );
            }
        });

        // If there are no topics for which persister delete state request succeeded, then we can return directly from here
        if (successTopics.isEmpty()) {
            return CompletableFuture.completedFuture(
                new DeleteShareGroupOffsetsResponseData()
                    .setResponses(errorTopicResponses)
            );
        }

        return completeDeleteShareGroupOffsets(groupId, successTopics, errorTopicResponses);
    }

    private CompletableFuture<DeleteShareGroupOffsetsResponseData> completeDeleteShareGroupOffsets(
        String groupId,
        Map<Uuid, String> successTopics,
        List<DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic> errorTopicResponses
    ) {
        return runtime.scheduleWriteOperation(
            "complete-delete-share-group-offsets",
            topicPartitionFor(groupId),
            Duration.ofMillis(config.offsetCommitTimeoutMs()),
            coordinator -> coordinator.completeDeleteShareGroupOffsets(groupId, successTopics, errorTopicResponses)
        ).exceptionally(exception -> handleOperationException(
            "complete-delete-share-group-offsets",
            groupId,
            exception,
            (error, __) -> DeleteShareGroupOffsetsRequest.getErrorDeleteResponseData(error),
            log
        ));
    }

    /**
     * See {@link GroupCoordinator#commitOffsets(AuthorizableRequestContext, OffsetCommitRequestData, BufferSupplier)}.
     */
    @Override
    public CompletableFuture<OffsetCommitResponseData> commitOffsets(
        AuthorizableRequestContext context,
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
     * See {@link GroupCoordinator#commitTransactionalOffsets(AuthorizableRequestContext, TxnOffsetCommitRequestData, BufferSupplier)}.
     */
    @Override
    public CompletableFuture<TxnOffsetCommitResponseData> commitTransactionalOffsets(
        AuthorizableRequestContext context,
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
            context.requestVersion()
        ).exceptionally(exception -> handleOperationException(
            "txn-commit-offset",
            request,
            exception,
            (error, __) -> TxnOffsetCommitRequest.getErrorResponse(request, error),
            log
        ));
    }

    /**
     * See {@link GroupCoordinator#deleteOffsets(AuthorizableRequestContext, OffsetDeleteRequestData, BufferSupplier)}.
     */
    @Override
    public CompletableFuture<OffsetDeleteResponseData> deleteOffsets(
        AuthorizableRequestContext context,
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
            ).toArray(new CompletableFuture<?>[0])
        ).get();

        // At this point the metadata will not have been updated
        // with the deleted topics. However, we must guard against it.
        if (metadataImage == null || metadataImage.equals(CoordinatorMetadataImage.EMPTY)) {
            return;
        }

        Set<Uuid> topicIds = topicPartitions.stream()
            .filter(tp -> metadataImage.topicMetadata(tp.topic()).isPresent())
            .map(tp -> metadataImage.topicMetadata(tp.topic()).get().id())
            .collect(Collectors.toSet());

        if (topicIds.isEmpty()) {
            return;
        }

        CompletableFuture.allOf(
            FutureUtils.mapExceptionally(
                runtime.scheduleWriteAllOperation(
                    "maybe-cleanup-share-group-state",
                    Duration.ofMillis(config.offsetCommitTimeoutMs()),
                    coordinator -> coordinator.maybeCleanupShareGroupState(topicIds)
                ),
                exception -> {
                    log.error("Unable to cleanup state for the deleted topics {}", topicIds, exception);
                    return null;
                }
            ).toArray(new CompletableFuture<?>[0])
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
     * See {@link GroupCoordinator#onNewMetadataImage(CoordinatorMetadataImage, CoordinatorMetadataDelta)}.
     */
    @Override
    public void onNewMetadataImage(
        CoordinatorMetadataImage newImage,
        CoordinatorMetadataDelta delta
    ) {
        throwIfNotActive();
        metadataImage = newImage;
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
     * @param context           The request context.
     * @param request           The OffsetFetchRequestGroup request.
     * @param exception         The exception to handle.
     * @return The OffsetFetchRequestGroup response.
     */
    private OffsetFetchResponseData.OffsetFetchResponseGroup handleOffsetFetchException(
        String operationName,
        AuthorizableRequestContext context,
        OffsetFetchRequestData.OffsetFetchRequestGroup request,
        Throwable exception
    ) {
        ApiError apiError = ApiError.fromThrowable(exception);

        return switch (apiError.error()) {
            case UNKNOWN_TOPIC_OR_PARTITION, NOT_ENOUGH_REPLICAS, REQUEST_TIMED_OUT ->
                    // Remap REQUEST_TIMED_OUT to NOT_COORDINATOR, since consumers on versions prior
                    // to 3.9 do not expect the error and won't retry the request. NOT_COORDINATOR
                    // additionally triggers coordinator re-lookup, which is necessary if the client is
                    // talking to a zombie coordinator.
                    //
                    // While handleOperationException does remap UNKNOWN_TOPIC_OR_PARTITION,
                    // NOT_ENOUGH_REPLICAS and REQUEST_TIMED_OUT to COORDINATOR_NOT_AVAILABLE,
                    // COORDINATOR_NOT_AVAILABLE is also not handled by consumers on versions prior to
                    // 3.9.
                    OffsetFetchResponse.groupError(
                            request,
                            Errors.NOT_COORDINATOR,
                            context.requestVersion()
                    );
            default -> handleOperationException(
                    operationName,
                    request,
                    exception,
                    (error, __) -> OffsetFetchResponse.groupError(
                        request,
                        error,
                        context.requestVersion()
                    ),
                    log
            );
        };
    }

    private static void requireNonNull(Object obj, String msg) {
        if (obj == null) {
            throw new IllegalArgumentException(msg);
        }
    }
}
