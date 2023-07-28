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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.CoordinatorLoadInProgressException;
import org.apache.kafka.common.errors.InvalidFetchSizeException;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.errors.RecordBatchTooLargeException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.Topic;
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
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.runtime.CoordinatorShardBuilderSupplier;
import org.apache.kafka.coordinator.group.runtime.CoordinatorEventProcessor;
import org.apache.kafka.coordinator.group.runtime.CoordinatorLoader;
import org.apache.kafka.coordinator.group.runtime.CoordinatorRuntime;
import org.apache.kafka.coordinator.group.runtime.MultiThreadedEventProcessor;
import org.apache.kafka.coordinator.group.runtime.PartitionWriter;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.server.record.BrokerCompressionType;
import org.apache.kafka.server.util.FutureUtils;
import org.apache.kafka.server.util.timer.Timer;
import org.slf4j.Logger;

import java.util.List;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntSupplier;

/**
 * The group coordinator service.
 */
public class GroupCoordinatorService implements GroupCoordinator {

    public static class Builder {
        private final int nodeId;
        private final GroupCoordinatorConfig config;
        private PartitionWriter<Record> writer;
        private CoordinatorLoader<Record> loader;
        private Time time;
        private Timer timer;

        public Builder(
            int nodeId,
            GroupCoordinatorConfig config
        ) {
            this.nodeId = nodeId;
            this.config = config;
        }

        public Builder withWriter(PartitionWriter<Record> writer) {
            this.writer = writer;
            return this;
        }

        public Builder withLoader(CoordinatorLoader<Record> loader) {
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

            String logPrefix = String.format("GroupCoordinator id=%d", nodeId);
            LogContext logContext = new LogContext(String.format("[%s] ", logPrefix));

            CoordinatorShardBuilderSupplier<GroupCoordinatorShard, Record> supplier = () ->
                new GroupCoordinatorShard.Builder(config);

            CoordinatorEventProcessor processor = new MultiThreadedEventProcessor(
                logContext,
                "group-coordinator-event-processor-",
                config.numThreads
            );

            CoordinatorRuntime<GroupCoordinatorShard, Record> runtime =
                new CoordinatorRuntime.Builder<GroupCoordinatorShard, Record>()
                    .withTime(time)
                    .withTimer(timer)
                    .withLogPrefix(logPrefix)
                    .withLogContext(logContext)
                    .withEventProcessor(processor)
                    .withPartitionWriter(writer)
                    .withLoader(loader)
                    .withCoordinatorShardBuilderSupplier(supplier)
                    .withTime(time)
                    .build();

            return new GroupCoordinatorService(
                logContext,
                config,
                runtime
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
    private final CoordinatorRuntime<GroupCoordinatorShard, Record> runtime;

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
     * @param logContext
     * @param config
     * @param runtime
     */
    GroupCoordinatorService(
        LogContext logContext,
        GroupCoordinatorConfig config,
        CoordinatorRuntime<GroupCoordinatorShard, Record> runtime
    ) {
        this.log = logContext.logger(CoordinatorLoader.class);
        this.config = config;
        this.runtime = runtime;
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
     * See {@link GroupCoordinator#consumerGroupHeartbeat(RequestContext, ConsumerGroupHeartbeatRequestData)}.
     */
    @Override
    public CompletableFuture<ConsumerGroupHeartbeatResponseData> consumerGroupHeartbeat(
        RequestContext context,
        ConsumerGroupHeartbeatRequestData request
    ) {
        if (!isActive.get()) {
            return FutureUtils.failedFuture(Errors.COORDINATOR_NOT_AVAILABLE.exception());
        }

        return runtime.scheduleWriteOperation(
            "consumer-group-heartbeat",
            topicPartitionFor(request.groupId()),
            coordinator -> coordinator.consumerGroupHeartbeat(context, request)
        ).exceptionally(exception -> {
            if (exception instanceof UnknownTopicOrPartitionException ||
                exception instanceof NotEnoughReplicasException) {
                return new ConsumerGroupHeartbeatResponseData()
                    .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code());
            }

            if (exception instanceof NotLeaderOrFollowerException ||
                exception instanceof KafkaStorageException) {
                return new ConsumerGroupHeartbeatResponseData()
                    .setErrorCode(Errors.NOT_COORDINATOR.code());
            }

            if (exception instanceof RecordTooLargeException ||
                exception instanceof RecordBatchTooLargeException ||
                exception instanceof InvalidFetchSizeException) {
                return new ConsumerGroupHeartbeatResponseData()
                    .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code());
            }

            return new ConsumerGroupHeartbeatResponseData()
                .setErrorCode(Errors.forException(exception).code())
                .setErrorMessage(exception.getMessage());
        });
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
            return FutureUtils.failedFuture(Errors.COORDINATOR_NOT_AVAILABLE.exception());
        }

        CompletableFuture<JoinGroupResponseData> responseFuture = new CompletableFuture<>();

        if (!isGroupIdNotEmpty(request.groupId())) {
            responseFuture.complete(new JoinGroupResponseData()
                .setMemberId(request.memberId())
                .setErrorCode(Errors.INVALID_GROUP_ID.code()));

            return responseFuture;
        }

        runtime.scheduleWriteOperation("generic-group-join",
            topicPartitionFor(request.groupId()),
            coordinator -> coordinator.genericGroupJoin(context, request, responseFuture)
        ).exceptionally(exception -> {
            if (!(exception instanceof KafkaException)) {
                log.error("JoinGroup request {} hit an unexpected exception: {}",
                    request, exception.getMessage());
            }
            
            if (!responseFuture.isDone()) {
                responseFuture.complete(new JoinGroupResponseData()
                    .setErrorCode(Errors.forException(exception).code()));
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
            return FutureUtils.failedFuture(Errors.COORDINATOR_NOT_AVAILABLE.exception());
        }

        if (!isGroupIdNotEmpty(request.groupId())) {
            return CompletableFuture.completedFuture(new SyncGroupResponseData()
                .setErrorCode(Errors.INVALID_GROUP_ID.code()));
        }

        CompletableFuture<SyncGroupResponseData> responseFuture = new CompletableFuture<>();

        runtime.scheduleWriteOperation("generic-group-sync",
            topicPartitionFor(request.groupId()),
            coordinator -> coordinator.genericGroupSync(context, request, responseFuture)
        ).exceptionally(exception -> {
            if (!(exception instanceof KafkaException)) {
                log.error("SyncGroup request {} hit an unexpected exception: {}",
                    request, exception.getMessage());
            }

            if (!responseFuture.isDone()) {
                responseFuture.complete(new SyncGroupResponseData()
                    .setErrorCode(Errors.forException(exception).code()));
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
            return FutureUtils.failedFuture(Errors.COORDINATOR_NOT_AVAILABLE.exception());
        }

        if (!isGroupIdNotEmpty(request.groupId())) {
            return CompletableFuture.completedFuture(new HeartbeatResponseData()
                .setErrorCode(Errors.INVALID_GROUP_ID.code()));
        }

        // Using a read operation is okay here as we ignore the last committed offset in the snapshot registry.
        // This means we will read whatever is in the latest snapshot, which is how the old coordinator behaves.
        return runtime.scheduleReadOperation("generic-group-heartbeat",
            topicPartitionFor(request.groupId()),
            (coordinator, __) -> coordinator.genericGroupHeartbeat(context, request)
        ).exceptionally(exception -> {
            if (!(exception instanceof KafkaException)) {
                log.error("Heartbeat request {} hit an unexpected exception: {}",
                    request, exception.getMessage());
            }

            if (exception instanceof CoordinatorLoadInProgressException) {
                // The group is still loading, so blindly respond
                return new HeartbeatResponseData()
                    .setErrorCode(Errors.NONE.code());
            }

            return new HeartbeatResponseData()
                .setErrorCode(Errors.forException(exception).code());
        });
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
            return FutureUtils.failedFuture(Errors.COORDINATOR_NOT_AVAILABLE.exception());
        }

        return FutureUtils.failedFuture(Errors.UNSUPPORTED_VERSION.exception(
            "This API is not implemented yet."
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
            return FutureUtils.failedFuture(Errors.COORDINATOR_NOT_AVAILABLE.exception());
        }

        return FutureUtils.failedFuture(Errors.UNSUPPORTED_VERSION.exception(
            "This API is not implemented yet."
        ));
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
            return FutureUtils.failedFuture(Errors.COORDINATOR_NOT_AVAILABLE.exception());
        }

        return FutureUtils.failedFuture(Errors.UNSUPPORTED_VERSION.exception(
            "This API is not implemented yet."
        ));
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
            return FutureUtils.failedFuture(Errors.COORDINATOR_NOT_AVAILABLE.exception());
        }

        return FutureUtils.failedFuture(Errors.UNSUPPORTED_VERSION.exception(
            "This API is not implemented yet."
        ));
    }

    /**
     * See {@link GroupCoordinator#fetchOffsets(RequestContext, String, List, boolean)}.
     */
    @Override
    public CompletableFuture<List<OffsetFetchResponseData.OffsetFetchResponseTopics>> fetchOffsets(
        RequestContext context,
        String groupId,
        List<OffsetFetchRequestData.OffsetFetchRequestTopics> topics,
        boolean requireStable
    ) {
        if (!isActive.get()) {
            return FutureUtils.failedFuture(Errors.COORDINATOR_NOT_AVAILABLE.exception());
        }

        return FutureUtils.failedFuture(Errors.UNSUPPORTED_VERSION.exception(
            "This API is not implemented yet."
        ));
    }

    /**
     * See {@link GroupCoordinator#fetchAllOffsets(RequestContext, String, boolean)}.
     */
    @Override
    public CompletableFuture<List<OffsetFetchResponseData.OffsetFetchResponseTopics>> fetchAllOffsets(
        RequestContext context,
        String groupId,
        boolean requireStable
    ) {
        if (!isActive.get()) {
            return FutureUtils.failedFuture(Errors.COORDINATOR_NOT_AVAILABLE.exception());
        }

        return FutureUtils.failedFuture(Errors.UNSUPPORTED_VERSION.exception(
            "This API is not implemented yet."
        ));
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
            return FutureUtils.failedFuture(Errors.COORDINATOR_NOT_AVAILABLE.exception());
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
            coordinator -> coordinator.commitOffset(context, request)
        ).exceptionally(exception -> {
            if (exception instanceof UnknownTopicOrPartitionException ||
                exception instanceof NotEnoughReplicasException) {
                return OffsetCommitRequest.getErrorResponse(
                    request,
                    Errors.COORDINATOR_NOT_AVAILABLE
                );
            }

            if (exception instanceof NotLeaderOrFollowerException ||
                exception instanceof KafkaStorageException) {
                return OffsetCommitRequest.getErrorResponse(
                    request,
                    Errors.NOT_COORDINATOR
                );
            }

            if (exception instanceof RecordTooLargeException ||
                exception instanceof RecordBatchTooLargeException ||
                exception instanceof InvalidFetchSizeException) {
                return OffsetCommitRequest.getErrorResponse(
                    request,
                    Errors.INVALID_COMMIT_OFFSET_SIZE
                );
            }

            return OffsetCommitRequest.getErrorResponse(
                request,
                Errors.forException(exception)
            );
        });
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
            return FutureUtils.failedFuture(Errors.COORDINATOR_NOT_AVAILABLE.exception());
        }

        return FutureUtils.failedFuture(Errors.UNSUPPORTED_VERSION.exception(
            "This API is not implemented yet."
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
            return FutureUtils.failedFuture(Errors.COORDINATOR_NOT_AVAILABLE.exception());
        }

        return FutureUtils.failedFuture(Errors.UNSUPPORTED_VERSION.exception(
            "This API is not implemented yet."
        ));
    }

    /**
     * See {@link GroupCoordinator#onTransactionCompleted(long, Iterable, TransactionResult)}.
     */
    @Override
    public void onTransactionCompleted(
        long producerId,
        Iterable<TopicPartition> partitions,
        TransactionResult transactionResult
    ) {
        throwIfNotActive();
    }

    /**
     * See {@link GroupCoordinator#onPartitionsDeleted(List, BufferSupplier)}.
     */
    @Override
    public void onPartitionsDeleted(
        List<TopicPartition> topicPartitions,
        BufferSupplier bufferSupplier
    ) {
        throwIfNotActive();
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
        if (!groupMetadataPartitionLeaderEpoch.isPresent()) {
            throw new IllegalArgumentException("The leader epoch should always be provided in KRaft.");
        }
        runtime.scheduleUnloadOperation(
            new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupMetadataPartitionIndex),
            groupMetadataPartitionLeaderEpoch.getAsInt()
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
        properties.put(TopicConfig.SEGMENT_BYTES_CONFIG, String.valueOf(config.offsetsTopicSegmentBytes));
        return properties;
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
        log.info("Shutdown complete.");
    }

    private static boolean isGroupIdNotEmpty(String groupId) {
        return groupId != null && !groupId.isEmpty();
    }
}
