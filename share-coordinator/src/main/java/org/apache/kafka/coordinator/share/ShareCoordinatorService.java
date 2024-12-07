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

package org.apache.kafka.coordinator.share;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.ReadShareGroupStateRequestData;
import org.apache.kafka.common.message.ReadShareGroupStateResponseData;
import org.apache.kafka.common.message.WriteShareGroupStateRequestData;
import org.apache.kafka.common.message.WriteShareGroupStateResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ReadShareGroupStateResponse;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.WriteShareGroupStateResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.common.runtime.CoordinatorEventProcessor;
import org.apache.kafka.coordinator.common.runtime.CoordinatorLoader;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRuntime;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRuntimeMetrics;
import org.apache.kafka.coordinator.common.runtime.CoordinatorShardBuilderSupplier;
import org.apache.kafka.coordinator.common.runtime.MultiThreadedEventProcessor;
import org.apache.kafka.coordinator.common.runtime.PartitionWriter;
import org.apache.kafka.coordinator.share.metrics.ShareCoordinatorMetrics;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.server.config.ShareCoordinatorConfig;
import org.apache.kafka.server.record.BrokerCompressionType;
import org.apache.kafka.server.share.SharePartitionKey;
import org.apache.kafka.server.util.timer.Timer;

import org.slf4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;

import static org.apache.kafka.coordinator.common.runtime.CoordinatorOperationExceptionHelper.handleOperationException;

public class ShareCoordinatorService implements ShareCoordinator {
    private final ShareCoordinatorConfig config;
    private final Logger log;
    private final AtomicBoolean isActive = new AtomicBoolean(false);  // for controlling start and stop
    private final CoordinatorRuntime<ShareCoordinatorShard, CoordinatorRecord> runtime;
    private final ShareCoordinatorMetrics shareCoordinatorMetrics;
    private volatile int numPartitions = -1; // Number of partitions for __share_group_state. Provided when component is started.
    private final Time time;

    public static class Builder {
        private final int nodeId;
        private final ShareCoordinatorConfig config;
        private PartitionWriter writer;
        private CoordinatorLoader<CoordinatorRecord> loader;
        private Time time;
        private Timer timer;

        private ShareCoordinatorMetrics coordinatorMetrics;
        private CoordinatorRuntimeMetrics coordinatorRuntimeMetrics;

        public Builder(int nodeId, ShareCoordinatorConfig config) {
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

        public Builder withCoordinatorMetrics(ShareCoordinatorMetrics coordinatorMetrics) {
            this.coordinatorMetrics = coordinatorMetrics;
            return this;
        }

        public Builder withCoordinatorRuntimeMetrics(CoordinatorRuntimeMetrics coordinatorRuntimeMetrics) {
            this.coordinatorRuntimeMetrics = coordinatorRuntimeMetrics;
            return this;
        }

        public ShareCoordinatorService build() {
            if (config == null) {
                throw new IllegalArgumentException("Config must be set.");
            }
            if (writer == null) {
                throw new IllegalArgumentException("Writer must be set.");
            }
            if (loader == null) {
                throw new IllegalArgumentException("Loader must be set.");
            }
            if (time == null) {
                throw new IllegalArgumentException("Time must be set.");
            }
            if (timer == null) {
                throw new IllegalArgumentException("Timer must be set.");
            }
            if (coordinatorMetrics == null) {
                throw new IllegalArgumentException("Share Coordinator metrics must be set.");
            }
            if (coordinatorRuntimeMetrics == null) {
                throw new IllegalArgumentException("Coordinator runtime metrics must be set.");
            }

            String logPrefix = String.format("ShareCoordinator id=%d", nodeId);
            LogContext logContext = new LogContext(String.format("[%s] ", logPrefix));

            CoordinatorShardBuilderSupplier<ShareCoordinatorShard, CoordinatorRecord> supplier = () ->
                new ShareCoordinatorShard.Builder(config);

            CoordinatorEventProcessor processor = new MultiThreadedEventProcessor(
                logContext,
                "share-coordinator-event-processor-",
                config.shareCoordinatorNumThreads(),
                time,
                coordinatorRuntimeMetrics
            );

            CoordinatorRuntime<ShareCoordinatorShard, CoordinatorRecord> runtime =
                new CoordinatorRuntime.Builder<ShareCoordinatorShard, CoordinatorRecord>()
                    .withTime(time)
                    .withTimer(timer)
                    .withLogPrefix(logPrefix)
                    .withLogContext(logContext)
                    .withEventProcessor(processor)
                    .withPartitionWriter(writer)
                    .withLoader(loader)
                    .withCoordinatorShardBuilderSupplier(supplier)
                    .withTime(time)
                    .withDefaultWriteTimeOut(Duration.ofMillis(config.shareCoordinatorWriteTimeoutMs()))
                    .withCoordinatorRuntimeMetrics(coordinatorRuntimeMetrics)
                    .withCoordinatorMetrics(coordinatorMetrics)
                    .withSerializer(new ShareCoordinatorRecordSerde())
                    .withCompression(Compression.of(config.shareCoordinatorStateTopicCompressionType()).build())
                    .withAppendLingerMs(config.shareCoordinatorAppendLingerMs())
                    .withExecutorService(Executors.newSingleThreadExecutor())
                    .build();

            return new ShareCoordinatorService(
                logContext,
                config,
                runtime,
                coordinatorMetrics,
                time
            );
        }
    }

    public ShareCoordinatorService(
        LogContext logContext,
        ShareCoordinatorConfig config,
        CoordinatorRuntime<ShareCoordinatorShard, CoordinatorRecord> runtime,
        ShareCoordinatorMetrics shareCoordinatorMetrics,
        Time time) {
        this.log = logContext.logger(ShareCoordinatorService.class);
        this.config = config;
        this.runtime = runtime;
        this.shareCoordinatorMetrics = shareCoordinatorMetrics;
        this.time = time;
    }

    @Override
    public int partitionFor(SharePartitionKey key) {
        throwIfNotActive();
        // Call to asCoordinatorKey is necessary as we depend only on topicId (Uuid) and
        // not topic name. We do not want this calculation to distinguish between 2
        // SharePartitionKeys where everything except topic name is the same.
        return Utils.abs(key.asCoordinatorKey().hashCode()) % numPartitions;
    }

    @Override
    public Properties shareGroupStateTopicConfigs() {
        Properties properties = new Properties();
        properties.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE); // as defined in KIP-932
        properties.put(TopicConfig.COMPRESSION_TYPE_CONFIG, BrokerCompressionType.PRODUCER.name);
        properties.put(TopicConfig.SEGMENT_BYTES_CONFIG, config.shareCoordinatorStateTopicSegmentBytes());
        return properties;
    }

    /**
     * The share coordinator startup method will get invoked from BrokerMetadataPublisher.
     * At the time of writing, the publisher uses metadata cache to fetch the number of partitions
     * of the share state topic. In case this information is not available, the user provided
     * config will be used to fetch the value.
     * This is consistent with the group coordinator startup functionality.
     *
     * @param shareGroupTopicPartitionCount - supplier returning the number of partitions for __share_group_state topic
     */
    @Override
    public void startup(
        IntSupplier shareGroupTopicPartitionCount
    ) {
        if (!isActive.compareAndSet(false, true)) {
            log.warn("Share coordinator is already running.");
            return;
        }

        log.info("Starting up.");
        numPartitions = shareGroupTopicPartitionCount.getAsInt();
        log.info("Startup complete.");
    }

    @Override
    public void shutdown() {
        if (!isActive.compareAndSet(true, false)) {
            log.warn("Share coordinator is already shutting down.");
            return;
        }

        log.info("Shutting down.");
        Utils.closeQuietly(runtime, "coordinator runtime");
        Utils.closeQuietly(shareCoordinatorMetrics, "share coordinator metrics");
        log.info("Shutdown complete.");
    }

    @Override
    public CompletableFuture<WriteShareGroupStateResponseData> writeState(RequestContext context, WriteShareGroupStateRequestData request) {
        // Send an empty response if topic data is empty
        if (isEmpty(request.topics())) {
            log.error("Topic Data is empty: {}", request);
            return CompletableFuture.completedFuture(
                new WriteShareGroupStateResponseData()
            );
        }

        // Send an empty response if partition data is empty for any topic
        for (WriteShareGroupStateRequestData.WriteStateData topicData : request.topics()) {
            if (isEmpty(topicData.partitions())) {
                log.error("Partition Data for topic {} is empty: {}", topicData.topicId(), request);
                return CompletableFuture.completedFuture(
                    new WriteShareGroupStateResponseData()
                );
            }
        }

        String groupId = request.groupId();
        // Send an empty response if groupId is invalid
        if (isGroupIdEmpty(groupId)) {
            log.error("Group id must be specified and non-empty: {}", request);
            return CompletableFuture.completedFuture(
                new WriteShareGroupStateResponseData()
            );
        }

        // Send an empty response if the coordinator is not active
        if (!isActive.get()) {
            return CompletableFuture.completedFuture(
                generateErrorWriteStateResponse(
                    request,
                    Errors.COORDINATOR_NOT_AVAILABLE,
                    "Share coordinator is not available."
                )
            );
        }

        // The request received here could have multiple keys of structure group:topic:partition. However,
        // the writeState method in ShareCoordinatorShard expects a single key in the request. Hence, we will
        // be looping over the keys below and constructing new WriteShareGroupStateRequestData objects to pass
        // onto the shard method.
        Map<Uuid, Map<Integer, CompletableFuture<WriteShareGroupStateResponseData>>> futureMap = new HashMap<>();
        long startTimeMs = time.hiResClockMs();

        request.topics().forEach(topicData -> {
            Map<Integer, CompletableFuture<WriteShareGroupStateResponseData>> partitionFut =
                futureMap.computeIfAbsent(topicData.topicId(), k -> new HashMap<>());
            topicData.partitions().forEach(
                partitionData -> {
                    CompletableFuture<WriteShareGroupStateResponseData> future = runtime.scheduleWriteOperation(
                            "write-share-group-state",
                            topicPartitionFor(SharePartitionKey.getInstance(groupId, topicData.topicId(), partitionData.partition())),
                            Duration.ofMillis(config.shareCoordinatorWriteTimeoutMs()),
                            coordinator -> coordinator.writeState(new WriteShareGroupStateRequestData()
                                .setGroupId(groupId)
                                .setTopics(Collections.singletonList(new WriteShareGroupStateRequestData.WriteStateData()
                                    .setTopicId(topicData.topicId())
                                    .setPartitions(Collections.singletonList(new WriteShareGroupStateRequestData.PartitionData()
                                        .setPartition(partitionData.partition())
                                        .setStartOffset(partitionData.startOffset())
                                        .setLeaderEpoch(partitionData.leaderEpoch())
                                        .setStateEpoch(partitionData.stateEpoch())
                                        .setStateBatches(partitionData.stateBatches())))))))
                        .exceptionally(exception -> handleOperationException(
                            "write-share-group-state",
                            request,
                            exception,
                            (error, message) -> WriteShareGroupStateResponse.toErrorResponseData(
                                topicData.topicId(),
                                partitionData.partition(),
                                error,
                                "Unable to write share group state: " + exception.getMessage()
                            ),
                            log
                        ));
                    partitionFut.put(partitionData.partition(), future);
                });
        });

        // Combine all futures into a single CompletableFuture<Void>.
        CompletableFuture<Void> combinedFuture = CompletableFuture.allOf(futureMap.values().stream()
            .flatMap(partMap -> partMap.values().stream()).toArray(CompletableFuture[]::new));

        // topicId -> {partitionId -> responseFuture}
        return combinedFuture.thenApply(v -> {
            List<WriteShareGroupStateResponseData.WriteStateResult> writeStateResults = new ArrayList<>(futureMap.size());
            futureMap.forEach(
                (topicId, topicEntry) -> {
                    List<WriteShareGroupStateResponseData.PartitionResult> partitionResults = new ArrayList<>(topicEntry.size());
                    topicEntry.forEach(
                        // Map of partition id -> responses from api.
                        (partitionId, responseFut) -> {
                            // This is the future returned by runtime.scheduleWriteOperation which returns when the
                            // operation has completed including error information. When this line executes, the future
                            // should be complete as we used CompletableFuture::allOf to get a combined future from
                            // all futures in the map.
                            WriteShareGroupStateResponseData partitionData = responseFut.getNow(null);
                            partitionResults.addAll(partitionData.results().get(0).partitions());
                        }
                    );
                    writeStateResults.add(WriteShareGroupStateResponse.toResponseWriteStateResult(topicId, partitionResults));
                }
            );

            // Time taken for write.
            // At this point all futures are completed written above.
            shareCoordinatorMetrics.record(ShareCoordinatorMetrics.SHARE_COORDINATOR_WRITE_LATENCY_SENSOR_NAME,
                time.hiResClockMs() - startTimeMs);

            return new WriteShareGroupStateResponseData()
                .setResults(writeStateResults);
        });
    }

    @Override
    public CompletableFuture<ReadShareGroupStateResponseData> readState(RequestContext context, ReadShareGroupStateRequestData request) {
        String groupId = request.groupId();
        // A map to store the futures for each topicId and partition.
        Map<Uuid, Map<Integer, CompletableFuture<ReadShareGroupStateResponseData>>> futureMap = new HashMap<>();

        // Send an empty response if topic data is empty.
        if (isEmpty(request.topics())) {
            log.error("Topic Data is empty: {}", request);
            return CompletableFuture.completedFuture(
                new ReadShareGroupStateResponseData()
            );
        }

        // Send an empty response if partition data is empty for any topic.
        for (ReadShareGroupStateRequestData.ReadStateData topicData : request.topics()) {
            if (isEmpty(topicData.partitions())) {
                log.error("Partition Data for topic {} is empty: {}", topicData.topicId(), request);
                return CompletableFuture.completedFuture(
                    new ReadShareGroupStateResponseData()
                );
            }
        }

        // Send an empty response if groupId is invalid.
        if (isGroupIdEmpty(groupId)) {
            log.error("Group id must be specified and non-empty: {}", request);
            return CompletableFuture.completedFuture(
                new ReadShareGroupStateResponseData()
            );
        }

        // Send an empty response if the coordinator is not active.
        if (!isActive.get()) {
            return CompletableFuture.completedFuture(
                generateErrorReadStateResponse(
                    request,
                    Errors.COORDINATOR_NOT_AVAILABLE,
                    "Share coordinator is not available."
                )
            );
        }

        // The request received here could have multiple keys of structure group:topic:partition. However,
        // the readState method in ShareCoordinatorShard expects a single key in the request. Hence, we will
        // be looping over the keys below and constructing new ReadShareGroupStateRequestData objects to pass
        // onto the shard method.

        // It is possible that a read state request contains a leaderEpoch which is the higher than seen so
        // far, for a specific share partition. Hence, for each read request - we must check for this
        // and update the state appropriately.

        for (ReadShareGroupStateRequestData.ReadStateData topicData : request.topics()) {
            Uuid topicId = topicData.topicId();
            for (ReadShareGroupStateRequestData.PartitionData partitionData : topicData.partitions()) {
                SharePartitionKey coordinatorKey = SharePartitionKey.getInstance(request.groupId(), topicId, partitionData.partition());

                ReadShareGroupStateRequestData requestForCurrentPartition = new ReadShareGroupStateRequestData()
                    .setGroupId(groupId)
                    .setTopics(Collections.singletonList(new ReadShareGroupStateRequestData.ReadStateData()
                        .setTopicId(topicId)
                        .setPartitions(Collections.singletonList(partitionData))));

                // We are issuing a scheduleWriteOperation even though the request is of read type since
                // we might want to update the leader epoch, if it is the highest seen so far for the specific
                // share partition. In that case, we require the strong consistency offered by scheduleWriteOperation.
                // At the time of writing, read after write consistency for the readState and writeState requests
                // is not guaranteed.
                CompletableFuture<ReadShareGroupStateResponseData> readFuture = runtime.scheduleWriteOperation(
                    "read-update-leader-epoch-state",
                    topicPartitionFor(coordinatorKey),
                    Duration.ofMillis(config.shareCoordinatorWriteTimeoutMs()),
                    coordinator -> coordinator.readStateAndMaybeUpdateLeaderEpoch(requestForCurrentPartition)
                ).exceptionally(readException ->
                    handleOperationException(
                        "read-update-leader-epoch-state",
                        request,
                        readException,
                        (error, message) -> ReadShareGroupStateResponse.toErrorResponseData(
                            topicData.topicId(),
                            partitionData.partition(),
                            error,
                            "Unable to read share group state: " + readException.getMessage()
                        ),
                        log
                    ));

                futureMap.computeIfAbsent(topicId, k -> new HashMap<>())
                    .put(partitionData.partition(), readFuture);
            }
        }

        // Combine all futures into a single CompletableFuture<Void>.
        CompletableFuture<Void> combinedFuture = CompletableFuture.allOf(futureMap.values().stream()
            .flatMap(map -> map.values().stream()).toArray(CompletableFuture[]::new));

        // Transform the combined CompletableFuture<Void> into CompletableFuture<ReadShareGroupStateResponseData>.
        return combinedFuture.thenApply(v -> {
            List<ReadShareGroupStateResponseData.ReadStateResult> readStateResult = new ArrayList<>(futureMap.size());
            futureMap.forEach(
                (topicId, topicEntry) -> {
                    List<ReadShareGroupStateResponseData.PartitionResult> partitionResults = new ArrayList<>(topicEntry.size());
                    topicEntry.forEach(
                        (partitionId, responseFut) -> {
                            // ResponseFut would already be completed by now since we have used
                            // CompletableFuture::allOf to create a combined future from the future map.
                            partitionResults.add(
                                responseFut.getNow(null).results().get(0).partitions().get(0)
                            );
                        }
                    );
                    readStateResult.add(ReadShareGroupStateResponse.toResponseReadStateResult(topicId, partitionResults));
                }
            );
            return new ReadShareGroupStateResponseData()
                .setResults(readStateResult);
        });
    }

    private ReadShareGroupStateResponseData generateErrorReadStateResponse(
        ReadShareGroupStateRequestData request,
        Errors error,
        String errorMessage
    ) {
        return new ReadShareGroupStateResponseData().setResults(request.topics().stream()
            .map(topicData -> {
                ReadShareGroupStateResponseData.ReadStateResult resultData = new ReadShareGroupStateResponseData.ReadStateResult();
                resultData.setTopicId(topicData.topicId());
                resultData.setPartitions(topicData.partitions().stream()
                    .map(partitionData -> ReadShareGroupStateResponse.toErrorResponsePartitionResult(
                        partitionData.partition(), error, errorMessage
                    )).collect(Collectors.toList()));
                return resultData;
            }).collect(Collectors.toList()));
    }

    private WriteShareGroupStateResponseData generateErrorWriteStateResponse(
        WriteShareGroupStateRequestData request,
        Errors error,
        String errorMessage
    ) {
        return new WriteShareGroupStateResponseData()
            .setResults(request.topics().stream()
                .map(topicData -> {
                    WriteShareGroupStateResponseData.WriteStateResult resultData = new WriteShareGroupStateResponseData.WriteStateResult();
                    resultData.setTopicId(topicData.topicId());
                    resultData.setPartitions(topicData.partitions().stream()
                        .map(partitionData -> WriteShareGroupStateResponse.toErrorResponsePartitionResult(
                            partitionData.partition(), error, errorMessage
                        )).collect(Collectors.toList()));
                    return resultData;
                }).collect(Collectors.toList()));
    }

    private static boolean isGroupIdEmpty(String groupId) {
        return groupId == null || groupId.isEmpty();
    }

    @Override
    public void onElection(int partitionIndex, int partitionLeaderEpoch) {
        throwIfNotActive();
        runtime.scheduleLoadOperation(
            new TopicPartition(Topic.SHARE_GROUP_STATE_TOPIC_NAME, partitionIndex),
            partitionLeaderEpoch
        );
    }

    @Override
    public void onResignation(int partitionIndex, OptionalInt partitionLeaderEpoch) {
        throwIfNotActive();
        runtime.scheduleUnloadOperation(
            new TopicPartition(Topic.SHARE_GROUP_STATE_TOPIC_NAME, partitionIndex),
            partitionLeaderEpoch
        );
    }

    @Override
    public void onNewMetadataImage(MetadataImage newImage, MetadataDelta delta) {
        throwIfNotActive();
        this.runtime.onNewMetadataImage(newImage, delta);
    }

    TopicPartition topicPartitionFor(SharePartitionKey key) {
        return new TopicPartition(Topic.SHARE_GROUP_STATE_TOPIC_NAME, partitionFor(key));
    }

    private static <P> boolean isEmpty(List<P> list) {
        return list == null || list.isEmpty();
    }

    private void throwIfNotActive() {
        if (!isActive.get()) {
            throw Errors.COORDINATOR_NOT_AVAILABLE.exception();
        }
    }
}
