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
import org.apache.kafka.common.message.ReadShareGroupStateRequestData;
import org.apache.kafka.common.message.ReadShareGroupStateResponseData;
import org.apache.kafka.common.message.WriteShareGroupStateRequestData;
import org.apache.kafka.common.message.WriteShareGroupStateResponseData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ReadShareGroupStateResponse;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.requests.WriteShareGroupStateResponse;
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
import org.apache.kafka.coordinator.share.generated.ShareSnapshotKey;
import org.apache.kafka.coordinator.share.generated.ShareSnapshotValue;
import org.apache.kafka.coordinator.share.generated.ShareUpdateKey;
import org.apache.kafka.coordinator.share.generated.ShareUpdateValue;
import org.apache.kafka.coordinator.share.metrics.ShareCoordinatorMetrics;
import org.apache.kafka.coordinator.share.metrics.ShareCoordinatorMetricsShard;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.config.ShareCoordinatorConfig;
import org.apache.kafka.server.share.SharePartitionKey;
import org.apache.kafka.server.share.persister.PartitionFactory;
import org.apache.kafka.server.share.persister.PersisterStateBatch;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;

import org.slf4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class ShareCoordinatorShard implements CoordinatorShard<CoordinatorRecord> {
    private final Logger log;
    private final ShareCoordinatorConfig config;
    private final CoordinatorMetrics coordinatorMetrics;
    private final CoordinatorMetricsShard metricsShard;
    private final TimelineHashMap<SharePartitionKey, ShareGroupOffset> shareStateMap;  // coord key -> ShareGroupOffset
    // leaderEpochMap can be updated by writeState call
    // or if a newer leader makes a readState call.
    private final TimelineHashMap<SharePartitionKey, Integer> leaderEpochMap;
    private final TimelineHashMap<SharePartitionKey, Integer> snapshotUpdateCount;
    private final TimelineHashMap<SharePartitionKey, Integer> stateEpochMap;
    private MetadataImage metadataImage;

    public static final Exception NULL_TOPIC_ID = new Exception("The topic id cannot be null.");
    public static final Exception NEGATIVE_PARTITION_ID = new Exception("The partition id cannot be a negative number.");

    public static class Builder implements CoordinatorShardBuilder<ShareCoordinatorShard, CoordinatorRecord> {
        private ShareCoordinatorConfig config;
        private LogContext logContext;
        private SnapshotRegistry snapshotRegistry;
        private CoordinatorMetrics coordinatorMetrics;
        private TopicPartition topicPartition;

        public Builder(ShareCoordinatorConfig config) {
            this.config = config;
        }

        @Override
        public CoordinatorShardBuilder<ShareCoordinatorShard, CoordinatorRecord> withSnapshotRegistry(SnapshotRegistry snapshotRegistry) {
            this.snapshotRegistry = snapshotRegistry;
            return this;
        }

        @Override
        public CoordinatorShardBuilder<ShareCoordinatorShard, CoordinatorRecord> withLogContext(LogContext logContext) {
            this.logContext = logContext;
            return this;
        }

        @Override
        public CoordinatorShardBuilder<ShareCoordinatorShard, CoordinatorRecord> withTime(Time time) {
            // method is required due to interface
            return this;
        }

        @Override
        public CoordinatorShardBuilder<ShareCoordinatorShard, CoordinatorRecord> withTimer(CoordinatorTimer<Void, CoordinatorRecord> timer) {
            // method is required due to interface
            return this;
        }

        @Override
        public CoordinatorShardBuilder<ShareCoordinatorShard, CoordinatorRecord> withExecutor(CoordinatorExecutor<CoordinatorRecord> executor) {
            // method is required due to interface
            return this;
        }

        @Override
        public CoordinatorShardBuilder<ShareCoordinatorShard, CoordinatorRecord> withCoordinatorMetrics(CoordinatorMetrics coordinatorMetrics) {
            this.coordinatorMetrics = coordinatorMetrics;
            return this;
        }

        @Override
        public CoordinatorShardBuilder<ShareCoordinatorShard, CoordinatorRecord> withTopicPartition(TopicPartition topicPartition) {
            this.topicPartition = topicPartition;
            return this;
        }

        @Override
        @SuppressWarnings("NPathComplexity")
        public ShareCoordinatorShard build() {
            if (logContext == null) logContext = new LogContext();
            if (config == null)
                throw new IllegalArgumentException("Config must be set.");
            if (snapshotRegistry == null)
                throw new IllegalArgumentException("SnapshotRegistry must be set.");
            if (coordinatorMetrics == null || !(coordinatorMetrics instanceof ShareCoordinatorMetrics))
                throw new IllegalArgumentException("CoordinatorMetrics must be set and be of type ShareCoordinatorMetrics.");
            if (topicPartition == null)
                throw new IllegalArgumentException("TopicPartition must be set.");

            ShareCoordinatorMetricsShard metricsShard = ((ShareCoordinatorMetrics) coordinatorMetrics)
                .newMetricsShard(snapshotRegistry, topicPartition);

            return new ShareCoordinatorShard(
                logContext,
                config,
                coordinatorMetrics,
                metricsShard,
                snapshotRegistry
            );
        }
    }

    ShareCoordinatorShard(
        LogContext logContext,
        ShareCoordinatorConfig config,
        CoordinatorMetrics coordinatorMetrics,
        CoordinatorMetricsShard metricsShard,
        SnapshotRegistry snapshotRegistry
    ) {
        this.log = logContext.logger(ShareCoordinatorShard.class);
        this.config = config;
        this.coordinatorMetrics = coordinatorMetrics;
        this.metricsShard = metricsShard;
        this.shareStateMap = new TimelineHashMap<>(snapshotRegistry, 0);
        this.leaderEpochMap = new TimelineHashMap<>(snapshotRegistry, 0);
        this.snapshotUpdateCount = new TimelineHashMap<>(snapshotRegistry, 0);
        this.stateEpochMap = new TimelineHashMap<>(snapshotRegistry, 0);
    }

    @Override
    public void onLoaded(MetadataImage newImage) {
        coordinatorMetrics.activateMetricsShard(metricsShard);
    }

    @Override
    public void onNewMetadataImage(MetadataImage newImage, MetadataDelta delta) {
        this.metadataImage = newImage;
    }

    @Override
    public void onUnloaded() {
        coordinatorMetrics.deactivateMetricsShard(metricsShard);
    }

    @Override
    public void replay(long offset, long producerId, short producerEpoch, CoordinatorRecord record) throws RuntimeException {
        ApiMessageAndVersion key = record.key();
        ApiMessageAndVersion value = record.value();

        switch (key.version()) {
            case ShareCoordinator.SHARE_SNAPSHOT_RECORD_KEY_VERSION: // ShareSnapshot
                handleShareSnapshot((ShareSnapshotKey) key.message(), (ShareSnapshotValue) messageOrNull(value));
                break;
            case ShareCoordinator.SHARE_UPDATE_RECORD_KEY_VERSION: // ShareUpdate
                handleShareUpdate((ShareUpdateKey) key.message(), (ShareUpdateValue) messageOrNull(value));
                break;
            default:
                // Noop
        }
    }

    private void handleShareSnapshot(ShareSnapshotKey key, ShareSnapshotValue value) {
        SharePartitionKey mapKey = SharePartitionKey.getInstance(key.groupId(), key.topicId(), key.partition());
        maybeUpdateLeaderEpochMap(mapKey, value.leaderEpoch());
        maybeUpdateStateEpochMap(mapKey, value.stateEpoch());

        ShareGroupOffset offsetRecord = ShareGroupOffset.fromRecord(value);
        // this record is the complete snapshot
        shareStateMap.put(mapKey, offsetRecord);
        // if number of share updates is exceeded, then reset it
        if (snapshotUpdateCount.containsKey(mapKey)) {
            if (snapshotUpdateCount.get(mapKey) >= config.shareCoordinatorSnapshotUpdateRecordsPerSnapshot()) {
                snapshotUpdateCount.put(mapKey, 0);
            }
        }
    }

    private void handleShareUpdate(ShareUpdateKey key, ShareUpdateValue value) {
        SharePartitionKey mapKey = SharePartitionKey.getInstance(key.groupId(), key.topicId(), key.partition());
        maybeUpdateLeaderEpochMap(mapKey, value.leaderEpoch());

        // Share update does not hold state epoch information.

        ShareGroupOffset offsetRecord = ShareGroupOffset.fromRecord(value);
        // This is an incremental snapshot,
        // so we need to apply it to our current soft state.
        shareStateMap.compute(mapKey, (k, v) -> v == null ? offsetRecord : merge(v, value));
        snapshotUpdateCount.compute(mapKey, (k, v) -> v == null ? 0 : v + 1);
    }

    private void maybeUpdateLeaderEpochMap(SharePartitionKey mapKey, int leaderEpoch) {
        leaderEpochMap.putIfAbsent(mapKey, leaderEpoch);
        if (leaderEpochMap.get(mapKey) < leaderEpoch) {
            leaderEpochMap.put(mapKey, leaderEpoch);
        }
    }

    private void maybeUpdateStateEpochMap(SharePartitionKey mapKey, int stateEpoch) {
        stateEpochMap.putIfAbsent(mapKey, stateEpoch);
        if (stateEpochMap.get(mapKey) < stateEpoch) {
            stateEpochMap.put(mapKey, stateEpoch);
        }
    }

    @Override
    public void replayEndTransactionMarker(long producerId, short producerEpoch, TransactionResult result) throws RuntimeException {
        CoordinatorShard.super.replayEndTransactionMarker(producerId, producerEpoch, result);
    }

    /**
     * This method generates the ShareSnapshotValue record corresponding to the requested topic partition information.
     * The generated record is then written to the __share_group_state topic and replayed to the in-memory state
     * of the coordinator shard, shareStateMap, by CoordinatorRuntime.
     * <p>
     * This method as called by the ShareCoordinatorService will be provided with
     * the request data which covers only a single key i.e. group1:topic1:partition1. The implementation
     * below was done keeping this in mind.
     *
     * @param request - WriteShareGroupStateRequestData for a single key
     * @return CoordinatorResult(records, response)
     */
    public CoordinatorResult<WriteShareGroupStateResponseData, CoordinatorRecord> writeState(
        WriteShareGroupStateRequestData request
    ) {
        // Records to write (with both key and value of snapshot type), response to caller
        // only one key will be there in the request by design.
        metricsShard.record(ShareCoordinatorMetrics.SHARE_COORDINATOR_WRITE_SENSOR_NAME);
        Optional<CoordinatorResult<WriteShareGroupStateResponseData, CoordinatorRecord>> error = maybeGetWriteStateError(request);
        if (error.isPresent()) {
            return error.get();
        }

        WriteShareGroupStateRequestData.WriteStateData topicData = request.topics().get(0);
        WriteShareGroupStateRequestData.PartitionData partitionData = topicData.partitions().get(0);
        SharePartitionKey key = SharePartitionKey.getInstance(request.groupId(), topicData.topicId(), partitionData.partition());

        CoordinatorRecord record = generateShareStateRecord(partitionData, key);
        // build successful response if record is correctly created
        WriteShareGroupStateResponseData responseData = new WriteShareGroupStateResponseData()
            .setResults(
                Collections.singletonList(
                    WriteShareGroupStateResponse.toResponseWriteStateResult(key.topicId(),
                        Collections.singletonList(
                            WriteShareGroupStateResponse.toResponsePartitionResult(
                                key.partition()
                            ))
                    ))
            );

        return new CoordinatorResult<>(Collections.singletonList(record), responseData);
    }

    /**
     * Method reads data from the soft state and if needed updates the leader epoch.
     * It can happen that a read state call for a share partition has a higher leaderEpoch
     * value than seen so far.
     * In case an update is not required, empty record list will be generated along with a success response.
     * @param request - represents ReadShareGroupStateRequestData
     * @return CoordinatorResult object
     */
    public CoordinatorResult<ReadShareGroupStateResponseData, CoordinatorRecord> readStateAndMaybeUpdateLeaderEpoch(
        ReadShareGroupStateRequestData request
    ) {
        // Only one key will be there in the request by design.
        Optional<ReadShareGroupStateResponseData> error = maybeGetReadStateError(request);
        if (error.isPresent()) {
            return new CoordinatorResult<>(Collections.emptyList(), error.get());
        }

        ReadShareGroupStateRequestData.ReadStateData topicData = request.topics().get(0);
        ReadShareGroupStateRequestData.PartitionData partitionData = topicData.partitions().get(0);

        Uuid topicId = topicData.topicId();
        int partitionId = partitionData.partition();
        int leaderEpoch = partitionData.leaderEpoch();
        SharePartitionKey key = SharePartitionKey.getInstance(request.groupId(), topicId, partitionId);

        ReadShareGroupStateResponseData responseData = null;

        if (!shareStateMap.containsKey(key)) {
            // Leader epoch update might be needed
            responseData = ReadShareGroupStateResponse.toResponseData(
                topicId,
                partitionId,
                PartitionFactory.UNINITIALIZED_START_OFFSET,
                PartitionFactory.DEFAULT_STATE_EPOCH,
                Collections.emptyList()
            );
        } else {
            // Leader epoch update might be needed
            ShareGroupOffset offsetValue = shareStateMap.get(key);
            List<ReadShareGroupStateResponseData.StateBatch> stateBatches = (offsetValue.stateBatches() != null && !offsetValue.stateBatches().isEmpty()) ?
                offsetValue.stateBatches().stream()
                    .map(
                        stateBatch -> new ReadShareGroupStateResponseData.StateBatch()
                            .setFirstOffset(stateBatch.firstOffset())
                            .setLastOffset(stateBatch.lastOffset())
                            .setDeliveryState(stateBatch.deliveryState())
                            .setDeliveryCount(stateBatch.deliveryCount())
                    ).collect(Collectors.toList()) : Collections.emptyList();

            responseData = ReadShareGroupStateResponse.toResponseData(
                topicId,
                partitionId,
                offsetValue.startOffset(),
                offsetValue.stateEpoch(),
                stateBatches
            );
        }

        // Optimization in case leaderEpoch update is not required.
        if (leaderEpoch == -1 ||
            (leaderEpochMap.get(key) != null && leaderEpochMap.get(key) == leaderEpoch)) {
            return new CoordinatorResult<>(Collections.emptyList(), responseData);
        }

        // It is OK to info log this since this reaching this codepoint should be quite infrequent.
        log.info("Read with leader epoch update call for key {} having new leader epoch {}.", key, leaderEpoch);

        // Recording the sensor here as above if condition will not produce any record.
        metricsShard.record(ShareCoordinatorMetrics.SHARE_COORDINATOR_WRITE_SENSOR_NAME);

        // Generate record with leaderEpoch info.
        WriteShareGroupStateRequestData.PartitionData writePartitionData = new WriteShareGroupStateRequestData.PartitionData()
            .setPartition(partitionId)
            .setLeaderEpoch(leaderEpoch)
            .setStateBatches(Collections.emptyList())
            .setStartOffset(responseData.results().get(0).partitions().get(0).startOffset())
            .setStateEpoch(responseData.results().get(0).partitions().get(0).stateEpoch());

        CoordinatorRecord record = generateShareStateRecord(writePartitionData, key);
        return new CoordinatorResult<>(Collections.singletonList(record), responseData);
    }

    /**
     * Util method to generate a ShareSnapshot or ShareUpdate type record for a key, based on various conditions.
     * <p>
     * if no snapshot has been created for the key => create a new ShareSnapshot record
     * else if number of ShareUpdate records for key >= max allowed per snapshot per key => create a new ShareSnapshot record
     * else create a new ShareUpdate record
     *
     * @param partitionData - Represents the data which should be written into the share state record.
     * @param key - The {@link SharePartitionKey} object.
     * @return {@link CoordinatorRecord} representing ShareSnapshot or ShareUpdate
     */
    private CoordinatorRecord generateShareStateRecord(
        WriteShareGroupStateRequestData.PartitionData partitionData,
        SharePartitionKey key
    ) {
        if (!shareStateMap.containsKey(key)) {
            // Since this is the first time we are getting a write request for key, we should be creating a share snapshot record.
            // The incoming partition data could have overlapping state batches, we must merge them
            return ShareCoordinatorRecordHelpers.newShareSnapshotRecord(
                key.groupId(), key.topicId(), partitionData.partition(),
                new ShareGroupOffset.Builder()
                    .setSnapshotEpoch(0)
                    .setStartOffset(partitionData.startOffset())
                    .setLeaderEpoch(partitionData.leaderEpoch())
                    .setStateEpoch(partitionData.stateEpoch())
                    .setStateBatches(mergeBatches(Collections.emptyList(), partitionData))
                    .build());
        } else if (snapshotUpdateCount.getOrDefault(key, 0) >= config.shareCoordinatorSnapshotUpdateRecordsPerSnapshot()) {
            ShareGroupOffset currentState = shareStateMap.get(key); // shareStateMap will have the entry as containsKey is true
            int newLeaderEpoch = partitionData.leaderEpoch() == -1 ? currentState.leaderEpoch() : partitionData.leaderEpoch();
            int newStateEpoch = partitionData.stateEpoch() == -1 ? currentState.stateEpoch() : partitionData.stateEpoch();
            long newStartOffset = partitionData.startOffset() == -1 ? currentState.startOffset() : partitionData.startOffset();

            // Since the number of update records for this share part key exceeds snapshotUpdateRecordsPerSnapshot,
            // we should be creating a share snapshot record.
            // The incoming partition data could have overlapping state batches, we must merge them.
            return ShareCoordinatorRecordHelpers.newShareSnapshotRecord(
                key.groupId(), key.topicId(), partitionData.partition(),
                new ShareGroupOffset.Builder()
                    .setSnapshotEpoch(currentState.snapshotEpoch() + 1)   // We must increment snapshot epoch as this is new snapshot.
                    .setStartOffset(newStartOffset)
                    .setLeaderEpoch(newLeaderEpoch)
                    .setStateEpoch(newStateEpoch)
                    .setStateBatches(mergeBatches(currentState.stateBatches(), partitionData, newStartOffset))
                    .build());
        } else {
            ShareGroupOffset currentState = shareStateMap.get(key); // shareStateMap will have the entry as containsKey is true.

            // Share snapshot is present and number of share snapshot update records < snapshotUpdateRecordsPerSnapshot
            // so create a share update record.
            // The incoming partition data could have overlapping state batches, we must merge them.
            return ShareCoordinatorRecordHelpers.newShareSnapshotUpdateRecord(
                key.groupId(), key.topicId(), partitionData.partition(),
                new ShareGroupOffset.Builder()
                    .setSnapshotEpoch(currentState.snapshotEpoch()) // Use same snapshotEpoch as last share snapshot.
                    .setStartOffset(partitionData.startOffset())
                    .setLeaderEpoch(partitionData.leaderEpoch())
                    .setStateBatches(mergeBatches(Collections.emptyList(), partitionData))
                    .build());
        }
    }

    private List<PersisterStateBatch> mergeBatches(
        List<PersisterStateBatch> soFar,
        WriteShareGroupStateRequestData.PartitionData partitionData) {
        return mergeBatches(soFar, partitionData, partitionData.startOffset());
    }

    private List<PersisterStateBatch> mergeBatches(
        List<PersisterStateBatch> soFar,
        WriteShareGroupStateRequestData.PartitionData partitionData,
        long startOffset) {
        return new PersisterStateBatchCombiner(
            soFar,
            partitionData.stateBatches().stream()
                .map(PersisterStateBatch::from)
                .collect(Collectors.toList()),
            startOffset
        )
            .combineStateBatches();
    }

    private Optional<CoordinatorResult<WriteShareGroupStateResponseData, CoordinatorRecord>> maybeGetWriteStateError(
        WriteShareGroupStateRequestData request
    ) {
        String groupId = request.groupId();
        WriteShareGroupStateRequestData.WriteStateData topicData = request.topics().get(0);
        WriteShareGroupStateRequestData.PartitionData partitionData = topicData.partitions().get(0);

        Uuid topicId = topicData.topicId();
        int partitionId = partitionData.partition();

        if (topicId == null) {
            return Optional.of(getWriteErrorResponse(Errors.INVALID_REQUEST, NULL_TOPIC_ID, null, partitionId));
        }

        if (partitionId < 0) {
            return Optional.of(getWriteErrorResponse(Errors.INVALID_REQUEST, NEGATIVE_PARTITION_ID, topicId, partitionId));
        }

        SharePartitionKey mapKey = SharePartitionKey.getInstance(groupId, topicId, partitionId);
        if (partitionData.leaderEpoch() != -1 && leaderEpochMap.containsKey(mapKey) && leaderEpochMap.get(mapKey) > partitionData.leaderEpoch()) {
            log.error("Request leader epoch smaller than last recorded.");
            return Optional.of(getWriteErrorResponse(Errors.FENCED_LEADER_EPOCH, null, topicId, partitionId));
        }
        if (partitionData.stateEpoch() != -1 && stateEpochMap.containsKey(mapKey) && stateEpochMap.get(mapKey) > partitionData.stateEpoch()) {
            log.error("Request state epoch smaller than last recorded.");
            return Optional.of(getWriteErrorResponse(Errors.FENCED_STATE_EPOCH, null, topicId, partitionId));
        }
        if (metadataImage == null) {
            log.error("Metadata image is null");
            return Optional.of(getWriteErrorResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, null, topicId, partitionId));
        }
        if (metadataImage.topics().getTopic(topicId) == null ||
            metadataImage.topics().getPartition(topicId, partitionId) == null) {
            log.error("Topic/TopicPartition not found in metadata image.");
            return Optional.of(getWriteErrorResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, null, topicId, partitionId));
        }

        return Optional.empty();
    }

    private Optional<ReadShareGroupStateResponseData> maybeGetReadStateError(ReadShareGroupStateRequestData request) {
        String groupId = request.groupId();
        ReadShareGroupStateRequestData.ReadStateData topicData = request.topics().get(0);
        ReadShareGroupStateRequestData.PartitionData partitionData = topicData.partitions().get(0);

        Uuid topicId = topicData.topicId();
        int partitionId = partitionData.partition();

        if (topicId == null) {
            log.error("Request topic id is null.");
            return Optional.of(ReadShareGroupStateResponse.toErrorResponseData(
                null, partitionId, Errors.INVALID_REQUEST, NULL_TOPIC_ID.getMessage()));
        }

        if (partitionId < 0) {
            log.error("Request partition id is negative.");
            return Optional.of(ReadShareGroupStateResponse.toErrorResponseData(
                topicId, partitionId, Errors.INVALID_REQUEST, NEGATIVE_PARTITION_ID.getMessage()));
        }

        SharePartitionKey mapKey = SharePartitionKey.getInstance(groupId, topicId, partitionId);
        if (leaderEpochMap.containsKey(mapKey) && leaderEpochMap.get(mapKey) > partitionData.leaderEpoch()) {
            log.error("Request leader epoch id is smaller than last recorded.");
            return Optional.of(ReadShareGroupStateResponse.toErrorResponseData(topicId, partitionId, Errors.FENCED_LEADER_EPOCH, Errors.FENCED_LEADER_EPOCH.message()));
        }

        if (metadataImage == null) {
            log.error("Metadata image is null");
            return Optional.of(ReadShareGroupStateResponse.toErrorResponseData(topicId, partitionId, Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.UNKNOWN_TOPIC_OR_PARTITION.message()));
        }

        if (metadataImage.topics().getTopic(topicId) == null ||
            metadataImage.topics().getPartition(topicId, partitionId) == null) {
            log.error("Topic/TopicPartition not found in metadata image.");
            return Optional.of(ReadShareGroupStateResponse.toErrorResponseData(topicId, partitionId, Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.UNKNOWN_TOPIC_OR_PARTITION.message()));
        }

        return Optional.empty();
    }

    private CoordinatorResult<WriteShareGroupStateResponseData, CoordinatorRecord> getWriteErrorResponse(
        Errors error,
        Exception exception,
        Uuid topicId,
        int partitionId
    ) {
        String message = exception == null ? error.message() : exception.getMessage();
        WriteShareGroupStateResponseData responseData = WriteShareGroupStateResponse.toErrorResponseData(topicId, partitionId, error, message);
        return new CoordinatorResult<>(Collections.emptyList(), responseData);
    }

    // Visible for testing
    Integer getLeaderMapValue(SharePartitionKey key) {
        return this.leaderEpochMap.get(key);
    }

    // Visible for testing
    Integer getStateEpochMapValue(SharePartitionKey key) {
        return this.stateEpochMap.get(key);
    }

    // Visible for testing
    ShareGroupOffset getShareStateMapValue(SharePartitionKey key) {
        return this.shareStateMap.get(key);
    }

    // Visible for testing
    CoordinatorMetricsShard getMetricsShard() {
        return metricsShard;
    }

    private static ShareGroupOffset merge(ShareGroupOffset soFar, ShareUpdateValue newData) {
        // Snapshot epoch should be same as last share snapshot.
        // state epoch is not present
        List<PersisterStateBatch> currentBatches = soFar.stateBatches();
        long newStartOffset = newData.startOffset() == -1 ? soFar.startOffset() : newData.startOffset();
        int newLeaderEpoch = newData.leaderEpoch() == -1 ? soFar.leaderEpoch() : newData.leaderEpoch();

        return new ShareGroupOffset.Builder()
            .setSnapshotEpoch(soFar.snapshotEpoch())
            .setStateEpoch(soFar.stateEpoch())
            .setStartOffset(newStartOffset)
            .setLeaderEpoch(newLeaderEpoch)
            .setStateBatches(new PersisterStateBatchCombiner(currentBatches, newData.stateBatches().stream()
                .map(ShareCoordinatorShard::toPersisterStateBatch)
                .collect(Collectors.toList()), newStartOffset)
                .combineStateBatches())
            .build();
    }

    private static ApiMessage messageOrNull(ApiMessageAndVersion apiMessageAndVersion) {
        if (apiMessageAndVersion == null) {
            return null;
        } else {
            return apiMessageAndVersion.message();
        }
    }

    /**
     * Util function to convert a state batch of type {@link ShareUpdateValue.StateBatch }
     * to {@link PersisterStateBatch}.
     *
     * @param batch - The object representing {@link ShareUpdateValue.StateBatch}
     * @return {@link PersisterStateBatch}
     */
    private static PersisterStateBatch toPersisterStateBatch(ShareUpdateValue.StateBatch batch) {
        return new PersisterStateBatch(
            batch.firstOffset(),
            batch.lastOffset(),
            batch.deliveryState(),
            batch.deliveryCount()
        );
    }
}
