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
import org.apache.kafka.server.group.share.PartitionFactory;
import org.apache.kafka.server.group.share.PersisterStateBatch;
import org.apache.kafka.server.group.share.SharePartitionKey;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeSet;
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
        this.metadataImage = newImage;
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
                // noop
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

        // share update does not hold state epoch information.

        ShareGroupOffset offsetRecord = ShareGroupOffset.fromRecord(value);
        // this is an incremental snapshot
        // so, we need to apply it to our current soft state
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
    @SuppressWarnings("NPathComplexity")
    public CoordinatorResult<WriteShareGroupStateResponseData, CoordinatorRecord> writeState(
        WriteShareGroupStateRequestData request
    ) {
        // records to write (with both key and value of snapshot type), response to caller
        // only one key will be there in the request by design
        metricsShard.record(ShareCoordinatorMetrics.SHARE_COORDINATOR_WRITE_SENSOR_NAME);
        Optional<CoordinatorResult<WriteShareGroupStateResponseData, CoordinatorRecord>> error = maybeGetWriteStateError(request);
        if (error.isPresent()) {
            return error.get();
        }

        String groupId = request.groupId();
        WriteShareGroupStateRequestData.WriteStateData topicData = request.topics().get(0);
        WriteShareGroupStateRequestData.PartitionData partitionData = topicData.partitions().get(0);

        SharePartitionKey key = SharePartitionKey.getInstance(groupId, topicData.topicId(), partitionData.partition());
        List<CoordinatorRecord> recordList;

        if (!shareStateMap.containsKey(key)) {
            // since this is the first time we are getting a write request, we should be creating a share snapshot record
            recordList = Collections.singletonList(ShareCoordinatorRecordHelpers.newShareSnapshotRecord(
                groupId, topicData.topicId(), partitionData.partition(), ShareGroupOffset.fromRequest(partitionData)
            ));
        } else if (snapshotUpdateCount.getOrDefault(key, 0) >= config.shareCoordinatorSnapshotUpdateRecordsPerSnapshot()) {
            int newLeaderEpoch = partitionData.leaderEpoch() == -1 ? shareStateMap.get(key).leaderEpoch() : partitionData.leaderEpoch();
            int newStateEpoch = partitionData.stateEpoch() == -1 ? shareStateMap.get(key).stateEpoch() : partitionData.stateEpoch();
            long newStartOffset = partitionData.startOffset() == -1 ? shareStateMap.get(key).startOffset() : partitionData.startOffset();

            // Since the number of update records for this share part key exceeds snapshotUpdateRecordsPerSnapshot,
            // we should be creating a share snapshot record.
            List<PersisterStateBatch> batchesToAdd = combineStateBatches(
                shareStateMap.get(key).stateBatches(),
                partitionData.stateBatches().stream()
                    .map(PersisterStateBatch::from)
                    .collect(Collectors.toList()),
                newStartOffset);

            recordList = Collections.singletonList(ShareCoordinatorRecordHelpers.newShareSnapshotRecord(
                groupId, topicData.topicId(), partitionData.partition(),
                new ShareGroupOffset.Builder()
                    .setStartOffset(newStartOffset)
                    .setLeaderEpoch(newLeaderEpoch)
                    .setStateEpoch(newStateEpoch)
                    .setStateBatches(batchesToAdd)
                    .build()));
        } else {
            // share snapshot is present and number of share snapshot update records < snapshotUpdateRecordsPerSnapshot
            recordList = Collections.singletonList(ShareCoordinatorRecordHelpers.newShareSnapshotUpdateRecord(
                groupId, topicData.topicId(), partitionData.partition(), ShareGroupOffset.fromRequest(partitionData, shareStateMap.get(key).snapshotEpoch())
            ));
        }

        List<CoordinatorRecord> validRecords = new LinkedList<>();

        WriteShareGroupStateResponseData responseData = new WriteShareGroupStateResponseData();
        for (CoordinatorRecord record : recordList) {  // should be single record
            if (!(record.key().message() instanceof ShareSnapshotKey) && !(record.key().message() instanceof ShareUpdateKey)) {
                continue;
            }
            SharePartitionKey mapKey = null;
            boolean shouldIncSnapshotEpoch = false;
            if (record.key().message() instanceof ShareSnapshotKey) {
                ShareSnapshotKey recordKey = (ShareSnapshotKey) record.key().message();
                responseData.setResults(Collections.singletonList(WriteShareGroupStateResponse.toResponseWriteStateResult(
                    recordKey.topicId(), Collections.singletonList(WriteShareGroupStateResponse.toResponsePartitionResult(
                        recordKey.partition())))));
                mapKey = SharePartitionKey.getInstance(recordKey.groupId(), recordKey.topicId(), recordKey.partition());
                shouldIncSnapshotEpoch = true;
            } else if (record.key().message() instanceof ShareUpdateKey) {
                ShareUpdateKey recordKey = (ShareUpdateKey) record.key().message();
                responseData.setResults(Collections.singletonList(WriteShareGroupStateResponse.toResponseWriteStateResult(
                    recordKey.topicId(), Collections.singletonList(WriteShareGroupStateResponse.toResponsePartitionResult(
                        recordKey.partition())))));
                mapKey = SharePartitionKey.getInstance(recordKey.groupId(), recordKey.topicId(), recordKey.partition());
            }

            if (shareStateMap.containsKey(mapKey) && shouldIncSnapshotEpoch) {
                ShareGroupOffset oldValue = shareStateMap.get(mapKey);
                ((ShareSnapshotValue) record.value().message()).setSnapshotEpoch(oldValue.snapshotEpoch() + 1);  // increment the snapshot epoch
            }
            validRecords.add(record); // this will have updated snapshot epoch and on replay the value will trickle down to the map
        }

        return new CoordinatorResult<>(validRecords, responseData);
    }

    /**
     * This method finds the ShareSnapshotValue record corresponding to the requested topic partition from the
     * in-memory state of coordinator shard, the shareStateMap.
     * <p>
     * This method as called by the ShareCoordinatorService will be provided with
     * the request data which covers only key i.e. group1:topic1:partition1. The implementation
     * below was done keeping this in mind.
     *
     * @param request - WriteShareGroupStateRequestData for a single key
     * @param offset  - offset to read from the __share_group_state topic partition
     * @return CoordinatorResult(records, response)
     */
    public ReadShareGroupStateResponseData readState(ReadShareGroupStateRequestData request, Long offset) {
        // records to read (with the key of snapshot type), response to caller
        // only one key will be there in the request by design
        Optional<ReadShareGroupStateResponseData> error = maybeGetReadStateError(request, offset);
        if (error.isPresent()) {
            return error.get();
        }

        Uuid topicId = request.topics().get(0).topicId();
        int partition = request.topics().get(0).partitions().get(0).partition();
        int leaderEpoch = request.topics().get(0).partitions().get(0).leaderEpoch();

        SharePartitionKey coordinatorKey = SharePartitionKey.getInstance(request.groupId(), topicId, partition);

        if (!shareStateMap.containsKey(coordinatorKey)) {
            return ReadShareGroupStateResponse.toResponseData(
                topicId,
                partition,
                PartitionFactory.DEFAULT_START_OFFSET,
                PartitionFactory.DEFAULT_STATE_EPOCH,
                Collections.emptyList()
            );
        }

        ShareGroupOffset offsetValue = shareStateMap.get(coordinatorKey, offset);

        if (offsetValue == null) {
            // Returning an error response as the snapshot value was not found
            return ReadShareGroupStateResponse.toErrorResponseData(
                topicId,
                partition,
                Errors.UNKNOWN_SERVER_ERROR,
                "Data not found for topic {}, partition {} for group {}, in the in-memory state of share coordinator"
            );
        }

        List<ReadShareGroupStateResponseData.StateBatch> stateBatches = (offsetValue.stateBatches() != null && !offsetValue.stateBatches().isEmpty()) ?
            offsetValue.stateBatches().stream().map(
                stateBatch -> new ReadShareGroupStateResponseData.StateBatch()
                    .setFirstOffset(stateBatch.firstOffset())
                    .setLastOffset(stateBatch.lastOffset())
                    .setDeliveryState(stateBatch.deliveryState())
                    .setDeliveryCount(stateBatch.deliveryCount())
            ).collect(java.util.stream.Collectors.toList()) : Collections.emptyList();

        // Updating the leader map with the new leader epoch
        leaderEpochMap.put(coordinatorKey, leaderEpoch);

        // Returning the successfully retrieved snapshot value
        return ReadShareGroupStateResponse.toResponseData(topicId, partition, offsetValue.startOffset(), offsetValue.stateEpoch(), stateBatches);
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
        if (leaderEpochMap.containsKey(mapKey) && leaderEpochMap.get(mapKey) > partitionData.leaderEpoch()) {
            log.error("Request leader epoch smaller than last recorded.");
            return Optional.of(getWriteErrorResponse(Errors.FENCED_LEADER_EPOCH, null, topicId, partitionId));
        }
        if (stateEpochMap.containsKey(mapKey) && stateEpochMap.get(mapKey) > partitionData.stateEpoch()) {
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

    private Optional<ReadShareGroupStateResponseData> maybeGetReadStateError(ReadShareGroupStateRequestData request, Long offset) {
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
        if (leaderEpochMap.containsKey(mapKey, offset) && leaderEpochMap.get(mapKey, offset) > partitionData.leaderEpoch()) {
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
        // snapshot epoch should be same as last share snapshot
        // state epoch is not present
        List<PersisterStateBatch> currentBatches = soFar.stateBatches();
        long newStartOffset = newData.startOffset() == -1 ? soFar.startOffset() : newData.startOffset();
        int newLeaderEpoch = newData.leaderEpoch() == -1 ? soFar.leaderEpoch() : newData.leaderEpoch();

        return new ShareGroupOffset.Builder()
            .setSnapshotEpoch(soFar.snapshotEpoch())
            .setStateEpoch(soFar.stateEpoch())
            .setStartOffset(newStartOffset)
            .setLeaderEpoch(newLeaderEpoch)
            .setStateBatches(combineStateBatches(currentBatches, newData.stateBatches().stream()
                .map(ShareCoordinatorShard::toPersisterStateBatch)
                .collect(Collectors.toList()), newStartOffset))
            .build();
    }

    /**
     * Util method which takes in 2 collections containing {@link PersisterStateBatch}
     * and the startOffset.
     * This method checks any overlap between current state batches and new state batches.
     * Based on various conditions it creates new non-overlapping records preferring new batches.
     * Finally, it removes any batches where the lastOffset < startOffset, if the startOffset > -1 and
     * merges any contiguous intervals with same state.
     * @param batchesSoFar - collection containing current soft state of batches
     * @param newBatches - collection containing batches in incoming request
     * @param startOffset - startOffset to consider when removing old batches.
     * @return List containing combined batches
     */
    // visibility for testing
    static List<PersisterStateBatch> combineStateBatches(
        List<PersisterStateBatch> batchesSoFar,
        List<PersisterStateBatch> newBatches,
        long startOffset
    ) {
        List<PersisterStateBatch> combinedList = new ArrayList<>(batchesSoFar.size() + newBatches.size());
        combinedList.addAll(batchesSoFar);
        combinedList.addAll(newBatches);

        // sort keeping delivery state in mind
        combinedList.sort(PersisterStateBatch::compareTo);

        return mergeBatches(
            pruneBatches(
                combinedList,
                startOffset
            )
        );
    }

    private static class BatchOverlapState {
        private final PersisterStateBatch last;
        private final PersisterStateBatch candidate;
        private final List<PersisterStateBatch> nonOverlapping;
        public static final BatchOverlapState SENTINEL = new BatchOverlapState(null, null, Collections.emptyList());

        public BatchOverlapState(
            PersisterStateBatch last,
            PersisterStateBatch candidate,
            List<PersisterStateBatch> nonOverlapping
        ) {
            this.last = last;
            this.candidate = candidate;
            this.nonOverlapping = nonOverlapping;
        }

        public PersisterStateBatch last() {
            return last;
        }

        public PersisterStateBatch candidate() {
            return candidate;
        }

        public List<PersisterStateBatch> nonOverlapping() {
            return nonOverlapping;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof BatchOverlapState)) return false;
            BatchOverlapState that = (BatchOverlapState) o;
            return Objects.equals(last, that.last) && Objects.equals(candidate, that.candidate) && Objects.equals(nonOverlapping, that.nonOverlapping);
        }

        @Override
        public int hashCode() {
            return Objects.hash(last, candidate, nonOverlapping);
        }
    }

    static List<PersisterStateBatch> mergeBatches(List<PersisterStateBatch> batches) {
        if (batches.size() < 2) {
            return batches;
        }
        TreeSet<PersisterStateBatch> sortedBatches = new TreeSet<>(batches);
        List<PersisterStateBatch> finalBatches = new ArrayList<>(batches.size() * 2); // heuristic size

        BatchOverlapState overlapState = getOverlappingState(sortedBatches);

        while (overlapState != BatchOverlapState.SENTINEL) {
            PersisterStateBatch last = overlapState.last();
            PersisterStateBatch candidate = overlapState.candidate();
            
            // remove non overlapping prefix from sortedBatches,
            // will make getting next overlapping pair efficient
            // as a prefix batch which is non overlapping will only
            // be checked once.
            if (overlapState.nonOverlapping() != null) {
                overlapState.nonOverlapping().forEach(sortedBatches::remove);
                finalBatches.addAll(overlapState.nonOverlapping());
            }

            if (candidate == null) {
                overlapState = BatchOverlapState.SENTINEL;
                continue;
            }

            // overlap and same state (last.firstOffset <= candidate.firstOffset) due to sort
            // covers:
            // case:        1        2          3            4          5           6          7 (contiguous)
            // last:        ______   _______    _______      _______   _______   ________    _______
            // candidate:   ______   ____       __________     ___        ____       _______        _______
            if (compareBatchState(candidate, last) == 0) {
                sortedBatches.remove(last);  // remove older smaller interval
                sortedBatches.remove(candidate);

                last = new PersisterStateBatch(
                    last.firstOffset(),
                    // cover cases
                    // last:      ______   ________       _________
                    // candidate:   ___       __________           _____
                    Math.max(candidate.lastOffset(), last.lastOffset()),
                    last.deliveryState(),
                    last.deliveryCount()
                );

                sortedBatches.add(last);
            } else if (candidate.firstOffset() <= last.lastOffset()) { // non-contiguous overlap
                // overlap and different state
                // covers:
                // case:        1        2 (NP)      3            4          5           6
                // last:        ______   _______    _______      _______    _______     ________
                // candidate:   ______   ____       _________      ____        ____          _______
                // max batches: 1           2       2                3          2            2
                // min batches: 1           1       1                1          1            2

                // remove both last and candidate for easier
                // assessment about adding batches to sortedBatches
                sortedBatches.remove(last);
                sortedBatches.remove(candidate);

                if (candidate.firstOffset() == last.firstOffset()) {
                    if (candidate.lastOffset() == last.lastOffset()) {  // case 1
                        // candidate can never have lower or equal priority
                        // since sortedBatches order takes that into account.
                        sortedBatches.add(candidate);
                    } else {
                        // case 2 is not possible with TreeSet. It is symmetric to case 3.
                        // case 3
                        if (compareBatchState(candidate, last) < 0) {
                            sortedBatches.add(last);
                            sortedBatches.add(new PersisterStateBatch(
                                last.lastOffset() + 1,
                                candidate.lastOffset(),
                                candidate.deliveryState(),
                                candidate.deliveryCount()
                            ));
                        } else {
                            // candidate priority is >= last
                            sortedBatches.add(candidate);
                        }
                    }
                } else {    // candidate.firstOffset() > last.firstOffset()
                    if (candidate.lastOffset() < last.lastOffset()) {    // case 4
                        if (compareBatchState(candidate, last) < 0) {
                            sortedBatches.add(last);
                        } else {
                            sortedBatches.add(new PersisterStateBatch(
                                last.firstOffset(),
                                candidate.firstOffset() - 1,
                                last.deliveryState(),
                                last.deliveryCount()
                            ));

                            sortedBatches.add(candidate);

                            sortedBatches.add(new PersisterStateBatch(
                                candidate.lastOffset() + 1,
                                last.lastOffset(),
                                last.deliveryState(),
                                last.deliveryCount()
                            ));
                        }
                    } else if (candidate.lastOffset() == last.lastOffset()) {    // case 5
                        if (compareBatchState(candidate, last) < 0) {
                            sortedBatches.add(last);
                        } else {
                            sortedBatches.add(new PersisterStateBatch(
                                last.firstOffset(),
                                candidate.firstOffset() - 1,
                                last.deliveryState(),
                                last.deliveryCount()
                            ));

                            sortedBatches.add(candidate);
                        }
                    } else {    // case 6
                        if (compareBatchState(candidate, last) < 0) {
                            sortedBatches.add(last);

                            sortedBatches.add(new PersisterStateBatch(
                                last.lastOffset() + 1,
                                candidate.lastOffset(),
                                candidate.deliveryState(),
                                candidate.deliveryCount()
                            ));
                        } else {
                            // candidate has higher priority
                            sortedBatches.add(new PersisterStateBatch(
                                last.firstOffset(),
                                candidate.firstOffset() - 1,
                                last.deliveryState(),
                                last.deliveryCount()
                            ));

                            sortedBatches.add(candidate);
                        }
                    }
                }
            }
            overlapState = getOverlappingState(sortedBatches);
        }
        finalBatches.addAll(sortedBatches);   // some non overlapping batches might have remained
        return finalBatches;
    }

    /**
     * Accepts a sorted set of state batches and finds the first 2 batches which overlap.
     * Overlap means that they have some offsets in common or they are contiguous with the same state.
     * Along with the 2 overlapping batches, also returns a list of non overlapping intervals
     * prefixing them. For example
     * _____ ____  _____ _____      _____
     *                      ______     __
     * <---------------> <-------->
     *  non-`overlapping   1st overlapping pair
     *
     * @param sortedBatches - TreeSet representing sorted set of {@link PersisterStateBatch}
     * @return object of {@link BatchOverlapState} representing overlapping pair and non-overlapping prefix
     */
    private static BatchOverlapState getOverlappingState(TreeSet<PersisterStateBatch> sortedBatches) {
        if (sortedBatches == null || sortedBatches.isEmpty()) {
            return BatchOverlapState.SENTINEL;
        }
        Iterator<PersisterStateBatch> iter = sortedBatches.iterator();
        PersisterStateBatch last = iter.next();
        List<PersisterStateBatch> nonOverlapping = new ArrayList<>(sortedBatches.size());
        while (iter.hasNext()) {
            PersisterStateBatch candidate = iter.next();
            if (candidate.firstOffset() <= last.lastOffset() || // overlap
                last.lastOffset() + 1 == candidate.firstOffset() && compareBatchState(last, candidate) == 0) {  // contiguous
                return new BatchOverlapState(
                    last,
                    candidate,
                    nonOverlapping
                );
            }
            nonOverlapping.add(last);
            last = candidate;
        }
        return new BatchOverlapState(null, null, nonOverlapping);
    }

    /**
     * Compares the state of 2 batches i.e. the deliveryCount and deliverState.
     * Uses standard compareTo contract x < y => +int, x > y => -int, x == y => 0
     * @param b1 - PersisterStateBatch to compare
     * @param b2 - PersisterStateBatch to compare
     * @return int representing comparison result.
     */
    private static int compareBatchState(PersisterStateBatch b1, PersisterStateBatch b2) {
        int deltaCount = Short.compare(b1.deliveryCount(), b2.deliveryCount());

        // Delivery state could be:
        // 0 - AVAILABLE (non-terminal)
        // 1 - ACQUIRED - should not be persisted yet
        // 2 - ACKNOWLEDGED (terminal)
        // 3 - ARCHIVING - not implemented in KIP-932 - non-terminal - leads only to ARCHIVED
        // 4 - ARCHIVED (terminal)

        if (deltaCount == 0) {   // same delivery count
            return Byte.compare(b1.deliveryState(), b2.deliveryState());
        }
        return deltaCount;
    }

    // Any batches where the last offset is < the current start offset
    // are now expired. We should remove them from the persister.
    private static List<PersisterStateBatch> pruneBatches(List<PersisterStateBatch> batches, long startOffset) {
        if (startOffset != -1) {
            List<PersisterStateBatch> prunedList = new ArrayList<>(batches.size());
            batches.forEach(batch -> {
                if (batch.firstOffset() >= startOffset) {
                    // covers:
                    //   ______
                    // | -> start offset
                    prunedList.add(batch);
                } else if (batch.lastOffset() >= startOffset) {
                    // covers:
                    //  ________
                    //       | -> start offset
                    prunedList.add(new PersisterStateBatch(startOffset, batch.lastOffset(), batch.deliveryState(), batch.deliveryCount()));
                }
                // in all other cases, the batch is completely expired.
            });
            return prunedList;
        }
        return batches;
    }

    private static PersisterStateBatch toPersisterStateBatch(ShareUpdateValue.StateBatch batch) {
        return new PersisterStateBatch(
            batch.firstOffset(),
            batch.lastOffset(),
            batch.deliveryState(),
            batch.deliveryCount()
        );
    }

    private static ApiMessage messageOrNull(ApiMessageAndVersion apiMessageAndVersion) {
        if (apiMessageAndVersion == null) {
            return null;
        } else {
            return apiMessageAndVersion.message();
        }
    }
}
