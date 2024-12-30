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
package kafka.server.share;

import kafka.server.ReplicaManager;
import kafka.server.share.SharePartitionManager.SharePartitionListener;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.CoordinatorNotAvailableException;
import org.apache.kafka.common.errors.FencedStateEpochException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.InvalidRecordStateException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.message.ShareFetchResponseData.AcquiredRecords;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.group.GroupConfig;
import org.apache.kafka.coordinator.group.GroupConfigManager;
import org.apache.kafka.server.share.acknowledge.ShareAcknowledgementBatch;
import org.apache.kafka.server.share.fetch.DelayedShareFetchGroupKey;
import org.apache.kafka.server.share.fetch.DelayedShareFetchKey;
import org.apache.kafka.server.share.fetch.ShareAcquiredRecords;
import org.apache.kafka.server.share.persister.GroupTopicPartitionData;
import org.apache.kafka.server.share.persister.PartitionAllData;
import org.apache.kafka.server.share.persister.PartitionErrorData;
import org.apache.kafka.server.share.persister.PartitionFactory;
import org.apache.kafka.server.share.persister.PartitionIdLeaderEpochData;
import org.apache.kafka.server.share.persister.PartitionStateBatchData;
import org.apache.kafka.server.share.persister.Persister;
import org.apache.kafka.server.share.persister.PersisterStateBatch;
import org.apache.kafka.server.share.persister.ReadShareGroupStateParameters;
import org.apache.kafka.server.share.persister.TopicData;
import org.apache.kafka.server.share.persister.WriteShareGroupStateParameters;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.server.util.timer.Timer;
import org.apache.kafka.server.util.timer.TimerTask;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static kafka.server.share.ShareFetchUtils.offsetForEarliestTimestamp;
import static kafka.server.share.ShareFetchUtils.offsetForLatestTimestamp;

/**
 * The SharePartition is used to track the state of a partition that is shared between multiple
 * consumers. The class maintains the state of the records that have been fetched from the leader
 * and are in-flight.
 */
public class SharePartition {

    private static final Logger log = LoggerFactory.getLogger(SharePartition.class);

    /**
     * empty member id used to indicate when a record is not acquired by any member.
     */
    static final String EMPTY_MEMBER_ID = Uuid.ZERO_UUID.toString();

    /**
     * The SharePartitionState is used to track the state of the share partition. The state of the
     * share partition determines if the partition is ready to receive requests, be initialized with
     * persisted state, or has failed to initialize.
     */
    // Visible for testing
    enum SharePartitionState {
        /**
         * The share partition is empty and has not been initialized with persisted state.
         */
        EMPTY,
        /**
         * The share partition is initializing with persisted state.
         */
        INITIALIZING,
        /**
         * The share partition is active and ready to serve requests.
         */
        ACTIVE,
        /**
         * The share partition failed to initialize with persisted state.
         */
        FAILED,
        /**
         * The share partition is fenced and cannot be used.
         */
        FENCED
    }

    /**
     * The RecordState is used to track the state of a record that has been fetched from the leader.
     * The state of the records determines if the records should be re-delivered, move the next fetch
     * offset, or be state persisted to disk.
     */
    // Visible for testing
    enum RecordState {
        AVAILABLE((byte) 0),
        ACQUIRED((byte) 1),
        ACKNOWLEDGED((byte) 2),
        ARCHIVED((byte) 4);

        public final byte id;

        RecordState(byte id) {
            this.id = id;
        }

        /**
         * Validates that the <code>newState</code> is one of the valid transition from the current
         * {@code RecordState}.
         *
         * @param newState State into which requesting to transition; must be non-<code>null</code>
         *
         * @return {@code RecordState} <code>newState</code> if validation succeeds. Returning
         *         <code>newState</code> helps state assignment chaining.
         *
         * @throws IllegalStateException if the state transition validation fails.
         */
        public RecordState validateTransition(RecordState newState) throws IllegalStateException {
            Objects.requireNonNull(newState, "newState cannot be null");
            if (this == newState) {
                throw new IllegalStateException("The state transition is invalid as the new state is"
                    + "the same as the current state");
            }

            if (this == ACKNOWLEDGED || this == ARCHIVED) {
                throw new IllegalStateException("The state transition is invalid from the current state: " + this);
            }

            if (this == AVAILABLE && newState != ACQUIRED) {
                throw new IllegalStateException("The state can only be transitioned to ACQUIRED from AVAILABLE");
            }

            // Either the transition is from Available -> Acquired or from Acquired -> Available/
            // Acknowledged/Archived.
            return newState;
        }

        public static RecordState forId(byte id) {
            switch (id) {
                case 0:
                    return AVAILABLE;
                case 1:
                    return ACQUIRED;
                case 2:
                    return ACKNOWLEDGED;
                case 4:
                    return ARCHIVED;
                default:
                    throw new IllegalArgumentException("Unknown record state id: " + id);
            }
        }
    }

    /**
     * The group id of the share partition belongs to.
     */
    private final String groupId;

    /**
     * The topic id partition of the share partition.
     */
    private final TopicIdPartition topicIdPartition;

    /**
     * The leader epoch is used to track the partition epoch.
     */
    private final int leaderEpoch;

    /**
     * The in-flight record is used to track the state of a record that has been fetched from the
     * leader. The state of the record is used to determine if the record should be re-fetched or if it
     * can be acknowledged or archived. Once share partition start offset is moved then the in-flight
     * records prior to the start offset are removed from the cache. The cache holds data against the
     * first offset of the in-flight batch.
     */
    private final NavigableMap<Long, InFlightBatch> cachedState;

    /**
     * The lock is used to synchronize access to the in-flight records. The lock is used to ensure that
     * the in-flight records are accessed in a thread-safe manner.
     */
    private final ReadWriteLock lock;

    /**
     * The find next fetch offset is used to indicate if the next fetch offset should be recomputed.
     */
    private final AtomicBoolean findNextFetchOffset;

    /**
     * The lock to ensure that the same share partition does not enter a fetch queue
     * while another one is being fetched within the queue.
     */
    private final AtomicBoolean fetchLock;

    /**
     * The max in-flight messages is used to limit the number of records that can be in-flight at any
     * given time. The max in-flight messages is used to prevent the consumer from fetching too many
     * records from the leader and running out of memory.
     */
    private final int maxInFlightMessages;

    /**
     * The max delivery count is used to limit the number of times a record can be delivered to the
     * consumer. The max delivery count is used to prevent the consumer re-delivering the same record
     * indefinitely.
     */
    private final int maxDeliveryCount;

    /**
     * The group config manager is used to retrieve the values for dynamic group configurations
     */
    private final GroupConfigManager groupConfigManager;

    /**
     * This is the default value which is used unless the group has a configuration which overrides it.
     * The record lock duration is used to limit the duration for which a consumer can acquire a record.
     * Once this time period is elapsed, the record will be made available or archived depending on the delivery count.
     */
    private final int defaultRecordLockDurationMs;

    /**
     * Timer is used to implement acquisition lock on records that guarantees the movement of records from
     * acquired to available/archived state upon timeout
     */
    private final Timer timer;

    /**
     * Time is used to get the currentTime.
     */
    private final Time time;

    /**
     * The persister is used to persist the state of the share partition to disk.
     */
    private final Persister persister;

    /**
     * The listener is used to notify the share partition manager when the share partition state changes.
     */
    private final SharePartitionListener listener;

    /**
     * The share partition start offset specifies the partition start offset from which the records
     * are cached in the cachedState of the sharePartition.
     */
    private long startOffset;

    /**
     * The share partition end offset specifies the partition end offset from which the records
     * are already fetched.
     */
    private long endOffset;

    /**
     * We maintain the latest fetch offset and its metadata to estimate the minBytes requirement more efficiently.
     */
    private final OffsetMetadata fetchOffsetMetadata;

    /**
     * The state epoch is used to track the version of the state of the share partition.
     */
    private int stateEpoch;

    /**
     * The partition state is used to track the state of the share partition.
     */
    private SharePartitionState partitionState;

    /**
     * The replica manager is used to check to see if any delayed share fetch request can be completed because of data
     * availability due to acquisition lock timeout.
     */
    private final ReplicaManager replicaManager;

    SharePartition(
        String groupId,
        TopicIdPartition topicIdPartition,
        int leaderEpoch,
        int maxInFlightMessages,
        int maxDeliveryCount,
        int defaultRecordLockDurationMs,
        Timer timer,
        Time time,
        Persister persister,
        ReplicaManager replicaManager,
        GroupConfigManager groupConfigManager,
        SharePartitionListener listener
    ) {
        this(groupId, topicIdPartition, leaderEpoch, maxInFlightMessages, maxDeliveryCount, defaultRecordLockDurationMs,
            timer, time, persister, replicaManager, groupConfigManager, SharePartitionState.EMPTY, listener);
    }

    SharePartition(
        String groupId,
        TopicIdPartition topicIdPartition,
        int leaderEpoch,
        int maxInFlightMessages,
        int maxDeliveryCount,
        int defaultRecordLockDurationMs,
        Timer timer,
        Time time,
        Persister persister,
        ReplicaManager replicaManager,
        GroupConfigManager groupConfigManager,
        SharePartitionState sharePartitionState,
        SharePartitionListener listener
    ) {
        this.groupId = groupId;
        this.topicIdPartition = topicIdPartition;
        this.leaderEpoch = leaderEpoch;
        this.maxInFlightMessages = maxInFlightMessages;
        this.maxDeliveryCount = maxDeliveryCount;
        this.cachedState = new ConcurrentSkipListMap<>();
        this.lock = new ReentrantReadWriteLock();
        this.findNextFetchOffset = new AtomicBoolean(false);
        this.fetchLock = new AtomicBoolean(false);
        this.defaultRecordLockDurationMs = defaultRecordLockDurationMs;
        this.timer = timer;
        this.time = time;
        this.persister = persister;
        this.partitionState = sharePartitionState;
        this.replicaManager = replicaManager;
        this.groupConfigManager = groupConfigManager;
        this.fetchOffsetMetadata = new OffsetMetadata();
        this.listener = listener;
    }

    /**
     * May initialize the share partition by reading the state from the persister. The share partition
     * is initialized only if the state is in the EMPTY state. If the share partition is in ACTIVE state,
     * the method completes the future successfully. For other states, the method completes the future
     * with exception, which might be re-triable.
     *
     * @return The method returns a future which is completed when the share partition is initialized
     *         or completes with an exception if the share partition is in non-initializable state.
     */
    public CompletableFuture<Void> maybeInitialize() {
        log.debug("Maybe initialize share partition: {}-{}", groupId, topicIdPartition);
        // Check if the share partition is already initialized.
        try {
            if (initializedOrThrowException()) return CompletableFuture.completedFuture(null);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }

        // If code reaches here then the share partition is not initialized. Initialize the share partition.
        // All the pending requests should wait to get completed before the share partition is initialized.
        // Attain lock while updating the state to avoid any concurrent requests to be processed.
        try {
            if (!emptyToInitialState()) return CompletableFuture.completedFuture(null);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }

        // The share partition is not initialized, hence try to initialize it. There shall be only one
        // request trying to initialize the share partition.
        CompletableFuture<Void> future = new CompletableFuture<>();
        // Initialize the share partition by reading the state from the persister.
        persister.readState(new ReadShareGroupStateParameters.Builder()
            .setGroupTopicPartitionData(new GroupTopicPartitionData.Builder<PartitionIdLeaderEpochData>()
                .setGroupId(this.groupId)
                .setTopicsData(Collections.singletonList(new TopicData<>(topicIdPartition.topicId(),
                    Collections.singletonList(PartitionFactory.newPartitionIdLeaderEpochData(topicIdPartition.partition(), leaderEpoch)))))
                .build())
            .build()
        ).whenComplete((result, exception) -> {
            Throwable throwable = null;
            lock.writeLock().lock();
            try {
                if (exception != null) {
                    log.error("Failed to initialize the share partition: {}-{}", groupId, topicIdPartition, exception);
                    throwable = exception;
                    return;
                }

                if (result == null || result.topicsData() == null || result.topicsData().size() != 1) {
                    log.error("Failed to initialize the share partition: {}-{}. Invalid state found: {}.",
                        groupId, topicIdPartition, result);
                    throwable = new IllegalStateException(String.format("Failed to initialize the share partition %s-%s", groupId, topicIdPartition));
                    return;
                }

                TopicData<PartitionAllData> state = result.topicsData().get(0);
                if (state.topicId() != topicIdPartition.topicId() || state.partitions().size() != 1) {
                    log.error("Failed to initialize the share partition: {}-{}. Invalid topic partition response: {}.",
                        groupId, topicIdPartition, result);
                    throwable = new IllegalStateException(String.format("Failed to initialize the share partition %s-%s", groupId, topicIdPartition));
                    return;
                }

                PartitionAllData partitionData = state.partitions().get(0);
                if (partitionData.partition() != topicIdPartition.partition()) {
                    log.error("Failed to initialize the share partition: {}-{}. Invalid partition response: {}.",
                        groupId, topicIdPartition, partitionData);
                    throwable = new IllegalStateException(String.format("Failed to initialize the share partition %s-%s", groupId, topicIdPartition));
                    return;
                }

                if (partitionData.errorCode() != Errors.NONE.code()) {
                    KafkaException ex = fetchPersisterError(partitionData.errorCode(), partitionData.errorMessage());
                    log.error("Failed to initialize the share partition: {}-{}. Exception occurred: {}.",
                        groupId, topicIdPartition, partitionData);
                    throwable = ex;
                    return;
                }

                startOffset = startOffsetDuringInitialization(partitionData.startOffset());
                stateEpoch = partitionData.stateEpoch();

                List<PersisterStateBatch> stateBatches = partitionData.stateBatches();
                for (PersisterStateBatch stateBatch : stateBatches) {
                    if (stateBatch.firstOffset() < startOffset) {
                        log.error("Invalid state batch found for the share partition: {}-{}. The base offset: {}"
                                + " is less than the start offset: {}.", groupId, topicIdPartition,
                            stateBatch.firstOffset(), startOffset);
                        throwable = new IllegalStateException(String.format("Failed to initialize the share partition %s-%s", groupId, topicIdPartition));
                        return;
                    }
                    InFlightBatch inFlightBatch = new InFlightBatch(EMPTY_MEMBER_ID, stateBatch.firstOffset(),
                        stateBatch.lastOffset(), RecordState.forId(stateBatch.deliveryState()), stateBatch.deliveryCount(), null);
                    cachedState.put(stateBatch.firstOffset(), inFlightBatch);
                }
                // Update the endOffset of the partition.
                if (!cachedState.isEmpty()) {
                    // If the cachedState is not empty, findNextFetchOffset flag is set to true so that any AVAILABLE records
                    // in the cached state are not missed
                    findNextFetchOffset.set(true);
                    endOffset = cachedState.lastEntry().getValue().lastOffset();
                    // In case the persister read state RPC result contains no AVAILABLE records, we can update cached state
                    // and start/end offsets.
                    maybeUpdateCachedStateAndOffsets();
                } else {
                    endOffset = startOffset;
                }
                // Set the partition state to Active and complete the future.
                partitionState = SharePartitionState.ACTIVE;
            } catch (Exception e) {
                throwable = e;
            } finally {
                boolean isFailed = throwable != null;
                if (isFailed) {
                    partitionState = SharePartitionState.FAILED;
                }
                // Release the lock.
                lock.writeLock().unlock();
                // Complete the future.
                if (isFailed) {
                    future.completeExceptionally(throwable);
                } else {
                    future.complete(null);
                }
            }
        });

        return future;
    }

    /**
     * The next fetch offset is used to determine the next offset that should be fetched from the leader.
     * The offset should be the next offset after the last fetched batch but there could be batches/
     * offsets that are either released by acknowledge API or lock timed out hence the next fetch
     * offset might be different from the last batch next offset. Hence, method checks if the next
     * fetch offset should be recomputed else returns the last computed next fetch offset.
     *
     * @return The next fetch offset that should be fetched from the leader.
     */
    public long nextFetchOffset() {
        /*
        The logic for determining the next offset to fetch data from a Share Partition hinges on a
        flag called findNextFetchOffset. If this flag is set to true, then the next fetch offset
        should be re-computed, otherwise the next fetch offset is Share Partition End Offset + 1.
        The flag is set to true in the following cases:
        1. When some previously acquired records are acknowledged with type RELEASE.
        2. When the record lock duration expires for some acquired records.
        3. When some records are released on share session close.
        The re-computation of next fetch offset is done by iterating over the cachedState and finding
        the first available record. If no available record is found, then the next fetch offset is
        set to Share Partition End Offset + 1 and findNextFetchOffset flag is set to false.
        */
        lock.writeLock().lock();
        try {
            // When none of the records in the cachedState are in the AVAILABLE state, findNextFetchOffset will be false
            if (!findNextFetchOffset.get()) {
                if (cachedState.isEmpty() || startOffset > cachedState.lastEntry().getValue().lastOffset()) {
                    // 1. When cachedState is empty, endOffset is set to the next offset of the last
                    // offset removed from batch, which is the next offset to be fetched.
                    // 2. When startOffset has moved beyond the in-flight records, startOffset and
                    // endOffset point to the LSO, which is the next offset to be fetched.
                    return endOffset;
                } else {
                    return endOffset + 1;
                }
            }

            // If this piece of code is reached, it means that findNextFetchOffset is true
            if (cachedState.isEmpty() || startOffset > cachedState.lastEntry().getValue().lastOffset()) {
                // If cachedState is empty, there is no need of re-computing next fetch offset in future fetch requests.
                // Same case when startOffset has moved beyond the in-flight records, startOffset and endOffset point to the LSO
                // and the cached state is fresh.
                findNextFetchOffset.set(false);
                return endOffset;
            }

            long nextFetchOffset = -1;

            for (Map.Entry<Long, InFlightBatch> entry : cachedState.entrySet()) {
                // Check if the state is maintained per offset or batch. If the offsetState
                // is not maintained then the batch state is used to determine the offsets state.
                if (entry.getValue().offsetState() == null) {
                    if (entry.getValue().batchState() == RecordState.AVAILABLE) {
                        nextFetchOffset = entry.getValue().firstOffset();
                        break;
                    }
                } else {
                    // The offset state is maintained hence find the next available offset.
                    for (Map.Entry<Long, InFlightState> offsetState : entry.getValue().offsetState().entrySet()) {
                        if (offsetState.getValue().state == RecordState.AVAILABLE) {
                            nextFetchOffset = offsetState.getKey();
                            break;
                        }
                    }
                    // Break from the outer loop if updated.
                    if (nextFetchOffset != -1) {
                        break;
                    }
                }
            }

            // If nextFetchOffset is -1, then no AVAILABLE records are found in the cachedState, so there is no need of
            // re-computing next fetch offset in future fetch requests
            if (nextFetchOffset == -1) {
                findNextFetchOffset.set(false);
                nextFetchOffset = endOffset + 1;
            }
            return nextFetchOffset;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Acquire the fetched records for the share partition. The acquired records are added to the
     * in-flight records and the next fetch offset is updated to the next offset that should be
     * fetched from the leader.
     *
     * @param memberId           The member id of the client that is fetching the record.
     * @param maxFetchRecords    The maximum number of records that should be acquired, this is a soft
     *                           limit and the method might acquire more records than the maxFetchRecords,
     *                           if the records are already part of the same fetch batch.
     * @param fetchPartitionData The fetched records for the share partition.
     * @return The acquired records for the share partition.
     */
    @SuppressWarnings("cyclomaticcomplexity") // Consider refactoring to avoid suppression
    public ShareAcquiredRecords acquire(
        String memberId,
        int maxFetchRecords,
        FetchPartitionData fetchPartitionData
    ) {
        log.trace("Received acquire request for share partition: {}-{} memberId: {}", groupId, topicIdPartition, memberId);
        if (stateNotActive() || maxFetchRecords <= 0) {
            // Nothing to acquire.
            return ShareAcquiredRecords.empty();
        }

        RecordBatch lastBatch = fetchPartitionData.records.lastBatch().orElse(null);
        if (lastBatch == null) {
            // Nothing to acquire.
            return ShareAcquiredRecords.empty();
        }

        // We require the first batch of records to get the base offset. Stop parsing further
        // batches.
        RecordBatch firstBatch = fetchPartitionData.records.batches().iterator().next();
        lock.writeLock().lock();
        try {
            long baseOffset = firstBatch.baseOffset();
            // Find the floor batch record for the request batch. The request batch could be
            // for a subset of the in-flight batch i.e. cached batch of offset 10-14 and request batch
            // of 12-13. Hence, floor entry is fetched to find the sub-map.
            Map.Entry<Long, InFlightBatch> floorOffset = cachedState.floorEntry(baseOffset);
            // We might find a batch with floor entry but not necessarily that batch has an overlap,
            // if the request batch base offset is ahead of last offset from floor entry i.e. cached
            // batch of 10-14 and request batch of 15-18, though floor entry is found but no overlap.
            if (floorOffset != null && floorOffset.getValue().lastOffset() >= baseOffset) {
                baseOffset = floorOffset.getKey();
            }
            // Validate if the fetch records are already part of existing batches and if available.
            NavigableMap<Long, InFlightBatch> subMap = cachedState.subMap(baseOffset, true, lastBatch.lastOffset(), true);
            // No overlap with request offsets in the cache for in-flight records. Acquire the complete
            // batch.
            if (subMap.isEmpty()) {
                log.trace("No cached data exists for the share partition for requested fetch batch: {}-{}",
                    groupId, topicIdPartition);
                AcquiredRecords acquiredRecords = acquireNewBatchRecords(memberId, fetchPartitionData.records.batches(),
                    firstBatch.baseOffset(), lastBatch.lastOffset(), maxFetchRecords);
                return ShareAcquiredRecords.fromAcquiredRecords(acquiredRecords);
            }

            log.trace("Overlap exists with in-flight records. Acquire the records if available for"
                + " the share partition: {}-{}", groupId, topicIdPartition);
            List<AcquiredRecords> result = new ArrayList<>();
            // The acquired count is used to track the number of records acquired for the request.
            int acquiredCount = 0;
            // The fetched records are already part of the in-flight records. The records might
            // be available for re-delivery hence try acquiring same. The request batches could
            // be an exact match, subset or span over multiple already fetched batches.
            for (Map.Entry<Long, InFlightBatch> entry : subMap.entrySet()) {
                // If the acquired count is equal to the max fetch records then break the loop.
                if (acquiredCount >= maxFetchRecords) {
                    break;
                }

                InFlightBatch inFlightBatch = entry.getValue();
                // Compute if the batch is a full match.
                boolean fullMatch = checkForFullMatch(inFlightBatch, firstBatch.baseOffset(), lastBatch.lastOffset());

                if (!fullMatch || inFlightBatch.offsetState() != null) {
                    log.trace("Subset or offset tracked batch record found for share partition,"
                            + " batch: {} request offsets - first: {}, last: {} for the share"
                            + " partition: {}-{}", inFlightBatch, firstBatch.baseOffset(),
                        lastBatch.lastOffset(), groupId, topicIdPartition);
                    if (inFlightBatch.offsetState() == null) {
                        // Though the request is a subset of in-flight batch but the offset
                        // tracking has not been initialized yet which means that we could only
                        // acquire subset of offsets from the in-flight batch but only if the
                        // complete batch is available yet. Hence, do a pre-check to avoid exploding
                        // the in-flight offset tracking unnecessarily.
                        if (inFlightBatch.batchState() != RecordState.AVAILABLE || inFlightBatch.batchHasOngoingStateTransition()) {
                            log.trace("The batch is not available to acquire in share partition: {}-{}, skipping: {}"
                                    + " skipping offset tracking for batch as well.", groupId,
                                topicIdPartition, inFlightBatch);
                            continue;
                        }
                        // The request batch is a subset or per offset state is managed hence update
                        // the offsets state in the in-flight batch.
                        inFlightBatch.maybeInitializeOffsetStateUpdate();
                    }
                    // Do not send max fetch records to acquireSubsetBatchRecords as we want to acquire
                    // all the records from the batch as the batch will anyway be part of the file-records
                    // response batch.
                    int acquiredSubsetCount = acquireSubsetBatchRecords(memberId, firstBatch.baseOffset(), lastBatch.lastOffset(), inFlightBatch, result);
                    acquiredCount += acquiredSubsetCount;
                    continue;
                }

                // The in-flight batch is a full match hence change the state of the complete batch.
                if (inFlightBatch.batchState() != RecordState.AVAILABLE || inFlightBatch.batchHasOngoingStateTransition()) {
                    log.trace("The batch is not available to acquire in share partition: {}-{}, skipping: {}",
                        groupId, topicIdPartition, inFlightBatch);
                    continue;
                }

                InFlightState updateResult = inFlightBatch.tryUpdateBatchState(RecordState.ACQUIRED, true, maxDeliveryCount, memberId);
                if (updateResult == null) {
                    log.info("Unable to acquire records for the batch: {} in share partition: {}-{}",
                        inFlightBatch, groupId, topicIdPartition);
                    continue;
                }
                // Schedule acquisition lock timeout for the batch.
                AcquisitionLockTimerTask acquisitionLockTimeoutTask = scheduleAcquisitionLockTimeout(memberId, inFlightBatch.firstOffset(), inFlightBatch.lastOffset());
                // Set the acquisition lock timeout task for the batch.
                inFlightBatch.updateAcquisitionLockTimeout(acquisitionLockTimeoutTask);

                result.add(new AcquiredRecords()
                    .setFirstOffset(inFlightBatch.firstOffset())
                    .setLastOffset(inFlightBatch.lastOffset())
                    .setDeliveryCount((short) inFlightBatch.batchDeliveryCount()));
                acquiredCount += (int) (inFlightBatch.lastOffset() - inFlightBatch.firstOffset() + 1);
            }

            // Some of the request offsets are not found in the fetched batches. Acquire the
            // missing records as well.
            if (acquiredCount < maxFetchRecords && subMap.lastEntry().getValue().lastOffset() < lastBatch.lastOffset()) {
                log.trace("There exists another batch which needs to be acquired as well");
                AcquiredRecords acquiredRecords = acquireNewBatchRecords(memberId, fetchPartitionData.records.batches(),
                    subMap.lastEntry().getValue().lastOffset() + 1,
                    lastBatch.lastOffset(), maxFetchRecords - acquiredCount);
                result.add(acquiredRecords);
                acquiredCount += (int) (acquiredRecords.lastOffset() - acquiredRecords.firstOffset() + 1);
            }
            return new ShareAcquiredRecords(result, acquiredCount);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Acknowledge the fetched records for the share partition. The accepted batches are removed
     * from the in-flight records once persisted. The next fetch offset is updated to the next offset
     * that should be fetched from the leader, if required.
     *
     * @param memberId               The member id of the client that is fetching the record.
     * @param acknowledgementBatches The acknowledgement batch list for the share partition.
     * @return A future which is completed when the records are acknowledged.
     */
    public CompletableFuture<Void> acknowledge(
        String memberId,
        List<ShareAcknowledgementBatch> acknowledgementBatches
    ) {
        log.trace("Acknowledgement batch request for share partition: {}-{}", groupId, topicIdPartition);

        CompletableFuture<Void> future = new CompletableFuture<>();
        Throwable throwable = null;
        List<InFlightState> updatedStates = new ArrayList<>();
        List<PersisterStateBatch> stateBatches = new ArrayList<>();
        lock.writeLock().lock();
        try {
            // Avoided using enhanced for loop as need to check if the last batch have offsets
            // in the range.
            for (ShareAcknowledgementBatch batch : acknowledgementBatches) {
                // Client can either send a single entry in acknowledgeTypes which represents the state
                // of the complete batch or can send individual offsets state.
                Map<Long, RecordState> recordStateMap;
                try {
                    recordStateMap = fetchRecordStateMapForAcknowledgementBatch(batch);
                } catch (IllegalArgumentException e) {
                    log.debug("Invalid acknowledge type: {} for share partition: {}-{}",
                        batch.acknowledgeTypes(), groupId, topicIdPartition);
                    throwable = new InvalidRequestException("Invalid acknowledge type: " + batch.acknowledgeTypes());
                    break;
                }

                if (batch.lastOffset() < startOffset) {
                    log.trace("All offsets in the acknowledgement batch {} are already archived: {}-{}",
                        batch, groupId, topicIdPartition);
                    continue;
                }

                // Fetch the sub-map from the cached map for the batch to acknowledge. The sub-map can
                // be a full match, subset or spans over multiple fetched batches.
                NavigableMap<Long, InFlightBatch> subMap;
                try {
                    subMap = fetchSubMapForAcknowledgementBatch(batch);
                } catch (InvalidRecordStateException | InvalidRequestException e) {
                    throwable = e;
                    break;
                }

                // Acknowledge the records for the batch.
                Optional<Throwable> ackThrowable = acknowledgeBatchRecords(
                    memberId,
                    batch,
                    recordStateMap,
                    subMap,
                    updatedStates,
                    stateBatches
                );

                if (ackThrowable.isPresent()) {
                    throwable = ackThrowable.get();
                    break;
                }
            }

            // If the acknowledgement is successful then persist state, complete the state transition
            // and update the cached state for start offset. Else rollback the state transition.
            rollbackOrProcessStateUpdates(future, throwable, updatedStates, stateBatches);
        } finally {
            lock.writeLock().unlock();
        }

        return future;
    }

    /**
     * Release the acquired records for the share partition. The next fetch offset is updated to the next offset
     * that should be fetched from the leader.
     *
     * @param memberId The member id of the client whose records shall be released.
     * @return A future which is completed when the records are released.
     */
    public CompletableFuture<Void> releaseAcquiredRecords(String memberId) {
        log.trace("Release acquired records request for share partition: {}-{} memberId: {}", groupId, topicIdPartition, memberId);

        CompletableFuture<Void> future = new CompletableFuture<>();
        Throwable throwable = null;
        List<InFlightState> updatedStates = new ArrayList<>();
        List<PersisterStateBatch> stateBatches = new ArrayList<>();

        lock.writeLock().lock();
        try {
            RecordState recordState = RecordState.AVAILABLE;
            // Iterate over multiple fetched batches. The state can vary per offset entry
            for (Map.Entry<Long, InFlightBatch> entry : cachedState.entrySet()) {
                InFlightBatch inFlightBatch = entry.getValue();

                if (inFlightBatch.offsetState() == null
                        && inFlightBatch.batchState() == RecordState.ACQUIRED
                        && inFlightBatch.batchMemberId().equals(memberId)
                        && checkForStartOffsetWithinBatch(inFlightBatch.firstOffset(), inFlightBatch.lastOffset())) {
                    // For the case when batch.firstOffset < start offset <= batch.lastOffset, we will be having some
                    // acquired records that need to move to archived state despite their delivery count.
                    inFlightBatch.maybeInitializeOffsetStateUpdate();
                }

                if (inFlightBatch.offsetState() != null) {
                    Optional<Throwable> releaseAcquiredRecordsThrowable = releaseAcquiredRecordsForPerOffsetBatch(memberId, inFlightBatch, recordState, updatedStates, stateBatches);
                    if (releaseAcquiredRecordsThrowable.isPresent()) {
                        throwable = releaseAcquiredRecordsThrowable.get();
                        break;
                    }
                    continue;
                }
                Optional<Throwable> releaseAcquiredRecordsThrowable = releaseAcquiredRecordsForCompleteBatch(memberId, inFlightBatch, recordState, updatedStates, stateBatches);
                if (releaseAcquiredRecordsThrowable.isPresent()) {
                    throwable = releaseAcquiredRecordsThrowable.get();
                    break;
                }
            }

            // If the release acquired records is successful then persist state, complete the state transition
            // and update the cached state for start offset. Else rollback the state transition.
            rollbackOrProcessStateUpdates(future, throwable, updatedStates, stateBatches);
        } finally {
            lock.writeLock().unlock();
        }
        return future;
    }

    private Optional<Throwable> releaseAcquiredRecordsForPerOffsetBatch(String memberId,
                                                                        InFlightBatch inFlightBatch,
                                                                        RecordState recordState,
                                                                        List<InFlightState> updatedStates,
                                                                        List<PersisterStateBatch> stateBatches) {

        log.trace("Offset tracked batch record found, batch: {} for the share partition: {}-{}", inFlightBatch,
                groupId, topicIdPartition);
        for (Map.Entry<Long, InFlightState> offsetState : inFlightBatch.offsetState.entrySet()) {

            // Check if member id is the owner of the offset.
            if (!offsetState.getValue().memberId().equals(memberId) && !offsetState.getValue().memberId().equals(EMPTY_MEMBER_ID)) {
                log.debug("Member {} is not the owner of offset: {} in batch: {} for the share"
                        + " partition: {}-{}. Skipping offset.", memberId, offsetState.getKey(), inFlightBatch, groupId, topicIdPartition);
                return Optional.empty();
            }
            if (offsetState.getValue().state == RecordState.ACQUIRED) {
                InFlightState updateResult = offsetState.getValue().startStateTransition(
                        offsetState.getKey() < startOffset ? RecordState.ARCHIVED : recordState,
                        false,
                        this.maxDeliveryCount,
                        EMPTY_MEMBER_ID
                );
                if (updateResult == null) {
                    log.debug("Unable to release records from acquired state for the offset: {} in batch: {}"
                                    + " for the share partition: {}-{}", offsetState.getKey(),
                            inFlightBatch, groupId, topicIdPartition);
                    return Optional.of(new InvalidRecordStateException("Unable to release acquired records for the offset"));
                }

                // Successfully updated the state of the offset.
                updatedStates.add(updateResult);
                stateBatches.add(new PersisterStateBatch(offsetState.getKey(), offsetState.getKey(),
                        updateResult.state.id, (short) updateResult.deliveryCount));

                // If the maxDeliveryCount limit has been exceeded, the record will be transitioned to ARCHIVED state.
                // This should not change the next fetch offset because the record is not available for acquisition
                if (updateResult.state != RecordState.ARCHIVED) {
                    findNextFetchOffset.set(true);
                }
            }
        }
        return Optional.empty();
    }

    private Optional<Throwable> releaseAcquiredRecordsForCompleteBatch(String memberId,
                                                                       InFlightBatch inFlightBatch,
                                                                       RecordState recordState,
                                                                       List<InFlightState> updatedStates,
                                                                       List<PersisterStateBatch> stateBatches) {

        // Check if member id is the owner of the batch.
        if (!inFlightBatch.batchMemberId().equals(memberId) && !inFlightBatch.batchMemberId().equals(EMPTY_MEMBER_ID)) {
            log.debug("Member {} is not the owner of batch record {} for share partition: {}-{}. Skipping batch.",
                    memberId, inFlightBatch, groupId, topicIdPartition);
            return Optional.empty();
        }

        // Change the state of complete batch since the same state exists for the entire inFlight batch.
        log.trace("Releasing acquired records for complete batch {} for the share partition: {}-{}",
                inFlightBatch, groupId, topicIdPartition);

        if (inFlightBatch.batchState() == RecordState.ACQUIRED) {
            InFlightState updateResult = inFlightBatch.startBatchStateTransition(
                    inFlightBatch.lastOffset() < startOffset ? RecordState.ARCHIVED : recordState,
                    false,
                    this.maxDeliveryCount,
                    EMPTY_MEMBER_ID
            );
            if (updateResult == null) {
                log.debug("Unable to release records from acquired state for the batch: {}"
                        + " for the share partition: {}-{}", inFlightBatch, groupId, topicIdPartition);
                return Optional.of(new InvalidRecordStateException("Unable to release acquired records for the batch"));
            }

            // Successfully updated the state of the batch.
            updatedStates.add(updateResult);
            stateBatches.add(new PersisterStateBatch(inFlightBatch.firstOffset(), inFlightBatch.lastOffset(),
                    updateResult.state.id, (short) updateResult.deliveryCount));

            // If the maxDeliveryCount limit has been exceeded, the record will be transitioned to ARCHIVED state.
            // This should not change the next fetch offset because the record is not available for acquisition
            if (updateResult.state != RecordState.ARCHIVED) {
                findNextFetchOffset.set(true);
            }
        }
        return Optional.empty();
    }

    /**
     * Updates the cached state, start and end offsets of the share partition as per the new log
     * start offset. The method is called when the log start offset is moved for the share partition.
     *
     * @param logStartOffset The new log start offset.
     */
    void updateCacheAndOffsets(long logStartOffset) {
        lock.writeLock().lock();
        try {
            if (logStartOffset <= startOffset) {
                log.error("The log start offset: {} is not greater than the start offset: {} for the share partition: {}-{}",
                        logStartOffset, startOffset, groupId, topicIdPartition);
                return;
            }
            log.debug("Updating start offset for share partition: {}-{} from: {} to: {} since LSO has moved to: {}",
                    groupId, topicIdPartition, startOffset, logStartOffset, logStartOffset);
            if (cachedState.isEmpty()) {
                // If the cached state is empty, then the start and end offset will be the new log start offset.
                // This can occur during the initialization of share partition if LSO has moved.
                startOffset = logStartOffset;
                endOffset = logStartOffset;
                return;
            }

            // Archive the available records in the cached state that are before the new log start offset.
            boolean anyRecordArchived = archiveAvailableRecordsOnLsoMovement(logStartOffset);
            // If we have transitioned the state of any batch/offset from AVAILABLE to ARCHIVED,
            // then there is a chance that the next fetch offset can change.
            if (anyRecordArchived) {
                findNextFetchOffset.set(true);
            }

            // The new startOffset will be the log start offset.
            startOffset = logStartOffset;
            if (endOffset < startOffset) {
                // This case means that the cached state is completely fresh now.
                // Example scenario - batch of 0-10 in acquired state in cached state, then LSO moves to 15,
                // then endOffset should be 15 as well.
                endOffset = startOffset;
            }

            // Note -
            // 1. We will be writing the new starOffset lazily during acknowledge/release acquired records API call.
            // 2. We will not be writing the archived state batches to the persister.
        } finally {
            lock.writeLock().unlock();
        }
    }

    private boolean archiveAvailableRecordsOnLsoMovement(long logStartOffset) {
        lock.writeLock().lock();
        try {
            boolean isAnyOffsetArchived = false, isAnyBatchArchived = false;
            for (Map.Entry<Long, InFlightBatch> entry : cachedState.entrySet()) {
                long batchStartOffset = entry.getKey();
                // We do not need to transition state of batches/offsets that are later than the new log start offset.
                if (batchStartOffset >= logStartOffset) {
                    break;
                }
                InFlightBatch inFlightBatch = entry.getValue();
                boolean fullMatch = checkForFullMatch(inFlightBatch, startOffset, logStartOffset - 1);

                // Maintain state per offset if the inflight batch is not a full match or the offset state is managed.
                if (!fullMatch || inFlightBatch.offsetState() != null) {
                    log.debug("Subset or offset tracked batch record found while trying to update offsets and cached" +
                                    " state map due to LSO movement, batch: {}, offsets to update - " +
                                    "first: {}, last: {} for the share partition: {}-{}", inFlightBatch, startOffset,
                            logStartOffset - 1, groupId, topicIdPartition);

                    if (inFlightBatch.offsetState() == null) {
                        if (inFlightBatch.batchState() != RecordState.AVAILABLE) {
                            continue;
                        }
                        inFlightBatch.maybeInitializeOffsetStateUpdate();
                    }
                    isAnyOffsetArchived = isAnyOffsetArchived || archivePerOffsetBatchRecords(inFlightBatch, startOffset, logStartOffset - 1);
                    continue;
                }
                // The in-flight batch is a full match hence change the state of the complete batch.
                isAnyBatchArchived = isAnyBatchArchived || archiveCompleteBatch(inFlightBatch);
            }
            return isAnyOffsetArchived || isAnyBatchArchived;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private boolean archivePerOffsetBatchRecords(InFlightBatch inFlightBatch,
                                                 long startOffsetToArchive,
                                                 long endOffsetToArchive) {
        lock.writeLock().lock();
        try {
            boolean isAnyOffsetArchived = false;
            log.trace("Archiving offset tracked batch: {} for the share partition: {}-{}", inFlightBatch, groupId, topicIdPartition);
            for (Map.Entry<Long, InFlightState> offsetState : inFlightBatch.offsetState().entrySet()) {
                if (offsetState.getKey() < startOffsetToArchive) {
                    continue;
                }
                if (offsetState.getKey() > endOffsetToArchive) {
                    // No further offsets to process.
                    break;
                }
                if (offsetState.getValue().state != RecordState.AVAILABLE) {
                    continue;
                }

                offsetState.getValue().archive(EMPTY_MEMBER_ID);
                isAnyOffsetArchived = true;
            }
            return isAnyOffsetArchived;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private boolean archiveCompleteBatch(InFlightBatch inFlightBatch) {
        lock.writeLock().lock();
        try {
            log.trace("Archiving complete batch: {} for the share partition: {}-{}", inFlightBatch, groupId, topicIdPartition);
            if (inFlightBatch.batchState() == RecordState.AVAILABLE) {
                // Change the state of complete batch since the same state exists for the entire inFlight batch.
                inFlightBatch.archiveBatch(EMPTY_MEMBER_ID);
                return true;
            }
        } finally {
            lock.writeLock().unlock();
        }
        return false;
    }

    /**
     * Checks if the records can be acquired for the share partition. The records can be acquired if
     * the number of records in-flight is less than the max in-flight messages. Or if the fetch is
     * to happen somewhere in between the record states cached in the share partition i.e. re-acquire
     * the records that are already fetched before.
     *
     * @return A boolean which indicates whether more records can be acquired or not.
     */
    boolean canAcquireRecords() {
        if (nextFetchOffset() != endOffset() + 1) {
            return true;
        }

        lock.readLock().lock();
        long numRecords;
        try {
            if (cachedState.isEmpty()) {
                numRecords = 0;
            } else {
                numRecords = this.endOffset - this.startOffset + 1;
            }
        } finally {
            lock.readLock().unlock();
        }
        return numRecords < maxInFlightMessages;
    }

    /**
     * Prior to fetching records from the leader, the fetch lock is acquired to ensure that the same
     * share partition is not fetched concurrently by multiple clients. The fetch lock is released once
     * the records are fetched and acquired.
     *
     * @return A boolean which indicates whether the fetch lock is acquired.
     */
    boolean maybeAcquireFetchLock() {
        if (stateNotActive()) {
            return false;
        }
        return fetchLock.compareAndSet(false, true);
    }

    /**
     * Release the fetch lock once the records are fetched from the leader.
     */
    void releaseFetchLock() {
        fetchLock.set(false);
    }

    /**
     * Marks the share partition as fenced.
     */
    void markFenced() {
        lock.writeLock().lock();
        try {
            partitionState = SharePartitionState.FENCED;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Returns the share partition listener.
     *
     * @return The share partition listener.
     */
    SharePartitionListener listener() {
        return this.listener;
    }

    int leaderEpoch() {
        return leaderEpoch;
    }

    private boolean stateNotActive() {
        return  partitionState() != SharePartitionState.ACTIVE;
    }

    private boolean emptyToInitialState() {
        lock.writeLock().lock();
        try {
            if (initializedOrThrowException()) return false;
            partitionState = SharePartitionState.INITIALIZING;
            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private boolean initializedOrThrowException() {
        SharePartitionState currentState = partitionState();
        return switch (currentState) {
            case ACTIVE -> true;
            case FAILED -> throw new IllegalStateException(
                String.format("Share partition failed to load %s-%s", groupId, topicIdPartition));
            case INITIALIZING -> throw new LeaderNotAvailableException(
                String.format("Share partition is already initializing %s-%s", groupId, topicIdPartition));
            case FENCED -> throw new FencedStateEpochException(
                String.format("Share partition is fenced %s-%s", groupId, topicIdPartition));
            case EMPTY ->
                // The share partition is not yet initialized.
                false;
        };
    }

    private AcquiredRecords acquireNewBatchRecords(
        String memberId,
        Iterable<? extends RecordBatch> batches,
        long firstOffset,
        long lastOffset,
        int maxFetchRecords
    ) {
        lock.writeLock().lock();
        try {
            // If same batch is fetched and previous batch is removed from the cache then we need to
            // update the batch first offset to endOffset, only if endOffset passed the firstOffset.
            // For an initial start of the share fetch from a topic partition the endOffset will be initialized
            // to 0 but firstOffset can be higher than 0.
            long firstAcquiredOffset = firstOffset;
            if (cachedState.isEmpty() && endOffset > firstAcquiredOffset) {
                firstAcquiredOffset = endOffset;
            }

            // Check how many messages can be acquired from the batch.
            long lastAcquiredOffset = lastOffset;
            if (maxFetchRecords < lastAcquiredOffset - firstAcquiredOffset + 1) {
                // The max messages to acquire is less than the complete available batches hence
                // limit the acquired records. The last offset shall be the batches last offset
                // which falls under the max messages limit. As the max fetch records is the soft
                // limit, the last offset can be higher than the max messages.
                lastAcquiredOffset = lastOffsetFromBatchWithRequestOffset(batches, firstAcquiredOffset + maxFetchRecords - 1);
            }

            // Schedule acquisition lock timeout for the batch.
            AcquisitionLockTimerTask timerTask = scheduleAcquisitionLockTimeout(memberId, firstAcquiredOffset, lastAcquiredOffset);
            // Add the new batch to the in-flight records along with the acquisition lock timeout task for the batch.
            cachedState.put(firstAcquiredOffset, new InFlightBatch(
                memberId,
                firstAcquiredOffset,
                lastAcquiredOffset,
                RecordState.ACQUIRED,
                1,
                timerTask));
            // if the cachedState was empty before acquiring the new batches then startOffset needs to be updated
            if (cachedState.firstKey() == firstAcquiredOffset)  {
                startOffset = firstAcquiredOffset;
            }
            endOffset = lastAcquiredOffset;
            return new AcquiredRecords()
                .setFirstOffset(firstAcquiredOffset)
                .setLastOffset(lastAcquiredOffset)
                .setDeliveryCount((short) 1);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private int acquireSubsetBatchRecords(
        String memberId,
        long requestFirstOffset,
        long requestLastOffset,
        InFlightBatch inFlightBatch,
        List<AcquiredRecords> result
    ) {
        lock.writeLock().lock();
        int acquiredCount = 0;
        try {
            for (Map.Entry<Long, InFlightState> offsetState : inFlightBatch.offsetState.entrySet()) {
                // For the first batch which might have offsets prior to the request base
                // offset i.e. cached batch of 10-14 offsets and request batch of 12-13.
                if (offsetState.getKey() < requestFirstOffset) {
                    continue;
                }

                if (offsetState.getKey() > requestLastOffset) {
                    // No further offsets to process.
                    break;
                }

                if (offsetState.getValue().state != RecordState.AVAILABLE || offsetState.getValue().hasOngoingStateTransition()) {
                    log.trace("The offset {} is not available in share partition: {}-{}, skipping: {}",
                        offsetState.getKey(), groupId, topicIdPartition, inFlightBatch);
                    continue;
                }

                InFlightState updateResult =  offsetState.getValue().tryUpdateState(RecordState.ACQUIRED, true, maxDeliveryCount,
                    memberId);
                if (updateResult == null) {
                    log.trace("Unable to acquire records for the offset: {} in batch: {}"
                            + " for the share partition: {}-{}", offsetState.getKey(), inFlightBatch,
                        groupId, topicIdPartition);
                    continue;
                }
                // Schedule acquisition lock timeout for the offset.
                AcquisitionLockTimerTask acquisitionLockTimeoutTask = scheduleAcquisitionLockTimeout(memberId, offsetState.getKey(), offsetState.getKey());
                // Update acquisition lock timeout task for the offset.
                offsetState.getValue().updateAcquisitionLockTimeoutTask(acquisitionLockTimeoutTask);

                // TODO: Maybe we can club the continuous offsets here.
                result.add(new AcquiredRecords()
                    .setFirstOffset(offsetState.getKey())
                    .setLastOffset(offsetState.getKey())
                    .setDeliveryCount((short) offsetState.getValue().deliveryCount));
                acquiredCount++;
            }
        } finally {
            lock.writeLock().unlock();
        }
        return acquiredCount;
    }

    /**
     * Check if the in-flight batch is a full match with the request offsets. The full match represents
     * complete overlap of the in-flight batch with the request offsets.
     *
     * @param inFlightBatch The in-flight batch to check for full match.
     * @param firstOffsetToCompare The first offset of the request batch.
     * @param lastOffsetToCompare The last offset of the request batch.
     *
     * @return True if the in-flight batch is a full match with the request offsets, false otherwise.
     */
    private boolean checkForFullMatch(InFlightBatch inFlightBatch, long firstOffsetToCompare, long lastOffsetToCompare) {
        return inFlightBatch.firstOffset() >= firstOffsetToCompare && inFlightBatch.lastOffset() <= lastOffsetToCompare;
    }

    /**
     * Check if the start offset has moved and within the request first and last offset.
     *
     * @param batchFirstOffset The first offset of the batch.
     * @param batchLastOffset The last offset of the batch.
     *
     * @return True if the start offset has moved and within the request first and last offset, false otherwise.
     */
    private boolean checkForStartOffsetWithinBatch(long batchFirstOffset, long batchLastOffset) {
        long localStartOffset = startOffset();
        return batchFirstOffset < localStartOffset && batchLastOffset >= localStartOffset;
    }

    private Map<Long, RecordState> fetchRecordStateMapForAcknowledgementBatch(
        ShareAcknowledgementBatch batch) {
        // Client can either send a single entry in acknowledgeTypes which represents the state
        // of the complete batch or can send individual offsets state. Construct a map with record state
        // for each offset in the batch, if single acknowledge type is sent, the map will have only one entry.
        Map<Long, RecordState> recordStateMap = new HashMap<>();
        for (int index = 0; index < batch.acknowledgeTypes().size(); index++) {
            recordStateMap.put(batch.firstOffset() + index,
                fetchRecordState(batch.acknowledgeTypes().get(index)));
        }
        return recordStateMap;
    }

    private static RecordState fetchRecordState(byte acknowledgeType) {
        switch (acknowledgeType) {
            case 1 /* ACCEPT */:
                return RecordState.ACKNOWLEDGED;
            case 2 /* RELEASE */:
                return RecordState.AVAILABLE;
            case 3 /* REJECT */:
            case 0 /* GAP */:
                return RecordState.ARCHIVED;
            default:
                throw new IllegalArgumentException("Invalid acknowledge type: " + acknowledgeType);
        }
    }

    private NavigableMap<Long, InFlightBatch> fetchSubMapForAcknowledgementBatch(
        ShareAcknowledgementBatch batch
    ) {
        lock.writeLock().lock();
        try {
            // Find the floor batch record for the request batch. The request batch could be
            // for a subset of the batch i.e. cached batch of offset 10-14 and request batch
            // of 12-13. Hence, floor entry is fetched to find the sub-map.
            Map.Entry<Long, InFlightBatch> floorOffset = cachedState.floorEntry(batch.firstOffset());
            if (floorOffset == null) {
                boolean hasStartOffsetMoved = checkForStartOffsetWithinBatch(batch.firstOffset(), batch.lastOffset());
                if (hasStartOffsetMoved) {
                    // If the start offset has been moved and within the request batch then fetch
                    // the floor entry from start offset and acknowledge cached offsets. Consider
                    // the case where the start offset has moved from 0 to 10, with the cached batch
                    // of 0 - 5, 5 - 10, 10 - 12, 12 - 15. The request batch for acknowledge is 5 - 15,
                    // then post acquisition lock timeout the cache will have data from only from 10 to 15.
                    // Hence, we need to fetch the floor entry from start offset.
                    floorOffset = cachedState.floorEntry(startOffset);
                } else {
                    log.debug("Batch record {} not found for share partition: {}-{}", batch, groupId,
                        topicIdPartition);
                    throw new InvalidRecordStateException(
                        "Batch record not found. The request batch offsets are not found in the cache.");
                }
            }

            NavigableMap<Long, InFlightBatch> subMap = cachedState.subMap(floorOffset.getKey(), true, batch.lastOffset(), true);
            // Validate if the request batch has the first offset greater than the last offset of the last
            // fetched cached batch, then there will be no offsets in the request that can be acknowledged.
            if (subMap.lastEntry().getValue().lastOffset < batch.firstOffset()) {
                log.debug("Request batch: {} has offsets which are not found for share partition: {}-{}", batch, groupId, topicIdPartition);
                throw new InvalidRequestException("Batch record not found. The first offset in request is past acquired records.");
            }

            // Validate if the request batch has the last offset greater than the last offset of
            // the last fetched cached batch, then there will be offsets in the request than cannot
            // be found in the fetched batches.
            if (batch.lastOffset() > subMap.lastEntry().getValue().lastOffset) {
                log.debug("Request batch: {} has offsets which are not found for share partition: {}-{}", batch, groupId, topicIdPartition);
                throw new InvalidRequestException("Batch record not found. The last offset in request is past acquired records.");
            }

            return subMap;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private Optional<Throwable> acknowledgeBatchRecords(
        String memberId,
        ShareAcknowledgementBatch batch,
        Map<Long, RecordState> recordStateMap,
        NavigableMap<Long, InFlightBatch> subMap,
        final List<InFlightState> updatedStates,
        List<PersisterStateBatch> stateBatches
    ) {
        Optional<Throwable> throwable;
        lock.writeLock().lock();
        try {
            // The acknowledgement batch either is exact fetch equivalent batch (mostly), subset
            // or spans over multiple fetched batches. The state can vary per offset itself from
            // the fetched batch in case of subset or client sent individual offsets state.
            for (Map.Entry<Long, InFlightBatch> entry : subMap.entrySet()) {
                InFlightBatch inFlightBatch = entry.getValue();

                // If startOffset has moved ahead of the in-flight batch, skip the batch.
                if (inFlightBatch.lastOffset() < startOffset) {
                    log.trace("All offsets in the inflight batch {} are already archived: {}-{}",
                        inFlightBatch, groupId, topicIdPartition);
                    continue;
                }

                // Validate if the requested member id is the owner of the batch.
                if (inFlightBatch.offsetState() == null) {
                    throwable = validateAcknowledgementBatchMemberId(memberId, inFlightBatch);
                    if (throwable.isPresent()) {
                        return throwable;
                    }
                }

                // Determine if the in-flight batch is a full match from the request batch.
                boolean fullMatch = checkForFullMatch(inFlightBatch, batch.firstOffset(), batch.lastOffset());
                boolean isPerOffsetClientAck = batch.acknowledgeTypes().size() > 1;
                boolean hasStartOffsetMoved = checkForStartOffsetWithinBatch(inFlightBatch.firstOffset(), inFlightBatch.lastOffset());

                // Maintain state per offset if the inflight batch is not a full match or the
                // offset state is managed or client sent individual offsets state or
                // the start offset is within this in-flight batch.
                if (!fullMatch || inFlightBatch.offsetState() != null || isPerOffsetClientAck || hasStartOffsetMoved) {
                    log.debug("Subset or offset tracked batch record found for acknowledgement,"
                            + " batch: {}, request offsets - first: {}, last: {}, client per offset"
                            + "state {} for the share partition: {}-{}", inFlightBatch, batch.firstOffset(),
                        batch.lastOffset(), isPerOffsetClientAck, groupId, topicIdPartition);
                    if (inFlightBatch.offsetState() == null) {
                        // Though the request is a subset of in-flight batch but the offset
                        // tracking has not been initialized yet which means that we could only
                        // acknowledge subset of offsets from the in-flight batch but only if the
                        // complete batch is acquired yet. Hence, do a pre-check to avoid exploding
                        // the in-flight offset tracking unnecessarily.
                        if (inFlightBatch.batchState() != RecordState.ACQUIRED) {
                            log.debug("The batch is not in the acquired state: {} for share partition: {}-{}",
                                inFlightBatch, groupId, topicIdPartition);
                            return Optional.of(new InvalidRecordStateException("The batch cannot be acknowledged. The subset batch is not in the acquired state."));
                        }
                        // The request batch is a subset and requires per offset state hence initialize
                        // the offsets state in the in-flight batch.
                        inFlightBatch.maybeInitializeOffsetStateUpdate();
                    }

                    throwable = acknowledgePerOffsetBatchRecords(memberId, batch, inFlightBatch,
                        recordStateMap, updatedStates, stateBatches);
                } else {
                    // The in-flight batch is a full match hence change the state of the complete batch.
                    throwable = acknowledgeCompleteBatch(batch, inFlightBatch,
                        recordStateMap.get(batch.firstOffset()), updatedStates, stateBatches);
                }

                if (throwable.isPresent()) {
                    return throwable;
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
        return Optional.empty();
    }

    private Optional<Throwable> validateAcknowledgementBatchMemberId(
        String memberId,
        InFlightBatch inFlightBatch
    ) {
        // EMPTY_MEMBER_ID is used to indicate that the batch is not in acquired state.
        if (inFlightBatch.batchMemberId().equals(EMPTY_MEMBER_ID)) {
            log.debug("The batch is not in the acquired state: {} for share partition: {}-{}. Empty member id for batch.",
                inFlightBatch, groupId, topicIdPartition);
            return Optional.of(new InvalidRecordStateException("The batch cannot be acknowledged. The batch is not in the acquired state."));
        }

        if (!inFlightBatch.batchMemberId().equals(memberId)) {
            log.debug("Member {} is not the owner of batch record {} for share partition: {}-{}",
                memberId, inFlightBatch, groupId, topicIdPartition);
            return Optional.of(new InvalidRecordStateException("Member is not the owner of batch record"));
        }
        return Optional.empty();
    }

    private Optional<Throwable> acknowledgePerOffsetBatchRecords(
        String memberId,
        ShareAcknowledgementBatch batch,
        InFlightBatch inFlightBatch,
        Map<Long, RecordState> recordStateMap,
        List<InFlightState> updatedStates,
        List<PersisterStateBatch> stateBatches
    ) {
        lock.writeLock().lock();
        try {
            // Fetch the first record state from the map to be used as default record state in case the
            // offset record state is not provided by client.
            RecordState recordStateDefault = recordStateMap.get(batch.firstOffset());
            for (Map.Entry<Long, InFlightState> offsetState : inFlightBatch.offsetState.entrySet()) {

                // 1. For the first batch which might have offsets prior to the request base
                // offset i.e. cached batch of 10-14 offsets and request batch of 12-13.
                // 2. Skip the offsets which are below the start offset of the share partition
                if (offsetState.getKey() < batch.firstOffset() || offsetState.getKey() < startOffset) {
                    continue;
                }

                if (offsetState.getKey() > batch.lastOffset()) {
                    // No further offsets to process.
                    break;
                }

                if (offsetState.getValue().state != RecordState.ACQUIRED) {
                    log.debug("The offset is not acquired, offset: {} batch: {} for the share"
                            + " partition: {}-{}", offsetState.getKey(), inFlightBatch, groupId,
                        topicIdPartition);
                    return Optional.of(new InvalidRecordStateException(
                        "The batch cannot be acknowledged. The offset is not acquired."));
                }

                // Check if member id is the owner of the offset.
                if (!offsetState.getValue().memberId.equals(memberId)) {
                    log.debug("Member {} is not the owner of offset: {} in batch: {} for the share"
                            + " partition: {}-{}", memberId, offsetState.getKey(), inFlightBatch,
                        groupId, topicIdPartition);
                    return Optional.of(
                        new InvalidRecordStateException("Member is not the owner of offset"));
                }

                // Determine the record state for the offset. If the per offset record state is not provided
                // by the client, then use the batch record state.
                RecordState recordState =
                    recordStateMap.size() > 1 ? recordStateMap.get(offsetState.getKey()) :
                        recordStateDefault;
                InFlightState updateResult = offsetState.getValue().startStateTransition(
                    recordState,
                    false,
                    this.maxDeliveryCount,
                    EMPTY_MEMBER_ID
                );
                if (updateResult == null) {
                    log.debug("Unable to acknowledge records for the offset: {} in batch: {}"
                            + " for the share partition: {}-{}", offsetState.getKey(),
                        inFlightBatch, groupId, topicIdPartition);
                    return Optional.of(new InvalidRecordStateException(
                        "Unable to acknowledge records for the batch"));
                }
                // Successfully updated the state of the offset.
                updatedStates.add(updateResult);
                stateBatches.add(new PersisterStateBatch(offsetState.getKey(), offsetState.getKey(),
                    updateResult.state.id, (short) updateResult.deliveryCount));
                // If the maxDeliveryCount limit has been exceeded, the record will be transitioned to ARCHIVED state.
                // This should not change the next fetch offset because the record is not available for acquisition
                if (recordState == RecordState.AVAILABLE
                    && updateResult.state != RecordState.ARCHIVED) {
                    findNextFetchOffset.set(true);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
        return Optional.empty();
    }

    private Optional<Throwable> acknowledgeCompleteBatch(
        ShareAcknowledgementBatch batch,
        InFlightBatch inFlightBatch,
        RecordState recordState,
        List<InFlightState> updatedStates,
        List<PersisterStateBatch> stateBatches
    ) {
        lock.writeLock().lock();
        try {
            // The in-flight batch is a full match hence change the state of the complete.
            log.trace("Acknowledging complete batch record {} for the share partition: {}-{}",
                batch, groupId, topicIdPartition);
            if (inFlightBatch.batchState() != RecordState.ACQUIRED) {
                log.debug("The batch is not in the acquired state: {} for share partition: {}-{}",
                    inFlightBatch, groupId, topicIdPartition);
                return Optional.of(new InvalidRecordStateException(
                    "The batch cannot be acknowledged. The batch is not in the acquired state."));
            }

            // Change the state of complete batch since the same state exists for the entire inFlight batch.
            // The member id is reset to EMPTY_MEMBER_ID irrespective of the acknowledge type as the batch is
            // either released or moved to a state where member id existence is not important. The member id
            // is only important when the batch is acquired.
            InFlightState updateResult = inFlightBatch.startBatchStateTransition(
                recordState,
                false,
                this.maxDeliveryCount,
                EMPTY_MEMBER_ID
            );
            if (updateResult == null) {
                log.debug("Unable to acknowledge records for the batch: {} with state: {}"
                        + " for the share partition: {}-{}", inFlightBatch, recordState, groupId,
                    topicIdPartition);
                return Optional.of(
                    new InvalidRecordStateException("Unable to acknowledge records for the batch"));
            }

            // Successfully updated the state of the batch.
            updatedStates.add(updateResult);
            stateBatches.add(
                new PersisterStateBatch(inFlightBatch.firstOffset, inFlightBatch.lastOffset,
                    updateResult.state.id, (short) updateResult.deliveryCount));

            // If the maxDeliveryCount limit has been exceeded, the record will be transitioned to ARCHIVED state.
            // This should not change the nextFetchOffset because the record is not available for acquisition
            if (recordState == RecordState.AVAILABLE
                && updateResult.state != RecordState.ARCHIVED) {
                findNextFetchOffset.set(true);
            }
        } finally {
            lock.writeLock().unlock();
        }
        return Optional.empty();
    }

    protected void updateFetchOffsetMetadata(long nextFetchOffset, LogOffsetMetadata logOffsetMetadata) {
        lock.writeLock().lock();
        try {
            fetchOffsetMetadata.updateOffsetMetadata(nextFetchOffset, logOffsetMetadata);
        } finally {
            lock.writeLock().unlock();
        }
    }

    protected Optional<LogOffsetMetadata> fetchOffsetMetadata(long nextFetchOffset) {
        lock.readLock().lock();
        try {
            if (fetchOffsetMetadata.offsetMetadata() == null || fetchOffsetMetadata.offset() != nextFetchOffset)
                return Optional.empty();
            return Optional.of(fetchOffsetMetadata.offsetMetadata());
        } finally {
            lock.readLock().unlock();
        }
    }

    // Visible for testing
    SharePartitionState partitionState() {
        lock.readLock().lock();
        try {
            return partitionState;
        } finally {
            lock.readLock().unlock();
        }
    }

    // Visible for testing
    void rollbackOrProcessStateUpdates(
        CompletableFuture<Void> future,
        Throwable throwable,
        List<InFlightState> updatedStates,
        List<PersisterStateBatch> stateBatches
    ) {
        lock.writeLock().lock();
        try {
            if (throwable != null) {
                // Log in DEBUG to avoid flooding of logs for a faulty client.
                log.debug("Request failed for updating state, rollback any changed state"
                    + " for the share partition: {}-{}", groupId, topicIdPartition);
                updatedStates.forEach(state -> state.completeStateTransition(false));
                future.completeExceptionally(throwable);
                return;
            }

            if (stateBatches.isEmpty() && updatedStates.isEmpty()) {
                future.complete(null);
                return;
            }
        } finally {
            lock.writeLock().unlock();
        }

        writeShareGroupState(stateBatches).whenComplete((result, exception) -> {
            lock.writeLock().lock();
            try {
                if (exception != null) {
                    log.error("Failed to write state to persister for the share partition: {}-{}",
                        groupId, topicIdPartition, exception);
                    updatedStates.forEach(state -> state.completeStateTransition(false));
                    future.completeExceptionally(exception);
                    return;
                }

                log.trace("State change request successful for share partition: {}-{}",
                    groupId, topicIdPartition);
                updatedStates.forEach(state -> {
                    state.completeStateTransition(true);
                    // Cancel the acquisition lock timeout task for the state since it is acknowledged/released successfully.
                    state.cancelAndClearAcquisitionLockTimeoutTask();
                });
                // Update the cached state and start and end offsets after acknowledging/releasing the acquired records.
                maybeUpdateCachedStateAndOffsets();
                future.complete(null);
            } finally {
                lock.writeLock().unlock();
            }
        });
    }

    private void maybeUpdateCachedStateAndOffsets() {
        lock.writeLock().lock();
        try {
            if (!canMoveStartOffset()) {
                return;
            }

            // This will help to find the next position for the startOffset.
            // The new position of startOffset will be lastOffsetAcknowledged + 1
            long lastOffsetAcknowledged = findLastOffsetAcknowledged();
            // If lastOffsetAcknowledged is -1, this means we cannot move out startOffset ahead
            if (lastOffsetAcknowledged == -1) {
                return;
            }

            // This is true if all records in the cachedState have been acknowledged (either Accept or Reject).
            // The resulting action should be to empty the cachedState altogether
            long lastCachedOffset = cachedState.lastEntry().getValue().lastOffset();
            if (lastOffsetAcknowledged == lastCachedOffset) {
                startOffset = lastCachedOffset + 1; // The next offset that will be fetched and acquired in the share partition
                endOffset = lastCachedOffset + 1;
                cachedState.clear();
                // Nothing further to do.
                return;
            }

            /*
             The cachedState contains some records that are yet to be acknowledged, and thus should
             not be removed. Only a subMap will be removed from the cachedState. The logic to remove
             batches from cachedState is as follows:
             a) Only full batches can be removed from the cachedState, For example if there is batch (0-99)
             and 0-49 records are acknowledged (ACCEPT or REJECT), the first 50 records will not be removed
             from the cachedState. Instead, the startOffset will be moved to 50, but the batch will only
             be removed once all the messages (0-99) are acknowledged (ACCEPT or REJECT).
            */

            // Since only a subMap will be removed, we need to find the first and last keys of that subMap
            long firstKeyToRemove = cachedState.firstKey();
            long lastKeyToRemove;
            NavigableMap.Entry<Long, InFlightBatch> entry = cachedState.floorEntry(lastOffsetAcknowledged);
            if (lastOffsetAcknowledged == entry.getValue().lastOffset()) {
                startOffset = cachedState.higherKey(lastOffsetAcknowledged);
                lastKeyToRemove = entry.getKey();
            } else {
                startOffset = lastOffsetAcknowledged + 1;
                if (entry.getKey().equals(cachedState.firstKey())) {
                    // If the first batch in cachedState has some records yet to be acknowledged,
                    // then nothing should be removed from cachedState
                    lastKeyToRemove = -1;
                } else {
                    lastKeyToRemove = cachedState.lowerKey(entry.getKey());
                }
            }

            if (lastKeyToRemove != -1) {
                NavigableMap<Long, InFlightBatch> subMap = cachedState.subMap(firstKeyToRemove, true, lastKeyToRemove, true);
                for (Long key : subMap.keySet()) {
                    cachedState.remove(key);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private boolean canMoveStartOffset() {
        // The Share Partition Start Offset may be moved after acknowledge request is complete.
        // The following conditions need to be met to move the startOffset:
        // 1. When the cachedState is not empty.
        // 2. When the acknowledgement type for the records is either ACCEPT or REJECT.
        // 3. When all the previous records have been acknowledged (ACCEPT or REJECT).
        if (cachedState.isEmpty()) {
            return false;
        }

        NavigableMap.Entry<Long, InFlightBatch> entry = cachedState.floorEntry(startOffset);
        if (entry == null) {
            log.error("The start offset: {} is not found in the cached state for share partition: {}-{}."
                + " Cannot move the start offset.", startOffset, groupId, topicIdPartition);
            return false;
        }
        RecordState startOffsetState = entry.getValue().offsetState == null ?
            entry.getValue().batchState() :
            entry.getValue().offsetState().get(startOffset).state();
        return isRecordStateAcknowledged(startOffsetState);
    }

    /**
     * The record state is considered acknowledged if it is either acknowledged or archived.
     * These are terminal states for the record.
     *
     * @param recordState The record state to check.
     *
     * @return True if the record state is acknowledged or archived, false otherwise.
     */
    private boolean isRecordStateAcknowledged(RecordState recordState) {
        return recordState == RecordState.ACKNOWLEDGED || recordState == RecordState.ARCHIVED;
    }

    private long findLastOffsetAcknowledged() {
        lock.readLock().lock();
        long lastOffsetAcknowledged = -1;
        try {
            for (NavigableMap.Entry<Long, InFlightBatch> entry : cachedState.entrySet()) {
                InFlightBatch inFlightBatch = entry.getValue();
                if (inFlightBatch.offsetState() == null) {
                    if (!isRecordStateAcknowledged(inFlightBatch.batchState())) {
                        return lastOffsetAcknowledged;
                    }
                    lastOffsetAcknowledged = inFlightBatch.lastOffset();
                } else {
                    for (Map.Entry<Long, InFlightState> offsetState : inFlightBatch.offsetState.entrySet()) {
                        if (!isRecordStateAcknowledged(offsetState.getValue().state())) {
                            return lastOffsetAcknowledged;
                        }
                        lastOffsetAcknowledged = offsetState.getKey();
                    }
                }
            }
        } finally {
            lock.readLock().unlock();
        }
        return lastOffsetAcknowledged;
    }

    /**
     * Find the last offset from the batch which contains the request offset. If found, return the last offset
     * of the batch, otherwise return the request offset.
     *
     * @param batches The batches to search for the request offset.
     * @param offset The request offset to find.
     * @return The last offset of the batch which contains the request offset, otherwise the request offset.
     */
    private long lastOffsetFromBatchWithRequestOffset(
        Iterable<? extends RecordBatch> batches,
        long offset
    ) {
        // Fetch the last batch which might contains the request offset. Avoid calling lastOffset()
        // on the batches as it loads the header in memory.
        RecordBatch previousBatch = null;
        for (RecordBatch batch : batches) {
            if (offset >= batch.baseOffset()) {
                previousBatch =  batch;
                continue;
            }
            break;
        }
        if (previousBatch != null && offset <= previousBatch.lastOffset())
            return previousBatch.lastOffset();
        return offset;
    }

    // Visible for testing
    CompletableFuture<Void> writeShareGroupState(List<PersisterStateBatch> stateBatches) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        persister.writeState(new WriteShareGroupStateParameters.Builder()
            .setGroupTopicPartitionData(new GroupTopicPartitionData.Builder<PartitionStateBatchData>()
                .setGroupId(this.groupId)
                .setTopicsData(Collections.singletonList(new TopicData<>(topicIdPartition.topicId(),
                    Collections.singletonList(PartitionFactory.newPartitionStateBatchData(
                        topicIdPartition.partition(), stateEpoch, startOffset, leaderEpoch, stateBatches))))
                ).build()).build())
            .whenComplete((result, exception) -> {
                if (exception != null) {
                    log.error("Failed to write the share group state for share partition: {}-{}", groupId, topicIdPartition, exception);
                    future.completeExceptionally(new IllegalStateException(String.format("Failed to write the share group state for share partition %s-%s",
                        groupId, topicIdPartition), exception));
                    return;
                }

                if (result == null || result.topicsData() == null || result.topicsData().size() != 1) {
                    log.error("Failed to write the share group state for share partition: {}-{}. Invalid state found: {}",
                        groupId, topicIdPartition, result);
                    future.completeExceptionally(new IllegalStateException(String.format("Failed to write the share group state for share partition %s-%s",
                        groupId, topicIdPartition)));
                    return;
                }

                TopicData<PartitionErrorData> state = result.topicsData().get(0);
                if (state.topicId() != topicIdPartition.topicId() || state.partitions().size() != 1
                    || state.partitions().get(0).partition() != topicIdPartition.partition()) {
                    log.error("Failed to write the share group state for share partition: {}-{}. Invalid topic partition response: {}",
                        groupId, topicIdPartition, result);
                    future.completeExceptionally(new IllegalStateException(String.format("Failed to write the share group state for share partition %s-%s",
                        groupId, topicIdPartition)));
                    return;
                }

                PartitionErrorData partitionData = state.partitions().get(0);
                if (partitionData.errorCode() != Errors.NONE.code()) {
                    KafkaException ex = fetchPersisterError(partitionData.errorCode(), partitionData.errorMessage());
                    log.error("Failed to write the share group state for share partition: {}-{} due to exception",
                        groupId, topicIdPartition, ex);
                    future.completeExceptionally(ex);
                    return;
                }
                future.complete(null);
            });
        return future;
    }

    private KafkaException fetchPersisterError(short errorCode, String errorMessage) {
        Errors error = Errors.forCode(errorCode);
        switch (error) {
            case NOT_COORDINATOR:
            case COORDINATOR_NOT_AVAILABLE:
            case COORDINATOR_LOAD_IN_PROGRESS:
                return new CoordinatorNotAvailableException(errorMessage);
            case GROUP_ID_NOT_FOUND:
                return new GroupIdNotFoundException(errorMessage);
            case UNKNOWN_TOPIC_OR_PARTITION:
                return new UnknownTopicOrPartitionException(errorMessage);
            case FENCED_STATE_EPOCH:
                return new FencedStateEpochException(errorMessage);
            case FENCED_LEADER_EPOCH:
                return new NotLeaderOrFollowerException(errorMessage);
            default:
                return new UnknownServerException(errorMessage);
        }
    }

    // Visible for testing
    AcquisitionLockTimerTask scheduleAcquisitionLockTimeout(String memberId, long firstOffset, long lastOffset) {
        // The recordLockDuration value would depend on whether the dynamic config SHARE_RECORD_LOCK_DURATION_MS in
        // GroupConfig.java is set or not. If dynamic config is set, then that is used, otherwise the value of
        // SHARE_GROUP_RECORD_LOCK_DURATION_MS_CONFIG defined in ShareGroupConfig is used
        int recordLockDurationMs;
        if (groupConfigManager.groupConfig(groupId).isPresent()) {
            recordLockDurationMs = groupConfigManager.groupConfig(groupId).get().shareRecordLockDurationMs();
        } else {
            recordLockDurationMs = defaultRecordLockDurationMs;
        }
        return scheduleAcquisitionLockTimeout(memberId, firstOffset, lastOffset, recordLockDurationMs);
    }

    /**
     * Apply acquisition lock to acquired records.
     *
     * @param memberId The member id of the client that is putting the acquisition lock.
     * @param firstOffset The first offset of the acquired records.
     * @param lastOffset The last offset of the acquired records.
     * @param delayMs The delay in milliseconds after which the acquisition lock will be released.
     */
    private AcquisitionLockTimerTask scheduleAcquisitionLockTimeout(
        String memberId,
        long firstOffset,
        long lastOffset,
        long delayMs
    ) {
        AcquisitionLockTimerTask acquisitionLockTimerTask = acquisitionLockTimerTask(memberId, firstOffset, lastOffset, delayMs);
        timer.add(acquisitionLockTimerTask);
        return acquisitionLockTimerTask;
    }

    private AcquisitionLockTimerTask acquisitionLockTimerTask(
        String memberId,
        long firstOffset,
        long lastOffset,
        long delayMs
    ) {
        return new AcquisitionLockTimerTask(delayMs, memberId, firstOffset, lastOffset);
    }

    private void releaseAcquisitionLockOnTimeout(String memberId, long firstOffset, long lastOffset) {
        List<PersisterStateBatch> stateBatches;
        lock.writeLock().lock();
        try {
            Map.Entry<Long, InFlightBatch> floorOffset = cachedState.floorEntry(firstOffset);
            if (floorOffset == null) {
                log.error("Base offset {} not found for share partition: {}-{}", firstOffset, groupId, topicIdPartition);
                return;
            }
            stateBatches = new ArrayList<>();
            NavigableMap<Long, InFlightBatch> subMap = cachedState.subMap(floorOffset.getKey(), true, lastOffset, true);
            for (Map.Entry<Long, InFlightBatch> entry : subMap.entrySet()) {
                InFlightBatch inFlightBatch = entry.getValue();

                if (inFlightBatch.offsetState() == null
                        && inFlightBatch.batchState() == RecordState.ACQUIRED
                        && checkForStartOffsetWithinBatch(inFlightBatch.firstOffset(), inFlightBatch.lastOffset())) {

                    // For the case when batch.firstOffset < start offset <= batch.lastOffset, we will be having some
                    // acquired records that need to move to archived state despite their delivery count.
                    inFlightBatch.maybeInitializeOffsetStateUpdate();
                }

                // Case when the state of complete batch is valid
                if (inFlightBatch.offsetState() == null) {
                    releaseAcquisitionLockOnTimeoutForCompleteBatch(inFlightBatch, stateBatches, memberId);
                } else { // Case when batch has a valid offset state map.
                    releaseAcquisitionLockOnTimeoutForPerOffsetBatch(inFlightBatch, stateBatches, memberId, firstOffset, lastOffset);
                }
            }

            if (!stateBatches.isEmpty()) {
                writeShareGroupState(stateBatches).whenComplete((result, exception) -> {
                    if (exception != null) {
                        log.error("Failed to write the share group state on acquisition lock timeout for share partition: {}-{} memberId: {}",
                            groupId, topicIdPartition, memberId, exception);
                    }
                    // Even if write share group state RPC call fails, we will still go ahead with the state transition.
                    // Update the cached state and start and end offsets after releasing the acquisition lock on timeout.
                    maybeUpdateCachedStateAndOffsets();
                });
            }
        } finally {
            lock.writeLock().unlock();
        }

        // Skip null check for stateBatches, it should always be initialized if reached here.
        if (!stateBatches.isEmpty()) {
            // If we have an acquisition lock timeout for a share-partition, then we should check if
            // there is a pending share fetch request for the share-partition and complete it.
            DelayedShareFetchKey delayedShareFetchKey = new DelayedShareFetchGroupKey(groupId, topicIdPartition.topicId(), topicIdPartition.partition());
            replicaManager.completeDelayedShareFetchRequest(delayedShareFetchKey);
        }
    }

    private void releaseAcquisitionLockOnTimeoutForCompleteBatch(InFlightBatch inFlightBatch,
                                                                 List<PersisterStateBatch> stateBatches,
                                                                 String memberId) {
        if (inFlightBatch.batchState() == RecordState.ACQUIRED) {
            InFlightState updateResult = inFlightBatch.tryUpdateBatchState(
                    inFlightBatch.lastOffset() < startOffset ? RecordState.ARCHIVED : RecordState.AVAILABLE,
                    false,
                    maxDeliveryCount,
                    EMPTY_MEMBER_ID);
            if (updateResult == null) {
                log.error("Unable to release acquisition lock on timeout for the batch: {}"
                        + " for the share partition: {}-{} memberId: {}", inFlightBatch, groupId, topicIdPartition, memberId);
                return;
            }
            stateBatches.add(new PersisterStateBatch(inFlightBatch.firstOffset(), inFlightBatch.lastOffset(),
                    updateResult.state.id, (short) updateResult.deliveryCount));

            // Cancel the acquisition lock timeout task for the batch since it is completed now.
            updateResult.cancelAndClearAcquisitionLockTimeoutTask();
            if (updateResult.state != RecordState.ARCHIVED) {
                findNextFetchOffset.set(true);
            }
            return;
        }
        log.debug("The batch is not in acquired state while release of acquisition lock on timeout, skipping, batch: {}"
                + " for the share partition: {}-{} memberId: {}", inFlightBatch, groupId, topicIdPartition, memberId);
    }

    private void releaseAcquisitionLockOnTimeoutForPerOffsetBatch(InFlightBatch inFlightBatch,
                                                                  List<PersisterStateBatch> stateBatches,
                                                                  String memberId,
                                                                  long firstOffset,
                                                                  long lastOffset) {
        for (Map.Entry<Long, InFlightState> offsetState : inFlightBatch.offsetState().entrySet()) {

            // For the first batch which might have offsets prior to the request base
            // offset i.e. cached batch of 10-14 offsets and request batch of 12-13.
            if (offsetState.getKey() < firstOffset) {
                continue;
            }
            if (offsetState.getKey() > lastOffset) {
                // No further offsets to process.
                break;
            }
            if (offsetState.getValue().state != RecordState.ACQUIRED) {
                log.debug("The offset is not in acquired state while release of acquisition lock on timeout, skipping, offset: {} batch: {}"
                                + " for the share partition: {}-{} memberId: {}", offsetState.getKey(), inFlightBatch,
                        groupId, topicIdPartition, memberId);
                continue;
            }
            InFlightState updateResult = offsetState.getValue().tryUpdateState(
                    offsetState.getKey() < startOffset ? RecordState.ARCHIVED : RecordState.AVAILABLE,
                    false,
                    maxDeliveryCount,
                    EMPTY_MEMBER_ID);
            if (updateResult == null) {
                log.error("Unable to release acquisition lock on timeout for the offset: {} in batch: {}"
                                + " for the share partition: {}-{} memberId: {}", offsetState.getKey(), inFlightBatch,
                        groupId, topicIdPartition, memberId);
                continue;
            }
            stateBatches.add(new PersisterStateBatch(offsetState.getKey(), offsetState.getKey(),
                    updateResult.state.id, (short) updateResult.deliveryCount));

            // Cancel the acquisition lock timeout task for the offset since it is completed now.
            updateResult.cancelAndClearAcquisitionLockTimeoutTask();
            if (updateResult.state != RecordState.ARCHIVED) {
                findNextFetchOffset.set(true);
            }
        }
    }

    private long startOffsetDuringInitialization(long partitionDataStartOffset) throws Exception {
        // Set the state epoch and end offset from the persisted state.
        if (partitionDataStartOffset != PartitionFactory.UNINITIALIZED_START_OFFSET) {
            return partitionDataStartOffset;
        }
        GroupConfig.ShareGroupAutoOffsetReset offsetResetStrategy;
        if (groupConfigManager.groupConfig(groupId).isPresent()) {
            offsetResetStrategy = groupConfigManager.groupConfig(groupId).get().shareAutoOffsetReset();
        } else {
            offsetResetStrategy = GroupConfig.defaultShareAutoOffsetReset();
        }

        if (offsetResetStrategy == GroupConfig.ShareGroupAutoOffsetReset.EARLIEST)
            return offsetForEarliestTimestamp(topicIdPartition, replicaManager, leaderEpoch);
        return offsetForLatestTimestamp(topicIdPartition, replicaManager, leaderEpoch);
    }

    // Visible for testing. Should only be used for testing purposes.
    NavigableMap<Long, InFlightBatch> cachedState() {
        return new ConcurrentSkipListMap<>(cachedState);
    }

    // Visible for testing.
    boolean findNextFetchOffset() {
        return findNextFetchOffset.get();
    }

    // Visible for testing. Should only be used for testing purposes.
    void findNextFetchOffset(boolean findNextOffset) {
        findNextFetchOffset.getAndSet(findNextOffset);
    }

    // Visible for testing
    long startOffset() {
        lock.readLock().lock();
        try {
            return this.startOffset;
        } finally {
            lock.readLock().unlock();
        }
    }

    // Visible for testing
    long endOffset() {
        lock.readLock().lock();
        try {
            return this.endOffset;
        } finally {
            lock.readLock().unlock();
        }
    }

    // Visible for testing.
    int stateEpoch() {
        return stateEpoch;
    }

    // Visible for testing.
    Timer timer() {
        return timer;
    }

    // Visible for testing
    final class AcquisitionLockTimerTask extends TimerTask {
        private final long expirationMs;
        private final String memberId;
        private final long firstOffset;
        private final long lastOffset;

        AcquisitionLockTimerTask(long delayMs, String memberId, long firstOffset, long lastOffset) {
            super(delayMs);
            this.expirationMs = time.hiResClockMs() + delayMs;
            this.memberId = memberId;
            this.firstOffset = firstOffset;
            this.lastOffset = lastOffset;
        }

        long expirationMs() {
            return expirationMs;
        }

        /**
         * The task is executed when the acquisition lock timeout is reached. The task releases the acquired records.
         */
        @Override
        public void run() {
            releaseAcquisitionLockOnTimeout(memberId, firstOffset, lastOffset);
        }
    }

    /**
     * The InFlightBatch maintains the in-memory state of the fetched records i.e. in-flight records.
     */
    final class InFlightBatch {
        // The offset of the first record in the batch that is fetched from the log.
        private final long firstOffset;
        // The last offset of the batch that is fetched from the log.
        private final long lastOffset;

        // The batch state of the fetched records. If the offset state map is empty then batchState
        // determines the state of the complete batch else individual offset determines the state of
        // the respective records.
        private InFlightState batchState;

        // The offset state map is used to track the state of the records per offset. However, the
        // offset state map is only required when the state of the offsets within same batch are
        // different. The states can be different when explicit offset acknowledgment is done which
        // is different from the batch state.
        private NavigableMap<Long, InFlightState> offsetState;

        InFlightBatch(String memberId, long firstOffset, long lastOffset, RecordState state,
            int deliveryCount, AcquisitionLockTimerTask acquisitionLockTimeoutTask
        ) {
            this.firstOffset = firstOffset;
            this.lastOffset = lastOffset;
            this.batchState = new InFlightState(state, deliveryCount, memberId, acquisitionLockTimeoutTask);
        }

        // Visible for testing.
        long firstOffset() {
            return firstOffset;
        }

        // Visible for testing.
        long lastOffset() {
            return lastOffset;
        }

        // Visible for testing.
        RecordState batchState() {
            return inFlightState().state;
        }

        // Visible for testing.
        String batchMemberId() {
            if (batchState == null) {
                throw new IllegalStateException("The batch member id is not available as the offset state is maintained");
            }
            return batchState.memberId;
        }

        // Visible for testing.
        int batchDeliveryCount() {
            if (batchState == null) {
                throw new IllegalStateException("The batch delivery count is not available as the offset state is maintained");
            }
            return batchState.deliveryCount;
        }

        // Visible for testing.
        AcquisitionLockTimerTask batchAcquisitionLockTimeoutTask() {
            return inFlightState().acquisitionLockTimeoutTask;
        }

        // Visible for testing.
        NavigableMap<Long, InFlightState> offsetState() {
            return offsetState;
        }

        private InFlightState inFlightState() {
            if (batchState == null) {
                throw new IllegalStateException("The batch state is not available as the offset state is maintained");
            }
            return batchState;
        }

        private boolean batchHasOngoingStateTransition() {
            return inFlightState().hasOngoingStateTransition();
        }

        private void archiveBatch(String newMemberId) {
            inFlightState().archive(newMemberId);
        }

        private InFlightState tryUpdateBatchState(RecordState newState, boolean incrementDeliveryCount, int maxDeliveryCount, String newMemberId) {
            if (batchState == null) {
                throw new IllegalStateException("The batch state update is not available as the offset state is maintained");
            }
            return batchState.tryUpdateState(newState, incrementDeliveryCount, maxDeliveryCount, newMemberId);
        }

        private InFlightState startBatchStateTransition(RecordState newState, boolean incrementDeliveryCount, int maxDeliveryCount,
                                                        String newMemberId) {
            if (batchState == null) {
                throw new IllegalStateException("The batch state update is not available as the offset state is maintained");
            }
            return batchState.startStateTransition(newState, incrementDeliveryCount, maxDeliveryCount, newMemberId);
        }

        private void maybeInitializeOffsetStateUpdate() {
            if (offsetState == null) {
                offsetState = new ConcurrentSkipListMap<>();
                // The offset state map is not initialized hence initialize the state of the offsets
                // from the first offset to the last offset. Mark the batch inflightState to null as
                // the state of the records is maintained in the offset state map now.
                for (long offset = this.firstOffset; offset <= this.lastOffset; offset++) {
                    if (batchState.acquisitionLockTimeoutTask != null) {
                        // The acquisition lock timeout task is already scheduled for the batch, hence we need to schedule
                        // the acquisition lock timeout task for the offset as well.
                        long delayMs = batchState.acquisitionLockTimeoutTask.expirationMs() - time.hiResClockMs();
                        AcquisitionLockTimerTask timerTask = acquisitionLockTimerTask(batchState.memberId, offset, offset, delayMs);
                        offsetState.put(offset, new InFlightState(batchState.state, batchState.deliveryCount, batchState.memberId, timerTask));
                        timer.add(timerTask);
                    } else {
                        offsetState.put(offset, new InFlightState(batchState.state, batchState.deliveryCount, batchState.memberId));
                    }
                }
                // Cancel the acquisition lock timeout task for the batch as the offset state is maintained.
                if (batchState.acquisitionLockTimeoutTask != null) {
                    batchState.cancelAndClearAcquisitionLockTimeoutTask();
                }
                batchState = null;
            }
        }

        private void updateAcquisitionLockTimeout(AcquisitionLockTimerTask acquisitionLockTimeoutTask) {
            inFlightState().acquisitionLockTimeoutTask = acquisitionLockTimeoutTask;
        }

        @Override
        public String toString() {
            return "InFlightBatch(" +
                "firstOffset=" + firstOffset +
                ", lastOffset=" + lastOffset +
                ", inFlightState=" + batchState +
                ", offsetState=" + ((offsetState == null) ? "null" : offsetState) +
                ")";
        }
    }

    /**
     * The InFlightState is used to track the state and delivery count of a record that has been
     * fetched from the leader. The state of the record is used to determine if the record should
     * be re-deliver or if it can be acknowledged or archived.
     */
    static final class InFlightState {

        // The state of the fetch batch records.
        private RecordState state;
        // The number of times the records has been delivered to the client.
        private int deliveryCount;
        // The member id of the client that is fetching/acknowledging the record.
        private String memberId;
        // The state of the records before the transition. In case we need to revert an in-flight state, we revert the above
        // attributes of InFlightState to this state, namely - state, deliveryCount and memberId.
        private InFlightState rollbackState;
        // The timer task for the acquisition lock timeout.
        private AcquisitionLockTimerTask acquisitionLockTimeoutTask;


        InFlightState(RecordState state, int deliveryCount, String memberId) {
            this(state, deliveryCount, memberId, null);
        }

        InFlightState(RecordState state, int deliveryCount, String memberId, AcquisitionLockTimerTask acquisitionLockTimeoutTask) {
            this.state = state;
            this.deliveryCount = deliveryCount;
            this.memberId = memberId;
            this.acquisitionLockTimeoutTask = acquisitionLockTimeoutTask;
        }

        // Visible for testing.
        RecordState state() {
            return state;
        }

        String memberId() {
            return memberId;
        }

        // Visible for testing.
        TimerTask acquisitionLockTimeoutTask() {
            return acquisitionLockTimeoutTask;
        }

        void updateAcquisitionLockTimeoutTask(AcquisitionLockTimerTask acquisitionLockTimeoutTask) throws IllegalArgumentException {
            if (this.acquisitionLockTimeoutTask != null) {
                throw new IllegalArgumentException("Existing acquisition lock timeout exists, cannot override.");
            }
            this.acquisitionLockTimeoutTask = acquisitionLockTimeoutTask;
        }

        void cancelAndClearAcquisitionLockTimeoutTask() {
            acquisitionLockTimeoutTask.cancel();
            acquisitionLockTimeoutTask = null;
        }

        private boolean hasOngoingStateTransition() {
            if (rollbackState == null) {
                // This case could occur when the batch/offset hasn't transitioned even once or the state transitions have
                // been committed.
                return false;
            }
            return rollbackState.state != null;
        }

        /**
         * Try to update the state of the records. The state of the records can only be updated if the
         * new state is allowed to be transitioned from old state. The delivery count is not incremented
         * if the state update is unsuccessful.
         *
         * @param newState The new state of the records.
         * @param incrementDeliveryCount Whether to increment the delivery count.
         *
         * @return {@code InFlightState} if update succeeds, null otherwise. Returning state
         *         helps update chaining.
         */
        private InFlightState tryUpdateState(RecordState newState, boolean incrementDeliveryCount, int maxDeliveryCount, String newMemberId) {
            try {
                if (newState == RecordState.AVAILABLE && deliveryCount >= maxDeliveryCount) {
                    newState = RecordState.ARCHIVED;
                }
                state = state.validateTransition(newState);
                if (incrementDeliveryCount && newState != RecordState.ARCHIVED) {
                    deliveryCount++;
                }
                memberId = newMemberId;
                return this;
            } catch (IllegalStateException e) {
                log.error("Failed to update state of the records", e);
                return null;
            }
        }

        private void archive(String newMemberId) {
            state = RecordState.ARCHIVED;
            memberId = newMemberId;
        }

        private InFlightState startStateTransition(RecordState newState, boolean incrementDeliveryCount, int maxDeliveryCount, String newMemberId) {
            rollbackState = new InFlightState(state, deliveryCount, memberId, acquisitionLockTimeoutTask);
            return tryUpdateState(newState, incrementDeliveryCount, maxDeliveryCount, newMemberId);
        }

        private void completeStateTransition(boolean commit) {
            if (commit) {
                rollbackState = null;
                return;
            }
            state = rollbackState.state;
            deliveryCount = rollbackState.deliveryCount;
            memberId = rollbackState.memberId;
            rollbackState = null;
        }

        @Override
        public int hashCode() {
            return Objects.hash(state, deliveryCount, memberId);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            InFlightState that = (InFlightState) o;
            return state == that.state && deliveryCount == that.deliveryCount && memberId.equals(that.memberId);
        }

        @Override
        public String toString() {
            return "InFlightState(" +
                "state=" + state.toString() +
                ", deliveryCount=" + deliveryCount +
                ", memberId=" + memberId +
                ")";
        }
    }

    /**
     * FetchOffsetMetadata class is used to cache offset and its log metadata.
     */
    static final class OffsetMetadata {
        // This offset could be different from offsetMetadata.messageOffset if it's in the middle of a batch.
        private long offset;
        private LogOffsetMetadata offsetMetadata;

        OffsetMetadata() {
            offset = -1;
        }

        long offset() {
            return offset;
        }

        LogOffsetMetadata offsetMetadata() {
            return offsetMetadata;
        }

        void updateOffsetMetadata(long offset, LogOffsetMetadata offsetMetadata) {
            this.offset = offset;
            this.offsetMetadata = offsetMetadata;
        }
    }
}
