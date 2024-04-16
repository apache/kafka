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
package kafka.server;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.errors.InvalidRecordStateException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.message.ShareFetchResponseData.AcquiredRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.util.timer.Timer;
import org.apache.kafka.server.util.timer.TimerTask;
import org.apache.kafka.storage.internals.log.FetchPartitionData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * The SharePartition is used to track the state of a partition that is shared between multiple
 * consumers. The class maintains the state of the records that have been fetched from the leader
 * and are in-flight.
 */
public class SharePartition {

    private final static Logger log = LoggerFactory.getLogger(SharePartition.class);

    /**
     * The RecordState is used to track the state of a record that has been fetched from the leader.
     * The state of the records determines if the records should be re-delivered, move the next fetch
     * offset, or be state persisted to disk.
     */
    public enum RecordState {
        AVAILABLE((byte) 0),
        ACQUIRED((byte) 1),
        ACKNOWLEDGED((byte) 2),
        ARCHIVED((byte) 3);

        public final byte id;
        RecordState(byte id) {
            this.id = id;
        }

        /**
         * Validates that the <code>newState</code> is one of the valid transition from the current
         * {@code RecordState}.
         *
         * @param newState State into which requesting to transition; must be non-<code>null</code>
         * @return {@code RecordState} <code>newState</code> if validation succeeds. Returning
         * <code>newState</code> helps state assignment chaining.
         * @throws IllegalStateException if the state transition validation fails.
         */

        public RecordState validateTransition(RecordState newState) {
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
                case 3:
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
     * The in-flight record is used to track the state of a record that has been fetched from the
     * leader. The state of the record is used to determine if the record should be re-fetched or if it
     * can be acknowledged or archived. Once share partition start offset is moved then the in-flight
     * records prior to the start offset are removed from the cache. The cache holds data against the
     * base offset of the in-flight batch.
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
     * The share partition end offset specifies the partition end offset from which the records
     * are already fetched.
     */
    private long endOffset;

    /**
     * The next fetch offset specifies the partition offset which should be fetched from the leader
     * for the share partition.
     */
    private long nextFetchOffset;
    /**
     * The record lock duration is used to limit the duration for which a consumer can acquire a record.
     * Once this time period is elapsed, the record will be made available or archived depending on the delivery count.
     */
    private final int recordLockDurationMs;
    /**
     * Timer is used to implement acquisition lock on records that guarantees the movement of records from
     * acquired to available/archived state upon timeout
     */
    private final Timer timer;
    /**
     * Time is used to get the currentTime.
     */
    private final Time time;

    SharePartition(String groupId, TopicIdPartition topicIdPartition, int maxInFlightMessages, int maxDeliveryCount,
                   int recordLockDurationMs, Timer timer, Time time) {
        this.groupId = groupId;
        this.topicIdPartition = topicIdPartition;
        this.maxInFlightMessages = maxInFlightMessages;
        this.maxDeliveryCount = maxDeliveryCount;
        this.cachedState = new ConcurrentSkipListMap<>();
        this.lock = new ReentrantReadWriteLock();
        // Should be initialized when the partition is loaded by reading state from the share persister.
        this.nextFetchOffset = 0;
        this.endOffset = 0;
        this.findNextFetchOffset = new AtomicBoolean(false);
        // TODO: Just a placeholder for now.
        assert this.maxInFlightMessages > 0;
        assert this.maxDeliveryCount > 0;
        this.fetchLock = new AtomicBoolean(false);
        this.recordLockDurationMs = recordLockDurationMs;
        this.timer = timer;
        this.time = time;
    }

    /**
     * The next fetch offset is used to determine the next offset that should be fetched from the leader.
     * The offset should be the next offset after the last fetched batch but there could be batches/
     * offsets that are either released by acknowledgement API or lock timed out hence the next fetch
     * offset might be different from the last batch next offset. Hence, method checks if the next
     * fetch offset should be recomputed else returns the last computed next fetch offset.
     *
     * @return The next fetch offset that should be fetched from the leader.
     */
    public long nextFetchOffset() {
        if (findNextFetchOffset.compareAndSet(true, false)) {
            lock.writeLock().lock();
            try {
                // Find the next fetch offset.
                long baseOffset = nextFetchOffset;
                // If the re-computation is required then there occurred an overlap with the existing
                // in-flight records, hence find the batch from which the last offsets were acquired.
                Map.Entry<Long, InFlightBatch> floorEntry = cachedState.floorEntry(nextFetchOffset);
                if (floorEntry != null) {
                    baseOffset = floorEntry.getKey();
                }
                NavigableMap<Long, InFlightBatch> subMap = cachedState.subMap(baseOffset, true, endOffset, true);
                if (subMap.isEmpty()) {
                    // The in-flight batches are removed, might be due to the start offset move. Hence,
                    // the next fetch offset should be the partition end offset + 1.
                    nextFetchOffset = endOffset + 1;
                } else {
                    boolean updated = false;
                    for (Map.Entry<Long, InFlightBatch> entry : subMap.entrySet()) {
                        // Check if the state is maintained per offset or batch. If the offsetState
                        // is not maintained then the batch state is used to determine the offsets state.
                        if (entry.getValue().offsetState() == null) {
                            if (entry.getValue().batchState() == RecordState.AVAILABLE) {
                                nextFetchOffset = entry.getValue().baseOffset();
                                updated = true;
                                break;
                            }
                        } else {
                            // The offset state is maintained hence find the next available offset.
                            for (Map.Entry<Long, InFlightState> offsetState : entry.getValue().offsetState().entrySet()) {
                                if (offsetState.getValue().state == RecordState.AVAILABLE) {
                                    nextFetchOffset = offsetState.getKey();
                                    updated = true;
                                    break;
                                }
                            }
                            // Break from the outer loop if updated.
                            if (updated) {
                                break;
                            }
                        }
                    }
                    // No available records found in the fetched batches.
                    if (!updated) {
                        nextFetchOffset = endOffset + 1;
                    }
                }
            } finally {
                lock.writeLock().unlock();
            }
        }

        lock.readLock().lock();
        try {
            return nextFetchOffset;
        } finally {
            lock.readLock().unlock();
        }
    }

    public void releaseFetchLock() {
        fetchLock.set(false);
    }

    public boolean maybeAcquireFetchLock() {
        return fetchLock.compareAndSet(false, true);
    }

    /**
     * Acquire the fetched records for the share partition. The acquired records are added to the
     * in-flight records and the next fetch offset is updated to the next offset that should be
     * fetched from the leader.
     *
     * @param memberId The member id of the client that is fetching the record.
     * @param fetchPartitionData The fetched records for the share partition.
     *
     * @return A future which is completed when the records are acquired.
     */
    public CompletableFuture<List<AcquiredRecords>> acquire(String memberId, FetchPartitionData fetchPartitionData) {
        RecordBatch lastBatch = fetchPartitionData.records.lastBatch().orElse(null);
        if (lastBatch == null) {
            // Nothing to acquire.
            return CompletableFuture.completedFuture(Collections.emptyList());
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
            if (floorOffset != null && floorOffset.getValue().lastOffset >= baseOffset) {
                baseOffset = floorOffset.getKey();
            }
            // Validate if the fetch records are already part of existing batches and if available.
            NavigableMap<Long, InFlightBatch> subMap = cachedState.subMap(baseOffset, true, lastBatch.lastOffset(), true);
            // No overlap with request offsets in the cache for in-flight records. Acquire the complete
            // batch.
            if (subMap.isEmpty()) {
                log.trace("No cached data exists for the share partition for requested fetch batch: {}-{}",
                    groupId, topicIdPartition);
                return CompletableFuture.completedFuture(Collections.singletonList(
                            acquireNewBatchRecords(memberId, firstBatch.baseOffset(), lastBatch.lastOffset(),
                            lastBatch.nextOffset())));
            }

            log.trace("Overlap exists with in-flight records. Acquire the records if available for"
                + " the share group: {}-{}", groupId, topicIdPartition);
            List<AcquiredRecords> result = new ArrayList<>();
            // The fetched records are already part of the in-flight records. The records might
            // be available for re-delivery hence try acquiring same. The request batches could
            // be an exact match, subset or span over multiple already fetched batches.
            for (Map.Entry<Long, InFlightBatch> entry : subMap.entrySet()) {
                InFlightBatch inFlightBatch = entry.getValue();
                // Compute if the batch is a full match.
                boolean fullMatch = inFlightBatch.baseOffset() >= firstBatch.baseOffset()
                    && inFlightBatch.lastOffset() <= lastBatch.lastOffset();

                if (!fullMatch || inFlightBatch.offsetState != null) {
                    log.trace("Subset or offset tracked batch record found for share partition,"
                            + " batch: {} request offsets - first: {}, last: {} for the share"
                            + " group: {}-{}", inFlightBatch, firstBatch.baseOffset(),
                            lastBatch.lastOffset(), groupId, topicIdPartition);
                    if (inFlightBatch.offsetState == null) {
                        // Though the request is a subset of in-flight batch but the offset
                        // tracking has not been initialized yet which means that we could only
                        // acquire subset of offsets from the in-flight batch but only if the
                        // complete batch is available yet. Hence, do a pre-check to avoid exploding
                        // the in-flight offset tracking unnecessarily.
                        if (inFlightBatch.batchState() != RecordState.AVAILABLE) {
                            log.trace("The batch is not available to acquire in share group: {}-{}, skipping: {}"
                                    + " skipping offset tracking for batch as well.", groupId,
                                topicIdPartition, inFlightBatch);
                            continue;
                        }
                        // The request batch is a subset or per offset state is managed hence update
                        // the offsets state in the in-flight batch.
                        inFlightBatch.maybeInitializeOffsetStateUpdate();
                    }
                    acquireSubsetBatchRecords(memberId, firstBatch.baseOffset(), lastBatch.lastOffset(), inFlightBatch, result);
                    continue;
                }

                // The in-flight batch is a full match hence change the state of the complete.
                if (inFlightBatch.batchState() != RecordState.AVAILABLE) {
                    log.trace("The batch is not available to acquire in share group: {}-{}, skipping: {}",
                        groupId, topicIdPartition, inFlightBatch);
                    continue;
                }

                InFlightState updateResult = inFlightBatch.tryUpdateBatchState(RecordState.ACQUIRED, true);
                if (updateResult == null) {
                    log.info("Unable to acquire records for the batch: {} in share group: {}-{}",
                        inFlightBatch, groupId, topicIdPartition);
                    continue;
                }
                // Schedule acquisition lock timeout for the batch.
                TimerTask acquisitionLockTimeoutTask = scheduleAcquisitionLockTimeout(memberId, inFlightBatch.baseOffset(), inFlightBatch.lastOffset());
                // Set the acquisition lock timeout task for the batch.
                inFlightBatch.updateAcquisitionLockTimeout(acquisitionLockTimeoutTask);

                findNextFetchOffset.set(true);
                result.add(new AcquiredRecords()
                    .setBaseOffset(inFlightBatch.baseOffset())
                    .setLastOffset(inFlightBatch.lastOffset())
                    .setDeliveryCount((short) inFlightBatch.batchDeliveryCount()));
            }

            // Some of the request offsets are not found in the fetched batches. Acquire the
            // missing records as well.
            if (subMap.lastEntry().getValue().lastOffset < lastBatch.lastOffset()) {
                log.trace("There exists another batch which needs to be acquired as well");
                result.add(acquireNewBatchRecords(memberId, subMap.lastEntry().getValue().lastOffset() + 1,
                    lastBatch.lastOffset(), lastBatch.nextOffset()));
            }
            return CompletableFuture.completedFuture(result);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Acknowledge the fetched records for the share partition. The accepted batches are removed
     * from the in-flight records once persisted. The next fetch offset is updated to the next offset
     * that should be fetched from the leader, if required.
     *
     * @param memberId The member id of the client that is fetching the record.
     * @param acknowledgementBatch The acknowledgement batch list for the share partition.
     *
     * @return A future which is completed when the records are acknowledged.
     */
    public CompletableFuture<Optional<Throwable>> acknowledge(String memberId, List<AcknowledgementBatch> acknowledgementBatch) {
        log.trace("Acknowledgement batch request for share partition: {}-{}", groupId, topicIdPartition);

        Throwable throwable = null;
        lock.writeLock().lock();
        List<InFlightState> updatedStates = new ArrayList<>();
        try {
            long localNextFetchOffset = nextFetchOffset;
            // Avoided using enhanced for loop as need to check if the last batch have offsets
            // in the range.
            for (int i = 0; i < acknowledgementBatch.size(); i++) {
                AcknowledgementBatch batch = acknowledgementBatch.get(i);

                RecordState recordState;
                try {
                    recordState = fetchRecordState(batch.acknowledgeType);
                } catch (IllegalArgumentException e) {
                    log.debug("Invalid acknowledge type: {} for share partition: {}-{}",
                        batch.acknowledgeType, groupId, topicIdPartition);
                    throwable = new InvalidRequestException("Invalid acknowledge type: " + batch.acknowledgeType);
                    break;
                }

                // Find the floor batch record for the request batch. The request batch could be
                // for a subset of the batch i.e. cached batch of offset 10-14 and request batch
                // of 12-13. Hence, floor entry is fetched to find the sub-map.
                Map.Entry<Long, InFlightBatch> floorOffset = cachedState.floorEntry(batch.baseOffset);
                if (floorOffset == null) {
                    log.debug("Batch record {} not found for share partition: {}-{}", batch, groupId, topicIdPartition);
                    throwable = new InvalidRecordStateException("Batch record not found. The base offset is not found in the cache.");
                    break;
                }

                NavigableMap<Long, InFlightBatch> subMap = cachedState.subMap(floorOffset.getKey(), true, batch.lastOffset, true);

                // Validate if the request batch has the last offset greater than the last offset of
                // the last fetched cached batch, then there will be offsets in the request than cannot
                // be found in the fetched batches.
                if (i == acknowledgementBatch.size() - 1 && batch.lastOffset > subMap.lastEntry().getValue().lastOffset) {
                    log.debug("Request batch: {} has offsets which are not found for share partition: {}-{}", batch, groupId, topicIdPartition);
                    throwable = new InvalidRequestException("Batch record not found. The last offset in request is past acquired records.");
                    break;
                }

                // Add the gap offsets. There is no need to roll back the gap offsets for any
                // in-flight batch if the acknowledgement request for any batch fails as once
                // identified gap offset should remain the gap offset in future requests.
                Set<Long> gapOffsetsSet = null;
                if (batch.gapOffsets != null && !batch.gapOffsets.isEmpty()) {
                    // Keep a set to easily check if the gap offset is part of the iterated in-flight
                    // batch.
                    gapOffsetsSet = new HashSet<>(batch.gapOffsets);
                }

                boolean updateNextFetchOffset = batch.acknowledgeType == AcknowledgeType.RELEASE;
                // The acknowledgement batch either is exact fetch equivalent batch (mostly), subset
                // or spans over multiple fetched batches. The state can vary per offset itself from
                // the fetched batch in case of subset.
                for (Map.Entry<Long, InFlightBatch> entry : subMap.entrySet()) {
                    InFlightBatch inFlightBatch = entry.getValue();

                    if (!inFlightBatch.memberId().equals(memberId)) {
                        log.debug("Member {} is not the owner of batch record {} for share partition: {}-{}",
                            memberId, inFlightBatch, groupId, topicIdPartition);
                        throwable = new InvalidRequestException("Member is not the owner of batch record");
                        break;
                    }

                    boolean fullMatch = inFlightBatch.baseOffset() >= batch.baseOffset
                        && inFlightBatch.lastOffset() <= batch.lastOffset;

                    if (!fullMatch || inFlightBatch.offsetState != null) {
                        log.debug("Subset or offset tracked batch record found for acknowledgement,"
                                + " batch: {} request offsets - first: {}, last: {} for the share"
                                + " partition: {}-{}", inFlightBatch, batch.baseOffset, batch.lastOffset,
                                groupId, topicIdPartition);
                        if (inFlightBatch.offsetState == null) {
                            // Though the request is a subset of in-flight batch but the offset
                            // tracking has not been initialized yet which means that we could only
                            // acknowledge subset of offsets from the in-flight batch but only if the
                            // complete batch is acquired yet. Hence, do a pre-check to avoid exploding
                            // the in-flight offset tracking unnecessarily.
                            if (inFlightBatch.batchState() != RecordState.ACQUIRED) {
                                log.debug("The batch is not in the acquired state: {} for share partition: {}-{}",
                                    inFlightBatch, groupId, topicIdPartition);
                                throwable = new InvalidRecordStateException("The batch cannot be acknowledged. The subset batch is not in the acquired state.");
                                break;
                            }
                            // The request batch is a subset or per offset state is managed hence update
                            // the offsets state in the in-flight batch.
                            inFlightBatch.maybeInitializeOffsetStateUpdate();
                        }
                        for (Map.Entry<Long, InFlightState> offsetState : inFlightBatch.offsetState.entrySet()) {
                            // For the first batch which might have offsets prior to the request base
                            // offset i.e. cached batch of 10-14 offsets and request batch of 12-13.
                            if (offsetState.getKey() < batch.baseOffset) {
                                continue;
                            }

                            if (offsetState.getKey() > batch.lastOffset) {
                                // No further offsets to process.
                                break;
                            }

                            // Add the gap offsets.
                            if (gapOffsetsSet != null && gapOffsetsSet.contains(offsetState.getKey())) {
                                inFlightBatch.addGapOffsets(offsetState.getKey());
                                // Force the state of the offset to Archived for the gap offset
                                // irrespectively.
                                offsetState.getValue().state = RecordState.ARCHIVED;
                                // Cancel and clear the acquisition lock timeout task for the state since it is moved to ARCHIVED state.
                                offsetState.getValue().cancelAndClearAcquisitionLockTimeoutTask();
                                continue;
                            }

                            if (offsetState.getValue().state != RecordState.ACQUIRED) {
                                log.debug("The offset is not acquired, offset: {} batch: {} for the share"
                                    + " partition: {}-{}", offsetState.getKey(), inFlightBatch, groupId, topicIdPartition);
                                throwable = new InvalidRecordStateException("The batch cannot be acknowledged. The offset is not acquired.");
                                break;
                            }

                            InFlightState updateResult =  offsetState.getValue().startStateTransition(recordState, false);
                            if (updateResult == null) {
                                log.debug("Unable to acknowledge records for the offset: {} in batch: {}"
                                        + " for the share partition: {}-{}", offsetState.getKey(),
                                    inFlightBatch, groupId, topicIdPartition);
                                throwable = new InvalidRecordStateException("Unable to acknowledge records for the batch");
                                break;
                            }
                            // Successfully updated the state of the offset.
                            updatedStates.add(updateResult);
                            if (updateNextFetchOffset) {
                                localNextFetchOffset = Math.min(offsetState.getKey(), localNextFetchOffset);
                            }
                        }
                        continue;
                    }

                    // The in-flight batch is a full match hence change the state of the complete.
                    log.trace("Acknowledging complete batch record {} for the share partition: {}-{}",
                        batch, groupId, topicIdPartition);
                    if (inFlightBatch.batchState() != RecordState.ACQUIRED) {
                        log.debug("The batch is not in the acquired state: {} for share partition: {}-{}",
                            inFlightBatch, groupId, topicIdPartition);
                        throwable = new InvalidRecordStateException("The batch cannot be acknowledged. The batch is not in the acquired state.");
                        break;
                    }

                    InFlightState updateResult = inFlightBatch.startBatchStateTransition(recordState, false);
                    if (updateResult == null) {
                        log.debug("Unable to acknowledge records for the batch: {} with state: {}"
                            + " for the share partition: {}-{}", inFlightBatch, recordState, groupId, topicIdPartition);
                        throwable = new InvalidRecordStateException("Unable to acknowledge records for the batch");
                        break;
                    }

                    // Add the gap offsets.
                    if (batch.gapOffsets != null && !batch.gapOffsets.isEmpty()) {
                        for (Long gapOffset : batch.gapOffsets) {
                            if (inFlightBatch.baseOffset() <= gapOffset && inFlightBatch.lastOffset() >= gapOffset) {
                                inFlightBatch.addGapOffsets(gapOffset);
                                // Just track the gap offset, no need to change the state of the offset
                                // as the offset tracking is not maintained for the batch.
                            }
                        }
                    }

                    // Successfully updated the state of the batch.
                    updatedStates.add(updateResult);
                    if (updateNextFetchOffset) {
                        localNextFetchOffset = Math.min(inFlightBatch.baseOffset, localNextFetchOffset);
                    }
                }

                if (throwable != null) {
                    break;
                }
            }

            if (throwable != null) {
                // the log should be DEBUG to avoid flooding of logs for a faulty client
                log.debug("Acknowledgement batch request failed for share partition, rollback any changed state"
                    + " for the share partition: {}-{}", groupId, topicIdPartition);
                updatedStates.forEach(state -> state.completeStateTransition(false));
            } else {
                log.trace("Acknowledgement batch request successful for share partition: {}-{}",
                    groupId, topicIdPartition);
                updatedStates.forEach(state -> {
                    state.completeStateTransition(true);
                    // Cancel the acquisition lock timeout task for the state since it is acknowledged successfully.
                    state.cancelAndClearAcquisitionLockTimeoutTask();
                });

                nextFetchOffset = localNextFetchOffset;
            }
        } finally {
            lock.writeLock().unlock();
        }

        return CompletableFuture.completedFuture(Optional.ofNullable(throwable));
    }

    /**
     * Release the acquired records for the share partition. The next fetch offset is updated to the next offset
     * that should be fetched from the leader.
     *
     * @param memberId The member id of the client that is fetching/acknowledging the record.
     *
     * @return A future which is completed when the records are released.
     */
    public CompletableFuture<Optional<Throwable>> releaseAcquiredRecords(String memberId) {
        log.trace("Release acquired records request for share partition: {}-{}-{}", groupId, memberId, topicIdPartition);

        Throwable throwable = null;
        lock.writeLock().lock();
        List<InFlightState> updatedStates = new ArrayList<>();
        try {
            long localNextFetchOffset = nextFetchOffset;
            // TODO: recordState can be ARCHIVED as well, but that will involve maxDeliveryCount which is not implemented so far
            RecordState recordState = RecordState.AVAILABLE;

            // Iterate over multiple fetched batches. The state can vary per offset entry
            for (Map.Entry<Long, InFlightBatch> entry : cachedState.entrySet()) {
                InFlightBatch inFlightBatch = entry.getValue();

                if (!inFlightBatch.memberId().equals(memberId)) {
                    log.info("Member {} is not the owner of batch record {} for share partition: {}-{}",
                            memberId, inFlightBatch, groupId, topicIdPartition);
                    continue;
                }

                if (inFlightBatch.offsetState != null) {
                    log.trace("Offset tracked batch record found, batch: {} for the share partition: {}-{}", inFlightBatch,
                            groupId, topicIdPartition);
                    for (Map.Entry<Long, InFlightState> offsetState : inFlightBatch.offsetState.entrySet()) {
                        if (offsetState.getValue().state == RecordState.ACQUIRED) {
                            InFlightState updateResult = offsetState.getValue().startStateTransition(recordState, false);
                            if (updateResult == null) {
                                log.debug("Unable to release records from acquired state for the offset: {} in batch: {}"
                                                + " for the share partition: {}-{}", offsetState.getKey(),
                                        inFlightBatch, groupId, topicIdPartition);
                                throwable = new InvalidRecordStateException("Unable to release acquired records for the batch");
                                break;
                            }
                            // Successfully updated the state of the offset.
                            updatedStates.add(updateResult);
                            localNextFetchOffset = Math.min(offsetState.getKey(), localNextFetchOffset);
                        }
                    }
                    if (throwable != null)
                        break;
                    continue;
                }

                // Change the state of complete batch since the same state exists for the entire inFlight batch
                log.trace("Releasing ACQUIRED records for complete batch {} for the share partition: {}-{}",
                        inFlightBatch, groupId, topicIdPartition);

                if (inFlightBatch.batchState() == RecordState.ACQUIRED) {
                    InFlightState updateResult = inFlightBatch.startBatchStateTransition(recordState, false);
                    if (updateResult == null) {
                        log.debug("Unable to release records from acquired state for the batch: {}"
                                        + " for the share partition: {}-{}", inFlightBatch, groupId, topicIdPartition);
                        throwable = new InvalidRecordStateException("Unable to release acquired records for the batch");
                        break;
                    }
                    // Successfully updated the state of the batch.
                    updatedStates.add(updateResult);
                    localNextFetchOffset = Math.min(inFlightBatch.baseOffset, localNextFetchOffset);
                }
            }

            if (throwable != null) {
                log.debug("Release records from acquired state failed for share partition, rollback any changed state"
                        + " for the share partition: {}-{}", groupId, topicIdPartition);
                updatedStates.forEach(state -> state.completeStateTransition(false));
            } else {
                log.trace("Release records from acquired state successful for share partition: {}-{}",
                        groupId, topicIdPartition);
                updatedStates.forEach(state -> {
                    state.completeStateTransition(true);
                    // Cancel the acquisition lock timeout task for the state since it is released successfully.
                    state.cancelAndClearAcquisitionLockTimeoutTask();
                });
                nextFetchOffset = localNextFetchOffset;
            }
        } finally {
            lock.writeLock().unlock();
        }
        return CompletableFuture.completedFuture(Optional.ofNullable(throwable));
    }

    private AcquiredRecords acquireNewBatchRecords(String memberId, long baseOffset, long lastOffset, long nextOffset) {
        lock.writeLock().lock();
        try {
            // Schedule acquisition lock timeout for the batch.
            TimerTask timerTask = scheduleAcquisitionLockTimeout(memberId, baseOffset, lastOffset);
            // Add the new batch to the in-flight records along with the acquisition lock timeout task for the batch.
            cachedState.put(baseOffset, new InFlightBatch(
                memberId,
                baseOffset,
                lastOffset,
                RecordState.ACQUIRED,
                1,
                timerTask));
            endOffset = lastOffset;
            nextFetchOffset = nextOffset;
            return new AcquiredRecords()
                .setBaseOffset(baseOffset)
                .setLastOffset(lastOffset)
                .setDeliveryCount((short) 1);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void acquireSubsetBatchRecords(String memberId, long requestBaseOffset, long requestLastOffset,
        InFlightBatch inFlightBatch, List<AcquiredRecords> result) {
        lock.writeLock().lock();
        try {
            for (Map.Entry<Long, InFlightState> offsetState : inFlightBatch.offsetState.entrySet()) {
                // For the first batch which might have offsets prior to the request base
                // offset i.e. cached batch of 10-14 offsets and request batch of 12-13.
                if (offsetState.getKey() < requestBaseOffset) {
                    continue;
                }

                if (offsetState.getKey() > requestLastOffset) {
                    // No further offsets to process.
                    break;
                }

                if (offsetState.getValue().state != RecordState.AVAILABLE) {
                    log.trace("The offset is not available skipping, offset: {} batch: {}"
                            + " for the share group: {}-{}", offsetState.getKey(), inFlightBatch,
                        groupId, topicIdPartition);
                    continue;
                }

                InFlightState updateResult =  offsetState.getValue().tryUpdateState(RecordState.ACQUIRED, true);
                if (updateResult == null) {
                    log.trace("Unable to acquire records for the offset: {} in batch: {}"
                            + " for the share group: {}-{}", offsetState.getKey(), inFlightBatch,
                        groupId, topicIdPartition);
                    continue;
                }
                // Schedule acquisition lock timeout for the offset.
                TimerTask acquisitionLockTimeoutTask = scheduleAcquisitionLockTimeout(memberId, offsetState.getKey(), offsetState.getKey());
                // Update acquisition lock timeout task for the offset.
                offsetState.getValue().updateAcquisitionLockTimeoutTask(acquisitionLockTimeoutTask);

                findNextFetchOffset.set(true);
                // TODO: Maybe we can club the continuous offsets here.
                result.add(new AcquiredRecords()
                    .setBaseOffset(offsetState.getKey())
                    .setLastOffset(offsetState.getKey())
                    .setDeliveryCount((short) offsetState.getValue().deliveryCount));
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private static RecordState fetchRecordState(AcknowledgeType acknowledgeType) {
        switch (acknowledgeType) {
            case ACCEPT:
                return RecordState.ACKNOWLEDGED;
            case RELEASE:
                return RecordState.AVAILABLE;
            case REJECT:
                return RecordState.ARCHIVED;
            default:
                throw new IllegalArgumentException("Invalid acknowledge type: " + acknowledgeType);
        }
    }

    // Visible for testing. Should only be used for testing purposes.
    NavigableMap<Long, InFlightBatch> cachedState() {
        return new ConcurrentSkipListMap<>(cachedState);
    }

    // Visible for testing.
    Timer timer() {
        return timer;
    }

    private TimerTask scheduleAcquisitionLockTimeout(String memberId, long baseOffset, long lastOffset) {
        return scheduleAcquisitionLockTimeout(memberId, baseOffset, lastOffset, recordLockDurationMs);
    }

    // TODO: maxDeliveryCount should be utilized here once it is implemented
    /**
     * Apply acquisition lock to acquired records.
     * @param memberId The member id of the client that is putting the acquisition lock.
     * @param baseOffset The base offset of the acquired records.
     * @param lastOffset The last offset of the acquired records.
     */
    private TimerTask scheduleAcquisitionLockTimeout(String memberId, long baseOffset, long lastOffset, long delayMs) {
        TimerTask acquistionLockTimerTask = acquisitionLockTimerTask(memberId, baseOffset, lastOffset, delayMs);
        timer.add(acquistionLockTimerTask);
        return acquistionLockTimerTask;
    }

    private TimerTask acquisitionLockTimerTask(String memberId, long baseOffset, long lastOffset, long delayMs) {
        return new AcquisitionLockTimerTask(delayMs, memberId, baseOffset, lastOffset);
    }

    private final class AcquisitionLockTimerTask extends TimerTask {
        private final long expirationMs;
        private final String memberId;
        private final long baseOffset, lastOffset;

        AcquisitionLockTimerTask(long delayMs, String memberId, long baseOffset, long lastOffset) {
            super(delayMs);
            this.expirationMs = time.hiResClockMs() + delayMs;
            this.memberId = memberId;
            this.baseOffset = baseOffset;
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
            releaseAcquisitionLockOnTimeout(memberId, baseOffset, lastOffset);
        }
    }

    private void releaseAcquisitionLockOnTimeout(String memberId, long baseOffset, long lastOffset) {
        lock.writeLock().lock();
        try {
            Map.Entry<Long, InFlightBatch> floorOffset = cachedState.floorEntry(baseOffset);
            if (floorOffset == null) {
                log.debug("Base Offset {} not found for share partition: {}-{}", baseOffset, groupId, topicIdPartition);
            } else {
                NavigableMap<Long, InFlightBatch> subMap = cachedState.subMap(floorOffset.getKey(), true, lastOffset, true);
                long localNextFetchOffset = nextFetchOffset;
                for (Map.Entry<Long, InFlightBatch> entry : subMap.entrySet()) {
                    InFlightBatch inFlightBatch = entry.getValue();
                    // Case when the state of complete batch is valid
                    if (inFlightBatch.offsetState == null) {
                        if (inFlightBatch.batchState() == RecordState.ACQUIRED) {
                            InFlightState updateResult = inFlightBatch.tryUpdateBatchState(RecordState.AVAILABLE, false);
                            if (updateResult == null) {
                                log.debug("Unable to release acquisition lock on timeout for the batch: {}"
                                        + " for the share partition: {}-{}-{}", inFlightBatch, groupId, memberId, topicIdPartition);
                            } else {
                                // Update acquisition lock timeout task for the batch to null since it is completed now.
                                updateResult.updateAcquisitionLockTimeoutTask(null);
                                localNextFetchOffset = Math.min(entry.getKey(), localNextFetchOffset);
                            }
                        } else {
                            log.debug("The batch is not in acquired state while release of acquisition lock on timeout, skipping, batch: {}"
                                    + " for the share group: {}-{}-{}", inFlightBatch, groupId, memberId, topicIdPartition);
                        }
                    } else { // Case when batch has a valid offset state map.
                        for (Map.Entry<Long, InFlightState> offsetState : inFlightBatch.offsetState.entrySet()) {
                            // For the first batch which might have offsets prior to the request base
                            // offset i.e. cached batch of 10-14 offsets and request batch of 12-13.
                            if (offsetState.getKey() < baseOffset) {
                                continue;
                            }

                            if (offsetState.getKey() > lastOffset) {
                                // No further offsets to process.
                                break;
                            }

                            if (offsetState.getValue().state != RecordState.ACQUIRED) {
                                log.debug("The offset is not in acquired state while release of acquisition lock on timeout, skipping, offset: {} batch: {}"
                                                + " for the share group: {}-{}-{}", offsetState.getKey(), inFlightBatch,
                                        groupId, memberId, topicIdPartition);
                                continue;
                            }
                            InFlightState updateResult = offsetState.getValue().tryUpdateState(RecordState.AVAILABLE, false);
                            if (updateResult == null) {
                                log.debug("Unable to release acquisition lock on timeout for the offset: {} in batch: {}"
                                                + " for the share group: {}-{}-{}", offsetState.getKey(), inFlightBatch,
                                        groupId, memberId, topicIdPartition);
                                continue;
                            }
                            // Update acquisition lock timeout task for the offset to null since it is completed now.
                            updateResult.updateAcquisitionLockTimeoutTask(null);
                            localNextFetchOffset = Math.min(offsetState.getKey(), localNextFetchOffset);
                        }
                    }
                }
                nextFetchOffset = localNextFetchOffset;
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * The InFlightBatch maintains the in-memory state of the fetched records i.e. in-flight records.
     */
    class InFlightBatch {

        // The member id of the client that is fetching the record.
        private final String memberId;
        // The offset of the first record in the batch that is fetched from the log.
        private final long baseOffset;
        // The last offset of the batch that is fetched from the log.
        private final long lastOffset;

        // The in-flight state of the fetched records. If the offset state map is empty then inflightState
        // determines the state of the complete batch else individual offset determines the state of
        // the respective records.
        private InFlightState inFlightState;
        // Must be null for most of the records hence lazily-initialize. Should hold the gap offsets for
        // the batch that are reported by the client.
        private Set<Long> gapOffsets;
        // The subset of offsets within the batch that holds different record states within the parent
        // fetched batch. This is used to maintain the state of the records per offset, it is complex
        // to maintain the subset batch (InFlightBatch) within the parent batch itself as the nesting
        // can be deep and the state of the records can be different per offset, not always though.
        private NavigableMap<Long, InFlightState> offsetState;

        InFlightBatch(String memberId, long baseOffset, long lastOffset, RecordState state, int deliveryCount, TimerTask acquisitionLockTimeoutTask) {
            this.memberId = memberId;
            this.baseOffset = baseOffset;
            this.lastOffset = lastOffset;
            this.inFlightState = new InFlightState(state, deliveryCount, acquisitionLockTimeoutTask);
        }

        // Visible for testing.
        String memberId() {
            return memberId;
        }

        // Visible for testing.
        long baseOffset() {
            return baseOffset;
        }

        // Visible for testing.
        long lastOffset() {
            return lastOffset;
        }

        // Visible for testing.
        RecordState batchState() {
            if (inFlightState == null) {
                  throw new IllegalStateException("The batch state is not available as the offset state is maintained");
            }
            return inFlightState.state;
        }

        // Visible for testing.
        int batchDeliveryCount() {
            if (inFlightState == null) {
                throw new IllegalStateException("The batch delivery count is not available as the offset state is maintained");
            }
            return inFlightState.deliveryCount;
        }

        // Visible for testing.
        Set<Long> gapOffsets() {
            return gapOffsets;
        }

        // Visible for testing.
        NavigableMap<Long, InFlightState> offsetState() {
            return offsetState;
        }

        private InFlightState tryUpdateBatchState(RecordState newState, boolean incrementDeliveryCount) {
            if (inFlightState == null) {
                throw new IllegalStateException("The batch state update is not available as the offset state is maintained");
            }
            return inFlightState.tryUpdateState(newState, incrementDeliveryCount);
        }

        private InFlightState startBatchStateTransition(RecordState newState, boolean incrementDeliveryCount) {
            if (inFlightState == null) {
                throw new IllegalStateException("The batch state update is not available as the offset state is maintained");
            }
            return inFlightState.startStateTransition(newState, incrementDeliveryCount);
        }

        private void addGapOffsets(Long gapOffset) {
            if (this.gapOffsets == null) {
                this.gapOffsets = new HashSet<>();
            }
            this.gapOffsets.add(gapOffset);
        }

        private void addOffsetState(long baseOffset, long lastOffset, RecordState state, boolean incrementDeliveryCount) {
            maybeInitializeOffsetStateUpdate();
            // The offset state map is already initialized hence update the state of the offsets.
            for (long offset = baseOffset; offset <= lastOffset; offset++) {
                offsetState.get(offset).tryUpdateState(state, incrementDeliveryCount);
            }
        }

        private void addOffsetState(Map<Long, RecordState> stateMap, boolean incrementDeliveryCount) {
            maybeInitializeOffsetStateUpdate();
            // The offset state map is already initialized hence update the state of the offsets.
            stateMap.forEach((offset, state) -> offsetState.get(offset).tryUpdateState(state, incrementDeliveryCount));
        }

        private void maybeInitializeOffsetStateUpdate() {
            if (offsetState == null) {
                offsetState = new ConcurrentSkipListMap<>();
                // The offset state map is not initialized hence initialize the state of the offsets
                // from the base offset to the last offset. Mark the batch inflightState to null as
                // the state of the records is maintained in the offset state map now.
                for (long offset = this.baseOffset; offset <= this.lastOffset; offset++) {
                    if (gapOffsets != null && gapOffsets.contains(offset)) {
                        // Directly move the record to archived if gap offset is already known.
                        offsetState.put(offset, new InFlightState(RecordState.ARCHIVED, 0));
                        continue;
                    }
                    if (inFlightState.acquisitionLockTimeoutTask != null) {
                        // The acquisition lock timeout task is already scheduled for the batch, hence we need to schedule
                        // the acquisition lock timeout task for the offset as well.
                        long delayMs = ((AcquisitionLockTimerTask) inFlightState.acquisitionLockTimeoutTask).expirationMs() - time.hiResClockMs();
                        TimerTask timerTask = scheduleAcquisitionLockTimeout(memberId, offset, offset, delayMs);
                        offsetState.put(offset, new InFlightState(inFlightState.state, inFlightState.deliveryCount, timerTask));
                        timer.add(timerTask);
                    } else {
                        offsetState.put(offset, new InFlightState(inFlightState.state, inFlightState.deliveryCount));
                    }
                }
                // Cancel the acquisition lock timeout task for the batch as the offset state is maintained.
                if (inFlightState.acquisitionLockTimeoutTask != null) {
                    inFlightState.cancelAndClearAcquisitionLockTimeoutTask();
                }
                inFlightState = null;
            }
        }

        // Visible for testing.
        TimerTask acquisitionLockTimeoutTask() {
            if (inFlightState == null) {
                throw new IllegalStateException("The batch state is not available as the offset state is maintained");
            }
            return inFlightState.acquisitionLockTimeoutTask;
        }

        private void updateAcquisitionLockTimeout(TimerTask acquisitionLockTimeoutTask) {
            if (inFlightState == null) {
                throw new IllegalStateException("The batch state is not available as the offset state is maintained");
            }
            inFlightState.acquisitionLockTimeoutTask = acquisitionLockTimeoutTask;
        }

        @Override
        public String toString() {
            return "InFlightBatch(" +
                " memberId=" + memberId +
                ", baseOffset=" + baseOffset +
                ", lastOffset=" + lastOffset +
                ", inFlightState=" + inFlightState +
                ", gapOffsets=" + ((gapOffsets == null) ? "" : gapOffsets) +
                ", offsetState=" + ((offsetState == null) ? "" : offsetState) +
                ")";
        }
    }

    /**
     * The InFlightState is used to track the state and delivery count of a record that has been
     * fetched from the leader. The state of the record is used to determine if the record should
     * be re-deliver or if it can be acknowledged or archived.
     */
    static class InFlightState {

        // The state of the fetch batch records.
        private RecordState state;
        // The number of times the records has been delivered to the client.
        private int deliveryCount;
        // The state of the records before the transition.
        private InFlightState rollbackState;
        // The timer task for the acquisition lock timeout.
        private TimerTask acquisitionLockTimeoutTask;

        InFlightState(RecordState state, int deliveryCount) {
            this(state, deliveryCount, null);
        }

        InFlightState(RecordState state, int deliveryCount, TimerTask acquisitionLockTimeoutTask) {
          this.state = state;
          this.deliveryCount = deliveryCount;
          this.acquisitionLockTimeoutTask = acquisitionLockTimeoutTask;
        }

        // Visible for testing.
        RecordState state() {
            return state;
        }

        // Visible for testing.
        int deliveryCount() {
            return deliveryCount;
        }

        // Visible for testing.
        TimerTask acquisitionLockTimeoutTask() {
            return acquisitionLockTimeoutTask;
        }

        void updateAcquisitionLockTimeoutTask(TimerTask acquisitionLockTimeoutTask) {
            this.acquisitionLockTimeoutTask = acquisitionLockTimeoutTask;
        }

        void cancelAndClearAcquisitionLockTimeoutTask() {
            acquisitionLockTimeoutTask.cancel();
            acquisitionLockTimeoutTask = null;
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
        private InFlightState tryUpdateState(RecordState newState, boolean incrementDeliveryCount) {
            try {
                state = state.validateTransition(newState);
                if (incrementDeliveryCount) {
                    deliveryCount++;
                }
                return this;
            } catch (IllegalStateException e) {
                log.info("Failed to update state of the records", e);
                return null;
            }
        }

        private InFlightState startStateTransition(RecordState newState, boolean incrementDeliveryCount) {
            try {
                rollbackState = new InFlightState(state, deliveryCount, acquisitionLockTimeoutTask);
                state = state.validateTransition(newState);
                if (incrementDeliveryCount) {
                    deliveryCount++;
                }
                return this;
            } catch (IllegalStateException e) {
                log.info("Failed to start state transition of the records", e);
                return null;
            }
        }

        private void completeStateTransition(boolean commit) {
            if (commit) {
                rollbackState = null;
                return;
            }
            state = rollbackState.state;
            deliveryCount = rollbackState.deliveryCount;
            rollbackState = null;
        }

        @Override
        public int hashCode() {
            return Objects.hash(state, deliveryCount);
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
            return state == that.state && deliveryCount == that.deliveryCount;
        }

        @Override
        public String toString() {
            return "InFlightState(" +
                " state=" + state.toString() +
                ", deliveryCount=" + ((deliveryCount == 0) ? "" : ("(" + deliveryCount + ")")) +
                ")";
        }
    }

    /**
     * The AcknowledgementBatch containing the fields required to acknowledge the fetched records.
     */
    public static class AcknowledgementBatch {

        private final long baseOffset;
        private final long lastOffset;
        private final List<Long> gapOffsets;
        private final AcknowledgeType acknowledgeType;

        public AcknowledgementBatch(long baseOffset, long lastOffset, List<Long> gapOffsets, AcknowledgeType acknowledgeType) {
            this.baseOffset = baseOffset;
            this.lastOffset = lastOffset;
            this.gapOffsets = gapOffsets;
            this.acknowledgeType = Objects.requireNonNull(acknowledgeType);
        }

        public long baseOffset() {
            return baseOffset;
        }

        public long lastOffset() {
            return lastOffset;
        }

        public List<Long> gapOffsets() {
            return gapOffsets;
        }

        public AcknowledgeType acknowledgeType() {
            return acknowledgeType;
        }

        @Override
        public String toString() {
            return "AcknowledgementBatch(" +
                " baseOffset=" + baseOffset +
                ", lastOffset=" + lastOffset +
                ", gapOffsets=" + ((gapOffsets == null) ? "" : gapOffsets) +
                ", acknowledgeType=" + acknowledgeType +
                ")";
        }
    }
}
