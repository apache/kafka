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
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ShareFetchResponseData.AcquiredRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.share.ShareAcknowledgementBatch;
import org.apache.kafka.server.util.timer.Timer;
import org.apache.kafka.server.util.timer.TimerTask;
import org.apache.kafka.storage.internals.log.FetchPartitionData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
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
     * The state epoch is used to track the version of the state of the share partition.
     */
    private int stateEpoch;

    /**
     * The replica manager is used to get the earliest offset of the share partition, so we can adjust the start offset.
     */
    private final ReplicaManager replicaManager;

    SharePartition(
        String groupId,
        TopicIdPartition topicIdPartition,
        int maxInFlightMessages,
        int maxDeliveryCount,
        int recordLockDurationMs,
        Timer timer,
        Time time,
        ReplicaManager replicaManager
    ) {
        this.groupId = groupId;
        this.topicIdPartition = topicIdPartition;
        this.maxInFlightMessages = maxInFlightMessages;
        this.maxDeliveryCount = maxDeliveryCount;
        this.cachedState = new ConcurrentSkipListMap<>();
        this.lock = new ReentrantReadWriteLock();
        this.findNextFetchOffset = new AtomicBoolean(false);
        this.fetchLock = new AtomicBoolean(false);
        this.recordLockDurationMs = recordLockDurationMs;
        this.timer = timer;
        this.time = time;
        this.replicaManager = replicaManager;
        // Initialize the partition.
        initialize();
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
     * @param memberId The member id of the client that is fetching the record.
     * @param fetchPartitionData The fetched records for the share partition.
     *
     * @return A future which is completed when the records are acquired.
     */
    public CompletableFuture<List<AcquiredRecords>> acquire(
        String memberId,
        FetchPartitionData fetchPartitionData
    ) {
        log.trace("Received acquire request for share partition: {}-{}", memberId, fetchPartitionData);
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
                return CompletableFuture.completedFuture(Collections.singletonList(
                    acquireNewBatchRecords(memberId, firstBatch.baseOffset(), lastBatch.lastOffset())));
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
                boolean fullMatch = checkForFullMatch(inFlightBatch, firstBatch.baseOffset(), lastBatch.lastOffset());

                if (!fullMatch || inFlightBatch.offsetState() != null) {
                    log.trace("Subset or offset tracked batch record found for share partition,"
                            + " batch: {} request offsets - first: {}, last: {} for the share"
                            + " group: {}-{}", inFlightBatch, firstBatch.baseOffset(),
                        lastBatch.lastOffset(), groupId, topicIdPartition);
                    if (inFlightBatch.offsetState() == null) {
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

                // The in-flight batch is a full match hence change the state of the complete batch.
                if (inFlightBatch.batchState() != RecordState.AVAILABLE) {
                    log.trace("The batch is not available to acquire in share group: {}-{}, skipping: {}",
                        groupId, topicIdPartition, inFlightBatch);
                    continue;
                }

                InFlightState updateResult = inFlightBatch.tryUpdateBatchState(RecordState.ACQUIRED, true, maxDeliveryCount, memberId);
                if (updateResult == null) {
                    log.info("Unable to acquire records for the batch: {} in share group: {}-{}",
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
            }

            // Some of the request offsets are not found in the fetched batches. Acquire the
            // missing records as well.
            if (subMap.lastEntry().getValue().lastOffset() < lastBatch.lastOffset()) {
                log.trace("There exists another batch which needs to be acquired as well");
                result.add(acquireNewBatchRecords(memberId, subMap.lastEntry().getValue().lastOffset() + 1,
                    lastBatch.lastOffset()));
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
    public CompletableFuture<Optional<Throwable>> acknowledge(
        String memberId,
        List<ShareAcknowledgementBatch> acknowledgementBatch
    ) {
        log.trace("Acknowledgement batch request for share partition: {}-{}", groupId, topicIdPartition);

        CompletableFuture<Optional<Throwable>> future = new CompletableFuture<>();
        future.completeExceptionally(new UnsupportedOperationException("Not implemented"));

        return future;
    }

    /**
     * Release the acquired records for the share partition. The next fetch offset is updated to the next offset
     * that should be fetched from the leader.
     *
     * @param memberId The member id of the client whose records shall be released.
     *
     * @return A future which is completed when the records are released.
     */
    public CompletableFuture<Optional<Throwable>> releaseAcquiredRecords(String memberId) {
        log.trace("Release acquired records request for share partition: {}-{}-{}", groupId, memberId, topicIdPartition);

        CompletableFuture<Optional<Throwable>> future = new CompletableFuture<>();
        future.completeExceptionally(new UnsupportedOperationException("Not implemented"));

        return future;
    }

    private void initialize() {
        // Initialize the partition.
        log.debug("Initializing share partition: {}-{}", groupId, topicIdPartition);

        // TODO: Provide implementation to initialize the share partition.
    }

    private AcquiredRecords acquireNewBatchRecords(
        String memberId,
        long firstOffset,
        long lastOffset
    ) {
        lock.writeLock().lock();
        try {
            // Schedule acquisition lock timeout for the batch.
            AcquisitionLockTimerTask timerTask = scheduleAcquisitionLockTimeout(memberId, firstOffset, lastOffset);
            // Add the new batch to the in-flight records along with the acquisition lock timeout task for the batch.
            cachedState.put(firstOffset, new InFlightBatch(
                memberId,
                firstOffset,
                lastOffset,
                RecordState.ACQUIRED,
                1,
                timerTask));
            // if the cachedState was empty before acquiring the new batches then startOffset needs to be updated
            if (cachedState.firstKey() == firstOffset)  {
                startOffset = firstOffset;
            }
            endOffset = lastOffset;
            return new AcquiredRecords()
                .setFirstOffset(firstOffset)
                .setLastOffset(lastOffset)
                .setDeliveryCount((short) 1);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void acquireSubsetBatchRecords(
        String memberId,
        long requestFirstOffset,
        long requestLastOffset,
        InFlightBatch inFlightBatch,
        List<AcquiredRecords> result
    ) {
        lock.writeLock().lock();
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

                if (offsetState.getValue().state != RecordState.AVAILABLE) {
                    log.trace("The offset is not available skipping, offset: {} batch: {}"
                            + " for the share group: {}-{}", offsetState.getKey(), inFlightBatch,
                        groupId, topicIdPartition);
                    continue;
                }

                InFlightState updateResult =  offsetState.getValue().tryUpdateState(RecordState.ACQUIRED, true, maxDeliveryCount,
                    memberId);
                if (updateResult == null) {
                    log.trace("Unable to acquire records for the offset: {} in batch: {}"
                            + " for the share group: {}-{}", offsetState.getKey(), inFlightBatch,
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
            }
        } finally {
            lock.writeLock().unlock();
        }
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

    private AcquisitionLockTimerTask scheduleAcquisitionLockTimeout(String memberId, long firstOffset, long lastOffset) {
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
        AcquisitionLockTimerTask acquistionLockTimerTask = acquisitionLockTimerTask(memberId, firstOffset, lastOffset, delayMs);
        timer.add(acquistionLockTimerTask);
        return acquistionLockTimerTask;
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
        // TODO: Implement the logic to release the acquisition lock on timeout.
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

    private final class AcquisitionLockTimerTask extends TimerTask {
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
            if (batchState == null) {
                throw new IllegalStateException("The batch state is not available as the offset state is maintained");
            }
            return batchState.state;
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
        NavigableMap<Long, InFlightState> offsetState() {
            return offsetState;
        }

        private InFlightState tryUpdateBatchState(RecordState newState, boolean incrementDeliveryCount, int maxDeliveryCount, String newMemberId) {
            if (batchState == null) {
                throw new IllegalStateException("The batch state update is not available as the offset state is maintained");
            }
            return batchState.tryUpdateState(newState, incrementDeliveryCount, maxDeliveryCount, newMemberId);
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
            if (batchState == null) {
                throw new IllegalStateException("The batch state is not available as the offset state is maintained");
            }
            batchState.acquisitionLockTimeoutTask = acquisitionLockTimeoutTask;
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

        void updateAcquisitionLockTimeoutTask(AcquisitionLockTimerTask acquisitionLockTimeoutTask) {
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

        @Override
        public String toString() {
            return "InFlightState(" +
                "state=" + state.toString() +
                ", deliveryCount=" + deliveryCount +
                ", memberId=" + memberId +
                ")";
        }
    }
}
