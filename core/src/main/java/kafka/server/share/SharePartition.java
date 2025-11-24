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

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ShareAcquireMode;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.CoordinatorNotAvailableException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.InvalidRecordStateException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ShareFetchResponseData.AcquiredRecords;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.group.GroupConfig;
import org.apache.kafka.coordinator.group.GroupConfigManager;
import org.apache.kafka.coordinator.group.ShareGroupAutoOffsetResetStrategy;
import org.apache.kafka.server.share.acknowledge.ShareAcknowledgementBatch;
import org.apache.kafka.server.share.fetch.AcquisitionLockTimeoutHandler;
import org.apache.kafka.server.share.fetch.AcquisitionLockTimerTask;
import org.apache.kafka.server.share.fetch.DelayedShareFetchGroupKey;
import org.apache.kafka.server.share.fetch.DelayedShareFetchKey;
import org.apache.kafka.server.share.fetch.DeliveryCountOps;
import org.apache.kafka.server.share.fetch.InFlightBatch;
import org.apache.kafka.server.share.fetch.InFlightState;
import org.apache.kafka.server.share.fetch.RecordState;
import org.apache.kafka.server.share.fetch.ShareAcquiredRecords;
import org.apache.kafka.server.share.metrics.SharePartitionMetrics;
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
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.server.util.timer.Timer;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static kafka.server.share.ShareFetchUtils.offsetForEarliestTimestamp;
import static kafka.server.share.ShareFetchUtils.offsetForLatestTimestamp;
import static kafka.server.share.ShareFetchUtils.offsetForTimestamp;
import static kafka.server.share.ShareFetchUtils.recordLockDurationMsOrDefault;

/**
 * The SharePartition is used to track the state of a partition that is shared between multiple
 * consumers. The class maintains the state of the records that have been fetched from the leader
 * and are in-flight.
 */
@SuppressWarnings({"ClassDataAbstractionCoupling", "ClassFanOutComplexity"})
public class SharePartition {

    private static final Logger log = LoggerFactory.getLogger(SharePartition.class);

    /**
     * Minimum number of records to deliver when throttling
     */
    private static final int MINIMUM_THROTTLE_RECORDS_DELIVERY_LIMIT = 2;

    /**
     * empty member id used to indicate when a record is not acquired by any member.
     */
    static final String EMPTY_MEMBER_ID = "";

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
     * To provide static mapping between acknowledgement type bytes to RecordState.
     */
    private static final Map<Byte, RecordState> ACK_TYPE_TO_RECORD_STATE = Map.of(
        (byte) 0, RecordState.ARCHIVED,                             // Represents gap
        AcknowledgeType.ACCEPT.id, RecordState.ACKNOWLEDGED,
        AcknowledgeType.RELEASE.id, RecordState.AVAILABLE,
        AcknowledgeType.REJECT.id, RecordState.ARCHIVED
    );

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
     * The lock to ensure that the same share partition does not enter a fetch queue
     * while another one is being fetched within the queue. The caller's id that acquires the fetch
     * lock is utilized for ensuring the above.
     */
    private final AtomicReference<Uuid> fetchLock;

    /**
     * The max in-flight records is used to limit the number of records that can be in-flight at any
     * given time. The max in-flight records is used to prevent the consumer from fetching too many
     * records from the leader and running out of memory.
     */
    private final int maxInFlightRecords;

    /**
     * The max delivery count is used to limit the number of times a record can be delivered to the
     * consumer. The max delivery count is used to prevent the consumer re-delivering the same record
     * indefinitely.
     */
    private final int maxDeliveryCount;

    /**
     * Records whose delivery count exceeds this are deemed abnormal and the batching of these records
     * should be reduced. The limit is set to half of maxDeliveryCount rounded up, with a minimum of 2.
     */
    private final int throttleRecordsDeliveryLimit;
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
     * The find next fetch offset is used to indicate if the next fetch offset should be recomputed.
     */
    private boolean findNextFetchOffset;

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
     * The load start time is used to track the time taken to load the share partition.
     */
    private final long loadStartTimeMs;

    /**
     * The share partition metrics is used to track the broker-side metrics for the share partition.
     */
    private final SharePartitionMetrics sharePartitionMetrics;

    /**
     * The acquisition lock timeout handler is used to handle the acquisition lock timeout for the share partition.
     */
    private final AcquisitionLockTimeoutHandler timeoutHandler;

    /**
     * The replica manager is used to check to see if any delayed share fetch request can be completed because of data
     * availability due to acquisition lock timeout.
     */
    private final ReplicaManager replicaManager;

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
     * The persister read result gap window tracks if there are any gaps in the in-flight batch during
     * initial read of the share partition state from the persister.
     */
    private GapWindow persisterReadResultGapWindow;

    /**
     * We maintain the latest fetch offset and its metadata to estimate the minBytes requirement more efficiently.
     */
    private final OffsetMetadata fetchOffsetMetadata;

    /**
     * The delayed share fetch key is used to track the delayed share fetch requests that are waiting
     * for the respective share partition.
     */
    private final DelayedShareFetchKey delayedShareFetchKey;

    /**
     * The deliveryCompleteCount tracks the number of terminal (ACKNOWLEDGED / ARCHIVED) records within the
     * cachedState. This is used in the calculations for determining the current Share Partition lag.
     */
    private final AtomicInteger deliveryCompleteCount;

    /**
     * The state epoch is used to track the version of the state of the share partition.
     */
    private int stateEpoch;

    /**
     * The partition state is used to track the state of the share partition.
     */
    private SharePartitionState partitionState;

    /**
     * The fetch lock acquired time is used to track the time when the lock for share partition is acquired.
     */
    private long fetchLockAcquiredTimeMs;

    /**
     * The fetch lock released time is used to track the time when the lock for share partition is released.
     */
    private long fetchLockReleasedTimeMs;

    /**
     * The fetch lock idle duration is used to track the time for which the fetch lock is idle.
     */
    private long fetchLockIdleDurationMs;

    SharePartition(
        String groupId,
        TopicIdPartition topicIdPartition,
        int leaderEpoch,
        int maxInFlightRecords,
        int maxDeliveryCount,
        int defaultRecordLockDurationMs,
        Timer timer,
        Time time,
        Persister persister,
        ReplicaManager replicaManager,
        GroupConfigManager groupConfigManager,
        SharePartitionListener listener
    ) {
        this(groupId, topicIdPartition, leaderEpoch, maxInFlightRecords, maxDeliveryCount, defaultRecordLockDurationMs,
            timer, time, persister, replicaManager, groupConfigManager, SharePartitionState.EMPTY, listener,
            new SharePartitionMetrics(groupId, topicIdPartition.topic(), topicIdPartition.partition()));
    }

    // Visible for testing
    @SuppressWarnings("ParameterNumber")
    SharePartition(
        String groupId,
        TopicIdPartition topicIdPartition,
        int leaderEpoch,
        int maxInFlightRecords,
        int maxDeliveryCount,
        int defaultRecordLockDurationMs,
        Timer timer,
        Time time,
        Persister persister,
        ReplicaManager replicaManager,
        GroupConfigManager groupConfigManager,
        SharePartitionState sharePartitionState,
        SharePartitionListener listener,
        SharePartitionMetrics sharePartitionMetrics
    ) {
        this.groupId = groupId;
        this.topicIdPartition = topicIdPartition;
        this.leaderEpoch = leaderEpoch;
        this.maxInFlightRecords = maxInFlightRecords;
        this.maxDeliveryCount = maxDeliveryCount;
        this.throttleRecordsDeliveryLimit = Math.max(MINIMUM_THROTTLE_RECORDS_DELIVERY_LIMIT, (int) Math.ceil((double) maxDeliveryCount / 2));
        this.cachedState = new ConcurrentSkipListMap<>();
        this.lock = new ReentrantReadWriteLock();
        this.findNextFetchOffset = false;
        this.fetchLock = new AtomicReference<>(null);
        this.defaultRecordLockDurationMs = defaultRecordLockDurationMs;
        this.timer = timer;
        this.time = time;
        this.loadStartTimeMs = time.hiResClockMs();
        this.persister = persister;
        this.partitionState = sharePartitionState;
        this.replicaManager = replicaManager;
        this.groupConfigManager = groupConfigManager;
        this.fetchOffsetMetadata = new OffsetMetadata();
        this.delayedShareFetchKey = new DelayedShareFetchGroupKey(groupId, topicIdPartition);
        this.listener = listener;
        this.sharePartitionMetrics = sharePartitionMetrics;
        this.timeoutHandler = releaseAcquisitionLockOnTimeout();
        this.registerGaugeMetrics();
        this.deliveryCompleteCount = new AtomicInteger(0);
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
        log.trace("Maybe initialize share partition: {}-{}", groupId, topicIdPartition);
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
                .setTopicsData(List.of(new TopicData<>(topicIdPartition.topicId(),
                    List.of(PartitionFactory.newPartitionIdLeaderEpochData(topicIdPartition.partition(), leaderEpoch)))))
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
                    maybeLogError(String.format("Failed to initialize the share partition: %s-%s. Exception occurred: %s.",
                        groupId, topicIdPartition, partitionData), Errors.forCode(partitionData.errorCode()), ex);
                    throwable = ex;
                    return;
                }

                startOffset = startOffsetDuringInitialization(partitionData.startOffset());
                stateEpoch = partitionData.stateEpoch();

                List<PersisterStateBatch> stateBatches = partitionData.stateBatches();
                long gapStartOffset = -1;
                // The previousBatchLastOffset is used to track the last offset of the previous batch.
                // For the first batch that should ideally start from startOffset if there are no gaps,
                // we assume the previousBatchLastOffset to be startOffset - 1.
                long previousBatchLastOffset = startOffset - 1;
                for (PersisterStateBatch stateBatch : stateBatches) {
                    if (stateBatch.firstOffset() < startOffset) {
                        log.error("Invalid state batch found for the share partition: {}-{}. The base offset: {}"
                                + " is less than the start offset: {}.", groupId, topicIdPartition,
                            stateBatch.firstOffset(), startOffset);
                        throwable = new IllegalStateException(String.format("Failed to initialize the share partition %s-%s", groupId, topicIdPartition));
                        return;
                    }
                    if (gapStartOffset == -1 && stateBatch.firstOffset() > previousBatchLastOffset + 1) {
                        gapStartOffset = previousBatchLastOffset + 1;
                    }
                    previousBatchLastOffset = stateBatch.lastOffset();
                    InFlightBatch inFlightBatch = new InFlightBatch(timer, time, EMPTY_MEMBER_ID, stateBatch.firstOffset(),
                        stateBatch.lastOffset(), RecordState.forId(stateBatch.deliveryState()), stateBatch.deliveryCount(),
                        null, timeoutHandler, sharePartitionMetrics);
                    cachedState.put(stateBatch.firstOffset(), inFlightBatch);
                    // During initialization, deliveryCompleteCount is updated with the number of records that are in the
                    // ACKNOWLEDGED or ARCHIVED state.
                    if (isStateTerminal(RecordState.forId(stateBatch.deliveryState()))) {
                        deliveryCompleteCount.addAndGet((int) (stateBatch.lastOffset() - stateBatch.firstOffset() + 1));
                    }
                    sharePartitionMetrics.recordInFlightBatchMessageCount(stateBatch.lastOffset() - stateBatch.firstOffset() + 1);
                }
                // Update the endOffset of the partition.
                if (!cachedState.isEmpty()) {
                    // If the cachedState is not empty, findNextFetchOffset flag is set to true so that any AVAILABLE records
                    // in the cached state are not missed
                    updateFindNextFetchOffset(true);
                    endOffset = cachedState.lastEntry().getValue().lastOffset();
                    // gapWindow is not required, if there are no gaps in the read state response
                    if (gapStartOffset != -1) {
                        persisterReadResultGapWindow = new GapWindow(endOffset, gapStartOffset);
                    }
                    // In case the persister read state RPC result contains no AVAILABLE records, we can update cached state
                    // and start/end offsets.
                    maybeUpdateCachedStateAndOffsets();
                } else {
                    endOffset = startOffset;
                }
                // Set the partition state to Active and complete the future.
                partitionState = SharePartitionState.ACTIVE;
                log.debug("Initialized share partition: {}-{} with persister read state: {}, cached state: {}",
                    groupId, topicIdPartition, result.topicsData(), cachedState);
            } catch (Exception e) {
                throwable = e;
            } finally {
                boolean isFailed = throwable != null;
                if (isFailed) {
                    partitionState = SharePartitionState.FAILED;
                }
                // Release the lock.
                lock.writeLock().unlock();
                // Avoid triggering the listener for waiting share fetch requests in purgatory as the
                // share partition manager keeps track of same and will trigger the listener for the
                // respective share partition.
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
            if (!findNextFetchOffset) {
                if (cachedState.isEmpty() || startOffset > cachedState.lastEntry().getValue().lastOffset()) {
                    // 1. When cachedState is empty, endOffset is set to the next offset of the last
                    // offset removed from batch, which is the next offset to be fetched.
                    // 2. When startOffset has moved beyond the in-flight records, startOffset and
                    // endOffset point to the LSO, which is the next offset to be fetched.
                    log.trace("The next fetch offset for the share partition {}-{} is {}", groupId, topicIdPartition, endOffset);
                    return endOffset;
                } else {
                    log.trace("The next fetch offset for the share partition {}-{} is {}", groupId, topicIdPartition, endOffset + 1);
                    return endOffset + 1;
                }
            }

            // If this piece of code is reached, it means that findNextFetchOffset is true
            if (cachedState.isEmpty() || startOffset > cachedState.lastEntry().getValue().lastOffset()) {
                // If cachedState is empty, there is no need of re-computing next fetch offset in future fetch requests.
                // Same case when startOffset has moved beyond the in-flight records, startOffset and endOffset point to the LSO
                // and the cached state is fresh.
                updateFindNextFetchOffset(false);
                log.trace("The next fetch offset for the share partition {}-{} is {}", groupId, topicIdPartition, endOffset);
                return endOffset;
            }

            long nextFetchOffset = -1;
            long gapStartOffset = isPersisterReadGapWindowActive() ? persisterReadResultGapWindow.gapStartOffset() : -1;
            for (Map.Entry<Long, InFlightBatch> entry : cachedState.entrySet()) {
                // Check if there exists any gap in the in-flight batch which needs to be fetched. If
                // gapWindow's endOffset is equal to the share partition's endOffset, then
                // only the initial gaps should be considered. Once share partition's endOffset is past
                // initial read end offset then all gaps are anyway fetched.
                if (isPersisterReadGapWindowActive()) {
                    if (entry.getKey() > gapStartOffset) {
                        nextFetchOffset = gapStartOffset;
                        break;
                    }
                    // If the gapStartOffset is already past the last offset of the in-flight batch,
                    // then do not consider this batch for finding the next fetch offset. For example,
                    // consider during initialization, the gapWindow is set to 5 and the
                    // first cached batch is 15-18. First read will happen at offset 5 and say the data
                    // fetched is [5-6], now next fetch offset should be 7. This works fine but say
                    // subsequent read returns batch 8-11, and the gapStartOffset will be 12. Without
                    // the max check, the next fetch offset returned will be 7 which is incorrect.
                    // The natural gaps for which no data is available shall be considered hence
                    // take the max of the gapStartOffset and the last offset of the in-flight batch.
                    gapStartOffset = Math.max(entry.getValue().lastOffset() + 1, gapStartOffset);
                }

                // Check if the state is maintained per offset or batch. If the offsetState
                // is not maintained then the batch state is used to determine the offsets state.
                if (entry.getValue().offsetState() == null) {
                    if (entry.getValue().batchState() == RecordState.AVAILABLE && !entry.getValue().batchHasOngoingStateTransition()) {
                        nextFetchOffset = entry.getValue().firstOffset();
                        break;
                    }
                } else {
                    // The offset state is maintained hence find the next available offset.
                    for (Map.Entry<Long, InFlightState> offsetState : entry.getValue().offsetState().entrySet()) {
                        if (offsetState.getValue().state() == RecordState.AVAILABLE && !offsetState.getValue().hasOngoingStateTransition()) {
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
                updateFindNextFetchOffset(false);
                nextFetchOffset = endOffset + 1;
            }
            log.trace("The next fetch offset for the share partition {}-{} is {}", groupId, topicIdPartition, nextFetchOffset);
            return nextFetchOffset;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Acquire the fetched records for the share partition. The acquired records are added to the
     * in-flight records and the next fetch offset is updated to the next offset that should be
     * fetched from the leader.
     * <p>
     * The method always acquire the full batch records. The cache state can consist of multiple
     * full batches as a single batch. This behavior is driven by client configurations (batch size
     * and max fetch records) and allows for efficient client acknowledgements. However, partial batches
     * can exist in the cache only after a leader change and partial acknowledgements have been persisted
     * prior leader change. In such case, when a share partition loses track of a batch's start and
     * end offsets (e.g., after a leader change and partial acknowledgements), the cache stores the
     * batch based on the offset range provided by the persister. This method handles these special
     * batches by maintaining this range up to the last offset returned by the persister.  No special
     * handling is required afterward; the cache will eventually return to managing full batches.
     * <p>
     * For compacted topics, batches may be non-contiguous, and records within cached batches may contain gaps.
     * Because this method operates at the batch level, it acquires entire batches and relies on the
     * client to report any gaps in the data. Whether non-contiguous batches are acquired depends on
     * the first and last offsets of the fetched batches. Batches outside of this boundary will never
     * be acquired. For instance, if fetched batches cover offsets [0-9 and 20-29], and the configured
     * batch size and maximum fetch records are large enough (greater than 30 in this example), the
     * intervening batch [10-19] will be acquired. Since full fetched batch is acquired, the client is
     * responsible for reporting any data gaps. However, if the [0-9] and [20-29] ranges are fetched
     * in separate calls to this method, the [10-19] batch will not be acquired and cannot exist in
     * the cache.
     * <p>
     * However, for compacted topics, previously acquired batches (e.g., due to acquisition lock timeout
     * or explicit client release) might become available for acquisition again. But subsequent fetches
     * may reveal that these batches, or parts of them, have been removed by compaction. Because this
     * method works with whole batches, the disappearance of individual offsets within a batch requires
     * no special handling; the batch will be re-acquired, and the client will report the gaps. But if
     * an entire batch has been compacted away, this method must archive it in the cache to allow the
     * Share Partition Start Offset (SPSO) to progress. This is accomplished by comparing the fetchOffset
     * (the offset from which the log was read) with the first base offset of the fetch response. Any
     * batches from fetchOffset to first base offset of the fetch response are archived.
     *
     * @param memberId           The member id of the client that is fetching the record.
     * @param shareAcquireMode   The share acquire mode to acquire the records.
     * @param batchSize          The number of records per acquired records batch.
     * @param maxFetchRecords    The maximum number of records that should be acquired, this is a soft
     *                           limit and the method might acquire more records than the maxFetchRecords,
     *                           if the records are already part of the same fetch batch.
     * @param fetchOffset        The fetch offset for which the records are fetched.
     * @param fetchPartitionData The fetched records for the share partition.
     * @param isolationLevel     The isolation level for the share fetch request.
     * @return The acquired records for the share partition.
     */
    @SuppressWarnings({"cyclomaticcomplexity", "methodlength"}) // Consider refactoring to avoid suppression
    public ShareAcquiredRecords acquire(
        String memberId,
        ShareAcquireMode shareAcquireMode,
        int batchSize,
        int maxFetchRecords,
        long fetchOffset,
        FetchPartitionData fetchPartitionData,
        FetchIsolation isolationLevel
    ) {
        log.trace("Received acquire request for share partition: {}-{} memberId: {}", groupId, topicIdPartition, memberId);
        boolean isRecordLimitMode = isRecordLimitMode(shareAcquireMode);
        if (stateNotActive() || maxFetchRecords <= 0) {
            // Nothing to acquire.
            return ShareAcquiredRecords.empty();
        }

        RecordBatch lastBatch = fetchPartitionData.records.lastBatch().orElse(null);
        if (lastBatch == null) {
            // Nothing to acquire.
            return ShareAcquiredRecords.empty();
        }

        LastOffsetAndMaxRecords lastOffsetAndMaxRecords = lastOffsetAndMaxRecordsToAcquire(fetchOffset,
            maxFetchRecords, lastBatch.lastOffset());
        if (lastOffsetAndMaxRecords.maxRecords() <= 0) {
            return ShareAcquiredRecords.empty();
        }
        // The lastOffsetAndMaxRecords contains the last offset to acquire and the maximum number of records
        // to acquire.
        int maxRecordsToAcquire = lastOffsetAndMaxRecords.maxRecords();
        long lastOffsetToAcquire = lastOffsetAndMaxRecords.lastOffset();

        // We require the first batch of records to get the base offset. Stop parsing further
        // batches.
        RecordBatch firstBatch = fetchPartitionData.records.batches().iterator().next();
        lock.writeLock().lock();
        try {
            long baseOffset = firstBatch.baseOffset();

            // There might be cached batches which are stale due to topic compaction hence archive them.
            maybeArchiveStaleBatches(fetchOffset, baseOffset);

            // Find the floor batch record for the request batch. The request batch could be
            // for a subset of the in-flight batch i.e. cached batch of offset 10-14 and request batch
            // of 12-13. Hence, floor entry is fetched to find the sub-map. Secondly, when the share
            // partition is initialized with persisted state, the start offset might be moved to a later
            // offset. In such case, the first batch base offset might be less than the start offset.
            Map.Entry<Long, InFlightBatch> floorEntry = cachedState.floorEntry(baseOffset);
            if (floorEntry == null) {
                // The initialize method check that there couldn't be any batches prior to the start offset.
                // And once share partition starts fetching records, it will always fetch records, at least,
                // from the start offset, but there could be cases where the batch base offset is prior
                // to the start offset. This can happen when the share partition is initialized with
                // partial persisted state and moved start offset i.e. start offset is not the batch's
                // first offset. In such case, we need to adjust the base offset to the start offset.
                // It's safe to adjust the base offset to the start offset when there isn't any floor
                // i.e. no cached batches available prior to the request batch base offset. Hence,
                // check for the floor entry and adjust the base offset accordingly.
                if (baseOffset < startOffset) {
                    log.info("Adjusting base offset for the fetch as it's prior to start offset: {}-{}"
                            + "from {} to {}", groupId, topicIdPartition, baseOffset, startOffset);
                    baseOffset = startOffset;
                }
            } else if (floorEntry.getValue().lastOffset() >= baseOffset) {
                // We might find a batch with floor entry but not necessarily that batch has an overlap,
                // if the request batch base offset is ahead of last offset from floor entry i.e. cached
                // batch of 10-14 and request batch of 15-18, though floor entry is found but no overlap.
                // Such scenario will be handled in the next step when considering the subMap. However,
                // if the floor entry is found and the request batch base offset is within the floor entry
                // then adjust the base offset to the floor entry so that acquire method can still work on
                // previously cached batch boundaries.
                baseOffset = floorEntry.getKey();
            }
            // Validate if the fetch records are already part of existing batches and if available.
            NavigableMap<Long, InFlightBatch> subMap = cachedState.subMap(baseOffset, true, lastOffsetToAcquire, true);
            // No overlap with request offsets in the cache for in-flight records. Acquire the complete
            // batch.
            if (subMap.isEmpty()) {
                log.trace("No cached data exists for the share partition for requested fetch batch: {}-{}",
                    groupId, topicIdPartition);
                // It's safe to use lastOffsetToAcquire instead of lastBatch.lastOffset() because there is no
                // overlap hence the lastOffsetToAcquire is same as lastBatch.lastOffset() or before that.
                ShareAcquiredRecords shareAcquiredRecords = acquireNewBatchRecords(memberId, fetchPartitionData.records.batches(), isRecordLimitMode,
                    firstBatch.baseOffset(), lastOffsetToAcquire, batchSize, maxRecordsToAcquire);
                return maybeFilterAbortedTransactionalAcquiredRecords(fetchPartitionData, isolationLevel, shareAcquiredRecords);
            }

            log.trace("Overlap exists with in-flight records. Acquire the records if available for"
                + " the share partition: {}-{}", groupId, topicIdPartition);
            List<AcquiredRecords> result = new ArrayList<>();
            // The acquired count is used to track the number of records acquired for the request.
            int acquiredCount = 0;
            // This tracks whether there is a gap between the subMap entries. If a gap is found, we will acquire
            // the corresponding offsets in a separate batch.
            long maybeGapStartOffset = baseOffset;
            // The fetched records are already part of the in-flight records. The records might
            // be available for re-delivery hence try acquiring same. The request batches could
            // be an exact match, subset or span over multiple already fetched batches.
            for (Map.Entry<Long, InFlightBatch> entry : subMap.entrySet()) {
                // If the acquired count is equal to the max fetch records then break the loop.
                if (acquiredCount >= maxRecordsToAcquire) {
                    break;
                }

                InFlightBatch inFlightBatch = entry.getValue();
                // If the gapWindow window is active, we need to treat the gaps in between the window as
                // acquirable. Once the window is inactive (when we have acquired all the gaps inside the window),
                // the remaining gaps are natural (data does not exist at those offsets) and we need not acquire them.
                if (isPersisterReadGapWindowActive()) {
                    // If nextBatchStartOffset is less than the key of the entry, this means the fetch happened for a gap in the cachedState.
                    // Thus, a new batch needs to be acquired for the gap.
                    if (maybeGapStartOffset < entry.getKey()) {
                        // It's safe to use entry.getKey() - 1 as the last offset to acquire for the
                        // gap as the sub map should contain either the next batch or this line should
                        // not have been executed i.e. say there is a gap from 10-20 and cache contains
                        // [0-9, 21-30], when fetch returns single/multiple batches from 0-15, then
                        // first sub map entry has no gap and there exists only 1 entry in sub map.
                        // Hence, for next batch the following code will not be executed and records
                        // from 10-15 will be acquired later in the code. In other case, when
                        // fetch returns batches from 0-25, then the sub map will have 2 entries and
                        // gap will be computed correctly.
                        int numRecordsRemaining = maxRecordsToAcquire - acquiredCount;
                        ShareAcquiredRecords shareAcquiredRecords = acquireNewBatchRecords(memberId, fetchPartitionData.records.batches(), isRecordLimitMode,
                            maybeGapStartOffset, entry.getKey() - 1, batchSize, numRecordsRemaining);
                        result.addAll(shareAcquiredRecords.acquiredRecords());
                        acquiredCount += shareAcquiredRecords.count();
                    }
                    // Set nextBatchStartOffset as the last offset of the current in-flight batch + 1.
                    // Hence, after the loop iteration the next gap can be considered.
                    maybeGapStartOffset = inFlightBatch.lastOffset() + 1;
                    // If the acquired count is equal to the max fetch records then break the loop.
                    if (acquiredCount >= maxRecordsToAcquire) {
                        break;
                    }
                }

                // Compute if the batch is a full match.
                boolean fullMatch = checkForFullMatch(inFlightBatch, firstBatch.baseOffset(), lastOffsetToAcquire);
                int numRecordsRemaining = maxRecordsToAcquire - acquiredCount;
                boolean recordLimitSubsetMatch = isRecordLimitMode && checkForRecordLimitSubsetMatch(inFlightBatch, maxRecordsToAcquire, acquiredCount);
                boolean throttleRecordsDelivery = shouldThrottleRecordsDelivery(inFlightBatch, firstBatch.baseOffset(), lastOffsetToAcquire);
                // Stop acquiring more records if records delivery has to be throttled. The throttling prevents
                // complete batch to be archived in case of a single record being corrupt.
                // Below check isolates the current batch/offsets to be delivered individually in subsequent fetches.
                if (throttleRecordsDelivery && acquiredCount > 0) {
                    // Set the max records to acquire as 0 to prevent further acquisition of records.
                    maxRecordsToAcquire = 0;
                    break;
                }
                if (!fullMatch || inFlightBatch.offsetState() != null || recordLimitSubsetMatch || throttleRecordsDelivery) {
                    log.trace("Subset or offset tracked batch record found for share partition,"
                            + " batch: {} request offsets - first: {}, last: {} for the share"
                            + " partition: {}-{}", inFlightBatch, firstBatch.baseOffset(),
                        lastOffsetToAcquire, groupId, topicIdPartition);
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
                    // In record_limit mode, we need to ensure that we do not acquire more than
                    // maxRecordsToAcquire. Hence, pass the remaining number of records that can
                    // be acquired.
                    int acquiredSubsetCount = acquireSubsetBatchRecords(memberId, isRecordLimitMode, numRecordsRemaining, firstBatch.baseOffset(), lastOffsetToAcquire, inFlightBatch, result);

                    acquiredCount += acquiredSubsetCount;
                    // If records are throttled, return immediately and set `maxRecordsToAcquire = 0`
                    // to prevent acquiring any new records afterwards.
                    if (throttleRecordsDelivery && acquiredSubsetCount > 0) {
                        maxRecordsToAcquire = 0;
                        break;
                    }
                    continue;
                }

                // The in-flight batch is a full match hence change the state of the complete batch.
                if (inFlightBatch.batchState() != RecordState.AVAILABLE || inFlightBatch.batchHasOngoingStateTransition()) {
                    log.trace("The batch is not available to acquire in share partition: {}-{}, skipping: {}",
                        groupId, topicIdPartition, inFlightBatch);
                    continue;
                }

                InFlightState updateResult = inFlightBatch.tryUpdateBatchState(RecordState.ACQUIRED, DeliveryCountOps.INCREASE, maxDeliveryCount, memberId);
                if (updateResult == null || updateResult.state() != RecordState.ACQUIRED) {
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
            if (acquiredCount < maxRecordsToAcquire && subMap.lastEntry().getValue().lastOffset() < lastOffsetToAcquire) {
                log.trace("There exists another batch which needs to be acquired as well");
                int numRecordsRemaining = maxRecordsToAcquire - acquiredCount;
                ShareAcquiredRecords shareAcquiredRecords = acquireNewBatchRecords(memberId, fetchPartitionData.records.batches(), isRecordLimitMode,
                    subMap.lastEntry().getValue().lastOffset() + 1,
                    lastOffsetToAcquire, batchSize, numRecordsRemaining);
                result.addAll(shareAcquiredRecords.acquiredRecords());
                acquiredCount += shareAcquiredRecords.count();
            }
            if (!result.isEmpty()) {
                maybeUpdatePersisterGapWindowStartOffset(result.get(result.size() - 1).lastOffset() + 1);
                return maybeFilterAbortedTransactionalAcquiredRecords(fetchPartitionData, isolationLevel, new ShareAcquiredRecords(result, acquiredCount));
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
        List<PersisterBatch> persisterBatches = new ArrayList<>();
        lock.writeLock().lock();
        try {
            // Avoided using enhanced for loop as need to check if the last batch have offsets
            // in the range.
            for (ShareAcknowledgementBatch batch : acknowledgementBatches) {
                // Client can either send a single entry in acknowledgeTypes which represents the state
                // of the complete batch or can send individual offsets state.
                Map<Long, Byte> ackTypeMap;
                try {
                    ackTypeMap = fetchAckTypeMapForBatch(batch);
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
                    ackTypeMap,
                    subMap,
                    persisterBatches
                );

                if (ackThrowable.isPresent()) {
                    throwable = ackThrowable.get();
                    break;
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
        // If the acknowledgement is successful then persist state, complete the state transition
        // and update the cached state for start offset. Else rollback the state transition.
        rollbackOrProcessStateUpdates(future, throwable, persisterBatches);
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
        List<PersisterBatch> persisterBatches = new ArrayList<>();

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
                    Optional<Throwable> releaseAcquiredRecordsThrowable = releaseAcquiredRecordsForPerOffsetBatch(memberId, inFlightBatch, recordState, persisterBatches);
                    if (releaseAcquiredRecordsThrowable.isPresent()) {
                        throwable = releaseAcquiredRecordsThrowable.get();
                        break;
                    }
                    continue;
                }
                Optional<Throwable> releaseAcquiredRecordsThrowable = releaseAcquiredRecordsForCompleteBatch(memberId, inFlightBatch, recordState, persisterBatches);
                if (releaseAcquiredRecordsThrowable.isPresent()) {
                    throwable = releaseAcquiredRecordsThrowable.get();
                    break;
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
        // If the release acquired records is successful then persist state, complete the state transition
        // and update the cached state for start offset. Else rollback the state transition.
        rollbackOrProcessStateUpdates(future, throwable, persisterBatches);
        return future;
    }

    long loadStartTimeMs() {
        return loadStartTimeMs;
    }

    private Optional<Throwable> releaseAcquiredRecordsForPerOffsetBatch(String memberId,
                                                                        InFlightBatch inFlightBatch,
                                                                        RecordState recordState,
                                                                        List<PersisterBatch> persisterBatches) {

        log.trace("Offset tracked batch record found, batch: {} for the share partition: {}-{}", inFlightBatch,
                groupId, topicIdPartition);
        for (Map.Entry<Long, InFlightState> offsetState : inFlightBatch.offsetState().entrySet()) {

            // Check if member id is the owner of the offset.
            if (!offsetState.getValue().memberId().equals(memberId) && !offsetState.getValue().memberId().equals(EMPTY_MEMBER_ID)) {
                log.debug("Member {} is not the owner of offset: {} in batch: {} for the share"
                        + " partition: {}-{}. Skipping offset.", memberId, offsetState.getKey(), inFlightBatch, groupId, topicIdPartition);
                return Optional.empty();
            }
            if (offsetState.getValue().state() == RecordState.ACQUIRED) {
                // These records were fetched but they were not actually delivered to the client.
                InFlightState updateResult = offsetState.getValue().startStateTransition(
                        offsetState.getKey() < startOffset ? RecordState.ARCHIVED : recordState,
                        DeliveryCountOps.DECREASE,
                        this.maxDeliveryCount,
                        EMPTY_MEMBER_ID
                );
                if (updateResult == null) {
                    log.debug("Unable to release records from acquired state for the offset: {} in batch: {}"
                                    + " for the share partition: {}-{}", offsetState.getKey(),
                            inFlightBatch, groupId, topicIdPartition);
                    return Optional.of(new InvalidRecordStateException("Unable to release acquired records for the offset"));
                }

                // Successfully updated the state of the offset and created a persister state batch for write to persister.
                persisterBatches.add(new PersisterBatch(updateResult, new PersisterStateBatch(offsetState.getKey(),
                    offsetState.getKey(), updateResult.state().id(), (short) updateResult.deliveryCount())));
                if (offsetState.getKey() >= startOffset && isStateTerminal(updateResult.state())) {
                    deliveryCompleteCount.incrementAndGet();
                }
                // Do not update the next fetch offset as the offset has not completed the transition yet.
            }
        }
        return Optional.empty();
    }

    private Optional<Throwable> releaseAcquiredRecordsForCompleteBatch(String memberId,
                                                                       InFlightBatch inFlightBatch,
                                                                       RecordState recordState,
                                                                       List<PersisterBatch> persisterBatches) {

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
                    DeliveryCountOps.DECREASE,
                    this.maxDeliveryCount,
                    EMPTY_MEMBER_ID
            );
            if (updateResult == null) {
                log.debug("Unable to release records from acquired state for the batch: {}"
                        + " for the share partition: {}-{}", inFlightBatch, groupId, topicIdPartition);
                return Optional.of(new InvalidRecordStateException("Unable to release acquired records for the batch"));
            }

            // Successfully updated the state of the batch and created a persister state batch for write to persister.
            persisterBatches.add(new PersisterBatch(updateResult, new PersisterStateBatch(inFlightBatch.firstOffset(),
                inFlightBatch.lastOffset(), updateResult.state().id(), (short) updateResult.deliveryCount())));
            if (isStateTerminal(updateResult.state())) {
                deliveryCompleteCount.addAndGet(numInFlightRecordsInBatch(inFlightBatch.firstOffset(), inFlightBatch.lastOffset()));
            }
            // Do not update the next fetch offset as the batch has not completed the transition yet.
        }
        return Optional.empty();
    }

    /**
     * Updates the cached state, start and end offsets of the share partition as per the new log
     * start offset. The method is called when the log start offset is moved for the share partition.
     * <p>
     * This method only archives the available records in the cached state that are before the new log
     * start offset. It does not persist the archived state batches to the persister, rather it
     * updates the cached state and offsets to reflect the new log start offset. The state in persister
     * will be updated lazily during the acknowledge/release records API calls or acquisition lock timeout.
     * <p>
     * The AVAILABLE state records can either have ongoing state transition or not. Hence, the archive
     * records method will update the state of the records to ARCHIVED and set the terminal state flag
     * hence if the transition is rolled back then the state will not be AVAILABLE again. However,
     * the ACQUIRED state records will not be archived as they are still in-flight and acknowledge
     * method also do not allow the state update for any offsets post the log start offset, hence those
     * records will only be archived once acquisition lock timeout occurs.
     *
     * @param logStartOffset The new log start offset.
     */
    void updateCacheAndOffsets(long logStartOffset) {
        log.debug("Updating cached states for share partition: {}-{} with new log start offset: {}",
            groupId, topicIdPartition, logStartOffset);
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
                deliveryCompleteCount.set(0);
                return;
            }

            // When LSO is moved forward, some Terminal records, that were previously in-flight, will now be removed.
            // We need to reduce isAnyOffsetArchived by the number of such Terminal records that are moved out of in-flight.
            // We cannot directly reduce it by new LSO - previous start offset, because there could be gap offsets as well.
            int numTerminalRecordsRemoved = countTerminalRecordsUpToNewLSO(logStartOffset);

            // Archive the available records in the cached state that are before the new log start offset.
            boolean anyRecordArchived = archiveAvailableRecordsOnLsoMovement(logStartOffset);
            // If we have transitioned the state of any batch/offset from AVAILABLE to ARCHIVED,
            // then there is a chance that the next fetch offset can change.
            if (anyRecordArchived) {
                updateFindNextFetchOffset(true);
            }

            // The new startOffset will be the log start offset.
            startOffset = logStartOffset;
            // After the start offset has moved, update the deliveryCompleteCount.
            deliveryCompleteCount.addAndGet((-1) * numTerminalRecordsRemoved);
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

    /**
     * The method archives the available records in the cached state that are between the fetch offset
     * and the base offset of the first fetched batch. This method is required to handle the compacted
     * topics where the already fetched batch which is marked re-available, might not result in subsequent
     * fetch response from log. Hence, the batches need to be archived to allow the SPSO and next fetch
     * offset to progress.
     *
     * @param fetchOffset The fetch offset.
     * @param baseOffset  The base offset of the first fetched batch.
     */
    private void maybeArchiveStaleBatches(long fetchOffset, long baseOffset) {
        lock.writeLock().lock();
        try {
            // If the fetch happens from within a batch then fetchOffset can be ahead of base offset else
            // should be same as baseOffset of the first fetched batch. Otherwise, we might need to archive
            // some stale batches.
            if (cachedState.isEmpty() || fetchOffset >= baseOffset) {
                // No stale batches to archive.
                return;
            }

            // The fetch offset can exist in the middle of the batch. Hence, find the floor offset
            // for the fetch offset and then find the sub-map from the floor offset to the base offset.
            long floorOffset = fetchOffset;
            Map.Entry<Long, InFlightBatch> floorEntry = cachedState.floorEntry(fetchOffset);
            if (floorEntry != null && floorEntry.getValue().lastOffset() >= fetchOffset) {
                floorOffset = floorEntry.getKey();
            }

            NavigableMap<Long, InFlightBatch> subMap = cachedState.subMap(floorOffset, true, baseOffset, false);
            if (subMap.isEmpty()) {
                // No stale batches to archive.
                return;
            }

            // Though such batches can be removed from the cache, but it is better to archive them so
            // that they are never acquired again.
            boolean anyRecordArchived = archiveRecords(fetchOffset, baseOffset, subMap, RecordState.AVAILABLE, true);

            // If we have transitioned the state of any batch/offset from AVAILABLE to ARCHIVED,
            // then there is a chance that the next fetch offset can change.
            if (anyRecordArchived) {
                updateFindNextFetchOffset(true);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * The method archives the available records in the cached state that are before the log start offset.
     *
     * @param logStartOffset The log start offset.
     * @return A boolean which indicates whether any record is archived or not.
     */
    private boolean archiveAvailableRecordsOnLsoMovement(long logStartOffset) {
        lock.writeLock().lock();
        try {
            return archiveRecords(startOffset, logStartOffset, cachedState, RecordState.AVAILABLE, false);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * The method calculates the number of Terminal records before the given offset. This method is called whenever LSO is
     * updated. Since that is a rare event, we can afford to do a linear scan of the cached state.
     *
     * @param newLSO The new LSO before which the Terminal record count is needed.
     * @return The count of Terminal records before the given offset.
     */
    private int countTerminalRecordsUpToNewLSO(long newLSO) {
        lock.readLock().lock();
        try {
            int count = 0;
            NavigableMap<Long, InFlightBatch> headMap = cachedState.headMap(newLSO, false);

            for (InFlightBatch inFlightBatch : headMap.values()) {
                // This batch is not in-flight, hence skip.
                if (inFlightBatch.lastOffset() < startOffset) {
                    continue;
                }

                if (inFlightBatch.offsetState() == null) {
                    if (isStateTerminal(inFlightBatch.batchState())) {
                        // If the code reaches this point, this means that the current batch is Terminal and some of its
                        // records lie beyond startOffset. Now we need to find how many of its records lie in-flight (
                        // between startOffset and endOffset).
                        long effectiveFirstOffset = Math.max(startOffset, inFlightBatch.firstOffset());
                        long effectiveLastOffset = Math.min(newLSO - 1, inFlightBatch.lastOffset());

                        if (effectiveFirstOffset <= effectiveLastOffset) {
                            count += (int) (effectiveLastOffset - effectiveFirstOffset + 1);
                        }
                    }
                } else {
                    // The current batch has offset tracking enabled. In this case only Terminal records that lie between
                    // startOffset and endOffset will be considered.
                    for (Map.Entry<Long, InFlightState> offsetEntry : inFlightBatch.offsetState().entrySet()) {
                        long offset = offsetEntry.getKey();
                        if (offset >= startOffset && offset < newLSO && isStateTerminal(offsetEntry.getValue().state())) {
                            count++;
                        }
                    }
                }
            }
            return count;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * The method archive the records in a given state in the map that are before the end offset.
     *
     * @param startOffset The offset from which the records should be archived.
     * @param endOffset The offset before which the records should be archived.
     * @param map The map containing the in-flight records.
     * @param initialState The initial state of the records to be archived.
     * @param updateDeliveryCompleteCount A boolean which indicates whether the deliveryCompleteCount should be updated.
     * @return A boolean which indicates whether any record is archived or not.
     */
    private boolean archiveRecords(
        long startOffset,
        long endOffset,
        NavigableMap<Long, InFlightBatch> map,
        RecordState initialState,
        boolean updateDeliveryCompleteCount) {
        lock.writeLock().lock();
        try {
            boolean isAnyOffsetArchived = false, isAnyBatchArchived = false;
            for (Map.Entry<Long, InFlightBatch> entry : map.entrySet()) {
                long batchStartOffset = entry.getKey();
                // We do not need to transition state of batches/offsets that are later than the offset.
                if (batchStartOffset >= endOffset) {
                    break;
                }
                InFlightBatch inFlightBatch = entry.getValue();
                boolean fullMatch = checkForFullMatch(inFlightBatch, startOffset, endOffset - 1);

                // Maintain state per offset if the inflight batch is not a full match or the offset state is managed.
                if (!fullMatch || inFlightBatch.offsetState() != null) {
                    log.debug("Subset or offset tracked batch record found while trying to update offsets "
                        + "and cached state map, batch: {}, offsets to update - first: {}, last: {} "
                        + "for the share partition: {}-{}", inFlightBatch, startOffset, endOffset - 1,
                        groupId, topicIdPartition);

                    if (inFlightBatch.offsetState() == null) {
                        if (inFlightBatch.batchState() != initialState) {
                            continue;
                        }
                        inFlightBatch.maybeInitializeOffsetStateUpdate();
                    }
                    isAnyOffsetArchived = archivePerOffsetBatchRecords(inFlightBatch, startOffset, endOffset - 1, initialState, updateDeliveryCompleteCount) || isAnyOffsetArchived;
                    continue;
                }
                // The in-flight batch is a full match hence change the state of the complete batch.
                isAnyBatchArchived = archiveCompleteBatch(inFlightBatch, initialState, updateDeliveryCompleteCount) || isAnyBatchArchived;
            }
            return isAnyOffsetArchived || isAnyBatchArchived;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private boolean archivePerOffsetBatchRecords(InFlightBatch inFlightBatch,
                                                 long startOffsetToArchive,
                                                 long endOffsetToArchive,
                                                 RecordState initialState,
                                                 boolean updateDeliveryCompleteCount
    ) {
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
                if (offsetState.getValue().state() != initialState) {
                    continue;
                }

                offsetState.getValue().archive();
                if (updateDeliveryCompleteCount) {
                    // If the record moves from a non-terminal state to a terminal state (in this case ARCHIVE), deliveryCompleteCount
                    // needs to be incremented.
                    deliveryCompleteCount.incrementAndGet();
                }
                isAnyOffsetArchived = true;
            }
            return isAnyOffsetArchived;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private boolean archiveCompleteBatch(
        InFlightBatch inFlightBatch,
        RecordState initialState,
        boolean updateDeliveryCompleteCount) {
        lock.writeLock().lock();
        try {
            log.trace("Archiving complete batch: {} for the share partition: {}-{}", inFlightBatch, groupId, topicIdPartition);
            if (inFlightBatch.batchState() == initialState) {
                // Change the state of complete batch since the same state exists for the entire inFlight batch.
                inFlightBatch.archiveBatch();
                if (updateDeliveryCompleteCount) {
                    // If the records move from a non-terminal state to a terminal state (in this case ARCHIVE), deliveryCompleteCount
                    // needs to be incremented by the number of records in the batch.
                    deliveryCompleteCount.addAndGet((int) (inFlightBatch.lastOffset() - inFlightBatch.firstOffset() + 1));
                }
                return true;
            }
        } finally {
            lock.writeLock().unlock();
        }
        return false;
    }

    /**
     * Checks if the records can be acquired for the share partition. The records can be acquired if
     * the number of records in-flight is less than the max in-flight records. Or if the fetch is
     * to happen somewhere in between the record states cached in the share partition i.e. re-acquire
     * the records that are already fetched before.
     *
     * @return A boolean which indicates whether more records can be acquired or not.
     */
    boolean canAcquireRecords() {
        if (nextFetchOffset() != endOffset() + 1) {
            return true;
        }
        return numInFlightRecords() < maxInFlightRecords;
    }

    /**
     * Prior to fetching records from the leader, the fetch lock is acquired to ensure that the same
     * share partition is not fetched concurrently by multiple clients. The fetch lock is released once
     * the records are fetched and acquired.
     *
     * @param fetchId - the caller's id that is trying to acquire the fetch lock.
     * @return A boolean which indicates whether the fetch lock is acquired.
     */
    public boolean maybeAcquireFetchLock(Uuid fetchId) {
        if (stateNotActive()) {
            return false;
        }
        boolean acquired = fetchLock.compareAndSet(null, Objects.requireNonNull(fetchId));
        if (acquired) {
            long currentTime = time.hiResClockMs();
            fetchLockAcquiredTimeMs = currentTime;
            fetchLockIdleDurationMs = fetchLockReleasedTimeMs != 0 ? currentTime - fetchLockReleasedTimeMs : 0;
        }
        return acquired;
    }

    /**
     * Release the fetch lock once the records are fetched from the leader. It is imperative that the caller
     * that acquired the fetch lock should be the one releasing it.
     * @param fetchId - The caller's id that is trying to release the fetch lock.
     */
    void releaseFetchLock(Uuid fetchId) {
        // Register the metric for the duration the fetch lock was held. Do not register the metric
        // if the fetch lock was not acquired.
        long currentTime = time.hiResClockMs();
        if (!fetchLock.compareAndSet(Objects.requireNonNull(fetchId), null)) {
            // This code should not be reached unless we are in error-prone scenarios. Since we are releasing the fetch
            // lock for multiple share partitions at different places in DelayedShareFetch (due to tackling remote
            // storage fetch and local log fetch from a single purgatory), in order to safeguard ourselves from bad code,
            // we are logging when an instance that does not hold the fetch lock tries to release it.
            Uuid fetchLockAcquiredBy = fetchLock.getAndSet(null);
            log.info("Instance {} does not hold the fetch lock, yet trying to release it for share partition {}-{}. The lock was held by {}",
                fetchId, groupId, topicIdPartition, fetchLockAcquiredBy);
        }
        long acquiredDurationMs = currentTime - fetchLockAcquiredTimeMs;
        // Update the metric for the fetch lock time.
        sharePartitionMetrics.recordFetchLockTimeMs(acquiredDurationMs);
        // Update fetch lock ratio metric.
        recordFetchLockRatioMetric(acquiredDurationMs);
        fetchLockReleasedTimeMs = currentTime;
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

    /**
     * Records the fetch lock ratio metric. The metric is the ratio of the time duration the fetch
     * lock was acquired to the total time since the last lock acquisition. The total time is calculated
     * by adding the duration of the fetch lock idle time to the time the fetch lock was acquired.
     *
     * @param acquiredDurationMs The time duration the fetch lock was acquired.
     */
    // Visible for testing
    void recordFetchLockRatioMetric(long acquiredDurationMs) {
        if (fetchLockIdleDurationMs < 0) {
            // This is just a safe check to avoid negative time for fetch lock idle duration. This
            // should not happen in any scenarios. If it does then just return from the method and
            // no metric update is an indicator of the issue.
            return;
        }

        // Update the total fetch lock acquired time.
        double fetchLockToTotalTime;
        if (acquiredDurationMs + fetchLockIdleDurationMs == 0) {
            // If the total time is 0 then the ratio is 1 i.e. the fetch lock was acquired for the complete time.
            fetchLockToTotalTime = 1.0;
        } else if (acquiredDurationMs == 0) {
            // If the acquired duration is 0 then the ratio is the calculated by the idle duration.
            fetchLockToTotalTime = 1.0 / fetchLockIdleDurationMs;
        } else {
            fetchLockToTotalTime = acquiredDurationMs * (1.0 / (acquiredDurationMs + fetchLockIdleDurationMs));
        }
        sharePartitionMetrics.recordFetchLockRatio((int) (fetchLockToTotalTime * 100));
    }

    private void registerGaugeMetrics() {
        sharePartitionMetrics.registerInFlightMessageCount(this::numInFlightRecords);
        sharePartitionMetrics.registerInFlightBatchCount(this.cachedState::size);
    }

    private int numInFlightRecords() {
        lock.readLock().lock();
        int numRecords;
        try {
            if (cachedState.isEmpty()) {
                numRecords = 0;
            } else {
                numRecords = (int) (this.endOffset - this.startOffset + 1);
            }
        } finally {
            lock.readLock().unlock();
        }
        return numRecords;
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
            case FENCED -> throw new LeaderNotAvailableException(
                String.format("Share partition is fenced %s-%s", groupId, topicIdPartition));
            case EMPTY ->
                // The share partition is not yet initialized.
                false;
        };
    }

    // Method to reduce the window that tracks gaps in the cachedState
    private void maybeUpdatePersisterGapWindowStartOffset(long offset) {
        lock.writeLock().lock();
        try {
            if (persisterReadResultGapWindow != null) {
                // When last cached batch for persister's read gap window is acquired, then endOffset is
                // same as the gapWindow's endOffset, but the gap offset to update in the method call
                // is endOffset + 1. Hence, do not update the gap start offset if the request offset
                // is ahead of the endOffset.
                if (persisterReadResultGapWindow.endOffset() == endOffset && offset <= persisterReadResultGapWindow.endOffset()) {
                    persisterReadResultGapWindow.gapStartOffset(offset);
                } else {
                    // The persister's read gap window is not valid anymore as the end offset has moved
                    // beyond the read gap window's endOffset. Hence, set the gap window to null.
                    persisterReadResultGapWindow = null;
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * The method calculates the last offset and maximum records to acquire. The adjustment is needed
     * to ensure that the records acquired do not exceed the maximum in-flight records limit.
     *
     * @param fetchOffset The offset from which the records are fetched.
     * @param maxFetchRecords The maximum number of records to acquire.
     * @param lastOffset The last offset to acquire records to, which is the last offset of the fetched batch.
     * @return LastOffsetAndMaxRecords object, containing the last offset to acquire and the maximum records to acquire.
     */
    private LastOffsetAndMaxRecords lastOffsetAndMaxRecordsToAcquire(long fetchOffset, int maxFetchRecords, long lastOffset) {
        // There can always be records fetched exceeding the max in-flight records limit. Hence,
        // we need to check if the share partition has reached the max in-flight records limit
        // and only acquire limited records.
        int maxRecordsToAcquire;
        long lastOffsetToAcquire = lastOffset;
        lock.readLock().lock();
        try {
            int inFlightRecordsCount = numInFlightRecords();
            // Take minimum of maxFetchRecords and remaining capacity to fill max in-flight records limit.
            maxRecordsToAcquire = Math.min(maxFetchRecords, maxInFlightRecords - inFlightRecordsCount);
            // If the maxRecordsToAcquire is less than or equal to 0, then ideally (check exists to not
            // fetch records for share partitions which are at capacity) the fetch must be happening
            // in-between the in-flight batches i.e. some in-flight records have been released (marked
            // re-available). In such case, last offset to acquire should be adjusted to the endOffset
            // of the share partition, if not adjusted then the records can be acquired post the endOffset.
            // For example, if 30 records are already acquired i.e. [0-29] and single offset 20 is released
            // then the next fetch request will be at 20. Difference from endOffset will be 10, which
            // means that some offset past the endOffset can be acquired (21-29 are already acquired).
            // Hence, the lastOffsetToAcquire should be adjusted to the endOffset.
            if (maxRecordsToAcquire <= 0) {
                if (fetchOffset <= endOffset()) {
                    // Adjust the max records to acquire to the capacity available to fill the max
                    // in-flight records limit. This can happen when the fetch is happening in-between
                    // the in-flight batches and the share partition has reached the max in-flight records limit.
                    maxRecordsToAcquire = Math.min(maxFetchRecords, (int) (endOffset() - fetchOffset + 1));
                    // Adjust the last offset to acquire to the minimum of fetched data's last or
                    // endOffset of the share partition. This is required as partition fetch bytes
                    // are dynamic and subsequent fetches can fetch lesser data i.e. lastOffset can
                    // be lesser than endOffset.
                    lastOffsetToAcquire = Math.min(lastOffset, endOffset());
                    log.debug("Share partition {}-{} is at max in-flight records limit: {}. "
                            + "However, fetch is happening in-between the in-flight batches, hence adjusting "
                            + "last offset to: {} and max records to: {}", groupId, topicIdPartition,
                        maxInFlightRecords, lastOffsetToAcquire, maxRecordsToAcquire);
                } else {
                    // The share partition is already at max in-flight records, hence cannot acquire more records.
                    log.debug("Share partition {}-{} has reached max in-flight records limit: {}. Cannot acquire more records, inflight records count: {}",
                        groupId, topicIdPartition, maxInFlightRecords, inFlightRecordsCount);
                }
            }
        } finally {
            lock.readLock().unlock();
        }
        return new LastOffsetAndMaxRecords(lastOffsetToAcquire, maxRecordsToAcquire);
    }

    private ShareAcquiredRecords acquireNewBatchRecords(
        String memberId,
        Iterable<? extends RecordBatch> batches,
        boolean isRecordLimitMode,
        long firstOffset,
        long lastOffset,
        int batchSize,
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

            // Check how many records can be acquired from the batch.
            long lastAcquiredOffset = lastOffset;
            if (maxFetchRecords < lastAcquiredOffset - firstAcquiredOffset + 1) {
                // The max records to acquire is less than the complete available batches, so we
                // limit the acquired records. Although shareAcquireMode is introduced, even in
                // record_limit mode, the last offset should still be the batch's last offset
                // that falls under the max records limit, because the InFlightBatch
                // in cachedState should still reflect the complete batch.
                lastAcquiredOffset = lastOffsetFromBatchWithRequestOffset(batches, firstAcquiredOffset + maxFetchRecords - 1);
                // If the initial read gap offset window is active then it's not guaranteed that the
                // batches align on batch boundaries. Hence, reset to last offset itself if the batch's
                // last offset is greater than the last offset for acquisition, else there could be
                // a situation where the batch overlaps with the initial read gap offset window batch.
                // For example, if the initial read gap offset window is 10-30 i.e. gapWindow's
                // startOffset is 10 and endOffset is 30, and the first persister's read batch is 15-30.
                // Say first fetched batch from log is 10-30 and maxFetchRecords is 1, then the lastOffset
                // in this method call would be 14. As the maxFetchRecords is lesser than the batch,
                // hence last batch offset for request offset is fetched. In this example it will
                // be 30, hence check if the initial read gap offset window is active and the last acquired
                // offset should be adjusted to 14 instead of 30.
                if (isPersisterReadGapWindowActive() && lastAcquiredOffset > lastOffset) {
                    lastAcquiredOffset = lastOffset;
                }
            }

            // Create batches of acquired records.
            List<AcquiredRecords> acquiredRecords = createBatches(memberId, batches, isRecordLimitMode, maxFetchRecords, firstAcquiredOffset, lastAcquiredOffset, batchSize);
            // if the cachedState was empty before acquiring the new batches then startOffset needs to be updated
            if (cachedState.firstKey() == firstAcquiredOffset)  {
                startOffset = firstAcquiredOffset;
            }

            // If the new batch acquired is part of a gap in the cachedState, then endOffset should not be updated.
            // Ex. if startOffset is 10, endOffset is 30, there is a gap from 10 to 20, and an inFlight batch from 21 to 30.
            // In this case, the nextFetchOffset results in 10 and the records are fetched. A new batch is acquired from
            // 10 to 20, but the endOffset remains at 30.
            if (lastAcquiredOffset > endOffset) {
                endOffset = lastAcquiredOffset;
            }
            // Update lastAcquireOffset to the last offset of acquired records in record_limit mode
            // Since this is required to calculate the actual number of offsets acquired.
            if (isRecordLimitMode) {
                lastAcquiredOffset = acquiredRecords.get(0).lastOffset();
            }
            maybeUpdatePersisterGapWindowStartOffset(lastAcquiredOffset + 1);
            return new ShareAcquiredRecords(acquiredRecords, (int) (lastAcquiredOffset - firstAcquiredOffset + 1));
        } finally {
            lock.writeLock().unlock();
        }
    }

    private List<AcquiredRecords> createBatches(
        String memberId,
        Iterable<? extends RecordBatch> batches,
        boolean isRecordLimitMode,
        int maxFetchRecords,
        long firstAcquiredOffset,
        long lastAcquiredOffset,
        int batchSize
    ) {
        lock.writeLock().lock();
        try {
            List<AcquiredRecords> result = new ArrayList<>();
            long currentFirstOffset = firstAcquiredOffset;
            // Batch splitting only occurs in batch_optimized mode when the number of records to acquire exceeds the batch size.
            // If acquireMode is record_limit, or the batch size is greater than the number of records to acquire, no splitting is performed.
            long recordCount = lastAcquiredOffset - firstAcquiredOffset + 1;
            if (!isRecordLimitMode && recordCount > batchSize) {
                // The batch is split into multiple batches considering batch size.
                // Note: Try reading only the baseOffset of the batch and avoid reading the lastOffset
                // as lastOffset call of RecordBatch is expensive (loads headers).
                for (RecordBatch batch : batches) {
                    long batchBaseOffset = batch.baseOffset();
                    // Check if the batch is already past the last acquired offset then break.
                    if (batchBaseOffset > lastAcquiredOffset) {
                        // Break the loop and the last batch will be processed outside the loop.
                        break;
                    }

                    // Create new batch once the batch size is reached.
                    if (batchBaseOffset - currentFirstOffset >= batchSize) {
                        result.add(new AcquiredRecords()
                            .setFirstOffset(currentFirstOffset)
                            .setLastOffset(batchBaseOffset - 1)
                            .setDeliveryCount((short) 1));
                        currentFirstOffset = batchBaseOffset;
                    }
                }
            }
            // Add the last batch or the only batch if the batch size is greater than the records which
            // can be acquired.
            result.add(new AcquiredRecords()
                .setFirstOffset(currentFirstOffset)
                .setLastOffset(lastAcquiredOffset)
                .setDeliveryCount((short) 1));

            if (isRecordLimitMode) {
                // In record_limit mode, there will always be only one single batch in the result.
                AcquiredRecords acquiredRecords = result.get(0);
                // When the count of acquired records exceeds the max fetch limit, only initialize and schedule acquisition lock for
                // acquired records up to the max fetch boundary and remaining offsets should still in available state.
                // i.e. acquired records are 10-19 (10 records) and max fetch records is 5, then only 10-14 should be acquired
                // and offset 15-19 should still in available state.
                if (acquiredRecords.lastOffset() - acquiredRecords.firstOffset() + 1 > maxFetchRecords) {
                    InFlightBatch inFlightBatch = new InFlightBatch(
                        timer,
                        time,
                        memberId,
                        acquiredRecords.firstOffset(),
                        acquiredRecords.lastOffset(),
                        RecordState.ACQUIRED,
                        1,
                        null,
                        timeoutHandler,
                        sharePartitionMetrics);
                    int delayMs = recordLockDurationMsOrDefault(groupConfigManager, groupId, defaultRecordLockDurationMs);
                    long lastOffset = acquiredRecords.firstOffset() + maxFetchRecords - 1;
                    acquiredRecords.setLastOffset(lastOffset);
                    inFlightBatch.maybeInitializeOffsetStateUpdate(lastOffset, delayMs);
                    updateFindNextFetchOffset(true);

                    cachedState.put(acquiredRecords.firstOffset(), inFlightBatch);
                    sharePartitionMetrics.recordInFlightBatchMessageCount(
                        acquiredRecords.lastOffset() - acquiredRecords.firstOffset() + 1);
                    return List.of(acquiredRecords);
                }
            }
            addBatches(memberId, result);
            return result;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Add the acquired record batches to the cache and schedule acquisition lock timeouts.
     *
     * @param memberId The member id for which the records are acquired.
     * @param acquiredRecordsList The list of acquired records to add to cache state.
     */
    private void addBatches(String memberId, List<AcquiredRecords> acquiredRecordsList) {
        lock.writeLock().lock();
        try {
            acquiredRecordsList.forEach(acquiredRecords -> {
                AcquisitionLockTimerTask timerTask = scheduleAcquisitionLockTimeout(
                    memberId, acquiredRecords.firstOffset(), acquiredRecords.lastOffset());

                cachedState.put(acquiredRecords.firstOffset(), new InFlightBatch(
                    timer,
                    time,
                    memberId,
                    acquiredRecords.firstOffset(),
                    acquiredRecords.lastOffset(),
                    RecordState.ACQUIRED,
                    1,
                    timerTask,
                    timeoutHandler,
                    sharePartitionMetrics));
                // Update the in-flight batch message count metrics for the share partition.
                sharePartitionMetrics.recordInFlightBatchMessageCount(acquiredRecords.lastOffset() - acquiredRecords.firstOffset() + 1);
            });
        } finally {
            lock.writeLock().unlock();
        }
    }

    private boolean checkForRecordLimitSubsetMatch(InFlightBatch inFlightBatch, int maxRecordsToAcquire, int acquiredCount) {
        long numRecordsInBatch = inFlightBatch.lastOffset() - inFlightBatch.firstOffset() + 1;
        int numRecordsRemaining = maxRecordsToAcquire - acquiredCount;
        return numRecordsInBatch > numRecordsRemaining;
    }

    private int acquireSubsetBatchRecords(
        String memberId,
        boolean isRecordLimitMode,
        long maxFetchRecords,
        long requestFirstOffset,
        long requestLastOffset,
        InFlightBatch inFlightBatch,
        List<AcquiredRecords> result
    ) {
        lock.writeLock().lock();
        int acquiredCount = 0;
        long maxFetchRecordsWhileThrottledRecords = -1;
        boolean hasThrottledRecord = false;
        try {
            for (Map.Entry<Long, InFlightState> offsetState : inFlightBatch.offsetState().entrySet()) {
                // For the first batch which might have offsets prior to the request base
                // offset i.e. cached batch of 10-14 offsets and request batch of 12-13.
                if (offsetState.getKey() < requestFirstOffset) {
                    continue;
                }

                if (offsetState.getKey() > requestLastOffset) {
                    // No further offsets to process.
                    break;
                }

                if (offsetState.getValue().state() != RecordState.AVAILABLE || offsetState.getValue().hasOngoingStateTransition()) {
                    log.trace("The offset {} is not available in share partition: {}-{}, skipping: {}",
                        offsetState.getKey(), groupId, topicIdPartition, inFlightBatch);
                    continue;
                }

                int recordDeliveryCount = offsetState.getValue().deliveryCount();
                // If the record is on last delivery attempt then isolate that record to be delivered alone.
                // If the respective record is corrupt then it prevents increasing delivery count of multiple
                // records in a single response batch. Condition below checks if the current record has reached
                // the delivery limit and already have some records to return in response then skip processing
                // the current record, which shall be delivered alone in next fetch.
                if (maxDeliveryCount > 2 && recordDeliveryCount == maxDeliveryCount - 1 && acquiredCount > 0) {
                    break;
                }

                // When record delivery count reach the throttle threshold, progressively reduce batch size to isolate records.
                // The `maxFetchRecordsWhileThrottledRecords` is halved with each additional delivery attempt beyond the throttle limit.
                // Example:
                //   - maxDeliveryCount = 6, throttleRecordsDeliveryLimit = 3, batch size = 500
                //   - deliveryCount = 3: maxFetchRecords = 500 >> (3 - 3 + 1) = 250
                //   - deliveryCount = 4: maxFetchRecords = 500 >> (4 - 3 + 1) = 125
                // The `maxFetchRecordsWhileThrottledRecords` is calculated based on the first acquirable record that meets the throttling criteria in the batch.
                if (recordDeliveryCount >= throttleRecordsDeliveryLimit && maxFetchRecordsWhileThrottledRecords < 0) {
                    maxFetchRecordsWhileThrottledRecords = Math.max(1, (long) inFlightBatch.offsetState().size() >> (recordDeliveryCount - throttleRecordsDeliveryLimit + 1));
                    hasThrottledRecord = true;
                }

                InFlightState updateResult = offsetState.getValue().tryUpdateState(RecordState.ACQUIRED, DeliveryCountOps.INCREASE,
                    maxDeliveryCount, memberId);
                if (updateResult == null || updateResult.state() != RecordState.ACQUIRED) {
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
                    .setDeliveryCount((short) offsetState.getValue().deliveryCount()));
                acquiredCount++;

                // Delivered alone.
                if (offsetState.getValue().deliveryCount() == maxDeliveryCount && maxDeliveryCount > 2) {
                    break;
                }
                if (isRecordLimitMode && acquiredCount == maxFetchRecords) {
                    // In record_limit mode, acquire only the requested number of records.
                    break;
                }
                if (hasThrottledRecord && acquiredCount == maxFetchRecordsWhileThrottledRecords) {
                    break;
                }
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

    /**
     * Check if the in-flight batch should be throttled based on delivery count.
     *
     * @param inFlightBatch       The in-flight batch to check for throttling.
     * @param requestFirstOffset  The first offset to acquire.
     * @param requestLastOffset   THe last offset to acquire.
     * @return True if the batch should be throttled (delivery count >= threshold), false otherwise.
     */
    private boolean shouldThrottleRecordsDelivery(InFlightBatch inFlightBatch, long requestFirstOffset, long requestLastOffset) {
        if (inFlightBatch.offsetState() == null) {
            return inFlightBatch.batchDeliveryCount() >= throttleRecordsDeliveryLimit;
        }

        return inFlightBatch.offsetState().entrySet().stream().filter(entry -> {
            if (entry.getKey() < requestFirstOffset) {
                return false;
            }
            if (entry.getKey() > requestLastOffset) {
                return false;
            }
            if (entry.getValue().state() != RecordState.AVAILABLE) {
                return false;
            }
            return true;
        }).mapToInt(entry -> entry.getValue().deliveryCount()).max().orElse(0) >= throttleRecordsDeliveryLimit;
    }

    // Visibility for test
    static Map<Long, Byte> fetchAckTypeMapForBatch(ShareAcknowledgementBatch batch) {
        // Client can either send a single entry in acknowledgeTypes which represents the state
        // of the complete batch or can send individual offsets state. Construct a map with record state
        // for each offset in the batch, if single acknowledge type is sent, the map will have only one entry.
        Map<Long, Byte> ackTypeMap = new HashMap<>();
        for (int index = 0; index < batch.acknowledgeTypes().size(); index++) {
            byte ackType = batch.acknowledgeTypes().get(index);
            // Validate
            if (ackType != 0) {
                AcknowledgeType.forId(ackType);
            }
            ackTypeMap.put(batch.firstOffset() + index, ackType);
        }
        return ackTypeMap;
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
            if (subMap.lastEntry().getValue().lastOffset() < batch.firstOffset()) {
                log.debug("Request batch: {} has offsets which are not found for share partition: {}-{}", batch, groupId, topicIdPartition);
                throw new InvalidRequestException("Batch record not found. The first offset in request is past acquired records.");
            }

            // Validate if the request batch has the last offset greater than the last offset of
            // the last fetched cached batch, then there will be offsets in the request than cannot
            // be found in the fetched batches.
            if (batch.lastOffset() > subMap.lastEntry().getValue().lastOffset()) {
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
        Map<Long, Byte> ackTypeMap,
        NavigableMap<Long, InFlightBatch> subMap,
        List<PersisterBatch> persisterBatches
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

                    if (inFlightBatch.batchHasOngoingStateTransition()) {
                        log.debug("The batch has on-going transition, batch: {} for the share "
                            + "partition: {}-{}", inFlightBatch, groupId, topicIdPartition);
                        return Optional.of(new InvalidRecordStateException("The record state is invalid. The acknowledgement of delivery could not be completed."));
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
                        ackTypeMap, persisterBatches);
                } else {
                    // The in-flight batch is a full match hence change the state of the complete batch.
                    throwable = acknowledgeCompleteBatch(batch, inFlightBatch,
                        ackTypeMap.get(batch.firstOffset()), persisterBatches, memberId);
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
        Map<Long, Byte> ackTypeMap,
        List<PersisterBatch> persisterBatches
    ) {
        lock.writeLock().lock();
        try {
            for (Map.Entry<Long, InFlightState> offsetState : inFlightBatch.offsetState().entrySet()) {

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

                if (offsetState.getValue().state() != RecordState.ACQUIRED) {
                    log.debug("The offset is not acquired, offset: {} batch: {} for the share"
                            + " partition: {}-{}", offsetState.getKey(), inFlightBatch, groupId,
                        topicIdPartition);
                    return Optional.of(new InvalidRecordStateException(
                        "The offset cannot be acknowledged. The offset is not acquired."));
                }

                if (offsetState.getValue().hasOngoingStateTransition()) {
                    log.debug("The offset has on-going transition, offset: {} batch: {} for the share"
                            + " partition: {}-{}", offsetState.getKey(), inFlightBatch, groupId,
                        topicIdPartition);
                    return Optional.of(new InvalidRecordStateException(
                        "The record state is invalid. The acknowledgement of delivery could not be completed."));
                }

                // Check if member id is the owner of the offset.
                if (!offsetState.getValue().memberId().equals(memberId)) {
                    log.debug("Member {} is not the owner of offset: {} in batch: {} for the share"
                            + " partition: {}-{}", memberId, offsetState.getKey(), inFlightBatch,
                        groupId, topicIdPartition);
                    return Optional.of(
                        new InvalidRecordStateException("Member is not the owner of offset"));
                }

                // In case of 0 size ackTypeMap, we have already validated the batch.acknowledgeTypes.
                byte ackType = ackTypeMap.size() > 1 ? ackTypeMap.get(offsetState.getKey()) : batch.acknowledgeTypes().get(0);

                if (ackType == AcknowledgeType.RENEW.id) {
                    // If RENEW, renew the acquisition lock timer for this offset and continue without changing state.
                    // We do not care about recordState map here.
                    // Only valid for ACQUIRED offsets; the check above ensures this.
                    long key = offsetState.getKey();
                    InFlightState state = offsetState.getValue();
                    log.debug("Renewing acq lock for {}-{} with offset {} in batch {} for member {}.",
                        groupId, topicIdPartition, key, inFlightBatch, memberId);
                    state.cancelAndClearAcquisitionLockTimeoutTask();
                    AcquisitionLockTimerTask renewalTask = scheduleAcquisitionLockTimeout(memberId, key, key);
                    state.updateAcquisitionLockTimeoutTask(renewalTask);
                } else {
                    // Determine the record state for the offset. If the per offset record state is not provided
                    // by the client, then use the batch record state. This will always be present as it is a static
                    // mapping between bytes and record state type. All ack types have been added except for RENEW which
                    // has been handled above.
                    RecordState recordState = ACK_TYPE_TO_RECORD_STATE.get(ackType);
                    Objects.requireNonNull(recordState);

                    InFlightState updateResult = offsetState.getValue().startStateTransition(
                        recordState,
                        DeliveryCountOps.NO_OP,
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
                    // Successfully updated the state of the offset and created a persister state batch for write to persister.
                    persisterBatches.add(new PersisterBatch(updateResult, new PersisterStateBatch(offsetState.getKey(),
                        offsetState.getKey(), updateResult.state().id(), (short) updateResult.deliveryCount())));
                    if (isStateTerminal(updateResult.state())) {
                        deliveryCompleteCount.incrementAndGet();
                    }
                    // Do not update the nextFetchOffset as the offset has not completed the transition yet.
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
        byte ackType,
        List<PersisterBatch> persisterBatches,
        String memberId
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

            // If the request is a full-batch RENEW acknowledgement (ack type 4), then renew the
            // acquisition lock without changing the state or persisting anything.
            // Before reaching this point, it should be verified that it is full batch ack and
            // not per offset ack as well as startOffset not moved.
            if (ackType == AcknowledgeType.RENEW.id) {
                // Renew the acquisition lock timer for the complete batch. We have already
                // checked that the batchState is ACQUIRED above.
                log.debug("Renewing acq lock for {}-{} with batch {}-{} for member {}.",
                    groupId, topicIdPartition, inFlightBatch.firstOffset(), inFlightBatch.lastOffset(), memberId);
                inFlightBatch.cancelAndClearAcquisitionLockTimeoutTask();
                AcquisitionLockTimerTask renewalTask = scheduleAcquisitionLockTimeout(memberId,
                    inFlightBatch.firstOffset(), inFlightBatch.lastOffset());
                inFlightBatch.updateAcquisitionLockTimeout(renewalTask);
                // Nothing to persist.
                return Optional.empty();
            }

            // Change the state of complete batch since the same state exists for the entire inFlight batch.
            // The member id is reset to EMPTY_MEMBER_ID irrespective of the ack type as the batch is
            // either released or moved to a state where member id existence is not important. The member id
            // is only important when the batch is acquired.
            RecordState recordState = ACK_TYPE_TO_RECORD_STATE.get(ackType);
            Objects.requireNonNull(recordState);

            InFlightState updateResult = inFlightBatch.startBatchStateTransition(
                recordState,
                DeliveryCountOps.NO_OP,
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

            // Successfully updated the state of the batch and created a persister state batch for write to persister.
            persisterBatches.add(new PersisterBatch(updateResult, new PersisterStateBatch(inFlightBatch.firstOffset(),
                inFlightBatch.lastOffset(), updateResult.state().id(), (short) updateResult.deliveryCount())));
            if (isStateTerminal(updateResult.state())) {
                deliveryCompleteCount.addAndGet((int) (inFlightBatch.lastOffset() - inFlightBatch.firstOffset() + 1));
            }
            // Do not update the next fetch offset as the batch has not completed the transition yet.
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
        List<PersisterBatch> persisterBatches
    ) {
        lock.writeLock().lock();
        try {
            if (throwable != null) {
                // Log in DEBUG to avoid flooding of logs for a faulty client.
                log.debug("Request failed for updating state, rollback any changed state"
                    + " for the share partition: {}-{}", groupId, topicIdPartition);
                persisterBatches.forEach(persisterBatch -> {
                    persisterBatch.updatedState.completeStateTransition(false);
                    if (persisterBatch.updatedState.state() == RecordState.AVAILABLE) {
                        updateFindNextFetchOffset(true);
                    }
                    // If there is a failure, then update the deliveryCompleteCount only in case there are some records
                    // which were in a Terminal state, but after rolling back they are in a non-Terminal state. We also
                    // need to consider only those records that lie after the start offset, because LSO movement can happen
                    // after local state transition begins but before writeState result is obtained.
                    if (isStateTerminal(RecordState.forId(persisterBatch.stateBatch.deliveryState())) && !isStateTerminal(persisterBatch.updatedState.state())) {
                        deliveryCompleteCount.addAndGet(-numInFlightRecordsInBatch(persisterBatch.stateBatch.firstOffset(), persisterBatch.stateBatch.lastOffset()));
                    }
                });
                future.completeExceptionally(throwable);
                return;
            }

            if (persisterBatches.isEmpty()) {
                future.complete(null);
                return;
            }
        } finally {
            lock.writeLock().unlock();
        }

        writeShareGroupState(persisterBatches.stream().map(PersisterBatch::stateBatch).toList())
            .whenComplete((result, exception) -> {
                // There can be a pending delayed share fetch requests for the share partition which are waiting
                // on the startOffset to move ahead, hence track if the state is updated in the cache. If
                // yes, then notify the delayed share fetch purgatory to complete the pending requests.
                boolean cacheStateUpdated = false;
                lock.writeLock().lock();
                try {
                    if (exception != null) {
                        log.debug("Failed to write state to persister for the share partition: {}-{}",
                            groupId, topicIdPartition, exception);
                        // In case of failure when transition state is rolled back then it should be rolled
                        // back to ACQUIRED state, unless acquisition lock for the state has expired.
                        persisterBatches.forEach(persisterBatch -> {
                            persisterBatch.updatedState().completeStateTransition(false);
                            if (persisterBatch.updatedState().state() == RecordState.AVAILABLE) {
                                updateFindNextFetchOffset(true);
                            }
                            // If there is a failure, then update the deliveryCompleteCount only in case there are some records
                            // which were in a Terminal state, but after rolling back they are in a non-Terminal state. We also
                            // need to consider only those records that lie after the start offset, because LSO movement can happen
                            // after local state transition begins but before writeState result is obtained.
                            if (isStateTerminal(RecordState.forId(persisterBatch.stateBatch.deliveryState())) && !isStateTerminal(persisterBatch.updatedState.state())) {
                                deliveryCompleteCount.addAndGet(-numInFlightRecordsInBatch(persisterBatch.stateBatch.firstOffset(), persisterBatch.stateBatch.lastOffset()));
                            }
                        });
                        future.completeExceptionally(exception);
                        return;
                    }

                    log.trace("State change request successful for share partition: {}-{}",
                        groupId, topicIdPartition);
                    persisterBatches.forEach(persisterBatch -> {
                        persisterBatch.updatedState.completeStateTransition(true);
                        if (persisterBatch.updatedState.state() == RecordState.AVAILABLE) {
                            updateFindNextFetchOffset(true);
                        }
                    });
                    // Update the cached state and start and end offsets after acknowledging/releasing the acquired records.
                    cacheStateUpdated = maybeUpdateCachedStateAndOffsets();
                    future.complete(null);
                } finally {
                    lock.writeLock().unlock();
                    // Maybe complete the delayed share fetch request if the state has been changed in cache
                    // which might have moved start offset ahead. Hence, the pending delayed share fetch
                    // request can be completed. The call should be made outside the lock to avoid deadlock.
                    maybeCompleteDelayedShareFetchRequest(cacheStateUpdated);
                }
            });
    }

    //  This method returns the number of records in the batch that are in-flight and thus lie after the start offset.
    private int numInFlightRecordsInBatch(long batchFirstOffset, long batchEndOffset) {
        if (batchEndOffset >= startOffset) {
            return (int) (batchEndOffset - Math.max(startOffset, batchFirstOffset) + 1);
        }
        return 0;
    }

    private boolean maybeUpdateCachedStateAndOffsets() {
        lock.writeLock().lock();
        try {
            if (!canMoveStartOffset()) {
                return false;
            }

            // This will help to find the next position for the startOffset.
            // The new position of startOffset will be lastOffsetAcknowledged + 1
            OffsetAndMetadata result = findLastOffsetAcknowledgedAndMetadata();
            long lastOffsetAcknowledged = result.lastAcknowledgedOffset;
            // If lastOffsetAcknowledged is -1, this means we cannot move startOffset ahead
            if (lastOffsetAcknowledged == -1) {
                return false;
            }

            // This is true if all records in the cachedState have been acknowledged (either Accept or Reject).
            // The resulting action should be to empty the cachedState altogether
            long lastCachedOffset = cachedState.lastEntry().getValue().lastOffset();
            if (lastOffsetAcknowledged == lastCachedOffset) {
                startOffset = lastCachedOffset + 1; // The next offset that will be fetched and acquired in the share partition
                endOffset = lastCachedOffset + 1;
                cachedState.clear();
                deliveryCompleteCount.set(0);
                // Nothing further to do.
                return true;
            }

            /*
             The cachedState contains some records that are yet to be acknowledged, and thus should
             not be removed. Only a subMap will be removed from the cachedState. The logic to remove
             batches from cachedState is as follows:
             a) Only full batches can be removed from the cachedState, For example if there is batch (0-99)
             and 0-49 records are acknowledged (ACCEPT or REJECT), the first 50 records will not be removed
             from the cachedState. Instead, the startOffset will be moved to 50, but the batch will only
             be removed once all the records (0-99) are acknowledged (ACCEPT or REJECT).
            */

            // Since only a subMap will be removed, we need to find the first and last keys of that subMap
            long firstKeyToRemove = cachedState.firstKey();
            long lastKeyToRemove;
            NavigableMap.Entry<Long, InFlightBatch> entry = cachedState.floorEntry(lastOffsetAcknowledged);
            // If the lastOffsetAcknowledged is equal to the last offset of entry, then the entire batch can potentially be removed.
            if (lastOffsetAcknowledged == entry.getValue().lastOffset()) {
                long lastOffsetAcknowledgedHigherKey = cachedState.higherKey(lastOffsetAcknowledged);
                if (lastOffsetAcknowledgedHigherKey > startOffset) {
                    startOffset = lastOffsetAcknowledgedHigherKey;
                }
                if (isPersisterReadGapWindowActive()) {
                    // This case will arise if we have a situation where there is an acquirable gap after the lastOffsetAcknowledged.
                    // Ex, the cachedState has following state batches -> {(0, 10), (11, 20), (31,40)} and all these batches are acked.
                    // There is a gap from 21 to 30. Let the gapWindow's gapStartOffset be 21. In this case,
                    // lastOffsetAcknowledged will be 20, but we cannot simply move the start offset to the first offset
                    // of next cachedState batch (next cachedState batch is 31 to 40). There is an acquirable gap in between (21 to 30)
                    // and The startOffset should be at 21. Hence, we set startOffset to the minimum of gapWindow.gapStartOffset
                    // and higher key of lastOffsetAcknowledged
                    startOffset = Math.min(persisterReadResultGapWindow.gapStartOffset(), startOffset);
                }
                lastKeyToRemove = entry.getKey();
            } else {
                // The code will reach this point only if lastOffsetAcknowledged is in the middle of some stateBatch. In this case
                // we can move the startOffset to the next offset of lastOffsetAcknowledged only if that offset is
                // ahead of start offset and should consider any read gap offsets.
                if (lastOffsetAcknowledged + 1 > startOffset) {
                    startOffset = lastOffsetAcknowledged + 1;
                }
                if (entry.getKey().equals(cachedState.firstKey())) {
                    // If the first batch in cachedState has some records yet to be acknowledged,
                    // then nothing should be removed from cachedState
                    lastKeyToRemove = -1;
                } else {
                    lastKeyToRemove = cachedState.lowerKey(entry.getKey());
                }
            }
            deliveryCompleteCount.addAndGet((-1) * result.numTerminalRecords);
            if (lastKeyToRemove != -1) {
                cachedState.subMap(firstKeyToRemove, true, lastKeyToRemove, true).clear();
            }
            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    // Visible for testing.
    boolean canMoveStartOffset() {
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
            // The start offset is not found in the cached state when there is a gap starting at the start offset.
            // For example, if the start offset is 10 and the cached state has batches -> { (21, 30), (31, 40) }.
            // This case arises only when the share partition is initialized and the read state response results in
            // state batches containing gaps. This situation is possible in the case where in the previous instance
            // of this share partition, the gap offsets were fetched but not acknowledged, and the next batch of offsets
            // were fetched as well as acknowledged. In the above example, possibly in the previous instance of the share
            // partition, the batch 10-20 was fetched but not acknowledged and the batch 21-30 was fetched and acknowledged.
            // Thus, the persister has no clue about what happened with the batch 10-20. During the re-initialization of
            // the share partition, the start offset is set to 10 and the cached state has the batch 21-30, resulting in a gap.
            log.debug("The start offset: {} is not found in the cached state for share partition: {}-{} " +
                "as there is an acquirable gap at the beginning. Cannot move the start offset.", startOffset, groupId, topicIdPartition);
            return false;
        }
        boolean isBatchState = entry.getValue().offsetState() == null;
        boolean isOngoingTransition = isBatchState ?
            entry.getValue().batchHasOngoingStateTransition() :
            entry.getValue().offsetState().get(startOffset).hasOngoingStateTransition();
        if (isOngoingTransition) {
            return false;
        }

        RecordState startOffsetState = isBatchState ?
            entry.getValue().batchState() :
            entry.getValue().offsetState().get(startOffset).state();
        return isStateTerminal(startOffsetState);
    }

    private boolean isPersisterReadGapWindowActive() {
        return persisterReadResultGapWindow != null && persisterReadResultGapWindow.endOffset() == endOffset;
    }

    /**
     * The record state is considered terminal if it is either acknowledged or archived.
     * These are terminal states for the record.
     *
     * @param recordState The record state to check.
     *
     * @return True if the record state is acknowledged or archived, false otherwise.
     */
    private boolean isStateTerminal(RecordState recordState) {
        return recordState == RecordState.ACKNOWLEDGED || recordState == RecordState.ARCHIVED;
    }

    /**
     * This method is used to find the new position for startOffset. If there are records in the cached state right after
     * the current startOffset which are in terminal state (ACKNOWLEDGED or ARCHIVED), then the startOffset can be moved
     * past these. The method iterates over the cached state and finds the last offset which is in terminal state which
     * can be moved out of in-flight. The method also counts the number of records which are in terminal state which would
     * be removed from in-flight if the startOffset is moved, in order to update the deliveryCompleteCount count.
     *
     * @return StartOffsetAdvanceResult, which contains the last offset post which the startOffset can move
     * and the number of terminal records which be moved out of in-flight if the startOffset is moved.
     */
    // Visible for testing.
    OffsetAndMetadata findLastOffsetAcknowledgedAndMetadata() {
        long lastOffsetAcknowledged = -1;
        int numTerminalRecords = 0;
        lock.readLock().lock();
        try {
            for (NavigableMap.Entry<Long, InFlightBatch> entry : cachedState.entrySet()) {
                InFlightBatch inFlightBatch = entry.getValue();

                if (isPersisterReadGapWindowActive() && inFlightBatch.lastOffset() >= persisterReadResultGapWindow.gapStartOffset()) {
                    return new OffsetAndMetadata(lastOffsetAcknowledged, numTerminalRecords);
                }

                if (inFlightBatch.offsetState() == null) {
                    if (inFlightBatch.batchHasOngoingStateTransition() || !isStateTerminal(inFlightBatch.batchState())) {
                        return new OffsetAndMetadata(lastOffsetAcknowledged, numTerminalRecords);
                    }
                    lastOffsetAcknowledged = inFlightBatch.lastOffset();
                    numTerminalRecords += numInFlightRecordsInBatch(inFlightBatch.firstOffset(), inFlightBatch.lastOffset());
                } else {
                    for (Map.Entry<Long, InFlightState> offsetState : inFlightBatch.offsetState().entrySet()) {
                        if (offsetState.getValue().hasOngoingStateTransition() || !isStateTerminal(offsetState.getValue().state())) {
                            return new OffsetAndMetadata(lastOffsetAcknowledged, numTerminalRecords);
                        }
                        lastOffsetAcknowledged = offsetState.getKey();
                        if (offsetState.getKey() >= startOffset) {
                            numTerminalRecords++;
                        }
                    }
                }
            }
        } finally {
            lock.readLock().unlock();
        }
        return new OffsetAndMetadata(lastOffsetAcknowledged, numTerminalRecords);
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
                .setTopicsData(List.of(new TopicData<>(topicIdPartition.topicId(),
                    List.of(PartitionFactory.newPartitionStateBatchData(
                        topicIdPartition.partition(), stateEpoch, startOffset(), deliveryCompleteCount(), leaderEpoch, stateBatches))))
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
                    maybeLogError(String.format("Failed to write the share group state for share partition: %s-%s due to exception",
                        groupId, topicIdPartition), Errors.forCode(partitionData.errorCode()), ex);
                    future.completeExceptionally(ex);
                    return;
                }
                future.complete(null);
            });
        return future;
    }

    private KafkaException fetchPersisterError(short errorCode, String errorMessage) {
        Errors error = Errors.forCode(errorCode);
        return switch (error) {
            case NOT_COORDINATOR, COORDINATOR_NOT_AVAILABLE, COORDINATOR_LOAD_IN_PROGRESS ->
                new CoordinatorNotAvailableException(errorMessage);
            case GROUP_ID_NOT_FOUND ->
                new GroupIdNotFoundException(errorMessage);
            case UNKNOWN_TOPIC_OR_PARTITION ->
                new UnknownTopicOrPartitionException(errorMessage);
            case FENCED_LEADER_EPOCH, FENCED_STATE_EPOCH ->
                new NotLeaderOrFollowerException(errorMessage);
            case SASL_AUTHENTICATION_FAILED, UNSUPPORTED_VERSION ->
                new UnknownServerException("Unable to complete operation due to server error.");
            default ->
                new UnknownServerException(errorMessage);
        };
    }

    // Visible for testing
    AcquisitionLockTimerTask scheduleAcquisitionLockTimeout(String memberId, long firstOffset, long lastOffset) {
        // The recordLockDuration value would depend on whether the dynamic config SHARE_RECORD_LOCK_DURATION_MS in
        // GroupConfig.java is set or not. If dynamic config is set, then that is used, otherwise the value of
        // SHARE_GROUP_RECORD_LOCK_DURATION_MS_CONFIG defined in ShareGroupConfig is used
        int recordLockDurationMs = recordLockDurationMsOrDefault(groupConfigManager, groupId, defaultRecordLockDurationMs);
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
        return new AcquisitionLockTimerTask(time, delayMs, memberId, firstOffset, lastOffset, releaseAcquisitionLockOnTimeout(), sharePartitionMetrics);
    }

    private AcquisitionLockTimeoutHandler releaseAcquisitionLockOnTimeout() {
        return (memberId, firstOffset, lastOffset, timerTask) -> {
            List<PersisterStateBatch> stateBatches;
            lock.writeLock().lock();
            try {
                // Check if timer task is already cancelled. This can happen when concurrent requests
                // happen to acknowledge in-flight state and timeout handler is waiting for the lock
                // but already cancelled.
                if (timerTask.isCancelled()) {
                    log.debug("Timer task is already cancelled, not executing further.");
                    return;
                }

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
            } finally {
                lock.writeLock().unlock();
            }

            if (!stateBatches.isEmpty()) {
                writeShareGroupState(stateBatches).whenComplete((result, exception) -> {
                    if (exception != null) {
                        log.debug("Failed to write the share group state on acquisition lock timeout for share partition: {}-{} memberId: {}",
                            groupId, topicIdPartition, memberId, exception);
                    }
                    // Even if write share group state RPC call fails, we will still go ahead with the state transition.
                    // Update the cached state and start and end offsets after releasing the acquisition lock on timeout.
                    maybeUpdateCachedStateAndOffsets();
                });
            }

            // If we have an acquisition lock timeout for a share-partition, then we should check if
            // there is a pending share fetch request for the share-partition and complete it.
            // Skip null check for stateBatches, it should always be initialized if reached here.
            maybeCompleteDelayedShareFetchRequest(!stateBatches.isEmpty());
        };
    }

    private void releaseAcquisitionLockOnTimeoutForCompleteBatch(InFlightBatch inFlightBatch,
                                                                 List<PersisterStateBatch> stateBatches,
                                                                 String memberId) {
        if (inFlightBatch.batchState() == RecordState.ACQUIRED) {
            InFlightState updateResult = inFlightBatch.tryUpdateBatchState(
                    inFlightBatch.lastOffset() < startOffset ? RecordState.ARCHIVED : RecordState.AVAILABLE,
                    DeliveryCountOps.NO_OP,
                    maxDeliveryCount,
                    EMPTY_MEMBER_ID);
            if (updateResult == null) {
                log.error("Unable to release acquisition lock on timeout for the batch: {}"
                        + " for the share partition: {}-{} memberId: {}", inFlightBatch, groupId, topicIdPartition, memberId);
                return;
            }
            stateBatches.add(new PersisterStateBatch(inFlightBatch.firstOffset(), inFlightBatch.lastOffset(),
                    updateResult.state().id(), (short) updateResult.deliveryCount()));

            // Cancel the acquisition lock timeout task for the batch since it is completed now.
            updateResult.cancelAndClearAcquisitionLockTimeoutTask();
            if (updateResult.state() != RecordState.ARCHIVED) {
                updateFindNextFetchOffset(true);
            }
            // If after acquisition lock timeout, a batch is moved to ARCHIVED state, then we have records moved from
            // non-Terminal state to a Terminal state. Hence, we need to update deliveryCompleteCount. But there could
            // be a situation where the batch was acquired, and LSO moved past. In that case, we only need to consider the
            // records from the current batch that are greater than or equal to the new startOffset. For the records that
            // lie before the new startOffset, they would have already been counted in deliveryCompleteCount when LSO moved.
            if (isStateTerminal(updateResult.state())) {
                deliveryCompleteCount.addAndGet(numInFlightRecordsInBatch(inFlightBatch.firstOffset(), inFlightBatch.lastOffset()));
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
            if (offsetState.getValue().state() != RecordState.ACQUIRED) {
                log.debug("The offset is not in acquired state while release of acquisition lock on timeout, skipping, offset: {} batch: {}"
                                + " for the share partition: {}-{} memberId: {}", offsetState.getKey(), inFlightBatch,
                        groupId, topicIdPartition, memberId);
                continue;
            }
            InFlightState updateResult = offsetState.getValue().tryUpdateState(
                    offsetState.getKey() < startOffset ? RecordState.ARCHIVED : RecordState.AVAILABLE,
                    DeliveryCountOps.NO_OP,
                    maxDeliveryCount,
                    EMPTY_MEMBER_ID);
            if (updateResult == null) {
                log.error("Unable to release acquisition lock on timeout for the offset: {} in batch: {}"
                                + " for the share partition: {}-{} memberId: {}", offsetState.getKey(), inFlightBatch,
                        groupId, topicIdPartition, memberId);
                continue;
            }
            stateBatches.add(new PersisterStateBatch(offsetState.getKey(), offsetState.getKey(),
                    updateResult.state().id(), (short) updateResult.deliveryCount()));

            // Cancel the acquisition lock timeout task for the offset since it is completed now.
            updateResult.cancelAndClearAcquisitionLockTimeoutTask();
            if (updateResult.state() != RecordState.ARCHIVED) {
                updateFindNextFetchOffset(true);
            }
            // If after acquisition lock timeout, an offset state is moved to ARCHIVED, then we have records moved from
            // non-Terminal state to a Terminal state. Hence, we need to update deliveryCompleteCount, but only if the
            // records are past the startOffset. If the LSO was moved in between, then we don't need to care about the
            // records that lie before the startOffset as they would have already been counted in deliveryCompleteCount.
            if (offsetState.getKey() >= startOffset && isStateTerminal(updateResult.state())) {
                deliveryCompleteCount.incrementAndGet();
            }
        }
    }

    private void maybeCompleteDelayedShareFetchRequest(boolean shouldComplete) {
        if (shouldComplete) {
            replicaManager.completeDelayedShareFetchRequest(delayedShareFetchKey);
        }
    }

    private long startOffsetDuringInitialization(long partitionDataStartOffset) {
        // Set the state epoch and end offset from the persisted state.
        if (partitionDataStartOffset != PartitionFactory.UNINITIALIZED_START_OFFSET) {
            return partitionDataStartOffset;
        }
        ShareGroupAutoOffsetResetStrategy offsetResetStrategy;
        if (groupConfigManager.groupConfig(groupId).isPresent()) {
            offsetResetStrategy = groupConfigManager.groupConfig(groupId).get().shareAutoOffsetReset();
        } else {
            offsetResetStrategy = GroupConfig.defaultShareAutoOffsetReset();
        }

        if (offsetResetStrategy.type() == ShareGroupAutoOffsetResetStrategy.StrategyType.LATEST) {
            return offsetForLatestTimestamp(topicIdPartition, replicaManager, leaderEpoch);
        } else if (offsetResetStrategy.type() == ShareGroupAutoOffsetResetStrategy.StrategyType.EARLIEST) {
            return offsetForEarliestTimestamp(topicIdPartition, replicaManager, leaderEpoch);
        } else {
            // offsetResetStrategy type is BY_DURATION
            return offsetForTimestamp(topicIdPartition, replicaManager, offsetResetStrategy.timestamp(), leaderEpoch);
        }
    }

    private ShareAcquiredRecords maybeFilterAbortedTransactionalAcquiredRecords(
        FetchPartitionData fetchPartitionData,
        FetchIsolation isolationLevel,
        ShareAcquiredRecords shareAcquiredRecords
    ) {
        if (isolationLevel != FetchIsolation.TXN_COMMITTED || fetchPartitionData.abortedTransactions.isEmpty() || fetchPartitionData.abortedTransactions.get().isEmpty())
            return shareAcquiredRecords;

        // When FetchIsolation.TXN_COMMITTED is used as isolation level by the share group, we need to filter any
        // transactions that were aborted/did not commit due to timeout.
        List<AcquiredRecords> result = filterAbortedTransactionalAcquiredRecords(fetchPartitionData.records.batches(),
            shareAcquiredRecords.acquiredRecords(), fetchPartitionData.abortedTransactions.get());
        int acquiredCount = 0;
        for (AcquiredRecords records : result) {
            acquiredCount += (int) (records.lastOffset() - records.firstOffset() + 1);
        }
        return new ShareAcquiredRecords(result, acquiredCount);
    }

    private List<AcquiredRecords> filterAbortedTransactionalAcquiredRecords(
        Iterable<? extends RecordBatch> batches,
        List<AcquiredRecords> acquiredRecords,
        List<FetchResponseData.AbortedTransaction> abortedTransactions
    ) {
        // The record batches that need to be archived in cachedState because they were a part of aborted transactions.
        List<RecordBatch> recordsToArchive = fetchAbortedTransactionRecordBatches(batches, abortedTransactions);
        for (RecordBatch recordBatch : recordsToArchive) {
            // Archive the offsets/batches in the cached state.
            NavigableMap<Long, InFlightBatch> subMap = fetchSubMap(recordBatch);
            archiveRecords(recordBatch.baseOffset(), recordBatch.lastOffset() + 1, subMap, RecordState.ACQUIRED, true);
        }
        return filterRecordBatchesFromAcquiredRecords(acquiredRecords, recordsToArchive);
    }

    private void maybeLogError(String message, Errors receivedError, Throwable wrappedException) {
        if (receivedError == Errors.NETWORK_EXCEPTION) {
            log.debug(message, wrappedException);
        } else {
            log.error(message, wrappedException);
        }
    }

    /**
     * This function filters out the offsets present in the acquired records list that are also a part of batches that need to be archived.
     * It follows an iterative refinement of acquired records to eliminate batches to be archived.
     * @param acquiredRecordsList The list containing acquired records. This list is sorted by the firstOffset of the acquired batch.
     * @param batchesToArchive The list containing record batches to archive. This list is sorted by the baseOffset of the record batch.
     * @return The list containing filtered acquired records offsets.
     */
    List<AcquiredRecords> filterRecordBatchesFromAcquiredRecords(
        List<AcquiredRecords> acquiredRecordsList,
        List<RecordBatch> batchesToArchive
    ) {
        Iterator<RecordBatch> batchesToArchiveIterator = batchesToArchive.iterator();
        if (!batchesToArchiveIterator.hasNext())
            return acquiredRecordsList;
        List<AcquiredRecords> result = new ArrayList<>();
        Iterator<AcquiredRecords> acquiredRecordsListIter = acquiredRecordsList.iterator();
        RecordBatch batchToArchive = batchesToArchiveIterator.next();
        AcquiredRecords unresolvedAcquiredRecords = null;

        while (unresolvedAcquiredRecords != null || acquiredRecordsListIter.hasNext()) {
            if (unresolvedAcquiredRecords == null)
                unresolvedAcquiredRecords = acquiredRecordsListIter.next();

            long unresolvedFirstOffset = unresolvedAcquiredRecords.firstOffset();
            long unresolvedLastOffset = unresolvedAcquiredRecords.lastOffset();
            short unresolvedDeliveryCount = unresolvedAcquiredRecords.deliveryCount();

            if (batchToArchive == null) {
                result.add(unresolvedAcquiredRecords);
                unresolvedAcquiredRecords = null;
                continue;
            }

            // Non-overlap check - unresolvedFirstOffset offsets lie before the batchToArchive offsets. No need to filter out the offsets in such a scenario.
            if (unresolvedLastOffset < batchToArchive.baseOffset()) {
                // Offsets in unresolvedAcquiredRecords do not overlap with batchToArchive, hence it should not get filtered out.
                result.add(unresolvedAcquiredRecords);
                unresolvedAcquiredRecords = null;
            }

            // Overlap check - unresolvedFirstOffset offsets overlap with the batchToArchive offsets. We need to filter out the overlapping
            // offsets in such a scenario.
            if (unresolvedFirstOffset <= batchToArchive.lastOffset() &&
                unresolvedLastOffset >= batchToArchive.baseOffset()) {
                unresolvedAcquiredRecords = null;
                // Split the unresolvedFirstOffset into parts - before and after the overlapping record batchToArchive.
                if (unresolvedFirstOffset < batchToArchive.baseOffset()) {
                    // The offsets in unresolvedAcquiredRecords that are present before batchToArchive's baseOffset should not get filtered out.
                    result.add(new AcquiredRecords()
                        .setFirstOffset(unresolvedFirstOffset)
                        .setLastOffset(batchToArchive.baseOffset() - 1)
                        .setDeliveryCount(unresolvedDeliveryCount));
                }
                if (unresolvedLastOffset > batchToArchive.lastOffset()) {
                    // The offsets in unresolvedAcquiredRecords that are present after batchToArchive's lastOffset should not get filtered out
                    // and should be taken forward for further processing since they could potentially contain offsets that need to be archived.
                    unresolvedAcquiredRecords = new AcquiredRecords()
                        .setFirstOffset(batchToArchive.lastOffset() + 1)
                        .setLastOffset(unresolvedLastOffset)
                        .setDeliveryCount(unresolvedDeliveryCount);
                }
            }

            // There is at least one offset in unresolvedFirstOffset which lies after the batchToArchive. Hence, we move forward
            // the batchToArchive to the next element in batchesToArchiveIterator.
            if (unresolvedLastOffset > batchToArchive.lastOffset()) {
                if (batchesToArchiveIterator.hasNext())
                    batchToArchive = batchesToArchiveIterator.next();
                else
                    batchToArchive = null;
            }
        }
        return result;
    }

    /**
     * This function fetches the sub map from cachedState where all the offset details present in the recordBatch can be referred to
     * OR it gives an exception if those offsets are not present in cachedState.
     * @param recordBatch The record batch for which we want to find the sub map.
     * @return the sub map containing all the offset details.
     */
    private NavigableMap<Long, InFlightBatch> fetchSubMap(RecordBatch recordBatch) {
        lock.readLock().lock();
        try {
            Map.Entry<Long, InFlightBatch> floorEntry = cachedState.floorEntry(recordBatch.baseOffset());
            if (floorEntry == null) {
                log.debug("Fetched batch record {} not found for share partition: {}-{}", recordBatch, groupId,
                    topicIdPartition);
                throw new IllegalStateException(
                    "Batch record not found. The request batch offsets are not found in the cache.");
            }
            return cachedState.subMap(floorEntry.getKey(), true, recordBatch.lastOffset(), true);
        } finally {
            lock.readLock().unlock();
        }
    }

    // Visible for testing.
    List<RecordBatch> fetchAbortedTransactionRecordBatches(
        Iterable<? extends RecordBatch> batches,
        List<FetchResponseData.AbortedTransaction> abortedTransactions
    ) {
        PriorityQueue<FetchResponseData.AbortedTransaction> orderedAbortedTransactions = orderedAbortedTransactions(abortedTransactions);
        Set<Long> abortedProducerIds = new HashSet<>();
        List<RecordBatch> recordsToArchive = new ArrayList<>();

        for (RecordBatch currentBatch : batches) {
            if (currentBatch.hasProducerId()) {
                // remove from the aborted transactions queue, all aborted transactions which have begun before the
                // current batch's last offset and add the associated producerIds to the aborted producer set.
                while (!orderedAbortedTransactions.isEmpty() && orderedAbortedTransactions.peek().firstOffset() <= currentBatch.lastOffset()) {
                    FetchResponseData.AbortedTransaction abortedTransaction = orderedAbortedTransactions.poll();
                    abortedProducerIds.add(abortedTransaction.producerId());
                }
                long producerId = currentBatch.producerId();
                if (containsAbortMarker(currentBatch)) {
                    abortedProducerIds.remove(producerId);
                } else if (isBatchAborted(currentBatch, abortedProducerIds)) {
                    log.debug("Skipping aborted record batch for share partition: {}-{} with producerId {} and " +
                        "offsets {} to {}", groupId, topicIdPartition, producerId, currentBatch.baseOffset(), currentBatch.lastOffset());
                    recordsToArchive.add(currentBatch);
                }
            }
        }
        return recordsToArchive;
    }

    private PriorityQueue<FetchResponseData.AbortedTransaction> orderedAbortedTransactions(List<FetchResponseData.AbortedTransaction> abortedTransactions) {
        PriorityQueue<FetchResponseData.AbortedTransaction> orderedAbortedTransactions = new PriorityQueue<>(
            abortedTransactions.size(), Comparator.comparingLong(FetchResponseData.AbortedTransaction::firstOffset)
        );
        orderedAbortedTransactions.addAll(abortedTransactions);
        return orderedAbortedTransactions;
    }

    private boolean isBatchAborted(RecordBatch batch, Set<Long> abortedProducerIds) {
        return batch.isTransactional() && abortedProducerIds.contains(batch.producerId());
    }

    // Visible for testing.
    boolean containsAbortMarker(RecordBatch batch) {
        if (!batch.isControlBatch())
            return false;

        Iterator<Record> batchIterator = batch.iterator();
        if (!batchIterator.hasNext())
            return false;

        Record firstRecord = batchIterator.next();
        return ControlRecordType.ABORT == ControlRecordType.parse(firstRecord.key());
    }

    // Visible for testing. Should only be used for testing purposes.
    NavigableMap<Long, InFlightBatch> cachedState() {
        return new ConcurrentSkipListMap<>(cachedState);
    }

    // Visible for testing.
    boolean findNextFetchOffset() {
        lock.readLock().lock();
        try {
            return findNextFetchOffset;
        } finally {
            lock.readLock().unlock();
        }
    }

    private boolean isRecordLimitMode(ShareAcquireMode shareAcquireMode) {
        return ShareAcquireMode.RECORD_LIMIT.equals(shareAcquireMode);
    }

    // Visible for testing.
    void updateFindNextFetchOffset(boolean value) {
        lock.writeLock().lock();
        try {
            findNextFetchOffset = value;
        } finally {
            lock.writeLock().unlock();
        }
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
    GapWindow persisterReadResultGapWindow() {
        return persisterReadResultGapWindow;
    }

    // Visible for testing.
    Uuid fetchLock() {
        return fetchLock.get();
    }

    // Visible for testing.
    int deliveryCompleteCount() {
        return deliveryCompleteCount.get();
    }

    /**
     * The GapWindow class is used to record the gap start and end offset of the probable gaps
     * of available records which are neither known to Persister nor to SharePartition. Share Partition
     * will use this information to determine the next fetch offset and should try to fetch the records
     * in the gap.
     */
    // Visible for Testing
    static class GapWindow {
        private final long endOffset;
        private long gapStartOffset;

        GapWindow(long endOffset, long gapStartOffset) {
            this.endOffset = endOffset;
            this.gapStartOffset = gapStartOffset;
        }

        long endOffset() {
            return endOffset;
        }

        long gapStartOffset() {
            return gapStartOffset;
        }

        void gapStartOffset(long gapStartOffset) {
            this.gapStartOffset = gapStartOffset;
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

    /**
     * PersisterBatch class is used to record the state updates for a batch or an offset.
     * It contains the updated in-flight state and the persister state batch to be sent to persister.
     */
    private record PersisterBatch(
        InFlightState updatedState,
        PersisterStateBatch stateBatch
    ) { }

    /**
     * OffsetAndMetadata class is used to record the last acknowledged offset post which the startOffset can be moved,
     * and the number of Terminal records removed out of the in-flight state if the startOffset is advanced.
     */
    // Visible for testing
    record OffsetAndMetadata(
        long lastAcknowledgedOffset,
        int numTerminalRecords
    ) { }

    /**
     * LastOffsetAndMaxRecords class is used to track the last offset to acquire and the maximum number
     * of records that can be acquired in a fetch request.
     */
    private record LastOffsetAndMaxRecords(
        long lastOffset,
        int maxRecords
    ) { }

    // Visibility for testing
    static Map<Byte, RecordState> ackTypeToRecordStateMapping() {
        return ACK_TYPE_TO_RECORD_STATE;
    }
}
