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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.MetadataSnapshot;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.CompressionRatioEstimator;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.CopyOnWriteMap;
import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.ProducerIdAndEpoch;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class acts as a queue that accumulates records into {@link MemoryRecords}
 * instances to be sent to the server.
 * <p>
 * The accumulator uses a bounded amount of memory and append calls will block when that memory is exhausted, unless
 * this behavior is explicitly disabled.
 */
public class RecordAccumulator {

    private final LogContext logContext;
    private final Logger log;
    private volatile boolean closed;
    private final AtomicInteger flushesInProgress;
    private final AtomicInteger appendsInProgress;
    private final int batchSize;
    private final Compression compression;
    private final int lingerMs;
    private final ExponentialBackoff retryBackoff;
    private final int deliveryTimeoutMs;
    private final long partitionAvailabilityTimeoutMs;  // latency threshold for marking partition temporary unavailable
    private final boolean enableAdaptivePartitioning;
    private final BufferPool free;
    private final Time time;
    private final ApiVersions apiVersions;
    private final ConcurrentMap<String /*topic*/, TopicInfo> topicInfoMap = new CopyOnWriteMap<>();
    private final ConcurrentMap<Integer /*nodeId*/, NodeLatencyStats> nodeStats = new CopyOnWriteMap<>();
    private final IncompleteBatches incomplete;
    // The following variables are only accessed by the sender thread, so we don't need to protect them.
    private final Set<TopicPartition> muted;
    private final Map<String, Integer> nodesDrainIndex;
    private final TransactionManager transactionManager;
    private long nextBatchExpiryTimeMs = Long.MAX_VALUE; // the earliest time (absolute) a batch will expire.

    /**
     * Create a new record accumulator
     *
     * @param logContext The log context used for logging
     * @param batchSize The size to use when allocating {@link MemoryRecords} instances
     * @param compression The compression codec for the records
     * @param lingerMs An artificial delay time to add before declaring a records instance that isn't full ready for
     *        sending. This allows time for more records to arrive. Setting a non-zero lingerMs will trade off some
     *        latency for potentially better throughput due to more batching (and hence fewer, larger requests).
     * @param retryBackoffMs An artificial delay time to retry the produce request upon receiving an error. This avoids
     *        exhausting all retries in a short period of time.
     * @param retryBackoffMaxMs The upper bound of the retry backoff time.
     * @param deliveryTimeoutMs An upper bound on the time to report success or failure on record delivery
     * @param partitionerConfig Partitioner config
     * @param metrics The metrics
     * @param metricGrpName The metric group name
     * @param time The time instance to use
     * @param apiVersions Request API versions for current connected brokers
     * @param transactionManager The shared transaction state object which tracks producer IDs, epochs, and sequence
     *                           numbers per partition.
     * @param bufferPool The buffer pool
     */
    public RecordAccumulator(LogContext logContext,
                             int batchSize,
                             Compression compression,
                             int lingerMs,
                             long retryBackoffMs,
                             long retryBackoffMaxMs,
                             int deliveryTimeoutMs,
                             PartitionerConfig partitionerConfig,
                             Metrics metrics,
                             String metricGrpName,
                             Time time,
                             ApiVersions apiVersions,
                             TransactionManager transactionManager,
                             BufferPool bufferPool) {
        this.logContext = logContext;
        this.log = logContext.logger(RecordAccumulator.class);
        this.closed = false;
        this.flushesInProgress = new AtomicInteger(0);
        this.appendsInProgress = new AtomicInteger(0);
        this.batchSize = batchSize;
        this.compression = compression;
        this.lingerMs = lingerMs;
        this.retryBackoff = new ExponentialBackoff(retryBackoffMs,
                CommonClientConfigs.RETRY_BACKOFF_EXP_BASE,
                retryBackoffMaxMs,
                CommonClientConfigs.RETRY_BACKOFF_JITTER);
        this.deliveryTimeoutMs = deliveryTimeoutMs;
        this.enableAdaptivePartitioning = partitionerConfig.enableAdaptivePartitioning;
        this.partitionAvailabilityTimeoutMs = partitionerConfig.partitionAvailabilityTimeoutMs;
        this.free = bufferPool;
        this.incomplete = new IncompleteBatches();
        this.muted = new HashSet<>();
        this.time = time;
        this.apiVersions = apiVersions;
        nodesDrainIndex = new HashMap<>();
        this.transactionManager = transactionManager;
        registerMetrics(metrics, metricGrpName);
    }

    /**
     * Create a new record accumulator with default partitioner config
     *
     * @param logContext The log context used for logging
     * @param batchSize The size to use when allocating {@link MemoryRecords} instances
     * @param compression The compression codec for the records
     * @param lingerMs An artificial delay time to add before declaring a records instance that isn't full ready for
     *        sending. This allows time for more records to arrive. Setting a non-zero lingerMs will trade off some
     *        latency for potentially better throughput due to more batching (and hence fewer, larger requests).
     * @param retryBackoffMs An artificial delay time to retry the produce request upon receiving an error. This avoids
     *        exhausting all retries in a short period of time.
     * @param retryBackoffMaxMs The upper bound of the retry backoff time.
     * @param deliveryTimeoutMs An upper bound on the time to report success or failure on record delivery
     * @param metrics The metrics
     * @param metricGrpName The metric group name
     * @param time The time instance to use
     * @param apiVersions Request API versions for current connected brokers
     * @param transactionManager The shared transaction state object which tracks producer IDs, epochs, and sequence
     *                           numbers per partition.
     * @param bufferPool The buffer pool
     */
    public RecordAccumulator(LogContext logContext,
                             int batchSize,
                             Compression compression,
                             int lingerMs,
                             long retryBackoffMs,
                             long retryBackoffMaxMs,
                             int deliveryTimeoutMs,
                             Metrics metrics,
                             String metricGrpName,
                             Time time,
                             ApiVersions apiVersions,
                             TransactionManager transactionManager,
                             BufferPool bufferPool) {
        this(logContext,
            batchSize,
            compression,
            lingerMs,
            retryBackoffMs,
            retryBackoffMaxMs,
            deliveryTimeoutMs,
            new PartitionerConfig(),
            metrics,
            metricGrpName,
            time,
            apiVersions,
            transactionManager,
            bufferPool);
    }

    private void registerMetrics(Metrics metrics, String metricGrpName) {
        metrics.addMetric(
            metrics.metricName("waiting-threads", metricGrpName,
                "The number of user threads blocked waiting for buffer memory to enqueue their records"),
            (config, now) -> free.queued());

        metrics.addMetric(
            metrics.metricName("buffer-total-bytes", metricGrpName,
                "The maximum amount of buffer memory the client can use (whether or not it is currently used)."),
            (config, now) -> free.totalMemory());

        metrics.addMetric(
            metrics.metricName("buffer-available-bytes", metricGrpName,
                "The total amount of buffer memory that is not being used (either unallocated or in the free list)."),
            (config, now) -> free.availableMemory());
    }

    private void setPartition(AppendCallbacks callbacks, int partition) {
        if (callbacks != null)
            callbacks.setPartition(partition);
    }

    /**
     * Check if partition concurrently changed, or we need to complete previously disabled partition change.
     *
     * @param topic The topic
     * @param topicInfo The topic info
     * @param partitionInfo The built-in partitioner's partition info
     * @param deque The partition queue
     * @param nowMs The current time, in milliseconds
     * @param cluster THe cluster metadata
     * @return 'true' if partition changed and we need to get new partition info and retry,
     *         'false' otherwise
     */
    private boolean partitionChanged(String topic,
                                     TopicInfo topicInfo,
                                     BuiltInPartitioner.StickyPartitionInfo partitionInfo,
                                     Deque<ProducerBatch> deque, long nowMs,
                                     Cluster cluster) {
        if (topicInfo.builtInPartitioner.isPartitionChanged(partitionInfo)) {
            log.trace("Partition {} for topic {} switched by a concurrent append, retrying",
                    partitionInfo.partition(), topic);
            return true;
        }

        // We might have disabled partition switch if the queue had incomplete batches.
        // Check if all batches are full now and switch .
        if (allBatchesFull(deque)) {
            topicInfo.builtInPartitioner.updatePartitionInfo(partitionInfo, 0, cluster, true);
            if (topicInfo.builtInPartitioner.isPartitionChanged(partitionInfo)) {
                log.trace("Completed previously disabled switch for topic {} partition {}, retrying",
                        topic, partitionInfo.partition());
                return true;
            }
        }

        return false;
    }

    /**
     * Add a record to the accumulator, return the append result
     * <p>
     * The append result will contain the future metadata, and flag for whether the appended batch is full or a new batch is created
     * <p>
     *
     * @param topic The topic to which this record is being sent
     * @param partition The partition to which this record is being sent or RecordMetadata.UNKNOWN_PARTITION
     *                  if any partition could be used
     * @param timestamp The timestamp of the record
     * @param key The key for the record
     * @param value The value for the record
     * @param headers the Headers for the record
     * @param callbacks The callbacks to execute
     * @param maxTimeToBlock The maximum time in milliseconds to block for buffer memory to be available
     * @param abortOnNewBatch A boolean that indicates returning before a new batch is created and
     *                        running the partitioner's onNewBatch method before trying to append again
     * @param nowMs The current time, in milliseconds
     * @param cluster The cluster metadata
     */
    public RecordAppendResult append(String topic,
                                     int partition,
                                     long timestamp,
                                     byte[] key,
                                     byte[] value,
                                     Header[] headers,
                                     AppendCallbacks callbacks,
                                     long maxTimeToBlock,
                                     boolean abortOnNewBatch,
                                     long nowMs,
                                     Cluster cluster) throws InterruptedException {
        TopicInfo topicInfo = topicInfoMap.computeIfAbsent(topic, k -> new TopicInfo(createBuiltInPartitioner(logContext, k, batchSize)));

        // We keep track of the number of appending thread to make sure we do not miss batches in
        // abortIncompleteBatches().
        appendsInProgress.incrementAndGet();
        ByteBuffer buffer = null;
        if (headers == null) headers = Record.EMPTY_HEADERS;
        try {
            // Loop to retry in case we encounter partitioner's race conditions.
            while (true) {
                // If the message doesn't have any partition affinity, so we pick a partition based on the broker
                // availability and performance.  Note, that here we peek current partition before we hold the
                // deque lock, so we'll need to make sure that it's not changed while we were waiting for the
                // deque lock.
                final BuiltInPartitioner.StickyPartitionInfo partitionInfo;
                final int effectivePartition;
                if (partition == RecordMetadata.UNKNOWN_PARTITION) {
                    partitionInfo = topicInfo.builtInPartitioner.peekCurrentPartitionInfo(cluster);
                    effectivePartition = partitionInfo.partition();
                } else {
                    partitionInfo = null;
                    effectivePartition = partition;
                }

                // Now that we know the effective partition, let the caller know.
                setPartition(callbacks, effectivePartition);

                // check if we have an in-progress batch
                Deque<ProducerBatch> dq = topicInfo.batches.computeIfAbsent(effectivePartition, k -> new ArrayDeque<>());
                synchronized (dq) {
                    // After taking the lock, validate that the partition hasn't changed and retry.
                    if (partitionChanged(topic, topicInfo, partitionInfo, dq, nowMs, cluster))
                        continue;

                    RecordAppendResult appendResult = tryAppend(timestamp, key, value, headers, callbacks, dq, nowMs);
                    if (appendResult != null) {
                        // If queue has incomplete batches we disable switch (see comments in updatePartitionInfo).
                        boolean enableSwitch = allBatchesFull(dq);
                        topicInfo.builtInPartitioner.updatePartitionInfo(partitionInfo, appendResult.appendedBytes, cluster, enableSwitch);
                        return appendResult;
                    }
                }

                // we don't have an in-progress record batch try to allocate a new batch
                if (abortOnNewBatch) {
                    // Return a result that will cause another call to append.
                    return new RecordAppendResult(null, false, false, true, 0);
                }

                if (buffer == null) {
                    int size = Math.max(this.batchSize, AbstractRecords.estimateSizeInBytesUpperBound(
                            RecordBatch.CURRENT_MAGIC_VALUE, compression.type(), key, value, headers));
                    log.trace("Allocating a new {} byte message buffer for topic {} partition {} with remaining timeout {}ms", size, topic, effectivePartition, maxTimeToBlock);
                    // This call may block if we exhausted buffer space.
                    buffer = free.allocate(size, maxTimeToBlock);
                    // Update the current time in case the buffer allocation blocked above.
                    // NOTE: getting time may be expensive, so calling it under a lock
                    // should be avoided.
                    nowMs = time.milliseconds();
                }

                synchronized (dq) {
                    // After taking the lock, validate that the partition hasn't changed and retry.
                    if (partitionChanged(topic, topicInfo, partitionInfo, dq, nowMs, cluster))
                        continue;

                    RecordAppendResult appendResult = appendNewBatch(topic, effectivePartition, dq, timestamp, key, value, headers, callbacks, buffer, nowMs);
                    // Set buffer to null, so that deallocate doesn't return it back to free pool, since it's used in the batch.
                    if (appendResult.newBatchCreated)
                        buffer = null;
                    // If queue has incomplete batches we disable switch (see comments in updatePartitionInfo).
                    boolean enableSwitch = allBatchesFull(dq);
                    topicInfo.builtInPartitioner.updatePartitionInfo(partitionInfo, appendResult.appendedBytes, cluster, enableSwitch);
                    return appendResult;
                }
            }
        } finally {
            free.deallocate(buffer);
            appendsInProgress.decrementAndGet();
        }
    }

    /**
     * Append a new batch to the queue
     *
     * @param topic The topic
     * @param partition The partition (cannot be RecordMetadata.UNKNOWN_PARTITION)
     * @param dq The queue
     * @param timestamp The timestamp of the record
     * @param key The key for the record
     * @param value The value for the record
     * @param headers the Headers for the record
     * @param callbacks The callbacks to execute
     * @param buffer The buffer for the new batch
     * @param nowMs The current time, in milliseconds
     */
    private RecordAppendResult appendNewBatch(String topic,
                                              int partition,
                                              Deque<ProducerBatch> dq,
                                              long timestamp,
                                              byte[] key,
                                              byte[] value,
                                              Header[] headers,
                                              AppendCallbacks callbacks,
                                              ByteBuffer buffer,
                                              long nowMs) {
        assert partition != RecordMetadata.UNKNOWN_PARTITION;

        RecordAppendResult appendResult = tryAppend(timestamp, key, value, headers, callbacks, dq, nowMs);
        if (appendResult != null) {
            // Somebody else found us a batch, return the one we waited for! Hopefully this doesn't happen often...
            return appendResult;
        }

        MemoryRecordsBuilder recordsBuilder = recordsBuilder(buffer);
        ProducerBatch batch = new ProducerBatch(new TopicPartition(topic, partition), recordsBuilder, nowMs);
        FutureRecordMetadata future = Objects.requireNonNull(batch.tryAppend(timestamp, key, value, headers,
                callbacks, nowMs));

        dq.addLast(batch);
        incomplete.add(batch);

        return new RecordAppendResult(future, dq.size() > 1 || batch.isFull(), true, false, batch.estimatedSizeInBytes());
    }

    private MemoryRecordsBuilder recordsBuilder(ByteBuffer buffer) {
        return MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, compression, TimestampType.CREATE_TIME, 0L);
    }

    /**
     * Check if all batches in the queue are full.
     */
    private boolean allBatchesFull(Deque<ProducerBatch> deque) {
        // Only the last batch may be incomplete, so we just check that.
        ProducerBatch last = deque.peekLast();
        return last == null || last.isFull();
    }

     /**
     *  Try to append to a ProducerBatch.
     *
     *  If it is full, we return null and a new batch is created. We also close the batch for record appends to free up
     *  resources like compression buffers. The batch will be fully closed (ie. the record batch headers will be written
     *  and memory records built) in one of the following cases (whichever comes first): right before send,
     *  if it is expired, or when the producer is closed.
     */
    private RecordAppendResult tryAppend(long timestamp, byte[] key, byte[] value, Header[] headers,
                                         Callback callback, Deque<ProducerBatch> deque, long nowMs) {
        if (closed)
            throw new KafkaException("Producer closed while send in progress");
        ProducerBatch last = deque.peekLast();
        if (last != null) {
            int initialBytes = last.estimatedSizeInBytes();
            FutureRecordMetadata future = last.tryAppend(timestamp, key, value, headers, callback, nowMs);
            if (future == null) {
                last.closeForRecordAppends();
            } else {
                int appendedBytes = last.estimatedSizeInBytes() - initialBytes;
                return new RecordAppendResult(future, deque.size() > 1 || last.isFull(), false, false, appendedBytes);
            }
        }
        return null;
    }

    private boolean isMuted(TopicPartition tp) {
        return muted.contains(tp);
    }

    public void resetNextBatchExpiryTime() {
        nextBatchExpiryTimeMs = Long.MAX_VALUE;
    }

    public void maybeUpdateNextBatchExpiryTime(ProducerBatch batch) {
        if (batch.createdMs + deliveryTimeoutMs  > 0) {
            // the non-negative check is to guard us against potential overflow due to setting
            // a large value for deliveryTimeoutMs
            nextBatchExpiryTimeMs = Math.min(nextBatchExpiryTimeMs, batch.createdMs + deliveryTimeoutMs);
        } else {
            log.warn("Skipping next batch expiry time update due to addition overflow: "
                + "batch.createMs={}, deliveryTimeoutMs={}", batch.createdMs, deliveryTimeoutMs);
        }
    }

    /**
     * Get a list of batches which have been sitting in the accumulator too long and need to be expired.
     */
    public List<ProducerBatch> expiredBatches(long now) {
        List<ProducerBatch> expiredBatches = new ArrayList<>();
        for (TopicInfo topicInfo : topicInfoMap.values()) {
            for (Deque<ProducerBatch> deque : topicInfo.batches.values()) {
                // expire the batches in the order of sending
                synchronized (deque) {
                    while (!deque.isEmpty()) {
                        ProducerBatch batch = deque.getFirst();
                        if (batch.hasReachedDeliveryTimeout(deliveryTimeoutMs, now)) {
                            deque.poll();
                            batch.abortRecordAppends();
                            expiredBatches.add(batch);
                        } else {
                            maybeUpdateNextBatchExpiryTime(batch);
                            break;
                        }
                    }
                }
            }
        }
        return expiredBatches;
    }

    public long getDeliveryTimeoutMs() {
        return deliveryTimeoutMs;
    }

    /**
     * Re-enqueue the given record batch in the accumulator. In Sender.completeBatch method, we check
     * whether the batch has reached deliveryTimeoutMs or not. Hence we do not do the delivery timeout check here.
     */
    public void reenqueue(ProducerBatch batch, long now) {
        batch.reenqueued(now);
        Deque<ProducerBatch> deque = getOrCreateDeque(batch.topicPartition);
        synchronized (deque) {
            if (transactionManager != null)
                insertInSequenceOrder(deque, batch);
            else
                deque.addFirst(batch);
        }
    }

    /**
     * Split the big batch that has been rejected and reenqueue the split batches in to the accumulator.
     * @return the number of split batches.
     */
    public int splitAndReenqueue(ProducerBatch bigBatch) {
        // Reset the estimated compression ratio to the initial value or the big batch compression ratio, whichever
        // is bigger. There are several different ways to do the reset. We chose the most conservative one to ensure
        // the split doesn't happen too often.
        CompressionRatioEstimator.setEstimation(bigBatch.topicPartition.topic(), compression.type(),
                                                Math.max(1.0f, (float) bigBatch.compressionRatio()));
        Deque<ProducerBatch> dq = bigBatch.split(this.batchSize);
        int numSplitBatches = dq.size();
        Deque<ProducerBatch> partitionDequeue = getOrCreateDeque(bigBatch.topicPartition);
        while (!dq.isEmpty()) {
            ProducerBatch batch = dq.pollLast();
            incomplete.add(batch);
            // We treat the newly split batches as if they are not even tried.
            synchronized (partitionDequeue) {
                if (transactionManager != null) {
                    // We should track the newly created batches since they already have assigned sequences.
                    transactionManager.addInFlightBatch(batch);
                    insertInSequenceOrder(partitionDequeue, batch);
                } else {
                    partitionDequeue.addFirst(batch);
                }
            }
        }
        return numSplitBatches;
    }

    // We will have to do extra work to ensure the queue is in order when requests are being retried and there are
    // multiple requests in flight to that partition. If the first in flight request fails to append, then all the
    // subsequent in flight requests will also fail because the sequence numbers will not be accepted.
    //
    // Further, once batches are being retried, we are reduced to a single in flight request for that partition. So when
    // the subsequent batches come back in sequence order, they will have to be placed further back in the queue.
    //
    // Note that this assumes that all the batches in the queue which have an assigned sequence also have the current
    // producer id. We will not attempt to reorder messages if the producer id has changed, we will throw an
    // IllegalStateException instead.
    private void insertInSequenceOrder(Deque<ProducerBatch> deque, ProducerBatch batch) {
        // When we are re-enqueueing and have enabled idempotence, the re-enqueued batch must always have a sequence.
        if (batch.baseSequence() == RecordBatch.NO_SEQUENCE)
            throw new IllegalStateException("Trying to re-enqueue a batch which doesn't have a sequence even " +
                "though idempotency is enabled.");

        if (!transactionManager.hasInflightBatches(batch.topicPartition))
            throw new IllegalStateException("We are re-enqueueing a batch which is not tracked as part of the in flight " +
                "requests. batch.topicPartition: " + batch.topicPartition + "; batch.baseSequence: " + batch.baseSequence());

        ProducerBatch firstBatchInQueue = deque.peekFirst();
        if (firstBatchInQueue != null && firstBatchInQueue.hasSequence() && firstBatchInQueue.baseSequence() < batch.baseSequence()) {
            // The incoming batch can't be inserted at the front of the queue without violating the sequence ordering.
            // This means that the incoming batch should be placed somewhere further back.
            // We need to find the right place for the incoming batch and insert it there.
            // We will only enter this branch if we have multiple inflights sent to different brokers and we need to retry
            // the inflight batches.
            //
            // Since we reenqueue exactly one batch a time and ensure that the queue is ordered by sequence always, it
            // is a simple linear scan of a subset of the in flight batches to find the right place in the queue each time.
            List<ProducerBatch> orderedBatches = new ArrayList<>();
            while (deque.peekFirst() != null && deque.peekFirst().hasSequence() && deque.peekFirst().baseSequence() < batch.baseSequence())
                orderedBatches.add(deque.pollFirst());

            log.debug("Reordered incoming batch with sequence {} for partition {}. It was placed in the queue at " +
                "position {}", batch.baseSequence(), batch.topicPartition, orderedBatches.size());
            // Either we have reached a point where there are batches without a sequence (ie. never been drained
            // and are hence in order by default), or the batch at the front of the queue has a sequence greater
            // than the incoming batch. This is the right place to add the incoming batch.
            deque.addFirst(batch);

            // Now we have to re insert the previously queued batches in the right order.
            for (int i = orderedBatches.size() - 1; i >= 0; --i) {
                deque.addFirst(orderedBatches.get(i));
            }

            // At this point, the incoming batch has been queued in the correct place according to its sequence.
        } else {
            deque.addFirst(batch);
        }
    }

    /**
     * Add the leader to the ready nodes if the batch is ready
     *
     * @param exhausted 'true' is the buffer pool is exhausted
     * @param part The partition
     * @param leader The leader for the partition
     * @param waitedTimeMs How long batch waited
     * @param backingOff Is backing off
     * @param backoffAttempts Number of attempts for calculating backoff delay
     * @param full Is batch full
     * @param nextReadyCheckDelayMs The delay for next check
     * @param readyNodes The set of ready nodes (to be filled in)
     * @return The delay for next check
     */
    private long batchReady(boolean exhausted, TopicPartition part, Node leader,
                            long waitedTimeMs, boolean backingOff, int backoffAttempts,
                            boolean full, long nextReadyCheckDelayMs, Set<Node> readyNodes) {
        if (!readyNodes.contains(leader) && !isMuted(part)) {
            long timeToWaitMs = backingOff ? retryBackoff.backoff(backoffAttempts > 0 ? backoffAttempts - 1 : 0) : lingerMs;
            boolean expired = waitedTimeMs >= timeToWaitMs;
            boolean transactionCompleting = transactionManager != null && transactionManager.isCompleting();
            boolean sendable = full
                    || expired
                    || exhausted
                    || closed
                    || flushInProgress()
                    || transactionCompleting;
            if (sendable && !backingOff) {
                readyNodes.add(leader);
            } else {
                long timeLeftMs = Math.max(timeToWaitMs - waitedTimeMs, 0);
                // Note that this results in a conservative estimate since an un-sendable partition may have
                // a leader that will later be found to have sendable data. However, this is good enough
                // since we'll just wake up and then sleep again for the remaining time.
                nextReadyCheckDelayMs = Math.min(timeLeftMs, nextReadyCheckDelayMs);
            }
        }
        return nextReadyCheckDelayMs;
    }

    /**
     * Iterate over partitions to see which one have batches ready and collect leaders of those
     * partitions into the set of ready nodes.  If partition has no leader, add the topic to the set
     * of topics with no leader.  This function also calculates stats for adaptive partitioning.
     *
     * @param metadataSnapshot      The cluster metadata
     * @param nowMs                 The current time
     * @param topic                 The topic
     * @param topicInfo             The topic info
     * @param nextReadyCheckDelayMs The delay for next check
     * @param readyNodes            The set of ready nodes (to be filled in)
     * @param unknownLeaderTopics   The set of topics with no leader (to be filled in)
     * @return The delay for next check
     */
    private long partitionReady(MetadataSnapshot metadataSnapshot, long nowMs, String topic,
                                TopicInfo topicInfo,
                                long nextReadyCheckDelayMs, Set<Node> readyNodes, Set<String> unknownLeaderTopics) {
        ConcurrentMap<Integer, Deque<ProducerBatch>> batches = topicInfo.batches;
        // Collect the queue sizes for available partitions to be used in adaptive partitioning.
        int[] queueSizes = null;
        int[] partitionIds = null;
        if (enableAdaptivePartitioning && batches.size() >= metadataSnapshot.cluster().partitionsForTopic(topic).size()) {
            // We don't do adaptive partitioning until we scheduled at least a batch for all
            // partitions (i.e. we have the corresponding entries in the batches map), we just
            // do uniform.  The reason is that we build queue sizes from the batches map,
            // and if an entry is missing in the batches map, then adaptive partitioning logic
            // won't know about it and won't switch to it.
            queueSizes = new int[batches.size()];
            partitionIds = new int[queueSizes.length];
        }

        int queueSizesIndex = -1;
        boolean exhausted = this.free.queued() > 0;
        for (Map.Entry<Integer, Deque<ProducerBatch>> entry : batches.entrySet()) {
            TopicPartition part = new TopicPartition(topic, entry.getKey());
            // Advance queueSizesIndex so that we properly index available
            // partitions.  Do it here so that it's done for all code paths.

            Node leader = metadataSnapshot.cluster().leaderFor(part);
            if (leader != null && queueSizes != null) {
                ++queueSizesIndex;
                assert queueSizesIndex < queueSizes.length;
                partitionIds[queueSizesIndex] = part.partition();
            }

            Deque<ProducerBatch> deque = entry.getValue();

            final long waitedTimeMs;
            final boolean backingOff;
            final int backoffAttempts;
            final int dequeSize;
            final boolean full;

            OptionalInt leaderEpoch = metadataSnapshot.leaderEpochFor(part);

            // This loop is especially hot with large partition counts. So -

            // 1. We should avoid code that increases synchronization between application thread calling
            // send(), and background thread running runOnce(), see https://issues.apache.org/jira/browse/KAFKA-16226

            // 2. We are careful to only perform the minimum required inside the
            // synchronized block, as this lock is also used to synchronize producer threads
            // attempting to append() to a partition/batch.

            synchronized (deque) {
                // Deques are often empty in this path, esp with large partition counts,
                // so we exit early if we can.
                ProducerBatch batch = deque.peekFirst();
                if (batch == null) {
                    continue;
                }

                waitedTimeMs = batch.waitedTimeMs(nowMs);
                batch.maybeUpdateLeaderEpoch(leaderEpoch);
                backingOff = shouldBackoff(batch.hasLeaderChangedForTheOngoingRetry(), batch, waitedTimeMs);
                backoffAttempts = batch.attempts();
                dequeSize = deque.size();
                full = dequeSize > 1 || batch.isFull();
            }

            if (leader == null) {
                // This is a partition for which leader is not known, but messages are available to send.
                // Note that entries are currently not removed from batches when deque is empty.
                unknownLeaderTopics.add(part.topic());
            } else {
                if (queueSizes != null)
                    queueSizes[queueSizesIndex] = dequeSize;
                if (partitionAvailabilityTimeoutMs > 0) {
                    // Check if we want to exclude the partition from the list of available partitions
                    // if the broker hasn't responded for some time.
                    NodeLatencyStats nodeLatencyStats = nodeStats.get(leader.id());
                    if (nodeLatencyStats != null) {
                        // NOTE: there is no synchronization between reading metrics,
                        // so we read ready time first to avoid accidentally marking partition
                        // unavailable if we read while the metrics are being updated.
                        long readyTimeMs = nodeLatencyStats.readyTimeMs;
                        if (readyTimeMs - nodeLatencyStats.drainTimeMs > partitionAvailabilityTimeoutMs)
                            --queueSizesIndex;
                    }
                }

                nextReadyCheckDelayMs = batchReady(exhausted, part, leader, waitedTimeMs, backingOff,
                    backoffAttempts, full, nextReadyCheckDelayMs, readyNodes);
            }
        }

        // We've collected the queue sizes for partitions of this topic, now we can calculate
        // load stats.  NOTE: the stats are calculated in place, modifying the
        // queueSizes array.
        topicInfo.builtInPartitioner.updatePartitionLoadStats(queueSizes, partitionIds, queueSizesIndex + 1);
        return nextReadyCheckDelayMs;
    }

    /**
     * Get a list of nodes whose partitions are ready to be sent, and the earliest time at which any non-sendable
     * partition will be ready; Also return the flag for whether there are any unknown leaders for the accumulated
     * partition batches.
     * <p>
     * A destination node is ready to send data if:
     * <ol>
     * <li>There is at least one partition that is not backing off its send
     * <li><b>and</b> those partitions are not muted (to prevent reordering if
     *   {@value org.apache.kafka.clients.producer.ProducerConfig#MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION}
     *   is set to one)</li>
     * <li><b>and <i>any</i></b> of the following are true</li>
     * <ul>
     *     <li>The record set is full</li>
     *     <li>The record set has sat in the accumulator for at least lingerMs milliseconds</li>
     *     <li>The accumulator is out of memory and threads are blocking waiting for data (in this case all partitions
     *     are immediately considered ready).</li>
     *     <li>The accumulator has been closed</li>
     * </ul>
     * </ol>
     */
    public ReadyCheckResult ready(MetadataSnapshot metadataSnapshot, long nowMs) {
        Set<Node> readyNodes = new HashSet<>();
        long nextReadyCheckDelayMs = Long.MAX_VALUE;
        Set<String> unknownLeaderTopics = new HashSet<>();
        // Go topic by topic so that we can get queue sizes for partitions in a topic and calculate
        // cumulative frequency table (used in partitioner).
        for (Map.Entry<String, TopicInfo> topicInfoEntry : this.topicInfoMap.entrySet()) {
            final String topic = topicInfoEntry.getKey();
            nextReadyCheckDelayMs = partitionReady(metadataSnapshot, nowMs, topic, topicInfoEntry.getValue(), nextReadyCheckDelayMs, readyNodes, unknownLeaderTopics);
        }
        return new ReadyCheckResult(readyNodes, nextReadyCheckDelayMs, unknownLeaderTopics);
    }

    /**
     * Check whether there are any batches which haven't been drained
     */
    public boolean hasUndrained() {
        for (TopicInfo topicInfo : topicInfoMap.values()) {
            for (Deque<ProducerBatch> deque : topicInfo.batches.values()) {
                synchronized (deque) {
                    if (!deque.isEmpty())
                        return true;
                }
            }
        }
        return false;
    }

    private boolean shouldBackoff(boolean hasLeaderChanged, final ProducerBatch batch, final long waitedTimeMs) {
        boolean shouldWaitMore = batch.attempts() > 0 && waitedTimeMs < retryBackoff.backoff(batch.attempts() - 1);
        boolean shouldBackoff = !hasLeaderChanged && shouldWaitMore;
        if (log.isTraceEnabled()) {
            if (shouldBackoff) {
                log.trace(
                    "For {}, will backoff", batch);
            } else {
                log.trace(
                    "For {}, will not backoff, shouldWaitMore {}, hasLeaderChanged {}", batch,
                    shouldWaitMore, hasLeaderChanged);
            }
        } else if (log.isDebugEnabled() && hasLeaderChanged) {
            // Add less-verbose log at DEBUG.
            log.debug("For {}, leader has changed, hence skipping backoff.", batch);
        }
        return shouldBackoff;
    }

    private boolean shouldStopDrainBatchesForPartition(ProducerBatch first, TopicPartition tp) {
        ProducerIdAndEpoch producerIdAndEpoch;
        if (transactionManager != null) {
            if (!transactionManager.isSendToPartitionAllowed(tp))
                return true;

            producerIdAndEpoch = transactionManager.producerIdAndEpoch();
            if (!producerIdAndEpoch.isValid())
                // we cannot send the batch until we have refreshed the producer id
                return true;

            if (!first.hasSequence()) {
                if (transactionManager.hasInflightBatches(tp) && transactionManager.hasStaleProducerIdAndEpoch(tp)) {
                    // Don't drain any new batches while the partition has in-flight batches with a different epoch
                    // and/or producer ID. Otherwise, a batch with a new epoch and sequence number
                    // 0 could be written before earlier batches complete, which would cause out of sequence errors
                    return true;
                }

                if (transactionManager.hasUnresolvedSequence(first.topicPartition))
                    // Don't drain any new batches while the state of previous sequence numbers
                    // is unknown. The previous batches would be unknown if they were aborted
                    // on the client after being sent to the broker at least once.
                    return true;
            }

            int firstInFlightSequence = transactionManager.firstInFlightSequence(first.topicPartition);
            // If the queued batch already has an assigned sequence, then it is being retried.
            // In this case, we wait until the next immediate batch is ready and drain that.
            // We only move on when the next in line batch is complete (either successfully or due to
            // a fatal broker error). This effectively reduces our in flight request count to 1.
            return firstInFlightSequence != RecordBatch.NO_SEQUENCE && first.hasSequence()
                    && first.baseSequence() != firstInFlightSequence;
        }
        return false;
    }

    private List<ProducerBatch> drainBatchesForOneNode(MetadataSnapshot metadataSnapshot, Node node, int maxSize, long now) {
        int size = 0;
        List<PartitionInfo> parts = metadataSnapshot.cluster().partitionsForNode(node.id());
        List<ProducerBatch> ready = new ArrayList<>();
        if (parts.isEmpty())
            return ready;
        /* to make starvation less likely each node has it's own drainIndex */
        int drainIndex = getDrainIndex(node.idString());
        int start = drainIndex = drainIndex % parts.size();
        do {
            PartitionInfo part = parts.get(drainIndex);

            TopicPartition tp = new TopicPartition(part.topic(), part.partition());
            updateDrainIndex(node.idString(), drainIndex);
            drainIndex = (drainIndex + 1) % parts.size();
            // Only proceed if the partition has no in-flight batches.
            if (isMuted(tp))
                continue;
            Deque<ProducerBatch> deque = getDeque(tp);
            if (deque == null)
                continue;

            OptionalInt leaderEpoch = metadataSnapshot.leaderEpochFor(tp);

            final ProducerBatch batch;
            synchronized (deque) {
                // invariant: !isMuted(tp,now) && deque != null
                ProducerBatch first = deque.peekFirst();
                if (first == null)
                    continue;

                // first != null
                // Only drain the batch if it is not during backoff period.
                first.maybeUpdateLeaderEpoch(leaderEpoch);
                if (shouldBackoff(first.hasLeaderChangedForTheOngoingRetry(), first, first.waitedTimeMs(now)))
                    continue;

                if (size + first.estimatedSizeInBytes() > maxSize && !ready.isEmpty()) {
                    // there is a rare case that a single batch size is larger than the request size due to
                    // compression; in this case we will still eventually send this batch in a single request
                    break;
                } else {
                    if (shouldStopDrainBatchesForPartition(first, tp))
                        break;
                }

                batch = deque.pollFirst();

                boolean isTransactional = transactionManager != null && transactionManager.isTransactional();
                ProducerIdAndEpoch producerIdAndEpoch =
                    transactionManager != null ? transactionManager.producerIdAndEpoch() : null;
                if (producerIdAndEpoch != null && !batch.hasSequence()) {
                    // If the producer id/epoch of the partition do not match the latest one
                    // of the producer, we update it and reset the sequence. This should be
                    // only done when all its in-flight batches have completed. This is guarantee
                    // in `shouldStopDrainBatchesForPartition`.
                    transactionManager.maybeUpdateProducerIdAndEpoch(batch.topicPartition);

                    // If the batch already has an assigned sequence, then we should not change the producer id and
                    // sequence number, since this may introduce duplicates. In particular, the previous attempt
                    // may actually have been accepted, and if we change the producer id and sequence here, this
                    // attempt will also be accepted, causing a duplicate.
                    //
                    // Additionally, we update the next sequence number bound for the partition, and also have
                    // the transaction manager track the batch so as to ensure that sequence ordering is maintained
                    // even if we receive out of order responses.
                    batch.setProducerState(producerIdAndEpoch, transactionManager.sequenceNumber(batch.topicPartition), isTransactional);
                    transactionManager.incrementSequenceNumber(batch.topicPartition, batch.recordCount);
                    log.debug("Assigned producerId {} and producerEpoch {} to batch with base sequence " +
                            "{} being sent to partition {}", producerIdAndEpoch.producerId,
                        producerIdAndEpoch.epoch, batch.baseSequence(), tp);

                    transactionManager.addInFlightBatch(batch);
                }
            }

            // the rest of the work by processing outside the lock
            // close() is particularly expensive
            batch.close();
            size += batch.records().sizeInBytes();
            ready.add(batch);

            batch.drained(now);
        } while (start != drainIndex);
        return ready;
    }

    private int getDrainIndex(String idString) {
        return nodesDrainIndex.computeIfAbsent(idString, s -> 0);
    }

    private void updateDrainIndex(String idString, int drainIndex) {
        nodesDrainIndex.put(idString, drainIndex);
    }

    /**
     * Drain all the data for the given nodes and collate them into a list of batches that will fit
     * within the specified size on a per-node basis. This method attempts to avoid choosing the same
     * topic-node over and over.
     *
     * @param metadataSnapshot  The current cluster metadata
     * @param nodes             The list of node to drain
     * @param maxSize           The maximum number of bytes to drain
     * @param now               The current unix time in milliseconds
     * @return A list of {@link ProducerBatch} for each node specified with total size less than the
     * requested maxSize.
     */
    public Map<Integer, List<ProducerBatch>> drain(MetadataSnapshot metadataSnapshot, Set<Node> nodes, int maxSize, long now) {
        if (nodes.isEmpty())
            return Collections.emptyMap();

        Map<Integer, List<ProducerBatch>> batches = new HashMap<>();
        for (Node node : nodes) {
            List<ProducerBatch> ready = drainBatchesForOneNode(metadataSnapshot, node, maxSize, now);
            batches.put(node.id(), ready);
        }
        return batches;
    }

    public void updateNodeLatencyStats(Integer nodeId, long nowMs, boolean canDrain) {
        // Don't bother with updating stats if the feature is turned off.
        if (partitionAvailabilityTimeoutMs <= 0)
            return;

        // When the sender gets a node (returned by the ready() function) that has data to send
        // but the node is not ready (and so we cannot drain the data), we only update the
        // ready time, then the difference would reflect for how long a node wasn't ready
        // to send the data.  Then we can temporarily remove partitions that are handled by the
        // node from the list of available partitions so that the partitioner wouldn't pick
        // this partition.
        // NOTE: there is no synchronization for metric updates, so drainTimeMs is updated
        // first to avoid accidentally marking a partition unavailable if the reader gets
        // values between updates.
        NodeLatencyStats nodeLatencyStats = nodeStats.computeIfAbsent(nodeId, id -> new NodeLatencyStats(nowMs));
        if (canDrain)
            nodeLatencyStats.drainTimeMs = nowMs;
        nodeLatencyStats.readyTimeMs = nowMs;
    }

    /* Visible for testing */
    public NodeLatencyStats getNodeLatencyStats(Integer nodeId) {
        return nodeStats.get(nodeId);
    }

    /* Visible for testing */
    public BuiltInPartitioner getBuiltInPartitioner(String topic) {
        return topicInfoMap.get(topic).builtInPartitioner;
    }

    /**
     * The earliest absolute time a batch will expire (in milliseconds)
     */
    public long nextExpiryTimeMs() {
        return this.nextBatchExpiryTimeMs;
    }

      /* Visible for testing */
    public Deque<ProducerBatch> getDeque(TopicPartition tp) {
        TopicInfo topicInfo = topicInfoMap.get(tp.topic());
        if (topicInfo == null)
            return null;
        return topicInfo.batches.get(tp.partition());
    }

    /**
     * Get the deque for the given topic-partition, creating it if necessary.
     */
    private Deque<ProducerBatch> getOrCreateDeque(TopicPartition tp) {
        TopicInfo topicInfo = topicInfoMap.computeIfAbsent(tp.topic(),
                k -> new TopicInfo(createBuiltInPartitioner(logContext, k, batchSize)));
        return topicInfo.batches.computeIfAbsent(tp.partition(), k -> new ArrayDeque<>());
    }

    BuiltInPartitioner createBuiltInPartitioner(LogContext logContext, String topic, int stickyBatchSize) {
        return new BuiltInPartitioner(logContext, topic, stickyBatchSize);
    }

    /**
     * Deallocate the record batch
     */
    public void deallocate(ProducerBatch batch) {
        incomplete.remove(batch);
        // Only deallocate the batch if it is not a split batch because split batch are allocated outside the
        // buffer pool.
        if (!batch.isSplitBatch())
            free.deallocate(batch.buffer(), batch.initialCapacity());
    }

    /**
     * Package private for unit test. Get the buffer pool remaining size in bytes.
     */
    long bufferPoolAvailableMemory() {
        return free.availableMemory();
    }

    /**
     * Are there any threads currently waiting on a flush?
     *
     * package private for test
     */
    boolean flushInProgress() {
        return flushesInProgress.get() > 0;
    }

    /**
     * Initiate the flushing of data from the accumulator...this makes all requests immediately ready
     */
    public void beginFlush() {
        this.flushesInProgress.getAndIncrement();
    }

    /**
     * Are there any threads currently appending messages?
     */
    private boolean appendsInProgress() {
        return appendsInProgress.get() > 0;
    }

    /**
     * Mark all partitions as ready to send and block until the send is complete
     */
    public void awaitFlushCompletion() throws InterruptedException {
        try {
            // Obtain a copy of all of the incomplete ProduceRequestResult(s) at the time of the flush.
            // We must be careful not to hold a reference to the ProduceBatch(s) so that garbage
            // collection can occur on the contents.
            // The sender will remove ProducerBatch(s) from the original incomplete collection.
            for (ProduceRequestResult result : this.incomplete.requestResults())
                result.await();
        } finally {
            this.flushesInProgress.decrementAndGet();
        }
    }

    /**
     * Check whether there are any pending batches (whether sent or unsent).
     */
    public boolean hasIncomplete() {
        return !this.incomplete.isEmpty();
    }

    /**
     * This function is only called when sender is closed forcefully. It will fail all the
     * incomplete batches and return.
     */
    public void abortIncompleteBatches() {
        // We need to keep aborting the incomplete batch until no thread is trying to append to
        // 1. Avoid losing batches.
        // 2. Free up memory in case appending threads are blocked on buffer full.
        // This is a tight loop but should be able to get through very quickly.
        do {
            abortBatches();
        } while (appendsInProgress());
        // After this point, no thread will append any messages because they will see the close
        // flag set. We need to do the last abort after no thread was appending in case there was a new
        // batch appended by the last appending thread.
        abortBatches();
        this.topicInfoMap.clear();
    }

    /**
     * Go through incomplete batches and abort them.
     */
    private void abortBatches() {
        abortBatches(new KafkaException("Producer is closed forcefully."));
    }

    /**
     * Abort all incomplete batches (whether they have been sent or not)
     */
    void abortBatches(final RuntimeException reason) {
        for (ProducerBatch batch : incomplete.copyAll()) {
            Deque<ProducerBatch> dq = getDeque(batch.topicPartition);
            synchronized (dq) {
                batch.abortRecordAppends();
                dq.remove(batch);
            }
            batch.abort(reason);
            deallocate(batch);
        }
    }

    /**
     * Abort any batches which have not been drained
     */
    void abortUndrainedBatches(RuntimeException reason) {
        for (ProducerBatch batch : incomplete.copyAll()) {
            Deque<ProducerBatch> dq = getDeque(batch.topicPartition);
            boolean aborted = false;
            synchronized (dq) {
                if ((transactionManager != null && !batch.hasSequence()) || (transactionManager == null && !batch.isClosed())) {
                    aborted = true;
                    batch.abortRecordAppends();
                    dq.remove(batch);
                }
            }
            if (aborted) {
                batch.abort(reason);
                deallocate(batch);
            }
        }
    }

    public void mutePartition(TopicPartition tp) {
        muted.add(tp);
    }

    public void unmutePartition(TopicPartition tp) {
        muted.remove(tp);
    }

    /**
     * Close this accumulator and force all the record buffers to be drained
     */
    public void close() {
        this.closed = true;
        this.free.close();
    }

    /**
     * Partitioner config for built-in partitioner
     */
    public static final class PartitionerConfig {
        private final boolean enableAdaptivePartitioning;
        private final long partitionAvailabilityTimeoutMs;

        /**
         * Partitioner config
         *
         * @param enableAdaptivePartitioning If it's true, partition switching adapts to broker load, otherwise partition
         *        switching is random.
         * @param partitionAvailabilityTimeoutMs If a broker cannot process produce requests from a partition
         *        for the specified time, the partition is treated by the partitioner as not available.
         *        If the timeout is 0, this logic is disabled.
         */
        public PartitionerConfig(boolean enableAdaptivePartitioning, long partitionAvailabilityTimeoutMs) {
            this.enableAdaptivePartitioning = enableAdaptivePartitioning;
            this.partitionAvailabilityTimeoutMs = partitionAvailabilityTimeoutMs;
        }

        public PartitionerConfig() {
            this(false, 0);
        }
    }

    /*
     * Metadata about a record just appended to the record accumulator
     */
    public static final class RecordAppendResult {
        public final FutureRecordMetadata future;
        public final boolean batchIsFull;
        public final boolean newBatchCreated;
        public final boolean abortForNewBatch;
        public final int appendedBytes;

        public RecordAppendResult(FutureRecordMetadata future,
                                  boolean batchIsFull,
                                  boolean newBatchCreated,
                                  boolean abortForNewBatch,
                                  int appendedBytes) {
            this.future = future;
            this.batchIsFull = batchIsFull;
            this.newBatchCreated = newBatchCreated;
            this.abortForNewBatch = abortForNewBatch;
            this.appendedBytes = appendedBytes;
        }
    }

    /*
     * The callbacks passed into append
     */
    public interface AppendCallbacks extends Callback {
        /**
         * Called to set partition (when append is called, partition may not be calculated yet).
         * @param partition The partition
         */
        void setPartition(int partition);
    }

    /*
     * The set of nodes that have at least one complete record batch in the accumulator
     */
    public static final class ReadyCheckResult {
        public final Set<Node> readyNodes;
        public final long nextReadyCheckDelayMs;
        public final Set<String> unknownLeaderTopics;

        public ReadyCheckResult(Set<Node> readyNodes, long nextReadyCheckDelayMs, Set<String> unknownLeaderTopics) {
            this.readyNodes = readyNodes;
            this.nextReadyCheckDelayMs = nextReadyCheckDelayMs;
            this.unknownLeaderTopics = unknownLeaderTopics;
        }
    }

    /**
     * Per topic info.
     */
    private static class TopicInfo {
        public final ConcurrentMap<Integer /*partition*/, Deque<ProducerBatch>> batches = new CopyOnWriteMap<>();
        public final BuiltInPartitioner builtInPartitioner;

        public TopicInfo(BuiltInPartitioner builtInPartitioner) {
            this.builtInPartitioner = builtInPartitioner;
        }
    }

    /**
     * Node latency stats for each node that are used for adaptive partition distribution
     * Visible for testing
     */
    public static final class NodeLatencyStats {
        public volatile long readyTimeMs;  // last time the node had batches ready to send
        public volatile long drainTimeMs;  // last time the node was able to drain batches

        NodeLatencyStats(long nowMs) {
            readyTimeMs = nowMs;
            drainTimeMs = nowMs;
        }
    }
}
