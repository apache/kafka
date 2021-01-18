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
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.common.utils.ProducerIdAndEpoch;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.CompressionRatioEstimator;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.CopyOnWriteMap;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

/**
 * This class acts as a queue that accumulates records into {@link MemoryRecords}
 * instances to be sent to the server.
 * <p>
 * The accumulator uses a bounded amount of memory and append calls will block when that memory is exhausted, unless
 * this behavior is explicitly disabled.
 */
public final class RecordAccumulator {

    private final Logger log;
    private volatile boolean closed;
    private final AtomicInteger flushesInProgress;
    private final AtomicInteger appendsInProgress;
    private final int batchSize;
    private final CompressionType compression;
    private final int lingerMs;
    private final long retryBackoffMs;
    private final int deliveryTimeoutMs;
    private final BufferPool free;
    private final Time time;
    private final ApiVersions apiVersions;
    private final ConcurrentMap<TopicPartition, Deque<ProducerBatch>> batches;
    private final IncompleteBatches incomplete;
    // The following variables are only accessed by the sender thread, so we don't need to protect them.
    private final Set<TopicPartition> muted;
    private int drainIndex;
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
     * @param metrics The metrics
     * @param time The time instance to use
     * @param apiVersions Request API versions for current connected brokers
     * @param transactionManager The shared transaction state object which tracks producer IDs, epochs, and sequence
     *                           numbers per partition.
     */
    public RecordAccumulator(LogContext logContext,
                             int batchSize,
                             CompressionType compression,
                             int lingerMs,
                             long retryBackoffMs,
                             int deliveryTimeoutMs,
                             Metrics metrics,
                             String metricGrpName,
                             Time time,
                             ApiVersions apiVersions,
                             TransactionManager transactionManager,
                             BufferPool bufferPool) {
        this.log = logContext.logger(RecordAccumulator.class);
        this.drainIndex = 0;
        this.closed = false;
        this.flushesInProgress = new AtomicInteger(0);
        this.appendsInProgress = new AtomicInteger(0);
        this.batchSize = batchSize;
        this.compression = compression;
        this.lingerMs = lingerMs;
        this.retryBackoffMs = retryBackoffMs;
        this.deliveryTimeoutMs = deliveryTimeoutMs;
        this.batches = new CopyOnWriteMap<>();
        this.free = bufferPool;
        this.incomplete = new IncompleteBatches();
        this.muted = new HashSet<>();
        this.time = time;
        this.apiVersions = apiVersions;
        this.transactionManager = transactionManager;
        registerMetrics(metrics, metricGrpName);
    }

    private void registerMetrics(Metrics metrics, String metricGrpName) {
        MetricName metricName = metrics.metricName("waiting-threads", metricGrpName, "The number of user threads blocked waiting for buffer memory to enqueue their records");
        Measurable waitingThreads = new Measurable() {
            public double measure(MetricConfig config, long now) {
                return free.queued();
            }
        };
        metrics.addMetric(metricName, waitingThreads);

        metricName = metrics.metricName("buffer-total-bytes", metricGrpName, "The maximum amount of buffer memory the client can use (whether or not it is currently used).");
        Measurable totalBytes = new Measurable() {
            public double measure(MetricConfig config, long now) {
                return free.totalMemory();
            }
        };
        metrics.addMetric(metricName, totalBytes);

        metricName = metrics.metricName("buffer-available-bytes", metricGrpName, "The total amount of buffer memory that is not being used (either unallocated or in the free list).");
        Measurable availableBytes = new Measurable() {
            public double measure(MetricConfig config, long now) {
                return free.availableMemory();
            }
        };
        metrics.addMetric(metricName, availableBytes);
    }

    /**
     * Add a record to the accumulator, return the append result
     * <p>
     * The append result will contain the future metadata, and flag for whether the appended batch is full or a new batch is created
     * <p>
     *
     * @param tp The topic/partition to which this record is being sent
     * @param timestamp The timestamp of the record
     * @param key The key for the record
     * @param value The value for the record
     * @param headers the Headers for the record
     * @param callback The user-supplied callback to execute when the request is complete
     * @param maxTimeToBlock The maximum time in milliseconds to block for buffer memory to be available
     * @param abortOnNewBatch A boolean that indicates returning before a new batch is created and
     *                        running the partitioner's onNewBatch method before trying to append again
     * @param nowMs The current time, in milliseconds
     */
    public RecordAppendResult append(TopicPartition tp,
                                     long timestamp,
                                     byte[] key,
                                     byte[] value,
                                     Header[] headers,
                                     Callback callback,
                                     long maxTimeToBlock,
                                     boolean abortOnNewBatch,
                                     long nowMs) throws InterruptedException {
        // We keep track of the number of appending thread to make sure we do not miss batches in
        // abortIncompleteBatches().
        appendsInProgress.incrementAndGet();
        ByteBuffer buffer = null;
        if (headers == null) headers = Record.EMPTY_HEADERS;
        try {
            // check if we have an in-progress batch
            Deque<ProducerBatch> dq = getOrCreateDeque(tp);
            synchronized (dq) {
                if (closed)
                    throw new KafkaException("Producer closed while send in progress");
                RecordAppendResult appendResult = tryAppend(timestamp, key, value, headers, callback, dq, nowMs);
                if (appendResult != null)
                    return appendResult;
            }

            // we don't have an in-progress record batch try to allocate a new batch
            if (abortOnNewBatch) {
                // Return a result that will cause another call to append.
                return new RecordAppendResult(null, false, false, true);
            }

            byte maxUsableMagic = apiVersions.maxUsableProduceMagic();
            int size = Math.max(this.batchSize, AbstractRecords.estimateSizeInBytesUpperBound(maxUsableMagic, compression, key, value, headers));
            log.trace("Allocating a new {} byte message buffer for topic {} partition {} with remaining timeout {}ms", size, tp.topic(), tp.partition(), maxTimeToBlock);
            buffer = free.allocate(size, maxTimeToBlock);

            // Update the current time in case the buffer allocation blocked above.
            nowMs = time.milliseconds();
            synchronized (dq) {
                // Need to check if producer is closed again after grabbing the dequeue lock.
                if (closed)
                    throw new KafkaException("Producer closed while send in progress");

                RecordAppendResult appendResult = tryAppend(timestamp, key, value, headers, callback, dq, nowMs);
                if (appendResult != null) {
                    // Somebody else found us a batch, return the one we waited for! Hopefully this doesn't happen often...
                    return appendResult;
                }

                MemoryRecordsBuilder recordsBuilder = recordsBuilder(buffer, maxUsableMagic);
                ProducerBatch batch = new ProducerBatch(tp, recordsBuilder, nowMs);
                FutureRecordMetadata future = Objects.requireNonNull(batch.tryAppend(timestamp, key, value, headers,
                        callback, nowMs));

                dq.addLast(batch);
                incomplete.add(batch);

                // Don't deallocate this buffer in the finally block as it's being used in the record batch
                buffer = null;
                return new RecordAppendResult(future, dq.size() > 1 || batch.isFull(), true, false);
            }
        } finally {
            if (buffer != null)
                free.deallocate(buffer);
            appendsInProgress.decrementAndGet();
        }
    }

    private MemoryRecordsBuilder recordsBuilder(ByteBuffer buffer, byte maxUsableMagic) {
        if (transactionManager != null && maxUsableMagic < RecordBatch.MAGIC_VALUE_V2) {
            throw new UnsupportedVersionException("Attempting to use idempotence with a broker which does not " +
                "support the required message format (v2). The broker must be version 0.11 or later.");
        }
        return MemoryRecords.builder(buffer, maxUsableMagic, compression, TimestampType.CREATE_TIME, 0L);
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
        ProducerBatch last = deque.peekLast();
        if (last != null) {
            FutureRecordMetadata future = last.tryAppend(timestamp, key, value, headers, callback, nowMs);
            if (future == null)
                last.closeForRecordAppends();
            else
                return new RecordAppendResult(future, deque.size() > 1 || last.isFull(), false, false);
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
        for (Map.Entry<TopicPartition, Deque<ProducerBatch>> entry : this.batches.entrySet()) {
            // expire the batches in the order of sending
            Deque<ProducerBatch> deque = entry.getValue();
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
        CompressionRatioEstimator.setEstimation(bigBatch.topicPartition.topic(), compression,
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
        // When we are requeing and have enabled idempotence, the reenqueued batch must always have a sequence.
        if (batch.baseSequence() == RecordBatch.NO_SEQUENCE)
            throw new IllegalStateException("Trying to re-enqueue a batch which doesn't have a sequence even " +
                "though idempotency is enabled.");

        if (transactionManager.nextBatchBySequence(batch.topicPartition) == null)
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
    public ReadyCheckResult ready(Cluster cluster, long nowMs) {
        Set<Node> readyNodes = new HashSet<>();
        long nextReadyCheckDelayMs = Long.MAX_VALUE;
        Set<String> unknownLeaderTopics = new HashSet<>();

        boolean exhausted = this.free.queued() > 0;
        for (Map.Entry<TopicPartition, Deque<ProducerBatch>> entry : this.batches.entrySet()) {
            Deque<ProducerBatch> deque = entry.getValue();
            synchronized (deque) {
                // When producing to a large number of partitions, this path is hot and deques are often empty.
                // We check whether a batch exists first to avoid the more expensive checks whenever possible.
                ProducerBatch batch = deque.peekFirst();
                if (batch != null) {
                    TopicPartition part = entry.getKey();
                    Node leader = cluster.leaderFor(part);
                    if (leader == null) {
                        // This is a partition for which leader is not known, but messages are available to send.
                        // Note that entries are currently not removed from batches when deque is empty.
                        unknownLeaderTopics.add(part.topic());
                    } else if (!readyNodes.contains(leader) && !isMuted(part)) {
                        long waitedTimeMs = batch.waitedTimeMs(nowMs);
                        boolean backingOff = batch.attempts() > 0 && waitedTimeMs < retryBackoffMs;
                        long timeToWaitMs = backingOff ? retryBackoffMs : lingerMs;
                        boolean full = deque.size() > 1 || batch.isFull();
                        boolean expired = waitedTimeMs >= timeToWaitMs;
                        boolean sendable = full || expired || exhausted || closed || flushInProgress();
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
                }
            }
        }
        return new ReadyCheckResult(readyNodes, nextReadyCheckDelayMs, unknownLeaderTopics);
    }

    /**
     * Check whether there are any batches which haven't been drained
     */
    public boolean hasUndrained() {
        for (Map.Entry<TopicPartition, Deque<ProducerBatch>> entry : this.batches.entrySet()) {
            Deque<ProducerBatch> deque = entry.getValue();
            synchronized (deque) {
                if (!deque.isEmpty())
                    return true;
            }
        }
        return false;
    }

    private boolean shouldStopDrainBatchesForPartition(ProducerBatch first, TopicPartition tp) {
        ProducerIdAndEpoch producerIdAndEpoch = null;
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
            if (firstInFlightSequence != RecordBatch.NO_SEQUENCE && first.hasSequence()
                && first.baseSequence() != firstInFlightSequence)
                // If the queued batch already has an assigned sequence, then it is being retried.
                // In this case, we wait until the next immediate batch is ready and drain that.
                // We only move on when the next in line batch is complete (either successfully or due to
                // a fatal broker error). This effectively reduces our in flight request count to 1.
                return true;
        }
        return false;
    }

    private List<ProducerBatch> drainBatchesForOneNode(Cluster cluster, Node node, int maxSize, long now) {
        int size = 0;
        List<PartitionInfo> parts = cluster.partitionsForNode(node.id());
        List<ProducerBatch> ready = new ArrayList<>();
        /* to make starvation less likely this loop doesn't start at 0 */
        int start = drainIndex = drainIndex % parts.size();
        do {
            PartitionInfo part = parts.get(drainIndex);
            TopicPartition tp = new TopicPartition(part.topic(), part.partition());
            this.drainIndex = (this.drainIndex + 1) % parts.size();

            // Only proceed if the partition has no in-flight batches.
            if (isMuted(tp))
                continue;

            Deque<ProducerBatch> deque = getDeque(tp);
            if (deque == null)
                continue;

            synchronized (deque) {
                // invariant: !isMuted(tp,now) && deque != null
                ProducerBatch first = deque.peekFirst();
                if (first == null)
                    continue;

                // first != null
                boolean backoff = first.attempts() > 0 && first.waitedTimeMs(now) < retryBackoffMs;
                // Only drain the batch if it is not during backoff period.
                if (backoff)
                    continue;

                if (size + first.estimatedSizeInBytes() > maxSize && !ready.isEmpty()) {
                    // there is a rare case that a single batch size is larger than the request size due to
                    // compression; in this case we will still eventually send this batch in a single request
                    break;
                } else {
                    if (shouldStopDrainBatchesForPartition(first, tp))
                        break;

                    boolean isTransactional = transactionManager != null && transactionManager.isTransactional();
                    ProducerIdAndEpoch producerIdAndEpoch =
                        transactionManager != null ? transactionManager.producerIdAndEpoch() : null;
                    ProducerBatch batch = deque.pollFirst();
                    if (producerIdAndEpoch != null && !batch.hasSequence()) {
                        // If the the producer id/epoch of the partition do not match the latest one
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
                    batch.close();
                    size += batch.records().sizeInBytes();
                    ready.add(batch);

                    batch.drained(now);
                }
            }
        } while (start != drainIndex);
        return ready;
    }

    /**
     * Drain all the data for the given nodes and collate them into a list of batches that will fit within the specified
     * size on a per-node basis. This method attempts to avoid choosing the same topic-node over and over.
     *
     * @param cluster The current cluster metadata
     * @param nodes The list of node to drain
     * @param maxSize The maximum number of bytes to drain
     * @param now The current unix time in milliseconds
     * @return A list of {@link ProducerBatch} for each node specified with total size less than the requested maxSize.
     */
    public Map<Integer, List<ProducerBatch>> drain(Cluster cluster, Set<Node> nodes, int maxSize, long now) {
        if (nodes.isEmpty())
            return Collections.emptyMap();

        Map<Integer, List<ProducerBatch>> batches = new HashMap<>();
        for (Node node : nodes) {
            List<ProducerBatch> ready = drainBatchesForOneNode(cluster, node, maxSize, now);
            batches.put(node.id(), ready);
        }
        return batches;
    }

    /**
     * The earliest absolute time a batch will expire (in milliseconds)
     */
    public long nextExpiryTimeMs() {
        return this.nextBatchExpiryTimeMs;
    }

    private Deque<ProducerBatch> getDeque(TopicPartition tp) {
        return batches.get(tp);
    }

    /**
     * Get the deque for the given topic-partition, creating it if necessary.
     */
    private Deque<ProducerBatch> getOrCreateDeque(TopicPartition tp) {
        Deque<ProducerBatch> d = this.batches.get(tp);
        if (d != null)
            return d;
        d = new ArrayDeque<>();
        Deque<ProducerBatch> previous = this.batches.putIfAbsent(tp, d);
        if (previous == null)
            return d;
        else
            return previous;
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

    /* Visible for testing */
    Map<TopicPartition, Deque<ProducerBatch>> batches() {
        return Collections.unmodifiableMap(batches);
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
            for (ProducerBatch batch : this.incomplete.copyAll())
                batch.produceFuture.await();
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
        this.batches.clear();
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

    /*
     * Metadata about a record just appended to the record accumulator
     */
    public final static class RecordAppendResult {
        public final FutureRecordMetadata future;
        public final boolean batchIsFull;
        public final boolean newBatchCreated;
        public final boolean abortForNewBatch;

        public RecordAppendResult(FutureRecordMetadata future, boolean batchIsFull, boolean newBatchCreated, boolean abortForNewBatch) {
            this.future = future;
            this.batchIsFull = batchIsFull;
            this.newBatchCreated = newBatchCreated;
            this.abortForNewBatch = abortForNewBatch;
        }
    }

    /*
     * The set of nodes that have at least one complete record batch in the accumulator
     */
    public final static class ReadyCheckResult {
        public final Set<Node> readyNodes;
        public final long nextReadyCheckDelayMs;
        public final Set<String> unknownLeaderTopics;

        public ReadyCheckResult(Set<Node> readyNodes, long nextReadyCheckDelayMs, Set<String> unknownLeaderTopics) {
            this.readyNodes = readyNodes;
            this.nextReadyCheckDelayMs = nextReadyCheckDelayMs;
            this.unknownLeaderTopics = unknownLeaderTopics;
        }
    }
}
