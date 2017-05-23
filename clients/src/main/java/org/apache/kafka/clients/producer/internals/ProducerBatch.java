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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordBatchTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.CompressionRatioEstimator;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.ProduceResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.kafka.common.record.RecordBatch.NO_TIMESTAMP;


/**
 * A batch of records that is or will be sent.
 *
 * This class is not thread safe and external synchronization must be used when modifying it
 */
public final class ProducerBatch {

    private static final Logger log = LoggerFactory.getLogger(ProducerBatch.class);

    final long createdMs;
    final TopicPartition topicPartition;
    final ProduceRequestResult produceFuture;

    private final List<Thunk> thunks = new ArrayList<>();
    private final MemoryRecordsBuilder recordsBuilder;

    private final AtomicInteger attempts = new AtomicInteger(0);
    private final boolean isSplitBatch;
    int recordCount;
    int maxRecordSize;
    private long lastAttemptMs;
    private long lastAppendTime;
    private long drainedMs;
    private String expiryErrorMessage;
    private AtomicBoolean completed;
    private boolean retry;

    public ProducerBatch(TopicPartition tp, MemoryRecordsBuilder recordsBuilder, long now) {
        this(tp, recordsBuilder, now, false);
    }

    public ProducerBatch(TopicPartition tp, MemoryRecordsBuilder recordsBuilder, long now, boolean isSplitBatch) {
        this.createdMs = now;
        this.lastAttemptMs = now;
        this.recordsBuilder = recordsBuilder;
        this.topicPartition = tp;
        this.lastAppendTime = createdMs;
        this.produceFuture = new ProduceRequestResult(topicPartition);
        this.completed = new AtomicBoolean();
        this.retry = false;
        this.isSplitBatch = isSplitBatch;
        float compressionRatioEstimation = CompressionRatioEstimator.estimation(topicPartition.topic(),
                                                                                recordsBuilder.compressionType());
        recordsBuilder.setEstimatedCompressionRatio(compressionRatioEstimation);
    }

    /**
     * Append the record to the current record set and return the relative offset within that record set
     *
     * @return The RecordSend corresponding to this record or null if there isn't sufficient room.
     */
    public FutureRecordMetadata tryAppend(long timestamp, byte[] key, byte[] value, Header[] headers, Callback callback, long now) {
        if (!recordsBuilder.hasRoomFor(timestamp, key, value)) {
            return null;
        } else {
            long checksum = this.recordsBuilder.append(timestamp, key, value, headers);
            this.maxRecordSize = Math.max(this.maxRecordSize, AbstractRecords.sizeInBytesUpperBound(magic(), key, value, headers));
            this.lastAppendTime = now;
            FutureRecordMetadata future = new FutureRecordMetadata(this.produceFuture, this.recordCount,
                                                                   timestamp, checksum,
                                                                   key == null ? -1 : key.length,
                                                                   value == null ? -1 : value.length);
            // we have to keep every future returned to the users in case the batch needs to be
            // split to several new batches and resent.
            thunks.add(new Thunk(callback, future));
            this.recordCount++;
            return future;
        }
    }

    /**
     +     * This method is only used by {@link #split(int)} when splitting a large batch to smaller ones.
     +     * @return true if the record has been successfully appended, false otherwise.
     +     */
    private boolean tryAppendForSplit(long timestamp, ByteBuffer key, ByteBuffer value, Header[] headers, Thunk thunk) {
        if (!recordsBuilder.hasRoomFor(timestamp, key, value)) {
            return false;
        } else {
            // No need to get the CRC.
            this.recordsBuilder.append(timestamp, key, value);
            this.maxRecordSize = Math.max(this.maxRecordSize,
                                          AbstractRecords.sizeInBytesUpperBound(magic(), key, value, headers));
            FutureRecordMetadata future = new FutureRecordMetadata(this.produceFuture, this.recordCount,
                                                                   timestamp, thunk.future.checksum(),
                                                                   key == null ? -1 : key.remaining(),
                                                                   value == null ? -1 : value.remaining());
            // Chain the future to the original thunk.
            thunk.future.chain(future);
            this.thunks.add(thunk);
            this.recordCount++;
            return true;
        }
    }

    /**
     * Complete the request.
     *
     * @param baseOffset The base offset of the messages assigned by the server
     * @param logAppendTime The log append time or -1 if CreateTime is being used
     * @param exception The exception that occurred (or null if the request was successful)
     */
    public void done(long baseOffset, long logAppendTime, RuntimeException exception) {
        log.trace("Produced messages to topic-partition {} with base offset offset {} and error: {}.",
                  topicPartition, baseOffset, exception);

        if (completed.getAndSet(true))
            throw new IllegalStateException("Batch has already been completed");

        // Set the future before invoking the callbacks as we rely on its state for the `onCompletion` call
        produceFuture.set(baseOffset, logAppendTime, exception);

        // execute callbacks
        for (Thunk thunk : thunks) {
            try {
                if (exception == null) {
                    RecordMetadata metadata = thunk.future.value();
                    if (thunk.callback != null)
                        thunk.callback.onCompletion(metadata, null);
                } else {
                    if (thunk.callback != null)
                        thunk.callback.onCompletion(null, exception);
                }
            } catch (Exception e) {
                log.error("Error executing user-provided callback on message for topic-partition '{}'", topicPartition, e);
            }
        }

        produceFuture.done();
    }

    public Deque<ProducerBatch> split(int splitBatchSize) {
        Deque<ProducerBatch> batches = new ArrayDeque<>();
        MemoryRecords memoryRecords = recordsBuilder.build();
        Iterator<MutableRecordBatch> recordBatchIter = memoryRecords.batches().iterator();
        if (!recordBatchIter.hasNext())
            throw new IllegalStateException("Cannot split an empty producer batch.");
        RecordBatch recordBatch = recordBatchIter.next();
        if (recordBatchIter.hasNext())
            throw new IllegalStateException("A producer batch should only have one record batch.");

        Iterator<Thunk> thunkIter = thunks.iterator();
        // We always allocate batch size because we are already splitting a big batch.
        // And we also Retain the create time of the original batch.
        ProducerBatch batch = null;
        for (Record record : recordBatch) {
            assert thunkIter.hasNext();
            Thunk thunk = thunkIter.next();
            if (batch == null) {
                batch = createBatchOffAccumulatorForRecord(this.topicPartition, this.recordsBuilder.compressionType(),
                                                           record, splitBatchSize, this.createdMs);
            }

            // A newly created batch can always host the first message.
            if (!batch.tryAppendForSplit(record.timestamp(), record.key(), record.value(), record.headers(), thunk)) {
                batches.add(batch);
                batch = createBatchOffAccumulatorForRecord(this.topicPartition, this.recordsBuilder.compressionType(),
                                                           record, splitBatchSize, this.createdMs);
                batch.tryAppendForSplit(record.timestamp(), record.key(), record.value(), record.headers(), thunk);
            }
        }
        // Close the last batch and add it to the batch list after split.
        if (batch != null)
            batches.add(batch);

        produceFuture.set(ProduceResponse.INVALID_OFFSET, NO_TIMESTAMP, new RecordBatchTooLargeException());
        produceFuture.done();
        return batches;
    }

    private ProducerBatch createBatchOffAccumulatorForRecord(TopicPartition tp,
                                                             CompressionType compressionType,
                                                             Record record,
                                                             int batchSize,
                                                             long createdMs) {
        int initialSize = Math.max(Records.LOG_OVERHEAD + AbstractRecords.sizeInBytesUpperBound(magic(),
                                                                                                record.key(),
                                                                                                record.value(),
                                                                                                record.headers()),
                                   batchSize);
        return createBatchOffAccumulator(tp, compressionType, initialSize, createdMs);
    }

    // package private for testing purpose.
    static ProducerBatch createBatchOffAccumulator(TopicPartition tp,
                                                   CompressionType compressionType,
                                                   int batchSize,
                                                   long createdMs) {
        ByteBuffer buffer = ByteBuffer.allocate(batchSize);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer,
                                                             compressionType,
                                                             TimestampType.CREATE_TIME,
                                                             batchSize);
        return new ProducerBatch(tp, builder, createdMs, true);
    }

    /**
     * A callback and the associated FutureRecordMetadata argument to pass to it.
     */
    final private static class Thunk {
        final Callback callback;
        final FutureRecordMetadata future;

        Thunk(Callback callback, FutureRecordMetadata future) {
            this.callback = callback;
            this.future = future;
        }
    }

    @Override
    public String toString() {
        return "ProducerBatch(topicPartition=" + topicPartition + ", recordCount=" + recordCount + ")";
    }

    /**
     * A batch whose metadata is not available should be expired if one of the following is true:
     * <ol>
     *     <li> the batch is not in retry AND request timeout has elapsed after it is ready (full or linger.ms has reached).
     *     <li> the batch is in retry AND request timeout has elapsed after the backoff period ended.
     * </ol>
     * This methods closes this batch and sets {@code expiryErrorMessage} if the batch has timed out.
     * {@link #expirationDone()} must be invoked to complete the produce future and invoke callbacks.
     */
    boolean maybeExpire(int requestTimeoutMs, long retryBackoffMs, long now, long lingerMs, boolean isFull) {
        if (!this.inRetry() && isFull && requestTimeoutMs < (now - this.lastAppendTime))
            expiryErrorMessage = (now - this.lastAppendTime) + " ms has passed since last append";
        else if (!this.inRetry() && requestTimeoutMs < (createdTimeMs(now) - lingerMs))
            expiryErrorMessage = (createdTimeMs(now) - lingerMs) + " ms has passed since batch creation plus linger time";
        else if (this.inRetry() && requestTimeoutMs < (waitedTimeMs(now) - retryBackoffMs))
            expiryErrorMessage = (waitedTimeMs(now) - retryBackoffMs) + " ms has passed since last attempt plus backoff time";

        boolean expired = expiryErrorMessage != null;
        if (expired)
            close();
        return expired;
    }

    /**
     * Completes the produce future with timeout exception and invokes callbacks.
     * This method should be invoked only if {@link #maybeExpire(int, long, long, long, boolean)}
     * returned true.
     */
    void expirationDone() {
        if (expiryErrorMessage == null)
            throw new IllegalStateException("Batch has not expired");
        this.done(-1L, NO_TIMESTAMP,
                  new TimeoutException("Expiring " + recordCount + " record(s) for " + topicPartition + ": " + expiryErrorMessage));
    }

    int attempts() {
        return attempts.get();
    }

    void reenqueued(long now) {
        attempts.getAndIncrement();
        lastAttemptMs = Math.max(lastAppendTime, now);
        lastAppendTime = Math.max(lastAppendTime, now);
        retry = true;
    }

    long queueTimeMs() {
        return drainedMs - createdMs;
    }

    long createdTimeMs(long nowMs) {
        return Math.max(0, nowMs - createdMs);
    }

    long waitedTimeMs(long nowMs) {
        return Math.max(0, nowMs - lastAttemptMs);
    }

    void drained(long nowMs) {
        this.drainedMs = Math.max(drainedMs, nowMs);
    }

    boolean isSplitBatch() {
        return isSplitBatch;
    }

    /**
     * Returns if the batch is been retried for sending to kafka
     */
    public boolean inRetry() {
        return this.retry;
    }

    public MemoryRecords records() {
        return recordsBuilder.build();
    }

    public int sizeInBytes() {
        return recordsBuilder.sizeInBytes();
    }

    public double compressionRatio() {
        return recordsBuilder.compressionRatio();
    }

    public boolean isFull() {
        return recordsBuilder.isFull();
    }

    public void setProducerState(ProducerIdAndEpoch producerIdAndEpoch, int baseSequence) {
        recordsBuilder.setProducerState(producerIdAndEpoch.producerId, producerIdAndEpoch.epoch, baseSequence);
    }

    /**
     * Release resources required for record appends (e.g. compression buffers). Once this method is called, it's only
     * possible to update the RecordBatch header.
     */
    public void closeForRecordAppends() {
        recordsBuilder.closeForRecordAppends();
    }

    public void close() {
        recordsBuilder.close();
        if (!recordsBuilder.isControlBatch()) {
            CompressionRatioEstimator.updateEstimation(topicPartition.topic(),
                                                       recordsBuilder.compressionType(),
                                                       (float) recordsBuilder.compressionRatio());
        }
    }

    public void abort() {
        recordsBuilder.abort();
    }

    public boolean isClosed() {
        return recordsBuilder.isClosed();
    }

    public ByteBuffer buffer() {
        return recordsBuilder.buffer();
    }

    public int initialCapacity() {
        return recordsBuilder.initialCapacity();
    }

    public boolean isWritable() {
        return !recordsBuilder.isClosed();
    }

    public byte magic() {
        return recordsBuilder.magic();
    }

    /**
     * Return the ProducerId (Pid) of the current batch.
     */
    public long producerId() {
        return recordsBuilder.producerId();
    }

    public short producerEpoch() {
        return recordsBuilder.producerEpoch();
    }
}
