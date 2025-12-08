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
package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.record.RecordBatch;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * This class represents the state of a specific producer-id.
 * It contains batchMetadata queue which is ordered such that the batch with the lowest sequence is at the head of the
 * queue while the batch with the highest sequence is at the tail of the queue. We will retain at most {@link ProducerStateEntry#NUM_BATCHES_TO_RETAIN}
 * elements in the queue. When the queue is at capacity, we remove the first element to make space for the incoming batch.
 */
public class ProducerStateEntry {
    public static final int NUM_BATCHES_TO_RETAIN = 5;
    private final long producerId;
    private final Deque<BatchMetadata> batchMetadata = new ArrayDeque<>();

    private short producerEpoch;
    private int coordinatorEpoch;
    private long lastTimestamp;
    private OptionalLong currentTxnFirstOffset;

    static ProducerStateEntry empty(long producerId) {
        return new ProducerStateEntry(producerId, RecordBatch.NO_PRODUCER_EPOCH, -1, RecordBatch.NO_TIMESTAMP, OptionalLong.empty(), Optional.empty());
    }

    public ProducerStateEntry(long producerId, short producerEpoch, int coordinatorEpoch, long lastTimestamp,
                              OptionalLong currentTxnFirstOffset, Optional<BatchMetadata> firstBatchMetadata) {
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.coordinatorEpoch = coordinatorEpoch;
        this.lastTimestamp = lastTimestamp;
        this.currentTxnFirstOffset = currentTxnFirstOffset;
        firstBatchMetadata.ifPresent(batch -> {
            batchMetadata.add(batch);
            this.lastTimestamp = batch.timestamp();
        });
    }

    int firstSeq() {
        return isEmpty() ? RecordBatch.NO_SEQUENCE : batchMetadata.getFirst().firstSeq();
    }

    int lastSeq() {
        return isEmpty() ? RecordBatch.NO_SEQUENCE : batchMetadata.getLast().lastSeq();
    }

    public long firstDataOffset() {
        return isEmpty() ? -1L : batchMetadata.getFirst().firstOffset();
    }

    public long lastDataOffset() {
        return isEmpty() ? -1L : batchMetadata.getLast().lastOffset();
    }

    int lastOffsetDelta() {
        return isEmpty() ? 0 : batchMetadata.getLast().offsetDelta();
    }

    boolean isEmpty() {
        return batchMetadata.isEmpty();
    }

    /**
     * Returns a new instance with the provided producer ID and the values from the current instance.
     */
    ProducerStateEntry withProducerId(long producerId) {
        return new ProducerStateEntry(producerId, producerEpoch(), coordinatorEpoch, lastTimestamp, currentTxnFirstOffset, Optional.empty());
    }

    void addBatch(short producerEpoch, int lastSeq, long lastOffset, int offsetDelta, long timestamp) {
        maybeUpdateProducerEpoch(producerEpoch);
        addBatchMetadata(new BatchMetadata(lastSeq, lastOffset, offsetDelta, timestamp));
        this.lastTimestamp = timestamp;
    }

    private void maybeUpdateProducerEpoch(short producerEpoch) {
        if (this.producerEpoch != producerEpoch) {
            batchMetadata.clear();
            this.producerEpoch = producerEpoch;
        }
    }

    private void addBatchMetadata(BatchMetadata batch) {
        if (batchMetadata.size() == ProducerStateEntry.NUM_BATCHES_TO_RETAIN) batchMetadata.removeFirst();
        batchMetadata.add(batch);
    }

    void update(ProducerStateEntry nextEntry) {
        update(nextEntry.producerEpoch, nextEntry.coordinatorEpoch, nextEntry.lastTimestamp, nextEntry.batchMetadata, nextEntry.currentTxnFirstOffset);
    }

    void update(short producerEpoch, int coordinatorEpoch, long lastTimestamp) {
        update(producerEpoch, coordinatorEpoch, lastTimestamp, new ArrayDeque<>(0), OptionalLong.empty());
    }

    private void update(short producerEpoch, int coordinatorEpoch, long lastTimestamp, Deque<BatchMetadata> batchMetadata,
                        OptionalLong currentTxnFirstOffset) {
        maybeUpdateProducerEpoch(producerEpoch);
        while (!batchMetadata.isEmpty())
            addBatchMetadata(batchMetadata.removeFirst());
        this.coordinatorEpoch = coordinatorEpoch;
        this.currentTxnFirstOffset = currentTxnFirstOffset;
        this.lastTimestamp = lastTimestamp;
    }

    void setCurrentTxnFirstOffset(long firstOffset) {
        this.currentTxnFirstOffset = OptionalLong.of(firstOffset);
    }

    Optional<BatchMetadata> findDuplicateBatch(RecordBatch batch) {
        return batch.producerEpoch() != producerEpoch ? Optional.empty() : batchWithSequenceRange(batch.baseSequence(), batch.lastSequence());
    }

    // Return the batch metadata of the cached batch having the exact sequence range, if any.
    private Optional<BatchMetadata> batchWithSequenceRange(int firstSeq, int lastSeq) {
        return batchMetadata.stream()
            .filter(metadata -> firstSeq == metadata.firstSeq() && lastSeq == metadata.lastSeq())
            .findFirst();
    }

    public Collection<BatchMetadata> batchMetadata() {
        return Collections.unmodifiableCollection(batchMetadata);
    }

    public short producerEpoch() {
        return producerEpoch;
    }

    public long producerId() {
        return producerId;
    }

    public int coordinatorEpoch() {
        return coordinatorEpoch;
    }

    public long lastTimestamp() {
        return lastTimestamp;
    }

    public OptionalLong currentTxnFirstOffset() {
        return currentTxnFirstOffset;
    }

    @Override
    public String toString() {
        return "ProducerStateEntry(" +
                "producerId=" + producerId +
                ", producerEpoch=" + producerEpoch +
                ", currentTxnFirstOffset=" + currentTxnFirstOffset +
                ", coordinatorEpoch=" + coordinatorEpoch +
                ", lastTimestamp=" + lastTimestamp +
                ", batchMetadata=" + batchMetadata +
                ')';
    }
}
