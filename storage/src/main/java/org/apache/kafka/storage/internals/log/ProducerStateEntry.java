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
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.stream.Stream;

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
    
    private VerificationState verificationState;
    
    // Before any batches are associated with the entry, the tentative sequence represents the lowest sequence seen.
    private OptionalInt tentativeSequence;
    
    public enum VerificationState {
        EMPTY,
        VERIFYING,
        VERIFIED
    }

    public static ProducerStateEntry empty(long producerId) {
        return new ProducerStateEntry(producerId, RecordBatch.NO_PRODUCER_EPOCH, -1, RecordBatch.NO_TIMESTAMP, OptionalLong.empty(), Optional.empty(), VerificationState.EMPTY, OptionalInt.empty());
    }

    public static ProducerStateEntry forVerification(long producerId, short producerEpoch, long lastTimestamp) {
        return new ProducerStateEntry(producerId, producerEpoch, -1, lastTimestamp, OptionalLong.empty(), Optional.empty(), VerificationState.EMPTY, OptionalInt.empty());
    }

    public ProducerStateEntry(long producerId, short producerEpoch, int coordinatorEpoch, long lastTimestamp, OptionalLong currentTxnFirstOffset, Optional<BatchMetadata> firstBatchMetadata, VerificationState verificationState, OptionalInt tentativeSequence) {
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.coordinatorEpoch = coordinatorEpoch;
        this.lastTimestamp = lastTimestamp;
        this.currentTxnFirstOffset = currentTxnFirstOffset;
        firstBatchMetadata.ifPresent(batchMetadata::add);
        this.verificationState = verificationState;
        this.tentativeSequence = tentativeSequence;
    }

    public int firstSeq() {
        return isEmpty() ? RecordBatch.NO_SEQUENCE : batchMetadata.getFirst().firstSeq();
    }

    public int lastSeq() {
        return isEmpty() ? RecordBatch.NO_SEQUENCE : batchMetadata.getLast().lastSeq;
    }

    public long firstDataOffset() {
        return isEmpty() ? -1L : batchMetadata.getFirst().firstOffset();
    }

    public long lastDataOffset() {
        return isEmpty() ? -1L : batchMetadata.getLast().lastOffset;
    }

    public int lastOffsetDelta() {
        return isEmpty() ? 0 : batchMetadata.getLast().offsetDelta;
    }

    public boolean isEmpty() {
        return batchMetadata.isEmpty();
    }

    /**
     * Returns a new instance with the provided parameters (when present) and the values from the current instance
     * otherwise.
     */
    public ProducerStateEntry withProducerIdAndBatchMetadata(long producerId, Optional<BatchMetadata> batchMetadata) {
        return new ProducerStateEntry(producerId, this.producerEpoch(), this.coordinatorEpoch, this.lastTimestamp,
            this.currentTxnFirstOffset, batchMetadata, this.verificationState, this.tentativeSequence);
    }

    public void addBatch(short producerEpoch, int lastSeq, long lastOffset, int offsetDelta, long timestamp) {
        maybeUpdateProducerEpoch(producerEpoch);
        addBatchMetadata(new BatchMetadata(lastSeq, lastOffset, offsetDelta, timestamp));
        this.lastTimestamp = timestamp;
    }

    public boolean maybeUpdateProducerEpoch(short producerEpoch) {
        if (this.producerEpoch != producerEpoch) {
            batchMetadata.clear();
            this.producerEpoch = producerEpoch;
            return true;
        } else {
            return false;
        }
    }

    public boolean maybeUpdateProducerHigherEpoch(short producerEpoch) {
        if (this.producerEpoch < producerEpoch) {
            batchMetadata.clear();
            this.producerEpoch = producerEpoch;
            return true;
        } else {
            return false;
        }
    }
    
    // We only set tentative sequence if no batches have been written to the log. It is used to avoid OutOfOrderSequenceExceptions
    // when we saw a lower sequence during transaction verification. We will update the sequence when there is no batch metadata if:
    //  a) There is no tentative sequence yet
    //  b) A lower sequence for the same epoch is seen and should thereby block records after that
    //  c) A higher producer epoch is found that will reset the lowest seen sequence
    public void maybeUpdateTentativeSequence(int sequence, short producerEpoch) {
        if (batchMetadata.isEmpty() && 
                (!this.tentativeSequence.isPresent() || (this.producerEpoch == producerEpoch && this.tentativeSequence.getAsInt() > sequence) || this.producerEpoch < producerEpoch))
            this.tentativeSequence = OptionalInt.of(sequence);
    }

    private void addBatchMetadata(BatchMetadata batch) {
        // When appending a batch, we no longer need tentative sequence.
        this.tentativeSequence = OptionalInt.empty();
        if (batchMetadata.size() == ProducerStateEntry.NUM_BATCHES_TO_RETAIN) batchMetadata.removeFirst();
        batchMetadata.add(batch);
    }
    
    public boolean compareAndSetVerificationState(short expectedProducerEpoch, VerificationState expectedVerificationState, VerificationState newVerificationState) {
        if (expectedProducerEpoch == this.producerEpoch && verificationState == expectedVerificationState) {
            this.verificationState = newVerificationState;
            return true;
        }
        return false;
    }

    public void update(ProducerStateEntry nextEntry) {
        update(nextEntry.producerEpoch, nextEntry.coordinatorEpoch, nextEntry.lastTimestamp, nextEntry.batchMetadata, nextEntry.currentTxnFirstOffset, nextEntry.verificationState);
    }

    public void update(short producerEpoch, int coordinatorEpoch, long lastTimestamp) {
        update(producerEpoch, coordinatorEpoch, lastTimestamp, new ArrayDeque<>(0), OptionalLong.empty(), VerificationState.EMPTY);
    }

    private void update(short producerEpoch,
                        int coordinatorEpoch,
                        long lastTimestamp,
                        Deque<BatchMetadata> batchMetadata,
                        OptionalLong currentTxnFirstOffset,
                        VerificationState verificationState) {
        maybeUpdateProducerEpoch(producerEpoch);
        while (!batchMetadata.isEmpty())
            addBatchMetadata(batchMetadata.removeFirst());
        this.coordinatorEpoch = coordinatorEpoch;
        this.currentTxnFirstOffset = currentTxnFirstOffset;
        this.lastTimestamp = lastTimestamp;
        this.verificationState = verificationState;
    }

    public void setCurrentTxnFirstOffset(long firstOffset) {
        this.currentTxnFirstOffset = OptionalLong.of(firstOffset);
    }

    public Optional<BatchMetadata> findDuplicateBatch(RecordBatch batch) {
        if (batch.producerEpoch() != producerEpoch) return Optional.empty();
        else return batchWithSequenceRange(batch.baseSequence(), batch.lastSequence());
    }

    // Return the batch metadata of the cached batch having the exact sequence range, if any.
    Optional<BatchMetadata> batchWithSequenceRange(int firstSeq, int lastSeq) {
        Stream<BatchMetadata> duplicate = batchMetadata.stream().filter(metadata -> firstSeq == metadata.firstSeq() && lastSeq == metadata.lastSeq);
        return duplicate.findFirst();
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
    
    public VerificationState verificationState() {
        return verificationState;
    }
    
    public OptionalInt tentativeSequence() {
        return tentativeSequence;
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
