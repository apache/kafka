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
package org.apache.kafka.server.log.internals;

import org.apache.kafka.common.record.RecordBatch;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Stream;

/**
 * The batchMetadata is ordered such that the batch with the lowest sequence is at the head of the queue while the
 * batch with the highest sequence is at the tail of the queue. We will retain at most {@link ProducerStateEntry#NumBatchesToRetain}
 * elements in the queue. When the queue is at capacity, we remove the first element to make space for the incoming batch.
 */
public class ProducerStateEntry {
    public static final int NumBatchesToRetain = 5;
    public final long producerId;
    private final List<BatchMetadata> batchMetadata;
    public short producerEpoch;
    public int coordinatorEpoch;
    public long lastTimestamp;
    public OptionalLong currentTxnFirstOffset;

    public ProducerStateEntry(long producerId) {
        this(producerId, Collections.emptyList(), RecordBatch.NO_PRODUCER_EPOCH, -1, RecordBatch.NO_TIMESTAMP, OptionalLong.empty());
    }

    public ProducerStateEntry(long producerId, List<BatchMetadata> batchMetadata, short producerEpoch, int coordinatorEpoch, long lastTimestamp, OptionalLong currentTxnFirstOffset) {
        this.producerId = producerId;
        this.batchMetadata = batchMetadata;
        this.producerEpoch = producerEpoch;
        this.coordinatorEpoch = coordinatorEpoch;
        this.lastTimestamp = lastTimestamp;
        this.currentTxnFirstOffset = currentTxnFirstOffset;
    }

    public int firstSeq() {
        return isEmpty() ? RecordBatch.NO_SEQUENCE : batchMetadata.get(0).firstSeq();
    }


    public long firstDataOffset() {
        return isEmpty() ? -1L : batchMetadata.get(0).firstOffset();
    }

    public int lastSeq() {
        return isEmpty() ? RecordBatch.NO_SEQUENCE : batchMetadata.get(batchMetadata.size() - 1).lastSeq;
    }

    public long lastDataOffset() {
        return isEmpty() ? -1L : batchMetadata.get(batchMetadata.size() - 1).lastOffset;
    }

    public int lastOffsetDelta() {
        return isEmpty() ? 0 : batchMetadata.get(batchMetadata.size() - 1).offsetDelta;
    }

    public boolean isEmpty() {
        return batchMetadata.isEmpty();
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

    private void addBatchMetadata(BatchMetadata batch) {
        if (batchMetadata.size() == ProducerStateEntry.NumBatchesToRetain) batchMetadata.remove(0);
        batchMetadata.add(batch);
    }

    public void update(ProducerStateEntry nextEntry) {
        maybeUpdateProducerEpoch(nextEntry.producerEpoch);
        while (!nextEntry.batchMetadata.isEmpty()) addBatchMetadata(nextEntry.batchMetadata.remove(0));
        this.coordinatorEpoch = nextEntry.coordinatorEpoch;
        this.currentTxnFirstOffset = nextEntry.currentTxnFirstOffset;
        this.lastTimestamp = nextEntry.lastTimestamp;
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

    public List<BatchMetadata> batchMetadata() {
        return Collections.unmodifiableList(batchMetadata);
    }

    public short producerEpoch() {
        return producerEpoch;
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
}