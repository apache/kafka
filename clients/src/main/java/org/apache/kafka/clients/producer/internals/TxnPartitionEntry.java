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

import java.util.Comparator;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.utils.PrimitiveRef;
import org.apache.kafka.common.utils.ProducerIdAndEpoch;

class TxnPartitionEntry {
    static final int NO_LAST_ACKED_SEQUENCE_NUMBER = -1;

    private final TopicPartition topicPartition;

    // The producer id/epoch being used for a given partition.
    private ProducerIdAndEpoch producerIdAndEpoch;

    // The base sequence of the next batch bound for a given partition.
    private int nextSequence;

    // The sequence number of the last record of the last ack'd batch from the given partition. When there are no
    // in flight requests for a partition, the lastAckedSequence(topicPartition) == nextSequence(topicPartition) - 1.
    private int lastAckedSequence;

    // Keep track of the in flight batches bound for a partition, ordered by sequence. This helps us to ensure that
    // we continue to order batches by the sequence numbers even when the responses come back out of order during
    // leader failover. We add a batch to the queue when it is drained, and remove it when the batch completes
    // (either successfully or through a fatal failure).
    private SortedSet<ProducerBatch> inflightBatchesBySequence;

    // We keep track of the last acknowledged offset on a per partition basis in order to disambiguate UnknownProducer
    // responses which are due to the retention period elapsing, and those which are due to actual lost data.
    private long lastAckedOffset;

    // `inflightBatchesBySequence` should only have batches with the same producer id and producer
    // epoch, but there is an edge case where we may remove the wrong batch if the comparator
    // only takes `baseSequence` into account.
    // See https://github.com/apache/kafka/pull/12096#pullrequestreview-955554191 for details.
    private static final Comparator<ProducerBatch> PRODUCER_BATCH_COMPARATOR =
        Comparator.comparingLong(ProducerBatch::producerId)
            .thenComparingInt(ProducerBatch::producerEpoch)
            .thenComparingInt(ProducerBatch::baseSequence);

    TxnPartitionEntry(TopicPartition topicPartition) {
        this.topicPartition = topicPartition;
        this.producerIdAndEpoch = ProducerIdAndEpoch.NONE;
        this.nextSequence = 0;
        this.lastAckedSequence = NO_LAST_ACKED_SEQUENCE_NUMBER;
        this.lastAckedOffset = ProduceResponse.INVALID_OFFSET;
        this.inflightBatchesBySequence = new TreeSet<>(PRODUCER_BATCH_COMPARATOR);
    }

    ProducerIdAndEpoch producerIdAndEpoch() {
        return producerIdAndEpoch;
    }

    int nextSequence() {
        return nextSequence;
    }

    OptionalLong lastAckedOffset() {
        if (lastAckedOffset != ProduceResponse.INVALID_OFFSET)
            return OptionalLong.of(lastAckedOffset);
        return OptionalLong.empty();
    }

    OptionalInt lastAckedSequence() {
        if (lastAckedSequence != TxnPartitionEntry.NO_LAST_ACKED_SEQUENCE_NUMBER)
            return OptionalInt.of(lastAckedSequence);
        return OptionalInt.empty();
    }

    boolean hasInflightBatches() {
        return !inflightBatchesBySequence.isEmpty();
    }

    ProducerBatch nextBatchBySequence() {
        return inflightBatchesBySequence.isEmpty() ? null : inflightBatchesBySequence.first();
    }

    void incrementSequence(int increment) {
        this.nextSequence = DefaultRecordBatch.incrementSequence(this.nextSequence, increment);
    }

    void addInflightBatch(ProducerBatch batch) {
        inflightBatchesBySequence.add(batch);
    }

    void setLastAckedOffset(long lastAckedOffset) {
        this.lastAckedOffset = lastAckedOffset;
    }

    void startSequencesAtBeginning(ProducerIdAndEpoch newProducerIdAndEpoch) {
        final PrimitiveRef.IntRef sequence = PrimitiveRef.ofInt(0);
        resetSequenceNumbers(inFlightBatch -> {
            inFlightBatch.resetProducerState(newProducerIdAndEpoch, sequence.value);
            sequence.value += inFlightBatch.recordCount;
        });
        producerIdAndEpoch = newProducerIdAndEpoch;
        nextSequence = sequence.value;
        lastAckedSequence = NO_LAST_ACKED_SEQUENCE_NUMBER;
    }

    int maybeUpdateLastAckedSequence(int sequence) {
        if (sequence > lastAckedSequence) {
            lastAckedSequence = sequence;
            return sequence;
        }
        return lastAckedSequence;
    }

    void removeInFlightBatch(ProducerBatch batch) {
        inflightBatchesBySequence.remove(batch);
    }

    void adjustSequencesDueToFailedBatch(long baseSequence, int recordCount) {
        decrementSequence(recordCount);
        resetSequenceNumbers(inFlightBatch -> {
            if (inFlightBatch.baseSequence() < baseSequence)
                return;

            int newSequence = inFlightBatch.baseSequence() - recordCount;
            if (newSequence < 0)
                throw new IllegalStateException("Sequence number for batch with sequence " + inFlightBatch.baseSequence()
                        + " for partition " + topicPartition + " is going to become negative: " + newSequence);

            inFlightBatch.resetProducerState(new ProducerIdAndEpoch(inFlightBatch.producerId(), inFlightBatch.producerEpoch()), newSequence);
        });
    }

    private void resetSequenceNumbers(Consumer<ProducerBatch> resetSequence) {
        TreeSet<ProducerBatch> newInflights = new TreeSet<>(PRODUCER_BATCH_COMPARATOR);
        for (ProducerBatch inflightBatch : inflightBatchesBySequence) {
            resetSequence.accept(inflightBatch);
            newInflights.add(inflightBatch);
        }
        inflightBatchesBySequence = newInflights;
    }

    private boolean decrementSequence(int decrement) {
        int updatedSequence = nextSequence;
        updatedSequence -= decrement;
        if (updatedSequence < 0) {
            throw new IllegalStateException(
                    "Sequence number for partition " + topicPartition + " is going to become negative: "
                            + updatedSequence);
        }
        this.nextSequence = updatedSequence;
        return true;
    }
}
