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

import java.util.HashMap;
import java.util.Map;
import java.util.OptionalInt;
import java.util.OptionalLong;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.ProducerIdAndEpoch;
import org.slf4j.Logger;

class TxnPartitionMap {

    private final Logger log;

    private final Map<TopicPartition, TxnPartitionEntry> topicPartitions = new HashMap<>();

    TxnPartitionMap(LogContext logContext) {
        this.log = logContext.logger(TxnPartitionMap.class);
    }

    TxnPartitionEntry get(TopicPartition topicPartition) {
        TxnPartitionEntry ent = topicPartitions.get(topicPartition);
        if (ent == null) {
            throw new IllegalStateException("Trying to get txnPartitionEntry for " + topicPartition +
                ", but it was never set for this partition.");
        }
        return ent;
    }

    TxnPartitionEntry getOrCreate(TopicPartition topicPartition) {
        return topicPartitions.computeIfAbsent(topicPartition, tp -> new TxnPartitionEntry(tp));
    }

    boolean contains(TopicPartition topicPartition) {
        return topicPartitions.containsKey(topicPartition);
    }

    void reset() {
        topicPartitions.clear();
    }

    OptionalLong lastAckedOffset(TopicPartition topicPartition) {
        TxnPartitionEntry entry = topicPartitions.get(topicPartition);
        if (entry != null)
            return entry.lastAckedOffset();
        return OptionalLong.empty();
    }

    OptionalInt lastAckedSequence(TopicPartition topicPartition) {
        TxnPartitionEntry entry = topicPartitions.get(topicPartition);
        if (entry != null)
            return entry.lastAckedSequence();
        return OptionalInt.empty();
    }

    void startSequencesAtBeginning(TopicPartition topicPartition, ProducerIdAndEpoch newProducerIdAndEpoch) {
        TxnPartitionEntry entry = get(topicPartition);
        if (entry != null)
            entry.startSequencesAtBeginning(newProducerIdAndEpoch);
    }

    void remove(TopicPartition topicPartition) {
        topicPartitions.remove(topicPartition);
    }


    void updateLastAckedOffset(TopicPartition topicPartition, boolean isTransactional, long lastOffset) {
        OptionalLong lastAckedOffset = lastAckedOffset(topicPartition);
        // It might happen that the TransactionManager has been reset while a request was reenqueued and got a valid
        // response for this. This can happen only if the producer is only idempotent (not transactional) and in
        // this case there will be no tracked bookkeeper entry about it, so we have to insert one.
        if (!lastAckedOffset.isPresent() && !isTransactional)
            getOrCreate(topicPartition);
        if (lastOffset > lastAckedOffset.orElse(ProduceResponse.INVALID_OFFSET))
            get(topicPartition).setLastAckedOffset(lastOffset);
        else
            log.trace("Partition {} keeps lastOffset at {}", topicPartition, lastOffset);
    }

    // If a batch is failed fatally, the sequence numbers for future batches bound for the partition must be adjusted
    // so that they don't fail with the OutOfOrderSequenceException.
    //
    // This method must only be called when we know that the batch is question has been unequivocally failed by the broker,
    // ie. it has received a confirmed fatal status code like 'Message Too Large' or something similar.
    void adjustSequencesDueToFailedBatch(ProducerBatch batch) {
        if (!contains(batch.topicPartition))
            // Sequence numbers are not being tracked for this partition. This could happen if the producer id was just
            // reset due to a previous OutOfOrderSequenceException.
            return;
        log.debug("producerId: {}, send to partition {} failed fatally. Reducing future sequence numbers by {}",
                batch.producerId(), batch.topicPartition, batch.recordCount);

        get(batch.topicPartition).adjustSequencesDueToFailedBatch(batch.baseSequence(), batch.recordCount);
    }

    int maybeUpdateLastAckedSequence(TopicPartition topicPartition, int sequence) {
        TxnPartitionEntry entry = topicPartitions.get(topicPartition);
        if (entry != null)
            return entry.maybeUpdateLastAckedSequence(sequence);
        return TxnPartitionEntry.NO_LAST_ACKED_SEQUENCE_NUMBER;
    }

    ProducerBatch nextBatchBySequence(TopicPartition topicPartition) {
        return get(topicPartition).nextBatchBySequence();
    }

    void removeInFlightBatch(ProducerBatch batch) {
        get(batch.topicPartition).removeInFlightBatch(batch);
    }
}
