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
import org.apache.kafka.common.utils.PrimitiveRef;
import org.apache.kafka.common.utils.ProducerIdAndEpoch;

class TxnPartitionMap {

    final Map<TopicPartition, TxnPartitionEntry> topicPartitions = new HashMap<>();

    TxnPartitionEntry get(TopicPartition topicPartition) {
        TxnPartitionEntry ent = topicPartitions.get(topicPartition);
        if (ent == null) {
            throw new IllegalStateException("Trying to get the sequence number for " + topicPartition +
                ", but the sequence number was never set for this partition.");
        }
        return ent;
    }

    TxnPartitionEntry getOrCreate(TopicPartition topicPartition) {
        return topicPartitions.computeIfAbsent(topicPartition, tp -> new TxnPartitionEntry());
    }

    boolean contains(TopicPartition topicPartition) {
        return topicPartitions.containsKey(topicPartition);
    }

    void reset() {
        topicPartitions.clear();
    }

    OptionalLong lastAckedOffset(TopicPartition topicPartition) {
        TxnPartitionEntry entry = topicPartitions.get(topicPartition);
        if (entry != null && entry.lastAckedOffset != ProduceResponse.INVALID_OFFSET) {
            return OptionalLong.of(entry.lastAckedOffset);
        } else {
            return OptionalLong.empty();
        }
    }

    OptionalInt lastAckedSequence(TopicPartition topicPartition) {
        TxnPartitionEntry entry = topicPartitions.get(topicPartition);
        if (entry != null && entry.lastAckedSequence != TransactionManager.NO_LAST_ACKED_SEQUENCE_NUMBER) {
            return OptionalInt.of(entry.lastAckedSequence);
        } else {
            return OptionalInt.empty();
        }
    }

    void startSequencesAtBeginning(TopicPartition topicPartition, ProducerIdAndEpoch newProducerIdAndEpoch) {
        final PrimitiveRef.IntRef sequence = PrimitiveRef.ofInt(0);
        TxnPartitionEntry topicPartitionEntry = get(topicPartition);
        topicPartitionEntry.resetSequenceNumbers(inFlightBatch -> {
            inFlightBatch.resetProducerState(newProducerIdAndEpoch, sequence.value, inFlightBatch.isTransactional());
            sequence.value += inFlightBatch.recordCount;
        });
        topicPartitionEntry.producerIdAndEpoch = newProducerIdAndEpoch;
        topicPartitionEntry.nextSequence = sequence.value;
        topicPartitionEntry.lastAckedSequence = TransactionManager.NO_LAST_ACKED_SEQUENCE_NUMBER;
    }
}
