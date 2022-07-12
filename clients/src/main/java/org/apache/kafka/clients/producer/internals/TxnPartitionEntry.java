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
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Consumer;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.utils.ProducerIdAndEpoch;

class TxnPartitionEntry {

    // The producer id/epoch being used for a given partition.
    ProducerIdAndEpoch producerIdAndEpoch;

    // The base sequence of the next batch bound for a given partition.
    int nextSequence;

    // The sequence number of the last record of the last ack'd batch from the given partition. When there are no
    // in flight requests for a partition, the lastAckedSequence(topicPartition) == nextSequence(topicPartition) - 1.
    int lastAckedSequence;

    // Keep track of the in flight batches bound for a partition, ordered by sequence. This helps us to ensure that
    // we continue to order batches by the sequence numbers even when the responses come back out of order during
    // leader failover. We add a batch to the queue when it is drained, and remove it when the batch completes
    // (either successfully or through a fatal failure).
    SortedSet<ProducerBatch> inflightBatchesBySequence;

    // We keep track of the last acknowledged offset on a per partition basis in order to disambiguate UnknownProducer
    // responses which are due to the retention period elapsing, and those which are due to actual lost data.
    long lastAckedOffset;

    // `inflightBatchesBySequence` should only have batches with the same producer id and producer
    // epoch, but there is an edge case where we may remove the wrong batch if the comparator
    // only takes `baseSequence` into account.
    // See https://github.com/apache/kafka/pull/12096#pullrequestreview-955554191 for details.
    private static final Comparator<ProducerBatch> PRODUCER_BATCH_COMPARATOR =
        Comparator.comparingLong(ProducerBatch::producerId)
            .thenComparingInt(ProducerBatch::producerEpoch)
            .thenComparingInt(ProducerBatch::baseSequence);

    TxnPartitionEntry() {
        this.producerIdAndEpoch = ProducerIdAndEpoch.NONE;
        this.nextSequence = 0;
        this.lastAckedSequence = TransactionManager.NO_LAST_ACKED_SEQUENCE_NUMBER;
        this.lastAckedOffset = ProduceResponse.INVALID_OFFSET;
        this.inflightBatchesBySequence = new TreeSet<>(PRODUCER_BATCH_COMPARATOR);
    }

    void resetSequenceNumbers(Consumer<ProducerBatch> resetSequence) {
        TreeSet<ProducerBatch> newInflights = new TreeSet<>(PRODUCER_BATCH_COMPARATOR);
        for (ProducerBatch inflightBatch : inflightBatchesBySequence) {
            resetSequence.accept(inflightBatch);
            newInflights.add(inflightBatch);
        }
        inflightBatchesBySequence = newInflights;
    }
}
