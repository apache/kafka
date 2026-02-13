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

import org.apache.kafka.common.record.internal.ControlRecordType;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * This is a helper class to facilitate tracking transaction state while cleaning the log. It maintains a set
 * of the ongoing aborted and committed transactions as the cleaner is working its way through the log. This
 * class is responsible for deciding when transaction markers can be removed and is therefore also responsible
 * for updating the cleaned transaction index accordingly.
 */
public class CleanedTransactionMetadata {
    private final Set<Long> ongoingCommittedTxns = new HashSet<>();
    private final Map<Long, AbortedTransactionMetadata> ongoingAbortedTxns = new HashMap<>();

    /**
     * Minheap of aborted transactions sorted by the transaction first offset
     */
    private final PriorityQueue<AbortedTxn> abortedTransactions = new PriorityQueue<>(
            Comparator.comparingLong(AbortedTxn::firstOffset)
    );

    /**
     * Output cleaned index to write retained aborted transactions
     */
    private Optional<TransactionIndex> cleanedIndex = Optional.empty();

    /**
     * Update the cleaned index.
     *
     * @param cleanedIndex The new cleaned index
     */
    public void setCleanedIndex(Optional<TransactionIndex> cleanedIndex) {
        this.cleanedIndex = cleanedIndex;
    }

    /**
     * Update the cleaned transaction state with the new found aborted transactions that has just been traversed.
     *
     * @param abortedTransactions The new found aborted transactions to add
     */
    public void addAbortedTransactions(List<AbortedTxn> abortedTransactions) {
        this.abortedTransactions.addAll(abortedTransactions);
    }

    /**
     * Update the cleaned transaction state with a control batch that has just been traversed by the cleaner.
     * Return true if the control batch can be discarded.
     *
     * @param controlBatch The control batch that been traversed
     *
     * @return True if the control batch can be discarded
     */
    public boolean onControlBatchRead(RecordBatch controlBatch) {
        consumeAbortedTxnsUpTo(controlBatch.lastOffset());

        Iterator<Record> controlRecordIterator = controlBatch.iterator();
        if (controlRecordIterator.hasNext()) {
            Record controlRecord = controlRecordIterator.next();
            ControlRecordType controlType = ControlRecordType.parse(controlRecord.key());
            long producerId = controlBatch.producerId();

            switch (controlType) {
                case ABORT:
                    AbortedTransactionMetadata abortedTxnMetadata = ongoingAbortedTxns.remove(producerId);

                    // Retain the marker until all batches from the transaction have been removed.
                    if (abortedTxnMetadata != null && abortedTxnMetadata.lastObservedBatchOffset.isPresent()) {
                        cleanedIndex.ifPresent(index -> {
                            try {
                                index.append(abortedTxnMetadata.abortedTxn);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
                        return false;
                    }

                    return true;

                case COMMIT:
                    // This marker is eligible for deletion if we didn't traverse any batches from the transaction
                    return !ongoingCommittedTxns.remove(producerId);

                default:
                    return false;
            }
        } else {
            // An empty control batch was already cleaned, so it's safe to discard
            return true;
        }
    }

    private void consumeAbortedTxnsUpTo(long offset) {
        while (!abortedTransactions.isEmpty() && abortedTransactions.peek().firstOffset() <= offset) {
            AbortedTxn abortedTxn = abortedTransactions.poll();
            if (abortedTxn != null) {
                ongoingAbortedTxns.computeIfAbsent(abortedTxn.producerId(), id -> new AbortedTransactionMetadata(abortedTxn));
            }
        }
    }

    /**
     * Update the transactional state for the incoming non-control batch. If the batch is part of
     * an aborted transaction, return true to indicate that it is safe to discard.
     *
     * @param batch The batch to read when updating the transactional state
     *
     * @return Whether the batch is part of an aborted transaction or not
     */
    public boolean onBatchRead(RecordBatch batch) {
        consumeAbortedTxnsUpTo(batch.lastOffset());
        if (batch.isTransactional()) {
            Optional<AbortedTransactionMetadata> metadata = Optional.ofNullable(ongoingAbortedTxns.get(batch.producerId()));

            if (metadata.isPresent()) {
                metadata.get().lastObservedBatchOffset = Optional.of(batch.lastOffset());
                return true;
            } else {
                ongoingCommittedTxns.add(batch.producerId());
                return false;
            }
        } else {
            return false;
        }
    }

    private static class AbortedTransactionMetadata {
        Optional<Long> lastObservedBatchOffset = Optional.empty();
        final AbortedTxn abortedTxn;

        public AbortedTransactionMetadata(AbortedTxn abortedTxn) {
            this.abortedTxn = abortedTxn;
        }
    }
}
