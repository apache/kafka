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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.InvalidTxnStateException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.TransactionCoordinatorFencedException;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * This class is used to validate the records appended by a given producer before they are written to the log.
 * It is initialized with the producer's state after the last successful append, and transitively validates the
 * sequence numbers and epochs of each new record. Additionally, this class accumulates transaction metadata
 * as the incoming records are validated.
 */
public class ProducerAppendInfo {
    private static final Logger log = LoggerFactory.getLogger(ProducerAppendInfo.class);
    private final TopicPartition topicPartition;
    private final long producerId;
    private final ProducerStateEntry currentEntry;
    private final AppendOrigin origin;
    private final VerificationStateEntry verificationStateEntry;

    private final List<TxnMetadata> transactions = new ArrayList<>();
    private final ProducerStateEntry updatedEntry;

    /**
     * Creates a new instance with the provided parameters.
     *
     * @param topicPartition         topic partition
     * @param producerId             The id of the producer appending to the log
     * @param currentEntry           The current entry associated with the producer id which contains metadata for a fixed number of
     *                               the most recent appends made by the producer. Validation of the first incoming append will
     *                               be made against the latest append in the current entry. New appends will replace older appends
     *                               in the current entry so that the space overhead is constant.
     * @param origin                 Indicates the origin of the append which implies the extent of validation. For example, offset
     *                               commits, which originate from the group coordinator, do not have sequence numbers and therefore
     *                               only producer epoch validation is done. Appends which come through replication are not validated
     *                               (we assume the validation has already been done) and appends from clients require full validation.
     * @param verificationStateEntry The most recent entry used for verification if no append has been completed yet otherwise null
     */
    public ProducerAppendInfo(TopicPartition topicPartition,
                              long producerId,
                              ProducerStateEntry currentEntry,
                              AppendOrigin origin,
                              VerificationStateEntry verificationStateEntry) {
        this.topicPartition = topicPartition;
        this.producerId = producerId;
        this.currentEntry = currentEntry;
        this.origin = origin;
        this.verificationStateEntry = verificationStateEntry;

        updatedEntry = currentEntry.withProducerIdAndBatchMetadata(producerId, Optional.empty());
    }

    public long producerId() {
        return producerId;
    }

    private void maybeValidateDataBatch(short producerEpoch, int firstSeq, long offset) {
        checkProducerEpoch(producerEpoch, offset);
        if (origin == AppendOrigin.CLIENT) {
            checkSequence(producerEpoch, firstSeq, offset);
        }
    }

    private void checkProducerEpoch(short producerEpoch, long offset) {
        if (producerEpoch < updatedEntry.producerEpoch()) {
            String message = "Epoch of producer " + producerId + " at offset " + offset + " in " + topicPartition +
                    " is " + producerEpoch + ", " + "which is smaller than the last seen epoch " + updatedEntry.producerEpoch();

            if (origin == AppendOrigin.REPLICATION) {
                log.warn(message);
            } else {
                // Starting from 2.7, we replaced ProducerFenced error with InvalidProducerEpoch in the
                // producer send response callback to differentiate from the former fatal exception,
                // letting client abort the ongoing transaction and retry.
                throw new InvalidProducerEpochException(message);
            }
        }
    }

    private void checkSequence(short producerEpoch, int appendFirstSeq, long offset) {
        if (verificationStateEntry != null && appendFirstSeq > verificationStateEntry.lowestSequence()) {
            throw new OutOfOrderSequenceException("Out of order sequence number for producer " + producerId + " at " +
                    "offset " + offset + " in partition " + topicPartition + ": " + appendFirstSeq +
                    " (incoming seq. number), " + verificationStateEntry.lowestSequence() + " (earliest seen sequence)");
        }
        if (producerEpoch != updatedEntry.producerEpoch()) {
            if (appendFirstSeq != 0) {
                if (updatedEntry.producerEpoch() != RecordBatch.NO_PRODUCER_EPOCH) {
                    throw new OutOfOrderSequenceException("Invalid sequence number for new epoch of producer " + producerId +
                            "at offset " + offset + " in partition " + topicPartition + ": " + producerEpoch + " (request epoch), "
                            + appendFirstSeq + " (seq. number), " + updatedEntry.producerEpoch() + " (current producer epoch)");
                }
            }
        } else {
            int currentLastSeq;
            if (!updatedEntry.isEmpty())
                currentLastSeq = updatedEntry.lastSeq();
            else if (producerEpoch == currentEntry.producerEpoch())
                currentLastSeq = currentEntry.lastSeq();
            else
                currentLastSeq = RecordBatch.NO_SEQUENCE;

            // If there is no current producer epoch (possibly because all producer records have been deleted due to
            // retention or the DeleteRecords API) accept writes with any sequence number
            if (!(currentEntry.producerEpoch() == RecordBatch.NO_PRODUCER_EPOCH || inSequence(currentLastSeq, appendFirstSeq))) {
                throw new OutOfOrderSequenceException("Out of order sequence number for producer " + producerId + " at " +
                        "offset " + offset + " in partition " + topicPartition + ": " + appendFirstSeq +
                        " (incoming seq. number), " + currentLastSeq + " (current end sequence number)");
            }
        }
    }

    private boolean inSequence(int lastSeq, int nextSeq) {
        return nextSeq == lastSeq + 1L || (nextSeq == 0 && lastSeq == Integer.MAX_VALUE);
    }

    public Optional<CompletedTxn> append(RecordBatch batch, Optional<LogOffsetMetadata> firstOffsetMetadataOpt) {
        if (batch.isControlBatch()) {
            Iterator<Record> recordIterator = batch.iterator();
            if (recordIterator.hasNext()) {
                Record record = recordIterator.next();
                EndTransactionMarker endTxnMarker = EndTransactionMarker.deserialize(record);
                return appendEndTxnMarker(endTxnMarker, batch.producerEpoch(), batch.baseOffset(), record.timestamp());
            } else {
                // An empty control batch means the entire transaction has been cleaned from the log, so no need to append
                return Optional.empty();
            }
        } else {
            LogOffsetMetadata firstOffsetMetadata = firstOffsetMetadataOpt.orElse(new LogOffsetMetadata(batch.baseOffset()));
            appendDataBatch(batch.producerEpoch(), batch.baseSequence(), batch.lastSequence(), batch.maxTimestamp(),
                    firstOffsetMetadata, batch.lastOffset(), batch.isTransactional());
            return Optional.empty();
        }
    }

    public void appendDataBatch(short epoch,
                                int firstSeq,
                                int lastSeq,
                                long lastTimestamp,
                                LogOffsetMetadata firstOffsetMetadata,
                                long lastOffset,
                                boolean isTransactional) {
        long firstOffset = firstOffsetMetadata.messageOffset;
        maybeValidateDataBatch(epoch, firstSeq, firstOffset);
        updatedEntry.addBatch(epoch, lastSeq, lastOffset, (int) (lastOffset - firstOffset), lastTimestamp);

        OptionalLong currentTxnFirstOffset = updatedEntry.currentTxnFirstOffset();
        if (currentTxnFirstOffset.isPresent() && !isTransactional) {
            // Received a non-transactional message while a transaction is active
            throw new InvalidTxnStateException("Expected transactional write from producer " + producerId + " at " +
                    "offset " + firstOffsetMetadata + " in partition " + topicPartition);
        } else if (currentTxnFirstOffset.isEmpty() && isTransactional) {
            // Began a new transaction
            updatedEntry.setCurrentTxnFirstOffset(firstOffset);
            transactions.add(new TxnMetadata(producerId, firstOffsetMetadata));
        }
    }

    private void checkCoordinatorEpoch(EndTransactionMarker endTxnMarker, long offset) {
        if (updatedEntry.coordinatorEpoch() > endTxnMarker.coordinatorEpoch()) {
            if (origin == AppendOrigin.REPLICATION) {
                log.info("Detected invalid coordinator epoch for producerId {} at offset {} in partition {}: {} is older than previously known coordinator epoch {}",
                        producerId, offset, topicPartition, endTxnMarker.coordinatorEpoch(), updatedEntry.coordinatorEpoch());
            } else {
                throw new TransactionCoordinatorFencedException("Invalid coordinator epoch for producerId " + producerId + " at " +
                        "offset " + offset + " in partition " + topicPartition + ": " + endTxnMarker.coordinatorEpoch() +
                        " (zombie), " + updatedEntry.coordinatorEpoch() + " (current)");
            }
        }
    }

    public Optional<CompletedTxn> appendEndTxnMarker(
            EndTransactionMarker endTxnMarker,
            short producerEpoch,
            long offset,
            long timestamp) {
        checkProducerEpoch(producerEpoch, offset);
        checkCoordinatorEpoch(endTxnMarker, offset);

        // Only emit the `CompletedTxn` for non-empty transactions. A transaction marker
        // without any associated data will not have any impact on the last stable offset
        // and would not need to be reflected in the transaction index.
        Optional<CompletedTxn> completedTxn = updatedEntry.currentTxnFirstOffset().isPresent() ?
                Optional.of(new CompletedTxn(producerId, updatedEntry.currentTxnFirstOffset().getAsLong(), offset,
                        endTxnMarker.controlType() == ControlRecordType.ABORT))
                : Optional.empty();
        updatedEntry.update(producerEpoch, endTxnMarker.coordinatorEpoch(), timestamp);
        return completedTxn;
    }

    public ProducerStateEntry toEntry() {
        return updatedEntry;
    }

    public List<TxnMetadata> startedTransactions() {
        return Collections.unmodifiableList(transactions);
    }

    @Override
    public String toString() {
        return "ProducerAppendInfo(" +
                "producerId=" + producerId +
                ", producerEpoch=" + updatedEntry.producerEpoch() +
                ", firstSequence=" + updatedEntry.firstSeq() +
                ", lastSequence=" + updatedEntry.lastSeq() +
                ", currentTxnFirstOffset=" + updatedEntry.currentTxnFirstOffset() +
                ", coordinatorEpoch=" + updatedEntry.coordinatorEpoch() +
                ", lastTimestamp=" + updatedEntry.lastTimestamp() +
                ", startedTransactions=" + transactions +
                ')';
    }
}
