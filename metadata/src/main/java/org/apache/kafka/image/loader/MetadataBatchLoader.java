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

package org.apache.kafka.image.loader;

import org.apache.kafka.common.metadata.MetadataRecordType;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.fault.FaultHandler;
import org.slf4j.Logger;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Loads batches of metadata updates from Raft commits into MetadataDelta-s. Multiple batches from a commit
 * are buffered into a MetadataDelta to achieve batching of records and reduce the number of times
 * MetadataPublishers must be updated. This class also supports metadata transactions (KIP-866).
 *
 *
 */
public class MetadataBatchLoader {

    enum TransactionState {
        NO_TRANSACTION,
        STARTED_TRANSACTION,
        CONTINUED_TRANSACTION,
        ENDED_TRANSACTION,
        ABORTED_TRANSACTION;
    }

    @FunctionalInterface
    public interface MetadataUpdater {
        void update(MetadataDelta delta, MetadataImage image, LogDeltaManifest manifest);
    }

    private final Logger log;
    private final Time time;
    private final FaultHandler faultHandler;
    private final MetadataUpdater callback;

    private MetadataImage image;
    private MetadataDelta delta;
    private long lastOffset;
    private int lastEpoch;
    private long lastContainedLogTimeMs;
    private long numBytes;
    private int numBatches;
    private long totalBatchElapsedNs;
    private TransactionState transactionState;
    private boolean hasSeenRecord;

    public MetadataBatchLoader(
        LogContext logContext,
        Time time,
        FaultHandler faultHandler,
        MetadataUpdater callback
    ) {
        this.log = logContext.logger(MetadataBatchLoader.class);
        this.time = time;
        this.faultHandler = faultHandler;
        this.callback = callback;
        this.resetToImage(MetadataImage.EMPTY);
        this.hasSeenRecord = false;
    }

    /**
     * @return True if this batch loader has seen at least one record.
     */
    public boolean hasSeenRecord() {
        return hasSeenRecord;
    }

    /**
     * Reset the state of this batch loader to the given image. Any un-flushed state will be
     * discarded. This is called after applying a delta and passing it back to MetadataLoader, or
     * when MetadataLoader loads a snapshot.
     *
     * @param image     Metadata image to reset this batch loader's state to.
     */
    public final void resetToImage(MetadataImage image) {
        this.image = image;
        this.hasSeenRecord = true;
        this.delta = new MetadataDelta.Builder().setImage(image).build();
        this.transactionState = TransactionState.NO_TRANSACTION;
        this.lastOffset = image.provenance().lastContainedOffset();
        this.lastEpoch = image.provenance().lastContainedEpoch();
        this.lastContainedLogTimeMs = image.provenance().lastContainedLogTimeMs();
        this.numBytes = 0;
        this.numBatches = 0;
        this.totalBatchElapsedNs = 0;
    }

    /**
     * Load a batch of records from the log. We have to do some bookkeeping here to
     * translate between batch offsets and record offsets, and track the number of bytes we
     * have read. Additionally, there is the chance that one of the records is a metadata
     * version change which needs to be handled differently.
     * </p>
     * If this batch starts a transaction, any records preceding the transaction in this
     * batch will be implicitly added to the transaction.
     *
     * @param batch    The reader which yields the batches.
     * @return         The time in nanoseconds that elapsed while loading this batch
     */

    public long loadBatch(Batch<ApiMessageAndVersion> batch, LeaderAndEpoch leaderAndEpoch) {
        long startNs = time.nanoseconds();
        int indexWithinBatch = 0;

        lastContainedLogTimeMs = batch.appendTimestamp();
        lastEpoch = batch.epoch();

        for (ApiMessageAndVersion record : batch.records()) {
            try {
                replay(record);
            } catch (Throwable e) {
                faultHandler.handleFault("Error loading metadata log record from offset " +
                    (batch.baseOffset() + indexWithinBatch), e);
            }

            // Emit the accumulated delta if a new transaction has been started and one of the following is true
            // 1) this is not the first record in this batch
            // 2) this is not the first batch since last emitting a delta
            if (transactionState == TransactionState.STARTED_TRANSACTION && (indexWithinBatch > 0 || numBatches > 0)) {
                MetadataProvenance provenance = new MetadataProvenance(lastOffset, lastEpoch, lastContainedLogTimeMs);
                LogDeltaManifest manifest = LogDeltaManifest.newBuilder()
                    .provenance(provenance)
                    .leaderAndEpoch(leaderAndEpoch)
                    .numBatches(numBatches) // This will be zero if we have not yet read a batch
                    .elapsedNs(totalBatchElapsedNs)
                    .numBytes(numBytes)     // This will be zero if we have not yet read a batch
                    .build();
                if (log.isDebugEnabled()) {
                    log.debug("handleCommit: Generated a metadata delta between {} and {} from {} batch(es) in {} us.",
                            image.offset(), manifest.provenance().lastContainedOffset(),
                            manifest.numBatches(), NANOSECONDS.toMicros(manifest.elapsedNs()));
                }
                applyDeltaAndUpdate(delta, manifest);
                transactionState = TransactionState.STARTED_TRANSACTION;
            }

            lastOffset = batch.baseOffset() + indexWithinBatch;
            indexWithinBatch++;
        }
        long elapsedNs = time.nanoseconds() - startNs;

        // Update state for the manifest. The actual byte count will only be included in the last delta emitted for
        // a given batch or transaction.
        lastOffset = batch.lastOffset();
        numBytes += batch.sizeInBytes();
        numBatches += 1;
        totalBatchElapsedNs += elapsedNs;
        return totalBatchElapsedNs;
    }

    /**
     * Flush the metadata accumulated in this batch loader if not in the middle of a transaction. The
     * flushed metadata will be passed to the {@link MetadataUpdater} configured for this class.
     */
    public void maybeFlushBatches(LeaderAndEpoch leaderAndEpoch) {
        MetadataProvenance provenance = new MetadataProvenance(lastOffset, lastEpoch, lastContainedLogTimeMs);
        LogDeltaManifest manifest = LogDeltaManifest.newBuilder()
            .provenance(provenance)
            .leaderAndEpoch(leaderAndEpoch)
            .numBatches(numBatches)
            .elapsedNs(totalBatchElapsedNs)
            .numBytes(numBytes)
            .build();
        switch (transactionState) {
            case STARTED_TRANSACTION:
            case CONTINUED_TRANSACTION:
                log.debug("handleCommit: not publishing since a transaction starting at {} is still in progress. " +
                    "{} batch(es) processed so far.", image.offset(), numBatches);
                break;
            case ABORTED_TRANSACTION:
                log.debug("handleCommit: publishing empty delta between {} and {} from {} batch(es) " +
                    "since a transaction was aborted", image.offset(), manifest.provenance().lastContainedOffset(),
                    manifest.numBatches());
                applyDeltaAndUpdate(new MetadataDelta.Builder().setImage(image).build(), manifest);
                break;
            case ENDED_TRANSACTION:
            case NO_TRANSACTION:
                if (log.isDebugEnabled()) {
                    log.debug("handleCommit: Generated a metadata delta between {} and {} from {} batch(es) in {} us.",
                        image.offset(), manifest.provenance().lastContainedOffset(),
                        manifest.numBatches(), NANOSECONDS.toMicros(manifest.elapsedNs()));
                }
                applyDeltaAndUpdate(delta, manifest);
                break;
        }
    }

    private void replay(ApiMessageAndVersion record) {
        MetadataRecordType type = MetadataRecordType.fromId(record.message().apiKey());
        switch (type) {
            case BEGIN_TRANSACTION_RECORD:
                if (transactionState == TransactionState.STARTED_TRANSACTION ||
                    transactionState == TransactionState.CONTINUED_TRANSACTION) {
                    throw new RuntimeException("Encountered BeginTransactionRecord while already in a transaction");
                } else {
                    transactionState = TransactionState.STARTED_TRANSACTION;
                }
                break;
            case END_TRANSACTION_RECORD:
                if (transactionState == TransactionState.CONTINUED_TRANSACTION ||
                    transactionState == TransactionState.STARTED_TRANSACTION) {
                    transactionState = TransactionState.ENDED_TRANSACTION;
                } else {
                    throw new RuntimeException("Encountered EndTransactionRecord without having seen a BeginTransactionRecord");
                }
                break;
            case ABORT_TRANSACTION_RECORD:
                if (transactionState == TransactionState.CONTINUED_TRANSACTION ||
                    transactionState == TransactionState.STARTED_TRANSACTION) {
                    transactionState = TransactionState.ABORTED_TRANSACTION;
                } else {
                    throw new RuntimeException("Encountered AbortTransactionRecord without having seen a BeginTransactionRecord");
                }
                break;
            default:
                switch (transactionState) {
                    case STARTED_TRANSACTION:
                        // If we see a non-transaction record after starting a transaction, transition to CONTINUED_TRANSACTION
                        transactionState = TransactionState.CONTINUED_TRANSACTION;
                        break;
                    case ENDED_TRANSACTION:
                    case ABORTED_TRANSACTION:
                        // If we see a non-transaction record after ending a transaction, transition back to NO_TRANSACTION
                        transactionState = TransactionState.NO_TRANSACTION;
                        break;
                    case CONTINUED_TRANSACTION:
                    case NO_TRANSACTION:
                    default:
                        break;
                }
                hasSeenRecord = true;
                delta.replay(record.message());
        }
    }

    private void applyDeltaAndUpdate(MetadataDelta delta, LogDeltaManifest manifest) {
        try {
            image = delta.apply(manifest.provenance());
        } catch (Throwable e) {
            faultHandler.handleFault("Error generating new metadata image from " +
                "metadata delta between offset " + image.offset() +
                " and " + manifest.provenance().lastContainedOffset(), e);
        }

        // Whether we can apply the delta or not, we need to make sure the batch loader gets reset
        // to the image known to MetadataLoader
        callback.update(delta, image, manifest);
        resetToImage(image);
    }
}
