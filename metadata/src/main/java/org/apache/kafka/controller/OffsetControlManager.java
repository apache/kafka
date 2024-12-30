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

package org.apache.kafka.controller;

import org.apache.kafka.common.metadata.AbortTransactionRecord;
import org.apache.kafka.common.metadata.BeginTransactionRecord;
import org.apache.kafka.common.metadata.EndTransactionRecord;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.controller.metrics.QuorumControllerMetrics;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.snapshot.Snapshots;
import org.apache.kafka.timeline.SnapshotRegistry;

import org.slf4j.Logger;

import java.util.Optional;


/**
 * Manages read and write offsets, and in-memory snapshots.
 * <p>
 * Also manages the following metrics:
 *      kafka.controller:type=KafkaController,name=ActiveControllerCount
 *      kafka.controller:type=KafkaController,name=LastAppliedRecordLagMs
 *      kafka.controller:type=KafkaController,name=LastAppliedRecordOffset
 *      kafka.controller:type=KafkaController,name=LastAppliedRecordTimestamp
 *      kafka.controller:type=KafkaController,name=LastCommittedRecordOffset
 */
class OffsetControlManager {
    static class Builder {
        private LogContext logContext = null;
        private SnapshotRegistry snapshotRegistry = null;
        private QuorumControllerMetrics metrics = null;
        private Time time = Time.SYSTEM;

        Builder setLogContext(LogContext logContext) {
            this.logContext = logContext;
            return this;
        }

        Builder setSnapshotRegistry(SnapshotRegistry snapshotRegistry) {
            this.snapshotRegistry = snapshotRegistry;
            return this;
        }

        Builder setMetrics(QuorumControllerMetrics metrics) {
            this.metrics = metrics;
            return this;
        }

        Builder setTime(Time time) {
            this.time = time;
            return this;
        }

        OffsetControlManager build() {
            if (logContext == null) logContext = new LogContext();
            if (snapshotRegistry == null) snapshotRegistry = new SnapshotRegistry(logContext);
            if (metrics == null) {
                metrics = new QuorumControllerMetrics(Optional.empty(), time);
            }
            return new OffsetControlManager(logContext,
                    snapshotRegistry,
                    metrics,
                    time);
        }
    }

    /**
     * The slf4j logger.
     */
    private final Logger log;

    /**
     * The snapshot registry.
     */
    private final SnapshotRegistry snapshotRegistry;

    /**
     * The quorum controller metrics.
     */
    private final QuorumControllerMetrics metrics;

    /**
     * The clock.
     */
    private final Time time;

    /**
     * The ID of the snapshot that we're currently replaying, or null if there is none.
     */
    private OffsetAndEpoch currentSnapshotId;

    /**
     * The name of the snapshot that we're currently replaying, or null if there is none.
     */
    private String currentSnapshotName;

    /**
     * The latest committed offset.
     */
    private long lastCommittedOffset;

    /**
     * The latest committed epoch.
     */
    private int lastCommittedEpoch;

    /**
     * The latest offset that it is safe to read from.
     */
    private long lastStableOffset;

    /**
     * The offset of the transaction we're in, or -1 if we are not in one.
     */
    private long transactionStartOffset;

    /**
     * The next offset we should write to, or -1 if the controller is not active. Exclusive offset.
     */
    private long nextWriteOffset;

    private OffsetControlManager(
        LogContext logContext,
        SnapshotRegistry snapshotRegistry,
        QuorumControllerMetrics metrics,
        Time time
    ) {
        this.log = logContext.logger(OffsetControlManager.class);
        this.snapshotRegistry = snapshotRegistry;
        this.metrics = metrics;
        this.time = time;
        this.currentSnapshotId = null;
        this.currentSnapshotName = null;
        this.lastCommittedOffset = -1L;
        this.lastCommittedEpoch = -1;
        this.lastStableOffset = -1L;
        this.transactionStartOffset = -1L;
        this.nextWriteOffset = -1L;
        snapshotRegistry.idempotentCreateSnapshot(-1L);
        metrics.setActive(false);
        metrics.setLastCommittedRecordOffset(-1L);
        metrics.setLastAppliedRecordOffset(-1L);
        metrics.setLastAppliedRecordTimestamp(-1L);
    }

    /**
     *  @return The SnapshotRegistry used by this offset control manager.
     */
    SnapshotRegistry snapshotRegistry() {
        return snapshotRegistry;
    }

    /**
     * @return QuorumControllerMetrics managed by this offset control manager.
     */
    QuorumControllerMetrics metrics() {
        return metrics;
    }

    /**
     * @return the ID of the current snapshot.
     */
    OffsetAndEpoch currentSnapshotId() {
        return currentSnapshotId;
    }

    /**
     * @return the name of the current snapshot.
     */
    String currentSnapshotName() {
        return currentSnapshotName;
    }

    /**
     * @return the last committed offset.
     */
    long lastCommittedOffset() {
        return lastCommittedOffset;
    }

    /**
     * @return the last committed epoch.
     */
    int lastCommittedEpoch() {
        return lastCommittedEpoch;
    }

    /**
     * @return the latest offset that it is safe to read from.
     */
    long lastStableOffset() {
        return lastStableOffset;
    }

    /**
     * @return the transaction start offset, or -1 if there is no transaction.
     */
    long transactionStartOffset() {
        return transactionStartOffset;
    }

    /**
     * @return the next offset that the active controller should write to.
     */
    long nextWriteOffset() {
        return nextWriteOffset;
    }

    /**
     * @return true only if the manager is active.
     */
    boolean active() {
        return nextWriteOffset != -1L;
    }

    /**
     * Called when the QuorumController becomes active.
     *
     * @param newNextWriteOffset The new next write offset to use. Must be non-negative.
     */
    void activate(long newNextWriteOffset) {
        if (active()) {
            throw new RuntimeException("Can't activate already active OffsetControlManager.");
        }
        if (newNextWriteOffset < 0) {
            throw new RuntimeException("Invalid negative newNextWriteOffset " +
                    newNextWriteOffset + ".");
        }
        // Before switching to active, create an in-memory snapshot at the last committed
        // offset. This is required because the active controller assumes that there is always
        // an in-memory snapshot at the last committed offset.
        snapshotRegistry.idempotentCreateSnapshot(lastStableOffset);
        this.nextWriteOffset = newNextWriteOffset;
        metrics.setActive(true);
    }

    /**
     * Called when the QuorumController becomes inactive.
     */
    void deactivate() {
        if (!active()) {
            throw new RuntimeException("Can't deactivate inactive OffsetControlManager.");
        }
        metrics.setActive(false);
        metrics.setLastAppliedRecordOffset(lastStableOffset);
        this.nextWriteOffset = -1L;
        if (!snapshotRegistry.hasSnapshot(lastStableOffset)) {
            throw new RuntimeException("Unable to reset to last stable offset " + lastStableOffset +
                    ". No in-memory snapshot found for this offset.");
        }
        snapshotRegistry.revertToSnapshot(lastStableOffset);
    }

    /**
     * Handle the callback from the Raft layer indicating that a batch was committed.
     *
     * @param batch The batch that has been committed.
     */
    void handleCommitBatch(Batch<ApiMessageAndVersion> batch) {
        this.lastCommittedOffset = batch.lastOffset();
        this.lastCommittedEpoch = batch.epoch();
        maybeAdvanceLastStableOffset();
        handleCommitBatchMetrics(batch);
    }

    void handleCommitBatchMetrics(Batch<ApiMessageAndVersion> batch) {
        metrics.setLastCommittedRecordOffset(batch.lastOffset());
        if (!active()) {
            // On standby controllers, the last applied record offset is equals to the last
            // committed offset.
            metrics.setLastAppliedRecordOffset(batch.lastOffset());
            metrics.setLastAppliedRecordTimestamp(batch.appendTimestamp());
        }
    }

    /**
     * Called by the active controller after it has invoked scheduleAtomicAppend to schedule some
     * records to be written.
     *
     * @param lastOffset The offset of the last record that was written.
     */
    void handleScheduleAppend(long lastOffset) {
        this.nextWriteOffset = lastOffset + 1;

        snapshotRegistry.idempotentCreateSnapshot(lastOffset);

        metrics.setLastAppliedRecordOffset(lastOffset);

        // This is not truly the append timestamp. The KRaft client doesn't expose the append
        // time when scheduling a write. This is good enough because this is called right after
        // the records were given to the KRAft client for appending and the default append linger
        // for KRaft is 25ms.
        metrics.setLastAppliedRecordTimestamp(time.milliseconds());
    }

    /**
     * Advance the last stable offset if needed.
     */
    void maybeAdvanceLastStableOffset() {
        long newLastStableOffset;
        if (transactionStartOffset == -1L) {
            newLastStableOffset = lastCommittedOffset;
        } else {
            newLastStableOffset = Math.min(transactionStartOffset - 1, lastCommittedOffset);
        }
        if (lastStableOffset < newLastStableOffset) {
            lastStableOffset = newLastStableOffset;
            snapshotRegistry.deleteSnapshotsUpTo(lastStableOffset);
            if (!active()) {
                snapshotRegistry.idempotentCreateSnapshot(lastStableOffset);
            }
        }
    }

    /**
     * Called before we load a Raft snapshot.
     *
     * @param snapshotId The Raft snapshot offset and epoch.
     */
    void beginLoadSnapshot(OffsetAndEpoch snapshotId) {
        if (currentSnapshotId != null) {
            throw new RuntimeException("Can't begin reading snapshot for " + snapshotId +
                    ", because we are already reading " + currentSnapshotId);
        }
        this.currentSnapshotId = snapshotId;
        this.currentSnapshotName = Snapshots.filenameFromSnapshotId(snapshotId);
        log.info("Starting to load snapshot {}. Previous lastCommittedOffset was {}. Previous " +
                "transactionStartOffset was {}.", currentSnapshotName, lastCommittedOffset,
                transactionStartOffset);
        this.snapshotRegistry.reset();
        this.lastCommittedOffset = -1L;
        this.lastCommittedEpoch = -1;
        this.lastStableOffset = -1L;
        this.transactionStartOffset = -1L;
        this.nextWriteOffset = -1L;
    }

    /**
     * Called after we have finished loading a Raft snapshot.
     *
     * @param timestamp The timestamp of the snapshot.
     */
    void endLoadSnapshot(long timestamp) {
        if (currentSnapshotId == null) {
            throw new RuntimeException("Can't end loading snapshot, because there is no " +
                    "current snapshot.");
        }
        log.info("Successfully loaded snapshot {}.", currentSnapshotName);
        this.snapshotRegistry.idempotentCreateSnapshot(currentSnapshotId.offset());
        this.lastCommittedOffset = currentSnapshotId.offset();
        this.lastCommittedEpoch = currentSnapshotId.epoch();
        this.lastStableOffset = currentSnapshotId.offset();
        this.transactionStartOffset = -1L;
        this.nextWriteOffset = -1L;
        metrics.setLastCommittedRecordOffset(currentSnapshotId.offset());
        metrics.setLastAppliedRecordOffset(currentSnapshotId.offset());
        metrics.setLastAppliedRecordTimestamp(timestamp);
        this.currentSnapshotId = null;
        this.currentSnapshotName = null;
    }

    public void replay(BeginTransactionRecord message, long offset) {
        if (currentSnapshotId != null) {
            throw new RuntimeException("BeginTransactionRecord cannot appear within a snapshot.");
        }
        if (transactionStartOffset != -1L) {
            throw new RuntimeException("Can't replay a BeginTransactionRecord at " + offset +
                " because the transaction at " + transactionStartOffset + " was never closed.");
        }
        snapshotRegistry.idempotentCreateSnapshot(offset - 1);
        transactionStartOffset = offset;
        log.info("Replayed {} at offset {}.", message, offset);
    }

    public void replay(EndTransactionRecord message, long offset) {
        if (currentSnapshotId != null) {
            throw new RuntimeException("EndTransactionRecord cannot appear within a snapshot.");
        }
        if (transactionStartOffset == -1L) {
            throw new RuntimeException("Can't replay an EndTransactionRecord at " + offset +
                    " because there is no open transaction.");
        }
        transactionStartOffset = -1L;
        log.info("Replayed {} at offset {}.", message, offset);
    }

    public void replay(AbortTransactionRecord message, long offset) {
        if (currentSnapshotId != null) {
            throw new RuntimeException("AbortTransactionRecord cannot appear within a snapshot.");
        }
        if (transactionStartOffset == -1L) {
            throw new RuntimeException("Can't replay an AbortTransactionRecord at " + offset +
                    " because there is no open transaction.");
        }
        long preTransactionOffset = transactionStartOffset - 1;
        snapshotRegistry.revertToSnapshot(preTransactionOffset);
        transactionStartOffset = -1L;
        log.info("Replayed {} at offset {}. Reverted to offset {}.",
                message, offset, preTransactionOffset);
    }

    // VisibleForTesting
    void setNextWriteOffset(long newNextWriteOffset) {
        this.nextWriteOffset = newNextWriteOffset;
    }
}
