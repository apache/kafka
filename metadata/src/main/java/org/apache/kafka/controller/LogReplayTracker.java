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
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.server.fault.FaultHandler;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.slf4j.Logger;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.BiConsumer;
import java.util.function.Consumer;


/**
 * The LogReplayTracker manages state associated with replaying the metadata log, such as whether
 * we have seen any records. It is accessed solely from the quorum controller thread.
 */
class LogReplayTracker {
    static class Builder {
        private LogContext logContext = null;
        private Time time = Time.SYSTEM;
        private int nodeId = -1;
        private QuorumControllerMetrics metrics = null;
        private Consumer<Integer> gainLeadershipCallback = __ -> { };
        private Consumer<Integer> loseLeadershipCallback = __ -> { };
        private BiConsumer<Long, String> revertCallback = (__, ___) -> { };
        private FaultHandler fatalFaultHandler = null;
        private SnapshotRegistry snapshotRegistry = null;

        Builder setLogContext(LogContext logContext) {
            this.logContext = logContext;
            return this;
        }

        Builder setTime(Time time) {
            this.time = time;
            return this;
        }

        Builder setNodeId(int nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        Builder setMetrics(QuorumControllerMetrics metrics) {
            this.metrics = metrics;
            return this;
        }

        Builder setGainLeadershipCallback(Consumer<Integer> gainLeadershipCallback) {
            this.gainLeadershipCallback = gainLeadershipCallback;
            return this;
        }

        Builder setLoseLeadershipCallback(Consumer<Integer> loseLeadershipCallback) {
            this.loseLeadershipCallback = loseLeadershipCallback;
            return this;
        }

        Builder setRevertCallback(BiConsumer<Long, String> revertCallback) {
            this.revertCallback = revertCallback;
            return this;
        }

        Builder setFatalFaultHandler(FaultHandler fatalFaultHandler) {
            this.fatalFaultHandler = fatalFaultHandler;
            return this;
        }

        Builder setSnapshotRegistry(SnapshotRegistry snapshotRegistry) {
            this.snapshotRegistry = snapshotRegistry;
            return this;
        }

        LogReplayTracker build() {
            if (logContext == null) logContext = new LogContext();
            if (metrics == null) {
                metrics = new QuorumControllerMetrics(Optional.empty(),
                    time,
                    false);
            }
            if (fatalFaultHandler == null) {
                throw new IllegalStateException("You must specify a fatal fault handler.");
            }
            if (snapshotRegistry == null) {
                snapshotRegistry = new SnapshotRegistry(logContext);
            }
            return new LogReplayTracker(logContext,
                time,
                nodeId,
                metrics,
                gainLeadershipCallback,
                loseLeadershipCallback,
                revertCallback,
                fatalFaultHandler,
                snapshotRegistry);
        }
    }

    static String leaderName(OptionalInt leaderId) {
        return leaderId.isPresent() ?
                String.valueOf(leaderId.getAsInt()) :
                "(none)";
    }

    /**
     * The slf4j logger.
     */
    private final Logger log;

    /**
     * The clock to use.
     */
    private final Time time;

    /**
     * The ID of this controller.
     */
    private final int nodeId;

    /**
     * The controller metrics.
     */
    private final QuorumControllerMetrics controllerMetrics;

    /**
     * Invoked whenever we gain leadership.
     */
    private final Consumer<Integer> gainLeadershipCallback;

    /**
     * Invoked whenever we lose leadership.
     */
    private final Consumer<Integer> loseLeadershipCallback;

    /**
     * Invoked when we need to revert to a snapshot.
     */
    private final BiConsumer<Long, String> revertCallback;

    /**
     * The fault handler to invoke if there is a fatal fault.
     */
    private final FaultHandler fatalFaultHandler;

    /**
     * A registry for snapshot data.
     */
    private final SnapshotRegistry snapshotRegistry;

    /**
     * True if we haven't replayed any records yet.
     */
    private boolean empty;

    /**
     * If the controller is active, the current leader epoch; -1 otherwise.
     * Accessible from multiple threads; written only by the controller event handler thread.
     */
    private volatile int curClaimEpoch;

    /**
     * True if the controller is active; false otherwise.
     */
    private boolean active;

    /**
     * The current leader and epoch of the Raft quorum. Note that if the controller voluntarily
     * resigns, there will be a brief period afterwards during which it will be the leader according
     * to Raft, but not according to itself.
     *
     * If the controller voluntarily resigns, this field will stay the same, but active will be set
     * to false. This reflects the fact that we've internally marked ourselves as inactive, but Raft
     * is still catching up.
     */
    private LeaderAndEpoch raftLeader;

    /**
     * If the controller is active, the next log offset to write to; 0 otherwise.
     */
    private long nextWriteOffset;

    /**
     * The last offset we have committed, or -1 if we have not committed any offsets.
     */
    private long lastCommittedOffset;

    /**
     * The epoch of the last offset we have committed, or -1 if we have not committed any offsets.
     */
    private int lastCommittedEpoch;

    /**
     * The timestamp in milliseconds of the last batch we have committed, or -1 if we have not committed any offset.
     */
    private long lastCommittedTimestamp;

    /**
     * If we are inside a transaction, this is its start offset; otherwise, -1.
     */
    private long transactionStartOffset;

    private LogReplayTracker(
        LogContext logContext,
        Time time,
        int nodeId,
        QuorumControllerMetrics controllerMetrics,
        Consumer<Integer> gainLeadershipCallback,
        Consumer<Integer> loseLeadershipCallback,
        BiConsumer<Long, String> revertCallback,
        FaultHandler fatalFaultHandler,
        SnapshotRegistry snapshotRegistry
    ) {
        this.log = logContext.logger(LogReplayTracker.class);
        this.time = time;
        this.nodeId = nodeId;
        this.controllerMetrics = controllerMetrics;
        this.gainLeadershipCallback = gainLeadershipCallback;
        this.loseLeadershipCallback = loseLeadershipCallback;
        this.revertCallback = revertCallback;
        this.fatalFaultHandler = fatalFaultHandler;
        this.snapshotRegistry = snapshotRegistry;
        resetToEmptyState();
    }

    /**
     * Reset all internal state.
     */
    void resetToEmptyState() {
        this.empty = true;
        this.curClaimEpoch = -1;
        this.active = false;
        this.raftLeader = LeaderAndEpoch.UNKNOWN;
        this.nextWriteOffset = 0L;
        this.lastCommittedOffset = -1L;
        this.lastCommittedEpoch = -1;
        this.lastCommittedTimestamp = -1L;
        this.transactionStartOffset = -1L;
        updateLastCommittedState(lastCommittedOffset, lastCommittedEpoch, lastCommittedTimestamp);
    }

    int nodeId() {
        return nodeId;
    }

    boolean empty() {
        return empty;
    }

    int curClaimEpoch() {
        return curClaimEpoch;
    }

    boolean active() {
        return active;
    }

    void setNotEmpty() {
        empty = false;
    }

    void updateLastCommittedState(
        long offset,
        int epoch,
        long timestamp
    ) {
        lastCommittedOffset = offset;
        lastCommittedEpoch = epoch;
        lastCommittedTimestamp = timestamp;
        controllerMetrics.setLastCommittedRecordOffset(lastCommittedOffset);
        if (!active) {
            controllerMetrics.setLastAppliedRecordOffset(lastCommittedOffset);
            controllerMetrics.setLastAppliedRecordTimestamp(lastCommittedTimestamp);
        }
        snapshotRegistry.deleteSnapshotsUpTo(lastStableOffset());
    }

    long lastCommittedOffset() {
        return lastCommittedOffset;
    }

    int lastCommittedEpoch() {
        return lastCommittedEpoch;
    }

    long lastCommittedTimestamp() {
        return lastCommittedTimestamp;
    }

    void updateNextWriteOffset(long newNextWriteOffset) {
        if (!active) {
            throw new RuntimeException("Cannot update the next write offset when we are not active.");
        }
        this.nextWriteOffset = newNextWriteOffset;
        controllerMetrics.setLastAppliedRecordOffset(newNextWriteOffset - 1);
        // This is not truly the append timestamp. The KRaft client doesn't expose the append
        // time when scheduling a write. Using the current time is good enough, because this
        // is called right after the records were given to the KRaft client for appending, and
        // the default append linger for KRaft is 25ms.
        controllerMetrics.setLastAppliedRecordTimestamp(time.milliseconds());
    }

    /**
     * Called by QuorumController when the controller wants to voluntarily resign.
     */
    void resign() {
        if (!active) {
            if (raftLeader.isLeader(nodeId)) {
                throw fatalFaultHandler.handleFault("Cannot resign at epoch " + raftLeader.epoch() +
                        ", because we already resigned.");
            } else {
                throw fatalFaultHandler.handleFault("Cannot resign at epoch " + raftLeader.epoch() +
                        ", because we are not the Raft leader at this epoch.");
            }
        }
        log.warn("Resigning as the active controller at epoch {}. Reverting to last " +
                "committed offset {}.", curClaimEpoch, lastCommittedOffset);
        becomeInactive(raftLeader.epoch());
    }

    void handleLeaderChange(
        LeaderAndEpoch newRaftLeader,
        long newNextWriteOffset
    ) {
        if (newRaftLeader.leaderId().isPresent()) {
            controllerMetrics.incrementNewActiveControllers();
        }
        if (active) {
            if (newRaftLeader.isLeader(nodeId)) {
                handleLeaderChangeActiveToActive(newRaftLeader, newNextWriteOffset);
            } else {
                handleLeaderChangeActiveToStandby(newRaftLeader);
            }
        } else {
            if (newRaftLeader.isLeader(nodeId)) {
                handleLeaderChangeStandbyToActive(newRaftLeader, newNextWriteOffset);
            } else {
                handleLeaderChangeStandbyToStandby(newRaftLeader);
            }
        }
    }

    private void handleLeaderChangeActiveToActive(
        LeaderAndEpoch newRaftLeader,
        long newNextWriteOffset
    ) {
        log.warn("We were the leader in epoch {}, and are still the leader in the new epoch {}.",
                raftLeader.epoch(), newRaftLeader.epoch());
        becomeActive(newRaftLeader, newNextWriteOffset);
    }

    private void handleLeaderChangeActiveToStandby(
        LeaderAndEpoch newRaftLeader
    ) {
        log.warn("Renouncing the leadership due to a metadata log event. " +
            "We were the leader at epoch {}, but in the new epoch {}, " +
            "the leader is {}. Reverting to last committed offset {}.",
            raftLeader.epoch(),
            newRaftLeader.epoch(), 
            leaderName(newRaftLeader.leaderId()),
            lastCommittedOffset);
        this.raftLeader = newRaftLeader;
        becomeInactive(raftLeader.epoch());
    }

    private void handleLeaderChangeStandbyToActive(
        LeaderAndEpoch newRaftLeader,
        long newNextWriteOffset
    ) {
        log.info("Becoming the active controller at epoch {}, new next write offset {}.",
            newRaftLeader.epoch(), newNextWriteOffset);
        becomeActive(newRaftLeader, newNextWriteOffset);
    }

    private void handleLeaderChangeStandbyToStandby(
        LeaderAndEpoch newRaftLeader
    ) {
        log.info("In the new epoch {}, the leader is {}.",
                newRaftLeader.epoch(), leaderName(newRaftLeader.leaderId()));
        this.raftLeader = newRaftLeader;
    }

    private void becomeActive(
        LeaderAndEpoch newRaftLeader,
        long newNextWriteOffset
    ) {
        this.active = true;
        this.raftLeader = newRaftLeader;
        this.curClaimEpoch = newRaftLeader.epoch();
        updateNextWriteOffset(newNextWriteOffset);
        controllerMetrics.setActive(true);

        // Before switching to active, create an in-memory snapshot at the last committed
        // offset. This is required because the active controller assumes that there is always
        // an in-memory snapshot at the last committed offset.
        snapshotRegistry.getOrCreateSnapshot(lastCommittedOffset());

        try {
            gainLeadershipCallback.accept(newRaftLeader.epoch());
        } catch (Throwable e) {
            fatalFaultHandler.handleFault("exception while claiming leadership", e);
        }
    }

    private void becomeInactive(int epoch) {
        this.active = false;
        this.nextWriteOffset = 0L;
        this.curClaimEpoch = -1;
        controllerMetrics.setActive(false);
        revert(lastCommittedOffset, "becoming inactive");
        try {
            loseLeadershipCallback.accept(epoch);
        } catch (Throwable e) {
            fatalFaultHandler.handleFault("exception in loseLeadership callback", e);
        }
    }

    /**
     * Find the latest offset that is safe to perform read operations at.
     *
     * @return The offset.
     */
    long lastStableOffset() {
        // Calculate what the stable offset would be if metadata transactions did not exist.
        //
        // On the active controller, this is the lastCommittedOffset. On the standby controller,
        // it is a special value indicating that we should always read the very latest data from
        // all data structures. The reason for this difference is that the standby never has
        // uncommitted data in memory. Using lastCommittedOffset for the standby would not work
        // because the standby (unlike the active) doesn't keep in-memory snapshots at every
        // committed offset at all times.
        long stableOffsetIgnoringTransactions = active ?
                lastCommittedOffset :
                SnapshotRegistry.LATEST_EPOCH;

        if (transactionStartOffset == -1) {
            // If we're not inside a transaction, use the offset calculated above.
            return stableOffsetIgnoringTransactions;
        } else {
            // If we're inside a transaction and it started before the above offset, use the
            // transaction start offset instead.
            return Math.min(stableOffsetIgnoringTransactions, transactionStartOffset);
        }
    }

    LeaderAndEpoch raftLeader() {
        return raftLeader;
    }

    long nextWriteOffset() {
        if (!active) {
            throw new RuntimeException("Cannot access the next write offset when we are not active.");
        }
        return nextWriteOffset;
    }

    QuorumControllerMetrics controllerMetrics() {
        return controllerMetrics;
    }

    void replay(BeginTransactionRecord record, long offset) {
        if (transactionStartOffset != -1L) {
            throw fatalFaultHandler.handleFault("tried to begin metadata transaction at " + offset +
                ", but we are still inside a metadata transaction starting at " +
                transactionStartOffset);
        }
        log.debug("Beginning metadata transaction at offset {}{}.", offset,
            record.name() == null ? "" : " named " + record.name());
        snapshotRegistry.getOrCreateSnapshot(offset);
        transactionStartOffset = offset;
    }

    void replay(EndTransactionRecord record, long offset) {
        if (transactionStartOffset != offset) {
            if (transactionStartOffset == -1L) {
                throw fatalFaultHandler.handleFault("tried to end metadata transaction at " + offset +
                        ", but we are not currently inside a metadata transaction.");
            } else {
                throw fatalFaultHandler.handleFault("tried to end metadata transaction at " + offset +
                        ", but the current transaction started at " + transactionStartOffset);
            }
        }
        log.debug("Successfully ending metadata transaction at offset {}.", offset);
        transactionStartOffset = -1L;
    }

    void replay(AbortTransactionRecord record, long offset) {
        if (transactionStartOffset != offset) {
            if (transactionStartOffset == -1L) {
                throw fatalFaultHandler.handleFault("tried to abort metadata transaction at " + offset +
                        ", but we are not currently inside a metadata transaction.");
            } else {
                throw fatalFaultHandler.handleFault("tried to abort metadata transaction at " + offset +
                        ", but the current transaction started at " + transactionStartOffset);
            }
        }
        log.warn("Aborting metadata transaction at offset {}.", offset);
        transactionStartOffset = -1L;
        revert(offset, "aborting transaction");
    }

    void revert(long offset, String purpose) {
        try {
            revertCallback.accept(offset, purpose);
        } catch (Throwable e) {
            fatalFaultHandler.handleFault("exception in revertCallback", e);
        }
    }
}
