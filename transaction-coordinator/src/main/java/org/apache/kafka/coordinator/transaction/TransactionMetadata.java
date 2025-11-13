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
package org.apache.kafka.coordinator.transaction;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.LogLevelConfig;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.server.common.TransactionVersion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MarkerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

public class TransactionMetadata {
    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionMetadata.class);
    private final String transactionalId;
    private long producerId;
    private long prevProducerId;
    private long nextProducerId;
    private short producerEpoch;
    private short lastProducerEpoch;
    private int txnTimeoutMs;
    private TransactionState state;
    // The topicPartitions is mutable, so using HashSet, instead of Set.
    private HashSet<TopicPartition> topicPartitions;
    private volatile long txnStartTimestamp;
    private volatile long txnLastUpdateTimestamp;
    private TransactionVersion clientTransactionVersion;

    // pending state is used to indicate the state that this transaction is going to
    // transit to, and for blocking future attempts to transit it again if it is not legal;
    // initialized as the same as the current state
    private Optional<TransactionState> pendingState;

    // Indicates that during a previous attempt to fence a producer, the bumped epoch may not have been
    // successfully written to the log. If this is true, we will not bump the epoch again when fencing
    private boolean hasFailedEpochFence;

    private final ReentrantLock lock;

    public static boolean isEpochExhausted(short producerEpoch) {
        return producerEpoch >= Short.MAX_VALUE - 1;
    }

    /**
     * @param transactionalId          transactional id
     * @param producerId               producer id
     * @param prevProducerId           producer id for the last committed transaction with this transactional ID
     * @param nextProducerId           Latest producer ID sent to the producer for the given transactional ID
     * @param producerEpoch            current epoch of the producer
     * @param lastProducerEpoch        last epoch of the producer
     * @param txnTimeoutMs             timeout to be used to abort long running transactions
     * @param state                    current state of the transaction
     * @param topicPartitions          current set of partitions that are part of this transaction
     * @param txnStartTimestamp        time the transaction was started, i.e., when first partition is added
     * @param txnLastUpdateTimestamp   updated when any operation updates the TransactionMetadata. To be used for expiration
     * @param clientTransactionVersion TransactionVersion used by the client when the state was transitioned
     */
    public TransactionMetadata(String transactionalId,
                               long producerId,
                               long prevProducerId,
                               long nextProducerId,
                               short producerEpoch,
                               short lastProducerEpoch,
                               int txnTimeoutMs,
                               TransactionState state,
                               Set<TopicPartition> topicPartitions,
                               long txnStartTimestamp,
                               long txnLastUpdateTimestamp,
                               TransactionVersion clientTransactionVersion) {
        this.transactionalId = transactionalId;
        this.producerId = producerId;
        this.prevProducerId = prevProducerId;
        this.nextProducerId = nextProducerId;
        this.producerEpoch = producerEpoch;
        this.lastProducerEpoch = lastProducerEpoch;
        this.txnTimeoutMs = txnTimeoutMs;
        this.state = state;
        this.topicPartitions = new HashSet<>(topicPartitions);
        this.txnStartTimestamp = txnStartTimestamp;
        this.txnLastUpdateTimestamp = txnLastUpdateTimestamp;
        this.clientTransactionVersion = clientTransactionVersion;
        this.pendingState = Optional.empty();
        this.hasFailedEpochFence = false;
        this.lock = new ReentrantLock();
    }

    public <T> T inLock(Supplier<T> function) {
        lock.lock();
        try {
            return function.get();
        } finally {
            lock.unlock();
        }
    }

    // VisibleForTesting
    public void addPartitions(Collection<TopicPartition> partitions) {
        topicPartitions.addAll(partitions);
    }

    public void removePartition(TopicPartition topicPartition) {
        if (state != TransactionState.PREPARE_COMMIT && state != TransactionState.PREPARE_ABORT)
            throw new IllegalStateException("Transaction metadata's current state is " + state + ", and its pending state is " +
                pendingState + " while trying to remove partitions whose txn marker has been sent, this is not expected");

        topicPartitions.remove(topicPartition);
    }

    // this is visible for test only
    public TxnTransitMetadata prepareNoTransit() {
        // do not call transitTo as it will set the pending state, a follow-up call to abort the transaction will set its pending state
        return new TxnTransitMetadata(producerId, prevProducerId, nextProducerId, producerEpoch, lastProducerEpoch, txnTimeoutMs,
            state, new HashSet<>(topicPartitions), txnStartTimestamp, txnLastUpdateTimestamp, clientTransactionVersion);
    }

    public TxnTransitMetadata prepareFenceProducerEpoch() {
        if (producerEpoch == Short.MAX_VALUE)
            throw new IllegalStateException("Cannot fence producer with epoch equal to Short.MaxValue since this would overflow");

        // If we've already failed to fence an epoch (because the write to the log failed), we don't increase it again.
        // This is safe because we never return the epoch to client if we fail to fence the epoch
        short bumpedEpoch = hasFailedEpochFence ? producerEpoch : (short) (producerEpoch + 1);

        TransitionData data = new TransitionData(TransactionState.PREPARE_EPOCH_FENCE);
        data.producerEpoch = bumpedEpoch;
        return prepareTransitionTo(data);
    }

    public TxnTransitMetadata prepareIncrementProducerEpoch(
        int newTxnTimeoutMs,
        Optional<Short> expectedProducerEpoch,
        long updateTimestamp) {
        if (isProducerEpochExhausted())
            throw new IllegalStateException("Cannot allocate any more producer epochs for producerId " + producerId);

        TransitionData data = new TransitionData(TransactionState.EMPTY);
        short bumpedEpoch = (short) (producerEpoch + 1);
        if (expectedProducerEpoch.isEmpty()) {
            // If no expected epoch was provided by the producer, bump the current epoch and set the last epoch to -1
            // In the case of a new producer, producerEpoch will be -1 and bumpedEpoch will be 0
            data.producerEpoch = bumpedEpoch;
            data.lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH;
        } else if (producerEpoch == RecordBatch.NO_PRODUCER_EPOCH || expectedProducerEpoch.get() == producerEpoch) {
            // If the expected epoch matches the current epoch, or if there is no current epoch, the producer is attempting
            // to continue after an error and no other producer has been initialized. Bump the current and last epochs.
            // The no current epoch case means this is a new producer; producerEpoch will be -1 and bumpedEpoch will be 0
            data.producerEpoch = bumpedEpoch;
            data.lastProducerEpoch = producerEpoch;
        } else if (expectedProducerEpoch.get() == lastProducerEpoch) {
            // If the expected epoch matches the previous epoch, it is a retry of a successful call, so just return the
            // current epoch without bumping. There is no danger of this producer being fenced, because a new producer
            // calling InitProducerId would have caused the last epoch to be set to -1.
            // Note that if the IBP is prior to 2.4.IV1, the lastProducerId and lastProducerEpoch will not be written to
            // the transaction log, so a retry that spans a coordinator change will fail. We expect this to be a rare case.
            data.producerEpoch = producerEpoch;
            data.lastProducerEpoch = lastProducerEpoch;
        } else {
            // Otherwise, the producer has a fenced epoch and should receive an PRODUCER_FENCED error
            LOGGER.info("Expected producer epoch {} does not match current producer epoch {} or previous producer epoch {}",
                expectedProducerEpoch.get(), producerEpoch, lastProducerEpoch);
            throw Errors.PRODUCER_FENCED.exception();
        }

        data.txnTimeoutMs = newTxnTimeoutMs;
        data.topicPartitions = new HashSet<>();
        data.txnStartTimestamp = -1L;
        data.txnLastUpdateTimestamp = updateTimestamp;
        return prepareTransitionTo(data);
    }

    public TxnTransitMetadata prepareProducerIdRotation(long newProducerId,
                                                        int newTxnTimeoutMs,
                                                        long updateTimestamp,
                                                        boolean recordLastEpoch) {
        if (hasPendingTransaction())
            throw new IllegalStateException("Cannot rotate producer ids while a transaction is still pending");

        TransitionData data = new TransitionData(TransactionState.EMPTY);
        data.producerId = newProducerId;
        data.producerEpoch = 0;
        data.lastProducerEpoch = recordLastEpoch ? producerEpoch : RecordBatch.NO_PRODUCER_EPOCH;
        data.txnTimeoutMs = newTxnTimeoutMs;
        data.topicPartitions = new HashSet<>();
        data.txnStartTimestamp = -1L;
        data.txnLastUpdateTimestamp = updateTimestamp;
        return prepareTransitionTo(data);
    }

    public TxnTransitMetadata prepareAddPartitions(Set<TopicPartition> addedTopicPartitions,
                                                   long updateTimestamp,
                                                   TransactionVersion clientTransactionVersion) {
        long newTxnStartTimestamp;
        if (state == TransactionState.EMPTY || state == TransactionState.COMPLETE_ABORT || state == TransactionState.COMPLETE_COMMIT) {
            newTxnStartTimestamp = updateTimestamp;
        } else {
            newTxnStartTimestamp = txnStartTimestamp;
        }

        HashSet<TopicPartition> newTopicPartitions = new HashSet<>(topicPartitions);
        newTopicPartitions.addAll(addedTopicPartitions);

        TransitionData data = new TransitionData(TransactionState.ONGOING);
        data.topicPartitions = newTopicPartitions;
        data.txnStartTimestamp = newTxnStartTimestamp;
        data.txnLastUpdateTimestamp = updateTimestamp;
        data.clientTransactionVersion = clientTransactionVersion;
        return prepareTransitionTo(data);
    }

    public TxnTransitMetadata prepareAbortOrCommit(TransactionState newState,
                                                   TransactionVersion clientTransactionVersion,
                                                   long nextProducerId,
                                                   long updateTimestamp,
                                                   boolean noPartitionAdded) {
        TransitionData data = new TransitionData(newState);
        if (clientTransactionVersion.supportsEpochBump()) {
            // We already ensured that we do not overflow here. MAX_SHORT is the highest possible value.
            data.producerEpoch = (short) (producerEpoch + 1);
            data.lastProducerEpoch = producerEpoch;
        } else {
            data.producerEpoch = producerEpoch;
            data.lastProducerEpoch = lastProducerEpoch;
        }

        // With transaction V2, it is allowed to abort the transaction without adding any partitions. Then, the transaction
        // start time is uncertain but it is still required. So we can use the update time as the transaction start time.
        data.txnStartTimestamp = noPartitionAdded ? updateTimestamp : txnStartTimestamp;
        data.nextProducerId = nextProducerId;
        data.txnLastUpdateTimestamp = updateTimestamp;
        data.clientTransactionVersion = clientTransactionVersion;
        return prepareTransitionTo(data);
    }

    public TxnTransitMetadata prepareComplete(long updateTimestamp) {
        // Since the state change was successfully written to the log, unset the flag for a failed epoch fence
        hasFailedEpochFence = false;

        TransitionData data = new TransitionData(state == TransactionState.PREPARE_COMMIT ?
            TransactionState.COMPLETE_COMMIT : TransactionState.COMPLETE_ABORT);
        // In the prepareComplete transition for the overflow case, the lastProducerEpoch is kept at MAX-1,
        // which is the last epoch visible to the client.
        // Internally, however, during the transition between prepareAbort/prepareCommit and prepareComplete, the producer epoch
        // reaches MAX but the client only sees the transition as MAX-1 followed by 0.
        // When an epoch overflow occurs, we set the producerId to nextProducerId and reset the epoch to 0,
        // but lastProducerEpoch remains MAX-1 to maintain consistency with what the client last saw.
        if (clientTransactionVersion.supportsEpochBump() && nextProducerId != RecordBatch.NO_PRODUCER_ID) {
            data.producerId = nextProducerId;
            data.producerEpoch = 0;
        } else {
            data.producerId = producerId;
            data.producerEpoch = producerEpoch;
        }
        data.nextProducerId = RecordBatch.NO_PRODUCER_ID;
        data.topicPartitions = new HashSet<>();
        data.txnLastUpdateTimestamp = updateTimestamp;
        return prepareTransitionTo(data);
    }

    public TxnTransitMetadata prepareDead() {
        TransitionData data = new TransitionData(TransactionState.DEAD);
        data.topicPartitions = new HashSet<>();
        return prepareTransitionTo(data);
    }

    /**
     * Check if the epochs have been exhausted for the current producerId. We do not allow the client to use an
     * epoch equal to Short.MaxValue to ensure that the coordinator will always be able to fence an existing producer.
     */
    public boolean isProducerEpochExhausted() {
        return isEpochExhausted(producerEpoch);
    }

    /**
     * Check if this is a distributed two phase commit transaction.
     * Such transactions have no timeout (identified by maximum value for timeout).
     */
    public boolean isDistributedTwoPhaseCommitTxn() {
        return txnTimeoutMs == Integer.MAX_VALUE;
    }

    private boolean hasPendingTransaction() {
        return state == TransactionState.ONGOING ||
            state == TransactionState.PREPARE_ABORT ||
            state == TransactionState.PREPARE_COMMIT;
    }

    private TxnTransitMetadata prepareTransitionTo(TransitionData data) {
        if (pendingState.isPresent())
            throw new IllegalStateException("Preparing transaction state transition to " + state +
                " while it already a pending state " + pendingState.get());

        if (data.producerId < 0)
            throw new IllegalArgumentException("Illegal new producer id " + data.producerId);

        // The epoch is initialized to NO_PRODUCER_EPOCH when the TransactionMetadata
        // is created for the first time and it could stay like this until transitioning
        // to Dead.
        if (data.state != TransactionState.DEAD && data.producerEpoch < 0)
            throw new IllegalArgumentException("Illegal new producer epoch " + data.producerEpoch);

        // check that the new state transition is valid and update the pending state if necessary
        if (data.state.validPreviousStates().contains(this.state)) {
            TxnTransitMetadata transitMetadata = new TxnTransitMetadata(
                data.producerId, this.producerId, data.nextProducerId, data.producerEpoch, data.lastProducerEpoch,
                data.txnTimeoutMs, data.state, data.topicPartitions,
                data.txnStartTimestamp, data.txnLastUpdateTimestamp, data.clientTransactionVersion
            );

            LOGGER.debug("TransactionalId {} prepare transition from {} to {}", transactionalId, this.state, data.state);
            pendingState = Optional.of(data.state);
            return transitMetadata;
        }
        throw new IllegalStateException("Preparing transaction state transition to " + data.state + " failed since the target state " +
            data.state + " is not a valid previous state of the current state " + this.state);
    }

    @SuppressWarnings("CyclomaticComplexity")
    public void completeTransitionTo(TxnTransitMetadata transitMetadata) {
        // metadata transition is valid only if all the following conditions are met:
        //
        // 1. the new state is already indicated in the pending state.
        // 2. the epoch should be either the same value, the old value + 1, or 0 if we have a new producerId.
        // 3. the last update time is no smaller than the old value.
        // 4. the old partitions set is a subset of the new partitions set.
        //
        // plus, we should only try to update the metadata after the corresponding log entry has been successfully
        // written and replicated (see TransactionStateManager#appendTransactionToLog)
        //
        // if valid, transition is done via overwriting the whole object to ensure synchronization

        TransactionState toState = pendingState.orElseThrow(() -> {
            LOGGER.error(MarkerFactory.getMarker(LogLevelConfig.FATAL_LOG_LEVEL),
                "{}'s transition to {} failed since pendingState is not defined: this should not happen", this, transitMetadata);
            return new IllegalStateException("TransactionalId " + transactionalId +
                " completing transaction state transition while it does not have a pending state");
        });

        if (!toState.equals(transitMetadata.txnState())) throwStateTransitionFailure(transitMetadata);

        switch (toState) {
            case EMPTY: // from initPid
                if ((producerEpoch != transitMetadata.producerEpoch() && !validProducerEpochBump(transitMetadata)) ||
                    !transitMetadata.topicPartitions().isEmpty() ||
                    transitMetadata.txnStartTimestamp() != -1) {
                    throwStateTransitionFailure(transitMetadata);
                }
                break;

            case ONGOING: // from addPartitions
                if (!validProducerEpoch(transitMetadata) ||
                    !transitMetadata.topicPartitions().containsAll(topicPartitions) ||
                    txnTimeoutMs != transitMetadata.txnTimeoutMs()) {
                    throwStateTransitionFailure(transitMetadata);
                }
                break;

            case PREPARE_ABORT: // from endTxn
            case PREPARE_COMMIT:
                // In V2, we allow state transits from Empty, CompleteCommit and CompleteAbort to PrepareAbort. It is possible
                // their updated start time is not equal to the current start time.
                boolean allowedEmptyAbort = toState == TransactionState.PREPARE_ABORT && transitMetadata.clientTransactionVersion().supportsEpochBump() &&
                    (state == TransactionState.EMPTY || state == TransactionState.COMPLETE_COMMIT || state == TransactionState.COMPLETE_ABORT);
                boolean validTimestamp = txnStartTimestamp == transitMetadata.txnStartTimestamp() || allowedEmptyAbort;

                if (!validProducerEpoch(transitMetadata) ||
                    !topicPartitions.equals(transitMetadata.topicPartitions()) ||
                    txnTimeoutMs != transitMetadata.txnTimeoutMs() ||
                    !validTimestamp) {
                    throwStateTransitionFailure(transitMetadata);
                }
                break;

            case COMPLETE_ABORT: // from write markers
            case COMPLETE_COMMIT:
                if (!validProducerEpoch(transitMetadata) ||
                    txnTimeoutMs != transitMetadata.txnTimeoutMs() ||
                    transitMetadata.txnStartTimestamp() == -1) {
                    throwStateTransitionFailure(transitMetadata);
                }
                break;

            case PREPARE_EPOCH_FENCE:
                // We should never get here, since once we prepare to fence the epoch, we immediately set the pending state
                // to PrepareAbort, and then consequently to CompleteAbort after the markers are written.. So we should never
                // ever try to complete a transition to PrepareEpochFence, as it is not a valid previous state for any other state, and hence
                // can never be transitioned out of.
                throwStateTransitionFailure(transitMetadata);
                break;

            case DEAD:
                // The transactionalId was being expired. The completion of the operation should result in removal of the
                // the metadata from the cache, so we should never realistically transition to the dead state.
                throw new IllegalStateException("TransactionalId " + transactionalId + " is trying to complete a transition to " +
                    toState + ". This means that the transactionalId was being expired, and the only acceptable completion of " +
                    "this operation is to remove the transaction metadata from the cache, not to persist the " + toState + " in the log.");

            default:
                break;
        }

        LOGGER.debug("TransactionalId {} complete transition from {} to {}", transactionalId, state, transitMetadata);
        producerId = transitMetadata.producerId();
        prevProducerId = transitMetadata.prevProducerId();
        nextProducerId = transitMetadata.nextProducerId();
        producerEpoch = transitMetadata.producerEpoch();
        lastProducerEpoch = transitMetadata.lastProducerEpoch();
        txnTimeoutMs = transitMetadata.txnTimeoutMs();
        topicPartitions = transitMetadata.topicPartitions();
        txnStartTimestamp = transitMetadata.txnStartTimestamp();
        txnLastUpdateTimestamp = transitMetadata.txnLastUpdateTimestamp();
        clientTransactionVersion = transitMetadata.clientTransactionVersion();

        pendingState = Optional.empty();
        state = toState;
    }

    /**
     * Validates the producer epoch and ID based on transaction state and version.
     * <p>
     * Logic:
     * * 1. **Overflow Case in Transactions V2:**
     * *    - During overflow (epoch reset to 0), we compare both `lastProducerEpoch` values since it
     * *      does not change during completion.
     * *    - For PrepareComplete, the producer ID has been updated. We ensure that the `prevProducerID`
     * *      in the transit metadata matches the current producer ID, confirming the change.
     * *
     * * 2. **Epoch Bump Case in Transactions V2:**
     * *    - For PrepareCommit or PrepareAbort, the producer epoch has been bumped. We ensure the `lastProducerEpoch`
     * *      in transit metadata matches the current producer epoch, confirming the bump.
     * *    - We also verify that the producer ID remains the same.
     * *
     * * 3. **Other Cases:**
     * *    - For other states and versions, check if the producer epoch and ID match the current values.
     *
     * @param transitMetadata The transaction transition metadata containing state, producer epoch, and ID.
     * @return true if the producer epoch and ID are valid; false otherwise.
     */
    private boolean validProducerEpoch(TxnTransitMetadata transitMetadata) {
        boolean isAtLeastTransactionsV2 = transitMetadata.clientTransactionVersion().supportsEpochBump();
        TransactionState txnState = transitMetadata.txnState();
        short transitProducerEpoch = transitMetadata.producerEpoch();
        long transitProducerId = transitMetadata.producerId();
        short transitLastProducerEpoch = transitMetadata.lastProducerEpoch();

        if (isAtLeastTransactionsV2 &&
            (txnState == TransactionState.COMPLETE_COMMIT || txnState == TransactionState.COMPLETE_ABORT) &&
            transitProducerEpoch == 0) {
            return transitLastProducerEpoch == lastProducerEpoch && transitMetadata.prevProducerId() == producerId;
        }

        if (isAtLeastTransactionsV2 &&
            (txnState == TransactionState.PREPARE_COMMIT || txnState == TransactionState.PREPARE_ABORT)) {
            return transitLastProducerEpoch == producerEpoch && transitProducerId == producerId;
        }
        return transitProducerEpoch == producerEpoch && transitProducerId == producerId;
    }

    private boolean validProducerEpochBump(TxnTransitMetadata transitMetadata) {
        short transitEpoch = transitMetadata.producerEpoch();
        long transitProducerId = transitMetadata.producerId();
        return transitEpoch == (short) (producerEpoch + 1) || (transitEpoch == 0 && transitProducerId != producerId);
    }

    private void throwStateTransitionFailure(TxnTransitMetadata txnTransitMetadata) {
        LOGGER.error(MarkerFactory.getMarker(LogLevelConfig.FATAL_LOG_LEVEL),
            "{}'s transition to {} failed: this should not happen", this, txnTransitMetadata);

        throw new IllegalStateException("TransactionalId " + transactionalId + " failed transition to state " + txnTransitMetadata +
            " due to unexpected metadata");
    }

    public boolean pendingTransitionInProgress() {
        return pendingState.isPresent();
    }

    public String transactionalId() {
        return transactionalId;
    }

    // VisibleForTesting
    public void setProducerId(long producerId) {
        this.producerId = producerId;
    }
    public long producerId() {
        return producerId;
    }

    // VisibleForTesting
    public void setPrevProducerId(long prevProducerId) {
        this.prevProducerId = prevProducerId;
    }
    public long prevProducerId() {
        return prevProducerId;
    }

    public void setProducerEpoch(short producerEpoch) {
        this.producerEpoch = producerEpoch;
    }

    public short producerEpoch() {
        return producerEpoch;
    }

    public void setLastProducerEpoch(short lastProducerEpoch) {
        this.lastProducerEpoch = lastProducerEpoch;
    }

    public short lastProducerEpoch() {
        return lastProducerEpoch;
    }

    public int txnTimeoutMs() {
        return txnTimeoutMs;
    }

    // VisibleForTesting
    public void state(TransactionState state) {
        this.state = state;
    }

    public TransactionState state() {
        return state;
    }

    public Set<TopicPartition> topicPartitions() {
        return topicPartitions;
    }

    public long txnStartTimestamp() {
        return txnStartTimestamp;
    }

    // VisibleForTesting
    public void txnLastUpdateTimestamp(long txnLastUpdateTimestamp) {
        this.txnLastUpdateTimestamp = txnLastUpdateTimestamp;
    }

    public long txnLastUpdateTimestamp() {
        return txnLastUpdateTimestamp;
    }

    public void clientTransactionVersion(TransactionVersion clientTransactionVersion) {
        this.clientTransactionVersion = clientTransactionVersion;
    }

    public TransactionVersion clientTransactionVersion() {
        return clientTransactionVersion;
    }

    public void pendingState(Optional<TransactionState> pendingState) {
        this.pendingState = pendingState;
    }

    public Optional<TransactionState> pendingState() {
        return pendingState;
    }

    public void hasFailedEpochFence(boolean hasFailedEpochFence) {
        this.hasFailedEpochFence = hasFailedEpochFence;
    }

    public boolean hasFailedEpochFence() {
        return hasFailedEpochFence;
    }

    @Override
    public String toString() {
        return "TransactionMetadata(" +
            "transactionalId=" + transactionalId +
            ", producerId=" + producerId +
            ", prevProducerId=" + prevProducerId +
            ", nextProducerId=" + nextProducerId +
            ", producerEpoch=" + producerEpoch +
            ", lastProducerEpoch=" + lastProducerEpoch +
            ", txnTimeoutMs=" + txnTimeoutMs +
            ", state=" + state +
            ", pendingState=" + pendingState +
            ", topicPartitions=" + topicPartitions +
            ", txnStartTimestamp=" + txnStartTimestamp +
            ", txnLastUpdateTimestamp=" + txnLastUpdateTimestamp +
            ", clientTransactionVersion=" + clientTransactionVersion +
            ")";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        TransactionMetadata other = (TransactionMetadata) obj;
        return transactionalId.equals(other.transactionalId) &&
            producerId == other.producerId &&
            prevProducerId == other.prevProducerId &&
            nextProducerId == other.nextProducerId &&
            producerEpoch == other.producerEpoch &&
            lastProducerEpoch == other.lastProducerEpoch &&
            txnTimeoutMs == other.txnTimeoutMs &&
            state.equals(other.state) &&
            topicPartitions.equals(other.topicPartitions) &&
            txnStartTimestamp == other.txnStartTimestamp &&
            txnLastUpdateTimestamp == other.txnLastUpdateTimestamp &&
            clientTransactionVersion.equals(other.clientTransactionVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            transactionalId,
            producerId,
            prevProducerId,
            nextProducerId,
            producerEpoch,
            lastProducerEpoch,
            txnTimeoutMs,
            state,
            topicPartitions,
            txnStartTimestamp,
            txnLastUpdateTimestamp,
            clientTransactionVersion
        );
    }

    /**
     * This class is used to hold the data that is needed to transition the transaction metadata to a new state.
     * The data is copied from the current transaction metadata to avoid a lot of duplicated code in the prepare methods.
     */
    private class TransitionData {
        final TransactionState state;
        long producerId = TransactionMetadata.this.producerId;
        long nextProducerId = TransactionMetadata.this.nextProducerId;
        short producerEpoch = TransactionMetadata.this.producerEpoch;
        short lastProducerEpoch = TransactionMetadata.this.lastProducerEpoch;
        int txnTimeoutMs = TransactionMetadata.this.txnTimeoutMs;
        HashSet<TopicPartition> topicPartitions = TransactionMetadata.this.topicPartitions;
        long txnStartTimestamp = TransactionMetadata.this.txnStartTimestamp;
        long txnLastUpdateTimestamp = TransactionMetadata.this.txnLastUpdateTimestamp;
        TransactionVersion clientTransactionVersion = TransactionMetadata.this.clientTransactionVersion;

        private TransitionData(TransactionState state) {
            this.state = state;
        }
    }
}