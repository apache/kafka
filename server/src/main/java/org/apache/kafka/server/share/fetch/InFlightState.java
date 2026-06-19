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
package org.apache.kafka.server.share.fetch;


import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.common.requests.TransactionResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * The InFlightState is used to track the state and delivery count of a record that has been
 * fetched from the leader. The state of the record is used to determine if the record should
 * be re-deliver or if it can be acknowledged or archived.
 * <p>
 * This class is not thread-safe and caller should attain locks if concurrent updates on same state
 * is expected.
 */
public class InFlightState {

    private static final Logger log = LoggerFactory.getLogger(InFlightState.class);

    /**
     * empty member id used to indicate when a record is not acquired by any member.
     */
    public static final String EMPTY_MEMBER_ID = "";

    // The state of the fetch batch records.
    private RecordState state;
    // The number of times the records has been delivered to the client.
    private int deliveryCount;
    // The member id of the client that is fetching/acknowledging the record.
    private String memberId;
    // The state of the records before the transition. In case we need to revert an in-flight state, we revert the above
    // attributes of InFlightState to this state, namely - state, deliveryCount and memberId.
    private RollbackState rollbackState;
    // The timer task for the acquisition lock timeout.
    private AcquisitionLockTimerTask acquisitionLockTimeoutTask;
    // The boolean determines if the record has achieved a terminal state of ARCHIVED from which it cannot transition
    // to any other state. This could happen because of LSO movement etc.
    private boolean isTerminalState = false;

    // Populated only when state == TX_PENDING; identifies the transaction owner that staged this ack.
    private long stagedTxnOwnerId = -1L;
    private short stagedTxnOwnerEpoch = -1;
    private byte stagedAckType = -1;
    private byte stagedDeliveryState = -1;

    // Visible for testing.
    public InFlightState(RecordState state, int deliveryCount, String memberId) {
        this(state, deliveryCount, memberId, null);
    }

    InFlightState(RecordState state, int deliveryCount, String memberId, AcquisitionLockTimerTask acquisitionLockTimeoutTask) {
        this(state, deliveryCount, memberId, acquisitionLockTimeoutTask, -1L, (short) -1, (byte) -1, (byte) -1);
    }

    InFlightState(
        RecordState state,
        int deliveryCount,
        String memberId,
        AcquisitionLockTimerTask acquisitionLockTimeoutTask,
        long stagedTxnOwnerId,
        short stagedTxnOwnerEpoch,
        byte stagedAckType,
        byte stagedDeliveryState
    ) {
        this.state = state;
        this.deliveryCount = deliveryCount;
        this.memberId = memberId;
        this.acquisitionLockTimeoutTask = acquisitionLockTimeoutTask;
        this.stagedTxnOwnerId = stagedTxnOwnerId;
        this.stagedTxnOwnerEpoch = stagedTxnOwnerEpoch;
        this.stagedAckType = stagedAckType;
        this.stagedDeliveryState = stagedDeliveryState;
    }

    /**
     * @return The current state of the record.
     */
    public RecordState state() {
        return state;
    }

    /**
     * @return The number of times the record has been delivered.
     */
    public int deliveryCount() {
        return deliveryCount;
    }

    /**
     * @return The member id of the client that is fetching/acknowledging the record.
     */
    public String memberId() {
        return memberId;
    }

    /**
     * @return The timer task for the acquisition lock timeout.
     */
    public AcquisitionLockTimerTask acquisitionLockTimeoutTask() {
        return acquisitionLockTimeoutTask;
    }

    /**
     * Update the acquisition lock timeout task. This method is used to set the acquisition lock
     * timeout task for the record. If there is already an acquisition lock timeout task set,
     * it throws an IllegalArgumentException.
     *
     * @param acquisitionLockTimeoutTask The new acquisition lock timeout task to set.
     * @throws IllegalArgumentException if there is already an acquisition lock timeout task set.
     */
    public void updateAcquisitionLockTimeoutTask(AcquisitionLockTimerTask acquisitionLockTimeoutTask) throws IllegalArgumentException {
        if (this.acquisitionLockTimeoutTask != null) {
            throw new IllegalArgumentException("Existing acquisition lock timeout exists, cannot override.");
        }
        this.acquisitionLockTimeoutTask = acquisitionLockTimeoutTask;
    }

    /**
     * Cancel the acquisition lock timeout task and clear the reference to it.
     * This method is used to cancel the acquisition lock timeout task if it exists
     * and clear the reference to it.
     */
    public void cancelAndClearAcquisitionLockTimeoutTask() {
        if (acquisitionLockTimeoutTask != null) {
            acquisitionLockTimeoutTask.cancel();
            acquisitionLockTimeoutTask = null;
        }
    }

    /**
     * Check if there is an ongoing state transition for the records.
     * This method checks if the rollbackState is not null, which indicates that
     * there has been a state transition that has not been committed yet.
     *
     * @return true if there is an ongoing state transition, false otherwise.
     */
    public boolean hasOngoingStateTransition() {
        // If batch/offset hasn't transitioned even once or the state transitions have been
        // committed then rollbackState should always be null.
        return rollbackState != null;
    }

    /**
     * Try to update the state of the records. The state of the records can only be updated if the
     * new state is allowed to be transitioned from old state. The delivery count is not changed
     * if the state update is unsuccessful.
     *
     * @param newState The new state of the records.
     * @param ops      The behavior on the delivery count.
     * @param maxDeliveryCount The maximum delivery count for the record.
     * @param newMemberId The member id of the client that is fetching/acknowledging the record.
     *
     * @return {@code InFlightState} if update succeeds, null otherwise. Returning state
     *         helps update chaining.
     */
    public InFlightState tryUpdateState(RecordState newState, DeliveryCountOps ops, int maxDeliveryCount, String newMemberId, boolean dlqSupportEnabled) {
        try {
            // If the state transition is in progress, the state should not be updated.
            if (hasOngoingStateTransition()) {
                // A misbehaving client can send multiple requests to update the same records hence
                // do not proceed if the transition is already in progress. Do not log an error here
                // as it might not be an error rather concurrent update of same state due to multiple
                // requests. This ideally should not happen hence log in info level, if it happens
                // frequently then it might be an issue which needs to be investigated.
                log.info("{} has ongoing state transition, cannot update to: {}", this, newState);
                return null;
            }

            if (newState == RecordState.AVAILABLE && ops != DeliveryCountOps.DECREASE && deliveryCount >= maxDeliveryCount) {
                newState = dlqSupportEnabled ? RecordState.ARCHIVING : RecordState.ARCHIVED;
            }
            state = state.validateTransition(newState);
            if (newState != RecordState.ARCHIVED) {
                deliveryCount = updatedDeliveryCount(ops);
            }
            memberId = newMemberId;
            return this;
        } catch (IllegalStateException e) {
            log.error("Failed to update state of the records", e);
            return null;
        }
    }

    /**
     * Archive the record by setting its state to ARCHIVED, clearing the memberId and
     * cancelling the acquisition lock timeout task.
     * This method is used to archive the record when it is no longer needed.
     */
    public void archive() {
        isTerminalState = true;
        state = RecordState.ARCHIVED;
        memberId = EMPTY_MEMBER_ID;
        cancelAndClearAcquisitionLockTimeoutTask();
    }

    /**
     * Start a state transition for the records. This method is used to start a state transition
     * for the records. It creates a copy of the current state and sets it as the rollback state.
     * If the state transition is successful, it returns the updated state.
     *
     * @param newState The new state of the records.
     * @param ops      The behavior on the delivery count.
     * @param maxDeliveryCount The maximum delivery count for the record.
     * @param newMemberId The member id of the client that is fetching/acknowledging the record.
     *
     * @return {@code InFlightState} if update succeeds, null otherwise. Returning state
     *         helps update chaining.
     */
    public InFlightState startStateTransition(RecordState newState, DeliveryCountOps ops, int maxDeliveryCount, String newMemberId, boolean dlqSupportEnabled) {
        InFlightState currentState = new InFlightState(state, deliveryCount, memberId, acquisitionLockTimeoutTask);
        InFlightState updatedState = tryUpdateState(newState, ops, maxDeliveryCount, newMemberId, dlqSupportEnabled);
        if (updatedState != null) {
            rollbackState = new RollbackState(currentState, maxDeliveryCount);
        }
        return updatedState;
    }

    /**
     * Complete the state transition for the records. If the commit is true or the state is terminal,
     * it cancels the acquisition lock timeout task and clears the rollback state.
     * If the commit is false and the state is not terminal, it rolls back the state transition.
     *
     * @param commit If true, commits the state transition, otherwise rolls back.
     */
    public void completeStateTransition(boolean commit) {
        if (commit || isTerminalState) {
            // Cancel the acquisition lock timeout task for the state since it is acknowledged/released successfully.
            cancelAndClearAcquisitionLockTimeoutTask();
            rollbackState = null;
            return;
        }
        InFlightState previousState = rollbackState.state();
        // Check is acquisition lock timeout task is expired then mark the message as Available.
        if (acquisitionLockTimeoutTask != null && acquisitionLockTimeoutTask.hasExpired()) {
            // If the acquisition lock timeout task has expired, we should mark the record as available.
            // However, if the delivery count has reached the maximum delivery count, we should archive the record.
            state = previousState.deliveryCount() >= rollbackState.maxDeliveryCount ?
                RecordState.ARCHIVED : RecordState.AVAILABLE;
            memberId = EMPTY_MEMBER_ID;
            cancelAndClearAcquisitionLockTimeoutTask();
        } else {
            state = previousState.state();
            memberId = previousState.memberId();
        }
        // Do not revert the delivery count as the delivery count should not be reverted for the failed
        // state transition. However, in the current implementation, the delivery count is only incremented
        // when the state is updated to Acquired, hence reverting the delivery count is not needed when
        // the state transition fails.
        rollbackState = null;
    }

    /**
     * @return The transaction owner id staged for this transactional acknowledgment, or -1 if not in TX_PENDING.
     */
    public long stagedTxnOwnerId() {
        return stagedTxnOwnerId;
    }

    /**
     * @return The transaction owner epoch staged for this transactional acknowledgment, or -1 if not in TX_PENDING.
     */
    public short stagedTxnOwnerEpoch() {
        return stagedTxnOwnerEpoch;
    }

    public long stagedProducerId() {
        return stagedTxnOwnerId;
    }

    public short stagedProducerEpoch() {
        return stagedTxnOwnerEpoch;
    }

    /**
     * @return The acknowledge type staged for this transactional acknowledgment, or -1 if not in TX_PENDING.
     */
    public byte stagedAckType() {
        return stagedAckType;
    }

    public byte stagedDeliveryState() {
        return stagedDeliveryState;
    }

    public InFlightState stageTxnAcknowledge(long txnOwnerId, short txnOwnerEpoch, AcknowledgeType ackType) {
        return stageTxnAcknowledge(txnOwnerId, txnOwnerEpoch, ackType, defaultStagedDeliveryState(ackType));
    }

    /**
     * Stage this record into an open producer transaction. Transitions state from ACQUIRED to TX_PENDING,
     * cancels the acquisition lock timer (transaction timeout governs the hold instead), and records the
     * transaction owner identity and ack type for later resolution by {@link #applyTxnMarker}.
     * Only ACCEPT and REJECT are valid ack types inside a transaction.
     */
    public InFlightState stageTxnAcknowledge(long txnOwnerId, short txnOwnerEpoch, AcknowledgeType ackType, RecordState stagedDeliveryState) {
        if (ackType != AcknowledgeType.ACCEPT && ackType != AcknowledgeType.REJECT) {
            throw new IllegalArgumentException("Only ACCEPT or REJECT are valid inside a transaction, got: " + ackType);
        }
        try {
            state = state.validateTransition(RecordState.TX_PENDING);
            this.stagedTxnOwnerId = txnOwnerId;
            this.stagedTxnOwnerEpoch = txnOwnerEpoch;
            this.stagedAckType = ackType.id;
            this.stagedDeliveryState = stagedDeliveryState.id;
            cancelAndClearAcquisitionLockTimeoutTask();
            return this;
        } catch (IllegalStateException e) {
            log.error("Failed to stage transactional acknowledgment", e);
            return null;
        }
    }

    public boolean revertStagedTxnAcknowledge(long txnOwnerId, short txnOwnerEpoch) {
        if (state != RecordState.TX_PENDING) {
            return false;
        }
        if (this.stagedTxnOwnerId != txnOwnerId || this.stagedTxnOwnerEpoch != txnOwnerEpoch) {
            return false;
        }
        state = RecordState.ACQUIRED;
        stagedTxnOwnerId = -1L;
        stagedTxnOwnerEpoch = -1;
        stagedAckType = -1;
        stagedDeliveryState = -1;
        return true;
    }

    /**
     * Apply a transaction commit or abort marker to a TX_PENDING record.
     * On COMMIT: ACCEPT resolves to ACKNOWLEDGED, REJECT resolves to ARCHIVING or ARCHIVED based on DLQ configuration.
     * On ABORT: reverts to AVAILABLE so the record can be redelivered.
     * Returns null if the state is not TX_PENDING or the transaction owner identity does not match.
     */
    public InFlightState applyTxnMarker(long txnOwnerId, short txnOwnerEpoch, TransactionResult result, boolean dlqSupportEnabled) {
        if (state != RecordState.TX_PENDING) {
            return null;
        }
        if (this.stagedTxnOwnerId != txnOwnerId || this.stagedTxnOwnerEpoch != txnOwnerEpoch) {
            return null;
        }
        try {
            if (result == TransactionResult.COMMIT) {
                RecordState nextState = stagedDeliveryState == -1
                    ? fallbackDeliveryState(dlqSupportEnabled)
                    : RecordState.forId(stagedDeliveryState);
                state = state.validateTransition(nextState);
                memberId = EMPTY_MEMBER_ID;
            } else {
                state = state.validateTransition(RecordState.AVAILABLE);
                memberId = EMPTY_MEMBER_ID;
            }
            stagedTxnOwnerId = -1L;
            stagedTxnOwnerEpoch = -1;
            stagedAckType = -1;
            stagedDeliveryState = -1;
            return this;
        } catch (IllegalStateException e) {
            log.error("Failed to apply transaction marker", e);
            return null;
        }
    }

    public InFlightState applyTxnMarker(long txnOwnerId, short txnOwnerEpoch, TransactionResult result) {
        return applyTxnMarker(txnOwnerId, txnOwnerEpoch, result, true);
    }

    private RecordState defaultStagedDeliveryState(AcknowledgeType ackType) {
        return ackType == AcknowledgeType.ACCEPT ? RecordState.ACKNOWLEDGED : RecordState.ARCHIVING;
    }

    private RecordState fallbackDeliveryState(boolean dlqSupportEnabled) {
        if (stagedAckType == AcknowledgeType.ACCEPT.id) {
            return RecordState.ACKNOWLEDGED;
        }
        return dlqSupportEnabled ? RecordState.ARCHIVING : RecordState.ARCHIVED;
    }

    private int updatedDeliveryCount(DeliveryCountOps ops) {
        return switch (ops) {
            case INCREASE -> deliveryCount + 1;
            case DECREASE -> deliveryCount - 1;
            // do nothing
            case NO_OP -> deliveryCount;
        };
    }

    @Override
    public int hashCode() {
        return Objects.hash(state, deliveryCount, memberId, stagedTxnOwnerId, stagedTxnOwnerEpoch, stagedAckType, stagedDeliveryState);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        InFlightState that = (InFlightState) o;
        return state == that.state
            && deliveryCount == that.deliveryCount
            && memberId.equals(that.memberId)
            && stagedTxnOwnerId == that.stagedTxnOwnerId
            && stagedTxnOwnerEpoch == that.stagedTxnOwnerEpoch
            && stagedAckType == that.stagedAckType
            && stagedDeliveryState == that.stagedDeliveryState;
    }

    @Override
    public String toString() {
        return "InFlightState(" +
            "state=" + state +
            ", deliveryCount=" + deliveryCount +
            ", memberId=" + memberId +
            ", stagedTxnOwnerId=" + stagedTxnOwnerId +
            ", stagedTxnOwnerEpoch=" + stagedTxnOwnerEpoch +
            ", stagedAckType=" + stagedAckType +
            ", stagedDeliveryState=" + stagedDeliveryState +
            ")";
    }

  /**
   * This record is used to store the state before the transition. It is used to revert the state if the transition fails.
   * @param state The state of the records before the transition.
   * @param maxDeliveryCount The maximum delivery count for the record.
   */
    private record RollbackState(InFlightState state, int maxDeliveryCount) {
    }
}
