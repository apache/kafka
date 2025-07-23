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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * The InFlightState is used to track the state and delivery count of a record that has been
 * fetched from the leader. The state of the record is used to determine if the record should
 * be re-deliver or if it can be acknowledged or archived.
 */
public class InFlightState {

    private static final Logger log = LoggerFactory.getLogger(InFlightState.class);

    // The state of the fetch batch records.
    private RecordState state;
    // The number of times the records has been delivered to the client.
    private int deliveryCount;
    // The member id of the client that is fetching/acknowledging the record.
    private String memberId;
    // The state of the records before the transition. In case we need to revert an in-flight state, we revert the above
    // attributes of InFlightState to this state, namely - state, deliveryCount and memberId.
    private InFlightState rollbackState;
    // The timer task for the acquisition lock timeout.
    private AcquisitionLockTimerTask acquisitionLockTimeoutTask;

    // Visible for testing.
    public InFlightState(RecordState state, int deliveryCount, String memberId) {
        this(state, deliveryCount, memberId, null);
    }

    InFlightState(RecordState state, int deliveryCount, String memberId, AcquisitionLockTimerTask acquisitionLockTimeoutTask) {
        this.state = state;
        this.deliveryCount = deliveryCount;
        this.memberId = memberId;
        this.acquisitionLockTimeoutTask = acquisitionLockTimeoutTask;
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
        acquisitionLockTimeoutTask.cancel();
        acquisitionLockTimeoutTask = null;
    }

    /**
     * Check if there is an ongoing state transition for the records.
     * This method checks if the rollbackState is not null, which indicates that
     * there has been a state transition that has not been committed yet.
     *
     * @return true if there is an ongoing state transition, false otherwise.
     */
    public boolean hasOngoingStateTransition() {
        if (rollbackState == null) {
            // This case could occur when the batch/offset hasn't transitioned even once or the state transitions have
            // been committed.
            return false;
        }
        return rollbackState.state != null;
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
    public InFlightState tryUpdateState(RecordState newState, DeliveryCountOps ops, int maxDeliveryCount, String newMemberId) {
        try {
            if (newState == RecordState.AVAILABLE && ops != DeliveryCountOps.DECREASE && deliveryCount >= maxDeliveryCount) {
                newState = RecordState.ARCHIVED;
            }
            state = state.validateTransition(newState);
            if (newState != RecordState.ARCHIVED) {
                deliveryCount = updatedDeliveryCount(ops);
            }
            memberId = newMemberId;
            return this;
        } catch (IllegalStateException e) {
            log.error("Failed to update state of the records", e);
            rollbackState = null;
            return null;
        }
    }

    /**
     * Archive the record by setting its state to ARCHIVED, clearing the memberId and
     * cancelling the acquisition lock timeout task.
     * This method is used to archive the record when it is no longer needed.
     */
    public void archive(String newMemberId) {
        state = RecordState.ARCHIVED;
        memberId = newMemberId;
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
    public InFlightState startStateTransition(RecordState newState, DeliveryCountOps ops, int maxDeliveryCount, String newMemberId) {
        rollbackState = new InFlightState(state, deliveryCount, memberId, acquisitionLockTimeoutTask);
        return tryUpdateState(newState, ops, maxDeliveryCount, newMemberId);
    }

    /**
     * Complete the state transition for the records. If the commit is true or the state is terminal,
     * it cancels the acquisition lock timeout task and clears the rollback state.
     * If the commit is false and the state is not terminal, it rolls back the state transition.
     *
     * @param commit If true, commits the state transition, otherwise rolls back.
     */
    public void completeStateTransition(boolean commit) {
        if (commit) {
            rollbackState = null;
            return;
        }
        state = rollbackState.state;
        deliveryCount = rollbackState.deliveryCount;
        memberId = rollbackState.memberId;
        rollbackState = null;
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
        return Objects.hash(state, deliveryCount, memberId);
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
        return state == that.state && deliveryCount == that.deliveryCount && memberId.equals(that.memberId);
    }

    @Override
    public String toString() {
        return "InFlightState(" +
            "state=" + state.toString() +
            ", deliveryCount=" + deliveryCount +
            ", memberId=" + memberId +
            ")";
    }
}
