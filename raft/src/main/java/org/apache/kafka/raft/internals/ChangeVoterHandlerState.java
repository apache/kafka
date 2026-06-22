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
package org.apache.kafka.raft.internals;

import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.raft.RaftUtil;

import java.util.Optional;

/**
 * Manages the state of add and remove voter operations.
 * <p>
 * This class maintains at most one pending voter change operation at a time. Add voter and
 * remove voter operations are mutually exclusive - only one type can be in progress at any
 * given time. When an operation is reset or expires, its associated future is completed with
 * an appropriate error response.
 * <p>
 * The class also updates the uncommitted voter change metric to reflect whether a voter
 * change operation is currently pending.
 */
public final class ChangeVoterHandlerState {
    private Optional<AddVoterHandlerState> addVoterHandlerState = Optional.empty();
    private Optional<RemoveVoterHandlerState> removeVoterHandlerState = Optional.empty();
    private final KafkaRaftMetrics kafkaRaftMetrics;

    /**
     * Constructs a new ChangeVoterHandlerState.
     *
     * @param kafkaRaftMetrics the metrics instance to update when voter change state changes
     */
    public ChangeVoterHandlerState(KafkaRaftMetrics kafkaRaftMetrics) {
        this.kafkaRaftMetrics = kafkaRaftMetrics;
    }

    /**
     * Returns the current add voter handler state, if one exists.
     *
     * @return an Optional containing the add voter handler state, or empty if no add voter
     *         operation is pending
     */
    public Optional<AddVoterHandlerState> addVoterHandlerState() {
        return addVoterHandlerState;
    }

    /**
     * Resets the add voter handler state to the specified state.
     * <p>
     * If an add voter handler state already exists, its future will be completed with the
     * provided error and message before being replaced. If the new state is non-empty and a
     * remove voter handler state is currently present, this method throws an IllegalStateException
     * to enforce mutual exclusivity.
     *
     * @param error the error to complete any existing add voter operation with
     * @param message the error message to include in the response, or null for no message
     * @param state the new add voter handler state, or empty to clear the state
     * @throws IllegalStateException if attempting to set a non-empty add voter state while a
     *         remove voter state is already present
     */
    public void resetAddVoterHandlerState(
        Errors error,
        String message,
        Optional<AddVoterHandlerState> state
    ) {
        if (state.isPresent() && removeVoterHandlerState.isPresent()) {
            throw new IllegalStateException(
                "Cannot set add voter handler state when remove voter handler state is already present"
            );
        }
        addVoterHandlerState.ifPresent(
            handlerState -> handlerState
                .future()
                .complete(RaftUtil.addVoterResponse(error, message))
        );
        addVoterHandlerState = state;
        updateUncommittedVoterChangeMetric();
    }

    /**
     * Returns the current remove voter handler state, if one exists.
     *
     * @return an Optional containing the remove voter handler state, or empty if no remove voter
     *         operation is pending
     */
    public Optional<RemoveVoterHandlerState> removeVoterHandlerState() {
        return removeVoterHandlerState;
    }

    /**
     * Resets the remove voter handler state to the specified state.
     * <p>
     * If a remove voter handler state already exists, its future will be completed with the
     * provided error and message before being replaced. If the new state is non-empty and an
     * add voter handler state is currently present, this method throws an IllegalStateException
     * to enforce mutual exclusivity.
     *
     * @param error the error to complete any existing remove voter operation with
     * @param message the error message to include in the response, or null for no message
     * @param state the new remove voter handler state, or empty to clear the state
     * @throws IllegalStateException if attempting to set a non-empty remove voter state while an
     *         add voter state is already present
     */
    public void resetRemoveVoterHandlerState(
        Errors error,
        String message,
        Optional<RemoveVoterHandlerState> state
    ) {
        if (state.isPresent() && addVoterHandlerState.isPresent()) {
            throw new IllegalStateException(
                "Cannot set remove voter handler state when add voter handler state is already present"
            );
        }
        removeVoterHandlerState.ifPresent(
            handlerState -> handlerState
                .future()
                .complete(RaftUtil.removeVoterResponse(error, message))
        );
        removeVoterHandlerState = state;
        updateUncommittedVoterChangeMetric();
    }

    private void updateUncommittedVoterChangeMetric() {
        kafkaRaftMetrics.updateUncommittedVoterChange(
            addVoterHandlerState.isPresent() || removeVoterHandlerState.isPresent()
        );
    }

    /**
     * Checks for and expires any pending voter change operations that have timed out.
     * <p>
     * This method evaluates both add voter and remove voter operations. Any operation that
     * has expired (timeUntilOperationExpiration returns 0) is reset with a REQUEST_TIMED_OUT
     * error. The method then returns the minimum time remaining until the next operation
     * expiration.
     *
     * @param currentTimeMs the current time in milliseconds
     * @return the time in milliseconds until the next operation expires, or Long.MAX_VALUE if
     *         no operations are pending
     */
    public long maybeExpirePendingOperation(long currentTimeMs) {
        // First abort any expired operations
        long timeUntilAddVoterExpiration = addVoterHandlerState()
            .map(state -> state.timeUntilOperationExpiration(currentTimeMs))
            .orElse(Long.MAX_VALUE);

        if (timeUntilAddVoterExpiration == 0) {
            resetAddVoterHandlerState(Errors.REQUEST_TIMED_OUT, null, Optional.empty());
        }

        long timeUntilRemoveVoterExpiration = removeVoterHandlerState()
            .map(state -> state.timeUntilOperationExpiration(currentTimeMs))
            .orElse(Long.MAX_VALUE);

        if (timeUntilRemoveVoterExpiration == 0) {
            resetRemoveVoterHandlerState(Errors.REQUEST_TIMED_OUT, null, Optional.empty());
        }

        // Reread the timeouts and return the smaller of them
        return Math.min(
            addVoterHandlerState()
                .map(state -> state.timeUntilOperationExpiration(currentTimeMs))
                .orElse(Long.MAX_VALUE),
            removeVoterHandlerState()
                .map(state -> state.timeUntilOperationExpiration(currentTimeMs))
                .orElse(Long.MAX_VALUE)
        );
    }

    /**
     * Resets all pending voter handler states, completing their futures with the specified error.
     * <p>
     * This method clears both add voter and remove voter handler states if they exist. Each
     * pending operation's future is completed with the provided error.
     *
     * @param error the error to complete any pending operations with
     */
    public void maybeResetPendingVoterHandlerState(Errors error) {
        resetAddVoterHandlerState(error, null, Optional.empty());
        resetRemoveVoterHandlerState(error, null, Optional.empty());
    }

    /**
     * Checks whether any voter change operation is currently pending.
     * <p>
     * This method first expires any operations that have timed out at the given timestamp,
     * then returns true if either an add voter or remove voter operation remains pending.
     *
     * @param currentTimeMs the current time in milliseconds
     * @return true if a voter change operation is pending, false otherwise
     */
    public boolean isOperationPending(long currentTimeMs) {
        maybeExpirePendingOperation(currentTimeMs);
        return addVoterHandlerState.isPresent() || removeVoterHandlerState.isPresent();
    }
}
