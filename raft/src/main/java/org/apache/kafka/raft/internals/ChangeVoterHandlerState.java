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
import org.apache.kafka.raft.Endpoints;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.raft.RaftUtil;

import java.util.Optional;

/**
 * Manages the state of add, remove, and update voter operations.
 * <p>
 * This class maintains at most one pending voter change operation at a time. Add voter, remove
 * voter, and update voter operations are mutually exclusive - only one type can be in progress
 * at any given time. When an operation is reset or expires, its associated future is completed
 * with an appropriate error response.
 * <p>
 * The class also updates the uncommitted voter change metric to reflect whether a voter
 * change operation is currently pending.
 */
public final class ChangeVoterHandlerState {
    private Optional<AddVoterHandlerState> addVoterHandlerState = Optional.empty();
    private Optional<RemoveVoterHandlerState> removeVoterHandlerState = Optional.empty();
    private Optional<UpdateVoterHandlerState> updateVoterHandlerState = Optional.empty();

    private final KafkaRaftMetrics kafkaRaftMetrics;

    /**
     * Creates a new change-voter handler state tracker.
     *
     * @param kafkaRaftMetrics used to report whether a voter change operation is currently pending
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
     * remove voter or update voter handler state is currently present, this method throws an
     * IllegalStateException to enforce mutual exclusivity.
     *
     * @param error the error to complete any existing add voter operation with
     * @param message the error message to include in the response, or null for no message
     * @param state the new add voter handler state, or empty to clear the state
     * @throws IllegalStateException if attempting to set a non-empty add voter state while a
     *         remove voter or update voter state is already present
     */
    public void resetAddVoterHandlerState(
        Errors error,
        String message,
        Optional<AddVoterHandlerState> state
    ) {
        validateMutualExclusivity(state, removeVoterHandlerState, updateVoterHandlerState);
        addVoterHandlerState.ifPresent(
            handlerState -> handlerState.completeFuture(RaftUtil.addVoterResponse(error, message))
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
     * add voter or update voter handler state is currently present, this method throws an
     * IllegalStateException to enforce mutual exclusivity.
     *
     * @param error the error to complete any existing remove voter operation with
     * @param message the error message to include in the response, or null for no message
     * @param state the new remove voter handler state, or empty to clear the state
     * @throws IllegalStateException if attempting to set a non-empty remove voter state while an
     *         add voter or update voter state is already present
     */
    public void resetRemoveVoterHandlerState(
        Errors error,
        String message,
        Optional<RemoveVoterHandlerState> state
    ) {
        validateMutualExclusivity(addVoterHandlerState, state, updateVoterHandlerState);
        removeVoterHandlerState.ifPresent(
            handlerState -> handlerState.completeFuture(RaftUtil.removeVoterResponse(error, message))
        );
        removeVoterHandlerState = state;
        updateUncommittedVoterChangeMetric();
    }

    /**
     * Returns the current update voter handler state, if one exists.
     *
     * @return an Optional containing the update voter handler state, or empty if no update voter
     *         operation is pending
     */
    public Optional<UpdateVoterHandlerState> updateVoterHandlerState() {
        return updateVoterHandlerState;
    }

    /**
     * Resets the update voter handler state to the specified state.
     * <p>
     * If an update voter handler state already exists, its future will be completed with the
     * provided error before being replaced. If the new state is non-empty and an add voter or
     * remove voter handler state is currently present, this method throws an IllegalStateException
     * to enforce mutual exclusivity.
     *
     * @param error the error to complete any existing update voter operation with
     * @param leaderAndEpoch the current leader and epoch information
     * @param leaderEndpoints the current leader endpoints
     * @param state the new update voter handler state, or empty to clear the state
     * @throws IllegalStateException if attempting to set a non-empty update voter state while an
     *         add voter or remove voter state is already present
     */
    public void resetUpdateVoterHandlerState(
        Errors error,
        LeaderAndEpoch leaderAndEpoch,
        Endpoints leaderEndpoints,
        Optional<UpdateVoterHandlerState> state
    ) {
        validateMutualExclusivity(addVoterHandlerState, removeVoterHandlerState, state);
        updateVoterHandlerState.ifPresent(
            handlerState -> handlerState.completeFuture(
                RaftUtil.updateVoterResponse(
                    error,
                    handlerState.requestListenerName(),
                    leaderAndEpoch,
                    leaderEndpoints
                )
            )
        );
        updateVoterHandlerState = state;
        updateUncommittedVoterChangeMetric();
    }

    /**
     * Validates that at most one voter change operation is active.
     * <p>
     * This enforces mutual exclusivity between add, remove, and update voter operations.
     *
     * @param newAdd the new add voter state being set (if any)
     * @param newRemove the new remove voter state being set (if any)
     * @param newUpdate the new update voter state being set (if any)
     * @throws IllegalStateException if more than one operation would be active
     */
    private void validateMutualExclusivity(
        Optional<AddVoterHandlerState> newAdd,
        Optional<RemoveVoterHandlerState> newRemove,
        Optional<UpdateVoterHandlerState> newUpdate
    ) {
        int activeCount = 0;
        if (newAdd.isPresent()) activeCount++;
        if (newRemove.isPresent()) activeCount++;
        if (newUpdate.isPresent()) activeCount++;

        if (activeCount > 1) {
            throw new IllegalStateException(
                String.format(
                    "Cannot have multiple voter change operations active simultaneously: " +
                    "add=%s, remove=%s, update=%s",
                    newAdd.isPresent(),
                    newRemove.isPresent(),
                    newUpdate.isPresent()
                )
            );
        }
    }

    private void updateUncommittedVoterChangeMetric() {
        kafkaRaftMetrics.updateUncommittedVoterChange(
            addVoterHandlerState.isPresent() ||
            removeVoterHandlerState.isPresent() ||
            updateVoterHandlerState.isPresent()
        );
    }

    /**
     * Checks for and expires any pending voter change operations that have timed out.
     * <p>
     * This method evaluates the add voter, remove voter, and update voter operations. Any
     * operation that has expired (timeUntilOperationExpiration returns 0) is reset with a
     * REQUEST_TIMED_OUT error. The method then returns the minimum time remaining until the next
     * operation expiration.
     *
     * @param leaderAndEpoch the current leader and epoch information, used to complete the
     *        future of an expired update voter operation
     * @param leaderEndpoints the current leader endpoints, used to complete the future of an
     *        expired update voter operation
     * @param currentTimeMs the current time in milliseconds
     * @return the time in milliseconds until the next operation expires, or Long.MAX_VALUE if
     *         no operations are pending
     */
    public long maybeExpirePendingOperation(
        LeaderAndEpoch leaderAndEpoch,
        Endpoints leaderEndpoints,
        long currentTimeMs
    ) {
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

        long timeUntilUpdateVoterExpiration = updateVoterHandlerState()
            .map(state -> state.timeUntilOperationExpiration(currentTimeMs))
            .orElse(Long.MAX_VALUE);

        if (timeUntilUpdateVoterExpiration == 0) {
            resetUpdateVoterHandlerState(
                Errors.REQUEST_TIMED_OUT,
                leaderAndEpoch,
                leaderEndpoints,
                Optional.empty()
            );
        }

        // Reread the timeouts and return the smaller of them
        return Math.min(
            addVoterHandlerState()
                .map(state -> state.timeUntilOperationExpiration(currentTimeMs))
                .orElse(Long.MAX_VALUE),
            Math.min(
                removeVoterHandlerState()
                    .map(state -> state.timeUntilOperationExpiration(currentTimeMs))
                    .orElse(Long.MAX_VALUE),
                updateVoterHandlerState()
                    .map(state -> state.timeUntilOperationExpiration(currentTimeMs))
                    .orElse(Long.MAX_VALUE)
            )
        );
    }

    /**
     * Resets all pending voter handler states with the given error.
     * <p>
     * This method completes the futures of any pending add voter, remove voter, and update voter
     * operations with the provided error.
     *
     * @param error the error to complete any existing operations with
     * @param leaderAndEpoch the current leader and epoch information
     * @param leaderEndpoints the current leader endpoints
     */
    public void maybeResetPendingVoterHandlerState(
        Errors error,
        LeaderAndEpoch leaderAndEpoch,
        Endpoints leaderEndpoints
    ) {
        resetAddVoterHandlerState(error, null, Optional.empty());
        resetRemoveVoterHandlerState(error, null, Optional.empty());
        resetUpdateVoterHandlerState(error, leaderAndEpoch, leaderEndpoints, Optional.empty());
    }

    /**
     * Checks whether any voter change operation is currently pending.
     * <p>
     * This method first expires any operations that have timed out, then checks if any
     * add voter, remove voter, or update voter operations remain active.
     *
     * @param leaderAndEpoch the current leader and epoch information
     * @param leaderEndpoints the current leader endpoints
     * @param currentTimeMs the current time in milliseconds
     * @return true if any voter change operation is pending, false otherwise
     */
    public boolean isOperationPending(
        LeaderAndEpoch leaderAndEpoch,
        Endpoints leaderEndpoints,
        long currentTimeMs
    ) {
        maybeExpirePendingOperation(leaderAndEpoch, leaderEndpoints, currentTimeMs);
        return addVoterHandlerState.isPresent() || removeVoterHandlerState.isPresent() || updateVoterHandlerState.isPresent();
    }

    @Override
    public String toString() {
        return String.format(
            "ChangeVoterHandlerState(addVoterHandlerState=%s, removeVoterHandlerState=%s, updateVoterHandlerState=%s)",
            addVoterHandlerState,
            removeVoterHandlerState,
            updateVoterHandlerState
        );
    }
}
