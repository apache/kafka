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
package org.apache.kafka.server.mirror;

import org.apache.kafka.common.mirror.LastMirror;

import java.util.Objects;
import java.util.Optional;

/**
 * An immutable snapshot of the mirror metadata of a partition.
 *
 * @param state          The mirror partition state.
 * @param stateEpoch     The current state epoch for the partition, which tells when the cached metadata can be safely
 *                       updated.
 * @param lastMirror     The last mirror offset and epoch, or {@link LastMirror#EMPTY} if not available.
 * @param errorMessage   The error message when in {@link MirrorPartitionState#FAILED} state, or empty otherwise.
 * @param retryAttempt   The retry attempt count for the current failure/retry sequence, or 0 if no retries have been
 *                       recorded. {@link #NON_RETRYABLE_ERROR} indicates a non-retryable failure. The count is
 *                       preserved when recovering from {@link MirrorPartitionState#FAILED}.
 * @param previousState  The mirror partition state before entering {@link MirrorPartitionState#FAILED},
 *                       or {@link MirrorPartitionState#UNKNOWN} if not recorded.
 */
public record MirrorPartitionMetadata(
    MirrorPartitionState state,
    int stateEpoch,
    LastMirror lastMirror,
    Optional<String> errorMessage,
    short retryAttempt,
    MirrorPartitionState previousState
) {
    /**
     * The retry attempt of a partition which failed with a non-retryable error.
     */
    public static final short NON_RETRYABLE_ERROR = -1;

    /**
     * The metadata of a partition with no recorded state, with the default value of every field.
     */
    public static final MirrorPartitionMetadata EMPTY = new MirrorPartitionMetadata(
        MirrorPartitionState.UNKNOWN, 0, LastMirror.EMPTY, Optional.empty(), (short) 0, MirrorPartitionState.UNKNOWN);

    /**
     * Returns the given metadata if non-null, or {@link #EMPTY} otherwise.
     *
     * @param metadata the metadata to check, may be {@code null}.
     * @return the given metadata, or {@link #EMPTY} if {@code null}.
     */
    public static MirrorPartitionMetadata orEmpty(MirrorPartitionMetadata metadata) {
        return metadata != null ? metadata : EMPTY;
    }

    public MirrorPartitionMetadata {
        Objects.requireNonNull(state);
        if (stateEpoch < 0) {
            throw new IllegalArgumentException("stateEpoch must be non-negative");
        }
        Objects.requireNonNull(lastMirror);
        Objects.requireNonNull(errorMessage);
        if (retryAttempt < NON_RETRYABLE_ERROR) {
            throw new IllegalArgumentException("retryAttempt must be >= " + NON_RETRYABLE_ERROR);
        }
        Objects.requireNonNull(previousState);
    }

    public static class Builder {
        private MirrorPartitionState state;
        private int stateEpoch;
        private LastMirror lastMirror;
        private Optional<String> errorMessage;
        private short retryAttempt;
        private MirrorPartitionState previousState;

        /**
         * Creates a builder with the default value of every field, as in {@link #EMPTY}.
         */
        public Builder() {
            this(EMPTY);
        }

        /**
         * Creates a builder with the fields of the given metadata, so that only the fields which change need to be set.
         */
        public Builder(MirrorPartitionMetadata metadata) {
            this.state = metadata.state();
            this.stateEpoch = metadata.stateEpoch();
            this.lastMirror = metadata.lastMirror();
            this.errorMessage = metadata.errorMessage();
            this.retryAttempt = metadata.retryAttempt();
            this.previousState = metadata.previousState();
        }

        public Builder withState(MirrorPartitionState state) {
            this.state = Objects.requireNonNull(state);
            return this;
        }

        public Builder withStateEpoch(int stateEpoch) {
            this.stateEpoch = stateEpoch;
            return this;
        }

        public Builder withLastMirror(LastMirror lastMirror) {
            this.lastMirror = Objects.requireNonNull(lastMirror);
            return this;
        }

        public Builder withErrorMessage(Optional<String> errorMessage) {
            this.errorMessage = Objects.requireNonNull(errorMessage);
            return this;
        }

        public Builder withRetryAttempt(short retryAttempt) {
            this.retryAttempt = retryAttempt;
            return this;
        }

        public Builder withPreviousState(MirrorPartitionState previousState) {
            this.previousState = Objects.requireNonNull(previousState);
            return this;
        }

        /**
         * Clears all failure-related fields: sets the error message to empty, the retry attempt count to zero,
         * and the previous state to {@link MirrorPartitionState#UNKNOWN}. Use this after a successful recovery
         * from {@link MirrorPartitionState#FAILED} to reset the failure/retry sequence.
         */
        public Builder withFailedStateReset() {
            this.errorMessage = Optional.empty();
            this.retryAttempt = (short) 0;
            this.previousState = MirrorPartitionState.UNKNOWN;
            return this;
        }

        /**
         * Transitions to the given state and computes error-related fields.
         *
         * <p>When entering {@link MirrorPartitionState#FAILED}, sets the error message, computes the next retry attempt
         * count, and records the previous state. If the partition is already in FAILED, the previous state is
         * preserved across consecutive failures.
         *
         * <p>When entering {@link MirrorPartitionState#STOPPED} or {@link MirrorPartitionState#PAUSED}, or when
         * transitioning between non-FAILED states, clears all error fields.
         *
         * <p>Otherwise, clears the error message.
         *
         * @param targetState      the target state of the transition.
         * @param newStateEpoch    the state epoch for the transition.
         * @param newErrorMessage  the error message to record, or empty if none.
         * @param nonRetryable     whether the error is non-retryable (only relevant when entering FAILED).
         * @param maxRetryAttempts the upper bound for the retry attempt count (only relevant when entering FAILED).
         */
        public Builder withStateTransition(MirrorPartitionState targetState,
                                           int newStateEpoch,
                                           Optional<String> newErrorMessage,
                                           boolean nonRetryable,
                                           short maxRetryAttempts) {
            if (targetState == MirrorPartitionState.FAILED) {
                if (maxRetryAttempts < 0) {
                    throw new IllegalArgumentException("maxRetryAttempts must be non-negative");
                }
                this.errorMessage = newErrorMessage;
                if (nonRetryable || this.retryAttempt == NON_RETRYABLE_ERROR) {
                    this.retryAttempt = NON_RETRYABLE_ERROR;
                } else {
                    this.retryAttempt = (short) Math.min(maxRetryAttempts, this.retryAttempt + 1);
                }
                if (this.state != MirrorPartitionState.FAILED) {
                    this.previousState = this.state;
                }
            } else if ((this.state != MirrorPartitionState.FAILED && this.state != targetState)
                    || targetState == MirrorPartitionState.STOPPED
                    || targetState == MirrorPartitionState.PAUSED) {
                withFailedStateReset();
            } else {
                this.errorMessage = Optional.empty();
            }
            this.state = targetState;
            this.stateEpoch = newStateEpoch;
            return this;
        }

        public MirrorPartitionMetadata build() {
            return new MirrorPartitionMetadata(state, stateEpoch,
                    lastMirror, errorMessage, retryAttempt, previousState);
        }
    }

    /**
     * Checks whether a transition from {@code source} to {@code target} is valid according to the
     * mirror partition state machine defined in KIP-1279. Self-transitions are always valid, the
     * {@link MirrorPartitionState#FAILED} state is reachable from any state, and any transition
     * out of {@link MirrorPartitionState#FAILED} is permitted to allow retries.
     * {@link MirrorPartitionState#UNKNOWN} is not a valid target (self-transition aside) because it
     * represents the absence of cached state, not a reachable lifecycle state.
     *
     * @param source the current state.
     * @param target the desired target state.
     * @return {@code true} if the transition is valid, {@code false} otherwise.
     */
    @SuppressWarnings("cyclomaticComplexity")
    public static boolean isValidStateTransition(MirrorPartitionState source,
                                                 MirrorPartitionState target) {
        if (source == null || target == null) {
            return false;
        }
        // Re-asserting the current state triggers side effects (e.g. re-creating fetchers after failover)
        if (source == target) {
            return true;
        }
        if (source == MirrorPartitionState.FAILED) {
            return target != MirrorPartitionState.UNKNOWN;
        }
        // No default, so a new enum constant is a compile error here
        return switch (target) {
            case LOG_ALIGNMENT -> source == MirrorPartitionState.UNKNOWN
                    || source == MirrorPartitionState.STOPPED;
            case EPOCH_FENCING, ULE_RECOVERY, PAUSING -> source == MirrorPartitionState.MIRRORING;
            case MIRRORING -> source == MirrorPartitionState.LOG_ALIGNMENT
                    || source == MirrorPartitionState.EPOCH_FENCING
                    || source == MirrorPartitionState.PAUSED
                    || source == MirrorPartitionState.ULE_RECOVERY;
            case PAUSED -> source == MirrorPartitionState.PAUSING;
            case STOPPING -> source == MirrorPartitionState.MIRRORING
                    || source == MirrorPartitionState.PAUSED;
            case STOPPED -> source == MirrorPartitionState.STOPPING;
            case FAILED -> true;
            case UNKNOWN -> false;
        };
    }
}
