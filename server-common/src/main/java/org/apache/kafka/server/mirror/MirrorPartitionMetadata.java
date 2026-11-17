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

import org.apache.kafka.server.common.OffsetAndEpoch;

import java.util.Objects;

/**
 * An immutable snapshot of the mirror metadata of a partition.
 *
 * @param state          The mirror partition state.
 * @param stateEpoch     The current state epoch for the partition, which tells when the cached metadata can be safely
 *                       updated.
 * @param lastMirror     The last mirror epoch and offset, or {@link #EMPTY_LAST_MIRROR} if not available.
 * @param errorMessage   The error message when in {@link MirrorPartitionState#FAILED} state, or null otherwise.
 * @param retryAttempt   The retry attempt count in {@link MirrorPartitionState#FAILED} state, 0 if not failed,
 *                       or -1 if non-retryable.
 * @param previousState  The mirror partition state before entering {@link MirrorPartitionState#FAILED},
 *                       or {@link MirrorPartitionState#UNKNOWN} if not recorded.
 */
public record MirrorPartitionMetadata(
    MirrorPartitionState state,
    int stateEpoch,
    OffsetAndEpoch lastMirror,
    String errorMessage,
    short retryAttempt,
    MirrorPartitionState previousState
) {
    /**
     * The last mirror epoch and offset when they are not available.
     */
    public static final OffsetAndEpoch EMPTY_LAST_MIRROR = new OffsetAndEpoch(-1L, -1);

    /**
     * The metadata of a partition with no recorded state, with the default value of every field.
     */
    public static final MirrorPartitionMetadata EMPTY = builder().build();

    public MirrorPartitionMetadata {
        Objects.requireNonNull(state);
        Objects.requireNonNull(lastMirror);
        Objects.requireNonNull(previousState);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private MirrorPartitionState state = MirrorPartitionState.UNKNOWN;
        private int stateEpoch = 0;
        private OffsetAndEpoch lastMirror = EMPTY_LAST_MIRROR;
        private String errorMessage = null;
        private short retryAttempt = 0;
        private MirrorPartitionState previousState = MirrorPartitionState.UNKNOWN;

        public Builder setState(MirrorPartitionState state) {
            this.state = state;
            return this;
        }

        public Builder setStateEpoch(int stateEpoch) {
            this.stateEpoch = stateEpoch;
            return this;
        }

        public Builder setLastMirror(OffsetAndEpoch lastMirror) {
            this.lastMirror = lastMirror;
            return this;
        }

        public Builder setErrorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
            return this;
        }

        public Builder setRetryAttempt(short retryAttempt) {
            this.retryAttempt = retryAttempt;
            return this;
        }

        public Builder setPreviousState(MirrorPartitionState previousState) {
            this.previousState = previousState;
            return this;
        }

        public MirrorPartitionMetadata build() {
            return new MirrorPartitionMetadata(state, stateEpoch, lastMirror, errorMessage, retryAttempt, previousState);
        }
    }
}
