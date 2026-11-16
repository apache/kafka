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

import java.util.Objects;

/**
 * An immutable snapshot of the mirror metadata of a partition.
 *
 * @param state             The mirror partition state.
 * @param leaderEpoch       The highest leader epoch seen for the partition, or -1 if not available.
 * @param stateEpoch        The current state epoch for the partition.
 * @param lastMirrorEpoch   The last mirror epoch, or -1 if not available.
 * @param lastMirrorOffset  The last mirror offset, or -1 if not available.
 * @param errorMessage      The error message when in {@link MirrorPartitionState#FAILED} state, or null otherwise.
 * @param retryAttempt      The retry attempt count in {@link MirrorPartitionState#FAILED} state, 0 if not failed,
 *                          or -1 if non-retryable.
 * @param previousState     The mirror partition state before entering {@link MirrorPartitionState#FAILED},
 *                          or {@link MirrorPartitionState#UNKNOWN} if not recorded.
 */
public record MirrorPartitionMetadata(
    MirrorPartitionState state,
    int leaderEpoch,
    int stateEpoch,
    int lastMirrorEpoch,
    long lastMirrorOffset,
    String errorMessage,
    short retryAttempt,
    MirrorPartitionState previousState
) {
    public MirrorPartitionMetadata {
        Objects.requireNonNull(state);
        Objects.requireNonNull(previousState);
    }
}
