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

/**
 * Lifecycle states of a mirror partition.
 * Values changes require an update to the javadoc of LeaderStateDescription.state().
 */
public enum MirrorPartitionState {
    /**
     * Initial state: aligning local log with the source leader's log.
     * Triggered when a partition is first added to mirroring or after recovery.
     */
    LOG_ALIGNMENT((byte) 0),

    /**
     * Local leader epoch is being bumped to match source partition's leader epoch.
     * Occurs when the source leader epoch exceeds the local epoch, protecting against
     * stale reads and ensuring epoch ordering.
     */
    EPOCH_FENCING((byte) 1),

    /**
     * Recovering from unclean leader election on the source cluster.
     * In this state, offsets are determined by reading the source leader's state,
     * then committed via the destination's group coordinator.
     */
    ULE_RECOVERY((byte) 2),

    /**
     * Actively mirroring data from source to destination.
     * The partition is fully initialized and continuously replicating records.
     */
    MIRRORING((byte) 3),

    /**
     * Pausing mirroring: replication is stopping but will resume.
     * Transient state, expected to move to PAUSED.
     */
    PAUSING((byte) 4),

    /**
     * Paused: replication is halted but can be resumed.
     * Useful for temporary maintenance or administrator control.
     */
    PAUSED((byte) 5),

    /**
     * Stopping mirroring: replication is terminating permanently.
     * Transient state, expected to move to STOPPED.
     */
    STOPPING((byte) 6),

    /**
     * Stopped: mirroring has been permanently terminated.
     * The partition will no longer be replicated.
     */
    STOPPED((byte) 7),

    /**
     * Failed: mirroring encountered an error and cannot proceed.
     * The error reason and retry information are stored in the metadata.
     * Retryable failures will eventually transition back to LOG_ALIGNMENT.
     * Non-retryable failures remain in FAILED state.
     */
    FAILED((byte) 8),

    /**
     * Unknown state: used when the state cannot be determined or is uninitialized.
     */
    UNKNOWN((byte) -1);

    private final byte value;

    MirrorPartitionState(byte value) {
        this.value = value;
    }

    public byte value() {
        return value;
    }

    public static MirrorPartitionState fromValue(byte value) {
        switch (value) {
            case 0: return LOG_ALIGNMENT;
            case 1: return EPOCH_FENCING;
            case 2: return ULE_RECOVERY;
            case 3: return MIRRORING;
            case 4: return PAUSING;
            case 5: return PAUSED;
            case 6: return STOPPING;
            case 7: return STOPPED;
            case 8: return FAILED;
            case -1: return UNKNOWN;
        }
        throw new IllegalArgumentException("Illegal mirror state: " + value);
    }
}
