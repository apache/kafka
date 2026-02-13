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

import java.util.Objects;

/**
 * The RecordState is used to track the state of a record that has been fetched from the leader.
 * The state of the records determines if the records should be re-delivered, move the next fetch
 * offset, or be state persisted to disk.
 */
public enum RecordState {
    AVAILABLE((byte) 0),
    ACQUIRED((byte) 1),
    ACKNOWLEDGED((byte) 2),
    ARCHIVED((byte) 4);

    public final byte id;

    RecordState(byte id) {
        this.id = id;
    }

    /**
     * Validates that the <code>newState</code> is one of the valid transition from the current
     * {@code RecordState}.
     *
     * @param newState State into which requesting to transition; must be non-<code>null</code>
     *
     * @return {@code RecordState} <code>newState</code> if validation succeeds. Returning
     *         <code>newState</code> helps state assignment chaining.
     *
     * @throws IllegalStateException if the state transition validation fails.
     */
    public RecordState validateTransition(RecordState newState) throws IllegalStateException {
        Objects.requireNonNull(newState, "newState cannot be null");
        if (this == newState) {
            throw new IllegalStateException("The state transition is invalid as the new state is "
                + "the same as the current state");
        }

        if (this == ACKNOWLEDGED || this == ARCHIVED) {
            throw new IllegalStateException("The state transition is invalid from the current state: " + this);
        }

        if (this == AVAILABLE && newState != ACQUIRED) {
            throw new IllegalStateException("The state can only be transitioned to ACQUIRED from AVAILABLE");
        }

        // Either the transition is from Available -> Acquired or from Acquired -> Available/
        // Acknowledged/Archived.
        return newState;
    }

    public static RecordState forId(byte id) {
        return switch (id) {
            case 0 -> AVAILABLE;
            case 1 -> ACQUIRED;
            case 2 -> ACKNOWLEDGED;
            case 4 -> ARCHIVED;
            default -> throw new IllegalArgumentException("Unknown record state id: " + id);
        };
    }

    public byte id() {
        return this.id;
    }
}
