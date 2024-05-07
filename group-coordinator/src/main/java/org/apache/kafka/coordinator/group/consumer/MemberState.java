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
package org.apache.kafka.coordinator.group.consumer;

import java.util.HashMap;
import java.util.Map;

/**
 * The various states that a member can be in. For their definition,
 * refer to the documentation of {{@link CurrentAssignmentBuilder}}.
 */
public enum MemberState {
    /**
     * The member is fully reconciled with the desired target assignment.
     */
    STABLE((byte) 0),

    /**
     * The member must revoke some partitions in order to be able to
     * transition to the next epoch.
     */
    UNREVOKED_PARTITIONS((byte) 1),

    /**
     * The member transitioned to the last epoch but waits on some
     * partitions which have not been revoked by their previous
     * owners yet.
     */
    UNRELEASED_PARTITIONS((byte) 2),

    /**
     * The member is in an unknown state. This can only happen if a future
     * version of the software introduces a new state unknown by this version.
     */
    UNKNOWN((byte) 127);

    private final static Map<Byte, MemberState> VALUES_TO_ENUMS = new HashMap<>();

    static {
        for (MemberState state: MemberState.values()) {
            VALUES_TO_ENUMS.put(state.value(), state);
        }
    }

    private final byte value;

    MemberState(byte value) {
        this.value = value;
    }

    public byte value() {
        return value;
    }

    public static MemberState fromValue(byte value) {
        MemberState state = VALUES_TO_ENUMS.get(value);
        if (state == null) {
            return UNKNOWN;
        }
        return state;
    }
}
