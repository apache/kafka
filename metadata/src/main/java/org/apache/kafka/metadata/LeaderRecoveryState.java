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

package org.apache.kafka.metadata;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public enum LeaderRecoveryState {
    /**
     * Represent that the election for the partition was either an ISR election or the
     * leader recovered from an unclean leader election.
     */
    RECOVERED((byte) 0),

    /**
     * Represent that the election for the partition was an unclean leader election and
     * that the leader is recovering from it.
     */
    RECOVERING((byte) 1);

    /**
     * A special value used to represent that the LeaderRecoveryState field of a
     * PartitionChangeRecord didn't change.
     */
    private static final byte NO_CHANGE = (byte) -1;

    private static final Map<Byte, LeaderRecoveryState> VALUE_TO_ENUM;

    static {
        Map<Byte, LeaderRecoveryState> map = new HashMap<>();
        for (LeaderRecoveryState recovery : LeaderRecoveryState.values()) {
            if (recovery.value() == NO_CHANGE) {
                throw new ExceptionInInitializerError(
                    String.format("Value %s for leader recovery state %s cannot be %s", recovery.value(), recovery, NO_CHANGE));
            }
            if (map.put(recovery.value(), recovery) != null) {
                throw new ExceptionInInitializerError(
                    String.format("Value %s for election state %s has already been used", recovery.value(), recovery));
            }
        }
        VALUE_TO_ENUM = Collections.unmodifiableMap(map);
    }

    public static LeaderRecoveryState of(byte value) {
        return Optional
            .ofNullable(VALUE_TO_ENUM.get(value))
            .orElseThrow(() -> new IllegalArgumentException(String.format("Value %s is not a valid leader recovery state", value)));
    }

    private final byte value;

    private LeaderRecoveryState(byte value) {
        this.value = value;
    }

    public byte value() {
        return value;
    }

    public LeaderRecoveryState changeTo(byte value) {
        if (value == NO_CHANGE) {
            return this;
        }

        return of(value);
    }
}
