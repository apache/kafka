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

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final public class LeaderRecoveryStateTest {
    private static final byte NO_CHANGE = (byte) -1;

    @Test
    void testUniqueValues() {
        Set<Byte> set = new HashSet<>();
        for (LeaderRecoveryState recovery : LeaderRecoveryState.values()) {
            assertTrue(
                set.add(recovery.value()),
                String.format("Value %s for election state %s has already been used", recovery.value(), recovery)
            );
        }
    }

    @Test
    void testDoesNotContainNoChange() {
        for (LeaderRecoveryState recovery : LeaderRecoveryState.values()) {
            assertNotEquals(NO_CHANGE, recovery.value());
        }
    }

    @Test
    void testByteToLeaderRecoveryState() {
        assertEquals(LeaderRecoveryState.RECOVERED, LeaderRecoveryState.of((byte) 0));
        assertEquals(LeaderRecoveryState.RECOVERING, LeaderRecoveryState.of((byte) 1));
    }

    @Test
    void testLeaderRecoveryStateValue() {
        assertEquals(0, LeaderRecoveryState.RECOVERED.value());
        assertEquals(1, LeaderRecoveryState.RECOVERING.value());
    }

    @Test
    void testInvalidValue() {
        assertThrows(
            IllegalArgumentException.class,
            () -> LeaderRecoveryState.of(NO_CHANGE)
        );
        assertThrows(IllegalArgumentException.class, () -> LeaderRecoveryState.of((byte) 2));
    }

    @Test
    void testOptionalInvalidValue() {
        assertEquals(Optional.empty(), LeaderRecoveryState.optionalOf(NO_CHANGE));
        assertEquals(Optional.empty(), LeaderRecoveryState.optionalOf((byte) 2));
    }

    @Test
    void testChangeTo() {
        LeaderRecoveryState state = LeaderRecoveryState.RECOVERED;
        assertEquals(LeaderRecoveryState.RECOVERED, state.changeTo(NO_CHANGE));
        state = state.changeTo(LeaderRecoveryState.RECOVERING.value());
        assertEquals(LeaderRecoveryState.RECOVERING, state);
        assertEquals(LeaderRecoveryState.RECOVERING, state.changeTo(NO_CHANGE));
        state = state.changeTo(LeaderRecoveryState.RECOVERED.value());
        assertEquals(LeaderRecoveryState.RECOVERED, state);
    }
}
