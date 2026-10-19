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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MirrorPartitionStateTest {
    @Test
    void testLogAlignmentState() {
        MirrorPartitionState state = MirrorPartitionState.LOG_ALIGNMENT;
        assertEquals((byte) 0, state.value());
    }

    @Test
    void testEpochFencingState() {
        MirrorPartitionState state = MirrorPartitionState.EPOCH_FENCING;
        assertEquals((byte) 1, state.value());
    }

    @Test
    void testUleRecoveryState() {
        MirrorPartitionState state = MirrorPartitionState.ULE_RECOVERY;
        assertEquals((byte) 2, state.value());
    }

    @Test
    void testMirroringState() {
        MirrorPartitionState state = MirrorPartitionState.MIRRORING;
        assertEquals((byte) 3, state.value());
    }

    @Test
    void testPausingState() {
        MirrorPartitionState state = MirrorPartitionState.PAUSING;
        assertEquals((byte) 4, state.value());
    }

    @Test
    void testPausedState() {
        MirrorPartitionState state = MirrorPartitionState.PAUSED;
        assertEquals((byte) 5, state.value());
    }

    @Test
    void testStoppingState() {
        MirrorPartitionState state = MirrorPartitionState.STOPPING;
        assertEquals((byte) 6, state.value());
    }

    @Test
    void testStoppedState() {
        MirrorPartitionState state = MirrorPartitionState.STOPPED;
        assertEquals((byte) 7, state.value());
    }

    @Test
    void testFailedState() {
        MirrorPartitionState state = MirrorPartitionState.FAILED;
        assertEquals((byte) 8, state.value());
    }

    @Test
    void testUnknownState() {
        MirrorPartitionState state = MirrorPartitionState.UNKNOWN;
        assertEquals((byte) -1, state.value());
    }

    @ParameterizedTest
    @ValueSource(bytes = {0, 1, 2, 3, 4, 5, 6, 7, 8, -1})
    void testFromValueValidBytes(byte value) {
        MirrorPartitionState state = MirrorPartitionState.fromValue(value);
        assertEquals(value, state.value());
    }

    @Test
    void testFromValueLogAlignment() {
        assertEquals(MirrorPartitionState.LOG_ALIGNMENT, MirrorPartitionState.fromValue((byte) 0));
    }

    @Test
    void testFromValueEpochFencing() {
        assertEquals(MirrorPartitionState.EPOCH_FENCING, MirrorPartitionState.fromValue((byte) 1));
    }

    @Test
    void testFromValueUleRecovery() {
        assertEquals(MirrorPartitionState.ULE_RECOVERY, MirrorPartitionState.fromValue((byte) 2));
    }

    @Test
    void testFromValueMirroring() {
        assertEquals(MirrorPartitionState.MIRRORING, MirrorPartitionState.fromValue((byte) 3));
    }

    @Test
    void testFromValuePausing() {
        assertEquals(MirrorPartitionState.PAUSING, MirrorPartitionState.fromValue((byte) 4));
    }

    @Test
    void testFromValuePaused() {
        assertEquals(MirrorPartitionState.PAUSED, MirrorPartitionState.fromValue((byte) 5));
    }

    @Test
    void testFromValueStopping() {
        assertEquals(MirrorPartitionState.STOPPING, MirrorPartitionState.fromValue((byte) 6));
    }

    @Test
    void testFromValueStopped() {
        assertEquals(MirrorPartitionState.STOPPED, MirrorPartitionState.fromValue((byte) 7));
    }

    @Test
    void testFromValueFailed() {
        assertEquals(MirrorPartitionState.FAILED, MirrorPartitionState.fromValue((byte) 8));
    }

    @Test
    void testFromValueUnknown() {
        assertEquals(MirrorPartitionState.UNKNOWN, MirrorPartitionState.fromValue((byte) -1));
    }

    @Test
    void testFromValueInvalidPositive() {
        assertThrows(
            IllegalArgumentException.class,
            () -> MirrorPartitionState.fromValue((byte) 9),
            "Should throw IllegalArgumentException for invalid positive value"
        );
    }

    @Test
    void testFromValueInvalidNegative() {
        assertThrows(
            IllegalArgumentException.class,
            () -> MirrorPartitionState.fromValue((byte) -2),
            "Should throw IllegalArgumentException for invalid negative value"
        );
    }

    @Test
    void testFromValueInvalidRange() {
        assertThrows(
            IllegalArgumentException.class,
            () -> MirrorPartitionState.fromValue((byte) 100),
            "Should throw IllegalArgumentException for out-of-range value"
        );
    }

    @Test
    void testAllStatesHaveUniqueValues() {
        MirrorPartitionState[] states = MirrorPartitionState.values();
        for (int i = 0; i < states.length; i++) {
            for (int j = i + 1; j < states.length; j++) {
                byte value1 = states[i].value();
                byte value2 = states[j].value();
                assertEquals(value1 != value2, true,
                    String.format("States %s and %s have the same value: %d", states[i], states[j], value1));
            }
        }
    }

    @Test
    void testRoundTripConversion() {
        for (MirrorPartitionState state : MirrorPartitionState.values()) {
            byte value = state.value();
            MirrorPartitionState recovered = MirrorPartitionState.fromValue(value);
            assertEquals(state, recovered, String.format("Round-trip conversion failed for %s", state));
        }
    }
}
