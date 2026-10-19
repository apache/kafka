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
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MirrorPartitionStateTest {
    @ParameterizedTest
    @MethodSource("expectedStateValues")
    void testValue(MirrorPartitionState state, byte expectedValue) {
        assertEquals(expectedValue, state.value());
    }

    @ParameterizedTest
    @MethodSource("expectedStateValues")
    void testFromValue(MirrorPartitionState expectedState, byte value) {
        assertEquals(expectedState, MirrorPartitionState.fromValue(value));
    }

    @ParameterizedTest
    @ValueSource(bytes = {9, -2, 100})
    void testFromValueInvalid(byte value) {
        assertThrows(
            IllegalArgumentException.class,
            () -> MirrorPartitionState.fromValue(value),
            "Should throw IllegalArgumentException for invalid value"
        );
    }

    @Test
    void testAllStatesAreCoveredByTable() {
        Set<MirrorPartitionState> tested = expectedStateValues()
            .map(args -> (MirrorPartitionState) args.get()[0])
            .collect(Collectors.toCollection(() -> EnumSet.noneOf(MirrorPartitionState.class)));

        assertEquals(EnumSet.allOf(MirrorPartitionState.class), tested,
            "Every MirrorPartitionState must have an entry in expectedStateValues() with its expected byte value");
    }

    @ParameterizedTest
    @EnumSource(MirrorPartitionState.class)
    void testRoundTrip(MirrorPartitionState state) {
        assertEquals(state, MirrorPartitionState.fromValue(state.value()));
    }

    // Explicit mapping of each state to its expected byte value.
    // Keep these values independent of MirrorPartitionState.value()
    // to catch accidental changes to the state representation.
    private static Stream<Arguments> expectedStateValues() {
        return Stream.of(
            Arguments.of(MirrorPartitionState.LOG_ALIGNMENT, (byte) 0),
            Arguments.of(MirrorPartitionState.EPOCH_FENCING, (byte) 1),
            Arguments.of(MirrorPartitionState.ULE_RECOVERY, (byte) 2),
            Arguments.of(MirrorPartitionState.MIRRORING, (byte) 3),
            Arguments.of(MirrorPartitionState.PAUSING, (byte) 4),
            Arguments.of(MirrorPartitionState.PAUSED, (byte) 5),
            Arguments.of(MirrorPartitionState.STOPPING, (byte) 6),
            Arguments.of(MirrorPartitionState.STOPPED, (byte) 7),
            Arguments.of(MirrorPartitionState.FAILED, (byte) 8),
            Arguments.of(MirrorPartitionState.UNKNOWN, (byte) -1)
        );
    }
}
