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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MirrorPartitionMetadataTest {
    @Test
    void testBuilder() {
        MirrorPartitionMetadata metadata = new MirrorPartitionMetadata.Builder()
            .withState(MirrorPartitionState.FAILED)
            .withStateEpoch(7)
            .withLastMirror(new LastMirror(4_294_967_313L, 42))
            .withErrorMessage(Optional.of("error"))
            .withRetryAttempt((short) 3)
            .withPreviousState(MirrorPartitionState.MIRRORING)
            .build();

        assertEquals(MirrorPartitionState.FAILED, metadata.state());
        assertEquals(7, metadata.stateEpoch());
        assertEquals(new LastMirror(4_294_967_313L, 42), metadata.lastMirror());
        assertEquals(Optional.of("error"), metadata.errorMessage());
        assertEquals((short) 3, metadata.retryAttempt());
        assertEquals(MirrorPartitionState.MIRRORING, metadata.previousState());
    }

    @Test
    void testEmpty() {
        MirrorPartitionMetadata empty = MirrorPartitionMetadata.EMPTY;

        assertEquals(MirrorPartitionState.UNKNOWN, empty.state());
        assertEquals(0, empty.stateEpoch());
        assertEquals(new LastMirror(-1L, -1), empty.lastMirror());
        assertEquals(LastMirror.EMPTY, empty.lastMirror());
        assertEquals(Optional.empty(), empty.errorMessage());
        assertEquals((short) 0, empty.retryAttempt());
        assertEquals(MirrorPartitionState.UNKNOWN, empty.previousState());
        assertEquals(empty, new MirrorPartitionMetadata.Builder().build());
    }

    @Test
    void testPartialBuilderKeepsDefaults() {
        MirrorPartitionMetadata metadata = new MirrorPartitionMetadata.Builder()
            .withState(MirrorPartitionState.MIRRORING)
            .withStateEpoch(3)
            .build();

        assertEquals(MirrorPartitionState.MIRRORING, metadata.state());
        assertEquals(3, metadata.stateEpoch());
        assertEquals(MirrorPartitionMetadata.EMPTY.lastMirror(), metadata.lastMirror());
        assertEquals(Optional.empty(), metadata.errorMessage());
        assertEquals(MirrorPartitionMetadata.EMPTY.retryAttempt(), metadata.retryAttempt());
        assertEquals(MirrorPartitionMetadata.EMPTY.previousState(), metadata.previousState());
    }

    @Test
    void testCopyBuilderChangesOnlyTheSetFields() {
        MirrorPartitionMetadata mirroring = new MirrorPartitionMetadata.Builder()
            .withState(MirrorPartitionState.MIRRORING)
            .withStateEpoch(3)
            .withLastMirror(new LastMirror(100L, 10))
            .build();

        MirrorPartitionMetadata failed = new MirrorPartitionMetadata.Builder(mirroring)
            .withState(MirrorPartitionState.FAILED)
            .withStateEpoch(4)
            .withErrorMessage(Optional.of("error"))
            .withRetryAttempt((short) 1)
            .withPreviousState(MirrorPartitionState.MIRRORING)
            .build();

        assertEquals(new MirrorPartitionMetadata(MirrorPartitionState.FAILED, 4, new LastMirror(100L, 10), Optional.of("error"),
            (short) 1, MirrorPartitionState.MIRRORING), failed);
        assertEquals(mirroring, new MirrorPartitionMetadata.Builder(mirroring).build());

        MirrorPartitionMetadata recovered = new MirrorPartitionMetadata.Builder(failed)
            .withState(MirrorPartitionState.MIRRORING)
            .withErrorMessage(Optional.empty())
            .withRetryAttempt((short) 0)
            .build();
        assertEquals(new MirrorPartitionMetadata(MirrorPartitionState.MIRRORING, 4, new LastMirror(100L, 10), Optional.empty(),
            (short) 0, MirrorPartitionState.MIRRORING), recovered);
    }

    @Test
    void testInvalidFieldsAreRejected() {
        assertThrows(NullPointerException.class, () -> new MirrorPartitionMetadata.Builder().withState(null).build());
        assertThrows(NullPointerException.class, () -> new MirrorPartitionMetadata.Builder().withLastMirror(null).build());
        assertThrows(NullPointerException.class, () -> new MirrorPartitionMetadata.Builder().withErrorMessage(null).build());
        assertThrows(NullPointerException.class, () -> new MirrorPartitionMetadata.Builder().withPreviousState(null).build());
        assertThrows(IllegalArgumentException.class, () -> new MirrorPartitionMetadata.Builder().withStateEpoch(-1).build());
        assertThrows(IllegalArgumentException.class, () -> new MirrorPartitionMetadata.Builder().withRetryAttempt((short) -7).build());
    }

    @Test
    void testEquality() {
        MirrorPartitionMetadata metadata = new MirrorPartitionMetadata.Builder()
            .withState(MirrorPartitionState.FAILED)
            .withStateEpoch(2)
            .withLastMirror(new LastMirror(100L, 10))
            .withErrorMessage(Optional.of("Error"))
            .withRetryAttempt((short) 1)
            .withPreviousState(MirrorPartitionState.MIRRORING)
            .build();

        assertEquals(metadata, new MirrorPartitionMetadata.Builder()
            .withState(MirrorPartitionState.FAILED)
            .withStateEpoch(2)
            .withLastMirror(new LastMirror(100L, 10))
            .withErrorMessage(Optional.of("Error"))
            .withRetryAttempt((short) 1)
            .withPreviousState(MirrorPartitionState.MIRRORING)
            .build());
        assertNotEquals(metadata, new MirrorPartitionMetadata.Builder(metadata).withState(MirrorPartitionState.STOPPED).build());
        assertNotEquals(metadata, new MirrorPartitionMetadata.Builder(metadata).withStateEpoch(3).build());
        assertNotEquals(metadata, new MirrorPartitionMetadata.Builder(metadata).withLastMirror(new LastMirror(200L, 20)).build());
        assertNotEquals(metadata, new MirrorPartitionMetadata.Builder(metadata).withErrorMessage(Optional.of("Other")).build());
        assertNotEquals(metadata, new MirrorPartitionMetadata.Builder(metadata).withRetryAttempt((short) 2).build());
        assertNotEquals(metadata, new MirrorPartitionMetadata.Builder(metadata).withPreviousState(MirrorPartitionState.STOPPING).build());
    }

    @Test
    void testOrEmptyWithNull() {
        assertSame(MirrorPartitionMetadata.EMPTY, MirrorPartitionMetadata.orEmpty(null));
    }

    @Test
    void testOrEmptyWithNonNull() {
        MirrorPartitionMetadata metadata = new MirrorPartitionMetadata.Builder()
            .withState(MirrorPartitionState.MIRRORING)
            .build();
        assertSame(metadata, MirrorPartitionMetadata.orEmpty(metadata));
    }

    @ParameterizedTest
    @EnumSource(MirrorPartitionState.class)
    void testSelfTransitionsAreAlwaysValid(MirrorPartitionState state) {
        assertTrue(MirrorPartitionMetadata.isValidStateTransition(state, state));
    }

    @ParameterizedTest
    @EnumSource(MirrorPartitionState.class)
    void testTransitionsToFailedAreAlwaysValid(MirrorPartitionState source) {
        assertTrue(MirrorPartitionMetadata.isValidStateTransition(source, MirrorPartitionState.FAILED));
    }

    @ParameterizedTest
    @EnumSource(value = MirrorPartitionState.class, names = "UNKNOWN", mode = EnumSource.Mode.EXCLUDE)
    void testTransitionsFromFailedAreAlwaysValid(MirrorPartitionState target) {
        assertTrue(MirrorPartitionMetadata.isValidStateTransition(MirrorPartitionState.FAILED, target));
    }

    @Test
    void testTransitionToUnknownIsInvalid() {
        assertFalse(MirrorPartitionMetadata.isValidStateTransition(MirrorPartitionState.FAILED, MirrorPartitionState.UNKNOWN));
        assertFalse(MirrorPartitionMetadata.isValidStateTransition(MirrorPartitionState.MIRRORING, MirrorPartitionState.UNKNOWN));
        assertFalse(MirrorPartitionMetadata.isValidStateTransition(MirrorPartitionState.STOPPED, MirrorPartitionState.UNKNOWN));
    }

    @Test
    void testTransitionFromNullSource() {
        assertFalse(MirrorPartitionMetadata.isValidStateTransition(null, MirrorPartitionState.LOG_ALIGNMENT));
        assertFalse(MirrorPartitionMetadata.isValidStateTransition(null, MirrorPartitionState.FAILED));
        assertFalse(MirrorPartitionMetadata.isValidStateTransition(null, MirrorPartitionState.MIRRORING));
        assertFalse(MirrorPartitionMetadata.isValidStateTransition(null, MirrorPartitionState.STOPPING));
    }

    @Test
    void testTransitionToNullTarget() {
        assertFalse(MirrorPartitionMetadata.isValidStateTransition(null, null));
        assertFalse(MirrorPartitionMetadata.isValidStateTransition(MirrorPartitionState.MIRRORING, null));
        assertFalse(MirrorPartitionMetadata.isValidStateTransition(MirrorPartitionState.FAILED, null));
    }

    @ParameterizedTest(name = "{0} -> {1}")
    @MethodSource("validTransitions")
    void testValidStateTransitions(MirrorPartitionState source, MirrorPartitionState target) {
        assertTrue(MirrorPartitionMetadata.isValidStateTransition(source, target));
    }

    // FAILED and self-transitions are excluded here because they are tested separately
    static Stream<Arguments> validTransitions() {
        return Stream.of(
            Arguments.of(MirrorPartitionState.UNKNOWN, MirrorPartitionState.LOG_ALIGNMENT),
            Arguments.of(MirrorPartitionState.STOPPED, MirrorPartitionState.LOG_ALIGNMENT),
            Arguments.of(MirrorPartitionState.MIRRORING, MirrorPartitionState.EPOCH_FENCING),
            Arguments.of(MirrorPartitionState.MIRRORING, MirrorPartitionState.ULE_RECOVERY),
            Arguments.of(MirrorPartitionState.MIRRORING, MirrorPartitionState.PAUSING),
            Arguments.of(MirrorPartitionState.LOG_ALIGNMENT, MirrorPartitionState.MIRRORING),
            Arguments.of(MirrorPartitionState.EPOCH_FENCING, MirrorPartitionState.MIRRORING),
            Arguments.of(MirrorPartitionState.PAUSED, MirrorPartitionState.MIRRORING),
            Arguments.of(MirrorPartitionState.ULE_RECOVERY, MirrorPartitionState.MIRRORING),
            Arguments.of(MirrorPartitionState.PAUSING, MirrorPartitionState.PAUSED),
            Arguments.of(MirrorPartitionState.MIRRORING, MirrorPartitionState.STOPPING),
            Arguments.of(MirrorPartitionState.PAUSED, MirrorPartitionState.STOPPING),
            Arguments.of(MirrorPartitionState.STOPPING, MirrorPartitionState.STOPPED)
        );
    }

    @ParameterizedTest(name = "{0} -> {1}")
    @MethodSource("invalidTransitions")
    void testInvalidStateTransitions(MirrorPartitionState source, MirrorPartitionState target) {
        assertFalse(MirrorPartitionMetadata.isValidStateTransition(source, target));
    }

    // FAILED and self-transitions are excluded here because they are tested separately
    static Stream<Arguments> invalidTransitions() {
        Set<List<MirrorPartitionState>> valid = validTransitions()
            .map(args -> List.of((MirrorPartitionState) args.get()[0], (MirrorPartitionState) args.get()[1]))
            .collect(Collectors.toSet());

        List<Arguments> invalid = Arrays.stream(MirrorPartitionState.values())
            .filter(source -> source != MirrorPartitionState.FAILED)
            .flatMap(source -> Arrays.stream(MirrorPartitionState.values())
                .filter(target -> target != MirrorPartitionState.FAILED && target != source)
                .filter(target -> !valid.contains(List.of(source, target)))
                .map(target -> Arguments.of(source, target)))
            .toList();
        return invalid.stream();
    }

    @Test
    void testStateTransitionEnteringFailed() {
        MirrorPartitionMetadata result = new MirrorPartitionMetadata.Builder()
            .withState(MirrorPartitionState.MIRRORING)
            .withStateTransition(MirrorPartitionState.FAILED, 3,
                Optional.of("Timeout"), false, (short) 5)
            .build();

        assertEquals(MirrorPartitionState.FAILED, result.state());
        assertEquals(3, result.stateEpoch());
        assertEquals(Optional.of("Timeout"), result.errorMessage());
        assertEquals((short) 1, result.retryAttempt());
        assertEquals(MirrorPartitionState.MIRRORING, result.previousState());
    }

    @Test
    void testStateTransitionEnteringFailedNonRetryable() {
        MirrorPartitionMetadata result = new MirrorPartitionMetadata.Builder()
            .withState(MirrorPartitionState.MIRRORING)
            .withStateTransition(MirrorPartitionState.FAILED, 1,
                Optional.of("Fatal"), true, (short) 5)
            .build();

        assertEquals(Optional.of("Fatal"), result.errorMessage());
        assertEquals(MirrorPartitionMetadata.NON_RETRYABLE_ERROR, result.retryAttempt());
        assertEquals(MirrorPartitionState.MIRRORING, result.previousState());
    }

    @Test
    void testNonRetryableErrorIsPreservedAcrossConsecutiveFailures() {
        MirrorPartitionMetadata failed = new MirrorPartitionMetadata.Builder()
            .withState(MirrorPartitionState.MIRRORING)
            .withStateTransition(MirrorPartitionState.FAILED, 1,
                Optional.of("Fatal"), true, (short) 5)
            .build();

        assertEquals(MirrorPartitionMetadata.NON_RETRYABLE_ERROR, failed.retryAttempt());

        // Consecutive failure with nonRetryable=false must stay NON_RETRYABLE_ERROR
        MirrorPartitionMetadata result = new MirrorPartitionMetadata.Builder(failed)
            .withStateTransition(MirrorPartitionState.FAILED, 2,
                Optional.of("Still fatal"), false, (short) 5)
            .build();

        assertEquals(MirrorPartitionMetadata.NON_RETRYABLE_ERROR, result.retryAttempt());
        assertEquals(MirrorPartitionState.MIRRORING, result.previousState());
    }

    @Test
    void testNonRetryableErrorSticksUntilReset() {
        // MIRRORING -> FAILED (non-retryable)
        MirrorPartitionMetadata failed = new MirrorPartitionMetadata.Builder()
            .withState(MirrorPartitionState.MIRRORING)
            .withStateTransition(MirrorPartitionState.FAILED, 1,
                Optional.of("Fatal"), true, (short) 5)
            .build();

        // FAILED -> MIRRORING: NON_RETRYABLE_ERROR is preserved
        MirrorPartitionMetadata recovered = new MirrorPartitionMetadata.Builder(failed)
            .withStateTransition(MirrorPartitionState.MIRRORING, 2,
                Optional.empty(), false, (short) 5)
            .build();

        assertEquals(MirrorPartitionMetadata.NON_RETRYABLE_ERROR, recovered.retryAttempt());

        // MIRRORING -> FAILED (retryable): stays non-retryable because reset was never called
        MirrorPartitionMetadata failedAgain = new MirrorPartitionMetadata.Builder(recovered)
            .withStateTransition(MirrorPartitionState.FAILED, 3,
                Optional.of("Retryable this time"), false, (short) 5)
            .build();

        assertEquals(MirrorPartitionMetadata.NON_RETRYABLE_ERROR, failedAgain.retryAttempt());

        // withFailedStateReset clears the sticky sentinel
        MirrorPartitionMetadata reset = new MirrorPartitionMetadata.Builder(failedAgain)
            .withStateTransition(MirrorPartitionState.MIRRORING, 4,
                Optional.empty(), false, (short) 5)
            .withFailedStateReset()
            .build();

        assertEquals((short) 0, reset.retryAttempt());

        // Next failure is retryable again
        MirrorPartitionMetadata retryable = new MirrorPartitionMetadata.Builder(reset)
            .withStateTransition(MirrorPartitionState.FAILED, 5,
                Optional.of("Now retryable"), false, (short) 5)
            .build();

        assertEquals((short) 1, retryable.retryAttempt());
    }

    @Test
    void testStateTransitionEnteringFailedZeroMaxRetries() {
        // maxRetryAttempts == 0 means no retry, distinct from NON_RETRYABLE_ERROR (-1)
        MirrorPartitionMetadata result = new MirrorPartitionMetadata.Builder()
            .withState(MirrorPartitionState.MIRRORING)
            .withStateTransition(MirrorPartitionState.FAILED, 1,
                Optional.of("No retries"), false, (short) 0)
            .build();

        assertEquals(Optional.of("No retries"), result.errorMessage());
        assertEquals((short) 0, result.retryAttempt());
        assertEquals(MirrorPartitionState.MIRRORING, result.previousState());
    }

    @Test
    void testStateTransitionNegativeMaxRetryAttempts() {
        assertThrows(IllegalArgumentException.class, () -> new MirrorPartitionMetadata.Builder()
            .withState(MirrorPartitionState.MIRRORING)
            .withStateTransition(MirrorPartitionState.FAILED, 1,
                Optional.of("Error"), false, (short) -1));
    }

    @Test
    void testStateTransitionRespectsMaxRetryAttempts() {
        MirrorPartitionMetadata failed = new MirrorPartitionMetadata.Builder()
            .withState(MirrorPartitionState.FAILED)
            .withRetryAttempt((short) 2)
            .withPreviousState(MirrorPartitionState.MIRRORING)
            .build();

        // retryAttempt reaches maxRetryAttempts
        MirrorPartitionMetadata capped = new MirrorPartitionMetadata.Builder(failed)
            .withStateTransition(MirrorPartitionState.FAILED, 2,
                Optional.of("Retry"), false, (short) 3)
            .build();

        assertEquals((short) 3, capped.retryAttempt());

        // retryAttempt stays at maxRetryAttempts on another consecutive failure
        MirrorPartitionMetadata stillCapped = new MirrorPartitionMetadata.Builder(capped)
            .withStateTransition(MirrorPartitionState.FAILED, 3,
                Optional.of("Retry again"), false, (short) 3)
            .build();

        assertEquals((short) 3, stillCapped.retryAttempt());

        // retryAttempt at Short.MAX_VALUE with max = Short.MAX_VALUE stays pinned
        MirrorPartitionMetadata atMax = new MirrorPartitionMetadata.Builder()
            .withState(MirrorPartitionState.FAILED)
            .withRetryAttempt(Short.MAX_VALUE)
            .withPreviousState(MirrorPartitionState.MIRRORING)
            .build();

        MirrorPartitionMetadata stillAtMax = new MirrorPartitionMetadata.Builder(atMax)
            .withStateTransition(MirrorPartitionState.FAILED, 4,
                Optional.of("Overflow"), false, Short.MAX_VALUE)
            .build();

        assertEquals(Short.MAX_VALUE, stillAtMax.retryAttempt());

        // retryAttempt > max clamps down after a config change
        MirrorPartitionMetadata highRetry = new MirrorPartitionMetadata.Builder()
            .withState(MirrorPartitionState.FAILED)
            .withRetryAttempt((short) 10)
            .withPreviousState(MirrorPartitionState.MIRRORING)
            .build();

        MirrorPartitionMetadata clampedDown = new MirrorPartitionMetadata.Builder(highRetry)
            .withStateTransition(MirrorPartitionState.FAILED, 5,
                Optional.of("Config changed"), false, (short) 5)
            .build();

        assertEquals((short) 5, clampedDown.retryAttempt());
    }

    @Test
    void testStateTransitionConsecutiveFailure() {
        MirrorPartitionMetadata failed = new MirrorPartitionMetadata.Builder()
                .withState(MirrorPartitionState.FAILED)
                .withRetryAttempt((short) 1)
                .withPreviousState(MirrorPartitionState.MIRRORING)
                .build();

        MirrorPartitionMetadata result = new MirrorPartitionMetadata.Builder(failed)
                .withStateTransition(MirrorPartitionState.FAILED, 2,
                        Optional.of("Retry"), false, (short) 5)
                .build();

        assertEquals(Optional.of("Retry"), result.errorMessage());
        assertEquals((short) 2, result.retryAttempt());
        assertEquals(MirrorPartitionState.MIRRORING, result.previousState());
    }

    @Test
    void testStateTransitionConsecutiveFailureFromUnknown() {
        // UNKNOWN -> FAILED: previousState is UNKNOWN (the original pre-failure state)
        MirrorPartitionMetadata failed = new MirrorPartitionMetadata.Builder()
            .withStateTransition(MirrorPartitionState.FAILED, 1,
                Optional.of("Error"), false, (short) 5)
            .build();

        assertEquals(MirrorPartitionState.UNKNOWN, failed.previousState());

        // FAILED -> FAILED: previousState must stay UNKNOWN, not be overwritten with FAILED
        MirrorPartitionMetadata result = new MirrorPartitionMetadata.Builder(failed)
            .withStateTransition(MirrorPartitionState.FAILED, 2,
                Optional.of("Retry"), false, (short) 5)
            .build();

        assertEquals(Optional.of("Retry"), result.errorMessage());
        assertEquals((short) 2, result.retryAttempt());
        assertEquals(MirrorPartitionState.UNKNOWN, result.previousState());
    }

    @ParameterizedTest
    @EnumSource(value = MirrorPartitionState.class, names = {"STOPPED", "PAUSED"})
    void testTransitionClearsErrorFieldsWhenEnteringStoppedOrPaused(MirrorPartitionState target) {
        MirrorPartitionState source = target == MirrorPartitionState.STOPPED
            ? MirrorPartitionState.STOPPING : MirrorPartitionState.PAUSING;
        MirrorPartitionMetadata result = new MirrorPartitionMetadata.Builder()
            .withState(source)
            .withErrorMessage(Optional.of("Old error"))
            .withRetryAttempt((short) 3)
            .withPreviousState(MirrorPartitionState.MIRRORING)
            .withStateTransition(target, 1,
                Optional.of("Ignored"), false, (short) 5)
            .build();

        assertEquals(Optional.empty(), result.errorMessage());
        assertEquals((short) 0, result.retryAttempt());
        assertEquals(MirrorPartitionState.UNKNOWN, result.previousState());
    }

    @ParameterizedTest
    @EnumSource(value = MirrorPartitionState.class, names = {"STOPPED", "PAUSED"})
    void testTransitionClearsErrorFieldsWhenEnteringStoppedOrPausedFromFailed(MirrorPartitionState target) {
        MirrorPartitionMetadata failed = new MirrorPartitionMetadata.Builder()
                .withState(MirrorPartitionState.MIRRORING)
                .withStateTransition(MirrorPartitionState.FAILED, 1, Optional.of("boom"), false, (short) 5)
                .build();

        MirrorPartitionMetadata result = new MirrorPartitionMetadata.Builder(failed)
                .withStateTransition(target, 2, Optional.of("ignored"), false, (short) 5)
                .build();

        assertEquals(target, result.state());
        assertEquals(Optional.empty(), result.errorMessage());
        assertEquals((short) 0, result.retryAttempt());
        assertEquals(MirrorPartitionState.UNKNOWN, result.previousState());
    }

    // Recovery is tentative: the partition might fail again shortly after, and the cumulative
    // failure count needs to survive so the coordinator knows how many attempts have been made
    @Test
    void testTransitionFromFailedClearsErrorMessageAndPreservesRetryFields() {
        MirrorPartitionMetadata failed = new MirrorPartitionMetadata.Builder()
            .withState(MirrorPartitionState.FAILED)
            .withErrorMessage(Optional.of("Error"))
            .withRetryAttempt((short) 2)
            .withPreviousState(MirrorPartitionState.MIRRORING)
            .build();

        MirrorPartitionMetadata result = new MirrorPartitionMetadata.Builder(failed)
            .withStateTransition(MirrorPartitionState.MIRRORING, 2,
                Optional.of("Ignored"), false, (short) 5)
            .build();

        assertEquals(Optional.empty(), result.errorMessage());
        assertEquals((short) 2, result.retryAttempt());
        assertEquals(MirrorPartitionState.MIRRORING, result.previousState());
    }

    // Clearing here is defensive rather than functionally necessary
    @Test
    void testTransitionSameStateClearsErrorMessage() {
        MirrorPartitionMetadata result = new MirrorPartitionMetadata.Builder()
            .withState(MirrorPartitionState.MIRRORING)
            .withErrorMessage(Optional.of("Old error"))
            .withRetryAttempt((short) 2)
            .withPreviousState(MirrorPartitionState.STOPPING)
            .withStateTransition(MirrorPartitionState.MIRRORING, 1,
                Optional.of("Ignored"), false, (short) 5)
            .build();

        assertEquals(Optional.empty(), result.errorMessage());
        assertEquals((short) 2, result.retryAttempt());
        assertEquals(MirrorPartitionState.STOPPING, result.previousState());
    }

    @Test
    void testStateTransitionSetsState() {
        MirrorPartitionMetadata mirroring = new MirrorPartitionMetadata.Builder()
            .withState(MirrorPartitionState.MIRRORING)
            .withStateEpoch(5)
            .withLastMirror(new LastMirror(200L, 20))
            .build();

        MirrorPartitionMetadata failed = new MirrorPartitionMetadata.Builder(mirroring)
            .withStateTransition(MirrorPartitionState.FAILED, 6,
                Optional.of("Timeout"), false, (short) 3)
            .build();

        assertEquals(MirrorPartitionState.FAILED, failed.state());
        assertEquals(6, failed.stateEpoch());
        assertEquals(new LastMirror(200L, 20), failed.lastMirror());
        assertEquals(Optional.of("Timeout"), failed.errorMessage());
        assertEquals((short) 1, failed.retryAttempt());
        assertEquals(MirrorPartitionState.MIRRORING, failed.previousState());

        // Recover from failure
        MirrorPartitionMetadata recovered = new MirrorPartitionMetadata.Builder(failed)
            .withStateTransition(MirrorPartitionState.MIRRORING, 7,
                Optional.empty(), false, (short) 3)
            .build();

        assertEquals(MirrorPartitionState.MIRRORING, recovered.state());
        assertEquals(7, recovered.stateEpoch());
        assertEquals(new LastMirror(200L, 20), recovered.lastMirror());
        assertEquals(Optional.empty(), recovered.errorMessage());
        assertEquals((short) 1, recovered.retryAttempt());
        assertEquals(MirrorPartitionState.MIRRORING, recovered.previousState());
    }

    @Test
    void testFailedStateResetAfterRecovery() {
        MirrorPartitionMetadata failed = new MirrorPartitionMetadata.Builder()
            .withState(MirrorPartitionState.MIRRORING)
            .withStateTransition(MirrorPartitionState.FAILED, 2,
                Optional.of("Timeout"), false, (short) 5)
            .build();

        assertEquals((short) 1, failed.retryAttempt());
        assertEquals(MirrorPartitionState.MIRRORING, failed.previousState());

        MirrorPartitionMetadata recovered = new MirrorPartitionMetadata.Builder(failed)
            .withStateTransition(MirrorPartitionState.MIRRORING, 3,
                Optional.empty(), false, (short) 5)
            .build();

        assertEquals(MirrorPartitionState.MIRRORING, recovered.state());
        assertEquals((short) 1, recovered.retryAttempt());
        assertEquals(MirrorPartitionState.MIRRORING, recovered.previousState());

        MirrorPartitionMetadata reset = new MirrorPartitionMetadata.Builder(recovered)
            .withFailedStateReset()
            .build();

        assertEquals(MirrorPartitionState.MIRRORING, reset.state());
        assertEquals(3, reset.stateEpoch());
        assertEquals(Optional.empty(), reset.errorMessage());
        assertEquals((short) 0, reset.retryAttempt());
        assertEquals(MirrorPartitionState.UNKNOWN, reset.previousState());
    }

    @Test
    void testFailedStateResetClearsNonRetryableAfterRecovery() {
        MirrorPartitionMetadata failed = new MirrorPartitionMetadata.Builder()
            .withState(MirrorPartitionState.MIRRORING)
            .withStateTransition(MirrorPartitionState.FAILED, 1,
                Optional.of("Fatal"), true, (short) 5)
            .build();

        assertEquals(MirrorPartitionMetadata.NON_RETRYABLE_ERROR, failed.retryAttempt());

        MirrorPartitionMetadata recovered = new MirrorPartitionMetadata.Builder(failed)
            .withStateTransition(MirrorPartitionState.MIRRORING, 2,
                Optional.empty(), false, (short) 5)
            .build();

        assertEquals(MirrorPartitionMetadata.NON_RETRYABLE_ERROR, recovered.retryAttempt());

        MirrorPartitionMetadata reset = new MirrorPartitionMetadata.Builder(recovered)
            .withFailedStateReset()
            .build();

        assertEquals(MirrorPartitionState.MIRRORING, reset.state());
        assertEquals(Optional.empty(), reset.errorMessage());
        assertEquals((short) 0, reset.retryAttempt());
        assertEquals(MirrorPartitionState.UNKNOWN, reset.previousState());
    }
}
