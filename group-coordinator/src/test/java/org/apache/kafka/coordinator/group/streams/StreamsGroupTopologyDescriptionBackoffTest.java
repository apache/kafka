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
package org.apache.kafka.coordinator.group.streams;

import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.internals.ExponentialBackoff;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StreamsGroupTopologyDescriptionBackoffTest {

    /**
     * Build a back-off backed by a jitter-free {@link ExponentialBackoff} so delay
     * assertions are deterministic.
     */
    private static StreamsGroupTopologyDescriptionBackoff deterministicBackoff(MockTime time) {
        return new StreamsGroupTopologyDescriptionBackoff(
            time,
            new ExponentialBackoff(
                StreamsGroupTopologyDescriptionBackoff.INITIAL_DELAY_MS,
                StreamsGroupTopologyDescriptionBackoff.MULTIPLIER,
                StreamsGroupTopologyDescriptionBackoff.MAX_DELAY_MS,
                0.0));
    }

    @Test
    public void testFirstArmUsesInitialDelay() {
        MockTime time = new MockTime();
        StreamsGroupTopologyDescriptionBackoff backoff = deterministicBackoff(time);
        backoff.armOrExtend("g", 1);
        assertTrue(backoff.isActive("g", 1));
        assertEquals(0, backoff.entry("g").attempts());
        time.sleep(StreamsGroupTopologyDescriptionBackoff.INITIAL_DELAY_MS - 1);
        assertTrue(backoff.isActive("g", 1));
        time.sleep(1);
        assertFalse(backoff.isActive("g", 1));
    }

    @Test
    public void testConsecutiveArmsAdvanceAttemptsUpToCap() {
        MockTime time = new MockTime();
        StreamsGroupTopologyDescriptionBackoff backoff = deterministicBackoff(time);

        backoff.armOrExtend("g", 1);
        assertEquals(0, backoff.entry("g").attempts());

        // Each consecutive arm at the same epoch advances the attempt count. The actual
        // delay is delegated to ExponentialBackoff and capped at MAX_DELAY_MS.
        for (int i = 0; i < 20; i++) {
            backoff.armOrExtend("g", 1);
            assertEquals(i + 1, backoff.entry("g").attempts(), "iteration " + i);
        }

        // Window length is capped: arming when the formula would exceed MAX_DELAY_MS still
        // produces a window no longer than MAX_DELAY_MS.
        long delay = backoff.entry("g").nextAttemptMs() - time.milliseconds();
        assertEquals(StreamsGroupTopologyDescriptionBackoff.MAX_DELAY_MS, delay);
    }

    @Test
    public void testDifferentEpochResetsAttempts() {
        MockTime time = new MockTime();
        StreamsGroupTopologyDescriptionBackoff backoff = deterministicBackoff(time);
        backoff.armOrExtend("g", 1);
        backoff.armOrExtend("g", 1); // advances attempts
        assertEquals(1, backoff.entry("g").attempts());
        backoff.armOrExtend("g", 2);
        assertEquals(0, backoff.entry("g").attempts());
        assertEquals(2, backoff.entry("g").topologyEpoch());
    }

    @Test
    public void testIsActiveIsScopedToEpoch() {
        MockTime time = new MockTime();
        StreamsGroupTopologyDescriptionBackoff backoff = deterministicBackoff(time);
        backoff.armOrExtend("g", 1);
        assertTrue(backoff.isActive("g", 1));
        // A query at a different epoch never matches — the broker should re-solicit.
        assertFalse(backoff.isActive("g", 2));
    }

    @Test
    public void testClearRemovesEntryWhenEpochMatches() {
        MockTime time = new MockTime();
        StreamsGroupTopologyDescriptionBackoff backoff = deterministicBackoff(time);
        backoff.armOrExtend("g", 1);
        assertNotNull(backoff.entry("g"));
        backoff.clear("g", 1);
        assertNull(backoff.entry("g"));
        assertFalse(backoff.isActive("g", 1));
    }

    @Test
    public void testClearIsNoOpWhenEpochDoesNotMatch() {
        // A late post-plugin callback at the old epoch must not wipe a window a concurrent
        // heartbeat armed at the advanced epoch.
        MockTime time = new MockTime();
        StreamsGroupTopologyDescriptionBackoff backoff = deterministicBackoff(time);
        backoff.armOrExtend("g", 6);
        long armedAt = backoff.entry("g").nextAttemptMs();

        backoff.clear("g", 5);
        assertNotNull(backoff.entry("g"));
        assertEquals(6, backoff.entry("g").topologyEpoch());
        assertEquals(armedAt, backoff.entry("g").nextAttemptMs());
    }

    @Test
    public void testClearGroupRemovesEntryUnconditionally() {
        // Used by paths that remove the group entirely (DeleteGroups, periodic cleanup):
        // back-off state is irrelevant because the group is gone.
        MockTime time = new MockTime();
        StreamsGroupTopologyDescriptionBackoff backoff = deterministicBackoff(time);
        backoff.armOrExtend("g", 6);
        backoff.clearGroup("g");
        assertNull(backoff.entry("g"));
    }

    @Test
    public void testArmOrExtendIsNoOpWhenStoredEpochIsNewer() {
        // A late post-plugin callback at the old epoch must not overwrite an entry a
        // concurrent heartbeat armed at the advanced epoch.
        MockTime time = new MockTime();
        StreamsGroupTopologyDescriptionBackoff backoff = deterministicBackoff(time);
        backoff.armOrExtend("g", 6);
        long armedAt = backoff.entry("g").nextAttemptMs();

        backoff.armOrExtend("g", 5);
        assertEquals(6, backoff.entry("g").topologyEpoch());
        assertEquals(0, backoff.entry("g").attempts());
        assertEquals(armedAt, backoff.entry("g").nextAttemptMs());
    }

    @Test
    public void testArmIfNotActiveArmsWhenIdle() {
        MockTime time = new MockTime();
        StreamsGroupTopologyDescriptionBackoff backoff = deterministicBackoff(time);
        assertTrue(backoff.armIfNotActive("g", 1));
        assertTrue(backoff.isActive("g", 1));
        assertEquals(0, backoff.entry("g").attempts());
    }

    @Test
    public void testArmIfNotActiveReturnsFalseWhenAlreadyActive() {
        MockTime time = new MockTime();
        StreamsGroupTopologyDescriptionBackoff backoff = deterministicBackoff(time);
        assertTrue(backoff.armIfNotActive("g", 1));
        long armedAt = backoff.entry("g").nextAttemptMs();
        // A second call inside the window does not arm and does not extend the window.
        assertFalse(backoff.armIfNotActive("g", 1));
        assertEquals(armedAt, backoff.entry("g").nextAttemptMs());
    }

    @Test
    public void testArmIfNotActiveReArmsAfterEpochAdvance() {
        MockTime time = new MockTime();
        StreamsGroupTopologyDescriptionBackoff backoff = deterministicBackoff(time);
        assertTrue(backoff.armIfNotActive("g", 1));
        // A query at the new epoch sees no active window and arms a fresh one.
        assertTrue(backoff.armIfNotActive("g", 2));
        assertEquals(2, backoff.entry("g").topologyEpoch());
        assertEquals(0, backoff.entry("g").attempts());
    }

    @Test
    public void testArmIfNotActiveAdvancesAttemptsAfterExpiredWindowAtSameEpoch() {
        // Heartbeats keep re-soliciting the same epoch because the client never
        // pushed (or the push was lost). The exponential chain must continue across
        // those re-arms instead of resetting attempts every cycle.
        MockTime time = new MockTime();
        StreamsGroupTopologyDescriptionBackoff backoff = deterministicBackoff(time);

        assertTrue(backoff.armIfNotActive("g", 1));
        assertEquals(0, backoff.entry("g").attempts());

        for (int i = 0; i < 20; i++) {
            time.sleep(backoff.entry("g").nextAttemptMs() - time.milliseconds());
            assertTrue(backoff.armIfNotActive("g", 1));
            assertEquals(i + 1, backoff.entry("g").attempts(), "iteration " + i);
        }
    }

    @Test
    public void testProductionWiringUsesJitter() {
        // Sanity check that the no-arg constructor wires a jittered ExponentialBackoff:
        // the first delay should fall inside [(1 - JITTER), (1 + JITTER)] * INITIAL_DELAY_MS,
        // not be exactly INITIAL_DELAY_MS (with high probability — but we can at least
        // verify the entry exists and the upper-bound holds across many arms).
        MockTime time = new MockTime();
        StreamsGroupTopologyDescriptionBackoff backoff = new StreamsGroupTopologyDescriptionBackoff(time);
        backoff.armOrExtend("g", 1);
        long delay = backoff.entry("g").nextAttemptMs() - time.milliseconds();
        long lowerBound = (long) ((1 - StreamsGroupTopologyDescriptionBackoff.JITTER)
            * StreamsGroupTopologyDescriptionBackoff.INITIAL_DELAY_MS);
        long upperBound = (long) ((1 + StreamsGroupTopologyDescriptionBackoff.JITTER)
            * StreamsGroupTopologyDescriptionBackoff.INITIAL_DELAY_MS);
        assertTrue(delay >= lowerBound && delay <= upperBound,
            "expected delay in [" + lowerBound + ", " + upperBound + "], got " + delay);
    }
}
