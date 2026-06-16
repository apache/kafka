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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StreamsGroupTopologyDescriptionBackoffTest {

    @Test
    public void testFirstArmUsesInitialDelay() {
        MockTime time = new MockTime();
        StreamsGroupTopologyDescriptionBackoff backoff = new StreamsGroupTopologyDescriptionBackoff(time);
        backoff.armOrExtend("g", 1);
        assertTrue(backoff.isActive("g", 1));
        time.sleep(StreamsGroupTopologyDescriptionBackoff.INITIAL_DELAY_MS - 1);
        assertTrue(backoff.isActive("g", 1));
        time.sleep(1);
        assertFalse(backoff.isActive("g", 1));
    }

    @Test
    public void testConsecutiveArmsDoubleTheWindowUpToMax() {
        MockTime time = new MockTime();
        StreamsGroupTopologyDescriptionBackoff backoff = new StreamsGroupTopologyDescriptionBackoff(time);

        long expected = StreamsGroupTopologyDescriptionBackoff.INITIAL_DELAY_MS;
        backoff.armOrExtend("g", 1);
        assertEquals(expected, backoff.entry("g").currentDelayMs());

        // Each consecutive arm at the same epoch doubles, until we hit the cap.
        for (int i = 0; i < 20; i++) {
            backoff.armOrExtend("g", 1);
            expected = Math.min(expected * 2, StreamsGroupTopologyDescriptionBackoff.MAX_DELAY_MS);
            assertEquals(expected, backoff.entry("g").currentDelayMs(),
                "iteration " + i);
        }
        assertEquals(StreamsGroupTopologyDescriptionBackoff.MAX_DELAY_MS,
            backoff.entry("g").currentDelayMs());
    }

    @Test
    public void testDifferentEpochResetsTheWindow() {
        MockTime time = new MockTime();
        StreamsGroupTopologyDescriptionBackoff backoff = new StreamsGroupTopologyDescriptionBackoff(time);
        backoff.armOrExtend("g", 1);
        backoff.armOrExtend("g", 1); // doubled
        long doubled = backoff.entry("g").currentDelayMs();
        assertTrue(doubled > StreamsGroupTopologyDescriptionBackoff.INITIAL_DELAY_MS);
        backoff.armOrExtend("g", 2);
        assertEquals(StreamsGroupTopologyDescriptionBackoff.INITIAL_DELAY_MS,
            backoff.entry("g").currentDelayMs());
        assertEquals(2, backoff.entry("g").topologyEpoch());
    }

    @Test
    public void testIsActiveIsScopedToEpoch() {
        MockTime time = new MockTime();
        StreamsGroupTopologyDescriptionBackoff backoff = new StreamsGroupTopologyDescriptionBackoff(time);
        backoff.armOrExtend("g", 1);
        assertTrue(backoff.isActive("g", 1));
        // A query at a different epoch never matches — the broker should re-solicit.
        assertFalse(backoff.isActive("g", 2));
    }

    @Test
    public void testClearRemovesEntry() {
        MockTime time = new MockTime();
        StreamsGroupTopologyDescriptionBackoff backoff = new StreamsGroupTopologyDescriptionBackoff(time);
        backoff.armOrExtend("g", 1);
        assertNotNull(backoff.entry("g"));
        backoff.clear("g");
        assertNull(backoff.entry("g"));
        assertFalse(backoff.isActive("g", 1));
    }

    @Test
    public void testArmIfNotActiveArmsWhenIdle() {
        MockTime time = new MockTime();
        StreamsGroupTopologyDescriptionBackoff backoff = new StreamsGroupTopologyDescriptionBackoff(time);
        assertTrue(backoff.armIfNotActive("g", 1));
        assertTrue(backoff.isActive("g", 1));
        assertEquals(StreamsGroupTopologyDescriptionBackoff.INITIAL_DELAY_MS,
            backoff.entry("g").currentDelayMs());
    }

    @Test
    public void testArmIfNotActiveReturnsFalseWhenAlreadyActive() {
        MockTime time = new MockTime();
        StreamsGroupTopologyDescriptionBackoff backoff = new StreamsGroupTopologyDescriptionBackoff(time);
        assertTrue(backoff.armIfNotActive("g", 1));
        long armedAt = backoff.entry("g").nextAttemptMs();
        // A second call inside the window does not arm and does not extend the window.
        assertFalse(backoff.armIfNotActive("g", 1));
        assertEquals(armedAt, backoff.entry("g").nextAttemptMs());
    }

    @Test
    public void testArmIfNotActiveReArmsAfterEpochAdvance() {
        MockTime time = new MockTime();
        StreamsGroupTopologyDescriptionBackoff backoff = new StreamsGroupTopologyDescriptionBackoff(time);
        assertTrue(backoff.armIfNotActive("g", 1));
        // A query at the new epoch sees no active window and arms a fresh one.
        assertTrue(backoff.armIfNotActive("g", 2));
        assertEquals(2, backoff.entry("g").topologyEpoch());
        assertEquals(StreamsGroupTopologyDescriptionBackoff.INITIAL_DELAY_MS,
            backoff.entry("g").currentDelayMs());
    }

    @Test
    public void testArmIfNotActiveDoublesAfterExpiredWindowAtSameEpoch() {
        // Heartbeats keep re-soliciting the same epoch because the client never
        // pushed (or the push was lost). The exponential chain must continue across
        // those re-arms instead of resetting to INITIAL_DELAY_MS every cycle.
        MockTime time = new MockTime();
        StreamsGroupTopologyDescriptionBackoff backoff = new StreamsGroupTopologyDescriptionBackoff(time);

        long expected = StreamsGroupTopologyDescriptionBackoff.INITIAL_DELAY_MS;
        assertTrue(backoff.armIfNotActive("g", 1));
        assertEquals(expected, backoff.entry("g").currentDelayMs());

        for (int i = 0; i < 20; i++) {
            time.sleep(backoff.entry("g").currentDelayMs());
            assertTrue(backoff.armIfNotActive("g", 1));
            expected = Math.min(expected * 2, StreamsGroupTopologyDescriptionBackoff.MAX_DELAY_MS);
            assertEquals(expected, backoff.entry("g").currentDelayMs(), "iteration " + i);
        }
        assertEquals(StreamsGroupTopologyDescriptionBackoff.MAX_DELAY_MS,
            backoff.entry("g").currentDelayMs());
    }
}
