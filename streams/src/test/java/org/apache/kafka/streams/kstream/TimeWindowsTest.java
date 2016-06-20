/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.kafka.streams.kstream;

import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TimeWindowsTest {

    private static long anySize = 123L;

    @Test
    public void shouldHaveSaneEqualsAndHashCode() {
        TimeWindows w1 = TimeWindows.of(anySize);
        TimeWindows w2 = TimeWindows.of(w1.size);

        // Reflexive
        assertTrue(w1.equals(w1));
        assertTrue(w1.hashCode() == w1.hashCode());

        // Symmetric
        assertTrue(w1.equals(w2));
        assertTrue(w1.hashCode() == w2.hashCode());
        assertTrue(w2.hashCode() == w1.hashCode());

        // Transitive
        TimeWindows w3 = TimeWindows.of(w2.size);
        assertTrue(w2.equals(w3));
        assertTrue(w2.hashCode() == w3.hashCode());
        assertTrue(w1.equals(w3));
        assertTrue(w1.hashCode() == w3.hashCode());

        // Inequality scenarios
        assertFalse("must be false for null", w1.equals(null));
        assertFalse("must be false for different window types", w1.equals(UnlimitedWindows.of()));
        assertFalse("must be false for different types", w1.equals(new Object()));

        TimeWindows differentWindowSize = TimeWindows.of(w1.size + 1);
        assertFalse("must be false when window sizes are different", w1.equals(differentWindowSize));

        TimeWindows differentAdvanceInterval = w1.advanceBy(w1.advance - 1);
        assertFalse("must be false when advance intervals are different", w1.equals(differentAdvanceInterval));
    }


    @Test(expected = IllegalArgumentException.class)
    public void windowSizeMustNotBeNegative() {
        TimeWindows.of(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void windowSizeMustNotBeZero() {
        TimeWindows.of(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void advanceIntervalMustNotBeNegative() {
        TimeWindows.of(anySize).advanceBy(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void advanceIntervalMustNotBeZero() {
        TimeWindows.of(anySize).advanceBy(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void advanceIntervalMustNotBeLargerThanWindowSize() {
        long size = anySize;
        TimeWindows.of(size).advanceBy(size + 1);
    }

    @Test
    public void windowsForHoppingWindows() {
        TimeWindows windows = TimeWindows.of(12L).advanceBy(5L);
        Map<Long, TimeWindow> matched = windows.windowsFor(21L);
        assertEquals(12L / 5L + 1, matched.size());
        assertEquals(new TimeWindow(10L, 22L), matched.get(10L));
        assertEquals(new TimeWindow(15L, 27L), matched.get(15L));
        assertEquals(new TimeWindow(20L, 32L), matched.get(20L));
    }

    @Test
    public void windowsForBarelyOverlappingHoppingWindows() {
        TimeWindows windows = TimeWindows.of(6L).advanceBy(5L);
        Map<Long, TimeWindow> matched = windows.windowsFor(7L);
        assertEquals(1, matched.size());
        assertEquals(new TimeWindow(5L, 11L), matched.get(5L));
    }

    @Test
    public void windowsForTumblingWindows() {
        TimeWindows windows = TimeWindows.of(12L);
        Map<Long, TimeWindow> matched = windows.windowsFor(21L);
        assertEquals(1, matched.size());
        assertEquals(new TimeWindow(12L, 24L), matched.get(12L));
    }

}