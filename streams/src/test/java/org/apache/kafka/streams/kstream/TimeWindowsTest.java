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
package org.apache.kafka.streams.kstream;

import org.apache.kafka.streams.kstream.internals.TimeWindow;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static java.time.Duration.ofMillis;
import static org.apache.kafka.streams.EqualityCheck.verifyEquality;
import static org.apache.kafka.streams.EqualityCheck.verifyInEquality;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

public class TimeWindowsTest {

    private static final long ANY_SIZE = 123L;
    private static final long ANY_GRACE = 1024L;

    @Test
    public void shouldSetWindowSize() {
        assertEquals(ANY_SIZE, TimeWindows.ofSizeWithNoGrace(ofMillis(ANY_SIZE)).sizeMs);
        assertEquals(ANY_SIZE, TimeWindows.ofSizeAndGrace(ofMillis(ANY_SIZE), ofMillis(ANY_GRACE)).sizeMs);
    }

    @Test
    public void shouldSetWindowAdvance() {
        final long anyAdvance = 4;
        assertEquals(anyAdvance, TimeWindows.ofSizeWithNoGrace(ofMillis(ANY_SIZE)).advanceBy(ofMillis(anyAdvance)).advanceMs);
    }

    @Test
    public void windowSizeMustNotBeZero() {
        assertThrows(IllegalArgumentException.class, () -> TimeWindows.ofSizeWithNoGrace(ofMillis(0)));
    }

    @Test
    public void windowSizeMustNotBeNegative() {
        assertThrows(IllegalArgumentException.class, () -> TimeWindows.ofSizeWithNoGrace(ofMillis(-1)));
    }

    @Test
    public void advanceIntervalMustNotBeZero() {
        final TimeWindows windowSpec = TimeWindows.ofSizeWithNoGrace(ofMillis(ANY_SIZE));
        try {
            windowSpec.advanceBy(ofMillis(0));
            fail("should not accept zero advance parameter");
        } catch (final IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void advanceIntervalMustNotBeNegative() {
        final TimeWindows windowSpec = TimeWindows.ofSizeWithNoGrace(ofMillis(ANY_SIZE));
        try {
            windowSpec.advanceBy(ofMillis(-1));
            fail("should not accept negative advance parameter");
        } catch (final IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void advanceIntervalMustNotBeLargerThanWindowSize() {
        final TimeWindows windowSpec = TimeWindows.ofSizeWithNoGrace(ofMillis(ANY_SIZE));
        try {
            windowSpec.advanceBy(ofMillis(ANY_SIZE + 1));
            fail("should not accept advance greater than window size");
        } catch (final IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void gracePeriodShouldEnforceBoundaries() {
        TimeWindows.ofSizeAndGrace(ofMillis(3L), ofMillis(0L));

        try {
            TimeWindows.ofSizeAndGrace(ofMillis(3L), ofMillis(-1L));
            fail("should not accept negatives");
        } catch (final IllegalArgumentException e) {
            //expected
        }
    }

    @Test
    public void shouldComputeWindowsForHoppingWindows() {
        final TimeWindows windows = TimeWindows.ofSizeWithNoGrace(ofMillis(12L)).advanceBy(ofMillis(5L));
        final Map<Long, TimeWindow> matched = windows.windowsFor(21L);
        assertEquals(12L / 5L + 1, matched.size());
        assertEquals(new TimeWindow(10L, 22L), matched.get(10L));
        assertEquals(new TimeWindow(15L, 27L), matched.get(15L));
        assertEquals(new TimeWindow(20L, 32L), matched.get(20L));
    }

    @Test
    public void shouldComputeWindowsForBarelyOverlappingHoppingWindows() {
        final TimeWindows windows = TimeWindows.ofSizeWithNoGrace(ofMillis(6L)).advanceBy(ofMillis(5L));
        final Map<Long, TimeWindow> matched = windows.windowsFor(7L);
        assertEquals(1, matched.size());
        assertEquals(new TimeWindow(5L, 11L), matched.get(5L));
    }

    @Test
    public void shouldComputeWindowsForTumblingWindows() {
        final TimeWindows windows = TimeWindows.ofSizeWithNoGrace(ofMillis(12L));
        final Map<Long, TimeWindow> matched = windows.windowsFor(21L);
        assertEquals(1, matched.size());
        assertEquals(new TimeWindow(12L, 24L), matched.get(12L));
    }


    @Test
    public void equalsAndHashcodeShouldBeValidForPositiveCases() {
        verifyEquality(TimeWindows.ofSizeWithNoGrace(ofMillis(3)), TimeWindows.ofSizeWithNoGrace(ofMillis(3)));

        verifyEquality(TimeWindows.ofSizeWithNoGrace(ofMillis(3)).advanceBy(ofMillis(1)), TimeWindows.ofSizeWithNoGrace(ofMillis(3)).advanceBy(ofMillis(1)));

        verifyEquality(TimeWindows.ofSizeAndGrace(ofMillis(3), ofMillis(4)), TimeWindows.ofSizeAndGrace(ofMillis(3), ofMillis(4)));

        verifyEquality(TimeWindows.ofSizeAndGrace(ofMillis(3), ofMillis(33)),
                TimeWindows.ofSizeAndGrace(ofMillis(3), ofMillis(33))
        );
    }

    @Test
    public void equalsAndHashcodeShouldBeValidForNegativeCases() {

        verifyInEquality(
                TimeWindows.ofSizeWithNoGrace(ofMillis(9)),
                TimeWindows.ofSizeWithNoGrace(ofMillis(3))
        );

        verifyInEquality(
                TimeWindows.ofSizeAndGrace(ofMillis(9), ofMillis(9)),
                TimeWindows.ofSizeAndGrace(ofMillis(3), ofMillis(9))
        );

        verifyInEquality(TimeWindows.ofSizeWithNoGrace(ofMillis(3)).advanceBy(ofMillis(2)), TimeWindows.ofSizeWithNoGrace(ofMillis(3)).advanceBy(ofMillis(1)));

        verifyInEquality(TimeWindows.ofSizeAndGrace(ofMillis(3), ofMillis(2)), TimeWindows.ofSizeAndGrace(ofMillis(3), ofMillis(1)));

        verifyInEquality(TimeWindows.ofSizeAndGrace(ofMillis(3), ofMillis(9)), TimeWindows.ofSizeAndGrace(ofMillis(3), ofMillis(4)));

        verifyInEquality(
            TimeWindows.ofSizeAndGrace(ofMillis(4), ofMillis(2)).advanceBy(ofMillis(2)),
            TimeWindows.ofSizeAndGrace(ofMillis(3), ofMillis(2)).advanceBy(ofMillis(2))
        );

        verifyInEquality(
            TimeWindows.ofSizeAndGrace(ofMillis(3), ofMillis(2)).advanceBy(ofMillis(1)),
            TimeWindows.ofSizeAndGrace(ofMillis(3), ofMillis(2)).advanceBy(ofMillis(2))
        );

        assertNotEquals(
            TimeWindows.ofSizeAndGrace(ofMillis(3), ofMillis(1)).advanceBy(ofMillis(2)),
            TimeWindows.ofSizeAndGrace(ofMillis(3), ofMillis(2)).advanceBy(ofMillis(2))
        );
    }
}
