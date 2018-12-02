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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.kstream.TimeWindows;
import org.junit.Test;

import java.util.Map;

import static java.time.Duration.ofMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TimeWindowTest {

    private long start = 50;
    private long end = 100;
    private final TimeWindow window = new TimeWindow(start, end);
    private final SessionWindow sessionWindow = new SessionWindow(start, end);

    @Test(expected = IllegalArgumentException.class)
    public void endMustBeLargerThanStart() {
        new TimeWindow(start, start);
    }

    @Test
    public void shouldNotOverlapIfOtherWindowIsBeforeThisWindow() {
        /*
         * This:        [-------)
         * Other: [-----)
         */
        assertFalse(window.overlap(new TimeWindow(0, 25)));
        assertFalse(window.overlap(new TimeWindow(0, start - 1)));
        assertFalse(window.overlap(new TimeWindow(0, start)));
    }

    @Test
    public void shouldOverlapIfOtherWindowEndIsWithinThisWindow() {
        /*
         * This:        [-------)
         * Other: [---------)
         */
        assertTrue(window.overlap(new TimeWindow(0, start + 1)));
        assertTrue(window.overlap(new TimeWindow(0, 75)));
        assertTrue(window.overlap(new TimeWindow(0, end - 1)));

        assertTrue(window.overlap(new TimeWindow(start - 1, start + 1)));
        assertTrue(window.overlap(new TimeWindow(start - 1, 75)));
        assertTrue(window.overlap(new TimeWindow(start - 1, end - 1)));
    }

    @Test
    public void shouldOverlapIfOtherWindowContainsThisWindow() {
        /*
         * This:        [-------)
         * Other: [------------------)
         */
        assertTrue(window.overlap(new TimeWindow(0, end)));
        assertTrue(window.overlap(new TimeWindow(0, end + 1)));
        assertTrue(window.overlap(new TimeWindow(0, 150)));

        assertTrue(window.overlap(new TimeWindow(start - 1, end)));
        assertTrue(window.overlap(new TimeWindow(start - 1, end + 1)));
        assertTrue(window.overlap(new TimeWindow(start - 1, 150)));

        assertTrue(window.overlap(new TimeWindow(start, end)));
        assertTrue(window.overlap(new TimeWindow(start, end + 1)));
        assertTrue(window.overlap(new TimeWindow(start, 150)));
    }

    @Test
    public void shouldOverlapIfOtherWindowIsWithinThisWindow() {
        /*
         * This:        [-------)
         * Other:         [---)
         */
        assertTrue(window.overlap(new TimeWindow(start, 75)));
        assertTrue(window.overlap(new TimeWindow(start, end)));
        assertTrue(window.overlap(new TimeWindow(75, end)));
    }

    @Test
    public void shouldOverlapIfOtherWindowStartIsWithinThisWindow() {
        /*
         * This:        [-------)
         * Other:           [-------)
         */
        assertTrue(window.overlap(new TimeWindow(start, end + 1)));
        assertTrue(window.overlap(new TimeWindow(start, 150)));
        assertTrue(window.overlap(new TimeWindow(75, end + 1)));
        assertTrue(window.overlap(new TimeWindow(75, 150)));
    }

    @Test
    public void shouldNotOverlapIsOtherWindowIsAfterThisWindow() {
        /*
         * This:        [-------)
         * Other:               [------)
         */
        assertFalse(window.overlap(new TimeWindow(end, end + 1)));
        assertFalse(window.overlap(new TimeWindow(end, 150)));
        assertFalse(window.overlap(new TimeWindow(end + 1, 150)));
        assertFalse(window.overlap(new TimeWindow(125, 150)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void cannotCompareTimeWindowWithDifferentWindowType() {
        window.overlap(sessionWindow);
    }

    @Test
    public void shouldReturnMatchedWindowsOrderedByTimestamp() {
        final TimeWindows windows = TimeWindows.of(ofMillis(12L)).advanceBy(ofMillis(5L));
        final Map<Long, TimeWindow> matched = windows.windowsFor(21L);

        final Long[] expected = matched.keySet().toArray(new Long[matched.size()]);
        assertEquals(expected[0].longValue(), 10L);
        assertEquals(expected[1].longValue(), 15L);
        assertEquals(expected[2].longValue(), 20L);
    }
}
