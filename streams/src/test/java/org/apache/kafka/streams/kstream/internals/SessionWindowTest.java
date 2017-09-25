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

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SessionWindowTest {

    private long start = 50;
    private long end = 100;
    private final SessionWindow window = new SessionWindow(start, end);
    private final TimeWindow timeWindow = new TimeWindow(start, end);

    @Test
    public void shouldNotOverlapIfOtherWindowIsBeforeThisWindow() {
        /*
         * This:        [-------]
         * Other: [---]
         */
        assertFalse(window.overlap(new SessionWindow(0, 25)));
        assertFalse(window.overlap(new SessionWindow(0, start - 1)));
        assertFalse(window.overlap(new SessionWindow(start - 1, start - 1)));
    }

    @Test
    public void shouldOverlapIfOtherWindowEndIsWithinThisWindow() {
        /*
         * This:        [-------]
         * Other: [---------]
         */
        assertTrue(window.overlap(new SessionWindow(0, start)));
        assertTrue(window.overlap(new SessionWindow(0, start + 1)));
        assertTrue(window.overlap(new SessionWindow(0, 75)));
        assertTrue(window.overlap(new SessionWindow(0, end - 1)));
        assertTrue(window.overlap(new SessionWindow(0, end)));

        assertTrue(window.overlap(new SessionWindow(start - 1, start)));
        assertTrue(window.overlap(new SessionWindow(start - 1, start + 1)));
        assertTrue(window.overlap(new SessionWindow(start - 1, 75)));
        assertTrue(window.overlap(new SessionWindow(start - 1, end - 1)));
        assertTrue(window.overlap(new SessionWindow(start - 1, end)));
    }

    @Test
    public void shouldOverlapIfOtherWindowContainsThisWindow() {
        /*
         * This:        [-------]
         * Other: [------------------]
         */
        assertTrue(window.overlap(new SessionWindow(0, end)));
        assertTrue(window.overlap(new SessionWindow(0, end + 1)));
        assertTrue(window.overlap(new SessionWindow(0, 150)));

        assertTrue(window.overlap(new SessionWindow(start - 1, end)));
        assertTrue(window.overlap(new SessionWindow(start - 1, end + 1)));
        assertTrue(window.overlap(new SessionWindow(start - 1, 150)));

        assertTrue(window.overlap(new SessionWindow(start, end)));
        assertTrue(window.overlap(new SessionWindow(start, end + 1)));
        assertTrue(window.overlap(new SessionWindow(start, 150)));
    }

    @Test
    public void shouldOverlapIfOtherWindowIsWithinThisWindow() {
        /*
         * This:        [-------]
         * Other:         [---]
         */
        assertTrue(window.overlap(new SessionWindow(start, start)));
        assertTrue(window.overlap(new SessionWindow(start, 75)));
        assertTrue(window.overlap(new SessionWindow(start, end)));
        assertTrue(window.overlap(new SessionWindow(75, end)));
        assertTrue(window.overlap(new SessionWindow(end, end)));
    }

    @Test
    public void shouldOverlapIfOtherWindowStartIsWithinThisWindow() {
        /*
         * This:        [-------]
         * Other:           [-------]
         */
        assertTrue(window.overlap(new SessionWindow(start, end + 1)));
        assertTrue(window.overlap(new SessionWindow(start, 150)));
        assertTrue(window.overlap(new SessionWindow(75, end + 1)));
        assertTrue(window.overlap(new SessionWindow(75, 150)));
        assertTrue(window.overlap(new SessionWindow(end, end + 1)));
        assertTrue(window.overlap(new SessionWindow(end, 150)));
    }

    @Test
    public void shouldNotOverlapIsOtherWindowIsAfterThisWindow() {
        /*
         * This:        [-------]
         * Other:                  [---]
         */
        assertFalse(window.overlap(new SessionWindow(end + 1, end + 1)));
        assertFalse(window.overlap(new SessionWindow(end + 1, 150)));
        assertFalse(window.overlap(new SessionWindow(125, 150)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void cannotCompareSessionWindowWithDifferentWindowType() {
        window.overlap(timeWindow);
    }
}