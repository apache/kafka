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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class SlidingWindowTest {

    private long start = 50;
    private long end = 100;
    private final SlidingWindow window = new SlidingWindow(start, end);
    private final SessionWindow sessionWindow = new SessionWindow(start, end);

    @Test
    public void shouldNotOverlapIfOtherWindowIsBeforeThisWindow() {
        /*
         * This:        [-------]
         * Other: [---]
         */
        assertFalse(window.overlap(new SlidingWindow(0, 25)));
        assertFalse(window.overlap(new SlidingWindow(0, start - 1)));
        assertFalse(window.overlap(new SlidingWindow(start - 1, start - 1)));
    }

    @Test
    public void shouldOverlapIfOtherWindowEndIsWithinThisWindow() {
        /*
         * This:        [-------]
         * Other: [---------]
         */
        assertTrue(window.overlap(new SlidingWindow(0, start)));
        assertTrue(window.overlap(new SlidingWindow(0, start + 1)));
        assertTrue(window.overlap(new SlidingWindow(0, 75)));
        assertTrue(window.overlap(new SlidingWindow(0, end - 1)));
        assertTrue(window.overlap(new SlidingWindow(0, end)));

        assertTrue(window.overlap(new SlidingWindow(start - 1, start)));
        assertTrue(window.overlap(new SlidingWindow(start - 1, start + 1)));
        assertTrue(window.overlap(new SlidingWindow(start - 1, 75)));
        assertTrue(window.overlap(new SlidingWindow(start - 1, end - 1)));
        assertTrue(window.overlap(new SlidingWindow(start - 1, end)));
    }

    @Test
    public void shouldOverlapIfOtherWindowContainsThisWindow() {
        /*
         * This:        [-------]
         * Other: [------------------]
         */
        assertTrue(window.overlap(new SlidingWindow(0, end)));
        assertTrue(window.overlap(new SlidingWindow(0, end + 1)));
        assertTrue(window.overlap(new SlidingWindow(0, 150)));

        assertTrue(window.overlap(new SlidingWindow(start - 1, end)));
        assertTrue(window.overlap(new SlidingWindow(start - 1, end + 1)));
        assertTrue(window.overlap(new SlidingWindow(start - 1, 150)));

        assertTrue(window.overlap(new SlidingWindow(start, end)));
        assertTrue(window.overlap(new SlidingWindow(start, end + 1)));
        assertTrue(window.overlap(new SlidingWindow(start, 150)));
    }

    @Test
    public void shouldOverlapIfOtherWindowIsWithinThisWindow() {
        /*
         * This:        [-------]
         * Other:         [---]
         */
        assertTrue(window.overlap(new SlidingWindow(start, start)));
        assertTrue(window.overlap(new SlidingWindow(start, 75)));
        assertTrue(window.overlap(new SlidingWindow(start, end)));
        assertTrue(window.overlap(new SlidingWindow(75, end)));
        assertTrue(window.overlap(new SlidingWindow(end, end)));
    }

    @Test
    public void shouldOverlapIfOtherWindowStartIsWithinThisWindow() {
        /*
         * This:        [-------]
         * Other:           [-------]
         */
        assertTrue(window.overlap(new SlidingWindow(start, end + 1)));
        assertTrue(window.overlap(new SlidingWindow(start, 150)));
        assertTrue(window.overlap(new SlidingWindow(75, end + 1)));
        assertTrue(window.overlap(new SlidingWindow(75, 150)));
        assertTrue(window.overlap(new SlidingWindow(end, end + 1)));
        assertTrue(window.overlap(new SlidingWindow(end, 150)));
    }

    @Test
    public void shouldNotOverlapIsOtherWindowIsAfterThisWindow() {
        /*
         * This:        [-------]
         * Other:                  [---]
         */
        assertFalse(window.overlap(new SlidingWindow(end + 1, end + 1)));
        assertFalse(window.overlap(new SlidingWindow(end + 1, 150)));
        assertFalse(window.overlap(new SlidingWindow(125, 150)));
    }

    @Test
    public void cannotCompareSlidingWindowWithDifferentWindowType() {
        assertThrows(IllegalArgumentException.class, () -> window.overlap(sessionWindow));
    }
}