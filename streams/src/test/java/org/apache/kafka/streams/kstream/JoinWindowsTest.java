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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;


public class JoinWindowsTest {

    private static long anySize = 123L;
    private static long anyOtherSize = 456L; // should be larger than anySize

    @Test
    public void shouldHaveSaneEqualsAndHashCode() {
        JoinWindows w1 = JoinWindows.of(anySize);
        JoinWindows w2 = JoinWindows.of(anySize);

        // Reflexive
        assertEquals(w1, w1);
        assertEquals(w1.hashCode(), w1.hashCode());

        // Symmetric
        assertEquals(w1, w2);
        assertEquals(w2, w1);
        assertEquals(w1.hashCode(), w2.hashCode());

        JoinWindows w3 = JoinWindows.of(w2.afterMs).before(anyOtherSize);
        JoinWindows w4 = JoinWindows.of(anyOtherSize).after(w2.afterMs);
        assertEquals(w3, w4);
        assertEquals(w4, w3);
        assertEquals(w3.hashCode(), w4.hashCode());

        // Inequality scenarios
        assertNotEquals("must be false for null", null, w1);
        assertNotEquals("must be false for different window types", UnlimitedWindows.of(), w1);
        assertNotEquals("must be false for different types", new Object(), w1);

        JoinWindows differentWindowSize = JoinWindows.of(w1.afterMs + 1);
        assertNotEquals("must be false when window sizes are different", differentWindowSize, w1);

        JoinWindows differentWindowSize2 = JoinWindows.of(w1.afterMs).after(w1.afterMs + 1);
        assertNotEquals("must be false when window sizes are different", differentWindowSize2, w1);

        JoinWindows differentWindowSize3 = JoinWindows.of(w1.afterMs).before(w1.beforeMs + 1);
        assertNotEquals("must be false when window sizes are different", differentWindowSize3, w1);
    }

    @Test
    public void validWindows() {
        JoinWindows.of(anyOtherSize)   // [ -anyOtherSize ; anyOtherSize ]
            .before(anySize)                    // [ -anySize ; anyOtherSize ]
            .before(0)                          // [ 0 ; anyOtherSize ]
            .before(-anySize)                   // [ anySize ; anyOtherSize ]
            .before(-anyOtherSize);             // [ anyOtherSize ; anyOtherSize ]

        JoinWindows.of(anyOtherSize)   // [ -anyOtherSize ; anyOtherSize ]
            .after(anySize)                     // [ -anyOtherSize ; anySize ]
            .after(0)                           // [ -anyOtherSize ; 0 ]
            .after(-anySize)                    // [ -anyOtherSize ; -anySize ]
            .after(-anyOtherSize);              // [ -anyOtherSize ; -anyOtherSize ]
    }

    @Test(expected = IllegalArgumentException.class)
    public void timeDifferenceMustNotBeNegative() {
        JoinWindows.of(-1);
    }

    @Test
    public void endTimeShouldNotBeBeforeStart() {
        final JoinWindows windowSpec = JoinWindows.of(anySize);
        try {
            windowSpec.after(-anySize - 1);
            fail("window end time should not be before window start time");
        } catch (final IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void startTimeShouldNotBeAfterEnd() {
        final JoinWindows windowSpec = JoinWindows.of(anySize);
        try {
            windowSpec.before(-anySize - 1);
            fail("window start time should not be after window end time");
        } catch (final IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void untilShouldSetMaintainDuration() {
        final JoinWindows windowSpec = JoinWindows.of(anySize);
        final long windowSize = windowSpec.size();
        assertEquals(windowSize, windowSpec.until(windowSize).maintainMs());
    }

    @Test
    public void shouldUseWindowSizeForMaintainDurationWhenSizeLargerThanDefaultMaintainMs() {
        final long size = Windows.DEFAULT_MAINTAIN_DURATION_MS;

        final JoinWindows windowSpec = JoinWindows.of(size);
        final long windowSize = windowSpec.size();

        assertEquals(windowSize, windowSpec.maintainMs());
    }

    @Test
    public void retentionTimeMustNoBeSmallerThanWindowSize() {
        final JoinWindows windowSpec = JoinWindows.of(anySize);
        final long windowSize = windowSpec.size();
        try {
            windowSpec.until(windowSize - 1);
            fail("should not accept retention time smaller than window size");
        } catch (final IllegalArgumentException e) {
            // expected
        }
    }

}