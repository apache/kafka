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

import org.junit.Test;

import java.time.Duration;

import static java.time.Duration.ofDays;
import static java.time.Duration.ofMillis;
import static org.apache.kafka.streams.EqualityCheck.verifyEquality;
import static org.apache.kafka.streams.EqualityCheck.verifyInEquality;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

@SuppressWarnings("deprecation")
public class JoinWindowsTest {

    private static final long ANY_SIZE = 123L;
    private static final long ANY_OTHER_SIZE = 456L; // should be larger than anySize

    @Test
    public void validWindows() {
        JoinWindows.of(ofMillis(ANY_OTHER_SIZE))   // [ -anyOtherSize ; anyOtherSize ]
                   .before(ofMillis(ANY_SIZE))                    // [ -anySize ; anyOtherSize ]
                   .before(ofMillis(0))                          // [ 0 ; anyOtherSize ]
                   .before(ofMillis(-ANY_SIZE))                   // [ anySize ; anyOtherSize ]
                   .before(ofMillis(-ANY_OTHER_SIZE));             // [ anyOtherSize ; anyOtherSize ]

        JoinWindows.of(ofMillis(ANY_OTHER_SIZE))   // [ -anyOtherSize ; anyOtherSize ]
                   .after(ofMillis(ANY_SIZE))                     // [ -anyOtherSize ; anySize ]
                   .after(ofMillis(0))                           // [ -anyOtherSize ; 0 ]
                   .after(ofMillis(-ANY_SIZE))                    // [ -anyOtherSize ; -anySize ]
                   .after(ofMillis(-ANY_OTHER_SIZE));              // [ -anyOtherSize ; -anyOtherSize ]
    }

    @Test
    public void timeDifferenceMustNotBeNegative() {
        assertThrows(IllegalArgumentException.class, () -> JoinWindows.of(ofMillis(-1)));
    }

    @Test
    public void endTimeShouldNotBeBeforeStart() {
        final JoinWindows windowSpec = JoinWindows.of(ofMillis(ANY_SIZE));
        try {
            windowSpec.after(ofMillis(-ANY_SIZE - 1));
            fail("window end time should not be before window start time");
        } catch (final IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void startTimeShouldNotBeAfterEnd() {
        final JoinWindows windowSpec = JoinWindows.of(ofMillis(ANY_SIZE));
        try {
            windowSpec.before(ofMillis(-ANY_SIZE - 1));
            fail("window start time should not be after window end time");
        } catch (final IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void untilShouldSetGraceDuration() {
        final JoinWindows windowSpec = JoinWindows.of(ofMillis(ANY_SIZE));
        final long windowSize = windowSpec.size();
        assertEquals(windowSize, windowSpec.grace(ofMillis(windowSize)).gracePeriodMs());
    }

    @SuppressWarnings("deprecation") // specifically testing deprecated APIs
    @Test
    public void shouldUseWindowSizeAsRetentionTimeIfWindowSizeIsLargerThanDefaultRetentionTime() {
        final long windowSize = 2 * JoinWindows.of(ofMillis(1)).maintainMs();
        assertEquals(2 * windowSize, JoinWindows.of(ofMillis(windowSize)).maintainMs());
    }

    @SuppressWarnings("deprecation")  // specifically testing deprecated APIs
    @Test
    public void shouldUseWindowSizeAndGraceAsRetentionTimeIfBothCombinedAreLargerThanDefaultRetentionTime() {
        final Duration windowsSize = ofDays(1).minus(ofMillis(1));
        final Duration gracePeriod = ofMillis(2);
        assertEquals(2 * windowsSize.toMillis(), JoinWindows.of(windowsSize).grace(gracePeriod).maintainMs());
    }

    @Deprecated
    @Test
    public void retentionTimeMustNoBeSmallerThanWindowSize() {
        final JoinWindows windowSpec = JoinWindows.of(ofMillis(ANY_SIZE));
        final long windowSize = windowSpec.size();
        try {
            windowSpec.until(windowSize - 1);
            fail("should not accept retention time smaller than window size");
        } catch (final IllegalArgumentException e) {
            // expected
        }
    }

    @SuppressWarnings("deprecation") // specifically testing deprecated APIs
    @Test
    public void shouldUseDefaultRetentionTimeWithDefaultGracePeriod() {
        final long windowSize1 = JoinWindows.of(ofMillis(1)).maintainMs();
        final long windowSize2 = JoinWindows.of(ofMillis(12 * 60 * 60 * 1000L)).maintainMs();
        assertEquals(windowSize1, 24 * 60 * 60 * 1000L);
        assertEquals(windowSize2, 24 * 60 * 60 * 1000L);
    }

    @Test
    public void gracePeriodShouldEnforceBoundaries() {
        JoinWindows.of(ofMillis(3L)).grace(ofMillis(0L));

        try {
            JoinWindows.of(ofMillis(3L)).grace(ofMillis(-1L));
            fail("should not accept negatives");
        } catch (final IllegalArgumentException e) {
            //expected
        }
    }

    @Test
    public void gracePeriodShouldBeDefaultRetentionTimeMinusWindowSize() {
        final long expectedDefaultRetentionTime = 24 * 60 * 60 * 1000L;
        assertEquals(expectedDefaultRetentionTime - 3L, TimeWindows.of(ofMillis(3L)).gracePeriodMs());
        assertEquals(0L, TimeWindows.of(ofMillis(expectedDefaultRetentionTime)).gracePeriodMs());
        assertEquals(0L, TimeWindows.of(ofMillis(expectedDefaultRetentionTime + 1L)).gracePeriodMs());
    }

    @Test
    public void equalsAndHashcodeShouldBeValidForPositiveCases() {
        verifyEquality(JoinWindows.of(ofMillis(3)), JoinWindows.of(ofMillis(3)));

        verifyEquality(JoinWindows.of(ofMillis(3)).after(ofMillis(2)), JoinWindows.of(ofMillis(3)).after(ofMillis(2)));

        verifyEquality(JoinWindows.of(ofMillis(3)).before(ofMillis(2)), JoinWindows.of(ofMillis(3)).before(ofMillis(2)));

        verifyEquality(JoinWindows.of(ofMillis(3)).grace(ofMillis(2)), JoinWindows.of(ofMillis(3)).grace(ofMillis(2)));

        verifyEquality(JoinWindows.of(ofMillis(3)).grace(ofMillis(60)), JoinWindows.of(ofMillis(3)).grace(ofMillis(60)));

        verifyEquality(
            JoinWindows.of(ofMillis(3)).before(ofMillis(1)).after(ofMillis(2)).grace(ofMillis(3)).grace(ofMillis(60)),
            JoinWindows.of(ofMillis(3)).before(ofMillis(1)).after(ofMillis(2)).grace(ofMillis(3)).grace(ofMillis(60))
        );
        // JoinWindows is a little weird in that before and after set the same fields as of.
        verifyEquality(
            JoinWindows.of(ofMillis(9)).before(ofMillis(1)).after(ofMillis(2)).grace(ofMillis(3)).grace(ofMillis(60)),
            JoinWindows.of(ofMillis(3)).before(ofMillis(1)).after(ofMillis(2)).grace(ofMillis(3)).grace(ofMillis(60))
        );
    }

    @Test
    public void equalsAndHashcodeShouldBeValidForNegativeCases() {
        verifyInEquality(JoinWindows.of(ofMillis(9)), JoinWindows.of(ofMillis(3)));

        verifyInEquality(JoinWindows.of(ofMillis(3)).after(ofMillis(9)), JoinWindows.of(ofMillis(3)).after(ofMillis(2)));

        verifyInEquality(JoinWindows.of(ofMillis(3)).before(ofMillis(9)), JoinWindows.of(ofMillis(3)).before(ofMillis(2)));

        verifyInEquality(JoinWindows.of(ofMillis(3)).grace(ofMillis(9)), JoinWindows.of(ofMillis(3)).grace(ofMillis(2)));

        verifyInEquality(JoinWindows.of(ofMillis(3)).grace(ofMillis(90)), JoinWindows.of(ofMillis(3)).grace(ofMillis(60)));


        verifyInEquality(
            JoinWindows.of(ofMillis(3)).before(ofMillis(9)).after(ofMillis(2)).grace(ofMillis(3)),
            JoinWindows.of(ofMillis(3)).before(ofMillis(1)).after(ofMillis(2)).grace(ofMillis(3))
        );

        verifyInEquality(
            JoinWindows.of(ofMillis(3)).before(ofMillis(1)).after(ofMillis(9)).grace(ofMillis(3)),
            JoinWindows.of(ofMillis(3)).before(ofMillis(1)).after(ofMillis(2)).grace(ofMillis(3))
        );

        verifyInEquality(
            JoinWindows.of(ofMillis(3)).before(ofMillis(1)).after(ofMillis(2)).grace(ofMillis(9)),
            JoinWindows.of(ofMillis(3)).before(ofMillis(1)).after(ofMillis(2)).grace(ofMillis(3))
        );
    }
}
