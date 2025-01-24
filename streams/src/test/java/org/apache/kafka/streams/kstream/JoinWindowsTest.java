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

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.apache.kafka.streams.EqualityCheck.verifyEquality;
import static org.apache.kafka.streams.EqualityCheck.verifyInEquality;
import static org.apache.kafka.streams.kstream.Windows.DEPRECATED_DEFAULT_24_HR_GRACE_PERIOD;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

public class JoinWindowsTest {

    private static final long ANY_SIZE = 123L;
    private static final long ANY_OTHER_SIZE = 456L; // should be larger than anySize
    private static final long ANY_GRACE = 1024L;

    @Test
    public void validWindows() {
        JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(ANY_OTHER_SIZE))   // [ -anyOtherSize ; anyOtherSize ]
                   .before(ofMillis(ANY_SIZE))                              // [ -anySize ; anyOtherSize ]
                   .before(ofMillis(0))                                     // [ 0 ; anyOtherSize ]
                   .before(ofMillis(-ANY_SIZE))                             // [ anySize ; anyOtherSize ]
                   .before(ofMillis(-ANY_OTHER_SIZE));                      // [ anyOtherSize ; anyOtherSize ]

        JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(ANY_OTHER_SIZE))   // [ -anyOtherSize ; anyOtherSize ]
                   .after(ofMillis(ANY_SIZE))                               // [ -anyOtherSize ; anySize ]
                   .after(ofMillis(0))                                      // [ -anyOtherSize ; 0 ]
                   .after(ofMillis(-ANY_SIZE))                              // [ -anyOtherSize ; -anySize ]
                   .after(ofMillis(-ANY_OTHER_SIZE));                       // [ -anyOtherSize ; -anyOtherSize ]
    }

    @Test
    public void beforeShouldNotModifyGrace() {
        final JoinWindows joinWindows = JoinWindows.ofTimeDifferenceAndGrace(ofMillis(ANY_SIZE), ofMillis(ANY_OTHER_SIZE))
            .before(ofSeconds(ANY_SIZE));

        assertThat(joinWindows.gracePeriodMs(), equalTo(ANY_OTHER_SIZE));
    }

    @Test
    public void afterShouldNotModifyGrace() {
        final JoinWindows joinWindows = JoinWindows.ofTimeDifferenceAndGrace(ofMillis(ANY_SIZE), ofMillis(ANY_OTHER_SIZE))
            .after(ofSeconds(ANY_SIZE));

        assertThat(joinWindows.gracePeriodMs(), equalTo(ANY_OTHER_SIZE));
    }

    @Test
    public void timeDifferenceMustNotBeNegative() {
        assertThrows(IllegalArgumentException.class, () -> JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(-1)));
        assertThrows(IllegalArgumentException.class, () -> JoinWindows.ofTimeDifferenceAndGrace(ofMillis(-1), ofMillis(ANY_GRACE)));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void graceShouldNotCalledAfterGraceSet() {
        assertThrows(IllegalStateException.class, () -> JoinWindows.ofTimeDifferenceAndGrace(ofMillis(10), ofMillis(10)).grace(ofMillis(10)));
        assertThrows(IllegalStateException.class, () -> JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(10)).grace(ofMillis(10)));
    }

    @Test
    public void endTimeShouldNotBeBeforeStart() {
        final JoinWindows windowSpec = JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(ANY_SIZE));
        try {
            windowSpec.after(ofMillis(-ANY_SIZE - 1));
            fail("window end time should not be before window start time");
        } catch (final IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void startTimeShouldNotBeAfterEnd() {
        final JoinWindows windowSpec = JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(ANY_SIZE));
        try {
            windowSpec.before(ofMillis(-ANY_SIZE - 1));
            fail("window start time should not be after window end time");
        } catch (final IllegalArgumentException e) {
            // expected
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    public void untilShouldSetGraceDuration() {
        final JoinWindows windowSpec = JoinWindows.of(ofMillis(ANY_SIZE));
        final long windowSize = windowSpec.size();
        assertEquals(windowSize, windowSpec.grace(ofMillis(windowSize)).gracePeriodMs());
    }

    @Test
    public void gracePeriodShouldEnforceBoundaries() {
        JoinWindows.ofTimeDifferenceAndGrace(ofMillis(3L), ofMillis(0L));

        try {
            JoinWindows.ofTimeDifferenceAndGrace(ofMillis(3L), ofMillis(-1L));
            fail("should not accept negatives");
        } catch (final IllegalArgumentException e) {
            //expected
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    public void oldAPIShouldSetDefaultGracePeriod() {
        assertEquals(Duration.ofDays(1).toMillis(), DEPRECATED_DEFAULT_24_HR_GRACE_PERIOD);
        assertEquals(DEPRECATED_DEFAULT_24_HR_GRACE_PERIOD - 6L, JoinWindows.of(ofMillis(3L)).gracePeriodMs());
        assertEquals(0L, JoinWindows.of(ofMillis(DEPRECATED_DEFAULT_24_HR_GRACE_PERIOD)).gracePeriodMs());
        assertEquals(0L, JoinWindows.of(ofMillis(DEPRECATED_DEFAULT_24_HR_GRACE_PERIOD + 1L)).gracePeriodMs());
    }

    @Test
    public void noGraceAPIShouldNotSetGracePeriod() {
        assertEquals(0L, JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(3L)).gracePeriodMs());
        assertEquals(0L, JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(ANY_SIZE)).gracePeriodMs());
        assertEquals(0L, JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(ANY_OTHER_SIZE)).gracePeriodMs());
    }

    @Test
    public void withGraceAPIShouldSetGracePeriod() {
        assertEquals(ANY_GRACE, JoinWindows.ofTimeDifferenceAndGrace(ofMillis(3L), ofMillis(ANY_GRACE)).gracePeriodMs());
        assertEquals(ANY_GRACE, JoinWindows.ofTimeDifferenceAndGrace(ofMillis(ANY_SIZE), ofMillis(ANY_GRACE)).gracePeriodMs());
        assertEquals(ANY_GRACE, JoinWindows.ofTimeDifferenceAndGrace(ofMillis(ANY_OTHER_SIZE), ofMillis(ANY_GRACE)).gracePeriodMs());
    }

    @Test
    public void equalsAndHashcodeShouldBeValidForPositiveCases() {
        verifyEquality(
            JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(3)),
            JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(3))
        );

        verifyEquality(
            JoinWindows.ofTimeDifferenceAndGrace(ofMillis(3), ofMillis(2)),
            JoinWindows.ofTimeDifferenceAndGrace(ofMillis(3), ofMillis(2))
        );

        verifyEquality(
            JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(3)).after(ofMillis(2)),
            JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(3)).after(ofMillis(2))
        );

        verifyEquality(
            JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(3)).before(ofMillis(2)),
            JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(3)).before(ofMillis(2))
        );

        verifyEquality(
            JoinWindows.ofTimeDifferenceAndGrace(ofMillis(3), ofMillis(2)).after(ofMillis(4)),
            JoinWindows.ofTimeDifferenceAndGrace(ofMillis(3), ofMillis(2)).after(ofMillis(4))
        );

        verifyEquality(
            JoinWindows.ofTimeDifferenceAndGrace(ofMillis(3), ofMillis(2)).before(ofMillis(4)),
            JoinWindows.ofTimeDifferenceAndGrace(ofMillis(3), ofMillis(2)).before(ofMillis(4))
        );
    }

    @Test
    public void equalsAndHashcodeShouldBeValidForNegativeCases() {
        verifyInEquality(
            JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(9)),
            JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(3))
        );

        verifyInEquality(
            JoinWindows.ofTimeDifferenceAndGrace(ofMillis(3), ofMillis(9)),
            JoinWindows.ofTimeDifferenceAndGrace(ofMillis(3), ofMillis(2))
        );

        verifyInEquality(
            JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(3)).after(ofMillis(9)),
            JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(3)).after(ofMillis(2))
        );

        verifyInEquality(
            JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(3)).before(ofMillis(9)),
            JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(3)).before(ofMillis(2))
        );

        verifyInEquality(
            JoinWindows.ofTimeDifferenceAndGrace(ofMillis(3), ofMillis(3)).before(ofMillis(9)).after(ofMillis(2)),
            JoinWindows.ofTimeDifferenceAndGrace(ofMillis(3), ofMillis(3)).before(ofMillis(1)).after(ofMillis(2))
        );

        verifyInEquality(
            JoinWindows.ofTimeDifferenceAndGrace(ofMillis(3), ofMillis(3)).before(ofMillis(1)).after(ofMillis(9)),
            JoinWindows.ofTimeDifferenceAndGrace(ofMillis(3), ofMillis(3)).before(ofMillis(1)).after(ofMillis(2))
        );

        verifyInEquality(
            JoinWindows.ofTimeDifferenceAndGrace(ofMillis(3), ofMillis(9)).before(ofMillis(1)).after(ofMillis(2)),
            JoinWindows.ofTimeDifferenceAndGrace(ofMillis(3), ofMillis(3)).before(ofMillis(1)).after(ofMillis(2))
        );
    }
}
