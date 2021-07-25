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

import static java.time.Duration.ofMillis;
import static org.apache.kafka.streams.EqualityCheck.verifyEquality;
import static org.apache.kafka.streams.EqualityCheck.verifyInEquality;
import static org.apache.kafka.streams.kstream.Windows.DEPRECATED_OLD_24_HR_GRACE_PERIOD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

@SuppressWarnings("deprecation")
public class SessionWindowsTest {

    @Test
    public void shouldSetWindowGap() {
        final long anyGap = 42L;
        final long anyGrace = 1024L;

        assertEquals(anyGap, SessionWindows.with(ofMillis(anyGap)).inactivityGap());
        assertEquals(anyGap, SessionWindows.ofInactivityGapWithNoGrace(ofMillis(anyGap)).inactivityGap());
        assertEquals(anyGap, SessionWindows.ofInactivityGapAndGrace(ofMillis(anyGap), ofMillis(anyGrace)).inactivityGap());
    }

    @Test
    public void shouldSetWindowGraceTime() {
        final long anyRetentionTime = 42L;
        assertEquals(anyRetentionTime, SessionWindows.with(ofMillis(1)).grace(ofMillis(anyRetentionTime)).gracePeriodMs());
    }

    @Test
    public void gracePeriodShouldEnforceBoundaries() {
        SessionWindows.with(ofMillis(3L)).grace(ofMillis(0));

        try {
            SessionWindows.with(ofMillis(3L)).grace(ofMillis(-1L));
            fail("should not accept negatives");
        } catch (final IllegalArgumentException e) {
            //expected
        }
    }

    @Test
    public void oldAPIShouldSetDefaultGracePeriod() {
        assertEquals(Duration.ofDays(1).toMillis(), DEPRECATED_OLD_24_HR_GRACE_PERIOD);
        assertEquals(DEPRECATED_OLD_24_HR_GRACE_PERIOD - 3L, SessionWindows.with(ofMillis(3L)).gracePeriodMs());
        assertEquals(0L, SessionWindows.with(ofMillis(DEPRECATED_OLD_24_HR_GRACE_PERIOD)).gracePeriodMs());
        assertEquals(0L, SessionWindows.with(ofMillis(DEPRECATED_OLD_24_HR_GRACE_PERIOD + 1L)).gracePeriodMs());
    }

    @Test
    public void windowSizeMustNotBeNegative() {
        assertThrows(IllegalArgumentException.class, () -> SessionWindows.with(ofMillis(-1)));
    }

    @Test
    public void windowSizeMustNotBeZero() {
        assertThrows(IllegalArgumentException.class, () -> SessionWindows.with(ofMillis(0)));
    }

    @Test
    public void equalsAndHashcodeShouldBeValidForPositiveCases() {
        verifyEquality(SessionWindows.with(ofMillis(1)), SessionWindows.with(ofMillis(1)));

        verifyEquality(SessionWindows.ofInactivityGapWithNoGrace(ofMillis(1)),
                SessionWindows.ofInactivityGapWithNoGrace(ofMillis(1))
        );

        verifyEquality(
                SessionWindows.ofInactivityGapAndGrace(ofMillis(1), ofMillis(11)),
                SessionWindows.ofInactivityGapAndGrace(ofMillis(1), ofMillis(11))
        );

        verifyEquality(SessionWindows.with(ofMillis(1)).grace(ofMillis(6)), SessionWindows.with(ofMillis(1)).grace(ofMillis(6)));

        verifyEquality(SessionWindows.with(ofMillis(1)).grace(ofMillis(7)), SessionWindows.with(ofMillis(1)).grace(ofMillis(7)));

        verifyEquality(SessionWindows.with(ofMillis(1)).grace(ofMillis(6)).grace(ofMillis(7)), SessionWindows.with(ofMillis(1)).grace(ofMillis(6)).grace(ofMillis(7)));
    }

    @Test
    public void equalsAndHashcodeShouldBeValidForNegativeCases() {

        verifyInEquality(
                SessionWindows.ofInactivityGapWithNoGrace(ofMillis(9)),
                SessionWindows.ofInactivityGapWithNoGrace(ofMillis(1)));

        verifyInEquality(
                SessionWindows.ofInactivityGapAndGrace(ofMillis(9), ofMillis(9)),
                SessionWindows.ofInactivityGapAndGrace(ofMillis(1), ofMillis(9)));

        verifyInEquality(SessionWindows.with(ofMillis(9)), SessionWindows.with(ofMillis(1)));

        verifyInEquality(SessionWindows.with(ofMillis(1)).grace(ofMillis(9)), SessionWindows.with(ofMillis(1)).grace(ofMillis(6)));

        verifyInEquality(SessionWindows.with(ofMillis(1)).grace(ofMillis(9)), SessionWindows.with(ofMillis(1)).grace(ofMillis(7)));

        verifyInEquality(SessionWindows.with(ofMillis(2)).grace(ofMillis(6)).grace(ofMillis(7)), SessionWindows.with(ofMillis(1)).grace(ofMillis(6)));

        verifyInEquality(SessionWindows.with(ofMillis(1)).grace(ofMillis(0)).grace(ofMillis(7)), SessionWindows.with(ofMillis(1)).grace(ofMillis(6)));
    }
}
