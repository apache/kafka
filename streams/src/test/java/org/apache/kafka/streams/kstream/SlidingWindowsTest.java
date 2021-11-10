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

import static java.time.Duration.ofMillis;
import static org.apache.kafka.streams.EqualityCheck.verifyEquality;
import static org.apache.kafka.streams.EqualityCheck.verifyInEquality;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class SlidingWindowsTest {

    private static final long ANY_SIZE = 123L;
    private static final long ANY_GRACE = 1024L;

    @SuppressWarnings("deprecation")
    @Test
    public void shouldSetTimeDifference() {
        assertEquals(ANY_SIZE, SlidingWindows.withTimeDifferenceAndGrace(ofMillis(ANY_SIZE), ofMillis(3)).timeDifferenceMs());
        assertEquals(ANY_SIZE, SlidingWindows.ofTimeDifferenceAndGrace(ofMillis(ANY_SIZE), ofMillis(ANY_GRACE)).timeDifferenceMs());
        assertEquals(ANY_SIZE, SlidingWindows.ofTimeDifferenceWithNoGrace(ofMillis(ANY_SIZE)).timeDifferenceMs());
    }

    @Test
    public void timeDifferenceMustNotBeNegative() {
        assertThrows(IllegalArgumentException.class, () ->  SlidingWindows.ofTimeDifferenceAndGrace(ofMillis(-1), ofMillis(5)));
    }

    @Test
    public void shouldSetGracePeriod() {
        assertEquals(ANY_SIZE, SlidingWindows.ofTimeDifferenceAndGrace(ofMillis(10), ofMillis(ANY_SIZE)).gracePeriodMs());
    }

    @Test
    public void gracePeriodMustNotBeNegative() {
        assertThrows(IllegalArgumentException.class, () ->  SlidingWindows.ofTimeDifferenceAndGrace(ofMillis(10), ofMillis(-1)));
    }

    @Test
    public void equalsAndHashcodeShouldBeValidForPositiveCases() {
        final long grace = 1L + (long) (Math.random() * (20L - 1L));
        final long timeDifference = 1L + (long) (Math.random() * (20L - 1L));
        verifyEquality(
                SlidingWindows.ofTimeDifferenceAndGrace(ofMillis(timeDifference), ofMillis(grace)),
                SlidingWindows.ofTimeDifferenceAndGrace(ofMillis(timeDifference), ofMillis(grace))
        );

        verifyEquality(
                SlidingWindows.ofTimeDifferenceAndGrace(ofMillis(timeDifference), ofMillis(grace)),
                SlidingWindows.ofTimeDifferenceAndGrace(ofMillis(timeDifference), ofMillis(grace))
        );

        verifyEquality(
                SlidingWindows.ofTimeDifferenceWithNoGrace(ofMillis(timeDifference)),
                SlidingWindows.ofTimeDifferenceWithNoGrace(ofMillis(timeDifference))
        );
    }

    @Test
    public void equalsAndHashcodeShouldNotBeEqualForDifferentTimeDifference() {
        final long grace = 1L + (long) (Math.random() * (10L - 1L));
        final long timeDifferenceOne = 1L + (long) (Math.random() * (10L - 1L));
        final long timeDifferenceTwo = 21L + (long) (Math.random() * (41L - 21L));
        verifyInEquality(
                SlidingWindows.ofTimeDifferenceAndGrace(ofMillis(timeDifferenceOne), ofMillis(grace)),
                SlidingWindows.ofTimeDifferenceAndGrace(ofMillis(timeDifferenceTwo), ofMillis(grace))
        );
    }

    @Test
    public void equalsAndHashcodeShouldNotBeEqualForDifferentGracePeriod() {
        final long timeDifference = 1L + (long) (Math.random() * (10L - 1L));
        final long graceOne = 1L + (long) (Math.random() * (10L - 1L));
        final long graceTwo = 21L + (long) (Math.random() * (41L - 21L));
        verifyInEquality(
                SlidingWindows.ofTimeDifferenceAndGrace(ofMillis(timeDifference), ofMillis(graceOne)),
                SlidingWindows.ofTimeDifferenceAndGrace(ofMillis(timeDifference), ofMillis(graceTwo))
        );
    }
}
