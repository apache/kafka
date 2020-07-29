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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

@SuppressWarnings("deprecation")
public class SlidingWindowsTest {

    private static final long ANY_SIZE = 123L;

    @Test
    public void shouldSetWindowSize() {
        assertEquals(ANY_SIZE, SlidingWindows.withTimeDifferenceAndGrace(ofMillis(ANY_SIZE), ofMillis(3)).timeDifference);
    }

    @Test(expected = IllegalArgumentException.class)
    public void windowSizeMustNotBeZero() {
        SlidingWindows.withTimeDifferenceAndGrace(ofMillis(0), ofMillis(5));
    }

    @Test(expected = IllegalArgumentException.class)
    public void windowSizeMustNotBeNegative() {
        SlidingWindows.withTimeDifferenceAndGrace(ofMillis(-1), ofMillis(5));
    }

    @Test
    public void shouldSetGracePeriod() {
        assertEquals(ANY_SIZE, SlidingWindows.withTimeDifferenceAndGrace(ofMillis(10), ofMillis(ANY_SIZE)).gracePeriodMs());
    }

    @Test(expected = IllegalArgumentException.class)
    public void graceMustNotBeNegative() {
        SlidingWindows.withTimeDifferenceAndGrace(ofMillis(10), ofMillis(-1));
    }

    @Test
    public void gracePeriodShouldEnforceBoundaries() {
        SlidingWindows.withTimeDifferenceAndGrace(ofMillis(3L), ofMillis(0L));

        try {
            SlidingWindows.withTimeDifferenceAndGrace(ofMillis(3L), ofMillis(-1L));
            fail("should not accept negatives");
        } catch (final IllegalArgumentException e) {
            //expected
        }
    }

    @Test
    public void equalsAndHashcodeShouldBeValidForPositiveCases() {
        verifyEquality(SlidingWindows.withTimeDifferenceAndGrace(ofMillis(3), ofMillis(3)), SlidingWindows.withTimeDifferenceAndGrace(ofMillis(3), ofMillis(3)));

        verifyEquality(SlidingWindows.withTimeDifferenceAndGrace(ofMillis(3), ofMillis(1)), SlidingWindows.withTimeDifferenceAndGrace(ofMillis(3), ofMillis(1)));

        verifyEquality(SlidingWindows.withTimeDifferenceAndGrace(ofMillis(3), ofMillis(4)), SlidingWindows.withTimeDifferenceAndGrace(ofMillis(3), ofMillis(4)));

    }

    @Test
    public void equalsAndHashcodeShouldBeValidForNegativeCases() {

        verifyInEquality(SlidingWindows.withTimeDifferenceAndGrace(ofMillis(3), ofMillis(2)), SlidingWindows.withTimeDifferenceAndGrace(ofMillis(3), ofMillis(1)));

        verifyInEquality(SlidingWindows.withTimeDifferenceAndGrace(ofMillis(3), ofMillis(9)), SlidingWindows.withTimeDifferenceAndGrace(ofMillis(3), ofMillis(4)));


        verifyInEquality(
                SlidingWindows.withTimeDifferenceAndGrace(ofMillis(4), ofMillis(2)),
                SlidingWindows.withTimeDifferenceAndGrace(ofMillis(3), ofMillis(2))
        );

        assertNotEquals(
                SlidingWindows.withTimeDifferenceAndGrace(ofMillis(3), ofMillis(1)),
                SlidingWindows.withTimeDifferenceAndGrace(ofMillis(3), ofMillis(2))
        );
    }
}
