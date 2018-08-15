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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

public class SessionWindowsTest {

    @Test
    public void shouldSetWindowGap() {
        final long anyGap = 42L;
        assertEquals(anyGap, SessionWindows.with(anyGap).inactivityGap());
    }

    @Deprecated
    @Test
    public void shouldSetWindowRetentionTime() {
        final long anyRetentionTime = 42L;
        assertEquals(anyRetentionTime, SessionWindows.with(1).until(anyRetentionTime).maintainMs());
    }


    @Test
    public void gracePeriodShouldEnforceBoundaries() {
        SessionWindows.with(3L).grace(0L);

        try {
            SessionWindows.with(3L).grace(-1L);
            fail("should not accept negatives");
        } catch (final IllegalArgumentException e) {
            //expected
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void windowSizeMustNotBeNegative() {
        SessionWindows.with(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void windowSizeMustNotBeZero() {
        SessionWindows.with(0);
    }

    @SuppressWarnings("deprecation") // specifically testing deprecated apis
    @Test
    public void retentionTimeShouldBeGapIfGapIsLargerThanDefaultRetentionTime() {
        final long windowGap = 2 * SessionWindows.with(1).maintainMs();
        assertEquals(windowGap, SessionWindows.with(windowGap).maintainMs());
    }

    @Deprecated
    @Test
    public void retentionTimeMustNotBeNegative() {
        final SessionWindows windowSpec = SessionWindows.with(42);
        try {
            windowSpec.until(41);
            fail("should not accept retention time smaller than gap");
        } catch (final IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void equalsAndHashcodeShouldBeValidForPositiveCases() {
        assertEquals(SessionWindows.with(1), SessionWindows.with(1));
        assertEquals(SessionWindows.with(1).hashCode(), SessionWindows.with(1).hashCode());

        assertEquals(SessionWindows.with(1).grace(6), SessionWindows.with(1).grace(6));
        assertEquals(SessionWindows.with(1).grace(6).hashCode(), SessionWindows.with(1).grace(6).hashCode());

        assertEquals(SessionWindows.with(1).until(7), SessionWindows.with(1).until(7));
        assertEquals(SessionWindows.with(1).until(7).hashCode(), SessionWindows.with(1).until(7).hashCode());

        assertEquals(SessionWindows.with(1).grace(6).until(7), SessionWindows.with(1).grace(6).until(7));
        assertEquals(SessionWindows.with(1).grace(6).until(7).hashCode(), SessionWindows.with(1).grace(6).until(7).hashCode());
    }

    @Test
    public void equalsAndHashcodeShouldBeValidForNegativeCases() {
        assertNotEquals(SessionWindows.with(9), SessionWindows.with(1));
        assertNotEquals(SessionWindows.with(9).hashCode(), SessionWindows.with(1).hashCode());

        assertNotEquals(SessionWindows.with(1).grace(9), SessionWindows.with(1).grace(6));
        assertNotEquals(SessionWindows.with(1).grace(9).hashCode(), SessionWindows.with(1).grace(6).hashCode());

        assertNotEquals(SessionWindows.with(1).until(9), SessionWindows.with(1).until(7));
        assertNotEquals(SessionWindows.with(1).until(9).hashCode(), SessionWindows.with(1).until(7).hashCode());


        assertNotEquals(SessionWindows.with(2).grace(6).until(7), SessionWindows.with(1).grace(6).until(7));
        assertNotEquals(SessionWindows.with(2).grace(6).until(7).hashCode(), SessionWindows.with(1).grace(6).until(7).hashCode());

        assertNotEquals(SessionWindows.with(1).grace(0).until(7), SessionWindows.with(1).grace(6).until(7));
        assertNotEquals(SessionWindows.with(1).grace(0).until(7).hashCode(), SessionWindows.with(1).grace(6).until(7).hashCode());

        assertNotEquals(SessionWindows.with(1).grace(6).until(70), SessionWindows.with(1).grace(6).until(7));
        assertNotEquals(SessionWindows.with(1).grace(6).until(70).hashCode(), SessionWindows.with(1).grace(6).until(7).hashCode());
    }
}