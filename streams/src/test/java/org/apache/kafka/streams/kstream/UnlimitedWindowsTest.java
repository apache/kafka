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

import java.util.Map;

import static java.time.Instant.ofEpochMilli;
import static org.apache.kafka.streams.EqualityCheck.verifyEquality;
import static org.apache.kafka.streams.EqualityCheck.verifyInEquality;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class UnlimitedWindowsTest {

    private static long anyStartTime = 10L;

    @Test
    public void shouldSetWindowStartTime() {
        assertEquals(anyStartTime, UnlimitedWindows.of().startOn(ofEpochMilli(anyStartTime)).startMs);
    }

    @Test(expected = IllegalArgumentException.class)
    public void startTimeMustNotBeNegative() {
        UnlimitedWindows.of().startOn(ofEpochMilli(-1));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldThrowOnUntil() {
        final UnlimitedWindows windowSpec = UnlimitedWindows.of();
        try {
            windowSpec.until(42);
            fail("should not allow to set window retention time");
        } catch (final IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void shouldIncludeRecordsThatHappenedOnWindowStart() {
        final UnlimitedWindows w = UnlimitedWindows.of().startOn(ofEpochMilli(anyStartTime));
        final Map<Long, Window> matchedWindows = w.windowsFor(w.startMs);
        assertEquals(1, matchedWindows.size());
        assertEquals(Window.withBounds(anyStartTime, Long.MAX_VALUE), matchedWindows.get(anyStartTime));
    }

    @Test
    public void shouldIncludeRecordsThatHappenedAfterWindowStart() {
        final UnlimitedWindows w = UnlimitedWindows.of().startOn(ofEpochMilli(anyStartTime));
        final long timestamp = w.startMs + 1;
        final Map<Long, Window> matchedWindows = w.windowsFor(timestamp);
        assertEquals(1, matchedWindows.size());
        assertEquals(Window.withBounds(anyStartTime, Long.MAX_VALUE), matchedWindows.get(anyStartTime));
    }

    @Test
    public void shouldExcludeRecordsThatHappenedBeforeWindowStart() {
        final UnlimitedWindows w = UnlimitedWindows.of().startOn(ofEpochMilli(anyStartTime));
        final long timestamp = w.startMs - 1;
        final Map<Long, Window> matchedWindows = w.windowsFor(timestamp);
        assertTrue(matchedWindows.isEmpty());
    }

    @Test
    public void equalsAndHashcodeShouldBeValidForPositiveCases() {
        verifyEquality(UnlimitedWindows.of(), UnlimitedWindows.of());

        verifyEquality(UnlimitedWindows.of().startOn(ofEpochMilli(1)), UnlimitedWindows.of().startOn(ofEpochMilli(1)));

    }

    @Test
    public void equalsAndHashcodeShouldBeValidForNegativeCases() {
        verifyInEquality(UnlimitedWindows.of().startOn(ofEpochMilli(9)), UnlimitedWindows.of().startOn(ofEpochMilli(1)));
    }

}
