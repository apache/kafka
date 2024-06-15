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

import org.apache.kafka.streams.kstream.internals.UnlimitedWindow;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static java.time.Instant.ofEpochMilli;
import static org.apache.kafka.streams.EqualityCheck.verifyEquality;
import static org.apache.kafka.streams.EqualityCheck.verifyInEquality;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class UnlimitedWindowsTest {

    private static final long ANY_START_TIME = 10L;

    @Test
    public void shouldSetWindowStartTime() {
        assertEquals(ANY_START_TIME, UnlimitedWindows.of().startOn(ofEpochMilli(ANY_START_TIME)).startMs);
    }

    @Test
    public void startTimeMustNotBeNegative() {
        assertThrows(IllegalArgumentException.class, () -> UnlimitedWindows.of().startOn(ofEpochMilli(-1)));
    }

    @Test
    public void shouldIncludeRecordsThatHappenedOnWindowStart() {
        final UnlimitedWindows w = UnlimitedWindows.of().startOn(ofEpochMilli(ANY_START_TIME));
        final Map<Long, UnlimitedWindow> matchedWindows = w.windowsFor(w.startMs);
        assertEquals(1, matchedWindows.size());
        assertEquals(new UnlimitedWindow(ANY_START_TIME), matchedWindows.get(ANY_START_TIME));
    }

    @Test
    public void shouldIncludeRecordsThatHappenedAfterWindowStart() {
        final UnlimitedWindows w = UnlimitedWindows.of().startOn(ofEpochMilli(ANY_START_TIME));
        final long timestamp = w.startMs + 1;
        final Map<Long, UnlimitedWindow> matchedWindows = w.windowsFor(timestamp);
        assertEquals(1, matchedWindows.size());
        assertEquals(new UnlimitedWindow(ANY_START_TIME), matchedWindows.get(ANY_START_TIME));
    }

    @Test
    public void shouldExcludeRecordsThatHappenedBeforeWindowStart() {
        final UnlimitedWindows w = UnlimitedWindows.of().startOn(ofEpochMilli(ANY_START_TIME));
        final long timestamp = w.startMs - 1;
        final Map<Long, UnlimitedWindow> matchedWindows = w.windowsFor(timestamp);
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
