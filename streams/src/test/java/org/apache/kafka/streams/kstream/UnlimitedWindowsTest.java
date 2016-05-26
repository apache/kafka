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

import org.apache.kafka.streams.kstream.internals.UnlimitedWindow;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class UnlimitedWindowsTest {

    private static String anyName = "window";
    private static long anyStartTime = 10L;

    @Test(expected = IllegalArgumentException.class)
    public void nameMustNotBeEmpty() {
        UnlimitedWindows.of("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void nameMustNotBeNull() {
        UnlimitedWindows.of(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void startTimeMustNotBeNegative() {
        UnlimitedWindows.of(anyName).startOn(-1);
    }

    @Test
    public void startTimeCanBeZero() {
        UnlimitedWindows.of(anyName).startOn(0);
    }

    @Test
    public void shouldIncludeRecordsThatHappenedOnWindowStart() {
        UnlimitedWindows w = UnlimitedWindows.of(anyName).startOn(anyStartTime);
        Map<Long, UnlimitedWindow> matchedWindows = w.windowsFor(w.start);
        assertEquals(1, matchedWindows.size());
        assertEquals(new UnlimitedWindow(anyStartTime), matchedWindows.get(anyStartTime));
    }

    @Test
    public void shouldIncludeRecordsThatHappenedAfterWindowStart() {
        UnlimitedWindows w = UnlimitedWindows.of(anyName).startOn(anyStartTime);
        long timestamp = w.start + 1;
        Map<Long, UnlimitedWindow> matchedWindows = w.windowsFor(timestamp);
        assertEquals(1, matchedWindows.size());
        assertEquals(new UnlimitedWindow(anyStartTime), matchedWindows.get(anyStartTime));
    }

    @Test
    public void shouldExcludeRecordsThatHappenedBeforeWindowStart() {
        UnlimitedWindows w = UnlimitedWindows.of(anyName).startOn(anyStartTime);
        long timestamp = w.start - 1;
        Map<Long, UnlimitedWindow> matchedWindows = w.windowsFor(timestamp);
        assertTrue(matchedWindows.isEmpty());
    }

}