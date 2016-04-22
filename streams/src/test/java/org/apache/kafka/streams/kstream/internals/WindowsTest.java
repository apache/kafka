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

package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.UnlimitedWindows;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class WindowsTest {

    @Test
    public void hoppingWindows() {
        TimeWindows windows = TimeWindows.of("test", 12L).shiftedBy(5L);
        Map<Long, TimeWindow> matched = windows.windowsFor(21L);
        assertEquals(3, matched.size());
        assertEquals(new TimeWindow(10L, 22L), matched.get(10L));
        assertEquals(new TimeWindow(15L, 27L), matched.get(15L));
        assertEquals(new TimeWindow(20L, 32L), matched.get(20L));
    }

    @Test
    public void tumblingWindows() {
        TimeWindows windows = TimeWindows.of("test", 12L);
        Map<Long, TimeWindow> matched = windows.windowsFor(21L);
        assertEquals(1, matched.size());
        assertEquals(new TimeWindow(12L, 24L), matched.get(12L));
    }

    @Test
    public void unlimitedWindows() {
        UnlimitedWindows windows = UnlimitedWindows.of("test").startOn(10L);
        Map<Long, UnlimitedWindow> matched = windows.windowsFor(21L);
        assertEquals(1, matched.size());
        assertEquals(new UnlimitedWindow(10L), matched.get(10L));
    }
}
