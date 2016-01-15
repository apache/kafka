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

import org.apache.kafka.streams.kstream.HoppingWindows;
import org.apache.kafka.streams.kstream.TumblingWindows;
import org.apache.kafka.streams.kstream.UnlimitedWindows;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class WindowsTest {

    @Test
    public void hoppingWindows() {

        HoppingWindows windows = HoppingWindows.of("test").with(12L).every(5L);

        Map<Long, HoppingWindow> matched = windows.windowsFor(21L);

        assertEquals(3, matched.size());

        assertEquals(new HoppingWindow(10L, 22L), matched.get(10L));
        assertEquals(new HoppingWindow(15L, 27L), matched.get(15L));
        assertEquals(new HoppingWindow(20L, 32L), matched.get(20L));
    }

    @Test
    public void tumblineWindows() {

        TumblingWindows windows = TumblingWindows.of("test").with(12L);

        Map<Long, TumblingWindow> matched = windows.windowsFor(21L);

        assertEquals(1, matched.size());

        assertEquals(new TumblingWindow(12L, 24L), matched.get(12L));
    }

    @Test
    public void unlimitedWindows() {

        UnlimitedWindows windows = UnlimitedWindows.of("test").startOn(10L);

        Map<Long, UnlimitedWindow> matched = windows.windowsFor(21L);

        assertEquals(1, matched.size());

        assertEquals(new UnlimitedWindow(10L), matched.get(10L));
    }
}
