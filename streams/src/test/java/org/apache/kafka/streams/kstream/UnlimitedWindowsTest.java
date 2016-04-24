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

public class UnlimitedWindowsTest {

    private static String ANY_NAME = "window";

    @Test
    public void unlimitedWindows() {
        long startTime = 10L;
        UnlimitedWindows w = UnlimitedWindows.of(ANY_NAME).startOn(startTime);

        Map<Long, UnlimitedWindow> matchedWindows1 = w.windowsFor(startTime + 11L);
        assertEquals(1, matchedWindows1.size());
        assertEquals(new UnlimitedWindow(startTime), matchedWindows1.get(startTime));
    }

}