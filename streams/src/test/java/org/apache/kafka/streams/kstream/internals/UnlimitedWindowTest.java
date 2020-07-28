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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.kstream.Window;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

@SuppressWarnings("deprecation")
public class UnlimitedWindowTest {

    private long start = 50;
    private final Window window = Window.withBounds(start, Long.MAX_VALUE);
    private final Window sessionWindow = Window.withBounds(start, start);

    @Test
    public void shouldAlwaysOverlap() {
        assertTrue(window.overlap(new UnlimitedWindow(start - 1)));
        assertTrue(window.overlap(new UnlimitedWindow(start)));
        assertTrue(window.overlap(new UnlimitedWindow(start + 1)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void cannotCompareUnlimitedWindowWithDifferentWindowType() {
        window.overlap(sessionWindow);
    }
}