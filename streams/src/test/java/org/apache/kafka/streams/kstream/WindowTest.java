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

public class WindowTest {
    private final Window window = Window.withBounds(5, 10);

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfStartIsNegative() {
        Window.withBounds(-1, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfEndIsSmallerThanStart() {
        Window.withBounds(1, 0);
    }

    @Test
    public void shouldBeEqualIfStartAndEndSame() {
        final Window window2 = Window.withBounds(window.startMs, window.endMs);

        assertEquals(window, window);
        assertEquals(window, window2);
        assertEquals(window2, window);
    }

    @Test
    public void shouldNotBeEqualIfNull() {
        assertNotEquals(window, null);
    }

    @Test
    public void shouldNotBeEqualIfStartOrEndIsDifferent() {
        assertNotEquals(window, Window.withBounds(0, window.endMs));
        assertNotEquals(window, Window.withBounds(7, window.endMs));
        assertNotEquals(window, Window.withBounds(window.startMs, 7));
        assertNotEquals(window, Window.withBounds(window.startMs, 15));
        assertNotEquals(window, Window.withBounds(7, 8));
        assertNotEquals(window, Window.withBounds(0, 15));
    }
}
