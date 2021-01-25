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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class WindowsTest {

    private class TestWindows extends Windows {
        @Override
        public Map windowsFor(final long timestamp) {
            return null;
        }

        @Override
        public long size() {
            return 0;
        }

        public long gracePeriodMs() {
            return 0L;
        }
    }

    @SuppressWarnings("deprecation") // specifically testing deprecated APIs
    @Test
    public void shouldSetWindowRetentionTime() {
        final int anyNotNegativeRetentionTime = 42;
        assertEquals(anyNotNegativeRetentionTime, new TestWindows().until(anyNotNegativeRetentionTime).maintainMs());
    }

    @SuppressWarnings("deprecation") // specifically testing deprecated APIs
    @Test
    public void numberOfSegmentsMustBeAtLeastTwo() {
        assertThrows(IllegalArgumentException.class, () -> new TestWindows().segments(1));
    }

    @SuppressWarnings("deprecation") // specifically testing deprecated APIs
    @Test
    public void retentionTimeMustNotBeNegative() {
        assertThrows(IllegalArgumentException.class, () -> new TestWindows().until(-1));
    }

}
