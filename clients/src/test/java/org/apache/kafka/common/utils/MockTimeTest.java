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
package org.apache.kafka.common.utils;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.junit.Assert.assertEquals;

public class MockTimeTest extends TimeTest {

    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    @Test
    public void testAdvanceClock() {
        MockTime time = new MockTime(0, 100, 200);
        assertEquals(100, time.milliseconds());
        assertEquals(200, time.nanoseconds());
        time.sleep(1);
        assertEquals(101, time.milliseconds());
        assertEquals(1000200, time.nanoseconds());
    }

    @Test
    public void testAutoTickMs() {
        MockTime time = new MockTime(1, 100, 200);
        assertEquals(101, time.milliseconds());
        assertEquals(2000200, time.nanoseconds());
        assertEquals(103, time.milliseconds());
        assertEquals(104, time.milliseconds());
    }

    @Override
    protected Time createTime() {
        return new MockTime();
    }
}
