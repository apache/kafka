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
package org.apache.kafka.streams;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MockTimeTest {

    @Test
    public void shouldSetStartTime() {
        final TopologyTestDriver.MockTime time = new TopologyTestDriver.MockTime(42L);
        assertEquals(42L, time.milliseconds());
        assertEquals(42L * 1000L * 1000L, time.nanoseconds());
    }

    @Test
    public void shouldGetNanosAsMillis() {
        final TopologyTestDriver.MockTime time = new TopologyTestDriver.MockTime(42L);
        assertEquals(42L, time.hiResClockMs());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAllowNegativeSleep() {
        new TopologyTestDriver.MockTime(42).sleep(-1L);
    }

    @Test
    public void shouldAdvanceTimeOnSleep() {
        final TopologyTestDriver.MockTime time = new TopologyTestDriver.MockTime(42L);

        assertEquals(42L, time.milliseconds());
        time.sleep(1L);
        assertEquals(43L, time.milliseconds());
        time.sleep(0L);
        assertEquals(43L, time.milliseconds());
        time.sleep(3L);
        assertEquals(46L, time.milliseconds());
    }

}
