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
package org.apache.kafka.streams.test;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MockTimeTest {

    @Test
    public void shouldInitWithCurrentSystemTimeByDefault() {
        final long beforeMs = System.currentTimeMillis();
        final long beforeNano = System.nanoTime();

        final MockTime time = new MockTime();

        final long afterMs = System.currentTimeMillis();
        final long afterNano = System.nanoTime();

        assertTrue(beforeMs <= time.milliseconds() && time.milliseconds() <= afterMs);
        assertTrue(beforeNano <= time.nanoseconds() && time.nanoseconds() <= afterNano);
    }

    @Test
    public void shouldSetCurrentMs() {
        final MockTime time = new MockTime();
        final long newMs = System.currentTimeMillis();

        time.setCurrentTimeMs(newMs);

        assertEquals(newMs, time.milliseconds());
        assertEquals(newMs * 1000000L, time.nanoseconds());
    }

    @Test
    public void shouldNotAllowToGoBackInTime() {
        final MockTime time = new MockTime();

        try {
            time.setCurrentTimeMs(time.milliseconds() - 1L);
            fail("Should have thrown");
        } catch (final IllegalArgumentException expected) {
            assertEquals("Setting the time to " + (time.milliseconds() - 1L) + " while current time "
                         + time.milliseconds() + " is newer; this is not allowed",
                expected.getMessage());
        }
    }

    @Test
    public void shouldNotAutoTickByDefault() {
        final MockTime time = new MockTime();
        assertEquals(time.milliseconds(), time.milliseconds());
        assertEquals(time.nanoseconds(), time.nanoseconds());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAllowNegativeAutoTickMs() {
        new MockTime(-1L);
    }

    @Test
    public void shouldAutoTickOnMilliseconds() {
        final MockTime time = new MockTime(2L);
        assertEquals(time.milliseconds() + 2L, time.milliseconds());
    }

    @Test
    public void shouldAutoTickOnNanoseconds() {
        final MockTime time = new MockTime(2L);
        assertEquals(time.nanoseconds() + 2000000L, time.nanoseconds());
    }

    @Test
    public void shouldSetAutoTick() {
        final MockTime time = new MockTime(2L);
        time.setAutoTickMs(1L);
        assertEquals(time.nanoseconds() + 1000000L, time.nanoseconds());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAllowNegativeAutoTick() {
        new MockTime().setAutoTickMs(-1L);
    }

    @Test
    public void shouldSetStartTime() {
        final MockTime time = new MockTime(0L, 42L, 2121L);
        assertEquals(42L, time.milliseconds());
        assertEquals(2121L, time.nanoseconds());
    }

    @Test
    public void shouldGetNanosAsMillis() {
        final MockTime time = new MockTime(0, 42L, 2121L * 1000000L);
        assertEquals(2121L, time.hiResClockMs());
    }

    @Test
    public void shouldAdvanceTimeOnSleep() {
        final MockTime time = new MockTime(0L, 42L, 0L);

        assertEquals(42L, time.milliseconds());
        time.sleep(1L);
        assertEquals(43L, time.milliseconds());
        time.sleep(0L);
        assertEquals(43L, time.milliseconds());
        time.sleep(3L);
        assertEquals(46L, time.milliseconds());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAllowNegativeSleep() {
        new MockTime().sleep(-1L);
    }

    @Test
    public void shouldCallListenerAfterAdvance() {
        final MockTime time = new MockTime(1L, 42L, 0L);
        final MockListener listener = new MockListener();
        time.addListener(listener);

        assertEquals(-1L, listener.lastTimestamp);

        time.sleep(0L);
        assertEquals(42L, listener.lastTimestamp);
        time.sleep(1L);
        assertEquals(43L, listener.lastTimestamp);
        time.sleep(5L);
        assertEquals(48L, listener.lastTimestamp);
        time.sleep(0L);
        assertEquals(48L, listener.lastTimestamp);

        time.milliseconds();
        assertEquals(49L, listener.lastTimestamp);

        time.nanoseconds();
        assertEquals(50L, listener.lastTimestamp);

        time.setCurrentTimeMs(100L);
        assertEquals(100L, listener.lastTimestamp);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullListener() {
        new MockTime().addListener(null);
    }

    private final class MockListener implements MockTime.MockTimeListener {
        long lastTimestamp = -1L;

        @Override
        public void tick(long currentTimeMs) {
            lastTimestamp = currentTimeMs;
        }
    }

}
