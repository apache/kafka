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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TimedRequestStateTest {

    private static final long DEFAULT_TIMEOUT_MS = 30000;
    private final Time time = new MockTime();

    @Test
    public void testIsExpired() {
        TimedRequestState state = new TimedRequestState(
            new LogContext(),
            this.getClass().getSimpleName(),
            100,
            1000,
            time.timer(DEFAULT_TIMEOUT_MS)
        );
        assertFalse(state.isExpired());
        time.sleep(DEFAULT_TIMEOUT_MS);
        assertTrue(state.isExpired());
    }

    @Test
    public void testRemainingMs() {
        TimedRequestState state = new TimedRequestState(
            new LogContext(),
            this.getClass().getSimpleName(),
            100,
            1000,
            time.timer(DEFAULT_TIMEOUT_MS)
        );
        assertEquals(DEFAULT_TIMEOUT_MS, state.remainingMs());
        time.sleep(DEFAULT_TIMEOUT_MS);
        assertEquals(0, state.remainingMs());
    }

    @Test
    public void testDeadlineTimer() {
        long deadlineMs = time.milliseconds() + DEFAULT_TIMEOUT_MS;
        Timer timer = TimedRequestState.deadlineTimer(time, deadlineMs);
        assertEquals(DEFAULT_TIMEOUT_MS, timer.remainingMs());
        timer.sleep(DEFAULT_TIMEOUT_MS);
        assertEquals(0, timer.remainingMs());
    }

    @Test
    public void testAllowOverdueDeadlineTimer() {
        long deadlineMs = time.milliseconds() - DEFAULT_TIMEOUT_MS;
        Timer timer = TimedRequestState.deadlineTimer(time, deadlineMs);
        assertEquals(0, timer.remainingMs());
    }

    @Test
    public void testToStringUpdatesTimer() {
        TimedRequestState state = new TimedRequestState(
            new LogContext(),
            this.getClass().getSimpleName(),
            100,
            1000,
            time.timer(DEFAULT_TIMEOUT_MS)
        );

        assertToString(state, DEFAULT_TIMEOUT_MS);
        time.sleep(DEFAULT_TIMEOUT_MS);
        assertToString(state, 0);
    }

    private void assertToString(TimedRequestState state, long timerMs) {
        assertTrue(state.toString().contains("remainingMs=" + timerMs + "}"));
    }
}
