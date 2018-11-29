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

import org.apache.kafka.common.utils.MockTime;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HeartbeatTest {

    private int sessionTimeoutMs = 300;
    private int heartbeatIntervalMs = 100;
    private int maxPollIntervalMs = 900;
    private long retryBackoffMs = 10L;
    private MockTime time = new MockTime();
    private Heartbeat heartbeat = new Heartbeat(time, sessionTimeoutMs, heartbeatIntervalMs,
            maxPollIntervalMs, retryBackoffMs);

    @Test
    public void testShouldHeartbeat() {
        heartbeat.sentHeartbeat(time.absoluteMilliseconds());
        time.sleep((long) ((float) heartbeatIntervalMs * 1.1));
        assertTrue(heartbeat.shouldHeartbeat(time.absoluteMilliseconds()));
    }

    @Test
    public void testShouldNotHeartbeat() {
        heartbeat.sentHeartbeat(time.absoluteMilliseconds());
        time.sleep(heartbeatIntervalMs / 2);
        assertFalse(heartbeat.shouldHeartbeat(time.absoluteMilliseconds()));
    }

    @Test
    public void testTimeToNextHeartbeat() {
        heartbeat.sentHeartbeat(time.absoluteMilliseconds());
        assertEquals(heartbeatIntervalMs, heartbeat.timeToNextHeartbeat(time.absoluteMilliseconds()));

        time.sleep(heartbeatIntervalMs);
        assertEquals(0, heartbeat.timeToNextHeartbeat(time.absoluteMilliseconds()));

        time.sleep(heartbeatIntervalMs);
        assertEquals(0, heartbeat.timeToNextHeartbeat(time.absoluteMilliseconds()));
    }

    @Test
    public void testSessionTimeoutExpired() {
        heartbeat.sentHeartbeat(time.absoluteMilliseconds());
        time.sleep(sessionTimeoutMs + 5);
        assertTrue(heartbeat.sessionTimeoutExpired(time.absoluteMilliseconds()));
    }

    @Test
    public void testResetSession() {
        heartbeat.sentHeartbeat(time.absoluteMilliseconds());
        time.sleep(sessionTimeoutMs + 5);
        heartbeat.resetSessionTimeout();
        assertFalse(heartbeat.sessionTimeoutExpired(time.absoluteMilliseconds()));

        // Resetting the session timeout should not reset the poll timeout
        time.sleep(maxPollIntervalMs + 1);
        heartbeat.resetSessionTimeout();
        assertTrue(heartbeat.pollTimeoutExpired(time.absoluteMilliseconds()));
    }

    @Test
    public void testResetTimeouts() {
        time.sleep(maxPollIntervalMs);
        assertTrue(heartbeat.sessionTimeoutExpired(time.absoluteMilliseconds()));
        assertEquals(0, heartbeat.timeToNextHeartbeat(time.absoluteMilliseconds()));
        assertTrue(heartbeat.pollTimeoutExpired(time.absoluteMilliseconds()));

        heartbeat.resetTimeouts();
        assertFalse(heartbeat.sessionTimeoutExpired(time.absoluteMilliseconds()));
        assertEquals(heartbeatIntervalMs, heartbeat.timeToNextHeartbeat(time.absoluteMilliseconds()));
        assertFalse(heartbeat.pollTimeoutExpired(time.absoluteMilliseconds()));
    }

    @Test
    public void testPollTimeout() {
        assertFalse(heartbeat.pollTimeoutExpired(time.absoluteMilliseconds()));
        time.sleep(maxPollIntervalMs / 2);

        assertFalse(heartbeat.pollTimeoutExpired(time.absoluteMilliseconds()));
        time.sleep(maxPollIntervalMs / 2 + 1);

        assertTrue(heartbeat.pollTimeoutExpired(time.absoluteMilliseconds()));
    }

}
