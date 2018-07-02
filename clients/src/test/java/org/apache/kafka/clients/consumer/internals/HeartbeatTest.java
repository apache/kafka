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
    private Heartbeat heartbeat = new Heartbeat(sessionTimeoutMs, heartbeatIntervalMs, maxPollIntervalMs, retryBackoffMs);

    @Test
    public void testShouldHeartbeat() {
        heartbeat.sentHeartbeat(time.milliseconds());
        time.sleep((long) ((float) heartbeatIntervalMs * 1.1));
        assertTrue(heartbeat.shouldHeartbeat(time.milliseconds()));
    }

    @Test
    public void testShouldNotHeartbeat() {
        heartbeat.sentHeartbeat(time.milliseconds());
        time.sleep(heartbeatIntervalMs / 2);
        assertFalse(heartbeat.shouldHeartbeat(time.milliseconds()));
    }

    @Test
    public void testTimeToNextHeartbeat() {
        heartbeat.sentHeartbeat(0);
        assertEquals(100, heartbeat.timeToNextHeartbeat(0));
        assertEquals(0, heartbeat.timeToNextHeartbeat(100));
        assertEquals(0, heartbeat.timeToNextHeartbeat(200));
    }

    @Test
    public void testSessionTimeoutExpired() {
        heartbeat.sentHeartbeat(time.milliseconds());
        time.sleep(305);
        assertTrue(heartbeat.sessionTimeoutExpired(time.milliseconds()));
    }

    @Test
    public void testResetSession() {
        heartbeat.sentHeartbeat(time.milliseconds());
        time.sleep(305);
        heartbeat.resetTimeouts(time.milliseconds());
        assertFalse(heartbeat.sessionTimeoutExpired(time.milliseconds()));
    }
}
