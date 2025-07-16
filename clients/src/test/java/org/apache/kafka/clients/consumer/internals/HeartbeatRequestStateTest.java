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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HeartbeatRequestStateTest {

    private static final LogContext LOG_CONTEXT = new LogContext("test ");
    private static final long HEARTBEAT_INTERVAL_MS = 1000;
    private static final long RETRY_BACKOFF_MS = 100;
    private static final long RETRY_BACKOFF_MAX_MS = 500;
    private static final double JITTER = 0.2;

    private final Time time = new MockTime();

    @Test
    public void testCanSendRequestAndTimeToNextHeartbeatMs() {
        final HeartbeatRequestState heartbeatRequestState = new HeartbeatRequestState(
            LOG_CONTEXT,
            time,
            HEARTBEAT_INTERVAL_MS,
            RETRY_BACKOFF_MS,
            RETRY_BACKOFF_MAX_MS,
            JITTER
        );

        assertFalse(heartbeatRequestState.canSendRequest(time.milliseconds()));
        assertEquals(HEARTBEAT_INTERVAL_MS, heartbeatRequestState.timeToNextHeartbeatMs(time.milliseconds()));
        time.sleep(HEARTBEAT_INTERVAL_MS - 1);
        assertFalse(heartbeatRequestState.canSendRequest(time.milliseconds()));
        assertEquals(1, heartbeatRequestState.timeToNextHeartbeatMs(time.milliseconds()));
        time.sleep(1);
        assertTrue(heartbeatRequestState.canSendRequest(time.milliseconds()));
        assertEquals(0, heartbeatRequestState.timeToNextHeartbeatMs(time.milliseconds()));
        time.sleep(100);
        assertTrue(heartbeatRequestState.canSendRequest(time.milliseconds()));
        assertEquals(0, heartbeatRequestState.timeToNextHeartbeatMs(time.milliseconds()));
    }

    @Test
    public void testResetTimer() {
        final HeartbeatRequestState heartbeatRequestState = new HeartbeatRequestState(
            LOG_CONTEXT,
            time,
            HEARTBEAT_INTERVAL_MS,
            RETRY_BACKOFF_MS,
            RETRY_BACKOFF_MAX_MS,
            JITTER
        );
        time.sleep(HEARTBEAT_INTERVAL_MS + 100);
        assertTrue(heartbeatRequestState.canSendRequest(time.milliseconds()));
        assertEquals(0, heartbeatRequestState.timeToNextHeartbeatMs(time.milliseconds()));

        heartbeatRequestState.resetTimer();

        assertFalse(heartbeatRequestState.canSendRequest(time.milliseconds()));
        assertEquals(HEARTBEAT_INTERVAL_MS, heartbeatRequestState.timeToNextHeartbeatMs(time.milliseconds()));
    }

    @Test
    public void testUpdateHeartbeatIntervalMs() {
        final HeartbeatRequestState heartbeatRequestState = new HeartbeatRequestState(
            LOG_CONTEXT,
            time,
            HEARTBEAT_INTERVAL_MS,
            RETRY_BACKOFF_MS,
            RETRY_BACKOFF_MAX_MS,
            JITTER
        );
        final long updatedHeartbeatIntervalMs = 2 * HEARTBEAT_INTERVAL_MS;
        time.sleep(HEARTBEAT_INTERVAL_MS + 100);

        heartbeatRequestState.updateHeartbeatIntervalMs(updatedHeartbeatIntervalMs);

        assertFalse(heartbeatRequestState.canSendRequest(time.milliseconds()));
        assertEquals(2 * HEARTBEAT_INTERVAL_MS, heartbeatRequestState.timeToNextHeartbeatMs(time.milliseconds()));
    }

    @Test
    public void testUpdateHeartbeatIntervalMsWithSameInterval() {
        final HeartbeatRequestState heartbeatRequestState = new HeartbeatRequestState(
            LOG_CONTEXT,
            time,
            HEARTBEAT_INTERVAL_MS,
            RETRY_BACKOFF_MS,
            RETRY_BACKOFF_MAX_MS,
            JITTER
        );
        time.sleep(HEARTBEAT_INTERVAL_MS + 100);

        heartbeatRequestState.updateHeartbeatIntervalMs(HEARTBEAT_INTERVAL_MS);

        assertEquals(HEARTBEAT_INTERVAL_MS, heartbeatRequestState.heartbeatIntervalMs());
        assertTrue(heartbeatRequestState.canSendRequest(time.milliseconds()));
    }

    @Test
    public void testOnFailedAttempt() {
        final HeartbeatRequestState heartbeatRequestState = new HeartbeatRequestState(
            LOG_CONTEXT,
            time,
            HEARTBEAT_INTERVAL_MS,
            RETRY_BACKOFF_MS,
            RETRY_BACKOFF_MAX_MS,
            JITTER
        );
        time.sleep(HEARTBEAT_INTERVAL_MS + 100);

        heartbeatRequestState.onFailedAttempt(time.milliseconds());

        assertFalse(heartbeatRequestState.canSendRequest(time.milliseconds()));
        assertTrue(heartbeatRequestState.timeToNextHeartbeatMs(time.milliseconds()) > 0);
    }
}