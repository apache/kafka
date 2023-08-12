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
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

public class HeartbeatRequestManagerTest {

    private static final long RETRY_BACKOFF_MS = 500;
    private static final long HEARTBEAT_INTERVAL_MS = 3000;
    private MemberState memberState = new MemberState("groupId",  null, null);
    private CoordinatorRequestManager coordinatorRequestManager = mock(CoordinatorRequestManager.class);
    private Time time = mock(Time.class);
    private HeartbeatRequestManager heartbeatRequestManager;

    @BeforeEach
    private void setup() {
        this.heartbeatRequestManager = createManger(
            time,
            new LogContext(),
            RETRY_BACKOFF_MS,
            HEARTBEAT_INTERVAL_MS,
            memberState,
            coordinatorRequestManager);
    }

    @Test
    public void testShouldHeartbeat() {
        assertTrue(heartbeatRequestManager.shouldHeartbeat(0));
    }

    private HeartbeatRequestManager createManger(
        final Time time,
        final LogContext logContext,
        final long retryBackoffMs,
        final long heartbeatIntervalMs,
        final MemberState memberState,
        final CoordinatorRequestManager coordinatorRequestManager) {
        return spy(new HeartbeatRequestManager(
            time,
            logContext,
            retryBackoffMs,
            heartbeatIntervalMs,
            0, // rebalance timeout
            memberState,
            coordinatorRequestManager,
            null // subscriptions
        ));
    }
}
