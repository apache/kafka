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

package org.apache.kafka.clients;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.kafka.common.utils.MockTime;
import org.junit.Before;
import org.junit.Test;

public class ClusterConnectionStatesTest {
    protected final MockTime time = new MockTime();
    protected final long reconnectBackoffMsTest = 10 * 1000;
    protected final long reconnectBackoffMaxTest = 60 * 1000;
    protected final double reconnectBackoffJitter = 0.2;
    private final String nodeId = "1001";

    private ClusterConnectionStates connectionStates;

    @Before
    public void setup() {
        this.connectionStates = new ClusterConnectionStates(reconnectBackoffMsTest, reconnectBackoffMaxTest);
    }

    @Test
    public void testMaxReconnectBackoff() {
        Long effectiveMaxReconnectBackoff = Math.round(reconnectBackoffMaxTest * (1 + reconnectBackoffJitter));
        connectionStates.connecting(nodeId, time.milliseconds());
        time.sleep(1000);
        connectionStates.disconnected(nodeId, time.milliseconds());

        // Do 100 reconnect attempts and check that MaxReconnectBackoff (plus jitter) is not exceeded
        for (int i = 0; i < 100; i++) {
            long reconnectBackoff = connectionStates.connectionDelay(nodeId, time.milliseconds());
            assertTrue("Expected reconnect backoff to be below 'reconnect.backoff.max.ms' (with 20% jitter).", reconnectBackoff <= effectiveMaxReconnectBackoff);
            assertFalse("Expected connection to be blocked immediately after disconnect.", connectionStates.canConnect(nodeId, time.milliseconds()));
            time.sleep(reconnectBackoff + 1);
            assertTrue("Expected connection to be ready for reconnect after waiting for backoff time to pass.", connectionStates.canConnect(nodeId, time.milliseconds()));
            connectionStates.connecting(nodeId, time.milliseconds());
            time.sleep(10);
            connectionStates.disconnected(nodeId, time.milliseconds());
        }
    }
}