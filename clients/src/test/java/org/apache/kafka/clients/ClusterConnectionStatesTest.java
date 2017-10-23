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

import static org.junit.Assert.assertEquals;

import org.apache.kafka.common.utils.MockTime;
import org.junit.Before;
import org.junit.Test;

public class ClusterConnectionStatesTest {

    protected final MockTime time = new MockTime();
    protected final long reconnectBackoffMsTest = 10 * 1000;
    protected final long reconnectBackoffMaxTest = 60 * 1000;
    protected final double reconnectBackoffJitter = 0.2;
    private final String nodeId1 = "1001";
    private final String nodeId2 = "2002";

    private ClusterConnectionStates connectionStates;

    @Before
    public void setup() {
        this.connectionStates = new ClusterConnectionStates(reconnectBackoffMsTest, reconnectBackoffMaxTest);
    }

    @Test
    public void testExponentialReconnectBackoff() {
        // Calculate fixed components for backoff process
        final int reconnectBackoffExpBase = 2;
        double reconnectBackoffMaxExp = Math.log(reconnectBackoffMaxTest / (double) Math.max(reconnectBackoffMsTest, 1))
            / Math.log(reconnectBackoffExpBase);

        // Run through 10 disconnects and check that reconnect backoff value is within expected range for every attempt
        for (int i = 0; i < 10; i++) {
            connectionStates.connecting(nodeId1, time.milliseconds());
            connectionStates.disconnected(nodeId1, time.milliseconds());
            // Calculate expected backoff value without jitter
            long expectedBackoff = Math.round(Math.pow(reconnectBackoffExpBase, Math.min(i, reconnectBackoffMaxExp))
                * reconnectBackoffMsTest);
            long currentBackoff = connectionStates.connectionDelay(nodeId1, time.milliseconds());
            assertEquals(expectedBackoff, currentBackoff, reconnectBackoffJitter * expectedBackoff);
            time.sleep(connectionStates.connectionDelay(nodeId1, time.milliseconds()) + 1);
        }
    }
}
