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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.utils.MockTime;
import org.junit.Before;
import org.junit.Test;

public class ClusterConnectionStatesTest {

    private final MockTime time = new MockTime();
    private final long reconnectBackoffMs = 10 * 1000;
    private final long reconnectBackoffMax = 60 * 1000;
    private final double reconnectBackoffJitter = 0.2;
    private final String nodeId1 = "1001";
    private final String nodeId2 = "2002";
    private final String hostTwoIps = "kafka.apache.org";

    private ClusterConnectionStates connectionStates;

    @Before
    public void setup() {
        this.connectionStates = new ClusterConnectionStates(reconnectBackoffMs, reconnectBackoffMax);
    }

    @Test
    public void testClusterConnectionStateChanges() throws UnknownHostException {
        assertTrue(connectionStates.canConnect(nodeId1, time.absoluteMilliseconds()));

        // Start connecting to Node and check state
        connectionStates.connecting(nodeId1, time.absoluteMilliseconds(), "localhost", ClientDnsLookup.DEFAULT);
        assertEquals(connectionStates.connectionState(nodeId1), ConnectionState.CONNECTING);
        assertTrue(connectionStates.isConnecting(nodeId1));
        assertFalse(connectionStates.isReady(nodeId1, time.absoluteMilliseconds()));
        assertFalse(connectionStates.isBlackedOut(nodeId1, time.absoluteMilliseconds()));
        assertFalse(connectionStates.hasReadyNodes(time.absoluteMilliseconds()));

        time.sleep(100);

        // Successful connection
        connectionStates.ready(nodeId1);
        assertEquals(connectionStates.connectionState(nodeId1), ConnectionState.READY);
        assertTrue(connectionStates.isReady(nodeId1, time.absoluteMilliseconds()));
        assertTrue(connectionStates.hasReadyNodes(time.absoluteMilliseconds()));
        assertFalse(connectionStates.isConnecting(nodeId1));
        assertFalse(connectionStates.isBlackedOut(nodeId1, time.absoluteMilliseconds()));
        assertEquals(connectionStates.connectionDelay(nodeId1, time.absoluteMilliseconds()), Long.MAX_VALUE);

        time.sleep(15000);

        // Disconnected from broker
        connectionStates.disconnected(nodeId1, time.absoluteMilliseconds());
        assertEquals(connectionStates.connectionState(nodeId1), ConnectionState.DISCONNECTED);
        assertTrue(connectionStates.isDisconnected(nodeId1));
        assertTrue(connectionStates.isBlackedOut(nodeId1, time.absoluteMilliseconds()));
        assertFalse(connectionStates.isConnecting(nodeId1));
        assertFalse(connectionStates.hasReadyNodes(time.absoluteMilliseconds()));
        assertFalse(connectionStates.canConnect(nodeId1, time.absoluteMilliseconds()));

        // After disconnecting we expect a backoff value equal to the reconnect.backoff.ms setting (plus minus 20% jitter)
        double backoffTolerance = reconnectBackoffMs * reconnectBackoffJitter;
        long currentBackoff = connectionStates.connectionDelay(nodeId1, time.absoluteMilliseconds());
        assertEquals(reconnectBackoffMs, currentBackoff, backoffTolerance);

        time.sleep(currentBackoff + 1);
        // after waiting for the current backoff value we should be allowed to connect again
        assertTrue(connectionStates.canConnect(nodeId1, time.absoluteMilliseconds()));
    }

    @Test
    public void testMultipleNodeConnectionStates() throws UnknownHostException {
        // Check initial state, allowed to connect to all nodes, but no nodes shown as ready
        assertTrue(connectionStates.canConnect(nodeId1, time.absoluteMilliseconds()));
        assertTrue(connectionStates.canConnect(nodeId2, time.absoluteMilliseconds()));
        assertFalse(connectionStates.hasReadyNodes(time.absoluteMilliseconds()));

        // Start connecting one node and check that the pool only shows ready nodes after
        // successful connect
        connectionStates.connecting(nodeId2, time.absoluteMilliseconds(), "localhost", ClientDnsLookup.DEFAULT);
        assertFalse(connectionStates.hasReadyNodes(time.absoluteMilliseconds()));
        time.sleep(1000);
        connectionStates.ready(nodeId2);
        assertTrue(connectionStates.hasReadyNodes(time.absoluteMilliseconds()));

        // Connect second node and check that both are shown as ready, pool should immediately
        // show ready nodes, since node2 is already connected
        connectionStates.connecting(nodeId1, time.absoluteMilliseconds(), "localhost", ClientDnsLookup.DEFAULT);
        assertTrue(connectionStates.hasReadyNodes(time.absoluteMilliseconds()));
        time.sleep(1000);
        connectionStates.ready(nodeId1);
        assertTrue(connectionStates.hasReadyNodes(time.absoluteMilliseconds()));

        time.sleep(12000);

        // disconnect nodes and check proper state of pool throughout
        connectionStates.disconnected(nodeId2, time.absoluteMilliseconds());
        assertTrue(connectionStates.hasReadyNodes(time.absoluteMilliseconds()));
        assertTrue(connectionStates.isBlackedOut(nodeId2, time.absoluteMilliseconds()));
        assertFalse(connectionStates.isBlackedOut(nodeId1, time.absoluteMilliseconds()));
        time.sleep(connectionStates.connectionDelay(nodeId2, time.absoluteMilliseconds()));
        // by the time node1 disconnects node2 should have been unblocked again
        connectionStates.disconnected(nodeId1, time.absoluteMilliseconds() + 1);
        assertTrue(connectionStates.isBlackedOut(nodeId1, time.absoluteMilliseconds()));
        assertFalse(connectionStates.isBlackedOut(nodeId2, time.absoluteMilliseconds()));
        assertFalse(connectionStates.hasReadyNodes(time.absoluteMilliseconds()));
    }

    @Test
    public void testAuthorizationFailed() throws UnknownHostException {
        // Try connecting
        connectionStates.connecting(nodeId1, time.absoluteMilliseconds(), "localhost", ClientDnsLookup.DEFAULT);

        time.sleep(100);

        connectionStates.authenticationFailed(nodeId1, time.absoluteMilliseconds(), new AuthenticationException("No path to CA for certificate!"));
        time.sleep(1000);
        assertEquals(connectionStates.connectionState(nodeId1), ConnectionState.AUTHENTICATION_FAILED);
        assertTrue(connectionStates.authenticationException(nodeId1) instanceof AuthenticationException);
        assertFalse(connectionStates.hasReadyNodes(time.absoluteMilliseconds()));
        assertFalse(connectionStates.canConnect(nodeId1, time.absoluteMilliseconds()));

        time.sleep(connectionStates.connectionDelay(nodeId1, time.absoluteMilliseconds()) + 1);

        assertTrue(connectionStates.canConnect(nodeId1, time.absoluteMilliseconds()));
        connectionStates.ready(nodeId1);
        assertNull(connectionStates.authenticationException(nodeId1));
    }

    @Test
    public void testRemoveNode() throws UnknownHostException {
        connectionStates.connecting(nodeId1, time.absoluteMilliseconds(), "localhost", ClientDnsLookup.DEFAULT);
        time.sleep(1000);
        connectionStates.ready(nodeId1);
        time.sleep(10000);

        connectionStates.disconnected(nodeId1, time.absoluteMilliseconds());
        // Node is disconnected and blocked, removing it from the list should reset all blocks
        connectionStates.remove(nodeId1);
        assertTrue(connectionStates.canConnect(nodeId1, time.absoluteMilliseconds()));
        assertFalse(connectionStates.isBlackedOut(nodeId1, time.absoluteMilliseconds()));
        assertEquals(connectionStates.connectionDelay(nodeId1, time.absoluteMilliseconds()), 0L);
    }

    @Test
    public void testMaxReconnectBackoff() throws UnknownHostException {
        long effectiveMaxReconnectBackoff = Math.round(reconnectBackoffMax * (1 + reconnectBackoffJitter));
        connectionStates.connecting(nodeId1, time.absoluteMilliseconds(), "localhost", ClientDnsLookup.DEFAULT);
        time.sleep(1000);
        connectionStates.disconnected(nodeId1, time.absoluteMilliseconds());

        // Do 100 reconnect attempts and check that MaxReconnectBackoff (plus jitter) is not exceeded
        for (int i = 0; i < 100; i++) {
            long reconnectBackoff = connectionStates.connectionDelay(nodeId1, time.absoluteMilliseconds());
            assertTrue(reconnectBackoff <= effectiveMaxReconnectBackoff);
            assertFalse(connectionStates.canConnect(nodeId1, time.absoluteMilliseconds()));
            time.sleep(reconnectBackoff + 1);
            assertTrue(connectionStates.canConnect(nodeId1, time.absoluteMilliseconds()));
            connectionStates.connecting(nodeId1, time.absoluteMilliseconds(), "localhost", ClientDnsLookup.DEFAULT);
            time.sleep(10);
            connectionStates.disconnected(nodeId1, time.absoluteMilliseconds());
        }
    }

    @Test
    public void testExponentialReconnectBackoff() throws UnknownHostException {
        // Calculate fixed components for backoff process
        final int reconnectBackoffExpBase = 2;
        double reconnectBackoffMaxExp = Math.log(reconnectBackoffMax / (double) Math.max(reconnectBackoffMs, 1))
            / Math.log(reconnectBackoffExpBase);

        // Run through 10 disconnects and check that reconnect backoff value is within expected range for every attempt
        for (int i = 0; i < 10; i++) {
            connectionStates.connecting(nodeId1, time.absoluteMilliseconds(), "localhost", ClientDnsLookup.DEFAULT);
            connectionStates.disconnected(nodeId1, time.absoluteMilliseconds());
            // Calculate expected backoff value without jitter
            long expectedBackoff = Math.round(Math.pow(reconnectBackoffExpBase, Math.min(i, reconnectBackoffMaxExp))
                * reconnectBackoffMs);
            long currentBackoff = connectionStates.connectionDelay(nodeId1, time.absoluteMilliseconds());
            assertEquals(expectedBackoff, currentBackoff, reconnectBackoffJitter * expectedBackoff);
            time.sleep(connectionStates.connectionDelay(nodeId1, time.absoluteMilliseconds()) + 1);
        }
    }

    @Test
    public void testThrottled() throws UnknownHostException {
        connectionStates.connecting(nodeId1, time.absoluteMilliseconds(), "localhost", ClientDnsLookup.DEFAULT);
        time.sleep(1000);
        connectionStates.ready(nodeId1);
        time.sleep(10000);

        // Initially not throttled.
        assertEquals(0, connectionStates.throttleDelayMs(nodeId1, time.absoluteMilliseconds()));

        // Throttle for 100ms from now.
        connectionStates.throttle(nodeId1, time.absoluteMilliseconds() + 100);
        assertEquals(100, connectionStates.throttleDelayMs(nodeId1, time.absoluteMilliseconds()));

        // Still throttled after 50ms. The remaining delay is 50ms. The poll delay should be same as throttling delay.
        time.sleep(50);
        assertEquals(50, connectionStates.throttleDelayMs(nodeId1, time.absoluteMilliseconds()));
        assertEquals(50, connectionStates.pollDelayMs(nodeId1, time.absoluteMilliseconds()));

        // Not throttled anymore when the deadline is reached. The poll delay should be same as connection delay.
        time.sleep(50);
        assertEquals(0, connectionStates.throttleDelayMs(nodeId1, time.absoluteMilliseconds()));
        assertEquals(connectionStates.connectionDelay(nodeId1, time.absoluteMilliseconds()),
            connectionStates.pollDelayMs(nodeId1, time.absoluteMilliseconds()));
    }

    @Test
    public void testSingleIPWithDefault() throws UnknownHostException {
        connectionStates.connecting(nodeId1, time.absoluteMilliseconds(), "localhost", ClientDnsLookup.DEFAULT);
        InetAddress currAddress = connectionStates.currentAddress(nodeId1);
        connectionStates.connecting(nodeId1, time.absoluteMilliseconds(), "localhost", ClientDnsLookup.DEFAULT);
        assertSame(currAddress, connectionStates.currentAddress(nodeId1));
    }

    @Test
    public void testSingleIPWithUseAll() throws UnknownHostException {
        assertEquals(1, ClientUtils.resolve("localhost", ClientDnsLookup.USE_ALL_DNS_IPS).size());

        connectionStates.connecting(nodeId1, time.absoluteMilliseconds(), "localhost", ClientDnsLookup.USE_ALL_DNS_IPS);
        InetAddress currAddress = connectionStates.currentAddress(nodeId1);
        connectionStates.connecting(nodeId1, time.absoluteMilliseconds(), "localhost", ClientDnsLookup.USE_ALL_DNS_IPS);
        assertSame(currAddress, connectionStates.currentAddress(nodeId1));
    }

    @Test
    public void testMultipleIPsWithDefault() throws UnknownHostException {
        assertEquals(2, ClientUtils.resolve(hostTwoIps, ClientDnsLookup.USE_ALL_DNS_IPS).size());

        connectionStates.connecting(nodeId1, time.absoluteMilliseconds(), hostTwoIps, ClientDnsLookup.DEFAULT);
        InetAddress currAddress = connectionStates.currentAddress(nodeId1);
        connectionStates.connecting(nodeId1, time.absoluteMilliseconds(), hostTwoIps, ClientDnsLookup.DEFAULT);
        assertSame(currAddress, connectionStates.currentAddress(nodeId1));
    }

    @Test
    public void testMultipleIPsWithUseAll() throws UnknownHostException {
        assertEquals(2, ClientUtils.resolve(hostTwoIps, ClientDnsLookup.USE_ALL_DNS_IPS).size());

        connectionStates.connecting(nodeId1, time.absoluteMilliseconds(), hostTwoIps, ClientDnsLookup.USE_ALL_DNS_IPS);
        InetAddress addr1 = connectionStates.currentAddress(nodeId1);
        connectionStates.connecting(nodeId1, time.absoluteMilliseconds(), hostTwoIps, ClientDnsLookup.USE_ALL_DNS_IPS);
        InetAddress addr2 = connectionStates.currentAddress(nodeId1);
        assertNotSame(addr1, addr2);

        connectionStates.connecting(nodeId1, time.absoluteMilliseconds(), hostTwoIps, ClientDnsLookup.USE_ALL_DNS_IPS);
        InetAddress addr3 = connectionStates.currentAddress(nodeId1);
        assertSame(addr1, addr3);
    }
}
