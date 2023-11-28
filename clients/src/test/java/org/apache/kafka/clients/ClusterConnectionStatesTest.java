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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.InetAddress;
import java.net.UnknownHostException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ClusterConnectionStatesTest {

    private static ArrayList<InetAddress> initialAddresses;
    private static ArrayList<InetAddress> newAddresses;

    static {
        try {
            initialAddresses = new ArrayList<>(Arrays.asList(
                    InetAddress.getByName("10.200.20.100"),
                    InetAddress.getByName("10.200.20.101"),
                    InetAddress.getByName("10.200.20.102")
            ));
            newAddresses = new ArrayList<>(Arrays.asList(
                    InetAddress.getByName("10.200.20.103"),
                    InetAddress.getByName("10.200.20.104"),
                    InetAddress.getByName("10.200.20.105")
            ));
        } catch (UnknownHostException e) {
            fail("Attempted to create an invalid InetAddress, this should not happen");
        }
    }

    private final MockTime time = new MockTime();
    private final long reconnectBackoffMs = 10 * 1000;
    private final long reconnectBackoffMax = 60 * 1000;
    private final long connectionSetupTimeoutMs = 10 * 1000;
    private final long connectionSetupTimeoutMaxMs = 127 * 1000;
    private final int reconnectBackoffExpBase = ClusterConnectionStates.RECONNECT_BACKOFF_EXP_BASE;
    private final double reconnectBackoffJitter = ClusterConnectionStates.RECONNECT_BACKOFF_JITTER;
    private final int connectionSetupTimeoutExpBase = ClusterConnectionStates.CONNECTION_SETUP_TIMEOUT_EXP_BASE;
    private final double connectionSetupTimeoutJitter = ClusterConnectionStates.CONNECTION_SETUP_TIMEOUT_JITTER;
    private final String nodeId1 = "1001";
    private final String nodeId2 = "2002";
    private final String nodeId3 = "3003";
    private final String hostTwoIps = "multiple.ip.address";
    private ClusterConnectionStates connectionStates;

    // For testing nodes with a single IP address, use localhost and default DNS resolution
    private DefaultHostResolver singleIPHostResolver = new DefaultHostResolver();

    // For testing nodes with multiple IP addresses, mock DNS resolution to get consistent results
    private AddressChangeHostResolver multipleIPHostResolver = new AddressChangeHostResolver(
            initialAddresses.toArray(new InetAddress[0]), newAddresses.toArray(new InetAddress[0]));

    @BeforeEach
    public void setup() {
        this.connectionStates = new ClusterConnectionStates(reconnectBackoffMs, reconnectBackoffMax,
                connectionSetupTimeoutMs, connectionSetupTimeoutMaxMs, new LogContext(), this.singleIPHostResolver);
    }

    @Test
    public void testClusterConnectionStateChanges() {
        assertTrue(connectionStates.canConnect(nodeId1, time.milliseconds()));
        assertEquals(0, connectionStates.connectionDelay(nodeId1, time.milliseconds()));

        // Start connecting to Node and check state
        connectionStates.connecting(nodeId1, time.milliseconds(), "localhost");
        assertEquals(ConnectionState.CONNECTING, connectionStates.connectionState(nodeId1));
        assertTrue(connectionStates.isConnecting(nodeId1));
        assertFalse(connectionStates.isReady(nodeId1, time.milliseconds()));
        assertFalse(connectionStates.isBlackedOut(nodeId1, time.milliseconds()));
        assertFalse(connectionStates.hasReadyNodes(time.milliseconds()));
        long connectionDelay = connectionStates.connectionDelay(nodeId1, time.milliseconds());
        double connectionDelayDelta = connectionSetupTimeoutMs * connectionSetupTimeoutJitter;
        assertEquals(connectionSetupTimeoutMs, connectionDelay, connectionDelayDelta);

        time.sleep(100);

        // Successful connection
        connectionStates.ready(nodeId1);
        assertEquals(ConnectionState.READY, connectionStates.connectionState(nodeId1));
        assertTrue(connectionStates.isReady(nodeId1, time.milliseconds()));
        assertTrue(connectionStates.hasReadyNodes(time.milliseconds()));
        assertFalse(connectionStates.isConnecting(nodeId1));
        assertFalse(connectionStates.isBlackedOut(nodeId1, time.milliseconds()));
        assertEquals(Long.MAX_VALUE, connectionStates.connectionDelay(nodeId1, time.milliseconds()));

        time.sleep(15000);

        // Disconnected from broker
        connectionStates.disconnected(nodeId1, time.milliseconds());
        assertEquals(ConnectionState.DISCONNECTED, connectionStates.connectionState(nodeId1));
        assertTrue(connectionStates.isDisconnected(nodeId1));
        assertTrue(connectionStates.isBlackedOut(nodeId1, time.milliseconds()));
        assertFalse(connectionStates.isConnecting(nodeId1));
        assertFalse(connectionStates.hasReadyNodes(time.milliseconds()));
        assertFalse(connectionStates.canConnect(nodeId1, time.milliseconds()));

        // After disconnecting we expect a backoff value equal to the reconnect.backoff.ms setting (plus minus 20% jitter)
        double backoffTolerance = reconnectBackoffMs * reconnectBackoffJitter;
        long currentBackoff = connectionStates.connectionDelay(nodeId1, time.milliseconds());
        assertEquals(reconnectBackoffMs, currentBackoff, backoffTolerance);

        time.sleep(currentBackoff + 1);
        // after waiting for the current backoff value we should be allowed to connect again
        assertTrue(connectionStates.canConnect(nodeId1, time.milliseconds()));
    }

    @Test
    public void testMultipleNodeConnectionStates() {
        // Check initial state, allowed to connect to all nodes, but no nodes shown as ready
        assertTrue(connectionStates.canConnect(nodeId1, time.milliseconds()));
        assertTrue(connectionStates.canConnect(nodeId2, time.milliseconds()));
        assertFalse(connectionStates.hasReadyNodes(time.milliseconds()));

        // Start connecting one node and check that the pool only shows ready nodes after
        // successful connect
        connectionStates.connecting(nodeId2, time.milliseconds(), "localhost");
        assertFalse(connectionStates.hasReadyNodes(time.milliseconds()));
        time.sleep(1000);
        connectionStates.ready(nodeId2);
        assertTrue(connectionStates.hasReadyNodes(time.milliseconds()));

        // Connect second node and check that both are shown as ready, pool should immediately
        // show ready nodes, since node2 is already connected
        connectionStates.connecting(nodeId1, time.milliseconds(), "localhost");
        assertTrue(connectionStates.hasReadyNodes(time.milliseconds()));
        time.sleep(1000);
        connectionStates.ready(nodeId1);
        assertTrue(connectionStates.hasReadyNodes(time.milliseconds()));

        time.sleep(12000);

        // disconnect nodes and check proper state of pool throughout
        connectionStates.disconnected(nodeId2, time.milliseconds());
        assertTrue(connectionStates.hasReadyNodes(time.milliseconds()));
        assertTrue(connectionStates.isBlackedOut(nodeId2, time.milliseconds()));
        assertFalse(connectionStates.isBlackedOut(nodeId1, time.milliseconds()));
        time.sleep(connectionStates.connectionDelay(nodeId2, time.milliseconds()));
        // by the time node1 disconnects node2 should have been unblocked again
        connectionStates.disconnected(nodeId1, time.milliseconds() + 1);
        assertTrue(connectionStates.isBlackedOut(nodeId1, time.milliseconds()));
        assertFalse(connectionStates.isBlackedOut(nodeId2, time.milliseconds()));
        assertFalse(connectionStates.hasReadyNodes(time.milliseconds()));
    }

    @Test
    public void testAuthorizationFailed() {
        // Try connecting
        connectionStates.connecting(nodeId1, time.milliseconds(), "localhost");

        time.sleep(100);

        connectionStates.authenticationFailed(nodeId1, time.milliseconds(), new AuthenticationException("No path to CA for certificate!"));
        time.sleep(1000);
        assertEquals(connectionStates.connectionState(nodeId1), ConnectionState.AUTHENTICATION_FAILED);
        assertNotNull(connectionStates.authenticationException(nodeId1));
        assertFalse(connectionStates.hasReadyNodes(time.milliseconds()));
        assertFalse(connectionStates.canConnect(nodeId1, time.milliseconds()));

        time.sleep(connectionStates.connectionDelay(nodeId1, time.milliseconds()) + 1);

        assertTrue(connectionStates.canConnect(nodeId1, time.milliseconds()));
        connectionStates.ready(nodeId1);
        assertNull(connectionStates.authenticationException(nodeId1));
    }

    @Test
    public void testRemoveNode() {
        connectionStates.connecting(nodeId1, time.milliseconds(), "localhost");
        time.sleep(1000);
        connectionStates.ready(nodeId1);
        time.sleep(10000);

        connectionStates.disconnected(nodeId1, time.milliseconds());
        // Node is disconnected and blocked, removing it from the list should reset all blocks
        connectionStates.remove(nodeId1);
        assertTrue(connectionStates.canConnect(nodeId1, time.milliseconds()));
        assertFalse(connectionStates.isBlackedOut(nodeId1, time.milliseconds()));
        assertEquals(connectionStates.connectionDelay(nodeId1, time.milliseconds()), 0L);
    }

    @Test
    public void testMaxReconnectBackoff() {
        long effectiveMaxReconnectBackoff = Math.round(reconnectBackoffMax * (1 + reconnectBackoffJitter));
        connectionStates.connecting(nodeId1, time.milliseconds(), "localhost");
        time.sleep(1000);
        connectionStates.disconnected(nodeId1, time.milliseconds());

        // Do 100 reconnect attempts and check that MaxReconnectBackoff (plus jitter) is not exceeded
        for (int i = 0; i < 100; i++) {
            long reconnectBackoff = connectionStates.connectionDelay(nodeId1, time.milliseconds());
            assertTrue(reconnectBackoff <= effectiveMaxReconnectBackoff);
            assertFalse(connectionStates.canConnect(nodeId1, time.milliseconds()));
            time.sleep(reconnectBackoff + 1);
            assertTrue(connectionStates.canConnect(nodeId1, time.milliseconds()));
            connectionStates.connecting(nodeId1, time.milliseconds(), "localhost");
            time.sleep(10);
            connectionStates.disconnected(nodeId1, time.milliseconds());
        }
    }

    @Test
    public void testExponentialReconnectBackoff() {
        verifyReconnectExponentialBackoff(false);
        verifyReconnectExponentialBackoff(true);
    }

    @Test
    public void testThrottled() {
        connectionStates.connecting(nodeId1, time.milliseconds(), "localhost");
        time.sleep(1000);
        connectionStates.ready(nodeId1);
        time.sleep(10000);

        // Initially not throttled.
        assertEquals(0, connectionStates.throttleDelayMs(nodeId1, time.milliseconds()));

        // Throttle for 100ms from now.
        connectionStates.throttle(nodeId1, time.milliseconds() + 100);
        assertEquals(100, connectionStates.throttleDelayMs(nodeId1, time.milliseconds()));

        // Still throttled after 50ms. The remaining delay is 50ms. The poll delay should be same as throttling delay.
        time.sleep(50);
        assertEquals(50, connectionStates.throttleDelayMs(nodeId1, time.milliseconds()));
        assertEquals(50, connectionStates.pollDelayMs(nodeId1, time.milliseconds()));

        // Not throttled anymore when the deadline is reached. The poll delay should be same as connection delay.
        time.sleep(50);
        assertEquals(0, connectionStates.throttleDelayMs(nodeId1, time.milliseconds()));
        assertEquals(connectionStates.connectionDelay(nodeId1, time.milliseconds()),
            connectionStates.pollDelayMs(nodeId1, time.milliseconds()));
    }

    @Test
    public void testSingleIP() throws UnknownHostException {
        InetAddress[] localhostIps = Stream.of(InetAddress.getByName("127.0.0.1")).toArray(InetAddress[]::new);
        HostResolver hostResolver = host -> {
            assertEquals("localhost", host);
            return localhostIps;
        };

        connectionStates = new ClusterConnectionStates(reconnectBackoffMs, reconnectBackoffMax,
            connectionSetupTimeoutMs, connectionSetupTimeoutMaxMs, new LogContext(), hostResolver);

        connectionStates.connecting(nodeId1, time.milliseconds(), "localhost");
        InetAddress currAddress = connectionStates.currentAddress(nodeId1);
        connectionStates.connecting(nodeId1, time.milliseconds(), "localhost");
        assertSame(currAddress, connectionStates.currentAddress(nodeId1));
    }

    @Test
    public void testMultipleIPs() throws UnknownHostException {
        setupMultipleIPs();

        assertTrue(ClientUtils.resolve(hostTwoIps, multipleIPHostResolver).size() > 1);

        connectionStates.connecting(nodeId1, time.milliseconds(), hostTwoIps);
        InetAddress addr1 = connectionStates.currentAddress(nodeId1);
        connectionStates.connecting(nodeId1, time.milliseconds(), hostTwoIps);
        InetAddress addr2 = connectionStates.currentAddress(nodeId1);
        assertNotSame(addr1, addr2);
        connectionStates.connecting(nodeId1, time.milliseconds(), hostTwoIps);
        InetAddress addr3 = connectionStates.currentAddress(nodeId1);
        assertNotSame(addr1, addr3);
    }

    @Test
    public void testHostResolveChange() throws UnknownHostException {
        setupMultipleIPs();

        assertTrue(ClientUtils.resolve(hostTwoIps, multipleIPHostResolver).size() > 1);

        connectionStates.connecting(nodeId1, time.milliseconds(), hostTwoIps);
        InetAddress addr1 = connectionStates.currentAddress(nodeId1);

        multipleIPHostResolver.changeAddresses();
        connectionStates.connecting(nodeId1, time.milliseconds(), "localhost");
        InetAddress addr2 = connectionStates.currentAddress(nodeId1);

        assertNotSame(addr1, addr2);
    }

    @Test
    public void testNodeWithNewHostname() throws UnknownHostException {
        setupMultipleIPs();

        connectionStates.connecting(nodeId1, time.milliseconds(), "localhost");
        InetAddress addr1 = connectionStates.currentAddress(nodeId1);

        this.multipleIPHostResolver.changeAddresses();
        connectionStates.connecting(nodeId1, time.milliseconds(), hostTwoIps);
        InetAddress addr2 = connectionStates.currentAddress(nodeId1);

        assertNotSame(addr1, addr2);
    }

    @Test
    public void testIsPreparingConnection() {
        assertFalse(connectionStates.isPreparingConnection(nodeId1));
        connectionStates.connecting(nodeId1, time.milliseconds(), "localhost");
        assertTrue(connectionStates.isPreparingConnection(nodeId1));
        connectionStates.checkingApiVersions(nodeId1);
        assertTrue(connectionStates.isPreparingConnection(nodeId1));
        connectionStates.disconnected(nodeId1, time.milliseconds());
        assertFalse(connectionStates.isPreparingConnection(nodeId1));
    }

    @Test
    public void testExponentialConnectionSetupTimeout() {
        assertTrue(connectionStates.canConnect(nodeId1, time.milliseconds()));

        // Check the exponential timeout growth
        for (int n = 0; n <= Math.log((double) connectionSetupTimeoutMaxMs / connectionSetupTimeoutMs) / Math.log(connectionSetupTimeoutExpBase); n++) {
            connectionStates.connecting(nodeId1, time.milliseconds(), "localhost");
            assertTrue(connectionStates.connectingNodes().contains(nodeId1));
            assertEquals(connectionSetupTimeoutMs * Math.pow(connectionSetupTimeoutExpBase, n),
                    connectionStates.connectionSetupTimeoutMs(nodeId1),
                    connectionSetupTimeoutMs * Math.pow(connectionSetupTimeoutExpBase, n) * connectionSetupTimeoutJitter);
            connectionStates.disconnected(nodeId1, time.milliseconds());
            assertFalse(connectionStates.connectingNodes().contains(nodeId1));
        }

        // Check the timeout value upper bound
        connectionStates.connecting(nodeId1, time.milliseconds(), "localhost");
        assertEquals(connectionSetupTimeoutMaxMs,
                connectionStates.connectionSetupTimeoutMs(nodeId1),
                connectionSetupTimeoutMaxMs * connectionSetupTimeoutJitter);
        assertTrue(connectionStates.connectingNodes().contains(nodeId1));

        // Should reset the timeout value to the init value
        connectionStates.ready(nodeId1);
        assertEquals(connectionSetupTimeoutMs,
                connectionStates.connectionSetupTimeoutMs(nodeId1),
                connectionSetupTimeoutMs * connectionSetupTimeoutJitter);
        assertFalse(connectionStates.connectingNodes().contains(nodeId1));
        connectionStates.disconnected(nodeId1, time.milliseconds());

        // Check if the connection state transition from ready to disconnected
        // won't increase the timeout value
        connectionStates.connecting(nodeId1, time.milliseconds(), "localhost");
        assertEquals(connectionSetupTimeoutMs,
                connectionStates.connectionSetupTimeoutMs(nodeId1),
                connectionSetupTimeoutMs * connectionSetupTimeoutJitter);
        assertTrue(connectionStates.connectingNodes().contains(nodeId1));
    }

    @Test
    public void testTimedOutConnections() {
        // Initiate two connections
        connectionStates.connecting(nodeId1, time.milliseconds(), "localhost");
        connectionStates.connecting(nodeId2, time.milliseconds(), "localhost");

        // Expect no timed out connections
        assertEquals(0, connectionStates.nodesWithConnectionSetupTimeout(time.milliseconds()).size());

        // Advance time by half of the connection setup timeout
        time.sleep(connectionSetupTimeoutMs / 2);

        // Initiate a third connection
        connectionStates.connecting(nodeId3, time.milliseconds(), "localhost");

        // Advance time beyond the connection setup timeout (+ max jitter) for the first two connections
        time.sleep((long) (connectionSetupTimeoutMs / 2 + connectionSetupTimeoutMs * connectionSetupTimeoutJitter));

        // Expect two timed out connections.
        List<String> timedOutConnections = connectionStates.nodesWithConnectionSetupTimeout(time.milliseconds());
        assertEquals(2, timedOutConnections.size());
        assertTrue(timedOutConnections.contains(nodeId1));
        assertTrue(timedOutConnections.contains(nodeId2));

        // Disconnect the first two connections
        connectionStates.disconnected(nodeId1, time.milliseconds());
        connectionStates.disconnected(nodeId2, time.milliseconds());

        // Advance time beyond the connection setup timeout (+ max jitter) for the third connections
        time.sleep((long) (connectionSetupTimeoutMs / 2 + connectionSetupTimeoutMs * connectionSetupTimeoutJitter));

        // Expect one timed out connection
        timedOutConnections = connectionStates.nodesWithConnectionSetupTimeout(time.milliseconds());
        assertEquals(1, timedOutConnections.size());
        assertTrue(timedOutConnections.contains(nodeId3));

        // Disconnect the third connection
        connectionStates.disconnected(nodeId3, time.milliseconds());

        // Expect no timed out connections
        assertEquals(0, connectionStates.nodesWithConnectionSetupTimeout(time.milliseconds()).size());
    }

    @Test
    public void testSkipLastAttemptedIp() throws UnknownHostException {
        setupMultipleIPs();

        assertTrue(ClientUtils.resolve(hostTwoIps, multipleIPHostResolver).size() > 1);

        // Connect to the first IP
        connectionStates.connecting(nodeId1, time.milliseconds(), hostTwoIps);
        InetAddress addr1 = connectionStates.currentAddress(nodeId1);

        // Disconnect, which will trigger re-resolution with the first IP still first
        connectionStates.disconnected(nodeId1, time.milliseconds());

        // Connect again, the first IP should get skipped
        connectionStates.connecting(nodeId1, time.milliseconds(), hostTwoIps);
        InetAddress addr2 = connectionStates.currentAddress(nodeId1);
        assertNotSame(addr1, addr2);
    }
    
    private void setupMultipleIPs() {
        this.connectionStates = new ClusterConnectionStates(reconnectBackoffMs, reconnectBackoffMax,
                connectionSetupTimeoutMs, connectionSetupTimeoutMaxMs, new LogContext(), this.multipleIPHostResolver);
    }

    private void verifyReconnectExponentialBackoff(boolean enterCheckingApiVersionState) {
        double reconnectBackoffMaxExp = Math.log(reconnectBackoffMax / (double) Math.max(reconnectBackoffMs, 1))
            / Math.log(reconnectBackoffExpBase);

        connectionStates.remove(nodeId1);
        // Run through 10 disconnects and check that reconnect backoff value is within expected range for every attempt
        for (int i = 0; i < 10; i++) {
            connectionStates.connecting(nodeId1, time.milliseconds(), "localhost");
            if (enterCheckingApiVersionState) {
                connectionStates.checkingApiVersions(nodeId1);
            }

            connectionStates.disconnected(nodeId1, time.milliseconds());
            // Calculate expected backoff value without jitter
            long expectedBackoff = Math.round(Math.pow(reconnectBackoffExpBase, Math.min(i, reconnectBackoffMaxExp))
                * reconnectBackoffMs);
            long currentBackoff = connectionStates.connectionDelay(nodeId1, time.milliseconds());
            assertEquals(expectedBackoff, currentBackoff, reconnectBackoffJitter * expectedBackoff);
            time.sleep(connectionStates.connectionDelay(nodeId1, time.milliseconds()) + 1);
        }
    }
}
