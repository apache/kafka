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
package org.apache.kafka.raft;

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ConnectionCacheTest {
    private final MockTime time = new MockTime();
    private final MockNetworkChannel networkChannel = new MockNetworkChannel();
    private final LogContext logContext = new LogContext();
    private final int requestTimeoutMs = 30000;
    private final int retryBackoffMs = 100;

    @Test
    public void testMaybeUpdate() {
        List<InetSocketAddress> bootstrapServers = Collections.singletonList(
            new InetSocketAddress("127.0.0.1", 9092)
        );

        ConnectionCache cache = new ConnectionCache(networkChannel,
            bootstrapServers,
            retryBackoffMs,
            requestTimeoutMs,
            logContext);

        ConnectionCache.HostInfo initialHostInfo = new ConnectionCache.HostInfo(
            new InetSocketAddress("127.0.0.1", 9093),
            1L
        );

        cache.maybeUpdate(1, initialHostInfo);
        assertEquals(Optional.of(initialHostInfo), cache.getOrCreate(1).hostInfo());

        ConnectionCache.HostInfo updatedHostInfo = new ConnectionCache.HostInfo(
            new InetSocketAddress("127.0.0.1", 9093),
            2L
        );

        cache.maybeUpdate(1, updatedHostInfo);
        assertEquals(Optional.of(updatedHostInfo), cache.getOrCreate(1).hostInfo());

        cache.maybeUpdate(1, initialHostInfo);
        assertEquals(Optional.of(updatedHostInfo), cache.getOrCreate(1).hostInfo());
    }

    @Test
    public void testResetConnectionStateAfterHostInfoUpdate() {
        List<InetSocketAddress> bootstrapServers = Collections.singletonList(
            new InetSocketAddress("127.0.0.1", 9092)
        );

        ConnectionCache cache = new ConnectionCache(networkChannel,
            bootstrapServers,
            retryBackoffMs,
            requestTimeoutMs,
            logContext);

        ConnectionCache.HostInfo initialHostInfo = new ConnectionCache.HostInfo(
            new InetSocketAddress("127.0.0.1", 9093),
            1L
        );

        ConnectionCache.ConnectionState connectionState = cache.getOrCreate(1);
        cache.maybeUpdate(1, initialHostInfo);

        long correlationId = 1;
        connectionState.onRequestSent(correlationId, time.milliseconds());
        assertFalse(connectionState.isReady(time.milliseconds()));

        ConnectionCache.HostInfo updatedHostInfo = new ConnectionCache.HostInfo(
            new InetSocketAddress("127.0.0.1", 9093),
            2L
        );

        cache.maybeUpdate(1, updatedHostInfo);
        assertTrue(connectionState.isReady(time.milliseconds()));
    }

    @Test
    public void testResetAllConnections() {
        List<InetSocketAddress> bootstrapServers = Collections.singletonList(
            new InetSocketAddress("127.0.0.1", 9092)
        );

        ConnectionCache cache = new ConnectionCache(networkChannel,
            bootstrapServers,
            retryBackoffMs,
            requestTimeoutMs,
            logContext);

        cache.maybeUpdate(1, new ConnectionCache.HostInfo(
            new InetSocketAddress("127.0.0.1", 9093),
            1L
        ));

        cache.maybeUpdate(2, new ConnectionCache.HostInfo(
            new InetSocketAddress("127.0.0.1", 9094),
            1L
        ));

        // One host has an inflight request
        ConnectionCache.ConnectionState connectionState1 = cache.getOrCreate(1);
        connectionState1.onRequestSent(1, time.milliseconds());
        assertFalse(connectionState1.isReady(time.milliseconds()));

        // Another is backing off
        ConnectionCache.ConnectionState connectionState2 = cache.getOrCreate(2);
        connectionState2.onRequestSent(2, time.milliseconds());
        connectionState2.onResponseError(2, time.milliseconds());
        assertFalse(connectionState2.isReady(time.milliseconds()));

        cache.resetAll();

        // Now both should be ready
        assertTrue(connectionState1.isReady(time.milliseconds()));
        assertTrue(connectionState2.isReady(time.milliseconds()));
    }

    @Test
    public void testBackoffAfterFailure() {
        List<InetSocketAddress> bootstrapServers = Collections.singletonList(
            new InetSocketAddress("127.0.0.1", 9092)
        );

        ConnectionCache cache = new ConnectionCache(networkChannel,
            bootstrapServers,
            retryBackoffMs,
            requestTimeoutMs,
            logContext);

        ConnectionCache.HostInfo hostInfo = new ConnectionCache.HostInfo(
            new InetSocketAddress("127.0.0.1", 9093),
            1L
        );

        ConnectionCache.ConnectionState connectionState = cache.getOrCreate(1);
        assertFalse(connectionState.isReady(time.milliseconds()));

        cache.maybeUpdate(1, hostInfo);
        assertTrue(connectionState.isReady(time.milliseconds()));

        long correlationId = 1;
        connectionState.onRequestSent(correlationId, time.milliseconds());
        assertFalse(connectionState.isReady(time.milliseconds()));

        connectionState.onResponseError(correlationId, time.milliseconds());
        assertFalse(connectionState.isReady(time.milliseconds()));

        time.sleep(retryBackoffMs);
        assertTrue(connectionState.isReady(time.milliseconds()));
    }

    @Test
    public void testSuccessfulResponse() {
        List<InetSocketAddress> bootstrapServers = Collections.singletonList(
            new InetSocketAddress("127.0.0.1", 9092)
        );

        ConnectionCache cache = new ConnectionCache(networkChannel,
            bootstrapServers,
            retryBackoffMs,
            requestTimeoutMs,
            logContext);

        ConnectionCache.HostInfo hostInfo = new ConnectionCache.HostInfo(
            new InetSocketAddress("127.0.0.1", 9093),
            1L
        );

        ConnectionCache.ConnectionState connectionState = cache.getOrCreate(1);
        cache.maybeUpdate(1, hostInfo);

        long correlationId = 1;
        connectionState.onRequestSent(correlationId, time.milliseconds());
        assertFalse(connectionState.isReady(time.milliseconds()));
        connectionState.onResponseReceived(correlationId);
        assertTrue(connectionState.isReady(time.milliseconds()));
    }

    @Test
    public void testIgnoreUnexpectedResponse() {
        List<InetSocketAddress> bootstrapServers = Collections.singletonList(
            new InetSocketAddress("127.0.0.1", 9092)
        );

        ConnectionCache cache = new ConnectionCache(networkChannel,
            bootstrapServers,
            retryBackoffMs,
            requestTimeoutMs,
            logContext);

        ConnectionCache.HostInfo hostInfo = new ConnectionCache.HostInfo(
            new InetSocketAddress("127.0.0.1", 9093),
            1L
        );

        ConnectionCache.ConnectionState connectionState = cache.getOrCreate(1);
        cache.maybeUpdate(1, hostInfo);

        long correlationId = 1;
        connectionState.onRequestSent(correlationId, time.milliseconds());
        assertFalse(connectionState.isReady(time.milliseconds()));
        connectionState.onResponseReceived(correlationId + 1);
        assertFalse(connectionState.isReady(time.milliseconds()));
    }

    @Test
    public void testRequestTimeout() {
        List<InetSocketAddress> bootstrapServers = Collections.singletonList(
            new InetSocketAddress("127.0.0.1", 9092)
        );

        ConnectionCache cache = new ConnectionCache(networkChannel,
            bootstrapServers,
            retryBackoffMs,
            requestTimeoutMs,
            logContext);

        ConnectionCache.HostInfo hostInfo = new ConnectionCache.HostInfo(
            new InetSocketAddress("127.0.0.1", 9093),
            1L
        );

        ConnectionCache.ConnectionState connectionState = cache.getOrCreate(1);
        cache.maybeUpdate(1, hostInfo);

        long correlationId = 1;
        connectionState.onRequestSent(correlationId, time.milliseconds());
        assertFalse(connectionState.isReady(time.milliseconds()));

        time.sleep(requestTimeoutMs - 1);
        assertFalse(connectionState.isReady(time.milliseconds()));

        time.sleep(1);
        assertTrue(connectionState.isReady(time.milliseconds()));
    }

}
