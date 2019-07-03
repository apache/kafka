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
package org.apache.kafka.common.network;

import java.nio.channels.SelectionKey;
import javax.net.ssl.SSLEngine;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.memory.SimpleMemoryPool;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.ssl.SslFactory;
import org.apache.kafka.common.security.ssl.mock.TestKeyManagerFactory;
import org.apache.kafka.common.security.ssl.mock.TestProvider;
import org.apache.kafka.common.security.ssl.mock.TestTrustManagerFactory;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestSslUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.security.Provider;
import java.security.Security;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * A set of tests for the selector. These use a test harness that runs a simple socket server that echos back responses.
 */
public class SslSelectorTest extends SelectorTest {

    private Map<String, Object> sslClientConfigs;

    @Before
    public void setUp() throws Exception {
        File trustStoreFile = File.createTempFile("truststore", ".jks");

        Map<String, Object> sslServerConfigs = TestSslUtils.createSslConfig(false, true, Mode.SERVER, trustStoreFile, "server");
        this.server = new EchoServer(SecurityProtocol.SSL, sslServerConfigs);
        this.server.start();
        this.time = new MockTime();
        sslClientConfigs = TestSslUtils.createSslConfig(false, false, Mode.CLIENT, trustStoreFile, "client");
        this.channelBuilder = new SslChannelBuilder(Mode.CLIENT, null, false);
        this.channelBuilder.configure(sslClientConfigs);
        this.metrics = new Metrics();
        this.selector = new Selector(5000, metrics, time, "MetricGroup", channelBuilder, new LogContext());
    }

    @After
    public void tearDown() throws Exception {
        this.selector.close();
        this.server.close();
        this.metrics.close();
    }

    @Override
    public SecurityProtocol securityProtocol() {
        return SecurityProtocol.PLAINTEXT;
    }

    @Test
    public void testConnectionWithCustomKeyManager() throws Exception {
        Provider provider = new TestProvider();
        Security.addProvider(provider);

        int requestSize = 100 * 1024;
        final String node = "0";
        String request = TestUtils.randomString(requestSize);

        Map<String, Object> sslServerConfigs = TestSslUtils.createSslConfig(
                TestKeyManagerFactory.ALGORITHM,
                TestTrustManagerFactory.ALGORITHM
        );
        EchoServer server = new EchoServer(SecurityProtocol.SSL, sslServerConfigs);
        server.start();
        Time time = new MockTime();
        File trustStoreFile = new File(TestKeyManagerFactory.TestKeyManager.mockTrustStoreFile);
        Map<String, Object> sslClientConfigs = TestSslUtils.createSslConfig(true, true, Mode.CLIENT, trustStoreFile, "client");

        ChannelBuilder channelBuilder = new TestSslChannelBuilder(Mode.CLIENT);
        channelBuilder.configure(sslClientConfigs);
        Metrics metrics = new Metrics();
        Selector selector = new Selector(5000, metrics, time, "MetricGroup", channelBuilder, new LogContext());

        selector.connect(node, new InetSocketAddress("localhost", server.port), BUFFER_SIZE, BUFFER_SIZE);
        while (!selector.connected().contains(node))
            selector.poll(10000L);
        while (!selector.isChannelReady(node))
            selector.poll(10000L);

        selector.send(createSend(node, request));

        waitForBytesBuffered(selector, node);

        selector.close(node);
        super.verifySelectorEmpty(selector);

        Security.removeProvider(provider.getName());
        selector.close();
        server.close();
        metrics.close();
    }

    @Test
    public void testDisconnectWithIntermediateBufferedBytes() throws Exception {
        int requestSize = 100 * 1024;
        final String node = "0";
        String request = TestUtils.randomString(requestSize);

        this.selector.close();

        this.channelBuilder = new TestSslChannelBuilder(Mode.CLIENT);
        this.channelBuilder.configure(sslClientConfigs);
        this.selector = new Selector(5000, metrics, time, "MetricGroup", channelBuilder, new LogContext());
        connect(node, new InetSocketAddress("localhost", server.port));
        selector.send(createSend(node, request));

        waitForBytesBuffered(selector, node);

        selector.close(node);
        verifySelectorEmpty();
    }

    private void waitForBytesBuffered(Selector selector, String node) throws Exception {
        TestUtils.waitForCondition(new TestCondition() {
            @Override
            public boolean conditionMet() {
                try {
                    selector.poll(0L);
                    return selector.channel(node).hasBytesBuffered();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }, 2000L, "Failed to reach socket state with bytes buffered");
    }

    @Test
    public void testBytesBufferedChannelWithNoIncomingBytes() throws Exception {
        verifyNoUnnecessaryPollWithBytesBuffered(key ->
            key.interestOps(key.interestOps() & ~SelectionKey.OP_READ));
    }

    @Test
    public void testBytesBufferedChannelAfterMute() throws Exception {
        verifyNoUnnecessaryPollWithBytesBuffered(key -> ((KafkaChannel) key.attachment()).mute());
    }

    private void verifyNoUnnecessaryPollWithBytesBuffered(Consumer<SelectionKey> disableRead)
            throws Exception {
        this.selector.close();

        String node1 = "1";
        String node2 = "2";
        final AtomicInteger node1Polls = new AtomicInteger();

        this.channelBuilder = new TestSslChannelBuilder(Mode.CLIENT);
        this.channelBuilder.configure(sslClientConfigs);
        this.selector = new Selector(5000, metrics, time, "MetricGroup", channelBuilder, new LogContext()) {
            @Override
            void pollSelectionKeys(Set<SelectionKey> selectionKeys, boolean isImmediatelyConnected, long currentTimeNanos) {
                for (SelectionKey key : selectionKeys) {
                    KafkaChannel channel = (KafkaChannel) key.attachment();
                    if (channel != null && channel.id().equals(node1))
                        node1Polls.incrementAndGet();
                }
                super.pollSelectionKeys(selectionKeys, isImmediatelyConnected, currentTimeNanos);
            }
        };

        // Get node1 into bytes buffered state and then disable read on the socket.
        // Truncate the read buffers to ensure that there is buffered data, but not enough to make progress.
        int largeRequestSize = 100 * 1024;
        connect(node1, new InetSocketAddress("localhost", server.port));
        selector.send(createSend(node1,  TestUtils.randomString(largeRequestSize)));
        waitForBytesBuffered(selector, node1);
        TestSslChannelBuilder.TestSslTransportLayer.transportLayers.get(node1).truncateReadBuffer();
        disableRead.accept(selector.channel(node1).selectionKey());

        // Clear poll count and count the polls from now on
        node1Polls.set(0);

        // Process sends and receives on node2. Test verifies that we don't process node1
        // unnecessarily on each of these polls.
        connect(node2, new InetSocketAddress("localhost", server.port));
        int received = 0;
        String request = TestUtils.randomString(10);
        selector.send(createSend(node2, request));
        while (received < 100) {
            received += selector.completedReceives().size();
            if (!selector.completedSends().isEmpty()) {
                selector.send(createSend(node2, request));
            }
            selector.poll(5);
        }

        // Verify that pollSelectionKeys was invoked once to process buffered data
        // but not again since there isn't sufficient data to process.
        assertEquals(1, node1Polls.get());
        selector.close(node1);
        selector.close(node2);
        verifySelectorEmpty();
    }

    /**
     * Renegotiation is not supported since it is potentially unsafe and it has been removed in TLS 1.3
     */
    @Test
    public void testRenegotiationFails() throws Exception {
        String node = "0";
        // create connections
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port);
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        // send echo requests and receive responses
        while (!selector.isChannelReady(node)) {
            selector.poll(1000L);
        }
        selector.send(createSend(node, node + "-" + 0));
        selector.poll(0L);
        server.renegotiate();
        selector.send(createSend(node, node + "-" + 1));
        long expiryTime = System.currentTimeMillis() + 2000;

        List<String> disconnected = new ArrayList<>();
        while (!disconnected.contains(node) && System.currentTimeMillis() < expiryTime) {
            selector.poll(10);
            disconnected.addAll(selector.disconnected().keySet());
        }
        assertTrue("Renegotiation should cause disconnection", disconnected.contains(node));

    }

    @Override
    public void testMuteOnOOM() throws Exception {
        //clean up default selector, replace it with one that uses a finite mem pool
        selector.close();
        MemoryPool pool = new SimpleMemoryPool(900, 900, false, null);
        //the initial channel builder is for clients, we need a server one
        File trustStoreFile = File.createTempFile("truststore", ".jks");
        Map<String, Object> sslServerConfigs = TestSslUtils.createSslConfig(false, true, Mode.SERVER, trustStoreFile, "server");
        channelBuilder = new SslChannelBuilder(Mode.SERVER, null, false);
        channelBuilder.configure(sslServerConfigs);
        selector = new Selector(NetworkReceive.UNLIMITED, 5000, metrics, time, "MetricGroup",
                new HashMap<String, String>(), true, false, channelBuilder, pool, new LogContext());

        try (ServerSocketChannel ss = ServerSocketChannel.open()) {
            ss.bind(new InetSocketAddress(0));

            InetSocketAddress serverAddress = (InetSocketAddress) ss.getLocalAddress();

            SslSender sender1 = createSender(serverAddress, randomPayload(900));
            SslSender sender2 = createSender(serverAddress, randomPayload(900));
            sender1.start();
            sender2.start();

            SocketChannel channelX = ss.accept(); //not defined if its 1 or 2
            channelX.configureBlocking(false);
            SocketChannel channelY = ss.accept();
            channelY.configureBlocking(false);
            selector.register("clientX", channelX);
            selector.register("clientY", channelY);

            boolean handshaked = false;
            NetworkReceive firstReceive = null;
            long deadline = System.currentTimeMillis() + 5000;
            //keep calling poll until:
            //1. both senders have completed the handshakes (so server selector has tried reading both payloads)
            //2. a single payload is actually read out completely (the other is too big to fit)
            while (System.currentTimeMillis() < deadline) {
                selector.poll(10);

                List<NetworkReceive> completed = selector.completedReceives();
                if (firstReceive == null) {
                    if (!completed.isEmpty()) {
                        assertEquals("expecting a single request", 1, completed.size());
                        firstReceive = completed.get(0);
                        assertTrue(selector.isMadeReadProgressLastPoll());
                        assertEquals(0, pool.availableMemory());
                    }
                } else {
                    assertTrue("only expecting single request", completed.isEmpty());
                }

                handshaked = sender1.waitForHandshake(1) && sender2.waitForHandshake(1);

                if (handshaked && firstReceive != null && selector.isOutOfMemory())
                    break;
            }
            assertTrue("could not initiate connections within timeout", handshaked);

            selector.poll(10);
            assertTrue(selector.completedReceives().isEmpty());
            assertEquals(0, pool.availableMemory());
            assertNotNull("First receive not complete", firstReceive);
            assertTrue("Selector not out of memory", selector.isOutOfMemory());

            firstReceive.close();
            assertEquals(900, pool.availableMemory()); //memory has been released back to pool

            List<NetworkReceive> completed = Collections.emptyList();
            deadline = System.currentTimeMillis() + 5000;
            while (System.currentTimeMillis() < deadline && completed.isEmpty()) {
                selector.poll(1000);
                completed = selector.completedReceives();
            }
            assertEquals("could not read remaining request within timeout", 1, completed.size());
            assertEquals(0, pool.availableMemory());
            assertFalse(selector.isOutOfMemory());
        }
    }

    /**
     * Connects and waits for handshake to complete. This is required since SslTransportLayer
     * implementation requires the channel to be ready before send is invoked (unlike plaintext
     * where send can be invoked straight after connect)
     */
    protected void connect(String node, InetSocketAddress serverAddr) throws IOException {
        blockingConnect(node, serverAddr);
    }

    private SslSender createSender(InetSocketAddress serverAddress, byte[] payload) {
        return new SslSender(serverAddress, payload);
    }

    private static class TestSslChannelBuilder extends SslChannelBuilder {

        public TestSslChannelBuilder(Mode mode) {
            super(mode, null, false);
        }

        @Override
        protected SslTransportLayer buildTransportLayer(SslFactory sslFactory, String id, SelectionKey key, String host) throws IOException {
            SocketChannel socketChannel = (SocketChannel) key.channel();
            SSLEngine sslEngine = sslFactory.createSslEngine(host, socketChannel.socket().getPort());
            TestSslTransportLayer transportLayer = new TestSslTransportLayer(id, key, sslEngine);
            return transportLayer;
        }

        /*
         * TestSslTransportLayer will read from socket once every two tries. This increases
         * the chance that there will be bytes buffered in the transport layer after read().
         */
        static class TestSslTransportLayer extends SslTransportLayer {
            static Map<String, TestSslTransportLayer> transportLayers = new HashMap<>();
            boolean muteSocket = false;

            public TestSslTransportLayer(String channelId, SelectionKey key, SSLEngine sslEngine) throws IOException {
                super(channelId, key, sslEngine);
                transportLayers.put(channelId, this);
            }

            @Override
            protected int readFromSocketChannel() throws IOException {
                if (muteSocket) {
                    if ((selectionKey().interestOps() & SelectionKey.OP_READ) != 0)
                        muteSocket = false;
                    return 0;
                }
                muteSocket = true;
                return super.readFromSocketChannel();
            }

            // Leave one byte in network read buffer so that some buffered bytes are present,
            // but not enough to make progress on a read.
            void truncateReadBuffer() throws Exception {
                netReadBuffer().position(1);
                appReadBuffer().position(0);
                muteSocket = true;
            }
        }
    }

}
