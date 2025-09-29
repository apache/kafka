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

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.memory.SimpleMemoryPool;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.MockedConstruction;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * A set of tests for the selector. These use a test harness that runs a simple socket server that echos back responses.
 */
@Timeout(240)
public class SelectorTest {
    protected static final int BUFFER_SIZE = 4 * 1024;
    private static final String METRIC_GROUP = "MetricGroup";
    private static final long CONNECTION_MAX_IDLE_MS = 5_000;

    protected EchoServer server;
    protected Time time;
    protected Selector selector;
    protected ChannelBuilder channelBuilder;
    protected Metrics metrics;

    @BeforeEach
    public void setUp() throws Exception {
        Map<String, Object> configs = new HashMap<>();
        this.server = new EchoServer(SecurityProtocol.PLAINTEXT, configs);
        this.server.start();
        this.time = new MockTime();
        this.channelBuilder = new PlaintextChannelBuilder(ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT));
        this.channelBuilder.configure(clientConfigs());
        this.metrics = new Metrics();
        this.selector = new Selector(CONNECTION_MAX_IDLE_MS, this.metrics, time, METRIC_GROUP, channelBuilder, new LogContext());
    }

    @AfterEach
    public void tearDown() throws Exception {
        try {
            verifySelectorEmpty();
        } finally {
            this.selector.close();
            this.server.close();
            this.metrics.close();
        }
    }

    protected Map<String, Object> clientConfigs() {
        return new HashMap<>();
    }

    /**
     * Validate that when the server disconnects, a client send ends up with that node in the disconnected list.
     */
    @Test
    public void testServerDisconnect() throws Exception {
        final String node = "0";

        // connect and do a simple request
        blockingConnect(node);
        assertEquals("hello", blockingRequest(node, "hello"));

        KafkaChannel channel = selector.channel(node);

        // disconnect
        this.server.closeConnections();
        waitForCondition(() -> {
            try {
                selector.poll(1000L);
                return selector.disconnected().containsKey(node);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, 5000, "Failed to observe disconnected node in disconnected set");

        assertNull(channel.selectionKey().attachment());

        // reconnect and do another request
        blockingConnect(node);
        assertEquals("hello", blockingRequest(node, "hello"));
    }

    /**
     * Sending a request with one already in flight should result in an exception
     */
    @Test
    public void testCantSendWithInProgress() throws Exception {
        String node = "0";
        blockingConnect(node);
        selector.send(createSend(node, "test1"));
        try {
            selector.send(createSend(node, "test2"));
            fail("IllegalStateException not thrown when sending a request with one in flight");
        } catch (IllegalStateException e) {
            // Expected exception
        }
        selector.poll(0);
        assertTrue(selector.disconnected().containsKey(node), "Channel not closed");
        assertEquals(ChannelState.FAILED_SEND, selector.disconnected().get(node));
    }

    /**
     * Sending a request to a node without an existing connection should result in an exception
     */
    @Test
    public void testSendWithoutConnecting() {
        assertThrows(IllegalStateException.class, () -> selector.send(createSend("0", "test")));
    }

    /**
     * Sending a request to a node with a bad hostname should result in an exception during connect
     */
    @Test
    public void testNoRouteToHost() {
        assertThrows(IOException.class,
            () -> selector.connect("0", new InetSocketAddress("some.invalid.hostname.foo.bar.local", server.port), BUFFER_SIZE, BUFFER_SIZE));
    }

    /**
     * Sending a request to a node not listening on that port should result in disconnection
     */
    @Test
    public void testConnectionRefused() throws Exception {
        String node = "0";
        ServerSocket nonListeningSocket = new ServerSocket(0);
        int nonListeningPort = nonListeningSocket.getLocalPort();
        selector.connect(node, new InetSocketAddress("localhost", nonListeningPort), BUFFER_SIZE, BUFFER_SIZE);
        while (selector.disconnected().containsKey(node)) {
            assertEquals(ChannelState.NOT_CONNECTED, selector.disconnected().get(node));
            selector.poll(1000L);
        }
        nonListeningSocket.close();
    }

    /**
     * Send multiple requests to several connections in parallel. Validate that responses are received in the order that
     * requests were sent.
     */
    @Test
    public void testNormalOperation() throws Exception {
        int conns = 5;
        int reqs = 500;

        // create connections
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port);
        for (int i = 0; i < conns; i++)
            connect(Integer.toString(i), addr);
        // send echo requests and receive responses
        Map<String, Integer> requests = new HashMap<>();
        Map<String, Integer> responses = new HashMap<>();
        int responseCount = 0;
        for (int i = 0; i < conns; i++) {
            String node = Integer.toString(i);
            selector.send(createSend(node, node + "-0"));
        }

        // loop until we complete all requests
        while (responseCount < conns * reqs) {
            // do the i/o
            selector.poll(0L);

            assertEquals(0, selector.disconnected().size(), "No disconnects should have occurred.");

            // handle any responses we may have gotten
            for (NetworkReceive receive : selector.completedReceives()) {
                String[] pieces = asString(receive).split("-");
                assertEquals(2, pieces.length, "Should be in the form 'conn-counter'");
                assertEquals(receive.source(), pieces[0], "Check the source");
                assertEquals(0, receive.payload().position(), "Check that the receive has kindly been rewound");
                if (responses.containsKey(receive.source())) {
                    assertEquals((int) responses.get(receive.source()), Integer.parseInt(pieces[1]), "Check the request counter");
                    responses.put(receive.source(), responses.get(receive.source()) + 1);
                } else {
                    assertEquals(0, Integer.parseInt(pieces[1]), "Check the request counter");
                    responses.put(receive.source(), 1);
                }
                responseCount++;
            }

            // prepare new sends for the next round
            for (NetworkSend send : selector.completedSends()) {
                String dest = send.destinationId();
                if (requests.containsKey(dest))
                    requests.put(dest, requests.get(dest) + 1);
                else
                    requests.put(dest, 1);
                if (requests.get(dest) < reqs)
                    selector.send(createSend(dest, dest + "-" + requests.get(dest)));
            }
        }
        if (channelBuilder instanceof PlaintextChannelBuilder) {
            assertEquals(0, cipherMetrics(metrics).size());
        } else {
            waitForCondition(() -> cipherMetrics(metrics).size() == 1,
                "Waiting for cipher metrics to be created.");
            assertEquals(5, cipherMetrics(metrics).get(0).metricValue());
        }
    }

    static List<KafkaMetric> cipherMetrics(Metrics metrics) {
        return metrics.metrics().entrySet().stream().
            filter(e -> e.getKey().description().
                contains("The number of connections with this SSL cipher and protocol.")).
            map(Map.Entry::getValue).
            collect(Collectors.toList());
    }

    /**
     * Validate that we can send and receive a message larger than the receive and send buffer size
     */
    @Test
    public void testSendLargeRequest() throws Exception {
        String node = "0";
        blockingConnect(node);
        String big = TestUtils.randomString(10 * BUFFER_SIZE);
        assertEquals(big, blockingRequest(node, big));
    }

    @Test
    public void testPartialSendAndReceiveReflectedInMetrics() throws Exception {
        // We use a large payload to attempt to trigger the partial send and receive logic.
        int payloadSize = 20 * BUFFER_SIZE;
        String payload = TestUtils.randomString(payloadSize);
        String nodeId = "0";
        blockingConnect(nodeId);
        ByteBufferSend send = ByteBufferSend.sizePrefixed(ByteBuffer.wrap(payload.getBytes()));
        NetworkSend networkSend = new NetworkSend(nodeId, send);

        selector.send(networkSend);
        KafkaChannel channel = selector.channel(nodeId);

        KafkaMetric outgoingByteTotal = findUntaggedMetricByName("outgoing-byte-total");
        KafkaMetric incomingByteTotal = findUntaggedMetricByName("incoming-byte-total");

        waitForCondition(() -> {
            long bytesSent = send.size() - send.remaining();
            assertEquals(bytesSent, ((Double) outgoingByteTotal.metricValue()).longValue());

            NetworkReceive currentReceive = channel.currentReceive();
            if (currentReceive != null) {
                assertEquals(currentReceive.bytesRead(), ((Double) incomingByteTotal.metricValue()).intValue());
            }

            selector.poll(50);
            return !selector.completedReceives().isEmpty();
        }, "Failed to receive expected response");

        KafkaMetric requestTotal = findUntaggedMetricByName("request-total");
        assertEquals(1, ((Double) requestTotal.metricValue()).intValue());

        KafkaMetric responseTotal = findUntaggedMetricByName("response-total");
        assertEquals(1, ((Double) responseTotal.metricValue()).intValue());
    }

    @Test
    public void testLargeMessageSequence() throws Exception {
        int bufferSize = 512 * 1024;
        String node = "0";
        int reqs = 50;
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port);
        connect(node, addr);
        String requestPrefix = TestUtils.randomString(bufferSize);
        sendAndReceive(node, requestPrefix, 0, reqs);
    }

    @Test
    public void testEmptyRequest() throws Exception {
        String node = "0";
        blockingConnect(node);
        assertEquals("", blockingRequest(node, ""));
    }

    @Test
    public void testClearCompletedSendsAndReceives() throws Exception {
        int bufferSize = 1024;
        String node = "0";
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port);
        connect(node, addr);
        String request = TestUtils.randomString(bufferSize);
        selector.send(createSend(node, request));
        boolean sent = false;
        boolean received = false;
        while (!sent || !received) {
            selector.poll(1000L);
            assertEquals(0, selector.disconnected().size(), "No disconnects should have occurred.");
            if (!selector.completedSends().isEmpty()) {
                assertEquals(1, selector.completedSends().size());
                selector.clearCompletedSends();
                assertEquals(0, selector.completedSends().size());
                sent = true;
            }

            if (!selector.completedReceives().isEmpty()) {
                assertEquals(1, selector.completedReceives().size());
                assertEquals(request, asString(selector.completedReceives().iterator().next()));
                selector.clearCompletedReceives();
                assertEquals(0, selector.completedReceives().size());
                received = true;
            }
        }
    }

    @Test
    public void testExistingConnectionId() throws IOException {
        blockingConnect("0");
        assertThrows(IllegalStateException.class, () -> blockingConnect("0"));
    }

    @Test
    public void testMute() throws Exception {
        blockingConnect("0");
        blockingConnect("1");

        selector.send(createSend("0", "hello"));
        selector.send(createSend("1", "hi"));

        selector.mute("1");

        while (selector.completedReceives().isEmpty())
            selector.poll(5);
        assertEquals(1, selector.completedReceives().size(), "We should have only one response");
        assertEquals("0", selector.completedReceives().iterator().next().source(),
            "The response should not be from the muted node");

        selector.unmute("1");
        do {
            selector.poll(5);
        } while (selector.completedReceives().isEmpty());
        assertEquals(1, selector.completedReceives().size(), "We should have only one response");
        assertEquals("1", selector.completedReceives().iterator().next().source(), "The response should be from the previously muted node");
    }

    @Test
    public void testCloseAllChannels() throws Exception {
        AtomicInteger closedChannelsCount = new AtomicInteger(0);
        ChannelBuilder channelBuilder = new PlaintextChannelBuilder(null) {
            private int channelIndex = 0;
            @Override
            KafkaChannel buildChannel(String id, TransportLayer transportLayer, Supplier<Authenticator> authenticatorCreator,
                                      int maxReceiveSize, MemoryPool memoryPool, ChannelMetadataRegistry metadataRegistry) {
                return new KafkaChannel(id, transportLayer, authenticatorCreator, maxReceiveSize, memoryPool, metadataRegistry) {
                    private final int index = channelIndex++;
                    @Override
                    public void close() throws IOException {
                        closedChannelsCount.getAndIncrement();
                        super.close();
                        if (index == 0) throw new RuntimeException("you should fail");
                    }
                };
            }
        };
        channelBuilder.configure(clientConfigs());
        Selector selector = new Selector(CONNECTION_MAX_IDLE_MS, new Metrics(), new MockTime(), "MetricGroup", channelBuilder, new LogContext());
        selector.connect("0", new InetSocketAddress("localhost", server.port), BUFFER_SIZE, BUFFER_SIZE);
        selector.connect("1", new InetSocketAddress("localhost", server.port), BUFFER_SIZE, BUFFER_SIZE);
        assertThrows(RuntimeException.class, selector::close);
        assertEquals(2, closedChannelsCount.get());
    }

    @Test
    public void registerFailure() throws Exception {
        final String channelId = "1";

        final ChannelBuilder channelBuilder = mock(ChannelBuilder.class);

        when(channelBuilder.buildChannel(eq(channelId), any(SelectionKey.class), anyInt(), any(MemoryPool.class),
                any(ChannelMetadataRegistry.class))).thenThrow(new RuntimeException("Test exception"));

        try (MockedConstruction<Selector.SelectorChannelMetadataRegistry> mockedMetadataRegistry =
                     mockConstruction(Selector.SelectorChannelMetadataRegistry.class)) {
            Selector selector = new Selector(CONNECTION_MAX_IDLE_MS, new Metrics(), new MockTime(), "MetricGroup", channelBuilder, new LogContext());
            final SocketChannel socketChannel = SocketChannel.open();
            socketChannel.configureBlocking(false);
            IOException e = assertThrows(IOException.class, () -> selector.register(channelId, socketChannel));
            assertTrue(e.getCause().getMessage().contains("Test exception"), "Unexpected exception: " + e);
            assertFalse(socketChannel.isOpen(), "Socket not closed");
            // Ideally, metadataRegistry is closed by the KafkaChannel but if the KafkaChannel is not created due to
            // an error such as in a case like this, the Selector should be closing the metadataRegistry instead.
            verify(mockedMetadataRegistry.constructed().get(0)).close();
            selector.close();
        }
    }

    @Test
    public void testCloseOldestConnection() throws Exception {
        String id = "0";
        selector.connect(id, new InetSocketAddress("localhost", server.port), BUFFER_SIZE, BUFFER_SIZE);
        NetworkTestUtils.waitForChannelConnected(selector, id);
        time.sleep(CONNECTION_MAX_IDLE_MS + 1_000);
        selector.poll(0);

        assertTrue(selector.disconnected().containsKey(id), "The idle connection should have been closed");
        assertEquals(ChannelState.EXPIRED, selector.disconnected().get(id));
    }

    @Test
    public void testIdleExpiryWithoutReadyKeys() throws IOException {
        String id = "0";
        selector.connect(id, new InetSocketAddress("localhost", server.port), BUFFER_SIZE, BUFFER_SIZE);
        KafkaChannel channel = selector.channel(id);
        channel.selectionKey().interestOps(0);

        time.sleep(CONNECTION_MAX_IDLE_MS + 1_000);
        selector.poll(0);
        assertTrue(selector.disconnected().containsKey(id), "The idle connection should have been closed");
        assertEquals(ChannelState.EXPIRED, selector.disconnected().get(id));
    }

    @Test
    public void testImmediatelyConnectedCleaned() throws Exception {
        Metrics metrics = new Metrics(); // new metrics object to avoid metric registration conflicts
        Selector selector = new ImmediatelyConnectingSelector(CONNECTION_MAX_IDLE_MS, metrics, time, "MetricGroup", channelBuilder, new LogContext());

        try {
            testImmediatelyConnectedCleaned(selector, true);
            testImmediatelyConnectedCleaned(selector, false);
        } finally {
            selector.close();
            metrics.close();
        }
    }

    private static class ImmediatelyConnectingSelector extends Selector {
        public ImmediatelyConnectingSelector(long connectionMaxIdleMS,
                                             Metrics metrics,
                                             Time time,
                                             String metricGrpPrefix,
                                             ChannelBuilder channelBuilder,
                                             LogContext logContext) {
            super(connectionMaxIdleMS, metrics, time, metricGrpPrefix, channelBuilder, logContext);
        }

        @Override
        protected boolean doConnect(SocketChannel channel, InetSocketAddress address) throws IOException {
            // Use a blocking connect to trigger the immediately connected path
            channel.configureBlocking(true);
            boolean connected = super.doConnect(channel, address);
            channel.configureBlocking(false);
            return connected;
        }
    }

    private void testImmediatelyConnectedCleaned(Selector selector, boolean closeAfterFirstPoll) throws Exception {
        String id = "0";
        selector.connect(id, new InetSocketAddress("localhost", server.port), BUFFER_SIZE, BUFFER_SIZE);
        verifyNonEmptyImmediatelyConnectedKeys(selector);
        if (closeAfterFirstPoll) {
            selector.poll(0);
            verifyEmptyImmediatelyConnectedKeys(selector);
        }
        selector.close(id);
        verifySelectorEmpty(selector);
    }

    /**
     * Verify that if Selector#connect fails and throws an Exception, all related objects
     * are cleared immediately before the exception is propagated.
     */
    @Test
    public void testConnectException() throws Exception {
        Metrics metrics = new Metrics();
        AtomicBoolean throwIOException = new AtomicBoolean();
        Selector selector = new ImmediatelyConnectingSelector(CONNECTION_MAX_IDLE_MS, metrics, time, "MetricGroup", channelBuilder, new LogContext()) {
            @Override
            protected SelectionKey registerChannel(String id, SocketChannel socketChannel, int interestedOps) throws IOException {
                SelectionKey key = super.registerChannel(id, socketChannel, interestedOps);
                key.cancel();
                if (throwIOException.get())
                    throw new IOException("Test exception");
                return key;
            }
        };

        try {
            verifyImmediatelyConnectedException(selector, "0");
            throwIOException.set(true);
            verifyImmediatelyConnectedException(selector, "1");
        } finally {
            selector.close();
            metrics.close();
        }
    }

    private void verifyImmediatelyConnectedException(Selector selector, String id) throws Exception {
        try {
            selector.connect(id, new InetSocketAddress("localhost", server.port), BUFFER_SIZE, BUFFER_SIZE);
            fail("Expected exception not thrown");
        } catch (Exception e) {
            verifyEmptyImmediatelyConnectedKeys(selector);
            assertNull(selector.channel(id), "Channel not removed");
            ensureEmptySelectorFields(selector);
        }
    }

    /*
     * Verifies that a muted connection is expired on idle timeout even if there are pending
     * receives on the socket.
     */
    @Test
    public void testExpireConnectionWithPendingReceives() throws Exception {
        KafkaChannel channel = createConnectionWithPendingReceives(5);
        verifyChannelExpiry(channel);
    }

    /**
     * Verifies that a muted connection closed by peer is expired on idle timeout even if there are pending
     * receives on the socket.
     */
    @Test
    public void testExpireClosedConnectionWithPendingReceives() throws Exception {
        KafkaChannel channel = createConnectionWithPendingReceives(5);
        server.closeConnections();
        verifyChannelExpiry(channel);
    }

    private void verifyChannelExpiry(KafkaChannel channel) throws Exception {
        String id = channel.id();
        selector.mute(id); // Mute to allow channel to be expired even if more data is available for read
        time.sleep(CONNECTION_MAX_IDLE_MS + 1_000);
        selector.poll(0);
        assertNull(selector.channel(id), "Channel not expired");
        assertNull(selector.closingChannel(id), "Channel not removed from closingChannels");
        assertEquals(ChannelState.EXPIRED, channel.state());
        assertNull(channel.selectionKey().attachment());
        assertTrue(selector.disconnected().containsKey(id), "Disconnect not notified");
        assertEquals(ChannelState.EXPIRED, selector.disconnected().get(id));
        verifySelectorEmpty();
    }

    /**
     * Verifies that sockets with incoming data available are not expired.
     * For PLAINTEXT, pending receives are always read from socket without any buffering, so this
     * test is only verifying that channels are not expired while there is data to read from socket.
     * For SSL, pending receives may also be in SSL netReadBuffer or appReadBuffer. So the test verifies
     * that connection is not expired when data is available from buffers or network.
     */
    @Test
    public void testCloseOldestConnectionWithMultiplePendingReceives() throws Exception {
        int expectedReceives = 5;
        KafkaChannel channel = createConnectionWithPendingReceives(expectedReceives);
        int completedReceives = selector.completedReceives().size();

        while (selector.disconnected().isEmpty()) {
            time.sleep(CONNECTION_MAX_IDLE_MS + 1_000);
            selector.poll(completedReceives == expectedReceives ? 0 : 1_000);
            completedReceives += selector.completedReceives().size();
        }

        assertEquals(expectedReceives, completedReceives);
        assertNull(selector.channel(channel.id()), "Channel not expired");
        assertNull(selector.closingChannel(channel.id()), "Channel not expired");
        assertTrue(selector.disconnected().containsKey(channel.id()), "Disconnect not notified");
        assertTrue(selector.completedReceives().isEmpty(), "Unexpected receive");
    }

    /**
     * Tests that graceful close of channel processes remaining data from socket read buffers.
     * Since we cannot determine how much data is available in the buffers, this test verifies that
     * multiple receives are completed after server shuts down connections, with retries to tolerate
     * cases where data may not be available in the socket buffer.
     */
    @Test
    public void testGracefulClose() throws Exception {
        int maxReceiveCountAfterClose = 0;
        for (int i = 6; i <= 100 && maxReceiveCountAfterClose < 5; i++) {
            int receiveCount = 0;
            KafkaChannel channel = createConnectionWithPendingReceives(i);
            // Poll until one or more receives complete and then close the server-side connection
            waitForCondition(() -> {
                selector.poll(1000);
                return !selector.completedReceives().isEmpty();
            }, 5000, "Receive not completed");
            server.closeConnections();
            while (selector.disconnected().isEmpty()) {
                selector.poll(1);
                receiveCount += selector.completedReceives().size();
                assertTrue(selector.completedReceives().size() <= 1, "Too many completed receives in one poll");
            }
            assertEquals(channel.id(), selector.disconnected().keySet().iterator().next());
            maxReceiveCountAfterClose = Math.max(maxReceiveCountAfterClose, receiveCount);
        }
        assertTrue(maxReceiveCountAfterClose >= 5, "Too few receives after close: " + maxReceiveCountAfterClose);
    }

    /**
     * Tests that graceful close is not delayed if only part of an incoming receive is
     * available in the socket buffer.
     */
    @Test
    public void testPartialReceiveGracefulClose() throws Exception {
        String id = "0";
        blockingConnect(id);
        KafkaChannel channel = selector.channel(id);
        // Inject a NetworkReceive into Kafka channel with a large size
        injectNetworkReceive(channel, 100000);
        sendNoReceive(channel, 2); // Send some data that gets received as part of injected receive
        selector.poll(1000); // Wait until some data arrives, but not a completed receive
        assertEquals(0, selector.completedReceives().size());
        server.closeConnections();
        waitForCondition(() -> {
            try {
                selector.poll(100);
                return !selector.disconnected().isEmpty();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, 10000, "Channel not disconnected");
        assertEquals(1, selector.disconnected().size());
        assertEquals(channel.id(), selector.disconnected().keySet().iterator().next());
        assertEquals(0, selector.completedReceives().size());
    }

    @Test
    public void testMuteOnOOM() throws Exception {
        //clean up default selector, replace it with one that uses a finite mem pool
        selector.close();
        MemoryPool pool = new SimpleMemoryPool(900, 900, false, null);
        selector = new Selector(NetworkReceive.UNLIMITED, CONNECTION_MAX_IDLE_MS, metrics, time, "MetricGroup",
            new HashMap<>(), true, false, channelBuilder, pool, new LogContext());

        try (ServerSocketChannel ss = ServerSocketChannel.open()) {
            ss.bind(new InetSocketAddress(0));

            InetSocketAddress serverAddress = (InetSocketAddress) ss.getLocalAddress();

            Thread sender1 = createSender(serverAddress, randomPayload(900));
            Thread sender2 = createSender(serverAddress, randomPayload(900));
            sender1.start();
            sender2.start();

            //wait until everything has been flushed out to network (assuming payload size is smaller than OS buffer size)
            //this is important because we assume both requests' prefixes (1st 4 bytes) have made it.
            sender1.join(5000);
            sender2.join(5000);

            SocketChannel channelX = ss.accept(); //not defined if its 1 or 2
            channelX.configureBlocking(false);
            SocketChannel channelY = ss.accept();
            channelY.configureBlocking(false);
            selector.register("clientX", channelX);
            selector.register("clientY", channelY);

            Collection<NetworkReceive> completed = Collections.emptyList();
            long deadline = System.currentTimeMillis() + 5000;
            while (System.currentTimeMillis() < deadline && completed.isEmpty()) {
                selector.poll(1000);
                completed = selector.completedReceives();
            }
            assertEquals(1, completed.size(), "could not read a single request within timeout");
            NetworkReceive firstReceive = completed.iterator().next();
            assertEquals(0, pool.availableMemory());
            assertTrue(selector.isOutOfMemory());

            selector.poll(10);
            assertTrue(selector.completedReceives().isEmpty());
            assertEquals(0, pool.availableMemory());
            assertTrue(selector.isOutOfMemory());

            firstReceive.close();
            assertEquals(900, pool.availableMemory()); //memory has been released back to pool

            completed = Collections.emptyList();
            deadline = System.currentTimeMillis() + 5000;
            while (System.currentTimeMillis() < deadline && completed.isEmpty()) {
                selector.poll(1000);
                completed = selector.completedReceives();
            }
            assertEquals(1, selector.completedReceives().size(), "could not read a single request within timeout");
            assertEquals(0, pool.availableMemory());
            assertFalse(selector.isOutOfMemory());
        }
    }

    private Thread createSender(InetSocketAddress serverAddress, byte[] payload) {
        return new PlaintextSender(serverAddress, payload);
    }

    protected byte[] randomPayload(int sizeBytes) throws Exception {
        Random random = new Random();
        byte[] payload = new byte[sizeBytes + 4];
        random.nextBytes(payload);
        ByteArrayOutputStream prefixOs = new ByteArrayOutputStream();
        DataOutputStream prefixDos = new DataOutputStream(prefixOs);
        prefixDos.writeInt(sizeBytes);
        prefixDos.flush();
        prefixDos.close();
        prefixOs.flush();
        prefixOs.close();
        byte[] prefix = prefixOs.toByteArray();
        System.arraycopy(prefix, 0, payload, 0, prefix.length);
        return payload;
    }

    /**
     * Tests that a connect and disconnect in a single poll invocation results in the channel id being
     * in `disconnected`, but not `connected`.
     */
    @Test
    public void testConnectDisconnectDuringInSinglePoll() throws Exception {
        // channel is connected, not ready and it throws an exception during prepare
        KafkaChannel kafkaChannel = mock(KafkaChannel.class);
        when(kafkaChannel.id()).thenReturn("1");
        when(kafkaChannel.socketDescription()).thenReturn("");
        when(kafkaChannel.state()).thenReturn(ChannelState.NOT_CONNECTED);
        when(kafkaChannel.finishConnect()).thenReturn(true);
        when(kafkaChannel.isConnected()).thenReturn(true);
        when(kafkaChannel.ready()).thenReturn(false);
        doThrow(new IOException()).when(kafkaChannel).prepare();

        SelectionKey selectionKey = mock(SelectionKey.class);
        when(kafkaChannel.selectionKey()).thenReturn(selectionKey);
        SocketChannel socket = SocketChannel.open();
        when(selectionKey.channel()).thenReturn(socket);
        when(selectionKey.readyOps()).thenReturn(SelectionKey.OP_CONNECT);
        when(selectionKey.attachment()).thenReturn(kafkaChannel);

        Set<SelectionKey> selectionKeys = Set.of(selectionKey);
        selector.pollSelectionKeys(selectionKeys, false, System.nanoTime());

        assertFalse(selector.connected().contains(kafkaChannel.id()));
        assertTrue(selector.disconnected().containsKey(kafkaChannel.id()));

        verify(kafkaChannel, atLeastOnce()).ready();
        verify(kafkaChannel).disconnect();
        verify(kafkaChannel).close();
        verify(selectionKey).cancel();
        socket.close();
    }

    @Test
    public void testOutboundConnectionsCountInConnectionCreationMetric() throws Exception {
        // create connections
        int expectedConnections = 5;
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port);
        for (int i = 0; i < expectedConnections; i++)
            connect(Integer.toString(i), addr);

        // Poll continuously, as we cannot guarantee that the first call will see all connections
        int seenConnections = 0;
        for (int i = 0; i < 10; i++) {
            selector.poll(100L);
            seenConnections += selector.connected().size();
            if (seenConnections == expectedConnections)
                break;
        }

        assertEquals((double) expectedConnections, getMetric("connection-creation-total").metricValue());
        assertEquals((double) expectedConnections, getMetric("connection-count").metricValue());
    }

    @Test
    public void testInboundConnectionsCountInConnectionCreationMetric() throws Exception {
        int conns = 5;

        try (ServerSocketChannel ss = ServerSocketChannel.open()) {
            ss.bind(new InetSocketAddress(0));
            InetSocketAddress serverAddress = (InetSocketAddress) ss.getLocalAddress();

            for (int i = 0; i < conns; i++) {
                Thread sender = createSender(serverAddress, randomPayload(1));
                sender.start();
                try (SocketChannel channel = ss.accept()) {
                    channel.configureBlocking(false);
                    selector.register(Integer.toString(i), channel);
                } finally {
                    sender.join();
                }
            }
        }

        assertEquals((double) conns, getMetric("connection-creation-total").metricValue());
        assertEquals((double) conns, getMetric("connection-count").metricValue());
    }

    @Test
    public void testConnectionsByClientMetric() throws Exception {
        String node = "0";
        Map<String, String> unknownNameAndVersion = softwareNameAndVersionTags(
            ClientInformation.UNKNOWN_NAME_OR_VERSION, ClientInformation.UNKNOWN_NAME_OR_VERSION);
        Map<String, String> knownNameAndVersion = softwareNameAndVersionTags("A", "B");

        try (ServerSocketChannel ss = ServerSocketChannel.open()) {
            ss.bind(new InetSocketAddress(0));
            InetSocketAddress serverAddress = (InetSocketAddress) ss.getLocalAddress();

            Thread sender = createSender(serverAddress, randomPayload(1));
            sender.start();
            SocketChannel channel = ss.accept();
            channel.configureBlocking(false);

            // Metric with unknown / unknown should be there
            selector.register(node, channel);
            assertEquals(1,
                getMetric("connections", unknownNameAndVersion).metricValue());
            assertEquals(ClientInformation.EMPTY,
                selector.channel(node).channelMetadataRegistry().clientInformation());

            // Metric with unknown / unknown should not be there, metric with A / B should be there
            ClientInformation clientInformation = new ClientInformation("A", "B");
            selector.channel(node).channelMetadataRegistry()
                .registerClientInformation(clientInformation);
            assertEquals(clientInformation,
                selector.channel(node).channelMetadataRegistry().clientInformation());
            assertEquals(0, getMetric("connections", unknownNameAndVersion).metricValue());
            assertEquals(1, getMetric("connections", knownNameAndVersion).metricValue());

            // Metric with A / B should not be there,
            selector.close(node);
            assertEquals(0, getMetric("connections", knownNameAndVersion).metricValue());
        }
    }

    private Map<String, String> softwareNameAndVersionTags(String clientSoftwareName, String clientSoftwareVersion) {
        Map<String, String> tags = new HashMap<>(2);
        tags.put("clientSoftwareName", clientSoftwareName);
        tags.put("clientSoftwareVersion", clientSoftwareVersion);
        return tags;
    }

    private KafkaMetric getMetric(String name, Map<String, String> tags) throws Exception {
        Optional<Map.Entry<MetricName, KafkaMetric>> metric = metrics.metrics().entrySet().stream()
            .filter(entry ->
                entry.getKey().name().equals(name) && entry.getKey().tags().equals(tags))
            .findFirst();
        if (!metric.isPresent())
            throw new Exception(String.format("Could not find metric called %s with tags %s", name, tags.toString()));

        return metric.get().getValue();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testLowestPriorityChannel() throws Exception {
        int conns = 5;
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port);
        for (int i = 0; i < conns; i++) {
            connect(String.valueOf(i), addr);
        }
        assertNotNull(selector.lowestPriorityChannel());
        for (int i = conns - 1; i >= 0; i--) {
            if (i != 2) 
                assertEquals("", blockingRequest(String.valueOf(i), ""));
            time.sleep(10);
        }
        assertEquals("2", selector.lowestPriorityChannel().id());

        Field field = Selector.class.getDeclaredField("closingChannels");
        field.setAccessible(true);
        Map<String, KafkaChannel> closingChannels = (Map<String, KafkaChannel>) field.get(selector);
        closingChannels.put("3", selector.channel("3"));
        assertEquals("3", selector.lowestPriorityChannel().id());
        closingChannels.remove("3");

        for (int i = 0; i < conns; i++) {
            selector.close(String.valueOf(i));
        }
        assertNull(selector.lowestPriorityChannel());
    }

    @Test
    public void testMetricsCleanupOnSelectorClose() throws Exception {
        Metrics metrics = new Metrics();
        Selector selector = new ImmediatelyConnectingSelector(CONNECTION_MAX_IDLE_MS, metrics, time, "MetricGroup", channelBuilder, new LogContext()) {
            @Override
            public void close(String id) {
                super.close(id);
                throw new RuntimeException();
            }
        };
        assertTrue(metrics.metrics().size() > 1);
        String id = "0";
        selector.connect(id, new InetSocketAddress("localhost", server.port), BUFFER_SIZE, BUFFER_SIZE);

        // Close the selector and ensure a RuntimeException has been throw
        assertThrows(RuntimeException.class, selector::close);

        // We should only have one remaining metric for kafka-metrics-count, which is a global metric
        assertEquals(1, metrics.metrics().size());
    }

    @Test
    public void testWriteCompletesSendWithNoBytesWritten() throws IOException {
        KafkaChannel channel = mock(KafkaChannel.class);
        when(channel.id()).thenReturn("1");
        when(channel.write()).thenReturn(0L);
        NetworkSend send = new NetworkSend("destination", new ByteBufferSend(ByteBuffer.allocate(0)));
        when(channel.maybeCompleteSend()).thenReturn(send);
        selector.write(channel);
        assertEquals(Collections.singletonList(send), selector.completedSends());
    }

    /**
     * Ensure that no errors are thrown if channels are closed while processing multiple completed receives
     */
    @Test
    public void testChannelCloseWhileProcessingReceives() throws Exception {
        int numChannels = 4;
        Map<String, KafkaChannel> channels = TestUtils.fieldValue(selector, Selector.class, "channels");
        Set<SelectionKey> selectionKeys = new HashSet<>();
        for (int i = 0; i < numChannels; i++) {
            String id = String.valueOf(i);
            KafkaChannel channel = mock(KafkaChannel.class);
            channels.put(id, channel);
            when(channel.id()).thenReturn(id);
            when(channel.state()).thenReturn(ChannelState.READY);
            when(channel.isConnected()).thenReturn(true);
            when(channel.ready()).thenReturn(true);
            when(channel.read()).thenReturn(1L);

            SelectionKey selectionKey = mock(SelectionKey.class);
            when(channel.selectionKey()).thenReturn(selectionKey);
            when(selectionKey.isValid()).thenReturn(true);
            when(selectionKey.isReadable()).thenReturn(true);
            when(selectionKey.readyOps()).thenReturn(SelectionKey.OP_READ);
            when(selectionKey.attachment())
                    .thenReturn(channel)
                    .thenReturn(null);
            selectionKeys.add(selectionKey);

            NetworkReceive receive = mock(NetworkReceive.class);
            when(receive.source()).thenReturn(id);
            when(receive.size()).thenReturn(10);
            when(receive.bytesRead()).thenReturn(1);
            when(receive.payload()).thenReturn(ByteBuffer.allocate(10));
            when(channel.maybeCompleteReceive()).thenReturn(receive);
        }

        selector.pollSelectionKeys(selectionKeys, false, System.nanoTime());
        assertEquals(numChannels, selector.completedReceives().size());
        Set<KafkaChannel> closed = new HashSet<>();
        Set<KafkaChannel> notClosed = new HashSet<>();
        for (NetworkReceive receive : selector.completedReceives()) {
            KafkaChannel channel = selector.channel(receive.source());
            assertNotNull(channel);
            if (closed.size() < 2) {
                selector.close(channel.id());
                closed.add(channel);
            } else
                notClosed.add(channel);
        }
        assertEquals(notClosed, new HashSet<>(selector.channels()));
        closed.forEach(channel -> assertNull(selector.channel(channel.id())));

        selector.poll(0);
        assertEquals(0, selector.completedReceives().size());
    }

    private String blockingRequest(String node, String s) throws IOException {
        selector.send(createSend(node, s));
        while (true) {
            selector.poll(1000L);
            for (NetworkReceive receive : selector.completedReceives())
                if (receive.source().equals(node))
                    return asString(receive);
        }
    }

    protected void connect(String node, InetSocketAddress serverAddr) throws IOException {
        selector.connect(node, serverAddr, BUFFER_SIZE, BUFFER_SIZE);
    }

    /* connect and wait for the connection to complete */
    private void blockingConnect(String node) throws IOException {
        blockingConnect(node, new InetSocketAddress("localhost", server.port));
    }

    protected void blockingConnect(String node, InetSocketAddress serverAddr) throws IOException {
        selector.connect(node, serverAddr, BUFFER_SIZE, BUFFER_SIZE);
        NetworkTestUtils.waitForChannelReady(selector, node);
    }

    protected final NetworkSend createSend(String node, String payload) {
        return new NetworkSend(node, ByteBufferSend.sizePrefixed(ByteBuffer.wrap(payload.getBytes())));
    }

    protected String asString(NetworkReceive receive) {
        return new String(Utils.toArray(receive.payload()));
    }

    private void sendAndReceive(String node, String requestPrefix, int startIndex, int endIndex) throws Exception {
        int requests = startIndex;
        int responses = startIndex;
        selector.send(createSend(node, requestPrefix + "-" + startIndex));
        requests++;
        while (responses < endIndex) {
            // do the i/o
            selector.poll(0L);
            assertEquals(0, selector.disconnected().size(), "No disconnects should have occurred.");
            // handle requests and responses of the fast node
            for (NetworkReceive receive : selector.completedReceives()) {
                assertEquals(requestPrefix + "-" + responses, asString(receive));
                responses++;
            }

            for (int i = 0; i < selector.completedSends().size() && requests < endIndex; i++, requests++) {
                selector.send(createSend(node, requestPrefix + "-" + requests));
            }
        }
    }

    private void verifyNonEmptyImmediatelyConnectedKeys(Selector selector) throws Exception {
        Field field = Selector.class.getDeclaredField("immediatelyConnectedKeys");
        field.setAccessible(true);
        Collection<?> immediatelyConnectedKeys = (Collection<?>) field.get(selector);
        assertFalse(immediatelyConnectedKeys.isEmpty());
    }

    private void verifyEmptyImmediatelyConnectedKeys(Selector selector) throws Exception {
        Field field = Selector.class.getDeclaredField("immediatelyConnectedKeys");
        ensureEmptySelectorField(selector, field);
    }

    protected void verifySelectorEmpty() throws Exception {
        verifySelectorEmpty(this.selector);
    }

    public void verifySelectorEmpty(Selector selector) throws Exception {
        for (KafkaChannel channel : selector.channels()) {
            selector.close(channel.id());
            assertNull(channel.selectionKey().attachment());
        }
        selector.poll(0);
        selector.poll(0); // Poll a second time to clear everything
        ensureEmptySelectorFields(selector);
    }

    private void ensureEmptySelectorFields(Selector selector) throws Exception {
        for (Field field : Selector.class.getDeclaredFields()) {
            ensureEmptySelectorField(selector, field);
        }
    }

    private void ensureEmptySelectorField(Selector selector, Field field) throws Exception {
        field.setAccessible(true);
        Object obj = field.get(selector);
        if (obj instanceof Collection)
            assertTrue(((Collection<?>) obj).isEmpty(), "Field not empty: " + field + " " + obj);
        else if (obj instanceof Map)
            assertTrue(((Map<?, ?>) obj).isEmpty(), "Field not empty: " + field + " " + obj);
    }

    private KafkaMetric getMetric(String name) throws Exception {
        Optional<Map.Entry<MetricName, KafkaMetric>> metric = metrics.metrics().entrySet().stream()
                .filter(entry -> entry.getKey().name().equals(name))
                .findFirst();
        if (!metric.isPresent())
            throw new Exception(String.format("Could not find metric called %s", name));

        return metric.get().getValue();
    }

    private KafkaMetric findUntaggedMetricByName(String name) {
        MetricName metricName = new MetricName(name, METRIC_GROUP + "-metrics", "", new HashMap<>());
        KafkaMetric metric = metrics.metrics().get(metricName);
        assertNotNull(metric);
        return metric;
    }

    /**
     * Creates a connection, sends the specified number of requests and returns without reading
     * any incoming data. Some of the incoming data may be in the socket buffers when this method
     * returns, but there is no guarantee that all the data from the server will be available
     * immediately. 
     */
    private KafkaChannel createConnectionWithPendingReceives(int pendingReceives) throws Exception {
        String id = "0";
        blockingConnect(id);
        KafkaChannel channel = selector.channel(id);
        sendNoReceive(channel, pendingReceives);
        return channel;
    }

    /**
     * Sends the specified number of requests and waits for the requests to be sent.
     * The channel is muted during polling to ensure that incoming data is not received.
     */
    private void sendNoReceive(KafkaChannel channel, int numRequests) throws Exception {
        selector.mute(channel.id());
        for (int i = 0; i < numRequests; i++) {
            selector.send(createSend(channel.id(), String.valueOf(i)));
            do {
                selector.poll(10);
            } while (selector.completedSends().isEmpty());
        }
        selector.unmute(channel.id());
    }

    /**
     * Injects a NetworkReceive for channel with size buffer filled in with the provided size
     * and a payload buffer allocated with that size, but no data in the payload buffer.
     */
    private void injectNetworkReceive(KafkaChannel channel, int size) throws Exception {
        NetworkReceive receive = new NetworkReceive();
        TestUtils.setFieldValue(channel, "receive", receive);
        ByteBuffer sizeBuffer = TestUtils.fieldValue(receive, NetworkReceive.class, "size");
        sizeBuffer.putInt(size);
        TestUtils.setFieldValue(receive, "buffer", ByteBuffer.allocate(size));
    }
}
