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

import org.apache.kafka.common.KafkaException;
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
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


/**
 * A set of tests for the selector. These use a test harness that runs a simple socket server that echos back responses.
 */
public class SelectorTest {

    protected static final int BUFFER_SIZE = 4 * 1024;

    protected EchoServer server;
    protected Time time;
    protected Selector selector;
    protected ChannelBuilder channelBuilder;
    protected Metrics metrics;

    @Before
    public void setUp() throws Exception {
        Map<String, Object> configs = new HashMap<>();
        this.server = new EchoServer(SecurityProtocol.PLAINTEXT, configs);
        this.server.start();
        this.time = new MockTime();
        this.channelBuilder = new PlaintextChannelBuilder(ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT));
        this.channelBuilder.configure(configs);
        this.metrics = new Metrics();
        this.selector = new Selector(5000, this.metrics, time, "MetricGroup", channelBuilder, new LogContext());
    }

    @After
    public void tearDown() throws Exception {
        try {
            verifySelectorEmpty();
        } finally {
            this.selector.close();

            assertEquals(0, this.selector.countOfClosingChannel());
            assertEquals(0, this.selector.countOfMutedChannels());
            assertEquals(0, this.selector.countOfConnectedChannels());
            this.server.close();
            this.metrics.close();
        }
    }

    public SecurityProtocol securityProtocol() {
        return SecurityProtocol.PLAINTEXT;
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
        TestUtils.waitForCondition(new TestCondition() {
            @Override
            public boolean conditionMet() {
                try {
                    selector.poll(1000L);
                    return selector.disconnected().containsKey(node);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
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
        assertTrue("Channel not closed", selector.disconnected().containsKey(node));
        assertEquals(ChannelState.FAILED_SEND, selector.disconnected().get(node));
    }

    /**
     * Sending a request to a node without an existing connection should result in an exception
     */
    @Test(expected = IllegalStateException.class)
    public void testCantSendWithoutConnecting() throws Exception {
        selector.send(createSend("0", "test"));
        selector.poll(1000L);
    }

    /**
     * Sending a request to a node with a bad hostname should result in an exception during connect
     */
    @Test(expected = IOException.class)
    public void testNoRouteToHost() throws Exception {
        selector.connect("0", new InetSocketAddress("some.invalid.hostname.foo.bar.local", server.port), BUFFER_SIZE, BUFFER_SIZE);
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

            assertEquals("No disconnects should have occurred.", 0, selector.disconnected().size());

            // handle any responses we may have gotten
            for (NetworkReceive receive : selector.completedReceives()) {
                String[] pieces = asString(receive).split("-");
                assertEquals("Should be in the form 'conn-counter'", 2, pieces.length);
                assertEquals("Check the source", receive.source(), pieces[0]);
                assertEquals("Check that the receive has kindly been rewound", 0, receive.payload().position());
                if (responses.containsKey(receive.source())) {
                    assertEquals("Check the request counter", (int) responses.get(receive.source()), Integer.parseInt(pieces[1]));
                    responses.put(receive.source(), responses.get(receive.source()) + 1);
                } else {
                    assertEquals("Check the request counter", 0, Integer.parseInt(pieces[1]));
                    responses.put(receive.source(), 1);
                }
                responseCount++;
            }

            // prepare new sends for the next round
            for (Send send : selector.completedSends()) {
                String dest = send.destination();
                if (requests.containsKey(dest))
                    requests.put(dest, requests.get(dest) + 1);
                else
                    requests.put(dest, 1);
                if (requests.get(dest) < reqs)
                    selector.send(createSend(dest, dest + "-" + requests.get(dest)));
            }
        }
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

    @Test(expected = IllegalStateException.class)
    public void testExistingConnectionId() throws IOException {
        blockingConnect("0");
        blockingConnect("0");
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
        assertEquals("We should have only one response", 1, selector.completedReceives().size());
        assertEquals("The response should not be from the muted node", "0", selector.completedReceives().get(0).source());

        selector.unmute("1");
        do {
            selector.poll(5);
        } while (selector.completedReceives().isEmpty());
        assertEquals("We should have only one response", 1, selector.completedReceives().size());
        assertEquals("The response should be from the previously muted node", "1", selector.completedReceives().get(0).source());
    }

    @Test
    public void registerFailure() throws Exception {
        ChannelBuilder channelBuilder = new PlaintextChannelBuilder(null) {
            @Override
            public KafkaChannel buildChannel(String id, SelectionKey key, int maxReceiveSize,
                    MemoryPool memoryPool) throws KafkaException {
                throw new RuntimeException("Test exception");
            }
            @Override
            public void close() {
            }
        };
        Selector selector = new Selector(5000, new Metrics(), new MockTime(), "MetricGroup", channelBuilder, new LogContext());
        SocketChannel socketChannel = SocketChannel.open();
        socketChannel.configureBlocking(false);
        try {
            selector.register("1", socketChannel);
            fail("Register did not fail");
        } catch (IOException e) {
            assertTrue("Unexpected exception: " + e, e.getCause().getMessage().contains("Test exception"));
            assertFalse("Socket not closed", socketChannel.isOpen());
        }
        selector.close();
    }

    @Test
    public void testCloseConnectionInClosingState() throws Exception {
        KafkaChannel channel = createConnectionWithStagedReceives(5);
        String id = channel.id();
        selector.mute(id); // Mute to allow channel to be expired even if more data is available for read
        time.sleep(6000);  // The max idle time is 5000ms
        selector.poll(0);
        assertNull("Channel not expired", selector.channel(id));
        assertEquals(channel, selector.closingChannel(id));
        assertEquals(ChannelState.EXPIRED, channel.state());
        selector.close(id);
        assertNull("Channel not removed from channels", selector.channel(id));
        assertNull("Channel not removed from closingChannels", selector.closingChannel(id));
        assertTrue("Unexpected disconnect notification", selector.disconnected().isEmpty());
        assertEquals(ChannelState.EXPIRED, channel.state());
        assertNull(channel.selectionKey().attachment());
        selector.poll(0);
        assertTrue("Unexpected disconnect notification", selector.disconnected().isEmpty());
    }

    @Test
    public void testCloseOldestConnection() throws Exception {
        String id = "0";
        blockingConnect(id);

        time.sleep(6000); // The max idle time is 5000ms
        selector.poll(0);

        assertTrue("The idle connection should have been closed", selector.disconnected().containsKey(id));
        assertEquals(ChannelState.EXPIRED, selector.disconnected().get(id));
    }

    @Test
    public void testIdleExpiryWithoutReadyKeys() throws IOException {
        String id = "0";
        selector.connect(id, new InetSocketAddress("localhost", server.port), BUFFER_SIZE, BUFFER_SIZE);
        KafkaChannel channel = selector.channel(id);
        channel.selectionKey().interestOps(0);

        time.sleep(6000); // The max idle time is 5000ms
        selector.poll(0);
        assertTrue("The idle connection should have been closed", selector.disconnected().containsKey(id));
        assertEquals(ChannelState.EXPIRED, selector.disconnected().get(id));
    }

    @Test
    public void testImmediatelyConnectedCleaned() throws Exception {
        Metrics metrics = new Metrics(); // new metrics object to avoid metric registration conflicts
        Selector selector = new ImmediatelyConnectingSelector(5000, metrics, time, "MetricGroup", channelBuilder, new LogContext());

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
        Selector selector = new ImmediatelyConnectingSelector(5000, metrics, time, "MetricGroup", channelBuilder, new LogContext()) {
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
            assertNull("Channel not removed", selector.channel(id));
            ensureEmptySelectorFields(selector);
        }
    }

    @Test
    public void testCloseOldestConnectionWithOneStagedReceive() throws Exception {
        verifyCloseOldestConnectionWithStagedReceives(1);
    }

    @Test
    public void testCloseOldestConnectionWithMultipleStagedReceives() throws Exception {
        verifyCloseOldestConnectionWithStagedReceives(5);
    }

    private KafkaChannel createConnectionWithStagedReceives(int maxStagedReceives) throws Exception {
        String id = "0";
        blockingConnect(id);
        KafkaChannel channel = selector.channel(id);
        int retries = 100;

        do {
            selector.mute(id);
            for (int i = 0; i <= maxStagedReceives; i++) {
                selector.send(createSend(id, String.valueOf(i)));
                do {
                    selector.poll(1000);
                } while (selector.completedSends().isEmpty());
            }

            selector.unmute(id);
            do {
                selector.poll(1000);
            } while (selector.completedReceives().isEmpty());
        } while (selector.numStagedReceives(channel) == 0 && !channel.hasBytesBuffered() && --retries > 0);
        assertTrue("No staged receives after 100 attempts", selector.numStagedReceives(channel) > 0);
        // We want to return without any bytes buffered to ensure that channel will be closed after idle time
        assertFalse("Channel has bytes buffered", channel.hasBytesBuffered());

        return channel;
    }

    private void verifyCloseOldestConnectionWithStagedReceives(int maxStagedReceives) throws Exception {
        KafkaChannel channel = createConnectionWithStagedReceives(maxStagedReceives);
        String id = channel.id();
        int stagedReceives = selector.numStagedReceives(channel);
        int completedReceives = 0;
        while (selector.disconnected().isEmpty()) {
            time.sleep(6000); // The max idle time is 5000ms
            selector.poll(0);
            completedReceives += selector.completedReceives().size();
            // With SSL, more receives may be staged from buffered data
            int newStaged = selector.numStagedReceives(channel) - (stagedReceives - completedReceives);
            if (newStaged > 0) {
                stagedReceives += newStaged;
                assertNotNull("Channel should not have been expired", selector.channel(id));
                assertFalse("Channel should not have been disconnected", selector.disconnected().containsKey(id));
            } else if (!selector.completedReceives().isEmpty()) {
                assertEquals(1, selector.completedReceives().size());
                assertTrue("Channel not found", selector.closingChannel(id) != null || selector.channel(id) != null);
                assertFalse("Disconnect notified too early", selector.disconnected().containsKey(id));
            }
        }
        assertEquals(stagedReceives, completedReceives);
        assertNull("Channel not removed", selector.channel(id));
        assertNull("Channel not removed", selector.closingChannel(id));
        assertTrue("Disconnect not notified", selector.disconnected().containsKey(id));
        assertTrue("Unexpected receive", selector.completedReceives().isEmpty());
    }

    @Test
    public void testMuteOnOOM() throws Exception {
        //clean up default selector, replace it with one that uses a finite mem pool
        selector.close();
        MemoryPool pool = new SimpleMemoryPool(900, 900, false, null);
        selector = new Selector(NetworkReceive.UNLIMITED, 5000, metrics, time, "MetricGroup",
            new HashMap<String, String>(), true, false, channelBuilder, pool, new LogContext());

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

            List<NetworkReceive> completed = Collections.emptyList();
            long deadline = System.currentTimeMillis() + 5000;
            while (System.currentTimeMillis() < deadline && completed.isEmpty()) {
                selector.poll(1000);
                completed = selector.completedReceives();
            }
            assertEquals("could not read a single request within timeout", 1, completed.size());
            NetworkReceive firstReceive = completed.get(0);
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
            assertEquals("could not read a single request within timeout", 1, selector.completedReceives().size());
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
        when(selectionKey.channel()).thenReturn(SocketChannel.open());
        when(selectionKey.readyOps()).thenReturn(SelectionKey.OP_CONNECT);

        selectionKey.attach(kafkaChannel);
        Set<SelectionKey> selectionKeys = Utils.mkSet(selectionKey);
        selector.pollSelectionKeys(selectionKeys, false, System.nanoTime());

        assertFalse(selector.connected().contains(kafkaChannel.id()));
        assertTrue(selector.disconnected().containsKey(kafkaChannel.id()));
        assertNull(selectionKey.attachment());

        verify(kafkaChannel, atLeastOnce()).ready();
        verify(kafkaChannel).disconnect();
        verify(kafkaChannel).close();
        verify(selectionKey).cancel();
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
                SocketChannel channel = ss.accept();
                channel.configureBlocking(false);

                selector.register(Integer.toString(i), channel);
            }
        }

        assertEquals((double) conns, getMetric("connection-creation-total").metricValue());
        assertEquals((double) conns, getMetric("connection-count").metricValue());
    }

    private String blockingRequest(String node, String s) throws IOException {
        selector.send(createSend(node, s));
        selector.poll(1000L);
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
        while (!selector.connected().contains(node))
            selector.poll(10000L);
        while (!selector.isChannelReady(node))
            selector.poll(10000L);
    }

    protected NetworkSend createSend(String node, String s) {
        return new NetworkSend(node, ByteBuffer.wrap(s.getBytes()));
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
            assertEquals("No disconnects should have occurred.", 0, selector.disconnected().size());

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

    private void verifySelectorEmpty(Selector selector) throws Exception {
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
            assertTrue("Field not empty: " + field + " " + obj, ((Collection<?>) obj).isEmpty());
        else if (obj instanceof Map)
            assertTrue("Field not empty: " + field + " " + obj, ((Map<?, ?>) obj).isEmpty());
    }

    private KafkaMetric getMetric(String name) throws Exception {
        Optional<Map.Entry<MetricName, KafkaMetric>> metric = metrics.metrics().entrySet().stream()
                .filter(entry -> entry.getKey().name().equals(name))
                .findFirst();
        if (!metric.isPresent())
            throw new Exception(String.format("Could not find metric called %s", name));

        return metric.get().getValue();
    }
}
