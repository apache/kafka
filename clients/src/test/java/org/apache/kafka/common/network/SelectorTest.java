/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.network;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * A set of tests for the selector. These use a test harness that runs a simple socket server that echos back responses.
 */
public class SelectorTest {

    protected static final int BUFFER_SIZE = 4 * 1024;

    protected EchoServer server;
    protected Time time;
    protected Selector selector;
    protected ChannelBuilder channelBuilder;
    private Metrics metrics;

    @Before
    public void setUp() throws Exception {
        Map<String, Object> configs = new HashMap<>();
        this.server = new EchoServer(SecurityProtocol.PLAINTEXT, configs);
        this.server.start();
        this.time = new MockTime();
        this.channelBuilder = new PlaintextChannelBuilder();
        this.channelBuilder.configure(configs);
        this.metrics = new Metrics();
        this.selector = new Selector(5000, this.metrics, time, "MetricGroup", channelBuilder);
    }

    @After
    public void tearDown() throws Exception {
        this.selector.close();
        this.server.close();
        this.metrics.close();
    }

    /**
     * Validate that when the server disconnects, a client send ends up with that node in the disconnected list.
     */
    @Test
    public void testServerDisconnect() throws Exception {
        String node = "0";

        // connect and do a simple request
        blockingConnect(node);
        assertEquals("hello", blockingRequest(node, "hello"));

        // disconnect
        this.server.closeConnections();
        while (!selector.disconnected().contains(node))
            selector.poll(1000L);

        // reconnect and do another request
        blockingConnect(node);
        assertEquals("hello", blockingRequest(node, "hello"));
    }

    /**
     * Sending a request with one already in flight should result in an exception
     */
    @Test(expected = IllegalStateException.class)
    public void testCantSendWithInProgress() throws Exception {
        String node = "0";
        blockingConnect(node);
        selector.send(createSend(node, "test1"));
        selector.send(createSend(node, "test2"));
        selector.poll(1000L);
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
        selector.connect("0", new InetSocketAddress("asdf.asdf.dsc", server.port), BUFFER_SIZE, BUFFER_SIZE);
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
        while (selector.disconnected().contains(node))
            selector.poll(1000L);
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
        Map<String, Integer> requests = new HashMap<String, Integer>();
        Map<String, Integer> responses = new HashMap<String, Integer>();
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



    /**
     * Test sending an empty string
     */
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
    public void testCloseOldestConnection() throws Exception {
        String id = "0";
        blockingConnect(id);

        time.sleep(6000); // The max idle time is 5000ms
        selector.poll(0);

        assertTrue("The idle connection should have been closed", selector.disconnected().contains(id));
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


}
