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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.channels.SelectionKey;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;

import org.apache.kafka.common.config.SecurityConfigs;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.security.auth.PrincipalBuilder;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestSSLUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * A set of tests for the selector over ssl. These use a test harness that runs a simple socket server that echos back responses.
 */

public class SSLSelectorTest {

    private static final int BUFFER_SIZE = 4 * 1024;

    private EchoServer server;
    private Selector selector;
    private ChannelBuilder channelBuilder;

    @Before
    public void setup() throws Exception {
        Map<SSLFactory.Mode, Map<String, Object>> sslConfigs = TestSSLUtils.createSSLConfigs(false, true);
        Map<String, Object> sslServerConfigs = sslConfigs.get(SSLFactory.Mode.SERVER);
        sslServerConfigs.put(SecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG, Class.forName(SecurityConfigs.DEFAULT_PRINCIPAL_BUILDER_CLASS));
        this.server = new EchoServer(sslServerConfigs);
        this.server.start();
        Map<String, Object> sslClientConfigs = sslConfigs.get(SSLFactory.Mode.CLIENT);
        sslClientConfigs.put(SecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG, Class.forName(SecurityConfigs.DEFAULT_PRINCIPAL_BUILDER_CLASS));

        this.channelBuilder = new SSLChannelBuilder();
        this.channelBuilder.configure(sslClientConfigs);
        this.selector = new Selector(new Metrics(), new MockTime(), "MetricGroup", new LinkedHashMap<String, String>(), channelBuilder);
    }

    @After
    public void teardown() throws Exception {
        this.selector.close();
        this.server.close();
    }


    /**
     * Validate that we can send and receive a message larger than the receive and send buffer size
     */
    @Test
    public void testSendLargeRequest() throws Exception {
        int node = 0;
        blockingConnect(node);
        String big = TestUtils.randomString(10 * BUFFER_SIZE);
        assertEquals(big, blockingRequest(node, big));
    }


    /**
     * Validate that when the server disconnects, a client send ends up with that node in the disconnected list.
     */
    @Test
    public void testServerDisconnect() throws Exception {
        int node = 0;
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
     * Validate that the client can intentionally disconnect and reconnect
     */
    @Test
    public void testClientDisconnect() throws Exception {
        int node = 0;
        blockingConnect(node);
        selector.disconnect(node);
        selector.send(createSend(node, "hello1"));
        selector.poll(10L);
        assertEquals("Request should not have succeeded", 0, selector.completedSends().size());
        assertEquals("There should be a disconnect", 1, selector.disconnected().size());
        assertTrue("The disconnect should be from our node", selector.disconnected().contains(node));
        blockingConnect(node);
        assertEquals("hello2", blockingRequest(node, "hello2"));
    }

     /**
     * Tests wrap BUFFER_OVERFLOW  and unwrap BUFFER_UNDERFLOW
     * @throws Exception
     */
    @Test
    public void testLargeMessageSequence() throws Exception {
        int bufferSize = 512 * 1024;
        int node = 0;
        int reqs = 50;
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port);
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);
        String requestPrefix = TestUtils.randomString(bufferSize);
        sendAndReceive(node, requestPrefix, 0, reqs);
    }


    /**
     * Test sending an empty string
     */
    @Test
    public void testEmptyRequest() throws Exception {
        int node = 0;
        blockingConnect(node);
        assertEquals("", blockingRequest(node, ""));
    }

    /**
     * Test sending an small string
     */
    @Test
    public void testIncompleteSend() throws Exception {
        int bufferSize = 16391;
        int node = 0;
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port);
        selector.connect(node, addr, bufferSize, bufferSize);
        String requestPrefix = TestUtils.randomString(bufferSize);
        assertEquals(requestPrefix, blockingRequest(node, requestPrefix));
    }

    @Test
    public void testMute() throws Exception {
        blockingConnect(0);
        blockingConnect(1);

        selector.send(createSend(0, "hello"));
        selector.send(createSend(1, "hi"));
        selector.mute(1);

        while (selector.completedReceives().isEmpty())
            selector.poll(5);
        assertEquals("We should have only one response", 1, selector.completedReceives().size());
        assertEquals("The response should not be from the muted node", 0, selector.completedReceives().get(0).source());
        selector.unmute(1);
        do {
            selector.poll(5);
        } while (selector.completedReceives().isEmpty());
        assertEquals("We should have only one response", 1, selector.completedReceives().size());
        assertEquals("The response should be from the previously muted node", 1, selector.completedReceives().get(0).source());
    }

    /**
     * Tests that SSL renegotiation initiated by the server are handled correctly by the client
     * @throws Exception
     */
    @Test
    public void testRenegotiation() throws Exception {
        int reqs = 500;
        int node = 0;

        // create connections
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port);
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        // send echo requests and receive responses
        int requests = 0;
        int responses = 0;
        int renegotiates = 0;
        selector.send(createSend(node, node + "-" + 0));
        requests++;

        // loop until we complete all requests
        while (responses < reqs) {
            selector.poll(0L);
            if (responses >= 100 && renegotiates == 0) {
                renegotiates++;
                server.renegotiate();
            }
            assertEquals("No disconnects should have occurred.", 0, selector.disconnected().size());

            // handle any responses we may have gotten
            for (NetworkReceive receive : selector.completedReceives()) {
                String[] pieces = asString(receive).split("-");
                assertEquals("Receive text should be in the form 'conn-counter'", 2, pieces.length);
                assertEquals("Check the source", receive.source(), Integer.parseInt(pieces[0]));
                assertEquals("Receive ByteBuffer position should be at 0", 0, receive.payload().position());
                assertEquals("Check the request counter", responses, Integer.parseInt(pieces[1]));
                responses++;
            }

            // prepare new sends for the next round
            for (int i = 0; i < selector.completedSends().size() && requests < reqs; i++, requests++) {
                selector.send(createSend(node, node + "-" + requests));
            }
        }
    }


    private String blockingRequest(int node, String s) throws IOException {
        selector.send(createSend(node, s));
        while (true) {
            selector.poll(1000L);
            for (NetworkReceive receive : selector.completedReceives())
                if (receive.source() == node)
                    return asString(receive);
        }
    }

    private String asString(NetworkReceive receive) {
        return new String(Utils.toArray(receive.payload()));
    }

    private NetworkSend createSend(int node, String s) {
        return new NetworkSend(node, ByteBuffer.wrap(s.getBytes()));
    }

    /* connect and wait for the connection to complete */
    private void blockingConnect(int node) throws IOException {
        selector.connect(node, new InetSocketAddress("localhost", server.port), BUFFER_SIZE, BUFFER_SIZE);
        while (!selector.connected().contains(node))
            selector.poll(10000L);
    }


    private void sendAndReceive(int node, String requestPrefix, int startIndex, int endIndex) throws Exception {
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
