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

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * A set of tests for the selector. These use a test harness that runs a simple socket server that echos back responses.
 */
public class SelectorTest {

    private static final List<NetworkSend> EMPTY = new ArrayList<NetworkSend>();
    private static final int BUFFER_SIZE = 4 * 1024;

    private EchoServer server;
    private Selectable selector;

    @Before
    public void setup() throws Exception {
        this.server = new EchoServer();
        this.server.start();
        this.selector = new Selector(new Metrics(), new MockTime() , "MetricGroup", new LinkedHashMap<String, String>());
    }

    @After
    public void teardown() throws Exception {
        this.selector.close();
        this.server.close();
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
            selector.poll(1000L, EMPTY);

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
        selector.poll(10, asList(createSend(node, "hello1")));
        assertEquals("Request should not have succeeded", 0, selector.completedSends().size());
        assertEquals("There should be a disconnect", 1, selector.disconnected().size());
        assertTrue("The disconnect should be from our node", selector.disconnected().contains(node));
        blockingConnect(node);
        assertEquals("hello2", blockingRequest(node, "hello2"));
    }

    /**
     * Sending a request with one already in flight should result in an exception
     */
    @Test(expected = IllegalStateException.class)
    public void testCantSendWithInProgress() throws Exception {
        int node = 0;
        blockingConnect(node);
        selector.poll(1000L, asList(createSend(node, "test1"), createSend(node, "test2")));
    }

    /**
     * Sending a request to a node without an existing connection should result in an exception
     */
    @Test(expected = IllegalStateException.class)
    public void testCantSendWithoutConnecting() throws Exception {
        selector.poll(1000L, asList(createSend(0, "test")));
    }

    /**
     * Sending a request to a node with a bad hostname should result in an exception during connect
     */
    @Test(expected = IOException.class)
    public void testNoRouteToHost() throws Exception {
        selector.connect(0, new InetSocketAddress("asdf.asdf.dsc", server.port), BUFFER_SIZE, BUFFER_SIZE);
    }

    /**
     * Sending a request to a node not listening on that port should result in disconnection
     */
    @Test
    public void testConnectionRefused() throws Exception {
        int node = 0;
        selector.connect(node, new InetSocketAddress("localhost", TestUtils.choosePort()), BUFFER_SIZE, BUFFER_SIZE);
        while (selector.disconnected().contains(node))
            selector.poll(1000L, EMPTY);
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
            selector.connect(i, addr, BUFFER_SIZE, BUFFER_SIZE);

        // send echo requests and receive responses
        int[] requests = new int[conns];
        int[] responses = new int[conns];
        int responseCount = 0;
        List<NetworkSend> sends = new ArrayList<NetworkSend>();
        for (int i = 0; i < conns; i++)
            sends.add(createSend(i, i + "-" + 0));

        // loop until we complete all requests
        while (responseCount < conns * reqs) {
            // do the i/o
            selector.poll(0L, sends);

            assertEquals("No disconnects should have occurred.", 0, selector.disconnected().size());

            // handle any responses we may have gotten
            for (NetworkReceive receive : selector.completedReceives()) {
                String[] pieces = asString(receive).split("-");
                assertEquals("Should be in the form 'conn-counter'", 2, pieces.length);
                assertEquals("Check the source", receive.source(), Integer.parseInt(pieces[0]));
                assertEquals("Check that the receive has kindly been rewound", 0, receive.payload().position());
                assertEquals("Check the request counter", responses[receive.source()], Integer.parseInt(pieces[1]));
                responses[receive.source()]++; // increment the expected counter
                responseCount++;
            }

            // prepare new sends for the next round
            sends.clear();
            for (NetworkSend send : selector.completedSends()) {
                int dest = send.destination();
                requests[dest]++;
                if (requests[dest] < reqs)
                    sends.add(createSend(dest, dest + "-" + requests[dest]));
            }
        }
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
     * Test sending an empty string
     */
    @Test
    public void testEmptyRequest() throws Exception {
        int node = 0;
        blockingConnect(node);
        assertEquals("", blockingRequest(node, ""));
    }

    @Test(expected = IllegalStateException.class)
    public void testExistingConnectionId() throws IOException {
        blockingConnect(0);
        blockingConnect(0);
    }

    private String blockingRequest(int node, String s) throws IOException {
        selector.poll(1000L, asList(createSend(node, s)));
        while (true) {
            selector.poll(1000L, EMPTY);
            for (NetworkReceive receive : selector.completedReceives())
                if (receive.source() == node)
                    return asString(receive);
        }
    }

    /* connect and wait for the connection to complete */
    private void blockingConnect(int node) throws IOException {
        selector.connect(node, new InetSocketAddress("localhost", server.port), BUFFER_SIZE, BUFFER_SIZE);
        while (!selector.connected().contains(node))
            selector.poll(10000L, EMPTY);
    }

    private NetworkSend createSend(int node, String s) {
        return new NetworkSend(node, ByteBuffer.wrap(s.getBytes()));
    }

    private String asString(NetworkReceive receive) {
        return new String(Utils.toArray(receive.payload()));
    }

    /**
     * A simple server that takes size delimited byte arrays and just echos them back to the sender.
     */
    static class EchoServer extends Thread {
        public final int port;
        private final ServerSocket serverSocket;
        private final List<Thread> threads;
        private final List<Socket> sockets;

        public EchoServer() throws Exception {
            this.port = TestUtils.choosePort();
            this.serverSocket = new ServerSocket(port);
            this.threads = Collections.synchronizedList(new ArrayList<Thread>());
            this.sockets = Collections.synchronizedList(new ArrayList<Socket>());
        }

        public void run() {
            try {
                while (true) {
                    final Socket socket = serverSocket.accept();
                    sockets.add(socket);
                    Thread thread = new Thread() {
                        public void run() {
                            try {
                                DataInputStream input = new DataInputStream(socket.getInputStream());
                                DataOutputStream output = new DataOutputStream(socket.getOutputStream());
                                while (socket.isConnected() && !socket.isClosed()) {
                                    int size = input.readInt();
                                    byte[] bytes = new byte[size];
                                    input.readFully(bytes);
                                    output.writeInt(size);
                                    output.write(bytes);
                                    output.flush();
                                }
                            } catch (IOException e) {
                                // ignore
                            } finally {
                                try {
                                    socket.close();
                                } catch (IOException e) {
                                    // ignore
                                }
                            }
                        }
                    };
                    thread.start();
                    threads.add(thread);
                }
            } catch (IOException e) {
                // ignore
            }
        }

        public void closeConnections() throws IOException {
            for (Socket socket : sockets)
                socket.close();
        }

        public void close() throws IOException, InterruptedException {
            this.serverSocket.close();
            closeConnections();
            for (Thread t : threads)
                t.join();
            join();
        }
    }

}
