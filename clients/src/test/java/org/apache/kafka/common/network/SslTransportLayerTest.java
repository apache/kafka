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
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.io.IOException;
import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.ssl.SslFactory;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestSslUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the SSL transport layer. These use a test harness that runs a simple socket server that echos back responses.
 */
public class SslTransportLayerTest {

    private static final int BUFFER_SIZE = 4 * 1024;

    private SslEchoServer server;
    private Selector selector;
    private ChannelBuilder channelBuilder;
    private CertStores serverCertStores;
    private CertStores clientCertStores;
    private Map<String, Object> sslClientConfigs;
    private Map<String, Object> sslServerConfigs;

    @Before
    public void setup() throws Exception {
        // Create certificates for use by client and server. Add server cert to client truststore and vice versa.
        serverCertStores = new CertStores(true, "localhost");
        clientCertStores = new CertStores(false, "localhost");
        sslServerConfigs = serverCertStores.getTrustingConfig(clientCertStores);
        sslClientConfigs = clientCertStores.getTrustingConfig(serverCertStores);
        this.channelBuilder = new SslChannelBuilder(Mode.CLIENT);
        this.channelBuilder.configure(sslClientConfigs);
        this.selector = new Selector(5000, new Metrics(), new MockTime(), "MetricGroup", channelBuilder);
    }

    @After
    public void teardown() throws Exception {
        if (selector != null)
            this.selector.close();
        if (server != null)
            this.server.close();
    }

    /**
     * Tests that server certificate with valid IP address is accepted by
     * a client that validates server endpoint.
     */
    @Test
    public void testValidEndpointIdentification() throws Exception {
        String node = "0";
        createEchoServer(sslServerConfigs);
        sslClientConfigs.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "HTTPS");
        createSelector(sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port);
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        testClientConnection(node, 100, 10);
    }
    
    /**
     * Tests that server certificate with invalid host name is not accepted by
     * a client that validates server endpoint. Server certificate uses
     * wrong hostname as common name to trigger endpoint validation failure.
     */
    @Test
    public void testInvalidEndpointIdentification() throws Exception {
        String node = "0";
        serverCertStores = new CertStores(true, "notahost");
        clientCertStores = new CertStores(false, "localhost");
        sslServerConfigs = serverCertStores.getTrustingConfig(clientCertStores);
        sslClientConfigs = clientCertStores.getTrustingConfig(serverCertStores);
        sslClientConfigs.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "HTTPS");
        createEchoServer(sslServerConfigs);
        createSelector(sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port);
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        waitForChannelClose(node);
    }
    
    /**
     * Tests that server certificate with invalid IP address is accepted by
     * a client that has disabled endpoint validation
     */
    @Test
    public void testEndpointIdentificationDisabled() throws Exception {
        String node = "0";
        String serverHost = InetAddress.getLocalHost().getHostAddress();
        server = new SslEchoServer(sslServerConfigs, serverHost);
        server.start();
        sslClientConfigs.remove(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG);
        createSelector(sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress(serverHost, server.port);
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        testClientConnection(node, 100, 10);
    }
    
    /**
     * Tests that server accepts connections from clients with a trusted certificate
     * when client authentication is required.
     */
    @Test
    public void testClientAuthenticationRequiredValidProvided() throws Exception {
        String node = "0";
        sslServerConfigs.put(SslConfigs.SSL_CLIENT_AUTH_CONFIG, "required");
        createEchoServer(sslServerConfigs);
        createSelector(sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port);
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        testClientConnection(node, 100, 10);
    }
    
    /**
     * Tests that server does not accept connections from clients with an untrusted certificate
     * when client authentication is required.
     */
    @Test
    public void testClientAuthenticationRequiredUntrustedProvided() throws Exception {
        String node = "0";
        sslServerConfigs = serverCertStores.getUntrustingConfig();
        sslServerConfigs.put(SslConfigs.SSL_CLIENT_AUTH_CONFIG, "required");
        createEchoServer(sslServerConfigs);        
        createSelector(sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port);
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        waitForChannelClose(node);
    }
    
    /**
     * Tests that server does not accept connections from clients which don't
     * provide a certificate when client authentication is required.
     */
    @Test
    public void testClientAuthenticationRequiredNotProvided() throws Exception {
        String node = "0";
        sslServerConfigs.put(SslConfigs.SSL_CLIENT_AUTH_CONFIG, "required");
        createEchoServer(sslServerConfigs);
        
        sslClientConfigs.remove(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
        sslClientConfigs.remove(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
        sslClientConfigs.remove(SslConfigs.SSL_KEY_PASSWORD_CONFIG);
        createSelector(sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port);
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        waitForChannelClose(node);
    }
    
    /**
     * Tests that server accepts connections from a client configured
     * with an untrusted certificate if client authentication is disabled
     */
    @Test
    public void testClientAuthenticationDisabledUntrustedProvided() throws Exception {
        String node = "0";
        sslServerConfigs = serverCertStores.getUntrustingConfig();
        sslServerConfigs.put(SslConfigs.SSL_CLIENT_AUTH_CONFIG, "none");
        createEchoServer(sslServerConfigs);      
        createSelector(sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port);
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        testClientConnection(node, 100, 10);
    }
    
    /**
     * Tests that server accepts connections from a client that does not provide
     * a certificate if client authentication is disabled
     */
    @Test
    public void testClientAuthenticationDisabledNotProvided() throws Exception {
        String node = "0";
        sslServerConfigs.put(SslConfigs.SSL_CLIENT_AUTH_CONFIG, "none");
        createEchoServer(sslServerConfigs);
        
        sslClientConfigs.remove(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
        sslClientConfigs.remove(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
        sslClientConfigs.remove(SslConfigs.SSL_KEY_PASSWORD_CONFIG);
        createSelector(sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port);
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        testClientConnection(node, 100, 10);
    }
    
    /**
     * Tests that server accepts connections from a client configured
     * with a valid certificate if client authentication is requested
     */
    @Test
    public void testClientAuthenticationRequestedValidProvided() throws Exception {
        String node = "0";
        sslServerConfigs.put(SslConfigs.SSL_CLIENT_AUTH_CONFIG, "requested");
        createEchoServer(sslServerConfigs);
        createSelector(sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port);
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        testClientConnection(node, 100, 10);
    }
    
    /**
     * Tests that server accepts connections from a client that does not provide
     * a certificate if client authentication is requested but not required
     */
    @Test
    public void testClientAuthenticationRequestedNotProvided() throws Exception {
        String node = "0";
        sslServerConfigs.put(SslConfigs.SSL_CLIENT_AUTH_CONFIG, "requested");
        createEchoServer(sslServerConfigs);
        
        sslClientConfigs.remove(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
        sslClientConfigs.remove(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
        sslClientConfigs.remove(SslConfigs.SSL_KEY_PASSWORD_CONFIG);
        createSelector(sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port);
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        testClientConnection(node, 100, 10);
    }
    
    /**
     * Tests that channels cannot be created if truststore cannot be loaded
     */
    @Test
    public void testInvalidTruststorePassword() throws Exception {
        SslChannelBuilder channelBuilder = new SslChannelBuilder(Mode.CLIENT);
        try {
            sslClientConfigs.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "invalid");
            channelBuilder.configure(sslClientConfigs);
            fail("SSL channel configured with invalid truststore password");
        } catch (KafkaException e) {
            // Expected exception
        }
    }
    
    /**
     * Tests that channels cannot be created if keystore cannot be loaded
     */
    @Test
    public void testInvalidKeystorePassword() throws Exception {
        SslChannelBuilder channelBuilder = new SslChannelBuilder(Mode.CLIENT);
        try {
            sslClientConfigs.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "invalid");
            channelBuilder.configure(sslClientConfigs);
            fail("SSL channel configured with invalid keystore password");
        } catch (KafkaException e) {
            // Expected exception
        }
    }
    
    /**
     * Tests that client connections cannot be created to a server
     * if key password is invalid
     */
    @Test
    public void testInvalidKeyPassword() throws Exception {
        String node = "0";
        sslServerConfigs.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, new Password("invalid"));
        createEchoServer(sslServerConfigs);        
        createSelector(sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port);
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        waitForChannelClose(node);
    }
    
    /**
     * Tests that connections cannot be made with unsupported TLS versions
     */
    @Test
    public void testUnsupportedTLSVersion() throws Exception {
        String node = "0";
        sslServerConfigs.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, Arrays.asList("TLSv1.2"));
        createEchoServer(sslServerConfigs);
        
        sslClientConfigs.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, Arrays.asList("TLSv1.1"));
        createSelector(sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port);
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        waitForChannelClose(node);
    }
    
    /**
     * Tests that connections cannot be made with unsupported TLS cipher suites
     */
    @Test
    public void testUnsupportedCiphers() throws Exception {
        String node = "0";
        String[] cipherSuites = SSLContext.getDefault().getDefaultSSLParameters().getCipherSuites();
        sslServerConfigs.put(SslConfigs.SSL_CIPHER_SUITES_CONFIG, Arrays.asList(cipherSuites[0]));
        createEchoServer(sslServerConfigs);
        
        sslClientConfigs.put(SslConfigs.SSL_CIPHER_SUITES_CONFIG, Arrays.asList(cipherSuites[1]));
        createSelector(sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port);
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        waitForChannelClose(node);
    }

    /**
     * Tests handling of BUFFER_UNDERFLOW during unwrap when network read buffer is smaller than SSL session packet buffer size.
     */
    @Test
    public void testNetReadBufferResize() throws Exception {
        String node = "0";
        createEchoServer(sslServerConfigs);
        createSelector(sslClientConfigs, 10, null, null);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port);
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        testClientConnection(node, 64000, 10);
    }
    
    /**
     * Tests handling of BUFFER_OVERFLOW during wrap when network write buffer is smaller than SSL session packet buffer size.
     */
    @Test
    public void testNetWriteBufferResize() throws Exception {
        String node = "0";
        createEchoServer(sslServerConfigs);
        createSelector(sslClientConfigs, null, 10, null);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port);
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        testClientConnection(node, 64000, 10);
    }

    /**
     * Tests handling of BUFFER_OVERFLOW during unwrap when application read buffer is smaller than SSL session application buffer size.
     */
    @Test
    public void testApplicationBufferResize() throws Exception {
        String node = "0";
        createEchoServer(sslServerConfigs);
        createSelector(sslClientConfigs, null, null, 10);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port);
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        testClientConnection(node, 64000, 10);
    }

    private void testClientConnection(String node, int minMessageSize, int messageCount) throws Exception {

        String prefix = TestUtils.randomString(minMessageSize);
        int requests = 0;
        int responses = 0;
        // wait for handshake to finish
        while (!selector.isChannelReady(node)) {
            selector.poll(1000L);
        }
        selector.send(new NetworkSend(node, ByteBuffer.wrap((prefix + "-0").getBytes())));
        requests++;
        while (responses < messageCount) {
            selector.poll(0L);
            assertEquals("No disconnects should have occurred.", 0, selector.disconnected().size());

            for (NetworkReceive receive : selector.completedReceives()) {
                assertEquals(prefix + "-" + responses, new String(Utils.toArray(receive.payload())));
                responses++;
            }

            for (int i = 0; i < selector.completedSends().size() && requests < messageCount && selector.isChannelReady(node); i++, requests++) {
                selector.send(new NetworkSend(node, ByteBuffer.wrap((prefix + "-" + requests).getBytes())));
            }
        }
    }
    
    private void waitForChannelClose(String node) throws IOException {
        boolean closed = false;
        for (int i = 0; i < 30; i++) {
            selector.poll(1000L);
            if (selector.channel(node) == null) {
                closed = true;
                break;
            }
        }
        assertTrue(closed);
    }
    
    private void createEchoServer(Map<String, Object> sslServerConfigs) throws Exception {
        server = new SslEchoServer(sslServerConfigs, "localhost");
        server.start();
    }
    
    private void createSelector(Map<String, Object> sslClientConfigs) {
        createSelector(sslClientConfigs, null, null, null);
    }      

    private void createSelector(Map<String, Object> sslClientConfigs, final Integer netReadBufSize, final Integer netWriteBufSize, final Integer appBufSize) {
        
        this.channelBuilder = new SslChannelBuilder(Mode.CLIENT) {

            @Override
            protected SslTransportLayer buildTransportLayer(SslFactory sslFactory, String id, SelectionKey key) throws IOException {
                SocketChannel socketChannel = (SocketChannel) key.channel();
                SSLEngine sslEngine = sslFactory.createSslEngine(socketChannel.socket().getInetAddress().getHostName(),
                                socketChannel.socket().getPort());
                TestSslTransportLayer transportLayer = new TestSslTransportLayer(id, key, sslEngine, netReadBufSize, netWriteBufSize, appBufSize);
                transportLayer.startHandshake();
                return transportLayer;
            }


        };
        this.channelBuilder.configure(sslClientConfigs);
        this.selector = new Selector(5000, new Metrics(), new MockTime(), "MetricGroup", channelBuilder);
    }
    
    private static class CertStores {
        
        Map<String, Object> sslConfig;
        
        CertStores(boolean server, String host) throws Exception {
            String name = server ? "server" : "client";
            Mode mode = server ? Mode.SERVER : Mode.CLIENT;
            File truststoreFile = File.createTempFile(name + "TS", ".jks");
            sslConfig = TestSslUtils.createSslConfig(!server, true, mode, truststoreFile, name, host);
            if (server)
                sslConfig.put(SslConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG, Class.forName(SslConfigs.DEFAULT_PRINCIPAL_BUILDER_CLASS));
        }
       
        private Map<String, Object> getTrustingConfig(CertStores truststoreConfig) {
            Map<String, Object> config = new HashMap<>(sslConfig);
            config.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststoreConfig.sslConfig.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
            config.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truststoreConfig.sslConfig.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG));
            config.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, truststoreConfig.sslConfig.get(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG));
            return config;
        }
        
        private Map<String, Object> getUntrustingConfig() {
            return sslConfig;
        }
    }

    /**
     * SSLTransportLayer with overrides for packet and application buffer size to test buffer resize
     * code path. The overridden buffer size starts with a small value and increases in size when the buffer
     * size is retrieved to handle overflow/underflow, until the actual session buffer size is reached.
     */
    private static class TestSslTransportLayer extends SslTransportLayer {

        private final ResizeableBufferSize netReadBufSize;
        private final ResizeableBufferSize netWriteBufSize;
        private final ResizeableBufferSize appBufSize;

        public TestSslTransportLayer(String channelId, SelectionKey key, SSLEngine sslEngine,
                                     Integer netReadBufSize, Integer netWriteBufSize, Integer appBufSize) throws IOException {
            super(channelId, key, sslEngine, false);
            this.netReadBufSize = new ResizeableBufferSize(netReadBufSize);
            this.netWriteBufSize = new ResizeableBufferSize(netWriteBufSize);
            this.appBufSize = new ResizeableBufferSize(appBufSize);
        }
        
        @Override
        protected int netReadBufferSize() {
            ByteBuffer netReadBuffer = netReadBuffer();
            // netReadBufferSize() is invoked in SSLTransportLayer.read() prior to the read
            // operation. To avoid the read buffer being expanded too early, increase buffer size
            // only when read buffer is full. This ensures that BUFFER_UNDERFLOW is always
            // triggered in testNetReadBufferResize().
            boolean updateBufSize = netReadBuffer != null && !netReadBuffer().hasRemaining();
            return netReadBufSize.updateAndGet(super.netReadBufferSize(), updateBufSize);
        }
        
        @Override
        protected int netWriteBufferSize() {
            return netWriteBufSize.updateAndGet(super.netWriteBufferSize(), true);
        }

        @Override
        protected int applicationBufferSize() {
            return appBufSize.updateAndGet(super.applicationBufferSize(), true);
        }
        
        private static class ResizeableBufferSize {
            private Integer bufSizeOverride;
            ResizeableBufferSize(Integer bufSizeOverride) {
                this.bufSizeOverride = bufSizeOverride;
            }
            int updateAndGet(int actualSize, boolean update) {
                int size = actualSize;
                if (bufSizeOverride != null) {
                    if (update)
                        bufSizeOverride = Math.min(bufSizeOverride * 2, size);
                    size = bufSizeOverride;
                }
                return size;
            }
        }
    }
    
    // Non-blocking EchoServer implementation that uses SSLTransportLayer
    private class SslEchoServer extends Thread {
        private final int port;
        private final ServerSocketChannel serverSocketChannel;
        private final List<SocketChannel> newChannels;
        private final List<SocketChannel> socketChannels;
        private final AcceptorThread acceptorThread;
        private SslFactory sslFactory;
        private final Selector selector;
        private final ConcurrentLinkedQueue<NetworkSend> inflightSends = new ConcurrentLinkedQueue<NetworkSend>();

        public SslEchoServer(Map<String, ?> configs, String serverHost) throws Exception {
            this.sslFactory = new SslFactory(Mode.SERVER);
            this.sslFactory.configure(configs);
            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.socket().bind(new InetSocketAddress(serverHost, 0));
            this.port = serverSocketChannel.socket().getLocalPort();
            this.socketChannels = Collections.synchronizedList(new ArrayList<SocketChannel>());
            this.newChannels = Collections.synchronizedList(new ArrayList<SocketChannel>());
            SslChannelBuilder channelBuilder = new SslChannelBuilder(Mode.SERVER);
            channelBuilder.configure(sslServerConfigs);
            this.selector = new Selector(5000, new Metrics(), new MockTime(), "MetricGroup", channelBuilder);
            setName("echoserver");
            setDaemon(true);
            acceptorThread = new AcceptorThread();
        }

        @Override
        public void run() {
            try {
                acceptorThread.start();
                while (serverSocketChannel.isOpen()) {
                    selector.poll(1000);
                    for (SocketChannel socketChannel : newChannels) {
                        String id = id(socketChannel);
                        selector.register(id, socketChannel);
                        socketChannels.add(socketChannel);
                    }
                    newChannels.clear();
                    while (true) {
                        NetworkSend send = inflightSends.peek();
                        if (send != null && !selector.channel(send.destination()).hasSend()) {
                            send = inflightSends.poll();
                            selector.send(send);
                        } else
                            break;
                    }
                    List<NetworkReceive> completedReceives = selector.completedReceives();
                    for (NetworkReceive rcv : completedReceives) {
                        NetworkSend send = new NetworkSend(rcv.source(), rcv.payload());
                        if (!selector.channel(send.destination()).hasSend())
                            selector.send(send);
                        else
                            inflightSends.add(send);
                    }
                }
            } catch (IOException e) {
                // ignore
            }
        }
        
        private String id(SocketChannel channel) {
            return channel.socket().getLocalAddress().getHostAddress() + ":" + channel.socket().getLocalPort() + "-" +
                    channel.socket().getInetAddress().getHostAddress() + ":" + channel.socket().getPort();
        }

        public void closeConnections() throws IOException {
            for (SocketChannel channel : socketChannels)
                channel.close();
            socketChannels.clear();
        }

        public void close() throws IOException, InterruptedException {
            this.serverSocketChannel.close();
            closeConnections();
            acceptorThread.interrupt();
            acceptorThread.join();
            interrupt();
            join();
        }
        
        private class AcceptorThread extends Thread {
            public AcceptorThread() throws IOException {
                setName("acceptor");
            }
            public void run() {
                try {

                    java.nio.channels.Selector acceptSelector = java.nio.channels.Selector.open();
                    serverSocketChannel.register(acceptSelector, SelectionKey.OP_ACCEPT);
                    while (serverSocketChannel.isOpen()) {
                        if (acceptSelector.select(1000) > 0) {
                            Iterator<SelectionKey> it = acceptSelector.selectedKeys().iterator();
                            while (it.hasNext()) {
                                SelectionKey key = it.next();
                                if (key.isAcceptable()) {
                                    SocketChannel socketChannel = ((ServerSocketChannel) key.channel()).accept();
                                    socketChannel.configureBlocking(false);
                                    newChannels.add(socketChannel);
                                    selector.wakeup();
                                }
                            }
                        }
                    }
                } catch (IOException e) {
                    // ignore
                }
            }
        }
    }

}
