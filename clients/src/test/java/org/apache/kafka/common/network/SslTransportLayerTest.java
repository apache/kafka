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
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.security.TestSecurityConfig;
import org.apache.kafka.common.security.ssl.SslFactory;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the SSL transport layer. These use a test harness that runs a simple socket server that echos back responses.
 */
public class SslTransportLayerTest {

    private static final int BUFFER_SIZE = 4 * 1024;

    private NioEchoServer server;
    private Selector selector;
    private ChannelBuilder channelBuilder;
    private CertStores serverCertStores;
    private CertStores clientCertStores;
    private Map<String, Object> sslClientConfigs;
    private Map<String, Object> sslServerConfigs;

    @Before
    public void setup() throws Exception {
        // Create certificates for use by client and server. Add server cert to client truststore and vice versa.
        serverCertStores = new CertStores(true, "server",  "localhost");
        clientCertStores = new CertStores(false, "client", "localhost");
        sslServerConfigs = serverCertStores.getTrustingConfig(clientCertStores);
        sslClientConfigs = clientCertStores.getTrustingConfig(serverCertStores);
        this.channelBuilder = new SslChannelBuilder(Mode.CLIENT);
        this.channelBuilder.configure(sslClientConfigs);
        this.selector = new Selector(5000, new Metrics(), new MockTime(), "MetricGroup", channelBuilder, new LogContext());
    }

    @After
    public void teardown() throws Exception {
        if (selector != null)
            this.selector.close();
        if (server != null)
            this.server.close();
    }

    /**
     * Tests that server certificate with SubjectAltName containing the valid hostname
     * is accepted by a client that connects using the hostname and validates server endpoint.
     */
    @Test
    public void testValidEndpointIdentificationSanDns() throws Exception {
        String node = "0";
        server = createEchoServer(SecurityProtocol.SSL);
        sslClientConfigs.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "HTTPS");
        createSelector(sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.checkClientConnection(selector, node, 100, 10);
    }

    /**
     * Tests that server certificate with SubjectAltName containing valid IP address
     * is accepted by a client that connects using IP address and validates server endpoint.
     */
    @Test
    public void testValidEndpointIdentificationSanIp() throws Exception {
        String node = "0";
        serverCertStores = new CertStores(true, "server", InetAddress.getByName("127.0.0.1"));
        clientCertStores = new CertStores(false, "client", InetAddress.getByName("127.0.0.1"));
        sslServerConfigs = serverCertStores.getTrustingConfig(clientCertStores);
        sslClientConfigs = clientCertStores.getTrustingConfig(serverCertStores);
        server = createEchoServer(SecurityProtocol.SSL);
        sslClientConfigs.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "HTTPS");
        createSelector(sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("127.0.0.1", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.checkClientConnection(selector, node, 100, 10);
    }

    /**
     * Tests that server certificate with CN containing valid hostname
     * is accepted by a client that connects using hostname and validates server endpoint.
     */
    @Test
    public void testValidEndpointIdentificationCN() throws Exception {
        String node = "0";
        serverCertStores = new CertStores(true, "localhost");
        clientCertStores = new CertStores(false, "localhost");
        sslServerConfigs = serverCertStores.getTrustingConfig(clientCertStores);
        sslClientConfigs = clientCertStores.getTrustingConfig(serverCertStores);
        server = createEchoServer(SecurityProtocol.SSL);
        sslClientConfigs.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "HTTPS");
        createSelector(sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.checkClientConnection(selector, node, 100, 10);
    }

    /**
     * Tests that hostname verification is performed on the host name or address
     * specified by the client without using reverse DNS lookup. Certificate is
     * created with hostname, client connection uses IP address. Endpoint validation
     * must fail.
     */
    @Test
    public void testEndpointIdentificationNoReverseLookup() throws Exception {
        String node = "0";
        server = createEchoServer(SecurityProtocol.SSL);
        sslClientConfigs.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "HTTPS");
        createSelector(sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("127.0.0.1", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.waitForChannelClose(selector, node, ChannelState.AUTHENTICATE.value());
    }

    /**
     * According to RFC 2818:
     * <blockquote>Typically, the server has no external knowledge of what the client's
     * identity ought to be and so checks (other than that the client has a
     * certificate chain rooted in an appropriate CA) are not possible. If a
     * server has such knowledge (typically from some source external to
     * HTTP or TLS) it SHOULD check the identity as described above.</blockquote>
     *
     * However, Java SSL engine does not perform any endpoint validation for client IP address.
     * Hence it is safe to avoid reverse DNS lookup while creating the SSL engine. This test checks
     * that client validation does not fail even if the client certificate has an invalid hostname.
     * This test is to ensure that if client endpoint validation is added to Java in future, we can detect
     * and update Kafka SSL code to enable validation on the server-side and provide hostname if required.
     */
    @Test
    public void testClientEndpointNotValidated() throws Exception {
        String node = "0";

        // Create client certificate with an invalid hostname
        clientCertStores = new CertStores(false, "non-existent.com");
        serverCertStores = new CertStores(true, "localhost");
        sslServerConfigs = serverCertStores.getTrustingConfig(clientCertStores);
        sslClientConfigs = clientCertStores.getTrustingConfig(serverCertStores);

        // Create a server with endpoint validation enabled on the server SSL engine
        SslChannelBuilder serverChannelBuilder = new SslChannelBuilder(Mode.SERVER) {
            @Override
            protected SslTransportLayer buildTransportLayer(SslFactory sslFactory, String id, SelectionKey key, String host) throws IOException {
                SocketChannel socketChannel = (SocketChannel) key.channel();
                SSLEngine sslEngine = sslFactory.createSslEngine(host, socketChannel.socket().getPort());
                SSLParameters sslParams = sslEngine.getSSLParameters();
                sslParams.setEndpointIdentificationAlgorithm("HTTPS");
                sslEngine.setSSLParameters(sslParams);
                TestSslTransportLayer transportLayer = new TestSslTransportLayer(id, key, sslEngine, BUFFER_SIZE, BUFFER_SIZE, BUFFER_SIZE);
                transportLayer.startHandshake();
                return transportLayer;
            }
        };
        serverChannelBuilder.configure(sslServerConfigs);
        server = new NioEchoServer(ListenerName.forSecurityProtocol(SecurityProtocol.SSL), SecurityProtocol.SSL,
                new TestSecurityConfig(sslServerConfigs), "localhost", serverChannelBuilder, null);
        server.start();

        createSelector(sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.checkClientConnection(selector, node, 100, 10);
    }
    
    /**
     * Tests that server certificate with invalid host name is not accepted by
     * a client that validates server endpoint. Server certificate uses
     * wrong hostname as common name to trigger endpoint validation failure.
     */
    @Test
    public void testInvalidEndpointIdentification() throws Exception {
        String node = "0";
        serverCertStores = new CertStores(true, "server", "notahost");
        clientCertStores = new CertStores(false, "client", "localhost");
        sslServerConfigs = serverCertStores.getTrustingConfig(clientCertStores);
        sslClientConfigs = clientCertStores.getTrustingConfig(serverCertStores);
        sslClientConfigs.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "HTTPS");
        server = createEchoServer(SecurityProtocol.SSL);
        createSelector(sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.waitForChannelClose(selector, node, ChannelState.AUTHENTICATE.value());
    }
    
    /**
     * Tests that server certificate with invalid IP address is accepted by
     * a client that has disabled endpoint validation
     */
    @Test
    public void testEndpointIdentificationDisabled() throws Exception {
        String node = "0";
        String serverHost = InetAddress.getLocalHost().getHostAddress();
        SecurityProtocol securityProtocol = SecurityProtocol.SSL;
        server = new NioEchoServer(ListenerName.forSecurityProtocol(securityProtocol), securityProtocol,
                new TestSecurityConfig(sslServerConfigs), serverHost, null, null);
        server.start();
        sslClientConfigs.remove(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG);
        createSelector(sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress(serverHost, server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.checkClientConnection(selector, node, 100, 10);
    }
    
    /**
     * Tests that server accepts connections from clients with a trusted certificate
     * when client authentication is required.
     */
    @Test
    public void testClientAuthenticationRequiredValidProvided() throws Exception {
        String node = "0";
        sslServerConfigs.put(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, "required");
        server = createEchoServer(SecurityProtocol.SSL);
        createSelector(sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.checkClientConnection(selector, node, 100, 10);
    }

    /**
     * Tests that disabling client authentication as a listener override has the desired effect.
     */
    @Test
    public void testListenerConfigOverride() throws Exception {
        String node = "0";
        ListenerName clientListenerName = new ListenerName("client");
        sslServerConfigs.put(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, "required");
        sslServerConfigs.put(clientListenerName.configPrefix() + BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, "none");

        // `client` listener is not configured at this point, so client auth should be required
        server = createEchoServer(SecurityProtocol.SSL);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());

        // Connect with client auth should work fine
        createSelector(sslClientConfigs);
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);
        NetworkTestUtils.checkClientConnection(selector, node, 100, 10);
        selector.close();

        // Remove client auth, so connection should fail
        sslClientConfigs.remove(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
        sslClientConfigs.remove(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
        sslClientConfigs.remove(SslConfigs.SSL_KEY_PASSWORD_CONFIG);
        createSelector(sslClientConfigs);
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);
        NetworkTestUtils.waitForChannelClose(selector, node, ChannelState.AUTHENTICATE.value());
        selector.close();
        server.close();

        // Listener-specific config should be used and client auth should be disabled
        server = createEchoServer(clientListenerName, SecurityProtocol.SSL);
        addr = new InetSocketAddress("localhost", server.port());

        // Connect without client auth should work fine now
        createSelector(sslClientConfigs);
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);
        NetworkTestUtils.checkClientConnection(selector, node, 100, 10);
    }
    
    /**
     * Tests that server does not accept connections from clients with an untrusted certificate
     * when client authentication is required.
     */
    @Test
    public void testClientAuthenticationRequiredUntrustedProvided() throws Exception {
        String node = "0";
        sslServerConfigs = serverCertStores.getUntrustingConfig();
        sslServerConfigs.put(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, "required");
        server = createEchoServer(SecurityProtocol.SSL);
        createSelector(sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.waitForChannelClose(selector, node, ChannelState.AUTHENTICATE.value());
    }
    
    /**
     * Tests that server does not accept connections from clients which don't
     * provide a certificate when client authentication is required.
     */
    @Test
    public void testClientAuthenticationRequiredNotProvided() throws Exception {
        String node = "0";
        sslServerConfigs.put(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, "required");
        server = createEchoServer(SecurityProtocol.SSL);
        
        sslClientConfigs.remove(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
        sslClientConfigs.remove(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
        sslClientConfigs.remove(SslConfigs.SSL_KEY_PASSWORD_CONFIG);
        createSelector(sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.waitForChannelClose(selector, node, ChannelState.AUTHENTICATE.value());
    }
    
    /**
     * Tests that server accepts connections from a client configured
     * with an untrusted certificate if client authentication is disabled
     */
    @Test
    public void testClientAuthenticationDisabledUntrustedProvided() throws Exception {
        String node = "0";
        sslServerConfigs = serverCertStores.getUntrustingConfig();
        sslServerConfigs.put(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, "none");
        server = createEchoServer(SecurityProtocol.SSL);
        createSelector(sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.checkClientConnection(selector, node, 100, 10);
    }
    
    /**
     * Tests that server accepts connections from a client that does not provide
     * a certificate if client authentication is disabled
     */
    @Test
    public void testClientAuthenticationDisabledNotProvided() throws Exception {
        String node = "0";
        sslServerConfigs.put(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, "none");
        server = createEchoServer(SecurityProtocol.SSL);
        
        sslClientConfigs.remove(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
        sslClientConfigs.remove(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
        sslClientConfigs.remove(SslConfigs.SSL_KEY_PASSWORD_CONFIG);
        createSelector(sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.checkClientConnection(selector, node, 100, 10);
    }
    
    /**
     * Tests that server accepts connections from a client configured
     * with a valid certificate if client authentication is requested
     */
    @Test
    public void testClientAuthenticationRequestedValidProvided() throws Exception {
        String node = "0";
        sslServerConfigs.put(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, "requested");
        server = createEchoServer(SecurityProtocol.SSL);
        createSelector(sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.checkClientConnection(selector, node, 100, 10);
    }
    
    /**
     * Tests that server accepts connections from a client that does not provide
     * a certificate if client authentication is requested but not required
     */
    @Test
    public void testClientAuthenticationRequestedNotProvided() throws Exception {
        String node = "0";
        sslServerConfigs.put(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, "requested");
        server = createEchoServer(SecurityProtocol.SSL);
        
        sslClientConfigs.remove(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
        sslClientConfigs.remove(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
        sslClientConfigs.remove(SslConfigs.SSL_KEY_PASSWORD_CONFIG);
        createSelector(sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.checkClientConnection(selector, node, 100, 10);
    }

    /**
     * Tests that an invalid SecureRandom implementation cannot be configured
     */
    @Test
    public void testInvalidSecureRandomImplementation() throws Exception {
        SslChannelBuilder channelBuilder = new SslChannelBuilder(Mode.CLIENT);
        try {
            sslClientConfigs.put(SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG, "invalid");
            channelBuilder.configure(sslClientConfigs);
            fail("SSL channel configured with invalid SecureRandom implementation");
        } catch (KafkaException e) {
            // Expected exception
        }
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
     * Tests that client connections can be created to a server
     * if null truststore password is used
     */
    @Test
    public void testNullTruststorePassword() throws Exception {
        String node = "0";
        sslClientConfigs.remove(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
        sslServerConfigs.remove(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);

        server = createEchoServer(SecurityProtocol.SSL);
        createSelector(sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.checkClientConnection(selector, node, 100, 10);
    }
    
    /**
     * Tests that client connections cannot be created to a server
     * if key password is invalid
     */
    @Test
    public void testInvalidKeyPassword() throws Exception {
        String node = "0";
        sslServerConfigs.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, new Password("invalid"));
        server = createEchoServer(SecurityProtocol.SSL);
        createSelector(sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.waitForChannelClose(selector, node, ChannelState.AUTHENTICATE.value());
    }
    
    /**
     * Tests that connections cannot be made with unsupported TLS versions
     */
    @Test
    public void testUnsupportedTLSVersion() throws Exception {
        String node = "0";
        sslServerConfigs.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, Arrays.asList("TLSv1.2"));
        server = createEchoServer(SecurityProtocol.SSL);
        
        sslClientConfigs.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, Arrays.asList("TLSv1.1"));
        createSelector(sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.waitForChannelClose(selector, node, ChannelState.AUTHENTICATE.value());
    }
    
    /**
     * Tests that connections cannot be made with unsupported TLS cipher suites
     */
    @Test
    public void testUnsupportedCiphers() throws Exception {
        String node = "0";
        String[] cipherSuites = SSLContext.getDefault().getDefaultSSLParameters().getCipherSuites();
        sslServerConfigs.put(SslConfigs.SSL_CIPHER_SUITES_CONFIG, Arrays.asList(cipherSuites[0]));
        server = createEchoServer(SecurityProtocol.SSL);
        
        sslClientConfigs.put(SslConfigs.SSL_CIPHER_SUITES_CONFIG, Arrays.asList(cipherSuites[1]));
        createSelector(sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.waitForChannelClose(selector, node, ChannelState.AUTHENTICATE.value());
    }

    /**
     * Tests handling of BUFFER_UNDERFLOW during unwrap when network read buffer is smaller than SSL session packet buffer size.
     */
    @Test
    public void testNetReadBufferResize() throws Exception {
        String node = "0";
        server = createEchoServer(SecurityProtocol.SSL);
        createSelector(sslClientConfigs, 10, null, null);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.checkClientConnection(selector, node, 64000, 10);
    }
    
    /**
     * Tests handling of BUFFER_OVERFLOW during wrap when network write buffer is smaller than SSL session packet buffer size.
     */
    @Test
    public void testNetWriteBufferResize() throws Exception {
        String node = "0";
        server = createEchoServer(SecurityProtocol.SSL);
        createSelector(sslClientConfigs, null, 10, null);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.checkClientConnection(selector, node, 64000, 10);
    }

    /**
     * Tests handling of BUFFER_OVERFLOW during unwrap when application read buffer is smaller than SSL session application buffer size.
     */
    @Test
    public void testApplicationBufferResize() throws Exception {
        String node = "0";
        server = createEchoServer(SecurityProtocol.SSL);
        createSelector(sslClientConfigs, null, null, 10);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.checkClientConnection(selector, node, 64000, 10);
    }

    /**
     * Tests that time spent on the network thread is accumulated on each channel
     */
    @Test
    public void testNetworkThreadTimeRecorded() throws Exception {
        selector.close();
        this.selector = new Selector(NetworkReceive.UNLIMITED, 5000, new Metrics(), Time.SYSTEM,
                "MetricGroup", new HashMap<String, String>(), false, true, channelBuilder, MemoryPool.NONE, new LogContext());

        String node = "0";
        server = createEchoServer(SecurityProtocol.SSL);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        String message = TestUtils.randomString(10 * 1024);
        NetworkTestUtils.waitForChannelReady(selector, node);
        KafkaChannel channel = selector.channel(node);
        assertTrue("SSL handshake time not recorded", channel.getAndResetNetworkThreadTimeNanos() > 0);
        assertEquals("Time not reset", 0, channel.getAndResetNetworkThreadTimeNanos());

        selector.mute(node);
        selector.send(new NetworkSend(node, ByteBuffer.wrap(message.getBytes())));
        while (selector.completedSends().isEmpty()) {
            selector.poll(100L);
        }
        assertTrue("Send time not recorded", channel.getAndResetNetworkThreadTimeNanos() > 0);
        assertEquals("Time not reset", 0, channel.getAndResetNetworkThreadTimeNanos());

        selector.unmute(node);
        while (selector.completedReceives().isEmpty()) {
            selector.poll(100L);
        }
        assertTrue("Receive time not recorded", channel.getAndResetNetworkThreadTimeNanos() > 0);
    }

    @Test
    public void testCloseSsl() throws Exception {
        testClose(SecurityProtocol.SSL, new SslChannelBuilder(Mode.CLIENT));
    }

    @Test
    public void testClosePlaintext() throws Exception {
        testClose(SecurityProtocol.PLAINTEXT, new PlaintextChannelBuilder());
    }

    private void testClose(SecurityProtocol securityProtocol, ChannelBuilder clientChannelBuilder) throws Exception {
        String node = "0";
        server = createEchoServer(securityProtocol);
        clientChannelBuilder.configure(sslClientConfigs);
        this.selector = new Selector(5000, new Metrics(), new MockTime(), "MetricGroup", clientChannelBuilder, new LogContext());
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.waitForChannelReady(selector, node);

        final ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        server.outputChannel(Channels.newChannel(bytesOut));
        server.selector().muteAll();
        byte[] message = TestUtils.randomString(100).getBytes();
        int count = 20;
        final int totalSendSize = count * (message.length + 4);
        for (int i = 0; i < count; i++) {
            selector.send(new NetworkSend(node, ByteBuffer.wrap(message)));
            do {
                selector.poll(0L);
            } while (selector.completedSends().isEmpty());
        }
        server.selector().unmuteAll();
        selector.close(node);
        TestUtils.waitForCondition(new TestCondition() {
            @Override
            public boolean conditionMet() {
                return bytesOut.toByteArray().length == totalSendSize;
            }
        }, 5000, "All requests sent were not processed");
    }

    private void createSelector(Map<String, Object> sslClientConfigs) {
        createSelector(sslClientConfigs, null, null, null);
    }      

    private void createSelector(Map<String, Object> sslClientConfigs, final Integer netReadBufSize,
                                final Integer netWriteBufSize, final Integer appBufSize) {
        
        this.channelBuilder = new SslChannelBuilder(Mode.CLIENT) {

            @Override
            protected SslTransportLayer buildTransportLayer(SslFactory sslFactory, String id, SelectionKey key, String host) throws IOException {
                SocketChannel socketChannel = (SocketChannel) key.channel();
                SSLEngine sslEngine = sslFactory.createSslEngine(host, socketChannel.socket().getPort());
                TestSslTransportLayer transportLayer = new TestSslTransportLayer(id, key, sslEngine, netReadBufSize, netWriteBufSize, appBufSize);
                transportLayer.startHandshake();
                return transportLayer;
            }


        };
        this.channelBuilder.configure(sslClientConfigs);
        this.selector = new Selector(5000, new Metrics(), new MockTime(), "MetricGroup", channelBuilder, new LogContext());
    }

    private NioEchoServer createEchoServer(ListenerName listenerName, SecurityProtocol securityProtocol) throws Exception {
        return NetworkTestUtils.createEchoServer(listenerName, securityProtocol, new TestSecurityConfig(sslServerConfigs), null);
    }

    private NioEchoServer createEchoServer(SecurityProtocol securityProtocol) throws Exception {
        return createEchoServer(ListenerName.forSecurityProtocol(securityProtocol), securityProtocol);
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

}
