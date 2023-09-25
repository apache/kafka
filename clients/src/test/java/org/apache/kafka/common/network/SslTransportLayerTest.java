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
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.security.TestSecurityConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.ssl.DefaultSslEngineFactory;
import org.apache.kafka.common.security.ssl.SslFactory;
import org.apache.kafka.common.utils.Java;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestSslUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Stream;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Tests for the SSL transport layer. These use a test harness that runs a simple socket server that echos back responses.
 */
public class SslTransportLayerTest {

    private static final int BUFFER_SIZE = 4 * 1024;
    private static Time time = Time.SYSTEM;

    private static class Args {
        private final String tlsProtocol;
        private final boolean useInlinePem;
        private CertStores serverCertStores;
        private CertStores clientCertStores;
        private Map<String, Object> sslClientConfigs;
        private Map<String, Object> sslServerConfigs;
        private Map<String, Object> sslConfigOverrides;

        public Args(String tlsProtocol, boolean useInlinePem) throws Exception {
            this.tlsProtocol = tlsProtocol;
            this.useInlinePem = useInlinePem;
            sslConfigOverrides = new HashMap<>();
            sslConfigOverrides.put(SslConfigs.SSL_PROTOCOL_CONFIG, tlsProtocol);
            sslConfigOverrides.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, Collections.singletonList(tlsProtocol));
            init();
        }

        Map<String, Object> getTrustingConfig(CertStores certStores, CertStores peerCertStores) {
            Map<String, Object> configs = certStores.getTrustingConfig(peerCertStores);
            configs.putAll(sslConfigOverrides);
            return configs;
        }

        private void init() throws Exception {
            // Create certificates for use by client and server. Add server cert to client truststore and vice versa.
            serverCertStores = certBuilder(true, "server", useInlinePem).addHostName("localhost").build();
            clientCertStores = certBuilder(false, "client", useInlinePem).addHostName("localhost").build();
            sslServerConfigs = getTrustingConfig(serverCertStores, clientCertStores);
            sslClientConfigs = getTrustingConfig(clientCertStores, serverCertStores);
            sslServerConfigs.put(SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG, DefaultSslEngineFactory.class);
            sslClientConfigs.put(SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG, DefaultSslEngineFactory.class);
        }

        @Override
        public String toString() {
            return "tlsProtocol=" + tlsProtocol +
                    ", useInlinePem=" + useInlinePem;
        }
    }

    private static class SslTransportLayerArgumentsProvider implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
            List<Arguments> parameters = new ArrayList<>();
            parameters.add(Arguments.of(new Args("TLSv1.2", false)));
            parameters.add(Arguments.of(new Args("TLSv1.2", true)));
            if (Java.IS_JAVA11_COMPATIBLE) {
                parameters.add(Arguments.of(new Args("TLSv1.3", false)));
            }
            return parameters.stream();
        }
    }

    private NioEchoServer server;
    private Selector selector;

    @AfterEach
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
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testValidEndpointIdentificationSanDns(Args args) throws Exception {
        createSelector(args);
        String node = "0";
        server = createEchoServer(args, SecurityProtocol.SSL);
        args.sslClientConfigs.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "HTTPS");
        createSelector(args.sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.checkClientConnection(selector, node, 100, 10);
        server.verifyAuthenticationMetrics(1, 0);
    }

    /**
     * Tests that server certificate with SubjectAltName containing valid IP address
     * is accepted by a client that connects using IP address and validates server endpoint.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testValidEndpointIdentificationSanIp(Args args) throws Exception {
        String node = "0";
        args.serverCertStores = certBuilder(true, "server", args.useInlinePem).hostAddress(InetAddress.getByName("127.0.0.1")).build();
        args.clientCertStores = certBuilder(false, "client", args.useInlinePem).hostAddress(InetAddress.getByName("127.0.0.1")).build();
        args.sslServerConfigs = args.getTrustingConfig(args.serverCertStores, args.clientCertStores);
        args.sslClientConfigs = args.getTrustingConfig(args.clientCertStores, args.serverCertStores);
        server = createEchoServer(args, SecurityProtocol.SSL);
        args.sslClientConfigs.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "HTTPS");
        createSelector(args.sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("127.0.0.1", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.checkClientConnection(selector, node, 100, 10);
    }

    /**
     * Tests that server certificate with CN containing valid hostname
     * is accepted by a client that connects using hostname and validates server endpoint.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testValidEndpointIdentificationCN(Args args) throws Exception {
        args.serverCertStores = certBuilder(true, "localhost", args.useInlinePem).build();
        args.clientCertStores = certBuilder(false, "localhost", args.useInlinePem).build();
        args.sslServerConfigs = args.getTrustingConfig(args.serverCertStores, args.clientCertStores);
        args.sslClientConfigs = args.getTrustingConfig(args.clientCertStores, args.serverCertStores);
        args.sslClientConfigs.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "HTTPS");
        verifySslConfigs(args);
    }

    /**
     * Tests that hostname verification is performed on the host name or address
     * specified by the client without using reverse DNS lookup. Certificate is
     * created with hostname, client connection uses IP address. Endpoint validation
     * must fail.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testEndpointIdentificationNoReverseLookup(Args args) throws Exception {
        String node = "0";
        server = createEchoServer(args, SecurityProtocol.SSL);
        args.sslClientConfigs.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "HTTPS");
        createSelector(args.sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("127.0.0.1", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.waitForChannelClose(selector, node, ChannelState.State.AUTHENTICATION_FAILED);
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
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testClientEndpointNotValidated(Args args) throws Exception {
        String node = "0";

        // Create client certificate with an invalid hostname
        args.clientCertStores = certBuilder(false, "non-existent.com", args.useInlinePem).build();
        args.serverCertStores = certBuilder(true, "localhost", args.useInlinePem).build();
        args.sslServerConfigs = args.getTrustingConfig(args.serverCertStores, args.clientCertStores);
        args.sslClientConfigs = args.getTrustingConfig(args.clientCertStores, args.serverCertStores);

        // Create a server with endpoint validation enabled on the server SSL engine
        SslChannelBuilder serverChannelBuilder = new TestSslChannelBuilder(Mode.SERVER) {
            @Override
            protected TestSslTransportLayer newTransportLayer(String id, SelectionKey key, SSLEngine sslEngine) throws IOException {
                SSLParameters sslParams = sslEngine.getSSLParameters();
                sslParams.setEndpointIdentificationAlgorithm("HTTPS");
                sslEngine.setSSLParameters(sslParams);
                return super.newTransportLayer(id, key, sslEngine);
            }
        };
        serverChannelBuilder.configure(args.sslServerConfigs);
        server = new NioEchoServer(ListenerName.forSecurityProtocol(SecurityProtocol.SSL), SecurityProtocol.SSL,
                new TestSecurityConfig(args.sslServerConfigs), "localhost", serverChannelBuilder, null, time);
        server.start();

        createSelector(args.sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.checkClientConnection(selector, node, 100, 10);
    }

    /**
     * Tests that server certificate with invalid host name is not accepted by
     * a client that validates server endpoint. Server certificate uses
     * wrong hostname as common name to trigger endpoint validation failure.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testInvalidEndpointIdentification(Args args) throws Exception {
        args.serverCertStores = certBuilder(true, "server", args.useInlinePem).addHostName("notahost").build();
        args.clientCertStores = certBuilder(false, "client", args.useInlinePem).addHostName("localhost").build();
        args.sslServerConfigs = args.getTrustingConfig(args.serverCertStores, args.clientCertStores);
        args.sslClientConfigs = args.getTrustingConfig(args.clientCertStores, args.serverCertStores);
        args.sslClientConfigs.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "HTTPS");
        verifySslConfigsWithHandshakeFailure(args);
    }

    /**
     * Tests that server certificate with invalid host name is accepted by
     * a client that has disabled endpoint validation
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testEndpointIdentificationDisabled(Args args) throws Exception {
        args.serverCertStores = certBuilder(true, "server", args.useInlinePem).addHostName("notahost").build();
        args.clientCertStores = certBuilder(false, "client", args.useInlinePem).addHostName("localhost").build();
        args.sslServerConfigs = args.getTrustingConfig(args.serverCertStores, args.clientCertStores);
        args.sslClientConfigs = args.getTrustingConfig(args.clientCertStores, args.serverCertStores);

        server = createEchoServer(args, SecurityProtocol.SSL);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());

        // Disable endpoint validation, connection should succeed
        String node = "1";
        args.sslClientConfigs.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
        createSelector(args.sslClientConfigs);
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);
        NetworkTestUtils.checkClientConnection(selector, node, 100, 10);

        // Disable endpoint validation using null value, connection should succeed
        String node2 = "2";
        args.sslClientConfigs.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, null);
        createSelector(args.sslClientConfigs);
        selector.connect(node2, addr, BUFFER_SIZE, BUFFER_SIZE);
        NetworkTestUtils.checkClientConnection(selector, node2, 100, 10);

        // Connection should fail with endpoint validation enabled
        String node3 = "3";
        args.sslClientConfigs.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "HTTPS");
        createSelector(args.sslClientConfigs);
        selector.connect(node3, addr, BUFFER_SIZE, BUFFER_SIZE);
        NetworkTestUtils.waitForChannelClose(selector, node3, ChannelState.State.AUTHENTICATION_FAILED);
        selector.close();
    }

    /**
     * Tests that server accepts connections from clients with a trusted certificate
     * when client authentication is required.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testClientAuthenticationRequiredValidProvided(Args args) throws Exception {
        args.sslServerConfigs.put(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, "required");
        verifySslConfigs(args);
    }

    /**
     * Tests that disabling client authentication as a listener override has the desired effect.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testListenerConfigOverride(Args args) throws Exception {
        String node = "0";
        ListenerName clientListenerName = new ListenerName("client");
        args.sslServerConfigs.put(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, "required");
        args.sslServerConfigs.put(clientListenerName.configPrefix() + BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, "none");

        // `client` listener is not configured at this point, so client auth should be required
        server = createEchoServer(args, SecurityProtocol.SSL);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());

        // Connect with client auth should work fine
        createSelector(args.sslClientConfigs);
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);
        NetworkTestUtils.checkClientConnection(selector, node, 100, 10);
        selector.close();

        // Remove client auth, so connection should fail
        CertStores.KEYSTORE_PROPS.forEach(args.sslClientConfigs::remove);
        createSelector(args.sslClientConfigs);
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);
        NetworkTestUtils.waitForChannelClose(selector, node, ChannelState.State.AUTHENTICATION_FAILED);
        selector.close();
        server.close();

        // Listener-specific config should be used and client auth should be disabled
        server = createEchoServer(args, clientListenerName, SecurityProtocol.SSL);
        addr = new InetSocketAddress("localhost", server.port());

        // Connect without client auth should work fine now
        createSelector(args.sslClientConfigs);
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);
        NetworkTestUtils.checkClientConnection(selector, node, 100, 10);
    }

    /**
     * Tests that server does not accept connections from clients with an untrusted certificate
     * when client authentication is required.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testClientAuthenticationRequiredUntrustedProvided(Args args) throws Exception {
        args.sslServerConfigs = args.serverCertStores.getUntrustingConfig();
        args.sslServerConfigs.putAll(args.sslConfigOverrides);
        args.sslServerConfigs.put(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, "required");
        verifySslConfigsWithHandshakeFailure(args);
    }

    /**
     * Tests that server does not accept connections from clients which don't
     * provide a certificate when client authentication is required.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testClientAuthenticationRequiredNotProvided(Args args) throws Exception {
        args.sslServerConfigs.put(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, "required");
        CertStores.KEYSTORE_PROPS.forEach(args.sslClientConfigs::remove);
        verifySslConfigsWithHandshakeFailure(args);
    }

    /**
     * Tests that server accepts connections from a client configured
     * with an untrusted certificate if client authentication is disabled
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testClientAuthenticationDisabledUntrustedProvided(Args args) throws Exception {
        args.sslServerConfigs = args.serverCertStores.getUntrustingConfig();
        args.sslServerConfigs.putAll(args.sslConfigOverrides);
        args.sslServerConfigs.put(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, "none");
        verifySslConfigs(args);
    }

    /**
     * Tests that server accepts connections from a client that does not provide
     * a certificate if client authentication is disabled
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testClientAuthenticationDisabledNotProvided(Args args) throws Exception {
        args.sslServerConfigs.put(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, "none");

        CertStores.KEYSTORE_PROPS.forEach(args.sslClientConfigs::remove);
        verifySslConfigs(args);
    }

    /**
     * Tests that server accepts connections from a client configured
     * with a valid certificate if client authentication is requested
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testClientAuthenticationRequestedValidProvided(Args args) throws Exception {
        args.sslServerConfigs.put(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, "requested");
        verifySslConfigs(args);
    }

    /**
     * Tests that server accepts connections from a client that does not provide
     * a certificate if client authentication is requested but not required
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testClientAuthenticationRequestedNotProvided(Args args) throws Exception {
        args.sslServerConfigs.put(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, "requested");

        CertStores.KEYSTORE_PROPS.forEach(args.sslClientConfigs::remove);
        verifySslConfigs(args);
    }

    /**
     * Tests key-pair created using DSA.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testDsaKeyPair(Args args) throws Exception {
        // DSA algorithms are not supported for TLSv1.3.
        assumeTrue(args.tlsProtocol.equals("TLSv1.2"));
        args.serverCertStores = certBuilder(true, "server", args.useInlinePem).keyAlgorithm("DSA").build();
        args.clientCertStores = certBuilder(false, "client", args.useInlinePem).keyAlgorithm("DSA").build();
        args.sslServerConfigs = args.getTrustingConfig(args.serverCertStores, args.clientCertStores);
        args.sslClientConfigs = args.getTrustingConfig(args.clientCertStores, args.serverCertStores);
        args.sslServerConfigs.put(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, "required");
        verifySslConfigs(args);
    }

    /**
     * Tests key-pair created using EC.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testECKeyPair(Args args) throws Exception {
        args.serverCertStores = certBuilder(true, "server", args.useInlinePem).keyAlgorithm("EC").build();
        args.clientCertStores = certBuilder(false, "client", args.useInlinePem).keyAlgorithm("EC").build();
        args.sslServerConfigs = args.getTrustingConfig(args.serverCertStores, args.clientCertStores);
        args.sslClientConfigs = args.getTrustingConfig(args.clientCertStores, args.serverCertStores);
        args.sslServerConfigs.put(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, "required");
        verifySslConfigs(args);
    }

    /**
     * Tests PEM key store and trust store files which don't have store passwords.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testPemFiles(Args args) throws Exception {
        TestSslUtils.convertToPem(args.sslServerConfigs, true, true);
        TestSslUtils.convertToPem(args.sslClientConfigs, true, true);
        args.sslServerConfigs.put(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, "required");
        verifySslConfigs(args);
    }

    /**
     * Test with PEM key store files without key password for client key store.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testPemFilesWithoutClientKeyPassword(Args args) throws Exception {
        boolean useInlinePem = args.useInlinePem;
        TestSslUtils.convertToPem(args.sslServerConfigs, !useInlinePem, true);
        TestSslUtils.convertToPem(args.sslClientConfigs, !useInlinePem, false);
        args.sslServerConfigs.put(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, "required");
        server = createEchoServer(args, SecurityProtocol.SSL);
        verifySslConfigs(args);
    }

    /**
     * Test with PEM key store files without key password for server key store.We don't allow this
     * with PEM files since unprotected private key on disk is not safe.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testPemFilesWithoutServerKeyPassword(Args args) throws Exception {
        TestSslUtils.convertToPem(args.sslServerConfigs, !args.useInlinePem, false);
        TestSslUtils.convertToPem(args.sslClientConfigs, !args.useInlinePem, true);
        verifySslConfigs(args);
    }

    /**
     * Tests that an invalid SecureRandom implementation cannot be configured
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testInvalidSecureRandomImplementation(Args args) {
        try (SslChannelBuilder channelBuilder = newClientChannelBuilder()) {
            args.sslClientConfigs.put(SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG, "invalid");
            assertThrows(KafkaException.class, () -> channelBuilder.configure(args.sslClientConfigs));
        }
    }

    /**
     * Tests that channels cannot be created if truststore cannot be loaded
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testInvalidTruststorePassword(Args args) {
        try (SslChannelBuilder channelBuilder = newClientChannelBuilder()) {
            args.sslClientConfigs.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "invalid");
            assertThrows(KafkaException.class, () -> channelBuilder.configure(args.sslClientConfigs));
        }
    }

    /**
     * Tests that channels cannot be created if keystore cannot be loaded
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testInvalidKeystorePassword(Args args) {
        try (SslChannelBuilder channelBuilder = newClientChannelBuilder()) {
            args.sslClientConfigs.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "invalid");
            assertThrows(KafkaException.class, () -> channelBuilder.configure(args.sslClientConfigs));
        }
    }

    /**
     * Tests that client connections can be created to a server
     * if null truststore password is used
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testNullTruststorePassword(Args args) throws Exception {
        args.sslClientConfigs.remove(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
        args.sslServerConfigs.remove(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);

        verifySslConfigs(args);
    }

    /**
     * Tests that client connections cannot be created to a server
     * if key password is invalid
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testInvalidKeyPassword(Args args) throws Exception {
        args.sslServerConfigs.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, new Password("invalid"));
        if (args.useInlinePem) {
            // We fail fast for PEM
            assertThrows(InvalidConfigurationException.class, () -> createEchoServer(args, SecurityProtocol.SSL));
            return;
        }
        verifySslConfigsWithHandshakeFailure(args);
    }

    /**
     * Tests that connection succeeds with the default TLS version.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testTlsDefaults(Args args) throws Exception {
        args.sslServerConfigs = args.serverCertStores.getTrustingConfig(args.clientCertStores);
        args.sslClientConfigs = args.clientCertStores.getTrustingConfig(args.serverCertStores);

        assertEquals(SslConfigs.DEFAULT_SSL_PROTOCOL, args.sslServerConfigs.get(SslConfigs.SSL_PROTOCOL_CONFIG));
        assertEquals(SslConfigs.DEFAULT_SSL_PROTOCOL, args.sslClientConfigs.get(SslConfigs.SSL_PROTOCOL_CONFIG));

        server = createEchoServer(args, SecurityProtocol.SSL);
        createSelector(args.sslClientConfigs);

        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect("0", addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.checkClientConnection(selector, "0", 10, 100);
        server.verifyAuthenticationMetrics(1, 0);
        selector.close();
    }

    /** Checks connection failed using the specified {@code tlsVersion}. */
    private void checkAuthenticationFailed(Args args, String node, String tlsVersion) throws IOException {
        args.sslClientConfigs.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, Arrays.asList(tlsVersion));
        createSelector(args.sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.waitForChannelClose(selector, node, ChannelState.State.AUTHENTICATION_FAILED);

        selector.close();
    }

    /**
     * Tests that connections cannot be made with unsupported TLS cipher suites
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testUnsupportedCiphers(Args args) throws Exception {
        SSLContext context = SSLContext.getInstance(args.tlsProtocol);
        context.init(null, null, null);
        String[] cipherSuites = context.getDefaultSSLParameters().getCipherSuites();
        args.sslServerConfigs.put(SslConfigs.SSL_CIPHER_SUITES_CONFIG, Arrays.asList(cipherSuites[0]));
        server = createEchoServer(args, SecurityProtocol.SSL);

        args.sslClientConfigs.put(SslConfigs.SSL_CIPHER_SUITES_CONFIG, Arrays.asList(cipherSuites[1]));
        createSelector(args.sslClientConfigs);

        checkAuthenticationFailed(args, "1", args.tlsProtocol);
        server.verifyAuthenticationMetrics(0, 1);
    }

    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testServerRequestMetrics(Args args) throws Exception {
        String node = "0";
        server = createEchoServer(args, SecurityProtocol.SSL);
        createSelector(args.sslClientConfigs, 16384, 16384, 16384);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect(node, addr, 102400, 102400);
        NetworkTestUtils.waitForChannelReady(selector, node);
        int messageSize = 1024 * 1024;
        String message = TestUtils.randomString(messageSize);
        selector.send(new NetworkSend(node, ByteBufferSend.sizePrefixed(ByteBuffer.wrap(message.getBytes()))));
        while (selector.completedReceives().isEmpty()) {
            selector.poll(100L);
        }
        int totalBytes = messageSize + 4; // including 4-byte size
        server.waitForMetric("incoming-byte", totalBytes);
        server.waitForMetric("outgoing-byte", totalBytes);
        server.waitForMetric("request", 1);
        server.waitForMetric("response", 1);
    }

    /**
     * selector.poll() should be able to fetch more data than netReadBuffer from the socket.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testSelectorPollReadSize(Args args) throws Exception {
        String node = "0";
        server = createEchoServer(args, SecurityProtocol.SSL);
        createSelector(args.sslClientConfigs, 16384, 16384, 16384);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect(node, addr, 102400, 102400);
        NetworkTestUtils.checkClientConnection(selector, node, 81920, 1);

        // Send a message of 80K. This is 5X as large as the socket buffer. It should take at least three selector.poll()
        // to read this message from socket if the SslTransportLayer.read() does not read all data from socket buffer.
        String message = TestUtils.randomString(81920);
        selector.send(new NetworkSend(node, ByteBufferSend.sizePrefixed(ByteBuffer.wrap(message.getBytes()))));

        // Send the message to echo server
        TestUtils.waitForCondition(() -> {
            try {
                selector.poll(100L);
            } catch (IOException e) {
                return false;
            }
            return selector.completedSends().size() > 0;
        }, "Timed out waiting for message to be sent");

        // Wait for echo server to send the message back
        TestUtils.waitForCondition(() ->
            server.numSent() >= 2, "Timed out waiting for echo server to send message");

        // Read the message from socket with only one poll()
        selector.poll(1000L);

        Collection<NetworkReceive> receiveList = selector.completedReceives();
        assertEquals(1, receiveList.size());
        assertEquals(message, new String(Utils.toArray(receiveList.iterator().next().payload())));
    }

    /**
     * Tests handling of BUFFER_UNDERFLOW during unwrap when network read buffer is smaller than SSL session packet buffer size.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testNetReadBufferResize(Args args) throws Exception {
        String node = "0";
        server = createEchoServer(args, SecurityProtocol.SSL);
        createSelector(args.sslClientConfigs, 10, null, null);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.checkClientConnection(selector, node, 64000, 10);
    }

    /**
     * Tests handling of BUFFER_OVERFLOW during wrap when network write buffer is smaller than SSL session packet buffer size.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testNetWriteBufferResize(Args args) throws Exception {
        String node = "0";
        server = createEchoServer(args, SecurityProtocol.SSL);
        createSelector(args.sslClientConfigs, null, 10, null);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.checkClientConnection(selector, node, 64000, 10);
    }

    /**
     * Tests handling of BUFFER_OVERFLOW during unwrap when application read buffer is smaller than SSL session application buffer size.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testApplicationBufferResize(Args args) throws Exception {
        String node = "0";
        server = createEchoServer(args, SecurityProtocol.SSL);
        createSelector(args.sslClientConfigs, null, null, 10);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.checkClientConnection(selector, node, 64000, 10);
    }

    /**
     * Tests that time spent on the network thread is accumulated on each channel
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testNetworkThreadTimeRecorded(Args args) throws Exception {
        LogContext logContext = new LogContext();
        ChannelBuilder channelBuilder = new SslChannelBuilder(Mode.CLIENT, null, false, logContext);
        channelBuilder.configure(args.sslClientConfigs);
        try (Selector selector = new Selector(NetworkReceive.UNLIMITED, Selector.NO_IDLE_TIMEOUT_MS, new Metrics(), Time.SYSTEM,
                "MetricGroup", new HashMap<>(), false, true, channelBuilder, MemoryPool.NONE, logContext)) {

            String node = "0";
            server = createEchoServer(args, SecurityProtocol.SSL);
            InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
            selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

            String message = TestUtils.randomString(1024 * 1024);
            NetworkTestUtils.waitForChannelReady(selector, node);
            final KafkaChannel channel = selector.channel(node);
            assertTrue(channel.getAndResetNetworkThreadTimeNanos() > 0, "SSL handshake time not recorded");
            assertEquals(0, channel.getAndResetNetworkThreadTimeNanos(), "Time not reset");

            selector.mute(node);
            selector.send(new NetworkSend(node, ByteBufferSend.sizePrefixed(ByteBuffer.wrap(message.getBytes()))));
            while (selector.completedSends().isEmpty()) {
                selector.poll(100L);
            }
            long sendTimeNanos = channel.getAndResetNetworkThreadTimeNanos();
            assertTrue(sendTimeNanos > 0, "Send time not recorded: " + sendTimeNanos);
            assertEquals(0, channel.getAndResetNetworkThreadTimeNanos(), "Time not reset");
            assertFalse(channel.hasBytesBuffered(), "Unexpected bytes buffered");
            assertEquals(0, selector.completedReceives().size());

            selector.unmute(node);
            // Wait for echo server to send the message back
            TestUtils.waitForCondition(() -> {
                try {
                    selector.poll(100L);
                } catch (IOException e) {
                    return false;
                }
                return !selector.completedReceives().isEmpty();
            }, "Timed out waiting for a message to receive from echo server");

            long receiveTimeNanos = channel.getAndResetNetworkThreadTimeNanos();
            assertTrue(receiveTimeNanos > 0, "Receive time not recorded: " + receiveTimeNanos);
        }
    }

    /**
     * Tests that IOExceptions from read during SSL handshake are not treated as authentication failures.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testIOExceptionsDuringHandshakeRead(Args args) throws Exception {
        server = createEchoServer(args, SecurityProtocol.SSL);
        testIOExceptionsDuringHandshake(args, FailureAction.THROW_IO_EXCEPTION, FailureAction.NO_OP);
    }

    /**
     * Tests that IOExceptions from write during SSL handshake are not treated as authentication failures.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testIOExceptionsDuringHandshakeWrite(Args args) throws Exception {
        server = createEchoServer(args, SecurityProtocol.SSL);
        testIOExceptionsDuringHandshake(args, FailureAction.NO_OP, FailureAction.THROW_IO_EXCEPTION);
    }

    /**
     * Tests that if the remote end closes connection ungracefully  during SSL handshake while reading data,
     * the disconnection is not treated as an authentication failure.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testUngracefulRemoteCloseDuringHandshakeRead(Args args) throws Exception {
        server = createEchoServer(args, SecurityProtocol.SSL);
        testIOExceptionsDuringHandshake(args, server::closeSocketChannels, FailureAction.NO_OP);
    }

    /**
     * Tests that if the remote end closes connection ungracefully during SSL handshake while writing data,
     * the disconnection is not treated as an authentication failure.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testUngracefulRemoteCloseDuringHandshakeWrite(Args args) throws Exception {
        server = createEchoServer(args, SecurityProtocol.SSL);
        testIOExceptionsDuringHandshake(args, FailureAction.NO_OP, server::closeSocketChannels);
    }

    /**
     * Tests that if the remote end closes the connection during SSL handshake while reading data,
     * the disconnection is not treated as an authentication failure.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testGracefulRemoteCloseDuringHandshakeRead(Args args) throws Exception {
        server = createEchoServer(args, SecurityProtocol.SSL);
        testIOExceptionsDuringHandshake(args, FailureAction.NO_OP, server::closeKafkaChannels);
    }

    /**
     * Tests that if the remote end closes the connection during SSL handshake while writing data,
     * the disconnection is not treated as an authentication failure.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testGracefulRemoteCloseDuringHandshakeWrite(Args args) throws Exception {
        server = createEchoServer(args, SecurityProtocol.SSL);
        testIOExceptionsDuringHandshake(args, server::closeKafkaChannels, FailureAction.NO_OP);
    }

    private void testIOExceptionsDuringHandshake(Args args,
                                                 FailureAction readFailureAction,
                                                 FailureAction flushFailureAction) throws Exception {
        TestSslChannelBuilder channelBuilder = new TestSslChannelBuilder(Mode.CLIENT);
        boolean done = false;
        for (int i = 1; i <= 100; i++) {
            String node = String.valueOf(i);

            channelBuilder.readFailureAction = readFailureAction;
            channelBuilder.flushFailureAction = flushFailureAction;
            channelBuilder.failureIndex = i;
            channelBuilder.configure(args.sslClientConfigs);
            this.selector = new Selector(10000, new Metrics(), time, "MetricGroup", channelBuilder, new LogContext());

            InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
            selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);
            for (int j = 0; j < 30; j++) {
                selector.poll(1000L);
                KafkaChannel channel = selector.channel(node);
                if (channel != null && channel.ready()) {
                    done = true;
                    break;
                }
                if (selector.disconnected().containsKey(node)) {
                    ChannelState.State state = selector.disconnected().get(node).state();
                    assertTrue(state == ChannelState.State.AUTHENTICATE || state == ChannelState.State.READY,
                        "Unexpected channel state " + state);
                    break;
                }
            }
            KafkaChannel channel = selector.channel(node);
            if (channel != null)
                assertTrue(channel.ready(), "Channel not ready or disconnected:" + channel.state().state());
            selector.close();
        }
        assertTrue(done, "Too many invocations of read/write during SslTransportLayer.handshake()");
    }

    /**
     * Tests that handshake failures are propagated only after writes complete, even when
     * there are delays in writes to ensure that clients see an authentication exception
     * rather than a connection failure.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testPeerNotifiedOfHandshakeFailure(Args args) throws Exception {
        args.sslServerConfigs = args.serverCertStores.getUntrustingConfig();
        args.sslServerConfigs.putAll(args.sslConfigOverrides);
        args.sslServerConfigs.put(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, "required");

        // Test without delay and a couple of delay counts to ensure delay applies to handshake failure
        for (int i = 0; i < 3; i++) {
            String node = String.valueOf(i);
            TestSslChannelBuilder serverChannelBuilder = new TestSslChannelBuilder(Mode.SERVER);
            serverChannelBuilder.configure(args.sslServerConfigs);
            serverChannelBuilder.flushDelayCount = i;
            server = new NioEchoServer(ListenerName.forSecurityProtocol(SecurityProtocol.SSL),
                    SecurityProtocol.SSL, new TestSecurityConfig(args.sslServerConfigs),
                    "localhost", serverChannelBuilder, null, time);
            server.start();
            createSelector(args.sslClientConfigs);
            InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
            selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

            NetworkTestUtils.waitForChannelClose(selector, node, ChannelState.State.AUTHENTICATION_FAILED);
            server.close();
            selector.close();
            serverChannelBuilder.close();
        }
    }

    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testPeerNotifiedOfHandshakeFailureWithClientSideDelay(Args args) throws Exception {
        args.sslServerConfigs.put(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, "required");
        CertStores.KEYSTORE_PROPS.forEach(args.sslClientConfigs::remove);
        verifySslConfigsWithHandshakeFailure(args, 1);
    }

    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testCloseSsl(Args args) throws Exception {
        testClose(args, SecurityProtocol.SSL, newClientChannelBuilder());
    }

    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testClosePlaintext(Args args) throws Exception {
        testClose(args, SecurityProtocol.PLAINTEXT, new PlaintextChannelBuilder(null));
    }

    private SslChannelBuilder newClientChannelBuilder() {
        return new SslChannelBuilder(Mode.CLIENT, null, false, new LogContext());
    }

    private void testClose(Args args, SecurityProtocol securityProtocol, ChannelBuilder clientChannelBuilder) throws Exception {
        String node = "0";
        server = createEchoServer(args, securityProtocol);
        clientChannelBuilder.configure(args.sslClientConfigs);
        this.selector = new Selector(5000, new Metrics(), time, "MetricGroup", clientChannelBuilder, new LogContext());
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.waitForChannelReady(selector, node);
        // `waitForChannelReady` waits for client-side channel to be ready. This is sufficient for other tests
        // operating on the client-side channel. But here, we are muting the server-side channel below, so we
        // need to wait for the server-side channel to be ready as well.
        TestUtils.waitForCondition(() -> server.selector().channels().stream().allMatch(KafkaChannel::ready),
                "Channel not ready");

        final ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        server.outputChannel(Channels.newChannel(bytesOut));
        server.selector().muteAll();
        byte[] message = TestUtils.randomString(100).getBytes();
        int count = 20;
        final int totalSendSize = count * (message.length + 4);
        for (int i = 0; i < count; i++) {
            selector.send(new NetworkSend(node, ByteBufferSend.sizePrefixed(ByteBuffer.wrap(message))));
            do {
                selector.poll(0L);
            } while (selector.completedSends().isEmpty());
        }
        server.selector().unmuteAll();
        selector.close(node);
        TestUtils.waitForCondition(() ->
            bytesOut.toByteArray().length == totalSendSize, 5000, "All requests sent were not processed");
    }

    /**
     * Verifies that inter-broker listener with validation of truststore against keystore works
     * with configs including mutual authentication and hostname verification.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testInterBrokerSslConfigValidation(Args args) throws Exception {
        SecurityProtocol securityProtocol = SecurityProtocol.SSL;
        args.sslServerConfigs.put(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, "required");
        args.sslServerConfigs.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "HTTPS");
        args.sslServerConfigs.putAll(args.serverCertStores.keyStoreProps());
        args.sslServerConfigs.putAll(args.serverCertStores.trustStoreProps());
        args.sslClientConfigs.putAll(args.serverCertStores.keyStoreProps());
        args.sslClientConfigs.putAll(args.serverCertStores.trustStoreProps());
        TestSecurityConfig config = new TestSecurityConfig(args.sslServerConfigs);
        ListenerName listenerName = ListenerName.forSecurityProtocol(securityProtocol);
        ChannelBuilder serverChannelBuilder = ChannelBuilders.serverChannelBuilder(listenerName,
            true, securityProtocol, config, null, null, time, new LogContext(),
            defaultApiVersionsSupplier());
        server = new NioEchoServer(listenerName, securityProtocol, config,
                "localhost", serverChannelBuilder, null, time);
        server.start();

        this.selector = createSelector(args.sslClientConfigs, null, null, null);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect("0", addr, BUFFER_SIZE, BUFFER_SIZE);
        NetworkTestUtils.checkClientConnection(selector, "0", 100, 10);
    }

    /**
     * Verifies that inter-broker listener with validation of truststore against keystore
     * fails if certs from keystore are not trusted.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testInterBrokerSslConfigValidationFailure(Args args) {
        SecurityProtocol securityProtocol = SecurityProtocol.SSL;
        args.sslServerConfigs.put(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, "required");
        TestSecurityConfig config = new TestSecurityConfig(args.sslServerConfigs);
        ListenerName listenerName = ListenerName.forSecurityProtocol(securityProtocol);
        assertThrows(KafkaException.class, () -> ChannelBuilders.serverChannelBuilder(
            listenerName, true, securityProtocol, config,
            null, null, time, new LogContext(), defaultApiVersionsSupplier()));
    }

    /**
     * Tests reconfiguration of server keystore. Verifies that existing connections continue
     * to work with old keystore and new connections work with new keystore.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testServerKeystoreDynamicUpdate(Args args) throws Exception {
        SecurityProtocol securityProtocol = SecurityProtocol.SSL;
        TestSecurityConfig config = new TestSecurityConfig(args.sslServerConfigs);
        ListenerName listenerName = ListenerName.forSecurityProtocol(securityProtocol);
        ChannelBuilder serverChannelBuilder = ChannelBuilders.serverChannelBuilder(listenerName,
            false, securityProtocol, config, null, null, time, new LogContext(),
            defaultApiVersionsSupplier());
        server = new NioEchoServer(listenerName, securityProtocol, config,
                "localhost", serverChannelBuilder, null, time);
        server.start();
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());

        // Verify that client with matching truststore can authenticate, send and receive
        String oldNode = "0";
        Selector oldClientSelector = createSelector(args.sslClientConfigs);
        oldClientSelector.connect(oldNode, addr, BUFFER_SIZE, BUFFER_SIZE);
        NetworkTestUtils.checkClientConnection(selector, oldNode, 100, 10);

        CertStores newServerCertStores = certBuilder(true, "server", args.useInlinePem).addHostName("localhost").build();
        Map<String, Object> newKeystoreConfigs = newServerCertStores.keyStoreProps();
        assertTrue(serverChannelBuilder instanceof ListenerReconfigurable, "SslChannelBuilder not reconfigurable");
        ListenerReconfigurable reconfigurableBuilder = (ListenerReconfigurable) serverChannelBuilder;
        assertEquals(listenerName, reconfigurableBuilder.listenerName());
        reconfigurableBuilder.validateReconfiguration(newKeystoreConfigs);
        reconfigurableBuilder.reconfigure(newKeystoreConfigs);

        // Verify that new client with old truststore fails
        oldClientSelector.connect("1", addr, BUFFER_SIZE, BUFFER_SIZE);
        NetworkTestUtils.waitForChannelClose(oldClientSelector, "1", ChannelState.State.AUTHENTICATION_FAILED);

        // Verify that new client with new truststore can authenticate, send and receive
        args.sslClientConfigs = args.getTrustingConfig(args.clientCertStores, newServerCertStores);
        Selector newClientSelector = createSelector(args.sslClientConfigs);
        newClientSelector.connect("2", addr, BUFFER_SIZE, BUFFER_SIZE);
        NetworkTestUtils.checkClientConnection(newClientSelector, "2", 100, 10);

        // Verify that old client continues to work
        NetworkTestUtils.checkClientConnection(oldClientSelector, oldNode, 100, 10);

        CertStores invalidCertStores = certBuilder(true, "server", args.useInlinePem).addHostName("127.0.0.1").build();
        Map<String, Object>  invalidConfigs = args.getTrustingConfig(invalidCertStores, args.clientCertStores);
        verifyInvalidReconfigure(reconfigurableBuilder, invalidConfigs, "keystore with different SubjectAltName");

        Map<String, Object>  missingStoreConfigs = new HashMap<>();
        missingStoreConfigs.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PKCS12");
        missingStoreConfigs.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "some.keystore.path");
        missingStoreConfigs.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, new Password("some.keystore.password"));
        missingStoreConfigs.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, new Password("some.key.password"));
        verifyInvalidReconfigure(reconfigurableBuilder, missingStoreConfigs, "keystore not found");

        // Verify that new connections continue to work with the server with previously configured keystore after failed reconfiguration
        newClientSelector.connect("3", addr, BUFFER_SIZE, BUFFER_SIZE);
        NetworkTestUtils.checkClientConnection(newClientSelector, "3", 100, 10);
    }

    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testServerKeystoreDynamicUpdateWithNewSubjectAltName(Args args) throws Exception {
        SecurityProtocol securityProtocol = SecurityProtocol.SSL;
        TestSecurityConfig config = new TestSecurityConfig(args.sslServerConfigs);
        ListenerName listenerName = ListenerName.forSecurityProtocol(securityProtocol);
        ChannelBuilder serverChannelBuilder = ChannelBuilders.serverChannelBuilder(listenerName,
            false, securityProtocol, config, null, null, time, new LogContext(),
            defaultApiVersionsSupplier());
        server = new NioEchoServer(listenerName, securityProtocol, config,
            "localhost", serverChannelBuilder, null, time);
        server.start();
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());

        Selector selector = createSelector(args.sslClientConfigs);
        String node1 = "1";
        selector.connect(node1, addr, BUFFER_SIZE, BUFFER_SIZE);
        NetworkTestUtils.checkClientConnection(selector, node1, 100, 10);
        selector.close();

        TestSslUtils.CertificateBuilder certBuilder = new TestSslUtils.CertificateBuilder().sanDnsNames("localhost", "*.example.com");
        String truststorePath = (String) args.sslClientConfigs.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
        File truststoreFile = truststorePath != null ? new File(truststorePath) : null;
        TestSslUtils.SslConfigsBuilder builder = new TestSslUtils.SslConfigsBuilder(Mode.SERVER)
                .useClientCert(false)
                .certAlias("server")
                .cn("server")
                .certBuilder(certBuilder)
                .createNewTrustStore(truststoreFile)
                .usePem(args.useInlinePem);
        Map<String, Object> newConfigs = builder.build();
        Map<String, Object> newKeystoreConfigs = new HashMap<>();
        for (String propName : CertStores.KEYSTORE_PROPS) {
            newKeystoreConfigs.put(propName, newConfigs.get(propName));
        }
        ListenerReconfigurable reconfigurableBuilder = (ListenerReconfigurable) serverChannelBuilder;
        reconfigurableBuilder.validateReconfiguration(newKeystoreConfigs);
        reconfigurableBuilder.reconfigure(newKeystoreConfigs);

        for (String propName : CertStores.TRUSTSTORE_PROPS) {
            args.sslClientConfigs.put(propName, newConfigs.get(propName));
        }
        selector = createSelector(args.sslClientConfigs);
        String node2 = "2";
        selector.connect(node2, addr, BUFFER_SIZE, BUFFER_SIZE);
        NetworkTestUtils.checkClientConnection(selector, node2, 100, 10);

        TestSslUtils.CertificateBuilder invalidBuilder = new TestSslUtils.CertificateBuilder().sanDnsNames("localhost");
        if (!args.useInlinePem)
            builder.useExistingTrustStore(truststoreFile);
        Map<String, Object> invalidConfig = builder.certBuilder(invalidBuilder).build();
        Map<String, Object> invalidKeystoreConfigs = new HashMap<>();
        for (String propName : CertStores.KEYSTORE_PROPS) {
            invalidKeystoreConfigs.put(propName, invalidConfig.get(propName));
        }
        verifyInvalidReconfigure(reconfigurableBuilder, invalidKeystoreConfigs, "keystore without existing SubjectAltName");
        String node3 = "3";
        selector.connect(node3, addr, BUFFER_SIZE, BUFFER_SIZE);
        NetworkTestUtils.checkClientConnection(selector, node3, 100, 10);
    }

    /**
     * Tests reconfiguration of server truststore. Verifies that existing connections continue
     * to work with old truststore and new connections work with new truststore.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testServerTruststoreDynamicUpdate(Args args) throws Exception {
        SecurityProtocol securityProtocol = SecurityProtocol.SSL;
        args.sslServerConfigs.put(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, "required");
        TestSecurityConfig config = new TestSecurityConfig(args.sslServerConfigs);
        ListenerName listenerName = ListenerName.forSecurityProtocol(securityProtocol);
        ChannelBuilder serverChannelBuilder = ChannelBuilders.serverChannelBuilder(listenerName,
            false, securityProtocol, config, null, null, time, new LogContext(),
            defaultApiVersionsSupplier());
        server = new NioEchoServer(listenerName, securityProtocol, config,
                "localhost", serverChannelBuilder, null, time);
        server.start();
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());

        // Verify that client with matching keystore can authenticate, send and receive
        String oldNode = "0";
        Selector oldClientSelector = createSelector(args.sslClientConfigs);
        oldClientSelector.connect(oldNode, addr, BUFFER_SIZE, BUFFER_SIZE);
        NetworkTestUtils.checkClientConnection(selector, oldNode, 100, 10);

        CertStores newClientCertStores = certBuilder(true, "client", args.useInlinePem).addHostName("localhost").build();
        args.sslClientConfigs = args.getTrustingConfig(newClientCertStores, args.serverCertStores);
        Map<String, Object> newTruststoreConfigs = newClientCertStores.trustStoreProps();
        assertTrue(serverChannelBuilder instanceof ListenerReconfigurable, "SslChannelBuilder not reconfigurable");
        ListenerReconfigurable reconfigurableBuilder = (ListenerReconfigurable) serverChannelBuilder;
        assertEquals(listenerName, reconfigurableBuilder.listenerName());
        reconfigurableBuilder.validateReconfiguration(newTruststoreConfigs);
        reconfigurableBuilder.reconfigure(newTruststoreConfigs);

        // Verify that new client with old truststore fails
        oldClientSelector.connect("1", addr, BUFFER_SIZE, BUFFER_SIZE);
        NetworkTestUtils.waitForChannelClose(oldClientSelector, "1", ChannelState.State.AUTHENTICATION_FAILED);

        // Verify that new client with new truststore can authenticate, send and receive
        Selector newClientSelector = createSelector(args.sslClientConfigs);
        newClientSelector.connect("2", addr, BUFFER_SIZE, BUFFER_SIZE);
        NetworkTestUtils.checkClientConnection(newClientSelector, "2", 100, 10);

        // Verify that old client continues to work
        NetworkTestUtils.checkClientConnection(oldClientSelector, oldNode, 100, 10);

        Map<String, Object>  invalidConfigs = new HashMap<>(newTruststoreConfigs);
        invalidConfigs.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "INVALID_TYPE");
        verifyInvalidReconfigure(reconfigurableBuilder, invalidConfigs, "invalid truststore type");

        Map<String, Object>  missingStoreConfigs = new HashMap<>();
        missingStoreConfigs.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PKCS12");
        missingStoreConfigs.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "some.truststore.path");
        missingStoreConfigs.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, new Password("some.truststore.password"));
        verifyInvalidReconfigure(reconfigurableBuilder, missingStoreConfigs, "truststore not found");

        // Verify that new connections continue to work with the server with previously configured keystore after failed reconfiguration
        newClientSelector.connect("3", addr, BUFFER_SIZE, BUFFER_SIZE);
        NetworkTestUtils.checkClientConnection(newClientSelector, "3", 100, 10);
    }

    /**
     * Tests if client can plugin customize ssl.engine.factory
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testCustomClientSslEngineFactory(Args args) throws Exception {
        args.sslClientConfigs.put(SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG, TestSslUtils.TestSslEngineFactory.class);
        verifySslConfigs(args);
    }

    /**
     * Tests if server can plugin customize ssl.engine.factory
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testCustomServerSslEngineFactory(Args args) throws Exception {
        args.sslServerConfigs.put(SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG, TestSslUtils.TestSslEngineFactory.class);
        verifySslConfigs(args);
    }

    /**
     * Tests if client and server both can plugin customize ssl.engine.factory and talk to each other!
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testCustomClientAndServerSslEngineFactory(Args args) throws Exception {
        args.sslClientConfigs.put(SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG, TestSslUtils.TestSslEngineFactory.class);
        args.sslServerConfigs.put(SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG, TestSslUtils.TestSslEngineFactory.class);
        verifySslConfigs(args);
    }

    /**
     * Tests invalid ssl.engine.factory plugin class
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider.class)
    public void testInvalidSslEngineFactory(Args args) {
        args.sslClientConfigs.put(SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG, String.class);
        assertThrows(KafkaException.class, () -> createSelector(args.sslClientConfigs));
    }

    private void verifyInvalidReconfigure(ListenerReconfigurable reconfigurable,
                                          Map<String, Object>  invalidConfigs, String errorMessage) {
        assertThrows(KafkaException.class, () -> reconfigurable.validateReconfiguration(invalidConfigs));
        assertThrows(KafkaException.class, () -> reconfigurable.reconfigure(invalidConfigs));
    }

    private Selector createSelector(Map<String, Object> sslClientConfigs) {
        return createSelector(sslClientConfigs, null, null, null);
    }

    private Selector createSelector(Map<String, Object> sslClientConfigs, final Integer netReadBufSize,
                                final Integer netWriteBufSize, final Integer appBufSize) {
        TestSslChannelBuilder channelBuilder = new TestSslChannelBuilder(Mode.CLIENT);
        channelBuilder.configureBufferSizes(netReadBufSize, netWriteBufSize, appBufSize);
        channelBuilder.configure(sslClientConfigs);
        this.selector = new Selector(100 * 5000, new Metrics(), time, "MetricGroup", channelBuilder, new LogContext());
        return selector;
    }

    private NioEchoServer createEchoServer(Args args, ListenerName listenerName, SecurityProtocol securityProtocol) throws Exception {
        return NetworkTestUtils.createEchoServer(listenerName, securityProtocol, new TestSecurityConfig(args.sslServerConfigs), null, time);
    }

    private NioEchoServer createEchoServer(Args args, SecurityProtocol securityProtocol) throws Exception {
        return createEchoServer(args, ListenerName.forSecurityProtocol(securityProtocol), securityProtocol);
    }

    private Selector createSelector(Args args) {
        LogContext logContext = new LogContext();
        ChannelBuilder channelBuilder = new SslChannelBuilder(Mode.CLIENT, null, false, logContext);
        channelBuilder.configure(args.sslClientConfigs);
        selector = new Selector(5000, new Metrics(), time, "MetricGroup", channelBuilder, logContext);
        return selector;
    }

    private void verifySslConfigs(Args args) throws Exception {
        server = createEchoServer(args, SecurityProtocol.SSL);
        createSelector(args.sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        String node = "0";
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);
        NetworkTestUtils.checkClientConnection(selector, node, 100, 10);
    }

    private void verifySslConfigsWithHandshakeFailure(Args args) throws Exception {
        verifySslConfigsWithHandshakeFailure(args, 0);
    }

    private void verifySslConfigsWithHandshakeFailure(Args args, int pollDelayMs) throws Exception {
        server = createEchoServer(args, SecurityProtocol.SSL);
        createSelector(args.sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        String node = "0";
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);
        NetworkTestUtils.waitForChannelClose(selector, node, ChannelState.State.AUTHENTICATION_FAILED, pollDelayMs);
        server.verifyAuthenticationMetrics(0, 1);
    }

    private static CertStores.Builder certBuilder(boolean isServer, String cn, boolean useInlinePem) {
        return new CertStores.Builder(isServer)
                .cn(cn)
                .usePem(useInlinePem);
    }

    @FunctionalInterface
    private interface FailureAction {
        FailureAction NO_OP = () -> { };
        FailureAction THROW_IO_EXCEPTION = () -> {
            throw new IOException("Test IO exception");
        };
        void run() throws IOException;
    }

    private Supplier<ApiVersionsResponse> defaultApiVersionsSupplier() {
        return () -> TestUtils.defaultApiVersionsResponse(ApiMessageType.ListenerType.ZK_BROKER);
    }

    static class TestSslChannelBuilder extends SslChannelBuilder {

        private Integer netReadBufSizeOverride;
        private Integer netWriteBufSizeOverride;
        private Integer appBufSizeOverride;
        private long failureIndex = Long.MAX_VALUE;
        FailureAction readFailureAction = FailureAction.NO_OP;
        FailureAction flushFailureAction = FailureAction.NO_OP;
        int flushDelayCount = 0;

        public TestSslChannelBuilder(Mode mode) {
            super(mode, null, false, new LogContext());
        }

        public void configureBufferSizes(Integer netReadBufSize, Integer netWriteBufSize, Integer appBufSize) {
            this.netReadBufSizeOverride = netReadBufSize;
            this.netWriteBufSizeOverride = netWriteBufSize;
            this.appBufSizeOverride = appBufSize;
        }

        @Override
        protected SslTransportLayer buildTransportLayer(SslFactory sslFactory, String id, SelectionKey key,
                                                        ChannelMetadataRegistry metadataRegistry) throws IOException {
            SocketChannel socketChannel = (SocketChannel) key.channel();
            SSLEngine sslEngine = sslFactory.createSslEngine(socketChannel.socket());
            return newTransportLayer(id, key, sslEngine);
        }

        protected TestSslTransportLayer newTransportLayer(String id, SelectionKey key, SSLEngine sslEngine) throws IOException {
            return new TestSslTransportLayer(id, key, sslEngine);
        }

        /**
         * SSLTransportLayer with overrides for testing including:
         * <ul>
         * <li>Overrides for packet and application buffer size to test buffer resize code path.
         * The overridden buffer size starts with a small value and increases in size when the buffer size
         * is retrieved to handle overflow/underflow, until the actual session buffer size is reached.</li>
         * <li>IOException injection for reads and writes for testing exception handling during handshakes.</li>
         * <li>Delayed writes to test handshake failure notifications to peer</li>
         * </ul>
         */
        class TestSslTransportLayer extends SslTransportLayer {

            private final ResizeableBufferSize netReadBufSize;
            private final ResizeableBufferSize netWriteBufSize;
            private final ResizeableBufferSize appBufSize;
            private final AtomicLong numReadsRemaining;
            private final AtomicLong numFlushesRemaining;
            private final AtomicInteger numDelayedFlushesRemaining;

            public TestSslTransportLayer(String channelId, SelectionKey key, SSLEngine sslEngine) {
                super(channelId, key, sslEngine, new DefaultChannelMetadataRegistry());
                this.netReadBufSize = new ResizeableBufferSize(netReadBufSizeOverride);
                this.netWriteBufSize = new ResizeableBufferSize(netWriteBufSizeOverride);
                this.appBufSize = new ResizeableBufferSize(appBufSizeOverride);
                numReadsRemaining = new AtomicLong(failureIndex);
                numFlushesRemaining = new AtomicLong(failureIndex);
                numDelayedFlushesRemaining = new AtomicInteger(flushDelayCount);
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

            @Override
            protected int readFromSocketChannel() throws IOException {
                if (numReadsRemaining.decrementAndGet() == 0 && !ready())
                    readFailureAction.run();
                return super.readFromSocketChannel();
            }

            @Override
            protected boolean flush(ByteBuffer buf) throws IOException {
                if (!buf.hasRemaining())
                    return super.flush(buf);
                if (numFlushesRemaining.decrementAndGet() == 0 && !ready())
                    flushFailureAction.run();
                else if (numDelayedFlushesRemaining.getAndDecrement() != 0)
                    return false;
                resetDelayedFlush();
                return super.flush(buf);
            }

            @Override
            protected void startHandshake() throws IOException {
                assertTrue(socketChannel().isConnected(), "SSL handshake initialized too early");
                super.startHandshake();
            }

            private void resetDelayedFlush() {
                numDelayedFlushesRemaining.set(flushDelayCount);
            }
        }

        static class ResizeableBufferSize {
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
