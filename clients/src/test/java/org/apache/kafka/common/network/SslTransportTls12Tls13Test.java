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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.security.TestSecurityConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Java;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assume.assumeTrue;

public class SslTransportTls12Tls13Test {
    private static final int BUFFER_SIZE = 4 * 1024;
    private static final Time TIME = Time.SYSTEM;

    private NioEchoServer server;
    private Selector selector;
    private Map<String, Object> sslClientConfigs;
    private Map<String, Object> sslServerConfigs;

    @Before
    public void setup() throws Exception {
        // Create certificates for use by client and server. Add server cert to client truststore and vice versa.
        CertStores serverCertStores = new CertStores(true, "server", "localhost");
        CertStores clientCertStores = new CertStores(false, "client", "localhost");
        sslServerConfigs = serverCertStores.getTrustingConfig(clientCertStores);
        sslClientConfigs = clientCertStores.getTrustingConfig(serverCertStores);

        LogContext logContext = new LogContext();
        ChannelBuilder channelBuilder = new SslChannelBuilder(Mode.CLIENT, null, false, logContext);
        channelBuilder.configure(sslClientConfigs);
        this.selector = new Selector(5000, new Metrics(), TIME, "MetricGroup", channelBuilder, logContext);
    }

    @After
    public void teardown() throws Exception {
        if (selector != null)
            this.selector.close();
        if (server != null)
            this.server.close();
    }

    /**
     * Tests that connections fails if TLSv1.3 enabled but cipher suite suitable only for TLSv1.2 used.
     */
    @Test
    public void testCiphersSuiteForTls12FailsForTls13() throws Exception {
        assumeTrue(Java.IS_JAVA11_COMPATIBLE);

        String cipherSuite = "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384";

        sslServerConfigs.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, Collections.singletonList("TLSv1.3"));
        sslServerConfigs.put(SslConfigs.SSL_CIPHER_SUITES_CONFIG, Collections.singletonList(cipherSuite));
        server = NetworkTestUtils.createEchoServer(ListenerName.forSecurityProtocol(SecurityProtocol.SSL),
            SecurityProtocol.SSL, new TestSecurityConfig(sslServerConfigs), null, TIME);

        sslClientConfigs.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, Collections.singletonList("TLSv1.3"));
        sslClientConfigs.put(SslConfigs.SSL_CIPHER_SUITES_CONFIG, Collections.singletonList(cipherSuite));

        checkAuthentiationFailed();
    }

    /**
     * Tests that connections can't be made if server uses TLSv1.2 with custom cipher suite and client uses TLSv1.3.
     */
    @Test
    public void testCiphersSuiteFailForServerTls12ClientTls13() throws Exception {
        assumeTrue(Java.IS_JAVA11_COMPATIBLE);

        String tls12CipherSuite = "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384";
        String tls13CipherSuite = "TLS_AES_128_GCM_SHA256";

        sslServerConfigs.put(SslConfigs.SSL_PROTOCOL_CONFIG, "TLSv1.2");
        sslServerConfigs.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, Collections.singletonList("TLSv1.2"));
        sslServerConfigs.put(SslConfigs.SSL_CIPHER_SUITES_CONFIG, Collections.singletonList(tls12CipherSuite));
        server = NetworkTestUtils.createEchoServer(ListenerName.forSecurityProtocol(SecurityProtocol.SSL),
            SecurityProtocol.SSL, new TestSecurityConfig(sslServerConfigs), null, TIME);

        sslClientConfigs.put(SslConfigs.SSL_PROTOCOL_CONFIG, "TLSv1.3");
        sslClientConfigs.put(SslConfigs.SSL_CIPHER_SUITES_CONFIG, Collections.singletonList(tls13CipherSuite));

        checkAuthentiationFailed();
    }

    /**
     * Tests that connections can be made with TLSv1.3 cipher suite.
     */
    @Test
    public void testCiphersSuiteForTls13() throws Exception {
        assumeTrue(Java.IS_JAVA11_COMPATIBLE);

        String cipherSuite = "TLS_AES_128_GCM_SHA256";

        sslServerConfigs.put(SslConfigs.SSL_CIPHER_SUITES_CONFIG, Collections.singletonList(cipherSuite));
        server = NetworkTestUtils.createEchoServer(ListenerName.forSecurityProtocol(SecurityProtocol.SSL),
            SecurityProtocol.SSL, new TestSecurityConfig(sslServerConfigs), null, TIME);

        sslClientConfigs.put(SslConfigs.SSL_CIPHER_SUITES_CONFIG, Collections.singletonList(cipherSuite));
        checkAuthenticationSucceed();
    }

    /**
     * Tests that connections can be made with TLSv1.2 cipher suite.
     */
    @Test
    public void testCiphersSuiteForTls12() throws Exception {
        String cipherSuite = "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384";

        sslServerConfigs.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, Arrays.asList(SslConfigs.DEFAULT_SSL_ENABLED_PROTOCOLS.split(",")));
        sslServerConfigs.put(SslConfigs.SSL_CIPHER_SUITES_CONFIG, Collections.singletonList(cipherSuite));
        server = NetworkTestUtils.createEchoServer(ListenerName.forSecurityProtocol(SecurityProtocol.SSL),
            SecurityProtocol.SSL, new TestSecurityConfig(sslServerConfigs), null, TIME);

        sslClientConfigs.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, Arrays.asList(SslConfigs.DEFAULT_SSL_ENABLED_PROTOCOLS.split(",")));
        sslClientConfigs.put(SslConfigs.SSL_CIPHER_SUITES_CONFIG, Collections.singletonList(cipherSuite));
        checkAuthenticationSucceed();
    }

    /** Checks connection failed using the specified {@code tlsVersion}. */
    private void checkAuthentiationFailed() throws IOException, InterruptedException {
        sslClientConfigs.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, Arrays.asList("TLSv1.3"));
        createSelector(sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect("0", addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.waitForChannelClose(selector, "0", ChannelState.State.AUTHENTICATION_FAILED);
        server.verifyAuthenticationMetrics(0, 1);
    }

    private void checkAuthenticationSucceed() throws IOException, InterruptedException {
        createSelector(sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect("0", addr, BUFFER_SIZE, BUFFER_SIZE);
        NetworkTestUtils.waitForChannelReady(selector, "0");
        server.verifyAuthenticationMetrics(1, 0);
    }

    private void createSelector(Map<String, Object> sslClientConfigs) {
        SslTransportLayerTest.TestSslChannelBuilder channelBuilder = new SslTransportLayerTest.TestSslChannelBuilder(Mode.CLIENT);
        channelBuilder.configureBufferSizes(null, null, null);
        channelBuilder.configure(sslClientConfigs);
        this.selector = new Selector(100 * 5000, new Metrics(), TIME, "MetricGroup", channelBuilder, new LogContext());
    }
}
