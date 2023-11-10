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

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.security.TestSecurityConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Java;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests for the SSL transport layer.
 * Checks different versions of the protocol usage on the server and client.
 */
public class SslVersionsTransportLayerTest {
    private static final int BUFFER_SIZE = 4 * 1024;
    private static final Time TIME = Time.SYSTEM;

    public static Stream<Arguments> parameters() {
        List<Arguments> parameters = new ArrayList<>();

        parameters.add(Arguments.of(Collections.singletonList("TLSv1.2"), Collections.singletonList("TLSv1.2")));

        if (Java.IS_JAVA11_COMPATIBLE) {
            parameters.add(Arguments.of(Collections.singletonList("TLSv1.2"), Collections.singletonList("TLSv1.3")));
            parameters.add(Arguments.of(Collections.singletonList("TLSv1.3"), Collections.singletonList("TLSv1.2")));
            parameters.add(Arguments.of(Collections.singletonList("TLSv1.3"), Collections.singletonList("TLSv1.3")));
            parameters.add(Arguments.of(Collections.singletonList("TLSv1.2"), Arrays.asList("TLSv1.2", "TLSv1.3")));
            parameters.add(Arguments.of(Collections.singletonList("TLSv1.2"), Arrays.asList("TLSv1.3", "TLSv1.2")));
            parameters.add(Arguments.of(Collections.singletonList("TLSv1.3"), Arrays.asList("TLSv1.2", "TLSv1.3")));
            parameters.add(Arguments.of(Collections.singletonList("TLSv1.3"), Arrays.asList("TLSv1.3", "TLSv1.2")));
            parameters.add(Arguments.of(Arrays.asList("TLSv1.3", "TLSv1.2"), Collections.singletonList("TLSv1.3")));
            parameters.add(Arguments.of(Arrays.asList("TLSv1.3", "TLSv1.2"), Collections.singletonList("TLSv1.2")));
            parameters.add(Arguments.of(Arrays.asList("TLSv1.3", "TLSv1.2"), Arrays.asList("TLSv1.2", "TLSv1.3")));
            parameters.add(Arguments.of(Arrays.asList("TLSv1.3", "TLSv1.2"), Arrays.asList("TLSv1.3", "TLSv1.2")));
            parameters.add(Arguments.of(Arrays.asList("TLSv1.2", "TLSv1.3"), Collections.singletonList("TLSv1.3")));
            parameters.add(Arguments.of(Arrays.asList("TLSv1.2", "TLSv1.3"), Collections.singletonList("TLSv1.2")));
            parameters.add(Arguments.of(Arrays.asList("TLSv1.2", "TLSv1.3"), Arrays.asList("TLSv1.2", "TLSv1.3")));
            parameters.add(Arguments.of(Arrays.asList("TLSv1.2", "TLSv1.3"), Arrays.asList("TLSv1.3", "TLSv1.2")));
        }

        return parameters.stream();
    }

    /**
     * Tests that connection success with the default TLS version.
     * Note that debug mode for javax.net.ssl can be enabled via {@code System.setProperty("javax.net.debug", "ssl:handshake");}
     */
    @ParameterizedTest(name = "tlsServerProtocol = {0}, tlsClientProtocol = {1}")
    @MethodSource("parameters")
    public void testTlsDefaults(List<String> serverProtocols, List<String> clientProtocols) throws Exception {
        // Create certificates for use by client and server. Add server cert to client truststore and vice versa.
        CertStores serverCertStores = new CertStores(true, "server",  "localhost");
        CertStores clientCertStores = new CertStores(false, "client", "localhost");

        Map<String, Object> sslClientConfigs = getTrustingConfig(clientCertStores, serverCertStores, clientProtocols);
        Map<String, Object> sslServerConfigs = getTrustingConfig(serverCertStores, clientCertStores, serverProtocols);

        NioEchoServer server = NetworkTestUtils.createEchoServer(ListenerName.forSecurityProtocol(SecurityProtocol.SSL),
            SecurityProtocol.SSL,
            new TestSecurityConfig(sslServerConfigs),
            null,
            TIME);
        Selector selector = createClientSelector(sslClientConfigs);

        String node = "0";
        selector.connect(node, new InetSocketAddress("localhost", server.port()), BUFFER_SIZE, BUFFER_SIZE);

        if (isCompatible(serverProtocols, clientProtocols)) {
            NetworkTestUtils.waitForChannelReady(selector, node);

            int msgSz = 1024 * 1024;
            String message = TestUtils.randomString(msgSz);
            selector.send(new NetworkSend(node, ByteBufferSend.sizePrefixed(ByteBuffer.wrap(message.getBytes()))));
            while (selector.completedReceives().isEmpty()) {
                selector.poll(100L);
            }
            int totalBytes = msgSz + 4; // including 4-byte size
            server.waitForMetric("incoming-byte", totalBytes);
            server.waitForMetric("outgoing-byte", totalBytes);
            server.waitForMetric("request", 1);
            server.waitForMetric("response", 1);
        } else {
            NetworkTestUtils.waitForChannelClose(selector, node, ChannelState.State.AUTHENTICATION_FAILED);
            server.verifyAuthenticationMetrics(0, 1);
        }
        server.close();
        selector.close();
    }

    /**
     * <p>
     * The explanation of this check in the structure of the ClientHello SSL message.
     * Please, take a look at the <a href="https://docs.oracle.com/en/java/javase/11/security/java-secure-socket-extension-jsse-reference-guide.html#GUID-4D421910-C36D-40A2-8BA2-7D42CCBED3C6">Guide</a>,
     * "Send ClientHello Message" section.
     * <p>
     * > Client version: For TLS 1.3, this has a fixed value, TLSv1.2; TLS 1.3 uses the extension supported_versions and not this field to negotiate protocol version
     * ...
     * > supported_versions: Lists which versions of TLS the client supports. In particular, if the client
     * > requests TLS 1.3, then the client version field has the value TLSv1.2 and this extension
     * > contains the value TLSv1.3; if the client requests TLS 1.2, then the client version field has the
     * > value TLSv1.2 and this extension either doesn’t exist or contains the value TLSv1.2 but not the value TLSv1.3.
     * <p>
     *
     * This mean that TLSv1.3 client can fallback to TLSv1.2 but TLSv1.2 client can't change protocol to TLSv1.3.
     *
     * @param serverProtocols Server protocols. Expected to be non empty.
     * @param clientProtocols Client protocols. Expected to be non empty.
     * @return {@code true} if client should be able to connect to the server.
     */
    private boolean isCompatible(List<String> serverProtocols, List<String> clientProtocols) {
        assertNotNull(serverProtocols);
        assertFalse(serverProtocols.isEmpty());
        assertNotNull(clientProtocols);
        assertFalse(clientProtocols.isEmpty());

        return serverProtocols.contains(clientProtocols.get(0)) ||
            (clientProtocols.get(0).equals("TLSv1.3") && !Collections.disjoint(serverProtocols, clientProtocols));
    }

    private static Map<String, Object> getTrustingConfig(CertStores certStores, CertStores peerCertStores, List<String> tlsProtocols) {
        Map<String, Object> configs = certStores.getTrustingConfig(peerCertStores);
        configs.putAll(sslConfig(tlsProtocols));
        return configs;
    }

    private static Map<String, Object> sslConfig(List<String> tlsProtocols) {
        Map<String, Object> sslConfig = new HashMap<>();
        sslConfig.put(SslConfigs.SSL_PROTOCOL_CONFIG, tlsProtocols.get(0));
        sslConfig.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, tlsProtocols);
        return sslConfig;
    }

    private Selector createClientSelector(Map<String, Object> sslClientConfigs) {
        SslTransportLayerTest.TestSslChannelBuilder channelBuilder =
            new SslTransportLayerTest.TestSslChannelBuilder(Mode.CLIENT);
        channelBuilder.configureBufferSizes(null, null, null);
        channelBuilder.configure(sslClientConfigs);
        return new Selector(100 * 5000, new Metrics(), TIME, "MetricGroup", channelBuilder, new LogContext());
    }
}
