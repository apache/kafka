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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.security.TestSecurityConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Java;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests for the SSL transport layer.
 * Checks different versions of the protocol usage on the server and client.
 */
@RunWith(value = Parameterized.class)
public class SslVersionsTransportLayerTest {
    private static final int BUFFER_SIZE = 4 * 1024;
    private static final Time TIME = Time.SYSTEM;

    private final List<String> tlsServerProtocols;
    private final List<String> tlsClientProtocols;

    @Parameterized.Parameters(name = "tlsServerProtocol={0},tlsClientProtocol={1}")
    public static Collection<Object[]> data() {
        List<Object[]> values = new ArrayList<>();
        values.add(new Object[] {Collections.singletonList("TLSv1.2"), Collections.singletonList("TLSv1.2")});
        if (Java.IS_JAVA11_COMPATIBLE) {
            values.add(new Object[] {Collections.singletonList("TLSv1.2"), Collections.singletonList("TLSv1.3")});
            values.add(new Object[] {Collections.singletonList("TLSv1.3"), Collections.singletonList("TLSv1.2")});
            values.add(new Object[] {Collections.singletonList("TLSv1.3"), Collections.singletonList("TLSv1.3")});
            values.add(new Object[] {Collections.singletonList("TLSv1.2"), Arrays.asList("TLSv1.2", "TLSv1.3")});
            values.add(new Object[] {Collections.singletonList("TLSv1.2"), Arrays.asList("TLSv1.3", "TLSv1.2")});
            values.add(new Object[] {Collections.singletonList("TLSv1.3"), Arrays.asList("TLSv1.2", "TLSv1.3")});
            values.add(new Object[] {Collections.singletonList("TLSv1.3"), Arrays.asList("TLSv1.3", "TLSv1.2")});
            values.add(new Object[] {Arrays.asList("TLSv1.3", "TLSv1.2"), Collections.singletonList("TLSv1.3")});
            values.add(new Object[] {Arrays.asList("TLSv1.3", "TLSv1.2"), Collections.singletonList("TLSv1.2")});
            values.add(new Object[] {Arrays.asList("TLSv1.3", "TLSv1.2"), Arrays.asList("TLSv1.2", "TLSv1.3")});
            values.add(new Object[] {Arrays.asList("TLSv1.3", "TLSv1.2"), Arrays.asList("TLSv1.3", "TLSv1.2")});
            values.add(new Object[] {Arrays.asList("TLSv1.2", "TLSv1.3"), Collections.singletonList("TLSv1.3")});
            values.add(new Object[] {Arrays.asList("TLSv1.2", "TLSv1.3"), Collections.singletonList("TLSv1.2")});
            values.add(new Object[] {Arrays.asList("TLSv1.2", "TLSv1.3"), Arrays.asList("TLSv1.2", "TLSv1.3")});
            values.add(new Object[] {Arrays.asList("TLSv1.2", "TLSv1.3"), Arrays.asList("TLSv1.3", "TLSv1.2")});

        }
        return values;
    }

    public SslVersionsTransportLayerTest(List<String> tlsServerProtocols, List<String> tlsClientProtocols) {
        this.tlsServerProtocols = tlsServerProtocols;
        this.tlsClientProtocols = tlsClientProtocols;
    }

    /**
     * Tests that connection success with the default TLS version.
     */
    @Test
    public void testTlsDefaults() throws Exception {
        // Create certificates for use by client and server. Add server cert to client truststore and vice versa.
        CertStores serverCertStores = new CertStores(true, "server",  "localhost");
        CertStores clientCertStores = new CertStores(false, "client", "localhost");

        Map<String, Object> sslClientConfigs = getTrustingConfig(clientCertStores, serverCertStores, tlsClientProtocols);
        Map<String, Object> sslServerConfigs = getTrustingConfig(serverCertStores, clientCertStores, tlsServerProtocols);

        NioEchoServer server = NetworkTestUtils.createEchoServer(ListenerName.forSecurityProtocol(SecurityProtocol.SSL),
             SecurityProtocol.SSL,
             new TestSecurityConfig(sslServerConfigs),
             null,
            TIME);
        Selector selector = createSelector(sslClientConfigs);

        String node = "0";
        selector.connect(node, new InetSocketAddress("localhost", server.port()), BUFFER_SIZE, BUFFER_SIZE);

        if (!Collections.disjoint(tlsServerProtocols, tlsClientProtocols)) {
            NetworkTestUtils.waitForChannelReady(selector, node);

            int msgSz = 1024 * 1024;
            String message = TestUtils.randomString(msgSz);
            selector.send(new NetworkSend(node, ByteBuffer.wrap(message.getBytes())));
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
        }
    }

    private static Map<String, Object> getTrustingConfig(CertStores certStores, CertStores peerCertStores, List<String> tlsProtocols) {
        Map<String, Object> configs = certStores.getTrustingConfig(peerCertStores);
        configs.putAll(sslConfig(tlsProtocols));
        return configs;
    }

    private static Map<String, Object> sslConfig(List<String> tlsServerProtocols) {
        Map<String, Object> sslConfig = new HashMap<>();
        sslConfig.put(SslConfigs.SSL_PROTOCOL_CONFIG, tlsServerProtocols.get(0));
        sslConfig.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, tlsServerProtocols);
        return sslConfig;
    }

    private Selector createSelector(Map<String, Object> sslClientConfigs) {
        SslTransportLayerTest.TestSslChannelBuilder channelBuilder = new SslTransportLayerTest.TestSslChannelBuilder(Mode.CLIENT);
        channelBuilder.configureBufferSizes(null, null, null);
        channelBuilder.configure(sslClientConfigs);
        return new Selector(100 * 5000, new Metrics(), TIME, "MetricGroup", channelBuilder, new LogContext());
    }
}
