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

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.test.TestSslUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledForJreRange;
import org.junit.jupiter.api.condition.JRE;

@EnabledForJreRange(min = JRE.JAVA_11) // TLS 1.3 is only supported with Java 11 and newer
public class Tls13SelectorTest extends SslSelectorTest {

    @Override
    protected Map<String, Object> createSslClientConfigs(File trustStoreFile) throws GeneralSecurityException, IOException {
        Map<String, Object> configs = TestSslUtils.createSslConfig(false, false, Mode.CLIENT,
            trustStoreFile, "client");
        configs.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, asList("TLSv1.3"));
        return configs;
    }

    /**
     * TLS 1.3 has a post-handshake key and IV update, which will update the sending and receiving keys
     * for one side of the connection.
     *
     * Key Usage Limits will trigger an update when the algorithm limits are reached, but the default
     * value is too large (2^37 bytes of plaintext data) for a unit test. This value can be overridden
     * via the security property `jdk.tls.keyLimits`, but that's also difficult to achieve in a unit
     * test.
     *
     * Applications can also trigger an update by calling `SSLSocket.startHandshake()` or
     * `SSLEngine.beginHandshake()` (this would trigger `renegotiation` with TLS 1.2) and that's the
     * approach we take here.
     */
    @Test
    public void testKeyUpdate() throws Exception {
        String node = "0";
        // create connections
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port);
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);
        NetworkTestUtils.waitForChannelReady(selector, node);

        // send echo requests and receive responses
        selector.send(createSend(node, node + "-" + 0));
        selector.poll(0L);
        server.renegotiate();
        selector.send(createSend(node,  node + "-" + 1));
        List<NetworkReceive> received = new ArrayList<>();
        TestUtils.waitForCondition(() -> {
            try {
                selector.poll(1000L);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            for (NetworkReceive receive : selector.completedReceives()) {
                if (receive.source().equals(node))
                    received.add(receive);
            }
            return received.size() == 2;
        }, "Expected two receives, got " + received.size());

        assertEquals(asList("0-0", "0-1"), received.stream().map(this::asString).collect(Collectors.toList()));
    }
}
