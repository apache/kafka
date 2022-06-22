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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.test.TestSslUtils;
import org.junit.jupiter.api.Test;

public class Tls12SelectorTest extends SslSelectorTest {

    @Override
    protected Map<String, Object> createSslClientConfigs(File trustStoreFile)
        throws GeneralSecurityException, IOException {
        Map<String, Object> configs = TestSslUtils.createSslConfig(false, false, Mode.CLIENT,
            trustStoreFile, "client");
        configs.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, asList("TLSv1.2"));
        return configs;
    }

    /**
     * Renegotiation is not supported when TLS 1.2 is used (renegotiation was removed from TLS 1.3)
     */
    @Test
    public void testRenegotiationFails() throws Exception {
        String node = "0";
        // create connections
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port);
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);
        NetworkTestUtils.waitForChannelReady(selector, node);

        // send echo requests and receive responses
        selector.send(createSend(node, node + "-" + 0));
        selector.poll(0L);
        server.renegotiate();
        selector.send(createSend(node, node + "-" + 1));
        long expiryTime = System.currentTimeMillis() + 2000;

        List<String> disconnected = new ArrayList<>();
        while (!disconnected.contains(node) && System.currentTimeMillis() < expiryTime) {
            selector.poll(10);
            disconnected.addAll(selector.disconnected().keySet());
        }
        assertTrue(disconnected.contains(node), "Renegotiation should cause disconnection");
    }
}
