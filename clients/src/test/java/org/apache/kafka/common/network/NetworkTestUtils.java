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

import java.util.HashMap;
import java.util.Map;
import java.io.IOException;
import java.io.File;
import java.nio.ByteBuffer;

import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestSslUtils;
import org.apache.kafka.test.TestUtils;

/**
 * Common utility functions used by transport layer and authenticator tests.
 */

public class NetworkTestUtils {
    
    
    public static NioEchoServer createEchoServer(SecurityProtocol securityProtocol, Map<String, Object> saslServerConfigs) throws Exception {
        NioEchoServer server = new NioEchoServer(securityProtocol, saslServerConfigs, "localhost");
        server.start();
        return server;
    }  

    public static void checkClientConnection(Selector selector, String node, int minMessageSize, int messageCount) throws Exception {

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
    
    public static void waitForChannelClose(Selector selector, String node) throws IOException {
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
    
    public static class CertStores {
        
        Map<String, Object> sslConfig;
        
        public CertStores(boolean server) throws Exception {
            String name = server ? "server" : "client";
            Mode mode = server ? Mode.SERVER : Mode.CLIENT;
            File truststoreFile = File.createTempFile(name + "TS", ".jks");
            sslConfig = TestSslUtils.createSslConfig(!server, true, mode, truststoreFile, name);
            sslConfig.put(SslConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG, Class.forName(SslConfigs.DEFAULT_PRINCIPAL_BUILDER_CLASS));
        }
       
        public Map<String, Object> getTrustingConfig(CertStores truststoreConfig) {
            Map<String, Object> config = new HashMap<String, Object>(sslConfig);
            config.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststoreConfig.sslConfig.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
            config.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truststoreConfig.sslConfig.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG));
            config.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, truststoreConfig.sslConfig.get(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG));
            return config;
        }
        
        public Map<String, Object> getUntrustingConfig() {
            return sslConfig;
        }
    }
}
