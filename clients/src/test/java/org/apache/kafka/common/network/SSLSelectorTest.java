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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;

import org.apache.kafka.common.config.SecurityConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.TestSSLUtils;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * A set of tests for the selector over ssl. These use a test harness that runs a simple socket server that echos back responses.
 */

public class SSLSelectorTest {

    private static final int BUFFER_SIZE = 4 * 1024;

    private EchoServer server;
    private Selectable selector;

    @Before
    public void setup() throws Exception {
        SecurityConfig serverSecurityConfig = TestSSLUtils.createSSLConfigFile(SSLFactory.Mode.SERVER, null);
        this.server = new EchoServer(serverSecurityConfig);
        this.server.start();
        String trustStoreServer = serverSecurityConfig.getString(SecurityConfig.SSL_TRUSTSTORE_LOCATION_CONFIG);
        SecurityConfig clientSecurityConfig = TestSSLUtils.createSSLConfigFile(SSLFactory.Mode.CLIENT, trustStoreServer);
        this.selector = new Selector(new Metrics(), new MockTime(), "MetricGroup", new LinkedHashMap<String, String>(), clientSecurityConfig);
    }

    @After
    public void teardown() throws Exception {
        this.selector.close();
        this.server.close();
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

    private String blockingRequest(int node, String s) throws IOException {
        selector.send(createSend(node, s));
        selector.poll(1000L);
        while (true) {
            selector.poll(1000L);
            for (NetworkReceive receive : selector.completedReceives())
                if (receive.source() == node)
                    return asString(receive);
        }
    }

    private String asString(NetworkReceive receive) {
        return new String(Utils.toArray(receive.payload()));
    }

    private NetworkSend createSend(int node, String s) {
        return new NetworkSend(node, ByteBuffer.wrap(s.getBytes()));
    }

    /* connect and wait for the connection to complete */
    private void blockingConnect(int node) throws IOException {
        selector.connect(node, new InetSocketAddress("localhost", server.port), BUFFER_SIZE, BUFFER_SIZE);
        while (!selector.connected().contains(node))
            selector.poll(10000L);
    }

}