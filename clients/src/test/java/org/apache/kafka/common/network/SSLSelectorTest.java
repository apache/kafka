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

import java.util.LinkedHashMap;
import java.util.Map;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.config.SSLConfigs;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.TestSSLUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * A set of tests for the selector. These use a test harness that runs a simple socket server that echos back responses.
 */
public class SSLSelectorTest extends SelectorTest {

    private Metrics metrics;

    @Before
    public void setup() throws Exception {
        File trustStoreFile = File.createTempFile("truststore", ".jks");

        Map<String, Object> sslServerConfigs = TestSSLUtils.createSSLConfig(false, true, Mode.SERVER, trustStoreFile, "server");
        sslServerConfigs.put(SSLConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG, Class.forName(SSLConfigs.DEFAULT_PRINCIPAL_BUILDER_CLASS));
        this.server = new EchoServer(sslServerConfigs);
        this.server.start();
        this.time = new MockTime();
        Map<String, Object> sslClientConfigs = TestSSLUtils.createSSLConfig(false, false, Mode.SERVER, trustStoreFile, "client");
        sslClientConfigs.put(SSLConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG, Class.forName(SSLConfigs.DEFAULT_PRINCIPAL_BUILDER_CLASS));

        this.channelBuilder = new SSLChannelBuilder(Mode.CLIENT);
        this.channelBuilder.configure(sslClientConfigs);
        this.metrics = new Metrics();
        this.selector = new Selector(5000, metrics, time, "MetricGroup", new LinkedHashMap<String, String>(), channelBuilder);
    }

    @After
    public void teardown() throws Exception {
        this.selector.close();
        this.server.close();
        this.metrics.close();
    }

    /**
     * Tests that SSL renegotiation initiated by the server are handled correctly by the client
     * @throws Exception
     */
    @Test
    public void testRenegotiation() throws Exception {
        int reqs = 500;
        String node = "0";
        // create connections
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port);
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        // send echo requests and receive responses
        int requests = 0;
        int responses = 0;
        int renegotiates = 0;
        while (!selector.isChannelReady(node)) {
            selector.poll(1000L);
        }
        selector.send(createSend(node, node + "-" + 0));
        requests++;

        // loop until we complete all requests
        while (responses < reqs) {
            selector.poll(0L);
            if (responses >= 100 && renegotiates == 0) {
                renegotiates++;
                server.renegotiate();
            }
            assertEquals("No disconnects should have occurred.", 0, selector.disconnected().size());

            // handle any responses we may have gotten
            for (NetworkReceive receive : selector.completedReceives()) {
                String[] pieces = asString(receive).split("-");
                assertEquals("Should be in the form 'conn-counter'", 2, pieces.length);
                assertEquals("Check the source", receive.source(), pieces[0]);
                assertEquals("Check that the receive has kindly been rewound", 0, receive.payload().position());
                assertEquals("Check the request counter", responses, Integer.parseInt(pieces[1]));
                responses++;
            }

            // prepare new sends for the next round
            for (int i = 0; i < selector.completedSends().size() && requests < reqs && selector.isChannelReady(node); i++, requests++) {
                selector.send(createSend(node, node + "-" + requests));
            }
        }
    }

    /**
     * Connects and waits for handshake to complete. This is required since SSLTransportLayer
     * implementation requires the channel to be ready before send is invoked (unlike plaintext
     * where send can be invoked straight after connect)
     */
    protected void connect(String node, InetSocketAddress serverAddr) throws IOException {
        blockingConnect(node, serverAddr);
    }

}
