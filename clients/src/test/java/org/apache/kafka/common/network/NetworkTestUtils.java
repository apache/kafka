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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;

/**
 * Common utility functions used by transport layer and authenticator tests.
 */
public class NetworkTestUtils {

    public static NioEchoServer createEchoServer(SecurityProtocol securityProtocol, Map<String, Object> serverConfigs) throws Exception {
        NioEchoServer server = new NioEchoServer(securityProtocol, serverConfigs, "localhost");
        server.start();
        return server;
    }

    public static Selector createSelector(ChannelBuilder channelBuilder) {
        return new Selector(5000, new Metrics(), new MockTime(), "MetricGroup", channelBuilder);
    }

    public static void checkClientConnection(Selector selector, String node, int minMessageSize, int messageCount) throws Exception {

        waitForChannelReady(selector, node);
        String prefix = TestUtils.randomString(minMessageSize);
        int requests = 0;
        int responses = 0;
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

    public static void waitForChannelReady(Selector selector, String node) throws IOException {
        // wait for handshake to finish
        int secondsLeft = 30;
        while (!selector.isChannelReady(node) && secondsLeft-- > 0) {
            selector.poll(1000L);
        }
        assertTrue(selector.isChannelReady(node));
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
}
