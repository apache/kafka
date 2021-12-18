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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.security.authenticator.CredentialCache;
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;

/**
 * Common utility functions used by transport layer and authenticator tests.
 */
public class NetworkTestUtils {
    public static NioEchoServer createEchoServer(ListenerName listenerName, SecurityProtocol securityProtocol,
                                                 AbstractConfig serverConfig, CredentialCache credentialCache, Time time) throws Exception {
        return createEchoServer(listenerName, securityProtocol, serverConfig, credentialCache, 100, time);
    }

    public static NioEchoServer createEchoServer(ListenerName listenerName, SecurityProtocol securityProtocol,
                                                 AbstractConfig serverConfig, CredentialCache credentialCache,
                                                 int failedAuthenticationDelayMs, Time time) throws Exception {
        NioEchoServer server = new NioEchoServer(listenerName, securityProtocol, serverConfig, "localhost",
                null, credentialCache, failedAuthenticationDelayMs, time);
        server.start();
        return server;
    }

    public static NioEchoServer createEchoServer(ListenerName listenerName, SecurityProtocol securityProtocol,
            AbstractConfig serverConfig, CredentialCache credentialCache,
            int failedAuthenticationDelayMs, Time time, DelegationTokenCache tokenCache) throws Exception {
        NioEchoServer server = new NioEchoServer(listenerName, securityProtocol, serverConfig, "localhost",
                null, credentialCache, failedAuthenticationDelayMs, time, tokenCache);
        server.start();
        return server;
    }

    public static Selector createSelector(ChannelBuilder channelBuilder, Time time) {
        return new Selector(5000, new Metrics(), time, "MetricGroup", channelBuilder, new LogContext());
    }

    public static void checkClientConnection(Selector selector, String node, int minMessageSize, int messageCount) throws Exception {
        waitForChannelReady(selector, node);
        String prefix = TestUtils.randomString(minMessageSize);
        int requests = 0;
        int responses = 0;
        selector.send(new NetworkSend(node, ByteBufferSend.sizePrefixed(ByteBuffer.wrap((prefix + "-0").getBytes(StandardCharsets.UTF_8)))));
        requests++;
        while (responses < messageCount) {
            selector.poll(0L);
            assertEquals(0, selector.disconnected().size(), "No disconnects should have occurred ." + selector.disconnected());

            for (NetworkReceive receive : selector.completedReceives()) {
                assertEquals(prefix + "-" + responses, new String(Utils.toArray(receive.payload()), StandardCharsets.UTF_8));
                responses++;
            }

            for (int i = 0; i < selector.completedSends().size() && requests < messageCount && selector.isChannelReady(node); i++, requests++) {
                selector.send(new NetworkSend(node, ByteBufferSend.sizePrefixed(ByteBuffer.wrap((prefix + "-" + requests).getBytes()))));
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

    public static ChannelState waitForChannelClose(Selector selector, String node, ChannelState.State channelState) throws IOException {
        return waitForChannelClose(selector, node, channelState, 0);
    }

    public static ChannelState waitForChannelClose(Selector selector, String node, ChannelState.State channelState, int delayBetweenPollMs)
            throws IOException {
        boolean closed = false;
        for (int i = 0; i < 300; i++) {
            if (delayBetweenPollMs > 0)
                Utils.sleep(delayBetweenPollMs);
            selector.poll(100L);
            if (selector.channel(node) == null && selector.closingChannel(node) == null) {
                closed = true;
                break;
            }
        }
        assertTrue(closed, "Channel was not closed by timeout");
        ChannelState finalState = selector.disconnected().get(node);
        assertEquals(channelState, finalState.state());
        return finalState;
    }

    public static void completeDelayedChannelClose(Selector selector, long currentTimeNanos) {
        selector.completeDelayedChannelClose(currentTimeNanos);
    }

    public static Map<?, ?> delayedClosingChannels(Selector selector) {
        return selector.delayedClosingChannels();
    }
}
