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
package org.apache.kafka.common.security.authenticator;

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.network.CertStores;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.ChannelBuilders;
import org.apache.kafka.common.network.ChannelState;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.network.NetworkTestUtils;
import org.apache.kafka.common.network.NioEchoServer;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.TestSecurityConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class SaslAuthenticatorFailureDelayTest {
    private static final int BUFFER_SIZE = 4 * 1024;

    private final MockTime time = new MockTime(1);
    private NioEchoServer server;
    private Selector selector;
    private Map<String, Object> saslClientConfigs;
    private Map<String, Object> saslServerConfigs;
    private CredentialCache credentialCache;
    private long startTimeMs;
    private final int failedAuthenticationDelayMs;

    public SaslAuthenticatorFailureDelayTest(int failedAuthenticationDelayMs) {
        this.failedAuthenticationDelayMs = failedAuthenticationDelayMs;
    }

    @BeforeEach
    public void setup() throws Exception {
        LoginManager.closeAll();
        CertStores serverCertStores = new CertStores(true, "localhost");
        CertStores clientCertStores = new CertStores(false, "localhost");
        saslServerConfigs = serverCertStores.getTrustingConfig(clientCertStores);
        saslClientConfigs = clientCertStores.getTrustingConfig(serverCertStores);
        credentialCache = new CredentialCache();
        SaslAuthenticatorTest.TestLogin.loginCount.set(0);
        startTimeMs = time.milliseconds();
    }

    @AfterEach
    public void teardown() throws Exception {
        long now = time.milliseconds();
        if (server != null)
            this.server.close();
        if (selector != null)
            this.selector.close();
        assertTrue(now - startTimeMs >= failedAuthenticationDelayMs, "timeSpent: " + (now - startTimeMs));
    }

    /**
     * Tests that SASL/PLAIN clients with invalid password fail authentication.
     */
    @Test
    public void testInvalidPasswordSaslPlain() throws Exception {
        String node = "0";
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
        TestJaasConfig jaasConfig = configureMechanisms("PLAIN", Collections.singletonList("PLAIN"));
        jaasConfig.setClientOptions("PLAIN", TestJaasConfig.USERNAME, "invalidpassword");

        server = createEchoServer(securityProtocol);
        createAndCheckClientAuthenticationFailure(securityProtocol, node, "PLAIN",
                "Authentication failed: Invalid username " + TestJaasConfig.USERNAME + " or password");
        server.verifyAuthenticationMetrics(0, 1);
    }

    /**
     * Tests that SASL/SCRAM clients with invalid password fail authentication with
     * connection close delay if configured.
     */
    @Test
    public void testInvalidPasswordSaslScram() throws Exception {
        String node = "0";
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
        TestJaasConfig jaasConfig = configureMechanisms("SCRAM-SHA-256", Collections.singletonList("SCRAM-SHA-256"));
        jaasConfig.setClientOptions("SCRAM-SHA-256", TestJaasConfig.USERNAME, "invalidpassword");

        server = createEchoServer(securityProtocol);
        createAndCheckClientAuthenticationFailure(securityProtocol, node, "SCRAM-SHA-256", null);
        server.verifyAuthenticationMetrics(0, 1);
    }

    /**
     * Tests that clients with disabled SASL mechanism fail authentication with
     * connection close delay if configured.
     */
    @Test
    public void testDisabledSaslMechanism() throws Exception {
        String node = "0";
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
        TestJaasConfig jaasConfig = configureMechanisms("SCRAM-SHA-256", Collections.singletonList("SCRAM-SHA-256"));
        jaasConfig.setClientOptions("PLAIN", TestJaasConfig.USERNAME, "invalidpassword");

        server = createEchoServer(securityProtocol);
        createAndCheckClientAuthenticationFailure(securityProtocol, node, "SCRAM-SHA-256", null);
        server.verifyAuthenticationMetrics(0, 1);
    }

    /**
     * Tests client connection close before response for authentication failure is sent.
     */
    @Test
    public void testClientConnectionClose() throws Exception {
        String node = "0";
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
        TestJaasConfig jaasConfig = configureMechanisms("PLAIN", Collections.singletonList("PLAIN"));
        jaasConfig.setClientOptions("PLAIN", TestJaasConfig.USERNAME, "invalidpassword");

        server = createEchoServer(securityProtocol);
        createClientConnection(securityProtocol, node);

        Map<?, ?> delayedClosingChannels = NetworkTestUtils.delayedClosingChannels(server.selector());

        // Wait until server has established connection with client and has processed the auth failure
        TestUtils.waitForCondition(() -> {
            poll(selector);
            return !server.selector().channels().isEmpty();
        }, "Timeout waiting for connection");
        TestUtils.waitForCondition(() -> {
            poll(selector);
            return failedAuthenticationDelayMs == 0 || !delayedClosingChannels.isEmpty();
        }, "Timeout waiting for auth failure");

        selector.close();
        selector = null;

        // Now that client connection is closed, wait until server notices the disconnection and removes it from the
        // list of connected channels and from delayed response for auth failure
        TestUtils.waitForCondition(() -> failedAuthenticationDelayMs == 0 || delayedClosingChannels.isEmpty(),
                "Timeout waiting for delayed response remove");
        TestUtils.waitForCondition(() -> server.selector().channels().isEmpty(),
                "Timeout waiting for connection close");

        // Try forcing completion of delayed channel close
        TestUtils.waitForCondition(() -> time.milliseconds() > startTimeMs + failedAuthenticationDelayMs + 1,
                "Timeout when waiting for auth failure response timeout to elapse");
        NetworkTestUtils.completeDelayedChannelClose(server.selector(), time.nanoseconds());
    }

    private void poll(Selector selector) {
        try {
            selector.poll(50);
        } catch (IOException e) {
            throw new RuntimeException("Unexpected failure during selector poll", e);
        }
    }

    private TestJaasConfig configureMechanisms(String clientMechanism, List<String> serverMechanisms) {
        saslClientConfigs.put(SaslConfigs.SASL_MECHANISM, clientMechanism);
        saslServerConfigs.put(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG, serverMechanisms);
        if (serverMechanisms.contains("DIGEST-MD5")) {
            saslServerConfigs.put("digest-md5." + BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS_CONFIG,
                    TestDigestLoginModule.DigestServerCallbackHandler.class.getName());
        }
        return TestJaasConfig.createConfiguration(clientMechanism, serverMechanisms);
    }

    private void createSelector(SecurityProtocol securityProtocol, Map<String, Object> clientConfigs) {
        if (selector != null) {
            selector.close();
            selector = null;
        }

        String saslMechanism = (String) saslClientConfigs.get(SaslConfigs.SASL_MECHANISM);
        ChannelBuilder channelBuilder = ChannelBuilders.clientChannelBuilder(securityProtocol, JaasContext.Type.CLIENT,
                new TestSecurityConfig(clientConfigs), null, saslMechanism, time, true,
                new LogContext());
        this.selector = NetworkTestUtils.createSelector(channelBuilder, time);
    }

    private NioEchoServer createEchoServer(SecurityProtocol securityProtocol) throws Exception {
        return createEchoServer(ListenerName.forSecurityProtocol(securityProtocol), securityProtocol);
    }

    private NioEchoServer createEchoServer(ListenerName listenerName, SecurityProtocol securityProtocol) throws Exception {
        return NetworkTestUtils.createEchoServer(listenerName, securityProtocol,
                new TestSecurityConfig(saslServerConfigs), credentialCache, time);
    }

    private void createClientConnection(SecurityProtocol securityProtocol, String node) throws Exception {
        createSelector(securityProtocol, saslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);
    }

    private void createAndCheckClientAuthenticationFailure(SecurityProtocol securityProtocol, String node,
                                                           String mechanism, String expectedErrorMessage) throws Exception {
        ChannelState finalState = createAndCheckClientConnectionFailure(securityProtocol, node);
        Exception exception = finalState.exception();
        assertInstanceOf(SaslAuthenticationException.class, exception, "Invalid exception class " + exception.getClass());
        if (expectedErrorMessage == null)
            expectedErrorMessage = "Authentication failed during authentication due to invalid credentials with SASL mechanism " + mechanism;
        assertEquals(expectedErrorMessage, exception.getMessage());
    }

    private ChannelState createAndCheckClientConnectionFailure(SecurityProtocol securityProtocol, String node)
            throws Exception {
        createClientConnection(securityProtocol, node);
        ChannelState finalState = NetworkTestUtils.waitForChannelClose(selector, node,
                ChannelState.State.AUTHENTICATION_FAILED);
        selector.close();
        selector = null;
        return finalState;
    }
}
