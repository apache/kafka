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

import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.network.CertStores;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.ChannelBuilders;
import org.apache.kafka.common.network.ChannelState;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.network.NetworkSend;
import org.apache.kafka.common.network.NetworkTestUtils;
import org.apache.kafka.common.network.NioEchoServer;
import org.apache.kafka.common.network.SaslChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.network.TransportLayer;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.SaslHandshakeRequest;
import org.apache.kafka.common.requests.SaslHandshakeResponse;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.TestSecurityConfig;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.scram.ScramCredential;
import org.apache.kafka.common.security.scram.ScramLoginModule;
import org.apache.kafka.common.security.scram.internal.ScramCredentialUtils;
import org.apache.kafka.common.security.scram.internal.ScramFormatter;
import org.apache.kafka.common.security.scram.internal.ScramMechanism;
import org.apache.kafka.common.utils.Time;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.security.auth.Subject;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(value = Parameterized.class)
public class SaslAuthenticatorFailureTest {
    private static final int BUFFER_SIZE = 4 * 1024;

    private NioEchoServer server;
    private Selector selector;
    private ChannelBuilder channelBuilder;
    private CertStores serverCertStores;
    private CertStores clientCertStores;
    private Map<String, Object> saslClientConfigs;
    private Map<String, Object> saslServerConfigs;
    private CredentialCache credentialCache;
    private int nextCorrelationId;
    private final int failedAuthenticationDelayMs;
    private long startTimeMs;

    public SaslAuthenticatorFailureTest(int failedAuthenticationDelayMs) {
        this.failedAuthenticationDelayMs = failedAuthenticationDelayMs;
    }

    @Parameterized.Parameters(name = "failedAuthenticationDelayMs={0}")
    public static Collection<Object[]> data() {
        List<Object[]> values = new ArrayList<>();
        values.add(new Object[]{-1});
        values.add(new Object[]{0});
        values.add(new Object[]{5000});
        return values;
    }

    @Before
    public void setup() throws Exception {
        LoginManager.closeAll();
        serverCertStores = new CertStores(true, "localhost");
        clientCertStores = new CertStores(false, "localhost");
        saslServerConfigs = serverCertStores.getTrustingConfig(clientCertStores);
        saslClientConfigs = clientCertStores.getTrustingConfig(serverCertStores);
        credentialCache = new CredentialCache();
        SaslAuthenticatorTest.TestLogin.loginCount.set(0);
        startTimeMs = Time.SYSTEM.milliseconds();
    }

    @After
    public void teardown() throws Exception {
        long now = Time.SYSTEM.milliseconds();
        if (server != null)
            this.server.close();
        if (selector != null)
            this.selector.close();
        if (failedAuthenticationDelayMs != -1)
            assertTrue("timeSpent: " + (now - startTimeMs), now - startTimeMs >= failedAuthenticationDelayMs);
    }

    /**
     * Tests that SASL/PLAIN clients with invalid password fail authentication.
     */
    @Test
    public void testInvalidPasswordSaslPlain() throws Exception {
        String node = "0";
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
        TestJaasConfig jaasConfig = configureMechanisms("PLAIN", Arrays.asList("PLAIN"));
        jaasConfig.setClientOptions("PLAIN", TestJaasConfig.USERNAME, "invalidpassword");

        server = createEchoServer(securityProtocol);
        createAndCheckClientAuthenticationFailure(securityProtocol, node, "PLAIN",
                "Authentication failed: Invalid username or password");
        server.verifyAuthenticationMetrics(0, 1);
    }

    /**
     * Tests that SASL/PLAIN clients with invalid username fail authentication.
     */
    @Test
    public void testInvalidUsernameSaslPlain() throws Exception {
        String node = "0";
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
        TestJaasConfig jaasConfig = configureMechanisms("PLAIN", Arrays.asList("PLAIN"));
        jaasConfig.setClientOptions("PLAIN", "invaliduser", TestJaasConfig.PASSWORD);

        server = createEchoServer(securityProtocol);
        createAndCheckClientAuthenticationFailure(securityProtocol, node, "PLAIN",
                "Authentication failed: Invalid username or password");
        server.verifyAuthenticationMetrics(0, 1);
    }

    /**
     * Tests that SASL/SCRAM clients fail authentication if password is invalid.
     */
    @Test
    public void testInvalidPasswordSaslScram() throws Exception {
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
        TestJaasConfig jaasConfig = configureMechanisms("SCRAM-SHA-256", Arrays.asList("SCRAM-SHA-256"));
        Map<String, Object> options = new HashMap<>();
        options.put("username", TestJaasConfig.USERNAME);
        options.put("password", "invalidpassword");
        jaasConfig.createOrUpdateEntry(TestJaasConfig.LOGIN_CONTEXT_CLIENT, ScramLoginModule.class.getName(), options);

        String node = "0";
        server = createEchoServer(securityProtocol);
        updateScramCredentialCache(TestJaasConfig.USERNAME, TestJaasConfig.PASSWORD);
        createAndCheckClientAuthenticationFailure(securityProtocol, node, "SCRAM-SHA-256", null);
        server.verifyAuthenticationMetrics(0, 1);
    }

    /**
     * Tests that SASL/SCRAM clients without valid username fail authentication.
     */
    @Test
    public void testUnknownUserSaslScram() throws Exception {
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
        TestJaasConfig jaasConfig = configureMechanisms("SCRAM-SHA-256", Arrays.asList("SCRAM-SHA-256"));
        Map<String, Object> options = new HashMap<>();
        options.put("username", "unknownUser");
        options.put("password", TestJaasConfig.PASSWORD);
        jaasConfig.createOrUpdateEntry(TestJaasConfig.LOGIN_CONTEXT_CLIENT, ScramLoginModule.class.getName(), options);

        String node = "0";
        server = createEchoServer(securityProtocol);
        updateScramCredentialCache(TestJaasConfig.USERNAME, TestJaasConfig.PASSWORD);
        createAndCheckClientAuthenticationFailure(securityProtocol, node, "SCRAM-SHA-256", null);
        server.verifyAuthenticationMetrics(0, 1);
    }

    /**
     * Tests that SASL/SCRAM clients fail authentication if credentials are not available for
     * the specific SCRAM mechanism.
     */
    @Test
    public void testUserCredentialsUnavailableForScramMechanism() throws Exception {
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
        configureMechanisms("SCRAM-SHA-256", new ArrayList<>(ScramMechanism.mechanismNames()));
        server = createEchoServer(securityProtocol);
        updateScramCredentialCache(TestJaasConfig.USERNAME, TestJaasConfig.PASSWORD);

        server.credentialCache().cache(ScramMechanism.SCRAM_SHA_256.mechanismName(), ScramCredential.class).remove(TestJaasConfig.USERNAME);
        String node = "1";
        saslClientConfigs.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256");
        createAndCheckClientAuthenticationFailure(securityProtocol, node, "SCRAM-SHA-256", null);
        server.verifyAuthenticationMetrics(0, 1);

        saslClientConfigs.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
        createAndCheckClientConnection(securityProtocol, "2");
        server.verifyAuthenticationMetrics(1, 1);
    }

    /**
     * Tests that any invalid data during Kafka SASL handshake request flow
     * or the actual SASL authentication flow result in authentication failure
     * and do not cause any failures in the server.
     */
    @Test
    public void testInvalidSaslPacket() throws Exception {
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_PLAINTEXT;
        configureMechanisms("PLAIN", Arrays.asList("PLAIN"));
        server = createEchoServer(securityProtocol);

        // Send invalid SASL packet after valid handshake request
        String node1 = "invalid1";
        createClientConnection(SecurityProtocol.PLAINTEXT, node1);
        sendHandshakeRequestReceiveResponse(node1, (short) 1);
        Random random = new Random();
        byte[] bytes = new byte[1024];
        random.nextBytes(bytes);
        selector.send(new NetworkSend(node1, ByteBuffer.wrap(bytes)));
        NetworkTestUtils.waitForChannelClose(selector, node1, ChannelState.READY.state());
        selector.close();

        // Test good connection still works
        createAndCheckClientConnection(securityProtocol, "good1");

        // Send invalid SASL packet before handshake request
        String node2 = "invalid2";
        createClientConnection(SecurityProtocol.PLAINTEXT, node2);
        random.nextBytes(bytes);
        selector.send(new NetworkSend(node2, ByteBuffer.wrap(bytes)));
        NetworkTestUtils.waitForChannelClose(selector, node2, ChannelState.READY.state());
        selector.close();

        // Test good connection still works
        createAndCheckClientConnection(securityProtocol, "good2");
    }

    /**
     * Tests that ApiVersionsRequest after Kafka SASL handshake request flow,
     * but prior to actual SASL authentication, results in authentication failure.
     * This is similar to {@link #testUnauthenticatedApiVersionsRequest(SecurityProtocol, short)}
     * where a non-SASL client is used to send requests that are processed by
     * {@link SaslServerAuthenticator} of the server prior to client authentication.
     */
    @Test
    public void testInvalidApiVersionsRequestSequence() throws Exception {
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_PLAINTEXT;
        configureMechanisms("PLAIN", Arrays.asList("PLAIN"));
        server = createEchoServer(securityProtocol);

        // Send handshake request followed by ApiVersionsRequest
        String node1 = "invalid1";
        createClientConnection(SecurityProtocol.PLAINTEXT, node1);
        sendHandshakeRequestReceiveResponse(node1, (short) 1);

        ApiVersionsRequest request = createApiVersionsRequestV0();
        RequestHeader versionsHeader = new RequestHeader(ApiKeys.API_VERSIONS, request.version(), "someclient", 2);
        selector.send(request.toSend(node1, versionsHeader));
        NetworkTestUtils.waitForChannelClose(selector, node1, ChannelState.READY.state());
        selector.close();

        // Test good connection still works
        createAndCheckClientConnection(securityProtocol, "good1");
    }

    /**
     * Tests that packets that are too big during Kafka SASL handshake request flow
     * or the actual SASL authentication flow result in authentication failure
     * and do not cause any failures in the server.
     */
    @Test
    public void testPacketSizeTooBig() throws Exception {
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_PLAINTEXT;
        configureMechanisms("PLAIN", Arrays.asList("PLAIN"));
        server = createEchoServer(securityProtocol);

        // Send SASL packet with large size after valid handshake request
        String node1 = "invalid1";
        createClientConnection(SecurityProtocol.PLAINTEXT, node1);
        sendHandshakeRequestReceiveResponse(node1, (short) 1);
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        buffer.putInt(Integer.MAX_VALUE);
        buffer.put(new byte[buffer.capacity() - 4]);
        buffer.rewind();
        selector.send(new NetworkSend(node1, buffer));
        NetworkTestUtils.waitForChannelClose(selector, node1, ChannelState.READY.state());
        selector.close();

        // Test good connection still works
        createAndCheckClientConnection(securityProtocol, "good1");

        // Send packet with large size before handshake request
        String node2 = "invalid2";
        createClientConnection(SecurityProtocol.PLAINTEXT, node2);
        buffer.clear();
        buffer.putInt(Integer.MAX_VALUE);
        buffer.put(new byte[buffer.capacity() - 4]);
        buffer.rewind();
        selector.send(new NetworkSend(node2, buffer));
        NetworkTestUtils.waitForChannelClose(selector, node2, ChannelState.READY.state());
        selector.close();

        // Test good connection still works
        createAndCheckClientConnection(securityProtocol, "good2");
    }

    /**
     * Tests that Kafka requests that are forbidden until successful authentication result
     * in authentication failure and do not cause any failures in the server.
     */
    @Test
    public void testDisallowedKafkaRequestsBeforeAuthentication() throws Exception {
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_PLAINTEXT;
        configureMechanisms("PLAIN", Arrays.asList("PLAIN"));
        server = createEchoServer(securityProtocol);

        // Send metadata request before Kafka SASL handshake request
        String node1 = "invalid1";
        createClientConnection(SecurityProtocol.PLAINTEXT, node1);
        MetadataRequest metadataRequest1 = new MetadataRequest.Builder(Collections.singletonList("sometopic"),
                true).build();
        RequestHeader metadataRequestHeader1 = new RequestHeader(ApiKeys.METADATA, metadataRequest1.version(),
                "someclient", 1);
        selector.send(metadataRequest1.toSend(node1, metadataRequestHeader1));
        NetworkTestUtils.waitForChannelClose(selector, node1, ChannelState.READY.state());
        selector.close();

        // Test good connection still works
        createAndCheckClientConnection(securityProtocol, "good1");

        // Send metadata request after Kafka SASL handshake request
        String node2 = "invalid2";
        createClientConnection(SecurityProtocol.PLAINTEXT, node2);
        sendHandshakeRequestReceiveResponse(node2, (short) 1);
        MetadataRequest metadataRequest2 = new MetadataRequest.Builder(Collections.singletonList("sometopic"), true).build();
        RequestHeader metadataRequestHeader2 = new RequestHeader(ApiKeys.METADATA,
                metadataRequest2.version(), "someclient", 2);
        selector.send(metadataRequest2.toSend(node2, metadataRequestHeader2));
        NetworkTestUtils.waitForChannelClose(selector, node2, ChannelState.READY.state());
        selector.close();

        // Test good connection still works
        createAndCheckClientConnection(securityProtocol, "good2");
    }

    /**
     * Tests that mechanisms with default implementation in Kafka may be disabled in
     * the Kafka server by removing from the enabled mechanism list.
     */
    @Test
    public void testDisabledMechanism() throws Exception {
        String node = "0";
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
        configureMechanisms("PLAIN", Arrays.asList("DIGEST-MD5"));

        server = createEchoServer(securityProtocol);
        createAndCheckClientConnectionFailure(securityProtocol, node);
        server.verifyAuthenticationMetrics(0, 1);
    }

    /**
     * Tests that clients using invalid SASL mechanisms fail authentication.
     */
    @Test
    public void testInvalidMechanism() throws Exception {
        String node = "0";
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
        configureMechanisms("PLAIN", Arrays.asList("PLAIN"));
        saslClientConfigs.put(SaslConfigs.SASL_MECHANISM, "INVALID");

        server = createEchoServer(securityProtocol);
        createAndCheckClientConnectionFailure(securityProtocol, node);
        server.verifyAuthenticationMetrics(0, 1);
    }

    /**
     * Tests SASL/PLAIN authentication failure over PLAINTEXT with old version of server
     * that does not support SASL_AUTHENTICATE headers and new version of client.
     */
    @Test
    public void oldSaslPlainPlaintextServerWithoutSaslAuthenticateHeaderFailure() throws Exception {
        verifySaslAuthenticateHeaderInteropWithFailure(false, true, SecurityProtocol.SASL_PLAINTEXT, "PLAIN");
    }

    /**
     * Tests SASL/PLAIN authentication failure over PLAINTEXT with old version of client
     * that does not support SASL_AUTHENTICATE headers and new version of server.
     */
    @Test
    public void oldSaslPlainPlaintextClientWithoutSaslAuthenticateHeaderFailure() throws Exception {
        verifySaslAuthenticateHeaderInteropWithFailure(true, false, SecurityProtocol.SASL_PLAINTEXT, "PLAIN");
    }

    /**
     * Tests SASL/SCRAM authentication failure over PLAINTEXT with old version of server
     * that does not support SASL_AUTHENTICATE headers and new version of client.
     */
    @Test
    public void oldSaslScramPlaintextServerWithoutSaslAuthenticateHeaderFailure() throws Exception {
        verifySaslAuthenticateHeaderInteropWithFailure(false, true, SecurityProtocol.SASL_PLAINTEXT, "SCRAM-SHA-256");
    }

    /**
     * Tests SASL/SCRAM authentication failure over PLAINTEXT with old version of client
     * that does not support SASL_AUTHENTICATE headers and new version of server.
     */
    @Test
    public void oldSaslScramPlaintextClientWithoutSaslAuthenticateHeaderFailure() throws Exception {
        verifySaslAuthenticateHeaderInteropWithFailure(true, false, SecurityProtocol.SASL_PLAINTEXT, "SCRAM-SHA-256");
    }

    /**
     * Tests SASL/PLAIN authentication failure over SSL with old version of server
     * that does not support SASL_AUTHENTICATE headers and new version of client.
     */
    @Test
    public void oldSaslPlainSslServerWithoutSaslAuthenticateHeaderFailure() throws Exception {
        verifySaslAuthenticateHeaderInteropWithFailure(false, true, SecurityProtocol.SASL_SSL, "PLAIN");
    }

    /**
     * Tests SASL/PLAIN authentication failure over SSL with old version of client
     * that does not support SASL_AUTHENTICATE headers and new version of server.
     */
    @Test
    public void oldSaslPlainSslClientWithoutSaslAuthenticateHeaderFailure() throws Exception {
        verifySaslAuthenticateHeaderInteropWithFailure(true, false, SecurityProtocol.SASL_SSL, "PLAIN");
    }

    /**
     * Tests SASL/SCRAM authentication failure over SSL with old version of server
     * that does not support SASL_AUTHENTICATE headers and new version of client.
     */
    @Test
    public void oldSaslScramSslServerWithoutSaslAuthenticateHeaderFailure() throws Exception {
        verifySaslAuthenticateHeaderInteropWithFailure(false, true, SecurityProtocol.SASL_SSL, "SCRAM-SHA-512");
    }

    /**
     * Tests SASL/SCRAM authentication failure over SSL with old version of client
     * that does not support SASL_AUTHENTICATE headers and new version of server.
     */
    @Test
    public void oldSaslScramSslClientWithoutSaslAuthenticateHeaderFailure() throws Exception {
        verifySaslAuthenticateHeaderInteropWithFailure(true, false, SecurityProtocol.SASL_SSL, "SCRAM-SHA-512");
    }

    /**
     * Tests OAUTHBEARER fails the connection when the client presents a token with
     * insufficient scope .
     */
    @Test
    public void testInsufficientScopeSaslOauthBearerMechanism() throws Exception {
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
        TestJaasConfig jaasConfig = configureMechanisms("OAUTHBEARER", Arrays.asList("OAUTHBEARER"));
        // now update the server side to require a scope the client does not provide
        Map<String, Object> serverJaasConfigOptionsMap = TestJaasConfig.defaultServerOptions("OAUTHBEARER");
        serverJaasConfigOptionsMap.put("unsecuredValidatorRequiredScope", "LOGIN_TO_KAFKA"); // causes the failure
        jaasConfig.createOrUpdateEntry("KafkaServer",
                "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule", serverJaasConfigOptionsMap);
        server = createEchoServer(securityProtocol);
        createAndCheckClientAuthenticationFailure(securityProtocol,
                "node-" + OAuthBearerLoginModule.OAUTHBEARER_MECHANISM, OAuthBearerLoginModule.OAUTHBEARER_MECHANISM,
                "{\"status\":\"insufficient_scope\", \"scope\":\"[LOGIN_TO_KAFKA]\"}");
    }

    private void verifySaslAuthenticateHeaderInteropWithFailure(boolean enableHeaderOnServer, boolean enableHeaderOnClient,
                                                                SecurityProtocol securityProtocol, String saslMechanism) throws Exception {
        TestJaasConfig jaasConfig = configureMechanisms(saslMechanism, Arrays.asList(saslMechanism));
        jaasConfig.setClientOptions(saslMechanism, TestJaasConfig.USERNAME, "invalidpassword");
        createServer(securityProtocol, saslMechanism, enableHeaderOnServer);

        String node = "0";
        createClientConnection(securityProtocol, saslMechanism, node, enableHeaderOnClient);
        // Without SASL_AUTHENTICATE headers, disconnect state is ChannelState.AUTHENTICATE which is
        // a hint that channel was closed during authentication, unlike ChannelState.AUTHENTICATE_FAILED
        // which is an actual authentication failure reported by the broker.
        NetworkTestUtils.waitForChannelClose(selector, node, ChannelState.AUTHENTICATE.state());
    }

    private void createServer(SecurityProtocol securityProtocol, String saslMechanism,
                              boolean enableSaslAuthenticateHeader) throws Exception {
        if (enableSaslAuthenticateHeader)
            server = createEchoServer(securityProtocol);
        else
            server = startServerWithoutSaslAuthenticateHeader(securityProtocol, saslMechanism);
        updateScramCredentialCache(TestJaasConfig.USERNAME, TestJaasConfig.PASSWORD);
    }

    private void createClientConnection(SecurityProtocol securityProtocol, String saslMechanism, String node,
                                        boolean enableSaslAuthenticateHeader) throws Exception {
        if (enableSaslAuthenticateHeader)
            createClientConnection(securityProtocol, node);
        else
            createClientConnectionWithoutSaslAuthenticateHeader(securityProtocol, saslMechanism, node);
    }

    private NioEchoServer startServerWithoutSaslAuthenticateHeader(final SecurityProtocol securityProtocol, String saslMechanism)
            throws Exception {
        final ListenerName listenerName = ListenerName.forSecurityProtocol(securityProtocol);
        final Map<String, ?> configs = Collections.emptyMap();
        final JaasContext jaasContext = JaasContext.loadServerContext(listenerName, saslMechanism, configs);
        final Map<String, JaasContext> jaasContexts = Collections.singletonMap(saslMechanism, jaasContext);

        boolean isScram = ScramMechanism.isScram(saslMechanism);
        if (isScram)
            ScramCredentialUtils.createCache(credentialCache, Arrays.asList(saslMechanism));
        SaslChannelBuilder serverChannelBuilder = new SaslChannelBuilder(Mode.SERVER, jaasContexts,
                securityProtocol, listenerName, false, saslMechanism, true, credentialCache, null) {

            @Override
            protected SaslServerAuthenticator buildServerAuthenticator(Map<String, ?> configs,
                                                                       Map<String, AuthenticateCallbackHandler> callbackHandlers,
                                                                       String id,
                                                                       TransportLayer transportLayer,
                                                                       Map<String, Subject> subjects) throws IOException {
                return new SaslServerAuthenticator(configs, callbackHandlers, id, subjects, null, listenerName, securityProtocol, transportLayer) {

                    @Override
                    protected ApiVersionsResponse apiVersionsResponse() {
                        List<ApiVersionsResponse.ApiVersion> apiVersions = new ArrayList<>(ApiVersionsResponse.defaultApiVersionsResponse().apiVersions());
                        for (Iterator<ApiVersionsResponse.ApiVersion> it = apiVersions.iterator(); it.hasNext(); ) {
                            ApiVersionsResponse.ApiVersion apiVersion = it.next();
                            if (apiVersion.apiKey == ApiKeys.SASL_AUTHENTICATE.id) {
                                it.remove();
                                break;
                            }
                        }
                        return new ApiVersionsResponse(0, Errors.NONE, apiVersions);
                    }

                    @Override
                    protected void enableKafkaSaslAuthenticateHeaders(boolean flag) {
                        // Don't enable Kafka SASL_AUTHENTICATE headers
                    }
                };
            }
        };
        serverChannelBuilder.configure(saslServerConfigs);
        if (failedAuthenticationDelayMs == -1)
            server = new NioEchoServer(listenerName, securityProtocol, new TestSecurityConfig(saslServerConfigs),
                    "localhost", serverChannelBuilder, credentialCache);
        else
            server = new NioEchoServer(listenerName, securityProtocol, new TestSecurityConfig(saslServerConfigs),
                    "localhost", serverChannelBuilder, credentialCache, failedAuthenticationDelayMs);
        server.start();
        return server;
    }

    private void createClientConnectionWithoutSaslAuthenticateHeader(final SecurityProtocol securityProtocol,
                                                                     final String saslMechanism, String node) throws Exception {

        final ListenerName listenerName = ListenerName.forSecurityProtocol(securityProtocol);
        final Map<String, ?> configs = Collections.emptyMap();
        final JaasContext jaasContext = JaasContext.loadClientContext(configs);
        final Map<String, JaasContext> jaasContexts = Collections.singletonMap(saslMechanism, jaasContext);

        SaslChannelBuilder clientChannelBuilder = new SaslChannelBuilder(Mode.CLIENT, jaasContexts,
                securityProtocol, listenerName, false, saslMechanism, true, null, null) {

            @Override
            protected SaslClientAuthenticator buildClientAuthenticator(Map<String, ?> configs,
                                                                       AuthenticateCallbackHandler callbackHandler,
                                                                       String id,
                                                                       String serverHost,
                                                                       String servicePrincipal,
                                                                       TransportLayer transportLayer,
                                                                       Subject subject) throws IOException {

                return new SaslClientAuthenticator(configs, callbackHandler, id, subject,
                        servicePrincipal, serverHost, saslMechanism, true, transportLayer) {
                    @Override
                    protected SaslHandshakeRequest createSaslHandshakeRequest(short version) {
                        return new SaslHandshakeRequest.Builder(saslMechanism).build((short) 0);
                    }
                    @Override
                    protected void saslAuthenticateVersion(short version) {
                        // Don't set version so that headers are disabled
                    }
                };
            }
        };
        clientChannelBuilder.configure(saslClientConfigs);
        this.selector = NetworkTestUtils.createSelector(clientChannelBuilder);
        InetSocketAddress addr = new InetSocketAddress("127.0.0.1", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);
    }

    private TestJaasConfig configureMechanisms(String clientMechanism, List<String> serverMechanisms) {
        saslClientConfigs.put(SaslConfigs.SASL_MECHANISM, clientMechanism);
        saslServerConfigs.put(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG, serverMechanisms);
        if (serverMechanisms.contains("DIGEST-MD5")) {
            saslServerConfigs.put("digest-md5." + BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS,
                    TestDigestLoginModule.DigestServerCallbackHandler.class.getName());
        }
        return TestJaasConfig.createConfiguration(clientMechanism, serverMechanisms);
    }

    private void configureDigestMd5ServerCallback(SecurityProtocol securityProtocol) {
        String callbackPrefix = ListenerName.forSecurityProtocol(securityProtocol).saslMechanismConfigPrefix("DIGEST-MD5");
        saslServerConfigs.put(callbackPrefix + BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS,
                TestDigestLoginModule.DigestServerCallbackHandler.class);
    }

    private void createSelector(SecurityProtocol securityProtocol, Map<String, Object> clientConfigs) {
        if (selector != null) {
            selector.close();
            selector = null;
        }

        String saslMechanism = (String) saslClientConfigs.get(SaslConfigs.SASL_MECHANISM);
        this.channelBuilder = ChannelBuilders.clientChannelBuilder(securityProtocol, JaasContext.Type.CLIENT,
                new TestSecurityConfig(clientConfigs), null, saslMechanism, true);
        this.selector = NetworkTestUtils.createSelector(channelBuilder);
    }

    private NioEchoServer createEchoServer(SecurityProtocol securityProtocol) throws Exception {
        return createEchoServer(ListenerName.forSecurityProtocol(securityProtocol), securityProtocol);
    }

    private NioEchoServer createEchoServer(ListenerName listenerName, SecurityProtocol securityProtocol) throws Exception {
        if (failedAuthenticationDelayMs != -1)
            return NetworkTestUtils.createEchoServer(listenerName, securityProtocol,
                    new TestSecurityConfig(saslServerConfigs), credentialCache, failedAuthenticationDelayMs);
        else
            return NetworkTestUtils.createEchoServer(listenerName, securityProtocol,
                    new TestSecurityConfig(saslServerConfigs), credentialCache);
    }

    private void createClientConnection(SecurityProtocol securityProtocol, String node) throws Exception {
        createSelector(securityProtocol, saslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("127.0.0.1", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);
    }

    private void createAndCheckClientConnection(SecurityProtocol securityProtocol, String node) throws Exception {
        createClientConnection(securityProtocol, node);
        NetworkTestUtils.checkClientConnection(selector, node, 100, 10);
        selector.close();
        selector = null;
    }

    private void createAndCheckClientAuthenticationFailure(SecurityProtocol securityProtocol, String node,
                                                           String mechanism, String expectedErrorMessage) throws Exception {
        ChannelState finalState = createAndCheckClientConnectionFailure(securityProtocol, node);
        Exception exception = finalState.exception();
        assertTrue("Invalid exception class " + exception.getClass(), exception instanceof SaslAuthenticationException);
        if (expectedErrorMessage == null)
            expectedErrorMessage = "Authentication failed due to invalid credentials with SASL mechanism " + mechanism;
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

    private AbstractResponse sendKafkaRequestReceiveResponse(String node, ApiKeys apiKey, AbstractRequest request) throws IOException {
        RequestHeader header = new RequestHeader(apiKey, request.version(), "someclient", nextCorrelationId++);
        Send send = request.toSend(node, header);
        selector.send(send);
        ByteBuffer responseBuffer = waitForResponse();
        return NetworkClient.parseResponse(responseBuffer, header);
    }

    private SaslHandshakeResponse sendHandshakeRequestReceiveResponse(String node, short version) throws Exception {
        SaslHandshakeRequest handshakeRequest = new SaslHandshakeRequest.Builder("PLAIN").build(version);
        SaslHandshakeResponse response = (SaslHandshakeResponse) sendKafkaRequestReceiveResponse(node, ApiKeys.SASL_HANDSHAKE, handshakeRequest);
        assertEquals(Errors.NONE, response.error());
        return response;
    }

    private ApiVersionsResponse sendVersionRequestReceiveResponse(String node) throws Exception {
        ApiVersionsRequest handshakeRequest = createApiVersionsRequestV0();
        ApiVersionsResponse response =  (ApiVersionsResponse) sendKafkaRequestReceiveResponse(node, ApiKeys.API_VERSIONS, handshakeRequest);
        assertEquals(Errors.NONE, response.error());
        return response;
    }

    private ByteBuffer waitForResponse() throws IOException {
        int waitSeconds = 10;
        do {
            selector.poll(1000);
        } while (selector.completedReceives().isEmpty() && waitSeconds-- > 0);
        assertEquals(1, selector.completedReceives().size());
        return selector.completedReceives().get(0).payload();
    }

    @SuppressWarnings("unchecked")
    private void updateScramCredentialCache(String username, String password) throws NoSuchAlgorithmException {
        for (String mechanism : (List<String>) saslServerConfigs.get(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG)) {
            ScramMechanism scramMechanism = ScramMechanism.forMechanismName(mechanism);
            if (scramMechanism != null) {
                ScramFormatter formatter = new ScramFormatter(scramMechanism);
                ScramCredential credential = formatter.generateCredential(password, 4096);
                credentialCache.cache(scramMechanism.mechanismName(), ScramCredential.class).put(username, credential);
            }
        }
    }

    // Creates an ApiVersionsRequest with version 0. Using v0 in tests since
    // SaslClientAuthenticator always uses version 0
    private ApiVersionsRequest createApiVersionsRequestV0() {
        return new ApiVersionsRequest.Builder((short) 0).build();
    }
}
