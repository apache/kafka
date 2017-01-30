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
package org.apache.kafka.common.security.authenticator;

import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.network.CertStores;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.ChannelBuilders;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.network.NetworkSend;
import org.apache.kafka.common.network.NetworkTestUtils;
import org.apache.kafka.common.network.NioEchoServer;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.requests.SaslHandshakeRequest;
import org.apache.kafka.common.requests.SaslHandshakeResponse;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.TestSecurityConfig;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.security.scram.ScramCredential;
import org.apache.kafka.common.security.scram.ScramFormatter;
import org.apache.kafka.common.security.scram.ScramLoginModule;
import org.apache.kafka.common.security.scram.ScramMechanism;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.security.auth.login.Configuration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests for the Sasl authenticator. These use a test harness that runs a simple socket server that echos back responses.
 */
public class SaslAuthenticatorTest {

    private static final int BUFFER_SIZE = 4 * 1024;

    private NioEchoServer server;
    private Selector selector;
    private ChannelBuilder channelBuilder;
    private CertStores serverCertStores;
    private CertStores clientCertStores;
    private Map<String, Object> saslClientConfigs;
    private Map<String, Object> saslServerConfigs;

    @Before
    public void setup() throws Exception {
        serverCertStores = new CertStores(true, "localhost");
        clientCertStores = new CertStores(false, "localhost");
        saslServerConfigs = serverCertStores.getTrustingConfig(clientCertStores);
        saslClientConfigs = clientCertStores.getTrustingConfig(serverCertStores);
    }

    @After
    public void teardown() throws Exception {
        if (server != null)
            this.server.close();
        if (selector != null)
            this.selector.close();
    }

    /**
     * Tests good path SASL/PLAIN client and server channels using SSL transport layer.
     */
    @Test
    public void testValidSaslPlainOverSsl() throws Exception {
        String node = "0";
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
        configureMechanisms("PLAIN", Arrays.asList("PLAIN"));

        server = createEchoServer(securityProtocol);
        createAndCheckClientConnection(securityProtocol, node);
    }

    /**
     * Tests good path SASL/PLAIN client and server channels using PLAINTEXT transport layer.
     */
    @Test
    public void testValidSaslPlainOverPlaintext() throws Exception {
        String node = "0";
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_PLAINTEXT;
        configureMechanisms("PLAIN", Arrays.asList("PLAIN"));

        server = createEchoServer(securityProtocol);
        createAndCheckClientConnection(securityProtocol, node);
    }

    /**
     * Tests that SASL/PLAIN clients with invalid password fail authentication.
     */
    @Test
    public void testInvalidPasswordSaslPlain() throws Exception {
        String node = "0";
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
        TestJaasConfig jaasConfig = configureMechanisms("PLAIN", Arrays.asList("PLAIN"));
        jaasConfig.setPlainClientOptions(TestJaasConfig.USERNAME, "invalidpassword");

        server = createEchoServer(securityProtocol);
        createClientConnection(securityProtocol, node);
        NetworkTestUtils.waitForChannelClose(selector, node);
    }

    /**
     * Tests that SASL/PLAIN clients with invalid username fail authentication.
     */
    @Test
    public void testInvalidUsernameSaslPlain() throws Exception {
        String node = "0";
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
        TestJaasConfig jaasConfig = configureMechanisms("PLAIN", Arrays.asList("PLAIN"));
        jaasConfig.setPlainClientOptions("invaliduser", TestJaasConfig.PASSWORD);

        server = createEchoServer(securityProtocol);
        createClientConnection(securityProtocol, node);
        NetworkTestUtils.waitForChannelClose(selector, node);
    }

    /**
     * Tests that SASL/PLAIN clients without valid username fail authentication.
     */
    @Test
    public void testMissingUsernameSaslPlain() throws Exception {
        String node = "0";
        TestJaasConfig jaasConfig = configureMechanisms("PLAIN", Arrays.asList("PLAIN"));
        jaasConfig.setPlainClientOptions(null, "mypassword");

        SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
        server = createEchoServer(securityProtocol);
        createSelector(securityProtocol, saslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("127.0.0.1", server.port());
        try {
            selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);
            fail("SASL/PLAIN channel created without username");
        } catch (KafkaException e) {
            // Expected exception
        }
    }

    /**
     * Tests that SASL/PLAIN clients with missing password in JAAS configuration fail authentication.
     */
    @Test
    public void testMissingPasswordSaslPlain() throws Exception {
        String node = "0";
        TestJaasConfig jaasConfig = configureMechanisms("PLAIN", Arrays.asList("PLAIN"));
        jaasConfig.setPlainClientOptions("myuser", null);

        SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
        server = createEchoServer(securityProtocol);
        createSelector(securityProtocol, saslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("127.0.0.1", server.port());
        try {
            selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);
            fail("SASL/PLAIN channel created without password");
        } catch (KafkaException e) {
            // Expected exception
        }
    }

    /**
     * Tests that mechanisms that are not supported in Kafka can be plugged in without modifying
     * Kafka code if Sasl client and server providers are available.
     */
    @Test
    public void testMechanismPluggability() throws Exception {
        String node = "0";
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
        configureMechanisms("DIGEST-MD5", Arrays.asList("DIGEST-MD5"));

        server = createEchoServer(securityProtocol);
        createAndCheckClientConnection(securityProtocol, node);
    }

    /**
     * Tests that servers supporting multiple SASL mechanisms work with clients using
     * any of the enabled mechanisms.
     */
    @Test
    public void testMultipleServerMechanisms() throws Exception {
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
        configureMechanisms("DIGEST-MD5", Arrays.asList("DIGEST-MD5", "PLAIN", "SCRAM-SHA-256"));
        server = createEchoServer(securityProtocol);
        updateScramCredentialCache(TestJaasConfig.USERNAME, TestJaasConfig.PASSWORD);

        String node1 = "1";
        saslClientConfigs.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        createAndCheckClientConnection(securityProtocol, node1);

        String node2 = "2";
        saslClientConfigs.put(SaslConfigs.SASL_MECHANISM, "DIGEST-MD5");
        createSelector(securityProtocol, saslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("127.0.0.1", server.port());
        selector.connect(node2, addr, BUFFER_SIZE, BUFFER_SIZE);
        NetworkTestUtils.checkClientConnection(selector, node2, 100, 10);

        String node3 = "3";
        saslClientConfigs.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256");
        createSelector(securityProtocol, saslClientConfigs);
        selector.connect(node3, new InetSocketAddress("127.0.0.1", server.port()), BUFFER_SIZE, BUFFER_SIZE);
        NetworkTestUtils.checkClientConnection(selector, node3, 100, 10);
    }

    /**
     * Tests good path SASL/SCRAM-SHA-256 client and server channels.
     */
    @Test
    public void testValidSaslScramSha256() throws Exception {
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
        configureMechanisms("SCRAM-SHA-256", Arrays.asList("SCRAM-SHA-256"));

        server = createEchoServer(securityProtocol);
        updateScramCredentialCache(TestJaasConfig.USERNAME, TestJaasConfig.PASSWORD);
        createAndCheckClientConnection(securityProtocol, "0");
    }

    /**
     * Tests all supported SCRAM client and server channels. Also tests that all
     * supported SCRAM mechanisms can be supported simultaneously on a server.
     */
    @Test
    public void testValidSaslScramMechanisms() throws Exception {
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
        configureMechanisms("SCRAM-SHA-256", new ArrayList<>(ScramMechanism.mechanismNames()));
        server = createEchoServer(securityProtocol);
        updateScramCredentialCache(TestJaasConfig.USERNAME, TestJaasConfig.PASSWORD);

        for (String mechanism : ScramMechanism.mechanismNames()) {
            saslClientConfigs.put(SaslConfigs.SASL_MECHANISM, mechanism);
            createAndCheckClientConnection(securityProtocol, "node-" + mechanism);
        }
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
        createClientConnection(securityProtocol, node);
        NetworkTestUtils.waitForChannelClose(selector, node);
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
        createClientConnection(securityProtocol, node);
        NetworkTestUtils.waitForChannelClose(selector, node);
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
        createClientConnection(securityProtocol, node);
        NetworkTestUtils.waitForChannelClose(selector, node);

        saslClientConfigs.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
        createAndCheckClientConnection(securityProtocol, "2");
    }

    /**
     * Tests SASL/SCRAM with username containing characters that need
     * to be encoded.
     */
    @Test
    public void testScramUsernameWithSpecialCharacters() throws Exception {
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
        String username = "special user= test,scram";
        String password = username + "-password";
        TestJaasConfig jaasConfig = configureMechanisms("SCRAM-SHA-256", Arrays.asList("SCRAM-SHA-256"));
        Map<String, Object> options = new HashMap<>();
        options.put("username", username);
        options.put("password", password);
        jaasConfig.createOrUpdateEntry(TestJaasConfig.LOGIN_CONTEXT_CLIENT, ScramLoginModule.class.getName(), options);

        server = createEchoServer(securityProtocol);
        updateScramCredentialCache(username, password);
        createAndCheckClientConnection(securityProtocol, "0");
    }

    /**
     * Tests that Kafka ApiVersionsRequests are handled by the SASL server authenticator
     * prior to SASL handshake flow and that subsequent authentication succeeds
     * when transport layer is PLAINTEXT. This test simulates SASL authentication using a
     * (non-SASL) PLAINTEXT client and sends ApiVersionsRequest straight after
     * connection to the server is established, before any SASL-related packets are sent.
     */
    @Test
    public void testUnauthenticatedApiVersionsRequestOverPlaintext() throws Exception {
        testUnauthenticatedApiVersionsRequest(SecurityProtocol.SASL_PLAINTEXT);
    }

    /**
     * Tests that Kafka ApiVersionsRequests are handled by the SASL server authenticator
     * prior to SASL handshake flow and that subsequent authentication succeeds
     * when transport layer is SSL. This test simulates SASL authentication using a
     * (non-SASL) SSL client and sends ApiVersionsRequest straight after
     * SSL handshake, before any SASL-related packets are sent.
     */
    @Test
    public void testUnauthenticatedApiVersionsRequestOverSsl() throws Exception {
        testUnauthenticatedApiVersionsRequest(SecurityProtocol.SASL_SSL);
    }

    /**
     * Tests that unsupported version of ApiVersionsRequest before SASL handshake request
     * returns error response and does not result in authentication failure. This test
     * is similar to {@link #testUnauthenticatedApiVersionsRequest(SecurityProtocol)}
     * where a non-SASL client is used to send requests that are processed by
     * {@link SaslServerAuthenticator} of the server prior to client authentication.
     */
    @Test
    public void testApiVersionsRequestWithUnsupportedVersion() throws Exception {
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_PLAINTEXT;
        configureMechanisms("PLAIN", Arrays.asList("PLAIN"));
        server = createEchoServer(securityProtocol);

        // Send ApiVersionsRequest with unsupported version and validate error response.
        String node = "1";
        createClientConnection(SecurityProtocol.PLAINTEXT, node);
        RequestHeader header = new RequestHeader(ApiKeys.API_VERSIONS.id, Short.MAX_VALUE, "someclient", 1);
        ApiVersionsRequest request = new ApiVersionsRequest.Builder().build();
        selector.send(request.toSend(node, header));
        ByteBuffer responseBuffer = waitForResponse();
        ResponseHeader.parse(responseBuffer);
        ApiVersionsResponse response = ApiVersionsResponse.parse(responseBuffer);
        assertEquals(Errors.UNSUPPORTED_VERSION.code(), response.errorCode());

        // Send ApiVersionsRequest with a supported version. This should succeed.
        sendVersionRequestReceiveResponse(node);

        // Test that client can authenticate successfully
        sendHandshakeRequestReceiveResponse(node);
        authenticateUsingSaslPlainAndCheckConnection(node);
    }

    /**
     * Tests that unsupported version of SASL handshake request returns error
     * response and fails authentication. This test is similar to
     * {@link #testUnauthenticatedApiVersionsRequest(SecurityProtocol)}
     * where a non-SASL client is used to send requests that are processed by
     * {@link SaslServerAuthenticator} of the server prior to client authentication.
     */
    @Test
    public void testSaslHandshakeRequestWithUnsupportedVersion() throws Exception {
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_PLAINTEXT;
        configureMechanisms("PLAIN", Arrays.asList("PLAIN"));
        server = createEchoServer(securityProtocol);

        // Send ApiVersionsRequest and validate error response.
        String node1 = "invalid1";
        createClientConnection(SecurityProtocol.PLAINTEXT, node1);
        SaslHandshakeRequest request = new SaslHandshakeRequest("PLAIN");
        RequestHeader header = new RequestHeader(ApiKeys.SASL_HANDSHAKE.id, Short.MAX_VALUE, "someclient", 2);
        selector.send(request.toSend(node1, header));
        NetworkTestUtils.waitForChannelClose(selector, node1);
        selector.close();

        // Test good connection still works
        createAndCheckClientConnection(securityProtocol, "good1");
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
        sendHandshakeRequestReceiveResponse(node1);
        Random random = new Random();
        byte[] bytes = new byte[1024];
        random.nextBytes(bytes);
        selector.send(new NetworkSend(node1, ByteBuffer.wrap(bytes)));
        NetworkTestUtils.waitForChannelClose(selector, node1);
        selector.close();

        // Test good connection still works
        createAndCheckClientConnection(securityProtocol, "good1");

        // Send invalid SASL packet before handshake request
        String node2 = "invalid2";
        createClientConnection(SecurityProtocol.PLAINTEXT, node2);
        random.nextBytes(bytes);
        selector.send(new NetworkSend(node2, ByteBuffer.wrap(bytes)));
        NetworkTestUtils.waitForChannelClose(selector, node2);
        selector.close();

        // Test good connection still works
        createAndCheckClientConnection(securityProtocol, "good2");
    }

    /**
     * Tests that ApiVersionsRequest after Kafka SASL handshake request flow,
     * but prior to actual SASL authentication, results in authentication failure.
     * This is similar to {@link #testUnauthenticatedApiVersionsRequest(SecurityProtocol)}
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
        sendHandshakeRequestReceiveResponse(node1);

        ApiVersionsRequest request = new ApiVersionsRequest.Builder().build();
        RequestHeader versionsHeader = new RequestHeader(ApiKeys.API_VERSIONS.id,
                request.version(), "someclient", 2);
        selector.send(request.toSend(node1, versionsHeader));
        NetworkTestUtils.waitForChannelClose(selector, node1);
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
        sendHandshakeRequestReceiveResponse(node1);
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        buffer.putInt(Integer.MAX_VALUE);
        buffer.put(new byte[buffer.capacity() - 4]);
        buffer.rewind();
        selector.send(new NetworkSend(node1, buffer));
        NetworkTestUtils.waitForChannelClose(selector, node1);
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
        NetworkTestUtils.waitForChannelClose(selector, node2);
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
        MetadataRequest metadataRequest1 =
                new MetadataRequest.Builder(Collections.singletonList("sometopic")).build();
        RequestHeader metadataRequestHeader1 = new RequestHeader(ApiKeys.METADATA.id,
                metadataRequest1.version(), "someclient", 1);
        selector.send(metadataRequest1.toSend(node1, metadataRequestHeader1));
        NetworkTestUtils.waitForChannelClose(selector, node1);
        selector.close();

        // Test good connection still works
        createAndCheckClientConnection(securityProtocol, "good1");

        // Send metadata request after Kafka SASL handshake request
        String node2 = "invalid2";
        createClientConnection(SecurityProtocol.PLAINTEXT, node2);
        sendHandshakeRequestReceiveResponse(node2);
        MetadataRequest metadataRequest2 =
                new MetadataRequest.Builder(Collections.singletonList("sometopic")).build();
        RequestHeader metadataRequestHeader2 = new RequestHeader(ApiKeys.METADATA.id,
                metadataRequest2.version(), "someclient", 2);
        selector.send(metadataRequest2.toSend(node2, metadataRequestHeader2));
        NetworkTestUtils.waitForChannelClose(selector, node2);
        selector.close();

        // Test good connection still works
        createAndCheckClientConnection(securityProtocol, "good2");
    }

    /**
     * Tests that connections cannot be created if the login module class is unavailable.
     */
    @Test
    public void testInvalidLoginModule() throws Exception {
        TestJaasConfig jaasConfig = configureMechanisms("PLAIN", Arrays.asList("PLAIN"));
        jaasConfig.createOrUpdateEntry(TestJaasConfig.LOGIN_CONTEXT_CLIENT, "InvalidLoginModule", TestJaasConfig.defaultClientOptions());

        SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
        server = createEchoServer(securityProtocol);
        try {
            createSelector(securityProtocol, saslClientConfigs);
            fail("SASL/PLAIN channel created without valid login module");
        } catch (KafkaException e) {
            // Expected exception
        }
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
        createClientConnection(securityProtocol, node);
        NetworkTestUtils.waitForChannelClose(selector, node);
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
        createClientConnection(securityProtocol, node);
        NetworkTestUtils.waitForChannelClose(selector, node);
    }

    /**
     * Tests dynamic JAAS configuration property for SASL clients. Invalid client credentials
     * are set in the static JVM-wide configuration instance to ensure that the dynamic
     * property override is used during authentication.
     */
    @Test
    public void testDynamicJaasConfiguration() throws Exception {
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
        saslClientConfigs.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        saslServerConfigs.put(SaslConfigs.SASL_ENABLED_MECHANISMS, Arrays.asList("PLAIN"));
        Map<String, Object> serverOptions = new HashMap<>();
        serverOptions.put("user_user1", "user1-secret");
        serverOptions.put("user_user2", "user2-secret");
        TestJaasConfig staticJaasConfig = new TestJaasConfig();
        staticJaasConfig.createOrUpdateEntry(TestJaasConfig.LOGIN_CONTEXT_SERVER, PlainLoginModule.class.getName(),
                serverOptions);
        staticJaasConfig.setPlainClientOptions("user1", "invalidpassword");
        Configuration.setConfiguration(staticJaasConfig);
        server = createEchoServer(securityProtocol);

        // Check that client using static Jaas config does not connect since password is invalid
        createAndCheckClientConnectionFailure(securityProtocol, "1");

        // Check that 'user1' can connect with a Jaas config property override
        saslClientConfigs.put(SaslConfigs.SASL_JAAS_CONFIG, TestJaasConfig.jaasConfigProperty("PLAIN", "user1", "user1-secret"));
        createAndCheckClientConnection(securityProtocol, "2");

        // Check that invalid password specified as Jaas config property results in connection failure
        saslClientConfigs.put(SaslConfigs.SASL_JAAS_CONFIG, TestJaasConfig.jaasConfigProperty("PLAIN", "user1", "user2-secret"));
        createAndCheckClientConnectionFailure(securityProtocol, "3");

        // Check that another user 'user2' can also connect with a Jaas config override without any changes to static configuration
        saslClientConfigs.put(SaslConfigs.SASL_JAAS_CONFIG, TestJaasConfig.jaasConfigProperty("PLAIN", "user2", "user2-secret"));
        createAndCheckClientConnection(securityProtocol, "4");

        // Check that clients specifying multiple login modules fail even if the credentials are valid
        String module1 = TestJaasConfig.jaasConfigProperty("PLAIN", "user1", "user1-secret").value();
        String module2 = TestJaasConfig.jaasConfigProperty("PLAIN", "user2", "user2-secret").value();
        saslClientConfigs.put(SaslConfigs.SASL_JAAS_CONFIG, new Password(module1 + " " + module2));
        try {
            createClientConnection(securityProtocol, "1");
            fail("Connection created with multiple login modules in sasl.jaas.config");
        } catch (IllegalArgumentException e) {
            // Expected
        }
    }

    @Test
    public void testJaasConfigurationForListener() throws Exception {
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_PLAINTEXT;
        saslClientConfigs.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        saslServerConfigs.put(SaslConfigs.SASL_ENABLED_MECHANISMS, Arrays.asList("PLAIN"));

        TestJaasConfig staticJaasConfig = new TestJaasConfig();

        Map<String, Object> globalServerOptions = new HashMap<>();
        globalServerOptions.put("user_global1", "gsecret1");
        globalServerOptions.put("user_global2", "gsecret2");
        staticJaasConfig.createOrUpdateEntry(TestJaasConfig.LOGIN_CONTEXT_SERVER, PlainLoginModule.class.getName(),
                globalServerOptions);

        Map<String, Object> clientListenerServerOptions = new HashMap<>();
        clientListenerServerOptions.put("user_client1", "csecret1");
        clientListenerServerOptions.put("user_client2", "csecret2");
        String clientJaasEntryName = "client." + TestJaasConfig.LOGIN_CONTEXT_SERVER;
        staticJaasConfig.createOrUpdateEntry(clientJaasEntryName, PlainLoginModule.class.getName(), clientListenerServerOptions);
        Configuration.setConfiguration(staticJaasConfig);

        // Listener-specific credentials
        server = createEchoServer(new ListenerName("client"), securityProtocol);
        saslClientConfigs.put(SaslConfigs.SASL_JAAS_CONFIG,
                TestJaasConfig.jaasConfigProperty("PLAIN", "client1", "csecret1"));
        createAndCheckClientConnection(securityProtocol, "1");
        saslClientConfigs.put(SaslConfigs.SASL_JAAS_CONFIG,
                TestJaasConfig.jaasConfigProperty("PLAIN", "global1", "gsecret1"));
        createAndCheckClientConnectionFailure(securityProtocol, "2");
        server.close();

        // Global credentials as there is no listener-specific JAAS entry
        server = createEchoServer(new ListenerName("other"), securityProtocol);
        saslClientConfigs.put(SaslConfigs.SASL_JAAS_CONFIG,
                TestJaasConfig.jaasConfigProperty("PLAIN", "global1", "gsecret1"));
        createAndCheckClientConnection(securityProtocol, "3");
        saslClientConfigs.put(SaslConfigs.SASL_JAAS_CONFIG,
                TestJaasConfig.jaasConfigProperty("PLAIN", "client1", "csecret1"));
        createAndCheckClientConnectionFailure(securityProtocol, "4");
    }

    /**
     * Tests that Kafka ApiVersionsRequests are handled by the SASL server authenticator
     * prior to SASL handshake flow and that subsequent authentication succeeds
     * when transport layer is PLAINTEXT/SSL. This test uses a non-SASL client that simulates
     * SASL authentication after ApiVersionsRequest.
     * <p>
     * Test sequence (using <tt>securityProtocol=PLAINTEXT</tt> as an example):
     * <ol>
     *   <li>Starts a SASL_PLAINTEXT test server that simply echoes back client requests after authentication.</li>
     *   <li>A (non-SASL) PLAINTEXT test client connects to the SASL server port. Client is now unauthenticated.<./li>
     *   <li>The unauthenticated non-SASL client sends an ApiVersionsRequest and validates the response.
     *       A valid response indicates that {@link SaslServerAuthenticator} of the test server responded to
     *       the ApiVersionsRequest even though the client is not yet authenticated.</li>
     *   <li>The unauthenticated non-SASL client sends a SaslHandshakeRequest and validates the response. A valid response
     *       indicates that {@link SaslServerAuthenticator} of the test server responded to the SaslHandshakeRequest
     *       after processing ApiVersionsRequest.</li>
     *   <li>The unauthenticated non-SASL client sends the SASL/PLAIN packet containing username/password to authenticate
     *       itself. The client is now authenticated by the server. At this point this test client is at the
     *       same state as a regular SASL_PLAINTEXT client that is <tt>ready</tt>.</li>
     *   <li>The authenticated client sends random data to the server and checks that the data is echoed
     *       back by the test server (ie, not Kafka request-response) to ensure that the client now
     *       behaves exactly as a regular SASL_PLAINTEXT client that has completed authentication.</li>
     * </ol>
     */
    private void testUnauthenticatedApiVersionsRequest(SecurityProtocol securityProtocol) throws Exception {
        configureMechanisms("PLAIN", Arrays.asList("PLAIN"));
        server = createEchoServer(securityProtocol);

        // Create non-SASL connection to manually authenticate after ApiVersionsRequest
        String node = "1";
        SecurityProtocol clientProtocol;
        switch (securityProtocol) {
            case SASL_PLAINTEXT:
                clientProtocol = SecurityProtocol.PLAINTEXT;
                break;
            case SASL_SSL:
                clientProtocol = SecurityProtocol.SSL;
                break;
            default:
                throw new IllegalArgumentException("Server protocol " + securityProtocol + " is not SASL");
        }
        createClientConnection(clientProtocol, node);
        NetworkTestUtils.waitForChannelReady(selector, node);

        // Send ApiVersionsRequest and check response
        ApiVersionsResponse versionsResponse = sendVersionRequestReceiveResponse(node);
        assertEquals(ProtoUtils.oldestVersion(ApiKeys.SASL_HANDSHAKE.id), versionsResponse.apiVersion(ApiKeys.SASL_HANDSHAKE.id).minVersion);
        assertEquals(ProtoUtils.latestVersion(ApiKeys.SASL_HANDSHAKE.id), versionsResponse.apiVersion(ApiKeys.SASL_HANDSHAKE.id).maxVersion);

        // Send SaslHandshakeRequest and check response
        SaslHandshakeResponse handshakeResponse = sendHandshakeRequestReceiveResponse(node);
        assertEquals(Collections.singletonList("PLAIN"), handshakeResponse.enabledMechanisms());

        // Complete manual authentication and check send/receive succeed
        authenticateUsingSaslPlainAndCheckConnection(node);
    }

    private void authenticateUsingSaslPlainAndCheckConnection(String node) throws Exception {
        // Authenticate using PLAIN username/password
        String authString = "\u0000" + TestJaasConfig.USERNAME + "\u0000" + TestJaasConfig.PASSWORD;
        selector.send(new NetworkSend(node, ByteBuffer.wrap(authString.getBytes("UTF-8"))));
        waitForResponse();

        // Check send/receive on the manually authenticated connection
        NetworkTestUtils.checkClientConnection(selector, node, 100, 10);
    }

    private TestJaasConfig configureMechanisms(String clientMechanism, List<String> serverMechanisms) {
        saslClientConfigs.put(SaslConfigs.SASL_MECHANISM, clientMechanism);
        saslServerConfigs.put(SaslConfigs.SASL_ENABLED_MECHANISMS, serverMechanisms);
        return TestJaasConfig.createConfiguration(clientMechanism, serverMechanisms);
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
        return NetworkTestUtils.createEchoServer(listenerName, securityProtocol,
                new TestSecurityConfig(saslServerConfigs));
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

    private void createAndCheckClientConnectionFailure(SecurityProtocol securityProtocol, String node) throws Exception {
        createClientConnection(securityProtocol, node);
        NetworkTestUtils.waitForChannelClose(selector, node);
        selector.close();
        selector = null;
    }

    private AbstractResponse sendKafkaRequestReceiveResponse(String node, ApiKeys apiKey, AbstractRequest request) throws IOException {
        RequestHeader header =
                new RequestHeader(apiKey.id, request.version(), "someclient", 1);
        Send send = request.toSend(node, header);
        selector.send(send);
        ByteBuffer responseBuffer = waitForResponse();
        return NetworkClient.parseResponse(responseBuffer, header);
    }

    private SaslHandshakeResponse sendHandshakeRequestReceiveResponse(String node) throws Exception {
        SaslHandshakeRequest handshakeRequest = new SaslHandshakeRequest("PLAIN");
        SaslHandshakeResponse response = (SaslHandshakeResponse) sendKafkaRequestReceiveResponse(node, ApiKeys.SASL_HANDSHAKE, handshakeRequest);
        assertEquals(Errors.NONE.code(), response.errorCode());
        return response;
    }

    private ApiVersionsResponse sendVersionRequestReceiveResponse(String node) throws Exception {
        ApiVersionsRequest handshakeRequest = new ApiVersionsRequest.Builder().build();
        ApiVersionsResponse response =  (ApiVersionsResponse) sendKafkaRequestReceiveResponse(node, ApiKeys.API_VERSIONS, handshakeRequest);
        assertEquals(Errors.NONE.code(), response.errorCode());
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
        for (String mechanism : (List<String>) saslServerConfigs.get(SaslConfigs.SASL_ENABLED_MECHANISMS)) {
            ScramMechanism scramMechanism = ScramMechanism.forMechanismName(mechanism);
            if (scramMechanism != null) {
                ScramFormatter formatter = new ScramFormatter(scramMechanism);
                ScramCredential credential = formatter.generateCredential(password, 4096);
                server.credentialCache().cache(scramMechanism.mechanismName(), ScramCredential.class).put(username, credential);
            }
        }
    }
}
