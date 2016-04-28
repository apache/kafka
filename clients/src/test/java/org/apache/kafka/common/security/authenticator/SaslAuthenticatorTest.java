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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import static org.junit.Assert.fail;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.network.CertStores;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.ChannelBuilders;
import org.apache.kafka.common.network.LoginType;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.network.NetworkSend;
import org.apache.kafka.common.network.NetworkTestUtils;
import org.apache.kafka.common.network.NioEchoServer;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestSend;
import org.apache.kafka.common.requests.SaslHandshakeRequest;
import org.apache.kafka.common.security.JaasUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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

        server = NetworkTestUtils.createEchoServer(securityProtocol, saslServerConfigs);
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

        server = NetworkTestUtils.createEchoServer(securityProtocol, saslServerConfigs);
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

        server = NetworkTestUtils.createEchoServer(securityProtocol, saslServerConfigs);
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

        server = NetworkTestUtils.createEchoServer(securityProtocol, saslServerConfigs);
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
        server = NetworkTestUtils.createEchoServer(securityProtocol, saslServerConfigs);
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
        server = NetworkTestUtils.createEchoServer(securityProtocol, saslServerConfigs);
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

        server = NetworkTestUtils.createEchoServer(securityProtocol, saslServerConfigs);
        createAndCheckClientConnection(securityProtocol, node);
    }

    /**
     * Tests that servers supporting multiple SASL mechanisms work with clients using
     * any of the enabled mechanisms.
     */
    @Test
    public void testMultipleServerMechanisms() throws Exception {
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
        configureMechanisms("DIGEST-MD5", Arrays.asList("DIGEST-MD5", "PLAIN"));
        server = NetworkTestUtils.createEchoServer(securityProtocol, saslServerConfigs);

        String node1 = "1";
        saslClientConfigs.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        createAndCheckClientConnection(securityProtocol, node1);

        String node2 = "2";
        saslClientConfigs.put(SaslConfigs.SASL_MECHANISM, "DIGEST-MD5");
        createSelector(securityProtocol, saslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("127.0.0.1", server.port());
        selector.connect(node2, addr, BUFFER_SIZE, BUFFER_SIZE);
        NetworkTestUtils.checkClientConnection(selector, node2, 100, 10);
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
        server = NetworkTestUtils.createEchoServer(securityProtocol, saslServerConfigs);

        // Send invalid SASL packet after valid handshake request
        String node1 = "invalid1";
        createClientConnection(SecurityProtocol.PLAINTEXT, node1);
        RequestHeader header = new RequestHeader(ApiKeys.SASL_HANDSHAKE.id, "someclient", 1);
        SaslHandshakeRequest handshakeRequest = new SaslHandshakeRequest("PLAIN");
        selector.send(new NetworkSend(node1, RequestSend.serialize(header, handshakeRequest.toStruct())));
        while (selector.completedSends().size() == 0)
            selector.poll(1000);
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
     * Tests that packets that are too big during Kafka SASL handshake request flow
     * or the actual SASL authentication flow result in authentication failure
     * and do not cause any failures in the server.
     */
    @Test
    public void testPacketSizeTooBig() throws Exception {
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_PLAINTEXT;
        configureMechanisms("PLAIN", Arrays.asList("PLAIN"));
        server = NetworkTestUtils.createEchoServer(securityProtocol, saslServerConfigs);

        // Send SASL packet with large size after valid handshake request
        String node1 = "invalid1";
        createClientConnection(SecurityProtocol.PLAINTEXT, node1);
        RequestHeader header = new RequestHeader(ApiKeys.SASL_HANDSHAKE.id, "someclient", 1);
        SaslHandshakeRequest handshakeRequest = new SaslHandshakeRequest("PLAIN");
        selector.send(new NetworkSend(node1, RequestSend.serialize(header, handshakeRequest.toStruct())));
        while (selector.completedSends().size() == 0)
            selector.poll(1000);
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
        server = NetworkTestUtils.createEchoServer(securityProtocol, saslServerConfigs);

        // Send metadata request before Kafka SASL handshake request
        String node1 = "invalid1";
        createClientConnection(SecurityProtocol.PLAINTEXT, node1);
        RequestHeader metadataRequestHeader1 = new RequestHeader(ApiKeys.METADATA.id, "someclient", 1);
        MetadataRequest metadataRequest1 = new MetadataRequest(Collections.singletonList("sometopic"));
        selector.send(new NetworkSend(node1, RequestSend.serialize(metadataRequestHeader1, metadataRequest1.toStruct())));
        NetworkTestUtils.waitForChannelClose(selector, node1);
        selector.close();

        // Test good connection still works
        createAndCheckClientConnection(securityProtocol, "good1");

        // Send metadata request after Kafka SASL handshake request
        String node2 = "invalid2";
        createClientConnection(SecurityProtocol.PLAINTEXT, node2);
        RequestHeader handshakeRequestHeader = new RequestHeader(ApiKeys.SASL_HANDSHAKE.id, "someclient", 1);
        SaslHandshakeRequest handshakeRequest = new SaslHandshakeRequest("PLAIN");
        selector.send(new NetworkSend(node2, RequestSend.serialize(handshakeRequestHeader, handshakeRequest.toStruct())));
        while (selector.completedSends().size() == 0)
            selector.poll(1000);
        RequestHeader metadataRequestHeader2 = new RequestHeader(ApiKeys.METADATA.id, "someclient", 2);
        MetadataRequest metadataRequest2 = new MetadataRequest(Collections.singletonList("sometopic"));
        selector.send(new NetworkSend(node2, RequestSend.serialize(metadataRequestHeader2, metadataRequest2.toStruct())));
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
        jaasConfig.createOrUpdateEntry(JaasUtils.LOGIN_CONTEXT_CLIENT, "InvalidLoginModule", TestJaasConfig.defaultClientOptions());

        SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
        server = NetworkTestUtils.createEchoServer(securityProtocol, saslServerConfigs);
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

        server = NetworkTestUtils.createEchoServer(securityProtocol, saslServerConfigs);
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

        server = NetworkTestUtils.createEchoServer(securityProtocol, saslServerConfigs);
        createClientConnection(securityProtocol, node);
        NetworkTestUtils.waitForChannelClose(selector, node);
    }

    private TestJaasConfig configureMechanisms(String clientMechanism, List<String> serverMechanisms) {
        saslClientConfigs.put(SaslConfigs.SASL_MECHANISM, clientMechanism);
        saslServerConfigs.put(SaslConfigs.SASL_ENABLED_MECHANISMS, serverMechanisms);
        return TestJaasConfig.createConfiguration(clientMechanism, serverMechanisms);
    }

    private void createSelector(SecurityProtocol securityProtocol, Map<String, Object> clientConfigs) {
        String saslMechanism = (String) saslClientConfigs.get(SaslConfigs.SASL_MECHANISM);
        this.channelBuilder = ChannelBuilders.create(securityProtocol, Mode.CLIENT, LoginType.CLIENT, clientConfigs, saslMechanism, true);
        this.selector = NetworkTestUtils.createSelector(channelBuilder);
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
}
