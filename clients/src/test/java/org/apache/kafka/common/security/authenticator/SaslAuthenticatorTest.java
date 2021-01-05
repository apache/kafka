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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Base64.Encoder;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersionsResponseKey;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersionsResponseKeyCollection;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsTopicResponse;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsPartitionResponse;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.network.ByteBufferSend;
import org.apache.kafka.common.network.CertStores;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.ChannelBuilders;
import org.apache.kafka.common.network.ChannelMetadataRegistry;
import org.apache.kafka.common.network.ChannelState;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.network.NetworkSend;
import org.apache.kafka.common.network.NetworkTestUtils;
import org.apache.kafka.common.network.NioEchoServer;
import org.apache.kafka.common.network.SaslChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.network.TransportLayer;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.security.auth.Login;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.requests.SaslAuthenticateRequest;
import org.apache.kafka.common.requests.SaslHandshakeRequest;
import org.apache.kafka.common.requests.SaslHandshakeResponse;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.TestSecurityConfig;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerConfigException;
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerIllegalTokenException;
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerUnsecuredJws;
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerUnsecuredLoginCallbackHandler;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.security.scram.ScramCredential;
import org.apache.kafka.common.security.scram.internals.ScramCredentialUtils;
import org.apache.kafka.common.security.scram.internals.ScramFormatter;
import org.apache.kafka.common.security.scram.ScramLoginModule;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.apache.kafka.common.security.token.delegation.TokenInformation;
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.authenticator.TestDigestLoginModule.DigestServerCallbackHandler;
import org.apache.kafka.common.security.plain.internals.PlainServerCallbackHandler;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.kafka.common.protocol.ApiKeys.LIST_OFFSETS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the Sasl authenticator. These use a test harness that runs a simple socket server that echos back responses.
 */
public class SaslAuthenticatorTest {

    private static final long CONNECTIONS_MAX_REAUTH_MS_VALUE = 100L;
    private static final int BUFFER_SIZE = 4 * 1024;
    private static Time time = Time.SYSTEM;

    private NioEchoServer server;
    private Selector selector;
    private ChannelBuilder channelBuilder;
    private CertStores serverCertStores;
    private CertStores clientCertStores;
    private Map<String, Object> saslClientConfigs;
    private Map<String, Object> saslServerConfigs;
    private CredentialCache credentialCache;
    private int nextCorrelationId;

    @Before
    public void setup() throws Exception {
        LoginManager.closeAll();
        time = Time.SYSTEM;
        serverCertStores = new CertStores(true, "localhost");
        clientCertStores = new CertStores(false, "localhost");
        saslServerConfigs = serverCertStores.getTrustingConfig(clientCertStores);
        saslClientConfigs = clientCertStores.getTrustingConfig(serverCertStores);
        credentialCache = new CredentialCache();
        TestLogin.loginCount.set(0);
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
     * Also tests successful re-authentication.
     */
    @Test
    public void testValidSaslPlainOverSsl() throws Exception {
        String node = "0";
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
        configureMechanisms("PLAIN", Arrays.asList("PLAIN"));

        server = createEchoServer(securityProtocol);
        checkAuthenticationAndReauthentication(securityProtocol, node);
    }

    /**
     * Tests good path SASL/PLAIN client and server channels using PLAINTEXT transport layer.
     * Also tests successful re-authentication.
     */
    @Test
    public void testValidSaslPlainOverPlaintext() throws Exception {
        String node = "0";
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_PLAINTEXT;
        configureMechanisms("PLAIN", Arrays.asList("PLAIN"));

        server = createEchoServer(securityProtocol);
        checkAuthenticationAndReauthentication(securityProtocol, node);
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
        server.verifyReauthenticationMetrics(0, 0);
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
        server.verifyReauthenticationMetrics(0, 0);
    }

    /**
     * Tests that SASL/PLAIN clients without valid username fail authentication.
     */
    @Test
    public void testMissingUsernameSaslPlain() throws Exception {
        String node = "0";
        TestJaasConfig jaasConfig = configureMechanisms("PLAIN", Arrays.asList("PLAIN"));
        jaasConfig.setClientOptions("PLAIN", null, "mypassword");

        SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
        server = createEchoServer(securityProtocol);
        createSelector(securityProtocol, saslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("127.0.0.1", server.port());
        try {
            selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);
            fail("SASL/PLAIN channel created without username");
        } catch (IOException e) {
            // Expected exception
            assertTrue("Channels not closed", selector.channels().isEmpty());
            for (SelectionKey key : selector.keys())
                assertFalse("Key not cancelled", key.isValid());
        }
    }

    /**
     * Tests that SASL/PLAIN clients with missing password in JAAS configuration fail authentication.
     */
    @Test
    public void testMissingPasswordSaslPlain() throws Exception {
        String node = "0";
        TestJaasConfig jaasConfig = configureMechanisms("PLAIN", Arrays.asList("PLAIN"));
        jaasConfig.setClientOptions("PLAIN", "myuser", null);

        SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
        server = createEchoServer(securityProtocol);
        createSelector(securityProtocol, saslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("127.0.0.1", server.port());
        try {
            selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);
            fail("SASL/PLAIN channel created without password");
        } catch (IOException e) {
            // Expected exception
        }
    }

    /**
     * Verify that messages from SaslExceptions thrown in the server during authentication are not
     * propagated to the client since these may contain sensitive data.
     */
    @Test
    public void testClientExceptionDoesNotContainSensitiveData() throws Exception {
        InvalidScramServerCallbackHandler.reset();

        SecurityProtocol securityProtocol = SecurityProtocol.SASL_PLAINTEXT;
        TestJaasConfig jaasConfig = configureMechanisms("SCRAM-SHA-256", Collections.singletonList("SCRAM-SHA-256"));
        jaasConfig.createOrUpdateEntry(TestJaasConfig.LOGIN_CONTEXT_SERVER, PlainLoginModule.class.getName(), new HashMap<>());
        String callbackPrefix = ListenerName.forSecurityProtocol(securityProtocol).saslMechanismConfigPrefix("SCRAM-SHA-256");
        saslServerConfigs.put(callbackPrefix + BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS,
                InvalidScramServerCallbackHandler.class.getName());
        server = createEchoServer(securityProtocol);

        try {
            InvalidScramServerCallbackHandler.sensitiveException =
                    new IOException("Could not connect to password database locahost:8000");
            createAndCheckClientAuthenticationFailure(securityProtocol, "1", "SCRAM-SHA-256", null);

            InvalidScramServerCallbackHandler.sensitiveException =
                    new SaslException("Password for existing user " + TestServerCallbackHandler.USERNAME + " is invalid");
            createAndCheckClientAuthenticationFailure(securityProtocol, "1", "SCRAM-SHA-256", null);

            InvalidScramServerCallbackHandler.reset();
            InvalidScramServerCallbackHandler.clientFriendlyException =
                    new SaslAuthenticationException("Credential verification failed");
            createAndCheckClientAuthenticationFailure(securityProtocol, "1", "SCRAM-SHA-256",
                    InvalidScramServerCallbackHandler.clientFriendlyException.getMessage());
        } finally {
            InvalidScramServerCallbackHandler.reset();
        }
    }

    public static class InvalidScramServerCallbackHandler implements AuthenticateCallbackHandler {
        // We want to test three types of exceptions:
        //   1) IOException since we can throw this from callback handlers. This may be sensitive.
        //   2) SaslException (also an IOException) which may contain data from external (or JRE) servers and callbacks and may be sensitive
        //   3) SaslAuthenticationException which is from our own code and is used only for client-friendly exceptions
        // We use two different exceptions here since the only checked exception CallbackHandler can throw is IOException,
        // covering case 1) and 2). For case 3), SaslAuthenticationException is a RuntimeExceptiom.
        static volatile IOException sensitiveException;
        static volatile SaslAuthenticationException clientFriendlyException;

        @Override
        public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        }

        @Override
        public void handle(Callback[] callbacks) throws IOException {
            if (sensitiveException != null)
                throw sensitiveException;
            if (clientFriendlyException != null)
                throw clientFriendlyException;
        }

        @Override
        public void close() {
            reset();
        }

        static void reset() {
            sensitiveException = null;
            clientFriendlyException = null;
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
        configureDigestMd5ServerCallback(securityProtocol);

        server = createEchoServer(securityProtocol);
        createAndCheckClientConnection(securityProtocol, node);
    }

    /**
     * Tests that servers supporting multiple SASL mechanisms work with clients using
     * any of the enabled mechanisms.
     * Also tests successful re-authentication over multiple mechanisms.
     */
    @Test
    public void testMultipleServerMechanisms() throws Exception {
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
        configureMechanisms("DIGEST-MD5", Arrays.asList("DIGEST-MD5", "PLAIN", "SCRAM-SHA-256"));
        configureDigestMd5ServerCallback(securityProtocol);
        server = createEchoServer(securityProtocol);
        updateScramCredentialCache(TestJaasConfig.USERNAME, TestJaasConfig.PASSWORD);

        String node1 = "1";
        saslClientConfigs.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        createAndCheckClientConnection(securityProtocol, node1);
        server.verifyAuthenticationMetrics(1, 0);

        Selector selector2 = null;
        Selector selector3 = null;
        try {
            String node2 = "2";
            saslClientConfigs.put(SaslConfigs.SASL_MECHANISM, "DIGEST-MD5");
            createSelector(securityProtocol, saslClientConfigs);
            selector2 = selector;
            InetSocketAddress addr = new InetSocketAddress("127.0.0.1", server.port());
            selector.connect(node2, addr, BUFFER_SIZE, BUFFER_SIZE);
            NetworkTestUtils.checkClientConnection(selector, node2, 100, 10);
            selector = null; // keeps it from being closed when next one is created
            server.verifyAuthenticationMetrics(2, 0);

            String node3 = "3";
            saslClientConfigs.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256");
            createSelector(securityProtocol, saslClientConfigs);
            selector3 = selector;
            selector.connect(node3, new InetSocketAddress("127.0.0.1", server.port()), BUFFER_SIZE, BUFFER_SIZE);
            NetworkTestUtils.checkClientConnection(selector, node3, 100, 10);
            server.verifyAuthenticationMetrics(3, 0);
            
            /*
             * Now re-authenticate the connections. First we have to sleep long enough so
             * that the next write will cause re-authentication, which we expect to succeed.
             */
            delay((long) (CONNECTIONS_MAX_REAUTH_MS_VALUE * 1.1));
            server.verifyReauthenticationMetrics(0, 0);

            NetworkTestUtils.checkClientConnection(selector2, node2, 100, 10);
            server.verifyReauthenticationMetrics(1, 0);

            NetworkTestUtils.checkClientConnection(selector3, node3, 100, 10);
            server.verifyReauthenticationMetrics(2, 0);
            
        } finally {
            if (selector2 != null)
                selector2.close();
            if (selector3 != null)
                selector3.close();
        }
    }

    /**
     * Tests good path SASL/SCRAM-SHA-256 client and server channels.
     * Also tests successful re-authentication.
     */
    @Test
    public void testValidSaslScramSha256() throws Exception {
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
        configureMechanisms("SCRAM-SHA-256", Arrays.asList("SCRAM-SHA-256"));

        server = createEchoServer(securityProtocol);
        updateScramCredentialCache(TestJaasConfig.USERNAME, TestJaasConfig.PASSWORD);
        checkAuthenticationAndReauthentication(securityProtocol, "0");
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
        createAndCheckClientAuthenticationFailure(securityProtocol, node, "SCRAM-SHA-256", null);
        server.verifyAuthenticationMetrics(0, 1);
        server.verifyReauthenticationMetrics(0, 0);
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
        server.verifyReauthenticationMetrics(0, 0);
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
        server.verifyReauthenticationMetrics(0, 0);
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


    @Test
    public void testTokenAuthenticationOverSaslScram() throws Exception {
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
        TestJaasConfig jaasConfig = configureMechanisms("SCRAM-SHA-256", Arrays.asList("SCRAM-SHA-256"));

        //create jaas config for token auth
        Map<String, Object> options = new HashMap<>();
        String tokenId = "token1";
        String tokenHmac = "abcdefghijkl";
        options.put("username", tokenId); //tokenId
        options.put("password", tokenHmac); //token hmac
        options.put(ScramLoginModule.TOKEN_AUTH_CONFIG, "true"); //enable token authentication
        jaasConfig.createOrUpdateEntry(TestJaasConfig.LOGIN_CONTEXT_CLIENT, ScramLoginModule.class.getName(), options);

        server = createEchoServer(securityProtocol);

        //Check invalid tokenId/tokenInfo in tokenCache
        createAndCheckClientConnectionFailure(securityProtocol, "0");
        server.verifyAuthenticationMetrics(0, 1);

        //Check valid token Info and invalid credentials
        KafkaPrincipal owner = SecurityUtils.parseKafkaPrincipal("User:Owner");
        KafkaPrincipal renewer = SecurityUtils.parseKafkaPrincipal("User:Renewer1");
        TokenInformation tokenInfo = new TokenInformation(tokenId, owner, Collections.singleton(renewer),
            System.currentTimeMillis(), System.currentTimeMillis(), System.currentTimeMillis());
        server.tokenCache().addToken(tokenId, tokenInfo);
        createAndCheckClientConnectionFailure(securityProtocol, "0");
        server.verifyAuthenticationMetrics(0, 2);

        //Check with valid token Info and credentials
        updateTokenCredentialCache(tokenId, tokenHmac);
        createAndCheckClientConnection(securityProtocol, "0");
        server.verifyAuthenticationMetrics(1, 2);
        server.verifyReauthenticationMetrics(0, 0);
    }

    @Test
    public void testTokenReauthenticationOverSaslScram() throws Exception {
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
        TestJaasConfig jaasConfig = configureMechanisms("SCRAM-SHA-256", Arrays.asList("SCRAM-SHA-256"));

        // create jaas config for token auth
        Map<String, Object> options = new HashMap<>();
        String tokenId = "token1";
        String tokenHmac = "abcdefghijkl";
        options.put("username", tokenId); // tokenId
        options.put("password", tokenHmac); // token hmac
        options.put(ScramLoginModule.TOKEN_AUTH_CONFIG, "true"); // enable token authentication
        jaasConfig.createOrUpdateEntry(TestJaasConfig.LOGIN_CONTEXT_CLIENT, ScramLoginModule.class.getName(), options);

        // ensure re-authentication based on token expiry rather than a default value
        saslServerConfigs.put(BrokerSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS, Long.MAX_VALUE);
        /*
         * create a token cache that adjusts the token expiration dynamically so that
         * the first time the expiry is read during authentication we use it to define a
         * session expiration time that we can then sleep through; then the second time
         * the value is read (during re-authentication) it will be in the future.
         */
        Function<Integer, Long> tokenLifetime = callNum -> 10 * callNum * CONNECTIONS_MAX_REAUTH_MS_VALUE;
        DelegationTokenCache tokenCache = new DelegationTokenCache(ScramMechanism.mechanismNames()) {
            int callNum = 0;

            @Override
            public TokenInformation token(String tokenId) {
                TokenInformation baseTokenInfo = super.token(tokenId);
                long thisLifetimeMs = System.currentTimeMillis() + tokenLifetime.apply(++callNum).longValue();
                TokenInformation retvalTokenInfo = new TokenInformation(baseTokenInfo.tokenId(), baseTokenInfo.owner(),
                        baseTokenInfo.renewers(), baseTokenInfo.issueTimestamp(), thisLifetimeMs, thisLifetimeMs);
                return retvalTokenInfo;
            }
        };
        server = createEchoServer(ListenerName.forSecurityProtocol(securityProtocol), securityProtocol, tokenCache);

        KafkaPrincipal owner = SecurityUtils.parseKafkaPrincipal("User:Owner");
        KafkaPrincipal renewer = SecurityUtils.parseKafkaPrincipal("User:Renewer1");
        TokenInformation tokenInfo = new TokenInformation(tokenId, owner, Collections.singleton(renewer),
                System.currentTimeMillis(), System.currentTimeMillis(), System.currentTimeMillis());
        server.tokenCache().addToken(tokenId, tokenInfo);
        updateTokenCredentialCache(tokenId, tokenHmac);
        // initial authentication must succeed
        createClientConnection(securityProtocol, "0");
        checkClientConnection("0");
        // ensure metrics are as expected before trying to re-authenticate
        server.verifyAuthenticationMetrics(1, 0);
        server.verifyReauthenticationMetrics(0, 0);
        /*
         * Now re-authenticate and ensure it succeeds. We have to sleep long enough so
         * that the current delegation token will be expired when the next write occurs;
         * this will trigger a re-authentication. Then the second time the delegation
         * token is read and transmitted to the server it will again have an expiration
         * date in the future.
         */
        delay(tokenLifetime.apply(1));
        checkClientConnection("0");
        server.verifyReauthenticationMetrics(1, 0);
    }

    /**
     * Tests that Kafka ApiVersionsRequests are handled by the SASL server authenticator
     * prior to SASL handshake flow and that subsequent authentication succeeds
     * when transport layer is PLAINTEXT. This test simulates SASL authentication using a
     * (non-SASL) PLAINTEXT client and sends ApiVersionsRequest straight after
     * connection to the server is established, before any SASL-related packets are sent.
     * This test is run with SaslHandshake version 0 and no SaslAuthenticate headers.
     */
    @Test
    public void testUnauthenticatedApiVersionsRequestOverPlaintextHandshakeVersion0() throws Exception {
        testUnauthenticatedApiVersionsRequest(SecurityProtocol.SASL_PLAINTEXT, (short) 0);
    }

    /**
     * See {@link #testUnauthenticatedApiVersionsRequestOverSslHandshakeVersion0()} for test scenario.
     * This test is run with SaslHandshake version 1 and SaslAuthenticate headers.
     */
    @Test
    public void testUnauthenticatedApiVersionsRequestOverPlaintextHandshakeVersion1() throws Exception {
        testUnauthenticatedApiVersionsRequest(SecurityProtocol.SASL_PLAINTEXT, (short) 1);
    }

    /**
     * Tests that Kafka ApiVersionsRequests are handled by the SASL server authenticator
     * prior to SASL handshake flow and that subsequent authentication succeeds
     * when transport layer is SSL. This test simulates SASL authentication using a
     * (non-SASL) SSL client and sends ApiVersionsRequest straight after
     * SSL handshake, before any SASL-related packets are sent.
     * This test is run with SaslHandshake version 0 and no SaslAuthenticate headers.
     */
    @Test
    public void testUnauthenticatedApiVersionsRequestOverSslHandshakeVersion0() throws Exception {
        testUnauthenticatedApiVersionsRequest(SecurityProtocol.SASL_SSL, (short) 0);
    }

    /**
     * See {@link #testUnauthenticatedApiVersionsRequestOverPlaintextHandshakeVersion0()} for test scenario.
     * This test is run with SaslHandshake version 1 and SaslAuthenticate headers.
     */
    @Test
    public void testUnauthenticatedApiVersionsRequestOverSslHandshakeVersion1() throws Exception {
        testUnauthenticatedApiVersionsRequest(SecurityProtocol.SASL_SSL, (short) 1);
    }

    /**
     * Tests that unsupported version of ApiVersionsRequest before SASL handshake request
     * returns error response and does not result in authentication failure. This test
     * is similar to {@link #testUnauthenticatedApiVersionsRequest(SecurityProtocol, short)}
     * where a non-SASL client is used to send requests that are processed by
     * {@link SaslServerAuthenticator} of the server prior to client authentication.
     */
    @Test
    public void testApiVersionsRequestWithServerUnsupportedVersion() throws Exception {
        short handshakeVersion = ApiKeys.SASL_HANDSHAKE.latestVersion();
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_PLAINTEXT;
        configureMechanisms("PLAIN", Arrays.asList("PLAIN"));
        server = createEchoServer(securityProtocol);

        // Send ApiVersionsRequest with unsupported version and validate error response.
        String node = "1";
        createClientConnection(SecurityProtocol.PLAINTEXT, node);

        RequestHeader header = new RequestHeader(new RequestHeaderData().
                setRequestApiKey(ApiKeys.API_VERSIONS.id).
                setRequestApiVersion(Short.MAX_VALUE).
                setClientId("someclient").
                setCorrelationId(1),
                (short) 2);
        ApiVersionsRequest request = new ApiVersionsRequest.Builder().build();
        selector.send(new NetworkSend(node, request.toSend(header)));
        ByteBuffer responseBuffer = waitForResponse();
        ResponseHeader.parse(responseBuffer, ApiKeys.API_VERSIONS.responseHeaderVersion((short) 0));
        ApiVersionsResponse response = ApiVersionsResponse.parse(responseBuffer, (short) 0);
        assertEquals(Errors.UNSUPPORTED_VERSION.code(), response.data().errorCode());

        ApiVersionsResponseKey apiVersion = response.data().apiKeys().find(ApiKeys.API_VERSIONS.id);
        assertNotNull(apiVersion);
        assertEquals(ApiKeys.API_VERSIONS.id, apiVersion.apiKey());
        assertEquals(ApiKeys.API_VERSIONS.oldestVersion(), apiVersion.minVersion());
        assertEquals(ApiKeys.API_VERSIONS.latestVersion(), apiVersion.maxVersion());

        // Send ApiVersionsRequest with a supported version. This should succeed.
        sendVersionRequestReceiveResponse(node);

        // Test that client can authenticate successfully
        sendHandshakeRequestReceiveResponse(node, handshakeVersion);
        authenticateUsingSaslPlainAndCheckConnection(node, handshakeVersion > 0);
    }

    /**
     * Tests correct negotiation of handshake and authenticate api versions by having the server
     * return a higher version than supported on the client.
     * Note, that due to KAFKA-9577 this will require a workaround to effectively bump
     * SASL_HANDSHAKE in the future.
     */
    @Test
    public void testSaslUnsupportedClientVersions() throws Exception {
        configureMechanisms("SCRAM-SHA-512", Arrays.asList("SCRAM-SHA-512"));

        server = startServerApiVersionsUnsupportedByClient(SecurityProtocol.SASL_SSL, "SCRAM-SHA-512");
        updateScramCredentialCache(TestJaasConfig.USERNAME, TestJaasConfig.PASSWORD);

        String node = "0";

        createClientConnection(SecurityProtocol.SASL_SSL, "SCRAM-SHA-512", node, true);
        NetworkTestUtils.checkClientConnection(selector, "0", 100, 10);
    }

    /**
     * Tests that invalid ApiVersionRequest is handled by the server correctly and
     * returns an INVALID_REQUEST error.
     */
    @Test
    public void testInvalidApiVersionsRequest() throws Exception {
        short handshakeVersion = ApiKeys.SASL_HANDSHAKE.latestVersion();
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_PLAINTEXT;
        configureMechanisms("PLAIN", Arrays.asList("PLAIN"));
        server = createEchoServer(securityProtocol);

        // Send ApiVersionsRequest with invalid version and validate error response.
        String node = "1";
        short version = ApiKeys.API_VERSIONS.latestVersion();
        createClientConnection(SecurityProtocol.PLAINTEXT, node);
        RequestHeader header = new RequestHeader(ApiKeys.API_VERSIONS, version, "someclient", 1);
        ApiVersionsRequest request = new ApiVersionsRequest(new ApiVersionsRequestData().
                setClientSoftwareName("  ").
                setClientSoftwareVersion("   "), version);
        selector.send(new NetworkSend(node, request.toSend(header)));
        ByteBuffer responseBuffer = waitForResponse();
        ResponseHeader.parse(responseBuffer, ApiKeys.API_VERSIONS.responseHeaderVersion(version));
        ApiVersionsResponse response =
            ApiVersionsResponse.parse(responseBuffer, version);
        assertEquals(Errors.INVALID_REQUEST.code(), response.data().errorCode());

        // Send ApiVersionsRequest with a supported version. This should succeed.
        sendVersionRequestReceiveResponse(node);

        // Test that client can authenticate successfully
        sendHandshakeRequestReceiveResponse(node, handshakeVersion);
        authenticateUsingSaslPlainAndCheckConnection(node, handshakeVersion > 0);
    }


    @Test
    public void testForBrokenSaslHandshakeVersionBump() {
        assertEquals("It is not possible to easily bump SASL_HANDSHAKE schema"
                        + " due to improper version negotiation in clients < 2.5."
                        + " Please see https://issues.apache.org/jira/browse/KAFKA-9577",
                ApiKeys.SASL_HANDSHAKE.latestVersion(),
                1);
    }

    /**
     * Tests that valid ApiVersionRequest is handled by the server correctly and
     * returns an NONE error.
     */
    @Test
    public void testValidApiVersionsRequest() throws Exception {
        short handshakeVersion = ApiKeys.SASL_HANDSHAKE.latestVersion();
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_PLAINTEXT;
        configureMechanisms("PLAIN", Arrays.asList("PLAIN"));
        server = createEchoServer(securityProtocol);

        // Send ApiVersionsRequest with valid version and validate error response.
        String node = "1";
        short version = ApiKeys.API_VERSIONS.latestVersion();
        createClientConnection(SecurityProtocol.PLAINTEXT, node);
        RequestHeader header = new RequestHeader(ApiKeys.API_VERSIONS, version, "someclient", 1);
        ApiVersionsRequest request = new ApiVersionsRequest.Builder().build(version);
        selector.send(new NetworkSend(node, request.toSend(header)));
        ByteBuffer responseBuffer = waitForResponse();
        ResponseHeader.parse(responseBuffer, ApiKeys.API_VERSIONS.responseHeaderVersion(version));
        ApiVersionsResponse response = ApiVersionsResponse.parse(responseBuffer, version);
        assertEquals(Errors.NONE.code(), response.data().errorCode());

        // Test that client can authenticate successfully
        sendHandshakeRequestReceiveResponse(node, handshakeVersion);
        authenticateUsingSaslPlainAndCheckConnection(node, handshakeVersion > 0);
    }

    /**
     * Tests that unsupported version of SASL handshake request returns error
     * response and fails authentication. This test is similar to
     * {@link #testUnauthenticatedApiVersionsRequest(SecurityProtocol, short)}
     * where a non-SASL client is used to send requests that are processed by
     * {@link SaslServerAuthenticator} of the server prior to client authentication.
     */
    @Test
    public void testSaslHandshakeRequestWithUnsupportedVersion() throws Exception {
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_PLAINTEXT;
        configureMechanisms("PLAIN", Arrays.asList("PLAIN"));
        server = createEchoServer(securityProtocol);

        // Send SaslHandshakeRequest and validate that connection is closed by server.
        String node1 = "invalid1";
        createClientConnection(SecurityProtocol.PLAINTEXT, node1);
        SaslHandshakeRequest request = buildSaslHandshakeRequest("PLAIN", ApiKeys.SASL_HANDSHAKE.latestVersion());
        RequestHeader header = new RequestHeader(ApiKeys.SASL_HANDSHAKE, Short.MAX_VALUE, "someclient", 2);
        
        selector.send(new NetworkSend(node1, request.toSend(header)));
        // This test uses a non-SASL PLAINTEXT client in order to do manual handshake.
        // So the channel is in READY state.
        NetworkTestUtils.waitForChannelClose(selector, node1, ChannelState.READY.state());
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
        sendHandshakeRequestReceiveResponse(node1, (short) 1);
        Random random = new Random();
        byte[] bytes = new byte[1024];
        random.nextBytes(bytes);
        selector.send(new NetworkSend(node1, ByteBufferSend.sizePrefixed(ByteBuffer.wrap(bytes))));
        NetworkTestUtils.waitForChannelClose(selector, node1, ChannelState.READY.state());
        selector.close();

        // Test good connection still works
        createAndCheckClientConnection(securityProtocol, "good1");

        // Send invalid SASL packet before handshake request
        String node2 = "invalid2";
        createClientConnection(SecurityProtocol.PLAINTEXT, node2);
        random.nextBytes(bytes);
        selector.send(new NetworkSend(node2, ByteBufferSend.sizePrefixed(ByteBuffer.wrap(bytes))));
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
        selector.send(new NetworkSend(node1, request.toSend(versionsHeader)));
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
        selector.send(new NetworkSend(node1, ByteBufferSend.sizePrefixed(buffer)));
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
        selector.send(new NetworkSend(node2, ByteBufferSend.sizePrefixed(buffer)));
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
        selector.send(new NetworkSend(node1, metadataRequest1.toSend(metadataRequestHeader1)));
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
        selector.send(new NetworkSend(node2, metadataRequest2.toSend(metadataRequestHeader2)));
        NetworkTestUtils.waitForChannelClose(selector, node2, ChannelState.READY.state());
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
     * Tests SASL client authentication callback handler override.
     */
    @Test
    public void testClientAuthenticateCallbackHandler() throws Exception {
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_PLAINTEXT;
        TestJaasConfig jaasConfig = configureMechanisms("PLAIN", Collections.singletonList("PLAIN"));
        saslClientConfigs.put(SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS, TestClientCallbackHandler.class.getName());
        jaasConfig.setClientOptions("PLAIN", "", ""); // remove username, password in login context

        Map<String, Object> options = new HashMap<>();
        options.put("user_" + TestClientCallbackHandler.USERNAME, TestClientCallbackHandler.PASSWORD);
        jaasConfig.createOrUpdateEntry(TestJaasConfig.LOGIN_CONTEXT_SERVER, PlainLoginModule.class.getName(), options);
        server = createEchoServer(securityProtocol);
        createAndCheckClientConnection(securityProtocol, "good");

        options.clear();
        options.put("user_" + TestClientCallbackHandler.USERNAME, "invalid-password");
        jaasConfig.createOrUpdateEntry(TestJaasConfig.LOGIN_CONTEXT_SERVER, PlainLoginModule.class.getName(), options);
        createAndCheckClientConnectionFailure(securityProtocol, "invalid");
    }

    /**
     * Tests SASL server authentication callback handler override.
     */
    @Test
    public void testServerAuthenticateCallbackHandler() throws Exception {
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_PLAINTEXT;
        TestJaasConfig jaasConfig = configureMechanisms("PLAIN", Collections.singletonList("PLAIN"));
        jaasConfig.createOrUpdateEntry(TestJaasConfig.LOGIN_CONTEXT_SERVER, PlainLoginModule.class.getName(), new HashMap<String, Object>());
        String callbackPrefix = ListenerName.forSecurityProtocol(securityProtocol).saslMechanismConfigPrefix("PLAIN");
        saslServerConfigs.put(callbackPrefix + BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS,
                TestServerCallbackHandler.class.getName());
        server = createEchoServer(securityProtocol);

        // Set client username/password to the values used by `TestServerCallbackHandler`
        jaasConfig.setClientOptions("PLAIN", TestServerCallbackHandler.USERNAME, TestServerCallbackHandler.PASSWORD);
        createAndCheckClientConnection(securityProtocol, "good");

        // Set client username/password to the invalid values
        jaasConfig.setClientOptions("PLAIN", TestJaasConfig.USERNAME, "invalid-password");
        createAndCheckClientConnectionFailure(securityProtocol, "invalid");
    }

    /**
     * Test that callback handlers are only applied to connections for the mechanisms
     * configured for the handler. Test enables two mechanisms 'PLAIN` and `DIGEST-MD5`
     * on the servers with different callback handlers for the two mechanisms. Verifies
     * that clients using both mechanisms authenticate successfully.
     */
    @Test
    public void testAuthenticateCallbackHandlerMechanisms() throws Exception {
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_PLAINTEXT;
        TestJaasConfig jaasConfig = configureMechanisms("DIGEST-MD5", Arrays.asList("DIGEST-MD5", "PLAIN"));

        // Connections should fail using the digest callback handler if listener.mechanism prefix not specified
        saslServerConfigs.put("plain." + BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS,
                TestServerCallbackHandler.class);
        saslServerConfigs.put("digest-md5." + BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS,
                DigestServerCallbackHandler.class);
        server = createEchoServer(securityProtocol);
        createAndCheckClientConnectionFailure(securityProtocol, "invalid");

        // Connections should succeed using the server callback handler associated with the listener
        ListenerName listener = ListenerName.forSecurityProtocol(securityProtocol);
        saslServerConfigs.remove("plain." + BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS);
        saslServerConfigs.remove("digest-md5." + BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS);
        saslServerConfigs.put(listener.saslMechanismConfigPrefix("plain") + BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS,
                TestServerCallbackHandler.class);
        saslServerConfigs.put(listener.saslMechanismConfigPrefix("digest-md5") + BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS,
                DigestServerCallbackHandler.class);
        server = createEchoServer(securityProtocol);

        // Verify that DIGEST-MD5 (currently configured for client) works with `DigestServerCallbackHandler`
        createAndCheckClientConnection(securityProtocol, "good-digest-md5");

        // Verify that PLAIN works with `TestServerCallbackHandler`
        jaasConfig.setClientOptions("PLAIN", TestServerCallbackHandler.USERNAME, TestServerCallbackHandler.PASSWORD);
        saslClientConfigs.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        createAndCheckClientConnection(securityProtocol, "good-plain");
    }

    /**
     * Tests SASL login class override.
     */
    @Test
    public void testClientLoginOverride() throws Exception {
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_PLAINTEXT;
        TestJaasConfig jaasConfig = configureMechanisms("PLAIN", Collections.singletonList("PLAIN"));
        jaasConfig.setClientOptions("PLAIN", "invaliduser", "invalidpassword");
        server = createEchoServer(securityProtocol);

        // Connection should succeed using login override that sets correct username/password in Subject
        saslClientConfigs.put(SaslConfigs.SASL_LOGIN_CLASS, TestLogin.class.getName());
        createAndCheckClientConnection(securityProtocol, "1");
        assertEquals(1, TestLogin.loginCount.get());

        // Connection should fail without login override since username/password in jaas config is invalid
        saslClientConfigs.remove(SaslConfigs.SASL_LOGIN_CLASS);
        createAndCheckClientConnectionFailure(securityProtocol, "invalid");
        assertEquals(1, TestLogin.loginCount.get());
    }

    /**
     * Tests SASL server login class override.
     */
    @Test
    public void testServerLoginOverride() throws Exception {
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_PLAINTEXT;
        configureMechanisms("PLAIN", Collections.singletonList("PLAIN"));
        String prefix = ListenerName.forSecurityProtocol(securityProtocol).saslMechanismConfigPrefix("PLAIN");
        saslServerConfigs.put(prefix + SaslConfigs.SASL_LOGIN_CLASS, TestLogin.class.getName());
        server = createEchoServer(securityProtocol);

        // Login is performed when server channel builder is created (before any connections are made on the server)
        assertEquals(1, TestLogin.loginCount.get());

        createAndCheckClientConnection(securityProtocol, "1");
        assertEquals(1, TestLogin.loginCount.get());
    }

    /**
     * Tests SASL login callback class override.
     */
    @Test
    public void testClientLoginCallbackOverride() throws Exception {
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_PLAINTEXT;
        TestJaasConfig jaasConfig = configureMechanisms("PLAIN", Collections.singletonList("PLAIN"));
        jaasConfig.createOrUpdateEntry(TestJaasConfig.LOGIN_CONTEXT_CLIENT, TestPlainLoginModule.class.getName(),
                Collections.emptyMap());
        server = createEchoServer(securityProtocol);

        // Connection should succeed using login callback override that sets correct username/password
        saslClientConfigs.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, TestLoginCallbackHandler.class.getName());
        createAndCheckClientConnection(securityProtocol, "1");

        // Connection should fail without login callback override since username/password in jaas config is invalid
        saslClientConfigs.remove(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS);
        try {
            createClientConnection(securityProtocol, "invalid");
        } catch (Exception e) {
            assertTrue("Unexpected exception " + e.getCause(), e.getCause() instanceof LoginException);
        }
    }

    /**
     * Tests SASL server login callback class override.
     */
    @Test
    public void testServerLoginCallbackOverride() throws Exception {
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_PLAINTEXT;
        TestJaasConfig jaasConfig = configureMechanisms("PLAIN", Collections.singletonList("PLAIN"));
        jaasConfig.createOrUpdateEntry(TestJaasConfig.LOGIN_CONTEXT_SERVER, TestPlainLoginModule.class.getName(),
                Collections.emptyMap());
        jaasConfig.setClientOptions("PLAIN", TestServerCallbackHandler.USERNAME, TestServerCallbackHandler.PASSWORD);
        ListenerName listenerName = ListenerName.forSecurityProtocol(securityProtocol);
        String prefix = listenerName.saslMechanismConfigPrefix("PLAIN");
        saslServerConfigs.put(prefix + BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS,
                TestServerCallbackHandler.class);
        Class<?> loginCallback = TestLoginCallbackHandler.class;

        try {
            createEchoServer(securityProtocol);
            fail("Should have failed to create server with default login handler");
        } catch (KafkaException e) {
            // Expected exception
        }

        try {
            saslServerConfigs.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, loginCallback);
            createEchoServer(securityProtocol);
            fail("Should have failed to create server with login handler config without listener+mechanism prefix");
        } catch (KafkaException e) {
            // Expected exception
            saslServerConfigs.remove(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS);
        }

        try {
            saslServerConfigs.put("plain." + SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, loginCallback);
            createEchoServer(securityProtocol);
            fail("Should have failed to create server with login handler config without listener prefix");
        } catch (KafkaException e) {
            // Expected exception
            saslServerConfigs.remove("plain." + SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS);
        }

        try {
            saslServerConfigs.put(listenerName.configPrefix() + SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, loginCallback);
            createEchoServer(securityProtocol);
            fail("Should have failed to create server with login handler config without mechanism prefix");
        } catch (KafkaException e) {
            // Expected exception
            saslServerConfigs.remove("plain." + SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS);
        }

        // Connection should succeed using login callback override for mechanism
        saslServerConfigs.put(prefix + SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, loginCallback);
        server = createEchoServer(securityProtocol);
        createAndCheckClientConnection(securityProtocol, "1");
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
        server.verifyReauthenticationMetrics(0, 0);
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
        try {
            createAndCheckClientConnectionFailure(securityProtocol, node);
            fail("Did not generate exception prior to creating channel");
        } catch (IOException expected) {
            server.verifyAuthenticationMetrics(0, 0);
            server.verifyReauthenticationMetrics(0, 0);
            Throwable underlyingCause = expected.getCause().getCause().getCause();
            assertEquals(SaslAuthenticationException.class, underlyingCause.getClass());
            assertEquals("Failed to create SaslClient with mechanism INVALID", underlyingCause.getMessage());
        } finally {
            closeClientConnectionIfNecessary();
        }
    }

    /**
     * Tests dynamic JAAS configuration property for SASL clients. Invalid client credentials
     * are set in the static JVM-wide configuration instance to ensure that the dynamic
     * property override is used during authentication.
     */
    @Test
    public void testClientDynamicJaasConfiguration() throws Exception {
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
        saslClientConfigs.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        saslServerConfigs.put(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG, Arrays.asList("PLAIN"));
        Map<String, Object> serverOptions = new HashMap<>();
        serverOptions.put("user_user1", "user1-secret");
        serverOptions.put("user_user2", "user2-secret");
        TestJaasConfig staticJaasConfig = new TestJaasConfig();
        staticJaasConfig.createOrUpdateEntry(TestJaasConfig.LOGIN_CONTEXT_SERVER, PlainLoginModule.class.getName(),
                serverOptions);
        staticJaasConfig.setClientOptions("PLAIN", "user1", "invalidpassword");
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

    /**
     * Tests dynamic JAAS configuration property for SASL server. Invalid server credentials
     * are set in the static JVM-wide configuration instance to ensure that the dynamic
     * property override is used during authentication.
     */
    @Test
    public void testServerDynamicJaasConfiguration() throws Exception {
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
        saslClientConfigs.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        saslServerConfigs.put(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG, Arrays.asList("PLAIN"));
        Map<String, Object> serverOptions = new HashMap<>();
        serverOptions.put("user_user1", "user1-secret");
        serverOptions.put("user_user2", "user2-secret");
        saslServerConfigs.put("listener.name.sasl_ssl.plain." + SaslConfigs.SASL_JAAS_CONFIG,
                TestJaasConfig.jaasConfigProperty("PLAIN", serverOptions));
        TestJaasConfig staticJaasConfig = new TestJaasConfig();
        staticJaasConfig.createOrUpdateEntry(TestJaasConfig.LOGIN_CONTEXT_SERVER, PlainLoginModule.class.getName(),
                Collections.emptyMap());
        staticJaasConfig.setClientOptions("PLAIN", "user1", "user1-secret");
        Configuration.setConfiguration(staticJaasConfig);
        server = createEchoServer(securityProtocol);

        // Check that 'user1' can connect with static Jaas config
        createAndCheckClientConnection(securityProtocol, "1");

        // Check that user 'user2' can also connect with a Jaas config override
        saslClientConfigs.put(SaslConfigs.SASL_JAAS_CONFIG,
                TestJaasConfig.jaasConfigProperty("PLAIN", "user2", "user2-secret"));
        createAndCheckClientConnection(securityProtocol, "2");
    }

    @Test
    public void testJaasConfigurationForListener() throws Exception {
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_PLAINTEXT;
        saslClientConfigs.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        saslServerConfigs.put(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG, Arrays.asList("PLAIN"));

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
     * Tests good path SASL/PLAIN authentication over PLAINTEXT with old version of server
     * that does not support SASL_AUTHENTICATE headers and new version of client.
     */
    @Test
    public void oldSaslPlainPlaintextServerWithoutSaslAuthenticateHeader() throws Exception {
        verifySaslAuthenticateHeaderInterop(false, true, SecurityProtocol.SASL_PLAINTEXT, "PLAIN");
    }

    /**
     * Tests good path SASL/PLAIN authentication over PLAINTEXT with old version of client
     * that does not support SASL_AUTHENTICATE headers and new version of server.
     */
    @Test
    public void oldSaslPlainPlaintextClientWithoutSaslAuthenticateHeader() throws Exception {
        verifySaslAuthenticateHeaderInterop(true, false, SecurityProtocol.SASL_PLAINTEXT, "PLAIN");
    }

    /**
     * Tests good path SASL/SCRAM authentication over PLAINTEXT with old version of server
     * that does not support SASL_AUTHENTICATE headers and new version of client.
     */
    @Test
    public void oldSaslScramPlaintextServerWithoutSaslAuthenticateHeader() throws Exception {
        verifySaslAuthenticateHeaderInterop(false, true, SecurityProtocol.SASL_PLAINTEXT, "SCRAM-SHA-256");
    }

    /**
     * Tests good path SASL/SCRAM authentication over PLAINTEXT with old version of client
     * that does not support SASL_AUTHENTICATE headers and new version of server.
     */
    @Test
    public void oldSaslScramPlaintextClientWithoutSaslAuthenticateHeader() throws Exception {
        verifySaslAuthenticateHeaderInterop(true, false, SecurityProtocol.SASL_PLAINTEXT, "SCRAM-SHA-256");
    }

    /**
     * Tests good path SASL/PLAIN authentication over SSL with old version of server
     * that does not support SASL_AUTHENTICATE headers and new version of client.
     */
    @Test
    public void oldSaslPlainSslServerWithoutSaslAuthenticateHeader() throws Exception {
        verifySaslAuthenticateHeaderInterop(false, true, SecurityProtocol.SASL_SSL, "PLAIN");
    }

    /**
     * Tests good path SASL/PLAIN authentication over SSL with old version of client
     * that does not support SASL_AUTHENTICATE headers and new version of server.
     */
    @Test
    public void oldSaslPlainSslClientWithoutSaslAuthenticateHeader() throws Exception {
        verifySaslAuthenticateHeaderInterop(true, false, SecurityProtocol.SASL_SSL, "PLAIN");
    }

    /**
     * Tests good path SASL/SCRAM authentication over SSL with old version of server
     * that does not support SASL_AUTHENTICATE headers and new version of client.
     */
    @Test
    public void oldSaslScramSslServerWithoutSaslAuthenticateHeader() throws Exception {
        verifySaslAuthenticateHeaderInterop(false, true, SecurityProtocol.SASL_SSL, "SCRAM-SHA-512");
    }

    /**
     * Tests good path SASL/SCRAM authentication over SSL with old version of client
     * that does not support SASL_AUTHENTICATE headers and new version of server.
     */
    @Test
    public void oldSaslScramSslClientWithoutSaslAuthenticateHeader() throws Exception {
        verifySaslAuthenticateHeaderInterop(true, false, SecurityProtocol.SASL_SSL, "SCRAM-SHA-512");
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
     * Tests OAUTHBEARER client and server channels.
     */
    @Test
    public void testValidSaslOauthBearerMechanism() throws Exception {
        String node = "0";
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
        configureMechanisms("OAUTHBEARER", Arrays.asList("OAUTHBEARER"));
        server = createEchoServer(securityProtocol);
        createAndCheckClientConnection(securityProtocol, node);
    }
    
    /**
     * Re-authentication must fail if principal changes
     */
    @Test
    public void testCannotReauthenticateWithDifferentPrincipal() throws Exception {
        String node = "0";
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
        saslClientConfigs.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS,
                AlternateLoginCallbackHandler.class.getName());
        configureMechanisms(OAuthBearerLoginModule.OAUTHBEARER_MECHANISM,
                Arrays.asList(OAuthBearerLoginModule.OAUTHBEARER_MECHANISM));
        server = createEchoServer(securityProtocol);
        // initial authentication must succeed
        createClientConnection(securityProtocol, node);
        checkClientConnection(node);
        // ensure metrics are as expected before trying to re-authenticate
        server.verifyAuthenticationMetrics(1, 0);
        server.verifyReauthenticationMetrics(0, 0);
        /*
         * Now re-authenticate with a different principal and ensure it fails. We first
         * have to sleep long enough for the background refresh thread to replace the
         * original token with a new one.
         */
        delay(1000L);
        try {
            checkClientConnection(node);
            fail("Re-authentication with a different principal should have failed but did not");
        } catch (AssertionError e) {
            // ignore, expected
            server.verifyReauthenticationMetrics(0, 1);
        }
    }

    @Test
    public void testCorrelationId() {
        SaslClientAuthenticator authenticator = new SaslClientAuthenticator(
              Collections.emptyMap(),
              null,
              "node",
              null,
              null,
              null,
              "plain",
              false,
              null,
              null,
            new LogContext()
        ) {
            @Override
            SaslClient createSaslClient() {
                return null;
            }
        };
        int count = (SaslClientAuthenticator.MAX_RESERVED_CORRELATION_ID - SaslClientAuthenticator.MIN_RESERVED_CORRELATION_ID) * 2;
        Set<Integer> ids = IntStream.range(0, count)
            .mapToObj(i -> authenticator.nextCorrelationId())
            .collect(Collectors.toSet());
        assertEquals(SaslClientAuthenticator.MAX_RESERVED_CORRELATION_ID - SaslClientAuthenticator.MIN_RESERVED_CORRELATION_ID + 1, ids.size());
        ids.forEach(id -> {
            assertTrue(id >= SaslClientAuthenticator.MIN_RESERVED_CORRELATION_ID);
            assertTrue(SaslClientAuthenticator.isReserved(id));
        });
    }

    @Test
    public void testConvertListOffsetResponseToSaslHandshakeResponse() {
        ListOffsetsResponseData data = new ListOffsetsResponseData()
                .setThrottleTimeMs(0)
                .setTopics(Collections.singletonList(new ListOffsetsTopicResponse()
                        .setName("topic")
                        .setPartitions(Collections.singletonList(new ListOffsetsPartitionResponse()
                                .setErrorCode(Errors.NONE.code())
                                .setLeaderEpoch(ListOffsetsResponse.UNKNOWN_EPOCH)
                                .setPartitionIndex(0)
                                .setOffset(0)
                                .setTimestamp(0)))));
        ListOffsetsResponse response = new ListOffsetsResponse(data);
        ByteBuffer buffer = RequestTestUtils.serializeResponseWithHeader(response, LIST_OFFSETS.latestVersion(), 0);
        final RequestHeader header0 = new RequestHeader(LIST_OFFSETS, LIST_OFFSETS.latestVersion(), "id", SaslClientAuthenticator.MIN_RESERVED_CORRELATION_ID);
        Assert.assertThrows(SchemaException.class, () -> NetworkClient.parseResponse(buffer.duplicate(), header0));
        final RequestHeader header1 = new RequestHeader(LIST_OFFSETS, LIST_OFFSETS.latestVersion(), "id", 1);
        Assert.assertThrows(IllegalStateException.class, () -> NetworkClient.parseResponse(buffer.duplicate(), header1));
    }
    
    /**
     * Re-authentication must fail if mechanism changes
     */
    @Test
    public void testCannotReauthenticateWithDifferentMechanism() throws Exception {
        String node = "0";
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
        configureMechanisms("DIGEST-MD5", Arrays.asList("DIGEST-MD5", "PLAIN"));
        configureDigestMd5ServerCallback(securityProtocol);
        server = createEchoServer(securityProtocol);

        String saslMechanism = (String) saslClientConfigs.get(SaslConfigs.SASL_MECHANISM);
        Map<String, ?> configs = new TestSecurityConfig(saslClientConfigs).values();
        this.channelBuilder = new AlternateSaslChannelBuilder(Mode.CLIENT,
                Collections.singletonMap(saslMechanism, JaasContext.loadClientContext(configs)), securityProtocol, null,
                false, saslMechanism, true, credentialCache, null, time);
        this.channelBuilder.configure(configs);
        // initial authentication must succeed
        this.selector = NetworkTestUtils.createSelector(channelBuilder, time);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);
        checkClientConnection(node);
        // ensure metrics are as expected before trying to re-authenticate
        server.verifyAuthenticationMetrics(1, 0);
        server.verifyReauthenticationMetrics(0, 0);
        /*
         * Now re-authenticate with a different mechanism and ensure it fails. We have
         * to sleep long enough so that the next write will trigger a re-authentication.
         */
        delay((long) (CONNECTIONS_MAX_REAUTH_MS_VALUE * 1.1));
        try {
            checkClientConnection(node);
            fail("Re-authentication with a different mechanism should have failed but did not");
        } catch (AssertionError e) {
            // ignore, expected
            server.verifyAuthenticationMetrics(1, 0);
            server.verifyReauthenticationMetrics(0, 1);
        }
    }

    /**
     * Second re-authentication must fail if it is sooner than one second after the first
     */
    @Test
    public void testCannotReauthenticateAgainFasterThanOneSecond() throws Exception {
        String node = "0";
        time = new MockTime();
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
        configureMechanisms(OAuthBearerLoginModule.OAUTHBEARER_MECHANISM,
                Arrays.asList(OAuthBearerLoginModule.OAUTHBEARER_MECHANISM));
        server = createEchoServer(securityProtocol);
        try {
            createClientConnection(securityProtocol, node);
            checkClientConnection(node);
            server.verifyAuthenticationMetrics(1, 0);
            server.verifyReauthenticationMetrics(0, 0);
            /*
             * Now sleep long enough so that the next write will cause re-authentication,
             * which we expect to succeed.
             */
            time.sleep((long) (CONNECTIONS_MAX_REAUTH_MS_VALUE * 1.1));
            checkClientConnection(node);
            server.verifyAuthenticationMetrics(1, 0);
            server.verifyReauthenticationMetrics(1, 0);
            /*
             * Now sleep long enough so that the next write will cause re-authentication,
             * but this time we expect re-authentication to not occur since it has been too
             * soon. The checkClientConnection() call should return an error saying it
             * expected the one byte-plus-node response but got the SaslHandshakeRequest
             * instead
             */
            time.sleep((long) (CONNECTIONS_MAX_REAUTH_MS_VALUE * 1.1));
            AssertionError exception = assertThrows(AssertionError.class,
                () -> NetworkTestUtils.checkClientConnection(selector, node, 1, 1));
            String expectedResponseTextRegex = "\\w-" + node;
            String receivedResponseTextRegex = ".*" + OAuthBearerLoginModule.OAUTHBEARER_MECHANISM;
            assertTrue(
                    "Should have received the SaslHandshakeRequest bytes back since we re-authenticated too quickly, " +
                    "but instead we got our generated message echoed back, implying re-auth succeeded when it " +
                    "should not have: " + exception,
                    exception.getMessage().matches(
                            ".*<\\[" + expectedResponseTextRegex + "]>.*<\\[" + receivedResponseTextRegex + ".*?]>"));
            server.verifyReauthenticationMetrics(1, 0); // unchanged
        } finally {
            selector.close();
            selector = null;
        }
    }

    /**
     * Tests good path SASL/PLAIN client and server channels using SSL transport layer.
     * Repeatedly tests successful re-authentication over several seconds.
     */
    @Test
    public void testRepeatedValidSaslPlainOverSsl() throws Exception {
        String node = "0";
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
        configureMechanisms("PLAIN", Arrays.asList("PLAIN"));
        /*
         * Make sure 85% of this value is at least 1 second otherwise it is possible for
         * the client to start re-authenticating but the server does not start due to
         * the 1-second minimum. If this happens the SASL HANDSHAKE request that was
         * injected to start re-authentication will be echoed back to the client instead
         * of the data that the client explicitly sent, and then the client will not
         * recognize that data and will throw an assertion error.
         */
        saslServerConfigs.put(BrokerSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS,
                Double.valueOf(1.1 * 1000L / 0.85).longValue());

        server = createEchoServer(securityProtocol);
        createClientConnection(securityProtocol, node);
        checkClientConnection(node);
        server.verifyAuthenticationMetrics(1, 0);
        server.verifyReauthenticationMetrics(0, 0);
        double successfulReauthentications = 0;
        int desiredNumReauthentications = 5;
        long startMs = Time.SYSTEM.milliseconds();
        long timeoutMs = startMs + 1000 * 15; // stop after 15 seconds
        while (successfulReauthentications < desiredNumReauthentications
                && Time.SYSTEM.milliseconds() < timeoutMs) {
            checkClientConnection(node);
            successfulReauthentications = server.metricValue("successful-reauthentication-total");
        }
        server.verifyReauthenticationMetrics(desiredNumReauthentications, 0);
    }
    
    /**
     * Tests OAUTHBEARER client channels without tokens for the server.
     */
    @Test
    public void testValidSaslOauthBearerMechanismWithoutServerTokens() throws Exception {
        String node = "0";
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
        saslClientConfigs.put(SaslConfigs.SASL_MECHANISM, "OAUTHBEARER");
        saslServerConfigs.put(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG, Arrays.asList("OAUTHBEARER"));
        saslClientConfigs.put(SaslConfigs.SASL_JAAS_CONFIG,
                TestJaasConfig.jaasConfigProperty("OAUTHBEARER", Collections.singletonMap("unsecuredLoginStringClaim_sub", TestJaasConfig.USERNAME)));
        saslServerConfigs.put("listener.name.sasl_ssl.oauthbearer." + SaslConfigs.SASL_JAAS_CONFIG,
                TestJaasConfig.jaasConfigProperty("OAUTHBEARER", Collections.emptyMap()));

        // Server without a token should start up successfully and authenticate clients.
        server = createEchoServer(securityProtocol);
        createAndCheckClientConnection(securityProtocol, node);

        // Client without a token should fail to connect
        saslClientConfigs.put(SaslConfigs.SASL_JAAS_CONFIG,
                TestJaasConfig.jaasConfigProperty("OAUTHBEARER", Collections.emptyMap()));
        createAndCheckClientConnectionFailure(securityProtocol, node);

        // Server with extensions, but without a token should fail to start up since it could indicate a configuration error
        saslServerConfigs.put("listener.name.sasl_ssl.oauthbearer." + SaslConfigs.SASL_JAAS_CONFIG,
                TestJaasConfig.jaasConfigProperty("OAUTHBEARER", Collections.singletonMap("unsecuredLoginExtension_test", "something")));
        try {
            createEchoServer(securityProtocol);
            fail("Server created with invalid login config containing extensions without a token");
        } catch (Throwable e) {
            assertTrue("Unexpected exception " + Utils.stackTrace(e), e.getCause() instanceof LoginException);
        }
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

    private void verifySaslAuthenticateHeaderInterop(boolean enableHeaderOnServer, boolean enableHeaderOnClient,
            SecurityProtocol securityProtocol, String saslMechanism) throws Exception {
        configureMechanisms(saslMechanism, Arrays.asList(saslMechanism));
        createServer(securityProtocol, saslMechanism, enableHeaderOnServer);

        String node = "0";
        createClientConnection(securityProtocol, saslMechanism, node, enableHeaderOnClient);
        NetworkTestUtils.checkClientConnection(selector, "0", 100, 10);
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
        NetworkTestUtils.waitForChannelClose(selector, node, ChannelState.State.AUTHENTICATE);
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

    private NioEchoServer startServerApiVersionsUnsupportedByClient(final SecurityProtocol securityProtocol, String saslMechanism) throws Exception {
        final ListenerName listenerName = ListenerName.forSecurityProtocol(securityProtocol);
        final Map<String, ?> configs = Collections.emptyMap();
        final JaasContext jaasContext = JaasContext.loadServerContext(listenerName, saslMechanism, configs);
        final Map<String, JaasContext> jaasContexts = Collections.singletonMap(saslMechanism, jaasContext);

        boolean isScram = ScramMechanism.isScram(saslMechanism);
        if (isScram)
            ScramCredentialUtils.createCache(credentialCache, Arrays.asList(saslMechanism));
        SaslChannelBuilder serverChannelBuilder = new SaslChannelBuilder(Mode.SERVER, jaasContexts,
                securityProtocol, listenerName, false, saslMechanism, true,
                credentialCache, null, time, new LogContext()) {

            @Override
            protected SaslServerAuthenticator buildServerAuthenticator(Map<String, ?> configs,
                                                                       Map<String, AuthenticateCallbackHandler> callbackHandlers,
                                                                       String id,
                                                                       TransportLayer transportLayer,
                                                                       Map<String, Subject> subjects,
                                                                       Map<String, Long> connectionsMaxReauthMsByMechanism,
                                                                       ChannelMetadataRegistry metadataRegistry) {
                return new SaslServerAuthenticator(configs, callbackHandlers, id, subjects, null, listenerName,
                        securityProtocol, transportLayer, connectionsMaxReauthMsByMechanism, metadataRegistry, time) {

                    @Override
                    protected ApiVersionsResponse apiVersionsResponse() {
                        ApiVersionsResponseKeyCollection versionCollection = new ApiVersionsResponseKeyCollection(2);
                        versionCollection.add(new ApiVersionsResponseKey().setApiKey(ApiKeys.SASL_HANDSHAKE.id).setMinVersion((short) 0).setMaxVersion((short) 100));
                        versionCollection.add(new ApiVersionsResponseKey().setApiKey(ApiKeys.SASL_AUTHENTICATE.id).setMinVersion((short) 0).setMaxVersion((short) 100));
                        return new ApiVersionsResponse(new ApiVersionsResponseData().setApiKeys(versionCollection));
                    }
                };
            }
        };
        serverChannelBuilder.configure(saslServerConfigs);
        server = new NioEchoServer(listenerName, securityProtocol, new TestSecurityConfig(saslServerConfigs),
                "localhost", serverChannelBuilder, credentialCache, time);
        server.start();
        return server;
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
                securityProtocol, listenerName, false, saslMechanism, true,
                credentialCache, null, time, new LogContext()) {

            @Override
            protected SaslServerAuthenticator buildServerAuthenticator(Map<String, ?> configs,
                                                                       Map<String, AuthenticateCallbackHandler> callbackHandlers,
                                                                       String id,
                                                                       TransportLayer transportLayer,
                                                                       Map<String, Subject> subjects,
                                                                       Map<String, Long> connectionsMaxReauthMsByMechanism,
                                                                       ChannelMetadataRegistry metadataRegistry) {
                return new SaslServerAuthenticator(configs, callbackHandlers, id, subjects, null, listenerName,
                    securityProtocol, transportLayer, connectionsMaxReauthMsByMechanism, metadataRegistry, time) {

                    @Override
                    protected ApiVersionsResponse apiVersionsResponse() {
                        ApiVersionsResponse defaultApiVersionResponse = ApiVersionsResponse.DEFAULT_API_VERSIONS_RESPONSE;
                        ApiVersionsResponseKeyCollection apiVersions = new ApiVersionsResponseKeyCollection();
                        for (ApiVersionsResponseKey apiVersion : defaultApiVersionResponse.data().apiKeys()) {
                            if (apiVersion.apiKey() != ApiKeys.SASL_AUTHENTICATE.id) {
                                // ApiVersionsResponseKey can NOT be reused in second ApiVersionsResponseKeyCollection
                                // due to the internal pointers it contains.
                                apiVersions.add(new ApiVersionsResponseKey()
                                    .setApiKey(apiVersion.apiKey())
                                    .setMinVersion(apiVersion.minVersion())
                                    .setMaxVersion(apiVersion.maxVersion())
                                );
                            }

                        }
                        ApiVersionsResponseData data = new ApiVersionsResponseData()
                            .setErrorCode(Errors.NONE.code())
                            .setThrottleTimeMs(0)
                            .setApiKeys(apiVersions);
                        return new ApiVersionsResponse(data);
                    }

                    @Override
                    protected void enableKafkaSaslAuthenticateHeaders(boolean flag) {
                        // Don't enable Kafka SASL_AUTHENTICATE headers
                    }
                };
            }
        };
        serverChannelBuilder.configure(saslServerConfigs);
        server = new NioEchoServer(listenerName, securityProtocol, new TestSecurityConfig(saslServerConfigs),
                "localhost", serverChannelBuilder, credentialCache, time);
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
                securityProtocol, listenerName, false, saslMechanism, true,
                null, null, time, new LogContext()) {

            @Override
            protected SaslClientAuthenticator buildClientAuthenticator(Map<String, ?> configs,
                                                                       AuthenticateCallbackHandler callbackHandler,
                                                                       String id,
                                                                       String serverHost,
                                                                       String servicePrincipal,
                                                                       TransportLayer transportLayer,
                                                                       Subject subject) {

                return new SaslClientAuthenticator(configs, callbackHandler, id, subject,
                        servicePrincipal, serverHost, saslMechanism, true,
                        transportLayer, time, new LogContext()) {
                    @Override
                    protected SaslHandshakeRequest createSaslHandshakeRequest(short version) {
                        return buildSaslHandshakeRequest(saslMechanism, (short) 0);
                    }
                    @Override
                    protected void setSaslAuthenticateAndHandshakeVersions(ApiVersionsResponse apiVersionsResponse) {
                        // Don't set version so that headers are disabled
                    }
                };
            }
        };
        clientChannelBuilder.configure(saslClientConfigs);
        this.selector = NetworkTestUtils.createSelector(clientChannelBuilder, time);
        InetSocketAddress addr = new InetSocketAddress("127.0.0.1", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);
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
    private void testUnauthenticatedApiVersionsRequest(SecurityProtocol securityProtocol, short saslHandshakeVersion) throws Exception {
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
        assertEquals(ApiKeys.SASL_HANDSHAKE.oldestVersion(), versionsResponse.apiVersion(ApiKeys.SASL_HANDSHAKE.id).minVersion());
        assertEquals(ApiKeys.SASL_HANDSHAKE.latestVersion(), versionsResponse.apiVersion(ApiKeys.SASL_HANDSHAKE.id).maxVersion());
        assertEquals(ApiKeys.SASL_AUTHENTICATE.oldestVersion(), versionsResponse.apiVersion(ApiKeys.SASL_AUTHENTICATE.id).minVersion());
        assertEquals(ApiKeys.SASL_AUTHENTICATE.latestVersion(), versionsResponse.apiVersion(ApiKeys.SASL_AUTHENTICATE.id).maxVersion());

        // Send SaslHandshakeRequest and check response
        SaslHandshakeResponse handshakeResponse = sendHandshakeRequestReceiveResponse(node, saslHandshakeVersion);
        assertEquals(Collections.singletonList("PLAIN"), handshakeResponse.enabledMechanisms());

        // Complete manual authentication and check send/receive succeed
        authenticateUsingSaslPlainAndCheckConnection(node, saslHandshakeVersion > 0);
    }

    private void authenticateUsingSaslPlainAndCheckConnection(String node, boolean enableSaslAuthenticateHeader) throws Exception {
        // Authenticate using PLAIN username/password
        String authString = "\u0000" + TestJaasConfig.USERNAME + "\u0000" + TestJaasConfig.PASSWORD;
        ByteBuffer authBuf = ByteBuffer.wrap(authString.getBytes("UTF-8"));
        if (enableSaslAuthenticateHeader) {
            SaslAuthenticateRequestData data = new SaslAuthenticateRequestData().setAuthBytes(authBuf.array());
            SaslAuthenticateRequest request = new SaslAuthenticateRequest.Builder(data).build();
            sendKafkaRequestReceiveResponse(node, ApiKeys.SASL_AUTHENTICATE, request);
        } else {
            selector.send(new NetworkSend(node, ByteBufferSend.sizePrefixed(authBuf)));
            waitForResponse();
        }

        // Check send/receive on the manually authenticated connection
        NetworkTestUtils.checkClientConnection(selector, node, 100, 10);
    }

    private TestJaasConfig configureMechanisms(String clientMechanism, List<String> serverMechanisms) {
        saslClientConfigs.put(SaslConfigs.SASL_MECHANISM, clientMechanism);
        saslServerConfigs.put(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG, serverMechanisms);
        saslServerConfigs.put(BrokerSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS, CONNECTIONS_MAX_REAUTH_MS_VALUE);
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
                new TestSecurityConfig(clientConfigs), null, saslMechanism, time,
                true, new LogContext());
        this.selector = NetworkTestUtils.createSelector(channelBuilder, time);
    }

    private NioEchoServer createEchoServer(SecurityProtocol securityProtocol) throws Exception {
        return createEchoServer(ListenerName.forSecurityProtocol(securityProtocol), securityProtocol);
    }

    private NioEchoServer createEchoServer(ListenerName listenerName, SecurityProtocol securityProtocol) throws Exception {
        return NetworkTestUtils.createEchoServer(listenerName, securityProtocol,
                new TestSecurityConfig(saslServerConfigs), credentialCache, time);
    }

    private NioEchoServer createEchoServer(ListenerName listenerName, SecurityProtocol securityProtocol,
            DelegationTokenCache tokenCache) throws Exception {
        return NetworkTestUtils.createEchoServer(listenerName, securityProtocol,
                new TestSecurityConfig(saslServerConfigs), credentialCache, 100, time, tokenCache);
    }

    private void createClientConnection(SecurityProtocol securityProtocol, String node) throws Exception {
        createSelector(securityProtocol, saslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);
    }
    
    private void checkClientConnection(String node) throws Exception {
        NetworkTestUtils.checkClientConnection(selector, node, 100, 10);
    }

    private void closeClientConnectionIfNecessary() throws Exception {
        if (selector != null) {
            selector.close();
            selector = null;
        }
    }

    /*
     * Also closes the connection after creating/checking it
     */
    private void createAndCheckClientConnection(SecurityProtocol securityProtocol, String node) throws Exception {
        try {
            createClientConnection(securityProtocol, node);
            checkClientConnection(node);
        } finally {
            closeClientConnectionIfNecessary();
        }
    }

    private void createAndCheckClientAuthenticationFailure(SecurityProtocol securityProtocol, String node,
            String mechanism, String expectedErrorMessage) throws Exception {
        ChannelState finalState = createAndCheckClientConnectionFailure(securityProtocol, node);
        Exception exception = finalState.exception();
        assertTrue("Invalid exception class " + exception.getClass(), exception instanceof SaslAuthenticationException);
        String expectedExceptionMessage = expectedErrorMessage != null ? expectedErrorMessage :
                "Authentication failed during authentication due to invalid credentials with SASL mechanism " + mechanism;
        assertEquals(expectedExceptionMessage, exception.getMessage());
    }

    private ChannelState createAndCheckClientConnectionFailure(SecurityProtocol securityProtocol, String node)
            throws Exception {
        try {
            createClientConnection(securityProtocol, node);
            ChannelState finalState = NetworkTestUtils.waitForChannelClose(selector, node, ChannelState.State.AUTHENTICATION_FAILED);
            return finalState;
        } finally {
            closeClientConnectionIfNecessary();
        }
    }

    private void checkAuthenticationAndReauthentication(SecurityProtocol securityProtocol, String node)
            throws Exception {
        try {
            createClientConnection(securityProtocol, node);
            checkClientConnection(node);
            server.verifyAuthenticationMetrics(1, 0);
            /*
             * Now re-authenticate the connection. First we have to sleep long enough so
             * that the next write will cause re-authentication, which we expect to succeed.
             */
            delay((long) (CONNECTIONS_MAX_REAUTH_MS_VALUE * 1.1));
            server.verifyReauthenticationMetrics(0, 0);
            checkClientConnection(node);
            server.verifyReauthenticationMetrics(1, 0);
        } finally {
            closeClientConnectionIfNecessary();
        }
    }

    private AbstractResponse sendKafkaRequestReceiveResponse(String node, ApiKeys apiKey, AbstractRequest request) throws IOException {
        RequestHeader header = new RequestHeader(apiKey, request.version(), "someclient", nextCorrelationId++);
        NetworkSend send = new NetworkSend(node, request.toSend(header));
        selector.send(send);
        ByteBuffer responseBuffer = waitForResponse();
        return NetworkClient.parseResponse(responseBuffer, header);
    }

    private SaslHandshakeResponse sendHandshakeRequestReceiveResponse(String node, short version) throws Exception {
        SaslHandshakeRequest handshakeRequest = buildSaslHandshakeRequest("PLAIN", version);
        SaslHandshakeResponse response = (SaslHandshakeResponse) sendKafkaRequestReceiveResponse(node, ApiKeys.SASL_HANDSHAKE, handshakeRequest);
        assertEquals(Errors.NONE, response.error());
        return response;
    }

    private ApiVersionsResponse sendVersionRequestReceiveResponse(String node) throws Exception {
        ApiVersionsRequest handshakeRequest = createApiVersionsRequestV0();
        ApiVersionsResponse response =  (ApiVersionsResponse) sendKafkaRequestReceiveResponse(node, ApiKeys.API_VERSIONS, handshakeRequest);
        assertEquals(Errors.NONE.code(), response.data().errorCode());
        return response;
    }

    private ByteBuffer waitForResponse() throws IOException {
        int waitSeconds = 10;
        do {
            selector.poll(1000);
        } while (selector.completedReceives().isEmpty() && waitSeconds-- > 0);
        assertEquals(1, selector.completedReceives().size());
        return selector.completedReceives().iterator().next().payload();
    }

    public static class TestServerCallbackHandler extends PlainServerCallbackHandler {

        static final String USERNAME = "TestServerCallbackHandler-user";
        static final String PASSWORD = "TestServerCallbackHandler-password";
        private volatile boolean configured;

        @Override
        public void configure(Map<String, ?> configs, String mechanism, List<AppConfigurationEntry> jaasConfigEntries) {
            if (configured)
                throw new IllegalStateException("Server callback handler configured twice");
            configured = true;
            super.configure(configs, mechanism, jaasConfigEntries);
        }

        @Override
        protected boolean authenticate(String username, char[] password) {
            if (!configured)
                throw new IllegalStateException("Server callback handler not configured");
            return USERNAME.equals(username) && new String(password).equals(PASSWORD);
        }
    }

    private SaslHandshakeRequest buildSaslHandshakeRequest(String mechanism, short version) {
        return new SaslHandshakeRequest.Builder(
                new SaslHandshakeRequestData().setMechanism(mechanism)).build(version);
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

    @SuppressWarnings("unchecked")
    private void updateTokenCredentialCache(String username, String password) throws NoSuchAlgorithmException {
        for (String mechanism : (List<String>) saslServerConfigs.get(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG)) {
            ScramMechanism scramMechanism = ScramMechanism.forMechanismName(mechanism);
            if (scramMechanism != null) {
                ScramFormatter formatter = new ScramFormatter(scramMechanism);
                ScramCredential credential = formatter.generateCredential(password, 4096);
                server.tokenCache().credentialCache(scramMechanism.mechanismName()).put(username, credential);
            }
        }
    }

    private static void delay(long delayMillis) throws InterruptedException {
        final long startTime = System.currentTimeMillis();
        while ((System.currentTimeMillis() - startTime) < delayMillis)
            Thread.sleep(CONNECTIONS_MAX_REAUTH_MS_VALUE / 5);
    }

    public static class TestClientCallbackHandler implements AuthenticateCallbackHandler {

        static final String USERNAME = "TestClientCallbackHandler-user";
        static final String PASSWORD = "TestClientCallbackHandler-password";
        private volatile boolean configured;

        @Override
        public void configure(Map<String, ?> configs, String mechanism, List<AppConfigurationEntry> jaasConfigEntries) {
            if (configured)
                throw new IllegalStateException("Client callback handler configured twice");
            configured = true;
        }

        @Override
        public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
            if (!configured)
                throw new IllegalStateException("Client callback handler not configured");
            for (Callback callback : callbacks) {
                if (callback instanceof NameCallback)
                    ((NameCallback) callback).setName(USERNAME);
                else if (callback instanceof PasswordCallback)
                    ((PasswordCallback) callback).setPassword(PASSWORD.toCharArray());
                else
                    throw new UnsupportedCallbackException(callback);
            }
        }

        @Override
        public void close() {
        }
    }

    public static class TestLogin implements Login {

        static AtomicInteger loginCount = new AtomicInteger();

        private String contextName;
        private Configuration configuration;
        private Subject subject;
        @Override
        public void configure(Map<String, ?> configs, String contextName, Configuration configuration,
                              AuthenticateCallbackHandler callbackHandler) {
            assertEquals(1, configuration.getAppConfigurationEntry(contextName).length);
            this.contextName = contextName;
            this.configuration = configuration;
        }

        @Override
        public LoginContext login() throws LoginException {
            LoginContext context = new LoginContext(contextName, null, new AbstractLogin.DefaultLoginCallbackHandler(), configuration);
            context.login();
            subject = context.getSubject();
            subject.getPublicCredentials().clear();
            subject.getPrivateCredentials().clear();
            subject.getPublicCredentials().add(TestJaasConfig.USERNAME);
            subject.getPrivateCredentials().add(TestJaasConfig.PASSWORD);
            loginCount.incrementAndGet();
            return context;
        }

        @Override
        public Subject subject() {
            return subject;
        }

        @Override
        public String serviceName() {
            return "kafka";
        }

        @Override
        public void close() {
        }
    }

    public static class TestLoginCallbackHandler implements AuthenticateCallbackHandler {
        private volatile boolean configured = false;
        @Override
        public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
            if (configured)
                throw new IllegalStateException("Login callback handler configured twice");
            configured = true;
        }

        @Override
        public void handle(Callback[] callbacks) {
            if (!configured)
                throw new IllegalStateException("Login callback handler not configured");

            for (Callback callback : callbacks) {
                if (callback instanceof NameCallback)
                    ((NameCallback) callback).setName(TestJaasConfig.USERNAME);
                else if (callback instanceof PasswordCallback)
                    ((PasswordCallback) callback).setPassword(TestJaasConfig.PASSWORD.toCharArray());
            }
        }

        @Override
        public void close() {
        }
    }

    public static final class TestPlainLoginModule extends PlainLoginModule {
        @Override
        public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String, ?> sharedState, Map<String, ?> options) {
            try {
                NameCallback nameCallback = new NameCallback("name:");
                PasswordCallback passwordCallback = new PasswordCallback("password:", false);
                callbackHandler.handle(new Callback[]{nameCallback, passwordCallback});
                subject.getPublicCredentials().add(nameCallback.getName());
                subject.getPrivateCredentials().add(new String(passwordCallback.getPassword()));
            } catch (Exception e) {
                throw new SaslAuthenticationException("Login initialization failed", e);
            }
        }
    }

    /*
     * Create an alternate login callback handler that continually returns a
     * different principal
     */
    public static class AlternateLoginCallbackHandler implements AuthenticateCallbackHandler {
        private static final OAuthBearerUnsecuredLoginCallbackHandler DELEGATE = new OAuthBearerUnsecuredLoginCallbackHandler();
        private static final String QUOTE = "\"";
        private static int numInvocations = 0;

        @Override
        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            DELEGATE.handle(callbacks);
            // now change any returned token to have a different principal name
            if (callbacks.length > 0)
                for (Callback callback : callbacks) {
                    if (callback instanceof OAuthBearerTokenCallback) {
                        OAuthBearerTokenCallback oauthBearerTokenCallback = (OAuthBearerTokenCallback) callback;
                        OAuthBearerToken token = oauthBearerTokenCallback.token();
                        if (token != null) {
                            String changedPrincipalNameToUse = token.principalName()
                                    + String.valueOf(++numInvocations);
                            String headerJson = "{" + claimOrHeaderJsonText("alg", "none") + "}";
                            /*
                             * Use a short lifetime so the background refresh thread replaces it before we
                             * re-authenticate
                             */
                            String lifetimeSecondsValueToUse = "1";
                            String claimsJson;
                            try {
                                claimsJson = String.format("{%s,%s,%s}",
                                        expClaimText(Long.parseLong(lifetimeSecondsValueToUse)),
                                        claimOrHeaderJsonText("iat", time.milliseconds() / 1000.0),
                                        claimOrHeaderJsonText("sub", changedPrincipalNameToUse));
                            } catch (NumberFormatException e) {
                                throw new OAuthBearerConfigException(e.getMessage());
                            }
                            try {
                                Encoder urlEncoderNoPadding = Base64.getUrlEncoder().withoutPadding();
                                OAuthBearerUnsecuredJws jws = new OAuthBearerUnsecuredJws(String.format("%s.%s.",
                                        urlEncoderNoPadding.encodeToString(headerJson.getBytes(StandardCharsets.UTF_8)),
                                        urlEncoderNoPadding
                                                .encodeToString(claimsJson.getBytes(StandardCharsets.UTF_8))),
                                        "sub", "scope");
                                oauthBearerTokenCallback.token(jws);
                            } catch (OAuthBearerIllegalTokenException e) {
                                // occurs if the principal claim doesn't exist or has an empty value
                                throw new OAuthBearerConfigException(e.getMessage(), e);
                            }
                        }
                    }
                }
        }

        private static String claimOrHeaderJsonText(String claimName, String claimValue) {
            return QUOTE + claimName + QUOTE + ":" + QUOTE + claimValue + QUOTE;
        }

        private static String claimOrHeaderJsonText(String claimName, Number claimValue) {
            return QUOTE + claimName + QUOTE + ":" + claimValue;
        }

        private static String expClaimText(long lifetimeSeconds) {
            return claimOrHeaderJsonText("exp", time.milliseconds() / 1000.0 + lifetimeSeconds);
        }

        @Override
        public void configure(Map<String, ?> configs, String saslMechanism,
                List<AppConfigurationEntry> jaasConfigEntries) {
            DELEGATE.configure(configs, saslMechanism, jaasConfigEntries);
        }

        @Override
        public void close() {
            DELEGATE.close();
        }
    }

    /*
     * Define a channel builder that starts with the DIGEST-MD5 mechanism and then
     * switches to the PLAIN mechanism
     */
    private static class AlternateSaslChannelBuilder extends SaslChannelBuilder {
        private int numInvocations = 0;

        public AlternateSaslChannelBuilder(Mode mode, Map<String, JaasContext> jaasContexts,
                SecurityProtocol securityProtocol, ListenerName listenerName, boolean isInterBrokerListener,
                String clientSaslMechanism, boolean handshakeRequestEnable, CredentialCache credentialCache,
                DelegationTokenCache tokenCache, Time time) {
            super(mode, jaasContexts, securityProtocol, listenerName, isInterBrokerListener, clientSaslMechanism,
                    handshakeRequestEnable, credentialCache, tokenCache, time, new LogContext());
        }

        @Override
        protected SaslClientAuthenticator buildClientAuthenticator(Map<String, ?> configs,
                AuthenticateCallbackHandler callbackHandler, String id, String serverHost, String servicePrincipal,
                TransportLayer transportLayer, Subject subject) {
            if (++numInvocations == 1)
                return new SaslClientAuthenticator(configs, callbackHandler, id, subject, servicePrincipal, serverHost,
                        "DIGEST-MD5", true, transportLayer, time, new LogContext());
            else
                return new SaslClientAuthenticator(configs, callbackHandler, id, subject, servicePrincipal, serverHost,
                        "PLAIN", true, transportLayer, time, new LogContext()) {
                    @Override
                    protected SaslHandshakeRequest createSaslHandshakeRequest(short version) {
                        return new SaslHandshakeRequest.Builder(
                                new SaslHandshakeRequestData().setMechanism("PLAIN")).build(version);
                    }
                };
        }
    }
}
