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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.oauthbearer.JwtBearerJwtRetriever;
import org.apache.kafka.common.security.oauthbearer.JwtRetriever;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfig;
import org.apache.kafka.common.test.api.ClusterTemplate;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.test.TestUtils;

import com.nimbusds.jose.jwk.RSAKey;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import no.nav.security.mock.oauth2.MockOAuth2Server;
import no.nav.security.mock.oauth2.OAuth2Config;
import no.nav.security.mock.oauth2.token.KeyProvider;
import no.nav.security.mock.oauth2.token.OAuth2TokenProvider;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ClusterTestDefaults(
    types = {Type.KRAFT}, 
    brokers = 3
)
public class ClientOAuthIntegrationTest {

    private static final String ISSUER_ID = "default";

    private static MockOAuth2Server mockOAuthServer;
    private static PrivateKey privateKey;
    private static String tokenEndpointUrl;
    private static String jwksUrl;

    @BeforeAll
    public static void startOAuthServer() throws Exception {
        // Step 1: Generate the key pair dynamically.
        var keyGen = KeyPairGenerator.getInstance("RSA");
        keyGen.initialize(2048);
        var keyPair = keyGen.generateKeyPair();

        privateKey = keyPair.getPrivate();

        // Step 2: Create the RSA JWK from key pair.
        var rsaJWK = new RSAKey.Builder((RSAPublicKey) keyPair.getPublic())
            .privateKey(privateKey)
            .keyID("foo")
            .build();

        // Step 3: Create the OAuth server using the keys just created
        var keyProvider = new KeyProvider(List.of(rsaJWK));
        var tokenProvider = new OAuth2TokenProvider(keyProvider);
        var oauthConfig = new OAuth2Config(false, null, null, false, tokenProvider);
        mockOAuthServer = new MockOAuth2Server(oauthConfig);
        mockOAuthServer.start();

        tokenEndpointUrl = mockOAuthServer.tokenEndpointUrl(ISSUER_ID).url().toString();
        jwksUrl = mockOAuthServer.jwksUrl(ISSUER_ID).url().toString();

        System.setProperty(BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, tokenEndpointUrl + "," + jwksUrl);
    }

    @AfterAll
    public static void stopOAuthServer() {
        if (mockOAuthServer != null) {
            mockOAuthServer.shutdown();
        }
        System.clearProperty(BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG);
        System.clearProperty(BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG);
    }

    @AfterEach
    public void clearFileProperty() {
        System.clearProperty(BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG);
    }

    private static List<ClusterConfig> clusterConfigs() {
        var serverProperties = new HashMap<String, String>();
        serverProperties.put("sasl.enabled.mechanisms", "PLAIN,OAUTHBEARER");
        serverProperties.put("sasl.mechanism.inter.broker.protocol", "PLAIN");
        serverProperties.put(GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, "1");
        serverProperties.put(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "1");
        serverProperties.put(
            "super.users",
            "User:plain-admin;User:test-client;User:jdoe;User:kafka-client-test-sub;User:kafka-e2e-test;User:kafka-admin-test"
        );

        var listenerPrefix = "listener.name.external.oauthbearer.";
        serverProperties.put(listenerPrefix + SaslConfigs.SASL_JAAS_CONFIG, OAuthBearerLoginModule.class.getName() + " required ;");
        serverProperties.put(listenerPrefix + SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE, ISSUER_ID);
        serverProperties.put(listenerPrefix + SaslConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER, mockOAuthServer.issuerUrl(ISSUER_ID).toString());
        serverProperties.put(listenerPrefix + SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL, jwksUrl);
        serverProperties.put(
            listenerPrefix + BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS_CONFIG,
            OAuthBearerValidatorCallbackHandler.class.getName()
        );

        return List.of(ClusterConfig.defaultBuilder()
            .setTypes(Set.of(Type.KRAFT))
            .setBrokers(3)
            .setBrokerSecurityProtocol(SecurityProtocol.SASL_PLAINTEXT)
            .setServerProperties(serverProperties)
            .build());
    }

    private Map<String, Object> defaultOAuthConfigs() {
        var configs = new HashMap<String, Object>();
        configs.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        configs.put(SaslConfigs.SASL_MECHANISM, "OAUTHBEARER");
        configs.put(SaslConfigs.SASL_JAAS_CONFIG, OAuthBearerLoginModule.class.getName() + " required ;");
        configs.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, OAuthBearerLoginCallbackHandler.class.getName());
        configs.put(SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, tokenEndpointUrl);
        return configs;
    }

    private Map<String, Object> defaultClientCredentialsConfigs() {
        var configs = defaultOAuthConfigs();
        configs.put(SaslConfigs.SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID, "test-client");
        configs.put(SaslConfigs.SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_SECRET, "test-secret");
        return configs;
    }

    private Map<String, Object> defaultJwtBearerConfigs() {
        var configs = defaultOAuthConfigs();
        configs.put(SaslConfigs.SASL_OAUTHBEARER_JWT_RETRIEVER_CLASS, JwtBearerJwtRetriever.class.getName());
        return configs;
    }

    private Map<String, Object> consumerConfigs(Map<String, Object> configs, String groupProtocol) {
        var result = new HashMap<>(configs);
        result.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol);
        return result;
    }

    private File generatePrivateKeyFile() throws Exception {
        var file = File.createTempFile("private-", ".key");
        file.deleteOnExit();
        var bytes = Base64.getEncoder().encode(privateKey.getEncoded());
        try (var channel = FileChannel.open(file.toPath(), EnumSet.of(StandardOpenOption.WRITE))) {
            Utils.writeFully(channel, ByteBuffer.wrap(bytes));
        }
        return file;
    }

    private void consumeAndVerify(ClusterInstance cluster, Map<String, Object> configs, String topic, String expectedValue) throws Exception {
        try (Consumer<byte[], byte[]> consumer = cluster.consumer(configs)) {
            consumer.subscribe(List.of(topic));
            var recordsRef = new AtomicReference<>(ConsumerRecords.<byte[], byte[]>empty());
            TestUtils.waitForCondition(
                () -> {
                    var polled = consumer.poll(Duration.ofMillis(500));
                    if (!polled.isEmpty()) {
                        recordsRef.set(polled);
                        return true;
                    }
                    return false;
                },
                30000,
                "Failed to consume expected records from topic " + topic);
            assertEquals(1, recordsRef.get().count());
            if (expectedValue != null) {
                assertEquals(expectedValue, new String(recordsRef.get().iterator().next().value()));
            }
        }
    }

    @ClusterTemplate("clusterConfigs")
    public void testBasicClientCredentials(ClusterInstance cluster) {
        var configs = defaultClientCredentialsConfigs();
        assertDoesNotThrow(() -> cluster.producer(configs).close());
        assertDoesNotThrow(() -> cluster.consumer(configs).close());
        assertDoesNotThrow(() -> cluster.admin(configs).close());
    }

    @ClusterTemplate("clusterConfigs")
    public void testBasicJwtBearer(ClusterInstance cluster) throws Exception {
        var jwt = mockOAuthServer.issueToken(ISSUER_ID, "jdoe", "someaudience",
            Collections.singletonMap("scope", "test")).serialize();
        var assertionFile = TestUtils.tempFile(jwt);
        System.setProperty(BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG, assertionFile.getAbsolutePath());

        var configs = defaultJwtBearerConfigs();
        configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_FILE, assertionFile.getAbsolutePath());

        assertDoesNotThrow(() -> cluster.producer(configs).close());
        assertDoesNotThrow(() -> cluster.consumer(configs).close());
        assertDoesNotThrow(() -> cluster.admin(configs).close());
    }

    @ClusterTemplate("clusterConfigs")
    public void testBasicJwtBearerWithPrivateKey(ClusterInstance cluster) throws Exception {
        var privateKeyFile = generatePrivateKeyFile();
        System.setProperty(BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG, privateKeyFile.getAbsolutePath());

        var configs = defaultJwtBearerConfigs();
        configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE, privateKeyFile.getPath());
        configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_CLAIM_AUD, "default");
        configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_CLAIM_SUB, "kafka-client-test-sub");
        configs.put(SaslConfigs.SASL_OAUTHBEARER_SCOPE, "default");

        assertDoesNotThrow(() -> cluster.producer(configs).close());
        assertDoesNotThrow(() -> cluster.consumer(configs).close());
        assertDoesNotThrow(() -> cluster.admin(configs).close());
    }

    @Disabled("KAFKA-19394: Failure in ConsumerNetworkThread.initializeResources() can cause hangs on AsyncKafkaConsumer.close()")
    @ClusterTemplate("clusterConfigs")
    public void testJwtBearerWithMalformedAssertionFile(ClusterInstance cluster) throws Exception {
        var assertionFile = TestUtils.tempFile("CQEN*)Q#F)&)^#QNC");
        System.setProperty(BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG, assertionFile.getAbsolutePath());

        var configs = defaultJwtBearerConfigs();
        configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_FILE, assertionFile.getAbsolutePath());

        assertThrows(KafkaException.class, () -> cluster.producer(configs).close());
        assertThrows(KafkaException.class, () -> cluster.consumer(configs).close());
        assertThrows(KafkaException.class, () -> cluster.admin(configs).close());
    }

    @Disabled("KAFKA-19394: Failure in ConsumerNetworkThread.initializeResources() can cause hangs on AsyncKafkaConsumer.close()")
    @ClusterTemplate("clusterConfigs")
    public void testJwtBearerWithEmptyAssertionFile(ClusterInstance cluster) throws Exception {
        var assertionFile = TestUtils.tempFile();
        System.setProperty(BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG, assertionFile.getAbsolutePath());

        var configs = defaultJwtBearerConfigs();
        configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_FILE, assertionFile.getAbsolutePath());

        assertThrows(KafkaException.class, () -> cluster.producer(configs).close());
        assertThrows(KafkaException.class, () -> cluster.consumer(configs).close());
        assertThrows(KafkaException.class, () -> cluster.admin(configs).close());
    }

    @Disabled("KAFKA-19394: Failure in ConsumerNetworkThread.initializeResources() can cause hangs on AsyncKafkaConsumer.close()")
    @ClusterTemplate("clusterConfigs")
    public void testJwtBearerWithMissingAssertionFile(ClusterInstance cluster) {
        var configs = defaultJwtBearerConfigs();
        configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_FILE, "/this/does/not/exist.txt");

        assertThrows(KafkaException.class, () -> cluster.producer(configs).close());
        assertThrows(KafkaException.class, () -> cluster.consumer(configs).close());
        assertThrows(KafkaException.class, () -> cluster.admin(configs).close());
    }

    @ClusterTemplate("clusterConfigs")
    public void testUnsupportedJwtRetriever(ClusterInstance cluster) {
        var configs = defaultOAuthConfigs();
        configs.put(SaslConfigs.SASL_OAUTHBEARER_JWT_RETRIEVER_CLASS,
            "org.apache.kafka.common.security.oauthbearer.ThisIsNotARealJwtRetriever");

        assertThrows(ConfigException.class, () -> cluster.producer(configs).close());
        assertThrows(ConfigException.class, () -> cluster.consumer(configs).close());
        assertThrows(ConfigException.class, () -> cluster.admin(configs).close());
    }

    @ClusterTemplate("clusterConfigs")
    public void testAuthenticationErrorOnTamperedJwt(ClusterInstance cluster) {
        var configs = defaultOAuthConfigs();
        configs.put(SaslConfigs.SASL_OAUTHBEARER_JWT_RETRIEVER_CLASS, TamperedJwtRetriever.class.getName());

        var tp = new TopicPartition("test-topic", 0);
        try (var admin = cluster.admin(configs)) {
            TestUtils.assertFutureThrows(SaslAuthenticationException.class, admin.describeCluster().clusterId());
        }
        try (Producer<byte[], byte[]> producer = cluster.producer(configs)) {
            assertThrows(SaslAuthenticationException.class, () -> producer.partitionsFor(tp.topic()));
        }

        try (Consumer<byte[], byte[]> consumer = cluster.consumer(configs)) {
            consumer.assign(List.of(tp));
            assertThrows(SaslAuthenticationException.class, () -> consumer.position(tp));
        }
    }

    @ClusterTemplate("clusterConfigs")
    public void testClientAssertionAdminOperations(ClusterInstance cluster) throws Exception {
        var privateKeyFile = generatePrivateKeyFile();
        System.setProperty(BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG, privateKeyFile.getAbsolutePath());

        var configs = defaultOAuthConfigs();
        configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE, privateKeyFile.getPath());
        configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_CLAIM_ISS, "kafka-admin-test");
        configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_CLAIM_AUD, "default");
        configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_CLAIM_SUB, "kafka-admin-test");
        configs.put(SaslConfigs.SASL_OAUTHBEARER_SCOPE, "default");

        try (var admin = cluster.admin(configs)) {
            var clusterId = admin.describeCluster().clusterId().get();
            assertNotNull(clusterId);

            var topic = "admin-assertion-test";
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
            TestUtils.waitForCondition(
                () -> admin.describeTopics(List.of(topic)).allTopicNames().get().containsKey(topic),
                "Topic metadata not available");

            assertNotNull(admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic));
        }
    }

    @ClusterTemplate("clusterConfigs")
    public void testClientAssertionProduceConsumeClassic(ClusterInstance cluster) throws Exception {
        verifyClientAssertionProduceConsume(cluster, "classic", "client-assertion-test-classic");
    }

    @ClusterTemplate("clusterConfigs")
    public void testClientAssertionProduceConsumeConsumer(ClusterInstance cluster) throws Exception {
        verifyClientAssertionProduceConsume(cluster, "consumer", "client-assertion-test-consumer");
    }

    private void verifyClientAssertionProduceConsume(ClusterInstance cluster, String groupProtocol, String topic) throws Exception {
        var privateKeyFile = generatePrivateKeyFile();
        System.setProperty(BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG, privateKeyFile.getAbsolutePath());
        var configs = defaultOAuthConfigs();
        configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE, privateKeyFile.getPath());
        configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_CLAIM_ISS, "kafka-e2e-test");
        configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_CLAIM_AUD, "default");
        configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_CLAIM_SUB, "kafka-e2e-test");
        configs.put(SaslConfigs.SASL_OAUTHBEARER_SCOPE, "default");

        try (var admin = cluster.admin(configs)) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
        }
        try (Producer<byte[], byte[]> producer = cluster.producer(configs)) {
            producer.send(new ProducerRecord<>(topic, "key".getBytes(), "value".getBytes())).get();
        }

        var consumerCfg = consumerConfigs(configs, groupProtocol);
        consumeAndVerify(cluster, consumerCfg, topic, "value");
    }

    @ClusterTemplate("clusterConfigs")
    public void testClientAssertionFileBasedProduceConsumeClassic(ClusterInstance cluster) throws Exception {
        verifyClientAssertionFileBasedProduceConsume(cluster, "classic", "file-assertion-test-classic");
    }

    @ClusterTemplate("clusterConfigs")
    public void testClientAssertionFileBasedProduceConsumeConsumer(ClusterInstance cluster) throws Exception {
        verifyClientAssertionFileBasedProduceConsume(cluster, "consumer", "file-assertion-test-consumer");
    }

    private void verifyClientAssertionFileBasedProduceConsume(ClusterInstance cluster, String groupProtocol, String topic) throws Exception {
        var jwt = mockOAuthServer.issueToken(ISSUER_ID, "jdoe", "someaudience", Map.of("scope", "test")).serialize();
        var assertionFile = TestUtils.tempFile(jwt);
        System.setProperty(BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG, assertionFile.getAbsolutePath());
        var configs = defaultOAuthConfigs();
        configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_FILE, assertionFile.getAbsolutePath());

        try (var admin = cluster.admin(configs)) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
        }
        try (Producer<byte[], byte[]> producer = cluster.producer(configs)) {
            producer.send(new ProducerRecord<>(topic, "key".getBytes(), "value".getBytes())).get();
        }
        var consumerCfg = consumerConfigs(configs, groupProtocol);
        consumeAndVerify(cluster, consumerCfg, topic, "value");
    }

    public static class TamperedJwtRetriever implements JwtRetriever {
        @Override
        public String retrieve() {
            return "eyJhbGciOiAiSFMyNTYiLCAidHlwIjogIkpXVCJ9."
                + "eyJzdWIiOiAiMTIzNDU2Nzg5MCIsICJuYW1lIjogIkpvaG4gRG9lIiwgInJvbGUiOiAiYWRtaW4iLCAiaWF0IjogMTUxNjIzOTAyMiwgImV4cCI6IDE5MTYyMzkwMjJ9."
                + "vVT5ylQCGvb0B-wv1YXHjmlMd-DZKCThUt5-enry_sA";
        }
    }
}
