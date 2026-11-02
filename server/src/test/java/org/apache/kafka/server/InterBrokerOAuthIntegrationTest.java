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
package org.apache.kafka.server;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.authenticator.LoginManager;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.common.test.api.TestKitDefaults;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.security.auth.login.Configuration;

import no.nav.security.mock.oauth2.MockOAuth2Server;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration test for brokers that use OAUTHBEARER as the inter-broker SASL mechanism, configured as described in
 * the "Configuring Production Kafka Brokers" section of the security documentation.
 */
@Tag("integration")
public class InterBrokerOAuthIntegrationTest {

    private static final String MECHANISM = "OAUTHBEARER";
    private static final String ISSUER_ID = "default";
    private static final int NUM_BROKERS = 3;

    private MockOAuth2Server oauthServer;
    private String jwksUrl;
    private String tokenEndpointUrl;

    @BeforeEach
    public void setUp() throws Exception {
        oauthServer = new MockOAuth2Server();
        oauthServer.start();
        jwksUrl = oauthServer.jwksUrl(ISSUER_ID).toString();
        tokenEndpointUrl = oauthServer.tokenEndpointUrl(ISSUER_ID).toString();

        // A static JAAS file without any login module options, and the allowed OAuth URLs
        File jaasFile = TestUtils.tempFile("KafkaServer {\n    " + OAuthBearerLoginModule.class.getName() + " required ;\n};\n");
        System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, jaasFile.getAbsolutePath());
        Configuration.setConfiguration(null);
        System.setProperty(BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, jwksUrl + "," + tokenEndpointUrl);
    }

    @AfterEach
    public void tearDown() {
        if (oauthServer != null)
            oauthServer.shutdown();
        LoginManager.closeAll();
        System.clearProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM);
        System.clearProperty(BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG);
        Configuration.setConfiguration(null);
    }

    @Test
    @Timeout(180)
    public void testReplicationWithBrokerLoginCallbackHandler() throws Exception {
        TestKitNodes nodes = new TestKitNodes.Builder()
            .setNumBrokerNodes(NUM_BROKERS)
            .setNumControllerNodes(1)
            .setBrokerSecurityProtocol(SecurityProtocol.SASL_PLAINTEXT)
            .build();
        String prefix = ListenerName.normalised(TestKitDefaults.DEFAULT_BROKER_LISTENER_NAME).saslMechanismConfigPrefix(MECHANISM);

        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(nodes)
                .setConfigProp(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG, MECHANISM)
                .setConfigProp(BrokerSecurityConfigs.SASL_MECHANISM_INTER_BROKER_PROTOCOL_CONFIG, MECHANISM)
                // Validate the tokens of incoming connections
                .setConfigProp(prefix + BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS_CONFIG, OAuthBearerValidatorCallbackHandler.class.getName())
                .setConfigProp(prefix + SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL, jwksUrl)
                .setConfigProp(prefix + SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE, ISSUER_ID)
                .setConfigProp(prefix + SaslConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER, oauthServer.issuerUrl(ISSUER_ID).toString())
                // Retrieve a token for outgoing inter-broker connections
                .setConfigProp(prefix + SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, OAuthBearerLoginCallbackHandler.class.getName())
                .setConfigProp(prefix + SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, tokenEndpointUrl)
                .setConfigProp(prefix + SaslConfigs.SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID, "kafka-broker")
                .setConfigProp(prefix + SaslConfigs.SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_SECRET, "broker-secret")
                .build()) {
            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();

            // min.insync.replicas equals the replication factor, so a send succeeds only if
            // the followers authenticate to the leader and replicate the record
            String topic = "inter-broker-replication-test";
            NewTopic newTopic = new NewTopic(topic, 1, (short) NUM_BROKERS)
                .configs(Map.of(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, String.valueOf(NUM_BROKERS)));
            try (Admin admin = cluster.admin(clientConfigs(cluster))) {
                admin.createTopics(List.of(newTopic)).all().get();
            }

            int numRecords = 10;
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(clientConfigs(cluster), new StringSerializer(), new StringSerializer())) {
                for (int i = 0; i < numRecords; i++) {
                    producer.send(new ProducerRecord<>(topic, "key-" + i, "value-" + i)).get();
                }
            }

            TopicPartition tp = new TopicPartition(topic, 0);
            List<ConsumerRecord<String, String>> records = new ArrayList<>();
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(clientConfigs(cluster), new StringDeserializer(), new StringDeserializer())) {
                consumer.assign(List.of(tp));
                consumer.seekToBeginning(List.of(tp));
                TestUtils.waitForCondition(() -> {
                    consumer.poll(Duration.ofMillis(100)).forEach(records::add);
                    return records.size() >= numRecords;
                }, "Should consume " + numRecords + " records");
            }
            assertEquals(numRecords, records.size());
            for (int i = 0; i < numRecords; i++) {
                assertEquals("key-" + i, records.get(i).key());
                assertEquals("value-" + i, records.get(i).value());
            }
        }
    }

    private Map<String, Object> clientConfigs(KafkaClusterTestKit cluster) {
        return Map.of(
            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers(),
            CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name,
            SaslConfigs.SASL_MECHANISM, MECHANISM,
            SaslConfigs.SASL_JAAS_CONFIG, OAuthBearerLoginModule.class.getName() + " required ;",
            SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, OAuthBearerLoginCallbackHandler.class.getName(),
            SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, tokenEndpointUrl,
            SaslConfigs.SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID, "test-client",
            SaslConfigs.SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_SECRET, "test-secret");
    }
}
