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

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.authenticator.LoginManager;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.apache.kafka.security.JaasTestUtils;
import org.apache.kafka.security.minikdc.MiniKdc;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import javax.security.auth.login.Configuration;

import static org.apache.kafka.clients.ClientsTestUtils.BaseConsumerTestcase.BROKER_COUNT;
import static org.apache.kafka.clients.ClientsTestUtils.BaseConsumerTestcase.TOPIC;
import static org.apache.kafka.clients.ClientsTestUtils.BaseConsumerTestcase.TP;
import static org.apache.kafka.clients.ClientsTestUtils.consumeAndVerifyRecords;
import static org.apache.kafka.clients.ClientsTestUtils.sendAndAwaitAsyncCommit;
import static org.apache.kafka.clients.ClientsTestUtils.sendRecords;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.SaslMultiMechanismConsumerTest.MECHANISMS;
import static org.apache.kafka.common.config.internals.BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG;
import static org.apache.kafka.common.config.internals.BrokerSecurityConfigs.SASL_MECHANISM_INTER_BROKER_PROTOCOL_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG;
import static org.apache.kafka.security.JaasTestUtils.KAFKA_CLIENT_PRINCIPAL_UNQUALIFIED_NAME;
import static org.apache.kafka.security.JaasTestUtils.KAFKA_CLIENT_PRINCIPAL_UNQUALIFIED_NAME_2;
import static org.apache.kafka.security.JaasTestUtils.KAFKA_SERVER_PRINCIPAL_UNQUALIFIED_NAME;

@Timeout(600)
@ClusterTestDefaults(
    types = {Type.KRAFT},
    brokers = BROKER_COUNT,
    serverProperties = {
        @ClusterConfigProperty(key = OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
        @ClusterConfigProperty(key = GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, value = "100"),
        @ClusterConfigProperty(key = SASL_ENABLED_MECHANISMS_CONFIG, value = MECHANISMS),
        @ClusterConfigProperty(key = SASL_MECHANISM_INTER_BROKER_PROTOCOL_CONFIG, value = "PLAIN"),
        @ClusterConfigProperty(key = StandardAuthorizer.ALLOW_EVERYONE_IF_NO_ACL_IS_FOUND_CONFIG, value = "true"),
    }
)
public class SaslMultiMechanismConsumerTest {

    private final ClusterInstance cluster;
    public static final String MECHANISMS = "GSSAPI,PLAIN";
    private static Properties kdcConf = MiniKdc.createConfig();
    private static MiniKdc kdc;
    private static Optional<File> clientKeytabFile;

    public SaslMultiMechanismConsumerTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    @BeforeAll
    public static void setup() throws Exception {
        // Important if tests leak consumers, producers or brokers
        LoginManager.closeAll();
        File workingDir = TestUtils.tempDirectory();
        Optional<File> serverKeytabFile = Optional.of(TestUtils.tempFile());
        clientKeytabFile = Optional.of(TestUtils.tempFile());
        List<JaasTestUtils.JaasSection> jaasSections = List.of(
            JaasTestUtils.kafkaServerSection(
                JaasTestUtils.KAFKA_SERVER_CONTEXT_NAME,
                List.of("GSSAPI", "PLAIN"),
                serverKeytabFile
            ), JaasTestUtils.kafkaClientSection(
                Optional.of("GSSAPI"),
                clientKeytabFile
            )
        );
        // init MiniKdc
        kdc = new MiniKdc(kdcConf, workingDir);
        kdc.start();
        kdc.createPrincipal(
            serverKeytabFile.get(),
            List.of(KAFKA_SERVER_PRINCIPAL_UNQUALIFIED_NAME + "/localhost")
        );
        kdc.createPrincipal(
            clientKeytabFile.get(),
            List.of(KAFKA_CLIENT_PRINCIPAL_UNQUALIFIED_NAME, KAFKA_CLIENT_PRINCIPAL_UNQUALIFIED_NAME_2)
        );
        // writeJaasConfigurationToFile
        File file = JaasTestUtils.writeJaasContextsToFile(jaasSections);
        System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, file.getAbsolutePath());
        // This will cause a reload of the Configuration singleton when `getConfiguration` is called
        Configuration.setConfiguration(null);
    }

    @BeforeEach
    public void beforeEach() throws InterruptedException {
        cluster.createTopic(TOPIC, 2, (short) BROKER_COUNT);
    }

    @AfterAll
    public static void teardown() {
        if (kdc != null)
            kdc.stop();
        // Important if tests leak consumers, producers or brokers
        LoginManager.closeAll();
        System.clearProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM);
        Configuration.setConfiguration(null);
    }

    @ClusterTest(
        brokerSecurityProtocol = SecurityProtocol.SASL_SSL
    )
    public void testClassicConsumerMultipleBrokerMechanisms() throws InterruptedException {
        testMultipleBrokerMechanisms(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT))
        );
    }

    @ClusterTest(
        brokerSecurityProtocol = SecurityProtocol.SASL_SSL
    )
    public void testAsyncConsumerMultipleBrokerMechanisms() throws InterruptedException {
        testMultipleBrokerMechanisms(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT))
        );
    }

    private void testMultipleBrokerMechanisms(Map<String, Object> consumerConfig) throws InterruptedException {
        Map<String, Object> gssapiConfig = new HashMap<>(consumerConfig);
        gssapiConfig.put(SaslConfigs.SASL_MECHANISM, "GSSAPI");
        gssapiConfig.put(SaslConfigs.SASL_JAAS_CONFIG, JaasTestUtils.clientLoginModule("GSSAPI", clientKeytabFile));

        Map<String, Object> gssapiProducerConfig = Map.of(
            SaslConfigs.SASL_MECHANISM, "GSSAPI",
            SaslConfigs.SASL_JAAS_CONFIG, JaasTestUtils.clientLoginModule("GSSAPI", clientKeytabFile)
        );

        try (Producer<byte[], byte[]> plainSaslProducer = cluster.producer();
             Consumer<byte[], byte[]> plainSaslConsumer = cluster.consumer(consumerConfig);
             Producer<byte[], byte[]> gssapiSaslProducer = cluster.producer(gssapiProducerConfig);
             Consumer<byte[], byte[]> gssapiSaslConsumer = cluster.consumer(gssapiConfig)
        ) {
            var numRecords = 1000;
            var startingOffset = 0;

            // Test SASL/PLAIN producer and consumer
            var startingTimestamp = System.currentTimeMillis();
            sendRecords(plainSaslProducer, TP, numRecords, startingTimestamp);
            plainSaslConsumer.assign(List.of(TP));
            plainSaslConsumer.seek(TP, 0);
            consumeAndVerifyRecords(plainSaslConsumer, TP, numRecords, startingOffset, 0, startingTimestamp);
            sendAndAwaitAsyncCommit(plainSaslConsumer, Optional.empty());
            startingOffset += numRecords;

            // Test SASL/GSSAPI producer and consumer
            startingTimestamp = System.currentTimeMillis();
            sendRecords(gssapiSaslProducer, TP, numRecords, startingTimestamp);
            gssapiSaslConsumer.assign(List.of(TP));
            gssapiSaslConsumer.seek(TP, startingOffset);
            consumeAndVerifyRecords(gssapiSaslConsumer, TP, numRecords, startingOffset, 0, startingTimestamp);
            sendAndAwaitAsyncCommit(gssapiSaslConsumer, Optional.empty());
            startingOffset += numRecords;

            // Test SASL/PLAIN producer and SASL/GSSAPI consumer
            startingTimestamp = System.currentTimeMillis();
            sendRecords(plainSaslProducer, TP, numRecords, startingTimestamp);
            gssapiSaslConsumer.assign(List.of(TP));
            gssapiSaslConsumer.seek(TP, startingOffset);
            consumeAndVerifyRecords(gssapiSaslConsumer, TP, numRecords, startingOffset, 0, startingTimestamp);
            startingOffset += numRecords;

            // Test SASL/GSSAPI producer and SASL/PLAIN consumer
            startingTimestamp = System.currentTimeMillis();
            sendRecords(gssapiSaslProducer, TP, numRecords, startingTimestamp);
            plainSaslConsumer.assign(List.of(TP));
            plainSaslConsumer.seek(TP, startingOffset);
            consumeAndVerifyRecords(plainSaslConsumer, TP, numRecords, startingOffset, 0, startingTimestamp);
        }
    }
}
