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

import kafka.security.JaasTestUtils;
import kafka.security.minikdc.MiniKdc;

import org.apache.kafka.clients.ClientsTestUtils;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.authenticator.LoginManager;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.TestUtils;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import javax.security.auth.login.Configuration;

import static kafka.security.JaasTestUtils.KAFKA_CLIENT_PRINCIPAL_UNQUALIFIED_NAME;
import static kafka.security.JaasTestUtils.KAFKA_CLIENT_PRINCIPAL_UNQUALIFIED_NAME_2;
import static kafka.security.JaasTestUtils.KAFKA_SERVER_PRINCIPAL_UNQUALIFIED_NAME;
import static org.apache.kafka.clients.ClientsTestUtils.BaseConsumerTestcase.testClusterResourceListener;
import static org.apache.kafka.clients.ClientsTestUtils.BaseConsumerTestcase.testCoordinatorFailover;
import static org.apache.kafka.clients.ClientsTestUtils.BaseConsumerTestcase.testSimpleConsumption;
import static org.apache.kafka.clients.CommonClientConfigs.MAX_POLL_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.SaslPlaintextConsumerTest.MECHANISMS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM;
import static org.apache.kafka.common.config.internals.BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG;
import static org.apache.kafka.common.config.internals.BrokerSecurityConfigs.SASL_MECHANISM_INTER_BROKER_PROTOCOL_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG;

@ClusterTestDefaults(
    types = {Type.KRAFT},
    brokers = ClientsTestUtils.BaseConsumerTestcase.BROKER_COUNT,
    serverProperties = {
        @ClusterConfigProperty(key = OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
        @ClusterConfigProperty(key = GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, value = "100"),
        @ClusterConfigProperty(key = SASL_ENABLED_MECHANISMS_CONFIG, value = MECHANISMS),
        @ClusterConfigProperty(key = SASL_MECHANISM_INTER_BROKER_PROTOCOL_CONFIG, value = MECHANISMS),
    }
)
public class SaslPlaintextConsumerTest {

    private final ClusterInstance cluster;
    public static final String MECHANISMS = "GSSAPI";
    private static Optional<File> serverKeytabFile = Optional.empty();
    private static Optional<File> clientKeytabFile = Optional.empty();
    private static File workingDir;
    private static Properties kdcConf = MiniKdc.createConfig();
    private static MiniKdc kdc;
    
    public SaslPlaintextConsumerTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    @BeforeAll
    public static void setup() throws Exception {
        // Important if tests leak consumers, producers or brokers
        LoginManager.closeAll();
        workingDir = TestUtils.tempDirectory();
        if (serverKeytabFile.isEmpty()) {
            serverKeytabFile = Optional.of(TestUtils.tempFile());
        }
        if (clientKeytabFile.isEmpty()) {
            clientKeytabFile = Optional.of(TestUtils.tempFile());
        }
        List<JaasTestUtils.JaasSection> jaasSections = List.of(
            JaasTestUtils.kafkaServerSection(
                JaasTestUtils.KAFKA_SERVER_CONTEXT_NAME,
                List.of(MECHANISMS),
                serverKeytabFile
            ), JaasTestUtils.kafkaClientSection(
                Optional.of(MECHANISMS),
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
        cluster.createTopic(ClientsTestUtils.BaseConsumerTestcase.TOPIC, 2, (short) ClientsTestUtils.BaseConsumerTestcase.BROKER_COUNT);
    }
    
    @AfterEach
    public void teardown() {
        if (kdc != null)
            kdc.stop();
        // Important if tests leak consumers, producers or brokers
        LoginManager.closeAll();
        System.clearProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM);
        Configuration.setConfiguration(null);
    }

    @ClusterTest(
        brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT
    )
    public void testClassicConsumerSimpleConsumption() throws InterruptedException {
        testSimpleConsumption(cluster, Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT))
        );
    }

    @ClusterTest(
            brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT
    )
    public void testAsyncConsumerSimpleConsumption() throws InterruptedException {
        testSimpleConsumption(cluster, Map.of(
                SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name,
                SASL_MECHANISM, MECHANISMS,
//                SASL_JAAS_CONFIG, jaasClientLoginModule(clientSaslMechanism),
                GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT))
        );
    }

    @ClusterTest(
            brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT
    )
    public void testClassicConsumerClusterResourceListener() throws InterruptedException {
        testClusterResourceListener(cluster, Map.of(
            SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name,
            SASL_MECHANISM, MECHANISMS,
//            SASL_JAAS_CONFIG, jaasClientLoginModule(clientSaslMechanism),
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT))
        );
    }

    @ClusterTest(
            brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT
    )
    public void testAsyncConsumerClusterResourceListener() throws InterruptedException {
        testClusterResourceListener(cluster, Map.of(
            SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name,
            SASL_MECHANISM, MECHANISMS,
//            SASL_JAAS_CONFIG, jaasClientLoginModule(clientSaslMechanism),
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT))
        );
    }

    @ClusterTest(
            brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT
    )
    public void testClassicConsumerCoordinatorFailover() throws InterruptedException {
        Map<String, Object> config = Map.of(
            SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name,
            SASL_MECHANISM, MECHANISMS,
//            SASL_JAAS_CONFIG, jaasClientLoginModule(clientSaslMechanism),
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT),
            SESSION_TIMEOUT_MS_CONFIG, 5001,
            HEARTBEAT_INTERVAL_MS_CONFIG, 1000,
            // Use higher poll timeout to avoid consumer leaving the group due to timeout
            MAX_POLL_INTERVAL_MS_CONFIG, 15000
        );
        testCoordinatorFailover(cluster, config);
    }

    @ClusterTest(
            brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT
    )
    public void testAsyncConsumeCoordinatorFailover() throws InterruptedException {
        Map<String, Object> config = Map.of(
            SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name,
            SASL_MECHANISM, MECHANISMS,
//            SASL_JAAS_CONFIG, jaasClientLoginModule(clientSaslMechanism),
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT),
            // Use higher poll timeout to avoid consumer leaving the group due to timeout
            MAX_POLL_INTERVAL_MS_CONFIG, 15000
        );
        testCoordinatorFailover(cluster, config);
    }
}
