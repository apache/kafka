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

import org.apache.kafka.clients.ClientsTestUtils;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;

import org.junit.jupiter.api.BeforeEach;

import java.util.Locale;
import java.util.Map;

import static org.apache.kafka.clients.ClientsTestUtils.BaseConsumerTestcase.testClusterResourceListener;
import static org.apache.kafka.clients.ClientsTestUtils.BaseConsumerTestcase.testCoordinatorFailover;
import static org.apache.kafka.clients.ClientsTestUtils.BaseConsumerTestcase.testSimpleConsumption;
import static org.apache.kafka.clients.CommonClientConfigs.MAX_POLL_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.SaslPlainPlaintextConsumerTest.MECHANISMS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM;
import static org.apache.kafka.common.config.internals.BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG;
import static org.apache.kafka.common.config.internals.BrokerSecurityConfigs.SASL_MECHANISM_INTER_BROKER_PROTOCOL_CONFIG;
import static org.apache.kafka.common.test.JaasUtils.KAFKA_PLAIN_ADMIN;
import static org.apache.kafka.common.test.JaasUtils.KAFKA_PLAIN_ADMIN_PASSWORD;
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
public class SaslPlainPlaintextConsumerTest {

    private final ClusterInstance cluster;
    public static final String MECHANISMS = "PLAIN";
    public static final String SASL_JAAS = "org.apache.kafka.common.security.plain.PlainLoginModule required "
        + "username=\"" + KAFKA_PLAIN_ADMIN + "\" "
        + "password=\"" + KAFKA_PLAIN_ADMIN_PASSWORD + "\";";

    public SaslPlainPlaintextConsumerTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    @BeforeEach
    public void setUp() throws InterruptedException {
        cluster.createTopic(ClientsTestUtils.BaseConsumerTestcase.TOPIC, 2, (short) ClientsTestUtils.BaseConsumerTestcase.BROKER_COUNT);
    }

    @ClusterTest(
        brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT
    )
    public void testClassicConsumerSimpleConsumption() throws InterruptedException {
        testSimpleConsumption(cluster, Map.of(
            SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name,
            SASL_MECHANISM, MECHANISMS,
            SASL_JAAS_CONFIG, SASL_JAAS,
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
            SASL_JAAS_CONFIG, SASL_JAAS,
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
            SASL_JAAS_CONFIG, SASL_JAAS,
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
            SASL_JAAS_CONFIG, SASL_JAAS,
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
            SASL_JAAS_CONFIG, SASL_JAAS,
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
            SASL_JAAS_CONFIG, SASL_JAAS,
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT),
            // Use higher poll timeout to avoid consumer leaving the group due to timeout
            MAX_POLL_INTERVAL_MS_CONFIG, 15000
        );
        testCoordinatorFailover(cluster, config);
    }
}
