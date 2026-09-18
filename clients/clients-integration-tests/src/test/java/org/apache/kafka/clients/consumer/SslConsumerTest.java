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
import org.junit.jupiter.api.Timeout;

import java.util.Locale;
import java.util.Map;

import static org.apache.kafka.clients.ClientsTestUtils.BaseConsumerTestcase.testClusterResourceListener;
import static org.apache.kafka.clients.ClientsTestUtils.BaseConsumerTestcase.testCoordinatorFailover;
import static org.apache.kafka.clients.ClientsTestUtils.BaseConsumerTestcase.testSimpleConsumption;
import static org.apache.kafka.clients.CommonClientConfigs.MAX_POLL_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG;

@Timeout(600)
@ClusterTestDefaults(
    types = {Type.KRAFT},
    brokers = ClientsTestUtils.BaseConsumerTestcase.BROKER_COUNT,
    serverProperties = {
        @ClusterConfigProperty(key = OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
        @ClusterConfigProperty(key = GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, value = "100"),
    }
)
public class SslConsumerTest {

    private final ClusterInstance cluster;

    public SslConsumerTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    @BeforeEach
    public void beforeEach() throws InterruptedException {
        cluster.createTopic(ClientsTestUtils.BaseConsumerTestcase.TOPIC, 2, (short) ClientsTestUtils.BaseConsumerTestcase.BROKER_COUNT);
    }

    @ClusterTest(
        brokerSecurityProtocol = SecurityProtocol.SSL,
        controllerSecurityProtocol = SecurityProtocol.SSL
    )
    public void testClassicConsumerSimpleConsumption() throws InterruptedException {
        testSimpleConsumption(cluster, Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT))
        );
    }

    @ClusterTest(
        brokerSecurityProtocol = SecurityProtocol.SSL,
        controllerSecurityProtocol = SecurityProtocol.SSL
    )
    public void testAsyncConsumerSimpleConsumption() throws InterruptedException {
        testSimpleConsumption(cluster, Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT))
        );
    }

    @ClusterTest(
        brokerSecurityProtocol = SecurityProtocol.SSL,
        controllerSecurityProtocol = SecurityProtocol.SSL
    )
    public void testClassicConsumerClusterResourceListener() throws InterruptedException {
        testClusterResourceListener(cluster, Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT))
        );
    }

    @ClusterTest(
        brokerSecurityProtocol = SecurityProtocol.SSL,
        controllerSecurityProtocol = SecurityProtocol.SSL
    )
    public void testAsyncConsumerClusterResourceListener() throws InterruptedException {
        testClusterResourceListener(cluster, Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT))
        );
    }

    @ClusterTest(
        brokerSecurityProtocol = SecurityProtocol.SSL,
        controllerSecurityProtocol = SecurityProtocol.SSL
    )
    public void testClassicConsumerCoordinatorFailover() throws InterruptedException {
        Map<String, Object> config = Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT),
            SESSION_TIMEOUT_MS_CONFIG, 5001,
            HEARTBEAT_INTERVAL_MS_CONFIG, 1000,
            // Use higher poll timeout to avoid consumer leaving the group due to timeout
            MAX_POLL_INTERVAL_MS_CONFIG, 15000
        );
        testCoordinatorFailover(cluster, config);
    }

    @ClusterTest(
        brokerSecurityProtocol = SecurityProtocol.SSL,
        controllerSecurityProtocol = SecurityProtocol.SSL
    )
    public void testAsyncConsumeCoordinatorFailover() throws InterruptedException {
        Map<String, Object> config = Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT),
            // Use higher poll timeout to avoid consumer leaving the group due to timeout
            MAX_POLL_INTERVAL_MS_CONFIG, 15000
        );
        testCoordinatorFailover(cluster, config);
    }
}
