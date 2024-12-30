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
package org.apache.kafka.connect.mirror;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.test.MockMetricsReporter;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.apache.kafka.connect.mirror.TestUtils.makeProps;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MirrorConnectorConfigTest {

    static class TestMirrorConnectorConfig extends MirrorConnectorConfig {

        protected TestMirrorConnectorConfig(Map<String, String> props) {
            super(BASE_CONNECTOR_CONFIG_DEF, props);
        }
    }

    @Test
    public void testSourceConsumerConfig() {
        Map<String, String> connectorProps = makeProps(
                MirrorConnectorConfig.CONSUMER_CLIENT_PREFIX + "max.poll.interval.ms", "120000",
                MirrorConnectorConfig.SOURCE_CLUSTER_PREFIX + "bootstrap.servers", "localhost:2345"
        );
        MirrorConnectorConfig config = new TestMirrorConnectorConfig(connectorProps);
        Map<String, Object> connectorConsumerProps = config.sourceConsumerConfig("test");
        Map<String, Object> expectedConsumerProps = new HashMap<>();
        expectedConsumerProps.put("enable.auto.commit", "false");
        expectedConsumerProps.put("auto.offset.reset", "earliest");
        expectedConsumerProps.put("max.poll.interval.ms", "120000");
        expectedConsumerProps.put("client.id", "source1->target2|ConnectorName|test");
        expectedConsumerProps.put("bootstrap.servers", "localhost:2345");
        assertEquals(expectedConsumerProps, connectorConsumerProps);

        // checking auto.offset.reset override works
        connectorProps = makeProps(
                MirrorConnectorConfig.CONSUMER_CLIENT_PREFIX + "auto.offset.reset", "latest",
                MirrorConnectorConfig.SOURCE_CLUSTER_PREFIX + "bootstrap.servers", "localhost:2345"
        );
        config = new TestMirrorConnectorConfig(connectorProps);
        connectorConsumerProps = config.sourceConsumerConfig("test");
        expectedConsumerProps.put("auto.offset.reset", "latest");
        expectedConsumerProps.remove("max.poll.interval.ms");
        assertEquals(expectedConsumerProps, connectorConsumerProps,
                MirrorConnectorConfig.CONSUMER_CLIENT_PREFIX + " source consumer config not matching");
    }

    @Test
    public void testSourceConsumerConfigWithSourcePrefix() {
        String prefix = MirrorConnectorConfig.SOURCE_PREFIX + MirrorConnectorConfig.CONSUMER_CLIENT_PREFIX;
        Map<String, String> connectorProps = makeProps(
                prefix + "auto.offset.reset", "latest",
                prefix + "max.poll.interval.ms", "100"
        );
        MirrorConnectorConfig config = new TestMirrorConnectorConfig(connectorProps);
        Map<String, Object> connectorConsumerProps = config.sourceConsumerConfig("test");
        Map<String, Object> expectedConsumerProps = new HashMap<>();
        expectedConsumerProps.put("enable.auto.commit", "false");
        expectedConsumerProps.put("auto.offset.reset", "latest");
        expectedConsumerProps.put("max.poll.interval.ms", "100");
        expectedConsumerProps.put("client.id", "source1->target2|ConnectorName|test");
        assertEquals(expectedConsumerProps, connectorConsumerProps,
                prefix + " source consumer config not matching");
    }

    @Test
    public void testSourceProducerConfig() {
        Map<String, String> connectorProps = makeProps(
                MirrorConnectorConfig.PRODUCER_CLIENT_PREFIX + "acks", "1"
        );
        MirrorConnectorConfig config = new TestMirrorConnectorConfig(connectorProps);
        Map<String, Object> connectorProducerProps = config.sourceProducerConfig("test");
        Map<String, Object> expectedProducerProps = new HashMap<>();
        expectedProducerProps.put("acks", "1");
        expectedProducerProps.put("client.id", "source1->target2|ConnectorName|test");
        assertEquals(expectedProducerProps, connectorProducerProps,
                MirrorConnectorConfig.PRODUCER_CLIENT_PREFIX  + " source product config not matching");
    }

    @Test
    public void testSourceProducerConfigWithSourcePrefix() {
        String prefix = MirrorConnectorConfig.SOURCE_PREFIX + MirrorConnectorConfig.PRODUCER_CLIENT_PREFIX;
        Map<String, String> connectorProps = makeProps(prefix + "acks", "1");
        MirrorConnectorConfig config = new TestMirrorConnectorConfig(connectorProps);
        Map<String, Object> connectorProducerProps = config.sourceProducerConfig("test");
        Map<String, Object> expectedProducerProps = new HashMap<>();
        expectedProducerProps.put("acks", "1");
        expectedProducerProps.put("client.id", "source1->target2|ConnectorName|test");
        assertEquals(expectedProducerProps, connectorProducerProps,
                prefix + " source producer config not matching");
    }

    @Test
    public void testSourceAdminConfig() {
        Map<String, String> connectorProps = makeProps(
                MirrorConnectorConfig.ADMIN_CLIENT_PREFIX +
                        "connections.max.idle.ms", "10000"
        );
        MirrorConnectorConfig config = new TestMirrorConnectorConfig(connectorProps);
        Map<String, Object> connectorAdminProps = config.sourceAdminConfig("test");
        Map<String, Object> expectedAdminProps = new HashMap<>();
        expectedAdminProps.put("connections.max.idle.ms", "10000");
        expectedAdminProps.put("client.id", "source1->target2|ConnectorName|test");
        assertEquals(expectedAdminProps, connectorAdminProps,
                MirrorConnectorConfig.ADMIN_CLIENT_PREFIX + " source connector admin props not matching");
    }

    @Test
    public void testSourceAdminConfigWithSourcePrefix() {
        String prefix = MirrorConnectorConfig.SOURCE_PREFIX + MirrorConnectorConfig.ADMIN_CLIENT_PREFIX;
        Map<String, String> connectorProps = makeProps(prefix + "connections.max.idle.ms", "10000");
        MirrorConnectorConfig config = new TestMirrorConnectorConfig(connectorProps);
        Map<String, Object> connectorAdminProps = config.sourceAdminConfig("test");
        Map<String, Object> expectedAdminProps = new HashMap<>();
        expectedAdminProps.put("connections.max.idle.ms", "10000");
        expectedAdminProps.put("client.id", "source1->target2|ConnectorName|test");
        assertEquals(expectedAdminProps, connectorAdminProps, prefix + " source connector admin props not matching");
    }

    @Test
    public void testTargetAdminConfig() {
        Map<String, String> connectorProps = makeProps(
                MirrorConnectorConfig.ADMIN_CLIENT_PREFIX +
                        "connections.max.idle.ms", "10000"
        );
        MirrorConnectorConfig config = new TestMirrorConnectorConfig(connectorProps);
        Map<String, Object> connectorAdminProps = config.targetAdminConfig("test");
        Map<String, Object> expectedAdminProps = new HashMap<>();
        expectedAdminProps.put("connections.max.idle.ms", "10000");
        expectedAdminProps.put("client.id", "source1->target2|ConnectorName|test");
        assertEquals(expectedAdminProps, connectorAdminProps,
                MirrorConnectorConfig.ADMIN_CLIENT_PREFIX + " target connector admin props not matching");
    }

    @Test
    public void testTargetAdminConfigWithSourcePrefix() {
        String prefix = MirrorConnectorConfig.TARGET_PREFIX + MirrorConnectorConfig.ADMIN_CLIENT_PREFIX;
        Map<String, String> connectorProps = makeProps(prefix + "connections.max.idle.ms", "10000");
        MirrorConnectorConfig config = new TestMirrorConnectorConfig(connectorProps);
        Map<String, Object> connectorAdminProps = config.targetAdminConfig("test");
        Map<String, Object> expectedAdminProps = new HashMap<>();
        expectedAdminProps.put("connections.max.idle.ms", "10000");
        expectedAdminProps.put("client.id", "source1->target2|ConnectorName|test");
        assertEquals(expectedAdminProps, connectorAdminProps, prefix + " source connector admin props not matching");
    }

    @Test
    public void testInvalidSecurityProtocol() {
        ConfigException ce = assertThrows(ConfigException.class,
                () -> new TestMirrorConnectorConfig(makeProps(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "abc")));
        assertTrue(ce.getMessage().contains(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
    }

    @Test
    public void testCaseInsensitiveSecurityProtocol() {
        final String saslSslLowerCase = SecurityProtocol.SASL_SSL.name.toLowerCase(Locale.ROOT);
        final TestMirrorConnectorConfig config = new TestMirrorConnectorConfig(makeProps(
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, saslSslLowerCase));
        assertEquals(saslSslLowerCase, config.originalsStrings().get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
    }

    @Test
    public void testMetricsReporters() {
        Map<String, String> connectorProps = makeProps();
        MirrorConnectorConfig config = new TestMirrorConnectorConfig(connectorProps);
        assertEquals(1, config.metricsReporters().size());

        connectorProps.put("metric.reporters", JmxReporter.class.getName() + "," + MockMetricsReporter.class.getName());
        config = new TestMirrorConnectorConfig(connectorProps);
        assertEquals(2, config.metricsReporters().size());
    }

    @Test
    public void testExplicitlyEnableJmxReporter() {
        String reporters = MockMetricsReporter.class.getName() + "," + JmxReporter.class.getName();
        Map<String, String> connectorProps = makeProps("metric.reporters", reporters);
        MirrorConnectorConfig config = new TestMirrorConnectorConfig(connectorProps);
        assertEquals(2, config.metricsReporters().size());
    }

    @Test
    public void testReplicationPolicy() {
        MirrorConnectorConfig config = new TestMirrorConnectorConfig(makeProps());
        assertSame(config.replicationPolicy(), config.replicationPolicy());
    }

}
