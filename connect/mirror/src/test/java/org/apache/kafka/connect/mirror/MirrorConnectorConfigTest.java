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
import org.apache.kafka.test.MockMetricsReporter;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.HashMap;

import static org.apache.kafka.connect.mirror.TestUtils.makeProps;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class MirrorConnectorConfigTest {

    static class TestMirrorConnectorConfig extends MirrorConnectorConfig {

        protected TestMirrorConnectorConfig(Map<String, String> props) {
            super(BASE_CONNECTOR_CONFIG_DEF, props);
        }
    }

    @Test
    public void testSourceConsumerConfig() {
        Map<String, String> connectorProps = makeProps(
                MirrorConnectorConfig.CONSUMER_CLIENT_PREFIX + "max.poll.interval.ms", "120000"
        );
        MirrorConnectorConfig config = new TestMirrorConnectorConfig(connectorProps);
        Map<String, Object> connectorConsumerProps = config.sourceConsumerConfig();
        Map<String, Object> expectedConsumerProps = new HashMap<>();
        expectedConsumerProps.put("enable.auto.commit", "false");
        expectedConsumerProps.put("auto.offset.reset", "earliest");
        expectedConsumerProps.put("max.poll.interval.ms", "120000");
        assertEquals(expectedConsumerProps, connectorConsumerProps);

        // checking auto.offset.reset override works
        connectorProps = makeProps(
                MirrorConnectorConfig.CONSUMER_CLIENT_PREFIX + "auto.offset.reset", "latest"
        );
        config = new TestMirrorConnectorConfig(connectorProps);
        connectorConsumerProps = config.sourceConsumerConfig();
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
        Map<String, Object> connectorConsumerProps = config.sourceConsumerConfig();
        Map<String, Object> expectedConsumerProps = new HashMap<>();
        expectedConsumerProps.put("enable.auto.commit", "false");
        expectedConsumerProps.put("auto.offset.reset", "latest");
        expectedConsumerProps.put("max.poll.interval.ms", "100");
        assertEquals(expectedConsumerProps, connectorConsumerProps,
                prefix + " source consumer config not matching");
    }

    @Test
    public void testSourceProducerConfig() {
        Map<String, String> connectorProps = makeProps(
                MirrorConnectorConfig.PRODUCER_CLIENT_PREFIX + "acks", "1"
        );
        MirrorConnectorConfig config = new TestMirrorConnectorConfig(connectorProps);
        Map<String, Object> connectorProducerProps = config.sourceProducerConfig();
        Map<String, Object> expectedProducerProps = new HashMap<>();
        expectedProducerProps.put("acks", "1");
        assertEquals(expectedProducerProps, connectorProducerProps,
                MirrorConnectorConfig.PRODUCER_CLIENT_PREFIX  + " source product config not matching");
    }

    @Test
    public void testSourceProducerConfigWithSourcePrefix() {
        String prefix = MirrorConnectorConfig.SOURCE_PREFIX + MirrorConnectorConfig.PRODUCER_CLIENT_PREFIX;
        Map<String, String> connectorProps = makeProps(prefix + "acks", "1");
        MirrorConnectorConfig config = new TestMirrorConnectorConfig(connectorProps);
        Map<String, Object> connectorProducerProps = config.sourceProducerConfig();
        Map<String, Object> expectedProducerProps = new HashMap<>();
        expectedProducerProps.put("acks", "1");
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
        Map<String, Object> connectorAdminProps = config.sourceAdminConfig();
        Map<String, Object> expectedAdminProps = new HashMap<>();
        expectedAdminProps.put("connections.max.idle.ms", "10000");
        assertEquals(expectedAdminProps, connectorAdminProps,
                MirrorConnectorConfig.ADMIN_CLIENT_PREFIX + " source connector admin props not matching");
    }

    @Test
    public void testSourceAdminConfigWithSourcePrefix() {
        String prefix = MirrorConnectorConfig.SOURCE_PREFIX + MirrorConnectorConfig.ADMIN_CLIENT_PREFIX;
        Map<String, String> connectorProps = makeProps(prefix + "connections.max.idle.ms", "10000");
        MirrorConnectorConfig config = new TestMirrorConnectorConfig(connectorProps);
        Map<String, Object> connectorAdminProps = config.sourceAdminConfig();
        Map<String, Object> expectedAdminProps = new HashMap<>();
        expectedAdminProps.put("connections.max.idle.ms", "10000");
        assertEquals(expectedAdminProps, connectorAdminProps, prefix + " source connector admin props not matching");
    }

    @Test
    public void testTargetAdminConfig() {
        Map<String, String> connectorProps = makeProps(
                MirrorConnectorConfig.ADMIN_CLIENT_PREFIX +
                        "connections.max.idle.ms", "10000"
        );
        MirrorConnectorConfig config = new TestMirrorConnectorConfig(connectorProps);
        Map<String, Object> connectorAdminProps = config.targetAdminConfig();
        Map<String, Object> expectedAdminProps = new HashMap<>();
        expectedAdminProps.put("connections.max.idle.ms", "10000");
        assertEquals(expectedAdminProps, connectorAdminProps,
                MirrorConnectorConfig.ADMIN_CLIENT_PREFIX + " target connector admin props not matching");
    }

    @Test
    public void testTargetAdminConfigWithSourcePrefix() {
        String prefix = MirrorConnectorConfig.TARGET_PREFIX + MirrorConnectorConfig.ADMIN_CLIENT_PREFIX;
        Map<String, String> connectorProps = makeProps(prefix + "connections.max.idle.ms", "10000");
        MirrorConnectorConfig config = new TestMirrorConnectorConfig(connectorProps);
        Map<String, Object> connectorAdminProps = config.targetAdminConfig();
        Map<String, Object> expectedAdminProps = new HashMap<>();
        expectedAdminProps.put("connections.max.idle.ms", "10000");
        assertEquals(expectedAdminProps, connectorAdminProps, prefix + " source connector admin props not matching");
    }

    @Test
    public void testInvalidSecurityProtocol() {
        ConfigException ce = assertThrows(ConfigException.class,
                () -> new TestMirrorConnectorConfig(makeProps(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "abc")));
        assertTrue(ce.getMessage().contains(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testMetricsReporters() {
        Map<String, String> connectorProps = makeProps("metric.reporters", MockMetricsReporter.class.getName());
        MirrorConnectorConfig config = new TestMirrorConnectorConfig(connectorProps);
        assertEquals(2, config.metricsReporters().size());

        connectorProps.put(CommonClientConfigs.AUTO_INCLUDE_JMX_REPORTER_CONFIG, "false");
        config = new TestMirrorConnectorConfig(connectorProps);
        assertEquals(1, config.metricsReporters().size());
    }

    @Test
    public void testExplicitlyEnableJmxReporter() {
        String reporters = MockMetricsReporter.class.getName() + "," + JmxReporter.class.getName();
        Map<String, String> connectorProps = makeProps("metric.reporters", reporters);
        MirrorConnectorConfig config = new TestMirrorConnectorConfig(connectorProps);
        assertEquals(2, config.metricsReporters().size());
    }

}
