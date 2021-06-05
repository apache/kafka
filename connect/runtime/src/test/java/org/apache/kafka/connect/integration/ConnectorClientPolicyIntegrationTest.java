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
package org.apache.kafka.connect.integration;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.rest.errors.ConnectRestException;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.TOPICS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Category(IntegrationTest.class)
public class ConnectorClientPolicyIntegrationTest {

    private static final int NUM_TASKS = 1;
    private static final int NUM_WORKERS = 1;
    private static final String CONNECTOR_NAME = "simple-conn";

    @After
    public void close() {
    }

    @Test
    public void testCreateWithOverridesForNonePolicy() throws Exception {
        Map<String, String> props = basicConnectorConfig();
        props.put(ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX + SaslConfigs.SASL_JAAS_CONFIG, "sasl");
        assertFailCreateConnector("None", props);
    }

    @Test
    public void testCreateWithNotAllowedOverridesForPrincipalPolicy() throws Exception {
        Map<String, String> props = basicConnectorConfig();
        props.put(ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX + SaslConfigs.SASL_JAAS_CONFIG, "sasl");
        props.put(ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        assertFailCreateConnector("Principal", props);
    }

    @Test
    public void testCreateWithAllowedOverridesForPrincipalPolicy() throws Exception {
        Map<String, String> props = basicConnectorConfig();
        props.put(ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX + CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");
        assertPassCreateConnector("Principal", props);
    }

    @Test
    public void testCreateWithAllowedOverridesForAllPolicy() throws Exception {
        // setup up props for the sink connector
        Map<String, String> props = basicConnectorConfig();
        props.put(ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX + CommonClientConfigs.CLIENT_ID_CONFIG, "test");
        assertPassCreateConnector("All", props);
    }

    private EmbeddedConnectCluster connectClusterWithPolicy(String policy) throws InterruptedException {
        // setup Connect worker properties
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put(OFFSET_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(5_000));
        workerProps.put(WorkerConfig.CONNECTOR_CLIENT_POLICY_CLASS_CONFIG, policy);

        // setup Kafka broker properties
        Properties exampleBrokerProps = new Properties();
        exampleBrokerProps.put("auto.create.topics.enable", "false");

        // build a Connect cluster backed by Kafka and Zk
        EmbeddedConnectCluster connect = new EmbeddedConnectCluster.Builder()
            .name("connect-cluster")
            .numWorkers(NUM_WORKERS)
            .numBrokers(1)
            .workerProps(workerProps)
            .brokerProps(exampleBrokerProps)
            .build();

        // start the clusters
        connect.start();
        connect.assertions().assertAtLeastNumWorkersAreUp(NUM_WORKERS,
                "Initial group of workers did not start in time.");

        return connect;
    }

    private void assertFailCreateConnector(String policy, Map<String, String> props) throws InterruptedException {
        EmbeddedConnectCluster connect = connectClusterWithPolicy(policy);
        try {
            connect.configureConnector(CONNECTOR_NAME, props);
            fail("Shouldn't be able to create connector");
        } catch (ConnectRestException e) {
            assertEquals(e.statusCode(), 400);
        } finally {
            connect.stop();
        }
    }

    private void assertPassCreateConnector(String policy, Map<String, String> props) throws InterruptedException {
        EmbeddedConnectCluster connect = connectClusterWithPolicy(policy);
        try {
            connect.configureConnector(CONNECTOR_NAME, props);
            connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, NUM_TASKS,
                    "Connector tasks did not start in time.");
        } catch (ConnectRestException e) {
            fail("Should be able to create connector");
        } finally {
            connect.stop();
        }
    }


    public Map<String, String> basicConnectorConfig() {
        Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR_CLASS_CONFIG, MonitorableSinkConnector.class.getSimpleName());
        props.put(TASKS_MAX_CONFIG, String.valueOf(NUM_TASKS));
        props.put(TOPICS_CONFIG, "test-topic");
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        return props;
    }

}
