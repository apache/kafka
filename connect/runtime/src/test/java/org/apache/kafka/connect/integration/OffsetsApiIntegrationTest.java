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
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.SourceConnectorConfig;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorOffset;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorOffsets;
import org.apache.kafka.connect.runtime.rest.errors.ConnectRestException;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.SinkUtils;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.connect.util.clusters.EmbeddedKafkaCluster;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.connect.integration.MonitorableSourceConnector.TOPIC_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.TOPICS_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.DEFAULT_TOPIC_CREATION_PREFIX;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.PARTITIONS_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests for Kafka Connect's connector offset management REST APIs
 */
@Category(IntegrationTest.class)
public class OffsetsApiIntegrationTest {

    private static final String CONNECTOR_NAME = "test-connector";
    private static final String TOPIC = "test-topic";
    private static final Integer NUM_TASKS = 2;
    private static final long OFFSET_COMMIT_INTERVAL_MS = TimeUnit.SECONDS.toMillis(1);
    private static final int NUM_WORKERS = 3;
    private EmbeddedConnectCluster connect;

    @Before
    public void setup() {
        // setup Connect worker properties
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put(OFFSET_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(OFFSET_COMMIT_INTERVAL_MS));

        // build a Connect cluster backed by Kafka and Zk
        connect = new EmbeddedConnectCluster.Builder()
                .name("connect-cluster")
                .numWorkers(NUM_WORKERS)
                .workerProps(workerProps)
                .build();
        connect.start();
    }

    @After
    public void tearDown() {
        connect.stop();
    }

    @Test
    public void testGetNonExistentConnectorOffsets() {
        ConnectRestException e = assertThrows(ConnectRestException.class,
                () -> connect.connectorOffsets("non-existent-connector"));
        assertEquals(404, e.errorCode());
    }

    @Test
    public void testGetSinkConnectorOffsets() throws Exception {
        getAndVerifySinkConnectorOffsets(baseSinkConnectorConfigs(), connect.kafka());
    }

    @Test
    public void testGetSinkConnectorOffsetsOverriddenConsumerGroupId() throws Exception {
        Map<String, String> connectorConfigs = baseSinkConnectorConfigs();
        connectorConfigs.put(ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX + CommonClientConfigs.GROUP_ID_CONFIG,
                "overridden-group-id");
        getAndVerifySinkConnectorOffsets(connectorConfigs, connect.kafka());

        // Ensure that the overridden consumer group ID was the one actually used
        try (Admin admin = connect.kafka().createAdminClient()) {
            Collection<ConsumerGroupListing> consumerGroups = admin.listConsumerGroups().all().get();
            assertTrue(consumerGroups.stream().anyMatch(consumerGroupListing -> "overridden-group-id".equals(consumerGroupListing.groupId())));
            assertTrue(consumerGroups.stream().noneMatch(consumerGroupListing -> SinkUtils.consumerGroupId(CONNECTOR_NAME).equals(consumerGroupListing.groupId())));
        }
    }

    @Test
    public void testGetSinkConnectorOffsetsDifferentKafkaClusterTargeted() throws Exception {
        EmbeddedKafkaCluster kafkaCluster = new EmbeddedKafkaCluster(1, new Properties());

        try (AutoCloseable ignored = kafkaCluster::stop) {
            kafkaCluster.start();

            Map<String, String> connectorConfigs = baseSinkConnectorConfigs();
            connectorConfigs.put(ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                    kafkaCluster.bootstrapServers());
            connectorConfigs.put(ConnectorConfig.CONNECTOR_CLIENT_ADMIN_OVERRIDES_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                    kafkaCluster.bootstrapServers());

            getAndVerifySinkConnectorOffsets(connectorConfigs, kafkaCluster);
        }
    }

    private void getAndVerifySinkConnectorOffsets(Map<String, String> connectorConfigs, EmbeddedKafkaCluster kafkaCluster) throws Exception {
        kafkaCluster.createTopic(TOPIC, 5);

        // Produce 10 messages to each partition
        for (int partition = 0; partition < 5; partition++) {
            for (int message = 0; message < 10; message++) {
                kafkaCluster.produce(TOPIC, partition, "key", "value");
            }
        }

        // Create sink connector
        connect.configureConnector(CONNECTOR_NAME, connectorConfigs);
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, NUM_TASKS,
                "Connector tasks did not start in time.");

        TestUtils.waitForCondition(() -> {
            ConnectorOffsets offsets = connect.connectorOffsets(CONNECTOR_NAME);
            // There should be 5 topic partitions
            if (offsets.offsets().size() != 5) {
                return false;
            }
            for (ConnectorOffset offset: offsets.offsets()) {
                assertEquals("test-topic", offset.partition().get(SinkUtils.KAFKA_TOPIC_KEY));
                if ((Integer) offset.offset().get(SinkUtils.KAFKA_OFFSET_KEY) != 10) {
                    return false;
                }
            }
            return true;
        }, "Sink connector consumer group offsets should catch up to the topic end offsets");

        // Produce 10 more messages to each partition
        for (int partition = 0; partition < 5; partition++) {
            for (int message = 0; message < 10; message++) {
                kafkaCluster.produce(TOPIC, partition, "key", "value");
            }
        }

        TestUtils.waitForCondition(() -> {
            ConnectorOffsets offsets = connect.connectorOffsets(CONNECTOR_NAME);
            // There should be 5 topic partitions
            if (offsets.offsets().size() != 5) {
                return false;
            }
            for (ConnectorOffset offset: offsets.offsets()) {
                assertEquals("test-topic", offset.partition().get(SinkUtils.KAFKA_TOPIC_KEY));
                if ((Integer) offset.offset().get(SinkUtils.KAFKA_OFFSET_KEY) != 20) {
                    return false;
                }
            }
            return true;
        }, "Sink connector consumer group offsets should catch up to the topic end offsets");
    }

    @Test
    public void testGetSourceConnectorOffsets() throws Exception {
        getAndVerifySourceConnectorOffsets(baseSourceConnectorConfigs());
    }

    @Test
    public void testGetSourceConnectorOffsetsCustomOffsetsTopic() throws Exception {
        Map<String, String> connectorConfigs = baseSourceConnectorConfigs();
        connectorConfigs.put(SourceConnectorConfig.OFFSETS_TOPIC_CONFIG, "custom-offsets-topic");
        getAndVerifySourceConnectorOffsets(connectorConfigs);
    }

    @Test
    public void testGetSourceConnectorOffsetsDifferentKafkaClusterTargeted() throws Exception {
        EmbeddedKafkaCluster kafkaCluster = new EmbeddedKafkaCluster(1, new Properties());

        try (AutoCloseable ignored = kafkaCluster::stop) {
            kafkaCluster.start();

            Map<String, String> connectorConfigs = baseSourceConnectorConfigs();
            connectorConfigs.put(ConnectorConfig.CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                    kafkaCluster.bootstrapServers());
            connectorConfigs.put(ConnectorConfig.CONNECTOR_CLIENT_ADMIN_OVERRIDES_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                    kafkaCluster.bootstrapServers());

            getAndVerifySourceConnectorOffsets(connectorConfigs);
        }
    }

    private void getAndVerifySourceConnectorOffsets(Map<String, String> connectorConfigs) throws Exception {
        // Create source connector
        connect.configureConnector(CONNECTOR_NAME, connectorConfigs);
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, NUM_TASKS,
                "Connector tasks did not start in time.");

        TestUtils.waitForCondition(() -> {
            ConnectorOffsets offsets = connect.connectorOffsets(CONNECTOR_NAME);
            // The MonitorableSourceConnector has a source partition per task
            if (offsets.offsets().size() != NUM_TASKS) {
                return false;
            }
            for (ConnectorOffset offset : offsets.offsets()) {
                assertTrue(((String) offset.partition().get("task.id")).startsWith(CONNECTOR_NAME));
                if ((Integer) offset.offset().get("saved") != 10) {
                    return false;
                }
            }
            return true;
        }, "Source connector offsets should reflect the expected number of records produced");

        // Each task should produce 10 more records
        connectorConfigs.put(MonitorableSourceConnector.MAX_MESSAGES_PRODUCED_CONFIG, "20");
        connect.configureConnector(CONNECTOR_NAME, connectorConfigs);

        TestUtils.waitForCondition(() -> {
            ConnectorOffsets offsets = connect.connectorOffsets(CONNECTOR_NAME);
            // The MonitorableSourceConnector has a source partition per task
            if (offsets.offsets().size() != NUM_TASKS) {
                return false;
            }
            for (ConnectorOffset offset : offsets.offsets()) {
                assertTrue(((String) offset.partition().get("task.id")).startsWith(CONNECTOR_NAME));
                if ((Integer) offset.offset().get("saved") != 20) {
                    return false;
                }
            }
            return true;
        }, "Source connector offsets should reflect the expected number of records produced");
    }

    private Map<String, String> baseSinkConnectorConfigs() {
        Map<String, String> configs = new HashMap<>();
        configs.put(CONNECTOR_CLASS_CONFIG, MonitorableSinkConnector.class.getSimpleName());
        configs.put(TASKS_MAX_CONFIG, String.valueOf(NUM_TASKS));
        configs.put(TOPICS_CONFIG, TOPIC);
        configs.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        configs.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        return configs;
    }

    private Map<String, String> baseSourceConnectorConfigs() {
        Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR_CLASS_CONFIG, MonitorableSourceConnector.class.getSimpleName());
        props.put(TASKS_MAX_CONFIG, String.valueOf(NUM_TASKS));
        props.put(TOPIC_CONFIG, TOPIC);
        props.put(MonitorableSourceConnector.MESSAGES_PER_POLL_CONFIG, "3");
        props.put(MonitorableSourceConnector.MAX_MESSAGES_PRODUCED_CONFIG, "10");
        props.put(ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + REPLICATION_FACTOR_CONFIG, "1");
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + PARTITIONS_CONFIG, "1");
        return props;
    }
}
