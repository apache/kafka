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
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
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

import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests for Kafka Connect's connector offset management REST APIs
 */
@Category(IntegrationTest.class)
public class OffsetsApiIntegrationTest {
    private static final long OFFSET_COMMIT_INTERVAL_MS = TimeUnit.SECONDS.toMillis(1);
    private static final long OFFSET_READ_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(30);
    private static final int NUM_WORKERS = 3;
    private static final String CONNECTOR_NAME = "test-connector";
    private static final String TOPIC = "test-topic";
    private static final int NUM_TASKS = 2;
    private static final int NUM_RECORDS_PER_PARTITION = 10;
    private Map<String, String> workerProps;
    private EmbeddedConnectCluster.Builder connectBuilder;
    private EmbeddedConnectCluster connect;

    @Before
    public void setup() {
        Properties brokerProps = new Properties();
        brokerProps.put("transaction.state.log.replication.factor", "1");
        brokerProps.put("transaction.state.log.min.isr", "1");

        // setup Connect worker properties
        workerProps = new HashMap<>();
        workerProps.put(OFFSET_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(OFFSET_COMMIT_INTERVAL_MS));

        // build a Connect cluster backed by Kafka and Zk
        connectBuilder = new EmbeddedConnectCluster.Builder()
                .name("connect-cluster")
                .numWorkers(NUM_WORKERS)
                .brokerProps(brokerProps)
                .workerProps(workerProps);
    }

    @After
    public void tearDown() {
        connect.stop();
    }

    @Test
    public void testGetNonExistentConnectorOffsets() {
        connect = connectBuilder.build();
        connect.start();
        ConnectRestException e = assertThrows(ConnectRestException.class,
                () -> connect.connectorOffsets("non-existent-connector"));
        assertEquals(404, e.errorCode());
    }

    @Test
    public void testGetSinkConnectorOffsets() throws Exception {
        connect = connectBuilder.build();
        connect.start();
        getAndVerifySinkConnectorOffsets(baseSinkConnectorConfigs(), connect.kafka());
    }

    @Test
    public void testGetSinkConnectorOffsetsOverriddenConsumerGroupId() throws Exception {
        connect = connectBuilder.build();
        connect.start();
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
        connect = connectBuilder.build();
        connect.start();
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

        // Produce records to each partition
        for (int partition = 0; partition < 5; partition++) {
            for (int record = 0; record < NUM_RECORDS_PER_PARTITION; record++) {
                kafkaCluster.produce(TOPIC, partition, "key", "value");
            }
        }

        // Create sink connector
        connect.configureConnector(CONNECTOR_NAME, connectorConfigs);
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, NUM_TASKS,
                "Connector tasks did not start in time.");

        verifyExpectedSinkConnectorOffsets(CONNECTOR_NAME, TOPIC, 5, NUM_RECORDS_PER_PARTITION,
                "Sink connector consumer group offsets should catch up to the topic end offsets");

        // Produce more records to each partition
        for (int partition = 0; partition < 5; partition++) {
            for (int record = 0; record < NUM_RECORDS_PER_PARTITION; record++) {
                kafkaCluster.produce(TOPIC, partition, "key", "value");
            }
        }

        verifyExpectedSinkConnectorOffsets(CONNECTOR_NAME, TOPIC, 5, 2 * NUM_RECORDS_PER_PARTITION,
                "Sink connector consumer group offsets should catch up to the topic end offsets");
    }

    @Test
    public void testGetSourceConnectorOffsets() throws Exception {
        connect = connectBuilder.build();
        connect.start();
        getAndVerifySourceConnectorOffsets(baseSourceConnectorConfigs());
    }

    @Test
    public void testGetSourceConnectorOffsetsCustomOffsetsTopic() throws Exception {
        connect = connectBuilder.build();
        connect.start();
        Map<String, String> connectorConfigs = baseSourceConnectorConfigs();
        connectorConfigs.put(SourceConnectorConfig.OFFSETS_TOPIC_CONFIG, "custom-offsets-topic");
        getAndVerifySourceConnectorOffsets(connectorConfigs);
    }

    @Test
    public void testGetSourceConnectorOffsetsDifferentKafkaClusterTargeted() throws Exception {
        connect = connectBuilder.build();
        connect.start();
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

        verifyExpectedSourceConnectorOffsets(CONNECTOR_NAME, NUM_TASKS, NUM_RECORDS_PER_PARTITION,
                "Source connector offsets should reflect the expected number of records produced");

        // Each task should produce more records
        connectorConfigs.put(MonitorableSourceConnector.MAX_MESSAGES_PRODUCED_CONFIG, String.valueOf(2 * NUM_RECORDS_PER_PARTITION));
        connect.configureConnector(CONNECTOR_NAME, connectorConfigs);

        verifyExpectedSourceConnectorOffsets(CONNECTOR_NAME, NUM_TASKS, 2 * NUM_RECORDS_PER_PARTITION,
                "Source connector offsets should reflect the expected number of records produced");
    }

    @Test
    public void testAlterOffsetsNonExistentConnector() throws Exception {
        connect = connectBuilder.build();
        connect.start();
        ConnectRestException e = assertThrows(ConnectRestException.class,
                () -> connect.alterConnectorOffsets("non-existent-connector", new ConnectorOffsets(Collections.singletonList(
                        new ConnectorOffset(Collections.emptyMap(), Collections.emptyMap())))));
        assertEquals(404, e.errorCode());
    }

    @Test
    public void testAlterOffsetsNonStoppedConnector() throws Exception {
        connect = connectBuilder.build();
        connect.start();
        // Create source connector
        connect.configureConnector(CONNECTOR_NAME, baseSourceConnectorConfigs());
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, NUM_TASKS,
                "Connector tasks did not start in time.");

        List<ConnectorOffset> offsets = new ArrayList<>();
        // The MonitorableSourceConnector has a source partition per task
        for (int i = 0; i < NUM_TASKS; i++) {
            offsets.add(
                    new ConnectorOffset(Collections.singletonMap("task.id", CONNECTOR_NAME + "-" + i),
                            Collections.singletonMap("saved", 5))
            );
        }

        // Try altering offsets for a running connector
        ConnectRestException e = assertThrows(ConnectRestException.class,
                () -> connect.alterConnectorOffsets(CONNECTOR_NAME, new ConnectorOffsets(offsets)));
        assertEquals(400, e.errorCode());

        connect.pauseConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorAndExactlyNumTasksArePaused(
                CONNECTOR_NAME,
                NUM_TASKS,
                "Connector did not pause in time"
        );

        // Try altering offsets for a paused (not stopped) connector
        e = assertThrows(ConnectRestException.class,
                () -> connect.alterConnectorOffsets(CONNECTOR_NAME, new ConnectorOffsets(offsets)));
        assertEquals(400, e.errorCode());
    }

    @Test
    public void testAlterSinkConnectorOffsets() throws Exception {
        connect = connectBuilder.build();
        connect.start();
        alterAndVerifySinkConnectorOffsets(baseSinkConnectorConfigs(), connect.kafka());
    }

    @Test
    public void testAlterSinkConnectorOffsetsOverriddenConsumerGroupId() throws Exception {
        connect = connectBuilder.build();
        connect.start();
        Map<String, String> connectorConfigs = baseSinkConnectorConfigs();
        connectorConfigs.put(ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX + CommonClientConfigs.GROUP_ID_CONFIG,
                "overridden-group-id");
        alterAndVerifySinkConnectorOffsets(connectorConfigs, connect.kafka());
        // Ensure that the overridden consumer group ID was the one actually used
        try (Admin admin = connect.kafka().createAdminClient()) {
            Collection<ConsumerGroupListing> consumerGroups = admin.listConsumerGroups().all().get();
            assertTrue(consumerGroups.stream().anyMatch(consumerGroupListing -> "overridden-group-id".equals(consumerGroupListing.groupId())));
            assertTrue(consumerGroups.stream().noneMatch(consumerGroupListing -> SinkUtils.consumerGroupId(CONNECTOR_NAME).equals(consumerGroupListing.groupId())));
        }
    }

    @Test
    public void testAlterSinkConnectorOffsetsDifferentKafkaClusterTargeted() throws Exception {
        connect = connectBuilder.build();
        connect.start();
        EmbeddedKafkaCluster kafkaCluster = new EmbeddedKafkaCluster(1, new Properties());

        try (AutoCloseable ignored = kafkaCluster::stop) {
            kafkaCluster.start();

            Map<String, String> connectorConfigs = baseSinkConnectorConfigs();
            connectorConfigs.put(ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                    kafkaCluster.bootstrapServers());
            connectorConfigs.put(ConnectorConfig.CONNECTOR_CLIENT_ADMIN_OVERRIDES_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                    kafkaCluster.bootstrapServers());

            alterAndVerifySinkConnectorOffsets(connectorConfigs, kafkaCluster);
        }
    }

    private void alterAndVerifySinkConnectorOffsets(Map<String, String> connectorConfigs, EmbeddedKafkaCluster kafkaCluster) throws Exception {
        int numPartitions = 3;
        kafkaCluster.createTopic(TOPIC, numPartitions);

        // Produce records to each partition
        for (int partition = 0; partition < numPartitions; partition++) {
            for (int record = 0; record < NUM_RECORDS_PER_PARTITION; record++) {
                kafkaCluster.produce(TOPIC, partition, "key", "value");
            }
        }
        // Create sink connector
        connect.configureConnector(CONNECTOR_NAME, connectorConfigs);
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, NUM_TASKS,
                "Connector tasks did not start in time.");

        verifyExpectedSinkConnectorOffsets(CONNECTOR_NAME, TOPIC, numPartitions, NUM_RECORDS_PER_PARTITION,
                "Sink connector consumer group offsets should catch up to the topic end offsets");

        connect.stopConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorIsStopped(
                CONNECTOR_NAME,
                "Connector did not stop in time"
        );

        // Delete the offset of one partition; alter the offsets of the others
        List<ConnectorOffset> offsetsToAlter = new ArrayList<>();
        Map<String, Object> partition = new HashMap<>();
        partition.put(SinkUtils.KAFKA_TOPIC_KEY, TOPIC);
        partition.put(SinkUtils.KAFKA_PARTITION_KEY, 0);
        offsetsToAlter.add(new ConnectorOffset(partition, null));

        for (int i = 1; i < numPartitions; i++) {
            partition = new HashMap<>();
            partition.put(SinkUtils.KAFKA_TOPIC_KEY, TOPIC);
            partition.put(SinkUtils.KAFKA_PARTITION_KEY, i);
            offsetsToAlter.add(new ConnectorOffset(partition, Collections.singletonMap(SinkUtils.KAFKA_OFFSET_KEY, 5)));
        }

        String response = connect.alterConnectorOffsets(CONNECTOR_NAME, new ConnectorOffsets(offsetsToAlter));
        assertThat(response, containsString("The Connect framework-managed offsets for this connector have been altered successfully. " +
                "However, if this connector manages offsets externally, they will need to be manually altered in the system that the connector uses."));

        verifyExpectedSinkConnectorOffsets(CONNECTOR_NAME, TOPIC, numPartitions - 1, 5,
                "Sink connector consumer group offsets should reflect the altered offsets");

        // Update the connector's configs; this time expect SinkConnector::alterOffsets to return true
        connectorConfigs.put(MonitorableSinkConnector.ALTER_OFFSETS_RESULT, "true");
        connect.configureConnector(CONNECTOR_NAME, connectorConfigs);

        // Alter offsets again while the connector is still in a stopped state
        offsetsToAlter.clear();
        for (int i = 1; i < numPartitions; i++) {
            partition = new HashMap<>();
            partition.put(SinkUtils.KAFKA_TOPIC_KEY, TOPIC);
            partition.put(SinkUtils.KAFKA_PARTITION_KEY, i);
            offsetsToAlter.add(new ConnectorOffset(partition, Collections.singletonMap(SinkUtils.KAFKA_OFFSET_KEY, 3)));
        }

        response = connect.alterConnectorOffsets(CONNECTOR_NAME, new ConnectorOffsets(offsetsToAlter));
        assertThat(response, containsString("The offsets for this connector have been altered successfully"));

        verifyExpectedSinkConnectorOffsets(CONNECTOR_NAME, TOPIC, numPartitions - 1, 3,
                "Sink connector consumer group offsets should reflect the altered offsets");

        // Resume the connector and expect its offsets to catch up to the latest offsets
        connect.resumeConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(
                CONNECTOR_NAME,
                NUM_TASKS,
                "Connector tasks did not resume in time"
        );
        verifyExpectedSinkConnectorOffsets(CONNECTOR_NAME, TOPIC, numPartitions, NUM_RECORDS_PER_PARTITION,
                "Sink connector consumer group offsets should catch up to the topic end offsets");
    }

    @Test
    public void testAlterSinkConnectorOffsetsZombieSinkTasks() throws Exception {
        connect = connectBuilder.build();
        connect.start();
        connect.kafka().createTopic(TOPIC, 1);

        // Produce records
        for (int record = 0; record < NUM_RECORDS_PER_PARTITION; record++) {
            connect.kafka().produce(TOPIC, 0, "key", "value");
        }

        // Configure a sink connector whose sink task blocks in its stop method
        Map<String, String> connectorConfigs = new HashMap<>();
        connectorConfigs.put(CONNECTOR_CLASS_CONFIG, BlockingConnectorTest.BlockingSinkConnector.class.getName());
        connectorConfigs.put(TOPICS_CONFIG, TOPIC);
        connectorConfigs.put("block", "Task::stop");

        connect.configureConnector(CONNECTOR_NAME, connectorConfigs);
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, 1,
                "Connector tasks did not start in time.");

        connect.stopConnector(CONNECTOR_NAME);

        // Try to delete the offsets for the single topic partition
        Map<String, Object> partition = new HashMap<>();
        partition.put(SinkUtils.KAFKA_TOPIC_KEY, TOPIC);
        partition.put(SinkUtils.KAFKA_PARTITION_KEY, 0);
        List<ConnectorOffset> offsetsToAlter = Collections.singletonList(new ConnectorOffset(partition, null));

        ConnectRestException e = assertThrows(ConnectRestException.class,
                () -> connect.alterConnectorOffsets(CONNECTOR_NAME, new ConnectorOffsets(offsetsToAlter)));
        assertThat(e.getMessage(), containsString("zombie sink task"));
    }

    @Test
    public void testAlterSinkConnectorOffsetsInvalidRequestBody() throws Exception {
        connect = connectBuilder.build();
        connect.start();
        // Create a sink connector and stop it
        connect.configureConnector(CONNECTOR_NAME, baseSinkConnectorConfigs());
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, NUM_TASKS,
                "Connector tasks did not start in time.");
        connect.stopConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorIsStopped(
                CONNECTOR_NAME,
                "Connector did not stop in time"
        );
        String url = connect.endpointForResource(String.format("connectors/%s/offsets", CONNECTOR_NAME));

        String content = "{}";
        try (Response response = connect.requestPatch(url, content)) {
            assertEquals(400, response.getStatus());
            assertThat(response.getEntity().toString(), containsString("Partitions / offsets need to be provided for an alter offsets request"));
        }

        content = "{\"offsets\": []}";
        try (Response response = connect.requestPatch(url, content)) {
            assertEquals(400, response.getStatus());
            assertThat(response.getEntity().toString(), containsString("Partitions / offsets need to be provided for an alter offsets request"));
        }

        content = "{\"offsets\": [{}]}";
        try (Response response = connect.requestPatch(url, content)) {
            assertEquals(400, response.getStatus());
            assertThat(response.getEntity().toString(), containsString("The partition for a sink connector offset cannot be null or missing"));
        }

        content = "{\"offsets\": [{\"partition\": null, \"offset\": null}]}";
        try (Response response = connect.requestPatch(url, content)) {
            assertEquals(400, response.getStatus());
            assertThat(response.getEntity().toString(), containsString("The partition for a sink connector offset cannot be null or missing"));
        }

        content = "{\"offsets\": [{\"partition\": {}, \"offset\": null}]}";
        try (Response response = connect.requestPatch(url, content)) {
            assertEquals(400, response.getStatus());
            assertThat(response.getEntity().toString(), containsString("The partition for a sink connector offset must contain the keys 'kafka_topic' and 'kafka_partition'"));
        }

        content = "{\"offsets\": [{\"partition\": {\"kafka_topic\": \"test\", \"kafka_partition\": \"not a number\"}, \"offset\": null}]}";
        try (Response response = connect.requestPatch(url, content)) {
            assertEquals(400, response.getStatus());
            assertThat(response.getEntity().toString(), containsString("Partition values for sink connectors need to be integers"));
        }

        content = "{\"offsets\": [{\"partition\": {\"kafka_topic\": \"test\", \"kafka_partition\": 1}, \"offset\": {}}]}";
        try (Response response = connect.requestPatch(url, content)) {
            assertEquals(400, response.getStatus());
            assertThat(response.getEntity().toString(), containsString("The offset for a sink connector should either be null or contain the key 'kafka_offset'"));
        }

        content = "{\"offsets\": [{\"partition\": {\"kafka_topic\": \"test\", \"kafka_partition\": 1}, \"offset\": {\"kafka_offset\": \"not a number\"}}]}";
        try (Response response = connect.requestPatch(url, content)) {
            assertEquals(400, response.getStatus());
            assertThat(response.getEntity().toString(), containsString("Offset values for sink connectors need to be integers"));
        }
    }

    @Test
    public void testAlterSourceConnectorOffsets() throws Exception {
        connect = connectBuilder.build();
        connect.start();
        alterAndVerifySourceConnectorOffsets(baseSourceConnectorConfigs());
    }

    @Test
    public void testAlterSourceConnectorOffsetsCustomOffsetsTopic() throws Exception {
        connect = connectBuilder.build();
        connect.start();
        Map<String, String> connectorConfigs = baseSourceConnectorConfigs();
        connectorConfigs.put(SourceConnectorConfig.OFFSETS_TOPIC_CONFIG, "custom-offsets-topic");
        alterAndVerifySourceConnectorOffsets(connectorConfigs);
    }

    @Test
    public void testAlterSourceConnectorOffsetsDifferentKafkaClusterTargeted() throws Exception {
        connect = connectBuilder.build();
        connect.start();
        EmbeddedKafkaCluster kafkaCluster = new EmbeddedKafkaCluster(1, new Properties());

        try (AutoCloseable ignored = kafkaCluster::stop) {
            kafkaCluster.start();

            Map<String, String> connectorConfigs = baseSourceConnectorConfigs();
            connectorConfigs.put(ConnectorConfig.CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                    kafkaCluster.bootstrapServers());
            connectorConfigs.put(ConnectorConfig.CONNECTOR_CLIENT_ADMIN_OVERRIDES_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                    kafkaCluster.bootstrapServers());

            alterAndVerifySourceConnectorOffsets(connectorConfigs);
        }
    }

    @Test
    public void testAlterSourceConnectorOffsetsExactlyOnceSupportEnabled() throws Exception {
        workerProps.put(DistributedConfig.EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG, "enabled");
        connect = connectBuilder.workerProps(workerProps).build();
        connect.start();

        alterAndVerifySourceConnectorOffsets(baseSourceConnectorConfigs());
    }

    public void alterAndVerifySourceConnectorOffsets(Map<String, String> connectorConfigs) throws Exception {
        // Create source connector
        connect.configureConnector(CONNECTOR_NAME, connectorConfigs);
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, NUM_TASKS,
                "Connector tasks did not start in time.");

        verifyExpectedSourceConnectorOffsets(CONNECTOR_NAME, NUM_TASKS, NUM_RECORDS_PER_PARTITION,
                "Source connector offsets should reflect the expected number of records produced");

        connect.stopConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorIsStopped(
                CONNECTOR_NAME,
                "Connector did not stop in time"
        );

        List<ConnectorOffset> offsetsToAlter = new ArrayList<>();
        // The MonitorableSourceConnector has a source partition per task
        for (int i = 0; i < NUM_TASKS; i++) {
            offsetsToAlter.add(
                    new ConnectorOffset(Collections.singletonMap("task.id", CONNECTOR_NAME + "-" + i),
                            Collections.singletonMap("saved", 5))
            );
        }

        String response = connect.alterConnectorOffsets(CONNECTOR_NAME, new ConnectorOffsets(offsetsToAlter));
        assertThat(response, containsString("The Connect framework-managed offsets for this connector have been altered successfully. " +
                "However, if this connector manages offsets externally, they will need to be manually altered in the system that the connector uses."));

        verifyExpectedSourceConnectorOffsets(CONNECTOR_NAME, NUM_TASKS, 5,
                "Source connector offsets should reflect the altered offsets");

        // Update the connector's configs; this time expect SourceConnector::alterOffsets to return true
        connectorConfigs.put(MonitorableSourceConnector.ALTER_OFFSETS_RESULT, "true");
        connect.configureConnector(CONNECTOR_NAME, connectorConfigs);

        // Alter offsets again while connector is in stopped state
        offsetsToAlter = new ArrayList<>();
        // The MonitorableSourceConnector has a source partition per task
        for (int i = 0; i < NUM_TASKS; i++) {
            offsetsToAlter.add(
                    new ConnectorOffset(Collections.singletonMap("task.id", CONNECTOR_NAME + "-" + i),
                            Collections.singletonMap("saved", 7))
            );
        }

        response = connect.alterConnectorOffsets(CONNECTOR_NAME, new ConnectorOffsets(offsetsToAlter));
        assertThat(response, containsString("The offsets for this connector have been altered successfully"));

        verifyExpectedSourceConnectorOffsets(CONNECTOR_NAME, NUM_TASKS, 7,
                "Source connector offsets should reflect the altered offsets");

        // Resume the connector and expect its offsets to catch up to the latest offsets
        connect.resumeConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(
                CONNECTOR_NAME,
                NUM_TASKS,
                "Connector tasks did not resume in time"
        );
        verifyExpectedSourceConnectorOffsets(CONNECTOR_NAME, NUM_TASKS, NUM_RECORDS_PER_PARTITION,
                "Source connector offsets should reflect the expected number of records produced");
    }

    @Test
    public void testAlterSourceConnectorOffsetsInvalidRequestBody() throws Exception {
        connect = connectBuilder.build();
        connect.start();
        // Create a source connector and stop it
        connect.configureConnector(CONNECTOR_NAME, baseSourceConnectorConfigs());
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, NUM_TASKS,
                "Connector tasks did not start in time.");
        connect.stopConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorIsStopped(
                CONNECTOR_NAME,
                "Connector did not stop in time"
        );
        String url = connect.endpointForResource(String.format("connectors/%s/offsets", CONNECTOR_NAME));

        String content = "[]";
        try (Response response = connect.requestPatch(url, content)) {
            assertEquals(500, response.getStatus());
            assertThat(response.getEntity().toString(), containsString("Cannot deserialize value"));
        }

        content = "{}";
        try (Response response = connect.requestPatch(url, content)) {
            assertEquals(400, response.getStatus());
            assertThat(response.getEntity().toString(), containsString("Partitions / offsets need to be provided for an alter offsets request"));
        }

        content = "{\"key\": []}";
        try (Response response = connect.requestPatch(url, content)) {
            assertEquals(500, response.getStatus());
            assertThat(response.getEntity().toString(), containsString("Unrecognized field"));
        }

        content = "{\"offsets\": []}";
        try (Response response = connect.requestPatch(url, content)) {
            assertEquals(400, response.getStatus());
            assertThat(response.getEntity().toString(), containsString("Partitions / offsets need to be provided for an alter offsets request"));
        }

        content = "{\"offsets\": {}}";
        try (Response response = connect.requestPatch(url, content)) {
            assertEquals(500, response.getStatus());
            assertThat(response.getEntity().toString(), containsString("Cannot deserialize value"));
        }

        content = "{\"offsets\": [123]}";
        try (Response response = connect.requestPatch(url, content)) {
            assertEquals(500, response.getStatus());
            assertThat(response.getEntity().toString(), containsString("Cannot construct instance"));
        }

        content = "{\"offsets\": [{\"key\": \"val\"}]}";
        try (Response response = connect.requestPatch(url, content)) {
            assertEquals(500, response.getStatus());
            assertThat(response.getEntity().toString(), containsString("Unrecognized field"));
        }

        content = "{\"offsets\": [{\"partition\": []]}]}";
        try (Response response = connect.requestPatch(url, content)) {
            assertEquals(500, response.getStatus());
            assertThat(response.getEntity().toString(), containsString("Cannot deserialize value"));
        }
    }

    @Test
    public void testResetSinkConnectorOffsets() throws Exception {
        connect = connectBuilder.build();
        connect.start();
        resetAndVerifySinkConnectorOffsets(baseSinkConnectorConfigs(), connect.kafka());
    }

    @Test
    public void testResetSinkConnectorOffsetsOverriddenConsumerGroupId() throws Exception {
        connect = connectBuilder.build();
        connect.start();
        Map<String, String> connectorConfigs = baseSinkConnectorConfigs();
        connectorConfigs.put(ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX + CommonClientConfigs.GROUP_ID_CONFIG,
                "overridden-group-id");
        resetAndVerifySinkConnectorOffsets(connectorConfigs, connect.kafka());
        // Ensure that the overridden consumer group ID was the one actually used
        try (Admin admin = connect.kafka().createAdminClient()) {
            Collection<ConsumerGroupListing> consumerGroups = admin.listConsumerGroups().all().get();
            assertTrue(consumerGroups.stream().anyMatch(consumerGroupListing -> "overridden-group-id".equals(consumerGroupListing.groupId())));
            assertTrue(consumerGroups.stream().noneMatch(consumerGroupListing -> SinkUtils.consumerGroupId(CONNECTOR_NAME).equals(consumerGroupListing.groupId())));
        }
    }

    @Test
    public void testResetSinkConnectorOffsetsDifferentKafkaClusterTargeted() throws Exception {
        connect = connectBuilder.build();
        connect.start();
        EmbeddedKafkaCluster kafkaCluster = new EmbeddedKafkaCluster(1, new Properties());

        try (AutoCloseable ignored = kafkaCluster::stop) {
            kafkaCluster.start();

            Map<String, String> connectorConfigs = baseSinkConnectorConfigs();
            connectorConfigs.put(ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                    kafkaCluster.bootstrapServers());
            connectorConfigs.put(ConnectorConfig.CONNECTOR_CLIENT_ADMIN_OVERRIDES_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                    kafkaCluster.bootstrapServers());

            resetAndVerifySinkConnectorOffsets(connectorConfigs, kafkaCluster);
        }
    }

    private void resetAndVerifySinkConnectorOffsets(Map<String, String> connectorConfigs, EmbeddedKafkaCluster kafkaCluster) throws Exception {
        int numPartitions = 3;
        kafkaCluster.createTopic(TOPIC, numPartitions);

        // Produce records to each partition
        for (int partition = 0; partition < numPartitions; partition++) {
            for (int record = 0; record < NUM_RECORDS_PER_PARTITION; record++) {
                kafkaCluster.produce(TOPIC, partition, "key", "value");
            }
        }
        // Create sink connector
        connect.configureConnector(CONNECTOR_NAME, connectorConfigs);
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, NUM_TASKS,
                "Connector tasks did not start in time.");

        verifyExpectedSinkConnectorOffsets(CONNECTOR_NAME, TOPIC, numPartitions, NUM_RECORDS_PER_PARTITION,
                "Sink connector consumer group offsets should catch up to the topic end offsets");

        connect.stopConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorIsStopped(
                CONNECTOR_NAME,
                "Connector did not stop in time"
        );

        // Reset the sink connector's offsets
        String response = connect.resetConnectorOffsets(CONNECTOR_NAME);
        assertThat(response, containsString("The Connect framework-managed offsets for this connector have been reset successfully. " +
                "However, if this connector manages offsets externally, they will need to be manually reset in the system that the connector uses."));

        verifyEmptyConnectorOffsets(CONNECTOR_NAME);

        // Reset the sink connector's offsets again while it is still in a STOPPED state and ensure that there is no error
        response = connect.resetConnectorOffsets(CONNECTOR_NAME);
        assertThat(response, containsString("The Connect framework-managed offsets for this connector have been reset successfully. " +
                "However, if this connector manages offsets externally, they will need to be manually reset in the system that the connector uses."));

        verifyEmptyConnectorOffsets(CONNECTOR_NAME);

        // Resume the connector and expect its offsets to catch up to the latest offsets
        connect.resumeConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(
                CONNECTOR_NAME,
                NUM_TASKS,
                "Connector tasks did not resume in time"
        );
        verifyExpectedSinkConnectorOffsets(CONNECTOR_NAME, TOPIC, numPartitions, NUM_RECORDS_PER_PARTITION,
                "Sink connector consumer group offsets should catch up to the topic end offsets");
    }

    @Test
    public void testResetSinkConnectorOffsetsZombieSinkTasks() throws Exception {
        connect = connectBuilder.build();
        connect.start();
        connect.kafka().createTopic(TOPIC, 1);

        // Produce records
        for (int record = 0; record < NUM_RECORDS_PER_PARTITION; record++) {
            connect.kafka().produce(TOPIC, 0, "key", "value");
        }

        // Configure a sink connector whose sink task blocks in its stop method
        Map<String, String> connectorConfigs = new HashMap<>();
        connectorConfigs.put(CONNECTOR_CLASS_CONFIG, BlockingConnectorTest.BlockingSinkConnector.class.getName());
        connectorConfigs.put(TOPICS_CONFIG, TOPIC);
        connectorConfigs.put("block", "Task::stop");

        connect.configureConnector(CONNECTOR_NAME, connectorConfigs);
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, 1,
                "Connector tasks did not start in time.");

        verifyExpectedSinkConnectorOffsets(CONNECTOR_NAME, TOPIC, 1, NUM_RECORDS_PER_PARTITION,
                "Sink connector consumer group offsets should catch up to the topic end offsets");

        connect.stopConnector(CONNECTOR_NAME);

        // Try to reset the offsets
        ConnectRestException e = assertThrows(ConnectRestException.class, () -> connect.resetConnectorOffsets(CONNECTOR_NAME));
        assertThat(e.getMessage(), containsString("zombie sink task"));
    }

    @Test
    public void testResetSourceConnectorOffsets() throws Exception {
        connect = connectBuilder.build();
        connect.start();
        resetAndVerifySourceConnectorOffsets(baseSourceConnectorConfigs());
    }

    @Test
    public void testResetSourceConnectorOffsetsCustomOffsetsTopic() throws Exception {
        connect = connectBuilder.build();
        connect.start();
        Map<String, String> connectorConfigs = baseSourceConnectorConfigs();
        connectorConfigs.put(SourceConnectorConfig.OFFSETS_TOPIC_CONFIG, "custom-offsets-topic");
        resetAndVerifySourceConnectorOffsets(connectorConfigs);
    }

    @Test
    public void testResetSourceConnectorOffsetsDifferentKafkaClusterTargeted() throws Exception {
        connect = connectBuilder.build();
        connect.start();
        EmbeddedKafkaCluster kafkaCluster = new EmbeddedKafkaCluster(1, new Properties());

        try (AutoCloseable ignored = kafkaCluster::stop) {
            kafkaCluster.start();

            Map<String, String> connectorConfigs = baseSourceConnectorConfigs();
            connectorConfigs.put(ConnectorConfig.CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                    kafkaCluster.bootstrapServers());
            connectorConfigs.put(ConnectorConfig.CONNECTOR_CLIENT_ADMIN_OVERRIDES_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                    kafkaCluster.bootstrapServers());

            resetAndVerifySourceConnectorOffsets(connectorConfigs);
        }
    }

    @Test
    public void testResetSourceConnectorOffsetsExactlyOnceSupportEnabled() throws Exception {
        workerProps.put(DistributedConfig.EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG, "enabled");
        connect = connectBuilder.workerProps(workerProps).build();
        connect.start();

        resetAndVerifySourceConnectorOffsets(baseSourceConnectorConfigs());
    }

    public void resetAndVerifySourceConnectorOffsets(Map<String, String> connectorConfigs) throws Exception {
        // Create source connector
        connect.configureConnector(CONNECTOR_NAME, connectorConfigs);
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, NUM_TASKS,
                "Connector tasks did not start in time.");

        verifyExpectedSourceConnectorOffsets(CONNECTOR_NAME, NUM_TASKS, NUM_RECORDS_PER_PARTITION,
                "Source connector offsets should reflect the expected number of records produced");

        connect.stopConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorIsStopped(
                CONNECTOR_NAME,
                "Connector did not stop in time"
        );

        // Reset the source connector's offsets
        String response = connect.resetConnectorOffsets(CONNECTOR_NAME);
        assertThat(response, containsString("The Connect framework-managed offsets for this connector have been reset successfully. " +
                "However, if this connector manages offsets externally, they will need to be manually reset in the system that the connector uses."));

        verifyEmptyConnectorOffsets(CONNECTOR_NAME);

        // Reset the source connector's offsets again while it is still in a STOPPED state and ensure that there is no error
        response = connect.resetConnectorOffsets(CONNECTOR_NAME);
        assertThat(response, containsString("The Connect framework-managed offsets for this connector have been reset successfully. " +
                "However, if this connector manages offsets externally, they will need to be manually reset in the system that the connector uses."));

        verifyEmptyConnectorOffsets(CONNECTOR_NAME);

        // Resume the connector and expect its offsets to catch up to the latest offsets
        connect.resumeConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(
                CONNECTOR_NAME,
                NUM_TASKS,
                "Connector tasks did not resume in time"
        );
        verifyExpectedSourceConnectorOffsets(CONNECTOR_NAME, NUM_TASKS, NUM_RECORDS_PER_PARTITION,
                "Source connector offsets should reflect the expected number of records produced");
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
        props.put(MonitorableSourceConnector.MAX_MESSAGES_PRODUCED_CONFIG, String.valueOf(NUM_RECORDS_PER_PARTITION));
        props.put(ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + REPLICATION_FACTOR_CONFIG, "1");
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + PARTITIONS_CONFIG, "1");
        return props;
    }

    /**
     * Verify whether the actual consumer group offsets for a sink connector match the expected offsets. The verification
     * is done using the <strong><em>GET /connectors/{connector}/offsets</em></strong> REST API which is repeatedly queried
     * until the offsets match or the {@link #OFFSET_READ_TIMEOUT_MS timeout} is reached. Note that this assumes the following:
     * <ol>
     *     <li>The sink connector is consuming from a single Kafka topic</li>
     *     <li>The expected offset for each partition in the topic is the same</li>
     * </ol>
     *
     * @param connectorName the name of the sink connector whose offsets are to be verified
     * @param expectedTopic the name of the Kafka topic that the sink connector is consuming from
     * @param expectedPartitions the number of partitions that exist for the Kafka topic
     * @param expectedOffset the expected consumer group offset for each partition
     * @param conditionDetails the condition that we're waiting to achieve (for example: Sink connector should process
     *                         10 records)
     * @throws InterruptedException if the thread is interrupted while waiting for the actual offsets to match the expected offsets
     */
    private void verifyExpectedSinkConnectorOffsets(String connectorName, String expectedTopic, int expectedPartitions,
                                                    int expectedOffset, String conditionDetails) throws InterruptedException {
        TestUtils.waitForCondition(() -> {
            ConnectorOffsets offsets = connect.connectorOffsets(connectorName);
            if (offsets.offsets().size() != expectedPartitions) {
                return false;
            }
            for (ConnectorOffset offset: offsets.offsets()) {
                assertEquals(expectedTopic, offset.partition().get(SinkUtils.KAFKA_TOPIC_KEY));
                if ((Integer) offset.offset().get(SinkUtils.KAFKA_OFFSET_KEY) != expectedOffset) {
                    return false;
                }
            }
            return true;
        }, OFFSET_READ_TIMEOUT_MS, conditionDetails);
    }

    /**
     * Verify whether the actual offsets for a source connector match the expected offsets. The verification is done using the
     * <strong><em>GET /connectors/{connector}/offsets</em></strong> REST API which is repeatedly queried until the offsets match
     * or the {@link #OFFSET_READ_TIMEOUT_MS timeout} is reached. Note that this assumes that the source connector is a
     * {@link MonitorableSourceConnector}
     *
     * @param connectorName the name of the source connector whose offsets are to be verified
     * @param numTasks the number of tasks for the source connector
     * @param expectedOffset the expected offset for each source partition
     * @param conditionDetails the condition that we're waiting to achieve (for example: Source connector should process
     *                         10 records)
     * @throws InterruptedException if the thread is interrupted while waiting for the actual offsets to match the expected offsets
     */
    private void verifyExpectedSourceConnectorOffsets(String connectorName, int numTasks,
                                                      int expectedOffset, String conditionDetails) throws InterruptedException {
        TestUtils.waitForCondition(() -> {
            ConnectorOffsets offsets = connect.connectorOffsets(connectorName);
            // The MonitorableSourceConnector has a source partition per task
            if (offsets.offsets().size() != numTasks) {
                return false;
            }
            for (ConnectorOffset offset : offsets.offsets()) {
                assertTrue(((String) offset.partition().get("task.id")).startsWith(CONNECTOR_NAME));
                if ((Integer) offset.offset().get("saved") != expectedOffset) {
                    return false;
                }
            }
            return true;
        }, OFFSET_READ_TIMEOUT_MS, conditionDetails);
    }

    /**
     * Verify whether the <strong><em>GET /connectors/{connector}/offsets</em></strong> returns empty offsets for a source
     * or sink connector whose offsets have been reset via the <strong><em>DELETE /connectors/{connector}/offsets</em></strong>
     * REST API
     *
     * @param connectorName the name of the connector whose offsets are to be verified
     * @throws InterruptedException if the thread is interrupted while waiting for the offsets to be empty
     */
    private void verifyEmptyConnectorOffsets(String connectorName) throws InterruptedException {
        TestUtils.waitForCondition(() -> {
            ConnectorOffsets offsets = connect.connectorOffsets(connectorName);
            return offsets.offsets().isEmpty();
        }, OFFSET_READ_TIMEOUT_MS, "Connector offsets should be empty after resetting offsets");
    }
}
