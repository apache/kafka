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
import org.apache.kafka.common.test.api.Flaky;
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
import org.apache.kafka.test.NoRetryException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.ws.rs.core.Response;

import static jakarta.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
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
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for Kafka Connect's connector offset management REST APIs
 */
@Tag("integration")
public class OffsetsApiIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(OffsetsApiIntegrationTest.class);

    private static final long OFFSET_COMMIT_INTERVAL_MS = TimeUnit.SECONDS.toMillis(1);
    private static final long OFFSET_READ_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(30);
    private static final int NUM_WORKERS = 3;
    private static final int NUM_TASKS = 2;
    private static final int NUM_RECORDS_PER_PARTITION = 10;
    private static final Map<Map<String, String>, EmbeddedConnectCluster> CONNECT_CLUSTERS = new ConcurrentHashMap<>();
    private EmbeddedConnectCluster connect;
    private String connectorName;
    private String topic;

    @BeforeEach
    public void setup(TestInfo testInfo) {
        connectorName = testInfo.getTestMethod().get().getName();
        topic = testInfo.getTestMethod().get().getName();
        connect = defaultConnectCluster();
    }

    @AfterEach
    public void tearDown() {
        Set<String> remainingConnectors = new HashSet<>(connect.connectors());
        if (remainingConnectors.remove(connectorName)) {
            connect.deleteConnector(connectorName);
        }
        try {
            assertEquals(
                    Collections.emptySet(),
                    remainingConnectors,
                    "Some connectors were not properly cleaned up after this test"
            );
        } finally {
            // Make a last-ditch effort to clean up the leaked connectors
            // so as not to interfere with other test cases
            remainingConnectors.forEach(connect::deleteConnector);
        }
    }

    @AfterAll
    public static void close() {
        CONNECT_CLUSTERS.values().forEach(EmbeddedConnectCluster::stop);
        // wait for all blocked threads created while testing zombie task scenarios to finish
        BlockingConnectorTest.Block.join();
    }

    private static EmbeddedConnectCluster createOrReuseConnectWithWorkerProps(Map<String, String> workerProps) {
        return CONNECT_CLUSTERS.computeIfAbsent(workerProps, props -> {
            Properties brokerProps = new Properties();
            brokerProps.put("transaction.state.log.replication.factor", "1");
            brokerProps.put("transaction.state.log.min.isr", "1");

            // Have to declare a new map since the passed-in one may be immutable
            Map<String, String> workerPropsWithDefaults = new HashMap<>(workerProps);
            // Enable fast offset commits by default
            workerPropsWithDefaults.putIfAbsent(OFFSET_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(OFFSET_COMMIT_INTERVAL_MS));

            EmbeddedConnectCluster result = new EmbeddedConnectCluster.Builder()
                    .name("connect-cluster")
                    .numWorkers(NUM_WORKERS)
                    .brokerProps(brokerProps)
                    .workerProps(workerPropsWithDefaults)
                    .build();

            result.start();

            return result;
        });
    }

    private static EmbeddedConnectCluster defaultConnectCluster() {
        return createOrReuseConnectWithWorkerProps(Collections.emptyMap());
    }

    private static EmbeddedConnectCluster exactlyOnceSourceConnectCluster() {
        Map<String, String> workerProps = Collections.singletonMap(
                DistributedConfig.EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG,
                "enabled"
        );
        return createOrReuseConnectWithWorkerProps(workerProps);
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
        String overriddenGroupId = connectorName + "-overridden-group-id";
        connectorConfigs.put(
                ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX + CommonClientConfigs.GROUP_ID_CONFIG,
                overriddenGroupId
        );
        getAndVerifySinkConnectorOffsets(connectorConfigs, connect.kafka());

        // Ensure that the overridden consumer group ID was the one actually used
        try (Admin admin = connect.kafka().createAdminClient()) {
            Collection<ConsumerGroupListing> consumerGroups = admin.listConsumerGroups().all().get();
            assertTrue(consumerGroups.stream().anyMatch(consumerGroupListing -> overriddenGroupId.equals(consumerGroupListing.groupId())));
            assertTrue(consumerGroups.stream().noneMatch(consumerGroupListing -> SinkUtils.consumerGroupId(connectorName).equals(consumerGroupListing.groupId())));
        }
    }

    @Flaky("KAFKA-14956")
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
        kafkaCluster.createTopic(topic, 5);

        // Produce records to each partition
        for (int partition = 0; partition < 5; partition++) {
            for (int record = 0; record < NUM_RECORDS_PER_PARTITION; record++) {
                kafkaCluster.produce(topic, partition, "key", "value");
            }
        }

        // Create sink connector
        connect.configureConnector(connectorName, connectorConfigs);
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(connectorName, NUM_TASKS,
                "Connector tasks did not start in time.");

        verifyExpectedSinkConnectorOffsets(connectorName, topic, 5, NUM_RECORDS_PER_PARTITION,
                "Sink connector consumer group offsets should catch up to the topic end offsets");

        // Produce more records to each partition
        for (int partition = 0; partition < 5; partition++) {
            for (int record = 0; record < NUM_RECORDS_PER_PARTITION; record++) {
                kafkaCluster.produce(topic, partition, "key", "value");
            }
        }

        verifyExpectedSinkConnectorOffsets(connectorName, topic, 5, 2 * NUM_RECORDS_PER_PARTITION,
                "Sink connector consumer group offsets should catch up to the topic end offsets");
    }

    @Test
    public void testGetSourceConnectorOffsets() throws Exception {
        getAndVerifySourceConnectorOffsets(baseSourceConnectorConfigs());
    }

    @Test
    public void testGetSourceConnectorOffsetsCustomOffsetsTopic() throws Exception {
        Map<String, String> connectorConfigs = baseSourceConnectorConfigs();
        String connectorOffsetsTopic = connectorName + "-custom-offsets-topic";
        connectorConfigs.put(SourceConnectorConfig.OFFSETS_TOPIC_CONFIG, connectorOffsetsTopic);
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
        connect.configureConnector(connectorName, connectorConfigs);
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(connectorName, NUM_TASKS,
                "Connector tasks did not start in time.");

        verifyExpectedSourceConnectorOffsets(connectorName, NUM_TASKS, NUM_RECORDS_PER_PARTITION,
                "Source connector offsets should reflect the expected number of records produced");

        // Each task should produce more records
        connectorConfigs.put(MonitorableSourceConnector.MAX_MESSAGES_PRODUCED_CONFIG, String.valueOf(2 * NUM_RECORDS_PER_PARTITION));
        connect.configureConnector(connectorName, connectorConfigs);

        verifyExpectedSourceConnectorOffsets(connectorName, NUM_TASKS, 2 * NUM_RECORDS_PER_PARTITION,
                "Source connector offsets should reflect the expected number of records produced");
    }

    @Test
    public void testAlterOffsetsNonExistentConnector() {
        ConnectRestException e = assertThrows(ConnectRestException.class,
                () -> connect.alterConnectorOffsets("non-existent-connector", new ConnectorOffsets(Collections.singletonList(
                        new ConnectorOffset(Collections.emptyMap(), Collections.emptyMap())))));
        assertEquals(404, e.errorCode());
    }

    @Test
    public void testAlterOffsetsNonStoppedConnector() throws Exception {
        // Create source connector
        connect.configureConnector(connectorName, baseSourceConnectorConfigs());
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(connectorName, NUM_TASKS,
                "Connector tasks did not start in time.");

        List<ConnectorOffset> offsets = new ArrayList<>();
        // The MonitorableSourceConnector has a source partition per task
        for (int i = 0; i < NUM_TASKS; i++) {
            offsets.add(
                    new ConnectorOffset(Collections.singletonMap("task.id", connectorName + "-" + i),
                            Collections.singletonMap("saved", 5))
            );
        }

        // Try altering offsets for a running connector
        ConnectRestException e = assertThrows(ConnectRestException.class,
                () -> connect.alterConnectorOffsets(connectorName, new ConnectorOffsets(offsets)));
        assertEquals(400, e.errorCode());

        connect.pauseConnector(connectorName);
        connect.assertions().assertConnectorAndExactlyNumTasksArePaused(
                connectorName,
                NUM_TASKS,
                "Connector did not pause in time"
        );

        // Try altering offsets for a paused (not stopped) connector
        e = assertThrows(ConnectRestException.class,
                () -> connect.alterConnectorOffsets(connectorName, new ConnectorOffsets(offsets)));
        assertEquals(400, e.errorCode());
    }

    @Test
    public void testAlterSinkConnectorOffsets() throws Exception {
        alterAndVerifySinkConnectorOffsets(baseSinkConnectorConfigs(), connect.kafka());
    }

    @Flaky("KAFKA-15914")
    @Test
    public void testAlterSinkConnectorOffsetsOverriddenConsumerGroupId() throws Exception {
        Map<String, String> connectorConfigs = baseSinkConnectorConfigs();
        String overriddenGroupId = connectorName + "-overridden-group-id";
        connectorConfigs.put(
                ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX + CommonClientConfigs.GROUP_ID_CONFIG,
                overriddenGroupId
        );
        alterAndVerifySinkConnectorOffsets(connectorConfigs, connect.kafka());
        // Ensure that the overridden consumer group ID was the one actually used
        try (Admin admin = connect.kafka().createAdminClient()) {
            Collection<ConsumerGroupListing> consumerGroups = admin.listConsumerGroups().all().get();
            assertTrue(consumerGroups.stream().anyMatch(consumerGroupListing -> overriddenGroupId.equals(consumerGroupListing.groupId())));
            assertTrue(consumerGroups.stream().noneMatch(consumerGroupListing -> SinkUtils.consumerGroupId(connectorName).equals(consumerGroupListing.groupId())));
        }
    }

    @Flaky("KAFKA-16492")
    @Test
    public void testAlterSinkConnectorOffsetsDifferentKafkaClusterTargeted() throws Exception {
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
        kafkaCluster.createTopic(topic, numPartitions);

        // Produce records to each partition
        for (int partition = 0; partition < numPartitions; partition++) {
            for (int record = 0; record < NUM_RECORDS_PER_PARTITION; record++) {
                kafkaCluster.produce(topic, partition, "key", "value");
            }
        }
        // Create sink connector
        connect.configureConnector(connectorName, connectorConfigs);
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(connectorName, NUM_TASKS,
                "Connector tasks did not start in time.");

        verifyExpectedSinkConnectorOffsets(connectorName, topic, numPartitions, NUM_RECORDS_PER_PARTITION,
                "Sink connector consumer group offsets should catch up to the topic end offsets");

        connect.stopConnector(connectorName);
        connect.assertions().assertConnectorIsStopped(
                connectorName,
                "Connector did not stop in time"
        );

        // Delete the offset of one partition; alter the offsets of the others
        List<ConnectorOffset> offsetsToAlter = new ArrayList<>();
        Map<String, Object> partition = new HashMap<>();
        partition.put(SinkUtils.KAFKA_TOPIC_KEY, topic);
        partition.put(SinkUtils.KAFKA_PARTITION_KEY, 0);
        offsetsToAlter.add(new ConnectorOffset(partition, null));

        for (int i = 1; i < numPartitions; i++) {
            partition = new HashMap<>();
            partition.put(SinkUtils.KAFKA_TOPIC_KEY, topic);
            partition.put(SinkUtils.KAFKA_PARTITION_KEY, i);
            offsetsToAlter.add(new ConnectorOffset(partition, Collections.singletonMap(SinkUtils.KAFKA_OFFSET_KEY, 5)));
        }

        // Alter the sink connector's offsets, with retry logic (since we just stopped the connector)
        String response = modifySinkConnectorOffsetsWithRetry(new ConnectorOffsets(offsetsToAlter));

        assertTrue(response.contains("The Connect framework-managed offsets for this connector have been altered successfully. " +
                "However, if this connector manages offsets externally, they will need to be manually altered in the system that the connector uses."));

        verifyExpectedSinkConnectorOffsets(connectorName, topic, numPartitions - 1, 5,
                "Sink connector consumer group offsets should reflect the altered offsets");

        // Update the connector's configs; this time expect SinkConnector::alterOffsets to return true
        connectorConfigs.put(MonitorableSinkConnector.ALTER_OFFSETS_RESULT, "true");
        connect.configureConnector(connectorName, connectorConfigs);

        // Alter offsets again while the connector is still in a stopped state
        offsetsToAlter.clear();
        for (int i = 1; i < numPartitions; i++) {
            partition = new HashMap<>();
            partition.put(SinkUtils.KAFKA_TOPIC_KEY, topic);
            partition.put(SinkUtils.KAFKA_PARTITION_KEY, i);
            offsetsToAlter.add(new ConnectorOffset(partition, Collections.singletonMap(SinkUtils.KAFKA_OFFSET_KEY, 3)));
        }

        response = connect.alterConnectorOffsets(connectorName, new ConnectorOffsets(offsetsToAlter));
        assertTrue(response.contains("The offsets for this connector have been altered successfully"));

        verifyExpectedSinkConnectorOffsets(connectorName, topic, numPartitions - 1, 3,
                "Sink connector consumer group offsets should reflect the altered offsets");

        // Resume the connector and expect its offsets to catch up to the latest offsets
        connect.resumeConnector(connectorName);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(
                connectorName,
                NUM_TASKS,
                "Connector tasks did not resume in time"
        );
        verifyExpectedSinkConnectorOffsets(connectorName, topic, numPartitions, NUM_RECORDS_PER_PARTITION,
                "Sink connector consumer group offsets should catch up to the topic end offsets");
    }

    @Test
    public void testAlterSinkConnectorOffsetsZombieSinkTasks() throws Exception {
        connect.kafka().createTopic(topic, 1);

        // Produce records
        for (int record = 0; record < NUM_RECORDS_PER_PARTITION; record++) {
            connect.kafka().produce(topic, 0, "key", "value");
        }

        // Configure a sink connector whose sink task blocks in its stop method
        Map<String, String> connectorConfigs = new HashMap<>();
        connectorConfigs.put(CONNECTOR_CLASS_CONFIG, BlockingConnectorTest.BlockingSinkConnector.class.getName());
        connectorConfigs.put(TOPICS_CONFIG, topic);
        connectorConfigs.put("block", "Task::stop");

        connect.configureConnector(connectorName, connectorConfigs);
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(connectorName, 1,
                "Connector tasks did not start in time.");

        // Make sure the tasks' consumers have had a chance to actually form a group
        // (otherwise, the reset request will succeed because there won't be any active consumers)
        verifyExpectedSinkConnectorOffsets(connectorName, topic, 1, NUM_RECORDS_PER_PARTITION,
                "Sink connector consumer group offsets should catch up to the topic end offsets");

        connect.stopConnector(connectorName);

        // Try to delete the offsets for the single topic partition
        Map<String, Object> partition = new HashMap<>();
        partition.put(SinkUtils.KAFKA_TOPIC_KEY, topic);
        partition.put(SinkUtils.KAFKA_PARTITION_KEY, 0);
        List<ConnectorOffset> offsetsToAlter = Collections.singletonList(new ConnectorOffset(partition, null));

        ConnectRestException e = assertThrows(ConnectRestException.class,
                () -> connect.alterConnectorOffsets(connectorName, new ConnectorOffsets(offsetsToAlter)));
        assertTrue(e.getMessage().contains("zombie sink task"));

        // clean up blocked threads created while testing zombie task scenarios
        BlockingConnectorTest.Block.reset();
    }

    @Test
    public void testAlterSinkConnectorOffsetsInvalidRequestBody() throws Exception {
        // Create a sink connector and stop it
        connect.configureConnector(connectorName, baseSinkConnectorConfigs());
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(connectorName, NUM_TASKS,
                "Connector tasks did not start in time.");
        connect.stopConnector(connectorName);
        connect.assertions().assertConnectorIsStopped(
                connectorName,
                "Connector did not stop in time"
        );
        String url = connect.endpointForResource(String.format("connectors/%s/offsets", connectorName));

        String content = "{}";
        try (Response response = connect.requestPatch(url, content)) {
            assertEquals(400, response.getStatus());
            assertTrue(response.getEntity().toString().contains("Partitions / offsets need to be provided for an alter offsets request"));
        }

        content = "{\"offsets\": []}";
        try (Response response = connect.requestPatch(url, content)) {
            assertEquals(400, response.getStatus());
            assertTrue(response.getEntity().toString().contains("Partitions / offsets need to be provided for an alter offsets request"));
        }

        content = "{\"offsets\": [{}]}";
        try (Response response = connect.requestPatch(url, content)) {
            assertEquals(400, response.getStatus());
            assertTrue(response.getEntity().toString().contains("The partition for a sink connector offset cannot be null or missing"));
        }

        content = "{\"offsets\": [{\"partition\": null, \"offset\": null}]}";
        try (Response response = connect.requestPatch(url, content)) {
            assertEquals(400, response.getStatus());
            assertTrue(response.getEntity().toString().contains("The partition for a sink connector offset cannot be null or missing"));
        }

        content = "{\"offsets\": [{\"partition\": {}, \"offset\": null}]}";
        try (Response response = connect.requestPatch(url, content)) {
            assertEquals(400, response.getStatus());
            assertTrue(response.getEntity().toString().contains("The partition for a sink connector offset must contain the keys 'kafka_topic' and 'kafka_partition'"));
        }

        content = "{\"offsets\": [{\"partition\": {\"kafka_topic\": \"test\", \"kafka_partition\": \"not a number\"}, \"offset\": null}]}";
        try (Response response = connect.requestPatch(url, content)) {
            assertEquals(400, response.getStatus());
            assertTrue(response.getEntity().toString().contains("Partition values for sink connectors need to be integers"));
        }

        content = "{\"offsets\": [{\"partition\": {\"kafka_topic\": \"test\", \"kafka_partition\": 1}, \"offset\": {}}]}";
        try (Response response = connect.requestPatch(url, content)) {
            assertEquals(400, response.getStatus());
            assertTrue(response.getEntity().toString().contains("The offset for a sink connector should either be null or contain the key 'kafka_offset'"));
        }

        content = "{\"offsets\": [{\"partition\": {\"kafka_topic\": \"test\", \"kafka_partition\": 1}, \"offset\": {\"kafka_offset\": \"not a number\"}}]}";
        try (Response response = connect.requestPatch(url, content)) {
            assertEquals(400, response.getStatus());
            assertTrue(response.getEntity().toString().contains("Offset values for sink connectors need to be integers"));
        }
    }

    @Test
    public void testAlterSourceConnectorOffsets() throws Exception {
        alterAndVerifySourceConnectorOffsets(baseSourceConnectorConfigs());
    }

    @Test
    public void testAlterSourceConnectorOffsetsCustomOffsetsTopic() throws Exception {
        Map<String, String> connectorConfigs = baseSourceConnectorConfigs();
        connectorConfigs.put(SourceConnectorConfig.OFFSETS_TOPIC_CONFIG, "custom-offsets-topic");
        alterAndVerifySourceConnectorOffsets(connectorConfigs);
    }

    @Test
    public void testAlterSourceConnectorOffsetsDifferentKafkaClusterTargeted() throws Exception {
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
        connect = exactlyOnceSourceConnectCluster();

        alterAndVerifySourceConnectorOffsets(baseSourceConnectorConfigs());
    }

    public void alterAndVerifySourceConnectorOffsets(Map<String, String> connectorConfigs) throws Exception {
        // Create source connector
        connect.configureConnector(connectorName, connectorConfigs);
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(connectorName, NUM_TASKS,
                "Connector tasks did not start in time.");

        verifyExpectedSourceConnectorOffsets(connectorName, NUM_TASKS, NUM_RECORDS_PER_PARTITION,
                "Source connector offsets should reflect the expected number of records produced");

        connect.stopConnector(connectorName);
        connect.assertions().assertConnectorIsStopped(
                connectorName,
                "Connector did not stop in time"
        );

        List<ConnectorOffset> offsetsToAlter = new ArrayList<>();
        // The MonitorableSourceConnector has a source partition per task
        for (int i = 0; i < NUM_TASKS; i++) {
            offsetsToAlter.add(
                    new ConnectorOffset(Collections.singletonMap("task.id", connectorName + "-" + i),
                            Collections.singletonMap("saved", 5))
            );
        }

        String response = connect.alterConnectorOffsets(connectorName, new ConnectorOffsets(offsetsToAlter));
        assertTrue(response.contains("The Connect framework-managed offsets for this connector have been altered successfully. " +
                "However, if this connector manages offsets externally, they will need to be manually altered in the system that the connector uses."));

        verifyExpectedSourceConnectorOffsets(connectorName, NUM_TASKS, 5,
                "Source connector offsets should reflect the altered offsets");

        // Update the connector's configs; this time expect SourceConnector::alterOffsets to return true
        connectorConfigs.put(MonitorableSourceConnector.ALTER_OFFSETS_RESULT, "true");
        connect.configureConnector(connectorName, connectorConfigs);

        // Alter offsets again while connector is in stopped state
        offsetsToAlter = new ArrayList<>();
        // The MonitorableSourceConnector has a source partition per task
        for (int i = 0; i < NUM_TASKS; i++) {
            offsetsToAlter.add(
                    new ConnectorOffset(Collections.singletonMap("task.id", connectorName + "-" + i),
                            Collections.singletonMap("saved", 7))
            );
        }

        response = connect.alterConnectorOffsets(connectorName, new ConnectorOffsets(offsetsToAlter));
        assertTrue(response.contains("The offsets for this connector have been altered successfully"));

        verifyExpectedSourceConnectorOffsets(connectorName, NUM_TASKS, 7,
                "Source connector offsets should reflect the altered offsets");

        // Resume the connector and expect its offsets to catch up to the latest offsets
        connect.resumeConnector(connectorName);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(
                connectorName,
                NUM_TASKS,
                "Connector tasks did not resume in time"
        );
        verifyExpectedSourceConnectorOffsets(connectorName, NUM_TASKS, NUM_RECORDS_PER_PARTITION,
                "Source connector offsets should reflect the expected number of records produced");
    }

    @Test
    public void testAlterSourceConnectorOffsetsInvalidRequestBody() throws Exception {
        // Create a source connector and stop it
        connect.configureConnector(connectorName, baseSourceConnectorConfigs());
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(connectorName, NUM_TASKS,
                "Connector tasks did not start in time.");
        connect.stopConnector(connectorName);
        connect.assertions().assertConnectorIsStopped(
                connectorName,
                "Connector did not stop in time"
        );
        String url = connect.endpointForResource(String.format("connectors/%s/offsets", connectorName));

        String content = "[]";
        try (Response response = connect.requestPatch(url, content)) {
            assertEquals(500, response.getStatus());
            assertTrue(response.getEntity().toString().contains("Cannot deserialize value"));
        }

        content = "{}";
        try (Response response = connect.requestPatch(url, content)) {
            assertEquals(400, response.getStatus());
            assertTrue(response.getEntity().toString().contains("Partitions / offsets need to be provided for an alter offsets request"));
        }

        content = "{\"key\": []}";
        try (Response response = connect.requestPatch(url, content)) {
            assertEquals(500, response.getStatus());
            assertTrue(response.getEntity().toString().contains("Unrecognized field"));
        }

        content = "{\"offsets\": []}";
        try (Response response = connect.requestPatch(url, content)) {
            assertEquals(400, response.getStatus());
            assertTrue(response.getEntity().toString().contains("Partitions / offsets need to be provided for an alter offsets request"));
        }

        content = "{\"offsets\": {}}";
        try (Response response = connect.requestPatch(url, content)) {
            assertEquals(500, response.getStatus());
            assertTrue(response.getEntity().toString().contains("Cannot deserialize value"));
        }

        content = "{\"offsets\": [123]}";
        try (Response response = connect.requestPatch(url, content)) {
            assertEquals(500, response.getStatus());
            assertTrue(response.getEntity().toString().contains("Cannot construct instance"));
        }

        content = "{\"offsets\": [{\"key\": \"val\"}]}";
        try (Response response = connect.requestPatch(url, content)) {
            assertEquals(500, response.getStatus());
            assertTrue(response.getEntity().toString().contains("Unrecognized field"));
        }

        content = "{\"offsets\": [{\"partition\": []]}]}";
        try (Response response = connect.requestPatch(url, content)) {
            assertEquals(500, response.getStatus());
            assertTrue(response.getEntity().toString().contains("Cannot deserialize value"));
        }
    }

    @Flaky("KAFKA-15918")
    @Test
    public void testResetSinkConnectorOffsets() throws Exception {
        resetAndVerifySinkConnectorOffsets(baseSinkConnectorConfigs(), connect.kafka());
    }

    @Flaky("KAFKA-15891")
    @Test
    public void testResetSinkConnectorOffsetsOverriddenConsumerGroupId() throws Exception {
        Map<String, String> connectorConfigs = baseSinkConnectorConfigs();
        String overriddenGroupId = connectorName + "-overridden-group-id";
        connectorConfigs.put(
                ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX + CommonClientConfigs.GROUP_ID_CONFIG,
                overriddenGroupId
        );
        resetAndVerifySinkConnectorOffsets(connectorConfigs, connect.kafka());
        // Ensure that the overridden consumer group ID was the one actually used
        try (Admin admin = connect.kafka().createAdminClient()) {
            Collection<ConsumerGroupListing> consumerGroups = admin.listConsumerGroups().all().get();
            assertTrue(consumerGroups.stream().anyMatch(consumerGroupListing -> overriddenGroupId.equals(consumerGroupListing.groupId())));
            assertTrue(consumerGroups.stream().noneMatch(consumerGroupListing -> SinkUtils.consumerGroupId(connectorName).equals(consumerGroupListing.groupId())));
        }
    }

    @Test
    public void testResetSinkConnectorOffsetsDifferentKafkaClusterTargeted() throws Exception {
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
        kafkaCluster.createTopic(topic, numPartitions);

        // Produce records to each partition
        for (int partition = 0; partition < numPartitions; partition++) {
            for (int record = 0; record < NUM_RECORDS_PER_PARTITION; record++) {
                kafkaCluster.produce(topic, partition, "key", "value");
            }
        }
        // Create sink connector
        connect.configureConnector(connectorName, connectorConfigs);
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(connectorName, NUM_TASKS,
                "Connector tasks did not start in time.");

        verifyExpectedSinkConnectorOffsets(connectorName, topic, numPartitions, NUM_RECORDS_PER_PARTITION,
                "Sink connector consumer group offsets should catch up to the topic end offsets");

        connect.stopConnector(connectorName);
        connect.assertions().assertConnectorIsStopped(
                connectorName,
                "Connector did not stop in time"
        );

        // Reset the sink connector's offsets, with retry logic (since we just stopped the connector)
        String response = modifySinkConnectorOffsetsWithRetry(null);
        assertTrue(response.contains("The Connect framework-managed offsets for this connector have been reset successfully. " +
                "However, if this connector manages offsets externally, they will need to be manually reset in the system that the connector uses."));

        verifyEmptyConnectorOffsets(connectorName);

        // Reset the sink connector's offsets again while it is still in a STOPPED state and ensure that there is no error
        response = connect.resetConnectorOffsets(connectorName);
        assertTrue(response.contains("The Connect framework-managed offsets for this connector have been reset successfully. " +
                "However, if this connector manages offsets externally, they will need to be manually reset in the system that the connector uses."));

        verifyEmptyConnectorOffsets(connectorName);

        // Resume the connector and expect its offsets to catch up to the latest offsets
        connect.resumeConnector(connectorName);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(
                connectorName,
                NUM_TASKS,
                "Connector tasks did not resume in time"
        );
        verifyExpectedSinkConnectorOffsets(connectorName, topic, numPartitions, NUM_RECORDS_PER_PARTITION,
                "Sink connector consumer group offsets should catch up to the topic end offsets");
    }

    @Test
    public void testResetSinkConnectorOffsetsZombieSinkTasks() throws Exception {
        connect.kafka().createTopic(topic, 1);

        // Produce records
        for (int record = 0; record < NUM_RECORDS_PER_PARTITION; record++) {
            connect.kafka().produce(topic, 0, "key", "value");
        }

        // Configure a sink connector whose sink task blocks in its stop method
        Map<String, String> connectorConfigs = new HashMap<>();
        connectorConfigs.put(CONNECTOR_CLASS_CONFIG, BlockingConnectorTest.BlockingSinkConnector.class.getName());
        connectorConfigs.put(TOPICS_CONFIG, topic);
        connectorConfigs.put("block", "Task::stop");

        connect.configureConnector(connectorName, connectorConfigs);
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(connectorName, 1,
                "Connector tasks did not start in time.");

        // Make sure the tasks' consumers have had a chance to actually form a group
        // (otherwise, the reset request will succeed because there won't be any active consumers)
        verifyExpectedSinkConnectorOffsets(connectorName, topic, 1, NUM_RECORDS_PER_PARTITION,
                "Sink connector consumer group offsets should catch up to the topic end offsets");

        connect.stopConnector(connectorName);

        // Try to reset the offsets
        ConnectRestException e = assertThrows(ConnectRestException.class, () -> connect.resetConnectorOffsets(connectorName));
        assertTrue(e.getMessage().contains("zombie sink task"));

        // clean up blocked threads created while testing zombie task scenarios
        BlockingConnectorTest.Block.reset();
    }

    @Test
    public void testResetSourceConnectorOffsets() throws Exception {
        resetAndVerifySourceConnectorOffsets(baseSourceConnectorConfigs());
    }

    @Test
    public void testResetSourceConnectorOffsetsCustomOffsetsTopic() throws Exception {
        Map<String, String> connectorConfigs = baseSourceConnectorConfigs();
        connectorConfigs.put(SourceConnectorConfig.OFFSETS_TOPIC_CONFIG, "custom-offsets-topic");
        resetAndVerifySourceConnectorOffsets(connectorConfigs);
    }

    @Test
    public void testResetSourceConnectorOffsetsDifferentKafkaClusterTargeted() throws Exception {
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
        connect = exactlyOnceSourceConnectCluster();

        resetAndVerifySourceConnectorOffsets(baseSourceConnectorConfigs());
    }

    public void resetAndVerifySourceConnectorOffsets(Map<String, String> connectorConfigs) throws Exception {
        // Create source connector
        connect.configureConnector(connectorName, connectorConfigs);
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(connectorName, NUM_TASKS,
                "Connector tasks did not start in time.");

        verifyExpectedSourceConnectorOffsets(connectorName, NUM_TASKS, NUM_RECORDS_PER_PARTITION,
                "Source connector offsets should reflect the expected number of records produced");

        connect.stopConnector(connectorName);
        connect.assertions().assertConnectorIsStopped(
                connectorName,
                "Connector did not stop in time"
        );

        // Reset the source connector's offsets
        String response = connect.resetConnectorOffsets(connectorName);
        assertTrue(response.contains("The Connect framework-managed offsets for this connector have been reset successfully. " +
                "However, if this connector manages offsets externally, they will need to be manually reset in the system that the connector uses."));

        verifyEmptyConnectorOffsets(connectorName);

        // Reset the source connector's offsets again while it is still in a STOPPED state and ensure that there is no error
        response = connect.resetConnectorOffsets(connectorName);
        assertTrue(response.contains("The Connect framework-managed offsets for this connector have been reset successfully. " +
                "However, if this connector manages offsets externally, they will need to be manually reset in the system that the connector uses."));

        verifyEmptyConnectorOffsets(connectorName);

        // Resume the connector and expect its offsets to catch up to the latest offsets
        connect.resumeConnector(connectorName);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(
                connectorName,
                NUM_TASKS,
                "Connector tasks did not resume in time"
        );
        verifyExpectedSourceConnectorOffsets(connectorName, NUM_TASKS, NUM_RECORDS_PER_PARTITION,
                "Source connector offsets should reflect the expected number of records produced");
    }

    private Map<String, String> baseSinkConnectorConfigs() {
        Map<String, String> configs = new HashMap<>();
        configs.put(CONNECTOR_CLASS_CONFIG, MonitorableSinkConnector.class.getSimpleName());
        configs.put(TASKS_MAX_CONFIG, String.valueOf(NUM_TASKS));
        configs.put(TOPICS_CONFIG, topic);
        configs.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        configs.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        return configs;
    }

    private Map<String, String> baseSourceConnectorConfigs() {
        Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR_CLASS_CONFIG, MonitorableSourceConnector.class.getSimpleName());
        props.put(TASKS_MAX_CONFIG, String.valueOf(NUM_TASKS));
        props.put(TOPIC_CONFIG, topic);
        props.put(MonitorableSourceConnector.MESSAGES_PER_POLL_CONFIG, "3");
        props.put(MonitorableSourceConnector.MAX_MESSAGES_PRODUCED_CONFIG, String.valueOf(NUM_RECORDS_PER_PARTITION));
        props.put(ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + REPLICATION_FACTOR_CONFIG, "1");
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + PARTITIONS_CONFIG, "1");
        return props;
    }

    /**
     * Modify (i.e., alter or reset) the offsets for a sink connector, with retry logic to
     * handle cases where laggy task shutdown may have left a consumer in the group.
     * @param offsetsToAlter the offsets to alter for the sink connector, or null if
     *                       the connector's offsets should be reset instead
     * @return the response from the REST API, if the request was successful
     * @throws InterruptedException if the thread is interrupted while waiting for a
     * request to modify the connector's offsets to succeed
     * @see <a href="https://issues.apache.org/jira/browse/KAFKA-15826">KAFKA-15826</a>
     */
    private String modifySinkConnectorOffsetsWithRetry(ConnectorOffsets offsetsToAlter) throws InterruptedException {
        // Some retry logic is necessary to account for KAFKA-15826,
        // where laggy sink task startup/shutdown can leave consumers running
        String modifyVerb = offsetsToAlter != null ?  "alter" : "reset";
        String conditionDetails = "Failed to " + modifyVerb + " sink connector offsets in time";
        AtomicReference<String> responseReference = new AtomicReference<>();
        waitForCondition(
                () -> {
                    try {
                        if (offsetsToAlter == null) {
                            responseReference.set(connect.resetConnectorOffsets(connectorName));
                        } else {
                            responseReference.set(connect.alterConnectorOffsets(connectorName, offsetsToAlter));
                        }
                        return true;
                    } catch (ConnectRestException e) {
                        boolean internalServerError = e.statusCode() == INTERNAL_SERVER_ERROR.getStatusCode();

                        String message = Optional.of(e.getMessage()).orElse("");
                        boolean failedToModifyConsumerOffsets = message.contains(
                                "Failed to " + modifyVerb + " consumer group offsets for connector"
                        );
                        boolean canBeRetried = message.contains("If the connector is in a stopped state, this operation can be safely retried");

                        boolean retriable = internalServerError && failedToModifyConsumerOffsets && canBeRetried;
                        if (retriable) {
                            return false;
                        } else {
                            throw new NoRetryException(e);
                        }
                    } catch (Throwable t) {
                        throw new NoRetryException(t);
                    }
                },
                30_000,
                conditionDetails
        );
        return responseReference.get();
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
        AtomicReference<ConnectorOffsets> latestOffsets = new AtomicReference<>();
        waitForCondition(
                () -> {
                    ConnectorOffsets offsets;
                    try {
                        offsets = connect.connectorOffsets(connectorName);
                    } catch (Throwable t) {
                        log.warn("Failed to list offsets for sink connector {}", connectorName, t);
                        return false;
                    }

                    latestOffsets.set(offsets);

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
                },
                OFFSET_READ_TIMEOUT_MS,
                () -> {
                    String result = conditionDetails + ". ";
                    ConnectorOffsets offsets = latestOffsets.get();
                    if (offsets == null) {
                        result += "No attempt to list the offsets for the connector ever succeeded.";
                    } else {
                        result += "Expected every committed offset to be for topic " + expectedTopic
                                + " and for " + expectedPartitions + " partition(s) of that topic to have "
                                + "a committed offset of " + expectedOffset + ". ";
                        result += "The most-recently-available offsets for the connector are: " + offsets;
                    }
                    return result;
                }
        );
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
        waitForCondition(() -> {
            ConnectorOffsets offsets = connect.connectorOffsets(connectorName);
            // The MonitorableSourceConnector has a source partition per task
            if (offsets.offsets().size() != numTasks) {
                return false;
            }
            for (ConnectorOffset offset : offsets.offsets()) {
                assertTrue(((String) offset.partition().get("task.id")).startsWith(connectorName));
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
        waitForCondition(() -> {
            ConnectorOffsets offsets = connect.connectorOffsets(connectorName);
            return offsets.offsets().isEmpty();
        }, OFFSET_READ_TIMEOUT_MS, "Connector offsets should be empty after resetting offsets");
    }
}
