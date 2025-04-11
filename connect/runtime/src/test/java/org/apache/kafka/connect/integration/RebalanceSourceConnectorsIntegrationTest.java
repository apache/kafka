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

import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;

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
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.connect.integration.TestableSourceConnector.TOPIC_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.DEFAULT_TOPIC_CREATION_PREFIX;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.PARTITIONS_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocolCompatibility.COMPATIBLE;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.CONNECT_PROTOCOL_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.SCHEDULED_REBALANCE_MAX_DELAY_MS_CONFIG;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for incremental cooperative rebalancing between Connect workers
 */
@Tag("integration")
public class RebalanceSourceConnectorsIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(RebalanceSourceConnectorsIntegrationTest.class);

    private static final int NUM_TOPIC_PARTITIONS = 3;
    private static final long CONNECTOR_SETUP_DURATION_MS = TimeUnit.SECONDS.toMillis(30);
    private static final long WORKER_SETUP_DURATION_MS = TimeUnit.SECONDS.toMillis(60);
    private static final int NUM_WORKERS = 3;
    private static final int NUM_TASKS = 4;
    private static final String CONNECTOR_NAME = "seq-source1";
    private static final String TOPIC_NAME = "sequential-topic";

    private EmbeddedConnectCluster connect;
    

    @BeforeEach
    public void setup(TestInfo testInfo) {
        log.info("Starting test {}", testInfo.getDisplayName());
        // setup Connect worker properties
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put(CONNECT_PROTOCOL_CONFIG, COMPATIBLE.toString());
        workerProps.put(OFFSET_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(TimeUnit.SECONDS.toMillis(30)));
        workerProps.put(SCHEDULED_REBALANCE_MAX_DELAY_MS_CONFIG, String.valueOf(TimeUnit.SECONDS.toMillis(30)));

        // setup Kafka broker properties
        Properties brokerProps = new Properties();
        brokerProps.put("auto.create.topics.enable", "false");

        // build a Connect cluster backed by a Kafka KRaft cluster
        connect = new EmbeddedConnectCluster.Builder()
                .name("connect-cluster")
                .numWorkers(NUM_WORKERS)
                .numBrokers(1)
                .workerProps(workerProps)
                .brokerProps(brokerProps)
                .build();

        // start the clusters
        connect.start();
    }

    @AfterEach
    public void close(TestInfo testInfo) {
        log.info("Finished test {}", testInfo.getDisplayName());
        // stop the Connect cluster and its backing Kafka cluster.
        connect.stop();
    }

    @Test
    public void testStartTwoConnectors() throws Exception {
        // create test topic
        connect.kafka().createTopic(TOPIC_NAME, NUM_TOPIC_PARTITIONS);

        // setup up props for the source connector
        Map<String, String> props = defaultSourceConnectorProps(TOPIC_NAME);

        // start a source connector
        connect.configureConnector(CONNECTOR_NAME, props);

        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, NUM_TASKS,
                "Connector tasks did not start in time.");

        // start a source connector
        connect.configureConnector("another-source", props);

        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, NUM_TASKS,
                "Connector tasks did not start in time.");

        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning("another-source", 4,
                "Connector tasks did not start in time.");
    }

    @Test
    public void testReconfigConnector() throws Exception {
        ConnectorHandle connectorHandle = RuntimeHandles.get().connectorHandle(CONNECTOR_NAME);

        // create test topic
        String anotherTopic = "another-topic";
        connect.kafka().createTopic(TOPIC_NAME, NUM_TOPIC_PARTITIONS);
        connect.kafka().createTopic(anotherTopic, NUM_TOPIC_PARTITIONS);

        // setup up props for the source connector
        Map<String, String> props = defaultSourceConnectorProps(TOPIC_NAME);

        // start a source connector
        connect.configureConnector(CONNECTOR_NAME, props);

        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, NUM_TASKS,
                "Connector tasks did not start in time.");

        int numRecordsProduced = 100;
        long recordTransferDurationMs = TimeUnit.SECONDS.toMillis(30);

        // consume all records from the source topic or fail, to ensure that they were correctly produced
        int recordNum = connect.kafka().consume(numRecordsProduced, recordTransferDurationMs, TOPIC_NAME).count();
        assertTrue(recordNum >= numRecordsProduced,
                "Not enough records produced by source connector. Expected at least: " + numRecordsProduced + " + but got " + recordNum);

        // expect that we're going to restart the connector and its tasks
        StartAndStopLatch restartLatch = connectorHandle.expectedStarts(1);

        // Reconfigure the source connector by changing the Kafka topic used as output
        props.put(TOPIC_CONFIG, anotherTopic);
        connect.configureConnector(CONNECTOR_NAME, props);

        // Wait for the connector *and tasks* to be restarted
        assertTrue(restartLatch.await(CONNECTOR_SETUP_DURATION_MS, TimeUnit.MILLISECONDS),
                "Failed to alter connector configuration and see connector and tasks restart "
                        + "within " + CONNECTOR_SETUP_DURATION_MS + "ms");

        // And wait for the Connect to show the connectors and tasks are running
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, NUM_TASKS,
                "Connector tasks did not start in time.");

        // consume all records from the source topic or fail, to ensure that they were correctly produced
        recordNum = connect.kafka().consume(numRecordsProduced, recordTransferDurationMs, anotherTopic).count();
        assertTrue(recordNum >= numRecordsProduced,
                "Not enough records produced by source connector. Expected at least: " + numRecordsProduced + " + but got " + recordNum);
    }

    @Test
    public void testDeleteConnector() throws Exception {
        // create test topic
        connect.kafka().createTopic(TOPIC_NAME, NUM_TOPIC_PARTITIONS);

        // setup up props for the source connector
        Map<String, String> props = defaultSourceConnectorProps(TOPIC_NAME);

        // start several source connectors
        IntStream.range(0, 4).forEachOrdered(i -> connect.configureConnector(CONNECTOR_NAME + i, props));

        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME + 3, NUM_TASKS,
                "Connector tasks did not start in time.");

        // delete connector
        connect.deleteConnector(CONNECTOR_NAME + 3);

        connect.assertions().assertConnectorDoesNotExist(CONNECTOR_NAME + 3,
                "Connector wasn't deleted in time.");

        waitForCondition(this::assertConnectorAndTasksAreUniqueAndBalanced,
                WORKER_SETUP_DURATION_MS, "Connect and tasks are imbalanced between the workers.");
    }

    @Test
    public void testAddingWorker() throws Exception {
        // create test topic
        connect.kafka().createTopic(TOPIC_NAME, NUM_TOPIC_PARTITIONS);

        // setup up props for the source connector
        Map<String, String> props = defaultSourceConnectorProps(TOPIC_NAME);

        // start a source connector
        IntStream.range(0, 4).forEachOrdered(i -> connect.configureConnector(CONNECTOR_NAME + i, props));

        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME + 3, NUM_TASKS,
                "Connector tasks did not start in time.");

        connect.addWorker();

        connect.assertions().assertAtLeastNumWorkersAreUp(NUM_WORKERS + 1,
                "Connect workers did not start in time.");

        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME + 3, NUM_TASKS,
                "Connector tasks did not start in time.");

        waitForCondition(this::assertConnectorAndTasksAreUniqueAndBalanced,
                WORKER_SETUP_DURATION_MS, "Connect and tasks are imbalanced between the workers.");
    }

    @Test
    public void testRemovingWorker() throws Exception {
        // create test topic
        connect.kafka().createTopic(TOPIC_NAME, NUM_TOPIC_PARTITIONS);

        // setup up props for the source connector
        Map<String, String> props = defaultSourceConnectorProps(TOPIC_NAME);

        // start a source connector
        IntStream.range(0, 4).forEachOrdered(i -> connect.configureConnector(CONNECTOR_NAME + i, props));

        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME + 3, NUM_TASKS,
                "Connector tasks did not start in time.");

        connect.removeWorker();

        connect.assertions().assertExactlyNumWorkersAreUp(NUM_WORKERS - 1,
                "Connect workers did not start in time.");

        waitForCondition(this::assertConnectorAndTasksAreUniqueAndBalanced,
                WORKER_SETUP_DURATION_MS, "Connect and tasks are imbalanced between the workers.");
    }

    @Test
    public void testMultipleWorkersRejoining() throws Exception {
        // create test topic
        connect.kafka().createTopic(TOPIC_NAME, NUM_TOPIC_PARTITIONS);

        // setup up props for the source connector
        Map<String, String> props = defaultSourceConnectorProps(TOPIC_NAME);

        // start a source connector
        IntStream.range(0, 4).forEachOrdered(i -> connect.configureConnector(CONNECTOR_NAME + i, props));

        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME + 3, NUM_TASKS,
                "Connector tasks did not start in time.");

        waitForCondition(this::assertConnectorAndTasksAreUniqueAndBalanced,
                WORKER_SETUP_DURATION_MS, "Connect and tasks are imbalanced between the workers.");

        Thread.sleep(TimeUnit.SECONDS.toMillis(10));

        connect.removeWorker();
        connect.removeWorker();

        connect.assertions().assertExactlyNumWorkersAreUp(NUM_WORKERS - 2,
                "Connect workers did not stop in time.");

        Thread.sleep(TimeUnit.SECONDS.toMillis(10));

        connect.addWorker();
        connect.addWorker();

        connect.assertions().assertExactlyNumWorkersAreUp(NUM_WORKERS,
                "Connect workers did not start in time.");

        Thread.sleep(TimeUnit.SECONDS.toMillis(10));

        for (int i = 0; i < 4; ++i) {
            connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME + i, NUM_TASKS, "Connector tasks did not start in time.");
        }

        waitForCondition(this::assertConnectorAndTasksAreUniqueAndBalanced,
                WORKER_SETUP_DURATION_MS, "Connect and tasks are imbalanced between the workers.");
    }

    private Map<String, String> defaultSourceConnectorProps(String topic) {
        // setup up props for the source connector
        Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR_CLASS_CONFIG, TestableSourceConnector.class.getSimpleName());
        props.put(TASKS_MAX_CONFIG, String.valueOf(NUM_TASKS));
        props.put(TOPIC_CONFIG, topic);
        props.put("throughput", String.valueOf(10));
        props.put("messages.per.poll", String.valueOf(10));
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + REPLICATION_FACTOR_CONFIG, String.valueOf(1));
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + PARTITIONS_CONFIG, String.valueOf(1));
        return props;
    }

    private boolean assertConnectorAndTasksAreUniqueAndBalanced() {
        try {
            Map<String, Collection<String>> connectors = new HashMap<>();
            Map<String, Collection<String>> tasks = new HashMap<>();
            for (String connector : connect.connectors()) {
                ConnectorStateInfo info = connect.connectorStatus(connector);
                connectors.computeIfAbsent(info.connector().workerId(), k -> new ArrayList<>())
                        .add(connector);
                info.tasks().forEach(
                    t -> tasks.computeIfAbsent(t.workerId(), k -> new ArrayList<>())
                           .add(connector + "-" + t.id()));
            }

            int maxConnectors = connectors.values().stream().mapToInt(Collection::size).max().orElse(0);
            int minConnectors = connectors.values().stream().mapToInt(Collection::size).min().orElse(0);
            int maxTasks = tasks.values().stream().mapToInt(Collection::size).max().orElse(0);
            int minTasks = tasks.values().stream().mapToInt(Collection::size).min().orElse(0);

            log.debug("Connector balance: {}", formatAssignment(connectors));
            log.debug("Task balance: {}", formatAssignment(tasks));

            assertNotEquals(0, maxConnectors, "Found no connectors running!");
            assertNotEquals(0, maxTasks, "Found no tasks running!");
            assertEquals(connectors.values().size(),
                    connectors.values().stream().distinct().count(),
                    "Connector assignments are not unique: " + connectors);
            assertEquals(tasks.values().size(),
                    tasks.values().stream().distinct().count(),
                    "Task assignments are not unique: " + tasks);
            assertTrue(maxConnectors - minConnectors < 2, "Connectors are imbalanced: " + formatAssignment(connectors));
            assertTrue(maxTasks - minTasks < 2, "Tasks are imbalanced: " + formatAssignment(tasks));
            return true;
        } catch (Exception e) {
            log.error("Could not check connector state info.", e);
            return false;
        }
    }

    private static String formatAssignment(Map<String, Collection<String>> assignment) {
        StringBuilder result = new StringBuilder();
        for (String worker : assignment.keySet().stream().sorted().collect(Collectors.toList())) {
            result.append(String.format("\n%s=%s", worker, assignment.getOrDefault(worker,
                    Collections.emptyList())));
        }
        return result.toString();
    }

}
