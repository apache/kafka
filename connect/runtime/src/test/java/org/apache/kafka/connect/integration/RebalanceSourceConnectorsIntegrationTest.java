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
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestRule;
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

import static org.apache.kafka.connect.integration.MonitorableSourceConnector.TOPIC_CONFIG;
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests for incremental cooperative rebalancing between Connect workers
 */
@Category(IntegrationTest.class)
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

    @Rule
    public TestRule watcher = ConnectIntegrationTestUtils.newTestWatcher(log);

    @Before
    public void setup() {
        // setup Connect worker properties
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put(CONNECT_PROTOCOL_CONFIG, COMPATIBLE.toString());
        workerProps.put(OFFSET_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(TimeUnit.SECONDS.toMillis(30)));
        workerProps.put(SCHEDULED_REBALANCE_MAX_DELAY_MS_CONFIG, String.valueOf(TimeUnit.SECONDS.toMillis(30)));

        // setup Kafka broker properties
        Properties brokerProps = new Properties();
        brokerProps.put("auto.create.topics.enable", "false");

        // build a Connect cluster backed by Kafka and Zk
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

    @After
    public void close() {
        // stop all Connect, Kafka and Zk threads.
        connect.stop();
    }

    @Test
    public void testStartTwoConnectors() throws Exception {
        // create test topic
        connect.kafka().createTopic(TOPIC_NAME, NUM_TOPIC_PARTITIONS);

        // setup up props for the source connector
        Map<String, String> props = defaultSourceConnectorProps(TOPIC_NAME);

        connect.assertions().assertAtLeastNumWorkersAreUp(NUM_WORKERS,
                "Connect workers did not start in time.");

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

        connect.assertions().assertAtLeastNumWorkersAreUp(NUM_WORKERS,
                "Connect workers did not start in time.");

        // start a source connector
        connect.configureConnector(CONNECTOR_NAME, props);

        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, NUM_TASKS,
                "Connector tasks did not start in time.");

        int numRecordsProduced = 100;
        long recordTransferDurationMs = TimeUnit.SECONDS.toMillis(30);

        // consume all records from the source topic or fail, to ensure that they were correctly produced
        int recordNum = connect.kafka().consume(numRecordsProduced, recordTransferDurationMs, TOPIC_NAME).count();
        assertTrue("Not enough records produced by source connector. Expected at least: " + numRecordsProduced + " + but got " + recordNum,
                recordNum >= numRecordsProduced);

        // expect that we're going to restart the connector and its tasks
        StartAndStopLatch restartLatch = connectorHandle.expectedStarts(1);

        // Reconfigure the source connector by changing the Kafka topic used as output
        props.put(TOPIC_CONFIG, anotherTopic);
        connect.configureConnector(CONNECTOR_NAME, props);

        // Wait for the connector *and tasks* to be restarted
        assertTrue("Failed to alter connector configuration and see connector and tasks restart "
                   + "within " + CONNECTOR_SETUP_DURATION_MS + "ms",
                restartLatch.await(CONNECTOR_SETUP_DURATION_MS, TimeUnit.MILLISECONDS));

        // And wait for the Connect to show the connectors and tasks are running
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, NUM_TASKS,
                "Connector tasks did not start in time.");

        // consume all records from the source topic or fail, to ensure that they were correctly produced
        recordNum = connect.kafka().consume(numRecordsProduced, recordTransferDurationMs, anotherTopic).count();
        assertTrue("Not enough records produced by source connector. Expected at least: " + numRecordsProduced + " + but got " + recordNum,
                recordNum >= numRecordsProduced);
    }

    @Test
    public void testDeleteConnector() throws Exception {
        // create test topic
        connect.kafka().createTopic(TOPIC_NAME, NUM_TOPIC_PARTITIONS);

        // setup up props for the source connector
        Map<String, String> props = defaultSourceConnectorProps(TOPIC_NAME);

        connect.assertions().assertAtLeastNumWorkersAreUp(NUM_WORKERS,
                "Connect workers did not start in time.");

        // start several source connectors
        IntStream.range(0, 4).forEachOrdered(i -> connect.configureConnector(CONNECTOR_NAME + i, props));

        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME + 3, NUM_TASKS,
                "Connector tasks did not start in time.");

        // delete connector
        connect.deleteConnector(CONNECTOR_NAME + 3);

        connect.assertions().assertConnectorAndTasksAreStopped(CONNECTOR_NAME + 3,
                "Connector tasks did not stop in time.");

        waitForCondition(this::assertConnectorAndTasksAreUniqueAndBalanced,
                WORKER_SETUP_DURATION_MS, "Connect and tasks are imbalanced between the workers.");
    }

    @Test
    public void testAddingWorker() throws Exception {
        // create test topic
        connect.kafka().createTopic(TOPIC_NAME, NUM_TOPIC_PARTITIONS);

        // setup up props for the source connector
        Map<String, String> props = defaultSourceConnectorProps(TOPIC_NAME);

        connect.assertions().assertAtLeastNumWorkersAreUp(NUM_WORKERS,
                "Connect workers did not start in time.");

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

        connect.assertions().assertExactlyNumWorkersAreUp(NUM_WORKERS,
                "Connect workers did not start in time.");

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

    // should enable it after KAFKA-12495 fixed
    @Ignore
    @Test
    public void testMultipleWorkersRejoining() throws Exception {
        // create test topic
        connect.kafka().createTopic(TOPIC_NAME, NUM_TOPIC_PARTITIONS);

        // setup up props for the source connector
        Map<String, String> props = defaultSourceConnectorProps(TOPIC_NAME);

        connect.assertions().assertExactlyNumWorkersAreUp(NUM_WORKERS,
                "Connect workers did not start in time.");

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
        props.put(CONNECTOR_CLASS_CONFIG, MonitorableSourceConnector.class.getSimpleName());
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

            assertNotEquals("Found no connectors running!", maxConnectors, 0);
            assertNotEquals("Found no tasks running!", maxTasks, 0);
            assertEquals("Connector assignments are not unique: " + connectors,
                    connectors.values().size(),
                    connectors.values().stream().distinct().collect(Collectors.toList()).size());
            assertEquals("Task assignments are not unique: " + tasks,
                    tasks.values().size(),
                    tasks.values().stream().distinct().collect(Collectors.toList()).size());
            assertTrue("Connectors are imbalanced: " + formatAssignment(connectors), maxConnectors - minConnectors < 2);
            assertTrue("Tasks are imbalanced: " + formatAssignment(tasks), maxTasks - minTasks < 2);
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
