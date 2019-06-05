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

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.connect.integration.MonitorableSourceConnector.TOPIC_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocolCompatibility.COMPATIBLE;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.CONNECT_PROTOCOL_CONFIG;
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
    private static final int CONNECTOR_SETUP_DURATION_MS = 30_000;
    private static final int WORKER_SETUP_DURATION_MS = 30_000;
    private static final int NUM_TASKS = 4;
    private static final String CONNECTOR_NAME = "seq-source1";
    private static final String TOPIC_NAME = "sequential-topic";

    private EmbeddedConnectCluster connect;

    @Before
    public void setup() throws IOException {
        // setup Connect worker properties
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put(CONNECT_PROTOCOL_CONFIG, COMPATIBLE.toString());
        workerProps.put(OFFSET_COMMIT_INTERVAL_MS_CONFIG, "30000");

        // setup Kafka broker properties
        Properties brokerProps = new Properties();
        brokerProps.put("auto.create.topics.enable", "false");

        // build a Connect cluster backed by Kafka and Zk
        connect = new EmbeddedConnectCluster.Builder()
                .name("connect-cluster")
                .numWorkers(3)
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
        Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR_CLASS_CONFIG, MonitorableSourceConnector.class.getSimpleName());
        props.put(TASKS_MAX_CONFIG, String.valueOf(NUM_TASKS));
        props.put("throughput", String.valueOf(1));
        props.put("messages.per.poll", String.valueOf(10));
        props.put(TOPIC_CONFIG, TOPIC_NAME);
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());

        // start a source connector
        connect.configureConnector(CONNECTOR_NAME, props);

        waitForCondition(() -> this.assertConnectorAndTasksRunning(CONNECTOR_NAME, NUM_TASKS).orElse(false),
                CONNECTOR_SETUP_DURATION_MS, "Connector tasks did not start in time.");

        // start a source connector
        connect.configureConnector("another-source", props);

        waitForCondition(() -> this.assertConnectorAndTasksRunning(CONNECTOR_NAME, NUM_TASKS).orElse(false),
                CONNECTOR_SETUP_DURATION_MS, "Connector tasks did not start in time.");

        waitForCondition(() -> this.assertConnectorAndTasksRunning("another-source", 4).orElse(false),
                CONNECTOR_SETUP_DURATION_MS, "Connector tasks did not start in time.");
    }

    @Test
    public void testReconfigConnector() throws Exception {
        ConnectorHandle connectorHandle = RuntimeHandles.get().connectorHandle(CONNECTOR_NAME);

        // create test topic
        String anotherTopic = "another-topic";
        connect.kafka().createTopic(TOPIC_NAME, NUM_TOPIC_PARTITIONS);
        connect.kafka().createTopic(anotherTopic, NUM_TOPIC_PARTITIONS);

        // setup up props for the source connector
        Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR_CLASS_CONFIG, MonitorableSourceConnector.class.getSimpleName());
        props.put(TASKS_MAX_CONFIG, String.valueOf(NUM_TASKS));
        props.put("throughput", String.valueOf(1));
        props.put("messages.per.poll", String.valueOf(10));
        props.put(TOPIC_CONFIG, TOPIC_NAME);
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());

        // start a source connector
        connect.configureConnector(CONNECTOR_NAME, props);

        waitForCondition(() -> this.assertConnectorAndTasksRunning(CONNECTOR_NAME, NUM_TASKS).orElse(false),
                CONNECTOR_SETUP_DURATION_MS, "Connector tasks did not start in time.");

        int numRecordsProduced = 100;
        int recordTransferDurationMs = 5000;

        // consume all records from the source topic or fail, to ensure that they were correctly produced
        int recordNum = connect.kafka().consume(numRecordsProduced, recordTransferDurationMs, TOPIC_NAME).count();
        assertTrue("Not enough records produced by source connector. Expected at least: " + numRecordsProduced + " + but got " + recordNum,
                recordNum >= numRecordsProduced);

        // Reconfigure the source connector by changing the Kafka topic used as output
        props.put(TOPIC_CONFIG, anotherTopic);
        connect.configureConnector(CONNECTOR_NAME, props);

        waitForCondition(() -> this.assertConnectorAndTasksRunning(CONNECTOR_NAME, NUM_TASKS).orElse(false),
                CONNECTOR_SETUP_DURATION_MS, "Connector tasks did not start in time.");

        // expect all records to be produced by the connector
        connectorHandle.expectedRecords(numRecordsProduced);

        // expect all records to be produced by the connector
        connectorHandle.expectedCommits(numRecordsProduced);

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
        Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR_CLASS_CONFIG, MonitorableSourceConnector.class.getSimpleName());
        props.put(TASKS_MAX_CONFIG, String.valueOf(NUM_TASKS));
        props.put("throughput", String.valueOf(1));
        props.put("messages.per.poll", String.valueOf(10));
        props.put(TOPIC_CONFIG, TOPIC_NAME);
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());

        waitForCondition(() -> this.assertWorkersUp(3),
                WORKER_SETUP_DURATION_MS, "Connect workers did not start in time.");

        // start a source connector
        IntStream.range(0, 4).forEachOrdered(
            i -> {
                try {
                    connect.configureConnector(CONNECTOR_NAME + i, props);
                } catch (IOException e) {
                    throw new ConnectException(e);
                }
            });

        waitForCondition(() -> this.assertConnectorAndTasksRunning(CONNECTOR_NAME + 3, NUM_TASKS).orElse(true),
                CONNECTOR_SETUP_DURATION_MS, "Connector tasks did not start in time.");

        // delete connector
        connect.deleteConnector(CONNECTOR_NAME + 3);

        waitForCondition(() -> !this.assertConnectorAndTasksRunning(CONNECTOR_NAME + 3, NUM_TASKS).orElse(true),
                CONNECTOR_SETUP_DURATION_MS, "Connector tasks did not stop in time.");

        waitForCondition(this::assertConnectorAndTasksAreUnique,
                WORKER_SETUP_DURATION_MS, "Connect and tasks are imbalanced between the workers.");
    }

    @Test
    public void testAddingWorker() throws Exception {
        // create test topic
        connect.kafka().createTopic(TOPIC_NAME, NUM_TOPIC_PARTITIONS);

        // setup up props for the source connector
        Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR_CLASS_CONFIG, MonitorableSourceConnector.class.getSimpleName());
        props.put(TASKS_MAX_CONFIG, String.valueOf(NUM_TASKS));
        props.put("throughput", String.valueOf(1));
        props.put("messages.per.poll", String.valueOf(10));
        props.put(TOPIC_CONFIG, TOPIC_NAME);
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());

        waitForCondition(() -> this.assertWorkersUp(3),
                WORKER_SETUP_DURATION_MS, "Connect workers did not start in time.");

        // start a source connector
        IntStream.range(0, 4).forEachOrdered(
            i -> {
                try {
                    connect.configureConnector(CONNECTOR_NAME + i, props);
                } catch (IOException e) {
                    throw new ConnectException(e);
                }
            });

        waitForCondition(() -> this.assertConnectorAndTasksRunning(CONNECTOR_NAME + 3, NUM_TASKS).orElse(false),
                CONNECTOR_SETUP_DURATION_MS, "Connector tasks did not start in time.");

        connect.addWorker();

        waitForCondition(() -> this.assertWorkersUp(4),
                WORKER_SETUP_DURATION_MS, "Connect workers did not start in time.");

        waitForCondition(() -> this.assertConnectorAndTasksRunning(CONNECTOR_NAME + 3, NUM_TASKS).orElse(false),
                CONNECTOR_SETUP_DURATION_MS, "Connector tasks did not start in time.");

        waitForCondition(this::assertConnectorAndTasksAreUnique,
                WORKER_SETUP_DURATION_MS, "Connect and tasks are imbalanced between the workers.");
    }

    @Test
    public void testRemovingWorker() throws Exception {
        // create test topic
        connect.kafka().createTopic(TOPIC_NAME, NUM_TOPIC_PARTITIONS);

        // setup up props for the source connector
        Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR_CLASS_CONFIG, MonitorableSourceConnector.class.getSimpleName());
        props.put(TASKS_MAX_CONFIG, String.valueOf(NUM_TASKS));
        props.put("throughput", String.valueOf(1));
        props.put("messages.per.poll", String.valueOf(10));
        props.put(TOPIC_CONFIG, TOPIC_NAME);
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());

        waitForCondition(() -> this.assertWorkersUp(3),
                WORKER_SETUP_DURATION_MS, "Connect workers did not start in time.");

        // start a source connector
        IntStream.range(0, 4).forEachOrdered(
            i -> {
                try {
                    connect.configureConnector(CONNECTOR_NAME + i, props);
                } catch (IOException e) {
                    throw new ConnectException(e);
                }
            });

        waitForCondition(() -> this.assertConnectorAndTasksRunning(CONNECTOR_NAME + 3, NUM_TASKS).orElse(false),
                CONNECTOR_SETUP_DURATION_MS, "Connector tasks did not start in time.");

        connect.removeWorker();

        waitForCondition(() -> this.assertWorkersUp(2),
                WORKER_SETUP_DURATION_MS, "Connect workers did not start in time.");

        waitForCondition(this::assertConnectorAndTasksAreUnique,
                WORKER_SETUP_DURATION_MS, "Connect and tasks are imbalanced between the workers.");
    }

    /**
     * Confirm that a connector with an exact number of tasks is running.
     *
     * @param connectorName the connector
     * @param numTasks the expected number of tasks
     * @return true if the connector and tasks are in RUNNING state; false otherwise
     */
    private Optional<Boolean> assertConnectorAndTasksRunning(String connectorName, int numTasks) {
        try {
            ConnectorStateInfo info = connect.connectorStatus(connectorName);
            boolean result = info != null
                    && info.tasks().size() == numTasks
                    && info.connector().state().equals(AbstractStatus.State.RUNNING.toString())
                    && info.tasks().stream().allMatch(s -> s.state().equals(AbstractStatus.State.RUNNING.toString()));
            return Optional.of(result);
        } catch (Exception e) {
            log.error("Could not check connector state info.", e);
            return Optional.empty();
        }
    }

    /**
     * Verifies whether the supplied number of workers matches the number of workers
     * currently running.
     * @param numWorkers the expected number of active workers
     * @return true if exactly numWorkers are active; false if more or fewer workers are running
     */
    private boolean assertWorkersUp(int numWorkers) {
        try {
            int numUp = connect.activeWorkers().size();
            return numUp == numWorkers;
        } catch (Exception e) {
            log.error("Could not check active workers.", e);
            return false;
        }
    }

    private boolean assertConnectorAndTasksAreUnique() {
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
            int maxTasks = tasks.values().stream().mapToInt(Collection::size).max().orElse(0);

            assertNotEquals("Found no connectors running!", maxConnectors, 0);
            assertNotEquals("Found no tasks running!", maxTasks, 0);
            assertEquals("Connector assignments are not unique: " + connectors,
                    connectors.values().size(),
                    connectors.values().stream().distinct().collect(Collectors.toList()).size());
            assertEquals("Task assignments are not unique: " + tasks,
                    tasks.values().size(),
                    tasks.values().stream().distinct().collect(Collectors.toList()).size());
            return true;
        } catch (Exception e) {
            log.error("Could not check connector state info.", e);
            return false;
        }
    }

}
