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
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
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
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.TOPICS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * An example integration test that demonstrates how to setup an integration test for Connect.
 * <p></p>
 * The following test configures and executes up a sink connector pipeline in a worker, produces messages into
 * the source topic-partitions, and demonstrates how to check the overall behavior of the pipeline.
 */
@Category(IntegrationTest.class)
public class RebalanceSourceConnectors {

    private static final Logger log = LoggerFactory.getLogger(RebalanceSourceConnectors.class);

    private static final int NUM_RECORDS_PRODUCED = 2000;
    private static final int NUM_TOPIC_PARTITIONS = 3;
    private static final int CONNECTOR_SETUP_DURATION_MS = 5000;
    private static final int NUM_TASKS = 3;
    private static final String CONNECTOR_NAME = "seq-source1";
    private static final String TOPIC_NAME = "sequential-topic";

    private EmbeddedConnectCluster connect;
    private ConnectorHandle connectorHandle;

    @Before
    public void setup() throws IOException {
        // setup Connect worker properties
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put(DistributedConfig.CONNECT_PROTOCOL_COMPATIBILITY, "COOP");
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

        // get a handle to the connector
        connectorHandle = RuntimeHandles.get().connectorHandle(CONNECTOR_NAME);
    }

    @After
    public void close() {
        // delete connector handle
        RuntimeHandles.get().deleteConnector(CONNECTOR_NAME);

        // stop all Connect, Kafka and Zk threads.
        connect.stop();
    }

    /**
     * Simple test case to configure and execute an embedded Connect cluster. The test will produce and consume
     * records, and start up a sink connector which will consume these records.
     */
    @Test
    public void testProduceConsumeConnector() throws Exception {
        // create test topic
        connect.kafka().createTopic(TOPIC_NAME, NUM_TOPIC_PARTITIONS);

        // setup up props for the source connector
        Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR_CLASS_CONFIG, MonitorableSourceConnector.class.getSimpleName());
        props.put(TASKS_MAX_CONFIG, String.valueOf(NUM_TASKS));
        props.put("throughput", String.valueOf(1));
        props.put("messages.per.poll", String.valueOf(10));
        props.put(TOPICS_CONFIG, TOPIC_NAME);
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());

        // expect all records to be consumed by the connector
        connectorHandle.expectedRecords(NUM_RECORDS_PRODUCED);

        // start a source connector
        connect.configureConnector(CONNECTOR_NAME, props);

        waitForCondition(() -> this.assertConnectorAndTasksRunning(CONNECTOR_NAME, NUM_TASKS),
                CONNECTOR_SETUP_DURATION_MS, "Connector tasks did not start in time.");

        /*
        // consume all records from the source topic or fail, to ensure that they were correctly produced.
        assertEquals("Unexpected number of records consumed", NUM_RECORDS_PRODUCED,
                connect.kafka().consume(NUM_RECORDS_PRODUCED, CONSUME_MAX_DURATION_MS, TOPIC_NAME).count());
        */

        // start a source connector
        props.put(TASKS_MAX_CONFIG, String.valueOf(4));
        connect.configureConnector("another-source", props);

        waitForCondition(() -> this.assertConnectorAndTasksRunning(CONNECTOR_NAME, NUM_TASKS),
                CONNECTOR_SETUP_DURATION_MS, "Connector tasks did not start in time.");

        waitForCondition(() -> this.assertConnectorAndTasksRunning("another-source", 4),
                CONNECTOR_SETUP_DURATION_MS, "Connector tasks did not start in time.");

        // delete connector
        connect.deleteConnector(CONNECTOR_NAME);
    }

    @Test
    public void testAddingWorker() throws Exception {
        // create test topic
        connect.kafka().createTopic(TOPIC_NAME, NUM_TOPIC_PARTITIONS);

        // setup up props for the source connector
        Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR_CLASS_CONFIG, MonitorableSourceConnector.class.getSimpleName());
        props.put(TASKS_MAX_CONFIG, String.valueOf(4));
        props.put("throughput", String.valueOf(1));
        props.put("messages.per.poll", String.valueOf(10));
        props.put(TOPICS_CONFIG, TOPIC_NAME);
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());

        // expect all records to be consumed by the connector
        connectorHandle.expectedRecords(NUM_RECORDS_PRODUCED);

        waitForCondition(() -> this.assertWorkersUp(3),
                5000, "Connect workers did not start in time.");

        // start a source connector
        IntStream.range(0, 4).forEachOrdered(
            i -> {
                try {
                    connect.configureConnector(CONNECTOR_NAME + i, props);
                } catch (IOException e) {
                    throw new ConnectException(e);
                }
            });

        waitForCondition(() -> this.assertConnectorAndTasksRunning(CONNECTOR_NAME + 3, 4),
                CONNECTOR_SETUP_DURATION_MS, "Connector tasks did not start in time.");

        connect.addWorker();

        waitForCondition(() -> this.assertWorkersUp(4),
                5000, "Connect workers did not start in time.");

        Thread.sleep(10000);

        waitForCondition(this::assertConnectorAndTasksAreBalanced,
                5000, "Connect and tasks are imbalanced between the workers.");

        // delete connector
        //connect.deleteConnector(CONNECTOR_NAME);
    }

    /**
     */
    private boolean assertConnectorAndTasksRunning(String connectorName, int numTasks) {
        try {
            ConnectorStateInfo info = connect.connectorStatus(connectorName);
            if (info.tasks().size() > 1) {
                System.err.println("All the info!" + info);
            }
            return info != null
                    && info.tasks().size() == numTasks
                    && info.connector().state().equals(AbstractStatus.State.RUNNING.toString())
                    && info.tasks().stream().allMatch(s -> s.state().equals(AbstractStatus.State.RUNNING.toString()));
        } catch (Exception e) {
            log.error("Could not check connector state info.", e);
            return false;
        }
    }

    /**
     */
    private boolean assertWorkersUp(int numWorkers) {
        try {
            int numUp = connect.activeWorkers().size();
            return numUp >= numWorkers;
        } catch (Exception e) {
            log.error("Could not check active workers.", e);
            return false;
        }
    }

    /**
     */
    private boolean assertConnectorAndTasksAreBalanced() {
        try {
            Map<String, Collection<String>> connectors = new HashMap<>();
            Map<String, Collection<String>> tasks = new HashMap<>();
            for (String connector : connect.connectors()) {
                ConnectorStateInfo info = connect.connectorStatus(connector);
                connectors.computeIfAbsent(info.connector().workerId(), k -> new ArrayList<>())
                        .add(connector);
                info.tasks().stream().forEach(
                    t -> tasks.computeIfAbsent(t.workerId(), k -> new ArrayList<>())
                           .add(connector + "-" + t.id()));
            }

            int maxConnectors = connectors.values().stream().mapToInt(Collection::size).max().orElse(0);
            int minConnectors = connectors.values().stream().mapToInt(Collection::size).min().orElse(0);
            int maxTasks = tasks.values().stream().mapToInt(Collection::size).max().orElse(0);
            int minTasks = tasks.values().stream().mapToInt(Collection::size).min().orElse(0);

            assertNotEquals("Found no connectors running!", maxConnectors, 0);
            assertNotEquals("Found no tasks running!", maxTasks, 0);
            assertEquals("Connector assignments are not unique: " + connectors,
                    connectors.values().size(),
                    connectors.values().stream().distinct().collect(Collectors.toList()).size());
            assertEquals("Task assignments are not unique: " + tasks,
                    tasks.values().size(),
                    tasks.values().stream().distinct().collect(Collectors.toList()).size());
            assertTrue("Connectors are imbalanced: " + connectors, maxConnectors - minConnectors < 3);
            assertTrue("Tasks are imbalanced: " + tasks, maxTasks - minTasks < 3);
            return true;
        } catch (Exception e) {
            log.error("Could not check connector state info.", e);
            return false;
        }
    }

}
