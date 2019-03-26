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

import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.connect.util.clusters.WorkerHandle;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test simple operations on the workers of a Connect cluster.
 */
@Category(IntegrationTest.class)
public class ConnectWorkerIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(ConnectWorkerIntegrationTest.class);

    private static final int NUM_TOPIC_PARTITIONS = 3;
    private static final int CONNECTOR_SETUP_DURATION_MS = 15_000;
    private static final int WORKER_SETUP_DURATION_MS = 20_000;
    private static final int OFFSET_COMMIT_INTERVAL_MS = 30_000;
    private static final int NUM_WORKERS = 3;
    private static final String CONNECTOR_NAME = "simple-source";

    private EmbeddedConnectCluster connect;

    @Before
    public void setup() throws IOException {
        // setup Connect worker properties
        Map<String, String> exampleWorkerProps = new HashMap<>();
        exampleWorkerProps.put(OFFSET_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(OFFSET_COMMIT_INTERVAL_MS));

        // setup Kafka broker properties
        Properties exampleBrokerProps = new Properties();
        exampleBrokerProps.put("auto.create.topics.enable", String.valueOf(false));

        // build a Connect cluster backed by Kafka and Zk
        connect = new EmbeddedConnectCluster.Builder()
                .name("connect-cluster")
                .numWorkers(NUM_WORKERS)
                .workerProps(exampleWorkerProps)
                .brokerProps(exampleBrokerProps)
                .maskExitProcedures(true) // true is the default, setting here as example
                .build();

        // start the clusters
        connect.start();
    }

    @After
    public void close() {
        // stop all Connect, Kafka and Zk threads.
        connect.stop();
    }

    /**
     * Simple test case to add and then remove a worker from the embedded Connect cluster while
     * running a simple source connector.
     */
    @Test
    public void testAddAndRemoveWorker() throws Exception {
        int numTasks = 4;
        // create test topic
        connect.kafka().createTopic("test-topic", NUM_TOPIC_PARTITIONS);

        // setup up props for the sink connector
        Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR_CLASS_CONFIG, MonitorableSourceConnector.class.getSimpleName());
        props.put(TASKS_MAX_CONFIG, String.valueOf(numTasks));
        props.put("throughput", String.valueOf(1));
        props.put("messages.per.poll", String.valueOf(10));
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());

        waitForCondition(() -> assertWorkersUp(NUM_WORKERS).orElse(false),
                WORKER_SETUP_DURATION_MS, "Initial group of workers did not start in time.");

        // start a source connector
        connect.configureConnector(CONNECTOR_NAME, props);

        waitForCondition(() -> assertConnectorAndTasksRunning(CONNECTOR_NAME, numTasks).orElse(false),
                CONNECTOR_SETUP_DURATION_MS, "Connector tasks did not start in time.");

        WorkerHandle extraWorker = connect.addWorker();

        waitForCondition(() -> assertWorkersUp(NUM_WORKERS + 1).orElse(false),
                WORKER_SETUP_DURATION_MS, "Expanded group of workers did not start in time.");

        waitForCondition(() -> assertConnectorAndTasksRunning(CONNECTOR_NAME, numTasks).orElse(false),
                CONNECTOR_SETUP_DURATION_MS, "Connector tasks are not all in running state.");

        Set<WorkerHandle> workers = connect.activeWorkers();
        assertTrue(workers.contains(extraWorker));

        connect.removeWorker(extraWorker);

        waitForCondition(() -> assertWorkersUp(NUM_WORKERS).orElse(false) && !assertWorkersUp(NUM_WORKERS + 1).orElse(false),
                WORKER_SETUP_DURATION_MS, "Group of workers did not shrink in time.");

        workers = connect.activeWorkers();
        assertFalse(workers.contains(extraWorker));
    }

    /**
     * Confirm that the requested number of workers is up and running.
     *
     * @param numWorkers the number of online workers
     * @return true if at least {@code numWorkers} are up; false otherwise
     */
    private Optional<Boolean> assertWorkersUp(int numWorkers) {
        try {
            int numUp = connect.activeWorkers().size();
            return Optional.of(numUp >= numWorkers);
        } catch (Exception e) {
            log.error("Could not check active workers.", e);
            return Optional.empty();
        }
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
}
