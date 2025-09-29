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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import jakarta.ws.rs.core.Response;

import static org.apache.kafka.connect.integration.MonitorableSourceConnector.TOPIC_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.DEFAULT_TOPIC_CREATION_PREFIX;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.PARTITIONS_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.CONNECTOR_CLIENT_POLICY_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.connect.util.clusters.ConnectAssertions.CONNECTOR_SETUP_DURATION_MS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test connectors restart API use cases.
 */
@Tag("integration")
public class ConnectorRestartApiIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(ConnectorRestartApiIntegrationTest.class);

    private static final long OFFSET_COMMIT_INTERVAL_MS = TimeUnit.SECONDS.toMillis(30);
    private static final int ONE_WORKER = 1;
    private static final int NUM_TASKS = 4;
    private static final int MESSAGES_PER_POLL = 10;
    private static final String CONNECTOR_NAME_PREFIX = "conn-";

    private static final String TOPIC_NAME = "test-topic";

    private static final Map<Integer, EmbeddedConnectCluster> CONNECT_CLUSTERS = new ConcurrentHashMap<>();

    private EmbeddedConnectCluster connect;
    private ConnectorHandle connectorHandle;
    private String connectorName;

    @BeforeEach
    public void setup(TestInfo testInfo) {
        log.info("Starting test {}", testInfo.getDisplayName());
        connectorName = CONNECTOR_NAME_PREFIX + testInfo.getTestMethod().get().getName();
        // get connector handles before starting test.
        connectorHandle = RuntimeHandles.get().connectorHandle(connectorName);
    }

    private void startOrReuseConnectWithNumWorkers(int numWorkers) {
        connect = CONNECT_CLUSTERS.computeIfAbsent(numWorkers, n -> {
            // setup Connect worker properties
            Map<String, String> workerProps = new HashMap<>();
            workerProps.put(OFFSET_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(OFFSET_COMMIT_INTERVAL_MS));
            workerProps.put(CONNECTOR_CLIENT_POLICY_CLASS_CONFIG, "All");

            // setup Kafka broker properties
            Properties brokerProps = new Properties();
            brokerProps.put("auto.create.topics.enable", String.valueOf(false));

            EmbeddedConnectCluster.Builder connectBuilder = new EmbeddedConnectCluster.Builder()
                    .name("connect-cluster")
                    .numWorkers(numWorkers)
                    .workerProps(workerProps)
                    .brokerProps(brokerProps)
                    // true is the default, setting here as example
                    .maskExitProcedures(true);
            EmbeddedConnectCluster connect = connectBuilder.build();
            // start the clusters
            connect.start();
            return connect;
        });
    }

    @AfterEach
    public void tearDown(TestInfo testInfo) {
        log.info("Finished test {}", testInfo.getDisplayName());
        RuntimeHandles.get().deleteConnector(connectorName);
    }

    @AfterAll
    public static void close() {
        CONNECT_CLUSTERS.values().forEach(EmbeddedConnectCluster::stop);
    }

    @Test
    public void testRestartUnknownConnectorNoParams() {
        String connectorName = "Unknown";

        // build a Connect cluster backed by a Kafka KRaft cluster
        startOrReuseConnectWithNumWorkers(ONE_WORKER);
        // Call the Restart API
        String restartEndpoint = connect.endpointForResource(
                String.format("connectors/%s/restart", connectorName));
        Response response = connect.requestPost(restartEndpoint, "", Collections.emptyMap());
        assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());

    }

    @Test
    public void testRestartUnknownConnector() {
        restartUnknownConnector(false, false);
        restartUnknownConnector(false, true);
        restartUnknownConnector(true, false);
        restartUnknownConnector(true, true);
    }

    private void restartUnknownConnector(boolean onlyFailed, boolean includeTasks) {
        String connectorName = "Unknown";

        // build a Connect cluster backed by a Kafka KRaft cluster
        startOrReuseConnectWithNumWorkers(ONE_WORKER);
        // Call the Restart API
        String restartEndpoint = connect.endpointForResource(
                String.format("connectors/%s/restart?onlyFailed=" + onlyFailed + "&includeTasks=" + includeTasks, connectorName));
        Response response = connect.requestPost(restartEndpoint, "", Collections.emptyMap());
        assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
    }

    @Test
    public void testRunningConnectorAndTasksRestartOnlyConnector() throws Exception {
        runningConnectorAndTasksRestart(false, false, 1, allTasksExpectedRestarts(0), false);
    }

    @Test
    public void testRunningConnectorAndTasksRestartBothConnectorAndTasks() throws Exception {
        runningConnectorAndTasksRestart(false, true, 1, allTasksExpectedRestarts(1), false);
    }

    @Test
    public void testRunningConnectorAndTasksRestartOnlyFailedConnectorNoop() throws Exception {
        runningConnectorAndTasksRestart(true, false, 0, allTasksExpectedRestarts(0), true);
    }

    @Test
    public void testRunningConnectorAndTasksRestartBothConnectorAndTasksNoop() throws Exception {
        runningConnectorAndTasksRestart(true, true, 0, allTasksExpectedRestarts(0), true);
    }

    @Test
    public void testFailedConnectorRestartOnlyConnector() throws Exception {
        failedConnectorRestart(false, false, 1);
    }

    @Test
    public void testFailedConnectorRestartBothConnectorAndTasks() throws Exception {
        failedConnectorRestart(false, true, 1);
    }

    @Test
    public void testFailedConnectorRestartOnlyFailedConnectorAndTasks() throws Exception {
        failedConnectorRestart(true, true, 1);
    }

    @Test
    public void testFailedTasksRestartOnlyConnector() throws Exception {
        failedTasksRestart(false, false, 1, allTasksExpectedRestarts(0), buildAllTasksToFail(), false);
    }

    @Test
    public void testFailedTasksRestartOnlyTasks() throws Exception {
        failedTasksRestart(true, true, 0, allTasksExpectedRestarts(1), buildAllTasksToFail(), false);
    }

    @Test
    public void testFailedTasksRestartWithoutIncludeTasksNoop() throws Exception {
        failedTasksRestart(true, false, 0, allTasksExpectedRestarts(0), buildAllTasksToFail(), true);
    }

    @Test
    public void testFailedTasksRestartBothConnectorAndTasks() throws Exception {
        failedTasksRestart(false, true, 1, allTasksExpectedRestarts(1), buildAllTasksToFail(), false);
    }

    @Test
    public void testOneFailedTasksRestartOnlyOneTasks() throws Exception {
        Set<String> tasksToFail = Collections.singleton(taskId(1));
        failedTasksRestart(true, true, 0, buildExpectedTasksRestarts(tasksToFail), tasksToFail, false);
    }

    @Test
    public void testMultiWorkerRestartOnlyConnector() throws Exception {
        //run two additional workers to ensure that one worker will always be free and not running any tasks or connector instance for this connector
        //we will call restart on that worker and that will test the distributed behavior of the restart API
        int numWorkers = NUM_TASKS + 2;
        runningConnectorAndTasksRestart(false, false, 1, allTasksExpectedRestarts(0), false, numWorkers);
    }

    @Test
    public void testMultiWorkerRestartBothConnectorAndTasks() throws Exception {
        //run 2 additional workers to ensure 1 worker will be free that is not running any tasks or connector instance for this connector
        int numWorkers = NUM_TASKS + 2;
        runningConnectorAndTasksRestart(false, true, 1, allTasksExpectedRestarts(1), false, numWorkers);
    }

    private void runningConnectorAndTasksRestart(boolean onlyFailed, boolean includeTasks, int expectedConnectorRestarts, Map<String, Integer> expectedTasksRestarts, boolean noopRequest) throws Exception {
        runningConnectorAndTasksRestart(onlyFailed, includeTasks, expectedConnectorRestarts, expectedTasksRestarts, noopRequest, ONE_WORKER);
    }

    private void runningConnectorAndTasksRestart(boolean onlyFailed, boolean includeTasks, int expectedConnectorRestarts, Map<String, Integer> expectedTasksRestarts, boolean noopRequest, int numWorkers) throws Exception {
        startOrReuseConnectWithNumWorkers(numWorkers);
        // setup up props for the source connector
        Map<String, String> props = defaultSourceConnectorProps(TOPIC_NAME);
        // Try to start the connector and its single task.
        connect.configureConnector(connectorName, props);

        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(connectorName, NUM_TASKS,
                "Connector tasks are not all in running state.");

        StartsAndStops beforeSnapshot = connectorHandle.startAndStopCounter().countsSnapshot();
        Map<String, StartsAndStops> beforeTasksSnapshot = connectorHandle.tasks().stream().collect(Collectors.toMap(TaskHandle::taskId, task -> task.startAndStopCounter().countsSnapshot()));

        StartAndStopLatch stopLatch = connectorHandle.expectedStops(expectedConnectorRestarts, expectedTasksRestarts, includeTasks);
        StartAndStopLatch startLatch = connectorHandle.expectedStarts(expectedConnectorRestarts, expectedTasksRestarts, includeTasks);
        ConnectorStateInfo connectorStateInfo;
        // Call the Restart API
        if (numWorkers == 1) {
            connectorStateInfo = connect.restartConnectorAndTasks(connectorName, onlyFailed, includeTasks, false);
        } else {
            connectorStateInfo = connect.restartConnectorAndTasks(connectorName, onlyFailed, includeTasks, true);
        }

        if (noopRequest) {
            assertNoRestartingState(connectorStateInfo);
        }

        // Wait for the connector to be stopped
        assertTrue(stopLatch.await(CONNECTOR_SETUP_DURATION_MS, TimeUnit.MILLISECONDS),
                "Failed to stop connector and tasks within "
                        + CONNECTOR_SETUP_DURATION_MS + "ms");

        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(connectorName, NUM_TASKS,
                "Connector tasks are not all in running state.");
        // Expect that the connector has started again
        assertTrue(startLatch.await(CONNECTOR_SETUP_DURATION_MS, TimeUnit.MILLISECONDS),
                "Failed to start connector and tasks within "
                        + CONNECTOR_SETUP_DURATION_MS + "ms");
        StartsAndStops afterSnapshot = connectorHandle.startAndStopCounter().countsSnapshot();

        assertEquals(beforeSnapshot.starts() + expectedConnectorRestarts, afterSnapshot.starts());
        assertEquals(beforeSnapshot.stops() + expectedConnectorRestarts, afterSnapshot.stops());
        connectorHandle.tasks().forEach(t -> {
            StartsAndStops afterTaskSnapshot = t.startAndStopCounter().countsSnapshot();
            if (numWorkers == 1) {
                assertEquals(beforeTasksSnapshot.get(t.taskId()).starts() + expectedTasksRestarts.get(t.taskId()), afterTaskSnapshot.starts());
                assertEquals(beforeTasksSnapshot.get(t.taskId()).stops() + expectedTasksRestarts.get(t.taskId()), afterTaskSnapshot.stops());
            } else {
                //validate tasks stop/start counts only in single worker test because the multi worker rebalance triggers stop/start on task and this make the exact counts unpredictable
                assertTrue(afterTaskSnapshot.starts() >= beforeTasksSnapshot.get(t.taskId()).starts() + expectedTasksRestarts.get(t.taskId()));
                assertTrue(afterTaskSnapshot.stops() >= beforeTasksSnapshot.get(t.taskId()).stops() + expectedTasksRestarts.get(t.taskId()));
            }
        });
    }

    private void failedConnectorRestart(boolean onlyFailed, boolean includeTasks, int expectedConnectorRestarts) throws Exception {
        //as connector is failed we expect 0 task to be started
        Map<String, Integer> expectedTasksStarts = allTasksExpectedRestarts(0);

        // setup up props for the source connector
        Map<String, String> props = defaultSourceConnectorProps(TOPIC_NAME);
        props.put("connector.start.inject.error", "true");
        // build a Connect cluster backed by a Kafka KRaft cluster
        startOrReuseConnectWithNumWorkers(ONE_WORKER);

        // Try to start the connector and its single task.
        connect.configureConnector(connectorName, props);

        connect.assertions().assertConnectorIsFailedAndTasksHaveFailed(connectorName, 0,
                "Connector or tasks are in running state.");

        StartsAndStops beforeSnapshot = connectorHandle.startAndStopCounter().countsSnapshot();

        StartAndStopLatch startLatch = connectorHandle.expectedStarts(expectedConnectorRestarts, expectedTasksStarts, includeTasks);

        // Call the Restart API
        connect.restartConnectorAndTasks(connectorName, onlyFailed, includeTasks, false);

        connect.assertions().assertConnectorIsFailedAndTasksHaveFailed(connectorName, 0,
                "Connector tasks are not all in running state.");
        // Expect that the connector has started again
        assertTrue(startLatch.await(CONNECTOR_SETUP_DURATION_MS, TimeUnit.MILLISECONDS),
                "Failed to start connector and tasks after coordinator failure within "
                        + CONNECTOR_SETUP_DURATION_MS + "ms");
        StartsAndStops afterSnapshot = connectorHandle.startAndStopCounter().countsSnapshot();

        assertEquals(beforeSnapshot.starts() + expectedConnectorRestarts, afterSnapshot.starts());
    }

    private void failedTasksRestart(boolean onlyFailed, boolean includeTasks, int expectedConnectorRestarts, Map<String, Integer> expectedTasksRestarts, Set<String> tasksToFail, boolean noopRequest) throws Exception {
        // setup up props for the source connector
        Map<String, String> props = defaultSourceConnectorProps(TOPIC_NAME);
        tasksToFail.forEach(taskId -> props.put("task-" + taskId + ".start.inject.error", "true"));
        // build a Connect cluster backed by a Kafka KRaft cluster
        startOrReuseConnectWithNumWorkers(ONE_WORKER);

        // Try to start the connector and its single task.
        connect.configureConnector(connectorName, props);

        connect.assertions().assertConnectorIsRunningAndNumTasksHaveFailed(connectorName, NUM_TASKS, tasksToFail.size(),
                "Connector tasks are in running state.");

        StartsAndStops beforeSnapshot = connectorHandle.startAndStopCounter().countsSnapshot();
        Map<String, StartsAndStops> beforeTasksSnapshot = connectorHandle.tasks().stream().collect(Collectors.toMap(TaskHandle::taskId, task -> task.startAndStopCounter().countsSnapshot()));

        StartAndStopLatch stopLatch = connectorHandle.expectedStops(expectedConnectorRestarts, expectedTasksRestarts, includeTasks);
        StartAndStopLatch startLatch = connectorHandle.expectedStarts(expectedConnectorRestarts, expectedTasksRestarts, includeTasks);

        // Call the Restart API
        ConnectorStateInfo connectorStateInfo = connect.restartConnectorAndTasks(connectorName, onlyFailed, includeTasks, false);

        if (noopRequest) {
            assertNoRestartingState(connectorStateInfo);
        }

        // Wait for the connector to be stopped
        assertTrue(stopLatch.await(CONNECTOR_SETUP_DURATION_MS, TimeUnit.MILLISECONDS),
                "Failed to stop connector and tasks within "
                        + CONNECTOR_SETUP_DURATION_MS + "ms");

        connect.assertions().assertConnectorIsRunningAndNumTasksHaveFailed(connectorName, NUM_TASKS, tasksToFail.size(),
                "Connector tasks are not all in running state.");
        // Expect that the connector has started again
        assertTrue(startLatch.await(CONNECTOR_SETUP_DURATION_MS, TimeUnit.MILLISECONDS),
                "Failed to start connector and tasks within "
                        + CONNECTOR_SETUP_DURATION_MS + "ms");

        StartsAndStops afterSnapshot = connectorHandle.startAndStopCounter().countsSnapshot();

        assertEquals(beforeSnapshot.starts() + expectedConnectorRestarts, afterSnapshot.starts());
        assertEquals(beforeSnapshot.stops() + expectedConnectorRestarts, afterSnapshot.stops());
        connectorHandle.tasks().forEach(t -> {
            StartsAndStops afterTaskSnapshot = t.startAndStopCounter().countsSnapshot();
            assertEquals(beforeTasksSnapshot.get(t.taskId()).starts() + expectedTasksRestarts.get(t.taskId()), afterTaskSnapshot.starts());
            assertEquals(beforeTasksSnapshot.get(t.taskId()).stops() + expectedTasksRestarts.get(t.taskId()), afterTaskSnapshot.stops());
        });
    }

    private void assertNoRestartingState(ConnectorStateInfo connectorStateInfo) {
        //for noop requests as everything is in RUNNING state, assert that plan was empty which means
        // no RESTARTING state for the connector or tasks
        assertNotEquals(AbstractStatus.State.RESTARTING.name(), connectorStateInfo.connector().state());
        connectorStateInfo.tasks().forEach(t -> assertNotEquals(AbstractStatus.State.RESTARTING.name(), t.state()));
    }

    private Set<String> buildAllTasksToFail() {
        Set<String> tasksToFail = new HashSet<>();
        for (int i = 0; i < NUM_TASKS; i++) {
            String taskId = taskId(i);
            tasksToFail.add(taskId);
        }
        return tasksToFail;
    }

    private Map<String, Integer> allTasksExpectedRestarts(int expectedRestarts) {
        Map<String, Integer> expectedTasksRestarts = new HashMap<>();
        for (int i = 0; i < NUM_TASKS; i++) {
            String taskId = taskId(i);
            expectedTasksRestarts.put(taskId, expectedRestarts);
        }
        return expectedTasksRestarts;
    }

    private Map<String, Integer> buildExpectedTasksRestarts(Set<String> tasksToFail) {
        Map<String, Integer> expectedTasksRestarts = new HashMap<>();
        for (int i = 0; i < NUM_TASKS; i++) {
            String taskId = taskId(i);
            expectedTasksRestarts.put(taskId, tasksToFail.contains(taskId) ? 1 : 0);
        }
        return expectedTasksRestarts;
    }

    private String taskId(int i) {
        return connectorName + "-" + i;
    }

    private Map<String, String> defaultSourceConnectorProps(String topic) {
        // setup up props for the source connector
        Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR_CLASS_CONFIG, MonitorableSourceConnector.class.getSimpleName());
        props.put(TASKS_MAX_CONFIG, String.valueOf(NUM_TASKS));
        props.put(TOPIC_CONFIG, topic);
        props.put("throughput", "10");
        props.put("messages.per.poll", String.valueOf(MESSAGES_PER_POLL));
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + REPLICATION_FACTOR_CONFIG, String.valueOf(1));
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + PARTITIONS_CONFIG, String.valueOf(1));
        return props;
    }
}
