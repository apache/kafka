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

import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.rest.resources.ConnectorsResource;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.connect.util.clusters.WorkerHandle;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.connect.integration.MonitorableSourceConnector.TOPIC_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.DEFAULT_TOPIC_CREATION_PREFIX;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.PARTITIONS_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.CONNECTOR_CLIENT_POLICY_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.apache.kafka.connect.util.clusters.ConnectAssertions.CONNECTOR_SETUP_DURATION_MS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test simple operations on the workers of a Connect cluster.
 */
@Category(IntegrationTest.class)
public class ConnectWorkerIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(ConnectWorkerIntegrationTest.class);

    private static final int NUM_TOPIC_PARTITIONS = 3;
    private static final long OFFSET_COMMIT_INTERVAL_MS = TimeUnit.SECONDS.toMillis(30);
    private static final int NUM_WORKERS = 3;
    private static final int NUM_TASKS = 4;
    private static final int MESSAGES_PER_POLL = 10;
    private static final String CONNECTOR_NAME = "simple-source";
    private static final String TOPIC_NAME = "test-topic";

    private EmbeddedConnectCluster.Builder connectBuilder;
    private EmbeddedConnectCluster connect;
    private Map<String, String> workerProps;
    private Properties brokerProps;

    @Rule
    public TestRule watcher = ConnectIntegrationTestUtils.newTestWatcher(log);

    @Before
    public void setup() {
        // setup Connect worker properties
        workerProps = new HashMap<>();
        workerProps.put(OFFSET_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(OFFSET_COMMIT_INTERVAL_MS));
        workerProps.put(CONNECTOR_CLIENT_POLICY_CLASS_CONFIG, "All");

        // setup Kafka broker properties
        brokerProps = new Properties();
        brokerProps.put("auto.create.topics.enable", String.valueOf(false));

        // build a Connect cluster backed by Kafka and Zk
        connectBuilder = new EmbeddedConnectCluster.Builder()
                .name("connect-cluster")
                .numWorkers(NUM_WORKERS)
                .workerProps(workerProps)
                .brokerProps(brokerProps)
                .maskExitProcedures(true); // true is the default, setting here as example
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
        connect = connectBuilder.build();
        // start the clusters
        connect.start();

        // create test topic
        connect.kafka().createTopic(TOPIC_NAME, NUM_TOPIC_PARTITIONS);

        // set up props for the source connector
        Map<String, String> props = defaultSourceConnectorProps(TOPIC_NAME);

        connect.assertions().assertAtLeastNumWorkersAreUp(NUM_WORKERS,
                "Initial group of workers did not start in time.");

        // start a source connector
        connect.configureConnector(CONNECTOR_NAME, props);

        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, NUM_TASKS,
                "Connector tasks did not start in time.");

        WorkerHandle extraWorker = connect.addWorker();

        connect.assertions().assertAtLeastNumWorkersAreUp(NUM_WORKERS + 1,
                "Expanded group of workers did not start in time.");

        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, NUM_TASKS,
                "Connector tasks are not all in running state.");

        Set<WorkerHandle> workers = connect.activeWorkers();
        assertTrue(workers.contains(extraWorker));

        connect.removeWorker(extraWorker);

        connect.assertions().assertExactlyNumWorkersAreUp(NUM_WORKERS,
                "Group of workers did not shrink in time.");

        workers = connect.activeWorkers();
        assertFalse(workers.contains(extraWorker));
    }

    /**
     * Verify that a failed task can be restarted successfully.
     */
    @Test
    public void testRestartFailedTask() throws Exception {
        connect = connectBuilder.build();
        // start the clusters
        connect.start();

        int numTasks = 1;

        // setup up props for the source connector
        Map<String, String> props = defaultSourceConnectorProps(TOPIC_NAME);
        // Properties for the source connector. The task should fail at startup due to the bad broker address.
        props.put(TASKS_MAX_CONFIG, Objects.toString(numTasks));
        props.put(CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX + BOOTSTRAP_SERVERS_CONFIG, "nobrokerrunningatthisaddress");

        connect.assertions().assertExactlyNumWorkersAreUp(NUM_WORKERS,
                "Initial group of workers did not start in time.");

        // Try to start the connector and its single task.
        connect.configureConnector(CONNECTOR_NAME, props);

        connect.assertions().assertConnectorIsRunningAndTasksHaveFailed(CONNECTOR_NAME, numTasks,
                "Connector tasks did not fail in time");

        // Reconfigure the connector without the bad broker address.
        props.remove(CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX + BOOTSTRAP_SERVERS_CONFIG);
        connect.configureConnector(CONNECTOR_NAME, props);

        // Restart the failed task
        String taskRestartEndpoint = connect.endpointForResource(
            String.format("connectors/%s/tasks/0/restart", CONNECTOR_NAME));
        connect.requestPost(taskRestartEndpoint, "", Collections.emptyMap());

        // Ensure the task started successfully this time
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, numTasks,
            "Connector tasks are not all in running state.");
    }

    /**
     * Verify that a set of tasks restarts correctly after a broker goes offline and back online
     */
    @Test
    public void testBrokerCoordinator() throws Exception {
        ConnectorHandle connectorHandle = RuntimeHandles.get().connectorHandle(CONNECTOR_NAME);
        workerProps.put(DistributedConfig.SCHEDULED_REBALANCE_MAX_DELAY_MS_CONFIG, String.valueOf(5000));
        connect = connectBuilder.workerProps(workerProps).build();
        // start the clusters
        connect.start();
        int numTasks = 4;
        // create test topic
        connect.kafka().createTopic(TOPIC_NAME, NUM_TOPIC_PARTITIONS);

        // set up props for the source connector
        Map<String, String> props = defaultSourceConnectorProps(TOPIC_NAME);

        connect.assertions().assertAtLeastNumWorkersAreUp(NUM_WORKERS,
                "Initial group of workers did not start in time.");

        // start a source connector
        connect.configureConnector(CONNECTOR_NAME, props);

        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, numTasks,
                "Connector tasks did not start in time.");

        // expect that the connector will be stopped once the coordinator is detected to be down
        StartAndStopLatch stopLatch = connectorHandle.expectedStops(1, false);

        connect.kafka().stopOnlyKafka();

        connect.assertions().assertExactlyNumWorkersAreUp(NUM_WORKERS,
                "Group of workers did not remain the same after broker shutdown");

        // Allow for the workers to discover that the coordinator is unavailable, wait is
        // heartbeat timeout * 2 + 4sec
        Thread.sleep(TimeUnit.SECONDS.toMillis(10));

        // Wait for the connector to be stopped
        assertTrue("Failed to stop connector and tasks after coordinator failure within "
                        + CONNECTOR_SETUP_DURATION_MS + "ms",
                stopLatch.await(CONNECTOR_SETUP_DURATION_MS, TimeUnit.MILLISECONDS));

        StartAndStopLatch startLatch = connectorHandle.expectedStarts(1, false);
        connect.kafka().startOnlyKafkaOnSamePorts();

        // Allow for the kafka brokers to come back online
        Thread.sleep(TimeUnit.SECONDS.toMillis(10));

        connect.assertions().assertExactlyNumWorkersAreUp(NUM_WORKERS,
                "Group of workers did not remain the same within the designated time.");

        // Allow for the workers to rebalance and reach a steady state
        Thread.sleep(TimeUnit.SECONDS.toMillis(10));

        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, numTasks,
                "Connector tasks did not start in time.");

        // Expect that the connector has started again
        assertTrue("Failed to stop connector and tasks after coordinator failure within "
                        + CONNECTOR_SETUP_DURATION_MS + "ms",
                startLatch.await(CONNECTOR_SETUP_DURATION_MS, TimeUnit.MILLISECONDS));
    }

    /**
     * Verify that the number of tasks listed in the REST API is updated correctly after changes to
     * the "tasks.max" connector configuration.
     */
    @Test
    public void testTaskStatuses() throws Exception {
        connect = connectBuilder.build();
        // start the clusters
        connect.start();

        connect.assertions().assertAtLeastNumWorkersAreUp(NUM_WORKERS,
                "Initial group of workers did not start in time.");

        // base connector props
        Map<String, String> props = defaultSourceConnectorProps(TOPIC_NAME);
        props.put(CONNECTOR_CLASS_CONFIG, MonitorableSourceConnector.class.getSimpleName());

        // start the connector with only one task
        int initialNumTasks = 1;
        props.put(TASKS_MAX_CONFIG, String.valueOf(initialNumTasks));
        connect.configureConnector(CONNECTOR_NAME, props);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(CONNECTOR_NAME,
                initialNumTasks, "Connector tasks did not start in time");

        // then reconfigure it to use more tasks
        int increasedNumTasks = 5;
        props.put(TASKS_MAX_CONFIG, String.valueOf(increasedNumTasks));
        connect.configureConnector(CONNECTOR_NAME, props);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(CONNECTOR_NAME,
                increasedNumTasks, "Connector task statuses did not update in time.");

        // then reconfigure it to use fewer tasks
        int decreasedNumTasks = 3;
        props.put(TASKS_MAX_CONFIG, String.valueOf(decreasedNumTasks));
        connect.configureConnector(CONNECTOR_NAME, props);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(CONNECTOR_NAME,
                decreasedNumTasks, "Connector task statuses did not update in time.");
    }

    @Test
    public void testSourceTaskNotBlockedOnShutdownWithNonExistentTopic() throws Exception {
        // When automatic topic creation is disabled on the broker
        brokerProps.put("auto.create.topics.enable", "false");
        connect = connectBuilder
            .brokerProps(brokerProps)
            .numWorkers(1)
            .numBrokers(1)
            .build();
        connect.start();

        connect.assertions().assertAtLeastNumWorkersAreUp(1, "Initial group of workers did not start in time.");

        // and when the connector is not configured to create topics
        Map<String, String> props = defaultSourceConnectorProps("nonexistenttopic");
        props.remove(DEFAULT_TOPIC_CREATION_PREFIX + REPLICATION_FACTOR_CONFIG);
        props.remove(DEFAULT_TOPIC_CREATION_PREFIX + PARTITIONS_CONFIG);
        props.put("throughput", "-1");

        ConnectorHandle connector = RuntimeHandles.get().connectorHandle(CONNECTOR_NAME);
        connector.expectedRecords(NUM_TASKS * MESSAGES_PER_POLL);
        connect.configureConnector(CONNECTOR_NAME, props);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(CONNECTOR_NAME,
            NUM_TASKS, "Connector tasks did not start in time");
        connector.awaitRecords(TimeUnit.MINUTES.toMillis(1));

        // Then if we delete the connector, it and each of its tasks should be stopped by the framework
        // even though the producer is blocked because there is no topic
        StartAndStopLatch stopCounter = connector.expectedStops(1);
        connect.deleteConnector(CONNECTOR_NAME);

        assertTrue("Connector and all tasks were not stopped in time", stopCounter.await(1, TimeUnit.MINUTES));
    }

    /**
     * Verify that the target state (started, paused, stopped) of a connector can be updated, with
     * an emphasis on ensuring that the transitions between each state are correct.
     * <p>
     * The transitions we need to cover are:
     * <ol>
     *     <li>RUNNING -> PAUSED</li>
     *     <li>RUNNING -> STOPPED</li>
     *     <li>PAUSED -> RUNNING</li>
     *     <li>PAUSED -> STOPPED</li>
     *     <li>STOPPED -> RUNNING</li>
     *     <li>STOPPED -> PAUSED</li>
     * </ol>
     * With some reordering, we can perform each transition just once:
     * <ul>
     *     <li>Start with RUNNING</li>
     *     <li>Transition to STOPPED (2)</li>
     *     <li>Transition to RUNNING (5)</li>
     *     <li>Transition to PAUSED (1)</li>
     *     <li>Transition to STOPPED (4)</li>
     *     <li>Transition to PAUSED (6)</li>
     *     <li>Transition to RUNNING (3)</li>
     * </ul>
     */
    @Test
    public void testPauseStopResume() throws Exception {
        connect = connectBuilder.build();
        // start the clusters
        connect.start();

        connect.assertions().assertAtLeastNumWorkersAreUp(NUM_WORKERS,
                "Initial group of workers did not start in time.");

        // Want to make sure to use multiple tasks
        final int numTasks = 4;
        Map<String, String> props = defaultSourceConnectorProps(TOPIC_NAME);
        props.put(TASKS_MAX_CONFIG, Integer.toString(numTasks));

        // Start with RUNNING
        connect.configureConnector(CONNECTOR_NAME, props);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(
                CONNECTOR_NAME,
                numTasks,
                "Connector tasks did not start in time"
        );

        // Transition to STOPPED
        connect.stopConnector(CONNECTOR_NAME);
        // Issue a second request to ensure that this operation is idempotent
        connect.stopConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorIsStopped(
                CONNECTOR_NAME,
                "Connector did not stop in time"
        );
        // If the connector is truly stopped, we should also see an empty set of tasks and task configs
        assertEquals(Collections.emptyList(), connect.connectorInfo(CONNECTOR_NAME).tasks());
        assertEquals(Collections.emptyList(), connect.taskConfigs(CONNECTOR_NAME));

        // Transition to RUNNING
        connect.resumeConnector(CONNECTOR_NAME);
        // Issue a second request to ensure that this operation is idempotent
        connect.resumeConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(
                CONNECTOR_NAME,
                numTasks,
                "Connector tasks did not resume in time"
        );

        // Transition to PAUSED
        connect.pauseConnector(CONNECTOR_NAME);
        // Issue a second request to ensure that this operation is idempotent
        connect.pauseConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorAndExactlyNumTasksArePaused(
                CONNECTOR_NAME,
                numTasks,
                "Connector did not pause in time"
        );

        // Transition to STOPPED
        connect.stopConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorIsStopped(
                CONNECTOR_NAME,
                "Connector did not stop in time"
        );
        assertEquals(Collections.emptyList(), connect.connectorInfo(CONNECTOR_NAME).tasks());
        assertEquals(Collections.emptyList(), connect.taskConfigs(CONNECTOR_NAME));

        // Transition to PAUSED
        connect.pauseConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorAndExactlyNumTasksArePaused(
                CONNECTOR_NAME,
                0,
                "Connector did not pause in time"
        );

        // Transition to RUNNING
        connect.resumeConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(
                CONNECTOR_NAME,
                numTasks,
                "Connector tasks did not resume in time"
        );

        // Delete the connector
        connect.deleteConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorDoesNotExist(
                CONNECTOR_NAME,
                "Connector wasn't deleted in time"
        );
    }

    /**
     * Test out the {@code STOPPED} state introduced in
     * <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-875%3A+First-class+offsets+support+in+Kafka+Connect#KIP875:FirstclassoffsetssupportinKafkaConnect-Newtargetstate:STOPPED">KIP-875</a>,
     * with an emphasis on correctly handling errors thrown from the connector.
     */
    @Test
    public void testStoppedState() throws Exception {
        connect = connectBuilder.build();
        // start the clusters
        connect.start();

        connect.assertions().assertAtLeastNumWorkersAreUp(NUM_WORKERS,
                "Initial group of workers did not start in time.");

        Map<String, String> props = defaultSourceConnectorProps(TOPIC_NAME);
        // Fail the connector on startup
        props.put("connector.start.inject.error", "true");

        // Start the connector (should fail immediately and generate no tasks)
        connect.configureConnector(CONNECTOR_NAME, props);
        connect.assertions().assertConnectorIsFailedAndTasksHaveFailed(
                CONNECTOR_NAME,
                0,
                "Connector should have failed and not generated any tasks"
        );

        // Stopping a failed connector updates its state to STOPPED in the REST API
        connect.stopConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorIsStopped(
                CONNECTOR_NAME,
                "Connector did not stop in time"
        );
        // If the connector is truly stopped, we should also see an empty set of tasks and task configs
        assertEquals(Collections.emptyList(), connect.connectorInfo(CONNECTOR_NAME).tasks());
        assertEquals(Collections.emptyList(), connect.taskConfigs(CONNECTOR_NAME));

        // Can resume a connector after its Connector has failed before shutdown after receiving a stop request
        props.remove("connector.start.inject.error");
        connect.configureConnector(CONNECTOR_NAME, props);
        connect.resumeConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(
                CONNECTOR_NAME,
                NUM_TASKS,
                "Connector or tasks did not start running healthily in time"
        );

        // Fail the connector on shutdown
        props.put("connector.stop.inject.error", "true");
        // Stopping a connector that fails during shutdown after receiving a stop request updates its state to STOPPED in the REST API
        connect.configureConnector(CONNECTOR_NAME, props);
        connect.stopConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorIsStopped(
                CONNECTOR_NAME,
                "Connector did not stop in time"
        );
        assertEquals(Collections.emptyList(), connect.connectorInfo(CONNECTOR_NAME).tasks());
        assertEquals(Collections.emptyList(), connect.taskConfigs(CONNECTOR_NAME));

        // Can resume a connector after its Connector has failed during shutdown after receiving a stop request
        connect.resumeConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(
                CONNECTOR_NAME,
                NUM_TASKS,
                "Connector or tasks did not start running healthily in time"
        );

        // Can delete a stopped connector
        connect.deleteConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorDoesNotExist(
                CONNECTOR_NAME,
                "Connector wasn't deleted in time"
        );
    }

    /**
     * The <strong><em>GET /connectors/{connector}/tasks-config</em></strong> endpoint was deprecated in
     * <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-970%3A+Deprecate+and+remove+Connect%27s+redundant+task+configurations+endpoint">KIP-970</a>
     * and is slated for removal in the next major release. This test verifies that the deprecation warning log is emitted on trying to use the
     * deprecated endpoint.
     */
    @Test
    public void testTasksConfigDeprecation() throws Exception {
        connect = connectBuilder.build();
        // start the clusters
        connect.start();

        connect.assertions().assertAtLeastNumWorkersAreUp(NUM_WORKERS,
            "Initial group of workers did not start in time.");

        connect.configureConnector(CONNECTOR_NAME, defaultSourceConnectorProps(TOPIC_NAME));
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(
            CONNECTOR_NAME,
            NUM_TASKS,
            "Connector tasks did not start in time"
        );

        try (LogCaptureAppender logCaptureAppender = LogCaptureAppender.createAndRegister(ConnectorsResource.class)) {
            connect.requestGet(connect.endpointForResource("connectors/" + CONNECTOR_NAME + "/tasks-config"));
            List<LogCaptureAppender.Event> logEvents = logCaptureAppender.getEvents();
            assertEquals(1, logEvents.size());
            assertEquals(Level.WARN.toString(), logEvents.get(0).getLevel());
            assertThat(logEvents.get(0).getMessage(), containsString("deprecated"));
        }
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
