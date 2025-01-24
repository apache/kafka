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

import org.apache.kafka.clients.MetadataRecoveryStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.provider.FileConfigProvider;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedHerder;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorOffset;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorOffsets;
import org.apache.kafka.connect.runtime.rest.entities.CreateConnectorRequest;
import org.apache.kafka.connect.runtime.rest.errors.ConnectRestException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.storage.KafkaConfigBackingStore;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.transforms.Filter;
import org.apache.kafka.connect.transforms.predicates.RecordIsTombstone;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.SinkUtils;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.connect.util.clusters.WorkerHandle;
import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import jakarta.ws.rs.core.Response;

import static jakarta.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.METADATA_RECOVERY_STRATEGY_CONFIG;
import static org.apache.kafka.common.config.AbstractConfig.CONFIG_PROVIDERS_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.DELETE_RETENTION_MS_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.SEGMENT_MS_CONFIG;
import static org.apache.kafka.connect.integration.BlockingConnectorTest.TASK_STOP;
import static org.apache.kafka.connect.integration.MonitorableSourceConnector.TOPIC_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX;
import static org.apache.kafka.connect.runtime.ConnectorConfig.HEADER_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.PREDICATES_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_ENFORCE_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TRANSFORMS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.TOPICS_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.DEFAULT_TOPIC_CREATION_PREFIX;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.PARTITIONS_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.CONNECTOR_CLIENT_POLICY_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.CONFIG_STORAGE_PREFIX;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.CONFIG_TOPIC_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.REBALANCE_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.SCHEDULED_REBALANCE_MAX_DELAY_MS_CONFIG;
import static org.apache.kafka.connect.util.clusters.ConnectAssertions.CONNECTOR_SETUP_DURATION_MS;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test simple operations on the workers of a Connect cluster.
 */
@Tag("integration")
public class ConnectWorkerIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(ConnectWorkerIntegrationTest.class);

    private static final int NUM_TOPIC_PARTITIONS = 3;
    private static final long RECORD_TRANSFER_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(60);
    private static final long OFFSET_COMMIT_INTERVAL_MS = TimeUnit.SECONDS.toMillis(30);
    private static final int NUM_WORKERS = 3;
    private static final int NUM_TASKS = 4;
    private static final int MESSAGES_PER_POLL = 10;
    private static final String CONNECTOR_NAME = "simple-connector";
    private static final String TOPIC_NAME = "test-topic";

    private EmbeddedConnectCluster.Builder connectBuilder;
    private EmbeddedConnectCluster connect;
    private Map<String, String> workerProps;
    private Properties brokerProps;

    @BeforeEach
    public void setup(TestInfo testInfo) {
        log.info("Starting test {}", testInfo.getDisplayName());
        // setup Connect worker properties
        workerProps = new HashMap<>();
        workerProps.put(OFFSET_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(OFFSET_COMMIT_INTERVAL_MS));
        workerProps.put(CONNECTOR_CLIENT_POLICY_CLASS_CONFIG, "All");

        // setup Kafka broker properties
        brokerProps = new Properties();
        brokerProps.put("auto.create.topics.enable", String.valueOf(false));

        // build a Connect cluster backed by a Kafka KRaft cluster
        connectBuilder = new EmbeddedConnectCluster.Builder()
                .name("connect-cluster")
                .numWorkers(NUM_WORKERS)
                .workerProps(workerProps)
                .brokerProps(brokerProps)
                .maskExitProcedures(true); // true is the default, setting here as example
    }

    @AfterEach
    public void close(TestInfo testInfo) {
        log.info("Finished test {}", testInfo.getDisplayName());
        // stop the Connect cluster and its backing Kafka cluster.
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

        // start a source connector
        connect.configureConnector(CONNECTOR_NAME, props);

        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, NUM_TASKS,
                "Connector tasks did not start in time.");

        WorkerHandle extraWorker = connect.addWorker();

        connect.assertions().assertAtLeastNumWorkersAreUp(NUM_WORKERS + 1,
                "Expanded group of workers did not start in time.");

        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, NUM_TASKS,
                "Connector tasks are not all in running state.");

        Set<WorkerHandle> workers = connect.healthyWorkers();
        assertTrue(workers.contains(extraWorker));

        connect.removeWorker(extraWorker);

        connect.assertions().assertExactlyNumWorkersAreUp(NUM_WORKERS,
                "Group of workers did not shrink in time.");

        workers = connect.healthyWorkers();
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

        useFixedBrokerPort();

        // start the clusters
        connect = connectBuilder.build();
        connect.start();
        int numTasks = 4;
        // create test topic
        connect.kafka().createTopic(TOPIC_NAME, NUM_TOPIC_PARTITIONS);

        // set up props for the source connector
        Map<String, String> props = defaultSourceConnectorProps(TOPIC_NAME);

        // start a source connector
        connect.configureConnector(CONNECTOR_NAME, props);

        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, numTasks,
                "Connector tasks did not start in time.");

        // expect that the connector will be stopped once the coordinator is detected to be down
        StartAndStopLatch stopLatch = connectorHandle.expectedStops(1, false);

        connect.kafka().stopOnlyBrokers();

        // Allow for the workers to discover that the coordinator is unavailable, wait is
        // heartbeat timeout * 2 + 4sec
        Thread.sleep(TimeUnit.SECONDS.toMillis(10));

        connect.requestTimeout(1000);
        assertFalse(
                connect.anyWorkersHealthy(),
                "No workers should be healthy when underlying Kafka cluster is down"
        );
        connect.workers().forEach(worker -> {
            try (Response response = connect.healthCheck(worker)) {
                assertEquals(INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
                assertNotNull(response.getEntity());
                String body = response.getEntity().toString();
                String expectedSubstring = "Worker was unable to handle this request and may be unable to handle other requests";
                assertTrue(
                        body.contains(expectedSubstring),
                        "Response body '" + body + "' did not contain expected message '" + expectedSubstring + "'"
                );
            }
        });
        connect.resetRequestTimeout();

        // Wait for the connector to be stopped
        assertTrue(stopLatch.await(CONNECTOR_SETUP_DURATION_MS, TimeUnit.MILLISECONDS),
                "Failed to stop connector and tasks after coordinator failure within "
                        + CONNECTOR_SETUP_DURATION_MS + "ms");

        StartAndStopLatch startLatch = connectorHandle.expectedStarts(1, false);
        connect.kafka().restartOnlyBrokers();

        // Allow for the kafka brokers to come back online
        Thread.sleep(TimeUnit.SECONDS.toMillis(10));

        connect.assertions().assertExactlyNumWorkersAreUp(NUM_WORKERS,
                "Group of workers did not remain the same within the designated time.");

        // Allow for the workers to rebalance and reach a steady state
        Thread.sleep(TimeUnit.SECONDS.toMillis(10));

        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, numTasks,
                "Connector tasks did not start in time.");

        // Expect that the connector has started again
        assertTrue(startLatch.await(CONNECTOR_SETUP_DURATION_MS, TimeUnit.MILLISECONDS),
                "Failed to stop connector and tasks after coordinator failure within "
                        + CONNECTOR_SETUP_DURATION_MS + "ms");
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

        assertTrue(stopCounter.await(1, TimeUnit.MINUTES), "Connector and all tasks were not stopped in time");
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

    @Test
    public void testCreateConnectorWithPausedInitialState() throws Exception {
        connect = connectBuilder.build();
        // start the clusters
        connect.start();

        CreateConnectorRequest createConnectorRequest = new CreateConnectorRequest(
            CONNECTOR_NAME,
            defaultSourceConnectorProps(TOPIC_NAME),
            CreateConnectorRequest.InitialState.PAUSED
        );
        connect.configureConnector(createConnectorRequest);

        // Verify that the connector's status is PAUSED and also that no tasks were spawned for the connector
        connect.assertions().assertConnectorAndExactlyNumTasksArePaused(
            CONNECTOR_NAME,
            0,
            "Connector was not created in a paused state"
        );
        assertEquals(Collections.emptyList(), connect.connectorInfo(CONNECTOR_NAME).tasks());
        assertEquals(Collections.emptyList(), connect.taskConfigs(CONNECTOR_NAME));

        // Verify that a connector created in the PAUSED state can be resumed successfully
        connect.resumeConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(
            CONNECTOR_NAME,
            NUM_TASKS,
            "Connector or tasks did not start running healthily in time"
        );
    }

    @Test
    public void testCreateSourceConnectorWithStoppedInitialStateAndModifyOffsets() throws Exception {
        connect = connectBuilder.build();
        // start the clusters
        connect.start();

        Map<String, String> props = defaultSourceConnectorProps(TOPIC_NAME);

        // Configure the connector to produce a maximum of 10 messages
        props.put("max.messages", "10");
        props.put(TASKS_MAX_CONFIG, "1");
        CreateConnectorRequest createConnectorRequest = new CreateConnectorRequest(
            CONNECTOR_NAME,
            props,
            CreateConnectorRequest.InitialState.STOPPED
        );
        connect.configureConnector(createConnectorRequest);

        // Verify that the connector's status is STOPPED and also that no tasks were spawned for the connector
        connect.assertions().assertConnectorIsStopped(
            CONNECTOR_NAME,
            "Connector was not created in a stopped state"
        );
        assertEquals(Collections.emptyList(), connect.connectorInfo(CONNECTOR_NAME).tasks());
        assertEquals(Collections.emptyList(), connect.taskConfigs(CONNECTOR_NAME));

        // Verify that the offsets can be modified for a source connector created in the STOPPED state

        // Alter the offsets so that only 5 messages are produced
        connect.alterSourceConnectorOffset(
            CONNECTOR_NAME,
            Collections.singletonMap("task.id", CONNECTOR_NAME + "-0"),
            Collections.singletonMap("saved", 5L)
        );

        // Verify that a connector created in the STOPPED state can be resumed successfully
        connect.resumeConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(
            CONNECTOR_NAME,
            1,
            "Connector or tasks did not start running healthily in time"
        );

        // Verify that only 5 messages were produced. We verify this by consuming all the messages from the topic after we've already ensured that at
        // least 5 messages can be consumed.
        long timeoutMs = TimeUnit.SECONDS.toMillis(10);
        connect.kafka().consume(5, timeoutMs, TOPIC_NAME);
        assertEquals(5, connect.kafka().consumeAll(timeoutMs, TOPIC_NAME).count());
    }

    @Test
    public void testCreateSinkConnectorWithStoppedInitialStateAndModifyOffsets() throws Exception {
        connect = connectBuilder.build();
        // start the clusters
        connect.start();

        // Create topic and produce 10 messages
        connect.kafka().createTopic(TOPIC_NAME);
        for (int i = 0; i < 10; i++) {
            connect.kafka().produce(TOPIC_NAME, "Message " + i);
        }

        Map<String, String> props = defaultSinkConnectorProps(TOPIC_NAME);
        props.put(TASKS_MAX_CONFIG, "1");

        CreateConnectorRequest createConnectorRequest = new CreateConnectorRequest(
            CONNECTOR_NAME,
            props,
            CreateConnectorRequest.InitialState.STOPPED
        );
        connect.configureConnector(createConnectorRequest);

        // Verify that the connector's status is STOPPED and also that no tasks were spawned for the connector
        connect.assertions().assertConnectorIsStopped(
            CONNECTOR_NAME,
            "Connector was not created in a stopped state"
        );
        assertEquals(Collections.emptyList(), connect.connectorInfo(CONNECTOR_NAME).tasks());
        assertEquals(Collections.emptyList(), connect.taskConfigs(CONNECTOR_NAME));

        // Verify that the offsets can be modified for a sink connector created in the STOPPED state

        // Alter the offsets so that the first 5 messages in the topic are skipped
        connect.alterSinkConnectorOffset(CONNECTOR_NAME, new TopicPartition(TOPIC_NAME, 0), 5L);

        // This will cause the connector task to fail if it encounters a record with offset < 5
        TaskHandle taskHandle = RuntimeHandles.get().connectorHandle(CONNECTOR_NAME).taskHandle(CONNECTOR_NAME + "-0",
            sinkRecord -> {
                if (sinkRecord.kafkaOffset() < 5L) {
                    throw new ConnectException("Unexpected record encountered: " + sinkRecord);
                }
            });

        // We produced 10 records and altered the connector offsets to skip over the first 5, so we expect 5 records to be consumed
        taskHandle.expectedRecords(5);

        // Verify that a connector created in the STOPPED state can be resumed successfully
        connect.resumeConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(
            CONNECTOR_NAME,
            1,
            "Connector or tasks did not start running healthily in time"
        );

        taskHandle.awaitRecords(TimeUnit.SECONDS.toMillis(10));

        // Confirm that the task is still running (i.e. it didn't fail due to encountering any records with offset < 5)
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(
            CONNECTOR_NAME,
            1,
            "Connector or tasks did not start running healthily in time"
        );
    }

    @Test
    public void testDeleteConnectorCreatedWithPausedOrStoppedInitialState() throws Exception {
        connect = connectBuilder.build();
        // start the clusters
        connect.start();

        // Create a connector with PAUSED initial state
        CreateConnectorRequest createConnectorRequest = new CreateConnectorRequest(
            CONNECTOR_NAME,
            defaultSourceConnectorProps(TOPIC_NAME),
            CreateConnectorRequest.InitialState.PAUSED
        );
        connect.configureConnector(createConnectorRequest);

        // Verify that the connector's status is PAUSED and also that no tasks were spawned for the connector
        connect.assertions().assertConnectorAndExactlyNumTasksArePaused(
            CONNECTOR_NAME,
            0,
            "Connector was not created in a paused state"
        );
        assertEquals(Collections.emptyList(), connect.connectorInfo(CONNECTOR_NAME).tasks());
        assertEquals(Collections.emptyList(), connect.taskConfigs(CONNECTOR_NAME));

        // Verify that a connector created in the PAUSED state can be deleted successfully
        connect.deleteConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorDoesNotExist(CONNECTOR_NAME, "Connector wasn't deleted in time");


        // Create a connector with STOPPED initial state
        createConnectorRequest = new CreateConnectorRequest(
            CONNECTOR_NAME,
            defaultSourceConnectorProps(TOPIC_NAME),
            CreateConnectorRequest.InitialState.STOPPED
        );
        connect.configureConnector(createConnectorRequest);

        // Verify that the connector's status is STOPPED and also that no tasks were spawned for the connector
        connect.assertions().assertConnectorIsStopped(
            CONNECTOR_NAME,
            "Connector was not created in a stopped state"
        );
        assertEquals(Collections.emptyList(), connect.connectorInfo(CONNECTOR_NAME).tasks());
        assertEquals(Collections.emptyList(), connect.taskConfigs(CONNECTOR_NAME));

        // Verify that a connector created in the STOPPED state can be deleted successfully
        connect.deleteConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorDoesNotExist(CONNECTOR_NAME, "Connector wasn't deleted in time");
    }

    @Test
    public void testPatchConnectorConfig() throws Exception {
        connect = connectBuilder.build();
        // start the clusters
        connect.start();

        connect.kafka().createTopic(TOPIC_NAME);

        Map<String, String> props = defaultSinkConnectorProps(TOPIC_NAME);
        props.put("unaffected-key", "unaffected-value");
        props.put("to-be-deleted-key", "value");
        props.put(TASKS_MAX_CONFIG, "2");

        Map<String, String> patch = new HashMap<>();
        patch.put(TASKS_MAX_CONFIG, "3");  // this plays as a value to be changed
        patch.put("to-be-added-key", "value");
        patch.put("to-be-deleted-key", null);

        connect.configureConnector(CONNECTOR_NAME, props);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(CONNECTOR_NAME, 2,
                "connector and tasks did not start in time");

        connect.patchConnectorConfig(CONNECTOR_NAME, patch);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(CONNECTOR_NAME, 3,
                "connector and tasks did not reconfigure and restart in time");

        Map<String, String> expectedConfig = new HashMap<>(props);
        expectedConfig.put("name", CONNECTOR_NAME);
        expectedConfig.put("to-be-added-key", "value");
        expectedConfig.put(TASKS_MAX_CONFIG, "3");
        expectedConfig.remove("to-be-deleted-key");
        assertEquals(expectedConfig, connect.connectorInfo(CONNECTOR_NAME).config());
    }

    private Map<String, String> defaultSinkConnectorProps(String topics) {
        // setup props for the sink connector
        Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR_CLASS_CONFIG, MonitorableSinkConnector.class.getSimpleName());
        props.put(TASKS_MAX_CONFIG, String.valueOf(NUM_TASKS));
        props.put(TOPICS_CONFIG, topics);

        return props;
    }

    @Test
    public void testRequestTimeouts() throws Exception {
        final String configTopic = "test-request-timeout-configs";
        workerProps.put(CONFIG_TOPIC_CONFIG, configTopic);
        // Workaround for KAFKA-15676, which can cause the scheduled rebalance delay to
        // be spuriously triggered after the group coordinator for a Connect cluster is bounced
        workerProps.put(SCHEDULED_REBALANCE_MAX_DELAY_MS_CONFIG, "0");
        workerProps.put(METADATA_RECOVERY_STRATEGY_CONFIG, MetadataRecoveryStrategy.NONE.name);

        useFixedBrokerPort();

        connect = connectBuilder
                .numWorkers(1)
                .build();
        connect.start();

        Map<String, String> connectorConfig1 = defaultSourceConnectorProps(TOPIC_NAME);
        Map<String, String> connectorConfig2 = new HashMap<>(connectorConfig1);
        connectorConfig2.put(TASKS_MAX_CONFIG, Integer.toString(NUM_TASKS + 1));

        // Create a connector to ensure that the worker has completed startup
        log.info("Creating initial connector");
        connect.configureConnector(CONNECTOR_NAME, connectorConfig1);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(
                CONNECTOR_NAME, NUM_TASKS, "connector and tasks did not start in time"
        );

        // Bring down Kafka, which should cause some REST requests to fail
        log.info("Stopping Kafka cluster");
        connect.kafka().stopOnlyBrokers();

        // Try to reconfigure the connector, which should fail with a timeout error
        log.info("Trying to reconfigure connector while Kafka cluster is down");
        assertTimeoutException(
                () -> connect.configureConnector(CONNECTOR_NAME, connectorConfig2),
                "flushing updates to the status topic",
                true
        );
        log.info("Restarting Kafka cluster");
        connect.kafka().restartOnlyBrokers();
        connect.assertions().assertExactlyNumBrokersAreUp(1, "Broker did not complete startup in time");
        log.info("Kafka cluster is restarted");

        // Reconfigure the connector to ensure that the broker has completed startup
        log.info("Reconfiguring connector with one more task");
        connect.configureConnector(CONNECTOR_NAME, connectorConfig2);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(
                CONNECTOR_NAME, NUM_TASKS + 1, "connector and tasks did not start in time"
        );

        // Delete the config topic--WCGW?
        log.info("Deleting Kafka Connect config topic");
        connect.kafka().deleteTopic(configTopic);

        // Try to reconfigure the connector, which should fail with a slightly-different timeout error
        log.info("Trying to reconfigure connector after config topic has been deleted");
        assertTimeoutException(
                () -> connect.configureConnector(CONNECTOR_NAME, connectorConfig1),
                "writing a config for connector " + CONNECTOR_NAME + " to the config topic",
                true
        );

        // The worker should still be blocked on the same operation, and the timeout should occur
        // immediately
        log.info("Trying to delete connector after config topic has been deleted");
        assertTimeoutException(
                () -> connect.deleteConnector(CONNECTOR_NAME),
                "writing a config for connector " + CONNECTOR_NAME + " to the config topic",
                false
        );
    }

    @Test
    public void testPollTimeoutExpiry() throws Exception {
        // This is a fabricated test to ensure that a poll timeout expiry happens. The tick thread awaits on
        // task#stop method which is blocked. The timeouts have been set accordingly
        workerProps.put(REBALANCE_TIMEOUT_MS_CONFIG, Long.toString(TimeUnit.SECONDS.toMillis(20)));
        workerProps.put(TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_CONFIG, Long.toString(TimeUnit.SECONDS.toMillis(30)));
        connect = connectBuilder
            .numBrokers(1)
            .numWorkers(1)
            .build();

        connect.start();

        Map<String, String> connectorWithBlockingTaskStopConfig = new HashMap<>();
        connectorWithBlockingTaskStopConfig.put(CONNECTOR_CLASS_CONFIG, BlockingConnectorTest.BlockingSourceConnector.class.getName());
        connectorWithBlockingTaskStopConfig.put(TASKS_MAX_CONFIG, "1");
        connectorWithBlockingTaskStopConfig.put(BlockingConnectorTest.Block.BLOCK_CONFIG, Objects.requireNonNull(TASK_STOP));

        connect.configureConnector(CONNECTOR_NAME, connectorWithBlockingTaskStopConfig);

        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(
            CONNECTOR_NAME, 1, "connector and tasks did not start in time"
        );

        try (LogCaptureAppender logCaptureAppender = LogCaptureAppender.createAndRegister(DistributedHerder.class)) {
            connect.restartTask(CONNECTOR_NAME, 0);
            TestUtils.waitForCondition(() -> logCaptureAppender.getEvents().stream().anyMatch(e -> e.getLevel().equals("WARN")) &&
                    logCaptureAppender.getEvents().stream().anyMatch(e ->
                        // Ensure that the tick thread is blocked on the stage which we expect it to be, i.e restarting the task.
                        e.getMessage().contains("worker poll timeout has expired") &&
                        e.getMessage().contains("The last known action being performed by the worker is : restarting task " + CONNECTOR_NAME + "-0")
                    ),
                "Coordinator did not poll for rebalance.timeout.ms");
            // This clean up ensures that the test ends quickly as o/w we will wait for task#stop.
            BlockingConnectorTest.Block.reset();
        }
    }

    private void assertTimeoutException(Runnable operation, String expectedStageDescription, boolean wait) throws InterruptedException {
        connect.requestTimeout(1_000);
        AtomicReference<Throwable> latestError = new AtomicReference<>();

        // If requested, wait for the specific operation against the Connect cluster to time out
        // Otherwise, assert that the operation times out immediately
        long timeoutMs = wait ? 30_000L : 0L;
        waitForCondition(
                () -> {
                    try {
                        operation.run();
                        latestError.set(null);
                        return false;
                    } catch (Throwable t) {
                        latestError.set(t);
                        assertInstanceOf(ConnectRestException.class, t);
                        ConnectRestException restException = (ConnectRestException) t;

                        assertEquals(INTERNAL_SERVER_ERROR.getStatusCode(), restException.statusCode());
                        assertNotNull(restException.getMessage());
                        assertTrue(
                                restException.getMessage().contains("Request timed out. The worker is currently " + expectedStageDescription),
                                "Message '" + restException.getMessage() + "' does not match expected format"
                        );

                        return true;
                    }
                },
                timeoutMs,
                () -> {
                    String baseMessage = "REST request did not time out with expected error message in time. ";
                    Throwable t = latestError.get();
                    if (t == null) {
                        return baseMessage + "The most recent request did not fail.";
                    } else {
                        return baseMessage + "Most recent error: " + t;
                    }
                }
        );

        // Ensure that the health check endpoints of all workers also report the same timeout message
        connect.workers().forEach(worker -> {
            try (Response response = connect.healthCheck(worker)) {
                assertEquals(INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
                assertNotNull(response.getEntity());
                String body = response.getEntity().toString();
                String expectedSubstring = "Worker was unable to handle this request and may be unable to handle other requests";
                assertTrue(
                        body.contains(expectedSubstring),
                        "Response body '" + body + "' did not contain expected message '" + expectedSubstring + "'"
                );
                assertTrue(
                        body.contains(expectedStageDescription),
                        "Response body '" + body + "' did not contain expected message '" + expectedStageDescription + "'"
                );
            }
        });
        connect.resetRequestTimeout();
    }

    /**
     * Tests the logic around enforcement of the
     * {@link org.apache.kafka.connect.runtime.ConnectorConfig#TASKS_MAX_CONFIG tasks.max}
     * property and how it can be toggled via the
     * {@link org.apache.kafka.connect.runtime.ConnectorConfig#TASKS_MAX_ENFORCE_CONFIG tasks.max.enforce}
     * property, following the test plain laid out in
     * <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-1004%3A+Enforce+tasks.max+property+in+Kafka+Connect#KIP1004:Enforcetasks.maxpropertyinKafkaConnect-TestPlan">KIP-1004</a>.
     */
    @Test
    public void testTasksMaxEnforcement() throws Exception {
        String configTopic = "tasks-max-enforcement-configs";
        workerProps.put(CONFIG_TOPIC_CONFIG, configTopic);
        connect = connectBuilder.build();
        // start the clusters
        connect.start();

        Map<String, String> connectorProps = defaultSourceConnectorProps(TOPIC_NAME);
        int maxTasks = 1;
        connectorProps.put(TASKS_MAX_CONFIG, Integer.toString(maxTasks));
        int numTasks = 2;
        connectorProps.put(MonitorableSourceConnector.NUM_TASKS, Integer.toString(numTasks));
        connect.configureConnector(CONNECTOR_NAME, connectorProps);

        // A connector that generates excessive tasks will be failed with an expected error message
        connect.assertions().assertConnectorIsFailedAndTasksHaveFailed(
                CONNECTOR_NAME,
                0,
                "connector did not fail in time"
        );

        String expectedErrorSnippet = String.format(
                "The connector %s has generated %d tasks, which is greater than %d, "
                        + "the maximum number of tasks it is configured to create. ",
                CONNECTOR_NAME,
                numTasks,
                maxTasks
        );
        String errorMessage = connect.connectorStatus(CONNECTOR_NAME).connector().trace();
        assertTrue(errorMessage.contains(expectedErrorSnippet));

        // Stop all workers in the cluster
        connect.workers().forEach(connect::removeWorker);

        // Publish a set of too many task configs to the config topic, to simulate
        // an existing set of task configs that was written before the cluster was upgraded
        try (JsonConverter converter = new JsonConverter()) {
            converter.configure(
                    Collections.singletonMap(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false"),
                    false
            );

            for (int i = 0; i < numTasks; i++) {
                Map<String, String> taskConfig = MonitorableSourceConnector.taskConfig(
                        connectorProps,
                        CONNECTOR_NAME,
                        i
                );
                Struct wrappedTaskConfig = new Struct(KafkaConfigBackingStore.TASK_CONFIGURATION_V0)
                        .put("properties", taskConfig);
                String key = KafkaConfigBackingStore.TASK_KEY(new ConnectorTaskId(CONNECTOR_NAME, i));
                byte[] value = converter.fromConnectData(
                        configTopic,
                        KafkaConfigBackingStore.TASK_CONFIGURATION_V0,
                        wrappedTaskConfig
                );
                connect.kafka().produce(configTopic, key, new String(value));
            }

            Struct taskCommitMessage = new Struct(KafkaConfigBackingStore.CONNECTOR_TASKS_COMMIT_V0);
            taskCommitMessage.put("tasks", numTasks);
            String key = KafkaConfigBackingStore.COMMIT_TASKS_KEY(CONNECTOR_NAME);
            byte[] value = converter.fromConnectData(
                    configTopic,
                    KafkaConfigBackingStore.CONNECTOR_TASKS_COMMIT_V0,
                    taskCommitMessage
            );
            connect.kafka().produce(configTopic, key, new String(value));
        }

        // Restart all the workers in the cluster
        for (int i = 0; i < NUM_WORKERS; i++)
            connect.addWorker();

        // An existing set of tasks that exceeds the tasks.max property
        // will be failed with an expected error message
        connect.assertions().assertConnectorIsFailedAndTasksHaveFailed(
                CONNECTOR_NAME,
                numTasks,
                "connector and tasks did not fail in time"
        );

        connectorProps.put(TASKS_MAX_ENFORCE_CONFIG, "false");
        connect.configureConnector(CONNECTOR_NAME, connectorProps);

        // That same existing set of tasks will be allowed to run
        // once the connector is reconfigured with tasks.max.enforce set to false
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(
                CONNECTOR_NAME,
                numTasks,
                "connector and tasks did not start in time"
        );

        numTasks++;
        connectorProps.put(MonitorableSourceConnector.NUM_TASKS, Integer.toString(numTasks));
        connect.configureConnector(CONNECTOR_NAME, connectorProps);

        // A connector will be allowed to generate excessive tasks when tasks.max.enforce is set to false
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(
                CONNECTOR_NAME,
                numTasks,
                "connector and tasks did not start in time"
        );

        numTasks = maxTasks;
        connectorProps.put(MonitorableSourceConnector.NUM_TASKS, Integer.toString(numTasks));
        connectorProps.put(TASKS_MAX_ENFORCE_CONFIG, "true");
        connect.configureConnector(CONNECTOR_NAME, connectorProps);

        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(
                CONNECTOR_NAME,
                numTasks,
                "connector and tasks did not start in time"
        );

        numTasks = maxTasks + 1;
        connectorProps.put(MonitorableSourceConnector.NUM_TASKS, Integer.toString(numTasks));
        connect.configureConnector(CONNECTOR_NAME, connectorProps);

        // A connector that generates excessive tasks after being reconfigured will be failed, but its existing tasks will continue running
        connect.assertions().assertConnectorIsFailedAndNumTasksAreRunning(
                CONNECTOR_NAME,
                maxTasks,
                "connector did not fail in time, or tasks were incorrectly failed"
        );

        // Make sure that the tasks have had a chance to fail (i.e., that the worker has been given
        // a chance to check on the number of tasks for the connector during task startup)
        for (int i = 0; i < maxTasks; i++)
            connect.restartTask(CONNECTOR_NAME, i);

        // Verify one more time that none of the tasks have actually failed
        connect.assertions().assertConnectorIsFailedAndNumTasksAreRunning(
                CONNECTOR_NAME,
                maxTasks,
                "connector did not fail in time, or tasks were incorrectly failed"
        );
    }

    /**
     * Task configs are not removed from the config topic after a connector is deleted.
     * When topic compaction takes place, this can cause the tombstone message for the
     * connector config to be deleted, leaving the task configs in the config topic with no
     * explicit record of the connector's deletion.
     * <p>
     * This test guarantees that those older task configs are never used, even when the
     * connector is recreated later.
     */
    @Test
    public void testCompactedDeletedOlderConnectorConfig() throws Exception {
        brokerProps.put("log.cleaner.backoff.ms", "100");
        brokerProps.put("log.cleaner.delete.retention.ms", "1");
        brokerProps.put("log.cleaner.max.compaction.lag.ms", "1");
        brokerProps.put("log.cleaner.min.cleanable.ratio", "0");
        brokerProps.put("log.cleaner.min.compaction.lag.ms", "1");
        brokerProps.put("log.cleaner.threads", "1");

        final String configTopic = "kafka-16838-configs";
        final int offsetCommitIntervalMs = 100;
        workerProps.put(CONFIG_TOPIC_CONFIG, configTopic);
        workerProps.put(CONFIG_STORAGE_PREFIX + SEGMENT_MS_CONFIG, "100");
        workerProps.put(CONFIG_STORAGE_PREFIX + DELETE_RETENTION_MS_CONFIG, "1");
        workerProps.put(OFFSET_COMMIT_INTERVAL_MS_CONFIG, Integer.toString(offsetCommitIntervalMs));

        final int numWorkers = 1;
        connect = connectBuilder
                .numWorkers(numWorkers)
                .build();
        // start the clusters
        connect.start();

        final String connectorTopic = "connector-topic";
        connect.kafka().createTopic(connectorTopic, 1);

        ConnectorHandle connectorHandle = RuntimeHandles.get().connectorHandle(CONNECTOR_NAME);
        connectorHandle.expectedCommits(NUM_TASKS * 2);

        Map<String, String> connectorConfig = defaultSourceConnectorProps(connectorTopic);
        connect.configureConnector(CONNECTOR_NAME, connectorConfig);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(
                CONNECTOR_NAME,
                NUM_TASKS,
                "Connector or its tasks did not start in time"
        );
        connectorHandle.awaitCommits(RECORD_TRANSFER_TIMEOUT_MS);

        connect.deleteConnector(CONNECTOR_NAME);

        // Roll the entire cluster
        connect.healthyWorkers().forEach(connect::removeWorker);

        // Miserable hack: produce directly to the config topic and then wait a little bit
        // in order to trigger segment rollover and allow compaction to take place
        connect.kafka().produce(configTopic, "garbage-key-1", null);
        Thread.sleep(1_000);
        connect.kafka().produce(configTopic, "garbage-key-2", null);
        Thread.sleep(1_000);

        for (int i = 0; i < numWorkers; i++)
            connect.addWorker();

        connect.assertions().assertAtLeastNumWorkersAreUp(
                numWorkers,
                "Workers did not start in time after cluster was rolled."
        );

        final TopicPartition connectorTopicPartition = new TopicPartition(connectorTopic, 0);
        final long initialEndOffset = connect.kafka().endOffset(connectorTopicPartition);
        assertTrue(
                initialEndOffset > 0,
                "Source connector should have published at least one record to Kafka"
        );

        connectorHandle.expectedCommits(NUM_TASKS * 2);

        // Re-create the connector with a different config (targets a different topic)
        final String otherConnectorTopic = "other-topic";
        connect.kafka().createTopic(otherConnectorTopic, 1);
        connectorConfig.put(TOPIC_CONFIG, otherConnectorTopic);
        connect.configureConnector(CONNECTOR_NAME, connectorConfig);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(
                CONNECTOR_NAME,
                NUM_TASKS,
                "Connector or its tasks did not start in time"
        );
        connectorHandle.awaitCommits(RECORD_TRANSFER_TIMEOUT_MS);

        // See if any new records got written to the old topic
        final long nextEndOffset = connect.kafka().endOffset(connectorTopicPartition);
        assertEquals(
                initialEndOffset,
                nextEndOffset,
                "No new records should have been written to the older topic"
        );
    }

    /**
     * If a connector has existing tasks, and then generates new task configs, workers compare the
     * new and existing configs before publishing them to the config topic. If there is no difference,
     * workers do not publish task configs (this is a workaround to prevent infinite loops with eager
     * rebalancing).
     * <p>
     * This test tries to guarantee that, if the old task configs become invalid because of
     * an invalid config provider reference, it will still be possible to reconfigure the connector.
     */
    @Test
    public void testReconfigureConnectorWithFailingTaskConfigs(@TempDir Path tmp) throws Exception {
        final int offsetCommitIntervalMs = 100;
        workerProps.put(CONFIG_PROVIDERS_CONFIG, "file");
        workerProps.put(CONFIG_PROVIDERS_CONFIG + ".file.class", FileConfigProvider.class.getName());
        workerProps.put(OFFSET_COMMIT_INTERVAL_MS_CONFIG, Integer.toString(offsetCommitIntervalMs));

        final int numWorkers = 1;
        connect = connectBuilder
                .numWorkers(numWorkers)
                .build();
        // start the clusters
        connect.start();

        final String firstConnectorTopic = "connector-topic-1";
        connect.kafka().createTopic(firstConnectorTopic);

        final File secretsFile = tmp.resolve("test-secrets").toFile();
        final Properties secrets = new Properties();
        final String throughputSecretKey = "secret-throughput";
        secrets.put(throughputSecretKey, "10");
        try (FileOutputStream secretsOutputStream = new FileOutputStream(secretsFile)) {
            secrets.store(secretsOutputStream, null);
        }

        ConnectorHandle connectorHandle = RuntimeHandles.get().connectorHandle(CONNECTOR_NAME);
        connectorHandle.expectedCommits(NUM_TASKS * 2);

        Map<String, String> connectorConfig = defaultSourceConnectorProps(firstConnectorTopic);
        connectorConfig.put(
                "throughput",
                "${file:" + secretsFile.getAbsolutePath() + ":" + throughputSecretKey + "}"
        );
        connect.configureConnector(CONNECTOR_NAME, connectorConfig);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(
                CONNECTOR_NAME,
                NUM_TASKS,
                "Connector or its tasks did not start in time"
        );
        connectorHandle.awaitCommits(RECORD_TRANSFER_TIMEOUT_MS);

        // Delete the secrets file, which should render the old task configs invalid
        assertTrue(secretsFile.delete(), "Failed to delete secrets file");

        // Use a start latch here instead of assertConnectorAndExactlyNumTasksAreRunning
        // since failure to reconfigure the tasks (which may occur if the bug this test was written
        // to help catch resurfaces) will not cause existing tasks to fail or stop running
        StartAndStopLatch restarts = connectorHandle.expectedStarts(1);

        final String secondConnectorTopic = "connector-topic-2";
        connect.kafka().createTopic(secondConnectorTopic, 1);

        // Stop using the config provider for this connector, and instruct it to start writing to the
        // old topic again
        connectorConfig.put("throughput", "10");
        connectorConfig.put(TOPIC_CONFIG, secondConnectorTopic);
        connect.configureConnector(CONNECTOR_NAME, connectorConfig);
        assertTrue(
                restarts.await(10, TimeUnit.SECONDS),
                "Connector tasks were not restarted in time"
        );

        // Wait for at least one task to commit offsets after being restarted
        connectorHandle.expectedCommits(1);
        connectorHandle.awaitCommits(RECORD_TRANSFER_TIMEOUT_MS);

        final long endOffset = connect.kafka().endOffset(new TopicPartition(secondConnectorTopic, 0));
        assertTrue(
                endOffset > 0,
                "Source connector should have published at least one record to new Kafka topic "
                        + "after being reconfigured"
        );
    }

    @Test
    public void testRuntimePropertyReconfiguration() throws Exception {
        final int offsetCommitIntervalMs = 1_000;
        // force fast offset commits
        workerProps.put(OFFSET_COMMIT_INTERVAL_MS_CONFIG, Integer.toString(offsetCommitIntervalMs));
        connect = connectBuilder.build();
        // start the clusters
        connect.start();

        final String topic = "kafka9228";
        connect.kafka().createTopic(topic, 1);
        connect.kafka().produce(topic, "non-json-value");

        Map<String, String> connectorConfig = new HashMap<>();
        connectorConfig.put(CONNECTOR_CLASS_CONFIG, EmptyTaskConfigsConnector.class.getName());
        connectorConfig.put(TASKS_MAX_CONFIG, "1");
        connectorConfig.put(TOPICS_CONFIG, topic);
        // Initially configure the connector to use the JSON converter, which should cause task failure(s)
        connectorConfig.put(VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
        connectorConfig.put(
                VALUE_CONVERTER_CLASS_CONFIG + "." + JsonConverterConfig.SCHEMAS_ENABLE_CONFIG,
                "false"
        );

        connect.configureConnector(CONNECTOR_NAME, connectorConfig);
        connect.assertions().assertConnectorIsRunningAndTasksHaveFailed(
                CONNECTOR_NAME,
                1,
                "Connector did not start or task did not fail in time"
        );
        assertEquals(
                new ConnectorOffsets(Collections.emptyList()),
                connect.connectorOffsets(CONNECTOR_NAME),
                "Connector should not have any committed offsets when only task fails on first record"
        );

        // Reconfigure the connector to use the string converter, which should not cause any more task failures
        connectorConfig.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        connectorConfig.remove(
                KEY_CONVERTER_CLASS_CONFIG + "." + JsonConverterConfig.SCHEMAS_ENABLE_CONFIG
        );
        connect.configureConnector(CONNECTOR_NAME, connectorConfig);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(
                CONNECTOR_NAME,
                1,
                "Connector or tasks did not start in time"
        );

        Map<String, Object> expectedOffsetKey = new HashMap<>();
        expectedOffsetKey.put(SinkUtils.KAFKA_TOPIC_KEY, topic);
        expectedOffsetKey.put(SinkUtils.KAFKA_PARTITION_KEY, 0);
        Map<String, Object> expectedOffsetValue = Collections.singletonMap(SinkUtils.KAFKA_OFFSET_KEY, 1);
        ConnectorOffset expectedOffset = new ConnectorOffset(expectedOffsetKey, expectedOffsetValue);
        ConnectorOffsets expectedOffsets = new ConnectorOffsets(Collections.singletonList(expectedOffset));

        // Wait for it to commit offsets, signaling that it has successfully processed the record we produced earlier
        waitForCondition(
                () -> expectedOffsets.equals(connect.connectorOffsets(CONNECTOR_NAME)),
                offsetCommitIntervalMs * 2,
                "Task did not successfully process record and/or commit offsets in time"
        );
    }

    @Test
    public void testPluginAliases() throws Exception {
        connect = connectBuilder.build();
        // start the clusters
        connect.start();

        // Create a topic; not strictly necessary but prevents log spam when we start a source connector later
        final String topic = "kafka17150";
        connect.kafka().createTopic(topic, 1);

        Map<String, String> baseConnectorConfig = new HashMap<>();
        // General connector properties
        baseConnectorConfig.put(TASKS_MAX_CONFIG, Integer.toString(NUM_TASKS));
        // Aliased converter classes
        baseConnectorConfig.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getSimpleName());
        baseConnectorConfig.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getSimpleName());
        baseConnectorConfig.put(HEADER_CONVERTER_CLASS_CONFIG, StringConverter.class.getSimpleName());
        // Aliased SMT and predicate classes
        baseConnectorConfig.put(TRANSFORMS_CONFIG, "filter");
        baseConnectorConfig.put(TRANSFORMS_CONFIG + ".filter.type", Filter.class.getSimpleName());
        baseConnectorConfig.put(TRANSFORMS_CONFIG + ".filter.predicate", "tombstone");
        baseConnectorConfig.put(PREDICATES_CONFIG, "tombstone");
        baseConnectorConfig.put(PREDICATES_CONFIG + ".tombstone.type", RecordIsTombstone.class.getSimpleName());

        // Test a source connector
        final String sourceConnectorName = "plugins-alias-test-source";
        Map<String, String> sourceConnectorConfig = new HashMap<>(baseConnectorConfig);
        // Aliased source connector class
        sourceConnectorConfig.put(CONNECTOR_CLASS_CONFIG, MonitorableSourceConnector.class.getSimpleName());
        // Connector-specific properties
        sourceConnectorConfig.put(TOPIC_CONFIG, topic);
        sourceConnectorConfig.put("throughput", "10");
        sourceConnectorConfig.put("messages.per.poll", String.valueOf(MESSAGES_PER_POLL));
        // Create the connector and ensure it and its tasks can start
        connect.configureConnector(sourceConnectorName, sourceConnectorConfig);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(sourceConnectorName, NUM_TASKS, "Connector and tasks did not start in time");
        connect.deleteConnector(sourceConnectorName);

        // Test a sink connector
        final String sinkConnectorName = "plugins-alias-test-sink";
        Map<String, String> sinkConnectorConfig = new HashMap<>(baseConnectorConfig);
        // Aliased sink connector class
        sinkConnectorConfig.put(CONNECTOR_CLASS_CONFIG, MonitorableSinkConnector.class.getSimpleName());
        // Connector-specific properties
        sinkConnectorConfig.put(TOPICS_CONFIG, topic);
        // Create the connector and ensure it and its tasks can start
        connect.configureConnector(sinkConnectorName, sinkConnectorConfig);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(sinkConnectorName, NUM_TASKS, "Connector and tasks did not start in time");
        connect.deleteConnector(sinkConnectorName);
    }

    private Map<String, String> defaultSourceConnectorProps(String topic) {
        // setup props for the source connector
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

    private void useFixedBrokerPort() throws IOException {
        // Find a free port and use it in the Kafka broker's listeners config. We can't use port 0 in the listeners
        // config to get a random free port because in this test we want to stop the Kafka broker and then bring it
        // back up and listening on the same port in order to verify that the Connect cluster can re-connect to Kafka
        // and continue functioning normally. If we were to use port 0 here, the Kafka broker would most likely listen
        // on a different random free port the second time it is started. Note that we can only use the static port
        // because we have a single broker setup in this test.
        int listenerPort;
        try (ServerSocket s = new ServerSocket(0)) {
            listenerPort = s.getLocalPort();
        }
        brokerProps.put(SocketServerConfigs.LISTENERS_CONFIG, String.format("EXTERNAL://localhost:%d,CONTROLLER://localhost:0", listenerPort));
        connectBuilder
                .numBrokers(1)
                .brokerProps(brokerProps);
    }

    public static class EmptyTaskConfigsConnector extends SinkConnector {
        @Override
        public String version() {
            return "0.0";
        }

        @Override
        public void start(Map<String, String> props) {
            // no-op
        }

        @Override
        public Class<? extends Task> taskClass() {
            return SimpleTask.class;
        }

        @Override
        public List<Map<String, String>> taskConfigs(int maxTasks) {
            return IntStream.range(0, maxTasks)
                    .mapToObj(i -> Collections.<String, String>emptyMap())
                    .collect(Collectors.toList());
        }

        @Override
        public void stop() {
            // no-op
        }

        @Override
        public ConfigDef config() {
            return new ConfigDef();
        }
    }

    public static class SimpleTask extends SinkTask {
        @Override
        public String version() {
            return "0.0";
        }

        @Override
        public void start(Map<String, String> props) {
            // no-op
        }

        @Override
        public void put(Collection<SinkRecord> records) {
            // no-op
        }

        @Override
        public void stop() {
            // no-op
        }
    }
}
