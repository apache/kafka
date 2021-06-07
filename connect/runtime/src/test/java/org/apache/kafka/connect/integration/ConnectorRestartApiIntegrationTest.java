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

import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
import static org.apache.kafka.connect.util.clusters.EmbeddedConnectClusterAssertions.CONNECTOR_SETUP_DURATION_MS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


/**
 * Test connectors restart API use cases.
 */
@Category(IntegrationTest.class)
public class ConnectorRestartApiIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(ConnectorRestartApiIntegrationTest.class);

    private static final long OFFSET_COMMIT_INTERVAL_MS = TimeUnit.SECONDS.toMillis(30);
    private static final int NUM_WORKERS = 1;
    private static final int NUM_TASKS = 4;
    private static final int MESSAGES_PER_POLL = 10;
    private static final String CONNECTOR_NAME = "simple-source";
    private static final String TOPIC_NAME = "test-topic";

    private EmbeddedConnectCluster.Builder connectBuilder;
    private EmbeddedConnectCluster connect;
    private Map<String, String> workerProps;
    private Properties brokerProps;
    private ConnectorHandle connectorHandle;

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
        // get connector handles before starting test.
        connectorHandle = RuntimeHandles.get().connectorHandle(CONNECTOR_NAME);
    }

    @After
    public void close() {
        RuntimeHandles.get().deleteConnector(CONNECTOR_NAME);
        // stop all Connect, Kafka and Zk threads.
        connect.stop();
    }

    @Test
    public void testRestartOnlyConnector() throws Exception {
        testRestartAPI(false, false, 1, 0, false);
    }

    @Test
    public void testRestartBoth() throws Exception {
        testRestartAPI(false, true, 1, 1, false);
    }

    @Test
    public void testRestartNoopConnector() throws Exception {
        testRestartAPI(true, false, 0, 0, true);
    }

    @Test
    public void testRestartNoopBoth() throws Exception {
        testRestartAPI(true, true, 0, 0, true);
    }

    private void testRestartAPI(boolean onlyFailed, boolean includeTasks, int expectedConnectorRestarts, int expectedTasksRestarts, boolean noopRequest) throws Exception {
        connect = connectBuilder.build();
        // start the clusters
        connect.start();

        // setup up props for the source connector
        Map<String, String> props = defaultSourceConnectorProps(TOPIC_NAME);

        connect.assertions().assertExactlyNumWorkersAreUp(NUM_WORKERS,
                "Initial group of workers did not start in time.");

        // Try to start the connector and its single task.
        connect.configureConnector(CONNECTOR_NAME, props);

        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, NUM_TASKS,
                "Connector tasks are not all in running state.");

        StartAndStopCounterSnapshot beforeSnapshot = connectorHandle.startAndStopCounter().countsSnapshot();
        Map<String, StartAndStopCounterSnapshot> beforeTasksSnapshot = connectorHandle.tasks().stream().collect(Collectors.toMap(TaskHandle::taskId, task -> task.startAndStopCounter().countsSnapshot()));

        StartAndStopLatch stopLatch = connectorHandle.expectedStops(expectedConnectorRestarts, includeTasks);
        StartAndStopLatch startLatch = connectorHandle.expectedStarts(expectedConnectorRestarts, includeTasks);

        // expect that the connector will be stopped once the restart request is sent
        // Call the Restart API
        String taskRestartEndpoint = connect.endpointForResource(
                String.format("connectors/%s/restart?onlyFailed=" + onlyFailed + "&includeTasks=" + includeTasks, CONNECTOR_NAME));
        connect.requestPost(taskRestartEndpoint, "", Collections.emptyMap());

        if (noopRequest) {
            //for noop requests as everything is in RUNNING state, we need to wait for CONNECTOR_SETUP_DURATION_MS to ensure no unexpected restart events were fired
            Thread.sleep(CONNECTOR_SETUP_DURATION_MS);
        }

        // Wait for the connector to be stopped
        assertTrue("Failed to stop connector and tasks after coordinator failure within "
                        + CONNECTOR_SETUP_DURATION_MS + "ms",
                stopLatch.await(CONNECTOR_SETUP_DURATION_MS, TimeUnit.MILLISECONDS));

        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, NUM_TASKS,
                "Connector tasks are not all in running state.");
        // Expect that the connector has started again
        assertTrue("Failed to stop connector and tasks after coordinator failure within "
                        + CONNECTOR_SETUP_DURATION_MS + "ms",
                startLatch.await(CONNECTOR_SETUP_DURATION_MS, TimeUnit.MILLISECONDS));
        StartAndStopCounterSnapshot afterSnapshot = connectorHandle.startAndStopCounter().countsSnapshot();

        assertEquals(beforeSnapshot.starts() + expectedConnectorRestarts, afterSnapshot.starts());
        assertEquals(beforeSnapshot.stops() + expectedConnectorRestarts, afterSnapshot.stops());
        connectorHandle.tasks().forEach(t -> {
            StartAndStopCounterSnapshot afterTaskSnapshot = t.startAndStopCounter().countsSnapshot();
            assertEquals(beforeTasksSnapshot.get(t.taskId()).starts() + expectedTasksRestarts, afterTaskSnapshot.starts());
            assertEquals(beforeTasksSnapshot.get(t.taskId()).stops() + expectedTasksRestarts, afterTaskSnapshot.stops());
        });
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
