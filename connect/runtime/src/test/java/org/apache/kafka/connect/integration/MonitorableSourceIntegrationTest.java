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

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectStandalone;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.connect.integration.TestableSourceConnector.MAX_MESSAGES_PER_SECOND_CONFIG;
import static org.apache.kafka.connect.integration.TestableSourceConnector.MESSAGES_PER_POLL_CONFIG;
import static org.apache.kafka.connect.integration.TestableSourceConnector.TOPIC_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for a source connector defining metrics via the PluginMetrics API
 */
@Tag("integration")
@Timeout(value = 600)
public class MonitorableSourceIntegrationTest {

    private static final String CONNECTOR_NAME = "monitorable-source";
    private static final String TASK_ID = CONNECTOR_NAME + "-0";

    private static final int NUM_TASKS = 1;
    private static final long SOURCE_TASK_PRODUCE_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(30);
    private static final int MINIMUM_MESSAGES = 100;
    private static final String MESSAGES_PER_POLL = Integer.toString(MINIMUM_MESSAGES);
    private static final String MESSAGES_PER_SECOND = Long.toString(MINIMUM_MESSAGES / 2);

    private EmbeddedConnectStandalone connect;
    private ConnectorHandle connectorHandle;

    @BeforeEach
    public void setup() throws InterruptedException {
        // setup Connect cluster with defaults
        connect = new EmbeddedConnectStandalone.Builder().build();

        // start Connect cluster
        connect.start();

        // get connector handles before starting test.
        connectorHandle = RuntimeHandles.get().connectorHandle(CONNECTOR_NAME);
    }

    @AfterEach
    public void close() {
        connect.stop();
    }

    @Test
    public void testMonitorableSourceConnectorAndTask() throws Exception {
        connect.kafka().createTopic("test-topic");

        Map<String, String> props = Map.of(
            CONNECTOR_CLASS_CONFIG, MonitorableSourceConnector.class.getSimpleName(),
            TASKS_MAX_CONFIG, "1",
            TOPIC_CONFIG, "test-topic",
            KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName(),
            VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName(),
            MESSAGES_PER_POLL_CONFIG, MESSAGES_PER_POLL,
            MAX_MESSAGES_PER_SECOND_CONFIG, MESSAGES_PER_SECOND);

        // set expected records to successfully reach the task
        // expect all records to be consumed and committed by the task
        connectorHandle.taskHandle(TASK_ID).expectedRecords(MINIMUM_MESSAGES);
        connectorHandle.taskHandle(TASK_ID).expectedCommits(MINIMUM_MESSAGES);

        connect.configureConnector(CONNECTOR_NAME, props);
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, NUM_TASKS,
                "Connector tasks did not start in time.");

        // wait for the connector tasks to produce enough records
        connectorHandle.taskHandle(TASK_ID).awaitRecords(SOURCE_TASK_PRODUCE_TIMEOUT_MS);
        connectorHandle.taskHandle(TASK_ID).awaitCommits(TimeUnit.MINUTES.toMillis(1));

        // check connector metric
        Map<MetricName, KafkaMetric> metrics = connect.connectMetrics().metrics().metrics();
        MetricName connectorMetric = MonitorableSourceConnector.metricsName;
        assertTrue(metrics.containsKey(connectorMetric));
        assertEquals(CONNECTOR_NAME, connectorMetric.tags().get("connector"));
        assertEquals(MonitorableSourceConnector.VALUE, metrics.get(connectorMetric).metricValue());

        // check task metric
        metrics = connect.connectMetrics().metrics().metrics();
        MetricName taskMetric = MonitorableSourceConnector.MonitorableSourceTask.metricsName;
        assertTrue(metrics.containsKey(taskMetric));
        assertEquals(CONNECTOR_NAME, taskMetric.tags().get("connector"));
        assertEquals("0", taskMetric.tags().get("task"));
        assertTrue(MINIMUM_MESSAGES <= (double) metrics.get(taskMetric).metricValue());

        connect.deleteConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorDoesNotExist(CONNECTOR_NAME,
                "Connector wasn't deleted in time.");

        // verify the connector and task metrics have been deleted
        metrics = connect.connectMetrics().metrics().metrics();
        assertFalse(metrics.containsKey(connectorMetric));
        assertFalse(metrics.containsKey(taskMetric));
    }
}
