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
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectStandalone;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.TOPICS_CONFIG;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for a sink connector defining metrics via the PluginMetrics API
 */
@Tag("integration")
@Timeout(value = 600)
public class MonitorableSinkIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(MonitorableSinkIntegrationTest.class);

    private static final String CONNECTOR_NAME = "monitorable-sink";
    private static final String TASK_ID = CONNECTOR_NAME + "-0";
    private static final int NUM_RECORDS_PRODUCED = 1000;
    private static final int NUM_TASKS = 1;
    private static final long CONNECTOR_SETUP_DURATION_MS = TimeUnit.SECONDS.toMillis(60);
    private static final long CONSUME_MAX_DURATION_MS = TimeUnit.SECONDS.toMillis(30);

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
    public void testMonitorableSinkConnectorAndTask() throws Exception {
        connect.kafka().createTopic("test-topic");

        Map<String, String> props = Map.of(
            CONNECTOR_CLASS_CONFIG, MonitorableSinkConnector.class.getSimpleName(),
            TASKS_MAX_CONFIG, "1",
            TOPICS_CONFIG, "test-topic",
            KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName(),
            VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());

        // set expected records to successfully reach the task
        connectorHandle.taskHandle(TASK_ID).expectedRecords(NUM_RECORDS_PRODUCED);

        connect.configureConnector(CONNECTOR_NAME, props);
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, NUM_TASKS,
                "Connector tasks did not start in time.");

        waitForCondition(this::checkForPartitionAssignment,
                CONNECTOR_SETUP_DURATION_MS,
                "Connector task was not assigned a partition.");

        // check connector metric
        Map<MetricName, KafkaMetric> metrics = connect.connectMetrics().metrics().metrics();
        MetricName connectorMetric = MonitorableSinkConnector.metricsName;
        assertTrue(metrics.containsKey(connectorMetric));
        assertEquals(CONNECTOR_NAME, connectorMetric.tags().get("connector"));
        KafkaMetric kafkaMetric = metrics.get(connectorMetric);
        assertEquals(MonitorableSinkConnector.VALUE, kafkaMetric.metricValue());

        // produce some records
        for (int i = 0; i < NUM_RECORDS_PRODUCED; i++) {
            connect.kafka().produce("test-topic", "key-" + i, "value-" + i);
        }

        // wait for records to reach the task
        connectorHandle.taskHandle(TASK_ID).awaitRecords(CONSUME_MAX_DURATION_MS);

        // check task metric
        metrics = connect.connectMetrics().metrics().metrics();
        MetricName taskMetric = MonitorableSinkConnector.MonitorableSinkTask.metricsName;
        assertTrue(metrics.containsKey(taskMetric));
        assertEquals(CONNECTOR_NAME, taskMetric.tags().get("connector"));
        assertEquals("0", taskMetric.tags().get("task"));
        assertEquals((double) NUM_RECORDS_PRODUCED, metrics.get(taskMetric).metricValue());

        connect.deleteConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorDoesNotExist(CONNECTOR_NAME,
                "Connector wasn't deleted in time.");

        // verify connector and task metrics have been deleted
        metrics = connect.connectMetrics().metrics().metrics();
        assertFalse(metrics.containsKey(connectorMetric));
        assertFalse(metrics.containsKey(taskMetric));
    }

    /**
     * Check if a partition was assigned to each task. This method swallows exceptions since it is invoked from a
     * {@link org.apache.kafka.test.TestUtils#waitForCondition} that will throw an error if this method continued
     * to return false after the specified duration has elapsed.
     *
     * @return true if each task was assigned a partition each, false if this was not true or an error occurred when
     * executing this operation.
     */
    private boolean checkForPartitionAssignment() {
        try {
            ConnectorStateInfo info = connect.connectorStatus(CONNECTOR_NAME);
            return info != null && info.tasks().size() == NUM_TASKS
                    && connectorHandle.taskHandle(TASK_ID).numPartitionsAssigned() == 1;
        }  catch (Exception e) {
            // Log the exception and return that the partitions were not assigned
            log.error("Could not check connector state info.", e);
            return false;
        }
    }
}
