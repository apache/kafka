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

import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;

import static org.apache.kafka.clients.consumer.ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.TOPICS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.CONNECTOR_CLIENT_POLICY_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Integration test for sink connectors
 */
@Category(IntegrationTest.class)
public class SinkConnectorsIntegrationTest {

    private static final int NUM_TASKS = 1;
    private static final int NUM_WORKERS = 1;
    private static final String CONNECTOR_NAME = "connect-integration-test-sink";
    private static final long TASK_CONSUME_TIMEOUT_MS = 10_000L;

    private EmbeddedConnectCluster connect;

    @Before
    public void setup() throws Exception {
        Map<String, String> workerProps = new HashMap<>();
        // permit all Kafka client overrides; required for testing different consumer partition assignment strategies
        workerProps.put(CONNECTOR_CLIENT_POLICY_CLASS_CONFIG, "All");

        // setup Kafka broker properties
        Properties brokerProps = new Properties();
        brokerProps.put("auto.create.topics.enable", "false");
        brokerProps.put("delete.topic.enable", "true");

        // build a Connect cluster backed by Kafka and Zk
        connect = new EmbeddedConnectCluster.Builder()
                .name("connect-cluster")
                .numWorkers(NUM_WORKERS)
                .workerProps(workerProps)
                .brokerProps(brokerProps)
                .build();
        connect.start();
        connect.assertions().assertAtLeastNumWorkersAreUp(NUM_WORKERS, "Initial group of workers did not start in time.");
    }

    @After
    public void close() {
        // delete connector handle
        RuntimeHandles.get().deleteConnector(CONNECTOR_NAME);

        // stop all Connect, Kafka and Zk threads.
        connect.stop();
    }

    @Test
    public void testEagerConsumerPartitionAssignment() throws Exception {
        final String topic1 = "topic1", topic2 = "topic2", topic3 = "topic3";
        final TopicPartition tp1 = new TopicPartition(topic1, 0), tp2 = new TopicPartition(topic2, 0), tp3 = new TopicPartition(topic3, 0);
        final Collection<String> topics = Arrays.asList(topic1, topic2, topic3);

        Map<String, String> connectorProps = baseSinkConnectorProps(String.join(",", topics));
        // Need an eager assignor here; round robin is as good as any
        connectorProps.put(
            CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX + PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
            RoundRobinAssignor.class.getName());
        // After deleting a topic, offset commits will fail for it; reduce the timeout here so that the test doesn't take forever to proceed past that point
        connectorProps.put(
            CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX + DEFAULT_API_TIMEOUT_MS_CONFIG,
            "5000");

        final Set<String> consumedRecordValues = new HashSet<>();
        Consumer<SinkRecord> onPut = record -> assertTrue("Task received duplicate record from Connect", consumedRecordValues.add(Objects.toString(record.value())));
        ConnectorHandle connector = RuntimeHandles.get().connectorHandle(CONNECTOR_NAME);
        TaskHandle task = connector.taskHandle(CONNECTOR_NAME + "-0", onPut);

        connect.configureConnector(CONNECTOR_NAME, connectorProps);
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, NUM_TASKS, "Connector tasks did not start in time.");

        // None of the topics has been created yet; the task shouldn't be assigned any partitions
        assertEquals(0, task.numPartitionsAssigned());

        Set<String> expectedRecordValues = new HashSet<>();
        Set<TopicPartition> expectedAssignment = new HashSet<>();

        connect.kafka().createTopic(topic1, 1);
        expectedAssignment.add(tp1);
        connect.kafka().produce(topic1, "t1v1");
        expectedRecordValues.add("t1v1");

        waitForCondition(
            () -> expectedRecordValues.equals(consumedRecordValues),
            TASK_CONSUME_TIMEOUT_MS,
            "Task did not receive records in time");
        assertEquals(1, task.timesAssigned(tp1));
        assertEquals(0, task.timesRevoked(tp1));
        assertEquals(expectedAssignment, task.assignment());

        connect.kafka().createTopic(topic2, 1);
        expectedAssignment.add(tp2);
        connect.kafka().produce(topic2, "t2v1");
        expectedRecordValues.add("t2v1");
        connect.kafka().produce(topic2, "t1v2");
        expectedRecordValues.add("t1v2");

        waitForCondition(
            () -> expectedRecordValues.equals(consumedRecordValues),
            TASK_CONSUME_TIMEOUT_MS,
            "Task did not receive records in time");
        assertEquals(2, task.timesAssigned(tp1));
        assertEquals(1, task.timesRevoked(tp1));
        assertEquals(1, task.timesCommitted(tp1));
        assertEquals(1, task.timesAssigned(tp2));
        assertEquals(0, task.timesRevoked(tp2));
        assertEquals(expectedAssignment, task.assignment());

        connect.kafka().createTopic(topic3, 1);
        expectedAssignment.add(tp3);
        connect.kafka().produce(topic3, "t3v1");
        expectedRecordValues.add("t3v1");
        connect.kafka().produce(topic2, "t2v2");
        expectedRecordValues.add("t2v2");
        connect.kafka().produce(topic2, "t1v3");
        expectedRecordValues.add("t1v3");

        expectedAssignment.add(tp3);
        waitForCondition(
            () -> expectedRecordValues.equals(consumedRecordValues),
            TASK_CONSUME_TIMEOUT_MS,
            "Task did not receive records in time");
        assertEquals(3, task.timesAssigned(tp1));
        assertEquals(2, task.timesRevoked(tp1));
        assertEquals(2, task.timesCommitted(tp1));
        assertEquals(2, task.timesAssigned(tp2));
        assertEquals(1, task.timesRevoked(tp2));
        assertEquals(1, task.timesCommitted(tp2));
        assertEquals(1, task.timesAssigned(tp3));
        assertEquals(0, task.timesRevoked(tp3));
        assertEquals(expectedAssignment, task.assignment());

        connect.kafka().deleteTopic(topic1);
        expectedAssignment.remove(tp1);
        connect.kafka().produce(topic3, "t3v2");
        expectedRecordValues.add("t3v2");
        connect.kafka().produce(topic2, "t2v3");
        expectedRecordValues.add("t2v3");

        waitForCondition(
            () -> expectedRecordValues.equals(consumedRecordValues) && expectedAssignment.equals(task.assignment()),
            TASK_CONSUME_TIMEOUT_MS,
            "Timed out while waiting for task to receive records and updated topic partition assignment");
        assertEquals(3, task.timesAssigned(tp1));
        assertEquals(3, task.timesRevoked(tp1));
        assertEquals(3, task.timesCommitted(tp1));
        assertEquals(3, task.timesAssigned(tp2));
        assertEquals(2, task.timesRevoked(tp2));
        assertEquals(2, task.timesCommitted(tp2));
        assertEquals(2, task.timesAssigned(tp3));
        assertEquals(1, task.timesRevoked(tp3));
        assertEquals(1, task.timesCommitted(tp3));
    }

    @Test
    public void testCooperativeConsumerPartitionAssignment() throws Exception {
        final String topic1 = "topic1", topic2 = "topic2", topic3 = "topic3";
        final TopicPartition tp1 = new TopicPartition(topic1, 0), tp2 = new TopicPartition(topic2, 0), tp3 = new TopicPartition(topic3, 0);
        final Collection<String> topics = Arrays.asList(topic1, topic2, topic3);

        Map<String, String> connectorProps = baseSinkConnectorProps(String.join(",", topics));
        // Need an eager assignor here; round robin is as good as any
        connectorProps.put(
                CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX + PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                CooperativeStickyAssignor.class.getName());
        // After deleting a topic, offset commits will fail for it; reduce the timeout here so that the test doesn't take forever to proceed past that point
        connectorProps.put(
                CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX + DEFAULT_API_TIMEOUT_MS_CONFIG,
                "5000");

        final Set<String> consumedRecordValues = new HashSet<>();
        Consumer<SinkRecord> onPut = record -> assertTrue("Task received duplicate record from Connect", consumedRecordValues.add(Objects.toString(record.value())));
        ConnectorHandle connector = RuntimeHandles.get().connectorHandle(CONNECTOR_NAME);
        TaskHandle task = connector.taskHandle(CONNECTOR_NAME + "-0", onPut);

        connect.configureConnector(CONNECTOR_NAME, connectorProps);
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, NUM_TASKS, "Connector tasks did not start in time.");

        // None of the topics has been created yet; the task shouldn't be assigned any partitions
        assertEquals(0, task.numPartitionsAssigned());

        Set<String> expectedRecordValues = new HashSet<>();
        Set<TopicPartition> expectedAssignment = new HashSet<>();

        connect.kafka().createTopic(topic1, 1);
        expectedAssignment.add(tp1);
        connect.kafka().produce(topic1, "t1v1");
        expectedRecordValues.add("t1v1");

        waitForCondition(
            () -> expectedRecordValues.equals(consumedRecordValues),
            TASK_CONSUME_TIMEOUT_MS,
            "Task did not receive records in time");
        assertEquals(1, task.timesAssigned(tp1));
        assertEquals(0, task.timesRevoked(tp1));
        assertEquals(expectedAssignment, task.assignment());

        connect.kafka().createTopic(topic2, 1);
        expectedAssignment.add(tp2);
        connect.kafka().produce(topic2, "t2v1");
        expectedRecordValues.add("t2v1");
        connect.kafka().produce(topic2, "t1v2");
        expectedRecordValues.add("t1v2");

        waitForCondition(
            () -> expectedRecordValues.equals(consumedRecordValues),
            TASK_CONSUME_TIMEOUT_MS,
            "Task did not receive records in time");
        assertEquals(1, task.timesAssigned(tp1));
        assertEquals(0, task.timesRevoked(tp1));
        assertEquals(0, task.timesCommitted(tp1));
        assertEquals(1, task.timesAssigned(tp2));
        assertEquals(0, task.timesRevoked(tp2));
        assertEquals(expectedAssignment, task.assignment());

        connect.kafka().createTopic(topic3, 1);
        expectedAssignment.add(tp3);
        connect.kafka().produce(topic3, "t3v1");
        expectedRecordValues.add("t3v1");
        connect.kafka().produce(topic2, "t2v2");
        expectedRecordValues.add("t2v2");
        connect.kafka().produce(topic2, "t1v3");
        expectedRecordValues.add("t1v3");

        expectedAssignment.add(tp3);
        waitForCondition(
            () -> expectedRecordValues.equals(consumedRecordValues),
            TASK_CONSUME_TIMEOUT_MS,
            "Task did not receive records in time");
        assertEquals(1, task.timesAssigned(tp1));
        assertEquals(0, task.timesRevoked(tp1));
        assertEquals(0, task.timesCommitted(tp1));
        assertEquals(1, task.timesAssigned(tp2));
        assertEquals(0, task.timesRevoked(tp2));
        assertEquals(0, task.timesCommitted(tp2));
        assertEquals(1, task.timesAssigned(tp3));
        assertEquals(0, task.timesRevoked(tp3));
        assertEquals(expectedAssignment, task.assignment());

        connect.kafka().deleteTopic(topic1);
        expectedAssignment.remove(tp1);
        connect.kafka().produce(topic3, "t3v2");
        expectedRecordValues.add("t3v2");
        connect.kafka().produce(topic2, "t2v3");
        expectedRecordValues.add("t2v3");

        waitForCondition(
            () -> expectedRecordValues.equals(consumedRecordValues) && expectedAssignment.equals(task.assignment()),
            TASK_CONSUME_TIMEOUT_MS,
            "Timed out while waiting for task to receive records and updated topic partition assignment");
        assertEquals(1, task.timesAssigned(tp1));
        assertEquals(1, task.timesRevoked(tp1));
        assertEquals(1, task.timesCommitted(tp1));
        assertEquals(1, task.timesAssigned(tp2));
        assertEquals(0, task.timesRevoked(tp2));
        assertEquals(0, task.timesCommitted(tp2));
        assertEquals(1, task.timesAssigned(tp3));
        assertEquals(0, task.timesRevoked(tp3));
        assertEquals(0, task.timesCommitted(tp3));
    }

    private Map<String, String> baseSinkConnectorProps(String topics) {
        Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR_CLASS_CONFIG, MonitorableSinkConnector.class.getSimpleName());
        props.put(TASKS_MAX_CONFIG, String.valueOf(NUM_TASKS));
        props.put(TOPICS_CONFIG, topics);
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        return props;
    }
}
