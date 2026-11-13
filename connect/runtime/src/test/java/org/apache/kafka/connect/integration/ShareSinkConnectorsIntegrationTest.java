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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorOffset;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.runtime.rest.entities.CreateConnectorRequest;
import org.apache.kafka.connect.runtime.rest.errors.ConnectRestException;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.SinkUtils;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX;
import static org.apache.kafka.connect.runtime.ConnectorConfig.NAME_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.TOPICS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.CONNECTOR_CLIENT_POLICY_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("integration")
@Timeout(60)
public class ShareSinkConnectorsIntegrationTest {
    private static final String CONNECTOR_NAME = "share-sink-admission";
    private static final String TOPIC = "share-sink-input";
    private static final String CONSUMER_TYPE_CONFIG = "consumer.type";
    private static final long TIMEOUT_MS = 10_000;

    private EmbeddedConnectCluster connect;

    @BeforeEach
    void setup() {
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put(CONNECTOR_CLIENT_POLICY_CLASS_CONFIG, "All");
        workerProps.put("offset.storage.partitions", "1");
        workerProps.put("status.storage.partitions", "1");
        workerProps.put("scheduled.rebalance.max.delay.ms", "0");
        connect = new EmbeddedConnectCluster.Builder()
                .name(CONNECTOR_NAME)
                .numWorkers(1)
                .numBrokers(1)
                .workerProps(workerProps)
                .build();
        connect.start();
        connect.kafka().createTopic(TOPIC, 1);
    }

    @AfterEach
    void cleanup() {
        try {
            if (connect != null) {
                connect.stop();
            }
        } finally {
            RuntimeHandles.get().deleteConnector(CONNECTOR_NAME);
        }
    }

    @Test
    void rejectsLegacySinkTaskInShareMode() throws Exception {
        try {
            connect.configureConnector(CONNECTOR_NAME, connectorConfig("share"));
        } catch (ConnectRestException e) {
            assertEquals(400, e.statusCode());
            assertDurableContractRejection(e.getMessage());
            assertFalse(connect.connectors().contains(CONNECTOR_NAME));
            assertEquals(0, RuntimeHandles.get().connectorHandle(CONNECTOR_NAME)
                    .taskHandle(CONNECTOR_NAME + "-0").startAndStopCounter().starts());
            return;
        }

        waitForCondition(() -> {
            try {
                List<ConnectorStateInfo.TaskState> tasks = connect.connectorStatus(CONNECTOR_NAME).tasks();
                return !tasks.isEmpty() && tasks.stream().allMatch(task ->
                        "FAILED".equals(task.state()) || "RUNNING".equals(task.state()));
            } catch (ConnectRestException e) {
                if (e.statusCode() == 404) {
                    return false;
                }
                throw e;
            }
        }, TIMEOUT_MS, "Legacy sink task did not reach an admission result");

        List<ConnectorStateInfo.TaskState> tasks = connect.connectorStatus(CONNECTOR_NAME).tasks();
        assertEquals(1, tasks.size());
        assertTrue(tasks.stream().allMatch(task -> "FAILED".equals(task.state())),
                "Share mode accepted a SinkTask without an explicit durable-write contract");
        tasks.forEach(task -> assertDurableContractRejection(task.trace()));
        assertEquals(0, RuntimeHandles.get().connectorHandle(CONNECTOR_NAME)
                .taskHandle(CONNECTOR_NAME + "-0").startAndStopCounter().starts());
    }

    @Test
    void rejectsIncompatibleActiveModePatchWithoutReplacingTheConfiguration() throws Exception {
        Map<String, String> props = connectorConfig("consumer");
        connect.configureConnector(CONNECTOR_NAME, props);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(
                CONNECTOR_NAME, 1, "Ordinary sink task did not start");

        ConnectRestException error = assertThrows(ConnectRestException.class,
                () -> connect.patchConnectorConfig(CONNECTOR_NAME, Map.of(CONSUMER_TYPE_CONFIG, "share")));

        assertTrue(error.statusCode() == 400 || error.statusCode() == 409, error.getMessage());
        assertEquals("consumer", connect.connectorInfo(CONNECTOR_NAME).config().get(CONSUMER_TYPE_CONFIG));
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(
                CONNECTOR_NAME, 1, "Rejected mode change disrupted the ordinary sink task");
    }

    @Test
    void rejectedShareAdmissionDoesNotRewriteExistingConsumerGroupOffsets() throws Exception {
        String groupId = "share-sink-offset-protection";
        TopicPartition partition = new TopicPartition(TOPIC, 0);
        try (Admin admin = connect.kafka().createAdminClient()) {
            admin.alterConsumerGroupOffsets(groupId, Map.of(partition, new OffsetAndMetadata(123L)))
                    .all().get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

            Map<String, String> props = connectorConfig("share");
            props.put("share.group.id", groupId);
            props.put(CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX + ConsumerConfig.GROUP_ID_CONFIG, groupId);
            List<ConnectorOffset> initialOffsets = List.of(new ConnectorOffset(
                    Map.of(SinkUtils.KAFKA_TOPIC_KEY, TOPIC, SinkUtils.KAFKA_PARTITION_KEY, 0),
                    Map.of(SinkUtils.KAFKA_OFFSET_KEY, 7L)));
            CreateConnectorRequest request = new CreateConnectorRequest(
                    CONNECTOR_NAME, props, CreateConnectorRequest.InitialState.STOPPED, initialOffsets);

            ConnectRestException error = assertThrows(ConnectRestException.class,
                    () -> connect.configureConnector(request));

            assertEquals(400, error.statusCode());
            assertEquals(123L, admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata()
                    .get(TIMEOUT_MS, TimeUnit.MILLISECONDS).get(partition).offset());
            assertFalse(connect.connectors().contains(CONNECTOR_NAME));
        }
    }

    private Map<String, String> connectorConfig(String consumerType) {
        Map<String, String> props = new HashMap<>();
        props.put(NAME_CONFIG, CONNECTOR_NAME);
        props.put(CONNECTOR_CLASS_CONFIG, TestableSinkConnector.class.getName());
        props.put(TASKS_MAX_CONFIG, "1");
        props.put(TOPICS_CONFIG, TOPIC);
        props.put(CONSUMER_TYPE_CONFIG, consumerType);
        props.put("share.processing.max.age.ms", "60000");
        props.put("share.processing.cancel.grace.ms", "5000");
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        return props;
    }

    private void assertDurableContractRejection(String reason) {
        assertTrue(reason != null && reason.contains("ShareSinkTask") && reason.contains("durable"),
                () -> "Expected explicit durable ShareSinkTask rejection: " + reason);
    }
}
