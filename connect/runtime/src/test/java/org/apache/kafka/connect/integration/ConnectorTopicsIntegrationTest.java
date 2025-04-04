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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.rest.errors.ConnectRestException;
import org.apache.kafka.connect.storage.KafkaStatusBackingStore;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.kafka.connect.integration.TestableSourceConnector.TOPIC_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.DEFAULT_TOPIC_CREATION_PREFIX;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.PARTITIONS_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.CONNECTOR_CLIENT_POLICY_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.TOPIC_TRACKING_ALLOW_RESET_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.TOPIC_TRACKING_ENABLE_CONFIG;
import static org.apache.kafka.connect.sink.SinkConnector.TOPICS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for the endpoints that offer topic tracking of a connector's active
 * topics.
 */
@Tag("integration")
public class ConnectorTopicsIntegrationTest {

    private static final int NUM_WORKERS = 5;
    private static final int NUM_TASKS = 1;
    private static final String FOO_TOPIC = "foo-topic";
    private static final String FOO_CONNECTOR = "foo-source";
    private static final String BAR_TOPIC = "bar-topic";
    private static final String BAR_CONNECTOR = "bar-source";
    private static final String SINK_CONNECTOR = "baz-sink";
    private static final int NUM_TOPIC_PARTITIONS = 3;

    private EmbeddedConnectCluster.Builder connectBuilder;
    private EmbeddedConnectCluster connect;
    Map<String, String> workerProps = new HashMap<>();
    Properties brokerProps = new Properties();

    @BeforeEach
    public void setup() {
        // setup Connect worker properties
        workerProps.put(CONNECTOR_CLIENT_POLICY_CLASS_CONFIG, "All");

        // setup Kafka broker properties
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
    public void close() {
        // stop the Connect cluster and its backing Kafka cluster.
        connect.stop();
    }

    @Test
    public void testGetActiveTopics() throws InterruptedException {
        connect = connectBuilder.build();
        // start the clusters
        connect.start();

        // create test topic
        connect.kafka().createTopic(FOO_TOPIC, NUM_TOPIC_PARTITIONS);
        connect.kafka().createTopic(BAR_TOPIC, NUM_TOPIC_PARTITIONS);

        connect.assertions().assertConnectorActiveTopics(FOO_CONNECTOR, Collections.emptyList(),
                "Active topic set is not empty for connector: " + FOO_CONNECTOR);

        // start a source connector
        connect.configureConnector(FOO_CONNECTOR, defaultSourceConnectorProps(FOO_TOPIC));

        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(FOO_CONNECTOR, NUM_TASKS,
                "Connector tasks did not start in time.");

        connect.assertions().assertConnectorActiveTopics(FOO_CONNECTOR, Collections.singletonList(FOO_TOPIC),
                "Active topic set is not: " + Collections.singletonList(FOO_TOPIC) + " for connector: " + FOO_CONNECTOR);

        // start another source connector
        connect.configureConnector(BAR_CONNECTOR, defaultSourceConnectorProps(BAR_TOPIC));

        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(BAR_CONNECTOR, NUM_TASKS,
                "Connector tasks did not start in time.");

        connect.assertions().assertConnectorActiveTopics(BAR_CONNECTOR, Collections.singletonList(BAR_TOPIC),
                "Active topic set is not: " + Collections.singletonList(BAR_TOPIC) + " for connector: " + BAR_CONNECTOR);

        // start a sink connector
        connect.configureConnector(SINK_CONNECTOR, defaultSinkConnectorProps(FOO_TOPIC, BAR_TOPIC));

        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(SINK_CONNECTOR, NUM_TASKS,
                "Connector tasks did not start in time.");

        connect.assertions().assertConnectorActiveTopics(SINK_CONNECTOR, Arrays.asList(FOO_TOPIC, BAR_TOPIC),
                "Active topic set is not: " + Arrays.asList(FOO_TOPIC, BAR_TOPIC) + " for connector: " + SINK_CONNECTOR);

        // deleting a connector resets its active topics
        connect.deleteConnector(BAR_CONNECTOR);

        connect.assertions().assertConnectorDoesNotExist(BAR_CONNECTOR,
                "Connector wasn't deleted in time.");

        connect.assertions().assertConnectorActiveTopics(BAR_CONNECTOR, Collections.emptyList(),
                "Active topic set is not empty for deleted connector: " + BAR_CONNECTOR);

        // Unfortunately there's currently no easy way to know when the consumer caught up with
        // the last records that the producer of the stopped connector managed to produce.
        // Repeated runs show that this amount of time is sufficient for the consumer to catch up.
        Thread.sleep(5000);

        // reset active topics for the sink connector after one of the topics has become idle
        connect.resetConnectorTopics(SINK_CONNECTOR);

        connect.assertions().assertConnectorActiveTopics(SINK_CONNECTOR, Collections.singletonList(FOO_TOPIC),
                "Active topic set is not: " + Collections.singletonList(FOO_TOPIC) + " for connector: " + SINK_CONNECTOR);
    }

    @Test
    public void testTopicTrackingResetIsDisabled() throws InterruptedException {
        workerProps.put(TOPIC_TRACKING_ALLOW_RESET_CONFIG, "false");
        connect = connectBuilder.build();
        // start the clusters
        connect.start();

        // create test topic
        connect.kafka().createTopic(FOO_TOPIC, NUM_TOPIC_PARTITIONS);
        connect.kafka().createTopic(BAR_TOPIC, NUM_TOPIC_PARTITIONS);

        connect.assertions().assertConnectorActiveTopics(FOO_CONNECTOR, Collections.emptyList(),
                "Active topic set is not empty for connector: " + FOO_CONNECTOR);

        // start a source connector
        connect.configureConnector(FOO_CONNECTOR, defaultSourceConnectorProps(FOO_TOPIC));

        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(FOO_CONNECTOR, NUM_TASKS,
                "Connector tasks did not start in time.");

        connect.assertions().assertConnectorActiveTopics(FOO_CONNECTOR, Collections.singletonList(FOO_TOPIC),
                "Active topic set is not: " + Collections.singletonList(FOO_TOPIC) + " for connector: " + FOO_CONNECTOR);

        // start a sink connector
        connect.configureConnector(SINK_CONNECTOR, defaultSinkConnectorProps(FOO_TOPIC));

        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(SINK_CONNECTOR, NUM_TASKS,
                "Connector tasks did not start in time.");

        connect.assertions().assertConnectorActiveTopics(SINK_CONNECTOR, Collections.singletonList(FOO_TOPIC),
                "Active topic set is not: " + Collections.singletonList(FOO_TOPIC) + " for connector: " + SINK_CONNECTOR);

        // deleting a connector resets its active topics
        connect.deleteConnector(FOO_CONNECTOR);

        connect.assertions().assertConnectorDoesNotExist(FOO_CONNECTOR,
                "Connector wasn't deleted in time.");

        connect.assertions().assertConnectorActiveTopics(FOO_CONNECTOR, Collections.emptyList(),
                "Active topic set is not empty for deleted connector: " + FOO_CONNECTOR);

        // Unfortunately there's currently no easy way to know when the consumer caught up with
        // the last records that the producer of the stopped connector managed to produce.
        // Repeated runs show that this amount of time is sufficient for the consumer to catch up.
        Thread.sleep(5000);

        // resetting active topics for the sink connector won't work when the config is disabled
        Exception e = assertThrows(ConnectRestException.class, () -> connect.resetConnectorTopics(SINK_CONNECTOR));
        assertTrue(e.getMessage().contains("Topic tracking reset is disabled."));

        connect.assertions().assertConnectorActiveTopics(SINK_CONNECTOR, Collections.singletonList(FOO_TOPIC),
                "Active topic set is not: " + Collections.singletonList(FOO_TOPIC) + " for connector: " + SINK_CONNECTOR);
    }

    @Test
    public void testTopicTrackingIsDisabled() throws InterruptedException {
        workerProps.put(TOPIC_TRACKING_ENABLE_CONFIG, "false");
        connect = connectBuilder.build();
        // start the clusters
        connect.start();

        // create test topic
        connect.kafka().createTopic(FOO_TOPIC, NUM_TOPIC_PARTITIONS);
        connect.kafka().createTopic(BAR_TOPIC, NUM_TOPIC_PARTITIONS);

        // start a source connector
        connect.configureConnector(FOO_CONNECTOR, defaultSourceConnectorProps(FOO_TOPIC));
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(FOO_CONNECTOR, NUM_TASKS,
                "Connector tasks did not start in time.");

        // resetting active topics for the sink connector won't work when the config is disabled
        Exception e = assertThrows(ConnectRestException.class, () -> connect.resetConnectorTopics(SINK_CONNECTOR));
        assertTrue(e.getMessage().contains("Topic tracking is disabled."));

        e = assertThrows(ConnectRestException.class, () -> connect.connectorTopics(SINK_CONNECTOR));
        assertTrue(e.getMessage().contains("Topic tracking is disabled."));

        // Wait for tasks to produce a few records
        Thread.sleep(5000);

        assertNoTopicStatusInStatusTopic();
    }

    public void assertNoTopicStatusInStatusTopic() {
        String statusTopic = workerProps.get(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG);
        Consumer<byte[], byte[]> verifiableConsumer = connect.kafka().createConsumer(
                Collections.singletonMap("group.id", "verifiable-consumer-group-0"));

        List<PartitionInfo> partitionInfos = verifiableConsumer.partitionsFor(statusTopic);
        if (partitionInfos.isEmpty()) {
            throw new AssertionError("Unable to retrieve partitions info for status topic");
        }
        List<TopicPartition> partitions = partitionInfos.stream()
                .map(info -> new TopicPartition(info.topic(), info.partition()))
                .collect(Collectors.toList());
        verifiableConsumer.assign(partitions);

        // Based on the implementation of {@link org.apache.kafka.connect.util.KafkaBasedLog#readToLogEnd}
        Set<TopicPartition> assignment = verifiableConsumer.assignment();
        verifiableConsumer.seekToBeginning(assignment);
        Map<TopicPartition, Long> endOffsets = verifiableConsumer.endOffsets(assignment);
        while (!endOffsets.isEmpty()) {
            Iterator<Map.Entry<TopicPartition, Long>> it = endOffsets.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<TopicPartition, Long> entry = it.next();
                if (verifiableConsumer.position(entry.getKey()) >= entry.getValue())
                    it.remove();
                else {
                    try {
                        StreamSupport.stream(verifiableConsumer.poll(Duration.ofMillis(Integer.MAX_VALUE)).spliterator(), false)
                                .map(ConsumerRecord::key)
                                .filter(Objects::nonNull)
                                .filter(key -> new String(key, StandardCharsets.UTF_8).startsWith(KafkaStatusBackingStore.TOPIC_STATUS_PREFIX))
                                .findFirst()
                                .ifPresent(key -> {
                                    throw new AssertionError("Found unexpected key: " + new String(key, StandardCharsets.UTF_8) + " in status topic");
                                });
                    } catch (KafkaException e) {
                        throw new AssertionError("Error while reading to the end of status topic", e);
                    }
                    break;
                }
            }
        }
        verifiableConsumer.close();
    }

    private Map<String, String> defaultSourceConnectorProps(String topic) {
        // setup up props for the source connector
        Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR_CLASS_CONFIG, TestableSourceConnector.class.getSimpleName());
        props.put(TASKS_MAX_CONFIG, String.valueOf(NUM_TASKS));
        props.put(TOPIC_CONFIG, topic);
        props.put("throughput", String.valueOf(10));
        props.put("messages.per.poll", String.valueOf(10));
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + REPLICATION_FACTOR_CONFIG, String.valueOf(1));
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + PARTITIONS_CONFIG, String.valueOf(1));
        return props;
    }

    private Map<String, String> defaultSinkConnectorProps(String... topics) {
        // setup up props for the sink connector
        Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR_CLASS_CONFIG, TestableSinkConnector.class.getSimpleName());
        props.put(TASKS_MAX_CONFIG, String.valueOf(NUM_TASKS));
        props.put(TOPICS_CONFIG, String.join(",", topics));
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        return props;
    }

}
