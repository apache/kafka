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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.rest.errors.ConnectRestException;
import org.apache.kafka.connect.storage.KafkaStatusBackingStore;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.kafka.connect.integration.MonitorableSourceConnector.TOPIC_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.CONNECTOR_CLIENT_POLICY_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.TOPIC_TRACKING_ALLOW_RESET_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.TOPIC_TRACKING_ENABLE_CONFIG;
import static org.apache.kafka.connect.sink.SinkConnector.TOPICS_CONFIG;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Integration test for the endpoints that offer topic tracking of a connector's active
 * topics.
 */
@Category(IntegrationTest.class)
public class SourceConnectorsIntegrationTest {

    private static final int NUM_WORKERS = 3;
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

    @Before
    public void setup() {
        // setup Connect worker properties
        workerProps.put(CONNECTOR_CLIENT_POLICY_CLASS_CONFIG, "All");

        // setup Kafka broker properties
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

    @Test
    public void testCreateTopic() throws InterruptedException {
        connect = connectBuilder.build();
        // start the clusters
        connect.start();

        connect.assertions().assertAtLeastNumWorkersAreUp(NUM_WORKERS, "Initial group of workers did not start in time.");

        // start a source connector
        connect.configureConnector(FOO_CONNECTOR, defaultSourceConnectorProps(FOO_TOPIC));

        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(FOO_CONNECTOR, NUM_TASKS,
                "Connector tasks did not start in time.");

        connect.assertions().assertKafkaTopicExists(FOO_TOPIC, 1,
                "Topic: " + FOO_TOPIC + " does not exist or does not have number of partitions: " + 1);
    }


    private Map<String, String> defaultSourceConnectorProps(String topic) {
        // setup up props for the source connector
        Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR_CLASS_CONFIG, MonitorableSourceConnector.class.getSimpleName());
        props.put(TASKS_MAX_CONFIG, String.valueOf(NUM_TASKS));
        props.put(TOPIC_CONFIG, topic);
        props.put("throughput", String.valueOf(10));
        props.put("messages.per.poll", String.valueOf(10));
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        return props;
    }

}
