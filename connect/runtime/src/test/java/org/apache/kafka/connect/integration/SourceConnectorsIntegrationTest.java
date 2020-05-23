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

import org.apache.kafka.connect.runtime.SourceConnectorConfig;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.IntStream;

import static org.apache.kafka.connect.integration.MonitorableSourceConnector.TOPIC_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.NAME_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.SourceConnectorConfig.TOPIC_CREATION_GROUPS_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.DEFAULT_TOPIC_CREATION_PREFIX;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.EXCLUDE_REGEX_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.INCLUDE_REGEX_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.PARTITIONS_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.CONNECTOR_CLIENT_POLICY_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.TOPIC_CREATION_ENABLE_CONFIG;

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
    private static final String FOO_GROUP = "foo";
    private static final String BAR_GROUP = "bar";

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

        Map<String, String> fooProps = sourceConnectorPropsWithGroups(FOO_TOPIC);

        // start a source connector
        connect.configureConnector(FOO_CONNECTOR, fooProps);
        fooProps.put(NAME_CONFIG, FOO_CONNECTOR);

        connect.assertions().assertExactlyNumErrorsOnConnectorConfigValidation(fooProps.get(CONNECTOR_CLASS_CONFIG), fooProps, 0,
                "Validating connector configuration produced an unexpected number or errors.");

        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(FOO_CONNECTOR, NUM_TASKS,
                "Connector tasks did not start in time.");

        connect.assertions().assertKafkaTopicExists(FOO_TOPIC, 1,
                "Topic: " + FOO_TOPIC + " does not exist or does not have number of partitions: " + 1);
    }

    @Test
    public void testSwitchingToTopicCreationEnabled() throws InterruptedException {
        workerProps.put(TOPIC_CREATION_ENABLE_CONFIG, String.valueOf(false));
        connect = connectBuilder.build();
        // start the clusters
        connect.start();

        connect.kafka().createTopic(FOO_TOPIC, 1);
        connect.assertions().assertAtLeastNumWorkersAreUp(NUM_WORKERS, "Initial group of workers did not start in time.");

        Map<String, String> fooProps = defaultSourceConnectorProps(FOO_TOPIC);
        // start a source connector without topic creation properties
        connect.configureConnector(FOO_CONNECTOR, fooProps);
        fooProps.put(NAME_CONFIG, FOO_CONNECTOR);

        Map<String, String> barProps = sourceConnectorPropsWithGroups(BAR_TOPIC);
        // start a source connector with topic creation properties
        connect.configureConnector(BAR_CONNECTOR, barProps);
        barProps.put(NAME_CONFIG, BAR_CONNECTOR);

        connect.assertions().assertExactlyNumErrorsOnConnectorConfigValidation(fooProps.get(CONNECTOR_CLASS_CONFIG), fooProps, 0,
                "Validating connector configuration produced an unexpected number or errors.");

        connect.assertions().assertExactlyNumErrorsOnConnectorConfigValidation(barProps.get(CONNECTOR_CLASS_CONFIG), barProps, 0,
                "Validating connector configuration produced an unexpected number or errors.");

        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(FOO_CONNECTOR, NUM_TASKS,
                "Connector tasks did not start in time.");

        connect.assertions().assertKafkaTopicExists(FOO_TOPIC, 1,
                "Topic: " + FOO_TOPIC + " does not exist or does not have number of partitions: " + 1);

        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(BAR_CONNECTOR, NUM_TASKS,
                "Connector tasks did not start in time.");

        connect.activeWorkers().forEach(w -> connect.removeWorker(w));

        workerProps.put(TOPIC_CREATION_ENABLE_CONFIG, String.valueOf(true));

        IntStream.range(0, 3).forEach(i -> connect.addWorker());
        connect.assertions().assertAtLeastNumWorkersAreUp(NUM_WORKERS, "Initial group of workers did not start in time.");

        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(FOO_CONNECTOR, NUM_TASKS,
                "Connector tasks did not start in time.");

        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(BAR_CONNECTOR, NUM_TASKS,
                "Connector tasks did not start in time.");

        connect.assertions().assertKafkaTopicExists(BAR_TOPIC, 1,
                "Topic: " + BAR_TOPIC + " does not exist or does not have number of partitions: " + 1);
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

    private Map<String, String> sourceConnectorPropsWithGroups(String topic) {
        // setup up props for the source connector
        Map<String, String> props = defaultSourceConnectorProps(topic);
        props.put(TOPIC_CREATION_GROUPS_CONFIG, String.join(",", FOO_GROUP, BAR_GROUP));
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + REPLICATION_FACTOR_CONFIG, String.valueOf(1));
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + PARTITIONS_CONFIG, String.valueOf(1));
        props.put(SourceConnectorConfig.TOPIC_CREATION_PREFIX + FOO_GROUP + "." + INCLUDE_REGEX_CONFIG, FOO_TOPIC);
        props.put(SourceConnectorConfig.TOPIC_CREATION_PREFIX + FOO_GROUP + "." + EXCLUDE_REGEX_CONFIG, BAR_TOPIC);
        return props;
    }
}
