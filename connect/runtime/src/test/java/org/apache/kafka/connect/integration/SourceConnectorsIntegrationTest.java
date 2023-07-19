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

import java.util.Collections;
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
import static org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster.DEFAULT_NUM_BROKERS;

/**
 * Integration test for source connectors with a focus on topic creation with custom properties by
 * the connector tasks.
 */
@Category(IntegrationTest.class)
public class SourceConnectorsIntegrationTest {

    private static final int NUM_WORKERS = 3;
    private static final int NUM_TASKS = 1;
    private static final String FOO_TOPIC = "foo-topic";
    private static final String FOO_CONNECTOR = "foo-source";
    private static final String BAR_TOPIC = "bar-topic";
    private static final String BAR_CONNECTOR = "bar-source";
    private static final String FOO_GROUP = "foo";
    private static final String BAR_GROUP = "bar";
    private static final int DEFAULT_REPLICATION_FACTOR = DEFAULT_NUM_BROKERS;
    private static final int DEFAULT_PARTITIONS = 1;
    private static final int FOO_GROUP_REPLICATION_FACTOR = DEFAULT_NUM_BROKERS;
    private static final int FOO_GROUP_PARTITIONS = 9;

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
    public void testTopicsAreCreatedWhenAutoCreateTopicsIsEnabledAtTheBroker() throws InterruptedException {
        brokerProps.put("auto.create.topics.enable", String.valueOf(true));
        workerProps.put(TOPIC_CREATION_ENABLE_CONFIG, String.valueOf(false));
        connect = connectBuilder.brokerProps(brokerProps).workerProps(workerProps).build();
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

        connect.assertions().assertTopicsExist(FOO_TOPIC);
        connect.assertions().assertTopicSettings(FOO_TOPIC, DEFAULT_REPLICATION_FACTOR,
                DEFAULT_PARTITIONS, "Topic " + FOO_TOPIC + " does not have the expected settings");
    }

    @Test
    public void testTopicsAreCreatedWhenTopicCreationIsEnabled() throws InterruptedException {
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

        connect.assertions().assertTopicsExist(FOO_TOPIC);
        connect.assertions().assertTopicSettings(FOO_TOPIC, FOO_GROUP_REPLICATION_FACTOR,
                FOO_GROUP_PARTITIONS, "Topic " + FOO_TOPIC + " does not have the expected settings");
    }

    @Test
    public void testSwitchingToTopicCreationEnabled() throws InterruptedException {
        workerProps.put(TOPIC_CREATION_ENABLE_CONFIG, String.valueOf(false));
        connect = connectBuilder.build();
        // start the clusters
        connect.start();

        connect.kafka().createTopic(BAR_TOPIC, DEFAULT_PARTITIONS, DEFAULT_REPLICATION_FACTOR, Collections.emptyMap());

        connect.assertions().assertTopicsExist(BAR_TOPIC);
        connect.assertions().assertTopicSettings(BAR_TOPIC, DEFAULT_REPLICATION_FACTOR,
                DEFAULT_PARTITIONS, "Topic " + BAR_TOPIC + " does not have the expected settings");

        connect.assertions().assertAtLeastNumWorkersAreUp(NUM_WORKERS, "Initial group of workers did not start in time.");

        Map<String, String> barProps = defaultSourceConnectorProps(BAR_TOPIC);
        // start a source connector with topic creation properties
        connect.configureConnector(BAR_CONNECTOR, barProps);
        barProps.put(NAME_CONFIG, BAR_CONNECTOR);

        Map<String, String> fooProps = sourceConnectorPropsWithGroups(FOO_TOPIC);
        // start a source connector without topic creation properties
        connect.configureConnector(FOO_CONNECTOR, fooProps);
        fooProps.put(NAME_CONFIG, FOO_CONNECTOR);

        connect.assertions().assertExactlyNumErrorsOnConnectorConfigValidation(fooProps.get(CONNECTOR_CLASS_CONFIG), fooProps, 0,
                "Validating connector configuration produced an unexpected number or errors.");

        connect.assertions().assertExactlyNumErrorsOnConnectorConfigValidation(barProps.get(CONNECTOR_CLASS_CONFIG), barProps, 0,
                "Validating connector configuration produced an unexpected number or errors.");

        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(FOO_CONNECTOR, NUM_TASKS,
                "Connector tasks did not start in time.");

        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(BAR_CONNECTOR, NUM_TASKS,
                "Connector tasks did not start in time.");

        connect.assertions().assertTopicsExist(BAR_TOPIC);
        connect.assertions().assertTopicSettings(BAR_TOPIC, DEFAULT_REPLICATION_FACTOR,
                DEFAULT_PARTITIONS, "Topic " + BAR_TOPIC + " does not have the expected settings");

        connect.assertions().assertTopicsDoNotExist(FOO_TOPIC);

        connect.activeWorkers().forEach(w -> connect.removeWorker(w));

        workerProps.put(TOPIC_CREATION_ENABLE_CONFIG, String.valueOf(true));

        IntStream.range(0, 3).forEach(i -> connect.addWorker());
        connect.assertions().assertAtLeastNumWorkersAreUp(NUM_WORKERS, "Initial group of workers did not start in time.");

        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(FOO_CONNECTOR, NUM_TASKS,
                "Connector tasks did not start in time.");

        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(BAR_CONNECTOR, NUM_TASKS,
                "Connector tasks did not start in time.");

        connect.assertions().assertTopicsExist(FOO_TOPIC);
        connect.assertions().assertTopicSettings(FOO_TOPIC, FOO_GROUP_REPLICATION_FACTOR,
                FOO_GROUP_PARTITIONS, "Topic " + FOO_TOPIC + " does not have the expected settings");
        connect.assertions().assertTopicsExist(BAR_TOPIC);
        connect.assertions().assertTopicSettings(BAR_TOPIC, DEFAULT_REPLICATION_FACTOR,
                DEFAULT_PARTITIONS, "Topic " + BAR_TOPIC + " does not have the expected settings");
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
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + REPLICATION_FACTOR_CONFIG, String.valueOf(DEFAULT_REPLICATION_FACTOR));
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + PARTITIONS_CONFIG, String.valueOf(DEFAULT_PARTITIONS));
        props.put(SourceConnectorConfig.TOPIC_CREATION_PREFIX + FOO_GROUP + "." + INCLUDE_REGEX_CONFIG, FOO_TOPIC);
        props.put(SourceConnectorConfig.TOPIC_CREATION_PREFIX + FOO_GROUP + "." + EXCLUDE_REGEX_CONFIG, BAR_TOPIC);
        props.put(SourceConnectorConfig.TOPIC_CREATION_PREFIX + FOO_GROUP + "." + PARTITIONS_CONFIG,
                String.valueOf(FOO_GROUP_PARTITIONS));
        return props;
    }
}
