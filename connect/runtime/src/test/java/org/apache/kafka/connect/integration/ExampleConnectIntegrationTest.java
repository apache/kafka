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
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.TOPICS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.REST_ADVERTISED_HOST_NAME_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.REST_ADVERTISED_PORT_CONFIG;
import static org.junit.Assert.assertEquals;

/**
 * An example integration test that demonstrates how to setup an integration test for Connect.
 * <p></p>
 * The following test configures and executes up a sink connector pipeline in a worker, produces messages into
 * the source topic-partitions, and demonstrates how to check the overall behavior of the pipeline.
 */
@Category(IntegrationTest.class)
public class ExampleConnectIntegrationTest {

    private static final int NUM_RECORDS_PRODUCED = 2000;
    private static final int NUM_TOPIC_PARTITIONS = 2;
    private static final int CONSUME_MAX_DURATION_MS = 5000;
    private static final String CONNECTOR_NAME = "simple-conn";
    private static final String TASK_1_ID = "simple-conn-0";
    private static final String TASK_2_ID = "simple-conn-1";

    private EmbeddedConnectCluster connect;
    private ConnectorHandle connectorHandle;

    @Before
    public void setup() throws IOException {
        // setup Connect worker properties
        Map<String, String> exampleWorkerProps = new HashMap<>();
        exampleWorkerProps.put(REST_ADVERTISED_HOST_NAME_CONFIG, "integration.test.host.io");
        exampleWorkerProps.put(REST_ADVERTISED_PORT_CONFIG, "8083");

        // setup Kafka broker properties
        Properties exampleBrokerProps = new Properties();
        exampleBrokerProps.put("auto.create.topics.enable", "false");

        // build a Connect cluster backed by Kakfa and Zk
        connect = new EmbeddedConnectCluster.Builder()
                .name("example-cluster")
                .numWorkers(1)
                .numBrokers(1)
                .workerProps(exampleWorkerProps)
                .brokerProps(exampleBrokerProps)
                .build();

        // start the clusters
        connect.start();

        // get a handle to the connector
        connectorHandle = RuntimeHandles.get().connectorHandle(CONNECTOR_NAME);
    }

    @After
    public void close() {
        // delete used tasks
        connectorHandle.deleteTask(TASK_1_ID);
        connectorHandle.deleteTask(TASK_2_ID);

        // delete connector handle
        RuntimeHandles.get().deleteConnector(CONNECTOR_NAME);

        // stop all Connect, Kakfa and Zk threads.
        connect.stop();
    }

    /**
     * Simple test case to configure and execute an embedded Connect cluster. The test will produce and consume
     * records, and start up a sink connector which will consume these records.
     */
    @Test
    public void testProduceConsumeConnector() throws Exception {
        // create test topic
        connect.kafka().createTopic("test-topic", NUM_TOPIC_PARTITIONS);

        // setup up props for the sink connector
        Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR_CLASS_CONFIG, "MonitorableSink");
        props.put(TASKS_MAX_CONFIG, "2");
        props.put(TOPICS_CONFIG, "test-topic");
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());

        // start a sink connector
        connect.configureConnector(CONNECTOR_NAME, props);

        // expect equal number of records for both tasks
        connectorHandle.taskHandle(TASK_1_ID).expectedRecords(NUM_RECORDS_PRODUCED / NUM_TOPIC_PARTITIONS);
        connectorHandle.taskHandle(TASK_2_ID).expectedRecords(NUM_RECORDS_PRODUCED / NUM_TOPIC_PARTITIONS);

        // wait for partition assignment
        connectorHandle.taskHandle(TASK_1_ID).awaitPartitionAssignment(CONSUME_MAX_DURATION_MS);
        connectorHandle.taskHandle(TASK_2_ID).awaitPartitionAssignment(CONSUME_MAX_DURATION_MS);

        // check that the REST API returns two tasks
        assertEquals("Incorrect task count in connector", 2, connect.getConnectorStatus(CONNECTOR_NAME).tasks().size());

        // produce some messages into source topic partitions
        for (int i = 0; i < NUM_RECORDS_PRODUCED; i++) {
            connect.kafka().produce("test-topic", i % NUM_TOPIC_PARTITIONS, "key", "simple-message-value-" + i);
        }

        // consume all records from the source topic or fail, to ensure that they were correctly produced.
        connect.kafka().consume(NUM_RECORDS_PRODUCED, CONSUME_MAX_DURATION_MS, "test-topic");

        // wait for the connector tasks to consume desired number of records.
        connectorHandle.taskHandle(TASK_1_ID).awaitRecords(CONSUME_MAX_DURATION_MS);
        connectorHandle.taskHandle(TASK_2_ID).awaitRecords(CONSUME_MAX_DURATION_MS);

        // delete connector
        connect.deleteConnector(CONNECTOR_NAME);
    }
}
