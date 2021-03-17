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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.transforms.Filter;
import org.apache.kafka.connect.transforms.predicates.HasHeaderKey;
import org.apache.kafka.connect.transforms.predicates.RecordIsTombstone;
import org.apache.kafka.connect.transforms.predicates.TopicNameMatches;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonMap;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.PREDICATES_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TRANSFORMS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.TOPICS_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.DEFAULT_TOPIC_CREATION_PREFIX;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.PARTITIONS_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * An integration test for connectors with transformations
 */
@Category(IntegrationTest.class)
public class TransformationIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(TransformationIntegrationTest.class);

    private static final int NUM_RECORDS_PRODUCED = 2000;
    private static final int NUM_TOPIC_PARTITIONS = 3;
    private static final long RECORD_TRANSFER_DURATION_MS = TimeUnit.SECONDS.toMillis(30);
    private static final long OBSERVED_RECORDS_DURATION_MS = TimeUnit.SECONDS.toMillis(60);
    private static final int NUM_TASKS = 1;
    private static final int NUM_WORKERS = 3;
    private static final String CONNECTOR_NAME = "simple-conn";
    private static final String SINK_CONNECTOR_CLASS_NAME = MonitorableSinkConnector.class.getSimpleName();
    private static final String SOURCE_CONNECTOR_CLASS_NAME = MonitorableSourceConnector.class.getSimpleName();

    private EmbeddedConnectCluster connect;
    private ConnectorHandle connectorHandle;

    @Before
    public void setup() {
        // setup Connect worker properties
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put(OFFSET_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(5_000));

        // setup Kafka broker properties
        Properties brokerProps = new Properties();
        // This is required because tests in this class also test per-connector topic creation with transformations
        brokerProps.put("auto.create.topics.enable", "false");

        // build a Connect cluster backed by Kafka and Zk
        connect = new EmbeddedConnectCluster.Builder()
                .name("connect-cluster")
                .numWorkers(NUM_WORKERS)
                .numBrokers(1)
                .workerProps(workerProps)
                .brokerProps(brokerProps)
                .build();

        // start the clusters
        connect.start();

        // get a handle to the connector
        connectorHandle = RuntimeHandles.get().connectorHandle(CONNECTOR_NAME);
    }

    @After
    public void close() {
        // delete connector handle
        RuntimeHandles.get().deleteConnector(CONNECTOR_NAME);

        // stop all Connect, Kafka and Zk threads.
        connect.stop();
    }

    /**
     * Test the {@link Filter} transformer with a
     * {@link TopicNameMatches} predicate on a sink connector.
     */
    @Test
    public void testFilterOnTopicNameWithSinkConnector() throws Exception {
        assertConnectReady();

        Map<String, Long> observedRecords = observeRecords();

        // create test topics
        String fooTopic = "foo-topic";
        String barTopic = "bar-topic";
        int numFooRecords = NUM_RECORDS_PRODUCED;
        int numBarRecords = NUM_RECORDS_PRODUCED;
        connect.kafka().createTopic(fooTopic, NUM_TOPIC_PARTITIONS);
        connect.kafka().createTopic(barTopic, NUM_TOPIC_PARTITIONS);

        // setup up props for the sink connector
        Map<String, String> props = new HashMap<>();
        props.put("name", CONNECTOR_NAME);
        props.put(CONNECTOR_CLASS_CONFIG, SINK_CONNECTOR_CLASS_NAME);
        props.put(TASKS_MAX_CONFIG, String.valueOf(NUM_TASKS));
        props.put(TOPICS_CONFIG, String.join(",", fooTopic, barTopic));
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(TRANSFORMS_CONFIG, "filter");
        props.put(TRANSFORMS_CONFIG + ".filter.type", Filter.class.getSimpleName());
        props.put(TRANSFORMS_CONFIG + ".filter.predicate", "barPredicate");
        props.put(PREDICATES_CONFIG, "barPredicate");
        props.put(PREDICATES_CONFIG + ".barPredicate.type", TopicNameMatches.class.getSimpleName());
        props.put(PREDICATES_CONFIG + ".barPredicate.pattern", "bar-.*");

        // expect all records to be consumed by the connector
        connectorHandle.expectedRecords(numFooRecords);

        // expect all records to be consumed by the connector
        connectorHandle.expectedCommits(numFooRecords);

        // start a sink connector
        connect.configureConnector(CONNECTOR_NAME, props);
        assertConnectorRunning();

        // produce some messages into source topic partitions
        for (int i = 0; i < numBarRecords; i++) {
            connect.kafka().produce(barTopic, i % NUM_TOPIC_PARTITIONS, "key", "simple-message-value-" + i);
        }
        for (int i = 0; i < numFooRecords; i++) {
            connect.kafka().produce(fooTopic, i % NUM_TOPIC_PARTITIONS, "key", "simple-message-value-" + i);
        }

        // consume all records from the source topic or fail, to ensure that they were correctly produced.
        assertEquals("Unexpected number of records consumed", numFooRecords,
                connect.kafka().consume(numFooRecords, RECORD_TRANSFER_DURATION_MS, fooTopic).count());
        assertEquals("Unexpected number of records consumed", numBarRecords,
                connect.kafka().consume(numBarRecords, RECORD_TRANSFER_DURATION_MS, barTopic).count());

        // wait for the connector tasks to consume all records.
        connectorHandle.awaitRecords(RECORD_TRANSFER_DURATION_MS);

        // wait for the connector tasks to commit all records.
        connectorHandle.awaitCommits(RECORD_TRANSFER_DURATION_MS);

        // Assert that we didn't see any baz
        Map<String, Long> expectedRecordCounts = singletonMap(fooTopic, (long) numFooRecords);
        assertObservedRecords(observedRecords, expectedRecordCounts);

        // delete connector
        connect.deleteConnector(CONNECTOR_NAME);
    }

    private void assertConnectReady() throws InterruptedException {
        connect.assertions().assertExactlyNumBrokersAreUp(1, "Brokers did not start in time.");
        connect.assertions().assertExactlyNumWorkersAreUp(NUM_WORKERS, "Worker did not start in time.");
        log.info("Completed startup of {} Kafka brokers and {} Connect workers", 1, NUM_WORKERS);
    }

    private void assertConnectorRunning() throws InterruptedException {
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, NUM_TASKS,
                "Connector tasks did not start in time.");
    }

    private void assertObservedRecords(Map<String, Long> observedRecords, Map<String, Long> expectedRecordCounts) throws InterruptedException {
        waitForCondition(() -> expectedRecordCounts.equals(observedRecords),
            OBSERVED_RECORDS_DURATION_MS,
            () -> "The observed records should be " + expectedRecordCounts + " but was " + observedRecords);
    }

    private Map<String, Long> observeRecords() {
        Map<String, Long> observedRecords = new HashMap<>();
        // record all the record we see
        connectorHandle.taskHandle(CONNECTOR_NAME + "-0",
            record -> observedRecords.compute(record.topic(),
                (key, value) -> value == null ? 1 : value + 1));
        return observedRecords;
    }

    /**
     * Test the {@link Filter} transformer with a
     * {@link RecordIsTombstone} predicate on a sink connector.
     */
    @Test
    public void testFilterOnTombstonesWithSinkConnector() throws Exception {
        assertConnectReady();

        Map<String, Long> observedRecords = observeRecords();

        // create test topics
        String topic = "foo-topic";
        int numRecords = NUM_RECORDS_PRODUCED;
        connect.kafka().createTopic(topic, NUM_TOPIC_PARTITIONS);

        // setup up props for the sink connector
        Map<String, String> props = new HashMap<>();
        props.put("name", CONNECTOR_NAME);
        props.put(CONNECTOR_CLASS_CONFIG, SINK_CONNECTOR_CLASS_NAME);
        props.put(TASKS_MAX_CONFIG, String.valueOf(NUM_TASKS));
        props.put(TOPICS_CONFIG, String.join(",", topic));
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(TRANSFORMS_CONFIG, "filter");
        props.put(TRANSFORMS_CONFIG + ".filter.type", Filter.class.getSimpleName());
        props.put(TRANSFORMS_CONFIG + ".filter.predicate", "barPredicate");
        props.put(PREDICATES_CONFIG, "barPredicate");
        props.put(PREDICATES_CONFIG + ".barPredicate.type", RecordIsTombstone.class.getSimpleName());

        // expect only half the records to be consumed by the connector
        connectorHandle.expectedCommits(numRecords);
        connectorHandle.expectedRecords(numRecords / 2);

        // start a sink connector
        connect.configureConnector(CONNECTOR_NAME, props);
        assertConnectorRunning();

        // produce some messages into source topic partitions
        for (int i = 0; i < numRecords; i++) {
            connect.kafka().produce(topic, i % NUM_TOPIC_PARTITIONS, "key", i % 2 == 0 ? "simple-message-value-" + i : null);
        }

        // consume all records from the source topic or fail, to ensure that they were correctly produced.
        assertEquals("Unexpected number of records consumed", numRecords,
                connect.kafka().consume(numRecords, RECORD_TRANSFER_DURATION_MS, topic).count());

        // wait for the connector tasks to consume all records.
        connectorHandle.awaitRecords(RECORD_TRANSFER_DURATION_MS);

        // wait for the connector tasks to commit all records.
        connectorHandle.awaitCommits(RECORD_TRANSFER_DURATION_MS);

        Map<String, Long> expectedRecordCounts = singletonMap(topic, (long) (numRecords / 2));
        assertObservedRecords(observedRecords, expectedRecordCounts);

        // delete connector
        connect.deleteConnector(CONNECTOR_NAME);
    }

    /**
     * Test the {@link Filter} transformer with a {@link HasHeaderKey} predicate on a source connector.
     * Note that this test uses topic creation configs to allow the source connector to create
     * the topic when it tries to produce the first source record, instead of requiring the topic
     * to exist before the connector starts.
     */
    @Test
    public void testFilterOnHasHeaderKeyWithSourceConnectorAndTopicCreation() throws Exception {
        assertConnectReady();

        // setup up props for the sink connector
        Map<String, String> props = new HashMap<>();
        props.put("name", CONNECTOR_NAME);
        props.put(CONNECTOR_CLASS_CONFIG, SOURCE_CONNECTOR_CLASS_NAME);
        props.put(TASKS_MAX_CONFIG, String.valueOf(NUM_TASKS));
        props.put("topic", "test-topic");
        props.put("throughput", String.valueOf(500));
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(TRANSFORMS_CONFIG, "filter");
        props.put(TRANSFORMS_CONFIG + ".filter.type", Filter.class.getSimpleName());
        props.put(TRANSFORMS_CONFIG + ".filter.predicate", "headerPredicate");
        props.put(TRANSFORMS_CONFIG + ".filter.negate", "true");
        props.put(PREDICATES_CONFIG, "headerPredicate");
        props.put(PREDICATES_CONFIG + ".headerPredicate.type", HasHeaderKey.class.getSimpleName());
        props.put(PREDICATES_CONFIG + ".headerPredicate.name", "header-8");
        // custom topic creation is used, so there's no need to proactively create the test topic
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + REPLICATION_FACTOR_CONFIG, String.valueOf(-1));
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + PARTITIONS_CONFIG, String.valueOf(NUM_TOPIC_PARTITIONS));

        // expect all records to be produced by the connector
        connectorHandle.expectedRecords(NUM_RECORDS_PRODUCED);

        // expect all records to be produced by the connector
        connectorHandle.expectedCommits(NUM_RECORDS_PRODUCED);

        // validate the intended connector configuration, a valid config
        connect.assertions().assertExactlyNumErrorsOnConnectorConfigValidation(SOURCE_CONNECTOR_CLASS_NAME, props, 0,
                "Validating connector configuration produced an unexpected number or errors.");

        // start a source connector
        connect.configureConnector(CONNECTOR_NAME, props);
        assertConnectorRunning();

        // wait for the connector tasks to produce enough records
        connectorHandle.awaitRecords(RECORD_TRANSFER_DURATION_MS);

        // wait for the connector tasks to commit enough records
        connectorHandle.awaitCommits(RECORD_TRANSFER_DURATION_MS);

        // consume all records from the source topic or fail, to ensure that they were correctly produced
        for (ConsumerRecord<byte[], byte[]> record : connect.kafka().consume(1, RECORD_TRANSFER_DURATION_MS, "test-topic")) {
            assertNotNull("Expected header to exist",
                    record.headers().lastHeader("header-8"));
        }

        // delete connector
        connect.deleteConnector(CONNECTOR_NAME);
    }
}
