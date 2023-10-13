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
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.ERRORS_LOG_ENABLE_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.ERRORS_LOG_INCLUDE_MESSAGES_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.ERRORS_RETRY_TIMEOUT_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.ERRORS_TOLERANCE_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TRANSFORMS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.DLQ_CONTEXT_HEADERS_ENABLE_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.DLQ_TOPIC_NAME_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.DLQ_TOPIC_REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.TOPICS_CONFIG;
import static org.apache.kafka.connect.runtime.errors.DeadLetterQueueReporter.ERROR_HEADER_EXCEPTION;
import static org.apache.kafka.connect.runtime.errors.DeadLetterQueueReporter.ERROR_HEADER_EXCEPTION_MESSAGE;
import static org.apache.kafka.connect.runtime.errors.DeadLetterQueueReporter.ERROR_HEADER_ORIG_TOPIC;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Integration test for the different error handling policies in Connect (namely, retry policies, skipping bad records,
 * and dead letter queues).
 */
@Category(IntegrationTest.class)
public class ErrorHandlingIntegrationTest {
    @Rule
    public Timeout globalTimeout = Timeout.seconds(600);
    private static final Logger log = LoggerFactory.getLogger(ErrorHandlingIntegrationTest.class);
    private static final int NUM_WORKERS = 1;
    private static final String DLQ_TOPIC = "my-connector-errors";
    private static final String CONNECTOR_NAME = "error-conn";
    private static final String TASK_ID = "error-conn-0";
    private static final int NUM_RECORDS_PRODUCED = 20;
    private static final int EXPECTED_CORRECT_RECORDS = 19;
    private static final int EXPECTED_INCORRECT_RECORDS = 1;
    private static final int NUM_TASKS = 1;
    private static final long CONNECTOR_SETUP_DURATION_MS = TimeUnit.SECONDS.toMillis(60);
    private static final long CONSUME_MAX_DURATION_MS = TimeUnit.SECONDS.toMillis(30);

    private EmbeddedConnectCluster connect;
    private ConnectorHandle connectorHandle;

    @Before
    public void setup() throws InterruptedException {
        // setup Connect cluster with defaults
        connect = new EmbeddedConnectCluster.Builder().build();

        // start Connect cluster
        connect.start();
        connect.assertions().assertAtLeastNumWorkersAreUp(NUM_WORKERS,
                "Initial group of workers did not start in time.");

        // get connector handles before starting test.
        connectorHandle = RuntimeHandles.get().connectorHandle(CONNECTOR_NAME);
    }

    @After
    public void close() {
        RuntimeHandles.get().deleteConnector(CONNECTOR_NAME);
        connect.stop();
    }

    @Test
    public void testSkipRetryAndDLQWithHeaders() throws Exception {
        // create test topic
        connect.kafka().createTopic("test-topic");

        // setup connector config
        Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR_CLASS_CONFIG, MonitorableSinkConnector.class.getSimpleName());
        props.put(TASKS_MAX_CONFIG, String.valueOf(NUM_TASKS));
        props.put(TOPICS_CONFIG, "test-topic");
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(TRANSFORMS_CONFIG, "failing_transform");
        props.put("transforms.failing_transform.type", FaultyPassthrough.class.getName());

        // log all errors, along with message metadata
        props.put(ERRORS_LOG_ENABLE_CONFIG, "true");
        props.put(ERRORS_LOG_INCLUDE_MESSAGES_CONFIG, "true");

        // produce bad messages into dead letter queue
        props.put(DLQ_TOPIC_NAME_CONFIG, DLQ_TOPIC);
        props.put(DLQ_CONTEXT_HEADERS_ENABLE_CONFIG, "true");
        props.put(DLQ_TOPIC_REPLICATION_FACTOR_CONFIG, "1");

        // tolerate all errors
        props.put(ERRORS_TOLERANCE_CONFIG, "all");

        // retry for up to one second
        props.put(ERRORS_RETRY_TIMEOUT_CONFIG, "1000");

        // set expected records to successfully reach the task
        connectorHandle.taskHandle(TASK_ID).expectedRecords(EXPECTED_CORRECT_RECORDS);

        connect.configureConnector(CONNECTOR_NAME, props);
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, NUM_TASKS,
                "Connector tasks did not start in time.");

        waitForCondition(this::checkForPartitionAssignment,
                CONNECTOR_SETUP_DURATION_MS,
                "Connector task was not assigned a partition.");

        // produce some strings into test topic
        for (int i = 0; i < NUM_RECORDS_PRODUCED; i++) {
            connect.kafka().produce("test-topic", "key-" + i, "value-" + i);
        }

        // consume all records from test topic
        log.info("Consuming records from test topic");
        int i = 0;
        for (ConsumerRecord<byte[], byte[]> rec : connect.kafka().consume(NUM_RECORDS_PRODUCED, CONSUME_MAX_DURATION_MS, "test-topic")) {
            String k = new String(rec.key());
            String v = new String(rec.value());
            log.debug("Consumed record (key='{}', value='{}') from topic {}", k, v, rec.topic());
            assertEquals("Unexpected key", k, "key-" + i);
            assertEquals("Unexpected value", v, "value-" + i);
            i++;
        }

        // wait for records to reach the task
        connectorHandle.taskHandle(TASK_ID).awaitRecords(CONSUME_MAX_DURATION_MS);

        // consume failed records from dead letter queue topic
        log.info("Consuming records from test topic");
        ConsumerRecords<byte[], byte[]> messages = connect.kafka().consume(EXPECTED_INCORRECT_RECORDS, CONSUME_MAX_DURATION_MS, DLQ_TOPIC);
        for (ConsumerRecord<byte[], byte[]> recs : messages) {
            log.debug("Consumed record (key={}, value={}) from dead letter queue topic {}",
                    new String(recs.key()), new String(recs.value()), DLQ_TOPIC);
            assertTrue(recs.headers().toArray().length > 0);
            assertValue("test-topic", recs.headers(), ERROR_HEADER_ORIG_TOPIC);
            assertValue(RetriableException.class.getName(), recs.headers(), ERROR_HEADER_EXCEPTION);
            assertValue("Error when value='value-7'", recs.headers(), ERROR_HEADER_EXCEPTION_MESSAGE);
        }

        connect.deleteConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorDoesNotExist(CONNECTOR_NAME,
                "Connector wasn't deleted in time.");

    }

    @Test
    public void testErrantRecordReporter() throws Exception {
        // create test topic
        connect.kafka().createTopic("test-topic");

        // setup connector config
        Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR_CLASS_CONFIG, ErrantRecordSinkConnector.class.getSimpleName());
        props.put(TASKS_MAX_CONFIG, String.valueOf(NUM_TASKS));
        props.put(TOPICS_CONFIG, "test-topic");
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());

        // log all errors, along with message metadata
        props.put(ERRORS_LOG_ENABLE_CONFIG, "true");
        props.put(ERRORS_LOG_INCLUDE_MESSAGES_CONFIG, "true");

        // produce bad messages into dead letter queue
        props.put(DLQ_TOPIC_NAME_CONFIG, DLQ_TOPIC);
        props.put(DLQ_CONTEXT_HEADERS_ENABLE_CONFIG, "true");
        props.put(DLQ_TOPIC_REPLICATION_FACTOR_CONFIG, "1");

        // tolerate all errors
        props.put(ERRORS_TOLERANCE_CONFIG, "all");

        // retry for up to one second
        props.put(ERRORS_RETRY_TIMEOUT_CONFIG, "1000");

        // set expected records to successfully reach the task
        connectorHandle.taskHandle(TASK_ID).expectedRecords(EXPECTED_CORRECT_RECORDS);

        connect.configureConnector(CONNECTOR_NAME, props);
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, NUM_TASKS,
            "Connector tasks did not start in time.");

        waitForCondition(this::checkForPartitionAssignment,
            CONNECTOR_SETUP_DURATION_MS,
            "Connector task was not assigned a partition.");

        // produce some strings into test topic
        for (int i = 0; i < NUM_RECORDS_PRODUCED; i++) {
            connect.kafka().produce("test-topic", "key-" + i, "value-" + i);
        }

        // consume all records from test topic
        log.info("Consuming records from test topic");
        int i = 0;
        for (ConsumerRecord<byte[], byte[]> rec : connect.kafka().consume(NUM_RECORDS_PRODUCED, CONSUME_MAX_DURATION_MS, "test-topic")) {
            String k = new String(rec.key());
            String v = new String(rec.value());
            log.debug("Consumed record (key='{}', value='{}') from topic {}", k, v, rec.topic());
            assertEquals("Unexpected key", k, "key-" + i);
            assertEquals("Unexpected value", v, "value-" + i);
            i++;
        }

        // wait for records to reach the task
        connectorHandle.taskHandle(TASK_ID).awaitRecords(CONSUME_MAX_DURATION_MS);

        // consume failed records from dead letter queue topic
        log.info("Consuming records from test topic");
        ConsumerRecords<byte[], byte[]> messages = connect.kafka().consume(EXPECTED_INCORRECT_RECORDS, CONSUME_MAX_DURATION_MS, DLQ_TOPIC);

        connect.deleteConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorDoesNotExist(CONNECTOR_NAME,
            "Connector wasn't deleted in time.");
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

    private void assertValue(String expected, Headers headers, String headerKey) {
        byte[] actual = headers.lastHeader(headerKey).value();
        if (expected == null && actual == null) {
            return;
        }
        if (expected == null || actual == null) {
            fail();
        }
        assertEquals(expected, new String(actual));
    }

    public static class FaultyPassthrough<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

        static final ConfigDef CONFIG_DEF = new ConfigDef();

        /**
         * An arbitrary id which causes this transformation to fail with a {@link RetriableException}, but succeeds
         * on subsequent attempt.
         */
        static final int BAD_RECORD_VAL_RETRIABLE = 4;

        /**
         * An arbitrary id which causes this transformation to fail with a {@link RetriableException}.
         */
        static final int BAD_RECORD_VAL = 7;

        private boolean shouldFail = true;

        @Override
        public String version() {
            return "1.0";
        }

        @Override
        public R apply(R record) {
            String badValRetriable = "value-" + BAD_RECORD_VAL_RETRIABLE;
            if (badValRetriable.equals(record.value()) && shouldFail) {
                shouldFail = false;
                throw new RetriableException("Error when value='" + badValRetriable
                        + "'. A reattempt with this record will succeed.");
            }
            String badVal = "value-" + BAD_RECORD_VAL;
            if (badVal.equals(record.value())) {
                throw new RetriableException("Error when value='" + badVal + "'");
            }
            return record;
        }

        @Override
        public ConfigDef config() {
            return CONFIG_DEF;
        }

        @Override
        public void close() {
        }

        @Override
        public void configure(Map<String, ?> configs) {
        }
    }
}
