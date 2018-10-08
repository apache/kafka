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
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Integration test for the different error handling policies in Connect (namely, retry policies, skipping bad records,
 * and dead letter queues).
 */
@Category(IntegrationTest.class)
public class ErrorHandlingIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(ErrorHandlingIntegrationTest.class);

    private static final String DLQ_TOPIC = "my-connector-errors";
    private static final int NUM_RECORDS_PRODUCED = 20;
    private static final int EXPECTED_CORRECT_RECORDS = 19;
    private static final int EXPECTED_INCORRECT_RECORDS = 1;
    private static final int CONSUME_MAX_DURATION_MS = 5000;

    private EmbeddedConnectCluster connect;

    @Before
    public void setup() throws IOException {
        // clean up connector status before starting test.
        MonitorableSinkConnector.cleanHandle("simple-conn-0");
        connect = new EmbeddedConnectCluster();
        connect.start();
    }

    @After
    public void close() {
        connect.stop();
    }

    @Test
    public void testSkipRetryAndDLQWithHeaders() throws Exception {
        // create test topic
        connect.kafka().createTopic("test-topic");

        // produce some strings into test topic
        for (int i = 0; i < NUM_RECORDS_PRODUCED; i++) {
            connect.kafka().produce("test-topic", "key-" + i, "value-" + String.valueOf(i));
        }

        // consume all records from test topic
        log.info("Consuming records from test topic");
        for (ConsumerRecord<byte[], byte[]> recs : connect.kafka().consume(NUM_RECORDS_PRODUCED, CONSUME_MAX_DURATION_MS, "test-topic")) {
            log.debug("Consumed record (key='{}', value='{}') from topic {}",
                    new String(recs.key()), new String(recs.value()), recs.topic());
        }

        Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR_CLASS_CONFIG, "MonitorableSink");
        props.put(TASKS_MAX_CONFIG, "1");
        props.put(TOPICS_CONFIG, "test-topic");
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(TRANSFORMS_CONFIG, "failing_transform");
        props.put("transforms.failing_transform.type", FaultyPassthrough.class.getName());

        props.put(MonitorableSinkConnector.EXPECTED_RECORDS, String.valueOf(EXPECTED_CORRECT_RECORDS));

        // log all errors, along with message metadata
        props.put(ERRORS_LOG_ENABLE_CONFIG, "true");
        props.put(ERRORS_LOG_INCLUDE_MESSAGES_CONFIG, "true");

        // produce bad messages into dead letter queue
        props.put(DLQ_TOPIC_NAME_CONFIG, DLQ_TOPIC);
        props.put(DLQ_CONTEXT_HEADERS_ENABLE_CONFIG, "true");
        props.put(DLQ_TOPIC_REPLICATION_FACTOR_CONFIG, "1");

        // tolerate all erros
        props.put(ERRORS_TOLERANCE_CONFIG, "all");

        // retry for up to one second
        props.put(ERRORS_RETRY_TIMEOUT_CONFIG, "1000");

        connect.configureConnector("simple-conn", props);

        MonitorableSinkConnector.taskInstances("simple-conn-0").task().awaitRecords(CONSUME_MAX_DURATION_MS);

        // consume failed records from dead letter queue topic
        log.info("Consuming records from test topic");
        ConsumerRecords<byte[], byte[]> messages = connect.kafka().consume(EXPECTED_INCORRECT_RECORDS, CONSUME_MAX_DURATION_MS, DLQ_TOPIC);
        for (ConsumerRecord<byte[], byte[]> recs : messages) {
            log.debug("Consumed record (key={}, value={}) from dead letter queue topic {}",
                    new String(recs.key()), new String(recs.value()), DLQ_TOPIC);
            assertTrue(recs.headers().toArray().length > 0);
            assertValue("test-topic", recs.headers(), ERROR_HEADER_ORIG_TOPIC);
            assertValue(RetriableException.class.getName(), recs.headers(), ERROR_HEADER_EXCEPTION);
            assertValue("Error when when value='value-7'", recs.headers(), ERROR_HEADER_EXCEPTION_MESSAGE);
        }

        connect.deleteConnector("simple-conn");
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

    public static class FaultyPassthrough<R extends ConnectRecord<R>> implements Transformation<R> {

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
        public R apply(R record) {
            String badValRetriable = "value-" + BAD_RECORD_VAL_RETRIABLE;
            if (badValRetriable.equals(record.value()) && shouldFail) {
                shouldFail = false;
                throw new RetriableException("Error when when value='" + badValRetriable
                        + "'. A reattempt with this record will succeed.");
            }
            String badVal = "value-" + BAD_RECORD_VAL;
            if (badVal.equals(record.value())) {
                throw new RetriableException("Error when when value='" + badVal + "'");
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
