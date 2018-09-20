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
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Category(IntegrationTest.class)
public class DeadLetterQueueIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterQueueIntegrationTest.class);

    private static final String DLQ_TOPIC = "my-connector-errors";
    private static final int NUM_RECORDS_PRODUCED = 20;
    private static final int EXPECTED_CORRECT_RECORDS = 18;
    private static final int EXPECTED_INCORRECT_RECORDS = 2;
    private static final int CONSUME_MAX_DURATION_MS = 5000;

    private EmbeddedConnectCluster connect;

    @Before
    public void setup() throws IOException {
        connect = new EmbeddedConnectCluster(ConnectIntegrationTest.class);
        connect.start();
    }

    @After
    public void close() {
        connect.stop();
    }

    @Test
    public void testTransformationErrorHandling() throws Exception {
        // create test topic
        connect.kafka().createTopic("test-topic");

        // produce some strings into test topic
        for (int i = 0; i < NUM_RECORDS_PRODUCED / 2; i++) {
            connect.kafka().produce("test-topic", "hello-" + i, String.valueOf(i));
            connect.kafka().produce("test-topic", "world-" + i, String.valueOf(i));
        }

        // consume all records from test topic
        log.info("Consuming records from test topic");
        for (ConsumerRecord<byte[], byte[]> recs : connect.kafka().consume(NUM_RECORDS_PRODUCED, CONSUME_MAX_DURATION_MS, "test-topic")) {
            log.debug("Consumed record ({}, {}) from topic {}", new String(recs.key()), new String(recs.value()), recs.topic());
        }

        Map<String, String> props = new HashMap<>();
        props.put("connector.class", "MonitorableSink");
        props.put("task.max", "2");
        props.put("topics", "test-topic");
        props.put("key.converter", StringConverter.class.getName());
        props.put("value.converter", StringConverter.class.getName());

        props.put("errors.log.enable", "true");
        props.put("errors.log.include.messages", "false");

        props.put("errors.deadletterqueue.topic.name", DLQ_TOPIC);
        props.put("errors.deadletterqueue.topic.replication.factor", "1");
        props.put("errors.tolerance", "all");
        props.put("transforms", "failing_transform");
        props.put("transforms.failing_transform.type", FaultyPassthrough.class.getName());

        connect.startConnector("simple-conn", props);

        int attempts = 0;
        while (attempts++ < 5 && MonitorableSinkConnector.COUNTER.get() < EXPECTED_CORRECT_RECORDS) {
            log.info("Received only {} records. Waiting .. ", MonitorableSinkConnector.COUNTER.get());
            Thread.sleep(500);
        }

        Assert.assertEquals(EXPECTED_CORRECT_RECORDS, MonitorableSinkConnector.COUNTER.get());

        // consume failed records from dead letter queue topic
        log.info("Consuming records from test topic");
        for (ConsumerRecord<byte[], byte[]> recs : connect.kafka().consume(EXPECTED_INCORRECT_RECORDS, CONSUME_MAX_DURATION_MS, DLQ_TOPIC)) {
            log.info("Consumed record (key={}, value={}) from dead letter queue topic {}", new String(recs.key()), new String(recs.value()), DLQ_TOPIC);
        }

        connect.deleteConnector("simple-conn");

    }

    public static class FaultyPassthrough<R extends ConnectRecord<R>> implements Transformation<R> {

        static final ConfigDef CONFIG_DEF = new ConfigDef();
        /**
         * an arbitrary id which causes this transformation to fail with a {@link RetriableException}.
         */
        private static final int BAD_RECORD_VAL = 7;

        @Override
        public R apply(R record) {
            long val = Long.parseLong(String.valueOf(record.value()));
            if (val == BAD_RECORD_VAL) {
                throw new RetriableException("Error when when val=" + val);
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
