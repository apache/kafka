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
import org.apache.kafka.connect.util.MonitorableSinkConnector;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.IntegrationTest;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

@Category(IntegrationTest.class)
public class DeadLetterQueueIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterQueueIntegrationTest.class);

    private static final String DLQ_TOPIC = "my-connector-errors";

    @ClassRule
    public static EmbeddedConnectCluster connect = new EmbeddedConnectCluster();

    @Test
    public void testTransformationErrorHandlingWithDeadLetterQueue() throws Exception {
        // create test topic
        connect.kafka().createTopic("test-topic");

        // produce some strings into test topic
        for (int i = 0; i < 10; i++) {
            connect.kafka().produce("test-topic", "hello-" + i, String.valueOf(i));
            connect.kafka().produce("test-topic", "world-" + i, String.valueOf(i));
        }

        // consume all records from test topic
        log.info("Consuming records from test topic");
        for (ConsumerRecord<byte[], byte[]> recs : connect.kafka().consumeNRecords(20, 5000, "test-topic")) {
            log.info("Consumed record ({}, {}) from topic {}", recs.key(), new String(recs.value()), recs.topic());
        }

        Map<String, String> confs = new HashMap<>();
        confs.put("connector.class", "MonitorableSink");
        confs.put("task.max", "2");
        confs.put("topics", "test-topic");
        confs.put("key.converter", StringConverter.class.getName());
        confs.put("value.converter", StringConverter.class.getName());

        confs.put("errors.log.enable", "true");
        confs.put("errors.log.include.messages", "false");

        confs.put("errors.deadletterqueue.topic.name", DLQ_TOPIC);
        confs.put("errors.deadletterqueue.topic.replication.factor", "1");
        confs.put("errors.tolerance", "all");
        confs.put("transforms", "failing_transform");
        confs.put("transforms.failing_transform.type", FaultyPassthrough.class.getName());

        connect.startConnector("simple-conn", confs);

        int attempts = 0;
        while (attempts++ < 5 && MonitorableSinkConnector.COUNTER.get() < 18) {
            log.info("Received only {} records. Waiting .. ", MonitorableSinkConnector.COUNTER.get());
            Thread.sleep(500);
        }

        Assert.assertEquals(18, MonitorableSinkConnector.COUNTER.get());

        // consume failed records from dead letter queue topic
        log.info("Consuming records from test topic");
        for (ConsumerRecord<byte[], byte[]> recs : connect.kafka().consumeNRecords(2, 5000, DLQ_TOPIC)) {
            log.info("Consumed record ({}, {}) from dead letter queue topic {}", new String(recs.key()), new String(recs.value()), DLQ_TOPIC);
        }

        connect.deleteConnector("simple-conn");

    }

    public static class FaultyPassthrough<R extends ConnectRecord<R>> implements Transformation<R> {

        private static final Logger log = LoggerFactory.getLogger(FaultyPassthrough.class);

        public static final ConfigDef CONFIG_DEF = new ConfigDef();

        private int invocations = 0;

        @Override
        public R apply(R record) {
            long val = Long.parseLong(String.valueOf(record.value()));
            if (val == 7) {
                log.debug("Failing record: {} at invocations={}", record, invocations);
                throw new RetriableException("Bad invocations " + invocations + " for val = " + val);
            }
            return record;
        }

        @Override
        public ConfigDef config() {
            return CONFIG_DEF;
        }

        @Override
        public void close() {
            log.info("Shutting down transform");
        }

        @Override
        public void configure(Map<String, ?> configs) {
            log.info("Configuring {}.", this.getClass());
        }
    }

}
