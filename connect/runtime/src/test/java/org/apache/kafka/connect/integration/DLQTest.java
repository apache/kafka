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
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DLQTest {

    private static final Logger log = LoggerFactory.getLogger(DLQTest.class);

    @ClassRule
    public static EmbeddedConnectCluster connect = new EmbeddedConnectCluster();

    @Test
    public void startConnect() {
        // create test topic
        connect.kafka().createTopic("test-topic");

        // produce some strings into test topic
        for (int i = 0; i < 1000; i++) {
            connect.kafka().produce("test-topic", "hello-" + i);
            connect.kafka().produce("test-topic", "world-" + i);
        }

        // consume all records from test topic
        log.info("Consuming records from test topic");
        for (ConsumerRecord<byte[], byte[]> recs : connect.kafka().consumeNRecords(2000, 5000, "test-topic")) {
            log.info("Consumed record ({}, {}) from topic {}", recs.key(), new String(recs.value()), recs.topic());
        }
    }
}
