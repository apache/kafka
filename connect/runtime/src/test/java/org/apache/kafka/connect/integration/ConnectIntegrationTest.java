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

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.storage.StringConverter;
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

@Category(IntegrationTest.class)
public class ConnectIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(ConnectIntegrationTest.class);

    private static final int NUM_RECORDS_PRODUCED = 2000;
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

    /**
     * Simple test case to bring up an embedded Connect cluster along a backing Kafka and Zk process.
     * The test will produce and consume records, and start up a sink connector which will consume these records.
     */
    @Test
    public void testProduceConsumeConnector() throws Exception {
        // create test topic
        connect.kafka().createTopic("test-topic");

        // produce some strings into test topic
        for (int i = 0; i < NUM_RECORDS_PRODUCED / 2; i++) {
            connect.kafka().produce("test-topic", "hello-" + i);
            connect.kafka().produce("test-topic", "world-" + i);
        }

        // consume all records from test topic or fail
        log.info("Consuming records from test topic");
        connect.kafka().consume(NUM_RECORDS_PRODUCED, CONSUME_MAX_DURATION_MS, "test-topic");

        Map<String, String> props = new HashMap<>();
        props.put("connector.class", "MonitorableSink");
        props.put("task.max", "2");
        props.put("topics", "test-topic");
        props.put("key.converter", StringConverter.class.getName());
        props.put("value.converter", StringConverter.class.getName());
        props.put(MonitorableSinkConnector.EXPECTED_RECORDS, String.valueOf(NUM_RECORDS_PRODUCED));

        connect.startConnector("simple-conn", props);

        MonitorableSinkConnector.taskInstances("simple-conn-0").task().awaitRecords(CONSUME_MAX_DURATION_MS);

        log.debug("Connector read {} records from topic", NUM_RECORDS_PRODUCED);
        connect.deleteConnector("simple-conn");
    }
}
