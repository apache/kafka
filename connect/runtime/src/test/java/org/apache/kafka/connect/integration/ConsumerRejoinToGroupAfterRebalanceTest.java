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

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.IntegrationTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static java.util.Collections.singletonList;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.junit.Assert.fail;

/** */
@Category({IntegrationTest.class})
public class ConsumerRejoinToGroupAfterRebalanceTest {
    private static final long TIMEOUT = 250L;

    private EmbeddedConnectCluster connect;

    @Test
    public void testConsumerRejoinAfterRebalance() throws Exception {
        Map<String, Object> consumerProps = new HashMap<>();

        consumerProps.put(GROUP_ID_CONFIG, UUID.randomUUID().toString());
        consumerProps.put(KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(MAX_POLL_INTERVAL_MS_CONFIG, Long.toString(TIMEOUT));

        KafkaConsumer<byte[], byte[]> consumer = connect.kafka().createConsumer(consumerProps);

        consumer.subscribe(singletonList("test-topic"));

        try {
            consumer.poll(Duration.ofMillis(TIMEOUT));

            Thread.sleep(2 * TIMEOUT);

            consumer.commitSync();
        } catch (CommitFailedException e) {
            // Ignore.
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Before
    public void setup() throws IOException {
        connect = new EmbeddedConnectCluster.Builder()
            .name("connect-cluster")
            .build();

        connect.start();
        connect.kafka().createTopic("test-topic");
    }
}
