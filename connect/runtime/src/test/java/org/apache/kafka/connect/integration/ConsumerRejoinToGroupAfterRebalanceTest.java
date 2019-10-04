/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.IntegrationTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.junit.Assert.assertFalse;

/** */
@Category({IntegrationTest.class})
public class ConsumerRejoinToGroupAfterRebalanceTest {
    private static final int NUM_BROKERS = 3;
    private static final int NUM_REPLICAS = 2;
    private static final int NUM_TOPIC_PARTITIONS = 2;
    private static final int NUM_WORKERS = 3;

    private static final long TEST_DURATION = 60*60*1_000L;
    private static final long TIMEOUT = 250L;

    private EmbeddedConnectCluster connect;

    @Before
    public void setup() throws IOException {
        // setup Kafka broker properties
        Properties brokerProps = new Properties();
        brokerProps.put("auto.create.topics.enable", String.valueOf(false));

        // build a Connect cluster backed by Kafka and Zk
        connect = new EmbeddedConnectCluster.Builder()
            .name("connect-cluster")
            .numWorkers(NUM_WORKERS)
            .numBrokers(NUM_BROKERS)
            .brokerProps(brokerProps)
            .build();

        // start the clusters
        connect.start();
    }

    @Test
    public void testConsumerRejoinAfterRebalance() throws Exception {
        // create test topic
        connect.kafka().createTopic("test-topic", NUM_TOPIC_PARTITIONS, NUM_REPLICAS, new HashMap<>());

        // produce some messages into source topic partitions
        for (int i = 0; i < 20_000; i++)
            connect.kafka().produce("test-topic", i % NUM_TOPIC_PARTITIONS, "key", "simple-message-value-" + i);

        Map<String, Object> consumerProps = new HashMap<>();

        consumerProps.put(GROUP_ID_CONFIG, UUID.randomUUID().toString());
        consumerProps.put(KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(MAX_POLL_INTERVAL_MS_CONFIG, Long.toString(TIMEOUT));

        List<Thread> consumerThreads = new ArrayList<>(NUM_TOPIC_PARTITIONS);

        AtomicBoolean failed = new AtomicBoolean();
        long start = System.currentTimeMillis();

        for (int i=0; i<NUM_TOPIC_PARTITIONS; i++) {
            KafkaConsumer<byte[], byte[]> consumer = connect.kafka().createConsumer(consumerProps);

            consumer.subscribe(Arrays.asList("test-topic"));

            Thread th = new Thread(() -> {
                try {
                    while (!failed.get() && (System.currentTimeMillis() - start < TEST_DURATION)) {
                        try {
                            ConsumerRecords<byte[], byte[]> recs = consumer.poll(Duration.ofMillis(TIMEOUT));

                            Thread.sleep(2*TIMEOUT);

                            consumer.commitSync();
                        }
                        catch (CommitFailedException e) {
                            e.printStackTrace();
                        }
                    }
                }
                catch (Exception e) {
                    failed.set(true);

                    e.printStackTrace();
                }
            }, "testConsumer-" + i);

            consumerThreads.add(th);

            th.start();
        }

        for (Thread th : consumerThreads)
            th.join();

        assertFalse(failed.get());
    }
}
