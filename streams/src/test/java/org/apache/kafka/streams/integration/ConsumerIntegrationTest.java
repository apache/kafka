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
package org.apache.kafka.streams.integration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;

@Category(IntegrationTest.class)
public class ConsumerIntegrationTest {
    private static final int NUM_BROKERS = 1;

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(
        NUM_BROKERS,
        Utils.mkProperties(Collections.singletonMap("auto.create.topics.enable", "false"))
    );

    @Rule
    public TestName testName = new TestName();
    private String topic1;
    private String topic2;

    @Before
    public void createTopics() throws Exception {
        final String uniqueTestName = IntegrationTestUtils.safeUniqueTestName(getClass(), testName);
        topic1 = uniqueTestName + "A";
        CLUSTER.createTopic(topic1, 1, 1);
        topic2 = uniqueTestName + "B";
        CLUSTER.createTopic(topic2, 1, 1);
    }

    @After
    public void deleteTopics() throws Exception {
        CLUSTER.deleteAllTopicsAndWait(IntegrationTestUtils.DEFAULT_TIMEOUT);
    }

    @Deprecated
    @Test
    public void asdf() throws Exception {
        final KafkaConsumer<byte[], byte[]> kafkaConsumer = new KafkaConsumer<>(mkMap(
            mkEntry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
            mkEntry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class),
            mkEntry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class)
        ));
        final TopicPartition topicPartition1 = new TopicPartition(topic1, 0);
        final TopicPartition topicPartition2 = new TopicPartition(topic2, 0);
        kafkaConsumer.assign(Arrays.asList(topicPartition1, topicPartition2));
        final long position = kafkaConsumer.position(topicPartition1);
        System.out.println(position);
        System.out.println(kafkaConsumer.position(topicPartition2));
        final ConsumerRecords<byte[], byte[]> poll = kafkaConsumer.poll(Duration.ZERO);
        System.out.println(poll);
        final ConsumerRecords<byte[], byte[]> poll1 = kafkaConsumer.poll(0L);
        System.out.println(poll1);
    }

}
