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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;

import java.util.regex.Pattern;

import static org.apache.kafka.clients.consumer.PlaintextConsumerSubscriptionTest.BROKER_COUNT;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG;

@ClusterTestDefaults(
    types = {Type.KRAFT},
    brokers = BROKER_COUNT,
    serverProperties = {
        @ClusterConfigProperty(key = OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
        @ClusterConfigProperty(key = OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "3"),
        @ClusterConfigProperty(key = GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, value = "100"),
    }
)
public class PlaintextConsumerSubscriptionTest {

    public static final int BROKER_COUNT = 3;
    private final ClusterInstance cluster;
    private final String topic = "topic";
    private final TopicPartition tp = new TopicPartition(topic, 0);
    private final TopicPartition tp2 = new TopicPartition(topic, 1);

    public void testPatternSubscription(String groupProtocol) {
        int numRecords = 10000;
        Producer<byte[], byte[]> producer = createProducer();
        sendRecords(producer, numRecords, tp);

        String topic1 = "tblablac"; // matches subscribed pattern
        createTopic(topic1, 2, brokerCount);
        sendRecords(producer, 1000, new TopicPartition(topic1, 0));
        sendRecords(producer, 1000, new TopicPartition(topic1, 1));

        String topic2 = "tblablak"; // does not match subscribed pattern
        createTopic(topic2, 2, brokerCount);
        sendRecords(producer, 1000, new TopicPartition(topic2, 0));
        sendRecords(producer, 1000, new TopicPartition(topic2, 1));

        String topic3 = "tblab1"; // does not match subscribed pattern
        createTopic(topic3, 2, brokerCount);
        sendRecords(producer, 1000, new TopicPartition(topic3, 0));
        sendRecords(producer, 1000, new TopicPartition(topic3, 1));

        Consumer<byte[], byte[]> consumer = createConsumer();
        assertEquals(0, consumer.assignment().size());

        Pattern pattern = Pattern.compile("t.*c");
        consumer.subscribe(pattern, new TestConsumerReassignmentListener());

        Set<TopicPartition> assignment = new HashSet<>();
        assignment.add(new TopicPartition(topic, 0));
        assignment.add(new TopicPartition(topic, 1));
        assignment.add(new TopicPartition(topic1, 0));
        assignment.add(new TopicPartition(topic1, 1));
        awaitAssignment(consumer, assignment);

        String topic4 = "tsomec"; // matches subscribed pattern
        createTopic(topic4, 2, brokerCount);
        sendRecords(producer, 1000, new TopicPartition(topic4, 0));
        sendRecords(producer, 1000, new TopicPartition(topic4, 1));

        assignment.add(new TopicPartition(topic4, 0));
        assignment.add(new TopicPartition(topic4, 1));
        awaitAssignment(consumer, assignment);

        consumer.unsubscribe();
        assertEquals(0, consumer.assignment().size());
    }
}
