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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.internals.AbstractHeartbeatRequestManager;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.TestUtils;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTests;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorRuntimeMetrics;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConsumerIntegrationTest {

    @ClusterTests({
        @ClusterTest(serverProperties = {
            @ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
            @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
            @ClusterConfigProperty(key = "group.coordinator.rebalance.protocols", value = "classic")
        })
    })
    public void testAsyncConsumerWithConsumerProtocolDisabled(ClusterInstance clusterInstance) throws Exception {
        String topic = "test-topic";
        clusterInstance.createTopic(topic, 1, (short) 1);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers(),
            ConsumerConfig.GROUP_ID_CONFIG, "test-group",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
            ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name()))) {
            consumer.subscribe(Collections.singletonList(topic));
            TestUtils.waitForCondition(() -> {
                try {
                    consumer.poll(Duration.ofMillis(1000));
                    return false;
                } catch (UnsupportedVersionException e) {
                    return e.getMessage().equals(AbstractHeartbeatRequestManager.CONSUMER_PROTOCOL_NOT_SUPPORTED_MSG);
                }
            }, "Should get UnsupportedVersionException and how to revert to classic protocol");
        }
    }

    @ClusterTest(serverProperties = {
        @ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
        @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
    })
    public void testFetchPartitionsAfterFailedListenerWithGroupProtocolClassic(ClusterInstance clusterInstance)
            throws InterruptedException {
        testFetchPartitionsAfterFailedListener(clusterInstance, GroupProtocol.CLASSIC);
    }

    @ClusterTest(serverProperties = {
        @ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
        @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
    })
    public void testFetchPartitionsAfterFailedListenerWithGroupProtocolConsumer(ClusterInstance clusterInstance)
            throws InterruptedException {
        testFetchPartitionsAfterFailedListener(clusterInstance, GroupProtocol.CONSUMER);
    }

    private static void testFetchPartitionsAfterFailedListener(ClusterInstance clusterInstance, GroupProtocol groupProtocol)
            throws InterruptedException {
        var topic = "topic";
        try (var producer = clusterInstance.producer(Map.of(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class))) {
            producer.send(new ProducerRecord<>(topic, "key".getBytes(), "value".getBytes()));
        }

        try (var consumer = clusterInstance.consumer(Map.of(
                ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol.name()))) {
            consumer.subscribe(List.of(topic), new ConsumerRebalanceListener() {
                private int count = 0;
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    count++;
                    if (count == 1) throw new IllegalArgumentException("temporary error");
                }
            });

            TestUtils.waitForCondition(() -> consumer.poll(Duration.ofSeconds(1)).count() == 1,
                    5000,
                    "failed to poll data");
        }
    }

    @ClusterTest(serverProperties = {
        @ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
        @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
    })
    public void testFetchPartitionsWithAlwaysFailedListenerWithGroupProtocolClassic(ClusterInstance clusterInstance)
            throws InterruptedException {
        testFetchPartitionsWithAlwaysFailedListener(clusterInstance, GroupProtocol.CLASSIC);
    }

    @ClusterTest(serverProperties = {
        @ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
        @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
    })
    public void testFetchPartitionsWithAlwaysFailedListenerWithGroupProtocolConsumer(ClusterInstance clusterInstance)
            throws InterruptedException {
        testFetchPartitionsWithAlwaysFailedListener(clusterInstance, GroupProtocol.CONSUMER);
    }

    private static void testFetchPartitionsWithAlwaysFailedListener(ClusterInstance clusterInstance, GroupProtocol groupProtocol)
            throws InterruptedException {
        var topic = "topic";
        try (var producer = clusterInstance.producer(Map.of(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class))) {
            producer.send(new ProducerRecord<>(topic, "key".getBytes(), "value".getBytes()));
        }

        try (var consumer = clusterInstance.consumer(Map.of(
                ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol.name()))) {
            consumer.subscribe(List.of(topic), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    throw new IllegalArgumentException("always failed");
                }
            });

            long startTimeMillis = System.currentTimeMillis();
            long currentTimeMillis = System.currentTimeMillis();
            while (currentTimeMillis < startTimeMillis + 3000) {
                currentTimeMillis = System.currentTimeMillis();
                try {
                    // In the async consumer, there is a possibility that the ConsumerRebalanceListenerCallbackCompletedEvent
                    // has not yet reached the application thread. And a poll operation might still succeed, but it
                    // should not return any records since none of the assigned topic partitions are marked as fetchable.
                    assertEquals(0, consumer.poll(Duration.ofSeconds(1)).count());
                } catch (KafkaException ex) {
                    assertEquals("User rebalance callback throws an error", ex.getMessage());
                }
                Thread.sleep(300);
            }
        }
    }

    @ClusterTest(types = {Type.KRAFT}, brokers = 3)
    public void testLeaderEpoch(ClusterInstance clusterInstance) throws Exception {
        String topic = "test-topic";
        clusterInstance.createTopic(topic, 1, (short) 2);
        var msgNum = 10;
        sendMsg(clusterInstance, topic, msgNum);

        try (var consumer = clusterInstance.consumer()) {
            TopicPartition targetTopicPartition = new TopicPartition(topic, 0);
            List<TopicPartition> topicPartitions = List.of(targetTopicPartition);
            consumer.assign(topicPartitions);
            consumer.seekToBeginning(List.of(targetTopicPartition));

            int consumed = 0;
            while (consumed < msgNum) {
                ConsumerRecords<Object, Object> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<Object, Object> record : records) {
                    assertTrue(record.leaderEpoch().isPresent());
                    assertEquals(0, record.leaderEpoch().get());
                }
                consumed += records.count();
            }

            // make the leader epoch increment by shutdown the leader broker
            clusterInstance.shutdownBroker(clusterInstance.getLeaderBrokerId(targetTopicPartition));

            sendMsg(clusterInstance, topic, msgNum);

            consumed = 0;
            while (consumed < msgNum) {
                ConsumerRecords<Object, Object> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<Object, Object> record : records) {
                    assertTrue(record.leaderEpoch().isPresent());
                    assertEquals(1, record.leaderEpoch().get());
                }
                consumed += records.count();
            }
        }
    }

    @ClusterTest(
        types = {Type.KRAFT},
        brokers = 3,
        serverProperties = {
            @ClusterConfigProperty(id = 0, key = "broker.rack", value = "rack0"),
            @ClusterConfigProperty(id = 1, key = "broker.rack", value = "rack1"),
            @ClusterConfigProperty(id = 2, key = "broker.rack", value = "rack2"),
            @ClusterConfigProperty(key = GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, value = "org.apache.kafka.clients.consumer.RackAwareAssignor")
        }
    )
    public void testRackAwareAssignment(ClusterInstance clusterInstance) throws ExecutionException, InterruptedException {
        String topic = "test-topic";
        try (Admin admin = clusterInstance.admin();
             Producer<byte[], byte[]> producer = clusterInstance.producer();
             Consumer<byte[], byte[]> consumer0 = clusterInstance.consumer(Map.of(
                 ConsumerConfig.GROUP_ID_CONFIG, "group0",
                 ConsumerConfig.CLIENT_RACK_CONFIG, "rack0",
                 ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name()
             ));
             Consumer<byte[], byte[]> consumer1 = clusterInstance.consumer(Map.of(
                 ConsumerConfig.GROUP_ID_CONFIG, "group0",
                 ConsumerConfig.CLIENT_RACK_CONFIG, "rack1",
                 ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name()
             ));
             Consumer<byte[], byte[]> consumer2 = clusterInstance.consumer(Map.of(
                 ConsumerConfig.GROUP_ID_CONFIG, "group0",
                 ConsumerConfig.CLIENT_RACK_CONFIG, "rack2",
                 ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name()
             ))
        ) {
            // Create a new topic with 1 partition on broker 0.
            admin.createTopics(List.of(new NewTopic(topic, Map.of(0, List.of(0)))));
            clusterInstance.waitForTopic(topic, 1);

            producer.send(new ProducerRecord<>(topic, "key".getBytes(), "value".getBytes()));
            producer.flush();

            consumer0.subscribe(List.of(topic));
            consumer1.subscribe(List.of(topic));
            consumer2.subscribe(List.of(topic));

            TestUtils.waitForCondition(() -> {
                consumer0.poll(Duration.ofMillis(1000));
                consumer1.poll(Duration.ofMillis(1000));
                consumer2.poll(Duration.ofMillis(1000));
                return consumer0.assignment().equals(Set.of(new TopicPartition(topic, 0))) &&
                    consumer1.assignment().isEmpty() &&
                    consumer2.assignment().isEmpty();
            }, "Consumer 0 should be assigned to topic partition 0");

            // Add a new partition 1 and 2 to broker 1.
            admin.createPartitions(
                Map.of(
                    topic,
                    NewPartitions.increaseTo(3, List.of(List.of(1), List.of(1)))
                )
            );
            clusterInstance.waitForTopic(topic, 3);
            TestUtils.waitForCondition(() -> {
                consumer0.poll(Duration.ofMillis(1000));
                consumer1.poll(Duration.ofMillis(1000));
                consumer2.poll(Duration.ofMillis(1000));
                return consumer0.assignment().equals(Set.of(new TopicPartition(topic, 0))) &&
                    consumer1.assignment().equals(Set.of(new TopicPartition(topic, 1), new TopicPartition(topic, 2))) &&
                    consumer2.assignment().isEmpty();
            }, "Consumer 1 should be assigned to topic partition 1 and 2");

            // Add a new partition 3, 4, and 5 to broker 2.
            admin.createPartitions(
                Map.of(
                    topic,
                    NewPartitions.increaseTo(6, List.of(List.of(2), List.of(2), List.of(2)))
                )
            );
            clusterInstance.waitForTopic(topic, 6);
            TestUtils.waitForCondition(() -> {
                consumer0.poll(Duration.ofMillis(1000));
                consumer1.poll(Duration.ofMillis(1000));
                consumer2.poll(Duration.ofMillis(1000));
                return consumer0.assignment().equals(Set.of(new TopicPartition(topic, 0))) &&
                    consumer1.assignment().equals(Set.of(new TopicPartition(topic, 1), new TopicPartition(topic, 2))) &&
                    consumer2.assignment().equals(Set.of(new TopicPartition(topic, 3), new TopicPartition(topic, 4), new TopicPartition(topic, 5)));
            }, "Consumer 2 should be assigned to topic partition 3, 4, and 5");

            // Change partitions to different brokers.
            // partition 0 -> broker 2
            // partition 1 -> broker 2
            // partition 2 -> broker 2
            // partition 3 -> broker 1
            // partition 4 -> broker 1
            // partition 5 -> broker 0
            admin.alterPartitionReassignments(Map.of(
                new TopicPartition(topic, 0), Optional.of(new NewPartitionReassignment(List.of(2))),
                new TopicPartition(topic, 1), Optional.of(new NewPartitionReassignment(List.of(2))),
                new TopicPartition(topic, 2), Optional.of(new NewPartitionReassignment(List.of(2))),
                new TopicPartition(topic, 3), Optional.of(new NewPartitionReassignment(List.of(1))),
                new TopicPartition(topic, 4), Optional.of(new NewPartitionReassignment(List.of(1))),
                new TopicPartition(topic, 5), Optional.of(new NewPartitionReassignment(List.of(0)))
            )).all().get();
            TestUtils.waitForCondition(() -> {
                consumer0.poll(Duration.ofMillis(1000));
                consumer1.poll(Duration.ofMillis(1000));
                consumer2.poll(Duration.ofMillis(1000));
                return consumer0.assignment().equals(Set.of(new TopicPartition(topic, 5))) &&
                    consumer1.assignment().equals(Set.of(new TopicPartition(topic, 3), new TopicPartition(topic, 4))) &&
                    consumer2.assignment().equals(Set.of(new TopicPartition(topic, 0), new TopicPartition(topic, 1), new TopicPartition(topic, 2)));
            }, "Consumer with topic partition mapping should be 0 -> 5 | 1 -> 3, 4 | 2 -> 0, 1, 2");
        }
    }

    @ClusterTest(
        brokers = 2,
        types = {Type.KRAFT},
        serverProperties = {
            @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
            @ClusterConfigProperty(key = GroupCoordinatorConfig.GROUP_COORDINATOR_APPEND_LINGER_MS_CONFIG, value = "3000")
        }
    )
    public void testSingleCoordinatorOwnershipAfterPartitionReassignment(ClusterInstance clusterInstance) throws InterruptedException, ExecutionException, TimeoutException {
        try (var producer = clusterInstance.<byte[], byte[]>producer()) {
            producer.send(new ProducerRecord<>("topic", "value".getBytes(StandardCharsets.UTF_8)));
        }

        try (var admin = clusterInstance.admin()) {
            admin.createTopics(List.of(new NewTopic(Topic.GROUP_METADATA_TOPIC_NAME, Map.of(0, List.of(0))))).all().get();
        }

        try (var consumer = clusterInstance.consumer(Map.of(ConsumerConfig.GROUP_ID_CONFIG, "test-group"));
             var admin = clusterInstance.admin()) {
            consumer.subscribe(List.of("topic"));
            TestUtils.waitForCondition(() -> consumer.poll(Duration.ofMillis(100)).isEmpty(), "polling to join group");
            // Append records to coordinator.
            consumer.commitSync();

            var broker0Metrics = clusterInstance.brokers().get(0).metrics();
            var broker1Metrics = clusterInstance.brokers().get(1).metrics();
            var activeNumPartitions = broker0Metrics.metricName(
                "num-partitions",
                GroupCoordinatorRuntimeMetrics.METRICS_GROUP,
                Map.of("state", "active")
            );

            assertEquals(1L, broker0Metrics.metric(activeNumPartitions).metricValue());
            assertEquals(0L, broker1Metrics.metric(activeNumPartitions).metricValue());

            // Unload the coordinator by changing leader (0 -> 1).
            admin.alterPartitionReassignments(
                Map.of(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0), Optional.of(new NewPartitionReassignment(List.of(1))))
            ).all().get();

            // Wait for the coordinator metrics to update after leadership change.
            TestUtils.waitForCondition(() ->
                0L == (Long) broker0Metrics.metric(activeNumPartitions).metricValue() &&
                    1L == (Long) broker1Metrics.metric(activeNumPartitions).metricValue(),
                "Incorrect num-partitions metric after partition reassignment to the new coordinator"
            );
        }
    }

    private void sendMsg(ClusterInstance clusterInstance, String topic, int sendMsgNum) {
        try (var producer = clusterInstance.producer(Map.of(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.ACKS_CONFIG, "-1"))) {
            for (int i = 0; i < sendMsgNum; i++) {
                producer.send(new ProducerRecord<>(topic, ("key_" + i), ("value_" + i)));
            }
            producer.flush();
        }
    }
}
