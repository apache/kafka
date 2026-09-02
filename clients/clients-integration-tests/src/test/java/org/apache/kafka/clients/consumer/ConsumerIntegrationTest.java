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
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterFeature;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTests;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorRuntimeMetrics;
import org.apache.kafka.server.common.Feature;
import org.apache.kafka.test.TestUtils;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
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
            @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
        }, features = {
            @ClusterFeature(feature = Feature.GROUP_VERSION, version = 0)
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

    @ClusterTests({
        @ClusterTest(
            types = {Type.KRAFT},
            brokers = 3,
            serverProperties = {
                @ClusterConfigProperty(id = 0, key = "broker.rack", value = "rack0"),
                @ClusterConfigProperty(id = 1, key = "broker.rack", value = "rack1"),
                @ClusterConfigProperty(id = 2, key = "broker.rack", value = "rack2"),
                @ClusterConfigProperty(key = GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG, value = "1000"),
                @ClusterConfigProperty(key = GroupCoordinatorConfig.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG, value = "1000"),
                @ClusterConfigProperty(key = GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, value = "org.apache.kafka.clients.consumer.RackAwareTestAssignor"),
                @ClusterConfigProperty(key = GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG, value = "0")
            }
        ),
        @ClusterTest(
            types = {Type.KRAFT},
            brokers = 3,
            serverProperties = {
                @ClusterConfigProperty(id = 0, key = "broker.rack", value = "rack0"),
                @ClusterConfigProperty(id = 1, key = "broker.rack", value = "rack1"),
                @ClusterConfigProperty(id = 2, key = "broker.rack", value = "rack2"),
                @ClusterConfigProperty(key = GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG, value = "1000"),
                @ClusterConfigProperty(key = GroupCoordinatorConfig.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG, value = "1000"),
                @ClusterConfigProperty(key = GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, value = "org.apache.kafka.clients.consumer.RackAwareTestAssignor"),
                @ClusterConfigProperty(key = GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG, value = "1000")
            }
        )
    })
    public void testRackAwareAssignment(ClusterInstance clusterInstance) throws ExecutionException, InterruptedException {
        String topic = "test-topic";
        try (Admin admin = clusterInstance.admin();
             Producer<byte[], byte[]> producer = clusterInstance.producer();
             Consumer<byte[], byte[]> consumer0 = clusterInstance.consumer(Map.of(
                 ConsumerConfig.GROUP_ID_CONFIG, "group0",
                 ConsumerConfig.CLIENT_RACK_CONFIG, "rack0",
                 ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false",
                 ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name()
             ));
             Consumer<byte[], byte[]> consumer1 = clusterInstance.consumer(Map.of(
                 ConsumerConfig.GROUP_ID_CONFIG, "group0",
                 ConsumerConfig.CLIENT_RACK_CONFIG, "rack1",
                 ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false",
                 ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name()
             ));
             Consumer<byte[], byte[]> consumer2 = clusterInstance.consumer(Map.of(
                 ConsumerConfig.GROUP_ID_CONFIG, "group0",
                 ConsumerConfig.CLIENT_RACK_CONFIG, "rack2",
                 ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false",
                 ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name()
             ))
        ) {
            // Create a new topic with 1 partition on broker 0.
            admin.createTopics(List.of(new NewTopic(topic, Map.of(0, List.of(0)))));
            clusterInstance.waitTopicCreation(topic, 1);

            producer.send(new ProducerRecord<>(topic, "key".getBytes(), "value".getBytes()));
            producer.flush();

            consumer0.subscribe(List.of(topic));
            consumer1.subscribe(List.of(topic));
            consumer2.subscribe(List.of(topic));

            TestUtils.waitForCondition(() -> {
                consumer0.poll(Duration.ofMillis(100));
                consumer1.poll(Duration.ofMillis(100));
                consumer2.poll(Duration.ofMillis(100));
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
            clusterInstance.waitTopicCreation(topic, 3);
            TestUtils.waitForCondition(() -> {
                consumer0.poll(Duration.ofMillis(100));
                consumer1.poll(Duration.ofMillis(100));
                consumer2.poll(Duration.ofMillis(100));
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
            clusterInstance.waitTopicCreation(topic, 6);
            TestUtils.waitForCondition(() -> {
                consumer0.poll(Duration.ofMillis(100));
                consumer1.poll(Duration.ofMillis(100));
                consumer2.poll(Duration.ofMillis(100));
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
                consumer0.poll(Duration.ofMillis(100));
                consumer1.poll(Duration.ofMillis(100));
                consumer2.poll(Duration.ofMillis(100));
                return consumer0.assignment().equals(Set.of(new TopicPartition(topic, 5))) &&
                    consumer1.assignment().equals(Set.of(new TopicPartition(topic, 3), new TopicPartition(topic, 4))) &&
                    consumer2.assignment().equals(Set.of(new TopicPartition(topic, 0), new TopicPartition(topic, 1), new TopicPartition(topic, 2)));
            }, 30000, "Consumer with topic partition mapping should be 0 -> 5 | 1 -> 3, 4 | 2 -> 0, 1, 2");
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

    /**
     * Verifies that rapidly switching partitions via assign() correctly commits offsets
     * for all previously-assigned partitions (classic consumer).
     */
    @ClusterTest(serverProperties = {
        @ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
        @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
    })
    public void testRapidAssignAutoCommitCorrectnessClassic(ClusterInstance clusterInstance) throws Exception {
        runRapidAssignAutoCommitCorrectness(clusterInstance, GroupProtocol.CLASSIC);
    }

    /**
     * Verifies that rapidly switching partitions via assign() correctly commits offsets
     * for all previously-assigned partitions (async consumer).
     */
    @ClusterTest(serverProperties = {
        @ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
        @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
    })
    public void testRapidAssignAutoCommitCorrectnessConsumer(ClusterInstance clusterInstance) throws Exception {
        runRapidAssignAutoCommitCorrectness(clusterInstance, GroupProtocol.CONSUMER);
    }

    private void runRapidAssignAutoCommitCorrectness(ClusterInstance clusterInstance, GroupProtocol protocol) throws Exception {
        String topic = "test-rapid-assign-autocommit";
        String groupId = "test-rapid-assign-autocommit-group-" + protocol.name().toLowerCase(Locale.ROOT);
        clusterInstance.createTopic(topic, 2, (short) 1);

        TopicPartition tp0 = new TopicPartition(topic, 0);
        TopicPartition tp1 = new TopicPartition(topic, 1);

        int msgCount = 10;
        try (var producer = clusterInstance.producer()) {
            for (int i = 0; i < msgCount; i++) {
                producer.send(new ProducerRecord<>(topic, 0, ("key" + i).getBytes(), ("val" + i).getBytes()));
                producer.send(new ProducerRecord<>(topic, 1, ("key" + i).getBytes(), ("val" + i).getBytes()));
            }
            producer.flush();
        }

        try (var consumer = clusterInstance.consumer(Map.of(
                ConsumerConfig.GROUP_ID_CONFIG, groupId,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true",
                ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "300000",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1",
                ConsumerConfig.GROUP_PROTOCOL_CONFIG, protocol.name().toLowerCase(Locale.ROOT)
            ));
             var admin = clusterInstance.admin()) {

            // Each iteration: assign tp0 → consume 1 msg → assign tp1 → consume 1 msg
            // assign() at the start of each round triggers a best-effort commit for the previous partition
            for (int i = 0; i < msgCount; i++) {
                consumer.assign(List.of(tp0));
                TestUtils.waitForCondition(
                    () -> !consumer.poll(Duration.ofMillis(500)).isEmpty(),
                    10000, "Should receive record from tp0 at iteration " + i);

                consumer.assign(List.of(tp1));
                TestUtils.waitForCondition(
                    () -> !consumer.poll(Duration.ofMillis(500)).isEmpty(),
                    10000, "Should receive record from tp1 at iteration " + i);
            }

            // Final: assign both to trigger commit for the last assigned partition (tp1)
            consumer.assign(List.of(tp0, tp1));

            // Verify both partitions committed all consumed offsets
            TestUtils.waitForCondition(() -> {
                var offsets = admin.listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata().get();
                return offsets.containsKey(tp0) && offsets.get(tp0) != null && offsets.get(tp0).offset() == msgCount &&
                       offsets.containsKey(tp1) && offsets.get(tp1) != null && offsets.get(tp1).offset() == msgCount;
            }, 10000, "Both tp0 and tp1 should have committed offset " + msgCount);
        }
    }

    /**
     * Verifies that calling assign() triggers a best-effort auto-commit for the
     * previously-assigned partition (classic consumer).
     */
    @ClusterTest(serverProperties = {
        @ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
        @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
    })
    public void testAssignTriggersAutoCommitClassic(ClusterInstance clusterInstance) throws Exception {
        runAssignTriggersAutoCommit(clusterInstance, GroupProtocol.CLASSIC);
    }

    /**
     * Verifies that calling assign() triggers a best-effort auto-commit for the
     * previously-assigned partition (async consumer).
     */
    @ClusterTest(serverProperties = {
        @ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
        @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
    })
    public void testAssignTriggersAutoCommitConsumer(ClusterInstance clusterInstance) throws Exception {
        runAssignTriggersAutoCommit(clusterInstance, GroupProtocol.CONSUMER);
    }

    private void runAssignTriggersAutoCommit(ClusterInstance clusterInstance, GroupProtocol protocol) throws Exception {
        String topic = "test-assign-autocommit-" + protocol.name().toLowerCase(Locale.ROOT);
        String groupId = "test-assign-autocommit-group-" + protocol.name().toLowerCase(Locale.ROOT);
        clusterInstance.createTopic(topic, 1, (short) 1);

        TopicPartition tp = new TopicPartition(topic, 0);
        int msgCount = 5;

        try (var producer = clusterInstance.producer()) {
            for (int i = 0; i < msgCount; i++) {
                producer.send(new ProducerRecord<>(topic, 0, ("key" + i).getBytes(), ("val" + i).getBytes()));
            }
            producer.flush();
        }

        try (var consumer = clusterInstance.consumer(Map.of(
                ConsumerConfig.GROUP_ID_CONFIG, groupId,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true",
                ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "300000",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.GROUP_PROTOCOL_CONFIG, protocol.name().toLowerCase(Locale.ROOT)
            ));
             var admin = clusterInstance.admin()) {

            // Assign and consume all messages
            consumer.assign(List.of(tp));
            int consumed = 0;
            while (consumed < msgCount) {
                consumed += consumer.poll(Duration.ofMillis(500)).count();
            }

            // Re-assign the same partition — triggers best-effort async commit for the previously-consumed offsets.
            // Poll once to drive the network layer so the queued commit is transmitted.
            consumer.assign(List.of(tp));
            consumer.poll(Duration.ofMillis(500));

            TestUtils.waitForCondition(() -> {
                var offsets = admin.listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata().get();
                return offsets.containsKey(tp) && offsets.get(tp) != null && offsets.get(tp).offset() == msgCount;
            }, 10000, "tp should have committed offset " + msgCount);
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
