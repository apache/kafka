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

package org.apache.kafka.clients.producer;

import org.apache.kafka.clients.ClientsTestUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.internals.ErrorLoggingCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.coordinator.transaction.TransactionLogConfig;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.test.TestUtils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ClusterTestDefaults(
    types = {Type.KRAFT},
    brokers = 4,
    serverProperties = {
        @ClusterConfigProperty(key = ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG, value = "false"),
        @ClusterConfigProperty(key = ServerConfigs.MESSAGE_MAX_BYTES_CONFIG, value = "32768"),
        @ClusterConfigProperty(key = ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG, value = "true"),
        @ClusterConfigProperty(key = ReplicationConfigs.AUTO_LEADER_REBALANCE_ENABLE_CONFIG, value = "false"),
        // Use fewer partitions for __consumer_offsets so topic creation and leader assignment do not take too long.
        @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
        @ClusterConfigProperty(key = TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, value = "2"),
        // Allow a small session timeout.
        @ClusterConfigProperty(key = GroupCoordinatorConfig.GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, value = "10"),
        @ClusterConfigProperty(key = GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, value = "0"),
        @ClusterConfigProperty(key = TransactionLogConfig.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG, value = "1"),
    }
)
public class TransactionsBounceTest {
    private static final int CONSUME_RECORD_TIMEOUT_MS = 30000;
    private static final int NUM_PARTITIONS = 3;
    private static final String OUTPUT_TOPIC = "output-topic";
    private static final String INPUT_TOPIC = "input-topic";

    private final ClusterInstance cluster;

    TransactionsBounceTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    @ClusterTest
    public void testWithGroupMetadataClassic() throws Exception {
        testBrokerFailure(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testWithGroupMetadataConsumer() throws Exception {
        testBrokerFailure(GroupProtocol.CONSUMER);
    }

    private void testBrokerFailure(GroupProtocol groupProtocol) throws Exception {
        // Seed a topic with 10000 records and copy it transactionally while constantly bouncing brokers.
        int numInputRecords = 10000;
        Map<String, String> topicConfigs = Map.of(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2");
        cluster.createTopic(INPUT_TOPIC, NUM_PARTITIONS, (short) 3, topicConfigs);
        cluster.createTopic(OUTPUT_TOPIC, NUM_PARTITIONS, (short) 3, topicConfigs);

        try (Producer<byte[], byte[]> producer = cluster.producer()) {
            for (int i = 0; i < numInputRecords; i++) {
                byte[] value = Integer.toString(i).getBytes(StandardCharsets.UTF_8);
                producer.send(new ProducerRecord<>(INPUT_TOPIC, value, value));
            }
            producer.flush();
        }

        try (Consumer<byte[], byte[]> consumer = createConsumerAndSubscribe("myGroup", INPUT_TOPIC, groupProtocol, false);
             Producer<byte[], byte[]> producer = cluster.producer(Map.of(
                 ProducerConfig.BATCH_SIZE_CONFIG, 512,
                 ProducerConfig.TRANSACTIONAL_ID_CONFIG, "test-txn"
             ))) {
            producer.initTransactions();
            AtomicBoolean bouncing = new AtomicBoolean(true);
            CompletableFuture<Void> bouncer = CompletableFuture.runAsync(() -> bounceBrokers(bouncing));
            try {
                int numMessagesProcessed = 0;
                int iteration = 0;
                while (numMessagesProcessed < numInputRecords) {
                    int toRead = Math.min(200, numInputRecords - numMessagesProcessed);
                    List<ConsumerRecord<byte[], byte[]>> records = ClientsTestUtils.consumeRecords(
                        consumer, toRead, Integer.MAX_VALUE, CONSUME_RECORD_TIMEOUT_MS);

                    producer.beginTransaction();
                    boolean shouldAbort = iteration % 3 == 0;
                    for (ConsumerRecord<byte[], byte[]> record : records) {
                        ProducerRecord<byte[], byte[]> outputRecord = new ProducerRecord<>(OUTPUT_TOPIC, record.key(), record.value());
                        // Mark the intended outcome so the verifying consumer can detect any aborted records it receives.
                        outputRecord.headers().add(ClientsTestUtils.transactionStatusHeader(!shouldAbort));
                        producer.send(outputRecord, new ErrorLoggingCallback(OUTPUT_TOPIC, record.key(), record.value(), true));
                    }

                    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                    for (TopicPartition partition : consumer.assignment()) {
                        offsets.put(partition, new OffsetAndMetadata(consumer.position(partition)));
                    }
                    producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());

                    if (shouldAbort) {
                        producer.abortTransaction();
                        resetToCommittedPositions(consumer);
                    } else {
                        producer.commitTransaction();
                        numMessagesProcessed += records.size();
                    }
                    iteration++;
                }
            } finally {
                bouncing.set(false);
                bouncer.get();
            }
        }

        try (Consumer<byte[], byte[]> consumer = createConsumerAndSubscribe("randomGroup", OUTPUT_TOPIC, groupProtocol, true)) {
            Map<TopicPartition, List<Integer>> recordsByPartition = new HashMap<>();
            List<ConsumerRecord<byte[], byte[]>> records = ClientsTestUtils.consumeRecords(
                consumer, numInputRecords, Integer.MAX_VALUE, CONSUME_RECORD_TIMEOUT_MS);
            for (ConsumerRecord<byte[], byte[]> record : records) {
                ClientsTestUtils.assertTransactionCommitted(record);
                TopicPartition partition = new TopicPartition(record.topic(), record.partition());
                recordsByPartition.computeIfAbsent(partition, __ -> new ArrayList<>())
                    .add(Integer.parseInt(new String(record.value(), StandardCharsets.UTF_8)));
            }

            Set<Integer> recordSet = new HashSet<>();
            for (List<Integer> partitionValues : recordsByPartition.values()) {
                assertEquals(partitionValues.stream().sorted().collect(Collectors.toList()), partitionValues,
                    "Out of order messages detected");
                recordSet.addAll(partitionValues);
            }
            assertEquals(numInputRecords, records.size(), "Unexpected number of committed records");
            assertEquals(numInputRecords, recordSet.size());
            assertEquals(IntStream.range(0, numInputRecords).boxed().collect(Collectors.toSet()), recordSet);
        }
    }

    private Consumer<byte[], byte[]> createConsumerAndSubscribe(
        String groupId,
        String topic,
        GroupProtocol groupProtocol,
        boolean readCommitted
    ) {
        Consumer<byte[], byte[]> consumer = cluster.consumer(Map.of(
            ConsumerConfig.GROUP_ID_CONFIG, groupId,
            ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol.name(),
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
            ConsumerConfig.ISOLATION_LEVEL_CONFIG, readCommitted ? "read_committed" : "read_uncommitted"
        ));
        consumer.subscribe(List.of(topic));
        return consumer;
    }

    private void resetToCommittedPositions(Consumer<byte[], byte[]> consumer) {
        Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(consumer.assignment());
        for (TopicPartition partition : consumer.assignment()) {
            OffsetAndMetadata offset = committed.get(partition);
            if (offset == null) {
                consumer.seekToBeginning(List.of(partition));
            } else {
                consumer.seek(partition, offset.offset());
            }
        }
    }

    private void bounceBrokers(AtomicBoolean bouncing) {
        try (Admin admin = cluster.admin()) {
            while (bouncing.get()) {
                for (int brokerId : cluster.brokerIds()) {
                    cluster.shutdownBroker(brokerId);
                    cluster.brokers().get(brokerId).awaitShutdown();
                    TimeUnit.MILLISECONDS.sleep(500);
                    cluster.startBroker(brokerId);
                    TimeUnit.MILLISECONDS.sleep(500);
                }
                for (int partition = 0; partition < NUM_PARTITIONS; partition++) {
                    TopicPartition topicPartition = new TopicPartition(OUTPUT_TOPIC, partition);
                    TestUtils.waitForCondition(() -> hasLeader(admin, topicPartition), "No leader elected for " + topicPartition);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Broker bouncer failed", e);
        }
    }

    private boolean hasLeader(Admin admin, TopicPartition topicPartition) throws Exception {
        TopicPartitionInfo info = admin.describeTopics(List.of(topicPartition.topic()))
            .allTopicNames().get().get(topicPartition.topic())
            .partitions().get(topicPartition.partition());
        return info.leader() != null && info.leader().id() >= 0;
    }
}
