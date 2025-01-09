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
package kafka.test.api;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.TransactionState;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.test.TestUtils;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterFeature;
import org.apache.kafka.common.test.api.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.ClusterTestExtensions;
import org.apache.kafka.common.test.api.ClusterTests;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.coordinator.transaction.TransactionLogConfig;
import org.apache.kafka.server.common.Feature;

import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.StreamSupport;

@ExtendWith(ClusterTestExtensions.class)
@ClusterTestDefaults(types = Type.KRAFT, serverProperties = {
    @ClusterConfigProperty(key = TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
    @ClusterConfigProperty(key = TransactionLogConfig.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG, value = "1"),
    @ClusterConfigProperty(key = TransactionLogConfig.TRANSACTIONS_TOPIC_MIN_ISR_CONFIG, value = "1"),
    @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
    @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
})
public class ProducerIntegrationTest {

    @ClusterTests({
        @ClusterTest(features = {
            @ClusterFeature(feature = Feature.TRANSACTION_VERSION, version = 0)}),
        @ClusterTest(features = {
            @ClusterFeature(feature = Feature.TRANSACTION_VERSION, version = 1)}),
        @ClusterTest(features = {
            @ClusterFeature(feature = Feature.TRANSACTION_VERSION, version = 2)})
    })
    public void testTransactionWithoutSend(ClusterInstance cluster) {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "foobar");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "test");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        try (Producer<byte[], byte[]> producer = cluster.producer(properties)) {
            producer.initTransactions();
            producer.beginTransaction();
            producer.commitTransaction();
        }
    }

    @ClusterTests({
        @ClusterTest(features = {
            @ClusterFeature(feature = Feature.TRANSACTION_VERSION, version = 0)}),
        @ClusterTest(features = {
            @ClusterFeature(feature = Feature.TRANSACTION_VERSION, version = 1)}),
        @ClusterTest(features = {
            @ClusterFeature(feature = Feature.TRANSACTION_VERSION, version = 2)})
    })
    public void testTransactionWithSend(ClusterInstance cluster) {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "foobar");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "test");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        try (Producer<byte[], byte[]> producer = cluster.producer(properties)) {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(new ProducerRecord<>("test", "key1".getBytes(), "value1".getBytes()));
            producer.send(new ProducerRecord<>("test", "key2".getBytes(), "value2".getBytes()));
            producer.send(new ProducerRecord<>("test", "key3".getBytes(), "value3".getBytes()));
            producer.commitTransaction();
        }
    }

    @ClusterTests({
        @ClusterTest(features = {
            @ClusterFeature(feature = Feature.TRANSACTION_VERSION, version = 0)}),
        @ClusterTest(features = {
            @ClusterFeature(feature = Feature.TRANSACTION_VERSION, version = 1)}),
        @ClusterTest(features = {
            @ClusterFeature(feature = Feature.TRANSACTION_VERSION, version = 2)})
    })
    public void testTransactionWithData(ClusterInstance cluster) throws ExecutionException, InterruptedException {
        String inputTopic = "my-input-topic";

        try (Producer<byte[], byte[]> producer = cluster.producer()) {
            for (int i = 0; i < 5; i++) {
                byte[] key = ("key-" + i).getBytes();
                byte[] value = ("value-" + i).getBytes();
                producer.send(new ProducerRecord<>(inputTopic, key, value)).get();
            }
        }

        String txnId = "foobar";
        Map<String, Object> producerProperties = new HashMap<>();
        producerProperties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, txnId);
        producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG, "test");
        producerProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        Map<String, Object> consumerProperties = new HashMap<>();
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (Producer<byte[], byte[]> producer = cluster.producer(producerProperties);
             Consumer<byte[], byte[]> consumer = cluster.consumer(consumerProperties)) {
            producer.initTransactions();
            producer.beginTransaction();

            consumer.subscribe(List.of(inputTopic));
            AtomicReference<ConsumerRecords<byte[], byte[]>> records = new AtomicReference<>();
            TestUtils.waitForCondition(() -> {
                ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(Duration.ZERO);
                records.set(consumerRecords);
                return consumerRecords.count() == 5;
            }, "poll records size not match");

            ConsumerRecord<byte[], byte[]> lastRecord = StreamSupport.stream(records.get().spliterator(), false)
                    .reduce((first, second) -> second).orElse(null);
            Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(
                    new TopicPartition(lastRecord.topic(), lastRecord.partition()),
                    new OffsetAndMetadata(lastRecord.offset() + 1));
            producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());

            producer.commitTransaction();
        }

        try (Admin admin = cluster.admin()) {
            TestUtils.waitForCondition(
                    () -> {
                        try {
                            return admin.listTransactions().all().get().stream()
                                    .filter(txn -> txn.transactionalId().equals(txnId))
                                    .anyMatch(txn -> txn.state() == TransactionState.COMPLETE_COMMIT);
                        } catch (ExecutionException | InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }, "transaction is not in COMPLETE_COMMIT state");
        }
    }
}
