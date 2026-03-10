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

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TransactionState;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.requests.InitProducerIdResponse;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterFeature;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.ClusterTests;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.coordinator.transaction.TransactionLogConfig;
import org.apache.kafka.server.IntegrationTestUtils;
import org.apache.kafka.server.common.Feature;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.test.TestUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterTestDefaults(types = {Type.KRAFT}, serverProperties = {
    @ClusterConfigProperty(key = TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
    @ClusterConfigProperty(key = TransactionLogConfig.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG, value = "1"),
    @ClusterConfigProperty(key = TransactionLogConfig.TRANSACTIONS_TOPIC_MIN_ISR_CONFIG, value = "1"),
    @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
    @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
})
class ProducerIntegrationTest {

    @ClusterTest(metadataVersion = MetadataVersion.IBP_3_3_IV3)
    void testUniqueProducerIds(ClusterInstance clusterInstance) {
        // Request enough PIDs from each broker to ensure each broker generates two blocks
        var ids = clusterInstance.brokers().values().stream().flatMap(broker -> {
            int port = broker.boundPort(clusterInstance.clientListener());
            return IntStream.range(0, 1001).parallel().mapToObj(i -> {
                try {
                    return nextProducerId(port);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }).toList();

        int brokerCount = clusterInstance.brokerIds().size();
        int expectedTotalCount = 1001 * brokerCount;
        assertEquals(expectedTotalCount, ids.size(), "Expected exactly " + expectedTotalCount + " IDs");
        assertEquals(expectedTotalCount, ids.stream().distinct().count(),
            "Found duplicate producer IDs");
    }

    @ClusterTests({
        @ClusterTest(features = {
            @ClusterFeature(feature = Feature.TRANSACTION_VERSION, version = 0)}),
        @ClusterTest(features = {
            @ClusterFeature(feature = Feature.TRANSACTION_VERSION, version = 1)}),
        @ClusterTest(features = {
            @ClusterFeature(feature = Feature.TRANSACTION_VERSION, version = 2)}),
    })
    void testTransactionWithAndWithoutSend(ClusterInstance cluster) {
        Map<String, Object> properties = Map.of(
            ProducerConfig.TRANSACTIONAL_ID_CONFIG, "foobar",
            ProducerConfig.CLIENT_ID_CONFIG, "test",
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        try (var producer = cluster.producer(properties)) {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(new ProducerRecord<>("test", "key".getBytes(), "value".getBytes()));
            producer.commitTransaction();

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
            @ClusterFeature(feature = Feature.TRANSACTION_VERSION, version = 2)}),
    })
    void testTransactionWithInvalidSendAndEndTxnRequestSent(ClusterInstance cluster) {
        var topic = new NewTopic("foobar", 1, (short) 1)
            .configs(Map.of(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "100"));
        String txnId = "test-txn";
        Map<String, Object> properties = Map.of(
            ProducerConfig.TRANSACTIONAL_ID_CONFIG, txnId,
            ProducerConfig.CLIENT_ID_CONFIG, "test",
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        try (var admin = cluster.admin();
             var producer = cluster.producer(properties)) {
            admin.createTopics(List.of(topic));

            producer.initTransactions();
            producer.beginTransaction();
            assertInstanceOf(RecordTooLargeException.class,
                assertThrows(ExecutionException.class,
                    () -> producer.send(new ProducerRecord<>(
                        topic.name(), new byte[100], new byte[100])).get()).getCause());

            producer.abortTransaction();
        }
    }

    @ClusterTests({
        @ClusterTest(features = {
            @ClusterFeature(feature = Feature.TRANSACTION_VERSION, version = 0)}),
        @ClusterTest(features = {
            @ClusterFeature(feature = Feature.TRANSACTION_VERSION, version = 1)}),
        @ClusterTest(features = {
            @ClusterFeature(feature = Feature.TRANSACTION_VERSION, version = 2)}),
    })
    void testTransactionWithSendOffset(ClusterInstance cluster) throws ExecutionException, InterruptedException {
        String inputTopic = "my-input-topic";
        try (var producer = cluster.producer()) {
            for (int i = 0; i < 5; i++) {
                byte[] key = ("key-" + i).getBytes();
                byte[] value = ("value-" + i).getBytes();
                producer.send(new ProducerRecord<>(inputTopic, key, value)).get();
            }
        }

        String txnId = "foobar";
        Map<String, Object> producerProperties = Map.of(
            ProducerConfig.TRANSACTIONAL_ID_CONFIG, txnId,
            ProducerConfig.CLIENT_ID_CONFIG, "test",
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        Map<String, Object> consumerProperties = Map.of(
            ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group",
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (var producer = cluster.producer(producerProperties);
             Consumer<byte[], byte[]> consumer = cluster.consumer(consumerProperties)) {
            producer.initTransactions();
            producer.beginTransaction();
            consumer.subscribe(List.of(inputTopic));
            var records = new ArrayList<ConsumerRecord<byte[], byte[]>>();
            TestUtils.waitForCondition(() -> {
                consumer.poll(Duration.ZERO).forEach(records::add);
                return records.size() == 5;
            }, "poll records size not match");
            var lastRecord = records.get(records.size() - 1);
            Map<TopicPartition, OffsetAndMetadata> offsets = Map.of(
                new TopicPartition(lastRecord.topic(), lastRecord.partition()),
                new OffsetAndMetadata(lastRecord.offset() + 1));
            producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
            producer.commitTransaction();
        }

        try (var admin = cluster.admin()) {
            TestUtils.waitForCondition(() ->
                admin.listTransactions().all().get().stream()
                    .filter(txn -> txn.transactionalId().equals(txnId))
                    .anyMatch(txn -> txn.state() == TransactionState.COMPLETE_COMMIT),
                "transaction is not in COMPLETE_COMMIT state");
        }
    }

    private long nextProducerId(int port) throws IOException {
        // Generating producer ids may fail while waiting for the initial block and also
        // when the current block is full and waiting for the prefetched block.
        var deadline = Instant.now().plusSeconds(5);
        var shouldRetry = true;
        InitProducerIdResponse response = null;
        while (shouldRetry && Instant.now().isBefore(deadline)) {
            var data = new InitProducerIdRequestData()
                .setProducerEpoch(RecordBatch.NO_PRODUCER_EPOCH)
                .setProducerId(RecordBatch.NO_PRODUCER_ID)
                .setTransactionalId(null)
                .setTransactionTimeoutMs(10);
            var request = new InitProducerIdRequest.Builder(data).build();
            response = IntegrationTestUtils.connectAndReceive(request, port);
            shouldRetry = response.data().errorCode() == Errors.COORDINATOR_LOAD_IN_PROGRESS.code();
        }
        assertTrue(Instant.now().isBefore(deadline));
        assertEquals(Errors.NONE.code(), response.data().errorCode());
        return response.data().producerId();
    }
}
