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
package org.apache.kafka.tiered.storage.integration;

import kafka.server.BrokerServer;

import org.apache.kafka.clients.admin.ProducerState;
import org.apache.kafka.clients.admin.TransactionState;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ConcurrentTransactionsException;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.test.TestUtils;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Helper class containing shared transaction test flows that can be reused by both
 * {@code TransactionsTest} and {@code TransactionsWithTieredStoreTest}.
 */
public class TransactionsTestHelper {

    public static final String TOPIC1 = "topic1";
    public static final String TOPIC2 = "topic2";
    public static final int NUM_PARTITIONS = 4;
    public static final short BROKER_COUNT = 3;

    public static final String HEADER_KEY = "transactionStatus";
    public static final byte[] COMMITTED_VALUE = "committed".getBytes(StandardCharsets.UTF_8);
    public static final byte[] ABORTED_VALUE = "aborted".getBytes(StandardCharsets.UTF_8);

    public interface TransactionHooks {
        void verifyLogStartOffsets(Map<TopicPartition, Long> expectedOffsets) throws InterruptedException;
        void maybeVerifyLocalLogStartOffsets(Map<TopicPartition, Long> expectedOffsets) throws InterruptedException;
        void maybeWaitForAtLeastOneSegmentUpload(List<TopicPartition> topicPartitions);
    }

    public static final TransactionHooks NO_OP_HOOKS = new TransactionHooks() {
        @Override
        public void verifyLogStartOffsets(Map<TopicPartition, Long> expectedOffsets) {
        }

        @Override
        public void maybeVerifyLocalLogStartOffsets(Map<TopicPartition, Long> expectedOffsets) {
        }

        @Override
        public void maybeWaitForAtLeastOneSegmentUpload(List<TopicPartition> topicPartitions) {
        }
    };

    public static void testBasicTransactions(
            ClusterInstance clusterInstance,
            GroupProtocol groupProtocol,
            TransactionHooks hooks,
            Map<String, String> topicConfig
    ) throws Exception {
        createTopicsWithConfig(clusterInstance, topicConfig);
        var tp11 = new TopicPartition(TOPIC1, 1);
        var tp22 = new TopicPartition(TOPIC2, 2);

        try (var producer = createTransactionalProducer(clusterInstance, "transactional-producer")) {
            producer.initTransactions();
            // First transaction: abort
            producer.beginTransaction();
            producer.send(producerRecordWithExpectedTransactionStatus(TOPIC2, 2, "2", "2", false));
            producer.send(producerRecordWithExpectedTransactionStatus(TOPIC1, 1, "4", "4", false));
            producer.flush();

            hooks.verifyLogStartOffsets(Map.of(tp11, 0L, tp22, 0L));
            hooks.maybeVerifyLocalLogStartOffsets(Map.of(tp11, 0L, tp22, 0L));
            producer.abortTransaction();

            hooks.maybeWaitForAtLeastOneSegmentUpload(List.of(tp11, tp22));

            hooks.verifyLogStartOffsets(Map.of(tp11, 0L, tp22, 0L));
            hooks.maybeVerifyLocalLogStartOffsets(Map.of(tp11, 1L, tp22, 1L));

            // Second transaction: commit
            producer.beginTransaction();
            producer.send(producerRecordWithExpectedTransactionStatus(TOPIC1, 1, "1", "1", true));
            producer.send(producerRecordWithExpectedTransactionStatus(TOPIC2, 2, "3", "3", true));

            hooks.verifyLogStartOffsets(Map.of(tp11, 0L, tp22, 0L));
            hooks.maybeVerifyLocalLogStartOffsets(Map.of(tp11, 1L, tp22, 1L));

            producer.commitTransaction();

            hooks.verifyLogStartOffsets(Map.of(tp11, 0L, tp22, 0L));
            hooks.maybeVerifyLocalLogStartOffsets(Map.of(tp11, 3L, tp22, 3L));
        }

        try (var consumer = createReadCommittedConsumer(
                clusterInstance, groupProtocol, "txn-group-" + groupProtocol.name())) {
            consumer.subscribe(List.of(TOPIC1, TOPIC2));
            var records = consumeRecords(consumer, 2);
            records.forEach(TransactionsTestHelper::assertCommittedAndGetValue);
        }

        try (var uncommittedConsumer = createReadUncommittedConsumer(
                clusterInstance, groupProtocol, "non-txn-group-" + groupProtocol.name())) {
            uncommittedConsumer.subscribe(List.of(TOPIC1, TOPIC2));
            var allRecords = consumeRecords(uncommittedConsumer, 4);
            var expectedValues = Set.of("1", "2", "3", "4");
            allRecords.forEach(record ->
                    assertTrue(expectedValues.contains(recordValueAsString(record))));
        }
    }

    public static void testDelayedFetchIncludesAbortedTransaction(
            ClusterInstance clusterInstance,
            GroupProtocol groupProtocol,
            TransactionHooks hooks,
            Map<String, String> topicConfig
    ) throws Exception {
        clusterInstance.createTopic(TOPIC1, NUM_PARTITIONS, BROKER_COUNT, topicConfig);
        var tp10 = new TopicPartition(TOPIC1, 0);

        try (var producer1 = createTransactionalProducer(clusterInstance, "transactional-producer");
             var producer2 = createTransactionalProducer(clusterInstance, "other")) {
            producer1.initTransactions();
            producer2.initTransactions();

            producer1.beginTransaction();
            producer2.beginTransaction();
            producer2.send(new ProducerRecord<>(TOPIC1, 0, "x".getBytes(), "1".getBytes()));
            producer2.flush();

            producer1.send(new ProducerRecord<>(TOPIC1, 0, "y".getBytes(), "1".getBytes()));
            producer1.send(new ProducerRecord<>(TOPIC1, 0, "y".getBytes(), "2".getBytes()));
            producer1.flush();

            producer2.send(new ProducerRecord<>(TOPIC1, 0, "x".getBytes(), "2".getBytes()));
            producer2.flush();

            hooks.verifyLogStartOffsets(Map.of(tp10, 0L));
            hooks.maybeVerifyLocalLogStartOffsets(Map.of(tp10, 0L));

            producer1.abortTransaction();
            producer2.commitTransaction();

            hooks.maybeWaitForAtLeastOneSegmentUpload(List.of(tp10));
            hooks.verifyLogStartOffsets(Map.of(tp10, 0L));
            hooks.maybeVerifyLocalLogStartOffsets(Map.of(tp10, 5L));
        }

        try (Consumer<byte[], byte[]> readCommittedConsumer = clusterInstance.consumer(Map.of(
                ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol.name(),
                ConsumerConfig.GROUP_ID_CONFIG, "delayed-fetch-" + groupProtocol.name(),
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false",
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "100000",
                ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "100"))
        ) {
            readCommittedConsumer.assign(Set.of(tp10));
            var records = consumeRecords(readCommittedConsumer, 2);
            assertEquals(2, records.size());

            var first = records.get(0);
            assertEquals("x", new String(first.key()));
            assertEquals("1", new String(first.value()));
            assertEquals(0L, first.offset());

            var second = records.get(1);
            assertEquals("x", new String(second.key()));
            assertEquals("2", new String(second.value()));
            assertEquals(3L, second.offset());
        }
    }

    public static void testSendOffsetsWithGroupMetadata(
            ClusterInstance clusterInstance,
            GroupProtocol groupProtocol,
            TransactionHooks hooks,
            Map<String, String> topicConfig
    ) throws Exception {
        var suffix = groupProtocol.name();
        var srcTopic = "src-topic-" + suffix;
        var dstTopic = "dst-topic-" + suffix;
        clusterInstance.createTopic(srcTopic, NUM_PARTITIONS, BROKER_COUNT, topicConfig);
        clusterInstance.createTopic(dstTopic, NUM_PARTITIONS, BROKER_COUNT, topicConfig);

        var consumerGroupId = "foobar-consumer-group-" + suffix;
        var numSeedMessages = 500;
        seedTopicWithNumberedRecords(clusterInstance, srcTopic, numSeedMessages);

        try (var producer = createTransactionalProducer(
                clusterInstance, "transactional-producer-" + suffix)) {
            try (var consumer = createReadCommittedConsumer(
                    clusterInstance, groupProtocol, consumerGroupId, numSeedMessages / 4)) {
                consumer.subscribe(List.of(srcTopic));
                producer.initTransactions();

                var shouldCommit = false;
                var recordsProcessed = 0;

                while (recordsProcessed < numSeedMessages) {
                    var records = pollUntilAtLeastNumRecords(
                            consumer, Math.min(10, numSeedMessages - recordsProcessed));

                    producer.beginTransaction();
                    shouldCommit = !shouldCommit;

                    for (var record : records) {
                        var key = new String(record.key(), StandardCharsets.UTF_8);
                        var value = new String(record.value(), StandardCharsets.UTF_8);
                        producer.send(producerRecordWithExpectedTransactionStatus(
                                dstTopic, null, key, value, shouldCommit));
                    }

                    var positions = new HashMap<TopicPartition, OffsetAndMetadata>();
                    for (var tp : consumer.assignment()) {
                        positions.put(tp, new OffsetAndMetadata(consumer.position(tp)));
                    }
                    producer.sendOffsetsToTransaction(positions, consumer.groupMetadata());

                    if (shouldCommit) {
                        producer.commitTransaction();
                        recordsProcessed += records.size();
                    } else {
                        producer.abortTransaction();
                        resetToCommittedPositions(consumer);
                    }
                }
            }
        }

        var dstPartitions = new ArrayList<TopicPartition>();
        for (var p = 0; p < NUM_PARTITIONS; p++) {
            dstPartitions.add(new TopicPartition(dstTopic, p));
        }
        hooks.maybeWaitForAtLeastOneSegmentUpload(dstPartitions);

        try (var verifyingConsumer = createReadCommittedConsumer(
                clusterInstance, groupProtocol, "verifying-group-" + suffix)) {
            verifyingConsumer.subscribe(List.of(dstTopic));
            var allRecords = pollUntilAtLeastNumRecords(
                    verifyingConsumer, numSeedMessages);
            var values = new ArrayList<Integer>();
            for (var record : allRecords) {
                values.add(Integer.parseInt(assertCommittedAndGetValue(record)));
            }
            var valueSet = new HashSet<>(values);
            assertEquals(numSeedMessages, values.size(),
                    "Expected " + numSeedMessages + " values in " + dstTopic);
            assertEquals(values.size(), valueSet.size(),
                    "Expected " + values.size() + " unique messages in " + dstTopic);
        }
    }

    public static void testReadCommittedConsumerShouldNotSeeUndecidedData(
            ClusterInstance clusterInstance,
            GroupProtocol groupProtocol,
            Map<String, String> topicConfig
    ) throws Exception {
        createTopicsWithConfig(clusterInstance, topicConfig);

        try (var producer1 = createTransactionalProducer(clusterInstance, "transactional-producer");
             var producer2 = createTransactionalProducer(clusterInstance, "other")) {

            producer1.initTransactions();
            producer2.initTransactions();

            producer1.beginTransaction();
            producer2.beginTransaction();

            var latestVisibleTimestamp = System.currentTimeMillis();
            producer2.send(new ProducerRecord<>(TOPIC1, 0, latestVisibleTimestamp, "x".getBytes(), "1".getBytes()));
            producer2.send(new ProducerRecord<>(TOPIC2, 0, latestVisibleTimestamp, "x".getBytes(), "1".getBytes()));
            producer2.flush();

            var latestWrittenTimestamp = latestVisibleTimestamp + 1;
            producer1.send(new ProducerRecord<>(TOPIC1, 0, latestWrittenTimestamp, "a".getBytes(), "1".getBytes()));
            producer1.send(new ProducerRecord<>(TOPIC1, 0, latestWrittenTimestamp, "b".getBytes(), "2".getBytes()));
            producer1.send(new ProducerRecord<>(TOPIC2, 0, latestWrittenTimestamp, "c".getBytes(), "3".getBytes()));
            producer1.send(new ProducerRecord<>(TOPIC2, 0, latestWrittenTimestamp, "d".getBytes(), "4".getBytes()));
            producer1.flush();

            producer2.send(new ProducerRecord<>(TOPIC1, 0, latestWrittenTimestamp, "x".getBytes(), "2".getBytes()));
            producer2.send(new ProducerRecord<>(TOPIC2, 0, latestWrittenTimestamp, "x".getBytes(), "2".getBytes()));
            producer2.commitTransaction();

            var tp1 = new TopicPartition(TOPIC1, 0);
            var tp2 = new TopicPartition(TOPIC2, 0);

            try (var readUncommittedConsumer = createReadUncommittedConsumer(
                    clusterInstance, groupProtocol, "uncommitted-" + groupProtocol.name())) {
                readUncommittedConsumer.assign(Set.of(tp1, tp2));
                consumeRecords(readUncommittedConsumer, 8);
                var timestampsToSearch = Map.of(
                        tp1, latestWrittenTimestamp,
                        tp2, latestWrittenTimestamp);
                var readUncommittedOffsetsForTimes = readUncommittedConsumer.offsetsForTimes(timestampsToSearch);
                assertEquals(2, readUncommittedOffsetsForTimes.size());
                assertEquals(latestWrittenTimestamp, readUncommittedOffsetsForTimes.get(tp1).timestamp());
                assertEquals(latestWrittenTimestamp, readUncommittedOffsetsForTimes.get(tp2).timestamp());
            }

            try (var readCommittedConsumer = createReadCommittedConsumer(
                    clusterInstance, groupProtocol, "committed-" + groupProtocol.name())) {
                readCommittedConsumer.assign(Set.of(tp1, tp2));
                var records = consumeRecords(readCommittedConsumer, 2);
                records.forEach(record -> {
                    assertEquals("x", new String(record.key()));
                    assertEquals("1", new String(record.value()));
                });

                assertEquals(2, readCommittedConsumer.assignment().size());
                readCommittedConsumer.seekToEnd(readCommittedConsumer.assignment());
                readCommittedConsumer.assignment().forEach(tp ->
                        assertEquals(1L, readCommittedConsumer.position(tp)));

                var readCommittedOffsetsForTimes = readCommittedConsumer.offsetsForTimes(Map.of(
                        tp1, latestWrittenTimestamp,
                        tp2, latestWrittenTimestamp));
                assertNull(readCommittedOffsetsForTimes.get(tp1));
                assertNull(readCommittedOffsetsForTimes.get(tp2));
            }
        }
    }

    public static void testFencingOnCommit(
            ClusterInstance clusterInstance,
            GroupProtocol groupProtocol,
            Map<String, String> topicConfig
    ) throws Exception {
        createTopicsWithConfig(clusterInstance, topicConfig);

        try (var producer1 = createTransactionalProducer(clusterInstance, "transactional-producer");
             var producer2 = createTransactionalProducer(clusterInstance, "transactional-producer")) {

            producer1.initTransactions();

            producer1.beginTransaction();
            producer1.send(producerRecordWithExpectedTransactionStatus(TOPIC1, null, "1", "1", false));
            producer1.send(producerRecordWithExpectedTransactionStatus(TOPIC2, null, "3", "3", false));
            producer1.flush();

            producer2.initTransactions();
            producer2.beginTransaction();
            producer2.send(producerRecordWithExpectedTransactionStatus(TOPIC1, null, "2", "4", true));
            producer2.send(producerRecordWithExpectedTransactionStatus(TOPIC2, null, "2", "4", true));

            assertThrows(ProducerFencedException.class, producer1::commitTransaction);

            producer2.commitTransaction();
        }
        assertConsumeCommittedRecords(clusterInstance, List.of(TOPIC1, TOPIC2), 2, groupProtocol);
    }

    @SuppressWarnings("removal")
    public static void testFencingOnSendOffsets(
            ClusterInstance clusterInstance,
            GroupProtocol groupProtocol,
            Map<String, String> topicConfig
    ) throws Exception {
        createTopicsWithConfig(clusterInstance, topicConfig);

        try (var producer1 = createTransactionalProducer(clusterInstance, "transactional-producer");
             var producer2 = createTransactionalProducer(clusterInstance, "transactional-producer")) {

            producer1.initTransactions();

            producer1.beginTransaction();
            producer1.send(producerRecordWithExpectedTransactionStatus(TOPIC1, null, "1", "1", false));
            producer1.send(producerRecordWithExpectedTransactionStatus(TOPIC2, null, "3", "3", false));
            producer1.flush();

            producer2.initTransactions();
            producer2.beginTransaction();
            producer2.send(producerRecordWithExpectedTransactionStatus(TOPIC1, null, "2", "4", true));
            producer2.send(producerRecordWithExpectedTransactionStatus(TOPIC2, null, "2", "4", true));

            assertThrows(ProducerFencedException.class, () ->
                    producer1.sendOffsetsToTransaction(
                            Map.of(new TopicPartition(TOPIC1, 0), new OffsetAndMetadata(110L)),
                            new ConsumerGroupMetadata("foobarGroup")));

            producer2.commitTransaction();
        }
        assertConsumeCommittedRecords(clusterInstance, List.of(TOPIC1, TOPIC2), 2, groupProtocol);
    }

    @SuppressWarnings("removal")
    public static void testOffsetMetadataInSendOffsetsToTransaction(
            ClusterInstance clusterInstance,
            GroupProtocol groupProtocol,
            Map<String, String> topicConfig
    ) throws Exception {
        createTopicsWithConfig(clusterInstance, topicConfig);
        var tp = new TopicPartition(TOPIC1, 0);
        var groupId = "group-" + groupProtocol.name();
        var txnId = "txn-offset-meta-" + groupProtocol.name();

        var offsetAndMetadata = new OffsetAndMetadata(110L, Optional.of(15), "some metadata");
        try (var producer = createTransactionalProducer(clusterInstance, txnId)) {
            producer.initTransactions();

            producer.beginTransaction();
            producer.sendOffsetsToTransaction(Map.of(tp, offsetAndMetadata), new ConsumerGroupMetadata(groupId));
            producer.commitTransaction();
        }
        try (var producer2 = createTransactionalProducer(clusterInstance, txnId)) {
            producer2.initTransactions();
        }
        try (var consumer = createReadCommittedConsumer(clusterInstance, groupProtocol, groupId)) {
            consumer.subscribe(List.of(TOPIC1));
            TestUtils.waitForCondition(() -> {
                var committed = consumer.committed(Set.of(tp)).get(tp);
                return offsetAndMetadata.equals(committed);
            }, "cannot read committed offset");
        }
    }

    public static void testInitTransactionsTimeout(
            ClusterInstance clusterInstance,
            Map<String, String> topicConfig
    ) throws Exception {
        createTopicsWithConfig(clusterInstance, topicConfig);
        testTimeout(clusterInstance, false, Producer::initTransactions);
    }

    @SuppressWarnings("removal")
    public static void testSendOffsetsToTransactionTimeout(
            ClusterInstance clusterInstance,
            Map<String, String> topicConfig
    ) throws Exception {
        createTopicsWithConfig(clusterInstance, topicConfig);
        testTimeout(clusterInstance, true, producer ->
                producer.sendOffsetsToTransaction(
                        Map.of(new TopicPartition(TOPIC1, 0), new OffsetAndMetadata(0)),
                        new ConsumerGroupMetadata("test-group")));
    }

    public static void testCommitTransactionTimeout(
            ClusterInstance clusterInstance,
            Map<String, String> topicConfig
    ) throws Exception {
        createTopicsWithConfig(clusterInstance, topicConfig);
        testTimeout(clusterInstance, true, Producer::commitTransaction);
    }

    public static void testAbortTransactionTimeout(
            ClusterInstance clusterInstance,
            Map<String, String> topicConfig
    ) throws Exception {
        createTopicsWithConfig(clusterInstance, topicConfig);
        testTimeout(clusterInstance, true, Producer::abortTransaction);
    }

    public static void testFencingOnSend(
            ClusterInstance clusterInstance,
            GroupProtocol groupProtocol,
            Map<String, String> topicConfig
    ) throws Exception {
        createTopicsWithConfig(clusterInstance, topicConfig);

        try (var producer1 = createTransactionalProducer(clusterInstance, "transactional-producer");
             var producer2 = createTransactionalProducer(clusterInstance, "transactional-producer")) {

            producer1.initTransactions();

            producer1.beginTransaction();
            producer1.send(producerRecordWithExpectedTransactionStatus(TOPIC1, null, "1", "1", false));
            producer1.send(producerRecordWithExpectedTransactionStatus(TOPIC2, null, "3", "3", false));

            producer2.initTransactions();
            producer2.beginTransaction();
            producer2.send(producerRecordWithExpectedTransactionStatus(TOPIC1, null, "2", "4", true)).get();
            producer2.send(producerRecordWithExpectedTransactionStatus(TOPIC2, null, "2", "4", true)).get();

            try {
                var result = producer1.send(
                        producerRecordWithExpectedTransactionStatus(TOPIC1, null, "1", "5", false));
                result.get();
                fail("Should not be able to send messages from a fenced producer.");
            } catch (ProducerFencedException e) {
                // expected
            } catch (ExecutionException e) {
                assertInstanceOf(InvalidProducerEpochException.class, e.getCause());
            } catch (Exception e) {
                throw new AssertionError("Got an unexpected exception from a fenced producer.", e);
            }
            producer2.commitTransaction();
        }
        assertConsumeCommittedRecords(clusterInstance, List.of(TOPIC1, TOPIC2), 2, groupProtocol);
    }

    public static void testFencingOnAddPartitions(
            ClusterInstance clusterInstance,
            GroupProtocol groupProtocol,
            Map<String, String> topicConfig
    ) throws Exception {
        createTopicsWithConfig(clusterInstance, topicConfig);
        try (var producer1 = createTransactionalProducer(clusterInstance, "transactional-producer");
             var producer2 = createTransactionalProducer(clusterInstance, "transactional-producer")) {

            producer1.initTransactions();
            producer1.beginTransaction();
            producer1.send(producerRecordWithExpectedTransactionStatus(TOPIC1, null, "1", "1", false));
            producer1.send(producerRecordWithExpectedTransactionStatus(TOPIC2, null, "3", "3", false));
            producer1.abortTransaction();

            producer2.initTransactions();
            producer2.beginTransaction();
            producer2.send(producerRecordWithExpectedTransactionStatus(TOPIC1, null, "2", "4", true))
                    .get(20, TimeUnit.SECONDS);
            producer2.send(producerRecordWithExpectedTransactionStatus(TOPIC2, null, "2", "4", true))
                    .get(20, TimeUnit.SECONDS);

            try {
                producer1.beginTransaction();
                var result = producer1.send(
                        producerRecordWithExpectedTransactionStatus(TOPIC1, null, "1", "5", false));
                result.get();
                fail("Should not be able to send messages from a fenced producer.");
            } catch (InvalidProducerEpochException e) {
                // expected
            } catch (ExecutionException e) {
                assertInstanceOf(InvalidProducerEpochException.class, e.getCause());
            } catch (Exception e) {
                throw new AssertionError("Got an unexpected exception from a fenced producer.", e);
            }

            producer2.commitTransaction();
        }

        assertConsumeCommittedRecords(clusterInstance, List.of(TOPIC1, TOPIC2), 2, groupProtocol);
    }

    public static void testFencingOnTransactionExpiration(
            ClusterInstance clusterInstance,
            GroupProtocol groupProtocol,
            Map<String, String> topicConfig
    ) throws Exception {
        createTopicsWithConfig(clusterInstance, topicConfig);

        try (var producer = createTransactionalProducer(
                clusterInstance, "expiringProducer", 300, 60000, 120000, 30000)) {

            producer.initTransactions();
            producer.beginTransaction();

            var firstMessageResult = producer.send(
                    producerRecordWithExpectedTransactionStatus(TOPIC1, null, "1", "1", false)).get();
            assertTrue(firstMessageResult.hasOffset());

            Thread.sleep(600);

            try {
                producer.send(producerRecordWithExpectedTransactionStatus(TOPIC1, null, "2", "2", false)).get();
                fail("should have raised an error due to concurrent transactions or invalid producer epoch");
            } catch (ConcurrentTransactionsException | InvalidProducerEpochException e) {
                // expected
            } catch (ExecutionException e) {
                assertInstanceOf(InvalidProducerEpochException.class, e.getCause(), "Error was " + e.getCause() + " and not InvalidProducerEpochException");
            }
        }

        try (var nonTransactionalConsumer = createReadUncommittedConsumer(
                clusterInstance, groupProtocol, "non-txn-expiration-" + groupProtocol.name())) {
            nonTransactionalConsumer.subscribe(List.of(TOPIC1));
            var records = consumeRecords(nonTransactionalConsumer, 1);
            assertEquals(1, records.size());
            assertEquals("1", recordValueAsString(records.get(0)));
        }

        try (var transactionalConsumer = createReadCommittedConsumer(
                clusterInstance, groupProtocol, "txn-expiration-" + groupProtocol.name())) {
            transactionalConsumer.subscribe(List.of(TOPIC1));
            var transactionalRecords = consumeRecordsFor(transactionalConsumer, 1000);
            assertTrue(transactionalRecords.isEmpty());
        }
    }

    public static void testMultipleMarkersOneLeader(
            ClusterInstance clusterInstance,
            GroupProtocol groupProtocol,
            Map<String, String> topicConfig
    ) throws Exception {
        createTopicsWithConfig(clusterInstance, topicConfig);
        var topicWith10Partitions = "largeTopic";
        var topicWith10PartitionsAndOneReplica = "largeTopicOneReplica";

        clusterInstance.createTopic(topicWith10Partitions, 10, BROKER_COUNT, topicConfig);
        clusterInstance.createTopic(topicWith10PartitionsAndOneReplica, 10, (short) 1);

        try (var producer = createTransactionalProducer(clusterInstance, "transactional-producer")) {
            producer.initTransactions();

            producer.beginTransaction();
            sendTransactionalMessagesWithValueRange(producer, topicWith10Partitions, 0, 5000, false);
            sendTransactionalMessagesWithValueRange(producer, topicWith10PartitionsAndOneReplica, 5000, 10000, false);
            producer.abortTransaction();

            producer.beginTransaction();
            sendTransactionalMessagesWithValueRange(producer, topicWith10Partitions, 10000, 11000, true);
            producer.commitTransaction();
        }

        try (var consumer = createReadCommittedConsumer(
                clusterInstance, groupProtocol, "markers-committed-" + groupProtocol.name())) {
            consumer.subscribe(List.of(topicWith10PartitionsAndOneReplica, topicWith10Partitions));
            var records = consumeRecords(consumer, 1000);
            records.forEach(TransactionsTestHelper::assertCommittedAndGetValue);
        }

        try (var uncommittedConsumer = createReadUncommittedConsumer(
                clusterInstance, groupProtocol, "markers-uncommitted-" + groupProtocol.name())) {
            uncommittedConsumer.subscribe(List.of(topicWith10PartitionsAndOneReplica, topicWith10Partitions));
            var allRecords = consumeRecords(uncommittedConsumer, 11000);
            var expectedValues = new HashSet<String>();
            for (var i = 0; i < 11000; i++) {
                expectedValues.add(String.valueOf(i));
            }
            allRecords.forEach(record -> assertTrue(expectedValues.contains(recordValueAsString(record))));
        }
    }

    public static void testConsecutivelyRunInitTransactions(ClusterInstance clusterInstance) {
        try (var producer = createTransactionalProducer(clusterInstance, "normalProducer")) {
            producer.initTransactions();
            assertThrows(IllegalStateException.class, producer::initTransactions);
        }
    }

    public static void testRecoveryFromEpochOverflow(
            ClusterInstance clusterInstance,
            Map<String, String> topicConfig
    ) throws Exception {
        createTopicsWithConfig(clusterInstance, topicConfig);
        var transactionalId = "test-overflow";
        var abortedRecord = new ProducerRecord<>(TOPIC1, 0, "key".getBytes(), "aborted".getBytes());

        try (var producer = createTransactionalProducer(
                clusterInstance, transactionalId, 500, 60000, 120000, 30000)) {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(abortedRecord);
            producer.abortTransaction();
        }

        try (var admin = clusterInstance.admin()) {
            var coordinatorId = admin.describeTransactions(List.of(transactionalId))
                    .description(transactionalId).get().coordinatorId();

            var brokerServer = (BrokerServer) clusterInstance.brokers().get(coordinatorId);
            var txnManager = brokerServer.transactionCoordinator().transactionManager();

            var optMeta = txnManager.getTransactionMetadata(transactionalId);
            assertTrue(optMeta.isPresent());
            optMeta.get().inLock(() -> {
                optMeta.get().setProducerEpoch((short) (Short.MAX_VALUE - 2));
                return null;
            });
        }

        try (var producer = createTransactionalProducer(
                clusterInstance, transactionalId, 500, 60000, 120000, 30000)) {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(abortedRecord).get();
            producer.flush();

            try (var admin = clusterInstance.admin()) {
                var coordinatorId = admin.describeTransactions(List.of(transactionalId))
                        .description(transactionalId).get().coordinatorId();
                var brokerServer = (BrokerServer) clusterInstance.brokers().get(coordinatorId);
                var txnManager = brokerServer.transactionCoordinator().transactionManager();

                var optMeta = txnManager.getTransactionMetadata(transactionalId);
                assertTrue(optMeta.isPresent());
                var currentEpoch = optMeta.get().producerEpoch();
                assertEquals((short) (Short.MAX_VALUE - 1), currentEpoch,
                        "Expected epoch to be " + (Short.MAX_VALUE - 1) + ", but got " + currentEpoch);

                TestUtils.waitForCondition(() -> {
                    try {
                        return admin.listTransactions().all().get().stream()
                                .anyMatch(txn -> txn.transactionalId().equals(transactionalId)
                                        && txn.state() == TransactionState.COMPLETE_ABORT);
                    } catch (Exception e) {
                        return false;
                    }
                }, "Transaction was not aborted on timeout");
            }

            producer.abortTransaction();

            producer.beginTransaction();
            producer.send(abortedRecord).get();
            producer.flush();
        }

        try (var producer2 = createTransactionalProducer(
                clusterInstance, transactionalId, 500, 60000, 120000, 30000)) {
            producer2.initTransactions();
            producer2.beginTransaction();
            var committedRecord = new ProducerRecord<>(TOPIC1, 0, "key".getBytes(), "committed".getBytes());
            producer2.send(committedRecord).get();
            producer2.commitTransaction();
        }

        try (Consumer<byte[], byte[]> consumer = clusterInstance.consumer(Map.of(
                ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false",
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))) {
            var tp = new TopicPartition(TOPIC1, 0);
            consumer.assign(List.of(tp));
            var records = consumeRecords(consumer, 1);

            var record = records.get(0);
            assertArrayEquals("key".getBytes(), record.key(), "Record key should match");
            assertArrayEquals("committed".getBytes(), record.value(), "Record value should be 'committed'");
            assertEquals(0, record.partition(), "Record should be in partition 0");
            assertEquals(TOPIC1, record.topic(), "Record should be in topic1");
        }
    }

    public static void testBumpTransactionalEpochWithTV2Disabled(
            ClusterInstance clusterInstance,
            GroupProtocol groupProtocol,
            Map<String, String> topicConfig
    ) throws Exception {
        createTopicsWithConfig(clusterInstance, topicConfig);
        var defaultLinger = 5;

        try (var producer = createTransactionalProducer(
                clusterInstance, "transactionalProducer", 60000, 60000, 5000 + defaultLinger, 5000)) {

            var testTopic = "test-topic";
            clusterInstance.createTopic(testTopic, NUM_PARTITIONS, (short) 1);
            var testTp = new TopicPartition(testTopic, 0);
            var partitionLeader = clusterInstance.getLeaderBrokerId(testTp);

            producer.initTransactions();

            producer.beginTransaction();
            producer.send(producerRecordWithExpectedTransactionStatus(testTopic, 0, "4", "4", true));
            producer.commitTransaction();

            var initialProducers = waitForActiveProducers(clusterInstance, testTp);
            var producerId = initialProducers.get(0).producerId();
            var initialProducerEpoch = initialProducers.get(0).producerEpoch();

            producer.beginTransaction();
            var successfulFuture = producer.send(
                    producerRecordWithExpectedTransactionStatus(TOPIC1, null, "2", "2", false));
            successfulFuture.get(20, TimeUnit.SECONDS);

            clusterInstance.shutdownBroker(partitionLeader);
            var failedFuture = producer.send(
                    producerRecordWithExpectedTransactionStatus(testTopic, 0, "3", "3", false));
            Thread.sleep(6000);
            restartDeadBrokers(clusterInstance);

            assertFutureThrows(failedFuture);

            TestUtils.waitForCondition(() -> {
                try {
                    producer.send(producerRecordWithExpectedTransactionStatus(testTopic, 0, "3", "3", false));
                    return false;
                } catch (KafkaException e) {
                    return true;
                }
            }, "The send request never failed as expected.");
            assertThrows(KafkaException.class, () ->
                    producer.send(producerRecordWithExpectedTransactionStatus(testTopic, 0, "3", "3", false)));
            producer.abortTransaction();

            producer.beginTransaction();
            producer.send(producerRecordWithExpectedTransactionStatus(TOPIC2, null, "2", "2", true));
            producer.send(producerRecordWithExpectedTransactionStatus(TOPIC1, null, "4", "4", true));
            producer.send(producerRecordWithExpectedTransactionStatus(testTopic, 0, "1", "1", true));
            producer.send(producerRecordWithExpectedTransactionStatus(testTopic, 0, "3", "3", true));
            producer.commitTransaction();

            try (var consumer = createReadCommittedConsumer(
                    clusterInstance, groupProtocol, "bump-tv2-disabled-" + groupProtocol.name())) {
                consumer.subscribe(List.of(TOPIC1, TOPIC2, testTopic));
                var records = consumeRecords(consumer, 5);
                records.forEach(TransactionsTestHelper::assertCommittedAndGetValue);
            }

            TestUtils.waitForCondition(() -> {
                try {
                    var producers = getActiveProducers(clusterInstance, testTp);
                    return producers.stream().anyMatch(p ->
                            p.producerId() == producerId && p.producerEpoch() > initialProducerEpoch);
                } catch (Exception e) {
                    return false;
                }
            }, "Producer epoch was not bumped. InitialProducerEpoch: " + initialProducerEpoch);
        }
    }

    public static void testBumpTransactionalEpochWithTV2Enabled(
            ClusterInstance clusterInstance,
            GroupProtocol groupProtocol,
            Map<String, String> topicConfig
    ) throws Exception {
        createTopicsWithConfig(clusterInstance, topicConfig);
        var defaultLinger = 5;

        try (var producer = createTransactionalProducer(
                clusterInstance, "transactionalProducer", 60000, 60000, 5000 + defaultLinger, 5000)) {

            var testTopic = "test-topic";
            clusterInstance.createTopic(testTopic, NUM_PARTITIONS, (short) 1);
            var testTp = new TopicPartition(testTopic, 0);
            var partitionLeader = clusterInstance.getLeaderBrokerId(testTp);

            producer.initTransactions();

            producer.beginTransaction();
            producer.send(producerRecordWithExpectedTransactionStatus(testTopic, 0, "4", "4", true));
            producer.commitTransaction();

            producer.beginTransaction();
            var successfulFuture = producer.send(
                    producerRecordWithExpectedTransactionStatus(TOPIC1, null, "2", "2", false));
            successfulFuture.get(20, TimeUnit.SECONDS);

            var initialProducers = waitForActiveProducers(clusterInstance, testTp);
            var producerId = initialProducers.get(0).producerId();
            var previousProducerEpoch = initialProducers.get(0).producerEpoch();

            clusterInstance.shutdownBroker(partitionLeader);
            var failedFuture = producer.send(
                    producerRecordWithExpectedTransactionStatus(testTopic, 0, "3", "3", false));
            Thread.sleep(6000);
            restartDeadBrokers(clusterInstance);

            assertFutureThrows(failedFuture);
            producer.abortTransaction();

            producer.beginTransaction();
            var nextMetadata = producer.send(
                    producerRecordWithExpectedTransactionStatus(TOPIC2, null, "2", "2", true))
                    .get(20, TimeUnit.SECONDS);

            var actualTp = new TopicPartition(TOPIC2, nextMetadata.partition());
            TestUtils.waitForCondition(() -> {
                try {
                    var producers = getActiveProducers(clusterInstance, actualTp);
                    return producers.stream().anyMatch(p ->
                            p.producerId() == producerId && p.producerEpoch() > previousProducerEpoch);
                } catch (Exception e) {
                    return false;
                }
            }, "Producer epoch was not bumped");

            producer.send(producerRecordWithExpectedTransactionStatus(TOPIC1, null, "4", "4", true));
            producer.send(producerRecordWithExpectedTransactionStatus(testTopic, 0, "1", "1", true));
            producer.send(producerRecordWithExpectedTransactionStatus(testTopic, 0, "3", "3", true));
            producer.commitTransaction();

            try (var consumer = createReadCommittedConsumer(
                    clusterInstance, groupProtocol, "bump-tv2-enabled-" + groupProtocol.name())) {
                consumer.subscribe(List.of(TOPIC1, TOPIC2, testTopic));
                var records = consumeRecords(consumer, 5);
                records.forEach(TransactionsTestHelper::assertCommittedAndGetValue);
            }
        }
    }

    public static void testFailureToFenceEpoch(
            ClusterInstance clusterInstance,
            boolean isTV2Enabled,
            Map<String, String> topicConfig
    ) throws Exception {
        createTopicsWithConfig(clusterInstance, topicConfig);
        var tp = new TopicPartition(TOPIC1, 0);
        long producerId;
        int initialProducerEpoch;

        try (var producer1 = createTransactionalProducer(clusterInstance, "transactional-producer")) {
            producer1.initTransactions();

            producer1.beginTransaction();
            producer1.send(producerRecordWithExpectedTransactionStatus(TOPIC1, 0, "4", "4", true));
            producer1.commitTransaction();

            var initialProducers = waitForActiveProducers(clusterInstance, tp);
            producerId = initialProducers.get(0).producerId();
            initialProducerEpoch = initialProducers.get(0).producerEpoch();

            clusterInstance.shutdownBroker(0);
            clusterInstance.shutdownBroker(1);

            try (var producer2 = createTransactionalProducer(
                    clusterInstance, "transactional-producer", 60000, 1000, 120000, 30000)) {
                try {
                    producer2.initTransactions();
                } catch (TimeoutException e) {
                    // expected
                } catch (Exception e) {
                    throw new AssertionError("Got an unexpected exception from initTransactions", e);
                }
            }

            restartDeadBrokers(clusterInstance);

            try {
                producer1.beginTransaction();
            } catch (ProducerFencedException e) {
                // expected
            } catch (Exception e) {
                throw new AssertionError("Got an unexpected exception from beginTransaction", e);
            }
        }

        try (var producer3 = createTransactionalProducer(clusterInstance, "transactional-producer")) {
            producer3.initTransactions();

            producer3.beginTransaction();
            producer3.send(producerRecordWithExpectedTransactionStatus(TOPIC1, 0, "4", "4", true));
            producer3.commitTransaction();

            var finalProducers = waitForActiveProducers(clusterInstance, tp);
            var finalEntry = finalProducers.stream()
                    .filter(p -> p.producerId() == producerId)
                    .findFirst().orElseThrow();

            if (!isTV2Enabled) {
                assertEquals(initialProducerEpoch + 1, finalEntry.producerEpoch());
            } else {
                assertTrue(initialProducerEpoch + 1 <= finalEntry.producerEpoch());
            }
        }
    }

    public static void testEmptyAbortAfterCommit(
            ClusterInstance clusterInstance,
            Map<String, String> topicConfig
    ) throws Exception {
        createTopicsWithConfig(clusterInstance, topicConfig);

        try (var producer = createTransactionalProducer(clusterInstance, "transactional-producer")) {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(producerRecordWithExpectedTransactionStatus(TOPIC1, 1, "4", "4", false));
            producer.commitTransaction();

            producer.beginTransaction();
            producer.abortTransaction();
        }
    }

    // ---------------------------------------------------------------
    // Utility methods
    // ---------------------------------------------------------------

    public static ProducerRecord<byte[], byte[]> producerRecordWithExpectedTransactionStatus(
            String topic,
            Integer partition,
            String key,
            String value,
            boolean willBeCommitted
    ) {
        var header = new RecordHeader(HEADER_KEY, willBeCommitted ? COMMITTED_VALUE : ABORTED_VALUE);
        return new ProducerRecord<>(topic, partition,
                key.getBytes(StandardCharsets.UTF_8),
                value.getBytes(StandardCharsets.UTF_8),
                Collections.singleton(header));
    }

    public static String assertCommittedAndGetValue(ConsumerRecord<byte[], byte[]> record) {
        var headers = record.headers().headers(HEADER_KEY).iterator();
        assertTrue(headers.hasNext(), "Expected transaction status header");
        var header = headers.next();
        assertArrayEquals(COMMITTED_VALUE, header.value(), "Expected committed status");
        return new String(record.value(), StandardCharsets.UTF_8);
    }

    public static String recordValueAsString(ConsumerRecord<byte[], byte[]> record) {
        return new String(record.value(), StandardCharsets.UTF_8);
    }

    public static Producer<byte[], byte[]> createTransactionalProducer(
            ClusterInstance clusterInstance,
            String transactionalId
    ) {
        return createTransactionalProducer(clusterInstance, transactionalId,
                60000, 60000, 120000, 30000);
    }

    public static Producer<byte[], byte[]> createTransactionalProducer(
            ClusterInstance clusterInstance,
            String transactionalId,
            long transactionTimeoutMs,
            long maxBlockMs,
            int deliveryTimeoutMs,
            int requestTimeoutMs
    ) {
        return clusterInstance.producer(Map.of(
                ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId,
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, String.valueOf(transactionTimeoutMs),
                ProducerConfig.MAX_BLOCK_MS_CONFIG, String.valueOf(maxBlockMs),
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, String.valueOf(deliveryTimeoutMs),
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(requestTimeoutMs)
        ));
    }

    public static Consumer<byte[], byte[]> createReadCommittedConsumer(
            ClusterInstance clusterInstance,
            GroupProtocol groupProtocol,
            String groupId
    ) {
        return createReadCommittedConsumer(clusterInstance, groupProtocol, groupId, 500);
    }

    public static Consumer<byte[], byte[]> createReadCommittedConsumer(
            ClusterInstance clusterInstance,
            GroupProtocol groupProtocol,
            String groupId,
            int maxPollRecords
    ) {
        return clusterInstance.consumer(Map.of(
                ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol.name(),
                ConsumerConfig.GROUP_ID_CONFIG, groupId,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false",
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(maxPollRecords)
        ));
    }

    public static Consumer<byte[], byte[]> createReadUncommittedConsumer(
            ClusterInstance clusterInstance,
            GroupProtocol groupProtocol,
            String groupId
    ) {
        return clusterInstance.consumer(Map.of(
                ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol.name(),
                ConsumerConfig.GROUP_ID_CONFIG, groupId,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
        ));
    }

    public static List<ConsumerRecord<byte[], byte[]>> consumeRecords(
            Consumer<byte[], byte[]> consumer,
            int numRecords
    ) throws InterruptedException {
        return consumeRecords(consumer, numRecords, 30_000);
    }

    public static List<ConsumerRecord<byte[], byte[]>> consumeRecords(
            Consumer<byte[], byte[]> consumer,
            int numRecords,
            long waitTimeMs
    ) throws InterruptedException {
        var records = pollUntilAtLeastNumRecords(consumer, numRecords, waitTimeMs);
        assertEquals(numRecords, records.size(),
                "Consumed " + records.size() + " records instead of expected " + numRecords);
        return records;
    }

    public static List<ConsumerRecord<byte[], byte[]>> pollUntilAtLeastNumRecords(
            Consumer<byte[], byte[]> consumer,
            int numRecords
    ) throws InterruptedException {
        return pollUntilAtLeastNumRecords(consumer, numRecords, 30_000);
    }

    public static List<ConsumerRecord<byte[], byte[]>> pollUntilAtLeastNumRecords(
            Consumer<byte[], byte[]> consumer,
            int numRecords,
            long waitTimeMs
    ) throws InterruptedException {
        var records = new ArrayList<ConsumerRecord<byte[], byte[]>>();
        TestUtils.waitForCondition(() -> {
            var polled = consumer.poll(Duration.ofMillis(100));
            polled.forEach(records::add);
            return records.size() >= numRecords;
        }, waitTimeMs, () -> "Consumed " + records.size() + " records before timeout instead of expected " + numRecords);
        return records;
    }

    public static List<ConsumerRecord<byte[], byte[]>> consumeRecordsFor(
            Consumer<byte[], byte[]> consumer,
            long durationMs
    ) throws InterruptedException {
        var records = new ArrayList<ConsumerRecord<byte[], byte[]>>();
        var startTime = System.currentTimeMillis();
        TestUtils.waitForCondition(() -> {
            consumer.poll(Duration.ofMillis(50)).forEach(records::add);
            return System.currentTimeMillis() - startTime > durationMs;
        }, durationMs + 5000, "The timeout " + durationMs + " was greater than the maximum wait time.");
        return records;
    }

    public static void seedTopicWithNumberedRecords(
            ClusterInstance clusterInstance,
            String topic,
            int numRecords
    ) {
        try (var producer = clusterInstance.producer(Map.of(
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"))) {
            for (var i = 0; i < numRecords; i++) {
                var value = String.valueOf(i).getBytes(StandardCharsets.UTF_8);
                producer.send(new ProducerRecord<>(topic, value, value));
            }
            producer.flush();
        }
    }

    public static void resetToCommittedPositions(Consumer<byte[], byte[]> consumer) {
        var committed = consumer.committed(consumer.assignment());
        for (var tp : consumer.assignment()) {
            var offsetAndMetadata = committed.get(tp);
            if (offsetAndMetadata != null) {
                consumer.seek(tp, offsetAndMetadata.offset());
            } else {
                consumer.seekToBeginning(List.of(tp));
            }
        }
    }

    public static void sendTransactionalMessagesWithValueRange(
            Producer<byte[], byte[]> producer,
            String topic,
            int start,
            int end,
            boolean willBeCommitted
    ) {
        for (var i = start; i < end; i++) {
            producer.send(producerRecordWithExpectedTransactionStatus(
                    topic, null, String.valueOf(i), String.valueOf(i), willBeCommitted));
        }
        producer.flush();
    }

    public static void createTopicsWithConfig(
            ClusterInstance clusterInstance,
            Map<String, String> topicConfig
    ) throws InterruptedException {
        clusterInstance.createTopic(TOPIC1, NUM_PARTITIONS, BROKER_COUNT, topicConfig);
        clusterInstance.createTopic(TOPIC2, NUM_PARTITIONS, BROKER_COUNT, topicConfig);
    }

    public static void restartDeadBrokers(ClusterInstance clusterInstance) {
        clusterInstance.brokers().forEach((id, broker) -> {
            if (broker.isShutdown()) {
                clusterInstance.startBroker(id);
            }
        });
    }

    public static void assertConsumeCommittedRecords(
            ClusterInstance clusterInstance,
            List<String> topics,
            int expectedCount,
            GroupProtocol groupProtocol
    ) throws InterruptedException {
        try (var consumer = createReadCommittedConsumer(
                clusterInstance, groupProtocol, "fencing-consumer-" + groupProtocol.name())) {
            consumer.subscribe(topics);
            var records = consumeRecords(consumer, expectedCount);
            records.forEach(TransactionsTestHelper::assertCommittedAndGetValue);
        }
    }

    public static List<ProducerState> getActiveProducers(
            ClusterInstance clusterInstance,
            TopicPartition tp
    ) throws ExecutionException, InterruptedException {
        try (var admin = clusterInstance.admin()) {
            return admin.describeProducers(List.of(tp)).partitionResult(tp).get().activeProducers();
        }
    }

    public static List<ProducerState> waitForActiveProducers(
            ClusterInstance clusterInstance,
            TopicPartition tp
    ) throws InterruptedException {
        var result = new ArrayList<ProducerState>();
        TestUtils.waitForCondition(() -> {
            try {
                var producers = getActiveProducers(clusterInstance, tp);
                if (!producers.isEmpty()) {
                    result.clear();
                    result.addAll(producers);
                    return true;
                }
                return false;
            } catch (Exception e) {
                return false;
            }
        }, "Active producers for " + tp + " did not propagate");
        return result;
    }

    private static void testTimeout(
            ClusterInstance clusterInstance,
            boolean needInitAndSendMsg,
            java.util.function.Consumer<Producer<byte[], byte[]>> timeoutProcess
    ) {
        var producer = createTransactionalProducer(
                clusterInstance, "transactionProducer", 60000, 3000, 120000, 30000);
        try {
            if (needInitAndSendMsg) {
                producer.initTransactions();
                producer.beginTransaction();
                producer.send(new ProducerRecord<>(TOPIC1, "foo".getBytes(), "bar".getBytes()));
            }

            for (var id : clusterInstance.brokerIds()) {
                clusterInstance.shutdownBroker(id);
            }

            assertThrows(TimeoutException.class, () -> timeoutProcess.accept(producer));
        } finally {
            producer.close(Duration.ZERO);
        }
    }

    private static void assertFutureThrows(Future<?> future) {
        try {
            future.get(30, TimeUnit.SECONDS);
            fail("Should throw expected exception " + TimeoutException.class.getSimpleName());
        } catch (InterruptedException | ExecutionException e) {
            var cause = e instanceof ExecutionException ? e.getCause() : e;
            assertEquals(TimeoutException.class, cause.getClass(),
                    "Expected " + TimeoutException.class.getSimpleName() + ", but got " + cause.getClass().getSimpleName());
        } catch (java.util.concurrent.TimeoutException e) {
            fail("Future is not completed within timeout");
        }
    }
}
