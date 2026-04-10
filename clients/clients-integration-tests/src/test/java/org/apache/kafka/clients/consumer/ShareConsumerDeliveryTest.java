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
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.coordinator.group.GroupConfig;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(1200)
@ClusterTestDefaults(
    types = {Type.KRAFT},
    serverProperties = {
        @ClusterConfigProperty(key = "auto.create.topics.enable", value = "false"),
        @ClusterConfigProperty(key = "group.share.max.partition.max.record.locks", value = "10000"),
        @ClusterConfigProperty(key = "group.share.partition.max.record.locks", value = "10000"),
        @ClusterConfigProperty(key = "group.share.record.lock.duration.ms", value = "15000"),
        @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
        @ClusterConfigProperty(key = "share.coordinator.state.topic.min.isr", value = "1"),
        @ClusterConfigProperty(key = "share.coordinator.state.topic.num.partitions", value = "3"),
        @ClusterConfigProperty(key = "share.coordinator.state.topic.replication.factor", value = "1"),
        @ClusterConfigProperty(key = "transaction.state.log.min.isr", value = "1"),
        @ClusterConfigProperty(key = "transaction.state.log.replication.factor", value = "1")
    }
)
public class ShareConsumerDeliveryTest extends ShareConsumerTestBase {

    public ShareConsumerDeliveryTest(ClusterInstance cluster) {
        super(cluster);
    }

    @ClusterTest
    public void testShareAutoOffsetResetDefaultValue() {
        try (ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1");
             Producer<byte[], byte[]> producer = createProducer()) {

            shareConsumer.subscribe(Set.of(tp.topic()));
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            // Producing a record.
            producer.send(record);
            producer.flush();
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 0, true, "group1", List.of(tp));
            // No records should be consumed because share.auto.offset.reset has a default of "latest". Since the record
            // was produced before share partition was initialized (which happens after the first share fetch request
            // in the poll method), the start offset would be the latest offset, i.e. 1 (the next offset after the already
            // present 0th record)
            assertEquals(0, records.count());
            // Producing another record.
            producer.send(record);
            producer.flush();
            records = shareConsumer.poll(Duration.ofMillis(5000));
            // Now the next record should be consumed successfully
            assertEquals(1, records.count());
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testShareAutoOffsetResetEarliest() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1");
             Producer<byte[], byte[]> producer = createProducer()) {

            shareConsumer.subscribe(Set.of(tp.topic()));
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            // Producing a record.
            producer.send(record);
            producer.flush();
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 1);
            // Since the value for share.auto.offset.reset has been altered to "earliest", the consumer should consume
            // all messages present on the partition
            assertEquals(1, records.count());
            // Producing another record.
            producer.send(record);
            producer.flush();
            records = shareConsumer.poll(Duration.ofMillis(5000));
            // The next records should also be consumed successfully
            assertEquals(1, records.count());
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testShareAutoOffsetResetEarliestAfterLsoMovement() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (
            Producer<byte[], byte[]> producer = createProducer();
            Admin adminClient = createAdminClient()
        ) {

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            // We write 10 records to the topic, so they would be written from offsets 0-9 on the topic.
            for (int i = 0; i < 10; i++) {
                assertDoesNotThrow(() -> producer.send(record).get(), "Failed to send records");
            }

            // We delete records before offset 5, so the LSO should move to 5.
            assertDoesNotThrow(() -> adminClient.deleteRecords(Map.of(tp, RecordsToDelete.beforeOffset(5L))).all().get(), "Failed to delete records");

            int consumedMessageCount = consumeMessages(new AtomicInteger(0), 5, "group1", 1, 10, true);
            // The records returned belong to offsets 5-9.
            assertEquals(5, consumedMessageCount);
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testShareAutoOffsetResetMultipleGroupsWithDifferentValue() {
        alterShareAutoOffsetReset("group1", "earliest");
        alterShareAutoOffsetReset("group2", "latest");
        try (ShareConsumer<byte[], byte[]> shareConsumerEarliest = createShareConsumer("group1");
             ShareConsumer<byte[], byte[]> shareConsumerLatest = createShareConsumer("group2");
             Producer<byte[], byte[]> producer = createProducer()) {

            shareConsumerEarliest.subscribe(Set.of(tp.topic()));
            shareConsumerLatest.subscribe(Set.of(tp.topic()));

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            // Producing a record.
            producer.send(record);
            producer.flush();
            ConsumerRecords<byte[], byte[]> records1 = waitedPoll(shareConsumerEarliest, 2500L, 1);
            // Since the value for share.auto.offset.reset has been altered to "earliest", the consumer should consume
            // all messages present on the partition
            assertEquals(1, records1.count());

            ConsumerRecords<byte[], byte[]> records2 = waitedPoll(shareConsumerLatest, 2500L, 0, true, "group2", List.of(tp));
            // Since the value for share.auto.offset.reset has been altered to "latest", the consumer should not consume
            // any message
            assertEquals(0, records2.count());

            // Producing another record.
            producer.send(record);

            records1 = shareConsumerEarliest.poll(Duration.ofMillis(5000));
            // The next record should also be consumed successfully by group1
            assertEquals(1, records1.count());

            records2 = shareConsumerLatest.poll(Duration.ofMillis(5000));
            // The next record should also be consumed successfully by group2
            assertEquals(1, records2.count());
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testShareAutoOffsetResetByDuration() throws Exception {
        // Set auto offset reset to 1 hour before current time
        alterShareAutoOffsetReset("group1", "by_duration:PT1H");

        try (ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1");
             Producer<byte[], byte[]> producer = createProducer()) {

            long currentTime = System.currentTimeMillis();
            long twoHoursAgo = currentTime - TimeUnit.HOURS.toMillis(2);
            long thirtyMinsAgo = currentTime - TimeUnit.MINUTES.toMillis(30);

            // Produce messages with different timestamps
            ProducerRecord<byte[], byte[]> oldRecord = new ProducerRecord<>(tp.topic(), tp.partition(),
                twoHoursAgo, "old_key".getBytes(), "old_value".getBytes());
            ProducerRecord<byte[], byte[]> recentRecord = new ProducerRecord<>(tp.topic(), tp.partition(),
                thirtyMinsAgo, "recent_key".getBytes(), "recent_value".getBytes());
            ProducerRecord<byte[], byte[]> currentRecord = new ProducerRecord<>(tp.topic(), tp.partition(),
                currentTime, "current_key".getBytes(), "current_value".getBytes());

            producer.send(oldRecord).get();
            producer.send(recentRecord).get();
            producer.send(currentRecord).get();
            producer.flush();

            shareConsumer.subscribe(Set.of(tp.topic()));

            // Should only receive messages from last hour (recent and current)
            List<ConsumerRecord<byte[], byte[]>> records = consumeRecords(shareConsumer, 2);
            assertEquals(2, records.size());

            // Verify timestamps and order
            assertEquals(thirtyMinsAgo, records.get(0).timestamp());
            assertEquals("recent_key", new String(records.get(0).key()));
            assertEquals(currentTime, records.get(1).timestamp());
            assertEquals("current_key", new String(records.get(1).key()));
        }

        // Set the auto offset reset to 3 hours before current time
        // so the consumer should consume all messages (3 records)
        alterShareAutoOffsetReset("group2", "by_duration:PT3H");
        try (ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group2")) {

            shareConsumer.subscribe(Set.of(tp.topic()));
            List<ConsumerRecord<byte[], byte[]>> records = consumeRecords(shareConsumer, 3);
            assertEquals(3, records.size());
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testShareAutoOffsetResetByDurationInvalidFormat() {
        // Test invalid duration format
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.GROUP, "group1");
        Map<ConfigResource, Collection<AlterConfigOp>> alterEntries = new HashMap<>();

        // Test invalid duration format
        try (Admin adminClient = createAdminClient()) {
            alterEntries.put(configResource, List.of(new AlterConfigOp(new ConfigEntry(
                GroupConfig.SHARE_AUTO_OFFSET_RESET_CONFIG, "by_duration:1h"), AlterConfigOp.OpType.SET)));
            ExecutionException e1 = assertThrows(ExecutionException.class, () ->
                adminClient.incrementalAlterConfigs(alterEntries).all().get());
            assertInstanceOf(InvalidConfigurationException.class, e1.getCause());

            // Test negative duration
            alterEntries.put(configResource, List.of(new AlterConfigOp(new ConfigEntry(
                GroupConfig.SHARE_AUTO_OFFSET_RESET_CONFIG, "by_duration:-PT1H"), AlterConfigOp.OpType.SET)));
            ExecutionException e2 = assertThrows(ExecutionException.class, () ->
                adminClient.incrementalAlterConfigs(alterEntries).all().get());
            assertInstanceOf(InvalidConfigurationException.class, e2.getCause());
        }
    }

    @ClusterTest
    public void testDeliveryCountDifferentBehaviorWhenClosingSessionWithExplicitAcknowledgement() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                 "group1",
                 Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))) {

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null,
                "key".getBytes(), "value".getBytes());
            producer.send(record);
            producer.send(record);
            producer.flush();

            shareConsumer.subscribe(Set.of(tp.topic()));
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 2);
            assertEquals(2, records.count());
            // Acknowledge the first record with AcknowledgeType.RELEASE
            shareConsumer.acknowledge(records.records(tp).get(0), AcknowledgeType.RELEASE);
            Map<TopicIdPartition, Optional<KafkaException>> result = shareConsumer.commitSync();
            assertEquals(1, result.size());
        }

        // Test delivery count
        try (ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1", Map.of())) {
            shareConsumer.subscribe(Set.of(tp.topic()));
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 2);
            assertEquals(2, records.count());
            assertEquals((short) 2, records.records(tp).get(0).deliveryCount().get());
            assertEquals((short) 2, records.records(tp).get(1).deliveryCount().get());
        }
    }

    @ClusterTest(
        serverProperties = {
            @ClusterConfigProperty(key = "group.share.delivery.count.limit", value = "3"),
        }
    )
    public void testBehaviorOnDeliveryCountBoundary() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                 "group1",
                 Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))) {

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null,
                "key".getBytes(), "value".getBytes());
            producer.send(record);
            producer.flush();

            shareConsumer.subscribe(Set.of(tp.topic()));
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 1);
            assertEquals(1, records.count());
            assertEquals((short) 1, records.records(tp).get(0).deliveryCount().get());
            // Acknowledge the record with AcknowledgeType.RELEASE.
            shareConsumer.acknowledge(records.records(tp).get(0), AcknowledgeType.RELEASE);
            Map<TopicIdPartition, Optional<KafkaException>> result = shareConsumer.commitSync();
            assertEquals(1, result.size());

            // Consume again, the delivery count should be 2.
            records = waitedPoll(shareConsumer, 2500L, 1);
            assertEquals(1, records.count());
            assertEquals((short) 2, records.records(tp).get(0).deliveryCount().get());
        }

        // Start again and same record should be delivered
        try (ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1", Map.of())) {
            shareConsumer.subscribe(Set.of(tp.topic()));
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 1);
            assertEquals(1, records.count());
            assertEquals((short) 3, records.records(tp).get(0).deliveryCount().get());
        }
    }

    @ClusterTest
    public void testReadCommittedIsolationLevel() {
        alterShareAutoOffsetReset("group1", "earliest");
        alterShareIsolationLevel("group1", "read_committed");
        try (Producer<byte[], byte[]> transactionalProducer = createProducer("T1");
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {
            produceCommittedAndAbortedTransactionsInInterval(transactionalProducer, 10, 5);
            shareConsumer.subscribe(Set.of(tp.topic()));
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 5000L, 8);
            // 5th and 10th message transaction was aborted, hence they won't be included in the fetched records.
            assertEquals(8, records.count());
            int messageCounter = 1;
            for (ConsumerRecord<byte[], byte[]> record : records) {
                assertEquals(tp.topic(), record.topic());
                assertEquals(tp.partition(), record.partition());
                if (messageCounter % 5 == 0)
                    messageCounter++;
                assertEquals("Message " + messageCounter, new String(record.value()));
                messageCounter++;
            }
        }
        verifyShareGroupStateTopicRecordsProduced();
    }

    @ClusterTest
    public void testReadUncommittedIsolationLevel() {
        alterShareAutoOffsetReset("group1", "earliest");
        alterShareIsolationLevel("group1", "read_uncommitted");
        try (Producer<byte[], byte[]> transactionalProducer = createProducer("T1");
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {
            produceCommittedAndAbortedTransactionsInInterval(transactionalProducer, 10, 5);
            shareConsumer.subscribe(Set.of(tp.topic()));
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 5000L, 10);
            // Even though 5th and 10th message transaction was aborted, they will be included in the fetched records since IsolationLevel is READ_UNCOMMITTED.
            assertEquals(10, records.count());
            int messageCounter = 1;
            for (ConsumerRecord<byte[], byte[]> record : records) {
                assertEquals(tp.topic(), record.topic());
                assertEquals(tp.partition(), record.partition());
                assertEquals("Message " + messageCounter, new String(record.value()));
                messageCounter++;
            }
        }
        verifyShareGroupStateTopicRecordsProduced();
    }

    @ClusterTest
    public void testAlterReadUncommittedToReadCommittedIsolationLevel() {
        alterShareAutoOffsetReset("group1", "earliest");
        alterShareIsolationLevel("group1", "read_uncommitted");
        try (Producer<byte[], byte[]> transactionalProducer = createProducer("T1");
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1", Map.of(
                 ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))) {
            shareConsumer.subscribe(Set.of(tp.topic()));
            transactionalProducer.initTransactions();
            try {
                // First transaction is committed.
                produceCommittedTransaction(transactionalProducer, "Message 1");

                ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 5000L, 1);
                assertEquals(1, records.count());
                ConsumerRecord<byte[], byte[]> record = records.iterator().next();
                assertEquals("Message 1", new String(record.value()));
                assertEquals(tp.topic(), record.topic());
                assertEquals(tp.partition(), record.partition());
                records.forEach(consumedRecord -> shareConsumer.acknowledge(consumedRecord, AcknowledgeType.ACCEPT));
                shareConsumer.commitSync();

                // Second transaction is aborted.
                produceAbortedTransaction(transactionalProducer, "Message 2");

                records = waitedPoll(shareConsumer, 5000L, 1);
                assertEquals(1, records.count());
                record = records.iterator().next();
                assertEquals("Message 2", new String(record.value()));
                records.forEach(consumedRecord -> shareConsumer.acknowledge(consumedRecord, AcknowledgeType.ACCEPT));
                shareConsumer.commitSync();

                // Third transaction is committed.
                produceCommittedTransaction(transactionalProducer, "Message 3");
                // Fourth transaction is aborted.
                produceAbortedTransaction(transactionalProducer, "Message 4");

                records = waitedPoll(shareConsumer, 5000L, 2);
                // Message 3 and Message 4 would be returned by this poll.
                assertEquals(2, records.count());
                Iterator<ConsumerRecord<byte[], byte[]>> recordIterator = records.iterator();
                record = recordIterator.next();
                assertEquals("Message 3", new String(record.value()));
                record = recordIterator.next();
                assertEquals("Message 4", new String(record.value()));
                // We will make Message 3 and Message 4 available for re-consumption.
                records.forEach(consumedRecord -> shareConsumer.acknowledge(consumedRecord, AcknowledgeType.RELEASE));
                shareConsumer.commitSync();

                // We are altering IsolationLevel to READ_COMMITTED now. We will only read committed transactions now.
                alterShareIsolationLevel("group1", "read_committed");

                // Fifth transaction is committed.
                produceCommittedTransaction(transactionalProducer, "Message 5");
                // Sixth transaction is aborted.
                produceAbortedTransaction(transactionalProducer, "Message 6");
                // Seventh transaction is aborted.
                produceAbortedTransaction(transactionalProducer, "Message 7");
                // Eighth transaction is committed.
                produceCommittedTransaction(transactionalProducer, "Message 8");

                // Since isolation level is READ_COMMITTED, we can consume Message 3 (committed transaction that was released), Message 5 and Message 8.
                // We cannot consume Message 4 (aborted transaction that was released), Message 6 and Message 7 since they were aborted.
                List<String> messages = new ArrayList<>();
                TestUtils.waitForCondition(() -> {
                    ConsumerRecords<byte[], byte[]> pollRecords = shareConsumer.poll(Duration.ofMillis(5000));
                    if (pollRecords.count() > 0) {
                        for (ConsumerRecord<byte[], byte[]> pollRecord : pollRecords)
                            messages.add(new String(pollRecord.value()));
                        pollRecords.forEach(consumedRecord -> shareConsumer.acknowledge(consumedRecord, AcknowledgeType.ACCEPT));
                        shareConsumer.commitSync();
                    }
                    return messages.size() == 3;
                }, DEFAULT_MAX_WAIT_MS, 100L, () -> "Failed to consume all records post altering share isolation level");

                assertEquals("Message 3", messages.get(0));
                assertEquals("Message 5", messages.get(1));
                assertEquals("Message 8", messages.get(2));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                transactionalProducer.close();
            }
        }
        verifyShareGroupStateTopicRecordsProduced();
    }

    @ClusterTest
    public void testAlterReadCommittedToReadUncommittedIsolationLevelWithReleaseAck() {
        alterShareAutoOffsetReset("group1", "earliest");
        alterShareIsolationLevel("group1", "read_committed");
        try (Producer<byte[], byte[]> transactionalProducer = createProducer("T1");
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1", Map.of(
                 ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))) {
            shareConsumer.subscribe(Set.of(tp.topic()));
            transactionalProducer.initTransactions();

            try {
                // First transaction is committed.
                produceCommittedTransaction(transactionalProducer, "Message 1");

                ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 5000L, 1);
                assertEquals(1, records.count());
                ConsumerRecord<byte[], byte[]> record = records.iterator().next();
                assertEquals("Message 1", new String(record.value()));
                assertEquals(tp.topic(), record.topic());
                assertEquals(tp.partition(), record.partition());
                records.forEach(consumedRecord -> shareConsumer.acknowledge(consumedRecord, AcknowledgeType.ACCEPT));
                shareConsumer.commitSync();

                // Second transaction is aborted.
                produceAbortedTransaction(transactionalProducer, "Message 2");

                // Setting the acknowledgement commit callback to verify acknowledgement completion.
                Map<TopicPartition, Set<Long>> partitionOffsetsMap = new HashMap<>();
                shareConsumer.setAcknowledgementCommitCallback(new TestableAcknowledgementCommitCallback(partitionOffsetsMap, Map.of()));

                // We will not receive any records since the transaction for Message 2 was aborted. Wait for the
                // aborted marker offset for Message 2 (3L) to be fetched and acknowledged by the consumer.
                TestUtils.waitForCondition(() -> {
                    ConsumerRecords<byte[], byte[]> pollRecords = shareConsumer.poll(Duration.ofMillis(500));
                    return pollRecords.count() == 0 && partitionOffsetsMap.containsKey(tp) && partitionOffsetsMap.get(tp).contains(3L);
                }, DEFAULT_MAX_WAIT_MS, 100L, () -> "Failed to consume abort transaction and marker offset for Message 2");

                // Third transaction is committed.
                produceCommittedTransaction(transactionalProducer, "Message 3");
                // Fourth transaction is aborted.
                produceAbortedTransaction(transactionalProducer, "Message 4");

                // Setting the acknowledgement commit callback to verify acknowledgement completion.
                Map<TopicPartition, Set<Long>> partitionOffsetsMap2 = new HashMap<>();
                shareConsumer.setAcknowledgementCommitCallback(new TestableAcknowledgementCommitCallback(partitionOffsetsMap2, Map.of()));

                records = waitedPoll(shareConsumer, 5000L, 1);
                // Message 3 would be returned by this poll.
                assertEquals(1, records.count());
                record = records.iterator().next();
                assertEquals("Message 3", new String(record.value()));
                // We will make Message 3 available for re-consumption.
                records.forEach(consumedRecord -> shareConsumer.acknowledge(consumedRecord, AcknowledgeType.RELEASE));
                shareConsumer.commitSync();

                // Wait for the aborted marker offset for Message 4 (7L) to be fetched and acknowledged by the consumer.
                TestUtils.waitForCondition(() -> {
                    ConsumerRecords<byte[], byte[]> pollRecords = shareConsumer.poll(Duration.ofMillis(500));
                    if (pollRecords.count() > 0) {
                        // We will release Message 3 again if it was received in this poll().
                        pollRecords.forEach(consumerRecord -> shareConsumer.acknowledge(consumerRecord, AcknowledgeType.RELEASE));
                    }
                    return partitionOffsetsMap2.containsKey(tp) && partitionOffsetsMap2.get(tp).contains(7L);
                }, DEFAULT_MAX_WAIT_MS, 100L, () -> "Failed to consume abort transaction marker offset for Message 4");

                // We are altering IsolationLevel to READ_UNCOMMITTED now. We will read both committed/aborted transactions now.
                alterShareIsolationLevel("group1", "read_uncommitted");

                // Fifth transaction is committed.
                produceCommittedTransaction(transactionalProducer, "Message 5");
                // Sixth transaction is aborted.
                produceAbortedTransaction(transactionalProducer, "Message 6");
                // Seventh transaction is aborted.
                produceAbortedTransaction(transactionalProducer, "Message 7");
                // Eighth transaction is committed.
                produceCommittedTransaction(transactionalProducer, "Message 8");

                // Since isolation level is READ_UNCOMMITTED, we can consume Message 3 (committed transaction that was released), Message 5, Message 6, Message 7 and Message 8.
                Set<String> finalMessages = new HashSet<>();
                TestUtils.waitForCondition(() -> {
                    ConsumerRecords<byte[], byte[]> pollRecords = shareConsumer.poll(Duration.ofMillis(5000));
                    if (pollRecords.count() > 0) {
                        for (ConsumerRecord<byte[], byte[]> pollRecord : pollRecords)
                            finalMessages.add(new String(pollRecord.value()));
                        pollRecords.forEach(consumedRecord -> shareConsumer.acknowledge(consumedRecord, AcknowledgeType.ACCEPT));
                        shareConsumer.commitSync();
                    }
                    return finalMessages.size() == 5;
                }, DEFAULT_MAX_WAIT_MS, 100L, () -> "Failed to consume all records post altering share isolation level");

                Set<String> expected = Set.of("Message 3", "Message 5", "Message 6", "Message 7", "Message 8");
                assertEquals(expected, finalMessages);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                transactionalProducer.close();
            }
        }
        verifyShareGroupStateTopicRecordsProduced();
    }

    @ClusterTest
    public void testAlterReadCommittedToReadUncommittedIsolationLevelWithRejectAck() {
        alterShareAutoOffsetReset("group1", "earliest");
        alterShareIsolationLevel("group1", "read_committed");
        try (Producer<byte[], byte[]> transactionalProducer = createProducer("T1");
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1", Map.of(
                 ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))) {
            shareConsumer.subscribe(Set.of(tp.topic()));
            transactionalProducer.initTransactions();

            try {
                // First transaction is committed.
                produceCommittedTransaction(transactionalProducer, "Message 1");

                ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 5000L, 1);
                assertEquals(1, records.count());
                ConsumerRecord<byte[], byte[]> record = records.iterator().next();
                assertEquals("Message 1", new String(record.value()));
                assertEquals(tp.topic(), record.topic());
                assertEquals(tp.partition(), record.partition());
                records.forEach(consumedRecord -> shareConsumer.acknowledge(consumedRecord, AcknowledgeType.ACCEPT));
                shareConsumer.commitSync();

                // Second transaction is aborted.
                produceAbortedTransaction(transactionalProducer, "Message 2");

                // Setting the acknowledgement commit callback to verify acknowledgement completion.
                Map<TopicPartition, Set<Long>> partitionOffsetsMap = new HashMap<>();
                shareConsumer.setAcknowledgementCommitCallback(new TestableAcknowledgementCommitCallback(partitionOffsetsMap, Map.of()));

                // We will not receive any records since the transaction for Message 2 was aborted. Wait for the
                // aborted marker offset for Message 2 (3L) to be fetched and acknowledged by the consumer.
                TestUtils.waitForCondition(() -> {
                    ConsumerRecords<byte[], byte[]> pollRecords = shareConsumer.poll(Duration.ofMillis(500));
                    return pollRecords.count() == 0 && partitionOffsetsMap.containsKey(tp) && partitionOffsetsMap.get(tp).contains(3L);
                }, DEFAULT_MAX_WAIT_MS, 100L, () -> "Failed to consume abort transaction and marker offset for Message 2");

                // Third transaction is committed.
                produceCommittedTransaction(transactionalProducer, "Message 3");
                // Fourth transaction is aborted.
                produceAbortedTransaction(transactionalProducer, "Message 4");

                // Setting the acknowledgement commit callback to verify acknowledgement completion.
                Map<TopicPartition, Set<Long>> partitionOffsetsMap2 = new HashMap<>();
                shareConsumer.setAcknowledgementCommitCallback(new TestableAcknowledgementCommitCallback(partitionOffsetsMap2, Map.of()));

                records = waitedPoll(shareConsumer, 5000L, 1);
                // Message 3 would be returned by this poll.
                assertEquals(1, records.count());
                record = records.iterator().next();
                assertEquals("Message 3", new String(record.value()));
                // We will make Message 3 available for re-consumption.
                records.forEach(consumedRecord -> shareConsumer.acknowledge(consumedRecord, AcknowledgeType.REJECT));
                shareConsumer.commitSync();

                // Wait for the aborted marker offset for Message 4 (7L) to be fetched and acknowledged by the consumer.
                TestUtils.waitForCondition(() -> {
                    shareConsumer.poll(Duration.ofMillis(500));
                    return partitionOffsetsMap2.containsKey(tp) && partitionOffsetsMap2.get(tp).contains(7L);
                }, DEFAULT_MAX_WAIT_MS, 100L, () -> "Failed to consume abort transaction marker offset for Message 4");

                // We are altering IsolationLevel to READ_UNCOMMITTED now. We will read both committed/aborted transactions now.
                alterShareIsolationLevel("group1", "read_uncommitted");

                // Fifth transaction is committed.
                produceCommittedTransaction(transactionalProducer, "Message 5");
                // Sixth transaction is aborted.
                produceAbortedTransaction(transactionalProducer, "Message 6");
                // Seventh transaction is aborted.
                produceAbortedTransaction(transactionalProducer, "Message 7");
                // Eighth transaction is committed.
                produceCommittedTransaction(transactionalProducer, "Message 8");

                // Since isolation level is READ_UNCOMMITTED, we can consume Message 5, Message 6, Message 7 and Message 8.
                List<String> finalMessages = new ArrayList<>();
                TestUtils.waitForCondition(() -> {
                    ConsumerRecords<byte[], byte[]> pollRecords = shareConsumer.poll(Duration.ofMillis(5000));
                    if (pollRecords.count() > 0) {
                        for (ConsumerRecord<byte[], byte[]> pollRecord : pollRecords)
                            finalMessages.add(new String(pollRecord.value()));
                        pollRecords.forEach(consumedRecord -> shareConsumer.acknowledge(consumedRecord, AcknowledgeType.ACCEPT));
                        shareConsumer.commitSync();
                    }
                    return finalMessages.size() == 4;
                }, DEFAULT_MAX_WAIT_MS, 100L, () -> "Failed to consume all records post altering share isolation level");

                assertEquals("Message 5", finalMessages.get(0));
                assertEquals("Message 6", finalMessages.get(1));
                assertEquals("Message 7", finalMessages.get(2));
                assertEquals("Message 8", finalMessages.get(3));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                transactionalProducer.close();
            }
        }
        verifyShareGroupStateTopicRecordsProduced();
    }

    @ClusterTest
    public void testPollInBatchOptimizedMode() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                 "group1",
                 Map.of(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5, ConsumerConfig.SHARE_ACQUIRE_MODE_CONFIG, BATCH_OPTIMIZED))
        ) {
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            for (int i = 0; i < 10; i++) {
                producer.send(record);
            }
            producer.flush();
            shareConsumer.subscribe(List.of(tp.topic()));

            // although max.poll.records is set to 5, in batch optimized mode we will still get all 10 records.
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 10);
            assertEquals(10, records.count());
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testPollInRecordLimitMode() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                 "group1",
                 Map.of(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5, ConsumerConfig.SHARE_ACQUIRE_MODE_CONFIG, RECORD_LIMIT))
        ) {
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            for (int i = 0; i < 10; i++) {
                producer.send(record);
            }
            producer.flush();
            shareConsumer.subscribe(List.of(tp.topic()));

            // In record limit mode we will get only up to max.poll.records number of records.
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 5);
            assertEquals(5, records.count());
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testPollAndExplicitAcknowledgeSingleMessageInRecordLimitMode() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                 "group1",
                 Map.of(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1,
                     ConsumerConfig.SHARE_ACQUIRE_MODE_CONFIG, RECORD_LIMIT,
                     ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))
        ) {
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            for (int i = 0; i < 10; i++) {
                producer.send(record);
            }
            producer.flush();
            shareConsumer.subscribe(List.of(tp.topic()));

            for (int i = 0; i < 10; i++) {
                ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 1);
                assertEquals(1, records.count());
                for (ConsumerRecord<byte[], byte[]> rec : records) {
                    shareConsumer.acknowledge(rec, AcknowledgeType.ACCEPT);
                }
                shareConsumer.commitSync();
            }
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testExplicitAcknowledgeSuccessInRecordLimitMode() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                 "group1",
                 Map.of(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10,
                     ConsumerConfig.SHARE_ACQUIRE_MODE_CONFIG, RECORD_LIMIT,
                     ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))
        ) {
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            for (int i = 0; i < 15; i++) {
                producer.send(record);
            }
            producer.flush();
            shareConsumer.subscribe(List.of(tp.topic()));
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 10);
            assertEquals(10, records.count());
            for (ConsumerRecord<byte[], byte[]> rec : records) {
                shareConsumer.acknowledge(rec, AcknowledgeType.ACCEPT);
            }
            shareConsumer.commitSync();

            records = waitedPoll(shareConsumer, 2500L, 5);
            assertEquals(5, records.count());
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testExplicitAcknowledgeReleaseAcceptInRecordLimitMode() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                 "group1",
                 Map.of(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10,
                     ConsumerConfig.SHARE_ACQUIRE_MODE_CONFIG, RECORD_LIMIT,
                     ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))
        ) {
            for (int i = 0; i < 20; i++) {
                ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp2.topic(), tp2.partition(), null, "key".getBytes(), ("Message " + i).getBytes());
                producer.send(record);
            }
            producer.flush();
            shareConsumer.subscribe(List.of(tp2.topic()));
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 10);
            assertEquals(10, records.count());

            int count = 0;
            Map<TopicIdPartition, Optional<KafkaException>> result;
            for (ConsumerRecord<byte[], byte[]> record : records) {
                if (count % 2 == 0) {
                    shareConsumer.acknowledge(record, AcknowledgeType.ACCEPT);
                } else {
                    shareConsumer.acknowledge(record, AcknowledgeType.RELEASE);
                }
                result = shareConsumer.commitSync();
                assertEquals(1, result.size());
                count++;
            }

            // Poll again to get 10 records.
            records = waitedPoll(shareConsumer, 2500L, 10);
            assertEquals(10, records.count());
            for (ConsumerRecord<byte[], byte[]> record : records) {
                shareConsumer.acknowledge(record, AcknowledgeType.ACCEPT);
            }

            // Get the rest of all 5 records.
            records = waitedPoll(shareConsumer, 2500L, 5);
            assertEquals(5, records.count());
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testFetchWithThrottledDelivery() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                 "group1",
                 Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))
        ) {
            // Produce a batch of 100 messages
            for (int i = 0; i < 100; i++) {
                ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), ("Message " + i).getBytes());
                producer.send(record);
            }
            producer.flush();

            shareConsumer.subscribe(List.of(tp.topic()));
            // Fetch records in 5 iterations, each time acknowledging with RELEASE. 5 is the default
            // delivery limit hence we should see throttling from Math.ceil(5/2) = 3 fetches.
            int throttleDeliveryLimit = 3;
            for (int i = 0; i < 5; i++) {
                // Adjust expected fetch count based on throttling. If i < throttleDeliveryLimit, we get full batch of 100.
                // If i == 4 i.e. the last delivery, then we get 1 record.
                // Otherwise, we get half the previous fetch count due to throttling. In this case, 100 >> (i - throttleDeliveryLimit + 1) it is 50 for i=3.
                int expectedFetchCount = (i < throttleDeliveryLimit) ? 100 : ((i == 4) ? 1 : 50);
                ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, expectedFetchCount);
                assertEquals(expectedFetchCount, records.count());

                records.forEach(record -> shareConsumer.acknowledge(record, AcknowledgeType.RELEASE));
                Map<TopicIdPartition, Optional<KafkaException>> result = shareConsumer.commitSync();
                assertEquals(1, result.size());
                assertEquals(Optional.empty(),
                    result.get(new TopicIdPartition(tpId, tp.partition(), tp.topic())));
            }

            // Offset 0 has already reached the delivery limit hence shall be archived.
            // Offset 1 to 49 shall be in last delivery attempt and hence 1 record per poll.
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 1, 50, 1);
            // Delivery limit 4.
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 50, 100, 50);
            // Delivery limit 5.
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 50, 100, 1);
            // Next poll should not have any records as all records have reached delivery limit.
            ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(2500L));
            assertTrue(records.isEmpty(), "Records should be empty as all records have reached delivery limit. But received: " + records.count());
        }
    }

    @ClusterTest(
        serverProperties = {
            @ClusterConfigProperty(key = "group.share.delivery.count.limit", value = "10"),
        }
    )
    public void testFetchWithThrottledDeliveryBatchesWithIncreasedDeliveryLimit() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                 "group1",
                 Map.of(
                     ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT,
                     ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 512
                 )
             )
        ) {
            // Produce records in complete power of 2 to fully test the throttling behavior.
            int producedMessageCount = 512;
            // Produce a batch of 512 messages
            for (int i = 0; i < producedMessageCount; i++) {
                ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(),
                    null, "key".getBytes(), ("Message " + i).getBytes());
                producer.send(record);
            }
            producer.flush();

            // Map which defines expected fetch count for each delivery attempt from 1 to 10.
            Map<Integer, Integer> expectedFetchCountMap = Map.of(1, 512, 2, 512, 3, 512, 4, 512, 5, 512,
                6, 256, 7, 128, 8, 64, 9, 32, 10, 1);
            shareConsumer.subscribe(List.of(tp.topic()));
            // Fetch records in 10 iterations, each time acknowledging with RELEASE. 10 is the
            // delivery limit hence we should see throttling from Math.ceil(10/2) = 5 fetches.
            for (int i = 0; i < 10; i++) {
                int expectedFetchCount = expectedFetchCountMap.get(i + 1);
                ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, expectedFetchCount);
                assertEquals(expectedFetchCount, records.count());
                // Acknowledge all records with RELEASE.
                records.forEach(record -> shareConsumer.acknowledge(record, AcknowledgeType.RELEASE));
                Map<TopicIdPartition, Optional<KafkaException>> result = shareConsumer.commitSync();
                assertEquals(1, result.size());
                assertEquals(Optional.empty(), result.get(new TopicIdPartition(tpId, tp.partition(), tp.topic())));
            }

            // Offset 0 is already verified above, so start from offset 1 and as it's last delivery cycle
            // hence expectedRecords is 1 for each poll till offset 32.
            // Delivery 10
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 1, 32, 1);
            // Delivery 9
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 32, 64, 32);
            // Delivery 10
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 32, 64, 1);
            // Delivery 8
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 64, 128, 64);
            // Delivery 9
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 64, 96, 32);
            // Delivery 10
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 64, 96, 1);
            // Delivery 9
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 96, 128, 32);
            // Delivery 10
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 96, 128, 1);
            // Delivery 7
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 128, 256, 128);
            // Delivery 8
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 128, 192, 64);
            // Delivery 9
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 128, 160, 32);
            // Delivery 10
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 128, 160, 1);
            // Delivery 9
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 160, 192, 32);
            // Delivery 10
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 160, 192, 1);
            // Delivery 8
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 192, 256, 64);
            // Delivery 9
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 192, 224, 32);
            // Delivery 10
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 192, 224, 1);
            // Delivery 9
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 224, 256, 32);
            // Delivery 10
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 224, 256, 1);
            // Delivery 6
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 256, 512, 256);
            // Delivery 7
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 256, 384, 128);
            // Delivery 8
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 256, 320, 64);
            // Delivery 9
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 256, 288, 32);
            // Delivery 10
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 256, 288, 1);
            // Delivery 9
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 288, 320, 32);
            // Delivery 10
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 288, 320, 1);
            // Delivery 8
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 320, 384, 64);
            // Delivery 9
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 320, 352, 32);
            // Delivery 10
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 320, 352, 1);
            // Delivery 9
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 352, 384, 32);
            // Delivery 10
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 352, 384, 1);
            // Delivery 7
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 384, 512, 128);
            // Delivery 8
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 384, 448, 64);
            // Delivery 9
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 384, 416, 32);
            // Delivery 10
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 384, 416, 1);
            // Delivery 9
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 416, 448, 32);
            // Delivery 10
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 416, 448, 1);
            // Delivery 8
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 448, 512, 64);
            // Delivery 9
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 448, 480, 32);
            // Delivery 10
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 448, 480, 1);
            // Delivery 9
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 480, 512, 32);
            // Delivery 10
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 480, 512, 1);
            // Next poll should not have any records as all records have reached delivery limit.
            ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(2500L));
            assertTrue(records.isEmpty(), "Records should be empty as all records have reached delivery limit. But received: " + records.count());
        }
    }

    @ClusterTest(
        serverProperties = {
            @ClusterConfigProperty(key = "group.share.delivery.count.limit", value = "10"),
        }
    )
    public void testFetchWithThrottledDeliveryValidateDeliveryCount() throws InterruptedException {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                 "group1",
                 Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))
        ) {
            int producedMessageCount = 500;
            // Produce a batch of 500 messages
            for (int i = 0; i < producedMessageCount; i++) {
                ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(),
                    null, "key".getBytes(), ("Message " + i).getBytes());
                producer.send(record);
            }
            producer.flush();

            // Map to track delivery count for each offset.
            Map<Long, Integer> offsetToDeliveryCountMap = new HashMap<>();
            // Map which defines expected fetch count for each delivery attempt from 1 to 10.
            Map<Integer, Integer> expectedFetchCountMap = Map.of(1, 500, 2, 500, 3, 500, 4, 500, 5, 500,
                6, 250, 7, 125, 8, 62, 9, 31, 10, 1);
            shareConsumer.subscribe(List.of(tp.topic()));
            // Fetch records in 10 iterations, each time acknowledging with RELEASE. 10 is the
            // delivery limit hence we should see throttling from Math.ceil(10/2) = 5 fetches.
            for (int i = 0; i < 10; i++) {
                int expectedFetchCount = expectedFetchCountMap.get(i + 1);
                ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, expectedFetchCount);
                assertEquals(expectedFetchCount, records.count());
                // Update delivery count for each offset.
                records.forEach(record -> {
                    if (!offsetToDeliveryCountMap.containsKey(record.offset())) {
                        offsetToDeliveryCountMap.put(record.offset(), 1);
                    } else  {
                        offsetToDeliveryCountMap.put(record.offset(), offsetToDeliveryCountMap.get(record.offset()) + 1);
                    }
                });
                // Acknowledge with RELEASE.
                records.forEach(record -> shareConsumer.acknowledge(record, AcknowledgeType.RELEASE));
                Map<TopicIdPartition, Optional<KafkaException>> result = shareConsumer.commitSync();
                assertEquals(1, result.size());
                assertEquals(Optional.empty(), result.get(new TopicIdPartition(tpId, tp.partition(), tp.topic())));
            }

            // Validate every offset is delivered at most till delivery limit.
            waitForCondition(() -> {
                    ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(2500L));
                    if (!records.isEmpty()) {
                        records.forEach(record -> {
                            if (!offsetToDeliveryCountMap.containsKey(record.offset())) {
                                offsetToDeliveryCountMap.put(record.offset(), 1);
                            } else  {
                                offsetToDeliveryCountMap.put(record.offset(), offsetToDeliveryCountMap.get(record.offset()) + 1);
                            }
                        });
                        records.forEach(record -> shareConsumer.acknowledge(record, AcknowledgeType.RELEASE));
                        Map<TopicIdPartition, Optional<KafkaException>> result = shareConsumer.commitSync();
                        assertEquals(1, result.size());
                        assertEquals(Optional.empty(),
                            result.get(new TopicIdPartition(tpId, tp.partition(), tp.topic())));
                    }
                    return offsetToDeliveryCountMap.size() == 500 &&
                        offsetToDeliveryCountMap.values().stream().allMatch(deliveryCount -> deliveryCount == 10);
                },
                120000L, // 120 seconds.
                50L,
                () -> "failed to get records till delivery limit"
            );

            // Next poll should not have any records as all records have reached delivery limit.
            ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(2500L));
            assertTrue(records.isEmpty(), "Records should be empty as all records have reached delivery limit. But received: " + records.count());
        }
    }

    @ClusterTest(
        serverProperties = {
            @ClusterConfigProperty(key = "group.share.delivery.count.limit", value = "2"),
        }
    )
    public void testFetchWithThrottledDeliveryBatchesWithDecreasedDeliveryLimit() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                 "group1",
                 Map.of(
                     ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT,
                     ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 512
                 ))
        ) {
            // Produce records in complete power of 2 to fully test the throttling behavior.
            int producedMessageCount = 512;
            // Produce a batch of 512 messages
            for (int i = 0; i < producedMessageCount; i++) {
                ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(),
                    null, "key".getBytes(), ("Message " + i).getBytes());
                producer.send(record);
            }
            producer.flush();

            shareConsumer.subscribe(List.of(tp.topic()));
            // Fetch records in 2 iterations, each time acknowledging with RELEASE. As throttling
            // currently applies for delivery limit > 2, hence we should get full batch in both fetches.
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 0, 512, 512);
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 0, 512, 512);
            // Next poll should not have any records as all records have reached delivery limit.
            ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(2500L));
            assertTrue(records.isEmpty(), "Records should be empty as all records have reached delivery limit. But received: " + records.count());
        }
    }

    @ClusterTest
    public void testFetchWithThrottledDeliveryBatchesMultipleConsumers() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer1 = createShareConsumer(
                 "group1",
                 Map.of(
                     ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT,
                     ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1
                 ));
             ShareConsumer<byte[], byte[]> shareConsumer2 = createShareConsumer(
                 "group1",
                 Map.of(
                     ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT,
                     ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 2
                 ))
        ) {
            // Produce 2 records in separate batches.
            for (int i = 0; i < 2; i++) {
                ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(),
                    null, "key".getBytes(), ("Message " + i).getBytes());
                producer.send(record);
                // Flush immediately to create 2 different batches.
                producer.flush();
            }

            shareConsumer1.subscribe(List.of(tp.topic()));
            shareConsumer2.subscribe(List.of(tp.topic()));
            // Fetch from consumer1 - should get 1 record as max.poll.records=1.
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer1, 2500L, 1);
            assertEquals(1, records.count());
            // Verify the first offset of the fetched records.
            assertEquals(0, records.iterator().next().offset());
            // Fetch from consumer2 - should get 1 record as offset 0 is Acquired by consumer1.
            // Release the record from consumer2 after fetching until the last delivery attempt.
            validateExpectedRecordsInEachPollAndRelease(shareConsumer2, 1, 2, 1);
            validateExpectedRecordsInEachPollAndRelease(shareConsumer2, 1, 2, 1);
            validateExpectedRecordsInEachPollAndRelease(shareConsumer2, 1, 2, 1);
            validateExpectedRecordsInEachPollAndRelease(shareConsumer2, 1, 2, 1);

            // Now release the record from consumer1. Fetch again from consumer2 to verify it gets the released record.
            // And should only get 1 record at offset 0 as offset 1 record is in final delivery attempt.
            records.forEach(record -> shareConsumer1.acknowledge(record, AcknowledgeType.RELEASE));
            Map<TopicIdPartition, Optional<KafkaException>> result = shareConsumer1.commitSync();
            assertEquals(1, result.size());
            assertEquals(Optional.empty(), result.get(new TopicIdPartition(tpId, tp.partition(), tp.topic())));
            // Fetch from consumer2 - should get the released record at offset 0. Accept the record after fetching.
            validateExpectedRecordsInEachPollAndAcknowledge(shareConsumer2, 0, 1, 1, AcknowledgeType.ACCEPT);
            // Now fetch the last record at offset 1 from consumer2 in its final delivery attempt.
            validateExpectedRecordsInEachPollAndAcknowledge(shareConsumer2, 1, 2, 1, AcknowledgeType.ACCEPT);

            // Next poll from consumer1 should not have any records as all records have reached delivery limit.
            records = shareConsumer1.poll(Duration.ofMillis(2500L));
            assertTrue(records.isEmpty(), "Records should be empty as all records have reached delivery limit. But received: " + records.count());
        }
    }

    @ClusterTest
    public void testDynamicDeliveryCountLimitDecreaseArchivesRecords() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                 "group1",
                 Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))
        ) {
            // Produce 1 record.
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(),
                null, "key".getBytes(), "value".getBytes());
            producer.send(record);
            producer.flush();

            shareConsumer.subscribe(Set.of(tp.topic()));

            // Consume and release twice (deliveryCount becomes 2).
            // Delivery 1: acquire (deliveryCount=1) → release (1 < default limit 5 → AVAILABLE).
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 1);
            assertEquals(1, records.count());
            records.forEach(r -> shareConsumer.acknowledge(r, AcknowledgeType.RELEASE));
            shareConsumer.commitSync();

            // Delivery 2: acquire (deliveryCount=2) → release (2 < 5 → AVAILABLE).
            records = waitedPoll(shareConsumer, 2500L, 1);
            assertEquals(1, records.count());
            assertEquals((short) 2, records.records(tp).get(0).deliveryCount().get());
            records.forEach(r -> shareConsumer.acknowledge(r, AcknowledgeType.RELEASE));
            shareConsumer.commitSync();

            // Dynamically decrease delivery count limit to 3 via group config.
            alterShareDeliveryCountLimit("group1", "3");

            // Delivery 3: acquire (deliveryCount=3) → release (3 >= 3 → ARCHIVED).
            records = waitedPoll(shareConsumer, 2500L, 1);
            assertEquals(1, records.count());
            assertEquals((short) 3, records.records(tp).get(0).deliveryCount().get());
            records.forEach(r -> shareConsumer.acknowledge(r, AcknowledgeType.RELEASE));
            shareConsumer.commitSync();

            // Next poll should have no records because the record was archived.
            records = shareConsumer.poll(Duration.ofMillis(2500L));
            assertTrue(records.isEmpty(),
                "Records should be empty as the record was archived. But received: " + records.count());
        }
    }

    @ClusterTest(
        serverProperties = {
            @ClusterConfigProperty(key = "group.share.delivery.count.limit", value = "3"),
        }
    )
    public void testDynamicDeliveryCountLimitIncreaseAllowsMoreDeliveries() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                 "group1",
                 Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))
        ) {
            // Produce 1 record.
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(),
                null, "key".getBytes(), "value".getBytes());
            producer.send(record);
            producer.flush();

            shareConsumer.subscribe(Set.of(tp.topic()));

            // Consume and release twice (deliveryCount becomes 2).
            // Delivery 1: acquire (deliveryCount=1) → release (1 < broker limit 3 → AVAILABLE).
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 1);
            assertEquals(1, records.count());
            records.forEach(r -> shareConsumer.acknowledge(r, AcknowledgeType.RELEASE));
            shareConsumer.commitSync();

            // Delivery 2: acquire (deliveryCount=2) → release (2 < 3 → AVAILABLE).
            records = waitedPoll(shareConsumer, 2500L, 1);
            assertEquals(1, records.count());
            assertEquals((short) 2, records.records(tp).get(0).deliveryCount().get());
            records.forEach(r -> shareConsumer.acknowledge(r, AcknowledgeType.RELEASE));
            shareConsumer.commitSync();

            // Dynamically increase delivery count limit to 5 via group config.
            alterShareDeliveryCountLimit("group1", "5");

            // Delivery 3: acquire (deliveryCount=3) → release (3 < 5 → AVAILABLE).
            // Without the config increase, 3 >= 3 would have caused archival.
            records = waitedPoll(shareConsumer, 2500L, 1);
            assertEquals(1, records.count());
            assertEquals((short) 3, records.records(tp).get(0).deliveryCount().get());
            records.forEach(r -> shareConsumer.acknowledge(r, AcknowledgeType.RELEASE));
            shareConsumer.commitSync();

            // Delivery 4: acquire (deliveryCount=4) → release (4 < 5 → AVAILABLE).
            records = waitedPoll(shareConsumer, 2500L, 1);
            assertEquals(1, records.count());
            assertEquals((short) 4, records.records(tp).get(0).deliveryCount().get());
            records.forEach(r -> shareConsumer.acknowledge(r, AcknowledgeType.RELEASE));
            shareConsumer.commitSync();

            // Delivery 5: acquire (deliveryCount=5) → release (5 >= 5 → ARCHIVED).
            records = waitedPoll(shareConsumer, 2500L, 1);
            assertEquals(1, records.count());
            assertEquals((short) 5, records.records(tp).get(0).deliveryCount().get());
            records.forEach(r -> shareConsumer.acknowledge(r, AcknowledgeType.RELEASE));
            shareConsumer.commitSync();

            // Next poll should have no records because the record was archived.
            records = shareConsumer.poll(Duration.ofMillis(2500L));
            assertTrue(records.isEmpty(),
                "Records should be empty as the record was archived. But received: " + records.count());
        }
    }

    @ClusterTest
    public void testDynamicPartitionMaxRecordLocks() {
        // Verify that the group-level share.partition.max.record.locks config can be
        // dynamically set and read back via describe configs.
        alterSharePartitionMaxRecordLocks("group1", "500");

        // Verify the config can be updated dynamically.
        alterSharePartitionMaxRecordLocks("group1", "1000");
    }
}