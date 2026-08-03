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

import kafka.server.KafkaBroker;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.group.GroupConfig;
import org.apache.kafka.coordinator.share.ShareCoordinatorRecordSerde;
import org.apache.kafka.coordinator.share.generated.ShareSnapshotKey;
import org.apache.kafka.coordinator.share.generated.ShareSnapshotValue;
import org.apache.kafka.coordinator.share.generated.ShareUpdateKey;
import org.apache.kafka.coordinator.share.generated.ShareUpdateValue;
import org.apache.kafka.server.share.SharePartitionKey;
import org.apache.kafka.server.share.fetch.RecordState;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Timeout;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;
import static org.apache.kafka.test.TestUtils.DEFAULT_POLL_INTERVAL_MS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

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
public class ShareConsumerTransactionTest extends ShareConsumerTestBase {

    public ShareConsumerTransactionTest(ClusterInstance cluster) {
        super(cluster);
    }

    @ClusterTest
    public void testTransactionalShareAckCommitAccept() throws Exception {
        String groupId = "txn-share-commit-accept";
        alterShareAutoOffsetReset(groupId, "earliest");

        try (Producer<byte[], byte[]> producer = createProducer();
             Producer<byte[], byte[]> transactionalProducer = createTransactionalProducer("txn-share-commit-accept-producer");
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                 groupId,
                 Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT));
             Admin admin = createAdminClient()) {

            producer.send(record("first")).get();
            producer.flush();

            shareConsumer.subscribe(Set.of(tp.topic()));
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 1);
            ConsumerRecord<byte[], byte[]> record = records.iterator().next();
            assertEquals(0L, record.offset());
            assertEquals("first", new String(record.value(), StandardCharsets.UTF_8));

            ShareGroupMetadata groupMetadata = shareConsumer.shareGroupMetadata();
            shareConsumer.acknowledge(record, AcknowledgeType.ACCEPT);
            ShareAcknowledgements acknowledgements = shareConsumer.acknowledgementsForTransaction();
            assertFalse(acknowledgements.isEmpty());
            TopicIdPartition acknowledgedPartition = acknowledgements.acknowledgements().keySet().iterator().next();
            assertEquals(tp, acknowledgedPartition.topicPartition());

            transactionalProducer.initTransactions();
            transactionalProducer.partitionsFor(tp.topic());
            transactionalProducer.beginTransaction();
            transactionalProducer.sendShareAcknowledgementsToTransaction(acknowledgements, groupMetadata);
            transactionalProducer.commitTransaction();

            verifySharePartitionLag(admin, groupId, tp, 0L);
            assertEquals(0, shareConsumer.poll(Duration.ofMillis(500)).count());
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testTransactionalShareAckCommitWithOutputRecordInSameTransaction() throws Exception {
        String groupId = "txn-share-commit-with-output";
        String inputTopic = "txn-share-commit-with-output-input";
        String outputTopic = "txn-share-commit-with-output-output";
        TopicPartition inputTopicPartition = new TopicPartition(inputTopic, 0);
        TopicPartition outputTopicPartition = new TopicPartition(outputTopic, 0);
        String outputValue = "derived-first";
        createTopic(inputTopic);
        createTopic(outputTopic);
        alterShareAutoOffsetReset(groupId, "earliest");

        try (Producer<byte[], byte[]> producer = createProducer();
             Producer<byte[], byte[]> transactionalProducer = createTransactionalProducer("txn-share-commit-with-output-producer");
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                 groupId,
                 Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT));
             Admin admin = createAdminClient()) {

            producer.send(record(inputTopicPartition, "first")).get();
            producer.flush();

            shareConsumer.subscribe(Set.of(inputTopic));
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 1);
            ConsumerRecord<byte[], byte[]> record = records.iterator().next();
            assertEquals(0L, record.offset());
            assertEquals("first", new String(record.value(), StandardCharsets.UTF_8));

            ShareGroupMetadata groupMetadata = shareConsumer.shareGroupMetadata();
            shareConsumer.acknowledge(record, AcknowledgeType.ACCEPT);
            ShareAcknowledgements acknowledgements = shareConsumer.acknowledgementsForTransaction();
            assertFalse(acknowledgements.isEmpty());
            TopicIdPartition acknowledgedPartition = acknowledgements.acknowledgements().keySet().iterator().next();
            assertEquals(inputTopicPartition, acknowledgedPartition.topicPartition());

            transactionalProducer.initTransactions();
            transactionalProducer.partitionsFor(inputTopic);
            transactionalProducer.partitionsFor(outputTopic);
            transactionalProducer.beginTransaction();
            transactionalProducer.send(record(outputTopicPartition, outputValue)).get();
            transactionalProducer.sendShareAcknowledgementsToTransaction(acknowledgements, groupMetadata);
            transactionalProducer.commitTransaction();

            List<ConsumerRecord<byte[], byte[]>> outputRecords = readCommittedRecords(outputTopicPartition, 1);
            ConsumerRecord<byte[], byte[]> outputRecord = outputRecords.get(0);
            assertEquals(0L, outputRecord.offset());
            assertEquals(outputValue, new String(outputRecord.value(), StandardCharsets.UTF_8));
            verifySharePartitionLag(admin, groupId, inputTopicPartition, 0L);
            assertEquals(0, shareConsumer.poll(Duration.ofMillis(500)).count());
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testTransactionalShareAckRejectsFencedMemberEpochs() throws Exception {
        assertTransactionalShareAckRejectedForInvalidMemberEpoch("future-member-epoch", 1);
        assertTransactionalShareAckRejectedForInvalidMemberEpoch("stale-member-epoch", -2);
    }

    private void assertTransactionalShareAckRejectedForInvalidMemberEpoch(
        String scenario,
        int memberEpochDelta
    ) throws Exception {
        String groupId = "txn-share-" + scenario;
        String topic = "txn-share-" + scenario;
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        createTopic(topic);
        alterShareAutoOffsetReset(groupId, "earliest");

        try (Producer<byte[], byte[]> producer = createProducer();
             Producer<byte[], byte[]> transactionalProducer = createTransactionalProducer(groupId + "-producer");
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                 groupId,
                 Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT));
             Admin admin = createAdminClient()) {

            producer.send(record(topicPartition, scenario)).get();
            producer.flush();

            shareConsumer.subscribe(Set.of(topic));
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 1);
            ConsumerRecord<byte[], byte[]> record = records.iterator().next();
            assertEquals(0L, record.offset());
            assertEquals(scenario, new String(record.value(), StandardCharsets.UTF_8));

            ShareGroupMetadata groupMetadata = shareConsumer.shareGroupMetadata();
            ShareGroupMetadata fencedGroupMetadata = new ShareGroupMetadata(
                groupMetadata.groupId(),
                groupMetadata.memberId(),
                invalidMemberEpoch(groupMetadata.memberEpoch(), memberEpochDelta)
            );
            shareConsumer.acknowledge(record, AcknowledgeType.ACCEPT);
            ShareAcknowledgements acknowledgements = shareConsumer.acknowledgementsForTransaction();
            assertFalse(acknowledgements.isEmpty());

            transactionalProducer.initTransactions();
            transactionalProducer.partitionsFor(topic);
            transactionalProducer.beginTransaction();
            try {
                assertThrows(CommitFailedException.class,
                    () -> transactionalProducer.sendShareAcknowledgementsToTransaction(acknowledgements, fencedGroupMetadata));
            } finally {
                transactionalProducer.abortTransaction();
            }

            verifySharePartitionLag(admin, groupId, topicPartition, 1L);
        }
    }

    private int invalidMemberEpoch(int memberEpoch, int memberEpochDelta) {
        int invalidMemberEpoch = memberEpoch + memberEpochDelta;
        return invalidMemberEpoch == 0 ? -1 : invalidMemberEpoch;
    }

    @ClusterTest
    public void testTransactionalShareAckCommitAcrossMultiplePolls() throws Exception {
        String groupId = "txn-share-commit-multiple-polls";
        alterShareAutoOffsetReset(groupId, "earliest");

        try (Producer<byte[], byte[]> producer = createProducer();
             Producer<byte[], byte[]> transactionalProducer = createTransactionalProducer("txn-share-commit-multiple-polls-producer");
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                 groupId,
                 Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT));
             Admin admin = createAdminClient()) {

            producer.send(record("first")).get();
            producer.flush();

            transactionalProducer.initTransactions();
            transactionalProducer.partitionsFor(tp.topic());

            shareConsumer.subscribe(Set.of(tp.topic()));
            ConsumerRecords<byte[], byte[]> firstRecords = waitedPoll(shareConsumer, 2500L, 1);
            ConsumerRecord<byte[], byte[]> firstRecord = firstRecords.iterator().next();
            assertEquals(0L, firstRecord.offset());
            assertEquals("first", new String(firstRecord.value(), StandardCharsets.UTF_8));

            ShareGroupMetadata groupMetadata = shareConsumer.shareGroupMetadata();
            shareConsumer.acknowledge(firstRecord, AcknowledgeType.ACCEPT);
            ShareAcknowledgements firstAcknowledgements = shareConsumer.acknowledgementsForTransaction();
            assertFalse(firstAcknowledgements.isEmpty());
            TopicIdPartition firstAcknowledgedPartition = firstAcknowledgements.acknowledgements().keySet().iterator().next();
            assertEquals(tp, firstAcknowledgedPartition.topicPartition());
            ShareAcknowledgementBatch firstBatch = firstAcknowledgements.acknowledgements().get(firstAcknowledgedPartition).get(0);
            assertEquals(0L, firstBatch.firstOffset());
            assertEquals(0L, firstBatch.lastOffset());
            assertEquals(List.of(AcknowledgeType.ACCEPT.id), firstBatch.acknowledgeTypes());
            transactionalProducer.beginTransaction();
            try {
                transactionalProducer.sendShareAcknowledgementsToTransaction(firstAcknowledgements, groupMetadata);
            } catch (Exception e) {
                fail("First transactional share acknowledgement failed", e);
            }

            producer.send(record("second")).get();
            producer.flush();

            ConsumerRecords<byte[], byte[]> secondRecords = waitedPoll(shareConsumer, 2500L, 1);
            ConsumerRecord<byte[], byte[]> secondRecord = secondRecords.iterator().next();
            assertEquals(1L, secondRecord.offset());
            assertEquals("second", new String(secondRecord.value(), StandardCharsets.UTF_8));

            shareConsumer.acknowledge(secondRecord, AcknowledgeType.ACCEPT);
            ShareAcknowledgements secondAcknowledgements = shareConsumer.acknowledgementsForTransaction();
            assertFalse(secondAcknowledgements.isEmpty());
            TopicIdPartition secondAcknowledgedPartition = secondAcknowledgements.acknowledgements().keySet().iterator().next();
            assertEquals(tp, secondAcknowledgedPartition.topicPartition());
            ShareAcknowledgementBatch secondBatch = secondAcknowledgements.acknowledgements().get(secondAcknowledgedPartition).get(0);
            assertEquals(1L, secondBatch.firstOffset());
            assertEquals(1L, secondBatch.lastOffset());
            assertEquals(List.of(AcknowledgeType.ACCEPT.id), secondBatch.acknowledgeTypes());
            try {
                transactionalProducer.sendShareAcknowledgementsToTransaction(secondAcknowledgements, groupMetadata);
            } catch (Exception e) {
                fail("Second transactional share acknowledgement failed", e);
            }

            transactionalProducer.commitTransaction();

            verifySharePartitionLag(admin, groupId, tp, 0L);
            assertEquals(0, shareConsumer.poll(Duration.ofMillis(500)).count());
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testTransactionalShareAckAbortAccept() throws Exception {
        String groupId = "txn-share-abort-accept";
        alterShareAutoOffsetReset(groupId, "earliest");

        try (Producer<byte[], byte[]> producer = createProducer();
             Producer<byte[], byte[]> transactionalProducer = createTransactionalProducer("txn-share-abort-accept-producer");
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                 groupId,
                 Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT));
             Admin admin = createAdminClient()) {

            producer.send(record("first")).get();
            producer.flush();

            shareConsumer.subscribe(Set.of(tp.topic()));
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 1);
            ConsumerRecord<byte[], byte[]> record = records.iterator().next();
            assertEquals(0L, record.offset());
            assertEquals("first", new String(record.value(), StandardCharsets.UTF_8));

            ShareGroupMetadata groupMetadata = shareConsumer.shareGroupMetadata();
            shareConsumer.acknowledge(record, AcknowledgeType.ACCEPT);
            ShareAcknowledgements acknowledgements = shareConsumer.acknowledgementsForTransaction();
            assertFalse(acknowledgements.isEmpty());
            TopicIdPartition acknowledgedPartition = acknowledgements.acknowledgements().keySet().iterator().next();
            assertEquals(tp, acknowledgedPartition.topicPartition());

            transactionalProducer.initTransactions();
            transactionalProducer.partitionsFor(tp.topic());
            transactionalProducer.beginTransaction();
            transactionalProducer.sendShareAcknowledgementsToTransaction(acknowledgements, groupMetadata);
            transactionalProducer.abortTransaction();

            verifySharePartitionLag(admin, groupId, tp, 1L);
            ConsumerRecords<byte[], byte[]> redeliveredRecords = waitedPoll(shareConsumer, 2500L, 1);
            ConsumerRecord<byte[], byte[]> redeliveredRecord = redeliveredRecords.iterator().next();
            assertEquals(0L, redeliveredRecord.offset());
            assertEquals("first", new String(redeliveredRecord.value(), StandardCharsets.UTF_8));
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testTransactionalShareAckAbortWithOutputRecordInSameTransaction() throws Exception {
        String groupId = "txn-share-abort-with-output";
        String inputTopic = "txn-share-abort-with-output-input";
        String outputTopic = "txn-share-abort-with-output-output";
        TopicPartition inputTopicPartition = new TopicPartition(inputTopic, 0);
        TopicPartition outputTopicPartition = new TopicPartition(outputTopic, 0);
        String outputValue = "derived-first";
        createTopic(inputTopic);
        createTopic(outputTopic);
        alterShareAutoOffsetReset(groupId, "earliest");

        try (Producer<byte[], byte[]> producer = createProducer();
             Producer<byte[], byte[]> transactionalProducer = createTransactionalProducer("txn-share-abort-with-output-producer");
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                 groupId,
                 Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT));
             Admin admin = createAdminClient()) {

            producer.send(record(inputTopicPartition, "first")).get();
            producer.flush();

            shareConsumer.subscribe(Set.of(inputTopic));
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 1);
            ConsumerRecord<byte[], byte[]> record = records.iterator().next();
            assertEquals(0L, record.offset());
            assertEquals("first", new String(record.value(), StandardCharsets.UTF_8));

            ShareGroupMetadata groupMetadata = shareConsumer.shareGroupMetadata();
            shareConsumer.acknowledge(record, AcknowledgeType.ACCEPT);
            ShareAcknowledgements acknowledgements = shareConsumer.acknowledgementsForTransaction();
            assertFalse(acknowledgements.isEmpty());
            TopicIdPartition acknowledgedPartition = acknowledgements.acknowledgements().keySet().iterator().next();
            assertEquals(inputTopicPartition, acknowledgedPartition.topicPartition());

            transactionalProducer.initTransactions();
            transactionalProducer.partitionsFor(inputTopic);
            transactionalProducer.partitionsFor(outputTopic);
            transactionalProducer.beginTransaction();
            transactionalProducer.send(record(outputTopicPartition, outputValue)).get();
            transactionalProducer.sendShareAcknowledgementsToTransaction(acknowledgements, groupMetadata);
            transactionalProducer.abortTransaction();

            readCommittedRecords(outputTopicPartition, 0);
            verifySharePartitionLag(admin, groupId, inputTopicPartition, 1L);
            ConsumerRecords<byte[], byte[]> redeliveredRecords = waitedPoll(shareConsumer, 2500L, 1);
            ConsumerRecord<byte[], byte[]> redeliveredRecord = redeliveredRecords.iterator().next();
            assertEquals(0L, redeliveredRecord.offset());
            assertEquals("first", new String(redeliveredRecord.value(), StandardCharsets.UTF_8));
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testTransactionalShareAckCommitReject() throws Exception {
        String groupId = "txn-share-commit-reject";
        alterShareAutoOffsetReset(groupId, "earliest");

        try (Producer<byte[], byte[]> producer = createProducer();
             Producer<byte[], byte[]> transactionalProducer = createTransactionalProducer("txn-share-commit-reject-producer");
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                 groupId,
                 Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT));
             Admin admin = createAdminClient()) {

            producer.send(record("rejected")).get();
            producer.flush();

            shareConsumer.subscribe(Set.of(tp.topic()));
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 1);
            ConsumerRecord<byte[], byte[]> record = records.iterator().next();
            assertEquals(0L, record.offset());
            assertEquals("rejected", new String(record.value(), StandardCharsets.UTF_8));

            ShareGroupMetadata groupMetadata = shareConsumer.shareGroupMetadata();
            shareConsumer.acknowledge(record, AcknowledgeType.REJECT);
            ShareAcknowledgements acknowledgements = shareConsumer.acknowledgementsForTransaction();
            assertFalse(acknowledgements.isEmpty());
            TopicIdPartition acknowledgedPartition = acknowledgements.acknowledgements().keySet().iterator().next();
            assertEquals(tp, acknowledgedPartition.topicPartition());

            transactionalProducer.initTransactions();
            transactionalProducer.partitionsFor(tp.topic());
            transactionalProducer.beginTransaction();
            transactionalProducer.sendShareAcknowledgementsToTransaction(acknowledgements, groupMetadata);
            transactionalProducer.commitTransaction();

            verifySharePartitionLag(admin, groupId, tp, 0L);
            assertEquals(0, shareConsumer.poll(Duration.ofMillis(500)).count());
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    @Timeout(120)
    public void testTransactionalShareAckCommitRejectWithDlqEnabled() throws Exception {
        String groupId = "txn-share-commit-reject-dlq";
        String dlqTopic = "dlq." + groupId;
        createDlqTopic(dlqTopic);
        alterShareAutoOffsetReset(groupId, "earliest");
        alterShareGroupConfig(groupId, GroupConfig.ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG, dlqTopic);

        try (Producer<byte[], byte[]> producer = createProducer();
             Producer<byte[], byte[]> transactionalProducer = createTransactionalProducer("txn-share-commit-reject-dlq-producer");
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                 groupId,
                 Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT));
             Admin admin = createAdminClient()) {

            producer.send(record("dlq-rejected")).get();
            producer.flush();

            shareConsumer.subscribe(Set.of(tp.topic()));
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 1);
            ConsumerRecord<byte[], byte[]> record = records.iterator().next();
            assertEquals(0L, record.offset());
            assertEquals("dlq-rejected", new String(record.value(), StandardCharsets.UTF_8));

            ShareGroupMetadata groupMetadata = shareConsumer.shareGroupMetadata();
            shareConsumer.acknowledge(record, AcknowledgeType.REJECT);
            ShareAcknowledgements acknowledgements = shareConsumer.acknowledgementsForTransaction();
            assertFalse(acknowledgements.isEmpty());
            TopicIdPartition acknowledgedPartition = acknowledgements.acknowledgements().keySet().iterator().next();
            assertEquals(tp, acknowledgedPartition.topicPartition());

            transactionalProducer.initTransactions();
            transactionalProducer.partitionsFor(tp.topic());
            transactionalProducer.beginTransaction();
            transactionalProducer.sendShareAcknowledgementsToTransaction(acknowledgements, groupMetadata);
            verifyLatestShareStateDeliveryState(groupId, acknowledgedPartition, 0L, RecordState.TX_PENDING);
            transactionalProducer.commitTransaction();

            assertEquals(0, shareConsumer.poll(Duration.ofMillis(1000)).count());
            verifySharePartitionLag(admin, groupId, tp, 0L);
            waitForDlqRecords(dlqTopic, 1);
            verifyLatestShareStateDeliveryState(groupId, acknowledgedPartition, 0L, RecordState.ARCHIVED);
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testTransactionalShareAckCommitSubsetAccept() throws Exception {
        String groupId = "txn-share-commit-subset-accept";
        alterShareAutoOffsetReset(groupId, "earliest");

        try (Producer<byte[], byte[]> producer = createProducer();
             Producer<byte[], byte[]> transactionalProducer = createTransactionalProducer("txn-share-commit-subset-accept-producer");
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                 groupId,
                 Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT));
             Admin admin = createAdminClient()) {

            producer.send(record("zero")).get();
            producer.send(record("one")).get();
            producer.send(record("two")).get();
            producer.flush();

            shareConsumer.subscribe(Set.of(tp.topic()));
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 3);
            ConsumerRecord<byte[], byte[]> targetRecord = null;
            for (ConsumerRecord<byte[], byte[]> record : records) {
                if (record.offset() == 1L) {
                    targetRecord = record;
                    break;
                }
            }
            assertNotNull(targetRecord);
            assertEquals("one", new String(targetRecord.value(), StandardCharsets.UTF_8));

            ShareGroupMetadata groupMetadata = shareConsumer.shareGroupMetadata();
            TopicIdPartition acknowledgedPartition = new TopicIdPartition(tpId, tp);
            ShareAcknowledgements acknowledgements = new ShareAcknowledgements(Map.of(
                acknowledgedPartition,
                List.of(new ShareAcknowledgementBatch(1L, 1L, List.of(AcknowledgeType.ACCEPT.id)))
            ));
            ShareAcknowledgementBatch batch = acknowledgements.acknowledgements().get(acknowledgedPartition).get(0);
            assertEquals(1L, batch.firstOffset());
            assertEquals(1L, batch.lastOffset());

            transactionalProducer.initTransactions();
            transactionalProducer.partitionsFor(tp.topic());
            transactionalProducer.beginTransaction();
            transactionalProducer.sendShareAcknowledgementsToTransaction(acknowledgements, groupMetadata);
            transactionalProducer.commitTransaction();

            verifySharePartitionLag(admin, groupId, tp, 2L);
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    @Timeout(120)
    public void testTransactionalShareAckCommitAcrossMultipleShareStatePartitions() throws Exception {
        String groupId = "txn-share-commit-multi-share-state";
        alterShareAutoOffsetReset(groupId, "earliest");

        try (Admin admin = createAdminClient()) {
            List<TopicIdPartition> topicIdPartitions = createTopicIdPartitionsMappedToAllShareStatePartitions(
                groupId,
                "txn-share-multi-state-topic",
                12,
                1
            );
            String topicName = topicIdPartitions.get(0).topic();
            List<TopicPartition> topicPartitions = topicIdPartitions.stream()
                .map(TopicIdPartition::topicPartition)
                .toList();

            try (Producer<byte[], byte[]> producer = createProducer();
                 Producer<byte[], byte[]> transactionalProducer = createTransactionalProducer("txn-share-commit-multi-share-state-producer");
                 ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                     groupId,
                     Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))) {

                shareConsumer.subscribe(Set.of(topicName));
                waitedPoll(shareConsumer, 2500L, 0, true, groupId, topicPartitions);

                for (TopicIdPartition topicIdPartition : topicIdPartitions) {
                    producer.send(record(topicIdPartition.topicPartition(), "partition-" + topicIdPartition.partition())).get();
                }
                producer.flush();

                Map<TopicPartition, ConsumerRecord<byte[], byte[]>> recordsByPartition = new HashMap<>();
                Map<TopicIdPartition, List<ShareAcknowledgementBatch>> acknowledgementsByPartition = new HashMap<>();
                TestUtils.waitForCondition(
                    () -> {
                        ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(500));
                        records.forEach(record -> {
                            recordsByPartition.put(new TopicPartition(record.topic(), record.partition()), record);
                            shareConsumer.acknowledge(record, AcknowledgeType.ACCEPT);
                        });
                        if (records.count() > 0) {
                            acknowledgementsByPartition.putAll(shareConsumer.acknowledgementsForTransaction().acknowledgements());
                        }
                        return recordsByPartition.keySet().containsAll(topicPartitions);
                    },
                    DEFAULT_MAX_WAIT_MS,
                    DEFAULT_POLL_INTERVAL_MS,
                    () -> "Timed out waiting for records from all partitions for " + topicName
                );

                ShareGroupMetadata groupMetadata = shareConsumer.shareGroupMetadata();
                for (TopicIdPartition topicIdPartition : topicIdPartitions) {
                    ConsumerRecord<byte[], byte[]> record = recordsByPartition.get(topicIdPartition.topicPartition());
                    assertNotNull(record);
                    assertEquals(0L, record.offset());
                    assertEquals(
                        "partition-" + topicIdPartition.partition(),
                        new String(record.value(), StandardCharsets.UTF_8)
                    );
                }

                ShareAcknowledgements acknowledgements = new ShareAcknowledgements(acknowledgementsByPartition);
                assertEquals(topicIdPartitions.size(), acknowledgements.acknowledgements().size());
                assertEquals(sgsTopicPartitions.size(), mappedShareStatePartitions(groupId, acknowledgements).size());

                transactionalProducer.initTransactions();
                transactionalProducer.partitionsFor(topicName);
                transactionalProducer.beginTransaction();
                transactionalProducer.sendShareAcknowledgementsToTransaction(acknowledgements, groupMetadata);
                transactionalProducer.commitTransaction();

                for (TopicIdPartition topicIdPartition : topicIdPartitions) {
                    verifySharePartitionLag(admin, groupId, topicIdPartition.topicPartition(), 0L);
                }
                assertEquals(0, shareConsumer.poll(Duration.ofMillis(500)).count());
                verifyShareGroupStateTopicRecordsProduced();
            }
        }
    }

    @ClusterTest(
        brokers = 3,
        serverProperties = {
            @ClusterConfigProperty(key = "auto.create.topics.enable", value = "false"),
            @ClusterConfigProperty(key = "group.share.max.partition.max.record.locks", value = "10000"),
            @ClusterConfigProperty(key = "group.share.partition.max.record.locks", value = "10000"),
            @ClusterConfigProperty(key = "group.share.record.lock.duration.ms", value = "15000"),
            @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "3"),
            @ClusterConfigProperty(key = "share.coordinator.state.topic.min.isr", value = "1"),
            @ClusterConfigProperty(key = "share.coordinator.state.topic.num.partitions", value = "3"),
            @ClusterConfigProperty(key = "share.coordinator.state.topic.replication.factor", value = "3"),
            @ClusterConfigProperty(key = "transaction.state.log.min.isr", value = "1"),
            @ClusterConfigProperty(key = "transaction.state.log.replication.factor", value = "3")
        }
    )
    @Timeout(120)
    public void testTransactionalShareAckCommitAcceptWithRemoteShareCoordinator() throws Exception {
        String groupId = "txn-share-remote-commit-accept";
        alterShareAutoOffsetReset(groupId, "earliest");

        try (Admin admin = createAdminClient()) {
            TopicIdPartition topicIdPartition = createTopicIdPartitionWithRemoteShareCoordinator(admin, groupId, "txn-share-remote-commit-topic", 0);
            TopicPartition topicPartition = topicIdPartition.topicPartition();

            try (Producer<byte[], byte[]> producer = createProducer();
                 Producer<byte[], byte[]> transactionalProducer = createRemoteTransactionalProducer("txn-share-remote-commit-accept-producer");
                 ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                     groupId,
                     Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))) {

                producer.send(record(topicPartition, "remote-commit")).get();
                producer.flush();

                shareConsumer.subscribe(Set.of(topicPartition.topic()));
                ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 1);
                ConsumerRecord<byte[], byte[]> record = records.iterator().next();
                assertEquals(0L, record.offset());
                assertEquals("remote-commit", new String(record.value(), StandardCharsets.UTF_8));

                ShareGroupMetadata groupMetadata = shareConsumer.shareGroupMetadata();
                shareConsumer.acknowledge(record, AcknowledgeType.ACCEPT);
                ShareAcknowledgements acknowledgements = shareConsumer.acknowledgementsForTransaction();
                assertFalse(acknowledgements.isEmpty());

                transactionalProducer.initTransactions();
                transactionalProducer.partitionsFor(topicPartition.topic());
                transactionalProducer.beginTransaction();
                transactionalProducer.sendShareAcknowledgementsToTransaction(acknowledgements, groupMetadata);
                verifyLatestShareStateDeliveryState(groupId, topicIdPartition, 0L, RecordState.TX_PENDING);
                assertEquals(0, shareConsumer.poll(Duration.ofMillis(1000)).count());
                transactionalProducer.commitTransaction();

                verifyLatestShareStateDeliveryState(groupId, topicIdPartition, 0L, RecordState.ACKNOWLEDGED);
                verifySharePartitionLag(admin, groupId, topicPartition, 0L);
                assertEquals(0, shareConsumer.poll(Duration.ofMillis(500)).count());
                verifyShareGroupStateTopicRecordsProduced();
            }
        }
    }

    @ClusterTest(
        brokers = 3,
        serverProperties = {
            @ClusterConfigProperty(key = "auto.create.topics.enable", value = "false"),
            @ClusterConfigProperty(key = "group.share.max.partition.max.record.locks", value = "10000"),
            @ClusterConfigProperty(key = "group.share.partition.max.record.locks", value = "10000"),
            @ClusterConfigProperty(key = "group.share.record.lock.duration.ms", value = "15000"),
            @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "3"),
            @ClusterConfigProperty(key = "share.coordinator.state.topic.min.isr", value = "1"),
            @ClusterConfigProperty(key = "share.coordinator.state.topic.num.partitions", value = "3"),
            @ClusterConfigProperty(key = "share.coordinator.state.topic.replication.factor", value = "3"),
            @ClusterConfigProperty(key = "transaction.state.log.min.isr", value = "1"),
            @ClusterConfigProperty(key = "transaction.state.log.replication.factor", value = "3")
        }
    )
    @Timeout(120)
    public void testTransactionalShareAckAbortAcceptWithRemoteShareCoordinator() throws Exception {
        String groupId = "txn-share-remote-abort-accept";
        alterShareAutoOffsetReset(groupId, "earliest");

        try (Admin admin = createAdminClient()) {
            TopicIdPartition topicIdPartition = createTopicIdPartitionWithRemoteShareCoordinator(admin, groupId, "txn-share-remote-abort-topic", 0);
            TopicPartition topicPartition = topicIdPartition.topicPartition();

            try (Producer<byte[], byte[]> producer = createProducer();
                 Producer<byte[], byte[]> transactionalProducer = createRemoteTransactionalProducer("txn-share-remote-abort-accept-producer");
                 ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                     groupId,
                     Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))) {

                producer.send(record(topicPartition, "remote-abort")).get();
                producer.flush();

                shareConsumer.subscribe(Set.of(topicPartition.topic()));
                ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 1);
                ConsumerRecord<byte[], byte[]> record = records.iterator().next();
                assertEquals(0L, record.offset());
                assertEquals("remote-abort", new String(record.value(), StandardCharsets.UTF_8));

                ShareGroupMetadata groupMetadata = shareConsumer.shareGroupMetadata();
                shareConsumer.acknowledge(record, AcknowledgeType.ACCEPT);
                ShareAcknowledgements acknowledgements = shareConsumer.acknowledgementsForTransaction();
                assertFalse(acknowledgements.isEmpty());

                transactionalProducer.initTransactions();
                transactionalProducer.partitionsFor(topicPartition.topic());
                transactionalProducer.beginTransaction();
                transactionalProducer.sendShareAcknowledgementsToTransaction(acknowledgements, groupMetadata);
                verifyLatestShareStateDeliveryState(groupId, topicIdPartition, 0L, RecordState.TX_PENDING);
                assertEquals(0, shareConsumer.poll(Duration.ofMillis(1000)).count());
                transactionalProducer.abortTransaction();

                verifyLatestShareStateDeliveryState(groupId, topicIdPartition, 0L, RecordState.AVAILABLE);
                verifySharePartitionLag(admin, groupId, topicPartition, 1L);
                ConsumerRecords<byte[], byte[]> redeliveredRecords = waitedPoll(shareConsumer, 2500L, 1);
                ConsumerRecord<byte[], byte[]> redeliveredRecord = redeliveredRecords.iterator().next();
                assertEquals(0L, redeliveredRecord.offset());
                assertEquals("remote-abort", new String(redeliveredRecord.value(), StandardCharsets.UTF_8));
                verifyShareGroupStateTopicRecordsProduced();
            }
        }
    }

    @ClusterTest(
        brokers = 3,
        serverProperties = {
            @ClusterConfigProperty(key = "auto.create.topics.enable", value = "false"),
            @ClusterConfigProperty(key = "group.share.max.partition.max.record.locks", value = "10000"),
            @ClusterConfigProperty(key = "group.share.partition.max.record.locks", value = "10000"),
            @ClusterConfigProperty(key = "group.share.record.lock.duration.ms", value = "15000"),
            @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "3"),
            @ClusterConfigProperty(key = "share.coordinator.state.topic.min.isr", value = "1"),
            @ClusterConfigProperty(key = "share.coordinator.state.topic.num.partitions", value = "3"),
            @ClusterConfigProperty(key = "share.coordinator.state.topic.replication.factor", value = "3"),
            @ClusterConfigProperty(key = "transaction.state.log.min.isr", value = "1"),
            @ClusterConfigProperty(key = "transaction.state.log.replication.factor", value = "3")
        }
    )
    @Timeout(180)
    public void testTransactionalShareAckCommitAfterSourceLeaderFailoverWithPendingState() throws Exception {
        String groupId = "txn-share-recovery-commit";
        alterShareAutoOffsetReset(groupId, "earliest");

        try (Admin admin = createAdminClient()) {
            TopicIdPartition topicIdPartition = createTopicIdPartitionWithRemoteShareCoordinator(admin, groupId, "txn-share-recovery-commit-topic", 0);
            TopicPartition topicPartition = topicIdPartition.topicPartition();

            try (Producer<byte[], byte[]> producer = createProducer();
                 Producer<byte[], byte[]> transactionalProducer = createRemoteTransactionalProducer("txn-share-recovery-commit-producer")) {
                producer.send(record(topicPartition, "pending-commit")).get();
                producer.flush();

                transactionalProducer.initTransactions();
                transactionalProducer.partitionsFor(topicPartition.topic());

                try (ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                    groupId,
                    Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))) {
                    shareConsumer.subscribe(Set.of(topicPartition.topic()));
                    ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 1);
                    ConsumerRecord<byte[], byte[]> record = records.iterator().next();
                    assertEquals(0L, record.offset());
                    assertEquals("pending-commit", new String(record.value(), StandardCharsets.UTF_8));

                    ShareGroupMetadata groupMetadata = shareConsumer.shareGroupMetadata();
                    shareConsumer.acknowledge(record, AcknowledgeType.ACCEPT);
                    ShareAcknowledgements acknowledgements = shareConsumer.acknowledgementsForTransaction();
                    assertFalse(acknowledgements.isEmpty());

                    transactionalProducer.beginTransaction();
                    transactionalProducer.sendShareAcknowledgementsToTransaction(acknowledgements, groupMetadata);
                    verifyLatestShareStateDeliveryState(groupId, topicIdPartition, 0L, RecordState.TX_PENDING);
                }

                shutdownLeaderAndWaitForNewLeader(admin, topicPartition);

                try (ShareConsumer<byte[], byte[]> reloadedConsumer = createShareConsumer(
                    groupId,
                    Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))) {
                    reloadedConsumer.subscribe(Set.of(topicPartition.topic()));
                    assertEquals(0, reloadedConsumer.poll(Duration.ofMillis(1000)).count());

                    transactionalProducer.commitTransaction();

                    verifyLatestShareStateDeliveryState(groupId, topicIdPartition, 0L, RecordState.ACKNOWLEDGED);
                    verifySharePartitionLag(admin, groupId, topicPartition, 0L);
                    assertEquals(0, reloadedConsumer.poll(Duration.ofMillis(500)).count());
                    verifyShareGroupStateTopicRecordsProduced();
                }
            }
        }
    }

    @ClusterTest(
        brokers = 3,
        serverProperties = {
            @ClusterConfigProperty(key = "auto.create.topics.enable", value = "false"),
            @ClusterConfigProperty(key = "group.share.max.partition.max.record.locks", value = "10000"),
            @ClusterConfigProperty(key = "group.share.partition.max.record.locks", value = "10000"),
            @ClusterConfigProperty(key = "group.share.record.lock.duration.ms", value = "15000"),
            @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "3"),
            @ClusterConfigProperty(key = "share.coordinator.state.topic.min.isr", value = "1"),
            @ClusterConfigProperty(key = "share.coordinator.state.topic.num.partitions", value = "3"),
            @ClusterConfigProperty(key = "share.coordinator.state.topic.replication.factor", value = "3"),
            @ClusterConfigProperty(key = "transaction.state.log.min.isr", value = "1"),
            @ClusterConfigProperty(key = "transaction.state.log.replication.factor", value = "3")
        }
    )
    @Timeout(180)
    public void testTransactionalShareAckAbortAfterSourceLeaderFailoverWithPendingState() throws Exception {
        String groupId = "txn-share-recovery-abort";
        alterShareAutoOffsetReset(groupId, "earliest");

        try (Admin admin = createAdminClient()) {
            TopicIdPartition topicIdPartition = createTopicIdPartitionWithRemoteShareCoordinator(admin, groupId, "txn-share-recovery-abort-topic", 0);
            TopicPartition topicPartition = topicIdPartition.topicPartition();

            try (Producer<byte[], byte[]> producer = createProducer();
                 Producer<byte[], byte[]> transactionalProducer = createRemoteTransactionalProducer("txn-share-recovery-abort-producer")) {
                producer.send(record(topicPartition, "pending-abort")).get();
                producer.flush();

                transactionalProducer.initTransactions();
                transactionalProducer.partitionsFor(topicPartition.topic());

                try (ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                    groupId,
                    Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))) {
                    shareConsumer.subscribe(Set.of(topicPartition.topic()));
                    ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 1);
                    ConsumerRecord<byte[], byte[]> record = records.iterator().next();
                    assertEquals(0L, record.offset());
                    assertEquals("pending-abort", new String(record.value(), StandardCharsets.UTF_8));

                    ShareGroupMetadata groupMetadata = shareConsumer.shareGroupMetadata();
                    shareConsumer.acknowledge(record, AcknowledgeType.ACCEPT);
                    ShareAcknowledgements acknowledgements = shareConsumer.acknowledgementsForTransaction();
                    assertFalse(acknowledgements.isEmpty());

                    transactionalProducer.beginTransaction();
                    transactionalProducer.sendShareAcknowledgementsToTransaction(acknowledgements, groupMetadata);
                    verifyLatestShareStateDeliveryState(groupId, topicIdPartition, 0L, RecordState.TX_PENDING);
                }

                shutdownLeaderAndWaitForNewLeader(admin, topicPartition);

                try (ShareConsumer<byte[], byte[]> reloadedConsumer = createShareConsumer(
                    groupId,
                    Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))) {
                    reloadedConsumer.subscribe(Set.of(topicPartition.topic()));
                    assertEquals(0, reloadedConsumer.poll(Duration.ofMillis(1000)).count());

                    transactionalProducer.abortTransaction();

                    verifyLatestShareStateDeliveryState(groupId, topicIdPartition, 0L, RecordState.AVAILABLE);
                    verifySharePartitionLag(admin, groupId, topicPartition, 1L);
                    ConsumerRecords<byte[], byte[]> redeliveredRecords = waitedPoll(reloadedConsumer, 2500L, 1);
                    ConsumerRecord<byte[], byte[]> redeliveredRecord = redeliveredRecords.iterator().next();
                    assertEquals(0L, redeliveredRecord.offset());
                    assertEquals("pending-abort", new String(redeliveredRecord.value(), StandardCharsets.UTF_8));
                    verifyShareGroupStateTopicRecordsProduced();
                }
            }
        }
    }

    @ClusterTest(
        brokers = 3,
        serverProperties = {
            @ClusterConfigProperty(key = "auto.create.topics.enable", value = "false"),
            @ClusterConfigProperty(key = "group.share.max.partition.max.record.locks", value = "10000"),
            @ClusterConfigProperty(key = "group.share.partition.max.record.locks", value = "10000"),
            @ClusterConfigProperty(key = "group.share.record.lock.duration.ms", value = "15000"),
            @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "3"),
            @ClusterConfigProperty(key = "share.coordinator.state.topic.min.isr", value = "1"),
            @ClusterConfigProperty(key = "share.coordinator.state.topic.num.partitions", value = "3"),
            @ClusterConfigProperty(key = "share.coordinator.state.topic.replication.factor", value = "3"),
            @ClusterConfigProperty(key = "transaction.state.log.min.isr", value = "1"),
            @ClusterConfigProperty(key = "transaction.state.log.replication.factor", value = "3")
        }
    )
    @Timeout(180)
    public void testTransactionalShareAckCommitAfterShareCoordinatorFailoverWithPendingState() throws Exception {
        completeTransactionAfterShareCoordinatorFailoverWithPendingState(
            "txn-share-coordinator-recovery-commit",
            "txn-share-coordinator-recovery-commit-topic",
            "txn-share-coordinator-recovery-commit-producer",
            true
        );
    }

    @ClusterTest(
        brokers = 3,
        serverProperties = {
            @ClusterConfigProperty(key = "auto.create.topics.enable", value = "false"),
            @ClusterConfigProperty(key = "group.share.max.partition.max.record.locks", value = "10000"),
            @ClusterConfigProperty(key = "group.share.partition.max.record.locks", value = "10000"),
            @ClusterConfigProperty(key = "group.share.record.lock.duration.ms", value = "15000"),
            @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "3"),
            @ClusterConfigProperty(key = "share.coordinator.state.topic.min.isr", value = "1"),
            @ClusterConfigProperty(key = "share.coordinator.state.topic.num.partitions", value = "3"),
            @ClusterConfigProperty(key = "share.coordinator.state.topic.replication.factor", value = "3"),
            @ClusterConfigProperty(key = "transaction.state.log.min.isr", value = "1"),
            @ClusterConfigProperty(key = "transaction.state.log.replication.factor", value = "3")
        }
    )
    @Timeout(180)
    public void testTransactionalShareAckAbortAfterShareCoordinatorFailoverWithPendingState() throws Exception {
        completeTransactionAfterShareCoordinatorFailoverWithPendingState(
            "txn-share-coordinator-recovery-abort",
            "txn-share-coordinator-recovery-abort-topic",
            "txn-share-coordinator-recovery-abort-producer",
            false
        );
    }

    private Producer<byte[], byte[]> createTransactionalProducer(String transactionalId) {
        return createProducer(Map.of(
            ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId,
            ProducerConfig.MAX_BLOCK_MS_CONFIG, "15000",
            ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000",
            ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "20000"
        ));
    }

    private Producer<byte[], byte[]> createRemoteTransactionalProducer(String transactionalId) {
        return createProducer(Map.of(
            ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId,
            ProducerConfig.MAX_BLOCK_MS_CONFIG, "60000",
            ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000",
            ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000"
        ));
    }

    private void createDlqTopic(String dlqTopic) throws Exception {
        try (Admin admin = createAdminClient()) {
            admin.createTopics(Set.of(new NewTopic(dlqTopic, 1, (short) 1).configs(Map.of(
                TopicConfig.ERRORS_DEADLETTERQUEUE_GROUP_ENABLE_CONFIG, Boolean.TRUE.toString()
            )))).all().get();
        }
    }

    private void waitForDlqRecords(String dlqTopic, int expectedCount) throws InterruptedException {
        TopicPartition dlqTopicPartition = new TopicPartition(dlqTopic, 0);
        List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        try (Consumer<byte[], byte[]> consumer = cluster.consumer()) {
            consumer.assign(List.of(dlqTopicPartition));
            consumer.seekToBeginning(List.of(dlqTopicPartition));
            TestUtils.waitForCondition(
                () -> {
                    consumer.poll(Duration.ofMillis(500)).records(dlqTopicPartition).forEach(records::add);
                    return records.size() >= expectedCount;
                },
                DEFAULT_MAX_WAIT_MS,
                DEFAULT_POLL_INTERVAL_MS,
                () -> "Timed out waiting for " + expectedCount + " DLQ records from " + dlqTopic);
        }
        assertEquals(expectedCount, records.size());
    }

    private void completeTransactionAfterShareCoordinatorFailoverWithPendingState(
        String groupId,
        String topicPrefix,
        String transactionalId,
        boolean commit
    ) throws Exception {
        alterShareAutoOffsetReset(groupId, "earliest");

        try (Admin admin = createAdminClient()) {
            TopicIdPartition topicIdPartition = createTopicIdPartitionWithRemoteShareCoordinator(admin, groupId, topicPrefix, 0);
            TopicPartition topicPartition = topicIdPartition.topicPartition();
            TopicPartition shareStateTopicPartition = new TopicPartition(
                Topic.SHARE_GROUP_STATE_TOPIC_NAME,
                shareStatePartition(groupId, topicIdPartition.topicId(), topicIdPartition.partition())
            );

            try (Producer<byte[], byte[]> producer = createProducer();
                 Producer<byte[], byte[]> transactionalProducer = createRemoteTransactionalProducer(transactionalId)) {
                String value = commit ? "pending-share-coordinator-commit" : "pending-share-coordinator-abort";
                producer.send(record(topicPartition, value)).get();
                producer.flush();

                transactionalProducer.initTransactions();
                transactionalProducer.partitionsFor(topicPartition.topic());

                try (ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                    groupId,
                    Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))) {
                    shareConsumer.subscribe(Set.of(topicPartition.topic()));
                    ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 1);
                    ConsumerRecord<byte[], byte[]> record = records.iterator().next();
                    assertEquals(0L, record.offset());
                    assertEquals(value, new String(record.value(), StandardCharsets.UTF_8));

                    ShareGroupMetadata groupMetadata = shareConsumer.shareGroupMetadata();
                    shareConsumer.acknowledge(record, AcknowledgeType.ACCEPT);
                    ShareAcknowledgements acknowledgements = shareConsumer.acknowledgementsForTransaction();
                    assertFalse(acknowledgements.isEmpty());

                    transactionalProducer.beginTransaction();
                    transactionalProducer.sendShareAcknowledgementsToTransaction(acknowledgements, groupMetadata);
                    verifyLatestShareStateDeliveryState(groupId, topicIdPartition, 0L, RecordState.TX_PENDING);
                }

                shutdownLeaderAndWaitForNewLeader(admin, shareStateTopicPartition);

                try (ShareConsumer<byte[], byte[]> reloadedConsumer = createShareConsumer(
                    groupId,
                    Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))) {
                    reloadedConsumer.subscribe(Set.of(topicPartition.topic()));
                    assertEquals(0, reloadedConsumer.poll(Duration.ofMillis(1000)).count());

                    if (commit) {
                        transactionalProducer.commitTransaction();

                        verifyLatestShareStateDeliveryState(groupId, topicIdPartition, 0L, RecordState.ACKNOWLEDGED);
                        verifySharePartitionLag(admin, groupId, topicPartition, 0L);
                        assertEquals(0, reloadedConsumer.poll(Duration.ofMillis(500)).count());
                    } else {
                        transactionalProducer.abortTransaction();

                        verifyLatestShareStateDeliveryState(groupId, topicIdPartition, 0L, RecordState.AVAILABLE);
                        verifySharePartitionLag(admin, groupId, topicPartition, 1L);
                        ConsumerRecords<byte[], byte[]> redeliveredRecords = waitedPoll(reloadedConsumer, 2500L, 1);
                        ConsumerRecord<byte[], byte[]> redeliveredRecord = redeliveredRecords.iterator().next();
                        assertEquals(0L, redeliveredRecord.offset());
                        assertEquals(value, new String(redeliveredRecord.value(), StandardCharsets.UTF_8));
                    }
                    verifyShareGroupStateTopicRecordsProduced();
                }
            }
        }
    }

    private ProducerRecord<byte[], byte[]> record(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        return new ProducerRecord<>(tp.topic(), tp.partition(), null, bytes, bytes);
    }

    private ProducerRecord<byte[], byte[]> record(TopicPartition topicPartition, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        return new ProducerRecord<>(topicPartition.topic(), topicPartition.partition(), null, bytes, bytes);
    }

    private List<ConsumerRecord<byte[], byte[]>> readCommittedRecords(
        TopicPartition topicPartition,
        int expectedCount
    ) throws InterruptedException {
        List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        try (Consumer<byte[], byte[]> consumer = cluster.consumer(Map.of(
            ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"
        ))) {
            consumer.assign(List.of(topicPartition));
            consumer.seekToBeginning(List.of(topicPartition));
            if (expectedCount == 0) {
                consumer.poll(Duration.ofMillis(1000)).records(topicPartition).forEach(records::add);
            } else {
                TestUtils.waitForCondition(
                    () -> {
                        consumer.poll(Duration.ofMillis(500)).records(topicPartition).forEach(records::add);
                        return records.size() >= expectedCount;
                    },
                    DEFAULT_MAX_WAIT_MS,
                    DEFAULT_POLL_INTERVAL_MS,
                    () -> "Timed out waiting for " + expectedCount + " read-committed records from " + topicPartition);
            }
        }
        assertEquals(expectedCount, records.size());
        return records;
    }

    private List<TopicIdPartition> createTopicIdPartitionsMappedToAllShareStatePartitions(
        String groupId,
        String topicPrefix,
        int partitionCount,
        int replicationFactor
    ) {
        for (int attempt = 0; attempt < 24; attempt++) {
            String topicName = topicPrefix + "-" + attempt;
            Uuid topicId = createTopic(topicName, partitionCount, replicationFactor);
            List<TopicIdPartition> topicIdPartitions = java.util.stream.IntStream.range(0, partitionCount)
                .mapToObj(partition -> new TopicIdPartition(topicId, new TopicPartition(topicName, partition)))
                .toList();
            Map<Integer, TopicIdPartition> topicIdPartitionByShareStatePartition = new HashMap<>();
            for (TopicIdPartition topicIdPartition : topicIdPartitions) {
                topicIdPartitionByShareStatePartition.putIfAbsent(
                    shareStatePartition(groupId, topicIdPartition.topicId(), topicIdPartition.partition()),
                    topicIdPartition
                );
            }
            if (topicIdPartitionByShareStatePartition.size() == sgsTopicPartitions.size()) {
                return new ArrayList<>(topicIdPartitionByShareStatePartition.values());
            }
        }
        fail("Could not find source partitions mapped to all share-state partitions.");
        return List.of();
    }

    private Set<Integer> mappedShareStatePartitions(String groupId, ShareAcknowledgements acknowledgements) {
        return mappedShareStatePartitions(groupId, acknowledgements.acknowledgements().keySet());
    }

    private Set<Integer> mappedShareStatePartitions(String groupId, Iterable<TopicIdPartition> topicIdPartitions) {
        Set<Integer> mappedShareStatePartitions = new HashSet<>();
        for (TopicIdPartition topicIdPartition : topicIdPartitions) {
            mappedShareStatePartitions.add(shareStatePartition(groupId, topicIdPartition.topicId(), topicIdPartition.partition()));
        }
        return mappedShareStatePartitions;
    }

    private TopicIdPartition createTopicIdPartitionWithRemoteShareCoordinator(
        Admin admin,
        String groupId,
        String topicPrefix,
        int sourceLeaderId
    ) throws Exception {
        for (int attempt = 0; attempt < 24; attempt++) {
            String topicName = topicPrefix + "-" + attempt;
            Uuid topicId = createTopic(topicName, 1, 3);
            TopicPartition topicPartition = new TopicPartition(topicName, 0);
            int shareStatePartition = shareStatePartition(groupId, topicId, topicPartition.partition());

            TestUtils.waitForCondition(() -> hasLeader(admin, topicPartition.topic(), topicPartition.partition()) &&
                    hasLeader(admin, Topic.SHARE_GROUP_STATE_TOPIC_NAME, shareStatePartition),
                DEFAULT_MAX_WAIT_MS,
                DEFAULT_POLL_INTERVAL_MS,
                () -> "Timed out waiting for leaders for " + topicPartition);

            int sourceLeader = topicPartitionLeader(admin, topicPartition.topic(), topicPartition.partition()).get(0);
            int shareStateLeader = topicPartitionLeader(admin, Topic.SHARE_GROUP_STATE_TOPIC_NAME, shareStatePartition).get(0);
            if (sourceLeader == sourceLeaderId && sourceLeader != shareStateLeader) {
                return new TopicIdPartition(topicId, topicPartition);
            }
        }
        fail("Could not find source partition on broker " + sourceLeaderId + " with a remote share-state coordinator.");
        return null;
    }

    private boolean hasLeader(Admin admin, String topicName, int partition) {
        try {
            return topicPartitionLeader(admin, topicName, partition).size() == 1;
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private int shareStatePartition(String groupId, Uuid topicId, int partition) {
        SharePartitionKey key = SharePartitionKey.getInstance(groupId, topicId, partition);
        return Utils.abs(key.asCoordinatorKey().hashCode()) % sgsTopicPartitions.size();
    }

    private void shutdownLeaderAndWaitForNewLeader(Admin admin, TopicPartition topicPartition) throws Exception {
        List<Integer> oldLeaders = topicPartitionLeader(admin, topicPartition.topic(), topicPartition.partition());
        assertEquals(1, oldLeaders.size());
        int oldLeader = oldLeaders.get(0);
        KafkaBroker broker = cluster.brokers().get(oldLeader);
        cluster.shutdownBroker(oldLeader);
        broker.awaitShutdown();
        TestUtils.waitForCondition(
            () -> hasDifferentLeader(admin, topicPartition, oldLeader),
            DEFAULT_MAX_WAIT_MS,
            DEFAULT_POLL_INTERVAL_MS,
            () -> "Failed to elect new leader for " + topicPartition);
    }

    private boolean hasDifferentLeader(Admin admin, TopicPartition topicPartition, int oldLeader) {
        try {
            List<Integer> leaders = topicPartitionLeader(admin, topicPartition.topic(), topicPartition.partition());
            return leaders.size() == 1 && leaders.get(0) != oldLeader;
        } catch (InterruptedException | ExecutionException e) {
            return false;
        }
    }

    private void verifyLatestShareStateDeliveryState(
        String groupId,
        TopicIdPartition topicIdPartition,
        long offset,
        RecordState expectedState
    ) throws InterruptedException {
        int shareStatePartition = shareStatePartition(groupId, topicIdPartition.topicId(), topicIdPartition.partition());
        TopicPartition shareStateTopicPartition = new TopicPartition(Topic.SHARE_GROUP_STATE_TOPIC_NAME, shareStatePartition);
        ShareCoordinatorRecordSerde serde = new ShareCoordinatorRecordSerde();

        try (Consumer<byte[], byte[]> consumer = cluster.consumer()) {
            consumer.assign(List.of(shareStateTopicPartition));
            TestUtils.waitForCondition(
                () -> {
                    Byte actualState = latestShareStateDeliveryState(consumer, serde, groupId, topicIdPartition, shareStateTopicPartition, offset);
                    return actualState != null && actualState == expectedState.id();
                },
                DEFAULT_MAX_WAIT_MS,
                DEFAULT_POLL_INTERVAL_MS,
                () -> "Timed out waiting for share state " + expectedState + " for " + topicIdPartition);
        }
    }

    private Byte latestShareStateDeliveryState(
        Consumer<byte[], byte[]> consumer,
        ShareCoordinatorRecordSerde serde,
        String groupId,
        TopicIdPartition topicIdPartition,
        TopicPartition shareStateTopicPartition,
        long offset
    ) {
        consumer.seekToBeginning(List.of(shareStateTopicPartition));
        long endOffset = consumer.endOffsets(List.of(shareStateTopicPartition)).get(shareStateTopicPartition);
        Byte latestDeliveryState = null;

        while (consumer.position(shareStateTopicPartition) < endOffset) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
            if (records.isEmpty()) {
                break;
            }

            for (ConsumerRecord<byte[], byte[]> record : records.records(shareStateTopicPartition)) {
                CoordinatorRecord coordinatorRecord = serde.deserialize(
                    ByteBuffer.wrap(record.key()),
                    record.value() == null ? null : ByteBuffer.wrap(record.value())
                );
                if (coordinatorRecord.value() == null ||
                    !matchesShareStateKey(coordinatorRecord, groupId, topicIdPartition)) {
                    continue;
                }

                Object value = coordinatorRecord.value().message();
                Byte deliveryState = null;
                if (value instanceof ShareSnapshotValue snapshot) {
                    deliveryState = snapshotDeliveryState(snapshot.stateBatches(), offset);
                } else if (value instanceof ShareUpdateValue update) {
                    deliveryState = updateDeliveryState(update.stateBatches(), offset);
                }
                if (deliveryState != null) {
                    latestDeliveryState = deliveryState;
                }
            }
        }

        return latestDeliveryState;
    }

    private boolean matchesShareStateKey(
        CoordinatorRecord coordinatorRecord,
        String groupId,
        TopicIdPartition topicIdPartition
    ) {
        Object key = coordinatorRecord.key();
        if (key instanceof ShareSnapshotKey snapshotKey) {
            return snapshotKey.groupId().equals(groupId) &&
                snapshotKey.topicId().equals(topicIdPartition.topicId()) &&
                snapshotKey.partition() == topicIdPartition.partition();
        }
        if (key instanceof ShareUpdateKey updateKey) {
            return updateKey.groupId().equals(groupId) &&
                updateKey.topicId().equals(topicIdPartition.topicId()) &&
                updateKey.partition() == topicIdPartition.partition();
        }
        return false;
    }

    private Byte snapshotDeliveryState(List<ShareSnapshotValue.StateBatch> stateBatches, long offset) {
        Byte deliveryState = null;
        for (ShareSnapshotValue.StateBatch stateBatch : stateBatches) {
            if (stateBatch.firstOffset() <= offset && offset <= stateBatch.lastOffset()) {
                deliveryState = stateBatch.deliveryState();
            }
        }
        return deliveryState;
    }

    private Byte updateDeliveryState(List<ShareUpdateValue.StateBatch> stateBatches, long offset) {
        Byte deliveryState = null;
        for (ShareUpdateValue.StateBatch stateBatch : stateBatches) {
            if (stateBatch.firstOffset() <= offset && offset <= stateBatch.lastOffset()) {
                deliveryState = stateBatch.deliveryState();
            }
        }
        return deliveryState;
    }
}
