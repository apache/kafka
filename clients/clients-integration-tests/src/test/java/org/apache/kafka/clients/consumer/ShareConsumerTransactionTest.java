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
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.share.SharePartitionKey;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;
import static org.apache.kafka.test.TestUtils.DEFAULT_POLL_INTERVAL_MS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
                transactionalProducer.commitTransaction();

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
                transactionalProducer.abortTransaction();

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
                }

                shutdownLeaderAndWaitForNewLeader(admin, topicPartition);

                try (ShareConsumer<byte[], byte[]> reloadedConsumer = createShareConsumer(
                    groupId,
                    Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))) {
                    reloadedConsumer.subscribe(Set.of(topicPartition.topic()));
                    assertEquals(0, reloadedConsumer.poll(Duration.ofMillis(1000)).count());

                    transactionalProducer.commitTransaction();

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
                }

                shutdownLeaderAndWaitForNewLeader(admin, topicPartition);

                try (ShareConsumer<byte[], byte[]> reloadedConsumer = createShareConsumer(
                    groupId,
                    Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))) {
                    reloadedConsumer.subscribe(Set.of(topicPartition.topic()));
                    assertEquals(0, reloadedConsumer.poll(Duration.ofMillis(1000)).count());

                    transactionalProducer.abortTransaction();

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

    private ProducerRecord<byte[], byte[]> record(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        return new ProducerRecord<>(tp.topic(), tp.partition(), null, bytes, bytes);
    }

    private ProducerRecord<byte[], byte[]> record(TopicPartition topicPartition, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        return new ProducerRecord<>(topicPartition.topic(), topicPartition.partition(), null, bytes, bytes);
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
}
