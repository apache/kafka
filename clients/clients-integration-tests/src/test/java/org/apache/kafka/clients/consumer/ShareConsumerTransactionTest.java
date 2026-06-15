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
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;

import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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

    private Producer<byte[], byte[]> createTransactionalProducer(String transactionalId) {
        return createProducer(Map.of(
            ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId,
            ProducerConfig.MAX_BLOCK_MS_CONFIG, "15000",
            ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000",
            ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "20000"
        ));
    }

    private ProducerRecord<byte[], byte[]> record(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        return new ProducerRecord<>(tp.topic(), tp.partition(), null, bytes, bytes);
    }
}
