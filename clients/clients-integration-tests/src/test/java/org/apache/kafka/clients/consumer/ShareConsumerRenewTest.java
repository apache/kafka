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

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.errors.InvalidRecordStateException;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;

import com.yammer.metrics.core.Meter;

import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

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
public class ShareConsumerRenewTest extends ShareConsumerTestBase {

    public ShareConsumerRenewTest(ClusterInstance cluster) {
        super(cluster);
    }

    @ClusterTest
    public void testRenewAcknowledgementOnPoll() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                 "group1",
                 Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))
        ) {
            AtomicInteger acknowledgementsCommitted = new AtomicInteger(0);
            shareConsumer.setAcknowledgementCommitCallback((offsetsByTopicPartition, exception) ->
                offsetsByTopicPartition.forEach((tip, offsets) -> acknowledgementsCommitted.addAndGet(offsets.size())));

            for (int i = 0; i < 10; i++) {
                ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), ("Message " + i).getBytes());
                producer.send(record);
            }
            producer.flush();

            shareConsumer.subscribe(List.of(tp.topic()));
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 10);
            assertEquals(10, records.count());

            int count = 0;
            for (ConsumerRecord<byte[], byte[]> record : records) {
                if (count % 2 == 0) {
                    shareConsumer.acknowledge(record, AcknowledgeType.ACCEPT);
                } else {
                    shareConsumer.acknowledge(record, AcknowledgeType.RENEW);
                }
                count++;
            }

            // Get the rest of all 5 records.
            records = waitedPoll(shareConsumer, 2500L, 5);
            assertEquals(5, records.count());
            for (ConsumerRecord<byte[], byte[]> record : records) {
                shareConsumer.acknowledge(record, AcknowledgeType.ACCEPT);
            }

            shareConsumer.commitSync();
            assertEquals(15, acknowledgementsCommitted.get());
        }
        verifyYammerMetricCount("ackType=Renew", 5);
    }

    @ClusterTest
    public void testRenewAcknowledgementOnCommitSync() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                 "group1",
                 Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))
        ) {
            AtomicInteger acknowledgementsCommitted = new AtomicInteger(0);
            shareConsumer.setAcknowledgementCommitCallback((offsetsByTopicPartition, exception) ->
                offsetsByTopicPartition.forEach((tip, offsets) -> acknowledgementsCommitted.addAndGet(offsets.size())));

            for (int i = 0; i < 10; i++) {
                ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), ("Message " + i).getBytes());
                producer.send(record);
            }
            producer.flush();

            shareConsumer.subscribe(List.of(tp.topic()));
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 10);
            assertEquals(10, records.count());
            assertEquals(Optional.of(15000), shareConsumer.acquisitionLockTimeoutMs());

            // The updated acquisition lock timeout is only applied when the next poll is called.
            alterShareRecordLockDurationMs("group1", 25000);

            int count = 0;
            Map<TopicIdPartition, Optional<KafkaException>> result;
            for (ConsumerRecord<byte[], byte[]> record : records) {
                if (count % 2 == 0) {
                    shareConsumer.acknowledge(record, AcknowledgeType.ACCEPT);
                } else {
                    shareConsumer.acknowledge(record, AcknowledgeType.RENEW);
                }
                result = shareConsumer.commitSync();
                assertEquals(1, result.size());
                assertEquals(Optional.of(15000), shareConsumer.acquisitionLockTimeoutMs());
                assertEquals(Optional.empty(), result.get(new TopicIdPartition(tpId, tp.partition(), tp.topic())));
                count++;
            }

            // Get the rest of all 5 records.
            records = waitedPoll(shareConsumer, 2500L, 5);
            assertEquals(5, records.count());
            assertEquals(Optional.of(25000), shareConsumer.acquisitionLockTimeoutMs());
            for (ConsumerRecord<byte[], byte[]> record : records) {
                shareConsumer.acknowledge(record, AcknowledgeType.ACCEPT);
            }
        }
        verifyYammerMetricCount("ackType=Renew", 5);
    }

    @ClusterTest
    public void testRenewAcknowledgementInvalidStateRecord() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                 "group1",
                 Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))
        ) {
            AtomicInteger acknowledgementsCommitted = new AtomicInteger(0);
            shareConsumer.setAcknowledgementCommitCallback((offsetsByTopicPartition, exception) ->
                offsetsByTopicPartition.forEach((tip, offsets) -> acknowledgementsCommitted.addAndGet(offsets.size())));

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "Message ".getBytes());
            producer.send(record);
            producer.flush();

            shareConsumer.subscribe(List.of(tp.topic()));
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 1);
            assertEquals(1, records.count());

            for (ConsumerRecord<byte[], byte[]> rec : records) {
                shareConsumer.acknowledge(rec, AcknowledgeType.REJECT);
                shareConsumer.commitSync();
                assertThrows(IllegalStateException.class, () -> shareConsumer.acknowledge(rec, AcknowledgeType.RENEW));
            }
        }
        verifyYammerMetricCount("ackType=Renew", 0);
    }

    @ClusterTest
    public void testRenewAcknowledgementDisabled() {
        alterShareAutoOffsetReset("group1", "earliest");
        alterShareRenewAcknowledgeEnable("group1", false);
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                 "group1",
                 Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))
        ) {
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "Message".getBytes());
            producer.send(record);
            producer.flush();

            shareConsumer.subscribe(List.of(tp.topic()));
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 1);
            assertEquals(1, records.count());

            for (ConsumerRecord<byte[], byte[]> rec : records) {
                shareConsumer.acknowledge(rec, AcknowledgeType.RENEW);
            }

            Map<TopicIdPartition, Optional<KafkaException>> result = shareConsumer.commitSync();
            assertEquals(1, result.size());
            Optional<KafkaException> error = result.get(new TopicIdPartition(tpId, tp.partition(), tp.topic()));
            assertTrue(error.isPresent());
            assertInstanceOf(InvalidRecordStateException.class, error.get());
        }
    }

    @ClusterTest(
        brokers = 1,
        serverProperties = {
            @ClusterConfigProperty(key = "group.share.record.lock.duration.ms", value = "12000"),
            @ClusterConfigProperty(key = "group.share.min.record.lock.duration.ms", value = "12000"),
        }
    )
    public void testRenewAcknowledgementNoResultInPoll() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                 "group1",
                 Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))
        ) {
            AtomicInteger acknowledgementsCommitted = new AtomicInteger(0);
            shareConsumer.setAcknowledgementCommitCallback((offsetsByTopicPartition, exception) ->
                offsetsByTopicPartition.forEach((tip, offsets) -> acknowledgementsCommitted.addAndGet(offsets.size())));

            for (int i = 0; i < 10; i++) {
                ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), ("Message " + i).getBytes());
                producer.send(record);
            }
            producer.flush();

            shareConsumer.subscribe(List.of(tp.topic()));
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 10);
            assertEquals(10, records.count());

            int count = 0;
            for (ConsumerRecord<byte[], byte[]> record : records) {
                if (count % 2 == 0) {
                    shareConsumer.acknowledge(record, AcknowledgeType.ACCEPT);
                } else {
                    shareConsumer.acknowledge(record, AcknowledgeType.RENEW);
                }
                count++;
            }

            // 5 more records (total 15 produced).
            for (int i = 10; i < 15; i++) {
                ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), ("Message " + i).getBytes());
                producer.send(record);
            }

            // Get the rest of all 5 records.
            records = waitedPoll(shareConsumer, 11500L, 0);  // This will send the acks but not return next 5 records (10-15)
            assertEquals(10, acknowledgementsCommitted.get());
            assertEquals(0, records.count());
            verifyYammerMetricCount("ackType=Renew", 5);

            // Renewal duration passed, now records will be back.
            records = waitedPoll(shareConsumer, 2500L, 5);  // Renewed records as well as 10-15 records.
            assertEquals(5, records.count());
            for (ConsumerRecord<byte[], byte[]> record : records) {
                shareConsumer.acknowledge(record, AcknowledgeType.ACCEPT);
            }

            shareConsumer.commitSync();

            records = waitedPoll(shareConsumer, 2500L, 5);  // 10-15 records.
            assertEquals(5, records.count());
            for (ConsumerRecord<byte[], byte[]> record : records) {
                shareConsumer.acknowledge(record, AcknowledgeType.ACCEPT);
            }

            shareConsumer.commitSync();

            // Initial - 5 renew + 5 accept, Subsequent - 5 renewed accepted + 5 fresh accepted (10-15)
            assertEquals(20, acknowledgementsCommitted.get());
        }
        verifyYammerMetricCount("ackType=Renew", 5);
    }

    private void verifyYammerMetricCount(String filterString, int count) {
        com.yammer.metrics.core.Metric renewAck = KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet().stream()
            .filter(entry -> entry.getKey().toString().contains(filterString))
            .findAny()
            .orElseThrow(() -> new AssertionError("metric not found"))
            .getValue();

        assertEquals(count, ((Meter) renewAck).count());
    }
}