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
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;
import static org.apache.kafka.test.TestUtils.DEFAULT_POLL_INTERVAL_MS;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
public class ShareConsumerLagTest extends ShareConsumerTestBase {

    public ShareConsumerLagTest(ClusterInstance cluster) {
        super(cluster);
    }

    @ClusterTest
    public void testSharePartitionLagForSingleShareConsumer() {
        String groupId = "group1";
        alterShareAutoOffsetReset(groupId, "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(groupId);
             Admin adminClient = createAdminClient()) {
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "Message".getBytes());
            producer.send(record);
            producer.flush();
            shareConsumer.subscribe(List.of(tp.topic()));
            // Polling share consumer to make sure the share partition is created and the record is consumed.
            waitedPoll(shareConsumer, 2500L, 1);
            // Acknowledge and commit the consumed record to update the share partition state.
            shareConsumer.commitSync();
            // After the acknowledgement is successful, the share partition lag should be 0 because the only produced record has been consumed.
            verifySharePartitionLag(adminClient, groupId, tp, 0L);
            // Producing another record to the share partition.
            producer.send(record);
            producer.flush();
            // Since the new record has not been consumed yet, the share partition lag should be 1.
            verifySharePartitionLag(adminClient, groupId, tp, 1L);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @ClusterTest
    public void testSharePartitionLagForMultipleShareConsumers() {
        String groupId = "group1";
        alterShareAutoOffsetReset(groupId, "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer1 = createShareConsumer(groupId);
             ShareConsumer<byte[], byte[]> shareConsumer2 = createShareConsumer(groupId);
             Admin adminClient = createAdminClient()) {
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "Message".getBytes());
            producer.send(record);
            producer.flush();
            shareConsumer1.subscribe(List.of(tp.topic()));
            shareConsumer2.subscribe(List.of(tp.topic()));
            // Polling share consumer 1 to make sure the share partition is created and the records are consumed.
            waitedPoll(shareConsumer1, 2500L, 1);
            // Acknowledge and commit the consumed records to update the share partition state.
            shareConsumer1.commitSync();
            // After the acknowledgement is successful, the share partition lag should be 0 because the all produced records have been consumed.
            verifySharePartitionLag(adminClient, groupId, tp, 0L);
            // Producing more records to the share partition.
            producer.send(record);
            // Polling share consumer 2 this time.
            waitedPoll(shareConsumer2, 2500L, 1);
            // Since the consumed record hasn't been acknowledged yet, the share partition lag should be 1.
            verifySharePartitionLag(adminClient, groupId, tp, 1L);
            // Acknowledge and commit the consumed records to update the share partition state.
            shareConsumer2.commitSync();
            // After the acknowledgement is successful, the share partition lag should be 0 because the all produced records have been consumed.
            verifySharePartitionLag(adminClient, groupId, tp, 0L);
            // Producing another record to the share partition.
            producer.send(record);
            producer.flush();
            // Since the new record has not been consumed yet, the share partition lag should be 1.
            verifySharePartitionLag(adminClient, groupId, tp, 1L);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @ClusterTest
    public void testSharePartitionLagWithReleaseAcknowledgement() {
        String groupId = "group1";
        alterShareAutoOffsetReset(groupId, "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(groupId, Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT));
             Admin adminClient = createAdminClient()) {
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "Message".getBytes());
            producer.send(record);
            producer.flush();
            shareConsumer.subscribe(List.of(tp.topic()));
            // Polling share consumer to make sure the share partition is created and the record is consumed.
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 1);
            // Accept the record first to move the offset forward and register the state with persister.
            records.forEach(r -> shareConsumer.acknowledge(r, AcknowledgeType.ACCEPT));
            shareConsumer.commitSync();
            // After accepting, the lag should be 0 because the record is consumed successfully.
            verifySharePartitionLag(adminClient, groupId, tp, 0L);
            // Producing another record to the share partition.
            producer.send(record);
            producer.flush();
            // The produced record is consumed.
            records = waitedPoll(shareConsumer, 2500L, 1);
            // Now release the record - it should be available for redelivery.
            records.forEach(r -> shareConsumer.acknowledge(r, AcknowledgeType.RELEASE));
            shareConsumer.commitSync();
            // After releasing the lag should be 1, because the record is released for redelivery.
            verifySharePartitionLag(adminClient, groupId, tp, 1L);
            // The record is now consumed again.
            records = waitedPoll(shareConsumer, 2500L, 1);
            // Accept the record to mark it as consumed.
            records.forEach(r -> shareConsumer.acknowledge(r, AcknowledgeType.ACCEPT));
            shareConsumer.commitSync();
            // After accepting the record, the lag should be 0 because all the produced records have been consumed.
            verifySharePartitionLag(adminClient, groupId, tp, 0L);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @ClusterTest
    public void testSharePartitionLagWithRejectAcknowledgement() {
        String groupId = "group1";
        alterShareAutoOffsetReset(groupId, "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(groupId, Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT));
             Admin adminClient = createAdminClient()) {
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "Message".getBytes());
            producer.send(record);
            producer.flush();
            shareConsumer.subscribe(List.of(tp.topic()));
            // Polling share consumer to make sure the share partition is created and the record is consumed.
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 1);
            // Accept the record first to move the offset forward and register the state with persister.
            records.forEach(r -> shareConsumer.acknowledge(r, AcknowledgeType.ACCEPT));
            shareConsumer.commitSync();
            // After accepting, the lag should be 0 because the record is consumed successfully.
            verifySharePartitionLag(adminClient, groupId, tp, 0L);
            // Producing another record to the share partition.
            producer.send(record);
            producer.flush();
            // The produced record is consumed.
            records = waitedPoll(shareConsumer, 2500L, 1);
            // Now reject the record - it should not be available for redelivery.
            records.forEach(r -> shareConsumer.acknowledge(r, AcknowledgeType.REJECT));
            shareConsumer.commitSync();
            // After rejecting the lag should be 0, because the record is permanently rejected and offset moves forward.
            verifySharePartitionLag(adminClient, groupId, tp, 0L);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @ClusterTest(
        brokers = 3,
        serverProperties = {
            @ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
            @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "3"),
            @ClusterConfigProperty(key = "share.coordinator.state.topic.num.partitions", value = "1"),
            @ClusterConfigProperty(key = "share.coordinator.state.topic.replication.factor", value = "3")
        }
    )
    public void testSharePartitionLagOnGroupCoordinatorMovement() {
        String groupId = "group1";
        alterShareAutoOffsetReset(groupId, "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(groupId);
             Admin adminClient = createAdminClient()) {
            String topicName = "testTopicWithReplicas";
            // Create a topic with replication factor 3
            createTopic(topicName, 1, 3);
            TopicPartition tp = new TopicPartition(topicName, 0);
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "Message".getBytes());
            // Produce first record and consume it
            producer.send(record);
            producer.flush();
            shareConsumer.subscribe(List.of(tp.topic()));
            // Polling share consumer to make sure the share partition is created and the record is consumed.
            waitedPoll(shareConsumer, 2500L, 1);
            // Acknowledge and commit the consumed record to update the share partition state.
            shareConsumer.commitSync();
            // After the acknowledgement is successful, the share partition lag should be 0 because the only produced record has been consumed.
            verifySharePartitionLag(adminClient, groupId, tp, 0L);
            // Producing another record to the share partition.
            producer.send(record);
            producer.flush();
            // Since the new record has not been consumed yet, the share partition lag should be 1.
            verifySharePartitionLag(adminClient, groupId, tp, 1L);
            List<Integer> curGroupCoordNodeId;
            // Find the broker which is the group coordinator for the share group.
            curGroupCoordNodeId = topicPartitionLeader(adminClient, Topic.GROUP_METADATA_TOPIC_NAME, 0);
            assertEquals(1, curGroupCoordNodeId.size());
            // Shut down the coordinator broker
            KafkaBroker broker = cluster.brokers().get(curGroupCoordNodeId.get(0));
            cluster.shutdownBroker(curGroupCoordNodeId.get(0));
            // Wait for it to be completely shutdown
            broker.awaitShutdown();
            // Wait for the leaders of share coordinator, group coordinator and topic partition to be elected, if needed, on a different broker.
            TestUtils.waitForCondition(() -> {
                List<Integer> newShareCoordNodeId = topicPartitionLeader(adminClient, Topic.SHARE_GROUP_STATE_TOPIC_NAME, 0);
                List<Integer> newGroupCoordNodeId = topicPartitionLeader(adminClient, Topic.GROUP_METADATA_TOPIC_NAME, 0);
                List<Integer> newTopicPartitionLeader = topicPartitionLeader(adminClient, tp.topic(), tp.partition());

                return newShareCoordNodeId.size() == 1 && !Objects.equals(newShareCoordNodeId.get(0), curGroupCoordNodeId.get(0)) &&
                    newGroupCoordNodeId.size() == 1 && !Objects.equals(newGroupCoordNodeId.get(0), curGroupCoordNodeId.get(0)) &&
                    newTopicPartitionLeader.size() == 1 && !Objects.equals(newTopicPartitionLeader.get(0), curGroupCoordNodeId.get(0));
            }, DEFAULT_MAX_WAIT_MS, DEFAULT_POLL_INTERVAL_MS, () -> "Failed to elect new leaders after broker shutdown");
            // After group coordinator shutdown, check that lag is still 1
            verifySharePartitionLag(adminClient, groupId, tp, 1L);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @ClusterTest(
        brokers = 3,
        serverProperties = {
            @ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
            @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "3"),
            @ClusterConfigProperty(key = "share.coordinator.state.topic.num.partitions", value = "1"),
            @ClusterConfigProperty(key = "share.coordinator.state.topic.replication.factor", value = "3")
        }
    )
    public void testSharePartitionLagOnShareCoordinatorMovement() {
        String groupId = "group1";
        alterShareAutoOffsetReset(groupId, "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(groupId);
             Admin adminClient = createAdminClient()) {
            String topicName = "testTopicWithReplicas";
            // Create a topic with replication factor 3
            createTopic(topicName, 1, 3);
            TopicPartition tp = new TopicPartition(topicName, 0);
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "Message".getBytes());
            // Produce first record and consume it
            producer.send(record);
            producer.flush();
            shareConsumer.subscribe(List.of(tp.topic()));
            // Polling share consumer to make sure the share partition is created and the record is consumed.
            waitedPoll(shareConsumer, 2500L, 1);
            // Acknowledge and commit the consumed record to update the share partition state.
            shareConsumer.commitSync();
            // After the acknowledgement is successful, the share partition lag should be 0 because the only produced record has been consumed.
            verifySharePartitionLag(adminClient, groupId, tp, 0L);
            // Producing another record to the share partition.
            producer.send(record);
            producer.flush();
            // Since the new record has not been consumed yet, the share partition lag should be 1.
            verifySharePartitionLag(adminClient, groupId, tp, 1L);
            List<Integer> curShareCoordNodeId;
            // Find the broker which is the share coordinator for the share partition.
            curShareCoordNodeId = topicPartitionLeader(adminClient, Topic.SHARE_GROUP_STATE_TOPIC_NAME, 0);
            assertEquals(1, curShareCoordNodeId.size());
            // Shut down the coordinator broker
            KafkaBroker broker = cluster.brokers().get(curShareCoordNodeId.get(0));
            cluster.shutdownBroker(curShareCoordNodeId.get(0));
            // Wait for it to be completely shutdown
            broker.awaitShutdown();
            // Wait for the leaders of share coordinator, group coordinator and topic partition to be elected, if needed, on a different broker.
            TestUtils.waitForCondition(() -> {
                List<Integer> newShareCoordNodeId = topicPartitionLeader(adminClient, Topic.SHARE_GROUP_STATE_TOPIC_NAME, 0);
                List<Integer> newGroupCoordNodeId = topicPartitionLeader(adminClient, Topic.GROUP_METADATA_TOPIC_NAME, 0);
                List<Integer> newTopicPartitionLeader = topicPartitionLeader(adminClient, tp.topic(), tp.partition());

                return newShareCoordNodeId.size() == 1 && !Objects.equals(newShareCoordNodeId.get(0), curShareCoordNodeId.get(0)) &&
                    newGroupCoordNodeId.size() == 1 && !Objects.equals(newGroupCoordNodeId.get(0), curShareCoordNodeId.get(0)) &&
                    newTopicPartitionLeader.size() == 1 && !Objects.equals(newTopicPartitionLeader.get(0), curShareCoordNodeId.get(0));
            }, DEFAULT_MAX_WAIT_MS, DEFAULT_POLL_INTERVAL_MS, () -> "Failed to elect new leaders after broker shutdown");
            // After share coordinator shutdown and new leader's election, check that lag is still 1
            verifySharePartitionLag(adminClient, groupId, tp, 1L);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @ClusterTest
    public void testSharePartitionLagAfterAlterShareGroupOffsets() {
        String groupId = "group1";
        try (Producer<byte[], byte[]> producer = createProducer();
             Admin adminClient = createAdminClient()) {
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "Message".getBytes());
            // Producing 100 records to the topic partition.
            for (int i = 0; i < 100; i++) {
                producer.send(record);
            }
            producer.flush();

            // Create a new share consumer. Since the share.auto.offset.reset is not altered, it should be latest by default.
            ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(groupId, Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT));
            shareConsumer.subscribe(List.of(tp.topic()));
            // Polling share consumer to make sure it joins the group and subscribes to the topic.
            waitedPoll(shareConsumer, 2500L, 0, true, groupId, List.of(new TopicPartition(tp.topic(), 0)));
            // Producing 5 additional records to the topic partition.
            for (int i = 0; i < 5; i++) {
                producer.send(record);
            }
            producer.flush();
            // Polling share consumer to make sure the records are consumed.
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 5);
            assertEquals(5, records.count());
            // Accept the record first to move the offset forward and register the state with persister.
            records.forEach(r -> shareConsumer.acknowledge(r, AcknowledgeType.ACCEPT));
            shareConsumer.commitSync();
            // After accepting, the lag should be 0 because the record is consumed successfully.
            verifySharePartitionLag(adminClient, groupId, tp, 0L);
            // Closing the share consumer so that the offsets can be altered.
            shareConsumer.close();
            // Alter the start offset of the share partition to 40.
            alterShareGroupOffsets(adminClient, groupId, tp, 40L);
            // After altering, the share partition start offset should be 40.
            verifySharePartitionStartOffset(adminClient, groupId, tp, 40L);
            // Verify that the lag is now 65 since the start offset is altered to 40 and there are total 105 records in the partition.
            verifySharePartitionLag(adminClient, groupId, tp, 65L);
        } catch (InterruptedException | ExecutionException e) {
            fail("Test failed with exception: " + e.getMessage());
        }
    }

    @ClusterTest
    public void testSharePartitionLagAfterDeleteShareGroupOffsets() {
        String groupId = "group1";
        alterShareAutoOffsetReset(groupId, "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             Admin adminClient = createAdminClient()) {
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "Message".getBytes());
            // Producing 5 records to the topic partition.
            for (int i = 0; i < 5; i++) {
                producer.send(record);
            }
            producer.flush();
            // Create a new share consumer.
            ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(groupId, Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT));
            shareConsumer.subscribe(List.of(tp.topic()));
            // Polling share consumer to make sure it joins the group and consumes the produced records.
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 5);
            assertEquals(5, records.count());
            // Accept the records first to move the offset forward and register the state with persister.
            records.forEach(r -> shareConsumer.acknowledge(r, AcknowledgeType.ACCEPT));
            shareConsumer.commitSync();
            // After accepting, the lag should be 0 because the record is consumed successfully.
            verifySharePartitionLag(adminClient, groupId, tp, 0L);
            // Closing the share consumer so that the offsets can be deleted.
            shareConsumer.close();
            // Delete the share group offsets.
            deleteShareGroupOffsets(adminClient, groupId, tp.topic());
            // Verify that the share partition offsets are deleted.
            verifySharePartitionOffsetsDeleted(adminClient, groupId, tp);
            // Create a new share consumer.
            ShareConsumer<byte[], byte[]> shareConsumer2 = createShareConsumer(groupId, Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT));
            shareConsumer2.subscribe(List.of(tp.topic()));
            // Since the offsets are deleted, the share consumer should consume from the beginning (share.auto.offset.reset is earliest).
            // Thus, the consumer should consume all 5 records again.
            records = waitedPoll(shareConsumer2, 2500L, 5);
            assertEquals(5, records.count());
            // Accept the records first to move the offset forward and register the state with persister.
            records.forEach(r -> shareConsumer2.acknowledge(r, AcknowledgeType.ACCEPT));
            shareConsumer2.commitSync();
            // After accepting, the lag should be 0 because the records are consumed successfully.
            verifySharePartitionLag(adminClient, groupId, tp, 0L);
            // Closing the share consumer so that the offsets can be deleted.
            shareConsumer2.close();
        } catch (InterruptedException | ExecutionException e) {
            fail("Test failed with exception: " + e.getMessage());
        }
    }
}