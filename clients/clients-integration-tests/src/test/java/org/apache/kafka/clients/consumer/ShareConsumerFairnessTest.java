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
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.record.internal.DefaultRecordBatch;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;

import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that a share fetch spreads its record budget across all the partitions with data and does not
 * keep serving the same partition while the others are starved.
 */
@Timeout(600)
@ClusterTestDefaults(
    types = {Type.KRAFT},
    serverProperties = {
        @ClusterConfigProperty(key = "auto.create.topics.enable", value = "false"),
        @ClusterConfigProperty(key = "group.share.max.partition.max.record.locks", value = "10000"),
        @ClusterConfigProperty(key = "group.share.partition.max.record.locks", value = "10000"),
        @ClusterConfigProperty(key = "group.share.record.lock.duration.ms", value = "60000"),
        @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
        @ClusterConfigProperty(key = "share.coordinator.state.topic.min.isr", value = "1"),
        @ClusterConfigProperty(key = "share.coordinator.state.topic.num.partitions", value = "3"),
        @ClusterConfigProperty(key = "share.coordinator.state.topic.replication.factor", value = "1"),
        @ClusterConfigProperty(key = "transaction.state.log.min.isr", value = "1"),
        @ClusterConfigProperty(key = "transaction.state.log.replication.factor", value = "1")
    }
)
public class ShareConsumerFairnessTest extends ShareConsumerTestBase {

    private static final int PARTITIONS = 2;
    private static final int RECORDS_PER_PARTITION = 8000;
    private static final int MAX_POLL_RECORDS = 100;
    private static final int POLLS_PER_PHASE = 20;
    private static final int RECORD_VALUE_SIZE = 1024;
    // Size on the log of a single record batch holding one record with a value of RECORD_VALUE_SIZE bytes.
    private static final int RECORD_SIZE = DefaultRecordBatch.sizeInBytes(List.of(new SimpleRecord(new byte[RECORD_VALUE_SIZE])));

    public ShareConsumerFairnessTest(ClusterInstance cluster) {
        super(cluster);
    }

    @ClusterTest
    public void testPartitionsServedFairlyInImplicitMode() {
        String topic = "fairness";
        String groupId = "fairness-group";
        createTopic(topic, PARTITIONS, 1);
        produceRecords(topic, PARTITIONS);
        alterShareAutoOffsetReset(groupId, "earliest");

        Map<Integer, Integer> recordsByPartition = new TreeMap<>();
        // The records by partition at the moment the first partition has been consumed completely.
        Map<Integer, Integer> recordsWhenFirstPartitionCompleted = null;
        int totalRecords = PARTITIONS * RECORDS_PER_PARTITION;
        int consumedRecords = 0;
        try (ShareConsumer<byte[], byte[]> consumer = createShareConsumer(groupId, Map.of(
            ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(MAX_POLL_RECORDS),
            ConsumerConfig.SHARE_ACQUIRE_MODE_CONFIG, "record_limit"))) {
            consumer.subscribe(Set.of(topic));
            // Consume all the records, the test timeout fails the test if some records are not delivered.
            while (consumedRecords < totalRecords) {
                for (ConsumerRecord<byte[], byte[]> record : consumer.poll(Duration.ofMillis(500))) {
                    consumedRecords++;
                    int partitionRecords = recordsByPartition.merge(record.partition(), 1, Integer::sum);
                    if (partitionRecords == RECORDS_PER_PARTITION && recordsWhenFirstPartitionCompleted == null) {
                        recordsWhenFirstPartitionCompleted = new TreeMap<>(recordsByPartition);
                    }
                }
            }
        }

        assertEquals(Map.of(0, RECORDS_PER_PARTITION, 1, RECORDS_PER_PARTITION), recordsByPartition);
        for (int p = 0; p < PARTITIONS; p++) {
            // Both partitions have a deep backlog and the fetches alternate between them, hence the other partition
            // is only a few polls behind when the first partition completes. Allow a generous buffer, a starved
            // partition is far below it.
            int recordsOfPartition = recordsWhenFirstPartitionCompleted.getOrDefault(p, 0);
            assertTrue(recordsOfPartition * 4 >= RECORDS_PER_PARTITION * 3,
                "Partition " + p + " was starved, records by partition when the first partition completed: "
                    + recordsWhenFirstPartitionCompleted);
        }
    }

    @ClusterTest
    public void testPartitionsServedFairlyWithExplicitAcknowledgements() {
        verifyPartitionsServedFairlyWithExplicitAcknowledgements("fairness-epoch-parity", PARTITIONS);
        verifyPartitionsServedFairlyWithExplicitAcknowledgements("fairness-epoch-parity-odd", 3);
    }

    @ClusterTest
    public void testPartitionsRotateWhenAllPartitionsReturnRecordsWithExplicitAcknowledgement() {
        verifyFirstPositionRotatesWhenAllPartitionsReturnRecords("fairness-rotation", PARTITIONS, "explicit");
        verifyFirstPositionRotatesWhenAllPartitionsReturnRecords("fairness-rotation-odd", 3, "explicit");
    }

    @ClusterTest
    public void testPartitionsRotateWhenAllPartitionsReturnRecordsWithImplicitAcknowledgement() {
        verifyFirstPositionRotatesWhenAllPartitionsReturnRecords("fairness-rotation-implicit", 3, "implicit");
    }

    /**
     * A partition which does not return records keeps its position at the front of the share session, hence
     * it is read first as soon as it has records, ahead of the partition which was served all along.
     */
    @ClusterTest
    public void testPartitionWithoutRecordsIsReadFirstWhenRecordsArrive() {
        String topic = "fairness-idle-partition";
        String groupId = "fairness-idle-partition-group";
        int lateRecords = MAX_POLL_RECORDS / 2;
        createTopic(topic, PARTITIONS, 1);
        // Only partition 0 has records initially.
        try (Producer<byte[], byte[]> producer = createProducer()) {
            for (int i = 0; i < RECORDS_PER_PARTITION; i++) {
                producer.send(new ProducerRecord<>(topic, 0, null, null, "value".getBytes()));
            }
            producer.flush();
        }
        alterShareAutoOffsetReset(groupId, "earliest");

        try (ShareConsumer<byte[], byte[]> consumer = createShareConsumer(groupId, Map.of(
            ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(MAX_POLL_RECORDS),
            ConsumerConfig.SHARE_ACQUIRE_MODE_CONFIG, "record_limit",
            ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, "explicit"))) {
            consumer.subscribe(Set.of(topic));
            Map<Integer, Integer> recordsByPartition = pollAndCommit(consumer);
            assertEquals(Map.of(0, MAX_POLL_RECORDS * POLLS_PER_PHASE), recordsByPartition);

            // Partition 1 gets records, fewer than a single poll can return.
            try (Producer<byte[], byte[]> producer = createProducer()) {
                for (int i = 0; i < lateRecords; i++) {
                    producer.send(new ProducerRecord<>(topic, 1, null, null, "value".getBytes()));
                }
                producer.flush();
            }

            // The next poll returns all the records of partition 1 and fills up from partition 0.
            Map<Integer, Integer> nextPoll = new TreeMap<>();
            pollRecords(consumer).forEach(record -> nextPoll.merge(record.partition(), 1, Integer::sum));
            assertEquals(Map.of(0, MAX_POLL_RECORDS - lateRecords, 1, lateRecords), nextPoll);
        }
    }

    private void verifyPartitionsServedFairlyWithExplicitAcknowledgements(String topic, int partitions) {
        String groupId = topic + "-group";
        createTopic(topic, partitions, 1);
        produceRecords(topic, partitions);
        alterShareAutoOffsetReset(groupId, "earliest");

        Map<Integer, Integer> afterSingleAcknowledgement;
        Map<Integer, Integer> afterMultipleAcknowledgements;
        try (ShareConsumer<byte[], byte[]> consumer = createShareConsumer(groupId, Map.of(
            ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(MAX_POLL_RECORDS),
            ConsumerConfig.SHARE_ACQUIRE_MODE_CONFIG, "record_limit",
            ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, "explicit"))) {
            consumer.subscribe(Set.of(topic));
            afterSingleAcknowledgement = pollAndCommit(consumer);

            // Commit the acknowledgements of one poll in two requests, which flips the parity of the epoch.
            List<ConsumerRecord<byte[], byte[]>> records = pollRecords(consumer);
            int half = records.size() / 2;
            records.subList(0, half).forEach(consumer::acknowledge);
            consumer.commitSync();
            records.subList(half, records.size()).forEach(consumer::acknowledge);
            consumer.commitSync();

            afterMultipleAcknowledgements = pollAndCommit(consumer);
        }

        // Each phase consumes far fewer records than the backlog of either partition, hence both partitions have
        // records throughout and each should get a meaningful share of the records of every phase.
        String recordsByPhase = "records by partition before the epoch parity changed: " + afterSingleAcknowledgement
            + ", after: " + afterMultipleAcknowledgements;
        for (Map<Integer, Integer> phase : List.of(afterSingleAcknowledgement, afterMultipleAcknowledgements)) {
            int phaseRecords = phase.values().stream().mapToInt(Integer::intValue).sum();
            for (int p = 0; p < partitions; p++) {
                assertTrue(phase.getOrDefault(p, 0) * partitions * 2 >= phaseRecords,
                    "Partition " + p + " was starved, " + recordsByPhase);
            }
        }
    }

    private void verifyFirstPositionRotatesWhenAllPartitionsReturnRecords(String topic, int partitions, String acknowledgementMode) {
        String groupId = topic + "-group";
        // Records per partition a single fetch can return, capped by the fetch bytes of the partition.
        int recordsPerPartitionPerFetch = 100;
        createTopic(topic, partitions, 1);
        // Enough records per partition to outlast the polls even if a partition were read first on every poll.
        produceRecords(topic, partitions, POLLS_PER_PHASE * recordsPerPartitionPerFetch, RECORD_VALUE_SIZE);
        alterShareAutoOffsetReset(groupId, "earliest");

        Map<Integer, Integer> recordsByPartition;
        try (ShareConsumer<byte[], byte[]> consumer = createShareConsumer(groupId, Map.of(
            ConsumerConfig.FETCH_MAX_BYTES_CONFIG, String.valueOf(partitions * recordsPerPartitionPerFetch * RECORD_SIZE),
            // Every partition but the last returns the records its fetch bytes allow, the last one gets half of it.
            ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(recordsPerPartitionPerFetch * partitions - recordsPerPartitionPerFetch / 2),
            ConsumerConfig.SHARE_ACQUIRE_MODE_CONFIG, "record_limit",
            ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, acknowledgementMode))) {
            consumer.subscribe(Set.of(topic));
            recordsByPartition = acknowledgementMode.equals("explicit") ? pollAndCommit(consumer) : pollImplicit(consumer);
        }

        // With the first position rotating, every partition takes every position in turn and the partitions
        // consume the same number of records. Without rotation the last partition is stuck with half the share.
        int totalRecords = recordsByPartition.values().stream().mapToInt(Integer::intValue).sum();
        for (int p = 0; p < partitions; p++) {
            assertTrue(recordsByPartition.getOrDefault(p, 0) * partitions * 4 >= totalRecords * 3,
                "Partition " + p + " did not take the first position, records by partition: " + recordsByPartition);
        }
    }

    private void produceRecords(String topic, int partitions) {
        produceRecords(topic, partitions, RECORDS_PER_PARTITION, "value".getBytes().length);
    }

    /**
     * Produces the given number of records of the given value size to every partition, each record in its own
     * batch so the size of the records on the log is known.
     */
    private void produceRecords(String topic, int partitions, int recordsPerPartition, int valueSize) {
        try (Producer<byte[], byte[]> producer = createProducer(Map.of(ProducerConfig.BATCH_SIZE_CONFIG, 0))) {
            for (int i = 0; i < recordsPerPartition; i++) {
                for (int p = 0; p < partitions; p++) {
                    producer.send(new ProducerRecord<>(topic, p, null, null, new byte[valueSize]));
                }
            }
            producer.flush();
        }
    }

    /**
     * Polls until {@link #POLLS_PER_PHASE} polls have returned records, and commits the acknowledgements of each poll
     * in one request.
     */
    private Map<Integer, Integer> pollAndCommit(ShareConsumer<byte[], byte[]> consumer) {
        Map<Integer, Integer> recordsByPartition = new TreeMap<>();
        for (int poll = 0; poll < POLLS_PER_PHASE; poll++) {
            for (ConsumerRecord<byte[], byte[]> record : pollRecords(consumer)) {
                recordsByPartition.merge(record.partition(), 1, Integer::sum);
                consumer.acknowledge(record);
            }
            consumer.commitSync();
        }
        return recordsByPartition;
    }

    /**
     * Polls until {@link #POLLS_PER_PHASE} polls have returned records, with implicit acknowledgement the records
     * of a poll are acknowledged on the next poll.
     */
    private Map<Integer, Integer> pollImplicit(ShareConsumer<byte[], byte[]> consumer) {
        Map<Integer, Integer> recordsByPartition = new TreeMap<>();
        for (int poll = 0; poll < POLLS_PER_PHASE; poll++) {
            pollRecords(consumer).forEach(record -> recordsByPartition.merge(record.partition(), 1, Integer::sum));
        }
        return recordsByPartition;
    }

    /**
     * Polls until records are returned, the test timeout fails the test if no records are delivered.
     */
    private List<ConsumerRecord<byte[], byte[]>> pollRecords(ShareConsumer<byte[], byte[]> consumer) {
        List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        while (records.isEmpty()) {
            consumer.poll(Duration.ofMillis(200)).forEach(records::add);
        }
        return records;
    }
}
