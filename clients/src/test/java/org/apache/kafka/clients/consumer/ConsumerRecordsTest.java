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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConsumerRecordsTest {

    @Test
    public void testIterator() {
        String topic = "topic";
        int recordSize = 10;
        int partitionSize = 15;
        int emptyPartitionIndex = 3;
        ConsumerRecords<Integer, String> records = buildTopicTestRecords(recordSize, partitionSize, emptyPartitionIndex, Collections.singleton(topic));
        Iterator<ConsumerRecord<Integer, String>> iterator = records.iterator();

        int recordCount = 0;
        int partitionCount = 0;
        int currentPartition = -1;

        while (iterator.hasNext()) {
            ConsumerRecord<Integer, String> record = iterator.next();
            validateEmptyPartition(record, emptyPartitionIndex);

            // Check if we have moved to a new partition
            if (currentPartition != record.partition()) {
                // Increment the partition count as we have encountered a new partition
                partitionCount++;
                // Update the current partition to the new partition
                currentPartition = record.partition();
            }

            validateRecordPayload(topic, record, currentPartition, recordCount, recordSize);
            recordCount++;
        }

        // Including empty partition
        assertEquals(partitionSize, partitionCount + 1);
    }

    @Test
    public void testRecordsByPartition() {
        List<String> topics = Arrays.asList("topic1", "topic2");
        int recordSize = 3;
        int partitionSize = 5;
        int emptyPartitionIndex = 2;

        ConsumerRecords<Integer, String> consumerRecords = buildTopicTestRecords(recordSize, partitionSize, emptyPartitionIndex, topics);

        assertEquals(partitionSize * topics.size(), consumerRecords.nextOffsets().size());
        for (String topic : topics) {
            for (int partition = 0; partition < partitionSize; partition++) {
                TopicPartition topicPartition = new TopicPartition(topic, partition);
                List<ConsumerRecord<Integer, String>> records = consumerRecords.records(topicPartition);

                if (partition == emptyPartitionIndex) {
                    assertTrue(records.isEmpty());
                } else {
                    assertEquals(recordSize, records.size());
                    final ConsumerRecord<Integer, String> lastRecord = records.get(recordSize - 1);
                    assertEquals(new OffsetAndMetadata(lastRecord.offset() + 1, lastRecord.leaderEpoch(), ""), consumerRecords.nextOffsets().get(topicPartition));
                    for (int i = 0; i < records.size(); i++) {
                        ConsumerRecord<Integer, String> record = records.get(i);
                        validateRecordPayload(topic, record, partition, i, recordSize);
                    }
                }
            }
        }
    }

    @Test
    public void testRecordsByNullTopic() {
        String nullTopic = null;
        ConsumerRecords<Integer, String> consumerRecords = ConsumerRecords.empty();
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> consumerRecords.records(nullTopic));
        assertEquals("Topic must be non-null.", exception.getMessage());
    }

    @Test
    public void testRecordsByTopic() {
        List<String> topics = Arrays.asList("topic1", "topic2", "topic3", "topic4");
        int recordSize = 3;
        int partitionSize = 10;
        int emptyPartitionIndex = 6;
        int expectedTotalRecordSizeOfEachTopic = recordSize * (partitionSize - 1);

        ConsumerRecords<Integer, String> consumerRecords = buildTopicTestRecords(recordSize, partitionSize, emptyPartitionIndex, topics);

        assertEquals(partitionSize * topics.size(), consumerRecords.nextOffsets().size());

        for (String topic : topics) {
            Iterable<ConsumerRecord<Integer, String>> records = consumerRecords.records(topic);
            int recordCount = 0;
            int partitionCount = 0;
            int currentPartition = -1;

            for (ConsumerRecord<Integer, String> record : records) {
                validateEmptyPartition(record, emptyPartitionIndex);

                // Check if we have moved to a new partition
                if (currentPartition != record.partition()) {
                    // Increment the partition count as we have encountered a new partition
                    partitionCount++;
                    // Update the current partition to the new partition
                    currentPartition = record.partition();
                }

                validateRecordPayload(topic, record, currentPartition, recordCount, recordSize);
                recordCount++;
            }

            // Including empty partition
            assertEquals(partitionSize, partitionCount + 1);
            assertEquals(expectedTotalRecordSizeOfEachTopic, recordCount);
        }
    }

    @Test
    public void testRecordsAreImmutable() {
        String topic = "topic";
        int recordSize = 3;
        int partitionSize = 6;
        int emptyPartitionIndex = 2;
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        ConsumerRecord<Integer, String> newRecord = new ConsumerRecord<>(topic, 0, 0, 0L, TimestampType.CREATE_TIME,
            0, 0, 0, "0", new RecordHeaders(), Optional.empty());
        ConsumerRecords<Integer, String> records = buildTopicTestRecords(recordSize, partitionSize, emptyPartitionIndex, Collections.singleton(topic));
        ConsumerRecords<Integer, String> emptyRecords = ConsumerRecords.empty();

        assertEquals(partitionSize, records.nextOffsets().size());
        // check records(TopicPartition) / partitions by add method
        // check iterator / records(String) by remove method
        // check data count after all operations
        assertThrows(UnsupportedOperationException.class, () -> records.records(topicPartition).add(newRecord));
        assertThrows(UnsupportedOperationException.class, () -> records.partitions().add(topicPartition));
        assertThrows(UnsupportedOperationException.class, () -> records.iterator().remove());
        assertThrows(UnsupportedOperationException.class, () -> records.records(topic).iterator().remove());
        assertEquals(recordSize * (partitionSize - 1), records.count());

        // do the same unittest on the empty records
        assertThrows(UnsupportedOperationException.class, () -> emptyRecords.records(topicPartition).add(newRecord));
        assertThrows(UnsupportedOperationException.class, () -> emptyRecords.partitions().add(topicPartition));
        assertThrows(UnsupportedOperationException.class, () -> emptyRecords.iterator().remove());
        assertThrows(UnsupportedOperationException.class, () -> emptyRecords.records(topic).iterator().remove());
        assertEquals(0, emptyRecords.count());
    }

    private ConsumerRecords<Integer, String> buildTopicTestRecords(int recordSize,
                                                                   int partitionSize,
                                                                   int emptyPartitionIndex,
                                                                   Collection<String> topics) {
        Map<TopicPartition, List<ConsumerRecord<Integer, String>>> partitionToRecords = new LinkedHashMap<>();
        Map<TopicPartition, OffsetAndMetadata> nextOffsets = new HashMap<>();
        for (String topic : topics) {
            for (int i = 0; i < partitionSize; i++) {
                List<ConsumerRecord<Integer, String>> records = new ArrayList<>(recordSize);
                if (i != emptyPartitionIndex) {
                    for (int j = 0; j < recordSize; j++) {
                        records.add(
                            new ConsumerRecord<>(topic, i, j, 0L, TimestampType.CREATE_TIME,
                                0, 0, j, String.valueOf(j), new RecordHeaders(), Optional.empty())
                        );
                    }
                }
                final TopicPartition tp = new TopicPartition(topic, i);
                partitionToRecords.put(tp, records);
                nextOffsets.put(tp, new OffsetAndMetadata(recordSize, Optional.empty(), ""));
            }
        }

        return new ConsumerRecords<>(partitionToRecords, nextOffsets);
    }

    private void validateEmptyPartition(ConsumerRecord<Integer, String> record, int emptyPartitionIndex) {
        assertNotEquals(emptyPartitionIndex, record.partition(), "Partition " + record.partition() + " is not empty");
    }

    private void validateRecordPayload(String topic, ConsumerRecord<Integer, String> record, int currentPartition, int recordCount, int recordSize) {
        assertEquals(topic, record.topic());
        assertEquals(currentPartition, record.partition());
        assertEquals(recordCount % recordSize, record.offset());
        assertEquals(recordCount % recordSize, record.key());
        assertEquals(String.valueOf(recordCount % recordSize), record.value());
    }
}
