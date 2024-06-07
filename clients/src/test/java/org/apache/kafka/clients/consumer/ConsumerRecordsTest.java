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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

public class ConsumerRecordsTest {

    @Test
    public void iterator() throws Exception {
        String topic = "topic";
        ConsumerRecords<Integer, String> consumerRecords = new ConsumerRecords<>(buildSingleTopicTestRecords(topic));
        Iterator<ConsumerRecord<Integer, String>> iter = consumerRecords.iterator();

        int c = 0;
        for (; iter.hasNext(); c++) {
            ConsumerRecord<Integer, String> record = iter.next();
            assertEquals(1, record.partition());
            assertEquals(topic, record.topic());
            assertEquals(c, record.offset());
        }
        assertEquals(2, c);
    }

    @Test
    public void testRecordsWithNullTopic() {
        String nullTopic = null;
        Map<TopicPartition, List<ConsumerRecord<Integer, String>>> records = new LinkedHashMap<>();
        ConsumerRecords<Integer, String> consumerRecords = new ConsumerRecords<>(records);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> consumerRecords.records(nullTopic));
        assertEquals("Topic must be non-null.", exception.getMessage());
    }

    @Test
    public void testRecords() {
        String[] topics = {"topic1", "topic2", "topic3", "topic4"};
        int recordSize = 3;
        Map<TopicPartition, List<ConsumerRecord<Integer, String>>> partitionToRecords = buildTopicTestRecords(recordSize, topics);
        ConsumerRecords<Integer, String> consumerRecords = new ConsumerRecords<>(partitionToRecords);
        for (String topic : topics) {
            Iterable<ConsumerRecord<Integer, String>> records = consumerRecords.records(topic);

            int count  = 0;
            Iterator<ConsumerRecord<Integer, String>> iterator = records.iterator();
            for (; iterator.hasNext() && count < recordSize; count++) {
                ConsumerRecord<Integer, String> record = iterator.next();
                assertEquals(topic, record.topic());
                assertEquals(count, record.partition());
                assertEquals(count, record.offset());
                assertEquals(count, record.key());
                assertEquals(String.valueOf(count), record.value());
            }
        }
    }

    private Map<TopicPartition, List<ConsumerRecord<Integer, String>>> buildSingleTopicTestRecords(String topic) {
        Map<TopicPartition, List<ConsumerRecord<Integer, String>>> records = new LinkedHashMap<>();
        ConsumerRecord<Integer, String> record1 = new ConsumerRecord<>(topic, 1, 0, 0L, TimestampType.CREATE_TIME,
            0, 0, 1, "value1", new RecordHeaders(), Optional.empty());
        ConsumerRecord<Integer, String> record2 = new ConsumerRecord<>(topic, 1, 1, 0L, TimestampType.CREATE_TIME,
            0, 0, 2, "value2", new RecordHeaders(), Optional.empty());

        new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            new ConsumerRecord<>(topic, i, i, 0L, TimestampType.CREATE_TIME,
                0, 0, 2, String.valueOf(i), new RecordHeaders(), Optional.empty());
        }

        records.put(new TopicPartition(topic, 0), new ArrayList<>());
        records.put(new TopicPartition(topic, 1), Arrays.asList(record1, record2));
        records.put(new TopicPartition(topic, 2), new ArrayList<>());
        return records;
    }

    private Map<TopicPartition, List<ConsumerRecord<Integer, String>>> buildTopicTestRecords(int recordSize, String... topics) {
        Map<TopicPartition, List<ConsumerRecord<Integer, String>>> partitionToRecords = new LinkedHashMap<>();
        for (String topic : topics) {
            ArrayList<ConsumerRecord<Integer, String>> records = new ArrayList<>(recordSize);
            int i = 0;
            while (i < recordSize) {
                records.add(
                    new ConsumerRecord<>(topic, i, i, 0L, TimestampType.CREATE_TIME,
                        0, 0, i, String.valueOf(i), new RecordHeaders(), Optional.empty())
                );
                i++;
            }
            partitionToRecords.put(new TopicPartition(topic, i), records);
        }

        return partitionToRecords;
    }
}
