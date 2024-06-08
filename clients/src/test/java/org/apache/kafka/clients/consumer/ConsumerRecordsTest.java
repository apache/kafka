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
        int recordSize = 10;
        int partitionSize = 10;
        int emptyPartitionInterval = 3;
        ConsumerRecords<Integer, String> records = buildTopicTestRecords(recordSize, partitionSize, emptyPartitionInterval, topic);
        Iterator<ConsumerRecord<Integer, String>> iterator = records.iterator();

        for (int count = 0; iterator.hasNext(); count++) {
            if (count % emptyPartitionInterval != 0) {
                int offset = 0;
                for (; offset < recordSize; offset++) {
                    ConsumerRecord<Integer, String> record = iterator.next();
                    assertEquals(count, record.partition());
                    assertEquals(topic, record.topic());
                    assertEquals(offset, record.offset());
                }
                assertEquals(recordSize, offset);
            }
        }
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
        int partitionSize = 10;
        int emptyPartitionInterval = 3;
        ConsumerRecords<Integer, String> consumerRecords = buildTopicTestRecords(recordSize, partitionSize, emptyPartitionInterval, topics);

        for (String topic : topics) {
            Iterable<ConsumerRecord<Integer, String>> records = consumerRecords.records(topic);
            Iterator<ConsumerRecord<Integer, String>> iterator = records.iterator();

            for (int count = 0; iterator.hasNext(); count++) {
                if (count % emptyPartitionInterval != 0) {
                    int offset = 0;
                    for (; offset < recordSize; offset++) {
                        ConsumerRecord<Integer, String> record = iterator.next();
                        assertEquals(count, record.partition());
                        assertEquals(topic, record.topic());
                        assertEquals(offset, record.offset());
                    }
                    assertEquals(recordSize, offset);
                }
            }
        }
    }

    private ConsumerRecords<Integer, String> buildTopicTestRecords(int recordSize,
                                                                   int partitionSize,
                                                                   int emptyPartitionInterval,
                                                                   String... topics) {
        Map<TopicPartition, List<ConsumerRecord<Integer, String>>> partitionToRecords = new LinkedHashMap<>();
        for (String topic : topics) {
            for (int i = 0; i < partitionSize; i++) {
                List<ConsumerRecord<Integer, String>> records = new ArrayList<>(recordSize);
                if (i % emptyPartitionInterval != 0) {
                    for (int j = 0; j < recordSize; j++) {
                        records.add(
                            new ConsumerRecord<>(topic, i, j, 0L, TimestampType.CREATE_TIME,
                                0, 0, j, String.valueOf(j), new RecordHeaders(), Optional.empty())
                        );
                    }
                }
                partitionToRecords.put(new TopicPartition(topic, i), records);
            }
        }

        return new ConsumerRecords<>(partitionToRecords);
    }
}
