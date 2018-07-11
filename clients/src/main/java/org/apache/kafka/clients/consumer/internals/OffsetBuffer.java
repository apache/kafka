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
package org.apache.kafka.clients.consumer.internals;

import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;
import java.util.PriorityQueue;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

public class OffsetBuffer<K, V> {
    private final Map<TopicPartition, PriorityQueue<ConsumerRecord<K, V>>> consumedOffsets;

    public OffsetBuffer() {
        consumedOffsets = new HashMap<>();
    }

    public void insertOffsets(ConsumerRecords<K, V> records) {
        if (records.isEmpty()) return;
        for (TopicPartition partition : records.partitions()) {
            if (consumedOffsets.containsKey(partition)) {
                consumedOffsets.get(partition).addAll(records.records(partition));
                continue;
            }
            consumedOffsets.put(partition, new PriorityQueue<>());
        }
    }

    public ConsumerRecords<K, V> pollOffsets() {
        Map<TopicPartition, List<ConsumerRecord<K, V>>> result = new HashMap<>();
        for (Map.Entry<TopicPartition, PriorityQueue<ConsumerRecord<K, V>>> offsetSeq : consumedOffsets.entrySet()) {
            ConsumerRecord<K, V>[] arrayOfRecords = offsetSeq.getValue().toArray(new ConsumerRecord[offsetSeq.getValue().size()]);
            result.put(offsetSeq.getKey(), Arrays.asList(arrayOfRecords));
            offsetSeq.getValue().clear();
        }
        return new ConsumerRecords<>(result);
    }
}
