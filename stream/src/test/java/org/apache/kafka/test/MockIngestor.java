/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.test;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.processor.internals.Ingestor;
import org.apache.kafka.clients.processor.internals.StreamGroup;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MockIngestor implements Ingestor {

    private HashMap<TopicPartition, StreamGroup> streamSynchronizers = new HashMap<>();

    public HashSet<TopicPartition> paused = new HashSet<>();

    @Override
    public Set<String> topics() {
        return null;
    }

    @Override
    public void poll(long timeoutMs) {
    }

    @Override
    public void pause(TopicPartition partition) {
        paused.add(partition);
    }

    @Override
    public void unpause(TopicPartition partition, long offset) {
        paused.remove(partition);
    }

    @Override
    public void commit(Map<TopicPartition, Long> offsets) { /* do nothing */}

    @Override
    public int numPartitions(String topic) {
        return 1;
    }

    @Override
    public void addPartitionStreamToGroup(StreamGroup streamGroup, TopicPartition partition) {
        streamSynchronizers.put(partition, streamGroup);
    }

    public void addRecords(TopicPartition partition, Iterable<ConsumerRecord<byte[], byte[]>> records) {
        streamSynchronizers.get(partition).addRecords(partition, records.iterator());
    }

}
