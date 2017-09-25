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
package org.apache.kafka.test;

import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.internals.InternalTopicConfig;
import org.apache.kafka.streams.processor.internals.InternalTopicManager;
import org.apache.kafka.streams.processor.internals.StreamsKafkaClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;



public class MockInternalTopicManager extends InternalTopicManager {

    public Map<String, Integer> readyTopics = new HashMap<>();
    private MockConsumer<byte[], byte[]> restoreConsumer;

    public MockInternalTopicManager(StreamsConfig streamsConfig, MockConsumer<byte[], byte[]> restoreConsumer) {
        super(StreamsKafkaClient.create(streamsConfig), 0, 0, new MockTime());

        this.restoreConsumer = restoreConsumer;
    }

    @Override
    public void makeReady(final Map<InternalTopicConfig, Integer> topics) {
        for (Map.Entry<InternalTopicConfig, Integer> entry : topics.entrySet()) {
            readyTopics.put(entry.getKey().name(), entry.getValue());

            final List<PartitionInfo> partitions = new ArrayList<>();
            for (int i = 0; i < entry.getValue(); i++) {
                partitions.add(new PartitionInfo(entry.getKey().name(), i, null, null, null));
            }

            restoreConsumer.updatePartitions(entry.getKey().name(), partitions);
        }
    }

    @Override
    public Map<String, Integer> getNumPartitions(final Set<String> topics) {
        final Map<String, Integer> partitions = new HashMap<>();
        for (String topic : topics) {
            partitions.put(topic, restoreConsumer.partitionsFor(topic) == null ?  null : restoreConsumer.partitionsFor(topic).size());
        }

        return partitions;
    }
}