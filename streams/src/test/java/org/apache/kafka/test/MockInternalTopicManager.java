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
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.internals.InternalTopicConfig;
import org.apache.kafka.streams.processor.internals.InternalTopicManager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MockInternalTopicManager extends InternalTopicManager {

    public final Map<String, Integer> readyTopics = new HashMap<>();
    private final MockConsumer<byte[], byte[]> restoreConsumer;
    private final boolean mockCreateInternalTopics;

    public MockInternalTopicManager(final Time time,
                                    final StreamsConfig streamsConfig,
                                    final MockConsumer<byte[], byte[]> restoreConsumer,
                                    final boolean mockCreateInternalTopics) {
        super(time, new MockClientSupplier().getAdmin(streamsConfig.originals()), streamsConfig);

        this.restoreConsumer = restoreConsumer;
        this.mockCreateInternalTopics = mockCreateInternalTopics;
    }

    @Override
    public Set<String> makeReady(final Map<String, InternalTopicConfig> topics) {
        for (final InternalTopicConfig topic : topics.values()) {
            final String topicName = topic.name();
            final int numberOfPartitions = topic.numberOfPartitions().get();
            readyTopics.put(topicName, numberOfPartitions);

            final List<PartitionInfo> partitions = new ArrayList<>();
            for (int i = 0; i < numberOfPartitions; i++) {
                partitions.add(new PartitionInfo(topicName, i, null, null, null));
            }

            restoreConsumer.updatePartitions(topicName, partitions);
        }
        return mockCreateInternalTopics ? topics.keySet() : Collections.emptySet();
    }

    @Override
    protected Map<String, Integer> getNumPartitions(final Set<String> topics,
                                                    final Set<String> tempUnknownTopics) {
        final Map<String, Integer> partitions = new HashMap<>();
        for (final String topic : topics) {
            partitions.put(topic, restoreConsumer.partitionsFor(topic) == null ?  null : restoreConsumer.partitionsFor(topic).size());
        }

        return partitions;
    }
}
