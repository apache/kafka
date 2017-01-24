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

import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.internals.InternalTopicConfig;
import org.apache.kafka.streams.processor.internals.InternalTopicManager;
import org.apache.kafka.streams.processor.internals.StreamsKafkaClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MockInternalTopicManager extends InternalTopicManager {

    public Map<String, Integer> readyTopics = new HashMap<>();
    public MockConsumer<byte[], byte[]> restoreConsumer;

    public MockInternalTopicManager(StreamsConfig streamsConfig, MockConsumer<byte[], byte[]> restoreConsumer) {
        super(new StreamsKafkaClient(streamsConfig), 0, 0);

        this.restoreConsumer = restoreConsumer;
    }

    @Override
    public void makeReady(InternalTopicConfig topic, int numPartitions) {
        readyTopics.put(topic.name(), numPartitions);

        List<PartitionInfo> partitions = new ArrayList<>();
        for (int i = 0; i < numPartitions; i++) {
            partitions.add(new PartitionInfo(topic.name(), i, null, null, null));
        }

        restoreConsumer.updatePartitions(topic.name(), partitions);
    }

}