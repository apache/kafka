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
package org.apache.kafka.clients;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestTestUtils;

import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.Collections;
import java.util.Optional;
import java.util.Arrays;


public class MetadataTestTopic {
    private final String topic1 = "topic1";
    private final String topic2 = "topic2";
    private final Set<String> internalTopics;
    private final Map<String, Uuid> topicIds;
    private final Map<String, Integer> topicPartitionCounts;
    private final TopicPartition tp11;
    private final TopicPartition tp21;
    private final RequestTestUtils.PartitionMetadataSupplier metadataSupplier;
    private MetadataResponse.PartitionMetadata part11Metadata;
    private MetadataResponse.PartitionMetadata part12Metadata;
    private MetadataResponse.PartitionMetadata part2Metadata;
    private MetadataResponse.PartitionMetadata internalTopicMetadata;

    public MetadataTestTopic() {
        Set<String> internalTopics = Collections.singleton(Topic.GROUP_METADATA_TOPIC_NAME);

        TopicPartition tp11 = new TopicPartition(topic1, 0);
        TopicPartition tp12 = new TopicPartition(topic1, 1);
        TopicPartition tp21 = new TopicPartition(topic2, 0);
        TopicPartition internalPart = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0);

        MetadataResponse.PartitionMetadata part11Metadata = new MetadataResponse.PartitionMetadata(Errors.NONE, tp11, Optional.of(1), Optional.of(100), Arrays.asList(1, 2), Arrays.asList(1, 2), Arrays.asList(3));
        MetadataResponse.PartitionMetadata part12Metadata = new MetadataResponse.PartitionMetadata(Errors.NONE, tp12, Optional.of(2), Optional.of(200), Arrays.asList(2, 3), Arrays.asList(2, 3), Arrays.asList(1));
        MetadataResponse.PartitionMetadata part2Metadata = new MetadataResponse.PartitionMetadata(Errors.NONE, tp21, Optional.of(2), Optional.of(200), Arrays.asList(2, 3), Arrays.asList(2, 3), Arrays.asList(1));
        MetadataResponse.PartitionMetadata internalTopicMetadata = new MetadataResponse.PartitionMetadata(Errors.NONE, internalPart, Optional.of(2), Optional.of(200), Arrays.asList(2, 3), Arrays.asList(2, 3), Arrays.asList(1));


        Uuid topic1Id = Uuid.randomUuid();
        Uuid topic2Id = Uuid.randomUuid();
        Uuid internalTopicId = Uuid.randomUuid();


        Map<String, Uuid> topicIds = new HashMap<>();
        topicIds.put(topic1, topic1Id);
        topicIds.put(topic2, topic2Id);
        topicIds.put(internalTopics.iterator().next(), internalTopicId);

        Map<String, Integer> topicPartitionCounts = new HashMap<>();
        topicPartitionCounts.put(topic1, 2);
        topicPartitionCounts.put(topic2, 1);
        topicPartitionCounts.put(internalTopics.iterator().next(), 1);

        RequestTestUtils.PartitionMetadataSupplier metadataSupplier = (error, partition, leaderId, leaderEpoch, replicas, isr, offlineReplicas) -> {
            if (partition.equals(tp11))
                return part11Metadata;
            else if (partition.equals(tp21))
                return part2Metadata;
            else if (partition.equals(tp12))
                return part12Metadata;
            else if (partition.equals(internalPart))
                return internalTopicMetadata;
            throw new RuntimeException("Unexpected partition " + partition);
        };

        this.topicIds = topicIds;
        this.topicPartitionCounts = topicPartitionCounts;
        this.tp11 = tp11;
        this.tp21 = tp21;
        this.metadataSupplier = metadataSupplier;
        this.internalTopics = internalTopics;
        this.part11Metadata = part11Metadata;
        this.part12Metadata = part12Metadata;
        this.part2Metadata = part2Metadata;
        this.internalTopicMetadata = internalTopicMetadata;
    }
    public String getTopic1() {
        return topic1;
    }
    public String getTopic2() {
        return topic2;
    }
    public Set<String> getInternalTopics() {
        return internalTopics;
    }
    public Map<String, Uuid> getTopicIds() {
        return topicIds;
    }
    public Map<String, Integer> getTopicPartitionCounts() {
        return topicPartitionCounts;
    }
    public TopicPartition getTp11() {
        return tp11;
    }
    public TopicPartition getTp21() {
        return tp21;
    }
    public RequestTestUtils.PartitionMetadataSupplier getMetadataSupplier() {
        return metadataSupplier;
    }
    public MetadataResponse.PartitionMetadata getPart11Metadata() {
        return part11Metadata;
    }
    public MetadataResponse.PartitionMetadata getPart12Metadata() {
        return part12Metadata;
    }
    public MetadataResponse.PartitionMetadata getPart2Metadata() {
        return part2Metadata;
    }
    public MetadataResponse.PartitionMetadata getInternalTopicMetadata() {
        return internalTopicMetadata;
    }
}
