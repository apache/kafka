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
package org.apache.kafka.coordinator.group.consumer;

import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.coordinator.group.CoordinatorRecordHelpersTest.mkMapOfPartitionRacks;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SubscribedTopicMetadataTest {

    private Map<Uuid, TopicMetadata> topicMetadataMap;
    private SubscribedTopicMetadata subscribedTopicMetadata;

    @BeforeEach
    public void setUp() {
        topicMetadataMap = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            Uuid topicId = Uuid.randomUuid();
            String topicName = "topic" + i;
            Map<Integer, Set<String>> partitionRacks = mkMapOfPartitionRacks(5);
            topicMetadataMap.put(
                topicId,
                new TopicMetadata(topicId, topicName, 5, partitionRacks)
            );
        }
        subscribedTopicMetadata = new SubscribedTopicMetadata(topicMetadataMap);
    }

    @Test
    public void testAttribute() {
        assertEquals(topicMetadataMap, subscribedTopicMetadata.topicMetadata());
    }

    @Test
    public void testTopicMetadataCannotBeNull() {
        assertThrows(NullPointerException.class, () -> new SubscribedTopicMetadata(null));
    }

    @Test
    public void testNumberOfPartitions() {
        Uuid topicId = Uuid.randomUuid();

        // Test -1 is returned when the topic Id doesn't exist.
        assertEquals(-1, subscribedTopicMetadata.numPartitions(topicId));

        topicMetadataMap.put(topicId, new TopicMetadata(topicId, "topic6", 3, Collections.emptyMap()));

        // Test that the correct number of partitions are returned for a given topic Id.
        assertEquals(3, subscribedTopicMetadata.numPartitions(topicId));
    }

    @Test
    public void testRacksForPartition() {
        Uuid topicId = Uuid.randomUuid();

        // Test that an empty set is returned for a non-existent topic Id.
        assertEquals(Collections.emptySet(), subscribedTopicMetadata.racksForPartition(topicId, 0));

        // Add topic Id with partition racks included.
        Map<Integer, Set<String>> partitionRacks = mkMapOfPartitionRacks(3);
        topicMetadataMap.put(topicId, new TopicMetadata(topicId, "topic6", 3, partitionRacks));

        // Test that an empty set is returned for a non-existent partition Id.
        assertEquals(Collections.emptySet(), subscribedTopicMetadata.racksForPartition(topicId, 4));

        // Test that a correct set of racks is returned for the given topic Id and partition Id.
        assertEquals(partitionRacks.get(2), subscribedTopicMetadata.racksForPartition(topicId, 2));

        // Add another topic Id without partition racks.
        topicId = Uuid.randomUuid();
        topicMetadataMap.put(topicId, new TopicMetadata(topicId, "topic6", 3, Collections.emptyMap()));

        // Test that an empty set is returned when the partition rack info is absent.
        assertEquals(Collections.emptySet(), subscribedTopicMetadata.racksForPartition(topicId, 1));
    }

    @Test
    public void testEquals() {
        assertEquals(new SubscribedTopicMetadata(topicMetadataMap), subscribedTopicMetadata);

        Map<Uuid, TopicMetadata> topicMetadataMap2 = new HashMap<>();
        Uuid topicId = Uuid.randomUuid();
        topicMetadataMap2.put(topicId, new TopicMetadata(topicId, "newTopic", 5, Collections.emptyMap()));
        assertNotEquals(new SubscribedTopicMetadata(topicMetadataMap2), subscribedTopicMetadata);
    }
}
