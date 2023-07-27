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
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.coordinator.group.RecordHelpersTest.mkMapOfPartitionRacks;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SubscribedTopicMetadataTest {

    @Test
    public void testAttribute() {
        Map<Uuid, TopicMetadata> topicMetadataMap = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            Uuid topicId = Uuid.randomUuid();
            String topicName = "topic" + i;
            Map<Integer, Set<String>> partitionRacks = mkMapOfPartitionRacks(5);
            topicMetadataMap.put(
                topicId,
                new TopicMetadata(topicId, topicName, 5, partitionRacks)
            );
        }
        assertEquals(topicMetadataMap, new SubscribedTopicMetadata(topicMetadataMap).topicMetadata());
    }

    @Test
    public void testTopicMetadataCannotBeNull() {
        assertThrows(NullPointerException.class, () -> new SubscribedTopicMetadata(null));
    }

    @Test
    public void testEquals() {
        Map<Uuid, TopicMetadata> topicMetadataMap = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            Uuid topicId = Uuid.randomUuid();
            String topicName = "topic" + i;
            Map<Integer, Set<String>> partitionRacks = mkMapOfPartitionRacks(5);
            topicMetadataMap.put(
                topicId,
                new TopicMetadata(topicId, topicName, 5, partitionRacks)
            );
        }
        SubscribedTopicMetadata subscribedTopicMetadata = new SubscribedTopicMetadata(topicMetadataMap);
        assertEquals(new SubscribedTopicMetadata(topicMetadataMap), subscribedTopicMetadata);

        Map<Uuid, TopicMetadata> topicMetadataMap2 = new HashMap<>();
        Uuid topicId = Uuid.randomUuid();
        topicMetadataMap2.put(topicId, new TopicMetadata(topicId, "newTopic", 5, Collections.emptyMap()));
        assertNotEquals(new SubscribedTopicMetadata(topicMetadataMap2), subscribedTopicMetadata);
    }
}
