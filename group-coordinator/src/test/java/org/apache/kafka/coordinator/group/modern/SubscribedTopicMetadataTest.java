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
package org.apache.kafka.coordinator.group.modern;

import org.apache.kafka.common.Uuid;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SubscribedTopicMetadataTest {

    private Map<Uuid, TopicMetadata> topicMetadataMap;
    private SubscribedTopicDescriberImpl subscribedTopicMetadata;

    @BeforeEach
    public void setUp() {
        topicMetadataMap = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            Uuid topicId = Uuid.randomUuid();
            String topicName = "topic" + i;
            topicMetadataMap.put(
                topicId,
                new TopicMetadata(topicId, topicName, 5)
            );
        }
        subscribedTopicMetadata = new SubscribedTopicDescriberImpl(topicMetadataMap);
    }

    @Test
    public void testAttribute() {
        assertEquals(topicMetadataMap, subscribedTopicMetadata.topicMetadata());
    }

    @Test
    public void testTopicMetadataCannotBeNull() {
        assertThrows(NullPointerException.class, () -> new SubscribedTopicDescriberImpl(null));
    }

    @Test
    public void testNumberOfPartitions() {
        Uuid topicId = Uuid.randomUuid();

        // Test -1 is returned when the topic Id doesn't exist.
        assertEquals(-1, subscribedTopicMetadata.numPartitions(topicId));

        topicMetadataMap.put(topicId, new TopicMetadata(topicId, "topic6", 3));

        // Test that the correct number of partitions are returned for a given topic Id.
        assertEquals(3, subscribedTopicMetadata.numPartitions(topicId));
    }

    @Test
    public void testEquals() {
        assertEquals(new SubscribedTopicDescriberImpl(topicMetadataMap), subscribedTopicMetadata);

        Map<Uuid, TopicMetadata> topicMetadataMap2 = new HashMap<>();
        Uuid topicId = Uuid.randomUuid();
        topicMetadataMap2.put(topicId, new TopicMetadata(topicId, "newTopic", 5));
        assertNotEquals(new SubscribedTopicDescriberImpl(topicMetadataMap2), subscribedTopicMetadata);
    }
}
