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
import org.apache.kafka.coordinator.group.MetadataImageBuilder;
import org.apache.kafka.image.MetadataImage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SubscribedTopicMetadataTest {

    private Set<Uuid> subscriptionTopicIdSet;
    private SubscribedTopicDescriberImpl subscribedTopicMetadata;
    private MetadataImage metadataImage;
    private final int numPartitions = 5;

    @BeforeEach
    public void setUp() {
        MetadataImageBuilder metadataImageBuilder = new MetadataImageBuilder();
        IntStream.range(0, 5).forEach(i -> {
            Uuid topicId = Uuid.randomUuid();
            String topicName = "topic" + i;
            metadataImageBuilder.addTopic(topicId, topicName, numPartitions);
        });
        metadataImageBuilder.addRacks();
        metadataImage = metadataImageBuilder.addRacks().build();

        subscriptionTopicIdSet = metadataImage.topics().topicsById().keySet();
        subscribedTopicMetadata = new SubscribedTopicDescriberImpl(subscriptionTopicIdSet, metadataImage);
    }

    @Test
    public void testAttribute() {
        assertEquals(subscriptionTopicIdSet, subscribedTopicMetadata.subscriptionTopicIdSet());
        assertEquals(metadataImage, subscribedTopicMetadata.metadataImage());
    }

    @Test
    public void testTopicMetadataCannotBeNull() {
        assertThrows(NullPointerException.class, () -> new SubscribedTopicDescriberImpl(null, metadataImage));
    }

    @Test
    public void testMetadataImageCannotBeNull() {
        assertThrows(NullPointerException.class, () -> new SubscribedTopicDescriberImpl(subscriptionTopicIdSet, null));
    }

    @Test
    public void testNumberOfPartitions() {
        Uuid topicId = Uuid.randomUuid();

        // Test -1 is returned when the topic Id doesn't exist.
        assertEquals(-1, subscribedTopicMetadata.numPartitions(topicId));

        // Test that the correct number of partitions are returned for a given topic Id.
        subscriptionTopicIdSet.forEach(id ->
            // Test that the correct number of partitions are returned for a given topic Id.
            assertEquals(numPartitions, subscribedTopicMetadata.numPartitions(id))
        );
    }

    @Test
    public void testRacksForPartition() {
        Uuid topicId = Uuid.randomUuid();

        // Test empty set is returned when the topic Id doesn't exist.
        assertEquals(Set.of(), subscribedTopicMetadata.racksForPartition(topicId, 0));

        // Test that the correct number of partitions are returned for a given topic Id.
        subscriptionTopicIdSet.forEach(id -> {
            // Test empty set is returned when the partition Id doesn't exist.
            assertEquals(Set.of(), subscribedTopicMetadata.racksForPartition(id, 10));

            // Test that the correct racks of partition are returned for a given topic Id.
            assertEquals(Set.of("rack0", "rack1"), subscribedTopicMetadata.racksForPartition(id, 0));
        });
    }

    @Test
    public void testEquals() {
        assertEquals(new SubscribedTopicDescriberImpl(subscriptionTopicIdSet, metadataImage), subscribedTopicMetadata);

        Set<Uuid> subscriptionTopicIdSet2 = new HashSet<>();
        Uuid topicId = Uuid.randomUuid();
        MetadataImage metadataImage2 = new MetadataImageBuilder()
            .addTopic(topicId, "newTopic", 5)
            .addRacks()
            .build();
        subscriptionTopicIdSet2.add(topicId);
        assertNotEquals(new SubscribedTopicDescriberImpl(subscriptionTopicIdSet2, metadataImage2), subscribedTopicMetadata);
    }
}
