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
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SubscribedTopicMetadataTest {

    private SubscribedTopicDescriberImpl subscribedTopicMetadata;
    private CoordinatorMetadataImage metadataImage;
    private final int numPartitions = 5;

    @BeforeEach
    public void setUp() {
        MetadataImageBuilder metadataImageBuilder = new MetadataImageBuilder();
        for (int i = 0; i < 5; i++) {
            Uuid topicId = Uuid.randomUuid();
            String topicName = "topic" + i;
            metadataImageBuilder.addTopic(topicId, topicName, numPartitions);
        }
        metadataImage = metadataImageBuilder.addRacks().buildCoordinatorMetadataImage();

        subscribedTopicMetadata = new SubscribedTopicDescriberImpl(metadataImage);
    }

    @Test
    public void testMetadataImageCannotBeNull() {
        assertThrows(NullPointerException.class, () -> new SubscribedTopicDescriberImpl(null));
    }

    @Test
    public void testNumberOfPartitions() {
        Uuid topicId = Uuid.randomUuid();

        // Test -1 is returned when the topic ID doesn't exist.
        assertEquals(-1, subscribedTopicMetadata.numPartitions(topicId));

        // Test that the correct number of partitions are returned for a given topic ID.
        metadataImage.topicIds().forEach(id ->
            // Test that the correct number of partitions are returned for a given topic ID.
            assertEquals(numPartitions, subscribedTopicMetadata.numPartitions(id))
        );
    }

    @Test
    public void testRacksForPartition() {
        Uuid topicId = Uuid.randomUuid();

        // Test empty set is returned when the topic ID doesn't exist.
        assertEquals(Set.of(), subscribedTopicMetadata.racksForPartition(topicId, 0));
        metadataImage.topicIds().forEach(id -> {
            // Test empty set is returned when the partition ID doesn't exist.
            assertEquals(Set.of(), subscribedTopicMetadata.racksForPartition(id, 10));

            // Test that the correct racks of partition are returned for a given topic ID.
            assertEquals(Set.of("rack0", "rack1"), subscribedTopicMetadata.racksForPartition(id, 0));
        });
    }

    @Test
    public void testEquals() {
        assertEquals(new SubscribedTopicDescriberImpl(metadataImage), subscribedTopicMetadata);

        Uuid topicId = Uuid.randomUuid();
        CoordinatorMetadataImage metadataImage2 = new MetadataImageBuilder()
            .addTopic(topicId, "newTopic", 5)
            .addRacks()
            .buildCoordinatorMetadataImage();
        assertNotEquals(new SubscribedTopicDescriberImpl(metadataImage2), subscribedTopicMetadata);
    }
}
