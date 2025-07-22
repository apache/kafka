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
package org.apache.kafka.coordinator.common.runtime;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.image.MetadataImage;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

class KRaftCoordinatorMetadataImageTest {

    @Test
    public void testKRaftCoordinatorMetadataImage() {
        Uuid topicId = Uuid.randomUuid();
        String topicName = "test-topic";
        int partitionCount = 2;
        Uuid topicId2 = Uuid.randomUuid();
        String topicName2 = "test-topic2";
        int partitionCount2 = 4;
        Uuid noPartitionTopicId = Uuid.randomUuid();
        String noPartitionTopic = "no-partition-topic";
        long imageVersion = 123L;

        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topicId, topicName, partitionCount)
            .addTopic(topicId2, topicName2, partitionCount2)
            .addTopic(noPartitionTopicId, noPartitionTopic, 0)
            .addRacks()
            .build(imageVersion);

        KRaftCoordinatorMetadataImage image = new KRaftCoordinatorMetadataImage(metadataImage);

        assertEquals(Set.of(topicName, topicName2, noPartitionTopic), image.topicNames());
        assertEquals(Set.of(topicId, topicId2, noPartitionTopicId), image.topicIds());

        image.topicMetadata(topicName).ifPresentOrElse(
            topicMetadata -> {
                assertEquals(topicName, topicMetadata.name());
                assertEquals(topicId, topicMetadata.id());
                assertEquals(partitionCount, topicMetadata.partitionCount());
                List<String> racks0 = topicMetadata.partitionRacks(0);
                List<String> racks1 = topicMetadata.partitionRacks(1);
                assertEquals(2, racks0.size());
                assertEquals(2, racks1.size());
                assertEquals("rack0", racks0.get(0));
                assertEquals("rack1", racks0.get(1));
                assertEquals("rack1", racks1.get(0));
                assertEquals("rack2", racks1.get(1));
            },
            () -> fail("Expected topic metadata for " + topicName)
        );

        image.topicMetadata(topicName2).ifPresentOrElse(
            topicMetadata -> {
                assertEquals(topicName2, topicMetadata.name());
                assertEquals(topicId2, topicMetadata.id());
                assertEquals(partitionCount2, topicMetadata.partitionCount());
                List<String> racks0 = topicMetadata.partitionRacks(0);
                List<String> racks1 = topicMetadata.partitionRacks(1);
                assertEquals(2, racks0.size());
                assertEquals(2, racks1.size());
                assertEquals("rack0", racks0.get(0));
                assertEquals("rack1", racks0.get(1));
                assertEquals("rack1", racks1.get(0));
                assertEquals("rack2", racks1.get(1));
            },
            () -> fail("Expected topic metadata for " + topicName)
        );

        image.topicMetadata(noPartitionTopic).ifPresentOrElse(
            topicMetadata -> {
                assertEquals(noPartitionTopic, topicMetadata.name());
                assertEquals(noPartitionTopicId, topicMetadata.id());
                assertEquals(0, topicMetadata.partitionCount());
                List<String> racks = topicMetadata.partitionRacks(0);
                assertEquals(0, racks.size());
            },
            () -> fail("Expected topic metadata for " + topicName)
        );

        assertNotNull(image.emptyDelta());

        assertEquals(metadataImage.offset(), image.version());
        assertEquals(imageVersion, image.version());

        assertFalse(image.isEmpty());
    }

    @Test
    public void testEqualsAndHashcode() {
        Uuid topicId = Uuid.randomUuid();
        String topicName = "test-topic";
        int partitionCount = 2;
        Uuid topicId2 = Uuid.randomUuid();
        String topicName2 = "test-topic2";
        int partitionCount2 = 4;
        long imageVersion = 123L;

        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topicId, topicName, partitionCount)
            .addRacks()
            .build(imageVersion);

        KRaftCoordinatorMetadataImage coordinatorMetadataImage = new KRaftCoordinatorMetadataImage(metadataImage);
        KRaftCoordinatorMetadataImage coordinatorMetadataImageCopy = new KRaftCoordinatorMetadataImage(metadataImage);

        MetadataImage metadataImage2 = new MetadataImageBuilder()
            .addTopic(topicId2, topicName2, partitionCount2)
            .addRacks()
            .build(imageVersion);

        KRaftCoordinatorMetadataImage coordinatorMetadataImage2 = new KRaftCoordinatorMetadataImage(metadataImage2);

        assertEquals(coordinatorMetadataImage, coordinatorMetadataImageCopy);
        assertNotEquals(coordinatorMetadataImage, coordinatorMetadataImage2);

        assertEquals(coordinatorMetadataImage.hashCode(), coordinatorMetadataImageCopy.hashCode());
        assertNotEquals(coordinatorMetadataImage.hashCode(), coordinatorMetadataImage2.hashCode());
    }
}
