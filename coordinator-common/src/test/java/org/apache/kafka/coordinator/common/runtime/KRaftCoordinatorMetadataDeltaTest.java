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
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;

import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KRaftCoordinatorMetadataDeltaTest {

    @Test
    public void testKRaftCoordinatorDeltaWithNulls() {
        assertThrows(NullPointerException.class, () -> new KRaftCoordinatorMetadataDelta(null));
    }

    @Test
    public void testKRaftCoordinatorDelta() {
        Uuid topicId = Uuid.randomUuid();
        String topicName = "test-topic";
        Uuid topicId2 = Uuid.randomUuid();
        String topicName2 = "test-topic2";
        Uuid deletedTopicId = Uuid.randomUuid();
        String deletedTopicName = "deleted-topic";
        Uuid changedTopicId = Uuid.randomUuid();
        String changedTopicName = "changed-topic";

        MetadataImage image = new MetadataImageBuilder()
            .addTopic(deletedTopicId, deletedTopicName, 1)
            .addTopic(changedTopicId, changedTopicName, 1)
            .build();
        MetadataDelta delta = new MetadataDelta(image);
        delta.replay(new TopicRecord().setTopicId(topicId).setName(topicName));
        delta.replay(new TopicRecord().setTopicId(topicId2).setName(topicName2));
        delta.replay(new RemoveTopicRecord().setTopicId(deletedTopicId));
        delta.replay(new PartitionChangeRecord().setTopicId(changedTopicId).setPartitionId(0));

        KRaftCoordinatorMetadataDelta coordinatorDelta = new KRaftCoordinatorMetadataDelta(delta);

        // created topics
        Collection<Uuid> createdTopicIds = coordinatorDelta.createdTopicIds();
        assertNotNull(createdTopicIds);
        assertEquals(2, createdTopicIds.size());
        assertTrue(createdTopicIds.contains(topicId));
        assertTrue(createdTopicIds.contains(topicId2));

        // deleted topics
        Set<Uuid> deletedTopicIds = coordinatorDelta.deletedTopicIds();
        assertNotNull(deletedTopicIds);
        assertEquals(1, deletedTopicIds.size());
        assertTrue(deletedTopicIds.contains(deletedTopicId));

        // changed topics (also includes created topics)
        Collection<Uuid> changedTopicIds = coordinatorDelta.changedTopicIds();
        assertNotNull(changedTopicIds);
        assertEquals(3, changedTopicIds.size());
        assertTrue(changedTopicIds.contains(changedTopicId));
        assertTrue(changedTopicIds.contains(topicId));
        assertTrue(changedTopicIds.contains(topicId2));

        CoordinatorMetadataImage coordinatorImage = coordinatorDelta.image();
        // the image only contains the original topics, not the new topics yet since we never called delta.apply()
        assertNotNull(coordinatorImage);
        assertEquals(Set.of(deletedTopicName, changedTopicName), coordinatorImage.topicNames());

        // the image contains the correct topics after calling apply
        MetadataImage imageAfterApply = delta.apply(new MetadataProvenance(123, 0, 0L, true));
        CoordinatorMetadataImage coordinatorImageApply = new KRaftCoordinatorMetadataImage(imageAfterApply);
        assertNotNull(coordinatorImageApply);
        assertEquals(Set.of(topicName, topicName2, changedTopicName), coordinatorImageApply.topicNames());
    }

    @Test
    public void testEqualsAndHashcode() {
        Uuid topicId = Uuid.randomUuid();
        String topicName = "test-topic";
        Uuid topicId2 = Uuid.randomUuid();
        String topicName2 = "test-topic2";
        Uuid topicId3 = Uuid.randomUuid();
        String topicName3 = "test-topic3";

        MetadataDelta delta = new MetadataDelta(MetadataImage.EMPTY);
        delta.replay(new TopicRecord().setTopicId(topicId).setName(topicName));
        delta.replay(new TopicRecord().setTopicId(topicId2).setName(topicName2));

        KRaftCoordinatorMetadataDelta coordinatorDelta = new KRaftCoordinatorMetadataDelta(delta);
        KRaftCoordinatorMetadataDelta coordinatorDeltaCopy = new KRaftCoordinatorMetadataDelta(delta);

        MetadataDelta delta2 = new MetadataDelta(MetadataImage.EMPTY);
        delta.replay(new TopicRecord().setTopicId(topicId3).setName(topicName3));
        KRaftCoordinatorMetadataDelta coordinatorDelta2 = new KRaftCoordinatorMetadataDelta(delta2);

        assertEquals(coordinatorDelta, coordinatorDeltaCopy);
        assertEquals(coordinatorDelta.hashCode(), coordinatorDeltaCopy.hashCode());
        assertNotEquals(coordinatorDelta, coordinatorDelta2);
        assertNotEquals(coordinatorDelta.hashCode(), coordinatorDelta2.hashCode());
    }
}
