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

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpersTest.mkMapOfPartitionRacks;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TopicMetadataTest {
    @Test
    public void testAttributes() {
        Uuid topicId = Uuid.randomUuid();
        Map<Integer, Set<String>> partitionRacks = mkMapOfPartitionRacks(15);
        TopicMetadata topicMetadata = new TopicMetadata(topicId, "foo", 15, partitionRacks);

        assertEquals(topicId, topicMetadata.id());
        assertEquals("foo", topicMetadata.name());
        assertEquals(15, topicMetadata.numPartitions());
        assertEquals(partitionRacks, topicMetadata.partitionRacks());
    }

    @Test
    public void testTopicIdAndNameCannotBeNull() {
        assertThrows(NullPointerException.class, () -> new TopicMetadata(Uuid.randomUuid(), null, 15));
        assertThrows(NullPointerException.class, () -> new TopicMetadata(null, "foo", 15));
    }

    @Test
    public void testEquals() {
        Uuid topicId = Uuid.randomUuid();
        Map<Integer, Set<String>> partitionRacks = mkMapOfPartitionRacks(15);
        TopicMetadata topicMetadata = new TopicMetadata(topicId, "foo", 15);

        assertEquals(new TopicMetadata(topicId, "foo", 15), topicMetadata);
        assertNotEquals(new TopicMetadata(topicId, "foo", 5), topicMetadata);
    }
}
