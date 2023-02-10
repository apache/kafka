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

package org.apache.kafka.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.Objects;
import org.junit.jupiter.api.Test;

class TopicIdPartitionTest {

    private final Uuid topicId0 = new Uuid(-4883993789924556279L, -5960309683534398572L);
    private final String topicName0 = "a_topic_name";
    private final int partition1 = 1;
    private final TopicPartition topicPartition0 = new TopicPartition(topicName0, partition1);
    private final TopicIdPartition topicIdPartition0 = new TopicIdPartition(topicId0, topicPartition0);
    private final TopicIdPartition topicIdPartition1 = new TopicIdPartition(topicId0,
        partition1, topicName0);

    private final TopicIdPartition topicIdPartitionWithNullTopic0 = new TopicIdPartition(topicId0,
        partition1, null);
    private final TopicIdPartition topicIdPartitionWithNullTopic1 = new TopicIdPartition(topicId0,
        new TopicPartition(null, partition1));

    private final Uuid topicId1 = new Uuid(7759286116672424028L, -5081215629859775948L);
    private final String topicName1 = "another_topic_name";
    private final TopicIdPartition topicIdPartition2 = new TopicIdPartition(topicId1,
        partition1, topicName1);
    private final TopicIdPartition topicIdPartitionWithNullTopic2 = new TopicIdPartition(topicId1,
        new TopicPartition(null, partition1));

    @Test
    public void testEquals() {
        assertEquals(topicIdPartition0, topicIdPartition1);
        assertEquals(topicIdPartition1, topicIdPartition0);
        assertEquals(topicIdPartitionWithNullTopic0, topicIdPartitionWithNullTopic1);

        assertNotEquals(topicIdPartition0, topicIdPartition2);
        assertNotEquals(topicIdPartition2, topicIdPartition0);
        assertNotEquals(topicIdPartition0, topicIdPartitionWithNullTopic0);
        assertNotEquals(topicIdPartitionWithNullTopic0, topicIdPartitionWithNullTopic2);
    }

    @Test
    public void testHashCode() {
        assertEquals(Objects.hash(topicIdPartition0.topicId(), topicIdPartition0.topicPartition()),
            topicIdPartition0.hashCode());
        assertEquals(topicIdPartition0.hashCode(), topicIdPartition1.hashCode());

        assertEquals(Objects.hash(topicIdPartitionWithNullTopic0.topicId(),
            new TopicPartition(null, partition1)), topicIdPartitionWithNullTopic0.hashCode());
        assertEquals(topicIdPartitionWithNullTopic0.hashCode(), topicIdPartitionWithNullTopic1.hashCode());

        assertNotEquals(topicIdPartition0.hashCode(), topicIdPartition2.hashCode());
        assertNotEquals(topicIdPartition0.hashCode(), topicIdPartitionWithNullTopic0.hashCode());
        assertNotEquals(topicIdPartitionWithNullTopic0.hashCode(), topicIdPartitionWithNullTopic2.hashCode());
    }

    @Test
    public void testToString() {
        assertEquals("vDiRhkpVQgmtSLnsAZx7lA:a_topic_name-1", topicIdPartition0.toString());
        assertEquals("vDiRhkpVQgmtSLnsAZx7lA:null-1", topicIdPartitionWithNullTopic0.toString());
    }

}
