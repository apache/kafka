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

package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TopicIdPartitionSetTest {

    private TopicIdPartitionSet topicIdPartitionSet;


    @BeforeEach
    public void setUp() {
        topicIdPartitionSet = new TopicIdPartitionSet();
    }

    @Test
    public void testIsEmpty() {
        assertTrue(topicIdPartitionSet.isEmpty());

        TopicIdPartition topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        topicIdPartitionSet.add(topicIdPartition);

        assertFalse(topicIdPartitionSet.isEmpty());
    }

    @Test
    public void testRetrieveTopicPartitions() {
        TopicPartition tp1 = new TopicPartition("foo", 0);
        TopicPartition tp2 = new TopicPartition("foo", 1);
        TopicPartition tp3 = new TopicPartition("bar", 0);
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();
        topicIdPartitionSet.add(new TopicIdPartition(topicId1, tp1));
        topicIdPartitionSet.add(new TopicIdPartition(topicId1, tp2));
        topicIdPartitionSet.add(new TopicIdPartition(topicId2, tp3));

        Set<TopicPartition> topicPartitionSet = topicIdPartitionSet.topicPartitions();
        assertEquals(3, topicPartitionSet.size());
        assertTrue(topicPartitionSet.contains(tp1));
        assertTrue(topicPartitionSet.contains(tp2));
        assertTrue(topicPartitionSet.contains(tp3));
    }

    @Test
    public void testRetrieveTopicIds() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();
        topicIdPartitionSet.add(new TopicIdPartition(topicId1, new TopicPartition("foo", 0)));
        topicIdPartitionSet.add(new TopicIdPartition(topicId1, new TopicPartition("foo", 1)));
        topicIdPartitionSet.add(new TopicIdPartition(topicId2, new TopicPartition("bar", 0)));

        Set<Uuid> topicIds = topicIdPartitionSet.topicIds();
        assertEquals(2, topicIds.size());
        assertTrue(topicIds.contains(topicId1));
        assertTrue(topicIds.contains(topicId2));
    }

    @Test
    public void testRetrieveTopicNames() {
        String topic1 = "foo";
        String topic2 = "bar";
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();
        topicIdPartitionSet.add(new TopicIdPartition(topicId1, new TopicPartition(topic1, 0)));
        topicIdPartitionSet.add(new TopicIdPartition(topicId1, new TopicPartition(topic1, 1)));
        topicIdPartitionSet.add(new TopicIdPartition(topicId2, new TopicPartition(topic2, 0)));

        Set<String> topicNames = topicIdPartitionSet.topicNames();
        assertEquals(2, topicNames.size());
        assertTrue(topicNames.contains(topic1));
        assertTrue(topicNames.contains(topic2));
    }

    @Test
    public void testRetrievedTopicNamesAreSorted() {
        LinkedHashSet<TopicIdPartition> expectedOrderedTopicPartitions = new LinkedHashSet<>();
        expectedOrderedTopicPartitions.add(new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("topic-z", 1)));
        expectedOrderedTopicPartitions.add(new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("topic-z", 0)));
        expectedOrderedTopicPartitions.add(new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("topic-a", 0)));
        expectedOrderedTopicPartitions.add(new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("topic-a", 1)));

        List<TopicIdPartition> reversed = new ArrayList<>(expectedOrderedTopicPartitions);
        Collections.reverse(reversed);
        reversed.forEach(tp -> topicIdPartitionSet.add(tp));

        List<TopicPartition> topicPartitions = new ArrayList<>(topicIdPartitionSet.toTopicNamePartitionSet());

        assertEquals(4, topicPartitions.size());
        assertEquals(new TopicPartition("topic-a", 0), topicPartitions.get(0));
        assertEquals(new TopicPartition("topic-a", 1), topicPartitions.get(1));
        assertEquals(new TopicPartition("topic-z", 0), topicPartitions.get(2));
        assertEquals(new TopicPartition("topic-z", 1), topicPartitions.get(3));
    }

    @Test
    public void testToString() {
        Uuid topicId1 = Uuid.randomUuid();
        TopicIdPartition tp1 = new TopicIdPartition(topicId1, new TopicPartition("topic-a", 0));
        TopicIdPartition tp2 = new TopicIdPartition(topicId1, new TopicPartition("topic-a", 1));
        TopicIdPartition tp3 = new TopicIdPartition(topicId1, new TopicPartition("topic-b", 0));
        topicIdPartitionSet.add(tp1);
        topicIdPartitionSet.add(tp2);
        topicIdPartitionSet.add(tp3);

        String toString = topicIdPartitionSet.toString();
        assertEquals(List.of(tp1, tp2, tp3).toString(), toString);
    }

    @Test
    public void testToStringSorted() {
        Uuid topicId1 = Uuid.randomUuid();
        TopicIdPartition tp1 = new TopicIdPartition(topicId1, new TopicPartition("topic-a", 0));
        TopicIdPartition tpz1 = new TopicIdPartition(topicId1, new TopicPartition("topic-z", 0));
        TopicIdPartition tpz2 = new TopicIdPartition(topicId1, new TopicPartition("topic-z", 1));
        topicIdPartitionSet.add(tpz2);
        topicIdPartitionSet.add(tpz1);
        topicIdPartitionSet.add(tp1);

        String toString = topicIdPartitionSet.toString();
        assertEquals(List.of(tp1, tpz1, tpz2).toString(), toString);
    }

}
