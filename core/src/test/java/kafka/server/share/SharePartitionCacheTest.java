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
package kafka.server.share;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.share.SharePartitionKey;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SharePartitionCacheTest {

    private static final String GROUP_ID = "test-group";
    private static final Uuid TOPIC_ID = Uuid.randomUuid();
    private static final TopicIdPartition TOPIC_ID_PARTITION = new TopicIdPartition(TOPIC_ID, new TopicPartition("test-topic", 1));
    private static final SharePartitionKey SHARE_PARTITION_KEY = new SharePartitionKey(GROUP_ID, TOPIC_ID_PARTITION);

    private SharePartitionCache cache;

    @BeforeEach
    public void setUp() {
        cache = new SharePartitionCache();
    }

    @Test
    public void testComputeIfAbsent() {
        // Test computeIfAbsent when key doesn't exist
        SharePartition sharePartition = Mockito.mock(SharePartition.class);
        SharePartition newPartition = cache.computeIfAbsent(SHARE_PARTITION_KEY, key -> sharePartition);

        assertEquals(sharePartition, newPartition);
        assertEquals(sharePartition, cache.get(SHARE_PARTITION_KEY));
        assertEquals(1, cache.groups().size());

        // Test computeIfAbsent when key exists
        SharePartition anotherPartition = Mockito.mock(SharePartition.class);
        SharePartition existingPartition = cache.computeIfAbsent(SHARE_PARTITION_KEY, key -> anotherPartition);
        assertEquals(sharePartition, existingPartition);
        assertEquals(sharePartition, cache.get(SHARE_PARTITION_KEY));
        assertEquals(1, cache.groups().size());
    }

    @Test
    public void testRemoveGroup() {
        // Add partitions for multiple groups
        String group1 = "group1";
        String group2 = "group2";
        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("test-topic1", 1));
        TopicIdPartition tp2 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("test-topic2", 2));
        TopicIdPartition tp3 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("test-topic3", 3));
        // Group1 with 2 partitions.
        SharePartitionKey key1 = new SharePartitionKey(group1, tp1);
        SharePartitionKey key2 = new SharePartitionKey(group1, tp2);
        // Group2 with 1 partition.
        SharePartitionKey key3 = new SharePartitionKey(group2, tp3);
        SharePartition sp1 = Mockito.mock(SharePartition.class);
        SharePartition sp2 = Mockito.mock(SharePartition.class);
        SharePartition sp3 = Mockito.mock(SharePartition.class);

        // Test computeIfAbsent adds to group map
        cache.computeIfAbsent(key1, k -> sp1);
        cache.computeIfAbsent(key2, k -> sp2);
        cache.computeIfAbsent(key3, k -> sp3);

        // Verify partitions are in the cache.
        assertEquals(3, cache.size());
        assertTrue(cache.containsKey(key1));
        assertTrue(cache.containsKey(key2));
        assertTrue(cache.containsKey(key3));
        // Verify groups are in the group map.
        assertEquals(2, cache.groups().size());
        assertTrue(cache.groups().containsKey(group1));
        assertTrue(cache.groups().containsKey(group2));
        // Verify topic partitions are in the group map.
        assertEquals(2, cache.groups().get(group1).size());
        assertEquals(1, cache.groups().get(group2).size());
        assertEquals(1, cache.groups().get(group1).stream().filter(tp -> tp.equals(tp1)).count());
        assertEquals(1, cache.groups().get(group1).stream().filter(tp -> tp.equals(tp2)).count());
        assertEquals(1, cache.groups().get(group2).stream().filter(tp -> tp.equals(tp3)).count());

        // Remove one group and verify only its partitions are removed.
        cache.topicIdPartitionsForGroup(group1).forEach(
            topicIdPartition -> cache.remove(new SharePartitionKey(group1, topicIdPartition)));
        assertEquals(1, cache.size());
        assertTrue(cache.containsKey(key3));
        assertEquals(1, cache.groups().size());
        assertTrue(cache.groups().containsKey(group2));
    }
} 