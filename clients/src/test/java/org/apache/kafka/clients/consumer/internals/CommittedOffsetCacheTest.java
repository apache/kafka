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

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CommittedOffsetCacheTest {

    @Test
    public void testSubscriptionStateShouldNotBeNull() {
        assertThrows(NullPointerException.class, () -> new CommittedOffsetCache(null));
    }

    @Test
    public void testAddToCache() {
        var committedOffsetCache = new CommittedOffsetCache(getMockSubscriptionState());
        TopicPartition topicPartition = new TopicPartition("topic", 0);
        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(12345, Optional.of(15), "abc");

        committedOffsetCache.tryAddToCache(topicPartition, offsetAndMetadata);
        assertTrue(committedOffsetCache.isHitCache(Map.of(topicPartition, offsetAndMetadata)));

        committedOffsetCache.clear(Set.of(topicPartition));
        assertFalse(committedOffsetCache.isHitCache(Map.of(topicPartition, offsetAndMetadata)));

        TopicPartition topicPartition2 = new TopicPartition("topic2", 0);
        OffsetAndMetadata offsetAndMetadata2 = new OffsetAndMetadata(7890, Optional.of(11), "efg");
        committedOffsetCache.tryAddToCache(Map.of(topicPartition, offsetAndMetadata, topicPartition2, offsetAndMetadata2));
        assertTrue(committedOffsetCache.isHitCache(Map.of(topicPartition, offsetAndMetadata)));
        assertTrue(committedOffsetCache.isHitCache(Map.of(topicPartition2, offsetAndMetadata2)));
        assertTrue(committedOffsetCache.isHitCache(Map.of(topicPartition, offsetAndMetadata, topicPartition2, offsetAndMetadata2)));

        committedOffsetCache.clear(Set.of(topicPartition, topicPartition2));
        assertFalse(committedOffsetCache.isHitCache(Map.of(topicPartition, offsetAndMetadata)));
        assertFalse(committedOffsetCache.isHitCache(Map.of(topicPartition2, offsetAndMetadata2)));
        assertFalse(committedOffsetCache.isHitCache(Map.of(topicPartition, offsetAndMetadata, topicPartition2, offsetAndMetadata2)));

        committedOffsetCache.tryAddToCache(Map.of(topicPartition, offsetAndMetadata));
        assertFalse(committedOffsetCache.isHitCache(Map.of(topicPartition, offsetAndMetadata, topicPartition2, offsetAndMetadata2)));
    }

    private SubscriptionState getMockSubscriptionState() {
        var mockSubscriptionState = Mockito.mock(SubscriptionState.class);
        Mockito.when(mockSubscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        return mockSubscriptionState;
    }
}
