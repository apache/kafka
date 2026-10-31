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
package org.apache.kafka.clients.admin.internals;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartitionLeaderCacheTest {

    private static final long TTL_MS = 10_000L;
    private static final TopicPartition TP0 = new TopicPartition("topic", 0);
    private static final TopicPartition TP1 = new TopicPartition("topic", 1);
    private static final TopicPartition TP2 = new TopicPartition("topic", 2);

    @Test
    void nonExpiredEntries() {
        MockTime time = new MockTime();
        PartitionLeaderCache cache = new PartitionLeaderCache(time, TTL_MS);

        Map<TopicPartition, Integer> entries = new HashMap<>();
        entries.put(TP0, 0);
        entries.put(TP1, 1);
        cache.put(entries);

        // Advance time but stay under TTL
        time.sleep(TTL_MS - 1);

        Map<TopicPartition, Integer> result = cache.get(Arrays.asList(TP0, TP1));
        assertEquals(2, result.size());
        assertEquals(0, result.get(TP0));
        assertEquals(1, result.get(TP1));
    }

    @Test
    void filtersExpiredEntries() {
        MockTime time = new MockTime();
        PartitionLeaderCache cache = new PartitionLeaderCache(time, TTL_MS);

        Map<TopicPartition, Integer> entries = new HashMap<>();
        entries.put(TP0, 0);
        entries.put(TP1, 1);
        cache.put(entries);

        // Advance time to exactly TTL (should expire)
        time.sleep(TTL_MS);

        Map<TopicPartition, Integer> result = cache.get(Arrays.asList(TP0, TP1));
        assertTrue(result.isEmpty());
    }

    @Test
    void putRefreshesTimestamp() {
        MockTime time = new MockTime();
        PartitionLeaderCache cache = new PartitionLeaderCache(time, TTL_MS);

        cache.put(Collections.singletonMap(TP0, 0));

        // Advance close to TTL
        time.sleep(TTL_MS - 2);

        // Re-put to refresh the timestamp
        cache.put(Collections.singletonMap(TP0, 0));

        // Advance again close to TTL from the refresh point
        time.sleep(TTL_MS - 2);

        // Should still be valid since we refreshed
        Map<TopicPartition, Integer> result = cache.get(Collections.singletonList(TP0));
        assertEquals(1, result.size());
        assertEquals(0, result.get(TP0));
    }

    @Test
    public void partialExpiration() {
        MockTime time = new MockTime();
        PartitionLeaderCache cache = new PartitionLeaderCache(time, TTL_MS);

        // Put TP0 first
        cache.put(Collections.singletonMap(TP0, 0));

        // Advance half the TTL
        time.sleep(TTL_MS / 2);

        // Put TP1 later
        cache.put(Collections.singletonMap(TP1, 1));

        // Advance so TP0 expires but TP1 does not
        time.sleep((TTL_MS / 2) + 1);

        Map<TopicPartition, Integer> result = cache.get(Arrays.asList(TP0, TP1));
        assertEquals(1, result.size());
        assertEquals(1, result.get(TP1));
    }

    @Test
    void noArgConstructorNeverExpires() { // backwards compatibility test
        PartitionLeaderCache cache = new PartitionLeaderCache();

        Map<TopicPartition, Integer> entries = new HashMap<>();
        entries.put(TP0, 0);
        entries.put(TP1, 1);
        entries.put(TP2, 2);
        cache.put(entries);

        Time.SYSTEM.sleep(TTL_MS * 2);

        // With the no-arg constructor using Long.MAX_VALUE TTL and Time.SYSTEM,
        // entries should effectively never expire
        Map<TopicPartition, Integer> result = cache.get(Arrays.asList(TP0, TP1, TP2));
        assertEquals(3, result.size());
        assertEquals(0, result.get(TP0));
        assertEquals(1, result.get(TP1));
        assertEquals(2, result.get(TP2));
    }
}
