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
import org.apache.kafka.common.utils.Time;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class PartitionLeaderCache {

    private final Map<TopicPartition, CacheEntry> cache = new HashMap<>();
    private final Time time;
    private final long ttlMs;

    public PartitionLeaderCache() {
        this(Time.SYSTEM, Long.MAX_VALUE);
    }

    public PartitionLeaderCache(Time time, long ttlMs) {
        this.time = time;
        this.ttlMs = ttlMs;
    }

    public Map<TopicPartition, Integer> get(Collection<TopicPartition> keys) {
        Map<TopicPartition, Integer> result = new HashMap<>();
        long now = time.milliseconds();
        synchronized (cache) {
            for (TopicPartition key : keys) {
                CacheEntry entry = cache.get(key);
                if (entry != null && !entry.isExpired(now, ttlMs)) {
                    result.put(key, entry.brokerId);
                }
            }
        }
        return result;
    }

    public void put(Map<TopicPartition, Integer> values) {
        long now = time.milliseconds();
        synchronized (cache) {
            values.forEach((tp, brokerId) ->
                cache.put(tp, new CacheEntry(brokerId, now)));
        }
    }

    public void remove(Collection<TopicPartition> keys) {
        synchronized (cache) {
            for (TopicPartition key : keys) {
                cache.remove(key);
            }
        }
    }

    private static class CacheEntry {
        final int brokerId;
        final long timestampMs;

        CacheEntry(int brokerId, long timestampMs) {
            this.brokerId = brokerId;
            this.timestampMs = timestampMs;
        }

        boolean isExpired(long nowMs, long ttlMs) {
            return (nowMs - timestampMs) >= ttlMs;
        }
    }
}
