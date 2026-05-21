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

package org.apache.kafka.server;

import org.apache.kafka.common.utils.Time;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Thread-safe cache that stores topic creation errors with per-entry expiration.
 * - Expiration: maintained by a min-heap (priority queue) on expiration time
 * - Capacity: enforced by evicting entries with earliest expiration time (not LRU)
 * - Updates: old entries remain in queue but are ignored via reference equality check
 */
class ExpiringErrorCache {

    private record Entry(String topicName, String errorMessage, long expirationTimeMs) {
    }

    private final int maxSize;
    private final Time time;
    private final ConcurrentHashMap<String, Entry> byTopic = new ConcurrentHashMap<>();
    private final PriorityQueue<Entry> expiryQueue =
            new PriorityQueue<>(11, Comparator.comparingLong(e -> e.expirationTimeMs));
    private final ReentrantLock lock = new ReentrantLock();

    ExpiringErrorCache(int maxSize, Time time) {
        this.maxSize = maxSize;
        this.time = time;
    }

    void put(String topicName, String errorMessage, long ttlMs) {
        lock.lock();
        try {
            var currentTimeMs = time.milliseconds();
            var expirationTimeMs = currentTimeMs + ttlMs;
            var entry = new Entry(topicName, errorMessage, expirationTimeMs);
            byTopic.put(topicName, entry);
            expiryQueue.add(entry);

            // Clean up expired entries and enforce capacity
            while (!expiryQueue.isEmpty() &&
                    (expiryQueue.peek().expirationTimeMs <= currentTimeMs || byTopic.size() > maxSize)) {
                var evicted = expiryQueue.poll();
                var current = byTopic.get(evicted.topicName);
                if (current != null && current == evicted) {
                    byTopic.remove(evicted.topicName);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    boolean hasError(String topicName, long currentTimeMs) {
        var entry = byTopic.get(topicName);
        return entry != null && entry.expirationTimeMs > currentTimeMs;
    }

    Map<String, String> getErrorsForTopics(Set<String> topicNames, long currentTimeMs) {
        var result = new HashMap<String, String>();
        for (var topicName : topicNames) {
            var entry = byTopic.get(topicName);
            if (entry != null && entry.expirationTimeMs > currentTimeMs) {
                result.put(topicName, entry.errorMessage);
            }
        }
        return result;
    }

    void clear() {
        lock.lock();
        try {
            byTopic.clear();
            expiryQueue.clear();
        } finally {
            lock.unlock();
        }
    }
}
