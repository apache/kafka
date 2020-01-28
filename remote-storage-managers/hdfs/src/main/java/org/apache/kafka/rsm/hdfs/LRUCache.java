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
package org.apache.kafka.rsm.hdfs;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A simple LRU cache of remote data.
 *
 * The cache is a hash map from (file path, offset) pairs to the corresponding data (byte[]).
 * When the cache is full (the total data size >= the maximum size), adding a new cache entry to the cache will replace
 * the least-recently-used entry.
 *
 * The data (byte[]) of each entry is assumed to have about the same size.
 */
class LRUCache {
    private long totalBytes;
    private LinkedHashMap<String, byte[]> cache;

    /**
     * Create a new LRU cache with the specified max size.
     * @param maxBytes The maximum bytes can be stored in this cache.
     */
    LRUCache(long maxBytes) {
        cache = new LinkedHashMap<String, byte[]>(1000, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, byte[]> eldest) {
                if (totalBytes >= maxBytes) {
                    totalBytes -= eldest.getValue().length;
                    return true;
                }
                return false;
            }
        };
    }

    /**
     * Adding a new entry into the cache, and replace the LRU entry if the cache is full.
     */
    synchronized void put(String path, long offset, byte[] data) {
        String key = path + ":" + offset;
        cache.put(key, data);
        totalBytes += data.length;
    }

    /**
     * Retrieve the cached data with the specified (path, offset) pair.
     * Returns null if the required cache entry does not exist.
     */
    synchronized byte[] get(String path, long offset) {
        String key = path + ":" + offset;
        return cache.get(key);
    }
}
