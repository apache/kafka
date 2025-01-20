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
package org.apache.kafka.common.cache;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A cache implementing a least recently used policy.
 */
public class LRUCache<K, V> implements Cache<K, V> {
    private final LinkedHashMap<K, V> cache;

    public LRUCache(final int maxSize) {
        cache = new LinkedHashMap<>(16, .75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                return this.size() > maxSize; // require this. prefix to make lgtm.com happy
            }
        };
    }

    @Override
    public V get(K key) {
        return cache.get(key);
    }

    @Override
    public void put(K key, V value) {
        cache.put(key, value);
    }

    @Override
    public boolean remove(K key) {
        return cache.remove(key) != null;
    }

    @Override
    public long size() {
        return cache.size();
    }
}
