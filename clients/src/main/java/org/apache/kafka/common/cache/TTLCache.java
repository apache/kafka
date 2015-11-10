/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.common.cache;

import org.apache.kafka.common.utils.Time;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A cache that can expire keys with TTL.
 */
public class TTLCache<K, V> implements Cache<K, V> {
    private final ConcurrentMap<K, ValueWithTimeStamp<V>> cache;
    private final Long ttlMillis;
    private final Time time;

    public TTLCache(final long ttlMilliSeconds, Time time) {
        this.ttlMillis = ttlMilliSeconds;
        this.cache = new ConcurrentHashMap<>();
        this.time = time;
    }

    @Override
    public V get(K key) {
        ValueWithTimeStamp<V> valueWithTimeStamp = cache.get(key);
        if(valueWithTimeStamp != null && (valueWithTimeStamp.timeStamp + ttlMillis) < time.milliseconds()) {
            return valueWithTimeStamp.value;
        } else {
            remove(key);
            return null;
        }
    }

    @Override
    public void put(K key, V value) {
        cache.putIfAbsent(key, new ValueWithTimeStamp<V>(value, this.time));
    }

    @Override
    public boolean remove(K key) {
        return cache.remove(key) != null;
    }

    @Override
    public long size() {
        return cache.size();
    }

    private static class ValueWithTimeStamp<V> {
        V value;
        long timeStamp;

        public ValueWithTimeStamp(V value, Time time) {
            this.value = value;
            this.timeStamp = time.milliseconds();
        }
    }
}
