/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.  You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.kafka.streams.state;

import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * A key value store that only supports read operations.
 * Implementations should be thread-safe as concurrent reads and writes
 * are expected
 * @param <K> the key type
 * @param <V> the value type
 */
@InterfaceStability.Unstable
public interface ReadOnlyKeyValueStore<K, V> {

    /**
     * Get the value corresponding to this key
     *
     * @param key The key to fetch
     * @return The value or null if no value is found.
     * @throws NullPointerException If null is used for key.
     */
    V get(K key);

    /**
     * Get an iterator over a given range of keys. This iterator MUST be closed after use.
     * The returned iterator must be safe from {@link java.util.ConcurrentModificationException}s
     * and must not return null values. No ordering guarantees are provided.
     * @param from The first key that could be in the range
     * @param to The last key that could be in the range
     * @return The iterator for this range.
     * @throws NullPointerException If null is used for from or to.
     */
    KeyValueIterator<K, V> range(K from, K to);

    /**
     * Return an iterator over all keys in this store. This iterator MUST be closed after use.
     * The returned iterator must be safe from {@link java.util.ConcurrentModificationException}s
     * and must not return null values. No ordering guarantees are provided.
     * @return An iterator of all key/value pairs in the store.
     */
    KeyValueIterator<K, V> all();
}
