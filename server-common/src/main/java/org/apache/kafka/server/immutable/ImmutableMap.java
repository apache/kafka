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

package org.apache.kafka.server.immutable;

import org.apache.kafka.server.immutable.pcollections.PCollectionsImmutableMap;

import java.util.Map;

/**
 * A persistent Hash-based Map wrapper.
 * java.util.Map methods that mutate in-place will throw UnsupportedOperationException
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public interface ImmutableMap<K, V> extends Map<K, V> {
    /**
     * @return a wrapped hash-based persistent map that is empty
     * @param <K> the key type
     * @param <V> the value type
     */
    static <K, V> ImmutableMap<K, V> empty() {
        return PCollectionsImmutableMap.empty();
    }

    /**
     * @param key the key
     * @param value the value
     * @return a wrapped hash-based persistent map that has a single mapping
     * @param <K> the key type
     * @param <V> the value type
     */
    static <K, V> ImmutableMap<K, V> singleton(K key, V value) {
        return PCollectionsImmutableMap.singleton(key, value);
    }

    /**
     * @param key the key
     * @param value the value
     * @return a wrapped persistent map that differs from this one in that the given mapping is added (if necessary)
     */
    ImmutableMap<K, V> updated(K key, V value);

    /**
     * @param key the key
     * @return a wrapped persistent map that differs from this one in that the given mapping is removed (if necessary)
     */
    ImmutableMap<K, V> removed(K key);
}
