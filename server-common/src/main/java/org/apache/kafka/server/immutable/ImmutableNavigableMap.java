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

import org.apache.kafka.server.immutable.pcollections.PCollectionsImmutableNavigableMap;

import java.util.NavigableMap;

/**
 * A persistent Tree-based NavigableMap wrapper
 * java.util.Map methods that mutate in-place will throw UnsupportedOperationException
 *
 * @param <K> the element type
 * @param <V> the value type
 */
public interface ImmutableNavigableMap<K, V> extends ImmutableMap<K, V>, NavigableMap<K, V> {
    /**
     * @return a wrapped Tree-based Navigable Map that is empty
     * @param <K> the key type
     * @param <V> the value type
     */
    static <K extends Comparable<? super K>, V> ImmutableNavigableMap<K, V> empty() {
        return PCollectionsImmutableNavigableMap.empty();
    }

    /**
     * @param key the key
     * @param value the value
     * @return a wrapped Tree-based Navigable map that has a single mapping
     * @param <K> the key type
     * @param <V> the value type
     */
    static <K extends Comparable<? super K>, V> ImmutableNavigableMap<K, V> singleton(K key, V value) {
        return PCollectionsImmutableNavigableMap.singleton(key, value);
    }

    /**
     * @param key the key
     * @param value the value
     * @return a wrapped persistent map that differs from this one in that the given mapping is added (if necessary)
     */
    ImmutableNavigableMap<K, V> updated(K key, V value);

    /**
     * @param key the key
     * @return a wrapped persistent map that differs from this one in that the given mapping is removed (if necessary)
     */
    ImmutableNavigableMap<K, V> removed(K key);
}































