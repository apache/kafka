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

package org.apache.kafka.pcoll;

import org.apache.kafka.server.util.TranslatedValueMapView;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * A persistent Hash-based Map wrapper
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public interface PHashMapWrapper<K, V> {
    /**
     * @return the underlying persistent map
     */
    Object underlying();

    /**
     * @return the number of mappings in the underlying persistent collection
     */
    int size();

    /**
     * @return true iff the map is empty
     */
    boolean isEmpty();

    /**
     * @return the persistent map as a standard Java {@link Map}
     */
    Map<K, V> asJava();

    /**
     * @return the persistent map as a standard Java {@link Map} with values translated according to the given function.
     */
    default <T> Map<K, T> asJava(Function<V, T> valueMapping) {
        return new TranslatedValueMapView<>(asJava(), valueMapping);
    }

    /**
     * @param key the key
     * @param value the value
     * @return a wrapped persistent map that differs from this one in that the given mapping is added (if necessary)
     */
    PHashMapWrapper<K, V> afterAdding(K key, V value);

    /**
     * @param key the key
     * @return a wrapped persistent map that differs from this one in that the given mapping is removed (if necessary)
     */
    PHashMapWrapper<K, V> afterRemoving(K key);

    /**
     * @param key the key
     * @return the value mapped to the given key (which may be null if the underlying persistent collection supports
     * null values).  Null is also returned if no such mapping exists.
     */
    V get(K key);

    /**
     * @return the set of map entries
     */
    Set<Map.Entry<K, V>> entrySet();

    /**
     * @return the set of map keys
     */
    Set<K> keySet();

    /**
     * @param key the key to be checked for existence in the map
     * @return true iff there is a mapping for the given key (which may be null if the underlying persistent collection supports
     * null values).
     */
    boolean containsKey(K key);

    /**
     * @param key the key
     * @param value the value to return if no mapping for the given key exists
     * @return the value mapped for the given key if there is such a mapping, otherwise the given value
     */
    default V getOrElse(K key, V value) {
        if (containsKey(key)) {
            return get(key);
        } else {
            return value;
        }
    }
}
