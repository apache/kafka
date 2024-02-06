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

package org.apache.kafka.server.immutable.pcollections;

import org.apache.kafka.server.immutable.ImmutableMap;
import org.pcollections.HashPMap;
import org.pcollections.HashTreePMap;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

@SuppressWarnings("deprecation")
public class PCollectionsImmutableMap<K, V> implements ImmutableMap<K, V> {

    private final HashPMap<K, V> underlying;

    /**
     * @return a wrapped hash-based persistent map that is empty
     * @param <K> the key type
     * @param <V> the value type
     */
    public static <K, V> PCollectionsImmutableMap<K, V> empty() {
        return new PCollectionsImmutableMap<>(HashTreePMap.empty());
    }

    /**
     * @param key the key
     * @param value the value
     * @return a wrapped hash-based persistent map that has a single mapping
     * @param <K> the key type
     * @param <V> the value type
     */
    public static <K, V> PCollectionsImmutableMap<K, V> singleton(K key, V value) {
        return new PCollectionsImmutableMap<>(HashTreePMap.singleton(key, value));
    }

    public PCollectionsImmutableMap(HashPMap<K, V> map) {
        this.underlying = Objects.requireNonNull(map);
    }

    @Override
    public ImmutableMap<K, V> updated(K key, V value) {
        return new PCollectionsImmutableMap<>(underlying().plus(key, value));
    }

    @Override
    public ImmutableMap<K, V> removed(K key) {
        return new PCollectionsImmutableMap<>(underlying().minus(key));
    }

    @Override
    public int size() {
        return underlying().size();
    }

    @Override
    public boolean isEmpty() {
        return underlying().isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return underlying().containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return underlying().containsValue(value);
    }

    @Override
    public V get(Object key) {
        return underlying().get(key);
    }

    @Override
    public V put(K key, V value) {
        // will throw UnsupportedOperationException; delegate anyway for testability
        return underlying().put(key, value);
    }

    @Override
    public V remove(Object key) {
        // will throw UnsupportedOperationException; delegate anyway for testability
        return underlying().remove(key);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        // will throw UnsupportedOperationException; delegate anyway for testability
        underlying().putAll(m);
    }

    @Override
    public void clear() {
        // will throw UnsupportedOperationException; delegate anyway for testability
        underlying().clear();
    }

    @Override
    public Set<K> keySet() {
        return underlying().keySet();
    }

    @Override
    public Collection<V> values() {
        return underlying().values();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return underlying().entrySet();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PCollectionsImmutableMap<?, ?> that = (PCollectionsImmutableMap<?, ?>) o;
        return underlying().equals(that.underlying());
    }

    @Override
    public int hashCode() {
        return underlying().hashCode();
    }

    @Override
    public V getOrDefault(Object key, V defaultValue) {
        return underlying().getOrDefault(key, defaultValue);
    }

    @Override
    public void forEach(BiConsumer<? super K, ? super V> action) {
        underlying().forEach(action);
    }

    @Override
    public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
        // will throw UnsupportedOperationException; delegate anyway for testability
        underlying().replaceAll(function);
    }

    @Override
    public V putIfAbsent(K key, V value) {
        // will throw UnsupportedOperationException; delegate anyway for testability
        return underlying().putIfAbsent(key, value);
    }

    @Override
    public boolean remove(Object key, Object value) {
        // will throw UnsupportedOperationException; delegate anyway for testability
        return underlying().remove(key, value);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        // will throw UnsupportedOperationException; delegate anyway for testability
        return underlying().replace(key, oldValue, newValue);
    }

    @Override
    public V replace(K key, V value) {
        // will throw UnsupportedOperationException; delegate anyway for testability
        return underlying().replace(key, value);
    }

    @Override
    public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
        // will throw UnsupportedOperationException; delegate anyway for testability
        return underlying().computeIfAbsent(key, mappingFunction);
    }

    @Override
    public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        // will throw UnsupportedOperationException; delegate anyway for testability
        return underlying().computeIfPresent(key, remappingFunction);
    }

    @Override
    public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        // will throw UnsupportedOperationException; delegate anyway for testability
        return underlying().compute(key, remappingFunction);
    }

    @Override
    public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
        // will throw UnsupportedOperationException; delegate anyway for testability
        return underlying().merge(key, value, remappingFunction);
    }

    @Override
    public String toString() {
        return "PCollectionsImmutableMap{" +
            "underlying=" + underlying() +
            '}';
    }

    // package-private for testing
    HashPMap<K, V> underlying() {
        return underlying;
    }
}
