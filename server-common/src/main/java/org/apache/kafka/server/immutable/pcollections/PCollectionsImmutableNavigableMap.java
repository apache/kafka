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

import org.apache.kafka.server.immutable.ImmutableNavigableMap;
import org.pcollections.PSortedSet;
import org.pcollections.TreePMap;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

@SuppressWarnings("deprecation")
public class PCollectionsImmutableNavigableMap<K, V> implements ImmutableNavigableMap<K, V> {

    private final TreePMap<K, V> underlying;

    /**
     * @return a wrapped Tree-based Navigable map that is empty
     * @param <K> the key type
     * @param <V> the value type
     */
    public static <K extends Comparable<? super K>, V> PCollectionsImmutableNavigableMap<K, V> empty() {
        return new PCollectionsImmutableNavigableMap<>(TreePMap.<K, V>empty());
    }

    /**
     * @param key the key
     * @param value the value
     * @return a wrapped Tree-based Navigable map that has a single mapping
     * @param <K> the key type
     * @param <V> the value type
     */
    public static <K extends Comparable<? super K>, V> PCollectionsImmutableNavigableMap<K, V> singleton(K key, V value) {
        return new PCollectionsImmutableNavigableMap<>(TreePMap.singleton(key, value));
    }

    public PCollectionsImmutableNavigableMap(TreePMap<K, V> map) {
        this.underlying = Objects.requireNonNull(map);
    }

    @Override
    public  ImmutableNavigableMap<K, V> updated(K key, V value) {
        return new PCollectionsImmutableNavigableMap<>(underlying().plus(key, value));
    }

    @Override
    public ImmutableNavigableMap<K, V> removed(K key) {
        return new PCollectionsImmutableNavigableMap<>(underlying().minus(key));
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
        PCollectionsImmutableNavigableMap<?, ?> that = (PCollectionsImmutableNavigableMap<?, ?>) o;
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
    public Map.Entry<K, V> lowerEntry(K key) {
        return underlying().lowerEntry(key);
    }

    @Override
    public K lowerKey(K key) {
        return underlying().lowerKey(key);
    }

    @Override
    public Map.Entry<K, V> floorEntry(K key) {
        return underlying().floorEntry(key);
    }

    @Override
    public K floorKey(K key) {
        return underlying().floorKey(key);
    }

    @Override
    public Map.Entry<K, V> ceilingEntry(K key) {
        return underlying().ceilingEntry(key);
    }

    @Override
    public K ceilingKey(K key) {
        return underlying().ceilingKey(key);
    }

    @Override
    public Map.Entry<K, V> higherEntry(K key) {
        return underlying().higherEntry(key);
    }

    @Override
    public K higherKey(K key) {
        return underlying().higherKey(key);
    }

    @Override
    public Map.Entry<K, V> firstEntry() {
        return underlying().firstEntry();
    }

    @Override
    public K firstKey() {
        return underlying().firstKey();
    }

    @Override
    public Map.Entry<K, V> lastEntry() {
        return underlying().lastEntry();
    }

    @Override
    public K lastKey() {
        return underlying().lastKey();
    }

    @Override
    public Map.Entry<K, V> pollFirstEntry() {
        return underlying().pollFirstEntry();
    }

    @Override
    public Map.Entry<K, V> pollLastEntry() {
        return underlying().pollLastEntry();
    }

    @Override
    public PCollectionsImmutableNavigableMap<K, V> descendingMap() {
        return new PCollectionsImmutableNavigableMap<>(underlying().descendingMap());
    }

    @Override
    public PCollectionsImmutableNavigableSet<K> navigableKeySet() {
        return new PCollectionsImmutableNavigableSet<>(underlying().navigableKeySet());
    }

    @Override
    public PSortedSet<K> descendingKeySet() {
        return underlying().descendingKeySet();
    }

    @Override
    public PCollectionsImmutableNavigableMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
        return new PCollectionsImmutableNavigableMap<>(underlying().subMap(fromKey, fromInclusive, toKey, toInclusive));
    }

    @Override
    public PCollectionsImmutableNavigableMap<K, V> subMap(K fromKey, K toKey) {
        return new PCollectionsImmutableNavigableMap<>(underlying().subMap(fromKey, toKey));
    }

    @Override
    public PCollectionsImmutableNavigableMap<K, V> headMap(K toKey, boolean inclusive) {
        return new PCollectionsImmutableNavigableMap<>(underlying().headMap(toKey, inclusive));
    }

    @Override
    public PCollectionsImmutableNavigableMap<K, V> headMap(K toKey) {
        return new PCollectionsImmutableNavigableMap<>(underlying().headMap(toKey));
    }

    @Override
    public PCollectionsImmutableNavigableMap<K, V> tailMap(K fromKey, boolean inclusive) {
        return new PCollectionsImmutableNavigableMap<>(underlying().tailMap(fromKey, inclusive));
    }

    @Override
    public PCollectionsImmutableNavigableMap<K, V> tailMap(K fromKey) {
        return new PCollectionsImmutableNavigableMap<>(underlying().tailMap(fromKey));
    }

    @Override
    public Comparator<? super K> comparator() {
        return underlying().comparator();
    }

    @Override
    public String toString() {
        return "PCollectionsImmutableNavigableMap{" +
                "underlying=" + underlying() +
                '}';
    }

    TreePMap<K, V> underlying() {
        return underlying;
    }
}