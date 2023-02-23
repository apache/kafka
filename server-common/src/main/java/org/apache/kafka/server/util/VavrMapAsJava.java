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

package org.apache.kafka.server.util;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Presents a Vavr Map as a Java Map, optionally translating the Value type
 * 
 * @param <K> the Map Key type
 * @param <B> the Map Value type of the Vavr Map
 * @param <V> the Map Value type of the Java Map
 */
public class VavrMapAsJava<K, B, V> extends AbstractMap<K, V> {

    static abstract class BaseImmutableSet<E> extends AbstractSet<E> {
        @Override
        public abstract boolean contains(Object o); // inherited implementation has O(N) performance, so must override

        @Override
        public boolean remove(Object o) {
            // override the inherited implementation to fail-fast
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean addAll(Collection<? extends E> c) {
            // override the inherited implementation to fail-fast
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            // override the inherited implementation to fail-fast
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            // override the inherited implementation to fail-fast
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean removeIf(Predicate<? super E> filter) {
            // override the inherited implementation to fail-fast
            throw new UnsupportedOperationException();
        }

        @Override
        public void clear() {
            // override the inherited implementation to fail-fast
            throw new UnsupportedOperationException();
        }
    }

    private class AsEntrySet extends BaseImmutableSet<Entry<K, V>> {
        @Override
        public int size() {
            return VavrMapAsJava.this.size();
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        public boolean contains(Object o) {
            Map.Entry<K, V> provided = (Map.Entry) Objects.requireNonNull(o);
            return VavrMapAsJava.this.containsKey(provided.getKey()) &&
                Objects.equals(VavrMapAsJava.this.get(provided.getKey()), provided.getValue());
        }

        @Override
        public Iterator<Entry<K, V>> iterator() {
            return VavrMapAsJava.this.vavrMap.iterator((k, v) -> new SimpleImmutableEntry<>(k, valueMapper.apply(v)));
        }
    }

    private final io.vavr.collection.Map<K, B> vavrMap;
    private final Function<B, V> valueMapper;

    public VavrMapAsJava(io.vavr.collection.Map<K, B> vavrMap, Function<B, V> valueMapper) {
        this.vavrMap = Objects.requireNonNull(vavrMap);
        this.valueMapper = Objects.requireNonNull(valueMapper);
    }

    @Override
    public int size() {
        return vavrMap.size();
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean containsKey(Object key) {
        // inherited implementation has O(N) performance, so must override
        return vavrMap.containsKey((K) key);
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean containsValue(Object value) {
        // inherited implementation depends on entrySet(), which is more expensive than simply delegating to vavr
        return vavrMap.find(v -> Objects.equals(value, valueMapper.apply(v._2))).isDefined();
    }

    @SuppressWarnings("unchecked")
    @Override
    public V get(Object key) {
        // inherited implementation has O(N) performance, so must override
        return vavrMap.get((K) key).map(valueMapper).getOrNull();
    }

    @Override
    public V remove(Object key) {
        // override the inherited implementation to fail-fast
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        // override the inherited implementation to fail-fast
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        // override the inherited implementation to fail-fast
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<K> keySet() {
        // inherited implementation has O(N) performance, so must override
        return new VavrSetAsJava<>(vavrMap.keySet());
    }

    @Override
    public Collection<V> values() {
        // inherited implementation depends on entrySet(), which is more expensive than simply delegating to vavr
        return vavrMap.values().map(valueMapper).asJava();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return new AsEntrySet();
    }

    @Override
    public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
        // override the inherited implementation to fail-fast
        throw new UnsupportedOperationException();
    }

    @Override
    public V putIfAbsent(K key, V value) {
        // override the inherited implementation to fail-fast
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object key, Object value) {
        // override the inherited implementation to fail-fast
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        // override the inherited implementation to fail-fast
        throw new UnsupportedOperationException();
    }

    @Override
    public V replace(K key, V value) {
        // override the inherited implementation to fail-fast
        throw new UnsupportedOperationException();
    }

    @Override
    public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
        // override the inherited implementation to fail-fast
        throw new UnsupportedOperationException();
    }

    @Override
    public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        // override the inherited implementation to fail-fast
        throw new UnsupportedOperationException();
    }

    @Override
    public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        // override the inherited implementation to fail-fast
        throw new UnsupportedOperationException();
    }

    @Override
    public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
        // override the inherited implementation to fail-fast
        throw new UnsupportedOperationException();
    }
}
