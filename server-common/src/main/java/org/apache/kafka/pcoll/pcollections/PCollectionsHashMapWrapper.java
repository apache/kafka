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

package org.apache.kafka.pcoll.pcollections;

import org.apache.kafka.pcoll.PHashMapWrapper;
import org.pcollections.HashPMap;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class PCollectionsHashMapWrapper<K, V> implements PHashMapWrapper<K, V> {

    private final HashPMap<K, V> underlying;

    public PCollectionsHashMapWrapper(HashPMap<K, V> map) {
        this.underlying = Objects.requireNonNull(map);
    }

    @Override
    public HashPMap<K, V> underlying() {
        return underlying;
    }

    @Override
    public boolean isEmpty() {
        return underlying().isEmpty();
    }

    @Override
    public int size() {
        return underlying().size();
    }

    @Override
    public Map<K, V> asJava() {
        return underlying();
    }

    @Override
    public PHashMapWrapper<K, V> afterAdding(K key, V value) {
        return new PCollectionsHashMapWrapper<>(underlying().plus(key, value));
    }

    @Override
    public PHashMapWrapper<K, V> afterRemoving(K key) {
        return new PCollectionsHashMapWrapper<>(underlying().minus(key));
    }

    @Override
    public V get(K key) {
        return underlying().get(key);
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        return underlying().entrySet();
    }

    @Override
    public Set<K> keySet() {
        return underlying().keySet();
    }

    @Override
    public boolean containsKey(K key) {
        return underlying().containsKey(key);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PCollectionsHashMapWrapper<?, ?> that = (PCollectionsHashMapWrapper<?, ?>) o;
        return underlying().equals(that.underlying());
    }

    @Override
    public int hashCode() {
        return underlying().hashCode();
    }

    @Override
    public String toString() {
        return "PCollectionsHashMapWrapper{" +
            "underlying=" + underlying() +
            '}';
    }
}
