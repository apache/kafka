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
package org.apache.kafka.coordinator.group.util;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

/**
 * A map which wraps an underlying base map and accepts incremental updates
 * that overlay on top of it. This class expects the underlying base map to
 * be immutable.
 *
 * <p>Null values are not supported.
 *
 * @param <K> The key type.
 * @param <V> The value type.
 */
public class OverlayMap<K, V> extends AbstractMap<K, V> {
    private final Map<K, V> base;

    /** Entries whose keys are not in the base. */
    private final Map<K, V> additions = new HashMap<>();

    /** Entries whose keys are in the base, with a value that supersedes the base value. */
    private final Map<K, V> replacements = new HashMap<>();

    /** Keys that are in the base but have been removed. */
    private final Set<Object> removals = new HashSet<>();

    private Set<Entry<K, V>> entrySet;

    public OverlayMap(Map<K, V> base) {
        this.base = Objects.requireNonNull(base);
    }

    @Override
    public int size() {
        return base.size() + additions.size() - removals.size();
    }

    @Override
    public boolean isEmpty() {
        return additions.isEmpty() && removals.size() == base.size();
    }

    @Override
    public boolean containsKey(Object key) {
        if (additions.containsKey(key)) return true;
        if (replacements.containsKey(key)) return true;
        if (removals.contains(key)) return false;
        return base.containsKey(key);
    }

    @Override
    public V get(Object key) {
        if (additions.containsKey(key)) return additions.get(key);
        if (replacements.containsKey(key)) return replacements.get(key);
        if (removals.contains(key)) return null;
        return base.get(key);
    }

    @Override
    public V put(K key, V value) {
        Objects.requireNonNull(value);

        if (additions.containsKey(key)) {
            return additions.put(key, value);
        }
        if (replacements.containsKey(key)) {
            return replacements.put(key, value);
        }
        if (removals.remove(key)) {
            replacements.put(key, value);
            return null;
        }
        if (base.containsKey(key)) {
            replacements.put(key, value);
            return base.get(key);
        }
        additions.put(key, value);
        return null;
    }

    @Override
    public V remove(Object key) {
        if (additions.containsKey(key)) {
            return additions.remove(key);
        }
        if (replacements.containsKey(key)) {
            V prev = replacements.remove(key);
            removals.add(key);
            return prev;
        }
        if (removals.contains(key)) {
            return null;
        }
        if (base.containsKey(key)) {
            removals.add(key);
            return base.get(key);
        }
        return null;
    }

    @Override
    public void clear() {
        additions.clear();
        replacements.clear();
        removals.clear();
        removals.addAll(base.keySet());
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        if (entrySet != null) return entrySet;

        entrySet = new AbstractSet<>() {
            @Override
            public Iterator<Entry<K, V>> iterator() {
                return new Iterator<>() {
                    private final Iterator<Entry<K, V>> baseIterator = base.entrySet().iterator();
                    private final Iterator<Entry<K, V>> additionsIterator = additions.entrySet().iterator();
                    private Entry<K, V> next = null;

                    @Override
                    public boolean hasNext() {
                        if (next != null) return true;
                        while (baseIterator.hasNext()) {
                            Entry<K, V> entry = baseIterator.next();
                            if (replacements.containsKey(entry.getKey())) {
                                next = Map.entry(entry.getKey(), replacements.get(entry.getKey()));
                            } else {
                                next = entry;
                            }
                            if (removals.contains(entry.getKey())) continue;
                            return true;
                        }
                        if (additionsIterator.hasNext()) {
                            next = additionsIterator.next();
                            return true;
                        }
                        next = null;
                        return false;
                    }

                    @Override
                    public Entry<K, V> next() {
                        if (!hasNext()) throw new NoSuchElementException();
                        Entry<K, V> result = next;
                        next = null;
                        return result;
                    }
                };
            }

            @Override
            public int size() {
                return OverlayMap.this.size();
            }
        };
        return entrySet;
    }

    @Override
    public String toString() {
        return "OverlayMap(" +
            "base=" + base +
            ", additions=" + additions +
            ", replacements=" + replacements +
            ", removals=" + removals +
            ')';
    }
}
