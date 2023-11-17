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

package org.apache.kafka.timeline;

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * This is a hash map which can be snapshotted.
 * <br>
 * See {@SnapshottableHashTable} for more details about the implementation.
 * <br>
 * This class requires external synchronization.  Null keys and values are not supported.
 *
 * @param <K>   The key type of the set.
 * @param <V>   The value type of the set.
 */
public class TimelineHashMap<K, V>
        extends SnapshottableHashTable<TimelineHashMap.TimelineHashMapEntry<K, V>>
        implements Map<K, V> {
    static class TimelineHashMapEntry<K, V>
            implements SnapshottableHashTable.ElementWithStartEpoch, Map.Entry<K, V> {
        private final K key;
        private final V value;
        private long startEpoch;

        TimelineHashMapEntry(K key, V value) {
            this.key = key;
            this.value = value;
            this.startEpoch = SnapshottableHashTable.LATEST_EPOCH;
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return value;
        }

        @Override
        public V setValue(V value) {
            // This would be inefficient to support since we'd need a back-reference
            // to the enclosing map in each Entry object.  There would also be
            // complications if this entry object was sourced from a historical iterator;
            // we don't support modifying the past.  Since we don't really need this API,
            // let's just not support it.
            throw new UnsupportedOperationException();
        }

        @Override
        public void setStartEpoch(long startEpoch) {
            this.startEpoch = startEpoch;
        }

        @Override
        public long startEpoch() {
            return startEpoch;
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean equals(Object o) {
            if (!(o instanceof TimelineHashMapEntry)) return false;
            TimelineHashMapEntry<K, V> other = (TimelineHashMapEntry<K, V>) o;
            return key.equals(other.key);
        }

        @Override
        public int hashCode() {
            return key.hashCode();
        }
    }

    public TimelineHashMap(SnapshotRegistry snapshotRegistry, int expectedSize) {
        super(snapshotRegistry, expectedSize);
    }

    @Override
    public int size() {
        return size(SnapshottableHashTable.LATEST_EPOCH);
    }

    public int size(long epoch) {
        return snapshottableSize(epoch);
    }

    @Override
    public boolean isEmpty() {
        return isEmpty(SnapshottableHashTable.LATEST_EPOCH);
    }

    public boolean isEmpty(long epoch) {
        return snapshottableSize(epoch) == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        return containsKey(key, SnapshottableHashTable.LATEST_EPOCH);
    }

    public boolean containsKey(Object key, long epoch) {
        return snapshottableGet(new TimelineHashMapEntry<>(key, null), epoch) != null;
    }

    @Override
    public boolean containsValue(Object value) {
        Iterator<Entry<K, V>> iter = entrySet().iterator();
        while (iter.hasNext()) {
            Entry<K, V> e = iter.next();
            if (value.equals(e.getValue())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public V get(Object key) {
        return get(key, SnapshottableHashTable.LATEST_EPOCH);
    }

    public V get(Object key, long epoch) {
        Entry<K, V> entry =
            snapshottableGet(new TimelineHashMapEntry<>(key, null), epoch);
        if (entry == null) {
            return null;
        }
        return entry.getValue();
    }

    @Override
    public V put(K key, V value) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
        TimelineHashMapEntry<K, V> entry = new TimelineHashMapEntry<>(key, value);
        TimelineHashMapEntry<K, V> prev = snapshottableAddOrReplace(entry);
        if (prev == null) {
            return null;
        }
        return prev.getValue();
    }

    @Override
    public V remove(Object key) {
        TimelineHashMapEntry<K, V> result = snapshottableRemove(
            new TimelineHashMapEntry<>(key, null));
        return result == null ? null : result.value;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        for (Map.Entry<? extends K, ? extends V> e : map.entrySet()) {
            put(e.getKey(), e.getValue());
        }
    }

    @Override
    public void clear() {
        reset();
    }

    final class KeySet extends AbstractSet<K> {
        private final long epoch;

        KeySet(long epoch) {
            this.epoch = epoch;
        }

        public int size() {
            return TimelineHashMap.this.size(epoch);
        }

        public void clear() {
            if (epoch != SnapshottableHashTable.LATEST_EPOCH) {
                throw new RuntimeException("can't modify snapshot");
            }
            TimelineHashMap.this.clear();
        }

        public Iterator<K> iterator() {
            return new KeyIterator(epoch);
        }

        public boolean contains(Object o) {
            return TimelineHashMap.this.containsKey(o, epoch);
        }

        public boolean remove(Object o) {
            if (epoch != SnapshottableHashTable.LATEST_EPOCH) {
                throw new RuntimeException("can't modify snapshot");
            }
            return TimelineHashMap.this.remove(o) != null;
        }
    }

    final class KeyIterator implements Iterator<K> {
        private final Iterator<TimelineHashMapEntry<K, V>> iter;

        KeyIterator(long epoch) {
            this.iter = snapshottableIterator(epoch);
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public K next() {
            TimelineHashMapEntry<K, V> next = iter.next();
            return next.getKey();
        }

        @Override
        public void remove() {
            iter.remove();
        }
    }

    @Override
    public Set<K> keySet() {
        return keySet(SnapshottableHashTable.LATEST_EPOCH);
    }

    public Set<K> keySet(long epoch) {
        return new KeySet(epoch);
    }

    final class Values extends AbstractCollection<V> {
        private final long epoch;

        Values(long epoch) {
            this.epoch = epoch;
        }

        public int size() {
            return TimelineHashMap.this.size(epoch);
        }

        public void clear() {
            if (epoch != SnapshottableHashTable.LATEST_EPOCH) {
                throw new RuntimeException("can't modify snapshot");
            }
            TimelineHashMap.this.clear();
        }

        public Iterator<V> iterator() {
            return new ValueIterator(epoch);
        }

        public boolean contains(Object o) {
            return TimelineHashMap.this.containsKey(o, epoch);
        }
    }

    final class ValueIterator implements Iterator<V> {
        private final Iterator<TimelineHashMapEntry<K, V>> iter;

        ValueIterator(long epoch) {
            this.iter = snapshottableIterator(epoch);
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public V next() {
            TimelineHashMapEntry<K, V> next = iter.next();
            return next.getValue();
        }

        @Override
        public void remove() {
            iter.remove();
        }
    }

    @Override
    public Collection<V> values() {
        return values(SnapshottableHashTable.LATEST_EPOCH);
    }

    public Collection<V> values(long epoch) {
        return new Values(epoch);
    }

    final class EntrySet extends AbstractSet<Map.Entry<K, V>> {
        private final long epoch;

        EntrySet(long epoch) {
            this.epoch = epoch;
        }

        public int size() {
            return TimelineHashMap.this.size(epoch);
        }

        public void clear() {
            if (epoch != SnapshottableHashTable.LATEST_EPOCH) {
                throw new RuntimeException("can't modify snapshot");
            }
            TimelineHashMap.this.clear();
        }

        public Iterator<Map.Entry<K, V>> iterator() {
            return new EntryIterator(epoch);
        }

        public boolean contains(Object o) {
            return snapshottableGet(o, epoch) != null;
        }

        public boolean remove(Object o) {
            if (epoch != SnapshottableHashTable.LATEST_EPOCH) {
                throw new RuntimeException("can't modify snapshot");
            }
            return snapshottableRemove(new TimelineHashMapEntry<>(o, null)) != null;
        }
    }

    final class EntryIterator implements Iterator<Map.Entry<K, V>> {
        private final Iterator<TimelineHashMapEntry<K, V>> iter;

        EntryIterator(long epoch) {
            this.iter = snapshottableIterator(epoch);
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public Map.Entry<K, V> next() {
            return iter.next();
        }

        @Override
        public void remove() {
            iter.remove();
        }
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return entrySet(SnapshottableHashTable.LATEST_EPOCH);
    }

    public Set<Entry<K, V>> entrySet(long epoch) {
        return new EntrySet(epoch);
    }

    @Override
    public int hashCode() {
        int hash = 0;
        Iterator<Entry<K, V>> iter = entrySet().iterator();
        while (iter.hasNext()) {
            hash += iter.next().hashCode();
        }
        return hash;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof Map))
            return false;
        Map<?, ?> m = (Map<?, ?>) o;
        if (m.size() != size())
            return false;
        try {
            Iterator<Entry<K, V>> iter = entrySet().iterator();
            while (iter.hasNext()) {
                Entry<K, V> entry = iter.next();
                if (!m.get(entry.getKey()).equals(entry.getValue())) {
                    return false;
                }
            }
        } catch (ClassCastException unused) {
            return false;
        }
        return true;

    }
}
