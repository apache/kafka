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

import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

/**
 * This is a hash set which can be snapshotted.
 * <br>
 * See {@SnapshottableHashTable} for more details about the implementation.
 * <br>
 * This class requires external synchronization.  Null values are not supported.
 *
 * @param <T>   The value type of the set.
 */
public class TimelineHashSet<T>
        extends SnapshottableHashTable<TimelineHashSet.TimelineHashSetEntry<T>>
        implements Set<T> {
    static class TimelineHashSetEntry<T>
            implements SnapshottableHashTable.ElementWithStartEpoch {
        private final T value;
        private long startEpoch;

        TimelineHashSetEntry(T value) {
            this.value = value;
            this.startEpoch = SnapshottableHashTable.LATEST_EPOCH;
        }

        public T getValue() {
            return value;
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
            if (!(o instanceof TimelineHashSetEntry)) return false;
            TimelineHashSetEntry<T> other = (TimelineHashSetEntry<T>) o;
            return value.equals(other.value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }
    }

    public TimelineHashSet(SnapshotRegistry snapshotRegistry, int expectedSize) {
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
    public boolean contains(Object key) {
        return contains(key, SnapshottableHashTable.LATEST_EPOCH);
    }

    public boolean contains(Object object, long epoch) {
        return snapshottableGet(new TimelineHashSetEntry<>(object), epoch) != null;
    }

    final class ValueIterator implements Iterator<T> {
        private final Iterator<TimelineHashSetEntry<T>> iter;

        ValueIterator(long epoch) {
            this.iter = snapshottableIterator(epoch);
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public T next() {
            return iter.next().value;
        }

        @Override
        public void remove() {
            iter.remove();
        }
    }

    @Override
    public Iterator<T> iterator() {
        return iterator(SnapshottableHashTable.LATEST_EPOCH);
    }

    public Iterator<T> iterator(long epoch) {
        return new ValueIterator(epoch);
    }

    @Override
    public Object[] toArray() {
        Object[] result = new Object[size()];
        Iterator<T> iter = iterator();
        int i = 0;
        while (iter.hasNext()) {
            result[i++] = iter.next();
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <R> R[] toArray(R[] a) {
        int size = size();
        if (size <= a.length) {
            Iterator<T> iter = iterator();
            int i = 0;
            while (iter.hasNext()) {
                a[i++] = (R) iter.next();
            }
            while (i < a.length) {
                a[i++] = null;
            }
            return a;
        } else {
            return (R[]) toArray();
        }
    }

    @Override
    public boolean add(T newValue) {
        Objects.requireNonNull(newValue);
        return snapshottableAddUnlessPresent(new TimelineHashSetEntry<>(newValue));
    }

    @Override
    public boolean remove(Object value) {
        return snapshottableRemove(new TimelineHashSetEntry<>(value)) != null;
    }

    @Override
    public boolean containsAll(Collection<?> collection) {
        for (Object value : collection) {
            if (!contains(value)) return false;
        }
        return true;
    }

    @Override
    public boolean addAll(Collection<? extends T> collection) {
        boolean modified = false;
        for (T value : collection) {
            if (add(value)) {
                modified = true;
            }
        }
        return modified;
    }

    @Override
    public boolean retainAll(Collection<?> collection) {
        Objects.requireNonNull(collection);
        boolean modified = false;
        Iterator<T> it = iterator();
        while (it.hasNext()) {
            if (!collection.contains(it.next())) {
                it.remove();
                modified = true;
            }
        }
        return modified;
    }

    @Override
    public boolean removeAll(Collection<?> collection) {
        Objects.requireNonNull(collection);
        boolean modified = false;
        Iterator<?> it = iterator();
        while (it.hasNext()) {
            if (collection.contains(it.next())) {
                it.remove();
                modified = true;
            }
        }
        return modified;
    }

    @Override
    public void clear() {
        reset();
    }

    @Override
    public int hashCode() {
        int hash = 0;
        Iterator<T> iter = iterator();
        while (iter.hasNext()) {
            hash += iter.next().hashCode();
        }
        return hash;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof Set))
            return false;
        Collection<?> c = (Collection<?>) o;
        if (c.size() != size())
            return false;
        try {
            return containsAll(c);
        } catch (ClassCastException unused)   {
            return false;
        }
    }
}
