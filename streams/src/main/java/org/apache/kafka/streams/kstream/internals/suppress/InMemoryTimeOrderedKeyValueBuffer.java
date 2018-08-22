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
package org.apache.kafka.streams.kstream.internals.suppress;

import org.apache.kafka.streams.KeyValue;

import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

class InMemoryTimeOrderedKeyValueBuffer<K, V> implements TimeOrderedKeyValueBuffer<K, V> {
    private long memBufferSize = 0;
    private final Map<K, TimeKey<K>> index = new HashMap<>();
    private final TreeMap<TimeKey<K>, SizedValue<V>> sortedMap = new TreeMap<>();

    private static class SizedValue<V> {
        private final V v;
        private final long size;

        private SizedValue(final V v, final long size) {
            this.v = v;
            this.size = size;
        }

        public V value() {
            return v;
        }

        public long size() {
            return size;
        }

        @Override
        public String toString() {
            return "SizedValue{v=" + v + ", size=" + size + '}';
        }
    }

    private class InnerIterator implements Iterator<KeyValue<TimeKey<K>, V>> {
        private final Iterator<Map.Entry<TimeKey<K>, SizedValue<V>>> delegate = sortedMap.entrySet().iterator();
        private Map.Entry<TimeKey<K>, SizedValue<V>> last;

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public KeyValue<TimeKey<K>, V> next() {
            final Map.Entry<TimeKey<K>, SizedValue<V>> next = delegate.next();
            this.last = next;
            return new KeyValue<>(next.getKey(), next.getValue().value());
        }

        @Override
        public void remove() {
            delegate.remove();
            index.remove(last.getKey().key());
            memBufferSize = memBufferSize - last.getValue().size();
        }
    }

    @Override
    public Iterator<KeyValue<TimeKey<K>, V>> iterator() {
        return new InnerIterator();
    }

    @Override
    public void put(final long time,
                    final K key,
                    final V value,
                    final long recordSize) {
        final TimeKey<K> previousKey = index.get(key);
        // non-resetting semantics:
        // if there was a previous version of the same record, insert the new record in the same place in the priority queue
        final TimeKey<K> timeKey = previousKey != null ? previousKey : new TimeKey<>(time, key);

        final TimeKey<K> replaced = index.put(key, timeKey);

        if (!Objects.equals(previousKey, replaced)) {
            throw new ConcurrentModificationException(
                String.format("expected to replace [%s], but got [%s]", previousKey, replaced)
            );
        }

        final SizedValue<V> removedValue = replaced == null ? null : sortedMap.remove(replaced);
        final SizedValue<V> previousValue = sortedMap.put(timeKey, new SizedValue<>(value, recordSize));
        memBufferSize =
            memBufferSize
                + recordSize
                - (removedValue == null ? 0 : removedValue.size())
                - (previousValue == null ? 0 : previousValue.size());
    }

    @Override
    public int numRecords() {
        return index.size();
    }

    @Override
    public long bufferSize() {
        return memBufferSize;
    }

    @Override
    public String toString() {
        return "InMemoryTimeOrderedKeyValueBuffer{" +
            ", \n\tmemBufferSize=" + memBufferSize +
            ", \n\tindex=" + index +
            ", \n\tsortedMap=" + sortedMap +
            '\n' + '}';
    }
}
