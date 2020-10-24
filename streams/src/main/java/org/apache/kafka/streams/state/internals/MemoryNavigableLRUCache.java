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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryNavigableLRUCache extends MemoryLRUCache {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryNavigableLRUCache.class);

    public MemoryNavigableLRUCache(final String name, final int maxCacheSize) {
        super(name, maxCacheSize);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to) {
        if (from.compareTo(to) > 0) {
            LOG.warn("Returning empty iterator for fetch with invalid key range: from > to. " +
                "This may be due to range arguments set in the wrong order, " +
                "or serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                "Note that the built-in numerical serdes do not follow this for negative numbers");
            return KeyValueIterators.emptyIterator();
        }

        final TreeMap<Bytes, byte[]> treeMap = toTreeMap();
        return new DelegatingPeekingKeyValueIterator<>(name(),
            new MemoryNavigableLRUCache.CacheIterator(treeMap.navigableKeySet()
                .subSet(from, true, to, true).iterator(), treeMap));
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> reverseRange(final Bytes from, final Bytes to) {
        if (from.compareTo(to) > 0) {
            LOG.warn("Returning empty iterator for fetch with invalid key range: from > to. " +
                "This may be due to range arguments set in the wrong order, " +
                "or serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                "Note that the built-in numerical serdes do not follow this for negative numbers");
            return KeyValueIterators.emptyIterator();
        }

        final TreeMap<Bytes, byte[]> treeMap = toTreeMap();
        return new DelegatingPeekingKeyValueIterator<>(name(),
            new MemoryNavigableLRUCache.CacheIterator(treeMap
                .subMap(from, true, to, true).descendingKeySet().iterator(), treeMap));
    }

    @Override
    public  KeyValueIterator<Bytes, byte[]> all() {
        final TreeMap<Bytes, byte[]> treeMap = toTreeMap();
        return new MemoryNavigableLRUCache.CacheIterator(treeMap.navigableKeySet().iterator(), treeMap);
    }

    @Override
    public  KeyValueIterator<Bytes, byte[]> reverseAll() {
        final TreeMap<Bytes, byte[]> treeMap = toTreeMap();
        return new MemoryNavigableLRUCache.CacheIterator(treeMap.descendingKeySet().iterator(), treeMap);
    }

    private synchronized TreeMap<Bytes, byte[]> toTreeMap() {
        return new TreeMap<>(this.map);
    }


    private static class CacheIterator implements KeyValueIterator<Bytes, byte[]> {
        private final Iterator<Bytes> keys;
        private final Map<Bytes, byte[]> entries;
        private Bytes lastKey;

        private CacheIterator(final Iterator<Bytes> keys, final Map<Bytes, byte[]> entries) {
            this.keys = keys;
            this.entries = entries;
        }

        @Override
        public boolean hasNext() {
            return keys.hasNext();
        }

        @Override
        public KeyValue<Bytes, byte[]> next() {
            lastKey = keys.next();
            return new KeyValue<>(lastKey, entries.get(lastKey));
        }

        @Override
        public void close() {
            // do nothing
        }

        @Override
        public Bytes peekNextKey() {
            throw new UnsupportedOperationException("peekNextKey not supported");
        }
    }
}
