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

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentNavigableMap;

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
            LOG.warn("Returning empty iterator for fetch with invalid key range: from > to. "
                + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                "Note that the built-in numerical serdes do not follow this for negative numbers");
            return KeyValueIterators.emptyIterator();
        }

        final TreeMap<Bytes, byte[]> treeMap = toTreeMap();
        return new DelegatingPeekingKeyValueIterator<>(name(),
            new MemoryNavigableLRUCache.CacheIterator(treeMap.navigableKeySet()
                .subSet(from, true, to, true).iterator(), treeMap));
    }

    @Override
    public  KeyValueIterator<Bytes, byte[]> all() {
        final TreeMap<Bytes, byte[]> treeMap = toTreeMap();
        return new MemoryNavigableLRUCache.CacheIterator(treeMap.navigableKeySet().iterator(), treeMap);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> prefixScan(final Bytes prefix) {
        final Bytes prefixEnd = Bytes.increment(prefix);
        final TreeMap<Bytes, byte[]> treeMap = toTreeMap();
        final Comparator<? super Bytes> comparator = treeMap.comparator();

        //We currently don't set a comparator for the treeMap, so the comparator will always be null.
        final int result = comparator==null ? prefix.compareTo(prefixEnd) : comparator.compare(prefix, prefixEnd);

        final NavigableMap<Bytes, byte[]> subMapResults;
        if (result > 0) {
            //Prefix increment would cause a wrap-around. Get the submap from toKey to the end of the map
            subMapResults = treeMap.tailMap(prefix, true);
        } else {
            subMapResults = treeMap.subMap(prefix, true, prefixEnd, false);
        }

        return new DelegatingPeekingKeyValueIterator<>(name(),
                new MemoryNavigableLRUCache.CacheIterator(subMapResults.navigableKeySet().iterator(), subMapResults));
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
        public void remove() {
            // do nothing
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
