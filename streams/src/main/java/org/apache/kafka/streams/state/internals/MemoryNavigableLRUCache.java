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

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

public class MemoryNavigableLRUCache extends MemoryLRUCache {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryNavigableLRUCache.class);

    public MemoryNavigableLRUCache(final String name, final int maxCacheSize) {
        super(name, maxCacheSize);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to) {
        if (Objects.nonNull(from) && Objects.nonNull(to) && from.compareTo(to) > 0) {
            LOG.warn("Returning empty iterator for fetch with invalid key range: from > to. " +
                    "This may be due to range arguments set in the wrong order, " +
                    "or serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                    "Note that the built-in numerical serdes do not follow this for negative numbers");
            return KeyValueIterators.emptyIterator();
        } else {
            final TreeMap<Bytes, byte[]> treeMap = toTreeMap();
            final Iterator<Bytes> keys = getIterator(treeMap, from, to, true);
            return new DelegatingPeekingKeyValueIterator<>(name(),
                    new MemoryNavigableLRUCache.CacheIterator(keys, treeMap));
        }
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> reverseRange(final Bytes from, final Bytes to) {
        if (Objects.nonNull(from) && Objects.nonNull(to) && from.compareTo(to) > 0) {
            LOG.warn("Returning empty iterator for fetch with invalid key range: from > to. " +
                    "This may be due to range arguments set in the wrong order, " +
                    "or serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                    "Note that the built-in numerical serdes do not follow this for negative numbers");
            return KeyValueIterators.emptyIterator();
        } else {
            final TreeMap<Bytes, byte[]> treeMap = toTreeMap();
            final Iterator<Bytes> keys = getIterator(treeMap, from, to, false);
            return new DelegatingPeekingKeyValueIterator<>(name(),
                    new MemoryNavigableLRUCache.CacheIterator(keys, treeMap));
        }
    }

    private Iterator<Bytes> getIterator(final TreeMap<Bytes, byte[]> treeMap, final Bytes from, final Bytes to, final boolean forward) {
        if (from == null && to == null) {
            return forward ? treeMap.navigableKeySet().iterator() : treeMap.navigableKeySet().descendingIterator();
        } else if (from == null) {
            return forward ? treeMap.navigableKeySet().headSet(to, true).iterator() : treeMap.navigableKeySet().headSet(to, true).descendingIterator();
        } else if (to == null) {
            return forward ? treeMap.navigableKeySet().tailSet(from, true).iterator() : treeMap.navigableKeySet().tailSet(from, true).descendingIterator();
        } else {
            return forward ? treeMap.navigableKeySet().subSet(from, true, to, true).iterator() : treeMap.navigableKeySet().subSet(from, true, to, true).descendingIterator();
        }
    }

    @Override
    public <PS extends Serializer<P>, P> KeyValueIterator<Bytes, byte[]> prefixScan(final P prefix, final PS prefixKeySerializer) {

        final Bytes from = Bytes.wrap(prefixKeySerializer.serialize(null, prefix));
        final Bytes to = Bytes.increment(from);

        final TreeMap<Bytes, byte[]> treeMap = toTreeMap();

        return new DelegatingPeekingKeyValueIterator<>(
            name(),
            new MemoryNavigableLRUCache.CacheIterator(treeMap.subMap(from, true, to, false).keySet().iterator(), treeMap)
        );
    }

    @Override
    public  KeyValueIterator<Bytes, byte[]> all() {
        return range(null, null);
    }

    @Override
    public  KeyValueIterator<Bytes, byte[]> reverseAll() {
        return reverseRange(null, null);
    }

    private synchronized TreeMap<Bytes, byte[]> toTreeMap() {
        return new TreeMap<>(this.map);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <R> QueryResult<R> query(
        final Query<R> query,
        final PositionBound positionBound,
        final QueryConfig config) {

        return StoreQueryUtils.handleBasicQueries(
            query,
            positionBound,
            config,
            this,
            getPosition(),
            context
        );
    }


    private static class CacheIterator implements KeyValueIterator<Bytes, byte[]> {
        private final Iterator<Bytes> keys;
        private final Map<Bytes, byte[]> entries;

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
            final Bytes lastKey = keys.next();
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
