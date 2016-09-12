/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.internals.RecordContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeSet;

/**
 * An in-memory LRU cache store similar to {@link MemoryLRUCache} but byte-based, not
 * record based
 *
 * @see org.apache.kafka.streams.state.Stores#create(String)
 */
public class ThreadCache {
    private final long maxCacheSizeBytes;
    private final Map<String, NamedCache> caches = new HashMap<>();


    public interface DirtyEntryFlushListener {
        void apply(final List<DirtyEntry> dirty);
    }

    public ThreadCache(long maxCacheSizeBytes) {
        this.maxCacheSizeBytes = maxCacheSizeBytes;
    }

    /**
     * Add a listener that is called each time an entry is evicted from the cache or an explicit flush is called
     *
     * @param namespace
     * @param listener
     */
    public void addDirtyEntryFlushListener(final String namespace, DirtyEntryFlushListener listener) {
        final NamedCache cache = getOrCreateCache(namespace);
        cache.setListener(listener);
    }

    public void flush(final String namespace) {
        final NamedCache cache = getCache(namespace);
        if (cache == null) {
            return;
        }
        cache.flush();
    }

    public LRUCacheEntry get(final String namespace, byte[] key) {
        final NamedCache cache = getCache(namespace);
        if (cache == null) {
            return null;
        }
        return cache.get(Bytes.wrap(key));
    }

    public void put(final String namespace, byte[] key, LRUCacheEntry value) {
        final NamedCache cache = getOrCreateCache(namespace);
        cache.put(Bytes.wrap(key), value);
        maybeEvict(namespace);
    }

    public LRUCacheEntry putIfAbsent(final String namespace, byte[] key, LRUCacheEntry value) {
        final NamedCache cache = getOrCreateCache(namespace);
        return cache.putIfAbsent(Bytes.wrap(key), value);
    }

    public void putAll(final String namespace, final List<KeyValue<byte[], LRUCacheEntry>> entries) {
        final NamedCache cache = getOrCreateCache(namespace);
        cache.putAll(entries);
    }

    public LRUCacheEntry delete(final String namespace, final byte[] key) {
        final NamedCache cache = getCache(namespace);
        if (cache == null) {
            return null;
        }

        return cache.delete(Bytes.wrap(key));
    }

    public MemoryLRUCacheBytesIterator range(final String namespace, final byte[] from, final byte[] to) {
        final NamedCache cache = getCache(namespace);
        if (cache == null) {
            return new MemoryLRUCacheBytesIterator(Collections.<Bytes>emptyIterator(), new NamedCache(namespace));
        }
        return new MemoryLRUCacheBytesIterator(cache.keyRange(cacheKey(from), cacheKey(to)), cache);
    }

    public MemoryLRUCacheBytesIterator all(final String namespace) {
        final NamedCache cache = getCache(namespace);
        if (cache == null) {
            return new MemoryLRUCacheBytesIterator(Collections.<Bytes>emptyIterator(), new NamedCache(namespace));
        }
        return new MemoryLRUCacheBytesIterator(cache.allKeys(), cache);
    }


    public long size() {
        long size = 0;
        for (NamedCache cache : caches.values()) {
            size += cache.size();
            if (size < 0) {
                return Long.MAX_VALUE;
            }
        }
        return size;
    }

    long dirtySize(final String name) {
        final NamedCache cache = getCache(name);
        if (cache == null) {
            return 0;
        }
        return cache.dirtySize();
    }

    long sizeBytes() {
        long sizeInBytes = 0;
        for (final NamedCache namedCache : caches.values()) {
            sizeInBytes += namedCache.sizeInBytes();
        }
        return sizeInBytes;
    }

    private void maybeEvict(final String namespace) {
        while (sizeBytes() > maxCacheSizeBytes) {
            final NamedCache cache = getOrCreateCache(namespace);
            cache.evict();
        }
    }

    private synchronized NamedCache getCache(final String namespace) {
        return caches.get(namespace);
    }

    private synchronized NamedCache getOrCreateCache(final String name) {
        NamedCache cache = caches.get(name);
        if (cache == null) {
            cache = new NamedCache(name);
            caches.put(name, cache);
        }
        return cache;
    }

    private Iterator<Bytes> keySetIterator(final Set<Bytes> keySet) {
        final TreeSet<Bytes> copy = new TreeSet<>();
        copy.addAll(keySet);
        return copy.iterator();
    }

    private Bytes cacheKey(final byte[] keyBytes) {
        return Bytes.wrap(keyBytes);
    }


    static class MemoryLRUCacheBytesIterator implements PeekingKeyValueIterator<byte[], LRUCacheEntry> {
        private final Iterator<Bytes> keys;
        private final NamedCache cache;
        private KeyValue<byte[], LRUCacheEntry> nextEntry;

        MemoryLRUCacheBytesIterator(final Iterator<Bytes> keys, final NamedCache cache) {
            this.keys = keys;
            this.cache = cache;
        }

        public byte[] peekNextKey() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return nextEntry.key;
        }

        @Override
        public boolean hasNext() {
            if (nextEntry != null) {
                return true;
            }

            while (keys.hasNext() && nextEntry == null) {
                internalNext();
            }

            return nextEntry != null;
        }

        @Override
        public KeyValue<byte[], LRUCacheEntry> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            final KeyValue<byte[], LRUCacheEntry> result = nextEntry;
            nextEntry = null;
            return result;
        }

        private void internalNext() {
            Bytes cacheKey = keys.next();
            final LRUCacheEntry entry = cache.get(cacheKey);
            if (entry == null) {
                return;
            }

            nextEntry = new KeyValue<>(cacheKey.get(), entry);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove not supported by MemoryLRUCacheBytesIterator");
        }

        @Override
        public void close() {
            // do nothing
        }
    }

    public static class DirtyEntry {
        private final Bytes key;
        private final byte[] newValue;
        private final RecordContext recordContext;

        public DirtyEntry(final Bytes key, final byte[] newValue, final RecordContext recordContext) {
            this.key = key;
            this.newValue = newValue;
            this.recordContext = recordContext;
        }

        public Bytes key() {
            return key;
        }

        public byte[] newValue() {
            return newValue;
        }

        public RecordContext recordContext() {
            return recordContext;
        }
    }

}
