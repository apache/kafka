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
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.internals.RecordContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * An in-memory LRU cache store similar to {@link MemoryLRUCache} but byte-based, not
 * record based
 *
 * @see org.apache.kafka.streams.state.Stores#create(String)
 */
public class ThreadCache {
    private static final Logger log = LoggerFactory.getLogger(ThreadCache.class);

    private final String name;
    private final long maxCacheSizeBytes;
    private final StreamsMetrics metrics;
    private final Map<String, NamedCache> caches = new HashMap<>();

    // internal stats
    private long numPuts = 0;
    private long numGets = 0;
    private long numEvicts = 0;
    private long numFlushes = 0;

    public interface DirtyEntryFlushListener {
        void apply(final List<DirtyEntry> dirty);
    }

    public ThreadCache(final String name, long maxCacheSizeBytes, final StreamsMetrics metrics) {
        this.name = name;
        this.maxCacheSizeBytes = maxCacheSizeBytes;
        this.metrics = metrics;
    }

    public long puts() {
        return numPuts;
    }

    public long gets() {
        return numGets;
    }

    public long evicts() {
        return numEvicts;
    }

    public long flushes() {
        return numFlushes;
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
        numFlushes++;

        final NamedCache cache = getCache(namespace);
        if (cache == null) {
            return;
        }
        cache.flush();

        log.debug("Thread {} cache stats on flush: #puts={}, #gets={}, #evicts={}, #flushes={}",
                  name, puts(), gets(), evicts(), flushes());
    }

    public LRUCacheEntry get(final String namespace, Bytes key) {
        numGets++;

        if (key == null) {
            return null;
        }

        final NamedCache cache = getCache(namespace);
        if (cache == null) {
            return null;
        }
        return cache.get(key);
    }

    public void put(final String namespace, Bytes key, LRUCacheEntry value) {
        numPuts++;

        final NamedCache cache = getOrCreateCache(namespace);
        cache.put(key, value);
        maybeEvict(namespace);
    }

    public LRUCacheEntry putIfAbsent(final String namespace, Bytes key, LRUCacheEntry value) {
        final NamedCache cache = getOrCreateCache(namespace);

        final LRUCacheEntry result = cache.putIfAbsent(key, value);
        maybeEvict(namespace);

        if (result == null) {
            numPuts++;
        }
        return result;
    }

    public void putAll(final String namespace, final List<KeyValue<Bytes, LRUCacheEntry>> entries) {
        for (KeyValue<Bytes, LRUCacheEntry> entry : entries) {
            put(namespace, entry.key, entry.value);
        }
    }

    public LRUCacheEntry delete(final String namespace, final Bytes key) {
        final NamedCache cache = getCache(namespace);
        if (cache == null) {
            return null;
        }

        return cache.delete(key);
    }

    public MemoryLRUCacheBytesIterator range(final String namespace, final Bytes from, final Bytes to) {
        final NamedCache cache = getCache(namespace);
        if (cache == null) {
            return new MemoryLRUCacheBytesIterator(Collections.<Bytes>emptyIterator(), new NamedCache(namespace, this.metrics));
        }
        return new MemoryLRUCacheBytesIterator(cache.keyRange(from, to), cache);
    }

    public MemoryLRUCacheBytesIterator all(final String namespace) {
        final NamedCache cache = getCache(namespace);
        if (cache == null) {
            return new MemoryLRUCacheBytesIterator(Collections.<Bytes>emptyIterator(), new NamedCache(namespace, this.metrics));
        }
        return new MemoryLRUCacheBytesIterator(cache.allKeys(), cache);
    }


    public long size() {
        long size = 0;
        for (NamedCache cache : caches.values()) {
            size += cache.size();
            if (isOverflowing(size)) {
                return Long.MAX_VALUE;
            }
        }

        if (isOverflowing(size)) {
            return Long.MAX_VALUE;
        }
        return size;
    }

    private boolean isOverflowing(final long size) {
        return size < 0;
    }

    long sizeBytes() {
        long sizeInBytes = 0;
        for (final NamedCache namedCache : caches.values()) {
            sizeInBytes += namedCache.sizeInBytes();
        }
        return sizeInBytes;
    }

    synchronized void close(final String namespace) {
        final NamedCache removed = caches.remove(namespace);
        if (removed != null) {
            removed.close();
        }
    }

    private void maybeEvict(final String namespace) {
        while (sizeBytes() > maxCacheSizeBytes) {
            final NamedCache cache = getOrCreateCache(namespace);
            // we abort here as the put on this cache may have triggered
            // a put on another cache. So even though the sizeInBytes() is
            // still > maxCacheSizeBytes there is nothing to evict from this
            // namespaced cache.
            if (cache.size() == 0) {
                return;
            }
            log.trace("Thread {} evicting cache {}", name, namespace);
            cache.evict();
            numEvicts++;
        }
    }

    private synchronized NamedCache getCache(final String namespace) {
        return caches.get(namespace);
    }

    private synchronized NamedCache getOrCreateCache(final String name) {
        NamedCache cache = caches.get(name);
        if (cache == null) {
            cache = new NamedCache(name, this.metrics);
            caches.put(name, cache);
        }
        return cache;
    }

    static class MemoryLRUCacheBytesIterator implements PeekingKeyValueIterator<Bytes, LRUCacheEntry> {
        private final Iterator<Bytes> keys;
        private final NamedCache cache;
        private KeyValue<Bytes, LRUCacheEntry> nextEntry;

        MemoryLRUCacheBytesIterator(final Iterator<Bytes> keys, final NamedCache cache) {
            this.keys = keys;
            this.cache = cache;
        }

        public Bytes peekNextKey() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return nextEntry.key;
        }


        public KeyValue<Bytes, LRUCacheEntry> peekNext() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return nextEntry;
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
        public KeyValue<Bytes, LRUCacheEntry> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            final KeyValue<Bytes, LRUCacheEntry> result = nextEntry;
            nextEntry = null;
            return result;
        }

        private void internalNext() {
            Bytes cacheKey = keys.next();
            final LRUCacheEntry entry = cache.get(cacheKey);
            if (entry == null) {
                return;
            }

            nextEntry = new KeyValue<>(cacheKey, entry);
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

    static class DirtyEntry {
        private final Bytes key;
        private final byte[] newValue;
        private final RecordContext recordContext;

        DirtyEntry(final Bytes key, final byte[] newValue, final RecordContext recordContext) {
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
