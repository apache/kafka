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
import org.apache.kafka.common.utils.CircularIterator;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An in-memory LRU cache store similar to {@link MemoryLRUCache} but byte-based, not
 * record based
 */
public class ThreadCache {
    private final Logger log;
    private volatile long maxCacheSizeBytes;
    private final StreamsMetricsImpl metrics;
    private final Map<String, NamedCache> caches = new HashMap<>();

    // Invariant: equal to sum of sizeInBytes of caches
    private final AtomicLong sizeInBytes = new AtomicLong();

    // internal stats
    private long numPuts = 0;
    private long numGets = 0;
    private long numEvicts = 0;
    private long numFlushes = 0;

    public interface DirtyEntryFlushListener {
        void apply(final List<DirtyEntry> dirty);
    }

    public ThreadCache(final LogContext logContext, final long maxCacheSizeBytes, final StreamsMetricsImpl metrics) {
        this.maxCacheSizeBytes = maxCacheSizeBytes;
        this.metrics = metrics;
        this.log = logContext.logger(getClass());
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

    public synchronized void resize(final long newCacheSizeBytes) {
        final boolean shrink = newCacheSizeBytes < maxCacheSizeBytes;
        maxCacheSizeBytes = newCacheSizeBytes;
        if (shrink) {
            log.debug("Cache size was shrunk to {}", newCacheSizeBytes);
            if (caches.values().isEmpty()) {
                return;
            }
            final CircularIterator<NamedCache> circularIterator = new CircularIterator<>(caches.values());
            while (sizeInBytes.get() > maxCacheSizeBytes) {
                final NamedCache cache = circularIterator.next();
                synchronized (cache) {
                    final long oldSize = cache.sizeInBytes();
                    cache.evict();
                    sizeInBytes.getAndAdd(cache.sizeInBytes() - oldSize);
                }
                numEvicts++;
            }
        } else {
            log.debug("Cache size was expanded to {}", newCacheSizeBytes);
        }
    }

    /**
     * The thread cache maintains a set of {@link NamedCache}s whose names are a concatenation of the task ID and the
     * underlying store name. This method creates those names.
     * @param taskIDString Task ID
     * @param underlyingStoreName Underlying store name
     */
    public static String nameSpaceFromTaskIdAndStore(final String taskIDString, final String underlyingStoreName) {
        return taskIDString + "-" + underlyingStoreName;
    }

    /**
     * Given a cache name of the form taskid-storename, return the task ID.
     */
    public static String taskIDfromCacheName(final String cacheName) {
        final String[] tokens = cacheName.split("-", 2);
        return tokens[0];
    }

    /**
     * Given a cache name of the form taskid-storename, return the store name.
     */
    public static String underlyingStoreNamefromCacheName(final String cacheName) {
        final String[] tokens = cacheName.split("-", 2);
        return tokens[1];
    }


    /**
     * Add a listener that is called each time an entry is evicted from the cache or an explicit flush is called
     */
    public void addDirtyEntryFlushListener(final String namespace, final DirtyEntryFlushListener listener) {
        final NamedCache cache = getOrCreateCache(namespace);
        cache.setListener(listener);
    }

    public void flush(final String namespace) {
        numFlushes++;

        final NamedCache cache = getCache(namespace);
        if (cache == null) {
            return;
        }

        synchronized (cache) {
            final long oldSize = cache.sizeInBytes();
            cache.flush();
            sizeInBytes.getAndAdd(cache.sizeInBytes() - oldSize);
        }

        if (log.isTraceEnabled()) {
            log.trace("Cache stats on flush: #puts={}, #gets={}, #evicts={}, #flushes={}", puts(), gets(), evicts(), flushes());
        }
    }

    public LRUCacheEntry get(final String namespace, final Bytes key) {
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

    public void put(final String namespace, final Bytes key, final LRUCacheEntry value) {
        numPuts++;

        final NamedCache cache = getOrCreateCache(namespace);

        synchronized (cache) {
            final long oldSize = cache.sizeInBytes();
            cache.put(key, value);
            sizeInBytes.getAndAdd(cache.sizeInBytes() - oldSize);
            maybeEvict(namespace, cache);
        }
    }

    public LRUCacheEntry putIfAbsent(final String namespace, final Bytes key, final LRUCacheEntry value) {
        final NamedCache cache = getOrCreateCache(namespace);
        final LRUCacheEntry result;
        synchronized (cache) {
            final long oldSize = cache.sizeInBytes();
            result = cache.putIfAbsent(key, value);
            sizeInBytes.getAndAdd(cache.sizeInBytes() - oldSize);
            maybeEvict(namespace, cache);
        }

        if (result == null) {
            numPuts++;
        }
        return result;
    }

    public void putAll(final String namespace, final List<KeyValue<Bytes, LRUCacheEntry>> entries) {
        for (final KeyValue<Bytes, LRUCacheEntry> entry : entries) {
            put(namespace, entry.key, entry.value);
        }
    }

    public LRUCacheEntry delete(final String namespace, final Bytes key) {
        final NamedCache cache = getCache(namespace);
        if (cache == null) {
            return null;
        }

        final LRUCacheEntry entry;
        synchronized (cache) {
            final long oldSize = cache.sizeInBytes();
            entry = cache.delete(key);
            sizeInBytes.getAndAdd(cache.sizeInBytes() - oldSize);
        }
        return entry;
    }

    public MemoryLRUCacheBytesIterator range(final String namespace, final Bytes from, final Bytes to) {
        return range(namespace, from, to, true);
    }

    public MemoryLRUCacheBytesIterator range(final String namespace, final Bytes from, final Bytes to, final boolean toInclusive) {
        final NamedCache cache = getCache(namespace);
        if (cache == null) {
            return new MemoryLRUCacheBytesIterator(Collections.emptyIterator(), new NamedCache(namespace, this.metrics));
        }
        return new MemoryLRUCacheBytesIterator(cache.keyRange(from, to, toInclusive), cache);
    }

    public MemoryLRUCacheBytesIterator reverseRange(final String namespace, final Bytes from, final Bytes to) {
        final NamedCache cache = getCache(namespace);
        if (cache == null) {
            return new MemoryLRUCacheBytesIterator(Collections.emptyIterator(), new NamedCache(namespace, this.metrics));
        }
        return new MemoryLRUCacheBytesIterator(cache.reverseKeyRange(from, to), cache);
    }

    public MemoryLRUCacheBytesIterator all(final String namespace) {
        final NamedCache cache = getCache(namespace);
        if (cache == null) {
            return new MemoryLRUCacheBytesIterator(Collections.emptyIterator(), new NamedCache(namespace, this.metrics));
        }
        return new MemoryLRUCacheBytesIterator(cache.allKeys(), cache);
    }

    public MemoryLRUCacheBytesIterator reverseAll(final String namespace) {
        final NamedCache cache = getCache(namespace);
        if (cache == null) {
            return new MemoryLRUCacheBytesIterator(Collections.emptyIterator(), new NamedCache(namespace, this.metrics));
        }
        return new MemoryLRUCacheBytesIterator(cache.reverseAllKeys(), cache);
    }

    public long size() {
        long size = 0;
        for (final NamedCache cache : caches.values()) {
            size += cache.size();
            if (isOverflowing(size)) {
                return Long.MAX_VALUE;
            }
        }
        return size;
    }

    private boolean isOverflowing(final long size) {
        return size < 0;
    }

    long sizeBytes() {
        return sizeInBytes.get();
    }

    synchronized void close(final String namespace) {
        final NamedCache removed = caches.remove(namespace);
        if (removed != null) {
            sizeInBytes.getAndAdd(-removed.sizeInBytes());
            removed.close();
        }
    }

    synchronized void clear(final String namespace) {
        final NamedCache cleared = caches.get(namespace);
        if (cleared != null) {
            sizeInBytes.getAndAdd(-cleared.sizeInBytes());
            cleared.clear();
        }
    }

    private void maybeEvict(final String namespace, final NamedCache cache) {
        int numEvicted = 0;
        while (sizeInBytes.get() > maxCacheSizeBytes) {
            // we abort here as the put on this cache may have triggered
            // a put on another cache. So even though the sizeInBytes() is
            // still > maxCacheSizeBytes there is nothing to evict from this
            // namespaced cache.
            if (cache.isEmpty()) {
                return;
            }
            final long oldSize = cache.sizeInBytes();
            cache.evict();
            sizeInBytes.getAndAdd(cache.sizeInBytes() - oldSize);
            numEvicts++;
            numEvicted++;
        }
        if (log.isTraceEnabled()) {
            log.trace("Evicted {} entries from cache {}", numEvicted, namespace);
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
            final Bytes cacheKey = keys.next();
            final LRUCacheEntry entry = cache.get(cacheKey);
            if (entry == null) {
                return;
            }

            nextEntry = new KeyValue<>(cacheKey, entry);
        }

        @Override
        public void close() {
            // do nothing
        }
    }

    static class DirtyEntry {
        private final Bytes key;
        private final byte[] newValue;
        private final LRUCacheEntry recordContext;

        DirtyEntry(final Bytes key, final byte[] newValue, final LRUCacheEntry recordContext) {
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

        public LRUCacheEntry entry() {
            return recordContext;
        }
    }
}
