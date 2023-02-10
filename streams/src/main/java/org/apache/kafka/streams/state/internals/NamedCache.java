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

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.metrics.TaskMetrics;
import org.apache.kafka.streams.state.internals.metrics.NamedCacheMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

class NamedCache {
    private static final Logger log = LoggerFactory.getLogger(NamedCache.class);
    private final String name;
    private final String storeName;
    private final String taskName;
    private final NavigableMap<Bytes, LRUNode> cache = new TreeMap<>();
    private final Set<Bytes> dirtyKeys = new LinkedHashSet<>();
    private ThreadCache.DirtyEntryFlushListener listener;
    private LRUNode tail;
    private LRUNode head;
    private long currentSizeBytes;

    private final StreamsMetricsImpl streamsMetrics;
    private final Sensor hitRatioSensor;
    private final Sensor totalCacheSizeSensor;

    // internal stats
    private long numReadHits = 0;
    private long numReadMisses = 0;
    private long numOverwrites = 0;
    private long numFlushes = 0;

    NamedCache(final String name, final StreamsMetricsImpl streamsMetrics) {
        this.name = name;
        this.streamsMetrics = streamsMetrics;
        storeName = ThreadCache.underlyingStoreNamefromCacheName(name);
        taskName = ThreadCache.taskIDfromCacheName(name);
        hitRatioSensor = NamedCacheMetrics.hitRatioSensor(
            streamsMetrics,
            Thread.currentThread().getName(),
            taskName,
            storeName
        );
        totalCacheSizeSensor = TaskMetrics.totalCacheSizeBytesSensor(
                Thread.currentThread().getName(),
                taskName,
                streamsMetrics
        );
    }

    synchronized final String name() {
        return name;
    }

    synchronized long hits() {
        return numReadHits;
    }

    synchronized long misses() {
        return numReadMisses;
    }

    synchronized long overwrites() {
        return numOverwrites;
    }

    synchronized long flushes() {
        return numFlushes;
    }

    synchronized LRUCacheEntry get(final Bytes key) {
        if (key == null) {
            return null;
        }

        final LRUNode node = getInternal(key);
        if (node == null) {
            return null;
        }
        updateLRU(node);
        return node.entry;
    }

    synchronized void setListener(final ThreadCache.DirtyEntryFlushListener listener) {
        this.listener = listener;
    }

    synchronized void flush() {
        flush(null);
    }

    private void flush(final LRUNode evicted) {
        numFlushes++;

        if (log.isTraceEnabled()) {
            log.trace("Named cache {} stats on flush: #hits={}, #misses={}, #overwrites={}, #flushes={}",
                name, hits(), misses(), overwrites(), flushes());
        }

        if (listener == null) {
            throw new IllegalArgumentException("No listener for namespace " + name + " registered with cache");
        }

        if (dirtyKeys.isEmpty()) {
            return;
        }

        final List<ThreadCache.DirtyEntry> entries = new ArrayList<>();
        final List<Bytes> deleted = new ArrayList<>();

        // evicted already been removed from the cache so add it to the list of
        // flushed entries and remove from dirtyKeys.
        if (evicted != null) {
            entries.add(new ThreadCache.DirtyEntry(evicted.key, evicted.entry.value(), evicted.entry));
            dirtyKeys.remove(evicted.key);
        }

        for (final Bytes key : dirtyKeys) {
            final LRUNode node = getInternal(key);
            if (node == null) {
                throw new IllegalStateException("Key = " + key + " found in dirty key set, but entry is null");
            }
            entries.add(new ThreadCache.DirtyEntry(key, node.entry.value(), node.entry));
            node.entry.markClean();
            if (node.entry.value() == null) {
                deleted.add(node.key);
            }
        }
        // clear dirtyKeys before the listener is applied as it may be re-entrant.
        dirtyKeys.clear();
        listener.apply(entries);
        for (final Bytes key : deleted) {
            delete(key);
        }
    }

    synchronized void put(final Bytes key, final LRUCacheEntry value) {
        if (!value.isDirty() && dirtyKeys.contains(key)) {
            throw new IllegalStateException(
                String.format(
                    "Attempting to put a clean entry for key [%s] into NamedCache [%s] when it already contains a dirty entry for the same key",
                    key, name
                )
            );
        }
        LRUNode node = cache.get(key);
        if (node != null) {
            numOverwrites++;

            currentSizeBytes -= node.size();
            node.update(value);
            updateLRU(node);
        } else {
            node = new LRUNode(key, value);
            // put element
            putHead(node);
            cache.put(key, node);
        }
        if (value.isDirty()) {
            // first remove and then add so we can maintain ordering as the arrival order of the records.
            dirtyKeys.remove(key);
            dirtyKeys.add(key);
        }
        currentSizeBytes += node.size();
        totalCacheSizeSensor.record(currentSizeBytes);
    }

    synchronized long sizeInBytes() {
        return currentSizeBytes;
    }

    private LRUNode getInternal(final Bytes key) {
        final LRUNode node = cache.get(key);
        if (node == null) {
            numReadMisses++;

            return null;
        } else {
            numReadHits++;
            hitRatioSensor.record((double) numReadHits / (double) (numReadHits + numReadMisses));
        }
        return node;
    }

    private void updateLRU(final LRUNode node) {
        remove(node);

        putHead(node);
    }

    private void remove(final LRUNode node) {
        if (node.previous != null) {
            node.previous.next = node.next;
        } else {
            head = node.next;
        }
        if (node.next != null) {
            node.next.previous = node.previous;
        } else {
            tail = node.previous;
        }
    }

    private void putHead(final LRUNode node) {
        node.next = head;
        node.previous = null;
        if (head != null) {
            head.previous = node;
        }
        head = node;
        if (tail == null) {
            tail = head;
        }
    }

    synchronized void evict() {
        if (tail == null) {
            return;
        }
        final LRUNode eldest = tail;
        currentSizeBytes -= eldest.size();
        remove(eldest);
        cache.remove(eldest.key);
        if (eldest.entry.isDirty()) {
            flush(eldest);
        }
        totalCacheSizeSensor.record(currentSizeBytes);
    }

    synchronized LRUCacheEntry putIfAbsent(final Bytes key, final LRUCacheEntry value) {
        final LRUCacheEntry originalValue = get(key);
        if (originalValue == null) {
            put(key, value);
        }
        return originalValue;
    }

    synchronized void putAll(final List<KeyValue<byte[], LRUCacheEntry>> entries) {
        for (final KeyValue<byte[], LRUCacheEntry> entry : entries) {
            put(Bytes.wrap(entry.key), entry.value);
        }
    }

    synchronized LRUCacheEntry delete(final Bytes key) {
        final LRUNode node = cache.remove(key);

        if (node == null) {
            return null;
        }

        remove(node);
        dirtyKeys.remove(key);
        currentSizeBytes -= node.size();
        totalCacheSizeSensor.record(currentSizeBytes);
        return node.entry();
    }

    public long size() {
        return cache.size();
    }

    public boolean isEmpty() {
        return cache.isEmpty();
    }

    synchronized Iterator<Bytes> keyRange(final Bytes from, final Bytes to, final boolean toInclusive) {
        final Set<Bytes> rangeSet = computeSubSet(from, to, toInclusive);
        return keySetIterator(rangeSet, true);
    }

    synchronized Iterator<Bytes> reverseKeyRange(final Bytes from, final Bytes to) {
        final Set<Bytes> rangeSet = computeSubSet(from, to, true);
        return keySetIterator(rangeSet, false);
    }

    private Set<Bytes> computeSubSet(final Bytes from, final Bytes to, final boolean toInclusive) {
        if (from == null && to == null) {
            return cache.navigableKeySet();
        } else if (from == null) {
            return cache.headMap(to, toInclusive).keySet();
        } else if (to == null) {
            return cache.tailMap(from, true).keySet();
        } else if (from.compareTo(to) > 0) {
            return Collections.emptyNavigableSet();
        } else {
            return cache.navigableKeySet().subSet(from, true, to, toInclusive);
        }
    }

    private Iterator<Bytes> keySetIterator(final Set<Bytes> keySet, final boolean forward) {
        if (forward) {
            return new TreeSet<>(keySet).iterator();
        } else {
            return new TreeSet<>(keySet).descendingIterator();
        }
    }

    synchronized Iterator<Bytes> allKeys() {
        return keySetIterator(cache.navigableKeySet(), true);
    }

    synchronized Iterator<Bytes> reverseAllKeys() {
        return keySetIterator(cache.navigableKeySet(), false);
    }

    synchronized LRUCacheEntry first() {
        if (head == null) {
            return null;
        }
        return head.entry;
    }

    synchronized LRUCacheEntry last() {
        if (tail == null) {
            return null;
        }
        return tail.entry;
    }

    synchronized LRUNode head() {
        return head;
    }

    synchronized LRUNode tail() {
        return tail;
    }

    synchronized void close() {
        head = tail = null;
        listener = null;
        currentSizeBytes = 0;
        dirtyKeys.clear();
        cache.clear();
        streamsMetrics.removeAllCacheLevelSensors(Thread.currentThread().getName(), taskName, storeName);
    }

    /**
     * A simple wrapper class to implement a doubly-linked list around MemoryLRUCacheBytesEntry
     */
    static class LRUNode {
        private final Bytes key;
        private LRUCacheEntry entry;
        private LRUNode previous;
        private LRUNode next;

        LRUNode(final Bytes key, final LRUCacheEntry entry) {
            this.key = key;
            this.entry = entry;
        }

        LRUCacheEntry entry() {
            return entry;
        }

        Bytes key() {
            return key;
        }

        long size() {
            return key.get().length +
                8 + // entry
                8 + // previous
                8 + // next
                entry.size();
        }

        LRUNode next() {
            return next;
        }

        LRUNode previous() {
            return previous;
        }

        private void update(final LRUCacheEntry entry) {
            this.entry = entry;
        }
    }

}
