/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

class NamedCache {
    private static final Logger log = LoggerFactory.getLogger(NamedCache.class);
    private final String name;
    private final TreeMap<Bytes, LRUNode> cache = new TreeMap<>();
    private final Set<Bytes> dirtyKeys = new LinkedHashSet<>();
    private ThreadCache.DirtyEntryFlushListener listener;
    private LRUNode tail;
    private LRUNode head;
    private long currentSizeBytes;
    private ThreadCacheMetrics metrics;

    // JMX stats
    private Sensor hitRatio = null;


    // internal stats
    private long numReadHits = 0;
    private long numReadMisses = 0;
    private long numOverwrites = 0;
    private long numFlushes = 0;

    NamedCache(final String name) {
        this(name, null);
    }

    NamedCache(final String name, final ThreadCacheMetrics metrics) {
        this.name = name;
        this.metrics = metrics != null ? metrics : new ThreadCache.NullThreadCacheMetrics();

        this.hitRatio = this.metrics.addCacheSensor(name, "hitRatio");
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
        numFlushes++;

        log.debug("Named cache {} stats on flush: #hits={}, #misses={}, #overwrites={}, #flushes={}",
            name, hits(), misses(), overwrites(), flushes());

        if (listener == null) {
            throw new IllegalArgumentException("No listener for namespace " + name + " registered with cache");
        }

        if (dirtyKeys.isEmpty()) {
            return;
        }

        final List<ThreadCache.DirtyEntry> entries  = new ArrayList<>();
        for (Bytes key : dirtyKeys) {
            final LRUNode node = getInternal(key);
            if (node == null) {
                throw new IllegalStateException("Key found in dirty key set, but entry is null");
            }
            entries.add(new ThreadCache.DirtyEntry(key, node.entry.value, node.entry));
            node.entry.markClean();
        }
        listener.apply(entries);
        dirtyKeys.clear();
    }


    synchronized void put(final Bytes key, final LRUCacheEntry value) {
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
            metrics.recordCacheSensor(hitRatio, (double) numReadHits / (double) (numReadHits + numReadMisses));
        }
        return node;
    }

    private void updateLRU(LRUNode node) {
        remove(node);

        putHead(node);
    }

    private void remove(LRUNode node) {
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

    private void putHead(LRUNode node) {
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
        if (eldest.entry.isDirty()) {
            flush();
        }
        remove(eldest);
        cache.remove(eldest.key);
    }

    synchronized LRUCacheEntry putIfAbsent(final Bytes key, final LRUCacheEntry value) {
        final LRUCacheEntry originalValue = get(key);
        if (originalValue == null) {
            put(key, value);
        }
        return originalValue;
    }

    synchronized void putAll(final List<KeyValue<byte[], LRUCacheEntry>> entries) {
        for (KeyValue<byte[], LRUCacheEntry> entry : entries) {
            put(Bytes.wrap(entry.key), entry.value);
        }
    }

    synchronized LRUCacheEntry delete(final Bytes key) {
        final LRUNode node = cache.remove(key);

        if (node == null) {
            return null;
        }

        remove(node);
        cache.remove(key);
        dirtyKeys.remove(key);
        currentSizeBytes -= node.size();
        return node.entry();
    }

    public long size() {
        return cache.size();
    }

    synchronized Iterator<Bytes> keyRange(final Bytes from, final Bytes to) {
        return keySetIterator(cache.navigableKeySet().subSet(from, true, to, true));
    }

    private Iterator<Bytes> keySetIterator(final Set<Bytes> keySet) {
        final TreeSet<Bytes> copy = new TreeSet<>();
        copy.addAll(keySet);
        return copy.iterator();
    }
    

    synchronized Iterator<Bytes> allKeys() {
        return keySetIterator(cache.navigableKeySet());
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

    synchronized long dirtySize() {
        return dirtyKeys.size();
    }

    /**
     * A simple wrapper class to implement a doubly-linked list around MemoryLRUCacheBytesEntry
     */
    private class LRUNode {
        private final Bytes key;
        private LRUCacheEntry entry;
        private LRUNode previous;
        private LRUNode next;

        LRUNode(final Bytes key, final LRUCacheEntry entry) {
            this.key = key;
            this.entry = entry;
        }

        public LRUCacheEntry entry() {
            return entry;
        }

        public void update(LRUCacheEntry entry) {
            this.entry = entry;
        }

        public long size() {
            return  key.get().length +
                    8 + // entry
                    8 + // previous
                    8 + // next
                    entry.size();
        }
    }

}
