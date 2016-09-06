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
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.RecordContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * An in-memory LRU cache store similar to {@link MemoryLRUCache} but byte-based, not
 * record based
 *
 * @see org.apache.kafka.streams.state.Stores#create(String)
 */
public class MemoryLRUCacheBytes {
    private final long maxCacheSizeBytes;
    private long currentSizeBytes = 0;
    private LRUNode head = null;
    private LRUNode tail = null;
    private final Map<String, TreeMap<Bytes, LRUNode>> map;
    private final Map<String, DirtyEntryFlushListener> listeners;
    private final Map<String, Set<Bytes>> dirtKeys;


    public interface DirtyEntryFlushListener {
        void apply(final List<DirtyEntry> dirty);
    }


    public MemoryLRUCacheBytes(long maxCacheSizeBytes) {
        this.maxCacheSizeBytes = maxCacheSizeBytes;
        this.map = new HashMap<>();
        listeners = new HashMap<>();
        dirtKeys = new HashMap<>();
    }

    /**
     * Add a listener that is called each time an entry is evicted from the cache or an explicit flush is called
     * @param namespace
     * @param listener
     */
    public synchronized void addDirtyEntryFlushListener(final String namespace, DirtyEntryFlushListener listener) {
        this.listeners.put(namespace, listener);
    }

    public synchronized void flush(final String namespace) {
        final DirtyEntryFlushListener listener = listeners.get(namespace);
        if (listener == null) {
            throw new IllegalArgumentException("No listener for namespace " + namespace + " registered with cache");
        }

        final Set<Bytes> keys = dirtKeys.get(namespace);
        if (keys == null || keys.isEmpty()) {
            return;
        }
        final List<DirtyEntry> entries  = new ArrayList<>();
        for (Bytes key : keys) {
            final MemoryLRUCacheBytesEntry entry = get(namespace, key.get());
            entries.add(new DirtyEntry(key, entry.value, entry));
        }
        listener.apply(entries);
        keys.clear();
    }

    private void callListener(final Bytes key, final LRUNode node) {
        final MemoryLRUCacheBytesEntry entry = node.entry();
        final DirtyEntryFlushListener listener = listeners.get(node.namespace);
        if (listener == null) {
            return;
        }
        listener.apply(Collections.singletonList(new DirtyEntry(key, entry.value, entry)));
    }

    public synchronized MemoryLRUCacheBytesEntry get(final String namespace, byte[] key) {
        MemoryLRUCacheBytesEntry entry = null;
        // get map
        final TreeMap<Bytes, LRUNode> treeMap = this.map.get(namespace);
        if (treeMap != null) {
            // get element
            LRUNode node = treeMap.get(cacheKey(key));
            if (node != null) {
                entry = node.entry();
                updateLRU(node);
            }
        }
        return entry;
    }

    private void maybeEvict() {
        while (sizeBytes() > maxCacheSizeBytes) {
            final Bytes key = tail.key;
            final String namespace = tail.namespace;
            final TreeMap<Bytes, LRUNode> treeMap = this.map.get(namespace);
            final LRUNode toRemove = treeMap.get(key);
            currentSizeBytes -= toRemove.size();
            if (toRemove.entry.isDirty()) {
                callListener(toRemove.key, toRemove);
            }
            remove(toRemove);
            treeMap.remove(key);

            if (toRemove.entry.isDirty()) {
                removeDirtyKey(namespace, key);
            }
        }
    }


    public synchronized void put(final String namespace, byte[] key, MemoryLRUCacheBytesEntry value) {
        final Bytes cacheKey = cacheKey(key);
        LRUNode node;
        // get map
        if (!this.map.containsKey(namespace)) {
            this.map.put(namespace, new TreeMap<Bytes, LRUNode>());
        }

        final TreeMap<Bytes, LRUNode> treeMap = this.map.get(namespace);

        if (treeMap.containsKey(cacheKey)) {
            node = treeMap.get(cacheKey);
            currentSizeBytes -= node.size();
            node.update(value);
            updateLRU(node);
        } else {
            node = new LRUNode(cacheKey, namespace, value);
            // put element
            putHead(node);
            treeMap.put(cacheKey, node);
        }
        if (value.isDirty()) {
            addDirtyKey(namespace, cacheKey);
        }

        currentSizeBytes += node.size();
        maybeEvict();
    }

    public synchronized MemoryLRUCacheBytesEntry putIfAbsent(final String namespace, byte[] key, MemoryLRUCacheBytesEntry value) {
        final MemoryLRUCacheBytesEntry originalValue = get(namespace, key);
        if (originalValue == null) {
            put(namespace, key, value);
        }
        return originalValue;
    }

    public synchronized void putAll(final String namespace, final List<KeyValue<byte[], MemoryLRUCacheBytesEntry>> entries) {
        for (KeyValue<byte[], MemoryLRUCacheBytesEntry> entry : entries) {
            put(namespace, entry.key, entry.value);
        }
    }

    public synchronized MemoryLRUCacheBytesEntry delete(final String namespace, final byte[] key) {
        final Bytes cacheKey = cacheKey(key);
        final TreeMap<Bytes, LRUNode> treeMap = this.map.get(namespace);
        if (treeMap == null) {
            return null;
        }

        final LRUNode node = treeMap.get(cacheKey);

        if (node == null) {
            return null;
        }

        // remove from LRU list
        remove(node);

        // remove from map
        final LRUNode value = treeMap.remove(cacheKey);
        if (treeMap.size() == 0) {
            this.map.remove(namespace);
        }
        removeDirtyKey(namespace, cacheKey);
        return value.entry();
    }


    public synchronized int dirtySize(final String namespace) {
        final Set<Bytes> dirtyKeys = dirtKeys.get(namespace);
        if (dirtyKeys == null) {
            return 0;
        }
        return dirtyKeys.size();
    }

    public int size() {
        int size = 0;
        for (Map.Entry<String, TreeMap<Bytes, LRUNode>> entry: this.map.entrySet()) {
            size += entry.getValue().size();
        }
        if (size < 0) {
            return Integer.MAX_VALUE;
        }
        return size;
    }

    public long sizeBytes() {
        return currentSizeBytes;
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


    private void addDirtyKey(final String namespace, final Bytes cacheKey) {
        if (!dirtKeys.containsKey(namespace)) {
            dirtKeys.put(namespace, new LinkedHashSet<Bytes>());
        }
        final Set<Bytes> dirtyKeys = dirtKeys.get(namespace);
        // first remove and then add so we can maintain ordering as the arrival order of the records.
        dirtyKeys.remove(cacheKey);
        dirtyKeys.add(cacheKey);
    }

    private void removeDirtyKey(final String namespace, final Bytes cacheKey) {
        if (dirtKeys.containsKey(namespace)) {
            final Set<Bytes> dirtyKeys = dirtKeys.get(namespace);
            dirtyKeys.remove(cacheKey);
        }
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

    /**
     * Update the LRU position of this node to be the head of list
     */
    private void updateLRU(LRUNode node) {
        remove(node);

        putHead(node);
    }

    protected LRUNode head() {
        return head;
    }

    protected LRUNode tail() {
        return tail;
    }


    /**
     * A simple wrapper class to implement a doubly-linked list around MemoryLRUCacheBytesEntry
     */
    protected class LRUNode {
        private final Bytes key;
        private final String namespace;
        private MemoryLRUCacheBytesEntry entry;
        private LRUNode previous;
        private LRUNode next;

        LRUNode(final Bytes key, final String namespace, final MemoryLRUCacheBytesEntry entry) {
            this.key = key;
            this.namespace = namespace;
            this.entry = entry;
        }

        public MemoryLRUCacheBytesEntry entry() {
            return entry;
        }

        public void update(MemoryLRUCacheBytesEntry entry) {
            this.entry = entry;
        }

        public long size() {
            return key.get().length +
                namespace.length() +
                8 + // entry
                8 + // previous
                8 + // next
                entry.size();
        }
    }

    public synchronized MemoryLRUCacheBytesIterator range(final String namespace, final byte[] from, final byte[] to) {
        final TreeMap<Bytes, LRUNode> treeMap = this.map.get(namespace);
        if (treeMap == null) {
            throw new NoSuchElementException("Namespace " + namespace + " doesn't exist");
        }
        return new MemoryLRUCacheBytesIterator(namespace, keySetIterator(treeMap.navigableKeySet().subSet(cacheKey(from), true, cacheKey(to), true)), treeMap);
    }

    public MemoryLRUCacheBytesIterator all(final String namespace) {
        final TreeMap<Bytes, LRUNode> treeMap = this.map.get(namespace);
        if (treeMap == null) {
            throw new NoSuchElementException("Namespace " + namespace + " doesn't exist");
        }
        return new MemoryLRUCacheBytesIterator(namespace, keySetIterator(treeMap.keySet()), treeMap);
    }


    private Iterator<Bytes> keySetIterator(final Set<Bytes> keySet) {
        final TreeSet<Bytes> copy = new TreeSet<>();
        copy.addAll(keySet);
        return copy.iterator();
    }

    private Bytes cacheKey(final byte[] keyBytes) {
        return Bytes.wrap(keyBytes);
    }


    static class MemoryLRUCacheBytesIterator implements PeekingKeyValueIterator<byte[], MemoryLRUCacheBytesEntry> {
        private final String namespace;
        private final Iterator<Bytes> keys;
        private final Map<Bytes, LRUNode> entries;
        private KeyValue<byte[], MemoryLRUCacheBytesEntry> nextEntry;

        MemoryLRUCacheBytesIterator(final String namespace, final Iterator<Bytes> keys, final Map<Bytes, LRUNode> entries) {
            this.namespace = namespace;
            this.keys = keys;
            this.entries = entries;
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
        public KeyValue<byte[], MemoryLRUCacheBytesEntry> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            final KeyValue<byte[], MemoryLRUCacheBytesEntry> result = nextEntry;
            nextEntry = null;
            return result;
        }

        private void internalNext() {
            Bytes cacheKey = keys.next();
            final LRUNode lruNode = entries.get(cacheKey);
            if (lruNode == null) {
                return;
            }

            if (!namespace.equals(lruNode.namespace)) {
                throw new ProcessorStateException("Namespace mismatch in MemoryLRUCacheBytesIterator");
            }

            nextEntry = new KeyValue<>(cacheKey.get(), lruNode.entry());
        }

        @Override
        public void remove() {
            // do nothing
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
