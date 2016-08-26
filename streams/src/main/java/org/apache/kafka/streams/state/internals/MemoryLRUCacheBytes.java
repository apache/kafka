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

import java.util.HashMap;
import java.util.Iterator;
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
    private final Map<String, MemoryLRUCacheBytes.EldestEntryRemovalListener> listeners;

    public interface EldestEntryRemovalListener {
        void apply(byte[] key, MemoryLRUCacheBytesEntry value);
    }

    public MemoryLRUCacheBytes(long maxCacheSizeBytes) {
        this.maxCacheSizeBytes = maxCacheSizeBytes;
        this.map = new HashMap<>();
        listeners = new HashMap<>();
    }

    /**
     * Add a listener that is called each time an entry is evicted from cache
     * @param namespace
     * @param listener
     */
    public void addEldestRemovedListener(final String namespace, EldestEntryRemovalListener listener) {
        this.listeners.put(namespace, listener);
    }

    private void callListener(final Bytes key, LRUNode node) {
        MemoryLRUCacheBytesEntry entry = node.entry();
        final EldestEntryRemovalListener listener = listeners.get(node.namespace);
        if (listener == null) {
            return;
        }
        listener.apply(key.get(), entry);
    }

    public synchronized MemoryLRUCacheBytesEntry get(final String namespace, byte[] key) {
        MemoryLRUCacheBytesEntry entry = null;
        // get map
        TreeMap<Bytes, LRUNode> treeMap = this.map.get(namespace);
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

    /**
     * Check if we have enough space in cache to accept new element
     *
     * @param newElement new element to be insterted
     */
    private void maybeEvict(LRUNode newElement) {
        while (sizeBytes() + newElement.size() > maxCacheSizeBytes) {
            final Bytes key = tail.key;
            final String namespace = tail.namespace;
            TreeMap<Bytes, LRUNode> treeMap = this.map.get(namespace);
            LRUNode toRemove = treeMap.get(key);
            currentSizeBytes -= toRemove.size();
            remove(tail);
            callListener(toRemove.key, toRemove);
            treeMap.remove(key);
        }
    }


    public synchronized void put(final String namespace, byte[] key, MemoryLRUCacheBytesEntry value) {
        final Bytes cacheKey = cacheKey(key);
        LRUNode node;
        // get map
        TreeMap<Bytes, LRUNode> treeMap = null;
        if (!this.map.containsKey(namespace)) {
            treeMap = new TreeMap<>();
            this.map.put(namespace, treeMap);
        } else {
            treeMap = this.map.get(namespace);
        }

        if (treeMap.containsKey(cacheKey)) {
            node = treeMap.get(cacheKey);
            currentSizeBytes -= node.size();
            node.update(value);
            updateLRU(node);
        } else {
            node = new LRUNode(cacheKey, namespace, value);
            // check if we need to evict anything
            maybeEvict(node);
            // put element
            putHead(node);
            treeMap.put(cacheKey, node);
        }

        currentSizeBytes += node.size();
    }

    public synchronized MemoryLRUCacheBytesEntry putIfAbsent(final String namespace, byte[] key, MemoryLRUCacheBytesEntry value) {
        MemoryLRUCacheBytesEntry originalValue = get(namespace, key);
        if (originalValue == null) {
            put(namespace, key, value);
        }
        return originalValue;
    }

    public synchronized void putAll(final String namespace, List<KeyValue<byte[], MemoryLRUCacheBytesEntry>> entries) {
        for (KeyValue<byte[], MemoryLRUCacheBytesEntry> entry : entries) {
            put(namespace, entry.key, entry.value);
        }
    }

    public synchronized MemoryLRUCacheBytesEntry delete(final String namespace, byte[] key) {
        final Bytes cacheKey = cacheKey(key);
        TreeMap<Bytes, LRUNode> treeMap = this.map.get(namespace);
        LRUNode node = treeMap.get(cacheKey);

        // remove from LRU list
        remove(node);

        // remove from map
        LRUNode value = treeMap.remove(cacheKey);
        if (treeMap.size() == 0) {
            this.map.remove(namespace);
        }
        return value.entry();
    }


    public int size() {
        int size = 0;
        for (Map.Entry<String, TreeMap<Bytes, LRUNode>> entry: this.map.entrySet()) {
            size += entry.getValue().size();
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
        private Bytes key;
        private String namespace;
        private MemoryLRUCacheBytesEntry entry;
        private LRUNode previous;
        private LRUNode next;

        LRUNode(final Bytes key, final String namespace, MemoryLRUCacheBytesEntry entry) {
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
            return key.get().length + namespace.length() + entry.size();
        }
    }

    public synchronized MemoryLRUCacheBytesIterator range(final String namespace, byte[] from, byte[] to) {
        TreeMap<Bytes, LRUNode> treeMap = this.map.get(namespace);
        if (treeMap == null) {
            throw new NoSuchElementException();
        }
        return new MemoryLRUCacheBytesIterator(namespace, keySetIterator(treeMap.navigableKeySet().subSet(cacheKey(from), true, cacheKey(to), true)), treeMap);
    }

    public MemoryLRUCacheBytesIterator all(final String namespace) {
        TreeMap<Bytes, LRUNode> treeMap = this.map.get(namespace);
        if (treeMap == null) {
            throw new NoSuchElementException();
        }
        return new MemoryLRUCacheBytesIterator(namespace, keySetIterator(treeMap.keySet()), treeMap);
    }


    private Iterator<Bytes> keySetIterator(final Set<Bytes> keySet) {
        final TreeSet<Bytes> copy = new TreeSet<>();
        copy.addAll(keySet);
        return copy.iterator();
    }

    private Bytes cacheKey(byte[] keyBytes) {
        return Bytes.wrap(keyBytes);
    }


    static class MemoryLRUCacheBytesIterator implements PeekingKeyValueIterator<byte[], MemoryLRUCacheBytesEntry> {
        private final String namespace;
        private final Iterator<Bytes> keys;
        private final Map<Bytes, LRUNode> entries;
        private KeyValue<byte[], MemoryLRUCacheBytesEntry> nextEntry;

        MemoryLRUCacheBytesIterator(final String namespace, Iterator<Bytes> keys, Map<Bytes, LRUNode> entries) {
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

}
