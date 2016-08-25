/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.List;
import java.util.Map;

/**
 * An in-memory LRU cache store similar to {@link MemoryLRUCache} but byte-based, not
 * record based
 *
 * @see org.apache.kafka.streams.state.Stores#create(String)
 */
public class MemoryLRUCacheBytes implements KeyValueStore<byte[], MemoryLRUCacheBytesEntry> {
    private long maxCacheSizeBytes;
    private long currentSizeBytes = 0;
    private LRUNode head = null;
    private LRUNode tail = null;
    private volatile boolean open = true;
    protected Map<byte[], LRUNode> map;
    protected List<MemoryLRUCacheBytes.EldestEntryRemovalListener<byte[], MemoryLRUCacheBytesEntry>> listeners;

    public interface EldestEntryRemovalListener<K, V> {
        void apply(K key, V value);
    }

    @Override
    public String name() {
        return null;
    }

    public MemoryLRUCacheBytes(long maxCacheSizeBytes) {
        this.maxCacheSizeBytes = maxCacheSizeBytes;
        this.map = new TreeMap<>(Bytes.BYTES_LEXICO_COMPARATOR);
        listeners = new ArrayList<>();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context, StateStore root) {
        // do nothing
    }

    /**
     * Add a listener that is called each time an entry is evicted from cache
     * @param listener
     */
    public void addEldestRemovedListener(MemoryLRUCacheBytes.EldestEntryRemovalListener<byte[],
        MemoryLRUCacheBytesEntry> listener) {
        this.listeners.add(listener);
    }

    private void callListeners(MemoryLRUCacheBytesEntry entry) {
        for (MemoryLRUCacheBytes.EldestEntryRemovalListener listener : listeners) {
            listener.apply(entry.key, entry);
        }
    }

    @Override
    public synchronized MemoryLRUCacheBytesEntry get(byte[] key) {
        MemoryLRUCacheBytesEntry entry = null;
        // get element
        LRUNode node = this.map.get(key);
        if (node != null) {
            entry = node.entry();
            updateLRU(node);
        }

        return entry;
    }

    /**
     * Check if we have enough space in cache to accept new element
     *
     * @param newElement new element to be insterted
     */
    private void maybeEvict(LRUNode newElement) {
        while (sizeBytes() + newElement.entry().size() > maxCacheSizeBytes) {
            LRUNode removed = this.map.remove(tail.entry().key);
            remove(tail);
            currentSizeBytes -= removed.entry().size();
            callListeners(removed.entry());
        }
    }

    @Override
    public synchronized void put(byte[] key, MemoryLRUCacheBytesEntry value) {
        if (this.map.containsKey(key)) {
            LRUNode node = this.map.get(key);
            currentSizeBytes -= node.entry().size();
            node.update(value);
            updateLRU(node);

        } else {
            LRUNode node = new LRUNode(value);
            // check if we need to evict anything
            maybeEvict(node);
            // put element
            putHead(node);
            this.map.put(key, node);
        }

        // TODO: memory size is more than the value size, it should include overheads
        currentSizeBytes += value.size();
    }

    public synchronized MemoryLRUCacheBytesEntry putIfAbsent(byte[] key, MemoryLRUCacheBytesEntry value) {
        MemoryLRUCacheBytesEntry originalValue = get(key);
        if (originalValue == null) {
            put(key, value);
        }
        return originalValue;
    }

    public synchronized void putAll(List<KeyValue<byte[], MemoryLRUCacheBytesEntry>> entries) {
        for (KeyValue<byte[], MemoryLRUCacheBytesEntry> entry : entries) {
            put(entry.key, entry.value);
        }
    }

    public synchronized MemoryLRUCacheBytesEntry delete(byte[] key) {
        LRUNode node = this.map.get(key);

        // remove from LRU list
        remove(node);

        // remove from map
        LRUNode value = this.map.remove(key);
        return value.entry();
    }


    public int size() {
        return this.map.size();
    }

    public long sizeBytes() {
        return currentSizeBytes;
    }

    @Override
    public long approximateNumEntries() {
        return size();
    }

    @Override
    public boolean persistent() {
        return false;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public void enableSendingOldValues() {
        // do nothing
    }

    @Override
    public void flush() {
        // do nothing
    }

    @Override
    public void close() {
        open = false;
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
        private MemoryLRUCacheBytesEntry entry;
        private LRUNode previous;
        private LRUNode next;

        LRUNode(MemoryLRUCacheBytesEntry entry) {
            this.entry = entry;
        }

        public MemoryLRUCacheBytesEntry entry() {
            return entry;
        }

        public void update(MemoryLRUCacheBytesEntry entry) {
            this.entry = entry;
        }
    }

    @Override
    public synchronized MemoryLRUCacheBytesIterator range(byte[] from, byte[] to) {
        return new MemoryLRUCacheBytesIterator(((TreeMap) map).navigableKeySet().subSet(from, true, to, true).iterator(), map);
    }


    public MemoryLRUCacheBytesIterator all() {
        return new MemoryLRUCacheBytesIterator(map.keySet().iterator(), map);
    }


    public static class MemoryLRUCacheBytesIterator implements KeyValueIterator<byte[], MemoryLRUCacheBytesEntry> {
        private final Iterator<byte[]> keys;
        private final Map<byte[], LRUNode> entries;
        private byte[] lastKey;

        public MemoryLRUCacheBytesIterator(Iterator<byte[]> keys, Map<byte[], LRUNode> entries) {
            this.keys = keys;
            this.entries = entries;
        }

        @Override
        public boolean hasNext() {
            return keys.hasNext();
        }

        @Override
        public KeyValue<byte[], MemoryLRUCacheBytesEntry> next() {
            lastKey = keys.next();
            final LRUNode lruNode = entries.get(lastKey);
            if (lruNode == null) {
                return new KeyValue<>(lastKey, null);
            }
            return new KeyValue<>(lastKey, lruNode.entry());
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
