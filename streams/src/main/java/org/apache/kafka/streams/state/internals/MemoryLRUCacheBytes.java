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
public class MemoryLRUCacheBytes implements KeyValueStore<byte[], MemoryLRUCacheBytesEntry<byte[], byte[]>> {
    private long maxCacheSizeBytes;
    private long currentSizeBytes = 0;
    private LRUNode<byte[], byte[]> head = null;
    private LRUNode<byte[], byte[]> tail = null;
    private volatile boolean open = true;
    private String name;
    protected Map<byte[], LRUNode<byte[], byte[]>> map;
    protected List<MemoryLRUCacheBytes.EldestEntryRemovalListener<byte[], MemoryLRUCacheBytesEntry<byte[], byte[]>>> listeners;

    public interface EldestEntryRemovalListener<K, V> {
        void apply(K key, V value);
    }

    @Override
    public String name() {
        return this.name;
    }

    public MemoryLRUCacheBytes(String name, long maxCacheSizeBytes) {
        this.name = name;
        this.maxCacheSizeBytes = maxCacheSizeBytes;
        this.map = new TreeMap(Bytes.BYTES_LEXICO_COMPARATOR);
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
        MemoryLRUCacheBytesEntry<byte[], byte[]>> listener) {
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
    public synchronized void put(byte[] key, MemoryLRUCacheBytesEntry<byte[], byte[]> value) {
        if (this.map.containsKey(key)) {
            LRUNode node = this.map.get(key);
            currentSizeBytes -= node.entry().size();
            node.update(value);
            updateLRU(node);

        } else {
            LRUNode<byte[], byte[]> node = new LRUNode(value);
            // check if we need to evict anything
            maybeEvict(node);
            // put element
            putHead(node);
            this.map.put(key, node);
        }

        // TODO: memory size is more than the value size, it should include overheads
        currentSizeBytes += value.size();
    }

    public synchronized MemoryLRUCacheBytesEntry<byte[], byte[]> putIfAbsent(byte[] key, MemoryLRUCacheBytesEntry<byte[], byte[]> value) {
        MemoryLRUCacheBytesEntry<byte[], byte[]> originalValue = get(key);
        if (originalValue == null) {
            put(key, value);
        }
        return originalValue;
    }

    public void putAll(List<KeyValue<byte[], MemoryLRUCacheBytesEntry<byte[], byte[]>>> entries) {
        for (KeyValue<byte[], MemoryLRUCacheBytesEntry<byte[], byte[]>> entry : entries)
            put(entry.key, entry.value);
    }

    public synchronized MemoryLRUCacheBytesEntry<byte[], byte[]> delete(byte[] key) {
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
    protected class LRUNode<K, V> {
        private MemoryLRUCacheBytesEntry<K, V> entry;
        private LRUNode previous;
        private LRUNode next;

        LRUNode(MemoryLRUCacheBytesEntry<K, V> entry) {
            this.entry = entry;
        }

        public MemoryLRUCacheBytesEntry<K, V> entry() {
            return entry;
        }

        public void update(MemoryLRUCacheBytesEntry<K, V> entry) {
            this.entry = entry;
        }
    }


    @Override
    public MemoryLRUCacheBytesIterator<byte[], MemoryLRUCacheBytesEntry<byte[], byte[]>> range(byte[] from, byte[] to) {
        return new MemoryLRUCacheBytesIterator(((TreeMap) map).navigableKeySet().subSet(from, true, to, true).iterator(), map);
    }

    public MemoryLRUCacheBytesIterator<byte[], MemoryLRUCacheBytesEntry<byte[], byte[]>> all() {
        return new MemoryLRUCacheBytesIterator(map.keySet().iterator(), map);
    }


    public static class MemoryLRUCacheBytesIterator<K, V> implements KeyValueIterator<K, V> {
        private final Iterator<K> keys;
        private final Map<K, LRUNode<K, V>> entries;
        private K lastKey;

        public MemoryLRUCacheBytesIterator(Iterator<K> keys, Map<K, LRUNode<K, V>> entries) {
            this.keys = keys;
            this.entries = entries;
        }

        @Override
        public boolean hasNext() {
            return keys.hasNext();
        }

        @Override
        public KeyValue<K, V> next() {
            lastKey = keys.next();
            return new KeyValue<>(lastKey, entries.get(lastKey).entry().value);
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
