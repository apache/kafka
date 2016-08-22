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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An in-memory LRU cache store based on HashSet and HashMap. XXX: the same as MemoryLRUCache for now.
 *
 * Note that the use of array-typed keys is discouraged because they result in incorrect ordering behavior.
 * If you intend to work on byte arrays as key, for example, you may want to wrap them with the {@code Bytes} class,
 * i.e. use {@code RocksDBStore<Bytes, ...>} rather than {@code RocksDBStore<byte[], ...>}.

 *
 * @param <K> The key type
 * @param <V> The value type
 *
 * @see org.apache.kafka.streams.state.Stores#create(String)
 */
public class MemoryLRUCacheBytes<K, V>  {
    static final int DEFAULT_CAPACITY = 1000;
    static final float DEFAULT_LOAD = 0.75f;
    private long maxCacheSizeBytes;
    private long currentSizeBytes = 0;
    private LRUNode head = null;
    private LRUNode tail = null;

    public interface EldestEntryRemovalListener<K, V> {

        void apply(K key, V value);
    }

    protected Map<Bytes, LRUNode> map;


    protected List<MemoryLRUCacheBytes.EldestEntryRemovalListener<Bytes, MemoryLRUCacheBytesEntry>> listeners;


    public MemoryLRUCacheBytes(String name, long maxCacheSizeBytes) {
        this.maxCacheSizeBytes = maxCacheSizeBytes;
        this.map = new HashMap<>(DEFAULT_CAPACITY, DEFAULT_CAPACITY);
        listeners = new ArrayList<>();
    }

    public void addEldestRemovedListener(MemoryLRUCacheBytes.EldestEntryRemovalListener<Bytes, MemoryLRUCacheBytesEntry> listener) {
        this.listeners.add(listener);
    }



    public synchronized MemoryLRUCacheBytesEntry<Bytes, byte[]> get(Bytes key) {
        MemoryLRUCacheBytesEntry entry = null;
        // get element
        LRUNode node = this.map.get(key);
        if (node != null) {
            entry = node.entry();
            updateLRU(node);
        }

        return entry;
    }

    private void callListeners(MemoryLRUCacheBytesEntry entry) {
        for (MemoryLRUCacheBytes.EldestEntryRemovalListener listener : listeners) {
            listener.apply(entry.key, entry);
        }
    }

    /**
     * Check if we have enough space in cache to accept new element
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

    public synchronized void put(Bytes key, MemoryLRUCacheBytesEntry<Bytes, byte[]> value) {
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
        currentSizeBytes += value.size();
    }

    public synchronized MemoryLRUCacheBytesEntry<Bytes, byte[]> putIfAbsent(Bytes key, MemoryLRUCacheBytesEntry<Bytes, byte[]> value) {
        MemoryLRUCacheBytesEntry originalValue = get(key);
        if (originalValue == null) {
            put(key, value);
        }
        return originalValue;
    }

    public void putAll(List<KeyValue<Bytes, MemoryLRUCacheBytesEntry<Bytes, byte[]>>> entries) {
        for (KeyValue<Bytes, MemoryLRUCacheBytesEntry<Bytes, byte[]>> entry : entries)
            put(entry.key, entry.value);
    }

    public synchronized MemoryLRUCacheBytesEntry<Bytes, byte[]> delete(Bytes key) {
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
     * @param node
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
     * A simple wrapper class to implement a doubly-linked list around
     * MemoryLRUCacheBytesEntry
     */
    protected class LRUNode {
        private MemoryLRUCacheBytesEntry<Bytes, byte[]> entry;
        private LRUNode previous;
        private LRUNode next;
        LRUNode(MemoryLRUCacheBytesEntry<Bytes, byte[]> entry) {
            this.entry = entry;
        }

        public MemoryLRUCacheBytesEntry<Bytes, byte[]> entry() {
            return entry;
        }

        public void update(MemoryLRUCacheBytesEntry<Bytes, byte[]> entry) {
            this.entry = entry;
        }
    }


}
