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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
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
    private final TreeMap<Bytes, LRUNode> map;
    private final Map<String, MemoryLRUCacheBytes.EldestEntryRemovalListener> listeners;

    public interface EldestEntryRemovalListener {
        void apply(byte[] key, MemoryLRUCacheBytesEntry value);
    }

    public MemoryLRUCacheBytes(long maxCacheSizeBytes) {
        this.maxCacheSizeBytes = maxCacheSizeBytes;
        this.map = new TreeMap<>();
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

    private void callListener(final Bytes key, MemoryLRUCacheBytesEntry entry) {
        try {
            final DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(key.get()));
            final String namespace = namespaceFromKey(dataInputStream);
            final EldestEntryRemovalListener listener = listeners.get(namespace);
            if (listener == null) {
                return;
            }
            listener.apply(remainingBytes(dataInputStream), entry);
        } catch (IOException e) {
            throw new ProcessorStateException(e);
        }
    }

    public synchronized MemoryLRUCacheBytesEntry get(final String namespace, byte[] key) {
        MemoryLRUCacheBytesEntry entry = null;
        // get element
        LRUNode node = this.map.get(cacheKey(namespace, key));
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
        while (sizeBytes() + newElement.size() > maxCacheSizeBytes) {
            final Bytes key = tail.key;
            LRUNode toRemove = this.map.get(key);
            currentSizeBytes -= toRemove.size();
            remove(tail);
            callListener(toRemove.key, toRemove.entry());
            this.map.remove(key);
        }
    }


    public synchronized void put(final String namespace, byte[] key, MemoryLRUCacheBytesEntry value) {
        final Bytes cacheKey = cacheKey(namespace, key);
        LRUNode node;
        if (this.map.containsKey(cacheKey)) {
            node = this.map.get(cacheKey);
            currentSizeBytes -= node.size();
            node.update(value);
            updateLRU(node);

        } else {
            node = new LRUNode(cacheKey, value);
            // check if we need to evict anything
            maybeEvict(node);
            // put element
            putHead(node);
            this.map.put(cacheKey, node);
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
        final Bytes cacheKey = cacheKey(namespace, key);
        LRUNode node = this.map.get(cacheKey);

        // remove from LRU list
        remove(node);

        // remove from map
        LRUNode value = this.map.remove(cacheKey);
        return value.entry();
    }


    public int size() {
        return this.map.size();
    }

    public long sizeBytes() {
        return currentSizeBytes;
    }


    private static String namespaceFromKey(final Bytes key) {
        try {
            return namespaceFromKey(new DataInputStream(new ByteArrayInputStream(key.get())));
        } catch (IOException e) {
            throw new ProcessorStateException(e);
        }
    }

    private static String namespaceFromKey(final DataInputStream stream) throws IOException {
        return stream.readUTF();
    }

    private static byte[] originalKey(final Bytes cacheKey) {
        final DataInputStream input = new DataInputStream(new ByteArrayInputStream(cacheKey.get()));
        try {
            namespaceFromKey(input);
            return remainingBytes(input);
        } catch (IOException e) {
            throw new ProcessorStateException(e);
        }
    }

    private static byte[] remainingBytes(final DataInputStream input) throws IOException {
        final byte[] originalKey = new byte[input.available()];
        input.read(originalKey);
        return originalKey;
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
        private MemoryLRUCacheBytesEntry entry;
        private LRUNode previous;
        private LRUNode next;

        LRUNode(final Bytes key, MemoryLRUCacheBytesEntry entry) {
            this.key = key;
            this.entry = entry;
        }

        public MemoryLRUCacheBytesEntry entry() {
            return entry;
        }

        public void update(MemoryLRUCacheBytesEntry entry) {
            this.entry = entry;
        }

        public long size() {
            return key.get().length + entry.size();
        }
    }

    public synchronized MemoryLRUCacheBytesIterator range(final String namespace, byte[] from, byte[] to) {
        return new MemoryLRUCacheBytesIterator(namespace, keySetIterator(map.navigableKeySet().subSet(cacheKey(namespace, from), true, cacheKey(namespace, to), true)), map);
    }

    public MemoryLRUCacheBytesIterator all(final String namespace) {
        return new MemoryLRUCacheBytesIterator(namespace, keySetIterator(map.tailMap(namespaceBytes(namespace)).keySet()), map);
    }


    private Iterator<Bytes> keySetIterator(final Set<Bytes> keySet) {
        final TreeSet<Bytes> copy = new TreeSet<>();
        copy.addAll(keySet);
        return copy.iterator();
    }

    private Bytes cacheKey(String namespace, byte[] keyBytes) {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        final DataOutputStream output = new DataOutputStream(bytes);
        try {
            output.writeUTF(namespace);
            output.write(keyBytes);
            return Bytes.wrap(bytes.toByteArray());
        } catch (IOException e) {
            throw new ProcessorStateException("Unable to convert key to cache key", e);
        }
    }

    private static Bytes namespaceBytes(final String namespace) {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        final DataOutputStream output = new DataOutputStream(bytes);
        try {
            output.writeUTF(namespace);
            return Bytes.wrap(bytes.toByteArray());
        } catch (IOException e) {
            throw new ProcessorStateException(e);
        }
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

            if (!namespace.equals(namespaceFromKey(cacheKey))) {
                return;
            }

            nextEntry = new KeyValue<>(originalKey(cacheKey), lruNode.entry());
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
