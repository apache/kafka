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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StateSerdes;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * An in-memory LRU cache store based on HashSet and HashMap.
 *
 *  * Note that the use of array-typed keys is discouraged because they result in incorrect ordering behavior.
 * If you intend to work on byte arrays as key, for example, you may want to wrap them with the {@code Bytes} class,
 * i.e. use {@code RocksDBStore<Bytes, ...>} rather than {@code RocksDBStore<byte[], ...>}.
 *
 * @param <K> The key type
 * @param <V> The value type
 */
public class MemoryLRUCache<K, V> implements KeyValueStore<K, V> {

    public interface EldestEntryRemovalListener<K, V> {

        void apply(K key, V value);
    }
    private final Serde<K> keySerde;

    private final Serde<V> valueSerde;
    private final String name;
    protected final Map<K, V> map;

    private StateSerdes<K, V> serdes;
    private boolean restoring = false;      // TODO: this is a sub-optimal solution to avoid logging during restoration.
                                            // in the future we should augment the StateRestoreCallback with onComplete etc to better resolve this.
    private volatile boolean open = true;

    private EldestEntryRemovalListener<K, V> listener;

    MemoryLRUCache(final String name,
                   final int maxCacheSize,
                   final Serde<K> keySerde,
                   final Serde<V> valueSerde) {
        this.name = name;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;

        // leave room for one extra entry to handle adding an entry before the oldest can be removed
        this.map = new LinkedHashMap<K, V>(maxCacheSize + 1, 1.01f, true) {
            private static final long serialVersionUID = 1L;

            @Override
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                boolean evict = super.size() > maxCacheSize;
                if (evict && !restoring && listener != null) {
                    listener.apply(eldest.getKey(), eldest.getValue());
                }
                return evict;
            }
        };
    }

    KeyValueStore<K, V> enableLogging() {
        return new InMemoryKeyValueLoggedStore<>(this, keySerde, valueSerde);
    }

    MemoryLRUCache<K, V> whenEldestRemoved(final EldestEntryRemovalListener<K, V> listener) {
        this.listener = listener;

        return this;
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context, StateStore root) {
        // construct the serde
        this.serdes = new StateSerdes<>(
            ProcessorStateManager.storeChangelogTopic(context.applicationId(), name),
            keySerde == null ? (Serde<K>) context.keySerde() : keySerde,
            valueSerde == null ? (Serde<V>) context.valueSerde() : valueSerde);

        // register the store
        context.register(root, new StateRestoreCallback() {
            @Override
            public void restore(byte[] key, byte[] value) {
                restoring = true;
                // check value for null, to avoid  deserialization error.
                if (value == null) {
                    delete(serdes.keyFrom(key));
                } else {
                    put(serdes.keyFrom(key), serdes.valueFrom(value));
                }
                restoring = false;
            }
        });
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
    public synchronized V get(final K key) {
        Objects.requireNonNull(key);

        return this.map.get(key);
    }

    @Override
    public synchronized void put(final K key, final V value) {
        Objects.requireNonNull(key);
        if (value == null) {
            this.map.remove(key);
        } else {
            this.map.put(key, value);
        }
    }

    @Override
    public synchronized V putIfAbsent(final K key, final V value) {
        Objects.requireNonNull(key);
        V originalValue = get(key);
        if (originalValue == null) {
            put(key, value);
        }
        return originalValue;
    }

    @Override
    public void putAll(final List<KeyValue<K, V>> entries) {
        for (KeyValue<K, V> entry : entries)
            put(entry.key, entry.value);
    }

    @Override
    public synchronized V delete(final K key) {
        Objects.requireNonNull(key);
        return this.map.remove(key);
    }

    /**
     * @throws UnsupportedOperationException
     */
    @Override
    public KeyValueIterator<K, V> range(final K from, final K to) {
        throw new UnsupportedOperationException("MemoryLRUCache does not support range() function.");
    }

    /**
     * @throws UnsupportedOperationException
     */
    @Override
    public KeyValueIterator<K, V> all() {
        throw new UnsupportedOperationException("MemoryLRUCache does not support all() function.");
    }

    @Override
    public long approximateNumEntries() {
        return this.map.size();
    }

    @Override
    public void flush() {
        // do-nothing since it is in-memory
    }

    @Override
    public void close() {
        open = false;
    }

    public int size() {
        return this.map.size();
    }
}
