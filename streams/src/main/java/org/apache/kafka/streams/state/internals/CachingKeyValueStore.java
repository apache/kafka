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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.internals.CacheFlushListener;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StateSerdes;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class CachingKeyValueStore<K, V> implements KeyValueStore<K, V> {

    private final KeyValueStore<Bytes, byte[]> underlying;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final String name;
    private final CacheFlushListener<K, V> flushListener;
    private final byte[] nameBytes;
    private MemoryLRUCacheBytes cache;
    private InternalProcessorContext context;
    private StateSerdes<K, V> serdes;
    private final LinkedHashMap<Bytes, Bytes> dirtyKeys = new LinkedHashMap<>();
    private boolean sendOldValues;

    public CachingKeyValueStore(final KeyValueStore<Bytes, byte[]> underlying,
                                final Serde<K> keySerde,
                                final Serde<V> valueSerde,
                                final CacheFlushListener<K, V> flushListener) {
        this.underlying = underlying;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.name = underlying.name();
        this.flushListener = flushListener;
        this.nameBytes = name.getBytes();
    }

    @Override
    public String name() {
        return name;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(final ProcessorContext context, final StateStore root) {
        underlying.init(context, root);
        initInternal(context);
    }

    void initInternal(final ProcessorContext context) {
        this.context = (InternalProcessorContext) context;
        this.serdes = new StateSerdes<>(underlying.name(),
                                        keySerde == null ? (Serde<K>) context.keySerde() : keySerde,
                                        valueSerde == null ? (Serde<V>) context.valueSerde() : valueSerde);
        this.cache = context.getCache();
        cache.addEldestRemovedListener(new MemoryLRUCacheBytes.EldestEntryRemovalListener<byte[], MemoryLRUCacheBytesEntry>() {
            @Override
            public void apply(final byte[] key, final MemoryLRUCacheBytesEntry entry) {
                if (entry.isDirty()) {
                    flush();
                }
            }
        });

    }

    @Override
    public synchronized void flush() {
        final Iterator<Map.Entry<Bytes, Bytes>> dirtyKeyIterator = dirtyKeys.entrySet().iterator();
        final List<KeyValue<Bytes, byte[]>> keyValues = new ArrayList<>();
        while (dirtyKeyIterator.hasNext()) {
            final Bytes dirtyKey = dirtyKeyIterator.next().getKey();
            dirtyKeyIterator.remove();
            final MemoryLRUCacheBytesEntry entry = cache.get(cacheKey(dirtyKey.get()));
            keyValues.add(KeyValue.pair(dirtyKey, entry.value));
            flushListener.forward(serdes.keyFrom(dirtyKey.get()),
                                  change(entry.value, underlying.get(dirtyKey)),
                                  entry,
                                  context);
        }
        underlying.putAll(keyValues);
        underlying.flush();
    }

    private Change<V> change(final byte[] newValue, final byte[] oldValue) {
        if (sendOldValues) {
            return new Change<>(serdes.valueFrom(newValue), serdes.valueFrom(oldValue));
        }
        return new Change<>(serdes.valueFrom(newValue), null);
    }

    @Override
    public void close() {
        flush();
        underlying.close();
    }

    @Override
    public boolean persistent() {
        return underlying.persistent();
    }

    @Override
    public boolean isOpen() {
        return underlying.isOpen();
    }

    @Override
    public void enableSendingOldValues() {
        this.sendOldValues = true;
    }

    @Override
    public synchronized V get(final K key) {
        final byte[] rawKey = serdes.rawKey(key);
        return get(rawKey);
    }

    private byte[] cacheKey(byte[] keyBytes) {
        byte[] merged = new byte[nameBytes.length + keyBytes.length];
        System.arraycopy(nameBytes, 0, merged, 0, nameBytes.length);
        System.arraycopy(keyBytes, 0, merged, nameBytes.length, keyBytes.length);
        return merged;
    }

    private V get(final byte[] rawKey) {
        final byte[] cacheKey = cacheKey(rawKey);
        final MemoryLRUCacheBytesEntry entry = cache.get(cacheKey);
        if (entry == null) {
            final byte[] rawValue = underlying.get(Bytes.wrap(rawKey));
            if (rawValue == null) {
                return null;
            }
            cache.put(cacheKey, new MemoryLRUCacheBytesEntry(cacheKey, rawValue));
            return serdes.valueFrom(rawValue);
        }

        if (entry.value == null) {
            return null;
        }

        return serdes.valueFrom(entry.value);
    }

    @Override
    public KeyValueIterator<K, V> range(final K from, final K to) {
        final byte[] origFrom = serdes.rawKey(from);
        final byte[] origTo = serdes.rawKey(to);
        final KeyValueIterator<Bytes, byte[]> storeIterator = underlying.range(Bytes.wrap(origFrom), Bytes.wrap(origTo));
        final MemoryLRUCacheBytes.MemoryLRUCacheBytesIterator cacheIterator = cache.range(cacheKey(origFrom), cacheKey(origTo));
        return new MergedSortedCacheRocksDBIterator<>(underlying, cacheIterator, storeIterator, serdes);
    }

    @Override
    public KeyValueIterator<K, V> all() {
        final KeyValueIterator<Bytes, byte[]> storeIterator = underlying.all();
        final MemoryLRUCacheBytes.MemoryLRUCacheBytesIterator cacheIterator = cache.all();
        return new MergedSortedCacheRocksDBIterator<>(underlying, cacheIterator, storeIterator, serdes);
    }

    @Override
    public synchronized long approximateNumEntries() {
        return underlying.approximateNumEntries() + dirtyKeys.size();
    }

    @Override
    public synchronized void put(final K key, final V value) {
        put(key, serdes.rawKey(key), value);
    }

    private synchronized void put(final K key, final byte[] rawKey, final V value) {
        final Bytes wrapped = Bytes.wrap(rawKey);
        dirtyKeys.put(wrapped, wrapped);
        final byte[] cacheKey = cacheKey(rawKey);
        final byte[] rawValue = serdes.rawValue(value);
        cache.put(cacheKey, new MemoryLRUCacheBytesEntry(rawKey, rawValue, true, context.offset(),
                                                         context.timestamp(), context.partition(), context.topic()));
    }

    @Override
    public synchronized V putIfAbsent(final K key, final V value) {
        final byte[] rawKey = serdes.rawKey(key);
        final V v = get(rawKey);
        if (v == null) {
            put(key, rawKey, value);
        }
        return v;
    }

    @Override
    public synchronized void putAll(final List<KeyValue<K, V>> entries) {
        for (KeyValue<K, V> entry : entries) {
            put(entry.key, entry.value);
        }
    }

    @Override
    public synchronized V delete(final K key) {
        final byte[] rawKey = serdes.rawKey(key);
        final V v = get(rawKey);
        put(key, rawKey, null);
        return v;
    }

    KeyValueStore<Bytes, byte[]> underlying() {
        return underlying;
    }
}
