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
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.RecordContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StateSerdes;

import java.util.ArrayList;
import java.util.List;

class CachingKeyValueStore<K, V> implements KeyValueStore<K, V>, CachedStateStore<K, V> {

    private final KeyValueStore<Bytes, byte[]> underlying;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private CacheFlushListener<K, V> flushListener;
    private String name;
    private ThreadCache cache;
    private InternalProcessorContext context;
    private StateSerdes<K, V> serdes;
    private Thread streamThread;

    CachingKeyValueStore(final KeyValueStore<Bytes, byte[]> underlying,
                         final Serde<K> keySerde,
                         final Serde<V> valueSerde) {
        this.underlying = underlying;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    @Override
    public String name() {
        return underlying.name();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(final ProcessorContext context, final StateStore root) {
        underlying.init(context, root);
        initInternal(context);
        // save the stream thread as we only ever want to trigger a flush
        // when the stream thread is the the current thread.
        streamThread = Thread.currentThread();
    }

    @SuppressWarnings("unchecked")
    void initInternal(final ProcessorContext context) {
        this.context = (InternalProcessorContext) context;
        this.serdes = new StateSerdes<>(underlying.name(),
                                        keySerde == null ? (Serde<K>) context.keySerde() : keySerde,
                                        valueSerde == null ? (Serde<V>) context.valueSerde() : valueSerde);

        this.name = context.taskId() + "-" + underlying.name();
        this.cache = this.context.getCache();
        cache.addDirtyEntryFlushListener(name, new ThreadCache.DirtyEntryFlushListener() {
            @Override
            public void apply(final List<ThreadCache.DirtyEntry> entries) {
                final List<KeyValue<Bytes, byte[]>> keyValues = new ArrayList<>();
                for (ThreadCache.DirtyEntry entry : entries) {
                    keyValues.add(KeyValue.pair(entry.key(), entry.newValue()));
                    maybeForward(entry, (InternalProcessorContext) context);
                }
                underlying.putAll(keyValues);
            }
        });

    }

    private void maybeForward(final ThreadCache.DirtyEntry entry, final InternalProcessorContext context) {
        if (flushListener != null) {
            final RecordContext current = context.recordContext();
            context.setRecordContext(entry.recordContext());
            try {
                flushListener.apply(serdes.keyFrom(entry.key().get()),
                                    serdes.valueFrom(entry.newValue()), serdes.valueFrom(underlying.get(entry.key())));
            } finally {
                context.setRecordContext(current);
            }
        }
    }

    public void setFlushListener(final CacheFlushListener<K, V> flushListener) {
        this.flushListener = flushListener;
    }

    @Override
    public synchronized void flush() {
        cache.flush(name);
        underlying.flush();
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
    public synchronized V get(final K key) {
        final byte[] rawKey = serdes.rawKey(key);
        return get(rawKey);
    }

    private V get(final byte[] rawKey) {
        final LRUCacheEntry entry = cache.get(name, rawKey);
        if (entry == null) {
            final byte[] rawValue = underlying.get(Bytes.wrap(rawKey));
            if (rawValue == null) {
                return null;
            }
            // only update the cache if this call is on the streamThread
            // as we don't want other threads to trigger an eviction/flush
            if (Thread.currentThread().equals(streamThread)) {
                cache.put(name, rawKey, new LRUCacheEntry(rawValue));
            }
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
        final PeekingKeyValueIterator<Bytes, byte[]> storeIterator = new DelegatingPeekingKeyValueIterator<>(underlying.range(Bytes.wrap(origFrom), Bytes.wrap(origTo)));
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = cache.range(name, origFrom, origTo);
        return new MergedSortedCacheKeyValueStoreIterator<>(cacheIterator, storeIterator, serdes);
    }

    @Override
    public KeyValueIterator<K, V> all() {
        final PeekingKeyValueIterator<Bytes, byte[]> storeIterator = new DelegatingPeekingKeyValueIterator<>(underlying.all());
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = cache.all(name);
        return new MergedSortedCacheKeyValueStoreIterator<>(cacheIterator, storeIterator, serdes);
    }

    @Override
    public synchronized long approximateNumEntries() {
        return underlying.approximateNumEntries();
    }

    @Override
    public synchronized void put(final K key, final V value) {
        put(serdes.rawKey(key), value);
    }

    private synchronized void put(final byte[] rawKey, final V value) {
        final byte[] rawValue = serdes.rawValue(value);
        cache.put(name, rawKey, new LRUCacheEntry(rawValue, true, context.offset(),
                                                  context.timestamp(), context.partition(), context.topic()));
    }

    @Override
    public synchronized V putIfAbsent(final K key, final V value) {
        final byte[] rawKey = serdes.rawKey(key);
        final V v = get(rawKey);
        if (v == null) {
            put(rawKey, value);
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
        put(rawKey, null);
        return v;
    }

    KeyValueStore<Bytes, byte[]> underlying() {
        return underlying;
    }
}
