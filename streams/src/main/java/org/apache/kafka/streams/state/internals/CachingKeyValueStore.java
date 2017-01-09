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
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.RecordContext;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StateSerdes;

import java.util.List;

/**
 * Caching-enabled KeyValueStore wrapper is used for recording operation metrics, and hence its
 * inner KeyValueStore implementation do not need to provide its own caching functionality.
 *
 * @param <K>
 * @param <V>
 */
class CachingKeyValueStore<K, V> extends WrapperKeyValueStore.AbstractKeyValueStore<K, V> implements CachedStateStore<K, V> {

    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final KeyValueStore<Bytes, byte[]> innerBytes;

    private String name;
    private ThreadCache cache;
    private Thread streamThread;
    private StateSerdes<K, V> serdes;
    private InternalProcessorContext context;
    private CacheFlushListener<K, V> flushListener;

    CachingKeyValueStore(final KeyValueStore<Bytes, byte[]> inner, final Serde<K> keySerde, final Serde<V> valueSerde) {
        super(inner);
        this.innerBytes = inner;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(final ProcessorContext context, final StateStore root) {
        innerBytes.init(context, root);

        this.context = (InternalProcessorContext) context;
        this.serdes = new StateSerdes<>(innerBytes.name(),
                keySerde == null ? (Serde<K>) context.keySerde() : keySerde,
                valueSerde == null ? (Serde<V>) context.valueSerde() : valueSerde);

        this.name = context.taskId() + "-" + innerBytes.name();
        this.cache = this.context.getCache();
        cache.addDirtyEntryFlushListener(name, new ThreadCache.DirtyEntryFlushListener() {
            @Override
            public void apply(final List<ThreadCache.DirtyEntry> entries) {
                for (ThreadCache.DirtyEntry entry : entries) {
                    putAndMaybeForward(entry, (InternalProcessorContext) context);
                }
            }
        });

        // save the stream thread as we only ever want to trigger a flush
        // when the stream thread is the the current thread.
        streamThread = Thread.currentThread();
    }

    private void putAndMaybeForward(final ThreadCache.DirtyEntry entry, final InternalProcessorContext context) {
        final RecordContext current = context.recordContext();
        try {
            context.setRecordContext(entry.recordContext());
            if (flushListener != null) {
                flushListener.apply(serdes.keyFrom(entry.key().get()),
                                    serdes.valueFrom(entry.newValue()), serdes.valueFrom(innerBytes.get(entry.key())));

            }
            innerBytes.put(entry.key(), entry.newValue());
        } finally {
            context.setRecordContext(current);
        }
    }

    public void setFlushListener(final CacheFlushListener<K, V> flushListener) {
        this.flushListener = flushListener;
    }

    @Override
    public synchronized void flush() {
        cache.flush(name);
        innerBytes.flush();
    }

    @Override
    public void close() {
        flush();
        cache.close(name);
        innerBytes.close();
    }

    @Override
    public synchronized V get(final K key) {
        // since this function may not access the underlying inner store, we need to validate
        // if store is open outside as well.
        validateStoreOpen();

        final Bytes rawKey = Bytes.wrap(serdes.rawKey(key));
        return getInternal(rawKey);
    }

    private V getInternal(final Bytes rawKey) {
        final LRUCacheEntry entry = cache.get(name, rawKey);
        if (entry == null) {
            final byte[] rawValue = innerBytes.get(rawKey);
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
        final Bytes fromBytes = Bytes.wrap(serdes.rawKey(from));
        final Bytes toBytes = Bytes.wrap(serdes.rawKey(to));
        final KeyValueIterator<Bytes, byte[]> storeIterator = innerBytes.range(fromBytes, toBytes);
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = cache.range(name, fromBytes, toBytes);

        return new MergedSortedCacheKeyValueStoreIterator<>(cacheIterator, storeIterator, serdes);
    }

    @Override
    public KeyValueIterator<K, V> all() {
        final KeyValueIterator<Bytes, byte[]> storeIterator = new DelegatingPeekingKeyValueIterator<>(name, innerBytes.all());
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = cache.all(name);
        return new MergedSortedCacheKeyValueStoreIterator<>(cacheIterator, storeIterator, serdes);
    }

    @Override
    public synchronized void put(final K key, final V value) {
        // since this function may not access the underlying inner store, we need to validate
        // if store is open outside as well.
        validateStoreOpen();

        putInternal(Bytes.wrap(serdes.rawKey(key)), value);
    }

    private void putInternal(final Bytes rawKey, final V value) {
        final byte[] rawValue = serdes.rawValue(value);
        final LRUCacheEntry entry = new LRUCacheEntry(rawValue, true,
                context.offset(), context.timestamp(), context.partition(), context.topic());

        cache.put(name, rawKey, entry);
    }

    @Override
    public synchronized V putIfAbsent(final K key, final V value) {
        // since this function may not access the underlying inner store, we need to validate
        // if store is open outside as well.
        validateStoreOpen();

        final Bytes rawKey = Bytes.wrap(serdes.rawKey(key));
        final V v = getInternal(rawKey);
        if (v == null) {
            putInternal(rawKey, value);
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
        // since this function only access the underlying inner store after
        // the cache is modified, we need to validate
        // if store is open outside before hand.
        validateStoreOpen();

        final Bytes rawKey = Bytes.wrap(serdes.rawKey(key));
        final V v = getInternal(rawKey);
        cache.delete(name, rawKey);
        innerBytes.delete(rawKey);
        return v;
    }
}
