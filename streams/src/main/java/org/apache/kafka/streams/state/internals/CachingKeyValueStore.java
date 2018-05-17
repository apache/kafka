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
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.internals.CacheFlushListener;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.RecordContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StateSerdes;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class CachingKeyValueStore<K, V> extends WrappedStateStore.AbstractStateStore implements KeyValueStore<Bytes, byte[]>, CachedStateStore<K, V> {

    private final KeyValueStore<Bytes, byte[]> underlying;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private CacheFlushListener<K, V> flushListener;
    private boolean sendOldValues;
    private String cacheName;
    private ThreadCache cache;
    private InternalProcessorContext context;
    private StateSerdes<K, V> serdes;
    private Thread streamThread;
    private ReadWriteLock lock = new ReentrantReadWriteLock();

    CachingKeyValueStore(final KeyValueStore<Bytes, byte[]> underlying,
                         final Serde<K> keySerde,
                         final Serde<V> valueSerde) {
        super(underlying);
        this.underlying = underlying;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    @Override
    public void init(final ProcessorContext context, final StateStore root) {
        initInternal(context);
        underlying.init(context, root);
        // save the stream thread as we only ever want to trigger a flush
        // when the stream thread is the current thread.
        streamThread = Thread.currentThread();
    }

    @SuppressWarnings("unchecked")
    private void initInternal(final ProcessorContext context) {
        this.context = (InternalProcessorContext) context;
        this.serdes = new StateSerdes<>(ProcessorStateManager.storeChangelogTopic(context.applicationId(), underlying.name()),
                                        keySerde == null ? (Serde<K>) context.keySerde() : keySerde,
                                        valueSerde == null ? (Serde<V>) context.valueSerde() : valueSerde);

        this.cache = this.context.getCache();
        this.cacheName = ThreadCache.nameSpaceFromTaskIdAndStore(context.taskId().toString(), underlying.name());
        cache.addDirtyEntryFlushListener(cacheName, new ThreadCache.DirtyEntryFlushListener() {
            @Override
            public void apply(final List<ThreadCache.DirtyEntry> entries) {
                for (ThreadCache.DirtyEntry entry : entries) {
                    putAndMaybeForward(entry, (InternalProcessorContext) context);
                }
            }
        });
    }

    private void putAndMaybeForward(final ThreadCache.DirtyEntry entry, final InternalProcessorContext context) {
        final RecordContext current = context.recordContext();
        try {
            context.setRecordContext(entry.recordContext());
            if (flushListener != null) {
                V oldValue = null;
                if (sendOldValues) {
                    final byte[] oldBytesValue = underlying.get(entry.key());
                    oldValue = oldBytesValue == null ? null : serdes.valueFrom(oldBytesValue);
                }
                // we rely on underlying store to handle null new value bytes as deletes
                underlying.put(entry.key(), entry.newValue());
                flushListener.apply(serdes.keyFrom(entry.key().get()),
                                    serdes.valueFrom(entry.newValue()),
                                    oldValue);
            } else {
                underlying.put(entry.key(), entry.newValue());
            }
        } finally {
            context.setRecordContext(current);
        }
    }

    public void setFlushListener(final CacheFlushListener<K, V> flushListener,
                                 final boolean sendOldValues) {

        this.flushListener = flushListener;
        this.sendOldValues = sendOldValues;
    }

    @Override
    public void flush() {
        lock.writeLock().lock();
        try {
            cache.flush(cacheName);
            underlying.flush();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void close() {
        flush();
        underlying.close();
        cache.close(cacheName);
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
    public byte[] get(final Bytes key) {
        Objects.requireNonNull(key, "key cannot be null");
        validateStoreOpen();
        Lock theLock;
        if (Thread.currentThread().equals(streamThread)) {
            theLock = lock.writeLock();
        } else {
            theLock = lock.readLock();
        }
        theLock.lock();
        try {
            return getInternal(key);
        } finally {
            theLock.unlock();
        }
    }

    private byte[] getInternal(final Bytes key) {
        LRUCacheEntry entry = null;
        if (cache != null) {
            entry = cache.get(cacheName, key);
        }
        if (entry == null) {
            final byte[] rawValue = underlying.get(key);
            if (rawValue == null) {
                return null;
            }
            // only update the cache if this call is on the streamThread
            // as we don't want other threads to trigger an eviction/flush
            if (Thread.currentThread().equals(streamThread)) {
                cache.put(cacheName, key, new LRUCacheEntry(rawValue));
            }
            return rawValue;
        } else {
            return entry.value;
        }
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to) {
        validateStoreOpen();
        final KeyValueIterator<Bytes, byte[]> storeIterator = underlying.range(from, to);
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = cache.range(cacheName, from, to);
        return new MergedSortedCacheKeyValueBytesStoreIterator(cacheIterator, storeIterator);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> all() {
        validateStoreOpen();
        final KeyValueIterator<Bytes, byte[]> storeIterator = new DelegatingPeekingKeyValueIterator<>(this.name(), underlying.all());
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = cache.all(cacheName);
        return new MergedSortedCacheKeyValueBytesStoreIterator(cacheIterator, storeIterator);
    }

    @Override
    public long approximateNumEntries() {
        validateStoreOpen();
        lock.readLock().lock();
        try {
            return underlying.approximateNumEntries();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void put(final Bytes key, final byte[] value) {
        Objects.requireNonNull(key, "key cannot be null");
        validateStoreOpen();
        lock.writeLock().lock();
        try {
            // for null bytes, we still put it into cache indicating tombstones
            putInternal(key, value);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void putInternal(final Bytes key, final byte[] value) {
        cache.put(cacheName, key, new LRUCacheEntry(value, true, context.offset(),
              context.timestamp(), context.partition(), context.topic()));
    }

    @Override
    public byte[] putIfAbsent(final Bytes key, final byte[] value) {
        Objects.requireNonNull(key, "key cannot be null");
        validateStoreOpen();
        lock.writeLock().lock();
        try {
            final byte[] v = getInternal(key);
            if (v == null) {
                putInternal(key, value);
            }
            return v;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void putAll(final List<KeyValue<Bytes, byte[]>> entries) {
        validateStoreOpen();
        lock.writeLock().lock();
        try {
            for (KeyValue<Bytes, byte[]> entry : entries) {
                Objects.requireNonNull(entry.key, "key cannot be null");
                put(entry.key, entry.value);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public byte[] delete(final Bytes key) {
        Objects.requireNonNull(key, "key cannot be null");
        validateStoreOpen();
        lock.writeLock().lock();
        try {
            return deleteInternal(key);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private byte[] deleteInternal(final Bytes key) {
        final byte[] v = getInternal(key);
        putInternal(key, null);
        return v;
    }

    KeyValueStore<Bytes, byte[]> underlying() {
        return underlying;
    }

    @Override
    public StateStore inner() {
        if (underlying instanceof WrappedStateStore) {
            return ((WrappedStateStore) underlying).inner();
        }
        return underlying;
    }
}
