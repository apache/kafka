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

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.kafka.streams.processor.internals.ProcessorContextUtils.asInternalProcessorContext;
import static org.apache.kafka.streams.state.internals.ExceptionUtils.executeAll;
import static org.apache.kafka.streams.state.internals.ExceptionUtils.throwSuppressed;

public class CachingKeyValueStore
    extends WrappedStateStore<KeyValueStore<Bytes, byte[]>, byte[], byte[]>
    implements KeyValueStore<Bytes, byte[]>, CachedStateStore<byte[], byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(CachingKeyValueStore.class);

    private CacheFlushListener<byte[], byte[]> flushListener;
    private boolean sendOldValues;
    private String cacheName;
    private InternalProcessorContext context;
    private Thread streamThread;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    CachingKeyValueStore(final KeyValueStore<Bytes, byte[]> underlying) {
        super(underlying);
    }

    @Deprecated
    @Override
    public void init(final ProcessorContext context,
                     final StateStore root) {
        initInternal(asInternalProcessorContext(context));
        super.init(context, root);
        // save the stream thread as we only ever want to trigger a flush
        // when the stream thread is the current thread.
        streamThread = Thread.currentThread();
    }

    @Override
    public void init(final StateStoreContext context,
                     final StateStore root) {
        initInternal(asInternalProcessorContext(context));
        super.init(context, root);
        // save the stream thread as we only ever want to trigger a flush
        // when the stream thread is the current thread.
        streamThread = Thread.currentThread();
    }

    private void initInternal(final InternalProcessorContext context) {
        this.context = context;

        this.cacheName = ThreadCache.nameSpaceFromTaskIdAndStore(context.taskId().toString(), name());
        this.context.registerCacheFlushListener(cacheName, entries -> {
            for (final ThreadCache.DirtyEntry entry : entries) {
                putAndMaybeForward(entry, context);
            }
        });
    }

    private void putAndMaybeForward(final ThreadCache.DirtyEntry entry,
                                    final InternalProcessorContext context) {
        if (flushListener != null) {
            final byte[] rawNewValue = entry.newValue();
            final byte[] rawOldValue = rawNewValue == null || sendOldValues ? wrapped().get(entry.key()) : null;

            // this is an optimization: if this key did not exist in underlying store and also not in the cache,
            // we can skip flushing to downstream as well as writing to underlying store
            if (rawNewValue != null || rawOldValue != null) {
                // we need to get the old values if needed, and then put to store, and then flush
                wrapped().put(entry.key(), entry.newValue());

                final ProcessorRecordContext current = context.recordContext();
                context.setRecordContext(entry.entry().context());
                try {
                    flushListener.apply(
                        entry.key().get(),
                        rawNewValue,
                        sendOldValues ? rawOldValue : null,
                        entry.entry().context().timestamp());
                } finally {
                    context.setRecordContext(current);
                }
            }
        } else {
            wrapped().put(entry.key(), entry.newValue());
        }
    }

    @Override
    public boolean setFlushListener(final CacheFlushListener<byte[], byte[]> flushListener,
                                    final boolean sendOldValues) {
        this.flushListener = flushListener;
        this.sendOldValues = sendOldValues;

        return true;
    }

    @Override
    public void put(final Bytes key,
                    final byte[] value) {
        Objects.requireNonNull(key, "key cannot be null");
        validateStoreOpen();
        lock.writeLock().lock();
        try {
            validateStoreOpen();
            // for null bytes, we still put it into cache indicating tombstones
            putInternal(key, value);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void putInternal(final Bytes key,
                             final byte[] value) {
        context.cache().put(
            cacheName,
            key,
            new LRUCacheEntry(
                value,
                context.headers(),
                true,
                context.offset(),
                context.timestamp(),
                context.partition(),
                context.topic()));
    }

    @Override
    public byte[] putIfAbsent(final Bytes key,
                              final byte[] value) {
        Objects.requireNonNull(key, "key cannot be null");
        validateStoreOpen();
        lock.writeLock().lock();
        try {
            validateStoreOpen();
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
            validateStoreOpen();
            for (final KeyValue<Bytes, byte[]> entry : entries) {
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
            validateStoreOpen();
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

    @Override
    public byte[] get(final Bytes key) {
        Objects.requireNonNull(key, "key cannot be null");
        validateStoreOpen();
        final Lock theLock;
        if (Thread.currentThread().equals(streamThread)) {
            theLock = lock.writeLock();
        } else {
            theLock = lock.readLock();
        }
        theLock.lock();
        try {
            validateStoreOpen();
            return getInternal(key);
        } finally {
            theLock.unlock();
        }
    }

    private byte[] getInternal(final Bytes key) {
        LRUCacheEntry entry = null;
        if (context.cache() != null) {
            entry = context.cache().get(cacheName, key);
        }
        if (entry == null) {
            final byte[] rawValue = wrapped().get(key);
            if (rawValue == null) {
                return null;
            }
            // only update the cache if this call is on the streamThread
            // as we don't want other threads to trigger an eviction/flush
            if (Thread.currentThread().equals(streamThread)) {
                context.cache().put(cacheName, key, new LRUCacheEntry(rawValue));
            }
            return rawValue;
        } else {
            return entry.value();
        }
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> range(final Bytes from,
                                                 final Bytes to) {
        if (from.compareTo(to) > 0) {
            LOG.warn("Returning empty iterator for fetch with invalid key range: from > to. " +
                "This may be due to range arguments set in the wrong order, " +
                "or serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                "Note that the built-in numerical serdes do not follow this for negative numbers");
            return KeyValueIterators.emptyIterator();
        }

        validateStoreOpen();
        final KeyValueIterator<Bytes, byte[]> storeIterator = wrapped().range(from, to);
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = context.cache().range(cacheName, from, to);
        return new MergedSortedCacheKeyValueBytesStoreIterator(cacheIterator, storeIterator, true);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> reverseRange(final Bytes from,
                                                        final Bytes to) {
        if (from.compareTo(to) > 0) {
            LOG.warn("Returning empty iterator for fetch with invalid key range: from > to. " +
                "This may be due to range arguments set in the wrong order, " +
                "or serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                "Note that the built-in numerical serdes do not follow this for negative numbers");
            return KeyValueIterators.emptyIterator();
        }

        validateStoreOpen();
        final KeyValueIterator<Bytes, byte[]> storeIterator = wrapped().reverseRange(from, to);
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = context.cache().reverseRange(cacheName, from, to);
        return new MergedSortedCacheKeyValueBytesStoreIterator(cacheIterator, storeIterator, false);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> all() {
        validateStoreOpen();
        final KeyValueIterator<Bytes, byte[]> storeIterator =
            new DelegatingPeekingKeyValueIterator<>(this.name(), wrapped().all());
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = context.cache().all(cacheName);
        return new MergedSortedCacheKeyValueBytesStoreIterator(cacheIterator, storeIterator, true);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> reverseAll() {
        validateStoreOpen();
        final KeyValueIterator<Bytes, byte[]> storeIterator =
            new DelegatingPeekingKeyValueIterator<>(this.name(), wrapped().reverseAll());
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = context.cache().reverseAll(cacheName);
        return new MergedSortedCacheKeyValueBytesStoreIterator(cacheIterator, storeIterator, false);
    }

    @Override
    public long approximateNumEntries() {
        validateStoreOpen();
        lock.readLock().lock();
        try {
            validateStoreOpen();
            return wrapped().approximateNumEntries();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void flush() {
        validateStoreOpen();
        lock.writeLock().lock();
        try {
            validateStoreOpen();
            context.cache().flush(cacheName);
            wrapped().flush();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void flushCache() {
        validateStoreOpen();
        lock.writeLock().lock();
        try {
            validateStoreOpen();
            context.cache().flush(cacheName);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void close() {
        lock.writeLock().lock();
        try {
            final LinkedList<RuntimeException> suppressed = executeAll(
                () -> context.cache().flush(cacheName),
                () -> context.cache().close(cacheName),
                wrapped()::close
            );
            if (!suppressed.isEmpty()) {
                throwSuppressed("Caught an exception while closing caching key value store for store " + name(),
                    suppressed);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
}
