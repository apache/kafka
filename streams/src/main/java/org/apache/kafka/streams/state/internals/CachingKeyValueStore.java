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

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.query.KeyQuery;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
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
    private InternalProcessorContext<?, ?> context;
    private Thread streamThread;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Position position;
    private final boolean timestampedSchema;

    @FunctionalInterface
    public interface CacheQueryHandler {
        QueryResult<?> apply(
            final Query<?> query,
            final Position mergedPosition,
            final PositionBound positionBound,
            final QueryConfig config,
            final StateStore store
        );
    }

    @SuppressWarnings("rawtypes")
    private final Map<Class, CacheQueryHandler> queryHandlers =
        mkMap(
            mkEntry(
                KeyQuery.class,
                (query, mergedPosition, positionBound, config, store) ->
                    runKeyQuery(query, mergedPosition, positionBound, config)
            )
        );


    CachingKeyValueStore(final KeyValueStore<Bytes, byte[]> underlying, final boolean timestampedSchema) {
        super(underlying);
        position = Position.emptyPosition();
        this.timestampedSchema = timestampedSchema;
    }

    @SuppressWarnings("deprecation") // This can be removed when it's removed from the interface.
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

    @Override
    public Position getPosition() {
        // We return the merged position since the query uses the merged position as well
        final Position mergedPosition = Position.emptyPosition();
        mergedPosition.merge(position);
        mergedPosition.merge(wrapped().getPosition());
        return mergedPosition;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <R> QueryResult<R> query(final Query<R> query,
                                    final PositionBound positionBound,
                                    final QueryConfig config) {

        final long start = config.isCollectExecutionInfo() ? System.nanoTime() : -1L;
        final QueryResult<R> result;

        final CacheQueryHandler handler = queryHandlers.get(query.getClass());
        if (handler == null) {
            result = wrapped().query(query, positionBound, config);

        } else {
            final int partition = context.taskId().partition();
            final Lock lock = this.lock.readLock();
            lock.lock();
            try {
                validateStoreOpen();
                final Position mergedPosition = getPosition();

                // We use the merged position since the cache and the store may be at different positions
                if (!StoreQueryUtils.isPermitted(mergedPosition, positionBound, partition)) {
                    result = QueryResult.notUpToBound(mergedPosition, positionBound, partition);
                } else {
                    result = (QueryResult<R>) handler.apply(
                        query,
                        mergedPosition,
                        positionBound,
                        config,
                        this
                    );
                }
            } finally {
                lock.unlock();
            }
        }
        if (config.isCollectExecutionInfo()) {
            result.addExecutionInfo(
                "Handled in " + getClass() + " in " + (System.nanoTime() - start) + "ns");
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private <R> QueryResult<R> runKeyQuery(final Query<R> query,
                                           final Position mergedPosition,
                                           final PositionBound positionBound,
                                           final QueryConfig config) {
        QueryResult<R> result = null;
        final KeyQuery<Bytes, byte[]> keyQuery = (KeyQuery<Bytes, byte[]>) query;

        if (keyQuery.isSkipCache()) {
            return wrapped().query(query, positionBound, config);
        }

        final Bytes key = keyQuery.getKey();

        if (context.cache() != null) {
            final LRUCacheEntry lruCacheEntry = context.cache().get(cacheName, key);
            if (lruCacheEntry != null) {
                final byte[] rawValue;
                if (timestampedSchema && !WrappedStateStore.isTimestamped(wrapped())) {
                    rawValue = ValueAndTimestampDeserializer.rawValue(lruCacheEntry.value());
                } else {
                    rawValue = lruCacheEntry.value();
                }
                result = (QueryResult<R>) QueryResult.forResult(rawValue);
            }
        }

        // We don't need to check the position at the state store since we already performed the check on
        // the merged position above
        if (result == null) {
            result = wrapped().query(query, PositionBound.unbounded(), config);
        }
        result.setPosition(mergedPosition);
        return result;
    }

    private void initInternal(final InternalProcessorContext<?, ?> context) {
        this.context = context;
        this.cacheName = ThreadCache.nameSpaceFromTaskIdAndStore(context.taskId().toString(), name());
        this.context.registerCacheFlushListener(cacheName, entries -> {
            for (final ThreadCache.DirtyEntry entry : entries) {
                putAndMaybeForward(entry, context);
            }
        });
    }

    private void putAndMaybeForward(final ThreadCache.DirtyEntry entry,
                                    final InternalProcessorContext<?, ?> context) {
        if (flushListener != null) {
            final byte[] rawNewValue = entry.newValue();
            final byte[] rawOldValue = rawNewValue == null || sendOldValues ? wrapped().get(entry.key()) : null;

            // this is an optimization: if this key did not exist in underlying store and also not in the cache,
            // we can skip flushing to downstream as well as writing to underlying store
            if (rawNewValue != null || rawOldValue != null) {
                // we need to get the old values if needed, and then put to store, and then flush
                final ProcessorRecordContext current = context.recordContext();
                try {
                    context.setRecordContext(entry.entry().context());
                    wrapped().put(entry.key(), entry.newValue());
                    flushListener.apply(
                        new Record<>(
                            entry.key().get(),
                            new Change<>(rawNewValue, sendOldValues ? rawOldValue : null),
                            entry.entry().context().timestamp(),
                            entry.entry().context().headers()));
                } finally {
                    context.setRecordContext(current);
                }
            }
        } else {
            final ProcessorRecordContext current = context.recordContext();
            try {
                context.setRecordContext(entry.entry().context());
                wrapped().put(entry.key(), entry.newValue());
            } finally {
                context.setRecordContext(current);
            }
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

        StoreQueryUtils.updatePosition(position, context);
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
        if (Objects.nonNull(from) && Objects.nonNull(to) && from.compareTo(to) > 0) {
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
        if (Objects.nonNull(from) && Objects.nonNull(to) && from.compareTo(to) > 0) {
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
    public <PS extends Serializer<P>, P> KeyValueIterator<Bytes, byte[]> prefixScan(final P prefix, final PS prefixKeySerializer) {
        validateStoreOpen();
        final KeyValueIterator<Bytes, byte[]> storeIterator = wrapped().prefixScan(prefix, prefixKeySerializer);
        final Bytes from = Bytes.wrap(prefixKeySerializer.serialize(null, prefix));
        final Bytes to = Bytes.increment(from);
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = context.cache().range(cacheName, from, to, false);
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
