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
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.RecordQueue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Objects;

import static org.apache.kafka.streams.processor.internals.ProcessorContextUtils.asInternalProcessorContext;
import static org.apache.kafka.streams.state.internals.ExceptionUtils.executeAll;
import static org.apache.kafka.streams.state.internals.ExceptionUtils.throwSuppressed;

class CachingSessionStore
    extends WrappedStateStore<SessionStore<Bytes, byte[]>, byte[], byte[]>
    implements SessionStore<Bytes, byte[]>, CachedStateStore<byte[], byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(CachingSessionStore.class);

    private final SessionKeySchema keySchema;
    private final SegmentedCacheFunction cacheFunction;
    private static final String INVALID_RANGE_WARN_MSG = "Returning empty iterator for fetch with invalid key range: from > to. " +
        "This may be due to range arguments set in the wrong order, " +
        "or serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
        "Note that the built-in numerical serdes do not follow this for negative numbers";

    private String cacheName;
    private InternalProcessorContext<?, ?> context;
    private CacheFlushListener<byte[], byte[]> flushListener;
    private boolean sendOldValues;

    private long maxObservedTimestamp; // Refers to the window end time (determines segmentId)

    CachingSessionStore(final SessionStore<Bytes, byte[]> bytesStore,
                        final long segmentInterval) {
        super(bytesStore);
        this.keySchema = new SessionKeySchema();
        this.cacheFunction = new SegmentedCacheFunction(keySchema, segmentInterval);
        this.maxObservedTimestamp = RecordQueue.UNKNOWN;
    }

    @Deprecated
    @Override
    public void init(final ProcessorContext context, final StateStore root) {
        initInternal(asInternalProcessorContext(context));
        super.init(context, root);
    }

    @Override
    public void init(final StateStoreContext context, final StateStore root) {
        initInternal(asInternalProcessorContext(context));
        super.init(context, root);
    }

    private void initInternal(final InternalProcessorContext<?, ?> context) {
        this.context = context;

        cacheName = context.taskId() + "-" + name();
        context.registerCacheFlushListener(cacheName, entries -> {
            for (final ThreadCache.DirtyEntry entry : entries) {
                putAndMaybeForward(entry, context);
            }
        });
    }

    private void putAndMaybeForward(final ThreadCache.DirtyEntry entry, final InternalProcessorContext<?, ?> context) {
        final Bytes binaryKey = cacheFunction.key(entry.key());
        final Windowed<Bytes> bytesKey = SessionKeySchema.from(binaryKey);
        if (flushListener != null) {
            final byte[] newValueBytes = entry.newValue();
            final byte[] oldValueBytes = newValueBytes == null || sendOldValues ?
                wrapped().fetchSession(bytesKey.key(), bytesKey.window().start(), bytesKey.window().end()) : null;

            // this is an optimization: if this key did not exist in underlying store and also not in the cache,
            // we can skip flushing to downstream as well as writing to underlying store
            if (newValueBytes != null || oldValueBytes != null) {
                // we need to get the old values if needed, and then put to store, and then flush

                final ProcessorRecordContext current = context.recordContext();
                try {
                    context.setRecordContext(entry.entry().context());
                    wrapped().put(bytesKey, entry.newValue());
                    flushListener.apply(
                        new Record<>(
                            binaryKey.get(),
                            new Change<>(newValueBytes, sendOldValues ? oldValueBytes : null),
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
                wrapped().put(bytesKey, entry.newValue());
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
    public void put(final Windowed<Bytes> key, final byte[] value) {
        validateStoreOpen();
        final Bytes binaryKey = SessionKeySchema.toBinary(key);
        final LRUCacheEntry entry =
            new LRUCacheEntry(
                value,
                context.headers(),
                true,
                context.offset(),
                context.timestamp(),
                context.partition(),
                context.topic());
        context.cache().put(cacheName, cacheFunction.cacheKey(binaryKey), entry);

        maxObservedTimestamp = Math.max(keySchema.segmentTimestamp(binaryKey), maxObservedTimestamp);
    }

    @Override
    public void remove(final Windowed<Bytes> sessionKey) {
        validateStoreOpen();
        put(sessionKey, null);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(final Bytes key,
                                                                  final long earliestSessionEndTime,
                                                                  final long latestSessionStartTime) {
        validateStoreOpen();

        final PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator = wrapped().persistent() ?
            new CacheIteratorWrapper(key, earliestSessionEndTime, latestSessionStartTime, true) :
            context.cache().range(cacheName,
                        cacheFunction.cacheKey(keySchema.lowerRangeFixedSize(key, earliestSessionEndTime)),
                        cacheFunction.cacheKey(keySchema.upperRangeFixedSize(key, latestSessionStartTime))
            );

        final KeyValueIterator<Windowed<Bytes>, byte[]> storeIterator = wrapped().findSessions(key,
                                                                                               earliestSessionEndTime,
                                                                                               latestSessionStartTime);
        final HasNextCondition hasNextCondition = keySchema.hasNextCondition(key,
                                                                             key,
                                                                             earliestSessionEndTime,
                                                                             latestSessionStartTime,
                                                                             true);
        final PeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator =
            new FilteredCacheIterator(cacheIterator, hasNextCondition, cacheFunction);
        return new MergedSortedCacheSessionStoreIterator(filteredCacheIterator, storeIterator, cacheFunction, true);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFindSessions(final Bytes key,
                                                                          final long earliestSessionEndTime,
                                                                          final long latestSessionStartTime) {
        validateStoreOpen();

        final PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator = wrapped().persistent() ?
            new CacheIteratorWrapper(key, earliestSessionEndTime, latestSessionStartTime, false) :
            context.cache().reverseRange(
                cacheName,
                cacheFunction.cacheKey(keySchema.lowerRangeFixedSize(key, earliestSessionEndTime)),
                cacheFunction.cacheKey(keySchema.upperRangeFixedSize(key, latestSessionStartTime)
                )
            );

        final KeyValueIterator<Windowed<Bytes>, byte[]> storeIterator = wrapped().backwardFindSessions(
            key,
            earliestSessionEndTime,
            latestSessionStartTime
        );
        final HasNextCondition hasNextCondition = keySchema.hasNextCondition(
            key,
            key,
            earliestSessionEndTime,
            latestSessionStartTime,
            false
        );
        final PeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator =
            new FilteredCacheIterator(cacheIterator, hasNextCondition, cacheFunction);
        return new MergedSortedCacheSessionStoreIterator(filteredCacheIterator, storeIterator, cacheFunction, false);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(final Bytes keyFrom,
                                                                  final Bytes keyTo,
                                                                  final long earliestSessionEndTime,
                                                                  final long latestSessionStartTime) {
        if (keyFrom != null && keyTo != null && keyFrom.compareTo(keyTo) > 0) {
            LOG.warn(INVALID_RANGE_WARN_MSG);
            return KeyValueIterators.emptyIterator();
        }

        validateStoreOpen();

        final Bytes cacheKeyFrom = keyFrom == null ? null : cacheFunction.cacheKey(keySchema.lowerRange(keyFrom, earliestSessionEndTime));
        final Bytes cacheKeyTo = keyTo == null ? null : cacheFunction.cacheKey(keySchema.upperRange(keyTo, latestSessionStartTime));
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = context.cache().range(cacheName, cacheKeyFrom, cacheKeyTo);

        final KeyValueIterator<Windowed<Bytes>, byte[]> storeIterator = wrapped().findSessions(
            keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime
        );
        final HasNextCondition hasNextCondition = keySchema.hasNextCondition(keyFrom,
                                                                             keyTo,
                                                                             earliestSessionEndTime,
                                                                             latestSessionStartTime,
                                                                     true);
        final PeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator =
            new FilteredCacheIterator(cacheIterator, hasNextCondition, cacheFunction);
        return new MergedSortedCacheSessionStoreIterator(filteredCacheIterator, storeIterator, cacheFunction, true);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFindSessions(final Bytes keyFrom,
                                                                          final Bytes keyTo,
                                                                          final long earliestSessionEndTime,
                                                                          final long latestSessionStartTime) {
        if (keyFrom != null && keyTo != null && keyFrom.compareTo(keyTo) > 0) {
            LOG.warn(INVALID_RANGE_WARN_MSG);
            return KeyValueIterators.emptyIterator();
        }

        validateStoreOpen();

        final Bytes cacheKeyFrom = keyFrom == null ? null : cacheFunction.cacheKey(keySchema.lowerRange(keyFrom, earliestSessionEndTime));
        final Bytes cacheKeyTo = keyTo == null ? null : cacheFunction.cacheKey(keySchema.upperRange(keyTo, latestSessionStartTime));
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = context.cache().reverseRange(cacheName, cacheKeyFrom, cacheKeyTo);

        final KeyValueIterator<Windowed<Bytes>, byte[]> storeIterator =
            wrapped().backwardFindSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime);
        final HasNextCondition hasNextCondition = keySchema.hasNextCondition(
            keyFrom,
            keyTo,
            earliestSessionEndTime,
            latestSessionStartTime,
            false
        );
        final PeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator =
            new FilteredCacheIterator(cacheIterator, hasNextCondition, cacheFunction);
        return new MergedSortedCacheSessionStoreIterator(filteredCacheIterator, storeIterator, cacheFunction, false);
    }

    @Override
    public byte[] fetchSession(final Bytes key, final long earliestSessionEndTime, final long latestSessionStartTime) {
        Objects.requireNonNull(key, "key cannot be null");
        validateStoreOpen();
        if (context.cache() == null) {
            return wrapped().fetchSession(key, earliestSessionEndTime, latestSessionStartTime);
        } else {
            final Bytes bytesKey = SessionKeySchema.toBinary(key, earliestSessionEndTime,
                latestSessionStartTime);
            final Bytes cacheKey = cacheFunction.cacheKey(bytesKey);
            final LRUCacheEntry entry = context.cache().get(cacheName, cacheKey);
            if (entry == null) {
                return wrapped().fetchSession(key, earliestSessionEndTime, latestSessionStartTime);
            } else {
                return entry.value();
            }
        }
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes key) {
        Objects.requireNonNull(key, "key cannot be null");
        return findSessions(key, 0, Long.MAX_VALUE);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(final Bytes key) {
        Objects.requireNonNull(key, "key cannot be null");
        return backwardFindSessions(key, 0, Long.MAX_VALUE);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes keyFrom,
                                                           final Bytes keyTo) {
        return findSessions(keyFrom, keyTo, 0, Long.MAX_VALUE);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(final Bytes keyFrom,
                                                                   final Bytes keyTo) {
        return backwardFindSessions(keyFrom, keyTo, 0, Long.MAX_VALUE);
    }

    public void flush() {
        context.cache().flush(cacheName);
        wrapped().flush();
    }

    @Override
    public void flushCache() {
        context.cache().flush(cacheName);
    }

    public void close() {
        final LinkedList<RuntimeException> suppressed = executeAll(
            () -> context.cache().flush(cacheName),
            () -> context.cache().close(cacheName),
            wrapped()::close
        );
        if (!suppressed.isEmpty()) {
            throwSuppressed("Caught an exception while closing caching session store for store " + name(),
                            suppressed);
        }
    }

    private class CacheIteratorWrapper implements PeekingKeyValueIterator<Bytes, LRUCacheEntry> {

        private final long segmentInterval;

        private final Bytes keyFrom;
        private final Bytes keyTo;
        private final long latestSessionStartTime;
        private final boolean forward;

        private long lastSegmentId;

        private long currentSegmentId;
        private Bytes cacheKeyFrom;
        private Bytes cacheKeyTo;

        private ThreadCache.MemoryLRUCacheBytesIterator current;

        private CacheIteratorWrapper(final Bytes key,
                                     final long earliestSessionEndTime,
                                     final long latestSessionStartTime,
                                     final boolean forward) {
            this(key, key, earliestSessionEndTime, latestSessionStartTime, forward);
        }

        private CacheIteratorWrapper(final Bytes keyFrom,
                                     final Bytes keyTo,
                                     final long earliestSessionEndTime,
                                     final long latestSessionStartTime,
                                     final boolean forward) {
            this.keyFrom = keyFrom;
            this.keyTo = keyTo;
            this.latestSessionStartTime = latestSessionStartTime;
            this.segmentInterval = cacheFunction.getSegmentInterval();
            this.forward = forward;


            if (forward) {
                this.currentSegmentId = cacheFunction.segmentId(earliestSessionEndTime);
                this.lastSegmentId = cacheFunction.segmentId(maxObservedTimestamp);

                setCacheKeyRange(earliestSessionEndTime, currentSegmentLastTime());
                this.current = context.cache().range(cacheName, cacheKeyFrom, cacheKeyTo);
            } else {
                this.lastSegmentId = cacheFunction.segmentId(earliestSessionEndTime);
                this.currentSegmentId = cacheFunction.segmentId(maxObservedTimestamp);

                setCacheKeyRange(currentSegmentBeginTime(), Math.min(latestSessionStartTime, maxObservedTimestamp));
                this.current = context.cache().reverseRange(cacheName, cacheKeyFrom, cacheKeyTo);
            }
        }

        @Override
        public boolean hasNext() {
            if (current == null) {
                return false;
            }

            if (current.hasNext()) {
                return true;
            }

            while (!current.hasNext()) {
                getNextSegmentIterator();
                if (current == null) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public Bytes peekNextKey() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return current.peekNextKey();
        }

        @Override
        public KeyValue<Bytes, LRUCacheEntry> peekNext() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return current.peekNext();
        }

        @Override
        public KeyValue<Bytes, LRUCacheEntry> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return current.next();
        }

        @Override
        public void close() {
            current.close();
        }

        private long currentSegmentBeginTime() {
            return currentSegmentId * segmentInterval;
        }

        private long currentSegmentLastTime() {
            return currentSegmentBeginTime() + segmentInterval - 1;
        }

        private void getNextSegmentIterator() {
            if (forward) {
                ++currentSegmentId;
                lastSegmentId = cacheFunction.segmentId(maxObservedTimestamp);

                if (currentSegmentId > lastSegmentId) {
                    current = null;
                    return;
                }

                setCacheKeyRange(currentSegmentBeginTime(), currentSegmentLastTime());

                current.close();

                current = context.cache().range(cacheName, cacheKeyFrom, cacheKeyTo);
            } else {
                --currentSegmentId;

                if (currentSegmentId < lastSegmentId) {
                    current = null;
                    return;
                }

                setCacheKeyRange(currentSegmentBeginTime(), currentSegmentLastTime());

                current.close();

                current = context.cache().reverseRange(cacheName, cacheKeyFrom, cacheKeyTo);
            }

        }

        private void setCacheKeyRange(final long lowerRangeEndTime, final long upperRangeEndTime) {
            if (cacheFunction.segmentId(lowerRangeEndTime) != cacheFunction.segmentId(upperRangeEndTime)) {
                throw new IllegalStateException("Error iterating over segments: segment interval has changed");
            }

            if (keyFrom.equals(keyTo)) {
                cacheKeyFrom = cacheFunction.cacheKey(segmentLowerRangeFixedSize(keyFrom, lowerRangeEndTime));
                cacheKeyTo = cacheFunction.cacheKey(segmentUpperRangeFixedSize(keyTo, upperRangeEndTime));
            } else {
                cacheKeyFrom = cacheFunction.cacheKey(keySchema.lowerRange(keyFrom, lowerRangeEndTime), currentSegmentId);
                cacheKeyTo = cacheFunction.cacheKey(keySchema.upperRange(keyTo, latestSessionStartTime), currentSegmentId);
            }
        }

        private Bytes segmentLowerRangeFixedSize(final Bytes key, final long segmentBeginTime) {
            final Windowed<Bytes> sessionKey = new Windowed<>(key, new SessionWindow(0, Math.max(0, segmentBeginTime)));
            return SessionKeySchema.toBinary(sessionKey);
        }

        private Bytes segmentUpperRangeFixedSize(final Bytes key, final long segmentEndTime) {
            final Windowed<Bytes> sessionKey = new Windowed<>(key, new SessionWindow(Math.min(latestSessionStartTime, segmentEndTime), segmentEndTime));
            return SessionKeySchema.toBinary(sessionKey);
        }
    }
}
