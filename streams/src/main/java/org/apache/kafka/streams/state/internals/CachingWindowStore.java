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

import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorContextUtils;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.RecordQueue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.kafka.streams.processor.internals.ProcessorContextUtils.asInternalProcessorContext;
import static org.apache.kafka.streams.state.internals.ExceptionUtils.executeAll;
import static org.apache.kafka.streams.state.internals.ExceptionUtils.throwSuppressed;

public class CachingWindowStore
    extends WrappedStateStore<WindowStore<Bytes, byte[]>, byte[], byte[]>
    implements WindowStore<Bytes, byte[]>, CachedStateStore<byte[], byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(CachingWindowStore.class);

    private final long windowSize;
    private final SegmentedCacheFunction cacheFunction;
    private final SegmentedBytesStore.KeySchema keySchema = new WindowKeySchema();

    private String cacheName;
    private boolean sendOldValues;
    private InternalProcessorContext<?, ?> internalContext;
    private StateSerdes<Bytes, byte[]> bytesSerdes;
    private CacheFlushListener<byte[], byte[]> flushListener;

    private final AtomicLong maxObservedTimestamp;

    CachingWindowStore(final WindowStore<Bytes, byte[]> underlying,
                       final long windowSize,
                       final long segmentInterval) {
        super(underlying);
        this.windowSize = windowSize;
        this.cacheFunction = new SegmentedCacheFunction(keySchema, segmentInterval);
        this.maxObservedTimestamp = new AtomicLong(RecordQueue.UNKNOWN);
    }

    @Override
    public void init(final StateStoreContext stateStoreContext, final StateStore root) {
        final String changelogTopic = ProcessorContextUtils.changelogFor(stateStoreContext, name(), Boolean.TRUE);
        internalContext = asInternalProcessorContext(stateStoreContext);
        bytesSerdes = new StateSerdes<>(
            changelogTopic,
            Serdes.Bytes(),
            Serdes.ByteArray());
        cacheName = internalContext.taskId() + "-" + name();

        internalContext.registerCacheFlushListener(cacheName, entries -> {
            for (final ThreadCache.DirtyEntry entry : entries) {
                putAndMaybeForward(entry, internalContext);
            }
        });

        super.init(stateStoreContext, root);
    }

    private void putAndMaybeForward(final ThreadCache.DirtyEntry entry,
                                    final InternalProcessorContext<?, ?> context) {
        final byte[] binaryWindowKey = cacheFunction.key(entry.key()).get();
        final Windowed<Bytes> windowedKeyBytes = WindowKeySchema.fromStoreBytesKey(binaryWindowKey, windowSize);
        final long windowStartTimestamp = windowedKeyBytes.window().start();
        final Bytes binaryKey = windowedKeyBytes.key();
        if (flushListener != null) {
            final byte[] rawNewValue = entry.newValue();
            final byte[] rawOldValue = rawNewValue == null || sendOldValues ?
                wrapped().fetch(binaryKey, windowStartTimestamp) : null;

            // this is an optimization: if this key did not exist in underlying store and also not in the cache,
            // we can skip flushing to downstream as well as writing to underlying store
            if (rawNewValue != null || rawOldValue != null) {
                // we need to get the old values if needed, and then put to store, and then flush

                final ProcessorRecordContext current = context.recordContext();
                try {
                    context.setRecordContext(entry.entry().context());
                    wrapped().put(binaryKey, entry.newValue(), windowStartTimestamp);
                    flushListener.apply(
                        new Record<>(
                            binaryWindowKey,
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
                wrapped().put(binaryKey, entry.newValue(), windowStartTimestamp);
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
    public synchronized void put(final Bytes key,
                                 final byte[] value,
                                 final long windowStartTimestamp) {
        // since this function may not access the underlying inner store, we need to validate
        // if store is open outside as well.
        validateStoreOpen();

        final Bytes keyBytes = WindowKeySchema.toStoreKeyBinary(key, windowStartTimestamp, 0);
        final LRUCacheEntry entry =
            new LRUCacheEntry(
                value,
                internalContext.recordContext().headers(),
                true,
                internalContext.recordContext().offset(),
                internalContext.recordContext().timestamp(),
                internalContext.recordContext().partition(),
                internalContext.recordContext().topic(),
                internalContext.recordContext().sourceRawKey(),
                internalContext.recordContext().sourceRawValue()
            );
        internalContext.cache().put(cacheName, cacheFunction.cacheKey(keyBytes), entry);

        maxObservedTimestamp.set(Math.max(keySchema.segmentTimestamp(keyBytes), maxObservedTimestamp.get()));
    }

    @Override
    public byte[] fetch(final Bytes key, final long timestamp) {
        return fetchPointInternal(key, timestamp, wrapped());
    }

    private byte[] fetchPointInternal(final Bytes key,
                                      final long timestamp,
                                      final ReadOnlyWindowStore<Bytes, byte[]> source) {
        validateStoreOpen();
        final Bytes bytesKey = WindowKeySchema.toStoreKeyBinary(key, timestamp, 0);
        final Bytes cacheKey = cacheFunction.cacheKey(bytesKey);
        if (internalContext.cache() == null) {
            return source.fetch(key, timestamp);
        }
        final LRUCacheEntry entry = internalContext.cache().get(cacheName, cacheKey);
        if (entry == null) {
            return source.fetch(key, timestamp);
        } else {
            return entry.value();
        }
    }


    @Override
    public synchronized WindowStoreIterator<byte[]> fetch(final Bytes key,
                                                          final long timeFrom,
                                                          final long timeTo) {
        validateStoreOpen();
        return fetchInternal(wrapped().fetch(key, timeFrom, timeTo), key, timeFrom, timeTo, true);
    }

    @Override
    public synchronized WindowStoreIterator<byte[]> backwardFetch(final Bytes key,
                                                                  final long timeFrom,
                                                                  final long timeTo) {
        validateStoreOpen();
        return fetchInternal(wrapped().backwardFetch(key, timeFrom, timeTo), key, timeFrom, timeTo, false);
    }

    private synchronized WindowStoreIterator<byte[]> fetchInternal(final WindowStoreIterator<byte[]> underlyingIterator,
                                                                   final Bytes key,
                                                                   final long timeFrom,
                                                                   final long timeTo,
                                                                   final boolean forward) {
        if (internalContext.cache() == null) {
            return underlyingIterator;
        }

        final PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator = wrapped().persistent() ?
            new CacheIteratorWrapper(key, timeFrom, timeTo, forward) :
            (forward ?
                internalContext.cache().range(
                    cacheName,
                    cacheFunction.cacheKey(keySchema.lowerRangeFixedSize(key, timeFrom)),
                    cacheFunction.cacheKey(keySchema.upperRangeFixedSize(key, timeTo))
                ) :
                internalContext.cache().reverseRange(
                    cacheName,
                    cacheFunction.cacheKey(keySchema.lowerRangeFixedSize(key, timeFrom)),
                    cacheFunction.cacheKey(keySchema.upperRangeFixedSize(key, timeTo))
                )
            );
        final HasNextCondition hasNextCondition = keySchema.hasNextCondition(key, key, timeFrom, timeTo, forward);
        final PeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator =
            new FilteredCacheIterator(cacheIterator, hasNextCondition, cacheFunction);
        return new MergedSortedCacheWindowStoreIterator(filteredCacheIterator, underlyingIterator, forward);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes keyFrom,
                                                           final Bytes keyTo,
                                                           final long timeFrom,
                                                           final long timeTo) {
        if (isInvalidKeyRange(keyFrom, keyTo)) {
            return KeyValueIterators.emptyIterator();
        }

        // since this function may not access the underlying inner store, we need to validate
        // if store is open outside as well.
        validateStoreOpen();
        return fetchKeyRangeInternal(wrapped().fetch(keyFrom, keyTo, timeFrom, timeTo), keyFrom, keyTo, timeFrom, timeTo, true);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(final Bytes keyFrom,
                                                                   final Bytes keyTo,
                                                                   final long timeFrom,
                                                                   final long timeTo) {
        if (isInvalidKeyRange(keyFrom, keyTo)) {
            return KeyValueIterators.emptyIterator();
        }

        // since this function may not access the underlying inner store, we need to validate
        // if store is open outside as well.
        validateStoreOpen();
        return fetchKeyRangeInternal(wrapped().backwardFetch(keyFrom, keyTo, timeFrom, timeTo), keyFrom, keyTo, timeFrom, timeTo, false);
    }

    private boolean isInvalidKeyRange(final Bytes keyFrom, final Bytes keyTo) {
        if (keyFrom != null && keyTo != null && keyFrom.compareTo(keyTo) > 0) {
            LOG.warn("Returning empty iterator for fetch with invalid key range: from > to. " +
                    "This may be due to range arguments set in the wrong order, " +
                    "or serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                    "Note that the built-in numerical serdes do not follow this for negative numbers");
            return true;
        }
        return false;
    }

    private synchronized KeyValueIterator<Windowed<Bytes>, byte[]> fetchKeyRangeInternal(final KeyValueIterator<Windowed<Bytes>, byte[]> underlyingIterator,
                                                                                         final Bytes keyFrom,
                                                                                         final Bytes keyTo,
                                                                                         final long timeFrom,
                                                                                         final long timeTo,
                                                                                         final boolean forward) {
        if (internalContext.cache() == null) {
            return underlyingIterator;
        }

        final PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator = wrapped().persistent() ?
            new CacheIteratorWrapper(keyFrom, keyTo, timeFrom, timeTo, forward) :
            (forward ?
                internalContext.cache().range(
                    cacheName,
                    keyFrom == null ? null : cacheFunction.cacheKey(keySchema.lowerRange(keyFrom, timeFrom)),
                    keyTo == null ? null : cacheFunction.cacheKey(keySchema.upperRange(keyTo, timeTo))
                ) :
                internalContext.cache().reverseRange(
                    cacheName,
                    keyFrom == null ? null : cacheFunction.cacheKey(keySchema.lowerRange(keyFrom, timeFrom)),
                    keyTo == null ? null : cacheFunction.cacheKey(keySchema.upperRange(keyTo, timeTo))
                )
            );
        final HasNextCondition hasNextCondition = keySchema.hasNextCondition(keyFrom, keyTo, timeFrom, timeTo, forward);
        final PeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator =
            new FilteredCacheIterator(cacheIterator, hasNextCondition, cacheFunction);

        return new MergedSortedCacheWindowStoreKeyValueIterator(
            filteredCacheIterator, underlyingIterator, bytesSerdes, windowSize, cacheFunction, forward);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(final long timeFrom, final long timeTo) {
        validateStoreOpen();
        return fetchAllInternal(wrapped().fetchAll(timeFrom, timeTo), timeFrom, timeTo, true);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetchAll(final long timeFrom, final long timeTo) {
        validateStoreOpen();
        return fetchAllInternal(wrapped().backwardFetchAll(timeFrom, timeTo), timeFrom, timeTo, false);
    }

    private KeyValueIterator<Windowed<Bytes>, byte[]> fetchAllInternal(final KeyValueIterator<Windowed<Bytes>, byte[]> underlyingIterator,
                                                                       final long timeFrom,
                                                                       final long timeTo,
                                                                       final boolean forward) {
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = forward ?
            internalContext.cache().all(cacheName) :
            internalContext.cache().reverseAll(cacheName);
        final HasNextCondition hasNextCondition = keySchema.hasNextCondition(null, null, timeFrom, timeTo, forward);
        final PeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator =
            new FilteredCacheIterator(cacheIterator, hasNextCondition, cacheFunction);
        return new MergedSortedCacheWindowStoreKeyValueIterator(
            filteredCacheIterator, underlyingIterator, bytesSerdes, windowSize, cacheFunction, forward);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> all() {
        validateStoreOpen();
        return allInternal(wrapped().all(), true);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardAll() {
        validateStoreOpen();
        return allInternal(wrapped().backwardAll(), false);
    }

    private KeyValueIterator<Windowed<Bytes>, byte[]> allInternal(final KeyValueIterator<Windowed<Bytes>, byte[]> underlyingIterator,
                                                                  final boolean forward) {
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = forward ?
            internalContext.cache().all(cacheName) :
            internalContext.cache().reverseAll(cacheName);
        return new MergedSortedCacheWindowStoreKeyValueIterator(
            cacheIterator, underlyingIterator, bytesSerdes, windowSize, cacheFunction, forward);
    }

    @Override
    public ReadOnlyWindowStore<Bytes, byte[]> readOnly(final IsolationLevel isolationLevel) {
        Objects.requireNonNull(isolationLevel, "isolationLevel cannot be null");
        if (isolationLevel == IsolationLevel.READ_COMMITTED) {
            return wrapped().readOnly(isolationLevel);
        }
        return new ReadOnlyView(wrapped().readOnly(isolationLevel));
    }

    @Override
    public synchronized void commit(final Map<TopicPartition, Long> changelogOffsets) {
        internalContext.cache().flush(cacheName);
        wrapped().commit(changelogOffsets);
    }

    @Override
    public void flushCache() {
        internalContext.cache().flush(cacheName);
    }

    @Override
    public void clearCache() {
        internalContext.cache().clear(cacheName);
    }

    @Override
    public synchronized void close() {
        final LinkedList<RuntimeException> suppressed = executeAll(
            () -> internalContext.cache().flush(cacheName),
            () -> internalContext.cache().close(cacheName),
            wrapped()::close
        );
        if (!suppressed.isEmpty()) {
            throwSuppressed("Caught an exception while closing caching window store for store " + name(),
                suppressed);
        }
    }

    private final class ReadOnlyView implements ReadOnlyWindowStore<Bytes, byte[]> {

        private final ReadOnlyWindowStore<Bytes, byte[]> underlying;

        ReadOnlyView(final ReadOnlyWindowStore<Bytes, byte[]> underlying) {
            this.underlying = underlying;
        }

        @Override
        public byte[] fetch(final Bytes key, final long timestamp) {
            return fetchPointInternal(key, timestamp, underlying);
        }

        @Override
        public WindowStoreIterator<byte[]> fetch(final Bytes key, final Instant timeFrom, final Instant timeTo) {
            validateStoreOpen();
            final long timeFromMs = timeFrom.toEpochMilli();
            final long timeToMs = timeTo.toEpochMilli();
            return fetchInternal(underlying.fetch(key, timeFrom, timeTo), key, timeFromMs, timeToMs, true);
        }

        @Override
        public WindowStoreIterator<byte[]> backwardFetch(final Bytes key, final Instant timeFrom, final Instant timeTo) {
            validateStoreOpen();
            final long timeFromMs = timeFrom.toEpochMilli();
            final long timeToMs = timeTo.toEpochMilli();
            return fetchInternal(underlying.backwardFetch(key, timeFrom, timeTo), key, timeFromMs, timeToMs, false);
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes keyFrom, final Bytes keyTo, final Instant timeFrom, final Instant timeTo) {
            if (isInvalidKeyRange(keyFrom, keyTo)) {
                return KeyValueIterators.emptyIterator();
            }
            validateStoreOpen();
            final long timeFromMs = timeFrom.toEpochMilli();
            final long timeToMs = timeTo.toEpochMilli();
            return fetchKeyRangeInternal(underlying.fetch(keyFrom, keyTo, timeFrom, timeTo), keyFrom, keyTo, timeFromMs, timeToMs, true);
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(final Bytes keyFrom, final Bytes keyTo, final Instant timeFrom, final Instant timeTo) {
            if (isInvalidKeyRange(keyFrom, keyTo)) {
                return KeyValueIterators.emptyIterator();
            }
            validateStoreOpen();
            final long timeFromMs = timeFrom.toEpochMilli();
            final long timeToMs = timeTo.toEpochMilli();
            return fetchKeyRangeInternal(underlying.backwardFetch(keyFrom, keyTo, timeFrom, timeTo), keyFrom, keyTo, timeFromMs, timeToMs, false);
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(final Instant timeFrom, final Instant timeTo) {
            validateStoreOpen();
            return fetchAllInternal(underlying.fetchAll(timeFrom, timeTo), timeFrom.toEpochMilli(), timeTo.toEpochMilli(), true);
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetchAll(final Instant timeFrom, final Instant timeTo) {
            validateStoreOpen();
            return fetchAllInternal(underlying.backwardFetchAll(timeFrom, timeTo), timeFrom.toEpochMilli(), timeTo.toEpochMilli(), false);
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> all() {
            validateStoreOpen();
            return allInternal(underlying.all(), true);
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> backwardAll() {
            validateStoreOpen();
            return allInternal(underlying.backwardAll(), false);
        }
    }

    private class CacheIteratorWrapper implements PeekingKeyValueIterator<Bytes, LRUCacheEntry> {

        private final long segmentInterval;
        private final Bytes keyFrom;
        private final Bytes keyTo;
        private final long timeTo;
        private final boolean forward;

        private long lastSegmentId;
        private long currentSegmentId;
        private Bytes cacheKeyFrom;
        private Bytes cacheKeyTo;

        private ThreadCache.MemoryLRUCacheBytesIterator current;

        private CacheIteratorWrapper(final Bytes key,
                                     final long timeFrom,
                                     final long timeTo,
                                     final boolean forward) {
            this(key, key, timeFrom, timeTo, forward);
        }

        private CacheIteratorWrapper(final Bytes keyFrom,
                                     final Bytes keyTo,
                                     final long timeFrom,
                                     final long timeTo,
                                     final boolean forward) {
            this.keyFrom = keyFrom;
            this.keyTo = keyTo;
            this.timeTo = timeTo;
            this.forward = forward;

            this.segmentInterval = cacheFunction.getSegmentInterval();

            if (forward) {
                this.lastSegmentId = cacheFunction.segmentId(Math.min(timeTo, maxObservedTimestamp.get()));
                this.currentSegmentId = cacheFunction.segmentId(timeFrom);

                setCacheKeyRange(timeFrom, currentSegmentLastTime());
                this.current = internalContext.cache().range(cacheName, cacheKeyFrom, cacheKeyTo);
            } else {
                this.currentSegmentId = cacheFunction.segmentId(Math.min(timeTo, maxObservedTimestamp.get()));
                this.lastSegmentId = cacheFunction.segmentId(timeFrom);

                setCacheKeyRange(currentSegmentBeginTime(), Math.min(timeTo, maxObservedTimestamp.get()));
                this.current = internalContext.cache().reverseRange(cacheName, cacheKeyFrom, cacheKeyTo);
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
            return Math.min(timeTo, currentSegmentBeginTime() + segmentInterval - 1);
        }

        private void getNextSegmentIterator() {
            if (forward) {
                ++currentSegmentId;
                // updating as maxObservedTimestamp can change while iterating
                lastSegmentId = cacheFunction.segmentId(Math.min(timeTo, maxObservedTimestamp.get()));

                if (currentSegmentId > lastSegmentId) {
                    current = null;
                    return;
                }

                setCacheKeyRange(currentSegmentBeginTime(), currentSegmentLastTime());

                current.close();

                current = internalContext.cache().range(cacheName, cacheKeyFrom, cacheKeyTo);
            } else {
                --currentSegmentId;

                // last segment id is stable when iterating backward, therefore no need to update
                if (currentSegmentId < lastSegmentId) {
                    current = null;
                    return;
                }

                setCacheKeyRange(currentSegmentBeginTime(), currentSegmentLastTime());

                current.close();

                current = internalContext.cache().reverseRange(cacheName, cacheKeyFrom, cacheKeyTo);
            }
        }

        private void setCacheKeyRange(final long lowerRangeEndTime, final long upperRangeEndTime) {
            if (cacheFunction.segmentId(lowerRangeEndTime) != cacheFunction.segmentId(upperRangeEndTime)) {
                throw new IllegalStateException("Error iterating over segments: segment interval has changed");
            }

            if (keyFrom != null && keyFrom.equals(keyTo)) {
                cacheKeyFrom = cacheFunction.cacheKey(segmentLowerRangeFixedSize(keyFrom, lowerRangeEndTime));
                cacheKeyTo = cacheFunction.cacheKey(segmentUpperRangeFixedSize(keyTo, upperRangeEndTime));
            } else {
                cacheKeyFrom = keyFrom == null ? null :
                    cacheFunction.cacheKey(keySchema.lowerRange(keyFrom, lowerRangeEndTime), currentSegmentId);
                cacheKeyTo = keyTo == null ? null :
                    cacheFunction.cacheKey(keySchema.upperRange(keyTo, timeTo), currentSegmentId);
            }
        }

        private Bytes segmentLowerRangeFixedSize(final Bytes key, final long segmentBeginTime) {
            return WindowKeySchema.toStoreKeyBinary(key, Math.max(0, segmentBeginTime), 0);
        }

        private Bytes segmentUpperRangeFixedSize(final Bytes key, final long segmentEndTime) {
            return WindowKeySchema.toStoreKeyBinary(key, segmentEndTime, Integer.MAX_VALUE);
        }
    }
}
