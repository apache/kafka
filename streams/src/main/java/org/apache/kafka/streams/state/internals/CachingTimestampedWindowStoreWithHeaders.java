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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorContextUtils;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.RecordQueue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.TimestampedWindowStoreWithHeaders;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.kafka.streams.processor.internals.ProcessorContextUtils.asInternalProcessorContext;
import static org.apache.kafka.streams.state.internals.ExceptionUtils.executeAll;
import static org.apache.kafka.streams.state.internals.ExceptionUtils.throwSuppressed;

class CachingTimestampedWindowStoreWithHeaders
    extends WrappedStateStore<TimestampedWindowStoreWithHeaders<Bytes, byte[]>, Windowed<Bytes>, ValueTimestampHeaders<byte[]>>
    implements TimestampedWindowStoreWithHeaders<Bytes, byte[]>, CachedStateStore<Windowed<Bytes>, ValueTimestampHeaders<byte[]>> {

    private final long windowSize;
    private final SegmentedCacheFunction cacheFunction;
    private final SegmentedBytesStore.KeySchema keySchema = new WindowKeySchema();

    private String cacheName;
    private boolean sendOldValues;
    private InternalProcessorContext<?, ?> internalContext;
    private StateSerdes<Bytes, byte[]> bytesSerdes;
    private CacheFlushListener<Windowed<Bytes>, ValueTimestampHeaders<byte[]>> flushListener;
    private final AtomicLong maxObservedTimestamp;

    CachingTimestampedWindowStoreWithHeaders(final TimestampedWindowStoreWithHeaders<Bytes, byte[]> underlying,
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

        internalContext.registerCacheFlushListener(cacheName, entries -> entries.forEach(entry -> putAndMaybeForward(entry, internalContext)));

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
            final ValueTimestampHeaders<byte[]> rawOldValue = rawNewValue == null || sendOldValues ?
                wrapped().fetch(binaryKey, windowStartTimestamp) : null;

            if (rawNewValue != null || rawOldValue != null) {
                final ProcessorRecordContext current = context.recordContext();
                try {
                    context.setRecordContext(entry.entry().context());
                    wrapped().put(binaryKey, rawNewValue, windowStartTimestamp,
                        entry.entry().context().timestamp(), entry.entry().context().headers());
                    // Flush listener would go here if needed
                } finally {
                    context.setRecordContext(current);
                }
            }
        } else {
            final ProcessorRecordContext current = context.recordContext();
            try {
                context.setRecordContext(entry.entry().context());
                wrapped().put(binaryKey, entry.newValue(), windowStartTimestamp,
                    entry.entry().context().timestamp(), entry.entry().context().headers());
            } finally {
                context.setRecordContext(current);
            }
        }
    }

    @Override
    public boolean setFlushListener(final CacheFlushListener<Windowed<Bytes>, ValueTimestampHeaders<byte[]>> flushListener,
                                    final boolean sendOldValues) {
        this.flushListener = flushListener;
        this.sendOldValues = sendOldValues;
        return true;
    }

    // write path
    @Override
    public synchronized void put(final Bytes key,
                                 final byte[] value,
                                 final long windowStartTimestamp,
                                 final long timestamp,
                                 final Headers headers) {
        validateStoreOpen();

        final Bytes keyBytes = WindowKeySchema.toStoreKeyBinary(key, windowStartTimestamp, 0);

        final LRUCacheEntry entry = new LRUCacheEntry(
            value,
            headers,
            true,
            internalContext.recordContext().offset(),
            timestamp,
            internalContext.recordContext().partition(),
            internalContext.recordContext().topic(),
            internalContext.recordContext().sourceRawKey(),
            internalContext.recordContext().sourceRawValue()
        );

        internalContext.cache().put(cacheName, cacheFunction.cacheKey(keyBytes), entry);
        maxObservedTimestamp.set(Math.max(timestamp, maxObservedTimestamp.get()));
    }

    @Override
    public void put(final Bytes key, final ValueTimestampHeaders<byte[]> value, final long windowStartTimestamp) {
        if (value == null) {
            put(key, null, windowStartTimestamp, 0, new RecordHeaders());
        } else {
            put(key, value.value(), windowStartTimestamp, value.timestamp(), value.headers());
        }
    }

    // read path
    @Override
    public WindowStoreIterator<ValueTimestampHeaders<byte[]>> fetchWithHeaders(final Bytes key,
                                                                               final long timeFrom,
                                                                               final long timeTo) {
        validateStoreOpen();

        final WindowStoreIterator<ValueTimestampHeaders<byte[]>> underlyingIterator =
            wrapped().fetchWithHeaders(key, timeFrom, timeTo);

        if (internalContext.cache() == null) {
            return underlyingIterator;
        }

        final PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator = wrapped().persistent() ?
            new CacheIteratorWrapper(key, timeFrom, timeTo, true) :
            internalContext.cache().range(
                cacheName,
                cacheFunction.cacheKey(keySchema.lowerRangeFixedSize(key, timeFrom)),
                cacheFunction.cacheKey(keySchema.upperRangeFixedSize(key, timeTo))
            );

        final HasNextCondition hasNextCondition = keySchema.hasNextCondition(key, key, timeFrom, timeTo, true);
        final PeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator =
            new FilteredCacheIterator(cacheIterator, hasNextCondition, cacheFunction);

        return new MergedSortedCacheWindowStoreWithHeadersIterator(filteredCacheIterator, underlyingIterator, true);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, ValueTimestampHeaders<byte[]>> fetchWithHeaders(Bytes keyFrom, Bytes keyTo, long timeFrom, long timeTo) {
        validateStoreOpen();

        final KeyValueIterator<Windowed<Bytes>, ValueTimestampHeaders<byte[]>> underlyingIterator =
            wrapped().fetchWithHeaders(keyFrom, keyTo, timeFrom, timeTo);

        if (internalContext.cache() == null) {
            return underlyingIterator;
        }

        final PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator = wrapped().persistent() ?
            new CacheIteratorWrapper(keyFrom, keyTo, timeFrom, timeTo, true) :
            internalContext.cache().range(
                cacheName,
                keyFrom == null ? null : cacheFunction.cacheKey(keySchema.lowerRange(keyFrom, timeFrom)),
                keyTo == null ? null : cacheFunction.cacheKey(keySchema.upperRange(keyTo, timeTo))
            );

        final HasNextCondition hasNextCondition = keySchema.hasNextCondition(keyFrom, keyTo, timeFrom, timeTo, true);
        final PeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator =
            new FilteredCacheIterator(cacheIterator, hasNextCondition, cacheFunction);

        return new MergedSortedCacheWindowStoreKeyValueWithHeadersIterator(
            filteredCacheIterator,
            underlyingIterator,
            bytesSerdes,
            windowSize,
            cacheFunction,
            true
        );
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, ValueTimestampHeaders<byte[]>> fetchAllWithHeaders(long timeFrom, long timeTo) {
        validateStoreOpen();

        final KeyValueIterator<Windowed<Bytes>, ValueTimestampHeaders<byte[]>> underlyingIterator =
            wrapped().fetchAllWithHeaders(timeFrom, timeTo);

        if (internalContext.cache() == null) {
            return underlyingIterator;
        }

        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = internalContext.cache().all(cacheName);

        final HasNextCondition hasNextCondition = keySchema.hasNextCondition(null, null, timeFrom, timeTo, true);
        final PeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator =
            new FilteredCacheIterator(cacheIterator, hasNextCondition, cacheFunction);

        return new MergedSortedCacheWindowStoreKeyValueWithHeadersIterator(
            filteredCacheIterator,
            underlyingIterator,
            bytesSerdes,
            windowSize,
            cacheFunction,
            true
        );
    }

    @Override
    public synchronized void flush() {
        internalContext.cache().flush(cacheName);
        wrapped().flush();
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

    // Delegate methods from WindowStore
    @Override
    public ValueTimestampHeaders<byte[]> fetch(Bytes key, long time) {
        throw new UnsupportedOperationException("Use fetchWithHeaders instead");
    }

    @Override
    public WindowStoreIterator<ValueTimestampHeaders<byte[]>> fetch(Bytes key, long timeFrom, long timeTo) {
        return fetchWithHeaders(key, timeFrom, timeTo);
    }

    @Override
    public WindowStoreIterator<ValueTimestampHeaders<byte[]>> backwardFetch(Bytes key, long timeFrom, long timeTo) {
        return wrapped().backwardFetch(key, timeFrom, timeTo);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, ValueTimestampHeaders<byte[]>> fetch(Bytes keyFrom, Bytes keyTo, long timeFrom, long timeTo) {
        return fetchWithHeaders(keyFrom, keyTo, timeFrom, timeTo);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, ValueTimestampHeaders<byte[]>> backwardFetch(Bytes keyFrom, Bytes keyTo, long timeFrom, long timeTo) {
        return wrapped().backwardFetch(keyFrom, keyTo, timeFrom, timeTo);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, ValueTimestampHeaders<byte[]>> fetchAll(long timeFrom, long timeTo) {
        return fetchAllWithHeaders(timeFrom, timeTo);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, ValueTimestampHeaders<byte[]>> backwardFetchAll(long timeFrom, long timeTo) {
        return wrapped().backwardFetchAll(timeFrom, timeTo);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, ValueTimestampHeaders<byte[]>> all() {
        return wrapped().all();
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, ValueTimestampHeaders<byte[]>> backwardAll() {
        return wrapped().backwardAll();
    }

    // CacheIteratorWrapper
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
                throw new java.util.NoSuchElementException();
            }
            return current.peekNextKey();
        }

        @Override
        public KeyValue<Bytes, LRUCacheEntry> peekNext() {
            if (!hasNext()) {
                throw new java.util.NoSuchElementException();
            }
            return current.peekNext();
        }

        @Override
        public KeyValue<Bytes, LRUCacheEntry> next() {
            if (!hasNext()) {
                throw new java.util.NoSuchElementException();
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
