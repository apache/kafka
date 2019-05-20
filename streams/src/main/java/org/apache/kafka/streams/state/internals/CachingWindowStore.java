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

import java.util.NoSuchElementException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.RecordQueue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CachingWindowStore
    extends WrappedStateStore<WindowStore<Bytes, byte[]>, byte[], byte[]>
    implements WindowStore<Bytes, byte[]>, CachedStateStore<byte[], byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(CachingWindowStore.class);

    private final long windowSize;
    private final SegmentedBytesStore.KeySchema keySchema = new WindowKeySchema();

    private String name;
    private ThreadCache cache;
    private boolean sendOldValues;
    private InternalProcessorContext context;
    private StateSerdes<Bytes, byte[]> bytesSerdes;
    private CacheFlushListener<byte[], byte[]> flushListener;

    private long maxObservedTimestamp;

    private final SegmentedCacheFunction cacheFunction;

    CachingWindowStore(final WindowStore<Bytes, byte[]> underlying,
                       final long windowSize,
                       final long segmentInterval) {
        super(underlying);
        this.windowSize = windowSize;
        this.cacheFunction = new SegmentedCacheFunction(keySchema, segmentInterval);
        this.maxObservedTimestamp = RecordQueue.UNKNOWN;
    }

    @Override
    public void init(final ProcessorContext context, final StateStore root) {
        initInternal((InternalProcessorContext) context);
        super.init(context, root);
    }

    @SuppressWarnings("unchecked")
    private void initInternal(final InternalProcessorContext context) {
        this.context = context;
        final String topic = ProcessorStateManager.storeChangelogTopic(context.applicationId(), name());

        bytesSerdes = new StateSerdes<>(
            topic,
            Serdes.Bytes(),
            Serdes.ByteArray());
        name = context.taskId() + "-" + name();
        cache = this.context.getCache();

        cache.addDirtyEntryFlushListener(name, entries -> {
            for (final ThreadCache.DirtyEntry entry : entries) {
                putAndMaybeForward(entry, context);
            }
        });
    }

    private void putAndMaybeForward(final ThreadCache.DirtyEntry entry,
                                    final InternalProcessorContext context) {
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
                wrapped().put(binaryKey, entry.newValue(), windowStartTimestamp);

                final ProcessorRecordContext current = context.recordContext();
                context.setRecordContext(entry.entry().context());
                try {
                    flushListener.apply(
                        binaryWindowKey,
                        rawNewValue,
                        sendOldValues ? rawOldValue : null,
                        entry.entry().context().timestamp());
                } finally {
                    context.setRecordContext(current);
                }
            }
        } else {
            wrapped().put(binaryKey, entry.newValue(), windowStartTimestamp);
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
                                 final byte[] value) {
        put(key, value, context.timestamp());
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
                context.headers(),
                true,
                context.offset(),
                context.timestamp(),
                context.partition(),
                context.topic());
        cache.put(name, cacheFunction.cacheKey(keyBytes), entry);

        maxObservedTimestamp = Math.max(keySchema.segmentTimestamp(keyBytes), maxObservedTimestamp);
    }

    @Override
    public byte[] fetch(final Bytes key,
                        final long timestamp) {
        validateStoreOpen();
        final Bytes bytesKey = WindowKeySchema.toStoreKeyBinary(key, timestamp, 0);
        final Bytes cacheKey = cacheFunction.cacheKey(bytesKey);
        if (cache == null) {
            return wrapped().fetch(key, timestamp);
        }
        final LRUCacheEntry entry = cache.get(name, cacheKey);
        if (entry == null) {
            return wrapped().fetch(key, timestamp);
        } else {
            return entry.value();
        }
    }

    @SuppressWarnings("deprecation") // note, this method must be kept if super#fetch(...) is removed
    @Override
    public synchronized WindowStoreIterator<byte[]> fetch(final Bytes key,
                                                          final long timeFrom,
                                                          final long timeTo) {
        // since this function may not access the underlying inner store, we need to validate
        // if store is open outside as well.
        validateStoreOpen();

        final WindowStoreIterator<byte[]> underlyingIterator = wrapped().fetch(key, timeFrom, timeTo);
        if (cache == null) {
            return underlyingIterator;
        }

        final PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator = wrapped().persistent() ?
            new CacheIteratorWrapper(key, timeFrom, timeTo) :
            cache.range(name,
                        cacheFunction.cacheKey(keySchema.lowerRangeFixedSize(key, timeFrom)),
                        cacheFunction.cacheKey(keySchema.upperRangeFixedSize(key, timeTo))
            );

        final HasNextCondition hasNextCondition = keySchema.hasNextCondition(key, key, timeFrom, timeTo);
        final PeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator = new FilteredCacheIterator(
            cacheIterator, hasNextCondition, cacheFunction
        );

        return new MergedSortedCacheWindowStoreIterator(filteredCacheIterator, underlyingIterator);
    }

    @SuppressWarnings("deprecation") // note, this method must be kept if super#fetch(...) is removed
    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes from,
                                                           final Bytes to,
                                                           final long timeFrom,
                                                           final long timeTo) {
        if (from.compareTo(to) > 0) {
            LOG.warn("Returning empty iterator for fetch with invalid key range: from > to. "
                + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                "Note that the built-in numerical serdes do not follow this for negative numbers");
            return KeyValueIterators.emptyIterator();
        }

        // since this function may not access the underlying inner store, we need to validate
        // if store is open outside as well.
        validateStoreOpen();

        final KeyValueIterator<Windowed<Bytes>, byte[]> underlyingIterator =
            wrapped().fetch(from, to, timeFrom, timeTo);
        if (cache == null) {
            return underlyingIterator;
        }

        final PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator = wrapped().persistent() ?
            new CacheIteratorWrapper(from, to, timeFrom, timeTo) :
            cache.range(name,
                        cacheFunction.cacheKey(keySchema.lowerRange(from, timeFrom)),
                        cacheFunction.cacheKey(keySchema.upperRange(to, timeTo))
            );

        final HasNextCondition hasNextCondition = keySchema.hasNextCondition(from, to, timeFrom, timeTo);
        final PeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator = new FilteredCacheIterator(cacheIterator, hasNextCondition, cacheFunction);

        return new MergedSortedCacheWindowStoreKeyValueIterator(
            filteredCacheIterator,
            underlyingIterator,
            bytesSerdes,
            windowSize,
            cacheFunction
        );
    }

    @SuppressWarnings("deprecation") // note, this method must be kept if super#fetchAll(...) is removed
    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(final long timeFrom,
                                                              final long timeTo) {
        validateStoreOpen();

        final KeyValueIterator<Windowed<Bytes>, byte[]> underlyingIterator = wrapped().fetchAll(timeFrom, timeTo);
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = cache.all(name);

        final HasNextCondition hasNextCondition = keySchema.hasNextCondition(null, null, timeFrom, timeTo);
        final PeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator =
            new FilteredCacheIterator(cacheIterator, hasNextCondition, cacheFunction);
        return new MergedSortedCacheWindowStoreKeyValueIterator(
                filteredCacheIterator,
                underlyingIterator,
                bytesSerdes,
                windowSize,
                cacheFunction
        );
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> all() {
        validateStoreOpen();

        final KeyValueIterator<Windowed<Bytes>, byte[]>  underlyingIterator = wrapped().all();
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = cache.all(name);

        return new MergedSortedCacheWindowStoreKeyValueIterator(
            cacheIterator,
            underlyingIterator,
            bytesSerdes,
            windowSize,
            cacheFunction
        );
    }

    @Override
    public synchronized void flush() {
        cache.flush(name);
        wrapped().flush();
    }

    @Override
    public void close() {
        flush();
        cache.close(name);
        wrapped().close();
    }

    private class CacheIteratorWrapper implements PeekingKeyValueIterator<Bytes, LRUCacheEntry> {

        private final long segmentInterval;

        private final Bytes keyFrom;
        private final Bytes keyTo;
        private final long timeTo;
        private long lastSegmentId;

        private long currentSegmentId;
        private Bytes cacheKeyFrom;
        private Bytes cacheKeyTo;

        private ThreadCache.MemoryLRUCacheBytesIterator current;

        private CacheIteratorWrapper(final Bytes key,
                                     final long timeFrom,
                                     final long timeTo) {
            this(key, key, timeFrom, timeTo);
        }

        private CacheIteratorWrapper(final Bytes keyFrom,
                                     final Bytes keyTo,
                                     final long timeFrom,
                                     final long timeTo) {
            this.keyFrom = keyFrom;
            this.keyTo = keyTo;
            this.timeTo = timeTo;
            this.lastSegmentId = cacheFunction.segmentId(Math.min(timeTo, maxObservedTimestamp));

            this.segmentInterval = cacheFunction.getSegmentInterval();
            this.currentSegmentId = cacheFunction.segmentId(timeFrom);

            setCacheKeyRange(timeFrom, currentSegmentLastTime());

            this.current = cache.range(name, cacheKeyFrom, cacheKeyTo);
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
            ++currentSegmentId;
            lastSegmentId = cacheFunction.segmentId(Math.min(timeTo, maxObservedTimestamp));

            if (currentSegmentId > lastSegmentId) {
                current = null;
                return;
            }

            setCacheKeyRange(currentSegmentBeginTime(), currentSegmentLastTime());

            current.close();
            current = cache.range(name, cacheKeyFrom, cacheKeyTo);
        }

        private void setCacheKeyRange(final long lowerRangeEndTime, final long upperRangeEndTime) {
            if (cacheFunction.segmentId(lowerRangeEndTime) != cacheFunction.segmentId(upperRangeEndTime)) {
                throw new IllegalStateException("Error iterating over segments: segment interval has changed");
            }

            if (keyFrom == keyTo) {
                cacheKeyFrom = cacheFunction.cacheKey(segmentLowerRangeFixedSize(keyFrom, lowerRangeEndTime));
                cacheKeyTo = cacheFunction.cacheKey(segmentUpperRangeFixedSize(keyTo, upperRangeEndTime));
            } else {
                cacheKeyFrom = cacheFunction.cacheKey(keySchema.lowerRange(keyFrom, lowerRangeEndTime), currentSegmentId);
                cacheKeyTo = cacheFunction.cacheKey(keySchema.upperRange(keyTo, timeTo), currentSegmentId);
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
