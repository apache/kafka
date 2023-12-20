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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.RecordQueue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.state.internals.MergedSortedCacheWindowStoreKeyValueIterator.StoreKeyToWindowKey;
import org.apache.kafka.streams.state.internals.MergedSortedCacheWindowStoreKeyValueIterator.WindowKeyToBytes;
import org.apache.kafka.streams.state.internals.PrefixedWindowKeySchemas.KeyFirstWindowKeySchema;
import org.apache.kafka.streams.state.internals.PrefixedWindowKeySchemas.TimeFirstWindowKeySchema;
import org.apache.kafka.streams.state.internals.SegmentedBytesStore.KeySchema;
import org.apache.kafka.streams.state.internals.ThreadCache.DirtyEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.kafka.streams.processor.internals.ProcessorContextUtils.asInternalProcessorContext;
import static org.apache.kafka.streams.state.internals.ExceptionUtils.executeAll;
import static org.apache.kafka.streams.state.internals.ExceptionUtils.throwSuppressed;

class TimeOrderedCachingWindowStore
    extends WrappedStateStore<WindowStore<Bytes, byte[]>, byte[], byte[]>
    implements WindowStore<Bytes, byte[]>, CachedStateStore<byte[], byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(TimeOrderedCachingWindowStore.class);

    private final long windowSize;
    private final SegmentedCacheFunction baseKeyCacheFunction;
    private final SegmentedCacheFunction indexKeyCacheFunction;
    private final TimeFirstWindowKeySchema baseKeySchema = new TimeFirstWindowKeySchema();
    private final KeyFirstWindowKeySchema indexKeySchema = new KeyFirstWindowKeySchema();

    private String cacheName;
    private boolean hasIndex;
    private boolean sendOldValues;
    private InternalProcessorContext<?, ?> context;
    private StateSerdes<Bytes, byte[]> bytesSerdes;
    private CacheFlushListener<byte[], byte[]> flushListener;

    private final AtomicLong maxObservedTimestamp;

    TimeOrderedCachingWindowStore(final WindowStore<Bytes, byte[]> underlying,
                                  final long windowSize,
                                  final long segmentInterval) {
        super(underlying);
        this.windowSize = windowSize;
        this.baseKeyCacheFunction = new SegmentedCacheFunction(baseKeySchema, segmentInterval);
        this.indexKeyCacheFunction = new SegmentedCacheFunction(indexKeySchema, segmentInterval);
        this.maxObservedTimestamp = new AtomicLong(RecordQueue.UNKNOWN);
        enforceWrappedStore(underlying);
    }

    private void enforceWrappedStore(final WindowStore<Bytes, byte[]> underlying) {
        final RocksDBTimeOrderedWindowStore timeOrderedWindowStore = getWrappedStore(underlying);
        if (timeOrderedWindowStore == null) {
            throw new IllegalArgumentException("TimeOrderedCachingWindowStore only supports RocksDBTimeOrderedWindowStore backed store");
        }

        hasIndex = timeOrderedWindowStore.hasIndex();
    }

    private RocksDBTimeOrderedWindowStore getWrappedStore(final StateStore wrapped) {
        if (wrapped instanceof RocksDBTimeOrderedWindowStore) {
            return (RocksDBTimeOrderedWindowStore) wrapped;
        }
        if (wrapped instanceof WrappedStateStore) {
            return getWrappedStore(((WrappedStateStore) wrapped).wrapped());
        }
        return null;
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
        final String prefix = StreamsConfig.InternalConfig.getString(
            context.appConfigs(),
            StreamsConfig.InternalConfig.TOPIC_PREFIX_ALTERNATIVE,
            context.applicationId()
        );
        this.context = context;
        final String topic = ProcessorStateManager.storeChangelogTopic(prefix, name(),  context.taskId().topologyName());

        bytesSerdes = new StateSerdes<>(
            topic,
            Serdes.Bytes(),
            Serdes.ByteArray());
        cacheName = context.taskId() + "-" + name();

        context.registerCacheFlushListener(cacheName, entries -> {
            putAndMaybeForward(entries, context);
        });
    }

    private void putAndMaybeForward(final List<DirtyEntry> entries,
                                    final InternalProcessorContext<?, ?> context) {

        // Track what base key or index key we already processed so don't reprocess
        final Set<Bytes> processedBasedKey = new HashSet<>();

        for (final ThreadCache.DirtyEntry entry : entries) {
            final byte[] binaryWindowKey = baseKeyCacheFunction.key(entry.key()).get();
            final boolean isBaseKey = PrefixedWindowKeySchemas.isTimeFirstSchemaKey(
                binaryWindowKey);

            final DirtyEntry finalEntry;
            if (!isBaseKey) {
                final Bytes baseKey = indexKeyToBaseKey(Bytes.wrap(binaryWindowKey));
                if (hasIndex && processedBasedKey.contains(baseKey)) {
                    // Processed in base
                    continue;
                }

                final Bytes cachedBaseKey = baseKeyCacheFunction.cacheKey(baseKey);
                final LRUCacheEntry value = context.cache().get(cacheName, cachedBaseKey);
                // Base key value is already evicted, which should be handled already
                if (value == null) {
                    continue;
                }

                finalEntry = new DirtyEntry(entry.key(), value.value(), value);

                if (hasIndex) {
                    processedBasedKey.add(baseKey);
                }
            } else {
                final Bytes baseKey = Bytes.wrap(binaryWindowKey);
                if (hasIndex && processedBasedKey.contains(baseKey)) {
                    // Processed in index
                    continue;
                }
                finalEntry = entry;
                if (hasIndex) {
                    processedBasedKey.add(Bytes.wrap(binaryWindowKey));
                }
            }

            final Windowed<Bytes> windowedKeyBytes;
            if (isBaseKey) {
                windowedKeyBytes = TimeFirstWindowKeySchema.fromStoreBytesKey(binaryWindowKey,
                    windowSize);
            } else {
                windowedKeyBytes = KeyFirstWindowKeySchema.fromStoreBytesKey(binaryWindowKey,
                    windowSize);
            }

            final long windowStartTimestamp = windowedKeyBytes.window().start();
            final Bytes binaryKey = windowedKeyBytes.key();

            putAndMaybeForward(context, finalEntry, binaryKey, windowStartTimestamp);
        }
    }

    private void putAndMaybeForward(final InternalProcessorContext<?, ?> context,
                                    final DirtyEntry finalEntry,
                                    final Bytes binaryKey,
                                    final long windowStartTimestamp) {
        if (flushListener != null) {
            final byte[] rawNewValue = finalEntry.newValue();
            final byte[] rawOldValue = rawNewValue == null || sendOldValues ?
                wrapped().fetch(binaryKey, windowStartTimestamp) : null;

            // this is an optimization: if this key did not exist in underlying store and also not in the cache,
            // we can skip flushing to downstream as well as writing to underlying store
            if (rawNewValue != null || rawOldValue != null) {
                // we need to get the old values if needed, and then put to store, and then flush
                final ProcessorRecordContext current = context.recordContext();
                try {
                    context.setRecordContext(finalEntry.entry().context());
                    wrapped().put(binaryKey, finalEntry.newValue(), windowStartTimestamp);

                    flushListener.apply(
                        new Record<>(
                            WindowKeySchema.toStoreKeyBinary(binaryKey,
                                    windowStartTimestamp, 0)
                                .get(),
                            new Change<>(rawNewValue, sendOldValues ? rawOldValue : null),
                            finalEntry.entry().context().timestamp(),
                            finalEntry.entry().context().headers()));
                } finally {
                    context.setRecordContext(current);
                }
            }
        } else {
            final ProcessorRecordContext current = context.recordContext();
            try {
                context.setRecordContext(finalEntry.entry().context());
                wrapped().put(binaryKey, finalEntry.newValue(), windowStartTimestamp);
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

    private Bytes indexKeyToBaseKey(final Bytes indexKey) {
        final byte[] key = KeyFirstWindowKeySchema.extractStoreKeyBytes(indexKey.get());
        final long timestamp = KeyFirstWindowKeySchema.extractStoreTimestamp(indexKey.get());
        final int seqnum = KeyFirstWindowKeySchema.extractStoreSequence(indexKey.get());
        return TimeFirstWindowKeySchema.toStoreKeyBinary(key, timestamp, seqnum);
    }

    @Override
    public synchronized void put(final Bytes key,
                                 final byte[] value,
                                 final long windowStartTimestamp) {
        // since this function may not access the underlying inner store, we need to validate
        // if store is open outside as well.
        validateStoreOpen();

        final Bytes baseKeyBytes = TimeFirstWindowKeySchema.toStoreKeyBinary(key, windowStartTimestamp, 0);
        final LRUCacheEntry entry =
            new LRUCacheEntry(
                value,
                context.headers(),
                true,
                context.offset(),
                context.timestamp(),
                context.partition(),
                context.topic());

        // Put to index first so that base can be evicted later
        if (hasIndex) {
            // Important: put base key first to avoid the situation that if we put index first,
            // it could be evicted when we are putting base key. In that case, base key is not yet
            // in cache so we can't store key/value to store when index is evicted. Then if we fetch
            // using index, we can't find it in either store or cache
            context.cache().put(cacheName, baseKeyCacheFunction.cacheKey(baseKeyBytes), entry);
            final LRUCacheEntry emptyEntry =
                new LRUCacheEntry(
                    new byte[0],
                    new RecordHeaders(),
                    true,
                    context.offset(),
                    context.timestamp(),
                    context.partition(),
                    "");
            final Bytes indexKey = KeyFirstWindowKeySchema.toStoreKeyBinary(key, windowStartTimestamp, 0);
            context.cache().put(cacheName, indexKeyCacheFunction.cacheKey(indexKey), emptyEntry);
        } else {
            context.cache().put(cacheName, baseKeyCacheFunction.cacheKey(baseKeyBytes), entry);
        }
        maxObservedTimestamp.set(Math.max(windowStartTimestamp, maxObservedTimestamp.get()));
    }

    @Override
    public byte[] fetch(final Bytes key,
                        final long timestamp) {
        validateStoreOpen();
        if (context.cache() == null) {
            return wrapped().fetch(key, timestamp);
        }

        final Bytes baseBytesKey = TimeFirstWindowKeySchema.toStoreKeyBinary(key, timestamp, 0);
        final Bytes cacheKey = baseKeyCacheFunction.cacheKey(baseBytesKey);

        final LRUCacheEntry entry = context.cache().get(cacheName, cacheKey);
        if (entry == null) {
            return wrapped().fetch(key, timestamp);
        } else {
            return entry.value();
        }
    }

    @Override
    public synchronized WindowStoreIterator<byte[]> fetch(final Bytes key,
                                                          final long timeFrom,
                                                          final long timeTo) {
        // since this function may not access the underlying inner store, we need to validate
        // if store is open outside as well.
        validateStoreOpen();

        final WindowStoreIterator<byte[]> underlyingIterator = wrapped().fetch(key, timeFrom, timeTo);
        if (context.cache() == null) {
            return underlyingIterator;
        }

        return fetchInternal(underlyingIterator, key, timeFrom, timeTo, true);
    }

    @Override
    public synchronized WindowStoreIterator<byte[]> backwardFetch(final Bytes key,
                                                                  final long timeFrom,
                                                                  final long timeTo) {
        // since this function may not access the underlying inner store, we need to validate
        // if store is open outside as well.
        validateStoreOpen();

        final WindowStoreIterator<byte[]> underlyingIterator = wrapped().backwardFetch(key, timeFrom, timeTo);
        if (context.cache() == null) {
            return underlyingIterator;
        }

        return fetchInternal(underlyingIterator, key, timeFrom, timeTo, false);
    }

    private WindowStoreIterator<byte[]> fetchInternal(final WindowStoreIterator<byte[]> underlyingIterator,
                                                      final Bytes key,
                                                      final long timeFrom,
                                                      final long timeTo,
                                                      final boolean forward) {
        final PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator = new CacheIteratorWrapper(
            key, timeFrom, timeTo, forward, hasIndex);
        final KeySchema keySchema = hasIndex ? indexKeySchema : baseKeySchema;
        final SegmentedCacheFunction cacheFunction = hasIndex ? indexKeyCacheFunction : baseKeyCacheFunction;
        final HasNextCondition hasNextCondition = keySchema.hasNextCondition(key, key, timeFrom, timeTo, forward);

        final PeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator =
            new FilteredCacheIterator(cacheIterator, hasNextCondition, cacheFunction);

        final Function<byte[], Long> tsExtractor = hasIndex ? KeyFirstWindowKeySchema::extractStoreTimestamp
            : TimeFirstWindowKeySchema::extractStoreTimestamp;
        return new MergedSortedCacheWindowStoreIterator(filteredCacheIterator, underlyingIterator, forward, tsExtractor);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes keyFrom,
                                                           final Bytes keyTo,
                                                           final long timeFrom,
                                                           final long timeTo) {
        if (keyFrom != null && keyTo != null && keyFrom.compareTo(keyTo) > 0) {
            LOG.warn("Returning empty iterator for fetch with invalid key range: from > to. " +
                "This may be due to range arguments set in the wrong order, " +
                "or serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                "Note that the built-in numerical serdes do not follow this for negative numbers");
            return KeyValueIterators.emptyIterator();
        }

        // since this function may not access the underlying inner store, we need to validate
        // if store is open outside as well.
        validateStoreOpen();

        final KeyValueIterator<Windowed<Bytes>, byte[]> underlyingIterator =
            wrapped().fetch(keyFrom, keyTo, timeFrom, timeTo);
        if (context.cache() == null) {
            return underlyingIterator;
        }

        return fetchKeyRange(underlyingIterator, keyFrom, keyTo, timeFrom, timeTo, true);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(final Bytes keyFrom,
                                                                   final Bytes keyTo,
                                                                   final long timeFrom,
                                                                   final long timeTo) {
        if (keyFrom != null && keyTo != null && keyFrom.compareTo(keyTo) > 0) {
            LOG.warn("Returning empty iterator for fetch with invalid key range: from > to. "
                + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                "Note that the built-in numerical serdes do not follow this for negative numbers");
            return KeyValueIterators.emptyIterator();
        }

        // since this function may not access the underlying inner store, we need to validate
        // if store is open outside as well.
        validateStoreOpen();

        final KeyValueIterator<Windowed<Bytes>, byte[]> underlyingIterator =
            wrapped().backwardFetch(keyFrom, keyTo, timeFrom, timeTo);
        if (context.cache() == null) {
            return underlyingIterator;
        }

        return fetchKeyRange(underlyingIterator, keyFrom, keyTo, timeFrom, timeTo, false);
    }

    private KeyValueIterator<Windowed<Bytes>, byte[]> fetchKeyRange(final KeyValueIterator<Windowed<Bytes>, byte[]> underlyingIterator,
                                                                    final Bytes keyFrom,
                                                                    final Bytes keyTo,
                                                                    final long timeFrom,
                                                                    final long timeTo,
                                                                    final boolean forward) {
        final PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator = new CacheIteratorWrapper(
            keyFrom, keyTo, timeFrom, timeTo, forward, hasIndex);

        final KeySchema keySchema = hasIndex ? indexKeySchema : baseKeySchema;
        final HasNextCondition hasNextCondition = keySchema.hasNextCondition(keyFrom, keyTo, timeFrom, timeTo, forward);
        final SegmentedCacheFunction cacheFunction = hasIndex ? indexKeyCacheFunction : baseKeyCacheFunction;

        final PeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator =
            new FilteredCacheIterator(cacheIterator, hasNextCondition, cacheFunction);
        final StoreKeyToWindowKey storeKeyToWindowKey = hasIndex ? KeyFirstWindowKeySchema::fromStoreKey : TimeFirstWindowKeySchema::fromStoreKey;
        final WindowKeyToBytes windowKeyToBytes = hasIndex ? KeyFirstWindowKeySchema::toStoreKeyBinary : TimeFirstWindowKeySchema::toStoreKeyBinary;

        return new MergedSortedCacheWindowStoreKeyValueIterator(
            filteredCacheIterator,
            underlyingIterator,
            bytesSerdes,
            windowSize,
            cacheFunction,
            forward,
            storeKeyToWindowKey,
            windowKeyToBytes
        );
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(final long timeFrom,
                                                              final long timeTo) {
        validateStoreOpen();

        final KeyValueIterator<Windowed<Bytes>, byte[]> underlyingIterator = wrapped().fetchAll(timeFrom, timeTo);
        return fetchAllInternal(underlyingIterator, timeFrom, timeTo, true);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetchAll(final long timeFrom,
                                                                      final long timeTo) {
        validateStoreOpen();

        final KeyValueIterator<Windowed<Bytes>, byte[]> underlyingIterator = wrapped().backwardFetchAll(timeFrom, timeTo);
        return fetchAllInternal(underlyingIterator, timeFrom, timeTo, false);
    }

    private KeyValueIterator<Windowed<Bytes>, byte[]> fetchAllInternal(final KeyValueIterator<Windowed<Bytes>, byte[]> underlyingIterator,
                                                                       final long timeFrom,
                                                                       final long timeTo,
                                                                       final boolean forward) {
        final PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator = new CacheIteratorWrapper(
            null, null, timeFrom, timeTo, forward, false);
        final HasNextCondition hasNextCondition = baseKeySchema.hasNextCondition(null, null, timeFrom, timeTo, forward);
        final PeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator =
            new FilteredCacheIterator(cacheIterator, hasNextCondition, baseKeyCacheFunction);

        final StoreKeyToWindowKey storeKeyToWindowKey = TimeFirstWindowKeySchema::fromStoreKey;
        final WindowKeyToBytes windowKeyToBytes = TimeFirstWindowKeySchema::toStoreKeyBinary;

        return new MergedSortedCacheWindowStoreKeyValueIterator(
            filteredCacheIterator,
            underlyingIterator,
            bytesSerdes,
            windowSize,
            baseKeyCacheFunction,
            forward,
            storeKeyToWindowKey,
            windowKeyToBytes
        );
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> all() {
        validateStoreOpen();

        final KeyValueIterator<Windowed<Bytes>, byte[]> underlyingIterator = wrapped().all();
        return fetchAllInternal(underlyingIterator, 0, Long.MAX_VALUE, true);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardAll() {
        validateStoreOpen();

        final KeyValueIterator<Windowed<Bytes>, byte[]> underlyingIterator = wrapped().backwardAll();
        return fetchAllInternal(underlyingIterator, 0, Long.MAX_VALUE, false);
    }

    @Override
    public synchronized void flush() {
        context.cache().flush(cacheName);
        wrapped().flush();
    }

    @Override
    public void flushCache() {
        context.cache().flush(cacheName);
    }

    @Override
    public void clearCache() {
        context.cache().clear(cacheName);
    }

    @Override
    public synchronized void close() {
        final LinkedList<RuntimeException> suppressed = executeAll(
            () -> context.cache().flush(cacheName),
            () -> context.cache().close(cacheName),
            wrapped()::close
        );
        if (!suppressed.isEmpty()) {
            throwSuppressed("Caught an exception while closing caching window store for store " + name(),
                suppressed);
        }
    }


    private class CacheIteratorWrapper implements PeekingKeyValueIterator<Bytes, LRUCacheEntry> {

        private final long segmentInterval;
        private final Bytes keyFrom;
        private final Bytes keyTo;
        private final long timeTo;
        private final boolean forward;
        private final boolean useIndex; // If we are iterating from index

        private long lastSegmentId;
        private long currentSegmentId;
        private Bytes cacheKeyFrom;
        private Bytes cacheKeyTo;
        private LRUCacheEntry cachedBaseValue;
        private final SegmentedCacheFunction cacheFunction;

        private ThreadCache.MemoryLRUCacheBytesIterator current;

        private CacheIteratorWrapper(final Bytes key,
                                     final long timeFrom,
                                     final long timeTo,
                                     final boolean forward,
                                     final boolean index) {
            this(key, key, timeFrom, timeTo, forward, index);
        }

        private CacheIteratorWrapper(final Bytes keyFrom,
                                     final Bytes keyTo,
                                     final long timeFrom,
                                     final long timeTo,
                                     final boolean forward,
                                     final boolean index) {
            this.keyFrom = keyFrom;
            this.keyTo = keyTo;
            this.timeTo = timeTo;
            this.forward = forward;
            this.useIndex = index;

            cacheFunction = index ? indexKeyCacheFunction : baseKeyCacheFunction;

            this.segmentInterval = cacheFunction.getSegmentInterval();

            if (forward) {
                this.lastSegmentId = cacheFunction.segmentId(Math.min(timeTo, maxObservedTimestamp.get()));
                this.currentSegmentId = cacheFunction.segmentId(timeFrom);

                setCacheKeyRange(timeFrom, currentSegmentLastTime());
                this.current = context.cache().range(cacheName, cacheKeyFrom, cacheKeyTo);
            } else {
                this.currentSegmentId = cacheFunction.segmentId(Math.min(timeTo, maxObservedTimestamp.get()));
                this.lastSegmentId = cacheFunction.segmentId(timeFrom);

                setCacheKeyRange(currentSegmentBeginTime(), Math.min(timeTo, maxObservedTimestamp.get()));
                this.current = context.cache().reverseRange(cacheName, cacheKeyFrom, cacheKeyTo);
            }
        }

        @Override
        public boolean hasNext() {
            if (current == null) {
                return false;
            }

            if (useIndex) {
                do {
                    // If iterating from index, need to make sure base key/value exist in cache
                    while (current.hasNext()) {
                        final Bytes cacheIndexKey = current.peekNextKey();
                        final Bytes indexKey = indexKeyCacheFunction.key(cacheIndexKey);
                        final Bytes baseKey = indexKeyToBaseKey(indexKey);
                        final Bytes cachedBaseKey = baseKeyCacheFunction.cacheKey(baseKey);
                        cachedBaseValue = context.cache().get(cacheName, cachedBaseKey);
                        if (cachedBaseValue != null) {
                            return true;
                        }
                        current.next();
                    }
                    getNextSegmentIterator();
                } while (current != null);
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
            if (useIndex) {
                final KeyValue<Bytes, LRUCacheEntry> kv = current.peekNext();
                return KeyValue.pair(kv.key, cachedBaseValue);
            }
            return current.peekNext();
        }

        @Override
        public KeyValue<Bytes, LRUCacheEntry> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            if (useIndex) {
                final KeyValue<Bytes, LRUCacheEntry> kv = current.next();
                return KeyValue.pair(kv.key, cachedBaseValue);
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

                current = context.cache().range(cacheName, cacheKeyFrom, cacheKeyTo);
            } else {
                --currentSegmentId;

                // last segment id is stable when iterating backward, therefore no need to update
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

            final KeySchema schema = useIndex ? indexKeySchema : baseKeySchema;

            if (keyFrom != null && keyFrom.equals(keyTo)) {
                cacheKeyFrom = cacheFunction.cacheKey(schema.lowerRangeFixedSize(keyFrom, lowerRangeEndTime), currentSegmentId);
                cacheKeyTo = cacheFunction.cacheKey(schema.upperRangeFixedSize(keyTo, upperRangeEndTime), currentSegmentId);
            } else {
                cacheKeyFrom = cacheFunction.cacheKey(schema.lowerRange(keyFrom, lowerRangeEndTime), currentSegmentId);
                cacheKeyTo = cacheFunction.cacheKey(schema.upperRange(keyTo, timeTo), currentSegmentId);
            }
        }
    }
}
