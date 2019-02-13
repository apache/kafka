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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

class CachingWindowStore<K, V> extends WrappedStateStore<WindowStore<Bytes, byte[]>> implements WindowStore<Bytes, byte[]>, CachedStateStore<Windowed<K>, V> {

    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final long windowSize;
    private final SegmentedBytesStore.KeySchema keySchema = new WindowKeySchema();


    private String name;
    private ThreadCache cache;
    private boolean sendOldValues;
    private StateSerdes<K, V> serdes;
    private InternalProcessorContext context;
    private StateSerdes<Bytes, byte[]> bytesSerdes;
    private CacheFlushListener<Windowed<K>, V> flushListener;

    private final SegmentedCacheFunction cacheFunction;

    CachingWindowStore(final WindowStore<Bytes, byte[]> underlying,
                       final Serde<K> keySerde,
                       final Serde<V> valueSerde,
                       final long windowSize,
                       final long segmentInterval) {
        super(underlying);
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.windowSize = windowSize;
        this.cacheFunction = new SegmentedCacheFunction(keySchema, segmentInterval);
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
        serdes = new StateSerdes<>(topic,
                                   keySerde == null ? (Serde<K>) context.keySerde() : keySerde,
                                   valueSerde == null ? (Serde<V>) context.valueSerde() : valueSerde);

        bytesSerdes = new StateSerdes<>(topic,
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
        final Bytes key = windowedKeyBytes.key();
        if (flushListener != null) {
            final byte[] newValueBytes = entry.newValue();
            final byte[] oldValueBytes = newValueBytes == null || sendOldValues ? wrapped().fetch(key, windowStartTimestamp) : null;

            // this is an optimization: if this key did not exist in underlying store and also not in the cache,
            // we can skip flushing to downstream as well as writing to underlying store
            if (newValueBytes != null || oldValueBytes != null) {
                final Windowed<K> windowedKey = WindowKeySchema.fromStoreKey(windowedKeyBytes, serdes.keyDeserializer(), serdes.topic());
                final V newValue = newValueBytes != null ? serdes.valueFrom(newValueBytes) : null;
                final V oldValue = sendOldValues && oldValueBytes != null ? serdes.valueFrom(oldValueBytes) : null;
                // we need to get the old values if needed, and then put to store, and then flush
                wrapped().put(key, entry.newValue(), windowStartTimestamp);

                final ProcessorRecordContext current = context.recordContext();
                context.setRecordContext(entry.entry().context());
                try {
                    flushListener.apply(
                        windowedKey,
                        newValue,
                        oldValue,
                        entry.entry().context().timestamp());
                } finally {
                    context.setRecordContext(current);
                }
            }
        } else {
            wrapped().put(key, entry.newValue(), windowStartTimestamp);
        }
    }

    public void setFlushListener(final CacheFlushListener<Windowed<K>, V> flushListener,
                                 final boolean sendOldValues) {

        this.flushListener = flushListener;
        this.sendOldValues = sendOldValues;
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

    @Override
    public synchronized void put(final Bytes key, final byte[] value) {
        put(key, value, context.timestamp());
    }

    @Override
    public synchronized void put(final Bytes key, final byte[] value, final long windowStartTimestamp) {
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
    }

    @Override
    public byte[] fetch(final Bytes key, final long timestamp) {
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

    @SuppressWarnings("deprecation")
    @Override
    public synchronized WindowStoreIterator<byte[]> fetch(final Bytes key, final long timeFrom, final long timeTo) {
        // since this function may not access the underlying inner store, we need to validate
        // if store is open outside as well.
        validateStoreOpen();

        final WindowStoreIterator<byte[]> underlyingIterator = wrapped().fetch(key, timeFrom, timeTo);
        if (cache == null) {
            return underlyingIterator;
        }
        final Bytes cacheKeyFrom = cacheFunction.cacheKey(keySchema.lowerRangeFixedSize(key, timeFrom));
        final Bytes cacheKeyTo = cacheFunction.cacheKey(keySchema.upperRangeFixedSize(key, timeTo));
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = cache.range(name, cacheKeyFrom, cacheKeyTo);

        final HasNextCondition hasNextCondition = keySchema.hasNextCondition(key,
                                                                             key,
                                                                             timeFrom,
                                                                             timeTo);
        final PeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator = new FilteredCacheIterator(
            cacheIterator, hasNextCondition, cacheFunction
        );

        return new MergedSortedCacheWindowStoreIterator(filteredCacheIterator, underlyingIterator);
    }

    @SuppressWarnings("deprecation")
    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes from, final Bytes to, final long timeFrom, final long timeTo) {
        // since this function may not access the underlying inner store, we need to validate
        // if store is open outside as well.
        validateStoreOpen();

        final KeyValueIterator<Windowed<Bytes>, byte[]> underlyingIterator = wrapped().fetch(from, to, timeFrom, timeTo);
        if (cache == null) {
            return underlyingIterator;
        }
        final Bytes cacheKeyFrom = cacheFunction.cacheKey(keySchema.lowerRange(from, timeFrom));
        final Bytes cacheKeyTo = cacheFunction.cacheKey(keySchema.upperRange(to, timeTo));
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = cache.range(name, cacheKeyFrom, cacheKeyTo);

        final HasNextCondition hasNextCondition = keySchema.hasNextCondition(from,
                                                                             to,
                                                                             timeFrom,
                                                                             timeTo);
        final PeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator = new FilteredCacheIterator(cacheIterator, hasNextCondition, cacheFunction);

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

    @SuppressWarnings("deprecation")
    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(final long timeFrom, final long timeTo) {
        validateStoreOpen();

        final KeyValueIterator<Windowed<Bytes>, byte[]> underlyingIterator = wrapped().fetchAll(timeFrom, timeTo);
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = cache.all(name);

        final HasNextCondition hasNextCondition = keySchema.hasNextCondition(null, null, timeFrom, timeTo);
        final PeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator = new FilteredCacheIterator(cacheIterator,
                                                                                                              hasNextCondition,
                                                                                                              cacheFunction);
        return new MergedSortedCacheWindowStoreKeyValueIterator(
                filteredCacheIterator,
                underlyingIterator,
                bytesSerdes,
                windowSize,
                cacheFunction
        );
    }
}
