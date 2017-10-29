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
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.CacheFlushListener;
import org.apache.kafka.streams.kstream.internals.SessionKeySerde;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.RecordContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StateSerdes;

import java.util.List;
import java.util.Objects;


class CachingSessionStore<K, AGG> extends WrappedStateStore.AbstractStateStore implements SessionStore<Bytes, byte[]>, CachedStateStore<Windowed<K>, AGG> {

    private final SessionStore<Bytes, byte[]> bytesStore;
    private final SessionKeySchema keySchema;
    private final Serde<K> keySerde;
    private final Serde<AGG> aggSerde;
    private final SegmentedCacheFunction cacheFunction;
    private String cacheName;
    private ThreadCache cache;
    private StateSerdes<K, AGG> serdes;
    private InternalProcessorContext context;
    private CacheFlushListener<Windowed<K>, AGG> flushListener;
    private boolean sendOldValues;
    private String topic;

    CachingSessionStore(final SessionStore<Bytes, byte[]> bytesStore,
                        final Serde<K> keySerde,
                        final Serde<AGG> aggSerde,
                        final long segmentInterval) {
        super(bytesStore);
        this.bytesStore = bytesStore;
        this.keySerde = keySerde;
        this.aggSerde = aggSerde;
        this.keySchema = new SessionKeySchema();
        this.cacheFunction = new SegmentedCacheFunction(keySchema, segmentInterval);
    }

    public void init(final ProcessorContext context, final StateStore root) {
        topic = ProcessorStateManager.storeChangelogTopic(context.applicationId(), root.name());
        bytesStore.init(context, root);
        initInternal((InternalProcessorContext) context);
    }

    @SuppressWarnings("unchecked")
    private void initInternal(final InternalProcessorContext context) {
        this.context = context;

        keySchema.init(topic);
        serdes = new StateSerdes<>(
            topic,
            keySerde == null ? (Serde<K>) context.keySerde() : keySerde,
            aggSerde == null ? (Serde<AGG>) context.valueSerde() : aggSerde);


        cacheName = context.taskId() + "-" + bytesStore.name();
        cache = context.getCache();
        cache.addDirtyEntryFlushListener(cacheName, new ThreadCache.DirtyEntryFlushListener() {
            @Override
            public void apply(final List<ThreadCache.DirtyEntry> entries) {
                for (ThreadCache.DirtyEntry entry : entries) {
                    putAndMaybeForward(entry, context);
                }
            }
        });
    }

    public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(final Bytes key,
                                                                  final long earliestSessionEndTime,
                                                                  final long latestSessionStartTime) {
        validateStoreOpen();
        final Bytes cacheKeyFrom = cacheFunction.cacheKey(keySchema.lowerRangeFixedSize(key, earliestSessionEndTime));
        final Bytes cacheKeyTo = cacheFunction.cacheKey(keySchema.upperRangeFixedSize(key, latestSessionStartTime));
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = cache.range(cacheName, cacheKeyFrom, cacheKeyTo);

        final KeyValueIterator<Windowed<Bytes>, byte[]> storeIterator = bytesStore.findSessions(key,
                                                                                                earliestSessionEndTime,
                                                                                                latestSessionStartTime);
        final HasNextCondition hasNextCondition = keySchema.hasNextCondition(key,
                                                                             key,
                                                                             earliestSessionEndTime,
                                                                             latestSessionStartTime);
        final PeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator = new FilteredCacheIterator(cacheIterator, hasNextCondition, cacheFunction);
        return new MergedSortedCacheSessionStoreIterator(filteredCacheIterator, storeIterator, cacheFunction);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(final Bytes keyFrom,
                                                                  final Bytes keyTo,
                                                                  final long earliestSessionEndTime,
                                                                  final long latestSessionStartTime) {
        validateStoreOpen();

        final Bytes cacheKeyFrom = cacheFunction.cacheKey(keySchema.lowerRange(keyFrom, earliestSessionEndTime));
        final Bytes cacheKeyTo = cacheFunction.cacheKey(keySchema.upperRange(keyTo, latestSessionStartTime));
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = cache.range(cacheName, cacheKeyFrom, cacheKeyTo);

        final KeyValueIterator<Windowed<Bytes>, byte[]> storeIterator = bytesStore.findSessions(
            keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime
        );
        final HasNextCondition hasNextCondition = keySchema.hasNextCondition(keyFrom,
                                                                             keyTo,
                                                                             earliestSessionEndTime,
                                                                             latestSessionStartTime);
        final PeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator = new FilteredCacheIterator(cacheIterator, hasNextCondition, cacheFunction);
        return new MergedSortedCacheSessionStoreIterator(filteredCacheIterator, storeIterator, cacheFunction);
    }

    @Override
    public void remove(final Windowed<Bytes> sessionKey) {
        validateStoreOpen();
        put(sessionKey, null);
    }

    @Override
    public void put(final Windowed<Bytes> key, byte[] value) {
        validateStoreOpen();
        final Bytes binaryKey = SessionKeySerde.bytesToBinary(key);
        final LRUCacheEntry entry = new LRUCacheEntry(value, true, context.offset(),
                                                      key.window().end(), context.partition(), context.topic());
        cache.put(cacheName, cacheFunction.cacheKey(binaryKey), entry);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes key) {
        Objects.requireNonNull(key, "key cannot be null");
        return findSessions(key, 0, Long.MAX_VALUE);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes from, final Bytes to) {
        Objects.requireNonNull(from, "from cannot be null");
        Objects.requireNonNull(to, "to cannot be null");
        return findSessions(from, to, 0, Long.MAX_VALUE);
    }



    private void putAndMaybeForward(final ThreadCache.DirtyEntry entry, final InternalProcessorContext context) {
        final Bytes binaryKey = cacheFunction.key(entry.key());
        final RecordContext current = context.recordContext();
        context.setRecordContext(entry.recordContext());
        try {
            final Windowed<K> key = SessionKeySerde.from(binaryKey.get(), serdes.keyDeserializer(), topic);
            final Bytes rawKey = Bytes.wrap(serdes.rawKey(key.key()));
            if (flushListener != null) {
                final AGG newValue = serdes.valueFrom(entry.newValue());
                final AGG oldValue = newValue == null || sendOldValues ? fetchPrevious(rawKey, key.window()) : null;
                if (!(newValue == null && oldValue == null)) {
                    flushListener.apply(key, newValue, oldValue);
                }
            }
            bytesStore.put(new Windowed<>(rawKey, key.window()), entry.newValue());
        } finally {
            context.setRecordContext(current);
        }
    }

    private AGG fetchPrevious(final Bytes rawKey, final Window window) {
        try (final KeyValueIterator<Windowed<Bytes>, byte[]> iterator = bytesStore
                .findSessions(rawKey, window.start(), window.end())) {
            if (!iterator.hasNext()) {
                return null;
            }
            return serdes.valueFrom(iterator.next().value);
        }
    }

    public void flush() {
        cache.flush(cacheName);
        bytesStore.flush();
    }

    public void close() {
        flush();
        cache.close(cacheName);
        bytesStore.close();
    }

    public void setFlushListener(final CacheFlushListener<Windowed<K>, AGG> flushListener,
                                 final boolean sendOldValues) {
        this.flushListener = flushListener;
        this.sendOldValues = sendOldValues;
    }

}
