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
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StateSerdes;

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
        initInternal((InternalProcessorContext) context);
        bytesStore.init(context, root);
    }

    @SuppressWarnings("unchecked")
    private void initInternal(final InternalProcessorContext context) {
        this.context = context;

        serdes = new StateSerdes<>(
            topic,
            keySerde == null ? (Serde<K>) context.keySerde() : keySerde,
            aggSerde == null ? (Serde<AGG>) context.valueSerde() : aggSerde);


        cacheName = context.taskId() + "-" + bytesStore.name();
        cache = context.getCache();
        cache.addDirtyEntryFlushListener(cacheName, entries -> {
            for (final ThreadCache.DirtyEntry entry : entries) {
                putAndMaybeForward(entry, context);
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
        cache.put(cacheName, cacheFunction.cacheKey(binaryKey), entry);
    }

    @Override
    public byte[] fetchSession(final Bytes key, final long startTime, final long endTime) {
        Objects.requireNonNull(key, "key cannot be null");
        validateStoreOpen();
        if (cache == null) {
            return bytesStore.fetchSession(key, startTime, endTime);
        } else {
            final Bytes bytesKey = SessionKeySchema.toBinary(key, startTime, endTime);
            final Bytes cacheKey = cacheFunction.cacheKey(bytesKey);
            final LRUCacheEntry entry = cache.get(cacheName, cacheKey);
            if (entry == null) {
                return bytesStore.fetchSession(key, startTime, endTime);
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
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes from, final Bytes to) {
        Objects.requireNonNull(from, "from cannot be null");
        Objects.requireNonNull(to, "to cannot be null");
        return findSessions(from, to, 0, Long.MAX_VALUE);
    }

    private void putAndMaybeForward(final ThreadCache.DirtyEntry entry, final InternalProcessorContext context) {
        final Bytes binaryKey = cacheFunction.key(entry.key());
        final ProcessorRecordContext current = context.recordContext();
        context.setRecordContext(entry.entry().context());
        try {
            final Windowed<K> key = SessionKeySchema.from(binaryKey.get(), serdes.keyDeserializer(), topic);
            final Bytes rawKey = Bytes.wrap(serdes.rawKey(key.key()));
            if (flushListener != null) {
                final AGG newValue = serdes.valueFrom(entry.newValue());
                final AGG oldValue = newValue == null || sendOldValues ?
                    serdes.valueFrom(bytesStore.fetchSession(rawKey, key.window().start(), key.window().end())) :
                    null;
                if (!(newValue == null && oldValue == null)) {
                    flushListener.apply(
                        key,
                        newValue,
                        oldValue,
                        entry.entry().context().timestamp());
                }
            }
            bytesStore.put(new Windowed<>(rawKey, key.window()), entry.newValue());
        } finally {
            context.setRecordContext(current);
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
