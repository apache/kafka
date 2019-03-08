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
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;

import java.util.Objects;

class CachingSessionStore
    extends WrappedStateStore<SessionStore<Bytes, byte[]>, byte[], byte[]>
    implements SessionStore<Bytes, byte[]>, CachedStateStore<byte[], byte[]> {

    private final SessionKeySchema keySchema;
    private final SegmentedCacheFunction cacheFunction;
    private String cacheName;
    private ThreadCache cache;
    private InternalProcessorContext context;
    private CacheFlushListener<byte[], byte[]> flushListener;
    private boolean sendOldValues;

    CachingSessionStore(final SessionStore<Bytes, byte[]> bytesStore,
                        final long segmentInterval) {
        super(bytesStore);
        this.keySchema = new SessionKeySchema();
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

        cacheName = context.taskId() + "-" + name();
        cache = context.getCache();
        cache.addDirtyEntryFlushListener(cacheName, entries -> {
            for (final ThreadCache.DirtyEntry entry : entries) {
                putAndMaybeForward(entry, context);
            }
        });
    }

    private void putAndMaybeForward(final ThreadCache.DirtyEntry entry, final InternalProcessorContext context) {
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
                wrapped().put(bytesKey, entry.newValue());

                final ProcessorRecordContext current = context.recordContext();
                context.setRecordContext(entry.entry().context());
                try {
                    flushListener.apply(
                        binaryKey.get(),
                        newValueBytes,
                        sendOldValues ? oldValueBytes : null,
                        entry.entry().context().timestamp());
                } finally {
                    context.setRecordContext(current);
                }
            }
        } else {
            wrapped().put(bytesKey, entry.newValue());
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
        cache.put(cacheName, cacheFunction.cacheKey(binaryKey), entry);
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
        final Bytes cacheKeyFrom = cacheFunction.cacheKey(keySchema.lowerRangeFixedSize(key, earliestSessionEndTime));
        final Bytes cacheKeyTo = cacheFunction.cacheKey(keySchema.upperRangeFixedSize(key, latestSessionStartTime));
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = cache.range(cacheName, cacheKeyFrom, cacheKeyTo);

        final KeyValueIterator<Windowed<Bytes>, byte[]> storeIterator = wrapped().findSessions(key,
                                                                                               earliestSessionEndTime,
                                                                                               latestSessionStartTime);
        final HasNextCondition hasNextCondition = keySchema.hasNextCondition(key,
                                                                             key,
                                                                             earliestSessionEndTime,
                                                                             latestSessionStartTime);
        final PeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator =
            new FilteredCacheIterator(cacheIterator, hasNextCondition, cacheFunction);
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

        final KeyValueIterator<Windowed<Bytes>, byte[]> storeIterator = wrapped().findSessions(
            keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime
        );
        final HasNextCondition hasNextCondition = keySchema.hasNextCondition(keyFrom,
                                                                             keyTo,
                                                                             earliestSessionEndTime,
                                                                             latestSessionStartTime);
        final PeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator =
            new FilteredCacheIterator(cacheIterator, hasNextCondition, cacheFunction);
        return new MergedSortedCacheSessionStoreIterator(filteredCacheIterator, storeIterator, cacheFunction);
    }

    @Override
    public byte[] fetchSession(final Bytes key, final long startTime, final long endTime) {
        Objects.requireNonNull(key, "key cannot be null");
        validateStoreOpen();
        if (cache == null) {
            return wrapped().fetchSession(key, startTime, endTime);
        } else {
            final Bytes bytesKey = SessionKeySchema.toBinary(key, startTime, endTime);
            final Bytes cacheKey = cacheFunction.cacheKey(bytesKey);
            final LRUCacheEntry entry = cache.get(cacheName, cacheKey);
            if (entry == null) {
                return wrapped().fetchSession(key, startTime, endTime);
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
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes from,
                                                           final Bytes to) {
        Objects.requireNonNull(from, "from cannot be null");
        Objects.requireNonNull(to, "to cannot be null");
        return findSessions(from, to, 0, Long.MAX_VALUE);
    }

    public void flush() {
        cache.flush(cacheName);
        super.flush();
    }

    public void close() {
        flush();
        cache.close(cacheName);
        super.close();
    }
}
