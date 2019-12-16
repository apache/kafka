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
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.RecordQueue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CachingSessionStore
    extends WrappedStateStore<SessionStore<Bytes, byte[]>, byte[], byte[]>
    implements SessionStore<Bytes, byte[]>, CachedStateStore<byte[], byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(CachingSessionStore.class);

    private final SessionKeySchema keySchema;
    private final SegmentedCacheFunction cacheFunction;
    private String cacheName;
    private ThreadCache cache;
    private InternalProcessorContext context;
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
            new CacheIteratorWrapper(key, earliestSessionEndTime, latestSessionStartTime) :
            cache.range(cacheName,
                        cacheFunction.cacheKey(keySchema.lowerRangeFixedSize(key, earliestSessionEndTime)),
                        cacheFunction.cacheKey(keySchema.upperRangeFixedSize(key, latestSessionStartTime))
            );

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
        if (keyFrom.compareTo(keyTo) > 0) {
            LOG.warn("Returning empty iterator for fetch with invalid key range: from > to. "
                + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                "Note that the built-in numerical serdes do not follow this for negative numbers");
            return KeyValueIterators.emptyIterator();
        }

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

    private class CacheIteratorWrapper implements PeekingKeyValueIterator<Bytes, LRUCacheEntry> {

        private final long segmentInterval;

        private final Bytes keyFrom;
        private final Bytes keyTo;
        private final long latestSessionStartTime;
        private long lastSegmentId;

        private long currentSegmentId;
        private Bytes cacheKeyFrom;
        private Bytes cacheKeyTo;

        private ThreadCache.MemoryLRUCacheBytesIterator current;

        private CacheIteratorWrapper(final Bytes key,
                                     final long earliestSessionEndTime,
                                     final long latestSessionStartTime) {
            this(key, key, earliestSessionEndTime, latestSessionStartTime);
        }

        private CacheIteratorWrapper(final Bytes keyFrom,
                                     final Bytes keyTo,
                                     final long earliestSessionEndTime,
                                     final long latestSessionStartTime) {
            this.keyFrom = keyFrom;
            this.keyTo = keyTo;
            this.latestSessionStartTime = latestSessionStartTime;
            this.lastSegmentId = cacheFunction.segmentId(maxObservedTimestamp);
            this.segmentInterval = cacheFunction.getSegmentInterval();

            this.currentSegmentId = cacheFunction.segmentId(earliestSessionEndTime);

            setCacheKeyRange(earliestSessionEndTime, currentSegmentLastTime());

            this.current = cache.range(cacheName, cacheKeyFrom, cacheKeyTo);
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
            ++currentSegmentId;
            lastSegmentId = cacheFunction.segmentId(maxObservedTimestamp);

            if (currentSegmentId > lastSegmentId) {
                current = null;
                return;
            }

            setCacheKeyRange(currentSegmentBeginTime(), currentSegmentLastTime());

            current.close();
            current = cache.range(cacheName, cacheKeyFrom, cacheKeyTo);
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
