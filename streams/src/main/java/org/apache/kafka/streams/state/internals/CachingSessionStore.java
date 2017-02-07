/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.CacheFlushListener;
import org.apache.kafka.streams.kstream.internals.SessionKeySerde;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.RecordContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StateSerdes;

import java.util.List;
import java.util.NoSuchElementException;


class CachingSessionStore<K, AGG> extends WrappedStateStore.AbstractStateStore implements SessionStore<K, AGG>, CachedStateStore<Windowed<K>, AGG> {

    private final SessionStore<Bytes, byte[]> bytesStore;
    private final SessionKeySchema keySchema;
    private final Serde<K> keySerde;
    private final Serde<AGG> aggSerde;
    private String cacheName;
    private ThreadCache cache;
    private StateSerdes<K, AGG> serdes;
    private InternalProcessorContext context;
    private CacheFlushListener<Windowed<K>, AGG> flushListener;

    CachingSessionStore(final SessionStore<Bytes, byte[]> bytesStore,
                        final Serde<K> keySerde,
                        final Serde<AGG> aggSerde) {
        super(bytesStore);
        this.bytesStore = bytesStore;
        this.keySerde = keySerde;
        this.aggSerde = aggSerde;
        this.keySchema = new SessionKeySchema();
    }

    @SuppressWarnings("unchecked")
    public void init(final ProcessorContext context, final StateStore root) {
        bytesStore.init(context, root);
        initInternal((InternalProcessorContext) context);
    }

    @SuppressWarnings("unchecked")
    private void initInternal(final InternalProcessorContext context) {
        this.context = context;

        this.serdes = new StateSerdes<>(bytesStore.name(),
                keySerde == null ? (Serde<K>) context.keySerde() : keySerde,
                aggSerde == null ? (Serde<AGG>) context.valueSerde() : aggSerde);


        this.cacheName = context.taskId() + "-" + bytesStore.name();
        this.cache = this.context.getCache();
        cache.addDirtyEntryFlushListener(cacheName, new ThreadCache.DirtyEntryFlushListener() {
            @Override
            public void apply(final List<ThreadCache.DirtyEntry> entries) {
                for (ThreadCache.DirtyEntry entry : entries) {
                    putAndMaybeForward(entry, context);
                }
            }
        });
    }

    public KeyValueIterator<Windowed<K>, AGG> findSessions(final K key,
                                                           final long earliestSessionEndTime,
                                                           final long latestSessionStartTime) {
        validateStoreOpen();
        final Bytes binarySessionId = Bytes.wrap(keySerde.serializer().serialize(this.name(), key));
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = cache.range(cacheName,
                                                                                  keySchema.lowerRange(binarySessionId, earliestSessionEndTime),
                                                                                  keySchema.upperRange(binarySessionId, latestSessionStartTime));
        final KeyValueIterator<Windowed<Bytes>, byte[]> storeIterator = bytesStore.findSessions(binarySessionId, earliestSessionEndTime, latestSessionStartTime);
        final HasNextCondition hasNextCondition = keySchema.hasNextCondition(binarySessionId,
                                                                             earliestSessionEndTime,
                                                                             latestSessionStartTime);
        final PeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator = new FilteredCacheIterator(cacheIterator, hasNextCondition);
        return new MergedSortedCacheSessionStoreIterator<>(filteredCacheIterator, storeIterator, serdes);
    }

    @Override
    public void remove(final Windowed<K> sessionKey) {
        validateStoreOpen();
        put(sessionKey, null);
    }

    @Override
    public void put(final Windowed<K> key, AGG value) {
        validateStoreOpen();
        final Bytes binaryKey = SessionKeySerde.toBinary(key, keySerde.serializer());
        final LRUCacheEntry entry = new LRUCacheEntry(serdes.rawValue(value), true, context.offset(),
                                                      key.window().end(), context.partition(), context.topic());
        cache.put(cacheName, binaryKey, entry);
    }

    @Override
    public KeyValueIterator<Windowed<K>, AGG> fetch(final K key) {
        return findSessions(key, 0, Long.MAX_VALUE);
    }

    private void putAndMaybeForward(final ThreadCache.DirtyEntry entry, final InternalProcessorContext context) {
        final Bytes binaryKey = entry.key();
        final RecordContext current = context.recordContext();
        context.setRecordContext(entry.recordContext());
        try {
            final Windowed<K> key = SessionKeySerde.from(binaryKey.get(), keySerde.deserializer());
            if (flushListener != null) {
                final AGG newValue = serdes.valueFrom(entry.newValue());
                final AGG oldValue = fetchPrevious(binaryKey);
                if (!(newValue == null && oldValue == null)) {
                    flushListener.apply(key, newValue == null ? null : newValue, oldValue);
                }
            }
            bytesStore.put(new Windowed<>(Bytes.wrap(serdes.rawKey(key.key())), key.window()), entry.newValue());
        } finally {
            context.setRecordContext(current);
        }
    }

    private AGG fetchPrevious(final Bytes key) {
        try (final KeyValueIterator<Windowed<Bytes>, byte[]> iterator = bytesStore.fetch(key)) {
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

    public void setFlushListener(CacheFlushListener<Windowed<K>, AGG> flushListener) {
        this.flushListener = flushListener;
    }

    private static class FilteredCacheIterator implements PeekingKeyValueIterator<Bytes, LRUCacheEntry> {
        private final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator;
        private final HasNextCondition hasNextCondition;

        FilteredCacheIterator(final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator,
                              final HasNextCondition hasNextCondition) {
            this.cacheIterator = cacheIterator;
            this.hasNextCondition = hasNextCondition;
        }

        @Override
        public void close() {
            // no-op
        }

        @Override
        public Bytes peekNextKey() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return cacheIterator.peekNextKey();
        }

        @Override
        public boolean hasNext() {
            return hasNextCondition.hasNext(cacheIterator);
        }

        @Override
        public KeyValue<Bytes, LRUCacheEntry> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return cacheIterator.next();

        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public KeyValue<Bytes, LRUCacheEntry> peekNext() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return cacheIterator.peekNext();
        }
    }
}
