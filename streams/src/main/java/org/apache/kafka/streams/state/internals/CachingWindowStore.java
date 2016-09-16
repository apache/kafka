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
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.CacheFlushListener;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.RecordContext;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.util.List;

class CachingWindowStore<K, V> implements WindowStore<K, V>, CachedStateStore<Windowed<K>, V> {

    private final WindowStore<Bytes, byte[]> underlying;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private CacheFlushListener<Windowed<K>, V> flushListener;
    private final long windowSize;
    private String name;
    private ThreadCache cache;
    private InternalProcessorContext context;
    private StateSerdes<K, V> serdes;

    CachingWindowStore(final WindowStore<Bytes, byte[]> underlying,
                       final Serde<K> keySerde,
                       final Serde<V> valueSerde,
                       final long windowSize) {
        this.underlying = underlying;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.windowSize = windowSize;
    }

    @Override
    public String name() {
        return underlying.name();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(final ProcessorContext context, final StateStore root) {
        underlying.init(context, root);
        initInternal(context);
    }

    @SuppressWarnings("unchecked")
    void initInternal(final ProcessorContext context) {
        this.context = (InternalProcessorContext) context;
        this.serdes = new StateSerdes<>(underlying.name(),
                                        keySerde == null ? (Serde<K>) context.keySerde() : keySerde,
                                        valueSerde == null ? (Serde<V>) context.valueSerde() : valueSerde);

        this.name = context.taskId() + "-" + underlying.name();
        this.cache = this.context.getCache();
        cache.addDirtyEntryFlushListener(name, new ThreadCache.DirtyEntryFlushListener() {
            @Override
            public void apply(final List<ThreadCache.DirtyEntry> entries) {
                for (ThreadCache.DirtyEntry entry : entries) {
                    final byte[] binaryKey = entry.key().get();
                    final Bytes key = WindowStoreUtils.bytesKeyFromBinaryKey(binaryKey);
                    final long timestamp = WindowStoreUtils.timestampFromBinaryKey(binaryKey);
                    final Windowed<K> windowedKey = new Windowed<>(WindowStoreUtils.keyFromBinaryKey(binaryKey, serdes),
                                                                   new TimeWindow(timestamp, timestamp + windowSize));
                    maybeForward(entry, key, timestamp, windowedKey, (InternalProcessorContext) context);
                    underlying.put(key, entry.newValue(), timestamp);
                }
            }
        });

    }

    private void maybeForward(final ThreadCache.DirtyEntry entry,
                              final Bytes key,
                              final long timestamp,
                              final Windowed<K> windowedKey,
                              final InternalProcessorContext context) {
        if (flushListener != null) {
            final RecordContext current = context.recordContext();
            context.setRecordContext(entry.recordContext());
            try {
                flushListener.apply(windowedKey,
                                    serdes.valueFrom(entry.newValue()), fetchPrevious(key, timestamp));
            } finally {
                context.setRecordContext(current);
            }

        }
    }

    public void setFlushListener(CacheFlushListener<Windowed<K>, V> flushListener) {
        this.flushListener = flushListener;
    }

    @Override
    public synchronized void flush() {
        cache.flush(name);
        underlying.flush();
    }

    @Override
    public void close() {
        flush();
        underlying.close();
    }

    @Override
    public boolean persistent() {
        return underlying.persistent();
    }

    @Override
    public boolean isOpen() {
        return underlying.isOpen();
    }

    @Override
    public synchronized void put(final K key, final V value) {
        put(key, value, context.timestamp());
    }


    @Override
    public synchronized void put(final K key, final V value, final long timestamp) {
        final byte[] binaryKey = WindowStoreUtils.toBinaryKey(key, timestamp, 0, serdes);
        final LRUCacheEntry entry = new LRUCacheEntry(serdes.rawValue(value), true, context.offset(),
                                                      timestamp, context.partition(), context.topic());
        cache.put(name, binaryKey, entry);
    }

    @Override
    public synchronized WindowStoreIterator<V> fetch(final K key, final long timeFrom, final long timeTo) {
        byte[] binaryFrom = WindowStoreUtils.toBinaryKey(key, timeFrom, 0, serdes);
        byte[] binaryTo = WindowStoreUtils.toBinaryKey(key, timeTo, 0, serdes);

        final WindowStoreIterator<byte[]> underlyingIterator = underlying.fetch(Bytes.wrap(serdes.rawKey(key)), timeFrom, timeTo);
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = cache.range(name, binaryFrom, binaryTo);
        return new MergedSortedCachedWindowStoreIterator<>(cacheIterator, new DelegatingPeekingWindowIterator<>(underlyingIterator), serdes);
    }

    private V fetchPrevious(final Bytes key, final long timestamp) {
        final WindowStoreIterator<byte[]> iterator = underlying.fetch(key, timestamp, timestamp);
        if (!iterator.hasNext()) {
            return null;
        }
        return serdes.valueFrom(iterator.next().value);
    }

}
