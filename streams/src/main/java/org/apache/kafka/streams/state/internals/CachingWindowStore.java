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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.internals.CacheFlushListener;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.RecordContext;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.state.internals.ThreadCache.MemoryLRUCacheBytesIterator;

import java.util.List;

class CachingWindowStore<K, V> extends WrapperWindowStore.AbstractWindowStore<K, V> implements CachedStateStore<Windowed<K>, V> {

    private final WindowStore<Bytes, byte[]> innerWindowBytes;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;

    private String name;
    private ThreadCache cache;
    private InternalProcessorContext context;
    private StateSerdes<K, V> serdes;
    private final long windowSize;
    private CacheFlushListener<Windowed<K>, V> flushListener;

    CachingWindowStore(final WindowStore<Bytes, byte[]> inner,
                       final Serde<K> keySerde,
                       final Serde<V> valueSerde,
                       final long windowSize) {
        super(inner);
        this.innerWindowBytes = inner;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.windowSize = windowSize;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(final ProcessorContext context, final StateStore root) {
        innerWindowBytes.init(context, root);

        this.context = (InternalProcessorContext) context;
        this.serdes = new StateSerdes<>(innerWindowBytes.name(),
                keySerde == null ? (Serde<K>) context.keySerde() : keySerde,
                valueSerde == null ? (Serde<V>) context.valueSerde() : valueSerde);
        this.name = context.taskId() + "-" + innerWindowBytes.name();
        this.cache = this.context.getCache();

        // set cache flush listener
        cache.addDirtyEntryFlushListener(name, new ThreadCache.DirtyEntryFlushListener() {
            @Override
            public void apply(final List<ThreadCache.DirtyEntry> entries) {
                for (ThreadCache.DirtyEntry entry : entries) {
                    final byte[] binaryComboKey = entry.key().get();
                    final Bytes comboKey = Bytes.wrap(binaryComboKey);
                    final K key = WindowStoreUtils.keyFromBinaryKey(binaryComboKey, serdes);
                    final long timestamp = WindowStoreUtils.timestampFromBinaryKey(binaryComboKey);
                    final Windowed<K> windowedKey = new Windowed<>(key, new TimeWindow(timestamp, timestamp + windowSize));

                    maybeForward(entry, comboKey, windowedKey, (InternalProcessorContext) context);

                    innerWindowBytes.put(comboKey, entry.newValue(), timestamp);
                }
            }
        });
    }

    private void maybeForward(final ThreadCache.DirtyEntry entry,
                              final Bytes key,
                              final Windowed<K> windowedKey,
                              final InternalProcessorContext context) {
        if (flushListener != null) {
            final RecordContext current = context.recordContext();
            context.setRecordContext(entry.recordContext());
            try {
                flushListener.apply(windowedKey, serdes.valueFrom(entry.newValue()), serdes.valueFrom(fetchPrevious(key, windowedKey.window().start())));
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
        innerWindowBytes.flush();
    }

    @Override
    public void close() {
        flush();
        cache.close(name);
        innerWindowBytes.close();
    }

    @Override
    public synchronized void put(final K key, final V value) {
        put(key, value, context.timestamp());
    }

    @Override
    public synchronized void put(final K key, final V value, final long timestamp) {
        // since this function may not access the underlying inner store, we need to validate
        // if store is open outside as well.
        validateStoreOpen();

        final Bytes binaryKey = Bytes.wrap(WindowStoreUtils.toBinaryKey(key, timestamp, 0, serdes));
        final LRUCacheEntry entry = new LRUCacheEntry(serdes.rawValue(value), true, context.offset(),
                                                      timestamp, context.partition(), context.topic());
        cache.put(name, binaryKey, entry);
    }

    @Override
    public synchronized WindowStoreIterator<V> fetch(final K key, final long timeFrom, final long timeTo) {
        // since this function may not access the underlying inner store, we need to validate
        // if store is open outside as well.
        validateStoreOpen();

        byte[] binaryKey = serdes.rawKey(key);
        Bytes binaryFrom = Bytes.wrap(WindowStoreUtils.toBinaryKey(binaryKey, timeFrom, 0));
        Bytes binaryTo = Bytes.wrap(WindowStoreUtils.toBinaryKey(binaryKey, timeTo, 0));

        final WindowStoreIterator<byte[]> storeIterator = innerWindowBytes.fetch(Bytes.wrap(binaryKey), timeFrom, timeTo);
        final MemoryLRUCacheBytesIterator cacheIterator = cache.range(name, binaryFrom, binaryTo);
        return new MergedSortedCacheWindowStoreIterator<>(cacheIterator, storeIterator,
                                                          new StateSerdes<>(serdes.stateName(), Serdes.Long(), serdes.valueSerde()));
    }

    private byte[] fetchPrevious(final Bytes binaryKey, long timestamp) {
        final WindowStoreIterator<byte[]> iter = innerWindowBytes.fetch(binaryKey, timestamp, timestamp);

        if (!iter.hasNext())
            return null;
        else
            return iter.next().value;
    }
}
