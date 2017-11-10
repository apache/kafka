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
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

public class RocksDBWindowStore<K, V> extends WrappedStateStore.AbstractStateStore implements WindowStore<K, V> {

    // this is optimizing the case when this store is already a bytes store, in which we can avoid Bytes.wrap() costs
    private static class RocksDBWindowBytesStore extends RocksDBWindowStore<Bytes, byte[]> {
        RocksDBWindowBytesStore(final SegmentedBytesStore inner, final boolean retainDuplicates, final long windowSize) {
            super(inner, Serdes.Bytes(), Serdes.ByteArray(), retainDuplicates, windowSize);
        }

        @Override
        public void put(Bytes key, byte[] value, long timestamp) {
            maybeUpdateSeqnumForDups();

            bytesStore.put(WindowStoreUtils.toBinaryKey(key.get(), timestamp, seqnum), value);
        }

        @Override
        public WindowStoreIterator<byte[]> fetch(Bytes key, long timeFrom, long timeTo) {
            final KeyValueIterator<Bytes, byte[]> bytesIterator = bytesStore.fetch(key, timeFrom, timeTo);
            return WindowStoreIteratorWrapper.bytesIterator(bytesIterator, serdes, windowSize).valuesIterator();
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes from, Bytes to, long timeFrom, long timeTo) {
            final KeyValueIterator<Bytes, byte[]> bytesIterator = bytesStore.fetch(from, to, timeFrom, timeTo);
            return WindowStoreIteratorWrapper.bytesIterator(bytesIterator, serdes, windowSize).keyValueIterator();
        }
    }

    static RocksDBWindowStore<Bytes, byte[]> bytesStore(final SegmentedBytesStore inner, final boolean retainDuplicates, final long windowSize) {
        return new RocksDBWindowBytesStore(inner, retainDuplicates, windowSize);
    }

    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final boolean retainDuplicates;
    protected final long windowSize;
    protected final SegmentedBytesStore bytesStore;

    private ProcessorContext context;
    protected StateSerdes<K, V> serdes;
    protected int seqnum = 0;

    RocksDBWindowStore(final SegmentedBytesStore bytesStore,
                       final Serde<K> keySerde,
                       final Serde<V> valueSerde,
                       final boolean retainDuplicates,
                       final long windowSize) {
        super(bytesStore);
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.bytesStore = bytesStore;
        this.retainDuplicates = retainDuplicates;
        this.windowSize = windowSize;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(final ProcessorContext context, final StateStore root) {
        this.context = context;
        // construct the serde
        serdes = new StateSerdes<>(ProcessorStateManager.storeChangelogTopic(context.applicationId(), bytesStore.name()),
                                   keySerde == null ? (Serde<K>) context.keySerde() : keySerde,
                                   valueSerde == null ? (Serde<V>) context.valueSerde() : valueSerde);

        bytesStore.init(context, root);
    }

    @Override
    public void put(K key, V value) {
        put(key, value, context.timestamp());
    }

    @Override
    public void put(K key, V value, long timestamp) {
        maybeUpdateSeqnumForDups();

        bytesStore.put(WindowStoreUtils.toBinaryKey(key, timestamp, seqnum, serdes), serdes.rawValue(value));
    }

    @Override
    public WindowStoreIterator<V> fetch(K key, long timeFrom, long timeTo) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = bytesStore.fetch(Bytes.wrap(serdes.rawKey(key)), timeFrom, timeTo);
        return new WindowStoreIteratorWrapper<>(bytesIterator, serdes, windowSize).valuesIterator();
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> fetch(K from, K to, long timeFrom, long timeTo) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = bytesStore.fetch(Bytes.wrap(serdes.rawKey(from)), Bytes.wrap(serdes.rawKey(to)), timeFrom, timeTo);
        return new WindowStoreIteratorWrapper<>(bytesIterator, serdes, windowSize).keyValueIterator();
    }

    void maybeUpdateSeqnumForDups() {
        if (retainDuplicates) {
            seqnum = (seqnum + 1) & 0x7FFFFFFF;
        }
    }
}
