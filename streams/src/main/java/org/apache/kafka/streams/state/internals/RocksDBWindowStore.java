/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

class RocksDBWindowStore<K, V> extends WrappedStateStore.AbstractStateStore implements WindowStore<K, V> {

    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    protected final SegmentedBytesStore bytesStore;
    protected final boolean retainDuplicates;

    private ProcessorContext context;
    protected StateSerdes<K, V> serdes;
    protected int seqnum = 0;

    // this is optimizing the case when this store is already a bytes store, in which we can avoid Bytes.wrap() costs
    private static class RocksDBWindowBytesStore extends RocksDBWindowStore<Bytes, byte[]> {
        RocksDBWindowBytesStore(final SegmentedBytesStore inner, final boolean retainDuplicates) {
            super(inner, Serdes.Bytes(), Serdes.ByteArray(), retainDuplicates);
        }

        @Override
        public void put(Bytes key, byte[] value, long timestamp) {
            maybeUpdateSeqnumForDups();

            bytesStore.put(WindowStoreUtils.toBinaryKey(key.get(), timestamp, seqnum), value);
        }

        @Override
        public WindowStoreIterator<byte[]> fetch(Bytes key, long timeFrom, long timeTo) {
            final KeyValueIterator<Bytes, byte[]> bytesIterator = bytesStore.fetch(key, timeFrom, timeTo);
            return WrappedWindowStoreIterator.bytesIterator(bytesIterator, serdes);
        }
    }

    static RocksDBWindowStore<Bytes, byte[]> bytesStore(final SegmentedBytesStore inner, final boolean retainDuplicates) {
        return new RocksDBWindowBytesStore(inner, retainDuplicates);
    }

    RocksDBWindowStore(final SegmentedBytesStore bytesStore,
                       final Serde<K> keySerde,
                       final Serde<V> valueSerde,
                       final boolean retainDuplicates) {
        super(bytesStore);
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.bytesStore = bytesStore;
        this.retainDuplicates = retainDuplicates;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(final ProcessorContext context, final StateStore root) {
        this.context = context;
        // construct the serde
        this.serdes = new StateSerdes<>(bytesStore.name(),
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
        return new WrappedWindowStoreIterator<>(bytesIterator, serdes);
    }

    void maybeUpdateSeqnumForDups() {
        if (retainDuplicates) {
            seqnum = (seqnum + 1) & 0x7FFFFFFF;
        }
    }
}
