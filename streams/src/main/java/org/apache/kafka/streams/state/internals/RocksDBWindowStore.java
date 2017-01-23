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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.util.NoSuchElementException;

class RocksDBWindowStore<K, V> implements WindowStore<K, V> {

    private final SegmentedBytesStore bytesStore;
    private final boolean retainDuplicates;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private ProcessorContext context;
    private int seqnum = 0;
    private StateSerdes<K, V> serdes;


    static RocksDBWindowStore<Bytes, byte[]> bytesStore(final SegmentedBytesStore inner, final boolean retainDuplicates) {
        return new RocksDBWindowStore<>(inner, Serdes.Bytes(), Serdes.ByteArray(), retainDuplicates);
    }


    RocksDBWindowStore(final SegmentedBytesStore bytesStore,
                       final Serde<K> keySerde,
                       final Serde<V> valueSerde,
                       final boolean retainDuplicates) {
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.retainDuplicates = retainDuplicates;
        this.bytesStore = bytesStore;
    }


    @Override
    public String name() {
        return bytesStore.name();
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
    public boolean persistent() {
        return true;
    }

    @Override
    public boolean isOpen() {
        return bytesStore.isOpen();
    }

    @Override
    public void flush() {
        bytesStore.flush();
    }

    @Override
    public void close() {
        bytesStore.close();
    }

    @Override
    public void put(K key, V value) {
        put(key, value, context.timestamp());
    }

    @Override
    public void put(K key, V value, long timestamp) {
        if (retainDuplicates) {
            seqnum = (seqnum + 1) & 0x7FFFFFFF;
        }
        bytesStore.put(Bytes.wrap(WindowStoreUtils.toBinaryKey(key, timestamp, seqnum, serdes)), serdes.rawValue(value));
    }


    @SuppressWarnings("unchecked")
    @Override
    public WindowStoreIterator<V> fetch(K key, long timeFrom, long timeTo) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = bytesStore.fetch(Bytes.wrap(serdes.rawKey(key)), timeFrom, timeTo);
        return new TheWindowStoreIterator<>(bytesIterator, serdes);
    }

    private static class TheWindowStoreIterator<V> implements WindowStoreIterator<V> {
        private final KeyValueIterator<Bytes, byte[]> actual;
        private final StateSerdes<?, V> serdes;

        TheWindowStoreIterator(final KeyValueIterator<Bytes, byte[]> actual, final StateSerdes<?, V> serdes) {
            this.actual = actual;
            this.serdes = serdes;
        }

        @Override
        public boolean hasNext() {
            return actual.hasNext();
        }

        /**
         * @throws NoSuchElementException if no next element exists
         */
        @Override
        public KeyValue<Long, V> next() {
            if (!actual.hasNext()) {
                throw new NoSuchElementException();
            }
            final KeyValue<Bytes, byte[]> next = actual.next();
            final long timestamp = WindowStoreUtils.timestampFromBinaryKey(next.key.get());
            final V value = serdes.valueFrom(next.value);
            return KeyValue.pair(timestamp, value);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            actual.close();
        }

        @Override
        public Long peekNextKey() {
            if (!actual.hasNext()) {
                throw new NoSuchElementException();
            }
            return WindowStoreUtils.timestampFromBinaryKey(actual.peekNextKey().get());
        }
    }

}
