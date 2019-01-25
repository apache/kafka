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
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

public class RocksDBWindowWithTimestampStore<K, V> extends WrappedStateStore.AbstractStateStore implements WindowStore<K, V> {

    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final boolean retainDuplicates;
    private final long windowSize;
    private final SegmentedBytesStore bytesStore;

    private ProcessorContext context;
    private StateSerdes<K, V> serdes;
    private int seqnum = 0;

    RocksDBWindowWithTimestampStore(final SegmentedBytesStore bytesStore,
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
    public void put(final K key, final V value) {
        put(key, value, context.timestamp());
    }

    @Override
    public void put(final K key, final V value, final long windowStartTimestamp) {
        maybeUpdateSeqnumForDups();

        bytesStore.put(WindowKeySchema.toStoreKeyBinary(key, windowStartTimestamp, seqnum, serdes), serdes.rawValue(value));
    }

    @Override
    public V fetch(final K key, final long timestamp) {
        final byte[] bytesValue = bytesStore.get(WindowKeySchema.toStoreKeyBinary(key, timestamp, seqnum, serdes));
        if (bytesValue == null) {
            return null;
        }
        return serdes.valueFrom(bytesValue);
    }

    @SuppressWarnings("deprecation")
    @Override
    public WindowStoreIterator<V> fetch(final K key, final long timeFrom, final long timeTo) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = bytesStore.fetch(Bytes.wrap(serdes.rawKey(key)), timeFrom, timeTo);
        return new WindowStoreIteratorWrapper<>(bytesIterator, serdes, windowSize).valuesIterator();
    }

    @SuppressWarnings("deprecation")
    @Override
    public KeyValueIterator<Windowed<K>, V> fetch(final K from, final K to, final long timeFrom, final long timeTo) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = bytesStore.fetch(Bytes.wrap(serdes.rawKey(from)), Bytes.wrap(serdes.rawKey(to)), timeFrom, timeTo);
        return new WindowStoreIteratorWrapper<>(bytesIterator, serdes, windowSize).keyValueIterator();
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> all() {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = bytesStore.all();
        return new WindowStoreIteratorWrapper<>(bytesIterator, serdes, windowSize).keyValueIterator();
    }

    @SuppressWarnings("deprecation")
    @Override
    public KeyValueIterator<Windowed<K>, V> fetchAll(final long timeFrom, final long timeTo) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = bytesStore.fetchAll(timeFrom, timeTo);
        return new WindowStoreIteratorWrapper<>(bytesIterator, serdes, windowSize).keyValueIterator();
    }

    private void maybeUpdateSeqnumForDups() {
        if (retainDuplicates) {
            seqnum = (seqnum + 1) & 0x7FFFFFFF;
        }
    }
}
