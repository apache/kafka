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
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StateSerdes;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

class ChangeLoggingKeyValueStore<K, V> extends WrapperKeyValueStore.AbstractKeyValueStore<K, V> {
    private final ChangeLoggingKeyValueBytesStore innerBytes;
    private final Serde keySerde;
    private final Serde valueSerde;
    private StateSerdes<K, V> serdes;


    ChangeLoggingKeyValueStore(final KeyValueStore<Bytes, byte[]> bytesStore,
                               final Serde keySerde,
                               final Serde valueSerde) {
        this(new ChangeLoggingKeyValueBytesStore(bytesStore), keySerde, valueSerde);
    }

    private ChangeLoggingKeyValueStore(final ChangeLoggingKeyValueBytesStore bytesStore,
                                       final Serde keySerde,
                                       final Serde valueSerde) {
        super(bytesStore);
        this.innerBytes = bytesStore;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void init(final ProcessorContext context, final StateStore root) {
        innerBytes.init(context, root);

        this.serdes = new StateSerdes<>(innerBytes.name(),
                                        keySerde == null ? (Serde<K>) context.keySerde() : keySerde,
                                        valueSerde == null ? (Serde<V>) context.valueSerde() : valueSerde);
    }

    @Override
    public void put(final K key, final V value) {
        final Bytes bytesKey = Bytes.wrap(serdes.rawKey(key));
        final byte[] bytesValue = serdes.rawValue(value);
        innerBytes.put(bytesKey, bytesValue);
    }

    @Override
    public V putIfAbsent(final K key, final V value) {
        final V v = get(key);
        if (v == null) {
            put(key, value);
        }
        return v;
    }

    @Override
    public void putAll(final List<KeyValue<K, V>> entries) {
        final List<KeyValue<Bytes, byte[]>> keyValues = new ArrayList<>();
        for (final KeyValue<K, V> entry : entries) {
            keyValues.add(KeyValue.pair(Bytes.wrap(serdes.rawKey(entry.key)), serdes.rawValue(entry.value)));
        }
        innerBytes.putAll(keyValues);
    }

    @Override
    public V delete(final K key) {
        final byte[] oldValue = innerBytes.delete(Bytes.wrap(serdes.rawKey(key)));
        if (oldValue == null) {
            return null;
        }
        return serdes.valueFrom(oldValue);
    }

    @Override
    public V get(final K key) {
        final byte[] rawValue = innerBytes.get(Bytes.wrap(serdes.rawKey(key)));
        if (rawValue == null) {
            return null;
        }
        return serdes.valueFrom(rawValue);
    }

    @Override
    public KeyValueIterator<K, V> range(final K from, final K to) {
        return new ChangeLoggingKeyValueIterator<>(innerBytes.range(Bytes.wrap(serdes.rawKey(from)),
                                                                    Bytes.wrap(serdes.rawKey(to))),
                                                                    serdes);
    }

    @Override
    public KeyValueIterator<K, V> all() {
        return new ChangeLoggingKeyValueIterator<>(innerBytes.all(), serdes);
    }

    private static class ChangeLoggingKeyValueIterator<K, V> extends AbstractKeyValueIterator<K, V> {

        private final KeyValueIterator<Bytes, byte[]> underlying;
        private final StateSerdes<K, V> serdes;

        // delegating peeks to optimize for cases when each element is peeked multiple times
        private KeyValue<Bytes, byte[]> rawNext;

        ChangeLoggingKeyValueIterator(final KeyValueIterator<Bytes, byte[]> underlying, final StateSerdes<K, V> serdes) {
            super(serdes.stateName());

            this.underlying = underlying;
            this.serdes = serdes;

            this.rawNext = null;
        }

        @Override
        public boolean hasNext() {
            validateIsOpen();

            if (rawNext == null && underlying.hasNext())
                rawNext = underlying.next();

            return rawNext != null;
        }

        /**
         * @throws NoSuchElementException if no next element exists
         */
        @Override
        public KeyValue<K, V> next() {
            validateIsOpen();

            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            final K key = serdes.keyFrom(rawNext.key.get());
            final V value = serdes.valueFrom(rawNext.value);
            return KeyValue.pair(key, value);
        }

        @Override
        public K peekNextKey() {
            validateIsOpen();

            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            return serdes.keyFrom(rawNext.key.get());
        }

        @Override
        public void close() {
            underlying.close();
            super.close();
        }
    }
}
