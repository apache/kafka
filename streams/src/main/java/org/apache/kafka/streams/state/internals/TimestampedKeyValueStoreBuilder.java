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
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.TimestampedBytesStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.List;
import java.util.Objects;

public class TimestampedKeyValueStoreBuilder<K, V>
    extends AbstractStoreBuilder<K, ValueAndTimestamp<V>, TimestampedKeyValueStore<K, V>> {

    private final KeyValueBytesStoreSupplier storeSupplier;

    public TimestampedKeyValueStoreBuilder(final KeyValueBytesStoreSupplier storeSupplier,
                                           final Serde<K> keySerde,
                                           final Serde<V> valueSerde,
                                           final Time time) {
        super(
            storeSupplier.name(),
            keySerde,
            valueSerde == null ? null : new ValueAndTimestampSerde<>(valueSerde),
            time);
        Objects.requireNonNull(storeSupplier, "bytesStoreSupplier can't be null");
        this.storeSupplier = storeSupplier;
    }

    @Override
    public TimestampedKeyValueStore<K, V> build() {
        KeyValueStore<Bytes, byte[]> store = storeSupplier.get();
        if (!(store instanceof TimestampedBytesStore)) {
            if (store.persistent()) {
                store = new KeyValueToTimestampedKeyValueByteStoreAdapter(store);
            } else {
                store = new InMemoryTimestampedKeyValueStoreMarker(store);
            }
        }
        return new MeteredTimestampedKeyValueStore<>(
            maybeWrapCaching(maybeWrapLogging(store)),
            storeSupplier.metricsScope(),
            time,
            keySerde,
            valueSerde);
    }

    private KeyValueStore<Bytes, byte[]> maybeWrapCaching(final KeyValueStore<Bytes, byte[]> inner) {
        if (!enableCaching) {
            return inner;
        }
        return new CachingKeyValueStore(inner);
    }

    private KeyValueStore<Bytes, byte[]> maybeWrapLogging(final KeyValueStore<Bytes, byte[]> inner) {
        if (!enableLogging) {
            return inner;
        }
        return new ChangeLoggingTimestampedKeyValueBytesStore(inner);
    }

    private final static class InMemoryTimestampedKeyValueStoreMarker
        implements KeyValueStore<Bytes, byte[]>, TimestampedBytesStore {

        final KeyValueStore<Bytes, byte[]> wrapped;

        private InMemoryTimestampedKeyValueStoreMarker(final KeyValueStore<Bytes, byte[]> wrapped) {
            if (wrapped.persistent()) {
                throw new IllegalArgumentException("Provided store must not be a persistent store, but it is.");
            }
            this.wrapped = wrapped;
        }

        @Override
        public void init(final ProcessorContext context,
                         final StateStore root) {
            wrapped.init(context, root);
        }

        @Override
        public void put(final Bytes key,
                        final byte[] value) {
            wrapped.put(key, value);
        }

        @Override
        public byte[] putIfAbsent(final Bytes key,
                                  final byte[] value) {
            return wrapped.putIfAbsent(key, value);
        }

        @Override
        public void putAll(final List<KeyValue<Bytes, byte[]>> entries) {
            wrapped.putAll(entries);
        }

        @Override
        public byte[] delete(final Bytes key) {
            return wrapped.delete(key);
        }

        @Override
        public byte[] get(final Bytes key) {
            return wrapped.get(key);
        }

        @Override
        public KeyValueIterator<Bytes, byte[]> range(final Bytes from,
                                                     final Bytes to) {
            return wrapped.range(from, to);
        }

        @Override
        public KeyValueIterator<Bytes, byte[]> all() {
            return wrapped.all();
        }

        @Override
        public long approximateNumEntries() {
            return wrapped.approximateNumEntries();
        }

        @Override
        public void flush() {
            wrapped.flush();
        }

        @Override
        public void close() {
            wrapped.close();
        }

        @Override
        public boolean isOpen() {
            return wrapped.isOpen();
        }

        @Override
        public String name() {
            return wrapped.name();
        }

        @Override
        public boolean persistent() {
            return false;
        }
    }
}