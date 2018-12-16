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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.DefaultRecordConverter;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.RecordConverter;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.KeyValueIteratorFacade;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;
import org.apache.kafka.streams.state.internals.WrappedStateStore;

import java.util.List;
import java.util.Map;

public class KeyValueWithTimestampStoreMaterializer<K, V> {
    private final MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>> materialized;

    public KeyValueWithTimestampStoreMaterializer(final MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        this.materialized = materialized;
    }

    /**
     * @return  StoreBuilder
     */
    public StoreBuilder<KeyValueStore<K, V>> materialize() {
        KeyValueBytesStoreSupplier supplier = (KeyValueBytesStoreSupplier) materialized.storeSupplier();
        if (supplier == null) {
            supplier = Stores.persistentKeyValueWithTimestampStore(materialized.storeName());
        }

        final KeyValueBytesStoreSupplier innerSupplier = supplier;
        final KeyValueStoreBuilder<K, V> builder = new KeyValueStoreBuilder<K, V>(supplier, null, null, Time.SYSTEM) {
            final StoreBuilder<KeyValueStore<K, ValueAndTimestamp<V>>> inner =
                Stores.keyValueWithTimestampStoreBuilder(
                    innerSupplier,
                    materialized.keySerde(),
                    materialized.valueSerde());

            @Override
            public KeyValueStoreBuilder<K, V> withCachingEnabled() {
                inner.withCachingEnabled();
                return this;
            }

            @Override
            public KeyValueStoreBuilder<K, V> withCachingDisabled() {
                inner.withCachingDisabled();
                return this;
            }

            @Override
            public KeyValueStoreBuilder<K, V> withLoggingEnabled(final Map<String, String> config) {
                inner.withLoggingEnabled(config);
                return this;
            }

            @Override
            public KeyValueStoreBuilder<K, V> withLoggingDisabled() {
                inner.withLoggingDisabled();
                return this;
            }

            @Override
            public Map<String, String> logConfig() {
                return inner.logConfig();
            }

            @Override
            public boolean loggingEnabled() {
                return inner.loggingEnabled();
            }

            @Override
            public String name() {
                return inner.name();
            }

            @Override
            public KeyValueStore<K, V> build() {
                return new KeyValueStoreFacade<>(inner.build());
            }
        };

        if (materialized.loggingEnabled()) {
            builder.withLoggingEnabled(materialized.logConfig());
        } else {
            builder.withLoggingDisabled();
        }

        if (materialized.cachingEnabled()) {
            builder.withCachingEnabled();
        }
        return builder;
    }

    public static class KeyValueStoreFacade<A, B> implements KeyValueStore<A, B>, RecordConverter {
        public final KeyValueStore<A, ValueAndTimestamp<B>> inner;
        private final RecordConverter innerRecordConvert;

        public KeyValueStoreFacade(final KeyValueStore<A, ValueAndTimestamp<B>> store) {
            inner = store;
            final StateStore rootStore = store instanceof WrappedStateStore ? ((WrappedStateStore) store).inner() : store;
            innerRecordConvert = rootStore instanceof RecordConverter ? (RecordConverter) rootStore : new DefaultRecordConverter();
        }

        @Override
        public void init(final ProcessorContext context,
                         final StateStore root) {
            inner.init(context, root);
        }

        @Override
        public void put(final A key,
                        final B value) {
            inner.put(key, ValueAndTimestamp.make(value, -1L));
        }

        @Override
        public B putIfAbsent(final A key,
                             final B value) {
            final ValueAndTimestamp<B> old = inner.putIfAbsent(key, ValueAndTimestamp.make(value, -1L));
            return old == null ? null : old.value();
        }

        @Override
        public void putAll(final List<KeyValue<A, B>> entries) {
            for (final KeyValue<A, B> entry : entries) {
                inner.put(entry.key, ValueAndTimestamp.make(entry.value, -1L));
            }
        }

        @Override
        public B delete(final A key) {
            final ValueAndTimestamp<B> old = inner.delete(key);
            return old == null ? null : old.value();
        }

        @Override
        public B get(final A key) {
            final ValueAndTimestamp<B> valueAndTimestamp = inner.get(key);
            return valueAndTimestamp == null ? null : valueAndTimestamp.value();
        }

        @Override
        public KeyValueIterator<A, B> range(final A from,
                                            final A to) {
            final KeyValueIterator<A, ValueAndTimestamp<B>> innerIterator = inner.range(from, to);
            return new KeyValueIteratorFacade<>(innerIterator);
        }

        @Override
        public KeyValueIterator<A, B> all() {
            final KeyValueIterator<A, ValueAndTimestamp<B>> innerIterator = inner.all();
            return new KeyValueIteratorFacade<>(innerIterator);
        }

        @Override
        public void flush() {
            inner.flush();
        }

        @Override
        public void close() {
            inner.close();
        }

        @Override
        public boolean isOpen() {
            return inner.isOpen();
        }

        @Override
        public long approximateNumEntries() {
            return inner.approximateNumEntries();
        }

        @Override
        public String name() {
            return inner.name();
        }

        @Override
        public boolean persistent() {
            return inner.persistent();
        }

        @Override
        public ConsumerRecord<byte[], byte[]> convert(final ConsumerRecord<byte[], byte[]> record) {
            return innerRecordConvert.convert(record);
        }
    }
}
