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

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockRecordCollector;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.util.Collections;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.apache.kafka.test.StreamsTestUtils.toListAndCloseIterator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ListValueStoreTest {
    public enum StoreType { InMemory, RocksDB }
    public enum ValueMode { Plain, Headers }

    static Stream<Arguments> modes() {
        return Stream.of(
            Arguments.of(StoreType.InMemory, ValueMode.Plain),
            Arguments.of(StoreType.InMemory, ValueMode.Headers),
            Arguments.of(StoreType.RocksDB, ValueMode.Plain),
            Arguments.of(StoreType.RocksDB, ValueMode.Headers)
        );
    }

    private StoreFacade facade;

    final File baseDir = TestUtils.tempDirectory("test");

    public void setup(final StoreType storeType, final ValueMode valueMode) {
        final MockRecordCollector recordCollector = new MockRecordCollector();
        final ThreadCache cache = new ThreadCache(
            new LogContext("testCache"),
            0,
            new MockStreamsMetrics(new Metrics()));

        if (valueMode == ValueMode.Plain) {
            final KeyValueStore<Integer, String> store = buildPlainStore(Serdes.Integer(), Serdes.String(), storeType);
            final InternalMockProcessorContext<Integer, String> context = new InternalMockProcessorContext<>(
                baseDir,
                Serdes.String(),
                Serdes.Integer(),
                recordCollector,
                cache);
            context.setTime(1L);
            store.init(context, store);
            facade = new PlainFacade(store);
        } else {
            final KeyValueStore<Integer, ValueTimestampHeaders<String>> store =
                buildHeadersStore(Serdes.Integer(), Serdes.String(), storeType);
            final InternalMockProcessorContext<Integer, ValueTimestampHeaders<String>> context = new InternalMockProcessorContext<>(
                baseDir,
                Serdes.Integer(),
                null,
                recordCollector,
                cache);
            context.setTime(1L);
            store.init(context, store);
            facade = new HeadersFacade(store);
        }
    }

    @AfterEach
    public void after() {
        if (facade != null) {
            facade.close();
        }
    }

    private <K, V> KeyValueStore<K, V> buildPlainStore(final Serde<K> keySerde,
                                                       final Serde<V> valueSerde,
                                                       final StoreType storeType) {
        return new ListValueStoreBuilder<>(
            storeType == StoreType.RocksDB ? Stores.persistentKeyValueStore("rocksDB list store")
                : Stores.inMemoryKeyValueStore("in-memory list store"),
            keySerde,
            valueSerde,
            Time.SYSTEM)
            .build();
    }

    private <K, V> KeyValueStore<K, ValueTimestampHeaders<V>> buildHeadersStore(final Serde<K> keySerde,
                                                                                 final Serde<V> valueSerde,
                                                                                 final StoreType storeType) {
        return new ListValueStoreBuilder<>(
            storeType == StoreType.RocksDB ? Stores.persistentKeyValueStore("rocksDB list store wh")
                : Stores.inMemoryKeyValueStore("in-memory list store wh"),
            keySerde,
            new ValueTimestampHeadersSerde<>(valueSerde),
            Time.SYSTEM)
            .build();
    }

    @ParameterizedTest
    @MethodSource("modes")
    public void shouldGetAll(final StoreType storeType, final ValueMode valueMode) {
        setup(storeType, valueMode);
        facade.put(0, "zero");
        // should retain duplicates
        facade.put(0, "zero again");
        facade.put(1, "one");
        facade.put(2, "two");

        final KeyValue<Integer, String> zero = KeyValue.pair(0, "zero");
        final KeyValue<Integer, String> zeroAgain = KeyValue.pair(0, "zero again");
        final KeyValue<Integer, String> one = KeyValue.pair(1, "one");
        final KeyValue<Integer, String> two = KeyValue.pair(2, "two");

        assertEquals(
            asList(zero, zeroAgain, one, two),
            toListAndCloseIterator(facade.all())
        );
    }

    @ParameterizedTest
    @MethodSource("modes")
    public void shouldGetAllNonDeletedRecords(final StoreType storeType, final ValueMode valueMode) {
        setup(storeType, valueMode);
        // Add some records
        facade.put(0, "zero");
        facade.put(1, "one");
        facade.put(1, "one again");
        facade.put(2, "two");
        facade.put(3, "three");
        facade.put(4, "four");

        // Delete some records
        facade.put(1, null);
        facade.put(3, null);

        // Only non-deleted records should appear in the all() iterator
        final KeyValue<Integer, String> zero = KeyValue.pair(0, "zero");
        final KeyValue<Integer, String> two = KeyValue.pair(2, "two");
        final KeyValue<Integer, String> four = KeyValue.pair(4, "four");

        assertEquals(
            asList(zero, two, four),
            toListAndCloseIterator(facade.all())
        );
    }

    @ParameterizedTest
    @MethodSource("modes")
    public void shouldGetAllReturnTimestampOrderedRecords(final StoreType storeType, final ValueMode valueMode) {
        setup(storeType, valueMode);
        // Add some records in different order
        facade.put(4, "four");
        facade.put(0, "zero");
        facade.put(2, "two1");
        facade.put(3, "three");
        facade.put(1, "one");

        // Add duplicates
        facade.put(2, "two2");

        // Only non-deleted records should appear in the all() iterator
        final KeyValue<Integer, String> zero = KeyValue.pair(0, "zero");
        final KeyValue<Integer, String> one = KeyValue.pair(1, "one");
        final KeyValue<Integer, String> two1 = KeyValue.pair(2, "two1");
        final KeyValue<Integer, String> two2 = KeyValue.pair(2, "two2");
        final KeyValue<Integer, String> three = KeyValue.pair(3, "three");
        final KeyValue<Integer, String> four = KeyValue.pair(4, "four");

        assertEquals(
            asList(zero, one, two1, two2, three, four),
            toListAndCloseIterator(facade.all())
        );
    }

    @ParameterizedTest
    @MethodSource("modes")
    public void shouldAllowDeleteWhileIterateRecords(final StoreType storeType, final ValueMode valueMode) {
        setup(storeType, valueMode);
        facade.put(0, "zero1");
        facade.put(0, "zero2");
        facade.put(1, "one");

        final KeyValue<Integer, String> zero1 = KeyValue.pair(0, "zero1");
        final KeyValue<Integer, String> zero2 = KeyValue.pair(0, "zero2");
        final KeyValue<Integer, String> one = KeyValue.pair(1, "one");

        final KeyValueIterator<Integer, String> it = facade.all();
        assertEquals(zero1, it.next());

        facade.put(0, null);

        // zero2 should still be returned from the iterator after the delete call
        assertEquals(zero2, it.next());

        it.close();

        // A new all() iterator after a previous all() iterator was closed should not return deleted records.
        assertEquals(Collections.singletonList(one), toListAndCloseIterator(facade.all()));
    }

    @ParameterizedTest
    @MethodSource("modes")
    public void shouldNotReturnMoreDataWhenIteratorClosed(final StoreType storeType, final ValueMode valueMode) {
        setup(storeType, valueMode);
        facade.put(0, "zero1");
        facade.put(0, "zero2");
        facade.put(1, "one");

        final KeyValueIterator<Integer, String> it = facade.all();

        it.close();

        // A new all() iterator after a previous all() iterator was closed should not return deleted records.
        if (storeType == StoreType.InMemory) {
            assertThrows(IllegalStateException.class, it::next);
        } else {
            assertThrows(InvalidStateStoreException.class, it::next);
        }
    }

    /**
     * Adapter that lets the test bodies operate on plain {@code String} values regardless
     * of whether the underlying store wraps them in {@link ValueTimestampHeaders}. Behaviors
     * exercised here (ordering, tombstones, iterator semantics) are properties of the list
     * encoding and should hold for both value modes — the wider byte distribution produced
     * by the headers serde is exactly the kind of input that exercises separator/length-prefix
     * edge cases in the underlying {@code ListValueStore}.
     */
    private interface StoreFacade {
        void put(int key, String value);
        KeyValueIterator<Integer, String> all();
        void close();
    }

    private static final class PlainFacade implements StoreFacade {
        private final KeyValueStore<Integer, String> store;

        PlainFacade(final KeyValueStore<Integer, String> store) {
            this.store = store;
        }

        @Override
        public void put(final int key, final String value) {
            store.put(key, value);
        }

        @Override
        public KeyValueIterator<Integer, String> all() {
            return store.all();
        }

        @Override
        public void close() {
            store.close();
        }
    }

    private static final class HeadersFacade implements StoreFacade {
        private final KeyValueStore<Integer, ValueTimestampHeaders<String>> store;

        HeadersFacade(final KeyValueStore<Integer, ValueTimestampHeaders<String>> store) {
            this.store = store;
        }

        @Override
        public void put(final int key, final String value) {
            store.put(key, value == null ? null : ValueTimestampHeaders.make(value, 0L, new RecordHeaders()));
        }

        @Override
        public KeyValueIterator<Integer, String> all() {
            return new UnwrappingIterator(store.all());
        }

        @Override
        public void close() {
            store.close();
        }

        private static final class UnwrappingIterator implements KeyValueIterator<Integer, String> {
            private final KeyValueIterator<Integer, ValueTimestampHeaders<String>> underlying;

            UnwrappingIterator(final KeyValueIterator<Integer, ValueTimestampHeaders<String>> underlying) {
                this.underlying = underlying;
            }

            @Override
            public boolean hasNext() {
                return underlying.hasNext();
            }

            @Override
            public KeyValue<Integer, String> next() {
                final KeyValue<Integer, ValueTimestampHeaders<String>> next = underlying.next();
                return KeyValue.pair(next.key, next.value.value());
            }

            @Override
            public Integer peekNextKey() {
                return underlying.peekNextKey();
            }

            @Override
            public void close() {
                underlying.close();
            }
        }
    }
}
