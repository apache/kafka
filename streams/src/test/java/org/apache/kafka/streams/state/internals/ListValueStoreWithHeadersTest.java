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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockRecordCollector;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.apache.kafka.test.StreamsTestUtils.toListAndCloseIterator;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Header-specific assertions for the headers-aware list-value store. List/iterator
 * invariants (ordering, tombstones, iterator-during-delete, closed-iterator semantics)
 * are covered for both value modes by {@link ListValueStoreTest}; this file only
 * asserts that per-record headers and timestamps survive a round-trip through the
 * builder → metered → change-logger → ListValueStore pipeline.
 */
public class ListValueStoreWithHeadersTest {
    public enum StoreType { InMemory, RocksDB }

    private KeyValueStore<Integer, ValueTimestampHeaders<String>> listStore;
    final File baseDir = TestUtils.tempDirectory("test");

    public void setup(final StoreType storeType) {
        listStore = buildStore(Serdes.Integer(), Serdes.String(), storeType);

        final MockRecordCollector recordCollector = new MockRecordCollector();
        final InternalMockProcessorContext<Integer, ValueTimestampHeaders<String>> context = new InternalMockProcessorContext<>(
            baseDir,
            Serdes.Integer(),
            null,
            recordCollector,
            new ThreadCache(
                new LogContext("testCache"),
                0,
                new MockStreamsMetrics(new Metrics())));
        context.setTime(1L);

        listStore.init(context, listStore);
    }

    <K, V> KeyValueStore<K, ValueTimestampHeaders<V>> buildStore(final Serde<K> keySerde,
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

    @AfterEach
    public void after() {
        if (listStore != null) {
            listStore.close();
        }
    }

    private ValueTimestampHeaders<String> wrap(final String value, final long ts, final String hKey, final String hValue) {
        final RecordHeaders h = new RecordHeaders();
        h.add(new RecordHeader(hKey, hValue.getBytes()));
        return ValueTimestampHeaders.make(value, ts, h);
    }

    @ParameterizedTest
    @EnumSource(StoreType.class)
    public void shouldRoundTripValuesAndHeadersAcrossList(final StoreType storeType) {
        setup(storeType);

        listStore.put(0, wrap("zero", 10L, "hk0", "hv0"));
        // duplicate key, different headers — both must survive
        listStore.put(0, wrap("zero again", 11L, "hk0b", "hv0b"));
        listStore.put(1, wrap("one", 20L, "hk1", "hv1"));
        listStore.put(2, wrap("two", 30L, "hk2", "hv2"));

        final List<KeyValue<Integer, ValueTimestampHeaders<String>>> all =
            toListAndCloseIterator(listStore.all());

        assertEquals(4, all.size());

        // First entry: key 0, "zero"
        assertEquals(0, all.get(0).key);
        assertEquals("zero", all.get(0).value.value());
        assertEquals(10L, all.get(0).value.timestamp());
        assertHeader(all.get(0).value.headers(), "hk0", "hv0");

        // Second entry: key 0, "zero again" (preserved from the same key's list)
        assertEquals(0, all.get(1).key);
        assertEquals("zero again", all.get(1).value.value());
        assertEquals(11L, all.get(1).value.timestamp());
        assertHeader(all.get(1).value.headers(), "hk0b", "hv0b");

        // Key 1
        assertEquals(1, all.get(2).key);
        assertEquals("one", all.get(2).value.value());
        assertHeader(all.get(2).value.headers(), "hk1", "hv1");

        // Key 2
        assertEquals(2, all.get(3).key);
        assertEquals("two", all.get(3).value.value());
        assertHeader(all.get(3).value.headers(), "hk2", "hv2");
    }

    @ParameterizedTest
    @EnumSource(StoreType.class)
    public void shouldHandleEmptyHeaders(final StoreType storeType) {
        setup(storeType);

        final ValueTimestampHeaders<String> emptyHeadersValue =
            ValueTimestampHeaders.make("v", 50L, new RecordHeaders());
        listStore.put(0, emptyHeadersValue);

        final List<KeyValue<Integer, ValueTimestampHeaders<String>>> all =
            toListAndCloseIterator(listStore.all());
        assertEquals(1, all.size());
        assertEquals("v", all.get(0).value.value());
        assertEquals(50L, all.get(0).value.timestamp());
        assertEquals(0, all.get(0).value.headers().toArray().length);
    }

    private void assertHeader(final Headers headers, final String key, final String expectedValue) {
        final List<String> values = new ArrayList<>();
        headers.headers(key).forEach(h -> values.add(new String(h.value())));
        assertEquals(List.of(expectedValue), values);
    }
}
