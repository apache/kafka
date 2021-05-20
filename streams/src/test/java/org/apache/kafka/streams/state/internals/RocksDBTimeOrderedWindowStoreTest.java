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

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockRecordCollector;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static java.util.Arrays.asList;
import static org.apache.kafka.streams.state.internals.RocksDbWindowBytesStoreSupplier.WindowStoreTypes;
import static org.apache.kafka.test.StreamsTestUtils.toList;
import static org.junit.Assert.assertEquals;

public class RocksDBTimeOrderedWindowStoreTest {
    private static final long WINDOW_SIZE = 3L;
    private static final long SEGMENT_INTERVAL = 60_000L;
    private static final long RETENTION_PERIOD = 2 * SEGMENT_INTERVAL;

    private static final String STORE_NAME = "rocksDB time-ordered window store";

    WindowStore<Integer, String> windowStore;
    InternalMockProcessorContext context;
    MockRecordCollector recordCollector;

    final File baseDir = TestUtils.tempDirectory("test");

    @Before
    public void setup() {
        windowStore = buildWindowStore(RETENTION_PERIOD, WINDOW_SIZE, true, Serdes.Integer(), Serdes.String());

        recordCollector = new MockRecordCollector();
        context = new InternalMockProcessorContext(
            baseDir,
            Serdes.String(),
            Serdes.Integer(),
            recordCollector,
            new ThreadCache(
                new LogContext("testCache"),
                0,
                new MockStreamsMetrics(new Metrics())));
        context.setTime(1L);

        windowStore.init((StateStoreContext) context, windowStore);
    }

    @After
    public void after() {
        windowStore.close();
    }

    <K, V> WindowStore<K, V> buildWindowStore(final long retentionPeriod,
                                              final long windowSize,
                                              final boolean retainDuplicates,
                                              final Serde<K> keySerde,
                                              final Serde<V> valueSerde) {
        return new TimeOrderedWindowStoreBuilder<>(
            new RocksDbWindowBytesStoreSupplier(
                STORE_NAME,
                retentionPeriod,
                Math.max(retentionPeriod / 2, 60_000L),
                windowSize,
                retainDuplicates,
                WindowStoreTypes.TIME_ORDERED_WINDOW_STORE),
            keySerde,
            valueSerde,
            Time.SYSTEM)
            .build();
    }

    @Test
    public void shouldGetAll() {
        final long startTime = SEGMENT_INTERVAL - 4L;

        windowStore.put(0, "zero", startTime + 0);
        windowStore.put(1, "one", startTime + 1);
        windowStore.put(2, "two", startTime + 2);

        final KeyValue<Windowed<Integer>, String> zero = windowedPair(0, "zero", startTime + 0);
        final KeyValue<Windowed<Integer>, String> one = windowedPair(1, "one", startTime + 1);
        final KeyValue<Windowed<Integer>, String> two = windowedPair(2, "two", startTime + 2);

        assertEquals(
            asList(zero, one, two),
            toList(windowStore.all())
        );
    }

    @Test
    public void shouldGetAllDuplicates() {
        final long startTime = SEGMENT_INTERVAL - 4L;

        windowStore.put(0, "zero1", startTime + 0);
        windowStore.put(0, "zero2", startTime + 0);
        windowStore.put(0, "zero3", startTime + 0);

        final KeyValue<Windowed<Integer>, String> zero1 = windowedPair(0, "zero1", startTime + 0);
        final KeyValue<Windowed<Integer>, String> zero2 = windowedPair(0, "zero2", startTime + 0);
        final KeyValue<Windowed<Integer>, String> zero3 = windowedPair(0, "zero3", startTime + 0);

        assertEquals(
            asList(zero1, zero2, zero3),
            toList(windowStore.all())
        );
    }

    @Test
    public void shouldGetAllNonDeletedRecords() {
        final long startTime = SEGMENT_INTERVAL - 4L;

        // Add some records
        windowStore.put(0, "zero", startTime + 0);
        windowStore.put(1, "one", startTime + 1);
        windowStore.put(2, "two", startTime + 2);
        windowStore.put(3, "three", startTime + 3);
        windowStore.put(4, "four", startTime + 4);

        // Delete some records
        windowStore.put(1, null, startTime + 1);
        windowStore.put(3, null, startTime + 3);

        // Only non-deleted records should appear in the all() iterator
        final KeyValue<Windowed<Integer>, String> zero = windowedPair(0, "zero", startTime + 0);
        final KeyValue<Windowed<Integer>, String> two = windowedPair(2, "two", startTime + 2);
        final KeyValue<Windowed<Integer>, String> four = windowedPair(4, "four", startTime + 4);

        assertEquals(
            asList(zero, two, four),
            toList(windowStore.all())
        );
    }

    @Test
    public void shouldDeleteAllDuplicates() {
        final long startTime = SEGMENT_INTERVAL - 4L;

        windowStore.put(0, "zero1", startTime + 0);
        windowStore.put(0, "zero2", startTime + 0);
        windowStore.put(0, "zero3", startTime + 0);
        windowStore.put(1, "one1", startTime + 1);
        windowStore.put(1, "one2", startTime + 1);

        windowStore.put(0, null, startTime + 0);

        final KeyValue<Windowed<Integer>, String> one1 = windowedPair(1, "one1", startTime + 1);
        final KeyValue<Windowed<Integer>, String> one2 = windowedPair(1, "one2", startTime + 1);

        assertEquals(
            asList(one1, one2),
            toList(windowStore.all())
        );
    }

    @Test
    public void shouldGetAllReturnTimestampOrderedRecords() {
        final long startTime = SEGMENT_INTERVAL - 4L;

        // Add some records in different order
        windowStore.put(4, "four", startTime + 4);
        windowStore.put(0, "zero", startTime + 0);
        windowStore.put(2, "two1", startTime + 2);
        windowStore.put(3, "three", startTime + 3);
        windowStore.put(1, "one", startTime + 1);

        // Add duplicates
        windowStore.put(2, "two2", startTime + 2);

        // Only non-deleted records should appear in the all() iterator
        final KeyValue<Windowed<Integer>, String> zero = windowedPair(0, "zero", startTime + 0);
        final KeyValue<Windowed<Integer>, String> one = windowedPair(1, "one", startTime + 1);
        final KeyValue<Windowed<Integer>, String> two1 = windowedPair(2, "two1", startTime + 2);
        final KeyValue<Windowed<Integer>, String> two2 = windowedPair(2, "two2", startTime + 2);
        final KeyValue<Windowed<Integer>, String> three = windowedPair(3, "three", startTime + 3);
        final KeyValue<Windowed<Integer>, String> four = windowedPair(4, "four", startTime + 4);

        assertEquals(
            asList(zero, one, two1, two2, three, four),
            toList(windowStore.all())
        );
    }

    @Test
    public void shouldEarlyClosedIteratorStillGetAllRecords() {
        final long startTime = SEGMENT_INTERVAL - 4L;

        windowStore.put(0, "zero", startTime + 0);
        windowStore.put(1, "one", startTime + 1);

        final KeyValue<Windowed<Integer>, String> zero = windowedPair(0, "zero", startTime + 0);
        final KeyValue<Windowed<Integer>, String> one = windowedPair(1, "one", startTime + 1);

        final KeyValueIterator<Windowed<Integer>, String> it = windowStore.all();
        assertEquals(zero, it.next());
        it.close();

        // A new all() iterator after a previous all() iterator was closed should return all elements.
        assertEquals(
            asList(zero, one),
            toList(windowStore.all())
        );
    }

    private static <K, V> KeyValue<Windowed<K>, V> windowedPair(final K key, final V value, final long timestamp) {
        return windowedPair(key, value, timestamp, WINDOW_SIZE);
    }

    private static <K, V> KeyValue<Windowed<K>, V> windowedPair(final K key, final V value, final long timestamp, final long windowSize) {
        return KeyValue.pair(new Windowed<>(key, WindowKeySchema.timeWindowForSize(timestamp, windowSize)), value);
    }
}
