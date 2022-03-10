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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsConfig.InternalConfig;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.ChangelogRecordDeserializationHelper;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.Task.TaskType;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.testutil.LogCaptureAppender;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.internals.PrefixedWindowKeySchemas.KeyFirstWindowKeySchema;
import org.apache.kafka.streams.state.internals.PrefixedWindowKeySchemas.TimeFirstWindowKeySchema;
import org.apache.kafka.streams.state.internals.SegmentedBytesStore.KeySchema;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockRecordCollector;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.WriteBatch;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.SimpleTimeZone;

import static java.util.Arrays.asList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.state.internals.WindowKeySchema.timeWindowForSize;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

public abstract class AbstractDualSchemaRocksDBSegmentedBytesStoreTest<S extends Segment> {

    private final long windowSizeForTimeWindow = 500;
    private InternalMockProcessorContext context;
    private AbstractDualSchemaRocksDBSegmentedBytesStore<S> bytesStore;
    private File stateDir;
    private final Window[] windows = new Window[4];
    private Window nextSegmentWindow;

    final long retention = 1000;
    final long segmentInterval = 60_000L;
    final String storeName = "bytes-store";

    @Before
    public void before() {
        if (getBaseSchema() instanceof TimeFirstWindowKeySchema) {
            windows[0] = timeWindowForSize(10L, windowSizeForTimeWindow);
            windows[1] = timeWindowForSize(500L, windowSizeForTimeWindow);
            windows[2] = timeWindowForSize(1_000L, windowSizeForTimeWindow);
            windows[3] = timeWindowForSize(60_000L, windowSizeForTimeWindow);
            // All four of the previous windows will go into segment 1.
            // The nextSegmentWindow is computed be a high enough time that when it gets written
            // to the segment store, it will advance stream time past the first segment's retention time and
            // expire it.
            nextSegmentWindow = timeWindowForSize(segmentInterval + retention, windowSizeForTimeWindow);
        }

        bytesStore = getBytesStore();

        stateDir = TestUtils.tempDirectory();
        context = new InternalMockProcessorContext<>(
            stateDir,
            Serdes.String(),
            Serdes.Long(),
            new MockRecordCollector(),
            new ThreadCache(new LogContext("testCache "), 0, new MockStreamsMetrics(new Metrics()))
        );
        bytesStore.init((StateStoreContext) context, bytesStore);
    }

    @After
    public void close() {
        bytesStore.close();
    }

    abstract AbstractDualSchemaRocksDBSegmentedBytesStore<S> getBytesStore();

    abstract AbstractSegments<S> newSegments();

    abstract KeySchema getBaseSchema();

    abstract KeySchema getIndexSchema();

    @Test
    public void shouldPutAndFetch() {
        final String keyA = "a";
        final String keyB = "b";
        final String keyC = "c";
        bytesStore.put(serializeKey(new Windowed<>(keyA, windows[0])), serializeValue(10));
        bytesStore.put(serializeKey(new Windowed<>(keyA, windows[1])), serializeValue(50));
        bytesStore.put(serializeKey(new Windowed<>(keyB, windows[2])), serializeValue(100));
        bytesStore.put(serializeKey(new Windowed<>(keyC, windows[3])), serializeValue(200));

        try (final KeyValueIterator<Bytes, byte[]> values = bytesStore.fetch(
            Bytes.wrap(keyA.getBytes()), 0, windows[2].start())) {

            final List<KeyValue<Windowed<String>, Long>> expected = asList(
                KeyValue.pair(new Windowed<>(keyA, windows[0]), 10L),
                KeyValue.pair(new Windowed<>(keyA, windows[1]), 50L)
            );

            assertEquals(expected, toList(values));
        }

        try (final KeyValueIterator<Bytes, byte[]> values = bytesStore.fetch(
            Bytes.wrap(keyA.getBytes()), Bytes.wrap(keyB.getBytes()), 0, windows[2].start())) {

            final List<KeyValue<Windowed<String>, Long>> expected = asList(
                KeyValue.pair(new Windowed<>(keyA, windows[0]), 10L),
                KeyValue.pair(new Windowed<>(keyA, windows[1]), 50L),
                KeyValue.pair(new Windowed<>(keyB, windows[2]), 100L)
            );

            assertEquals(expected, toList(values));
        }

        try (final KeyValueIterator<Bytes, byte[]> values = bytesStore.fetch(
            null, Bytes.wrap(keyB.getBytes()), 0, windows[2].start())) {

            final List<KeyValue<Windowed<String>, Long>> expected = asList(
                KeyValue.pair(new Windowed<>(keyA, windows[0]), 10L),
                KeyValue.pair(new Windowed<>(keyA, windows[1]), 50L),
                KeyValue.pair(new Windowed<>(keyB, windows[2]), 100L)
            );

            assertEquals(expected, toList(values));
        }

        try (final KeyValueIterator<Bytes, byte[]> values = bytesStore.fetch(
            Bytes.wrap(keyB.getBytes()), null, 0, windows[3].start())) {

            final List<KeyValue<Windowed<String>, Long>> expected = asList(
                KeyValue.pair(new Windowed<>(keyB, windows[2]), 100L),
                KeyValue.pair(new Windowed<>(keyC, windows[3]), 200L)
            );

            assertEquals(expected, toList(values));
        }

        try (final KeyValueIterator<Bytes, byte[]> values = bytesStore.fetch(
            null, null, 0, windows[3].start())) {

            final List<KeyValue<Windowed<String>, Long>> expected = asList(
                KeyValue.pair(new Windowed<>(keyA, windows[0]), 10L),
                KeyValue.pair(new Windowed<>(keyA, windows[1]), 50L),
                KeyValue.pair(new Windowed<>(keyB, windows[2]), 100L),
                KeyValue.pair(new Windowed<>(keyC, windows[3]), 200L)
            );

            assertEquals(expected, toList(values));
        }
    }

    @Test
    public void shouldPutAndBackwardFetch() {
        final String keyA = "a";
        final String keyB = "b";
        final String keyC = "c";

        bytesStore.put(serializeKey(new Windowed<>(keyA, windows[0])), serializeValue(10));
        bytesStore.put(serializeKey(new Windowed<>(keyA, windows[1])), serializeValue(50));
        bytesStore.put(serializeKey(new Windowed<>(keyB, windows[2])), serializeValue(100));
        bytesStore.put(serializeKey(new Windowed<>(keyC, windows[3])), serializeValue(200));

        try (final KeyValueIterator<Bytes, byte[]> values = bytesStore.backwardFetch(
            Bytes.wrap(keyA.getBytes()), 0, windows[2].start())) {

            final List<KeyValue<Windowed<String>, Long>> expected = asList(
                KeyValue.pair(new Windowed<>(keyA, windows[1]), 50L),
                KeyValue.pair(new Windowed<>(keyA, windows[0]), 10L)
            );

            assertEquals(expected, toList(values));
        }

        try (final KeyValueIterator<Bytes, byte[]> values = bytesStore.backwardFetch(
            Bytes.wrap(keyA.getBytes()), Bytes.wrap(keyB.getBytes()), 0, windows[2].start())) {

            final List<KeyValue<Windowed<String>, Long>> expected = asList(
                KeyValue.pair(new Windowed<>(keyB, windows[2]), 100L),
                KeyValue.pair(new Windowed<>(keyA, windows[1]), 50L),
                KeyValue.pair(new Windowed<>(keyA, windows[0]), 10L)
            );

            assertEquals(expected, toList(values));
        }

        try (final KeyValueIterator<Bytes, byte[]> values = bytesStore.backwardFetch(
            null, Bytes.wrap(keyB.getBytes()), 0, windows[2].start())) {

            final List<KeyValue<Windowed<String>, Long>> expected = asList(
                KeyValue.pair(new Windowed<>(keyB, windows[2]), 100L),
                KeyValue.pair(new Windowed<>(keyA, windows[1]), 50L),
                KeyValue.pair(new Windowed<>(keyA, windows[0]), 10L)
            );

            assertEquals(expected, toList(values));
        }

        try (final KeyValueIterator<Bytes, byte[]> values = bytesStore.backwardFetch(
            Bytes.wrap(keyB.getBytes()), null, 0, windows[3].start())) {

            final List<KeyValue<Windowed<String>, Long>> expected = asList(
                KeyValue.pair(new Windowed<>(keyC, windows[3]), 200L),
                KeyValue.pair(new Windowed<>(keyB, windows[2]), 100L)
            );

            assertEquals(expected, toList(values));
        }

        try (final KeyValueIterator<Bytes, byte[]> values = bytesStore.backwardFetch(
            null, null, 0, windows[3].start())) {

            final List<KeyValue<Windowed<String>, Long>> expected = asList(
                KeyValue.pair(new Windowed<>(keyC, windows[3]), 200L),
                KeyValue.pair(new Windowed<>(keyB, windows[2]), 100L),
                KeyValue.pair(new Windowed<>(keyA, windows[1]), 50L),
                KeyValue.pair(new Windowed<>(keyA, windows[0]), 10L)
            );

            assertEquals(expected, toList(values));
        }
    }

    @Test
    public void shouldPutAndFetchWithPrefixKey() {
        final String keyA = "a";
        final String keyB = "aa";
        final String keyC = "aaa";

        final Window maxWindow = new TimeWindow(Long.MAX_VALUE - 1, Long.MAX_VALUE);
        final Bytes serializedKeyA = serializeKey(new Windowed<>(keyA, maxWindow), false, Integer.MAX_VALUE);
        final Bytes serializedKeyB = serializeKey(new Windowed<>(keyB, maxWindow), false, Integer.MAX_VALUE);
        final Bytes serializedKeyC = serializeKey(new Windowed<>(keyC, maxWindow), false, Integer.MAX_VALUE);

        // Key are in decrease order but base storage binary key are in increase order
        assertTrue(serializedKeyA.compareTo(serializedKeyB) > 0);
        assertTrue(serializedKeyB.compareTo(serializedKeyC) > 0);

        bytesStore.put(serializedKeyA, serializeValue(10));
        bytesStore.put(serializedKeyB, serializeValue(50));
        bytesStore.put(serializedKeyC, serializeValue(100));

        try (final KeyValueIterator<Bytes, byte[]> values = bytesStore.fetch(
            Bytes.wrap(keyA.getBytes()), 0, Long.MAX_VALUE)) {

            final List<KeyValue<Windowed<String>, Long>> expected = asList(
                KeyValue.pair(new Windowed<>(keyA, maxWindow), 10L)
            );

            assertEquals(expected, toList(values));
        }

        try (final KeyValueIterator<Bytes, byte[]> values = bytesStore.fetch(
            Bytes.wrap(keyA.getBytes()), Bytes.wrap(keyB.getBytes()), 0, Long.MAX_VALUE)) {

            final List<KeyValue<Windowed<String>, Long>> expected = asList(
                KeyValue.pair(new Windowed<>(keyB, maxWindow), 50L),
                KeyValue.pair(new Windowed<>(keyA, maxWindow), 10L)
            );

            assertEquals(expected, toList(values));
        }

        // KeyC should be ignored and KeyA should be included even in storage, KeyC is before KeyB
        // and KeyA is after KeyB
        try (final KeyValueIterator<Bytes, byte[]> values = bytesStore.fetch(
            null, Bytes.wrap(keyB.getBytes()), 0, Long.MAX_VALUE)) {

            final List<KeyValue<Windowed<String>, Long>> expected = asList(
                KeyValue.pair(new Windowed<>(keyB, maxWindow), 50L),
                KeyValue.pair(new Windowed<>(keyA, maxWindow), 10L)
            );

            assertEquals(expected, toList(values));
        }

        // KeyC should be included even in storage KeyC is before KeyB
        try (final KeyValueIterator<Bytes, byte[]> values = bytesStore.fetch(
            Bytes.wrap(keyB.getBytes()), null, 0, Long.MAX_VALUE)) {

            final List<KeyValue<Windowed<String>, Long>> expected = asList(
                KeyValue.pair(new Windowed<>(keyC, maxWindow), 100L),
                KeyValue.pair(new Windowed<>(keyB, maxWindow), 50L)
            );

            assertEquals(expected, toList(values));
        }

        try (final KeyValueIterator<Bytes, byte[]> values = bytesStore.fetch(
            null, null, 0, Long.MAX_VALUE)) {

            final List<KeyValue<Windowed<String>, Long>> expected = asList(
                KeyValue.pair(new Windowed<>(keyC, maxWindow), 100L),
                KeyValue.pair(new Windowed<>(keyB, maxWindow), 50L),
                KeyValue.pair(new Windowed<>(keyA, maxWindow), 10L)
            );

            assertEquals(expected, toList(values));
        }
    }

    @Test
    public void shouldPutAndBackwardFetchWithPrefix() {
        final String keyA = "a";
        final String keyB = "aa";
        final String keyC = "aaa";

        final Window maxWindow = new TimeWindow(Long.MAX_VALUE - 1, Long.MAX_VALUE);
        final Bytes serializedKeyA = serializeKey(new Windowed<>(keyA, maxWindow), false, Integer.MAX_VALUE);
        final Bytes serializedKeyB = serializeKey(new Windowed<>(keyB, maxWindow), false, Integer.MAX_VALUE);
        final Bytes serializedKeyC = serializeKey(new Windowed<>(keyC, maxWindow), false, Integer.MAX_VALUE);

        // Key are in decrease order but base storage binary key are in increase order
        assertTrue(serializedKeyA.compareTo(serializedKeyB) > 0);
        assertTrue(serializedKeyB.compareTo(serializedKeyC) > 0);

        bytesStore.put(serializedKeyA, serializeValue(10));
        bytesStore.put(serializedKeyB, serializeValue(50));
        bytesStore.put(serializedKeyC, serializeValue(100));

        try (final KeyValueIterator<Bytes, byte[]> values = bytesStore.backwardFetch(
            Bytes.wrap(keyA.getBytes()), 0, Long.MAX_VALUE)) {

            final List<KeyValue<Windowed<String>, Long>> expected = asList(
                KeyValue.pair(new Windowed<>(keyA, maxWindow), 10L)
            );

            assertEquals(expected, toList(values));
        }

        try (final KeyValueIterator<Bytes, byte[]> values = bytesStore.backwardFetch(
            Bytes.wrap(keyA.getBytes()), Bytes.wrap(keyB.getBytes()), 0, Long.MAX_VALUE)) {

            final List<KeyValue<Windowed<String>, Long>> expected = asList(
                KeyValue.pair(new Windowed<>(keyA, maxWindow), 10L),
                KeyValue.pair(new Windowed<>(keyB, maxWindow), 50L)
            );

            assertEquals(expected, toList(values));
        }

        try (final KeyValueIterator<Bytes, byte[]> values = bytesStore.backwardFetch(
            null, Bytes.wrap(keyB.getBytes()), 0, Long.MAX_VALUE)) {

            final List<KeyValue<Windowed<String>, Long>> expected = asList(
                KeyValue.pair(new Windowed<>(keyA, maxWindow), 10L),
                KeyValue.pair(new Windowed<>(keyB, maxWindow), 50L)
            );

            assertEquals(expected, toList(values));
        }

        try (final KeyValueIterator<Bytes, byte[]> values = bytesStore.backwardFetch(
            Bytes.wrap(keyB.getBytes()), null, 0, Long.MAX_VALUE)) {

            final List<KeyValue<Windowed<String>, Long>> expected = asList(
                KeyValue.pair(new Windowed<>(keyB, maxWindow), 50L),
                KeyValue.pair(new Windowed<>(keyC, maxWindow), 100L)
            );

            assertEquals(expected, toList(values));
        }

        try (final KeyValueIterator<Bytes, byte[]> values = bytesStore.backwardFetch(
            null, null, 0, Long.MAX_VALUE)) {

            final List<KeyValue<Windowed<String>, Long>> expected = asList(
                KeyValue.pair(new Windowed<>(keyA, maxWindow), 10L),
                KeyValue.pair(new Windowed<>(keyB, maxWindow), 50L),
                KeyValue.pair(new Windowed<>(keyC, maxWindow), 100L)
            );

            assertEquals(expected, toList(values));
        }
    }

    @Test
    public void shouldSkipAndRemoveDanglingIndex() {
        final String keyA = "a";
        final String keyB = "b";
        if (getIndexSchema() == null) {
            assertThrows(
                IllegalStateException.class,
                () -> bytesStore.putIndex(Bytes.wrap(keyA.getBytes()), new byte[0])
            );
        } else {
            // Only put to index
            final Bytes serializedKey1 = serializeKeyForIndex(new Windowed<>(keyA, windows[1]));
            bytesStore.putIndex(serializedKey1, new byte[0]);

            byte[] value = bytesStore.getIndex(serializedKey1);
            assertThat(Bytes.wrap(value), is(Bytes.wrap(new byte[0])));

            final Bytes serializedKey0 = serializeKey(new Windowed<>(keyA, windows[0]));
            bytesStore.put(serializedKey0, serializeValue(10L));

            final Bytes serializedKey2 = serializeKey(new Windowed<>(keyB, windows[2]));
            bytesStore.put(serializedKey2, serializeValue(20L));

            try (final KeyValueIterator<Bytes, byte[]> results = bytesStore.fetch(
                Bytes.wrap(keyA.getBytes()), Bytes.wrap(keyB.getBytes()), 1, 2000)) {

                final List<KeyValue<Windowed<String>, Long>> expected = asList(
                    KeyValue.pair(new Windowed<>(keyA, windows[0]), 10L),
                    KeyValue.pair(new Windowed<>(keyB, windows[2]), 20L)
                );
                assertEquals(expected, toList(results));
            }

            // Dangling index should be deleted.
            value = bytesStore.getIndex(serializedKey1);
            assertThat(value, is(nullValue()));
        }
    }

    @Test
    public void shouldFindValuesWithinRange() {
        final String key = "a";
        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(10));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[1])), serializeValue(50));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[2])), serializeValue(100));
        try (final KeyValueIterator<Bytes, byte[]> results = bytesStore.fetch(Bytes.wrap(key.getBytes()), 1, 999)) {
            final List<KeyValue<Windowed<String>, Long>> expected = asList(
                KeyValue.pair(new Windowed<>(key, windows[0]), 10L),
                KeyValue.pair(new Windowed<>(key, windows[1]), 50L)
            );

            assertEquals(expected, toList(results));
        }
    }

    @Test
    public void shouldRemove() {
        bytesStore.put(serializeKey(new Windowed<>("a", windows[0])), serializeValue(30));
        bytesStore.put(serializeKey(new Windowed<>("a", windows[1])), serializeValue(50));

        bytesStore.remove(serializeKey(new Windowed<>("a", windows[0])));
        try (final KeyValueIterator<Bytes, byte[]> value = bytesStore.fetch(Bytes.wrap("a".getBytes()), 0, 100)) {
            assertFalse(value.hasNext());
        }

        if (getIndexSchema() != null) {
            // Index should also be removed.
            final Bytes indexKey = serializeKeyForIndex(new Windowed<>("a", windows[0]));
            final byte[] value = bytesStore.getIndex(indexKey);
            assertThat(value, is(nullValue()));
        }
    }

    @Test
    public void shouldRollSegments() {
        // just to validate directories
        final AbstractSegments<S> segments = newSegments();
        final String key = "a";

        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(50));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[1])), serializeValue(100));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[2])), serializeValue(500));
        assertEquals(Collections.singleton(segments.segmentName(0)), segmentDirs());

        bytesStore.put(serializeKey(new Windowed<>(key, windows[3])), serializeValue(1000));
        assertEquals(Utils.mkSet(segments.segmentName(0), segments.segmentName(1)), segmentDirs());

        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.fetch(Bytes.wrap(key.getBytes()), 0, 1500));

        assertEquals(
            asList(
                KeyValue.pair(new Windowed<>(key, windows[0]), 50L),
                KeyValue.pair(new Windowed<>(key, windows[1]), 100L),
                KeyValue.pair(new Windowed<>(key, windows[2]), 500L)
            ),
            results
        );

        segments.close();
    }

    @Test
    public void shouldGetAllSegments() {
        // just to validate directories
        final AbstractSegments<S> segments = newSegments();
        final String keyA = "a";
        final String keyB = "b";

        bytesStore.put(serializeKey(new Windowed<>(keyA, windows[0])), serializeValue(50L));
        assertEquals(Collections.singleton(segments.segmentName(0)), segmentDirs());

        bytesStore.put(serializeKey(new Windowed<>(keyB, windows[3])), serializeValue(100L));
        assertEquals(
            Utils.mkSet(
                segments.segmentName(0),
                segments.segmentName(1)
            ),
            segmentDirs()
        );

        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.all());
        assertEquals(
            asList(
                KeyValue.pair(new Windowed<>(keyA, windows[0]), 50L),
                KeyValue.pair(new Windowed<>(keyB, windows[3]), 100L)
            ),
            results
        );

        segments.close();
    }

    @Test
    public void shouldGetAllBackwards() {
        // just to validate directories
        final AbstractSegments<S> segments = newSegments();
        final String keyA = "a";
        final String keyB = "b";

        bytesStore.put(serializeKey(new Windowed<>(keyA, windows[0])), serializeValue(50L));
        assertEquals(Collections.singleton(segments.segmentName(0)), segmentDirs());

        bytesStore.put(serializeKey(new Windowed<>(keyB, windows[3])), serializeValue(100L));
        assertEquals(
            Utils.mkSet(
                segments.segmentName(0),
                segments.segmentName(1)
            ),
            segmentDirs()
        );

        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.backwardAll());
        assertEquals(
            asList(
                KeyValue.pair(new Windowed<>(keyB, windows[3]), 100L),
                KeyValue.pair(new Windowed<>(keyA, windows[0]), 50L)
            ),
            results
        );

        segments.close();
    }

    @Test
    public void shouldFetchAllSegments() {
        // just to validate directories
        final AbstractSegments<S> segments = newSegments();
        final String key = "a";

        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(50L));
        assertEquals(Collections.singleton(segments.segmentName(0)), segmentDirs());

        bytesStore.put(serializeKey(new Windowed<>(key, windows[3])), serializeValue(100L));
        assertEquals(
            Utils.mkSet(
                segments.segmentName(0),
                segments.segmentName(1)
            ),
            segmentDirs()
        );

        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.fetchAll(0L, 60_000L));
        assertEquals(
            asList(
                KeyValue.pair(new Windowed<>(key, windows[0]), 50L),
                KeyValue.pair(new Windowed<>(key, windows[3]), 100L)
            ),
            results
        );

        segments.close();
    }

    @Test
    public void shouldLoadSegmentsWithOldStyleDateFormattedName() {
        final AbstractSegments<S> segments = newSegments();
        final String key = "a";

        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(50L));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[3])), serializeValue(100L));
        bytesStore.close();

        final String firstSegmentName = segments.segmentName(0);
        final String[] nameParts = firstSegmentName.split("\\.");
        final long segmentId = Long.parseLong(nameParts[1]);
        final SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
        formatter.setTimeZone(new SimpleTimeZone(0, "UTC"));
        final String formatted = formatter.format(new Date(segmentId * segmentInterval));
        final File parent = new File(stateDir, storeName);
        final File oldStyleName = new File(parent, nameParts[0] + "-" + formatted);
        assertTrue(new File(parent, firstSegmentName).renameTo(oldStyleName));

        bytesStore = getBytesStore();

        bytesStore.init((StateStoreContext) context, bytesStore);
        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.fetch(Bytes.wrap(key.getBytes()), 0L, 60_000L));
        assertThat(
            results,
            equalTo(
                asList(
                    KeyValue.pair(new Windowed<>(key, windows[0]), 50L),
                    KeyValue.pair(new Windowed<>(key, windows[3]), 100L)
                )
            )
        );

        segments.close();
    }

    @Test
    public void shouldLoadSegmentsWithOldStyleColonFormattedName() {
        final AbstractSegments<S> segments = newSegments();
        final String key = "a";

        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(50L));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[3])), serializeValue(100L));
        bytesStore.close();

        final String firstSegmentName = segments.segmentName(0);
        final String[] nameParts = firstSegmentName.split("\\.");
        final File parent = new File(stateDir, storeName);
        final File oldStyleName = new File(parent, nameParts[0] + ":" + Long.parseLong(nameParts[1]));
        assertTrue(new File(parent, firstSegmentName).renameTo(oldStyleName));

        bytesStore = getBytesStore();

        bytesStore.init((StateStoreContext) context, bytesStore);
        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.fetch(Bytes.wrap(key.getBytes()), 0L, 60_000L));
        assertThat(
            results,
            equalTo(
                asList(
                    KeyValue.pair(new Windowed<>(key, windows[0]), 50L),
                    KeyValue.pair(new Windowed<>(key, windows[3]), 100L)
                )
            )
        );

        segments.close();
    }

    @Test
    public void shouldBeAbleToWriteToReInitializedStore() {
        final String key = "a";
        // need to create a segment so we can attempt to write to it again.
        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(50));
        bytesStore.close();
        bytesStore.init((StateStoreContext) context, bytesStore);
        bytesStore.put(serializeKey(new Windowed<>(key, windows[1])), serializeValue(100));
    }

    @Test
    public void shouldCreateWriteBatches() {
        final String key = "a";
        final Collection<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        records.add(new ConsumerRecord<>("", 0, 0L, serializeKey(new Windowed<>(key, windows[0]), true).get(), serializeValue(50L)));
        records.add(new ConsumerRecord<>("", 0, 0L, serializeKey(new Windowed<>(key, windows[3]), true).get(), serializeValue(100L)));
        final Map<S, WriteBatch> writeBatchMap = bytesStore.getWriteBatches(records);
        assertEquals(2, writeBatchMap.size());

        final int expectedCount = getIndexSchema() == null ? 1 : 2;
        for (final WriteBatch batch : writeBatchMap.values()) {
            // 2 includes base and index record
            assertEquals(expectedCount, batch.count());
        }
    }

    @Test
    public void shouldRestoreToByteStoreForActiveTask() {
        shouldRestoreToByteStore(TaskType.ACTIVE);
    }

    @Test
    public void shouldRestoreToByteStoreForStandbyTask() {
        context.transitionToStandby(null);
        shouldRestoreToByteStore(TaskType.STANDBY);
    }

    private void shouldRestoreToByteStore(final TaskType taskType) {
        bytesStore.init((StateStoreContext) context, bytesStore);
        // 0 segments initially.
        assertEquals(0, bytesStore.getSegments().size());
        final String key = "a";
        final Collection<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        records.add(new ConsumerRecord<>("", 0, 0L, serializeKey(new Windowed<>(key, windows[0]), true).get(), serializeValue(50L)));
        records.add(new ConsumerRecord<>("", 0, 0L, serializeKey(new Windowed<>(key, windows[3]), true).get(), serializeValue(100L)));
        bytesStore.restoreAllInternal(records);

        // 2 segments are created during restoration.
        assertEquals(2, bytesStore.getSegments().size());

        final List<KeyValue<Windowed<String>, Long>> expected = new ArrayList<>();
        expected.add(new KeyValue<>(new Windowed<>(key, windows[0]), 50L));
        expected.add(new KeyValue<>(new Windowed<>(key, windows[3]), 100L));

        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.all());
        assertEquals(expected, results);
    }

    @Test
    public void shouldMatchPositionAfterPut() {
        bytesStore.init((StateStoreContext) context, bytesStore);

        final String keyA = "a";
        final String keyB = "b";
        final String keyC = "c";

        context.setRecordContext(new ProcessorRecordContext(0, 1, 0, "", new RecordHeaders()));
        bytesStore.put(serializeKey(new Windowed<>(keyA, windows[0])), serializeValue(10));
        context.setRecordContext(new ProcessorRecordContext(0, 2, 0, "", new RecordHeaders()));
        bytesStore.put(serializeKey(new Windowed<>(keyA, windows[1])), serializeValue(50));
        context.setRecordContext(new ProcessorRecordContext(0, 3, 0, "", new RecordHeaders()));
        bytesStore.put(serializeKey(new Windowed<>(keyB, windows[2])), serializeValue(100));
        context.setRecordContext(new ProcessorRecordContext(0, 4, 0, "", new RecordHeaders()));
        bytesStore.put(serializeKey(new Windowed<>(keyC, windows[3])), serializeValue(200));

        final Position expected = Position.fromMap(mkMap(mkEntry("", mkMap(mkEntry(0, 4L)))));
        final Position actual = bytesStore.getPosition();
        assertEquals(expected, actual);
    }

    @Test
    public void shouldRestoreRecordsAndConsistencyVectorSingleTopic() {
        final Properties props = StreamsTestUtils.getStreamsConfig();
        props.put(InternalConfig.IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED, true);
        final File dir = TestUtils.tempDirectory();
        context = new InternalMockProcessorContext<>(
                dir,
                Serdes.String(),
                Serdes.String(),
                new StreamsMetricsImpl(new Metrics(), "mock", StreamsConfig.METRICS_LATEST, new MockTime()),
                new StreamsConfig(props),
                MockRecordCollector::new,
                new ThreadCache(new LogContext("testCache "), 0, new MockStreamsMetrics(new Metrics())),
                Time.SYSTEM
        );
        bytesStore = getBytesStore();
        bytesStore.init((StateStoreContext) context, bytesStore);
        // 0 segments initially.
        assertEquals(0, bytesStore.getSegments().size());

        bytesStore.restoreAllInternal(getChangelogRecords());
        // 2 segments are created during restoration.
        assertEquals(2, bytesStore.getSegments().size());

        final String key = "a";
        final List<KeyValue<Windowed<String>, Long>> expected = new ArrayList<>();
        expected.add(new KeyValue<>(new Windowed<>(key, windows[0]), 50L));
        expected.add(new KeyValue<>(new Windowed<>(key, windows[2]), 100L));
        expected.add(new KeyValue<>(new Windowed<>(key, windows[3]), 200L));

        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.all());
        assertEquals(expected, results);
        assertThat(bytesStore.getPosition(), Matchers.notNullValue());
        assertThat(bytesStore.getPosition().getPartitionPositions(""), Matchers.notNullValue());
        assertThat(bytesStore.getPosition().getPartitionPositions(""), hasEntry(0, 3L));
    }

    @Test
    public void shouldRestoreRecordsAndConsistencyVectorMultipleTopics() {
        final Properties props = StreamsTestUtils.getStreamsConfig();
        props.put(InternalConfig.IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED, true);
        final File dir = TestUtils.tempDirectory();
        context = new InternalMockProcessorContext<>(
                dir,
                Serdes.String(),
                Serdes.String(),
                new StreamsMetricsImpl(new Metrics(), "mock", StreamsConfig.METRICS_LATEST, new MockTime()),
                new StreamsConfig(props),
                MockRecordCollector::new,
                new ThreadCache(new LogContext("testCache "), 0, new MockStreamsMetrics(new Metrics())),
                Time.SYSTEM
        );
        bytesStore = getBytesStore();
        bytesStore.init((StateStoreContext) context, bytesStore);
        // 0 segments initially.
        assertEquals(0, bytesStore.getSegments().size());

        bytesStore.restoreAllInternal(getChangelogRecordsMultipleTopics());
        // 2 segments are created during restoration.
        assertEquals(2, bytesStore.getSegments().size());

        final String key = "a";
        final List<KeyValue<Windowed<String>, Long>> expected = new ArrayList<>();
        expected.add(new KeyValue<>(new Windowed<>(key, windows[0]), 50L));
        expected.add(new KeyValue<>(new Windowed<>(key, windows[2]), 100L));
        expected.add(new KeyValue<>(new Windowed<>(key, windows[3]), 200L));

        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.all());
        assertEquals(expected, results);
        assertThat(bytesStore.getPosition(), Matchers.notNullValue());
        assertThat(bytesStore.getPosition().getPartitionPositions("A"), Matchers.notNullValue());
        assertThat(bytesStore.getPosition().getPartitionPositions("A"), hasEntry(0, 3L));
        assertThat(bytesStore.getPosition().getPartitionPositions("B"), Matchers.notNullValue());
        assertThat(bytesStore.getPosition().getPartitionPositions("B"), hasEntry(0, 2L));
    }

    @Test
    public void shouldHandleTombstoneRecords() {
        final Properties props = StreamsTestUtils.getStreamsConfig();
        props.put(InternalConfig.IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED, true);
        final File dir = TestUtils.tempDirectory();
        context = new InternalMockProcessorContext<>(
                dir,
                Serdes.String(),
                Serdes.String(),
                new StreamsMetricsImpl(new Metrics(), "mock", StreamsConfig.METRICS_LATEST, new MockTime()),
                new StreamsConfig(props),
                MockRecordCollector::new,
                new ThreadCache(new LogContext("testCache "), 0, new MockStreamsMetrics(new Metrics())),
                Time.SYSTEM
        );
        bytesStore = getBytesStore();
        bytesStore.init((StateStoreContext) context, bytesStore);
        // 0 segments initially.
        assertEquals(0, bytesStore.getSegments().size());

        bytesStore.restoreAllInternal(getChangelogRecordsWithTombstones());
        // 1 segments are created during restoration.
        assertEquals(1, bytesStore.getSegments().size());
        final String key = "a";
        final List<KeyValue<Windowed<String>, Long>> expected = new ArrayList<>();
        expected.add(new KeyValue<>(new Windowed<>(key, windows[0]), 50L));

        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.all());
        assertEquals(expected, results);
        assertThat(bytesStore.getPosition(), Matchers.notNullValue());
        assertThat(bytesStore.getPosition().getPartitionPositions("A"), hasEntry(0, 2L));
    }

    @Test
    public void shouldNotThrowWhenRestoringOnMissingHeaders() {
        final Properties props = StreamsTestUtils.getStreamsConfig();
        props.put(InternalConfig.IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED, true);
        final File dir = TestUtils.tempDirectory();
        context = new InternalMockProcessorContext<>(
                dir,
                Serdes.String(),
                Serdes.String(),
                new StreamsMetricsImpl(new Metrics(), "mock", StreamsConfig.METRICS_LATEST, new MockTime()),
                new StreamsConfig(props),
                MockRecordCollector::new,
                new ThreadCache(new LogContext("testCache "), 0, new MockStreamsMetrics(new Metrics())),
                Time.SYSTEM
        );
        bytesStore = getBytesStore();
        bytesStore.init((StateStoreContext) context, bytesStore);
        bytesStore.restoreAllInternal(getChangelogRecordsWithoutHeaders());
        assertThat(bytesStore.getPosition(), is(Position.emptyPosition()));
    }

    private List<ConsumerRecord<byte[], byte[]>> getChangelogRecords() {
        final List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        final Headers headers = new RecordHeaders();

        Position position1 = Position.emptyPosition();
        position1 = position1.withComponent("", 0, 1);
        headers.add(ChangelogRecordDeserializationHelper.CHANGELOG_VERSION_HEADER_RECORD_CONSISTENCY);
        headers.add(new RecordHeader(
                ChangelogRecordDeserializationHelper.CHANGELOG_POSITION_HEADER_KEY,
                PositionSerde.serialize(position1).array())
        );
        records.add(new ConsumerRecord<>("", 0, 0L,  RecordBatch.NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE, -1, -1,
                serializeKey(new Windowed<>("a", windows[0]), true).get(), serializeValue(50L), headers, Optional.empty()));

        headers.remove(ChangelogRecordDeserializationHelper.CHANGELOG_POSITION_HEADER_KEY);
        position1 = position1.withComponent("", 0, 2);
        headers.add(new RecordHeader(
                ChangelogRecordDeserializationHelper.CHANGELOG_POSITION_HEADER_KEY,
                PositionSerde.serialize(position1).array())
        );
        records.add(new ConsumerRecord<>("", 0, 0L,  RecordBatch.NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE, -1, -1,
                serializeKey(new Windowed<>("a", windows[2]), true).get(), serializeValue(100L), headers, Optional.empty()));

        headers.remove(ChangelogRecordDeserializationHelper.CHANGELOG_POSITION_HEADER_KEY);
        position1 = position1.withComponent("", 0, 3);
        headers.add(new RecordHeader(
                ChangelogRecordDeserializationHelper.CHANGELOG_POSITION_HEADER_KEY,
                PositionSerde.serialize(position1).array())
        );
        records.add(new ConsumerRecord<>("", 0, 0L,  RecordBatch.NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE, -1, -1,
                serializeKey(new Windowed<>("a", windows[3]), true).get(), serializeValue(200L), headers, Optional.empty()));

        return records;
    }

    private List<ConsumerRecord<byte[], byte[]>> getChangelogRecordsMultipleTopics() {
        final List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        final Headers headers = new RecordHeaders();
        Position position1 = Position.emptyPosition();

        position1 = position1.withComponent("A", 0, 1);
        headers.add(ChangelogRecordDeserializationHelper.CHANGELOG_VERSION_HEADER_RECORD_CONSISTENCY);
        headers.add(new RecordHeader(
                ChangelogRecordDeserializationHelper.CHANGELOG_POSITION_HEADER_KEY,
                PositionSerde.serialize(position1).array())
        );
        records.add(new ConsumerRecord<>("", 0, 0L,  RecordBatch.NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE, -1, -1,
                serializeKey(new Windowed<>("a", windows[0]), true).get(), serializeValue(50L), headers, Optional.empty()));

        headers.remove(ChangelogRecordDeserializationHelper.CHANGELOG_POSITION_HEADER_KEY);
        position1 = position1.withComponent("B", 0, 2);
        headers.add(new RecordHeader(
                ChangelogRecordDeserializationHelper.CHANGELOG_POSITION_HEADER_KEY,
                PositionSerde.serialize(position1).array())
        );
        records.add(new ConsumerRecord<>("", 0, 0L,  RecordBatch.NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE, -1, -1,
                serializeKey(new Windowed<>("a", windows[2]), true).get(), serializeValue(100L), headers, Optional.empty()));

        headers.remove(ChangelogRecordDeserializationHelper.CHANGELOG_POSITION_HEADER_KEY);
        position1 = position1.withComponent("A", 0, 3);
        headers.add(new RecordHeader(
                ChangelogRecordDeserializationHelper.CHANGELOG_POSITION_HEADER_KEY,
                PositionSerde.serialize(position1).array())
        );
        records.add(new ConsumerRecord<>("", 0, 0L,  RecordBatch.NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE, -1, -1,
                serializeKey(new Windowed<>("a", windows[3]), true).get(), serializeValue(200L), headers, Optional.empty()));

        return records;
    }

    private List<ConsumerRecord<byte[], byte[]>> getChangelogRecordsWithTombstones() {
        final List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        final Headers headers = new RecordHeaders();
        Position position = Position.emptyPosition();

        position = position.withComponent("A", 0, 1);
        headers.add(ChangelogRecordDeserializationHelper.CHANGELOG_VERSION_HEADER_RECORD_CONSISTENCY);
        headers.add(new RecordHeader(
                ChangelogRecordDeserializationHelper.CHANGELOG_POSITION_HEADER_KEY,
                PositionSerde.serialize(position).array()));
        records.add(new ConsumerRecord<>("", 0, 0L,  RecordBatch.NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE, -1, -1,
                serializeKey(new Windowed<>("a", windows[0]), true).get(), serializeValue(50L), headers, Optional.empty()));

        position = position.withComponent("A", 0, 2);
        headers.add(ChangelogRecordDeserializationHelper.CHANGELOG_VERSION_HEADER_RECORD_CONSISTENCY);
        headers.add(new RecordHeader(
                ChangelogRecordDeserializationHelper.CHANGELOG_POSITION_HEADER_KEY,
                PositionSerde.serialize(position).array()));
        records.add(new ConsumerRecord<>("", 0, 0L,  RecordBatch.NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE, -1, -1,
                serializeKey(new Windowed<>("a", windows[2]), true).get(), null, headers, Optional.empty()));

        return records;
    }

    private List<ConsumerRecord<byte[], byte[]>> getChangelogRecordsWithoutHeaders() {
        final List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        records.add(new ConsumerRecord<>("", 0, 0L, serializeKey(new Windowed<>("a", windows[2])).get(), serializeValue(50L)));
        return records;
    }



    @Test
    public void shouldLogAndMeasureExpiredRecords() {
        final Properties streamsConfig = StreamsTestUtils.getStreamsConfig();
        final AbstractDualSchemaRocksDBSegmentedBytesStore<S> bytesStore = getBytesStore();
        final InternalMockProcessorContext context = new InternalMockProcessorContext(
            TestUtils.tempDirectory(),
            new StreamsConfig(streamsConfig)
        );
        final Time time = new SystemTime();
        context.setSystemTimeMs(time.milliseconds());
        bytesStore.init((StateStoreContext) context, bytesStore);

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister()) {
            // write a record to advance stream time, with a high enough timestamp
            // that the subsequent record in windows[0] will already be expired.
            bytesStore.put(serializeKey(new Windowed<>("dummy", nextSegmentWindow)), serializeValue(0));

            final Bytes key = serializeKey(new Windowed<>("a", windows[0]));
            final byte[] value = serializeValue(5);
            bytesStore.put(key, value);

            final List<String> messages = appender.getMessages();
            assertThat(messages, hasItem("Skipping record for expired segment."));
        }

        final Map<MetricName, ? extends Metric> metrics = context.metrics().metrics();
        final String threadId = Thread.currentThread().getName();
        final Metric dropTotal;
        final Metric dropRate;
        dropTotal = metrics.get(new MetricName(
            "dropped-records-total",
            "stream-task-metrics",
            "",
            mkMap(
                mkEntry("thread-id", threadId),
                mkEntry("task-id", "0_0")
            )
        ));

        dropRate = metrics.get(new MetricName(
            "dropped-records-rate",
            "stream-task-metrics",
            "",
            mkMap(
                mkEntry("thread-id", threadId),
                mkEntry("task-id", "0_0")
            )
        ));
        assertEquals(1.0, dropTotal.metricValue());
        assertNotEquals(0.0, dropRate.metricValue());

        bytesStore.close();
    }

    private Set<String> segmentDirs() {
        final File windowDir = new File(stateDir, storeName);

        return Utils.mkSet(Objects.requireNonNull(windowDir.list()));
    }

    private Bytes serializeKey(final Windowed<String> key) {
        return serializeKey(key, false);
    }

    private Bytes serializeKey(final Windowed<String> key, final boolean changeLog) {
        return serializeKey(key, changeLog, 0);
    }

    private Bytes serializeKey(final Windowed<String> key, final boolean changeLog, final int seq) {
        final StateSerdes<String, Long> stateSerdes = StateSerdes.withBuiltinTypes("dummy", String.class, Long.class);
        if (changeLog) {
            return WindowKeySchema.toStoreKeyBinary(key, seq, stateSerdes);
        } else if (getBaseSchema() instanceof TimeFirstWindowKeySchema) {
            return TimeFirstWindowKeySchema.toStoreKeyBinary(key, seq, stateSerdes);
        } else {
            throw new IllegalStateException("Unrecognized serde schema");
        }
    }

    private Bytes serializeKeyForIndex(final Windowed<String> key) {
        final StateSerdes<String, Long> stateSerdes = StateSerdes.withBuiltinTypes("dummy", String.class, Long.class);
        if (getIndexSchema() instanceof KeyFirstWindowKeySchema) {
            return KeyFirstWindowKeySchema.toStoreKeyBinary(key, 0, stateSerdes);
        } else {
            throw new IllegalStateException("Unrecognized serde schema");
        }
    }

    private byte[] serializeValue(final long value) {
        return Serdes.Long().serializer().serialize("", value);
    }

    private List<KeyValue<Windowed<String>, Long>> toList(final KeyValueIterator<Bytes, byte[]> iterator) {
        final List<KeyValue<Windowed<String>, Long>> results = new ArrayList<>();
        final StateSerdes<String, Long> stateSerdes = StateSerdes.withBuiltinTypes("dummy", String.class, Long.class);
        while (iterator.hasNext()) {
            final KeyValue<Bytes, byte[]> next = iterator.next();
            if (getBaseSchema() instanceof TimeFirstWindowKeySchema) {
                final KeyValue<Windowed<String>, Long> deserialized = KeyValue.pair(
                    TimeFirstWindowKeySchema.fromStoreKey(
                        next.key.get(),
                        windowSizeForTimeWindow,
                        stateSerdes.keyDeserializer(),
                        stateSerdes.topic()
                    ),
                    stateSerdes.valueDeserializer().deserialize("dummy", next.value)
                );
                results.add(deserialized);
            } else {
                throw new IllegalStateException("Unrecognized serde schema");
            }
        }
        return results;
    }
}
