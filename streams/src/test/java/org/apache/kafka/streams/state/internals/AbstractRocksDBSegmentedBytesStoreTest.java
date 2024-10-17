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
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsConfig.InternalConfig;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.ChangelogRecordDeserializationHelper;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockRecordCollector;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.rocksdb.WriteBatch;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.stream.Stream;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.state.internals.WindowKeySchema.timeWindowForSize;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


public abstract class AbstractRocksDBSegmentedBytesStoreTest<S extends Segment> {

    private final long windowSizeForTimeWindow = 500;
    private InternalMockProcessorContext context;
    private AbstractRocksDBSegmentedBytesStore<S> bytesStore;
    private File stateDir;
    private final Window[] windows = new Window[4];
    private Window nextSegmentWindow;

    final long retention = 1000;
    final long segmentInterval = 60_000L;
    final String storeName = "bytes-store";

    public SegmentedBytesStore.KeySchema schema;

    public static Stream<Arguments> getKeySchemas() {
        return Stream.of(Arguments.of(new SessionKeySchema()),
                Arguments.of(new WindowKeySchema()));
    }

    public void before(final SegmentedBytesStore.KeySchema schema) {
        this.schema = schema;
        if (schema instanceof SessionKeySchema) {
            windows[0] = new SessionWindow(10L, 10L);
            windows[1] = new SessionWindow(500L, 1000L);
            windows[2] = new SessionWindow(1_000L, 1_500L);
            windows[3] = new SessionWindow(30_000L, 60_000L);
            // All four of the previous windows will go into segment 1.
            // The nextSegmentWindow is computed be a high enough time that when it gets written
            // to the segment store, it will advance stream time past the first segment's retention time and
            // expire it.
            nextSegmentWindow = new SessionWindow(segmentInterval + retention, segmentInterval + retention);
        }
        if (schema instanceof WindowKeySchema) {
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

    @AfterEach
    public void close() {
        bytesStore.close();
    }

    abstract AbstractRocksDBSegmentedBytesStore<S> getBytesStore();

    abstract AbstractSegments<S> newSegments();

    @ParameterizedTest
    @MethodSource("getKeySchemas")
    public void shouldPutAndFetch(final SegmentedBytesStore.KeySchema schema) {
        before(schema);
        final String keyA = "a";
        final String keyB = "b";
        final String keyC = "c";
        bytesStore.put(serializeKey(new Windowed<>(keyA, windows[0])), serializeValue(10));
        bytesStore.put(serializeKey(new Windowed<>(keyA, windows[1])), serializeValue(50));
        bytesStore.put(serializeKey(new Windowed<>(keyB, windows[2])), serializeValue(100));
        bytesStore.put(serializeKey(new Windowed<>(keyC, windows[3])), serializeValue(200));

        try (final KeyValueIterator<Bytes, byte[]> values = bytesStore.fetch(
            Bytes.wrap(keyA.getBytes()), 0, windows[2].start())) {
            // All Records expired as observed stream time = 60000 implying actual-from = 59001 (60000 - 1000 + 1)
            // for WindowKeySchema, to = 60000 while for SessionKeySchema, to = 30000
            assertEquals(Collections.emptyList(), toList(values));
        }

        try (final KeyValueIterator<Bytes, byte[]> values = bytesStore.fetch(
            Bytes.wrap(keyA.getBytes()), Bytes.wrap(keyB.getBytes()), 0, windows[2].start())) {
            // All Records expired as observed stream time = 60000 implying actual-from = 59001 (60000 - 1000 + 1)
            // for WindowKeySchema, to = 60000 while for SessionKeySchema, to = 30000
            assertEquals(Collections.emptyList(), toList(values));
        }

        try (final KeyValueIterator<Bytes, byte[]> values = bytesStore.fetch(
            null, Bytes.wrap(keyB.getBytes()), 0, windows[2].start())) {
            // All Records expired as observed stream time = 60000 implying actual-from = 59001 (60000 - 1000 + 1)
            // for WindowKeySchema, to = 60000 while for SessionKeySchema, to = 30000
            assertEquals(Collections.emptyList(), toList(values));
        }

        try (final KeyValueIterator<Bytes, byte[]> values = bytesStore.fetch(
            Bytes.wrap(keyB.getBytes()), null, 0, windows[3].start())) {
            // Only 1 record not expired as observed stream time = 60000 implying actual-from = 59001 (60000 - 1000 + 1)
            // for WindowKeySchema, to = 60000 while for SessionKeySchema, to = 30000
            final List<KeyValue<Windowed<String>, Long>> expected = Collections.singletonList(
                KeyValue.pair(new Windowed<>(keyC, windows[3]), 200L)
            );

            assertEquals(expected, toList(values));
        }

        try (final KeyValueIterator<Bytes, byte[]> values = bytesStore.fetch(
            null, null, 0, windows[3].start())) {
            // Only 1 record not expired as observed stream time = 60000 implying actual-from = 59001 (60000 - 1000 + 1)
            // for WindowKeySchema, to = 60000 while for SessionKeySchema, to = 30000
            final List<KeyValue<Windowed<String>, Long>> expected = Collections.singletonList(
                KeyValue.pair(new Windowed<>(keyC, windows[3]), 200L)
            );

            assertEquals(expected, toList(values));
        }
    }

    @ParameterizedTest
    @MethodSource("getKeySchemas")
    public void shouldPutAndBackwardFetch(final SegmentedBytesStore.KeySchema schema) {
        before(schema);
        final String keyA = "a";
        final String keyB = "b";
        final String keyC = "c";
        bytesStore.put(serializeKey(new Windowed<>(keyA, windows[0])), serializeValue(10));
        bytesStore.put(serializeKey(new Windowed<>(keyA, windows[1])), serializeValue(50));
        bytesStore.put(serializeKey(new Windowed<>(keyB, windows[2])), serializeValue(100));
        bytesStore.put(serializeKey(new Windowed<>(keyC, windows[3])), serializeValue(200));

        try (final KeyValueIterator<Bytes, byte[]> values = bytesStore.backwardFetch(
            Bytes.wrap(keyA.getBytes()), 0, windows[2].start())) {

            // All Records expired as observed stream time = 60000 implying actual-from = 59001 (60000 - 1000 + 1)
            // for WindowKeySchema, to = 60000 while for SessionKeySchema, to = 30000
            assertEquals(Collections.emptyList(), toList(values));
        }

        try (final KeyValueIterator<Bytes, byte[]> values = bytesStore.backwardFetch(
            Bytes.wrap(keyA.getBytes()), Bytes.wrap(keyB.getBytes()), 0, windows[2].start())) {

            // All Records expired as observed stream time = 60000 implying actual-from = 59001 (60000 - 1000 + 1)
            // for WindowKeySchema, to = 60000 while for SessionKeySchema, to = 30000
            assertEquals(Collections.emptyList(), toList(values));
        }

        try (final KeyValueIterator<Bytes, byte[]> values = bytesStore.backwardFetch(
            null, Bytes.wrap(keyB.getBytes()), 0, windows[2].start())) {

            // All Records expired as observed stream time = 60000 implying actual-from = 59001 (60000 - 1000 + 1)
            // for WindowKeySchema, to = 60000 while for SessionKeySchema, to = 30000
            assertEquals(Collections.emptyList(), toList(values));
        }

        try (final KeyValueIterator<Bytes, byte[]> values = bytesStore.backwardFetch(
            Bytes.wrap(keyB.getBytes()), null, 0, windows[3].start())) {
            // Only 1 record not expired as observed stream time = 60000 implying actual-from = 59001 (60000 - 1000 + 1)
            // for WindowKeySchema, to = 60000 while for SessionKeySchema, to = 30000
            final List<KeyValue<Windowed<String>, Long>> expected = Collections.singletonList(
                KeyValue.pair(new Windowed<>(keyC, windows[3]), 200L)
            );

            assertEquals(expected, toList(values));
        }

        try (final KeyValueIterator<Bytes, byte[]> values = bytesStore.backwardFetch(
            null, null, 0, windows[3].start())) {
            // Only 1 record not expired as observed stream time = 60000 implying actual-from = 59001 (60000 - 1000 + 1)
            // for WindowKeySchema, to = 60000 while for SessionKeySchema, to = 30000
            final List<KeyValue<Windowed<String>, Long>> expected = Collections.singletonList(
                KeyValue.pair(new Windowed<>(keyC, windows[3]), 200L)
            );

            assertEquals(expected, toList(values));
        }
    }

    @ParameterizedTest
    @MethodSource("getKeySchemas")
    public void shouldFindValuesWithinRange(final SegmentedBytesStore.KeySchema schema) {
        before(schema);
        final String key = "a";
        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(10));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[1])), serializeValue(50));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[2])), serializeValue(100));
        try (final KeyValueIterator<Bytes, byte[]> results = bytesStore.fetch(Bytes.wrap(key.getBytes()), 1, 999)) {
            final List<KeyValue<Windowed<String>, Long>> expected = new ArrayList<>();
            /*
            * For WindowKeySchema, the observedStreamTime is 1000 which means 1 extra record gets returned while for
            * SessionKeySchema, it's 1500. Which changes the actual-from while fetching. In case of SessionKeySchema, the
            * fetch happens from 501-999 while for WindowKeySchema it's from 1-999.
            */
            if (schema instanceof SessionKeySchema) {
                expected.add(KeyValue.pair(new Windowed<>(key, windows[1]), 50L));
            } else {
                expected.add(KeyValue.pair(new Windowed<>(key, windows[0]), 10L));
                expected.add(KeyValue.pair(new Windowed<>(key, windows[1]), 50L));
            }

            assertEquals(expected, toList(results));
        }
    }

    @ParameterizedTest
    @MethodSource("getKeySchemas")
    public void shouldRemove(final SegmentedBytesStore.KeySchema schema) {
        before(schema);
        bytesStore.put(serializeKey(new Windowed<>("a", windows[0])), serializeValue(30));
        bytesStore.put(serializeKey(new Windowed<>("a", windows[1])), serializeValue(50));

        bytesStore.remove(serializeKey(new Windowed<>("a", windows[0])));
        try (final KeyValueIterator<Bytes, byte[]> value = bytesStore.fetch(Bytes.wrap("a".getBytes()), 0, 100)) {
            assertFalse(value.hasNext());
        }
    }

    @ParameterizedTest
    @MethodSource("getKeySchemas")
    public void shouldRollSegments(final SegmentedBytesStore.KeySchema schema) {
        before(schema);
        // just to validate directories
        final AbstractSegments<S> segments = newSegments();
        final String key = "a";

        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(50));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[1])), serializeValue(100));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[2])), serializeValue(500));
        assertEquals(Collections.singleton(segments.segmentName(0)), segmentDirs());

        bytesStore.put(serializeKey(new Windowed<>(key, windows[3])), serializeValue(1000));
        assertEquals(Set.of(segments.segmentName(0), segments.segmentName(1)), segmentDirs());

        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.fetch(Bytes.wrap(key.getBytes()), 0, 1500));
        /*
        * All records expired as observed stream time = 60,000 which sets actual-from to 59001(60,000 - 1000 + 1). to = 1500.
         */
        assertEquals(
            Collections.emptyList(),
            results
        );
        segments.close();
    }

    @ParameterizedTest
    @MethodSource("getKeySchemas")
    public void shouldGetAllSegments(final SegmentedBytesStore.KeySchema schema) {
        before(schema);
        // just to validate directories
        final AbstractSegments<S> segments = newSegments();
        final String key = "a";

        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(50L));
        assertEquals(Collections.singleton(segments.segmentName(0)), segmentDirs());

        bytesStore.put(serializeKey(new Windowed<>(key, windows[3])), serializeValue(100L));
        assertEquals(
            Set.of(
                segments.segmentName(0),
                segments.segmentName(1)
            ),
            segmentDirs()
        );
        /*
        * Only 1 record returned. observed stream time = 60000, actual from = 59001 (60000 - 1000 + 1) and to = Long.MAX.
         */
        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.all());
        assertEquals(
            Collections.singletonList(
                KeyValue.pair(new Windowed<>(key, windows[3]), 100L)
            ),
            results
        );

        segments.close();
    }

    @ParameterizedTest
    @MethodSource("getKeySchemas")
    public void shouldFetchAllSegments(final SegmentedBytesStore.KeySchema schema) {
        before(schema);
        // just to validate directories
        final AbstractSegments<S> segments = newSegments();
        final String key = "a";

        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(50L));
        assertEquals(Collections.singleton(segments.segmentName(0)), segmentDirs());

        bytesStore.put(serializeKey(new Windowed<>(key, windows[3])), serializeValue(100L));
        assertEquals(
            Set.of(
                segments.segmentName(0),
                segments.segmentName(1)
            ),
            segmentDirs()
        );
        /*
         * Only 1 record returned. observed stream time = 60000, actual from = 59001 (60000 - 1000 + 1) and to = 60,000.
         */
        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.fetchAll(0L, 60_000L));
        assertEquals(
            Collections.singletonList(
                KeyValue.pair(new Windowed<>(key, windows[3]), 100L)
            ),
            results
        );

        segments.close();
    }

    @ParameterizedTest
    @MethodSource("getKeySchemas")
    public void shouldLoadSegmentsWithOldStyleDateFormattedName(final SegmentedBytesStore.KeySchema schema) {
        before(schema);
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
                Arrays.asList(
                    KeyValue.pair(new Windowed<>(key, windows[0]), 50L),
                    KeyValue.pair(new Windowed<>(key, windows[3]), 100L)
                )
            )
        );

        segments.close();
    }

    @ParameterizedTest
    @MethodSource("getKeySchemas")
    public void shouldLoadSegmentsWithOldStyleColonFormattedName(final SegmentedBytesStore.KeySchema schema) {
        before(schema);
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
                Arrays.asList(
                    KeyValue.pair(new Windowed<>(key, windows[0]), 50L),
                    KeyValue.pair(new Windowed<>(key, windows[3]), 100L)
                )
            )
        );

        segments.close();
    }

    @ParameterizedTest
    @MethodSource("getKeySchemas")
    public void shouldBeAbleToWriteToReInitializedStore(final SegmentedBytesStore.KeySchema schema) {
        before(schema);
        final String key = "a";
        // need to create a segment so we can attempt to write to it again.
        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(50));
        bytesStore.close();
        bytesStore.init((StateStoreContext) context, bytesStore);
        bytesStore.put(serializeKey(new Windowed<>(key, windows[1])), serializeValue(100));
    }

    @ParameterizedTest
    @MethodSource("getKeySchemas")
    public void shouldCreateWriteBatches(final SegmentedBytesStore.KeySchema schema) {
        before(schema);
        final String key = "a";
        final Collection<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        records.add(new ConsumerRecord<>("", 0, 0L, serializeKey(new Windowed<>(key, windows[0])).get(), serializeValue(50L)));
        records.add(new ConsumerRecord<>("", 0, 0L, serializeKey(new Windowed<>(key, windows[3])).get(), serializeValue(100L)));
        final Map<S, WriteBatch> writeBatchMap = bytesStore.getWriteBatches(records);
        assertEquals(2, writeBatchMap.size());
        for (final WriteBatch batch : writeBatchMap.values()) {
            assertEquals(1, batch.count());
            batch.close();
        }
    }

    @ParameterizedTest
    @MethodSource("getKeySchemas")
    public void shouldRestoreToByteStoreForActiveTask(final SegmentedBytesStore.KeySchema schema) {
        before(schema);
        shouldRestoreToByteStore();
    }

    @ParameterizedTest
    @MethodSource("getKeySchemas")
    public void shouldRestoreToByteStoreForStandbyTask(final SegmentedBytesStore.KeySchema schema) {
        before(schema);
        context.transitionToStandby(null);
        shouldRestoreToByteStore();
    }

    private void shouldRestoreToByteStore() {
        bytesStore.init((StateStoreContext) context, bytesStore);
        // 0 segments initially.
        assertEquals(0, bytesStore.getSegments().size());
        final String key = "a";
        final Collection<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        records.add(new ConsumerRecord<>("", 0, 0L, serializeKey(new Windowed<>(key, windows[0])).get(), serializeValue(50L)));
        records.add(new ConsumerRecord<>("", 0, 0L, serializeKey(new Windowed<>(key, windows[3])).get(), serializeValue(100L)));
        bytesStore.restoreAllInternal(records);

        // 2 segments are created during restoration.
        assertEquals(2, bytesStore.getSegments().size());

        /*
         * Only 1 record returned. observed stream time = 60000, actual from = 59001 (60000 - 1000 + 1) and to = Long.MAX.
         */
        final List<KeyValue<Windowed<String>, Long>> expected = new ArrayList<>();
        expected.add(new KeyValue<>(new Windowed<>(key, windows[3]), 100L));

        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.all());
        assertEquals(expected, results);
    }

    @ParameterizedTest
    @MethodSource("getKeySchemas")
    public void shouldMatchPositionAfterPut(final SegmentedBytesStore.KeySchema schema) {
        before(schema);
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

    @ParameterizedTest
    @MethodSource("getKeySchemas")
    public void shouldRestoreRecordsAndConsistencyVectorSingleTopic(final SegmentedBytesStore.KeySchema schema) {
        before(schema);
        final Properties props = StreamsTestUtils.getStreamsConfig();
        props.put(InternalConfig.IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED, true);
        final File dir = TestUtils.tempDirectory();
        context = new InternalMockProcessorContext<>(
                dir,
                Serdes.String(),
                Serdes.String(),
                new StreamsMetricsImpl(new Metrics(), "mock", new MockTime()),
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
        /*
         * Only 1 record returned. observed stream time = 60000, actual from = 59001 (60000 - 1000 + 1) and to = Long.MAX.
         */
        final List<KeyValue<Windowed<String>, Long>> expected = new ArrayList<>();
        expected.add(new KeyValue<>(new Windowed<>(key, windows[3]), 200L));

        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.all());
        assertEquals(expected, results);
        assertThat(bytesStore.getPosition(), Matchers.notNullValue());
        assertThat(bytesStore.getPosition().getPartitionPositions(""), Matchers.notNullValue());
        assertThat(bytesStore.getPosition().getPartitionPositions(""), hasEntry(0, 3L));
    }

    @ParameterizedTest
    @MethodSource("getKeySchemas")
    public void shouldRestoreRecordsAndConsistencyVectorMultipleTopics(final SegmentedBytesStore.KeySchema schema) {
        before(schema);
        final Properties props = StreamsTestUtils.getStreamsConfig();
        props.put(InternalConfig.IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED, true);
        final File dir = TestUtils.tempDirectory();
        context = new InternalMockProcessorContext<>(
                dir,
                Serdes.String(),
                Serdes.String(),
                new StreamsMetricsImpl(new Metrics(), "mock", new MockTime()),
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
        /*
         * Only 1 record returned. observed stream time = 60000, actual from = 59001 (60000 - 1000 + 1) and to = Long.MAX.
         */
        final List<KeyValue<Windowed<String>, Long>> expected = new ArrayList<>();
        expected.add(new KeyValue<>(new Windowed<>(key, windows[3]), 200L));

        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.all());
        assertEquals(expected, results);
        assertThat(bytesStore.getPosition(), Matchers.notNullValue());
        assertThat(bytesStore.getPosition().getPartitionPositions("A"), Matchers.notNullValue());
        assertThat(bytesStore.getPosition().getPartitionPositions("A"), hasEntry(0, 3L));
        assertThat(bytesStore.getPosition().getPartitionPositions("B"), Matchers.notNullValue());
        assertThat(bytesStore.getPosition().getPartitionPositions("B"), hasEntry(0, 2L));
    }

    @ParameterizedTest
    @MethodSource("getKeySchemas")
    public void shouldHandleTombstoneRecords(final SegmentedBytesStore.KeySchema schema) {
        before(schema);
        final Properties props = StreamsTestUtils.getStreamsConfig();
        props.put(InternalConfig.IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED, true);
        final File dir = TestUtils.tempDirectory();
        context = new InternalMockProcessorContext<>(
                dir,
                Serdes.String(),
                Serdes.String(),
                new StreamsMetricsImpl(new Metrics(), "mock", new MockTime()),
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

        /*
         * For WindowKeySchema, the observedStreamTime is 1000 which means 1 extra record gets returned while for
         * SessionKeySchema, it's 1500. Which changes the actual-from while fetching. In case of SessionKeySchema, the
         * fetch happens from 501 to end while for WindowKeySchema it's from 1 to end.
         */
        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.all());
        if (schema instanceof SessionKeySchema) {
            assertEquals(Collections.emptyList(), results);
        } else {
            final List<KeyValue<Windowed<String>, Long>> expected = new ArrayList<>();
            expected.add(new KeyValue<>(new Windowed<>(key, windows[0]), 50L));
            assertEquals(expected, results);
        }
        assertThat(bytesStore.getPosition(), Matchers.notNullValue());
        assertThat(bytesStore.getPosition().getPartitionPositions("A"), hasEntry(0, 2L));
    }

    @ParameterizedTest
    @MethodSource("getKeySchemas")
    public void shouldNotThrowWhenRestoringOnMissingHeaders(final SegmentedBytesStore.KeySchema schema) {
        before(schema);
        final Properties props = StreamsTestUtils.getStreamsConfig();
        props.put(InternalConfig.IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED, true);
        final File dir = TestUtils.tempDirectory();
        context = new InternalMockProcessorContext<>(
                dir,
                Serdes.String(),
                Serdes.String(),
                new StreamsMetricsImpl(new Metrics(), "mock", new MockTime()),
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
                serializeKey(new Windowed<>("a", windows[0])).get(), serializeValue(50L), headers, Optional.empty()));

        headers.remove(ChangelogRecordDeserializationHelper.CHANGELOG_POSITION_HEADER_KEY);
        position1 = position1.withComponent("", 0, 2);
        headers.add(new RecordHeader(
                ChangelogRecordDeserializationHelper.CHANGELOG_POSITION_HEADER_KEY,
                PositionSerde.serialize(position1).array())
        );
        records.add(new ConsumerRecord<>("", 0, 0L,  RecordBatch.NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE, -1, -1,
                serializeKey(new Windowed<>("a", windows[2])).get(), serializeValue(100L), headers, Optional.empty()));

        headers.remove(ChangelogRecordDeserializationHelper.CHANGELOG_POSITION_HEADER_KEY);
        position1 = position1.withComponent("", 0, 3);
        headers.add(new RecordHeader(
                ChangelogRecordDeserializationHelper.CHANGELOG_POSITION_HEADER_KEY,
                PositionSerde.serialize(position1).array())
        );
        records.add(new ConsumerRecord<>("", 0, 0L,  RecordBatch.NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE, -1, -1,
                serializeKey(new Windowed<>("a", windows[3])).get(), serializeValue(200L), headers, Optional.empty()));

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
                serializeKey(new Windowed<>("a", windows[0])).get(), serializeValue(50L), headers, Optional.empty()));

        headers.remove(ChangelogRecordDeserializationHelper.CHANGELOG_POSITION_HEADER_KEY);
        position1 = position1.withComponent("B", 0, 2);
        headers.add(new RecordHeader(
                ChangelogRecordDeserializationHelper.CHANGELOG_POSITION_HEADER_KEY,
                PositionSerde.serialize(position1).array())
        );
        records.add(new ConsumerRecord<>("", 0, 0L,  RecordBatch.NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE, -1, -1,
                serializeKey(new Windowed<>("a", windows[2])).get(), serializeValue(100L), headers, Optional.empty()));

        headers.remove(ChangelogRecordDeserializationHelper.CHANGELOG_POSITION_HEADER_KEY);
        position1 = position1.withComponent("A", 0, 3);
        headers.add(new RecordHeader(
                ChangelogRecordDeserializationHelper.CHANGELOG_POSITION_HEADER_KEY,
                PositionSerde.serialize(position1).array())
        );
        records.add(new ConsumerRecord<>("", 0, 0L,  RecordBatch.NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE, -1, -1,
                serializeKey(new Windowed<>("a", windows[3])).get(), serializeValue(200L), headers, Optional.empty()));

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
                serializeKey(new Windowed<>("a", windows[0])).get(), serializeValue(50L), headers, Optional.empty()));

        position = position.withComponent("A", 0, 2);
        headers.add(ChangelogRecordDeserializationHelper.CHANGELOG_VERSION_HEADER_RECORD_CONSISTENCY);
        headers.add(new RecordHeader(
                ChangelogRecordDeserializationHelper.CHANGELOG_POSITION_HEADER_KEY,
                PositionSerde.serialize(position).array()));
        records.add(new ConsumerRecord<>("", 0, 0L,  RecordBatch.NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE, -1, -1,
                serializeKey(new Windowed<>("a", windows[2])).get(), null, headers, Optional.empty()));

        return records;
    }

    private List<ConsumerRecord<byte[], byte[]>> getChangelogRecordsWithoutHeaders() {
        final List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        records.add(new ConsumerRecord<>("", 0, 0L, serializeKey(new Windowed<>("a", windows[2])).get(), serializeValue(50L)));
        return records;
    }
    
    @ParameterizedTest
    @MethodSource("getKeySchemas")
    public void shouldMeasureExpiredRecords(final SegmentedBytesStore.KeySchema schema) {
        before(schema);
        final Properties streamsConfig = StreamsTestUtils.getStreamsConfig();
        final AbstractRocksDBSegmentedBytesStore<S> bytesStore = getBytesStore();
        final InternalMockProcessorContext context = new InternalMockProcessorContext(
            TestUtils.tempDirectory(),
            new StreamsConfig(streamsConfig)
        );
        final Time time = Time.SYSTEM;
        context.setSystemTimeMs(time.milliseconds());
        bytesStore.init((StateStoreContext) context, bytesStore);

        // write a record to advance stream time, with a high enough timestamp
        // that the subsequent record in windows[0] will already be expired.
        bytesStore.put(serializeKey(new Windowed<>("dummy", nextSegmentWindow)), serializeValue(0));

        final Bytes key = serializeKey(new Windowed<>("a", windows[0]));
        final byte[] value = serializeValue(5);
        bytesStore.put(key, value);

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

        return Set.of(Objects.requireNonNull(windowDir.list()));
    }

    private Bytes serializeKey(final Windowed<String> key) {
        final StateSerdes<String, Long> stateSerdes = StateSerdes.withBuiltinTypes("dummy", String.class, Long.class);
        if (schema instanceof SessionKeySchema) {
            return Bytes.wrap(SessionKeySchema.toBinary(key, stateSerdes.keySerializer(), "dummy"));
        } else if (schema instanceof WindowKeySchema) {
            return WindowKeySchema.toStoreKeyBinary(key, 0, stateSerdes);
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
            if (schema instanceof WindowKeySchema) {
                final KeyValue<Windowed<String>, Long> deserialized = KeyValue.pair(
                    WindowKeySchema.fromStoreKey(
                        next.key.get(),
                        windowSizeForTimeWindow,
                        stateSerdes.keyDeserializer(),
                        stateSerdes.topic()
                    ),
                    stateSerdes.valueDeserializer().deserialize("dummy", next.value)
                );
                results.add(deserialized);
            } else if (schema instanceof SessionKeySchema) {
                final KeyValue<Windowed<String>, Long> deserialized = KeyValue.pair(
                    SessionKeySchema.from(next.key.get(), stateSerdes.keyDeserializer(), "dummy"),
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
