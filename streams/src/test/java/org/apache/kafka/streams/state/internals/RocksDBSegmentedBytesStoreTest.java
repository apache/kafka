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

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.processor.internals.testutil.LogCaptureAppender;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.NoOpRecordCollector;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
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
import java.util.Set;
import java.util.SimpleTimeZone;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.state.internals.WindowKeySchema.timeWindowForSize;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;


@RunWith(Parameterized.class)
public class RocksDBSegmentedBytesStoreTest {

    private final long windowSizeForTimeWindow = 500;
    private final long retention = 1000;
    private final long segmentInterval = 60_000L;
    private InternalMockProcessorContext context;
    private final String storeName = "bytes-store";
    private RocksDBSegmentedBytesStore bytesStore;
    private File stateDir;
    private final Window[] windows = new Window[4];

    @Parameter
    public SegmentedBytesStore.KeySchema<PlainSegment> schema;

    @Parameters(name = "{0}")
    public static Object[] getKeySchemas() {
        return new Object[] {new SessionKeySchema(), new WindowKeySchema()};
    }

    @Before
    public void before() {

        if (schema instanceof SessionKeySchema) {
            windows[0] = new SessionWindow(10L, 10L);
            windows[1] = new SessionWindow(500L, 1000L);
            windows[2] = new SessionWindow(1_000L, 1_500L);
            windows[3] = new SessionWindow(30_000L, 60_000L);
        }
        if (schema instanceof WindowKeySchema) {
            windows[0] = timeWindowForSize(10L, windowSizeForTimeWindow);
            windows[1] = timeWindowForSize(500L, windowSizeForTimeWindow);
            windows[2] = timeWindowForSize(1_000L, windowSizeForTimeWindow);
            windows[3] = timeWindowForSize(60_000L, windowSizeForTimeWindow);
        }


        bytesStore = new RocksDBSegmentedBytesStore(
            storeName,
            "metrics-scope",
            retention,
            segmentInterval,
            schema
        );

        stateDir = TestUtils.tempDirectory();
        context = new InternalMockProcessorContext(
            stateDir,
            Serdes.String(),
            Serdes.Long(),
            new NoOpRecordCollector(),
            new ThreadCache(new LogContext("testCache "), 0, new MockStreamsMetrics(new Metrics()))
        );
        bytesStore.init(context, bytesStore);
    }

    @After
    public void close() {
        bytesStore.close();
    }

    @Test
    public void shouldPutAndFetch() {
        final String key = "a";
        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(10));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[1])), serializeValue(50));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[2])), serializeValue(100));

        final KeyValueIterator<Bytes, byte[]> values = bytesStore.fetch(Bytes.wrap(key.getBytes()), 0, 500);

        final List<KeyValue<Windowed<String>, Long>> expected = Arrays.asList(
            KeyValue.pair(new Windowed<>(key, windows[0]), 10L),
            KeyValue.pair(new Windowed<>(key, windows[1]), 50L)
        );

        assertEquals(expected, toList(values));
    }

    @Test
    public void shouldFindValuesWithinRange() {
        final String key = "a";
        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(10));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[1])), serializeValue(50));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[2])), serializeValue(100));
        final KeyValueIterator<Bytes, byte[]> results = bytesStore.fetch(Bytes.wrap(key.getBytes()), 1, 999);
        final List<KeyValue<Windowed<String>, Long>> expected = Arrays.asList(
            KeyValue.pair(new Windowed<>(key, windows[0]), 10L),
            KeyValue.pair(new Windowed<>(key, windows[1]), 50L)
        );

        assertEquals(expected, toList(results));
    }


    @Test
    public void shouldRemove() {
        bytesStore.put(serializeKey(new Windowed<>("a", windows[0])), serializeValue(30));
        bytesStore.put(serializeKey(new Windowed<>("a", windows[1])), serializeValue(50));

        bytesStore.remove(serializeKey(new Windowed<>("a", windows[0])));
        final KeyValueIterator<Bytes, byte[]> value = bytesStore.fetch(Bytes.wrap("a".getBytes()), 0, 100);
        assertFalse(value.hasNext());
    }


    @Test
    public void shouldRollSegments() {
        // just to validate directories
        final PlainSegments segments = new PlainSegments(storeName, retention, segmentInterval);
        final String key = "a";

        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(50));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[1])), serializeValue(100));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[2])), serializeValue(500));
        assertEquals(Collections.singleton(segments.segmentName(0)), segmentDirs());

        bytesStore.put(serializeKey(new Windowed<>(key, windows[3])), serializeValue(1000));
        assertEquals(Utils.mkSet(segments.segmentName(0), segments.segmentName(1)), segmentDirs());

        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.fetch(Bytes.wrap(key.getBytes()), 0, 1500));

        assertEquals(
            Arrays.asList(
                KeyValue.pair(new Windowed<>(key, windows[0]), 50L),
                KeyValue.pair(new Windowed<>(key, windows[1]), 100L),
                KeyValue.pair(new Windowed<>(key, windows[2]), 500L)
            ),
            results
        );

    }


    @Test
    public void shouldGetAllSegments() {
        // just to validate directories
        final PlainSegments segments = new PlainSegments(storeName, retention, segmentInterval);
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

        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.all());
        assertEquals(
            Arrays.asList(
                KeyValue.pair(new Windowed<>(key, windows[0]), 50L),
                KeyValue.pair(new Windowed<>(key, windows[3]), 100L)
            ),
            results
        );

    }

    @Test
    public void shouldFetchAllSegments() {
        // just to validate directories
        final PlainSegments segments = new PlainSegments(storeName, retention, segmentInterval);
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
            Arrays.asList(
                KeyValue.pair(new Windowed<>(key, windows[0]), 50L),
                KeyValue.pair(new Windowed<>(key, windows[3]), 100L)
            ),
            results
        );

    }

    @Test
    public void shouldLoadSegmentsWithOldStyleDateFormattedName() {
        final PlainSegments segments = new PlainSegments(storeName, retention, segmentInterval);
        final String key = "a";

        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(50L));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[3])), serializeValue(100L));
        bytesStore.close();

        final String firstSegmentName = segments.segmentName(0);
        final String[] nameParts = firstSegmentName.split("\\.");
        final Long segmentId = Long.parseLong(nameParts[1]);
        final SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
        formatter.setTimeZone(new SimpleTimeZone(0, "UTC"));
        final String formatted = formatter.format(new Date(segmentId * segmentInterval));
        final File parent = new File(stateDir, storeName);
        final File oldStyleName = new File(parent, nameParts[0] + "-" + formatted);
        assertTrue(new File(parent, firstSegmentName).renameTo(oldStyleName));

        bytesStore = new RocksDBSegmentedBytesStore(
            storeName,
            "metrics-scope",
            retention,
            segmentInterval,
            schema
        );

        bytesStore.init(context, bytesStore);
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
    }


    @Test
    public void shouldLoadSegmentsWithOldStyleColonFormattedName() {
        final PlainSegments segments = new PlainSegments(storeName, retention, segmentInterval);
        final String key = "a";

        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(50L));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[3])), serializeValue(100L));
        bytesStore.close();

        final String firstSegmentName = segments.segmentName(0);
        final String[] nameParts = firstSegmentName.split("\\.");
        final File parent = new File(stateDir, storeName);
        final File oldStyleName = new File(parent, nameParts[0] + ":" + Long.parseLong(nameParts[1]));
        assertTrue(new File(parent, firstSegmentName).renameTo(oldStyleName));

        bytesStore = new RocksDBSegmentedBytesStore(
            storeName,
            "metrics-scope",
            retention,
            segmentInterval,
            schema
        );

        bytesStore.init(context, bytesStore);
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
    }


    @Test
    public void shouldBeAbleToWriteToReInitializedStore() {
        final String key = "a";
        // need to create a segment so we can attempt to write to it again.
        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(50));
        bytesStore.close();
        bytesStore.init(context, bytesStore);
        bytesStore.put(serializeKey(new Windowed<>(key, windows[1])), serializeValue(100));
    }

    @Test
    public void shouldCreateWriteBatches() {
        final String key = "a";
        final Collection<KeyValue<byte[], byte[]>> records = new ArrayList<>();
        records.add(new KeyValue<>(serializeKey(new Windowed<>(key, windows[0])).get(), serializeValue(50L)));
        records.add(new KeyValue<>(serializeKey(new Windowed<>(key, windows[3])).get(), serializeValue(100L)));
        final Map<PlainSegment, WriteBatch> writeBatchMap = bytesStore.getWriteBatches(records);
        assertEquals(2, writeBatchMap.size());
        for (final WriteBatch batch : writeBatchMap.values()) {
            assertEquals(1, batch.count());
        }
    }

    @Test
    public void shouldRestoreToByteStore() {
        // 0 segments initially.
        assertEquals(0, bytesStore.getSegments().size());
        final String key = "a";
        final Collection<KeyValue<byte[], byte[]>> records = new ArrayList<>();
        records.add(new KeyValue<>(serializeKey(new Windowed<>(key, windows[0])).get(), serializeValue(50L)));
        records.add(new KeyValue<>(serializeKey(new Windowed<>(key, windows[3])).get(), serializeValue(100L)));
        bytesStore.restoreAllInternal(records);

        // 2 segments are created during restoration.
        assertEquals(2, bytesStore.getSegments().size());

        // Bulk loading is enabled during recovery.
        for (final PlainSegment segment : bytesStore.getSegments()) {
            Assert.assertThat(segment.getOptions().level0FileNumCompactionTrigger(), equalTo(1 << 30));
        }

        final List<KeyValue<Windowed<String>, Long>> expected = new ArrayList<>();
        expected.add(new KeyValue<>(new Windowed<>(key, windows[0]), 50L));
        expected.add(new KeyValue<>(new Windowed<>(key, windows[3]), 100L));

        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.all());
        assertEquals(expected, results);
    }

    @Test
    public void shouldRespectBulkLoadOptionsDuringInit() {
        bytesStore.init(context, bytesStore);
        final String key = "a";
        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(50L));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[3])), serializeValue(100L));
        assertEquals(2, bytesStore.getSegments().size());

        final StateRestoreListener restoreListener = context.getRestoreListener(bytesStore.name());

        restoreListener.onRestoreStart(null, bytesStore.name(), 0L, 0L);

        for (final PlainSegment segment : bytesStore.getSegments()) {
            Assert.assertThat(segment.getOptions().level0FileNumCompactionTrigger(), equalTo(1 << 30));
        }

        restoreListener.onRestoreEnd(null, bytesStore.name(), 0L);
        for (final PlainSegment segment : bytesStore.getSegments()) {
            Assert.assertThat(segment.getOptions().level0FileNumCompactionTrigger(), equalTo(4));
        }
    }

    @Test
    public void shouldLogAndMeasureExpiredRecords() {
        LogCaptureAppender.setClassLoggerToDebug(RocksDBSegmentedBytesStore.class);
        final LogCaptureAppender appender = LogCaptureAppender.createAndRegister();

        context.setStreamTime(Math.max(retention, segmentInterval) * 2);
        bytesStore.put(serializeKey(new Windowed<>("a", windows[0])), serializeValue(5));

        LogCaptureAppender.unregister(appender);

        final Map<MetricName, ? extends Metric> metrics = context.metrics().metrics();

        final Metric dropTotal = metrics.get(new MetricName(
            "expired-window-record-drop-total",
            "stream-metrics-scope-metrics",
            "The total number of occurrence of expired-window-record-drop operations.",
            mkMap(
                mkEntry("client-id", "mock"),
                mkEntry("task-id", "0_0"),
                mkEntry("metrics-scope-id", "bytes-store")
            )
        ));

        final Metric dropRate = metrics.get(new MetricName(
            "expired-window-record-drop-rate",
            "stream-metrics-scope-metrics",
            "The average number of occurrence of expired-window-record-drop operation per second.",
            mkMap(
                mkEntry("client-id", "mock"),
                mkEntry("task-id", "0_0"),
                mkEntry("metrics-scope-id", "bytes-store")
            )
        ));

        assertEquals(1.0, dropTotal.metricValue());
        assertNotEquals(0.0, dropRate.metricValue());
        final List<String> messages = appender.getMessages();
        assertThat(messages, hasItem("Skipping record for expired segment."));
    }

    private Set<String> segmentDirs() {
        final File windowDir = new File(stateDir, storeName);

        return Utils.mkSet(Objects.requireNonNull(windowDir.list()));
    }

    private byte[] serializeValue(final long value) {
        return Serdes.Long().serializer().serialize("", value);
    }

    private Bytes serializeKey(final Windowed<String> key) {
        final StateSerdes<String, Long> stateSerdes = StateSerdes.withBuiltinTypes("dummy", String.class, Long.class);
        if (schema instanceof SessionKeySchema) {
            return Bytes.wrap(SessionKeySchema.toBinary(key, stateSerdes.keySerializer(), "dummy"));
        } else {
            return WindowKeySchema.toStoreKeyBinary(key, 0, stateSerdes);
        }
    }

    private List<KeyValue<Windowed<String>, Long>> toList(final KeyValueIterator<Bytes, byte[]> iterator) {
        final List<KeyValue<Windowed<String>, Long>> results = new ArrayList<>();
        final StateSerdes<String, Long> stateSerdes = StateSerdes.withBuiltinTypes("dummy", String.class, Long.class);
        while (iterator.hasNext()) {
            final KeyValue<Bytes, byte[]> next = iterator.next();
            if (schema instanceof WindowKeySchema) {
                final KeyValue<Windowed<String>, Long> deserialized = KeyValue.pair(
                    WindowKeySchema.fromStoreKey(next.key.get(), windowSizeForTimeWindow, stateSerdes.keyDeserializer(), stateSerdes.topic()),
                    stateSerdes.valueDeserializer().deserialize("dummy", next.value)
                );
                results.add(deserialized);
            } else {
                final KeyValue<Windowed<String>, Long> deserialized = KeyValue.pair(
                    SessionKeySchema.from(next.key.get(), stateSerdes.keyDeserializer(), "dummy"),
                    stateSerdes.valueDeserializer().deserialize("dummy", next.value)
                );
                results.add(deserialized);
            }
        }
        return results;
    }
}
