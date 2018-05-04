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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.NoOpRecordCollector;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SimpleTimeZone;

import static org.apache.kafka.streams.state.internals.Segments.segmentInterval;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

// TODO: this test does not cover time window serdes
public class RocksDBSegmentedBytesStoreTest {

    private final long retention = 60000L;
    private final int numSegments = 3;
    private InternalMockProcessorContext context;
    private final String storeName = "bytes-store";
    private RocksDBSegmentedBytesStore bytesStore;
    private File stateDir;
    private final SessionKeySchema schema = new SessionKeySchema();

    @Before
    public void before() {
        schema.init("topic");
        bytesStore = new RocksDBSegmentedBytesStore(storeName,
                                                    retention,
                                                    numSegments,
                                                    schema);

        stateDir = TestUtils.tempDirectory();
        context = new InternalMockProcessorContext(
            stateDir,
            Serdes.String(),
            Serdes.Long(),
            new NoOpRecordCollector(),
            new ThreadCache(new LogContext("testCache "), 0, new MockStreamsMetrics(new Metrics())));
        bytesStore.init(context, bytesStore);
    }

    @After
    public void close() {
        bytesStore.close();
    }

    @Test
    public void shouldPutAndFetch() {
        final String key = "a";
        bytesStore.put(serializeKey(new Windowed<>(key, new SessionWindow(10, 10L))), serializeValue(10L));
        bytesStore.put(serializeKey(new Windowed<>(key, new SessionWindow(500L, 1000L))), serializeValue(50L));
        bytesStore.put(serializeKey(new Windowed<>(key, new SessionWindow(1500L, 2000L))), serializeValue(100L));
        bytesStore.put(serializeKey(new Windowed<>(key, new SessionWindow(2500L, 3000L))), serializeValue(200L));

        final List<KeyValue<Windowed<String>, Long>> expected = Arrays.asList(KeyValue.pair(new Windowed<>(key, new SessionWindow(10, 10)), 10L),
                                                                                    KeyValue.pair(new Windowed<>(key, new SessionWindow(500, 1000)), 50L));

        final KeyValueIterator<Bytes, byte[]> values = bytesStore.fetch(Bytes.wrap(key.getBytes()), 0, 1000L);
        assertEquals(expected, toList(values));
    }

    @Test
    public void shouldFindValuesWithinRange() {
        final String key = "a";
        bytesStore.put(serializeKey(new Windowed<>(key, new SessionWindow(0L, 0L))), serializeValue(50L));
        bytesStore.put(serializeKey(new Windowed<>(key, new SessionWindow(1000L, 1000L))), serializeValue(10L));
        final KeyValueIterator<Bytes, byte[]> results = bytesStore.fetch(Bytes.wrap(key.getBytes()), 1L, 1999L);
        assertEquals(Collections.singletonList(KeyValue.pair(new Windowed<>(key, new SessionWindow(1000L, 1000L)), 10L)), toList(results));
    }

    @Test
    public void shouldRemove() {
        bytesStore.put(serializeKey(new Windowed<>("a", new SessionWindow(0, 1000))), serializeValue(30L));
        bytesStore.put(serializeKey(new Windowed<>("a", new SessionWindow(1500, 2500))), serializeValue(50L));

        bytesStore.remove(serializeKey(new Windowed<>("a", new SessionWindow(0, 1000))));
        final KeyValueIterator<Bytes, byte[]> value = bytesStore.fetch(Bytes.wrap("a".getBytes()), 0, 1000L);
        assertFalse(value.hasNext());
    }

    @Test
    public void shouldRollSegments() {
        // just to validate directories
        final Segments segments = new Segments(storeName, retention, numSegments);
        final String key = "a";
        bytesStore.put(serializeKey(new Windowed<>(key, new SessionWindow(0L, 0L))), serializeValue(50L));
        assertEquals(Collections.singleton(segments.segmentName(0)), segmentDirs());

        bytesStore.put(serializeKey(new Windowed<>(key, new SessionWindow(30000L, 60000L))), serializeValue(100L));
        assertEquals(Utils.mkSet(segments.segmentName(0),
                                 segments.segmentName(1)), segmentDirs());

        bytesStore.put(serializeKey(new Windowed<>(key, new SessionWindow(61000L, 120000L))), serializeValue(200L));
        assertEquals(Utils.mkSet(segments.segmentName(0),
                                 segments.segmentName(1),
                                 segments.segmentName(2)), segmentDirs());

        bytesStore.put(serializeKey(new Windowed<>(key, new SessionWindow(121000L, 180000L))), serializeValue(300L));
        assertEquals(Utils.mkSet(segments.segmentName(1),
                                 segments.segmentName(2),
                                 segments.segmentName(3)), segmentDirs());

        bytesStore.put(serializeKey(new Windowed<>(key, new SessionWindow(181000L, 240000L))), serializeValue(400L));
        assertEquals(Utils.mkSet(segments.segmentName(2),
                                 segments.segmentName(3),
                                 segments.segmentName(4)), segmentDirs());

        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.fetch(Bytes.wrap(key.getBytes()), 0, 240000));
        assertEquals(Arrays.asList(KeyValue.pair(new Windowed<>(key, new SessionWindow(61000L, 120000L)), 200L),
                                   KeyValue.pair(new Windowed<>(key, new SessionWindow(121000L, 180000L)), 300L),
                                   KeyValue.pair(new Windowed<>(key, new SessionWindow(181000L, 240000L)), 400L)
                                                 ), results);

    }

    @Test
    public void shouldGetAllSegments() {
        // just to validate directories
        final Segments segments = new Segments(storeName, retention, numSegments);
        final String key = "a";
        bytesStore.put(serializeKey(new Windowed<>(key, new SessionWindow(0L, 0L))), serializeValue(50L));
        assertEquals(Collections.singleton(segments.segmentName(0)), segmentDirs());

        bytesStore.put(serializeKey(new Windowed<>(key, new SessionWindow(30000L, 60000L))), serializeValue(100L));
        assertEquals(Utils.mkSet(segments.segmentName(0),
                                 segments.segmentName(1)), segmentDirs());

        bytesStore.put(serializeKey(new Windowed<>(key, new SessionWindow(61000L, 120000L))), serializeValue(200L));
        assertEquals(Utils.mkSet(segments.segmentName(0),
                                 segments.segmentName(1),
                                 segments.segmentName(2)), segmentDirs());

        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.all());
        assertEquals(Arrays.asList(KeyValue.pair(new Windowed<>(key, new SessionWindow(0L, 0L)), 50L),
                                   KeyValue.pair(new Windowed<>(key, new SessionWindow(30000L, 60000L)), 100L),
                                   KeyValue.pair(new Windowed<>(key, new SessionWindow(61000L, 120000L)), 200L)
                                                 ), results);

    }

    @Test
    public void shouldFetchAllSegments() {
        // just to validate directories
        final Segments segments = new Segments(storeName, retention, numSegments);
        final String key = "a";
        bytesStore.put(serializeKey(new Windowed<>(key, new SessionWindow(0L, 0L))), serializeValue(50L));
        assertEquals(Collections.singleton(segments.segmentName(0)), segmentDirs());

        bytesStore.put(serializeKey(new Windowed<>(key, new SessionWindow(30000L, 60000L))), serializeValue(100L));
        assertEquals(Utils.mkSet(segments.segmentName(0),
                                 segments.segmentName(1)), segmentDirs());

        bytesStore.put(serializeKey(new Windowed<>(key, new SessionWindow(61000L, 120000L))), serializeValue(200L));
        assertEquals(Utils.mkSet(segments.segmentName(0),
                                 segments.segmentName(1),
                                 segments.segmentName(2)), segmentDirs());

        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.fetchAll(0L, 60000L));
        assertEquals(Arrays.asList(KeyValue.pair(new Windowed<>(key, new SessionWindow(0L, 0L)), 50L),
                                   KeyValue.pair(new Windowed<>(key, new SessionWindow(30000L, 60000L)), 100L)
                                                 ), results);

    }

    @Test
    public void shouldLoadSegementsWithOldStyleDateFormattedName() {
        final Segments segments = new Segments(storeName, retention, numSegments);
        final String key = "a";
        bytesStore.put(serializeKey(new Windowed<>(key, new SessionWindow(0L, 0L))), serializeValue(50L));
        bytesStore.put(serializeKey(new Windowed<>(key, new SessionWindow(30000L, 60000L))), serializeValue(100L));
        bytesStore.close();

        final String firstSegmentName = segments.segmentName(0);
        final String[] nameParts = firstSegmentName.split("\\.");
        final Long segmentId = Long.parseLong(nameParts[1]);
        final SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
        formatter.setTimeZone(new SimpleTimeZone(0, "UTC"));
        final String formatted = formatter.format(new Date(segmentId * segmentInterval(retention, numSegments)));
        final File parent = new File(stateDir, storeName);
        final File oldStyleName = new File(parent, nameParts[0] + "-" + formatted);
        assertTrue(new File(parent, firstSegmentName).renameTo(oldStyleName));

        bytesStore = new RocksDBSegmentedBytesStore(storeName,
                                                    retention,
                                                    numSegments,
                                                    schema);

        bytesStore.init(context, bytesStore);
        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.fetch(Bytes.wrap(key.getBytes()), 0L, 60000L));
        assertThat(results, equalTo(Arrays.asList(KeyValue.pair(new Windowed<>(key, new SessionWindow(0L, 0L)), 50L),
                                                  KeyValue.pair(new Windowed<>(key, new SessionWindow(30000L, 60000L)), 100L))));
    }

    @Test
    public void shouldLoadSegementsWithOldStyleColonFormattedName() {
        final Segments segments = new Segments(storeName, retention, numSegments);
        final String key = "a";
        bytesStore.put(serializeKey(new Windowed<>(key, new SessionWindow(0L, 0L))), serializeValue(50L));
        bytesStore.put(serializeKey(new Windowed<>(key, new SessionWindow(30000L, 60000L))), serializeValue(100L));
        bytesStore.close();

        final String firstSegmentName = segments.segmentName(0);
        final String[] nameParts = firstSegmentName.split("\\.");
        final File parent = new File(stateDir, storeName);
        final File oldStyleName = new File(parent, nameParts[0] + ":" + Long.parseLong(nameParts[1]));
        assertTrue(new File(parent, firstSegmentName).renameTo(oldStyleName));

        bytesStore = new RocksDBSegmentedBytesStore(storeName,
            retention,
            numSegments,
            schema);

        bytesStore.init(context, bytesStore);
        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.fetch(Bytes.wrap(key.getBytes()), 0L, 60000L));
        assertThat(results, equalTo(Arrays.asList(KeyValue.pair(new Windowed<>(key, new SessionWindow(0L, 0L)), 50L),
            KeyValue.pair(new Windowed<>(key, new SessionWindow(30000L, 60000L)), 100L))));
    }

    @Test
    public void shouldBeAbleToWriteToReInitializedStore() {
        final String key = "a";
        // need to create a segment so we can attempt to write to it again.
        bytesStore.put(serializeKey(new Windowed<>(key, new SessionWindow(0L, 0L))), serializeValue(50L));
        bytesStore.close();
        bytesStore.init(context, bytesStore);
        bytesStore.put(serializeKey(new Windowed<>(key, new SessionWindow(0L, 0L))), serializeValue(50L));
    }

    private Set<String> segmentDirs() {
        File windowDir = new File(stateDir, storeName);

        return new HashSet<>(Arrays.asList(windowDir.list()));
    }

    private byte[] serializeValue(final long value) {
        return Serdes.Long().serializer().serialize("", value);
    }

    private Bytes serializeKey(final Windowed<String> key) {
        return Bytes.wrap(SessionKeySchema.toBinary(key, Serdes.String().serializer(), "dummy"));
    }

    private List<KeyValue<Windowed<String>, Long>> toList(final KeyValueIterator<Bytes, byte[]> iterator) {
        final List<KeyValue<Windowed<String>, Long>> results = new ArrayList<>();
        while (iterator.hasNext()) {
            final KeyValue<Bytes, byte[]> next = iterator.next();
            final KeyValue<Windowed<String>, Long> deserialized = KeyValue.pair(
                    SessionKeySchema.from(next.key.get(), Serdes.String().deserializer(), "dummy"),
                    Serdes.Long().deserializer().deserialize("dummy", next.value)
            );
            results.add(deserialized);
        }
        return results;
    }

}
