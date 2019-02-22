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

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.DefaultProductionExceptionHandler;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.processor.internals.RecordCollectorImpl;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.time.Duration.ofMillis;
import static java.time.Instant.ofEpochMilli;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("PointlessArithmeticExpression")
public class RocksDBWindowStoreTest {

    private static final long DEFAULT_CACHE_SIZE_BYTES = 1024 * 1024L;

    private final int numSegments = 3;
    private final long windowSize = 3L;
    private final long segmentInterval = 60_000L;
    private final long retentionPeriod = segmentInterval * (numSegments - 1);
    private final String windowName = "window";
    private final KeyValueSegments segments = new KeyValueSegments(windowName, retentionPeriod, segmentInterval);
    private final StateSerdes<Integer, String> serdes = new StateSerdes<>("", Serdes.Integer(), Serdes.String());

    private final List<KeyValue<byte[], byte[]>> changeLog = new ArrayList<>();
    private final ThreadCache cache = new ThreadCache(
        new LogContext("TestCache "),
        DEFAULT_CACHE_SIZE_BYTES,
        new MockStreamsMetrics(new Metrics()));

    private final Producer<byte[], byte[]> producer =
        new MockProducer<>(true, Serdes.ByteArray().serializer(), Serdes.ByteArray().serializer());
    private final RecordCollector recordCollector = new RecordCollectorImpl(
        "RocksDBWindowStoreTestTask",
        new LogContext("RocksDBWindowStoreTestTask "),
        new DefaultProductionExceptionHandler(),
        new Metrics().sensor("skipped-records")
    ) {
        @Override
        public <K1, V1> void send(final String topic,
                                  final K1 key,
                                  final V1 value,
                                  final Headers headers,
                                  final Integer partition,
                                  final Long timestamp,
                                  final Serializer<K1> keySerializer,
                                  final Serializer<V1> valueSerializer) {
            changeLog.add(new KeyValue<>(
                keySerializer.serialize(topic, headers, key),
                valueSerializer.serialize(topic, headers, value))
            );
        }
    };

    private final File baseDir = TestUtils.tempDirectory("test");
    private final InternalMockProcessorContext context =
        new InternalMockProcessorContext(baseDir, Serdes.ByteArray(), Serdes.ByteArray(), recordCollector, cache);
    private WindowStore<Integer, String> windowStore;

    private WindowStore<Integer, String> createWindowStore(final ProcessorContext context, final boolean retainDuplicates) {
        final WindowStore<Integer, String> store = Stores.windowStoreBuilder(
            Stores.persistentWindowStore(
                windowName,
                ofMillis(retentionPeriod),
                ofMillis(windowSize),
                retainDuplicates),
            Serdes.Integer(),
            Serdes.String()).build();

        store.init(context, store);
        return store;
    }

    @Before
    public void initRecordCollector() {
        recordCollector.init(producer);
    }

    @After
    public void closeStore() {
        if (windowStore != null) {
            windowStore.close();
        }
    }

    @Test
    public void shouldOnlyIterateOpenSegments() {
        windowStore = createWindowStore(context, false);
        long currentTime = 0;
        setCurrentTime(currentTime);
        windowStore.put(1, "one");

        currentTime = currentTime + segmentInterval;
        setCurrentTime(currentTime);
        windowStore.put(1, "two");
        currentTime = currentTime + segmentInterval;

        setCurrentTime(currentTime);
        windowStore.put(1, "three");

        final WindowStoreIterator<String> iterator = windowStore.fetch(1, ofEpochMilli(0), ofEpochMilli(currentTime));

        // roll to the next segment that will close the first
        currentTime = currentTime + segmentInterval;
        setCurrentTime(currentTime);
        windowStore.put(1, "four");

        // should only have 2 values as the first segment is no longer open
        assertEquals(new KeyValue<>(segmentInterval, "two"), iterator.next());
        assertEquals(new KeyValue<>(2 * segmentInterval, "three"), iterator.next());
        assertFalse(iterator.hasNext());
    }

    private void setCurrentTime(final long currentTime) {
        context.setRecordContext(createRecordContext(currentTime));
    }

    private ProcessorRecordContext createRecordContext(final long time) {
        return new ProcessorRecordContext(time, 0, 0, "topic", null);
    }

    @Test
    public void testRangeAndSinglePointFetch() {
        windowStore = createWindowStore(context, false);
        final long startTime = segmentInterval - 4L;

        putFirstBatch(windowStore, startTime, context);

        assertEquals("zero", windowStore.fetch(0, startTime));
        assertEquals("one", windowStore.fetch(1, startTime + 1L));
        assertEquals("two", windowStore.fetch(2, startTime + 2L));
        assertEquals("four", windowStore.fetch(4, startTime + 4L));
        assertEquals("five", windowStore.fetch(5, startTime + 5L));

        assertEquals(
            Collections.singletonList("zero"),
            toList(windowStore.fetch(
                0,
                ofEpochMilli(startTime + 0 - windowSize),
                ofEpochMilli(startTime + 0 + windowSize))));

        putSecondBatch(windowStore, startTime, context);

        assertEquals("two+1", windowStore.fetch(2, startTime + 3L));
        assertEquals("two+2", windowStore.fetch(2, startTime + 4L));
        assertEquals("two+3", windowStore.fetch(2, startTime + 5L));
        assertEquals("two+4", windowStore.fetch(2, startTime + 6L));
        assertEquals("two+5", windowStore.fetch(2, startTime + 7L));
        assertEquals("two+6", windowStore.fetch(2, startTime + 8L));

        assertEquals(
            Collections.emptyList(),
            toList(windowStore.fetch(
                2,
                ofEpochMilli(startTime - 2L - windowSize),
                ofEpochMilli(startTime - 2L + windowSize))));
        assertEquals(
            Collections.singletonList("two"),
            toList(windowStore.fetch(
                2,
                ofEpochMilli(startTime - 1L - windowSize),
                ofEpochMilli(startTime - 1L + windowSize))));
        assertEquals(
            asList("two", "two+1"),
            toList(windowStore.fetch(
                2,
                ofEpochMilli(startTime - windowSize),
                ofEpochMilli(startTime + windowSize))));
        assertEquals(
            asList("two", "two+1", "two+2"),
            toList(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 1L - windowSize),
                ofEpochMilli(startTime + 1L + windowSize))));
        assertEquals(
            asList("two", "two+1", "two+2", "two+3"),
            toList(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 2L - windowSize),
                ofEpochMilli(startTime + 2L + windowSize))));
        assertEquals(
            asList("two", "two+1", "two+2", "two+3", "two+4"),
            toList(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 3L - windowSize),
                ofEpochMilli(startTime + 3L + windowSize))));
        assertEquals(
            asList("two", "two+1", "two+2", "two+3", "two+4", "two+5"),
            toList(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 4L - windowSize),
                ofEpochMilli(startTime + 4L + windowSize))));
        assertEquals(
            asList("two", "two+1", "two+2", "two+3", "two+4", "two+5", "two+6"),
            toList(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 5L - windowSize),
                ofEpochMilli(startTime + 5L + windowSize))));
        assertEquals(
            asList("two+1", "two+2", "two+3", "two+4", "two+5", "two+6"),
            toList(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 6L - windowSize),
                ofEpochMilli(startTime + 6L + windowSize))));
        assertEquals(
            asList("two+2", "two+3", "two+4", "two+5", "two+6"),
            toList(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 7L - windowSize),
                ofEpochMilli(startTime + 7L + windowSize))));
        assertEquals(
            asList("two+3", "two+4", "two+5", "two+6"),
            toList(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 8L - windowSize),
                ofEpochMilli(startTime + 8L + windowSize))));
        assertEquals(
            asList("two+4", "two+5", "two+6"),
            toList(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 9L - windowSize),
                ofEpochMilli(startTime + 9L + windowSize))));
        assertEquals(
            asList("two+5", "two+6"),
            toList(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 10L - windowSize),
                ofEpochMilli(startTime + 10L + windowSize))));
        assertEquals(
            Collections.singletonList("two+6"),
            toList(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 11L - windowSize),
                ofEpochMilli(startTime + 11L + windowSize))));
        assertEquals(
            Collections.emptyList(),
            toList(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 12L - windowSize),
                ofEpochMilli(startTime + 12L + windowSize))));

        // Flush the store and verify all current entries were properly flushed ...
        windowStore.flush();

        final Map<Integer, Set<String>> entriesByKey = entriesByKey(changeLog, startTime);

        assertEquals(Utils.mkSet("zero@0"), entriesByKey.get(0));
        assertEquals(Utils.mkSet("one@1"), entriesByKey.get(1));
        assertEquals(Utils.mkSet("two@2", "two+1@3", "two+2@4", "two+3@5", "two+4@6", "two+5@7", "two+6@8"), entriesByKey.get(2));
        assertNull(entriesByKey.get(3));
        assertEquals(Utils.mkSet("four@4"), entriesByKey.get(4));
        assertEquals(Utils.mkSet("five@5"), entriesByKey.get(5));
        assertNull(entriesByKey.get(6));
    }

    @Test
    public void shouldGetAll() {
        windowStore = createWindowStore(context, false);
        final long startTime = segmentInterval - 4L;

        putFirstBatch(windowStore, startTime, context);

        final KeyValue<Windowed<Integer>, String> zero = windowedPair(0, "zero", startTime + 0);
        final KeyValue<Windowed<Integer>, String> one = windowedPair(1, "one", startTime + 1);
        final KeyValue<Windowed<Integer>, String> two = windowedPair(2, "two", startTime + 2);
        final KeyValue<Windowed<Integer>, String> four = windowedPair(4, "four", startTime + 4);
        final KeyValue<Windowed<Integer>, String> five = windowedPair(5, "five", startTime + 5);

        assertEquals(
            asList(zero, one, two, four, five),
            StreamsTestUtils.toList(windowStore.all())
        );
    }

    @Test
    public void shouldFetchAllInTimeRange() {
        windowStore = createWindowStore(context, false);
        final long startTime = segmentInterval - 4L;

        putFirstBatch(windowStore, startTime, context);

        final KeyValue<Windowed<Integer>, String> zero = windowedPair(0, "zero", startTime + 0);
        final KeyValue<Windowed<Integer>, String> one = windowedPair(1, "one", startTime + 1);
        final KeyValue<Windowed<Integer>, String> two = windowedPair(2, "two", startTime + 2);
        final KeyValue<Windowed<Integer>, String> four = windowedPair(4, "four", startTime + 4);
        final KeyValue<Windowed<Integer>, String> five = windowedPair(5, "five", startTime + 5);

        assertEquals(
            asList(one, two, four),
            StreamsTestUtils.toList(windowStore.fetchAll(ofEpochMilli(startTime + 1), ofEpochMilli(startTime + 4)))
        );
        assertEquals(
            asList(zero, one, two),
            StreamsTestUtils.toList(windowStore.fetchAll(ofEpochMilli(startTime + 0), ofEpochMilli(startTime + 3)))
        );
        assertEquals(
            asList(one, two, four, five),
            StreamsTestUtils.toList(windowStore.fetchAll(ofEpochMilli(startTime + 1), ofEpochMilli(startTime + 5)))
        );
    }

    @Test
    public void testFetchRange() {
        windowStore = createWindowStore(context, false);
        final long startTime = segmentInterval - 4L;

        putFirstBatch(windowStore, startTime, context);

        final KeyValue<Windowed<Integer>, String> zero = windowedPair(0, "zero", startTime + 0);
        final KeyValue<Windowed<Integer>, String> one = windowedPair(1, "one", startTime + 1);
        final KeyValue<Windowed<Integer>, String> two = windowedPair(2, "two", startTime + 2);
        final KeyValue<Windowed<Integer>, String> four = windowedPair(4, "four", startTime + 4);
        final KeyValue<Windowed<Integer>, String> five = windowedPair(5, "five", startTime + 5);

        assertEquals(
            asList(zero, one),
            StreamsTestUtils.toList(windowStore.fetch(
                0,
                1,
                ofEpochMilli(startTime + 0L - windowSize),
                ofEpochMilli(startTime + 0L + windowSize)))
        );
        assertEquals(
            Collections.singletonList(one),
            StreamsTestUtils.toList(windowStore.fetch(
                1,
                1,
                ofEpochMilli(startTime + 0L - windowSize),
                ofEpochMilli(startTime + 0L + windowSize)))
        );
        assertEquals(
            asList(one, two),
            StreamsTestUtils.toList(windowStore.fetch(
                1,
                3,
                ofEpochMilli(startTime + 0L - windowSize),
                ofEpochMilli(startTime + 0L + windowSize)))
        );
        assertEquals(
            asList(zero, one, two),
            StreamsTestUtils.toList(windowStore.fetch(
                0,
                5,
                ofEpochMilli(startTime + 0L - windowSize),
                ofEpochMilli(startTime + 0L + windowSize)))
        );
        assertEquals(
            asList(zero, one, two, four, five),
            StreamsTestUtils.toList(windowStore.fetch(
                0,
                5,
                ofEpochMilli(startTime + 0L - windowSize),
                ofEpochMilli(startTime + 0L + windowSize + 5L)))
        );
        assertEquals(
            asList(two, four, five),
            StreamsTestUtils.toList(windowStore.fetch(
                0,
                5,
                ofEpochMilli(startTime + 2L),
                ofEpochMilli(startTime + 0L + windowSize + 5L)))
        );
        assertEquals(
            Collections.emptyList(),
            StreamsTestUtils.toList(windowStore.fetch(
                4,
                5,
                ofEpochMilli(startTime + 2L),
                ofEpochMilli(startTime + windowSize)))
        );
        assertEquals(
            Collections.emptyList(),
            StreamsTestUtils.toList(windowStore.fetch(
                0,
                3,
                ofEpochMilli(startTime + 3L),
                ofEpochMilli(startTime + windowSize + 5)))
        );
    }

    @Test
    public void testPutAndFetchBefore() {
        windowStore = createWindowStore(context, false);
        final long startTime = segmentInterval - 4L;

        putFirstBatch(windowStore, startTime, context);

        assertEquals(
            Collections.singletonList("zero"),
            toList(windowStore.fetch(
                0,
                ofEpochMilli(startTime + 0L - windowSize),
                ofEpochMilli(startTime + 0L))));
        assertEquals(
            Collections.singletonList("one"),
            toList(windowStore.fetch(
                1,
                ofEpochMilli(startTime + 1L - windowSize),
                ofEpochMilli(startTime + 1L))));
        assertEquals(
            Collections.singletonList("two"),
            toList(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 2L - windowSize),
                ofEpochMilli(startTime + 2L))));
        assertEquals(
            Collections.emptyList(),
            toList(windowStore.fetch(
                3,
                ofEpochMilli(startTime + 3L - windowSize),
                ofEpochMilli(startTime + 3L))));
        assertEquals(
            Collections.singletonList("four"),
            toList(windowStore.fetch(
                4,
                ofEpochMilli(startTime + 4L - windowSize),
                ofEpochMilli(startTime + 4L))));
        assertEquals(
            Collections.singletonList("five"),
            toList(windowStore.fetch(
                5,
                ofEpochMilli(startTime + 5L - windowSize),
                ofEpochMilli(startTime + 5L))));

        putSecondBatch(windowStore, startTime, context);

        assertEquals(
            Collections.emptyList(),
            toList(windowStore.fetch(
                2,
                ofEpochMilli(startTime - 1L - windowSize),
                ofEpochMilli(startTime - 1L))));
        assertEquals(
            Collections.emptyList(),
            toList(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 0L - windowSize),
                ofEpochMilli(startTime + 0L))));
        assertEquals(
            Collections.emptyList(),
            toList(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 1L - windowSize),
                ofEpochMilli(startTime + 1L))));
        assertEquals(
            Collections.singletonList("two"),
            toList(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 2L - windowSize),
                ofEpochMilli(startTime + 2L))));
        assertEquals(
            asList("two", "two+1"),
            toList(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 3L - windowSize),
                ofEpochMilli(startTime + 3L))));
        assertEquals(
            asList("two", "two+1", "two+2"),
            toList(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 4L - windowSize),
                ofEpochMilli(startTime + 4L))));
        assertEquals(
            asList("two", "two+1", "two+2", "two+3"),
            toList(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 5L - windowSize),
                ofEpochMilli(startTime + 5L))));
        assertEquals(
            asList("two+1", "two+2", "two+3", "two+4"),
            toList(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 6L - windowSize),
                ofEpochMilli(startTime + 6L))));
        assertEquals(
            asList("two+2", "two+3", "two+4", "two+5"),
            toList(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 7L - windowSize),
                ofEpochMilli(startTime + 7L))));
        assertEquals(
            asList("two+3", "two+4", "two+5", "two+6"),
            toList(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 8L - windowSize),
                ofEpochMilli(startTime + 8L))));
        assertEquals(
            asList("two+4", "two+5", "two+6"),
            toList(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 9L - windowSize),
                ofEpochMilli(startTime + 9L))));
        assertEquals(
            asList("two+5", "two+6"),
            toList(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 10L - windowSize),
                ofEpochMilli(startTime + 10L))));
        assertEquals(
            Collections.singletonList("two+6"),
            toList(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 11L - windowSize),
                ofEpochMilli(startTime + 11L))));
        assertEquals(
            Collections.emptyList(),
            toList(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 12L - windowSize),
                ofEpochMilli(startTime + 12L))));
        assertEquals(
            Collections.emptyList(),
            toList(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 13L - windowSize),
                ofEpochMilli(startTime + 13L))));

        // Flush the store and verify all current entries were properly flushed ...
        windowStore.flush();

        final Map<Integer, Set<String>> entriesByKey = entriesByKey(changeLog, startTime);
        assertEquals(Utils.mkSet("zero@0"), entriesByKey.get(0));
        assertEquals(Utils.mkSet("one@1"), entriesByKey.get(1));
        assertEquals(
            Utils.mkSet("two@2", "two+1@3", "two+2@4", "two+3@5", "two+4@6", "two+5@7", "two+6@8"),
            entriesByKey.get(2));
        assertNull(entriesByKey.get(3));
        assertEquals(Utils.mkSet("four@4"), entriesByKey.get(4));
        assertEquals(Utils.mkSet("five@5"), entriesByKey.get(5));
        assertNull(entriesByKey.get(6));
    }

    @Test
    public void testPutAndFetchAfter() {
        windowStore = createWindowStore(context, false);
        final long startTime = segmentInterval - 4L;

        putFirstBatch(windowStore, startTime, context);

        assertEquals(
            Collections.singletonList("zero"),
            toList(windowStore.fetch(0, ofEpochMilli(startTime + 0L), ofEpochMilli(startTime + 0L + windowSize))));
        assertEquals(
            Collections.singletonList("one"),
            toList(windowStore.fetch(1, ofEpochMilli(startTime + 1L), ofEpochMilli(startTime + 1L + windowSize))));
        assertEquals(
            Collections.singletonList("two"),
            toList(windowStore.fetch(2, ofEpochMilli(startTime + 2L), ofEpochMilli(startTime + 2L + windowSize))));
        assertEquals(
            Collections.emptyList(),
            toList(windowStore.fetch(3, ofEpochMilli(startTime + 3L), ofEpochMilli(startTime + 3L + windowSize))));
        assertEquals(
            Collections.singletonList("four"),
            toList(windowStore.fetch(4, ofEpochMilli(startTime + 4L), ofEpochMilli(startTime + 4L + windowSize))));
        assertEquals(
            Collections.singletonList("five"),
            toList(windowStore.fetch(5, ofEpochMilli(startTime + 5L), ofEpochMilli(startTime + 5L + windowSize))));

        putSecondBatch(windowStore, startTime, context);

        assertEquals(
            Collections.emptyList(),
            toList(windowStore.fetch(2, ofEpochMilli(startTime - 2L), ofEpochMilli(startTime - 2L + windowSize))));
        assertEquals(
            Collections.singletonList("two"),
            toList(windowStore.fetch(2, ofEpochMilli(startTime - 1L), ofEpochMilli(startTime - 1L + windowSize))));
        assertEquals(
            asList("two", "two+1"),
            toList(windowStore.fetch(2, ofEpochMilli(startTime), ofEpochMilli(startTime + windowSize))));
        assertEquals(
            asList("two", "two+1", "two+2"),
            toList(windowStore.fetch(2, ofEpochMilli(startTime + 1L), ofEpochMilli(startTime + 1L + windowSize))));
        assertEquals(
            asList("two", "two+1", "two+2", "two+3"),
            toList(windowStore.fetch(2, ofEpochMilli(startTime + 2L), ofEpochMilli(startTime + 2L + windowSize))));
        assertEquals(
            asList("two+1", "two+2", "two+3", "two+4"),
            toList(windowStore.fetch(2, ofEpochMilli(startTime + 3L), ofEpochMilli(startTime + 3L + windowSize))));
        assertEquals(
            asList("two+2", "two+3", "two+4", "two+5"),
            toList(windowStore.fetch(2, ofEpochMilli(startTime + 4L), ofEpochMilli(startTime + 4L + windowSize))));
        assertEquals(
            asList("two+3", "two+4", "two+5", "two+6"),
            toList(windowStore.fetch(2, ofEpochMilli(startTime + 5L), ofEpochMilli(startTime + 5L + windowSize))));
        assertEquals(
            asList("two+4", "two+5", "two+6"),
            toList(windowStore.fetch(2, ofEpochMilli(startTime + 6L), ofEpochMilli(startTime + 6L + windowSize))));
        assertEquals(
            asList("two+5", "two+6"),
            toList(windowStore.fetch(2, ofEpochMilli(startTime + 7L), ofEpochMilli(startTime + 7L + windowSize))));
        assertEquals(
            Collections.singletonList("two+6"),
            toList(windowStore.fetch(2, ofEpochMilli(startTime + 8L), ofEpochMilli(startTime + 8L + windowSize))));
        assertEquals(
            Collections.emptyList(),
            toList(windowStore.fetch(2, ofEpochMilli(startTime + 9L), ofEpochMilli(startTime + 9L + windowSize))));
        assertEquals(
            Collections.emptyList(),
            toList(windowStore.fetch(2, ofEpochMilli(startTime + 10L), ofEpochMilli(startTime + 10L + windowSize))));
        assertEquals(
            Collections.emptyList(),
            toList(windowStore.fetch(2, ofEpochMilli(startTime + 11L), ofEpochMilli(startTime + 11L + windowSize))));
        assertEquals(
            Collections.emptyList(),
            toList(windowStore.fetch(2, ofEpochMilli(startTime + 12L), ofEpochMilli(startTime + 12L + windowSize))));

        // Flush the store and verify all current entries were properly flushed ...
        windowStore.flush();

        final Map<Integer, Set<String>> entriesByKey = entriesByKey(changeLog, startTime);

        assertEquals(Utils.mkSet("zero@0"), entriesByKey.get(0));
        assertEquals(Utils.mkSet("one@1"), entriesByKey.get(1));
        assertEquals(
            Utils.mkSet("two@2", "two+1@3", "two+2@4", "two+3@5", "two+4@6", "two+5@7", "two+6@8"),
            entriesByKey.get(2));
        assertNull(entriesByKey.get(3));
        assertEquals(Utils.mkSet("four@4"), entriesByKey.get(4));
        assertEquals(Utils.mkSet("five@5"), entriesByKey.get(5));
        assertNull(entriesByKey.get(6));
    }

    @Test
    public void testPutSameKeyTimestamp() {
        windowStore = createWindowStore(context, true);
        final long startTime = segmentInterval - 4L;

        setCurrentTime(startTime);
        windowStore.put(0, "zero");

        assertEquals(
            Collections.singletonList("zero"),
            toList(windowStore.fetch(0, ofEpochMilli(startTime - windowSize), ofEpochMilli(startTime + windowSize))));

        windowStore.put(0, "zero");
        windowStore.put(0, "zero+");
        windowStore.put(0, "zero++");

        assertEquals(
            asList("zero", "zero", "zero+", "zero++"),
            toList(windowStore.fetch(
                0,
                ofEpochMilli(startTime - windowSize),
                ofEpochMilli(startTime + windowSize))));
        assertEquals(
            asList("zero", "zero", "zero+", "zero++"),
            toList(windowStore.fetch(
                0,
                ofEpochMilli(startTime + 1L - windowSize),
                ofEpochMilli(startTime + 1L + windowSize))));
        assertEquals(
            asList("zero", "zero", "zero+", "zero++"),
            toList(windowStore.fetch(
                0,
                ofEpochMilli(startTime + 2L - windowSize),
                ofEpochMilli(startTime + 2L + windowSize))));
        assertEquals(
            asList("zero", "zero", "zero+", "zero++"),
            toList(windowStore.fetch(
                0,
                ofEpochMilli(startTime + 3L - windowSize),
                ofEpochMilli(startTime + 3L + windowSize))));
        assertEquals(
            Collections.emptyList(),
            toList(windowStore.fetch(
                0,
                ofEpochMilli(startTime + 4L - windowSize),
                ofEpochMilli(startTime + 4L + windowSize))));

        // Flush the store and verify all current entries were properly flushed ...
        windowStore.flush();

        final Map<Integer, Set<String>> entriesByKey = entriesByKey(changeLog, startTime);

        assertEquals(Utils.mkSet("zero@0", "zero@0", "zero+@0", "zero++@0"), entriesByKey.get(0));
    }

    @Test
    public void testRolling() {
        windowStore = createWindowStore(context, false);

        // to validate segments
        final long startTime = segmentInterval * 2;
        final long increment = segmentInterval / 2;
        setCurrentTime(startTime);
        windowStore.put(0, "zero");
        assertEquals(Utils.mkSet(segments.segmentName(2)), segmentDirs(baseDir));

        setCurrentTime(startTime + increment);
        windowStore.put(1, "one");
        assertEquals(Utils.mkSet(segments.segmentName(2)), segmentDirs(baseDir));

        setCurrentTime(startTime + increment * 2);
        windowStore.put(2, "two");
        assertEquals(
            Utils.mkSet(
                segments.segmentName(2),
                segments.segmentName(3)
            ),
            segmentDirs(baseDir)
        );

        setCurrentTime(startTime + increment * 4);
        windowStore.put(4, "four");
        assertEquals(
            Utils.mkSet(
                segments.segmentName(2),
                segments.segmentName(3),
                segments.segmentName(4)
            ),
            segmentDirs(baseDir)
        );

        setCurrentTime(startTime + increment * 5);
        windowStore.put(5, "five");
        assertEquals(
            Utils.mkSet(
                segments.segmentName(2),
                segments.segmentName(3),
                segments.segmentName(4)
            ),
            segmentDirs(baseDir)
        );

        assertEquals(
            Collections.singletonList("zero"),
            toList(windowStore.fetch(
                0,
                ofEpochMilli(startTime - windowSize),
                ofEpochMilli(startTime + windowSize))));
        assertEquals(
            Collections.singletonList("one"),
            toList(windowStore.fetch(
                1,
                ofEpochMilli(startTime + increment - windowSize),
                ofEpochMilli(startTime + increment + windowSize))));
        assertEquals(
            Collections.singletonList("two"),
            toList(windowStore.fetch(
                2,
                ofEpochMilli(startTime + increment * 2 - windowSize),
                ofEpochMilli(startTime + increment * 2 + windowSize))));
        assertEquals(
            Collections.emptyList(),
            toList(windowStore.fetch(
                3,
                ofEpochMilli(startTime + increment * 3 - windowSize),
                ofEpochMilli(startTime + increment * 3 + windowSize))));
        assertEquals(
            Collections.singletonList("four"),
            toList(windowStore.fetch(
                4,
                ofEpochMilli(startTime + increment * 4 - windowSize),
                ofEpochMilli(startTime + increment * 4 + windowSize))));
        assertEquals(
            Collections.singletonList("five"),
            toList(windowStore.fetch(
                5,
                ofEpochMilli(startTime + increment * 5 - windowSize),
                ofEpochMilli(startTime + increment * 5 + windowSize))));

        setCurrentTime(startTime + increment * 6);
        windowStore.put(6, "six");
        assertEquals(
            Utils.mkSet(
                segments.segmentName(3),
                segments.segmentName(4),
                segments.segmentName(5)
            ),
            segmentDirs(baseDir)
        );

        assertEquals(
            Collections.emptyList(),
            toList(windowStore.fetch(
                0,
                ofEpochMilli(startTime - windowSize),
                ofEpochMilli(startTime + windowSize))));
        assertEquals(
            Collections.emptyList(),
            toList(windowStore.fetch(
                1,
                ofEpochMilli(startTime + increment - windowSize),
                ofEpochMilli(startTime + increment + windowSize))));
        assertEquals(
            Collections.singletonList("two"),
            toList(windowStore.fetch(
                2,
                ofEpochMilli(startTime + increment * 2 - windowSize),
                ofEpochMilli(startTime + increment * 2 + windowSize))));
        assertEquals(
            Collections.emptyList(),
            toList(windowStore.fetch(
                3,
                ofEpochMilli(startTime + increment * 3 - windowSize),
                ofEpochMilli(startTime + increment * 3 + windowSize))));
        assertEquals(Collections.singletonList("four"),
            toList(windowStore.fetch(
                4,
                ofEpochMilli(startTime + increment * 4 - windowSize),
                ofEpochMilli(startTime + increment * 4 + windowSize))));
        assertEquals(
            Collections.singletonList("five"),
            toList(windowStore.fetch(
                5,
                ofEpochMilli(startTime + increment * 5 - windowSize),
                ofEpochMilli(startTime + increment * 5 + windowSize))));
        assertEquals(
            Collections.singletonList("six"),
            toList(windowStore.fetch(
                6,
                ofEpochMilli(startTime + increment * 6 - windowSize),
                ofEpochMilli(startTime + increment * 6 + windowSize))));

        setCurrentTime(startTime + increment * 7);
        windowStore.put(7, "seven");
        assertEquals(
            Utils.mkSet(
                segments.segmentName(3),
                segments.segmentName(4),
                segments.segmentName(5)
            ),
            segmentDirs(baseDir)
        );

        assertEquals(
            Collections.emptyList(),
            toList(windowStore.fetch(
                0,
                ofEpochMilli(startTime - windowSize),
                ofEpochMilli(startTime + windowSize))));
        assertEquals(
            Collections.emptyList(),
            toList(windowStore.fetch(
                1,
                ofEpochMilli(startTime + increment - windowSize),
                ofEpochMilli(startTime + increment + windowSize))));
        assertEquals(
            Collections.singletonList("two"),
            toList(windowStore.fetch(
                2,
                ofEpochMilli(startTime + increment * 2 - windowSize),
                ofEpochMilli(startTime + increment * 2 + windowSize))));
        assertEquals(
            Collections.emptyList(),
            toList(windowStore.fetch(
                3,
                ofEpochMilli(startTime + increment * 3 - windowSize),
                ofEpochMilli(startTime + increment * 3 + windowSize))));
        assertEquals(
            Collections.singletonList("four"),
            toList(windowStore.fetch(
                4,
                ofEpochMilli(startTime + increment * 4 - windowSize),
                ofEpochMilli(startTime + increment * 4 + windowSize))));
        assertEquals(
            Collections.singletonList("five"),
            toList(windowStore.fetch(
                5,
                ofEpochMilli(startTime + increment * 5 - windowSize),
                ofEpochMilli(startTime + increment * 5 + windowSize))));
        assertEquals(
            Collections.singletonList("six"),
            toList(windowStore.fetch(
                6,
                ofEpochMilli(startTime + increment * 6 - windowSize),
                ofEpochMilli(startTime + increment * 6 + windowSize))));
        assertEquals(
            Collections.singletonList("seven"),
            toList(windowStore.fetch(
                7,
                ofEpochMilli(startTime + increment * 7 - windowSize),
                ofEpochMilli(startTime + increment * 7 + windowSize))));

        setCurrentTime(startTime + increment * 8);
        windowStore.put(8, "eight");
        assertEquals(
            Utils.mkSet(
                segments.segmentName(4),
                segments.segmentName(5),
                segments.segmentName(6)
            ),
            segmentDirs(baseDir)
        );

        assertEquals(
            Collections.emptyList(),
            toList(windowStore.fetch(
                0,
                ofEpochMilli(startTime - windowSize),
                ofEpochMilli(startTime + windowSize))));
        assertEquals(
            Collections.emptyList(),
            toList(windowStore.fetch(
                1,
                ofEpochMilli(startTime + increment - windowSize),
                ofEpochMilli(startTime + increment + windowSize))));
        assertEquals(
            Collections.emptyList(),
            toList(windowStore.fetch(
                2,
                ofEpochMilli(startTime + increment * 2 - windowSize),
                ofEpochMilli(startTime + increment * 2 + windowSize))));
        assertEquals(
            Collections.emptyList(),
            toList(windowStore.fetch(
                3,
                ofEpochMilli(startTime + increment * 3 - windowSize),
                ofEpochMilli(startTime + increment * 3 + windowSize))));
        assertEquals(
            Collections.singletonList("four"),
            toList(windowStore.fetch(
                4,
                ofEpochMilli(startTime + increment * 4 - windowSize),
                ofEpochMilli(startTime + increment * 4 + windowSize))));
        assertEquals(
            Collections.singletonList("five"),
            toList(windowStore.fetch(
                5,
                ofEpochMilli(startTime + increment * 5 - windowSize),
                ofEpochMilli(startTime + increment * 5 + windowSize))));
        assertEquals(
            Collections.singletonList("six"),
            toList(windowStore.fetch(
                6,
                ofEpochMilli(startTime + increment * 6 - windowSize),
                ofEpochMilli(startTime + increment * 6 + windowSize))));
        assertEquals(
            Collections.singletonList("seven"),
            toList(windowStore.fetch(
                7,
                ofEpochMilli(startTime + increment * 7 - windowSize),
                ofEpochMilli(startTime + increment * 7 + windowSize))));
        assertEquals(
            Collections.singletonList("eight"),
            toList(windowStore.fetch(
                8,
                ofEpochMilli(startTime + increment * 8 - windowSize),
                ofEpochMilli(startTime + increment * 8 + windowSize))));

        // check segment directories
        windowStore.flush();
        assertEquals(
            Utils.mkSet(
                segments.segmentName(4),
                segments.segmentName(5),
                segments.segmentName(6)
            ),
            segmentDirs(baseDir)
        );


    }

    @Test
    public void testRestore() throws Exception {
        final long startTime = segmentInterval * 2;
        final long increment = segmentInterval / 2;

        windowStore = createWindowStore(context, false);
        setCurrentTime(startTime);
        windowStore.put(0, "zero");
        setCurrentTime(startTime + increment);
        windowStore.put(1, "one");
        setCurrentTime(startTime + increment * 2);
        windowStore.put(2, "two");
        setCurrentTime(startTime + increment * 3);
        windowStore.put(3, "three");
        setCurrentTime(startTime + increment * 4);
        windowStore.put(4, "four");
        setCurrentTime(startTime + increment * 5);
        windowStore.put(5, "five");
        setCurrentTime(startTime + increment * 6);
        windowStore.put(6, "six");
        setCurrentTime(startTime + increment * 7);
        windowStore.put(7, "seven");
        setCurrentTime(startTime + increment * 8);
        windowStore.put(8, "eight");
        windowStore.flush();

        windowStore.close();

        // remove local store image
        Utils.delete(baseDir);

        windowStore = createWindowStore(context, false);
        assertEquals(
            Collections.emptyList(),
            toList(windowStore.fetch(
                0,
                ofEpochMilli(startTime - windowSize),
                ofEpochMilli(startTime + windowSize))));
        assertEquals(
            Collections.emptyList(),
            toList(windowStore.fetch(
                1,
                ofEpochMilli(startTime + increment - windowSize),
                ofEpochMilli(startTime + increment + windowSize))));
        assertEquals(
            Collections.emptyList(),
            toList(windowStore.fetch(
                2,
                ofEpochMilli(startTime + increment * 2 - windowSize),
                ofEpochMilli(startTime + increment * 2 + windowSize))));
        assertEquals(
            Collections.emptyList(),
            toList(windowStore.fetch(
                3,
                ofEpochMilli(startTime + increment * 3 - windowSize),
                ofEpochMilli(startTime + increment * 3 + windowSize))));
        assertEquals(
            Collections.emptyList(),
            toList(windowStore.fetch(
                4,
                ofEpochMilli(startTime + increment * 4 - windowSize),
                ofEpochMilli(startTime + increment * 4 + windowSize))));
        assertEquals(
            Collections.emptyList(),
            toList(windowStore.fetch(
                5,
                ofEpochMilli(startTime + increment * 5 - windowSize),
                ofEpochMilli(startTime + increment * 5 + windowSize))));
        assertEquals(
            Collections.emptyList(),
            toList(windowStore.fetch(
                6,
                ofEpochMilli(startTime + increment * 6 - windowSize),
                ofEpochMilli(startTime + increment * 6 + windowSize))));
        assertEquals(
            Collections.emptyList(),
            toList(windowStore.fetch(
                7,
                ofEpochMilli(startTime + increment * 7 - windowSize),
                ofEpochMilli(startTime + increment * 7 + windowSize))));
        assertEquals(
            Collections.emptyList(),
            toList(windowStore.fetch(
                8,
                ofEpochMilli(startTime + increment * 8 - windowSize),
                ofEpochMilli(startTime + increment * 8 + windowSize))));

        context.restore(windowName, changeLog);

        assertEquals(
            Collections.emptyList(),
            toList(windowStore.fetch(
                0,
                ofEpochMilli(startTime - windowSize),
                ofEpochMilli(startTime + windowSize))));
        assertEquals(
            Collections.emptyList(),
            toList(windowStore.fetch(
                1,
                ofEpochMilli(startTime + increment - windowSize),
                ofEpochMilli(startTime + increment + windowSize))));
        assertEquals(
            Collections.emptyList(),
            toList(windowStore.fetch(
                2,
                ofEpochMilli(startTime + increment * 2 - windowSize),
                ofEpochMilli(startTime + increment * 2 + windowSize))));
        assertEquals(
            Collections.emptyList(),
            toList(windowStore.fetch(
                3,
                ofEpochMilli(startTime + increment * 3 - windowSize),
                ofEpochMilli(startTime + increment * 3 + windowSize))));
        assertEquals(
            Collections.singletonList("four"),
            toList(windowStore.fetch(
                4,
                ofEpochMilli(startTime + increment * 4 - windowSize),
                ofEpochMilli(startTime + increment * 4 + windowSize))));
        assertEquals(
            Collections.singletonList("five"),
            toList(windowStore.fetch(
                5,
                ofEpochMilli(startTime + increment * 5 - windowSize),
                ofEpochMilli(startTime + increment * 5 + windowSize))));
        assertEquals(
            Collections.singletonList("six"),
            toList(windowStore.fetch(
                6,
                ofEpochMilli(startTime + increment * 6 - windowSize),
                ofEpochMilli(startTime + increment * 6 + windowSize))));
        assertEquals(
            Collections.singletonList("seven"),
            toList(windowStore.fetch(
                7,
                ofEpochMilli(startTime + increment * 7 - windowSize),
                ofEpochMilli(startTime + increment * 7 + windowSize))));
        assertEquals(
            Collections.singletonList("eight"),
            toList(windowStore.fetch(
                8,
                ofEpochMilli(startTime + increment * 8 - windowSize),
                ofEpochMilli(startTime + increment * 8 + windowSize))));

        // check segment directories
        windowStore.flush();
        assertEquals(
            Utils.mkSet(
                segments.segmentName(4L),
                segments.segmentName(5L),
                segments.segmentName(6L)),
            segmentDirs(baseDir)
        );
    }

    @Test
    public void testSegmentMaintenance() {
        windowStore = createWindowStore(context, true);
        context.setTime(0L);
        setCurrentTime(0);
        windowStore.put(0, "v");
        assertEquals(
            Utils.mkSet(segments.segmentName(0L)),
            segmentDirs(baseDir)
        );

        setCurrentTime(segmentInterval - 1);
        windowStore.put(0, "v");
        windowStore.put(0, "v");
        assertEquals(
            Utils.mkSet(segments.segmentName(0L)),
            segmentDirs(baseDir)
        );

        setCurrentTime(segmentInterval);
        windowStore.put(0, "v");
        assertEquals(
            Utils.mkSet(segments.segmentName(0L), segments.segmentName(1L)),
            segmentDirs(baseDir)
        );

        WindowStoreIterator iter;
        int fetchedCount;

        iter = windowStore.fetch(0, ofEpochMilli(0L), ofEpochMilli(segmentInterval * 4));
        fetchedCount = 0;
        while (iter.hasNext()) {
            iter.next();
            fetchedCount++;
        }
        assertEquals(4, fetchedCount);

        assertEquals(
            Utils.mkSet(segments.segmentName(0L), segments.segmentName(1L)),
            segmentDirs(baseDir)
        );

        setCurrentTime(segmentInterval * 3);
        windowStore.put(0, "v");

        iter = windowStore.fetch(0, ofEpochMilli(0L), ofEpochMilli(segmentInterval * 4));
        fetchedCount = 0;
        while (iter.hasNext()) {
            iter.next();
            fetchedCount++;
        }
        assertEquals(2, fetchedCount);

        assertEquals(
            Utils.mkSet(segments.segmentName(1L), segments.segmentName(3L)),
            segmentDirs(baseDir)
        );

        setCurrentTime(segmentInterval * 5);
        windowStore.put(0, "v");

        iter = windowStore.fetch(0, ofEpochMilli(segmentInterval * 4), ofEpochMilli(segmentInterval * 10));
        fetchedCount = 0;
        while (iter.hasNext()) {
            iter.next();
            fetchedCount++;
        }
        assertEquals(1, fetchedCount);

        assertEquals(
            Utils.mkSet(segments.segmentName(3L), segments.segmentName(5L)),
            segmentDirs(baseDir)
        );

    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void testInitialLoading() {
        final File storeDir = new File(baseDir, windowName);

        windowStore = createWindowStore(context, false);

        new File(storeDir, segments.segmentName(0L)).mkdir();
        new File(storeDir, segments.segmentName(1L)).mkdir();
        new File(storeDir, segments.segmentName(2L)).mkdir();
        new File(storeDir, segments.segmentName(3L)).mkdir();
        new File(storeDir, segments.segmentName(4L)).mkdir();
        new File(storeDir, segments.segmentName(5L)).mkdir();
        new File(storeDir, segments.segmentName(6L)).mkdir();
        windowStore.close();

        windowStore = createWindowStore(context, false);

        // put something in the store to advance its stream time and expire the old segments
        windowStore.put(1, "v", 6L * segmentInterval);

        final List<String> expected = asList(
            segments.segmentName(4L),
            segments.segmentName(5L),
            segments.segmentName(6L));
        expected.sort(String::compareTo);

        final List<String> actual = Utils.toList(segmentDirs(baseDir).iterator());
        actual.sort(String::compareTo);

        assertEquals(expected, actual);

        try (final WindowStoreIterator iter = windowStore.fetch(0, ofEpochMilli(0L), ofEpochMilli(1000000L))) {
            while (iter.hasNext()) {
                iter.next();
            }
        }

        assertEquals(
            Utils.mkSet(
                segments.segmentName(4L),
                segments.segmentName(5L),
                segments.segmentName(6L)),
            segmentDirs(baseDir)
        );
    }

    @Test
    public void shouldCloseOpenIteratorsWhenStoreIsClosedAndNotThrowInvalidStateStoreExceptionOnHasNext() {
        windowStore = createWindowStore(context, false);
        setCurrentTime(0);
        windowStore.put(1, "one", 1L);
        windowStore.put(1, "two", 2L);
        windowStore.put(1, "three", 3L);

        final WindowStoreIterator<String> iterator = windowStore.fetch(1, ofEpochMilli(1L), ofEpochMilli(3L));
        assertTrue(iterator.hasNext());
        windowStore.close();

        assertFalse(iterator.hasNext());

    }

    @Test
    public void shouldFetchAndIterateOverExactKeys() {
        final long windowSize = 0x7a00000000000000L;
        final long retentionPeriod = 0x7a00000000000000L;

        final WindowStore<String, String> windowStore = Stores.windowStoreBuilder(
            Stores.persistentWindowStore(windowName, ofMillis(retentionPeriod), ofMillis(windowSize), true),
            Serdes.String(),
            Serdes.String()).build();

        windowStore.init(context, windowStore);

        windowStore.put("a", "0001", 0);
        windowStore.put("aa", "0002", 0);
        windowStore.put("a", "0003", 1);
        windowStore.put("aa", "0004", 1);
        windowStore.put("a", "0005", 0x7a00000000000000L - 1);


        final List expected = asList("0001", "0003", "0005");
        assertThat(toList(windowStore.fetch("a", ofEpochMilli(0), ofEpochMilli(Long.MAX_VALUE))), equalTo(expected));

        List<KeyValue<Windowed<String>, String>> list =
            StreamsTestUtils.toList(windowStore.fetch("a", "a", ofEpochMilli(0), ofEpochMilli(Long.MAX_VALUE)));
        assertThat(list, equalTo(asList(
            windowedPair("a", "0001", 0, windowSize),
            windowedPair("a", "0003", 1, windowSize),
            windowedPair("a", "0005", 0x7a00000000000000L - 1, windowSize)
        )));

        list = StreamsTestUtils.toList(windowStore.fetch("aa", "aa", ofEpochMilli(0), ofEpochMilli(Long.MAX_VALUE)));
        assertThat(list, equalTo(asList(
            windowedPair("aa", "0002", 0, windowSize),
            windowedPair("aa", "0004", 1, windowSize)
        )));
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnPutNullKey() {
        windowStore = createWindowStore(context, false);
        windowStore.put(null, "anyValue");
    }

    @Test
    public void shouldNotThrowNullPointerExceptionOnPutNullValue() {
        windowStore = createWindowStore(context, false);
        windowStore.put(1, null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnGetNullKey() {
        windowStore = createWindowStore(context, false);
        windowStore.fetch(null, ofEpochMilli(1L), ofEpochMilli(2L));
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnRangeNullFromKey() {
        windowStore = createWindowStore(context, false);
        windowStore.fetch(null, 2, ofEpochMilli(1L), ofEpochMilli(2L));
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnRangeNullToKey() {
        windowStore = createWindowStore(context, false);
        windowStore.fetch(1, null, ofEpochMilli(1L), ofEpochMilli(2L));
    }

    @Test
    public void shouldNoNullPointerWhenSerdeDoesNotHandleNull() {
        windowStore = new RocksDBWindowStore<>(
            new RocksDBSegmentedBytesStore(
                windowName,
                "metrics-scope",
                retentionPeriod,
                segmentInterval,
                new WindowKeySchema()),
            Serdes.Integer(),
            new SerdeThatDoesntHandleNull(),
            false,
            windowSize);
        windowStore.init(context, windowStore);

        assertNull(windowStore.fetch(1, 0));
    }

    @Test
    public void shouldFetchAndIterateOverExactBinaryKeys() {
        final WindowStore<Bytes, String> windowStore = Stores.windowStoreBuilder(
            Stores.persistentWindowStore(windowName, ofMillis(60_000L), ofMillis(60_000L), true),
            Serdes.Bytes(),
            Serdes.String()).build();

        windowStore.init(context, windowStore);

        final Bytes key1 = Bytes.wrap(new byte[] {0});
        final Bytes key2 = Bytes.wrap(new byte[] {0, 0});
        final Bytes key3 = Bytes.wrap(new byte[] {0, 0, 0});
        windowStore.put(key1, "1", 0);
        windowStore.put(key2, "2", 0);
        windowStore.put(key3, "3", 0);
        windowStore.put(key1, "4", 1);
        windowStore.put(key2, "5", 1);
        windowStore.put(key3, "6", 59999);
        windowStore.put(key1, "7", 59999);
        windowStore.put(key2, "8", 59999);
        windowStore.put(key3, "9", 59999);

        final List expectedKey1 = asList("1", "4", "7");
        assertThat(toList(windowStore.fetch(key1, ofEpochMilli(0), ofEpochMilli(Long.MAX_VALUE))), equalTo(expectedKey1));
        final List expectedKey2 = asList("2", "5", "8");
        assertThat(toList(windowStore.fetch(key2, ofEpochMilli(0), ofEpochMilli(Long.MAX_VALUE))), equalTo(expectedKey2));
        final List expectedKey3 = asList("3", "6", "9");
        assertThat(toList(windowStore.fetch(key3, ofEpochMilli(0), ofEpochMilli(Long.MAX_VALUE))), equalTo(expectedKey3));
    }

    private void putFirstBatch(final WindowStore<Integer, String> store,
                               @SuppressWarnings("SameParameterValue") final long startTime,
                               final InternalMockProcessorContext context) {
        context.setRecordContext(createRecordContext(startTime));
        store.put(0, "zero");
        context.setRecordContext(createRecordContext(startTime + 1L));
        store.put(1, "one");
        context.setRecordContext(createRecordContext(startTime + 2L));
        store.put(2, "two");
        context.setRecordContext(createRecordContext(startTime + 4L));
        store.put(4, "four");
        context.setRecordContext(createRecordContext(startTime + 5L));
        store.put(5, "five");
    }

    private void putSecondBatch(final WindowStore<Integer, String> store,
                                @SuppressWarnings("SameParameterValue") final long startTime,
                                final InternalMockProcessorContext context) {
        context.setRecordContext(createRecordContext(startTime + 3L));
        store.put(2, "two+1");
        context.setRecordContext(createRecordContext(startTime + 4L));
        store.put(2, "two+2");
        context.setRecordContext(createRecordContext(startTime + 5L));
        store.put(2, "two+3");
        context.setRecordContext(createRecordContext(startTime + 6L));
        store.put(2, "two+4");
        context.setRecordContext(createRecordContext(startTime + 7L));
        store.put(2, "two+5");
        context.setRecordContext(createRecordContext(startTime + 8L));
        store.put(2, "two+6");
    }

    private <E> List<E> toList(final WindowStoreIterator<E> iterator) {
        final ArrayList<E> list = new ArrayList<>();
        while (iterator.hasNext()) {
            list.add(iterator.next().value);
        }
        return list;
    }

    private Set<String> segmentDirs(final File baseDir) {
        final File windowDir = new File(baseDir, windowName);

        return new HashSet<>(asList(requireNonNull(windowDir.list())));
    }

    private Map<Integer, Set<String>> entriesByKey(final List<KeyValue<byte[], byte[]>> changeLog,
                                                   @SuppressWarnings("SameParameterValue") final long startTime) {
        final HashMap<Integer, Set<String>> entriesByKey = new HashMap<>();

        for (final KeyValue<byte[], byte[]> entry : changeLog) {
            final long timestamp = WindowKeySchema.extractStoreTimestamp(entry.key);

            final Integer key = WindowKeySchema.extractStoreKey(entry.key, serdes);
            final String value = entry.value == null ? null : serdes.valueFrom(entry.value);

            final Set<String> entries = entriesByKey.computeIfAbsent(key, k -> new HashSet<>());
            entries.add(value + "@" + (timestamp - startTime));
        }

        return entriesByKey;
    }

    private <K, V> KeyValue<Windowed<K>, V> windowedPair(final K key, final V value, final long timestamp) {
        return windowedPair(key, value, timestamp, windowSize);
    }

    private static <K, V> KeyValue<Windowed<K>, V> windowedPair(final K key, final V value, final long timestamp, final long windowSize) {
        return KeyValue.pair(new Windowed<>(key, WindowKeySchema.timeWindowForSize(timestamp, windowSize)), value);
    }
}
