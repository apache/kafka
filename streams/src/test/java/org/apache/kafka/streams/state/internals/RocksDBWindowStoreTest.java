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
import org.apache.kafka.streams.errors.InvalidStateStoreException;
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
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings("PointlessArithmeticExpression")
public class RocksDBWindowStoreTest {

    private static final long DEFAULT_CACHE_SIZE_BYTES = 1024 * 1024L;

    private final int numSegments = 3;
    private final long windowSize = 3L;
    private final String windowName = "window";
    private final long segmentSize = Segments.MIN_SEGMENT_INTERVAL;
    private final long retentionPeriod = segmentSize * (numSegments - 1);
    private final Segments segments = new Segments(windowName, retentionPeriod, numSegments);
    private final StateSerdes<Integer, String> serdes = new StateSerdes<>("", Serdes.Integer(), Serdes.String());

    private final List<KeyValue<byte[], byte[]>> changeLog = new ArrayList<>();
    private final ThreadCache cache = new ThreadCache(new LogContext("TestCache "), DEFAULT_CACHE_SIZE_BYTES, new MockStreamsMetrics(new Metrics()));

    private final Producer<byte[], byte[]> producer = new MockProducer<>(true, Serdes.ByteArray().serializer(), Serdes.ByteArray().serializer());
    private final RecordCollector recordCollector = new RecordCollectorImpl(
        producer,
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
                keySerializer.serialize(topic, key),
                valueSerializer.serialize(topic, value))
            );
        }
    };

    private final File baseDir = TestUtils.tempDirectory("test");
    private final InternalMockProcessorContext context = new InternalMockProcessorContext(baseDir, Serdes.ByteArray(), Serdes.ByteArray(), recordCollector, cache);
    private WindowStore<Integer, String> windowStore;

    private WindowStore<Integer, String> createWindowStore(final ProcessorContext context, final boolean retainDuplicates) {
        final WindowStore<Integer, String> store = Stores.windowStoreBuilder(
            Stores.persistentWindowStore(
                windowName,
                retentionPeriod,
                numSegments,
                windowSize,
                retainDuplicates),
            Serdes.Integer(),
            Serdes.String()).build();

        store.init(context, store);
        return store;
    }

    private WindowStore<Integer, String> createWindowStore(final ProcessorContext context) {
        return createWindowStore(context, false);
    }

    @After
    public void closeStore() {
        if (windowStore != null) {
            windowStore.close();
        }
    }

    @Test
    public void shouldOnlyIterateOpenSegments() {
        windowStore = createWindowStore(context);
        long currentTime = 0;
        context.setRecordContext(createRecordContext(currentTime));
        windowStore.put(1, "one");

        currentTime = currentTime + segmentSize;
        context.setRecordContext(createRecordContext(currentTime));
        windowStore.put(1, "two");
        currentTime = currentTime + segmentSize;

        context.setRecordContext(createRecordContext(currentTime));
        windowStore.put(1, "three");

        final WindowStoreIterator<String> iterator = windowStore.fetch(1, 0, currentTime);

        // roll to the next segment that will close the first
        currentTime = currentTime + segmentSize;
        context.setRecordContext(createRecordContext(currentTime));
        windowStore.put(1, "four");

        // should only have 2 values as the first segment is no longer open
        assertEquals(new KeyValue<>(60000L, "two"), iterator.next());
        assertEquals(new KeyValue<>(120000L, "three"), iterator.next());
        assertFalse(iterator.hasNext());
    }

    private ProcessorRecordContext createRecordContext(final long time) {
        return new ProcessorRecordContext(time, 0, 0, "topic", null);
    }

    @Test
    public void testRangeAndSinglePointFetch() {
        windowStore = createWindowStore(context);
        final long startTime = segmentSize - 4L;

        putFirstBatch(windowStore, startTime, context);

        assertEquals("zero", windowStore.fetch(0, startTime));
        assertEquals("one", windowStore.fetch(1, startTime + 1L));
        assertEquals("two", windowStore.fetch(2, startTime + 2L));
        assertEquals("four", windowStore.fetch(4, startTime + 4L));
        assertEquals("five", windowStore.fetch(5, startTime + 5L));

        assertEquals(Utils.mkList("zero"), toList(windowStore.fetch(0, startTime + 0L - windowSize, startTime + 0L + windowSize)));
        assertEquals(Utils.mkList("one"), toList(windowStore.fetch(1, startTime + 1L - windowSize, startTime + 1L + windowSize)));
        assertEquals(Utils.mkList("two"), toList(windowStore.fetch(2, startTime + 2L - windowSize, startTime + 2L + windowSize)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(3, startTime + 3L - windowSize, startTime + 3L + windowSize)));
        assertEquals(Utils.mkList("four"), toList(windowStore.fetch(4, startTime + 4L - windowSize, startTime + 4L + windowSize)));
        assertEquals(Utils.mkList("five"), toList(windowStore.fetch(5, startTime + 5L - windowSize, startTime + 5L + windowSize)));

        putSecondBatch(windowStore, startTime, context);

        assertEquals("two+1", windowStore.fetch(2, startTime + 3L));
        assertEquals("two+2", windowStore.fetch(2, startTime + 4L));
        assertEquals("two+3", windowStore.fetch(2, startTime + 5L));
        assertEquals("two+4", windowStore.fetch(2, startTime + 6L));
        assertEquals("two+5", windowStore.fetch(2, startTime + 7L));
        assertEquals("two+6", windowStore.fetch(2, startTime + 8L));

        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, startTime - 2L - windowSize, startTime - 2L + windowSize)));
        assertEquals(Utils.mkList("two"), toList(windowStore.fetch(2, startTime - 1L - windowSize, startTime - 1L + windowSize)));
        assertEquals(Utils.mkList("two", "two+1"), toList(windowStore.fetch(2, startTime - windowSize, startTime + windowSize)));
        assertEquals(Utils.mkList("two", "two+1", "two+2"), toList(windowStore.fetch(2, startTime + 1L - windowSize, startTime + 1L + windowSize)));
        assertEquals(Utils.mkList("two", "two+1", "two+2", "two+3"), toList(windowStore.fetch(2, startTime + 2L - windowSize, startTime + 2L + windowSize)));
        assertEquals(Utils.mkList("two", "two+1", "two+2", "two+3", "two+4"), toList(windowStore.fetch(2, startTime + 3L - windowSize, startTime + 3L + windowSize)));
        assertEquals(Utils.mkList("two", "two+1", "two+2", "two+3", "two+4", "two+5"), toList(windowStore.fetch(2, startTime + 4L - windowSize, startTime + 4L + windowSize)));
        assertEquals(Utils.mkList("two", "two+1", "two+2", "two+3", "two+4", "two+5", "two+6"), toList(windowStore.fetch(2, startTime + 5L - windowSize, startTime + 5L + windowSize)));
        assertEquals(Utils.mkList("two+1", "two+2", "two+3", "two+4", "two+5", "two+6"), toList(windowStore.fetch(2, startTime + 6L - windowSize, startTime + 6L + windowSize)));
        assertEquals(Utils.mkList("two+2", "two+3", "two+4", "two+5", "two+6"), toList(windowStore.fetch(2, startTime + 7L - windowSize, startTime + 7L + windowSize)));
        assertEquals(Utils.mkList("two+3", "two+4", "two+5", "two+6"), toList(windowStore.fetch(2, startTime + 8L - windowSize, startTime + 8L + windowSize)));
        assertEquals(Utils.mkList("two+4", "two+5", "two+6"), toList(windowStore.fetch(2, startTime + 9L - windowSize, startTime + 9L + windowSize)));
        assertEquals(Utils.mkList("two+5", "two+6"), toList(windowStore.fetch(2, startTime + 10L - windowSize, startTime + 10L + windowSize)));
        assertEquals(Utils.mkList("two+6"), toList(windowStore.fetch(2, startTime + 11L - windowSize, startTime + 11L + windowSize)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, startTime + 12L - windowSize, startTime + 12L + windowSize)));

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
        windowStore = createWindowStore(context);
        final long startTime = segmentSize - 4L;

        putFirstBatch(windowStore, startTime, context);

        final KeyValue<Windowed<Integer>, String> zero = windowedPair(0, "zero", startTime + 0);
        final KeyValue<Windowed<Integer>, String> one = windowedPair(1, "one", startTime + 1);
        final KeyValue<Windowed<Integer>, String> two = windowedPair(2, "two", startTime + 2);
        final KeyValue<Windowed<Integer>, String> four = windowedPair(4, "four", startTime + 4);
        final KeyValue<Windowed<Integer>, String> five = windowedPair(5, "five", startTime + 5);

        assertEquals(
            Utils.mkList(zero, one, two, four, five),
            StreamsTestUtils.toList(windowStore.all())
        );
    }

    @Test
    public void shouldFetchAllInTimeRange() {
        windowStore = createWindowStore(context);
        final long startTime = segmentSize - 4L;

        putFirstBatch(windowStore, startTime, context);

        final KeyValue<Windowed<Integer>, String> zero = windowedPair(0, "zero", startTime + 0);
        final KeyValue<Windowed<Integer>, String> one = windowedPair(1, "one", startTime + 1);
        final KeyValue<Windowed<Integer>, String> two = windowedPair(2, "two", startTime + 2);
        final KeyValue<Windowed<Integer>, String> four = windowedPair(4, "four", startTime + 4);
        final KeyValue<Windowed<Integer>, String> five = windowedPair(5, "five", startTime + 5);

        assertEquals(
            Utils.mkList(one, two, four),
            StreamsTestUtils.toList(windowStore.fetchAll(startTime + 1, startTime + 4))
        );

        assertEquals(
            Utils.mkList(zero, one, two),
            StreamsTestUtils.toList(windowStore.fetchAll(startTime + 0, startTime + 3))
        );

        assertEquals(
            Utils.mkList(one, two, four, five),
            StreamsTestUtils.toList(windowStore.fetchAll(startTime + 1, startTime + 5))
        );
    }

    @Test
    public void testFetchRange() {
        windowStore = createWindowStore(context);
        final long startTime = segmentSize - 4L;

        putFirstBatch(windowStore, startTime, context);

        final KeyValue<Windowed<Integer>, String> zero = windowedPair(0, "zero", startTime + 0);
        final KeyValue<Windowed<Integer>, String> one = windowedPair(1, "one", startTime + 1);
        final KeyValue<Windowed<Integer>, String> two = windowedPair(2, "two", startTime + 2);
        final KeyValue<Windowed<Integer>, String> four = windowedPair(4, "four", startTime + 4);
        final KeyValue<Windowed<Integer>, String> five = windowedPair(5, "five", startTime + 5);

        assertEquals(
            Utils.mkList(zero, one),
            StreamsTestUtils.toList(windowStore.fetch(0, 1, startTime + 0L - windowSize, startTime + 0L + windowSize))
        );
        assertEquals(
            Utils.mkList(one),
            StreamsTestUtils.toList(windowStore.fetch(1, 1, startTime + 0L - windowSize, startTime + 0L + windowSize))
        );
        assertEquals(
            Utils.mkList(one, two),
            StreamsTestUtils.toList(windowStore.fetch(1, 3, startTime + 0L - windowSize, startTime + 0L + windowSize))
        );
        assertEquals(
            Utils.mkList(zero, one, two),
            StreamsTestUtils.toList(windowStore.fetch(0, 5, startTime + 0L - windowSize, startTime + 0L + windowSize))
        );
        assertEquals(
            Utils.mkList(zero, one, two,
                four, five),
            StreamsTestUtils.toList(windowStore.fetch(0, 5, startTime + 0L - windowSize, startTime + 0L + windowSize + 5L))
        );
        assertEquals(
            Utils.mkList(two, four, five),
            StreamsTestUtils.toList(windowStore.fetch(0, 5, startTime + 2L, startTime + 0L + windowSize + 5L))
        );
        assertEquals(
            Utils.mkList(),
            StreamsTestUtils.toList(windowStore.fetch(4, 5, startTime + 2L, startTime + windowSize))
        );
        assertEquals(
            Utils.mkList(),
            StreamsTestUtils.toList(windowStore.fetch(0, 3, startTime + 3L, startTime + windowSize + 5))
        );
    }

    @Test
    public void testPutAndFetchBefore() {
        windowStore = createWindowStore(context);
        final long startTime = segmentSize - 4L;

        putFirstBatch(windowStore, startTime, context);

        assertEquals(Utils.mkList("zero"), toList(windowStore.fetch(0, startTime + 0L - windowSize, startTime + 0L)));
        assertEquals(Utils.mkList("one"), toList(windowStore.fetch(1, startTime + 1L - windowSize, startTime + 1L)));
        assertEquals(Utils.mkList("two"), toList(windowStore.fetch(2, startTime + 2L - windowSize, startTime + 2L)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(3, startTime + 3L - windowSize, startTime + 3L)));
        assertEquals(Utils.mkList("four"), toList(windowStore.fetch(4, startTime + 4L - windowSize, startTime + 4L)));
        assertEquals(Utils.mkList("five"), toList(windowStore.fetch(5, startTime + 5L - windowSize, startTime + 5L)));

        putSecondBatch(windowStore, startTime, context);

        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, startTime - 1L - windowSize, startTime - 1L)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, startTime + 0L - windowSize, startTime + 0L)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, startTime + 1L - windowSize, startTime + 1L)));
        assertEquals(Utils.mkList("two"), toList(windowStore.fetch(2, startTime + 2L - windowSize, startTime + 2L)));
        assertEquals(Utils.mkList("two", "two+1"), toList(windowStore.fetch(2, startTime + 3L - windowSize, startTime + 3L)));
        assertEquals(Utils.mkList("two", "two+1", "two+2"), toList(windowStore.fetch(2, startTime + 4L - windowSize, startTime + 4L)));
        assertEquals(Utils.mkList("two", "two+1", "two+2", "two+3"), toList(windowStore.fetch(2, startTime + 5L - windowSize, startTime + 5L)));
        assertEquals(Utils.mkList("two+1", "two+2", "two+3", "two+4"), toList(windowStore.fetch(2, startTime + 6L - windowSize, startTime + 6L)));
        assertEquals(Utils.mkList("two+2", "two+3", "two+4", "two+5"), toList(windowStore.fetch(2, startTime + 7L - windowSize, startTime + 7L)));
        assertEquals(Utils.mkList("two+3", "two+4", "two+5", "two+6"), toList(windowStore.fetch(2, startTime + 8L - windowSize, startTime + 8L)));
        assertEquals(Utils.mkList("two+4", "two+5", "two+6"), toList(windowStore.fetch(2, startTime + 9L - windowSize, startTime + 9L)));
        assertEquals(Utils.mkList("two+5", "two+6"), toList(windowStore.fetch(2, startTime + 10L - windowSize, startTime + 10L)));
        assertEquals(Utils.mkList("two+6"), toList(windowStore.fetch(2, startTime + 11L - windowSize, startTime + 11L)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, startTime + 12L - windowSize, startTime + 12L)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, startTime + 13L - windowSize, startTime + 13L)));

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
    public void testPutAndFetchAfter() {
        windowStore = createWindowStore(context);
        final long startTime = segmentSize - 4L;

        putFirstBatch(windowStore, startTime, context);

        assertEquals(Utils.mkList("zero"), toList(windowStore.fetch(0, startTime + 0L, startTime + 0L + windowSize)));
        assertEquals(Utils.mkList("one"), toList(windowStore.fetch(1, startTime + 1L, startTime + 1L + windowSize)));
        assertEquals(Utils.mkList("two"), toList(windowStore.fetch(2, startTime + 2L, startTime + 2L + windowSize)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(3, startTime + 3L, startTime + 3L + windowSize)));
        assertEquals(Utils.mkList("four"), toList(windowStore.fetch(4, startTime + 4L, startTime + 4L + windowSize)));
        assertEquals(Utils.mkList("five"), toList(windowStore.fetch(5, startTime + 5L, startTime + 5L + windowSize)));

        putSecondBatch(windowStore, startTime, context);

        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, startTime - 2L, startTime - 2L + windowSize)));
        assertEquals(Utils.mkList("two"), toList(windowStore.fetch(2, startTime - 1L, startTime - 1L + windowSize)));
        assertEquals(Utils.mkList("two", "two+1"), toList(windowStore.fetch(2, startTime, startTime + windowSize)));
        assertEquals(Utils.mkList("two", "two+1", "two+2"), toList(windowStore.fetch(2, startTime + 1L, startTime + 1L + windowSize)));
        assertEquals(Utils.mkList("two", "two+1", "two+2", "two+3"), toList(windowStore.fetch(2, startTime + 2L, startTime + 2L + windowSize)));
        assertEquals(Utils.mkList("two+1", "two+2", "two+3", "two+4"), toList(windowStore.fetch(2, startTime + 3L, startTime + 3L + windowSize)));
        assertEquals(Utils.mkList("two+2", "two+3", "two+4", "two+5"), toList(windowStore.fetch(2, startTime + 4L, startTime + 4L + windowSize)));
        assertEquals(Utils.mkList("two+3", "two+4", "two+5", "two+6"), toList(windowStore.fetch(2, startTime + 5L, startTime + 5L + windowSize)));
        assertEquals(Utils.mkList("two+4", "two+5", "two+6"), toList(windowStore.fetch(2, startTime + 6L, startTime + 6L + windowSize)));
        assertEquals(Utils.mkList("two+5", "two+6"), toList(windowStore.fetch(2, startTime + 7L, startTime + 7L + windowSize)));
        assertEquals(Utils.mkList("two+6"), toList(windowStore.fetch(2, startTime + 8L, startTime + 8L + windowSize)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, startTime + 9L, startTime + 9L + windowSize)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, startTime + 10L, startTime + 10L + windowSize)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, startTime + 11L, startTime + 11L + windowSize)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, startTime + 12L, startTime + 12L + windowSize)));

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
    public void testPutSameKeyTimestamp() {
        windowStore = createWindowStore(context, true);
        final long startTime = segmentSize - 4L;

        context.setRecordContext(createRecordContext(startTime));
        windowStore.put(0, "zero");

        assertEquals(Utils.mkList("zero"), toList(windowStore.fetch(0, startTime - windowSize, startTime + windowSize)));

        windowStore.put(0, "zero");
        windowStore.put(0, "zero+");
        windowStore.put(0, "zero++");

        assertEquals(Utils.mkList("zero", "zero", "zero+", "zero++"), toList(windowStore.fetch(0, startTime - windowSize, startTime + windowSize)));
        assertEquals(Utils.mkList("zero", "zero", "zero+", "zero++"), toList(windowStore.fetch(0, startTime + 1L - windowSize, startTime + 1L + windowSize)));
        assertEquals(Utils.mkList("zero", "zero", "zero+", "zero++"), toList(windowStore.fetch(0, startTime + 2L - windowSize, startTime + 2L + windowSize)));
        assertEquals(Utils.mkList("zero", "zero", "zero+", "zero++"), toList(windowStore.fetch(0, startTime + 3L - windowSize, startTime + 3L + windowSize)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(0, startTime + 4L - windowSize, startTime + 4L + windowSize)));

        // Flush the store and verify all current entries were properly flushed ...
        windowStore.flush();

        final Map<Integer, Set<String>> entriesByKey = entriesByKey(changeLog, startTime);

        assertEquals(Utils.mkSet("zero@0", "zero@0", "zero+@0", "zero++@0"), entriesByKey.get(0));
    }

    @Test
    public void testRolling() {
        windowStore = createWindowStore(context);

        // to validate segments
        final Segments segments = new Segments(windowName, retentionPeriod, numSegments);
        final long startTime = segmentSize * 2;
        final long increment = segmentSize / 2;
        context.setRecordContext(createRecordContext(startTime));
        windowStore.put(0, "zero");
        assertEquals(Utils.mkSet(segments.segmentName(2)), segmentDirs(baseDir));

        context.setRecordContext(createRecordContext(startTime + increment));
        windowStore.put(1, "one");
        assertEquals(Utils.mkSet(segments.segmentName(2)), segmentDirs(baseDir));

        context.setRecordContext(createRecordContext(startTime + increment * 2));
        windowStore.put(2, "two");
        assertEquals(
            Utils.mkSet(
                segments.segmentName(2),
                segments.segmentName(3)
            ),
            segmentDirs(baseDir)
        );

        context.setRecordContext(createRecordContext(startTime + increment * 4));
        windowStore.put(4, "four");
        assertEquals(
            Utils.mkSet(
                segments.segmentName(2),
                segments.segmentName(3),
                segments.segmentName(4)
            ),
            segmentDirs(baseDir)
        );


        context.setRecordContext(createRecordContext(startTime + increment * 5));
        windowStore.put(5, "five");
        assertEquals(
            Utils.mkSet(
                segments.segmentName(2),
                segments.segmentName(3),
                segments.segmentName(4)
            ),
            segmentDirs(baseDir)
        );

        assertEquals(Utils.mkList("zero"), toList(windowStore.fetch(0, startTime - windowSize, startTime + windowSize)));
        assertEquals(Utils.mkList("one"), toList(windowStore.fetch(1, startTime + increment - windowSize, startTime + increment + windowSize)));
        assertEquals(Utils.mkList("two"), toList(windowStore.fetch(2, startTime + increment * 2 - windowSize, startTime + increment * 2 + windowSize)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(3, startTime + increment * 3 - windowSize, startTime + increment * 3 + windowSize)));
        assertEquals(Utils.mkList("four"), toList(windowStore.fetch(4, startTime + increment * 4 - windowSize, startTime + increment * 4 + windowSize)));
        assertEquals(Utils.mkList("five"), toList(windowStore.fetch(5, startTime + increment * 5 - windowSize, startTime + increment * 5 + windowSize)));

        context.setRecordContext(createRecordContext(startTime + increment * 6));
        windowStore.put(6, "six");
        assertEquals(
            Utils.mkSet(
                segments.segmentName(3),
                segments.segmentName(4),
                segments.segmentName(5)
            ),
            segmentDirs(baseDir)
        );


        assertEquals(Utils.mkList(), toList(windowStore.fetch(0, startTime - windowSize, startTime + windowSize)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(1, startTime + increment - windowSize, startTime + increment + windowSize)));
        assertEquals(Utils.mkList("two"), toList(windowStore.fetch(2, startTime + increment * 2 - windowSize, startTime + increment * 2 + windowSize)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(3, startTime + increment * 3 - windowSize, startTime + increment * 3 + windowSize)));
        assertEquals(Utils.mkList("four"), toList(windowStore.fetch(4, startTime + increment * 4 - windowSize, startTime + increment * 4 + windowSize)));
        assertEquals(Utils.mkList("five"), toList(windowStore.fetch(5, startTime + increment * 5 - windowSize, startTime + increment * 5 + windowSize)));
        assertEquals(Utils.mkList("six"), toList(windowStore.fetch(6, startTime + increment * 6 - windowSize, startTime + increment * 6 + windowSize)));


        context.setRecordContext(createRecordContext(startTime + increment * 7));
        windowStore.put(7, "seven");
        assertEquals(
            Utils.mkSet(
                segments.segmentName(3),
                segments.segmentName(4),
                segments.segmentName(5)
            ),
            segmentDirs(baseDir)
        );

        assertEquals(Utils.mkList(), toList(windowStore.fetch(0, startTime - windowSize, startTime + windowSize)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(1, startTime + increment - windowSize, startTime + increment + windowSize)));
        assertEquals(Utils.mkList("two"), toList(windowStore.fetch(2, startTime + increment * 2 - windowSize, startTime + increment * 2 + windowSize)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(3, startTime + increment * 3 - windowSize, startTime + increment * 3 + windowSize)));
        assertEquals(Utils.mkList("four"), toList(windowStore.fetch(4, startTime + increment * 4 - windowSize, startTime + increment * 4 + windowSize)));
        assertEquals(Utils.mkList("five"), toList(windowStore.fetch(5, startTime + increment * 5 - windowSize, startTime + increment * 5 + windowSize)));
        assertEquals(Utils.mkList("six"), toList(windowStore.fetch(6, startTime + increment * 6 - windowSize, startTime + increment * 6 + windowSize)));
        assertEquals(Utils.mkList("seven"), toList(windowStore.fetch(7, startTime + increment * 7 - windowSize, startTime + increment * 7 + windowSize)));

        context.setRecordContext(createRecordContext(startTime + increment * 8));
        windowStore.put(8, "eight");
        assertEquals(
            Utils.mkSet(
                segments.segmentName(4),
                segments.segmentName(5),
                segments.segmentName(6)
            ),
            segmentDirs(baseDir)
        );


        assertEquals(Utils.mkList(), toList(windowStore.fetch(0, startTime - windowSize, startTime + windowSize)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(1, startTime + increment - windowSize, startTime + increment + windowSize)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, startTime + increment * 2 - windowSize, startTime + increment * 2 + windowSize)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(3, startTime + increment * 3 - windowSize, startTime + increment * 3 + windowSize)));
        assertEquals(Utils.mkList("four"), toList(windowStore.fetch(4, startTime + increment * 4 - windowSize, startTime + increment * 4 + windowSize)));
        assertEquals(Utils.mkList("five"), toList(windowStore.fetch(5, startTime + increment * 5 - windowSize, startTime + increment * 5 + windowSize)));
        assertEquals(Utils.mkList("six"), toList(windowStore.fetch(6, startTime + increment * 6 - windowSize, startTime + increment * 6 + windowSize)));
        assertEquals(Utils.mkList("seven"), toList(windowStore.fetch(7, startTime + increment * 7 - windowSize, startTime + increment * 7 + windowSize)));
        assertEquals(Utils.mkList("eight"), toList(windowStore.fetch(8, startTime + increment * 8 - windowSize, startTime + increment * 8 + windowSize)));

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
    public void testRestore() throws IOException {
        final long startTime = segmentSize * 2;
        final long increment = segmentSize / 2;

        windowStore = createWindowStore(context);
        context.setRecordContext(createRecordContext(startTime));
        windowStore.put(0, "zero");
        context.setRecordContext(createRecordContext(startTime + increment));
        windowStore.put(1, "one");
        context.setRecordContext(createRecordContext(startTime + increment * 2));
        windowStore.put(2, "two");
        context.setRecordContext(createRecordContext(startTime + increment * 3));
        windowStore.put(3, "three");
        context.setRecordContext(createRecordContext(startTime + increment * 4));
        windowStore.put(4, "four");
        context.setRecordContext(createRecordContext(startTime + increment * 5));
        windowStore.put(5, "five");
        context.setRecordContext(createRecordContext(startTime + increment * 6));
        windowStore.put(6, "six");
        context.setRecordContext(createRecordContext(startTime + increment * 7));
        windowStore.put(7, "seven");
        context.setRecordContext(createRecordContext(startTime + increment * 8));
        windowStore.put(8, "eight");
        windowStore.flush();

        windowStore.close();

        // remove local store image
        Utils.delete(baseDir);

        windowStore = createWindowStore(context);
        assertEquals(Utils.mkList(), toList(windowStore.fetch(0, startTime - windowSize, startTime + windowSize)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(1, startTime + increment - windowSize, startTime + increment + windowSize)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, startTime + increment * 2 - windowSize, startTime + increment * 2 + windowSize)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(3, startTime + increment * 3 - windowSize, startTime + increment * 3 + windowSize)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(4, startTime + increment * 4 - windowSize, startTime + increment * 4 + windowSize)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(5, startTime + increment * 5 - windowSize, startTime + increment * 5 + windowSize)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(6, startTime + increment * 6 - windowSize, startTime + increment * 6 + windowSize)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(7, startTime + increment * 7 - windowSize, startTime + increment * 7 + windowSize)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(8, startTime + increment * 8 - windowSize, startTime + increment * 8 + windowSize)));

        context.restore(windowName, changeLog);

        assertEquals(Utils.mkList(), toList(windowStore.fetch(0, startTime - windowSize, startTime + windowSize)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(1, startTime + increment - windowSize, startTime + increment + windowSize)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, startTime + increment * 2 - windowSize, startTime + increment * 2 + windowSize)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(3, startTime + increment * 3 - windowSize, startTime + increment * 3 + windowSize)));
        assertEquals(Utils.mkList("four"), toList(windowStore.fetch(4, startTime + increment * 4 - windowSize, startTime + increment * 4 + windowSize)));
        assertEquals(Utils.mkList("five"), toList(windowStore.fetch(5, startTime + increment * 5 - windowSize, startTime + increment * 5 + windowSize)));
        assertEquals(Utils.mkList("six"), toList(windowStore.fetch(6, startTime + increment * 6 - windowSize, startTime + increment * 6 + windowSize)));
        assertEquals(Utils.mkList("seven"), toList(windowStore.fetch(7, startTime + increment * 7 - windowSize, startTime + increment * 7 + windowSize)));
        assertEquals(Utils.mkList("eight"), toList(windowStore.fetch(8, startTime + increment * 8 - windowSize, startTime + increment * 8 + windowSize)));

        // check segment directories
        windowStore.flush();
        assertEquals(
            Utils.mkSet(segments.segmentName(4L), segments.segmentName(5L), segments.segmentName(6L)),
            segmentDirs(baseDir)
        );
    }

    @Test
    public void testSegmentMaintenance() {
        windowStore = createWindowStore(context, true);
        context.setTime(0L);
        context.setRecordContext(createRecordContext(0));
        windowStore.put(0, "v");
        assertEquals(
            Utils.mkSet(segments.segmentName(0L)),
            segmentDirs(baseDir)
        );

        context.setRecordContext(createRecordContext(59999));
        windowStore.put(0, "v");
        windowStore.put(0, "v");
        assertEquals(
            Utils.mkSet(segments.segmentName(0L)),
            segmentDirs(baseDir)
        );

        context.setRecordContext(createRecordContext(60000));
        windowStore.put(0, "v");
        assertEquals(
            Utils.mkSet(segments.segmentName(0L), segments.segmentName(1L)),
            segmentDirs(baseDir)
        );

        WindowStoreIterator iter;
        int fetchedCount;

        iter = windowStore.fetch(0, 0L, 240000L);
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

        context.setRecordContext(createRecordContext(180000));
        windowStore.put(0, "v");

        iter = windowStore.fetch(0, 0L, 240000L);
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

        context.setRecordContext(createRecordContext(300000));
        windowStore.put(0, "v");

        iter = windowStore.fetch(0, 240000L, 1000000L);
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

        windowStore = createWindowStore(context);

        new File(storeDir, segments.segmentName(0L)).mkdir();
        new File(storeDir, segments.segmentName(1L)).mkdir();
        new File(storeDir, segments.segmentName(2L)).mkdir();
        new File(storeDir, segments.segmentName(3L)).mkdir();
        new File(storeDir, segments.segmentName(4L)).mkdir();
        new File(storeDir, segments.segmentName(5L)).mkdir();
        new File(storeDir, segments.segmentName(6L)).mkdir();
        windowStore.close();

        windowStore = createWindowStore(context);

        assertEquals(
            Utils.mkSet(segments.segmentName(4L), segments.segmentName(5L), segments.segmentName(6L)),
            segmentDirs(baseDir)
        );

        try (final WindowStoreIterator iter = windowStore.fetch(0, 0L, 1000000L)) {
            while (iter.hasNext()) {
                iter.next();
            }
        }

        assertEquals(
            Utils.mkSet(segments.segmentName(4L), segments.segmentName(5L), segments.segmentName(6L)),
            segmentDirs(baseDir)
        );
    }

    @Test
    public void shouldCloseOpenIteratorsWhenStoreIsClosedAndThrowInvalidStateStoreExceptionOnHasNextAndNext() {
        windowStore = createWindowStore(context);
        context.setRecordContext(createRecordContext(0));
        windowStore.put(1, "one", 1L);
        windowStore.put(1, "two", 2L);
        windowStore.put(1, "three", 3L);

        final WindowStoreIterator<String> iterator = windowStore.fetch(1, 1L, 3L);
        assertTrue(iterator.hasNext());
        windowStore.close();
        try {
            //noinspection ResultOfMethodCallIgnored
            iterator.hasNext();
            fail("should have thrown InvalidStateStoreException on closed store");
        } catch (final InvalidStateStoreException e) {
            // ok
        }

        try {
            iterator.next();
            fail("should have thrown InvalidStateStoreException on closed store");
        } catch (final InvalidStateStoreException e) {
            // ok
        }
    }

    @Test
    public void shouldFetchAndIterateOverExactKeys() {
        final long windowSize = 0x7a00000000000000L;
        final long retentionPeriod = 0x7a00000000000000L;
        final WindowStore<String, String> windowStore = Stores.windowStoreBuilder(
            Stores.persistentWindowStore(
                windowName,
                retentionPeriod,
                2,
                windowSize,
                true),
            Serdes.String(),
            Serdes.String()).build();

        windowStore.init(context, windowStore);

        windowStore.put("a", "0001", 0);
        windowStore.put("aa", "0002", 0);
        windowStore.put("a", "0003", 1);
        windowStore.put("aa", "0004", 1);
        windowStore.put("a", "0005", 0x7a00000000000000L - 1);


        final List expected = Utils.mkList("0001", "0003", "0005");
        assertThat(toList(windowStore.fetch("a", 0, Long.MAX_VALUE)), equalTo(expected));

        List<KeyValue<Windowed<String>, String>> list = StreamsTestUtils.toList(windowStore.fetch("a", "a", 0, Long.MAX_VALUE));
        assertThat(list, equalTo(Utils.mkList(
            windowedPair("a", "0001", 0, windowSize),
            windowedPair("a", "0003", 1, windowSize),
            windowedPair("a", "0005", 0x7a00000000000000L - 1, windowSize)
        )));

        list = StreamsTestUtils.toList(windowStore.fetch("aa", "aa", 0, Long.MAX_VALUE));
        assertThat(list, equalTo(Utils.mkList(
            windowedPair("aa", "0002", 0, windowSize),
            windowedPair("aa", "0004", 1, windowSize)
        )));
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnPutNullKey() {
        windowStore = createWindowStore(context);
        windowStore.put(null, "anyValue");
    }

    @Test
    public void shouldNotThrowNullPointerExceptionOnPutNullValue() {
        windowStore = createWindowStore(context);
        windowStore.put(1, null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnGetNullKey() {
        windowStore = createWindowStore(context);
        windowStore.fetch(null, 1L, 2L);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnRangeNullFromKey() {
        windowStore = createWindowStore(context);
        windowStore.fetch(null, 2, 1L, 2L);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnRangeNullToKey() {
        windowStore = createWindowStore(context);
        windowStore.fetch(1, null, 1L, 2L);
    }

    @Test
    public void shouldNoNullPointerWhenSerdeDoesNotHandleNull() {
        windowStore = new RocksDBWindowStore<>(
            new RocksDBSegmentedBytesStore(windowName, retentionPeriod, numSegments, new WindowKeySchema()),
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
            Stores.persistentWindowStore(
                windowName,
                60000,
                2,
                60000,
                true),
            Serdes.Bytes(),
            Serdes.String()).build();

        windowStore.init(context, windowStore);

        final Bytes key1 = Bytes.wrap(new byte[]{0});
        final Bytes key2 = Bytes.wrap(new byte[]{0, 0});
        final Bytes key3 = Bytes.wrap(new byte[]{0, 0, 0});
        windowStore.put(key1, "1", 0);
        windowStore.put(key2, "2", 0);
        windowStore.put(key3, "3", 0);
        windowStore.put(key1, "4", 1);
        windowStore.put(key2, "5", 1);
        windowStore.put(key3, "6", 59999);
        windowStore.put(key1, "7", 59999);
        windowStore.put(key2, "8", 59999);
        windowStore.put(key3, "9", 59999);

        final List expectedKey1 = Utils.mkList("1", "4", "7");
        assertThat(toList(windowStore.fetch(key1, 0, Long.MAX_VALUE)), equalTo(expectedKey1));
        final List expectedKey2 = Utils.mkList("2", "5", "8");
        assertThat(toList(windowStore.fetch(key2, 0, Long.MAX_VALUE)), equalTo(expectedKey2));
        final List expectedKey3 = Utils.mkList("3", "6", "9");
        assertThat(toList(windowStore.fetch(key3, 0, Long.MAX_VALUE)), equalTo(expectedKey3));
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

        return new HashSet<>(Arrays.asList(requireNonNull(windowDir.list())));
    }

    private Map<Integer, Set<String>> entriesByKey(final List<KeyValue<byte[], byte[]>> changeLog,
                                                   @SuppressWarnings("SameParameterValue") final long startTime) {
        final HashMap<Integer, Set<String>> entriesByKey = new HashMap<>();

        for (final KeyValue<byte[], byte[]> entry : changeLog) {
            final long timestamp = WindowKeySchema.extractStoreTimestamp(entry.key);

            final Integer key = WindowKeySchema.extractStoreKey(entry.key, serdes);
            final String value = entry.value == null ? null : serdes.valueFrom(entry.value);

            Set<String> entries = entriesByKey.get(key);
            if (entries == null) {
                entries = new HashSet<>();
                entriesByKey.put(key, entries);
            }
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
