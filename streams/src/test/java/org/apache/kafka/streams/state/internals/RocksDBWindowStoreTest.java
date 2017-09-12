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
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.processor.internals.RecordCollectorImpl;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.test.MockProcessorContext;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RocksDBWindowStoreTest {

    private static final long DEFAULT_CACHE_SIZE_BYTES = 1024 * 1024L;

    private final int numSegments = 3;
    private final static long WINDOW_SIZE = 3;
    private final String windowName = "window";
    private final long segmentSize = Segments.MIN_SEGMENT_INTERVAL;
    private final long retentionPeriod = segmentSize * (numSegments - 1);
    private final Segments segments = new Segments(windowName, retentionPeriod, numSegments);
    private final StateSerdes<Integer, String> serdes = new StateSerdes<>("", Serdes.Integer(), Serdes.String());

    private final List<KeyValue<byte[], byte[]>> changeLog = new ArrayList<>();
    private final ThreadCache cache = new ThreadCache(new LogContext("TestCache "), DEFAULT_CACHE_SIZE_BYTES, new MockStreamsMetrics(new Metrics()));

    private final Producer<byte[], byte[]> producer = new MockProducer<>(true, Serdes.ByteArray().serializer(), Serdes.ByteArray().serializer());
    private final RecordCollector recordCollector = new RecordCollectorImpl(producer, "RocksDBWindowStoreTestTask", new LogContext("RocksDBWindowStoreTestTask ")) {
        @Override
        public <K1, V1> void send(final String topic,
                                  K1 key,
                                  V1 value,
                                  Integer partition,
                                  Long timestamp,
                                  Serializer<K1> keySerializer,
                                  Serializer<V1> valueSerializer) {
            changeLog.add(new KeyValue<>(
                    keySerializer.serialize(topic, key),
                    valueSerializer.serialize(topic, value))
            );
        }
    };

    private final File baseDir = TestUtils.tempDirectory("test");
    private final MockProcessorContext context = new MockProcessorContext(baseDir, Serdes.ByteArray(), Serdes.ByteArray(), recordCollector, cache);
    private WindowStore windowStore;

    @SuppressWarnings("unchecked")
    private <K, V> WindowStore<K, V> createWindowStore(ProcessorContext context, final boolean enableCaching, final boolean retainDuplicates) {
        final RocksDBWindowStoreSupplier supplier = new RocksDBWindowStoreSupplier<>(windowName, retentionPeriod, numSegments, retainDuplicates, Serdes.Integer(), Serdes.String(),
                                                                                     WINDOW_SIZE, true, Collections.<String, String>emptyMap(), enableCaching);
        final WindowStore<K, V> store = (WindowStore<K, V>) supplier.get();
        store.init(context, store);
        return store;
    }

    @After
    public void closeStore() {
        context.close();
        windowStore.close();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldOnlyIterateOpenSegments() {
        windowStore = createWindowStore(context, false, true);
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
        return new ProcessorRecordContext(time, 0, 0, "topic");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testPutAndFetch() throws IOException {
        windowStore = createWindowStore(context, false, true);
        long startTime = segmentSize - 4L;

        putFirstBatch(windowStore, startTime, context);

        assertEquals(Utils.mkList("zero"), toList(windowStore.fetch(0, startTime + 0L - WINDOW_SIZE, startTime + 0L + WINDOW_SIZE)));
        assertEquals(Utils.mkList("one"), toList(windowStore.fetch(1, startTime + 1L - WINDOW_SIZE, startTime + 1L + WINDOW_SIZE)));
        assertEquals(Utils.mkList("two"), toList(windowStore.fetch(2, startTime + 2L - WINDOW_SIZE, startTime + 2L + WINDOW_SIZE)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(3, startTime + 3L - WINDOW_SIZE, startTime + 3L + WINDOW_SIZE)));
        assertEquals(Utils.mkList("four"), toList(windowStore.fetch(4, startTime + 4L - WINDOW_SIZE, startTime + 4L + WINDOW_SIZE)));
        assertEquals(Utils.mkList("five"), toList(windowStore.fetch(5, startTime + 5L - WINDOW_SIZE, startTime + 5L + WINDOW_SIZE)));

        putSecondBatch(windowStore, startTime, context);

        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, startTime - 2L - WINDOW_SIZE, startTime - 2L + WINDOW_SIZE)));
        assertEquals(Utils.mkList("two"), toList(windowStore.fetch(2, startTime - 1L - WINDOW_SIZE, startTime - 1L + WINDOW_SIZE)));
        assertEquals(Utils.mkList("two", "two+1"), toList(windowStore.fetch(2, startTime - WINDOW_SIZE, startTime + WINDOW_SIZE)));
        assertEquals(Utils.mkList("two", "two+1", "two+2"), toList(windowStore.fetch(2, startTime + 1L - WINDOW_SIZE, startTime + 1L + WINDOW_SIZE)));
        assertEquals(Utils.mkList("two", "two+1", "two+2", "two+3"), toList(windowStore.fetch(2, startTime + 2L - WINDOW_SIZE, startTime + 2L + WINDOW_SIZE)));
        assertEquals(Utils.mkList("two", "two+1", "two+2", "two+3", "two+4"), toList(windowStore.fetch(2, startTime + 3L - WINDOW_SIZE, startTime + 3L + WINDOW_SIZE)));
        assertEquals(Utils.mkList("two", "two+1", "two+2", "two+3", "two+4", "two+5"), toList(windowStore.fetch(2, startTime + 4L - WINDOW_SIZE, startTime + 4L + WINDOW_SIZE)));
        assertEquals(Utils.mkList("two", "two+1", "two+2", "two+3", "two+4", "two+5", "two+6"), toList(windowStore.fetch(2, startTime + 5L - WINDOW_SIZE, startTime + 5L + WINDOW_SIZE)));
        assertEquals(Utils.mkList("two+1", "two+2", "two+3", "two+4", "two+5", "two+6"), toList(windowStore.fetch(2, startTime + 6L - WINDOW_SIZE, startTime + 6L + WINDOW_SIZE)));
        assertEquals(Utils.mkList("two+2", "two+3", "two+4", "two+5", "two+6"), toList(windowStore.fetch(2, startTime + 7L - WINDOW_SIZE, startTime + 7L + WINDOW_SIZE)));
        assertEquals(Utils.mkList("two+3", "two+4", "two+5", "two+6"), toList(windowStore.fetch(2, startTime + 8L - WINDOW_SIZE, startTime + 8L + WINDOW_SIZE)));
        assertEquals(Utils.mkList("two+4", "two+5", "two+6"), toList(windowStore.fetch(2, startTime + 9L - WINDOW_SIZE, startTime + 9L + WINDOW_SIZE)));
        assertEquals(Utils.mkList("two+5", "two+6"), toList(windowStore.fetch(2, startTime + 10L - WINDOW_SIZE, startTime + 10L + WINDOW_SIZE)));
        assertEquals(Utils.mkList("two+6"), toList(windowStore.fetch(2, startTime + 11L - WINDOW_SIZE, startTime + 11L + WINDOW_SIZE)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, startTime + 12L - WINDOW_SIZE, startTime + 12L + WINDOW_SIZE)));

        // Flush the store and verify all current entries were properly flushed ...
        windowStore.flush();

        Map<Integer, Set<String>> entriesByKey = entriesByKey(changeLog, startTime);

        assertEquals(Utils.mkSet("zero@0"), entriesByKey.get(0));
        assertEquals(Utils.mkSet("one@1"), entriesByKey.get(1));
        assertEquals(Utils.mkSet("two@2", "two+1@3", "two+2@4", "two+3@5", "two+4@6", "two+5@7", "two+6@8"), entriesByKey.get(2));
        assertNull(entriesByKey.get(3));
        assertEquals(Utils.mkSet("four@4"), entriesByKey.get(4));
        assertEquals(Utils.mkSet("five@5"), entriesByKey.get(5));
        assertNull(entriesByKey.get(6));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testFetchRange() throws IOException {
        windowStore = createWindowStore(context, false, true);
        long startTime = segmentSize - 4L;

        putFirstBatch(windowStore, startTime, context);

        final KeyValue<Windowed<Integer>, String> zero = windowedPair(0, "zero", startTime + 0);
        final KeyValue<Windowed<Integer>, String> one = windowedPair(1, "one", startTime + 1);
        final KeyValue<Windowed<Integer>, String> two = windowedPair(2, "two", startTime + 2);
        final KeyValue<Windowed<Integer>, String> four = windowedPair(4, "four", startTime + 4);
        final KeyValue<Windowed<Integer>, String> five = windowedPair(5, "five", startTime + 5);

        assertEquals(
            Utils.mkList(zero, one),
            StreamsTestUtils.toList(windowStore.fetch(0, 1, startTime + 0L - WINDOW_SIZE, startTime + 0L + WINDOW_SIZE))
        );
        assertEquals(
            Utils.mkList(one),
            StreamsTestUtils.toList(windowStore.fetch(1, 1, startTime + 0L - WINDOW_SIZE, startTime + 0L + WINDOW_SIZE))
        );
        assertEquals(
            Utils.mkList(one, two),
            StreamsTestUtils.toList(windowStore.fetch(1, 3, startTime + 0L - WINDOW_SIZE, startTime + 0L + WINDOW_SIZE))
        );
        assertEquals(
            Utils.mkList(zero, one, two),
            StreamsTestUtils.toList(windowStore.fetch(0, 5, startTime + 0L - WINDOW_SIZE, startTime + 0L + WINDOW_SIZE))
        );
        assertEquals(
            Utils.mkList(zero, one, two,
                         four, five),
            StreamsTestUtils.toList(windowStore.fetch(0, 5, startTime + 0L - WINDOW_SIZE, startTime + 0L + WINDOW_SIZE + 5L))
        );
        assertEquals(
            Utils.mkList(two, four, five),
            StreamsTestUtils.toList(windowStore.fetch(0, 5, startTime + 2L, startTime + 0L + WINDOW_SIZE + 5L))
        );
        assertEquals(
            Utils.mkList(),
            StreamsTestUtils.toList(windowStore.fetch(4, 5, startTime + 2L, startTime + WINDOW_SIZE))
        );
        assertEquals(
            Utils.mkList(),
            StreamsTestUtils.toList(windowStore.fetch(0, 3, startTime + 3L, startTime + WINDOW_SIZE + 5))
        );
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testPutAndFetchBefore() throws IOException {
        windowStore = createWindowStore(context, false, true);
        long startTime = segmentSize - 4L;

        putFirstBatch(windowStore, startTime, context);

        assertEquals(Utils.mkList("zero"), toList(windowStore.fetch(0, startTime + 0L - WINDOW_SIZE, startTime + 0L)));
        assertEquals(Utils.mkList("one"), toList(windowStore.fetch(1, startTime + 1L - WINDOW_SIZE, startTime + 1L)));
        assertEquals(Utils.mkList("two"), toList(windowStore.fetch(2, startTime + 2L - WINDOW_SIZE, startTime + 2L)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(3, startTime + 3L - WINDOW_SIZE, startTime + 3L)));
        assertEquals(Utils.mkList("four"), toList(windowStore.fetch(4, startTime + 4L - WINDOW_SIZE, startTime + 4L)));
        assertEquals(Utils.mkList("five"), toList(windowStore.fetch(5, startTime + 5L - WINDOW_SIZE, startTime + 5L)));

        putSecondBatch(windowStore, startTime, context);

        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, startTime - 1L - WINDOW_SIZE, startTime - 1L)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, startTime + 0L - WINDOW_SIZE, startTime + 0L)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, startTime + 1L - WINDOW_SIZE, startTime + 1L)));
        assertEquals(Utils.mkList("two"), toList(windowStore.fetch(2, startTime + 2L - WINDOW_SIZE, startTime + 2L)));
        assertEquals(Utils.mkList("two", "two+1"), toList(windowStore.fetch(2, startTime + 3L - WINDOW_SIZE, startTime + 3L)));
        assertEquals(Utils.mkList("two", "two+1", "two+2"), toList(windowStore.fetch(2, startTime + 4L - WINDOW_SIZE, startTime + 4L)));
        assertEquals(Utils.mkList("two", "two+1", "two+2", "two+3"), toList(windowStore.fetch(2, startTime + 5L - WINDOW_SIZE, startTime + 5L)));
        assertEquals(Utils.mkList("two+1", "two+2", "two+3", "two+4"), toList(windowStore.fetch(2, startTime + 6L - WINDOW_SIZE, startTime + 6L)));
        assertEquals(Utils.mkList("two+2", "two+3", "two+4", "two+5"), toList(windowStore.fetch(2, startTime + 7L - WINDOW_SIZE, startTime + 7L)));
        assertEquals(Utils.mkList("two+3", "two+4", "two+5", "two+6"), toList(windowStore.fetch(2, startTime + 8L - WINDOW_SIZE, startTime + 8L)));
        assertEquals(Utils.mkList("two+4", "two+5", "two+6"), toList(windowStore.fetch(2, startTime + 9L - WINDOW_SIZE, startTime + 9L)));
        assertEquals(Utils.mkList("two+5", "two+6"), toList(windowStore.fetch(2, startTime + 10L - WINDOW_SIZE, startTime + 10L)));
        assertEquals(Utils.mkList("two+6"), toList(windowStore.fetch(2, startTime + 11L - WINDOW_SIZE, startTime + 11L)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, startTime + 12L - WINDOW_SIZE, startTime + 12L)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, startTime + 13L - WINDOW_SIZE, startTime + 13L)));

        // Flush the store and verify all current entries were properly flushed ...
        windowStore.flush();

        Map<Integer, Set<String>> entriesByKey = entriesByKey(changeLog, startTime);

        assertEquals(Utils.mkSet("zero@0"), entriesByKey.get(0));
        assertEquals(Utils.mkSet("one@1"), entriesByKey.get(1));
        assertEquals(Utils.mkSet("two@2", "two+1@3", "two+2@4", "two+3@5", "two+4@6", "two+5@7", "two+6@8"), entriesByKey.get(2));
        assertNull(entriesByKey.get(3));
        assertEquals(Utils.mkSet("four@4"), entriesByKey.get(4));
        assertEquals(Utils.mkSet("five@5"), entriesByKey.get(5));
        assertNull(entriesByKey.get(6));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testPutAndFetchAfter() throws IOException {
        windowStore = createWindowStore(context, false, true);
        long startTime = segmentSize - 4L;

        putFirstBatch(windowStore, startTime, context);

        assertEquals(Utils.mkList("zero"), toList(windowStore.fetch(0, startTime + 0L, startTime + 0L + WINDOW_SIZE)));
        assertEquals(Utils.mkList("one"), toList(windowStore.fetch(1, startTime + 1L, startTime + 1L + WINDOW_SIZE)));
        assertEquals(Utils.mkList("two"), toList(windowStore.fetch(2, startTime + 2L, startTime + 2L + WINDOW_SIZE)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(3, startTime + 3L, startTime + 3L + WINDOW_SIZE)));
        assertEquals(Utils.mkList("four"), toList(windowStore.fetch(4, startTime + 4L, startTime + 4L + WINDOW_SIZE)));
        assertEquals(Utils.mkList("five"), toList(windowStore.fetch(5, startTime + 5L, startTime + 5L + WINDOW_SIZE)));

        putSecondBatch(windowStore, startTime, context);

        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, startTime - 2L, startTime - 2L + WINDOW_SIZE)));
        assertEquals(Utils.mkList("two"), toList(windowStore.fetch(2, startTime - 1L, startTime - 1L + WINDOW_SIZE)));
        assertEquals(Utils.mkList("two", "two+1"), toList(windowStore.fetch(2, startTime, startTime + WINDOW_SIZE)));
        assertEquals(Utils.mkList("two", "two+1", "two+2"), toList(windowStore.fetch(2, startTime + 1L, startTime + 1L + WINDOW_SIZE)));
        assertEquals(Utils.mkList("two", "two+1", "two+2", "two+3"), toList(windowStore.fetch(2, startTime + 2L, startTime + 2L + WINDOW_SIZE)));
        assertEquals(Utils.mkList("two+1", "two+2", "two+3", "two+4"), toList(windowStore.fetch(2, startTime + 3L, startTime + 3L + WINDOW_SIZE)));
        assertEquals(Utils.mkList("two+2", "two+3", "two+4", "two+5"), toList(windowStore.fetch(2, startTime + 4L, startTime + 4L + WINDOW_SIZE)));
        assertEquals(Utils.mkList("two+3", "two+4", "two+5", "two+6"), toList(windowStore.fetch(2, startTime + 5L, startTime + 5L + WINDOW_SIZE)));
        assertEquals(Utils.mkList("two+4", "two+5", "two+6"), toList(windowStore.fetch(2, startTime + 6L, startTime + 6L + WINDOW_SIZE)));
        assertEquals(Utils.mkList("two+5", "two+6"), toList(windowStore.fetch(2, startTime + 7L, startTime + 7L + WINDOW_SIZE)));
        assertEquals(Utils.mkList("two+6"), toList(windowStore.fetch(2, startTime + 8L, startTime + 8L + WINDOW_SIZE)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, startTime + 9L, startTime + 9L + WINDOW_SIZE)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, startTime + 10L, startTime + 10L + WINDOW_SIZE)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, startTime + 11L, startTime + 11L + WINDOW_SIZE)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, startTime + 12L, startTime + 12L + WINDOW_SIZE)));

        // Flush the store and verify all current entries were properly flushed ...
        windowStore.flush();

        Map<Integer, Set<String>> entriesByKey = entriesByKey(changeLog, startTime);

        assertEquals(Utils.mkSet("zero@0"), entriesByKey.get(0));
        assertEquals(Utils.mkSet("one@1"), entriesByKey.get(1));
        assertEquals(Utils.mkSet("two@2", "two+1@3", "two+2@4", "two+3@5", "two+4@6", "two+5@7", "two+6@8"), entriesByKey.get(2));
        assertNull(entriesByKey.get(3));
        assertEquals(Utils.mkSet("four@4"), entriesByKey.get(4));
        assertEquals(Utils.mkSet("five@5"), entriesByKey.get(5));
        assertNull(entriesByKey.get(6));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testPutSameKeyTimestamp() throws IOException {
        windowStore = createWindowStore(context, false, true);
        long startTime = segmentSize - 4L;

        context.setRecordContext(createRecordContext(startTime));
        windowStore.put(0, "zero");

        assertEquals(Utils.mkList("zero"), toList(windowStore.fetch(0, startTime - WINDOW_SIZE, startTime + WINDOW_SIZE)));

        windowStore.put(0, "zero");
        windowStore.put(0, "zero+");
        windowStore.put(0, "zero++");

        assertEquals(Utils.mkList("zero", "zero", "zero+", "zero++"), toList(windowStore.fetch(0, startTime - WINDOW_SIZE, startTime + WINDOW_SIZE)));
        assertEquals(Utils.mkList("zero", "zero", "zero+", "zero++"), toList(windowStore.fetch(0, startTime + 1L - WINDOW_SIZE, startTime + 1L + WINDOW_SIZE)));
        assertEquals(Utils.mkList("zero", "zero", "zero+", "zero++"), toList(windowStore.fetch(0, startTime + 2L - WINDOW_SIZE, startTime + 2L + WINDOW_SIZE)));
        assertEquals(Utils.mkList("zero", "zero", "zero+", "zero++"), toList(windowStore.fetch(0, startTime + 3L - WINDOW_SIZE, startTime + 3L + WINDOW_SIZE)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(0, startTime + 4L - WINDOW_SIZE, startTime + 4L + WINDOW_SIZE)));

        // Flush the store and verify all current entries were properly flushed ...
        windowStore.flush();

        Map<Integer, Set<String>> entriesByKey = entriesByKey(changeLog, startTime);

        assertEquals(Utils.mkSet("zero@0", "zero@0", "zero+@0", "zero++@0"), entriesByKey.get(0));
    }


    @SuppressWarnings("unchecked")
    @Test
    public void testRolling() throws IOException {
        windowStore = createWindowStore(context, false, true);

        // to validate segments
        final Segments segments = new Segments(windowName, retentionPeriod, numSegments);
        long startTime = segmentSize * 2;
        long incr = segmentSize / 2;
        context.setRecordContext(createRecordContext(startTime));
        windowStore.put(0, "zero");
        assertEquals(Utils.mkSet(segments.segmentName(2)), segmentDirs(baseDir));

        context.setRecordContext(createRecordContext(startTime + incr));
        windowStore.put(1, "one");
        assertEquals(Utils.mkSet(segments.segmentName(2)), segmentDirs(baseDir));

        context.setRecordContext(createRecordContext(startTime + incr * 2));
        windowStore.put(2, "two");
        assertEquals(Utils.mkSet(segments.segmentName(2),
                                 segments.segmentName(3)), segmentDirs(baseDir));

        context.setRecordContext(createRecordContext(startTime + incr * 4));
        windowStore.put(4, "four");
        assertEquals(Utils.mkSet(segments.segmentName(2),
                                 segments.segmentName(3),
                                 segments.segmentName(4)), segmentDirs(baseDir));


        context.setRecordContext(createRecordContext(startTime + incr * 5));
        windowStore.put(5, "five");
        assertEquals(Utils.mkSet(segments.segmentName(2),
                                 segments.segmentName(3),
                                 segments.segmentName(4)), segmentDirs(baseDir));

        assertEquals(Utils.mkList("zero"), toList(windowStore.fetch(0, startTime - WINDOW_SIZE, startTime + WINDOW_SIZE)));
        assertEquals(Utils.mkList("one"), toList(windowStore.fetch(1, startTime + incr - WINDOW_SIZE, startTime + incr + WINDOW_SIZE)));
        assertEquals(Utils.mkList("two"), toList(windowStore.fetch(2, startTime + incr * 2 - WINDOW_SIZE, startTime + incr * 2 + WINDOW_SIZE)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(3, startTime + incr * 3 - WINDOW_SIZE, startTime + incr * 3 + WINDOW_SIZE)));
        assertEquals(Utils.mkList("four"), toList(windowStore.fetch(4, startTime + incr * 4 - WINDOW_SIZE, startTime + incr * 4 + WINDOW_SIZE)));
        assertEquals(Utils.mkList("five"), toList(windowStore.fetch(5, startTime + incr * 5 - WINDOW_SIZE, startTime + incr * 5 + WINDOW_SIZE)));

        context.setRecordContext(createRecordContext(startTime + incr * 6));
        windowStore.put(6, "six");
        assertEquals(Utils.mkSet(segments.segmentName(3),
                                 segments.segmentName(4),
                                 segments.segmentName(5)), segmentDirs(baseDir));


        assertEquals(Utils.mkList(), toList(windowStore.fetch(0, startTime - WINDOW_SIZE, startTime + WINDOW_SIZE)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(1, startTime + incr - WINDOW_SIZE, startTime + incr + WINDOW_SIZE)));
        assertEquals(Utils.mkList("two"), toList(windowStore.fetch(2, startTime + incr * 2 - WINDOW_SIZE, startTime + incr * 2 + WINDOW_SIZE)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(3, startTime + incr * 3 - WINDOW_SIZE, startTime + incr * 3 + WINDOW_SIZE)));
        assertEquals(Utils.mkList("four"), toList(windowStore.fetch(4, startTime + incr * 4 - WINDOW_SIZE, startTime + incr * 4 + WINDOW_SIZE)));
        assertEquals(Utils.mkList("five"), toList(windowStore.fetch(5, startTime + incr * 5 - WINDOW_SIZE, startTime + incr * 5 + WINDOW_SIZE)));
        assertEquals(Utils.mkList("six"), toList(windowStore.fetch(6, startTime + incr * 6 - WINDOW_SIZE, startTime + incr * 6 + WINDOW_SIZE)));


        context.setRecordContext(createRecordContext(startTime + incr * 7));
        windowStore.put(7, "seven");
        assertEquals(Utils.mkSet(segments.segmentName(3),
                                 segments.segmentName(4),
                                 segments.segmentName(5)), segmentDirs(baseDir));

        assertEquals(Utils.mkList(), toList(windowStore.fetch(0, startTime - WINDOW_SIZE, startTime + WINDOW_SIZE)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(1, startTime + incr - WINDOW_SIZE, startTime + incr + WINDOW_SIZE)));
        assertEquals(Utils.mkList("two"), toList(windowStore.fetch(2, startTime + incr * 2 - WINDOW_SIZE, startTime + incr * 2 + WINDOW_SIZE)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(3, startTime + incr * 3 - WINDOW_SIZE, startTime + incr * 3 + WINDOW_SIZE)));
        assertEquals(Utils.mkList("four"), toList(windowStore.fetch(4, startTime + incr * 4 - WINDOW_SIZE, startTime + incr * 4 + WINDOW_SIZE)));
        assertEquals(Utils.mkList("five"), toList(windowStore.fetch(5, startTime + incr * 5 - WINDOW_SIZE, startTime + incr * 5 + WINDOW_SIZE)));
        assertEquals(Utils.mkList("six"), toList(windowStore.fetch(6, startTime + incr * 6 - WINDOW_SIZE, startTime + incr * 6 + WINDOW_SIZE)));
        assertEquals(Utils.mkList("seven"), toList(windowStore.fetch(7, startTime + incr * 7 - WINDOW_SIZE, startTime + incr * 7 + WINDOW_SIZE)));

        context.setRecordContext(createRecordContext(startTime + incr * 8));
        windowStore.put(8, "eight");
        assertEquals(Utils.mkSet(segments.segmentName(4),
                                 segments.segmentName(5),
                                 segments.segmentName(6)), segmentDirs(baseDir));


        assertEquals(Utils.mkList(), toList(windowStore.fetch(0, startTime - WINDOW_SIZE, startTime + WINDOW_SIZE)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(1, startTime + incr - WINDOW_SIZE, startTime + incr + WINDOW_SIZE)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, startTime + incr * 2 - WINDOW_SIZE, startTime + incr * 2 + WINDOW_SIZE)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(3, startTime + incr * 3 - WINDOW_SIZE, startTime + incr * 3 + WINDOW_SIZE)));
        assertEquals(Utils.mkList("four"), toList(windowStore.fetch(4, startTime + incr * 4 - WINDOW_SIZE, startTime + incr * 4 + WINDOW_SIZE)));
        assertEquals(Utils.mkList("five"), toList(windowStore.fetch(5, startTime + incr * 5 - WINDOW_SIZE, startTime + incr * 5 + WINDOW_SIZE)));
        assertEquals(Utils.mkList("six"), toList(windowStore.fetch(6, startTime + incr * 6 - WINDOW_SIZE, startTime + incr * 6 + WINDOW_SIZE)));
        assertEquals(Utils.mkList("seven"), toList(windowStore.fetch(7, startTime + incr * 7 - WINDOW_SIZE, startTime + incr * 7 + WINDOW_SIZE)));
        assertEquals(Utils.mkList("eight"), toList(windowStore.fetch(8, startTime + incr * 8 - WINDOW_SIZE, startTime + incr * 8 + WINDOW_SIZE)));

        // check segment directories
        windowStore.flush();
        assertEquals(Utils.mkSet(segments.segmentName(4),
                                 segments.segmentName(5),
                                 segments.segmentName(6)), segmentDirs(baseDir));


    }


    @SuppressWarnings("unchecked")
    @Test
    public void testRestore() throws IOException {
        long startTime = segmentSize * 2;
        long incr = segmentSize / 2;

        windowStore = createWindowStore(context, false, true);
        context.setRecordContext(createRecordContext(startTime));
        windowStore.put(0, "zero");
        context.setRecordContext(createRecordContext(startTime + incr));
        windowStore.put(1, "one");
        context.setRecordContext(createRecordContext(startTime + incr * 2));
        windowStore.put(2, "two");
        context.setRecordContext(createRecordContext(startTime + incr * 3));
        windowStore.put(3, "three");
        context.setRecordContext(createRecordContext(startTime + incr * 4));
        windowStore.put(4, "four");
        context.setRecordContext(createRecordContext(startTime + incr * 5));
        windowStore.put(5, "five");
        context.setRecordContext(createRecordContext(startTime + incr * 6));
        windowStore.put(6, "six");
        context.setRecordContext(createRecordContext(startTime + incr * 7));
        windowStore.put(7, "seven");
        context.setRecordContext(createRecordContext(startTime + incr * 8));
        windowStore.put(8, "eight");
        windowStore.flush();

        windowStore.close();

        // remove local store image
        Utils.delete(baseDir);

        windowStore = createWindowStore(context, false, true);
        assertEquals(Utils.mkList(), toList(windowStore.fetch(0, startTime - WINDOW_SIZE, startTime + WINDOW_SIZE)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(1, startTime + incr - WINDOW_SIZE, startTime + incr + WINDOW_SIZE)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, startTime + incr * 2 - WINDOW_SIZE, startTime + incr * 2 + WINDOW_SIZE)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(3, startTime + incr * 3 - WINDOW_SIZE, startTime + incr * 3 + WINDOW_SIZE)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(4, startTime + incr * 4 - WINDOW_SIZE, startTime + incr * 4 + WINDOW_SIZE)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(5, startTime + incr * 5 - WINDOW_SIZE, startTime + incr * 5 + WINDOW_SIZE)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(6, startTime + incr * 6 - WINDOW_SIZE, startTime + incr * 6 + WINDOW_SIZE)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(7, startTime + incr * 7 - WINDOW_SIZE, startTime + incr * 7 + WINDOW_SIZE)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(8, startTime + incr * 8 - WINDOW_SIZE, startTime + incr * 8 + WINDOW_SIZE)));

        context.restore(windowName, changeLog);

        assertEquals(Utils.mkList(), toList(windowStore.fetch(0, startTime - WINDOW_SIZE, startTime + WINDOW_SIZE)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(1, startTime + incr - WINDOW_SIZE, startTime + incr + WINDOW_SIZE)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, startTime + incr * 2 - WINDOW_SIZE, startTime + incr * 2 + WINDOW_SIZE)));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(3, startTime + incr * 3 - WINDOW_SIZE, startTime + incr * 3 + WINDOW_SIZE)));
        assertEquals(Utils.mkList("four"), toList(windowStore.fetch(4, startTime + incr * 4 - WINDOW_SIZE, startTime + incr * 4 + WINDOW_SIZE)));
        assertEquals(Utils.mkList("five"), toList(windowStore.fetch(5, startTime + incr * 5 - WINDOW_SIZE, startTime + incr * 5 + WINDOW_SIZE)));
        assertEquals(Utils.mkList("six"), toList(windowStore.fetch(6, startTime + incr * 6 - WINDOW_SIZE, startTime + incr * 6 + WINDOW_SIZE)));
        assertEquals(Utils.mkList("seven"), toList(windowStore.fetch(7, startTime + incr * 7 - WINDOW_SIZE, startTime + incr * 7 + WINDOW_SIZE)));
        assertEquals(Utils.mkList("eight"), toList(windowStore.fetch(8, startTime + incr * 8 - WINDOW_SIZE, startTime + incr * 8 + WINDOW_SIZE)));

        // check segment directories
        windowStore.flush();
        assertEquals(
                Utils.mkSet(segments.segmentName(4L), segments.segmentName(5L), segments.segmentName(6L)),
                segmentDirs(baseDir)
        );
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSegmentMaintenance() throws IOException {
        windowStore = createWindowStore(context, false, true);
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

    @SuppressWarnings("unchecked")
    @Test
    public void testInitialLoading() throws IOException {
        File storeDir = new File(baseDir, windowName);

        windowStore = createWindowStore(context, false, true);

        new File(storeDir, segments.segmentName(0L)).mkdir();
        new File(storeDir, segments.segmentName(1L)).mkdir();
        new File(storeDir, segments.segmentName(2L)).mkdir();
        new File(storeDir, segments.segmentName(3L)).mkdir();
        new File(storeDir, segments.segmentName(4L)).mkdir();
        new File(storeDir, segments.segmentName(5L)).mkdir();
        new File(storeDir, segments.segmentName(6L)).mkdir();
        windowStore.close();

        windowStore = createWindowStore(context, false, true);

        assertEquals(
                Utils.mkSet(segments.segmentName(4L), segments.segmentName(5L), segments.segmentName(6L)),
                segmentDirs(baseDir)
        );

        try (WindowStoreIterator iter = windowStore.fetch(0, 0L, 1000000L)) {
            while (iter.hasNext()) {
                iter.next();
            }
        }

        assertEquals(
                Utils.mkSet(segments.segmentName(4L), segments.segmentName(5L), segments.segmentName(6L)),
                segmentDirs(baseDir)
        );
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldCloseOpenIteratorsWhenStoreIsClosedAndThrowInvalidStateStoreExceptionOnHasNextAndNext() {
        windowStore = createWindowStore(context, false, true);
        context.setRecordContext(createRecordContext(0));
        windowStore.put(1, "one", 1L);
        windowStore.put(1, "two", 2L);
        windowStore.put(1, "three", 3L);

        final WindowStoreIterator<String> iterator = windowStore.fetch(1, 1L, 3L);
        assertTrue(iterator.hasNext());
        windowStore.close();
        try {
            iterator.hasNext();
            fail("should have thrown InvalidStateStoreException on closed store");
        } catch (InvalidStateStoreException e) {
            // ok
        }

        try {
            iterator.next();
            fail("should have thrown InvalidStateStoreException on closed store");
        } catch (InvalidStateStoreException e) {
            // ok
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldFetchAndIterateOverExactKeys() {
        final long windowSize = 0x7a00000000000000L;
        final long retentionPeriod = 0x7a00000000000000L;
        final RocksDBWindowStoreSupplier<String, String> supplier =
                new RocksDBWindowStoreSupplier<>(
                    "window",
                    retentionPeriod, 2,
                    true,
                    Serdes.String(),
                    Serdes.String(),
                    windowSize,
                    true,
                    Collections.<String, String>emptyMap(),
                    false);

        windowStore = supplier.get();
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
        windowStore = createWindowStore(context, false, true);
        windowStore.put(null, "anyValue");
    }

    @Test
    public void shouldNotThrowNullPointerExceptionOnPutNullValue() {
        windowStore = createWindowStore(context, false, true);
        windowStore.put(1, null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnGetNullKey() {
        windowStore = createWindowStore(context, false, true);
        windowStore.fetch(null, 1L, 2L);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnRangeNullFromKey() {
        windowStore = createWindowStore(context, false, true);
        windowStore.fetch(null, 2, 1L, 2L);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnRangeNullToKey() {
        windowStore = createWindowStore(context, false, true);
        windowStore.fetch(1, null, 1L, 2L);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldFetchAndIterateOverExactBinaryKeys() {
        final RocksDBWindowStoreSupplier<Bytes, String> supplier =
                new RocksDBWindowStoreSupplier<>(
                        "window",
                        60000, 2,
                        true,
                        Serdes.Bytes(),
                        Serdes.String(),
                        60000,
                        true,
                        Collections.<String, String>emptyMap(),
                        false);

        windowStore = supplier.get();
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

    private void putFirstBatch(final WindowStore<Integer, String> store, final long startTime, final MockProcessorContext context) {
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

    private void putSecondBatch(final WindowStore<Integer, String> store, final long startTime, MockProcessorContext context) {
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

    private <E> List<E> toList(WindowStoreIterator<E> iterator) {
        ArrayList<E> list = new ArrayList<>();
        while (iterator.hasNext()) {
            list.add(iterator.next().value);
        }
        return list;
    }

    private Set<String> segmentDirs(File baseDir) {
        File windowDir = new File(baseDir, windowName);

        return new HashSet<>(Arrays.asList(windowDir.list()));
    }

    private Map<Integer, Set<String>> entriesByKey(List<KeyValue<byte[], byte[]>> changeLog, long startTime) {
        HashMap<Integer, Set<String>> entriesByKey = new HashMap<>();

        for (KeyValue<byte[], byte[]> entry : changeLog) {
            long timestamp = WindowStoreUtils.timestampFromBinaryKey(entry.key);
            Integer key = WindowStoreUtils.keyFromBinaryKey(entry.key, serdes);
            String value = entry.value == null ? null : serdes.valueFrom(entry.value);

            Set<String> entries = entriesByKey.get(key);
            if (entries == null) {
                entries = new HashSet<>();
                entriesByKey.put(key, entries);
            }
            entries.add(value + "@" + (timestamp - startTime));
        }

        return entriesByKey;
    }

    private static <K, V> KeyValue<Windowed<K>, V> windowedPair(K key, V value, long timestamp) {
        return windowedPair(key, value, timestamp, WINDOW_SIZE);
    }

    private static <K, V> KeyValue<Windowed<K>, V> windowedPair(K key, V value, long timestamp, long windowSize) {
        return KeyValue.pair(new Windowed<>(key, WindowStoreUtils.timeWindowForSize(timestamp, windowSize)), value);
    }
}
