/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.processor.internals.RecordCollectorImpl;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.test.MockProcessorContext;
import org.apache.kafka.test.TestUtils;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RocksDBWindowStoreTest {

    private static final long DEFAULT_CACHE_SIZE_BYTES = 1024 * 1024L;

    private final int numSegments = 3;
    private final long windowSize = 3;
    private final String windowName = "window";
    private final long segmentSize = Segments.MIN_SEGMENT_INTERVAL;
    private final long retentionPeriod = segmentSize * (numSegments - 1);
    private final Segments segments = new Segments(windowName, retentionPeriod, numSegments);
    private final StateSerdes<Integer, String> serdes = new StateSerdes<>("", Serdes.Integer(), Serdes.String());

    private final List<KeyValue<byte[], byte[]>> changeLog = new ArrayList<>();
    private final ThreadCache cache = new ThreadCache("TestCache", DEFAULT_CACHE_SIZE_BYTES, new MockStreamsMetrics(new Metrics()));

    private final Producer<byte[], byte[]> producer = new MockProducer<>(true, Serdes.ByteArray().serializer(), Serdes.ByteArray().serializer());
    private final RecordCollector recordCollector = new RecordCollectorImpl(producer, "RocksDBWindowStoreTestTask") {
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

    @SuppressWarnings("unchecked")
    private <K, V> WindowStore<K, V> createWindowStore(ProcessorContext context, final boolean enableCaching, final boolean retainDuplicates) {
        final RocksDBWindowStoreSupplier supplier = new RocksDBWindowStoreSupplier<>(windowName, retentionPeriod, numSegments, retainDuplicates, Serdes.Integer(), Serdes.String(), windowSize, true, Collections.<String, String>emptyMap(), enableCaching);
        final WindowStore<K, V> store = (WindowStore<K, V>) supplier.get();
        store.init(context, store);
        return store;
    }

    @Test
    public void shouldOnlyIterateOpenSegments() throws Exception {
        final WindowStore<Integer, String> windowStore = createWindowStore(context, false, true);
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

    @Test
    public void testPutAndFetch() throws IOException {
        WindowStore<Integer, String> store = createWindowStore(context, false, true);
        try {
            long startTime = segmentSize - 4L;

            putFirstBatch(store, startTime, context);

            assertEquals(Utils.mkList("zero"), toList(store.fetch(0, startTime + 0L - windowSize, startTime + 0L + windowSize)));
            assertEquals(Utils.mkList("one"), toList(store.fetch(1, startTime + 1L - windowSize, startTime + 1L + windowSize)));
            assertEquals(Utils.mkList("two"), toList(store.fetch(2, startTime + 2L - windowSize, startTime + 2L + windowSize)));
            assertEquals(Utils.mkList(), toList(store.fetch(3, startTime + 3L - windowSize, startTime + 3L + windowSize)));
            assertEquals(Utils.mkList("four"), toList(store.fetch(4, startTime + 4L - windowSize, startTime + 4L + windowSize)));
            assertEquals(Utils.mkList("five"), toList(store.fetch(5, startTime + 5L - windowSize, startTime + 5L + windowSize)));

            putSecondBatch(store, startTime, context);

            assertEquals(Utils.mkList(), toList(store.fetch(2, startTime - 2L - windowSize, startTime - 2L + windowSize)));
            assertEquals(Utils.mkList("two"), toList(store.fetch(2, startTime - 1L - windowSize, startTime - 1L + windowSize)));
            assertEquals(Utils.mkList("two", "two+1"), toList(store.fetch(2, startTime - windowSize, startTime + windowSize)));
            assertEquals(Utils.mkList("two", "two+1", "two+2"), toList(store.fetch(2, startTime + 1L - windowSize, startTime + 1L + windowSize)));
            assertEquals(Utils.mkList("two", "two+1", "two+2", "two+3"), toList(store.fetch(2, startTime + 2L - windowSize, startTime + 2L + windowSize)));
            assertEquals(Utils.mkList("two", "two+1", "two+2", "two+3", "two+4"), toList(store.fetch(2, startTime + 3L - windowSize, startTime + 3L + windowSize)));
            assertEquals(Utils.mkList("two", "two+1", "two+2", "two+3", "two+4", "two+5"), toList(store.fetch(2, startTime + 4L - windowSize, startTime + 4L + windowSize)));
            assertEquals(Utils.mkList("two", "two+1", "two+2", "two+3", "two+4", "two+5", "two+6"), toList(store.fetch(2, startTime + 5L - windowSize, startTime + 5L + windowSize)));
            assertEquals(Utils.mkList("two+1", "two+2", "two+3", "two+4", "two+5", "two+6"), toList(store.fetch(2, startTime + 6L - windowSize, startTime + 6L + windowSize)));
            assertEquals(Utils.mkList("two+2", "two+3", "two+4", "two+5", "two+6"), toList(store.fetch(2, startTime + 7L - windowSize, startTime + 7L + windowSize)));
            assertEquals(Utils.mkList("two+3", "two+4", "two+5", "two+6"), toList(store.fetch(2, startTime + 8L - windowSize, startTime + 8L + windowSize)));
            assertEquals(Utils.mkList("two+4", "two+5", "two+6"), toList(store.fetch(2, startTime + 9L - windowSize, startTime + 9L + windowSize)));
            assertEquals(Utils.mkList("two+5", "two+6"), toList(store.fetch(2, startTime + 10L - windowSize, startTime + 10L + windowSize)));
            assertEquals(Utils.mkList("two+6"), toList(store.fetch(2, startTime + 11L - windowSize, startTime + 11L + windowSize)));
            assertEquals(Utils.mkList(), toList(store.fetch(2, startTime + 12L - windowSize, startTime + 12L + windowSize)));

            // Flush the store and verify all current entries were properly flushed ...
            store.flush();

            Map<Integer, Set<String>> entriesByKey = entriesByKey(changeLog, startTime);

            assertEquals(Utils.mkSet("zero@0"), entriesByKey.get(0));
            assertEquals(Utils.mkSet("one@1"), entriesByKey.get(1));
            assertEquals(Utils.mkSet("two@2", "two+1@3", "two+2@4", "two+3@5", "two+4@6", "two+5@7", "two+6@8"), entriesByKey.get(2));
            assertNull(entriesByKey.get(3));
            assertEquals(Utils.mkSet("four@4"), entriesByKey.get(4));
            assertEquals(Utils.mkSet("five@5"), entriesByKey.get(5));
            assertNull(entriesByKey.get(6));

        } finally {
            store.close();
        }
    }

    @Test
    public void testPutAndFetchBefore() throws IOException {
        WindowStore<Integer, String> store = createWindowStore(context, false, true);
        try {
            long startTime = segmentSize - 4L;

            putFirstBatch(store, startTime, context);

            assertEquals(Utils.mkList("zero"), toList(store.fetch(0, startTime + 0L - windowSize, startTime + 0L)));
            assertEquals(Utils.mkList("one"), toList(store.fetch(1, startTime + 1L - windowSize, startTime + 1L)));
            assertEquals(Utils.mkList("two"), toList(store.fetch(2, startTime + 2L - windowSize, startTime + 2L)));
            assertEquals(Utils.mkList(), toList(store.fetch(3, startTime + 3L - windowSize, startTime + 3L)));
            assertEquals(Utils.mkList("four"), toList(store.fetch(4, startTime + 4L - windowSize, startTime + 4L)));
            assertEquals(Utils.mkList("five"), toList(store.fetch(5, startTime + 5L - windowSize, startTime + 5L)));

            putSecondBatch(store, startTime, context);

            assertEquals(Utils.mkList(), toList(store.fetch(2, startTime - 1L - windowSize, startTime - 1L)));
            assertEquals(Utils.mkList(), toList(store.fetch(2, startTime + 0L - windowSize, startTime + 0L)));
            assertEquals(Utils.mkList(), toList(store.fetch(2, startTime + 1L - windowSize, startTime + 1L)));
            assertEquals(Utils.mkList("two"), toList(store.fetch(2, startTime + 2L - windowSize, startTime + 2L)));
            assertEquals(Utils.mkList("two", "two+1"), toList(store.fetch(2, startTime + 3L - windowSize, startTime + 3L)));
            assertEquals(Utils.mkList("two", "two+1", "two+2"), toList(store.fetch(2, startTime + 4L - windowSize, startTime + 4L)));
            assertEquals(Utils.mkList("two", "two+1", "two+2", "two+3"), toList(store.fetch(2, startTime + 5L - windowSize, startTime + 5L)));
            assertEquals(Utils.mkList("two+1", "two+2", "two+3", "two+4"), toList(store.fetch(2, startTime + 6L - windowSize, startTime + 6L)));
            assertEquals(Utils.mkList("two+2", "two+3", "two+4", "two+5"), toList(store.fetch(2, startTime + 7L - windowSize, startTime + 7L)));
            assertEquals(Utils.mkList("two+3", "two+4", "two+5", "two+6"), toList(store.fetch(2, startTime + 8L - windowSize, startTime + 8L)));
            assertEquals(Utils.mkList("two+4", "two+5", "two+6"), toList(store.fetch(2, startTime + 9L - windowSize, startTime + 9L)));
            assertEquals(Utils.mkList("two+5", "two+6"), toList(store.fetch(2, startTime + 10L - windowSize, startTime + 10L)));
            assertEquals(Utils.mkList("two+6"), toList(store.fetch(2, startTime + 11L - windowSize, startTime + 11L)));
            assertEquals(Utils.mkList(), toList(store.fetch(2, startTime + 12L - windowSize, startTime + 12L)));
            assertEquals(Utils.mkList(), toList(store.fetch(2, startTime + 13L - windowSize, startTime + 13L)));

            // Flush the store and verify all current entries were properly flushed ...
            store.flush();

            Map<Integer, Set<String>> entriesByKey = entriesByKey(changeLog, startTime);

            assertEquals(Utils.mkSet("zero@0"), entriesByKey.get(0));
            assertEquals(Utils.mkSet("one@1"), entriesByKey.get(1));
            assertEquals(Utils.mkSet("two@2", "two+1@3", "two+2@4", "two+3@5", "two+4@6", "two+5@7", "two+6@8"), entriesByKey.get(2));
            assertNull(entriesByKey.get(3));
            assertEquals(Utils.mkSet("four@4"), entriesByKey.get(4));
            assertEquals(Utils.mkSet("five@5"), entriesByKey.get(5));
            assertNull(entriesByKey.get(6));

        } finally {
            store.close();
        }
    }

    @Test
    public void testPutAndFetchAfter() throws IOException {
        WindowStore<Integer, String> store = createWindowStore(context, false, true);
        try {
            long startTime = segmentSize - 4L;

            putFirstBatch(store, startTime, context);

            assertEquals(Utils.mkList("zero"), toList(store.fetch(0, startTime + 0L, startTime + 0L + windowSize)));
            assertEquals(Utils.mkList("one"), toList(store.fetch(1, startTime + 1L, startTime + 1L + windowSize)));
            assertEquals(Utils.mkList("two"), toList(store.fetch(2, startTime + 2L, startTime + 2L + windowSize)));
            assertEquals(Utils.mkList(), toList(store.fetch(3, startTime + 3L, startTime + 3L + windowSize)));
            assertEquals(Utils.mkList("four"), toList(store.fetch(4, startTime + 4L, startTime + 4L + windowSize)));
            assertEquals(Utils.mkList("five"), toList(store.fetch(5, startTime + 5L, startTime + 5L + windowSize)));

            putSecondBatch(store, startTime, context);

            assertEquals(Utils.mkList(), toList(store.fetch(2, startTime - 2L, startTime - 2L + windowSize)));
            assertEquals(Utils.mkList("two"), toList(store.fetch(2, startTime - 1L, startTime - 1L + windowSize)));
            assertEquals(Utils.mkList("two", "two+1"), toList(store.fetch(2, startTime, startTime + windowSize)));
            assertEquals(Utils.mkList("two", "two+1", "two+2"), toList(store.fetch(2, startTime + 1L, startTime + 1L + windowSize)));
            assertEquals(Utils.mkList("two", "two+1", "two+2", "two+3"), toList(store.fetch(2, startTime + 2L, startTime + 2L + windowSize)));
            assertEquals(Utils.mkList("two+1", "two+2", "two+3", "two+4"), toList(store.fetch(2, startTime + 3L, startTime + 3L + windowSize)));
            assertEquals(Utils.mkList("two+2", "two+3", "two+4", "two+5"), toList(store.fetch(2, startTime + 4L, startTime + 4L + windowSize)));
            assertEquals(Utils.mkList("two+3", "two+4", "two+5", "two+6"), toList(store.fetch(2, startTime + 5L, startTime + 5L + windowSize)));
            assertEquals(Utils.mkList("two+4", "two+5", "two+6"), toList(store.fetch(2, startTime + 6L, startTime + 6L + windowSize)));
            assertEquals(Utils.mkList("two+5", "two+6"), toList(store.fetch(2, startTime + 7L, startTime + 7L + windowSize)));
            assertEquals(Utils.mkList("two+6"), toList(store.fetch(2, startTime + 8L, startTime + 8L + windowSize)));
            assertEquals(Utils.mkList(), toList(store.fetch(2, startTime + 9L, startTime + 9L + windowSize)));
            assertEquals(Utils.mkList(), toList(store.fetch(2, startTime + 10L, startTime + 10L + windowSize)));
            assertEquals(Utils.mkList(), toList(store.fetch(2, startTime + 11L, startTime + 11L + windowSize)));
            assertEquals(Utils.mkList(), toList(store.fetch(2, startTime + 12L, startTime + 12L + windowSize)));

            // Flush the store and verify all current entries were properly flushed ...
            store.flush();

            Map<Integer, Set<String>> entriesByKey = entriesByKey(changeLog, startTime);

            assertEquals(Utils.mkSet("zero@0"), entriesByKey.get(0));
            assertEquals(Utils.mkSet("one@1"), entriesByKey.get(1));
            assertEquals(Utils.mkSet("two@2", "two+1@3", "two+2@4", "two+3@5", "two+4@6", "two+5@7", "two+6@8"), entriesByKey.get(2));
            assertNull(entriesByKey.get(3));
            assertEquals(Utils.mkSet("four@4"), entriesByKey.get(4));
            assertEquals(Utils.mkSet("five@5"), entriesByKey.get(5));
            assertNull(entriesByKey.get(6));

        } finally {
            store.close();
        }
    }

    @Test
    public void testPutSameKeyTimestamp() throws IOException {
        WindowStore<Integer, String> store = createWindowStore(context, false, true);
        try {
            long startTime = segmentSize - 4L;

            context.setRecordContext(createRecordContext(startTime));
            store.put(0, "zero");

            assertEquals(Utils.mkList("zero"), toList(store.fetch(0, startTime - windowSize, startTime + windowSize)));

            store.put(0, "zero");
            store.put(0, "zero+");
            store.put(0, "zero++");

            assertEquals(Utils.mkList("zero", "zero", "zero+", "zero++"), toList(store.fetch(0, startTime - windowSize, startTime + windowSize)));
            assertEquals(Utils.mkList("zero", "zero", "zero+", "zero++"), toList(store.fetch(0, startTime + 1L - windowSize, startTime + 1L + windowSize)));
            assertEquals(Utils.mkList("zero", "zero", "zero+", "zero++"), toList(store.fetch(0, startTime + 2L - windowSize, startTime + 2L + windowSize)));
            assertEquals(Utils.mkList("zero", "zero", "zero+", "zero++"), toList(store.fetch(0, startTime + 3L - windowSize, startTime + 3L + windowSize)));
            assertEquals(Utils.mkList(), toList(store.fetch(0, startTime + 4L - windowSize, startTime + 4L + windowSize)));

            // Flush the store and verify all current entries were properly flushed ...
            store.flush();

            Map<Integer, Set<String>> entriesByKey = entriesByKey(changeLog, startTime);

            assertEquals(Utils.mkSet("zero@0", "zero@0", "zero+@0", "zero++@0"), entriesByKey.get(0));

        } finally {
            store.close();
        }
    }

    @Test
    public void testCachingEnabled() throws IOException {
        WindowStore<Integer, String> store = createWindowStore(context, true, false);
        assertTrue(store instanceof CachedStateStore);
    }

    @Test
    public void testRolling() throws IOException {
        WindowStore<Integer, String> store = createWindowStore(context, false, true);

        try {
            // to validate segments
            final Segments segments = new Segments(windowName, retentionPeriod, numSegments);
            long startTime = segmentSize * 2;
            long incr = segmentSize / 2;
            context.setRecordContext(createRecordContext(startTime));
            store.put(0, "zero");
            assertEquals(Utils.mkSet(segments.segmentName(2)), segmentDirs(baseDir));

            context.setRecordContext(createRecordContext(startTime + incr));
            store.put(1, "one");
            assertEquals(Utils.mkSet(segments.segmentName(2)), segmentDirs(baseDir));

            context.setRecordContext(createRecordContext(startTime + incr * 2));
            store.put(2, "two");
            assertEquals(Utils.mkSet(segments.segmentName(2),
                    segments.segmentName(3)), segmentDirs(baseDir));

            context.setRecordContext(createRecordContext(startTime + incr * 4));
            store.put(4, "four");
            assertEquals(Utils.mkSet(segments.segmentName(2),
                    segments.segmentName(3),
                    segments.segmentName(4)), segmentDirs(baseDir));


            context.setRecordContext(createRecordContext(startTime + incr * 5));
            store.put(5, "five");
            assertEquals(Utils.mkSet(segments.segmentName(2),
                    segments.segmentName(3),
                    segments.segmentName(4)), segmentDirs(baseDir));

            assertEquals(Utils.mkList("zero"), toList(store.fetch(0, startTime - windowSize, startTime + windowSize)));
            assertEquals(Utils.mkList("one"), toList(store.fetch(1, startTime + incr - windowSize, startTime + incr + windowSize)));
            assertEquals(Utils.mkList("two"), toList(store.fetch(2, startTime + incr * 2 - windowSize, startTime + incr * 2 + windowSize)));
            assertEquals(Utils.mkList(), toList(store.fetch(3, startTime + incr * 3 - windowSize, startTime + incr * 3 + windowSize)));
            assertEquals(Utils.mkList("four"), toList(store.fetch(4, startTime + incr * 4 - windowSize, startTime + incr * 4 + windowSize)));
            assertEquals(Utils.mkList("five"), toList(store.fetch(5, startTime + incr * 5 - windowSize, startTime + incr * 5 + windowSize)));

            context.setRecordContext(createRecordContext(startTime + incr * 6));
            store.put(6, "six");
            assertEquals(Utils.mkSet(segments.segmentName(3),
                    segments.segmentName(4),
                    segments.segmentName(5)), segmentDirs(baseDir));


            assertEquals(Utils.mkList(), toList(store.fetch(0, startTime - windowSize, startTime + windowSize)));
            assertEquals(Utils.mkList(), toList(store.fetch(1, startTime + incr - windowSize, startTime + incr + windowSize)));
            assertEquals(Utils.mkList("two"), toList(store.fetch(2, startTime + incr * 2 - windowSize, startTime + incr * 2 + windowSize)));
            assertEquals(Utils.mkList(), toList(store.fetch(3, startTime + incr * 3 - windowSize, startTime + incr * 3 + windowSize)));
            assertEquals(Utils.mkList("four"), toList(store.fetch(4, startTime + incr * 4 - windowSize, startTime + incr * 4 + windowSize)));
            assertEquals(Utils.mkList("five"), toList(store.fetch(5, startTime + incr * 5 - windowSize, startTime + incr * 5 + windowSize)));
            assertEquals(Utils.mkList("six"), toList(store.fetch(6, startTime + incr * 6 - windowSize, startTime + incr * 6 + windowSize)));


            context.setRecordContext(createRecordContext(startTime + incr * 7));
            store.put(7, "seven");
            assertEquals(Utils.mkSet(segments.segmentName(3),
                    segments.segmentName(4),
                    segments.segmentName(5)), segmentDirs(baseDir));

            assertEquals(Utils.mkList(), toList(store.fetch(0, startTime - windowSize, startTime + windowSize)));
            assertEquals(Utils.mkList(), toList(store.fetch(1, startTime + incr - windowSize, startTime + incr + windowSize)));
            assertEquals(Utils.mkList("two"), toList(store.fetch(2, startTime + incr * 2 - windowSize, startTime + incr * 2 + windowSize)));
            assertEquals(Utils.mkList(), toList(store.fetch(3, startTime + incr * 3 - windowSize, startTime + incr * 3 + windowSize)));
            assertEquals(Utils.mkList("four"), toList(store.fetch(4, startTime + incr * 4 - windowSize, startTime + incr * 4 + windowSize)));
            assertEquals(Utils.mkList("five"), toList(store.fetch(5, startTime + incr * 5 - windowSize, startTime + incr * 5 + windowSize)));
            assertEquals(Utils.mkList("six"), toList(store.fetch(6, startTime + incr * 6 - windowSize, startTime + incr * 6 + windowSize)));
            assertEquals(Utils.mkList("seven"), toList(store.fetch(7, startTime + incr * 7 - windowSize, startTime + incr * 7 + windowSize)));

            context.setRecordContext(createRecordContext(startTime + incr * 8));
            store.put(8, "eight");
            assertEquals(Utils.mkSet(segments.segmentName(4),
                    segments.segmentName(5),
                    segments.segmentName(6)), segmentDirs(baseDir));


            assertEquals(Utils.mkList(), toList(store.fetch(0, startTime - windowSize, startTime + windowSize)));
            assertEquals(Utils.mkList(), toList(store.fetch(1, startTime + incr - windowSize, startTime + incr + windowSize)));
            assertEquals(Utils.mkList(), toList(store.fetch(2, startTime + incr * 2 - windowSize, startTime + incr * 2 + windowSize)));
            assertEquals(Utils.mkList(), toList(store.fetch(3, startTime + incr * 3 - windowSize, startTime + incr * 3 + windowSize)));
            assertEquals(Utils.mkList("four"), toList(store.fetch(4, startTime + incr * 4 - windowSize, startTime + incr * 4 + windowSize)));
            assertEquals(Utils.mkList("five"), toList(store.fetch(5, startTime + incr * 5 - windowSize, startTime + incr * 5 + windowSize)));
            assertEquals(Utils.mkList("six"), toList(store.fetch(6, startTime + incr * 6 - windowSize, startTime + incr * 6 + windowSize)));
            assertEquals(Utils.mkList("seven"), toList(store.fetch(7, startTime + incr * 7 - windowSize, startTime + incr * 7 + windowSize)));
            assertEquals(Utils.mkList("eight"), toList(store.fetch(8, startTime + incr * 8 - windowSize, startTime + incr * 8 + windowSize)));

            // check segment directories
            store.flush();
            assertEquals(Utils.mkSet(segments.segmentName(4),
                    segments.segmentName(5),
                    segments.segmentName(6)), segmentDirs(baseDir));

        } finally {
            store.close();
        }
    }


    @Test
    public void testRestore() throws IOException {
        long startTime = segmentSize * 2;
        long incr = segmentSize / 2;

        WindowStore<Integer, String> store = createWindowStore(context, false, true);
        try {
            context.setRecordContext(createRecordContext(startTime));
            store.put(0, "zero");
            context.setRecordContext(createRecordContext(startTime + incr));
            store.put(1, "one");
            context.setRecordContext(createRecordContext(startTime + incr * 2));
            store.put(2, "two");
            context.setRecordContext(createRecordContext(startTime + incr * 3));
            store.put(3, "three");
            context.setRecordContext(createRecordContext(startTime + incr * 4));
            store.put(4, "four");
            context.setRecordContext(createRecordContext(startTime + incr * 5));
            store.put(5, "five");
            context.setRecordContext(createRecordContext(startTime + incr * 6));
            store.put(6, "six");
            context.setRecordContext(createRecordContext(startTime + incr * 7));
            store.put(7, "seven");
            context.setRecordContext(createRecordContext(startTime + incr * 8));
            store.put(8, "eight");
            store.flush();

        } finally {
            store.close();
        }

        // remove local store image
        Utils.delete(baseDir);

        WindowStore<Integer, String> store2 = createWindowStore(context, false, true);
        assertEquals(Utils.mkList(), toList(store2.fetch(0, startTime - windowSize, startTime + windowSize)));
        assertEquals(Utils.mkList(), toList(store2.fetch(1, startTime + incr - windowSize, startTime + incr + windowSize)));
        assertEquals(Utils.mkList(), toList(store2.fetch(2, startTime + incr * 2 - windowSize, startTime + incr * 2 + windowSize)));
        assertEquals(Utils.mkList(), toList(store2.fetch(3, startTime + incr * 3 - windowSize, startTime + incr * 3 + windowSize)));
        assertEquals(Utils.mkList(), toList(store2.fetch(4, startTime + incr * 4 - windowSize, startTime + incr * 4 + windowSize)));
        assertEquals(Utils.mkList(), toList(store2.fetch(5, startTime + incr * 5 - windowSize, startTime + incr * 5 + windowSize)));
        assertEquals(Utils.mkList(), toList(store2.fetch(6, startTime + incr * 6 - windowSize, startTime + incr * 6 + windowSize)));
        assertEquals(Utils.mkList(), toList(store2.fetch(7, startTime + incr * 7 - windowSize, startTime + incr * 7 + windowSize)));
        assertEquals(Utils.mkList(), toList(store2.fetch(8, startTime + incr * 8 - windowSize, startTime + incr * 8 + windowSize)));

        try {
            context.restore(windowName, changeLog);

            assertEquals(Utils.mkList(), toList(store2.fetch(0, startTime - windowSize, startTime + windowSize)));
            assertEquals(Utils.mkList(), toList(store2.fetch(1, startTime + incr - windowSize, startTime + incr + windowSize)));
            assertEquals(Utils.mkList(), toList(store2.fetch(2, startTime + incr * 2 - windowSize, startTime + incr * 2 + windowSize)));
            assertEquals(Utils.mkList(), toList(store2.fetch(3, startTime + incr * 3 - windowSize, startTime + incr * 3 + windowSize)));
            assertEquals(Utils.mkList("four"), toList(store2.fetch(4, startTime + incr * 4 - windowSize, startTime + incr * 4 + windowSize)));
            assertEquals(Utils.mkList("five"), toList(store2.fetch(5, startTime + incr * 5 - windowSize, startTime + incr * 5 + windowSize)));
            assertEquals(Utils.mkList("six"), toList(store2.fetch(6, startTime + incr * 6 - windowSize, startTime + incr * 6 + windowSize)));
            assertEquals(Utils.mkList("seven"), toList(store2.fetch(7, startTime + incr * 7 - windowSize, startTime + incr * 7 + windowSize)));
            assertEquals(Utils.mkList("eight"), toList(store2.fetch(8, startTime + incr * 8 - windowSize, startTime + incr * 8 + windowSize)));

            // check segment directories
            store2.flush();
            assertEquals(
                    Utils.mkSet(segments.segmentName(4L), segments.segmentName(5L), segments.segmentName(6L)),
                    segmentDirs(baseDir)
            );
        } finally {
            store2.close();
        }
    }

    @Test
    public void testSegmentMaintenance() throws IOException {
        WindowStore<Integer, String> store = createWindowStore(context, false, true);
        try {
            context.setTime(0L);
            context.setRecordContext(createRecordContext(0));
            store.put(0, "v");
            assertEquals(
                    Utils.mkSet(segments.segmentName(0L)),
                    segmentDirs(baseDir)
            );

            context.setRecordContext(createRecordContext(59999));
            store.put(0, "v");
            store.put(0, "v");
            assertEquals(
                    Utils.mkSet(segments.segmentName(0L)),
                    segmentDirs(baseDir)
            );

            context.setRecordContext(createRecordContext(60000));
            store.put(0, "v");
            assertEquals(
                    Utils.mkSet(segments.segmentName(0L), segments.segmentName(1L)),
                    segmentDirs(baseDir)
            );

            WindowStoreIterator iter;
            int fetchedCount;

            iter = store.fetch(0, 0L, 240000L);
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
            store.put(0, "v");

            iter = store.fetch(0, 0L, 240000L);
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
            store.put(0, "v");

            iter = store.fetch(0, 240000L, 1000000L);
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

        } finally {
            store.close();
        }
    }

    @Test
    public void testInitialLoading() throws IOException {
        File storeDir = new File(baseDir, windowName);

        WindowStore<Integer, String> store = createWindowStore(context, false, true);

        try {
            new File(storeDir, segments.segmentName(0L)).mkdir();
            new File(storeDir, segments.segmentName(1L)).mkdir();
            new File(storeDir, segments.segmentName(2L)).mkdir();
            new File(storeDir, segments.segmentName(3L)).mkdir();
            new File(storeDir, segments.segmentName(4L)).mkdir();
            new File(storeDir, segments.segmentName(5L)).mkdir();
            new File(storeDir, segments.segmentName(6L)).mkdir();
        } finally {
            store.close();
        }

        store = createWindowStore(context, false, true);

        try {
            assertEquals(
                    Utils.mkSet(segments.segmentName(4L), segments.segmentName(5L), segments.segmentName(6L)),
                    segmentDirs(baseDir)
            );

            try (WindowStoreIterator iter = store.fetch(0, 0L, 1000000L)) {
                while (iter.hasNext()) {
                    iter.next();
                }
            }

            assertEquals(
                    Utils.mkSet(segments.segmentName(4L), segments.segmentName(5L), segments.segmentName(6L)),
                    segmentDirs(baseDir)
            );

        } finally {
            store.close();
        }
    }

    @Test
    public void shouldCloseOpenIteratorsWhenStoreIsClosedAndThrowInvalidStateStoreExceptionOnHasNextAndNext() throws Exception {
        final WindowStore<Integer, String> windowStore = createWindowStore(context, false, true);
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
}
