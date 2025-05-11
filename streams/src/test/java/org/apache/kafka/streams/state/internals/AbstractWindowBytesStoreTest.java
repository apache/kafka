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

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockRecordCollector;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static java.time.Instant.ofEpochMilli;
import static java.util.Arrays.asList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.test.StreamsTestUtils.toListAndCloseIterator;
import static org.apache.kafka.test.StreamsTestUtils.toSet;
import static org.apache.kafka.test.StreamsTestUtils.valuesToSetAndCloseIterator;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class AbstractWindowBytesStoreTest {

    static final long WINDOW_SIZE = 3L;
    static final long SEGMENT_INTERVAL = 60_000L;
    static final long RETENTION_PERIOD = 2 * SEGMENT_INTERVAL;

    final long defaultStartTime = SEGMENT_INTERVAL - 4L;

    final KeyValue<Windowed<Integer>, String> zero = windowedPair(0, "zero", defaultStartTime);
    final KeyValue<Windowed<Integer>, String> one = windowedPair(1, "one", defaultStartTime + 1);
    final KeyValue<Windowed<Integer>, String> two = windowedPair(2, "two", defaultStartTime + 2);
    final KeyValue<Windowed<Integer>, String> three = windowedPair(3, "three", defaultStartTime + 2);
    final KeyValue<Windowed<Integer>, String> four = windowedPair(4, "four", defaultStartTime + 4);
    final KeyValue<Windowed<Integer>, String> five = windowedPair(5, "five", defaultStartTime + 5);

    WindowStore<Integer, String> windowStore;
    InternalMockProcessorContext<?, ?> context;
    MockRecordCollector recordCollector;

    final File baseDir = TestUtils.tempDirectory("test");
    private final StateSerdes<Integer, String> serdes = new StateSerdes<>("", Serdes.Integer(), Serdes.String());

    abstract <K, V> WindowStore<K, V> buildWindowStore(final long retentionPeriod,
                                                       final long windowSize,
                                                       final boolean retainDuplicates,
                                                       final Serde<K> keySerde,
                                                       final Serde<V> valueSerde);
    @BeforeEach
    protected void setup() {
        
        windowStore = buildWindowStore(RETENTION_PERIOD, WINDOW_SIZE, false, Serdes.Integer(), Serdes.String());

        recordCollector = new MockRecordCollector();
        context = new InternalMockProcessorContext<>(
            baseDir,
            Serdes.String(),
            Serdes.Integer(),
            recordCollector,
            new ThreadCache(
                new LogContext("testCache"),
                0,
                new MockStreamsMetrics(new Metrics())));
        context.setTime(1L);

        windowStore.init(context, windowStore);
    }

    @AfterEach
    public void after() {
        windowStore.close();
    }

    @Test
    public void testRangeAndSinglePointFetch() {
        putFirstBatch(windowStore, defaultStartTime, context);

        assertEquals(
            new HashSet<>(Collections.singletonList("zero")),
            valuesToSetAndCloseIterator(windowStore.fetch(
                0,
                ofEpochMilli(defaultStartTime - WINDOW_SIZE),
                ofEpochMilli(defaultStartTime + WINDOW_SIZE))));

        putSecondBatch(windowStore, defaultStartTime);

        assertEquals("two+1", windowStore.fetch(2, defaultStartTime + 3L));
        assertEquals("two+2", windowStore.fetch(2, defaultStartTime + 4L));
        assertEquals("two+3", windowStore.fetch(2, defaultStartTime + 5L));
        assertEquals("two+4", windowStore.fetch(2, defaultStartTime + 6L));
        assertEquals("two+5", windowStore.fetch(2, defaultStartTime + 7L));
        assertEquals("two+6", windowStore.fetch(2, defaultStartTime + 8L));

        assertEquals(
            new HashSet<>(Collections.emptyList()),
            valuesToSetAndCloseIterator(windowStore.fetch(
                2,
                ofEpochMilli(defaultStartTime - 2L - WINDOW_SIZE),
                ofEpochMilli(defaultStartTime - 2L + WINDOW_SIZE))));
        assertEquals(
            new HashSet<>(Collections.singletonList("two")),
            valuesToSetAndCloseIterator(windowStore.fetch(
                2,
                ofEpochMilli(defaultStartTime - 1L - WINDOW_SIZE),
                ofEpochMilli(defaultStartTime - 1L + WINDOW_SIZE))));
        assertEquals(
            new HashSet<>(asList("two", "two+1")),
            valuesToSetAndCloseIterator(windowStore.fetch(
                2,
                ofEpochMilli(defaultStartTime - WINDOW_SIZE),
                ofEpochMilli(defaultStartTime + WINDOW_SIZE))));
        assertEquals(
            new HashSet<>(asList("two", "two+1", "two+2")),
            valuesToSetAndCloseIterator(windowStore.fetch(
                2,
                ofEpochMilli(defaultStartTime + 1L - WINDOW_SIZE),
                ofEpochMilli(defaultStartTime + 1L + WINDOW_SIZE))));
        assertEquals(
            new HashSet<>(asList("two", "two+1", "two+2", "two+3")),
            valuesToSetAndCloseIterator(windowStore.fetch(
                2,
                ofEpochMilli(defaultStartTime + 2L - WINDOW_SIZE),
                ofEpochMilli(defaultStartTime + 2L + WINDOW_SIZE))));
        assertEquals(
            new HashSet<>(asList("two", "two+1", "two+2", "two+3", "two+4")),
            valuesToSetAndCloseIterator(windowStore.fetch(
                2,
                ofEpochMilli(defaultStartTime + 3L - WINDOW_SIZE),
                ofEpochMilli(defaultStartTime + 3L + WINDOW_SIZE))));
        assertEquals(
            new HashSet<>(asList("two", "two+1", "two+2", "two+3", "two+4", "two+5")),
            valuesToSetAndCloseIterator(windowStore.fetch(
                2,
                ofEpochMilli(defaultStartTime + 4L - WINDOW_SIZE),
                ofEpochMilli(defaultStartTime + 4L + WINDOW_SIZE))));
        assertEquals(
            new HashSet<>(asList("two", "two+1", "two+2", "two+3", "two+4", "two+5", "two+6")),
            valuesToSetAndCloseIterator(windowStore.fetch(
                2,
                ofEpochMilli(defaultStartTime + 5L - WINDOW_SIZE),
                ofEpochMilli(defaultStartTime + 5L + WINDOW_SIZE))));
        assertEquals(
            new HashSet<>(asList("two+1", "two+2", "two+3", "two+4", "two+5", "two+6")),
            valuesToSetAndCloseIterator(windowStore.fetch(
                2,
                ofEpochMilli(defaultStartTime + 6L - WINDOW_SIZE),
                ofEpochMilli(defaultStartTime + 6L + WINDOW_SIZE))));
        assertEquals(
            new HashSet<>(asList("two+2", "two+3", "two+4", "two+5", "two+6")),
            valuesToSetAndCloseIterator(windowStore.fetch(
                2,
                ofEpochMilli(defaultStartTime + 7L - WINDOW_SIZE),
                ofEpochMilli(defaultStartTime + 7L + WINDOW_SIZE))));
        assertEquals(
            new HashSet<>(asList("two+3", "two+4", "two+5", "two+6")),
            valuesToSetAndCloseIterator(windowStore.fetch(
                2,
                ofEpochMilli(defaultStartTime + 8L - WINDOW_SIZE),
                ofEpochMilli(defaultStartTime + 8L + WINDOW_SIZE))));
        assertEquals(
            new HashSet<>(asList("two+4", "two+5", "two+6")),
            valuesToSetAndCloseIterator(windowStore.fetch(
                2,
                ofEpochMilli(defaultStartTime + 9L - WINDOW_SIZE),
                ofEpochMilli(defaultStartTime + 9L + WINDOW_SIZE))));
        assertEquals(
            new HashSet<>(asList("two+5", "two+6")),
            valuesToSetAndCloseIterator(windowStore.fetch(
                2,
                ofEpochMilli(defaultStartTime + 10L - WINDOW_SIZE),
                ofEpochMilli(defaultStartTime + 10L + WINDOW_SIZE))));
        assertEquals(
            new HashSet<>(Collections.singletonList("two+6")),
            valuesToSetAndCloseIterator(windowStore.fetch(
                2,
                ofEpochMilli(defaultStartTime + 11L - WINDOW_SIZE),
                ofEpochMilli(defaultStartTime + 11L + WINDOW_SIZE))));
        assertEquals(
            new HashSet<>(Collections.emptyList()),
            valuesToSetAndCloseIterator(windowStore.fetch(
                2,
                ofEpochMilli(defaultStartTime + 12L - WINDOW_SIZE),
                ofEpochMilli(defaultStartTime + 12L + WINDOW_SIZE))));

        // Flush the store and verify all current entries were properly flushed ...
        windowStore.flush();

        final List<KeyValue<byte[], byte[]>> changeLog = new ArrayList<>();
        for (final ProducerRecord<Object, Object> record : recordCollector.collected()) {
            changeLog.add(new KeyValue<>(((Bytes) record.key()).get(), (byte[]) record.value()));
        }

        final Map<Integer, Set<String>> entriesByKey = entriesByKey(changeLog, defaultStartTime);

        assertEquals(Set.of("zero@0"), entriesByKey.get(0));
        assertEquals(Set.of("one@1"), entriesByKey.get(1));
        assertEquals(
            Set.of("two@2", "two+1@3", "two+2@4", "two+3@5", "two+4@6", "two+5@7", "two+6@8"),
            entriesByKey.get(2));
        assertEquals(Set.of("three@2"), entriesByKey.get(3));
        assertEquals(Set.of("four@4"), entriesByKey.get(4));
        assertEquals(Set.of("five@5"), entriesByKey.get(5));
        assertNull(entriesByKey.get(6));
    }

    @Test
    public void shouldGetAll() {
        
        putFirstBatch(windowStore, defaultStartTime, context);

        assertEquals(
            asList(zero, one, two, three, four, five),
            toListAndCloseIterator(windowStore.all())
        );
    }

    @Test
    public void shouldGetAllNonDeletedRecords() {
        // Add some records
        windowStore.put(0, "zero", defaultStartTime);
        windowStore.put(1, "one", defaultStartTime + 1);
        windowStore.put(2, "two", defaultStartTime + 2);
        windowStore.put(3, "three", defaultStartTime + 3);
        windowStore.put(4, "four", defaultStartTime + 4);

        // Delete some records
        windowStore.put(1, null, defaultStartTime + 1);
        windowStore.put(3, null, defaultStartTime + 3);

        // Only non-deleted records should appear in the all() iterator
        assertEquals(
            asList(zero, two, four),
            toListAndCloseIterator(windowStore.all())
        );
    }

    @Test
    public void shouldGetAllReturnTimestampOrderedRecords() {
        // Add some records in different order
        windowStore.put(4, "four", defaultStartTime + 4);
        windowStore.put(0, "zero", defaultStartTime);
        windowStore.put(2, "two", defaultStartTime + 2);
        windowStore.put(3, "three", defaultStartTime + 3);
        windowStore.put(1, "one", defaultStartTime + 1);

        // Only non-deleted records should appear in the all() iterator
        final KeyValue<Windowed<Integer>, String> three = windowedPair(3, "three", defaultStartTime + 3);

        assertEquals(
            asList(zero, one, two, three, four),
            toListAndCloseIterator(windowStore.all())
        );
    }

    @Test
    public void shouldEarlyClosedIteratorStillGetAllRecords() {
        windowStore.put(0, "zero", defaultStartTime);
        windowStore.put(1, "one", defaultStartTime + 1);

        final KeyValueIterator<Windowed<Integer>, String> it = windowStore.all();
        assertEquals(zero, it.next());
        it.close();

        // A new all() iterator after a previous all() iterator was closed should return all elements.
        assertEquals(
            asList(zero, one),
            toListAndCloseIterator(windowStore.all())
        );
    }

    @Test
    public void shouldGetBackwardAll() {
        putFirstBatch(windowStore, defaultStartTime, context);

        assertEquals(
            asList(five, four, three, two, one, zero),
            toListAndCloseIterator(windowStore.backwardAll())
        );
    }

    @Test
    public void shouldFetchAllInTimeRange() {
        putFirstBatch(windowStore, defaultStartTime, context);

        assertEquals(
            asList(one, two, three, four),
            toListAndCloseIterator(windowStore.fetchAll(ofEpochMilli(defaultStartTime + 1), ofEpochMilli(defaultStartTime + 4)))
        );
        assertEquals(
            asList(zero, one, two, three),
            toListAndCloseIterator(windowStore.fetchAll(ofEpochMilli(defaultStartTime), ofEpochMilli(defaultStartTime + 3)))
        );
        assertEquals(
            asList(one, two, three, four, five),
            toListAndCloseIterator(windowStore.fetchAll(ofEpochMilli(defaultStartTime + 1), ofEpochMilli(defaultStartTime + 5)))
        );
    }

    @Test
    public void shouldBackwardFetchAllInTimeRange() {
        putFirstBatch(windowStore, defaultStartTime, context);

        assertEquals(
            asList(four, three, two, one),
            toListAndCloseIterator(windowStore.backwardFetchAll(ofEpochMilli(defaultStartTime + 1), ofEpochMilli(defaultStartTime + 4)))
        );
        assertEquals(
            asList(three, two, one, zero),
            toListAndCloseIterator(windowStore.backwardFetchAll(ofEpochMilli(defaultStartTime), ofEpochMilli(defaultStartTime + 3)))
        );
        assertEquals(
            asList(five, four, three, two, one),
            toListAndCloseIterator(windowStore.backwardFetchAll(ofEpochMilli(defaultStartTime + 1), ofEpochMilli(defaultStartTime + 5)))
        );
    }

    @Test
    public void testFetchRange() {
        putFirstBatch(windowStore, defaultStartTime, context);

        assertEquals(
            asList(zero, one),
            toListAndCloseIterator(windowStore.fetch(
                0,
                1,
                ofEpochMilli(defaultStartTime - WINDOW_SIZE),
                ofEpochMilli(defaultStartTime + WINDOW_SIZE)))
        );
        assertEquals(
            Collections.singletonList(one),
            toListAndCloseIterator(windowStore.fetch(
                1,
                1,
                ofEpochMilli(defaultStartTime - WINDOW_SIZE),
                ofEpochMilli(defaultStartTime + WINDOW_SIZE)))
        );
        assertEquals(
            asList(one, two, three),
            toListAndCloseIterator(windowStore.fetch(
                1,
                3,
                ofEpochMilli(defaultStartTime - WINDOW_SIZE),
                ofEpochMilli(defaultStartTime + WINDOW_SIZE)))
        );
        assertEquals(
            asList(zero, one, two, three),
            toListAndCloseIterator(windowStore.fetch(
                0,
                5,
                ofEpochMilli(defaultStartTime - WINDOW_SIZE),
                ofEpochMilli(defaultStartTime + WINDOW_SIZE)))
        );
        assertEquals(
            asList(zero, one, two, three, four, five),
            toListAndCloseIterator(windowStore.fetch(
                0,
                5,
                ofEpochMilli(defaultStartTime - WINDOW_SIZE),
                ofEpochMilli(defaultStartTime + WINDOW_SIZE + 5L)))
        );
        assertEquals(
            asList(two, three, four, five),
            toListAndCloseIterator(windowStore.fetch(
                0,
                5,
                ofEpochMilli(defaultStartTime + 2L),
                ofEpochMilli(defaultStartTime + WINDOW_SIZE + 5L)))
        );
        assertEquals(
            Collections.emptyList(),
            toListAndCloseIterator(windowStore.fetch(
                4,
                5,
                ofEpochMilli(defaultStartTime + 2L),
                ofEpochMilli(defaultStartTime + WINDOW_SIZE)))
        );
        assertEquals(
            Collections.emptyList(),
            toListAndCloseIterator(windowStore.fetch(
                0,
                3,
                ofEpochMilli(defaultStartTime + 3L),
                ofEpochMilli(defaultStartTime + WINDOW_SIZE + 5)))
        );
        assertEquals(
            asList(zero, one, two),
            toListAndCloseIterator(windowStore.fetch(
                null,
                2,
                ofEpochMilli(defaultStartTime - WINDOW_SIZE),
                ofEpochMilli(defaultStartTime + WINDOW_SIZE + 2L)))
        );
        assertEquals(
            asList(two, three, four, five),
            toListAndCloseIterator(windowStore.fetch(
                2,
                null,
                ofEpochMilli(defaultStartTime - WINDOW_SIZE),
                ofEpochMilli(defaultStartTime + WINDOW_SIZE + 5L)))
        );
        assertEquals(
            asList(zero, one, two, three, four, five),
            toListAndCloseIterator(windowStore.fetch(
                null,
                null,
                ofEpochMilli(defaultStartTime - WINDOW_SIZE),
                ofEpochMilli(defaultStartTime + WINDOW_SIZE + 5L)))
        );
    }

    @Test
    public void testBackwardFetchRange() {
        
        putFirstBatch(windowStore, defaultStartTime, context);

        assertEquals(
            asList(one, zero),
            toListAndCloseIterator(windowStore.backwardFetch(
                0,
                1,
                ofEpochMilli(defaultStartTime - WINDOW_SIZE),
                ofEpochMilli(defaultStartTime + WINDOW_SIZE)))
        );
        assertEquals(
            Collections.singletonList(one),
            toListAndCloseIterator(windowStore.backwardFetch(
                1,
                1,
                ofEpochMilli(defaultStartTime - WINDOW_SIZE),
                ofEpochMilli(defaultStartTime + WINDOW_SIZE)))
        );
        assertEquals(
            asList(three, two, one),
            toListAndCloseIterator(windowStore.backwardFetch(
                1,
                3,
                ofEpochMilli(defaultStartTime - WINDOW_SIZE),
                ofEpochMilli(defaultStartTime + WINDOW_SIZE)))
        );
        assertEquals(
            asList(three, two, one, zero),
            toListAndCloseIterator(windowStore.backwardFetch(
                0,
                5,
                ofEpochMilli(defaultStartTime - WINDOW_SIZE),
                ofEpochMilli(defaultStartTime + WINDOW_SIZE)))
        );
        assertEquals(
            asList(five, four, three, two, one, zero),
            toListAndCloseIterator(windowStore.backwardFetch(
                0,
                5,
                ofEpochMilli(defaultStartTime - WINDOW_SIZE),
                ofEpochMilli(defaultStartTime + WINDOW_SIZE + 5L)))
        );
        assertEquals(
            asList(five, four, three, two),
            toListAndCloseIterator(windowStore.backwardFetch(
                0,
                5,
                ofEpochMilli(defaultStartTime + 2L),
                ofEpochMilli(defaultStartTime + WINDOW_SIZE + 5L)))
        );
        assertEquals(
            Collections.emptyList(),
            toListAndCloseIterator(windowStore.backwardFetch(
                4,
                5,
                ofEpochMilli(defaultStartTime + 2L),
                ofEpochMilli(defaultStartTime + WINDOW_SIZE)))
        );
        assertEquals(
            Collections.emptyList(),
            toListAndCloseIterator(windowStore.backwardFetch(
                0,
                3,
                ofEpochMilli(defaultStartTime + 3L),
                ofEpochMilli(defaultStartTime + WINDOW_SIZE + 5)))
        );
        assertEquals(
            asList(two, one, zero),
            toListAndCloseIterator(windowStore.backwardFetch(
                null,
                2,
                ofEpochMilli(defaultStartTime - WINDOW_SIZE),
                ofEpochMilli(defaultStartTime + WINDOW_SIZE + 2L)))
        );
        assertEquals(
            asList(five, four, three, two),
            toListAndCloseIterator(windowStore.backwardFetch(
                2,
                null,
                ofEpochMilli(defaultStartTime - WINDOW_SIZE),
                ofEpochMilli(defaultStartTime + WINDOW_SIZE + 5L)))
        );
        assertEquals(
            asList(five, four, three, two, one, zero),
            toListAndCloseIterator(windowStore.backwardFetch(
                null,
                null,
                ofEpochMilli(defaultStartTime - WINDOW_SIZE),
                ofEpochMilli(defaultStartTime + WINDOW_SIZE + 5L)))
        );
    }

    @Test
    public void testPutAndFetchBefore() {
        putFirstBatch(windowStore, defaultStartTime, context);

        assertEquals(
            new HashSet<>(Collections.singletonList("zero")),
            valuesToSetAndCloseIterator(windowStore.fetch(0, ofEpochMilli(defaultStartTime - WINDOW_SIZE), ofEpochMilli(defaultStartTime))));
        assertEquals(
            new HashSet<>(Collections.singletonList("one")),
            valuesToSetAndCloseIterator(windowStore.fetch(1, ofEpochMilli(defaultStartTime + 1L - WINDOW_SIZE), ofEpochMilli(defaultStartTime + 1L))));
        assertEquals(
            new HashSet<>(Collections.singletonList("two")),
            valuesToSetAndCloseIterator(windowStore.fetch(2, ofEpochMilli(defaultStartTime + 2L - WINDOW_SIZE), ofEpochMilli(defaultStartTime + 2L))));
        assertEquals(
            new HashSet<>(Collections.singletonList("three")),
            valuesToSetAndCloseIterator(windowStore.fetch(3, ofEpochMilli(defaultStartTime + 3L - WINDOW_SIZE), ofEpochMilli(defaultStartTime + 3L))));
        assertEquals(
            new HashSet<>(Collections.singletonList("four")),
            valuesToSetAndCloseIterator(windowStore.fetch(4, ofEpochMilli(defaultStartTime + 4L - WINDOW_SIZE), ofEpochMilli(defaultStartTime + 4L))));
        assertEquals(
            new HashSet<>(Collections.singletonList("five")),
            valuesToSetAndCloseIterator(windowStore.fetch(5, ofEpochMilli(defaultStartTime + 5L - WINDOW_SIZE), ofEpochMilli(defaultStartTime + 5L))));

        putSecondBatch(windowStore, defaultStartTime);

        assertEquals(
            new HashSet<>(Collections.emptyList()),
            valuesToSetAndCloseIterator(windowStore.fetch(2, ofEpochMilli(defaultStartTime - 1L - WINDOW_SIZE), ofEpochMilli(defaultStartTime - 1L))));
        assertEquals(
            new HashSet<>(Collections.emptyList()),
            valuesToSetAndCloseIterator(windowStore.fetch(2, ofEpochMilli(defaultStartTime - WINDOW_SIZE), ofEpochMilli(defaultStartTime))));
        assertEquals(
            new HashSet<>(Collections.emptyList()),
            valuesToSetAndCloseIterator(windowStore.fetch(2, ofEpochMilli(defaultStartTime + 1L - WINDOW_SIZE), ofEpochMilli(defaultStartTime + 1L))));
        assertEquals(
            new HashSet<>(Collections.singletonList("two")),
            valuesToSetAndCloseIterator(windowStore.fetch(2, ofEpochMilli(defaultStartTime + 2L - WINDOW_SIZE), ofEpochMilli(defaultStartTime + 2L))));
        assertEquals(
            new HashSet<>(asList("two", "two+1")),
            valuesToSetAndCloseIterator(windowStore.fetch(2, ofEpochMilli(defaultStartTime + 3L - WINDOW_SIZE), ofEpochMilli(defaultStartTime + 3L))));
        assertEquals(
            new HashSet<>(asList("two", "two+1", "two+2")),
            valuesToSetAndCloseIterator(windowStore.fetch(2, ofEpochMilli(defaultStartTime + 4L - WINDOW_SIZE), ofEpochMilli(defaultStartTime + 4L))));
        assertEquals(
            new HashSet<>(asList("two", "two+1", "two+2", "two+3")),
            valuesToSetAndCloseIterator(windowStore.fetch(2, ofEpochMilli(defaultStartTime + 5L - WINDOW_SIZE), ofEpochMilli(defaultStartTime + 5L))));
        assertEquals(
            new HashSet<>(asList("two+1", "two+2", "two+3", "two+4")),
            valuesToSetAndCloseIterator(windowStore.fetch(2, ofEpochMilli(defaultStartTime + 6L - WINDOW_SIZE), ofEpochMilli(defaultStartTime + 6L))));
        assertEquals(
            new HashSet<>(asList("two+2", "two+3", "two+4", "two+5")),
            valuesToSetAndCloseIterator(windowStore.fetch(2, ofEpochMilli(defaultStartTime + 7L - WINDOW_SIZE), ofEpochMilli(defaultStartTime + 7L))));
        assertEquals(
            new HashSet<>(asList("two+3", "two+4", "two+5", "two+6")),
            valuesToSetAndCloseIterator(windowStore.fetch(2, ofEpochMilli(defaultStartTime + 8L - WINDOW_SIZE), ofEpochMilli(defaultStartTime + 8L))));
        assertEquals(
            new HashSet<>(asList("two+4", "two+5", "two+6")),
            valuesToSetAndCloseIterator(windowStore.fetch(2, ofEpochMilli(defaultStartTime + 9L - WINDOW_SIZE), ofEpochMilli(defaultStartTime + 9L))));
        assertEquals(
            new HashSet<>(asList("two+5", "two+6")),
            valuesToSetAndCloseIterator(windowStore.fetch(2, ofEpochMilli(defaultStartTime + 10L - WINDOW_SIZE), ofEpochMilli(defaultStartTime + 10L))));
        assertEquals(
            new HashSet<>(Collections.singletonList("two+6")),
            valuesToSetAndCloseIterator(windowStore.fetch(2, ofEpochMilli(defaultStartTime + 11L - WINDOW_SIZE), ofEpochMilli(defaultStartTime + 11L))));
        assertEquals(
            new HashSet<>(Collections.emptyList()),
            valuesToSetAndCloseIterator(windowStore.fetch(2, ofEpochMilli(defaultStartTime + 12L - WINDOW_SIZE), ofEpochMilli(defaultStartTime + 12L))));
        assertEquals(
            new HashSet<>(Collections.emptyList()),
            valuesToSetAndCloseIterator(windowStore.fetch(2, ofEpochMilli(defaultStartTime + 13L - WINDOW_SIZE), ofEpochMilli(defaultStartTime + 13L))));

        // Flush the store and verify all current entries were properly flushed ...
        windowStore.flush();

        final List<KeyValue<byte[], byte[]>> changeLog = new ArrayList<>();
        for (final ProducerRecord<Object, Object> record : recordCollector.collected()) {
            changeLog.add(new KeyValue<>(((Bytes) record.key()).get(), (byte[]) record.value()));
        }

        final Map<Integer, Set<String>> entriesByKey = entriesByKey(changeLog, defaultStartTime);
        assertEquals(Set.of("zero@0"), entriesByKey.get(0));
        assertEquals(Set.of("one@1"), entriesByKey.get(1));
        assertEquals(Set.of("two@2", "two+1@3", "two+2@4", "two+3@5", "two+4@6", "two+5@7", "two+6@8"), entriesByKey.get(2));
        assertEquals(Set.of("three@2"), entriesByKey.get(3));
        assertEquals(Set.of("four@4"), entriesByKey.get(4));
        assertEquals(Set.of("five@5"), entriesByKey.get(5));
        assertNull(entriesByKey.get(6));
    }

    @Test
    public void testPutAndFetchAfter() {
        putFirstBatch(windowStore, defaultStartTime, context);

        assertEquals(
            new HashSet<>(Collections.singletonList("zero")),
            valuesToSetAndCloseIterator(windowStore.fetch(0, ofEpochMilli(defaultStartTime),
                ofEpochMilli(defaultStartTime + WINDOW_SIZE))));
        assertEquals(
            new HashSet<>(Collections.singletonList("one")),
            valuesToSetAndCloseIterator(windowStore.fetch(1, ofEpochMilli(defaultStartTime + 1L),
                ofEpochMilli(defaultStartTime + 1L + WINDOW_SIZE))));
        assertEquals(
            new HashSet<>(Collections.singletonList("two")),
            valuesToSetAndCloseIterator(windowStore.fetch(2, ofEpochMilli(defaultStartTime + 2L),
                ofEpochMilli(defaultStartTime + 2L + WINDOW_SIZE))));
        assertEquals(
            new HashSet<>(Collections.emptyList()),
            valuesToSetAndCloseIterator(windowStore.fetch(3, ofEpochMilli(defaultStartTime + 3L),
                ofEpochMilli(defaultStartTime + 3L + WINDOW_SIZE))));
        assertEquals(
            new HashSet<>(Collections.singletonList("four")),
            valuesToSetAndCloseIterator(windowStore.fetch(4, ofEpochMilli(defaultStartTime + 4L),
                ofEpochMilli(defaultStartTime + 4L + WINDOW_SIZE))));
        assertEquals(
            new HashSet<>(Collections.singletonList("five")),
            valuesToSetAndCloseIterator(windowStore.fetch(5, ofEpochMilli(defaultStartTime + 5L),
                ofEpochMilli(defaultStartTime + 5L + WINDOW_SIZE))));

        putSecondBatch(windowStore, defaultStartTime);

        assertEquals(
            new HashSet<>(Collections.emptyList()),
            valuesToSetAndCloseIterator(windowStore.fetch(2, ofEpochMilli(defaultStartTime - 2L),
                ofEpochMilli(defaultStartTime - 2L + WINDOW_SIZE))));
        assertEquals(
            new HashSet<>(Collections.singletonList("two")),
            valuesToSetAndCloseIterator(windowStore.fetch(2, ofEpochMilli(defaultStartTime - 1L),
                ofEpochMilli(defaultStartTime - 1L + WINDOW_SIZE))));
        assertEquals(
            new HashSet<>(asList("two", "two+1")),
            valuesToSetAndCloseIterator(windowStore
                .fetch(2, ofEpochMilli(defaultStartTime), ofEpochMilli(defaultStartTime + WINDOW_SIZE))));
        assertEquals(
            new HashSet<>(asList("two", "two+1", "two+2")),
            valuesToSetAndCloseIterator(windowStore.fetch(2, ofEpochMilli(defaultStartTime + 1L),
                ofEpochMilli(defaultStartTime + 1L + WINDOW_SIZE))));
        assertEquals(
            new HashSet<>(asList("two", "two+1", "two+2", "two+3")),
            valuesToSetAndCloseIterator(windowStore.fetch(2, ofEpochMilli(defaultStartTime + 2L),
                ofEpochMilli(defaultStartTime + 2L + WINDOW_SIZE))));
        assertEquals(
            new HashSet<>(asList("two+1", "two+2", "two+3", "two+4")),
            valuesToSetAndCloseIterator(windowStore.fetch(2, ofEpochMilli(defaultStartTime + 3L),
                ofEpochMilli(defaultStartTime + 3L + WINDOW_SIZE))));
        assertEquals(
            new HashSet<>(asList("two+2", "two+3", "two+4", "two+5")),
            valuesToSetAndCloseIterator(windowStore.fetch(2, ofEpochMilli(defaultStartTime + 4L),
                ofEpochMilli(defaultStartTime + 4L + WINDOW_SIZE))));
        assertEquals(
            new HashSet<>(asList("two+3", "two+4", "two+5", "two+6")),
            valuesToSetAndCloseIterator(windowStore.fetch(2, ofEpochMilli(defaultStartTime + 5L),
                ofEpochMilli(defaultStartTime + 5L + WINDOW_SIZE))));
        assertEquals(
            new HashSet<>(asList("two+4", "two+5", "two+6")),
            valuesToSetAndCloseIterator(windowStore.fetch(2, ofEpochMilli(defaultStartTime + 6L),
                ofEpochMilli(defaultStartTime + 6L + WINDOW_SIZE))));
        assertEquals(
            new HashSet<>(asList("two+5", "two+6")),
            valuesToSetAndCloseIterator(windowStore.fetch(2, ofEpochMilli(defaultStartTime + 7L),
                ofEpochMilli(defaultStartTime + 7L + WINDOW_SIZE))));
        assertEquals(
            new HashSet<>(Collections.singletonList("two+6")),
            valuesToSetAndCloseIterator(windowStore.fetch(2, ofEpochMilli(defaultStartTime + 8L),
                ofEpochMilli(defaultStartTime + 8L + WINDOW_SIZE))));
        assertEquals(
            new HashSet<>(Collections.emptyList()),
            valuesToSetAndCloseIterator(windowStore.fetch(2, ofEpochMilli(defaultStartTime + 9L),
                ofEpochMilli(defaultStartTime + 9L + WINDOW_SIZE))));
        assertEquals(
            new HashSet<>(Collections.emptyList()),
            valuesToSetAndCloseIterator(windowStore.fetch(2, ofEpochMilli(defaultStartTime + 10L),
                ofEpochMilli(defaultStartTime + 10L + WINDOW_SIZE))));
        assertEquals(
            new HashSet<>(Collections.emptyList()),
            valuesToSetAndCloseIterator(windowStore.fetch(2, ofEpochMilli(defaultStartTime + 11L),
                ofEpochMilli(defaultStartTime + 11L + WINDOW_SIZE))));
        assertEquals(
            new HashSet<>(Collections.emptyList()),
            valuesToSetAndCloseIterator(windowStore.fetch(2, ofEpochMilli(defaultStartTime + 12L),
                ofEpochMilli(defaultStartTime + 12L + WINDOW_SIZE))));

        // Flush the store and verify all current entries were properly flushed ...
        windowStore.flush();

        final List<KeyValue<byte[], byte[]>> changeLog = new ArrayList<>();
        for (final ProducerRecord<Object, Object> record : recordCollector.collected()) {
            changeLog.add(new KeyValue<>(((Bytes) record.key()).get(), (byte[]) record.value()));
        }

        final Map<Integer, Set<String>> entriesByKey = entriesByKey(changeLog, defaultStartTime);

        assertEquals(Set.of("zero@0"), entriesByKey.get(0));
        assertEquals(Set.of("one@1"), entriesByKey.get(1));
        assertEquals(
            Set.of("two@2", "two+1@3", "two+2@4", "two+3@5", "two+4@6", "two+5@7", "two+6@8"),
            entriesByKey.get(2));
        assertEquals(Set.of("three@2"), entriesByKey.get(3));
        assertEquals(Set.of("four@4"), entriesByKey.get(4));
        assertEquals(Set.of("five@5"), entriesByKey.get(5));
        assertNull(entriesByKey.get(6));
    }

    @Test
    public void testPutSameKeyTimestamp() {
        windowStore.close();
        windowStore = buildWindowStore(RETENTION_PERIOD, WINDOW_SIZE, true, Serdes.Integer(), Serdes.String());
        windowStore.init(context, windowStore);

        windowStore.put(0, "zero", defaultStartTime);

        assertEquals(
            new HashSet<>(Collections.singletonList("zero")),
            valuesToSetAndCloseIterator(windowStore.fetch(0, ofEpochMilli(defaultStartTime - WINDOW_SIZE),
                ofEpochMilli(defaultStartTime + WINDOW_SIZE))));

        windowStore.put(0, "zero", defaultStartTime);
        windowStore.put(0, "zero+", defaultStartTime);
        windowStore.put(0, "zero++", defaultStartTime);

        assertEquals(
            new HashSet<>(asList("zero", "zero", "zero+", "zero++")),
            valuesToSetAndCloseIterator(windowStore.fetch(
                0,
                ofEpochMilli(defaultStartTime - WINDOW_SIZE),
                ofEpochMilli(defaultStartTime + WINDOW_SIZE))));
        assertEquals(
            new HashSet<>(asList("zero", "zero", "zero+", "zero++")),
            valuesToSetAndCloseIterator(windowStore.fetch(
                0,
                ofEpochMilli(defaultStartTime + 1L - WINDOW_SIZE),
                ofEpochMilli(defaultStartTime + 1L + WINDOW_SIZE))));
        assertEquals(
            new HashSet<>(asList("zero", "zero", "zero+", "zero++")),
            valuesToSetAndCloseIterator(windowStore.fetch(
                0,
                ofEpochMilli(defaultStartTime + 2L - WINDOW_SIZE),
                ofEpochMilli(defaultStartTime + 2L + WINDOW_SIZE))));
        assertEquals(
            new HashSet<>(asList("zero", "zero", "zero+", "zero++")),
            valuesToSetAndCloseIterator(windowStore.fetch(
                0,
                ofEpochMilli(defaultStartTime + 3L - WINDOW_SIZE),
                ofEpochMilli(defaultStartTime + 3L + WINDOW_SIZE))));
        assertEquals(
            new HashSet<>(Collections.emptyList()),
            valuesToSetAndCloseIterator(windowStore.fetch(
                0,
                ofEpochMilli(defaultStartTime + 4L - WINDOW_SIZE),
                ofEpochMilli(defaultStartTime + 4L + WINDOW_SIZE))));

        // Flush the store and verify all current entries were properly flushed ...
        windowStore.flush();

        final List<KeyValue<byte[], byte[]>> changeLog = new ArrayList<>();
        for (final ProducerRecord<Object, Object> record : recordCollector.collected()) {
            changeLog.add(new KeyValue<>(((Bytes) record.key()).get(), (byte[]) record.value()));
        }

        final Map<Integer, Set<String>> entriesByKey = entriesByKey(changeLog, defaultStartTime);

        assertEquals(new HashSet<>(asList("zero@0", "zero@0", "zero+@0", "zero++@0")), entriesByKey.get(0));
    }

    @Test
    public void shouldCloseOpenIteratorsWhenStoreIsClosedAndNotThrowInvalidStateStoreExceptionOnHasNext() {
        
        windowStore.put(1, "one", 1L);
        windowStore.put(1, "two", 2L);
        windowStore.put(1, "three", 3L);

        try (final WindowStoreIterator<String> iterator = windowStore.fetch(1, ofEpochMilli(1L), ofEpochMilli(3L))) {
            assertTrue(iterator.hasNext());
            windowStore.close();

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    public void shouldFetchAndIterateOverExactKeys() {
        final long windowSize = 0x7a00000000000000L;
        final long retentionPeriod = 0x7a00000000000000L;
        final WindowStore<String, String> windowStore = buildWindowStore(retentionPeriod,
            windowSize,
            false,
            Serdes.String(),
            Serdes.String());

        windowStore.init(context, windowStore);

        windowStore.put("a", "0001", 0);
        windowStore.put("aa", "0002", 0);
        windowStore.put("a", "0003", 1);
        windowStore.put("aa", "0004", 1);
        windowStore.put("a", "0005", 0x7a00000000000000L - 1);

        final Set<String> expected = new HashSet<>(asList("0001", "0003", "0005"));
        assertThat(
            valuesToSetAndCloseIterator(windowStore.fetch("a", ofEpochMilli(0), ofEpochMilli(Long.MAX_VALUE))),
            equalTo(expected)
        );

        Set<KeyValue<Windowed<String>, String>> set =
            toSet(windowStore.fetch("a", "a", ofEpochMilli(0), ofEpochMilli(Long.MAX_VALUE)));
        assertThat(
            set,
            equalTo(new HashSet<>(asList(
                windowedPair("a", "0001", 0, windowSize),
                windowedPair("a", "0003", 1, windowSize),
                windowedPair("a", "0005", 0x7a00000000000000L - 1, windowSize)
            )))
        );

        set = toSet(windowStore.fetch("aa", "aa", ofEpochMilli(0), ofEpochMilli(Long.MAX_VALUE)));
        assertThat(
            set,
            equalTo(new HashSet<>(asList(
                windowedPair("aa", "0002", 0, windowSize),
                windowedPair("aa", "0004", 1, windowSize)
            )))
        );
        windowStore.close();
    }

    @Test
    public void testDeleteAndUpdate() {
        final long currentTime = 0;
        windowStore.put(1, "one", currentTime);
        windowStore.put(1, "one v2", currentTime);

        try (final WindowStoreIterator<String> iterator = windowStore.fetch(1, 0, currentTime)) {
            assertEquals(new KeyValue<>(currentTime, "one v2"), iterator.next());
        }

        windowStore.put(1, null, currentTime);
        try (final WindowStoreIterator<String> iterator = windowStore.fetch(1, 0, currentTime)) {
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    public void shouldReturnNullOnWindowNotFound() {
        assertNull(windowStore.fetch(1, 0L));
    }

    @Test
    public void shouldThrowNullPointerExceptionOnPutNullKey() {
        assertThrows(NullPointerException.class, () -> windowStore.put(null, "anyValue", 0L));
    }

    @SuppressWarnings("resource")
    @Test
    public void shouldThrowNullPointerExceptionOnGetNullKey() {
        assertThrows(NullPointerException.class, () -> windowStore.fetch(null, ofEpochMilli(1L), ofEpochMilli(2L)));
    }

    @Test
    public void shouldFetchAndIterateOverExactBinaryKeys() {
        final WindowStore<Bytes, String> windowStore = buildWindowStore(RETENTION_PERIOD,
            WINDOW_SIZE,
            true,
            Serdes.Bytes(),
            Serdes.String());
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

        final Set<String> expectedKey1 = new HashSet<>(asList("1", "4", "7"));
        assertThat(
            valuesToSetAndCloseIterator(windowStore.fetch(key1, ofEpochMilli(0), ofEpochMilli(Long.MAX_VALUE))),
            equalTo(expectedKey1)
        );
        final Set<String> expectedKey2 = new HashSet<>(asList("2", "5", "8"));
        assertThat(
            valuesToSetAndCloseIterator(windowStore.fetch(key2, ofEpochMilli(0), ofEpochMilli(Long.MAX_VALUE))),
            equalTo(expectedKey2)
        );
        final Set<String> expectedKey3 = new HashSet<>(asList("3", "6", "9"));
        assertThat(
            valuesToSetAndCloseIterator(windowStore.fetch(key3, ofEpochMilli(0), ofEpochMilli(Long.MAX_VALUE))),
            equalTo(expectedKey3)
        );

        windowStore.close();
    }

    @Test
    public void shouldReturnSameResultsForSingleKeyFetchAndEqualKeyRangeFetch() {
        windowStore.put(1, "one", 0L);
        windowStore.put(2, "two", 1L);
        windowStore.put(2, "two", 2L);
        windowStore.put(3, "three", 3L);

        try (final WindowStoreIterator<String> singleKeyIterator = windowStore.fetch(2, 0L, 5L);
             final KeyValueIterator<Windowed<Integer>, String> keyRangeIterator = windowStore.fetch(2, 2, 0L, 5L)) {

            assertEquals(singleKeyIterator.next().value, keyRangeIterator.next().value);
            assertEquals(singleKeyIterator.next().value, keyRangeIterator.next().value);
            assertFalse(singleKeyIterator.hasNext());
            assertFalse(keyRangeIterator.hasNext());
        }
    }

    @Test
    public void shouldNotThrowInvalidRangeExceptionWithNegativeFromKey() {
        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister();
             final KeyValueIterator<Windowed<Integer>, String> iterator = windowStore.fetch(-1, 1, 0L, 10L)) {
            assertFalse(iterator.hasNext());

            final List<String> messages = appender.getMessages();
            assertThat(
                messages,
                hasItem("Returning empty iterator for fetch with invalid key range: from > to." +
                    " This may be due to range arguments set in the wrong order, " +
                    "or serdes that don't preserve ordering when lexicographically comparing the serialized bytes." +
                    " Note that the built-in numerical serdes do not follow this for negative numbers")
            );
        }
    }

    @Test
    public void shouldMeasureExpiredRecords() {
        final Properties streamsConfig = StreamsTestUtils.getStreamsConfig();
        final WindowStore<Integer, String> windowStore =
            buildWindowStore(RETENTION_PERIOD, WINDOW_SIZE, false, Serdes.Integer(), Serdes.String());
        final InternalMockProcessorContext<?, ?> context = new InternalMockProcessorContext<>(
            TestUtils.tempDirectory(),
            new StreamsConfig(streamsConfig),
            recordCollector
        );
        final Time time = Time.SYSTEM;
        context.setSystemTimeMs(time.milliseconds());
        context.setTime(1L);
        windowStore.init(context, windowStore);

        // Advance stream time by inserting record with large enough timestamp that records with timestamp 0 are expired
        windowStore.put(1, "initial record", 2 * RETENTION_PERIOD);

        // Try inserting a record with timestamp 0 -- should be dropped
        windowStore.put(1, "late record", 0L);
        windowStore.put(1, "another on-time record", RETENTION_PERIOD + 1);

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

        windowStore.close();
    }

    @Test
    public void shouldNotThrowExceptionWhenFetchRangeIsExpired() {
        windowStore.put(1, "one", 0L);
        windowStore.put(1, "two", 4 * RETENTION_PERIOD);

        try (final WindowStoreIterator<String> iterator = windowStore.fetch(1, 0L, 10L)) {

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    public void testWindowIteratorPeek() {
        final long currentTime = 0;
        windowStore.put(1, "one", currentTime);

        try (final KeyValueIterator<Windowed<Integer>, String> iterator = windowStore.fetchAll(0L, currentTime)) {

            assertTrue(iterator.hasNext());
            final Windowed<Integer> nextKey = iterator.peekNextKey();

            assertEquals(iterator.peekNextKey(), nextKey);
            assertEquals(iterator.peekNextKey(), iterator.next().key);
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    public void testValueIteratorPeek() {
        windowStore.put(1, "one", 0L);

        try (final WindowStoreIterator<String> iterator = windowStore.fetch(1, 0L, 10L)) {

            assertTrue(iterator.hasNext());
            final Long nextKey = iterator.peekNextKey();

            assertEquals(iterator.peekNextKey(), nextKey);
            assertEquals(iterator.peekNextKey(), iterator.next().key);
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    public void shouldNotThrowConcurrentModificationException() {
        long currentTime = 0;
        windowStore.put(1, "one", currentTime);

        currentTime += WINDOW_SIZE * 10;
        windowStore.put(1, "two", currentTime);

        try (final KeyValueIterator<Windowed<Integer>, String> iterator = windowStore.all()) {

            currentTime += WINDOW_SIZE * 10;
            windowStore.put(1, "three", currentTime);

            currentTime += WINDOW_SIZE * 10;
            windowStore.put(2, "four", currentTime);

            // Iterator should return all records in store and not throw exception b/c some were added after fetch
            assertEquals(windowedPair(1, "one", 0), iterator.next());
            assertEquals(windowedPair(1, "two", WINDOW_SIZE * 10), iterator.next());
            assertEquals(windowedPair(1, "three", WINDOW_SIZE * 20), iterator.next());
            assertEquals(windowedPair(2, "four", WINDOW_SIZE * 30), iterator.next());
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    public void testFetchDuplicates() {
        windowStore.close();
        windowStore = buildWindowStore(RETENTION_PERIOD, WINDOW_SIZE, true, Serdes.Integer(), Serdes.String());
        windowStore.init(context, windowStore);

        long currentTime = 0;
        windowStore.put(1, "one", currentTime);
        windowStore.put(1, "one-2", currentTime);

        currentTime += WINDOW_SIZE * 10;
        windowStore.put(1, "two", currentTime);
        windowStore.put(1, "two-2", currentTime);

        currentTime += WINDOW_SIZE * 10;
        windowStore.put(1, "three", currentTime);
        windowStore.put(1, "three-2", currentTime);

        try (final WindowStoreIterator<String> iterator = windowStore.fetch(1, 0, WINDOW_SIZE * 10)) {

            assertEquals(new KeyValue<>(0L, "one"), iterator.next());
            assertEquals(new KeyValue<>(0L, "one-2"), iterator.next());
            assertEquals(new KeyValue<>(WINDOW_SIZE * 10, "two"), iterator.next());
            assertEquals(new KeyValue<>(WINDOW_SIZE * 10, "two-2"), iterator.next());
            assertFalse(iterator.hasNext());
        }
    }


    private void putFirstBatch(final WindowStore<Integer, String> store,
                               @SuppressWarnings("SameParameterValue") final long startTime,
                               final InternalMockProcessorContext<?, ?> context) {
        context.setRecordContext(createRecordContext(startTime));
        store.put(0, "zero", startTime);
        store.put(1, "one", startTime + 1L);
        store.put(2, "two", startTime + 2L);
        store.put(3, "three", startTime + 2L);
        store.put(4, "four", startTime + 4L);
        store.put(5, "five", startTime + 5L);
    }

    private void putSecondBatch(final WindowStore<Integer, String> store,
                                @SuppressWarnings("SameParameterValue") final long startTime) {
        store.put(2, "two+1", startTime + 3L);
        store.put(2, "two+2", startTime + 4L);
        store.put(2, "two+3", startTime + 5L);
        store.put(2, "two+4", startTime + 6L);
        store.put(2, "two+5", startTime + 7L);
        store.put(2, "two+6", startTime + 8L);
    }

    long extractStoreTimestamp(final byte[] binaryKey) {
        return WindowKeySchema.extractStoreTimestamp(binaryKey);
    }

    <K> K extractStoreKey(final byte[] binaryKey,
                          final StateSerdes<K, ?> serdes) {
        return WindowKeySchema.extractStoreKey(binaryKey, serdes);
    }

    private Map<Integer, Set<String>> entriesByKey(final List<KeyValue<byte[], byte[]>> changeLog,
                                                   @SuppressWarnings("SameParameterValue") final long startTime) {
        final HashMap<Integer, Set<String>> entriesByKey = new HashMap<>();

        for (final KeyValue<byte[], byte[]> entry : changeLog) {
            final long timestamp = extractStoreTimestamp(entry.key);

            final Integer key = extractStoreKey(entry.key, serdes);
            final String value = entry.value == null ? null : serdes.valueFrom(entry.value);

            final Set<String> entries = entriesByKey.computeIfAbsent(key, k -> new HashSet<>());
            entries.add(value + "@" + (timestamp - startTime));
        }

        return entriesByKey;
    }

    protected static <K, V> KeyValue<Windowed<K>, V> windowedPair(final K key, final V value, final long timestamp) {
        return windowedPair(key, value, timestamp, WINDOW_SIZE);
    }

    private static <K, V> KeyValue<Windowed<K>, V> windowedPair(final K key, final V value, final long timestamp, final long windowSize) {
        return KeyValue.pair(new Windowed<>(key, WindowKeySchema.timeWindowForSize(timestamp, windowSize)), value);
    }

    private ProcessorRecordContext createRecordContext(final long time) {
        return new ProcessorRecordContext(time, 0, 0, "topic", new RecordHeaders());
    }
}
