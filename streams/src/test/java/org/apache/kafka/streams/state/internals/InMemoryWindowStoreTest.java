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

import static java.time.Duration.ofMillis;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.state.internals.WindowKeySchema.toStoreKeyBinary;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.DefaultProductionExceptionHandler;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.processor.internals.RecordCollectorImpl;
import org.apache.kafka.streams.processor.internals.testutil.LogCaptureAppender;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class InMemoryWindowStoreTest {

    private static final long DEFAULT_CACHE_SIZE_BYTES = 1024 * 1024L;

    private final String storeName = "InMemoryWindowStore";
    private final long retentionPeriod = 40L * 1000L;
    private final long windowSize = 10L;

    private final StateSerdes<Integer, String> serdes = new StateSerdes<>("", Serdes.Integer(), Serdes.String());

    private final List<KeyValue<byte[], byte[]>> changeLog = new ArrayList<>();
    private final ThreadCache cache = new ThreadCache(new LogContext("TestCache "),
                                                      DEFAULT_CACHE_SIZE_BYTES,
                                                      new MockStreamsMetrics(new Metrics()));

    private final Producer<byte[], byte[]> producer =
        new MockProducer<>(true, Serdes.ByteArray().serializer(), Serdes.ByteArray().serializer());
    private final RecordCollector recordCollector = new RecordCollectorImpl("InMemoryWindowStoreTestTask",
                                                                            new LogContext("InMemoryWindowStoreTestTask "),
                                                                            new DefaultProductionExceptionHandler(),
                                                                            new Metrics().sensor("skipped-records")) {
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
    private final InternalMockProcessorContext context = new InternalMockProcessorContext(baseDir, Serdes.ByteArray(), Serdes.ByteArray(), recordCollector, cache);
    private WindowStore<Integer, String> windowStore;

    private WindowStore<Integer, String> createInMemoryWindowStore(final ProcessorContext context, final boolean retainDuplicates) {
        final WindowStore<Integer, String> store = Stores.windowStoreBuilder(Stores.inMemoryWindowStore(
                                                                             storeName,
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

    private void setCurrentTime(final long currentTime) {
        context.setRecordContext(createRecordContext(currentTime));
    }

    private ProcessorRecordContext createRecordContext(final long time) {
        return new ProcessorRecordContext(time, 0, 0, "topic", null);
    }

    private <K, V> KeyValue<Windowed<K>, V> windowedPair(final K key, final V value, final long timestamp) {
        return windowedPair(key, value, timestamp, windowSize);
    }

    private static <K, V> KeyValue<Windowed<K>, V> windowedPair(final K key, final V value, final long timestamp, final long windowSize) {
        return KeyValue.pair(new Windowed<>(key, WindowKeySchema.timeWindowForSize(timestamp, windowSize)), value);
    }

    @Test
    public void testSingleFetch() {
        windowStore = createInMemoryWindowStore(context, false);

        long currentTime = 0;
        setCurrentTime(currentTime);
        windowStore.put(1, "one");

        currentTime += windowSize;
        setCurrentTime(currentTime);
        windowStore.put(1, "two");

        currentTime += 3 * windowSize;
        setCurrentTime(currentTime);
        windowStore.put(1, "three");

        assertEquals("one", windowStore.fetch(1, 0));
        assertEquals("two", windowStore.fetch(1, windowSize));
        assertEquals("three", windowStore.fetch(1, 4 * windowSize));
    }

    @Test
    public void testDeleteAndUpdate() {
        windowStore = createInMemoryWindowStore(context, false);

        final long currentTime = 0;
        setCurrentTime(currentTime);
        windowStore.put(1, "one");
        windowStore.put(1, "one v2");

        WindowStoreIterator<String> iterator = windowStore.fetch(1, 0, currentTime);
        assertEquals(new KeyValue<>(currentTime, "one v2"), iterator.next());

        windowStore.put(1, null);
        iterator = windowStore.fetch(1, 0, currentTime);
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testFetchAll() {
        windowStore = createInMemoryWindowStore(context, false);

        long currentTime = 0;
        setCurrentTime(currentTime);
        windowStore.put(1, "one");

        currentTime += windowSize * 10;
        setCurrentTime(currentTime);
        windowStore.put(1, "two");

        currentTime += windowSize * 10;
        setCurrentTime(currentTime);
        windowStore.put(1, "three");

        currentTime += windowSize * 10;
        setCurrentTime(currentTime);
        windowStore.put(2, "four");

        currentTime += windowSize * 10;
        setCurrentTime(currentTime);
        windowStore.put(2, "five");

        final KeyValueIterator<Windowed<Integer>, String> iterator = windowStore.fetchAll(windowSize * 10, windowSize * 30);

        assertEquals(windowedPair(1, "two", windowSize * 10), iterator.next());
        assertEquals(windowedPair(1, "three", windowSize * 20), iterator.next());
        assertEquals(windowedPair(2, "four", windowSize * 30), iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testAll() {
        windowStore = createInMemoryWindowStore(context, false);

        long currentTime = 0;
        setCurrentTime(currentTime);
        windowStore.put(1, "one");

        currentTime += windowSize * 10;
        setCurrentTime(currentTime);
        windowStore.put(1, "two");

        currentTime += windowSize * 10;
        setCurrentTime(currentTime);
        windowStore.put(1, "three");

        currentTime += windowSize * 10;
        setCurrentTime(currentTime);
        windowStore.put(2, "four");

        final KeyValueIterator<Windowed<Integer>, String> iterator = windowStore.all();

        assertEquals(windowedPair(1, "one", 0), iterator.next());
        assertEquals(windowedPair(1, "two", windowSize * 10), iterator.next());
        assertEquals(windowedPair(1, "three", windowSize * 20), iterator.next());
        assertEquals(windowedPair(2, "four", windowSize * 30), iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testTimeRangeFetch() {

        windowStore = createInMemoryWindowStore(context, false);

        long currentTime = 0;
        setCurrentTime(currentTime);
        windowStore.put(1, "one");

        currentTime += windowSize * 10;
        setCurrentTime(currentTime);
        windowStore.put(1, "two");

        currentTime += windowSize * 10;
        setCurrentTime(currentTime);
        windowStore.put(1, "three");

        currentTime += windowSize * 10;
        setCurrentTime(currentTime);
        windowStore.put(1, "four");

        currentTime += windowSize * 10;
        setCurrentTime(currentTime);
        windowStore.put(1, "five");

        final WindowStoreIterator<String> iterator = windowStore.fetch(1, windowSize * 10, 3 * windowSize * 10);

        // should return only the middle three records
        assertEquals(new KeyValue<>(windowSize * 10, "two"), iterator.next());
        assertEquals(new KeyValue<>(2 * windowSize * 10, "three"), iterator.next());
        assertEquals(new KeyValue<>(3 * windowSize * 10, "four"), iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testKeyRangeFetch() {

        windowStore = createInMemoryWindowStore(context, false);

        long currentTime = 0;
        setCurrentTime(currentTime);
        windowStore.put(1, "one");

        currentTime += windowSize * 10;
        setCurrentTime(currentTime);
        windowStore.put(2, "two");

        currentTime += windowSize * 10;
        setCurrentTime(currentTime);
        windowStore.put(3, "three");

        currentTime += windowSize * 10;
        setCurrentTime(currentTime);
        windowStore.put(4, "four");

        windowStore.put(5, "five");

        final KeyValueIterator<Windowed<Integer>, String> iterator = windowStore.fetch(1, 4, 0L, currentTime);

        // should return only the first four keys
        assertEquals(windowedPair(1, "one", 0), iterator.next());
        assertEquals(windowedPair(2, "two", windowSize * 10), iterator.next());
        assertEquals(windowedPair(3, "three", windowSize * 20), iterator.next());
        assertEquals(windowedPair(4, "four", windowSize * 30), iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testFetchDuplicates() {
        windowStore = createInMemoryWindowStore(context, true);

        long currentTime = 0;
        setCurrentTime(currentTime);
        windowStore.put(1, "one");
        windowStore.put(1, "one-2");

        currentTime += windowSize * 10;
        setCurrentTime(currentTime);
        windowStore.put(1, "two");
        windowStore.put(1, "two-2");

        currentTime += windowSize * 10;
        setCurrentTime(currentTime);
        windowStore.put(1, "three");
        windowStore.put(1, "three-2");

        final WindowStoreIterator<String> iterator = windowStore.fetch(1, 0, windowSize * 10);

        assertEquals(new KeyValue<>(0L, "one"), iterator.next());
        assertEquals(new KeyValue<>(0L, "one-2"), iterator.next());
        assertEquals(new KeyValue<>(windowSize * 10, "two"), iterator.next());
        assertEquals(new KeyValue<>(windowSize * 10, "two-2"), iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testSegmentExpiration() {
        windowStore = createInMemoryWindowStore(context, false);

        long currentTime = 0;
        setCurrentTime(currentTime);
        windowStore.put(1, "one");

        currentTime += retentionPeriod / 4;
        setCurrentTime(currentTime);
        windowStore.put(1, "two");

        currentTime += retentionPeriod / 4;
        setCurrentTime(currentTime);
        windowStore.put(1, "three");

        currentTime += retentionPeriod / 4;
        setCurrentTime(currentTime);
        windowStore.put(1, "four");

        // increase current time to the full retentionPeriod to expire first record
        currentTime = currentTime + retentionPeriod / 4;
        setCurrentTime(currentTime);
        windowStore.put(1, "five");

        KeyValueIterator<Windowed<Integer>, String> iterator = windowStore.fetchAll(0L, currentTime);

        // effect of this put (expires next oldest record, adds new one) should not be reflected in the already fetched results
        currentTime = currentTime + retentionPeriod / 4;
        setCurrentTime(currentTime);
        windowStore.put(1, "six");

        // should only have middle 4 values, as (only) the first record was expired at the time of the fetch
        // and the last was inserted after the fetch
        assertEquals(windowedPair(1, "two", retentionPeriod / 4), iterator.next());
        assertEquals(windowedPair(1, "three", retentionPeriod / 2), iterator.next());
        assertEquals(windowedPair(1, "four", 3 * (retentionPeriod / 4)), iterator.next());
        assertEquals(windowedPair(1, "five", retentionPeriod), iterator.next());
        assertFalse(iterator.hasNext());

        iterator = windowStore.fetchAll(0L, currentTime);

        // If we fetch again after the last put, the second oldest record should have expired and newest should appear in results
        assertEquals(windowedPair(1, "three", retentionPeriod / 2), iterator.next());
        assertEquals(windowedPair(1, "four", 3 * (retentionPeriod / 4)), iterator.next());
        assertEquals(windowedPair(1, "five", retentionPeriod), iterator.next());
        assertEquals(windowedPair(1, "six", 5 * (retentionPeriod / 4)), iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testWindowIteratorPeek() {
        windowStore = createInMemoryWindowStore(context, false);

        final long currentTime = 0;
        setCurrentTime(currentTime);
        windowStore.put(1, "one");

        final KeyValueIterator<Windowed<Integer>, String> iterator = windowStore.fetchAll(0L, currentTime);

        assertEquals(iterator.peekNextKey(), iterator.next().key);
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testValueIteratorPeek() {
        windowStore = createInMemoryWindowStore(context, false);

        final long currentTime = 0;
        setCurrentTime(currentTime);
        windowStore.put(1, "one");

        final WindowStoreIterator<String> iterator = windowStore.fetch(1, 0L, currentTime);

        assertEquals(iterator.peekNextKey(), iterator.next().key);
        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldRestore() {
        windowStore = createInMemoryWindowStore(context, false);

        // should be empty initially
        assertFalse(windowStore.all().hasNext());

        final List<KeyValue<byte[], byte[]>> restorableEntries = new LinkedList<>();

        restorableEntries.add(new KeyValue<>(toStoreKeyBinary(1, 0L, 0, serdes).get(), serdes.rawValue("one")));
        restorableEntries.add(new KeyValue<>(toStoreKeyBinary(2, windowSize, 0, serdes).get(), serdes.rawValue("two")));
        restorableEntries.add(new KeyValue<>(toStoreKeyBinary(3, 2 * windowSize, 0, serdes).get(), serdes.rawValue("three")));

        context.restore(storeName, restorableEntries);
        final KeyValueIterator<Windowed<Integer>, String> iterator = windowStore.fetchAll(0L, 2 * windowSize);

        assertEquals(windowedPair(1, "one", 0L), iterator.next());
        assertEquals(windowedPair(2, "two", windowSize), iterator.next());
        assertEquals(windowedPair(3, "three", 2 * windowSize), iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldLogAndMeasureExpiredRecords() {
        LogCaptureAppender.setClassLoggerToDebug(InMemoryWindowStore.class);
        final LogCaptureAppender appender = LogCaptureAppender.createAndRegister();

        windowStore = createInMemoryWindowStore(context, false);
        setCurrentTime(retentionPeriod);

        // Advance stream time by inserting record with large enough timestamp that records with timestamp 0 are expired
        windowStore.put(1, "initial record");

        // Try inserting a record with timestamp 0 -- should be dropped
        windowStore.put(1, "late record", 0L);
        windowStore.put(1, "another on-time record");

        LogCaptureAppender.unregister(appender);

        final Map<MetricName, ? extends Metric> metrics = context.metrics().metrics();

        final Metric dropTotal = metrics.get(new MetricName(
            "expired-window-record-drop-total",
            "stream-in-memory-window-state-metrics",
            "The total number of occurrence of expired-window-record-drop operations.",
            mkMap(
                mkEntry("client-id", "mock"),
                mkEntry("task-id", "0_0"),
                mkEntry("in-memory-window-state-id", storeName)
            )
        ));

        final Metric dropRate = metrics.get(new MetricName(
            "expired-window-record-drop-rate",
            "stream-in-memory-window-state-metrics",
            "The average number of occurrence of expired-window-record-drop operation per second.",
            mkMap(
                mkEntry("client-id", "mock"),
                mkEntry("task-id", "0_0"),
                mkEntry("in-memory-window-state-id", storeName)
            )
        ));

        assertEquals(1.0, dropTotal.metricValue());
        assertNotEquals(0.0, dropRate.metricValue());
        final List<String> messages = appender.getMessages();
        assertThat(messages, hasItem("Skipping record for expired segment."));
    }

    @Test
    public void testIteratorMultiplePeekAndHasNext() {
        windowStore = createInMemoryWindowStore(context, false);

        long currentTime = 0;
        setCurrentTime(currentTime);
        windowStore.put(1, "one");

        currentTime += windowSize * 10;
        setCurrentTime(currentTime);
        windowStore.put(2, "two");

        currentTime += windowSize * 10;
        setCurrentTime(currentTime);
        windowStore.put(3, "three");

        final KeyValueIterator<Windowed<Integer>, String> iterator = windowStore.fetch(1, 4, 0L, currentTime);

        assertFalse(!iterator.hasNext());
        assertFalse(!iterator.hasNext());
        assertEquals(new Windowed<>(1, WindowKeySchema.timeWindowForSize(0L, windowSize)), iterator.peekNextKey());
        assertEquals(new Windowed<>(1, WindowKeySchema.timeWindowForSize(0L, windowSize)), iterator.peekNextKey());

        assertEquals(windowedPair(1, "one", 0), iterator.next());
        assertEquals(windowedPair(2, "two", windowSize * 10), iterator.next());
        assertEquals(windowedPair(3, "three", windowSize * 20), iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldNotThrowConcurrentModificationException() {
        windowStore = createInMemoryWindowStore(context, false);

        long currentTime = 0;
        setCurrentTime(currentTime);
        windowStore.put(1, "one");

        currentTime += windowSize * 10;
        setCurrentTime(currentTime);
        windowStore.put(1, "two");

        final KeyValueIterator<Windowed<Integer>, String> iterator = windowStore.all();

        currentTime += windowSize * 10;
        setCurrentTime(currentTime);
        windowStore.put(1, "three");

        currentTime += windowSize * 10;
        setCurrentTime(currentTime);
        windowStore.put(2, "four");

        // Iterator should return all records in store and not throw exception b/c some were added after fetch
        assertEquals(windowedPair(1, "one", 0), iterator.next());
        assertEquals(windowedPair(1, "two", windowSize * 10), iterator.next());
        assertEquals(windowedPair(1, "three", windowSize * 20), iterator.next());
        assertEquals(windowedPair(2, "four", windowSize * 30), iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldNotExpireFromOpenIterator() {
        windowStore = createInMemoryWindowStore(context, false);

        windowStore.put(1, "one", 0L);
        windowStore.put(1, "two", 10L);

        windowStore.put(2, "one", 5L);
        windowStore.put(2, "two", 15L);

        final WindowStoreIterator<String> iterator1 = windowStore.fetch(1, 0L, 50L);
        final WindowStoreIterator<String> iterator2 = windowStore.fetch(2, 0L, 50L);

        // This put expires all four previous records, but they should still be returned from already open iterators
        windowStore.put(1, "four", retentionPeriod + 50L);

        assertEquals(new KeyValue<>(0L, "one"), iterator1.next());
        assertEquals(new KeyValue<>(5L, "one"), iterator2.next());

        assertEquals(new KeyValue<>(15L, "two"), iterator2.next());
        assertEquals(new KeyValue<>(10L, "two"), iterator1.next());

        assertFalse(iterator1.hasNext());
        assertFalse(iterator2.hasNext());
    }

    @Test
    public void shouldNotThrowExceptionWhenFetchRangeIsExpired() {
        windowStore = createInMemoryWindowStore(context, false);

        windowStore.put(1, "one", 0L);
        windowStore.put(1, "two", retentionPeriod);

        final WindowStoreIterator<String> iterator = windowStore.fetch(1, 0L, 10L);

        assertFalse(iterator.hasNext());
    }
}
