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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.state.internals.PrefixedWindowKeySchemas.KeyFirstWindowKeySchema;
import org.apache.kafka.streams.state.internals.PrefixedWindowKeySchemas.TimeFirstWindowKeySchema;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static java.time.Duration.ofHours;
import static java.time.Duration.ofMinutes;
import static java.time.Instant.ofEpochMilli;
import static java.util.Arrays.asList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.state.internals.ThreadCacheTest.memoryCacheEntrySize;
import static org.apache.kafka.test.StreamsTestUtils.toListAndCloseIterator;
import static org.apache.kafka.test.StreamsTestUtils.verifyAllWindowedKeyValues;
import static org.apache.kafka.test.StreamsTestUtils.verifyKeyValueList;
import static org.apache.kafka.test.StreamsTestUtils.verifyWindowedKeyValue;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TimeOrderedCachingPersistentWindowStoreTest {

    private static final int MAX_CACHE_SIZE_BYTES = 300;
    private static final long DEFAULT_TIMESTAMP = 10L;
    private static final Long WINDOW_SIZE = 10L;
    private static final long SEGMENT_INTERVAL = 100L;
    private static final String TOPIC = "topic";
    private static final String CACHE_NAMESPACE = "0_0-store-name";

    private ThreadCache cache;
    private InternalMockProcessorContext<?, ?> context;
    private TimeFirstWindowKeySchema baseKeySchema;
    private WindowStore<Bytes, byte[]> underlyingStore;
    private TimeOrderedCachingWindowStore cachingStore;
    private RocksDBTimeOrderedWindowSegmentedBytesStore bytesStore;
    private CacheFlushListenerStub<Windowed<String>, String> cacheListener;

    private void setUp(final boolean hasIndex) {
        baseKeySchema = new TimeFirstWindowKeySchema();
        bytesStore = new RocksDBTimeOrderedWindowSegmentedBytesStore("test", "metrics-scope", 100, SEGMENT_INTERVAL, hasIndex);
        underlyingStore = new RocksDBTimeOrderedWindowStore(bytesStore, false, WINDOW_SIZE);
        final TimeWindowedDeserializer<String> keyDeserializer = new TimeWindowedDeserializer<>(new StringDeserializer(), WINDOW_SIZE);
        keyDeserializer.setIsChangelogTopic(true);
        cacheListener = new CacheFlushListenerStub<>(keyDeserializer, new StringDeserializer());
        cachingStore = new TimeOrderedCachingWindowStore(underlyingStore, WINDOW_SIZE, SEGMENT_INTERVAL);
        cachingStore.setFlushListener(cacheListener, false);
        cache = new ThreadCache(new LogContext("testCache "), MAX_CACHE_SIZE_BYTES, new MockStreamsMetrics(new Metrics()));
        context = new InternalMockProcessorContext<>(TestUtils.tempDirectory(), null, null, null, cache);
        context.setRecordContext(new ProcessorRecordContext(DEFAULT_TIMESTAMP, 0, 0, TOPIC, new RecordHeaders()));
        cachingStore.init(context, cachingStore);
    }

    @AfterEach
    public void closeStore() {
        cachingStore.close();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldDelegateInit(final boolean hasIndex) {
        setUp(hasIndex);
        final RocksDBTimeOrderedWindowStore inner = mock(RocksDBTimeOrderedWindowStore.class);
        when(inner.hasIndex()).thenReturn(hasIndex);

        final TimeOrderedCachingWindowStore outer = new TimeOrderedCachingWindowStore(inner, WINDOW_SIZE, SEGMENT_INTERVAL);
        outer.init(context, outer);
        verify(inner, times(1)).init(context, outer);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldThrowIfWrongStore(final boolean hasIndex) {
        setUp(hasIndex);
        final RocksDBTimestampedWindowStore innerWrong = mock(RocksDBTimestampedWindowStore.class);
        final Exception e = assertThrows(IllegalArgumentException.class,
            () -> new TimeOrderedCachingWindowStore(innerWrong, WINDOW_SIZE, SEGMENT_INTERVAL));
        assertThat(e.getMessage(),
            containsString("TimeOrderedCachingWindowStore only supports RocksDBTimeOrderedWindowStore backed store"));

        final RocksDBTimeOrderedWindowStore inner = mock(RocksDBTimeOrderedWindowStore.class);
        // Nothing happens
        new TimeOrderedCachingWindowStore(inner, WINDOW_SIZE, SEGMENT_INTERVAL);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldNotReturnDuplicatesInRanges(final boolean hasIndex) {
        setUp(hasIndex);
        final StreamsBuilder builder = new StreamsBuilder();

        final StoreBuilder<TimestampedWindowStore<String, String>> storeBuilder = Stores.timestampedWindowStoreBuilder(
            RocksDbIndexedTimeOrderedWindowBytesStoreSupplier.create(
                "store-name",
                ofHours(1L),
                ofMinutes(1),
                false,
                hasIndex
            ), Serdes.String(), Serdes.String())
            .withCachingEnabled();

        builder.addStateStore(storeBuilder);

        builder.stream(TOPIC,
            Consumed.with(Serdes.String(), Serdes.String()))
            .process(() -> new Processor<String, String, String, String>() {
                private int numRecordsProcessed;
                private WindowStore<String, ValueAndTimestamp<String>> store;

                @Override
                public void init(final ProcessorContext<String, String> processorContext) {
                    this.store = processorContext.getStateStore("store-name");
                    int count = 0;

                    try (final KeyValueIterator<Windowed<String>, ValueAndTimestamp<String>> all = store.all()) {
                        while (all.hasNext()) {
                            count++;
                            all.next();
                        }
                    }

                    assertThat(count, equalTo(0));
                }

                @Override
                public void process(final Record<String, String> record) {
                    int count = 0;

                    try (final KeyValueIterator<Windowed<String>, ValueAndTimestamp<String>> all = store.all()) {
                        while (all.hasNext()) {
                            count++;
                            all.next();
                        }
                    }

                    assertThat(count, equalTo(numRecordsProcessed));

                    store.put(record.value(), ValueAndTimestamp.make(record.value(), record.timestamp()), record.timestamp());

                    numRecordsProcessed++;
                }

            }, "store-name");

        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000L);

        final Instant initialWallClockTime = Instant.ofEpochMilli(0L);
        final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), streamsConfiguration, initialWallClockTime);

        final TestInputTopic<String, String> inputTopic = driver.createInputTopic(TOPIC,
            new StringSerializer(),
            new StringSerializer(),
            initialWallClockTime,
            Duration.ZERO);

        for (int i = 0; i < 5; i++) {
            inputTopic.pipeInput(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        }
        driver.advanceWallClockTime(Duration.ofSeconds(10));
        inputTopic.advanceTime(Duration.ofSeconds(10));
        for (int i = 0; i < 5; i++) {
            inputTopic.pipeInput(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        }
        driver.advanceWallClockTime(Duration.ofSeconds(10));
        inputTopic.advanceTime(Duration.ofSeconds(10));
        for (int i = 0; i < 5; i++) {
            inputTopic.pipeInput(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        }
        driver.advanceWallClockTime(Duration.ofSeconds(10));
        inputTopic.advanceTime(Duration.ofSeconds(10));
        for (int i = 0; i < 5; i++) {
            inputTopic.pipeInput(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        }

        driver.close();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldPutFetchFromCache(final boolean hasIndex) {
        setUp(hasIndex);
        cachingStore.put(bytesKey("a"), bytesValue("a"), DEFAULT_TIMESTAMP);
        cachingStore.put(bytesKey("b"), bytesValue("b"), DEFAULT_TIMESTAMP);

        assertThat(cachingStore.fetch(bytesKey("a"), 10), equalTo(bytesValue("a")));
        assertThat(cachingStore.fetch(bytesKey("b"), 10), equalTo(bytesValue("b")));
        assertThat(cachingStore.fetch(bytesKey("c"), 10), equalTo(null));
        assertThat(cachingStore.fetch(bytesKey("a"), 0), equalTo(null));

        try (final WindowStoreIterator<byte[]> a = cachingStore.fetch(bytesKey("a"), ofEpochMilli(10), ofEpochMilli(10));
             final WindowStoreIterator<byte[]> b = cachingStore.fetch(bytesKey("b"), ofEpochMilli(10), ofEpochMilli(10))) {
            verifyKeyValue(a.next(), DEFAULT_TIMESTAMP, "a");
            verifyKeyValue(b.next(), DEFAULT_TIMESTAMP, "b");
            assertFalse(a.hasNext());
            assertFalse(b.hasNext());
            final int expectedSize = hasIndex ? 4 : 2;
            assertEquals(expectedSize, cache.size());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldMatchPositionAfterPutWithFlushListener(final boolean hasIndex) {
        setUp(hasIndex);
        cachingStore.setFlushListener(record -> { }, false);
        shouldMatchPositionAfterPut();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldMatchPositionAfterPutWithoutFlushListener(final boolean hasIndex) {
        setUp(hasIndex);
        cachingStore.setFlushListener(null, false);
        shouldMatchPositionAfterPut();
    }

    private void shouldMatchPositionAfterPut() {
        context.setRecordContext(new ProcessorRecordContext(0, 1, 0, "", new RecordHeaders()));
        cachingStore.put(bytesKey("key1"), bytesValue("value1"), DEFAULT_TIMESTAMP);
        context.setRecordContext(new ProcessorRecordContext(0, 2, 0, "", new RecordHeaders()));
        cachingStore.put(bytesKey("key2"), bytesValue("value2"), DEFAULT_TIMESTAMP);

        // Position should correspond to the last record's context, not the current context.
        context.setRecordContext(
            new ProcessorRecordContext(0, 3, 0, "", new RecordHeaders())
        );

        // the caching window store doesn't maintain a separate
        // position because it never serves queries from the cache
        assertEquals(Position.emptyPosition(), cachingStore.getPosition());
        assertEquals(Position.emptyPosition(), underlyingStore.getPosition());

        cachingStore.flush();

        assertEquals(
            Position.fromMap(mkMap(mkEntry("", mkMap(mkEntry(0, 2L))))),
            cachingStore.getPosition()
        );
        assertEquals(
            Position.fromMap(mkMap(mkEntry("", mkMap(mkEntry(0, 2L))))),
            underlyingStore.getPosition()
        );
    }

    private void verifyKeyValue(final KeyValue<Long, byte[]> next,
                                final long expectedKey,
                                final String expectedValue) {
        assertThat(next.key, equalTo(expectedKey));
        assertThat(next.value, equalTo(bytesValue(expectedValue)));
    }

    private static byte[] bytesValue(final String value) {
        return value.getBytes();
    }

    private static Bytes bytesKey(final String key) {
        return Bytes.wrap(key.getBytes());
    }

    @SuppressWarnings("resource")
    private String stringFrom(final byte[] from) {
        return new StringDeserializer().deserialize("", from);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldPutFetchRangeFromCache(final boolean hasIndex) {
        setUp(hasIndex);
        cachingStore.put(bytesKey("a"), bytesValue("a"), DEFAULT_TIMESTAMP);
        cachingStore.put(bytesKey("b"), bytesValue("b"), DEFAULT_TIMESTAMP);

        try (final KeyValueIterator<Windowed<Bytes>, byte[]> iterator =
                 cachingStore.fetch(bytesKey("a"), bytesKey("b"), ofEpochMilli(DEFAULT_TIMESTAMP), ofEpochMilli(DEFAULT_TIMESTAMP))) {
            final List<Windowed<Bytes>> expectedKeys = Arrays.asList(
                new Windowed<>(bytesKey("a"), new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE)),
                new Windowed<>(bytesKey("b"), new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE))
            );

            final List<String> expectedValues = Arrays.asList("a", "b");

            verifyAllWindowedKeyValues(iterator, expectedKeys, expectedValues);
            final int expectedSize = hasIndex ? 4 : 2;
            assertEquals(expectedSize, cache.size());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldPutFetchRangeFromCacheForNullKeyFrom(final boolean hasIndex) {
        setUp(hasIndex);
        cachingStore.put(bytesKey("a"), bytesValue("a"), DEFAULT_TIMESTAMP);
        cachingStore.put(bytesKey("b"), bytesValue("b"), DEFAULT_TIMESTAMP);
        cachingStore.put(bytesKey("c"), bytesValue("c"), DEFAULT_TIMESTAMP + 10L);
        cachingStore.put(bytesKey("d"), bytesValue("d"), DEFAULT_TIMESTAMP + 20L);
        cachingStore.put(bytesKey("e"), bytesValue("e"), DEFAULT_TIMESTAMP + 20L);

        try (final KeyValueIterator<Windowed<Bytes>, byte[]> iterator =
                 cachingStore.fetch(null, bytesKey("d"), ofEpochMilli(DEFAULT_TIMESTAMP), ofEpochMilli(DEFAULT_TIMESTAMP + 20L))) {
            final List<Windowed<Bytes>> expectedKeys = Arrays.asList(
                new Windowed<>(bytesKey("a"), new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE)),
                new Windowed<>(bytesKey("b"), new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE)),
                new Windowed<>(bytesKey("c"), new TimeWindow(DEFAULT_TIMESTAMP + 10L, DEFAULT_TIMESTAMP + 10L + WINDOW_SIZE)),
                new Windowed<>(bytesKey("d"), new TimeWindow(DEFAULT_TIMESTAMP + 20L, DEFAULT_TIMESTAMP + 20L + WINDOW_SIZE))
            );

            final List<String> expectedValues = Arrays.asList("a", "b", "c", "d");

            verifyAllWindowedKeyValues(iterator, expectedKeys, expectedValues);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldPutFetchRangeFromCacheForNullKeyTo(final boolean hasIndex) {
        setUp(hasIndex);
        cachingStore.put(bytesKey("a"), bytesValue("a"), DEFAULT_TIMESTAMP);
        cachingStore.put(bytesKey("b"), bytesValue("b"), DEFAULT_TIMESTAMP);
        cachingStore.put(bytesKey("c"), bytesValue("c"), DEFAULT_TIMESTAMP + 10L);
        cachingStore.put(bytesKey("d"), bytesValue("d"), DEFAULT_TIMESTAMP + 20L);
        cachingStore.put(bytesKey("e"), bytesValue("e"), DEFAULT_TIMESTAMP + 20L);

        try (final KeyValueIterator<Windowed<Bytes>, byte[]> iterator =
                 cachingStore.fetch(bytesKey("b"), null, ofEpochMilli(DEFAULT_TIMESTAMP), ofEpochMilli(DEFAULT_TIMESTAMP + 20L))) {
            final List<Windowed<Bytes>> expectedKeys = Arrays.asList(
                new Windowed<>(bytesKey("b"), new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE)),
                new Windowed<>(bytesKey("c"), new TimeWindow(DEFAULT_TIMESTAMP + 10L, DEFAULT_TIMESTAMP + 10L + WINDOW_SIZE)),
                new Windowed<>(bytesKey("d"), new TimeWindow(DEFAULT_TIMESTAMP + 20L, DEFAULT_TIMESTAMP + 20L + WINDOW_SIZE)),
                new Windowed<>(bytesKey("e"), new TimeWindow(DEFAULT_TIMESTAMP + 20L, DEFAULT_TIMESTAMP + 20L + WINDOW_SIZE))
            );

            final List<String> expectedValues = Arrays.asList("b", "c", "d", "e");

            verifyAllWindowedKeyValues(iterator, expectedKeys, expectedValues);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldPutFetchRangeFromCacheForNullKeyFromKeyTo(final boolean hasIndex) {
        setUp(hasIndex);
        cachingStore.put(bytesKey("a"), bytesValue("a"), DEFAULT_TIMESTAMP);
        cachingStore.put(bytesKey("b"), bytesValue("b"), DEFAULT_TIMESTAMP);
        cachingStore.put(bytesKey("c"), bytesValue("c"), DEFAULT_TIMESTAMP + 10L);
        cachingStore.put(bytesKey("d"), bytesValue("d"), DEFAULT_TIMESTAMP + 20L);
        cachingStore.put(bytesKey("e"), bytesValue("e"), DEFAULT_TIMESTAMP + 20L);

        try (final KeyValueIterator<Windowed<Bytes>, byte[]> iterator =
                 cachingStore.fetch(null, null, ofEpochMilli(DEFAULT_TIMESTAMP), ofEpochMilli(DEFAULT_TIMESTAMP + 20L))) {
            final List<Windowed<Bytes>> expectedKeys = Arrays.asList(
                new Windowed<>(bytesKey("a"), new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE)),
                new Windowed<>(bytesKey("b"), new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE)),
                new Windowed<>(bytesKey("c"), new TimeWindow(DEFAULT_TIMESTAMP + 10L, DEFAULT_TIMESTAMP + 10L + WINDOW_SIZE)),
                new Windowed<>(bytesKey("d"), new TimeWindow(DEFAULT_TIMESTAMP + 20L, DEFAULT_TIMESTAMP + 20L + WINDOW_SIZE)),
                new Windowed<>(bytesKey("e"), new TimeWindow(DEFAULT_TIMESTAMP + 20L, DEFAULT_TIMESTAMP + 20L + WINDOW_SIZE))
            );

            final List<String> expectedValues = Arrays.asList("a", "b", "c", "d", "e");

            verifyAllWindowedKeyValues(iterator, expectedKeys, expectedValues);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldPutBackwardFetchRangeFromCacheForNullKeyFrom(final boolean hasIndex) {
        setUp(hasIndex);
        cachingStore.put(bytesKey("a"), bytesValue("a"), DEFAULT_TIMESTAMP);
        cachingStore.put(bytesKey("b"), bytesValue("b"), DEFAULT_TIMESTAMP);
        cachingStore.put(bytesKey("c"), bytesValue("c"), DEFAULT_TIMESTAMP + 10L);
        cachingStore.put(bytesKey("d"), bytesValue("d"), DEFAULT_TIMESTAMP + 20L);
        cachingStore.put(bytesKey("e"), bytesValue("e"), DEFAULT_TIMESTAMP + 20L);

        try (final KeyValueIterator<Windowed<Bytes>, byte[]> iterator =
                 cachingStore.backwardFetch(null, bytesKey("c"), ofEpochMilli(DEFAULT_TIMESTAMP), ofEpochMilli(DEFAULT_TIMESTAMP + 20L))) {
            final List<Windowed<Bytes>> expectedKeys = Arrays.asList(
                new Windowed<>(bytesKey("c"), new TimeWindow(DEFAULT_TIMESTAMP + 10L, DEFAULT_TIMESTAMP + 10L + WINDOW_SIZE)),
                new Windowed<>(bytesKey("b"), new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE)),
                new Windowed<>(bytesKey("a"), new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE))
            );

            final List<String> expectedValues = Arrays.asList("c", "b", "a");

            verifyAllWindowedKeyValues(iterator, expectedKeys, expectedValues);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldPutBackwardFetchRangeFromCacheForNullKeyTo(final boolean hasIndex) {
        setUp(hasIndex);
        cachingStore.put(bytesKey("a"), bytesValue("a"), DEFAULT_TIMESTAMP);
        cachingStore.put(bytesKey("b"), bytesValue("b"), DEFAULT_TIMESTAMP);
        cachingStore.put(bytesKey("c"), bytesValue("c"), DEFAULT_TIMESTAMP + 10L);
        cachingStore.put(bytesKey("d"), bytesValue("d"), DEFAULT_TIMESTAMP + 20L);
        cachingStore.put(bytesKey("e"), bytesValue("e"), DEFAULT_TIMESTAMP + 20L);

        try (final KeyValueIterator<Windowed<Bytes>, byte[]> iterator =
                 cachingStore.backwardFetch(bytesKey("c"), null, ofEpochMilli(DEFAULT_TIMESTAMP), ofEpochMilli(DEFAULT_TIMESTAMP + 20L))) {
            final List<Windowed<Bytes>> expectedKeys = Arrays.asList(
                new Windowed<>(bytesKey("e"), new TimeWindow(DEFAULT_TIMESTAMP + 20L, DEFAULT_TIMESTAMP + 20L + WINDOW_SIZE)),
                new Windowed<>(bytesKey("d"), new TimeWindow(DEFAULT_TIMESTAMP + 20L, DEFAULT_TIMESTAMP + 20L + WINDOW_SIZE)),
                new Windowed<>(bytesKey("c"), new TimeWindow(DEFAULT_TIMESTAMP + 10L, DEFAULT_TIMESTAMP + 10L + WINDOW_SIZE))
            );

            final List<String> expectedValues = Arrays.asList("e", "d", "c");

            verifyAllWindowedKeyValues(iterator, expectedKeys, expectedValues);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldPutBackwardFetchRangeFromCacheForNullKeyFromKeyTo(final boolean hasIndex) {
        setUp(hasIndex);
        cachingStore.put(bytesKey("a"), bytesValue("a"), DEFAULT_TIMESTAMP);
        cachingStore.put(bytesKey("b"), bytesValue("b"), DEFAULT_TIMESTAMP);
        cachingStore.put(bytesKey("c"), bytesValue("c"), DEFAULT_TIMESTAMP + 10L);
        cachingStore.put(bytesKey("d"), bytesValue("d"), DEFAULT_TIMESTAMP + 20L);
        cachingStore.put(bytesKey("e"), bytesValue("e"), DEFAULT_TIMESTAMP + 20L);

        try (final KeyValueIterator<Windowed<Bytes>, byte[]> iterator =
                 cachingStore.backwardFetch(null, null, ofEpochMilli(DEFAULT_TIMESTAMP), ofEpochMilli(DEFAULT_TIMESTAMP + 20L))) {
            final List<Windowed<Bytes>> expectedKeys = Arrays.asList(
                new Windowed<>(bytesKey("e"), new TimeWindow(DEFAULT_TIMESTAMP + 20L, DEFAULT_TIMESTAMP + 20L + WINDOW_SIZE)),
                new Windowed<>(bytesKey("d"), new TimeWindow(DEFAULT_TIMESTAMP + 20L, DEFAULT_TIMESTAMP + 20L + WINDOW_SIZE)),
                new Windowed<>(bytesKey("c"), new TimeWindow(DEFAULT_TIMESTAMP + 10L, DEFAULT_TIMESTAMP + 10L + WINDOW_SIZE)),
                new Windowed<>(bytesKey("b"), new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE)),
                new Windowed<>(bytesKey("a"), new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE))
            );

            final List<String> expectedValues = Arrays.asList("e", "d", "c", "b", "a");

            verifyAllWindowedKeyValues(iterator, expectedKeys, expectedValues);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldGetAllFromCache(final boolean hasIndex) {
        setUp(hasIndex);
        cachingStore.put(bytesKey("a"), bytesValue("a"), DEFAULT_TIMESTAMP);
        cachingStore.put(bytesKey("b"), bytesValue("b"), DEFAULT_TIMESTAMP);
        cachingStore.put(bytesKey("c"), bytesValue("c"), DEFAULT_TIMESTAMP);
        cachingStore.put(bytesKey("d"), bytesValue("d"), DEFAULT_TIMESTAMP);
        cachingStore.put(bytesKey("e"), bytesValue("e"), DEFAULT_TIMESTAMP);
        cachingStore.put(bytesKey("f"), bytesValue("f"), DEFAULT_TIMESTAMP);
        cachingStore.put(bytesKey("g"), bytesValue("g"), DEFAULT_TIMESTAMP);
        cachingStore.put(bytesKey("h"), bytesValue("h"), DEFAULT_TIMESTAMP);

        try (final KeyValueIterator<Windowed<Bytes>, byte[]> iterator = cachingStore.all()) {
            final String[] array = {"a", "b", "c", "d", "e", "f", "g", "h"};
            for (final String s : array) {
                verifyWindowedKeyValue(
                    iterator.next(),
                    new Windowed<>(bytesKey(s), new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE)),
                    s);
            }
            assertFalse(iterator.hasNext());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldGetAllBackwardFromCache(final boolean hasIndex) {
        setUp(hasIndex);
        cachingStore.put(bytesKey("a"), bytesValue("a"), DEFAULT_TIMESTAMP);
        cachingStore.put(bytesKey("b"), bytesValue("b"), DEFAULT_TIMESTAMP);
        cachingStore.put(bytesKey("c"), bytesValue("c"), DEFAULT_TIMESTAMP);
        cachingStore.put(bytesKey("d"), bytesValue("d"), DEFAULT_TIMESTAMP);
        cachingStore.put(bytesKey("e"), bytesValue("e"), DEFAULT_TIMESTAMP);
        cachingStore.put(bytesKey("f"), bytesValue("f"), DEFAULT_TIMESTAMP);
        cachingStore.put(bytesKey("g"), bytesValue("g"), DEFAULT_TIMESTAMP);
        cachingStore.put(bytesKey("h"), bytesValue("h"), DEFAULT_TIMESTAMP);

        try (final KeyValueIterator<Windowed<Bytes>, byte[]> iterator = cachingStore.backwardAll()) {
            final String[] array = {"h", "g", "f", "e", "d", "c", "b", "a"};
            for (final String s : array) {
                verifyWindowedKeyValue(
                    iterator.next(),
                    new Windowed<>(bytesKey(s), new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE)),
                    s);
            }
            assertFalse(iterator.hasNext());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldFetchAllWithinTimestampRange(final boolean hasIndex) {
        setUp(hasIndex);
        final String[] array = {"a", "b", "c", "d", "e", "f", "g", "h"};
        for (int i = 0; i < array.length; i++) {
            cachingStore.put(bytesKey(array[i]), bytesValue(array[i]), i);
        }

        try (final KeyValueIterator<Windowed<Bytes>, byte[]> iterator =
                 cachingStore.fetchAll(ofEpochMilli(0), ofEpochMilli(7))) {
            for (int i = 0; i < array.length; i++) {
                final String str = array[i];
                verifyWindowedKeyValue(
                    iterator.next(),
                    new Windowed<>(bytesKey(str), new TimeWindow(i, i + WINDOW_SIZE)),
                    str);
            }
            assertFalse(iterator.hasNext());
        }

        try (final KeyValueIterator<Windowed<Bytes>, byte[]> iterator1 =
                 cachingStore.fetchAll(ofEpochMilli(2), ofEpochMilli(4))) {
            for (int i = 2; i <= 4; i++) {
                final String str = array[i];
                verifyWindowedKeyValue(
                    iterator1.next(),
                    new Windowed<>(bytesKey(str), new TimeWindow(i, i + WINDOW_SIZE)),
                    str);
            }
            assertFalse(iterator1.hasNext());
        }

        try (final KeyValueIterator<Windowed<Bytes>, byte[]> iterator2 =
                 cachingStore.fetchAll(ofEpochMilli(5), ofEpochMilli(7))) {
            for (int i = 5; i <= 7; i++) {
                final String str = array[i];
                verifyWindowedKeyValue(
                    iterator2.next(),
                    new Windowed<>(bytesKey(str), new TimeWindow(i, i + WINDOW_SIZE)),
                    str);
            }
            assertFalse(iterator2.hasNext());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldFetchAllBackwardWithinTimestampRange(final boolean hasIndex) {
        setUp(hasIndex);
        final String[] array = {"a", "b", "c", "d", "e", "f", "g", "h"};
        for (int i = 0; i < array.length; i++) {
            cachingStore.put(bytesKey(array[i]), bytesValue(array[i]), i);
        }

        try (final KeyValueIterator<Windowed<Bytes>, byte[]> iterator =
                 cachingStore.backwardFetchAll(ofEpochMilli(0), ofEpochMilli(7))) {
            for (int i = array.length - 1; i >= 0; i--) {
                final String str = array[i];
                verifyWindowedKeyValue(
                    iterator.next(),
                    new Windowed<>(bytesKey(str), new TimeWindow(i, i + WINDOW_SIZE)),
                    str);
            }
            assertFalse(iterator.hasNext());
        }

        try (final KeyValueIterator<Windowed<Bytes>, byte[]> iterator1 =
                 cachingStore.backwardFetchAll(ofEpochMilli(2), ofEpochMilli(4))) {
            for (int i = 4; i >= 2; i--) {
                final String str = array[i];
                verifyWindowedKeyValue(
                    iterator1.next(),
                    new Windowed<>(bytesKey(str), new TimeWindow(i, i + WINDOW_SIZE)),
                    str);
            }
            assertFalse(iterator1.hasNext());
        }

        try (final KeyValueIterator<Windowed<Bytes>, byte[]> iterator2 =
                 cachingStore.backwardFetchAll(ofEpochMilli(5), ofEpochMilli(7))) {
            for (int i = 7; i >= 5; i--) {
                final String str = array[i];
                verifyWindowedKeyValue(
                    iterator2.next(),
                    new Windowed<>(bytesKey(str), new TimeWindow(i, i + WINDOW_SIZE)),
                    str);
            }
            assertFalse(iterator2.hasNext());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldFlushEvictedItemsIntoUnderlyingStore(final boolean hasIndex) {
        setUp(hasIndex);
        final int added = addItemsToCache();
        // all dirty entries should have been flushed
        try (final KeyValueIterator<Bytes, byte[]> iter = bytesStore.fetch(
            Bytes.wrap("0".getBytes(StandardCharsets.UTF_8)),
            DEFAULT_TIMESTAMP,
            DEFAULT_TIMESTAMP)) {
            final KeyValue<Bytes, byte[]> next = iter.next();
            assertEquals(DEFAULT_TIMESTAMP, baseKeySchema.segmentTimestamp(next.key));
            assertArrayEquals("0".getBytes(), next.value);
            assertFalse(iter.hasNext());
            assertEquals(added - 1, cache.size());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldForwardDirtyItemsWhenFlushCalled(final boolean hasIndex) {
        setUp(hasIndex);
        final Windowed<String> windowedKey =
            new Windowed<>("1", new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE));
        cachingStore.put(bytesKey("1"), bytesValue("a"), DEFAULT_TIMESTAMP);
        cachingStore.flush();
        assertEquals("a", cacheListener.forwarded.get(windowedKey).newValue);
        assertNull(cacheListener.forwarded.get(windowedKey).oldValue);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldSetFlushListener(final boolean hasIndex) {
        setUp(hasIndex);
        assertTrue(cachingStore.setFlushListener(null, true));
        assertTrue(cachingStore.setFlushListener(null, false));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldForwardOldValuesWhenEnabled(final boolean hasIndex) {
        setUp(hasIndex);
        cachingStore.setFlushListener(cacheListener, true);
        final Windowed<String> windowedKey =
            new Windowed<>("1", new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE));
        cachingStore.put(bytesKey("1"), bytesValue("a"), DEFAULT_TIMESTAMP);
        cachingStore.put(bytesKey("1"), bytesValue("b"), DEFAULT_TIMESTAMP);
        cachingStore.flush();
        assertEquals("b", cacheListener.forwarded.get(windowedKey).newValue);
        assertNull(cacheListener.forwarded.get(windowedKey).oldValue);
        cacheListener.forwarded.clear();
        cachingStore.put(bytesKey("1"), bytesValue("c"), DEFAULT_TIMESTAMP);
        cachingStore.flush();
        assertEquals("c", cacheListener.forwarded.get(windowedKey).newValue);
        assertEquals("b", cacheListener.forwarded.get(windowedKey).oldValue);
        cachingStore.put(bytesKey("1"), null, DEFAULT_TIMESTAMP);
        cachingStore.flush();
        assertNull(cacheListener.forwarded.get(windowedKey).newValue);
        assertEquals("c", cacheListener.forwarded.get(windowedKey).oldValue);
        cacheListener.forwarded.clear();
        cachingStore.put(bytesKey("1"), bytesValue("a"), DEFAULT_TIMESTAMP);
        cachingStore.put(bytesKey("1"), bytesValue("b"), DEFAULT_TIMESTAMP);
        cachingStore.put(bytesKey("1"), null, DEFAULT_TIMESTAMP);
        cachingStore.flush();
        assertNull(cacheListener.forwarded.get(windowedKey));
        cacheListener.forwarded.clear();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldNotForwardOldValuesWhenDisabled(final boolean hasIndex) {
        setUp(hasIndex);
        final Windowed<String> windowedKey =
            new Windowed<>("1", new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE));
        cachingStore.put(bytesKey("1"), bytesValue("a"), DEFAULT_TIMESTAMP);
        cachingStore.put(bytesKey("1"), bytesValue("b"), DEFAULT_TIMESTAMP);
        cachingStore.flush();
        assertEquals("b", cacheListener.forwarded.get(windowedKey).newValue);
        assertNull(cacheListener.forwarded.get(windowedKey).oldValue);
        cachingStore.put(bytesKey("1"), bytesValue("c"), DEFAULT_TIMESTAMP);
        cachingStore.flush();
        assertEquals("c", cacheListener.forwarded.get(windowedKey).newValue);
        assertNull(cacheListener.forwarded.get(windowedKey).oldValue);
        cachingStore.put(bytesKey("1"), null, DEFAULT_TIMESTAMP);
        cachingStore.flush();
        assertNull(cacheListener.forwarded.get(windowedKey).newValue);
        assertNull(cacheListener.forwarded.get(windowedKey).oldValue);
        cacheListener.forwarded.clear();
        cachingStore.put(bytesKey("1"), bytesValue("a"), DEFAULT_TIMESTAMP);
        cachingStore.put(bytesKey("1"), bytesValue("b"), DEFAULT_TIMESTAMP);
        cachingStore.put(bytesKey("1"), null, DEFAULT_TIMESTAMP);
        cachingStore.flush();
        assertNull(cacheListener.forwarded.get(windowedKey));
        cacheListener.forwarded.clear();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldForwardDirtyItemToListenerWhenEvicted(final boolean hasIndex) {
        setUp(hasIndex);
        final int numRecords = addItemsToCache();
        assertEquals(numRecords, cacheListener.forwarded.size());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldTakeValueFromCacheIfSameTimestampFlushedToRocks(final boolean hasIndex) {
        setUp(hasIndex);
        cachingStore.put(bytesKey("1"), bytesValue("a"), DEFAULT_TIMESTAMP);
        cachingStore.flush();
        cachingStore.put(bytesKey("1"), bytesValue("b"), DEFAULT_TIMESTAMP);

        try (final WindowStoreIterator<byte[]> fetch =
                 cachingStore.fetch(bytesKey("1"), ofEpochMilli(DEFAULT_TIMESTAMP), ofEpochMilli(DEFAULT_TIMESTAMP))) {
            verifyKeyValue(fetch.next(), DEFAULT_TIMESTAMP, "b");
            assertFalse(fetch.hasNext());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldIterateAcrossWindows(final boolean hasIndex) {
        setUp(hasIndex);
        cachingStore.put(bytesKey("1"), bytesValue("a"), DEFAULT_TIMESTAMP);
        cachingStore.put(bytesKey("1"), bytesValue("b"), DEFAULT_TIMESTAMP + WINDOW_SIZE);

        try (final WindowStoreIterator<byte[]> fetch =
                 cachingStore.fetch(bytesKey("1"), ofEpochMilli(DEFAULT_TIMESTAMP), ofEpochMilli(DEFAULT_TIMESTAMP + WINDOW_SIZE))) {
            verifyKeyValue(fetch.next(), DEFAULT_TIMESTAMP, "a");
            verifyKeyValue(fetch.next(), DEFAULT_TIMESTAMP + WINDOW_SIZE, "b");
            assertFalse(fetch.hasNext());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldIterateBackwardAcrossWindows(final boolean hasIndex) {
        setUp(hasIndex);
        cachingStore.put(bytesKey("1"), bytesValue("a"), DEFAULT_TIMESTAMP);
        cachingStore.put(bytesKey("1"), bytesValue("b"), DEFAULT_TIMESTAMP + WINDOW_SIZE);

        try (final WindowStoreIterator<byte[]> fetch =
                 cachingStore.backwardFetch(bytesKey("1"), ofEpochMilli(DEFAULT_TIMESTAMP), ofEpochMilli(DEFAULT_TIMESTAMP + WINDOW_SIZE))) {
            verifyKeyValue(fetch.next(), DEFAULT_TIMESTAMP + WINDOW_SIZE, "b");
            verifyKeyValue(fetch.next(), DEFAULT_TIMESTAMP, "a");
            assertFalse(fetch.hasNext());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldIterateCacheAndStore(final boolean hasIndex) {
        setUp(hasIndex);
        final Bytes key = Bytes.wrap("1".getBytes());
        bytesStore.put(TimeFirstWindowKeySchema.toStoreKeyBinary(key, DEFAULT_TIMESTAMP, 0), "a".getBytes());
        cachingStore.put(key, bytesValue("b"), DEFAULT_TIMESTAMP + WINDOW_SIZE);
        try (final WindowStoreIterator<byte[]> fetch =
                 cachingStore.fetch(bytesKey("1"), ofEpochMilli(DEFAULT_TIMESTAMP), ofEpochMilli(DEFAULT_TIMESTAMP + WINDOW_SIZE))) {
            verifyKeyValue(fetch.next(), DEFAULT_TIMESTAMP, "a");
            verifyKeyValue(fetch.next(), DEFAULT_TIMESTAMP + WINDOW_SIZE, "b");
            assertFalse(fetch.hasNext());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldIterateBackwardCacheAndStore(final boolean hasIndex) {
        setUp(hasIndex);
        final Bytes key = Bytes.wrap("1".getBytes());
        bytesStore.put(TimeFirstWindowKeySchema.toStoreKeyBinary(key, DEFAULT_TIMESTAMP, 0), "a".getBytes());
        cachingStore.put(key, bytesValue("b"), DEFAULT_TIMESTAMP + WINDOW_SIZE);
        try (final WindowStoreIterator<byte[]> fetch =
                 cachingStore.backwardFetch(bytesKey("1"), ofEpochMilli(DEFAULT_TIMESTAMP), ofEpochMilli(DEFAULT_TIMESTAMP + WINDOW_SIZE))) {
            verifyKeyValue(fetch.next(), DEFAULT_TIMESTAMP + WINDOW_SIZE, "b");
            verifyKeyValue(fetch.next(), DEFAULT_TIMESTAMP, "a");
            assertFalse(fetch.hasNext());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldIterateCacheAndStoreKeyRange(final boolean hasIndex) {
        setUp(hasIndex);
        final Bytes key = Bytes.wrap("1".getBytes());
        bytesStore.put(TimeFirstWindowKeySchema.toStoreKeyBinary(key, DEFAULT_TIMESTAMP, 0), "a".getBytes());
        cachingStore.put(key, bytesValue("b"), DEFAULT_TIMESTAMP + WINDOW_SIZE);

        try (final KeyValueIterator<Windowed<Bytes>, byte[]> fetchRange =
                 cachingStore.fetch(key, bytesKey("2"), ofEpochMilli(DEFAULT_TIMESTAMP), ofEpochMilli(DEFAULT_TIMESTAMP + WINDOW_SIZE))) {
            verifyWindowedKeyValue(
                fetchRange.next(),
                new Windowed<>(key, new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE)),
                "a");
            verifyWindowedKeyValue(
                fetchRange.next(),
                new Windowed<>(key, new TimeWindow(DEFAULT_TIMESTAMP + WINDOW_SIZE, DEFAULT_TIMESTAMP + WINDOW_SIZE + WINDOW_SIZE)),
                "b");
            assertFalse(fetchRange.hasNext());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldIterateBackwardCacheAndStoreKeyRange(final boolean hasIndex) {
        setUp(hasIndex);
        final Bytes key = Bytes.wrap("1".getBytes());
        bytesStore.put(TimeFirstWindowKeySchema.toStoreKeyBinary(key, DEFAULT_TIMESTAMP, 0), "a".getBytes());
        cachingStore.put(key, bytesValue("b"), DEFAULT_TIMESTAMP + WINDOW_SIZE);

        try (final KeyValueIterator<Windowed<Bytes>, byte[]> fetchRange =
                 cachingStore.backwardFetch(key, bytesKey("2"), ofEpochMilli(DEFAULT_TIMESTAMP), ofEpochMilli(DEFAULT_TIMESTAMP + WINDOW_SIZE))) {
            verifyWindowedKeyValue(
                fetchRange.next(),
                new Windowed<>(key, new TimeWindow(DEFAULT_TIMESTAMP + WINDOW_SIZE, DEFAULT_TIMESTAMP + WINDOW_SIZE + WINDOW_SIZE)),
                "b");
            verifyWindowedKeyValue(
                fetchRange.next(),
                new Windowed<>(key, new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE)),
                "a");
            assertFalse(fetchRange.hasNext());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldClearNamespaceCacheOnClose(final boolean hasIndex) {
        setUp(hasIndex);
        cachingStore.put(bytesKey("a"), bytesValue("a"), 0L);
        final int size = hasIndex ? 2 : 1;
        assertEquals(size, cache.size());
        cachingStore.close();
        assertEquals(0, cache.size());
    }

    @SuppressWarnings("resource")
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldThrowIfTryingToFetchFromClosedCachingStore(final boolean hasIndex) {
        setUp(hasIndex);
        cachingStore.close();
        assertThrows(InvalidStateStoreException.class, () -> cachingStore.fetch(bytesKey("a"), ofEpochMilli(0), ofEpochMilli(10)));
    }

    @SuppressWarnings("resource")
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldThrowIfTryingToFetchRangeFromClosedCachingStore(final boolean hasIndex) {
        setUp(hasIndex);
        cachingStore.close();
        assertThrows(InvalidStateStoreException.class, () -> cachingStore.fetch(bytesKey("a"), bytesKey("b"), ofEpochMilli(0), ofEpochMilli(10)));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldThrowIfTryingToWriteToClosedCachingStore(final boolean hasIndex) {
        setUp(hasIndex);
        cachingStore.close();
        assertThrows(InvalidStateStoreException.class, () -> cachingStore.put(bytesKey("a"), bytesValue("a"), 0L));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldSkipNonExistBaseKeyInCache(final boolean hasIndex) {
        setUp(hasIndex);
        cachingStore.put(bytesKey("aa"), bytesValue("0002"), 0);

        final SegmentedCacheFunction indexCacheFunction = new SegmentedCacheFunction(new KeyFirstWindowKeySchema(), SEGMENT_INTERVAL);

        final Bytes key = bytesKey("a");
        final byte[] value = bytesValue("0001");
        final Bytes cacheIndexKey = indexCacheFunction.cacheKey(KeyFirstWindowKeySchema.toStoreKeyBinary(key, 1, 0));
        final String cacheName = context.taskId() + "-test";

        // Only put index to store
        cache.put(cacheName,
            cacheIndexKey,
            new LRUCacheEntry(
                new byte[0],
                new RecordHeaders(),
                true,
                context.offset(),
                context.timestamp(),
                context.partition(),
                "")
        );

        underlyingStore.put(key, value, 1);

        if (hasIndex) {
            verifyKeyValueList(
                asList(
                    windowedPair("a", "0001", 1),
                    windowedPair("aa", "0002", 0)
                ),
                toListAndCloseIterator(cachingStore.fetch(bytesKey("a"), bytesKey("aa"), ofEpochMilli(0),
                    ofEpochMilli(Long.MAX_VALUE)))
            );
        } else {
            verifyKeyValueList(
                asList(
                    windowedPair("aa", "0002", 0),
                    windowedPair("a", "0001", 1)
                ),
                toListAndCloseIterator(cachingStore.fetch(bytesKey("a"), bytesKey("aa"), ofEpochMilli(0),
                    ofEpochMilli(Long.MAX_VALUE)))
            );
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldFetchAndIterateOverExactKeys(final boolean hasIndex) {
        setUp(hasIndex);
        cachingStore.put(bytesKey("a"), bytesValue("0001"), 0);
        cachingStore.put(bytesKey("aa"), bytesValue("0002"), 0);
        cachingStore.put(bytesKey("a"), bytesValue("0003"), 1);
        cachingStore.put(bytesKey("aa"), bytesValue("0004"), 1);
        cachingStore.put(bytesKey("a"), bytesValue("0005"), SEGMENT_INTERVAL);

        final List<KeyValue<Long, byte[]>> expected = asList(
            KeyValue.pair(0L, bytesValue("0001")),
            KeyValue.pair(1L, bytesValue("0003")),
            KeyValue.pair(SEGMENT_INTERVAL, bytesValue("0005"))
        );
        final List<KeyValue<Long, byte[]>> actual =
            toListAndCloseIterator(cachingStore.fetch(bytesKey("a"), ofEpochMilli(0), ofEpochMilli(Long.MAX_VALUE)));
        verifyKeyValueList(expected, actual);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldBackwardFetchAndIterateOverExactKeys(final boolean hasIndex) {
        setUp(hasIndex);
        cachingStore.put(bytesKey("a"), bytesValue("0001"), 0);
        cachingStore.put(bytesKey("aa"), bytesValue("0002"), 0);
        cachingStore.put(bytesKey("a"), bytesValue("0003"), 1);
        cachingStore.put(bytesKey("aa"), bytesValue("0004"), 1);
        cachingStore.put(bytesKey("a"), bytesValue("0005"), SEGMENT_INTERVAL);

        final List<KeyValue<Long, byte[]>> expected = asList(
            KeyValue.pair(SEGMENT_INTERVAL, bytesValue("0005")),
            KeyValue.pair(1L, bytesValue("0003")),
            KeyValue.pair(0L, bytesValue("0001"))
        );
        final List<KeyValue<Long, byte[]>> actual =
            toListAndCloseIterator(cachingStore.backwardFetch(bytesKey("a"), ofEpochMilli(0), ofEpochMilli(Long.MAX_VALUE)));
        verifyKeyValueList(expected, actual);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldFetchAndIterateOverKeyRange(final boolean hasIndex) {
        setUp(hasIndex);
        cachingStore.put(bytesKey("a"), bytesValue("0001"), 0);
        cachingStore.put(bytesKey("aa"), bytesValue("0002"), 0);
        cachingStore.put(bytesKey("a"), bytesValue("0003"), 1);
        cachingStore.put(bytesKey("aa"), bytesValue("0004"), 1);
        cachingStore.put(bytesKey("a"), bytesValue("0005"), SEGMENT_INTERVAL);

        verifyKeyValueList(
            asList(
                windowedPair("a", "0001", 0),
                windowedPair("a", "0003", 1),
                windowedPair("a", "0005", SEGMENT_INTERVAL)
            ),
            toListAndCloseIterator(cachingStore.fetch(bytesKey("a"), bytesKey("a"), ofEpochMilli(0), ofEpochMilli(Long.MAX_VALUE)))
        );

        verifyKeyValueList(
            asList(
                windowedPair("aa", "0002", 0),
                windowedPair("aa", "0004", 1)),
            toListAndCloseIterator(cachingStore.fetch(bytesKey("aa"), bytesKey("aa"), ofEpochMilli(0), ofEpochMilli(Long.MAX_VALUE)))
        );

        if (hasIndex) {
            verifyKeyValueList(
                asList(
                    windowedPair("a", "0001", 0),
                    windowedPair("a", "0003", 1),
                    windowedPair("aa", "0002", 0),
                    windowedPair("aa", "0004", 1),
                    windowedPair("a", "0005", SEGMENT_INTERVAL)
                ),
                toListAndCloseIterator(cachingStore.fetch(bytesKey("a"), bytesKey("aa"), ofEpochMilli(0),
                    ofEpochMilli(Long.MAX_VALUE)))
            );
        } else {
            verifyKeyValueList(
                asList(
                    windowedPair("a", "0001", 0),
                    windowedPair("aa", "0002", 0),
                    windowedPair("a", "0003", 1),
                    windowedPair("aa", "0004", 1),
                    windowedPair("a", "0005", SEGMENT_INTERVAL)
                ),
                toListAndCloseIterator(cachingStore.fetch(bytesKey("a"), bytesKey("aa"), ofEpochMilli(0),
                    ofEpochMilli(Long.MAX_VALUE)))
            );
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldFetchAndIterateOverKeyBackwardRange(final boolean hasIndex) {
        setUp(hasIndex);
        cachingStore.put(bytesKey("a"), bytesValue("0001"), 0);
        cachingStore.put(bytesKey("aa"), bytesValue("0002"), 0);
        cachingStore.put(bytesKey("a"), bytesValue("0003"), 1);
        cachingStore.put(bytesKey("aa"), bytesValue("0004"), 1);
        cachingStore.put(bytesKey("a"), bytesValue("0005"), SEGMENT_INTERVAL);

        verifyKeyValueList(
            asList(
                windowedPair("a", "0005", SEGMENT_INTERVAL),
                windowedPair("a", "0003", 1),
                windowedPair("a", "0001", 0)
            ),
            toListAndCloseIterator(cachingStore.backwardFetch(bytesKey("a"), bytesKey("a"), ofEpochMilli(0), ofEpochMilli(Long.MAX_VALUE)))
        );

        verifyKeyValueList(
            asList(
                windowedPair("aa", "0004", 1),
                windowedPair("aa", "0002", 0)),
            toListAndCloseIterator(cachingStore.backwardFetch(bytesKey("aa"), bytesKey("aa"), ofEpochMilli(0), ofEpochMilli(Long.MAX_VALUE)))
        );

        if (!hasIndex) {
            verifyKeyValueList(
                // Ordered by timestamp if has no index
                asList(
                    windowedPair("a", "0005", SEGMENT_INTERVAL),
                    windowedPair("aa", "0004", 1),
                    windowedPair("a", "0003", 1),
                    windowedPair("aa", "0002", 0),
                    windowedPair("a", "0001", 0)
                ),
                toListAndCloseIterator(cachingStore.backwardFetch(bytesKey("a"), bytesKey("aa"), ofEpochMilli(0),
                    ofEpochMilli(Long.MAX_VALUE)))
            );
        } else {
            verifyKeyValueList(
                asList(
                    // First because in larger segments
                    windowedPair("a", "0005", SEGMENT_INTERVAL),
                    windowedPair("aa", "0004", 1),
                    windowedPair("aa", "0002", 0),
                    windowedPair("a", "0003", 1),
                    windowedPair("a", "0001", 0)
                ),
                toListAndCloseIterator(cachingStore.backwardFetch(bytesKey("a"), bytesKey("aa"), ofEpochMilli(0),
                    ofEpochMilli(Long.MAX_VALUE)))
            );
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldReturnSameResultsForSingleKeyFetchAndEqualKeyRangeFetch(final boolean hasIndex) {
        setUp(hasIndex);
        cachingStore.put(bytesKey("a"), bytesValue("0001"), 0);
        cachingStore.put(bytesKey("aa"), bytesValue("0002"), 1);
        cachingStore.put(bytesKey("aa"), bytesValue("0003"), 2);
        cachingStore.put(bytesKey("aaa"), bytesValue("0004"), 3);

        try (final WindowStoreIterator<byte[]> singleKeyIterator = cachingStore.fetch(bytesKey("aa"), 0L, 5L);
             final KeyValueIterator<Windowed<Bytes>, byte[]> keyRangeIterator = cachingStore.fetch(bytesKey("aa"), bytesKey("aa"), 0L, 5L)) {

            assertEquals(stringFrom(singleKeyIterator.next().value), stringFrom(keyRangeIterator.next().value));
            assertEquals(stringFrom(singleKeyIterator.next().value), stringFrom(keyRangeIterator.next().value));
            assertFalse(singleKeyIterator.hasNext());
            assertFalse(keyRangeIterator.hasNext());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldReturnSameResultsForSingleKeyFetchAndEqualKeyRangeBackwardFetch(final boolean hasIndex) {
        setUp(hasIndex);
        cachingStore.put(bytesKey("a"), bytesValue("0001"), 0);
        cachingStore.put(bytesKey("aa"), bytesValue("0002"), 1);
        cachingStore.put(bytesKey("aa"), bytesValue("0003"), 2);
        cachingStore.put(bytesKey("aaa"), bytesValue("0004"), 3);

        try (final WindowStoreIterator<byte[]> singleKeyIterator =
                 cachingStore.backwardFetch(bytesKey("aa"), Instant.ofEpochMilli(0L), Instant.ofEpochMilli(5L));
             final KeyValueIterator<Windowed<Bytes>, byte[]> keyRangeIterator =
                 cachingStore.backwardFetch(bytesKey("aa"), bytesKey("aa"), Instant.ofEpochMilli(0L), Instant.ofEpochMilli(5L))) {

            assertEquals(stringFrom(singleKeyIterator.next().value), stringFrom(keyRangeIterator.next().value));
            assertEquals(stringFrom(singleKeyIterator.next().value), stringFrom(keyRangeIterator.next().value));
            assertFalse(singleKeyIterator.hasNext());
            assertFalse(keyRangeIterator.hasNext());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldThrowNullPointerExceptionOnPutNullKey(final boolean hasIndex) {
        setUp(hasIndex);
        assertThrows(NullPointerException.class, () -> cachingStore.put(null, bytesValue("anyValue"), 0L));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldNotThrowNullPointerExceptionOnPutNullValue(final boolean hasIndex) {
        setUp(hasIndex);
        cachingStore.put(bytesKey("a"), null, 0L);
    }

    @SuppressWarnings("resource")
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldThrowNullPointerExceptionOnFetchNullKey(final boolean hasIndex) {
        setUp(hasIndex);
        assertThrows(NullPointerException.class, () -> cachingStore.fetch(null, ofEpochMilli(1L), ofEpochMilli(2L)));
    }

    @SuppressWarnings("resource")
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldNotThrowInvalidRangeExceptionWithNegativeFromKey(final boolean hasIndex) {
        setUp(hasIndex);
        final Bytes keyFrom = Bytes.wrap(new IntegerSerializer().serialize("", -1));
        final Bytes keyTo = Bytes.wrap(new IntegerSerializer().serialize("", 1));

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(TimeOrderedCachingWindowStore.class);
             final KeyValueIterator<Windowed<Bytes>, byte[]> iterator = cachingStore.fetch(keyFrom, keyTo, 0L, 10L)) {
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

    @SuppressWarnings("resource")
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldNotThrowInvalidBackwardRangeExceptionWithNegativeFromKey(final boolean hasIndex) {
        setUp(hasIndex);
        final Bytes keyFrom = Bytes.wrap(new IntegerSerializer().serialize("", -1));
        final Bytes keyTo = Bytes.wrap(new IntegerSerializer().serialize("", 1));

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(TimeOrderedCachingWindowStore.class);
             final KeyValueIterator<Windowed<Bytes>, byte[]> iterator =
                 cachingStore.backwardFetch(keyFrom, keyTo, Instant.ofEpochMilli(0L), Instant.ofEpochMilli(10L))) {
            assertFalse(iterator.hasNext());

            final List<String> messages = appender.getMessages();
            assertThat(
                messages,
                hasItem("Returning empty iterator for fetch with invalid key range: from > to." +
                    " This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes." +
                    " Note that the built-in numerical serdes do not follow this for negative numbers")
            );
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldCloseCacheAndWrappedStoreAfterErrorDuringCacheFlush(final boolean hasIndex) {
        setUp(hasIndex);
        setUpCloseTests();
        doThrow(new RuntimeException("Simulating an error on flush2")).doNothing()
                .when(cache).flush(CACHE_NAMESPACE);
        assertThrows(RuntimeException.class, cachingStore::close);
        verifyAndTearDownCloseTests();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldCloseWrappedStoreAfterErrorDuringCacheClose(final boolean hasIndex) {
        setUp(hasIndex);
        setUpCloseTests();
        doThrow(new RuntimeException("Simulating an error on close")).doNothing().when(cache).close(CACHE_NAMESPACE);
        assertThrows(RuntimeException.class, cachingStore::close);
        verifyAndTearDownCloseTests();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldCloseCacheAfterErrorDuringStateStoreClose(final boolean hasIndex) {
        setUp(hasIndex);
        setUpCloseTests();
        doThrow(new RuntimeException("Simulating an error on close")).doNothing().when(underlyingStore).close();
        assertThrows(RuntimeException.class, cachingStore::close);
        verifyAndTearDownCloseTests();
    }

    private void setUpCloseTests() {
        underlyingStore = mock(RocksDBTimeOrderedWindowStore.class);
        when(underlyingStore.name()).thenReturn("store-name");
        when(underlyingStore.isOpen()).thenReturn(true);
        cachingStore = new TimeOrderedCachingWindowStore(underlyingStore, WINDOW_SIZE, SEGMENT_INTERVAL);
        cache = mock(ThreadCache.class);
        context = new InternalMockProcessorContext<>(TestUtils.tempDirectory(), null, null, null, cache);
        context.setRecordContext(new ProcessorRecordContext(10, 0, 0, TOPIC, new RecordHeaders()));
        cachingStore.init(context, cachingStore);
    }

    private static KeyValue<Windowed<Bytes>, byte[]> windowedPair(final String key, final String value, final long timestamp) {
        return KeyValue.pair(
            new Windowed<>(bytesKey(key), new TimeWindow(timestamp, timestamp + WINDOW_SIZE)),
            bytesValue(value));
    }

    private int addItemsToCache() {
        long cachedSize = 0;
        int i = 0;
        while (cachedSize < MAX_CACHE_SIZE_BYTES) {
            final String kv = String.valueOf(i++);
            cachingStore.put(bytesKey(kv), bytesValue(kv), DEFAULT_TIMESTAMP);
            cachedSize += memoryCacheEntrySize(kv.getBytes(), kv.getBytes(), TOPIC) +
                8 + // timestamp
                4; // sequenceNumber
        }
        return i;
    }

    private void verifyAndTearDownCloseTests() {
        verify(underlyingStore).close();
        verify(cache).flush(CACHE_NAMESPACE);
        verify(cache).close(CACHE_NAMESPACE);
    }

}
