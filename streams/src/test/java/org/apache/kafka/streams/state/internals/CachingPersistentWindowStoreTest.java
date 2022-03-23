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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.testutil.LogCaptureAppender;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.TestUtils;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
import static org.apache.kafka.test.StreamsTestUtils.toList;
import static org.apache.kafka.test.StreamsTestUtils.verifyAllWindowedKeyValues;
import static org.apache.kafka.test.StreamsTestUtils.verifyKeyValueList;
import static org.apache.kafka.test.StreamsTestUtils.verifyWindowedKeyValue;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class CachingPersistentWindowStoreTest {

    private static final int MAX_CACHE_SIZE_BYTES = 150;
    private static final long DEFAULT_TIMESTAMP = 10L;
    private static final Long WINDOW_SIZE = 10L;
    private static final long SEGMENT_INTERVAL = 100L;
    private final static String TOPIC = "topic";
    private static final String CACHE_NAMESPACE = "0_0-store-name";

    private InternalMockProcessorContext context;
    private RocksDBSegmentedBytesStore bytesStore;
    private WindowStore<Bytes, byte[]> underlyingStore;
    private CachingWindowStore cachingStore;
    private CacheFlushListenerStub<Windowed<String>, String> cacheListener;
    private ThreadCache cache;
    private WindowKeySchema keySchema;

    @Before
    public void setUp() {
        keySchema = new WindowKeySchema();
        bytesStore = new RocksDBSegmentedBytesStore("test", "metrics-scope", 0, SEGMENT_INTERVAL, keySchema);
        underlyingStore = new RocksDBWindowStore(bytesStore, false, WINDOW_SIZE);
        final TimeWindowedDeserializer<String> keyDeserializer = new TimeWindowedDeserializer<>(new StringDeserializer(), WINDOW_SIZE);
        keyDeserializer.setIsChangelogTopic(true);
        cacheListener = new CacheFlushListenerStub<>(keyDeserializer, new StringDeserializer());
        cachingStore = new CachingWindowStore(underlyingStore, WINDOW_SIZE, SEGMENT_INTERVAL);
        cachingStore.setFlushListener(cacheListener, false);
        cache = new ThreadCache(new LogContext("testCache "), MAX_CACHE_SIZE_BYTES, new MockStreamsMetrics(new Metrics()));
        context = new InternalMockProcessorContext<>(TestUtils.tempDirectory(), null, null, null, cache);
        context.setRecordContext(new ProcessorRecordContext(DEFAULT_TIMESTAMP, 0, 0, TOPIC, new RecordHeaders()));
        cachingStore.init((StateStoreContext) context, cachingStore);
    }

    @After
    public void closeStore() {
        cachingStore.close();
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldDelegateDeprecatedInit() {
        final WindowStore<Bytes, byte[]> inner = EasyMock.mock(WindowStore.class);
        final CachingWindowStore outer = new CachingWindowStore(inner, WINDOW_SIZE, SEGMENT_INTERVAL);
        EasyMock.expect(inner.name()).andStubReturn("store");
        inner.init((ProcessorContext) context, outer);
        EasyMock.expectLastCall();
        EasyMock.replay(inner);
        outer.init((ProcessorContext) context, outer);
        EasyMock.verify(inner);
    }

    @Test
    public void shouldDelegateInit() {
        final WindowStore<Bytes, byte[]> inner = EasyMock.mock(WindowStore.class);
        final CachingWindowStore outer = new CachingWindowStore(inner, WINDOW_SIZE, SEGMENT_INTERVAL);
        EasyMock.expect(inner.name()).andStubReturn("store");
        inner.init((StateStoreContext) context, outer);
        EasyMock.expectLastCall();
        EasyMock.replay(inner);
        outer.init((StateStoreContext) context, outer);
        EasyMock.verify(inner);
    }

    @Test
    public void shouldNotReturnDuplicatesInRanges() {
        final StreamsBuilder builder = new StreamsBuilder();

        final StoreBuilder<WindowStore<String, String>> storeBuilder = Stores.windowStoreBuilder(
            Stores.persistentWindowStore("store-name", ofHours(1L), ofMinutes(1L), false),
            Serdes.String(),
            Serdes.String())
            .withCachingEnabled();

        builder.addStateStore(storeBuilder);

        builder.stream(TOPIC,
            Consumed.with(Serdes.String(), Serdes.String()))
            .transform(() -> new Transformer<String, String, KeyValue<String, String>>() {
                private WindowStore<String, String> store;
                private int numRecordsProcessed;
                private ProcessorContext context;

                @SuppressWarnings("unchecked")
                @Override
                public void init(final ProcessorContext processorContext) {
                    this.context = processorContext;
                    this.store = (WindowStore<String, String>) processorContext.getStateStore("store-name");
                    int count = 0;

                    try (final KeyValueIterator<Windowed<String>, String> all = store.all()) {
                        while (all.hasNext()) {
                            count++;
                            all.next();
                        }
                    }

                    assertThat(count, equalTo(0));
                }

                @Override
                public KeyValue<String, String> transform(final String key, final String value) {
                    int count = 0;

                    try (final KeyValueIterator<Windowed<String>, String> all = store.all()) {
                        while (all.hasNext()) {
                            count++;
                            all.next();
                        }
                    }

                    assertThat(count, equalTo(numRecordsProcessed));

                    store.put(value, value, context.timestamp());

                    numRecordsProcessed++;

                    return new KeyValue<>(key, value);
                }

                @Override
                public void close() {
                }
            }, "store-name");

        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000L);

        final Instant initialWallClockTime = Instant.ofEpochMilli(0L);
        final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), streamsConfiguration, initialWallClockTime);

        final TestInputTopic<String, String> inputTopic = driver.createInputTopic(TOPIC,
            Serdes.String().serializer(),
            Serdes.String().serializer(),
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

    @Test
    public void shouldPutFetchFromCache() {
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
            assertEquals(2, cache.size());
        }
    }

    @Test
    public void shouldMatchPositionAfterPutWithFlushListener() {
        cachingStore.setFlushListener(record -> { }, false);
        shouldMatchPositionAfterPut();
    }

    @Test
    public void shouldMatchPositionAfterPutWithoutFlushListener() {
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

    private String stringFrom(final byte[] from) {
        return Serdes.String().deserializer().deserialize("", from);
    }

    @Test
    public void shouldPutFetchRangeFromCache() {
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
            assertEquals(2, cache.size());
        }
    }

    @Test
    public void shouldPutFetchRangeFromCacheForNullKeyFrom() {
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

    @Test
    public void shouldPutFetchRangeFromCacheForNullKeyTo() {
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

    @Test
    public void shouldPutFetchRangeFromCacheForNullKeyFromKeyTo() {
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

    @Test
    public void shouldPutBackwardFetchRangeFromCacheForNullKeyFrom() {
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

    @Test
    public void shouldPutBackwardFetchRangeFromCacheForNullKeyTo() {
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

    @Test
    public void shouldPutBackwardFetchRangeFromCacheForNullKeyFromKeyTo() {
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

    @Test
    public void shouldGetAllFromCache() {
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

    @Test
    public void shouldGetAllBackwardFromCache() {
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

    @Test
    public void shouldFetchAllWithinTimestampRange() {
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

    @Test
    public void shouldFetchAllBackwardWithinTimestampRange() {
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

    @Test
    public void shouldFlushEvictedItemsIntoUnderlyingStore() {
        final int added = addItemsToCache();
        // all dirty entries should have been flushed
        try (final KeyValueIterator<Bytes, byte[]> iter = bytesStore.fetch(
            Bytes.wrap("0".getBytes(StandardCharsets.UTF_8)),
            DEFAULT_TIMESTAMP,
            DEFAULT_TIMESTAMP)) {
            final KeyValue<Bytes, byte[]> next = iter.next();
            assertEquals(DEFAULT_TIMESTAMP, keySchema.segmentTimestamp(next.key));
            assertArrayEquals("0".getBytes(), next.value);
            assertFalse(iter.hasNext());
            assertEquals(added - 1, cache.size());
        }
    }

    @Test
    public void shouldForwardDirtyItemsWhenFlushCalled() {
        final Windowed<String> windowedKey =
            new Windowed<>("1", new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE));
        cachingStore.put(bytesKey("1"), bytesValue("a"), DEFAULT_TIMESTAMP);
        cachingStore.flush();
        assertEquals("a", cacheListener.forwarded.get(windowedKey).newValue);
        assertNull(cacheListener.forwarded.get(windowedKey).oldValue);
    }

    @Test
    public void shouldSetFlushListener() {
        assertTrue(cachingStore.setFlushListener(null, true));
        assertTrue(cachingStore.setFlushListener(null, false));
    }

    @Test
    public void shouldForwardOldValuesWhenEnabled() {
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

    @Test
    public void shouldForwardOldValuesWhenDisabled() {
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

    @Test
    public void shouldForwardDirtyItemToListenerWhenEvicted() {
        final int numRecords = addItemsToCache();
        assertEquals(numRecords, cacheListener.forwarded.size());
    }

    @Test
    public void shouldTakeValueFromCacheIfSameTimestampFlushedToRocks() {
        cachingStore.put(bytesKey("1"), bytesValue("a"), DEFAULT_TIMESTAMP);
        cachingStore.flush();
        cachingStore.put(bytesKey("1"), bytesValue("b"), DEFAULT_TIMESTAMP);

        try (final WindowStoreIterator<byte[]> fetch =
                 cachingStore.fetch(bytesKey("1"), ofEpochMilli(DEFAULT_TIMESTAMP), ofEpochMilli(DEFAULT_TIMESTAMP))) {
            verifyKeyValue(fetch.next(), DEFAULT_TIMESTAMP, "b");
            assertFalse(fetch.hasNext());
        }
    }

    @Test
    public void shouldIterateAcrossWindows() {
        cachingStore.put(bytesKey("1"), bytesValue("a"), DEFAULT_TIMESTAMP);
        cachingStore.put(bytesKey("1"), bytesValue("b"), DEFAULT_TIMESTAMP + WINDOW_SIZE);

        try (final WindowStoreIterator<byte[]> fetch =
                 cachingStore.fetch(bytesKey("1"), ofEpochMilli(DEFAULT_TIMESTAMP), ofEpochMilli(DEFAULT_TIMESTAMP + WINDOW_SIZE))) {
            verifyKeyValue(fetch.next(), DEFAULT_TIMESTAMP, "a");
            verifyKeyValue(fetch.next(), DEFAULT_TIMESTAMP + WINDOW_SIZE, "b");
            assertFalse(fetch.hasNext());
        }
    }

    @Test
    public void shouldIterateBackwardAcrossWindows() {
        cachingStore.put(bytesKey("1"), bytesValue("a"), DEFAULT_TIMESTAMP);
        cachingStore.put(bytesKey("1"), bytesValue("b"), DEFAULT_TIMESTAMP + WINDOW_SIZE);

        try (final WindowStoreIterator<byte[]> fetch =
                 cachingStore.backwardFetch(bytesKey("1"), ofEpochMilli(DEFAULT_TIMESTAMP), ofEpochMilli(DEFAULT_TIMESTAMP + WINDOW_SIZE))) {
            verifyKeyValue(fetch.next(), DEFAULT_TIMESTAMP + WINDOW_SIZE, "b");
            verifyKeyValue(fetch.next(), DEFAULT_TIMESTAMP, "a");
            assertFalse(fetch.hasNext());
        }
    }

    @Test
    public void shouldIterateCacheAndStore() {
        final Bytes key = Bytes.wrap("1".getBytes());
        bytesStore.put(WindowKeySchema.toStoreKeyBinary(key, DEFAULT_TIMESTAMP, 0), "a".getBytes());
        cachingStore.put(key, bytesValue("b"), DEFAULT_TIMESTAMP + WINDOW_SIZE);
        try (final WindowStoreIterator<byte[]> fetch =
                 cachingStore.fetch(bytesKey("1"), ofEpochMilli(DEFAULT_TIMESTAMP), ofEpochMilli(DEFAULT_TIMESTAMP + WINDOW_SIZE))) {
            verifyKeyValue(fetch.next(), DEFAULT_TIMESTAMP, "a");
            verifyKeyValue(fetch.next(), DEFAULT_TIMESTAMP + WINDOW_SIZE, "b");
            assertFalse(fetch.hasNext());
        }
    }

    @Test
    public void shouldIterateBackwardCacheAndStore() {
        final Bytes key = Bytes.wrap("1".getBytes());
        bytesStore.put(WindowKeySchema.toStoreKeyBinary(key, DEFAULT_TIMESTAMP, 0), "a".getBytes());
        cachingStore.put(key, bytesValue("b"), DEFAULT_TIMESTAMP + WINDOW_SIZE);
        try (final WindowStoreIterator<byte[]> fetch =
                 cachingStore.backwardFetch(bytesKey("1"), ofEpochMilli(DEFAULT_TIMESTAMP), ofEpochMilli(DEFAULT_TIMESTAMP + WINDOW_SIZE))) {
            verifyKeyValue(fetch.next(), DEFAULT_TIMESTAMP + WINDOW_SIZE, "b");
            verifyKeyValue(fetch.next(), DEFAULT_TIMESTAMP, "a");
            assertFalse(fetch.hasNext());
        }
    }

    @Test
    public void shouldIterateCacheAndStoreKeyRange() {
        final Bytes key = Bytes.wrap("1".getBytes());
        bytesStore.put(WindowKeySchema.toStoreKeyBinary(key, DEFAULT_TIMESTAMP, 0), "a".getBytes());
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

    @Test
    public void shouldIterateBackwardCacheAndStoreKeyRange() {
        final Bytes key = Bytes.wrap("1".getBytes());
        bytesStore.put(WindowKeySchema.toStoreKeyBinary(key, DEFAULT_TIMESTAMP, 0), "a".getBytes());
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

    @Test
    public void shouldClearNamespaceCacheOnClose() {
        cachingStore.put(bytesKey("a"), bytesValue("a"), 0L);
        assertEquals(1, cache.size());
        cachingStore.close();
        assertEquals(0, cache.size());
    }

    @Test
    public void shouldThrowIfTryingToFetchFromClosedCachingStore() {
        cachingStore.close();
        assertThrows(InvalidStateStoreException.class, () -> cachingStore.fetch(bytesKey("a"), ofEpochMilli(0), ofEpochMilli(10)));
    }

    @Test
    public void shouldThrowIfTryingToFetchRangeFromClosedCachingStore() {
        cachingStore.close();
        assertThrows(InvalidStateStoreException.class, () -> cachingStore.fetch(bytesKey("a"), bytesKey("b"), ofEpochMilli(0), ofEpochMilli(10)));
    }

    @Test
    public void shouldThrowIfTryingToWriteToClosedCachingStore() {
        cachingStore.close();
        assertThrows(InvalidStateStoreException.class, () -> cachingStore.put(bytesKey("a"), bytesValue("a"), 0L));
    }

    @Test
    public void shouldFetchAndIterateOverExactKeys() {
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
            toList(cachingStore.fetch(bytesKey("a"), ofEpochMilli(0), ofEpochMilli(Long.MAX_VALUE)));
        verifyKeyValueList(expected, actual);
    }

    @Test
    public void shouldBackwardFetchAndIterateOverExactKeys() {
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
            toList(cachingStore.backwardFetch(bytesKey("a"), ofEpochMilli(0), ofEpochMilli(Long.MAX_VALUE)));
        verifyKeyValueList(expected, actual);
    }

    @Test
    public void shouldFetchAndIterateOverKeyRange() {
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
            toList(cachingStore.fetch(bytesKey("a"), bytesKey("a"), ofEpochMilli(0), ofEpochMilli(Long.MAX_VALUE)))
        );

        verifyKeyValueList(
            asList(
                windowedPair("aa", "0002", 0),
                windowedPair("aa", "0004", 1)),
            toList(cachingStore.fetch(bytesKey("aa"), bytesKey("aa"), ofEpochMilli(0), ofEpochMilli(Long.MAX_VALUE)))
        );

        verifyKeyValueList(
            asList(
                windowedPair("a", "0001", 0),
                windowedPair("a", "0003", 1),
                windowedPair("aa", "0002", 0),
                windowedPair("aa", "0004", 1),
                windowedPair("a", "0005", SEGMENT_INTERVAL)
            ),
            toList(cachingStore.fetch(bytesKey("a"), bytesKey("aa"), ofEpochMilli(0), ofEpochMilli(Long.MAX_VALUE)))
        );
    }

    @Test
    public void shouldFetchAndIterateOverKeyBackwardRange() {
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
            toList(cachingStore.backwardFetch(bytesKey("a"), bytesKey("a"), ofEpochMilli(0), ofEpochMilli(Long.MAX_VALUE)))
        );

        verifyKeyValueList(
            asList(
                windowedPair("aa", "0004", 1),
                windowedPair("aa", "0002", 0)),
            toList(cachingStore.backwardFetch(bytesKey("aa"), bytesKey("aa"), ofEpochMilli(0), ofEpochMilli(Long.MAX_VALUE)))
        );

        verifyKeyValueList(
            asList(
                windowedPair("a", "0005", SEGMENT_INTERVAL),
                windowedPair("aa", "0004", 1),
                windowedPair("aa", "0002", 0),
                windowedPair("a", "0003", 1),
                windowedPair("a", "0001", 0)
            ),
            toList(cachingStore.backwardFetch(bytesKey("a"), bytesKey("aa"), ofEpochMilli(0), ofEpochMilli(Long.MAX_VALUE)))
        );
    }

    @Test
    public void shouldReturnSameResultsForSingleKeyFetchAndEqualKeyRangeFetch() {
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

    @Test
    public void shouldReturnSameResultsForSingleKeyFetchAndEqualKeyRangeBackwardFetch() {
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

    @Test
    public void shouldThrowNullPointerExceptionOnPutNullKey() {
        assertThrows(NullPointerException.class, () -> cachingStore.put(null, bytesValue("anyValue"), 0L));
    }

    @Test
    public void shouldNotThrowNullPointerExceptionOnPutNullValue() {
        cachingStore.put(bytesKey("a"), null, 0L);
    }

    @Test
    public void shouldThrowNullPointerExceptionOnFetchNullKey() {
        assertThrows(NullPointerException.class, () -> cachingStore.fetch(null, ofEpochMilli(1L), ofEpochMilli(2L)));
    }

    @Test
    public void shouldNotThrowInvalidRangeExceptionWithNegativeFromKey() {
        final Bytes keyFrom = Bytes.wrap(Serdes.Integer().serializer().serialize("", -1));
        final Bytes keyTo = Bytes.wrap(Serdes.Integer().serializer().serialize("", 1));

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(CachingWindowStore.class);
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

    @Test
    public void shouldNotThrowInvalidBackwardRangeExceptionWithNegativeFromKey() {
        final Bytes keyFrom = Bytes.wrap(Serdes.Integer().serializer().serialize("", -1));
        final Bytes keyTo = Bytes.wrap(Serdes.Integer().serializer().serialize("", 1));

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(CachingWindowStore.class);
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

    @Test
    public void shouldCloseCacheAndWrappedStoreAfterErrorDuringCacheFlush() {
        setUpCloseTests();
        EasyMock.reset(cache);
        cache.flush(CACHE_NAMESPACE);
        EasyMock.expectLastCall().andThrow(new RuntimeException("Simulating an error on flush"));
        cache.close(CACHE_NAMESPACE);
        EasyMock.replay(cache);
        EasyMock.reset(underlyingStore);
        underlyingStore.close();
        EasyMock.replay(underlyingStore);

        assertThrows(RuntimeException.class, cachingStore::close);
        EasyMock.verify(cache, underlyingStore);
    }

    @Test
    public void shouldCloseWrappedStoreAfterErrorDuringCacheClose() {
        setUpCloseTests();
        EasyMock.reset(cache);
        cache.flush(CACHE_NAMESPACE);
        cache.close(CACHE_NAMESPACE);
        EasyMock.expectLastCall().andThrow(new RuntimeException("Simulating an error on close"));
        EasyMock.replay(cache);
        EasyMock.reset(underlyingStore);
        underlyingStore.close();
        EasyMock.replay(underlyingStore);

        assertThrows(RuntimeException.class, cachingStore::close);
        EasyMock.verify(cache, underlyingStore);
    }

    @Test
    public void shouldCloseCacheAfterErrorDuringStateStoreClose() {
        setUpCloseTests();
        EasyMock.reset(cache);
        cache.flush(CACHE_NAMESPACE);
        cache.close(CACHE_NAMESPACE);
        EasyMock.replay(cache);
        EasyMock.reset(underlyingStore);
        underlyingStore.close();
        EasyMock.expectLastCall().andThrow(new RuntimeException("Simulating an error on close"));
        EasyMock.replay(underlyingStore);

        assertThrows(RuntimeException.class, cachingStore::close);
        EasyMock.verify(cache, underlyingStore);
    }

    private void setUpCloseTests() {
        underlyingStore = EasyMock.createNiceMock(WindowStore.class);
        EasyMock.expect(underlyingStore.name()).andStubReturn("store-name");
        EasyMock.expect(underlyingStore.isOpen()).andStubReturn(true);
        EasyMock.replay(underlyingStore);
        cachingStore = new CachingWindowStore(underlyingStore, WINDOW_SIZE, SEGMENT_INTERVAL);
        cache = EasyMock.createNiceMock(ThreadCache.class);
        context = new InternalMockProcessorContext<>(TestUtils.tempDirectory(), null, null, null, cache);
        context.setRecordContext(new ProcessorRecordContext(10, 0, 0, TOPIC, new RecordHeaders()));
        cachingStore.init((StateStoreContext) context, cachingStore);
    }

    private static KeyValue<Windowed<Bytes>, byte[]> windowedPair(final String key, final String value, final long timestamp) {
        return KeyValue.pair(
            new Windowed<>(bytesKey(key), new TimeWindow(timestamp, timestamp + WINDOW_SIZE)),
            bytesValue(value));
    }

    private int addItemsToCache() {
        int cachedSize = 0;
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

}
