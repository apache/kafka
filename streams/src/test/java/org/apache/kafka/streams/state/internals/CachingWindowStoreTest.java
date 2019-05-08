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
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.testutil.LogCaptureAppender;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static java.time.Duration.ofHours;
import static java.time.Duration.ofMinutes;
import static java.time.Instant.ofEpochMilli;
import static java.util.Arrays.asList;
import static org.apache.kafka.streams.state.internals.ThreadCacheTest.memoryCacheEntrySize;
import static org.apache.kafka.test.StreamsTestUtils.toList;
import static org.apache.kafka.test.StreamsTestUtils.verifyKeyValueList;
import static org.apache.kafka.test.StreamsTestUtils.verifyWindowedKeyValue;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CachingWindowStoreTest {

    private static final int MAX_CACHE_SIZE_BYTES = 150;
    private static final long DEFAULT_TIMESTAMP = 10L;
    private static final Long WINDOW_SIZE = 10L;
    private static final long SEGMENT_INTERVAL = 100L;
    private InternalMockProcessorContext context;
    private RocksDBSegmentedBytesStore underlying;
    private CachingWindowStore cachingStore;
    private CachingKeyValueStoreTest.CacheFlushListenerStub<Windowed<String>, String> cacheListener;
    private ThreadCache cache;
    private String topic;
    private WindowKeySchema keySchema;

    @Before
    public void setUp() {
        keySchema = new WindowKeySchema();
        underlying = new RocksDBSegmentedBytesStore("test", "metrics-scope", 0, SEGMENT_INTERVAL, keySchema);
        final RocksDBWindowStore windowStore = new RocksDBWindowStore(
            underlying,
            false,
            WINDOW_SIZE);
        final TimeWindowedDeserializer<String> keyDeserializer = new TimeWindowedDeserializer<>(new StringDeserializer(), WINDOW_SIZE);
        keyDeserializer.setIsChangelogTopic(true);
        cacheListener = new CachingKeyValueStoreTest.CacheFlushListenerStub<>(keyDeserializer, new StringDeserializer());
        cachingStore = new CachingWindowStore(windowStore, WINDOW_SIZE, SEGMENT_INTERVAL);
        cachingStore.setFlushListener(cacheListener, false);
        cache = new ThreadCache(new LogContext("testCache "), MAX_CACHE_SIZE_BYTES, new MockStreamsMetrics(new Metrics()));
        topic = "topic";
        context = new InternalMockProcessorContext(TestUtils.tempDirectory(), null, null, null, cache);
        context.setRecordContext(new ProcessorRecordContext(DEFAULT_TIMESTAMP, 0, 0, topic, null));
        cachingStore.init(context, cachingStore);
    }

    @After
    public void closeStore() {
        cachingStore.close();
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

        builder.stream(topic,
            Consumed.with(Serdes.String(), Serdes.String()))
            .transform(() -> new Transformer<String, String, KeyValue<String, String>>() {
                private WindowStore<String, String> store;
                private int numRecordsProcessed;

                @SuppressWarnings("unchecked")
                @Override
                public void init(final ProcessorContext processorContext) {
                    this.store = (WindowStore<String, String>) processorContext.getStateStore("store-name");
                    int count = 0;

                    final KeyValueIterator<Windowed<String>, String> all = store.all();
                    while (all.hasNext()) {
                        count++;
                        all.next();
                    }

                    assertThat(count, equalTo(0));
                }

                @Override
                public KeyValue<String, String> transform(final String key, final String value) {
                    int count = 0;

                    final KeyValueIterator<Windowed<String>, String> all = store.all();
                    while (all.hasNext()) {
                        count++;
                        all.next();
                    }
                    assertThat(count, equalTo(numRecordsProcessed));

                    store.put(value, value);

                    numRecordsProcessed++;

                    return new KeyValue<>(key, value);
                }

                @Override
                public void close() {}
            }, "store-name");

        final String bootstrapServers = "localhost:9092";
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);

        final long initialWallClockTime = 0L;
        final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), streamsConfiguration, initialWallClockTime);

        final ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(
            Serdes.String().serializer(),
            Serdes.String().serializer(),
            initialWallClockTime);

        for (int i = 0; i < 5; i++) {
            driver.pipeInput(recordFactory.create(topic, UUID.randomUUID().toString(), UUID.randomUUID().toString()));
        }
        driver.advanceWallClockTime(10 * 1000L);
        recordFactory.advanceTimeMs(10 * 1000L);
        for (int i = 0; i < 5; i++) {
            driver.pipeInput(recordFactory.create(topic, UUID.randomUUID().toString(), UUID.randomUUID().toString()));
        }
        driver.advanceWallClockTime(10 * 1000L);
        recordFactory.advanceTimeMs(10 * 1000L);
        for (int i = 0; i < 5; i++) {
            driver.pipeInput(recordFactory.create(topic, UUID.randomUUID().toString(), UUID.randomUUID().toString()));
        }
        driver.advanceWallClockTime(10 * 1000L);
        recordFactory.advanceTimeMs(10 * 1000L);
        for (int i = 0; i < 5; i++) {
            driver.pipeInput(recordFactory.create(topic, UUID.randomUUID().toString(), UUID.randomUUID().toString()));
        }
    }

    @Test
    public void shouldPutFetchFromCache() {
        cachingStore.put(bytesKey("a"), bytesValue("a"));
        cachingStore.put(bytesKey("b"), bytesValue("b"));

        assertThat(cachingStore.fetch(bytesKey("a"), 10), equalTo(bytesValue("a")));
        assertThat(cachingStore.fetch(bytesKey("b"), 10), equalTo(bytesValue("b")));
        assertThat(cachingStore.fetch(bytesKey("c"), 10), equalTo(null));
        assertThat(cachingStore.fetch(bytesKey("a"), 0), equalTo(null));

        final WindowStoreIterator<byte[]> a = cachingStore.fetch(bytesKey("a"), ofEpochMilli(10), ofEpochMilli(10));
        final WindowStoreIterator<byte[]> b = cachingStore.fetch(bytesKey("b"), ofEpochMilli(10), ofEpochMilli(10));
        verifyKeyValue(a.next(), DEFAULT_TIMESTAMP, "a");
        verifyKeyValue(b.next(), DEFAULT_TIMESTAMP, "b");
        assertFalse(a.hasNext());
        assertFalse(b.hasNext());
        assertEquals(2, cache.size());
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
        cachingStore.put(bytesKey("a"), bytesValue("a"));
        cachingStore.put(bytesKey("b"), bytesValue("b"));

        final KeyValueIterator<Windowed<Bytes>, byte[]> iterator =
            cachingStore.fetch(bytesKey("a"), bytesKey("b"), ofEpochMilli(10), ofEpochMilli(10));
        verifyWindowedKeyValue(
            iterator.next(),
            new Windowed<>(bytesKey("a"), new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE)),
            "a");
        verifyWindowedKeyValue(
            iterator.next(),
            new Windowed<>(bytesKey("b"), new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE)),
            "b");
        assertFalse(iterator.hasNext());
        assertEquals(2, cache.size());
    }

    @Test
    public void shouldGetAllFromCache() {
        cachingStore.put(bytesKey("a"), bytesValue("a"));
        cachingStore.put(bytesKey("b"), bytesValue("b"));
        cachingStore.put(bytesKey("c"), bytesValue("c"));
        cachingStore.put(bytesKey("d"), bytesValue("d"));
        cachingStore.put(bytesKey("e"), bytesValue("e"));
        cachingStore.put(bytesKey("f"), bytesValue("f"));
        cachingStore.put(bytesKey("g"), bytesValue("g"));
        cachingStore.put(bytesKey("h"), bytesValue("h"));

        final KeyValueIterator<Windowed<Bytes>, byte[]> iterator = cachingStore.all();
        final String[] array = {"a", "b", "c", "d", "e", "f", "g", "h"};
        for (final String s : array) {
            verifyWindowedKeyValue(
                iterator.next(),
                new Windowed<>(bytesKey(s), new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE)),
                s);
        }
        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldFetchAllWithinTimestampRange() {
        final String[] array = {"a", "b", "c", "d", "e", "f", "g", "h"};
        for (int i = 0; i < array.length; i++) {
            context.setTime(i);
            cachingStore.put(bytesKey(array[i]), bytesValue(array[i]));
        }

        final KeyValueIterator<Windowed<Bytes>, byte[]> iterator =
            cachingStore.fetchAll(ofEpochMilli(0), ofEpochMilli(7));
        for (int i = 0; i < array.length; i++) {
            final String str = array[i];
            verifyWindowedKeyValue(
                iterator.next(),
                new Windowed<>(bytesKey(str), new TimeWindow(i, i + WINDOW_SIZE)),
                str);
        }
        assertFalse(iterator.hasNext());

        final KeyValueIterator<Windowed<Bytes>, byte[]> iterator1 =
            cachingStore.fetchAll(ofEpochMilli(2), ofEpochMilli(4));
        for (int i = 2; i <= 4; i++) {
            final String str = array[i];
            verifyWindowedKeyValue(
                iterator1.next(),
                new Windowed<>(bytesKey(str), new TimeWindow(i, i + WINDOW_SIZE)),
                str);
        }
        assertFalse(iterator1.hasNext());

        final KeyValueIterator<Windowed<Bytes>, byte[]> iterator2 =
            cachingStore.fetchAll(ofEpochMilli(5), ofEpochMilli(7));
        for (int i = 5; i <= 7; i++) {
            final String str = array[i];
            verifyWindowedKeyValue(
                iterator2.next(),
                new Windowed<>(bytesKey(str), new TimeWindow(i, i + WINDOW_SIZE)),
                str);
        }
        assertFalse(iterator2.hasNext());
    }

    @Test
    public void shouldFlushEvictedItemsIntoUnderlyingStore() {
        final int added = addItemsToCache();
        // all dirty entries should have been flushed
        final KeyValueIterator<Bytes, byte[]> iter = underlying.fetch(
            Bytes.wrap("0".getBytes(StandardCharsets.UTF_8)),
            DEFAULT_TIMESTAMP,
            DEFAULT_TIMESTAMP);
        final KeyValue<Bytes, byte[]> next = iter.next();
        assertEquals(DEFAULT_TIMESTAMP, keySchema.segmentTimestamp(next.key));
        assertArrayEquals("0".getBytes(), next.value);
        assertFalse(iter.hasNext());
        assertEquals(added - 1, cache.size());
    }

    @Test
    public void shouldForwardDirtyItemsWhenFlushCalled() {
        final Windowed<String> windowedKey =
            new Windowed<>("1", new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE));
        cachingStore.put(bytesKey("1"), bytesValue("a"));
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
        cachingStore.put(bytesKey("1"), bytesValue("a"));
        cachingStore.put(bytesKey("1"), bytesValue("b"));
        cachingStore.flush();
        assertEquals("b", cacheListener.forwarded.get(windowedKey).newValue);
        assertNull(cacheListener.forwarded.get(windowedKey).oldValue);
        cacheListener.forwarded.clear();
        cachingStore.put(bytesKey("1"), bytesValue("c"));
        cachingStore.flush();
        assertEquals("c", cacheListener.forwarded.get(windowedKey).newValue);
        assertEquals("b", cacheListener.forwarded.get(windowedKey).oldValue);
        cachingStore.put(bytesKey("1"), null);
        cachingStore.flush();
        assertNull(cacheListener.forwarded.get(windowedKey).newValue);
        assertEquals("c", cacheListener.forwarded.get(windowedKey).oldValue);
        cacheListener.forwarded.clear();
        cachingStore.put(bytesKey("1"), bytesValue("a"));
        cachingStore.put(bytesKey("1"), bytesValue("b"));
        cachingStore.put(bytesKey("1"), null);
        cachingStore.flush();
        assertNull(cacheListener.forwarded.get(windowedKey));
        cacheListener.forwarded.clear();
    }

    @Test
    public void shouldForwardOldValuesWhenDisabled() {
        final Windowed<String> windowedKey =
            new Windowed<>("1", new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE));
        cachingStore.put(bytesKey("1"), bytesValue("a"));
        cachingStore.put(bytesKey("1"), bytesValue("b"));
        cachingStore.flush();
        assertEquals("b", cacheListener.forwarded.get(windowedKey).newValue);
        assertNull(cacheListener.forwarded.get(windowedKey).oldValue);
        cachingStore.put(bytesKey("1"), bytesValue("c"));
        cachingStore.flush();
        assertEquals("c", cacheListener.forwarded.get(windowedKey).newValue);
        assertNull(cacheListener.forwarded.get(windowedKey).oldValue);
        cachingStore.put(bytesKey("1"), null);
        cachingStore.flush();
        assertNull(cacheListener.forwarded.get(windowedKey).newValue);
        assertNull(cacheListener.forwarded.get(windowedKey).oldValue);
        cacheListener.forwarded.clear();
        cachingStore.put(bytesKey("1"), bytesValue("a"));
        cachingStore.put(bytesKey("1"), bytesValue("b"));
        cachingStore.put(bytesKey("1"), null);
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

        final WindowStoreIterator<byte[]> fetch =
            cachingStore.fetch(bytesKey("1"), ofEpochMilli(DEFAULT_TIMESTAMP), ofEpochMilli(DEFAULT_TIMESTAMP));
        verifyKeyValue(fetch.next(), DEFAULT_TIMESTAMP, "b");
        assertFalse(fetch.hasNext());
    }

    @Test
    public void shouldIterateAcrossWindows() {
        cachingStore.put(bytesKey("1"), bytesValue("a"), DEFAULT_TIMESTAMP);
        cachingStore.put(bytesKey("1"), bytesValue("b"), DEFAULT_TIMESTAMP + WINDOW_SIZE);

        final WindowStoreIterator<byte[]> fetch =
            cachingStore.fetch(bytesKey("1"), ofEpochMilli(DEFAULT_TIMESTAMP), ofEpochMilli(DEFAULT_TIMESTAMP + WINDOW_SIZE));
        verifyKeyValue(fetch.next(), DEFAULT_TIMESTAMP, "a");
        verifyKeyValue(fetch.next(), DEFAULT_TIMESTAMP + WINDOW_SIZE, "b");
        assertFalse(fetch.hasNext());
    }

    @Test
    public void shouldIterateCacheAndStore() {
        final Bytes key = Bytes.wrap("1".getBytes());
        underlying.put(WindowKeySchema.toStoreKeyBinary(key, DEFAULT_TIMESTAMP, 0), "a".getBytes());
        cachingStore.put(key, bytesValue("b"), DEFAULT_TIMESTAMP + WINDOW_SIZE);
        final WindowStoreIterator<byte[]> fetch =
            cachingStore.fetch(bytesKey("1"), ofEpochMilli(DEFAULT_TIMESTAMP), ofEpochMilli(DEFAULT_TIMESTAMP + WINDOW_SIZE));
        verifyKeyValue(fetch.next(), DEFAULT_TIMESTAMP, "a");
        verifyKeyValue(fetch.next(), DEFAULT_TIMESTAMP + WINDOW_SIZE, "b");
        assertFalse(fetch.hasNext());
    }

    @Test
    public void shouldIterateCacheAndStoreKeyRange() {
        final Bytes key = Bytes.wrap("1".getBytes());
        underlying.put(WindowKeySchema.toStoreKeyBinary(key, DEFAULT_TIMESTAMP, 0), "a".getBytes());
        cachingStore.put(key, bytesValue("b"), DEFAULT_TIMESTAMP + WINDOW_SIZE);

        final KeyValueIterator<Windowed<Bytes>, byte[]> fetchRange =
            cachingStore.fetch(key, bytesKey("2"), ofEpochMilli(DEFAULT_TIMESTAMP), ofEpochMilli(DEFAULT_TIMESTAMP + WINDOW_SIZE));
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

    @Test
    public void shouldClearNamespaceCacheOnClose() {
        cachingStore.put(bytesKey("a"), bytesValue("a"));
        assertEquals(1, cache.size());
        cachingStore.close();
        assertEquals(0, cache.size());
    }

    @Test(expected = InvalidStateStoreException.class)
    public void shouldThrowIfTryingToFetchFromClosedCachingStore() {
        cachingStore.close();
        cachingStore.fetch(bytesKey("a"), ofEpochMilli(0), ofEpochMilli(10));
    }

    @Test(expected = InvalidStateStoreException.class)
    public void shouldThrowIfTryingToFetchRangeFromClosedCachingStore() {
        cachingStore.close();
        cachingStore.fetch(bytesKey("a"), bytesKey("b"), ofEpochMilli(0), ofEpochMilli(10));
    }

    @Test(expected = InvalidStateStoreException.class)
    public void shouldThrowIfTryingToWriteToClosedCachingStore() {
        cachingStore.close();
        cachingStore.put(bytesKey("a"), bytesValue("a"));
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
    public void shouldReturnSameResultsForSingleKeyFetchAndEqualKeyRangeFetch() {
        cachingStore.put(bytesKey("a"), bytesValue("0001"), 0);
        cachingStore.put(bytesKey("aa"), bytesValue("0002"), 1);
        cachingStore.put(bytesKey("aa"), bytesValue("0003"), 2);
        cachingStore.put(bytesKey("aaa"), bytesValue("0004"), 3);

        final WindowStoreIterator<byte[]> singleKeyIterator = cachingStore.fetch(bytesKey("aa"), 0L, 5L);
        final KeyValueIterator<Windowed<Bytes>, byte[]> keyRangeIterator = cachingStore.fetch(bytesKey("aa"), bytesKey("aa"), 0L, 5L);

        assertEquals(stringFrom(singleKeyIterator.next().value), stringFrom(keyRangeIterator.next().value));
        assertEquals(stringFrom(singleKeyIterator.next().value), stringFrom(keyRangeIterator.next().value));
        assertFalse(singleKeyIterator.hasNext());
        assertFalse(keyRangeIterator.hasNext());
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnPutNullKey() {
        cachingStore.put(null, bytesValue("anyValue"));
    }

    @Test
    public void shouldNotThrowNullPointerExceptionOnPutNullValue() {
        cachingStore.put(bytesKey("a"), null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnFetchNullKey() {
        cachingStore.fetch(null, ofEpochMilli(1L), ofEpochMilli(2L));
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnRangeNullFromKey() {
        cachingStore.fetch(null, bytesKey("anyTo"), ofEpochMilli(1L), ofEpochMilli(2L));
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnRangeNullToKey() {
        cachingStore.fetch(bytesKey("anyFrom"), null, ofEpochMilli(1L), ofEpochMilli(2L));
    }

    @Test
    public void shouldNotThrowInvalidRangeExceptionWithNegativeFromKey() {
        LogCaptureAppender.setClassLoggerToDebug(InMemoryWindowStore.class);
        final LogCaptureAppender appender = LogCaptureAppender.createAndRegister();

        final Bytes keyFrom = Bytes.wrap(Serdes.Integer().serializer().serialize("", -1));
        final Bytes keyTo = Bytes.wrap(Serdes.Integer().serializer().serialize("", 1));

        final KeyValueIterator<Windowed<Bytes>, byte[]> iterator = cachingStore.fetch(keyFrom, keyTo, 0L, 10L);
        assertFalse(iterator.hasNext());

        final List<String> messages = appender.getMessages();
        assertThat(messages, hasItem("Returning empty iterator for fetch with invalid key range: from > to. "
            + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. "
            + "Note that the built-in numerical serdes do not follow this for negative numbers"));
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
            cachingStore.put(bytesKey(kv), bytesValue(kv));
            cachedSize += memoryCacheEntrySize(kv.getBytes(), kv.getBytes(), topic) +
                8 + // timestamp
                4; // sequenceNumber
        }
        return i;
    }

}
