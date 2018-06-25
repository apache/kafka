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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.apache.kafka.common.utils.Utils.mkList;
import static org.apache.kafka.streams.state.internals.ThreadCacheTest.memoryCacheEntrySize;
import static org.apache.kafka.test.StreamsTestUtils.toList;
import static org.apache.kafka.test.StreamsTestUtils.verifyKeyValueList;
import static org.apache.kafka.test.StreamsTestUtils.verifyWindowedKeyValue;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;


public class CachingWindowStoreTest {

    private static final int MAX_CACHE_SIZE_BYTES = 150;
    private static final long DEFAULT_TIMESTAMP = 10L;
    private static final Long WINDOW_SIZE = 10000L;
    private InternalMockProcessorContext context;
    private RocksDBSegmentedBytesStore underlying;
    private CachingWindowStore<String, String> cachingStore;
    private CachingKeyValueStoreTest.CacheFlushListenerStub<Windowed<String>, String> cacheListener;
    private ThreadCache cache;
    private String topic;
    private WindowKeySchema keySchema;

    @Before
    public void setUp() {
        keySchema = new WindowKeySchema();
        final int retention = 60_000;
        final int segmentInterval = 60_000;
        underlying = new RocksDBSegmentedBytesStore("test", retention, segmentInterval, keySchema);
        final RocksDBWindowStore<Bytes, byte[]> windowStore = new RocksDBWindowStore<>(underlying, Serdes.Bytes(), Serdes.ByteArray(), false, WINDOW_SIZE);
        cacheListener = new CachingKeyValueStoreTest.CacheFlushListenerStub<>();
        cachingStore = new CachingWindowStore<>(windowStore,
                                                Serdes.String(),
                                                Serdes.String(),
                                                WINDOW_SIZE,
                                                segmentInterval);
        cachingStore.setFlushListener(cacheListener, false);
        cache = new ThreadCache(new LogContext("testCache "), MAX_CACHE_SIZE_BYTES, new MockStreamsMetrics(new Metrics()));
        topic = "topic";
        context = new InternalMockProcessorContext(TestUtils.tempDirectory(), null, null, (RecordCollector) null, cache);
        context.setRecordContext(new ProcessorRecordContext(DEFAULT_TIMESTAMP, 0, 0, topic, null));
        cachingStore.init(context, cachingStore);
    }

    @After
    public void closeStore() {
        cachingStore.close();
    }

    @Test
    public void shouldPutFetchFromCache() {
        cachingStore.put(bytesKey("a"), bytesValue("a"));
        cachingStore.put(bytesKey("b"), bytesValue("b"));

        assertThat(cachingStore.fetch(bytesKey("a"), 10), equalTo(bytesValue("a")));
        assertThat(cachingStore.fetch(bytesKey("b"), 10), equalTo(bytesValue("b")));
        assertThat(cachingStore.fetch(bytesKey("c"), 10), equalTo(null));
        assertThat(cachingStore.fetch(bytesKey("a"), 0), equalTo(null));

        final WindowStoreIterator<byte[]> a = cachingStore.fetch(bytesKey("a"), 10, 10);
        final WindowStoreIterator<byte[]> b = cachingStore.fetch(bytesKey("b"), 10, 10);
        verifyKeyValue(a.next(), DEFAULT_TIMESTAMP, "a");
        verifyKeyValue(b.next(), DEFAULT_TIMESTAMP, "b");
        assertFalse(a.hasNext());
        assertFalse(b.hasNext());
        assertEquals(2, cache.size());
    }

    private void verifyKeyValue(final KeyValue<Long, byte[]> next, final long expectedKey, final String expectedValue) {
        assertThat(next.key, equalTo(expectedKey));
        assertThat(next.value, equalTo(bytesValue(expectedValue)));
    }

    private static byte[] bytesValue(final String value) {
        return value.getBytes();
    }

    private static Bytes bytesKey(final String key) {
        return Bytes.wrap(key.getBytes());
    }

    @Test
    public void shouldPutFetchRangeFromCache() {
        cachingStore.put(bytesKey("a"), bytesValue("a"));
        cachingStore.put(bytesKey("b"), bytesValue("b"));

        final KeyValueIterator<Windowed<Bytes>, byte[]> iterator = cachingStore.fetch(bytesKey("a"), bytesKey("b"), 10, 10);
        verifyWindowedKeyValue(iterator.next(), new Windowed<>(bytesKey("a"), new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE)), "a");
        verifyWindowedKeyValue(iterator.next(), new Windowed<>(bytesKey("b"), new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE)), "b");
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
        String[] array = {"a", "b", "c", "d", "e", "f", "g", "h"};
        for (String s : array) {
            verifyWindowedKeyValue(iterator.next(), new Windowed<>(bytesKey(s), new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE)), s);
        }
        assertFalse(iterator.hasNext());
    }
    
    @Test
    public void shouldFetchAllWithinTimestampRange() {
        String[] array = {"a", "b", "c", "d", "e", "f", "g", "h"};
        for (int i = 0; i < array.length; i++) {
            context.setTime(i);
            cachingStore.put(bytesKey(array[i]), bytesValue(array[i]));
        }
        
        final KeyValueIterator<Windowed<Bytes>, byte[]> iterator = cachingStore.fetchAll(0, 7);
        for (int i = 0; i < array.length; i++) {
            String str = array[i];
            verifyWindowedKeyValue(iterator.next(), new Windowed<>(bytesKey(str), new TimeWindow(i, i + WINDOW_SIZE)), str);
        }
        assertFalse(iterator.hasNext());
        
        final KeyValueIterator<Windowed<Bytes>, byte[]> iterator1 = cachingStore.fetchAll(2, 4);
        for (int i = 2; i <= 4; i++) {
            String str = array[i];
            verifyWindowedKeyValue(iterator1.next(), new Windowed<>(bytesKey(str), new TimeWindow(i, i + WINDOW_SIZE)), str);
        }
        assertFalse(iterator1.hasNext());
        
        final KeyValueIterator<Windowed<Bytes>, byte[]> iterator2 = cachingStore.fetchAll(5, 7);
        for (int i = 5; i <= 7; i++) {
            String str = array[i];
            verifyWindowedKeyValue(iterator2.next(), new Windowed<>(bytesKey(str), new TimeWindow(i, i + WINDOW_SIZE)), str);
        }
        assertFalse(iterator2.hasNext());
    }

    @Test
    public void shouldFlushEvictedItemsIntoUnderlyingStore() throws IOException {
        int added = addItemsToCache();
        // all dirty entries should have been flushed
        final KeyValueIterator<Bytes, byte[]> iter = underlying.fetch(Bytes.wrap("0".getBytes()), DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP);
        final KeyValue<Bytes, byte[]> next = iter.next();
        assertEquals(DEFAULT_TIMESTAMP, keySchema.segmentTimestamp(next.key));
        assertArrayEquals("0".getBytes(), next.value);
        assertFalse(iter.hasNext());
        assertEquals(added - 1, cache.size());
    }

    @Test
    public void shouldForwardDirtyItemsWhenFlushCalled() {
        final Windowed<String> windowedKey = new Windowed<>("1", new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE));
        cachingStore.put(bytesKey("1"), bytesValue("a"));
        cachingStore.flush();
        assertEquals("a", cacheListener.forwarded.get(windowedKey).newValue);
        assertNull(cacheListener.forwarded.get(windowedKey).oldValue);
    }

    @Test
    public void shouldForwardOldValuesWhenEnabled() {
        cachingStore.setFlushListener(cacheListener, true);
        final Windowed<String> windowedKey = new Windowed<>("1", new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE));
        cachingStore.put(bytesKey("1"), bytesValue("a"));
        cachingStore.flush();
        cachingStore.put(bytesKey("1"), bytesValue("b"));
        cachingStore.flush();
        assertEquals("b", cacheListener.forwarded.get(windowedKey).newValue);
        assertEquals("a", cacheListener.forwarded.get(windowedKey).oldValue);
    }

    @Test
    public void shouldForwardOldValuesWhenDisabled() {
        final Windowed<String> windowedKey = new Windowed<>("1", new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE));
        cachingStore.put(bytesKey("1"), bytesValue("a"));
        cachingStore.flush();
        cachingStore.put(bytesKey("1"), bytesValue("b"));
        cachingStore.flush();
        assertEquals("b", cacheListener.forwarded.get(windowedKey).newValue);
        assertNull(cacheListener.forwarded.get(windowedKey).oldValue);
    }

    @Test
    public void shouldForwardDirtyItemToListenerWhenEvicted() throws IOException {
        int numRecords = addItemsToCache();
        assertEquals(numRecords, cacheListener.forwarded.size());
    }

    @Test
    public void shouldTakeValueFromCacheIfSameTimestampFlushedToRocks() {
        cachingStore.put(bytesKey("1"), bytesValue("a"), DEFAULT_TIMESTAMP);
        cachingStore.flush();
        cachingStore.put(bytesKey("1"), bytesValue("b"), DEFAULT_TIMESTAMP);

        final WindowStoreIterator<byte[]> fetch = cachingStore.fetch(bytesKey("1"), DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP);
        verifyKeyValue(fetch.next(), DEFAULT_TIMESTAMP, "b");
        assertFalse(fetch.hasNext());
    }

    @Test
    public void shouldIterateAcrossWindows() {
        cachingStore.put(bytesKey("1"), bytesValue("a"), DEFAULT_TIMESTAMP);
        cachingStore.put(bytesKey("1"), bytesValue("b"), DEFAULT_TIMESTAMP + WINDOW_SIZE);

        final WindowStoreIterator<byte[]> fetch = cachingStore.fetch(bytesKey("1"), DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE);
        verifyKeyValue(fetch.next(), DEFAULT_TIMESTAMP, "a");
        verifyKeyValue(fetch.next(), DEFAULT_TIMESTAMP + WINDOW_SIZE, "b");
        assertFalse(fetch.hasNext());
    }

    @Test
    public void shouldIterateCacheAndStore() {
        final Bytes key = Bytes.wrap("1" .getBytes());
        underlying.put(WindowKeySchema.toStoreKeyBinary(key, DEFAULT_TIMESTAMP, 0), "a".getBytes());
        cachingStore.put(key, bytesValue("b"), DEFAULT_TIMESTAMP + WINDOW_SIZE);
        final WindowStoreIterator<byte[]> fetch = cachingStore.fetch(bytesKey("1"), DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE);
        verifyKeyValue(fetch.next(), DEFAULT_TIMESTAMP, "a");
        verifyKeyValue(fetch.next(), DEFAULT_TIMESTAMP + WINDOW_SIZE, "b");
        assertFalse(fetch.hasNext());
    }

    @Test
    public void shouldIterateCacheAndStoreKeyRange() {
        final Bytes key = Bytes.wrap("1" .getBytes());
        underlying.put(WindowKeySchema.toStoreKeyBinary(key, DEFAULT_TIMESTAMP, 0), "a".getBytes());
        cachingStore.put(key, bytesValue("b"), DEFAULT_TIMESTAMP + WINDOW_SIZE);

        final KeyValueIterator<Windowed<Bytes>, byte[]> fetchRange =
            cachingStore.fetch(key, bytesKey("2"), DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE);
        verifyWindowedKeyValue(fetchRange.next(), new Windowed<>(key, new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE)), "a");
        verifyWindowedKeyValue(fetchRange.next(), new Windowed<>(key, new TimeWindow(DEFAULT_TIMESTAMP + WINDOW_SIZE, DEFAULT_TIMESTAMP + WINDOW_SIZE + WINDOW_SIZE)), "b");
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
        cachingStore.fetch(bytesKey("a"), 0, 10);
    }

    @Test(expected = InvalidStateStoreException.class)
    public void shouldThrowIfTryingToFetchRangeFromClosedCachingStore() {
        cachingStore.close();
        cachingStore.fetch(bytesKey("a"), bytesKey("b"), 0, 10);
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
        cachingStore.put(bytesKey("a"), bytesValue("0005"), 60000);

        final List<KeyValue<Long, byte[]>> expected = mkList(KeyValue.pair(0L, bytesValue("0001")), KeyValue.pair(1L, bytesValue("0003")), KeyValue.pair(60000L, bytesValue("0005")));
        final List<KeyValue<Long, byte[]>> actual = toList(cachingStore.fetch(bytesKey("a"), 0, Long.MAX_VALUE));
        verifyKeyValueList(expected, actual);
    }

    @Test
    public void shouldFetchAndIterateOverKeyRange() {
        cachingStore.put(bytesKey("a"), bytesValue("0001"), 0);
        cachingStore.put(bytesKey("aa"), bytesValue("0002"), 0);
        cachingStore.put(bytesKey("a"), bytesValue("0003"), 1);
        cachingStore.put(bytesKey("aa"), bytesValue("0004"), 1);
        cachingStore.put(bytesKey("a"), bytesValue("0005"), 60000);

        verifyKeyValueList(mkList(windowedPair("a", "0001", 0), windowedPair("a", "0003", 1), windowedPair("a", "0005", 60000L)),
                           toList(cachingStore.fetch(bytesKey("a"), bytesKey("a"), 0, Long.MAX_VALUE)));

        verifyKeyValueList(mkList(windowedPair("aa", "0002", 0), windowedPair("aa", "0004", 1)),
                           toList(cachingStore.fetch(bytesKey("aa"), bytesKey("aa"), 0, Long.MAX_VALUE)));

        verifyKeyValueList(mkList(windowedPair("a", "0001", 0), windowedPair("a", "0003", 1), windowedPair("aa", "0002", 0), windowedPair("aa", "0004", 1), windowedPair("a", "0005", 60000L)),
                           toList(cachingStore.fetch(bytesKey("a"), bytesKey("aa"), 0, Long.MAX_VALUE)));
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
        cachingStore.fetch(null, 1L, 2L);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnRangeNullFromKey() {
        cachingStore.fetch(null, bytesKey("anyTo"), 1L, 2L);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnRangeNullToKey() {
        cachingStore.fetch(bytesKey("anyFrom"), null, 1L, 2L);
    }

    private static KeyValue<Windowed<Bytes>, byte[]> windowedPair(String key, String value, long timestamp) {
        return KeyValue.pair(new Windowed<>(bytesKey(key), new TimeWindow(timestamp, timestamp + WINDOW_SIZE)), bytesValue(value));
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
