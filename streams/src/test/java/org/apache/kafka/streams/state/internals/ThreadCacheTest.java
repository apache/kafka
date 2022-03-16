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


import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class ThreadCacheTest {
    final String namespace = "0.0-namespace";
    final String namespace1 = "0.1-namespace";
    final String namespace2 = "0.2-namespace";
    private final LogContext logContext = new LogContext("testCache ");
    private final byte[][] bytes = new byte[][]{{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}};

    @Test
    public void basicPutGet() {
        final List<KeyValue<String, String>> toInsert = Arrays.asList(
            new KeyValue<>("K1", "V1"),
            new KeyValue<>("K2", "V2"),
            new KeyValue<>("K3", "V3"),
            new KeyValue<>("K4", "V4"),
            new KeyValue<>("K5", "V5"));
        final KeyValue<String, String> kv = toInsert.get(0);
        final ThreadCache cache = new ThreadCache(logContext,
            toInsert.size() * memoryCacheEntrySize(kv.key.getBytes(), kv.value.getBytes(), ""),
            new MockStreamsMetrics(new Metrics()));

        for (final KeyValue<String, String> kvToInsert : toInsert) {
            final Bytes key = Bytes.wrap(kvToInsert.key.getBytes());
            final byte[] value = kvToInsert.value.getBytes();
            cache.put(namespace, key, new LRUCacheEntry(value, new RecordHeaders(), true, 1L, 1L, 1, ""));
        }

        for (final KeyValue<String, String> kvToInsert : toInsert) {
            final Bytes key = Bytes.wrap(kvToInsert.key.getBytes());
            final LRUCacheEntry entry = cache.get(namespace, key);
            assertTrue(entry.isDirty());
            assertEquals(new String(entry.value()), kvToInsert.value);
        }
        assertEquals(cache.gets(), 5);
        assertEquals(cache.puts(), 5);
        assertEquals(cache.evicts(), 0);
        assertEquals(cache.flushes(), 0);
    }

    private void checkOverheads(final double entryFactor,
                                final double systemFactor,
                                final long desiredCacheSize,
                                final int keySizeBytes,
                                final int valueSizeBytes) {
        final Runtime runtime = Runtime.getRuntime();
        final long numElements = desiredCacheSize / memoryCacheEntrySize(new byte[keySizeBytes], new byte[valueSizeBytes], "");

        System.gc();
        final long prevRuntimeMemory = runtime.totalMemory() - runtime.freeMemory();

        final ThreadCache cache = new ThreadCache(logContext, desiredCacheSize, new MockStreamsMetrics(new Metrics()));
        final long size = cache.sizeBytes();
        assertEquals(size, 0);
        for (int i = 0; i < numElements; i++) {
            final String keyStr = "K" + i;
            final Bytes key = Bytes.wrap(keyStr.getBytes());
            final byte[] value = new byte[valueSizeBytes];
            cache.put(namespace, key, new LRUCacheEntry(value, new RecordHeaders(), true, 1L, 1L, 1, ""));
        }


        System.gc();
        final double ceiling = desiredCacheSize + desiredCacheSize * entryFactor;
        final long usedRuntimeMemory = runtime.totalMemory() - runtime.freeMemory() - prevRuntimeMemory;
        assertTrue((double) cache.sizeBytes() <= ceiling);

        assertTrue("Used memory size " + usedRuntimeMemory + " greater than expected " + cache.sizeBytes() * systemFactor,
            cache.sizeBytes() * systemFactor >= usedRuntimeMemory);
    }

    @Test
    public void cacheOverheadsSmallValues() {
        final Runtime runtime = Runtime.getRuntime();
        final double factor = 0.05;
        final double systemFactor = 3; // if I ask for a cache size of 10 MB, accept an overhead of 3x, i.e., 30 MBs might be allocated
        final long desiredCacheSize = Math.min(100 * 1024 * 1024L, runtime.maxMemory());
        final int keySizeBytes = 8;
        final int valueSizeBytes = 100;

        checkOverheads(factor, systemFactor, desiredCacheSize, keySizeBytes, valueSizeBytes);
    }

    @Test
    public void cacheOverheadsLargeValues() {
        final Runtime runtime = Runtime.getRuntime();
        final double factor = 0.05;
        final double systemFactor = 2; // if I ask for a cache size of 10 MB, accept an overhead of 2x, i.e., 20 MBs might be allocated
        final long desiredCacheSize = Math.min(100 * 1024 * 1024L, runtime.maxMemory());
        final int keySizeBytes = 8;
        final int valueSizeBytes = 1000;

        checkOverheads(factor, systemFactor, desiredCacheSize, keySizeBytes, valueSizeBytes);
    }


    static long memoryCacheEntrySize(final byte[] key, final byte[] value, final String topic) {
        return key.length +
            value.length +
            1 + // isDirty
            8 + // timestamp
            8 + // offset
            4 +
            topic.length() +
            // LRU Node entries
            key.length +
            8 + // entry
            8 + // previous
            8; // next
    }

    @Test
    public void evict() {
        final List<KeyValue<String, String>> received = new ArrayList<>();
        final List<KeyValue<String, String>> expected = Collections.singletonList(
            new KeyValue<>("K1", "V1"));

        final List<KeyValue<String, String>> toInsert = Arrays.asList(
            new KeyValue<>("K1", "V1"),
            new KeyValue<>("K2", "V2"),
            new KeyValue<>("K3", "V3"),
            new KeyValue<>("K4", "V4"),
            new KeyValue<>("K5", "V5"));
        final KeyValue<String, String> kv = toInsert.get(0);
        final ThreadCache cache = new ThreadCache(logContext,
            memoryCacheEntrySize(kv.key.getBytes(), kv.value.getBytes(), ""),
            new MockStreamsMetrics(new Metrics()));
        cache.addDirtyEntryFlushListener(namespace, dirty -> {
            for (final ThreadCache.DirtyEntry dirtyEntry : dirty) {
                received.add(new KeyValue<>(dirtyEntry.key().toString(), new String(dirtyEntry.newValue())));
            }
        });

        for (final KeyValue<String, String> kvToInsert : toInsert) {
            final Bytes key = Bytes.wrap(kvToInsert.key.getBytes());
            final byte[] value = kvToInsert.value.getBytes();
            cache.put(namespace, key, new LRUCacheEntry(value, new RecordHeaders(), true, 1, 1, 1, ""));
        }

        for (int i = 0; i < expected.size(); i++) {
            final KeyValue<String, String> expectedRecord = expected.get(i);
            final KeyValue<String, String> actualRecord = received.get(i);
            assertEquals(expectedRecord, actualRecord);
        }
        assertEquals(cache.evicts(), 4);
    }

    @Test
    public void shouldDelete() {
        final ThreadCache cache = new ThreadCache(logContext, 10000L, new MockStreamsMetrics(new Metrics()));
        final Bytes key = Bytes.wrap(new byte[]{0});

        cache.put(namespace, key, dirtyEntry(key.get()));
        assertEquals(key.get(), cache.delete(namespace, key).value());
        assertNull(cache.get(namespace, key));
    }

    @Test
    public void shouldNotFlushAfterDelete() {
        final Bytes key = Bytes.wrap(new byte[]{0});
        final ThreadCache cache = new ThreadCache(logContext, 10000L, new MockStreamsMetrics(new Metrics()));
        final List<ThreadCache.DirtyEntry> received = new ArrayList<>();
        cache.addDirtyEntryFlushListener(namespace, received::addAll);
        cache.put(namespace, key, dirtyEntry(key.get()));
        assertEquals(key.get(), cache.delete(namespace, key).value());

        // flushing should have no further effect
        cache.flush(namespace);
        assertEquals(0, received.size());
        assertEquals(cache.flushes(), 1);
    }

    @Test
    public void shouldNotBlowUpOnNonExistentKeyWhenDeleting() {
        final Bytes key = Bytes.wrap(new byte[]{0});
        final ThreadCache cache = new ThreadCache(logContext, 10000L, new MockStreamsMetrics(new Metrics()));

        cache.put(namespace, key, dirtyEntry(key.get()));
        assertNull(cache.delete(namespace, Bytes.wrap(new byte[]{1})));
    }

    @Test
    public void shouldNotBlowUpOnNonExistentNamespaceWhenDeleting() {
        final ThreadCache cache = new ThreadCache(logContext, 10000L, new MockStreamsMetrics(new Metrics()));
        assertNull(cache.delete(namespace, Bytes.wrap(new byte[]{1})));
    }

    @Test
    public void shouldNotClashWithOverlappingNames() {
        final ThreadCache cache = new ThreadCache(logContext, 10000L, new MockStreamsMetrics(new Metrics()));
        final Bytes nameByte = Bytes.wrap(new byte[]{0});
        final Bytes name1Byte = Bytes.wrap(new byte[]{1});
        cache.put(namespace1, nameByte, dirtyEntry(nameByte.get()));
        cache.put(namespace2, nameByte, dirtyEntry(name1Byte.get()));

        assertArrayEquals(nameByte.get(), cache.get(namespace1, nameByte).value());
        assertArrayEquals(name1Byte.get(), cache.get(namespace2, nameByte).value());
    }

    private ThreadCache setupThreadCache(final int first, final int last, final long entrySize, final boolean reverse) {
        final ThreadCache cache = new ThreadCache(logContext, entrySize, new MockStreamsMetrics(new Metrics()));
        cache.addDirtyEntryFlushListener(namespace, dirty -> { });
        int index = first;
        while ((!reverse && index < last) || (reverse && index >= last)) {
            cache.put(namespace, Bytes.wrap(bytes[index]), dirtyEntry(bytes[index]));
            if (!reverse)
                index++;
            else
                index--;
        }
        return cache;
    }

    @Test
    public void shouldPeekNextKey() {
        final ThreadCache cache = setupThreadCache(0, 1, 10000L, false);
        final Bytes theByte = Bytes.wrap(new byte[]{0});
        final ThreadCache.MemoryLRUCacheBytesIterator iterator = cache.range(namespace, theByte, Bytes.wrap(new byte[]{1}));
        assertEquals(theByte, iterator.peekNextKey());
        assertEquals(theByte, iterator.peekNextKey());
    }

    @Test
    public void shouldPeekNextKeyReverseRange() {
        final ThreadCache cache = setupThreadCache(1, 1, 10000L, true);
        final Bytes theByte = Bytes.wrap(new byte[]{1});
        final ThreadCache.MemoryLRUCacheBytesIterator iterator = cache.reverseRange(namespace, Bytes.wrap(new byte[]{0}), theByte);
        assertThat(iterator.peekNextKey(), is(theByte));
        assertThat(iterator.peekNextKey(), is(theByte));
    }

    @Test
    public void shouldGetSameKeyAsPeekNext() {
        final ThreadCache cache = setupThreadCache(0, 1, 10000L, false);
        final Bytes theByte = Bytes.wrap(new byte[]{0});
        final ThreadCache.MemoryLRUCacheBytesIterator iterator = cache.range(namespace, theByte, Bytes.wrap(new byte[]{1}));
        assertThat(iterator.peekNextKey(), is(iterator.next().key));
    }

    @Test
    public void shouldGetSameKeyAsPeekNextReverseRange() {
        final ThreadCache cache = setupThreadCache(1, 1, 10000L, true);
        final Bytes theByte = Bytes.wrap(new byte[]{1});
        final ThreadCache.MemoryLRUCacheBytesIterator iterator = cache.reverseRange(namespace, Bytes.wrap(new byte[]{0}), theByte);
        assertThat(iterator.peekNextKey(), is(iterator.next().key));
    }

    private void shouldThrowIfNoPeekNextKey(final Supplier<ThreadCache.MemoryLRUCacheBytesIterator> methodUnderTest) {
        final ThreadCache.MemoryLRUCacheBytesIterator iterator = methodUnderTest.get();
        assertThrows(NoSuchElementException.class, iterator::peekNextKey);
    }

    @Test
    public void shouldThrowIfNoPeekNextKeyRange() {
        final ThreadCache cache = setupThreadCache(0, 0, 10000L, false);
        shouldThrowIfNoPeekNextKey(() -> cache.range(namespace, Bytes.wrap(new byte[]{0}), Bytes.wrap(new byte[]{1})));
    }

    @Test
    public void shouldThrowIfNoPeekNextKeyReverseRange() {
        final ThreadCache cache = setupThreadCache(-1, 0, 10000L, true);
        shouldThrowIfNoPeekNextKey(() -> cache.reverseRange(namespace, Bytes.wrap(new byte[]{0}), Bytes.wrap(new byte[]{1})));
    }

    @Test
    public void shouldReturnFalseIfNoNextKey() {
        final ThreadCache cache = setupThreadCache(0, 0, 10000L, false);
        final ThreadCache.MemoryLRUCacheBytesIterator iterator = cache.range(namespace, Bytes.wrap(new byte[]{0}), Bytes.wrap(new byte[]{1}));
        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldReturnFalseIfNoNextKeyReverseRange() {
        final ThreadCache cache = setupThreadCache(-1, 0, 10000L, true);
        final ThreadCache.MemoryLRUCacheBytesIterator iterator = cache.reverseRange(namespace, Bytes.wrap(new byte[]{0}), Bytes.wrap(new byte[]{1}));
        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldPeekAndIterateOverRange() {
        final ThreadCache cache = setupThreadCache(0, 10, 10000L, false);
        final ThreadCache.MemoryLRUCacheBytesIterator iterator = cache.range(namespace, Bytes.wrap(new byte[]{1}), Bytes.wrap(new byte[]{4}));
        int bytesIndex = 1;
        while (iterator.hasNext()) {
            final Bytes peekedKey = iterator.peekNextKey();
            final KeyValue<Bytes, LRUCacheEntry> next = iterator.next();
            assertArrayEquals(bytes[bytesIndex], peekedKey.get());
            assertArrayEquals(bytes[bytesIndex], next.key.get());
            bytesIndex++;
        }
        assertEquals(5, bytesIndex);
    }

    @Test
    public void shouldSkipToEntryWhenToInclusiveIsFalseInRange() {
        final ThreadCache cache = setupThreadCache(0, 10, 10000L, false);
        final ThreadCache.MemoryLRUCacheBytesIterator iterator = cache.range(namespace, Bytes.wrap(new byte[]{1}), Bytes.wrap(new byte[]{4}), false);
        int bytesIndex = 1;
        while (iterator.hasNext()) {
            final Bytes peekedKey = iterator.peekNextKey();
            final KeyValue<Bytes, LRUCacheEntry> next = iterator.next();
            assertArrayEquals(bytes[bytesIndex], peekedKey.get());
            assertArrayEquals(bytes[bytesIndex], next.key.get());
            bytesIndex++;
        }
        assertEquals(4, bytesIndex);
    }

    @Test
    public void shouldPeekAndIterateOverReverseRange() {
        final ThreadCache cache = setupThreadCache(10, 0, 10000L, true);
        final ThreadCache.MemoryLRUCacheBytesIterator iterator = cache.reverseRange(namespace, Bytes.wrap(new byte[]{1}), Bytes.wrap(new byte[]{4}));
        int bytesIndex = 4;
        while (iterator.hasNext()) {
            final Bytes peekedKey = iterator.peekNextKey();
            final KeyValue<Bytes, LRUCacheEntry> next = iterator.next();
            assertArrayEquals(bytes[bytesIndex], peekedKey.get());
            assertArrayEquals(bytes[bytesIndex], next.key.get());
            bytesIndex--;
        }
        assertEquals(0, bytesIndex);
    }

    @Test
    public void shouldSkipEntriesWhereValueHasBeenEvictedFromCache() {
        final long entrySize = memoryCacheEntrySize(new byte[1], new byte[1], "");
        final ThreadCache cache = setupThreadCache(0, 5, entrySize * 5L, false);
        assertEquals(5, cache.size());
        // should evict byte[] {0}
        cache.put(namespace, Bytes.wrap(new byte[]{6}), dirtyEntry(new byte[]{6}));
        final ThreadCache.MemoryLRUCacheBytesIterator range = cache.range(namespace, Bytes.wrap(new byte[]{0}), Bytes.wrap(new byte[]{5}));
        assertEquals(Bytes.wrap(new byte[]{1}), range.peekNextKey());
    }

    @Test
    public void shouldSkipEntriesWhereValueHasBeenEvictedFromCacheReverseRange() {
        final long entrySize = memoryCacheEntrySize(new byte[1], new byte[1], "");
        final ThreadCache cache = setupThreadCache(4, 0, entrySize * 5L, true);
        assertEquals(5, cache.size());
        // should evict byte[] {4}
        cache.put(namespace, Bytes.wrap(new byte[]{6}), dirtyEntry(new byte[]{6}));
        final ThreadCache.MemoryLRUCacheBytesIterator range = cache.reverseRange(namespace, Bytes.wrap(new byte[]{0}), Bytes.wrap(new byte[]{5}));
        assertEquals(Bytes.wrap(new byte[]{3}), range.peekNextKey());
    }

    @Test
    public void shouldFetchAllEntriesInCache() {
        final ThreadCache cache = setupThreadCache(0, 11, 10000L, false);
        final ThreadCache.MemoryLRUCacheBytesIterator iterator = cache.all(namespace);
        int bytesIndex = 0;
        while (iterator.hasNext()) {
            final Bytes peekedKey = iterator.peekNextKey();
            final KeyValue<Bytes, LRUCacheEntry> next = iterator.next();
            assertArrayEquals(bytes[bytesIndex], peekedKey.get());
            assertArrayEquals(bytes[bytesIndex], next.key.get());
            bytesIndex++;
        }
        assertEquals(11, bytesIndex);
    }

    @Test
    public void shouldFetchAllEntriesInCacheInReverseOrder() {
        final ThreadCache cache = setupThreadCache(10, 0, 10000L, true);
        final ThreadCache.MemoryLRUCacheBytesIterator iterator = cache.reverseAll(namespace);
        int bytesIndex = 10;
        while (iterator.hasNext()) {
            final Bytes peekedKey = iterator.peekNextKey();
            final KeyValue<Bytes, LRUCacheEntry> next = iterator.next();
            assertArrayEquals(bytes[bytesIndex], peekedKey.get());
            assertArrayEquals(bytes[bytesIndex], next.key.get());
            bytesIndex--;
        }
        assertEquals(-1, bytesIndex);
    }

    @Test
    public void shouldReturnAllUnevictedValuesFromCache() {
        final long entrySize = memoryCacheEntrySize(new byte[1], new byte[1], "");
        final ThreadCache cache = setupThreadCache(0, 5, entrySize * 5L, false);
        assertEquals(5, cache.size());
        // should evict byte[] {0}
        cache.put(namespace, Bytes.wrap(new byte[]{6}), dirtyEntry(new byte[]{6}));
        final ThreadCache.MemoryLRUCacheBytesIterator range = cache.all(namespace);
        assertEquals(Bytes.wrap(new byte[]{1}), range.peekNextKey());
    }

    @Test
    public void shouldReturnAllUnevictedValuesFromCacheInReverseOrder() {
        final long entrySize = memoryCacheEntrySize(new byte[1], new byte[1], "");
        final ThreadCache cache = setupThreadCache(4, 0, entrySize * 5L, true);
        assertEquals(5, cache.size());
        // should evict byte[] {4}
        cache.put(namespace, Bytes.wrap(new byte[]{6}), dirtyEntry(new byte[]{6}));
        final ThreadCache.MemoryLRUCacheBytesIterator range = cache.reverseAll(namespace);
        assertEquals(Bytes.wrap(new byte[]{6}), range.peekNextKey());
    }

    @Test
    public void shouldFlushDirtyEntriesForNamespace() {
        final ThreadCache cache = new ThreadCache(logContext, 100000, new MockStreamsMetrics(new Metrics()));
        final List<byte[]> received = new ArrayList<>();
        cache.addDirtyEntryFlushListener(namespace1, dirty -> {
            for (final ThreadCache.DirtyEntry dirtyEntry : dirty) {
                received.add(dirtyEntry.key().get());
            }
        });
        final List<byte[]> expected = Arrays.asList(new byte[]{0}, new byte[]{1}, new byte[]{2});
        for (final byte[] bytes : expected) {
            cache.put(namespace1, Bytes.wrap(bytes), dirtyEntry(bytes));
        }
        cache.put(namespace2, Bytes.wrap(new byte[]{4}), dirtyEntry(new byte[]{4}));

        cache.flush(namespace1);
        assertEquals(expected, received);
    }

    @Test
    public void shouldNotFlushCleanEntriesForNamespace() {
        final ThreadCache cache = new ThreadCache(logContext, 100000, new MockStreamsMetrics(new Metrics()));
        final List<byte[]> received = new ArrayList<>();
        cache.addDirtyEntryFlushListener(namespace1, dirty -> {
            for (final ThreadCache.DirtyEntry dirtyEntry : dirty) {
                received.add(dirtyEntry.key().get());
            }
        });
        final List<byte[]> toInsert =  Arrays.asList(new byte[]{0}, new byte[]{1}, new byte[]{2});
        for (final byte[] bytes : toInsert) {
            cache.put(namespace1, Bytes.wrap(bytes), cleanEntry(bytes));
        }
        cache.put(namespace2, Bytes.wrap(new byte[]{4}), cleanEntry(new byte[]{4}));

        cache.flush(namespace1);
        assertEquals(Collections.emptyList(), received);
    }


    private void shouldEvictImmediatelyIfCacheSizeIsZeroOrVerySmall(final ThreadCache cache) {
        final List<ThreadCache.DirtyEntry> received = new ArrayList<>();

        cache.addDirtyEntryFlushListener(namespace, received::addAll);
        cache.put(namespace, Bytes.wrap(new byte[]{0}), dirtyEntry(new byte[]{0}));
        assertEquals(1, received.size());

        // flushing should have no further effect
        cache.flush(namespace);
        assertEquals(1, received.size());
    }

    @Test
    public void shouldEvictImmediatelyIfCacheSizeIsVerySmall() {
        final ThreadCache cache = new ThreadCache(logContext, 1, new MockStreamsMetrics(new Metrics()));
        shouldEvictImmediatelyIfCacheSizeIsZeroOrVerySmall(cache);
    }

    @Test
    public void shouldEvictImmediatelyIfCacheSizeIsZero() {
        final ThreadCache cache = new ThreadCache(logContext, 0, new MockStreamsMetrics(new Metrics()));
        shouldEvictImmediatelyIfCacheSizeIsZeroOrVerySmall(cache);
    }

    @Test
    public void shouldEvictAfterPutAll() {
        final List<ThreadCache.DirtyEntry> received = new ArrayList<>();
        final ThreadCache cache = new ThreadCache(logContext, 1, new MockStreamsMetrics(new Metrics()));
        cache.addDirtyEntryFlushListener(namespace, received::addAll);

        cache.putAll(namespace, Arrays.asList(KeyValue.pair(Bytes.wrap(new byte[]{0}), dirtyEntry(new byte[]{5})),
            KeyValue.pair(Bytes.wrap(new byte[]{1}), dirtyEntry(new byte[]{6}))));

        assertEquals(cache.evicts(), 2);
        assertEquals(received.size(), 2);
    }

    @Test
    public void shouldPutAll() {
        final ThreadCache cache = new ThreadCache(logContext, 100000, new MockStreamsMetrics(new Metrics()));

        cache.putAll(namespace, Arrays.asList(KeyValue.pair(Bytes.wrap(new byte[]{0}), dirtyEntry(new byte[]{5})),
            KeyValue.pair(Bytes.wrap(new byte[]{1}), dirtyEntry(new byte[]{6}))));

        assertArrayEquals(new byte[]{5}, cache.get(namespace, Bytes.wrap(new byte[]{0})).value());
        assertArrayEquals(new byte[]{6}, cache.get(namespace, Bytes.wrap(new byte[]{1})).value());
    }

    @Test
    public void shouldNotForwardCleanEntryOnEviction() {
        final ThreadCache cache = new ThreadCache(logContext, 0, new MockStreamsMetrics(new Metrics()));
        final List<ThreadCache.DirtyEntry> received = new ArrayList<>();
        cache.addDirtyEntryFlushListener(namespace, received::addAll);
        cache.put(namespace, Bytes.wrap(new byte[]{1}), cleanEntry(new byte[]{0}));
        assertEquals(0, received.size());
    }
    @Test
    public void shouldPutIfAbsent() {
        final ThreadCache cache = new ThreadCache(logContext, 100000, new MockStreamsMetrics(new Metrics()));
        final Bytes key = Bytes.wrap(new byte[]{10});
        final byte[] value = {30};
        assertNull(cache.putIfAbsent(namespace, key, dirtyEntry(value)));
        assertArrayEquals(value, cache.putIfAbsent(namespace, key, dirtyEntry(new byte[]{8})).value());
        assertArrayEquals(value, cache.get(namespace, key).value());
    }

    @Test
    public void shouldEvictAfterPutIfAbsent() {
        final List<ThreadCache.DirtyEntry> received = new ArrayList<>();
        final ThreadCache cache = new ThreadCache(logContext, 1, new MockStreamsMetrics(new Metrics()));
        cache.addDirtyEntryFlushListener(namespace, received::addAll);

        cache.putIfAbsent(namespace, Bytes.wrap(new byte[]{0}), dirtyEntry(new byte[]{5}));
        cache.putIfAbsent(namespace, Bytes.wrap(new byte[]{1}), dirtyEntry(new byte[]{6}));
        cache.putIfAbsent(namespace, Bytes.wrap(new byte[]{1}), dirtyEntry(new byte[]{6}));

        assertEquals(cache.evicts(), 3);
        assertEquals(received.size(), 3);
    }

    @Test
    public void shouldNotLoopForEverWhenEvictingAndCurrentCacheIsEmpty() {
        final int maxCacheSizeInBytes = 100;
        final ThreadCache threadCache = new ThreadCache(logContext, maxCacheSizeInBytes, new MockStreamsMetrics(new Metrics()));
        // trigger a put into another cache on eviction from "name"
        threadCache.addDirtyEntryFlushListener(namespace, dirty -> {
            // put an item into an empty cache when the total cache size
            // is already > than maxCacheSizeBytes
            threadCache.put(namespace1, Bytes.wrap(new byte[]{0}), dirtyEntry(new byte[2]));
        });
        threadCache.addDirtyEntryFlushListener(namespace1, dirty -> { });
        threadCache.addDirtyEntryFlushListener(namespace2, dirty -> { });

        threadCache.put(namespace2, Bytes.wrap(new byte[]{1}), dirtyEntry(new byte[1]));
        threadCache.put(namespace, Bytes.wrap(new byte[]{1}), dirtyEntry(new byte[1]));
        // Put a large item such that when the eldest item is removed
        // cache sizeInBytes() > maxCacheSizeBytes
        final int remaining = (int) (maxCacheSizeInBytes - threadCache.sizeBytes());
        threadCache.put(namespace, Bytes.wrap(new byte[]{2}), dirtyEntry(new byte[remaining + 100]));
    }

    @Test
    public void shouldCleanupNamedCacheOnClose() {
        final ThreadCache cache = new ThreadCache(logContext, 100000, new MockStreamsMetrics(new Metrics()));
        cache.put(namespace1, Bytes.wrap(new byte[]{1}), cleanEntry(new byte[] {1}));
        cache.put(namespace2, Bytes.wrap(new byte[]{1}), cleanEntry(new byte[] {1}));
        assertEquals(cache.size(), 2);
        cache.close(namespace2);
        assertEquals(cache.size(), 1);
        assertNull(cache.get(namespace2, Bytes.wrap(new byte[]{1})));
    }

    @Test
    public void shouldReturnNullIfKeyIsNull() {
        final ThreadCache threadCache = new ThreadCache(logContext, 10, new MockStreamsMetrics(new Metrics()));
        threadCache.put(namespace, Bytes.wrap(new byte[]{1}), cleanEntry(new byte[] {1}));
        assertNull(threadCache.get(namespace, null));
    }

    @Test
    public void shouldCalculateSizeInBytes() {
        final ThreadCache cache = new ThreadCache(logContext, 100000, new MockStreamsMetrics(new Metrics()));
        final NamedCache.LRUNode node = new NamedCache.LRUNode(Bytes.wrap(new byte[]{1}), dirtyEntry(new byte[]{0}));
        cache.put(namespace1, Bytes.wrap(new byte[]{1}), cleanEntry(new byte[]{0}));
        assertEquals(cache.sizeBytes(), node.size());
    }

    @Test
    public void shouldResizeAndShrink() {
        final ThreadCache cache = new ThreadCache(logContext, 10000, new MockStreamsMetrics(new Metrics()));
        cache.put(namespace, Bytes.wrap(new byte[]{1}), cleanEntry(new byte[]{0}));
        cache.put(namespace, Bytes.wrap(new byte[]{2}), cleanEntry(new byte[]{0}));
        cache.put(namespace, Bytes.wrap(new byte[]{3}), cleanEntry(new byte[]{0}));
        assertEquals(141, cache.sizeBytes());
        cache.resize(100);
        assertEquals(94, cache.sizeBytes());
        cache.put(namespace1, Bytes.wrap(new byte[]{4}), cleanEntry(new byte[]{0}));
        assertEquals(94, cache.sizeBytes());
    }

    private LRUCacheEntry dirtyEntry(final byte[] key) {
        return new LRUCacheEntry(key, new RecordHeaders(), true, -1, -1, -1, "");
    }

    private LRUCacheEntry cleanEntry(final byte[] key) {
        return new LRUCacheEntry(key);
    }


}
