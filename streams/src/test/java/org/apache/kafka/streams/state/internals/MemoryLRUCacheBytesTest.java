/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.streams.KeyValue;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MemoryLRUCacheBytesTest {

    @Test
    public void basicPutGet() throws IOException {
        List<KeyValue<String, String>> toInsert = Arrays.asList(
                new KeyValue<>("K1", "V1"),
                new KeyValue<>("K2", "V2"),
                new KeyValue<>("K3", "V3"),
                new KeyValue<>("K4", "V4"),
                new KeyValue<>("K5", "V5"));
        final KeyValue<String, String> kv = toInsert.get(0);
        final String name = "name";
        MemoryLRUCacheBytes cache = new MemoryLRUCacheBytes(
                toInsert.size() * memoryCacheEntrySize(name, kv.key.getBytes(), kv.value.getBytes(), ""));

        for (int i = 0; i < toInsert.size(); i++) {
            byte[] key = toInsert.get(i).key.getBytes();
            byte[] value = toInsert.get(i).value.getBytes();
            cache.put(name, key, new MemoryLRUCacheBytesEntry(value, true, 1L, 1L, 1, ""));
        }

        for (int i = 0; i < toInsert.size(); i++) {
            byte[] key = toInsert.get(i).key.getBytes();
            MemoryLRUCacheBytesEntry entry = cache.get(name, key);
            assertEquals(entry.isDirty, true);
            assertEquals(new String(entry.value), toInsert.get(i).value);
        }
    }

    private void checkOverheads(double entryFactor, double systemFactor, long desiredCacheSize, int keySizeBytes,
                            int valueSizeBytes) {
        Runtime runtime = Runtime.getRuntime();
        byte[] key = new byte[keySizeBytes];
        byte[] value = new byte[valueSizeBytes];
        final String name = "name";
        long numElements = desiredCacheSize / memoryCacheEntrySize(name, key, value, "");

        System.gc();
        long prevRuntimeMemory = runtime.totalMemory() - runtime.freeMemory();

        MemoryLRUCacheBytes cache = new MemoryLRUCacheBytes(desiredCacheSize);
        long size = cache.sizeBytes();
        assertEquals(size, 0);
        for (int i = 0; i < numElements; i++) {
            String keyStr = "K" + i;
            key = keyStr.getBytes();
            value = new byte[valueSizeBytes];
            cache.put(name, key, new MemoryLRUCacheBytesEntry(value, true, 1L, 1L, 1, ""));
        }


        System.gc();
        double floor = desiredCacheSize - desiredCacheSize * entryFactor;
        double ceiling = desiredCacheSize + desiredCacheSize * entryFactor;
        long usedRuntimeMemory = runtime.totalMemory() - runtime.freeMemory() - prevRuntimeMemory;
        assertTrue(floor <= (double) cache.sizeBytes()
            && (double) cache.sizeBytes() <= ceiling);
        assertTrue("Used memory size " + usedRuntimeMemory + " greater than expected " + cache.sizeBytes() * systemFactor,
            cache.sizeBytes() * systemFactor >= usedRuntimeMemory);
    }

    @Test
    public void cacheOverheadsSmallValues() {
        Runtime runtime = Runtime.getRuntime();
        double factor = 0.05;
        double systemFactor = 2.5;
        long desiredCacheSize = Math.min(1 * 1024 * 1024 * 1024L, runtime.maxMemory());
        int keySizeBytes = 8;
        int valueSizeBytes = 100;

        checkOverheads(factor, systemFactor, desiredCacheSize, keySizeBytes, valueSizeBytes);
    }

    @Test
    public void cacheOverheadsLargeValues() {
        Runtime runtime = Runtime.getRuntime();
        double factor = 0.05;
        double systemFactor = 1.5;
        long desiredCacheSize = Math.min(1 * 1024 * 1024 * 1024L, runtime.maxMemory());
        int keySizeBytes = 8;
        int valueSizeBytes = 1000;

        checkOverheads(factor, systemFactor, desiredCacheSize, keySizeBytes, valueSizeBytes);
    }

    @Test
    public void headTail() throws IOException {
        List<KeyValue<String, String>> toInsert = Arrays.asList(
                new KeyValue<>("K1", "V1"),
                new KeyValue<>("K2", "V2"),
                new KeyValue<>("K3", "V3"),
                new KeyValue<>("K4", "V4"),
                new KeyValue<>("K5", "V5"));
        final KeyValue<String, String> kv = toInsert.get(0);
        final String namespace = "jo";
        final int length = memoryCacheEntrySize(namespace, kv.key.getBytes(), kv.value.getBytes(), "");

        MemoryLRUCacheBytes cache = new MemoryLRUCacheBytes(
                toInsert.size() * length);

        for (int i = 0; i < toInsert.size(); i++) {
            byte[] key = toInsert.get(i).key.getBytes();
            byte[] value = toInsert.get(i).value.getBytes();
            cache.put(namespace, key, new MemoryLRUCacheBytesEntry(value, true, 1, 1, 1, ""));
            MemoryLRUCacheBytesEntry head = cache.head().entry();
            MemoryLRUCacheBytesEntry tail = cache.tail().entry();
            assertEquals(new String(head.value), toInsert.get(i).value);
            assertEquals(new String(tail.value), toInsert.get(0).value);
        }
    }

    static int memoryCacheEntrySize(String namespace, byte[] key, byte[] value, final String topic) {
        return key.length +
                value.length +
                1 + // isDirty
                8 + // timestamp
                8 + // offset
                4 +
                topic.length() +
                // LRU Node entries
                key.length +
                namespace.length() +
                8 + // entry
                8 + // previous
                8; // next
    }

    @Test
    public void evict() throws IOException {
        final List<KeyValue<String, String>> received = new ArrayList<>();
        List<KeyValue<String, String>> expected = Arrays.asList(
                new KeyValue<>("K1", "V1"));

        List<KeyValue<String, String>> toInsert = Arrays.asList(
                new KeyValue<>("K1", "V1"),
                new KeyValue<>("K2", "V2"),
                new KeyValue<>("K3", "V3"),
                new KeyValue<>("K4", "V4"),
                new KeyValue<>("K5", "V5"));
        final KeyValue<String, String> kv = toInsert.get(0);
        final String namespace = "kafka";
        MemoryLRUCacheBytes cache = new MemoryLRUCacheBytes(
                memoryCacheEntrySize(namespace, kv.key.getBytes(), kv.value.getBytes(), ""));
        cache.addDirtyEntryFlushListener(namespace, new MemoryLRUCacheBytes.DirtyEntryFlushListener() {
            @Override
            public void apply(final List<MemoryLRUCacheBytes.DirtyEntry> dirty) {
                for (MemoryLRUCacheBytes.DirtyEntry dirtyEntry : dirty) {
                    received.add(new KeyValue<>(dirtyEntry.key().toString(), new String(dirtyEntry.newValue())));
                }
            }

        });


        for (int i = 0; i < toInsert.size(); i++) {
            byte[] key = toInsert.get(i).key.getBytes();
            byte[] value = toInsert.get(i).value.getBytes();
            cache.put(namespace, key, new MemoryLRUCacheBytesEntry(value, true, 1, 1, 1, ""));
        }

        for (int i = 0; i < expected.size(); i++) {
            KeyValue<String, String> expectedRecord = expected.get(i);
            KeyValue<String, String> actualRecord = received.get(i);
            assertEquals(expectedRecord, actualRecord);
        }
    }

    @Test
    public void shouldDelete() throws Exception {
        final MemoryLRUCacheBytes cache = new MemoryLRUCacheBytes(10000L);
        final byte[] key = new byte[]{0};

        cache.put("name", key, dirtyEntry(key));
        assertEquals(key, cache.delete("name", key).value);
        assertNull(cache.get("name", key));
    }

    @Test
    public void shouldNotFlushAfterDelete() throws Exception {
        final byte[] key = new byte[]{0};
        final MemoryLRUCacheBytes cache = new MemoryLRUCacheBytes(10000L);
        final List<MemoryLRUCacheBytes.DirtyEntry> received = new ArrayList<>();
        final String namespace = "namespace";
        cache.addDirtyEntryFlushListener(namespace, new MemoryLRUCacheBytes.DirtyEntryFlushListener() {
            @Override
            public void apply(final List<MemoryLRUCacheBytes.DirtyEntry> dirty) {
                received.addAll(dirty);
            }
        });
        cache.put(namespace, key, dirtyEntry(key));
        assertEquals(key, cache.delete(namespace, key).value);

        // flushing should have no further effect
        cache.flush(namespace);
        assertEquals(0, received.size());
    }

    @Test
    public void shouldNotBlowUpOnNonExistentKeyWhenDeleting() throws Exception {
        final MemoryLRUCacheBytes cache = new MemoryLRUCacheBytes(10000L);
        final byte[] key = new byte[]{0};

        cache.put("name", key, dirtyEntry(key));
        assertNull(cache.delete("name", new byte[]{1}));
    }

    @Test
    public void shouldNotBlowUpOnNonExistentNamespaceWhenDeleting() throws Exception {
        final MemoryLRUCacheBytes cache = new MemoryLRUCacheBytes(10000L);
        assertNull(cache.delete("name", new byte[]{1}));
    }

    @Test
    public void shouldNotClashWithOverlappingNames() throws Exception {
        final MemoryLRUCacheBytes cache = new MemoryLRUCacheBytes(10000L);
        final byte[] nameByte = new byte[]{0};
        final byte[] name1Byte = new byte[]{1};
        cache.put("name", nameByte, dirtyEntry(nameByte));
        cache.put("name1", nameByte, dirtyEntry(name1Byte));

        assertArrayEquals(nameByte, cache.get("name", nameByte).value);
        assertArrayEquals(name1Byte, cache.get("name1", nameByte).value);
    }

    @Test
    public void shouldPeekNextKey() throws Exception {
        final MemoryLRUCacheBytes cache = new MemoryLRUCacheBytes(10000L);
        final byte[] theByte = {0};
        final String namespace = "streams";
        cache.put(namespace, theByte, dirtyEntry(theByte));
        final MemoryLRUCacheBytes.MemoryLRUCacheBytesIterator iterator = cache.range(namespace, theByte, new byte[]{1});
        assertArrayEquals(theByte, iterator.peekNextKey());
        assertArrayEquals(theByte, iterator.peekNextKey());
    }

    @Test
    public void shouldGetSameKeyAsPeekNext() throws Exception {
        final MemoryLRUCacheBytes cache = new MemoryLRUCacheBytes(10000L);
        final byte[] theByte = {0};
        final String namespace = "streams";
        cache.put(namespace, theByte, dirtyEntry(theByte));
        final MemoryLRUCacheBytes.MemoryLRUCacheBytesIterator iterator = cache.range(namespace, theByte, new byte[]{1});
        assertArrayEquals(iterator.peekNextKey(), iterator.next().key);
    }

    @Test(expected = NoSuchElementException.class)
    public void shouldThrowIfNoPeekNextKey() throws Exception {
        final MemoryLRUCacheBytes cache = new MemoryLRUCacheBytes(10000L);
        final MemoryLRUCacheBytes.MemoryLRUCacheBytesIterator iterator = cache.range("", new byte[]{0}, new byte[]{1});
        iterator.peekNextKey();
    }

    @Test(expected = NoSuchElementException.class)
    public void shouldThrowIfNoNextKey() throws Exception {
        final MemoryLRUCacheBytes cache = new MemoryLRUCacheBytes(10000L);
        final MemoryLRUCacheBytes.MemoryLRUCacheBytesIterator iterator = cache.range("", new byte[]{0}, new byte[]{1});
        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldPeekAndIterateOverRange() throws Exception {
        final MemoryLRUCacheBytes cache = new MemoryLRUCacheBytes(10000L);
        final byte[][] bytes = {{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}};
        final String namespace = "streams";
        for (final byte[] aByte : bytes) {
            cache.put(namespace, aByte, dirtyEntry(aByte));
        }
        final MemoryLRUCacheBytes.MemoryLRUCacheBytesIterator iterator = cache.range(namespace, new byte[]{1}, new byte[]{4});
        int bytesIndex = 1;
        while (iterator.hasNext()) {
            byte[] peekedKey = iterator.peekNextKey();
            final KeyValue<byte[], MemoryLRUCacheBytesEntry> next = iterator.next();
            assertArrayEquals(bytes[bytesIndex], peekedKey);
            assertArrayEquals(bytes[bytesIndex], next.key);
            bytesIndex++;
        }
        assertEquals(5, bytesIndex);
    }

    @Test
    public void shouldSkipEntriesWhereValueHasBeenEvictedFromCache() throws Exception {
        final String namespace = "streams";
        final int entrySize = memoryCacheEntrySize(namespace, new byte[1], new byte[1], "");
        final MemoryLRUCacheBytes cache = new MemoryLRUCacheBytes(entrySize * 5);
        byte[][] bytes = {{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}};
        for (int i = 0; i < 5; i++) {
            cache.put(namespace, bytes[i], dirtyEntry(bytes[i]));
        }
        assertEquals(5, cache.size());

        final MemoryLRUCacheBytes.MemoryLRUCacheBytesIterator range = cache.range(namespace, new byte[]{0}, new byte[]{5});
        // should evict byte[] {0}
        cache.put(namespace, new byte[]{6}, dirtyEntry(new byte[]{6}));

        assertArrayEquals(new byte[]{1}, range.peekNextKey());
    }

    @Test
    public void shouldFlushDirtyEntriesForNamespace() throws Exception {
        final MemoryLRUCacheBytes cache = new MemoryLRUCacheBytes(100000);
        final List<byte[]> received = new ArrayList<>();
        cache.addDirtyEntryFlushListener("1", new MemoryLRUCacheBytes.DirtyEntryFlushListener() {
            @Override
            public void apply(final List<MemoryLRUCacheBytes.DirtyEntry> dirty) {
                for (MemoryLRUCacheBytes.DirtyEntry dirtyEntry : dirty) {
                    received.add(dirtyEntry.key().get());
                }
            }
        });
        final List<byte[]> expected = Arrays.asList(new byte[]{0}, new byte[]{1}, new byte[]{2});
        for (byte[] bytes : expected) {
            cache.put("1", bytes, dirtyEntry(bytes));
        }
        cache.put("2", new byte[]{4}, dirtyEntry(new byte[]{4}));

        cache.flush("1");
        assertEquals(expected, received);
    }

    @Test
    public void shouldNotFlushCleanEntriesForNamespace() throws Exception {
        final MemoryLRUCacheBytes cache = new MemoryLRUCacheBytes(100000);
        final List<byte[]> received = new ArrayList<>();
        cache.addDirtyEntryFlushListener("1", new MemoryLRUCacheBytes.DirtyEntryFlushListener() {
            @Override
            public void apply(final List<MemoryLRUCacheBytes.DirtyEntry> dirty) {
                for (MemoryLRUCacheBytes.DirtyEntry dirtyEntry : dirty) {
                    received.add(dirtyEntry.key().get());
                }
            }
        });
        final List<byte[]> toInsert =  Arrays.asList(new byte[]{0}, new byte[]{1}, new byte[]{2});
        for (byte[] bytes : toInsert) {
            cache.put("1", bytes, cleanEntry(bytes));
        }
        cache.put("2", new byte[]{4}, cleanEntry(new byte[]{4}));

        cache.flush("1");
        assertEquals(Collections.EMPTY_LIST, received);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentOnFlushIfNoListenerRegistered() throws Exception {
        final MemoryLRUCacheBytes cache = new MemoryLRUCacheBytes(100000);
        cache.flush("whateva");
    }

    private void shouldEvictImmediatelyIfCacheSizeIsZeroOrVerySmall(final MemoryLRUCacheBytes cache) {
        final List<MemoryLRUCacheBytes.DirtyEntry> received = new ArrayList<>();
        final String namespace = "namespace";
        cache.addDirtyEntryFlushListener(namespace, new MemoryLRUCacheBytes.DirtyEntryFlushListener() {
            @Override
            public void apply(final List<MemoryLRUCacheBytes.DirtyEntry> dirty) {
                received.addAll(dirty);
            }
        });
        cache.put(namespace, new byte[]{0}, dirtyEntry(new byte[]{0}));
        assertEquals(1, received.size());

        // flushing should have no further effect
        cache.flush(namespace);
        assertEquals(1, received.size());
    }

    @Test
    public void shouldEvictImmediatelyIfCacheSizeIsVerySmall() throws Exception {
        final MemoryLRUCacheBytes cache = new MemoryLRUCacheBytes(1);
        shouldEvictImmediatelyIfCacheSizeIsZeroOrVerySmall(cache);
    }

    @Test
    public void shouldEvictImmediatelyIfCacheSizeIsZero() throws Exception {
        final MemoryLRUCacheBytes cache = new MemoryLRUCacheBytes(0);
        shouldEvictImmediatelyIfCacheSizeIsZeroOrVerySmall(cache);
    }

    @Test
    public void shouldPutAll() throws Exception {
        final MemoryLRUCacheBytes cache = new MemoryLRUCacheBytes(100000);

        cache.putAll("name", Arrays.asList(KeyValue.pair(new byte[]{0}, dirtyEntry(new byte[]{5})),
                                           KeyValue.pair(new byte[]{1}, dirtyEntry(new byte[]{6}))));

        assertArrayEquals(new byte[]{5}, cache.get("name", new byte[]{0}).value);
        assertArrayEquals(new byte[]{6}, cache.get("name", new byte[]{1}).value);
    }

    @Test
    public void shouldNotForwardCleanEntryOnEviction() throws Exception {
        final MemoryLRUCacheBytes cache = new MemoryLRUCacheBytes(0);
        final List<MemoryLRUCacheBytes.DirtyEntry> received = new ArrayList<>();
        cache.addDirtyEntryFlushListener("name", new MemoryLRUCacheBytes.DirtyEntryFlushListener() {
            @Override
            public void apply(final List<MemoryLRUCacheBytes.DirtyEntry> dirty) {
                received.addAll(dirty);
            }
        });
        cache.put("name", new byte[] {1}, cleanEntry(new byte[]{0}));
        assertEquals(0, received.size());
    }
    @Test
    public void shouldPutIfAbsent() throws Exception {
        final MemoryLRUCacheBytes cache = new MemoryLRUCacheBytes(100000);
        final byte[] key = {10};
        final byte[] value = {30};
        assertNull(cache.putIfAbsent("n", key, dirtyEntry(value)));
        assertArrayEquals(value, cache.putIfAbsent("n", key, dirtyEntry(new byte[]{8})).value);
        assertArrayEquals(value, cache.get("n", key).value);
    }

    private MemoryLRUCacheBytesEntry dirtyEntry(final byte[] key) {
        return new MemoryLRUCacheBytesEntry(key, true, -1, -1, -1, "");
    }

    private MemoryLRUCacheBytesEntry cleanEntry(final byte[] key) {
        return new MemoryLRUCacheBytesEntry(key);
    }


}