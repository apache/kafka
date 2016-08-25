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

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class MemoryLRUCacheBytesTest {

    @Test
    public void basicPutGet() {
        List<KeyValue<String, String>> toInsert = Arrays.asList(
                new KeyValue<>("K1", "V1"),
                new KeyValue<>("K2", "V2"),
                new KeyValue<>("K3", "V3"),
                new KeyValue<>("K4", "V4"),
                new KeyValue<>("K5", "V5"));
        final KeyValue<String, String> kv = toInsert.get(0);
        MemoryLRUCacheBytes cache = new MemoryLRUCacheBytes(
                toInsert.size() * memoryCacheEntrySize(kv.key, kv.value));

        for (int i = 0; i < toInsert.size(); i++) {
            byte[] key = toInsert.get(i).key.getBytes();
            byte[] value = toInsert.get(i).value.getBytes();
            cache.put(key, new MemoryLRUCacheBytesEntry(key, value, true, 1L, 1L, 1, ""));
        }

        for (int i = 0; i < toInsert.size(); i++) {
            byte[] key = toInsert.get(i).key.getBytes();
            MemoryLRUCacheBytesEntry entry = cache.get(key);
            assertEquals(entry.isDirty, true);
            assertEquals(new String(entry.value), toInsert.get(i).value);
        }
    }

    @Test
    public void headTail() {
        List<KeyValue<String, String>> toInsert = Arrays.asList(
                new KeyValue<>("K1", "V1"),
                new KeyValue<>("K2", "V2"),
                new KeyValue<>("K3", "V3"),
                new KeyValue<>("K4", "V4"),
                new KeyValue<>("K5", "V5"));
        final KeyValue<String, String> kv = toInsert.get(0);
        final int length = memoryCacheEntrySize(kv.key, kv.value);

        MemoryLRUCacheBytes cache = new MemoryLRUCacheBytes(
                toInsert.size() * length);

        for (int i = 0; i < toInsert.size(); i++) {
            byte[] key = toInsert.get(i).key.getBytes();
            byte[] value = toInsert.get(i).value.getBytes();
            cache.put(key, new MemoryLRUCacheBytesEntry(key, value, true, 1, 1, 1, ""));
            MemoryLRUCacheBytesEntry head = cache.head().entry();
            MemoryLRUCacheBytesEntry tail = cache.tail().entry();
            assertEquals(new String(head.value), toInsert.get(i).value);
            assertEquals(new String(tail.value), toInsert.get(0).value);
        }
    }

    static int memoryCacheEntrySize(String key, String value) {
        return key.getBytes().length +
                value.getBytes().length +
                1 + // isDirty
                8 + // timestamp
                8 + // offset
                4;
    }

    @Test
    public void evict() {
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
        MemoryLRUCacheBytes cache = new MemoryLRUCacheBytes(
                memoryCacheEntrySize(kv.key, kv.value));
        cache.addEldestRemovedListener(new MemoryLRUCacheBytes.EldestEntryRemovalListener<byte[],
                MemoryLRUCacheBytesEntry>() {
            @Override
            public void apply(byte[] key, MemoryLRUCacheBytesEntry value) {

                received.add(new KeyValue<>(new String(key),
                                            new String(value.value)));
            }
        });

        for (int i = 0; i < toInsert.size(); i++) {
            byte[] key = toInsert.get(i).key.getBytes();
            byte[] value = toInsert.get(i).value.getBytes();
            cache.put(key, new MemoryLRUCacheBytesEntry(key, value, true, 1, 1, 1, ""));
        }

        for (int i = 0; i < expected.size(); i++) {
            KeyValue<String, String> expectedRecord = expected.get(i);
            KeyValue<String, String> actualRecord = received.get(i);
            assertEquals(expectedRecord, actualRecord);
        }
    }

    @Test
    public void shouldPeekNextKey() throws Exception {
        final MemoryLRUCacheBytes cache = new MemoryLRUCacheBytes(10000L);
        final byte[] theByte = {0};
        cache.put(theByte, entry(theByte));
        final MemoryLRUCacheBytes.MemoryLRUCacheBytesIterator iterator = cache.range("", theByte, new byte[]{1});
        assertArrayEquals(theByte, iterator.peekNextKey());
        assertArrayEquals(theByte, iterator.peekNextKey());
    }

    @Test
    public void shouldGetSameKeyAsPeekNext() throws Exception {
        final MemoryLRUCacheBytes cache = new MemoryLRUCacheBytes(10000L);
        final byte[] theByte = {0};
        cache.put(theByte, entry(theByte));
        final MemoryLRUCacheBytes.MemoryLRUCacheBytesIterator iterator = cache.range("", theByte, new byte[]{1});
        assertArrayEquals(iterator.peekNextKey(), iterator.next().key);
    }

    @Test(expected = NoSuchElementException.class)
    public void shouldThrowIfNoNextKey() throws Exception {
        final MemoryLRUCacheBytes cache = new MemoryLRUCacheBytes(10000L);
        final MemoryLRUCacheBytes.MemoryLRUCacheBytesIterator iterator = cache.range("", new byte[]{0}, new byte[]{1});
        iterator.peekNextKey();
    }

    @Test
    public void shouldBeFalseIfNoNextKey() throws Exception {
        final MemoryLRUCacheBytes cache = new MemoryLRUCacheBytes(10000L);
        final MemoryLRUCacheBytes.MemoryLRUCacheBytesIterator iterator = cache.range("", new byte[]{0}, new byte[]{1});
        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldPeekAndIterateOverRange() throws Exception {
        final MemoryLRUCacheBytes cache = new MemoryLRUCacheBytes(10000L);
        byte[][] bytes = {{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}};
        for (final byte[] aByte : bytes) {
            cache.put(aByte, entry(aByte));
        }
        final MemoryLRUCacheBytes.MemoryLRUCacheBytesIterator iterator = cache.range("", new byte[]{1}, new byte[]{4});
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
        final int entrySize = 2 + // key + val
                1 + // isDirty
                8 + // timestamp
                8 + // offset
                4;
        final MemoryLRUCacheBytes cache = new MemoryLRUCacheBytes(entrySize * 5);
        byte[][] bytes = {{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}};
        for (int i = 0; i < 5; i++) {
            cache.put(bytes[i], entry(bytes[i]));
        }
        assertEquals(5, cache.size());

        final MemoryLRUCacheBytes.MemoryLRUCacheBytesIterator range = cache.range("", new byte[]{0}, new byte[]{5});
        // should evict byte[] {0}
        cache.put(new byte[]{6}, entry(new byte[]{6}));

        assertArrayEquals(new byte[]{1}, range.peekNextKey());
    }

    private MemoryLRUCacheBytesEntry entry(final byte[] key) {
        return new MemoryLRUCacheBytesEntry(key, key);
    }


}