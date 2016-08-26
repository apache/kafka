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
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

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
                toInsert.size() * memoryCacheEntrySize(kv.key.getBytes(), kv.value.getBytes()));

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
        final int length = memoryCacheEntrySize(kv.key.getBytes(), kv.value.getBytes());

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

    static int memoryCacheEntrySize(byte[] key, byte[] value) {
        return key.length +
                value.length +
                1 + // isDirty
                8 + // timestamp
                8 + // offset
                4;
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
                memoryCacheEntrySize(kv.key.getBytes(), kv.value.getBytes()));
        cache.addEldestRemovedListener(namespace, new MemoryLRUCacheBytes.EldestEntryRemovalListener() {
            @Override
            public void apply(byte[] key, MemoryLRUCacheBytesEntry value) {

                received.add(new KeyValue<>(new String(key),
                                            new String(value.value)));
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
        final byte [] key = new byte[] {0};

        cache.put("name", key, entry(key));
        assertEquals(key, cache.delete("name", key).value);
        assertNull(cache.get("name", key));
    }

    @Test
    public void shouldNotBlowUpOnNonExistentKeyWhenDeleting() throws Exception {
        final MemoryLRUCacheBytes cache = new MemoryLRUCacheBytes(10000L);
        final byte [] key = new byte[] {0};

        cache.put("name", key, entry(key));
        assertNull(cache.delete("name", new byte[] {1}));
    }

    @Test
    public void shouldNotBlowUpOnNonExistentNamespaceWhenDeleting() throws Exception {
        final MemoryLRUCacheBytes cache = new MemoryLRUCacheBytes(10000L);
        assertNull(cache.delete("name", new byte[] {1}));
    }

    @Test
    public void shouldNotClashWithOverlappingNames() throws Exception {
        final MemoryLRUCacheBytes cache = new MemoryLRUCacheBytes(10000L);
        final byte [] nameByte = new byte[] {0};
        final byte [] name1Byte = new byte[] {1};
        cache.put("name", nameByte, entry(nameByte));
        cache.put("name1", nameByte, entry(name1Byte));

        assertArrayEquals(nameByte, cache.get("name", nameByte).value);
        assertArrayEquals(name1Byte, cache.get("name1", nameByte).value);
    }

    @Test
    public void shouldPeekNextKey() throws Exception {
        final MemoryLRUCacheBytes cache = new MemoryLRUCacheBytes(10000L);
        final byte[] theByte = {0};
        final String namespace = "streams";
        cache.put(namespace, theByte, entry(theByte));
        final MemoryLRUCacheBytes.MemoryLRUCacheBytesIterator iterator = cache.range(namespace, theByte, new byte[]{1});
        assertArrayEquals(theByte, iterator.peekNextKey());
        assertArrayEquals(theByte, iterator.peekNextKey());
    }

    @Test
    public void shouldGetSameKeyAsPeekNext() throws Exception {
        final MemoryLRUCacheBytes cache = new MemoryLRUCacheBytes(10000L);
        final byte[] theByte = {0};
        final String namespace = "streams";
        cache.put(namespace, theByte, entry(theByte));
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
            cache.put(namespace, aByte, entry(aByte));
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
        final int entrySize = memoryCacheEntrySize(new byte[1], new byte[1]);
        final MemoryLRUCacheBytes cache = new MemoryLRUCacheBytes(entrySize * 5);
        byte[][] bytes = {{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}};
        for (int i = 0; i < 5; i++) {
            cache.put(namespace, bytes[i], entry(bytes[i]));
        }
        assertEquals(5, cache.size());

        final MemoryLRUCacheBytes.MemoryLRUCacheBytesIterator range = cache.range(namespace, new byte[]{0}, new byte[]{5});
        // should evict byte[] {0}
        cache.put(namespace, new byte[]{6}, entry(new byte[]{6}));

        assertArrayEquals(new byte[]{1}, range.peekNextKey());
    }

    private MemoryLRUCacheBytesEntry entry(final byte[] key) {
        return new MemoryLRUCacheBytesEntry(key);
    }


}