/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

public class NamedCacheTest {

    private NamedCache cache;

    @Before
    public void setUp() throws Exception {
        cache = new NamedCache("name");
    }

    @Test
    public void shouldKeepTrackOfMostRecentlyAndLeastRecentlyUsed() throws IOException {
        List<KeyValue<String, String>> toInsert = Arrays.asList(
                new KeyValue<>("K1", "V1"),
                new KeyValue<>("K2", "V2"),
                new KeyValue<>("K3", "V3"),
                new KeyValue<>("K4", "V4"),
                new KeyValue<>("K5", "V5"));
        for (int i = 0; i < toInsert.size(); i++) {
            byte[] key = toInsert.get(i).key.getBytes();
            byte[] value = toInsert.get(i).value.getBytes();
            cache.put(Bytes.wrap(key), new LRUCacheEntry(value, true, 1, 1, 1, ""));
            LRUCacheEntry head = cache.first();
            LRUCacheEntry tail = cache.last();
            assertEquals(new String(head.value), toInsert.get(i).value);
            assertEquals(new String(tail.value), toInsert.get(0).value);
            assertEquals(cache.flushes(), 0);
            assertEquals(cache.hits(), 0);
            assertEquals(cache.misses(), 0);
            assertEquals(cache.overwrites(), 0);
        }
    }

    @Test
    public void shouldKeepTrackOfSize() throws Exception {
        final LRUCacheEntry value = new LRUCacheEntry(new byte[]{0});
        cache.put(Bytes.wrap(new byte[]{0}), value);
        cache.put(Bytes.wrap(new byte[]{1}), value);
        cache.put(Bytes.wrap(new byte[]{2}), value);
        final long size = cache.sizeInBytes();
        // 1 byte key + 24 bytes overhead
        assertEquals((value.size() + 25) * 3, size);
    }

    @Test
    public void shouldPutGet() throws Exception {
        cache.put(Bytes.wrap(new byte[]{0}), new LRUCacheEntry(new byte[]{10}));
        cache.put(Bytes.wrap(new byte[]{1}), new LRUCacheEntry(new byte[]{11}));
        cache.put(Bytes.wrap(new byte[]{2}), new LRUCacheEntry(new byte[]{12}));

        assertArrayEquals(new byte[] {10}, cache.get(Bytes.wrap(new byte[] {0})).value);
        assertArrayEquals(new byte[] {11}, cache.get(Bytes.wrap(new byte[] {1})).value);
        assertArrayEquals(new byte[] {12}, cache.get(Bytes.wrap(new byte[] {2})).value);
        assertEquals(cache.hits(), 3);
    }

    @Test
    public void shouldPutIfAbsent() throws Exception {
        cache.put(Bytes.wrap(new byte[]{0}), new LRUCacheEntry(new byte[]{10}));
        cache.putIfAbsent(Bytes.wrap(new byte[]{0}), new LRUCacheEntry(new byte[]{20}));
        cache.putIfAbsent(Bytes.wrap(new byte[]{1}), new LRUCacheEntry(new byte[]{30}));

        assertArrayEquals(new byte[] {10}, cache.get(Bytes.wrap(new byte[] {0})).value);
        assertArrayEquals(new byte[] {30}, cache.get(Bytes.wrap(new byte[] {1})).value);
    }

    @Test
    public void shouldDeleteAndUpdateSize() throws Exception {
        cache.put(Bytes.wrap(new byte[]{0}), new LRUCacheEntry(new byte[]{10}));
        final LRUCacheEntry deleted = cache.delete(Bytes.wrap(new byte[]{0}));
        assertArrayEquals(new byte[] {10}, deleted.value);
        assertEquals(0, cache.sizeInBytes());
    }

    @Test
    public void shouldPutAll() throws Exception {
        cache.putAll(Arrays.asList(KeyValue.pair(new byte[] {0}, new LRUCacheEntry(new byte[]{0})),
                                   KeyValue.pair(new byte[] {1}, new LRUCacheEntry(new byte[]{1})),
                                   KeyValue.pair(new byte[] {2}, new LRUCacheEntry(new byte[]{2}))));

        assertArrayEquals(new byte[]{0}, cache.get(Bytes.wrap(new byte[]{0})).value);
        assertArrayEquals(new byte[]{1}, cache.get(Bytes.wrap(new byte[]{1})).value);
        assertArrayEquals(new byte[]{2}, cache.get(Bytes.wrap(new byte[]{2})).value);
    }

    @Test
    public void shouldOverwriteAll() throws Exception {
        cache.putAll(Arrays.asList(KeyValue.pair(new byte[] {0}, new LRUCacheEntry(new byte[]{0})),
            KeyValue.pair(new byte[] {0}, new LRUCacheEntry(new byte[]{1})),
            KeyValue.pair(new byte[] {0}, new LRUCacheEntry(new byte[]{2}))));

        assertArrayEquals(new byte[]{2}, cache.get(Bytes.wrap(new byte[]{0})).value);
        assertEquals(cache.overwrites(), 2);
    }

    @Test
    public void shouldEvictEldestEntry() throws Exception {
        cache.put(Bytes.wrap(new byte[]{0}), new LRUCacheEntry(new byte[]{10}));
        cache.put(Bytes.wrap(new byte[]{1}), new LRUCacheEntry(new byte[]{20}));
        cache.put(Bytes.wrap(new byte[]{2}), new LRUCacheEntry(new byte[]{30}));

        cache.evict();
        assertNull(cache.get(Bytes.wrap(new byte[]{0})));
        assertEquals(2, cache.size());
    }

    @Test
    public void shouldFlushDirtEntriesOnEviction() throws Exception {
        final List<ThreadCache.DirtyEntry> flushed = new ArrayList<>();
        cache.put(Bytes.wrap(new byte[]{0}), new LRUCacheEntry(new byte[]{10}, true, 0, 0, 0, ""));
        cache.put(Bytes.wrap(new byte[]{1}), new LRUCacheEntry(new byte[]{20}));
        cache.put(Bytes.wrap(new byte[]{2}), new LRUCacheEntry(new byte[]{30}, true, 0, 0, 0, ""));

        cache.setListener(new ThreadCache.DirtyEntryFlushListener() {
            @Override
            public void apply(final List<ThreadCache.DirtyEntry> dirty) {
                flushed.addAll(dirty);
            }
        });

        cache.evict();

        assertEquals(2, flushed.size());
        assertEquals(Bytes.wrap(new byte[] {0}), flushed.get(0).key());
        assertArrayEquals(new byte[] {10}, flushed.get(0).newValue());
        assertEquals(Bytes.wrap(new byte[] {2}), flushed.get(1).key());
        assertArrayEquals(new byte[] {30}, flushed.get(1).newValue());
        assertEquals(cache.flushes(), 1);
    }

    @Test
    public void shouldGetRangeIteratorOverKeys() throws Exception {
        cache.put(Bytes.wrap(new byte[]{0}), new LRUCacheEntry(new byte[]{10}, true, 0, 0, 0, ""));
        cache.put(Bytes.wrap(new byte[]{1}), new LRUCacheEntry(new byte[]{20}));
        cache.put(Bytes.wrap(new byte[]{2}), new LRUCacheEntry(new byte[]{30}, true, 0, 0, 0, ""));

        final Iterator<Bytes> iterator = cache.keyRange(Bytes.wrap(new byte[]{1}), Bytes.wrap(new byte[]{2}));
        assertEquals(Bytes.wrap(new byte[]{1}), iterator.next());
        assertEquals(Bytes.wrap(new byte[]{2}), iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldGetIteratorOverAllKeys() throws Exception {
        cache.put(Bytes.wrap(new byte[]{0}), new LRUCacheEntry(new byte[]{10}, true, 0, 0, 0, ""));
        cache.put(Bytes.wrap(new byte[]{1}), new LRUCacheEntry(new byte[]{20}));
        cache.put(Bytes.wrap(new byte[]{2}), new LRUCacheEntry(new byte[]{30}, true, 0, 0, 0, ""));

        final Iterator<Bytes> iterator = cache.allKeys();
        assertEquals(Bytes.wrap(new byte[]{0}), iterator.next());
        assertEquals(Bytes.wrap(new byte[]{1}), iterator.next());
        assertEquals(Bytes.wrap(new byte[]{2}), iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldNotThrowNullPointerWhenCacheIsEmptyAndEvictionCalled() throws Exception {
        cache.evict();
    }
}