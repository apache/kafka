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

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

public class NamedCacheTest {

    private final Headers headers = new RecordHeaders(new Header[]{new RecordHeader("key", "value".getBytes())});
    private NamedCache cache;

    @Before
    public void setUp() {
        final Metrics innerMetrics = new Metrics();
        final StreamsMetricsImpl metrics = new MockStreamsMetrics(innerMetrics);
        cache = new NamedCache("dummy-name", metrics);
    }

    @Test
    public void shouldKeepTrackOfMostRecentlyAndLeastRecentlyUsed() {
        final List<KeyValue<String, String>> toInsert = Arrays.asList(
                new KeyValue<>("K1", "V1"),
                new KeyValue<>("K2", "V2"),
                new KeyValue<>("K3", "V3"),
                new KeyValue<>("K4", "V4"),
                new KeyValue<>("K5", "V5"));
        for (final KeyValue<String, String> stringStringKeyValue : toInsert) {
            final byte[] key = stringStringKeyValue.key.getBytes();
            final byte[] value = stringStringKeyValue.value.getBytes();
            cache.put(Bytes.wrap(key),
                new LRUCacheEntry(value, new RecordHeaders(), true, 1, 1, 1, ""));
            final LRUCacheEntry head = cache.first();
            final LRUCacheEntry tail = cache.last();
            assertEquals(new String(head.value()), stringStringKeyValue.value);
            assertEquals(new String(tail.value()), toInsert.get(0).value);
            assertEquals(cache.flushes(), 0);
            assertEquals(cache.hits(), 0);
            assertEquals(cache.misses(), 0);
            assertEquals(cache.overwrites(), 0);
        }
    }

    @Test
    public void shouldKeepTrackOfSize() {
        final LRUCacheEntry value = new LRUCacheEntry(new byte[]{0});
        cache.put(Bytes.wrap(new byte[]{0}), value);
        cache.put(Bytes.wrap(new byte[]{1}), value);
        cache.put(Bytes.wrap(new byte[]{2}), value);
        final long size = cache.sizeInBytes();
        // 1 byte key + 24 bytes overhead
        assertEquals((value.size() + 25) * 3, size);
    }

    @Test
    public void shouldPutGet() {
        cache.put(Bytes.wrap(new byte[]{0}), new LRUCacheEntry(new byte[]{10}));
        cache.put(Bytes.wrap(new byte[]{1}), new LRUCacheEntry(new byte[]{11}));
        cache.put(Bytes.wrap(new byte[]{2}), new LRUCacheEntry(new byte[]{12}));

        assertArrayEquals(new byte[] {10}, cache.get(Bytes.wrap(new byte[] {0})).value());
        assertArrayEquals(new byte[] {11}, cache.get(Bytes.wrap(new byte[] {1})).value());
        assertArrayEquals(new byte[] {12}, cache.get(Bytes.wrap(new byte[] {2})).value());
        assertEquals(cache.hits(), 3);
    }

    @Test
    public void shouldPutIfAbsent() {
        cache.put(Bytes.wrap(new byte[]{0}), new LRUCacheEntry(new byte[]{10}));
        cache.putIfAbsent(Bytes.wrap(new byte[]{0}), new LRUCacheEntry(new byte[]{20}));
        cache.putIfAbsent(Bytes.wrap(new byte[]{1}), new LRUCacheEntry(new byte[]{30}));

        assertArrayEquals(new byte[] {10}, cache.get(Bytes.wrap(new byte[] {0})).value());
        assertArrayEquals(new byte[] {30}, cache.get(Bytes.wrap(new byte[] {1})).value());
    }

    @Test
    public void shouldDeleteAndUpdateSize() {
        cache.put(Bytes.wrap(new byte[]{0}), new LRUCacheEntry(new byte[]{10}));
        final LRUCacheEntry deleted = cache.delete(Bytes.wrap(new byte[]{0}));
        assertArrayEquals(new byte[] {10}, deleted.value());
        assertEquals(0, cache.sizeInBytes());
    }

    @Test
    public void shouldPutAll() {
        cache.putAll(Arrays.asList(KeyValue.pair(new byte[] {0}, new LRUCacheEntry(new byte[]{0})),
                                   KeyValue.pair(new byte[] {1}, new LRUCacheEntry(new byte[]{1})),
                                   KeyValue.pair(new byte[] {2}, new LRUCacheEntry(new byte[]{2}))));

        assertArrayEquals(new byte[]{0}, cache.get(Bytes.wrap(new byte[]{0})).value());
        assertArrayEquals(new byte[]{1}, cache.get(Bytes.wrap(new byte[]{1})).value());
        assertArrayEquals(new byte[]{2}, cache.get(Bytes.wrap(new byte[]{2})).value());
    }

    @Test
    public void shouldOverwriteAll() {
        cache.putAll(Arrays.asList(KeyValue.pair(new byte[] {0}, new LRUCacheEntry(new byte[]{0})),
            KeyValue.pair(new byte[] {0}, new LRUCacheEntry(new byte[]{1})),
            KeyValue.pair(new byte[] {0}, new LRUCacheEntry(new byte[]{2}))));

        assertArrayEquals(new byte[]{2}, cache.get(Bytes.wrap(new byte[]{0})).value());
        assertEquals(cache.overwrites(), 2);
    }

    @Test
    public void shouldEvictEldestEntry() {
        cache.put(Bytes.wrap(new byte[]{0}), new LRUCacheEntry(new byte[]{10}));
        cache.put(Bytes.wrap(new byte[]{1}), new LRUCacheEntry(new byte[]{20}));
        cache.put(Bytes.wrap(new byte[]{2}), new LRUCacheEntry(new byte[]{30}));

        cache.evict();
        assertNull(cache.get(Bytes.wrap(new byte[]{0})));
        assertEquals(2, cache.size());
    }

    @Test
    public void shouldFlushDirtEntriesOnEviction() {
        final List<ThreadCache.DirtyEntry> flushed = new ArrayList<>();
        cache.put(Bytes.wrap(new byte[]{0}), new LRUCacheEntry(new byte[]{10}, headers, true, 0, 0, 0, ""));
        cache.put(Bytes.wrap(new byte[]{1}), new LRUCacheEntry(new byte[]{20}));
        cache.put(Bytes.wrap(new byte[]{2}), new LRUCacheEntry(new byte[]{30}, headers, true, 0, 0, 0, ""));

        cache.setListener(flushed::addAll);

        cache.evict();

        assertEquals(2, flushed.size());
        assertEquals(Bytes.wrap(new byte[] {0}), flushed.get(0).key());
        assertEquals(headers, flushed.get(0).entry().context().headers());
        assertArrayEquals(new byte[] {10}, flushed.get(0).newValue());
        assertEquals(Bytes.wrap(new byte[] {2}), flushed.get(1).key());
        assertArrayEquals(new byte[] {30}, flushed.get(1).newValue());
        assertEquals(cache.flushes(), 1);
    }

    @Test
    public void shouldNotThrowNullPointerWhenCacheIsEmptyAndEvictionCalled() {
        cache.evict();
    }

    @Test
    public void shouldThrowIllegalStateExceptionWhenTryingToOverwriteDirtyEntryWithCleanEntry() {
        cache.put(Bytes.wrap(new byte[]{0}), new LRUCacheEntry(new byte[]{10}, headers, true, 0, 0, 0, ""));
        assertThrows(IllegalStateException.class, () -> cache.put(Bytes.wrap(new byte[]{0}),
            new LRUCacheEntry(new byte[]{10}, new RecordHeaders(), false, 0, 0, 0, "")));
    }

    @Test
    public void shouldRemoveDeletedValuesOnFlush() {
        cache.setListener(dirty -> { /* no-op */ });
        cache.put(Bytes.wrap(new byte[]{0}), new LRUCacheEntry(null, headers, true, 0, 0, 0, ""));
        cache.put(Bytes.wrap(new byte[]{1}), new LRUCacheEntry(new byte[]{20}, new RecordHeaders(), true, 0, 0, 0, ""));
        cache.flush();
        assertEquals(1, cache.size());
        assertNotNull(cache.get(Bytes.wrap(new byte[]{1})));
    }

    @Test
    public void shouldBeReentrantAndNotBreakLRU() {
        final LRUCacheEntry dirty = new LRUCacheEntry(new byte[]{3}, new RecordHeaders(), true, 0, 0, 0, "");
        final LRUCacheEntry clean = new LRUCacheEntry(new byte[]{3});
        cache.put(Bytes.wrap(new byte[]{0}), dirty);
        cache.put(Bytes.wrap(new byte[]{1}), clean);
        cache.put(Bytes.wrap(new byte[]{2}), clean);
        assertEquals(3 * cache.head().size(), cache.sizeInBytes());
        cache.setListener(dirty1 -> {
            cache.put(Bytes.wrap(new byte[]{3}), clean);
            // evict key 1
            cache.evict();
            // evict key 2
            cache.evict();
        });

        assertEquals(3 * cache.head().size(), cache.sizeInBytes());
        // Evict key 0
        cache.evict();
        final Bytes entryFour = Bytes.wrap(new byte[]{4});
        cache.put(entryFour, dirty);

        // check that the LRU is still correct
        final NamedCache.LRUNode head = cache.head();
        final NamedCache.LRUNode tail = cache.tail();
        assertEquals(2, cache.size());
        assertEquals(2 * head.size(), cache.sizeInBytes());
        // dirty should be the newest
        assertEquals(entryFour, head.key());
        assertEquals(Bytes.wrap(new byte[] {3}), tail.key());
        assertSame(tail, head.next());
        assertNull(head.previous());
        assertSame(head, tail.previous());
        assertNull(tail.next());

        // evict key 3
        cache.evict();
        assertSame(cache.head(), cache.tail());
        assertEquals(entryFour, cache.head().key());
        assertNull(cache.head().next());
        assertNull(cache.head().previous());
    }

    @Test
    public void shouldNotThrowIllegalArgumentAfterEvictingDirtyRecordAndThenPuttingNewRecordWithSameKey() {
        final LRUCacheEntry dirty = new LRUCacheEntry(new byte[]{3}, new RecordHeaders(), true, 0, 0, 0, "");
        final LRUCacheEntry clean = new LRUCacheEntry(new byte[]{3});
        final Bytes key = Bytes.wrap(new byte[] {3});
        cache.setListener(dirty1 -> cache.put(key, clean));
        cache.put(key, dirty);
        cache.evict();
    }

    @Test
    public void shouldReturnNullIfKeyIsNull() {
        assertNull(cache.get(null));
    }
}