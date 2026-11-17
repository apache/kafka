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
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ThreadCacheRawRecordTest {
    @Test
    void shouldCountNullableAndEmptyRawArrays() {
        final byte[] value = {1};
        final long baseSize = new LRUCacheEntry(value, new RecordHeaders(), true,
            0L, 0L, 0, "", null, null).size();
        for (final byte[] rawKey : new byte[][] {null, new byte[0], new byte[7]}) {
            for (final byte[] rawValue : new byte[][] {null, new byte[0], new byte[1024]}) {
                final LRUCacheEntry entry = new LRUCacheEntry(value, new RecordHeaders(), true,
                    0L, 0L, 0, "", rawKey, rawValue);
                assertEquals(baseSize + (rawKey == null ? 0 : rawKey.length)
                    + (rawValue == null ? 0 : rawValue.length), entry.size());
            }
        }
    }

    @Test
    void shouldEvictWhenRawRecordsExceedBudget() {
        final String namespace = "0.0-raw-record";
        final long budget = 4096L;
        try (final Metrics metrics = new Metrics()) {
            final ThreadCache cache = new ThreadCache(new LogContext("testCache "),
                budget, new MockStreamsMetrics(metrics));
            cache.addDirtyEntryFlushListener(namespace, entries -> { });
            for (int i = 0; i < 8; i++) {
                cache.put(namespace, Bytes.wrap(new byte[] {(byte) i}),
                    new LRUCacheEntry(new byte[1], new RecordHeaders(), true,
                        i, i, 0, "", new byte[1], new byte[1024]));
            }
            // The transformed values are tiny, but each entry retains the source payload.
            assertTrue(cache.evicts() > 0);
            assertTrue(cache.sizeBytes() <= budget);
            cache.close(namespace);
            assertEquals(0L, cache.sizeBytes());
        }
    }

    @Test
    void shouldConservativelyCountSharedArraysAndBalanceReplacementAndDeletion() {
        final byte[] shared = new byte[1024];
        final Bytes first = Bytes.wrap(new byte[] {1});
        final Bytes second = Bytes.wrap(new byte[] {2});
        final String namespace = "0_0-shared";
        try (final Metrics metrics = new Metrics()) {
            final ThreadCache cache = new ThreadCache(new LogContext(), 100_000L, new MockStreamsMetrics(metrics));
            final LRUCacheEntry entry = new LRUCacheEntry(shared, new RecordHeaders(), true,
                0L, 0L, 0, "", shared, shared);
            final LRUCacheEntry withoutRaw = new LRUCacheEntry(shared, new RecordHeaders(), true,
                0L, 0L, 0, "", null, null);
            assertEquals(withoutRaw.size() + 2L * shared.length, entry.size());
            cache.put(namespace, first, entry);
            final long oneEntrySize = cache.sizeBytes();
            final LRUCacheEntry otherEntry = new LRUCacheEntry(shared, new RecordHeaders(), true,
                0L, 0L, 0, "", shared, shared);
            cache.put(namespace, second, otherEntry);
            assertEquals(2L * oneEntrySize, cache.sizeBytes());
            cache.put(namespace, first, withoutRaw);
            assertEquals(2L * oneEntrySize - 2L * shared.length, cache.sizeBytes());
            cache.delete(namespace, first);
            assertEquals(oneEntrySize, cache.sizeBytes());
            // Accounting is a construction-time snapshot even if the raw references are released.
            otherEntry.context().freeRawRecord();
            cache.delete(namespace, second);
            assertEquals(0L, cache.sizeBytes());
            cache.close(namespace);
        }
    }

    @Test
    void shouldReleaseCleanRawReferencesAfterFlush() {
        final byte[] raw = new byte[1024];
        final Bytes key = Bytes.wrap(new byte[] {1});
        final String namespace = "0_0-flush";
        try (final Metrics metrics = new Metrics()) {
            final ThreadCache cache = new ThreadCache(new LogContext(), 100_000L, new MockStreamsMetrics(metrics));
            final LRUCacheEntry entry = new LRUCacheEntry(new byte[] {7}, new RecordHeaders(), true,
                0L, 0L, 0, "", null, raw);
            final int[] forwarded = {0};
            cache.addDirtyEntryFlushListener(namespace, entries -> {
                forwarded[0] += entries.size();
                assertSame(raw, entries.get(0).entry().context().sourceRawValue());
            });
            cache.put(namespace, key, entry);
            final long beforeFlush = cache.sizeBytes();
            cache.flush(namespace);
            assertEquals(1, forwarded[0]);
            assertFalse(entry.isDirty());
            assertNull(cache.get(namespace, key).context().sourceRawValue());
            assertEquals(beforeFlush - raw.length, cache.sizeBytes());
            cache.flush(namespace);
            assertEquals(1, forwarded[0]);
            cache.delete(namespace, key);
            assertEquals(0L, cache.sizeBytes());
            cache.close(namespace);
        }
    }
    @Test
    void shouldRefundRawBytesAlreadyReleasedByListener() {
        final String namespace = "0_0-sink";
        final Bytes key = Bytes.wrap(new byte[] {1});
        try (final Metrics metrics = new Metrics()) {
            final ThreadCache cache = new ThreadCache(new LogContext(), 100_000L, new MockStreamsMetrics(metrics));
            final LRUCacheEntry entry = rawEntry(new byte[] {7});
            cache.addDirtyEntryFlushListener(namespace, entries -> entry.context().freeRawRecord());
            cache.put(namespace, key, entry);
            final long charged = cache.sizeBytes();
            cache.flush(namespace);
            assertEquals(charged - 1031L, cache.sizeBytes());
            cache.flush(namespace);
            assertEquals(charged - 1031L, cache.sizeBytes());
            cache.delete(namespace, key);
            assertEquals(0L, cache.sizeBytes());
            cache.close(namespace);
        }
    }

    @Test
    void shouldPreserveReentrantReplacementOfFlushedTombstone() {
        final String namespace = "0_0-replacement";
        final Bytes key = Bytes.wrap(new byte[] {1});
        try (final Metrics metrics = new Metrics()) {
            final ThreadCache cache = new ThreadCache(new LogContext(), 100_000L, new MockStreamsMetrics(metrics));
            final LRUCacheEntry replacement = rawEntry(new byte[] {9});
            cache.addDirtyEntryFlushListener(namespace, entries -> cache.put(namespace, key, replacement));
            cache.put(namespace, key, rawEntry(null));
            cache.flush(namespace);
            assertSame(replacement, cache.get(namespace, key));
            assertTrue(replacement.isDirty());
            assertEquals(1024, replacement.context().sourceRawValue().length);
            assertEquals(new NamedCache.LRUNode(key, replacement).size(), cache.sizeBytes());
            cache.delete(namespace, key);
            assertEquals(0L, cache.sizeBytes());
            cache.close(namespace);
        }
    }

    @Test
    void shouldBalanceReentrantDeleteAndCrossNamespacePut() {
        final String namespace = "0_0-delete";
        final String other = "0_0-other";
        final Bytes key = Bytes.wrap(new byte[] {1});
        try (final Metrics metrics = new Metrics()) {
            final ThreadCache cache = new ThreadCache(new LogContext(), 100_000L, new MockStreamsMetrics(metrics));
            final LRUCacheEntry replacement = rawEntry(new byte[] {9});
            cache.addDirtyEntryFlushListener(namespace, entries -> {
                cache.delete(namespace, key);
                cache.put(other, key, replacement);
            });
            cache.put(namespace, key, rawEntry(new byte[] {7}));
            cache.flush(namespace);
            assertNull(cache.get(namespace, key));
            assertEquals(new NamedCache.LRUNode(key, replacement).size(), cache.sizeBytes());
            cache.close(namespace);
            cache.close(other);
            assertEquals(0L, cache.sizeBytes());
        }
    }

    @Test
    void shouldBalanceNestedFlushWithoutDoubleRefund() {
        final String namespace = "0_0-nested";
        final Bytes key = Bytes.wrap(new byte[] {1});
        try (final Metrics metrics = new Metrics()) {
            final ThreadCache cache = new ThreadCache(new LogContext(), 100_000L, new MockStreamsMetrics(metrics));
            final LRUCacheEntry replacement = rawEntry(new byte[] {9});
            final int[] calls = {0};
            cache.addDirtyEntryFlushListener(namespace, entries -> {
                if (++calls[0] == 1) {
                    cache.put(namespace, key, replacement);
                    cache.flush(namespace);
                }
            });
            cache.put(namespace, key, rawEntry(new byte[] {7}));
            cache.flush(namespace);
            assertEquals(2, calls[0]);
            assertSame(replacement, cache.get(namespace, key));
            assertNull(replacement.context().sourceRawValue());
            assertEquals(new NamedCache.LRUNode(key, replacement).size(), cache.sizeBytes());
            cache.close(namespace);
            assertEquals(0L, cache.sizeBytes());
        }
    }

    @Test
    void shouldKeepRawBytesWhenFlushFails() {
        final String namespace = "0_0-failed";
        final Bytes key = Bytes.wrap(new byte[] {1});
        try (final Metrics metrics = new Metrics()) {
            final ThreadCache cache = new ThreadCache(new LogContext(), 100_000L, new MockStreamsMetrics(metrics));
            final LRUCacheEntry entry = rawEntry(new byte[] {7});
            final RuntimeException failure = new RuntimeException("flush failed");
            cache.addDirtyEntryFlushListener(namespace, entries -> {
                throw failure;
            });
            cache.put(namespace, key, entry);
            final long charged = cache.sizeBytes();
            assertSame(failure, assertThrows(RuntimeException.class, () -> cache.flush(namespace)));
            assertEquals(1024, entry.context().sourceRawValue().length);
            assertEquals(charged, cache.sizeBytes());
            cache.close(namespace);
            assertEquals(0L, cache.sizeBytes());
        }
    }

    @Test
    void shouldAccountForEvictedEntryWhenListenerThrows() {
        final String namespace = "0_0-evict-failed";
        final Bytes key = Bytes.wrap(new byte[] {1});
        try (final Metrics metrics = new Metrics()) {
            final ThreadCache cache = new ThreadCache(new LogContext(), 1L, new MockStreamsMetrics(metrics));
            cache.addDirtyEntryFlushListener(namespace, entries -> {
                throw new RuntimeException("flush failed");
            });
            assertThrows(RuntimeException.class, () -> cache.put(namespace, key, rawEntry(new byte[] {7})));
            assertNull(cache.get(namespace, key));
            assertEquals(0L, cache.sizeBytes());
            cache.close(namespace);
        }
    }

    @Test
    void shouldBalanceSharedEntryInstanceAfterFlush() {
        final String namespace = "0_0-shared-entry";
        final Bytes first = Bytes.wrap(new byte[] {1});
        final Bytes second = Bytes.wrap(new byte[] {2});
        try (final Metrics metrics = new Metrics()) {
            final ThreadCache cache = new ThreadCache(new LogContext(), 100_000L, new MockStreamsMetrics(metrics));
            final LRUCacheEntry entry = rawEntry(new byte[] {7});
            cache.addDirtyEntryFlushListener(namespace, entries -> { });
            cache.put(namespace, first, entry);
            cache.put(namespace, second, entry);
            final long charged = cache.sizeBytes();
            cache.flush(namespace);
            assertEquals(charged - 2L * 1031L, cache.sizeBytes());
            cache.delete(namespace, first);
            cache.delete(namespace, second);
            assertEquals(0L, cache.sizeBytes());
            cache.close(namespace);
        }
    }

    @Test
    void shouldRefundOnlyResidentEntriesDuringEvictionFlush() {
        final String namespace = "0_0-evict";
        final Bytes first = Bytes.wrap(new byte[] {1});
        final Bytes second = Bytes.wrap(new byte[] {2});
        try (final Metrics metrics = new Metrics()) {
            final ThreadCache cache = new ThreadCache(new LogContext(), 1500L, new MockStreamsMetrics(metrics));
            final LRUCacheEntry remaining = rawEntry(new byte[] {9});
            cache.addDirtyEntryFlushListener(namespace, entries -> { });
            cache.put(namespace, first, rawEntry(new byte[] {7}));
            cache.put(namespace, second, remaining);
            assertNull(cache.get(namespace, first));
            assertSame(remaining, cache.get(namespace, second));
            assertNull(remaining.context().sourceRawValue());
            assertEquals(new NamedCache.LRUNode(second, remaining).size(), cache.sizeBytes());
            cache.close(namespace);
            assertEquals(0L, cache.sizeBytes());
        }
    }

    @Test
    void shouldPreserveReentrantChangesWhenListenerThrows() {
        final String namespace = "0_0-partial-failure";
        final Bytes key = Bytes.wrap(new byte[] {1});
        try (final Metrics metrics = new Metrics()) {
            final ThreadCache cache = new ThreadCache(new LogContext(), 100_000L, new MockStreamsMetrics(metrics));
            final LRUCacheEntry replacement = rawEntry(new byte[] {9});
            cache.addDirtyEntryFlushListener(namespace, entries -> {
                cache.put(namespace, key, replacement);
                throw new RuntimeException("flush failed after replacement");
            });
            cache.put(namespace, key, rawEntry(new byte[] {7}));
            assertThrows(RuntimeException.class, () -> cache.flush(namespace));
            assertSame(replacement, cache.get(namespace, key));
            assertTrue(replacement.isDirty());
            assertEquals(1024, replacement.context().sourceRawValue().length);
            assertEquals(new NamedCache.LRUNode(key, replacement).size(), cache.sizeBytes());
            cache.close(namespace);
            assertEquals(0L, cache.sizeBytes());
        }
    }

    @Test
    void shouldBalanceReentrantPutThatEvictsFlushedEntry() {
        final String namespace = "0_0-reentrant-evict";
        final Bytes first = Bytes.wrap(new byte[] {1});
        final Bytes second = Bytes.wrap(new byte[] {2});
        try (final Metrics metrics = new Metrics()) {
            final ThreadCache cache = new ThreadCache(new LogContext(), 1500L, new MockStreamsMetrics(metrics));
            final LRUCacheEntry replacement = rawEntry(new byte[] {9});
            cache.addDirtyEntryFlushListener(namespace, entries -> cache.put(namespace, second, replacement));
            cache.put(namespace, first, rawEntry(new byte[] {7}));
            cache.flush(namespace);
            assertNull(cache.get(namespace, first));
            assertSame(replacement, cache.get(namespace, second));
            assertTrue(replacement.isDirty());
            assertEquals(1024, replacement.context().sourceRawValue().length);
            assertEquals(new NamedCache.LRUNode(second, replacement).size(), cache.sizeBytes());
            cache.close(namespace);
            assertEquals(0L, cache.sizeBytes());
        }
    }

    @Test
    void shouldBalanceReentrantClearOrCloseAndRecreate() {
        final String namespace = "0_0-recreate";
        final Bytes key = Bytes.wrap(new byte[] {1});
        for (final boolean close : new boolean[] {false, true}) {
            try (final Metrics metrics = new Metrics()) {
                final ThreadCache cache = new ThreadCache(new LogContext(), 100_000L, new MockStreamsMetrics(metrics));
                final LRUCacheEntry replacement = rawEntry(new byte[] {9});
                cache.addDirtyEntryFlushListener(namespace, entries -> {
                    if (close) {
                        cache.close(namespace);
                    } else {
                        cache.clear(namespace);
                    }
                    cache.put(namespace, key, replacement);
                });
                cache.put(namespace, key, rawEntry(new byte[] {7}));
                cache.flush(namespace);
                assertSame(replacement, cache.get(namespace, key));
                assertTrue(replacement.isDirty());
                assertEquals(1024, replacement.context().sourceRawValue().length);
                assertEquals(new NamedCache.LRUNode(key, replacement).size(), cache.sizeBytes());
                cache.close(namespace);
                assertEquals(0L, cache.sizeBytes());
            }
        }
    }

    @Test
    void shouldPreserveSharedRawArraysInAnotherDirtyNamespace() {
        final String firstNamespace = "0_0-first";
        final String secondNamespace = "0_0-second";
        final Bytes key = Bytes.wrap(new byte[] {1});
        final byte[] rawKey = new byte[7];
        final byte[] rawValue = new byte[1024];
        try (final Metrics metrics = new Metrics()) {
            final ThreadCache cache = new ThreadCache(new LogContext(), 100_000L, new MockStreamsMetrics(metrics));
            final LRUCacheEntry first = new LRUCacheEntry(new byte[] {7}, new RecordHeaders(), true,
                0L, 0L, 0, "", rawKey, rawValue);
            final LRUCacheEntry second = new LRUCacheEntry(new byte[] {9}, new RecordHeaders(), true,
                0L, 0L, 0, "", rawKey, rawValue);
            cache.addDirtyEntryFlushListener(firstNamespace, entries -> { });
            cache.addDirtyEntryFlushListener(secondNamespace, entries -> {
                assertSame(rawKey, entries.get(0).entry().context().sourceRawKey());
                assertSame(rawValue, entries.get(0).entry().context().sourceRawValue());
            });
            cache.put(firstNamespace, key, first);
            cache.put(secondNamespace, key, second);
            cache.flush(firstNamespace);
            assertNull(first.context().sourceRawValue());
            assertTrue(second.isDirty());
            assertSame(rawKey, second.context().sourceRawKey());
            assertSame(rawValue, second.context().sourceRawValue());
            assertEquals(new NamedCache.LRUNode(key, first).size()
                + new NamedCache.LRUNode(key, second).size(), cache.sizeBytes());
            cache.flush(secondNamespace);
            assertNull(second.context().sourceRawValue());
            cache.close(firstNamespace);
            cache.close(secondNamespace);
            assertEquals(0L, cache.sizeBytes());
        }
    }

    private static LRUCacheEntry rawEntry(final byte[] value) {
        return new LRUCacheEntry(value, new RecordHeaders(), true,
            0L, 0L, 0, "", new byte[7], new byte[1024]);
    }
}
