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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ThreadCacheRawRecordTest {
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
            entry.context().freeRawRecord();
            assertSame(shared, otherEntry.context().sourceRawValue());
            otherEntry.context().freeRawRecord();
            cache.delete(namespace, second);
            assertEquals(0L, cache.sizeBytes());
            cache.close(namespace);
        }
    }

    @Test
    void shouldKeepCleanRawReferencesAfterFlushUntilDeletion() {
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
            assertSame(raw, cache.get(namespace, key).context().sourceRawValue());
            assertEquals(beforeFlush, cache.sizeBytes());
            cache.flush(namespace);
            assertEquals(1, forwarded[0]);
            cache.delete(namespace, key);
            assertEquals(0L, cache.sizeBytes());
            cache.close(namespace);
        }
    }
}
