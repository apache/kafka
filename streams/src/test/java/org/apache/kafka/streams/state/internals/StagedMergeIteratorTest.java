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

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.KeyValueIterator;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StagedMergeIteratorTest {

    private static Bytes key(final String k) {
        return Bytes.wrap(k.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] val(final String v) {
        return v.getBytes(StandardCharsets.UTF_8);
    }

    private static String str(final byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    @Test
    public void shouldMergeDisjointStagingAndBase() {
        final NavigableMap<Bytes, Optional<byte[]>> staging = new TreeMap<>();
        staging.put(key("a"), Optional.of(val("staged-a")));
        staging.put(key("c"), Optional.of(val("staged-c")));

        final List<KeyValue<Bytes, byte[]>> baseEntries = List.of(
            new KeyValue<>(key("b"), val("base-b")),
            new KeyValue<>(key("d"), val("base-d"))
        );

        try (KeyValueIterator<Bytes, byte[]> iter = new StagedMergeIterator(staging, new ListIterator(baseEntries))) {
            final List<String> keys = new ArrayList<>();
            while (iter.hasNext()) {
                keys.add(iter.next().key.toString());
            }
            assertEquals(List.of("a", "b", "c", "d"), keys);
        }
    }

    @Test
    public void shouldPreferStagingOverBaseForSameKey() {
        final NavigableMap<Bytes, Optional<byte[]>> staging = new TreeMap<>();
        staging.put(key("a"), Optional.of(val("staged-a")));

        final List<KeyValue<Bytes, byte[]>> baseEntries = List.of(
            new KeyValue<>(key("a"), val("base-a"))
        );

        try (KeyValueIterator<Bytes, byte[]> iter = new StagedMergeIterator(staging, new ListIterator(baseEntries))) {
            assertTrue(iter.hasNext());
            final KeyValue<Bytes, byte[]> entry = iter.next();
            assertEquals("a", entry.key.toString());
            assertEquals("staged-a", str(entry.value));
            assertFalse(iter.hasNext());
        }
    }

    @Test
    public void shouldThrowInvalidStateStoreExceptionOnHasNextAfterClose() {
        final NavigableMap<Bytes, Optional<byte[]>> staging = new TreeMap<>();
        staging.put(key("a"), Optional.of(val("staged-a")));

        final KeyValueIterator<Bytes, byte[]> iter =
            new StagedMergeIterator(staging, new ListIterator(List.of()));
        iter.close();

        // A closed store iterator must signal via InvalidStateStoreException -- the type
        // SegmentIterator catches to close open iterators gracefully -- not a bare
        // IllegalStateException, which would escape that handling.
        assertThrows(InvalidStateStoreException.class, iter::hasNext);
        assertThrows(InvalidStateStoreException.class, iter::next);
        assertThrows(InvalidStateStoreException.class, iter::peekNextKey);
    }

    @Test
    public void shouldSkipTombstones() {
        final NavigableMap<Bytes, Optional<byte[]>> staging = new TreeMap<>();
        staging.put(key("a"), Optional.empty()); // tombstone
        staging.put(key("c"), Optional.of(val("staged-c")));

        final List<KeyValue<Bytes, byte[]>> baseEntries = List.of(
            new KeyValue<>(key("b"), val("base-b"))
        );

        try (KeyValueIterator<Bytes, byte[]> iter = new StagedMergeIterator(staging, new ListIterator(baseEntries))) {
            final List<String> keys = new ArrayList<>();
            while (iter.hasNext()) {
                keys.add(iter.next().key.toString());
            }
            assertEquals(List.of("b", "c"), keys);
        }
    }

    @Test
    public void shouldSkipBaseKeyWhenStagingHasTombstoneForSameKey() {
        final NavigableMap<Bytes, Optional<byte[]>> staging = new TreeMap<>();
        staging.put(key("b"), Optional.empty()); // tombstone for key in base

        final List<KeyValue<Bytes, byte[]>> baseEntries = List.of(
            new KeyValue<>(key("a"), val("base-a")),
            new KeyValue<>(key("b"), val("base-b")),
            new KeyValue<>(key("c"), val("base-c"))
        );

        try (KeyValueIterator<Bytes, byte[]> iter = new StagedMergeIterator(staging, new ListIterator(baseEntries))) {
            final List<String> keys = new ArrayList<>();
            while (iter.hasNext()) {
                keys.add(iter.next().key.toString());
            }
            assertEquals(List.of("a", "c"), keys);
        }
    }

    @Test
    public void shouldHandleEmptyStaging() {
        final NavigableMap<Bytes, Optional<byte[]>> staging = new TreeMap<>();

        final List<KeyValue<Bytes, byte[]>> baseEntries = List.of(
            new KeyValue<>(key("a"), val("base-a")),
            new KeyValue<>(key("b"), val("base-b"))
        );

        try (KeyValueIterator<Bytes, byte[]> iter = new StagedMergeIterator(staging, new ListIterator(baseEntries))) {
            final List<String> keys = new ArrayList<>();
            while (iter.hasNext()) {
                keys.add(iter.next().key.toString());
            }
            assertEquals(List.of("a", "b"), keys);
        }
    }

    @Test
    public void shouldHandleEmptyBase() {
        final NavigableMap<Bytes, Optional<byte[]>> staging = new TreeMap<>();
        staging.put(key("a"), Optional.of(val("staged-a")));
        staging.put(key("b"), Optional.of(val("staged-b")));

        try (KeyValueIterator<Bytes, byte[]> iter = new StagedMergeIterator(staging, new ListIterator(List.of()))) {
            final List<String> keys = new ArrayList<>();
            while (iter.hasNext()) {
                keys.add(iter.next().key.toString());
            }
            assertEquals(List.of("a", "b"), keys);
        }
    }

    @Test
    public void shouldHandleBothEmpty() {
        final NavigableMap<Bytes, Optional<byte[]>> staging = new TreeMap<>();

        try (KeyValueIterator<Bytes, byte[]> iter = new StagedMergeIterator(staging, new ListIterator(List.of()))) {
            assertFalse(iter.hasNext());
        }
    }

    @Test
    public void shouldHandleAllTombstones() {
        final NavigableMap<Bytes, Optional<byte[]>> staging = new TreeMap<>();
        staging.put(key("a"), Optional.empty());
        staging.put(key("b"), Optional.empty());

        final List<KeyValue<Bytes, byte[]>> baseEntries = List.of(
            new KeyValue<>(key("a"), val("base-a")),
            new KeyValue<>(key("b"), val("base-b"))
        );

        try (KeyValueIterator<Bytes, byte[]> iter = new StagedMergeIterator(staging, new ListIterator(baseEntries))) {
            assertFalse(iter.hasNext());
        }
    }

    @Test
    public void shouldSupportPeekNextKey() {
        final NavigableMap<Bytes, Optional<byte[]>> staging = new TreeMap<>();
        staging.put(key("b"), Optional.of(val("staged-b")));

        final List<KeyValue<Bytes, byte[]>> baseEntries = List.of(
            new KeyValue<>(key("a"), val("base-a"))
        );

        try (KeyValueIterator<Bytes, byte[]> iter = new StagedMergeIterator(staging, new ListIterator(baseEntries))) {
            assertEquals(key("a"), iter.peekNextKey());
            iter.next(); // consume a
            assertEquals(key("b"), iter.peekNextKey());
            iter.next(); // consume b
            assertFalse(iter.hasNext());
        }
    }

    @Test
    public void shouldThrowOnNextWhenExhausted() {
        final NavigableMap<Bytes, Optional<byte[]>> staging = new TreeMap<>();

        try (KeyValueIterator<Bytes, byte[]> iter = new StagedMergeIterator(staging, new ListIterator(List.of()))) {
            assertThrows(NoSuchElementException.class, iter::next);
        }
    }

    @Test
    public void shouldHandleConsecutiveTombstonesInStaging() {
        final NavigableMap<Bytes, Optional<byte[]>> staging = new TreeMap<>();
        staging.put(key("a"), Optional.empty());
        staging.put(key("b"), Optional.empty());
        staging.put(key("c"), Optional.empty());
        staging.put(key("d"), Optional.of(val("staged-d")));

        try (KeyValueIterator<Bytes, byte[]> iter = new StagedMergeIterator(staging, new ListIterator(List.of()))) {
            assertTrue(iter.hasNext());
            assertEquals("d", iter.next().key.toString());
            assertFalse(iter.hasNext());
        }
    }

    @Test
    public void shouldMaintainSortOrderWithInterleavedEntries() {
        final NavigableMap<Bytes, Optional<byte[]>> staging = new TreeMap<>();
        staging.put(key("b"), Optional.of(val("staged-b")));
        staging.put(key("d"), Optional.of(val("staged-d")));
        staging.put(key("f"), Optional.of(val("staged-f")));

        final List<KeyValue<Bytes, byte[]>> baseEntries = List.of(
            new KeyValue<>(key("a"), val("base-a")),
            new KeyValue<>(key("c"), val("base-c")),
            new KeyValue<>(key("e"), val("base-e")),
            new KeyValue<>(key("g"), val("base-g"))
        );

        try (KeyValueIterator<Bytes, byte[]> iter = new StagedMergeIterator(staging, new ListIterator(baseEntries))) {
            final List<String> keys = new ArrayList<>();
            while (iter.hasNext()) {
                keys.add(iter.next().key.toString());
            }
            assertEquals(List.of("a", "b", "c", "d", "e", "f", "g"), keys);
        }
    }

    @Test
    public void shouldReverseMergeDisjointStagingAndBase() {
        final NavigableMap<Bytes, Optional<byte[]>> staging = new TreeMap<>();
        staging.put(key("a"), Optional.of(val("staged-a")));
        staging.put(key("c"), Optional.of(val("staged-c")));

        final List<KeyValue<Bytes, byte[]>> baseEntries = List.of(
            new KeyValue<>(key("d"), val("base-d")),
            new KeyValue<>(key("b"), val("base-b"))
        );

        try (KeyValueIterator<Bytes, byte[]> iter = new StagedMergeIterator(staging, new ListIterator(baseEntries), false)) {
            final List<String> keys = new ArrayList<>();
            while (iter.hasNext()) {
                keys.add(iter.next().key.toString());
            }
            assertEquals(List.of("d", "c", "b", "a"), keys);
        }
    }

    @Test
    public void shouldReversePreferStagingOverBaseForSameKey() {
        final NavigableMap<Bytes, Optional<byte[]>> staging = new TreeMap<>();
        staging.put(key("a"), Optional.of(val("staged-a")));

        final List<KeyValue<Bytes, byte[]>> baseEntries = List.of(
            new KeyValue<>(key("a"), val("base-a"))
        );

        try (KeyValueIterator<Bytes, byte[]> iter = new StagedMergeIterator(staging, new ListIterator(baseEntries), false)) {
            assertTrue(iter.hasNext());
            final KeyValue<Bytes, byte[]> entry = iter.next();
            assertEquals("a", entry.key.toString());
            assertEquals("staged-a", str(entry.value));
            assertFalse(iter.hasNext());
        }
    }

    @Test
    public void shouldReverseSkipTombstones() {
        final NavigableMap<Bytes, Optional<byte[]>> staging = new TreeMap<>();
        staging.put(key("c"), Optional.empty()); // tombstone
        staging.put(key("a"), Optional.of(val("staged-a")));

        final List<KeyValue<Bytes, byte[]>> baseEntries = List.of(
            new KeyValue<>(key("b"), val("base-b"))
        );

        try (KeyValueIterator<Bytes, byte[]> iter = new StagedMergeIterator(staging, new ListIterator(baseEntries), false)) {
            final List<String> keys = new ArrayList<>();
            while (iter.hasNext()) {
                keys.add(iter.next().key.toString());
            }
            assertEquals(List.of("b", "a"), keys);
        }
    }

    @Test
    public void shouldReverseSkipBaseKeyWhenStagingHasTombstoneForSameKey() {
        final NavigableMap<Bytes, Optional<byte[]>> staging = new TreeMap<>();
        staging.put(key("b"), Optional.empty()); // tombstone for key in base

        final List<KeyValue<Bytes, byte[]>> baseEntries = List.of(
            new KeyValue<>(key("c"), val("base-c")),
            new KeyValue<>(key("b"), val("base-b")),
            new KeyValue<>(key("a"), val("base-a"))
        );

        try (KeyValueIterator<Bytes, byte[]> iter = new StagedMergeIterator(staging, new ListIterator(baseEntries), false)) {
            final List<String> keys = new ArrayList<>();
            while (iter.hasNext()) {
                keys.add(iter.next().key.toString());
            }
            assertEquals(List.of("c", "a"), keys);
        }
    }

    /**
     * Simple list-backed KeyValueIterator for testing.
     */
    private static class ListIterator implements KeyValueIterator<Bytes, byte[]> {
        private final List<KeyValue<Bytes, byte[]>> entries;
        private int index = 0;

        ListIterator(final List<KeyValue<Bytes, byte[]>> entries) {
            this.entries = new ArrayList<>(entries);
        }

        @Override
        public boolean hasNext() {
            return index < entries.size();
        }

        @Override
        public KeyValue<Bytes, byte[]> next() {
            if (!hasNext()) throw new NoSuchElementException();
            return entries.get(index++);
        }

        @Override
        public Bytes peekNextKey() {
            if (!hasNext()) throw new NoSuchElementException();
            return entries.get(index).key;
        }

        @Override
        public void close() {
            // no-op
        }
    }
}
