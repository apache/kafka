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
import org.apache.kafka.streams.state.KeyValueIterator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class InMemoryTransactionBufferTest {

    private NavigableMap<Bytes, byte[]> baseMap;
    private InMemoryTransactionBuffer buffer;

    private static Bytes key(final String k) {
        return Bytes.wrap(k.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] val(final String v) {
        return v.getBytes(StandardCharsets.UTF_8);
    }

    private static String str(final byte[] bytes) {
        return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
    }

    @BeforeEach
    public void setUp() {
        baseMap = new TreeMap<>();
        baseMap.put(key("a"), val("base-a"));
        baseMap.put(key("b"), val("base-b"));
        baseMap.put(key("c"), val("base-c"));
        buffer = new InMemoryTransactionBuffer(baseMap);
    }

    @Test
    public void shouldReturnNullForUnstagedKey() {
        assertNull(buffer.get(key("a")));
        assertNull(buffer.get(key("z")));
    }

    @Test
    public void shouldReturnStagedValue() {
        buffer.stage(key("a"), val("staged-a"));
        final Optional<byte[]> staged = buffer.get(key("a"));
        assertTrue(staged.isPresent());
        assertArrayEquals(val("staged-a"), staged.get());
    }

    @Test
    public void shouldReturnEmptyOptionalForStagedTombstone() {
        buffer.stage(key("a"), null);
        final Optional<byte[]> staged = buffer.get(key("a"));
        assertEquals(Optional.empty(), staged);
    }

    @Test
    public void shouldReturnStagedValueForNewKey() {
        buffer.stage(key("z"), val("staged-z"));
        final Optional<byte[]> staged = buffer.get(key("z"));
        assertTrue(staged.isPresent());
        assertArrayEquals(val("staged-z"), staged.get());
    }

    @Test
    public void shouldReportIsEmpty() {
        assertTrue(buffer.isEmpty());
        buffer.stage(key("x"), val("v"));
        assertFalse(buffer.isEmpty());
    }

    @Test
    public void shouldCommitStagedWritesToBase() {
        buffer.stage(key("a"), val("new-a"));
        buffer.stage(key("d"), val("new-d"));
        buffer.stage(key("b"), null); // delete b

        buffer.commit();

        assertEquals("new-a", str(baseMap.get(key("a"))));
        assertNull(baseMap.get(key("b")));
        assertEquals("base-c", str(baseMap.get(key("c"))));
        assertEquals("new-d", str(baseMap.get(key("d"))));
        assertTrue(buffer.isEmpty());
    }

    @Test
    public void shouldRollbackWithoutAffectingBase() {
        buffer.stage(key("a"), val("new-a"));
        buffer.stage(key("d"), val("new-d"));

        buffer.rollback();

        assertEquals("base-a", str(baseMap.get(key("a"))));
        assertNull(baseMap.get(key("d")));
        assertTrue(buffer.isEmpty());
    }

    @Test
    public void shouldMergeStagedWritesInAllScan() {
        buffer.stage(key("a"), val("staged-a"));
        buffer.stage(key("d"), val("staged-d"));
        buffer.stage(key("b"), null); // tombstone

        try (KeyValueIterator<Bytes, byte[]> iter = buffer.all(true)) {
            final List<String> keys = new ArrayList<>();
            while (iter.hasNext()) {
                keys.add(iter.next().key.toString());
            }
            assertEquals(List.of("a", "c", "d"), keys);
        }
    }

    @Test
    public void shouldMergeStagedWritesInOwnerAllScan() {
        buffer.stage(key("a"), val("staged-a"));
        buffer.stage(key("d"), val("staged-d"));

        try (KeyValueIterator<Bytes, byte[]> iter = buffer.all(true)) {
            final List<String> keys = new ArrayList<>();
            while (iter.hasNext()) {
                keys.add(iter.next().key.toString());
            }
            assertEquals(List.of("a", "b", "c", "d"), keys);
        }
    }

    @Test
    public void shouldMergeStagedWritesInRangeScan() {
        buffer.stage(key("b"), val("staged-b"));

        try (KeyValueIterator<Bytes, byte[]> iter = buffer.range(key("a"), key("c"), true, true)) {
            final List<String> keys = new ArrayList<>();
            final List<String> values = new ArrayList<>();
            while (iter.hasNext()) {
                final KeyValue<Bytes, byte[]> entry = iter.next();
                keys.add(entry.key.toString());
                values.add(str(entry.value));
            }
            assertEquals(List.of("a", "b", "c"), keys);
            assertEquals(List.of("base-a", "staged-b", "base-c"), values);
        }
    }

    @Test
    public void shouldSeeStagedWritesFromAnotherThread() throws Exception {
        buffer.stage(key("x"), val("staged-x"));

        final AtomicReference<Optional<byte[]>> result = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);

        final Thread reader = new Thread(() -> {
            result.set(buffer.get(key("x")));
            latch.countDown();
        });
        reader.start();
        latch.await();

        assertTrue(result.get().isPresent());
        assertArrayEquals(val("staged-x"), result.get().get());
    }

    @Test
    public void shouldScanFromAnotherThread() throws Exception {
        buffer.stage(key("d"), val("staged-d"));

        final AtomicReference<List<String>> result = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);

        final Thread reader = new Thread(() -> {
            try (KeyValueIterator<Bytes, byte[]> iter = buffer.all(true)) {
                final List<String> keys = new ArrayList<>();
                while (iter.hasNext()) {
                    keys.add(iter.next().key.toString());
                }
                result.set(keys);
            }
            latch.countDown();
        });
        reader.start();
        latch.await();

        assertEquals(List.of("a", "b", "c", "d"), result.get());
    }

    @Test
    public void shouldNotShowStagedWritesInBaseAfterRollback() {
        buffer.stage(key("x"), val("staged-x"));
        buffer.rollback();

        assertNull(baseMap.get(key("x")));
        assertNull(buffer.get(key("x")));
    }

}
