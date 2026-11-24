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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class InMemoryWindowTransactionBufferTest {

    private ConcurrentNavigableMap<Long, ConcurrentNavigableMap<Bytes, byte[]>> segmentMap;
    private InMemoryWindowTransactionBuffer buffer;

    @BeforeEach
    public void setUp() {
        segmentMap = new ConcurrentSkipListMap<>();
        buffer = new InMemoryWindowTransactionBuffer(segmentMap, false);
    }

    @Test
    public void windowEntryKeyOrdersByTimestampThenKey() {
        assertTrue(windowKey(1, key("z")).compareTo(windowKey(2, key("a"))) < 0);
        assertTrue(windowKey(5, key("a")).compareTo(windowKey(5, key("b"))) < 0);
        assertEquals(0, windowKey(5, key("a")).compareTo(windowKey(5, key("a"))));
    }

    @Test
    public void windowEntryKeyNullKeyIsUnboundedUpperBoundAtSameTimestamp() {
        assertTrue(windowKey(5, null).compareTo(windowKey(5, key("zzz"))) > 0);
        assertTrue(windowKey(5, key("zzz")).compareTo(windowKey(5, null)) < 0);
        assertEquals(0, windowKey(5, null).compareTo(windowKey(5, null)));
    }

    @Test
    public void shouldReadStagedPutByTimestampAndKey() {
        buffer.stage(10L, key("a"), new byte[]{7});
        assertArrayEquals(new byte[]{7}, buffer.get(10L, key("a")).get());
    }

    @Test
    public void shouldReadStagedTombstoneAsEmptyOptional() {
        buffer.stage(10L, key("a"), new byte[]{7});
        buffer.stage(10L, key("a"), null);
        assertEquals(Optional.empty(), buffer.get(10L, key("a")));
    }

    @Test
    public void shouldReturnNullForUnstagedEntry() {
        assertNull(buffer.get(10L, key("a")));
    }

    @Test
    public void commitShouldFlushStagedPutIntoSegmentMap() {
        buffer.stage(10L, key("a"), new byte[]{7});
        buffer.commit();
        assertArrayEquals(new byte[]{7}, segmentMap.get(10L).get(key("a")));
        assertTrue(buffer.isEmpty());
    }

    @Test
    public void commitShouldRemoveKeyAndDropEmptiedSegmentOnTombstone() {
        segmentMap.computeIfAbsent(10L, t -> new ConcurrentSkipListMap<>()).put(key("a"), new byte[]{1});
        buffer.stage(10L, key("a"), null);
        buffer.commit();
        assertNull(segmentMap.get(10L));
    }

    @Test
    public void commitShouldKeepSegmentWhenOtherKeysRemain() {
        segmentMap.computeIfAbsent(10L, t -> new ConcurrentSkipListMap<>()).put(key("a"), new byte[]{1});
        segmentMap.get(10L).put(key("b"), new byte[]{2});
        buffer.stage(10L, key("a"), null);
        buffer.commit();
        assertNull(segmentMap.get(10L).get(key("a")));
        assertArrayEquals(new byte[]{2}, segmentMap.get(10L).get(key("b")));
    }

    @Test
    public void approximateBytesShouldCountTimestampPlusKeyPlusValue() {
        buffer.stage(10L, key("abc"), new byte[]{1, 2});
        assertEquals(Long.BYTES + 3 + 2, buffer.approximateNumUncommittedBytes());
    }

    private static Bytes key(final String k) {
        return Bytes.wrap(k.getBytes(UTF_8));
    }

    private static InMemoryWindowTransactionBuffer.WindowEntryKey windowKey(final long timestamp, final Bytes key) {
        return new InMemoryWindowTransactionBuffer.WindowEntryKey(timestamp, key);
    }
}
