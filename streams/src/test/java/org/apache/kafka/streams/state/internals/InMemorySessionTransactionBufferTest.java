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
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;

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

public class InMemorySessionTransactionBufferTest {

    private ConcurrentNavigableMap<Long, ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, byte[]>>> endTimeMap;
    private InMemorySessionTransactionBuffer buffer;

    @BeforeEach
    public void setUp() {
        endTimeMap = new ConcurrentSkipListMap<>();
        buffer = new InMemorySessionTransactionBuffer(endTimeMap);
    }

    @Test
    public void sessionEntryKeyOrdersByEndTimeThenKeyThenDescendingStartTime() {
        assertTrue(sessionKey(1, key("a"), 0).compareTo(sessionKey(2, key("a"), 0)) < 0);
        assertTrue(sessionKey(5, key("a"), 0).compareTo(sessionKey(5, key("b"), 0)) < 0);
        assertTrue(sessionKey(5, key("a"), 20).compareTo(sessionKey(5, key("a"), 10)) < 0);
        assertEquals(0, sessionKey(5, key("a"), 10).compareTo(sessionKey(5, key("a"), 10)));
    }

    @Test
    public void shouldReadStagedPutBySessionCoordinates() {
        buffer.stage(session("a", 10, 20), new byte[]{7});
        assertArrayEquals(new byte[]{7}, buffer.get(key("a"), 10, 20).get());
    }

    @Test
    public void shouldReadStagedTombstoneAsEmptyOptional() {
        buffer.stage(session("a", 10, 20), new byte[]{7});
        buffer.stage(session("a", 10, 20), null);
        assertEquals(Optional.empty(), buffer.get(key("a"), 10, 20));
    }

    @Test
    public void shouldReturnNullForUnstagedSession() {
        assertNull(buffer.get(key("a"), 10, 20));
    }

    @Test
    public void commitShouldFlushStagedPutIntoNestedMap() {
        buffer.stage(session("a", 10, 20), new byte[]{7});
        buffer.commit();
        assertArrayEquals(new byte[]{7}, endTimeMap.get(20L).get(key("a")).get(10L));
        assertTrue(buffer.isEmpty());
    }

    @Test
    public void commitShouldUnwindNestedMapsOnTombstone() {
        endTimeMap.computeIfAbsent(20L, t -> new ConcurrentSkipListMap<>())
            .computeIfAbsent(key("a"), k -> new ConcurrentSkipListMap<>())
            .put(10L, new byte[]{1});
        buffer.stage(session("a", 10, 20), null);
        buffer.commit();
        assertNull(endTimeMap.get(20L));
    }

    @Test
    public void commitShouldKeepOuterLevelsWhenSiblingSessionsRemain() {
        endTimeMap.computeIfAbsent(20L, t -> new ConcurrentSkipListMap<>())
            .computeIfAbsent(key("a"), k -> new ConcurrentSkipListMap<>())
            .put(10L, new byte[]{1});
        endTimeMap.get(20L).get(key("a")).put(5L, new byte[]{2});
        buffer.stage(session("a", 10, 20), null);
        buffer.commit();
        assertNull(endTimeMap.get(20L).get(key("a")).get(10L));
        assertArrayEquals(new byte[]{2}, endTimeMap.get(20L).get(key("a")).get(5L));
    }

    @Test
    public void approximateBytesShouldCountTwoTimestampsPlusKeyPlusValue() {
        buffer.stage(session("ab", 10, 20), new byte[]{9});
        assertEquals(2 * Long.BYTES + 2 + 1, buffer.approximateNumUncommittedBytes());
    }

    private static Bytes key(final String k) {
        return Bytes.wrap(k.getBytes(UTF_8));
    }

    private static Windowed<Bytes> session(final String k, final long start, final long end) {
        return new Windowed<>(key(k), new SessionWindow(start, end));
    }

    private static InMemorySessionTransactionBuffer.SessionEntryKey sessionKey(final long endTime,
                                                                               final Bytes key,
                                                                               final long startTime) {
        return new InMemorySessionTransactionBuffer.SessionEntryKey(endTime, key, startTime);
    }
}
