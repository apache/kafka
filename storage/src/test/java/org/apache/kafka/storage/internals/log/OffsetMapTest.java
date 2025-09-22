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
package org.apache.kafka.storage.internals.log;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class OffsetMapTest {

    private static final int MEMORY_SIZE = 4096;

    @ParameterizedTest
    @ValueSource(ints = {10, 100, 1000, 5000})
    public void testBasicValidation(int items) throws Exception {
        SkimpyOffsetMap map = new SkimpyOffsetMap(items * 48);
        IntStream.range(0, items).forEach(i -> assertDoesNotThrow(() -> map.put(key(i), i)));
        for (int i = 0; i < items; i++) {
            assertEquals(i, map.get(key(i)));
        }
    }

    @Test
    public void testClear() throws Exception {
        SkimpyOffsetMap map = new SkimpyOffsetMap(MEMORY_SIZE);
        IntStream.range(0, 10).forEach(i -> assertDoesNotThrow(() -> map.put(key(i), i)));
        for (int i = 0; i < 10; i++) {
            assertEquals(i, map.get(key(i)));
        }
        map.clear();
        for (int i = 0; i < 10; i++) {
            assertEquals(-1, map.get(key(i)));
        }
    }

    @Test
    public void testGetWhenFull() throws Exception {
        SkimpyOffsetMap map = new SkimpyOffsetMap(MEMORY_SIZE);
        int i = 37;
        while (map.size() < map.slots()) {
            map.put(key(i), i);
            i++;
        }
        assertEquals(-1, map.get(key(i)));
        assertEquals(i - 1, map.get(key(i - 1)));
    }

    @Test
    public void testUpdateLatestOffset() throws Exception {
        SkimpyOffsetMap map = new SkimpyOffsetMap(MEMORY_SIZE);
        int i = 37;
        while (map.size() < map.slots()) {
            map.put(key(i), i);
            i++;
        }
        int lastOffsets = 40;
        assertEquals(i - 1, map.get(key(i - 1)));
        map.updateLatestOffset(lastOffsets);
        assertEquals(lastOffsets, map.get(key(lastOffsets)));
    }

    @Test
    public void testLatestOffset() throws Exception {
        SkimpyOffsetMap map = new SkimpyOffsetMap(MEMORY_SIZE);
        int i = 37;
        while (map.size() < map.slots()) {
            map.put(key(i), i);
            i++;
        }
        assertEquals(i - 1, map.latestOffset());
    }
    
    @Test
    public void testUtilization() throws Exception {
        SkimpyOffsetMap map = new SkimpyOffsetMap(MEMORY_SIZE);
        int i = 37;
        assertEquals(0.0, map.utilization());
        while (map.size() < map.slots()) {
            map.put(key(i), i);
            assertEquals((double) map.size() / map.slots(), map.utilization());
            i++;
        }
    }

    private ByteBuffer key(Integer key) {
        return ByteBuffer.wrap(key.toString().getBytes());
    }
}