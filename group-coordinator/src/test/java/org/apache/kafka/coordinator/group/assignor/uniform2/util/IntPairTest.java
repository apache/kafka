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
package org.apache.kafka.coordinator.group.assignor.uniform2.util;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests the pairs of ints packed in a long.
 */
public class IntPairTest {
    /**
     * Pairs covering zero, negative ints and the extremes of both ints.
     */
    private static final List<int[]> PAIRS = List.of(
        new int[] {3, 7},
        new int[] {0, 0},
        new int[] {0, -1},
        new int[] {-1, 0},
        new int[] {Integer.MAX_VALUE, Integer.MIN_VALUE},
        new int[] {Integer.MIN_VALUE, Integer.MAX_VALUE}
    );

    @Test
    public void testPack() {
        assertEquals((3L << 32) | 7L, IntPair.pack(3, 7));
        assertEquals(0xFFFFFFFFL, IntPair.pack(0, -1));
        assertEquals(-1L, IntPair.pack(-1, -1));
    }

    @Test
    public void testHigh() {
        for (int[] pair : PAIRS) {
            assertEquals(pair[0], IntPair.high(IntPair.pack(pair[0], pair[1])));
        }
    }

    @Test
    public void testLow() {
        for (int[] pair : PAIRS) {
            assertEquals(pair[1], IntPair.low(IntPair.pack(pair[0], pair[1])));
        }
    }

    /**
     * Pairs of non-negative ints sort by the high int, then the low int.
     */
    @Test
    public void testPairsOfNonNegativeIntsSortByHighThenLow() {
        long[] pairs = {
            IntPair.pack(2, 0),
            IntPair.pack(1, 5),
            IntPair.pack(1, 2),
            IntPair.pack(0, Integer.MAX_VALUE)
        };

        Arrays.sort(pairs);

        assertArrayEquals(new long[] {
            IntPair.pack(0, Integer.MAX_VALUE),
            IntPair.pack(1, 2),
            IntPair.pack(1, 5),
            IntPair.pack(2, 0)
        }, pairs);
    }
}
