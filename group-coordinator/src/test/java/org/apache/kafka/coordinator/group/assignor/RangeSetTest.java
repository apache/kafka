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
package org.apache.kafka.coordinator.group.assignor;

import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class RangeSetTest {
    @Test
    void testNegativeSize() {
        assertThrows(IllegalArgumentException.class, () -> new RangeSet(0, -1));
    }

    @Test
    void testOverflow() {
        // This range is the maximum size we allow.
        assertDoesNotThrow(() -> new RangeSet(0, Integer.MAX_VALUE));

        assertThrows(IllegalArgumentException.class, () -> new RangeSet(-1, Integer.MAX_VALUE));
    }

    @Test
    void testSize() {
        RangeSet rangeSet = new RangeSet(5, 10);
        assertEquals(5, rangeSet.size());
    }

    @Test
    void testIsEmpty() {
        RangeSet rangeSet = new RangeSet(5, 5);
        assertTrue(rangeSet.isEmpty());
    }

    @Test
    void testContains() {
        RangeSet rangeSet = new RangeSet(5, 10);
        assertTrue(rangeSet.contains(5));
        assertTrue(rangeSet.contains(9));
        assertFalse(rangeSet.contains(10));
        assertFalse(rangeSet.contains(4));
    }

    @Test
    void testIterator() {
        RangeSet rangeSet = new RangeSet(5, 10);
        Iterator<Integer> iterator = rangeSet.iterator();
        for (int i = 5; i < 10; i++) {
            assertTrue(iterator.hasNext());
            assertEquals(i, iterator.next());
        }
        assertFalse(iterator.hasNext());
        assertThrows(NoSuchElementException.class, iterator::next);
    }

    @Test
    void testUnsupportedOperations() {
        RangeSet rangeSet = new RangeSet(5, 10);
        assertThrows(UnsupportedOperationException.class, () -> rangeSet.add(5));
        assertThrows(UnsupportedOperationException.class, () -> rangeSet.remove(5));
        assertThrows(UnsupportedOperationException.class, () -> rangeSet.addAll(null));
        assertThrows(UnsupportedOperationException.class, () -> rangeSet.retainAll(null));
        assertThrows(UnsupportedOperationException.class, () -> rangeSet.removeAll(null));
        assertThrows(UnsupportedOperationException.class, rangeSet::clear);
    }

    @Test
    void testToArray() {
        RangeSet rangeSet = new RangeSet(5, 10);
        Object[] expectedArray = {5, 6, 7, 8, 9};
        assertArrayEquals(expectedArray, rangeSet.toArray());
    }

    @Test
    void testToArrayWithArrayParameter() {
        RangeSet rangeSet = new RangeSet(5, 10);
        Integer[] inputArray = new Integer[5];
        Integer[] expectedArray = {5, 6, 7, 8, 9};
        assertArrayEquals(expectedArray, rangeSet.toArray(inputArray));
    }

    @Test
    void testContainsAll() {
        RangeSet rangeSet = new RangeSet(5, 10);
        assertTrue(rangeSet.containsAll(Set.of(5, 6, 7, 8, 9)));
        assertFalse(rangeSet.containsAll(Set.of(5, 6, 10)));
    }

    @Test
    void testToString() {
        RangeSet rangeSet = new RangeSet(5, 8);
        assertEquals("RangeSet(from=5 (inclusive), to=8 (exclusive))", rangeSet.toString());
    }

    @Test
    void testEquals() {
        RangeSet rangeSet1 = new RangeSet(5, 10);
        RangeSet rangeSet2 = new RangeSet(5, 10);
        RangeSet rangeSet3 = new RangeSet(6, 10);
        Set<Integer> set = Set.of(5, 6, 7, 8, 9);
        Set<Integer> hashSet = Set.of(6, 7, 8, 9);

        assertEquals(rangeSet1, rangeSet2);
        assertNotEquals(rangeSet1, rangeSet3);
        assertEquals(set, rangeSet1);
        assertEquals(hashSet, rangeSet3);
        assertNotEquals(new Object(), rangeSet1);

        // Empty sets are equal.
        RangeSet emptyRangeSet1 = new RangeSet(0, 0);
        RangeSet emptyRangeSet2 = new RangeSet(5, 5);
        Set<Integer> emptySet = Set.of();

        assertEquals(emptySet, emptyRangeSet1);
        assertEquals(emptySet, emptyRangeSet2);
        assertEquals(emptyRangeSet1, emptyRangeSet2);
    }

    @Test
    void testHashCode() {
        RangeSet rangeSet1 = new RangeSet(5, 10);
        RangeSet rangeSet2 = new RangeSet(6, 10);

        assertEquals(Set.of(5, 6, 7, 8, 9).hashCode(), rangeSet1.hashCode());
        assertEquals(Set.of(6, 7, 8, 9).hashCode(), rangeSet2.hashCode());

        RangeSet emptySet = new RangeSet(5, 5);
        assertEquals(Set.of().hashCode(), emptySet.hashCode());

        // Both these cases overflow the range of an int when calculating the hash code. They're
        // chosen so that their hash codes are almost the same except for the most significant bit.
        RangeSet overflowRangeSet1 = new RangeSet(0x3FFFFFFD, 0x3FFFFFFF);
        RangeSet overflowRangeSet2 = new RangeSet(0x7FFFFFFD, 0x7FFFFFFF);
        assertEquals(
            Set.of(0x3FFFFFFD, 0x3FFFFFFE).hashCode(),
            overflowRangeSet1.hashCode() // == 0x0_FFFFFFF6 / 2
        );
        assertEquals(
            Set.of(0x7FFFFFFD, 0x7FFFFFFE).hashCode(),
            overflowRangeSet2.hashCode() // == 0x1_FFFFFFF6 / 2
        );
    }
}
