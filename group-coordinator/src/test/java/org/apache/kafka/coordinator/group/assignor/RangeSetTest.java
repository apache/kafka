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

import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class RangeSetTest {
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
        HashSet<Integer> hashSet = new HashSet<>(Set.of(6, 7, 8, 9));

        assertEquals(rangeSet1, rangeSet2);
        assertNotEquals(rangeSet1, rangeSet3);
        assertEquals(rangeSet1, set);
        assertEquals(rangeSet3, hashSet);
        assertNotEquals(rangeSet1, new Object());
    }

    @Test
    void testHashCode() {
        RangeSet rangeSet1 = new RangeSet(5, 10);
        RangeSet rangeSet2 = new RangeSet(5, 10);
        RangeSet rangeSet3 = new RangeSet(6, 10);

        assertEquals(rangeSet1.hashCode(), rangeSet2.hashCode());
        assertNotEquals(rangeSet1.hashCode(), rangeSet3.hashCode());
    }
}
