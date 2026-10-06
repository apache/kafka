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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests of {@link IntList}. The lists are built with {@link #listOf(int...)}, whose capacity is
 * exactly the number of elements, so that the next addition grows the backing array.
 */
public class IntListTest {

    @Test
    public void testNegativeCapacityThrows() {
        assertThrows(IllegalArgumentException.class, () -> new IntList(-1));
    }

    /**
     * A list without capacity grows to 4 elements on its first addition, then doubles.
     */
    @Test
    public void testZeroCapacityGrows() {
        var list = new IntList(0);
        assertTrue(list.isEmpty());
        assertEquals(0, list.size());

        for (int i = 0; i < 9; i++) {
            list.add(10 * i);
            assertEquals(i + 1, list.size());
        }

        assertFalse(list.isEmpty());
        for (int i = 0; i < 9; i++) {
            assertEquals(10 * i, list.get(i));
        }
    }

    @Test
    public void testAddBeyondTheInitialCapacity() {
        var list = new IntList(1);
        list.add(3);
        list.add(1);
        list.add(2);

        assertEquals(3, list.size());
        assertArrayEquals(new int[] {3, 1, 2}, list.toArray());
    }

    @Test
    public void testSet() {
        var list = listOf(1, 2, 3);

        list.set(1, 20);

        assertEquals(3, list.size());
        assertArrayEquals(new int[] {1, 20, 3}, list.toArray());
    }

    /**
     * Neither a negative index nor one between the size and the capacity can be set, and the list
     * is left unchanged.
     */
    @Test
    public void testSetOutOfRangeThrows() {
        var list = new IntList(16);
        list.add(1);
        list.add(2);

        assertThrows(IndexOutOfBoundsException.class, () -> list.set(2, 9));
        assertThrows(IndexOutOfBoundsException.class, () -> list.set(-1, 9));

        list.add(5);
        assertArrayEquals(new int[] {1, 2, 5}, list.toArray());
    }

    /**
     * Removing an element returns it and moves the last element into its place; removing the
     * last element only shortens the list.
     */
    @Test
    public void testRemove() {
        var list = listOf(10, 20, 30, 40);

        assertEquals(20, list.remove(1));
        assertArrayEquals(new int[] {10, 40, 30}, list.toArray());

        assertEquals(30, list.remove(2));
        assertArrayEquals(new int[] {10, 40}, list.toArray());

        assertEquals(10, list.remove(0));
        assertArrayEquals(new int[] {40}, list.toArray());

        assertEquals(40, list.remove(0));
        assertTrue(list.isEmpty());

        list.add(50);
        assertArrayEquals(new int[] {50}, list.toArray());
    }

    /**
     * Neither a negative index nor one between the size and the capacity can be removed, and the
     * list is left unchanged, also when it is empty.
     */
    @Test
    public void testRemoveOutOfRangeThrows() {
        var list = new IntList(4);
        list.add(10);
        list.add(20);

        assertThrows(IndexOutOfBoundsException.class, () -> list.remove(2));
        assertThrows(IndexOutOfBoundsException.class, () -> list.remove(-1));
        assertArrayEquals(new int[] {10, 20}, list.toArray());

        var empty = new IntList(4);
        assertThrows(IndexOutOfBoundsException.class, () -> empty.remove(0));
        assertTrue(empty.isEmpty());
        assertArrayEquals(new int[0], empty.toArray());
    }

    /**
     * The index is the one of the first occurrence, and only the elements below the size count:
     * a new list, whose backing array holds zeros, does not contain 0, and neither does a list
     * once its last element, 0, is removed.
     */
    @Test
    public void testIndexOf() {
        var list = listOf(7, 3, 7, 0);
        assertEquals(0, list.indexOf(7));
        assertEquals(1, list.indexOf(3));
        assertEquals(3, list.indexOf(0));
        assertEquals(-1, list.indexOf(5));

        assertEquals(0, list.remove(3));
        assertEquals(-1, list.indexOf(0));

        assertEquals(-1, new IntList(4).indexOf(0));
    }

    @Test
    public void testClear() {
        var list = listOf(1, 2, 3);

        list.clear();

        assertTrue(list.isEmpty());
        assertEquals(0, list.size());
        assertArrayEquals(new int[0], list.toArray());
        assertEquals(-1, list.indexOf(1));

        list.add(4);
        assertArrayEquals(new int[] {4}, list.toArray());
    }

    @Test
    public void testTruncate() {
        var list = listOf(1, 2, 3);

        list.truncate(1);

        assertEquals(1, list.size());
        assertArrayEquals(new int[] {1}, list.toArray());
        assertEquals(-1, list.indexOf(2));

        list.add(4);
        assertArrayEquals(new int[] {1, 4}, list.toArray());
    }

    /**
     * Truncating keeps at most the elements of the list: it cannot grow it, not even back to
     * elements cleared before.
     */
    @Test
    public void testTruncateOutOfRangeThrows() {
        var list = listOf(1, 2, 3);

        assertThrows(IndexOutOfBoundsException.class, () -> list.truncate(4));
        assertThrows(IndexOutOfBoundsException.class, () -> list.truncate(-1));
        assertArrayEquals(new int[] {1, 2, 3}, list.toArray());

        list.truncate(3);
        assertArrayEquals(new int[] {1, 2, 3}, list.toArray());

        list.clear();
        assertThrows(IndexOutOfBoundsException.class, () -> list.truncate(2));
        assertTrue(list.isEmpty());
    }

    @Test
    public void testSort() {
        var list = listOf(2, -1, 5, 2, 0);

        list.sort();

        assertArrayEquals(new int[] {-1, 0, 2, 2, 5}, list.toArray());
    }

    /**
     * Only the elements below the size are sorted: the 0 removed from the end stays out of the
     * list, although it is still in the backing array.
     */
    @Test
    public void testSortIgnoresRemovedElements() {
        var list = listOf(3, 2, 1, 0);
        assertEquals(0, list.remove(3));

        list.sort();

        assertArrayEquals(new int[] {1, 2, 3}, list.toArray());
    }

    @Test
    public void testToArrayIsANewArrayOfTheSize() {
        var list = new IntList(8);
        list.add(1);
        list.add(2);

        int[] array = list.toArray();
        assertArrayEquals(new int[] {1, 2}, array);
        assertNotSame(array, list.toArray());

        array[0] = 9;
        assertEquals(1, list.get(0));
    }

    @Test
    public void testToString() {
        assertEquals("[]", new IntList(0).toString());
        assertEquals("[]", new IntList(4).toString());

        var list = listOf(1, 2, 3);
        assertEquals("[1, 2, 3]", list.toString());

        list.remove(0);
        assertEquals("[3, 2]", list.toString());
    }

    private static IntList listOf(int... elements) {
        var list = new IntList(elements.length);
        for (int element : elements) {
            list.add(element);
        }
        return list;
    }
}
