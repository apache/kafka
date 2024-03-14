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

package org.apache.kafka.server.mutable;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(120)
public class BoundedListTest {

    @Test
    public void testMaxLengthMustNotBeZero() {
        assertEquals("Invalid non-positive maxLength of 0",
            assertThrows(IllegalArgumentException.class,
                () -> BoundedList.newArrayBacked(0)).getMessage());

        assertEquals("Invalid non-positive maxLength of 0",
            assertThrows(IllegalArgumentException.class,
                () -> BoundedList.newArrayBacked(0, 100)).getMessage());
    }

    @Test
    public void testMaxLengthMustNotBeNegative() {
        assertEquals("Invalid non-positive maxLength of -123",
            assertThrows(IllegalArgumentException.class,
                () -> BoundedList.newArrayBacked(-123)).getMessage());

        assertEquals("Invalid non-positive maxLength of -123",
            assertThrows(IllegalArgumentException.class,
                () -> BoundedList.newArrayBacked(-123, 100)).getMessage());
    }

    @Test
    public void testInitialCapacityMustNotBeZero() {
        assertEquals("Invalid non-positive initialCapacity of 0",
            assertThrows(IllegalArgumentException.class,
                () -> BoundedList.newArrayBacked(100, 0)).getMessage());
    }

    @Test
    public void testInitialCapacityMustNotBeNegative() {
        assertEquals("Invalid non-positive initialCapacity of -123",
            assertThrows(IllegalArgumentException.class,
                () -> BoundedList.newArrayBacked(100, -123)).getMessage());
    }

    @Test
    public void testAddingToBoundedList() {
        BoundedList<Integer> list = BoundedList.newArrayBacked(2);
        assertEquals(0, list.size());
        assertTrue(list.isEmpty());
        assertTrue(list.add(456));
        assertTrue(list.contains(456));
        assertEquals(1, list.size());
        assertFalse(list.isEmpty());
        assertTrue(list.add(789));
        assertEquals("Cannot add another element to the list because it would exceed the " +
            "maximum length of 2",
                assertThrows(BoundedListTooLongException.class,
                    () -> list.add(912)).getMessage());
        assertEquals("Cannot add another element to the list because it would exceed the " +
            "maximum length of 2",
                assertThrows(BoundedListTooLongException.class,
                    () -> list.add(0, 912)).getMessage());
    }

    @Test
    public void testHashCodeAndEqualsForNonEmptyList() {
        BoundedList<Integer> boundedList = BoundedList.newArrayBacked(7);
        List<Integer> otherList = Arrays.asList(1, 2, 3, 4, 5, 6, 7);
        boundedList.addAll(otherList);

        assertEquals(otherList, boundedList);
        assertEquals(otherList.hashCode(), boundedList.hashCode());
    }

    @Test
    public void testSet() {
        BoundedList<Integer> list = BoundedList.newArrayBacked(3);
        list.add(1);
        list.add(200);
        list.add(3);
        assertEquals(Arrays.asList(1, 200, 3), list);
        list.set(0, 100);
        list.set(1, 200);
        list.set(2, 300);
        assertEquals(Arrays.asList(100, 200, 300), list);
    }

    @Test
    public void testRemove() {
        BoundedList<String> list = BoundedList.newArrayBacked(3);
        list.add("a");
        list.add("a");
        list.add("c");
        assertEquals(0, list.indexOf("a"));
        assertEquals(1, list.lastIndexOf("a"));
        list.remove("a");
        assertEquals(Arrays.asList("a", "c"), list);
        list.remove(0);
        assertEquals(Arrays.asList("c"), list);
    }

    @Test
    public void testClear() {
        BoundedList<String> list = BoundedList.newArrayBacked(3);
        list.add("a");
        list.add("a");
        list.add("c");
        list.clear();
        assertEquals(Arrays.asList(), list);
        assertTrue(list.isEmpty());
    }

    @Test
    public void testGet() {
        BoundedList<Integer> list = BoundedList.newArrayBacked(3);
        list.add(1);
        list.add(2);
        list.add(3);
        assertEquals(1, list.get(0));
        assertEquals(2, list.get(1));
        assertEquals(3, list.get(2));
    }

    @Test
    public void testToArray() {
        BoundedList<Integer> list = BoundedList.newArrayBacked(3);
        list.add(1);
        list.add(2);
        list.add(3);
        assertArrayEquals(new Integer[] {1, 2, 3}, list.toArray());
        assertArrayEquals(new Integer[] {1, 2, 3}, list.toArray(new Integer[3]));
    }

    @Test
    public void testAddAll() {
        BoundedList<String> list = BoundedList.newArrayBacked(5);
        list.add("a");
        list.add("b");
        list.add("c");
        assertEquals("Cannot add another 3 element(s) to the list because it would exceed the " +
            "maximum length of 5",
                assertThrows(BoundedListTooLongException.class,
                        () -> list.addAll(Arrays.asList("d", "e", "f"))).getMessage());
        assertEquals("Cannot add another 3 element(s) to the list because it would exceed the " +
            "maximum length of 5",
                assertThrows(BoundedListTooLongException.class,
                        () -> list.addAll(0, Arrays.asList("d", "e", "f"))).getMessage());
        list.addAll(Arrays.asList("d", "e"));
        assertEquals(Arrays.asList("a", "b", "c", "d", "e"), list);
    }

    @Test
    public void testIterator() {
        BoundedList<Integer> list = BoundedList.newArrayBacked(3);
        list.add(1);
        list.add(2);
        list.add(3);
        assertEquals(1, list.iterator().next());
        assertEquals(1, list.listIterator().next());
        assertEquals(3, list.listIterator(2).next());
        assertFalse(list.listIterator(3).hasNext());
    }

    @Test
    public void testIteratorIsImmutable() {
        BoundedList<Integer> list = BoundedList.newArrayBacked(3);
        list.add(1);
        list.add(2);
        list.add(3);
        assertThrows(UnsupportedOperationException.class,
            () -> list.iterator().remove());
        assertThrows(UnsupportedOperationException.class,
            () -> list.listIterator().remove());
    }

    @Test
    public void testSubList() {
        BoundedList<Integer> list = BoundedList.newArrayBacked(3);
        list.add(1);
        list.add(2);
        list.add(3);
        assertEquals(Arrays.asList(2), list.subList(1, 2));
        assertThrows(UnsupportedOperationException.class,
            () -> list.subList(1, 2).remove(2));
    }
}
