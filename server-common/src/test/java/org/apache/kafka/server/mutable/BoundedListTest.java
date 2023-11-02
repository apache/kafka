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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
                () -> new BoundedList<>(0, new ArrayList<Integer>())).
                    getMessage());
    }

    @Test
    public void testMaxLengthMustNotBeNegative() {
        assertEquals("Invalid non-positive maxLength of -123",
            assertThrows(IllegalArgumentException.class,
                () -> new BoundedList<>(-123, new ArrayList<Integer>())).
                    getMessage());
    }

    @Test
    public void testOwnedListMustNotBeTooLong() {
        assertEquals("Cannot wrap list, because it is longer than the maximum length 1",
            assertThrows(BoundedListTooLongException.class,
                () -> new BoundedList<>(1, new ArrayList<>(Arrays.asList(1, 2)))).
                    getMessage());
    }

    @Test
    public void testAddingToBoundedList() {
        BoundedList<Integer> list = new BoundedList<>(2, new ArrayList<>(3));
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
                    () -> list.add(912)).
                        getMessage());
        assertEquals("Cannot add another element to the list because it would exceed the " +
            "maximum length of 2",
                assertThrows(BoundedListTooLongException.class,
                    () -> list.add(0, 912)).
                        getMessage());
    }

    private static <E> void testHashCodeAndEquals(List<E> a) {
        assertEquals(a, new BoundedList<>(123, a));
        assertEquals(a.hashCode(), new BoundedList<>(123, a).hashCode());
    }

    @Test
    public void testHashCodeAndEqualsForEmptyList() {
        testHashCodeAndEquals(Collections.emptyList());
    }

    @Test
    public void testHashCodeAndEqualsForNonEmptyList() {
        testHashCodeAndEquals(Arrays.asList(1, 2, 3, 4, 5, 6, 7));
    }

    @Test
    public void testSet() {
        ArrayList<Integer> underlying = new ArrayList<>(Arrays.asList(1, 2, 3));
        BoundedList<Integer> list = new BoundedList<>(3, underlying);
        list.set(1, 200);
        assertEquals(Arrays.asList(1, 200, 3), list);
    }

    @Test
    public void testRemove() {
        ArrayList<String> underlying = new ArrayList<>(Arrays.asList("a", "a", "c"));
        BoundedList<String> list = new BoundedList<>(3, underlying);
        assertEquals(0, list.indexOf("a"));
        assertEquals(1, list.lastIndexOf("a"));
        list.remove("a");
        assertEquals(Arrays.asList("a", "c"), list);
        list.remove(0);
        assertEquals(Arrays.asList("c"), list);
    }

    @Test
    public void testClear() {
        ArrayList<String> underlying = new ArrayList<>(Arrays.asList("a", "b", "c"));
        BoundedList<String> list = new BoundedList<>(3, underlying);
        list.clear();
        assertEquals(Arrays.asList(), list);
        assertTrue(list.isEmpty());
    }

    @Test
    public void testGet() {
        BoundedList<Integer> list = new BoundedList<>(3, Arrays.asList(1, 2, 3));
        assertEquals(2, list.get(1));
    }

    @Test
    public void testToArray() {
        BoundedList<Integer> list = new BoundedList<>(3, Arrays.asList(1, 2, 3));
        assertArrayEquals(new Integer[] {1, 2, 3}, list.toArray());
        assertArrayEquals(new Integer[] {1, 2, 3}, list.toArray(new Integer[3]));
    }

    @Test
    public void testAddAll() {
        ArrayList<String> underlying = new ArrayList<>(Arrays.asList("a", "b", "c"));
        BoundedList<String> list = new BoundedList<>(5, underlying);
        assertEquals("Cannot add another 3 element(s) to the list because it would exceed the " +
            "maximum length of 5",
                assertThrows(BoundedListTooLongException.class,
                    () -> list.addAll(Arrays.asList("d", "e", "f"))).
                        getMessage());
        assertEquals("Cannot add another 3 element(s) to the list because it would exceed the " +
            "maximum length of 5",
                assertThrows(BoundedListTooLongException.class,
                        () -> list.addAll(0, Arrays.asList("d", "e", "f"))).
                        getMessage());
        list.addAll(Arrays.asList("d", "e"));
        assertEquals(Arrays.asList("a", "b", "c", "d", "e"), list);
    }

    @Test
    public void testIterator() {
        BoundedList<Integer> list = new BoundedList<>(3, Arrays.asList(1, 2, 3));
        assertEquals(1, list.iterator().next());
        assertEquals(1, list.listIterator().next());
        assertEquals(3, list.listIterator(2).next());
        assertFalse(list.listIterator(3).hasNext());
    }

    @Test
    public void testIteratorIsImmutable() {
        BoundedList<Integer> list = new BoundedList<>(3, new ArrayList<>(Arrays.asList(1, 2, 3)));
        assertThrows(UnsupportedOperationException.class,
            () -> list.iterator().remove());
        assertThrows(UnsupportedOperationException.class,
            () -> list.listIterator().remove());
    }

    @Test
    public void testSubList() {
        BoundedList<Integer> list = new BoundedList<>(3, new ArrayList<>(Arrays.asList(1, 2, 3)));
        assertEquals(Arrays.asList(2), list.subList(1, 2));
        assertThrows(UnsupportedOperationException.class,
            () -> list.subList(1, 2).remove(2));
    }
}
