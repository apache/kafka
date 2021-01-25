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
package org.apache.kafka.common.utils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * A unit test for ImplicitLinkedHashCollection.
 */
@Timeout(120)
public class ImplicitLinkedHashCollectionTest {

    final static class TestElement implements ImplicitLinkedHashCollection.Element {
        private int prev = ImplicitLinkedHashCollection.INVALID_INDEX;
        private int next = ImplicitLinkedHashCollection.INVALID_INDEX;
        private final int key;
        private final int val;

        TestElement(int key) {
            this.key = key;
            this.val = 0;
        }

        TestElement(int key, int val) {
            this.key = key;
            this.val = val;
        }

        @Override
        public int prev() {
            return prev;
        }

        @Override
        public void setPrev(int prev) {
            this.prev = prev;
        }

        @Override
        public int next() {
            return next;
        }

        @Override
        public void setNext(int next) {
            this.next = next;
        }

        @Override
        public boolean elementKeysAreEqual(Object o) {
            if (this == o) return true;
            if ((o == null) || (o.getClass() != TestElement.class)) return false;
            TestElement that = (TestElement) o;
            return key == that.key;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if ((o == null) || (o.getClass() != TestElement.class)) return false;
            TestElement that = (TestElement) o;
            return key == that.key && val == that.val;
        }

        @Override
        public String toString() {
            return "TestElement(key=" + key + ", val=" + val + ")";
        }

        @Override
        public int hashCode() {
            long hashCode = 2654435761L * key;
            return (int) (hashCode >> 32);
        }
    }

    @Test
    public void testNullForbidden() {
        ImplicitLinkedHashMultiCollection<TestElement> multiColl = new ImplicitLinkedHashMultiCollection<>();
        assertFalse(multiColl.add(null));
    }

    @Test
    public void testInsertDelete() {
        ImplicitLinkedHashCollection<TestElement> coll = new ImplicitLinkedHashCollection<>(100);
        assertTrue(coll.add(new TestElement(1)));
        TestElement second = new TestElement(2);
        assertTrue(coll.add(second));
        assertTrue(coll.add(new TestElement(3)));
        assertFalse(coll.add(new TestElement(3)));
        assertEquals(3, coll.size());
        assertTrue(coll.contains(new TestElement(1)));
        assertFalse(coll.contains(new TestElement(4)));
        TestElement secondAgain = coll.find(new TestElement(2));
        assertTrue(second == secondAgain);
        assertTrue(coll.remove(new TestElement(1)));
        assertFalse(coll.remove(new TestElement(1)));
        assertEquals(2, coll.size());
        coll.clear();
        assertEquals(0, coll.size());
    }

    static void expectTraversal(Iterator<TestElement> iterator, Integer... sequence) {
        int i = 0;
        while (iterator.hasNext()) {
            TestElement element = iterator.next();
            assertTrue(i < sequence.length, "Iterator yieled " + (i + 1) + " elements, but only " +
                sequence.length + " were expected.");
            assertEquals(sequence[i].intValue(), element.key, "Iterator value number " + (i + 1) + " was incorrect.");
            i = i + 1;
        }
        assertTrue(i == sequence.length, "Iterator yieled " + (i + 1) + " elements, but " +
            sequence.length + " were expected.");
    }

    static void expectTraversal(Iterator<TestElement> iter, Iterator<Integer> expectedIter) {
        int i = 0;
        while (iter.hasNext()) {
            TestElement element = iter.next();
            assertTrue(expectedIter.hasNext(), "Iterator yieled " + (i + 1) + " elements, but only " + i +
                " were expected.");
            Integer expected = expectedIter.next();
            assertEquals(expected.intValue(), element.key, "Iterator value number " + (i + 1) + " was incorrect.");
            i = i + 1;
        }
        assertFalse(expectedIter.hasNext(), "Iterator yieled " + i + " elements, but at least " + (i + 1) +
            " were expected.");
    }

    @Test
    public void testTraversal() {
        ImplicitLinkedHashCollection<TestElement> coll = new ImplicitLinkedHashCollection<>();
        expectTraversal(coll.iterator());
        assertTrue(coll.add(new TestElement(2)));
        expectTraversal(coll.iterator(), 2);
        assertTrue(coll.add(new TestElement(1)));
        expectTraversal(coll.iterator(), 2, 1);
        assertTrue(coll.add(new TestElement(100)));
        expectTraversal(coll.iterator(), 2, 1, 100);
        assertTrue(coll.remove(new TestElement(1)));
        expectTraversal(coll.iterator(), 2, 100);
        assertTrue(coll.add(new TestElement(1)));
        expectTraversal(coll.iterator(), 2, 100, 1);
        Iterator<TestElement> iter = coll.iterator();
        iter.next();
        iter.next();
        iter.remove();
        iter.next();
        assertFalse(iter.hasNext());
        expectTraversal(coll.iterator(), 2, 1);
        List<TestElement> list = new ArrayList<>();
        list.add(new TestElement(1));
        list.add(new TestElement(2));
        assertTrue(coll.removeAll(list));
        assertFalse(coll.removeAll(list));
        expectTraversal(coll.iterator());
        assertEquals(0, coll.size());
        assertTrue(coll.isEmpty());
    }

    @Test
    public void testSetViewGet() {
        ImplicitLinkedHashCollection<TestElement> coll = new ImplicitLinkedHashCollection<>();
        coll.add(new TestElement(1));
        coll.add(new TestElement(2));
        coll.add(new TestElement(3));

        Set<TestElement> set = coll.valuesSet();
        assertTrue(set.contains(new TestElement(1)));
        assertTrue(set.contains(new TestElement(2)));
        assertTrue(set.contains(new TestElement(3)));
        assertEquals(3, set.size());
    }

    @Test
    public void testSetViewModification() {
        ImplicitLinkedHashCollection<TestElement> coll = new ImplicitLinkedHashCollection<>();
        coll.add(new TestElement(1));
        coll.add(new TestElement(2));
        coll.add(new TestElement(3));

        // Removal from set is reflected in collection
        Set<TestElement> set = coll.valuesSet();
        set.remove(new TestElement(1));
        assertFalse(coll.contains(new TestElement(1)));
        assertEquals(2, coll.size());

        // Addition to set is reflected in collection
        set.add(new TestElement(4));
        assertTrue(coll.contains(new TestElement(4)));
        assertEquals(3, coll.size());

        // Removal from collection is reflected in set
        coll.remove(new TestElement(2));
        assertFalse(set.contains(new TestElement(2)));
        assertEquals(2, set.size());

        // Addition to collection is reflected in set
        coll.add(new TestElement(5));
        assertTrue(set.contains(new TestElement(5)));
        assertEquals(3, set.size());

        // Ordering in the collection is maintained
        int key = 3;
        for (TestElement e : coll) {
            assertEquals(key, e.key);
            ++key;
        }
    }

    @Test
    public void testListViewGet() {
        ImplicitLinkedHashCollection<TestElement> coll = new ImplicitLinkedHashCollection<>();
        coll.add(new TestElement(1));
        coll.add(new TestElement(2));
        coll.add(new TestElement(3));

        List<TestElement> list = coll.valuesList();
        assertEquals(1, list.get(0).key);
        assertEquals(2, list.get(1).key);
        assertEquals(3, list.get(2).key);
        assertEquals(3, list.size());
    }

    @Test
    public void testListViewModification() {
        ImplicitLinkedHashCollection<TestElement> coll = new ImplicitLinkedHashCollection<>();
        coll.add(new TestElement(1));
        coll.add(new TestElement(2));
        coll.add(new TestElement(3));

        // Removal from list is reflected in collection
        List<TestElement> list = coll.valuesList();
        list.remove(1);
        assertTrue(coll.contains(new TestElement(1)));
        assertFalse(coll.contains(new TestElement(2)));
        assertTrue(coll.contains(new TestElement(3)));
        assertEquals(2, coll.size());

        // Removal from collection is reflected in list
        coll.remove(new TestElement(1));
        assertEquals(3, list.get(0).key);
        assertEquals(1, list.size());

        // Addition to collection is reflected in list
        coll.add(new TestElement(4));
        assertEquals(3, list.get(0).key);
        assertEquals(4, list.get(1).key);
        assertEquals(2, list.size());
    }

    @Test
    public void testEmptyListIterator() {
        ImplicitLinkedHashCollection<TestElement> coll = new ImplicitLinkedHashCollection<>();
        ListIterator<TestElement> iter = coll.valuesList().listIterator();
        assertFalse(iter.hasNext());
        assertFalse(iter.hasPrevious());
        assertEquals(0, iter.nextIndex());
        assertEquals(-1, iter.previousIndex());
    }

    @Test
    public void testListIteratorCreation() {
        ImplicitLinkedHashCollection<TestElement> coll = new ImplicitLinkedHashCollection<>();
        coll.add(new TestElement(1));
        coll.add(new TestElement(2));
        coll.add(new TestElement(3));

        // Iterator created at the start of the list should have a next but no prev
        ListIterator<TestElement> iter = coll.valuesList().listIterator();
        assertTrue(iter.hasNext());
        assertFalse(iter.hasPrevious());
        assertEquals(0, iter.nextIndex());
        assertEquals(-1, iter.previousIndex());

        // Iterator created in the middle of the list should have both a next and a prev
        iter = coll.valuesList().listIterator(2);
        assertTrue(iter.hasNext());
        assertTrue(iter.hasPrevious());
        assertEquals(2, iter.nextIndex());
        assertEquals(1, iter.previousIndex());

        // Iterator created at the end of the list should have a prev but no next
        iter = coll.valuesList().listIterator(3);
        assertFalse(iter.hasNext());
        assertTrue(iter.hasPrevious());
        assertEquals(3, iter.nextIndex());
        assertEquals(2, iter.previousIndex());
    }

    @Test
    public void testListIteratorTraversal() {
        ImplicitLinkedHashCollection<TestElement> coll = new ImplicitLinkedHashCollection<>();
        coll.add(new TestElement(1));
        coll.add(new TestElement(2));
        coll.add(new TestElement(3));
        ListIterator<TestElement> iter = coll.valuesList().listIterator();

        // Step the iterator forward to the end of the list
        assertTrue(iter.hasNext());
        assertFalse(iter.hasPrevious());
        assertEquals(0, iter.nextIndex());
        assertEquals(-1, iter.previousIndex());

        assertEquals(1, iter.next().key);
        assertTrue(iter.hasNext());
        assertTrue(iter.hasPrevious());
        assertEquals(1, iter.nextIndex());
        assertEquals(0, iter.previousIndex());

        assertEquals(2, iter.next().key);
        assertTrue(iter.hasNext());
        assertTrue(iter.hasPrevious());
        assertEquals(2, iter.nextIndex());
        assertEquals(1, iter.previousIndex());

        assertEquals(3, iter.next().key);
        assertFalse(iter.hasNext());
        assertTrue(iter.hasPrevious());
        assertEquals(3, iter.nextIndex());
        assertEquals(2, iter.previousIndex());

        // Step back to the middle of the list
        assertEquals(3, iter.previous().key);
        assertTrue(iter.hasNext());
        assertTrue(iter.hasPrevious());
        assertEquals(2, iter.nextIndex());
        assertEquals(1, iter.previousIndex());

        assertEquals(2, iter.previous().key);
        assertTrue(iter.hasNext());
        assertTrue(iter.hasPrevious());
        assertEquals(1, iter.nextIndex());
        assertEquals(0, iter.previousIndex());

        // Step forward one and then back one, return value should remain the same
        assertEquals(2, iter.next().key);
        assertTrue(iter.hasNext());
        assertTrue(iter.hasPrevious());
        assertEquals(2, iter.nextIndex());
        assertEquals(1, iter.previousIndex());

        assertEquals(2, iter.previous().key);
        assertTrue(iter.hasNext());
        assertTrue(iter.hasPrevious());
        assertEquals(1, iter.nextIndex());
        assertEquals(0, iter.previousIndex());

        // Step back to the front of the list
        assertEquals(1, iter.previous().key);
        assertTrue(iter.hasNext());
        assertFalse(iter.hasPrevious());
        assertEquals(0, iter.nextIndex());
        assertEquals(-1, iter.previousIndex());
    }

    @Test
    public void testListIteratorRemove() {
        ImplicitLinkedHashCollection<TestElement> coll = new ImplicitLinkedHashCollection<>();
        coll.add(new TestElement(1));
        coll.add(new TestElement(2));
        coll.add(new TestElement(3));
        coll.add(new TestElement(4));
        coll.add(new TestElement(5));

        ListIterator<TestElement> iter = coll.valuesList().listIterator();
        try {
            iter.remove();
            fail("Calling remove() without calling next() or previous() should raise an exception");
        } catch (IllegalStateException e) {
            // expected
        }

        // Remove after next()
        iter.next();
        iter.next();
        iter.next();
        iter.remove();
        assertTrue(iter.hasNext());
        assertTrue(iter.hasPrevious());
        assertEquals(2, iter.nextIndex());
        assertEquals(1, iter.previousIndex());

        try {
            iter.remove();
            fail("Calling remove() twice without calling next() or previous() in between should raise an exception");
        } catch (IllegalStateException e) {
            // expected
        }

        // Remove after previous()
        assertEquals(2, iter.previous().key);
        iter.remove();
        assertTrue(iter.hasNext());
        assertTrue(iter.hasPrevious());
        assertEquals(1, iter.nextIndex());
        assertEquals(0, iter.previousIndex());

        // Remove the first element of the list
        assertEquals(1, iter.previous().key);
        iter.remove();
        assertTrue(iter.hasNext());
        assertFalse(iter.hasPrevious());
        assertEquals(0, iter.nextIndex());
        assertEquals(-1, iter.previousIndex());

        // Remove the last element of the list
        assertEquals(4, iter.next().key);
        assertEquals(5, iter.next().key);
        iter.remove();
        assertFalse(iter.hasNext());
        assertTrue(iter.hasPrevious());
        assertEquals(1, iter.nextIndex());
        assertEquals(0, iter.previousIndex());

        // Remove the final remaining element of the list
        assertEquals(4, iter.previous().key);
        iter.remove();
        assertFalse(iter.hasNext());
        assertFalse(iter.hasPrevious());
        assertEquals(0, iter.nextIndex());
        assertEquals(-1, iter.previousIndex());

    }

    @Test
    public void testCollisions() {
        ImplicitLinkedHashCollection<TestElement> coll = new ImplicitLinkedHashCollection<>(5);
        assertEquals(11, coll.numSlots());
        assertTrue(coll.add(new TestElement(11)));
        assertTrue(coll.add(new TestElement(0)));
        assertTrue(coll.add(new TestElement(22)));
        assertTrue(coll.add(new TestElement(33)));
        assertEquals(11, coll.numSlots());
        expectTraversal(coll.iterator(), 11, 0, 22, 33);
        assertTrue(coll.remove(new TestElement(22)));
        expectTraversal(coll.iterator(), 11, 0, 33);
        assertEquals(3, coll.size());
        assertFalse(coll.isEmpty());
    }

    @Test
    public void testEnlargement() {
        ImplicitLinkedHashCollection<TestElement> coll = new ImplicitLinkedHashCollection<>(5);
        assertEquals(11, coll.numSlots());
        for (int i = 0; i < 6; i++) {
            assertTrue(coll.add(new TestElement(i)));
        }
        assertEquals(23, coll.numSlots());
        assertEquals(6, coll.size());
        expectTraversal(coll.iterator(), 0, 1, 2, 3, 4, 5);
        for (int i = 0; i < 6; i++) {
            assertTrue(coll.contains(new TestElement(i)), "Failed to find element " + i);
        }
        coll.remove(new TestElement(3));
        assertEquals(23, coll.numSlots());
        assertEquals(5, coll.size());
        expectTraversal(coll.iterator(), 0, 1, 2, 4, 5);
    }

    @Test
    public void testManyInsertsAndDeletes() {
        Random random = new Random(123);
        LinkedHashSet<Integer> existing = new LinkedHashSet<>();
        ImplicitLinkedHashCollection<TestElement> coll = new ImplicitLinkedHashCollection<>();
        for (int i = 0; i < 100; i++) {
            addRandomElement(random, existing, coll);
            addRandomElement(random, existing, coll);
            addRandomElement(random, existing, coll);
            removeRandomElement(random, existing);
            expectTraversal(coll.iterator(), existing.iterator());
        }
    }

    @Test
    public void testInsertingTheSameObjectMultipleTimes() {
        ImplicitLinkedHashCollection<TestElement> coll = new ImplicitLinkedHashCollection<>();
        TestElement element = new TestElement(123);
        assertTrue(coll.add(element));
        assertFalse(coll.add(element));
        assertFalse(coll.add(element));
        assertTrue(coll.remove(element));
        assertFalse(coll.remove(element));
        assertTrue(coll.add(element));
        assertFalse(coll.add(element));
    }

    @Test
    public void testEquals() {
        ImplicitLinkedHashCollection<TestElement> coll1 = new ImplicitLinkedHashCollection<>();
        coll1.add(new TestElement(1));
        coll1.add(new TestElement(2));
        coll1.add(new TestElement(3));

        ImplicitLinkedHashCollection<TestElement> coll2 = new ImplicitLinkedHashCollection<>();
        coll2.add(new TestElement(1));
        coll2.add(new TestElement(2));
        coll2.add(new TestElement(3));

        ImplicitLinkedHashCollection<TestElement> coll3 = new ImplicitLinkedHashCollection<>();
        coll3.add(new TestElement(1));
        coll3.add(new TestElement(3));
        coll3.add(new TestElement(2));

        assertEquals(coll1, coll2);
        assertNotEquals(coll1, coll3);
        assertNotEquals(coll2, coll3);
    }

    @Test
    public void testFindContainsRemoveOnEmptyCollection() {
        ImplicitLinkedHashCollection<TestElement> coll = new ImplicitLinkedHashCollection<>();
        assertNull(coll.find(new TestElement(2)));
        assertFalse(coll.contains(new TestElement(2)));
        assertFalse(coll.remove(new TestElement(2)));
    }

    private void addRandomElement(Random random, LinkedHashSet<Integer> existing,
                                  ImplicitLinkedHashCollection<TestElement> set) {
        int next;
        do {
            next = random.nextInt();
        } while (existing.contains(next));
        existing.add(next);
        set.add(new TestElement(next));
    }

    @SuppressWarnings("unlikely-arg-type")
    private void removeRandomElement(Random random, Collection<Integer> existing) {
        int removeIdx = random.nextInt(existing.size());
        Iterator<Integer> iter = existing.iterator();
        Integer element = null;
        for (int i = 0; i <= removeIdx; i++) {
            element = iter.next();
        }
        existing.remove(new TestElement(element));
    }

    @Test
    public void testSameKeysDifferentValues() {
        ImplicitLinkedHashCollection<TestElement> coll = new ImplicitLinkedHashCollection<>();
        assertTrue(coll.add(new TestElement(1, 1)));
        assertFalse(coll.add(new TestElement(1, 2)));
        TestElement element2 = new TestElement(1, 2);
        TestElement element1 = coll.find(element2);
        assertFalse(element2.equals(element1));
        assertTrue(element2.elementKeysAreEqual(element1));
    }

    @Test
    public void testMoveToEnd() {
        ImplicitLinkedHashCollection<TestElement> coll = new ImplicitLinkedHashCollection<>();
        TestElement e1 = new TestElement(1, 1);
        TestElement e2 = new TestElement(2, 2);
        TestElement e3 = new TestElement(3, 3);
        assertTrue(coll.add(e1));
        assertTrue(coll.add(e2));
        assertTrue(coll.add(e3));
        coll.moveToEnd(e1);
        expectTraversal(coll.iterator(), 2, 3, 1);
        assertThrows(RuntimeException.class, () -> coll.moveToEnd(new TestElement(4, 4)));
    }

    @Test
    public void testRemovals() {
        ImplicitLinkedHashCollection<TestElement> coll = new ImplicitLinkedHashCollection<>();
        List<TestElement> elements  = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            TestElement element  = new TestElement(i, i);
            elements.add(element);
            coll.add(element);
        }
        assertEquals(100, coll.size());
        Iterator<TestElement> iter = coll.iterator();
        for (int i = 0; i < 50; i++) {
            iter.next();
            iter.remove();
        }
        assertEquals(50, coll.size());
        for (int i = 50; i < 100; i++) {
            assertEquals(new TestElement(i, i), coll.find(elements.get(i)));
        }
    }
}
