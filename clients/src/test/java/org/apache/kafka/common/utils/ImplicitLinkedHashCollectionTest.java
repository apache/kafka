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

import org.apache.kafka.common.utils.ImplicitLinkedHashCollection.Element;
import org.apache.kafka.common.utils.ImplicitLinkedHashCollection.ObjectEqualityComparator;
import org.apache.kafka.common.utils.ImplicitLinkedHashCollection.ReferenceEqualityComparator;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * A unit test for ImplicitLinkedHashCollection.
 */
public class ImplicitLinkedHashCollectionTest {
    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    final static class TestElement implements Element {
        private int prev = ImplicitLinkedHashCollection.INVALID_INDEX;
        private int next = ImplicitLinkedHashCollection.INVALID_INDEX;
        private final int val;
        private final boolean wasDuplicated;

        TestElement(int val) {
            this.val = val;
            this.wasDuplicated = false;
        }

        private TestElement(int val, boolean wasDuplicated) {
            this.val = val;
            this.wasDuplicated = wasDuplicated;
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
        public Element duplicate() {
            return new TestElement(val, true);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if ((o == null) || (o.getClass() != TestElement.class)) return false;
            TestElement that = (TestElement) o;
            return val == that.val;
        }

        @Override
        public String toString() {
            return "TestElement(" + val + ")";
        }

        @Override
        public int hashCode() {
            return val;
        }
    }

    @Test
    public void testNullForbidden() {
        ImplicitLinkedHashCollection<TestElement> coll = new ImplicitLinkedHashCollection<>();
        assertFalse(coll.add(null));
    }

    @Test
    public void testInsertDelete() {
        ImplicitLinkedHashCollection<TestElement> coll = new ImplicitLinkedHashCollection<>(100);
        assertTrue(coll.add(new TestElement(1)));
        TestElement second = new TestElement(2);
        assertTrue(coll.add(second));
        assertTrue(coll.add(new TestElement(3)));
        assertTrue(coll.addOrReplace(new TestElement(3), ObjectEqualityComparator.INSTANCE));
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

    @Test
    public void testInsertDeleteDuplicates() {
        ImplicitLinkedHashCollection<TestElement> coll = new ImplicitLinkedHashCollection<>(100);
        TestElement e1 = new TestElement(1);
        TestElement e2 = new TestElement(1);
        TestElement e3 = new TestElement(2);
        assertTrue(coll.add(e1));
        assertTrue(coll.add(e2));
        assertTrue(coll.add(e3));
        assertTrue(coll.add(e3));
        assertEquals(4, coll.size());
        List<TestElement> ones = coll.findAll(e1);
        assertEquals(Arrays.asList(e1, e1), ones);
        assertTrue(e1 == coll.find(e1, ReferenceEqualityComparator.INSTANCE));
        assertTrue(e2 == coll.find(e2, ReferenceEqualityComparator.INSTANCE));
        List<TestElement> threes = coll.findAll(e3);
        assertEquals(2, threes.size());
        assertEquals(1, threes.stream().filter(e -> e == e3).count());
        assertEquals(2, threes.stream().filter(e -> e.equals(e3)).count());
        coll.remove(e2);
    }

    static void expectTraversal(Iterator<TestElement> iterator, Integer... sequence) {
        int i = 0;
        while (iterator.hasNext()) {
            TestElement element = iterator.next();
            Assert.assertTrue("Iterator yieled " + (i + 1) + " elements, but only " +
                    sequence.length + " were expected.", i < sequence.length);
            Assert.assertEquals("Iterator value number " + (i + 1) + " was incorrect.",
                    sequence[i].intValue(), element.val);
            i = i + 1;
        }
        Assert.assertTrue("Iterator yieled " + (i + 1) + " elements, but " +
                sequence.length + " were expected.", i == sequence.length);
    }

    static void expectTraversal(Iterator<TestElement> iter, Iterator<TestElement> expectedIter) {
        int i = 0;
        while (iter.hasNext()) {
            TestElement element = iter.next();
            Assert.assertTrue("Iterator yieled " + (i + 1) + " elements, but only " +
                i + " were expected.", expectedIter.hasNext());
            TestElement expected = expectedIter.next();
            assertTrue("Iterator value number " + (i + 1) + " was incorrect.",
                expected == element);
            i = i + 1;
        }
        Assert.assertFalse("Iterator yieled " + i + " elements, but at least " +
            (i + 1) + " were expected.", expectedIter.hasNext());
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
    public void testRetainAll() {
        ImplicitLinkedHashCollection<TestElement> coll = new ImplicitLinkedHashCollection<>();
        assertTrue(coll.addAll(
            Arrays.asList(new TestElement(2), new TestElement(3), new TestElement(1))));
        assertTrue(coll.addAll(
            Arrays.asList(new TestElement(3), new TestElement(2), new TestElement(1))));
        assertFalse(coll.retainAll(
            Arrays.asList(new TestElement(1), new TestElement(2), new TestElement(3))));
        assertEquals(6, coll.size());
        assertTrue(coll.containsAll(
            Arrays.asList(new TestElement(1), new TestElement(3))));
        assertFalse(coll.containsAll(
            Arrays.asList(new TestElement(1), new TestElement(3), new TestElement(4))));
        assertTrue(coll.containsAll(
            Arrays.asList(new TestElement(1), new TestElement(2), new TestElement(3))));
        assertTrue(coll.retainAll(
            Arrays.asList(new TestElement(1), new TestElement(3))));
        assertEquals(4, coll.size());
        assertTrue(coll.containsAll(
            Arrays.asList(new TestElement(1), new TestElement(3))));
        assertFalse(coll.containsAll(
            Arrays.asList(new TestElement(1), new TestElement(2), new TestElement(3))));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testToArray() {
        TestElement[] elements = new TestElement[] {
            new TestElement(1),
            new TestElement(5),
            new TestElement(2)
        };
        ImplicitLinkedHashCollection<TestElement> coll = new ImplicitLinkedHashCollection<>();
        assertTrue(coll.addAll(Arrays.asList(elements)));
        Object[] elements2 = coll.toArray();
        assertArrayEquals(elements, elements2);
        TestElement[] elements3 = new TestElement[2];
        assertArrayEquals(elements, coll.toArray(elements3));
        TestElement[] elements4 = new TestElement[3];
        assertArrayEquals(elements, coll.toArray(elements4));
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
        int val = 3;
        for (TestElement e : coll) {
            assertEquals(val, e.val);
            ++val;
        }
    }

    @Test
    public void testListViewGet() {
        ImplicitLinkedHashCollection<TestElement> coll = new ImplicitLinkedHashCollection<>();
        coll.add(new TestElement(1));
        coll.add(new TestElement(2));
        coll.add(new TestElement(3));

        List<TestElement> list = coll.valuesList();
        assertEquals(1, list.get(0).val);
        assertEquals(2, list.get(1).val);
        assertEquals(3, list.get(2).val);
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
        assertEquals(3, list.get(0).val);
        assertEquals(1, list.size());

        // Addition to collection is reflected in list
        coll.add(new TestElement(4));
        assertEquals(3, list.get(0).val);
        assertEquals(4, list.get(1).val);
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

        assertEquals(1, iter.next().val);
        assertTrue(iter.hasNext());
        assertTrue(iter.hasPrevious());
        assertEquals(1, iter.nextIndex());
        assertEquals(0, iter.previousIndex());

        assertEquals(2, iter.next().val);
        assertTrue(iter.hasNext());
        assertTrue(iter.hasPrevious());
        assertEquals(2, iter.nextIndex());
        assertEquals(1, iter.previousIndex());

        assertEquals(3, iter.next().val);
        assertFalse(iter.hasNext());
        assertTrue(iter.hasPrevious());
        assertEquals(3, iter.nextIndex());
        assertEquals(2, iter.previousIndex());

        // Step back to the middle of the list
        assertEquals(3, iter.previous().val);
        assertTrue(iter.hasNext());
        assertTrue(iter.hasPrevious());
        assertEquals(2, iter.nextIndex());
        assertEquals(1, iter.previousIndex());

        assertEquals(2, iter.previous().val);
        assertTrue(iter.hasNext());
        assertTrue(iter.hasPrevious());
        assertEquals(1, iter.nextIndex());
        assertEquals(0, iter.previousIndex());

        // Step forward one and then back one, return value should remain the same
        assertEquals(2, iter.next().val);
        assertTrue(iter.hasNext());
        assertTrue(iter.hasPrevious());
        assertEquals(2, iter.nextIndex());
        assertEquals(1, iter.previousIndex());

        assertEquals(2, iter.previous().val);
        assertTrue(iter.hasNext());
        assertTrue(iter.hasPrevious());
        assertEquals(1, iter.nextIndex());
        assertEquals(0, iter.previousIndex());

        // Step back to the front of the list
        assertEquals(1, iter.previous().val);
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
        assertEquals(2, iter.previous().val);
        iter.remove();
        assertTrue(iter.hasNext());
        assertTrue(iter.hasPrevious());
        assertEquals(1, iter.nextIndex());
        assertEquals(0, iter.previousIndex());

        // Remove the first element of the list
        assertEquals(1, iter.previous().val);
        iter.remove();
        assertTrue(iter.hasNext());
        assertFalse(iter.hasPrevious());
        assertEquals(0, iter.nextIndex());
        assertEquals(-1, iter.previousIndex());

        // Remove the last element of the list
        assertEquals(4, iter.next().val);
        assertEquals(5, iter.next().val);
        iter.remove();
        assertFalse(iter.hasNext());
        assertTrue(iter.hasPrevious());
        assertEquals(1, iter.nextIndex());
        assertEquals(0, iter.previousIndex());

        // Remove the final remaining element of the list
        assertEquals(4, iter.previous().val);
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
            assertTrue("Failed to find element " + i, coll.contains(new TestElement(i)));
        }
        coll.remove(new TestElement(3));
        assertEquals(23, coll.numSlots());
        assertEquals(5, coll.size());
        expectTraversal(coll.iterator(), 0, 1, 2, 4, 5);
    }

    @Test
    public void testManyInsertsAndDeletes() {
        Random random = new Random(123);
        LinkedList<TestElement> existing = new LinkedList<>();
        ImplicitLinkedHashCollection<TestElement> coll = new ImplicitLinkedHashCollection<>();
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 4; j++) {
                TestElement testElement = new TestElement(random.nextInt());
                coll.add(testElement);
                existing.add(testElement);
            }
            int elementToRemove = random.nextInt(coll.size());
            Iterator<TestElement> iter1 = coll.iterator();
            Iterator<TestElement> iter2 = existing.iterator();
            for (int j = 0; j <= elementToRemove; j++) {
                iter1.next();
                iter2.next();
            }
            iter1.remove();
            iter2.remove();
            expectTraversal(coll.iterator(), existing.iterator());
        }
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

        assertTrue(coll1.toString() + " was not equal to " + coll2.toString(),
            coll1.equals(coll2));
        assertFalse(coll1.toString() + " was equal to " + coll3.toString(),
            coll1.equals(coll3));
        assertFalse(coll2.toString() + " was equal to " + coll3.toString(),
            coll2.equals(coll3));
    }

    @Test
    public void testFindContainsRemoveOnEmptyCollection() {
        ImplicitLinkedHashCollection<TestElement> coll = new ImplicitLinkedHashCollection<>();
        assertNull(coll.find(new TestElement(2)));
        assertFalse(coll.contains(new TestElement(2)));
        assertFalse(coll.remove(new TestElement(2)));
    }

    @Test
    public void testFindFindAllContainsRemoveOnEmptyCollection() {
        ImplicitLinkedHashCollection<TestElement> coll = new ImplicitLinkedHashCollection<>();
        assertNull(coll.find(new TestElement(2)));
        assertFalse(coll.contains(new TestElement(2)));
        assertFalse(coll.remove(new TestElement(2)));
        assertTrue(coll.findAll(new TestElement(2)).isEmpty());
    }

    @Test
    public void testReinsertExistingElement() {
        ImplicitLinkedHashCollection<TestElement> coll = new ImplicitLinkedHashCollection<>();
        TestElement testElement = new TestElement(1);
        assertTrue(coll.add(testElement));
        assertTrue(coll.add(testElement));
        Iterator<TestElement> iter = coll.iterator();
        assertTrue(iter.hasNext());
        assertEquals(testElement, iter.next());
        assertFalse(testElement.wasDuplicated);
        assertTrue(iter.hasNext());
        TestElement clone = iter.next();
        assertTrue(clone.wasDuplicated);
        assertEquals(1, clone.val);
    }
}
