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

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;

/**
 * A unit test for ImplicitLinkedHashSet.
 */
public class ImplicitLinkedHashSetTest {
    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    final static class TestElement implements ImplicitLinkedHashSet.Element {
        private int prev = ImplicitLinkedHashSet.INVALID_INDEX;
        private int next = ImplicitLinkedHashSet.INVALID_INDEX;
        private final int val;

        TestElement(int val) {
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
        ImplicitLinkedHashMultiSet<TestElement> multiSet = new ImplicitLinkedHashMultiSet<>();
        assertFalse(multiSet.add(null));
    }

    @Test
    public void testInsertDelete() {
        ImplicitLinkedHashSet<TestElement> set = new ImplicitLinkedHashSet<>(100);
        assertTrue(set.add(new TestElement(1)));
        TestElement second = new TestElement(2);
        assertTrue(set.add(second));
        assertTrue(set.add(new TestElement(3)));
        assertFalse(set.add(new TestElement(3)));
        assertEquals(3, set.size());
        assertTrue(set.contains(new TestElement(1)));
        assertFalse(set.contains(new TestElement(4)));
        TestElement secondAgain = set.find(new TestElement(2));
        assertTrue(second == secondAgain);
        assertTrue(set.remove(new TestElement(1)));
        assertFalse(set.remove(new TestElement(1)));
        assertEquals(2, set.size());
        set.clear();
        assertEquals(0, set.size());
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

    static void expectTraversal(Iterator<TestElement> iter, Iterator<Integer> expectedIter) {
        int i = 0;
        while (iter.hasNext()) {
            TestElement element = iter.next();
            Assert.assertTrue("Iterator yieled " + (i + 1) + " elements, but only " +
                i + " were expected.", expectedIter.hasNext());
            Integer expected = expectedIter.next();
            Assert.assertEquals("Iterator value number " + (i + 1) + " was incorrect.",
                expected.intValue(), element.val);
            i = i + 1;
        }
        Assert.assertFalse("Iterator yieled " + i + " elements, but at least " +
            (i + 1) + " were expected.", expectedIter.hasNext());
    }

    @Test
    public void testTraversal() {
        ImplicitLinkedHashSet<TestElement> set = new ImplicitLinkedHashSet<>();
        expectTraversal(set.iterator());
        assertTrue(set.add(new TestElement(2)));
        expectTraversal(set.iterator(), 2);
        assertTrue(set.add(new TestElement(1)));
        expectTraversal(set.iterator(), 2, 1);
        assertTrue(set.add(new TestElement(100)));
        expectTraversal(set.iterator(), 2, 1, 100);
        assertTrue(set.remove(new TestElement(1)));
        expectTraversal(set.iterator(), 2, 100);
        assertTrue(set.add(new TestElement(1)));
        expectTraversal(set.iterator(), 2, 100, 1);
        Iterator<TestElement> iter = set.iterator();
        iter.next();
        iter.next();
        iter.remove();
        iter.next();
        assertFalse(iter.hasNext());
        expectTraversal(set.iterator(), 2, 1);
        List<TestElement> list = new ArrayList<>();
        list.add(new TestElement(1));
        list.add(new TestElement(2));
        assertTrue(set.removeAll(list));
        assertFalse(set.removeAll(list));
        expectTraversal(set.iterator());
        assertEquals(0, set.size());
        assertTrue(set.isEmpty());
    }

    @Test
    public void testCollisions() {
        ImplicitLinkedHashSet<TestElement> set = new ImplicitLinkedHashSet<>(5);
        assertEquals(11, set.numSlots());
        assertTrue(set.add(new TestElement(11)));
        assertTrue(set.add(new TestElement(0)));
        assertTrue(set.add(new TestElement(22)));
        assertTrue(set.add(new TestElement(33)));
        assertEquals(11, set.numSlots());
        expectTraversal(set.iterator(), 11, 0, 22, 33);
        assertTrue(set.remove(new TestElement(22)));
        expectTraversal(set.iterator(), 11, 0, 33);
        assertEquals(3, set.size());
        assertFalse(set.isEmpty());
    }

    @Test
    public void testEnlargement() {
        ImplicitLinkedHashSet<TestElement> set = new ImplicitLinkedHashSet<>(5);
        assertEquals(11, set.numSlots());
        for (int i = 0; i < 6; i++) {
            assertTrue(set.add(new TestElement(i)));
        }
        assertEquals(23, set.numSlots());
        assertEquals(6, set.size());
        expectTraversal(set.iterator(), 0, 1, 2, 3, 4, 5);
        for (int i = 0; i < 6; i++) {
            assertTrue("Failed to find element " + i, set.contains(new TestElement(i)));
        }
        set.remove(new TestElement(3));
        assertEquals(23, set.numSlots());
        assertEquals(5, set.size());
        expectTraversal(set.iterator(), 0, 1, 2, 4, 5);
    }

    @Test
    public void testManyInsertsAndDeletes() {
        Random random = new Random(123);
        LinkedHashSet<Integer> existing = new LinkedHashSet<>();
        ImplicitLinkedHashSet<TestElement> set = new ImplicitLinkedHashSet<>();
        for (int i = 0; i < 100; i++) {
            addRandomElement(random, existing, set);
            addRandomElement(random, existing, set);
            addRandomElement(random, existing, set);
            removeRandomElement(random, existing, set);
            expectTraversal(set.iterator(), existing.iterator());
        }
    }

    private void addRandomElement(Random random, LinkedHashSet<Integer> existing,
                                  ImplicitLinkedHashSet<TestElement> set) {
        int next;
        do {
            next = random.nextInt();
        } while (existing.contains(next));
        existing.add(next);
        set.add(new TestElement(next));
    }

    private void removeRandomElement(Random random, Collection<Integer> existing,
                             ImplicitLinkedHashSet<TestElement> set) {
        int removeIdx = random.nextInt(existing.size());
        Iterator<Integer> iter = existing.iterator();
        Integer element = null;
        for (int i = 0; i <= removeIdx; i++) {
            element = iter.next();
        }
        existing.remove(new TestElement(element));
    }
}
