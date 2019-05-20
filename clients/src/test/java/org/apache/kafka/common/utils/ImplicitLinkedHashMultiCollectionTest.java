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

import org.apache.kafka.common.utils.ImplicitLinkedHashCollectionTest.TestElement;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A unit test for ImplicitLinkedHashMultiCollection.
 */
public class ImplicitLinkedHashMultiCollectionTest {
    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    @Test
    public void testNullForbidden() {
        ImplicitLinkedHashMultiCollection<TestElement> multiSet = new ImplicitLinkedHashMultiCollection<>();
        assertFalse(multiSet.add(null));
    }

    @Test
    public void testInsertDelete() {
        ImplicitLinkedHashMultiCollection<TestElement> multiSet = new ImplicitLinkedHashMultiCollection<>(100);
        TestElement e1 = new TestElement(1);
        TestElement e2 = new TestElement(1);
        TestElement e3 = new TestElement(2);
        multiSet.mustAdd(e1);
        multiSet.mustAdd(e2);
        multiSet.mustAdd(e3);
        assertFalse(multiSet.add(e3));
        assertEquals(3, multiSet.size());
        expectExactTraversal(multiSet.findAll(e1).iterator(), e1, e2);
        expectExactTraversal(multiSet.findAll(e3).iterator(), e3);
        multiSet.remove(e2);
        expectExactTraversal(multiSet.findAll(e1).iterator(), e1);
        assertTrue(multiSet.contains(e2));
    }

    @Test
    public void testTraversal() {
        ImplicitLinkedHashMultiCollection<TestElement> multiSet = new ImplicitLinkedHashMultiCollection<>();
        expectExactTraversal(multiSet.iterator());
        TestElement e1 = new TestElement(1);
        TestElement e2 = new TestElement(1);
        TestElement e3 = new TestElement(2);
        assertTrue(multiSet.add(e1));
        assertTrue(multiSet.add(e2));
        assertTrue(multiSet.add(e3));
        expectExactTraversal(multiSet.iterator(), e1, e2, e3);
        assertTrue(multiSet.remove(e2));
        expectExactTraversal(multiSet.iterator(), e1, e3);
        assertTrue(multiSet.remove(e1));
        expectExactTraversal(multiSet.iterator(), e3);
    }

    static void expectExactTraversal(Iterator<TestElement> iterator, TestElement... sequence) {
        int i = 0;
        while (iterator.hasNext()) {
            TestElement element = iterator.next();
            assertTrue("Iterator yieled " + (i + 1) + " elements, but only " +
                sequence.length + " were expected.", i < sequence.length);
            if (sequence[i] != element) {
                fail("Iterator value number " + (i + 1) + " was incorrect.");
            }
            i = i + 1;
        }
        assertTrue("Iterator yieled " + (i + 1) + " elements, but " +
            sequence.length + " were expected.", i == sequence.length);
    }

    @Test
    public void testEnlargement() {
        ImplicitLinkedHashMultiCollection<TestElement> multiSet = new ImplicitLinkedHashMultiCollection<>(5);
        assertEquals(11, multiSet.numSlots());
        TestElement[] testElements = {
            new TestElement(100),
            new TestElement(101),
            new TestElement(102),
            new TestElement(100),
            new TestElement(101),
            new TestElement(105)
        };
        for (int i = 0; i < testElements.length; i++) {
            assertTrue(multiSet.add(testElements[i]));
        }
        for (int i = 0; i < testElements.length; i++) {
            assertFalse(multiSet.add(testElements[i]));
        }
        assertEquals(23, multiSet.numSlots());
        assertEquals(testElements.length, multiSet.size());
        expectExactTraversal(multiSet.iterator(), testElements);
        multiSet.remove(testElements[1]);
        assertEquals(23, multiSet.numSlots());
        assertEquals(5, multiSet.size());
        expectExactTraversal(multiSet.iterator(),
            testElements[0], testElements[2], testElements[3], testElements[4], testElements[5]);
    }

    @Test
    public void testManyInsertsAndDeletes() {
        Random random = new Random(123);
        LinkedList<TestElement> existing = new LinkedList<>();
        ImplicitLinkedHashMultiCollection<TestElement> multiSet = new ImplicitLinkedHashMultiCollection<>();
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 4; j++) {
                TestElement testElement = new TestElement(random.nextInt());
                multiSet.mustAdd(testElement);
                existing.add(testElement);
            }
            int elementToRemove = random.nextInt(multiSet.size());
            Iterator<TestElement> iter1 = multiSet.iterator();
            Iterator<TestElement> iter2 = existing.iterator();
            for (int j = 0; j <= elementToRemove; j++) {
                iter1.next();
                iter2.next();
            }
            iter1.remove();
            iter2.remove();
            expectTraversal(multiSet.iterator(), existing.iterator());
        }
    }

    void expectTraversal(Iterator<TestElement> iter, Iterator<TestElement> expectedIter) {
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
}
