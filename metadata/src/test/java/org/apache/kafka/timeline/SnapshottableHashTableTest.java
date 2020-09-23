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

package org.apache.kafka.timeline;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class SnapshottableHashTableTest {
    @Rule
    final public Timeout globalTimeout = Timeout.seconds(40);

    /**
     * The class of test elements.
     *
     * This class is intended to help test how the table handles distinct objects which
     * are equal to each other.  Therefore, for the purpose of hashing and equality, we
     * only check i here, and ignore j.
     */
    static class TestElement implements SnapshottableHashTable.ElementWithStartEpoch {
        private final int i;
        private final char j;
        private long startEpoch = Long.MAX_VALUE;

        TestElement(int i, char j) {
            this.i = i;
            this.j = j;
        }

        @Override
        public void setStartEpoch(long startEpoch) {
            this.startEpoch = startEpoch;
        }

        @Override
        public long startEpoch() {
            return startEpoch;
        }

        @Override
        public int hashCode() {
            return i;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof TestElement)) {
                return false;
            }
            TestElement other = (TestElement) o;
            return other.i == i;
        }

        @Override
        public String toString() {
            return String.format("E_%d%c(%s)", i, j, System.identityHashCode(this));
        }
    }

    private static final TestElement E_1A = new TestElement(1, 'A');
    private static final TestElement E_1B = new TestElement(1, 'B');
    private static final TestElement E_2A = new TestElement(2, 'A');
    private static final TestElement E_3A = new TestElement(3, 'A');
    private static final TestElement E_3B = new TestElement(3, 'B');

    @Test
    public void testEmptyTable() {
        SnapshotRegistry registry = new SnapshotRegistry(0);
        SnapshottableHashTable<TestElement> table =
            new SnapshottableHashTable<>(registry, 1);
        assertEquals(0, table.snapshottableSize(Long.MAX_VALUE));
    }

    @Test
    public void testAddAndRemove() {
        SnapshotRegistry registry = new SnapshotRegistry(0);
        SnapshottableHashTable<TestElement> table =
            new SnapshottableHashTable<>(registry, 1);
        assertTrue(null == table.snapshottableAddOrReplace(E_1B));
        assertEquals(1, table.snapshottableSize(Long.MAX_VALUE));
        registry.createSnapshot(0);
        assertTrue(E_1B == table.snapshottableAddOrReplace(E_1A));
        assertTrue(E_1B == table.snapshottableGet(E_1A, 0));
        assertTrue(E_1A == table.snapshottableGet(E_1A, Long.MAX_VALUE));
        assertEquals(null, table.snapshottableAddOrReplace(E_2A));
        assertEquals(null, table.snapshottableAddOrReplace(E_3A));
        assertEquals(3, table.snapshottableSize(Long.MAX_VALUE));
        assertEquals(1, table.snapshottableSize(0));
        registry.createSnapshot(1);
        assertEquals(E_1A, table.snapshottableRemove(E_1B));
        assertEquals(E_2A, table.snapshottableRemove(E_2A));
        assertEquals(E_3A, table.snapshottableRemove(E_3A));
        assertEquals(0, table.snapshottableSize(Long.MAX_VALUE));
        assertEquals(1, table.snapshottableSize(0));
        assertEquals(3, table.snapshottableSize(1));
        registry.deleteSnapshot(0);
        Assert.assertEquals("No snapshot for epoch 0",
            Assert.assertThrows(RuntimeException.class, () ->
                table.snapshottableSize(0)).getMessage());
        registry.deleteSnapshot(1);
        assertEquals(0, table.snapshottableSize(Long.MAX_VALUE));
    }

    @Test
    public void testIterateOverSnapshot() {
        SnapshotRegistry registry = new SnapshotRegistry(0);
        SnapshottableHashTable<TestElement> table =
            new SnapshottableHashTable<>(registry, 1);
        assertTrue(table.snapshottableAddUnlessPresent(E_1B));
        assertFalse(table.snapshottableAddUnlessPresent(E_1A));
        assertTrue(table.snapshottableAddUnlessPresent(E_2A));
        assertTrue(table.snapshottableAddUnlessPresent(E_3A));
        registry.createSnapshot(0);
        assertIteratorContains(table.snapshottableIterator(0), E_1B, E_2A, E_3A);
        table.snapshottableRemove(E_1B);
        assertIteratorContains(table.snapshottableIterator(0), E_1B, E_2A, E_3A);
        table.snapshottableRemove(E_1A);
        assertIteratorContains(table.snapshottableIterator(Long.MAX_VALUE), E_2A, E_3A);
        table.snapshottableRemove(E_2A);
        table.snapshottableRemove(E_3A);
        assertIteratorContains(table.snapshottableIterator(0), E_1B, E_2A, E_3A);
    }

    @Test
    public void testIterateOverSnapshotWhileExpandingTable() {
        SnapshotRegistry registry = new SnapshotRegistry(0);
        SnapshottableHashTable<TestElement> table =
            new SnapshottableHashTable<>(registry, 1);
        assertEquals(null, table.snapshottableAddOrReplace(E_1A));
        registry.createSnapshot(0);
        Iterator<TestElement> iter = table.snapshottableIterator(0);
        assertTrue(table.snapshottableAddUnlessPresent(E_2A));
        assertTrue(table.snapshottableAddUnlessPresent(E_3A));
        assertIteratorContains(iter, E_1A);
    }

    @Test
    public void testIterateOverSnapshotWhileDeletingAndReplacing() {
        SnapshotRegistry registry = new SnapshotRegistry(0);
        SnapshottableHashTable<TestElement> table =
            new SnapshottableHashTable<>(registry, 1);
        assertEquals(null, table.snapshottableAddOrReplace(E_1A));
        assertEquals(null, table.snapshottableAddOrReplace(E_2A));
        assertEquals(null, table.snapshottableAddOrReplace(E_3A));
        assertEquals(E_1A, table.snapshottableRemove(E_1A));
        assertEquals(null, table.snapshottableAddOrReplace(E_1B));
        registry.createSnapshot(0);
        Iterator<TestElement> iter = table.snapshottableIterator(0);
        List<TestElement> iterElements = new ArrayList<>();
        iterElements.add(iter.next());
        assertEquals(E_2A, table.snapshottableRemove(E_2A));
        assertEquals(E_3A, table.snapshottableAddOrReplace(E_3B));
        iterElements.add(iter.next());
        assertEquals(E_1B, table.snapshottableRemove(E_1B));
        iterElements.add(iter.next());
        assertFalse(iter.hasNext());
        assertIteratorContains(iterElements.iterator(), E_1B, E_2A, E_3A);
    }

    @Test
    public void testRevert() {
        SnapshotRegistry registry = new SnapshotRegistry(0);
        SnapshottableHashTable<TestElement> table =
            new SnapshottableHashTable<>(registry, 1);
        assertEquals(null, table.snapshottableAddOrReplace(E_1A));
        assertEquals(null, table.snapshottableAddOrReplace(E_2A));
        assertEquals(null, table.snapshottableAddOrReplace(E_3A));
        registry.createSnapshot(0);
        assertEquals(E_1A, table.snapshottableAddOrReplace(E_1B));
        assertEquals(E_3A, table.snapshottableAddOrReplace(E_3B));
        registry.createSnapshot(1);
        assertEquals(3, table.snapshottableSize(Long.MAX_VALUE));
        assertIteratorContains(table.snapshottableIterator(Long.MAX_VALUE), E_1B, E_2A, E_3B);
        table.snapshottableRemove(E_1B);
        table.snapshottableRemove(E_2A);
        table.snapshottableRemove(E_3B);
        assertEquals(0, table.snapshottableSize(Long.MAX_VALUE));
        assertEquals(3, table.snapshottableSize(0));
        assertEquals(3, table.snapshottableSize(1));
        registry.revertToSnapshot(0);
        assertIteratorContains(table.snapshottableIterator(Long.MAX_VALUE), E_1A, E_2A, E_3A);
    }

    /**
     * Assert that the given iterator contains the given elements, in any order.
     * We compare using reference equality here, rather than object equality.
     */
    private static void assertIteratorContains(Iterator<? extends Object> iter,
                                               Object... expected) {
        IdentityHashMap<Object, Boolean> remaining = new IdentityHashMap<>();
        for (Object object : expected) {
            remaining.put(object, true);
        }
        List<Object> extraObjects = new ArrayList<>();
        int i = 0;
        while (iter.hasNext()) {
            Object object = iter.next();
            assertNotNull(object);
            if (remaining.remove(object) == null) {
                extraObjects.add(object);
            }
        }
        if (!extraObjects.isEmpty() || !remaining.isEmpty()) {
            throw new RuntimeException("Found extra object(s): [" + String.join(", ",
                extraObjects.stream().map(e -> e.toString()).collect(Collectors.toList())) +
                "] and didn't find object(s): [" + String.join(", ",
                remaining.keySet().stream().map(e -> e.toString()).collect(Collectors.toList())) + "]");
        }
    }
}
