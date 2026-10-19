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
package org.apache.kafka.coordinator.group.util;

import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SmallIntegerSetTest {

    @Test
    public void testNegativeIntegersAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> new SmallIntegerSet(Set.of(1, -1)));
    }

    @Test
    public void testSize() {
        assertEquals(0, new SmallIntegerSet(Set.of()).size());
        assertEquals(3, new SmallIntegerSet(Set.of(1, 3, 5)).size());
    }

    @Test
    public void testContains() {
        Set<Integer> reference = Set.of(1, 3, 5);
        SmallIntegerSet set = new SmallIntegerSet(reference);

        for (int i = -1; i <= 128; i++) {
            assertEquals(reference.contains(i), set.contains(i), "contains(" + i + ")");
            assertEquals(reference.contains(i), set.contains(Integer.valueOf(i)), "contains(Integer.valueOf(" + i + "))");
        }

        assertFalse(set.contains((Object) 1L));
        assertEquals(reference.contains((Object) 1L), set.contains((Object) 1L));
        assertFalse(set.contains(null));
    }

    @Test
    public void testIterator() {
        Iterator<Integer> iterator = new SmallIntegerSet(Set.of(1, 3, 5)).iterator();

        assertTrue(iterator.hasNext());
        assertEquals(1, iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals(3, iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals(5, iterator.next());
        assertFalse(iterator.hasNext());
        assertThrows(NoSuchElementException.class, iterator::next);
    }

    @Test
    public void testHashCode() {
        assertEquals(Set.of(1, 3, 5).hashCode(), new SmallIntegerSet(Set.of(1, 3, 5)).hashCode());
    }

    @Test
    public void testEquals() {
        assertEquals(Set.of(1, 3, 5), new SmallIntegerSet(Set.of(1, 3, 5)));
    }
}
