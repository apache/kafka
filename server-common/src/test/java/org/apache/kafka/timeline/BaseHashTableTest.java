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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 40)
public class BaseHashTableTest {

    @Test
    public void testEmptyTable() {
        BaseHashTable<Integer> table = new BaseHashTable<>(0);
        assertEquals(0, table.baseSize());
        assertNull(table.baseGet(1));
    }

    @Test
    public void testFindSlot() {
        Random random = new Random(123);
        for (int i = 1; i <= 5; i++) {
            int numSlots = 2 << i;
            HashSet<Integer> slotsReturned = new HashSet<>();
            while (slotsReturned.size() < numSlots) {
                int slot = BaseHashTable.findSlot(random.nextInt(), numSlots);
                assertTrue(slot >= 0);
                assertTrue(slot < numSlots);
                slotsReturned.add(slot);
            }
        }
    }

    @Test
    public void testInsertAndRemove() {
        BaseHashTable<Integer> table = new BaseHashTable<>(20);
        Integer one = 1;
        Integer two = 2;
        Integer three = 3;
        Integer four = 4;
        assertNull(table.baseAddOrReplace(one));
        assertNull(table.baseAddOrReplace(two));
        assertNull(table.baseAddOrReplace(three));
        assertEquals(3, table.baseSize());
        assertEquals(one, table.baseGet(one));
        assertEquals(two, table.baseGet(two));
        assertEquals(three, table.baseGet(three));
        assertNull(table.baseGet(four));
        assertEquals(one, table.baseRemove(one));
        assertEquals(2, table.baseSize());
        assertNull(table.baseGet(one));
        assertEquals(2, table.baseSize());
    }

    static class Foo {
        @Override
        public boolean equals(Object o) {
            return this == o;
        }

        @Override
        public int hashCode() {
            return 42;
        }
    }

    @Test
    public void testHashCollisions() {
        Foo one = new Foo();
        Foo two = new Foo();
        Foo three = new Foo();
        Foo four = new Foo();
        BaseHashTable<Foo> table = new BaseHashTable<>(20);
        assertNull(table.baseAddOrReplace(one));
        assertNull(table.baseAddOrReplace(two));
        assertNull(table.baseAddOrReplace(three));
        assertEquals(3, table.baseSize());
        assertEquals(one, table.baseGet(one));
        assertEquals(two, table.baseGet(two));
        assertEquals(three, table.baseGet(three));
        assertNull(table.baseGet(four));
        assertEquals(one, table.baseRemove(one));
        assertEquals(three, table.baseRemove(three));
        assertEquals(1, table.baseSize());
        assertNull(table.baseGet(four));
        assertEquals(two, table.baseGet(two));
        assertEquals(two, table.baseRemove(two));
        assertEquals(0, table.baseSize());
    }

    @Test
    public void testExpansion() {
        BaseHashTable<Integer> table = new BaseHashTable<>(0);

        for (int i = 0; i < 4096; i++) {
            assertEquals(i, table.baseSize());
            assertNull(table.baseAddOrReplace(i));
        }

        for (int i = 0; i < 4096; i++) {
            assertEquals(4096 - i, table.baseSize());
            assertEquals(Integer.valueOf(i), table.baseRemove(i));
        }
    }

    @Test
    public void testExpectedSizeToCapacity() {
        assertEquals(2, BaseHashTable.expectedSizeToCapacity(Integer.MIN_VALUE));
        assertEquals(2, BaseHashTable.expectedSizeToCapacity(-123));
        assertEquals(2, BaseHashTable.expectedSizeToCapacity(0));
        assertEquals(2, BaseHashTable.expectedSizeToCapacity(1));
        assertEquals(4, BaseHashTable.expectedSizeToCapacity(2));
        assertEquals(4, BaseHashTable.expectedSizeToCapacity(3));
        assertEquals(8, BaseHashTable.expectedSizeToCapacity(4));
        assertEquals(16, BaseHashTable.expectedSizeToCapacity(12));
        assertEquals(32, BaseHashTable.expectedSizeToCapacity(13));
        assertEquals(0x2000000, BaseHashTable.expectedSizeToCapacity(0x1010400));
        assertEquals(0x4000000, BaseHashTable.expectedSizeToCapacity(0x2000000));
        assertEquals(0x4000000, BaseHashTable.expectedSizeToCapacity(0x2000001));
        assertEquals(BaseHashTable.MAX_CAPACITY, BaseHashTable.expectedSizeToCapacity(BaseHashTable.MAX_CAPACITY));
        assertEquals(BaseHashTable.MAX_CAPACITY, BaseHashTable.expectedSizeToCapacity(BaseHashTable.MAX_CAPACITY + 1));
        assertEquals(BaseHashTable.MAX_CAPACITY, BaseHashTable.expectedSizeToCapacity(Integer.MAX_VALUE - 1));
        assertEquals(BaseHashTable.MAX_CAPACITY, BaseHashTable.expectedSizeToCapacity(Integer.MAX_VALUE));
    }
}
