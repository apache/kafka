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

import java.util.HashSet;
import java.util.Random;

public class BaseHashTableTest {
    @Rule
    final public Timeout globalTimeout = Timeout.seconds(40);

    @Test
    public void testEmptyTable() {
        BaseHashTable<Integer> table = new BaseHashTable<>(0);
        Assert.assertEquals(0, table.baseSize());
        Assert.assertEquals(null, table.baseGet(Integer.valueOf(1)));
    }

    @Test
    public void testFindSlot() {
        Random random = new Random(123);
        for (int i = 1; i <= 5; i++) {
            int numSlots = 2 << i;
            HashSet<Integer> slotsReturned = new HashSet<>();
            while (slotsReturned.size() < numSlots) {
                int slot = BaseHashTable.findSlot(random.nextInt(), numSlots);
                Assert.assertTrue(slot >= 0);
                Assert.assertTrue(slot < numSlots);
                slotsReturned.add(slot);
            }
        }
    }

    @Test
    public void testInsertAndRemove() {
        BaseHashTable<Integer> table = new BaseHashTable<>(20);
        Integer one = Integer.valueOf(1);
        Integer two = Integer.valueOf(2);
        Integer three = Integer.valueOf(3);
        Integer four = Integer.valueOf(4);
        Assert.assertEquals(null, table.baseAddOrReplace(one));
        Assert.assertEquals(null, table.baseAddOrReplace(two));
        Assert.assertEquals(null, table.baseAddOrReplace(three));
        Assert.assertEquals(3, table.baseSize());
        Assert.assertEquals(one, table.baseGet(one));
        Assert.assertEquals(two, table.baseGet(two));
        Assert.assertEquals(three, table.baseGet(three));
        Assert.assertEquals(null, table.baseGet(four));
        Assert.assertEquals(one, table.baseRemove(one));
        Assert.assertEquals(2, table.baseSize());
        Assert.assertEquals(null, table.baseGet(one));
        Assert.assertEquals(2, table.baseSize());
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
    public void testHashCollisons() {
        Foo one = new Foo();
        Foo two = new Foo();
        Foo three = new Foo();
        Foo four = new Foo();
        BaseHashTable<Foo> table = new BaseHashTable<>(20);
        Assert.assertEquals(null, table.baseAddOrReplace(one));
        Assert.assertEquals(null, table.baseAddOrReplace(two));
        Assert.assertEquals(null, table.baseAddOrReplace(three));
        Assert.assertEquals(3, table.baseSize());
        Assert.assertEquals(one, table.baseGet(one));
        Assert.assertEquals(two, table.baseGet(two));
        Assert.assertEquals(three, table.baseGet(three));
        Assert.assertEquals(null, table.baseGet(four));
        Assert.assertEquals(one, table.baseRemove(one));
        Assert.assertEquals(three, table.baseRemove(three));
        Assert.assertEquals(1, table.baseSize());
        Assert.assertEquals(null, table.baseGet(four));
        Assert.assertEquals(two, table.baseGet(two));
        Assert.assertEquals(two, table.baseRemove(two));
        Assert.assertEquals(0, table.baseSize());
    }

    @Test
    public void testExpansion() {
        BaseHashTable<Integer> table = new BaseHashTable<>(0);

        for (int i = 0; i < 4096; i++) {
            Assert.assertEquals(i, table.baseSize());
            Assert.assertEquals(null, table.baseAddOrReplace(Integer.valueOf(i)));
        }

        for (int i = 0; i < 4096; i++) {
            Assert.assertEquals(4096 - i, table.baseSize());
            Assert.assertEquals(Integer.valueOf(i), table.baseRemove(Integer.valueOf(i)));
        }
    }
}
