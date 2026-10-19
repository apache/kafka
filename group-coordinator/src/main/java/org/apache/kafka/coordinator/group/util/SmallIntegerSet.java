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

import java.util.AbstractSet;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * An immutable set of integers in the range [0, N) where N is small, backed by a {@link BitSet}.
 * <p>
 * For N <= 512, this set is more memory-efficient than a non-empty {@link HashSet} without an
 * explicit capacity, since a {@link HashSet} allocates a 16-element array of references by default,
 * which comes out to 64 bytes when -XX:+UseCompressedOops is enabled, as it is by default for heaps
 * under 32 GiB.
 * <p>
 * For larger N, this set is more memory-efficient than a {@link HashSet} when more than 1/427 of
 * the integers in the range [0, N) are present, since a {@link HashSet} of {@link Integer} elements
 * takes up at least 5.33 bytes per element for its table at a 0.75 load factor, plus 32 bytes for
 * each {@link HashSet} node and 16 bytes for each {@link Integer} element.
 */
public class SmallIntegerSet extends AbstractSet<Integer> {
    private final BitSet bits;
    private final int size;

    public SmallIntegerSet(Set<Integer> integers) {
        BitSet bits = new BitSet(integers.size());
        for (int integer : integers) {
            if (integer < 0) {
                throw new IllegalArgumentException("Negative integer " + integer + " cannot be added to a SmallIntegerSet.");
            }
            bits.set(integer);
        }
        this.bits = bits;
        this.size = integers.size();
    }

    @Override
    public int size() {
        return size;
    }

    public boolean contains(int integer) {
        return integer >= 0 && bits.get(integer);
    }

    @Override
    public boolean contains(Object o) {
        return o instanceof Integer integer && contains(integer.intValue());
    }

    @Override
    public Iterator<Integer> iterator() {
        return new Iterator<>() {
            private int next = bits.nextSetBit(0);

            @Override
            public boolean hasNext() {
                return next >= 0;
            }

            @Override
            public Integer next() {
                if (next < 0) {
                    throw new NoSuchElementException();
                }
                int current = next;
                next = bits.nextSetBit(current + 1);
                return current;
            }
        };
    }
}
