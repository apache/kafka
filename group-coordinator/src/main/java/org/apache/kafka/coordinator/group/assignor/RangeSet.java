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
package org.apache.kafka.coordinator.group.assignor;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * A {@code RangeSet} represents a range of integers from {@code from} (inclusive)
 * to {@code to} (exclusive).
 * This implementation provides a view over a continuous range of integers without actually storing them.
 */
class RangeSet implements Set<Integer> {
    private final int from;
    private final int to;

    /**
     * Constructs a {@code RangeSet} with the specified range.
     *
     * @param from      The starting value (inclusive) of the range.
     * @param to        The ending value (exclusive) of the range.
     */
    public RangeSet(int from, int to) {
        this.from = from;
        this.to = to;

        if (to < from) {
            throw new IllegalArgumentException("Invalid range: to must be greater than or equal to from");
        }

        if ((long) to - (long) from > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Range exceeds the maximum size of Integer.MAX_VALUE");
        }
    }

    @Override
    public int size() {
        return to - from;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean contains(Object o) {
        if (o instanceof Integer) {
            int value = (Integer) o;
            return value >= from && value < to;
        }
        return false;
    }

    @Override
    public Iterator<Integer> iterator() {
        return new Iterator<>() {
            private int current = from;

            @Override
            public boolean hasNext() {
                return current < to;
            }

            @Override
            public Integer next() {
                if (!hasNext()) throw new NoSuchElementException();
                return current++;
            }
        };
    }

    @Override
    public Object[] toArray() {
        Object[] array = new Object[size()];
        for (int i = 0; i < size(); i++) {
            array[i] = from + i;
        }
        return array;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T[] toArray(T[] a) {
        int size = size();
        if (a.length < size) {
            // Create a new array of the same type as a with the correct size
            a = (T[]) Array.newInstance(a.getClass().getComponentType(), size);
        }
        for (int i = 0; i < size; i++) {
            a[i] = (T) Integer.valueOf(from + i);
        }
        if (a.length > size) {
            a[size] = null;
        }
        return a;
    }

    @Override
    public boolean add(Integer integer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        for (Object o : c) {
            if (!contains(o)) return false;
        }
        return true;
    }

    @Override
    public boolean addAll(Collection<? extends Integer> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return "RangeSet(from=" + from + " (inclusive), to=" + to + " (exclusive))";
    }

    /**
     * Compares the specified object with this set for equality.
     * Returns {@code true} if the specified object is also a set,
     * the two sets have the same size, and every member of the specified
     * set is contained in this set.
     *
     * @param o object to be compared for equality with this set
     * @return {@code true} if the specified object is equal to this set
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Set<?> otherSet)) return false;

        if (o instanceof RangeSet other) {
            if (this.size() == 0 && other.size() == 0) return true;
            return this.from == other.from && this.to == other.to;
        }

        if (otherSet.size() != this.size()) return false;

        for (int i = from; i < to; i++) {
            if (!otherSet.contains(i)) return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        // The hash code of a Set is defined as the sum of the hash codes of its elements.
        // The hash code of an integer is the integer itself.

        // The sum of the integers from 1 to n is n * (n + 1) / 2.
        // To get the sum of the integers from 1 + k to n + k, we can add n * k.
        // So our hash code comes out to n * (from + to - 1) / 2.

        // The arithmetic has to be done using longs, since the division by 2 is equivalent to
        // shifting the 33rd bit right.
        long sum = size() * ((long) from + (long) to - 1) / 2;
        return (int) sum;
    }
}
