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
package org.apache.kafka.coordinator.group.assignor.uniform2.util;

import java.util.Arrays;
import java.util.Objects;

/**
 * A growable list of primitive ints, avoiding the boxing of a {@code List<Integer>}.
 *
 * <p>{@link #get(int)} does not check its index beyond what the backing array does, to keep the
 * reads cheap: reading at an index between the size and the capacity returns a stale value
 * instead of throwing. The methods that change the list check their arguments.
 */
public final class IntList {
    private int[] elements;
    private int size;

    /**
     * Creates an empty list.
     *
     * @param capacity The initial capacity.
     * @throws IllegalArgumentException If the capacity is negative.
     */
    public IntList(int capacity) {
        if (capacity < 0) {
            throw new IllegalArgumentException("Negative capacity " + capacity);
        }
        this.elements = new int[capacity];
    }

    /**
     * @return The number of elements.
     */
    public int size() {
        return size;
    }

    /**
     * @return True if the list has no element.
     */
    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * @param index The index, below the size.
     * @return The element at the index.
     */
    public int get(int index) {
        return elements[index];
    }

    /**
     * Replaces the element at an index.
     *
     * @param index   The index, below the size.
     * @param element The new element.
     * @throws IndexOutOfBoundsException If the index is negative or not below the size.
     */
    public void set(int index, int element) {
        Objects.checkIndex(index, size);
        elements[index] = element;
    }

    /**
     * Appends an element, growing the backing array when it is full.
     *
     * @param element The element.
     */
    public void add(int element) {
        if (size == elements.length) {
            elements = Arrays.copyOf(elements, Math.max(4, 2 * size));
        }
        elements[size++] = element;
    }

    /**
     * Removes the element at an index in constant time. The last element takes its place, so the
     * order of the elements is not preserved.
     *
     * @param index The index, below the size.
     * @return The removed element.
     * @throws IndexOutOfBoundsException If the index is negative or not below the size.
     */
    public int remove(int index) {
        Objects.checkIndex(index, size);
        int removed = elements[index];
        elements[index] = elements[--size];
        return removed;
    }

    /**
     * @param element The element.
     * @return The index of the first occurrence of the element, or -1 if it is absent.
     */
    public int indexOf(int element) {
        for (int i = 0; i < size; i++) {
            if (elements[i] == element) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Removes all the elements, keeping the capacity.
     */
    public void clear() {
        size = 0;
    }

    /**
     * Keeps the first elements only, keeping the capacity.
     *
     * @param size The number of elements to keep, at most the size.
     * @throws IndexOutOfBoundsException If the number is negative or above the size.
     */
    public void truncate(int size) {
        Objects.checkFromToIndex(0, size, this.size);
        this.size = size;
    }

    /**
     * Sorts the elements in ascending order.
     */
    public void sort() {
        Arrays.sort(elements, 0, size);
    }

    /**
     * @return A new array holding the elements.
     */
    public int[] toArray() {
        return Arrays.copyOf(elements, size);
    }

    @Override
    public String toString() {
        return Arrays.toString(toArray());
    }
}
