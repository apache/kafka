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

/**
 * A binary min heap of primitive longs, in their natural order, avoiding the boxing of a
 * {@code PriorityQueue<Long>}.
 */
public final class LongHeap {
    private long[] elements;
    private int size;

    /**
     * Creates an empty heap.
     *
     * @param capacity The initial capacity.
     * @throws IllegalArgumentException If the capacity is negative.
     */
    public LongHeap(int capacity) {
        if (capacity < 0) {
            throw new IllegalArgumentException("Negative capacity " + capacity);
        }
        this.elements = new long[capacity];
    }

    /**
     * Creates a heap of the first elements of an array, ordering them in one pass rather than
     * adding them one at a time.
     *
     * @param elements The array, which the heap then owns.
     * @param size     The number of elements, at most the length of the array.
     * @throws IllegalArgumentException If the size is negative or above the length of the array.
     */
    public LongHeap(long[] elements, int size) {
        if (size < 0 || size > elements.length) {
            throw new IllegalArgumentException("Size " + size + " outside of [0, " + elements.length + "]");
        }
        this.elements = elements;
        this.size = size;
        for (int index = size / 2 - 1; index >= 0; index--) {
            siftDown(index, elements[index]);
        }
    }

    /**
     * @return The number of elements.
     */
    public int size() {
        return size;
    }

    /**
     * @return {@code true} if the heap has no elements.
     */
    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * Adds an element.
     *
     * @param element The element.
     */
    public void add(long element) {
        if (size == elements.length) {
            elements = Arrays.copyOf(elements, Math.max(4, 2 * size));
        }
        int index = size++;
        while (index > 0) {
            int parent = (index - 1) / 2;
            if (element >= elements[parent]) {
                break;
            }
            elements[index] = elements[parent];
            index = parent;
        }
        elements[index] = element;
    }

    /**
     * @return The smallest element.
     * @throws IllegalStateException If the heap is empty.
     */
    public long peek() {
        if (size == 0) {
            throw new IllegalStateException("The heap is empty.");
        }
        return elements[0];
    }

    /**
     * Removes the smallest element.
     *
     * @return The smallest element.
     * @throws IllegalStateException If the heap is empty.
     */
    public long poll() {
        long top = peek();
        long last = elements[--size];
        if (size > 0) {
            siftDown(0, last);
        }
        return top;
    }

    /**
     * Places an element at an index, then moves it down to its place in the subtree of the index.
     *
     * @param index   The index.
     * @param element The element.
     */
    private void siftDown(int index, long element) {
        while (true) {
            int child = 2 * index + 1;
            if (child >= size) {
                break;
            }
            if (child + 1 < size && elements[child + 1] < elements[child]) {
                child++;
            }
            if (elements[child] >= element) {
                break;
            }
            elements[index] = elements[child];
            index = child;
        }
        elements[index] = element;
    }

    /**
     * Removes all the elements, keeping the capacity.
     */
    public void clear() {
        size = 0;
    }
}
