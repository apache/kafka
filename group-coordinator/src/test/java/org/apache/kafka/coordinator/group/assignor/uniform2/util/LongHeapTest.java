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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the heap of longs.
 */
public class LongHeapTest {
    @Test
    public void testNegativeCapacityThrows() {
        assertThrows(IllegalArgumentException.class, () -> new LongHeap(-1));
    }

    @Test
    public void testEmptyHeap() {
        var heap = new LongHeap(0);
        assertTrue(heap.isEmpty());
        assertEquals(0, heap.size());
        assertThrows(IllegalStateException.class, heap::peek);
        assertThrows(IllegalStateException.class, heap::poll);
    }

    /**
     * Elements added in any order come out smallest first, negative ones included, and the heap
     * grows from a zero capacity.
     */
    @Test
    public void testPollsInOrder() {
        var heap = new LongHeap(0);
        for (long element : List.of(5L, -3L, 9L, 1L << 40, 7L, -3L)) {
            heap.add(element);
        }
        assertEquals(6, heap.size());
        assertEquals(-3, heap.peek());
        assertEquals(List.of(-3L, -3L, 5L, 7L, 9L, 1L << 40), pollAll(heap));
    }

    @Test
    public void testClear() {
        var heap = new LongHeap(2);
        heap.add(2);
        heap.add(1);
        heap.clear();
        assertTrue(heap.isEmpty());
        assertEquals(List.of(), pollAll(heap));

        heap.add(3);
        assertEquals(List.of(3L), pollAll(heap));
    }

    @Test
    public void testHeapOfAnArrayWithAnInvalidSizeThrows() {
        assertThrows(IllegalArgumentException.class, () -> new LongHeap(new long[3], -1));
        assertThrows(IllegalArgumentException.class, () -> new LongHeap(new long[3], 4));
    }

    /**
     * A heap of no element of an array is empty, and a heap of its first element ignores the
     * others.
     */
    @Test
    public void testHeapOfZeroOrOneElementOfAnArray() {
        var empty = new LongHeap(new long[] {1, 2}, 0);
        assertTrue(empty.isEmpty());
        assertThrows(IllegalStateException.class, empty::peek);
        assertEquals(List.of(), pollAll(empty));
        empty.add(3);
        assertEquals(List.of(3L), pollAll(empty));

        var single = new LongHeap(new long[] {7, 1}, 1);
        assertEquals(1, single.size());
        assertEquals(List.of(7L), pollAll(single));
    }

    /**
     * A heap of a full array grows on the next addition.
     */
    @Test
    public void testHeapOfAFullArrayGrows() {
        var heap = new LongHeap(new long[] {3, 1, 2}, 3);
        heap.add(0);
        heap.add(4);

        assertEquals(5, heap.size());
        assertEquals(List.of(0L, 1L, 2L, 3L, 4L), pollAll(heap));
    }

    /**
     * A heap built from the first elements of an array, ties and negative elements included,
     * polls them in order, after another addition.
     */
    @Test
    public void testHeapOfAnArrayPollsInOrder() {
        var random = new Random(2);
        var elements = new long[100];
        var queue = new PriorityQueue<Long>();
        for (int i = 0; i < 90; i++) {
            elements[i] = random.nextInt(50) - 10;
            queue.add(elements[i]);
        }

        var heap = new LongHeap(elements, 90);
        heap.add(-20);
        queue.add(-20L);

        assertEquals(91, heap.size());
        while (!queue.isEmpty()) {
            assertEquals((long) queue.poll(), heap.poll());
        }
        assertTrue(heap.isEmpty());
    }

    /**
     * Random additions and polls, and smallest elements polled and added back larger, agree with
     * a priority queue.
     */
    @Test
    public void testAgreesWithAPriorityQueue() {
        var random = new Random(1);
        var heap = new LongHeap(1);
        var queue = new PriorityQueue<Long>();
        int added = 0;
        for (int step = 0; step < 5000; step++) {
            int action = random.nextInt(3);
            if (action == 0 && added < 1000) {
                long element = random.nextInt(100);
                heap.add(element);
                queue.add(element);
                added++;
            } else if (action == 1 && !queue.isEmpty()) {
                assertEquals((long) queue.poll(), heap.poll());
            } else if (!queue.isEmpty()) {
                long top = queue.poll();
                assertEquals(top, heap.poll());
                long grown = top + random.nextInt(10);
                heap.add(grown);
                queue.add(grown);
            }
            assertEquals(queue.size(), heap.size());
        }
    }

    private static List<Long> pollAll(LongHeap heap) {
        var polled = new ArrayList<Long>();
        while (!heap.isEmpty()) {
            polled.add(heap.poll());
        }
        return polled;
    }
}
