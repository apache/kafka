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

package org.apache.kafka.server.util;

import io.vavr.collection.HashSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.function.Executable;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 60)
public class VavrSetAsJavaTest {
    @Test
    public void testSet() {
        int size = 10;
        AtomicInteger value = new AtomicInteger();
        HashSet<Integer> vavrSet = HashSet.fill(size, value::getAndIncrement);
        assertEquals(size, vavrSet.size());
        VavrSetAsJava<Integer> javaSet = new VavrSetAsJava<>(vavrSet);
        performIntegerSetOrCollectionAssertions(javaSet, size, 1);
    }

    static void performIntegerSetOrCollectionAssertions(Collection<Integer> c, int size, int valueMultiplier) {
        assertEquals(size, c.size());
        assertFalse(c.isEmpty());
        assertFalse(c.contains(-1 * valueMultiplier));
        assertFalse(c.contains(size * valueMultiplier));
        assertTrue(c.contains(0));
        assertTrue(c.contains((size - 1) * valueMultiplier));
        assertTrue(c.containsAll(IntStream.range(0, size).boxed().map(i -> i * valueMultiplier).collect(Collectors.toList())));
        assertFalse(c.containsAll(IntStream.range(-1, size).boxed().map(i -> i * valueMultiplier).collect(Collectors.toList())));
        assertFalse(c.containsAll(Arrays.asList(-1 * valueMultiplier, 0)));
        for (Integer x : c) { // tests iterator()
            if (valueMultiplier > 0) {
                assertTrue(x >= 0 && x < size * valueMultiplier);
            } else {
                assertTrue(x <= 0 && x > size * valueMultiplier);
            }
        }
        assertUnsupportedOperation(() -> c.remove(0));
        assertUnsupportedOperation(() -> c.addAll(Collections.emptyList()));
        assertUnsupportedOperation(() -> c.removeAll(Collections.emptyList()));
        assertUnsupportedOperation(() -> c.retainAll(Collections.emptyList()));
        assertUnsupportedOperation(() -> c.removeIf(e -> true));
        assertUnsupportedOperation(c::clear);
    }

    @Test
    public void testSetContainsPerformance() {
        assertBetterThanLinearPerformance(Set::contains);
    }

    private static void assertBetterThanLinearPerformance(BiConsumer<Set<Integer>, Integer> f) {
        int size = 10;
        AtomicInteger value = new AtomicInteger();
        HashSet<Integer> vavrSet = HashSet.fill(size, value::getAndIncrement);
        assertEquals(size, vavrSet.size());
        VavrSetAsJava<Integer> javaSet = new VavrSetAsJava<>(vavrSet);
        long durationNanos = getDurationNanos(javaSet, f);
        // now perform the same test with a collection that is 100 times as large
        value.set(0);
        VavrSetAsJava<Integer> javaSet100x = new VavrSetAsJava<>(HashSet.fill(100 * size, value::getAndIncrement));
        long durationNanos100x = getDurationNanos(javaSet100x, f);
        assertTrue(durationNanos100x < 4 * durationNanos,
            "Ratio expected to be < 4 but was " + (durationNanos100x * 1.0 / durationNanos));
    }

    static long getDurationNanos(Set<Integer> set, BiConsumer<Set<Integer>, Integer> f) {
        long startNanos = System.nanoTime();
        // run it a bunch of times
        for (int i = 0; i < 10000; ++i) {
            f.accept(set, Integer.MAX_VALUE - i);
        }
        return System.nanoTime() - startNanos;
    }

    private static void assertUnsupportedOperation(Executable operation) {
        assertThrows(UnsupportedOperationException.class, operation);
    }
}
