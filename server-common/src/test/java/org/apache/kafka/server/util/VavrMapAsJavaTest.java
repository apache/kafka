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

import io.vavr.Tuple;
import io.vavr.collection.HashMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.function.Executable;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.vavr.API.None;
import static io.vavr.API.Some;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 60)
public class VavrMapAsJavaTest {
    @Test
    public void testMap() {
        int size = 10;
        AtomicInteger value = new AtomicInteger();
        HashMap<Integer, Integer> vavrMap = HashMap.fill(size, () -> {
            int n = value.getAndIncrement();
            return Tuple.of(n, n);
        });
        assertEquals(size, vavrMap.size());
        Map<Integer, Integer> javaMap = new VavrMapAsJava<>(vavrMap, Function.identity());
        assertEquals(size, javaMap.size());
        assertFalse(javaMap.isEmpty());
        assertFalse(javaMap.containsKey(-1));
        assertFalse(javaMap.containsKey(size));
        assertTrue(javaMap.containsKey(0));
        assertTrue(javaMap.containsKey(size - 1));
        assertFalse(javaMap.containsValue(-1));
        assertFalse(javaMap.containsValue(size));
        assertTrue(javaMap.containsValue(0));
        assertTrue(javaMap.containsValue(size - 1));
        assertNull(javaMap.get(-1));
        assertNull(javaMap.get(size));
        assertEquals(0, javaMap.get(0));
        assertEquals(size - 1, javaMap.get(size - 1));
        VavrSetAsJavaTest.performIntegerSetOrCollectionAssertions(javaMap.keySet(), size, 1);
        VavrSetAsJavaTest.performIntegerSetOrCollectionAssertions(javaMap.values(), size, 1);
        performEntrySetAssertions(javaMap.entrySet(), size, 1);
        assertUnsupportedOperation(() -> javaMap.remove(0));
        assertUnsupportedOperation(() -> javaMap.putAll(Collections.emptyMap()));
        assertUnsupportedOperation(javaMap::clear);
        assertUnsupportedOperation(() -> javaMap.replaceAll((a, b) -> 0));
        assertUnsupportedOperation(() -> javaMap.putIfAbsent(0, 0));
        assertUnsupportedOperation(() -> javaMap.remove(0, 0));
        assertUnsupportedOperation(() -> javaMap.replace(0, 0, 0));
        assertUnsupportedOperation(() -> javaMap.replace(0, 0));
        assertUnsupportedOperation(() -> javaMap.computeIfAbsent(0, a -> 0));
        assertUnsupportedOperation(() -> javaMap.computeIfPresent(0, (a, b) -> 0));
        assertUnsupportedOperation(() -> javaMap.compute(0, (a, b) -> 0));
        assertUnsupportedOperation(() -> javaMap.merge(0, 0, (a, b) -> 0));
    }

    @Test
    public void testNulls() {
        HashMap<String, String> vavrMap = HashMap.of("a", "a");
        Map<String, String> javaMap = new VavrMapAsJava<>(vavrMap, s -> s + s);
        assertTrue(vavrMap.containsKey("a"));
        assertTrue(javaMap.containsKey("a"));
        assertFalse(vavrMap.containsKey("b"));
        assertFalse(javaMap.containsKey("b"));
        assertEquals(Some("a"), vavrMap.get("a"));
        assertEquals("aa", javaMap.get("a"));
        assertEquals(None(), vavrMap.get("b"));
        assertNull(javaMap.get("b"));
        HashMap<String, String> vavrMapWithNullValue = HashMap.of("a", null);
        Map<String, String> javaMapWithNullValueAndSimpleMapper = new VavrMapAsJava<>(vavrMapWithNullValue, String::toUpperCase);
        assertTrue(vavrMapWithNullValue.containsKey("a"));
        assertTrue(javaMapWithNullValueAndSimpleMapper.containsKey("a"));
        assertFalse(vavrMapWithNullValue.containsKey("b"));
        assertFalse(javaMapWithNullValueAndSimpleMapper.containsKey("b"));
        assertEquals(Some(null), vavrMapWithNullValue.get("a"));
        assertThrows(NullPointerException.class, () -> javaMapWithNullValueAndSimpleMapper.get("a"));
        assertEquals(None(), vavrMapWithNullValue.get("b"));
        assertNull(javaMapWithNullValueAndSimpleMapper.get("b"));
        assertThrows(NullPointerException.class, () -> javaMapWithNullValueAndSimpleMapper.containsValue("anything"));
        assertThrows(NullPointerException.class, () -> {
            AtomicBoolean hasLowerCaseValues = new AtomicBoolean();
            for (Map.Entry<String, String> entry: javaMapWithNullValueAndSimpleMapper.entrySet()) {
                String value = entry.getValue();
                if (value != null) {
                    hasLowerCaseValues.set(hasLowerCaseValues.get() || !Objects.equals(value, value.toUpperCase(Locale.ROOT)));
                }
            }
        });
        Map<String, String> javaMapWithNullValueAndNullAwareMapper = new VavrMapAsJava<>(vavrMapWithNullValue, s -> {
            if (s ==  null) {
                return null;
            } else {
                return s.toUpperCase(Locale.ROOT);
            }
        });
        assertTrue(javaMapWithNullValueAndNullAwareMapper.containsKey("a"));
        assertFalse(javaMapWithNullValueAndNullAwareMapper.containsKey("b"));
        assertNull(javaMapWithNullValueAndNullAwareMapper.get("a"));
        assertNull(javaMapWithNullValueAndNullAwareMapper.get("b"));
        assertFalse(javaMapWithNullValueAndNullAwareMapper.containsValue("anything"));
        assertFalse(() -> {
            AtomicBoolean hasLowerCaseValues = new AtomicBoolean();
            for (Map.Entry<String, String> entry: javaMapWithNullValueAndNullAwareMapper.entrySet()) {
                String value = entry.getValue();
                if (value != null) {
                    hasLowerCaseValues.set(hasLowerCaseValues.get() || !Objects.equals(value, value.toUpperCase(Locale.ROOT)));
                }
            }
            return hasLowerCaseValues.get();
        });
    }

    @Test
    public void testMapValueTranslation() {
        int size = 10;
        AtomicInteger value = new AtomicInteger();
        HashMap<Integer, Integer> vavrMap = HashMap.fill(size, () -> {
            int n = value.getAndIncrement();
            return Tuple.of(n, n);
        });
        assertEquals(size, vavrMap.size());
        Map<Integer, Integer> javaMap = new VavrMapAsJava<>(vavrMap, v -> -v);
        assertEquals(size, javaMap.size());
        assertFalse(javaMap.isEmpty());
        assertFalse(javaMap.containsKey(-1));
        assertFalse(javaMap.containsKey(size));
        assertTrue(javaMap.containsKey(0));
        assertTrue(javaMap.containsKey(size - 1));
        assertFalse(javaMap.containsValue(1));
        assertFalse(javaMap.containsValue(-size));
        assertTrue(javaMap.containsValue(0));
        assertTrue(javaMap.containsValue(-size + 1));
        assertNull(javaMap.get(-1));
        assertNull(javaMap.get(size));
        assertEquals(0, javaMap.get(0));
        assertEquals(-size + 1, javaMap.get(size - 1));
        VavrSetAsJavaTest.performIntegerSetOrCollectionAssertions(javaMap.keySet(), size, 1);
        VavrSetAsJavaTest.performIntegerSetOrCollectionAssertions(javaMap.values(), size, -1);
        performEntrySetAssertions(javaMap.entrySet(), size, -1);
        assertUnsupportedOperation(() -> javaMap.remove(0));
        assertUnsupportedOperation(() -> javaMap.putAll(Collections.emptyMap()));
        assertUnsupportedOperation(javaMap::clear);
        assertUnsupportedOperation(() -> javaMap.replaceAll((a, b) -> 0));
        assertUnsupportedOperation(() -> javaMap.putIfAbsent(0, 0));
        assertUnsupportedOperation(() -> javaMap.remove(0, 0));
        assertUnsupportedOperation(() -> javaMap.replace(0, 0, 0));
        assertUnsupportedOperation(() -> javaMap.replace(0, 0));
        assertUnsupportedOperation(() -> javaMap.computeIfAbsent(0, a -> 0));
        assertUnsupportedOperation(() -> javaMap.computeIfPresent(0, (a, b) -> 0));
        assertUnsupportedOperation(() -> javaMap.compute(0, (a, b) -> 0));
        assertUnsupportedOperation(() -> javaMap.merge(0, 0, (a, b) -> 0));
    }

    @Test
    public void testMapContainsKeyPerformance() {
        assertBetterThanLinearPerformance(Map::containsKey);
    }

    @Test
    public void testMapGetPerformance() {
        assertBetterThanLinearPerformance(Map::get);
    }

    @Test
    public void testMapKeySetPerformance() {
        int size = 10;
        AtomicInteger value = new AtomicInteger();
        HashMap<Integer, Integer> vavrMap = HashMap.fill(size, () -> {
            int n = value.getAndIncrement();
            return Tuple.of(n, n);
        });
        assertEquals(size, vavrMap.size());
        Map<Integer, Integer> javaMap = new VavrMapAsJava<>(vavrMap, Function.identity());
        long durationNanos = VavrSetAsJavaTest.getDurationNanos(javaMap.keySet(), Set::contains);
        // now perform the same test with a collection that is 100 times as large
        value.set(0);
        HashMap<Integer, Integer> vavrMap100x = HashMap.fill(size * 100, () -> {
            int n = value.getAndIncrement();
            return Tuple.of(n, n);
        });
        Map<Integer, Integer> javaMap100x = new VavrMapAsJava<>(vavrMap100x, Function.identity());
        long durationNanos100x = VavrSetAsJavaTest.getDurationNanos(javaMap100x.keySet(), Set::contains);
        assertTrue(durationNanos100x < 4 * durationNanos,
            "Ratio expected to be < 4 but was " + (durationNanos100x * 1.0 / durationNanos));
    }

    private static void assertBetterThanLinearPerformance(BiConsumer<Map<Integer, Integer>, Integer> f) {
        int size = 10;
        AtomicInteger value = new AtomicInteger();
        HashMap<Integer, Integer> vavrMap = HashMap.fill(size, () -> {
            int n = value.getAndIncrement();
            return Tuple.of(n, n);
        });
        assertEquals(size, vavrMap.size());
        Map<Integer, Integer> javaMap = new VavrMapAsJava<>(vavrMap, Function.identity());
        long durationNanos = getDurationNanos(javaMap, f);
        // now perform the same test with a collection that is 100 times as large
        value.set(0);
        HashMap<Integer, Integer> vavrMap100x = HashMap.fill(size * 100, () -> {
            int n = value.getAndIncrement();
            return Tuple.of(n, n);
        });
        Map<Integer, Integer> javaMap100x = new VavrMapAsJava<>(vavrMap100x, Function.identity());
        long durationNanos100x = getDurationNanos(javaMap100x, f);
        assertTrue(durationNanos100x < 4 * durationNanos,
            "Ratio expected to be < 4 but was " + (durationNanos100x * 1.0 / durationNanos));
    }

    private static long getDurationNanos(Map<Integer, Integer> map, BiConsumer<Map<Integer, Integer>, Integer> f) {
        long startNanos = System.nanoTime();
        // run it a bunch of times
        for (int i = 0; i < 10000; ++i) {
            f.accept(map, Integer.MAX_VALUE - i);
        }
        return System.nanoTime() - startNanos;
    }

    static void performEntrySetAssertions(Set<Map.Entry<Integer, Integer>> set, int size, int valueMultiplier) {
        assertEquals(size, set.size());
        assertFalse(set.isEmpty());
        assertFalse(set.contains(new AbstractMap.SimpleImmutableEntry<>(-1, -1 * valueMultiplier)));
        assertFalse(set.contains(new AbstractMap.SimpleImmutableEntry<>(size, size * valueMultiplier)));
        assertTrue(set.contains(new AbstractMap.SimpleImmutableEntry<>(0, 0)));
        assertTrue(set.contains(new AbstractMap.SimpleImmutableEntry<>(size - 1, (size - 1) * valueMultiplier)));
        assertTrue(set.containsAll(IntStream.range(0, size).boxed().map(i -> new AbstractMap.SimpleImmutableEntry<>(i, i * valueMultiplier)).collect(Collectors.toList())));
        assertFalse(set.containsAll(IntStream.range(-1, size).boxed().map(i -> new AbstractMap.SimpleImmutableEntry<>(i, i * valueMultiplier)).collect(Collectors.toList())));
        assertFalse(set.containsAll(Arrays.asList(-1, 0).stream().map(i -> new AbstractMap.SimpleImmutableEntry<>(i, i * valueMultiplier)).collect(Collectors.toList())));
        for (Map.Entry<Integer, Integer> entry : set) { // tests iterator()
            assertTrue(entry.getKey() >= 0 && entry.getKey().equals(entry.getValue() * valueMultiplier));
            if (valueMultiplier > 0) {
                assertTrue(entry.getValue() >= 0);
            } else {
                assertTrue(entry.getValue() <= 0);
            }
        }
        assertUnsupportedOperation(() -> set.remove(0));
        assertUnsupportedOperation(() -> set.addAll(Collections.emptyList()));
        assertUnsupportedOperation(() -> set.removeAll(Collections.emptyList()));
        assertUnsupportedOperation(() -> set.retainAll(Collections.emptyList()));
        assertUnsupportedOperation(() -> set.removeIf(e -> true));
        assertUnsupportedOperation(set::clear);
    }

    private static void assertUnsupportedOperation(Executable operation) {
        assertThrows(UnsupportedOperationException.class, operation);
    }
}
