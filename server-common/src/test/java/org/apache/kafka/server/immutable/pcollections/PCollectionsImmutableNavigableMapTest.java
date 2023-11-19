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

package org.apache.kafka.server.immutable.pcollections;

import org.apache.kafka.server.immutable.DelegationChecker;
import org.apache.kafka.server.immutable.ImmutableNavigableMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pcollections.TreePMap;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.function.Function.identity;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;

@SuppressWarnings({"unchecked", "deprecation"})
class PCollectionsImmutableNavigableMapTest {
    private static final TreePMap<Integer, Integer> SINGLETON_MAP = TreePMap.singleton(new Random().nextInt(), new Random().nextInt());

    private static final class PCollectionsTreeMapWrapperDelegationChecker<R> extends DelegationChecker<TreePMap<Object, Object>, PCollectionsImmutableNavigableMap<Object, Object>, R> {
        public PCollectionsTreeMapWrapperDelegationChecker() {
            super(mock(TreePMap.class), PCollectionsImmutableNavigableMap::new);
        }

        public TreePMap<Object, Object> unwrap(PCollectionsImmutableNavigableMap<Object, Object> wrapper) {
            return wrapper.underlying();
        }
    }

    @Test
    public void testEmptyMap() {
        assertEquals(TreePMap.empty(), ((PCollectionsImmutableNavigableMap<?, ?>) ImmutableNavigableMap.empty()).underlying());
    }

    @Test
    public void testSingletonMap() {
        assertEquals(TreePMap.singleton(1, 2), ((PCollectionsImmutableNavigableMap<?, ?>) ImmutableNavigableMap.singleton(1, 2)).underlying());
    }

    @Test
    public void testUnderlying() {
        assertSame(SINGLETON_MAP, new PCollectionsImmutableNavigableMap<>(SINGLETON_MAP).underlying());
    }

    @Test
    public void testDelegationOfUpdated() {
        new PCollectionsImmutableNavigableMapTest.PCollectionsTreeMapWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(mock -> mock.plus(eq(this), eq(this)), SINGLETON_MAP)
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.updated(this, this), identity())
                .expectWrapperToWrapMockFunctionReturnValue()
                .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfRemoved() {
        new PCollectionsImmutableNavigableMapTest.PCollectionsTreeMapWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(mock -> mock.minus(eq(this)), SINGLETON_MAP)
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.removed(this), identity())
                .expectWrapperToWrapMockFunctionReturnValue()
                .doFunctionDelegationCheck();
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    public void testDelegationOfSize(int mockFunctionReturnValue) {
        new PCollectionsImmutableNavigableMapTest.PCollectionsTreeMapWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(TreePMap::size, mockFunctionReturnValue)
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(PCollectionsImmutableNavigableMap::size, identity())
                .doFunctionDelegationCheck();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testDelegationOfIsEmpty(boolean mockFunctionReturnValue) {
        new PCollectionsImmutableNavigableMapTest.PCollectionsTreeMapWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(TreePMap::isEmpty, mockFunctionReturnValue)
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(PCollectionsImmutableNavigableMap::isEmpty, identity())
                .doFunctionDelegationCheck();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testDelegationOfContainsKey(boolean mockFunctionReturnValue) {
        new PCollectionsImmutableNavigableMapTest.PCollectionsTreeMapWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(mock -> mock.containsKey(eq(this)), mockFunctionReturnValue)
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.containsKey(this), identity())
                .doFunctionDelegationCheck();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testDelegationOfContainsValue(boolean mockFunctionReturnValue) {
        new PCollectionsImmutableNavigableMapTest.PCollectionsTreeMapWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(mock -> mock.containsValue(eq(this)), mockFunctionReturnValue)
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.containsValue(this), identity())
                .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfGet() {
        new PCollectionsImmutableNavigableMapTest.PCollectionsTreeMapWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(mock -> mock.get(eq(this)), new Object())
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.get(this), identity())
                .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfUnsupportedFunctionPut() {
        new PCollectionsImmutableNavigableMapTest.PCollectionsTreeMapWrapperDelegationChecker<>()
                .defineMockConfigurationForUnsupportedFunction(mock -> mock.put(eq(this), eq(this)))
                .defineWrapperUnsupportedFunctionInvocation(wrapper -> wrapper.put(this, this))
                .doUnsupportedFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfUnsupportedFunctionRemoveByKey() {
        new PCollectionsImmutableNavigableMapTest.PCollectionsTreeMapWrapperDelegationChecker<>()
                .defineMockConfigurationForUnsupportedFunction(mock -> mock.remove(eq(this)))
                .defineWrapperUnsupportedFunctionInvocation(wrapper -> wrapper.remove(this))
                .doUnsupportedFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfUnsupportedFunctionPutAll() {
        new PCollectionsImmutableNavigableMapTest.PCollectionsTreeMapWrapperDelegationChecker<>()
                .defineMockConfigurationForVoidMethodInvocation(mock -> mock.putAll(eq(Collections.emptyMap())))
                .defineWrapperVoidMethodInvocation(wrapper -> wrapper.putAll(Collections.emptyMap()))
                .doUnsupportedVoidFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfUnsupportedFunctionClear() {
        new PCollectionsImmutableNavigableMapTest.PCollectionsTreeMapWrapperDelegationChecker<>()
                .defineMockConfigurationForVoidMethodInvocation(TreePMap::clear)
                .defineWrapperVoidMethodInvocation(PCollectionsImmutableNavigableMap::clear)
                .doUnsupportedVoidFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfValues() {
        new PCollectionsImmutableNavigableMapTest.PCollectionsTreeMapWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(TreePMap::values, Collections.emptySet())
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(PCollectionsImmutableNavigableMap::values, identity())
                .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfEntrySet() {
        new PCollectionsImmutableNavigableMapTest.PCollectionsTreeMapWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(TreePMap::entrySet, Collections.emptySet())
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(PCollectionsImmutableNavigableMap::entrySet, identity())
                .doFunctionDelegationCheck();
    }

    @Test
    public void testEquals() {
        final TreePMap<Object, Object> mock = mock(TreePMap.class);
        assertEquals(new PCollectionsImmutableNavigableMap<>(mock), new PCollectionsImmutableNavigableMap<>(mock));
        final TreePMap<Object, Object> someOtherMock = mock(TreePMap.class);
        assertNotEquals(new PCollectionsImmutableNavigableMap<>(mock), new PCollectionsImmutableNavigableMap<>(someOtherMock));
    }

    @Test
    public void testHashCode() {
        final TreePMap<Object, Object> mock = mock(TreePMap.class);
        assertEquals(mock.hashCode(), new PCollectionsImmutableNavigableMap<>(mock).hashCode());
        final TreePMap<Object, Object> someOtherMock = mock(TreePMap.class);
        assertNotEquals(mock.hashCode(), new PCollectionsImmutableNavigableMap<>(someOtherMock).hashCode());
    }

    @Test
    public void testDelegationOfGetOrDefault() {
        new PCollectionsImmutableNavigableMapTest.PCollectionsTreeMapWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(mock -> mock.getOrDefault(eq(this), eq(this)), this)
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.getOrDefault(this, this), identity())
                .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfForEach() {
        final BiConsumer<Object, Object> mockBiConsumer = mock(BiConsumer.class);
        new PCollectionsImmutableNavigableMapTest.PCollectionsTreeMapWrapperDelegationChecker<>()
                .defineMockConfigurationForVoidMethodInvocation(mock -> mock.forEach(eq(mockBiConsumer)))
                .defineWrapperVoidMethodInvocation(wrapper -> wrapper.forEach(mockBiConsumer))
                .doVoidMethodDelegationCheck();
    }

    @Test
    public void testDelegationOfUnsupportedFunctionReplaceAll() {
        final BiFunction<Object, Object, Object> mockBiFunction = mock(BiFunction.class);
        new PCollectionsImmutableNavigableMapTest.PCollectionsTreeMapWrapperDelegationChecker<>()
                .defineMockConfigurationForVoidMethodInvocation(mock -> mock.replaceAll(eq(mockBiFunction)))
                .defineWrapperVoidMethodInvocation(wrapper -> wrapper.replaceAll(mockBiFunction))
                .doUnsupportedVoidFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfUnsupportedFunctionPutIfAbsent() {
        new PCollectionsImmutableNavigableMapTest.PCollectionsTreeMapWrapperDelegationChecker<>()
                .defineMockConfigurationForUnsupportedFunction(mock -> mock.putIfAbsent(eq(this), eq(this)))
                .defineWrapperUnsupportedFunctionInvocation(wrapper -> wrapper.putIfAbsent(this, this))
                .doUnsupportedFunctionDelegationCheck();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testDelegationOfUnsupportedFunctionRemoveByKeyAndValue(boolean mockFunctionReturnValue) {
        new PCollectionsImmutableNavigableMapTest.PCollectionsTreeMapWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(mock -> mock.remove(eq(this), eq(this)), mockFunctionReturnValue)
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.remove(this, this), identity())
                .doFunctionDelegationCheck();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testDelegationOfUnsupportedFunctionReplaceWhenMappedToSpecificValue(boolean mockFunctionReturnValue) {
        new PCollectionsImmutableNavigableMapTest.PCollectionsTreeMapWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(mock -> mock.replace(eq(this), eq(this), eq(this)), mockFunctionReturnValue)
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.replace(this, this, this), identity())
                .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfUnsupportedFunctionReplaceWhenMappedToAnyValue() {
        new PCollectionsImmutableNavigableMapTest.PCollectionsTreeMapWrapperDelegationChecker<>()
                .defineMockConfigurationForUnsupportedFunction(mock -> mock.replace(eq(this), eq(this)))
                .defineWrapperUnsupportedFunctionInvocation(wrapper -> wrapper.replace(this, this))
                .doUnsupportedFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfUnsupportedFunctionComputeIfAbsent() {
        final Function<Object, Object> mockFunction = mock(Function.class);
        new PCollectionsImmutableNavigableMapTest.PCollectionsTreeMapWrapperDelegationChecker<>()
                .defineMockConfigurationForUnsupportedFunction(mock -> mock.computeIfAbsent(eq(this), eq(mockFunction)))
                .defineWrapperUnsupportedFunctionInvocation(wrapper -> wrapper.computeIfAbsent(this, mockFunction))
                .doUnsupportedFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfUnsupportedFunctionComputeIfPresent() {
        final BiFunction<Object, Object, Object> mockBiFunction = mock(BiFunction.class);
        new PCollectionsImmutableNavigableMapTest.PCollectionsTreeMapWrapperDelegationChecker<>()
                .defineMockConfigurationForUnsupportedFunction(mock -> mock.computeIfPresent(eq(this), eq(mockBiFunction)))
                .defineWrapperUnsupportedFunctionInvocation(wrapper -> wrapper.computeIfPresent(this, mockBiFunction))
                .doUnsupportedFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfUnsupportedFunctionCompute() {
        final BiFunction<Object, Object, Object> mockBiFunction = mock(BiFunction.class);
        new PCollectionsImmutableNavigableMapTest.PCollectionsTreeMapWrapperDelegationChecker<>()
                .defineMockConfigurationForUnsupportedFunction(mock -> mock.compute(eq(this), eq(mockBiFunction)))
                .defineWrapperUnsupportedFunctionInvocation(wrapper -> wrapper.compute(this, mockBiFunction))
                .doUnsupportedFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfUnsupportedFunctionMerge() {
        final BiFunction<Object, Object, Object> mockBiFunction = mock(BiFunction.class);
        new PCollectionsImmutableNavigableMapTest.PCollectionsTreeMapWrapperDelegationChecker<>()
                .defineMockConfigurationForUnsupportedFunction(mock -> mock.merge(eq(this), eq(this), eq(mockBiFunction)))
                .defineWrapperUnsupportedFunctionInvocation(wrapper -> wrapper.merge(this, this, mockBiFunction))
                .doUnsupportedFunctionDelegationCheck();
    }

    @ParameterizedTest
    @ValueSource(strings = {"a", "b"})
    public void testDelegationOfToString(String mockFunctionReturnValue) {
        new PCollectionsImmutableNavigableMapTest.PCollectionsTreeMapWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(TreePMap::toString, mockFunctionReturnValue)
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(PCollectionsImmutableNavigableMap::toString,
                        text -> "PCollectionsImmutableNavigableMap{underlying=" + text + "}")
                .doFunctionDelegationCheck();
    }

    @ParameterizedTest
    @ValueSource(ints = {9, 4})
    public void testDelegationOfLowerKey(int mockFunctionReturnValue) {
        new PCollectionsImmutableNavigableMapTest.PCollectionsTreeMapWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(mock -> mock.lowerKey(eq(10)), mockFunctionReturnValue)
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.lowerKey(10), identity())
                .doFunctionDelegationCheck();
    }

    @ParameterizedTest
    @ValueSource(ints = {9, 4})
    public void testDelegationOfLowerEntry(int mockFunctionReturnValue) {
        Map.Entry<Integer, Integer> returnValue = new AbstractMap.SimpleEntry<>(mockFunctionReturnValue, 1);
        new PCollectionsImmutableNavigableMapTest.PCollectionsTreeMapWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(mock -> mock.lowerEntry(eq(10)), returnValue)
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.lowerEntry(10), identity())
                .doFunctionDelegationCheck();
    }

    @ParameterizedTest
    @ValueSource(ints = {9, 10})
    public void testDelegationOfFloorKey(int mockFunctionReturnValue) {
        new PCollectionsImmutableNavigableMapTest.PCollectionsTreeMapWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(mock -> mock.floorKey(eq(10)), mockFunctionReturnValue)
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.floorKey(10), identity())
                .doFunctionDelegationCheck();
    }

    @ParameterizedTest
    @ValueSource(ints = {9, 10})
    public void testDelegationOfFloorEntry(int mockFunctionReturnValue) {
        Map.Entry<Integer, Integer> returnValue = new AbstractMap.SimpleEntry<>(mockFunctionReturnValue, 1);
        new PCollectionsImmutableNavigableMapTest.PCollectionsTreeMapWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(mock -> mock.floorEntry(eq(10)), returnValue)
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.floorEntry(10), identity())
                .doFunctionDelegationCheck();
    }

    @ParameterizedTest
    @ValueSource(ints = {11, 10})
    public void testDelegationOfCeilingKey(int mockFunctionReturnValue) {
        new PCollectionsImmutableNavigableMapTest.PCollectionsTreeMapWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(mock -> mock.ceilingKey(eq(10)), mockFunctionReturnValue)
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.ceilingKey(10), identity())
                .doFunctionDelegationCheck();
    }

    @ParameterizedTest
    @ValueSource(ints = {11, 10})
    public void testDelegationOfCeilingEntry(int mockFunctionReturnValue) {
        Map.Entry<Integer, Integer> returnValue = new AbstractMap.SimpleEntry<>(mockFunctionReturnValue, 1);
        new PCollectionsImmutableNavigableMapTest.PCollectionsTreeMapWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(mock -> mock.ceilingKey(eq(10)), returnValue)
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.ceilingKey(10), identity())
                .doFunctionDelegationCheck();
    }

    @ParameterizedTest
    @ValueSource(ints = {12, 13})
    public void testDelegationOfHigherKey(int mockFunctionReturnValue) {
        new PCollectionsImmutableNavigableMapTest.PCollectionsTreeMapWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(mock -> mock.higherKey(eq(10)), mockFunctionReturnValue)
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.higherKey(10), identity())
                .doFunctionDelegationCheck();
    }

    @ParameterizedTest
    @ValueSource(ints = {12, 13})
    public void testDelegationOfHigherKeyEntry(int mockFunctionReturnValue) {
        Map.Entry<Integer, Integer> returnValue = new AbstractMap.SimpleEntry<>(mockFunctionReturnValue, 1);
        new PCollectionsImmutableNavigableMapTest.PCollectionsTreeMapWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(mock -> mock.higherEntry(eq(10)), returnValue)
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.higherEntry(10), identity())
                .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfDescendingMap() {
        HashMap<Integer, Integer> initializeMap = new HashMap<>();
        initializeMap.put(1, 1);
        initializeMap.put(2, 2);
        initializeMap.put(3, 3);
        TreePMap<Integer, Integer> testMap = TreePMap.from(initializeMap);
        new PCollectionsImmutableNavigableMapTest.PCollectionsTreeMapWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(TreePMap::descendingMap, testMap.descendingMap())
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(PCollectionsImmutableNavigableMap::descendingMap, identity())
                .expectWrapperToWrapMockFunctionReturnValue()
                .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfSubMapWithFromAndToKeys() {
        new PCollectionsImmutableNavigableMapTest.PCollectionsTreeMapWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(mock -> mock.subMap(eq(10), eq(true), eq(30), eq(false)), TreePMap.singleton(15, 10))
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.subMap(10, true, 30, false), identity())
                .expectWrapperToWrapMockFunctionReturnValue()
                .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfHeadMapInclusive() {
        new PCollectionsImmutableNavigableMapTest.PCollectionsTreeMapWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(mock -> mock.headMap(eq(15), eq(true)), TreePMap.singleton(13, 10))
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.headMap(15, true), identity())
                .expectWrapperToWrapMockFunctionReturnValue()
                .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfTailMapInclusive() {
        new PCollectionsImmutableNavigableMapTest.PCollectionsTreeMapWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(mock -> mock.tailMap(eq(15), eq(true)), TreePMap.singleton(15, 15))
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.tailMap(15, true), identity())
                .expectWrapperToWrapMockFunctionReturnValue()
                .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfComparator() {
        HashMap<Integer, Integer> initializeMap = new HashMap<>();
        initializeMap.put(1, 1);
        initializeMap.put(2, 2);
        initializeMap.put(3, 3);
        TreePMap<Integer, Integer> testMap = TreePMap.from(initializeMap);
        new PCollectionsImmutableNavigableMapTest.PCollectionsTreeMapWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(TreePMap::comparator, testMap.comparator())
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(PCollectionsImmutableNavigableMap::comparator, identity())
                .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfSubMapWithFromKey() {
        new PCollectionsImmutableNavigableMapTest.PCollectionsTreeMapWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(mock -> mock.subMap(eq(15), eq(true)), TreePMap.singleton(13, 13))
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.subMap(15, true), identity())
                .expectWrapperToWrapMockFunctionReturnValue()
                .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfHeadMap() {
        new PCollectionsImmutableNavigableMapTest.PCollectionsTreeMapWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(mock -> mock.headMap(eq(15)), TreePMap.singleton(13, 13))
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.headMap(15), identity())
                .expectWrapperToWrapMockFunctionReturnValue()
                .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfTailMap() {
        new PCollectionsImmutableNavigableMapTest.PCollectionsTreeMapWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(mock -> mock.tailMap(eq(15)), TreePMap.singleton(13, 13))
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.tailMap(15), identity())
                .expectWrapperToWrapMockFunctionReturnValue()
                .doFunctionDelegationCheck();
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    public void testDelegationOfFirstKey(int val) {
        new PCollectionsImmutableNavigableMapTest.PCollectionsTreeMapWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(TreePMap::firstKey, val)
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(PCollectionsImmutableNavigableMap::firstKey, identity())
                .doFunctionDelegationCheck();
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    public void testDelegationOfFirstEntry(int val) {
        Map.Entry<Integer, Integer> returnValue = new AbstractMap.SimpleEntry<>(val, 1);
        new PCollectionsImmutableNavigableMapTest.PCollectionsTreeMapWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(TreePMap::firstKey, returnValue)
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(PCollectionsImmutableNavigableMap::firstKey, identity())
                .doFunctionDelegationCheck();
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    public void testDelegationOfLastKey(int val) {
        new PCollectionsImmutableNavigableMapTest.PCollectionsTreeMapWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(TreePMap::lastKey, val)
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(PCollectionsImmutableNavigableMap::lastKey, identity())
                .doFunctionDelegationCheck();
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    public void testDelegationOfLastEntry(int val) {
        Map.Entry<Integer, Integer> returnValue = new AbstractMap.SimpleEntry<>(val, 1);
        new PCollectionsImmutableNavigableMapTest.PCollectionsTreeMapWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(TreePMap::lastKey, returnValue)
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(PCollectionsImmutableNavigableMap::lastKey, identity())
                .doFunctionDelegationCheck();
    }
}