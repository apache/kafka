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
import org.apache.kafka.server.immutable.ImmutableNavigableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pcollections.HashTreePSet;
import org.pcollections.TreePSet;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Random;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.function.Function.identity;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;

@SuppressWarnings({"unchecked", "deprecation"})
public class PCollectionsImmutableNavigableSetTest {

    private static final TreePSet<Integer> SINGLETON_SET = TreePSet.singleton(new Random().nextInt());

    private static final class PCollectionsTreeSetWrapperDelegationChecker<R> extends DelegationChecker<TreePSet<Object>, PCollectionsImmutableNavigableSet<Object>, R> {
        public PCollectionsTreeSetWrapperDelegationChecker() {
            super(mock(TreePSet.class), PCollectionsImmutableNavigableSet::new);
        }

        public TreePSet<Object> unwrap(PCollectionsImmutableNavigableSet<Object> wrapper) {
            return wrapper.underlying();
        }
    }

    @Test
    public void testEmptySet() {
        Assertions.assertEquals(HashTreePSet.empty(), ((PCollectionsImmutableNavigableSet<?>) ImmutableNavigableSet.empty()).underlying());
    }

    @Test
    public void testSingletonSet() {
        Assertions.assertEquals(HashTreePSet.singleton(1), ((PCollectionsImmutableNavigableSet<?>) ImmutableNavigableSet.singleton(1)).underlying());
    }

    @Test
    public void testUnderlying() {
        assertSame(SINGLETON_SET, new PCollectionsImmutableNavigableSet<>(SINGLETON_SET).underlying());
    }

    @Test
    public void testDelegationOfAdded() {
        new PCollectionsTreeSetWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(mock -> mock.plus(eq(SINGLETON_SET.first())), SINGLETON_SET)
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.added(SINGLETON_SET.first()), identity())
                .expectWrapperToWrapMockFunctionReturnValue()
                .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfRemoved() {
        new PCollectionsTreeSetWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(mock -> mock.minus(eq(10)), SINGLETON_SET)
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.removed(10), identity())
                .expectWrapperToWrapMockFunctionReturnValue()
                .doFunctionDelegationCheck();
    }

    @ParameterizedTest
    @ValueSource(ints = {9, 4})
    public void testDelegationOfLower(int mockFunctionReturnValue) {
        new PCollectionsTreeSetWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(mock -> mock.lower(eq(10)), mockFunctionReturnValue)
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.lower(10), identity())
                .doFunctionDelegationCheck();
    }

    @ParameterizedTest
    @ValueSource(ints = {9, 10})
    public void testDelegationOfFloor(int mockFunctionReturnValue) {
        new PCollectionsTreeSetWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(mock -> mock.floor(eq(10)), mockFunctionReturnValue)
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.floor(10), identity())
                .doFunctionDelegationCheck();
    }

    @ParameterizedTest
    @ValueSource(ints = {11, 10})
    public void testDelegationOfCeiling(int mockFunctionReturnValue) {
        new PCollectionsTreeSetWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(mock -> mock.ceiling(eq(10)), mockFunctionReturnValue)
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.ceiling(10), identity())
                .doFunctionDelegationCheck();
    }

    @ParameterizedTest
    @ValueSource(ints = {12, 13})
    public void testDelegationOfHigher(int mockFunctionReturnValue) {
        new PCollectionsTreeSetWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(mock -> mock.higher(eq(10)), mockFunctionReturnValue)
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.higher(10), identity())
                .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfUnsupportedFunctionPollFirst() {
        new PCollectionsTreeSetWrapperDelegationChecker<>()
                .defineMockConfigurationForUnsupportedFunction(TreePSet::pollFirst)
                .defineWrapperUnsupportedFunctionInvocation(PCollectionsImmutableNavigableSet::pollFirst)
                .doUnsupportedFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfUnsupportedFunctionPollLast() {
        new PCollectionsTreeSetWrapperDelegationChecker<>()
                .defineMockConfigurationForUnsupportedFunction(TreePSet::pollLast)
                .defineWrapperUnsupportedFunctionInvocation(PCollectionsImmutableNavigableSet::pollLast)
                .doUnsupportedFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfDescendingSet() {
        TreePSet<Integer> testSet = TreePSet.from(Arrays.asList(2, 3, 4));
        new PCollectionsTreeSetWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(TreePSet::descendingSet, testSet.descendingSet())
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(PCollectionsImmutableNavigableSet::descendingSet, identity())
                .expectWrapperToWrapMockFunctionReturnValue()
                .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfDescendingIterator() {
        TreePSet<Integer> testSet = TreePSet.from(Arrays.asList(2, 3, 4));
        new PCollectionsTreeSetWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(TreePSet::descendingIterator, testSet.descendingIterator())
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(PCollectionsImmutableNavigableSet::descendingIterator, identity())
                .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfSubSetWithFromAndToElements() {
        new PCollectionsTreeSetWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(mock -> mock.subSet(eq(10), eq(true), eq(30), eq(false)), TreePSet.singleton(15))
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.subSet(10, true, 30, false), identity())
                .expectWrapperToWrapMockFunctionReturnValue()
                .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfHeadSetInclusive() {
        new PCollectionsTreeSetWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(mock -> mock.headSet(eq(15), eq(true)), TreePSet.singleton(13))
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.headSet(15, true), identity())
                .expectWrapperToWrapMockFunctionReturnValue()
                .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfTailSetInclusive() {
        new PCollectionsTreeSetWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(mock -> mock.tailSet(eq(15), eq(true)), TreePSet.singleton(15))
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.tailSet(15, true), identity())
                .expectWrapperToWrapMockFunctionReturnValue()
                .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfComparator() {
        TreePSet<Integer> testSet = TreePSet.from(Arrays.asList(3, 4, 5));
        new PCollectionsTreeSetWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(TreePSet::comparator, testSet.comparator())
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(PCollectionsImmutableNavigableSet::comparator, identity())
                .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfSubSetWithFromElement() {
        new PCollectionsTreeSetWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(mock -> mock.subSet(eq(15), eq(true)), TreePSet.singleton(13))
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.subSet(15, true), identity())
                .expectWrapperToWrapMockFunctionReturnValue()
                .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfHeadSet() {
        new PCollectionsTreeSetWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(mock -> mock.headSet(eq(15)), TreePSet.singleton(13))
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.headSet(15), identity())
                .expectWrapperToWrapMockFunctionReturnValue()
                .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfTailSet() {
        new PCollectionsTreeSetWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(mock -> mock.tailSet(eq(15)), TreePSet.singleton(13))
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.tailSet(15), identity())
                .expectWrapperToWrapMockFunctionReturnValue()
                .doFunctionDelegationCheck();
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    public void testDelegationOfFirst() {
        new PCollectionsTreeSetWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(TreePSet::first, 13)
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(PCollectionsImmutableNavigableSet::first, identity())
                .doFunctionDelegationCheck();
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    public void testDelegationOfLast() {
        new PCollectionsTreeSetWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(TreePSet::last, 13)
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(PCollectionsImmutableNavigableSet::last, identity())
                .doFunctionDelegationCheck();
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    public void testDelegationOfSize(int mockFunctionReturnValue) {
        new PCollectionsTreeSetWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(TreePSet::size, mockFunctionReturnValue)
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(PCollectionsImmutableNavigableSet::size, identity())
                .doFunctionDelegationCheck();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testDelegationOfIsEmpty(boolean mockFunctionReturnValue) {
        new PCollectionsTreeSetWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(TreePSet::isEmpty, mockFunctionReturnValue)
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(PCollectionsImmutableNavigableSet::isEmpty, identity())
                .doFunctionDelegationCheck();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testDelegationOfContains(boolean mockFunctionReturnValue) {
        new PCollectionsTreeSetWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(mock -> mock.contains(eq(this)), mockFunctionReturnValue)
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.contains(this), identity())
                .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfIterator() {
        new PCollectionsTreeSetWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(TreePSet::iterator, mock(Iterator.class))
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(PCollectionsImmutableNavigableSet::iterator, identity())
                .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfForEach() {
        final Consumer<Object> mockConsumer = mock(Consumer.class);
        new PCollectionsTreeSetWrapperDelegationChecker<>()
                .defineMockConfigurationForVoidMethodInvocation(mock -> mock.forEach(eq(mockConsumer)))
                .defineWrapperVoidMethodInvocation(wrapper -> wrapper.forEach(mockConsumer))
                .doVoidMethodDelegationCheck();
    }

    @Test
    public void testDelegationOfToArray() {
        new PCollectionsTreeSetWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(TreePSet::toArray, new Object[0])
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(PCollectionsImmutableNavigableSet::toArray, identity())
                .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfToArrayIntoGivenDestination() {
        Object[] destinationArray = new Object[0];
        new PCollectionsTreeSetWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(mock -> mock.toArray(eq(destinationArray)), new Object[0])
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.toArray(destinationArray), identity())
                .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfUnsupportedFunctionAdd() {
        new PCollectionsTreeSetWrapperDelegationChecker<>()
                .defineMockConfigurationForUnsupportedFunction(mock -> mock.add(eq(this)))
                .defineWrapperUnsupportedFunctionInvocation(wrapper -> wrapper.add(this))
                .doUnsupportedFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfUnsupportedFunctionRemove() {
        new PCollectionsTreeSetWrapperDelegationChecker<>()
                .defineMockConfigurationForUnsupportedFunction(mock -> mock.remove(eq(this)))
                .defineWrapperUnsupportedFunctionInvocation(wrapper -> wrapper.remove(this))
                .doUnsupportedFunctionDelegationCheck();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testDelegationOfContainsAll(boolean mockFunctionReturnValue) {
        new PCollectionsTreeSetWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(mock -> mock.containsAll(eq(Collections.emptyList())), mockFunctionReturnValue)
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.containsAll(Collections.emptyList()), identity())
                .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfUnsupportedFunctionAddAll() {
        new PCollectionsTreeSetWrapperDelegationChecker<>()
                .defineMockConfigurationForUnsupportedFunction(mock -> mock.addAll(eq(Collections.emptyList())))
                .defineWrapperUnsupportedFunctionInvocation(wrapper -> wrapper.addAll(Collections.emptyList()))
                .doUnsupportedFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfUnsupportedFunctionRetainAll() {
        new PCollectionsTreeSetWrapperDelegationChecker<>()
                .defineMockConfigurationForUnsupportedFunction(mock -> mock.retainAll(eq(Collections.emptyList())))
                .defineWrapperUnsupportedFunctionInvocation(wrapper -> wrapper.retainAll(Collections.emptyList()))
                .doUnsupportedFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfUnsupportedFunctionRemoveAll() {
        new PCollectionsTreeSetWrapperDelegationChecker<>()
                .defineMockConfigurationForUnsupportedFunction(mock -> mock.removeAll(eq(Collections.emptyList())))
                .defineWrapperUnsupportedFunctionInvocation(wrapper -> wrapper.removeAll(Collections.emptyList()))
                .doUnsupportedFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfUnsupportedFunctionRemoveIf() {
        final Predicate<Object> mockPredicate = mock(Predicate.class);
        new PCollectionsTreeSetWrapperDelegationChecker<>()
                .defineMockConfigurationForUnsupportedFunction(mock -> mock.removeIf(eq(mockPredicate)))
                .defineWrapperUnsupportedFunctionInvocation(wrapper -> wrapper.removeIf(mockPredicate))
                .doUnsupportedFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfUnsupportedFunctionClear() {
        new PCollectionsTreeSetWrapperDelegationChecker<>()
                .defineMockConfigurationForVoidMethodInvocation(TreePSet::clear)
                .defineWrapperVoidMethodInvocation(PCollectionsImmutableNavigableSet::clear)
                .doUnsupportedVoidFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfSpliterator() {
        new PCollectionsTreeSetWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(TreePSet::spliterator, mock(Spliterator.class))
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(PCollectionsImmutableNavigableSet::spliterator, identity())
                .doFunctionDelegationCheck();
    }


    @Test
    public void testDelegationOfStream() {
        new PCollectionsTreeSetWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(TreePSet::stream, mock(Stream.class))
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(PCollectionsImmutableNavigableSet::stream, identity())
                .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfParallelStream() {
        new PCollectionsTreeSetWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(TreePSet::parallelStream, mock(Stream.class))
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(PCollectionsImmutableNavigableSet::parallelStream, identity())
                .doFunctionDelegationCheck();
    }

    @Test
    public void testEquals() {
        final TreePSet<Object> mock = mock(TreePSet.class);
        assertEquals(new PCollectionsImmutableNavigableSet<>(mock), new PCollectionsImmutableNavigableSet<>(mock));
        final TreePSet<Object> someOtherMock = mock(TreePSet.class);
        assertNotEquals(new PCollectionsImmutableNavigableSet<>(mock), new PCollectionsImmutableNavigableSet<>(someOtherMock));
    }

    @Test
    public void testHashCode() {
        final TreePSet<Object> mock = mock(TreePSet.class);
        assertEquals(mock.hashCode(), new PCollectionsImmutableNavigableSet<>(mock).hashCode());
        final TreePSet<Object> someOtherMock = mock(TreePSet.class);
        assertNotEquals(mock.hashCode(), new PCollectionsImmutableNavigableSet<>(someOtherMock).hashCode());
    }

    @ParameterizedTest
    @ValueSource(strings = {"a", "b"})
    public void testDelegationOfToString(String mockFunctionReturnValue) {
        new PCollectionsTreeSetWrapperDelegationChecker<>()
                .defineMockConfigurationForFunctionInvocation(TreePSet::toString, mockFunctionReturnValue)
                .defineWrapperFunctionInvocationAndMockReturnValueTransformation(PCollectionsImmutableNavigableSet::toString,
                        text -> "PCollectionsImmutableNavigableSet{underlying=" + text + "}")
                .doFunctionDelegationCheck();
    }
}
