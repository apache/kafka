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
import org.apache.kafka.server.immutable.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pcollections.HashTreePSet;
import org.pcollections.MapPSet;

import java.util.Collections;
import java.util.Iterator;
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
public class PCollectionsImmutableSetTest {

    private static final MapPSet<Object> SINGLETON_SET = HashTreePSet.singleton(new Object());

    private static final class PCollectionsHashSetWrapperDelegationChecker<R> extends DelegationChecker<MapPSet<Object>, PCollectionsImmutableSet<Object>, R> {
        public PCollectionsHashSetWrapperDelegationChecker() {
            super(mock(MapPSet.class), PCollectionsImmutableSet::new);
        }

        public MapPSet<Object> unwrap(PCollectionsImmutableSet<Object> wrapper) {
            return wrapper.underlying();
        }
    }

    @Test
    public void testEmptySet() {
        Assertions.assertEquals(HashTreePSet.empty(), ((PCollectionsImmutableSet<?>) ImmutableSet.empty()).underlying());
    }

    @Test
    public void testSingletonSet() {
        Assertions.assertEquals(HashTreePSet.singleton(1), ((PCollectionsImmutableSet<?>) ImmutableSet.singleton(1)).underlying());
    }

    @Test
    public void testUnderlying() {
        assertSame(SINGLETON_SET, new PCollectionsImmutableSet<>(SINGLETON_SET).underlying());
    }

    @Test
    public void testDelegationOfAfterAdding() {
        new PCollectionsHashSetWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(mock -> mock.plus(eq(this)), SINGLETON_SET)
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.added(this), identity())
            .expectWrapperToWrapMockFunctionReturnValue()
            .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfAfterRemoving() {
        new PCollectionsHashSetWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(mock -> mock.minus(eq(this)), SINGLETON_SET)
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.removed(this), identity())
            .expectWrapperToWrapMockFunctionReturnValue()
            .doFunctionDelegationCheck();
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    public void testDelegationOfSize(int mockFunctionReturnValue) {
        new PCollectionsHashSetWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(MapPSet::size, mockFunctionReturnValue)
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(PCollectionsImmutableSet::size, identity())
            .doFunctionDelegationCheck();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testDelegationOfIsEmpty(boolean mockFunctionReturnValue) {
        new PCollectionsHashSetWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(MapPSet::isEmpty, mockFunctionReturnValue)
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(PCollectionsImmutableSet::isEmpty, identity())
            .doFunctionDelegationCheck();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testDelegationOfContains(boolean mockFunctionReturnValue) {
        new PCollectionsHashSetWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(mock -> mock.contains(eq(this)), mockFunctionReturnValue)
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.contains(this), identity())
            .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfIterator() {
        new PCollectionsHashSetWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(MapPSet::iterator, mock(Iterator.class))
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(PCollectionsImmutableSet::iterator, identity())
            .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfForEach() {
        final Consumer<Object> mockConsumer = mock(Consumer.class);
        new PCollectionsHashSetWrapperDelegationChecker<>()
            .defineMockConfigurationForVoidMethodInvocation(mock -> mock.forEach(eq(mockConsumer)))
            .defineWrapperVoidMethodInvocation(wrapper -> wrapper.forEach(mockConsumer))
            .doVoidMethodDelegationCheck();
    }

    @Test
    public void testDelegationOfToArray() {
        new PCollectionsHashSetWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(MapPSet::toArray, new Object[0])
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(PCollectionsImmutableSet::toArray, identity())
            .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfToArrayIntoGivenDestination() {
        Object[] destinationArray = new Object[0];
        new PCollectionsHashSetWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(mock -> mock.toArray(eq(destinationArray)), new Object[0])
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.toArray(destinationArray), identity())
            .doFunctionDelegationCheck();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testDelegationOfAdd(boolean mockFunctionReturnValue) {
        new PCollectionsHashSetWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(mock -> mock.add(eq(this)), mockFunctionReturnValue)
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.add(this), identity())
            .doFunctionDelegationCheck();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testDelegationOfRemove(boolean mockFunctionReturnValue) {
        new PCollectionsHashSetWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(mock -> mock.remove(eq(this)), mockFunctionReturnValue)
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.remove(this), identity())
            .doFunctionDelegationCheck();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testDelegationOfContainsAll(boolean mockFunctionReturnValue) {
        new PCollectionsHashSetWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(mock -> mock.containsAll(eq(Collections.emptyList())), mockFunctionReturnValue)
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.containsAll(Collections.emptyList()), identity())
            .doFunctionDelegationCheck();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testDelegationOfAddAll(boolean mockFunctionReturnValue) {
        new PCollectionsHashSetWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(mock -> mock.addAll(eq(Collections.emptyList())), mockFunctionReturnValue)
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.addAll(Collections.emptyList()), identity())
            .doFunctionDelegationCheck();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testDelegationOfRetainAll(boolean mockFunctionReturnValue) {
        new PCollectionsHashSetWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(mock -> mock.retainAll(eq(Collections.emptyList())), mockFunctionReturnValue)
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.retainAll(Collections.emptyList()), identity())
            .doFunctionDelegationCheck();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testDelegationOfRemoveAll(boolean mockFunctionReturnValue) {
        new PCollectionsHashSetWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(mock -> mock.removeAll(eq(Collections.emptyList())), mockFunctionReturnValue)
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.removeAll(Collections.emptyList()), identity())
            .doFunctionDelegationCheck();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testDelegationOfRemoveIf(boolean mockFunctionReturnValue) {
        final Predicate<Object> mockPredicate = mock(Predicate.class);
        new PCollectionsHashSetWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(mock -> mock.removeIf(eq(mockPredicate)), mockFunctionReturnValue)
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.removeIf(mockPredicate), identity())
            .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfClear() {
        new PCollectionsHashSetWrapperDelegationChecker<>()
            .defineMockConfigurationForVoidMethodInvocation(MapPSet::clear)
            .defineWrapperVoidMethodInvocation(PCollectionsImmutableSet::clear)
            .doVoidMethodDelegationCheck();
    }

    @Test
    public void testDelegationOfSpliterator() {
        new PCollectionsHashSetWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(MapPSet::spliterator, mock(Spliterator.class))
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(PCollectionsImmutableSet::spliterator, identity())
            .doFunctionDelegationCheck();
    }


    @Test
    public void testDelegationOfStream() {
        new PCollectionsHashSetWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(MapPSet::stream, mock(Stream.class))
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(PCollectionsImmutableSet::stream, identity())
            .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfParallelStream() {
        new PCollectionsHashSetWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(MapPSet::parallelStream, mock(Stream.class))
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(PCollectionsImmutableSet::parallelStream, identity())
            .doFunctionDelegationCheck();
    }

    @Test
    public void testEquals() {
        final MapPSet<Object> mock = mock(MapPSet.class);
        assertEquals(new PCollectionsImmutableSet<>(mock), new PCollectionsImmutableSet<>(mock));
        final MapPSet<Object> someOtherMock = mock(MapPSet.class);
        assertNotEquals(new PCollectionsImmutableSet<>(mock), new PCollectionsImmutableSet<>(someOtherMock));
    }

    @Test
    public void testHashCode() {
        final MapPSet<Object> mock = mock(MapPSet.class);
        assertEquals(mock.hashCode(), new PCollectionsImmutableSet<>(mock).hashCode());
        final MapPSet<Object> someOtherMock = mock(MapPSet.class);
        assertNotEquals(mock.hashCode(), new PCollectionsImmutableSet<>(someOtherMock).hashCode());
    }

    @ParameterizedTest
    @ValueSource(strings = {"a", "b"})
    public void testDelegationOfToString(String mockFunctionReturnValue) {
        new PCollectionsHashSetWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(MapPSet::toString, mockFunctionReturnValue)
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(PCollectionsImmutableSet::toString,
                text -> "PCollectionsImmutableSet{underlying=" + text + "}")
            .doFunctionDelegationCheck();
    }
}
