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

package org.apache.kafka.pcoll.pcollections;

import org.apache.kafka.pcoll.DelegationChecker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pcollections.HashTreePSet;
import org.pcollections.MapPSet;

import java.util.Collections;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static java.util.function.Function.identity;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;

@SuppressWarnings("unchecked")
public class PCollectionsHashSetWrapperTest {

    private static final MapPSet<Object> SINGLETON_SET = HashTreePSet.singleton(new Object());

    private static final class PCollectionsHashSetWrapperDelegationChecker<R> extends DelegationChecker<MapPSet<Object>, PCollectionsHashSetWrapper<Object>, R> {
        public PCollectionsHashSetWrapperDelegationChecker() {
            super(mock(MapPSet.class), PCollectionsHashSetWrapper::new);
        }

        public MapPSet<Object> unwrap(PCollectionsHashSetWrapper<Object> wrapper) {
            return wrapper.underlying();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testDelegationOfIsEmpty(boolean mockFunctionReturnValue) {
        new PCollectionsHashSetWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(MapPSet::isEmpty, mockFunctionReturnValue)
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(PCollectionsHashSetWrapper::isEmpty, identity())
            .doFunctionDelegationCheck();
    }

    @Test
    public void testUnderlying() {
        assertSame(SINGLETON_SET, new PCollectionsHashSetWrapper<>(SINGLETON_SET).underlying());
    }

    @Test
    public void testAsJava() {
        assertSame(SINGLETON_SET, new PCollectionsHashSetWrapper<>(SINGLETON_SET).asJava());
    }

    @Test
    public void testDelegationOfAfterAdding() {
        new PCollectionsHashSetWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(mock -> mock.plus(eq(this)), SINGLETON_SET)
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.afterAdding(this), identity())
            .expectWrapperToWrapMockFunctionReturnValue()
            .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfAfterRemoving() {
        new PCollectionsHashSetWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(mock -> mock.minus(eq(this)), SINGLETON_SET)
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.afterRemoving(this), identity())
            .expectWrapperToWrapMockFunctionReturnValue()
            .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfStream() {
        new PCollectionsHashSetWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(MapPSet::stream, mock(Stream.class))
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(PCollectionsHashSetWrapper::stream, identity())
            .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfParallelStream() {
        new PCollectionsHashSetWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(MapPSet::parallelStream, mock(Stream.class))
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(PCollectionsHashSetWrapper::parallelStream, identity())
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

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testDelegationOfContainsAll(boolean mockFunctionReturnValue) {
        Set<Object> c = Collections.emptySet();
        new PCollectionsHashSetWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(mock -> mock.containsAll(eq(c)), mockFunctionReturnValue)
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.containsAll(c), identity())
            .doFunctionDelegationCheck();
    }

    @Test
    public void testHashCode() {
        final MapPSet<Object> mock = mock(MapPSet.class);
        assertEquals(mock.hashCode(), new PCollectionsHashSetWrapper<>(mock).hashCode());
        final MapPSet<Object> someOtherMock = mock(MapPSet.class);
        assertNotEquals(mock.hashCode(), new PCollectionsHashSetWrapper<>(someOtherMock).hashCode());
    }

    @Test
    public void testEquals() {
        final MapPSet<Object> mock = mock(MapPSet.class);
        assertEquals(new PCollectionsHashSetWrapper<>(mock), new PCollectionsHashSetWrapper<>(mock));
        final MapPSet<Object> someOtherMock = mock(MapPSet.class);
        assertNotEquals(new PCollectionsHashSetWrapper<>(mock), new PCollectionsHashSetWrapper<>(someOtherMock));
    }

    @ParameterizedTest
    @ValueSource(strings = {"a", "b"})
    public void testDelegationOfToString(String mockFunctionReturnValue) {
        new PCollectionsHashSetWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(MapPSet::toString, mockFunctionReturnValue)
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(PCollectionsHashSetWrapper::toString,
                text -> "PCollectionsHashSetWrapper{underlying=" + text + "}")
            .doFunctionDelegationCheck();
    }
}
