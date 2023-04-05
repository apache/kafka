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
import org.apache.kafka.server.util.TranslatedValueMapView;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pcollections.HashPMap;
import org.pcollections.HashTreePMap;

import java.util.Collections;
import java.util.Map;

import static java.util.function.Function.identity;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;

@SuppressWarnings("unchecked")
public class PCollectionsHashMapWrapperTest {
    private static final HashPMap<Object, Object> SINGLETON_MAP = HashTreePMap.singleton(new Object(), new Object());

    private static final class PCollectionsHashMapWrapperDelegationChecker<R> extends DelegationChecker<HashPMap<Object, Object>, PCollectionsHashMapWrapper<Object, Object>, R> {
        public PCollectionsHashMapWrapperDelegationChecker() {
            super(mock(HashPMap.class), PCollectionsHashMapWrapper::new);
        }

        public HashPMap<Object, Object> unwrap(PCollectionsHashMapWrapper<Object, Object> wrapper) {
            return wrapper.underlying();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testDelegationOfIsEmpty(boolean mockFunctionReturnValue) {
        new PCollectionsHashMapWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(HashPMap::isEmpty, mockFunctionReturnValue)
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(PCollectionsHashMapWrapper::isEmpty, identity())
            .doFunctionDelegationCheck();
    }


    @Test
    public void testUnderlying() {
        assertSame(SINGLETON_MAP, new PCollectionsHashMapWrapper<>(SINGLETON_MAP).underlying());
    }

    @Test
    public void testAsJava() {
        assertSame(SINGLETON_MAP, new PCollectionsHashMapWrapper<>(SINGLETON_MAP).asJava());
    }

    @Test
    public void testAsJavaWithValueTranslation() {
        HashPMap<Integer, Integer> singletonMap = HashTreePMap.singleton(1, 1);
        Map<Integer, Integer> mapWithValuesTranslated = new PCollectionsHashMapWrapper<>(singletonMap).asJava(value -> value + 1);
        assertTrue(mapWithValuesTranslated instanceof TranslatedValueMapView);
        assertEquals(2, mapWithValuesTranslated.get(1));
    }

    @Test
    public void testDelegationOfAfterAdding() {
        new PCollectionsHashMapWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(mock -> mock.plus(eq(this), eq(this)), SINGLETON_MAP)
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.afterAdding(this, this), identity())
            .expectWrapperToWrapMockFunctionReturnValue()
            .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfAfterRemoving() {
        new PCollectionsHashMapWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(mock -> mock.minus(eq(this)), SINGLETON_MAP)
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.afterRemoving(this), identity())
            .expectWrapperToWrapMockFunctionReturnValue()
            .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfGet() {
        new PCollectionsHashMapWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(mock -> mock.get(eq(this)), new Object())
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.get(this), identity())
            .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfEntrySet() {
        new PCollectionsHashMapWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(HashPMap::entrySet, Collections.emptySet())
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(PCollectionsHashMapWrapper::entrySet, identity())
            .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfKeySet() {
        new PCollectionsHashMapWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(HashPMap::keySet, Collections.emptySet())
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(PCollectionsHashMapWrapper::keySet, identity())
            .doFunctionDelegationCheck();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testDelegationOfContainsKey(boolean mockFunctionReturnValue) {
        new PCollectionsHashMapWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(mock -> mock.containsKey(eq(this)), mockFunctionReturnValue)
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.containsKey(this), identity())
            .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfGetOrElse() {
        new PCollectionsHashMapWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(mock -> mock.getOrDefault(eq(this), eq(this)), this)
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.getOrElse(this, this), identity())
            .doFunctionDelegationCheck();
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    public void testDelegationOfSize(int mockFunctionReturnValue) {
        new PCollectionsHashMapWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(HashPMap::size, mockFunctionReturnValue)
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(PCollectionsHashMapWrapper::size, identity())
            .doFunctionDelegationCheck();
    }

    @Test
    public void testHashCode() {
        final HashPMap<Object, Object> mock = mock(HashPMap.class);
        assertEquals(mock.hashCode(), new PCollectionsHashMapWrapper<>(mock).hashCode());
        final HashPMap<Object, Object> someOtherMock = mock(HashPMap.class);
        assertNotEquals(mock.hashCode(), new PCollectionsHashMapWrapper<>(someOtherMock).hashCode());
    }

    @Test
    public void testEquals() {
        final HashPMap<Object, Object> mock = mock(HashPMap.class);
        assertEquals(new PCollectionsHashMapWrapper<>(mock), new PCollectionsHashMapWrapper<>(mock));
        final HashPMap<Object, Object> someOtherMock = mock(HashPMap.class);
        assertNotEquals(new PCollectionsHashMapWrapper<>(mock), new PCollectionsHashMapWrapper<>(someOtherMock));
    }

    @ParameterizedTest
    @ValueSource(strings = {"a", "b"})
    public void testDelegationOfToString(String mockFunctionReturnValue) {
        new PCollectionsHashMapWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(HashPMap::toString, mockFunctionReturnValue)
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(PCollectionsHashMapWrapper::toString,
                text -> "PCollectionsHashMapWrapper{underlying=" + text + "}")
            .doFunctionDelegationCheck();
    }
}
