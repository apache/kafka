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
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class PCollectionsHashSetWrapperTest {

    public static final MapPSet<Object> SINGLETON_SET = HashTreePSet.singleton(new Object());

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testIsEmpty(boolean expectedResult) {
        final MapPSet<Object> mock = mock(MapPSet.class);
        DelegationChecker<Boolean> delegationChecker = new DelegationChecker<>(expectedResult).answers(when(mock.isEmpty()));
        delegationChecker.assertDelegatesAndAnswersCorrectly(new PCollectionsHashSetWrapper<>(mock).isEmpty());
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
    public void testAfterAdding() {
        final MapPSet<Object> mock = mock(MapPSet.class);
        DelegationChecker<MapPSet<Object>> delegationChecker = new DelegationChecker<>(SINGLETON_SET).answers(when(mock.plus(eq(this))));
        delegationChecker.assertDelegatesAndAnswersCorrectly(new PCollectionsHashSetWrapper<>(mock).afterAdding(this).underlying());
    }

    @Test
    public void testAfterRemoving() {
        final MapPSet<Object> mock = mock(MapPSet.class);
        DelegationChecker<MapPSet<Object>> delegationChecker = new DelegationChecker<>(SINGLETON_SET).answers(when(mock.minus(eq(this))));
        delegationChecker.assertDelegatesAndAnswersCorrectly(new PCollectionsHashSetWrapper<>(mock).afterRemoving(this).underlying());
    }

    @Test
    public void testStream() {
        final MapPSet<Object> mock = mock(MapPSet.class);
        final Stream<Object> stream = mock(Stream.class);
        DelegationChecker<Stream<Object>> delegationChecker = new DelegationChecker<>(stream).answers(when(mock.stream()));
        delegationChecker.assertDelegatesAndAnswersCorrectly(new PCollectionsHashSetWrapper<>(mock).stream());
    }

    @Test
    public void testParallelStream() {
        final MapPSet<Object> mock = mock(MapPSet.class);
        final Stream<Object> stream = mock(Stream.class);
        DelegationChecker<Stream<Object>> delegationChecker = new DelegationChecker<>(stream).answers(when(mock.parallelStream()));
        delegationChecker.assertDelegatesAndAnswersCorrectly(new PCollectionsHashSetWrapper<>(mock).parallelStream());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testContainsAll(boolean expectedResult) {
        final MapPSet<Object> mock = mock(MapPSet.class);
        Set<Object> c = Collections.emptySet();
        DelegationChecker<Boolean> delegationChecker = new DelegationChecker<>(expectedResult).answers(when(mock.containsAll(c)));
        delegationChecker.assertDelegatesAndAnswersCorrectly(new PCollectionsHashSetWrapper<>(mock).containsAll(c));
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
    public void testToString(String underlyingToStringResult) {
        final MapPSet<Object> mock = mock(MapPSet.class);
        when(mock.toString()).thenReturn(underlyingToStringResult);
        assertEquals("PCollectionsHashSetWrapper{underlying=" + underlyingToStringResult + "}",
            new PCollectionsHashSetWrapper<>(mock).toString());
    }
}
