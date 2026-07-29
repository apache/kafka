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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.streams.state.WindowStore;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Requires every {@link WindowStore} method to be classified into a contract group, so a header-aware
 * override that is silently dropped (returning raw bytes) fails this test rather than escaping to
 * manual testing (KAFKA-20328). Behavioral assertions live in
 * {@link TimestampedToHeadersWindowStoreAdapterTest}.
 * <p>
 * Signatures use erased parameter types, so a key ({@code K}) or value ({@code V}) appears as {@code Object}.
 */
public class TimestampedToHeadersWindowStoreAdapterCompletenessTest {

    private static final Set<String> VALUE_CONVERTING = Set.of(
        "fetch(Object,long)"
    );

    private static final Set<String> ITERATOR_WRAPPING = Set.of(
        "fetch(Object,long,long)",
        "fetch(Object,Instant,Instant)",
        "backwardFetch(Object,long,long)",
        "backwardFetch(Object,Instant,Instant)",
        "fetch(Object,Object,long,long)",
        "fetch(Object,Object,Instant,Instant)",
        "backwardFetch(Object,Object,long,long)",
        "backwardFetch(Object,Object,Instant,Instant)",
        "fetchAll(long,long)",
        "fetchAll(Instant,Instant)",
        "backwardFetchAll(long,long)",
        "backwardFetchAll(Instant,Instant)",
        "all()",
        "backwardAll()"
    );

    private static final Set<String> HEADER_STRIPPING = Set.of(
        "put(Object,Object,long)"
    );

    private static final Set<String> DELEGATING = Set.of(
        "name()",
        "init(StateStoreContext,StateStore)",
        "commit(Map)",
        "managesOffsets()",
        "committedOffset(TopicPartition)",
        "approximateNumUncommittedBytes()",
        "close()",
        "persistent()",
        "isOpen()",
        "getPosition()"
    );

    // query has bespoke iterator wrapping; flush is a deprecated no-op; readOnly returns `this`.
    private static final Set<String> SPECIAL = Set.of(
        "query(Query,PositionBound,QueryConfig)",
        "flush()",
        "readOnly(IsolationLevel)"
    );

    @Test
    public void everyWindowStoreMethodMustBeClassified() {
        final Set<String> classified = Stream.of(
                VALUE_CONVERTING, ITERATOR_WRAPPING, HEADER_STRIPPING, DELEGATING, SPECIAL)
            .flatMap(Set::stream)
            .collect(Collectors.toSet());

        final Set<String> unclassified = Arrays.stream(WindowStore.class.getMethods())
            .filter(m -> !m.isSynthetic())
            .filter(m -> !Modifier.isStatic(m.getModifiers()))
            .map(TimestampedToHeadersWindowStoreAdapterCompletenessTest::signature)
            .filter(sig -> !classified.contains(sig))
            .collect(Collectors.toCollection(TreeSet::new));

        assertTrue(unclassified.isEmpty(),
            "Unclassified WindowStore method(s): " + unclassified + ". Add each to a contract group in "
                + "this test AND cover it with a behavioral test in TimestampedToHeadersWindowStoreAdapterTest, "
                + "so a forgotten header conversion cannot leak raw bytes silently.");
    }

    private static String signature(final Method m) {
        return m.getName() + "(" + Arrays.stream(m.getParameterTypes())
            .map(Class::getSimpleName)
            .collect(Collectors.joining(",")) + ")";
    }
}
