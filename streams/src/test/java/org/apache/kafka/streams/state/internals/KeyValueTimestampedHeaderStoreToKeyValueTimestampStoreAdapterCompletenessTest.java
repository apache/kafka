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

import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;

import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.streams.state.internals.StoreMethodSignatures.inheritedInterfaceDefaults;
import static org.apache.kafka.streams.state.internals.StoreMethodSignatures.instanceMethods;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Requires every {@link TimestampedKeyValueStoreWithHeaders} method to be classified into a contract group,
 * and every method except {@link #INHERITED_DEFAULTS} to be overridden by
 * {@link KeyValueTimestampedHeaderStoreToKeyValueTimestampStoreAdapter} (or its {@link WrappedStateStore}
 * base), so a dropped override fails this test rather than escaping to manual testing (KAFKA-20328).
 * Behavioral assertions live in {@link KeyValueTimestampedHeaderStoreToKeyValueTimestampStoreAdapterTest}.
 */
public class KeyValueTimestampedHeaderStoreToKeyValueTimestampStoreAdapterCompletenessTest {

    private static final Set<String> VALUE_CONVERTING = Set.of(
        "get(Object)",
        "delete(Object)",
        "putIfAbsent(Object,Object)"
    );

    private static final Set<String> ITERATOR_WRAPPING = Set.of(
        "range(Object,Object)",
        "reverseRange(Object,Object)",
        "all()",
        "reverseAll()",
        "prefixScan(Object,Serializer)"
    );

    private static final Set<String> HEADER_STRIPPING = Set.of(
        "put(Object,Object)",
        "putAll(List)"
    );

    private static final Set<String> DELEGATING = Set.of(
        "approximateNumEntries()",
        "name()",
        "init(StateStoreContext,StateStore)",
        "commit(Map)",
        "managesOffsets()",
        "committedOffset(TopicPartition)",
        "approximateNumUncommittedBytes()",
        "close()",
        "persistent()",
        "isOpen()",
        "query(Query,PositionBound,QueryConfig)",
        "getPosition()",
        "flush()"
    );

    // readOnly is NOT harmless: the adapter inherits the default that returns `this`, so the isolation
    // level never reaches the inner store and READ_COMMITTED reads can observe uncommitted writes.
    // Tracked by KAFKA-21505; remove readOnly from here once the adapter overrides it.
    private static final Set<String> INHERITED_DEFAULTS = Set.of(
        "readOnly(IsolationLevel)"
    );

    @Test
    public void everyStoreMethodMustBeClassified() {
        final Set<String> classified = Stream.of(
                VALUE_CONVERTING, ITERATOR_WRAPPING, HEADER_STRIPPING, DELEGATING, INHERITED_DEFAULTS)
            .flatMap(Set::stream)
            .collect(Collectors.toSet());

        final Set<String> unclassified = instanceMethods(TimestampedKeyValueStoreWithHeaders.class)
            .map(StoreMethodSignatures::signature)
            .filter(sig -> !classified.contains(sig))
            .collect(Collectors.toCollection(TreeSet::new));

        assertTrue(unclassified.isEmpty(),
            "Unclassified TimestampedKeyValueStoreWithHeaders method(s): " + unclassified + ". Add each to a "
                + "contract group in this test AND cover it with a behavioral test in "
                + "KeyValueTimestampedHeaderStoreToKeyValueTimestampStoreAdapterTest.");
    }

    @Test
    public void onlyAllowListedMethodsMayFallBackToInterfaceDefaults() {
        assertEquals(INHERITED_DEFAULTS,
            inheritedInterfaceDefaults(
                KeyValueTimestampedHeaderStoreToKeyValueTimestampStoreAdapter.class,
                TimestampedKeyValueStoreWithHeaders.class),
            "An inherited interface default cannot reach the inner store: it either throws "
                + "UnsupportedOperationException or silently stops delegating. Override the method in the "
                + "adapter, or (if it now overrides an allow-listed method) remove it from INHERITED_DEFAULTS.");
    }
}
