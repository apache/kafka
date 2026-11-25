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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.AbstractReadOnlyDecorator.KeyValueStoreReadOnlyDecorator;
import org.apache.kafka.streams.processor.internals.AbstractReadOnlyDecorator.SessionStoreReadOnlyDecorator;
import org.apache.kafka.streams.processor.internals.AbstractReadOnlyDecorator.TimestampedKeyValueStoreReadOnlyDecorator;
import org.apache.kafka.streams.processor.internals.AbstractReadOnlyDecorator.TimestampedKeyValueStoreReadOnlyDecoratorWithHeaders;
import org.apache.kafka.streams.processor.internals.AbstractReadOnlyDecorator.TimestampedWindowStoreReadOnlyDecorator;
import org.apache.kafka.streams.processor.internals.AbstractReadOnlyDecorator.VersionedKeyValueStoreReadOnlyDecorator;
import org.apache.kafka.streams.processor.internals.AbstractReadOnlyDecorator.WindowStoreReadOnlyDecorator;
import org.apache.kafka.streams.processor.internals.AbstractReadWriteDecorator.KeyValueStoreReadWriteDecorator;
import org.apache.kafka.streams.processor.internals.AbstractReadWriteDecorator.SessionStoreReadWriteDecorator;
import org.apache.kafka.streams.processor.internals.AbstractReadWriteDecorator.SessionStoreWithHeadersReadWriteDecorator;
import org.apache.kafka.streams.processor.internals.AbstractReadWriteDecorator.TimestampedKeyValueStoreReadWriteDecorator;
import org.apache.kafka.streams.processor.internals.AbstractReadWriteDecorator.TimestampedKeyValueStoreReadWriteDecoratorWithHeaders;
import org.apache.kafka.streams.processor.internals.AbstractReadWriteDecorator.TimestampedWindowStoreReadWriteDecorator;
import org.apache.kafka.streams.processor.internals.AbstractReadWriteDecorator.TimestampedWindowStoreWithHeadersReadWriteDecorator;
import org.apache.kafka.streams.processor.internals.AbstractReadWriteDecorator.VersionedKeyValueStoreReadWriteDecorator;
import org.apache.kafka.streams.processor.internals.AbstractReadWriteDecorator.WindowStoreReadWriteDecorator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.SessionStoreWithHeaders;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.TimestampedWindowStoreWithHeaders;
import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.apache.kafka.streams.state.WindowStore;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

import static org.apache.kafka.streams.state.internals.StoreMethodSignatures.inheritedInterfaceDefaults;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Pins, for every {@link AbstractReadWriteDecorator} and {@link AbstractReadOnlyDecorator} subclass, the exact
 * set of store-interface methods it leaves to the interface default (KAFKA-20328). A decorator that drops an
 * override silently changes behavior inside a processor: e.g. a missing {@code backwardFetch} inherits the
 * {@link WindowStore} default that throws {@link UnsupportedOperationException}. Behavioral coverage lives in
 * {@link AbstractReadWriteDecoratorTest}, {@link AbstractReadOnlyDecoratorTest} and {@link ProcessorContextImplTest}.
 * <p>
 * The allow-lists are exact, so adding an override also requires updating this test.
 */
public class DecoratorCompletenessTest {

    // readOnly is NOT harmless: the default returns `this`, so the isolation level is not propagated to the
    // wrapped store even though ReadOnlyKeyValueStore#readOnly documents that wrapping stores propagate it.
    // Tracked by KAFKA-21505; remove it from the allow-lists once the decorators override it.
    private static final String READ_ONLY = "readOnly(IsolationLevel)";

    // Deprecated StateStore#flush is a no-op default; read-write decorators intentionally do not forward it,
    // read-only decorators override it to throw.
    private static final String FLUSH = "flush()";

    // WindowStore defaults that validate the Instants and delegate to the long overloads the decorators override.
    private static final Set<String> WINDOW_INSTANT_DEFAULTS = Set.of(
        "fetch(Object,Instant,Instant)",
        "fetch(Object,Object,Instant,Instant)",
        "backwardFetch(Object,Instant,Instant)",
        "backwardFetch(Object,Object,Instant,Instant)",
        "fetchAll(Instant,Instant)",
        "backwardFetchAll(Instant,Instant)"
    );

    private static Stream<Arguments> decorators() {
        final Set<String> readWrite = Set.of(READ_ONLY, FLUSH);
        final Set<String> readWriteWindow = union(readWrite, WINDOW_INSTANT_DEFAULTS);
        final Set<String> readOnly = Set.of(READ_ONLY);
        final Set<String> readOnlyWindow = union(readOnly, WINDOW_INSTANT_DEFAULTS);
        return Stream.of(
            Arguments.of(KeyValueStoreReadWriteDecorator.class, KeyValueStore.class, readWrite),
            Arguments.of(TimestampedKeyValueStoreReadWriteDecorator.class, TimestampedKeyValueStore.class, readWrite),
            Arguments.of(TimestampedKeyValueStoreReadWriteDecoratorWithHeaders.class, TimestampedKeyValueStoreWithHeaders.class, readWrite),
            Arguments.of(VersionedKeyValueStoreReadWriteDecorator.class, VersionedKeyValueStore.class, readWrite),
            Arguments.of(WindowStoreReadWriteDecorator.class, WindowStore.class, readWriteWindow),
            Arguments.of(TimestampedWindowStoreReadWriteDecorator.class, TimestampedWindowStore.class, readWriteWindow),
            Arguments.of(TimestampedWindowStoreWithHeadersReadWriteDecorator.class, TimestampedWindowStoreWithHeaders.class, readWriteWindow),
            Arguments.of(SessionStoreReadWriteDecorator.class, SessionStore.class, readWrite),
            Arguments.of(SessionStoreWithHeadersReadWriteDecorator.class, SessionStoreWithHeaders.class, readWrite),
            Arguments.of(KeyValueStoreReadOnlyDecorator.class, KeyValueStore.class, readOnly),
            Arguments.of(TimestampedKeyValueStoreReadOnlyDecorator.class, TimestampedKeyValueStore.class, readOnly),
            Arguments.of(TimestampedKeyValueStoreReadOnlyDecoratorWithHeaders.class, TimestampedKeyValueStoreWithHeaders.class, readOnly),
            Arguments.of(VersionedKeyValueStoreReadOnlyDecorator.class, VersionedKeyValueStore.class, readOnly),
            Arguments.of(WindowStoreReadOnlyDecorator.class, WindowStore.class, readOnlyWindow),
            Arguments.of(TimestampedWindowStoreReadOnlyDecorator.class, TimestampedWindowStore.class, readOnlyWindow),
            Arguments.of(SessionStoreReadOnlyDecorator.class, SessionStore.class, readOnly)
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("decorators")
    public void shouldOnlyFallBackToAllowListedInterfaceDefaults(final Class<? extends StateStore> decorator,
                                                                 final Class<? extends StateStore> storeInterface,
                                                                 final Set<String> allowedDefaults) {
        assertEquals(new TreeSet<>(allowedDefaults), inheritedInterfaceDefaults(decorator, storeInterface),
            decorator.getSimpleName() + " falls back to a different set of " + storeInterface.getSimpleName()
                + " interface defaults than expected. An inherited default cannot reach the wrapped store: it "
                + "either throws UnsupportedOperationException or silently stops delegating. Override the "
                + "method, or (if a new override was added) remove it from the allow-list.");
    }

    private static Set<String> union(final Set<String> a, final Set<String> b) {
        final Set<String> result = new TreeSet<>(a);
        result.addAll(b);
        return result;
    }
}
