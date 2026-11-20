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

import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.VersionedBytesStore;
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
 * Pins, for each format-converting byte-store adapter without a dedicated completeness test, the exact set of
 * store-interface methods it leaves to the interface default (KAFKA-20328). A dropped override silently changes
 * behavior: e.g. a missing {@code backwardFetch} inherits the {@link WindowStore} default that throws
 * {@link UnsupportedOperationException}, and a missing {@code reverseRange} stops reaching the inner store.
 * <p>
 * {@link SessionToHeadersStoreAdapter}, {@link TimestampedToHeadersWindowStoreAdapter},
 * {@link KeyValueTimestampedHeaderStoreToKeyValueTimestampStoreAdapter} and
 * {@link WindowedTimestampedHeadersStoreToWindowedTimestampedStoreAdapter} have their own completeness tests.
 * <p>
 * The allow-lists are exact, so adding an override also requires updating this test.
 */
public class StoreAdapterCompletenessTest {

    // readOnly is NOT harmless: the default returns `this`, so the isolation level never reaches the inner
    // store and READ_COMMITTED reads can observe uncommitted writes. Tracked by KAFKA-21505; remove it from
    // the allow-lists once the adapters override it.
    private static final String READ_ONLY = "readOnly(IsolationLevel)";

    // Deprecated StateStore#flush is a no-op default; the adapters intentionally do not forward it.
    private static final String FLUSH = "flush()";

    private static Stream<Arguments> adapters() {
        final Set<String> flushAndReadOnly = Set.of(FLUSH, READ_ONLY);
        return Stream.of(
            Arguments.of(PlainToHeadersStoreAdapter.class, KeyValueStore.class, flushAndReadOnly),
            Arguments.of(TimestampedToHeadersStoreAdapter.class, KeyValueStore.class, flushAndReadOnly),
            Arguments.of(PlainToHeadersWindowStoreAdapter.class, WindowStore.class, flushAndReadOnly),
            Arguments.of(KeyValueToTimestampedKeyValueByteStoreAdapter.class, KeyValueStore.class, flushAndReadOnly),
            Arguments.of(WindowToTimestampedWindowByteStoreAdapter.class, WindowStore.class, flushAndReadOnly),
            Arguments.of(VersionedKeyValueToBytesStoreAdapter.class, VersionedBytesStore.class, Set.of(FLUSH))
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("adapters")
    public void shouldOnlyFallBackToAllowListedInterfaceDefaults(final Class<? extends StateStore> adapter,
                                                                 final Class<? extends StateStore> storeInterface,
                                                                 final Set<String> allowedDefaults) {
        assertEquals(new TreeSet<>(allowedDefaults), inheritedInterfaceDefaults(adapter, storeInterface),
            adapter.getSimpleName() + " falls back to a different set of " + storeInterface.getSimpleName()
                + " interface defaults than expected. An inherited default cannot reach the inner store: it "
                + "either throws UnsupportedOperationException or silently stops delegating. Override the "
                + "method, or (if a new override was added) remove it from the allow-list.");
    }
}
