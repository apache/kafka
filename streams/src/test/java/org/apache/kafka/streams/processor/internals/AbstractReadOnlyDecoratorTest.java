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
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.SessionStoreWithHeaders;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.TimestampedWindowStoreWithHeaders;
import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.apache.kafka.streams.state.WindowStore;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.stream.Stream;

import static org.apache.kafka.streams.processor.internals.AbstractReadOnlyDecorator.getReadOnlyStore;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class AbstractReadOnlyDecoratorTest {

    // Dispatch tests pin getReadOnlyStore to the exact decorator per store type. Because the
    // *WithHeaders interfaces extend their base store interface, a reordered/removed instanceof check
    // would silently fall through to the base decorator; asserting the exact class catches that.
    private static Stream<Arguments> storeTypes() {
        return Stream.of(
            Arguments.of(TimestampedKeyValueStoreWithHeaders.class, TimestampedKeyValueStoreReadOnlyDecoratorWithHeaders.class),
            Arguments.of(TimestampedKeyValueStore.class, TimestampedKeyValueStoreReadOnlyDecorator.class),
            Arguments.of(VersionedKeyValueStore.class, VersionedKeyValueStoreReadOnlyDecorator.class),
            Arguments.of(KeyValueStore.class, KeyValueStoreReadOnlyDecorator.class),
            Arguments.of(TimestampedWindowStore.class, TimestampedWindowStoreReadOnlyDecorator.class),
            Arguments.of(WindowStore.class, WindowStoreReadOnlyDecorator.class),
            Arguments.of(SessionStore.class, SessionStoreReadOnlyDecorator.class)
        );
    }

    @ParameterizedTest(name = "{0} -> {1}")
    @MethodSource("storeTypes")
    public void shouldWrapWithMatchingDecorator(final Class<? extends StateStore> storeType,
                                                final Class<? extends StateStore> expectedDecorator) {
        assertEquals(expectedDecorator, getReadOnlyStore(mock(storeType)).getClass());
    }

    // Pins current behavior: unlike wrapWithReadWriteStore, getReadOnlyStore has no branch for the
    // window/session headers stores, so a global store of either type falls through to the base
    // decorator, which does not implement the headers interface.
    private static Stream<Arguments> headersStoreTypesWithoutReadOnlyDecorator() {
        return Stream.of(
            Arguments.of(TimestampedWindowStoreWithHeaders.class, WindowStoreReadOnlyDecorator.class),
            Arguments.of(SessionStoreWithHeaders.class, SessionStoreReadOnlyDecorator.class)
        );
    }

    @ParameterizedTest(name = "{0} -> {1}")
    @MethodSource("headersStoreTypesWithoutReadOnlyDecorator")
    public void shouldFallBackToBaseDecoratorForHeadersStore(final Class<? extends StateStore> storeType,
                                                             final Class<? extends StateStore> expectedDecorator) {
        final StateStore decorated = getReadOnlyStore(mock(storeType));
        assertEquals(expectedDecorator, decorated.getClass());
        assertFalse(storeType.isInstance(decorated));
    }

    @Test
    public void shouldReturnUnknownStoreTypeUnwrapped() {
        final StateStore store = mock(StateStore.class);
        assertSame(store, getReadOnlyStore(store));
    }

    // flush/init/close and the KeyValueStore writes are covered by ProcessorContextImplTest's
    // global*StoreShouldBeReadOnly tests; commit is not, and is shared by every decorator.
    @Test
    public void shouldThrowOnCommit() {
        final StateStore store = getReadOnlyStore(mock(KeyValueStore.class));
        final UnsupportedOperationException e = assertThrows(UnsupportedOperationException.class,
            () -> store.commit(null));
        assertEquals(AbstractReadOnlyDecorator.ERROR_MESSAGE, e.getMessage());
    }
}
