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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.stream.Stream;

import static org.apache.kafka.streams.processor.internals.AbstractReadWriteDecorator.wrapWithReadWriteStore;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class AbstractReadWriteDecoratorTest {

    // Dispatch tests pin wrapWithReadWriteStore to the exact decorator per store type. Because the
    // *WithHeaders interfaces extend their base store interface, a reordered/removed instanceof check
    // would silently fall through to the base decorator; asserting the exact class catches that.
    private static Stream<Arguments> storeTypes() {
        return Stream.of(
            Arguments.of(TimestampedKeyValueStoreWithHeaders.class, TimestampedKeyValueStoreReadWriteDecoratorWithHeaders.class),
            Arguments.of(TimestampedKeyValueStore.class, TimestampedKeyValueStoreReadWriteDecorator.class),
            Arguments.of(VersionedKeyValueStore.class, VersionedKeyValueStoreReadWriteDecorator.class),
            Arguments.of(KeyValueStore.class, KeyValueStoreReadWriteDecorator.class),
            Arguments.of(TimestampedWindowStoreWithHeaders.class, TimestampedWindowStoreWithHeadersReadWriteDecorator.class),
            Arguments.of(TimestampedWindowStore.class, TimestampedWindowStoreReadWriteDecorator.class),
            Arguments.of(WindowStore.class, WindowStoreReadWriteDecorator.class),
            Arguments.of(SessionStoreWithHeaders.class, SessionStoreWithHeadersReadWriteDecorator.class),
            Arguments.of(SessionStore.class, SessionStoreReadWriteDecorator.class)
        );
    }

    @ParameterizedTest(name = "{0} -> {1}")
    @MethodSource("storeTypes")
    public void shouldWrapWithMatchingDecorator(final Class<? extends StateStore> storeType,
                                                final Class<? extends StateStore> expectedDecorator) {
        assertEquals(expectedDecorator, wrapWithReadWriteStore(mock(storeType)).getClass());
    }

    @Test
    public void shouldReturnUnknownStoreTypeUnwrapped() {
        final StateStore store = mock(StateStore.class);
        assertSame(store, wrapWithReadWriteStore(store));
    }

    // init/close are covered by ProcessorContextImplTest's local*StoreShouldNotAllowInitOrClose tests;
    // commit is not, and is shared by every decorator.
    @Test
    public void shouldThrowOnCommit() {
        final StateStore store = wrapWithReadWriteStore(mock(KeyValueStore.class));
        final UnsupportedOperationException e = assertThrows(UnsupportedOperationException.class,
            () -> store.commit(null));
        assertEquals(AbstractReadWriteDecorator.ERROR_MESSAGE, e.getMessage());
    }
}
