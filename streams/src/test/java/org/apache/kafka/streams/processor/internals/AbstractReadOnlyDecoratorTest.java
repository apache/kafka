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
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.AbstractReadOnlyDecorator.KeyValueStoreReadOnlyDecorator;
import org.apache.kafka.streams.processor.internals.AbstractReadOnlyDecorator.SessionStoreReadOnlyDecorator;
import org.apache.kafka.streams.processor.internals.AbstractReadOnlyDecorator.TimestampedKeyValueStoreReadOnlyDecorator;
import org.apache.kafka.streams.processor.internals.AbstractReadOnlyDecorator.TimestampedKeyValueStoreReadOnlyDecoratorWithHeaders;
import org.apache.kafka.streams.processor.internals.AbstractReadOnlyDecorator.TimestampedWindowStoreReadOnlyDecorator;
import org.apache.kafka.streams.processor.internals.AbstractReadOnlyDecorator.VersionedKeyValueStoreReadOnlyDecorator;
import org.apache.kafka.streams.processor.internals.AbstractReadOnlyDecorator.WindowStoreReadOnlyDecorator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.apache.kafka.streams.state.WindowStore;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Collections;

import static org.apache.kafka.streams.processor.internals.AbstractReadOnlyDecorator.getReadOnlyStore;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class AbstractReadOnlyDecoratorTest {

    // Dispatch tests pin getReadOnlyStore to the exact decorator per store type. Because the
    // *WithHeaders interfaces extend their base store interface, a reordered/removed instanceof check
    // would silently fall through to the base decorator; asserting the exact class catches that.

    @Test
    public void shouldWrapTimestampedKeyValueStoreWithHeaders() {
        assertEquals(TimestampedKeyValueStoreReadOnlyDecoratorWithHeaders.class,
            getReadOnlyStore(mock(TimestampedKeyValueStoreWithHeaders.class)).getClass());
    }

    @Test
    public void shouldWrapTimestampedKeyValueStore() {
        assertEquals(TimestampedKeyValueStoreReadOnlyDecorator.class,
            getReadOnlyStore(mock(TimestampedKeyValueStore.class)).getClass());
    }

    @Test
    public void shouldWrapVersionedKeyValueStore() {
        assertEquals(VersionedKeyValueStoreReadOnlyDecorator.class,
            getReadOnlyStore(mock(VersionedKeyValueStore.class)).getClass());
    }

    @Test
    public void shouldWrapKeyValueStore() {
        assertEquals(KeyValueStoreReadOnlyDecorator.class,
            getReadOnlyStore(mock(KeyValueStore.class)).getClass());
    }

    @Test
    public void shouldWrapTimestampedWindowStore() {
        assertEquals(TimestampedWindowStoreReadOnlyDecorator.class,
            getReadOnlyStore(mock(TimestampedWindowStore.class)).getClass());
    }

    @Test
    public void shouldWrapWindowStore() {
        assertEquals(WindowStoreReadOnlyDecorator.class,
            getReadOnlyStore(mock(WindowStore.class)).getClass());
    }

    @Test
    public void shouldWrapSessionStore() {
        assertEquals(SessionStoreReadOnlyDecorator.class,
            getReadOnlyStore(mock(SessionStore.class)).getClass());
    }

    @Test
    public void shouldReturnUnknownStoreTypeUnwrapped() {
        final StateStore store = mock(StateStore.class);
        assertSame(store, getReadOnlyStore(store));
    }

    // flush/init/commit/close are defined on the abstract parent and shared by every decorator, so
    // one representative subtype suffices to verify they are blocked on a read-only global store.
    @Test
    public void shouldThrowOnFlush() {
        final StateStore store = getReadOnlyStore(mock(KeyValueStore.class));
        final UnsupportedOperationException e = assertThrows(UnsupportedOperationException.class, store::flush);
        assertEquals(AbstractReadOnlyDecorator.ERROR_MESSAGE, e.getMessage());
    }

    @Test
    public void shouldThrowOnInit() {
        final StateStore store = getReadOnlyStore(mock(KeyValueStore.class));
        final UnsupportedOperationException e = assertThrows(UnsupportedOperationException.class,
            () -> store.init((StateStoreContext) null, null));
        assertEquals(AbstractReadOnlyDecorator.ERROR_MESSAGE, e.getMessage());
    }

    @Test
    public void shouldThrowOnCommit() {
        final StateStore store = getReadOnlyStore(mock(KeyValueStore.class));
        final UnsupportedOperationException e = assertThrows(UnsupportedOperationException.class,
            () -> store.commit(null));
        assertEquals(AbstractReadOnlyDecorator.ERROR_MESSAGE, e.getMessage());
    }

    @Test
    public void shouldThrowOnClose() {
        final StateStore store = getReadOnlyStore(mock(KeyValueStore.class));
        final UnsupportedOperationException e = assertThrows(UnsupportedOperationException.class, store::close);
        assertEquals(AbstractReadOnlyDecorator.ERROR_MESSAGE, e.getMessage());
    }

    // Read-only decorators must reject every write; the base KeyValueStore mutators are representative.
    @SuppressWarnings("unchecked")
    @Test
    public void shouldThrowOnKeyValueStoreWrites() {
        final KeyValueStore<Object, Object> store =
            (KeyValueStore<Object, Object>) getReadOnlyStore(mock(KeyValueStore.class));
        assertThrows(UnsupportedOperationException.class, () -> store.put("k", "v"));
        assertThrows(UnsupportedOperationException.class, () -> store.putIfAbsent("k", "v"));
        assertThrows(UnsupportedOperationException.class, () -> store.putAll(Collections.emptyList()));
        assertThrows(UnsupportedOperationException.class, () -> store.delete("k"));
    }
}
