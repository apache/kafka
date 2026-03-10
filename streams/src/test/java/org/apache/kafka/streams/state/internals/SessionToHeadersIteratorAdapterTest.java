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

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.state.KeyValueIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static org.apache.kafka.streams.state.HeadersBytesStore.convertToHeaderFormat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class SessionToHeadersIteratorAdapterTest {

    private static final Bytes KEY = Bytes.wrap("key".getBytes());
    private static final byte[] RAW_VALUE = "value".getBytes();
    private static final byte[] VALUE_WITH_EMPTY_HEADERS = convertToHeaderFormat(RAW_VALUE);
    private static final Windowed<Bytes> SESSION_KEY =
        new Windowed<>(KEY, new SessionWindow(10L, 20L));

    @Mock
    private KeyValueIterator<Windowed<Bytes>, byte[]> innerIterator;

    @Test
    public void shouldAddHeadersOnNext() {
        when(innerIterator.hasNext()).thenReturn(true);
        when(innerIterator.next())
            .thenReturn(KeyValue.pair(SESSION_KEY, RAW_VALUE));

        final SessionToHeadersIteratorAdapter adapter =
            new SessionToHeadersIteratorAdapter(innerIterator);

        assertTrue(adapter.hasNext());
        final KeyValue<Windowed<Bytes>, byte[]> result = adapter.next();
        assertEquals(SESSION_KEY, result.key);
        assertArrayEquals(VALUE_WITH_EMPTY_HEADERS, result.value);
    }

    @Test
    public void shouldHandleNullValueOnNext() {
        when(innerIterator.next())
            .thenReturn(KeyValue.pair(SESSION_KEY, null));

        final SessionToHeadersIteratorAdapter adapter =
            new SessionToHeadersIteratorAdapter(innerIterator);

        final KeyValue<Windowed<Bytes>, byte[]> result = adapter.next();
        assertEquals(SESSION_KEY, result.key);
        assertNull(result.value);
    }

    @Test
    public void shouldDelegateHasNext() {
        when(innerIterator.hasNext()).thenReturn(false);

        final SessionToHeadersIteratorAdapter adapter =
            new SessionToHeadersIteratorAdapter(innerIterator);

        assertFalse(adapter.hasNext());
    }

    @Test
    public void shouldDelegatePeekNextKey() {
        when(innerIterator.peekNextKey()).thenReturn(SESSION_KEY);

        final SessionToHeadersIteratorAdapter adapter =
            new SessionToHeadersIteratorAdapter(innerIterator);

        assertEquals(SESSION_KEY, adapter.peekNextKey());
    }

    @Test
    public void shouldDelegateClose() {
        final SessionToHeadersIteratorAdapter adapter =
            new SessionToHeadersIteratorAdapter(innerIterator);

        adapter.close();
        verify(innerIterator).close();
    }
}
