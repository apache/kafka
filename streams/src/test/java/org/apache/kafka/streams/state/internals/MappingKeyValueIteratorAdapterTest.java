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
import org.apache.kafka.streams.state.WindowStoreIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static org.apache.kafka.streams.state.HeadersBytesStore.convertFromPlainToHeaderFormat;
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
public class MappingKeyValueIteratorAdapterTest {

    private static final Bytes KEY = Bytes.wrap("key".getBytes());
    private static final byte[] RAW_VALUE = "value".getBytes();
    private static final long TIMESTAMP = 42L;
    private static final Windowed<Bytes> SESSION_KEY =
        new Windowed<>(KEY, new SessionWindow(10L, 20L));

    @Mock
    private KeyValueIterator<Bytes, byte[]> inner;

    @Mock
    private KeyValueIterator<Windowed<Bytes>, byte[]> sessionInner;

    @Mock
    private KeyValueIterator<Long, byte[]> windowInner;

    @Test
    public void plainToHeadersShouldConvertValueOnNext() {
        when(inner.hasNext()).thenReturn(true);
        when(inner.next()).thenReturn(KeyValue.pair(KEY, RAW_VALUE));

        final KeyValueIterator<Bytes, byte[]> adapter =
            MappingKeyValueIteratorAdapter.plainToHeaders(inner);

        assertTrue(adapter.hasNext());
        final KeyValue<Bytes, byte[]> result = adapter.next();
        assertEquals(KEY, result.key);
        assertArrayEquals(convertFromPlainToHeaderFormat(RAW_VALUE), result.value);
    }

    @Test
    public void timestampedToHeadersShouldConvertValueOnNext() {
        when(inner.hasNext()).thenReturn(true);
        when(inner.next()).thenReturn(KeyValue.pair(KEY, RAW_VALUE));

        final KeyValueIterator<Bytes, byte[]> adapter =
            MappingKeyValueIteratorAdapter.timestampedToHeaders(inner);

        assertTrue(adapter.hasNext());
        final KeyValue<Bytes, byte[]> result = adapter.next();
        assertEquals(KEY, result.key);
        assertArrayEquals(convertToHeaderFormat(RAW_VALUE), result.value);
    }

    @Test
    public void timestampedToHeadersShouldConvertValueForSessionKeys() {
        when(sessionInner.hasNext()).thenReturn(true);
        when(sessionInner.next()).thenReturn(KeyValue.pair(SESSION_KEY, RAW_VALUE));

        final KeyValueIterator<Windowed<Bytes>, byte[]> adapter =
            MappingKeyValueIteratorAdapter.timestampedToHeaders(sessionInner);

        assertTrue(adapter.hasNext());
        final KeyValue<Windowed<Bytes>, byte[]> result = adapter.next();
        assertEquals(SESSION_KEY, result.key);
        assertArrayEquals(convertToHeaderFormat(RAW_VALUE), result.value);
    }

    @Test
    public void plainToHeadersWindowShouldConvertValueOnNext() {
        when(windowInner.hasNext()).thenReturn(true);
        when(windowInner.next()).thenReturn(KeyValue.pair(TIMESTAMP, RAW_VALUE));

        final WindowStoreIterator<byte[]> adapter =
            MappingKeyValueIteratorAdapter.plainToHeadersWindow(windowInner);

        assertTrue(adapter.hasNext());
        final KeyValue<Long, byte[]> result = adapter.next();
        assertEquals(TIMESTAMP, result.key);
        assertArrayEquals(convertFromPlainToHeaderFormat(RAW_VALUE), result.value);
    }

    @Test
    public void timestampedToHeadersWindowShouldConvertValueOnNext() {
        when(windowInner.hasNext()).thenReturn(true);
        when(windowInner.next()).thenReturn(KeyValue.pair(TIMESTAMP, RAW_VALUE));

        final WindowStoreIterator<byte[]> adapter =
            MappingKeyValueIteratorAdapter.timestampedToHeadersWindow(windowInner);

        assertTrue(adapter.hasNext());
        final KeyValue<Long, byte[]> result = adapter.next();
        assertEquals(TIMESTAMP, result.key);
        assertArrayEquals(convertToHeaderFormat(RAW_VALUE), result.value);
    }

    @Test
    public void shouldReturnNullWhenInnerReturnsNullEntry() {
        when(inner.next()).thenReturn(null);

        final KeyValueIterator<Bytes, byte[]> adapter =
            MappingKeyValueIteratorAdapter.plainToHeaders(inner);

        assertNull(adapter.next());
    }

    @Test
    public void shouldPassThroughNullValue() {
        when(inner.next()).thenReturn(KeyValue.pair(KEY, null));

        final KeyValueIterator<Bytes, byte[]> adapter =
            MappingKeyValueIteratorAdapter.plainToHeaders(inner);

        final KeyValue<Bytes, byte[]> result = adapter.next();
        assertEquals(KEY, result.key);
        assertNull(result.value);
    }

    @Test
    public void shouldDelegateHasNext() {
        when(inner.hasNext()).thenReturn(false);

        final KeyValueIterator<Bytes, byte[]> adapter =
            MappingKeyValueIteratorAdapter.plainToHeaders(inner);

        assertFalse(adapter.hasNext());
    }

    @Test
    public void shouldDelegatePeekNextKey() {
        when(inner.peekNextKey()).thenReturn(KEY);

        final KeyValueIterator<Bytes, byte[]> adapter =
            MappingKeyValueIteratorAdapter.plainToHeaders(inner);

        assertEquals(KEY, adapter.peekNextKey());
    }

    @Test
    public void shouldDelegateClose() {
        final KeyValueIterator<Bytes, byte[]> adapter =
            MappingKeyValueIteratorAdapter.plainToHeaders(inner);

        adapter.close();
        verify(inner).close();
    }
}
