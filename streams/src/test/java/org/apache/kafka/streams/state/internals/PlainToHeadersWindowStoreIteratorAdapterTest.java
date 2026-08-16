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

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStoreIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static org.apache.kafka.streams.state.HeadersBytesStore.convertFromPlainToHeaderFormat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class PlainToHeadersWindowStoreIteratorAdapterTest {

    private static final byte[] PLAIN_VALUE = "value".getBytes();
    private static final byte[] VALUE_WITH_EMPTY_HEADERS_AND_TS =
        convertFromPlainToHeaderFormat(PLAIN_VALUE);

    @Mock
    private WindowStoreIterator<byte[]> innerIterator;

    @Test
    public void shouldImplementWindowStoreIteratorInterface() {
        final PlainToHeadersWindowStoreIteratorAdapter adapter =
            new PlainToHeadersWindowStoreIteratorAdapter(innerIterator);

        assertInstanceOf(WindowStoreIterator.class, adapter);
        assertInstanceOf(KeyValueIterator.class, adapter);
        assertInstanceOf(PlainToHeadersIteratorAdapter.class, adapter);
    }

    @Test
    public void shouldPrependEmptyHeadersAndSentinelTimestampOnNext() {
        when(innerIterator.hasNext()).thenReturn(true);
        when(innerIterator.next()).thenReturn(KeyValue.pair(42L, PLAIN_VALUE));

        final PlainToHeadersWindowStoreIteratorAdapter adapter =
            new PlainToHeadersWindowStoreIteratorAdapter(innerIterator);

        assertTrue(adapter.hasNext());
        final KeyValue<Long, byte[]> result = adapter.next();
        assertEquals(42L, result.key.longValue());
        assertArrayEquals(VALUE_WITH_EMPTY_HEADERS_AND_TS, result.value);
    }

    @Test
    public void shouldReturnNullValueWhenInnerValueIsNull() {
        when(innerIterator.next()).thenReturn(KeyValue.pair(42L, null));

        final PlainToHeadersWindowStoreIteratorAdapter adapter =
            new PlainToHeadersWindowStoreIteratorAdapter(innerIterator);

        final KeyValue<Long, byte[]> result = adapter.next();
        assertEquals(42L, result.key.longValue());
        assertNull(result.value);
    }

    @Test
    public void shouldReturnNullWhenInnerKeyValueIsNull() {
        when(innerIterator.next()).thenReturn(null);

        final PlainToHeadersWindowStoreIteratorAdapter adapter =
            new PlainToHeadersWindowStoreIteratorAdapter(innerIterator);

        assertNull(adapter.next());
    }

    @Test
    public void shouldDelegatePeekNextKey() {
        when(innerIterator.peekNextKey()).thenReturn(100L);

        final PlainToHeadersWindowStoreIteratorAdapter adapter =
            new PlainToHeadersWindowStoreIteratorAdapter(innerIterator);

        assertEquals(100L, adapter.peekNextKey().longValue());
    }

    @Test
    public void shouldDelegateClose() {
        final PlainToHeadersWindowStoreIteratorAdapter adapter =
            new PlainToHeadersWindowStoreIteratorAdapter(innerIterator);

        adapter.close();
        verify(innerIterator).close();
    }
}
