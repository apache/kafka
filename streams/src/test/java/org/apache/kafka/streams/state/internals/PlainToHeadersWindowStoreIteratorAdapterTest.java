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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class PlainToHeadersWindowStoreIteratorAdapterTest {

    private static final long WINDOW_START = 1000L;
    private static final byte[] PLAIN_VALUE = "value".getBytes();
    private static final byte[] VALUE_WITH_HEADERS = convertFromPlainToHeaderFormat(PLAIN_VALUE);

    @Mock
    private WindowStoreIterator<byte[]> innerIterator;

    @Test
    public void shouldImplementWindowStoreIterator() {
        final PlainToHeadersWindowStoreIteratorAdapter adapter =
            new PlainToHeadersWindowStoreIteratorAdapter(innerIterator);

        assertInstanceOf(WindowStoreIterator.class, adapter);
    }

    @Test
    public void shouldConvertPlainValueToHeaderFormatOnNext() {
        when(innerIterator.hasNext()).thenReturn(true);
        when(innerIterator.next())
            .thenReturn(KeyValue.pair(WINDOW_START, PLAIN_VALUE));

        final PlainToHeadersWindowStoreIteratorAdapter adapter =
            new PlainToHeadersWindowStoreIteratorAdapter(innerIterator);

        assertTrue(adapter.hasNext());
        final KeyValue<Long, byte[]> result = adapter.next();
        assertEquals(WINDOW_START, result.key);
        assertArrayEquals(VALUE_WITH_HEADERS, result.value);
    }

    @Test
    public void shouldHandleNullValueOnNext() {
        when(innerIterator.next())
            .thenReturn(KeyValue.pair(WINDOW_START, null));

        final PlainToHeadersWindowStoreIteratorAdapter adapter =
            new PlainToHeadersWindowStoreIteratorAdapter(innerIterator);

        final KeyValue<Long, byte[]> result = adapter.next();
        assertEquals(WINDOW_START, result.key);
        assertNull(result.value);
    }

    @Test
    public void shouldDelegateHasNext() {
        when(innerIterator.hasNext()).thenReturn(false);

        final PlainToHeadersWindowStoreIteratorAdapter adapter =
            new PlainToHeadersWindowStoreIteratorAdapter(innerIterator);

        assertFalse(adapter.hasNext());
    }

    @Test
    public void shouldDelegatePeekNextKey() {
        when(innerIterator.peekNextKey()).thenReturn(WINDOW_START);

        final PlainToHeadersWindowStoreIteratorAdapter adapter =
            new PlainToHeadersWindowStoreIteratorAdapter(innerIterator);

        assertEquals(WINDOW_START, adapter.peekNextKey());
    }

    @Test
    public void shouldDelegateClose() {
        final PlainToHeadersWindowStoreIteratorAdapter adapter =
            new PlainToHeadersWindowStoreIteratorAdapter(innerIterator);

        adapter.close();
        verify(innerIterator).close();
    }
}
