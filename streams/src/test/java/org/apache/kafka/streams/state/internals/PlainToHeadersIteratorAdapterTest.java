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
import org.apache.kafka.streams.state.KeyValueIterator;

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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class PlainToHeadersIteratorAdapterTest {

    private static final Bytes KEY = Bytes.wrap("key".getBytes());
    private static final byte[] PLAIN_VALUE = "value".getBytes();
    private static final byte[] VALUE_WITH_EMPTY_HEADERS_AND_TS =
        convertFromPlainToHeaderFormat(PLAIN_VALUE);

    @Mock
    private KeyValueIterator<Bytes, byte[]> innerIterator;

    @Test
    public void shouldPrependEmptyHeadersAndSentinelTimestampOnNext() {
        when(innerIterator.hasNext()).thenReturn(true);
        when(innerIterator.next())
            .thenReturn(KeyValue.pair(KEY, PLAIN_VALUE));

        final PlainToHeadersIteratorAdapter<Bytes> adapter =
            new PlainToHeadersIteratorAdapter<>(innerIterator);

        assertTrue(adapter.hasNext());
        final KeyValue<Bytes, byte[]> result = adapter.next();
        assertEquals(KEY, result.key);
        assertArrayEquals(VALUE_WITH_EMPTY_HEADERS_AND_TS, result.value);
    }

    @Test
    public void shouldReturnNullValueWhenInnerValueIsNull() {
        when(innerIterator.next())
            .thenReturn(KeyValue.pair(KEY, null));

        final PlainToHeadersIteratorAdapter<Bytes> adapter =
            new PlainToHeadersIteratorAdapter<>(innerIterator);

        final KeyValue<Bytes, byte[]> result = adapter.next();
        assertEquals(KEY, result.key);
        assertNull(result.value);
    }

    @Test
    public void shouldReturnNullWhenInnerNextReturnsNull() {
        when(innerIterator.next()).thenReturn(null);

        final PlainToHeadersIteratorAdapter<Bytes> adapter =
            new PlainToHeadersIteratorAdapter<>(innerIterator);

        assertNull(adapter.next());
    }

    @Test
    public void shouldDelegateHasNext() {
        when(innerIterator.hasNext()).thenReturn(false);

        final PlainToHeadersIteratorAdapter<Bytes> adapter =
            new PlainToHeadersIteratorAdapter<>(innerIterator);

        assertFalse(adapter.hasNext());
    }

    @Test
    public void shouldDelegatePeekNextKey() {
        when(innerIterator.peekNextKey()).thenReturn(KEY);

        final PlainToHeadersIteratorAdapter<Bytes> adapter =
            new PlainToHeadersIteratorAdapter<>(innerIterator);

        assertEquals(KEY, adapter.peekNextKey());
    }

    @Test
    public void shouldDelegateClose() {
        final PlainToHeadersIteratorAdapter<Bytes> adapter =
            new PlainToHeadersIteratorAdapter<>(innerIterator);

        adapter.close();
        verify(innerIterator).close();
    }
}
