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

import org.junit.jupiter.api.BeforeEach;
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
public class TimestampedToHeadersIteratorAdapterTest {

    private static final String KEY = "key";
    private static final byte[] RAW_VALUE = "value".getBytes();
    private static final byte[] VALUE_WITH_EMPTY_HEADERS = convertToHeaderFormat(RAW_VALUE);

    @Mock
    private KeyValueIterator<String, byte[]> innerIterator;

    private TimestampedToHeadersIteratorAdapter<String> adapter;

    @BeforeEach
    public void setUp() {
        adapter = new TimestampedToHeadersIteratorAdapter<>(innerIterator);
    }

    @Test
    public void shouldConvertNextValueToHeaderFormat() {
        when(innerIterator.next()).thenReturn(KeyValue.pair(KEY, RAW_VALUE));

        final KeyValue<String, byte[]> result = adapter.next();

        assertArrayEquals(VALUE_WITH_EMPTY_HEADERS, result.value);
    }

    @Test
    public void shouldKeepKeyUnchangedOnNext() {
        when(innerIterator.next()).thenReturn(KeyValue.pair(KEY, RAW_VALUE));

        final KeyValue<String, byte[]> result = adapter.next();

        assertEquals(KEY, result.key);
    }

    @Test
    public void shouldReturnNullWhenInnerNextReturnsNull() {
        when(innerIterator.next()).thenReturn(null);

        assertNull(adapter.next());
    }

    @Test
    public void shouldConvertNullValueWithinKeyValueToNull() {
        when(innerIterator.next()).thenReturn(KeyValue.pair(KEY, null));

        final KeyValue<String, byte[]> result = adapter.next();

        assertEquals(KEY, result.key);
        assertNull(result.value);
    }

    @Test
    public void shouldDelegatePeekNextKey() {
        when(innerIterator.peekNextKey()).thenReturn(KEY);

        assertEquals(KEY, adapter.peekNextKey());
    }

    @Test
    public void shouldDelegateHasNextWhenTrue() {
        when(innerIterator.hasNext()).thenReturn(true);

        assertTrue(adapter.hasNext());
    }

    @Test
    public void shouldDelegateHasNextWhenFalse() {
        when(innerIterator.hasNext()).thenReturn(false);

        assertFalse(adapter.hasNext());
    }

    @Test
    public void shouldDelegateClose() {
        adapter.close();

        verify(innerIterator).close();
    }
}
