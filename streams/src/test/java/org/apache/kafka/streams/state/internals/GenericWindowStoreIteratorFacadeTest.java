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
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStoreIterator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.function.Function;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class GenericWindowStoreIteratorFacadeTest {

    @Mock
    private KeyValueIterator<Long, ValueAndTimestamp<String>> mockedInnerIterator;

    private WindowStoreIterator<String> facade;

    @BeforeEach
    public void setup() {
        final Function<ValueAndTimestamp<String>, String> converter = ValueConverters.extractValue();
        facade = new GenericWindowStoreIteratorFacade<>(mockedInnerIterator, converter);
    }

    @Test
    public void shouldConvertValues() {
        when(mockedInnerIterator.next())
            .thenReturn(KeyValue.pair(100L, ValueAndTimestamp.make("value1", 42L)))
            .thenReturn(KeyValue.pair(200L, ValueAndTimestamp.make("value2", 84L)));

        assertThat(facade.next(), is(KeyValue.pair(100L, "value1")));
        assertThat(facade.next(), is(KeyValue.pair(200L, "value2")));
    }

    @Test
    public void shouldHandleNullValues() {
        when(mockedInnerIterator.next())
            .thenReturn(KeyValue.pair(100L, null))
            .thenReturn(KeyValue.pair(200L, ValueAndTimestamp.make("value2", 42L)));

        assertThat(facade.next(), is(KeyValue.pair(100L, null)));
        assertThat(facade.next(), is(KeyValue.pair(200L, "value2")));
    }

    @Test
    public void shouldDelegateHasNext() {
        when(mockedInnerIterator.hasNext()).thenReturn(true, false);

        assertTrue(facade.hasNext());
        assertFalse(facade.hasNext());
    }

    @Test
    public void shouldDelegatePeekNextKey() {
        when(mockedInnerIterator.peekNextKey()).thenReturn(100L, null);

        assertThat(facade.peekNextKey(), is(100L));
        assertNull(facade.peekNextKey());
    }

    @Test
    public void shouldDelegateClose() {
        facade.close();
        verify(mockedInnerIterator).close();
    }
}