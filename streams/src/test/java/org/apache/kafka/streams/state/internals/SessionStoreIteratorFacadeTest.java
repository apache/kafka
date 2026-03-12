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

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.AggregationWithHeaders;
import org.apache.kafka.streams.state.KeyValueIterator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class SessionStoreIteratorFacadeTest {
    @Mock
    private KeyValueIterator<String, AggregationWithHeaders<String>> mockedInnerIterator;

    private SessionStoreIteratorFacade<String, String> facade;

    @BeforeEach
    public void setup() {
        facade = new SessionStoreIteratorFacade<>(mockedInnerIterator);
    }

    @Test
    public void shouldForwardHasNext() {
        when(mockedInnerIterator.hasNext()).thenReturn(true);
        assertTrue(facade.hasNext());
    }

    @Test
    public void shouldForwardPeekNextKey() {
        when(mockedInnerIterator.peekNextKey()).thenReturn("key");
        assertThat(facade.peekNextKey(), is("key"));
    }

    @Test
    public void shouldReturnPlainKeyValuePairOnNext() {
        final AggregationWithHeaders<String> aggregation =
            AggregationWithHeaders.make("value", new RecordHeaders());
        when(mockedInnerIterator.next()).thenReturn(new KeyValue<>("key", aggregation));
        assertThat(facade.next(), is(KeyValue.pair("key", "value")));
    }

    @Test
    public void shouldReturnNullValueWhenAggregationWithHeadersIsNull() {
        when(mockedInnerIterator.next()).thenReturn(new KeyValue<>("key", null));
        final KeyValue<String, String> result = facade.next();
        assertThat(result.key, is("key"));
        assertThat(result.value, is(nullValue()));
    }

    @Test
    public void shouldReturnNullWhenInnerNextReturnsNull() {
        when(mockedInnerIterator.next()).thenReturn(null);
        assertThat(facade.next(), is(nullValue()));
    }

    @Test
    public void shouldCloseInnerIterator() {
        facade.close();
        verify(mockedInnerIterator).close();
    }
}
