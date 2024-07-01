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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class KeyValueIteratorFacadeTest {
    @Mock
    private KeyValueIterator<String, ValueAndTimestamp<String>> mockedKeyValueIterator;

    private KeyValueIteratorFacade<String, String> keyValueIteratorFacade;

    @BeforeEach
    public void setup() {
        keyValueIteratorFacade = new KeyValueIteratorFacade<>(mockedKeyValueIterator);
    }

    @Test
    public void shouldForwardHasNext() {
        when(mockedKeyValueIterator.hasNext()).thenReturn(true).thenReturn(false);

        assertTrue(keyValueIteratorFacade.hasNext());
        assertFalse(keyValueIteratorFacade.hasNext());
    }

    @Test
    public void shouldForwardPeekNextKey() {
        when(mockedKeyValueIterator.peekNextKey()).thenReturn("key");

        assertThat(keyValueIteratorFacade.peekNextKey(), is("key"));
    }

    @Test
    public void shouldReturnPlainKeyValuePairOnGet() {
        when(mockedKeyValueIterator.next()).thenReturn(
            new KeyValue<>("key", ValueAndTimestamp.make("value", 42L)));

        assertThat(keyValueIteratorFacade.next(), is(KeyValue.pair("key", "value")));
    }

    @Test
    public void shouldCloseInnerIterator() {
        doNothing().when(mockedKeyValueIterator).close();

        keyValueIteratorFacade.close();
    }
}
