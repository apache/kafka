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
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(EasyMockRunner.class)
public class KeyValueIteratorFacadeTest {
    @Mock
    private KeyValueIterator<String, ValueAndTimestamp<String>> mockedKeyValueIterator;

    private KeyValueIteratorFacade<String, String> keyValueIteratorFacade;

    @Before
    public void setup() {
        keyValueIteratorFacade = new KeyValueIteratorFacade<>(mockedKeyValueIterator);
    }

    @Test
    public void shouldForwardHasNext() {
        expect(mockedKeyValueIterator.hasNext()).andReturn(true).andReturn(false);
        replay(mockedKeyValueIterator);

        assertTrue(keyValueIteratorFacade.hasNext());
        assertFalse(keyValueIteratorFacade.hasNext());
        verify(mockedKeyValueIterator);
    }

    @Test
    public void shouldForwardPeekNextKey() {
        expect(mockedKeyValueIterator.peekNextKey()).andReturn("key");
        replay(mockedKeyValueIterator);

        assertThat(keyValueIteratorFacade.peekNextKey(), is("key"));
        verify(mockedKeyValueIterator);
    }

    @Test
    public void shouldReturnPlainKeyValuePairOnGet() {
        expect(mockedKeyValueIterator.next()).andReturn(
            new KeyValue<>("key", ValueAndTimestamp.make("value", 42L)));
        replay(mockedKeyValueIterator);

        assertThat(keyValueIteratorFacade.next(), is(KeyValue.pair("key", "value")));
        verify(mockedKeyValueIterator);
    }

    @Test
    public void shouldCloseInnerIterator() {
        mockedKeyValueIterator.close();
        expectLastCall();
        replay(mockedKeyValueIterator);

        keyValueIteratorFacade.close();
        verify(mockedKeyValueIterator);
    }
}
