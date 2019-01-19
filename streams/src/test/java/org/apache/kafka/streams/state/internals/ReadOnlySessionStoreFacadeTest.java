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
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedSessionStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

@RunWith(EasyMockRunner.class)
public class ReadOnlySessionStoreFacadeTest {
    @Mock
    private TimestampedSessionStore<String, String> mockedSessionTimestampStore;
    @Mock
    private KeyValueIterator<Windowed<String>, ValueAndTimestamp<String>> mockedSessionTimestampIterator;

    private ReadOnlySessionStoreFacade<String, String> readOnlySessionStoreFacade;

    @Before
    public void setup() {
        readOnlySessionStoreFacade = new ReadOnlySessionStoreFacade<>(mockedSessionTimestampStore);
    }

    @Test
    public void shouldReturnPlainKeyValuePairsOnSingleKeyFetch() {
        expect(mockedSessionTimestampIterator.next())
            .andReturn(KeyValue.pair(
                new Windowed<>("key1", new SessionWindow(21L, 42L)),
                ValueAndTimestamp.make("value1", 21L)))
            .andReturn(KeyValue.pair(
                new Windowed<>("key1", new SessionWindow(100L, 101L)),
                ValueAndTimestamp.make("value2", 22L)));
        expect(mockedSessionTimestampStore.fetch("key1")).andReturn(mockedSessionTimestampIterator);
        replay(mockedSessionTimestampIterator, mockedSessionTimestampStore);

        final KeyValueIterator<Windowed<String>, String> iterator = readOnlySessionStoreFacade.fetch("key1");
        assertThat(iterator.next(), is(KeyValue.pair(new Windowed<>("key1", new SessionWindow(21L, 42L)), "value1")));
        assertThat(iterator.next(), is(KeyValue.pair(new Windowed<>("key1", new SessionWindow(100L, 101L)), "value2")));
        verify(mockedSessionTimestampIterator, mockedSessionTimestampStore);
    }

    @Test
    public void shouldReturnPlainKeyValuePairsOnRangeFetch() {
        expect(mockedSessionTimestampIterator.next())
            .andReturn(KeyValue.pair(
                new Windowed<>("key1", new SessionWindow(21L, 42L)),
                ValueAndTimestamp.make("value1", 21L)))
            .andReturn(KeyValue.pair(
                new Windowed<>("key2", new SessionWindow(22L, 43L)),
                ValueAndTimestamp.make("value2", 43L)));
        expect(mockedSessionTimestampStore.fetch("key1", "key2")).andReturn(mockedSessionTimestampIterator);
        replay(mockedSessionTimestampIterator, mockedSessionTimestampStore);

        final KeyValueIterator<Windowed<String>, String> iterator = readOnlySessionStoreFacade.fetch("key1", "key2");
        assertThat(iterator.next(), is(KeyValue.pair(new Windowed<>("key1", new SessionWindow(21L, 42L)), "value1")));
        assertThat(iterator.next(), is(KeyValue.pair(new Windowed<>("key2", new SessionWindow(22L, 43L)), "value2")));
        verify(mockedSessionTimestampIterator, mockedSessionTimestampStore);
    }
}
