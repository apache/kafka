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
import org.apache.kafka.streams.kstream.internals.TimeWindow;
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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@RunWith(EasyMockRunner.class)
public class ReadOnlySessionStoreFacadeTest {
    @Mock
    private TimestampedSessionStore<String, String> mockedSessionTimestampStore;
    @Mock
    private KeyValueIterator<Windowed<String>, ValueAndTimestamp<String>> mockedKeyValueSessionTimestampIterator;

    private ReadOnlySessionStoreFacade<String, String> readOnlySessionStoreFacade;

    @Before
    public void setup() {
        readOnlySessionStoreFacade = new ReadOnlySessionStoreFacade<>(mockedSessionTimestampStore);
    }

    @Test
    public void shouldReturnPlainKeyValuePairsOnFetchKeyParameters() {
        expect(mockedKeyValueSessionTimestampIterator.next())
            .andReturn(KeyValue.pair(
                new Windowed<>("key1", new TimeWindow(21L, 22L)),
                ValueAndTimestamp.make("value1", 22L)));
        expect(mockedSessionTimestampStore.fetch("key1"))
            .andReturn(mockedKeyValueSessionTimestampIterator);
        replay(mockedKeyValueSessionTimestampIterator, mockedSessionTimestampStore);

        final KeyValueIterator<Windowed<String>, String> iterator =
            readOnlySessionStoreFacade.fetch("key1");

        assertThat(iterator.next(), is(KeyValue.pair(new Windowed<>("key1", new TimeWindow(21L, 22L)), "value1")));
        verify(mockedKeyValueSessionTimestampIterator, mockedSessionTimestampStore);
    }

    @Test
    public void shouldReturnPlainKeyValuePairsOnFetchFromToParameters() {
        expect(mockedKeyValueSessionTimestampIterator.next())
            .andReturn(KeyValue.pair(
                new Windowed<>("key1", new TimeWindow(21L, 22L)),
                ValueAndTimestamp.make("value1", 22L)))
            .andReturn(KeyValue.pair(
                new Windowed<>("key2", new TimeWindow(42L, 43L)),
                ValueAndTimestamp.make("value2", 100L)));

        expect(mockedSessionTimestampStore.fetch("key1", "key2"))
            .andReturn(mockedKeyValueSessionTimestampIterator);
        replay(mockedKeyValueSessionTimestampIterator, mockedSessionTimestampStore);

        final KeyValueIterator<Windowed<String>, String> iterator =
            readOnlySessionStoreFacade.fetch("key1", "key2");

        assertThat(iterator.next(), is(KeyValue.pair(new Windowed<>("key1", new TimeWindow(21L, 22L)), "value1")));
        assertThat(iterator.next(), is(KeyValue.pair(new Windowed<>("key2", new TimeWindow(42L, 43L)), "value2")));
        verify(mockedKeyValueSessionTimestampIterator, mockedSessionTimestampStore);
    }
}
