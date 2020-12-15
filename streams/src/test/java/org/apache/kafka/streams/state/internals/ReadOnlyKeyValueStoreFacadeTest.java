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
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
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
import static org.junit.Assert.assertNull;

@RunWith(EasyMockRunner.class)
public class ReadOnlyKeyValueStoreFacadeTest {
    @Mock
    private TimestampedKeyValueStore<String, String> mockedKeyValueTimestampStore;
    @Mock
    private KeyValueIterator<String, ValueAndTimestamp<String>> mockedKeyValueTimestampIterator;

    private ReadOnlyKeyValueStoreFacade<String, String> readOnlyKeyValueStoreFacade;

    @Before
    public void setup() {
        readOnlyKeyValueStoreFacade = new ReadOnlyKeyValueStoreFacade<>(mockedKeyValueTimestampStore);
    }

    @Test
    public void shouldReturnPlainValueOnGet() {
        expect(mockedKeyValueTimestampStore.get("key"))
            .andReturn(ValueAndTimestamp.make("value", 42L));
        expect(mockedKeyValueTimestampStore.get("unknownKey"))
            .andReturn(null);
        replay(mockedKeyValueTimestampStore);

        assertThat(readOnlyKeyValueStoreFacade.get("key"), is("value"));
        assertNull(readOnlyKeyValueStoreFacade.get("unknownKey"));
        verify(mockedKeyValueTimestampStore);
    }

    @Test
    public void shouldReturnPlainKeyValuePairsForRangeIterator() {
        expect(mockedKeyValueTimestampIterator.next())
            .andReturn(KeyValue.pair("key1", ValueAndTimestamp.make("value1", 21L)))
            .andReturn(KeyValue.pair("key2", ValueAndTimestamp.make("value2", 42L)));
        expect(mockedKeyValueTimestampStore.range("key1", "key2")).andReturn(mockedKeyValueTimestampIterator);
        replay(mockedKeyValueTimestampIterator, mockedKeyValueTimestampStore);

        final KeyValueIterator<String, String> iterator = readOnlyKeyValueStoreFacade.range("key1", "key2");
        assertThat(iterator.next(), is(KeyValue.pair("key1", "value1")));
        assertThat(iterator.next(), is(KeyValue.pair("key2", "value2")));
        verify(mockedKeyValueTimestampIterator, mockedKeyValueTimestampStore);
    }

    @Test
    public void shouldReturnPlainKeyValuePairsForAllIterator() {
        expect(mockedKeyValueTimestampIterator.next())
            .andReturn(KeyValue.pair("key1", ValueAndTimestamp.make("value1", 21L)))
            .andReturn(KeyValue.pair("key2", ValueAndTimestamp.make("value2", 42L)));
        expect(mockedKeyValueTimestampStore.all()).andReturn(mockedKeyValueTimestampIterator);
        replay(mockedKeyValueTimestampIterator, mockedKeyValueTimestampStore);

        final KeyValueIterator<String, String> iterator = readOnlyKeyValueStoreFacade.all();
        assertThat(iterator.next(), is(KeyValue.pair("key1", "value1")));
        assertThat(iterator.next(), is(KeyValue.pair("key2", "value2")));
        verify(mockedKeyValueTimestampIterator, mockedKeyValueTimestampStore);
    }

    @Test
    public void shouldForwardApproximateNumEntries() {
        expect(mockedKeyValueTimestampStore.approximateNumEntries()).andReturn(42L);
        replay(mockedKeyValueTimestampStore);

        assertThat(readOnlyKeyValueStoreFacade.approximateNumEntries(), is(42L));
        verify(mockedKeyValueTimestampStore);
    }
}
