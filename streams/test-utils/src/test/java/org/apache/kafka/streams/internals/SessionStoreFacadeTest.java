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
package org.apache.kafka.streams.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedSessionStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

@RunWith(EasyMockRunner.class)
public class SessionStoreFacadeTest {
    @Mock
    private TimestampedSessionStore<String, String> mockedSessionTimestampStore;
    @Mock
    private KeyValueIterator<Windowed<String>, ValueAndTimestamp<String>> mockedKeyValueWindowTimestampIterator;

    private SessionStoreFacade<String, String> sessionStoreFacade;

    @Before
    public void setup() {
        sessionStoreFacade = new SessionStoreFacade<>(mockedSessionTimestampStore);
    }

    @Test
    public void shouldForwardInit() {
        final ProcessorContext context = mock(ProcessorContext.class);
        final StateStore store = mock(StateStore.class);
        mockedSessionTimestampStore.init(context, store);
        expectLastCall();
        replay(mockedSessionTimestampStore);

        sessionStoreFacade.init(context, store);
        verify(mockedSessionTimestampStore);
    }

    @Test
    public void shouldPutWithUnknownTimestamp() {
        mockedSessionTimestampStore.put(
            new Windowed<>("key", new SessionWindow(21L, 42L)),
            ValueAndTimestamp.make("value", ConsumerRecord.NO_TIMESTAMP));
        expectLastCall();
        replay(mockedSessionTimestampStore);

        sessionStoreFacade.put(new Windowed<>("key", new SessionWindow(21L, 42L)), "value");
        verify(mockedSessionTimestampStore);
    }

    @Test
    public void shouldRemoveSession() {
        mockedSessionTimestampStore.remove(new Windowed<>("key", new SessionWindow(21L, 42L)));
        expectLastCall();
        replay(mockedSessionTimestampStore);

        sessionStoreFacade.remove(new Windowed<>("key", new SessionWindow(21L, 42L)));
        verify(mockedSessionTimestampStore);
    }

    @Test
    public void shouldReturnPlainValueOnFindSession() {
        expect(mockedKeyValueWindowTimestampIterator.next())
            .andReturn(KeyValue.pair(
                new Windowed<>("key", new SessionWindow(21L, 42L)),
                ValueAndTimestamp.make("value1", 22L)))
            .andReturn(KeyValue.pair(
                new Windowed<>("key", new SessionWindow(100L, 200L)),
                ValueAndTimestamp.make("value2", 150L)));
        expect(mockedSessionTimestampStore.findSessions("key", 21L, 200L))
            .andReturn(mockedKeyValueWindowTimestampIterator);
        replay(mockedKeyValueWindowTimestampIterator, mockedSessionTimestampStore);

        final KeyValueIterator<Windowed<String>, String> iterator =
            sessionStoreFacade.findSessions("key", 21L, 200L);
        assertThat(iterator.next(), is(KeyValue.pair(new Windowed<>("key", new SessionWindow(21L, 42L)), "value1")));
        assertThat(iterator.next(), is(KeyValue.pair(new Windowed<>("key", new SessionWindow(100L, 200L)), "value2")));

        verify(mockedKeyValueWindowTimestampIterator, mockedSessionTimestampStore);
    }

    @Test
    public void shouldReturnPlainValueOnFindSessionRange() {
        expect(mockedKeyValueWindowTimestampIterator.next())
            .andReturn(KeyValue.pair(
                new Windowed<>("key1", new SessionWindow(21L, 42L)),
                ValueAndTimestamp.make("value1", 22L)))
            .andReturn(KeyValue.pair(
                new Windowed<>("key2", new SessionWindow(100L, 200L)),
                ValueAndTimestamp.make("value2", 150L)));
        expect(mockedSessionTimestampStore.findSessions("key1", "key2", 21L, 200L))
            .andReturn(mockedKeyValueWindowTimestampIterator);
        replay(mockedKeyValueWindowTimestampIterator, mockedSessionTimestampStore);

        final KeyValueIterator<Windowed<String>, String> iterator =
            sessionStoreFacade.findSessions("key1", "key2", 21L, 200L);
        assertThat(iterator.next(), is(KeyValue.pair(new Windowed<>("key1", new SessionWindow(21L, 42L)), "value1")));
        assertThat(iterator.next(), is(KeyValue.pair(new Windowed<>("key2", new SessionWindow(100L, 200L)), "value2")));

        verify(mockedKeyValueWindowTimestampIterator, mockedSessionTimestampStore);
    }

    @Test
    public void shouldForwardFlush() {
        mockedSessionTimestampStore.flush();
        expectLastCall();
        replay(mockedSessionTimestampStore);

        sessionStoreFacade.flush();
        verify(mockedSessionTimestampStore);
    }

    @Test
    public void shouldForwardClose() {
        mockedSessionTimestampStore.close();
        expectLastCall();
        replay(mockedSessionTimestampStore);

        sessionStoreFacade.close();
        verify(mockedSessionTimestampStore);
    }

    @Test
    public void shouldReturnName() {
        expect(mockedSessionTimestampStore.name()).andReturn("name");
        replay(mockedSessionTimestampStore);

        assertThat(sessionStoreFacade.name(), is("name"));
        verify(mockedSessionTimestampStore);
    }

    @Test
    public void shouldReturnIsPersistent() {
        expect(mockedSessionTimestampStore.persistent())
            .andReturn(true)
            .andReturn(false);
        replay(mockedSessionTimestampStore);

        assertThat(sessionStoreFacade.persistent(), is(true));
        assertThat(sessionStoreFacade.persistent(), is(false));
        verify(mockedSessionTimestampStore);
    }

    @Test
    public void shouldReturnIsOpen() {
        expect(mockedSessionTimestampStore.isOpen())
            .andReturn(true)
            .andReturn(false);
        replay(mockedSessionTimestampStore);

        assertThat(sessionStoreFacade.isOpen(), is(true));
        assertThat(sessionStoreFacade.isOpen(), is(false));
        verify(mockedSessionTimestampStore);
    }
}
