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
package org.apache.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.TopologyTestDriver.WindowStoreFacade;
import org.easymock.EasyMock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class WindowStoreFacadeTest {
    private final TimestampedWindowStore<String, String> mockedWindowTimestampStore = EasyMock.mock(TimestampedWindowStore.class);

    private WindowStoreFacade<String, String> windowStoreFacade;

    @BeforeEach
    public void setup() {
        windowStoreFacade = new WindowStoreFacade<>(mockedWindowTimestampStore);
    }

    @SuppressWarnings("deprecation") // test of deprecated method
    @Test
    public void shouldForwardDeprecatedInit() {
        final ProcessorContext context = mock(ProcessorContext.class);
        final StateStore store = mock(StateStore.class);
        mockedWindowTimestampStore.init(context, store);
        expectLastCall();
        replay(mockedWindowTimestampStore);

        windowStoreFacade.init(context, store);
        verify(mockedWindowTimestampStore);
    }

    @Test
    public void shouldForwardInit() {
        final StateStoreContext context = mock(StateStoreContext.class);
        final StateStore store = mock(StateStore.class);
        mockedWindowTimestampStore.init(context, store);
        expectLastCall();
        replay(mockedWindowTimestampStore);

        windowStoreFacade.init(context, store);
        verify(mockedWindowTimestampStore);
    }

    @Test
    public void shouldPutWindowStartTimestampWithUnknownTimestamp() {
        mockedWindowTimestampStore.put("key", ValueAndTimestamp.make("value", ConsumerRecord.NO_TIMESTAMP), 21L);
        expectLastCall();
        replay(mockedWindowTimestampStore);

        windowStoreFacade.put("key", "value", 21L);
        verify(mockedWindowTimestampStore);
    }

    @Test
    public void shouldForwardFlush() {
        mockedWindowTimestampStore.flush();
        expectLastCall();
        replay(mockedWindowTimestampStore);

        windowStoreFacade.flush();
        verify(mockedWindowTimestampStore);
    }

    @Test
    public void shouldForwardClose() {
        mockedWindowTimestampStore.close();
        expectLastCall();
        replay(mockedWindowTimestampStore);

        windowStoreFacade.close();
        verify(mockedWindowTimestampStore);
    }

    @Test
    public void shouldReturnName() {
        expect(mockedWindowTimestampStore.name()).andReturn("name");
        replay(mockedWindowTimestampStore);

        assertThat(windowStoreFacade.name(), is("name"));
        verify(mockedWindowTimestampStore);
    }

    @Test
    public void shouldReturnIsPersistent() {
        expect(mockedWindowTimestampStore.persistent())
            .andReturn(true)
            .andReturn(false);
        replay(mockedWindowTimestampStore);

        assertThat(windowStoreFacade.persistent(), is(true));
        assertThat(windowStoreFacade.persistent(), is(false));
        verify(mockedWindowTimestampStore);
    }

    @Test
    public void shouldReturnIsOpen() {
        expect(mockedWindowTimestampStore.isOpen())
            .andReturn(true)
            .andReturn(false);
        replay(mockedWindowTimestampStore);

        assertThat(windowStoreFacade.isOpen(), is(true));
        assertThat(windowStoreFacade.isOpen(), is(false));
        verify(mockedWindowTimestampStore);
    }

}
