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
import org.apache.kafka.streams.TopologyTestDriver.WindowStoreFacade;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class WindowStoreFacadeTest {
    @SuppressWarnings("unchecked")
    private final TimestampedWindowStore<String, String> mockedWindowTimestampStore = mock(TimestampedWindowStore.class);

    private WindowStoreFacade<String, String> windowStoreFacade;

    @BeforeEach
    public void setup() {
        windowStoreFacade = new WindowStoreFacade<>(mockedWindowTimestampStore);
    }

    @Test
    public void shouldForwardInit() {
        final StateStoreContext context = mock(StateStoreContext.class);
        final StateStore store = mock(StateStore.class);

        windowStoreFacade.init(context, store);
        verify(mockedWindowTimestampStore)
            .init(context, store);
    }

    @Test
    public void shouldPutWindowStartTimestampWithUnknownTimestamp() {
        windowStoreFacade.put("key", "value", 21L);
        verify(mockedWindowTimestampStore)
            .put("key", ValueAndTimestamp.make("value", ConsumerRecord.NO_TIMESTAMP), 21L);
    }

    @Test
    public void shouldForwardFlush() {
        windowStoreFacade.flush();
        verify(mockedWindowTimestampStore).flush();
    }

    @Test
    public void shouldForwardClose() {
        windowStoreFacade.close();
        verify(mockedWindowTimestampStore).close();
    }

    @Test
    public void shouldReturnName() {
        when(mockedWindowTimestampStore.name()).thenReturn("name");

        assertThat(windowStoreFacade.name(), is("name"));
        verify(mockedWindowTimestampStore).name();
    }

    @Test
    public void shouldReturnIsPersistent() {
        when(mockedWindowTimestampStore.persistent())
            .thenReturn(true, false);

        assertThat(windowStoreFacade.persistent(), is(true));
        assertThat(windowStoreFacade.persistent(), is(false));
        verify(mockedWindowTimestampStore, times(2)).persistent();
    }

    @Test
    public void shouldReturnIsOpen() {
        when(mockedWindowTimestampStore.isOpen())
            .thenReturn(true, false);

        assertThat(windowStoreFacade.isOpen(), is(true));
        assertThat(windowStoreFacade.isOpen(), is(false));
        verify(mockedWindowTimestampStore, times(2)).isOpen();
    }

}
