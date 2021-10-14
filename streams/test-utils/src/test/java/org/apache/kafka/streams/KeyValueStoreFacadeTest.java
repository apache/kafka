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
import org.apache.kafka.streams.TopologyTestDriver.KeyValueStoreFacade;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KeyValueStoreFacadeTest {
    @SuppressWarnings("unchecked")
    private final TimestampedKeyValueStore<String, String> mockedKeyValueTimestampStore = mock(TimestampedKeyValueStore.class);

    private KeyValueStoreFacade<String, String> keyValueStoreFacade;

    @BeforeEach
    public void setup() {
        keyValueStoreFacade = new KeyValueStoreFacade<>(mockedKeyValueTimestampStore);
    }

    @SuppressWarnings("deprecation") // test of deprecated method
    @Test
    public void shouldForwardDeprecatedInit() {
        final ProcessorContext context = mock(ProcessorContext.class);
        final StateStore store = mock(StateStore.class);

        keyValueStoreFacade.init(context, store);
        verify(mockedKeyValueTimestampStore).init(context, store);
    }

    @Test
    public void shouldForwardInit() {
        final StateStoreContext context = mock(StateStoreContext.class);
        final StateStore store = mock(StateStore.class);

        keyValueStoreFacade.init(context, store);
        verify(mockedKeyValueTimestampStore).init(context, store);
    }

    @Test
    public void shouldPutWithUnknownTimestamp() {
        keyValueStoreFacade.put("key", "value");
        verify(mockedKeyValueTimestampStore)
            .put("key", ValueAndTimestamp.make("value", ConsumerRecord.NO_TIMESTAMP));
    }

    @Test
    public void shouldPutIfAbsentWithUnknownTimestamp() {
        doReturn(null, ValueAndTimestamp.make("oldValue", 42L))
            .when(mockedKeyValueTimestampStore)
            .putIfAbsent("key", ValueAndTimestamp.make("value", ConsumerRecord.NO_TIMESTAMP));

        assertNull(keyValueStoreFacade.putIfAbsent("key", "value"));
        assertThat(keyValueStoreFacade.putIfAbsent("key", "value"), is("oldValue"));
        verify(mockedKeyValueTimestampStore, times(2))
            .putIfAbsent("key", ValueAndTimestamp.make("value", ConsumerRecord.NO_TIMESTAMP));
    }

    @Test
    public void shouldPutAllWithUnknownTimestamp() {
        keyValueStoreFacade.putAll(asList(
            KeyValue.pair("key1", "value1"),
            KeyValue.pair("key2", "value2")
        ));
        verify(mockedKeyValueTimestampStore)
            .put("key1", ValueAndTimestamp.make("value1", ConsumerRecord.NO_TIMESTAMP));
        verify(mockedKeyValueTimestampStore)
            .put("key2", ValueAndTimestamp.make("value2", ConsumerRecord.NO_TIMESTAMP));
    }

    @Test
    public void shouldDeleteAndReturnPlainValue() {
        doReturn(null, ValueAndTimestamp.make("oldValue", 42L))
            .when(mockedKeyValueTimestampStore).delete("key");

        assertNull(keyValueStoreFacade.delete("key"));
        assertThat(keyValueStoreFacade.delete("key"), is("oldValue"));
        verify(mockedKeyValueTimestampStore, times(2)).delete("key");
    }

    @Test
    public void shouldForwardFlush() {
        keyValueStoreFacade.flush();
        verify(mockedKeyValueTimestampStore).flush();
    }

    @Test
    public void shouldForwardClose() {
        keyValueStoreFacade.close();
        verify(mockedKeyValueTimestampStore).close();
    }

    @Test
    public void shouldReturnName() {
        when(mockedKeyValueTimestampStore.name()).thenReturn("name");

        assertThat(keyValueStoreFacade.name(), is("name"));
        verify(mockedKeyValueTimestampStore).name();
    }

    @Test
    public void shouldReturnIsPersistent() {
        when(mockedKeyValueTimestampStore.persistent())
            .thenReturn(true, false);

        assertThat(keyValueStoreFacade.persistent(), is(true));
        assertThat(keyValueStoreFacade.persistent(), is(false));
        verify(mockedKeyValueTimestampStore, times(2)).persistent();
    }

    @Test
    public void shouldReturnIsOpen() {
        when(mockedKeyValueTimestampStore.isOpen())
            .thenReturn(true, false);

        assertThat(keyValueStoreFacade.isOpen(), is(true));
        assertThat(keyValueStoreFacade.isOpen(), is(false));
        verify(mockedKeyValueTimestampStore, times(2)).isOpen();
    }
}
