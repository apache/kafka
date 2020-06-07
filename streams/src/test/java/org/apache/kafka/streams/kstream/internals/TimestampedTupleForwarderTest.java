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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.WrappedStateStore;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

public class TimestampedTupleForwarderTest {

    @Test
    public void shouldSetFlushListenerOnWrappedStateStore() {
        setFlushListener(true);
        setFlushListener(false);
    }

    private void setFlushListener(final boolean sendOldValues) {
        final WrappedStateStore<StateStore, Object, ValueAndTimestamp<Object>> store = mock(WrappedStateStore.class);
        final TimestampedCacheFlushListener<Object, Object> flushListener = mock(TimestampedCacheFlushListener.class);

        expect(store.setFlushListener(flushListener, sendOldValues)).andReturn(false);
        replay(store);

        new TimestampedTupleForwarder<>(store, null, flushListener, sendOldValues);

        verify(store);
    }

    @Test
    public void shouldForwardRecordsIfWrappedStateStoreDoesNotCache() {
        shouldForwardRecordsIfWrappedStateStoreDoesNotCache(false);
        shouldForwardRecordsIfWrappedStateStoreDoesNotCache(true);
    }

    private void shouldForwardRecordsIfWrappedStateStoreDoesNotCache(final boolean sendOldValues) {
        final WrappedStateStore<StateStore, String, String> store = mock(WrappedStateStore.class);
        final ProcessorContext context = mock(ProcessorContext.class);

        expect(store.setFlushListener(null, sendOldValues)).andReturn(false);
        if (sendOldValues) {
            context.forward("key1", new Change<>("newValue1",  "oldValue1"));
            context.forward("key2", new Change<>("newValue2",  "oldValue2"), To.all().withTimestamp(42L));
        } else {
            context.forward("key1", new Change<>("newValue1", null));
            context.forward("key2", new Change<>("newValue2", null), To.all().withTimestamp(42L));
        }
        expectLastCall();
        replay(store, context);

        final TimestampedTupleForwarder<String, String> forwarder =
            new TimestampedTupleForwarder<>(store, context, null, sendOldValues);
        forwarder.maybeForward("key1", "newValue1", "oldValue1");
        forwarder.maybeForward("key2", "newValue2", "oldValue2", 42L);

        verify(store, context);
    }

    @Test
    public void shouldNotForwardRecordsIfWrappedStateStoreDoesCache() {
        final WrappedStateStore<StateStore, String, String> store = mock(WrappedStateStore.class);
        final ProcessorContext context = mock(ProcessorContext.class);

        expect(store.setFlushListener(null, false)).andReturn(true);
        replay(store, context);

        final TimestampedTupleForwarder<String, String> forwarder =
            new TimestampedTupleForwarder<>(store, context, null, false);
        forwarder.maybeForward("key", "newValue", "oldValue");
        forwarder.maybeForward("key", "newValue", "oldValue", 42L);

        verify(store, context);
    }
}
