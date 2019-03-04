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
import org.apache.kafka.streams.state.internals.WrappedStateStore;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

public class TupleForwarderTest {

    @Test
    public void shouldSetFlushListenerOnWrappedStateStore() {
        setFlushListener(true);
        setFlushListener(false);
    }

    private void setFlushListener(final boolean sendOldValues) {
        final WrappedStateStore<StateStore, Object, Object> store = mock(WrappedStateStore.class);
        final ForwardingCacheFlushListener<Object, Object> flushListener = mock(ForwardingCacheFlushListener.class);

        expect(store.setFlushListener(flushListener, sendOldValues)).andReturn(false);
        replay(store);

        new TupleForwarder<>(store, null, flushListener, sendOldValues);

        verify(store);
    }

    @Test
    public void shouldForwardRecordsIfWrappedStateStoreDoesNotCache() {
        final WrappedStateStore<StateStore, String, String> store = mock(WrappedStateStore.class);
        final ProcessorContext context = mock(ProcessorContext.class);

        expect(store.setFlushListener(null, false)).andReturn(false);
        context.forward("key", new Change<>("value", "oldValue"));
        expectLastCall();
        replay(store, context);

        new TupleForwarder<>(store, context, null, false)
            .maybeForward("key", "value", "oldValue");

        verify(store, context);
    }

    @Test
    public void shouldNotForwardRecordsIfWrappedStateStoreDoesCache() {
        final WrappedStateStore<StateStore, String, String> store = mock(WrappedStateStore.class);
        final ProcessorContext context = mock(ProcessorContext.class);

        expect(store.setFlushListener(null, false)).andReturn(true);
        replay(store, context);

        new TupleForwarder<>(store, context, null, false)
            .maybeForward("key", "value", "oldValue");

        verify(store, context);
    }

}
