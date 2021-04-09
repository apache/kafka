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

import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.api.Record;
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
        final TupleChangeCacheFlushListener<Object, Object> flushListener = mock(TupleChangeCacheFlushListener.class);

        expect(store.setFlushListener(flushListener, sendOldValues)).andReturn(false);
        replay(store);

        new TupleChangeForwarder<>(store, null, flushListener, sendOldValues);

        verify(store);
    }

    @Test
    public void shouldForwardRecordsIfWrappedStateStoreDoesNotCache() {
        shouldForwardRecordsIfWrappedStateStoreDoesNotCache(false);
        shouldForwardRecordsIfWrappedStateStoreDoesNotCache(true);
    }

    private void shouldForwardRecordsIfWrappedStateStoreDoesNotCache(final boolean sendOldValues) {
        final WrappedStateStore<StateStore, String, String> store = mock(WrappedStateStore.class);
        final ProcessorContext<String, Change<String>> context = mock(ProcessorContext.class);

        expect(store.setFlushListener(null, sendOldValues)).andReturn(false);
        if (sendOldValues) {
            final Record<String, Change<String>> record1 = new Record<>("key1", new Change<>("newValue1",  "oldValue1"), 0L);
            context.forward(record1);
            final Record<String, Change<String>> record2 = new Record<>("key2", new Change<>("newValue2",  "oldValue2"), 42L);
            context.forward(record2);
        } else {
            final Record<String, Change<String>> record1 = new Record<>("key1", new Change<>("newValue1",  null), 0L);
            context.forward(record1);
            final Record<String, Change<String>> record2 = new Record<>("key2", new Change<>("newValue2",  null), 42L);
            context.forward(record2);
        }
        expectLastCall();
        replay(store, context);

        final TupleChangeForwarder<String, String> forwarder =
            new TupleChangeForwarder<>(store, context, null, sendOldValues);
        final Record<String, String> record1 = new Record<>("key1", "newValue1", 0L);
        forwarder.maybeForward(record1, "newValue1", "oldValue1");
        final Record<String, String> record2 = new Record<>("key2", "newValue2", 0L);
        forwarder.maybeForward(record2, "newValue2", "oldValue2", 42L);

        verify(store, context);
    }

    @Test
    public void shouldNotForwardRecordsIfWrappedStateStoreDoesCache() {
        final WrappedStateStore<StateStore, String, String> store = mock(WrappedStateStore.class);
        final ProcessorContext<String, Change<String>> context = mock(ProcessorContext.class);

        expect(store.setFlushListener(null, false)).andReturn(true);
        replay(store, context);

        final TupleChangeForwarder<String, String> forwarder =
            new TupleChangeForwarder<>(store, context, null, false);
        final Record<String, Change<String>> record1 = new Record<>("key", null, 0L);
        forwarder.maybeForward(record1, "newValue", "oldValue");
        final Record<String, Change<String>> record2 = new Record<>("key", null, 0L);
        forwarder.maybeForward(record2, "newValue", "oldValue", 42L);

        verify(store, context);
    }
}
