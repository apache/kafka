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

import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.WrappedStateStore;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class TimestampedTupleForwarderTest {

    @Test
    public void shouldSetFlushListenerOnWrappedStateStore() {
        setFlushListener(true);
        setFlushListener(false);
    }

    private void setFlushListener(final boolean sendOldValues) {
        @SuppressWarnings("unchecked")
        final WrappedStateStore<StateStore, Object, ValueAndTimestamp<Object>> store = mock(WrappedStateStore.class);
        @SuppressWarnings("unchecked")
        final TimestampedCacheFlushListener<Object, Object> flushListener = mock(TimestampedCacheFlushListener.class);

        when(store.setFlushListener(flushListener, sendOldValues)).thenReturn(false);

        new TimestampedTupleForwarder<>(
            store,
            null,
            flushListener,
            sendOldValues
        );
    }

    @Test
    public void shouldForwardRecordsIfWrappedStateStoreDoesNotCache() {
        shouldForwardRecordsIfWrappedStateStoreDoesNotCache(false);
        shouldForwardRecordsIfWrappedStateStoreDoesNotCache(true);
    }

    private void shouldForwardRecordsIfWrappedStateStoreDoesNotCache(final boolean sendOldValues) {
        @SuppressWarnings("unchecked")
        final WrappedStateStore<StateStore, String, String> store = mock(WrappedStateStore.class);
        @SuppressWarnings("unchecked")
        final InternalProcessorContext<String, Change<String>> context = mock(InternalProcessorContext.class);

        when(store.setFlushListener(null, sendOldValues)).thenReturn(false);
        if (sendOldValues) {
            doNothing().when(context).forward(new Record<>("key1", new Change<>("newValue1",  "oldValue1", true), 0L));
            doNothing().when(context).forward(new Record<>("key2", new Change<>("newValue2",  "oldValue2", false), 42L));
        } else {
            doNothing().when(context).forward(new Record<>("key1", new Change<>("newValue1", null, true), 0L));
            doNothing().when(context).forward(new Record<>("key2", new Change<>("newValue2", null, false), 42L));
        }

        final TimestampedTupleForwarder<String, String> forwarder =
            new TimestampedTupleForwarder<>(
                store,
                context,
                null,
                sendOldValues
            );
        forwarder.maybeForward(new Record<>("key1", new Change<>("newValue1", "oldValue1", true), 0L));
        forwarder.maybeForward(new Record<>("key2", new Change<>("newValue2", "oldValue2", false), 42L));
    }

    @Test
    public void shouldNotForwardRecordsIfWrappedStateStoreDoesCache() {
        @SuppressWarnings("unchecked")
        final WrappedStateStore<StateStore, String, String> store = mock(WrappedStateStore.class);
        @SuppressWarnings("unchecked")
        final InternalProcessorContext<String, Change<String>> context = mock(InternalProcessorContext.class);

        when(store.setFlushListener(null, false)).thenReturn(true);

        final TimestampedTupleForwarder<String, String> forwarder =
            new TimestampedTupleForwarder<>(
                store,
                context,
                null,
                false
            );
        forwarder.maybeForward(new Record<>("key", new Change<>("newValue", "oldValue", true), 0L));
        forwarder.maybeForward(new Record<>("key", new Change<>("newValue", "oldValue", true), 42L));
    }
}
