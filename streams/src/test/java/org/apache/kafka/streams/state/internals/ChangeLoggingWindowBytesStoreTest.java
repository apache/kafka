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

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.WindowStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static java.time.Instant.ofEpochMilli;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class ChangeLoggingWindowBytesStoreTest {

    private final byte[] value = {0};
    private final Bytes bytesKey = Bytes.wrap(value);

    @Mock
    private WindowStore<Bytes, byte[]> inner;
    @Mock
    private ProcessorContextImpl context;
    private ChangeLoggingWindowBytesStore store;

    private final static Position POSITION = Position.fromMap(mkMap(mkEntry("", mkMap(mkEntry(0, 1L)))));

    @Before
    public void setUp() {
        store = new ChangeLoggingWindowBytesStore(inner, false, WindowKeySchema::toStoreKeyBinary);
        store.init((StateStoreContext) context, store);
    }

    @After
    public void tearDown() {
        verify(inner).init((StateStoreContext) context, store);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldDelegateDeprecatedInit() {
        store.init((ProcessorContext) context, store);

        verify(inner).init((ProcessorContext) context, store);
    }

    @Test
    public void shouldDelegateInit() {
        // testing the combination of setUp and tearDown
    }

    @Test
    public void shouldLogPuts() {
        final Bytes key = WindowKeySchema.toStoreKeyBinary(bytesKey, 0, 0);
        when(inner.getPosition()).thenReturn(Position.emptyPosition());

        store.put(bytesKey, value, context.timestamp());

        verify(inner).put(bytesKey, value, 0);
        verify(context).logChange(store.name(), key, value, 0L, Position.emptyPosition());
    }

    @Test
    public void shouldLogPutsWithPosition() {
        final Bytes key = WindowKeySchema.toStoreKeyBinary(bytesKey, 0, 0);
        when(inner.getPosition()).thenReturn(POSITION);

        store.put(bytesKey, value, context.timestamp());

        verify(inner).put(bytesKey, value, 0);
        verify(context).logChange(store.name(), key, value, 0L, POSITION);
    }

    @Test
    public void shouldDelegateToUnderlyingStoreWhenFetching() {
        store.fetch(bytesKey, ofEpochMilli(0), ofEpochMilli(10));

        verify(inner).fetch(bytesKey, 0, 10);
    }

    @Test
    public void shouldDelegateToUnderlyingStoreWhenBackwardFetching() {
        store.backwardFetch(bytesKey, ofEpochMilli(0), ofEpochMilli(10));

        verify(inner).backwardFetch(bytesKey, 0, 10);
    }

    @Test
    public void shouldDelegateToUnderlyingStoreWhenFetchingRange() {
        store.fetch(bytesKey, bytesKey, ofEpochMilli(0), ofEpochMilli(1));

        verify(inner).fetch(bytesKey, bytesKey, 0, 1);
    }

    @Test
    public void shouldDelegateToUnderlyingStoreWhenBackwardFetchingRange() {
        store.backwardFetch(bytesKey, bytesKey, ofEpochMilli(0), ofEpochMilli(1));

        verify(inner).backwardFetch(bytesKey, bytesKey, 0, 1);
    }

    @Test
    public void shouldRetainDuplicatesWhenSet() {
        store = new ChangeLoggingWindowBytesStore(inner, true, WindowKeySchema::toStoreKeyBinary);
        store.init((StateStoreContext) context, store);
        when(inner.getPosition()).thenReturn(Position.emptyPosition());

        final Bytes key1 = WindowKeySchema.toStoreKeyBinary(bytesKey, 0, 1);
        final Bytes key2 = WindowKeySchema.toStoreKeyBinary(bytesKey, 0, 2);

        store.put(bytesKey, value, context.timestamp());
        store.put(bytesKey, value, context.timestamp());

        verify(inner, times(2)).put(bytesKey, value, 0);
        verify(context).logChange(store.name(), key1, value, 0L, Position.emptyPosition());
        verify(context).logChange(store.name(), key2, value, 0L, Position.emptyPosition());
    }

}
