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

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static java.time.Instant.ofEpochMilli;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class ChangeLoggingWindowBytesStoreTest {

    private final byte[] value = {0};
    private final Bytes bytesKey = Bytes.wrap(value);

    @Mock
    private WindowStore<Bytes, byte[]> inner;
    @Mock
    private ProcessorContextImpl context;
    private ChangeLoggingWindowBytesStore store;

    private static final Position POSITION = Position.fromMap(mkMap(mkEntry("", mkMap(mkEntry(0, 1L)))));

    @BeforeEach
    public void setUp() {
        store = new ChangeLoggingWindowBytesStore(inner, false, WindowKeySchema::toStoreKeyBinary);
        store.init(context, store);
    }

    @AfterEach
    public void tearDown() {
        verify(inner).init(context, store);
    }

    @Test
    public void shouldDelegateInit() {
        // testing the combination of setUp and tearDown
    }

    @Test
    public void shouldLogPuts() {
        final Bytes key = WindowKeySchema.toStoreKeyBinary(bytesKey, 0, 0);
        when(inner.getPosition()).thenReturn(Position.emptyPosition());
        when(context.recordContext()).thenReturn(new ProcessorRecordContext(0, 0, 0, "topic", new RecordHeaders()));

        store.put(bytesKey, value, context.recordContext().timestamp());

        verify(inner).put(bytesKey, value, 0);
        verify(context).logChange(store.name(), key, value, 0L, Position.emptyPosition());
    }

    @Test
    public void shouldLogPutsWithPosition() {
        final Bytes key = WindowKeySchema.toStoreKeyBinary(bytesKey, 0, 0);
        when(inner.getPosition()).thenReturn(POSITION);
        when(context.recordContext()).thenReturn(new ProcessorRecordContext(0, 0, 0, "topic", new RecordHeaders()));

        store.put(bytesKey, value, context.recordContext().timestamp());

        verify(inner).put(bytesKey, value, 0);
        verify(context).logChange(store.name(), key, value, 0L, POSITION);
    }

    @SuppressWarnings({"resource", "unused"})
    @Test
    public void shouldDelegateToUnderlyingStoreWhenFetching() {
        try (final WindowStoreIterator<byte[]> unused = store.fetch(bytesKey, ofEpochMilli(0), ofEpochMilli(10))) {
            verify(inner).fetch(bytesKey, 0, 10);
        }
    }

    @SuppressWarnings({"resource", "unused"})
    @Test
    public void shouldDelegateToUnderlyingStoreWhenBackwardFetching() {
        try (final WindowStoreIterator<byte[]> unused = store.backwardFetch(bytesKey, ofEpochMilli(0), ofEpochMilli(10))) {
            verify(inner).backwardFetch(bytesKey, 0, 10);
        }
    }

    @SuppressWarnings({"resource", "unused"})
    @Test
    public void shouldDelegateToUnderlyingStoreWhenFetchingRange() {
        try (final KeyValueIterator<Windowed<Bytes>, byte[]> unused = store.fetch(bytesKey, bytesKey, ofEpochMilli(0), ofEpochMilli(1))) {
            verify(inner).fetch(bytesKey, bytesKey, 0, 1);
        }
    }

    @SuppressWarnings({"resource", "unused"})
    @Test
    public void shouldDelegateToUnderlyingStoreWhenBackwardFetchingRange() {
        try (final KeyValueIterator<Windowed<Bytes>, byte[]> unused =  store.backwardFetch(bytesKey, bytesKey, ofEpochMilli(0), ofEpochMilli(1))) {
            verify(inner).backwardFetch(bytesKey, bytesKey, 0, 1);
        }
    }

    @Test
    public void shouldRetainDuplicatesWhenSet() {
        store = new ChangeLoggingWindowBytesStore(inner, true, WindowKeySchema::toStoreKeyBinary);
        store.init(context, store);
        when(inner.getPosition()).thenReturn(Position.emptyPosition());
        when(context.recordContext()).thenReturn(new ProcessorRecordContext(0, 0, 0, "topic", new RecordHeaders()));

        final Bytes key1 = WindowKeySchema.toStoreKeyBinary(bytesKey, 0, 1);
        final Bytes key2 = WindowKeySchema.toStoreKeyBinary(bytesKey, 0, 2);

        store.put(bytesKey, value, context.recordContext().timestamp());
        store.put(bytesKey, value, context.recordContext().timestamp());

        verify(inner, times(2)).put(bytesKey, value, 0);
        verify(context).logChange(store.name(), key1, value, 0L, Position.emptyPosition());
        verify(context).logChange(store.name(), key2, value, 0L, Position.emptyPosition());
    }

}
