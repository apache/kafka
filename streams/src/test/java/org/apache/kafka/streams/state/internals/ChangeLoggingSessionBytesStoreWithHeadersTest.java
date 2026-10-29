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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.AggregationWithHeaders;
import org.apache.kafka.streams.state.SessionStore;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class ChangeLoggingSessionBytesStoreWithHeadersTest {

    private static final String TOPIC = "topic";
    private static final Position POSITION = Position.fromMap(mkMap(mkEntry("", mkMap(mkEntry(0, 1L)))));

    @Mock
    private SessionStore<Bytes, byte[]> inner;
    @Mock
    private ProcessorContextImpl context;

    private ChangeLoggingSessionBytesStoreWithHeaders store;

    private final byte[] value1 = {0};
    private final Bytes bytesKey = Bytes.wrap(value1);
    private final Windowed<Bytes> key1 = new Windowed<>(bytesKey, new SessionWindow(0, 0));

    private final AggregationWithHeadersSerializer<byte[]> serializer =
        new AggregationWithHeadersSerializer<>(Serdes.ByteArray().serializer());

    @BeforeEach
    public void setUp() {
        store = new ChangeLoggingSessionBytesStoreWithHeaders(inner);
        store.init(context, store);
    }

    @AfterEach
    public void tearDown() {
        verify(inner).init(context, store);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldDelegateInit() {
        final SessionStore<Bytes, byte[]> innerMock = mock(SessionStore.class);
        final ChangeLoggingSessionBytesStoreWithHeaders outer =
            new ChangeLoggingSessionBytesStoreWithHeaders(innerMock);

        outer.init(context, outer);
        verify(innerMock).init(context, outer);
    }

    @Test
    public void shouldLogPutWithHeaders() {
        final RecordHeaders headers = new RecordHeaders();
        headers.add("key1", "value1".getBytes());
        final AggregationWithHeaders<byte[]> aggWithHeaders = AggregationWithHeaders.make(value1, headers);
        final byte[] serializedValue = serializer.serialize(TOPIC, aggWithHeaders);

        final Bytes binaryKey = SessionKeySchema.toBinary(key1);
        when(inner.getPosition()).thenReturn(Position.emptyPosition());
        when(context.recordContext()).thenReturn(new ProcessorRecordContext(0, 0, 0, TOPIC, new RecordHeaders()));

        store.put(key1, serializedValue);

        verify(inner).put(key1, serializedValue);
        verify(context).logChange(
            store.name(),
            binaryKey,
            value1,
            0L,
            headers,
            Position.emptyPosition()
        );
    }

    @Test
    public void shouldLogPutWithPosition() {
        final RecordHeaders headers = new RecordHeaders();
        headers.add("key1", "value1".getBytes());
        final AggregationWithHeaders<byte[]> aggWithHeaders = AggregationWithHeaders.make(value1, headers);
        final byte[] serializedValue = serializer.serialize(TOPIC, aggWithHeaders);

        final Bytes binaryKey = SessionKeySchema.toBinary(key1);
        when(inner.getPosition()).thenReturn(POSITION);
        when(context.recordContext()).thenReturn(new ProcessorRecordContext(0, 0, 0, TOPIC, new RecordHeaders()));

        store.put(key1, serializedValue);

        verify(inner).put(key1, serializedValue);
        verify(context).logChange(
            store.name(),
            binaryKey,
            value1,
            0L,
            headers,
            POSITION
        );
    }

    @Test
    public void shouldHandleNullValueInPut() {
        final RecordHeaders contextHeaders = new RecordHeaders();
        contextHeaders.add("headerKey", "headerValue".getBytes());

        final Bytes binaryKey = SessionKeySchema.toBinary(key1);
        when(inner.getPosition()).thenReturn(Position.emptyPosition());
        when(context.recordContext()).thenReturn(new ProcessorRecordContext(42L, 0, 0, TOPIC, contextHeaders));

        store.put(key1, null);

        verify(inner).put(key1, null);
        verify(context).logChange(
            store.name(),
            binaryKey,
            null,
            42L,
            contextHeaders,
            Position.emptyPosition()
        );
    }

    @Test
    public void shouldHandleEmptyHeaders() {
        final RecordHeaders emptyHeaders = new RecordHeaders();
        final AggregationWithHeaders<byte[]> aggWithHeaders = AggregationWithHeaders.make(value1, emptyHeaders);
        final byte[] serializedValue = serializer.serialize(TOPIC, aggWithHeaders);

        final Bytes binaryKey = SessionKeySchema.toBinary(key1);
        when(inner.getPosition()).thenReturn(Position.emptyPosition());
        when(context.recordContext()).thenReturn(new ProcessorRecordContext(0, 0, 0, TOPIC, new RecordHeaders()));

        store.put(key1, serializedValue);

        verify(inner).put(key1, serializedValue);
        verify(context).logChange(
            store.name(),
            binaryKey,
            value1,
            0L,
            emptyHeaders,
            Position.emptyPosition()
        );
    }

    @Test
    public void shouldLogRemoveWithRecordContextHeaders() {
        final RecordHeaders contextHeaders = new RecordHeaders();
        contextHeaders.add("contextKey", "contextValue".getBytes());

        final Bytes binaryKey = SessionKeySchema.toBinary(key1);
        when(inner.getPosition()).thenReturn(Position.emptyPosition());
        when(context.recordContext()).thenReturn(new ProcessorRecordContext(42L, 0, 0, TOPIC, contextHeaders));

        store.remove(key1);

        verify(inner).remove(key1);
        verify(context).logChange(
            store.name(),
            binaryKey,
            null,
            42L,
            contextHeaders,
            Position.emptyPosition()
        );
    }

    @Test
    public void shouldHandleMultipleHeadersInSingleRecord() {
        final RecordHeaders headers = new RecordHeaders();
        headers.add("header1", "value1".getBytes());
        headers.add("header2", "value2".getBytes());
        headers.add("header3", "value3".getBytes());
        final AggregationWithHeaders<byte[]> aggWithHeaders = AggregationWithHeaders.make(value1, headers);
        final byte[] serializedValue = serializer.serialize(TOPIC, aggWithHeaders);

        final Bytes binaryKey = SessionKeySchema.toBinary(key1);
        when(inner.getPosition()).thenReturn(Position.emptyPosition());
        when(context.recordContext()).thenReturn(new ProcessorRecordContext(0, 0, 0, TOPIC, new RecordHeaders()));

        store.put(key1, serializedValue);

        verify(inner).put(key1, serializedValue);
        verify(context).logChange(
            store.name(),
            binaryKey,
            value1,
            0L,
            headers,
            Position.emptyPosition()
        );
    }
}
