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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockRecordCollector;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.HashMap;
import java.util.Map;

import static java.time.Instant.ofEpochMilli;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class ChangeLoggingTimestampedWindowBytesStoreWithHeadersTest {

    private final byte[] value = {0};
    private final Bytes bytesKey = Bytes.wrap(value);
    private final StreamsConfig streamsConfig = streamsConfigMock();
    private final Headers testHeaders = new RecordHeaders()
        .add(new RecordHeader("key1", "value1".getBytes()))
        .add(new RecordHeader("key2", "value2".getBytes()));
    private final long testTimestamp = 42L;
    private byte[] valueTimestampHeaders;

    @Mock
    private WindowStore<Bytes, byte[]> inner;
    @Mock
    private ProcessorContextImpl context;
    private ChangeLoggingTimestampedWindowBytesStoreWithHeaders store;

    private static final Position POSITION = Position.fromMap(mkMap(mkEntry("", mkMap(mkEntry(0, 1L)))));

    @BeforeEach
    public void setUp() {
        final ValueTimestampHeaders<byte[]> valueWithHeaders =
            ValueTimestampHeaders.make(value, testTimestamp, testHeaders);
        final ValueTimestampHeadersSerializer<byte[]> serializer =
            new ValueTimestampHeadersSerializer<>(new ByteArraySerializer());
        valueTimestampHeaders = serializer.serialize("topic", valueWithHeaders);

        store = new ChangeLoggingTimestampedWindowBytesStoreWithHeaders(inner, false);
        store.init(context, store);
    }

    @AfterEach
    public void tearDown() {
        verify(inner).init(context, store);
    }

    @Test
    public void shouldDelegateInit() {
        final InternalMockProcessorContext<String, Long> context = mockContext();
        final WindowStore<Bytes, byte[]> inner = mock(InMemoryWindowStore.class);
        final StateStore outer = new ChangeLoggingTimestampedWindowBytesStoreWithHeaders(inner, false);

        outer.init(context, outer);
        verify(inner).init(context, outer);
    }

    @Test
    public void shouldLogPuts() {
        final Bytes key = WindowKeySchema.toStoreKeyBinary(bytesKey, 0, 0);
        when(inner.getPosition()).thenReturn(Position.emptyPosition());
        when(context.recordContext()).thenReturn(new ProcessorRecordContext(0, 0, 0, "topic", new RecordHeaders()));

        store.put(bytesKey, valueTimestampHeaders, context.recordContext().timestamp());

        verify(inner).put(bytesKey, valueTimestampHeaders, 0);

        final ArgumentCaptor<Headers> headersCaptor = ArgumentCaptor.forClass(Headers.class);
        verify(context).logChange(
            eq(store.name()),
            eq(key),
            eq(value),
            eq(testTimestamp),
            headersCaptor.capture(),
            eq(Position.emptyPosition())
        );

        final Headers capturedHeaders = headersCaptor.getValue();
        assertEquals(2, capturedHeaders.toArray().length);
        assertEquals("value1", new String(capturedHeaders.lastHeader("key1").value()));
        assertEquals("value2", new String(capturedHeaders.lastHeader("key2").value()));
    }

    @Test
    public void shouldLogPutsWithPosition() {
        final Bytes key = WindowKeySchema.toStoreKeyBinary(bytesKey, 0, 0);
        when(inner.getPosition()).thenReturn(POSITION);
        when(context.recordContext()).thenReturn(new ProcessorRecordContext(0, 0, 0, "topic", new RecordHeaders()));

        store.put(bytesKey, valueTimestampHeaders, context.recordContext().timestamp());

        verify(inner).put(bytesKey, valueTimestampHeaders, 0);

        final ArgumentCaptor<Headers> headersCaptor = ArgumentCaptor.forClass(Headers.class);
        verify(context).logChange(
            eq(store.name()),
            eq(key),
            eq(value),
            eq(testTimestamp),
            headersCaptor.capture(),
            eq(POSITION)
        );

        final Headers capturedHeaders = headersCaptor.getValue();
        assertEquals(2, capturedHeaders.toArray().length);
        assertEquals("value1", new String(capturedHeaders.lastHeader("key1").value()));
        assertEquals("value2", new String(capturedHeaders.lastHeader("key2").value()));
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
        try (final KeyValueIterator<Windowed<Bytes>, byte[]> unused = store.backwardFetch(bytesKey, bytesKey, ofEpochMilli(0), ofEpochMilli(1))) {
            verify(inner).backwardFetch(bytesKey, bytesKey, 0, 1);
        }
    }

    @Test
    public void shouldRetainDuplicatesWhenSet() {
        store = new ChangeLoggingTimestampedWindowBytesStoreWithHeaders(inner, true);
        store.init(context, store);
        when(inner.getPosition()).thenReturn(Position.emptyPosition());
        when(context.recordContext()).thenReturn(new ProcessorRecordContext(0, 0, 0, "topic", new RecordHeaders()));

        store.put(bytesKey, valueTimestampHeaders, context.recordContext().timestamp());
        store.put(bytesKey, valueTimestampHeaders, context.recordContext().timestamp());

        verify(inner, times(2)).put(bytesKey, valueTimestampHeaders, 0);

        final ArgumentCaptor<Headers> headersCaptor = ArgumentCaptor.forClass(Headers.class);
        verify(context, times(2)).logChange(
            eq(store.name()),
            any(Bytes.class),
            eq(value),
            eq(testTimestamp),
            headersCaptor.capture(),
            eq(Position.emptyPosition())
        );

        final Headers capturedHeaders = headersCaptor.getValue();
        assertEquals(2, capturedHeaders.toArray().length);
        assertEquals("value1", new String(capturedHeaders.lastHeader("key1").value()));
        assertEquals("value2", new String(capturedHeaders.lastHeader("key2").value()));
    }

    private InternalMockProcessorContext mockContext() {
        return new InternalMockProcessorContext<>(
            TestUtils.tempDirectory(),
            Serdes.String(),
            Serdes.Long(),
            new StreamsMetricsImpl(new Metrics(), "mock", new MockTime()),
            streamsConfig,
            MockRecordCollector::new,
            new ThreadCache(new LogContext("testCache "), 0, new MockStreamsMetrics(new Metrics())),
            Time.SYSTEM
        );
    }

    private StreamsConfig streamsConfigMock() {
        final StreamsConfig streamsConfig = mock(StreamsConfig.class);

        final Map<String, Object> myValues = new HashMap<>();
        myValues.put(StreamsConfig.InternalConfig.IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED, true);
        when(streamsConfig.originals()).thenReturn(myValues);
        when(streamsConfig.values()).thenReturn(Map.of());
        when(streamsConfig.getString(StreamsConfig.APPLICATION_ID_CONFIG)).thenReturn("add-id");
        return streamsConfig;
    }
}
