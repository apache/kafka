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

import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.ByteUtils;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsConfig.InternalConfig;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.ChangelogRecordDeserializationHelper;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockRecordCollector;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("rawtypes")
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class ChangeLoggingKeyValueBytesStoreTest {

    private final MockRecordCollector collector = new MockRecordCollector();
    private final InMemoryKeyValueStore inner = new InMemoryKeyValueStore("kv");
    private final ChangeLoggingKeyValueBytesStore store = new ChangeLoggingKeyValueBytesStore(inner);
    private InternalMockProcessorContext<?, ?> context;
    private final StreamsConfig streamsConfig = streamsConfigMock();
    private final Bytes hi = Bytes.wrap("hi".getBytes());
    private final Bytes hello = Bytes.wrap("hello".getBytes());
    private final byte[] there = "there".getBytes();
    private final byte[] world = "world".getBytes();

    private static final String INPUT_TOPIC_NAME = "input-topic";
    private static final Integer INPUT_PARTITION = 0;
    private static final Long INPUT_OFFSET = 100L;

    @BeforeEach
    public void before() {
        context = mockContext();
        context.setTime(0);
        store.init(context, store);
    }

    private InternalMockProcessorContext mockContext() {
        return new InternalMockProcessorContext<>(
            TestUtils.tempDirectory(),
            Serdes.String(),
            Serdes.Long(),
            new StreamsMetricsImpl(new Metrics(), "mock", new MockTime()),
            streamsConfig,
            () -> collector,
            new ThreadCache(new LogContext("testCache "), 0, new MockStreamsMetrics(new Metrics())),
            Time.SYSTEM
        );
    }

    @AfterEach
    public void after() {
        store.close();
    }

    @Test
    public void shouldDelegateInit() {
        final InternalMockProcessorContext context = mockContext();
        final KeyValueStore<Bytes, byte[]> innerMock = mock(InMemoryKeyValueStore.class);
        final StateStore outer = new ChangeLoggingKeyValueBytesStore(innerMock);
        outer.init(context, outer);
        verify(innerMock).init(context, outer);
    }

    @Test
    public void shouldWriteKeyValueBytesToInnerStoreOnPut() {
        store.put(hi, there);
        assertArrayEquals(there, inner.get(hi));
        assertEquals(1, collector.collected().size());
        assertEquals(hi, collector.collected().get(0).key());
        assertArrayEquals(there, (byte[]) collector.collected().get(0).value());
    }

    @Test
    public void shouldWriteAllKeyValueToInnerStoreOnPutAll() {
        store.putAll(Arrays.asList(KeyValue.pair(hi, there),
                                   KeyValue.pair(hello, world)));
        assertArrayEquals(there, inner.get(hi));
        assertArrayEquals(world, inner.get(hello));

        assertEquals(2, collector.collected().size());
        assertEquals(hi, collector.collected().get(0).key());
        assertArrayEquals(there, (byte[]) collector.collected().get(0).value());
        assertEquals(hello, collector.collected().get(1).key());
        assertArrayEquals(world, (byte[]) collector.collected().get(1).value());
    }

    @Test
    public void shouldPropagateDelete() {
        store.put(hi, there);
        store.delete(hi);
        assertEquals(0L, inner.approximateNumEntries());
        assertNull(inner.get(hi));
    }

    @Test
    public void shouldReturnOldValueOnDelete() {
        store.put(hi, there);
        assertArrayEquals(there, store.delete(hi));
    }

    @Test
    public void shouldLogKeyNullOnDelete() {
        store.put(hi, there);
        assertArrayEquals(there, store.delete(hi));

        assertEquals(2, collector.collected().size());
        assertEquals(hi, collector.collected().get(0).key());
        assertArrayEquals(there, (byte[]) collector.collected().get(0).value());
        assertEquals(hi, collector.collected().get(1).key());
        assertNull(collector.collected().get(1).value());
    }

    @Test
    public void shouldWriteToInnerOnPutIfAbsentNoPreviousValue() {
        store.putIfAbsent(hi, there);
        assertArrayEquals(there, inner.get(hi));
    }

    @Test
    public void shouldNotWriteToInnerOnPutIfAbsentWhenValueForKeyExists() {
        store.put(hi, there);
        store.putIfAbsent(hi, world);
        assertArrayEquals(there, inner.get(hi));
    }

    @Test
    public void shouldWriteToChangelogOnPutIfAbsentWhenNoPreviousValue() {
        store.putIfAbsent(hi, there);

        assertEquals(1, collector.collected().size());
        assertEquals(hi, collector.collected().get(0).key());
        assertArrayEquals(there, (byte[]) collector.collected().get(0).value());
    }

    @Test
    public void shouldNotWriteToChangeLogOnPutIfAbsentWhenValueForKeyExists() {
        store.put(hi, there);
        store.putIfAbsent(hi, world);

        assertEquals(1, collector.collected().size());
        assertEquals(hi, collector.collected().get(0).key());
        assertArrayEquals(there, (byte[]) collector.collected().get(0).value());
    }

    @Test
    public void shouldReturnCurrentValueOnPutIfAbsent() {
        store.put(hi, there);
        assertArrayEquals(there, store.putIfAbsent(hi, world));
    }

    @Test
    public void shouldReturnNullOnPutIfAbsentWhenNoPreviousValue() {
        assertNull(store.putIfAbsent(hi, there));
    }

    @Test
    public void shouldReturnValueOnGetWhenExists() {
        store.put(hello, world);
        assertArrayEquals(world, store.get(hello));
    }

    @Test
    public void shouldGetRecordsWithPrefixKey() {
        store.put(hi, there);
        store.put(ByteUtils.increment(hi), world);

        final List<Bytes> keys = new ArrayList<>();
        final List<Bytes> values = new ArrayList<>();
        int numberOfKeysReturned = 0;

        try (final KeyValueIterator<Bytes, byte[]> keysWithPrefix = store.prefixScan(hi.toString(), new StringSerializer())) {
            while (keysWithPrefix.hasNext()) {
                final KeyValue<Bytes, byte[]> next = keysWithPrefix.next();
                keys.add(next.key);
                values.add(Bytes.wrap(next.value));
                numberOfKeysReturned++;
            }
        }

        assertEquals(1, numberOfKeysReturned);
        assertEquals(List.of(hi), keys);
        assertEquals(List.of(Bytes.wrap(there)), values);
    }

    @Test
    public void shouldReturnNullOnGetWhenDoesntExist() {
        assertNull(store.get(hello));
    }

    @Test
    public void shouldLogPositionOnPut() {
        context.setRecordContext(new ProcessorRecordContext(-1, INPUT_OFFSET, INPUT_PARTITION, INPUT_TOPIC_NAME, new RecordHeaders()));
        context.setTime(1L);
        store.put(hi, there);
        assertEquals(1, collector.collected().size());
        assertNotNull(collector.collected().get(0).headers());
        final Header versionHeader = collector.collected().get(0).headers().lastHeader(ChangelogRecordDeserializationHelper.CHANGELOG_VERSION_HEADER_KEY);
        assertNotNull(versionHeader);
        assertTrue(versionHeader.equals(ChangelogRecordDeserializationHelper.CHANGELOG_VERSION_HEADER_RECORD_CONSISTENCY));
        final Header vectorHeader = collector.collected().get(0).headers().lastHeader(ChangelogRecordDeserializationHelper.CHANGELOG_POSITION_HEADER_KEY);
        assertNotNull(vectorHeader);
        final Position position = PositionSerde.deserialize(ByteBuffer.wrap(vectorHeader.value()));
        assertNotNull(position.getPartitionPositions(INPUT_TOPIC_NAME));
        assertEquals(100L, position.getPartitionPositions(INPUT_TOPIC_NAME).get(0));

    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldDelegateReadOnlyUncommittedToInner() {
        final KeyValueStore<Bytes, byte[]> innerMock = mock(KeyValueStore.class);
        final ChangeLoggingKeyValueBytesStore outer = new ChangeLoggingKeyValueBytesStore(innerMock);
        final ReadOnlyKeyValueStore<Bytes, byte[]> view = mock(ReadOnlyKeyValueStore.class);
        when(innerMock.readOnly(IsolationLevel.READ_UNCOMMITTED)).thenReturn(view);

        assertSame(view, outer.readOnly(IsolationLevel.READ_UNCOMMITTED));
        verify(innerMock).readOnly(IsolationLevel.READ_UNCOMMITTED);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldDelegateReadOnlyCommittedToInner() {
        final KeyValueStore<Bytes, byte[]> innerMock = mock(KeyValueStore.class);
        final ChangeLoggingKeyValueBytesStore outer = new ChangeLoggingKeyValueBytesStore(innerMock);
        final ReadOnlyKeyValueStore<Bytes, byte[]> view = mock(ReadOnlyKeyValueStore.class);
        when(innerMock.readOnly(IsolationLevel.READ_COMMITTED)).thenReturn(view);

        assertSame(view, outer.readOnly(IsolationLevel.READ_COMMITTED));
        verify(innerMock).readOnly(IsolationLevel.READ_COMMITTED);
    }

    private StreamsConfig streamsConfigMock() {
        final StreamsConfig streamsConfig = mock(StreamsConfig.class);

        final Map<String, Object> myValues = new HashMap<>();
        myValues.put(InternalConfig.IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED, true);
        when(streamsConfig.originals()).thenReturn(myValues);
        when(streamsConfig.values()).thenReturn(Collections.emptyMap());
        when(streamsConfig.getString(StreamsConfig.APPLICATION_ID_CONFIG)).thenReturn("add-id");
        return streamsConfig;
    }
}
