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

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
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
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockRecordCollector;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("rawtypes")
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class ChangeLoggingKeyValueBytesStoreTest {

    private final MockRecordCollector collector = new MockRecordCollector();

    @Mock
    private InMemoryKeyValueStore innerMock;
    private ChangeLoggingKeyValueBytesStore store;
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
        store = new ChangeLoggingKeyValueBytesStore(innerMock);
        store.init(context, store);
    }

    // Utility function to mock the getPosition function on inner store
    private void mockPosition() {
        when(innerMock.getPosition())
                .thenReturn(Position.emptyPosition());
    }

    // Utility function to mock the get function on inner store
    private void mockGet(final Map<Bytes, byte[]> mockMap) {
        doAnswer(invocation -> mockMap
                .get(invocation.getArgument(0, Bytes.class)))
                .when(innerMock).get(any(Bytes.class));
    }

    // Utility function to mock the put function on inner store
    private void mockPut(final Map<Bytes, byte[]> mockMap) {
        doAnswer(invocation -> {
            mockMap.put(invocation.getArgument(0), invocation.getArgument(1));
            StoreQueryUtils.updatePosition(innerMock.getPosition(), context);
            return null;
        }).when(innerMock).put(any(Bytes.class), any(byte[].class));
    }

    // Utility function to mock the putAll function on inner store
    private void mockPutAll(final Map<Bytes, byte[]> mockMap) {
        doAnswer(invocation -> {
            final List<KeyValue<Bytes, byte[]>> entries = invocation.getArgument(0);
            for (final KeyValue<Bytes, byte[]> entry : entries) {
                mockMap.put(entry.key, entry.value);
            }
            return null;
        }).when(innerMock).putAll(anyList());
    }

    // Utility function to mock the delete function on inner store
    private void mockDelete(final Map<Bytes, byte[]> mockMap) {
        doAnswer(invocation -> {
            final Bytes key = invocation.getArgument(0);
            final byte[] oldValue = mockMap.get(key);
            mockMap.remove(key);
            return oldValue;
        }).when(innerMock).delete(any(Bytes.class));
    }

    // Utility function to mock the putIfAbsent on inner store
    private void mockPutIfAbsent(final Map<Bytes, byte[]> mockMap) {
        doAnswer(invocation -> {
            final Bytes key = invocation.getArgument(0);
            final byte[] value = invocation.getArgument(1);
            return mockMap.putIfAbsent(key, value);
        }).when(innerMock).putIfAbsent(any(Bytes.class), any(byte[].class));
    }

    // Utility function to mock the prefix scan function on inner store
    private void mockPrefixScan(final Map<Bytes, byte[]> mockMap) {
        when(innerMock.prefixScan(anyString(), any())).thenAnswer(invocation -> {
            final String prefix = invocation.getArgument(0);
            final List<KeyValue<Bytes, byte[]>> matchingRecords = new ArrayList<>();
            for (final Map.Entry<Bytes, byte[]> entry : mockMap.entrySet()) {
                if (entry.getKey().toString().startsWith(prefix)) {
                    matchingRecords.add(KeyValue.pair(entry.getKey(), entry.getValue()));
                }
            }
            return new KeyValueIterator<Bytes, byte[]>() {
                private final Iterator<KeyValue<Bytes, byte[]>> iterator = matchingRecords.iterator();

                @Override
                public boolean hasNext() {
                    return iterator.hasNext();
                }

                @Override
                public KeyValue<Bytes, byte[]> next() {
                    return iterator.next();
                }

                @Override
                public void close() {
                    // No resources to clean up in this mock
                }

                @Override
                public Bytes peekNextKey() {
                    return null;
                }
            };
        });
    }

    private InternalMockProcessorContext mockContext() {
        return new InternalMockProcessorContext<>(
            TestUtils.tempDirectory(),
            Serdes.String(),
            Serdes.Long(),
            new StreamsMetricsImpl(new Metrics(), "mock", "processId", new MockTime()),
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
        final InternalMockProcessorContext mockContext = mockContext();
        final StateStore outer = new ChangeLoggingKeyValueBytesStore(innerMock);
        outer.init(mockContext, outer);
        verify(innerMock).init(mockContext, outer);
    }

    @Test
    public void shouldWriteKeyValuePairBytesToInnerStoreOnPut() {
        final Map<Bytes, byte[]> mockMap = new HashMap<>();
        mockPut(mockMap);
        mockGet(mockMap);
        mockPosition();

        store.put(hi, there);
        assertThat(innerMock.get(hi), equalTo(there));
    }

    @Test
    public void shouldWriteKeyValuePairToChangeLogOnPut() {
        final Map<Bytes, byte[]> mockMap = new HashMap<>();
        mockPut(mockMap);
        mockPosition();

        store.put(hi, there);

        assertThat(collector.collected().size(), equalTo(1));
        assertThat(collector.collected().get(0).key(), equalTo(hi));
        assertThat(collector.collected().get(0).value(), equalTo(there));
    }

    @Test
    public void shouldWriteAllKeyValuePairsToInnerStoreOnPutAll() {
        final Map<Bytes, byte[]> mockMap = new HashMap<>();

        mockPutAll(mockMap);
        mockGet(mockMap);
        mockPosition();

        store.putAll(Arrays.asList(KeyValue.pair(hi, there),
                                   KeyValue.pair(hello, world)));
        assertThat(innerMock.get(hi), equalTo(there));
        assertThat(innerMock.get(hello), equalTo(world));
    }

    @Test
    public void shouldWriteAllKeyValuePairsToChangeLogOnPutAll() {
        final Map<Bytes, byte[]> mockMap = new HashMap<>();

        mockPutAll(mockMap);
        mockPosition();

        store.putAll(Arrays.asList(KeyValue.pair(hi, there),
                KeyValue.pair(hello, world)));

        assertThat(collector.collected().size(), equalTo(2));
        assertThat(collector.collected().get(0).key(), equalTo(hi));
        assertThat(collector.collected().get(0).value(), equalTo(there));
        assertThat(collector.collected().get(1).key(), equalTo(hello));
        assertThat(collector.collected().get(1).value(), equalTo(world));
    }

    @Test
    public void shouldPropagateDelete() {
        final Map<Bytes, byte[]> mockMap = new HashMap<>();
        mockPut(mockMap);
        mockGet(mockMap);
        mockDelete(mockMap);
        mockPosition();

        store.put(hi, there);
        store.delete(hi);

        assertThat(innerMock.approximateNumEntries(), equalTo(0L));
        assertThat(innerMock.get(hi), nullValue());
    }

    @Test
    public void shouldReturnCurrentValueOnDelete() {
        final Map<Bytes, byte[]> mockMap = new HashMap<>();
        mockPut(mockMap);
        mockDelete(mockMap);
        mockPosition();

        store.put(hi, there);
        assertThat(store.delete(hi), equalTo(there));
    }

    @Test
    public void shouldWriteKeyNullToChangeLogOnDelete() {

        final Map<Bytes, byte[]> mockMap = new HashMap<>();

        mockPut(mockMap);
        mockDelete(mockMap);
        mockPosition();

        store.put(hi, there);
        store.delete(hi);

        assertThat(collector.collected().size(), equalTo(2));
        assertThat(collector.collected().get(0).key(), equalTo(hi));
        assertThat(collector.collected().get(0).value(), equalTo(there));
        assertThat(collector.collected().get(1).key(), equalTo(hi));
        assertThat(collector.collected().get(1).value(), nullValue());
    }

    @Test
    public void shouldWriteToInnerStoreOnPutIfAbsentWhenKeyDoesntExists() {
        final Map<Bytes, byte[]> mockMap = new HashMap<>();

        mockPutIfAbsent(mockMap);
        mockGet(mockMap);
        mockPosition();

        store.putIfAbsent(hi, there);
        assertThat(innerMock.get(hi), equalTo(there));
    }

    @Test
    public void shouldNotWriteToInnerStoreOnPutIfAbsentWhenValueForKeyExists() {
        final Map<Bytes, byte[]> mockMap = new HashMap<>();

        mockPut(mockMap);
        mockPutIfAbsent(mockMap);
        mockGet(mockMap);
        mockPosition();

        store.put(hi, there);
        store.putIfAbsent(hi, world);

        assertThat(innerMock.get(hi), equalTo(there));
    }

    @Test
    public void shouldWriteToChangeLogOnPutIfAbsentWhenKeyDoesntExists() {

        final Map<Bytes, byte[]> mockMap = new HashMap<>();

        mockPutIfAbsent(mockMap);
        mockPosition();

        store.putIfAbsent(hi, there);

        assertThat(collector.collected().size(), equalTo(1));
        assertThat(collector.collected().get(0).key(), equalTo(hi));
        assertThat(collector.collected().get(0).value(), equalTo(there));
    }

    @Test
    public void shouldNotWriteToChangeLogOnPutIfAbsentWhenKeyExists() {
        final Map<Bytes, byte[]> mockMap = new HashMap<>();
        mockPut(mockMap);
        mockPutIfAbsent(mockMap);
        mockPosition();

        store.put(hi, there);
        store.putIfAbsent(hi, world);

        assertThat(collector.collected().size(), equalTo(1));
        assertThat(collector.collected().get(0).key(), equalTo(hi));
        assertThat(collector.collected().get(0).value(), equalTo(there));
    }

    @Test
    public void shouldReturnCurrentValueOnPutIfAbsentWhenKeyExists() {

        final Map<Bytes, byte[]> mockMap = new HashMap<>();

        mockPut(mockMap);
        mockPutIfAbsent(mockMap);
        mockPosition();

        store.put(hi, there);
        assertThat(store.putIfAbsent(hi, world), equalTo(there));
    }

    @Test
    public void shouldReturnNullOnPutIfAbsentWhenNoPreviousValueExists() {
        final Map<Bytes, byte[]> mockMap = new HashMap<>();
        mockPutIfAbsent(mockMap);
        mockPosition();

        assertThat(store.putIfAbsent(hi, there), is(nullValue()));
    }

    @Test
    public void shouldReturnCurrentValueOnGetWhenKeyExists() {
        final Map<Bytes, byte[]> mockMap = new HashMap<>();
        mockPut(mockMap);
        mockGet(mockMap);
        mockPosition();

        store.put(hello, world);
        assertThat(store.get(hello), equalTo(world));
    }

    @Test
    public void shouldGetRecordsWithPrefixKey() {
        final Map<Bytes, byte[]> mockMap = new HashMap<>();

        // Setting up mock inner store
        mockPut(mockMap);
        mockPrefixScan(mockMap);
        mockPosition();

        store.put(hi, there);
        store.put(Bytes.increment(hi), world);

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

        assertThat(numberOfKeysReturned, is(1));
        assertThat(keys, is(Collections.singletonList(hi)));
        assertThat(values, is(Collections.singletonList(Bytes.wrap(there))));
    }

    @Test
    public void shouldReturnNullValueOnGetWhenKeyDoesntExist() {
        final Map<Bytes, byte[]> mockMap = new HashMap<>();
        mockGet(mockMap);
        assertThat(store.get(hello), is(nullValue()));
    }

    @Test
    public void shouldWritePositionToChangeLogOnPut() {

        final Map<Bytes, byte[]> mockMap = new HashMap<>();

        mockPut(mockMap);
        mockPosition();

        context.setRecordContext(new ProcessorRecordContext(-1, INPUT_OFFSET, INPUT_PARTITION, INPUT_TOPIC_NAME, new RecordHeaders()));
        context.setTime(1L);
        store.put(hi, there);
        assertThat(collector.collected().size(), equalTo(1));
        assertThat(collector.collected().get(0).headers(), is(notNullValue()));
        final Header versionHeader = collector.collected().get(0).headers().lastHeader(ChangelogRecordDeserializationHelper.CHANGELOG_VERSION_HEADER_KEY);
        assertThat(versionHeader, is(notNullValue()));
        assertThat(versionHeader.equals(ChangelogRecordDeserializationHelper.CHANGELOG_VERSION_HEADER_RECORD_CONSISTENCY), is(true));
        final Header vectorHeader = collector.collected().get(0).headers().lastHeader(ChangelogRecordDeserializationHelper.CHANGELOG_POSITION_HEADER_KEY);
        assertThat(vectorHeader, is(notNullValue()));
        final Position position = PositionSerde.deserialize(ByteBuffer.wrap(vectorHeader.value()));
        assertThat(position.getPartitionPositions(INPUT_TOPIC_NAME), is(notNullValue()));
        assertThat(position.getPartitionPositions(INPUT_TOPIC_NAME), hasEntry(0, 100L));

    }

    private StreamsConfig streamsConfigMock() {

        final StreamsConfig mockedStreamsConfig = mock(StreamsConfig.class);

        final Map<String, Object> myValues = new HashMap<>();
        myValues.put(InternalConfig.IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED, true);

        when(mockedStreamsConfig.originals()).thenReturn(myValues);
        when(mockedStreamsConfig.values()).thenReturn(Collections.emptyMap());
        when(mockedStreamsConfig.getString(StreamsConfig.APPLICATION_ID_CONFIG)).thenReturn("add-id");

        return mockedStreamsConfig;
    }
}
