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
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.ReadOnlyRecord;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.query.FailureReason;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.TimestampedWindowKeyWithHeadersQuery;
import org.apache.kafka.streams.query.TimestampedWindowRangeWithHeadersQuery;
import org.apache.kafka.streams.query.WindowKeyQuery;
import org.apache.kafka.streams.query.WindowRangeQuery;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyRecordIterator;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.KeyValueIteratorStub;
import org.apache.kafka.test.MockRecordCollector;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class MeteredTimestampedWindowStoreWithHeadersTest {
    private static final String STORE_NAME = "mocked-store";
    private static final String STORE_TYPE = "scope";
    private static final String CHANGELOG_TOPIC = "changelog-topic";
    private static final String KEY = "key";
    private static final Bytes KEY_BYTES = Bytes.wrap(KEY.getBytes());
    // timestamp is 97 what is ASCII of 'a'
    private static final long TIMESTAMP = 97L;
    private static final RecordHeaders HEADERS = makeHeaders();
    private static final ValueTimestampHeaders<String> VALUE_TIMESTAMP_HEADERS =
        ValueTimestampHeaders.make("value", TIMESTAMP, HEADERS);
    private static final byte[] VALUE_TIMESTAMP_HEADERS_BYTES = serializeValueTimestampHeaders();
    private static final int WINDOW_SIZE_MS = 10;

    private InternalMockProcessorContext<String, Long> context;
    private final TaskId taskId = new TaskId(0, 0, "My-Topology");
    @Mock
    private WindowStore<Bytes, byte[]> innerStoreMock;
    private final Metrics metrics = new Metrics(new MetricConfig().recordLevel(Sensor.RecordingLevel.DEBUG));
    private MeteredTimestampedWindowStoreWithHeaders<String, String> store;
    private MockTime mockTime;
    private Deserializer<String> keyDeserializer;

    public void setUp() {
        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, "test", new MockTime());

        context = new InternalMockProcessorContext<>(
            TestUtils.tempDirectory(),
            Serdes.String(),
            Serdes.Long(),
            streamsMetrics,
            new StreamsConfig(StreamsTestUtils.getStreamsConfig()),
            MockRecordCollector::new,
            new ThreadCache(new LogContext("testCache "), 0, streamsMetrics),
            Time.SYSTEM,
            taskId
        );

        when(innerStoreMock.name()).thenReturn(STORE_NAME);

        mockTime = new MockTime();
        store = new MeteredTimestampedWindowStoreWithHeaders<>(
            innerStoreMock,
            WINDOW_SIZE_MS, // any size
            STORE_TYPE,
            mockTime,
            Serdes.String(),
            new ValueTimestampHeadersSerde<>(new SerdeThatDoesntHandleNull())
        );
    }

    public void setUpWithoutContextName() {
        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, "test", new MockTime());

        context = new InternalMockProcessorContext<>(
            TestUtils.tempDirectory(),
            Serdes.String(),
            Serdes.Long(),
            streamsMetrics,
            new StreamsConfig(StreamsTestUtils.getStreamsConfig()),
            MockRecordCollector::new,
            new ThreadCache(new LogContext("testCache "), 0, streamsMetrics),
            Time.SYSTEM,
            taskId
        );

        store = new MeteredTimestampedWindowStoreWithHeaders<>(
            innerStoreMock,
            WINDOW_SIZE_MS, // any size
            STORE_TYPE,
            new MockTime(),
            Serdes.String(),
            new ValueTimestampHeadersSerde<>(new SerdeThatDoesntHandleNull())
        );
    }

    @Test
    public void shouldDelegateInit() {
        setUpWithoutContextName();
        @SuppressWarnings("unchecked")
        final WindowStore<Bytes, byte[]> inner = mock(WindowStore.class);
        final MeteredTimestampedWindowStoreWithHeaders<String, String> outer = new MeteredTimestampedWindowStoreWithHeaders<>(
            inner,
            WINDOW_SIZE_MS, // any size
            STORE_TYPE,
            new MockTime(),
            Serdes.String(),
            new ValueTimestampHeadersSerde<>(new SerdeThatDoesntHandleNull())
        );
        when(inner.name()).thenReturn("store");

        outer.init(context, outer);

        verify(inner).init(context, outer);
    }

    @Test
    public void shouldPassChangelogTopicNameToStateStoreSerde() {
        setUp();
        context.addChangelogForStore(STORE_NAME, CHANGELOG_TOPIC);
        doShouldPassChangelogTopicNameToStateStoreSerde(CHANGELOG_TOPIC);
    }

    @Test
    public void shouldPassDefaultChangelogTopicNameToStateStoreSerdeIfLoggingDisabled() {
        setUp();
        final String defaultChangelogTopicName =
            ProcessorStateManager.storeChangelogTopic(context.applicationId(), STORE_NAME, taskId.topologyName());
        doShouldPassChangelogTopicNameToStateStoreSerde(defaultChangelogTopicName);
    }

    @Test
    public void shouldCloseUnderlyingStore() {
        setUp();
        store.init(context, store);
        store.close();

        verify(innerStoreMock).close();
    }

    @Test
    public void shouldNotExceptionIfFetchReturnsNull() {
        setUp();
        when(innerStoreMock.fetch(Bytes.wrap("a".getBytes()), 0)).thenReturn(null);

        store.init(context, store);
        assertNull(store.fetch("a", 0));
    }

    @Test
    public void shouldNotThrowExceptionIfSerdesCorrectlySetFromProcessorContext() {
        setUp();
        when(innerStoreMock.name()).thenReturn("mocked-store");
        final MeteredTimestampedWindowStoreWithHeaders<String, Long> store = new MeteredTimestampedWindowStoreWithHeaders<>(
            innerStoreMock,
            10L, // any size
            "scope",
            new MockTime(),
            null,
            null
        );
        store.init(context, innerStoreMock);

        try {
            store.put("key", ValueTimestampHeaders.make(42L, 60000, new RecordHeaders()), 60000L);
        } catch (final StreamsException exception) {
            if (exception.getCause() instanceof ClassCastException) {
                fail("Serdes are not correctly set from processor context.");
            }
            throw exception;
        }
    }

    @Test
    public void shouldNotThrowExceptionIfSerdesCorrectlySetFromConstructorParameters() {
        setUp();
        when(innerStoreMock.name()).thenReturn("mocked-store");
        final MeteredTimestampedWindowStoreWithHeaders<String, Long> store = new MeteredTimestampedWindowStoreWithHeaders<>(
            innerStoreMock,
            10L, // any size
            "scope",
            new MockTime(),
            Serdes.String(),
            new ValueTimestampHeadersSerde<>(Serdes.Long())
        );
        store.init(context, innerStoreMock);

        try {
            store.put("key", ValueTimestampHeaders.make(42L, 60000, new RecordHeaders()), 60000L);
        } catch (final StreamsException exception) {
            if (exception.getCause() instanceof ClassCastException) {
                fail("Serdes are not correctly set from constructor parameters.");
            }
            throw exception;
        }
    }

    private static RecordHeaders makeHeaders() {
        final RecordHeaders headers = new RecordHeaders();
        headers.add("header-key", "header-value".getBytes());
        return headers;
    }

    private static byte[] serializeValueTimestampHeaders() {
        final ValueTimestampHeadersSerializer<String> serializer = new ValueTimestampHeadersSerializer<>(new StringSerializer());
        return serializer.serialize("topic", VALUE_TIMESTAMP_HEADERS);
    }

    @SuppressWarnings("unchecked")
    private void doShouldPassChangelogTopicNameToStateStoreSerde(final String topic) {
        final Serde<String> keySerde = mock(Serde.class);
        final Serializer<String> keySerializer = mock(Serializer.class);
        final Serde<ValueTimestampHeaders<String>> valueSerde = mock(Serde.class);
        final Deserializer<ValueTimestampHeaders<String>> valueDeserializer = mock(Deserializer.class);
        final Serializer<ValueTimestampHeaders<String>> valueSerializer = mock(Serializer.class);
        when(keySerde.serializer()).thenReturn(keySerializer);
        // For put: key serialization uses value's headers
        when(keySerializer.serialize(topic, HEADERS, KEY)).thenReturn(KEY.getBytes());
        when(valueSerde.deserializer()).thenReturn(valueDeserializer);
        when(valueDeserializer.deserialize(topic, HEADERS, VALUE_TIMESTAMP_HEADERS_BYTES)).thenReturn(VALUE_TIMESTAMP_HEADERS);
        when(valueSerde.serializer()).thenReturn(valueSerializer);
        // For put: value serialization uses value's headers
        when(valueSerializer.serialize(topic, HEADERS, VALUE_TIMESTAMP_HEADERS)).thenReturn(VALUE_TIMESTAMP_HEADERS_BYTES);
        context.setRecordContext(new ProcessorRecordContext(
            0L,
            0L,
            0,
            topic,
            HEADERS
        ));
        when(innerStoreMock.fetch(KEY_BYTES, TIMESTAMP)).thenReturn(VALUE_TIMESTAMP_HEADERS_BYTES);
        store = new MeteredTimestampedWindowStoreWithHeaders<>(
            innerStoreMock,
            WINDOW_SIZE_MS,
            STORE_TYPE,
            new MockTime(),
            keySerde,
            valueSerde
        );

        store.init(context, store);
        store.fetch(KEY, TIMESTAMP);
        store.put(KEY, VALUE_TIMESTAMP_HEADERS, TIMESTAMP);

        verify(innerStoreMock).fetch(KEY_BYTES, TIMESTAMP);
        verify(innerStoreMock).put(KEY_BYTES, VALUE_TIMESTAMP_HEADERS_BYTES, TIMESTAMP);
    }

    @SuppressWarnings("unchecked")
    private MeteredTimestampedWindowStoreWithHeaders<String, String> createStoreWithMockSerdes() {
        final Serde<String> keySerde = mock(Serde.class);
        final Serializer<String> keySerializer = mock(Serializer.class);
        keyDeserializer = mock(Deserializer.class);
        final Serde<ValueTimestampHeaders<String>> valueSerde = mock(Serde.class);
        final Deserializer<ValueTimestampHeaders<String>> valueDeserializer = mock(Deserializer.class);

        lenient().when(keySerde.deserializer()).thenReturn(keyDeserializer);
        lenient().when(keySerde.serializer()).thenReturn(keySerializer);
        lenient().when(valueSerde.deserializer()).thenReturn(valueDeserializer);

        lenient().when(keySerializer.serialize(any(), any(RecordHeaders.class), any())).thenReturn(KEY.getBytes());

        lenient().when(valueDeserializer.deserialize(any(), any(RecordHeaders.class), eq(VALUE_TIMESTAMP_HEADERS_BYTES)))
            .thenReturn(VALUE_TIMESTAMP_HEADERS);

        lenient().when(keyDeserializer.deserialize(any(), eq(HEADERS), eq(KEY.getBytes())))
            .thenReturn(KEY);

        final MeteredTimestampedWindowStoreWithHeaders<String, String> mockStore = new MeteredTimestampedWindowStoreWithHeaders<>(
            innerStoreMock,
            WINDOW_SIZE_MS,
            STORE_TYPE,
            new MockTime(),
            keySerde,
            valueSerde
        );
        mockStore.init(context, mockStore);
        return mockStore;
    }

    @Test
    public void shouldUseHeadersFromValueToDeserializeKeyInFetchAll() {
        setUp();

        final Windowed<Bytes> windowedKey = new Windowed<>(KEY_BYTES, new TimeWindow(0, WINDOW_SIZE_MS));
        final KeyValue<Windowed<Bytes>, byte[]> testData = KeyValue.pair(windowedKey, VALUE_TIMESTAMP_HEADERS_BYTES);
        when(innerStoreMock.fetchAll(0, 100))
            .thenReturn(new KeyValueIteratorStub<>(List.of(testData).iterator()));

        store = createStoreWithMockSerdes();

        final KeyValueIterator<Windowed<String>, ValueTimestampHeaders<String>> iterator = store.fetchAll(0, 100);

        assertTrue(iterator.hasNext());
        assertEquals(KEY, iterator.peekNextKey().key());
        final KeyValue<Windowed<String>, ValueTimestampHeaders<String>> result = iterator.next();

        assertEquals(KEY, result.key.key());
        assertEquals(VALUE_TIMESTAMP_HEADERS, result.value);
        assertFalse(iterator.hasNext());
        iterator.close();

        // The critical verification: key deserializer must have been called with HEADERS (not empty headers)
        verify(keyDeserializer).deserialize(any(), eq(HEADERS), eq(KEY.getBytes()));
    }

    @Test
    public void shouldUseHeadersFromValueToDeserializeKeyInAll() {
        setUp();

        final Windowed<Bytes> windowedKey = new Windowed<>(KEY_BYTES, new TimeWindow(0, WINDOW_SIZE_MS));
        final KeyValue<Windowed<Bytes>, byte[]> testData = KeyValue.pair(windowedKey, VALUE_TIMESTAMP_HEADERS_BYTES);
        when(innerStoreMock.all())
            .thenReturn(new KeyValueIteratorStub<>(List.of(testData).iterator()));

        store = createStoreWithMockSerdes();

        final KeyValueIterator<Windowed<String>, ValueTimestampHeaders<String>> iterator = store.all();

        assertTrue(iterator.hasNext());
        assertEquals(KEY, iterator.peekNextKey().key());
        final KeyValue<Windowed<String>, ValueTimestampHeaders<String>> result = iterator.next();

        assertEquals(KEY, result.key.key());
        assertEquals(VALUE_TIMESTAMP_HEADERS, result.value);
        assertFalse(iterator.hasNext());
        iterator.close();

        verify(keyDeserializer).deserialize(any(), eq(HEADERS), eq(KEY.getBytes()));
    }

    @Test
    public void shouldUseHeadersFromValueToDeserializeKeyInFetchRange() {
        setUp();

        final Windowed<Bytes> windowedKey = new Windowed<>(KEY_BYTES, new TimeWindow(0, WINDOW_SIZE_MS));
        final KeyValue<Windowed<Bytes>, byte[]> testData = KeyValue.pair(windowedKey, VALUE_TIMESTAMP_HEADERS_BYTES);
        when(innerStoreMock.fetch(any(Bytes.class), any(Bytes.class), eq(0L), eq(100L)))
            .thenReturn(new KeyValueIteratorStub<>(List.of(testData).iterator()));

        store = createStoreWithMockSerdes();

        final KeyValueIterator<Windowed<String>, ValueTimestampHeaders<String>> iterator =
            store.fetch(KEY, KEY, 0, 100);

        assertTrue(iterator.hasNext());
        assertEquals(KEY, iterator.peekNextKey().key());
        final KeyValue<Windowed<String>, ValueTimestampHeaders<String>> result =
            iterator.next();

        assertEquals(KEY, result.key.key());
        assertEquals(VALUE_TIMESTAMP_HEADERS, result.value);
        assertFalse(iterator.hasNext());
        iterator.close();

        verify(keyDeserializer).deserialize(any(), eq(HEADERS), eq(KEY.getBytes()));
    }

    @Test
    public void shouldUseHeadersFromValueToDeserializeKeyInBackwardFetchAll() {
        setUp();

        final Windowed<Bytes> windowedKey = new Windowed<>(KEY_BYTES, new TimeWindow(0, WINDOW_SIZE_MS));
        final KeyValue<Windowed<Bytes>, byte[]> testData = KeyValue.pair(windowedKey, VALUE_TIMESTAMP_HEADERS_BYTES);
        when(innerStoreMock.backwardFetchAll(0, 100))
            .thenReturn(new KeyValueIteratorStub<>(List.of(testData).iterator()));

        store = createStoreWithMockSerdes();

        final KeyValueIterator<Windowed<String>, ValueTimestampHeaders<String>> iterator = store.backwardFetchAll(0, 100);

        assertTrue(iterator.hasNext());
        assertEquals(KEY, iterator.peekNextKey().key());
        final KeyValue<Windowed<String>, ValueTimestampHeaders<String>> result = iterator.next();

        assertEquals(KEY, result.key.key());
        assertEquals(VALUE_TIMESTAMP_HEADERS, result.value);
        assertFalse(iterator.hasNext());
        iterator.close();

        verify(keyDeserializer).deserialize(any(), eq(HEADERS), eq(KEY.getBytes()));
    }

    @Test
    public void shouldUseHeadersFromValueToDeserializeKeyInBackwardAll() {
        setUp();

        final Windowed<Bytes> windowedKey = new Windowed<>(KEY_BYTES, new TimeWindow(0, WINDOW_SIZE_MS));
        final KeyValue<Windowed<Bytes>, byte[]> testData = KeyValue.pair(windowedKey, VALUE_TIMESTAMP_HEADERS_BYTES);
        when(innerStoreMock.backwardAll())
            .thenReturn(new KeyValueIteratorStub<>(List.of(testData).iterator()));

        store = createStoreWithMockSerdes();

        final KeyValueIterator<Windowed<String>, ValueTimestampHeaders<String>> iterator = store.backwardAll();

        assertTrue(iterator.hasNext());
        assertEquals(KEY, iterator.peekNextKey().key());
        final KeyValue<Windowed<String>, ValueTimestampHeaders<String>> result = iterator.next();

        assertEquals(KEY, result.key.key());
        assertEquals(VALUE_TIMESTAMP_HEADERS, result.value);
        assertFalse(iterator.hasNext());
        iterator.close();

        verify(keyDeserializer).deserialize(any(), eq(HEADERS), eq(KEY.getBytes()));
    }

    @Test
    public void shouldUseHeadersFromValueToDeserializeKeyInBackwardFetchRange() {
        setUp();

        final Windowed<Bytes> windowedKey = new Windowed<>(KEY_BYTES, new TimeWindow(0, WINDOW_SIZE_MS));
        final KeyValue<Windowed<Bytes>, byte[]> testData = KeyValue.pair(windowedKey, VALUE_TIMESTAMP_HEADERS_BYTES);
        when(innerStoreMock.backwardFetch(any(Bytes.class), any(Bytes.class), eq(0L), eq(100L)))
            .thenReturn(new KeyValueIteratorStub<>(List.of(testData).iterator()));

        store = createStoreWithMockSerdes();

        final KeyValueIterator<Windowed<String>, ValueTimestampHeaders<String>> iterator =
            store.backwardFetch(KEY, KEY, 0, 100);

        assertTrue(iterator.hasNext());
        assertEquals(KEY, iterator.peekNextKey().key());
        final KeyValue<Windowed<String>, ValueTimestampHeaders<String>> result =
            iterator.next();

        assertEquals(KEY, result.key.key());
        assertEquals(VALUE_TIMESTAMP_HEADERS, result.value);
        assertFalse(iterator.hasNext());
        iterator.close();

        verify(keyDeserializer).deserialize(any(), eq(HEADERS), eq(KEY.getBytes()));
    }

    @Test
    public void shouldUseHeadersFromValueToDeserializeKeyInReadOnlyFetchAll() {
        setUp();

        final Windowed<Bytes> windowedKey = new Windowed<>(KEY_BYTES, new TimeWindow(0, WINDOW_SIZE_MS));
        final KeyValue<Windowed<Bytes>, byte[]> testData = KeyValue.pair(windowedKey, VALUE_TIMESTAMP_HEADERS_BYTES);

        final ReadOnlyWindowStore<Bytes, byte[]> readOnlyInner = mock(ReadOnlyWindowStore.class);
        when(innerStoreMock.readOnly(IsolationLevel.READ_COMMITTED)).thenReturn(readOnlyInner);
        when(readOnlyInner.fetchAll(Instant.ofEpochMilli(0), Instant.ofEpochMilli(100)))
            .thenReturn(new KeyValueIteratorStub<>(List.of(testData).iterator()));

        store = createStoreWithMockSerdes();

        final KeyValueIterator<Windowed<String>, ValueTimestampHeaders<String>> iterator =
            store.readOnly(IsolationLevel.READ_COMMITTED).fetchAll(Instant.ofEpochMilli(0), Instant.ofEpochMilli(100));

        assertTrue(iterator.hasNext());
        assertEquals(KEY, iterator.peekNextKey().key());
        final KeyValue<Windowed<String>, ValueTimestampHeaders<String>> result = iterator.next();

        assertEquals(KEY, result.key.key());
        assertEquals(VALUE_TIMESTAMP_HEADERS, result.value);
        assertFalse(iterator.hasNext());
        iterator.close();

        verify(keyDeserializer).deserialize(any(), eq(HEADERS), eq(KEY.getBytes()));
    }

    @Test
    public void shouldPropagateWindowKeyQueryBoundsForTimestampedWindowKeyWithHeadersQuery() {
        setUp();
        store.init(context, store);

        final Instant timeFrom = Instant.ofEpochMilli(5);
        final Instant timeTo = Instant.ofEpochMilli(100);
        final WindowKeyQuery<?, ?> rawQuery = forwardedRawWindowKeyQuery(
            TimestampedWindowKeyWithHeadersQuery.<String, String>withKeyAndWindowStartRange(KEY, timeFrom, timeTo));

        // The typed query is translated into a raw byte-level WindowKeyQuery with the serialized key
        // and the same window-start range, forwarded to the wrapped store.
        assertEquals(KEY_BYTES, rawQuery.getKey());
        assertEquals(Optional.of(timeFrom), rawQuery.getTimeFrom());
        assertEquals(Optional.of(timeTo), rawQuery.getTimeTo());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void shouldPropagateWrappedStoreFailureForTimestampedWindowKeyWithHeadersQuery() {
        setUp();
        store.init(context, store);
        when(innerStoreMock.query(any(), any(PositionBound.class), any(QueryConfig.class)))
                .thenReturn((QueryResult) QueryResult.forFailure(FailureReason.STORE_EXCEPTION, "boom"));

        final QueryResult<ReadOnlyRecordIterator<Windowed<String>, String>> result = store.query(
                TimestampedWindowKeyWithHeadersQuery.<String, String>withKeyAndWindowStartRange(
                        KEY, Instant.ofEpochMilli(5), Instant.ofEpochMilli(100)),
                PositionBound.unbounded(),
                new QueryConfig(false));

        assertFalse(result.isSuccess());
        assertEquals(FailureReason.STORE_EXCEPTION, result.getFailureReason());
        assertEquals("boom", result.getFailureMessage());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void shouldTrackNumOpenIteratorsForTimestampedWindowKeyWithHeadersQuery() {
        setUp();
        store.init(context, store);
        when(innerStoreMock.query(any(), any(PositionBound.class), any(QueryConfig.class)))
            .thenReturn((QueryResult) QueryResult.forResult(windowKeyIterator(List.of())));

        final KafkaMetric openIterators = numOpenIteratorsMetric();
        assertEquals(0L, (Long) openIterators.metricValue());

        final QueryResult<ReadOnlyRecordIterator<Windowed<String>, String>> result = store.query(
            TimestampedWindowKeyWithHeadersQuery.<String, String>withKeyAndWindowStartRange(
                KEY, Instant.ofEpochMilli(5), Instant.ofEpochMilli(100)),
            PositionBound.unbounded(),
            new QueryConfig(false));
        assertTrue(result.isSuccess());

        // The query's ReadOnlyRecordIterator registers itself on open and deregisters on close.
        try (ReadOnlyRecordIterator<Windowed<String>, String> iterator = result.getResult()) {
            assertEquals(1L, (Long) openIterators.metricValue());
        }
        assertEquals(0L, (Long) openIterators.metricValue());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void shouldDecrementOpenIteratorsTwiceWhenClosedTwiceForTimestampedWindowKeyWithHeadersQuery() {
        setUp();
        store.init(context, store);
        when(innerStoreMock.query(any(), any(PositionBound.class), any(QueryConfig.class)))
            .thenReturn((QueryResult) QueryResult.forResult(windowKeyIterator(List.of())));

        final KafkaMetric openIterators = numOpenIteratorsMetric();
        final ReadOnlyRecordIterator<Windowed<String>, String> iterator = store.query(
            TimestampedWindowKeyWithHeadersQuery.<String, String>withKeyAndWindowStartRange(
                KEY, Instant.ofEpochMilli(5), Instant.ofEpochMilli(100)),
            PositionBound.unbounded(),
            new QueryConfig(false)).getResult();

        assertEquals(1L, (Long) openIterators.metricValue());
        iterator.close();
        assertEquals(0L, (Long) openIterators.metricValue());
        // close() is intentionally not idempotent (matching the sibling metered iterators): each call
        // decrements, so a repeated close drives the gauge below zero. Callers must close exactly once.
        iterator.close();
        assertEquals(-1L, (Long) openIterators.metricValue());
    }

    // The window store previously had no iterator-duration coverage at all. This mirrors the
    // session/KV shouldTimeIteratorDuration: it goes through store.all() -> the KeyValueIterator
    // sibling, whose close() records the operation (fetch) and iterator-duration sensors via the
    // shared AbstractMeteredIterator lifecycle.
    @Test
    public void shouldTimeIteratorDuration() {
        setUp();
        store.init(context, store);
        when(innerStoreMock.all()).thenReturn(windowRangeIterator(List.of()), windowRangeIterator(List.of()));

        final KafkaMetric iteratorDurationAvg = metric("iterator-duration-avg");
        final KafkaMetric iteratorDurationMax = metric("iterator-duration-max");
        assertEquals(Double.NaN, (Double) iteratorDurationAvg.metricValue());
        assertEquals(Double.NaN, (Double) iteratorDurationMax.metricValue());

        // Two samples (2ms then 3ms) so avg (2.5ms) and max (3ms) differ -- one sample would leave them
        // identical and not actually pin avg.
        try (KeyValueIterator<Windowed<String>, ValueTimestampHeaders<String>> iterator = store.all()) {
            mockTime.sleep(2);
        }

        assertEquals(2.0 * TimeUnit.MILLISECONDS.toNanos(1), (double) iteratorDurationAvg.metricValue());
        assertEquals(2.0 * TimeUnit.MILLISECONDS.toNanos(1), (double) iteratorDurationMax.metricValue());

        try (KeyValueIterator<Windowed<String>, ValueTimestampHeaders<String>> iterator = store.all()) {
            mockTime.sleep(3);
        }

        assertEquals(2.5 * TimeUnit.MILLISECONDS.toNanos(1), (double) iteratorDurationAvg.metricValue());
        assertEquals(3.0 * TimeUnit.MILLISECONDS.toNanos(1), (double) iteratorDurationMax.metricValue());
    }

    // The above shouldTimeIteratorDuration goes through store.all() -> the KeyValueIterator sibling.
    // This pins the same close()-path recording for the ReadOnlyRecordIterator that backs
    // TimestampedWindowKeyWithHeadersQuery, whose close() records both the operation sensor (fetch)
    // and the iterator-duration sensor via the shared AbstractMeteredIterator lifecycle.
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void shouldTimeIteratorDurationForTimestampedWindowKeyWithHeadersQuery() {
        setUp();
        store.init(context, store);
        when(innerStoreMock.query(any(), any(PositionBound.class), any(QueryConfig.class)))
            .thenReturn(
                (QueryResult) QueryResult.forResult(windowKeyIterator(List.of())),
                (QueryResult) QueryResult.forResult(windowKeyIterator(List.of())));

        final KafkaMetric iteratorDurationAvg = metric("iterator-duration-avg");
        final KafkaMetric iteratorDurationMax = metric("iterator-duration-max");
        final KafkaMetric fetchLatencyAvg = metric("fetch-latency-avg");
        assertEquals(Double.NaN, (Double) iteratorDurationAvg.metricValue());
        assertEquals(Double.NaN, (Double) iteratorDurationMax.metricValue());

        // Two samples (2ms then 3ms) so avg (2.5ms) and max (3ms) differ -- one sample would leave them
        // identical and not actually pin avg. Mirrors the sibling shouldTimeIteratorDuration above.
        try (ReadOnlyRecordIterator<Windowed<String>, String> iterator = store.query(
                TimestampedWindowKeyWithHeadersQuery.<String, String>withKeyAndWindowStartRange(
                    KEY, Instant.ofEpochMilli(5), Instant.ofEpochMilli(100)),
                PositionBound.unbounded(), new QueryConfig(false)).getResult()) {
            mockTime.sleep(2);
        }

        assertEquals(2.0 * TimeUnit.MILLISECONDS.toNanos(1), (double) iteratorDurationAvg.metricValue());
        assertEquals(2.0 * TimeUnit.MILLISECONDS.toNanos(1), (double) iteratorDurationMax.metricValue());

        try (ReadOnlyRecordIterator<Windowed<String>, String> iterator = store.query(
                TimestampedWindowKeyWithHeadersQuery.<String, String>withKeyAndWindowStartRange(
                    KEY, Instant.ofEpochMilli(5), Instant.ofEpochMilli(100)),
                PositionBound.unbounded(), new QueryConfig(false)).getResult()) {
            mockTime.sleep(3);
        }

        assertEquals(2.5 * TimeUnit.MILLISECONDS.toNanos(1), (double) iteratorDurationAvg.metricValue());
        assertEquals(3.0 * TimeUnit.MILLISECONDS.toNanos(1), (double) iteratorDurationMax.metricValue());
        // fetchSensor is recorded only from the iterator's close() on this path, so the two samples
        // (2ms, 3ms) average to exactly 2.5ms.
        assertEquals(2.5 * TimeUnit.MILLISECONDS.toNanos(1), (double) fetchLatencyAvg.metricValue());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void shouldLeaveIteratorOpenWhenNextThrowsAndNotClosed() {
        setUp();
        store.init(context, store);
        // A stored entry with a negative timestamp cannot be represented as a ReadOnlyRecord, so next() throws.
        final byte[] negativeTimestampBytes = new ValueTimestampHeadersSerializer<>(new StringSerializer())
            .serialize("topic", ValueTimestampHeaders.make("value", -1L, HEADERS));
        when(innerStoreMock.query(any(), any(PositionBound.class), any(QueryConfig.class)))
            .thenReturn((QueryResult) QueryResult.forResult(
                windowKeyIterator(List.of(KeyValue.pair(5L, negativeTimestampBytes)))));

        final KafkaMetric openIterators = numOpenIteratorsMetric();
        final ReadOnlyRecordIterator<Windowed<String>, String> iterator = store.query(
            TimestampedWindowKeyWithHeadersQuery.<String, String>withKeyAndWindowStartRange(
                KEY, Instant.ofEpochMilli(5), Instant.ofEpochMilli(100)),
            PositionBound.unbounded(),
            new QueryConfig(false)).getResult();

        assertEquals(1L, (Long) openIterators.metricValue());
        // next() throws on the negative-timestamp entry but does not close the iterator, so it stays
        // registered (num-open-iterators stays incremented) until the caller closes it.
        assertThrows(StreamsException.class, iterator::next);
        assertEquals(1L, (Long) openIterators.metricValue());
        iterator.close();
        assertEquals(0L, (Long) openIterators.metricValue());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private WindowKeyQuery<?, ?> forwardedRawWindowKeyQuery(final Query<?> query) {
        when(innerStoreMock.query(any(), any(PositionBound.class), any(QueryConfig.class)))
            .thenReturn((QueryResult) QueryResult.forResult(windowKeyIterator(List.of())));
        // Close the iterator the query opens so num-open-iterators returns to 0.
        ((ReadOnlyRecordIterator<?, ?>) store.query(query, PositionBound.unbounded(), new QueryConfig(false)).getResult()).close();
        final ArgumentCaptor<WindowKeyQuery> captor = ArgumentCaptor.forClass(WindowKeyQuery.class);
        verify(innerStoreMock).query(captor.capture(), any(PositionBound.class), any(QueryConfig.class));
        return captor.getValue();
    }

    private static WindowStoreIterator<byte[]> windowKeyIterator(final List<KeyValue<Long, byte[]>> data) {
        final Iterator<KeyValue<Long, byte[]>> iterator = data.iterator();
        return new WindowStoreIterator<>() {
            @Override
            public void close() { }

            @Override
            public Long peekNextKey() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public KeyValue<Long, byte[]> next() {
                return iterator.next();
            }
        };
    }

    @Test
    public void shouldPropagateWindowRangeBoundsForTimestampedWindowRangeWithHeadersQuery() {
        setUp();
        store.init(context, store);

        final Instant timeFrom = Instant.ofEpochMilli(5);
        final Instant timeTo = Instant.ofEpochMilli(100);
        final WindowRangeQuery<?, ?> rawQuery = forwardedRawRangeQuery(
            TimestampedWindowRangeWithHeadersQuery.<String, String>withWindowStartRange(timeFrom, timeTo));

        // The typed query is translated into a raw byte-level WindowRangeQuery.withWindowStartRange
        // with the same window-start range, forwarded to the wrapped store.
        assertEquals(Optional.empty(), rawQuery.getKey());
        assertEquals(Optional.of(timeFrom), rawQuery.getTimeFrom());
        assertEquals(Optional.of(timeTo), rawQuery.getTimeTo());
    }

    @Test
    public void shouldRejectWithKeyFormForTimestampedWindowRangeWithHeadersQuery() {
        setUp();
        store.init(context, store);

        final QueryResult<ReadOnlyRecordIterator<Windowed<String>, String>> result = store.query(
            TimestampedWindowRangeWithHeadersQuery.<String, String>withKey(KEY),
            PositionBound.unbounded(),
            new QueryConfig(false));

        assertFalse(result.isSuccess());
        assertEquals(FailureReason.UNKNOWN_QUERY_TYPE, result.getFailureReason());
        assertTrue(
            result.getFailureMessage().contains("WindowStores only supports TimestampedWindowRangeWithHeadersQuery.withWindowStartRange"),
            "unexpected message: " + result.getFailureMessage());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void shouldPropagateWrappedStoreFailureForTimestampedWindowRangeWithHeadersQuery() {
        setUp();
        store.init(context, store);
        when(innerStoreMock.query(any(), any(PositionBound.class), any(QueryConfig.class)))
                .thenReturn((QueryResult) QueryResult.forFailure(FailureReason.STORE_EXCEPTION, "boom"));

        final QueryResult<ReadOnlyRecordIterator<Windowed<String>, String>> result = store.query(
                TimestampedWindowRangeWithHeadersQuery.<String, String>withWindowStartRange(
                        Instant.ofEpochMilli(5), Instant.ofEpochMilli(100)),
                PositionBound.unbounded(),
                new QueryConfig(false));

        assertFalse(result.isSuccess());
        assertEquals(FailureReason.STORE_EXCEPTION, result.getFailureReason());
        assertEquals("boom", result.getFailureMessage());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void shouldTrackNumOpenIteratorsForTimestampedWindowRangeWithHeadersQuery() {
        setUp();
        store.init(context, store);
        when(innerStoreMock.query(any(), any(PositionBound.class), any(QueryConfig.class)))
            .thenReturn((QueryResult) QueryResult.forResult(windowRangeIterator(List.of())));

        final KafkaMetric openIterators = numOpenIteratorsMetric();
        assertEquals(0L, (Long) openIterators.metricValue());

        final QueryResult<ReadOnlyRecordIterator<Windowed<String>, String>> result = store.query(
            TimestampedWindowRangeWithHeadersQuery.<String, String>withWindowStartRange(
                Instant.ofEpochMilli(5), Instant.ofEpochMilli(100)),
            PositionBound.unbounded(),
            new QueryConfig(false));
        assertTrue(result.isSuccess());

        // The query's ReadOnlyRecordIterator registers itself on open and deregisters on close.
        try (ReadOnlyRecordIterator<Windowed<String>, String> iterator = result.getResult()) {
            assertEquals(1L, (Long) openIterators.metricValue());
        }
        assertEquals(0L, (Long) openIterators.metricValue());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void shouldDecrementOpenIteratorsTwiceWhenClosedTwiceForTimestampedWindowRangeWithHeadersQuery() {
        setUp();
        store.init(context, store);
        when(innerStoreMock.query(any(), any(PositionBound.class), any(QueryConfig.class)))
            .thenReturn((QueryResult) QueryResult.forResult(windowRangeIterator(List.of())));

        final KafkaMetric openIterators = numOpenIteratorsMetric();
        final ReadOnlyRecordIterator<Windowed<String>, String> iterator = store.query(
            TimestampedWindowRangeWithHeadersQuery.<String, String>withWindowStartRange(
                Instant.ofEpochMilli(5), Instant.ofEpochMilli(100)),
            PositionBound.unbounded(),
            new QueryConfig(false)).getResult();

        assertEquals(1L, (Long) openIterators.metricValue());
        iterator.close();
        assertEquals(0L, (Long) openIterators.metricValue());
        // close() is intentionally not idempotent (matching the sibling metered iterators): each call
        // decrements, so a repeated close drives the gauge below zero. Callers must close exactly once.
        iterator.close();
        assertEquals(-1L, (Long) openIterators.metricValue());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void shouldLeaveIteratorOpenWhenNextThrowsAndNotClosedForTimestampedWindowRangeWithHeadersQuery() {
        setUp();
        store.init(context, store);
        // A stored entry with a negative timestamp cannot be represented as a ReadOnlyRecord, so next() throws.
        final byte[] negativeTimestampBytes = new ValueTimestampHeadersSerializer<>(new StringSerializer())
            .serialize("topic", ValueTimestampHeaders.make("value", -1L, HEADERS));
        final Windowed<Bytes> windowedKeyBytes = new Windowed<>(KEY_BYTES, new TimeWindow(5L, 5L + WINDOW_SIZE_MS));
        when(innerStoreMock.query(any(), any(PositionBound.class), any(QueryConfig.class)))
            .thenReturn((QueryResult) QueryResult.forResult(
                windowRangeIterator(List.of(KeyValue.pair(windowedKeyBytes, negativeTimestampBytes)))));

        final KafkaMetric openIterators = numOpenIteratorsMetric();
        final ReadOnlyRecordIterator<Windowed<String>, String> iterator = store.query(
            TimestampedWindowRangeWithHeadersQuery.<String, String>withWindowStartRange(
                Instant.ofEpochMilli(5), Instant.ofEpochMilli(100)),
            PositionBound.unbounded(),
            new QueryConfig(false)).getResult();

        assertEquals(1L, (Long) openIterators.metricValue());
        assertThrows(StreamsException.class, iterator::next);
        assertEquals(1L, (Long) openIterators.metricValue());
        iterator.close();
        assertEquals(0L, (Long) openIterators.metricValue());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void shouldUseWindowFromRawResultForTimestampedWindowRangeWithHeadersQuery() {
        setUp();
        store.init(context, store);
        // Deliberately a window whose length differs from WINDOW_SIZE_MS, proving the returned
        // Windowed<K> comes straight from the raw range result rather than being reconstructed from
        // windowSizeMs (unlike the single-key point query, which only gets a window-start Long back).
        final Windowed<Bytes> windowedKeyBytes = new Windowed<>(KEY_BYTES, new TimeWindow(1_000L, 5_000L));
        final KeyValue<Windowed<Bytes>, byte[]> testData = KeyValue.pair(windowedKeyBytes, VALUE_TIMESTAMP_HEADERS_BYTES);
        when(innerStoreMock.query(any(), any(PositionBound.class), any(QueryConfig.class)))
            .thenReturn((QueryResult) QueryResult.forResult(windowRangeIterator(List.of(testData))));

        final QueryResult<ReadOnlyRecordIterator<Windowed<String>, String>> result = store.query(
            TimestampedWindowRangeWithHeadersQuery.<String, String>withWindowStartRange(
                Instant.ofEpochMilli(0), Instant.ofEpochMilli(10_000)),
            PositionBound.unbounded(),
            new QueryConfig(false));

        assertTrue(result.isSuccess());
        try (ReadOnlyRecordIterator<Windowed<String>, String> iterator = result.getResult()) {
            assertTrue(iterator.hasNext());
            final ReadOnlyRecord<Windowed<String>, String> record = iterator.next();
            assertEquals(KEY, record.key().key());
            assertEquals(1_000L, record.key().window().start());
            assertEquals(5_000L, record.key().window().end());
            assertEquals("value", record.value());
            assertEquals(TIMESTAMP, record.timestamp());
            assertEquals(HEADERS, record.headers());
            // returned headers are a read-only snapshot: neither add nor remove is allowed
            assertThrows(IllegalStateException.class, () -> record.headers().add("x", new byte[0]));
            assertThrows(IllegalStateException.class, () -> record.headers().remove("header-key"));
            assertFalse(iterator.hasNext());
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void shouldThrowForNullStoredValueForTimestampedWindowRangeWithHeadersQuery() {
        setUp();
        store.init(context, store);

        // A ReadOnlyRecord carries the stored event-time; a value that deserializes to null has none,
        // so the entry cannot be represented and next() throws (rather than NPE-ing on the null value).
        final Windowed<Bytes> windowedKeyBytes = new Windowed<>(KEY_BYTES, new TimeWindow(1_000L, 5_000L));
        when(innerStoreMock.query(any(), any(PositionBound.class), any(QueryConfig.class)))
            .thenReturn((QueryResult) QueryResult.forResult(
                windowRangeIterator(List.of(KeyValue.pair(windowedKeyBytes, (byte[]) null)))));

        final QueryResult<ReadOnlyRecordIterator<Windowed<String>, String>> result = store.query(
            TimestampedWindowRangeWithHeadersQuery.<String, String>withWindowStartRange(
                Instant.ofEpochMilli(0), Instant.ofEpochMilli(10_000)),
            PositionBound.unbounded(),
            new QueryConfig(false));

        assertTrue(result.isSuccess());
        try (ReadOnlyRecordIterator<Windowed<String>, String> iterator = result.getResult()) {
            assertTrue(iterator.hasNext());
            final StreamsException exception = assertThrows(StreamsException.class, iterator::next);
            assertTrue(exception.getMessage().contains("its value is null"), exception.getMessage());
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private WindowRangeQuery<?, ?> forwardedRawRangeQuery(final Query<?> query) {
        when(innerStoreMock.query(any(), any(PositionBound.class), any(QueryConfig.class)))
            .thenReturn((QueryResult) QueryResult.forResult(windowRangeIterator(List.of())));
        // Close the iterator the query opens so num-open-iterators returns to 0.
        ((ReadOnlyRecordIterator<?, ?>) store.query(query, PositionBound.unbounded(), new QueryConfig(false)).getResult()).close();
        final ArgumentCaptor<WindowRangeQuery> captor = ArgumentCaptor.forClass(WindowRangeQuery.class);
        verify(innerStoreMock).query(captor.capture(), any(PositionBound.class), any(QueryConfig.class));
        return captor.getValue();
    }

    private static KeyValueIterator<Windowed<Bytes>, byte[]> windowRangeIterator(final List<KeyValue<Windowed<Bytes>, byte[]>> data) {
        return new KeyValueIteratorStub<>(data.iterator());
    }

    private KafkaMetric numOpenIteratorsMetric() {
        return metric("num-open-iterators");
    }

    private KafkaMetric metric(final String name) {
        return metrics.metrics().entrySet().stream()
                .filter(entry -> entry.getKey().name().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError(name + " metric not registered"))
                .getValue();
    }
}
