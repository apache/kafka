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

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.AggregationWithHeaders;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.test.KeyValueIteratorStub;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class MeteredSessionStoreWithHeadersTest {

    private static final String APPLICATION_ID = "test-app";
    private static final String STORE_TYPE = "scope";
    private static final String STORE_NAME = "mocked-store";
    private static final String STORE_LEVEL_GROUP = "stream-state-metrics";
    private static final String THREAD_ID_TAG_KEY = "thread-id";
    private static final String CHANGELOG_TOPIC = "changelog-topic";
    private static final String KEY = "key";
    private static final Bytes KEY_BYTES = Bytes.wrap(KEY.getBytes());
    private static final Windowed<String> WINDOWED_KEY = new Windowed<>(KEY, new SessionWindow(0, 0));
    private static final Windowed<Bytes> WINDOWED_KEY_BYTES = new Windowed<>(KEY_BYTES, new SessionWindow(0, 0));
    private static final String VALUE = "value";
    private static final long START_TIMESTAMP = 24L;
    private static final long END_TIMESTAMP = 42L;

    private final String threadId = Thread.currentThread().getName();
    private final TaskId taskId = new TaskId(0, 0, "My-Topology");
    private final Metrics metrics = new Metrics();
    private MockTime mockTime;
    private MeteredSessionStoreWithHeaders<String, String> store;

    @Mock
    private SessionStore<Bytes, byte[]> innerStore;

    @Mock
    private InternalProcessorContext<?, ?> context;

    private Map<String, String> tags;

    private void setUp() {
        mockTime = new MockTime();
        store = new MeteredSessionStoreWithHeaders<>(
            innerStore,
            STORE_TYPE,
            Serdes.String(),
            createAggregationWithHeadersSerde(Serdes.String()),
            mockTime
        );
        tags = mkMap(
            mkEntry(THREAD_ID_TAG_KEY, threadId),
            mkEntry("task-id", taskId.toString()),
            mkEntry(STORE_TYPE + "-state-id", STORE_NAME)
        );

        metrics.config().recordLevel(Sensor.RecordingLevel.DEBUG);
        when(context.applicationId()).thenReturn(APPLICATION_ID);
        when(context.metrics())
            .thenReturn(new StreamsMetricsImpl(metrics, "test", mockTime));
        when(context.taskId()).thenReturn(taskId);
        when(context.changelogFor(STORE_NAME)).thenReturn(CHANGELOG_TOPIC);
        when(innerStore.name()).thenReturn(STORE_NAME);
    }

    private void init() {
        store.init(context, store);
    }

    private KafkaMetric metric(final String name) {
        return this.metrics.metric(new MetricName(name, STORE_LEVEL_GROUP, "", this.tags));
    }

    private List<MetricName> storeMetrics() {
        return metrics.metrics()
            .keySet()
            .stream()
            .filter(name -> name.group().equals(STORE_LEVEL_GROUP) && name.tags().equals(tags))
            .collect(Collectors.toList());
    }

    private <AGG> Serde<AggregationWithHeaders<AGG>> createAggregationWithHeadersSerde(final Serde<AGG> aggSerde) {
        return new Serde<>() {
            @Override
            public Serializer<AggregationWithHeaders<AGG>> serializer() {
                return new AggregationWithHeadersSerializer<>(aggSerde.serializer());
            }

            @Override
            public Deserializer<AggregationWithHeaders<AGG>> deserializer() {
                return new AggregationWithHeadersDeserializer<>(aggSerde.deserializer());
            }
        };
    }

    @Test
    public void shouldDelegateInit() {
        setUp();
        final MeteredSessionStoreWithHeaders<String, String> outer = new MeteredSessionStoreWithHeaders<>(
            innerStore,
            STORE_TYPE,
            Serdes.String(),
            createAggregationWithHeadersSerde(Serdes.String()),
            new MockTime()
        );
        doNothing().when(innerStore).init(context, outer);
        outer.init(context, outer);
    }

    @Test
    public void testMetrics() {
        setUp();
        init();
        final JmxReporter reporter = new JmxReporter();
        final MetricsContext metricsContext = new KafkaMetricsContext("kafka.streams");
        reporter.contextChange(metricsContext);

        metrics.addReporter(reporter);
        assertTrue(reporter.containsMbean(String.format(
            "kafka.streams:type=%s,%s=%s,task-id=%s,%s-state-id=%s",
            STORE_LEVEL_GROUP,
            THREAD_ID_TAG_KEY,
            threadId,
            taskId,
            STORE_TYPE,
            STORE_NAME
        )));
    }

    @Test
    public void shouldWriteBytesToInnerStoreAndRecordPutMetric() {
        setUp();
        init();

        final Headers headers = new RecordHeaders();
        headers.add("key1", "value1".getBytes());
        final AggregationWithHeaders<String> valueAndHeaders = AggregationWithHeaders.make(VALUE, headers);

        doNothing().when(innerStore).put(any(Windowed.class), any(byte[].class));

        store.put(WINDOWED_KEY, valueAndHeaders);

        final ArgumentCaptor<byte[]> byteCaptor = ArgumentCaptor.forClass(byte[].class);
        verify(innerStore).put(any(Windowed.class), byteCaptor.capture());

        final AggregationWithHeadersDeserializer<String> deserializer =
            new AggregationWithHeadersDeserializer<>(Serdes.String().deserializer());
        final AggregationWithHeaders<String> deserialized = deserializer.deserialize(CHANGELOG_TOPIC, byteCaptor.getValue());
        assertEquals(VALUE, deserialized.aggregation());
        assertNotNull(deserialized.headers());
        assertEquals("value1", new String(deserialized.headers().lastHeader("key1").value()));

        final KafkaMetric metric = metric("put-rate");
        assertTrue((Double) metric.metricValue() > 0);
    }

    @Test
    public void shouldFetchSessionAndReturnValueWithHeaders() {
        setUp();
        init();

        final Headers headers = new RecordHeaders();
        headers.add("key1", "value1".getBytes());
        final AggregationWithHeaders<String> valueAndHeaders = AggregationWithHeaders.make(VALUE, headers);

        final AggregationWithHeadersSerializer<String> serializer = new AggregationWithHeadersSerializer<>(Serdes.String().serializer());
        final byte[] serializedValue = serializer.serialize(CHANGELOG_TOPIC, valueAndHeaders);

        when(innerStore.fetchSession(KEY_BYTES, START_TIMESTAMP, END_TIMESTAMP))
            .thenReturn(serializedValue);

        final AggregationWithHeaders<String> result = store.fetchSession(KEY, START_TIMESTAMP, END_TIMESTAMP);

        assertNotNull(result);
        assertEquals(VALUE, result.aggregation());
        assertNotNull(result.headers());
        assertEquals("value1", new String(result.headers().lastHeader("key1").value()));

        final KafkaMetric metric = metric("fetch-rate");
        assertTrue((Double) metric.metricValue() > 0);
    }

    @Test
    public void shouldFindSessionsFromStoreAndRecordFetchMetric() {
        setUp();
        init();

        final Headers headers = new RecordHeaders();
        headers.add("key1", "value1".getBytes());
        final AggregationWithHeaders<String> valueAndHeaders = AggregationWithHeaders.make(VALUE, headers);

        final AggregationWithHeadersSerializer<String> serializer = new AggregationWithHeadersSerializer<>(Serdes.String().serializer());
        final byte[] serializedValue = serializer.serialize(CHANGELOG_TOPIC, valueAndHeaders);

        when(innerStore.findSessions(KEY_BYTES, 0, 0))
            .thenReturn(new KeyValueIteratorStub<>(
                Collections.singleton(KeyValue.pair(WINDOWED_KEY_BYTES, serializedValue)).iterator()));

        final KeyValueIterator<Windowed<String>, AggregationWithHeaders<String>> iterator = store.findSessions(KEY, 0, 0);

        assertTrue(iterator.hasNext());
        final KeyValue<Windowed<String>, AggregationWithHeaders<String>> next = iterator.next();
        assertEquals(VALUE, next.value.aggregation());
        assertNotNull(next.value.headers());
        assertEquals("value1", new String(next.value.headers().lastHeader("key1").value()));
        assertFalse(iterator.hasNext());
        iterator.close();

        final KafkaMetric metric = metric("fetch-rate");
        assertTrue((Double) metric.metricValue() > 0);
    }

    @Test
    public void shouldBackwardFindSessionsFromStoreAndRecordFetchMetric() {
        setUp();
        init();

        final Headers headers = new RecordHeaders();
        headers.add("key1", "value1".getBytes());
        final AggregationWithHeaders<String> valueAndHeaders = AggregationWithHeaders.make(VALUE, headers);

        final AggregationWithHeadersSerializer<String> serializer = new AggregationWithHeadersSerializer<>(Serdes.String().serializer());
        final byte[] serializedValue = serializer.serialize(CHANGELOG_TOPIC, valueAndHeaders);

        when(innerStore.backwardFindSessions(KEY_BYTES, 0, 0))
            .thenReturn(new KeyValueIteratorStub<>(
                Collections.singleton(KeyValue.pair(WINDOWED_KEY_BYTES, serializedValue)).iterator()));

        final KeyValueIterator<Windowed<String>, AggregationWithHeaders<String>> iterator = store.backwardFindSessions(KEY, 0, 0);

        assertTrue(iterator.hasNext());
        final KeyValue<Windowed<String>, AggregationWithHeaders<String>> next = iterator.next();
        assertEquals(VALUE, next.value.aggregation());
        assertNotNull(next.value.headers());
        assertEquals("value1", new String(next.value.headers().lastHeader("key1").value()));
        assertFalse(iterator.hasNext());
        iterator.close();

        final KafkaMetric metric = metric("fetch-rate");
        assertTrue((Double) metric.metricValue() > 0);
    }

    @Test
    public void shouldFindSessionRangeFromStoreAndRecordFetchMetric() {
        setUp();
        init();

        final Headers headers = new RecordHeaders();
        headers.add("key1", "value1".getBytes());
        final AggregationWithHeaders<String> valueAndHeaders = AggregationWithHeaders.make(VALUE, headers);

        final AggregationWithHeadersSerializer<String> serializer = new AggregationWithHeadersSerializer<>(Serdes.String().serializer());
        final byte[] serializedValue = serializer.serialize(CHANGELOG_TOPIC, valueAndHeaders);

        when(innerStore.findSessions(KEY_BYTES, KEY_BYTES, 0, 0))
            .thenReturn(new KeyValueIteratorStub<>(
                Collections.singleton(KeyValue.pair(WINDOWED_KEY_BYTES, serializedValue)).iterator()));

        final KeyValueIterator<Windowed<String>, AggregationWithHeaders<String>> iterator = store.findSessions(KEY, KEY, 0, 0);

        assertTrue(iterator.hasNext());
        final KeyValue<Windowed<String>, AggregationWithHeaders<String>> next = iterator.next();
        assertEquals(VALUE, next.value.aggregation());
        assertNotNull(next.value.headers());
        assertEquals("value1", new String(next.value.headers().lastHeader("key1").value()));
        assertFalse(iterator.hasNext());
        iterator.close();

        final KafkaMetric metric = metric("fetch-rate");
        assertTrue((Double) metric.metricValue() > 0);
    }

    @Test
    public void shouldRemoveFromStoreAndRecordRemoveMetric() {
        setUp();
        init();

        doNothing().when(innerStore).remove(WINDOWED_KEY_BYTES);

        store.remove(new Windowed<>(KEY, new SessionWindow(0, 0)));

        verify(innerStore).remove(any(Windowed.class));

        final KafkaMetric metric = metric("remove-rate");
        assertTrue((Double) metric.metricValue() > 0);
    }

    @Test
    public void shouldFetchForKeyAndRecordFetchMetric() {
        setUp();
        init();

        final Headers headers = new RecordHeaders();
        headers.add("key1", "value1".getBytes());
        final AggregationWithHeaders<String> valueAndHeaders = AggregationWithHeaders.make(VALUE, headers);

        final AggregationWithHeadersSerializer<String> serializer = new AggregationWithHeadersSerializer<>(Serdes.String().serializer());
        final byte[] serializedValue = serializer.serialize(CHANGELOG_TOPIC, valueAndHeaders);

        when(innerStore.fetch(KEY_BYTES))
            .thenReturn(new KeyValueIteratorStub<>(
                Collections.singleton(KeyValue.pair(WINDOWED_KEY_BYTES, serializedValue)).iterator()));

        final KeyValueIterator<Windowed<String>, AggregationWithHeaders<String>> iterator = store.fetch(KEY);

        assertTrue(iterator.hasNext());
        final KeyValue<Windowed<String>, AggregationWithHeaders<String>> next = iterator.next();
        assertEquals(VALUE, next.value.aggregation());
        assertNotNull(next.value.headers());
        assertEquals("value1", new String(next.value.headers().lastHeader("key1").value()));
        assertFalse(iterator.hasNext());
        iterator.close();

        final KafkaMetric metric = metric("fetch-rate");
        assertTrue((Double) metric.metricValue() > 0);
    }

    @Test
    public void shouldBackwardFetchForKeyAndRecordFetchMetric() {
        setUp();
        init();

        final Headers headers = new RecordHeaders();
        headers.add("key1", "value1".getBytes());
        final AggregationWithHeaders<String> valueAndHeaders = AggregationWithHeaders.make(VALUE, headers);

        final AggregationWithHeadersSerializer<String> serializer = new AggregationWithHeadersSerializer<>(Serdes.String().serializer());
        final byte[] serializedValue = serializer.serialize(CHANGELOG_TOPIC, valueAndHeaders);

        when(innerStore.backwardFetch(KEY_BYTES))
            .thenReturn(new KeyValueIteratorStub<>(
                Collections.singleton(KeyValue.pair(WINDOWED_KEY_BYTES, serializedValue)).iterator()));

        final KeyValueIterator<Windowed<String>, AggregationWithHeaders<String>> iterator = store.backwardFetch(KEY);

        assertTrue(iterator.hasNext());
        final KeyValue<Windowed<String>, AggregationWithHeaders<String>> next = iterator.next();
        assertEquals(VALUE, next.value.aggregation());
        assertNotNull(next.value.headers());
        assertEquals("value1", new String(next.value.headers().lastHeader("key1").value()));
        assertFalse(iterator.hasNext());
        iterator.close();

        final KafkaMetric metric = metric("fetch-rate");
        assertTrue((Double) metric.metricValue() > 0);
    }

    @Test
    public void shouldFetchRangeFromStoreAndRecordFetchMetric() {
        setUp();
        init();

        final Headers headers = new RecordHeaders();
        headers.add("key1", "value1".getBytes());
        final AggregationWithHeaders<String> valueAndHeaders = AggregationWithHeaders.make(VALUE, headers);

        final AggregationWithHeadersSerializer<String> serializer = new AggregationWithHeadersSerializer<>(Serdes.String().serializer());
        final byte[] serializedValue = serializer.serialize(CHANGELOG_TOPIC, valueAndHeaders);

        when(innerStore.fetch(KEY_BYTES, KEY_BYTES))
            .thenReturn(new KeyValueIteratorStub<>(
                Collections.singleton(KeyValue.pair(WINDOWED_KEY_BYTES, serializedValue)).iterator()));

        final KeyValueIterator<Windowed<String>, AggregationWithHeaders<String>> iterator = store.fetch(KEY, KEY);

        assertTrue(iterator.hasNext());
        final KeyValue<Windowed<String>, AggregationWithHeaders<String>> next = iterator.next();
        assertEquals(VALUE, next.value.aggregation());
        assertNotNull(next.value.headers());
        assertEquals("value1", new String(next.value.headers().lastHeader("key1").value()));
        assertFalse(iterator.hasNext());
        iterator.close();

        final KafkaMetric metric = metric("fetch-rate");
        assertTrue((Double) metric.metricValue() > 0);
    }

    @Test
    public void shouldReturnNullOnFetchSessionWhenSessionDoesNotExist() {
        setUp();
        init();

        when(innerStore.fetchSession(KEY_BYTES, START_TIMESTAMP, END_TIMESTAMP))
            .thenReturn(null);

        final AggregationWithHeaders<String> result = store.fetchSession(KEY, START_TIMESTAMP, END_TIMESTAMP);

        assertNull(result);
    }

    @Test
    public void shouldRecordRestoreTime() {
        setUp();
        init();

        store.recordRestoreTime(100L);

        final Map<MetricName, ? extends org.apache.kafka.common.Metric> allMetrics = metrics.metrics();
        final List<MetricName> restoreMetrics = allMetrics.keySet().stream()
            .filter(metricName -> metricName.name().equals("restore-rate"))
            .collect(Collectors.toList());

        assertThat(restoreMetrics, not(empty()));
    }

    @Test
    public void shouldCloseInnerStore() {
        setUp();
        init();

        doNothing().when(innerStore).close();

        store.close();

        verify(innerStore).close();
    }

    @Test
    public void shouldSetFlushListenerOnWrappedCachingStore() {
        final CachingSessionStore cachedSessionStore = mock(CachingSessionStore.class);

        when(cachedSessionStore.setFlushListener(any(CacheFlushListener.class), any(Boolean.class)))
            .thenReturn(true);

        final MeteredSessionStoreWithHeaders<String, String> cachedStore = new MeteredSessionStoreWithHeaders<>(
            cachedSessionStore,
            STORE_TYPE,
            Serdes.String(),
            createAggregationWithHeadersSerde(Serdes.String()),
            new MockTime()
        );

        assertTrue(cachedStore.setFlushListener(null, false));
    }

    @Test
    public void shouldBackwardFetchRangeFromStoreAndRecordFetchMetric() {
        setUp();
        init();

        final Headers headers = new RecordHeaders();
        headers.add("key1", "value1".getBytes());
        final AggregationWithHeaders<String> valueAndHeaders = AggregationWithHeaders.make(VALUE, headers);

        final AggregationWithHeadersSerializer<String> serializer = new AggregationWithHeadersSerializer<>(Serdes.String().serializer());
        final byte[] serializedValue = serializer.serialize(CHANGELOG_TOPIC, valueAndHeaders);

        when(innerStore.backwardFetch(KEY_BYTES, KEY_BYTES))
            .thenReturn(new KeyValueIteratorStub<>(
                Collections.singleton(KeyValue.pair(WINDOWED_KEY_BYTES, serializedValue)).iterator()));

        final KeyValueIterator<Windowed<String>, AggregationWithHeaders<String>> iterator = store.backwardFetch(KEY, KEY);

        assertTrue(iterator.hasNext());
        final KeyValue<Windowed<String>, AggregationWithHeaders<String>> next = iterator.next();
        assertEquals(VALUE, next.value.aggregation());
        assertNotNull(next.value.headers());
        assertEquals("value1", new String(next.value.headers().lastHeader("key1").value()));
        assertFalse(iterator.hasNext());
        iterator.close();

        final KafkaMetric metric = metric("fetch-rate");
        assertTrue((Double) metric.metricValue() > 0);
    }

    @Test
    public void shouldBackwardFindSessionRangeFromStoreAndRecordFetchMetric() {
        setUp();
        init();

        final Headers headers = new RecordHeaders();
        headers.add("key1", "value1".getBytes());
        final AggregationWithHeaders<String> valueAndHeaders = AggregationWithHeaders.make(VALUE, headers);

        final AggregationWithHeadersSerializer<String> serializer = new AggregationWithHeadersSerializer<>(Serdes.String().serializer());
        final byte[] serializedValue = serializer.serialize(CHANGELOG_TOPIC, valueAndHeaders);

        when(innerStore.backwardFindSessions(KEY_BYTES, KEY_BYTES, 0, 0))
            .thenReturn(new KeyValueIteratorStub<>(
                Collections.singleton(KeyValue.pair(WINDOWED_KEY_BYTES, serializedValue)).iterator()));

        final KeyValueIterator<Windowed<String>, AggregationWithHeaders<String>> iterator = store.backwardFindSessions(KEY, KEY, 0, 0);

        assertTrue(iterator.hasNext());
        final KeyValue<Windowed<String>, AggregationWithHeaders<String>> next = iterator.next();
        assertEquals(VALUE, next.value.aggregation());
        assertNotNull(next.value.headers());
        assertEquals("value1", new String(next.value.headers().lastHeader("key1").value()));
        assertFalse(iterator.hasNext());
        iterator.close();

        final KafkaMetric metric = metric("fetch-rate");
        assertTrue((Double) metric.metricValue() > 0);
    }

    @Test
    public void shouldTrackOpenIteratorsMetric() {
        setUp();
        init();

        final Headers headers = new RecordHeaders();
        headers.add("key1", "value1".getBytes());
        final AggregationWithHeaders<String> valueAndHeaders = AggregationWithHeaders.make(VALUE, headers);

        final AggregationWithHeadersSerializer<String> serializer = new AggregationWithHeadersSerializer<>(Serdes.String().serializer());
        final byte[] serializedValue = serializer.serialize(CHANGELOG_TOPIC, valueAndHeaders);

        when(innerStore.fetch(KEY_BYTES))
            .thenReturn(new KeyValueIteratorStub<>(
                Collections.singleton(KeyValue.pair(WINDOWED_KEY_BYTES, serializedValue)).iterator()));

        final KafkaMetric openIteratorsMetric = metric("num-open-iterators");
        assertNotNull(openIteratorsMetric);

        assertThat((Long) openIteratorsMetric.metricValue(), equalTo(0L));

        final KeyValueIterator<Windowed<String>, AggregationWithHeaders<String>> iterator = store.fetch(KEY);

        assertThat((Long) openIteratorsMetric.metricValue(), equalTo(1L));

        iterator.close();

        assertThat((Long) openIteratorsMetric.metricValue(), equalTo(0L));
    }

    @Test
    public void shouldTrackOldestOpenIteratorTimestamp() {
        setUp();
        init();

        final Headers headers = new RecordHeaders();
        headers.add("key1", "value1".getBytes());
        final AggregationWithHeaders<String> valueAndHeaders = AggregationWithHeaders.make(VALUE, headers);

        final AggregationWithHeadersSerializer<String> serializer = new AggregationWithHeadersSerializer<>(Serdes.String().serializer());
        final byte[] serializedValue = serializer.serialize(CHANGELOG_TOPIC, valueAndHeaders);

        when(innerStore.fetch(KEY_BYTES))
            .thenReturn(new KeyValueIteratorStub<>(
                Collections.singleton(KeyValue.pair(WINDOWED_KEY_BYTES, serializedValue)).iterator()));

        final KafkaMetric oldestIteratorMetric = metric("oldest-iterator-open-since-ms");
        assertNotNull(oldestIteratorMetric);

        assertThat(oldestIteratorMetric.metricValue(), equalTo(0L));

        final long beforeOpen = mockTime.milliseconds();
        final KeyValueIterator<Windowed<String>, AggregationWithHeaders<String>> iterator = store.fetch(KEY);
        final long afterOpen = mockTime.milliseconds();

        final long oldestTimestamp = (Long) oldestIteratorMetric.metricValue();
        assertTrue(oldestTimestamp >= beforeOpen && oldestTimestamp <= afterOpen);

        iterator.close();

        assertThat(oldestIteratorMetric.metricValue(), equalTo(0L));
    }

    @Test
    public void shouldTimeIteratorDuration() {
        setUp();
        init();

        final Headers headers = new RecordHeaders();
        headers.add("key1", "value1".getBytes());
        final AggregationWithHeaders<String> valueAndHeaders = AggregationWithHeaders.make(VALUE, headers);

        final AggregationWithHeadersSerializer<String> serializer = new AggregationWithHeadersSerializer<>(Serdes.String().serializer());
        final byte[] serializedValue = serializer.serialize(CHANGELOG_TOPIC, valueAndHeaders);

        when(innerStore.fetch(KEY_BYTES))
            .thenReturn(new KeyValueIteratorStub<>(
                Collections.singleton(KeyValue.pair(WINDOWED_KEY_BYTES, serializedValue)).iterator()));

        final KeyValueIterator<Windowed<String>, AggregationWithHeaders<String>> iterator = store.fetch(KEY);

        mockTime.sleep(100L);

        iterator.close();

        final KafkaMetric iteratorDurationMetric = metric("iterator-duration-avg");
        assertTrue((Double) iteratorDurationMetric.metricValue() > 0.0);
    }

    @Test
    public void shouldRemoveMetricsOnClose() {
        setUp();
        init();

        doNothing().when(innerStore).close();

        assertNotNull(metric("put-rate"));
        assertNotNull(metric("fetch-rate"));

        store.close();

        assertNull(metric("put-rate"));
        assertNull(metric("fetch-rate"));
    }

    @Test
    public void shouldRemoveMetricsEvenIfWrappedStoreThrowsOnClose() {
        setUp();
        doThrow(new RuntimeException("Oops!")).when(innerStore).close();
        init();

        assertThat(storeMetrics(), not(empty()));
        assertThrows(RuntimeException.class, store::close);
        assertThat(storeMetrics(), empty());
    }

    @Test
    public void shouldThrowNullPointerOnPutIfKeyIsNull() {
        setUp();
        init();

        final Headers headers = new RecordHeaders();
        final AggregationWithHeaders<String> valueAndHeaders = AggregationWithHeaders.make(VALUE, headers);

        try {
            store.put(null, valueAndHeaders);
            throw new AssertionError("Should have thrown NullPointerException");
        } catch (final NullPointerException expected) {
            // Expected
        }
    }

    @Test
    public void shouldThrowNullPointerOnPutIfWrappedKeyIsNull() {
        setUp();
        init();

        final Headers headers = new RecordHeaders();
        final AggregationWithHeaders<String> valueAndHeaders = AggregationWithHeaders.make(VALUE, headers);

        try {
            store.put(null, valueAndHeaders);
            throw new AssertionError("Should have thrown NullPointerException");
        } catch (final NullPointerException expected) {
            // Expected
        }
    }

    @Test
    public void shouldThrowNullPointerOnRemoveIfKeyIsNull() {
        setUp();
        init();

        try {
            store.remove(new Windowed<>(null, new SessionWindow(0, 0)));
            throw new AssertionError("Should have thrown NullPointerException");
        } catch (final NullPointerException expected) {
            // Expected
        }
    }

    @Test
    public void shouldThrowNullPointerOnFetchSessionIfKeyIsNull() {
        setUp();
        init();

        try {
            store.fetchSession(null, START_TIMESTAMP, END_TIMESTAMP);
            throw new AssertionError("Should have thrown NullPointerException");
        } catch (final NullPointerException expected) {
            // Expected
        }
    }
}
