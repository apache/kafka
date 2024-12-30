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
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockRecordCollector;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.time.Instant.ofEpochMilli;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class MeteredWindowStoreTest {

    private static final String STORE_TYPE = "scope";
    private static final String STORE_LEVEL_GROUP = "stream-state-metrics";
    private static final String THREAD_ID_TAG_KEY = "thread-id";
    private static final String STORE_NAME = "mocked-store";
    private static final String CHANGELOG_TOPIC = "changelog-topic";
    private static final String KEY = "key";
    private static final Bytes KEY_BYTES = Bytes.wrap(KEY.getBytes());
    private static final String VALUE = "value";
    private static final byte[] VALUE_BYTES = VALUE.getBytes();
    private static final int WINDOW_SIZE_MS = 10;
    private static final int RETENTION_PERIOD = 100;
    private static final long TIMESTAMP = 42L;

    private final String threadId = Thread.currentThread().getName();
    private InternalMockProcessorContext context;
    @SuppressWarnings("unchecked")
    private final WindowStore<Bytes, byte[]> innerStoreMock = mock(WindowStore.class);
    private final MockTime mockTime = new MockTime();
    private MeteredWindowStore<String, String> store = new MeteredWindowStore<>(
        innerStoreMock,
        WINDOW_SIZE_MS, // any size
        STORE_TYPE,
        mockTime,
        Serdes.String(),
        new SerdeThatDoesntHandleNull()
    );
    private final Metrics metrics = new Metrics(new MetricConfig().recordLevel(Sensor.RecordingLevel.DEBUG));
    private Map<String, String> tags;

    {
        when(innerStoreMock.name()).thenReturn(STORE_NAME);
    }

    @BeforeEach
    public void setUp() {
        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, "test", "processId", new MockTime());
        context = new InternalMockProcessorContext<>(
            TestUtils.tempDirectory(),
            Serdes.String(),
            Serdes.Long(),
            streamsMetrics,
            new StreamsConfig(StreamsTestUtils.getStreamsConfig()),
            MockRecordCollector::new,
            new ThreadCache(new LogContext("testCache "), 0, streamsMetrics),
            Time.SYSTEM
        );
        tags = mkMap(
            mkEntry(THREAD_ID_TAG_KEY, threadId),
            mkEntry("task-id", context.taskId().toString()),
            mkEntry(STORE_TYPE + "-state-id", STORE_NAME)
        );
    }

    @Test
    public void shouldDelegateInit() {
        final MeteredWindowStore<String, String> outer = new MeteredWindowStore<>(
            innerStoreMock,
            WINDOW_SIZE_MS, // any size
            STORE_TYPE,
            new MockTime(),
            Serdes.String(),
            new SerdeThatDoesntHandleNull()
        );
        when(innerStoreMock.name()).thenReturn("store");
        doNothing().when(innerStoreMock).init((StateStoreContext) context, outer);
        outer.init((StateStoreContext) context, outer);
    }

    @Test
    public void shouldPassChangelogTopicNameToStateStoreSerde() {
        context.addChangelogForStore(STORE_NAME, CHANGELOG_TOPIC);
        doShouldPassChangelogTopicNameToStateStoreSerde(CHANGELOG_TOPIC);
    }

    @Test
    public void shouldPassDefaultChangelogTopicNameToStateStoreSerdeIfLoggingDisabled() {
        final String defaultChangelogTopicName =
            ProcessorStateManager.storeChangelogTopic(context.applicationId(), STORE_NAME, context.taskId().topologyName());
        doShouldPassChangelogTopicNameToStateStoreSerde(defaultChangelogTopicName);
    }

    @SuppressWarnings("unchecked")
    private void doShouldPassChangelogTopicNameToStateStoreSerde(final String topic) {
        final Serde<String> keySerde = mock(Serde.class);
        final Serializer<String> keySerializer = mock(Serializer.class);
        final Serde<String> valueSerde = mock(Serde.class);
        final Deserializer<String> valueDeserializer = mock(Deserializer.class);
        final Serializer<String> valueSerializer = mock(Serializer.class);
        when(keySerde.serializer()).thenReturn(keySerializer);
        when(keySerializer.serialize(topic, KEY)).thenReturn(KEY.getBytes());
        when(valueSerde.deserializer()).thenReturn(valueDeserializer);
        when(valueDeserializer.deserialize(topic, VALUE_BYTES)).thenReturn(VALUE);
        when(valueSerde.serializer()).thenReturn(valueSerializer);
        when(valueSerializer.serialize(topic, VALUE)).thenReturn(VALUE_BYTES);
        when(innerStoreMock.fetch(KEY_BYTES, TIMESTAMP)).thenReturn(VALUE_BYTES);
        store = new MeteredWindowStore<>(
            innerStoreMock,
            WINDOW_SIZE_MS,
            STORE_TYPE,
            new MockTime(),
            keySerde,
            valueSerde
        );
        store.init((StateStoreContext) context, store);

        store.fetch(KEY, TIMESTAMP);
        store.put(KEY, VALUE, TIMESTAMP);
    }

    @Test
    public void testMetrics() {
        store.init((StateStoreContext) context, store);
        final JmxReporter reporter = new JmxReporter();
        final MetricsContext metricsContext = new KafkaMetricsContext("kafka.streams");
        reporter.contextChange(metricsContext);

        metrics.addReporter(reporter);
        assertTrue(reporter.containsMbean(String.format(
            "kafka.streams:type=%s,%s=%s,task-id=%s,%s-state-id=%s",
            STORE_LEVEL_GROUP,
            THREAD_ID_TAG_KEY,
            threadId,
            context.taskId().toString(),
            STORE_TYPE,
            STORE_NAME
        )));
    }

    @Test
    public void shouldRecordRestoreLatencyOnInit() {
        doNothing().when(innerStoreMock).init((StateStoreContext) context, store);
        store.init((StateStoreContext) context, store);

        // it suffices to verify one restore metric since all restore metrics are recorded by the same sensor
        // and the sensor is tested elsewhere
        final KafkaMetric metric = metric("restore-rate");
        assertThat((Double) metric.metricValue(), greaterThan(0.0));
    }

    @Test
    public void shouldPutToInnerStoreAndRecordPutMetrics() {
        final byte[] bytes = "a".getBytes();
        doNothing().when(innerStoreMock).put(eq(Bytes.wrap(bytes)), any(), eq(context.timestamp()));

        store.init((StateStoreContext) context, store);
        store.put("a", "a", context.timestamp());

        // it suffices to verify one put metric since all put metrics are recorded by the same sensor
        // and the sensor is tested elsewhere
        final KafkaMetric metric = metric("put-rate");
        assertThat((Double) metric.metricValue(), greaterThan(0.0));
    }

    @Test
    public void shouldFetchFromInnerStoreAndRecordFetchMetrics() {
        when(innerStoreMock.fetch(Bytes.wrap("a".getBytes()), 1, 1))
            .thenReturn(KeyValueIterators.emptyWindowStoreIterator());

        store.init((StateStoreContext) context, store);
        store.fetch("a", ofEpochMilli(1), ofEpochMilli(1)).close(); // recorded on close;

        // it suffices to verify one fetch metric since all fetch metrics are recorded by the same sensor
        // and the sensor is tested elsewhere
        final KafkaMetric metric = metric("fetch-rate");
        assertThat((Double) metric.metricValue(), greaterThan(0.0));
    }

    @Test
    public void shouldReturnNoRecordWhenFetchedKeyHasExpired() {
        when(innerStoreMock.fetch(Bytes.wrap("a".getBytes()), 1, 1 + RETENTION_PERIOD))
                .thenReturn(KeyValueIterators.emptyWindowStoreIterator());

        store.init((StateStoreContext) context, store);
        store.fetch("a", ofEpochMilli(1), ofEpochMilli(1).plus(RETENTION_PERIOD, ChronoUnit.MILLIS)).close(); // recorded on close;
    }

    @Test
    public void shouldFetchRangeFromInnerStoreAndRecordFetchMetrics() {
        when(innerStoreMock.fetch(Bytes.wrap("a".getBytes()), Bytes.wrap("b".getBytes()), 1, 1))
            .thenReturn(KeyValueIterators.emptyIterator());
        when(innerStoreMock.fetch(null, Bytes.wrap("b".getBytes()), 1, 1))
            .thenReturn(KeyValueIterators.emptyIterator());
        when(innerStoreMock.fetch(Bytes.wrap("a".getBytes()), null, 1, 1))
            .thenReturn(KeyValueIterators.emptyIterator());
        when(innerStoreMock.fetch(null, null, 1, 1))
            .thenReturn(KeyValueIterators.emptyIterator());

        store.init((StateStoreContext) context, store);
        store.fetch("a", "b", ofEpochMilli(1), ofEpochMilli(1)).close(); // recorded on close;
        store.fetch(null, "b", ofEpochMilli(1), ofEpochMilli(1)).close(); // recorded on close;
        store.fetch("a", null, ofEpochMilli(1), ofEpochMilli(1)).close(); // recorded on close;
        store.fetch(null, null, ofEpochMilli(1), ofEpochMilli(1)).close(); // recorded on close;

        // it suffices to verify one fetch metric since all fetch metrics are recorded by the same sensor
        // and the sensor is tested elsewhere
        final KafkaMetric metric = metric("fetch-rate");
        assertThat((Double) metric.metricValue(), greaterThan(0.0));
    }

    @Test
    public void shouldBackwardFetchFromInnerStoreAndRecordFetchMetrics() {
        when(innerStoreMock.backwardFetch(Bytes.wrap("a".getBytes()), Bytes.wrap("b".getBytes()), 1, 1))
            .thenReturn(KeyValueIterators.emptyIterator());

        store.init((StateStoreContext) context, store);
        store.backwardFetch("a", "b", ofEpochMilli(1), ofEpochMilli(1)).close(); // recorded on close;

        // it suffices to verify one fetch metric since all fetch metrics are recorded by the same sensor
        // and the sensor is tested elsewhere
        final KafkaMetric metric = metric("fetch-rate");
        assertThat((Double) metric.metricValue(), greaterThan(0.0));
    }

    @Test
    public void shouldBackwardFetchRangeFromInnerStoreAndRecordFetchMetrics() {
        when(innerStoreMock.backwardFetch(Bytes.wrap("a".getBytes()), Bytes.wrap("b".getBytes()), 1, 1))
            .thenReturn(KeyValueIterators.emptyIterator());
        when(innerStoreMock.backwardFetch(null, Bytes.wrap("b".getBytes()), 1, 1))
            .thenReturn(KeyValueIterators.emptyIterator());
        when(innerStoreMock.backwardFetch(Bytes.wrap("a".getBytes()), null, 1, 1))
            .thenReturn(KeyValueIterators.emptyIterator());
        when(innerStoreMock.backwardFetch(null, null, 1, 1))
            .thenReturn(KeyValueIterators.emptyIterator());

        store.init((StateStoreContext) context, store);
        store.backwardFetch("a", "b", ofEpochMilli(1), ofEpochMilli(1)).close(); // recorded on close;
        store.backwardFetch(null, "b", ofEpochMilli(1), ofEpochMilli(1)).close(); // recorded on close;
        store.backwardFetch("a", null, ofEpochMilli(1), ofEpochMilli(1)).close(); // recorded on close;
        store.backwardFetch(null, null, ofEpochMilli(1), ofEpochMilli(1)).close(); // recorded on close;

        // it suffices to verify one fetch metric since all fetch metrics are recorded by the same sensor
        // and the sensor is tested elsewhere
        final KafkaMetric metric = metric("fetch-rate");
        assertThat((Double) metric.metricValue(), greaterThan(0.0));
    }

    @Test
    public void shouldFetchAllFromInnerStoreAndRecordFetchMetrics() {
        when(innerStoreMock.fetchAll(1, 1)).thenReturn(KeyValueIterators.emptyIterator());

        store.init((StateStoreContext) context, store);
        store.fetchAll(ofEpochMilli(1), ofEpochMilli(1)).close(); // recorded on close;

        // it suffices to verify one fetch metric since all fetch metrics are recorded by the same sensor
        // and the sensor is tested elsewhere
        final KafkaMetric metric = metric("fetch-rate");
        assertThat((Double) metric.metricValue(), greaterThan(0.0));
    }

    @Test
    public void shouldBackwardFetchAllFromInnerStoreAndRecordFetchMetrics() {
        when(innerStoreMock.backwardFetchAll(1, 1)).thenReturn(KeyValueIterators.emptyIterator());

        store.init((StateStoreContext) context, store);
        store.backwardFetchAll(ofEpochMilli(1), ofEpochMilli(1)).close(); // recorded on close;

        // it suffices to verify one fetch metric since all fetch metrics are recorded by the same sensor
        // and the sensor is tested elsewhere
        final KafkaMetric metric = metric("fetch-rate");
        assertThat((Double) metric.metricValue(), greaterThan(0.0));
    }

    @Test
    public void shouldRecordFlushLatency() {
        doNothing().when(innerStoreMock).flush();

        store.init((StateStoreContext) context, store);
        store.flush();

        // it suffices to verify one flush metric since all flush metrics are recorded by the same sensor
        // and the sensor is tested elsewhere
        final KafkaMetric metric = metric("flush-rate");
        assertTrue((Double) metric.metricValue() > 0);
    }

    @Test
    public void shouldNotThrowNullPointerExceptionIfFetchReturnsNull() {
        when(innerStoreMock.fetch(Bytes.wrap("a".getBytes()), 0)).thenReturn(null);

        store.init((StateStoreContext) context, store);
        assertNull(store.fetch("a", 0));
    }

    private interface CachedWindowStore extends WindowStore<Bytes, byte[]>, CachedStateStore<byte[], byte[]> {
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldSetFlushListenerOnWrappedCachingStore() {
        final CachedWindowStore cachedWindowStore = mock(CachedWindowStore.class);

        when(cachedWindowStore.setFlushListener(any(CacheFlushListener.class), eq(false))).thenReturn(true);

        final MeteredWindowStore<String, String> metered = new MeteredWindowStore<>(
            cachedWindowStore,
            10L, // any size
            STORE_TYPE,
            new MockTime(),
            Serdes.String(),
            new SerdeThatDoesntHandleNull()
        );
        assertTrue(metered.setFlushListener(null, false));
    }

    @Test
    public void shouldNotSetFlushListenerOnWrappedNoneCachingStore() {
        assertFalse(store.setFlushListener(null, false));
    }

    @Test
    public void shouldCloseUnderlyingStore() {
        doNothing().when(innerStoreMock).close();
        store.init((StateStoreContext) context, store);

        store.close();
    }

    @Test
    public void shouldRemoveMetricsOnClose() {
        doNothing().when(innerStoreMock).close();
        store.init((StateStoreContext) context, store);

        assertThat(storeMetrics(), not(empty()));
        store.close();
        assertThat(storeMetrics(), empty());
    }

    @Test
    public void shouldRemoveMetricsEvenIfWrappedStoreThrowsOnClose() {
        doThrow(new RuntimeException("Oops!")).when(innerStoreMock).close();
        store.init((StateStoreContext) context, store);

        // There's always a "count" metric registered
        assertThat(storeMetrics(), not(empty()));
        assertThrows(RuntimeException.class, store::close);
        assertThat(storeMetrics(), empty());
    }

    @Test
    public void shouldThrowNullPointerOnPutIfKeyIsNull() {
        assertThrows(NullPointerException.class, () -> store.put(null, "a", 1L));
    }

    @Test
    public void shouldThrowNullPointerOnFetchIfKeyIsNull() {
        assertThrows(NullPointerException.class, () -> store.fetch(null, 0L, 1L));
    }

    @Test
    public void shouldThrowNullPointerOnBackwardFetchIfKeyIsNull() {
        assertThrows(NullPointerException.class, () -> store.backwardFetch(null, 0L, 1L));
    }

    @Test
    public void shouldTrackOpenIteratorsMetric() {
        when(innerStoreMock.all()).thenReturn(KeyValueIterators.emptyIterator());
        store.init((StateStoreContext) context, store);

        final KafkaMetric openIteratorsMetric = metric("num-open-iterators");
        assertThat(openIteratorsMetric, not(nullValue()));

        assertThat((Long) openIteratorsMetric.metricValue(), equalTo(0L));

        try (final KeyValueIterator<Windowed<String>, String> iterator = store.all()) {
            assertThat((Long) openIteratorsMetric.metricValue(), equalTo(1L));
        }

        assertThat((Long) openIteratorsMetric.metricValue(), equalTo(0L));
    }

    @Test
    public void shouldTimeIteratorDuration() {
        when(innerStoreMock.all()).thenReturn(KeyValueIterators.emptyIterator());
        store.init((StateStoreContext) context, store);

        final KafkaMetric iteratorDurationAvgMetric = metric("iterator-duration-avg");
        final KafkaMetric iteratorDurationMaxMetric = metric("iterator-duration-max");
        assertThat(iteratorDurationAvgMetric, not(nullValue()));
        assertThat(iteratorDurationMaxMetric, not(nullValue()));

        assertThat((Double) iteratorDurationAvgMetric.metricValue(), equalTo(Double.NaN));
        assertThat((Double) iteratorDurationMaxMetric.metricValue(), equalTo(Double.NaN));

        try (final KeyValueIterator<Windowed<String>, String> iterator = store.all()) {
            // nothing to do, just close immediately
            mockTime.sleep(2);
        }

        assertThat((double) iteratorDurationAvgMetric.metricValue(), equalTo(2.0 * TimeUnit.MILLISECONDS.toNanos(1)));
        assertThat((double) iteratorDurationMaxMetric.metricValue(), equalTo(2.0 * TimeUnit.MILLISECONDS.toNanos(1)));

        try (final KeyValueIterator<Windowed<String>, String> iterator = store.all()) {
            // nothing to do, just close immediately
            mockTime.sleep(3);
        }

        assertThat((double) iteratorDurationAvgMetric.metricValue(), equalTo(2.5 * TimeUnit.MILLISECONDS.toNanos(1)));
        assertThat((double) iteratorDurationMaxMetric.metricValue(), equalTo(3.0 * TimeUnit.MILLISECONDS.toNanos(1)));
    }

    @Test
    public void shouldTrackOldestOpenIteratorTimestamp() {
        when(innerStoreMock.all()).thenReturn(KeyValueIterators.emptyIterator());
        store.init((StateStoreContext) context, store);

        final KafkaMetric oldestIteratorTimestampMetric = metric("oldest-iterator-open-since-ms");
        assertThat(oldestIteratorTimestampMetric, not(nullValue()));

        assertThat(oldestIteratorTimestampMetric.metricValue(), nullValue());

        KeyValueIterator<Windowed<String>, String> second = null;
        final long secondTimestamp;
        try {
            try (final KeyValueIterator<Windowed<String>, String> first = store.all()) {
                final long oldestTimestamp = mockTime.milliseconds();
                assertThat((Long) oldestIteratorTimestampMetric.metricValue(), equalTo(oldestTimestamp));
                mockTime.sleep(100);

                // open a second iterator before closing the first to test that we still produce the first iterator's timestamp
                second = store.all();
                secondTimestamp = mockTime.milliseconds();
                assertThat((Long) oldestIteratorTimestampMetric.metricValue(), equalTo(oldestTimestamp));
                mockTime.sleep(100);
            }

            // now that the first iterator is closed, check that the timestamp has advanced to the still open second iterator
            assertThat((Long) oldestIteratorTimestampMetric.metricValue(), equalTo(secondTimestamp));
        } finally {
            if (second != null) {
                second.close();
            }
        }

        assertThat((Integer) oldestIteratorTimestampMetric.metricValue(), nullValue());
    }

    private KafkaMetric metric(final String name) {
        return metrics.metric(new MetricName(name, STORE_LEVEL_GROUP, "", tags));
    }

    private List<MetricName> storeMetrics() {
        return metrics.metrics()
            .keySet()
            .stream()
            .filter(name -> name.group().equals(STORE_LEVEL_GROUP) && name.tags().equals(tags))
            .collect(Collectors.toList());
    }
}
