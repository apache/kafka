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
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.test.KeyValueIteratorStub;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class MeteredTimestampedKeyValueStoreWithHeadersTest {
    private static final String APPLICATION_ID = "test-app";
    private static final String STORE_NAME = "store-name";
    private static final String STORE_TYPE = "scope";
    private static final String STORE_LEVEL_GROUP = "stream-state-metrics";
    private static final String CHANGELOG_TOPIC = "changelog-topic-name";
    private static final String THREAD_ID_TAG_KEY = "thread-id";
    private static final String KEY = "key";
    private static final Bytes KEY_BYTES = Bytes.wrap(KEY.getBytes());
    private static final RecordHeaders HEADERS = makeHeaders();
    private static final ValueTimestampHeaders<String> VALUE_TIMESTAMP_HEADERS =
        ValueTimestampHeaders.make("value", 97L, HEADERS);
    private static final byte[] VALUE_TIMESTAMP_HEADERS_BYTES = serializeValueTimestampHeaders();
    private final String threadId = Thread.currentThread().getName();
    private final TaskId taskId = new TaskId(0, 0, "My-Topology");
    @Mock
    private KeyValueStore<Bytes, byte[]> inner;
    @Mock
    private InternalProcessorContext<?, ?> context;
    private MockTime mockTime;

    private static final Map<String, Object> CONFIGS =
        mkMap(mkEntry(StreamsConfig.InternalConfig.TOPIC_PREFIX_ALTERNATIVE, APPLICATION_ID));

    private MeteredTimestampedKeyValueStoreWithHeaders<String, String> metered;
    private final KeyValue<Bytes, byte[]> byteKeyValueTimestampHeadersPair = KeyValue.pair(KEY_BYTES,
        VALUE_TIMESTAMP_HEADERS_BYTES
    );
    private final Metrics metrics = new Metrics();
    private Map<String, String> tags;

    private void setUpWithoutContext() {
        mockTime = new MockTime();
        metered = new MeteredTimestampedKeyValueStoreWithHeaders<>(
            inner,
            "scope",
            mockTime,
            Serdes.String(),
            new ValueTimestampHeadersSerde<>(Serdes.String())
        );
        metrics.config().recordLevel(Sensor.RecordingLevel.DEBUG);
        tags = mkMap(
            mkEntry(THREAD_ID_TAG_KEY, threadId),
            mkEntry("task-id", taskId.toString()),
            mkEntry(STORE_TYPE + "-state-id", STORE_NAME)
        );
    }

    private void setUp() {
        setUpWithoutContext();
        when(context.applicationId()).thenReturn(APPLICATION_ID);
        when(context.metrics())
            .thenReturn(new StreamsMetricsImpl(metrics, "test", mockTime));
        when(context.taskId()).thenReturn(taskId);
        when(context.changelogFor(STORE_NAME)).thenReturn(CHANGELOG_TOPIC);
        when(inner.name()).thenReturn(STORE_NAME);
        when(context.appConfigs()).thenReturn(CONFIGS);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void setUpWithExpectSerdes() {
        setUp();
        when(context.keySerde()).thenReturn((Serde) Serdes.String());
        when(context.valueSerde()).thenReturn((Serde) Serdes.Long());
    }

    private void init() {
        metered.init(context, metered);
    }

    @Test
    public void shouldDelegateInit() {
        setUp();
        final MeteredTimestampedKeyValueStoreWithHeaders<String, String> outer = new MeteredTimestampedKeyValueStoreWithHeaders<>(
            inner,
            STORE_TYPE,
            new MockTime(),
            Serdes.String(),
            new ValueTimestampHeadersSerde<>(Serdes.String())
        );
        doNothing().when(inner).init(context, outer);
        outer.init(context, outer);
    }

    @Test
    public void shouldPassChangelogTopicNameToStateStoreSerde() {
        setUp();
        doShouldPassChangelogTopicNameToStateStoreSerde(CHANGELOG_TOPIC);
    }

    @Test
    public void shouldPassDefaultChangelogTopicNameToStateStoreSerdeIfLoggingDisabled() {
        setUp();
        final String defaultChangelogTopicName = ProcessorStateManager.storeChangelogTopic(APPLICATION_ID, STORE_NAME, taskId.topologyName());
        when(context.changelogFor(STORE_NAME)).thenReturn(null);
        doShouldPassChangelogTopicNameToStateStoreSerde(defaultChangelogTopicName);
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
        doNothing().when(inner).put(any(Bytes.class), any(byte[].class));
        init();

        metered.put(KEY, VALUE_TIMESTAMP_HEADERS);

        final KafkaMetric metric = metric("put-rate");
        assertTrue((Double) metric.metricValue() > 0);
    }

    @Test
    public void shouldGetBytesFromInnerStoreAndReturnGetMetric() {
        setUp();
        when(inner.get(any(Bytes.class))).thenReturn(VALUE_TIMESTAMP_HEADERS_BYTES);
        init();

        assertEquals(VALUE_TIMESTAMP_HEADERS, metered.get(KEY));

        final KafkaMetric metric = metric("get-rate");
        assertTrue((Double) metric.metricValue() > 0);
    }

    @Test
    public void shouldPutIfAbsentAndRecordPutIfAbsentMetric() {
        setUp();
        when(inner.putIfAbsent(any(Bytes.class), any(byte[].class))).thenReturn(null);
        init();

        metered.putIfAbsent(KEY, VALUE_TIMESTAMP_HEADERS);

        final KafkaMetric metric = metric("put-if-absent-rate");
        assertTrue((Double) metric.metricValue() > 0);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldPutAllToInnerStoreAndRecordPutAllMetric() {
        setUp();
        doNothing().when(inner).putAll(any(List.class));
        init();

        metered.putAll(Collections.singletonList(KeyValue.pair(KEY, VALUE_TIMESTAMP_HEADERS)));

        final KafkaMetric metric = metric("put-all-rate");
        assertTrue((Double) metric.metricValue() > 0);
    }

    @Test
    public void shouldDeleteFromInnerStoreAndRecordDeleteMetric() {
        setUp();
        when(inner.delete(any(Bytes.class))).thenReturn(VALUE_TIMESTAMP_HEADERS_BYTES);
        init();

        metered.delete(KEY);

        final KafkaMetric metric = metric("delete-rate");
        assertTrue((Double) metric.metricValue() > 0);
    }

    @Test
    public void shouldGetRangeFromInnerStoreAndRecordRangeMetric() {
        setUp();
        when(inner.range(any(Bytes.class), any(Bytes.class))).thenReturn(
            new KeyValueIteratorStub<>(Collections.singletonList(byteKeyValueTimestampHeadersPair).iterator()));
        init();

        try (final KeyValueIterator<String, ValueTimestampHeaders<String>> iterator = metered.range(KEY, KEY)) {
            assertEquals(VALUE_TIMESTAMP_HEADERS, iterator.next().value);
            assertFalse(iterator.hasNext());
        }

        final KafkaMetric metric = metric("range-rate");
        assertTrue((Double) metric.metricValue() > 0);
    }

    @Test
    public void shouldGetAllFromInnerStoreAndRecordAllMetric() {
        setUp();
        when(inner.all())
            .thenReturn(new KeyValueIteratorStub<>(Collections.singletonList(byteKeyValueTimestampHeadersPair).iterator()));
        init();

        try (final KeyValueIterator<String, ValueTimestampHeaders<String>> iterator = metered.all()) {
            assertEquals(VALUE_TIMESTAMP_HEADERS, iterator.next().value);
            assertFalse(iterator.hasNext());
        }

        final KafkaMetric metric = metric(new MetricName("all-rate", STORE_LEVEL_GROUP, "", tags));
        assertTrue((Double) metric.metricValue() > 0);
    }

    @Test
    public void shouldCommitInnerWhenCommitTimeRecords() {
        setUp();
        doNothing().when(inner).commit(Map.of());
        init();

        metered.commit(Map.of());

        final KafkaMetric metric = metric("flush-rate");
        assertTrue((Double) metric.metricValue() > 0);
    }

    private interface CachedKeyValueStore extends KeyValueStore<Bytes, byte[]>, CachedStateStore<byte[], byte[]> { }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldSetFlushListenerOnWrappedCachingStore() {
        setUpWithoutContext();
        final CachedKeyValueStore cachedKeyValueStore = mock(CachedKeyValueStore.class);

        when(cachedKeyValueStore.setFlushListener(any(CacheFlushListener.class), eq(false))).thenReturn(true);

        metered = new MeteredTimestampedKeyValueStoreWithHeaders<>(
            cachedKeyValueStore,
            STORE_TYPE,
            new MockTime(),
            Serdes.String(),
            new ValueTimestampHeadersSerde<>(Serdes.String()));
        assertTrue(metered.setFlushListener(null, false));
    }

    @Test
    public void shouldNotSetFlushListenerOnWrappedNoneCachingStore() {
        setUpWithoutContext();
        assertFalse(metered.setFlushListener(null, false));
    }

    @Test
    public void shouldNotThrowExceptionIfSerdesCorrectlySetFromProcessorContext() {
        setUpWithExpectSerdes();
        final MeteredTimestampedKeyValueStoreWithHeaders<String, Long> store = new MeteredTimestampedKeyValueStoreWithHeaders<>(
            inner,
            STORE_TYPE,
            new MockTime(),
            null,
            null
        );
        store.init(context, inner);

        try {
            store.put("key", ValueTimestampHeaders.make(42L, 60000, new RecordHeaders()));
        } catch (final StreamsException exception) {
            if (exception.getCause() instanceof ClassCastException) {
                throw new AssertionError(
                    "Serdes are not correctly set from processor context.", exception);
            } else {
                throw exception;
            }
        }
    }

    @Test
    public void shouldNotThrowExceptionIfSerdesCorrectlySetFromConstructorParameters() {
        setUp();
        final MeteredTimestampedKeyValueStoreWithHeaders<String, Long> store = new MeteredTimestampedKeyValueStoreWithHeaders<>(
            inner,
            STORE_TYPE,
            new MockTime(),
            Serdes.String(),
            new ValueTimestampHeadersSerde<>(Serdes.Long())
        );
        store.init(context, inner);

        try {
            store.put("key", ValueTimestampHeaders.make(42L, 60000, new RecordHeaders()));
        } catch (final StreamsException exception) {
            if (exception.getCause() instanceof ClassCastException) {
                fail("Serdes are not correctly set from constructor parameters.");
            }
            throw exception;
        }
    }

    @SuppressWarnings("unused")
    @Test
    public void shouldTrackOpenIteratorsMetric() {
        setUp();
        when(inner.all()).thenReturn(KeyValueIterators.emptyIterator());
        init();

        final KafkaMetric openIteratorsMetric = metric("num-open-iterators");
        assertNotNull(openIteratorsMetric);

        assertEquals(0L, (Long) openIteratorsMetric.metricValue());

        try (final KeyValueIterator<String, ValueTimestampHeaders<String>> unused = metered.all()) {
            assertEquals(1L, (Long) openIteratorsMetric.metricValue());
        }

        assertEquals(0L, (Long) openIteratorsMetric.metricValue());
    }

    @SuppressWarnings("unused")
    @Test
    public void shouldTimeIteratorDuration() {
        setUp();
        when(inner.all()).thenReturn(KeyValueIterators.emptyIterator());
        init();

        final KafkaMetric iteratorDurationAvgMetric = metric("iterator-duration-avg");
        final KafkaMetric iteratorDurationMaxMetric = metric("iterator-duration-max");
        assertNotNull(iteratorDurationAvgMetric);
        assertNotNull(iteratorDurationMaxMetric);

        assertEquals(Double.NaN, (Double) iteratorDurationAvgMetric.metricValue());
        assertEquals(Double.NaN, (Double) iteratorDurationMaxMetric.metricValue());

        try (final KeyValueIterator<String, ValueTimestampHeaders<String>> unused = metered.all()) {
            // nothing to do, just close immediately
            mockTime.sleep(2);
        }

        assertEquals(2.0 * TimeUnit.MILLISECONDS.toNanos(1), (double) iteratorDurationAvgMetric.metricValue());
        assertEquals(2.0 * TimeUnit.MILLISECONDS.toNanos(1), (double) iteratorDurationMaxMetric.metricValue());

        try (final KeyValueIterator<String, ValueTimestampHeaders<String>> iterator = metered.all()) {
            // nothing to do, just close immediately
            mockTime.sleep(3);
        }

        assertEquals(2.5 * TimeUnit.MILLISECONDS.toNanos(1), (double) iteratorDurationAvgMetric.metricValue());
        assertEquals(3.0 * TimeUnit.MILLISECONDS.toNanos(1), (double) iteratorDurationMaxMetric.metricValue());
    }

    @SuppressWarnings("unused")
    @Test
    public void shouldTrackOldestOpenIteratorTimestamp() {
        setUp();
        when(inner.all()).thenReturn(KeyValueIterators.emptyIterator());
        init();

        final KafkaMetric oldestIteratorTimestampMetric = metric("oldest-iterator-open-since-ms");
        assertNotNull(oldestIteratorTimestampMetric);

        KeyValueIterator<String, ValueTimestampHeaders<String>> second = null;
        final long secondTimestamp;
        try {
            try (final KeyValueIterator<String, ValueTimestampHeaders<String>> unused = metered.all()) {

                final long oldestTimestamp = mockTime.milliseconds();
                assertEquals(oldestTimestamp, (Long) oldestIteratorTimestampMetric.metricValue());
                mockTime.sleep(100);

                // open a second iterator before closing the first to test that we still produce the first iterator's timestamp
                second = metered.all();
                secondTimestamp = mockTime.milliseconds();
                assertEquals(oldestTimestamp, (Long) oldestIteratorTimestampMetric.metricValue());
                mockTime.sleep(100);
            }

            // now that the first iterator is closed, check that the timestamp has advanced to the still open second iterator
            assertEquals(secondTimestamp, (Long) oldestIteratorTimestampMetric.metricValue());
        } finally {
            if (second != null) {
                second.close();
            }
        }
        // now that all iterators are closed, the metric should be zero
        assertEquals(0L, (Long) oldestIteratorTimestampMetric.metricValue());
    }

    private static RecordHeaders makeHeaders() {
        final RecordHeaders headers = new RecordHeaders();
        headers.add("header-key", "header-value".getBytes());
        return headers;
    }

    private static byte[] serializeValueTimestampHeaders() {
        final ValueTimestampHeadersSerializer<String> serializer = new ValueTimestampHeadersSerializer<>(Serdes.String().serializer());
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
        lenient().when(keySerializer.serialize(eq(topic), any(RecordHeaders.class), eq(KEY))).thenReturn(KEY.getBytes());
        when(valueSerde.deserializer()).thenReturn(valueDeserializer);
        lenient().when(valueDeserializer.deserialize(eq(topic), any(RecordHeaders.class), any(byte[].class))).thenReturn(VALUE_TIMESTAMP_HEADERS);
        when(valueSerde.serializer()).thenReturn(valueSerializer);
        lenient().when(valueSerializer.serialize(eq(topic), any(RecordHeaders.class), eq(VALUE_TIMESTAMP_HEADERS))).thenReturn(VALUE_TIMESTAMP_HEADERS_BYTES);
        when(inner.get(any(Bytes.class))).thenReturn(VALUE_TIMESTAMP_HEADERS_BYTES);
        metered = new MeteredTimestampedKeyValueStoreWithHeaders<>(
            inner,
            STORE_TYPE,
            new MockTime(),
            keySerde,
            valueSerde
        );
        metered.init(context, metered);

        metered.get(KEY);
        metered.put(KEY, VALUE_TIMESTAMP_HEADERS);
    }

    private KafkaMetric metric(final String name) {
        return this.metrics.metric(new MetricName(name, STORE_LEVEL_GROUP, "", tags));
    }

    private KafkaMetric metric(final MetricName metricName) {
        return this.metrics.metric(metricName);
    }
}
