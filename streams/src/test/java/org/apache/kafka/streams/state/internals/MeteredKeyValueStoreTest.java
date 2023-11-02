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
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.test.KeyValueIteratorStub;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class MeteredKeyValueStoreTest {

    private static final String APPLICATION_ID = "test-app";
    private static final String STORE_NAME = "store-name";
    private static final String STORE_TYPE = "scope";
    private static final String STORE_LEVEL_GROUP = "stream-state-metrics";
    private static final String CHANGELOG_TOPIC = "changelog-topic";
    private static final String THREAD_ID_TAG_KEY = "thread-id";
    private static final String KEY = "key";
    private static final Bytes KEY_BYTES = Bytes.wrap(KEY.getBytes());
    private static final String VALUE = "value";
    private static final byte[] VALUE_BYTES = VALUE.getBytes();
    private static final KeyValue<Bytes, byte[]> BYTE_KEY_VALUE_PAIR = KeyValue.pair(KEY_BYTES, VALUE_BYTES);

    private final String threadId = Thread.currentThread().getName();
    private final TaskId taskId = new TaskId(0, 0, "My-Topology");

    @Mock
    private KeyValueStore<Bytes, byte[]> inner;
    @Mock
    private InternalProcessorContext context;

    private MeteredKeyValueStore<String, String> metered;
    private final Metrics metrics = new Metrics();
    private Map<String, String> tags;

    @Before
    public void before() {
        final Time mockTime = new MockTime();
        metered = new MeteredKeyValueStore<>(
            inner,
            STORE_TYPE,
            mockTime,
            Serdes.String(),
            Serdes.String()
        );
        metrics.config().recordLevel(Sensor.RecordingLevel.DEBUG);
        when(context.applicationId()).thenReturn(APPLICATION_ID);
        when(context.metrics()).thenReturn(
            new StreamsMetricsImpl(metrics, "test", StreamsConfig.METRICS_LATEST, mockTime)
        );
        when(context.taskId()).thenReturn(taskId);
        when(context.changelogFor(STORE_NAME)).thenReturn(CHANGELOG_TOPIC);
        when(inner.name()).thenReturn(STORE_NAME);
        tags = mkMap(
            mkEntry(THREAD_ID_TAG_KEY, threadId),
            mkEntry("task-id", taskId.toString()),
            mkEntry(STORE_TYPE + "-state-id", STORE_NAME)
        );
    }

    private void init() {
        metered.init((StateStoreContext) context, metered);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldDelegateDeprecatedInit() {
        final MeteredKeyValueStore<String, String> outer = new MeteredKeyValueStore<>(
            inner,
            STORE_TYPE,
            new MockTime(),
            Serdes.String(),
            Serdes.String()
        );
        doNothing().when(inner).init((ProcessorContext) context, outer);
        outer.init((ProcessorContext) context, outer);
    }

    @Test
    public void shouldDelegateInit() {
        final MeteredKeyValueStore<String, String> outer = new MeteredKeyValueStore<>(
            inner,
            STORE_TYPE,
            new MockTime(),
            Serdes.String(),
            Serdes.String()
        );
        doNothing().when(inner).init((StateStoreContext) context, outer);
        outer.init((StateStoreContext) context, outer);
    }

    @Test
    public void shouldPassChangelogTopicNameToStateStoreSerde() {
        doShouldPassChangelogTopicNameToStateStoreSerde(CHANGELOG_TOPIC);
    }

    @Test
    public void shouldPassDefaultChangelogTopicNameToStateStoreSerdeIfLoggingDisabled() {
        final String defaultChangelogTopicName = ProcessorStateManager.storeChangelogTopic(APPLICATION_ID, STORE_NAME, taskId.topologyName());
        when(context.changelogFor(STORE_NAME)).thenReturn(null);
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
        when(inner.get(KEY_BYTES)).thenReturn(VALUE_BYTES);
        metered = new MeteredKeyValueStore<>(
            inner,
            STORE_TYPE,
            new MockTime(),
            keySerde,
            valueSerde
        );
        metered.init((StateStoreContext) context, metered);

        metered.get(KEY);
        metered.put(KEY, VALUE);
    }

    @Test
    public void testMetrics() {
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
    public void shouldRecordRestoreLatencyOnInit() {
        doNothing().when(inner).init((StateStoreContext) context, metered);

        init();

        // it suffices to verify one restore metric since all restore metrics are recorded by the same sensor
        // and the sensor is tested elsewhere
        final KafkaMetric metric = metric("restore-rate");
        assertThat((Double) metric.metricValue(), greaterThan(0.0));
    }

    @Test
    public void shouldWriteBytesToInnerStoreAndRecordPutMetric() {
        doNothing().when(inner).put(KEY_BYTES, VALUE_BYTES);
        init();

        metered.put(KEY, VALUE);

        final KafkaMetric metric = metric("put-rate");
        assertTrue((Double) metric.metricValue() > 0);
    }

    @Test
    public void shouldGetBytesFromInnerStoreAndReturnGetMetric() {
        when(inner.get(KEY_BYTES)).thenReturn(VALUE_BYTES);
        init();

        assertThat(metered.get(KEY), equalTo(VALUE));

        final KafkaMetric metric = metric("get-rate");
        assertTrue((Double) metric.metricValue() > 0);
    }

    @Test
    public void shouldPutIfAbsentAndRecordPutIfAbsentMetric() {
        when(inner.putIfAbsent(KEY_BYTES, VALUE_BYTES)).thenReturn(null);
        init();

        metered.putIfAbsent(KEY, VALUE);

        final KafkaMetric metric = metric("put-if-absent-rate");
        assertTrue((Double) metric.metricValue() > 0);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldPutAllToInnerStoreAndRecordPutAllMetric() {
        doNothing().when(inner).putAll(any(List.class));
        init();

        metered.putAll(Collections.singletonList(KeyValue.pair(KEY, VALUE)));

        final KafkaMetric metric = metric("put-all-rate");
        assertTrue((Double) metric.metricValue() > 0);
    }

    @Test
    public void shouldDeleteFromInnerStoreAndRecordDeleteMetric() {
        when(inner.delete(KEY_BYTES)).thenReturn(VALUE_BYTES);
        init();

        metered.delete(KEY);

        final KafkaMetric metric = metric("delete-rate");
        assertTrue((Double) metric.metricValue() > 0);
    }

    @Test
    public void shouldGetRangeFromInnerStoreAndRecordRangeMetric() {
        when(inner.range(KEY_BYTES, KEY_BYTES))
            .thenReturn(new KeyValueIteratorStub<>(Collections.singletonList(BYTE_KEY_VALUE_PAIR).iterator()));
        init();

        final KeyValueIterator<String, String> iterator = metered.range(KEY, KEY);
        assertThat(iterator.next().value, equalTo(VALUE));
        assertFalse(iterator.hasNext());
        iterator.close();

        final KafkaMetric metric = metric("range-rate");
        assertTrue((Double) metric.metricValue() > 0);
    }

    @Test
    public void shouldGetAllFromInnerStoreAndRecordAllMetric() {
        when(inner.all()).thenReturn(new KeyValueIteratorStub<>(Collections.singletonList(BYTE_KEY_VALUE_PAIR).iterator()));
        init();

        final KeyValueIterator<String, String> iterator = metered.all();
        assertThat(iterator.next().value, equalTo(VALUE));
        assertFalse(iterator.hasNext());
        iterator.close();

        final KafkaMetric metric = metric(new MetricName("all-rate", STORE_LEVEL_GROUP, "", tags));
        assertTrue((Double) metric.metricValue() > 0);
    }

    @Test
    public void shouldFlushInnerWhenFlushTimeRecords() {
        doNothing().when(inner).flush();
        init();

        metered.flush();

        final KafkaMetric metric = metric("flush-rate");
        assertTrue((Double) metric.metricValue() > 0);
    }

    private interface CachedKeyValueStore extends KeyValueStore<Bytes, byte[]>, CachedStateStore<byte[], byte[]> { }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldSetFlushListenerOnWrappedCachingStore() {
        final CachedKeyValueStore cachedKeyValueStore = mock(CachedKeyValueStore.class);

        when(cachedKeyValueStore.setFlushListener(any(CacheFlushListener.class), eq(false))).thenReturn(true);

        metered = new MeteredKeyValueStore<>(
            cachedKeyValueStore,
            STORE_TYPE,
            new MockTime(),
            Serdes.String(),
            Serdes.String()
        );
        assertTrue(metered.setFlushListener(null, false));
    }

    @Test
    public void shouldNotThrowNullPointerExceptionIfGetReturnsNull() {
        when(inner.get(Bytes.wrap("a".getBytes()))).thenReturn(null);

        init();
        assertNull(metered.get("a"));
    }

    @Test
    public void shouldNotSetFlushListenerOnWrappedNoneCachingStore() {
        assertFalse(metered.setFlushListener(null, false));
    }

    @Test
    public void shouldRemoveMetricsOnClose() {
        doNothing().when(inner).close();
        init(); // replays "inner"

        // There's always a "count" metric registered
        assertThat(storeMetrics(), not(empty()));
        metered.close();
        assertThat(storeMetrics(), empty());
    }

    @Test
    public void shouldRemoveMetricsEvenIfWrappedStoreThrowsOnClose() {
        doThrow(new RuntimeException("Oops!")).when(inner).close();
        init(); // replays "inner"

        assertThat(storeMetrics(), not(empty()));
        assertThrows(RuntimeException.class, metered::close);
        assertThat(storeMetrics(), empty());
    }

    @Test
    public void shouldThrowNullPointerOnGetIfKeyIsNull() {
        assertThrows(NullPointerException.class, () -> metered.get(null));
    }

    @Test
    public void shouldThrowNullPointerOnPutIfKeyIsNull() {
        assertThrows(NullPointerException.class, () -> metered.put(null, VALUE));
    }

    @Test
    public void shouldThrowNullPointerOnPutIfAbsentIfKeyIsNull() {
        assertThrows(NullPointerException.class, () -> metered.putIfAbsent(null, VALUE));
    }

    @Test
    public void shouldThrowNullPointerOnDeleteIfKeyIsNull() {
        assertThrows(NullPointerException.class, () -> metered.delete(null));
    }

    @Test
    public void shouldThrowNullPointerOnPutAllIfAnyKeyIsNull() {
        assertThrows(NullPointerException.class, () -> metered.putAll(Collections.singletonList(KeyValue.pair(null, VALUE))));
    }

    @Test
    public void shouldThrowNullPointerOnPrefixScanIfPrefixIsNull() {
        final StringSerializer stringSerializer = new StringSerializer();
        assertThrows(NullPointerException.class, () -> metered.prefixScan(null, stringSerializer));
    }

    @Test
    public void shouldThrowNullPointerOnRangeIfFromIsNull() {
        assertThrows(NullPointerException.class, () -> metered.range(null, "to"));
    }

    @Test
    public void shouldThrowNullPointerOnRangeIfToIsNull() {
        assertThrows(NullPointerException.class, () -> metered.range("from", null));
    }

    @Test
    public void shouldThrowNullPointerOnReverseRangeIfFromIsNull() {
        assertThrows(NullPointerException.class, () -> metered.reverseRange(null, "to"));
    }

    @Test
    public void shouldThrowNullPointerOnReverseRangeIfToIsNull() {
        assertThrows(NullPointerException.class, () -> metered.reverseRange("from", null));
    }

    @Test
    public void shouldGetRecordsWithPrefixKey() {
        final StringSerializer stringSerializer = new StringSerializer();
        when(inner.prefixScan(KEY, stringSerializer))
            .thenReturn(new KeyValueIteratorStub<>(Collections.singletonList(BYTE_KEY_VALUE_PAIR).iterator()));
        init();

        final KeyValueIterator<String, String> iterator = metered.prefixScan(KEY, stringSerializer);
        assertThat(iterator.next().value, equalTo(VALUE));
        iterator.close();

        final KafkaMetric metric = metrics.metric(new MetricName("prefix-scan-rate", STORE_LEVEL_GROUP, "", tags));
        assertTrue((Double) metric.metricValue() > 0);
    }

    private KafkaMetric metric(final MetricName metricName) {
        return this.metrics.metric(metricName);
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
