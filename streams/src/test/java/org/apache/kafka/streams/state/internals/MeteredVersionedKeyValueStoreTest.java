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

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.VersionedBytesStore;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class MeteredVersionedKeyValueStoreTest {

    private static final String STORE_NAME = "versioned_store";
    private static final Serde<String> STRING_SERDE = new StringSerde();
    private static final Serde<ValueAndTimestamp<String>> VALUE_AND_TIMESTAMP_SERDE = new NullableValueAndTimestampSerde<>(STRING_SERDE);
    private static final String METRICS_SCOPE = "scope";
    private static final String STORE_LEVEL_GROUP = "stream-state-metrics";
    private static final String APPLICATION_ID = "test-app";
    private static final TaskId TASK_ID = new TaskId(0, 0, "My-Topology");

    private static final String KEY = "k";
    private static final String VALUE = "v";
    private static final long TIMESTAMP = 10L;
    private static final Bytes RAW_KEY = new Bytes(STRING_SERDE.serializer().serialize(null, KEY));
    private static final byte[] RAW_VALUE_AND_TIMESTAMP = VALUE_AND_TIMESTAMP_SERDE.serializer()
        .serialize(null, ValueAndTimestamp.make(VALUE, TIMESTAMP));

    private final VersionedBytesStore inner = mock(VersionedBytesStore.class);
    private final Metrics metrics = new Metrics();
    private final Time mockTime = new MockTime();
    private final String threadId = Thread.currentThread().getName();
    private InternalProcessorContext context = mock(InternalProcessorContext.class);
    private Map<String, String> tags;

    private MeteredVersionedKeyValueStore<String, String> store;

    @Before
    public void setUp() {
        when(inner.name()).thenReturn(STORE_NAME);
        when(context.metrics()).thenReturn(new StreamsMetricsImpl(metrics, "test", StreamsConfig.METRICS_LATEST, mockTime));
        when(context.applicationId()).thenReturn(APPLICATION_ID);
        when(context.taskId()).thenReturn(TASK_ID);

        metrics.config().recordLevel(Sensor.RecordingLevel.DEBUG);
        tags = mkMap(
            mkEntry("thread-id", threadId),
            mkEntry("task-id", TASK_ID.toString()),
            mkEntry(METRICS_SCOPE + "-state-id", STORE_NAME)
        );

        store = newMeteredStore(inner);
        store.init((StateStoreContext) context, store);
    }

    private MeteredVersionedKeyValueStore<String, String> newMeteredStore(final VersionedBytesStore inner) {
        return new MeteredVersionedKeyValueStore<>(
            inner,
            METRICS_SCOPE,
            mockTime,
            STRING_SERDE,
            VALUE_AND_TIMESTAMP_SERDE
        );
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldDelegateDeprecatedInit() {
        // recreate store in order to re-init
        store.close();
        final VersionedBytesStore mockInner = mock(VersionedBytesStore.class);
        store = newMeteredStore(mockInner);

        store.init((ProcessorContext) context, store);

        verify(mockInner).init((ProcessorContext) context, store);
    }

    @Test
    public void shouldDelegateInit() {
        // init is already called in setUp()
        verify(inner).init((StateStoreContext) context, store);
    }

    @Test
    public void shouldPassChangelogTopicNameToStateStoreSerde() {
        final String changelogTopicName = "changelog-topic";
        when(context.changelogFor(STORE_NAME)).thenReturn(changelogTopicName);
        doShouldPassChangelogTopicNameToStateStoreSerde(changelogTopicName);
    }

    @Test
    public void shouldPassDefaultChangelogTopicNameToStateStoreSerdeIfLoggingDisabled() {
        final String defaultChangelogTopicName = ProcessorStateManager.storeChangelogTopic(APPLICATION_ID, STORE_NAME, TASK_ID.topologyName());
        when(context.changelogFor(STORE_NAME)).thenReturn(null);
        doShouldPassChangelogTopicNameToStateStoreSerde(defaultChangelogTopicName);
    }

    @SuppressWarnings("unchecked")
    private void doShouldPassChangelogTopicNameToStateStoreSerde(final String changelogTopicName) {
        // recreate store with mock serdes
        final Serde<String> keySerde = mock(Serde.class);
        final Serializer<String> keySerializer = mock(Serializer.class);
        final Serde<String> valueSerde = mock(Serde.class);
        final Serializer<String> valueSerializer = mock(Serializer.class);
        final Deserializer<String> valueDeserializer = mock(Deserializer.class);
        when(keySerde.serializer()).thenReturn(keySerializer);
        when(valueSerde.serializer()).thenReturn(valueSerializer);
        when(valueSerde.deserializer()).thenReturn(valueDeserializer);

        store.close();
        store = new MeteredVersionedKeyValueStore<>(
            inner,
            METRICS_SCOPE,
            mockTime,
            keySerde,
            new NullableValueAndTimestampSerde<>(valueSerde)
        );
        store.init((StateStoreContext) context, store);

        store.put(KEY, VALUE, TIMESTAMP);

        verify(keySerializer).serialize(changelogTopicName, KEY);
        verify(valueSerializer).serialize(changelogTopicName, VALUE);
    }

    @Test
    public void shouldRecordMetricsOnInit() {
        // init is called in setUp(). it suffices to verify one restore metric since all restore
        // metrics are recorded by the same sensor, and the sensor is tested elsewhere.
        assertThat((Double) getMetric("restore-rate").metricValue(), greaterThan(0.0));
    }

    @Test
    public void shouldDelegateAndRecordMetricsOnPut() {
        store.put(KEY, VALUE, TIMESTAMP);

        verify(inner).put(RAW_KEY, RAW_VALUE_AND_TIMESTAMP);
        assertThat((Double) getMetric("put-rate").metricValue(), greaterThan(0.0));
    }

    @Test
    public void shouldDelegateAndRecordMetricsOnDelete() {
        store.delete(KEY, TIMESTAMP);

        verify(inner).delete(RAW_KEY, TIMESTAMP);
        assertThat((Double) getMetric("delete-rate").metricValue(), greaterThan(0.0));
    }

    @Test
    public void shouldDelegateAndRecordMetricsOnGet() {
        store.get(KEY);

        verify(inner).get(RAW_KEY);
        assertThat((Double) getMetric("get-rate").metricValue(), greaterThan(0.0));
    }

    @Test
    public void shouldDelegateAndRecordMetricsOnGetWithTimestamp() {
        store.get(KEY, TIMESTAMP);

        verify(inner).get(RAW_KEY, TIMESTAMP);
        assertThat((Double) getMetric("get-rate").metricValue(), greaterThan(0.0));
    }

    @Test
    public void shouldDelegateAndRecordMetricsOnFlush() {
        store.flush();

        verify(inner).flush();
        assertThat((Double) getMetric("flush-rate").metricValue(), greaterThan(0.0));
    }

    @Test
    public void shouldDelegateAndRemoveMetricsOnClose() {
        assertThat(storeMetrics(), not(empty()));

        store.close();

        verify(inner).close();
        assertThat(storeMetrics(), empty());
    }

    @Test
    public void shouldRemoveMetricsOnCloseEvenIfInnerThrows() {
        doThrow(new RuntimeException("uh oh")).when(inner).close();
        assertThat(storeMetrics(), not(empty()));

        assertThrows(RuntimeException.class, () -> store.close());

        assertThat(storeMetrics(), empty());
    }

    @Test
    public void shouldNotSetFlushListenerIfInnerIsNotCaching() {
        assertThat(store.setFlushListener(null, false), is(false));
    }

    @Test
    public void shouldThrowNullPointerOnPutIfKeyIsNull() {
        assertThrows(NullPointerException.class, () -> store.put(null, VALUE, TIMESTAMP));
    }

    @Test
    public void shouldThrowNullPointerOnDeleteIfKeyIsNull() {
        assertThrows(NullPointerException.class, () -> store.delete(null, TIMESTAMP));
    }

    @Test
    public void shouldThrowNullPointerOnGetIfKeyIsNull() {
        assertThrows(NullPointerException.class, () -> store.get(null));
    }

    @Test
    public void shouldThrowNullPointerOnGetWithTimestampIfKeyIsNull() {
        assertThrows(NullPointerException.class, () -> store.get(null, TIMESTAMP));
    }

    private KafkaMetric getMetric(final String name) {
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